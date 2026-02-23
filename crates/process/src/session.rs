use std::collections::VecDeque;

use crate::action::{OutputStream, ProcessAction};
use crate::event::ProcessEvent;
use crate::subscriber::SubscriberSet;
use crate::types::{ExitStatus, FlowControl, ProcessError, ProcessMode, ProcessSpec, Signal};

/// The lifecycle states of a process session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessState {
    Starting,
    Running,
    Stopping,
    Exited,
}

impl ProcessState {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Starting => "Starting",
            Self::Running => "Running",
            Self::Stopping => "Stopping",
            Self::Exited => "Exited",
        }
    }
}

/// Pure-logic state machine for managing a process lifecycle.
///
/// Created via `new()` which returns the session plus initial actions (SpawnProcess).
/// Drive it forward by calling `apply(event)` which returns actions to execute.
pub struct ProcessSession {
    spec: ProcessSpec,
    state: ProcessState,
    subscribers: SubscriberSet,
    flow: FlowControl,
    exit_status: Option<ExitStatus>,
    stdin_closed: bool,
    close_requested_before_start: bool,
    stdin_buffer: VecDeque<Vec<u8>>,
    stdin_buffer_bytes: usize,
}

impl ProcessSession {
    /// Create a new session. Returns the session and the initial actions to execute
    /// (always a single `SpawnProcess` action).
    pub fn new(spec: ProcessSpec) -> (Self, Vec<ProcessAction>) {
        let actions = vec![ProcessAction::SpawnProcess { spec: spec.clone() }];
        let session = Self {
            spec,
            state: ProcessState::Starting,
            subscribers: SubscriberSet::new(),
            flow: FlowControl::default(),
            exit_status: None,
            stdin_closed: false,
            close_requested_before_start: false,
            stdin_buffer: VecDeque::new(),
            stdin_buffer_bytes: 0,
        };
        (session, actions)
    }

    /// Apply an event and return the resulting actions.
    pub fn apply(&mut self, event: ProcessEvent) -> Vec<ProcessAction> {
        // Subscribe/Unsubscribe handled in all states
        match &event {
            ProcessEvent::Subscribe { address } => {
                self.subscribers.add(*address);
                return vec![];
            }
            ProcessEvent::Unsubscribe { address } => {
                self.subscribers.remove(address);
                return vec![];
            }
            _ => {}
        }

        // Driver acks — silently consumed in all states
        match &event {
            ProcessEvent::StdinWritten { byte_count } => {
                self.flow.pending_stdin_bytes =
                    self.flow.pending_stdin_bytes.saturating_sub(*byte_count);
                return self.drain_stdin_buffer();
            }
            ProcessEvent::SignalSent | ProcessEvent::PtyResized => {
                return vec![];
            }
            _ => {}
        }

        // KillTimeout — handled in all states before per-state dispatch
        if matches!(event, ProcessEvent::KillTimeout) {
            return if self.state == ProcessState::Stopping {
                vec![ProcessAction::SendSignal { signal: Signal::Kill }]
            } else {
                vec![]
            };
        }

        // Dispatch to per-state handler
        match self.state {
            ProcessState::Starting => self.handle_starting(event),
            ProcessState::Running => self.handle_running(event),
            ProcessState::Stopping => self.handle_stopping(event),
            ProcessState::Exited => self.handle_exited(event),
        }
    }

    // --- Per-state handlers ---

    fn handle_starting(&mut self, event: ProcessEvent) -> Vec<ProcessAction> {
        match event {
            ProcessEvent::Started => {
                self.state = ProcessState::Running;
                let mut actions = vec![ProcessAction::NotifyStarted {
                    subscribers: self.subscribers.snapshot(),
                }];
                // If close was requested before the process started, transition to Stopping
                if self.close_requested_before_start {
                    self.state = ProcessState::Stopping;
                    actions.push(ProcessAction::SendSignal {
                        signal: Signal::Terminate,
                    });
                    if let Some(duration) = self.spec.kill_timeout {
                        actions.push(ProcessAction::ScheduleKillTimeout { duration });
                    }
                }
                actions
            }
            ProcessEvent::SpawnFailed { reason } => {
                self.state = ProcessState::Exited;
                vec![
                    ProcessAction::NotifyError {
                        subscribers: self.subscribers.snapshot(),
                        error: ProcessError::SpawnFailed { reason },
                    },
                    ProcessAction::SelfTerminate,
                ]
            }
            ProcessEvent::CloseRequested => {
                self.close_requested_before_start = true;
                vec![]
            }
            _ => self.invalid_state_error(&event),
        }
    }

    fn handle_running(&mut self, event: ProcessEvent) -> Vec<ProcessAction> {
        match event {
            ProcessEvent::OutputReceived { data, is_stderr } => {
                let stream = if is_stderr {
                    OutputStream::Stderr
                } else {
                    OutputStream::Stdout
                };
                vec![ProcessAction::NotifyOutput {
                    subscribers: self.subscribers.snapshot(),
                    data,
                    stream,
                }]
            }
            ProcessEvent::Exited { status } => {
                self.enter_exited(status)
            }
            ProcessEvent::ConnectionLost { reason } => {
                self.state = ProcessState::Exited;
                self.exit_status = Some(ExitStatus::Unknown);
                self.clear_stdin_buffer();
                vec![
                    ProcessAction::NotifyError {
                        subscribers: self.subscribers.snapshot(),
                        error: ProcessError::ConnectionLost { reason },
                    },
                    ProcessAction::SelfTerminate,
                ]
            }
            ProcessEvent::WriteStdin { data } => {
                if self.stdin_closed {
                    return self.notify_error(ProcessError::InvalidState {
                        attempted: "WriteStdin",
                        current_state: "Running (stdin closed)",
                    });
                }
                // Backpressure: buffer if over limit
                if let Some(limit) = self.spec.stdin_buffer_limit {
                    if self.flow.pending_stdin_bytes >= limit {
                        self.stdin_buffer_bytes += data.len();
                        self.stdin_buffer.push_back(data);
                        return vec![];
                    }
                }
                self.flow.pending_stdin_bytes += data.len();
                vec![ProcessAction::WriteStdin { data }]
            }
            ProcessEvent::SendSignal { signal } => {
                vec![ProcessAction::SendSignal { signal }]
            }
            ProcessEvent::ResizePty { size } => {
                vec![ProcessAction::ResizePty { size }]
            }
            ProcessEvent::CloseStdin => {
                if self.stdin_closed {
                    return vec![];
                }
                self.stdin_closed = true;
                self.clear_stdin_buffer();
                vec![ProcessAction::CloseStdin]
            }
            ProcessEvent::CloseRequested => {
                self.state = ProcessState::Stopping;
                self.clear_stdin_buffer();
                let mut actions = vec![ProcessAction::SendSignal {
                    signal: Signal::Terminate,
                }];
                if let Some(duration) = self.spec.kill_timeout {
                    actions.push(ProcessAction::ScheduleKillTimeout { duration });
                }
                actions
            }
            _ => self.invalid_state_error(&event),
        }
    }

    fn handle_stopping(&mut self, event: ProcessEvent) -> Vec<ProcessAction> {
        match event {
            ProcessEvent::OutputReceived { data, is_stderr } => {
                let stream = if is_stderr {
                    OutputStream::Stderr
                } else {
                    OutputStream::Stdout
                };
                vec![ProcessAction::NotifyOutput {
                    subscribers: self.subscribers.snapshot(),
                    data,
                    stream,
                }]
            }
            ProcessEvent::Exited { status } => {
                self.enter_exited(status)
            }
            ProcessEvent::ConnectionLost { reason } => {
                self.state = ProcessState::Exited;
                self.exit_status = Some(ExitStatus::Unknown);
                vec![
                    ProcessAction::NotifyError {
                        subscribers: self.subscribers.snapshot(),
                        error: ProcessError::ConnectionLost { reason },
                    },
                    ProcessAction::SelfTerminate,
                ]
            }
            ProcessEvent::SendSignal { signal } => {
                // Escalation (e.g., Kill after Terminate) is allowed in Stopping
                vec![ProcessAction::SendSignal { signal }]
            }
            ProcessEvent::CloseStdin => {
                if self.stdin_closed {
                    return vec![];
                }
                self.stdin_closed = true;
                vec![ProcessAction::CloseStdin]
            }
            ProcessEvent::CloseRequested => {
                // Already stopping, no-op
                vec![]
            }
            _ => self.invalid_state_error(&event),
        }
    }

    fn handle_exited(&mut self, event: ProcessEvent) -> Vec<ProcessAction> {
        // Everything in Exited is invalid — produce an error.
        // (Acks and Subscribe/Unsubscribe are already handled before dispatch.)
        self.invalid_state_error(&event)
    }

    // --- Helpers ---

    fn enter_exited(&mut self, status: ExitStatus) -> Vec<ProcessAction> {
        self.state = ProcessState::Exited;
        self.exit_status = Some(status);
        self.clear_stdin_buffer();
        vec![
            ProcessAction::NotifyExited {
                subscribers: self.subscribers.snapshot(),
                status,
            },
            ProcessAction::SelfTerminate,
        ]
    }

    fn clear_stdin_buffer(&mut self) {
        self.stdin_buffer.clear();
        self.stdin_buffer_bytes = 0;
    }

    fn drain_stdin_buffer(&mut self) -> Vec<ProcessAction> {
        let limit = match self.spec.stdin_buffer_limit {
            Some(limit) => limit,
            None => return vec![],
        };
        let mut actions = Vec::new();
        while self.flow.pending_stdin_bytes < limit {
            match self.stdin_buffer.pop_front() {
                Some(data) => {
                    self.stdin_buffer_bytes -= data.len();
                    self.flow.pending_stdin_bytes += data.len();
                    actions.push(ProcessAction::WriteStdin { data });
                }
                None => break,
            }
        }
        actions
    }

    fn invalid_state_error(&self, event: &ProcessEvent) -> Vec<ProcessAction> {
        self.notify_error(ProcessError::InvalidState {
            attempted: event.name(),
            current_state: self.state.name(),
        })
    }

    fn notify_error(&self, error: ProcessError) -> Vec<ProcessAction> {
        vec![ProcessAction::NotifyError {
            subscribers: self.subscribers.snapshot(),
            error,
        }]
    }

    // --- Query methods ---

    pub fn state(&self) -> ProcessState {
        self.state
    }

    pub fn spec(&self) -> &ProcessSpec {
        &self.spec
    }

    pub fn mode(&self) -> ProcessMode {
        self.spec.mode
    }

    pub fn exit_status(&self) -> Option<ExitStatus> {
        self.exit_status
    }

    pub fn flow_control(&self) -> &FlowControl {
        &self.flow
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscribers.count()
    }

    pub fn stdin_closed(&self) -> bool {
        self.stdin_closed
    }

    pub fn stdin_buffer_bytes(&self) -> usize {
        self.stdin_buffer_bytes
    }
}
