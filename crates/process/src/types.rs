use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_queue::SegQueue;

use crate::action::ProcessAction;
use crate::event::ProcessEvent;

/// Describes how to spawn a process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSpec {
    pub command: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub working_dir: Option<String>,
    pub mode: ProcessMode,
    pub initial_pty_size: Option<PtySize>,
    /// If set, escalate to SIGKILL after this duration if the process hasn't exited
    /// after SIGTERM. None = no escalation.
    pub kill_timeout: Option<Duration>,
    /// If set, buffer stdin writes when pending bytes exceed this limit.
    /// None = unlimited (current behavior).
    pub stdin_buffer_limit: Option<usize>,
}

/// Whether the process is interactive (PTY) or automated (pipes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessMode {
    Interactive,
    Automated,
}

/// Dimensions of a pseudo-terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PtySize {
    pub cols: u16,
    pub rows: u16,
}

/// How a process exited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitStatus {
    Code(i32),
    Signal(i32),
    Unknown,
}

/// Signals that can be sent to a process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Terminate,
    Kill,
    Hangup,
    Interrupt,
    Other(i32),
}

/// Errors produced by the process session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessError {
    SpawnFailed { reason: String },
    ConnectionLost { reason: String },
    InvalidState { attempted: &'static str, current_state: &'static str },
}

/// Passive tracking of stdin backpressure.
#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Default)]
pub struct FlowControl {
    pub pending_stdin_bytes: usize,
}

// ─── ProcessDriver ─────────────────────────────────────────────────────────

/// Abstraction over the mechanism that actually runs a process.
///
/// Implementations translate `ProcessAction` commands into real I/O (or mock I/O)
/// and produce `ProcessEvent`s by polling for state changes.
pub trait ProcessDriver: Send {
    /// Execute an action (spawn, write stdin, send signal, etc.).
    fn execute(&mut self, action: ProcessAction);

    /// Poll for new events from the underlying process.
    fn poll(&mut self) -> Vec<ProcessEvent>;
}

// ─── ProcessWaker ──────────────────────────────────────────────────────────

/// A handle that I/O threads use to wake the owning actor.
///
/// Constructed with a closure that sends a `ProcessCommand::PollTick`
/// to the actor via `ExternalSender`. Thread-safe and cloneable.
#[derive(Clone)]
pub struct ProcessWaker(Arc<dyn Fn() + Send + Sync>);

impl std::fmt::Debug for ProcessWaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessWaker").finish_non_exhaustive()
    }
}

impl ProcessWaker {
    pub fn new(f: impl Fn() + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    /// Wake the owning actor so it drains pending events.
    pub fn wake(&self) {
        (self.0)();
    }
}

// ─── EventQueue ────────────────────────────────────────────────────────────

/// Thread-safe queue for buffering process events from I/O threads.
///
/// Cloneable via inner `Arc` — I/O threads push events, the driver's
/// `poll()` drains them.
#[derive(Clone)]
pub struct EventQueue {
    inner: Arc<SegQueue<ProcessEvent>>,
}

impl EventQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SegQueue::new()),
        }
    }

    /// Push an event (called from I/O threads).
    pub fn push(&self, event: ProcessEvent) {
        self.inner.push(event);
    }

    /// Drain all pending events (called from driver's `poll()`).
    pub fn drain(&self) -> Vec<ProcessEvent> {
        let mut events = Vec::new();
        while let Some(event) = self.inner.pop() {
            events.push(event);
        }
        events
    }
}

impl Default for EventQueue {
    fn default() -> Self {
        Self::new()
    }
}

