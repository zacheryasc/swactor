use std::sync::{Arc, OnceLock};

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::ExternalSender;

use crate::lifecycle::PreparedProcessOutput;
use crate::message::{ProcessActorCommand, ProcessCommand, ProcessOutput};
#[cfg(unix)]
use crate::resources::ProcessSpawnResources;
use crate::supervisor::{
    ProcessSupervisorThread, ProcessThreadHandle, ThreadEvent, ThreadEventReceiver,
    thread_event_channel,
};
use crate::types::{ExitStatus, ProcessSpec};

enum ProcessActorState {
    Spawning { stop_requested: bool },
    Running,
    Stopping,
    Done(ProcessDoneState),
}

enum ProcessDoneState {
    Exited(ExitStatus),
    SpawnFailed(String),
    SupervisorFailed(String),
}

impl ProcessDoneState {
    fn observe(&self) {
        match self {
            Self::Exited(status) => {
                let _ = *status;
            }
            Self::SpawnFailed(error) | Self::SupervisorFailed(error) => {
                let _ = error.as_str();
            }
        }
    }
}

pub(crate) struct ProcessActor {
    spec: Option<ProcessSpec>,
    #[cfg(unix)]
    resources: Option<ProcessSpawnResources>,
    output: PreparedProcessOutput,
    sender: ExternalSender,
    addr_slot: Arc<OnceLock<ActorAddress>>,
    state: ProcessActorState,
    pid: Option<u32>,
    supervisor: Option<ProcessThreadHandle>,
    supervisor_events: Option<ThreadEventReceiver>,
}

impl ProcessActor {
    pub(crate) fn new(
        spec: ProcessSpec,
        #[cfg(unix)] resources: ProcessSpawnResources,
        output: PreparedProcessOutput,
        sender: ExternalSender,
        addr_slot: Arc<OnceLock<ActorAddress>>,
    ) -> Self {
        Self {
            spec: Some(spec),
            #[cfg(unix)]
            resources: Some(resources),
            output,
            sender,
            addr_slot,
            state: ProcessActorState::Spawning {
                stop_requested: false,
            },
            pid: None,
            supervisor: None,
            supervisor_events: None,
        }
    }

    fn emit_process_output(&self, ctx: &Ctx, output: ProcessOutput) {
        let _ = ctx.send(self.output.upstream, output.clone());
        if let Some(mirror) = &self.output.mirror {
            mirror.submit(&output);
        }
    }

    fn apply_stop(&mut self, ctx: &Ctx, kill_after: Option<std::time::Duration>) {
        let mut next_state = None;
        let mut terminal_error = None;

        match &mut self.state {
            ProcessActorState::Spawning { stop_requested } => {
                if *stop_requested {
                    return;
                }

                match self
                    .supervisor
                    .as_ref()
                    .expect("supervisor started before commands")
                    .stop(kill_after)
                {
                    Ok(()) => *stop_requested = true,
                    Err(error) => terminal_error = Some(error),
                }
            }
            ProcessActorState::Running => {
                match self
                    .supervisor
                    .as_ref()
                    .expect("supervisor started before commands")
                    .stop(kill_after)
                {
                    Ok(()) => next_state = Some(ProcessActorState::Stopping),
                    Err(error) => terminal_error = Some(error),
                }
            }
            ProcessActorState::Stopping | ProcessActorState::Done(_) => {}
        }

        if let Some(state) = next_state {
            self.state = state;
        }
        if let Some(error) = terminal_error {
            self.emit_terminal_error(ctx, error);
        }
    }

    fn drain_supervisor_events(&mut self, ctx: &Ctx) {
        let events = self
            .supervisor_events
            .as_ref()
            .map(ThreadEventReceiver::drain)
            .unwrap_or_default();

        for event in events {
            self.handle_thread_event(ctx, event);
            self.finish_if_safe(ctx);
        }
        self.finish_if_safe(ctx);
    }

    fn handle_thread_event(&mut self, ctx: &Ctx, event: ThreadEvent) {
        match event {
            ThreadEvent::Started { pid } => {
                let stop_requested = match &self.state {
                    ProcessActorState::Spawning { stop_requested } => *stop_requested,
                    ProcessActorState::Running
                    | ProcessActorState::Stopping
                    | ProcessActorState::Done(_) => return,
                };

                self.pid = Some(pid);
                self.emit_process_output(ctx, ProcessOutput::Started { pid });
                self.state = if stop_requested {
                    ProcessActorState::Stopping
                } else {
                    ProcessActorState::Running
                };
            }
            ThreadEvent::SpawnFailed { error } => {
                if !matches!(self.state, ProcessActorState::Done(_)) {
                    self.emit_process_output(
                        ctx,
                        ProcessOutput::SpawnFailed {
                            error: error.clone(),
                        },
                    );
                    self.state = ProcessActorState::Done(ProcessDoneState::SpawnFailed(error));
                }
            }
            ThreadEvent::Exited { status } => {
                if !matches!(self.state, ProcessActorState::Done(_)) {
                    self.emit_process_output(ctx, ProcessOutput::Exited { status });
                    self.state = ProcessActorState::Done(ProcessDoneState::Exited(status));
                }
            }
            ThreadEvent::Output { stderr, bytes } => {
                self.emit_process_output(
                    ctx,
                    if stderr {
                        ProcessOutput::Stderr(bytes)
                    } else {
                        ProcessOutput::Stdout(bytes)
                    },
                );
            }
            ThreadEvent::Error { error } => {
                if !matches!(self.state, ProcessActorState::Done(_)) {
                    self.emit_process_output(
                        ctx,
                        ProcessOutput::Error {
                            error: error.clone(),
                        },
                    );
                    self.state = ProcessActorState::Done(ProcessDoneState::SupervisorFailed(error));
                }
            }
            ThreadEvent::ThreadFinished => {
                if let Some(supervisor) = self.supervisor.as_mut() {
                    match supervisor.join_if_finished() {
                        Ok(_) => {}
                        Err(_) => {
                            if !matches!(self.state, ProcessActorState::Done(_)) {
                                let error = "process supervisor thread failed".to_owned();
                                self.emit_process_output(
                                    ctx,
                                    ProcessOutput::Error {
                                        error: error.clone(),
                                    },
                                );
                                self.state = ProcessActorState::Done(
                                    ProcessDoneState::SupervisorFailed(error),
                                );
                            }
                        }
                    }
                }
                self.supervisor = None;
                self.supervisor_events = None;
            }
        }
    }

    fn finish_if_safe(&mut self, ctx: &Ctx) {
        if let ProcessActorState::Done(done) = &self.state {
            done.observe();
            let _ = self.pid;
            if self.supervisor.is_none() {
                ctx.stop_self();
            }
        }
    }

    fn emit_terminal_error(&mut self, ctx: &Ctx, error: String) {
        if matches!(self.state, ProcessActorState::Done(_)) {
            return;
        }

        self.emit_process_output(
            ctx,
            ProcessOutput::Error {
                error: error.clone(),
            },
        );
        self.state = ProcessActorState::Done(ProcessDoneState::SupervisorFailed(error));
        ctx.stop_self();
    }
}

impl ActorInterface for ProcessActor {
    type Incoming = ProcessActorCommand;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let sender = self.sender.clone();
        let addr_slot = self.addr_slot.clone();
        let (event_sink, event_receiver) = thread_event_channel(move || {
            if let Some(addr) = addr_slot.get() {
                let _ = sender.send_to(*addr, ProcessActorCommand::SupervisorWake);
            }
        });

        let spec = self.spec.take().expect("process spec already taken");
        #[cfg(unix)]
        let resources = self
            .resources
            .take()
            .expect("process resources already taken");
        match ProcessSupervisorThread::start(
            spec,
            #[cfg(unix)]
            resources,
            event_sink,
        ) {
            Ok(supervisor) => {
                self.supervisor = Some(supervisor);
                self.supervisor_events = Some(event_receiver);
                self.drain_supervisor_events(ctx);
            }
            Err(err) => {
                let error = format!("process supervisor thread failed: {err}");
                self.emit_process_output(
                    ctx,
                    ProcessOutput::Error {
                        error: error.clone(),
                    },
                );
                self.state = ProcessActorState::Done(ProcessDoneState::SupervisorFailed(error));
                ctx.stop_self();
            }
        }
    }

    fn handle(&mut self, ctx: &Ctx, msg: ProcessActorCommand) {
        match msg {
            ProcessActorCommand::SupervisorWake => self.drain_supervisor_events(ctx),
            ProcessActorCommand::Command(ProcessCommand::Stop { kill_after }) => {
                self.drain_supervisor_events(ctx);
                self.apply_stop(ctx, kill_after);
                self.drain_supervisor_events(ctx);
            }
        }
    }

    fn on_stop(&mut self, _ctx: &Ctx) {
        if let Some(supervisor) = &self.supervisor {
            let _ = supervisor.shutdown_now();
        }
    }
}
