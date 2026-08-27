use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::time::Duration;

use data_plane::bootstrap::channel::{
    AttachmentResult, BootstrapCancellation, BootstrapChannelError, BootstrapHost,
    CHILD_BOOTSTRAP_FD, SessionBootstrap,
};
use data_plane::protocol::{DataPlaneError, HostSessionIn};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::ExternalSender;
use swactor_engine::{ActorCompletion, ActorTimer, BlockingWorkSender, EngineHandle};
use swactor_process::{
    ProcessCommand, ProcessOutput, ProcessSpawnResources, send_process_command,
    spawn_local_process_with_resources,
};

use crate::coordinator::Coordinator;
use crate::model::{
    BootstrapFailure, ContextResolution, ContextualProcessSpec, Effect, Event, EventKind,
    ExecutionIdentity,
};
use crate::output::ContextualProcessOutputConfig;
use crate::ports::{ContextProvisioner, ProvisionedContext};

#[derive(Clone, Debug)]
pub enum ContextualProcessCommand {
    Stop { kill_after: Option<Duration> },
}

#[derive(Clone)]
pub(crate) enum ContextualActorMessage {
    Event(EventKind),
    Process(ProcessOutput),
    AttachmentResult {
        result: AttachmentResult,
        acknowledged: ActorCompletion<()>,
    },
    Command(ContextualProcessCommand),
    SessionClosed(Result<(), String>),
}

struct ActiveContext {
    host_session: ActorAddress,
    arena_fd: Option<OwnedFd>,
    child_bootstrap: Option<OwnedFd>,
    bootstrap_host: Option<BootstrapHost>,
    bootstrap_cancellation: Option<BootstrapCancellation>,
    material: SessionBootstrap,
    session_closed: bool,
    close_requested: bool,
}

impl From<ProvisionedContext> for ActiveContext {
    fn from(context: ProvisionedContext) -> Self {
        Self {
            host_session: context.host_session,
            arena_fd: Some(context.arena_fd),
            child_bootstrap: Some(context.child_bootstrap),
            bootstrap_host: Some(context.bootstrap_host),
            bootstrap_cancellation: Some(context.bootstrap_cancellation),
            material: context.material,
            session_closed: false,
            close_requested: false,
        }
    }
}

pub(crate) struct ContextualProcessActor {
    identity: ExecutionIdentity,
    coordinator: Coordinator,
    spec: ContextualProcessSpec,
    output: ContextualProcessOutputConfig,
    provisioner: Arc<dyn ContextProvisioner>,
    engine: EngineHandle,
    blocking_work: BlockingWorkSender,
    sender: ExternalSender,
    default_kill_after: Option<Duration>,
    requested_kill_after: Option<Option<Duration>>,
    active: Option<ActiveContext>,
    process: Option<ActorAddress>,
    deadline: Option<ActorTimer>,
}

pub(crate) struct ContextualProcessActorConfig {
    pub identity: ExecutionIdentity,
    pub spec: ContextualProcessSpec,
    pub output: ContextualProcessOutputConfig,
    pub provisioner: Arc<dyn ContextProvisioner>,
    pub engine: EngineHandle,
    pub sender: ExternalSender,
    pub default_kill_after: Option<Duration>,
}

impl ContextualProcessActor {
    pub(crate) fn new(config: ContextualProcessActorConfig) -> Self {
        Self {
            identity: config.identity,
            coordinator: Coordinator::new(config.identity, config.spec.attach_deadline),
            spec: config.spec,
            output: config.output,
            provisioner: config.provisioner,
            blocking_work: config.engine.blocking_work_sender(),
            engine: config.engine,
            sender: config.sender,
            default_kill_after: config.default_kill_after,
            requested_kill_after: None,
            active: None,
            process: None,
            deadline: None,
        }
    }

    fn deliver(&mut self, ctx: &Ctx<'_>, kind: EventKind) {
        let effects = self.coordinator.apply(Event::new(self.identity, kind));
        self.execute(ctx, effects);
    }

    fn execute(&mut self, ctx: &Ctx<'_>, effects: Vec<Effect>) {
        let mut follow_up = Vec::new();
        for effect in effects {
            match effect {
                Effect::ProvisionSession => {
                    match self
                        .provisioner
                        .provision(self.identity, self.spec.access.clone())
                    {
                        Ok(context) => {
                            self.active = Some(context.into());
                            follow_up.push(EventKind::ProvisionSucceeded);
                        }
                        Err(error) => follow_up.push(EventKind::ProvisionFailed(error)),
                    }
                }
                Effect::SpawnNativeProcess => {
                    if let Err(error) = self.spawn_native(ctx) {
                        follow_up.push(EventKind::Process(ProcessOutput::Error { error }));
                    }
                }
                Effect::ArmAttachmentDeadline(duration) => {
                    self.deadline = Some(self.engine.send_after(
                        duration,
                        self.sender.clone(),
                        ctx.self_addr(),
                        ContextualActorMessage::Event(EventKind::AttachmentDeadline),
                    ));
                }
                Effect::CancelAttachmentDeadline => {
                    if let Some(deadline) = self.deadline.take() {
                        deadline.cancel();
                    }
                }
                Effect::AcceptBootstrap => {
                    if let Err(error) = self.start_bootstrap_task(ctx.self_addr()) {
                        follow_up.push(EventKind::BootstrapRejected(error));
                    }
                }
                Effect::RejectBootstrap { reason } => {
                    if let Some(active) = &mut self.active
                        && let Some(mut host) = active.bootstrap_host.take()
                    {
                        let _ = host.reject_claim(&format!("{reason:?}"));
                    }
                }
                Effect::CloseBootstrap => {
                    let must_interrupt = matches!(
                        self.coordinator.snapshot().context_resolution,
                        Some(ContextResolution::Failed(
                            BootstrapFailure::StopRequested
                                | BootstrapFailure::AttachmentDeadline
                                | BootstrapFailure::ProcessExitedBeforeReady
                                | BootstrapFailure::ProcessErrorBeforeReady(_)
                                | BootstrapFailure::SessionFault(_)
                        ))
                    );
                    if let Some(active) = &mut self.active {
                        if let Some(mut cancellation) = active.bootstrap_cancellation.take()
                            && must_interrupt
                        {
                            cancellation.cancel();
                        }
                        active.bootstrap_host = None;
                        active.child_bootstrap = None;
                    }
                }
                Effect::Emit(output) => {
                    let _ = ctx.send(self.output.upstream(), output);
                }
                Effect::StopNativeProcess => {
                    if let Some(process) = self.process {
                        let kill_after = self
                            .requested_kill_after
                            .flatten()
                            .or(self.default_kill_after);
                        let _ = send_process_command(
                            &self.sender,
                            process,
                            ProcessCommand::Stop { kill_after },
                        );
                    }
                }
                Effect::RevokeSession => {
                    if let Some(active) = &self.active {
                        let _ = self
                            .sender
                            .send_to(active.host_session, HostSessionIn::Revoke);
                    }
                }
                Effect::ReleaseArena => {
                    if let Err(error) = self.release_context(ctx) {
                        follow_up.push(EventKind::SessionFault(error));
                    }
                }
                Effect::Finish => ctx.stop_self(),
            }
        }
        for event in follow_up {
            self.deliver(ctx, event);
        }
    }

    fn spawn_native(&mut self, ctx: &Ctx<'_>) -> Result<(), String> {
        let active = self
            .active
            .as_mut()
            .ok_or_else(|| "context was not provisioned before process spawn".to_owned())?;
        let child_bootstrap = active
            .child_bootstrap
            .take()
            .ok_or_else(|| "child bootstrap descriptor was already consumed".to_owned())?;
        let resources = ProcessSpawnResources::new()
            .with_descriptor(child_bootstrap, CHILD_BOOTSTRAP_FD)
            .map_err(|error| error.to_string())?;
        let relay = ctx
            .spawn(ProcessOutputRelay {
                owner: ctx.self_addr(),
            })
            .map_err(|error| format!("spawn contextual process output relay: {error}"))?;
        let process = spawn_local_process_with_resources(
            ctx,
            &self.sender,
            self.spec.process.clone(),
            resources,
            self.output.process_output(relay),
        )
        .map_err(|error| format!("spawn contextual native process: {error}"))?;
        self.process = Some(process);
        Ok(())
    }

    fn start_bootstrap_task(&mut self, owner: ActorAddress) -> Result<(), String> {
        let active = self
            .active
            .as_mut()
            .ok_or_else(|| "context was not provisioned before bootstrap".to_owned())?;
        let mut host = active
            .bootstrap_host
            .take()
            .ok_or_else(|| "bootstrap host endpoint is unavailable".to_owned())?;
        let arena_fd = active
            .arena_fd
            .as_ref()
            .ok_or_else(|| "context arena was released before bootstrap".to_owned())?
            .try_clone()
            .map_err(|error| format!("duplicate bootstrap arena descriptor: {error}"))?;
        let material = active.material.clone();
        let sender = self.sender.clone();
        let identity = self.identity;
        let work = Box::new(move || {
            let result = host.accept_claim(&material, std::os::fd::AsRawFd::as_raw_fd(&arena_fd));
            let mut attachment = match result {
                Ok(attachment) => attachment,
                Err(BootstrapChannelError::ChannelClosed) => {
                    let _ = sender.send_to(
                        owner,
                        ContextualActorMessage::Event(EventKind::BootstrapClosed),
                    );
                    return;
                }
                Err(error) => {
                    let _ = sender.send_to(
                        owner,
                        ContextualActorMessage::Event(EventKind::BootstrapRejected(
                            error.to_string(),
                        )),
                    );
                    return;
                }
            };
            if sender
                .send_to(
                    owner,
                    ContextualActorMessage::Event(EventKind::BootstrapClaimed {
                        handle_execution_id: identity.execution_id,
                    }),
                )
                .is_err()
            {
                return;
            }
            let result = attachment.receive_result();
            match result {
                Ok(result) => {
                    let acknowledged = ActorCompletion::new();
                    if sender
                        .send_to(
                            owner,
                            ContextualActorMessage::AttachmentResult {
                                result,
                                acknowledged: acknowledged.clone(),
                            },
                        )
                        .is_ok()
                    {
                        acknowledged.wait();
                        let _ = attachment.acknowledge();
                    }
                }
                Err(BootstrapChannelError::ChannelClosed) => {
                    let _ = sender.send_to(
                        owner,
                        ContextualActorMessage::Event(EventKind::BootstrapClosed),
                    );
                }
                Err(error) => {
                    let _ = sender.send_to(
                        owner,
                        ContextualActorMessage::Event(EventKind::AttachmentFailed(
                            error.to_string(),
                        )),
                    );
                }
            }
        });
        self.blocking_work
            .submit(work)
            .map_err(|_| "contextual bootstrap blocking service is unavailable".to_owned())
    }

    fn release_context(&mut self, ctx: &Ctx<'_>) -> Result<(), String> {
        let Some(active) = &mut self.active else {
            self.deliver(ctx, EventKind::CleanupCompleted);
            return Ok(());
        };
        active.arena_fd = None;
        if active.close_requested {
            return Ok(());
        }
        active.close_requested = true;
        let relay = ctx
            .spawn(SessionCloseRelay {
                owner: ctx.self_addr(),
            })
            .map_err(|error| format!("spawn session close relay: {error}"))?;
        self.sender
            .send_to(
                active.host_session,
                HostSessionIn::Close {
                    reply_to: Some(relay),
                },
            )
            .map_err(|error| format!("close contextual host session: {error}"))
    }

    fn session_closed(&mut self, ctx: &Ctx<'_>, result: Result<(), String>) {
        if let Some(active) = &mut self.active {
            active.session_closed = true;
        }
        if let Err(error) = result {
            self.deliver(ctx, EventKind::SessionFault(error));
        }
        self.deliver(ctx, EventKind::CleanupCompleted);
    }
}

impl ActorInterface for ContextualProcessActor {
    type Incoming = ContextualActorMessage;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.deliver(ctx, EventKind::SpawnRequested);
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: ContextualActorMessage) {
        match message {
            ContextualActorMessage::Event(event) => self.deliver(ctx, event),
            ContextualActorMessage::Process(output) => {
                self.deliver(ctx, EventKind::Process(output));
            }
            ContextualActorMessage::Command(ContextualProcessCommand::Stop { kill_after }) => {
                self.requested_kill_after = Some(kill_after);
                self.deliver(ctx, EventKind::StopRequested);
            }
            ContextualActorMessage::AttachmentResult {
                result,
                acknowledged,
            } => {
                let event = match result {
                    AttachmentResult::Succeeded => EventKind::AttachmentSucceeded,
                    AttachmentResult::Failed(reason) => EventKind::AttachmentFailed(reason),
                };
                self.deliver(ctx, event);
                let _ = acknowledged.complete(());
            }
            ContextualActorMessage::SessionClosed(result) => self.session_closed(ctx, result),
        }
    }

    fn on_stop(&mut self, _ctx: &Ctx<'_>) {
        if let Some(deadline) = self.deadline.take() {
            deadline.cancel();
        }
        if let Some(active) = &mut self.active
            && let Some(cancellation) = &mut active.bootstrap_cancellation
        {
            cancellation.cancel();
        }
    }
}

struct ProcessOutputRelay {
    owner: ActorAddress,
}

impl ActorInterface for ProcessOutputRelay {
    type Incoming = ProcessOutput;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, output: ProcessOutput) {
        let terminal = matches!(
            output,
            ProcessOutput::SpawnFailed { .. }
                | ProcessOutput::Exited { .. }
                | ProcessOutput::Error { .. }
        );
        let _ = ctx.send(self.owner, ContextualActorMessage::Process(output));
        if terminal {
            ctx.stop_self();
        }
    }
}

struct SessionCloseRelay {
    owner: ActorAddress,
}

impl ActorInterface for SessionCloseRelay {
    type Incoming = Result<(), DataPlaneError>;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        let result = result.map_err(|error| error.to_string());
        let _ = ctx.send(self.owner, ContextualActorMessage::SessionClosed(result));
        ctx.stop_self();
    }
}
