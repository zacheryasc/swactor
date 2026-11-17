use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::time::Duration;

use data_plane::bootstrap::channel::{
    AttachmentResult, BootstrapCancellation, BootstrapChannelError, BootstrapHost,
    CHILD_BOOTSTRAP_FD, SessionBootstrap,
};
use data_plane::protocol::{DataPlaneError, HostSessionIn};
use protocol_telemetry::TransitionEmitter;
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
    Stop {
        kill_after: Option<Duration>,
    },
    StopAcknowledged {
        kill_after: Option<Duration>,
        operation: u64,
    },
    /// Request graceful native termination without timed escalation.
    StopGracefully,
    StopGracefullyAcknowledged {
        operation: u64,
    },
    /// Cancel the owned native process immediately and observe its real exit.
    Cancel,
    CancelAcknowledged {
        operation: u64,
    },
}

#[derive(Clone)]
pub(crate) enum ContextualActorMessage {
    Event(EventKind),
    NativeLifecycle {
        identity: ExecutionIdentity,
        output: ProcessOutput,
    },
    ProcessStream {
        relay: ActorAddress,
        output: ProcessOutput,
    },
    ProcessStreamDrained {
        relay: ActorAddress,
    },
    ProcessObserverLost {
        relay: ActorAddress,
    },
    NativeControlLost {
        identity: ExecutionIdentity,
        error: String,
    },
    AttachmentResult {
        result: AttachmentResult,
        acknowledged: Arc<AttachmentAcknowledgement>,
    },
    Command(ContextualProcessCommand),
    SessionClosed(Result<(), String>),
    SessionCloseDeadline,
}

pub(crate) struct AttachmentAcknowledgement(ActorCompletion<bool>);

impl Drop for AttachmentAcknowledgement {
    fn drop(&mut self) {
        let _ = self.0.complete(false);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionClosePhase {
    Open,
    Closing,
    Closed,
}
struct ActiveContext {
    host_session: ActorAddress,
    arena_fd: Option<OwnedFd>,
    child_bootstrap: Option<OwnedFd>,
    bootstrap_host: Option<BootstrapHost>,
    bootstrap_cancellation: Option<BootstrapCancellation>,
    material: SessionBootstrap,
    close_phase: SessionClosePhase,
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
            close_phase: SessionClosePhase::Open,
        }
    }
}

#[derive(Clone, Copy)]
struct NativeProcess {
    actor: ActorAddress,
    output: ActorAddress,
    observer_lost: bool,
    streams_drained: bool,
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
    force_cancel: bool,
    active: Option<ActiveContext>,
    process: Option<NativeProcess>,
    native_completion: Option<ProcessOutput>,
    deadline: Option<ActorTimer>,
    close_deadline: Option<ActorTimer>,
    close_relay: Option<ActorAddress>,
    transitions: Option<TransitionEmitter>,
}

pub(crate) struct ContextualProcessActorConfig {
    pub identity: ExecutionIdentity,
    pub spec: ContextualProcessSpec,
    pub output: ContextualProcessOutputConfig,
    pub provisioner: Arc<dyn ContextProvisioner>,
    pub engine: EngineHandle,
    pub sender: ExternalSender,
    pub default_kill_after: Option<Duration>,
    pub transitions: Option<TransitionEmitter>,
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
            force_cancel: false,
            native_completion: None,
            active: None,
            process: None,
            deadline: None,
            close_deadline: None,
            close_relay: None,
            transitions: config.transitions,
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
                    self.output.emit(ctx, output);
                }
                Effect::StopNativeProcess => self.stop_native(ctx),
                Effect::RevokeSession => {
                    if let Some(active) = &self.active {
                        let _ = self
                            .sender
                            .send_to(active.host_session, HostSessionIn::Revoke);
                    }
                }
                Effect::ReleaseArena => {
                    if let Err(error) = self.release_context(ctx) {
                        self.session_closed(ctx, Err(error));
                    }
                }
                Effect::Finish => ctx.stop_self(),
            }
        }
        for event in follow_up {
            self.deliver(ctx, event);
        }
    }

    fn stop_native(&self, ctx: &Ctx<'_>) {
        let Some(process) = self.process else {
            return;
        };
        let command = if self.force_cancel {
            ProcessCommand::Cancel
        } else {
            ProcessCommand::Stop {
                kill_after: if self.spec.attach_deadline.is_none() {
                    None
                } else {
                    self.requested_kill_after.unwrap_or(self.default_kill_after)
                },
            }
        };
        if let Err(error) = send_process_command(&self.sender, process.actor, command) {
            let _ = ctx.send(
                ctx.self_addr(),
                ContextualActorMessage::NativeControlLost {
                    identity: self.identity,
                    error: format!("native process stop route disappeared: {error}"),
                },
            );
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
                owner: Some(ctx.self_addr()),
            })
            .map_err(|error| format!("spawn contextual process output relay: {error}"))?;
        let owner = ctx.self_addr();
        let sender = self.sender.clone();
        let identity = self.identity;
        let output = self
            .output
            .process_output(relay)
            .with_lifecycle_observer(Arc::new(move |output| {
                let _ = sender.send_to(
                    owner,
                    ContextualActorMessage::NativeLifecycle { identity, output },
                );
            }));
        let process = spawn_local_process_with_resources(
            &self.blocking_work,
            ctx,
            &self.sender,
            self.spec.process.clone(),
            resources,
            output,
        )
        .map_err(|error| {
            let _ = ctx.stop_actor(relay);
            format!("spawn contextual native process: {error}")
        })?;
        self.process = Some(NativeProcess {
            actor: process,
            output: relay,
            observer_lost: false,
            streams_drained: false,
        });
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
                                acknowledged: Arc::new(AttachmentAcknowledgement(
                                    acknowledged.clone(),
                                )),
                            },
                        )
                        .is_ok()
                    {
                        if acknowledged.wait() {
                            let _ = attachment.acknowledge();
                        }
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
        if active.close_phase != SessionClosePhase::Open {
            return Ok(());
        }
        active.close_phase = SessionClosePhase::Closing;
        let relay = ctx
            .spawn(SessionCloseRelay {
                owner: Some(ctx.self_addr()),
            })
            .map_err(|error| format!("spawn session close relay: {error}"))?;
        self.close_relay = Some(relay);
        if let Some(deadline) = self.spec.attach_deadline {
            self.close_deadline = Some(self.engine.send_after(
                deadline,
                self.sender.clone(),
                ctx.self_addr(),
                ContextualActorMessage::SessionCloseDeadline,
            ));
        }
        match self.sender.send_to(
            active.host_session,
            HostSessionIn::Close {
                reply_to: Some(relay),
            },
        ) {
            Ok(()) => Ok(()),
            Err(error) => {
                let _ = ctx.stop_actor(relay);
                self.session_closed(ctx, Err(format!("close contextual host session: {error}")));
                Ok(())
            }
        }
    }

    fn session_closed(&mut self, ctx: &Ctx<'_>, result: Result<(), String>) {
        if let Some(deadline) = self.close_deadline.take() {
            deadline.cancel();
        }
        if let Some(relay) = self.close_relay.take() {
            let _ = ctx.stop_actor(relay);
        }
        if let Some(active) = &mut self.active {
            if active.close_phase == SessionClosePhase::Closed {
                return;
            }
            active.close_phase = SessionClosePhase::Closed;
        }
        if let Err(error) = result {
            self.deliver(ctx, EventKind::SessionFault(error));
        }
        self.deliver(ctx, EventKind::CleanupCompleted);
    }

    fn session_close_deadline(&mut self, ctx: &Ctx<'_>) {
        if self
            .active
            .as_ref()
            .is_none_or(|active| active.close_phase != SessionClosePhase::Closing)
        {
            return;
        }
        if let Some(active) = &self.active {
            let _ = ctx.stop_actor(active.host_session);
        }
        self.session_closed(
            ctx,
            Err("contextual session close acknowledgement deadline expired".to_owned()),
        );
    }

    fn stop_requested(
        &mut self,
        ctx: &Ctx<'_>,
        kill_after: Option<Duration>,
        operation: Option<u64>,
    ) {
        self.requested_kill_after = Some(kill_after);
        self.deliver(ctx, EventKind::StopRequested);
        if let Some(operation) = operation {
            self.output.emit(
                ctx,
                crate::model::ContextualProcessOutput::StopAcknowledged { operation },
            );
        }
    }

    fn cancel_requested(&mut self, ctx: &Ctx<'_>, operation: Option<u64>) {
        let escalate = !self.force_cancel
            && (self.coordinator.snapshot().stop_requested
                || self.coordinator.phase() == crate::coordinator::Phase::Stopping);
        self.force_cancel = true;
        self.stop_requested(ctx, None, operation);
        // A prior graceful stop already consumed the coordinator's stop effect;
        // escalation still belongs to this same native process owner.
        if escalate && !self.coordinator.snapshot().process_terminal {
            self.stop_native(ctx);
        }
    }

    fn complete_native(&mut self, ctx: &Ctx<'_>) {
        if self
            .process
            .is_some_and(|process| process.observer_lost || process.streams_drained)
            && let Some(output) = self.native_completion.take()
        {
            self.deliver(ctx, EventKind::Process(output));
        }
    }

    fn attachment_result(
        &mut self,
        ctx: &Ctx<'_>,
        result: AttachmentResult,
        acknowledged: Arc<AttachmentAcknowledgement>,
    ) {
        let pending = self.coordinator.phase() == crate::coordinator::Phase::Attaching;
        let event = match result {
            AttachmentResult::Succeeded => EventKind::AttachmentSucceeded,
            AttachmentResult::Failed(reason) => EventKind::AttachmentFailed(reason),
        };
        self.deliver(ctx, event);
        let _ = acknowledged.0.complete(pending);
    }
}

impl ActorInterface for ContextualProcessActor {
    type Incoming = ContextualActorMessage;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.coordinator
            .set_transitions(ctx.self_addr(), self.transitions.take());
        self.deliver(ctx, EventKind::SpawnRequested);
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: ContextualActorMessage) {
        match message {
            ContextualActorMessage::Event(event) => self.deliver(ctx, event),
            ContextualActorMessage::NativeLifecycle { identity, output } => {
                if identity == self.identity && !self.coordinator.snapshot().process_terminal {
                    if matches!(&output, ProcessOutput::Started { .. }) {
                        self.deliver(ctx, EventKind::Process(output));
                    } else {
                        self.native_completion = Some(output);
                        self.complete_native(ctx);
                    }
                }
            }
            ContextualActorMessage::ProcessStream { relay, output } => {
                if self
                    .process
                    .is_some_and(|process| process.output == relay && !process.observer_lost)
                {
                    self.deliver(ctx, EventKind::Process(output));
                }
            }
            ContextualActorMessage::ProcessStreamDrained { relay } => {
                if let Some(process) = &mut self.process
                    && process.output == relay
                {
                    process.streams_drained = true;
                    self.complete_native(ctx);
                }
            }
            ContextualActorMessage::ProcessObserverLost { relay } => {
                if let Some(process) = &mut self.process
                    && process.output == relay
                    && !process.observer_lost
                    && !self.coordinator.snapshot().process_terminal
                {
                    process.observer_lost = true;
                    if self.native_completion.is_some() {
                        self.complete_native(ctx);
                    } else {
                        self.cancel_requested(ctx, None);
                    }
                }
            }
            ContextualActorMessage::NativeControlLost { identity, error } => {
                if identity == self.identity && !self.coordinator.snapshot().process_terminal {
                    self.deliver(ctx, EventKind::SessionFault(error));
                }
            }
            ContextualActorMessage::Command(ContextualProcessCommand::Stop { kill_after }) => {
                self.stop_requested(ctx, kill_after, None)
            }
            ContextualActorMessage::Command(ContextualProcessCommand::StopAcknowledged {
                kill_after,
                operation,
            }) => self.stop_requested(ctx, kill_after, Some(operation)),
            ContextualActorMessage::Command(ContextualProcessCommand::StopGracefully) => {
                self.stop_requested(ctx, None, None)
            }
            ContextualActorMessage::Command(
                ContextualProcessCommand::StopGracefullyAcknowledged { operation },
            ) => self.stop_requested(ctx, None, Some(operation)),
            ContextualActorMessage::Command(ContextualProcessCommand::Cancel) => {
                self.cancel_requested(ctx, None)
            }
            ContextualActorMessage::Command(ContextualProcessCommand::CancelAcknowledged {
                operation,
            }) => self.cancel_requested(ctx, Some(operation)),
            ContextualActorMessage::AttachmentResult {
                result,
                acknowledged,
            } => self.attachment_result(ctx, result, acknowledged),
            ContextualActorMessage::SessionClosed(result) => self.session_closed(ctx, result),
            ContextualActorMessage::SessionCloseDeadline => {
                if self.spec.attach_deadline.is_some() {
                    self.session_close_deadline(ctx);
                }
            }
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if !self.coordinator.snapshot().finished {
            if self.spec.attach_deadline.is_none() {
                // Losing this owner is explicit cancellation, not an ordinary
                // graceful stop that may still need a later force request.
                self.cancel_requested(ctx, None);
            }
            let effects = self
                .coordinator
                .apply(Event::new(self.identity, EventKind::ParentStopped));
            for effect in effects {
                match effect {
                    Effect::Emit(output) => {
                        self.output.emit(ctx, output);
                    }
                    Effect::StopNativeProcess => self.stop_native(ctx),
                    Effect::RevokeSession | Effect::ReleaseArena => {
                        if let Some(active) = &mut self.active {
                            active.arena_fd = None;
                            let _ = self.sender.send_to(
                                active.host_session,
                                HostSessionIn::Close { reply_to: None },
                            );
                        }
                    }
                    _ => {}
                }
            }
        }
        if let Some(deadline) = self.deadline.take() {
            deadline.cancel();
        }
        if let Some(deadline) = self.close_deadline.take() {
            deadline.cancel();
        }
        if let Some(relay) = self.close_relay.take() {
            let _ = ctx.stop_actor(relay);
        }
        if let Some(active) = &mut self.active
            && let Some(cancellation) = &mut active.bootstrap_cancellation
        {
            cancellation.cancel();
        }
    }
}

struct ProcessOutputRelay {
    owner: Option<ActorAddress>,
}

impl ActorInterface for ProcessOutputRelay {
    type Incoming = ProcessOutput;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, output: ProcessOutput) {
        let Some(owner) = self.owner else {
            return;
        };
        let terminal = matches!(
            output,
            ProcessOutput::SpawnFailed { .. }
                | ProcessOutput::Exited { .. }
                | ProcessOutput::Error { .. }
        );
        if matches!(&output, ProcessOutput::Stdout(_) | ProcessOutput::Stderr(_)) {
            let _ = ctx.send(
                owner,
                ContextualActorMessage::ProcessStream {
                    relay: ctx.self_addr(),
                    output,
                },
            );
        }
        if terminal {
            let _ = ctx.send(
                owner,
                ContextualActorMessage::ProcessStreamDrained {
                    relay: ctx.self_addr(),
                },
            );
            self.owner = None;
            ctx.stop_self();
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if let Some(owner) = self.owner.take() {
            let _ = ctx.send(
                owner,
                ContextualActorMessage::ProcessObserverLost {
                    relay: ctx.self_addr(),
                },
            );
        }
    }
}

struct SessionCloseRelay {
    owner: Option<ActorAddress>,
}

impl ActorInterface for SessionCloseRelay {
    type Incoming = Result<(), DataPlaneError>;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        let Some(owner) = self.owner.take() else {
            return;
        };
        let result = result.map_err(|error| error.to_string());
        let _ = ctx.send(owner, ContextualActorMessage::SessionClosed(result));
        ctx.stop_self();
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if let Some(owner) = self.owner.take() {
            let _ = ctx.send(
                owner,
                ContextualActorMessage::SessionClosed(Err(
                    "session close observer stopped".to_owned()
                )),
            );
        }
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use swactor::runtime::{RuntimeConfig, RuntimeParts};
    use swactor_engine::{Engine, SteppingBackend};

    struct RejectedProvisioner;

    impl ContextProvisioner for RejectedProvisioner {
        fn provision(
            &self,
            _: ExecutionIdentity,
            _: data_plane::path::SessionAccess,
        ) -> Result<ProvisionedContext, String> {
            Err("provision rejected".to_owned())
        }
    }

    #[test]
    fn provisioning_failure_resolves_native_launch_once_and_quiesces() {
        let parts = RuntimeParts::new(RuntimeConfig {
            worker_count: 1,
            ..RuntimeConfig::default()
        });
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).unwrap();
        let outputs = runtime
            .new_inbox::<crate::model::ContextualProcessOutput>()
            .unwrap();
        let actor = runtime
            .spawn(ContextualProcessActor::new(ContextualProcessActorConfig {
                identity: ExecutionIdentity {
                    execution_id: 1,
                    generation: 1,
                },
                spec: ContextualProcessSpec {
                    process: swactor_process::ProcessSpec {
                        command: "not-executed".to_owned(),
                        args: vec![],
                        env: Default::default(),
                        working_dir: None,
                        label: None,
                    },
                    access: data_plane::path::SessionAccess {
                        execution_id: "rejected".to_owned(),
                        read_prefixes: vec![],
                        write_prefixes: vec![],
                    },
                    attach_deadline: Some(Duration::from_secs(1)),
                },
                output: ContextualProcessOutputConfig::disabled(*outputs.addr()),
                provisioner: Arc::new(RejectedProvisioner),
                engine: engine.handle(),
                sender: runtime.create_sender(),
                default_kill_after: None,
                transitions: None,
            }))
            .unwrap();
        runtime
            .send_to(
                actor,
                ContextualActorMessage::Event(EventKind::ProvisionFailed("duplicate".to_owned())),
            )
            .unwrap();
        for _ in 0..32 {
            backend.step();
        }
        assert!(
            matches!(outputs.try_recv(), Some(crate::model::ContextualProcessOutput::Process(ProcessOutput::SpawnFailed { error })) if error == "provision rejected")
        );
        assert!(outputs.try_recv().is_none());
        assert!(runtime.stats().actors.is_empty());
    }

    struct StopRouteRace {
        actor: ContextualProcessActor,
        queued_terminal: Option<ProcessOutput>,
    }

    impl ActorInterface for StopRouteRace {
        type Incoming = ContextualActorMessage;
        type Response = ();

        fn on_start(&mut self, ctx: &Ctx<'_>) {
            let relay = ctx
                .spawn(ProcessOutputRelay {
                    owner: Some(ctx.self_addr()),
                })
                .unwrap();
            if let Some(terminal) = self.queued_terminal.take() {
                ctx.send(
                    ctx.self_addr(),
                    ContextualActorMessage::NativeLifecycle {
                        identity: self.actor.identity,
                        output: terminal.clone(),
                    },
                )
                .unwrap();
                ctx.send(relay, terminal).unwrap();
            } else {
                ctx.stop_actor(relay).unwrap();
            }
            // The native actor has retired, but its output relay has not run.
            self.actor.process = Some(NativeProcess {
                actor: ActorAddress::new_random(),
                output: relay,
                observer_lost: false,
                streams_drained: false,
            });
            self.actor.execute(ctx, vec![Effect::StopNativeProcess]);
        }

        fn handle(&mut self, ctx: &Ctx<'_>, message: Self::Incoming) {
            self.actor.handle(ctx, message);
        }

        fn on_stop(&mut self, ctx: &Ctx<'_>) {
            self.actor.on_stop(ctx);
        }
    }

    #[test]
    fn stop_route_loss_waits_for_authoritative_completion_and_fences_stale_lifecycle() {
        for queued_exit in [true, false] {
            let parts = RuntimeParts::new(RuntimeConfig {
                worker_count: 1,
                ..RuntimeConfig::default()
            });
            let runtime = parts.runtime().clone();
            let backend = SteppingBackend::new();
            let engine = Engine::new(parts, backend.clone()).unwrap();
            let outputs = runtime
                .new_inbox::<crate::model::ContextualProcessOutput>()
                .unwrap();
            let identity = ExecutionIdentity {
                execution_id: 7,
                generation: 3,
            };
            let mut actor = ContextualProcessActor::new(ContextualProcessActorConfig {
                identity,
                spec: ContextualProcessSpec {
                    process: swactor_process::ProcessSpec {
                        command: "not-executed".to_owned(),
                        args: vec![],
                        env: Default::default(),
                        working_dir: None,
                        label: None,
                    },
                    access: data_plane::path::SessionAccess {
                        execution_id: "stop-route-race".to_owned(),
                        read_prefixes: vec![],
                        write_prefixes: vec![],
                    },
                    attach_deadline: Some(Duration::from_secs(1)),
                },
                output: ContextualProcessOutputConfig::disabled(*outputs.addr()),
                provisioner: Arc::new(RejectedProvisioner),
                engine: engine.handle(),
                sender: runtime.create_sender(),
                default_kill_after: None,
                transitions: None,
            });
            // Start at the native-running/attachment-pending boundary without
            // an OS scheduler: only the actual stop and output paths run below.
            for event in [
                EventKind::SpawnRequested,
                EventKind::ProvisionSucceeded,
                EventKind::Process(ProcessOutput::Started { pid: 41 }),
            ] {
                actor.coordinator.apply(Event::new(identity, event));
            }
            let owner = runtime
                .spawn(StopRouteRace {
                    actor,
                    queued_terminal: queued_exit.then_some(ProcessOutput::Exited {
                        status: swactor_process::ExitStatus::Code(0),
                    }),
                })
                .unwrap();
            for _ in 0..32 {
                backend.step();
            }
            assert!(matches!(
                outputs.try_recv(),
                Some(crate::model::ContextualProcessOutput::BootstrapFailed { .. })
            ));
            let terminal = outputs.try_recv();
            if queued_exit {
                assert_eq!(
                    terminal,
                    Some(crate::model::ContextualProcessOutput::Process(
                        ProcessOutput::Exited {
                            status: swactor_process::ExitStatus::Code(0)
                        }
                    ))
                );
            } else {
                assert!(terminal.is_none(), "route loss is not native completion");
                let stale = ExecutionIdentity {
                    generation: identity.generation - 1,
                    ..identity
                };
                runtime
                    .send_to(
                        owner,
                        ContextualActorMessage::NativeLifecycle {
                            identity: stale,
                            output: ProcessOutput::Exited {
                                status: swactor_process::ExitStatus::Code(0),
                            },
                        },
                    )
                    .unwrap();
                for _ in 0..32 {
                    backend.step();
                }
                assert!(
                    outputs.try_recv().is_none(),
                    "stale native exit cannot release ownership"
                );
                assert!(runtime.stats().actors.iter().any(|actor| actor.0 == owner));
                runtime
                    .send_to(
                        owner,
                        ContextualActorMessage::NativeLifecycle {
                            identity,
                            output: ProcessOutput::Exited {
                                status: swactor_process::ExitStatus::Signal(9),
                            },
                        },
                    )
                    .unwrap();
                for _ in 0..32 {
                    backend.step();
                }
                assert!(matches!(
                    outputs.try_recv(),
                    Some(crate::model::ContextualProcessOutput::Process(
                        ProcessOutput::Exited {
                            status: swactor_process::ExitStatus::Signal(9)
                        }
                    ))
                ));
            }
            assert!(outputs.try_recv().is_none());
            assert!(runtime.stats().actors.is_empty());
        }
    }

    #[test]
    fn dropped_attachment_reply_releases_blocking_acknowledgement_owner() {
        let completion = ActorCompletion::new();
        let acknowledgement = Arc::new(AttachmentAcknowledgement(completion.clone()));
        let queued = ContextualActorMessage::AttachmentResult {
            result: AttachmentResult::Succeeded,
            acknowledged: acknowledgement,
        };
        let duplicate = queued.clone();
        drop(queued);
        assert_eq!(completion.wait_deadline(Duration::ZERO), None);
        drop(duplicate);
        assert_eq!(completion.wait_deadline(Duration::ZERO), Some(false));
    }
}
