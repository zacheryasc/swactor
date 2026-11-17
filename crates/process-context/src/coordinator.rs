use protocol_telemetry::{
    ActorType, Outcome, Phase as TelemetryPhase, Protocol, Reason, TransitionEmitter,
    TransitionRecord,
};
use swactor::actor::ActorAddress;
use swactor_process::ProcessOutput;

use crate::model::{
    BootstrapFailure, ContextResolution, ContextualProcessOutput, CoordinatorSnapshot, Effect,
    Event, EventKind, ExecutionIdentity,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Idle,
    Provisioning,
    Spawning,
    Attaching,
    Running,
    Stopping,
    Releasing,
    Closed,
}

impl Phase {
    fn telemetry(self) -> TelemetryPhase {
        match self {
            Self::Idle => TelemetryPhase::Idle,
            Self::Provisioning => TelemetryPhase::Resolving,
            Self::Spawning => TelemetryPhase::Pending,
            Self::Attaching => TelemetryPhase::AwaitingReadiness,
            Self::Running => TelemetryPhase::Running,
            Self::Stopping => TelemetryPhase::Closing,
            Self::Releasing => TelemetryPhase::Releasing,
            Self::Closed => TelemetryPhase::Closed,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NativePhase {
    Absent,
    Spawning,
    Running,
    Terminal { started: bool },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionPhase {
    Absent,
    Active,
    Revoked,
    Releasing,
    Released,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BootstrapPhase {
    Absent,
    Available,
    Claimed,
    Closed,
}

pub struct Coordinator {
    identity: ExecutionIdentity,
    attach_deadline: Option<std::time::Duration>,
    phase: Phase,
    native: NativePhase,
    session: SessionPhase,
    bootstrap: BootstrapPhase,
    context_resolution: Option<ContextResolution>,
    terminal_outcome: Option<Outcome>,
    bootstrap_claimed: bool,
    stop_requested: bool,
    deadline_armed: bool,
    stop_effect_issued: bool,
    observer: Option<(ActorAddress, TransitionEmitter)>,
}

impl Coordinator {
    pub fn new(identity: ExecutionIdentity, attach_deadline: Option<std::time::Duration>) -> Self {
        Self {
            identity,
            attach_deadline,
            phase: Phase::Idle,
            native: NativePhase::Absent,
            session: SessionPhase::Absent,
            bootstrap: BootstrapPhase::Absent,
            context_resolution: None,
            terminal_outcome: None,
            bootstrap_claimed: false,
            stop_requested: false,
            deadline_armed: false,
            stop_effect_issued: false,
            observer: None,
        }
    }

    pub fn set_transitions(&mut self, actor: ActorAddress, observer: Option<TransitionEmitter>) {
        self.observer = observer.map(|observer| (actor, observer));
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    fn enter_phase(&mut self, next: Phase, reason: Reason, outcome: Option<Outcome>) {
        let previous = std::mem::replace(&mut self.phase, next);
        if previous == next {
            return;
        }
        if let Some((actor, observer)) = &self.observer {
            let mut operation = [0; 16];
            operation[..8].copy_from_slice(&self.identity.execution_id.to_le_bytes());
            operation[8..].copy_from_slice(&self.identity.generation.to_le_bytes());
            observer.emit(TransitionRecord {
                actor: *actor,
                actor_type: ActorType::ContextualProcessActor,
                protocol: Protocol::ContextualProcess,
                operation: Some(operation),
                generation: Some(self.identity.generation),
                incarnation: None,
                previous: previous.telemetry(),
                next: next.telemetry(),
                reason,
                outcome,
            });
        }
    }

    pub fn snapshot(&self) -> CoordinatorSnapshot {
        CoordinatorSnapshot {
            identity: self.identity,
            process_started: self.process_started(),
            process_terminal: self.process_terminal(),
            context_resolution: self.context_resolution.clone(),
            bootstrap_claimed: self.bootstrap_claimed,
            stop_requested: self.stop_requested,
            finished: self.phase == Phase::Closed,
        }
    }

    fn process_started(&self) -> bool {
        matches!(
            self.native,
            NativePhase::Running | NativePhase::Terminal { started: true }
        )
    }

    fn process_terminal(&self) -> bool {
        matches!(self.native, NativePhase::Terminal { .. })
    }

    pub fn apply(&mut self, event: Event) -> Vec<Effect> {
        if event.identity != self.identity || self.phase == Phase::Closed {
            return Vec::new();
        }
        let mut effects = Vec::new();
        match event.kind {
            EventKind::SpawnRequested => self.spawn_requested(&mut effects),
            EventKind::ProvisionSucceeded => self.provision_succeeded(&mut effects),
            EventKind::ProvisionFailed(reason) => self.provision_failed(reason, &mut effects),
            EventKind::Process(output) => self.process_output(output, &mut effects),
            EventKind::BootstrapClaimed {
                handle_execution_id,
            } => self.bootstrap_claimed(handle_execution_id, &mut effects),
            EventKind::BootstrapRejected(reason) => {
                self.attachment_failed(BootstrapFailure::ClaimRejected(reason), &mut effects)
            }
            EventKind::BootstrapClosed => {
                self.attachment_failed(BootstrapFailure::ChannelClosed, &mut effects)
            }
            EventKind::AttachmentSucceeded => self.attachment_succeeded(&mut effects),
            EventKind::AttachmentFailed(reason) => {
                self.attachment_failed(BootstrapFailure::Attachment(reason), &mut effects)
            }
            EventKind::AttachmentDeadline => {
                if self.deadline_armed {
                    self.attachment_failed(BootstrapFailure::AttachmentDeadline, &mut effects);
                }
            }
            EventKind::StopRequested => self.stop_requested(&mut effects),
            EventKind::ParentStopped => self.parent_stopped(&mut effects),
            EventKind::SessionFault(reason) => self.session_fault(reason, &mut effects),
            EventKind::CleanupCompleted => self.cleanup_completed(&mut effects),
        }
        effects
    }

    fn spawn_requested(&mut self, effects: &mut Vec<Effect>) {
        if self.phase != Phase::Idle {
            return;
        }
        self.enter_phase(Phase::Provisioning, Reason::Requested, None);
        effects.push(Effect::ProvisionSession);
    }

    fn provision_succeeded(&mut self, effects: &mut Vec<Effect>) {
        if self.phase != Phase::Provisioning {
            return;
        }
        self.session = SessionPhase::Active;
        self.bootstrap = BootstrapPhase::Available;
        if self.stop_requested {
            self.native = NativePhase::Terminal { started: false };
            effects.push(Effect::Emit(ContextualProcessOutput::Process(
                ProcessOutput::SpawnFailed {
                    error: "contextual launch cancelled before native spawn".to_owned(),
                },
            )));
            self.request_terminal_cleanup(effects);
        } else {
            self.native = NativePhase::Spawning;
            self.enter_phase(Phase::Spawning, Reason::Completed, None);
            effects.push(Effect::SpawnNativeProcess);
        }
    }

    fn provision_failed(&mut self, reason: String, effects: &mut Vec<Effect>) {
        if self.phase != Phase::Provisioning {
            return;
        }
        self.context_resolution = Some(ContextResolution::Failed(BootstrapFailure::Provisioning(
            reason.clone(),
        )));
        self.native = NativePhase::Terminal { started: false };
        effects.push(Effect::Emit(ContextualProcessOutput::Process(
            ProcessOutput::SpawnFailed { error: reason },
        )));
        self.enter_phase(Phase::Closed, Reason::Failed, Some(Outcome::Failed));
        effects.push(Effect::Finish);
    }

    fn bootstrap_claimed(&mut self, execution_id: u64, effects: &mut Vec<Effect>) {
        if execution_id != self.identity.execution_id {
            effects.push(Effect::RejectBootstrap {
                reason: BootstrapFailure::ClaimRejected(
                    "bootstrap handle belongs to another execution".to_owned(),
                ),
            });
        } else if self.phase != Phase::Attaching || self.bootstrap != BootstrapPhase::Available {
            effects.push(Effect::RejectBootstrap {
                reason: BootstrapFailure::ClaimRejected(
                    "bootstrap handle was already claimed or is no longer active".to_owned(),
                ),
            });
        } else {
            self.bootstrap = BootstrapPhase::Claimed;
            self.bootstrap_claimed = true;
        }
    }

    fn attachment_succeeded(&mut self, effects: &mut Vec<Effect>) {
        if self.phase != Phase::Attaching || self.bootstrap != BootstrapPhase::Claimed {
            return;
        }
        self.context_resolution = Some(ContextResolution::Ready);
        self.cancel_deadline(effects);
        self.enter_phase(Phase::Running, Reason::Ready, None);
        effects.push(Effect::Emit(ContextualProcessOutput::ContextReady));
        self.close_bootstrap(effects);
    }

    fn attachment_failed(&mut self, reason: BootstrapFailure, effects: &mut Vec<Effect>) {
        if self.process_started() && !self.process_terminal() {
            self.fail_context(reason, true, effects);
        }
    }

    fn stop_requested(&mut self, effects: &mut Vec<Effect>) {
        if self.stop_requested || self.process_terminal() {
            return;
        }
        self.stop_requested = true;
        self.request_stop(effects);
        if self.process_started() {
            self.fail_context(BootstrapFailure::StopRequested, false, effects);
            self.enter_phase(Phase::Stopping, Reason::Cancelled, None);
        }
    }

    fn parent_stopped(&mut self, effects: &mut Vec<Effect>) {
        self.stop_requested(effects);
        self.terminal_outcome = Some(Outcome::Cancelled);
        if !self.process_terminal() {
            if self.process_started() {
                effects.push(Effect::Emit(ContextualProcessOutput::Process(
                    ProcessOutput::Error {
                        error: "contextual process owner stopped".to_owned(),
                    },
                )));
            } else {
                effects.push(Effect::Emit(ContextualProcessOutput::Process(
                    ProcessOutput::SpawnFailed {
                        error: "contextual process owner stopped before launch completed"
                            .to_owned(),
                    },
                )));
            }
            self.native = NativePhase::Terminal {
                started: self.process_started(),
            };
        }
        self.request_terminal_cleanup(effects);
        self.enter_phase(
            Phase::Closed,
            Reason::ParentStopped,
            Some(Outcome::Cancelled),
        );
    }

    fn session_fault(&mut self, reason: String, effects: &mut Vec<Effect>) {
        if self.session == SessionPhase::Absent {
            return;
        }
        self.terminal_outcome = Some(Outcome::Failed);
        if self.process_started() && self.context_resolution.is_none() {
            self.fail_context(BootstrapFailure::SessionFault(reason), true, effects);
        } else if self.process_started() && !self.process_terminal() {
            self.request_stop(effects);
            self.close_bootstrap(effects);
            self.revoke_session(effects);
            self.enter_phase(Phase::Stopping, Reason::Failed, None);
        }
    }

    fn cleanup_completed(&mut self, effects: &mut Vec<Effect>) {
        if self.phase != Phase::Releasing {
            return;
        }
        self.session = SessionPhase::Released;
        let outcome = self.terminal_outcome.unwrap_or(if self.stop_requested {
            Outcome::Cancelled
        } else {
            Outcome::Success
        });
        self.enter_phase(Phase::Closed, Reason::Completed, Some(outcome));
        effects.push(Effect::Finish);
    }

    fn process_output(&mut self, output: ProcessOutput, effects: &mut Vec<Effect>) {
        match output {
            ProcessOutput::Stdout(_) | ProcessOutput::Stderr(_) => {
                if matches!(self.native, NativePhase::Spawning | NativePhase::Running) {
                    effects.push(Effect::Emit(ContextualProcessOutput::Process(output)));
                }
            }
            ProcessOutput::Started { .. } => self.process_started_output(output, effects),
            ProcessOutput::SpawnFailed { .. } => {
                if self.native != NativePhase::Spawning {
                    return;
                }
                self.native = NativePhase::Terminal { started: false };
                self.terminal_outcome = Some(Outcome::Failed);
                effects.push(Effect::Emit(ContextualProcessOutput::Process(output)));
                self.request_terminal_cleanup(effects);
            }
            ProcessOutput::Exited { ref status } => {
                if self.native != NativePhase::Running {
                    return;
                }
                self.native = NativePhase::Terminal { started: true };
                self.fail_context(BootstrapFailure::ProcessExitedBeforeReady, false, effects);
                self.terminal_outcome.get_or_insert(
                    if matches!(status, swactor_process::ExitStatus::Code(0)) {
                        Outcome::Success
                    } else {
                        Outcome::Failed
                    },
                );
                effects.push(Effect::Emit(ContextualProcessOutput::Process(output)));
                self.request_terminal_cleanup(effects);
            }
            ProcessOutput::Error { ref error } => {
                if !matches!(self.native, NativePhase::Spawning | NativePhase::Running) {
                    return;
                }
                let started = self.process_started();
                self.native = NativePhase::Terminal { started };
                if started {
                    self.fail_context(
                        BootstrapFailure::ProcessErrorBeforeReady(error.clone()),
                        false,
                        effects,
                    );
                }
                self.terminal_outcome.get_or_insert(Outcome::Failed);
                effects.push(Effect::Emit(ContextualProcessOutput::Process(output)));
                self.request_terminal_cleanup(effects);
            }
        }
    }

    fn process_started_output(&mut self, output: ProcessOutput, effects: &mut Vec<Effect>) {
        if self.native != NativePhase::Spawning {
            return;
        }
        self.native = NativePhase::Running;
        effects.push(Effect::Emit(ContextualProcessOutput::Process(output)));
        if self.stop_requested {
            self.fail_context(BootstrapFailure::StopRequested, true, effects);
        } else {
            self.enter_phase(Phase::Attaching, Reason::Started, None);
            if let Some(deadline) = self.attach_deadline {
                self.deadline_armed = true;
                effects.push(Effect::ArmAttachmentDeadline(deadline));
            }
            effects.push(Effect::AcceptBootstrap);
        }
    }

    fn fail_context(
        &mut self,
        reason: BootstrapFailure,
        stop_native: bool,
        effects: &mut Vec<Effect>,
    ) {
        if self.context_resolution.is_some() {
            return;
        }
        let transition_reason = match reason {
            BootstrapFailure::StopRequested => Reason::Cancelled,
            BootstrapFailure::AttachmentDeadline => Reason::Timeout,
            _ => Reason::Failed,
        };
        self.terminal_outcome = Some(match reason {
            BootstrapFailure::StopRequested => Outcome::Cancelled,
            BootstrapFailure::AttachmentDeadline => Outcome::TimedOut,
            _ => Outcome::Failed,
        });
        self.context_resolution = Some(ContextResolution::Failed(reason.clone()));
        self.cancel_deadline(effects);
        effects.push(Effect::Emit(ContextualProcessOutput::BootstrapFailed {
            reason,
        }));
        self.close_bootstrap(effects);
        self.revoke_session(effects);
        if stop_native && !self.process_terminal() {
            self.request_stop(effects);
        }
        self.enter_phase(Phase::Stopping, transition_reason, None);
    }

    fn request_stop(&mut self, effects: &mut Vec<Effect>) {
        if !self.stop_effect_issued
            && matches!(self.native, NativePhase::Spawning | NativePhase::Running)
        {
            self.stop_effect_issued = true;
            effects.push(Effect::StopNativeProcess);
        }
    }

    fn cancel_deadline(&mut self, effects: &mut Vec<Effect>) {
        if std::mem::take(&mut self.deadline_armed) {
            effects.push(Effect::CancelAttachmentDeadline);
        }
    }

    fn close_bootstrap(&mut self, effects: &mut Vec<Effect>) {
        if matches!(
            self.bootstrap,
            BootstrapPhase::Available | BootstrapPhase::Claimed
        ) {
            self.bootstrap = BootstrapPhase::Closed;
            effects.push(Effect::CloseBootstrap);
        }
    }

    fn revoke_session(&mut self, effects: &mut Vec<Effect>) {
        if self.session == SessionPhase::Active {
            self.session = SessionPhase::Revoked;
            effects.push(Effect::RevokeSession);
        }
    }

    fn request_terminal_cleanup(&mut self, effects: &mut Vec<Effect>) {
        self.cancel_deadline(effects);
        self.close_bootstrap(effects);
        self.revoke_session(effects);
        match self.session {
            SessionPhase::Active | SessionPhase::Revoked => {
                self.session = SessionPhase::Releasing;
                self.enter_phase(Phase::Releasing, Reason::Completed, None);
                effects.push(Effect::ReleaseArena);
            }
            SessionPhase::Absent => {
                self.enter_phase(Phase::Closed, Reason::Completed, Some(Outcome::Cancelled));
                effects.push(Effect::Finish);
            }
            SessionPhase::Releasing | SessionPhase::Released => {}
        }
    }
}
