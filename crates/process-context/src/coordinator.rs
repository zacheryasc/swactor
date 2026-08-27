use swactor_process::ProcessOutput;

use crate::model::{
    BootstrapFailure, ContextResolution, ContextualProcessOutput, CoordinatorSnapshot, Effect,
    Event, EventKind, ExecutionIdentity,
};

pub struct Coordinator {
    identity: ExecutionIdentity,
    attach_deadline: std::time::Duration,
    spawn_requested: bool,
    provisioned: bool,
    native_spawn_requested: bool,
    process_started: bool,
    process_terminal: bool,
    context_resolution: Option<ContextResolution>,
    bootstrap_claimed: bool,
    stop_requested: bool,
    deadline_armed: bool,
    stop_effect_issued: bool,
    close_effect_issued: bool,
    revoke_effect_issued: bool,
    release_effect_issued: bool,
    cleanup_waiting: bool,
    finished: bool,
}

impl Coordinator {
    pub fn new(identity: ExecutionIdentity, attach_deadline: std::time::Duration) -> Self {
        Self {
            identity,
            attach_deadline,
            spawn_requested: false,
            provisioned: false,
            native_spawn_requested: false,
            process_started: false,
            process_terminal: false,
            context_resolution: None,
            bootstrap_claimed: false,
            stop_requested: false,
            deadline_armed: false,
            stop_effect_issued: false,
            close_effect_issued: false,
            revoke_effect_issued: false,
            release_effect_issued: false,
            cleanup_waiting: false,
            finished: false,
        }
    }

    pub fn snapshot(&self) -> CoordinatorSnapshot {
        CoordinatorSnapshot {
            identity: self.identity,
            process_started: self.process_started,
            process_terminal: self.process_terminal,
            context_resolution: self.context_resolution.clone(),
            bootstrap_claimed: self.bootstrap_claimed,
            stop_requested: self.stop_requested,
            finished: self.finished,
        }
    }

    pub fn apply(&mut self, event: Event) -> Vec<Effect> {
        if event.identity != self.identity || self.finished {
            return Vec::new();
        }

        let mut effects = Vec::new();
        match event.kind {
            EventKind::SpawnRequested => {
                if !self.spawn_requested {
                    self.spawn_requested = true;
                    effects.push(Effect::ProvisionSession);
                }
            }
            EventKind::ProvisionSucceeded => {
                if !self.spawn_requested || self.provisioned || self.native_spawn_requested {
                    return effects;
                }
                self.provisioned = true;
                if self.stop_requested {
                    self.request_terminal_cleanup(&mut effects);
                } else {
                    self.native_spawn_requested = true;
                    effects.push(Effect::SpawnNativeProcess);
                }
            }
            EventKind::ProvisionFailed(_reason) => {
                if self.provisioned || self.native_spawn_requested {
                    return effects;
                }
                self.finished = true;
                effects.push(Effect::Finish);
            }
            EventKind::Process(output) => self.apply_process_output(output, &mut effects),
            EventKind::BootstrapClaimed {
                handle_execution_id,
            } => {
                if handle_execution_id != self.identity.execution_id {
                    effects.push(Effect::RejectBootstrap {
                        reason: BootstrapFailure::ClaimRejected(
                            "bootstrap handle belongs to another execution".to_owned(),
                        ),
                    });
                } else if !self.process_started
                    || self.context_resolution.is_some()
                    || self.bootstrap_claimed
                {
                    effects.push(Effect::RejectBootstrap {
                        reason: BootstrapFailure::ClaimRejected(
                            "bootstrap handle was already claimed or is no longer active"
                                .to_owned(),
                        ),
                    });
                } else {
                    self.bootstrap_claimed = true;
                }
            }
            EventKind::BootstrapRejected(reason) => {
                if self.process_started {
                    self.fail_context(BootstrapFailure::ClaimRejected(reason), true, &mut effects);
                }
            }
            EventKind::BootstrapClosed => {
                if self.process_started {
                    self.fail_context(BootstrapFailure::ChannelClosed, true, &mut effects);
                }
            }
            EventKind::AttachmentSucceeded => {
                if self.process_started
                    && self.bootstrap_claimed
                    && self.context_resolution.is_none()
                {
                    self.resolve_ready(&mut effects);
                }
            }
            EventKind::AttachmentFailed(reason) => {
                if self.process_started {
                    self.fail_context(BootstrapFailure::Attachment(reason), true, &mut effects);
                }
            }
            EventKind::AttachmentDeadline => {
                if self.process_started {
                    self.fail_context(BootstrapFailure::AttachmentDeadline, true, &mut effects);
                }
            }
            EventKind::StopRequested => {
                if self.stop_requested || self.process_terminal {
                    return effects;
                }
                self.stop_requested = true;
                if self.native_spawn_requested {
                    self.request_stop(&mut effects);
                }
                if self.process_started {
                    self.fail_context(BootstrapFailure::StopRequested, false, &mut effects);
                }
            }
            EventKind::SessionFault(reason) => {
                if self.process_started && self.context_resolution.is_none() {
                    self.fail_context(BootstrapFailure::SessionFault(reason), true, &mut effects);
                } else if self.process_started && !self.process_terminal {
                    self.request_stop(&mut effects);
                    self.close_bootstrap(&mut effects);
                    self.revoke_session(&mut effects);
                }
            }
            EventKind::CleanupCompleted => {
                if self.cleanup_waiting {
                    self.cleanup_waiting = false;
                    self.finished = true;
                    effects.push(Effect::Finish);
                }
            }
        }
        effects
    }

    fn apply_process_output(&mut self, output: ProcessOutput, effects: &mut Vec<Effect>) {
        match output {
            ProcessOutput::Stdout(_) | ProcessOutput::Stderr(_) => {
                if self.native_spawn_requested && !self.process_terminal {
                    effects.push(Effect::Emit(ContextualProcessOutput::Process(output)));
                }
            }
            ProcessOutput::Started { .. } => {
                if !self.native_spawn_requested || self.process_started || self.process_terminal {
                    return;
                }
                self.process_started = true;
                effects.push(Effect::Emit(ContextualProcessOutput::Process(output)));
                if self.stop_requested {
                    self.fail_context(BootstrapFailure::StopRequested, true, effects);
                } else {
                    self.deadline_armed = true;
                    effects.push(Effect::ArmAttachmentDeadline(self.attach_deadline));
                    effects.push(Effect::AcceptBootstrap);
                }
            }
            ProcessOutput::SpawnFailed { .. } => {
                if !self.native_spawn_requested || self.process_started || self.process_terminal {
                    return;
                }
                self.process_terminal = true;
                effects.push(Effect::Emit(ContextualProcessOutput::Process(output)));
                self.request_terminal_cleanup(effects);
            }
            ProcessOutput::Exited { .. } => {
                if !self.process_started || self.process_terminal {
                    return;
                }
                self.process_terminal = true;
                if self.context_resolution.is_none() {
                    self.fail_context(BootstrapFailure::ProcessExitedBeforeReady, false, effects);
                }
                effects.push(Effect::Emit(ContextualProcessOutput::Process(output)));
                self.request_terminal_cleanup(effects);
            }
            ProcessOutput::Error { ref error } => {
                if !self.native_spawn_requested || self.process_terminal {
                    return;
                }
                self.process_terminal = true;
                if self.process_started && self.context_resolution.is_none() {
                    self.fail_context(
                        BootstrapFailure::ProcessErrorBeforeReady(error.clone()),
                        false,
                        effects,
                    );
                }
                effects.push(Effect::Emit(ContextualProcessOutput::Process(output)));
                self.request_terminal_cleanup(effects);
            }
        }
    }

    fn resolve_ready(&mut self, effects: &mut Vec<Effect>) {
        if self.context_resolution.is_some() {
            return;
        }
        self.context_resolution = Some(ContextResolution::Ready);
        self.cancel_deadline(effects);
        effects.push(Effect::Emit(ContextualProcessOutput::ContextReady));
        self.close_bootstrap(effects);
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
        self.context_resolution = Some(ContextResolution::Failed(reason.clone()));
        self.cancel_deadline(effects);
        effects.push(Effect::Emit(ContextualProcessOutput::BootstrapFailed {
            reason,
        }));
        self.close_bootstrap(effects);
        self.revoke_session(effects);
        if stop_native && !self.process_terminal {
            self.request_stop(effects);
        }
    }

    fn request_stop(&mut self, effects: &mut Vec<Effect>) {
        if !self.stop_effect_issued && self.native_spawn_requested && !self.process_terminal {
            self.stop_effect_issued = true;
            effects.push(Effect::StopNativeProcess);
        }
    }

    fn cancel_deadline(&mut self, effects: &mut Vec<Effect>) {
        if self.deadline_armed {
            self.deadline_armed = false;
            effects.push(Effect::CancelAttachmentDeadline);
        }
    }

    fn close_bootstrap(&mut self, effects: &mut Vec<Effect>) {
        if self.provisioned && !self.close_effect_issued {
            self.close_effect_issued = true;
            effects.push(Effect::CloseBootstrap);
        }
    }

    fn revoke_session(&mut self, effects: &mut Vec<Effect>) {
        if self.provisioned && !self.revoke_effect_issued {
            self.revoke_effect_issued = true;
            effects.push(Effect::RevokeSession);
        }
    }

    fn request_terminal_cleanup(&mut self, effects: &mut Vec<Effect>) {
        self.cancel_deadline(effects);
        self.close_bootstrap(effects);
        self.revoke_session(effects);
        if self.provisioned && !self.release_effect_issued {
            self.release_effect_issued = true;
            self.cleanup_waiting = true;
            effects.push(Effect::ReleaseArena);
        } else if !self.provisioned {
            self.finished = true;
            effects.push(Effect::Finish);
        }
    }
}
