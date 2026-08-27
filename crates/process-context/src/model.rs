use std::time::Duration;

use data_plane::path::SessionAccess;
use swactor_process::{ProcessOutput, ProcessSpec};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExecutionIdentity {
    pub execution_id: u64,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextualProcessSpec {
    pub process: ProcessSpec,
    pub access: SessionAccess,
    pub attach_deadline: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapFailure {
    Provisioning(String),
    StopRequested,
    ClaimRejected(String),
    ChannelClosed,
    Attachment(String),
    AttachmentDeadline,
    ProcessExitedBeforeReady,
    ProcessErrorBeforeReady(String),
    SessionFault(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContextualProcessOutput {
    Process(ProcessOutput),
    ContextReady,
    BootstrapFailed { reason: BootstrapFailure },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventKind {
    SpawnRequested,
    ProvisionSucceeded,
    ProvisionFailed(String),
    Process(ProcessOutput),
    BootstrapClaimed { handle_execution_id: u64 },
    BootstrapRejected(String),
    BootstrapClosed,
    AttachmentSucceeded,
    AttachmentFailed(String),
    AttachmentDeadline,
    StopRequested,
    SessionFault(String),
    CleanupCompleted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub identity: ExecutionIdentity,
    pub kind: EventKind,
}

impl Event {
    pub fn new(identity: ExecutionIdentity, kind: EventKind) -> Self {
        Self { identity, kind }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    ProvisionSession,
    SpawnNativeProcess,
    ArmAttachmentDeadline(Duration),
    CancelAttachmentDeadline,
    AcceptBootstrap,
    RejectBootstrap { reason: BootstrapFailure },
    CloseBootstrap,
    Emit(ContextualProcessOutput),
    StopNativeProcess,
    RevokeSession,
    ReleaseArena,
    Finish,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContextResolution {
    Ready,
    Failed(BootstrapFailure),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatorSnapshot {
    pub identity: ExecutionIdentity,
    pub process_started: bool,
    pub process_terminal: bool,
    pub context_resolution: Option<ContextResolution>,
    pub bootstrap_claimed: bool,
    pub stop_requested: bool,
    pub finished: bool,
}
