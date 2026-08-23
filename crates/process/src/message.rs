use crate::types::ExitStatus;

/// Public managed-process commands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessCommand {
    Stop {
        kill_after: Option<std::time::Duration>,
    },
}

/// Public managed-process outputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessOutput {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Started { pid: u32 },
    SpawnFailed { error: String },
    Exited { status: ExitStatus },
    Error { error: String },
}

#[derive(Debug, Clone)]
pub(crate) enum ProcessActorCommand {
    Command(ProcessCommand),
    SupervisorWake,
}
