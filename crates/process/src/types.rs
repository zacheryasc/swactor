use std::collections::HashMap;
use std::time::Duration;

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

