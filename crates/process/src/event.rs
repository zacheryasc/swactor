use crate::types::{ExitStatus, PtySize, Signal};
use swactor::actor::ActorAddress;

/// Events that can be applied to a ProcessSession.
///
/// Some events come from the driver (Started, SpawnFailed, OutputReceived, etc.),
/// others come from the owning actor (WriteStdin, SendSignal, Subscribe, etc.).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessEvent {
    // --- Driver-sourced events ---
    /// The process spawned successfully.
    Started,
    /// The kill timeout fired (process didn't exit after SIGTERM).
    KillTimeout,
    /// The process failed to spawn.
    SpawnFailed { reason: String },
    /// Output received on stdout or stderr.
    OutputReceived { data: Vec<u8>, is_stderr: bool },
    /// The process exited.
    Exited { status: ExitStatus },
    /// Connection to the process was lost unexpectedly.
    ConnectionLost { reason: String },

    // --- Driver acknowledgement events ---
    /// Stdin bytes were successfully written.
    StdinWritten { byte_count: usize },
    /// A signal was delivered.
    SignalSent,
    /// The PTY was resized.
    PtyResized,

    // --- Actor-sourced events ---
    /// Write data to the process's stdin.
    WriteStdin { data: Vec<u8> },
    /// Send a signal to the process.
    SendSignal { signal: Signal },
    /// Resize the process's PTY.
    ResizePty { size: PtySize },
    /// Close the process's stdin.
    CloseStdin,
    /// Request a graceful close of the process.
    CloseRequested,
    /// Subscribe an actor to process notifications.
    Subscribe { address: ActorAddress },
    /// Unsubscribe an actor from process notifications.
    Unsubscribe { address: ActorAddress },
}

impl ProcessEvent {
    /// Human-readable name for error messages.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Started => "Started",
            Self::KillTimeout => "KillTimeout",
            Self::SpawnFailed { .. } => "SpawnFailed",
            Self::OutputReceived { .. } => "OutputReceived",
            Self::Exited { .. } => "Exited",
            Self::ConnectionLost { .. } => "ConnectionLost",
            Self::StdinWritten { .. } => "StdinWritten",
            Self::SignalSent => "SignalSent",
            Self::PtyResized => "PtyResized",
            Self::WriteStdin { .. } => "WriteStdin",
            Self::SendSignal { .. } => "SendSignal",
            Self::ResizePty { .. } => "ResizePty",
            Self::CloseStdin => "CloseStdin",
            Self::CloseRequested => "CloseRequested",
            Self::Subscribe { .. } => "Subscribe",
            Self::Unsubscribe { .. } => "Unsubscribe",
        }
    }
}
