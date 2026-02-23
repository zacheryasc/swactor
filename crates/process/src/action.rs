use std::time::Duration;

use crate::types::{ExitStatus, ProcessError, ProcessSpec, PtySize, Signal};
use swactor::actor::ActorAddress;

/// Which output stream produced data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// Actions emitted by ProcessSession for the driver or actor layer to execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessAction {
    // --- Driver commands ---
    /// Spawn the process described by the spec.
    SpawnProcess { spec: ProcessSpec },
    /// Write bytes to the process's stdin.
    WriteStdin { data: Vec<u8> },
    /// Send a signal to the process.
    SendSignal { signal: Signal },
    /// Resize the process's PTY.
    ResizePty { size: PtySize },
    /// Close the process's stdin pipe.
    CloseStdin,
    /// Schedule a kill timeout that fires KillTimeout after the given duration.
    ScheduleKillTimeout { duration: Duration },

    // --- Subscriber notifications ---
    /// Notify subscribers that the process started.
    NotifyStarted { subscribers: Vec<ActorAddress> },
    /// Notify subscribers of output.
    NotifyOutput {
        subscribers: Vec<ActorAddress>,
        data: Vec<u8>,
        stream: OutputStream,
    },
    /// Notify subscribers that the process exited.
    NotifyExited {
        subscribers: Vec<ActorAddress>,
        status: ExitStatus,
    },
    /// Notify subscribers of an error.
    NotifyError {
        subscribers: Vec<ActorAddress>,
        error: ProcessError,
    },

    // --- Lifecycle ---
    /// The session is done; the owning actor should stop itself.
    SelfTerminate,
}
