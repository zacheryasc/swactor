use swactor::actor::ActorAddress;

use crate::action::OutputStream;
use crate::types::{ExitStatus, ProcessError, PtySize, Signal};

/// Commands sent to a process actor.
#[derive(Debug, Clone)]
pub enum ProcessCommand {
    /// Write data to the process's stdin.
    WriteStdin { data: Vec<u8> },
    /// Send a signal to the process.
    SendSignal { signal: Signal },
    /// Resize the process's PTY.
    ResizePty { size: PtySize },
    /// Close the process's stdin pipe.
    CloseStdin,
    /// Request a graceful close of the process.
    Close,
    /// Subscribe to process notifications.
    Subscribe { address: ActorAddress },
    /// Unsubscribe from process notifications.
    Unsubscribe { address: ActorAddress },
    /// Internal: sent by the waker to trigger event draining.
    #[doc(hidden)]
    PollTick,
}

/// Notifications sent from a process actor to subscribers.
#[derive(Debug, Clone)]
pub enum ProcessNotification {
    /// The process started successfully.
    Started { process: ActorAddress },
    /// Output was received from the process.
    Output {
        process: ActorAddress,
        data: Vec<u8>,
        stream: OutputStream,
    },
    /// The process exited.
    Exited {
        process: ActorAddress,
        status: ExitStatus,
    },
    /// An error occurred.
    Error {
        process: ActorAddress,
        error: ProcessError,
    },
}
