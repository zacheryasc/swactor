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
    ///
    /// `pid` is `Some(u32)` when the underlying driver knows the OS
    /// pid (real `LocalDriver`) and `None` when it doesn't
    /// (mock drivers, future SSH-tunnel-style drivers). Observability
    /// hooks read this to register the subprocess with the
    /// `SubprocessIntrospector` from
    /// `distribution::diagnostics`
    /// (`N3_OBSERVABILITY_UPGRADE_SPEC.md` §4).
    Started {
        process: ActorAddress,
        #[doc(hidden)]
        pid: Option<u32>,
    },
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
