use std::sync::{Arc, OnceLock};

use crate::event::ProcessEvent;
use crate::queue::EventQueue;
use crate::types::ExitStatus;
use crate::waker::ProcessWaker;

/// Wait for a child process to exit, then push the appropriate event.
///
/// Runs in a background thread. Uses `libc::waitpid` for accurate exit status.
pub(crate) fn wait_for_exit(
    pid: u32,
    queue: EventQueue,
    waker: Arc<OnceLock<ProcessWaker>>,
) {
    let mut status: libc::c_int = 0;
    let ret = unsafe { libc::waitpid(pid as libc::pid_t, &mut status, 0) };

    let exit_status = if ret < 0 {
        ExitStatus::Unknown
    } else {
        decode_wait_status(status)
    };

    queue.push(ProcessEvent::Exited {
        status: exit_status,
    });
    if let Some(w) = waker.get() {
        w.wake();
    }
}

fn decode_wait_status(status: libc::c_int) -> ExitStatus {
    if libc::WIFEXITED(status) {
        ExitStatus::Code(libc::WEXITSTATUS(status))
    } else if libc::WIFSIGNALED(status) {
        ExitStatus::Signal(libc::WTERMSIG(status))
    } else {
        ExitStatus::Unknown
    }
}
