use std::io::Read;
use std::sync::{Arc, OnceLock};

use crate::event::ProcessEvent;
use crate::queue::EventQueue;
use crate::waker::ProcessWaker;

/// Read from a pipe in a loop, pushing events to the queue and waking the actor.
///
/// Runs in a background thread. Exits when the pipe reaches EOF or errors.
pub(crate) fn read_pipe(
    mut pipe: impl Read + Send + 'static,
    is_stderr: bool,
    queue: EventQueue,
    waker: Arc<OnceLock<ProcessWaker>>,
) {
    let mut buf = [0u8; 8192];
    loop {
        match pipe.read(&mut buf) {
            Ok(0) => break, // EOF
            Ok(n) => {
                queue.push(ProcessEvent::OutputReceived {
                    data: buf[..n].to_vec(),
                    is_stderr,
                });
                if let Some(w) = waker.get() {
                    w.wake();
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
}
