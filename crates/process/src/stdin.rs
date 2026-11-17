use std::io::{self, Write};
use std::process::ChildStdin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use swactor_engine::{BlockingWorkSender, OwnedIoThread};
use tokio::sync::{mpsc, oneshot};

use crate::terminal::{TerminalInput, TerminalSize, TerminalWriter};

const MAX_WRITE: usize = 16 * 1024;

type Acknowledgment = oneshot::Sender<io::Result<()>>;

enum InputRequest {
    Write(Vec<u8>, Acknowledgment),
    Resize(TerminalSize, Acknowledgment),
    Close(Acknowledgment),
    Shutdown,
}

/// Ordered, bounded input for one managed process. This handle is not cloneable.
///
/// Dropping it delivers EOF after accepted writes, without cancelling the child.
pub struct ProcessStdin {
    sender: Option<mpsc::Sender<InputRequest>>,
    closed: Arc<AtomicBool>,
}

impl ProcessStdin {
    /// Write at most 16 KiB, acknowledging delivery to the OS pipe, not execution.
    /// Cancelling this future does not retract an already accepted write.
    pub async fn write_all(&mut self, bytes: Vec<u8>) -> io::Result<()> {
        if bytes.len() > MAX_WRITE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "process stdin writes must not exceed 16 KiB",
            ));
        }
        let sender = self
            .sender
            .as_ref()
            .filter(|sender| !sender.is_closed() && !self.closed.load(Ordering::Acquire))
            .ok_or_else(closed_error)?;
        if bytes.is_empty() {
            return Ok(());
        }
        let (acknowledge, acknowledgment) = oneshot::channel();
        sender
            .send(InputRequest::Write(bytes, acknowledge))
            .await
            .map_err(|_| closed_error())?;
        acknowledgment.await.unwrap_or_else(|_| Err(closed_error()))
    }

    pub(crate) async fn resize(&mut self, size: TerminalSize) -> io::Result<()> {
        let sender = self.sender.as_ref()
            .filter(|sender| !sender.is_closed() && !self.closed.load(Ordering::Acquire))
            .ok_or_else(closed_error)?;
        let (acknowledge, acknowledgment) = oneshot::channel();
        sender.send(InputRequest::Resize(size, acknowledge)).await.map_err(|_| closed_error())?;
        acknowledgment.await.unwrap_or_else(|_| Err(closed_error()))
    }

    /// Deliver EOF after prior accepted writes. Already unavailable input is closed.
    /// This operation is idempotent; later writes fail with `BrokenPipe`.
    pub async fn close(&mut self) -> io::Result<()> {
        let Some(sender) = self.sender.take() else {
            return Ok(());
        };
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let (acknowledge, acknowledgment) = oneshot::channel();
        if sender.send(InputRequest::Close(acknowledge)).await.is_ok() {
            let _ = acknowledgment.await;
        }
        Ok(())
    }
}

fn closed_error() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "process stdin is closed")
}

#[derive(Clone)]
struct InputControl {
    closed: Arc<AtomicBool>,
    sender: mpsc::WeakSender<InputRequest>,
}

impl InputControl {
    fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        // Never keep a strong sender: dropping the public handle must deliver EOF.
        // A full queue already wakes the writer, which checks closed before writing.
        if let Some(sender) = self.sender.upgrade() {
            let _ = sender.try_send(InputRequest::Shutdown);
        }
    }
}

pub(crate) struct ProcessInput {
    requests: mpsc::Receiver<InputRequest>,
    control: InputControl,
    pub(crate) terminal_size: Option<TerminalSize>,
}

pub(crate) fn process_input_channel() -> (ProcessStdin, ProcessInput) {
    let (sender, requests) = mpsc::channel(1);
    let closed = Arc::new(AtomicBool::new(false));
    let control = InputControl {
        closed: Arc::clone(&closed),
        sender: sender.downgrade(),
    };
    (
        ProcessStdin {
            sender: Some(sender),
            closed,
        },
        ProcessInput { requests, control, terminal_size: None },
    )
}

pub(crate) fn terminal_input_channel(size: TerminalSize) -> (TerminalInput, ProcessInput) {
    let (input, mut requests) = process_input_channel();
    requests.terminal_size = Some(size);
    (TerminalInput { input }, requests)
}

enum InputTarget {
    Pipe(ChildStdin),
    Terminal(TerminalWriter),
}

impl InputTarget {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self {
            Self::Pipe(pipe) => pipe.write_all(bytes),
            Self::Terminal(terminal) => terminal.write_all(bytes),
        }
    }

    fn resize(&mut self, size: TerminalSize) -> io::Result<()> {
        match self {
            Self::Terminal(terminal) => terminal.resize(size),
            Self::Pipe(_) => Err(io::Error::new(io::ErrorKind::Unsupported, "stdin is not a terminal")),
        }
    }
}

impl ProcessInput {
    pub(crate) fn spawn_writer(
        self,
        blocking: &BlockingWorkSender,
        stdin: ChildStdin,
    ) -> io::Result<ProcessInputWriter> {
        self.spawn_target(blocking, InputTarget::Pipe(stdin))
    }

    pub(crate) fn spawn_terminal_writer(
        self,
        blocking: &BlockingWorkSender,
        terminal: TerminalWriter,
    ) -> io::Result<ProcessInputWriter> {
        self.spawn_target(blocking, InputTarget::Terminal(terminal))
    }

    fn spawn_target(
        self,
        blocking: &BlockingWorkSender,
        target: InputTarget,
    ) -> io::Result<ProcessInputWriter> {
        #[cfg(unix)]
        let (target, terminal_cancel) = {
            let mut target = target;
            let cancel = match &mut target {
                InputTarget::Terminal(terminal) => Some(terminal.take_cancellation()),
                InputTarget::Pipe(_) => None,
            };
            (target, cancel)
        };
        let control = self.control.clone();
        let thread = blocking.spawn_owned_thread("process-input-writer", move || {
            self.write_requests(target);
        })?;
        Ok(ProcessInputWriter {
            control,
            thread: Some(thread),
            #[cfg(unix)]
            terminal_cancel,
        })
    }

    fn write_requests(mut self, mut stdin: InputTarget) {
        while !self.control.closed.load(Ordering::Acquire) {
            let Some(request) = self.requests.blocking_recv() else {
                break;
            };
            if self.control.closed.load(Ordering::Acquire) {
                break;
            }
            match request {
                InputRequest::Write(bytes, acknowledge) => {
                    // std::io::Write::write_all retries Interrupted without copying.
                    let result = stdin.write_all(&bytes).and_then(|()| {
                        if self.control.closed.load(Ordering::Acquire) {
                            Err(closed_error())
                        } else {
                            Ok(())
                        }
                    });
                    let failed = result.is_err();
                    if failed {
                        self.control.closed.store(true, Ordering::Release);
                    }
                    let _ = acknowledge.send(result);
                    if failed {
                        break;
                    }
                }
                InputRequest::Resize(size, acknowledge) => {
                    let _ = acknowledge.send(stdin.resize(size));
                }
                InputRequest::Close(acknowledge) => {
                    drop(stdin);
                    self.control.closed.store(true, Ordering::Release);
                    let _ = acknowledge.send(Ok(()));
                    return;
                }
                InputRequest::Shutdown => break,
            }
        }
        // Dropping the receiver settles queued acknowledgments and blocked senders.
    }
}

impl Drop for ProcessInput {
    fn drop(&mut self) {
        self.control.closed.store(true, Ordering::Release);
        self.requests.close();
    }
}

/// The child owner must kill/reap before dropping or joining this writer.
pub(crate) struct ProcessInputWriter {
    control: InputControl,
    thread: Option<OwnedIoThread<()>>,
    #[cfg(unix)]
    terminal_cancel: Option<std::os::fd::OwnedFd>,
}

impl ProcessInputWriter {
    /// Wake a saturated PTY writer before process teardown. Joining still
    /// belongs after kill/reap; ordinary pipe input is deliberately unchanged.
    #[cfg(unix)]
    pub(crate) fn shutdown_terminal(&mut self) {
        if self.terminal_cancel.is_some() {
            self.control.shutdown();
            // Closing this private socket end permanently wakes poll, without
            // relying on terminal hangup or space becoming available to write.
            drop(self.terminal_cancel.take());
        }
    }

    pub(crate) fn finish(&mut self) -> io::Result<()> {
        #[cfg(unix)]
        self.shutdown_terminal();
        if let Some(thread) = self.thread.take() {
            self.control.shutdown();
            thread
                .join()
                .map_err(|_| io::Error::other("process stdin writer panicked"))?;
        }
        Ok(())
    }
}

impl Drop for ProcessInputWriter {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}
