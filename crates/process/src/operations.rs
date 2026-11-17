//! Concrete operating-system process mechanics owned by `swactor-process`.
//!
//! Domain crates may choose a command or decide to stop a resource, but the
//! actual process creation, waiting, and signaling stays behind this substrate
//! boundary.

#[cfg(target_os = "linux")]
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
#[cfg(target_os = "linux")]
use std::io::Write;
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Output};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
#[cfg(target_os = "linux")]
use std::time::Instant;

use swactor::actor::ActorAddress;
use swactor::runtime::ExternalSender;
use swactor_engine::{BlockingWorkHandle, BlockingWorkSender, OwnedIoThread};

pub fn command_output(command: &mut Command) -> io::Result<Output> {
    command.output()
}

/// Capture a command's exact output within an absolute owner deadline.
///
/// Uses the same process-group, parent-death and joined-I/O ownership as
/// [`SupervisedChild`], without requiring an actor mailbox to make progress.
/// Timeout kills and reaps the owned child before returning.
#[cfg(target_os = "linux")]
pub fn command_output_until(
    blocking: &BlockingWorkSender,
    command: &mut Command,
    deadline: Instant,
) -> io::Result<Output> {
    let output = Arc::new(Mutex::new(CapturedCommandOutput::default()));
    let (finished, completion) = std::sync::mpsc::channel();
    let owner = SupervisedChild::spawn_observed(
        blocking,
        command.stdin(std::process::Stdio::null()),
        None,
        Some(deadline),
        SupervisedOutputObserver::Capture {
            output: Arc::clone(&output),
            finished,
        },
    )?;
    let completed = completion.recv_timeout(deadline.saturating_duration_since(Instant::now()));
    // On timeout this cancels the same owner; on completion it joins the
    // waiter that has already reaped the child and joined both output readers.
    drop(owner);
    let status = match completed {
        Ok(result) => result.map_err(|error| {
            io::Error::new(
                if Instant::now() >= deadline {
                    io::ErrorKind::TimedOut
                } else {
                    io::ErrorKind::Other
                },
                error,
            )
        })?,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "process owner deadline expired; pending command output/exit",
            ));
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            return Err(io::Error::other(
                "process owner finished without an exit result",
            ));
        }
    };
    let mut output = output
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(error) = output.error.take() {
        return Err(error);
    }
    Ok(Output {
        status,
        stdout: std::mem::take(&mut output.stdout),
        stderr: std::mem::take(&mut output.stderr),
    })
}

/// Capture an owned command until its actual exit and both output streams close.
/// Unwinding cancels the process group and joins all command work.
#[cfg(target_os = "linux")]
pub fn command_output_owned(
    blocking: &BlockingWorkSender,
    command: &mut Command,
) -> io::Result<Output> {
    let output = Arc::new(Mutex::new(CapturedCommandOutput::default()));
    let (finished, completion) = std::sync::mpsc::channel();
    let owner = SupervisedChild::spawn_observed(
        blocking,
        command.stdin(std::process::Stdio::null()),
        None,
        None,
        SupervisedOutputObserver::Capture {
            output: Arc::clone(&output),
            finished,
        },
    )?;
    let completed = completion.recv();
    drop(owner);
    let status = completed
        .map_err(|_| io::Error::other("process owner finished without an exit result"))?
        .map_err(io::Error::other)?;
    let mut output = output
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(error) = output.error.take() {
        return Err(error);
    }
    Ok(Output {
        status,
        stdout: std::mem::take(&mut output.stdout),
        stderr: std::mem::take(&mut output.stderr),
    })
}

/// Stream a command's output while retaining joined process-group ownership.
/// The callback runs on the calling thread; its panic cancels the command.
#[cfg(target_os = "linux")]
pub fn command_status_observed(
    blocking: &BlockingWorkSender,
    command: &mut Command,
    mut observe: impl FnMut(ProcessStreamObservation),
) -> io::Result<ExitStatus> {
    let (events, observations) = std::sync::mpsc::channel();
    let owner = SupervisedChild::spawn_observed(
        blocking,
        command,
        None,
        None,
        SupervisedOutputObserver::Channel { events },
    )?;
    let result = loop {
        match observations.recv() {
            Ok(CommandObservation::Stream(observation)) => {
                let error = match &observation {
                    ProcessStreamObservation::Error { error, .. } => Some(error.clone()),
                    _ => None,
                };
                observe(observation);
                if let Some(error) = error {
                    break Err(io::Error::other(error));
                }
            }
            Ok(CommandObservation::Exited(result)) => break result.map_err(io::Error::other),
            Err(_) => {
                break Err(io::Error::other(
                    "process owner finished without an exit result",
                ));
            }
        }
    };
    drop(owner);
    result
}

pub fn command_status(command: &mut Command) -> io::Result<ExitStatus> {
    command.status()
}

pub fn command_spawn(command: &mut Command) -> io::Result<Child> {
    command.spawn()
}

pub fn child_kill(child: &mut Child) -> io::Result<()> {
    child.kill()
}

/// Ask a child process to shut down through its ordinary SIGTERM path.
///
/// Unlike [`child_kill`], this gives the child an opportunity to flush durable
/// state and release owned resources before exiting.
#[cfg(unix)]
pub fn request_child_termination(child: &Child) -> io::Result<()> {
    if unsafe { libc::kill(child.id() as i32, libc::SIGTERM) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

pub fn child_wait(child: &mut Child) -> io::Result<ExitStatus> {
    child.wait()
}

pub fn child_try_wait(child: &mut Child) -> io::Result<Option<ExitStatus>> {
    child.try_wait()
}

pub fn child_wait_with_output(child: Child) -> io::Result<Output> {
    child.wait_with_output()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProcessStreamObservation {
    Line {
        stream: ProcessStream,
        line: String,
    },
    Error {
        stream: ProcessStream,
        error: String,
    },
    Closed {
        stream: ProcessStream,
    },
}

pub struct LineReaderHandle {
    join: Option<BlockingWorkHandle<()>>,
}

impl LineReaderHandle {
    pub fn join(self) {
        if let Some(join) = self.join {
            let _ = join.join();
        }
    }
}

fn read_process_lines<R: Read>(
    stream: ProcessStream,
    reader: R,
    mut observe: impl FnMut(ProcessStreamObservation) -> bool,
) {
    for next in BufReader::new(reader).lines() {
        let observation = match next {
            Ok(line) => ProcessStreamObservation::Line { stream, line },
            Err(error) => {
                observe(ProcessStreamObservation::Error {
                    stream,
                    error: error.to_string(),
                });
                break;
            }
        };
        if !observe(observation) {
            return;
        }
    }
    observe(ProcessStreamObservation::Closed { stream });
}

/// Read one child stream and deliver typed observations to an actor relay.
pub fn spawn_line_reader<R>(
    blocking: &BlockingWorkSender,
    stream: ProcessStream,
    reader: R,
    sender: ExternalSender,
    actor: ActorAddress,
) -> LineReaderHandle
where
    R: Read + Send + 'static,
{
    let observer = sender.clone();
    let join = match blocking.submit_joinable(move || {
        read_process_lines(stream, reader, |observation| {
            observer.send_to(actor, observation).is_ok()
        });
    }) {
        Ok(join) => Some(join),
        Err(error) => {
            let _ = sender.send_to(
                actor,
                ProcessStreamObservation::Error {
                    stream,
                    error: error.to_string(),
                },
            );
            let _ = sender.send_to(actor, ProcessStreamObservation::Closed { stream });
            None
        }
    };
    LineReaderHandle { join }
}

/// Read a stream under an explicit resource owner, independently of pool work.
/// The owner must make the reader reach EOF or cancel it before joining.
pub fn spawn_owned_line_reader<R>(
    blocking: &BlockingWorkSender,
    stream: ProcessStream,
    reader: R,
    sender: ExternalSender,
    actor: ActorAddress,
) -> io::Result<OwnedIoThread<()>>
where
    R: Read + Send + 'static,
{
    blocking.spawn_owned_thread("process-owned-line-reader", move || {
        read_process_lines(stream, reader, |observation| {
            sender.send_to(actor, observation).is_ok()
        });
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessStopSignal;

/// Wait for an embedding process-control channel and notify an actor once.
pub fn spawn_stop_channel_wait(
    blocking: &BlockingWorkSender,
    receiver: Receiver<()>,
    sender: ExternalSender,
    actor: ActorAddress,
) {
    let _ = blocking.submit(Box::new(move || {
        if receiver.recv().is_ok() {
            let _ = sender.send_to(actor, ProcessStopSignal);
        }
    }));
}

/// Wait for SIGINT/SIGTERM and notify an actor once.
///
/// Signal handlers are installed before this function returns. A caller may
/// therefore publish readiness immediately after a successful return without
/// racing the operating system's default signal action.
#[cfg(target_os = "linux")]
pub fn spawn_os_stop_signal_wait(
    blocking: &BlockingWorkSender,
    sender: ExternalSender,
    actor: ActorAddress,
) -> io::Result<()> {
    let mut signals = signal_hook::iterator::Signals::new([
        signal_hook::consts::signal::SIGINT,
        signal_hook::consts::signal::SIGTERM,
    ])?;
    blocking
        .submit(Box::new(move || {
            if signals.forever().next().is_some() {
                let _ = sender.send_to(actor, ProcessStopSignal);
            }
        }))
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "process signal watcher engine stopped",
            )
        })?;
    Ok(())
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub environment: Vec<(String, String)>,
    pub process_group_leader: bool,
}

#[cfg(target_os = "linux")]
impl ProcessIdentity {
    pub fn matches(&self) -> bool {
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{}/stat", self.pid)) else {
            return false;
        };
        let Some((_, tail)) = stat.rsplit_once(')') else {
            return false;
        };
        let mut fields = tail.split_whitespace();
        let state = fields.next();
        let _parent_pid = fields.next();
        let process_group = fields.next().and_then(|value| value.parse::<u32>().ok());
        if state == Some("Z") || self.process_group_leader && process_group != Some(self.pid) {
            return false;
        }
        let Ok(environ) = std::fs::read(format!("/proc/{}/environ", self.pid)) else {
            return false;
        };
        self.environment.iter().all(|(key, value)| {
            environ.split(|byte| *byte == 0).any(|entry| {
                entry
                    .strip_prefix(format!("{key}=").as_bytes())
                    .is_some_and(|actual| actual == value.as_bytes())
            })
        })
    }
}

#[cfg(target_os = "linux")]
pub struct FollowProcessFile {
    file: File,
    identity: ProcessIdentity,
    poll_interval: Duration,
}

#[cfg(target_os = "linux")]
impl FollowProcessFile {
    pub fn new(file: File, identity: ProcessIdentity, poll_interval: Duration) -> Self {
        Self {
            file,
            identity,
            poll_interval,
        }
    }
}

#[cfg(target_os = "linux")]
impl Read for FollowProcessFile {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        loop {
            let count = self.file.read(buffer)?;
            if count > 0 || !self.identity.matches() {
                return Ok(count);
            }
            thread::sleep(self.poll_interval);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessExitObservation {
    pub status: Option<i32>,
    pub error: Option<String>,
}

#[cfg(target_os = "linux")]
fn process_exit_fd(pid: u32) -> io::Result<OwnedFd> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) as i32 };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

#[cfg(target_os = "linux")]
fn wait_process_fds<const N: usize>(
    fds: &[Option<&OwnedFd>; N],
    timeout: Duration,
) -> io::Result<[bool; N]> {
    let mut descriptors: [libc::pollfd; N] = std::array::from_fn(|index| libc::pollfd {
        fd: fds[index].map_or(-1, AsRawFd::as_raw_fd),
        events: libc::POLLIN,
        revents: 0,
    });
    let millis = timeout.as_millis().min(i32::MAX as u128) as i32;
    let result = unsafe {
        libc::poll(
            descriptors.as_mut_ptr(),
            descriptors.len() as libc::nfds_t,
            millis,
        )
    };
    if result < 0 {
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
    Ok(std::array::from_fn(|index| descriptors[index].revents != 0))
}

/// Wait for descriptor readiness, closure, or a descriptor error.
///
/// This operation has no clock or wake cadence. Owners must consume the
/// readiness event (or finish the operation) before waiting on the same
/// level-triggered descriptor again.
#[cfg(target_os = "linux")]
pub fn wait_readable<const N: usize>(fds: &[Option<&OwnedFd>; N]) -> io::Result<[bool; N]> {
    if fds.iter().all(Option::is_none) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no readiness descriptors",
        ));
    }
    let mut descriptors: [libc::pollfd; N] = std::array::from_fn(|index| libc::pollfd {
        fd: fds[index].map_or(-1, AsRawFd::as_raw_fd),
        events: libc::POLLIN,
        revents: 0,
    });
    loop {
        let result = unsafe {
            libc::poll(
                descriptors.as_mut_ptr(),
                descriptors.len() as libc::nfds_t,
                -1,
            )
        };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if descriptors
            .iter()
            .any(|fd| fd.revents & (libc::POLLERR | libc::POLLNVAL) != 0)
        {
            return Err(io::Error::other("process readiness descriptor failed"));
        }
        return Ok(std::array::from_fn(|index| descriptors[index].revents != 0));
    }
}

/// A cancellable exit subscription. Dropping it joins its observer without
/// stopping the process, so adopted workers may outlive their observation.
#[cfg(target_os = "linux")]
pub struct ProcessWatch {
    cancel: Arc<OwnedFd>,
    thread: Option<OwnedIoThread<()>>,
}

#[cfg(target_os = "linux")]
impl Drop for ProcessWatch {
    fn drop(&mut self) {
        let value = 1_u64;
        loop {
            let result = unsafe {
                libc::write(
                    self.cancel.as_raw_fd(),
                    (&value as *const u64).cast(),
                    std::mem::size_of::<u64>(),
                )
            };
            if result >= 0 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                break;
            }
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(target_os = "linux")]
pub fn watch_process(
    blocking: &BlockingWorkSender,
    identity: ProcessIdentity,
    child: Option<Arc<Mutex<Option<Child>>>>,
    sender: ExternalSender,
    actor: ActorAddress,
) -> io::Result<ProcessWatch> {
    let exit = match process_exit_fd(identity.pid) {
        Ok(exit) => Some(exit),
        Err(_) if child.is_none() && !identity.matches() => None,
        Err(error) => return Err(error),
    };
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let cancel = Arc::new(unsafe { OwnedFd::from_raw_fd(fd) });
    let cancelled = Arc::clone(&cancel);
    let thread = blocking.spawn_owned_thread("process-exit-observer", move || {
        let observation = (|| -> io::Result<Option<i32>> {
            if child.is_none() && !identity.matches() {
                return Ok(None);
            }
            let Some(exit) = exit else {
                return Ok(None);
            };
            if wait_readable(&[Some(&exit), Some(&cancelled)])?[1] {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "process observation cancelled",
                ));
            }
            let Some(child) = &child else {
                return Ok(None);
            };
            let mut slot = child
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(child) = slot.as_mut() else {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "process already reaped by owner",
                ));
            };
            terminate_owned_child(child);
            let status = child.wait()?;
            *slot = None;
            Ok(status.code())
        })();
        let (status, error) = match observation {
            Ok(status) => (status, None),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => return,
            Err(error) => (None, Some(error.to_string())),
        };
        let _ = sender.send_to(actor, ProcessExitObservation { status, error });
    })?;
    Ok(ProcessWatch {
        cancel,
        thread: Some(thread),
    })
}

/// Deliver a graceful shutdown request, then wait for the owned child to exit.
/// A concurrent `cancel_shared_child` remains able to force cancellation.
#[cfg(target_os = "linux")]
pub fn stop_shared_child_with_input(
    child: &Arc<Mutex<Option<Child>>>,
    stdin: &mut std::process::ChildStdin,
    input: &[u8],
) -> io::Result<Option<ExitStatus>> {
    let exit = {
        let mut slot = child
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(child) = slot.as_mut() else {
            return Ok(None);
        };
        process_exit_fd(child.id())?
    };
    // No child mutex is held while writing or awaiting completion. Cancellation
    // can close the group's pipe readers and wake both operations.
    match stdin.write_all(input).and_then(|()| stdin.flush()) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {}
        Err(error) => return Err(error),
    }
    wait_readable(&[Some(&exit)])?;
    let mut slot = child
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(child) = slot.as_mut() else {
        return Ok(None);
    };
    // The leader has completed gracefully. Dispose of any remaining group
    // members before reaping releases its process-group identity.
    terminate_owned_child(child);
    let status = child.wait()?;
    *slot = None;
    Ok(Some(status))
}

/// Force-cancel a shared owned child, reap it, and wake graceful waiters.
#[cfg(target_os = "linux")]
pub fn cancel_shared_child(child: &Arc<Mutex<Option<Child>>>) -> io::Result<Option<ExitStatus>> {
    let mut slot = child
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(child) = slot.as_mut() else {
        return Ok(None);
    };
    terminate_owned_child(child);
    let status = child.wait()?;
    *slot = None;
    Ok(Some(status))
}

#[cfg(target_os = "linux")]
fn terminate_owned_child(child: &mut Child) {
    // The unreaped leader pins the process-group number against PID reuse.
    unsafe {
        libc::kill(-(child.id() as i32), libc::SIGKILL);
    }
    let _ = child.kill();
}

#[derive(Clone, Debug)]
pub enum SupervisedProcessObservation {
    Stream(ProcessStreamObservation),
    Exited {
        operation: u64,
        result: Result<ExitStatus, String>,
    },
}

/// Owns an entire command attempt: child, input, incremental output and waiter.
/// Completion and cancellation both reap the group and join every I/O thread.
#[cfg(target_os = "linux")]
pub struct SupervisedChild {
    child: Arc<Mutex<Option<Child>>>,
    waiter: Option<OwnedIoThread<()>>,
}

#[cfg(target_os = "linux")]
#[derive(Default)]
struct CapturedCommandOutput {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    error: Option<io::Error>,
}

#[cfg(target_os = "linux")]
enum CommandObservation {
    Stream(ProcessStreamObservation),
    Exited(Result<ExitStatus, String>),
}

#[cfg(target_os = "linux")]
#[derive(Clone)]
enum SupervisedOutputObserver {
    Actor {
        sender: ExternalSender,
        actor: ActorAddress,
        operation: u64,
    },
    Capture {
        output: Arc<Mutex<CapturedCommandOutput>>,
        finished: std::sync::mpsc::Sender<Result<ExitStatus, String>>,
    },
    Channel {
        events: std::sync::mpsc::Sender<CommandObservation>,
    },
}

#[cfg(target_os = "linux")]
impl SupervisedOutputObserver {
    fn close_stream(&self, stream: ProcessStream) {
        let observation = ProcessStreamObservation::Closed { stream };
        match self {
            Self::Actor { sender, actor, .. } => {
                let _ = sender.send_to(*actor, SupervisedProcessObservation::Stream(observation));
            }
            Self::Channel { events } => {
                let _ = events.send(CommandObservation::Stream(observation));
            }
            Self::Capture { .. } => {}
        }
    }

    fn exited(&self, result: Result<ExitStatus, String>) {
        match self {
            Self::Actor {
                sender,
                actor,
                operation,
            } => {
                let _ = sender.send_to(
                    *actor,
                    SupervisedProcessObservation::Exited {
                        operation: *operation,
                        result,
                    },
                );
            }
            Self::Capture { finished, .. } => {
                let _ = finished.send(result);
            }
            Self::Channel { events } => {
                let _ = events.send(CommandObservation::Exited(result));
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn spawn_supervised_reader<R: Read + Send + 'static>(
    blocking: &BlockingWorkSender,
    stream: ProcessStream,
    mut reader: R,
    observer: SupervisedOutputObserver,
) -> io::Result<OwnedIoThread<()>> {
    blocking.spawn_owned_thread("process-output-reader", move || match observer {
        SupervisedOutputObserver::Actor { sender, actor, .. } => {
            read_process_lines(stream, reader, |observation| {
                sender
                    .send_to(actor, SupervisedProcessObservation::Stream(observation))
                    .is_ok()
            });
        }
        SupervisedOutputObserver::Capture { output, .. } => {
            let mut bytes = Vec::new();
            let result = reader.read_to_end(&mut bytes);
            let mut output = output
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match stream {
                ProcessStream::Stdout => output.stdout = bytes,
                ProcessStream::Stderr => output.stderr = bytes,
            }
            if let Err(error) = result {
                output.error = Some(error);
            }
        }
        SupervisedOutputObserver::Channel { events } => {
            read_process_lines(stream, reader, |observation| {
                events.send(CommandObservation::Stream(observation)).is_ok()
            });
        }
    })
}

#[cfg(target_os = "linux")]
impl SupervisedChild {
    pub fn spawn(
        blocking: &BlockingWorkSender,
        command: &mut Command,
        input: Option<Arc<[u8]>>,
        sender: ExternalSender,
        actor: ActorAddress,
        operation: u64,
    ) -> io::Result<Self> {
        Self::spawn_observed(
            blocking,
            command,
            input,
            None,
            SupervisedOutputObserver::Actor {
                sender,
                actor,
                operation,
            },
        )
    }

    fn spawn_observed(
        blocking: &BlockingWorkSender,
        command: &mut Command,
        input: Option<Arc<[u8]>>,
        deadline: Option<Instant>,
        observer: SupervisedOutputObserver,
    ) -> io::Result<Self> {
        if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "process owner deadline expired",
            ));
        }
        command.process_group(0);
        let parent = unsafe { libc::getpid() };
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() != parent {
                    return Err(io::Error::other("process owner exited during spawn"));
                }
                Ok(())
            });
        }
        command
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if input.is_some() {
            command.stdin(std::process::Stdio::piped());
        }
        let mut child = command.spawn()?;
        let exit = match process_exit_fd(child.id()) {
            Ok(exit) => exit,
            Err(error) => {
                terminate_owned_child(&mut child);
                let _ = child.wait();
                return Err(error);
            }
        };
        let stdout = child
            .stdout
            .take()
            .expect("supervisor configures piped stdout");
        let stderr = child
            .stderr
            .take()
            .expect("supervisor configures piped stderr");
        let stdin = input.map(|bytes| {
            (
                child
                    .stdin
                    .take()
                    .expect("supervisor configures piped stdin"),
                bytes,
            )
        });
        let child = Arc::new(Mutex::new(Some(child)));
        let owned = Arc::clone(&child);
        let io_work = blocking.clone();
        let waiter = match blocking.spawn_owned_thread("process-owner", move || {
            let mut setup_error = None;
            let readers = [
                (
                    ProcessStream::Stdout,
                    spawn_supervised_reader(
                        &io_work,
                        ProcessStream::Stdout,
                        stdout,
                        observer.clone(),
                    ),
                ),
                (
                    ProcessStream::Stderr,
                    spawn_supervised_reader(
                        &io_work,
                        ProcessStream::Stderr,
                        stderr,
                        observer.clone(),
                    ),
                ),
            ]
            .map(|(stream, reader)| {
                (
                    stream,
                    match reader {
                        Ok(reader) => Some(reader),
                        Err(error) => {
                            setup_error = Some(error.to_string());
                            observer.close_stream(stream);
                            None
                        }
                    },
                )
            });
            let writer = stdin.and_then(|(mut stdin, bytes)| {
                match io_work.spawn_owned_thread("process-input-writer", move || {
                    stdin.write_all(&bytes).and_then(|()| stdin.flush())
                }) {
                    Ok(writer) => Some(writer),
                    Err(error) => {
                        setup_error = Some(error.to_string());
                        None
                    }
                }
            });
            let waited = if let Some(error) = setup_error {
                Err(error)
            } else if let Some(deadline) = deadline {
                loop {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        break Err("process owner deadline expired; pending child exit".to_owned());
                    }
                    match wait_process_fds(&[Some(&exit)], left) {
                        Ok([true]) => break Ok(()),
                        Ok([false]) => {}
                        Err(error) => break Err(error.to_string()),
                    }
                }
            } else {
                wait_readable(&[Some(&exit)])
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            };
            let status = {
                let mut slot = owned
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                slot.take().map(|mut child| {
                    terminate_owned_child(&mut child);
                    child.wait().map_err(|error| error.to_string())
                })
            };
            let input_result = writer
                .map(|writer| {
                    writer
                        .join()
                        .map_err(|_| "process stdin writer panicked".to_owned())?
                        .map_err(|error| format!("write process stdin: {error}"))
                })
                .unwrap_or(Ok(()));
            let mut reader_error = None;
            for (stream, reader) in readers {
                if reader.is_some_and(|reader| reader.join().is_err()) {
                    reader_error = Some("process output reader panicked".to_owned());
                    observer.close_stream(stream);
                }
            }
            if let Some(status) = status {
                let result = waited.and(status).and_then(|status| {
                    if let Some(error) = reader_error {
                        return Err(error);
                    }
                    if status.success() {
                        input_result.map(|()| status)
                    } else {
                        Ok(status)
                    }
                });
                observer.exited(result);
            }
        }) {
            Ok(waiter) => waiter,
            Err(error) => {
                if let Some(mut child) = child
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                {
                    terminate_owned_child(&mut child);
                    let _ = child.wait();
                }
                return Err(error);
            }
        };
        Ok(Self {
            child,
            waiter: Some(waiter),
        })
    }
}

#[cfg(target_os = "linux")]
impl Drop for SupervisedChild {
    fn drop(&mut self) {
        let mut slot = self
            .child
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(child) = slot.as_mut() {
            terminate_owned_child(child);
        }
        drop(slot);
        if let Some(waiter) = self.waiter.take() {
            let _ = waiter.join();
        }
        // Recover ownership even if the waiter panicked before it could reap.
        if let Some(mut child) = self
            .child
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            terminate_owned_child(&mut child);
            let _ = child.wait();
        }
    }
}

pub fn spawn_shared_child_wait(
    blocking: &BlockingWorkSender,
    child: Arc<Mutex<Option<Child>>>,
    poll_interval: Duration,
    sender: ExternalSender,
    actor: ActorAddress,
) -> Result<(), Arc<Mutex<Option<Child>>>> {
    blocking
        .submit_owned((child, sender), move |(child, sender)| {
            loop {
                let observation = {
                    let mut slot = child
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let Some(child) = slot.as_mut() else {
                        return;
                    };
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            *slot = None;
                            Some(ProcessExitObservation {
                                status: status.code(),
                                error: None,
                            })
                        }
                        Ok(None) => None,
                        Err(error) => Some(ProcessExitObservation {
                            status: None,
                            error: Some(error.to_string()),
                        }),
                    }
                };
                if let Some(observation) = observation {
                    let _ = sender.send_to(actor, observation);
                    return;
                }
                thread::sleep(poll_interval);
            }
        })
        .map_err(|(child, sender)| {
            let _ = sender.send_to(
                actor,
                ProcessExitObservation {
                    status: None,
                    error: Some("process wait engine stopped".to_owned()),
                },
            );
            child
        })
}

/// Request graceful process-group termination and await the leader's exit.
#[cfg(target_os = "linux")]
pub fn terminate_process_group(identity: &ProcessIdentity) -> Result<(), String> {
    signal_process_group_and_wait(identity, libc::SIGTERM)
}

/// Explicitly cancel a process group and await its leader's exit.
#[cfg(target_os = "linux")]
pub fn cancel_process_group(identity: &ProcessIdentity) -> Result<(), String> {
    signal_process_group_and_wait(identity, libc::SIGKILL)
}

#[cfg(target_os = "linux")]
fn signal_process_group_and_wait(identity: &ProcessIdentity, signal: i32) -> Result<(), String> {
    if !identity.matches() {
        return Ok(());
    }
    let exit = match process_exit_fd(identity.pid) {
        Ok(exit) => exit,
        Err(_) if !identity.matches() => return Ok(()),
        Err(error) => return Err(format!("observe process group {}: {error}", identity.pid)),
    };
    if !identity.matches() {
        return Ok(());
    }
    if unsafe { libc::kill(-(identity.pid as i32), signal) } != 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(format!("signal process group {}: {error}", identity.pid));
        }
    }
    wait_readable(&[Some(&exit)])
        .map_err(|error| format!("wait process group {}: {error}", identity.pid))?;
    // Reap when this adapter still happens to be the parent (e.g. re-adoption
    // in the same daemon). ECHILD is normal for an externally adopted worker.
    loop {
        let result = unsafe { libc::waitpid(identity.pid as i32, std::ptr::null_mut(), 0) };
        if result >= 0 {
            break;
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::ECHILD) => break,
            _ => return Err(format!("reap process group {}: {error}", identity.pid)),
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn spawn_identity_exit_wait(
    blocking: &BlockingWorkSender,
    identity: ProcessIdentity,
    poll_interval: Duration,
    sender: ExternalSender,
    actor: ActorAddress,
) {
    if let Err(sender) = blocking.submit_owned(sender, move |sender| {
        while identity.matches() {
            thread::sleep(poll_interval);
        }
        let _ = sender.send_to(
            actor,
            ProcessExitObservation {
                status: None,
                error: None,
            },
        );
    }) {
        let _ = sender.send_to(
            actor,
            ProcessExitObservation {
                status: None,
                error: Some("process identity watcher engine stopped".to_owned()),
            },
        );
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandOutputObservation {
    pub status: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub error: Option<String>,
}

pub fn spawn_command_output(
    blocking: &BlockingWorkSender,
    mut command: Command,
    sender: ExternalSender,
    actor: ActorAddress,
) {
    if let Err(sender) = blocking.submit_owned(sender, move |sender| {
        let observation = match command.output() {
            Ok(output) => CommandOutputObservation {
                status: output.status.code(),
                stdout: output.stdout,
                stderr: output.stderr,
                error: None,
            },
            Err(error) => CommandOutputObservation {
                status: None,
                stdout: Vec::new(),
                stderr: Vec::new(),
                error: Some(error.to_string()),
            },
        };
        let _ = sender.send_to(actor, observation);
    }) {
        let _ = sender.send_to(
            actor,
            CommandOutputObservation {
                status: None,
                stdout: Vec::new(),
                stderr: Vec::new(),
                error: Some("process command output engine stopped".to_owned()),
            },
        );
    }
}

pub fn spawn_detached_command_status(
    blocking: &BlockingWorkSender,
    mut command: Command,
    label: impl Into<String>,
) {
    let label = label.into();
    if let Err(label) = blocking.submit_owned(label, move |label| {
        if let Err(error) = command.status() {
            eprintln!("{label}: {error}");
        }
    }) {
        eprintln!("{label}: process command engine stopped");
    }
}

/// Transfer a child to an engine-owned waiter.
///
/// On rejected submission, notify the observer of failure and return the live
/// child to the caller, which remains responsible for stopping and reaping it.
pub fn spawn_child_wait(
    blocking: &BlockingWorkSender,
    child: Child,
    sender: ExternalSender,
    actor: ActorAddress,
) -> Result<(), Child> {
    blocking
        .submit_owned((child, sender), move |(mut child, sender)| {
            let observation = match child.wait() {
                Ok(status) => ProcessExitObservation {
                    status: status.code(),
                    error: None,
                },
                Err(error) => ProcessExitObservation {
                    status: None,
                    error: Some(error.to_string()),
                },
            };
            let _ = sender.send_to(actor, observation);
        })
        .map_err(|(child, sender)| {
            let _ = sender.send_to(
                actor,
                ProcessExitObservation {
                    status: None,
                    error: Some("process wait engine stopped".to_owned()),
                },
            );
            child
        })
}

pub fn spawn_mapped_line_reader<R, M, L, E>(
    blocking: &BlockingWorkSender,
    reader: R,
    sender: ExternalSender,
    actor: ActorAddress,
    line_message: L,
    error_message: E,
    closed_message: M,
) where
    R: Read + Send + 'static,
    M: swactor::actor::Message,
    L: Fn(String) -> M + Send + 'static,
    E: Fn(String) -> M + Send + 'static,
{
    if let Err((sender, error_message, closed_message)) = blocking.submit_owned(
        (sender, error_message, closed_message),
        move |(sender, error_message, closed_message)| {
            for line in BufReader::new(reader).lines() {
                match line {
                    Ok(line) => {
                        let _ = sender.send_to(actor, line_message(line));
                    }
                    Err(error) => {
                        let _ = sender.send_to(actor, error_message(error.to_string()));
                        break;
                    }
                }
            }
            let _ = sender.send_to(actor, closed_message);
        },
    ) {
        let _ = sender.send_to(
            actor,
            error_message("process line reader engine stopped".to_owned()),
        );
        let _ = sender.send_to(actor, closed_message);
    }
}

pub fn spawn_mapped_line_channel<R, M, L, E>(
    blocking: &BlockingWorkSender,
    reader: R,
    sender: std::sync::mpsc::Sender<M>,
    line_message: L,
    error_message: E,
    closed_message: M,
) where
    R: Read + Send + 'static,
    M: Send + 'static,
    L: Fn(String) -> M + Send + 'static,
    E: Fn(String) -> M + Send + 'static,
{
    if let Err((sender, error_message, closed_message)) = blocking.submit_owned(
        (sender, error_message, closed_message),
        move |(sender, error_message, closed_message)| {
            for line in BufReader::new(reader).lines() {
                let message = match line {
                    Ok(line) => line_message(line),
                    Err(error) => {
                        let _ = sender.send(error_message(error.to_string()));
                        break;
                    }
                };
                if sender.send(message).is_err() {
                    return;
                }
            }
            let _ = sender.send(closed_message);
        },
    ) {
        let _ = sender.send(error_message(
            "process line reader engine stopped".to_owned(),
        ));
        let _ = sender.send(closed_message);
    }
}

pub fn spawn_line_channel<R>(
    blocking: &BlockingWorkSender,
    reader: R,
    sender: std::sync::mpsc::Sender<String>,
) where
    R: Read + Send + 'static,
{
    let _ = blocking.submit(Box::new(move || {
        for line in BufReader::new(reader).lines().map_while(Result::ok) {
            if sender.send(line).is_err() {
                return;
            }
        }
    }));
}

fn forward_stdin_command<R>(
    reader: R,
    command: &str,
    trigger_on_eof: bool,
    sender: ExternalSender,
    actor: ActorAddress,
) where
    R: BufRead,
{
    for line in reader.lines().map_while(Result::ok) {
        if line.trim().eq_ignore_ascii_case(command) {
            let _ = sender.send_to(actor, ProcessStopSignal);
            return;
        }
    }
    if trigger_on_eof {
        let _ = sender.send_to(actor, ProcessStopSignal);
    }
}

pub fn spawn_stdin_command_wait(
    blocking: &BlockingWorkSender,
    command: &'static str,
    trigger_on_eof: bool,
    sender: ExternalSender,
    actor: ActorAddress,
) {
    let _ = blocking.submit(Box::new(move || {
        forward_stdin_command(
            std::io::stdin().lock(),
            command,
            trigger_on_eof,
            sender,
            actor,
        );
    }));
}

#[cfg(target_os = "linux")]
pub fn find_process_identities_by_environment(
    environment: &[(String, String)],
    process_group_leader: bool,
) -> io::Result<Vec<ProcessIdentity>> {
    let mut matches = Vec::new();
    for entry in std::fs::read_dir("/proc")?.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let identity = ProcessIdentity {
            pid,
            environment: environment.to_vec(),
            process_group_leader,
        };
        if identity.matches() {
            matches.push(identity);
        }
    }
    Ok(matches)
}

#[cfg(target_os = "linux")]
pub fn find_process_identities_with_retry(
    environment: &[(String, String)],
    process_group_leader: bool,
    attempts: usize,
    poll_interval: Duration,
) -> io::Result<Vec<ProcessIdentity>> {
    let attempts = attempts.max(1);
    for attempt in 0..attempts {
        let matches = find_process_identities_by_environment(environment, process_group_leader)?;
        if !matches.is_empty() || attempt + 1 == attempts {
            return Ok(matches);
        }
        thread::sleep(poll_interval);
    }
    unreachable!("at least one process discovery attempt runs")
}

pub fn wait_for_path(path: &Path, attempts: usize, poll_interval: Duration) -> bool {
    let attempts = attempts.max(1);
    for attempt in 0..attempts {
        if path.exists() {
            return true;
        }
        if attempt + 1 < attempts {
            thread::sleep(poll_interval);
        }
    }
    false
}

#[cfg(unix)]
struct BoundUnixSocketPath {
    path: std::path::PathBuf,
    device: u64,
    inode: u64,
}

#[cfg(unix)]
impl BoundUnixSocketPath {
    fn new(path: std::path::PathBuf) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt;

        let metadata = std::fs::symlink_metadata(&path)?;
        Ok(Self {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

#[cfg(unix)]
impl Drop for BoundUnixSocketPath {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;

        let owns_path = std::fs::symlink_metadata(&self.path)
            .is_ok_and(|metadata| metadata.dev() == self.device && metadata.ino() == self.inode);
        if owns_path {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(unix)]
pub fn spawn_unix_stream_listener<H, F>(
    tasks: swactor_engine::TaskSpawner,
    path: impl AsRef<Path>,
    handler: H,
) -> io::Result<()>
where
    H: Fn(tokio::net::UnixStream) -> F + Clone + Send + Sync + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    use std::os::unix::net::UnixListener;

    let path = path.as_ref().to_path_buf();
    let listener = UnixListener::bind(&path)?;
    let bound_path = BoundUnixSocketPath::new(path)?;
    if let Err(error) = listener.set_nonblocking(true) {
        drop(bound_path);
        return Err(error);
    }
    let connection_tasks = tasks.clone();
    tasks.spawn(async move {
        let _bound_path = bound_path;
        let listener = match tokio::net::UnixListener::from_std(listener) {
            Ok(listener) => listener,
            Err(error) => {
                eprintln!("Unix stream listener stopped during startup: {error}");
                return;
            }
        };
        loop {
            match listener.accept().await {
                Ok((stream, _address)) => {
                    connection_tasks.spawn(handler.clone()(stream));
                }
                Err(error) => {
                    eprintln!("Unix stream listener stopped: {error}");
                    return;
                }
            }
        }
    });
    Ok(())
}

#[cfg(test)]
mod properties {
    use std::collections::VecDeque;
    use std::fmt::Debug;
    use std::io::Cursor;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;

    use parking_lot::Mutex as ParkingMutex;
    use proptest::prelude::*;
    use swactor::actor::{ActorInterface, Ctx};
    use swactor::config::RuntimeConfig;
    use swactor::runtime::{Runtime, RuntimeParts, SingleThreadRuntime};
    use swactor_engine::{Engine, SteppingBackend};

    use super::*;

    const DRIVER_BUDGET: usize = 64;

    #[cfg(target_os = "linux")]
    #[test]
    fn owned_command_completes_inside_the_only_blocking_pool_worker() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            swactor_engine::TokioBackend::from_runtime(runtime),
        )
        .unwrap();
        let handle = engine.handle();
        let blocking = handle.blocking_work_sender();
        let command_work = blocking.clone();
        let output = blocking
            .submit_joinable(move || {
                command_output_owned(
                    &command_work,
                    Command::new("/bin/sh").args([
                        "-c",
                        "printf 'ready\\n'; dd if=/dev/zero bs=65536 count=2 2>/dev/null; printf 'rejected\\n' >&2; exit 7",
                    ]),
                )
            })
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
        assert_eq!(output.status.code(), Some(7));
        assert_eq!(&output.stdout[..6], b"ready\n");
        assert_eq!(output.stdout.len(), 6 + 131_072);
        assert!(output.stdout[6..].iter().all(|byte| *byte == 0));
        assert_eq!(output.stderr, b"rejected\n");
        assert_eq!(handle.ownership_snapshot().blocking, 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn graceful_shared_stop_preserves_child_cleanup_and_exit_status() {
        let mut child = Command::new("/bin/sh")
            .args(["-c", "IFS= read -r line; printf '%s\\n' \"$line\"; exit 7"])
            .process_group(0)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        let mut output = child.stdout.take().unwrap();
        let child = Arc::new(Mutex::new(Some(child)));
        let status = stop_shared_child_with_input(&child, &mut stdin, b"shutdown\n").unwrap();
        assert_eq!(status.and_then(|status| status.code()), Some(7));
        let mut bytes = Vec::new();
        output.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"shutdown\n");
        assert!(child.lock().is_ok_and(|child| child.is_none()));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn explicit_cancellation_releases_a_pending_graceful_stop() {
        use std::os::unix::process::ExitStatusExt;
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            SteppingBackend::new(),
        )
        .unwrap();
        let mut child = Command::new("/bin/sh")
            .args([
                "-c",
                "IFS= read -r line; printf '%s\\n' \"$line\"; IFS= read -r next",
            ])
            .process_group(0)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let child = Arc::new(Mutex::new(Some(child)));
        let graceful_child = Arc::clone(&child);
        let graceful = engine
            .handle()
            .blocking_work_sender()
            .submit_joinable(move || {
                stop_shared_child_with_input(&graceful_child, &mut stdin, b"shutdown\n")
            })
            .unwrap();
        let mut accepted = String::new();
        output.read_line(&mut accepted).unwrap();
        assert_eq!(accepted, "shutdown\n");
        let cancelled = cancel_shared_child(&child).unwrap().unwrap();
        assert_eq!(cancelled.signal(), Some(libc::SIGKILL));
        assert!(graceful.join().unwrap().unwrap().is_none());
        assert!(child.lock().is_ok_and(|child| child.is_none()));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn observed_command_callback_panic_cancels_and_reaps_the_child() {
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            SteppingBackend::new(),
        )
        .unwrap();
        let mut pid = None;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut command = Command::new("/bin/sh");
            command
                .args(["-c", "printf '%s\\n' \"$$\"; IFS= read -r line"])
                .stdin(std::process::Stdio::piped());
            let _ = command_status_observed(
                &engine.handle().blocking_work_sender(),
                &mut command,
                |observation| {
                    if let ProcessStreamObservation::Line {
                        stream: ProcessStream::Stdout,
                        line,
                    } = observation
                    {
                        pid = Some(line.parse::<i32>().unwrap());
                        panic!("consumer rejected command output");
                    }
                },
            );
        }));
        assert!(result.is_err());
        assert_eq!(
            unsafe { libc::kill(pid.expect("observed child PID"), 0) },
            -1
        );
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn bounded_command_output_kills_and_reaps_a_withheld_response() {
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            SteppingBackend::new(),
        )
        .expect("process test engine");
        let blocking = engine.handle().blocking_work_sender();
        let pid_path = std::env::temp_dir().join(format!(
            "swactor-command-deadline-{}-{}.pid",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut command = Command::new("/bin/sh");
        command
            .args([
                "-c",
                "trap '' TERM; printf '%s' \"$$\" > \"$1\"; printf partial; printf diagnostic >&2; exec sleep 30",
                "withheld-response",
            ])
            .arg(&pid_path);
        let started = Instant::now();
        let result =
            command_output_until(&blocking, &mut command, started + Duration::from_secs(1));
        let elapsed = started.elapsed();
        let pid = std::fs::read_to_string(&pid_path);
        let _ = std::fs::remove_file(&pid_path);

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert!(
            elapsed < Duration::from_secs(5),
            "withheld child lasted {elapsed:?}"
        );
        let pid = pid
            .expect("child reached its withheld response")
            .parse::<i32>()
            .unwrap();
        let mut status = 0;
        assert_eq!(
            unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) },
            -1
        );
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::ECHILD)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn owned_command_output_drains_raw_bytes_and_inherited_pipes_on_rejection() {
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            SteppingBackend::new(),
        )
        .expect("process test engine");
        let blocking = engine.handle().blocking_work_sender();
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            "printf '\\000\\377partial'; printf 'rejected\\n' >&2; sh -c 'kill -STOP $$' & exit 7",
        ]);
        let output = command_output_owned(&blocking, &mut command).unwrap();

        assert_eq!(output.status.code(), Some(7));
        assert_eq!(output.stdout, b"\0\xffpartial");
        assert_eq!(output.stderr, b"rejected\n");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn bounded_command_output_does_not_spawn_after_owner_expiry() {
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            SteppingBackend::new(),
        )
        .expect("process test engine");
        let blocking = engine.handle().blocking_work_sender();
        // A nonexistent executable distinguishes pre-spawn expiry from a
        // freshly minted command lifetime that attempts execution anyway.
        let error = command_output_until(
            &blocking,
            &mut Command::new("/definitely/not/a/real/bounded-command"),
            Instant::now(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[cfg(unix)]
    #[test]
    fn unix_listener_unlinks_its_bound_path_when_engine_stops() {
        let path = std::env::temp_dir().join(format!(
            "swactor-process-listener-cleanup-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        ));
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let backend = swactor_engine::SteppingBackend::new();
        let engine = swactor_engine::Engine::new(parts, backend).expect("stepping engine");

        spawn_unix_stream_listener(engine.task_spawner(), &path, |_stream| async {})
            .expect("bind test Unix listener");
        assert!(path.exists(), "listener path was never created");

        drop(engine);
        assert!(!path.exists(), "listener path survived its owning engine");
    }

    struct StreamProbe {
        observations: Arc<ParkingMutex<Vec<ProcessStreamObservation>>>,
        closes_left: usize,
    }

    impl ActorInterface for StreamProbe {
        type Incoming = ProcessStreamObservation;
        type Response = ();

        fn handle(&mut self, ctx: &Ctx, observation: Self::Incoming) {
            if matches!(observation, ProcessStreamObservation::Closed { .. })
                && self.closes_left > 0
            {
                self.closes_left -= 1;
            }
            self.observations.lock().push(observation);
            if self.closes_left == 0 {
                ctx.stop_self();
            }
        }
    }

    struct SignalProbe {
        count: Arc<AtomicUsize>,
    }

    impl ActorInterface for SignalProbe {
        type Incoming = ProcessStopSignal;
        type Response = ();

        fn handle(&mut self, ctx: &Ctx, _signal: Self::Incoming) {
            self.count.fetch_add(1, Ordering::SeqCst);
            ctx.stop_self();
        }
    }

    #[cfg(unix)]
    struct ExitProbe {
        observations: Arc<ParkingMutex<Vec<ProcessExitObservation>>>,
    }

    #[cfg(unix)]
    impl ActorInterface for ExitProbe {
        type Incoming = ProcessExitObservation;
        type Response = ();

        fn handle(&mut self, ctx: &Ctx, observation: Self::Incoming) {
            self.observations.lock().push(observation);
            ctx.stop_self();
        }
    }

    #[derive(Clone, Debug)]
    enum ReadAction {
        Line(String),
        Error,
        Eof,
    }

    #[derive(Clone, Debug)]
    struct StreamAction {
        stream: ProcessStream,
        action: ReadAction,
    }

    struct ScriptedReader {
        actions: VecDeque<ReadAction>,
        terminal: bool,
    }

    impl ScriptedReader {
        fn new(actions: impl IntoIterator<Item = ReadAction>) -> Self {
            Self {
                actions: actions.into_iter().collect(),
                terminal: false,
            }
        }
    }

    impl Read for ScriptedReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if self.terminal {
                return Ok(0);
            }
            match self.actions.pop_front() {
                Some(ReadAction::Line(line)) => {
                    let bytes = format!("{line}\n").into_bytes();
                    assert!(
                        bytes.len() <= buffer.len(),
                        "scripted line exceeds reader buffer"
                    );
                    buffer[..bytes.len()].copy_from_slice(&bytes);
                    Ok(bytes.len())
                }
                Some(ReadAction::Error) => {
                    self.terminal = true;
                    Err(io::Error::other("scripted read failure"))
                }
                Some(ReadAction::Eof) | None => {
                    self.terminal = true;
                    Ok(0)
                }
            }
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ExpectedStream {
        lines: Vec<String>,
        error: bool,
    }

    #[derive(Clone, Debug)]
    enum LifecycleAction {
        Stop,
        Exit(ProcessExitObservation),
    }

    struct LifecycleProbe {
        observations: Arc<ParkingMutex<Vec<ProcessExitObservation>>>,
        stop_effects: Arc<AtomicUsize>,
        stop_requested: bool,
    }

    impl ActorInterface for LifecycleProbe {
        type Incoming = LifecycleAction;
        type Response = ();

        fn handle(&mut self, ctx: &Ctx, action: Self::Incoming) {
            match action {
                LifecycleAction::Stop if !self.stop_requested => {
                    self.stop_requested = true;
                    self.stop_effects.fetch_add(1, Ordering::SeqCst);
                }
                LifecycleAction::Stop => {}
                LifecycleAction::Exit(observation) => {
                    self.observations.lock().push(observation);
                    ctx.stop_self();
                }
            }
        }
    }

    #[derive(Clone, Debug)]
    enum StopAction {
        Notify,
        Disconnect,
    }

    #[derive(Clone, Debug)]
    enum StdinAction {
        Command,
        MixedCaseCommand,
        Other(String),
    }

    fn runtime_host() -> (Runtime, SingleThreadRuntime) {
        let parts = RuntimeParts::new(RuntimeConfig {
            worker_count: 1,
            ..RuntimeConfig::default()
        });
        let runtime = parts.runtime().clone();
        (runtime, SingleThreadRuntime::new(parts))
    }

    fn drive(host: &mut SingleThreadRuntime) {
        for _ in 0..DRIVER_BUDGET {
            host.try_tick();
        }
    }

    fn stream_action_strategy() -> impl Strategy<Value = StreamAction> {
        let line = proptest::string::string_regex("[a-zA-Z0-9 ]{0,16}")
            .expect("valid generated line expression")
            .prop_map(ReadAction::Line);
        (
            any::<bool>(),
            prop_oneof![8 => line, 1 => Just(ReadAction::Error), 1 => Just(ReadAction::Eof)],
        )
            .prop_map(|(stderr, action)| StreamAction {
                stream: if stderr {
                    ProcessStream::Stderr
                } else {
                    ProcessStream::Stdout
                },
                action,
            })
    }

    fn lifecycle_action_strategy() -> impl Strategy<Value = LifecycleAction> {
        prop_oneof![
            4 => Just(LifecycleAction::Stop),
            5 => (-2_i32..=2).prop_map(|status| {
                LifecycleAction::Exit(ProcessExitObservation {
                    status: Some(status),
                    error: None,
                })
            }),
            1 => Just(LifecycleAction::Exit(ProcessExitObservation {
                status: None,
                error: Some("scripted wait failure".to_owned()),
            })),
        ]
    }

    fn stop_action_strategy() -> impl Strategy<Value = StopAction> {
        prop_oneof![4 => Just(StopAction::Notify), 1 => Just(StopAction::Disconnect)]
    }

    fn stdin_action_strategy() -> impl Strategy<Value = StdinAction> {
        prop_oneof![
            2 => Just(StdinAction::Command),
            1 => Just(StdinAction::MixedCaseCommand),
            5 => proptest::string::string_regex("[a-zA-Z0-9 ]{0,16}")
                .expect("valid generated stdin expression")
                .prop_filter("other input must not be the command", |line| {
                    !line.trim().eq_ignore_ascii_case("stop")
                })
                .prop_map(StdinAction::Other),
        ]
    }

    fn expected_stream(actions: &[StreamAction], stream: ProcessStream) -> ExpectedStream {
        let mut expected = ExpectedStream {
            lines: Vec::new(),
            error: false,
        };
        for action in actions
            .iter()
            .filter(|action| action.stream == stream)
            .map(|action| &action.action)
        {
            match action {
                ReadAction::Line(line) => expected.lines.push(line.clone()),
                ReadAction::Error => {
                    expected.error = true;
                    break;
                }
                ReadAction::Eof => break,
            }
        }
        expected
    }

    fn check_stream_invariant(
        observations: &[ProcessStreamObservation],
        stream: ProcessStream,
        expected: &ExpectedStream,
    ) -> Result<(), String> {
        let mut lines = Vec::new();
        let mut errors = 0;
        let mut closes = 0;
        let mut terminal_seen = false;
        for observation in observations {
            let observed_stream = match observation {
                ProcessStreamObservation::Line { stream, .. }
                | ProcessStreamObservation::Error { stream, .. }
                | ProcessStreamObservation::Closed { stream } => *stream,
            };
            if observed_stream != stream {
                continue;
            }
            match observation {
                ProcessStreamObservation::Line { line, .. } => {
                    if terminal_seen {
                        return Err(format!("line after terminal observation: {line:?}"));
                    }
                    lines.push(line.clone());
                }
                ProcessStreamObservation::Error { error, .. } => {
                    if terminal_seen {
                        return Err(format!("duplicate terminal error: {error}"));
                    }
                    if !error.contains("scripted read failure") {
                        return Err(format!("unexpected read error: {error}"));
                    }
                    errors += 1;
                    terminal_seen = true;
                }
                ProcessStreamObservation::Closed { .. } => {
                    if closes > 0 {
                        return Err("stream closed more than once".to_owned());
                    }
                    closes += 1;
                    terminal_seen = true;
                }
            }
        }
        if lines != expected.lines {
            return Err(format!(
                "line order/content changed: expected={:?}, actual={lines:?}",
                expected.lines
            ));
        }
        if errors != usize::from(expected.error) {
            return Err(format!(
                "read error count changed: expected={}, actual={errors}",
                usize::from(expected.error)
            ));
        }
        if closes != 1 {
            return Err(format!("expected one stream close, actual={closes}"));
        }
        Ok(())
    }

    fn check_lifecycle_invariant(
        observations: &[ProcessExitObservation],
        expected: &ProcessExitObservation,
    ) -> Result<(), String> {
        match observations {
            [actual] if actual == expected => Ok(()),
            [actual] => Err(format!(
                "terminal process observation changed: expected={expected:?}, actual={actual:?}"
            )),
            _ => Err(format!(
                "expected one terminal process observation, actual={observations:?}"
            )),
        }
    }

    fn check_signal_count(actual: usize, expected: usize) -> Result<(), String> {
        if actual == expected {
            Ok(())
        } else {
            Err(format!(
                "stop notification count changed: expected={expected}, actual={actual}"
            ))
        }
    }

    fn quiescence_violation(runtime: &Runtime) -> Option<String> {
        let stats = runtime.stats();
        let mailbox_depth = stats
            .workers
            .iter()
            .map(|worker| worker.mailbox_depth)
            .sum::<usize>();
        let panics = stats
            .workers
            .iter()
            .map(|worker| worker.panics)
            .sum::<u64>();
        if stats.actors.is_empty() && mailbox_depth == 0 && panics == 0 {
            None
        } else {
            Some(format!(
                "actors={:?}, mailbox_depth={mailbox_depth}, panics={panics}",
                stats.actors
            ))
        }
    }

    fn evidence<A: Debug, O: Debug, S: Debug>(
        actions: &A,
        observations: &O,
        runtime: &Runtime,
        outstanding: &S,
    ) -> String {
        let stats = runtime.stats();
        format!(
            "actions={actions:?}; observations={observations:?}; actor_census={:?}; \
             workers={:?}; outstanding={outstanding:?}",
            stats.actors, stats.workers
        )
    }

    #[cfg(unix)]
    #[test]
    fn trivial_real_child_exit_has_a_hard_timeout() {
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            SteppingBackend::new(),
        )
        .expect("process test engine");
        let blocking = engine.handle().blocking_work_sender();
        let (runtime, mut host) = runtime_host();
        let observations = Arc::new(ParkingMutex::new(Vec::new()));
        let actor = runtime
            .spawn(ExitProbe {
                observations: Arc::clone(&observations),
            })
            .expect("spawn process exit probe");
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "exit 0"]);
        let child = command_spawn(&mut command).expect("spawn trivial child");
        spawn_child_wait(&blocking, child, runtime.create_sender(), actor)
            .expect("live engine accepts process waiter");

        let deadline = Instant::now() + Duration::from_secs(2);
        while observations.lock().is_empty() && Instant::now() < deadline {
            host.try_tick();
            thread::yield_now();
        }
        drive(&mut host);

        let observations = observations.lock().clone();
        assert_eq!(
            observations,
            vec![ProcessExitObservation {
                status: Some(0),
                error: None,
            }],
            "trivial child did not exit before hard timeout; observations={observations:?}; \
             actor_census={:?}",
            runtime.stats().actors
        );
        assert_eq!(quiescence_violation(&runtime), None);
    }

    #[cfg(unix)]
    #[test]
    fn rejected_child_wait_returns_reapable_child_and_reports_failure() {
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            SteppingBackend::new(),
        )
        .expect("process test engine");
        let blocking = engine.handle().blocking_work_sender();
        drop(engine);
        let (runtime, mut host) = runtime_host();
        let observations = Arc::new(ParkingMutex::new(Vec::new()));
        let observer = runtime
            .spawn(ExitProbe {
                observations: Arc::clone(&observations),
            })
            .unwrap();
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "exit 7"]);
        let child = command_spawn(&mut command).expect("spawn child before rejected handoff");
        let mut child = spawn_child_wait(&blocking, child, runtime.create_sender(), observer)
            .expect_err("stopped engine must return child ownership");
        assert_eq!(child.wait().expect("reap returned child").code(), Some(7));
        drive(&mut host);
        let observations = observations.lock();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].status, None);
        assert!(observations[0].error.is_some());
        assert_eq!(quiescence_violation(&runtime), None);
    }

    #[test]
    fn rejected_output_work_reports_error_and_closes_stream() {
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            SteppingBackend::new(),
        )
        .expect("process test engine");
        let blocking = engine.handle().blocking_work_sender();
        drop(engine);
        let (runtime, mut host) = runtime_host();
        let observations = Arc::new(ParkingMutex::new(Vec::new()));
        let observer = runtime
            .spawn(StreamProbe {
                observations: Arc::clone(&observations),
                closes_left: 1,
            })
            .unwrap();
        let reader = spawn_line_reader(
            &blocking,
            ProcessStream::Stdout,
            Cursor::new(b"must not be read\n"),
            runtime.create_sender(),
            observer,
        );
        reader.join();
        drive(&mut host);
        assert!(matches!(
            observations.lock().as_slice(),
            [
                ProcessStreamObservation::Error {
                    stream: ProcessStream::Stdout,
                    ..
                },
                ProcessStreamObservation::Closed {
                    stream: ProcessStream::Stdout
                },
            ]
        ));
        assert_eq!(quiescence_violation(&runtime), None);
    }

    #[test]
    fn property_invariants_reject_controlled_defects() {
        let late_output = vec![
            ProcessStreamObservation::Closed {
                stream: ProcessStream::Stdout,
            },
            ProcessStreamObservation::Line {
                stream: ProcessStream::Stdout,
                line: "late".to_owned(),
            },
        ];
        assert!(
            check_stream_invariant(
                &late_output,
                ProcessStream::Stdout,
                &ExpectedStream {
                    lines: Vec::new(),
                    error: false,
                },
            )
            .is_err(),
            "stream invariant accepted output after close"
        );
        let duplicate_exit = ProcessExitObservation {
            status: Some(0),
            error: None,
        };
        assert!(
            check_lifecycle_invariant(
                &[duplicate_exit.clone(), duplicate_exit.clone()],
                &duplicate_exit,
            )
            .is_err(),
            "lifecycle invariant accepted duplicate terminal output"
        );
        assert!(
            check_signal_count(2, 1).is_err(),
            "stop invariant accepted a duplicate notification"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            max_shrink_iters: 2_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn generated_stream_observations_close_once_and_stay_closed(
            actions in prop::collection::vec(stream_action_strategy(), 0..=32),
        ) {
            let engine = Engine::new(
                RuntimeParts::new(RuntimeConfig::default()),
                SteppingBackend::new(),
            ).expect("process test engine");
            let blocking = engine.handle().blocking_work_sender();
            let (runtime, mut host) = runtime_host();
            let observations = Arc::new(ParkingMutex::new(Vec::new()));
            let actor = runtime
                .spawn(StreamProbe {
                    observations: Arc::clone(&observations),
                    closes_left: 2,
                })
                .expect("spawn process stream probe");
            let sender = runtime.create_sender();
            let stdout = ScriptedReader::new(
                actions
                    .iter()
                    .filter(|action| action.stream == ProcessStream::Stdout)
                    .map(|action| action.action.clone()),
            );
            let stderr = ScriptedReader::new(
                actions
                    .iter()
                    .filter(|action| action.stream == ProcessStream::Stderr)
                    .map(|action| action.action.clone()),
            );
            let stdout_handle =
                spawn_line_reader(&blocking, ProcessStream::Stdout, stdout, sender.clone(), actor);
            let stderr_handle =
                spawn_line_reader(&blocking, ProcessStream::Stderr, stderr, sender, actor);
            stdout_handle.join();
            stderr_handle.join();
            drive(&mut host);

            let observations = observations.lock().clone();
            let stdout_result = check_stream_invariant(
                &observations,
                ProcessStream::Stdout,
                &expected_stream(&actions, ProcessStream::Stdout),
            );
            let stderr_result = check_stream_invariant(
                &observations,
                ProcessStream::Stderr,
                &expected_stream(&actions, ProcessStream::Stderr),
            );
            let quiescence = quiescence_violation(&runtime);
            let diagnostic = evidence(
                &actions,
                &observations,
                &runtime,
                &("reader_threads=0", &quiescence),
            );
            prop_assert!(
                stdout_result.is_ok(),
                "{diagnostic}; stdout_violation={stdout_result:?}"
            );
            prop_assert!(
                stderr_result.is_ok(),
                "{diagnostic}; stderr_violation={stderr_result:?}"
            );
            prop_assert!(
                quiescence.is_none(),
                "{diagnostic}; quiescence_violation={quiescence:?}"
            );
        }

        #[test]
        fn generated_lifecycle_actions_make_stop_idempotent_and_exit_terminal(
            generated in prop::collection::vec(lifecycle_action_strategy(), 0..=31),
        ) {
            let mut actions = generated;
            if !actions
                .iter()
                .any(|action| matches!(action, LifecycleAction::Exit(_)))
            {
                actions.push(LifecycleAction::Exit(ProcessExitObservation {
                    status: Some(0),
                    error: None,
                }));
            }
            let first_exit = actions
                .iter()
                .position(|action| matches!(action, LifecycleAction::Exit(_)))
                .expect("normalization adds an exit");
            let expected_exit = match &actions[first_exit] {
                LifecycleAction::Exit(observation) => observation.clone(),
                LifecycleAction::Stop => unreachable!("first_exit points at exit"),
            };
            let expected_stop_effects = usize::from(
                actions[..first_exit]
                    .iter()
                    .any(|action| matches!(action, LifecycleAction::Stop)),
            );

            let (runtime, mut host) = runtime_host();
            let observations = Arc::new(ParkingMutex::new(Vec::new()));
            let stop_effects = Arc::new(AtomicUsize::new(0));
            let actor = runtime
                .spawn(LifecycleProbe {
                    observations: Arc::clone(&observations),
                    stop_effects: Arc::clone(&stop_effects),
                    stop_requested: false,
                })
                .expect("spawn lifecycle probe");
            let mut rejected = Vec::new();
            for (index, action) in actions.iter().cloned().enumerate() {
                if runtime.send_to(actor, action).is_err() {
                    rejected.push(index);
                }
                drive(&mut host);
            }

            let observations = observations.lock().clone();
            let lifecycle_result =
                check_lifecycle_invariant(&observations, &expected_exit);
            let actual_stop_effects = stop_effects.load(Ordering::SeqCst);
            let stop_result =
                check_signal_count(actual_stop_effects, expected_stop_effects);
            let quiescence = quiescence_violation(&runtime);
            let diagnostic = evidence(
                &actions,
                &observations,
                &runtime,
                &(
                    format!("rejected_action_indices={rejected:?}"),
                    &quiescence,
                ),
            );
            prop_assert!(
                lifecycle_result.is_ok(),
                "{diagnostic}; lifecycle_violation={lifecycle_result:?}"
            );
            prop_assert!(
                stop_result.is_ok(),
                "{diagnostic}; stop_violation={stop_result:?}"
            );
            prop_assert!(
                quiescence.is_none(),
                "{diagnostic}; quiescence_violation={quiescence:?}"
            );
        }

        #[test]
        fn generated_stop_notifications_are_delivered_at_most_once(
            actions in prop::collection::vec(stop_action_strategy(), 0..=32),
        ) {
            let backend = SteppingBackend::new();
            let engine = Engine::new(
                RuntimeParts::new(RuntimeConfig::default()),
                backend.clone(),
            ).expect("process test engine");
            let blocking = engine.handle().blocking_work_sender();
            let (runtime, mut host) = runtime_host();
            let count = Arc::new(AtomicUsize::new(0));
            let actor = runtime
                .spawn(SignalProbe {
                    count: Arc::clone(&count),
                })
                .expect("spawn process signal probe");
            let (sender, receiver) = mpsc::channel();
            let mut sender = Some(sender);
            spawn_stop_channel_wait(&blocking, receiver, runtime.create_sender(), actor);
            let mut expected = 0;
            let mut rejected = Vec::new();
            for (index, action) in actions.iter().enumerate() {
                match action {
                    StopAction::Notify => match sender.as_ref() {
                        Some(sender) => {
                            if expected == 0 {
                                expected = 1;
                            }
                            if sender.send(()).is_err() {
                                rejected.push(index);
                            }
                        }
                        None => rejected.push(index),
                    },
                    StopAction::Disconnect => drop(sender.take()),
                }
            }
            drop(sender);
            backend.join_blocking().expect("join process stop waiter");
            drive(&mut host);
            if expected == 0 {
                runtime.stop_actor(actor).expect("stop unused signal probe");
                drive(&mut host);
            }

            let actual = count.load(Ordering::SeqCst);
            let signal_result = check_signal_count(actual, expected);
            let quiescence = quiescence_violation(&runtime);
            let diagnostic = evidence(
                &actions,
                &format!("stop_notifications={actual}"),
                &runtime,
                &(
                    format!("rejected_action_indices={rejected:?}"),
                    &quiescence,
                ),
            );
            prop_assert!(
                signal_result.is_ok(),
                "{diagnostic}; signal_violation={signal_result:?}"
            );
            prop_assert!(
                quiescence.is_none(),
                "{diagnostic}; quiescence_violation={quiescence:?}"
            );
        }

        #[test]
        fn generated_stdin_commands_and_eof_notify_once(
            actions in prop::collection::vec(stdin_action_strategy(), 0..=32),
            trigger_on_eof in any::<bool>(),
        ) {
            let mut bytes = Vec::new();
            let mut has_command = false;
            for action in &actions {
                let line = match action {
                    StdinAction::Command => {
                        has_command = true;
                        "stop"
                    }
                    StdinAction::MixedCaseCommand => {
                        has_command = true;
                        "  StOp  "
                    }
                    StdinAction::Other(line) => line,
                };
                bytes.extend_from_slice(line.as_bytes());
                bytes.push(b'\n');
            }
            let expected = usize::from(has_command || trigger_on_eof);
            let (runtime, mut host) = runtime_host();
            let count = Arc::new(AtomicUsize::new(0));
            let actor = runtime
                .spawn(SignalProbe {
                    count: Arc::clone(&count),
                })
                .expect("spawn stdin signal probe");
            forward_stdin_command(
                Cursor::new(bytes),
                "stop",
                trigger_on_eof,
                runtime.create_sender(),
                actor,
            );
            drive(&mut host);
            if expected == 0 {
                runtime.stop_actor(actor).expect("stop unused stdin probe");
                drive(&mut host);
            }

            let actual = count.load(Ordering::SeqCst);
            let signal_result = check_signal_count(actual, expected);
            let quiescence = quiescence_violation(&runtime);
            let diagnostic = evidence(
                &(actions, trigger_on_eof),
                &format!("stop_notifications={actual}"),
                &runtime,
                &("stdin_reader=closed", &quiescence),
            );
            prop_assert!(
                signal_result.is_ok(),
                "{diagnostic}; signal_violation={signal_result:?}"
            );
            prop_assert!(
                quiescence.is_none(),
                "{diagnostic}; quiescence_violation={quiescence:?}"
            );
        }
    }
}
