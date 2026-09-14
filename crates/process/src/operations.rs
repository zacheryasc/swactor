//! Concrete operating-system process mechanics owned by `swactor-process`.
//!
//! Domain crates may choose a command or decide to stop a resource, but the
//! actual process creation, waiting, and signaling stays behind this substrate
//! boundary.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Output};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use swactor::actor::ActorAddress;
use swactor::runtime::ExternalSender;

pub fn command_output(command: &mut Command) -> io::Result<Output> {
    command.output()
}

/// Capture a command's exact output within an absolute owner deadline.
///
/// Uses the same process-group, parent-death and joined-I/O ownership as
/// [`SupervisedChild`], without requiring an actor mailbox to make progress.
/// Timeout kills and reaps the owned child before returning.
#[cfg(target_os = "linux")]
pub fn command_output_until(command: &mut Command, deadline: Instant) -> io::Result<Output> {
    let output = Arc::new(Mutex::new(CapturedCommandOutput::default()));
    let (finished, completion) = std::sync::mpsc::channel();
    let owner = SupervisedChild::spawn_observed(
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
    join: JoinHandle<()>,
}

impl LineReaderHandle {
    pub fn join(self) {
        let _ = self.join.join();
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
    stream: ProcessStream,
    reader: R,
    sender: ExternalSender,
    actor: ActorAddress,
) -> LineReaderHandle
where
    R: Read + Send + 'static,
{
    let join = thread::spawn(move || {
        read_process_lines(stream, reader, |observation| {
            sender.send_to(actor, observation).is_ok()
        });
    });
    LineReaderHandle { join }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessStopSignal;

/// Wait for an embedding process-control channel and notify an actor once.
pub fn spawn_stop_channel_wait(
    receiver: Receiver<()>,
    sender: ExternalSender,
    actor: ActorAddress,
) {
    drop(spawn_stop_channel_wait_thread(receiver, sender, actor));
}

fn spawn_stop_channel_wait_thread(
    receiver: Receiver<()>,
    sender: ExternalSender,
    actor: ActorAddress,
) -> JoinHandle<()> {
    thread::spawn(move || {
        if receiver.recv().is_ok() {
            let _ = sender.send_to(actor, ProcessStopSignal);
        }
    })
}

/// Wait for SIGINT/SIGTERM and notify an actor once.
///
/// Signal handlers are installed before this function returns. A caller may
/// therefore publish readiness immediately after a successful return without
/// racing the operating system's default signal action.
#[cfg(target_os = "linux")]
pub fn spawn_os_stop_signal_wait(sender: ExternalSender, actor: ActorAddress) -> io::Result<()> {
    let mut signals = signal_hook::iterator::Signals::new([
        signal_hook::consts::signal::SIGINT,
        signal_hook::consts::signal::SIGTERM,
    ])?;
    thread::spawn(move || {
        if signals.forever().next().is_some() {
            let _ = sender.send_to(actor, ProcessStopSignal);
        }
    });
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub environment: Vec<(String, String)>,
    pub process_group_leader: bool,
}

impl ProcessIdentity {
    pub fn matches(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
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
        #[cfg(not(target_os = "linux"))]
        false
    }
}

pub struct FollowProcessFile {
    file: File,
    identity: ProcessIdentity,
    poll_interval: Duration,
}

impl FollowProcessFile {
    pub fn new(file: File, identity: ProcessIdentity, poll_interval: Duration) -> Self {
        Self {
            file,
            identity,
            poll_interval,
        }
    }
}

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

/// A cancellable exit subscription. Dropping it joins its observer without
/// stopping the process, so adopted workers may outlive their observation.
#[cfg(target_os = "linux")]
pub struct ProcessWatch {
    cancel: Arc<OwnedFd>,
    thread: Option<JoinHandle<()>>,
}

#[cfg(target_os = "linux")]
impl Drop for ProcessWatch {
    fn drop(&mut self) {
        let value = 1_u64;
        unsafe {
            libc::write(
                self.cancel.as_raw_fd(),
                (&value as *const u64).cast(),
                std::mem::size_of::<u64>(),
            );
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(target_os = "linux")]
pub fn watch_process(
    identity: ProcessIdentity,
    child: Option<Arc<Mutex<Option<Child>>>>,
    sender: ExternalSender,
    actor: ActorAddress,
) -> io::Result<ProcessWatch> {
    let exit = process_exit_fd(identity.pid).ok();
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let cancel = Arc::new(unsafe { OwnedFd::from_raw_fd(fd) });
    let cancelled = Arc::clone(&cancel);
    let thread = thread::Builder::new().spawn(move || {
        let mut error = None;
        let status = loop {
            if let Some(child) = &child {
                let mut slot = child
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let Some(child) = slot.as_mut() else {
                    return;
                };
                match child.try_wait() {
                    Ok(Some(status)) => {
                        *slot = None;
                        break status.code();
                    }
                    Ok(None) => {}
                    Err(reason) => {
                        error = Some(reason.to_string());
                        break None;
                    }
                }
            } else if !identity.matches() {
                break None;
            }
            let delay = if exit.is_some() {
                Duration::from_secs(30)
            } else {
                Duration::from_millis(100)
            };
            match wait_process_fds(&[exit.as_ref(), Some(&cancelled)], delay) {
                Ok(ready) if ready[1] => return,
                Ok(ready) if ready[0] && child.is_none() => break None,
                Ok(_) => {}
                Err(reason) => {
                    error = Some(reason.to_string());
                    break None;
                }
            }
        };
        let _ = sender.send_to(actor, ProcessExitObservation { status, error });
    })?;
    Ok(ProcessWatch {
        cancel,
        thread: Some(thread),
    })
}

/// Send a best-effort shutdown command without blocking on stdin, then observe
/// exit until the absolute deadline. Kill and reap the owned group on expiry.
#[cfg(target_os = "linux")]
pub fn stop_shared_child_with_input(
    child: &Arc<Mutex<Option<Child>>>,
    stdin: &mut std::process::ChildStdin,
    input: &[u8],
    deadline: Instant,
) -> io::Result<Option<ExitStatus>> {
    let fd = stdin.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags >= 0 && unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } >= 0 {
        let _ = stdin.write_all(input);
        let _ = stdin.flush();
    }
    let exit = child
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .and_then(|child| process_exit_fd(child.id()).ok());
    loop {
        let mut slot = child
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(child) = slot.as_mut() else {
            return Ok(None);
        };
        if let Some(status) = child.try_wait()? {
            *slot = None;
            return Ok(Some(status));
        }
        if Instant::now() >= deadline {
            terminate_owned_child(child);
            let status = child.wait()?;
            *slot = None;
            return Ok(Some(status));
        }
        drop(slot);
        let left = deadline.saturating_duration_since(Instant::now());
        wait_process_fds(
            &[exit.as_ref()],
            if exit.is_some() {
                left
            } else {
                left.min(Duration::from_millis(50))
            },
        )?;
    }
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
    waiter: Option<JoinHandle<()>>,
}

#[cfg(target_os = "linux")]
#[derive(Default)]
struct CapturedCommandOutput {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    error: Option<io::Error>,
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
}

#[cfg(target_os = "linux")]
impl SupervisedOutputObserver {
    fn close_stream(&self, stream: ProcessStream) {
        if let Self::Actor { sender, actor, .. } = self {
            let _ = sender.send_to(
                *actor,
                SupervisedProcessObservation::Stream(ProcessStreamObservation::Closed { stream }),
            );
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
        }
    }
}

#[cfg(target_os = "linux")]
fn spawn_supervised_reader<R: Read + Send + 'static>(
    stream: ProcessStream,
    mut reader: R,
    observer: SupervisedOutputObserver,
) -> io::Result<JoinHandle<()>> {
    thread::Builder::new().spawn(move || match observer {
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
    })
}

#[cfg(target_os = "linux")]
impl SupervisedChild {
    pub fn spawn(
        command: &mut Command,
        input: Option<Arc<[u8]>>,
        deadline: Option<Instant>,
        sender: ExternalSender,
        actor: ActorAddress,
        operation: u64,
    ) -> io::Result<Self> {
        Self::spawn_observed(
            command,
            input,
            deadline,
            SupervisedOutputObserver::Actor {
                sender,
                actor,
                operation,
            },
        )
    }

    fn spawn_observed(
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
        let waiter = match thread::Builder::new().spawn(move || {
            let mut setup_error = None;
            let readers = [
                (
                    ProcessStream::Stdout,
                    spawn_supervised_reader(ProcessStream::Stdout, stdout, observer.clone()),
                ),
                (
                    ProcessStream::Stderr,
                    spawn_supervised_reader(ProcessStream::Stderr, stderr, observer.clone()),
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
                match thread::Builder::new()
                    .spawn(move || stdin.write_all(&bytes).and_then(|()| stdin.flush()))
                {
                    Ok(writer) => Some(writer),
                    Err(error) => {
                        setup_error = Some(error.to_string());
                        None
                    }
                }
            });
            let waited = if let Some(error) = setup_error {
                Err(error)
            } else {
                loop {
                    let left = deadline
                        .map_or(Duration::from_millis(i32::MAX as u64), |deadline| {
                            deadline.saturating_duration_since(Instant::now())
                        });
                    if left.is_zero() {
                        break Err("process owner deadline expired; pending child exit".to_owned());
                    }
                    match wait_process_fds(&[Some(&exit)], left) {
                        Ok([true]) => break Ok(()),
                        Ok([false]) => {}
                        Err(error) => break Err(error.to_string()),
                    }
                }
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
        if let Some(mut child) = slot.take() {
            terminate_owned_child(&mut child);
            let _ = child.wait();
        }
        drop(slot);
        if let Some(waiter) = self.waiter.take() {
            let _ = waiter.join();
        }
    }
}

pub fn spawn_shared_child_wait(
    child: Arc<Mutex<Option<Child>>>,
    poll_interval: Duration,
    sender: ExternalSender,
    actor: ActorAddress,
) {
    thread::spawn(move || {
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
    });
}

#[cfg(target_os = "linux")]
pub fn terminate_process_group(
    identity: &ProcessIdentity,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(), String> {
    if !identity.matches() {
        return Ok(());
    }
    let exit = process_exit_fd(identity.pid).ok();
    if !identity.matches() {
        return Ok(());
    }
    if unsafe { libc::kill(-(identity.pid as i32), libc::SIGTERM) } != 0 && identity.matches() {
        return Err(format!(
            "terminate process group {}: {}",
            identity.pid,
            io::Error::last_os_error()
        ));
    }
    let deadline = Instant::now() + timeout;
    while identity.matches() && Instant::now() < deadline {
        let left = deadline.saturating_duration_since(Instant::now());
        if wait_process_fds(
            &[exit.as_ref()],
            if exit.is_some() {
                left
            } else {
                left.min(poll_interval)
            },
        )
        .map_err(|error| format!("wait process group {}: {error}", identity.pid))?[0]
        {
            return Ok(());
        }
    }
    if identity.matches() {
        if unsafe { libc::kill(-(identity.pid as i32), libc::SIGKILL) } != 0 && identity.matches() {
            return Err(format!(
                "kill process group {}: {}",
                identity.pid,
                io::Error::last_os_error()
            ));
        }
        if let Some(exit) = &exit {
            let deadline = Instant::now() + timeout;
            loop {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err(format!(
                        "process group {} did not exit after KILL",
                        identity.pid
                    ));
                }
                if wait_process_fds(&[Some(exit)], left).map_err(|error| {
                    format!("observe killed process group {}: {error}", identity.pid)
                })?[0]
                {
                    break;
                }
            }
        } else if identity.matches() {
            return Err(format!(
                "process group {} exit cannot be confirmed",
                identity.pid
            ));
        }
    }
    Ok(())
}

pub fn spawn_identity_exit_wait(
    identity: ProcessIdentity,
    poll_interval: Duration,
    sender: ExternalSender,
    actor: ActorAddress,
) {
    thread::spawn(move || {
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
    });
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandOutputObservation {
    pub status: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub error: Option<String>,
}

pub fn spawn_command_output(mut command: Command, sender: ExternalSender, actor: ActorAddress) {
    thread::spawn(move || {
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
    });
}

pub fn spawn_detached_command_status(mut command: Command, label: impl Into<String>) {
    let label = label.into();
    thread::spawn(move || {
        if let Err(error) = command.status() {
            eprintln!("{label}: {error}");
        }
    });
}

pub fn spawn_child_wait(mut child: Child, sender: ExternalSender, actor: ActorAddress) {
    thread::spawn(move || {
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
    });
}

pub fn spawn_mapped_line_reader<R, M, L, E>(
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
    thread::spawn(move || {
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
    });
}

pub fn spawn_mapped_line_channel<R, M, L, E>(
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
    thread::spawn(move || {
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
    });
}

pub fn spawn_line_channel<R>(reader: R, sender: std::sync::mpsc::Sender<String>)
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        for line in BufReader::new(reader).lines().map_while(Result::ok) {
            if sender.send(line).is_err() {
                return;
            }
        }
    });
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
    command: &'static str,
    trigger_on_eof: bool,
    sender: ExternalSender,
    actor: ActorAddress,
) {
    thread::spawn(move || {
        forward_stdin_command(
            std::io::stdin().lock(),
            command,
            trigger_on_eof,
            sender,
            actor,
        );
    });
}

pub fn find_process_identities_by_environment(
    environment: &[(String, String)],
    process_group_leader: bool,
) -> io::Result<Vec<ProcessIdentity>> {
    #[cfg(target_os = "linux")]
    {
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
    #[cfg(not(target_os = "linux"))]
    Ok(Vec::new())
}

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
    engine: swactor_engine::EngineHandle,
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
    let connection_engine = engine.clone();
    engine.spawn(async move {
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
                    connection_engine.spawn(handler.clone()(stream));
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

    use super::*;

    const DRIVER_BUDGET: usize = 64;

    #[cfg(target_os = "linux")]
    #[test]
    fn bounded_command_output_kills_and_reaps_a_withheld_response() {
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
        let result = command_output_until(&mut command, started + Duration::from_secs(1));
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
    fn bounded_command_output_drains_raw_bytes_and_inherited_pipes_on_rejection() {
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            "printf '\\000\\377partial'; printf 'rejected\\n' >&2; sleep 30 & exit 7",
        ]);
        let started = Instant::now();
        let output = command_output_until(&mut command, started + Duration::from_secs(2)).unwrap();

        assert_eq!(output.status.code(), Some(7));
        assert_eq!(output.stdout, b"\0\xffpartial");
        assert_eq!(output.stderr, b"rejected\n");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn bounded_command_output_does_not_spawn_after_owner_expiry() {
        // A nonexistent executable distinguishes pre-spawn expiry from a
        // freshly minted command lifetime that attempts execution anyway.
        let error = command_output_until(
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

        spawn_unix_stream_listener(engine.handle(), &path, |_stream| async {})
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

    struct ExitProbe {
        observations: Arc<ParkingMutex<Vec<ProcessExitObservation>>>,
    }

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
        spawn_child_wait(child, runtime.create_sender(), actor);

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
                spawn_line_reader(ProcessStream::Stdout, stdout, sender.clone(), actor);
            let stderr_handle =
                spawn_line_reader(ProcessStream::Stderr, stderr, sender, actor);
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
            let (runtime, mut host) = runtime_host();
            let count = Arc::new(AtomicUsize::new(0));
            let actor = runtime
                .spawn(SignalProbe {
                    count: Arc::clone(&count),
                })
                .expect("spawn process signal probe");
            let (sender, receiver) = mpsc::channel();
            let mut sender = Some(sender);
            let waiter =
                spawn_stop_channel_wait_thread(receiver, runtime.create_sender(), actor);
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
            waiter.join().expect("join process stop waiter");
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
