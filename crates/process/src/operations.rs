//! Concrete operating-system process mechanics owned by `swactor-process`.
//!
//! Domain crates may choose a command or decide to stop a resource, but the
//! actual process creation, waiting, and signaling stays behind this substrate
//! boundary.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
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
        for next in BufReader::new(reader).lines() {
            let observation = match next {
                Ok(line) => ProcessStreamObservation::Line { stream, line },
                Err(error) => {
                    let _ = sender.send_to(
                        actor,
                        ProcessStreamObservation::Error {
                            stream,
                            error: error.to_string(),
                        },
                    );
                    break;
                }
            };
            if sender.send_to(actor, observation).is_err() {
                return;
            }
        }
        let _ = sender.send_to(actor, ProcessStreamObservation::Closed { stream });
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

pub fn wait_shared_child_or_kill(
    child: &Arc<Mutex<Option<Child>>>,
    timeout: Duration,
    process_group: bool,
    poll_interval: Duration,
) -> io::Result<Option<ExitStatus>> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let mut slot = child
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(child) = slot.as_mut() else {
            return Ok(None);
        };
        match child.try_wait()? {
            Some(status) => {
                *slot = None;
                return Ok(Some(status));
            }
            None => drop(slot),
        }
        thread::sleep(poll_interval);
    }

    let mut slot = child
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(child) = slot.as_mut() else {
        return Ok(None);
    };
    #[cfg(target_os = "linux")]
    if process_group {
        let _ = unsafe { libc::kill(-(child.id() as i32), libc::SIGKILL) };
    }
    child.kill()?;
    let status = child.wait()?;
    *slot = None;
    Ok(Some(status))
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
    if unsafe { libc::kill(-(identity.pid as i32), libc::SIGTERM) } != 0 {
        return Err(format!(
            "terminate process group {}: {}",
            identity.pid,
            io::Error::last_os_error()
        ));
    }
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !identity.matches() {
            return Ok(());
        }
        thread::sleep(poll_interval);
    }
    if unsafe { libc::kill(-(identity.pid as i32), libc::SIGKILL) } != 0 && identity.matches() {
        return Err(format!(
            "kill process group {}: {}",
            identity.pid,
            io::Error::last_os_error()
        ));
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
