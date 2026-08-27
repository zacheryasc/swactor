use std::io::{self, Read};
use std::mem;
use std::os::fd::RawFd;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_queue::SegQueue;

#[cfg(unix)]
use crate::resources::ProcessSpawnResources;
use crate::types::{ExitStatus, ProcessSpec, Signal};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ThreadCommand {
    Stop { kill_after: Option<Duration> },
    ShutdownNow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ThreadEvent {
    Started { pid: u32 },
    SpawnFailed { error: String },
    Exited { status: ExitStatus },
    Output { stderr: bool, bytes: Vec<u8> },
    Error { error: String },
    ThreadFinished,
}

#[derive(Clone)]
pub(crate) struct ThreadEventSink {
    queue: Arc<SegQueue<ThreadEvent>>,
    wake: Arc<dyn Fn() + Send + Sync>,
}

pub(crate) struct ThreadEventReceiver {
    queue: Arc<SegQueue<ThreadEvent>>,
}

pub(crate) fn thread_event_channel(
    wake: impl Fn() + Send + Sync + 'static,
) -> (ThreadEventSink, ThreadEventReceiver) {
    let queue = Arc::new(SegQueue::new());
    (
        ThreadEventSink {
            queue: queue.clone(),
            wake: Arc::new(wake),
        },
        ThreadEventReceiver { queue },
    )
}

impl ThreadEventSink {
    pub(crate) fn push(&self, event: ThreadEvent) {
        self.queue.push(event);
        (self.wake)();
    }
}

impl ThreadEventReceiver {
    pub(crate) fn drain(&self) -> Vec<ThreadEvent> {
        let mut events = Vec::new();
        while let Some(event) = self.queue.pop() {
            events.push(event);
        }
        events
    }
}

#[derive(Clone)]
struct WakeFd(Arc<WakeFdInner>);

struct WakeFdInner {
    fd: RawFd,
}

impl WakeFd {
    fn new() -> io::Result<Self> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self(Arc::new(WakeFdInner { fd })))
    }

    fn fd(&self) -> RawFd {
        self.0.fd
    }

    fn wake(&self) -> io::Result<()> {
        let value: u64 = 1;
        let ptr = (&value as *const u64).cast::<libc::c_void>();
        let len = mem::size_of::<u64>();

        loop {
            let written = unsafe { libc::write(self.fd(), ptr, len) };
            if written == len as libc::ssize_t {
                return Ok(());
            }
            if written < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                if is_would_block(&err) {
                    return Ok(());
                }
                return Err(err);
            }

            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short write to process supervisor wake fd",
            ));
        }
    }

    fn drain(&self) -> io::Result<()> {
        let mut value: u64 = 0;
        let ptr = (&mut value as *mut u64).cast::<libc::c_void>();
        let len = mem::size_of::<u64>();

        loop {
            let read = unsafe { libc::read(self.fd(), ptr, len) };
            if read == len as libc::ssize_t {
                continue;
            }
            if read < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                if is_would_block(&err) {
                    return Ok(());
                }
                return Err(err);
            }

            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short read from process supervisor wake fd",
            ));
        }
    }
}

impl Drop for WakeFdInner {
    fn drop(&mut self) {
        let _ = unsafe { libc::close(self.fd) };
    }
}

fn poll_command_wake(wake: &WakeFd, timeout: Duration) -> io::Result<bool> {
    let timeout_ms = poll_timeout_ms(timeout);
    let mut pollfd = libc::pollfd {
        fd: wake.fd(),
        events: libc::POLLIN,
        revents: 0,
    };

    loop {
        let result = unsafe { libc::poll(&mut pollfd, 1, timeout_ms) };
        if result == 0 {
            return Ok(false);
        }
        if result > 0 {
            let revents = pollfd.revents;
            if revents & (libc::POLLERR | libc::POLLNVAL | libc::POLLHUP) != 0 {
                return Err(io::Error::other(format!(
                    "process supervisor wake fd poll failed: revents={revents}"
                )));
            }
            return Ok(revents & libc::POLLIN != 0);
        }

        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(err);
    }
}

fn poll_timeout_ms(timeout: Duration) -> libc::c_int {
    if timeout.is_zero() {
        return 0;
    }

    let millis = timeout.as_millis();
    if millis == 0 {
        1
    } else {
        millis.min(libc::c_int::MAX as u128) as libc::c_int
    }
}

fn is_would_block(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(code) if code == libc::EAGAIN || code == libc::EWOULDBLOCK
    )
}

pub(crate) struct ProcessThreadHandle {
    commands: Arc<SegQueue<ThreadCommand>>,
    wake: WakeFd,
    join: Option<JoinHandle<()>>,
}

pub(crate) struct ProcessSupervisorThread;

impl ProcessSupervisorThread {
    pub(crate) fn start(
        spec: ProcessSpec,
        #[cfg(unix)] resources: ProcessSpawnResources,
        events: ThreadEventSink,
    ) -> Result<ProcessThreadHandle, swactor::Error> {
        let commands = Arc::new(SegQueue::new());
        let wake = WakeFd::new().map_err(|err| {
            swactor::Error::from(format!(
                "failed to create process supervisor wake fd: {err}"
            ))
        })?;

        let thread_commands = commands.clone();
        let thread_wake = wake.clone();
        let join = thread::Builder::new()
            .name("swactor-process-supervisor".to_owned())
            .spawn(move || {
                supervisor_thread_main(
                    spec,
                    #[cfg(unix)]
                    resources,
                    events,
                    thread_commands,
                    thread_wake,
                )
            })
            .map_err(|err| {
                swactor::Error::from(format!("failed to start process supervisor thread: {err}"))
            })?;

        Ok(ProcessThreadHandle {
            commands,
            wake,
            join: Some(join),
        })
    }
}

impl ProcessThreadHandle {
    pub(crate) fn send(&self, command: ThreadCommand) -> Result<(), String> {
        self.commands.push(command);
        self.wake
            .wake()
            .map_err(|err| format!("failed to wake process supervisor: {err}"))
    }

    pub(crate) fn stop(&self, kill_after: Option<Duration>) -> Result<(), String> {
        self.send(ThreadCommand::Stop { kill_after })
    }

    pub(crate) fn shutdown_now(&self) -> Result<(), String> {
        self.send(ThreadCommand::ShutdownNow)
    }

    pub(crate) fn is_finished(&self) -> bool {
        match &self.join {
            Some(join) => join.is_finished(),
            None => true,
        }
    }

    pub(crate) fn join_if_finished(&mut self) -> Result<bool, String> {
        if !self.is_finished() {
            return Ok(false);
        }

        if let Some(join) = self.join.take() {
            join.join()
                .map_err(|_| "process supervisor thread panicked".to_owned())?;
        }

        Ok(true)
    }
}

impl Drop for ProcessThreadHandle {
    fn drop(&mut self) {
        let _ = self.shutdown_now();
        let _ = self.join_if_finished();
    }
}

const WAITPID_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SupervisorState {
    Spawning,
    Running,
    Stopping,
    Done,
}

enum CommandOutcome {
    Continue,
    Exited(ExitStatus),
    Error(String),
}

fn supervisor_thread_main(
    spec: ProcessSpec,
    #[cfg(unix)] resources: ProcessSpawnResources,
    events: ThreadEventSink,
    commands: Arc<SegQueue<ThreadCommand>>,
    wake: WakeFd,
) {
    let mut state = SupervisorState::Spawning;
    let mut child: Option<Child>;
    let pid: Option<u32>;
    let mut kill_deadline: Option<Instant> = None;
    let mut kill_sent = false;
    let mut output_threads = Vec::new();
    debug_assert!(matches!(state, SupervisorState::Spawning));

    let mut cmd = Command::new(&spec.command);
    cmd.args(&spec.args);
    for (key, value) in &spec.env {
        cmd.env(key, value);
    }
    if let Some(dir) = &spec.working_dir {
        cmd.current_dir(dir);
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    resources.configure_command(&mut cmd);

    match cmd.spawn() {
        Ok(mut spawned_child) => {
            let child_pid = spawned_child.id();
            if let Some(stdout) = spawned_child.stdout.take() {
                output_threads.push(spawn_output_reader(stdout, events.clone(), false));
            }
            if let Some(stderr) = spawned_child.stderr.take() {
                output_threads.push(spawn_output_reader(stderr, events.clone(), true));
            }
            pid = Some(child_pid);
            child = Some(spawned_child);
            events.push(ThreadEvent::Started { pid: child_pid });
            state = SupervisorState::Running;

            match drain_queued_commands(
                child_pid,
                &commands,
                &mut state,
                &mut kill_deadline,
                &mut kill_sent,
            ) {
                CommandOutcome::Continue => {}
                CommandOutcome::Exited(status) => {
                    finish_with_exit(&events, &mut state, &mut child, &mut output_threads, status);
                    return;
                }
                CommandOutcome::Error(error) => {
                    finish_with_error(&events, &mut state, &mut child, &mut output_threads, error);
                    return;
                }
            }
        }
        Err(err) => {
            events.push(ThreadEvent::SpawnFailed {
                error: err.to_string(),
            });
            events.push(ThreadEvent::ThreadFinished);
            return;
        }
    }

    let child_pid = pid.expect("supervisor pid stored after successful spawn");
    loop {
        let timeout = command_poll_timeout(kill_deadline);
        match poll_command_wake(&wake, timeout) {
            Ok(true) => {
                if let Err(err) = wake.drain() {
                    finish_with_error(
                        &events,
                        &mut state,
                        &mut child,
                        &mut output_threads,
                        format!("process supervisor command wake failed: {err}"),
                    );
                    return;
                }

                match drain_queued_commands(
                    child_pid,
                    &commands,
                    &mut state,
                    &mut kill_deadline,
                    &mut kill_sent,
                ) {
                    CommandOutcome::Continue => {}
                    CommandOutcome::Exited(status) => {
                        finish_with_exit(
                            &events,
                            &mut state,
                            &mut child,
                            &mut output_threads,
                            status,
                        );
                        return;
                    }
                    CommandOutcome::Error(error) => {
                        finish_with_error(
                            &events,
                            &mut state,
                            &mut child,
                            &mut output_threads,
                            error,
                        );
                        return;
                    }
                }
            }
            Ok(false) => {}
            Err(err) => {
                finish_with_error(
                    &events,
                    &mut state,
                    &mut child,
                    &mut output_threads,
                    format!("process supervisor command wake failed: {err}"),
                );
                return;
            }
        }

        match try_wait_pid(child_pid) {
            Ok(Some(status)) => {
                finish_with_exit(&events, &mut state, &mut child, &mut output_threads, status);
                return;
            }
            Ok(None) => {}
            Err(error) => {
                finish_with_error(&events, &mut state, &mut child, &mut output_threads, error);
                return;
            }
        }

        if kill_deadline.is_some_and(|deadline| deadline <= Instant::now()) && !kill_sent {
            match send_kill_or_observe_exit(child_pid) {
                CommandOutcome::Continue => {
                    kill_sent = true;
                    kill_deadline = None;
                    continue;
                }
                CommandOutcome::Exited(status) => {
                    finish_with_exit(&events, &mut state, &mut child, &mut output_threads, status);
                    return;
                }
                CommandOutcome::Error(error) => {
                    finish_with_error(&events, &mut state, &mut child, &mut output_threads, error);
                    return;
                }
            }
        }
    }
}

fn command_poll_timeout(kill_deadline: Option<Instant>) -> Duration {
    let Some(deadline) = kill_deadline else {
        return WAITPID_POLL_INTERVAL;
    };

    let now = Instant::now();
    if deadline <= now {
        Duration::ZERO
    } else {
        (deadline - now).min(WAITPID_POLL_INTERVAL)
    }
}

fn drain_queued_commands(
    pid: u32,
    commands: &SegQueue<ThreadCommand>,
    state: &mut SupervisorState,
    kill_deadline: &mut Option<Instant>,
    kill_sent: &mut bool,
) -> CommandOutcome {
    while let Some(command) = commands.pop() {
        match apply_thread_command(pid, command, state, kill_deadline, kill_sent) {
            CommandOutcome::Continue => {}
            outcome => return outcome,
        }
    }

    CommandOutcome::Continue
}

fn apply_thread_command(
    pid: u32,
    command: ThreadCommand,
    state: &mut SupervisorState,
    kill_deadline: &mut Option<Instant>,
    kill_sent: &mut bool,
) -> CommandOutcome {
    match command {
        ThreadCommand::Stop { kill_after } => match state {
            SupervisorState::Running => match send_terminate_or_observe_exit(pid) {
                CommandOutcome::Continue => {
                    if let Some(duration) = kill_after {
                        *kill_deadline = Some(Instant::now() + duration);
                    } else {
                        *kill_deadline = None;
                    }
                    *state = SupervisorState::Stopping;
                    CommandOutcome::Continue
                }
                outcome => outcome,
            },
            SupervisorState::Spawning | SupervisorState::Stopping | SupervisorState::Done => {
                CommandOutcome::Continue
            }
        },
        ThreadCommand::ShutdownNow => match state {
            SupervisorState::Done => CommandOutcome::Continue,
            SupervisorState::Spawning | SupervisorState::Running | SupervisorState::Stopping => {
                *state = SupervisorState::Stopping;
                *kill_deadline = None;
                match send_terminate_or_observe_exit(pid) {
                    CommandOutcome::Continue => match try_wait_pid(pid) {
                        Ok(Some(status)) => CommandOutcome::Exited(status),
                        Ok(None) => match send_kill_or_observe_exit(pid) {
                            CommandOutcome::Continue => {
                                *kill_sent = true;
                                CommandOutcome::Continue
                            }
                            outcome => outcome,
                        },
                        Err(error) => CommandOutcome::Error(error),
                    },
                    outcome => outcome,
                }
            }
        },
    }
}

fn send_terminate_or_observe_exit(pid: u32) -> CommandOutcome {
    match send_signal(pid, Signal::Terminate) {
        Ok(()) => CommandOutcome::Continue,
        Err(kill_error) => match try_wait_pid(pid) {
            Ok(Some(status)) => CommandOutcome::Exited(status),
            Ok(None) => {
                CommandOutcome::Error(format!("failed to terminate process {pid}: {kill_error}"))
            }
            Err(error) => CommandOutcome::Error(error),
        },
    }
}

fn send_kill_or_observe_exit(pid: u32) -> CommandOutcome {
    match send_signal(pid, Signal::Kill) {
        Ok(()) => CommandOutcome::Continue,
        Err(kill_error) => match try_wait_pid(pid) {
            Ok(Some(status)) => CommandOutcome::Exited(status),
            Ok(None) => {
                CommandOutcome::Error(format!("failed to kill process {pid}: {kill_error}"))
            }
            Err(error) => CommandOutcome::Error(error),
        },
    }
}

fn spawn_output_reader(
    mut reader: impl Read + Send + 'static,
    events: ThreadEventSink,
    stderr: bool,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut buffer = vec![0_u8; 4_096];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => events.push(ThreadEvent::Output {
                    stderr,
                    bytes: buffer[..read].to_vec(),
                }),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
    })
}

fn join_output_readers(readers: &mut Vec<JoinHandle<()>>) {
    for reader in readers.drain(..) {
        let _ = reader.join();
    }
}

fn finish_with_exit(
    events: &ThreadEventSink,
    state: &mut SupervisorState,
    child: &mut Option<Child>,
    output_threads: &mut Vec<JoinHandle<()>>,
    status: ExitStatus,
) {
    *state = SupervisorState::Done;
    debug_assert!(matches!(*state, SupervisorState::Done));
    let _ = child.take();
    join_output_readers(output_threads);
    events.push(ThreadEvent::Exited { status });
    events.push(ThreadEvent::ThreadFinished);
}

fn finish_with_error(
    events: &ThreadEventSink,
    state: &mut SupervisorState,
    child: &mut Option<Child>,
    output_threads: &mut Vec<JoinHandle<()>>,
    error: String,
) {
    *state = SupervisorState::Done;
    debug_assert!(matches!(*state, SupervisorState::Done));
    if let Some(child) = child.as_mut() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let _ = child.take();
    join_output_readers(output_threads);
    events.push(ThreadEvent::Error { error });
    events.push(ThreadEvent::ThreadFinished);
}

fn signal_to_libc(signal: Signal) -> libc::c_int {
    match signal {
        Signal::Terminate => libc::SIGTERM,
        Signal::Kill => libc::SIGKILL,
    }
}

fn send_signal(pid: u32, signal: Signal) -> Result<(), String> {
    let sig = signal_to_libc(signal);
    let ret = unsafe { libc::kill(pid as libc::pid_t, sig) };
    if ret == 0 {
        Ok(())
    } else {
        Err(format!(
            "kill({}, {}) failed: {}",
            pid,
            sig,
            std::io::Error::last_os_error()
        ))
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

fn try_wait_pid(pid: u32) -> Result<Option<ExitStatus>, String> {
    loop {
        let mut status: libc::c_int = 0;
        let result = unsafe { libc::waitpid(pid as libc::pid_t, &mut status, libc::WNOHANG) };

        if result == 0 {
            return Ok(None);
        }
        if result == pid as libc::pid_t {
            return Ok(Some(decode_wait_status(status)));
        }
        if result > 0 {
            return Ok(Some(ExitStatus::Unknown));
        }

        let err = std::io::Error::last_os_error();
        if err.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(format!("waitpid({pid}) failed: {err}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn spec(command: &str, args: Vec<&str>) -> ProcessSpec {
        ProcessSpec {
            command: command.to_owned(),
            args: args.into_iter().map(str::to_owned).collect(),
            env: HashMap::new(),
            working_dir: None,
            label: None,
        }
    }

    fn shell_spec(script: &str) -> ProcessSpec {
        spec("sh", vec!["-c", script])
    }

    fn collect_until_finished(
        receiver: &ThreadEventReceiver,
        timeout: Duration,
    ) -> Vec<ThreadEvent> {
        let deadline = Instant::now() + timeout;
        let mut events = Vec::new();

        while Instant::now() < deadline {
            events.extend(receiver.drain());
            if matches!(events.last(), Some(ThreadEvent::ThreadFinished)) {
                return events;
            }
            thread::sleep(Duration::from_millis(5));
        }

        events.extend(receiver.drain());
        events
    }

    fn join_finished(handle: &mut ProcessThreadHandle) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if handle
                .join_if_finished()
                .expect("supervisor thread should join")
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "supervisor thread did not finish before join timeout"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn thread_command_and_event_shapes_match_target_contract() {
        let stop_with_deadline = ThreadCommand::Stop {
            kill_after: Some(Duration::from_millis(10)),
        };
        assert_eq!(
            stop_with_deadline,
            ThreadCommand::Stop {
                kill_after: Some(Duration::from_millis(10))
            }
        );
        match stop_with_deadline {
            ThreadCommand::Stop {
                kill_after: Some(duration),
            } => assert_eq!(duration, Duration::from_millis(10)),
            _ => panic!("expected stop command with deadline"),
        }

        let stop_without_deadline = ThreadCommand::Stop { kill_after: None };
        assert_eq!(
            stop_without_deadline,
            ThreadCommand::Stop { kill_after: None }
        );
        match stop_without_deadline {
            ThreadCommand::Stop { kill_after: None } => {}
            _ => panic!("expected stop command without deadline"),
        }

        assert_eq!(ThreadCommand::ShutdownNow, ThreadCommand::ShutdownNow);
        match ThreadCommand::ShutdownNow {
            ThreadCommand::ShutdownNow => {}
            _ => panic!("expected shutdown command"),
        }

        let started = ThreadEvent::Started { pid: 42 };
        assert_eq!(started, ThreadEvent::Started { pid: 42 });
        match started {
            ThreadEvent::Started { pid } => assert_eq!(pid, 42),
            _ => panic!("expected started event"),
        }

        let spawn_failed = ThreadEvent::SpawnFailed {
            error: "spawn failed".to_owned(),
        };
        assert_eq!(
            spawn_failed,
            ThreadEvent::SpawnFailed {
                error: "spawn failed".to_owned()
            }
        );
        match spawn_failed {
            ThreadEvent::SpawnFailed { error } => assert_eq!(error, "spawn failed"),
            _ => panic!("expected spawn failed event"),
        }

        let exited = ThreadEvent::Exited {
            status: ExitStatus::Code(7),
        };
        assert_eq!(
            exited,
            ThreadEvent::Exited {
                status: ExitStatus::Code(7)
            }
        );
        match exited {
            ThreadEvent::Exited { status } => assert_eq!(status, ExitStatus::Code(7)),
            _ => panic!("expected exited event"),
        }

        let error = ThreadEvent::Error {
            error: "lost".to_owned(),
        };
        assert_eq!(
            error,
            ThreadEvent::Error {
                error: "lost".to_owned()
            }
        );
        match error {
            ThreadEvent::Error { error } => assert_eq!(error, "lost"),
            _ => panic!("expected error event"),
        }

        assert_eq!(ThreadEvent::ThreadFinished, ThreadEvent::ThreadFinished);
        match ThreadEvent::ThreadFinished {
            ThreadEvent::ThreadFinished => {}
            _ => panic!("expected thread finished event"),
        }
    }

    #[test]
    fn thread_event_sink_enqueues_then_wakes() {
        let wake_count = Arc::new(AtomicUsize::new(0));
        let wake_count_for_callback = wake_count.clone();
        let (sink, receiver) = thread_event_channel(move || {
            wake_count_for_callback.fetch_add(1, Ordering::SeqCst);
        });

        sink.push(ThreadEvent::Started { pid: 1 });
        sink.push(ThreadEvent::Exited {
            status: ExitStatus::Code(0),
        });

        assert_eq!(wake_count.load(Ordering::SeqCst), 2);
        assert_eq!(
            receiver.drain(),
            vec![
                ThreadEvent::Started { pid: 1 },
                ThreadEvent::Exited {
                    status: ExitStatus::Code(0)
                }
            ]
        );
        assert!(receiver.drain().is_empty());
    }

    #[test]
    fn process_thread_handle_queues_commands_and_wakes_supervisor() {
        let commands = Arc::new(SegQueue::new());
        let wake = WakeFd::new().expect("wake fd should be created");
        let handle = ProcessThreadHandle {
            commands: commands.clone(),
            wake: wake.clone(),
            join: None,
        };

        handle
            .send(ThreadCommand::Stop {
                kill_after: Some(Duration::from_millis(25)),
            })
            .expect("stop command should queue");
        handle
            .send(ThreadCommand::ShutdownNow)
            .expect("shutdown command should queue");

        assert!(
            poll_command_wake(&wake, Duration::ZERO).expect("wake poll should succeed"),
            "wake fd should be readable after queued commands"
        );
        wake.drain().expect("wake fd should drain");
        assert_eq!(
            commands.pop(),
            Some(ThreadCommand::Stop {
                kill_after: Some(Duration::from_millis(25))
            })
        );
        assert_eq!(commands.pop(), Some(ThreadCommand::ShutdownNow));
        assert_eq!(commands.pop(), None);
    }

    #[test]
    fn supervisor_reports_started_exited_and_finished() {
        let (sink, receiver) = thread_event_channel(|| {});
        let mut handle = ProcessSupervisorThread::start(
            shell_spec("exit 7"),
            ProcessSpawnResources::new(),
            sink,
        )
        .expect("supervisor should start");

        let events = collect_until_finished(&receiver, Duration::from_secs(2));
        join_finished(&mut handle);

        assert_eq!(events.len(), 3);
        assert!(matches!(
            events.first(),
            Some(ThreadEvent::Started { pid }) if *pid > 0
        ));
        assert_eq!(
            events.get(1),
            Some(&ThreadEvent::Exited {
                status: ExitStatus::Code(7)
            })
        );
        assert_eq!(events.last(), Some(&ThreadEvent::ThreadFinished));
    }

    #[test]
    fn supervisor_reports_spawn_failed_and_finished() {
        let (sink, receiver) = thread_event_channel(|| {});
        let mut handle = ProcessSupervisorThread::start(
            spec("/definitely/not/a/real/binary", vec![]),
            ProcessSpawnResources::new(),
            sink,
        )
        .expect("supervisor should start");

        let events = collect_until_finished(&receiver, Duration::from_secs(2));
        join_finished(&mut handle);

        assert_eq!(events.len(), 2);
        assert!(matches!(
            events.first(),
            Some(ThreadEvent::SpawnFailed { error }) if !error.is_empty()
        ));
        assert_eq!(events.last(), Some(&ThreadEvent::ThreadFinished));
        assert!(!events.iter().any(|event| {
            matches!(
                event,
                ThreadEvent::Started { .. }
                    | ThreadEvent::Exited { .. }
                    | ThreadEvent::Error { .. }
            )
        }));
    }

    #[test]
    fn supervisor_captures_stdout_and_stderr_before_lifecycle_completion() {
        let (sink, receiver) = thread_event_channel(|| {});
        let mut handle = ProcessSupervisorThread::start(
            shell_spec("echo stdout; echo stderr >&2; exit 0"),
            ProcessSpawnResources::new(),
            sink,
        )
        .expect("supervisor should start");

        let events = collect_until_finished(&receiver, Duration::from_secs(2));
        join_finished(&mut handle);

        assert!(matches!(
            events.first(),
            Some(ThreadEvent::Started { pid }) if *pid > 0
        ));
        let exit = events
            .iter()
            .position(|event| matches!(event, ThreadEvent::Exited { .. }))
            .expect("exited event");
        assert!(events[..exit]
            .iter()
            .any(|event| matches!(event, ThreadEvent::Output { stderr: false, bytes } if bytes == b"stdout\n")));
        assert!(events[..exit]
            .iter()
            .any(|event| matches!(event, ThreadEvent::Output { stderr: true, bytes } if bytes == b"stderr\n")));
        assert_eq!(
            events.get(exit),
            Some(&ThreadEvent::Exited {
                status: ExitStatus::Code(0)
            })
        );
        assert_eq!(events.last(), Some(&ThreadEvent::ThreadFinished));
    }

    #[test]
    fn supervisor_stop_escalates_to_kill_after_deadline() {
        let (sink, receiver) = thread_event_channel(|| {});
        let mut handle = ProcessSupervisorThread::start(
            shell_spec("trap '' TERM; while true; do sleep 1; done"),
            ProcessSpawnResources::new(),
            sink,
        )
        .expect("supervisor should start");
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut events = Vec::new();
        let mut stop_sent = false;

        while Instant::now() < deadline {
            events.extend(receiver.drain());
            if !stop_sent
                && events
                    .iter()
                    .any(|event| matches!(event, ThreadEvent::Started { pid } if *pid > 0))
            {
                handle
                    .stop(Some(Duration::from_millis(20)))
                    .expect("stop command should queue");
                stop_sent = true;
            }
            if matches!(events.last(), Some(ThreadEvent::ThreadFinished)) {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }

        if !matches!(events.last(), Some(ThreadEvent::ThreadFinished)) {
            let _ = handle.shutdown_now();
            events.extend(collect_until_finished(&receiver, Duration::from_secs(2)));
        }
        join_finished(&mut handle);

        assert!(
            stop_sent,
            "supervisor did not report Started before timeout"
        );
        assert_eq!(events.last(), Some(&ThreadEvent::ThreadFinished));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ThreadEvent::Exited {
                    status: ExitStatus::Signal(9)
                }
            )
        }));
    }
}
