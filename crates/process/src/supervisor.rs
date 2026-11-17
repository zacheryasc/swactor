use std::io;
use std::mem;
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_queue::SegQueue;
use swactor_engine::{BlockingWorkSender, OwnedIoThread};

#[cfg(unix)]
use crate::resources::ProcessSpawnResources;
use crate::stdin::{ProcessInput, ProcessInputWriter};
use crate::types::{ExitStatus, ProcessSpec, Signal};
use crate::terminal::TerminalSession;
use crate::supervisor_common::{
    ThreadCommand, ThreadEvent, ThreadEventSink, join_output_readers, spawn_output_reader,
};
#[cfg(test)]
use crate::supervisor_common::{ThreadEventReceiver, thread_event_channel};


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

fn poll_command_wake(
    wake: &WakeFd,
    child_exit: Option<RawFd>,
    timeout: Option<Duration>,
) -> io::Result<bool> {
    let timeout_ms = timeout.map_or(-1, poll_timeout_ms);
    let mut pollfds = [
        libc::pollfd {
            fd: wake.fd(),
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: child_exit.unwrap_or(-1),
            events: libc::POLLIN,
            revents: 0,
        },
    ];

    loop {
        let result = unsafe { libc::poll(pollfds.as_mut_ptr(), pollfds.len() as _, timeout_ms) };
        if result == 0 {
            return Ok(false);
        }
        if result > 0 {
            let revents = pollfds[0].revents;
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
    join: Option<OwnedIoThread<()>>,
}

pub(crate) struct ProcessSupervisorThread;

impl ProcessSupervisorThread {
    pub(crate) fn start(
        blocking: &BlockingWorkSender,
        spec: ProcessSpec,
        #[cfg(unix)] resources: ProcessSpawnResources,
        input: Option<ProcessInput>,
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
        let io_work = blocking.clone();
        let join = blocking
            .spawn_owned_thread("process-supervisor", move || {
                let failure_events = events.clone();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    supervisor_thread_main(
                        &io_work,
                        spec,
                        #[cfg(unix)]
                        resources,
                        input,
                        events,
                        thread_commands,
                        thread_wake,
                    );
                }));
                if result.is_err() {
                    failure_events.push(ThreadEvent::Error {
                        error: "process supervisor thread panicked".to_owned(),
                    });
                    failure_events.push(ThreadEvent::ThreadFinished);
                }
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
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

const WAITPID_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// A pidfd is tied to this exact child, including an exit before poll starts.
/// Kernels/platforms without pidfds retain the existing waitpid reconciliation.
fn child_exit_fd(pid: u32) -> Option<OwnedFd> {
    #[cfg(target_os = "linux")]
    {
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if fd >= 0 {
            return Some(unsafe { OwnedFd::from_raw_fd(fd as RawFd) });
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = pid;
    None
}

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

/// Unwind ownership is installed before any observer callback or I/O setup.
/// Declare this after reader handles so kill/reap precedes all worker joins.
struct ChildOwner {
    child: Option<Child>,
    input_writer: Option<ProcessInputWriter>,
    terminal_session: bool,
}

impl ChildOwner {
    fn kill_owned_tree(&mut self) -> Result<(), String> {
        self.shutdown_terminal_input();
        let Some(child) = self.child.as_ref() else { return Ok(()); };
        let _ = send_signal(child.id(), Signal::Kill);
        #[cfg(target_os = "linux")]
        if self.terminal_session {
            crate::terminal::cancel_session(child.id()).map_err(|error| {
                format!("cancel terminal session: {error}")
            })?;
        }
        Ok(())
    }

    fn shutdown_terminal_input(&mut self) {
        if let Some(writer) = &mut self.input_writer {
            writer.shutdown_terminal();
        }
    }

    fn finish_input(&mut self) -> Result<(), String> {
        self.input_writer.take().map_or(Ok(()), |mut writer| {
            writer.finish().map_err(|error| error.to_string())
        })
    }
}

impl std::ops::Deref for ChildOwner {
    type Target = Option<Child>;

    fn deref(&self) -> &Self::Target {
        &self.child
    }
}

impl std::ops::DerefMut for ChildOwner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.child
    }
}

impl Drop for ChildOwner {
    fn drop(&mut self) {
        let _ = self.kill_owned_tree();
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = self.finish_input();
    }
}

fn supervisor_thread_main(
    blocking: &BlockingWorkSender,
    spec: ProcessSpec,
    #[cfg(unix)] resources: ProcessSpawnResources,
    mut input: Option<ProcessInput>,
    events: ThreadEventSink,
    commands: Arc<SegQueue<ThreadCommand>>,
    wake: WakeFd,
) {
    let mut state = SupervisorState::Spawning;
    let mut output_threads = Vec::new();
    let mut child = ChildOwner {
        child: None,
        input_writer: None,
        terminal_session: input.as_ref().is_some_and(|input| input.terminal_size.is_some()),
    };
    let pid: Option<u32>;
    let mut kill_deadline: Option<Instant> = None;
    let mut kill_sent = false;
    debug_assert!(matches!(state, SupervisorState::Spawning));

    let mut cmd = Command::new(&spec.command);
    cmd.args(&spec.args);
    for (key, value) in &spec.env {
        cmd.env(key, value);
    }
    if let Some(dir) = &spec.working_dir {
        cmd.current_dir(dir);
    }
    let terminal_io = (|| -> io::Result<_> {
        if let Some(size) = input.as_ref().and_then(|input| input.terminal_size) {
            let terminal = TerminalSession::new(size)?;
            terminal.configure_command(&mut cmd)?;
            Ok(Some(terminal.into_io()?))
        } else {
            cmd.stdin(if input.is_some() { Stdio::piped() } else { Stdio::null() })
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            cmd.process_group(0);
            Ok(None)
        }
    })();
    let (mut terminal_reader, mut terminal_writer) = match terminal_io {
        Ok(Some((reader, writer))) => (Some(reader), Some(writer)),
        Ok(None) => (None, None),
        Err(error) => {
            drop(input.take());
            events.push(ThreadEvent::SpawnFailed { error: error.to_string() });
            events.push(ThreadEvent::ThreadFinished);
            return;
        }
    };
    #[cfg(unix)]
    resources.configure_command(&mut cmd);

    let spawned = cmd.spawn();
    drop(cmd);
    #[cfg(unix)]
    drop(resources);
    match spawned {
        Ok(spawned_child) => {
            *child = Some(spawned_child);
            let spawned_child = child.as_mut().expect("spawned child is owned");
            let child_pid = spawned_child.id();
            events.push(ThreadEvent::Started { pid: child_pid });
            let readers = (|| -> io::Result<()> {
                if let Some(reader) = terminal_reader.take() {
                    output_threads.push(spawn_output_reader(blocking, reader, events.clone(), false)?);
                }
                if let Some(stdout) = spawned_child.stdout.take() {
                    output_threads.push(spawn_output_reader(
                        blocking,
                        stdout,
                        events.clone(),
                        false,
                    )?);
                }
                if let Some(stderr) = spawned_child.stderr.take() {
                    output_threads.push(spawn_output_reader(
                        blocking,
                        stderr,
                        events.clone(),
                        true,
                    )?);
                }
                Ok(())
            })();
            pid = Some(child_pid);
            state = SupervisorState::Running;
            if let Err(error) = readers {
                drop(input.take());
                finish_with_error(
                    &events,
                    &mut state,
                    &mut child,
                    &mut output_threads,
                    format!("failed to start process output reader: {error}"),
                );
                return;
            }
            if let Some(input) = input.take() {
                let writer = if let Some(terminal) = terminal_writer.take() {
                    input.spawn_terminal_writer(blocking, terminal)
                } else {
                    let stdin = child.as_mut().expect("spawned child is owned")
                        .stdin.take().expect("supervisor configures piped stdin");
                    input.spawn_writer(blocking, stdin)
                };
                match writer {
                    Ok(writer) => child.input_writer = Some(writer),
                    Err(error) => {
                        finish_with_error(
                            &events,
                            &mut state,
                            &mut child,
                            &mut output_threads,
                            format!("failed to start process input writer: {error}"),
                        );
                        return;
                    }
                }
            }

            match drain_queued_commands(
                &mut child,
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
            drop(input.take());
            events.push(ThreadEvent::SpawnFailed {
                error: err.to_string(),
            });
            events.push(ThreadEvent::ThreadFinished);
            return;
        }
    }

    let child_pid = pid.expect("supervisor pid stored after successful spawn");
    let child_exit = child_exit_fd(child_pid);
    #[cfg(target_os = "linux")]
    if child_exit.is_none() {
        finish_with_error(
            &events,
            &mut state,
            &mut child,
            &mut output_threads,
            format!("subscribe to child exit: {}", io::Error::last_os_error()),
        );
        return;
    }
    loop {
        let timeout = if child_exit.is_some() {
            kill_deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()))
        } else {
            Some(command_poll_timeout(kill_deadline))
        };
        match poll_command_wake(&wake, child_exit.as_ref().map(AsRawFd::as_raw_fd), timeout) {
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
                    &mut child,
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
    child: &mut ChildOwner,
    commands: &SegQueue<ThreadCommand>,
    state: &mut SupervisorState,
    kill_deadline: &mut Option<Instant>,
    kill_sent: &mut bool,
) -> CommandOutcome {
    while let Some(command) = commands.pop() {
        child.shutdown_terminal_input();
        let pid = child.as_ref().expect("commands follow child creation").id();
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


fn finish_with_exit(
    events: &ThreadEventSink,
    state: &mut SupervisorState,
    child: &mut ChildOwner,
    output_threads: &mut Vec<OwnedIoThread<()>>,
    status: ExitStatus,
) {
    *state = SupervisorState::Done;
    debug_assert!(matches!(*state, SupervisorState::Done));
    // Keep the unreaped leader's group/session identity pinned through cleanup.
    let tree_result = child.kill_owned_tree();
    let reaped = if let Some(mut process) = child.take() {
        process
            .wait()
            .map(|_| ())
            .map_err(|error| format!("reap completed process: {error}"))
    } else {
        Ok(())
    };
    let input_result = child.finish_input();
    join_output_readers(output_threads);
    match tree_result.and(reaped).and(input_result) {
        Ok(()) => events.push(ThreadEvent::Exited { status }),
        Err(error) => events.push(ThreadEvent::Error { error }),
    }
    events.push(ThreadEvent::ThreadFinished);
}

fn finish_with_error(
    events: &ThreadEventSink,
    state: &mut SupervisorState,
    child: &mut ChildOwner,
    output_threads: &mut Vec<OwnedIoThread<()>>,
    error: String,
) {
    *state = SupervisorState::Done;
    debug_assert!(matches!(*state, SupervisorState::Done));
    let _ = child.kill_owned_tree();
    if let Some(child) = child.as_mut() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let _ = child.take();
    let _ = child.finish_input();
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
    // The supervised child leads a private process group. Signalling the group
    // prevents descendants from retaining its output pipes after the leader
    // exits, which would otherwise indefinitely delay terminal observation.
    let target = -(pid as libc::pid_t);
    let ret = unsafe { libc::kill(target, sig) };
    if ret == 0 {
        Ok(())
    } else {
        Err(format!(
            "kill({target}, {sig}) failed: {}",
            std::io::Error::last_os_error()
        ))
    }
}

fn try_wait_pid(pid: u32) -> Result<Option<ExitStatus>, String> {
    loop {
        let mut info: libc::siginfo_t = unsafe { mem::zeroed() };
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result == 0 {
            if unsafe { info.si_pid() } == 0 {
                return Ok(None);
            }
            let status = unsafe { info.si_status() };
            return Ok(Some(match info.si_code {
                libc::CLD_EXITED => ExitStatus::Code(status),
                libc::CLD_KILLED | libc::CLD_DUMPED => ExitStatus::Signal(status),
                _ => ExitStatus::Unknown,
            }));
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(format!("observe process {pid}: {error}"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use swactor::runtime::{RuntimeConfig, RuntimeParts};
    use swactor_engine::{Engine, SteppingBackend};

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

    #[cfg(target_os = "linux")]
    #[test]
    fn supervisor_panic_reaps_child_before_publishing_terminal_failure() {
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            SteppingBackend::new(),
        )
        .unwrap();
        let (wake, ready) = std::sync::mpsc::channel();
        let first = std::sync::atomic::AtomicBool::new(true);
        let (events, receiver) = thread_event_channel(move || {
            if first.swap(false, Ordering::SeqCst) {
                panic!("startup observer failure");
            }
            let _ = wake.send(());
        });
        let owner = ProcessSupervisorThread::start(
            &engine.handle().blocking_work_sender(),
            shell_spec("kill -STOP $$"),
            ProcessSpawnResources::default(),
            None,
            events,
        )
        .unwrap();
        let mut observed = Vec::new();
        loop {
            ready.recv().unwrap();
            observed.extend(receiver.drain());
            if observed
                .iter()
                .any(|event| matches!(event, ThreadEvent::ThreadFinished))
            {
                break;
            }
        }
        drop(owner);
        let pid = observed
            .iter()
            .find_map(|event| match event {
                ThreadEvent::Started { pid } => Some(*pid),
                _ => None,
            })
            .expect("child started before injected observer panic");
        assert_eq!(unsafe { libc::kill(pid as i32, 0) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
        assert_eq!(
            observed
                .iter()
                .filter(|event| matches!(event, ThreadEvent::Error { .. }))
                .count(),
            1
        );
        assert!(
            !observed
                .iter()
                .any(|event| matches!(event, ThreadEvent::Exited { .. }))
        );
        assert_eq!(engine.handle().ownership_snapshot().blocking, 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn supervisor_completes_inside_the_only_blocking_pool_worker() {
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
        let engine_handle = engine.handle();
        let blocking = engine_handle.blocking_work_sender();
        let supervisor_work = blocking.clone();
        let events = blocking
            .submit_joinable(move || {
                let (wake, ready) = std::sync::mpsc::channel();
                let (events, receiver) = thread_event_channel(move || {
                    let _ = wake.send(());
                });
                let owner = ProcessSupervisorThread::start(
                    &supervisor_work,
                    shell_spec("printf 'stdout'; printf 'stderr' >&2; exit 7"),
                    ProcessSpawnResources::default(),
                    None,
                    events,
                )
                .unwrap();
                let mut observed = Vec::new();
                loop {
                    ready.recv().unwrap();
                    observed.extend(receiver.drain());
                    if observed
                        .iter()
                        .any(|event| matches!(event, ThreadEvent::ThreadFinished))
                    {
                        break;
                    }
                }
                drop(owner);
                observed
            })
            .unwrap()
            .join()
            .unwrap();
        assert!(events.iter().any(|event| matches!(
            event,
            ThreadEvent::Exited {
                status: ExitStatus::Code(7)
            }
        )));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        for event in events {
            match event {
                ThreadEvent::Output {
                    stderr: false,
                    bytes,
                } => stdout.extend(bytes),
                ThreadEvent::Output {
                    stderr: true,
                    bytes,
                } => stderr.extend(bytes),
                ThreadEvent::Error { error } | ThreadEvent::SpawnFailed { error } => {
                    panic!("{error}")
                }
                _ => {}
            }
        }
        assert_eq!(stdout, b"stdout");
        assert_eq!(stderr, b"stderr");
        assert_eq!(engine_handle.ownership_snapshot().blocking, 0);
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
            poll_command_wake(&wake, None, Some(Duration::ZERO)).expect("wake poll should succeed"),
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
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            SteppingBackend::new(),
        )
        .expect("process test engine");
        let mut handle = ProcessSupervisorThread::start(
            &engine.handle().blocking_work_sender(),
            shell_spec("exit 7"),
            ProcessSpawnResources::new(),
            None,
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
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            SteppingBackend::new(),
        )
        .expect("process test engine");
        let mut handle = ProcessSupervisorThread::start(
            &engine.handle().blocking_work_sender(),
            spec("/definitely/not/a/real/binary", vec![]),
            ProcessSpawnResources::new(),
            None,
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
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            SteppingBackend::new(),
        )
        .expect("process test engine");
        let mut handle = ProcessSupervisorThread::start(
            &engine.handle().blocking_work_sender(),
            shell_spec("echo stdout; echo stderr >&2; exit 0"),
            ProcessSpawnResources::new(),
            None,
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
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            SteppingBackend::new(),
        )
        .expect("process test engine");
        let mut handle = ProcessSupervisorThread::start(
            &engine.handle().blocking_work_sender(),
            shell_spec("trap '' TERM; printf 'ready\\n'; while true; do sleep 1; done"),
            ProcessSpawnResources::new(),
            None,
            sink,
        )
        .expect("supervisor should start");
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut events = Vec::new();
        let mut stop_sent = false;

        while Instant::now() < deadline {
            events.extend(receiver.drain());
            if !stop_sent
                && events.iter().any(
                    |event| matches!(event, ThreadEvent::Output { stderr: false, bytes } if bytes == b"ready\n"),
                )
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
            "supervisor process did not report readiness before timeout"
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
