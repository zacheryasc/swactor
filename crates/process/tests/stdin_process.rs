#![cfg(any(target_os = "linux", windows))]

use std::collections::HashMap;
use std::io::{self, BufRead, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::pin::Pin;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::{ExternalSender, Inbox, Runtime, RuntimeConfig, RuntimeParts, SingleThreadRuntime};
use swactor_engine::{BlockingWorkSender, Engine, OwnedIoThread, SteppingBackend};
use swactor_process::{
    ExitStatus, ProcessCommand, ProcessOutput, ProcessOutputConfig, ProcessSpec, ProcessStdin,
    TerminalInput, TerminalSize, command_spawn, send_process_command, spawn_local_process,
    spawn_local_process_with_stdin, spawn_local_terminal,
};

const ROLE: &str = "SWACTOR_STDIN_PROCESS_TEST_ROLE";
const CONTROL: &str = "SWACTOR_STDIN_PROCESS_TEST_CONTROL";
const BEGIN: &[u8] = b"STDIN_CHILD_BEGIN\n";
const MAX_WRITE: usize = 16 * 1024;
const DEADLINE: Duration = Duration::from_secs(20);

#[derive(Clone, Copy)]
enum InputMode {
    Null,
    Piped,
    Terminal(TerminalSize),
    #[cfg(target_os = "linux")]
    Resources,
}

struct Spawned {
    address: ActorAddress,
    input: Option<ProcessStdin>,
    terminal: Option<TerminalInput>,
}

#[derive(Clone)]
struct Spawn {
    spec: ProcessSpec,
    mode: InputMode,
    sender: ExternalSender,
    output: ActorAddress,
    reply: Arc<Mutex<Option<Result<Spawned, String>>>>,
}

struct Spawner(BlockingWorkSender);

impl ActorInterface for Spawner {
    type Incoming = Spawn;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, request: Spawn) {
        let output = ProcessOutputConfig::disabled(request.output);
        let result = match request.mode {
            InputMode::Piped => spawn_local_process_with_stdin(
                &self.0, ctx, &request.sender, request.spec, output,
            ).map(|(address, input)| Spawned { address, input: Some(input), terminal: None }),
            InputMode::Null => spawn_local_process(
                &self.0, ctx, &request.sender, request.spec, output,
            ).map(|address| Spawned { address, input: None, terminal: None }),
            #[cfg(target_os = "linux")]
            InputMode::Resources => swactor_process::spawn_local_process_with_resources(
                &self.0, ctx, &request.sender, request.spec,
                swactor_process::ProcessSpawnResources::new(), output,
            ).map(|address| Spawned { address, input: None, terminal: None }),
            InputMode::Terminal(size) => spawn_local_terminal(
                &self.0, ctx, &request.sender, request.spec, size, output,
            ).map(|(address, terminal)| Spawned { address, input: None, terminal: Some(terminal) }),
        }.map_err(|error| error.to_string());
        *request.reply.lock().unwrap() = Some(result);
        ctx.stop_self();
    }
}

fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

// Retain OS identities before asking the owner to terminate them. Neither a PID
// lookup after cancellation nor a sleep establishes that the owned child died.
struct ProcessIdentity {
    #[cfg(target_os = "linux")]
    handle: std::os::fd::OwnedFd,
    #[cfg(windows)]
    handle: std::os::windows::io::OwnedHandle,
}

impl ProcessIdentity {
    fn open(pid: u32) -> Self {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::FromRawFd;
            let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
            assert!(fd >= 0, "pidfd_open({pid}): {}", io::Error::last_os_error());
            Self { handle: unsafe { std::os::fd::OwnedFd::from_raw_fd(fd as i32) } }
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::FromRawHandle;
            use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_SYNCHRONIZE, PROCESS_TERMINATE};
            let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE | PROCESS_TERMINATE, 0, pid) };
            assert!(!handle.is_null(), "OpenProcess({pid}): {}", io::Error::last_os_error());
            Self { handle: unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(handle) } }
        }
    }

    fn exited(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let mut descriptor = libc::pollfd {
                fd: self.handle.as_raw_fd(), events: libc::POLLIN, revents: 0,
            };
            let result = unsafe { libc::poll(&mut descriptor, 1, 0) };
            if result < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                return false;
            }
            assert!(result >= 0, "poll pidfd: {}", io::Error::last_os_error());
            descriptor.revents & (libc::POLLIN | libc::POLLHUP) != 0
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT};
            use windows_sys::Win32::System::Threading::WaitForSingleObject;
            match unsafe { WaitForSingleObject(self.handle.as_raw_handle(), 0) } {
                WAIT_OBJECT_0 => true,
                WAIT_TIMEOUT => false,
                other => panic!("wait process handle: {other:#x}, {}", io::Error::last_os_error()),
            }
        }
    }

    fn kill(&self) {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal, self.handle.as_raw_fd(), libc::SIGKILL,
                    std::ptr::null::<libc::siginfo_t>(), 0,
                );
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            unsafe {
                windows_sys::Win32::System::Threading::TerminateProcess(self.handle.as_raw_handle(), 99);
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn reap_adopted_descendant(&self) {
        use std::os::fd::AsRawFd;
        let mut status: libc::siginfo_t = unsafe { std::mem::zeroed() };
        loop {
            let result = unsafe {
                libc::waitid(libc::P_PIDFD, self.handle.as_raw_fd() as u32, &mut status, libc::WEXITED)
            };
            if result == 0 {
                assert_ne!(unsafe { status.si_pid() }, 0, "descendant was not reaped");
                return;
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                panic!("reap exact adopted descendant: {error}");
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn assert_leader_reaped(&self) {
        use std::os::fd::AsRawFd;
        let mut status: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let result = unsafe {
            libc::waitid(
                libc::P_PIDFD, self.handle.as_raw_fd() as u32, &mut status,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        assert_eq!(result, -1, "supervisor left its leader unreaped");
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ECHILD));
    }
}

fn adopt_orphaned_descendants() {
    #[cfg(target_os = "linux")]
    {
        // Process-wide, idempotent, and never reset while another test might own
        // an adopted child. Each test waits only on its retained exact pidfd.
        assert_eq!(unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) }, 0);
    }
}

// The ordinary deadlines diagnose failures. This independent fuse also covers
// a regression that blocks host.tick() while joining a stuck stdin writer.
struct Fuse {
    done: Option<mpsc::Sender<()>>,
    join: Option<OwnedIoThread<()>>,
    children: Arc<Mutex<Vec<Arc<ProcessIdentity>>>>,
}

impl Fuse {
    fn new(blocking: &BlockingWorkSender) -> Self {
        let (done, receiver) = mpsc::channel();
        let children = Arc::new(Mutex::new(Vec::<Arc<ProcessIdentity>>::new()));
        let owned = Arc::clone(&children);
        let join = blocking.spawn_owned_thread("stdin-test-fuse", move || {
            if matches!(receiver.recv_timeout(Duration::from_secs(60)), Err(mpsc::RecvTimeoutError::Timeout)) {
                let children = owned.lock().unwrap();
                for child in children.iter().rev() {
                    child.kill();
                }
                let deadline = Instant::now() + Duration::from_secs(5);
                while children.iter().any(|child| !child.exited()) && Instant::now() < deadline {
                    std::thread::yield_now();
                }
                #[cfg(target_os = "linux")]
                for child in children.iter().rev() {
                    use std::os::fd::AsRawFd;
                    let mut status: libc::siginfo_t = unsafe { std::mem::zeroed() };
                    unsafe {
                        libc::waitid(
                            libc::P_PIDFD, child.handle.as_raw_fd() as u32, &mut status,
                            libc::WEXITED | libc::WNOHANG,
                        );
                    }
                }
                eprintln!("stdin process contract exceeded its hard fuse; cleaned retained child identities");
                std::process::abort();
            }
        }).unwrap();
        Self { done: Some(done), join: Some(join), children }
    }

    fn track(&self, pid: u32) -> Arc<ProcessIdentity> {
        let process = Arc::new(ProcessIdentity::open(pid));
        self.children.lock().unwrap().push(Arc::clone(&process));
        process
    }
}

impl Drop for Fuse {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
        if let Some(join) = self.join.take() { let _ = join.join(); }
    }
}

struct Harness {
    runtime: Runtime,
    host: SingleThreadRuntime,
    output: Inbox<ProcessOutput>,
    events: Vec<ProcessOutput>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    process: Option<ActorAddress>,
    terminal_input: Option<TerminalInput>,
    fuse: Fuse,
    engine: Engine,
}

impl Harness {
    fn spawn(spec: ProcessSpec, mode: InputMode) -> (Self, Option<ProcessStdin>) {
        let engine = Engine::new(RuntimeParts::new(RuntimeConfig::default()), SteppingBackend::new()).unwrap();
        let fuse = Fuse::new(&engine.handle().blocking_work_sender());
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let host = SingleThreadRuntime::new(parts);
        let output = runtime.new_inbox::<ProcessOutput>().unwrap();
        let reply = Arc::new(Mutex::new(None));
        let spawner = runtime.spawn(Spawner(engine.handle().blocking_work_sender())).unwrap();
        runtime.send_to(spawner, Spawn {
            spec, mode, sender: runtime.create_sender(), output: *output.addr(), reply: Arc::clone(&reply),
        }).unwrap();
        let mut harness = Self {
            runtime, host, output, events: Vec::new(), stdout: Vec::new(), stderr: Vec::new(),
            process: None, terminal_input: None, engine, fuse,
        };
        let deadline = Instant::now() + DEADLINE;
        let spawned = loop {
            harness.tick();
            if let Some(result) = reply.lock().unwrap().take() {
                break result.unwrap_or_else(|error| panic!("spawn process actor: {error}"));
            }
            assert!(Instant::now() < deadline, "spawner did not hand off input");
        };
        harness.process = Some(spawned.address);
        harness.terminal_input = spawned.terminal;
        (harness, spawned.input)
    }

    fn piped(spec: ProcessSpec) -> (Self, ProcessStdin) {
        let (harness, input) = Self::spawn(spec, InputMode::Piped);
        (harness, input.unwrap())
    }

    fn native_terminal(spec: ProcessSpec) -> (Self, TerminalInput) {
        let (mut harness, _) = Self::spawn(spec, InputMode::Terminal(TerminalSize { rows: 25, cols: 90 }));
        let terminal = harness.terminal_input.take().unwrap();
        (harness, terminal)
    }

    fn tick(&mut self) {
        self.host.tick();
        while let Some(event) = self.output.try_recv() {
            match &event {
                ProcessOutput::Stdout(bytes) => self.stdout.extend_from_slice(bytes),
                ProcessOutput::Stderr(bytes) => self.stderr.extend_from_slice(bytes),
                _ => {}
            }
            self.events.push(event);
        }
        std::thread::yield_now();
    }

    fn wait(&mut self, description: &str, ready: impl Fn(&Self) -> bool) {
        let deadline = Instant::now() + DEADLINE;
        while !ready(self) {
            self.tick();
            assert!(Instant::now() < deadline, "{description} timed out; stdout={:?}; stderr={:?}",
                String::from_utf8_lossy(&self.stdout), String::from_utf8_lossy(&self.stderr));
        }
    }

    fn complete<F: Future>(&mut self, future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        let deadline = Instant::now() + DEADLINE;
        loop {
            if let Poll::Ready(result) = poll_once(future.as_mut()) {
                return result;
            }
            self.tick();
            assert!(Instant::now() < deadline, "stdin acknowledgment did not settle");
        }
    }

    fn marker(&mut self, marker: &[u8]) {
        self.wait("child output marker", |h| contains(&h.stdout, marker));
    }

    fn started_pid(&mut self) -> u32 {
        self.wait("Started", |h| h.events.iter().any(|event| matches!(event, ProcessOutput::Started { .. })));
        self.events.iter().find_map(|event| match event {
            ProcessOutput::Started { pid } => Some(*pid),
            _ => None,
        }).unwrap()
    }

    fn track_leader(&mut self) -> Arc<ProcessIdentity> {
        let pid = self.started_pid();
        self.fuse.track(pid)
    }

    fn cancel(&self) {
        send_process_command(&self.runtime.create_sender(), self.process.unwrap(), ProcessCommand::Cancel).unwrap();
    }

    fn terminal(&mut self) {
        self.wait("terminal event", |h| h.events.iter().any(is_terminal));
        // The sole remaining owned thread is the independent test fuse.
        self.wait("all owned process threads joined", |h| h.engine.handle().ownership_snapshot().blocking == 1);
        assert_eq!(self.events.iter().filter(|event| is_terminal(event)).count(), 1);
        assert!(is_terminal(self.events.last().unwrap()), "output arrived after terminal event");
    }

    fn exited(&mut self, code: i32) {
        self.terminal();
        assert_eq!(self.events.last(), Some(&ProcessOutput::Exited { status: ExitStatus::Code(code) }));
        assert!(self.events.iter().any(|event| matches!(event, ProcessOutput::Started { .. })));
        assert!(!self.events.iter().any(|event| matches!(event, ProcessOutput::Error { .. } | ProcessOutput::SpawnFailed { .. })));
    }

    fn reaped(&mut self, process: &ProcessIdentity) {
        self.wait("exact child process exit", |_| process.exited());
        #[cfg(target_os = "linux")]
        process.assert_leader_reaped();
    }

    fn child_stdout(&self) -> &[u8] {
        let start = self.stdout.windows(BEGIN.len()).position(|bytes| bytes == BEGIN).expect("child begin marker");
        &self.stdout[start + BEGIN.len()..]
    }

    fn capacity(&mut self) -> usize {
        self.wait("pipe-capacity handshake", |h| line_value(&h.stdout, "CAPACITY=").is_some());
        let capacity: usize = line_value(&self.stdout, "CAPACITY=").unwrap().parse().unwrap();
        assert!((1..=1024 * 1024).contains(&capacity), "unexpected pipe capacity {capacity}");
        capacity
    }

    fn fill_pipe(&mut self, input: &mut ProcessStdin, capacity: usize) {
        for offset in (0..capacity).step_by(MAX_WRITE) {
            self.complete(input.write_all(vec![0xa5; (capacity - offset).min(MAX_WRITE)])).unwrap();
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("stdin child failure: stdout={:?}; stderr={:?}",
                String::from_utf8_lossy(&self.stdout), String::from_utf8_lossy(&self.stderr));
        }
        if let Some(process) = self.process {
            let _ = send_process_command(&self.runtime.create_sender(), process, ProcessCommand::Cancel);
            let _ = self.runtime.stop_actor(process);
            // on_stop drops the supervisor owner and must cancel before joining.
            self.host.tick();
        }
        let children = self.fuse.children.lock().unwrap();
        for child in children.iter().rev() {
            child.kill();
        }
        #[cfg(target_os = "linux")]
        for child in children.iter().rev() {
            use std::os::fd::AsRawFd;
            let mut status: libc::siginfo_t = unsafe { std::mem::zeroed() };
            // ECHILD is expected for leaders already reaped by their owner and
            // descendants explicitly reaped by the assertion path.
            unsafe {
                libc::waitid(
                    libc::P_PIDFD, child.handle.as_raw_fd() as u32, &mut status,
                    libc::WEXITED | libc::WNOHANG,
                );
            }
        }
    }
}

fn contains(bytes: &[u8], expected: &[u8]) -> bool {
    bytes.windows(expected.len()).any(|window| window == expected)
}

fn line_value(bytes: &[u8], prefix: &str) -> Option<String> {
    // A partial last line is not a handshake.
    bytes.split_inclusive(|byte| *byte == b'\n').filter(|line| line.ends_with(b"\n"))
        .filter_map(|line| std::str::from_utf8(line).ok())
        .find_map(|line| line.trim_end_matches(['\r', '\n']).strip_prefix(prefix).map(str::to_owned))
}

fn is_terminal(event: &ProcessOutput) -> bool {
    matches!(event, ProcessOutput::Exited { .. } | ProcessOutput::SpawnFailed { .. } | ProcessOutput::Error { .. })
}

fn child_spec(role: &str) -> ProcessSpec {
    ProcessSpec {
        command: std::env::current_exe().unwrap().to_string_lossy().into_owned(),
        args: vec!["--exact".to_owned(), "stdin_child".to_owned(), "--nocapture".to_owned()],
        env: HashMap::from([(ROLE.to_owned(), role.to_owned())]),
        working_dir: None,
        label: None,
    }
}

fn child_output(bytes: &[u8]) {
    let mut stdout = io::stdout().lock();
    stdout.write_all(bytes).unwrap();
    stdout.flush().unwrap();
}

fn child_error(bytes: &[u8]) {
    let mut stderr = io::stderr().lock();
    stderr.write_all(bytes).unwrap();
    stderr.flush().unwrap();
}

fn pipe_capacity() -> usize {
    #[cfg(target_os = "linux")]
    {
        // A one-page pipe makes saturation independent of the machine's default
        // pipe size. The child does not consume any bytes before announcing it.
        assert!(unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_SETPIPE_SZ, 4096) } > 0);
        let capacity = unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_GETPIPE_SZ) };
        assert!(capacity > 0);
        capacity as usize
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Console::{GetStdHandle, STD_INPUT_HANDLE};
        use windows_sys::Win32::System::Pipes::GetNamedPipeInfo;
        let mut output = 0;
        let mut input = 0;
        assert_ne!(unsafe {
            GetNamedPipeInfo(GetStdHandle(STD_INPUT_HANDLE), std::ptr::null_mut(), &mut output, &mut input, std::ptr::null_mut())
        }, 0, "query actual stdin pipe capacity: {}", io::Error::last_os_error());
        output.max(input) as usize
    }
}

struct ExitControl(TcpListener);

impl ExitControl {
    fn new(spec: &mut ProcessSpec) -> Self {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        spec.env.insert(CONTROL.to_owned(), listener.local_addr().unwrap().to_string());
        Self(listener)
    }

    fn release(&self, harness: &mut Harness) {
        let deadline = Instant::now() + DEADLINE;
        loop {
            match self.0.accept() {
                Ok((mut stream, _)) => {
                    stream.set_write_timeout(Some(DEADLINE)).unwrap();
                    stream.write_all(b"exit").unwrap();
                    return;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => harness.tick(),
                Err(error) => panic!("accept child control: {error}"),
            }
            assert!(Instant::now() < deadline, "child did not connect its exit control");
        }
    }
}

#[test]
fn stdin_child() {
    let Ok(role) = std::env::var(ROLE) else { return; };
    child_output(BEGIN);
    match role.as_str() {
        "terminal-size" => {
            use std::io::IsTerminal;
            assert!(io::stdin().is_terminal() && io::stdout().is_terminal() && io::stderr().is_terminal());
            assert_eq!(std::env::var("SWACTOR_TERMINAL_ENV").unwrap(), "value with spaces = 日本語");
            assert_eq!(std::env::current_dir().unwrap().canonicalize().unwrap(),
                std::path::PathBuf::from(std::env::var_os("SWACTOR_TERMINAL_CWD").unwrap()).canonicalize().unwrap());
            let size = native_terminal_size();
            child_output(format!("TERMINAL_SIZE={},{}\n", size.rows, size.cols).as_bytes());
            let mut line = String::new();
            io::stdin().read_line(&mut line).unwrap();
            assert_eq!(line.trim(), "resized");
            let size = native_terminal_size();
            child_output(format!("TERMINAL_RESIZED={},{}\n", size.rows, size.cols).as_bytes());
            child_error(b"TERMINAL_MERGED_STDERR\n");
            std::process::exit(23);
        }
        "terminal-tree" => {
            let spec = child_spec("descendant");
            let mut command = Command::new(spec.command);
            command.args(spec.args).envs(spec.env);
            #[cfg(target_os = "linux")]
            {
                use std::os::unix::process::CommandExt;
                // Reproduce shell job control: a different PG in our session.
                command.process_group(0);
            }
            let _child = command_spawn(&mut command).unwrap();
            child_output(b"TERMINAL_TREE_READY\n");
            loop { std::thread::park(); }
        }
        "terminal-interrupt" => {
            child_output(format!("INTERRUPT_READY={}\n", std::process::id()).as_bytes());
            loop { std::thread::park(); }
        }
        "terminal-idle" => {
            child_output(b"TERMINAL_IDLE_READY\n");
            let mut control = TcpStream::connect(std::env::var(CONTROL).unwrap()).unwrap();
            let mut command = [0; 4];
            control.read_exact(&mut command).unwrap();
            assert_eq!(&command, b"exit");
            std::process::exit(23);
        }
        #[cfg(target_os = "linux")]
        "terminal-blocked" => {
            let mut settings: libc::termios = unsafe { std::mem::zeroed() };
            assert_eq!(unsafe { libc::tcgetattr(0, &mut settings) }, 0);
            unsafe { libc::cfmakeraw(&mut settings) };
            assert_eq!(unsafe { libc::tcsetattr(0, libc::TCSANOW, &settings) }, 0);
            child_output(b"TERMINAL_RAW_READY\n");
            let mut control = TcpStream::connect(std::env::var(CONTROL).unwrap()).unwrap();
            let mut command = [0; 4];
            control.read_exact(&mut command).unwrap();
            assert_eq!(&command, b"exit");
            std::process::exit(23);
        }
        "default" => {
            let mut bytes = Vec::new();
            io::stdin().read_to_end(&mut bytes).unwrap();
            assert!(bytes.is_empty(), "default stdin was not null/EOF");
            child_output(b"DEFAULT_EOF\n");
            child_error(b"default stderr\n");
            std::process::exit(17);
        }
        "interactive" => {
            child_output(b"READY\n");
            let mut input = io::stdin().lock();
            let mut first = [0; 6];
            input.read_exact(&mut first).unwrap();
            assert_eq!(&first, b"first\n");
            child_output(b"ACK:first\n");
            child_error(b"ERR:first\n");
            let mut second = [0; 7];
            input.read_exact(&mut second).unwrap();
            assert_eq!(&second, b"second\n");
            child_output(b"ACK:second\n");
            child_error(b"ERR:second\n");
            let mut rest = Vec::new();
            input.read_to_end(&mut rest).unwrap();
            assert!(rest.is_empty());
            child_output(b"EOF\n");
            std::process::exit(23);
        }
        "binary" | "final-write" | "eof-survivor" => {
            let capacity = pipe_capacity();
            child_output(format!("CAPACITY={capacity}\n").as_bytes());
            let mut bytes = Vec::new();
            io::stdin().read_to_end(&mut bytes).unwrap();
            child_output(b"PAYLOAD\n");
            child_output(&bytes);
            if role == "eof-survivor" {
                child_output(b"\nEOF_ALIVE\n");
                loop { std::thread::park(); }
            }
            child_error(b"\0\xffbinary-stderr\n");
            std::process::exit(37);
        }
        "exit-controlled" | "never-read" | "tree" => {
            let capacity = pipe_capacity();
            if role == "tree" {
                let spec = child_spec("descendant");
                let mut command = Command::new(spec.command);
                command.args(spec.args).envs(spec.env)
                    .stdin(Stdio::inherit()).stdout(Stdio::piped()).stderr(Stdio::inherit());
                let mut descendant = command_spawn(&mut command).unwrap();
                let mut output = io::BufReader::new(descendant.stdout.take().unwrap());
                loop {
                    let mut line = String::new();
                    assert_ne!(output.read_line(&mut line).unwrap(), 0, "descendant exited before handshake");
                    if line.trim_end() == format!("DESCENDANT={}", descendant.id()) {
                        child_output(line.as_bytes());
                        break;
                    }
                }
                // The descendant keeps inherited stdin open and belongs to the
                // leader's group/Job. Only managed tree cancellation releases it.
            }
            child_output(format!("CAPACITY={capacity}\nREADY\n").as_bytes());
            if role == "exit-controlled" {
                let mut control = TcpStream::connect(std::env::var(CONTROL).unwrap()).unwrap();
                let mut command = [0; 4];
                control.read_exact(&mut command).unwrap();
                assert_eq!(&command, b"exit");
                child_output(b"EXITING\n");
                std::process::exit(23);
            }
            loop { std::thread::park(); }
        }
        "descendant" => {
            child_output(format!("DESCENDANT={}\n", std::process::id()).as_bytes());
            loop { std::thread::park(); }
        }
        #[cfg(windows)]
        "handles" => windows_handle_diagnostic_child(),
        other => panic!("unknown stdin child role {other}"),
    }
}

#[test]
fn default_spawn_keeps_null_stdin_and_existing_apis() {
    let modes = [InputMode::Null, #[cfg(target_os = "linux")] InputMode::Resources];
    for mode in modes {
        let (mut harness, input) = Harness::spawn(child_spec("default"), mode);
        assert!(input.is_none());
        harness.exited(17);
        assert_eq!(harness.child_stdout(), b"DEFAULT_EOF\n");
        assert_eq!(harness.stderr, b"default stderr\n");
    }
}

#[test]
fn interactive_stdin_is_fifo_and_outputs_before_eof() {
    let (mut harness, mut input) = Harness::piped(child_spec("interactive"));
    let _leader = harness.track_leader();
    harness.marker(b"READY\n");
    harness.complete(input.write_all(Vec::new())).unwrap();
    harness.complete(input.write_all(b"first\n".to_vec())).unwrap();
    harness.wait("first response before second command", |h| {
        contains(&h.stdout, b"ACK:first\n") && contains(&h.stderr, b"ERR:first\n")
    });
    assert!(!harness.events.iter().any(is_terminal));
    harness.complete(input.write_all(b"second\n".to_vec())).unwrap();
    harness.wait("second response before EOF", |h| {
        contains(&h.stdout, b"ACK:second\n") && contains(&h.stderr, b"ERR:second\n")
    });
    assert!(!harness.events.iter().any(is_terminal));
    harness.complete(input.close()).unwrap();
    harness.exited(23);
    assert_eq!(harness.child_stdout(), b"READY\nACK:first\nACK:second\nEOF\n");
    assert_eq!(harness.stderr, b"ERR:first\nERR:second\n");
}

#[test]
fn binary_stdin_larger_than_pipe_capacity_is_exact() {
    let (mut harness, mut input) = Harness::piped(child_spec("binary"));
    let _leader = harness.track_leader();
    let capacity = harness.capacity();
    let payload: Vec<u8> = (0..capacity * 4 + MAX_WRITE + 7919)
        .map(|index| (index as u8).wrapping_mul(197).wrapping_add((index >> 8) as u8)).collect();
    for chunk in payload.chunks(MAX_WRITE) {
        harness.complete(input.write_all(chunk.to_vec())).unwrap();
    }
    harness.complete(input.close()).unwrap();
    harness.exited(37);
    let prefix = format!("CAPACITY={capacity}\nPAYLOAD\n");
    assert_eq!(harness.child_stdout().strip_prefix(prefix.as_bytes()).unwrap(), payload);
    assert_eq!(harness.stderr, b"\0\xffbinary-stderr\n");
}

#[test]
fn stdin_limits_close_idempotence_and_final_write() {
    let (mut harness, mut input) = Harness::piped(child_spec("final-write"));
    let _leader = harness.track_leader();
    let capacity = harness.capacity();
    assert_eq!(harness.complete(input.write_all(vec![0xfe; MAX_WRITE + 1])).unwrap_err().kind(), io::ErrorKind::InvalidInput);
    let final_write = b"last accepted bytes\0\xff\n";
    harness.complete(input.write_all(final_write.to_vec())).unwrap();
    harness.complete(input.close()).unwrap();
    harness.complete(input.close()).unwrap();
    assert_eq!(harness.complete(input.write_all(b"rejected".to_vec())).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(harness.complete(input.write_all(Vec::new())).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    harness.exited(37);
    let prefix = format!("CAPACITY={capacity}\nPAYLOAD\n");
    assert_eq!(harness.child_stdout().strip_prefix(prefix.as_bytes()).unwrap(), final_write);
}

#[test]
fn spawn_failure_and_early_exit_settle_input() {
    for close_first in [false, true] {
        let mut spec = child_spec("default");
        spec.command = std::env::current_exe().unwrap().with_file_name("swactor-stdin-deliberately-missing-executable")
            .to_string_lossy().into_owned();
        let (mut harness, mut input) = Harness::piped(spec);
        if !close_first {
            assert_eq!(harness.complete(input.write_all(b"pending at spawn".to_vec())).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        }
        harness.complete(input.close()).unwrap();
        harness.terminal();
        assert!(matches!(harness.events.last(), Some(ProcessOutput::SpawnFailed { .. })));
        assert!(!harness.events.iter().any(|event| matches!(event, ProcessOutput::Started { .. })));
    }

    for pending_close in [false, true] {
        let mut spec = child_spec("exit-controlled");
        let control = ExitControl::new(&mut spec);
        let (mut harness, mut input) = Harness::piped(spec);
        let capacity = harness.capacity();
        let leader = harness.track_leader();
        harness.fill_pipe(&mut input, capacity);
        if pending_close {
            {
                let mut accepted = std::pin::pin!(input.write_all(vec![0xb7; MAX_WRITE]));
                assert!(poll_once(accepted.as_mut()).is_pending());
            }
            let mut pending = std::pin::pin!(input.close());
            assert!(poll_once(pending.as_mut()).is_pending());
            control.release(&mut harness);
            harness.complete(pending).unwrap();
        } else {
            let mut pending = std::pin::pin!(input.write_all(vec![0xb7; MAX_WRITE]));
            assert!(poll_once(pending.as_mut()).is_pending(), "full pipe unexpectedly accepted a complete write");
            control.release(&mut harness);
            assert_eq!(harness.complete(pending).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        }
        harness.complete(input.close()).unwrap();
        harness.exited(23);
        harness.reaped(&leader);
    }
}

#[test]
fn saturated_cancel_reaps_child_tree_and_joins_writer() {
    adopt_orphaned_descendants();
    let (mut harness, mut input) = Harness::piped(child_spec("tree"));
    let capacity = harness.capacity();
    let leader = harness.track_leader();
    let descendant_pid: u32 = line_value(&harness.stdout, "DESCENDANT=").unwrap().parse().unwrap();
    let descendant = harness.fuse.track(descendant_pid);
    assert!(!leader.exited() && !descendant.exited());
    harness.fill_pipe(&mut input, capacity);
    {
        let mut pending = std::pin::pin!(input.write_all(vec![0xc3; MAX_WRITE]));
        assert!(poll_once(pending.as_mut()).is_pending());
        harness.cancel();
        assert_eq!(harness.complete(pending).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    }
    harness.terminal();
    assert!(matches!(harness.events.last(), Some(ProcessOutput::Exited { .. })));
    harness.reaped(&leader);
    harness.wait("exact owned descendant exit", |_| descendant.exited());
    #[cfg(target_os = "linux")]
    descendant.reap_adopted_descendant();
    harness.complete(input.close()).unwrap();
}

#[test]
fn dropping_stdin_delivers_eof_without_cancelling_child() {
    let (mut harness, mut input) = Harness::piped(child_spec("eof-survivor"));
    let capacity = harness.capacity();
    let leader = harness.track_leader();
    let bytes = b"accepted before dropping the only input handle\0\xff";
    {
        // The empty request queue admits this request on its first poll. Drop
        // only its acknowledgment future, then the sender: accepted data drains.
        let mut accepted = std::pin::pin!(input.write_all(bytes.to_vec()));
        if let Poll::Ready(result) = poll_once(accepted.as_mut()) {
            result.unwrap();
        }
    }
    drop(input);
    harness.marker(b"\nEOF_ALIVE\n");
    let mut expected = format!("CAPACITY={capacity}\nPAYLOAD\n").into_bytes();
    expected.extend_from_slice(bytes);
    expected.extend_from_slice(b"\nEOF_ALIVE\n");
    assert_eq!(harness.child_stdout(), expected);
    assert!(!leader.exited(), "dropping stdin cancelled a healthy process");
    assert!(!harness.events.iter().any(is_terminal));
    harness.cancel();
    harness.terminal();
    harness.reaped(&leader);
}

#[test]
fn dropping_process_owner_settles_saturated_input() {
    let (mut harness, mut input) = Harness::piped(child_spec("never-read"));
    let capacity = harness.capacity();
    let leader = harness.track_leader();
    harness.fill_pipe(&mut input, capacity);
    {
        let mut accepted = std::pin::pin!(input.write_all(vec![0xd1; MAX_WRITE]));
        assert!(poll_once(accepted.as_mut()).is_pending());
        // Abandon one accepted acknowledgment, leaving real writer work behind.
    }
    {
        let mut pending = std::pin::pin!(input.write_all(vec![0xe2; MAX_WRITE]));
        assert!(poll_once(pending.as_mut()).is_pending());
        harness.runtime.stop_actor(harness.process.unwrap()).unwrap();
        assert_eq!(harness.complete(pending).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    }
    harness.wait("dropped owner joined every process thread", |h| h.engine.handle().ownership_snapshot().blocking == 1);
    harness.reaped(&leader);
    harness.complete(input.close()).unwrap();
    assert_eq!(harness.complete(input.write_all(vec![1])).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
}

#[test]
fn idle_retained_stdin_does_not_block_exit_or_cancel() {
    for cancel in [false, true] {
        let mut spec = child_spec("exit-controlled");
        let control = ExitControl::new(&mut spec);
        let (mut harness, mut input) = Harness::piped(spec);
        harness.marker(b"READY\n");
        let leader = harness.track_leader();
        // No write or close request has ever been made; the writer is idle in
        // receive and this live handle must not be needed to unblock its join.
        if cancel {
            harness.cancel();
            harness.terminal();
            assert!(matches!(harness.events.last(), Some(ProcessOutput::Exited { .. })));
        } else {
            control.release(&mut harness);
            harness.exited(23);
        }
        harness.reaped(&leader);
        assert_eq!(harness.complete(input.write_all(vec![1])).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        harness.complete(input.close()).unwrap();
        harness.complete(input.close()).unwrap();
    }
}

#[cfg(windows)]
fn windows_handle_diagnostic_child() -> ! {
    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::Storage::FileSystem::{FILE_TYPE_PIPE, GetFileType};
    use windows_sys::Win32::System::Console::{GetConsoleMode, GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE};
    for (name, which) in [("stdin", STD_INPUT_HANDLE), ("stdout", STD_OUTPUT_HANDLE), ("stderr", STD_ERROR_HANDLE)] {
        let handle = unsafe { GetStdHandle(which) };
        let file_type = unsafe { GetFileType(handle) };
        let mut mode = 0;
        let console = unsafe { GetConsoleMode(handle, &mut mode) };
        let error = io::Error::last_os_error();
        child_output(format!("HANDLE {name}: GetStdHandle={handle:p} GetFileType={file_type} GetConsoleMode={console} mode={mode} error={error}\n").as_bytes());
        assert!(!handle.is_null() && handle != INVALID_HANDLE_VALUE, "{name} is not valid");
        assert_eq!(file_type, FILE_TYPE_PIPE, "{name} is not an ordinary pipe");
        assert_eq!(console, 0, "{name} unexpectedly has a console mode");
    }
    std::process::exit(0);
}

#[cfg(windows)]
fn powershell_pipe_session(executable: String) {
    let spec = ProcessSpec {
        command: executable,
        args: ["-NoLogo", "-NoProfile", "-InputFormat", "Text", "-OutputFormat", "Text", "-Command", "-"]
            .into_iter().map(str::to_owned).collect(),
        env: HashMap::new(), working_dir: None, label: Some("powershell-pipe-fixture".to_owned()),
    };
    let (mut harness, mut input) = Harness::piped(spec);
    let _leader = harness.track_leader();
    harness.complete(input.write_all(
        b"$global:swactorPipeProof = 'persistent'; Write-Output 'SWACTOR_PS_FIRST'\n".to_vec(),
    )).unwrap();
    harness.marker(b"SWACTOR_PS_FIRST\r\n");
    assert!(!harness.events.iter().any(is_terminal), "PowerShell did not stay interactive");
    harness.complete(input.write_all(concat!(
        "Write-Output ('SWACTOR_PS_SECOND=' + $global:swactorPipeProof); ",
        "Write-Output ('SWACTOR_PS_WINDOWS=' + $IsWindows); ",
        "Write-Output ('SWACTOR_PS_VERSION=' + $PSVersionTable.PSVersion.ToString()); ",
        "Write-Output ('SWACTOR_PS_FRAMEWORK=' + [System.Runtime.InteropServices.RuntimeInformation]::FrameworkDescription); ",
        "Write-Output ('SWACTOR_PS_OS=' + [System.Runtime.InteropServices.RuntimeInformation]::OSDescription); ",
        "Write-Output ('SWACTOR_PS_EXE=' + [Environment]::ProcessPath); ",
        "[Console]::Out.WriteLine('SWACTOR_PS_OUT'); [Console]::Error.WriteLine('SWACTOR_PS_ERR')\n",
    ).as_bytes().to_vec())).unwrap();
    harness.wait("second command and separate streams before EOF", |h| {
        contains(&h.stdout, b"SWACTOR_PS_SECOND=persistent\r\n")
            && contains(&h.stdout, b"SWACTOR_PS_OUT\r\n")
            && contains(&h.stderr, b"SWACTOR_PS_ERR\r\n")
    });
    assert_eq!(line_value(&harness.stdout, "SWACTOR_PS_WINDOWS=").as_deref(), Some("True"));
    assert_eq!(line_value(&harness.stdout, "SWACTOR_PS_VERSION=").as_deref(), Some("7.6.6"));
    let framework = line_value(&harness.stdout, "SWACTOR_PS_FRAMEWORK=").expect("framework fact");
    assert!(framework.starts_with(".NET 10."), "unexpected framework: {framework}");
    let os = line_value(&harness.stdout, "SWACTOR_PS_OS=").expect("Windows OS fact");
    assert!(os.contains("Windows"), "not Windows .NET: {os}");
    let executable = line_value(&harness.stdout, "SWACTOR_PS_EXE=").expect("executable fact");
    assert!(executable.replace('/', "\\").to_ascii_lowercase().ends_with("\\pwsh.exe"), "wrong executable: {executable}");
    assert!(!contains(&harness.stdout, b"SWACTOR_PS_ERR"));
    assert!(!contains(&harness.stderr, b"SWACTOR_PS_OUT"));
    assert!(!harness.events.iter().any(is_terminal));
    harness.complete(input.write_all(b"exit 23\n".to_vec())).unwrap();
    harness.complete(input.close()).unwrap();
    harness.exited(23);
    eprintln!("PowerShell pipe fixture stdout:\n{}\nstderr:\n{}", String::from_utf8_lossy(&harness.stdout), String::from_utf8_lossy(&harness.stderr));
}

#[cfg(windows)]
#[test]
#[ignore = "requires the Windows PowerShell fixture"]
fn windows_powershell_pipe_session() {
    let executable = std::env::var("SWACTOR_POWERSHELL_EXE").expect("SWACTOR_POWERSHELL_EXE must name the real Windows PowerShell fixture");
    assert!(std::path::Path::new(&executable).is_file(), "PowerShell fixture is absent: {executable}");
    if let Err(failure) = std::panic::catch_unwind(|| powershell_pipe_session(executable)) {
        let diagnostic = std::panic::catch_unwind(|| {
            let (mut harness, mut input) = Harness::piped(child_spec("handles"));
            harness.complete(input.close()).unwrap();
            harness.exited(0);
            eprintln!("same-supervisor Windows pipe diagnostic:\n{}", String::from_utf8_lossy(&harness.stdout));
        });
        if diagnostic.is_err() {
            eprintln!("same-supervisor Windows handle diagnostic also failed");
        }
        std::panic::resume_unwind(failure);
    }
}

fn native_terminal_size() -> TerminalSize {
    #[cfg(target_os = "linux")]
    {
        let mut size: libc::winsize = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::ioctl(0, libc::TIOCGWINSZ, &mut size) }, 0);
        assert_eq!(unsafe { libc::tcgetsid(0) }, unsafe { libc::getsid(0) }, "missing controlling terminal");
        TerminalSize { rows: size.ws_row, cols: size.ws_col }
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Console::{
            CONSOLE_SCREEN_BUFFER_INFO, GetConsoleScreenBufferInfo, GetStdHandle, STD_OUTPUT_HANDLE,
        };
        let mut info: CONSOLE_SCREEN_BUFFER_INFO = unsafe { std::mem::zeroed() };
        assert_ne!(unsafe { GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &mut info) }, 0);
        TerminalSize {
            rows: (info.srWindow.Bottom - info.srWindow.Top + 1) as u16,
            cols: (info.srWindow.Right - info.srWindow.Left + 1) as u16,
        }
    }
}

fn terminal_pid(bytes: &[u8], prefix: &str) -> Option<u32> {
    let text = std::str::from_utf8(bytes).ok()?;
    let tail = text.split_once(prefix)?.1;
    let end = tail.find(|ch: char| !ch.is_ascii_digit())?;
    tail[..end].parse().ok()
}

#[test]
fn native_terminal_isatty_resize_and_merged_output() {
    let mut spec = child_spec("terminal-size");
    let directory = std::env::temp_dir();
    spec.env.insert("SWACTOR_TERMINAL_ENV".to_owned(), "value with spaces = 日本語".to_owned());
    spec.env.insert("SWACTOR_TERMINAL_CWD".to_owned(), directory.to_string_lossy().into_owned());
    spec.working_dir = Some(directory);
    let (mut harness, mut input) = Harness::native_terminal(spec);
    let leader = harness.track_leader();
    harness.marker(b"TERMINAL_SIZE=25,90");
    assert_eq!(harness.complete(input.resize(TerminalSize { rows: 0, cols: 80 })).unwrap_err().kind(), io::ErrorKind::InvalidInput);
    assert_eq!(harness.complete(input.write_all(vec![b'x'; MAX_WRITE + 1])).unwrap_err().kind(), io::ErrorKind::InvalidInput);
    harness.complete(input.resize(TerminalSize { rows: 42, cols: 110 })).unwrap();
    harness.complete(input.write_all(b"resized\r".to_vec())).unwrap();
    harness.exited(23);
    assert!(contains(&harness.stdout, b"TERMINAL_RESIZED=42,110"));
    assert!(contains(&harness.stdout, b"TERMINAL_MERGED_STDERR"));
    assert!(harness.stderr.is_empty(), "terminal output must be merged");
    harness.reaped(&leader);
    assert_eq!(harness.complete(input.resize(TerminalSize { rows: 24, cols: 80 })).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
}

#[test]
fn native_terminal_cancel_and_stop_kill_separate_job_groups() {
    adopt_orphaned_descendants();
    for stop in [false, true] {
        let (mut harness, mut input) = Harness::native_terminal(child_spec("terminal-tree"));
        let leader = harness.track_leader();
        harness.wait("terminal descendant handshake", |h| terminal_pid(&h.stdout, "DESCENDANT=").is_some());
        let descendant = harness.fuse.track(terminal_pid(&harness.stdout, "DESCENDANT=").unwrap());
        assert!(!leader.exited() && !descendant.exited());
        if stop {
            send_process_command(&harness.runtime.create_sender(), harness.process.unwrap(),
                ProcessCommand::Stop { kill_after: Some(Duration::from_millis(25)) }).unwrap();
        } else {
            harness.cancel();
        }
        harness.terminal();
        assert!(matches!(harness.events.last(), Some(ProcessOutput::Exited { .. })));
        harness.reaped(&leader);
        harness.wait("terminal job exit", |_| descendant.exited());
        #[cfg(target_os = "linux")]
        descendant.reap_adopted_descendant();
        assert_eq!(harness.complete(input.write_all(b"retained".to_vec())).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    }
}

#[test]
fn native_terminal_retained_idle_input_settles_on_exit_and_cancel() {
    for cancel in [false, true] {
        let mut spec = child_spec("terminal-idle");
        let control = ExitControl::new(&mut spec);
        let (mut harness, mut input) = Harness::native_terminal(spec);
        let leader = harness.track_leader();
        harness.marker(b"TERMINAL_IDLE_READY");
        if cancel {
            harness.cancel();
            harness.terminal();
            assert!(matches!(harness.events.last(), Some(ProcessOutput::Exited { .. })));
        } else {
            control.release(&mut harness);
            harness.exited(23);
        }
        harness.reaped(&leader);
        assert_eq!(harness.complete(input.write_all(vec![1])).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn native_terminal_blocked_write_settles_on_cancel_and_owner_drop() {
    #[derive(Clone, Copy)]
    enum Completion { Cancel, DropOwner, Exit }
    for completion in [Completion::Cancel, Completion::DropOwner, Completion::Exit] {
        let mut spec = child_spec("terminal-blocked");
        let control = ExitControl::new(&mut spec);
        let (mut harness, mut input) = Harness::native_terminal(spec);
        let leader = harness.track_leader();
        harness.marker(b"TERMINAL_RAW_READY");
        // Linux N_TTY and PTY buffers cannot retain 512 KiB without a reader.
        // Poll each accepted request until either completion or the deadline;
        // then leave the physically blocked request alive during cancellation.
        let mut saturated = false;
        for _ in 0..32 {
            let mut write = std::pin::pin!(input.write_all(vec![b'x'; MAX_WRITE]));
            let deadline = Instant::now() + Duration::from_millis(100);
            let result = loop {
                if let Poll::Ready(result) = poll_once(write.as_mut()) { break Some(result); }
                harness.tick();
                if Instant::now() >= deadline { break None; }
            };
            if let Some(result) = result {
                result.unwrap();
                continue;
            }
            saturated = true;
            match completion {
                Completion::DropOwner => harness.runtime.stop_actor(harness.process.unwrap()).unwrap(),
                Completion::Cancel => harness.cancel(),
                Completion::Exit => control.release(&mut harness),
            }
            assert_eq!(harness.complete(write).unwrap_err().kind(), io::ErrorKind::BrokenPipe,
                "blocked terminal write must settle as closed input");
            break;
        }
        assert!(saturated, "terminal input did not apply backpressure");
        match completion {
            Completion::DropOwner => {
                harness.wait("terminal owner joined workers", |h| h.engine.handle().ownership_snapshot().blocking == 1);
            }
            Completion::Cancel => {
                harness.terminal();
                assert!(matches!(harness.events.last(), Some(ProcessOutput::Exited { .. })));
            }
            Completion::Exit => harness.exited(23),
        }
        harness.reaped(&leader);
        assert_eq!(harness.complete(input.write_all(vec![1])).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    }
}

#[test]
fn native_terminal_shell_editing_history_and_ctrl_c() {
    let executable = std::env::current_exe().unwrap();
    #[cfg(target_os = "linux")]
    let (command, args, child_command, edit_command, survive_command) = (
        "/bin/bash".to_owned(),
        vec!["--noprofile".to_owned(), "--norc".to_owned(), "-i".to_owned()],
        format!("'{}' --exact stdin_child --nocapture\r", executable.to_string_lossy().replace('\'', "'\"'\"'")),
        b"printf 'EDIT_%s\\n' BAQ\x7fR\r".as_slice(),
        b"printf 'SHELL_%s\\n' SURVIVED\r".as_slice(),
    );
    #[cfg(windows)]
    let (command, args, child_command, edit_command, survive_command) = (
        "cmd.exe".to_owned(),
        vec!["/D".to_owned(), "/Q".to_owned()],
        format!("\"{}\" --exact stdin_child --nocapture\r", executable.to_string_lossy()),
        b"echo EDIT_^BAQ\x08R\r".as_slice(),
        b"echo SHELL_^SURVIVED\r".as_slice(),
    );
    let spec = ProcessSpec {
        command, args,
        env: HashMap::from([
            (ROLE.to_owned(), "terminal-interrupt".to_owned()),
            ("TERM".to_owned(), "xterm-256color".to_owned()),
            ("HISTFILE".to_owned(), String::new()),
        ]),
        working_dir: None, label: None,
    };
    let (mut harness, mut input) = Harness::native_terminal(spec);
    let leader = harness.track_leader();
    harness.complete(input.write_all(child_command.into_bytes())).unwrap();
    harness.wait("foreground terminal child", |h| terminal_pid(&h.stdout, "INTERRUPT_READY=").is_some());
    let foreground = harness.fuse.track(terminal_pid(&harness.stdout, "INTERRUPT_READY=").unwrap());
    harness.complete(input.write_all(vec![3])).unwrap();
    harness.wait("Ctrl-C terminates foreground command", |_| foreground.exited());
    assert!(!leader.exited(), "Ctrl-C killed the interactive shell");
    harness.complete(input.write_all(survive_command.to_vec())).unwrap();
    harness.marker(b"SHELL_SURVIVED");
    harness.complete(input.write_all(edit_command.to_vec())).unwrap();
    harness.marker(b"EDIT_BAR");
    let before = harness.stdout.windows(b"EDIT_BAR".len()).filter(|bytes| *bytes == b"EDIT_BAR").count();
    harness.complete(input.write_all(b"\x1b[A\r".to_vec())).unwrap();
    harness.wait("native command history", |h| {
        h.stdout.windows(b"EDIT_BAR".len()).filter(|bytes| *bytes == b"EDIT_BAR").count() > before
    });
    assert!(!harness.events.iter().any(is_terminal));
    harness.complete(input.write_all(b"exit 23\r".to_vec())).unwrap();
    harness.exited(23);
    harness.reaped(&leader);
}

#[test]
fn native_terminal_spawn_failure_settles_input() {
    let mut spec = child_spec("terminal-size");
    spec.command = std::env::current_exe().unwrap().with_file_name("swactor-terminal-missing-executable")
        .to_string_lossy().into_owned();
    let (mut harness, mut input) = Harness::native_terminal(spec);
    assert_eq!(harness.complete(input.write_all(b"pending".to_vec())).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    harness.terminal();
    assert!(matches!(harness.events.last(), Some(ProcessOutput::SpawnFailed { .. })));
    assert!(!harness.events.iter().any(|event| matches!(event, ProcessOutput::Started { .. })));
}

#[test]
fn native_terminal_owner_drop_joins_idle_workers() {
    let mut spec = child_spec("terminal-idle");
    let _control = ExitControl::new(&mut spec);
    let (mut harness, mut input) = Harness::native_terminal(spec);
    let leader = harness.track_leader();
    harness.marker(b"TERMINAL_IDLE_READY");
    harness.runtime.stop_actor(harness.process.unwrap()).unwrap();
    harness.wait("terminal owner joined idle workers", |h| h.engine.handle().ownership_snapshot().blocking == 1);
    harness.reaped(&leader);
    assert_eq!(harness.complete(input.write_all(vec![1])).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
}
