use std::io;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
#[cfg(not(test))]
use distribution::types::MemberState;
#[cfg(not(test))]
use iroh::PublicKey;
#[cfg(not(test))]
use iroh::{EndpointAddr, RelayUrl};
#[cfg(not(test))]
use iroh::endpoint::Connection;
use iroh_driver::IrohDriver;
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::{ExternalSender, Runtime};
use swactor_engine::{BlockingWorkSender, EngineHandle, TaskSpawner};
use swactor_process::{
    ExitStatus, ProcessCommand, ProcessOutput, ProcessOutputConfig, ProcessSpec, ProcessStdin,
    TerminalInput, TerminalSize, send_process_command, spawn_local_process_with_stdin,
    spawn_local_terminal,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};

#[cfg(not(test))]
use crate::cluster::Membership;
#[cfg(not(test))]
use crate::local_control::{self, Listener, LocalStream};
use crate::protocol::{Frame, read_frame, write_frame};

#[cfg(not(test))]
pub const ALPN: &[u8] = b"swactor/shell/1";

pub struct Service {
    #[cfg(not(test))]
    pub tasks: TaskSpawner,
    pub clock: EngineHandle,
    pub runtime: Runtime,
    pub driver: Arc<IrohDriver>,
    #[cfg(not(test))]
    pub membership: Membership,
    #[cfg(not(test))]
    pub operator: PublicKey,
    pub name: String,
    pub shell: String,
}

impl Service {
    #[cfg(not(test))]
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let mut listener = if self.driver.node_id().0 == *self.operator.as_bytes() {
            Some(local_control::bind(self.operator).await.context("start operator control socket")?)
        } else { None };
        let mut ingress = self.driver.ingress_changed();
        loop {
            for (peer, connection) in self.driver.drain_accepted_for_alpn(ALPN) {
                // Gate Describe and process creation on authenticated TLS identity,
                // never enrollment, advertised names, or a request payload.
                if peer.0 != *self.operator.as_bytes() {
                    connection.close(1u32.into(), b"operator identity required");
                    continue;
                }
                let service = self.clone();
                self.tasks.spawn(async move {
                    while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                        let service = service.clone();
                        let connection = connection.clone();
                        service.tasks.clone().spawn(async move {
                            let result = tokio::select! {
                                result = service.request(&mut recv, &mut send) => result,
                                _ = connection.closed() => return,
                            };
                            if let Err(error) = result {
                                eprintln!("Remote shell: {error:#}");
                                let _ = write_frame(&mut send, &error_frame(error)).await;
                            }
                            let _ = send.finish();
                        });
                    }
                });
            }
            tokio::select! {
                changed = ingress.changed() => { changed.context("shell transport closed")?; }
                accepted = accept_local(&mut listener) => {
                    let stream = accepted?;
                    let service = self.clone();
                    self.tasks.spawn(async move {
                        if let Err(error) = service.local_session(stream).await {
                            eprintln!("Operator shell: {error:#}");
                        }
                    });
                }
            }
        }
    }

    async fn request(&self, recv: &mut (impl AsyncRead + Unpin), send: &mut (impl AsyncWrite + Unpin)) -> Result<()> {
        let request = self.clock.timeout(Duration::from_secs(10), read_frame(recv)).await
            .map_err(|_| anyhow::anyhow!("shell request timed out"))??.context("shell request closed")?;
        let (target, size) = match request {
            Frame::Describe => { write_frame(send, &Frame::Name(self.name.clone())).await?; return Ok(()); }
            Frame::Open { target } => (target, None),
            Frame::OpenTerminal { target, size } => (target, Some(size)),
            _ => bail!("expected a shell open request"),
        };
        ensure!(target == self.driver.listen_addr(), "shell request targets another node");
        let mut session = start_session(
            &self.runtime, self.clock.blocking_work_sender(), shell_spec(&self.shell, size.is_some()), size,
        ).await?;
        match session.events.next().await? {
            ProcessOutput::Started { .. } => {}
            ProcessOutput::SpawnFailed { error } | ProcessOutput::Error { error } => bail!("{error}"),
            _ => bail!("remote shell ended before starting"),
        }
        write_frame(send, &Frame::Ready).await?;
        // Neither a backpressured pipe write nor ordered stdin EOF may stop the
        // independent output pump. Dropping either request cancels its child.
        tokio::select! {
            result = receive_input(recv, &mut session.input) => result,
            result = session_output(send, &mut session.events, &mut session.cancel) => result,
        }
    }

    #[cfg(not(test))]
    async fn dial(&self, peer: PublicKey) -> Result<Connection> {
        let relay: RelayUrl = self.driver.relay_url().context("relay is not configured")?.parse()?;
        Ok(self.clock.timeout(Duration::from_secs(10),
            self.driver.endpoint().connect(EndpointAddr::new(peer).with_relay_url(relay), ALPN)
        ).await.map_err(|_| anyhow::anyhow!("remote node connection timed out"))??)
    }

    #[cfg(not(test))]
    async fn describe(&self, peer: PublicKey) -> Result<String> {
        let connection = ConnectionGuard(self.dial(peer).await?);
        self.clock.timeout(Duration::from_secs(3), async {
            let (mut send, mut recv) = connection.0.open_bi().await?;
            write_frame(&mut send, &Frame::Describe).await?;
            match read_frame(&mut recv).await? {
                Some(Frame::Name(name)) => Ok(name),
                Some(Frame::Error(error)) => bail!("{error}"),
                _ => bail!("invalid node description"),
            }
        }).await.map_err(|_| anyhow::anyhow!("node description timed out"))?
    }

    #[cfg(not(test))]
    async fn resolve(&self, target: &str) -> Result<PublicKey> {
        if let Ok(peer) = target.parse::<PublicKey>() { return Ok(peer); }
        let peers: Vec<_> = self.membership.borrow().iter()
            .filter(|(peer, state)| **state == MemberState::Alive && **peer != self.driver.node_id())
            .map(|(peer, _)| *peer).collect();
        let mut found = None;
        for peer in peers {
            let peer = PublicKey::from_bytes(&peer.0)?;
            if let Ok(name) = self.describe(peer).await {
                if name == target {
                    ensure!(found.is_none(), "multiple nodes named {target}; use the full public node ID");
                    found = Some(peer);
                }
            }
        }
        found.with_context(|| format!("no reachable node named {target}; check the dashboard or use its public node ID"))
    }

    #[cfg(not(test))]
    async fn local_session(&self, mut stream: LocalStream) -> Result<()> {
        let result = async {
            let request = self.clock.timeout(Duration::from_secs(10), read_frame(&mut stream)).await
                .map_err(|_| anyhow::anyhow!("local shell request timed out"))??;
            let (target, size) = match request {
                Some(Frame::Open { target }) => (target, None),
                Some(Frame::OpenTerminal { target, size }) => (target, Some(size)),
                _ => bail!("expected a shell open request"),
            };
            let peer = self.resolve(&target).await?;
            // Install before open_bi or any fallible/async startup handoff.
            let connection = ConnectionGuard(self.dial(peer).await?);
            let (mut send, mut recv) = connection.0.open_bi().await?;
            let open = match size {
                Some(size) => Frame::OpenTerminal { target: peer.to_string(), size },
                None => Frame::Open { target: peer.to_string() },
            };
            write_frame(&mut send, &open).await?;
            self.await_startup(&mut recv, &mut stream).await?;
            write_frame(&mut stream, &Frame::Ready).await?;
            let (mut local_read, mut local_write) = tokio::io::split(&mut stream);
            tokio::select! {
                result = forward_input(&mut local_read, &mut send, size.is_some()) => result,
                result = self.forward_output(&mut recv, &mut local_write) => result,
            }
        }.await;
        if let Err(error) = result {
            write_frame(&mut stream, &error_frame(error)).await?;
        }
        Ok(())
    }

    async fn await_startup<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
        &self, remote: &mut R, local: &mut W,
    ) -> Result<()> {
        // One absolute admission deadline; watchdog ticks borrow the same read,
        // including any partially consumed header or payload.
        let admission = self.clock.timeout(Duration::from_secs(30), read_frame(remote));
        tokio::pin!(admission);
        loop {
            match self.clock.timeout(Duration::from_secs(1), &mut admission).await {
                Ok(result) => {
                    let frame = result.map_err(|_| anyhow::anyhow!("remote shell startup timed out"))??
                        .context("remote shell disconnected during startup")?;
                    return match frame {
                        Frame::Ready => Ok(()),
                        Frame::Error(error) => bail!("{error}"),
                        _ => bail!("invalid remote shell opening response"),
                    };
                }
                Err(_) => write_frame(local, &Frame::Ping).await?,
            }
        }
    }

    async fn forward_output<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
        &self, remote: &mut R, local: &mut W,
    ) -> Result<()> {
        loop {
            let incoming = read_frame(remote);
            tokio::pin!(incoming);
            let frame = loop {
                match self.clock.timeout(Duration::from_secs(1), &mut incoming).await {
                    Ok(result) => break result?.context("remote shell disconnected")?,
                    Err(_) => write_frame(local, &Frame::Ping).await?,
                }
            };
            let done = matches!(frame, Frame::Exit(_) | Frame::Error(_));
            ensure!(matches!(frame, Frame::Stdout(_) | Frame::Stderr(_) | Frame::Exit(_) | Frame::Error(_)),
                "invalid remote shell output");
            write_frame(local, &frame).await?;
            if done { return Ok(()); }
        }
    }
}

fn shell_spec(shell: &str, terminal: bool) -> ProcessSpec {
    let args: &[&str] = if cfg!(windows) {
        if terminal {
            &[
                "-NoLogo", "-NoProfile", "-NoExit", "-Command",
                "Set-PSReadLineOption -EditMode Emacs; \
                 if ((Get-Command Set-PSReadLineOption).Parameters.ContainsKey('PredictionSource')) { \
                 Set-PSReadLineOption -PredictionSource None }",
            ]
        }
        else { &["-NoLogo", "-NoProfile", "-InputFormat", "Text", "-OutputFormat", "Text", "-Command", "-"] }
    } else if terminal { &["-i"] } else { &[] };
    ProcessSpec {
        command: shell.to_owned(),
        args: args.iter().map(|arg| (*arg).to_owned()).collect(),
        env: if terminal { [("TERM".to_owned(), "xterm-256color".to_owned())].into() } else { Default::default() },
        working_dir: None,
        label: Some("remote-shell".to_owned()),
    }
}

enum ShellInput {
    Pipe(ProcessStdin),
    Terminal(TerminalInput),
}

impl ShellInput {
    async fn write_all(&mut self, bytes: Vec<u8>) -> io::Result<()> {
        match self {
            Self::Pipe(input) => input.write_all(bytes).await,
            Self::Terminal(input) => input.write_all(bytes).await,
        }
    }
}

#[cfg(not(test))]
struct ConnectionGuard(Connection);
#[cfg(not(test))]
impl Drop for ConnectionGuard {
    fn drop(&mut self) { self.0.close(0u32.into(), b"shell request complete"); }
}

struct ProcessGuard {
    sender: ExternalSender,
    process: Option<ActorAddress>,
}
impl Drop for ProcessGuard {
    fn drop(&mut self) {
        if let Some(process) = self.process.take() {
            let _ = send_process_command(&self.sender, process, ProcessCommand::Cancel);
        }
    }
}

struct SessionActorGuard {
    runtime: Runtime,
    actor: ActorAddress,
}
impl Drop for SessionActorGuard {
    fn drop(&mut self) { let _ = self.runtime.stop_actor(self.actor); }
}

struct SessionInput {
    input: ShellInput,
    cancel: ProcessGuard,
}

struct ShellSession {
    _actor: SessionActorGuard,
    input: ShellInput,
    cancel: ProcessGuard,
    events: SessionEvents,
}

struct SessionEvents {
    events: mpsc::Receiver<ProcessOutput>,
    failure: Option<oneshot::Receiver<String>>,
}
impl SessionEvents {
    async fn next(&mut self) -> Result<ProcessOutput> {
        loop {
            let failure = async {
                match self.failure.as_mut() {
                    Some(failure) => failure.await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                result = failure => {
                    self.failure = None;
                    // Normal actor completion drops the notifier. Its ordered
                    // event channel still owns all output and the actual exit.
                    if let Ok(error) = result { bail!("{error}"); }
                }
                event = self.events.recv() => return event.context("remote shell ended without an exit status"),
            }
        }
    }
}

async fn start_session(
    runtime: &Runtime, blocking: BlockingWorkSender, spec: ProcessSpec, size: Option<TerminalSize>,
) -> Result<ShellSession> {
    let (handoff, receive) = oneshot::channel();
    let (events, output) = mpsc::channel(256);
    let (failure, failed) = oneshot::channel();
    let actor = runtime.spawn(ShellSessionActor {
        blocking, sender: runtime.create_sender(), spec: Some(spec), size,
        handoff: Some(handoff), events, failure: Some(failure), process: None, cancel: None,
    }).map_err(|error| anyhow::anyhow!("start shell session actor: {error:?}"))?;
    let actor = SessionActorGuard { runtime: runtime.clone(), actor };
    let input = receive.await.context("shell startup abandoned")?.map_err(anyhow::Error::msg)?;
    Ok(ShellSession {
        _actor: actor, input: input.input, cancel: input.cancel,
        events: SessionEvents { events: output, failure: Some(failed) },
    })
}

struct ShellSessionActor {
    blocking: BlockingWorkSender,
    sender: ExternalSender,
    spec: Option<ProcessSpec>,
    size: Option<TerminalSize>,
    handoff: Option<oneshot::Sender<Result<SessionInput, String>>>,
    events: mpsc::Sender<ProcessOutput>,
    failure: Option<oneshot::Sender<String>>,
    process: Option<ActorAddress>,
    cancel: Option<ProcessGuard>,
}

impl ActorInterface for ShellSessionActor {
    type Incoming = ProcessOutput;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        let spec = self.spec.take().expect("single session start");
        let output = ProcessOutputConfig::disabled(ctx.self_addr());
        let spawned = match self.size {
            Some(size) => spawn_local_terminal(&self.blocking, ctx, &self.sender, spec, size, output)
                .map(|(process, input)| (process, ShellInput::Terminal(input))),
            None => spawn_local_process_with_stdin(&self.blocking, ctx, &self.sender, spec, output)
                .map(|(process, input)| (process, ShellInput::Pipe(input))),
        };
        match spawned {
            Ok((process, input)) => {
                // Own cancellation before the handoff can fail or be abandoned.
                self.cancel = Some(ProcessGuard { sender: self.sender.clone(), process: Some(process) });
                self.process = Some(process);
                let input = SessionInput { input, cancel: self.cancel.take().expect("installed process guard") };
                if self.handoff.take().expect("single startup handoff").send(Ok(input)).is_err() {
                    ctx.stop_self();
                }
            }
            Err(error) => {
                let _ = self.handoff.take().expect("single startup handoff").send(Err(format!("start remote shell: {error:?}")));
                ctx.stop_self();
            }
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: ProcessOutput) {
        let terminal = matches!(message, ProcessOutput::Exited { .. } | ProcessOutput::SpawnFailed { .. } | ProcessOutput::Error { .. });
        if terminal { self.process = None; }
        match self.events.try_send(message) {
            Ok(()) if !terminal => return,
            Err(mpsc::error::TrySendError::Full(_)) => {
                if let Some(failure) = self.failure.take() {
                    let _ = failure.send("remote output consumer stalled".to_owned());
                }
            }
            _ => {}
        }
        ctx.stop_self();
    }

    fn on_stop(&mut self, _ctx: &Ctx<'_>) {
        self.handoff.take();
        self.cancel.take();
        if let Some(process) = self.process.take() {
            let _ = send_process_command(&self.sender, process, ProcessCommand::Cancel);
        }
    }
}

async fn receive_input<R: AsyncRead + Unpin>(reader: &mut R, input: &mut ShellInput) -> Result<()> {
    let mut closed = false;
    loop {
        let result = match read_frame(reader).await? {
            Some(Frame::Stdin(bytes)) if !closed => input.write_all(bytes).await,
            Some(Frame::StdinClosed) if !closed => {
                closed = true;
                match input {
                    ShellInput::Pipe(input) => input.close().await,
                    // Returning drops the session's process guard; a terminal
                    // cannot half-close its input without hanging its shell.
                    ShellInput::Terminal(_) => return Ok(()),
                }
            }
            Some(Frame::Resize(size)) if !closed => match input {
                ShellInput::Terminal(input) => input.resize(size).await,
                ShellInput::Pipe(_) => bail!("resize requires a terminal session"),
            },
            None => bail!("shell input disconnected"),
            _ => bail!("invalid shell input frame"),
        };
        match result {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {
                // The real process exit (possibly nonzero) is authoritative.
                // Connection loss still cancels the enclosing request.
                return std::future::pending().await;
            }
            Err(error) => return Err(error).context("write remote shell input"),
        }
    }
}

async fn session_output<W: AsyncWrite + Unpin>(
    writer: &mut W, events: &mut SessionEvents, cancel: &mut ProcessGuard,
) -> Result<()> {
    loop {
        let frame = match events.next().await? {
            ProcessOutput::Stdout(bytes) => Frame::Stdout(bytes),
            ProcessOutput::Stderr(bytes) => Frame::Stderr(bytes),
            ProcessOutput::Exited { status } => {
                cancel.process = None;
                let code = match status { ExitStatus::Code(code) => code, ExitStatus::Signal(signal) => 128 + signal, ExitStatus::Unknown => 1 };
                write_frame(writer, &Frame::Exit(code)).await?;
                return Ok(());
            }
            ProcessOutput::SpawnFailed { error } | ProcessOutput::Error { error } => bail!("{error}"),
            ProcessOutput::Started { .. } => bail!("duplicate shell start"),
        };
        write_frame(writer, &frame).await?;
    }
}

async fn forward_input<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R, writer: &mut W, terminal: bool,
) -> Result<()> {
    let mut closed = false;
    loop {
        let frame = read_frame(reader).await?.context("local shell disconnected")?;
        match &frame {
            Frame::Stdin(_) if !closed => {}
            Frame::StdinClosed if !closed => closed = true,
            Frame::Resize(_) if terminal && !closed => {}
            _ => bail!("invalid local shell input"),
        }
        write_frame(writer, &frame).await?;
        // Do not finish after StdinClosed: keep observing real IPC disconnect
        // while remote output/Exit drains, even for a long-running final command.
    }
}

#[cfg(not(test))]
async fn accept_local(listener: &mut Option<Listener>) -> Result<LocalStream> {
    match listener {
        Some(listener) => listener.accept().await,
        None => std::future::pending().await,
    }
}

#[cfg(not(test))]
fn error_frame(error: anyhow::Error) -> Frame {
    let mut text = format!("{error:#}");
    let mut end = text.len().min(4096);
    while !text.is_char_boundary(end) { end -= 1; }
    text.truncate(end);
    Frame::Error(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use swactor::runtime::RuntimeParts;
    use swactor_engine::{Engine, TokioBackend, TokioConfig};
    use tokio::io::AsyncWriteExt;

    const CHILD: &str = "SWITCHBOARD_SHELL_TEST_CHILD";

    // Mint task capability at each compiler-identified test composition root.
    macro_rules! run {
        ($test:expr) => {{
            let parts = RuntimeParts::new(Default::default());
            let runtime = parts.runtime().clone();
            let engine = Engine::new(parts, TokioBackend::new(TokioConfig {
                worker_threads: 2, ..Default::default()
            }).unwrap()).unwrap();
            run(&engine, runtime, engine.task_spawner(), $test);
        }};
    }

    fn run<F: Future<Output = ()> + Send + 'static>(
        engine: &Engine, runtime: Runtime, tasks: TaskSpawner,
        test: impl FnOnce(Arc<Service>) -> F + Send + 'static,
    ) {
        let driver = Arc::new(IrohDriver::with_engine(tasks.clone(), engine.handle(),
            iroh_driver::IrohDriverConfig {
                secret_key: None, relay_mode: iroh::RelayMode::Disabled, bind_port: None,
                node: Default::default(), peer_auth: None, additional_alpns: Vec::new(),
            }
        ).unwrap());
        let service = Arc::new(Service {
            clock: engine.handle(), runtime,
            driver, name: "test".to_owned(), shell: String::new(),
        });
        crate::test_support::run(tasks, async move {
            test(service.clone()).await;
            service.driver.close().await;
        });
    }

    fn child_spec(role: &str) -> ProcessSpec {
        ProcessSpec {
            command: std::env::current_exe().unwrap().to_string_lossy().into_owned(),
            args: vec!["--exact".to_owned(), "remote_shell::tests::controlled_child".to_owned(), "--nocapture".to_owned()],
            env: [(CHILD.to_owned(), role.to_owned())].into(),
            working_dir: None, label: Some("remote-shell".to_owned()),
        }
    }

    #[test]
    fn controlled_child() {
        let Ok(role) = std::env::var(CHILD) else { return; };
        let mut input = std::io::stdin().lock();
        input.read_exact(&mut [0]).unwrap();
        match role.as_str() {
            "overflow" => {
                let mut output = std::io::stdout().lock();
                // A finite burst exceeds the adapter's 256 events even at the
                // process reader's maximum chunk size, then leaves a live child
                // to cancel. Do not turn this into a core-queue throughput test.
                for _ in 0..1024 { output.write_all(&[b'x'; 4096]).unwrap(); }
                output.flush().unwrap();
                let mut eof = Vec::new();
                input.read_to_end(&mut eof).unwrap();
            }
            "echo" => {
                std::io::stdout().write_all(b"OUT_LIVE\n").unwrap();
                std::io::stdout().flush().unwrap();
                std::io::stderr().write_all(b"ERR_LIVE\n").unwrap();
                std::io::stderr().flush().unwrap();
                let mut rest = Vec::new();
                input.read_to_end(&mut rest).unwrap();
                std::io::stdout().write_all(&rest).unwrap();
                std::io::stdout().flush().unwrap();
            }
            "exit" => {}
            _ => panic!("unknown child role"),
        }
        std::process::exit(23);
    }

    #[cfg(target_os = "linux")]
    struct ChildIdentity(std::os::fd::OwnedFd);
    #[cfg(target_os = "linux")]
    impl ChildIdentity {
        fn open(pid: u32) -> Self {
            use std::os::fd::FromRawFd;
            // SAFETY: pidfd_open retains this exact live process identity.
            let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) } as i32;
            assert!(fd >= 0, "pidfd_open: {}", io::Error::last_os_error());
            Self(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) })
        }
        fn wait(&self) {
            use std::os::fd::AsRawFd;
            let mut fd = libc::pollfd { fd: self.0.as_raw_fd(), events: libc::POLLIN, revents: 0 };
            // SAFETY: a single valid pollfd, retained until poll completes.
            assert_eq!(unsafe { libc::poll(&mut fd, 1, 5_000) }, 1, "child survived session cleanup");
            assert_ne!(fd.revents & libc::POLLIN, 0);
        }
    }

    #[cfg(windows)]
    struct ChildIdentity(std::os::windows::io::OwnedHandle);
    #[cfg(windows)]
    impl ChildIdentity {
        fn open(pid: u32) -> Self {
            use std::os::windows::io::FromRawHandle;
            use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_SYNCHRONIZE};
            // SAFETY: the returned handle owns a reference to the exact process.
            let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
            assert!(!handle.is_null(), "OpenProcess: {}", io::Error::last_os_error());
            Self(unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(handle) })
        }
        fn wait(&self) {
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::{Foundation::WAIT_OBJECT_0, System::Threading::WaitForSingleObject};
            // SAFETY: the owned process handle remains valid throughout the wait.
            assert_eq!(unsafe { WaitForSingleObject(self.0.as_raw_handle(), 5_000) }, WAIT_OBJECT_0);
        }
    }

    #[test]
    fn partial_startup_and_output_frames_survive_watchdog_ticks() {
        run!(|service| async move {
            let (mut remote, mut member) = tokio::io::duplex(32);
            let (mut local, mut client) = tokio::io::duplex(32);
            let ready = service.await_startup(&mut remote, &mut local);
            let peer = async {
                member.write_all(&[3, 0]).await.unwrap();
                assert_eq!(read_frame(&mut client).await.unwrap(), Some(Frame::Ping));
                member.write_all(&[0, 0, 0]).await.unwrap();
            };
            let (result, ()) = tokio::join!(ready, peer);
            result.unwrap();
            let output = service.forward_output(&mut remote, &mut local);
            let peer = async {
                member.write_all(&[5, 0, 0, 0, 3, b'a']).await.unwrap();
                assert_eq!(read_frame(&mut client).await.unwrap(), Some(Frame::Ping));
                member.write_all(b"bc").await.unwrap();
                assert_eq!(read_frame(&mut client).await.unwrap(), Some(Frame::Stdout(b"abc".to_vec())));
                write_frame(&mut member, &Frame::Stderr(b"error".to_vec())).await.unwrap();
                assert_eq!(read_frame(&mut client).await.unwrap(), Some(Frame::Stderr(b"error".to_vec())));
                write_frame(&mut member, &Frame::Exit(23)).await.unwrap();
                assert_eq!(read_frame(&mut client).await.unwrap(), Some(Frame::Exit(23)));
            };
            let (result, ()) = tokio::join!(output, peer);
            result.unwrap();
        });
    }

    #[test]
    fn idle_watchdog_detects_local_loss_during_startup_and_output() {
        run!(|service| async move {
            let (mut remote, _member) = tokio::io::duplex(32);
            let (mut local, client) = tokio::io::duplex(32);
            drop(client);
            assert!(service.await_startup(&mut remote, &mut local).await.is_err());
            assert!(service.forward_output(&mut remote, &mut local).await.is_err());
        });
    }

    #[test]
    fn invalid_opening_order_and_target_never_start_a_shell() {
        run!(|service| async move {
            for frame in [
                Frame::Stdin(b"bad".to_vec()), Frame::Open { target: "wrong-node".to_owned() },
                Frame::OpenTerminal { target: "wrong-node".to_owned(), size: TerminalSize { rows: 24, cols: 80 } },
            ] {
                let mut bytes = Vec::new();
                write_frame(&mut bytes, &frame).await.unwrap();
                let mut output = Vec::new();
                assert!(service.request(&mut bytes.as_slice(), &mut output).await.is_err());
                assert!(output.is_empty());
            }
        });
    }

    #[test]
    fn daemon_eof_is_forwarded_without_finishing_the_request() {
        run!(|_| async move {
            let (mut source, mut local) = tokio::io::duplex(32);
            let (mut remote, mut member) = tokio::io::duplex(32);
            let pump = forward_input(&mut local, &mut remote, false);
            let peer = async {
                write_frame(&mut source, &Frame::Stdin(b"command".to_vec())).await.unwrap();
                assert_eq!(read_frame(&mut member).await.unwrap(), Some(Frame::Stdin(b"command".to_vec())));
                write_frame(&mut source, &Frame::StdinClosed).await.unwrap();
                assert_eq!(read_frame(&mut member).await.unwrap(), Some(Frame::StdinClosed));
                // Closing the actual socket, unlike StdinClosed, must end the pump.
                drop(source);
            };
            let (result, ()) = tokio::join!(pump, peer);
            assert!(result.unwrap_err().to_string().contains("disconnected"));
        });
    }

    #[test]
    fn daemon_resize_requires_terminal_mode_and_preserves_frame_order() {
        engine_test!({
            let size = TerminalSize { rows: 40, cols: 120 };
            let frames = [Frame::Stdin(b"\x1b[A\x03".to_vec()), Frame::Resize(size), Frame::StdinClosed];
            let mut source = Vec::new();
            for frame in &frames { write_frame(&mut source, frame).await.unwrap(); }
            let mut output = Vec::new();
            assert!(forward_input(&mut source.as_slice(), &mut output, true).await.is_err());
            let mut reader = output.as_slice();
            for frame in &frames { assert_eq!(read_frame(&mut reader).await.unwrap().as_ref(), Some(frame)); }
            assert_eq!(read_frame(&mut reader).await.unwrap(), None);
            output.clear();
            let mut resize = Vec::new();
            write_frame(&mut resize, &Frame::Resize(size)).await.unwrap();
            assert!(forward_input(&mut resize.as_slice(), &mut output, false).await.is_err());
            assert!(output.is_empty());
        });
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn terminal_session_keeps_shell_state_and_applies_resize() {
        run!(|service| async move {
            let mut session = start_session(&service.runtime, service.clock.blocking_work_sender(),
                shell_spec("/bin/sh", true), Some(TerminalSize { rows: 24, cols: 80 })).await.unwrap();
            assert!(matches!(session.events.next().await.unwrap(), ProcessOutput::Started { .. }));
            let (mut source, mut reader) = tokio::io::duplex(128);
            let (mut writer, mut destination) = tokio::io::duplex(128);
            let pump = async {
                tokio::select! {
                    result = receive_input(&mut reader, &mut session.input) => panic!("input preempted terminal output: {result:?}"),
                    result = session_output(&mut writer, &mut session.events, &mut session.cancel) => result.unwrap(),
                }
            };
            let peer = async {
                write_frame(&mut source, &Frame::Stdin(b"value=41\nstty size\nprintf '%s\\n' \"$((value + 1))\"\n".to_vec())).await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    match read_frame(&mut destination).await.unwrap().unwrap() {
                        Frame::Stdout(data) => bytes.extend(data),
                        frame => panic!("unexpected terminal output {frame:?}"),
                    }
                    if bytes.windows(7).any(|s| s == b"24 80\r\n") && bytes.windows(4).any(|s| s == b"42\r\n") { break; }
                }
                write_frame(&mut source, &Frame::Resize(TerminalSize { rows: 50, cols: 132 })).await.unwrap();
                write_frame(&mut source, &Frame::Stdin(b"stty size\nprintf '%s\\n' \"$((value + 2))\" >&2\nexit 23\n".to_vec())).await.unwrap();
                bytes.clear();
                loop {
                    match read_frame(&mut destination).await.unwrap().unwrap() {
                        Frame::Stdout(data) => bytes.extend(data),
                        Frame::Exit(code) => { assert_eq!(code, 23); break; }
                        frame => panic!("unexpected terminal output {frame:?}"),
                    }
                }
                assert!(bytes.windows(8).any(|s| s == b"50 132\r\n"));
                assert!(bytes.windows(4).any(|s| s == b"43\r\n"));
            };
            tokio::join!(pump, peer);
        });
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn terminal_eof_cancels_instead_of_leaving_an_interactive_child() {
        run!(|service| async move {
            let mut session = start_session(&service.runtime, service.clock.blocking_work_sender(),
                shell_spec("/bin/sh", true), Some(TerminalSize { rows: 24, cols: 80 })).await.unwrap();
            let ProcessOutput::Started { pid } = session.events.next().await.unwrap() else { panic!("not started"); };
            let identity = ChildIdentity::open(pid);
            let mut bytes = Vec::new();
            write_frame(&mut bytes, &Frame::StdinClosed).await.unwrap();
            receive_input(&mut bytes.as_slice(), &mut session.input).await.unwrap();
            drop(session);
            identity.wait();
        });
    }

    #[test]
    fn member_output_remains_live_and_eof_drains_the_real_exit() {
        run!(|service| async move {
            let mut session = start_session(&service.runtime, service.clock.blocking_work_sender(), child_spec("echo"), None).await.unwrap();
            assert!(matches!(session.events.next().await.unwrap(), ProcessOutput::Started { .. }));
            let (mut source, mut reader) = tokio::io::duplex(64);
            let (mut writer, mut destination) = tokio::io::duplex(64);
            let pump = async {
                tokio::select! {
                    result = receive_input(&mut reader, &mut session.input) => panic!("stdin preempted output: {result:?}"),
                    result = session_output(&mut writer, &mut session.events, &mut session.cancel) => result.unwrap(),
                }
            };
            let peer = async {
                write_frame(&mut source, &Frame::Stdin(b"x".to_vec())).await.unwrap();
                let (mut out, mut err) = (Vec::new(), Vec::new());
                while !out.ends_with(b"OUT_LIVE\n") || !err.ends_with(b"ERR_LIVE\n") {
                    match read_frame(&mut destination).await.unwrap().unwrap() {
                        Frame::Stdout(bytes) => out.extend(bytes),
                        Frame::Stderr(bytes) => err.extend(bytes),
                        frame => panic!("early terminal event {frame:?}"),
                    }
                }
                write_frame(&mut source, &Frame::Stdin(b"after eof".to_vec())).await.unwrap();
                write_frame(&mut source, &Frame::StdinClosed).await.unwrap();
                loop {
                    match read_frame(&mut destination).await.unwrap().unwrap() {
                        Frame::Stdout(bytes) => out.extend(bytes),
                        Frame::Exit(23) => break,
                        frame => panic!("unexpected final frame {frame:?}"),
                    }
                }
                assert!(out.ends_with(b"after eof"));
                assert_eq!(err, b"ERR_LIVE\n");
            };
            tokio::join!(pump, peer);
        });
    }

    #[cfg(any(target_os = "linux", windows))]
    #[test]
    fn broken_stdin_does_not_mask_a_real_nonzero_exit() {
        run!(|service| async move {
            let mut session = start_session(&service.runtime, service.clock.blocking_work_sender(), child_spec("exit"), None).await.unwrap();
            let ProcessOutput::Started { pid } = session.events.next().await.unwrap() else { panic!("not started"); };
            let identity = ChildIdentity::open(pid);
            session.input.write_all(b"x".to_vec()).await.unwrap();
            identity.wait();
            let (mut source, mut reader) = tokio::io::duplex(32);
            write_frame(&mut source, &Frame::Stdin(b"too late".to_vec())).await.unwrap();
            let mut bytes = Vec::new();
            tokio::select! {
                biased;
                result = receive_input(&mut reader, &mut session.input) => panic!("input masked exit: {result:?}"),
                result = session_output(&mut bytes, &mut session.events, &mut session.cancel) => result.unwrap(),
            }
            let mut reader = bytes.as_slice();
            while let Some(frame) = read_frame(&mut reader).await.unwrap() {
                match frame {
                    Frame::Stdout(_) | Frame::Stderr(_) => {}
                    Frame::Exit(code) => { assert_eq!(code, 23); return; }
                    frame => panic!("unexpected frame {frame:?}"),
                }
            }
            panic!("missing real exit status");
        });
    }

    #[cfg(any(target_os = "linux", windows))]
    #[test]
    fn unconsumed_output_bridge_fails_explicitly_and_reaps_the_child() {
        run!(|service| async move {
            let mut session = start_session(&service.runtime, service.clock.blocking_work_sender(), child_spec("overflow"), None).await.unwrap();
            let ProcessOutput::Started { pid } = session.events.next().await.unwrap() else { panic!("not started"); };
            let identity = ChildIdentity::open(pid);
            session.input.write_all(b"x".to_vec()).await.unwrap();
            // Deliberately do not consume the real child's bounded event bridge.
            let failure = session.events.failure.take().unwrap().await.unwrap();
            assert_eq!(failure, "remote output consumer stalled");
            identity.wait();
            assert!(session.input.write_all(b"late".to_vec()).await.is_err());
        });
    }
}
