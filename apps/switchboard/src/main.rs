// Unit tests exclude the binary composition root; production builds still
// check every entrypoint-only item and import with the workspace's strict lints.

#[cfg(test)]
macro_rules! engine_test {
    ($body:block) => {{
        let engine = swactor_engine::Engine::new(
            swactor::runtime::RuntimeParts::new(Default::default()),
            swactor_engine::TokioBackend::new(swactor_engine::TokioConfig {
                worker_threads: 1, ..Default::default()
            }).unwrap(),
        ).unwrap();
        crate::test_support::run(engine.task_spawner(), async move $body);
    }};
}

#[cfg(test)]
mod test_support {
    pub fn run(tasks: swactor_engine::TaskSpawner, test: impl Future<Output = ()> + Send + 'static) {
        let (done, receive) = std::sync::mpsc::channel();
        tasks.spawn(async move {
            test.await;
            let _ = done.send(());
        });
        receive.recv_timeout(std::time::Duration::from_secs(20))
            .expect("shell/IPC test task failed or stalled");
    }
}

#[cfg(not(test))]
mod bootstrap;
#[cfg(not(test))]
mod cluster;
#[cfg(any(not(test), windows))]
mod cli_terminal;
#[cfg(not(test))]
mod telemetry;
#[cfg(not(test))]
mod lifecycle;
#[cfg(any(not(test), unix))]
mod local_control;
mod protocol;
mod remote_shell;
mod shell_cli;

#[cfg(not(test))]
use std::fs::{self, OpenOptions};
#[cfg(not(test))]
use std::io::Write;
#[cfg(not(test))]
use std::path::PathBuf;
#[cfg(not(test))]
use std::sync::Arc;

#[cfg(not(test))]
use dashboard::{DashboardConfig, DashboardHandle};
#[cfg(not(test))]
use anyhow::{Context, Result, bail, ensure};
#[cfg(not(test))]
use distribution::peer_auth::PeerAllowList;
#[cfg(not(test))]
use distribution::types::NodeId;
#[cfg(not(test))]
use iroh::{EndpointAddr, PublicKey, RelayMode, RelayUrl, SecretKey};
#[cfg(not(test))]
use iroh_driver::{IrohDriver, IrohDriverConfig, JoinPhase};
#[cfg(not(test))]
use parking_lot::Mutex;
#[cfg(not(test))]
use swactor::actor::ActorAddress;
#[cfg(not(test))]
use swactor::runtime::Runtime;
#[cfg(not(test))]
use swactor_engine::{ActorCompletion, Engine, TokioBackend, TokioConfig};

#[cfg(not(test))]
use bootstrap::Bootstrap;
#[cfg(not(test))]
use lifecycle::{Effect, Lifecycle, NodeIn, PeerObservation};

#[cfg(not(test))]
const HELP: &str = "Switchboard — a standalone swactor node\n\
Usage: switchboard [identity] [OPTIONS]\n\
       switchboard connect <computer>\n\
  connect COMPUTER         Open an interactive terminal (redirected I/O uses ordinary pipes)\n\
  identity                 Create/load the identity and print its public node ID, then exit\n\
  --state PATH             Identity directory (default: per-user application data)\n\
  --name NAME              Dashboard node label (default: Switchboard)\n\
  --relay URL              Override the bootstrap relay URL\n\
  --peer NODE_ID           Trust and automatically join an additional peer (repeatable)\n\
  --dashboard-port PORT    Loopback dashboard port (default: 9091; 0 chooses a free port)\n\
  --shell PATH             Remote shell executable (default: PowerShell on Windows, $SHELL on Unix)\n\
  -h, --help               Show this help\n\n\
Clients pin the operator identity from bootstrap.json. The operator accepts public enrollment.\n\
Only the holder of the pinned operator private key is the operator; joining grants no operator authority.\n\
The same identity directory must not be used by multiple running nodes.\n\
Interactive connect forwards Ctrl-C to the remote shell; Ctrl-] disconnects.";

#[cfg(not(test))]
struct Options {
    identity: bool,
    state: PathBuf,
    name: String,
    bootstrap: PathBuf,
    relay: Option<RelayUrl>,
    peers: Vec<PublicKey>,
    dashboard_port: u16,
    shell: String,
}

#[cfg(not(test))]
impl Options {
    fn parse(mut args: impl Iterator<Item = String>) -> Result<Option<Self>> {
        let mut options = Self {
            identity: false,
            state: default_state()?,
            name: "Switchboard".to_owned(),
            bootstrap: Bootstrap::default_path()?,
            relay: None,
            peers: Vec::new(),
            dashboard_port: 9091,
            shell: if cfg!(windows) { "powershell.exe".to_owned() }
                else { std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_owned()) },
        };
        while let Some(arg) = args.next() {
            if arg == "--help" || arg == "-h" {
                println!("{HELP}");
                return Ok(None);
            }
            if arg == "identity" {
                options.identity = true;
                continue;
            }
            let value = args.next().with_context(|| format!("missing value for {arg}"))?;
            match arg.as_str() {
                "--state" => options.state = value.into(),
                "--name" => options.name = value,
                "--relay" => options.relay = Some(value.parse().context("invalid relay URL")?),
                "--peer" => {
                    let peer = value.parse().context("invalid peer node ID")?;
                    if !options.peers.contains(&peer) {
                        options.peers.push(peer);
                    }
                }
                "--dashboard-port" => options.dashboard_port = value.parse().context("invalid dashboard port")?,
                "--shell" => options.shell = value,
                _ => bail!("unknown option {arg}; use --help"),
            }
        }
        Ok(Some(options))
    }
}

#[cfg(not(test))]
fn default_state() -> Result<PathBuf> {
    #[cfg(windows)]
    let root = std::env::var_os("LOCALAPPDATA").or_else(|| std::env::var_os("APPDATA"))
        .map(PathBuf::from).context("set LOCALAPPDATA or pass --state PATH")?;
    #[cfg(target_os = "macos")]
    let root = PathBuf::from(std::env::var_os("HOME").context("set HOME or pass --state PATH")?)
        .join("Library/Application Support");
    #[cfg(not(any(windows, target_os = "macos")))]
    let root = match std::env::var_os("XDG_DATA_HOME") {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(std::env::var_os("HOME").context("set HOME or pass --state PATH")?)
            .join(".local/share"),
    };
    Ok(root.join("switchboard"))
}

#[cfg(not(test))]
fn identity(options: &Options) -> Result<SecretKey> {
    let mut directory = fs::DirBuilder::new();
    directory.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        directory.mode(0o700);
    }
    directory.create(&options.state).context("create identity directory")?;
    let path = options.state.join("identity.key");
    match fs::read(&path) {
        Ok(bytes) => {
            let seed: [u8; 32] = bytes.try_into().map_err(|_| anyhow::anyhow!("{} must contain exactly 32 bytes", path.display()))?;
            Ok(SecretKey::from_bytes(&seed))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let key = SecretKey::generate();
            let mut file = OpenOptions::new();
            file.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                file.mode(0o600);
            }
            file.open(&path).context("create identity key")?.write_all(&key.to_bytes())?;
            Ok(key)
        }
        Err(error) => Err(error).with_context(|| format!("read identity {}", path.display())),
    }
}

#[cfg(not(test))]
async fn drive_peers(
    driver: &IrohDriver,
    peers: &[EndpointAddr],
    runtime: &Runtime,
    owner: ActorAddress,
    mut effects: tokio::sync::mpsc::UnboundedReceiver<Effect>,
    telemetry_wake: tokio::sync::mpsc::Sender<()>,
) -> Result<()> {
    let mut changes = driver.connection_changes();
    loop {
        let message = tokio::select! {
            effect = effects.recv() => match effect.context("node lifecycle closed")? {
                Effect::Observe => {
                    let statuses = driver.join_statuses();
                    NodeIn::Peers(peers.iter().map(|peer| {
                        let id = NodeId(*peer.id.as_bytes());
                        PeerObservation {
                            connected: driver.has_active_connection(&id),
                            joining: statuses.get(&id).is_some_and(|status| matches!(
                                status.phase, JoinPhase::Connecting { .. } | JoinPhase::Sending { .. }
                            )),
                        }
                    }).collect())
                }
                Effect::Join(index) => {
                    driver.join(std::slice::from_ref(&peers[index]));
                    continue;
                }
                Effect::Telemetry => {
                    match telemetry_wake.try_send(()) {
                        Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {
                            bail!("telemetry stopped unexpectedly");
                        }
                    }
                    continue;
                }
            },
            result = changes.changed() => {
                result.context("transport observer closed")?;
                ensure!(*changes.borrow_and_update() != 0, "transport closed");
                NodeIn::Changed
            }
        };
        runtime.send_to(owner, message)
            .map_err(|error| anyhow::anyhow!("deliver transport observation: {error:?}"))?;
    }
}

#[cfg(not(test))]
struct ClientCompletion {
    done: ActorCompletion<Result<i32, String>>,
    result: Option<Result<i32, String>>,
}

#[cfg(not(test))]
impl Drop for ClientCompletion {
    fn drop(&mut self) {
        let result = self.result.take().unwrap_or_else(|| Err("shell client task stopped unexpectedly".to_owned()));
        let _ = self.done.complete(result);
    }
}

#[cfg(not(test))]
struct TerminalObservation {
    runtime: Runtime,
    owner: ActorAddress,
    result: Option<Result<(), String>>,
}

#[cfg(not(test))]
impl Drop for TerminalObservation {
    fn drop(&mut self) {
        let result = self.result.take().unwrap_or_else(|| Err("node task stopped unexpectedly".to_owned()));
        let _ = self.runtime.send_to(self.owner, NodeIn::Stopped(result));
    }
}

#[cfg(not(test))]
async fn stop_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let mut hangup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
            _ = hangup.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

#[cfg(not(test))]
fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let first = args.next();
    if first.as_deref() == Some("connect") {
        let target = args.next().context("usage: switchboard connect <computer>")?;
        ensure!(args.next().is_none(), "usage: switchboard connect <computer>");
        // The CLI has one I/O worker but no node identity, transport, or actors.
        // All async execution still belongs to the engine substrate.
        let engine = Engine::new(
            swactor::runtime::RuntimeParts::new(Default::default()),
            TokioBackend::new(TokioConfig { worker_threads: 1, ..Default::default() })?,
        )?;
        let completion = ActorCompletion::new();
        let observation = ClientCompletion { done: completion.clone(), result: None };
        let tasks = engine.task_spawner();
        let blocking = engine.handle().blocking_work_sender();
        tasks.spawn(async move {
            let mut observation = observation;
            observation.result = Some(tokio::select! {
                result = shell_cli::run(target, &blocking) => result,
                result = stop_signal() => result.map(|()| 130),
            }.map_err(|error| format!("{error:#}")));
        });
        let result = completion.wait().map_err(anyhow::Error::msg);
        drop(engine);
        std::process::exit(result?);
    }
    let Some(mut options) = Options::parse(first.into_iter().chain(args))? else { return Ok(()); };
    let secret = identity(&options)?;
    if options.identity {
        println!("{}", secret.public());
        return Ok(());
    }
    let bootstrap = Bootstrap::load(&options.bootstrap)?;
    let relay = options.relay.take().unwrap_or(bootstrap.relay_url);
    let operator = bootstrap.operator_public_key;
    let is_operator = secret.public() == operator;
    if !is_operator && !options.peers.contains(&operator) {
        options.peers.push(operator);
    }
    ensure!(!options.peers.contains(&secret.public()), "a node cannot be its own peer");
    // Enrollment is public at the operator. Clients admit only the pinned
    // operator and explicitly configured peers, never arbitrary roster entries.
    let mut allow = PeerAllowList::open();
    if !is_operator {
        for peer in &options.peers {
            allow.add_peer(NodeId(*peer.as_bytes()), peer.to_string());
        }
    }
    let (parts, codec, router) = cluster::runtime_parts();
    let runtime = parts.runtime().clone();
    let telemetry = telemetry::NodeTelemetry::new(&options.state, secret.public(), &options.name)?;
    telemetry.install_stats_hook(&runtime);
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default())?)?;
    let clock = engine.handle();
    let mut driver = IrohDriver::with_engine(engine.task_spawner(), clock.clone(), IrohDriverConfig {
        secret_key: Some(secret),
        relay_mode: RelayMode::Custom(relay.clone().into()),
        bind_port: None,
        node: Default::default(),
        peer_auth: Some(Arc::new(Mutex::new(allow))),
        additional_alpns: vec![iroh_driver::TELEMETRY_ALPN.to_vec(), remote_shell::ALPN.to_vec()],
    }).map_err(|error| anyhow::anyhow!(error.to_string()))?;
    driver.retain_telemetry_connections();
    let membership = cluster::install(&mut driver, runtime.clone(), codec, router, options.name.clone())
        .map_err(|error| anyhow::anyhow!("start distribution actors: {error:?}"))?;
    let driver = Arc::new(driver);
    let peers: Vec<_> = options.peers.iter().map(|peer|
        EndpointAddr::new(*peer).with_relay_url(relay.clone())
    ).collect();
    let completion = ActorCompletion::new();
    let (effects, effect_requests) = tokio::sync::mpsc::unbounded_channel();
    let owner = runtime.spawn(Lifecycle::new(
        clock.clone(), runtime.create_sender(), effects, completion.clone(),
    )).map_err(|error| anyhow::anyhow!("start node lifecycle: {error:?}"))?;
    let dashboard = DashboardHandle::new(DashboardConfig {
        port: options.dashboard_port,
        ..Default::default()
    });
    let shell_service = Arc::new(remote_shell::Service {
        tasks: engine.task_spawner(), clock: clock.clone(), runtime: runtime.clone(), driver: driver.clone(),
        membership: membership.clone(), operator, name: options.name.clone(), shell: options.shell,
    });
    let telemetry_wake = telemetry.spawn(
        &engine.task_spawner(), &clock, driver.clone(), dashboard.clone(), membership, operator,
    );
    let server = dashboard.http_server();
    let terminal = TerminalObservation { runtime: runtime.clone(), owner, result: None };
    engine.task_spawner().spawn(async move {
        let mut terminal = terminal;
        tokio::pin!(server);
        let remote_shells = shell_service.run();
        tokio::pin!(remote_shells);
        let result = async {
            let address = tokio::select! {
                address = dashboard.wait_until_ready() => address?,
                _ = &mut server => bail!("dashboard stopped during startup"),
            };
            println!("Node: {}\nRole: {}\nOperator: {operator}\nDashboard: http://{address}/", driver.listen_addr(), if is_operator { "operator (public enrollment)" } else { "member" });
            tokio::select! {
                result = stop_signal() => result,
                result = drive_peers(&driver, &peers, &runtime, owner, effect_requests, telemetry_wake) => result,
                result = &mut remote_shells => result.context("shell session service stopped"),
                _ = &mut server => bail!("dashboard stopped unexpectedly"),
            }
        }.await;
        dashboard.shutdown();
        driver.close().await;
        terminal.result = Some(result.map_err(|error| error.to_string()));
    });
    let result = completion.wait().map_err(anyhow::Error::msg);
    drop(engine);
    result
}
