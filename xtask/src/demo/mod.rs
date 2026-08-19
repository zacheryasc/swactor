//! `cargo xtask demo` — a visual, human-checked E2E
//! sanity scenario for the provisioning reconciler.
//!
//! Supervisor role (default): a lightweight orchestrator — swactor engine +
//! real `ClusterDriver` + demo provider (real node-role children over iroh) +
//! telemetry + dashboard. Node role (`--demo-node <supervisor-addr-json>`):
//! re-exec of this binary as a real swactor runtime that joins the
//! supervisor's iroh endpoint.
//!
//! Humans watch the Fleet Control view (reconciler current-vs-desired, node
//! stages, command/result feeds, kill/provision controls) and the fleet cards
//! (per-node PID/state), kill nodes from Fleet Control or a shell, and watch
//! the reconciler replace them for real.

pub mod bootstrap;
pub mod control;
pub mod docker;
pub mod edge;
pub mod feed;
pub mod node;
pub mod provider;
pub mod view;

/// How the supervisor launches node-role children (bootstrap kind + launch
/// facts). Selected by `--docker`; default is local processes.
#[derive(Clone)]
pub enum LaunchStyle {
    /// Re-exec this binary as a local child process (`kind: "process"`).
    Process { exe: std::path::PathBuf },
    /// Run the demo docker image per node on the per-run bridge network
    /// (`kind: "docker"`) — nodes are foreign: own IPs, gateway-dialed
    /// supervisor, no shared filesystem.
    Docker(docker::DockerLaunch),
}

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use iroh::RelayMode;
use swactor::config::RuntimeConfig;
use swactor::runtime::RuntimeParts;
use swactor_engine::{Engine, TokioBackend, TokioConfig};

use distribution::node::DistributedNodeConfig;
use iroh_driver::{IrohDriver, IrohDriverConfig};
use provisioning::executor::IdempotentEffectExecutor;
use provisioning::reconciler::ClusterDriver;
use provisioning::{ClusterShape, RunId};

use feed::{
    EngineSpawner, SupervisorActor, SupervisorMsg, SupervisorTelemetry, demo_retry_policy,
    initial_slots,
};
use provider::{DemoBackend, DemoProvider, NodeManager};

/// Supervisor tick period.
pub const TICK: Duration = Duration::from_millis(250);
/// Node heartbeat period (node role writes a key-file line this often).
pub const HEARTBEAT_PERIOD: Duration = Duration::from_secs(2);
/// Default dashboard port.
pub const DEFAULT_PORT: u16 = 9871;
/// Default initial cluster size.
pub const DEFAULT_NODES: u64 = 3;

/// Resolve the executable path for node-role re-execs. `current_exe()`
/// returns a "(deleted)" path or fails once the binary file has been
/// replaced by a rebuild while this process runs, so fall back through
/// argv[0] and PATH.
fn resolve_exe() -> std::path::PathBuf {
    if let Ok(path) = std::env::current_exe() {
        if !path.to_string_lossy().ends_with(" (deleted)") {
            return path;
        }
    }
    if let Some(arg0) = std::env::args_os().next() {
        let candidate = std::path::PathBuf::from(&arg0);
        if candidate.is_absolute() {
            return candidate;
        }
        if let Ok(cwd) = std::env::current_dir() {
            let joined = cwd.join(&candidate);
            if joined.exists() {
                return joined;
            }
        }
        if let Ok(path_var) = std::env::var("PATH") {
            for dir in path_var.split(':') {
                let joined = std::path::PathBuf::from(dir).join(&candidate);
                if joined.exists() {
                    return joined;
                }
            }
        }
    }
    std::path::PathBuf::from("xtask")
}

/// Shared iroh driver handle: the supervisor's endpoint address (handed to
/// node roles) and the endpoint for outbound telemetry pulls.
pub struct DemoDriverHandle {
    pub supervisor_addr_json: String,
    pub(crate) driver: std::sync::Arc<IrohDriver>,
}

impl DemoDriverHandle {
    /// Clone the iroh endpoint for outbound telemetry pulls.
    pub fn endpoint(&self) -> iroh::Endpoint {
        self.driver.endpoint()
    }
}

/// Pull collector fired by bootstrap actors: dials the freshly-bootstrapped
/// node on `TELEMETRY_ALPN`, sends the subscription request, and streams its
/// telemetry into the supervisor's remote-stream fanout. The header lands in
/// the supervisor actor so frames can be classified before publishing.
struct DemoTelemetryCollector {
    engine: swactor_engine::EngineHandle,
    endpoint: iroh::Endpoint,
    fanout: Arc<telemetry::DeliveryFanout>,
    sender: swactor::runtime::ExternalSender,
    supervisor_slot: Arc<std::sync::OnceLock<swactor::actor::ActorAddress>>,
}

impl provisioning::NodeTelemetryCollector for DemoTelemetryCollector {
    fn collect(&self, identity: &provisioning::NodeIdentity) {
        let Ok(addr) = serde_json::from_str::<iroh::EndpointAddr>(&identity.transport_addr) else {
            eprintln!(
                "demo: node {} advertised unparsable endpoint address",
                identity.logical_node
            );
            return;
        };
        println!(
            "demo: pulling telemetry from node {} (key {}…)",
            identity.logical_node,
            &identity.key_hex[..8.min(identity.key_hex.len())]
        );
        let mut flow_id = [0u8; 16];
        flow_id[..8].copy_from_slice(&identity.attempt.to_le_bytes());
        let (header_tx, header_rx) = std::sync::mpsc::channel();
        iroh_driver::spawn_pull_collector(
            &self.engine,
            self.endpoint.clone(),
            addr,
            flow_id,
            Vec::new(),
            telemetry::SubscriptionRequest::all(),
            Arc::clone(&self.fanout),
            header_tx,
        );
        let sender = self.sender.clone();
        if let Some(supervisor) = self.supervisor_slot.get() {
            let supervisor = supervisor.clone();
            let logical_node = identity.logical_node.clone();
            let attempt = identity.attempt;
            std::thread::spawn(move || {
                if let Ok(header) = header_rx.recv() {
                    let _ = sender.send_to(
                        supervisor,
                        feed::SupervisorMsg::NodeStream {
                            header,
                            logical_node,
                            attempt,
                        },
                    );
                }
            });
        }
    }
}

/// Entry point: `demo [--port N] [--nodes N]
/// [--docker]` for the supervisor, or `--demo-node <supervisor-addr-json>
/// --demo-attempt <n>` for node children.
pub fn run(args: &[String]) -> ExitCode {
    if let Some(index) = args.iter().position(|arg| arg == "--demo-node") {
        let addr = args.get(index + 1).map(String::as_str).unwrap_or_default();
        let attempt = arg_value(args, "--demo-attempt").and_then(|value| value.parse::<u64>().ok());
        let Some(attempt) = attempt else {
            eprintln!("demo node: --demo-attempt <n> is required");
            return ExitCode::FAILURE;
        };
        return match node::run_node_role(addr, attempt) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("demo node: {error}");
                ExitCode::FAILURE
            }
        };
    }
    match run_supervisor(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("demo: {error}");
            ExitCode::FAILURE
        }
    }
}

fn arg_value(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|index| args.get(index + 1).cloned())
}
fn run_supervisor(args: &[String]) -> Result<(), String> {
    let port: u16 = arg_value(args, "--port")
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_PORT);
    let nodes: u64 = arg_value(args, "--nodes")
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_NODES);
    let docker_mode = args.iter().any(|arg| arg == "--docker");

    // Node registry + spawn channel: needed before the actor bridge so the
    // announce relay can route wire announces into bootstrap actors.
    let manager = NodeManager::new();
    let (spawn_tx, spawn_rx) = std::sync::mpsc::channel::<provider::SpawnNodeRequest>();
    manager.set_spawn_channel(spawn_tx);

    // Telemetry endpoint with the runtime stats hook attached before the
    // engine takes the parts.
    let mut telemetry = SupervisorTelemetry::new("supervisor");
    let actors_channel = telemetry.register("runtime.actors");
    let stats_hook = telemetry.producer.stats_hook_on(actors_channel);

    let mut parts = RuntimeParts::new(RuntimeConfig::default());
    parts = parts.with_stats_hook(stats_hook);
    let runtime = parts.runtime().clone();
    let sender = runtime.create_sender();
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).map_err(|e| format!("backend: {e}"))?,
    )
    .map_err(|e| format!("engine: {e}"))?;

    // iroh: accept joins from node-role children. The minimal actor bridge
    // exists so the production adapter pump folds accepted connections into
    // the driver cache — `has_active_connection` is our real join signal.
    // Frames that decode against the (empty) codec registry drop harmlessly.
    let mut driver = IrohDriver::with_engine(
        engine.handle(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: RelayMode::Disabled,
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![],
        },
    )
    .map_err(|e| format!("iroh driver: {e}"))?;
    let supervisor_addr_json =
        serde_json::to_string(&driver.endpoint_addr()).map_err(|e| format!("addr: {e}"))?;
    // The supervisor's address travels through a shared slot; long-lived
    // engine tasks installed below wait for it lazily. (Spawning engine
    // tasks after the actor spawn proved flaky at startup.)
    let supervisor_slot: Arc<std::sync::OnceLock<swactor::actor::ActorAddress>> =
        Arc::new(std::sync::OnceLock::new());
    {
        use distribution::transport_bridge::{Outbox, RelayMirror, RouteView};
        use swactor_transport::CodecRegistry;

        // The announce relay is the bridge's only decoded ingress: node
        // roles announce over the control plane (tag-routed gossip), and
        // the relay correlates by attempt → bootstrap actor. The
        // `swim_addr` argument only names a SendFailed target, which never
        // fires for inbound frames; the relay doubles for it. Undecodable
        // frames (e.g. the driver's own JoinRequest) still drop harmlessly.
        let announce = runtime
            .spawn(provider::AnnounceActor::new(
                manager.clone(),
                sender.clone(),
            ))
            .map_err(|e| format!("spawn announce actor: {e}"))?;
        let ack_relay = runtime
            .spawn(edge::EdgeAckRelay::new(
                sender.clone(),
                supervisor_slot.clone(),
            ))
            .map_err(|e| format!("spawn edge ack relay: {e}"))?;
        let mut codec = CodecRegistry::new();
        codec.register_decoder::<node::NodeAnnounce>(node::ANNOUNCE_TAG, |bytes| {
            serde_json::from_slice(bytes)
                .map_err(|e| swactor::Error::from(format!("announce decode: {e}")))
        });
        codec.register_decoder::<edge::EdgeAck>(edge::EDGE_ACK_TAG, |bytes| {
            serde_json::from_slice(bytes)
                .map_err(|e| swactor::Error::from(format!("edge ack decode: {e}")))
        });
        let mut routes = std::collections::HashMap::new();
        routes.insert(node::ANNOUNCE_TAG.to_owned(), announce);
        routes.insert(edge::EDGE_ACK_TAG.to_owned(), ack_relay);
        let relay_mirror: RelayMirror =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let route_view: RouteView =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let outbox: Outbox = Arc::new(std::sync::Mutex::new(Vec::new()));
        driver.enable_actor_bridge(
            runtime.clone(),
            Arc::new(codec),
            routes,
            announce,
            relay_mirror,
            route_view,
            outbox,
        );
        driver.install_actor_bridge_pump(Duration::from_millis(250));
    }

    let driver_handle = Arc::new(DemoDriverHandle {
        supervisor_addr_json: supervisor_addr_json.clone(),
        driver: Arc::new(driver),
    });

    // Dashboard.
    let dashboard = dashboard::DashboardHandle::new(dashboard::DashboardConfig {
        port,
        ..dashboard::DashboardConfig::default()
    });
    dashboard.register_view(Arc::new(view::ReconcilerDashboardView::default()));
    engine.handle().spawn(dashboard.http_server());

    // Provisioning: driver + plugin + executor.
    let launch = if docker_mode {
        let addrs = driver_handle.driver.direct_addresses();
        let port = addrs
            .iter()
            .find(|addr| addr.is_ipv4())
            .or_else(|| addrs.first())
            .map(|addr| addr.port())
            .ok_or_else(|| "supervisor has no bound direct address".to_owned())?;
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask manifest dir has a parent")
            .to_path_buf();
        let launch = docker::preflight(&root, driver_handle.driver.endpoint().id(), port)?;
        LaunchStyle::Docker(launch)
    } else {
        LaunchStyle::Process { exe: resolve_exe() }
    };

    let slots = initial_slots(nodes);
    let shape = ClusterShape {
        run_id: RunId(1),
        generation: 1,
        groups: slots.iter().map(|slot| feed::demo_group(slot, 1)).collect(),
    };
    let cluster_driver =
        ClusterDriver::new(shape, demo_retry_policy()).map_err(|e| format!("driver: {e}"))?;

    let plugin = DemoProvider::new(manager.clone());
    let spawner = EngineSpawner::new(engine.handle());
    let executor = IdempotentEffectExecutor::new(
        DemoBackend {
            plugin: Arc::new(std::sync::Mutex::new(plugin)),
            manager: manager.clone(),
            sender: sender.clone(),
        },
        spawner,
    );

    // Bootstrap machinery: kind registry with the process logic, the
    // remote-stream fanout, and the pull collector.
    let fanout = Arc::new(telemetry::DeliveryFanout::new(1024));
    let remote_sub = fanout.subscribe_all(
        "dashboard",
        telemetry::TelemetrySnapshot {
            streams: Vec::new(),
            channels: Vec::new(),
        },
    );
    let mut bootstrap_registry = provisioning::BootstrapRegistry::new();
    bootstrap_registry.register("process", {
        let manager = manager.clone();
        Arc::new(move |spec| {
            Ok(Box::new(bootstrap::LocalProcessLogic::new(
                spec.clone(),
                manager.clone(),
            )) as Box<dyn provisioning::BootstrapLogic>)
        })
    });
    bootstrap_registry.register("docker", {
        let manager = manager.clone();
        Arc::new(move |spec| {
            Ok(Box::new(docker::DockerProcessLogic::new(
                spec.clone(),
                manager.clone(),
            )) as Box<dyn provisioning::BootstrapLogic>)
        })
    });
    let edge_driver = std::sync::Arc::clone(&driver_handle.driver);
    let collector: Arc<dyn provisioning::NodeTelemetryCollector> =
        Arc::new(DemoTelemetryCollector {
            engine: engine.handle(),
            endpoint: driver_handle.endpoint(),
            fanout: Arc::clone(&fanout),
            sender: sender.clone(),
            supervisor_slot: supervisor_slot.clone(),
        });

    let (edge_cmd_tx, edge_cmd_rx) = std::sync::mpsc::channel::<edge::EdgePumpCmd>();
    let supervisor = SupervisorActor::new(
        cluster_driver,
        executor,
        manager,
        driver_handle,
        telemetry,
        dashboard.clone(),
        sender.clone(),
        bootstrap_registry,
        collector,
        engine.handle(),
        remote_sub,
        slots,
        RunId(1),
        launch.clone(),
        edge_cmd_tx,
    );

    // Control plane: dashboard → supervisor.
    control::install(&engine.handle(), sender.clone(), supervisor_slot.clone());

    // Spawn-request pump: provider blocking threads → supervisor actor.
    let pump_sender = sender.clone();
    let pump_slot = supervisor_slot.clone();
    engine.handle().spawn_blocking(move || {
        loop {
            match spawn_rx.recv() {
                Ok(request) => {
                    if let Some(addr) = pump_slot.get() {
                        let _ = pump_sender.send_to(addr.clone(), SupervisorMsg::Spawn(request));
                    }
                }
                Err(_) => return,
            }
        }
    });

    // Tick: engine interval → supervisor actor.
    let tick_sender = sender.clone();
    let tick_engine = engine.handle();
    let interval_engine = tick_engine.clone();
    let tick_slot = supervisor_slot.clone();
    tick_engine.spawn(async move {
        let mut interval = interval_engine.interval(TICK);
        loop {
            (&mut interval).await;
            if let Some(addr) = tick_slot.get() {
                let _ = tick_sender.send_to(addr.clone(), SupervisorMsg::Tick);
            }
        }
    });

    let supervisor_addr = runtime
        .spawn(supervisor)
        .map_err(|e| format!("spawn supervisor actor: {e}"))?;
    supervisor_slot
        .set(supervisor_addr.clone())
        .expect("supervisor address slot set once");

    // Edge pump on the blocking pool: it solely owns the edge sessions —
    // edge runtime polls block their thread (connect handshakes), which
    // must never run on a Tokio worker or hold a lock the actor needs.
    edge::start_edge_pump(
        &engine.handle(),
        edge_driver,
        edge_cmd_rx,
        sender.clone(),
        supervisor_slot.clone(),
    );

    println!("demo: dashboard on http://localhost:{port}");
    println!("  /view/fleet        — per-node cards (pid, lifecycle)");
    println!("  /view/demo-control — Fleet Control: stages, feeds, kill / provision / edge");
    println!("  Ctrl-C to tear down.");

    // Block until Ctrl-C (synchronous signal flag — the wait must not depend
    // on engine task progression).
    install_sigint_flag();
    while !sigint_requested() {
        std::thread::sleep(Duration::from_millis(100));
    }

    // Teardown: drain the cluster through the real destroy path (desired →
    // empty → BeginDelete → DestroyLease → process actors stop children),
    // pumping ticks until the driver quiesces.
    let drained = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _ = sender.send_to(
        supervisor_addr.clone(),
        SupervisorMsg::Shutdown {
            drained: drained.clone(),
        },
    );
    let mut settle_ticks = 0_u32;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        if drained.load(std::sync::atomic::Ordering::SeqCst) {
            // Give DestroyLease results a few extra ticks to land.
            settle_ticks += 1;
            if settle_ticks >= 8 {
                break;
            }
        }
        std::thread::sleep(TICK);
    }
    // Best-effort drain window closed: children still alive (if any) are
    // killed by the kernel parent-death signal armed in the node role
    // (process kind) or swept by label below (docker kind).
    if let LaunchStyle::Docker(docker) = &launch {
        docker::sweep_run(docker);
    }
    std::process::exit(0);
}

static SIGINT_REQUESTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn sigint_requested() -> bool {
    SIGINT_REQUESTED.load(std::sync::atomic::Ordering::SeqCst)
}

#[cfg(target_os = "linux")]
fn install_sigint_flag() {
    unsafe {
        let handler: extern "C" fn(libc::c_int) = sigint_handler;
        libc::signal(libc::SIGINT, handler as usize);
        // Supervisors under process managers (systemd, container runtimes,
        // harnesses) stop children with SIGTERM; treat it exactly like
        // Ctrl-C so the drain + sweep path runs instead of a default kill.
        libc::signal(libc::SIGTERM, handler as usize);
    }
}

#[cfg(target_os = "linux")]
extern "C" fn sigint_handler(_signal: libc::c_int) {
    SIGINT_REQUESTED.store(true, std::sync::atomic::Ordering::SeqCst);
}

#[cfg(not(target_os = "linux"))]
fn install_sigint_flag() {
    // Non-Linux builds wait for SIGTERM's default disposition instead.
}
