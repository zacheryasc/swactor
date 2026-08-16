//! `cargo xtask provisioning-reconciler-demo` — a visual, human-checked E2E
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

pub mod control;
pub mod feed;
pub mod node;
pub mod provider;
pub mod view;

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
use provisioning::{ClusterShape, RunId};
use provisioning::reconciler::ClusterDriver;

use feed::{
    demo_retry_policy, initial_slots, EngineSpawner, SupervisorActor, SupervisorMsg,
    SupervisorTelemetry,
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

/// Shared handle the supervisor actor uses to check iroh connections.
pub struct DemoDriverHandle {
    pub supervisor_addr_json: String,
    driver: IrohDriver,
}

impl DemoDriverHandle {
    pub fn has_active_connection(&self, node: swactor_transport::NodeId) -> bool {
        self.driver.has_active_connection(&node)
    }
}

/// Entry point: `provisioning-reconciler-demo [--port N] [--nodes N]` for the
/// supervisor, or `--demo-node <supervisor-addr-json>` for node children.
pub fn run(args: &[String]) -> ExitCode {
    if let Some(index) = args.iter().position(|arg| arg == "--demo-node") {
        let addr = args
            .get(index + 1)
            .map(String::as_str)
            .unwrap_or_default();
        return match node::run_node_role(addr) {
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
            eprintln!("provisioning-reconciler-demo: {error}");
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
    {
        use distribution::transport_bridge::{Outbox, RelayMirror, RouteView};
        use swactor_transport::CodecRegistry;

        struct NoopActor;
        impl swactor::actor::ActorInterface for NoopActor {
            type Incoming = ();
            type Response = ();
            fn handle(&mut self, _ctx: &swactor::actor::Ctx, _msg: ()) {}
        }
        let noop = runtime
            .spawn(NoopActor)
            .map_err(|e| format!("spawn noop actor: {e}"))?;
        let relay_mirror: RelayMirror = Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let route_view: RouteView = Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let outbox: Outbox = Arc::new(std::sync::Mutex::new(Vec::new()));
        driver.enable_actor_bridge(
            runtime.clone(),
            Arc::new(CodecRegistry::new()),
            std::collections::HashMap::new(),
            noop,
            relay_mirror,
            route_view,
            outbox,
        );
        driver.install_actor_bridge_pump(Duration::from_millis(250));
    }
    let driver_handle = Arc::new(DemoDriverHandle {
        supervisor_addr_json: supervisor_addr_json.clone(),
        driver,
    });

    // Dashboard.
    let dashboard = dashboard::DashboardHandle::new(dashboard::DashboardConfig {
        port,
        ..dashboard::DashboardConfig::default()
    });
    dashboard.register_view(Arc::new(view::ReconcilerDashboardView::default()));
    engine.handle().spawn(dashboard.http_server());

    // Provisioning: driver + plugin + executor.
    let keys_dir = std::env::temp_dir().join(format!(
        "provisioning-demo-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&keys_dir).map_err(|e| format!("keys dir: {e}"))?;

    let manager = NodeManager::new();
    let (spawn_tx, spawn_rx) = std::sync::mpsc::channel::<provider::SpawnNodeRequest>();
    manager.set_spawn_channel(spawn_tx);

    let slots = initial_slots(nodes);
    let shape = ClusterShape {
        run_id: RunId(1),
        generation: 1,
        groups: slots.iter().map(|slot| feed::demo_group(slot, 1)).collect(),
    };
    let cluster_driver =
        ClusterDriver::new(shape, demo_retry_policy()).map_err(|e| format!("driver: {e}"))?;

    let plugin = DemoProvider::new(manager.clone(), keys_dir.clone());
    let spawner = EngineSpawner::new(engine.handle());
    let executor = IdempotentEffectExecutor::new(
        DemoBackend {
            plugin: Arc::new(std::sync::Mutex::new(plugin)),
            manager: manager.clone(),
            sender: sender.clone(),
        },
        spawner,
    );

    let supervisor = SupervisorActor::new(
        cluster_driver,
        executor,
        manager,
        driver_handle,
        telemetry,
        dashboard.clone(),
        sender.clone(),
        slots,
        RunId(1),
        resolve_exe(),
    );
    // Long-lived engine tasks are installed BEFORE the actor spawn: the
    // supervisor's address travels through a shared slot, and the tasks wait
    // for it lazily. (Spawning engine tasks after the actor spawn proved
    // flaky at startup.)
    let supervisor_slot: Arc<std::sync::OnceLock<swactor::actor::ActorAddress>> =
        Arc::new(std::sync::OnceLock::new());

    // Control plane: dashboard → supervisor.
    control::install(&engine.handle(), sender.clone(), supervisor_slot.clone());

    // Spawn-request pump: provider blocking threads → supervisor actor.
    let pump_sender = sender.clone();
    let pump_slot = supervisor_slot.clone();
    engine.handle().spawn_blocking(move || loop {
        match spawn_rx.recv() {
            Ok(request) => {
                if let Some(addr) = pump_slot.get() {
                    let _ = pump_sender.send_to(addr.clone(), SupervisorMsg::Spawn(request));
                }
            }
            Err(_) => return,
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

    println!("provisioning-reconciler-demo: dashboard on http://localhost:{port}");
    println!("  /view/fleet        — per-node cards (pid, lifecycle)");
    println!("  /view/demo-control — Fleet Control: stages, feeds, kill / provision");
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
    // killed by the kernel parent-death signal armed in the node role.
    let _ = std::fs::remove_dir_all(&keys_dir);
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
