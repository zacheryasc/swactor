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
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::{ExternalSender, Runtime, RuntimeParts};
use swactor_engine::{ActorCompletion, Engine, TokioBackend, TokioConfig};

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

#[cfg(test)]
pub(super) fn shared_test_driver() -> Arc<IrohDriver> {
    thread_local! {
        static DRIVER: (Arc<IrohDriver>, Engine) = {
            let mut config = RuntimeConfig::default();
            config.worker_count = 1;
            let parts = RuntimeParts::new(config);
            let backend = TokioBackend::new(TokioConfig::default())
                .expect("create shared demo test backend");
            let engine =
                Engine::new(parts, backend).expect("create shared demo test I/O engine");
            let driver = Arc::new(
                IrohDriver::with_engine(
                    engine.handle(),
                    IrohDriverConfig {
                        secret_key: None,
                        relay_mode: RelayMode::Disabled,
                        node: DistributedNodeConfig::default(),
                        peer_auth: None,
                        additional_alpns: vec![],
                    },
                )
                .expect("create shared demo test driver"),
            );
            (driver, engine)
        };
    }
    DRIVER.with(|(driver, _)| Arc::clone(driver))
}

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
    runtime: Runtime,
    endpoint: iroh::Endpoint,
    fanout: Arc<telemetry::DeliveryFanout>,
    sender: swactor::runtime::ExternalSender,
    supervisor_slot: Arc<std::sync::OnceLock<ActorAddress>>,
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
        let Some(supervisor) = self.supervisor_slot.get().copied() else {
            return;
        };
        let logical_node = identity.logical_node.clone();
        let attempt = identity.attempt;
        let header_actor = match self.runtime.spawn(PullHeaderActor {
            sender: self.sender.clone(),
            supervisor,
            logical_node,
            attempt,
        }) {
            Ok(actor) => actor,
            Err(error) => {
                eprintln!("demo: cannot spawn telemetry header actor: {error}");
                return;
            }
        };
        iroh_driver::spawn_pull_collector_to_actor(
            &self.engine,
            self.endpoint.clone(),
            addr,
            flow_id,
            Vec::new(),
            telemetry::SubscriptionRequest::all(),
            Arc::clone(&self.fanout),
            self.sender.clone(),
            header_actor,
        );
    }
}

struct PullHeaderActor {
    sender: swactor::runtime::ExternalSender,
    supervisor: ActorAddress,
    logical_node: String,
    attempt: u64,
}

impl ActorInterface for PullHeaderActor {
    type Incoming = iroh_driver::TelemetryQuicHeader;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, header: Self::Incoming) {
        let _ = self.sender.send_to(
            self.supervisor,
            feed::SupervisorMsg::NodeStream {
                header,
                logical_node: self.logical_node.clone(),
                attempt: self.attempt,
            },
        );
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

    // Node registry: wire announces and provisioning effects converge through
    // the supervisor actor once it is installed below.
    let manager = NodeManager::new();

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
    dashboard.spawn(&engine.handle());

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
    let spawner = EngineSpawner::new(&engine.handle());
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
            runtime: runtime.clone(),
            endpoint: driver_handle.endpoint(),
            fanout: Arc::clone(&fanout),
            sender: sender.clone(),
            supervisor_slot: supervisor_slot.clone(),
        });

    let edge_actor = edge::start_edge_pump(
        &runtime,
        engine.handle(),
        edge_driver,
        sender.clone(),
        supervisor_slot.clone(),
    )?;
    let supervisor = SupervisorActor::new(
        cluster_driver,
        executor,
        manager.clone(),
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
        edge_actor,
    );

    // Control plane: dashboard → supervisor.
    control::install(&runtime, supervisor_slot.clone());

    let supervisor_addr = runtime
        .spawn(supervisor)
        .map_err(|e| format!("spawn supervisor actor: {e}"))?;
    supervisor_slot
        .set(supervisor_addr.clone())
        .expect("supervisor address slot set once");
    manager.set_spawn_actor(sender.clone(), supervisor_slot.clone());

    let completion = ActorCompletion::new();
    let stop_actor = runtime
        .spawn(SupervisorStopActor {
            sender: sender.clone(),
            supervisor: supervisor_addr,
            completion: completion.clone(),
        })
        .map_err(|error| format!("spawn supervisor stop actor: {error}"))?;
    #[cfg(target_os = "linux")]
    swactor_process::spawn_os_stop_signal_wait(runtime.create_sender(), stop_actor);
    println!("demo: dashboard on http://localhost:{port}");
    println!("  /view/fleet        — per-node cards (pid, lifecycle)");
    println!("  /view/demo-control — Fleet Control: stages, feeds, kill / provision / edge");
    println!("  Ctrl-C to tear down.");
    completion.wait();

    if let LaunchStyle::Docker(docker) = &launch {
        docker::sweep_run(docker);
    }
    Ok(())
}

struct SupervisorStopActor {
    sender: ExternalSender,
    supervisor: ActorAddress,
    completion: ActorCompletion<()>,
}

impl ActorInterface for SupervisorStopActor {
    type Incoming = swactor_process::ProcessStopSignal;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, _message: Self::Incoming) {
        let _ = self.sender.send_to(
            self.supervisor,
            SupervisorMsg::Shutdown {
                completion: self.completion.clone(),
            },
        );
        ctx.stop_self();
    }
}

#[cfg(all(test, target_os = "linux"))]
mod properties {
    use std::process::{Command, Stdio};
    use std::time::Duration;

    use swactor_process::ProcessExitObservation;

    use super::*;

    #[derive(Clone, Debug)]
    enum ReadinessObservation {
        Line(String),
        Error(String),
        Closed,
        Timeout,
    }

    struct ReadinessObserver {
        pid: i32,
        completion: ActorCompletion<Result<(), String>>,
    }

    impl ReadinessObserver {
        fn fail(&self, ctx: &Ctx, error: String) {
            let _ = unsafe { libc::kill(self.pid, libc::SIGKILL) };
            let _ = self.completion.complete(Err(error));
            ctx.stop_self();
        }
    }

    impl ActorInterface for ReadinessObserver {
        type Incoming = ReadinessObservation;
        type Response = ();

        fn handle(&mut self, ctx: &Ctx, observation: Self::Incoming) {
            match observation {
                ReadinessObservation::Line(line) if line.contains("demo: dashboard on") => {
                    if unsafe { libc::kill(self.pid, libc::SIGTERM) } != 0 {
                        self.fail(
                            ctx,
                            format!(
                                "send SIGTERM to direct demo binary: {}",
                                std::io::Error::last_os_error()
                            ),
                        );
                    } else {
                        ctx.stop_self();
                    }
                }
                ReadinessObservation::Line(_) => {}
                ReadinessObservation::Error(error) => {
                    self.fail(ctx, format!("read direct demo stdout: {error}"));
                }
                ReadinessObservation::Closed => {
                    self.fail(ctx, "direct demo stdout closed before readiness".to_owned());
                }
                ReadinessObservation::Timeout => {
                    self.fail(
                        ctx,
                        "direct demo binary did not become ready within ten seconds".to_owned(),
                    );
                }
            }
        }
    }

    struct ExitObserver {
        pid: i32,
        completion: ActorCompletion<Result<(), String>>,
    }

    impl ActorInterface for ExitObserver {
        type Incoming = ProcessExitObservation;
        type Response = ();

        fn handle(&mut self, ctx: &Ctx, observation: Self::Incoming) {
            let result = match (observation.status, observation.error) {
                (Some(0), None) => Ok(()),
                (status, Some(error)) => {
                    let _ = unsafe { libc::kill(self.pid, libc::SIGKILL) };
                    Err(format!(
                        "direct demo process observation failed: status={status:?}, error={error}"
                    ))
                }
                (status, None) => Err(format!(
                    "direct demo binary did not exit cleanly after SIGTERM: status={status:?}"
                )),
            };
            let _ = self.completion.complete(result);
            ctx.stop_self();
        }
    }

    #[test]
    #[ignore = "subprocess entrypoint for direct_binary_signal_smoke_has_a_hard_timeout"]
    fn direct_binary_signal_smoke_child() {
        run_supervisor(&[
            "--nodes".to_owned(),
            "0".to_owned(),
            "--port".to_owned(),
            "0".to_owned(),
        ])
        .expect("empty demo supervisor exits after its OS stop signal");
    }

    #[test]
    fn direct_binary_signal_smoke_has_a_hard_timeout() {
        const CHILD_TEST: &str = "demo::properties::direct_binary_signal_smoke_child";
        const HARD_TIMEOUT: Duration = Duration::from_secs(10);

        let mut command = Command::new(std::env::current_exe().expect("current xtask test binary"));
        command
            .args(["--ignored", "--exact", CHILD_TEST, "--nocapture"])
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let mut child =
            swactor_process::command_spawn(&mut command).expect("spawn direct xtask test binary");
        let pid = child.id() as i32;
        let stdout = child.stdout.take().expect("capture demo supervisor stdout");

        let mut config = RuntimeConfig::default();
        config.worker_count = 1;
        let parts = RuntimeParts::new(config);
        let runtime = parts.runtime().clone();
        let engine = Engine::new(
            parts,
            TokioBackend::new(TokioConfig::default()).expect("create signal smoke backend"),
        )
        .expect("create signal smoke engine");
        let sender = runtime.create_sender();
        let completion = ActorCompletion::new();
        let readiness = runtime
            .spawn(ReadinessObserver {
                pid,
                completion: completion.clone(),
            })
            .expect("spawn readiness observer");
        let exit = runtime
            .spawn(ExitObserver {
                pid,
                completion: completion.clone(),
            })
            .expect("spawn exit observer");

        swactor_process::spawn_mapped_line_reader(
            stdout,
            sender.clone(),
            readiness,
            ReadinessObservation::Line,
            ReadinessObservation::Error,
            ReadinessObservation::Closed,
        );
        swactor_process::spawn_child_wait(child, sender.clone(), exit);
        let _readiness_timeout = engine.handle().send_after(
            HARD_TIMEOUT,
            sender.clone(),
            readiness,
            ReadinessObservation::Timeout,
        );
        let _exit_timeout = engine.handle().send_after(
            HARD_TIMEOUT,
            sender,
            exit,
            ProcessExitObservation {
                status: None,
                error: Some("demo subprocess exceeded ten-second hard timeout".to_owned()),
            },
        );

        completion.wait().expect("direct demo signal smoke");
    }
}
