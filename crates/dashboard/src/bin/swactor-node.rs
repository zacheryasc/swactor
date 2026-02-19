use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use clap::Parser;

use swactor::actor::{ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;

use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::snapshot::DistributionNodeSnapshot;
use distribution::swim::probe::SwimConfig;

use dashboard::collector::StatsCollector;
use dashboard::distribution_collector::DistributionStatsProvider;
use dashboard::{start_dashboard, DashboardConfig};

// ── CLI ──────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "swactor-node", about = "Swactor distributed node")]
struct Args {
    /// Transport to use: tcp or iroh
    #[arg(long, default_value = "tcp")]
    transport: String,

    /// Address to listen on for TCP transport (e.g. 10.0.1.10:7000)
    #[arg(long)]
    listen: Option<std::net::SocketAddr>,

    /// Seed node address to join (TCP mode: host:port)
    #[arg(long)]
    seed: Option<String>,

    /// Seed node's iroh public key (iroh mode: hex-encoded 32-byte key)
    #[arg(long)]
    seed_node_id: Option<String>,

    /// Dashboard HTTP port
    #[arg(long, default_value = "9090")]
    dashboard_port: u16,

    /// Number of dummy actors to register in the directory
    #[arg(long, default_value = "0")]
    actors: usize,
}

// ── Dummy actor ──────────────────────────────────────────────────────────

#[derive(Clone)]
struct Heartbeat;

struct HeartbeatActor;

impl ActorInterface for HeartbeatActor {
    type Incoming = Heartbeat;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Heartbeat) {}
}

// ── Snapshot provider ────────────────────────────────────────────────────

struct SnapshotProvider {
    snapshot: Arc<Mutex<Option<DistributionNodeSnapshot>>>,
}

impl DistributionStatsProvider for SnapshotProvider {
    fn snapshot(&self) -> Option<DistributionNodeSnapshot> {
        self.snapshot.lock().unwrap().clone()
    }
}

// ── Main ─────────────────────────────────────────────────────────────────

fn main() {
    let args = Args::parse();
    let stop = Arc::new(AtomicBool::new(false));

    // Handle SIGTERM / Ctrl+C
    {
        let stop = Arc::clone(&stop);
        ctrlc::set_handler(move || {
            stop.store(true, Ordering::Relaxed);
        })
        .expect("failed to set signal handler");
    }

    // Start dashboard
    let dash = start_dashboard(DashboardConfig {
        port: args.dashboard_port,
        ..Default::default()
    });
    dash.install_tracing();
    dash.start_http_standalone();

    // Create actor runtime
    let num_threads = 2;
    let collector = StatsCollector::new(num_threads);
    let mut rt = Runtime::new(RuntimeConfig {
        num_threads,
        max_actors: 1024,
        channel_buffer_size: 2000,
        ..Default::default()
    });
    rt.set_stats_hook(collector.clone());

    let handle = rt.run().expect("failed to start runtime");
    dash.set_runtime(handle.runtime.clone(), collector);

    // Distribution config (shared between transports)
    let swim_config = SwimConfig {
        probe_interval: 5,
        probe_timeout: 3,
        indirect_probes: 2,
        suspicion_timeout: 20,
        dead_reprobe_interval: 50,
    };
    let node_config = DistributedNodeConfig {
        swim: swim_config,
        cache_capacity: 1000,
        republish_interval: 500,
        ..Default::default()
    };

    match args.transport.as_str() {
        #[cfg(feature = "tcp")]
        "tcp" => run_tcp(args, node_config, &handle, &dash, &stop),
        #[cfg(feature = "iroh")]
        "iroh" => run_iroh(args, node_config, &handle, &dash, &stop),
        other => {
            eprintln!("Unknown or unavailable transport: {other}");
            eprintln!("Available transports:");
            #[cfg(feature = "tcp")]
            eprintln!("  tcp");
            #[cfg(feature = "iroh")]
            eprintln!("  iroh");
            std::process::exit(1);
        }
    }

    eprintln!("\nShutting down...");
    handle.shutdown();
    dash.shutdown();
    handle.join();
}

// ── TCP transport ────────────────────────────────────────────────────────

#[cfg(feature = "tcp")]
fn run_tcp(
    args: Args,
    node_config: DistributedNodeConfig,
    handle: &swactor::runtime::RuntimeHandle,
    dash: &dashboard::DashboardHandle,
    stop: &Arc<AtomicBool>,
) {
    use distribution::driver::NodeDriver;

    let listen_addr = args.listen.expect("--listen is required for TCP mode");
    let mut driver = NodeDriver::new(listen_addr, node_config).expect("failed to create node driver");

    eprintln!(
        "Node {} listening on {} (TCP)",
        hex(&driver.node_id().0[..4]),
        driver.listen_addr(),
    );

    // Join seed if provided
    if let Some(seed) = args.seed {
        let seed_addr: std::net::SocketAddr = seed.parse().expect("invalid seed address");
        eprintln!("Joining cluster via seed {seed_addr}");
        driver.join(&[seed_addr]);
    }

    // Spawn and register actors
    let actor_addrs = spawn_actors(args.actors, handle, driver.node_mut());

    // Wire distribution snapshot to dashboard
    let cached_snapshot: Arc<Mutex<Option<DistributionNodeSnapshot>>> =
        Arc::new(Mutex::new(Some(driver.snapshot())));
    let provider = SnapshotProvider {
        snapshot: Arc::clone(&cached_snapshot),
    };
    dash.set_distribution(Arc::new(provider));

    eprintln!("Dashboard at http://0.0.0.0:{}", args.dashboard_port);

    // Main loop
    while !stop.load(Ordering::Relaxed) {
        driver.recv();
        driver.tick();

        for addr in &actor_addrs {
            let _ = handle.runtime.send_to(*addr, Heartbeat);
        }

        *cached_snapshot.lock().unwrap() = Some(driver.snapshot());
        thread::sleep(Duration::from_millis(100));
    }
}

// ── iroh transport ───────────────────────────────────────────────────────

#[cfg(feature = "iroh")]
fn run_iroh(
    args: Args,
    node_config: DistributedNodeConfig,
    handle: &swactor::runtime::RuntimeHandle,
    dash: &dashboard::DashboardHandle,
    stop: &Arc<AtomicBool>,
) {
    use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
    use iroh::RelayMode;

    let iroh_config = IrohDriverConfig {
        secret_key: None,
        relay_mode: RelayMode::Default,
        node: node_config,
        peer_auth: None,
    };
    let mut driver = IrohDriver::new(iroh_config).expect("failed to create iroh driver");

    eprintln!(
        "Node {} started (iroh)",
        hex(&driver.node_id().0[..4]),
    );

    // Join seed if provided
    if let Some(seed_hex) = args.seed_node_id {
        let seed_bytes = hex_to_bytes(&seed_hex).expect("invalid seed node ID hex");
        let seed_key = iroh::PublicKey::from_bytes(&seed_bytes).expect("invalid seed public key");
        eprintln!("Joining cluster via seed {}", &seed_hex[..8]);
        driver.join(&[seed_key]);
    }

    // Spawn and register actors
    let actor_addrs = spawn_actors(args.actors, handle, driver.node_mut());

    // Wire distribution snapshot to dashboard
    let cached_snapshot: Arc<Mutex<Option<DistributionNodeSnapshot>>> =
        Arc::new(Mutex::new(Some(driver.snapshot())));
    let provider = SnapshotProvider {
        snapshot: Arc::clone(&cached_snapshot),
    };
    dash.set_distribution(Arc::new(provider));

    eprintln!("Dashboard at http://0.0.0.0:{}", args.dashboard_port);

    // Main loop
    while !stop.load(Ordering::Relaxed) {
        driver.recv();
        driver.tick();

        for addr in &actor_addrs {
            let _ = handle.runtime.send_to(*addr, Heartbeat);
        }

        *cached_snapshot.lock().unwrap() = Some(driver.snapshot());
        thread::sleep(Duration::from_millis(100));
    }

    driver.shutdown();
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn spawn_actors(
    count: usize,
    handle: &swactor::runtime::RuntimeHandle,
    node: &mut distribution::node::DistributedNode,
) -> Vec<swactor::actor::ActorAddress> {
    let mut addrs = Vec::new();
    for _ in 0..count {
        match handle.runtime.spawn(HeartbeatActor) {
            Ok(addr) => {
                node.register_actor(addr, 1);
                addrs.push(addr);
            }
            Err(e) => eprintln!("failed to spawn actor: {e}"),
        }
    }
    if !addrs.is_empty() {
        eprintln!("Registered {} actors", addrs.len());
    }
    addrs
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(feature = "iroh")]
fn hex_to_bytes(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}
