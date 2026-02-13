use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use clap::Parser;

use swactor::actor::{ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;

use distribution::driver::NodeDriver;
use distribution::node::DistributedNodeConfig;
use distribution::snapshot::DistributionNodeSnapshot;
use distribution::swim::probe::SwimConfig;

use runtime_dashboard::collector::StatsCollector;
use runtime_dashboard::distribution_collector::DistributionStatsProvider;
use runtime_dashboard::{start_dashboard, DashboardConfig};

// ── CLI ──────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "swactor-node", about = "Swactor distributed node")]
struct Args {
    /// Address to listen on for SWIM protocol (e.g. 10.0.1.10:7000)
    #[arg(long)]
    listen: SocketAddr,

    /// Seed node address to join (omit for the seed node itself)
    #[arg(long)]
    seed: Option<SocketAddr>,

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

    // Create distribution node driver
    let swim_config = SwimConfig {
        probe_interval: 5,
        probe_timeout: 3,
        indirect_probes: 2,
        suspicion_timeout: 20,
        dead_reprobe_interval: 50,
    };
    let node_config = DistributedNodeConfig {
        listen_addr: args.listen,
        swim: swim_config,
        cache_capacity: 1000,
        republish_interval: 500,
        ..Default::default()
    };
    let mut driver = NodeDriver::new(node_config).expect("failed to create node driver");

    eprintln!(
        "Node {} listening on {}",
        hex(&driver.node_id().0[..4]),
        driver.listen_addr(),
    );

    // Join seed if provided
    if let Some(seed) = args.seed {
        eprintln!("Joining cluster via seed {seed}");
        driver.join(&[seed]);
    }

    // Spawn and register actors
    let mut actor_addrs = Vec::new();
    for _ in 0..args.actors {
        match handle.runtime.spawn(HeartbeatActor) {
            Ok(addr) => {
                driver.node_mut().register_actor(addr, 1);
                actor_addrs.push(addr);
            }
            Err(e) => eprintln!("failed to spawn actor: {e}"),
        }
    }

    if !actor_addrs.is_empty() {
        eprintln!("Registered {} actors", actor_addrs.len());
    }

    // Wire distribution snapshot to dashboard
    let cached_snapshot: Arc<Mutex<Option<DistributionNodeSnapshot>>> =
        Arc::new(Mutex::new(Some(driver.snapshot())));
    let provider = SnapshotProvider {
        snapshot: Arc::clone(&cached_snapshot),
    };
    dash.set_distribution(Arc::new(provider));

    eprintln!(
        "Dashboard at http://0.0.0.0:{}",
        args.dashboard_port
    );

    // Main loop
    while !stop.load(Ordering::Relaxed) {
        driver.recv();
        driver.tick();

        // Send heartbeats to keep actors alive
        for addr in &actor_addrs {
            let _ = handle.runtime.send_to(*addr, Heartbeat);
        }

        // Update dashboard snapshot
        *cached_snapshot.lock().unwrap() = Some(driver.snapshot());

        thread::sleep(Duration::from_millis(100));
    }

    eprintln!("\nShutting down...");
    handle.shutdown();
    dash.shutdown();
    handle.join();
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
