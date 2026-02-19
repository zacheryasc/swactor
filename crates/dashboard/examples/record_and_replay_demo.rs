use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;

use dashboard::collector::StatsCollector;
use dashboard::{serve_replay, start_dashboard, DashboardConfig, ReplayConfig};

// ── Demo actors ─────────────────────────────────────────────────────────

#[derive(Clone)]
struct Ping(ActorAddress);

struct PingActor {
    count: u32,
}

impl PingActor {
    fn new() -> Self {
        Self { count: 0 }
    }
}

impl ActorInterface for PingActor {
    type Incoming = Ping;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        self.count += 1;
        if self.count < 100 {
            let _ = ctx.send(msg.0, Ping(ctx.self_addr()));
        }
    }
}

#[derive(Clone)]
struct Tick;

struct CounterActor;

impl ActorInterface for CounterActor {
    type Incoming = Tick;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Tick) {}
}

// ── Main ────────────────────────────────────────────────────────────────

fn main() {
    // ── Phase 1: Record ─────────────────────────────────────────────────

    let dash = start_dashboard(DashboardConfig {
        port: 9090,
        record: true,
        ..Default::default()
    });
    dash.install_tracing();

    let num_threads = 4;
    let collector = StatsCollector::new(num_threads);

    let mut rt = Runtime::new(RuntimeConfig {
        num_threads,
        max_actors: 512,
        channel_buffer_size: 1000,
        ..Default::default()
    });
    rt.set_stats_hook(collector.clone());

    let mut ping_addrs = Vec::new();
    for _ in 0..12 {
        let addr = rt.spawn(PingActor::new()).unwrap();
        ping_addrs.push(addr);
    }

    let mut counter_addrs = Vec::new();
    for _ in 0..16 {
        let addr = rt.spawn(CounterActor).unwrap();
        counter_addrs.push(addr);
    }

    let handle = rt.run().expect("failed to start runtime");
    dash.set_runtime(handle.runtime.clone(), collector);

    eprintln!("Recording trace for ~10 seconds...");
    eprintln!("Live dashboard at http://localhost:9090");

    // Kick off ping-pong chains
    for i in 0..ping_addrs.len() {
        let target = ping_addrs[(i + 1) % ping_addrs.len()];
        let _ = handle.runtime.send_to(ping_addrs[i], Ping(target));
    }

    for round in 0..50 {
        for addr in &counter_addrs {
            let _ = handle.runtime.send_to(*addr, Tick);
        }

        if round == 20 {
            for _ in 0..8 {
                let addr = handle.runtime.spawn(CounterActor).unwrap();
                counter_addrs.push(addr);
            }
            eprintln!("  Spawned 8 more actors");
        }

        if round == 25 {
            for i in 0..ping_addrs.len() {
                let target = ping_addrs[(i + 1) % ping_addrs.len()];
                let _ = handle.runtime.send_to(ping_addrs[i], Ping(target));
            }
        }

        thread::sleep(Duration::from_millis(200));
    }

    handle.shutdown();
    dash.shutdown();
    handle.join();

    // Save trace
    let path = "runtime_trace.json";
    match dash.save_trace(path) {
        Ok(()) => eprintln!("Trace saved to {path}"),
        Err(e) => {
            eprintln!("Failed to save trace: {e}");
            std::process::exit(1);
        }
    }

    // ── Phase 2: Replay ─────────────────────────────────────────────────

    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = Arc::clone(&stop);
        ctrlc::set_handler(move || {
            stop.store(true, Ordering::Relaxed);
        })
        .expect("failed to set Ctrl+C handler");
    }

    eprintln!("\nStarting replay at 2x speed — press Ctrl+C to stop");

    // Spawn replay server in a background thread so we can check Ctrl+C
    let replay_path = path.to_string();
    thread::spawn(move || {
        if let Err(e) = serve_replay(&replay_path, ReplayConfig { port: 9091, speed: 2.0 }) {
            eprintln!("Replay error: {e}");
        }
    });

    while !stop.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(200));
    }

    eprintln!("Done.");
}
