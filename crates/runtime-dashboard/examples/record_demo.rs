use std::thread;
use std::time::Duration;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;

use runtime_dashboard::{start_dashboard, DashboardConfig};

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
    let dash = start_dashboard(DashboardConfig {
        port: 9090,
        record: true, // Enable trace recording
        ..Default::default()
    });
    dash.install_tracing();

    let rt = Runtime::new(RuntimeConfig {
        num_threads: 4,
        max_actors: 512,
        channel_buffer_size: 1000,
        ..Default::default()
    });

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
    dash.set_runtime(handle.runtime.clone());

    eprintln!("Recording trace for 10 seconds...");
    eprintln!("Dashboard at http://localhost:9090");

    // Kick off ping-pong chains
    for i in 0..ping_addrs.len() {
        let target = ping_addrs[(i + 1) % ping_addrs.len()];
        let _ = handle.runtime.send_to(ping_addrs[i], Ping(target));
    }

    for round in 0..50 {
        for addr in &counter_addrs {
            let _ = handle.runtime.send_to(*addr, Tick);
        }

        // Spawn more actors mid-recording
        if round == 20 {
            for _ in 0..8 {
                let addr = handle.runtime.spawn(CounterActor).unwrap();
                counter_addrs.push(addr);
            }
            eprintln!("  Spawned 8 more actors");
        }

        // Re-kick pings
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
        Err(e) => eprintln!("Failed to save trace: {e}"),
    }
}
