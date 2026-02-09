use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
        // Forward to the target — creates cross-worker traffic
        if self.count < 200 {
            let _ = ctx.send(msg.0, Ping(ctx.self_addr()));
        }
    }
}

#[derive(Clone)]
struct Tick;

struct CounterActor {
    ticks: u64,
}

impl CounterActor {
    fn new() -> Self {
        Self { ticks: 0 }
    }
}

impl ActorInterface for CounterActor {
    type Incoming = Tick;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Tick) {
        self.ticks += 1;
    }
}

// ── Main ────────────────────────────────────────────────────────────────

fn main() {
    let stop = Arc::new(AtomicBool::new(false));

    // Handle Ctrl+C gracefully
    {
        let stop = Arc::clone(&stop);
        let _ = std::panic::catch_unwind(|| {
            // Try to register a signal handler; fall back to running until killed
            unsafe {
                libc_signal(2, move || stop.store(true, Ordering::Relaxed));
            }
        });
    }

    let dash = start_dashboard(DashboardConfig {
        port: 9090,
        ..Default::default()
    });
    dash.install_tracing();

    let rt = Runtime::new(RuntimeConfig {
        num_threads: 4,
        max_actors: 1024,
        actor_max_messages: 2000,
        ..Default::default()
    });

    // Spawn ping actors for cross-worker traffic
    let mut ping_addrs = Vec::new();
    for _ in 0..16 {
        let addr = rt.spawn(PingActor::new()).unwrap();
        ping_addrs.push(addr);
    }

    // Spawn counter actors for sustained traffic
    let mut counter_addrs = Vec::new();
    for _ in 0..20 {
        let addr = rt.spawn(CounterActor::new()).unwrap();
        counter_addrs.push(addr);
    }

    let handle = rt.run().expect("failed to start runtime");
    dash.set_runtime(handle.runtime.clone());

    eprintln!("Dashboard at http://localhost:9090 — press Ctrl+C to stop");

    // Kick off ping-pong chains
    for i in 0..ping_addrs.len() {
        let target = ping_addrs[(i + 1) % ping_addrs.len()];
        let _ = handle.runtime.send_to(ping_addrs[i], Ping(target));
    }

    let mut round: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        // Send ticks to all counter actors
        for addr in &counter_addrs {
            let _ = handle.runtime.send_to(*addr, Tick);
        }

        // Periodically spawn more actors
        if round % 150 == 75 && counter_addrs.len() < 200 {
            for _ in 0..8 {
                match handle.runtime.spawn(CounterActor::new()) {
                    Ok(addr) => counter_addrs.push(addr),
                    Err(_) => break,
                }
            }
        }

        // Periodically re-kick ping chains
        if round % 80 == 0 && round > 0 {
            for i in 0..ping_addrs.len() {
                let target = ping_addrs[(i + 1) % ping_addrs.len()];
                let _ = handle.runtime.send_to(ping_addrs[i], Ping(target));
            }
        }

        round += 1;
        thread::sleep(Duration::from_millis(200));
    }

    eprintln!("\nShutting down...");
    handle.shutdown();
    dash.shutdown();
    handle.join();
}

// Minimal signal handling without external deps
unsafe fn libc_signal(_sig: i32, _handler: impl FnOnce()) {
    // This is a no-op fallback; the loop checks the AtomicBool
    // In practice, Ctrl+C will terminate the process
}
