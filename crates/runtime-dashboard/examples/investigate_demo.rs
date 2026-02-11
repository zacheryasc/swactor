use std::sync::Arc;
use std::thread;
use std::time::Duration;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;

use runtime_dashboard::collector::StatsCollector;
use runtime_dashboard::investigate::run_investigate;

// ── Demo actors (same as tui_demo) ─────────────────────────────────────

#[derive(Clone)]
struct Ping(ActorAddress);

struct PingActor {
    count: u32,
    limit: u32,
}

impl PingActor {
    fn new(limit: u32) -> Self {
        Self { count: 0, limit }
    }
}

impl ActorInterface for PingActor {
    type Incoming = Ping;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        self.count += 1;
        if self.count < self.limit {
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

fn main() -> std::io::Result<()> {
    let num_threads = 4;
    let collector = StatsCollector::new(num_threads);

    let mut rt = Runtime::new(RuntimeConfig {
        num_threads,
        max_actors: 1024,
        channel_buffer_size: 2000,
        ..Default::default()
    });
    rt.set_stats_hook(collector.clone());

    // Spawn some actors
    let mut ping_addrs = Vec::new();
    for _ in 0..16 {
        ping_addrs.push(rt.spawn(PingActor::new(500)).unwrap());
    }

    let mut counter_addrs = Vec::new();
    for _ in 0..40 {
        counter_addrs.push(rt.spawn(CounterActor::new()).unwrap());
    }

    let handle = rt.run().expect("failed to start runtime");
    let runtime = Arc::clone(&handle.runtime);

    // Wire up ping-pong
    for i in 0..ping_addrs.len() {
        let target = ping_addrs[(i + 1) % ping_addrs.len()];
        let _ = runtime.send_to(ping_addrs[i], Ping(target));
    }

    // Feeder thread
    let rt_feeder = Arc::clone(&runtime);
    let ping_clone = ping_addrs.clone();
    thread::spawn(move || {
        let mut round: u64 = 0;
        loop {
            for addr in &counter_addrs {
                let _ = rt_feeder.send_to(*addr, Tick);
            }
            if round % 40 == 0 && round > 0 {
                for i in 0..ping_clone.len() {
                    let target = ping_clone[(i + 1) % ping_clone.len()];
                    let _ = rt_feeder.send_to(ping_clone[i], Ping(target));
                }
            }
            round += 1;
            thread::sleep(Duration::from_millis(100));
        }
    });

    // Blocks on stdin — send commands, get JSON back
    run_investigate(runtime, collector)?;

    handle.shutdown();
    handle.join();
    Ok(())
}
