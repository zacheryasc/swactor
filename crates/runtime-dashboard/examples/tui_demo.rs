use std::sync::Arc;
use std::thread;
use std::time::Duration;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;

use runtime_dashboard::collector::StatsCollector;
use runtime_dashboard::tui::{TuiConfig, start_tui};

// ── Demo actors ─────────────────────────────────────────────────────────

/// Ping-pong actor: bounces messages back and forth creating cross-worker traffic.
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

/// Simple counter that tallies tick messages.
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

/// Fan-out actor: on each message, forwards to all targets — amplifies traffic.
#[derive(Clone)]
struct Fanout(Vec<ActorAddress>);

struct FanoutActor {
    targets: Vec<ActorAddress>,
}

impl ActorInterface for FanoutActor {
    type Incoming = Fanout;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: Fanout) {
        self.targets = msg.0;
        for &t in &self.targets {
            let _ = ctx.send(t, Tick);
        }
    }
}

/// Chain actor: receives a hop count, decrements, and forwards to the next in chain.
#[derive(Clone)]
struct Hop {
    remaining: u32,
    chain: Vec<ActorAddress>,
    index: usize,
}

struct ChainActor;

impl ActorInterface for ChainActor {
    type Incoming = Hop;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: Hop) {
        if msg.remaining > 0 {
            let next_idx = (msg.index + 1) % msg.chain.len();
            let _ = ctx.send(
                msg.chain[next_idx],
                Hop {
                    remaining: msg.remaining - 1,
                    chain: msg.chain,
                    index: next_idx,
                },
            );
        }
    }
}

// ── Main ────────────────────────────────────────────────────────────────

fn main() -> std::io::Result<()> {
    let num_threads = 8;
    let collector = StatsCollector::new(num_threads);

    let mut rt = Runtime::new(RuntimeConfig {
        num_threads,
        max_actors: 4096,
        channel_buffer_size: 4000,
        ..Default::default()
    });
    rt.set_stats_hook(collector.clone());

    // ── Ping-pong pairs (cross-worker bouncing) ────────────────────────
    let mut ping_addrs = Vec::new();
    for _ in 0..32 {
        ping_addrs.push(rt.spawn(PingActor::new(500)).unwrap());
    }

    // ── Counter actors (sustained tick traffic) ────────────────────────
    let mut counter_addrs: Vec<ActorAddress> = Vec::new();
    for _ in 0..60 {
        counter_addrs.push(rt.spawn(CounterActor::new()).unwrap());
    }

    // ── Fan-out hubs (traffic amplifiers) ──────────────────────────────
    let mut fanout_addrs = Vec::new();
    for _ in 0..4 {
        fanout_addrs.push(
            rt.spawn(FanoutActor {
                targets: Vec::new(),
            })
            .unwrap(),
        );
    }

    // ── Chain rings (message relay loops) ──────────────────────────────
    let mut chain_addrs = Vec::new();
    for _ in 0..24 {
        chain_addrs.push(rt.spawn(ChainActor).unwrap());
    }

    let handle = rt.run().expect("failed to start runtime");
    let runtime = Arc::clone(&handle.runtime);

    // Wire up ping-pong chains
    for i in 0..ping_addrs.len() {
        let target = ping_addrs[(i + 1) % ping_addrs.len()];
        let _ = runtime.send_to(ping_addrs[i], Ping(target));
    }

    // Wire up fan-out hubs: each hub fans to a slice of counter actors
    let chunk_size = counter_addrs.len() / fanout_addrs.len().max(1);
    for (i, &hub) in fanout_addrs.iter().enumerate() {
        let start = i * chunk_size;
        let end = (start + chunk_size).min(counter_addrs.len());
        let targets: Vec<_> = counter_addrs[start..end].to_vec();
        let _ = runtime.send_to(hub, Fanout(targets));
    }

    // Kick off chain rings: 3 rings of 8 actors each
    for ring_start in (0..chain_addrs.len()).step_by(8) {
        let ring: Vec<_> = chain_addrs[ring_start..ring_start + 8].to_vec();
        let _ = runtime.send_to(
            ring[0],
            Hop {
                remaining: 200,
                chain: ring,
                index: 0,
            },
        );
    }

    // ── Feeder threads ─────────────────────────────────────────────────

    // Thread 1: tick all counters + periodically spawn more
    let rt1 = Arc::clone(&runtime);
    let fanout_addrs_clone = fanout_addrs.clone();
    thread::spawn(move || {
        let mut counter_addrs = counter_addrs;
        let mut round: u64 = 0;
        loop {
            // Tick every counter
            for addr in &counter_addrs {
                let _ = rt1.send_to(*addr, Tick);
            }

            // Periodically spawn more counters (grow from 60 up to 400)
            if round % 50 == 25 && counter_addrs.len() < 400 {
                let mut new_addrs = Vec::new();
                for _ in 0..12 {
                    match rt1.spawn(CounterActor::new()) {
                        Ok(addr) => new_addrs.push(addr),
                        Err(_) => break,
                    }
                }
                // Re-wire fan-out hubs with expanded target list
                let chunk = new_addrs.len() / fanout_addrs_clone.len().max(1);
                for (i, &hub) in fanout_addrs_clone.iter().enumerate() {
                    let start = i * chunk;
                    let end = (start + chunk).min(new_addrs.len());
                    if start < end {
                        let targets: Vec<_> = new_addrs[start..end].to_vec();
                        let _ = rt1.send_to(hub, Fanout(targets));
                    }
                }
                counter_addrs.extend(new_addrs);
            }

            round += 1;
            thread::sleep(Duration::from_millis(100));
        }
    });

    // Thread 2: re-kick ping chains + chain rings periodically
    let rt2 = Arc::clone(&runtime);
    let ping_clone = ping_addrs.clone();
    let chain_clone = chain_addrs.clone();
    thread::spawn(move || {
        let mut round: u64 = 0;
        loop {
            // Re-kick ping-pong chains
            if round % 40 == 0 {
                for i in 0..ping_clone.len() {
                    let target = ping_clone[(i + 1) % ping_clone.len()];
                    let _ = rt2.send_to(ping_clone[i], Ping(target));
                }
            }

            // Re-kick chain rings
            if round % 30 == 0 {
                for ring_start in (0..chain_clone.len()).step_by(8) {
                    let ring: Vec<_> = chain_clone[ring_start..ring_start + 8].to_vec();
                    let _ = rt2.send_to(
                        ring[0],
                        Hop {
                            remaining: 200,
                            chain: ring,
                            index: 0,
                        },
                    );
                }
            }

            // Periodically spawn short-lived ping bursts
            if round % 60 == 30 {
                let mut burst = Vec::new();
                for _ in 0..8 {
                    match rt2.spawn(PingActor::new(50)) {
                        Ok(addr) => burst.push(addr),
                        Err(_) => break,
                    }
                }
                for i in 0..burst.len() {
                    let target = burst[(i + 1) % burst.len()];
                    let _ = rt2.send_to(burst[i], Ping(target));
                }
            }

            round += 1;
            thread::sleep(Duration::from_millis(150));
        }
    });

    // Thread 3: fan-out re-trigger (keeps hubs active)
    let rt3 = Arc::clone(&runtime);
    let fanout_clone = fanout_addrs.clone();
    thread::spawn(move || loop {
        for &hub in &fanout_clone {
            // Re-send so hub forwards again to its targets
            let _ = rt3.send_to(hub, Fanout(Vec::new()));
        }
        thread::sleep(Duration::from_millis(80));
    });

    // This blocks until the user presses 'q'
    start_tui(runtime, collector, TuiConfig::default())?;

    handle.shutdown();
    handle.join();
    Ok(())
}
