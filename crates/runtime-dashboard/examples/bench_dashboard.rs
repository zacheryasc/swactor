use std::thread;
use std::time::{Duration, Instant};

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::config::{BackoffPolicy, RuntimeConfig};
use swactor::runtime::Runtime;

use runtime_dashboard::collector::StatsCollector;
use runtime_dashboard::{start_dashboard, DashboardConfig};

// ---------------------------------------------------------------------------
// Actors
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Work;

struct SinkActor {
    count: u64,
}

impl SinkActor {
    fn new() -> Self {
        Self { count: 0 }
    }
}

impl ActorInterface for SinkActor {
    type Incoming = Work;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: Work) {
        self.count += 1;
    }
}

#[derive(Clone)]
struct RingMsg;

struct RingActor {
    next: ActorAddress,
}

impl ActorInterface for RingActor {
    type Incoming = RingMsg;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, _msg: RingMsg) {
        let _ = ctx.send(self.next, RingMsg);
    }
}

#[derive(Clone)]
struct SpawnCmd;

struct SpawnerActor {
    spawned: u64,
}

impl SpawnerActor {
    fn new() -> Self {
        Self { spawned: 0 }
    }
}

impl ActorInterface for SpawnerActor {
    type Incoming = SpawnCmd;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, _msg: SpawnCmd) {
        for _ in 0..20 {
            let _ = ctx.spawn(SinkActor::new());
            self.spawned += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bench_config(threads: usize, max_actors: usize, max_messages: usize) -> RuntimeConfig {
    RuntimeConfig {
        num_threads: threads,
        max_actors,
        channel_buffer_size: max_messages,
        backoff_policy: BackoffPolicy {
            spin_threshold: 32,
            yield_threshold: 64,
            sleep_increment_us: 10,
            sleep_max_us: 100,
        },
    }
}

fn run_for(duration: Duration, mut tick: impl FnMut()) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        tick();
    }
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

fn scenario_single_actor(dash: &runtime_dashboard::DashboardHandle) {
    eprintln!("  [1/4] Single-actor bombardment (5s)");
    let collector = StatsCollector::new(4);
    let mut rt = Runtime::new(bench_config(4, 64, 100_000));
    rt.set_stats_hook(collector.clone());
    let addr = rt.spawn(SinkActor::new()).unwrap();
    let handle = rt.run().unwrap();
    dash.set_runtime(handle.runtime.clone(), collector);

    run_for(Duration::from_secs(5), || {
        for _ in 0..100 {
            let _ = handle.runtime.send_to(addr, Work);
        }
        thread::sleep(Duration::from_millis(10));
    });

    handle.shutdown();
    handle.join();
}

fn scenario_multi_actor(dash: &runtime_dashboard::DashboardHandle) {
    eprintln!("  [2/4] Multi-actor fan-out (5s)");
    let collector = StatsCollector::new(4);
    let mut rt = Runtime::new(bench_config(4, 128, 10_000));
    rt.set_stats_hook(collector.clone());
    let addrs: Vec<_> = (0..50)
        .map(|_| rt.spawn(SinkActor::new()).unwrap())
        .collect();
    let handle = rt.run().unwrap();
    dash.set_runtime(handle.runtime.clone(), collector);

    run_for(Duration::from_secs(5), || {
        for &addr in &addrs {
            for _ in 0..10 {
                let _ = handle.runtime.send_to(addr, Work);
            }
        }
        thread::sleep(Duration::from_millis(20));
    });

    handle.shutdown();
    handle.join();
}

fn scenario_ring(dash: &runtime_dashboard::DashboardHandle) {
    eprintln!("  [3/4] Ring topology (5s)");
    let ring_size = 100;
    let collector = StatsCollector::new(4);
    let mut rt = Runtime::new(bench_config(4, ring_size + 64, 1_024));
    rt.set_stats_hook(collector.clone());

    // Build ring backwards: last spawned actor is the entry point
    let mut addrs = Vec::with_capacity(ring_size);
    // First actor has no valid next yet — will be the tail of the chain
    let first = rt.spawn(RingActor { next: ActorAddress::default() }).unwrap();
    addrs.push(first);
    let mut prev = first;
    for _ in 1..ring_size {
        let addr = rt.spawn(RingActor { next: prev }).unwrap();
        addrs.push(addr);
        prev = addr;
    }
    // The first actor's "next" should be the last actor to close the ring,
    // but we can't mutate it. Instead, we inject at the last actor and
    // the message flows: last -> second-to-last -> ... -> first -> (dead end).
    // For dashboard visualization, a chain is fine — it creates sustained cross-worker traffic.
    let entry = *addrs.last().unwrap();

    let handle = rt.run().unwrap();
    dash.set_runtime(handle.runtime.clone(), collector);

    run_for(Duration::from_secs(5), || {
        let _ = handle.runtime.send_to(entry, RingMsg);
        thread::sleep(Duration::from_millis(50));
    });

    handle.shutdown();
    handle.join();
}

fn scenario_spawn_storm(dash: &runtime_dashboard::DashboardHandle) {
    eprintln!("  [4/4] Spawn storm (5s)");
    let collector = StatsCollector::new(4);
    let mut rt = Runtime::new(bench_config(4, 50_000, 1_024));
    rt.set_stats_hook(collector.clone());
    let spawner = rt.spawn(SpawnerActor::new()).unwrap();
    let handle = rt.run().unwrap();
    dash.set_runtime(handle.runtime.clone(), collector);

    run_for(Duration::from_secs(5), || {
        let _ = handle.runtime.send_to(spawner, SpawnCmd);
        thread::sleep(Duration::from_millis(200));
    });

    handle.shutdown();
    handle.join();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let dash = start_dashboard(DashboardConfig {
        port: 9090,
        ..Default::default()
    });
    dash.install_tracing();

    eprintln!("Dashboard at http://localhost:9090");
    eprintln!("Running 4 benchmark scenarios (~20s total)...\n");

    scenario_single_actor(&dash);
    scenario_multi_actor(&dash);
    scenario_ring(&dash);
    scenario_spawn_storm(&dash);

    eprintln!("\nAll scenarios complete. Shutting down.");
    dash.shutdown();
}
