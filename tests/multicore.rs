//! Multicore runtime contract tests.
//!
//! These tests exercise the multi-worker ownership and routing model defined in
//! `docs/specs/drafts/MULTICORE_SPEC.md`. They observe behavior through public
//! APIs only — never inspecting source layout.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

use common::*;
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::admin::OperationResult;
use swactor::runtime::RuntimeConfig;

/// A minimal message delivered to probe actors.
#[derive(Clone, Debug)]
pub struct Probe;

/// A sequenced message used to verify delivery order.
#[derive(Clone, Debug)]
pub struct Seq(pub usize);

/// Report sent by a spawning parent: its own worker id and the child address.
#[derive(Clone)]
struct ParentReport {
    parent_worker: usize,
    child_addr: ActorAddress,
}

/// Records every `Probe` it handles into its own shared counter.
struct CountingProbe(Arc<AtomicUsize>);

impl ActorInterface for CountingProbe {
    type Incoming = Probe;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: Probe) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct StopCountingProbe {
    stopped: Arc<AtomicUsize>,
}

impl ActorInterface for StopCountingProbe {
    type Incoming = Probe;
    type Response = ();

    fn on_stop(&mut self, _ctx: &Ctx) {
        self.stopped.fetch_add(1, Ordering::SeqCst);
    }

    fn handle(&mut self, _ctx: &Ctx, _msg: Probe) {}
}

struct SpawnTwoAndSendSecond {
    second_count: Arc<AtomicUsize>,
}

impl ActorInterface for SpawnTwoAndSendSecond {
    type Incoming = Probe;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, _msg: Probe) {
        let _first = ctx
            .spawn(CountingProbe(Arc::new(AtomicUsize::new(0))))
            .expect("spawn first child");
        let second = ctx
            .spawn(CountingProbe(self.second_count.clone()))
            .expect("spawn second child");
        ctx.send(second, Probe).expect("send second child");
    }
}

/// Records every `Seq` value it handles, preserving arrival order.
struct Recorder {
    out: Arc<Mutex<Vec<usize>>>,
}

impl ActorInterface for Recorder {
    type Incoming = Seq;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, msg: Seq) {
        self.out.lock().push(msg.0);
    }
}

/// On a `Probe`, sends a burst of `Seq` values to a target address.
struct BurstSender {
    target: ActorAddress,
    values: Vec<usize>,
}

impl ActorInterface for BurstSender {
    type Incoming = Probe;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, _msg: Probe) {
        for &v in &self.values {
            let _ = ctx.send(self.target, Seq(v));
        }
    }
}

/// Records each `Seq` and, while below `limit`, sends itself the next value
/// (a same-worker self-send).
struct ChainSelf {
    out: Arc<Mutex<Vec<usize>>>,
    limit: usize,
}

impl ActorInterface for ChainSelf {
    type Incoming = Seq;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: Seq) {
        self.out.lock().push(msg.0);
        if msg.0 < self.limit {
            let _ = ctx.send(ctx.self_addr(), Seq(msg.0 + 1));
        }
    }
}

/// On a `Probe`, spawns a `CountingProbe` child and reports its own worker id
/// plus the child address.
struct SpawningParent {
    reply_to: ActorAddress,
}

impl ActorInterface for SpawningParent {
    type Incoming = Probe;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, _msg: Probe) {
        let child = ctx
            .spawn(CountingProbe(Arc::new(AtomicUsize::new(0))))
            .expect("spawn child");
        let _ = ctx.send(
            self.reply_to,
            ParentReport {
                parent_worker: ctx.system_info().worker_id,
                child_addr: child,
            },
        );
    }
}

/// Look up the worker id for `addr` in a stats snapshot.
fn worker_of(stats: &swactor::stats::RuntimeStats, addr: ActorAddress) -> usize {
    stats
        .actors
        .iter()
        .find(|(a, _)| *a == addr)
        .map(|(_, w)| *w)
        .expect("address placed")
}

fn config_with(workers: usize) -> RuntimeConfig {
    let mut c = RuntimeConfig::default();
    c.worker_count = workers;
    c
}

// ─── Phase 1: single-thread host advances every worker once ─────────────────

#[test]
fn single_thread_host_advances_every_worker_once() {
    // Three workers; round-robin runtime spawns place one actor on each.
    let (rt, mut host) = std_host(config_with(3));

    let c0 = Arc::new(AtomicUsize::new(0));
    let c1 = Arc::new(AtomicUsize::new(0));
    let c2 = Arc::new(AtomicUsize::new(0));
    let a = rt.spawn(CountingProbe(c0.clone())).expect("spawn a");
    let b = rt.spawn(CountingProbe(c1.clone())).expect("spawn b");
    let c = rt.spawn(CountingProbe(c2.clone())).expect("spawn c");

    rt.send_to(a, Probe).expect("send a");
    rt.send_to(b, Probe).expect("send b");
    rt.send_to(c, Probe).expect("send c");

    // A single pass must tick every worker — not stop after the first
    // productive one. If any worker were skipped, its actor would not have
    // processed its probe.
    let did_work = host.try_tick();
    assert!(did_work, "try_tick must report work when workers produced");
    assert_eq!(
        c0.load(Ordering::SeqCst),
        1,
        "worker 0 actor processed its probe"
    );
    assert_eq!(
        c1.load(Ordering::SeqCst),
        1,
        "worker 1 actor processed its probe"
    );
    assert_eq!(
        c2.load(Ordering::SeqCst),
        1,
        "worker 2 actor processed its probe"
    );
}

#[test]
fn single_worker_runtime_remains_equivalent() {
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().expect("inbox");
    let addr = rt.spawn(PingPongActor).expect("spawn ping-pong");

    rt.send_to(
        addr,
        Ping {
            reply_to: *inbox.addr(),
        },
    )
    .expect("send ping");
    let pong = tick_until_recv(&mut host, &inbox, 16);
    assert_eq!(pong, Some(Pong), "single-worker delivery still works");
}

// ─── Phase 2: worker-aware routing and bounded passes ───────────────────────

#[test]
fn external_spawns_distribute_round_robin() {
    let (rt, _host) = std_host(config_with(4));

    let mut addrs = Vec::new();
    for _ in 0..8 {
        addrs.push(
            rt.spawn(CountingProbe(Arc::new(AtomicUsize::new(0))))
                .expect("spawn"),
        );
    }

    // Placement is recorded in the address map at spawn time.
    let stats = rt.stats();
    let workers: Vec<usize> = addrs.iter().map(|a| worker_of(&stats, *a)).collect();
    assert_eq!(
        workers,
        vec![0, 1, 2, 3, 0, 1, 2, 3],
        "runtime-handle spawns are round-robin across workers"
    );
}

#[test]
fn ctx_spawn_places_child_on_parents_worker() {
    let (rt, mut host) = std_host(config_with(2));
    let report = rt.new_inbox::<ParentReport>().expect("inbox");

    // First runtime spawn → worker 0; the parent reports its own worker id.
    let parent = rt
        .spawn(SpawningParent {
            reply_to: *report.addr(),
        })
        .expect("spawn parent");
    let stats = rt.stats();
    assert_eq!(worker_of(&stats, parent), 0, "parent placed on worker 0");

    rt.send_to(parent, Probe).expect("probe parent");
    let msg = tick_until_recv(&mut host, &report, 16).expect("parent reported");

    assert_eq!(msg.parent_worker, 0, "parent handler observes worker 0");
    let stats = rt.stats();
    assert_eq!(
        worker_of(&stats, msg.child_addr),
        0,
        "ctx.spawn pins the child to the parent's worker"
    );
}

#[test]
fn cross_worker_delivery_and_fifo_hold() {
    // worker 0: Recorder. worker 1: BurstSender targeting the Recorder.
    let (rt, mut host) = std_host(config_with(2));
    let recorded = Arc::new(Mutex::new(Vec::new()));

    let recorder = rt
        .spawn(Recorder {
            out: recorded.clone(),
        })
        .expect("spawn recorder");
    let sender = rt
        .spawn(BurstSender {
            target: recorder,
            values: vec![1, 2, 3],
        })
        .expect("spawn sender");

    let stats = rt.stats();
    assert_eq!(worker_of(&stats, recorder), 0, "recorder on worker 0");
    assert_eq!(worker_of(&stats, sender), 1, "sender on worker 1");

    rt.send_to(sender, Probe).expect("trigger sender");
    // Drive enough passes for the cross-worker transfer + handler round trips.
    tick_n(&mut host, 8);

    let got = recorded.lock().clone();
    assert_eq!(
        got,
        vec![1, 2, 3],
        "cross-worker delivery preserves per-(sender,target) FIFO"
    );
}

#[test]
fn same_worker_sends_are_not_recursive_in_the_current_pass() {
    let (rt, mut host) = std_host(config_with(1));
    let out = Arc::new(Mutex::new(Vec::new()));

    let addr = rt
        .spawn(ChainSelf {
            out: out.clone(),
            limit: 5,
        })
        .expect("spawn chain");
    rt.send_to(addr, Seq(1)).expect("seed");

    // One pass: the seed is handled and the self-send is staged for next pass.
    host.try_tick();
    assert_eq!(
        out.lock().len(),
        1,
        "same-worker self-send must not be handled recursively this pass"
    );

    // Subsequent passes drain the self-chain one value per pass.
    tick_n(&mut host, 8);
    assert_eq!(
        *out.lock(),
        vec![1, 2, 3, 4, 5],
        "chain completes across passes"
    );
}

#[test]
fn transfer_backlog_is_consumed_across_multiple_passes() {
    // Ingress budget is the limiter; the actor message budget stays independent.
    let mut config = config_with(1);
    config.worker_ingress_budget = 4;
    let (rt, mut host) = std_host(config);
    let recorded = Arc::new(Mutex::new(Vec::new()));

    let recorder = rt
        .spawn(Recorder {
            out: recorded.clone(),
        })
        .expect("spawn recorder");
    host.try_tick(); // install recorder

    for v in 1..=10u32 {
        rt.send_to(recorder, Seq(v as usize)).expect("send");
    }

    host.try_tick();
    assert_eq!(
        recorded.lock().len(),
        4,
        "a single transfer drain is bounded by worker_ingress_budget"
    );

    // The remaining backlog drains over further passes.
    tick_n(&mut host, 8);
    assert_eq!(
        recorded.lock().len(),
        10,
        "the full backlog is eventually consumed across passes"
    );
}

#[test]
fn messages_to_budget_delayed_runtime_spawn_are_retained() {
    let mut config = config_with(1);
    config.worker_ingress_budget = 1;
    let (rt, mut host) = std_host(config);
    let first_count = Arc::new(AtomicUsize::new(0));
    let second_count = Arc::new(AtomicUsize::new(0));

    let _first = rt
        .spawn(CountingProbe(first_count))
        .expect("spawn first actor");
    let second = rt
        .spawn(CountingProbe(second_count.clone()))
        .expect("spawn second actor");
    rt.send_to(second, Probe).expect("send to second actor");

    tick_n(&mut host, 8);

    assert_eq!(
        second_count.load(Ordering::SeqCst),
        1,
        "message to mapped but budget-delayed spawn is delivered after install"
    );
}

#[test]
fn stop_signal_to_budget_delayed_runtime_spawn_is_retained() {
    let mut config = config_with(1);
    config.worker_ingress_budget = 1;
    let (rt, mut host) = std_host(config);
    let stopped = Arc::new(AtomicUsize::new(0));

    let _first = rt
        .spawn(CountingProbe(Arc::new(AtomicUsize::new(0))))
        .expect("spawn first actor");
    let second = rt
        .spawn(StopCountingProbe {
            stopped: stopped.clone(),
        })
        .expect("spawn second actor");
    rt.stop_actor(second).expect("request stop");

    tick_n(&mut host, 8);

    assert_eq!(
        stopped.load(Ordering::SeqCst),
        1,
        "stop signal waits for the delayed spawn instead of being dropped"
    );
    assert!(
        rt.send_to(second, Probe).is_err(),
        "stopped actor is removed from routing"
    );
}

#[test]
fn targeted_admin_to_budget_delayed_runtime_spawn_is_retained() {
    let mut config = config_with(1);
    config.worker_ingress_budget = 1;
    let (rt, mut host) = std_host(config);
    let stopped = Arc::new(AtomicUsize::new(0));

    let _first = rt
        .spawn(CountingProbe(Arc::new(AtomicUsize::new(0))))
        .expect("spawn first actor");
    let second = rt
        .spawn(StopCountingProbe {
            stopped: stopped.clone(),
        })
        .expect("spawn second actor");
    let admin = rt.admin().stop_actor(second).expect("admin stop");

    let result = admin.recv_ticking(&mut host, 8);

    assert_eq!(result, Ok(OperationResult { applied: true }));
    assert_eq!(
        stopped.load(Ordering::SeqCst),
        1,
        "admin stop waits for the delayed spawn instead of returning ActorNotFound"
    );
}

#[test]
fn local_messages_to_budget_delayed_handler_spawn_are_retained() {
    let mut config = config_with(1);
    config.worker_ingress_budget = 1;
    let (rt, mut host) = std_host(config);
    let second_count = Arc::new(AtomicUsize::new(0));

    let parent = rt
        .spawn(SpawnTwoAndSendSecond {
            second_count: second_count.clone(),
        })
        .expect("spawn parent");
    rt.send_to(parent, Probe).expect("trigger parent");

    tick_n(&mut host, 8);

    assert_eq!(
        second_count.load(Ordering::SeqCst),
        1,
        "staged local message waits for handler-spawned child install"
    );
}

#[test]
fn spawn_and_admin_drains_are_not_blocked_by_transfer_backlog() {
    let mut config = config_with(1);
    config.worker_ingress_budget = 4;
    let (rt, mut host) = std_host(config);
    let recorded = Arc::new(Mutex::new(Vec::new()));

    let recorder = rt
        .spawn(Recorder {
            out: recorded.clone(),
        })
        .expect("spawn recorder");

    host.try_tick(); // install recorder

    // Build a transfer backlog that exceeds the ingress budget.
    for v in 1..=10u32 {
        rt.send_to(recorder, Seq(v as usize)).expect("send");
    }
    // Queue a spawn and an admin command alongside the backlog.
    let probe_addr = rt
        .spawn(CountingProbe(Arc::new(AtomicUsize::new(0))))
        .expect("spawn probe");
    let admin_handle = rt.admin().list_actors().expect("list_actors");

    // A single pass: the spawn drain installs the new actor, the admin drain
    // answers list_actors, and the transfer drain consumes only its own budget.
    host.try_tick();

    let stats = rt.stats();
    assert!(
        stats.actors.iter().any(|(a, _)| *a == probe_addr),
        "spawn drain is not blocked by the transfer backlog"
    );
    // The admin reply was produced during that same pass — no extra ticking.
    let resp = admin_handle
        .try_recv()
        .expect("admin reply delivered in the first pass")
        .expect("list_actors ok");
    assert!(
        resp.actors.iter().any(|a| a.address == probe_addr),
        "admin drain observes the newly spawned actor"
    );
    assert_eq!(
        recorded.lock().len(),
        4,
        "transfer drain still bounded while spawn/admin progress"
    );
}

#[test]
fn process_local_inbox_routing_precedes_remote_transport() {
    // A non-actor address resolves through the process-local inbox registry,
    // which route_nonlocal consults before any remote transport seam.
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Probe>().expect("inbox");

    rt.send_to(*inbox.addr(), Probe)
        .expect("send to inbox address");
    // Inbox delivery is synchronous through the registry; one tick suffices to
    // also prove no actor path captured it.
    host.try_tick();
    assert!(
        inbox.try_recv().is_some(),
        "non-actor address delivered to the local inbox"
    );
}

// ─── Phase 3: multicore admin, stats, lifecycle, extensions ─────────────────

use std::any::Any;

use swactor::actor::ActorExited;
use swactor::extension::{RuntimeExtension, WorkerExtension};
use swactor::runtime::{RuntimeParts, SingleThreadRuntime};

/// Reports `system_info()` observed from inside a handler.
#[derive(Clone)]
struct SystemReport {
    num_workers: usize,
    total_actors: usize,
    worker_id: usize,
}

struct SystemReporter {
    reply_to: ActorAddress,
}

impl ActorInterface for SystemReporter {
    type Incoming = Probe;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, _msg: Probe) {
        let si = ctx.system_info();
        let _ = ctx.send(
            self.reply_to,
            SystemReport {
                num_workers: si.num_workers,
                total_actors: si.total_actors,
                worker_id: si.worker_id,
            },
        );
    }
}

/// Panics on every message — used to prove panic isolation across workers.
struct PanicOnProbe;

impl ActorInterface for PanicOnProbe {
    type Incoming = Probe;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: Probe) {
        panic!("boom");
    }
}

/// Watches a target on start and records an `ActorExited` notification.
struct CrossWorkerWatcher {
    target: ActorAddress,
    got: Arc<AtomicUsize>,
}

impl ActorInterface for CrossWorkerWatcher {
    type Incoming = Probe;
    type Response = ();
    fn on_start(&mut self, ctx: &Ctx) {
        ctx.watch(self.target);
    }
    fn on_actor_exit(&mut self, _ctx: &Ctx, _exited: ActorExited) {
        self.got.fetch_add(1, Ordering::SeqCst);
    }
    fn handle(&mut self, _ctx: &Ctx, _msg: Probe) {}
}

/// Marker fired once by each per-worker extension instance.
#[derive(Clone)]
struct WorkerExtFired(usize);

struct DistinctWorkerExt {
    fired: bool,
    id: usize,
    report: ActorAddress,
}

impl WorkerExtension for DistinctWorkerExt {
    fn has_pending_work(&self) -> bool {
        !self.fired
    }
    fn on_tick(&mut self) -> Vec<(ActorAddress, Box<dyn Any + Send>)> {
        if self.fired {
            return Vec::new();
        }
        self.fired = true;
        vec![(self.report, Box::new(WorkerExtFired(self.id)))]
    }
    fn handle_request(&mut self, _request: Box<dyn Any + Send>) {}
    fn gc_dead(&mut self, _dead: &[ActorAddress]) {}
}

struct DistinctExt {
    report: ActorAddress,
    next: AtomicUsize,
}

impl RuntimeExtension for DistinctExt {
    fn on_actor_death(
        &self,
        _dead: &[(
            ActorAddress,
            swactor::actor::StopReason,
            Option<swactor::actor::ExitValue>,
        )],
    ) -> Vec<(ActorAddress, Box<dyn Any + Send>)> {
        Vec::new()
    }
    fn cleanup_dead(&self, _dead: &[ActorAddress]) {}
    fn on_spawn(
        &self,
        _child: ActorAddress,
        _parent: Option<ActorAddress>,
        env: swactor::actor::Environment,
        _uptime_ms: u64,
    ) -> swactor::actor::Environment {
        env
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn create_worker_extension(&self) -> Option<Box<dyn WorkerExtension>> {
        Some(Box::new(DistinctWorkerExt {
            fired: false,
            id: self.next.fetch_add(1, Ordering::SeqCst),
            report: self.report,
        }))
    }
}

#[test]
fn targeted_admin_mutates_only_owning_worker() {
    let (rt, mut host) = std_host(config_with(2));
    let a = rt
        .spawn(CountingProbe(Arc::new(AtomicUsize::new(0))))
        .expect("spawn a");
    let b = rt
        .spawn(CountingProbe(Arc::new(AtomicUsize::new(0))))
        .expect("spawn b");
    // a → worker 0, b → worker 1 (round-robin).

    // Suspend only a; b on the other worker must be untouched.
    rt.admin()
        .suspend_actor(a)
        .expect("suspend")
        .recv_ticking(&mut host, 8)
        .expect("suspend ok");

    let summary_a = rt
        .admin()
        .inspect_actor(a)
        .expect("inspect")
        .recv_ticking(&mut host, 8)
        .expect("inspect a ok");
    let summary_b = rt
        .admin()
        .inspect_actor(b)
        .expect("inspect")
        .recv_ticking(&mut host, 8)
        .expect("inspect b ok");
    assert!(summary_a.summary.status.suspended, "a suspended");
    assert!(
        !summary_b.summary.status.suspended,
        "b on another worker is not affected by a's targeted admin"
    );
}

#[test]
fn list_actors_spans_every_worker() {
    let (rt, mut host) = std_host(config_with(3));
    let mut addrs = Vec::new();
    for _ in 0..3 {
        addrs.push(
            rt.spawn(CountingProbe(Arc::new(AtomicUsize::new(0))))
                .expect("spawn"),
        );
    }
    host.try_tick(); // ensure actors are installed before listing

    let resp = rt
        .admin()
        .list_actors()
        .expect("list")
        .recv_ticking(&mut host, 8)
        .expect("list ok");

    // Exactly one summary per actor, spanning all three workers exactly once.
    assert_eq!(
        resp.actors.len(),
        3,
        "list_actors completes once with every actor"
    );
    let mut workers: Vec<usize> = resp.actors.iter().map(|s| s.worker_id).collect();
    workers.sort();
    assert_eq!(
        workers,
        vec![0, 1, 2],
        "actors from every worker are represented"
    );
}

#[test]
fn stats_report_real_worker_ids_and_runtime_width() {
    let (rt, _host) = std_host(config_with(3));
    let mut addrs = Vec::new();
    for _ in 0..5 {
        addrs.push(
            rt.spawn(CountingProbe(Arc::new(AtomicUsize::new(0))))
                .expect("spawn"),
        );
    }

    let stats = rt.stats();
    assert_eq!(stats.num_workers, 3, "num_workers reflects worker_count");
    assert_eq!(stats.workers.len(), 3, "one WorkerInfo per worker");
    let workers: Vec<usize> = addrs.iter().map(|a| worker_of(&stats, *a)).collect();
    assert_eq!(
        workers,
        vec![0, 1, 2, 0, 1],
        "placements use real worker ids"
    );
}

#[test]
fn system_info_reflects_runtime_width() {
    let (rt, mut host) = std_host(config_with(3));
    let report = rt.new_inbox::<SystemReport>().expect("inbox");
    // First spawn → worker 0.
    let reporter = rt
        .spawn(SystemReporter {
            reply_to: *report.addr(),
        })
        .expect("spawn reporter");

    // Spawn two more so the runtime-wide actor count is observably > 1.
    let _ = rt
        .spawn(CountingProbe(Arc::new(AtomicUsize::new(0))))
        .expect("spawn extra");
    let _ = rt
        .spawn(CountingProbe(Arc::new(AtomicUsize::new(0))))
        .expect("spawn extra");

    rt.send_to(reporter, Probe).expect("trigger");
    let msg = tick_until_recv(&mut host, &report, 16).expect("system report");

    assert_eq!(msg.worker_id, 0, "reporter observes its own worker");
    assert_eq!(
        msg.num_workers, 3,
        "system_info reports runtime-wide worker count"
    );
    assert!(
        msg.total_actors >= 3,
        "total_actors is runtime-wide, not per-worker"
    );
}

#[test]
fn per_worker_extensions_are_distinct() {
    let parts = RuntimeParts::new(config_with(3));
    let rt = parts.runtime().clone();
    let inbox = rt.new_inbox::<WorkerExtFired>().expect("inbox");
    let ext = Arc::new(DistinctExt {
        report: *inbox.addr(),
        next: AtomicUsize::new(0),
    });
    let parts = parts.with_extension(ext);
    let mut host = SingleThreadRuntime::new(parts);

    host.try_tick();

    let mut ids = Vec::new();
    while let Some(m) = inbox.try_recv() {
        ids.push(m.0);
    }
    ids.sort();
    ids.dedup();
    assert_eq!(
        ids,
        vec![0, 1, 2],
        "three distinct per-worker extension instances fired"
    );
}

#[test]
fn panic_on_one_worker_does_not_stop_another() {
    let (rt, mut host) = std_host(config_with(2));
    let healthy_counter = Arc::new(AtomicUsize::new(0));
    // Round-robin: panicker → worker 0, healthy → worker 1.
    let _panicker = rt.spawn(PanicOnProbe).expect("spawn panicker");
    let healthy = rt
        .spawn(CountingProbe(healthy_counter.clone()))
        .expect("spawn healthy");

    rt.send_to(_panicker, Probe).expect("trigger panic");
    rt.send_to(healthy, Probe).expect("trigger healthy");
    tick_n(&mut host, 8);

    assert_eq!(
        healthy_counter.load(Ordering::SeqCst),
        1,
        "worker 1 keeps processing after worker 0's actor panicked"
    );
    // The panicked actor is gone from the runtime's address map.
    let stats = rt.stats();
    assert!(
        !stats.actors.iter().any(|(a, _)| *a == _panicker),
        "panicked actor is cleaned up"
    );
}

#[test]
fn death_notification_crosses_workers() {
    let (rt, mut host) = std_host(config_with(2));
    let got = Arc::new(AtomicUsize::new(0));

    // watched → worker 0; watcher → worker 1.
    let watched = rt
        .spawn(CountingProbe(Arc::new(AtomicUsize::new(0))))
        .expect("spawn watched");
    let watcher = rt
        .spawn(CrossWorkerWatcher {
            target: watched,
            got: got.clone(),
        })
        .expect("spawn watcher");
    let _ = watcher;

    // Install both and run on_start (which registers the watch).
    tick_n(&mut host, 4);

    rt.stop_actor(watched).expect("stop watched");
    tick_n(&mut host, 12);

    assert_eq!(
        got.load(Ordering::SeqCst),
        1,
        "watcher on worker 1 received the exit notification from worker 0"
    );
}
