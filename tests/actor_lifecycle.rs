//! Actor Lifecycle Tests — birth, life, death of individual actors.
//!
//! Covers: spawning, on_start, lifecycle decision paths, parent-child delegation,
//! graceful stop, panic isolation, dead actor cleanup, and watching (ActorExited).

mod common;
use common::*;

use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// ── Local actors ────────────────────────────────────────────────────────────

/// Records lifecycle events to shared counters.
struct LifecycleActor {
    started: Arc<AtomicUsize>,
    stopped: Arc<AtomicUsize>,
    handled: Arc<AtomicUsize>,
}

impl ActorInterface for LifecycleActor {
    type Incoming = Ping;
    type Response = Pong;
    fn on_start(&mut self, _ctx: &Ctx) {
        self.started.fetch_add(1, Ordering::Relaxed);
    }
    fn on_stop(&mut self, _ctx: &Ctx) {
        self.stopped.fetch_add(1, Ordering::Relaxed);
    }
    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        self.handled.fetch_add(1, Ordering::Relaxed);
        let _ = ctx.send(msg.reply_to, Pong);
    }
}

/// Stops itself after processing `stop_after` messages.
struct SelfStopActor {
    count: usize,
    stop_after: usize,
    stopped: Arc<AtomicUsize>,
}

impl ActorInterface for SelfStopActor {
    type Incoming = Forward;
    type Response = Done;
    fn on_stop(&mut self, _ctx: &Ctx) {
        self.stopped.fetch_add(1, Ordering::Relaxed);
    }
    fn handle(&mut self, ctx: &Ctx, msg: Forward) {
        self.count += 1;
        let _ = ctx.send(msg.reply_to, Done(msg.value));
        if self.count >= self.stop_after {
            ctx.stop_self();
        }
    }
}

/// Sends a farewell Pong in on_stop.
struct FarewellActor {
    farewell_to: ActorAddress,
}

impl ActorInterface for FarewellActor {
    type Incoming = Ping;
    type Response = Pong;
    fn on_stop(&mut self, ctx: &Ctx) {
        let _ = ctx.send(self.farewell_to, Pong);
    }
    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        let _ = ctx.send(msg.reply_to, Pong);
    }
}

/// Panics in on_start.
struct PanicOnStartActor {
    handled: Arc<AtomicUsize>,
}

impl ActorInterface for PanicOnStartActor {
    type Incoming = Ping;
    type Response = Pong;
    fn on_start(&mut self, _ctx: &Ctx) {
        panic!("on_start panic");
    }
    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {
        self.handled.fetch_add(1, Ordering::Relaxed);
    }
}

/// Spawns a DoubleActor child, sends it work, then panics.
struct SpawnThenPanicActor;

impl ActorInterface for SpawnThenPanicActor {
    type Incoming = Forward;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: Forward) {
        let child = ctx.spawn(DoubleActor).unwrap();
        let _ = ctx.send(
            child,
            Forward {
                value: msg.value,
                reply_to: msg.reply_to,
            },
        );
        panic!("intentional panic after spawn+send");
    }
}

/// Sends a Pong reply, then panics.
struct SendThenPanicActor;

impl ActorInterface for SendThenPanicActor {
    type Incoming = Ping;
    type Response = Pong;
    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        let _ = ctx.send(msg.reply_to, Pong);
        panic!("intentional panic after send");
    }
}

/// Processes `remaining_good` messages then panics.
struct PanicAfterNActor {
    remaining_good: usize,
    counter: Arc<AtomicUsize>,
}

impl ActorInterface for PanicAfterNActor {
    type Incoming = Ping;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {
        if self.remaining_good == 0 {
            panic!("intentional delayed panic");
        }
        self.remaining_good -= 1;
        self.counter.fetch_add(1, Ordering::SeqCst);
    }
}

/// Stops on a trigger message.
struct StopOnTrigger(Arc<AtomicUsize>);

#[derive(Clone)]
struct Trigger(bool);

impl ActorInterface for StopOnTrigger {
    type Incoming = Trigger;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: Trigger) {
        self.0.fetch_add(1, Ordering::Relaxed);
        if msg.0 {
            ctx.stop_self();
        }
    }
}

/// Watches targets and counts exit notifications via on_actor_exit.
struct ExitWatcher {
    exit_count: Arc<AtomicUsize>,
    last_reason: Arc<Mutex<Option<ExitReason>>>,
    last_addr: Arc<Mutex<Option<ActorAddress>>>,
}

#[derive(Clone)]
enum WatcherCmd {
    WatchThis(ActorAddress),
}

impl ActorInterface for ExitWatcher {
    type Incoming = WatcherCmd;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: WatcherCmd) {
        match msg {
            WatcherCmd::WatchThis(target) => ctx.watch(target),
        }
    }
    fn on_actor_exit(&mut self, _ctx: &Ctx, exited: ActorExited) {
        self.exit_count.fetch_add(1, Ordering::SeqCst);
        *self.last_reason.lock() = Some(exited.reason);
        *self.last_addr.lock() = Some(exited.addr);
    }
}

struct WatcherState {
    exit_count: Arc<AtomicUsize>,
    last_reason: Arc<Mutex<Option<ExitReason>>>,
}

impl WatcherState {
    fn count(&self) -> usize {
        self.exit_count.load(Ordering::SeqCst)
    }
    fn last_reason(&self) -> Option<ExitReason> {
        self.last_reason.lock().clone()
    }
}

fn new_exit_watcher() -> (ExitWatcher, WatcherState) {
    let exit_count = Arc::new(AtomicUsize::new(0));
    let last_reason = Arc::new(Mutex::new(None));
    let last_addr = Arc::new(Mutex::new(None));
    let state = WatcherState {
        exit_count: exit_count.clone(),
        last_reason: last_reason.clone(),
    };
    (
        ExitWatcher {
            exit_count,
            last_reason,
            last_addr,
        },
        state,
    )
}

/// A silent actor that does nothing (target for watching tests).
struct Sleeper;
#[derive(Clone)]
struct Noop;

impl ActorInterface for Sleeper {
    type Incoming = Noop;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: Noop) {}
}

#[derive(Clone)]
struct Work;

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkCount(usize);

struct StartStopCountingActor {
    started: Arc<AtomicUsize>,
    stopped: Arc<AtomicUsize>,
    handled: Arc<AtomicUsize>,
}

impl ActorInterface for StartStopCountingActor {
    type Incoming = Work;
    type Response = ();

    fn on_start(&mut self, _ctx: &Ctx) {
        self.started.fetch_add(1, Ordering::SeqCst);
    }

    fn handle(&mut self, _ctx: &Ctx, _msg: Work) {
        self.handled.fetch_add(1, Ordering::SeqCst);
    }

    fn on_stop(&mut self, _ctx: &Ctx) {
        self.stopped.fetch_add(1, Ordering::SeqCst);
    }
}

struct StopReportingCounter {
    handled: usize,
    report_to: ActorAddress,
}

impl ActorInterface for StopReportingCounter {
    type Incoming = Work;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Work) {
        self.handled += 1;
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        let _ = ctx.send(self.report_to, WorkCount(self.handled));
    }
}

struct PanicOnWorkNumber {
    handled: usize,
    panic_at: usize,
}

impl ActorInterface for PanicOnWorkNumber {
    type Incoming = Work;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Work) {
        self.handled += 1;
        if self.handled == self.panic_at {
            panic!("intentional panic at work item {}", self.panic_at);
        }
    }
}

struct PanicOnStartWithStopReport {
    handled: Arc<AtomicUsize>,
    stopped: Arc<AtomicUsize>,
}

impl ActorInterface for PanicOnStartWithStopReport {
    type Incoming = Work;
    type Response = ();

    fn on_start(&mut self, _ctx: &Ctx) {
        panic!("intentional on_start panic");
    }

    fn handle(&mut self, _ctx: &Ctx, _msg: Work) {
        self.handled.fetch_add(1, Ordering::SeqCst);
    }

    fn on_stop(&mut self, _ctx: &Ctx) {
        self.stopped.fetch_add(1, Ordering::SeqCst);
    }
}

struct PanicOnHandleWithStopReport {
    stopped: Arc<AtomicUsize>,
}

impl ActorInterface for PanicOnHandleWithStopReport {
    type Incoming = Work;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Work) {
        panic!("intentional handle panic");
    }

    fn on_stop(&mut self, _ctx: &Ctx) {
        self.stopped.fetch_add(1, Ordering::SeqCst);
    }
}

fn drain_work_counts(inbox: &Inbox<WorkCount>) -> Vec<usize> {
    std::iter::from_fn(|| inbox.try_recv())
        .map(|WorkCount(count)| count)
        .collect()
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

/// Actors are spawned, on_start fires exactly once per instance before any
/// message, then state accumulates across messages.
#[test]
fn actor_from_birth_to_first_message() {
    let started = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(AtomicUsize::new(0));
    let handled = Arc::new(AtomicUsize::new(0));

    let (rt, mut host) = std_host(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();

    // Spawn one tracked actor + 4 more sharing the same counters
    let addr = rt
        .spawn(LifecycleActor {
            started: started.clone(),
            stopped: stopped.clone(),
            handled: handled.clone(),
        })
        .unwrap();
    for _ in 0..4 {
        rt.spawn(LifecycleActor {
            started: started.clone(),
            stopped: stopped.clone(),
            handled: handled.clone(),
        })
        .unwrap();
    }

    // First tick: all 5 on_start fire, no messages processed yet
    host.try_tick();
    assert_eq!(started.load(Ordering::Relaxed), 5, "on_start per instance");
    assert_eq!(
        handled.load(Ordering::Relaxed),
        0,
        "no messages before first send"
    );

    // Send 3 Increments to a CounterActor to verify state accumulation
    let counter_addr = rt.spawn(CounterActor { count: 0 }).unwrap();
    let count_inbox = rt.new_inbox::<Count>().unwrap();
    for _ in 0..3 {
        rt.send_to(
            counter_addr,
            Increment {
                reply_to: *count_inbox.addr(),
            },
        )
        .unwrap();
    }
    let replies = tick_and_drain(&mut host, &count_inbox, 10);
    assert_eq!(
        replies,
        vec![Count(1), Count(2), Count(3)],
        "state accumulates"
    );

    // on_start must not fire again on subsequent ticks
    host.try_tick();
    host.try_tick();
    assert_eq!(started.load(Ordering::Relaxed), 5, "on_start not repeated");

    // Verify the first actor still responds normally
    rt.send_to(
        addr,
        Ping {
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();
    let reply = tick_until_recv(&mut host, &inbox, 10);
    assert!(reply.is_some(), "actor handles messages after on_start");
}

/// Delegation chains: parent spawns child, child spawns grandchild, fan-out
/// distributes work. Spawn+send interleaving in a single handler works.
#[test]
fn parent_child_delegation_and_spawn_chains() {
    let (rt, mut host) = std_host(RuntimeConfig {
        max_actors: 2000,
        ..Default::default()
    });

    // Act 1: DelegatorActor spawns child, forwards value 7 → Done(14)
    let delegator = rt.spawn(DelegatorActor).unwrap();
    let inbox = rt.new_inbox::<Done>().unwrap();
    rt.send_to(
        delegator,
        Forward {
            value: 7,
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();
    let reply = tick_until_recv(&mut host, &inbox, 20);
    assert_eq!(reply, Some(Done(14)), "delegator child doubles value");

    // Act 2: Chain of depth 20
    let chain = rt.spawn(ChainActor).unwrap();
    rt.send_to(
        chain,
        ChainMsg {
            remaining: 20,
            depth: 0,
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();
    let reply = tick_until_recv(&mut host, &inbox, 200);
    assert_eq!(reply, Some(Done(20)), "chain reaches depth 20");

    // Act 3: Fan-out to 20 children
    let fan = rt.spawn(FanOutActor).unwrap();
    rt.send_to(
        fan,
        FanOut {
            count: 20,
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();
    let replies = tick_and_drain(&mut host, &inbox, 50);
    assert_eq!(replies.len(), 20, "all 20 fan-out children reply");
}

/// The full graceful-stop story: self-stop with on_stop, farewell messages,
/// external stop ordering vs pending messages, mid-mailbox stop trigger.
#[test]
fn graceful_stop_lifecycle() {
    // --- Part A: SelfStopActor ---
    let stopped = Arc::new(AtomicUsize::new(0));
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Done>().unwrap();

    let addr = rt
        .spawn(SelfStopActor {
            count: 0,
            stop_after: 3,
            stopped: stopped.clone(),
        })
        .unwrap();

    for i in 0..5 {
        let _ = rt.send_to(
            addr,
            Forward {
                value: i,
                reply_to: *inbox.addr(),
            },
        );
    }
    tick_n(&mut host, 10);

    let mut replies = Vec::new();
    while let Some(Done(v)) = inbox.try_recv() {
        replies.push(v);
    }
    assert_eq!(
        replies.len(),
        3,
        "only 3 messages processed before self-stop"
    );
    assert_eq!(stopped.load(Ordering::Relaxed), 1, "on_stop fired");
    assert!(
        rt.send_to(
            addr,
            Forward {
                value: 99,
                reply_to: *inbox.addr()
            }
        )
        .is_err(),
        "send to stopped actor fails"
    );

    // --- Part B: FarewellActor sends farewell in on_stop ---
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let addr = rt
        .spawn(FarewellActor {
            farewell_to: *inbox.addr(),
        })
        .unwrap();
    host.try_tick();
    rt.stop_actor(addr).unwrap();
    tick_n(&mut host, 5);
    assert_eq!(
        inbox.try_recv(),
        Some(Pong),
        "farewell message delivered from on_stop"
    );

    // --- Part C: External stop after pending messages ---
    let stopped = Arc::new(AtomicUsize::new(0));
    let handled = Arc::new(AtomicUsize::new(0));
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let addr = rt
        .spawn(LifecycleActor {
            started: Arc::new(AtomicUsize::new(0)),
            stopped: stopped.clone(),
            handled: handled.clone(),
        })
        .unwrap();
    for _ in 0..10 {
        let _ = rt.send_to(
            addr,
            Ping {
                reply_to: *inbox.addr(),
            },
        );
    }
    rt.stop_actor(addr).unwrap();
    tick_n(&mut host, 10);
    assert_eq!(
        handled.load(Ordering::Relaxed),
        10,
        "all pending messages processed before stop"
    );
    assert_eq!(
        stopped.load(Ordering::Relaxed),
        1,
        "on_stop fires after messages"
    );

    // --- Part D: External stop before messages → 0 processed ---
    let handled = Arc::new(AtomicUsize::new(0));
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let addr = rt
        .spawn(LifecycleActor {
            started: Arc::new(AtomicUsize::new(0)),
            stopped: Arc::new(AtomicUsize::new(0)),
            handled: handled.clone(),
        })
        .unwrap();
    host.try_tick(); // on_start
    rt.stop_actor(addr).unwrap();
    for _ in 0..5 {
        let _ = rt.send_to(
            addr,
            Ping {
                reply_to: *inbox.addr(),
            },
        );
    }
    tick_n(&mut host, 10);
    assert_eq!(
        handled.load(Ordering::Relaxed),
        0,
        "stop before messages prevents processing"
    );

    // --- Part E: Mid-mailbox stop trigger ---
    let processed = Arc::new(AtomicUsize::new(0));
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let addr = rt.spawn(StopOnTrigger(processed.clone())).unwrap();
    host.try_tick();
    rt.send_to(addr, Trigger(false)).unwrap();
    rt.send_to(addr, Trigger(false)).unwrap();
    rt.send_to(addr, Trigger(true)).unwrap(); // stop trigger
    rt.send_to(addr, Trigger(false)).unwrap();
    rt.send_to(addr, Trigger(false)).unwrap();
    tick_n(&mut host, 5);
    assert_eq!(
        processed.load(Ordering::Relaxed),
        3,
        "only messages up to and including stop trigger processed"
    );
    assert!(rt.send_to(addr, Trigger(false)).is_err());
}

/// Panics are caught: healthy siblings survive, panicked actors are poisoned
/// and cleaned from stats/address map, mid-batch panic discards remaining,
/// child spawned before parent panic survives, message sent before panic is
/// delivered, bulk cleanup, on_start panic also poisons.
#[test]
fn panic_isolation_and_cleanup() {
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let count_inbox = rt.new_inbox::<Count>().unwrap();

    // Spawn a healthy counter, a PanicActor, and a PanicOnStartActor
    let good = rt.spawn(CounterActor { count: 0 }).unwrap();
    let bad = rt.spawn(PanicActor).unwrap();
    let bad_start_handled = Arc::new(AtomicUsize::new(0));
    let bad_start = rt
        .spawn(PanicOnStartActor {
            handled: bad_start_handled.clone(),
        })
        .unwrap();

    // Trigger panics
    rt.send_to(bad, PanicMsg).unwrap();
    let _ = rt.send_to(
        bad_start,
        Ping {
            reply_to: *inbox.addr(),
        },
    );
    tick_n(&mut host, 10);

    // Healthy actor still works
    rt.send_to(
        good,
        Increment {
            reply_to: *count_inbox.addr(),
        },
    )
    .unwrap();
    rt.send_to(
        good,
        Increment {
            reply_to: *count_inbox.addr(),
        },
    )
    .unwrap();
    let replies = tick_and_drain(&mut host, &count_inbox, 10);
    assert_eq!(
        replies,
        vec![Count(1), Count(2)],
        "healthy actor unaffected by peer panics"
    );

    // Poisoned actors are cleaned from address map
    assert!(
        rt.send_to(bad, PanicMsg).is_err(),
        "send to cleaned-up actor fails"
    );
    assert_eq!(
        bad_start_handled.load(Ordering::Relaxed),
        0,
        "on_start panic prevents messages"
    );

    // Stats track panics vs stops separately
    let stats = rt.stats();
    let panics: u64 = stats.workers.iter().map(|w| w.panics).sum();
    assert!(
        panics >= 2,
        "at least 2 panics recorded (PanicActor + PanicOnStartActor)"
    );

    // --- Mid-batch panic discards remaining ---
    let counter = Arc::new(AtomicUsize::new(0));
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let dummy = rt.new_inbox::<Pong>().unwrap();
    let addr = rt
        .spawn(PanicAfterNActor {
            remaining_good: 2,
            counter: counter.clone(),
        })
        .unwrap();
    for _ in 0..5 {
        rt.send_to(
            addr,
            Ping {
                reply_to: *dummy.addr(),
            },
        )
        .unwrap();
    }
    tick_n(&mut host, 20);
    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "only messages before panic processed"
    );

    // --- Child spawned before parent panic survives ---
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Done>().unwrap();
    let parent = rt.spawn(SpawnThenPanicActor).unwrap();
    rt.send_to(
        parent,
        Forward {
            value: 5,
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();
    let reply = tick_until_recv(&mut host, &inbox, 30);
    assert_eq!(reply, Some(Done(10)), "child survives parent panic");

    // --- Message sent before panic is delivered ---
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let addr = rt.spawn(SendThenPanicActor).unwrap();
    rt.send_to(
        addr,
        Ping {
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();
    let reply = tick_until_recv(&mut host, &inbox, 20);
    assert!(reply.is_some(), "message sent before panic still delivered");

    // --- Bulk cleanup: 20 panicking actors all cleaned ---
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let mut addrs = Vec::new();
    for _ in 0..20 {
        addrs.push(rt.spawn(PanicActor).unwrap());
    }
    for &addr in &addrs {
        let _ = rt.send_to(addr, PanicMsg);
    }
    tick_n(&mut host, 10);
    let stats = rt.stats();
    assert_eq!(
        stats.workers[0].num_actors, 0,
        "all poisoned actors cleaned up"
    );
}

/// Watch API contract: watchers are notified on death, unwatch cancels,
/// double-watch is idempotent, multiple watchers all notified,
/// runtime-level watch works.
#[test]
fn watch_notification_contract() {
    let (rt, mut host) = std_host(RuntimeConfig::default());

    let target = rt.spawn(PanicActor).unwrap();
    let (w1, s1) = new_exit_watcher();
    let (w2, s2) = new_exit_watcher();
    let (w3, s3) = new_exit_watcher();

    let w1_addr = rt.spawn(w1).unwrap();
    let w2_addr = rt.spawn(w2).unwrap();
    let w3_addr = rt.spawn(w3).unwrap();

    rt.send_to(w1_addr, WatcherCmd::WatchThis(target)).unwrap();
    rt.send_to(w2_addr, WatcherCmd::WatchThis(target)).unwrap();
    rt.send_to(w3_addr, WatcherCmd::WatchThis(target)).unwrap();
    tick_n(&mut host, 3);

    // w2 double-watches: registration is idempotent.
    rt.send_to(w2_addr, WatcherCmd::WatchThis(target)).unwrap();
    tick_n(&mut host, 3);

    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&mut host, 5);

    assert_eq!(s1.count(), 1, "watcher 1 notified");
    assert_eq!(s2.count(), 1, "double-watch still only one notification");
    assert_eq!(s3.count(), 1, "watcher 3 notified");
    assert_eq!(s1.last_reason(), Some(ExitReason::Panicked));
}

/// Watch edge cases: watcher dies before target (no crash), self-watch (no
/// crash), watcher reacts to death by spawning a replacement.
#[test]
fn watch_edge_cases() {
    // Self-watch — no crash
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let (w, _s) = new_exit_watcher();
    let addr = rt.spawn(w).unwrap();
    rt.send_to(addr, WatcherCmd::WatchThis(addr)).unwrap();
    tick_n(&mut host, 5);

    // Watcher reacts to death by spawning replacement
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let spawned = Arc::new(AtomicUsize::new(0));

    struct SupervisorWatcher {
        spawned_count: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    enum SupCmd {
        WatchThis(ActorAddress),
    }

    impl ActorInterface for SupervisorWatcher {
        type Incoming = SupCmd;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: SupCmd) {
            match msg {
                SupCmd::WatchThis(target) => ctx.watch(target),
            }
        }
        fn on_actor_exit(&mut self, ctx: &Ctx, _exited: ActorExited) {
            let _ = ctx.spawn(Sleeper);
            self.spawned_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    let target = rt.spawn(PanicActor).unwrap();
    let sup = rt
        .spawn(SupervisorWatcher {
            spawned_count: spawned.clone(),
        })
        .unwrap();
    rt.send_to(sup, SupCmd::WatchThis(target)).unwrap();
    tick_n(&mut host, 3);
    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&mut host, 5);
    assert_eq!(
        spawned.load(Ordering::SeqCst),
        1,
        "watcher spawned replacement"
    );
}

/// Core lifecycle decision paths are observable without the old guarantee module:
/// healthy actors handle work, stopping actors skip later work and run `on_stop`
/// once, and poisoned actors never run `handle` or `on_stop` after poisoning.
#[test]
fn lifecycle_decision_paths_match_runtime_behavior() {
    for msg_count in 1..=5 {
        let (rt, mut host) = plain_host(RuntimeConfig::default());
        let started = Arc::new(AtomicUsize::new(0));
        let stopped = Arc::new(AtomicUsize::new(0));
        let handled = Arc::new(AtomicUsize::new(0));

        let addr = rt
            .spawn(StartStopCountingActor {
                started: started.clone(),
                stopped: stopped.clone(),
                handled: handled.clone(),
            })
            .unwrap();
        host.try_tick();

        for _ in 0..msg_count {
            rt.send_to(addr, Work).unwrap();
        }
        tick_n(&mut host, msg_count + 3);
        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert_eq!(handled.load(Ordering::SeqCst), msg_count);

        rt.stop_actor(addr).unwrap();
        tick_n(&mut host, 3);
        assert_eq!(stopped.load(Ordering::SeqCst), 1);
    }

    for msg_count in 1..=5 {
        let (rt, mut host) = plain_host(RuntimeConfig::default());
        let started = Arc::new(AtomicUsize::new(0));
        let stopped = Arc::new(AtomicUsize::new(0));
        let handled = Arc::new(AtomicUsize::new(0));

        let addr = rt
            .spawn(StartStopCountingActor {
                started,
                stopped: stopped.clone(),
                handled: handled.clone(),
            })
            .unwrap();
        host.try_tick();

        rt.stop_actor(addr).unwrap();
        for _ in 0..msg_count {
            let _ = rt.send_to(addr, Work);
        }
        tick_n(&mut host, 5);
        assert_eq!(handled.load(Ordering::SeqCst), 0);
        assert_eq!(stopped.load(Ordering::SeqCst), 1);
    }

    for msg_count in 1..=5 {
        let (rt, mut host) = plain_host(RuntimeConfig::default());
        let handled = Arc::new(AtomicUsize::new(0));
        let stopped = Arc::new(AtomicUsize::new(0));

        let addr = rt
            .spawn(PanicOnStartWithStopReport {
                handled: handled.clone(),
                stopped: stopped.clone(),
            })
            .unwrap();
        host.try_tick();

        for _ in 0..msg_count {
            let _ = rt.send_to(addr, Work);
        }
        tick_n(&mut host, msg_count + 3);
        assert_eq!(handled.load(Ordering::SeqCst), 0);
        assert_eq!(stopped.load(Ordering::SeqCst), 0);
    }

    let (rt, mut host) = plain_host(RuntimeConfig::default());
    let stopped = Arc::new(AtomicUsize::new(0));
    let addr = rt
        .spawn(PanicOnHandleWithStopReport {
            stopped: stopped.clone(),
        })
        .unwrap();
    host.try_tick();

    rt.send_to(addr, Work).unwrap();
    tick_n(&mut host, 5);
    assert_eq!(stopped.load(Ordering::SeqCst), 0);
}

/// Deterministic replacements for the old property guarantee: delayed panics,
/// start panics, and same-tick multi-panics must not reduce sibling progress.
#[test]
fn panicking_actors_do_not_affect_sibling_progress() {
    for (healthy_count, msg_count, panic_at) in [(2, 1, 1), (4, 65, 17), (8, 130, 1)] {
        let (rt, mut host) = plain_host(RuntimeConfig::default());
        let report_inbox = rt.new_inbox::<WorkCount>().unwrap();
        let report_to = *report_inbox.addr();

        let mut healthy = Vec::with_capacity(healthy_count);
        for _ in 0..healthy_count {
            healthy.push(
                rt.spawn(StopReportingCounter {
                    handled: 0,
                    report_to,
                })
                .unwrap(),
            );
        }
        let panicker = rt
            .spawn(PanicOnWorkNumber {
                handled: 0,
                panic_at,
            })
            .unwrap();
        host.try_tick();

        for _ in 0..msg_count {
            for &addr in &healthy {
                rt.send_to(addr, Work).unwrap();
            }
            rt.send_to(panicker, Work).unwrap();
        }
        tick_n(&mut host, (msg_count / 64) + 10);

        for &addr in &healthy {
            rt.stop_actor(addr).unwrap();
        }
        tick_n(&mut host, 3);

        let reports = drain_work_counts(&report_inbox);
        assert_eq!(reports.len(), healthy_count);
        assert!(
            reports.iter().all(|&count| count == msg_count),
            "healthy reports were {reports:?}, expected every actor to process {msg_count}"
        );
    }
}

#[test]
fn on_start_panic_does_not_block_siblings() {
    for (before_count, after_count, msgs_each) in [(1, 1, 1), (4, 4, 25), (8, 3, 70)] {
        let (rt, mut host) = plain_host(RuntimeConfig::default());
        let started = Arc::new(AtomicUsize::new(0));
        let stopped = Arc::new(AtomicUsize::new(0));
        let handled = Arc::new(AtomicUsize::new(0));
        let panic_handled = Arc::new(AtomicUsize::new(0));
        let panic_stopped = Arc::new(AtomicUsize::new(0));

        let mut siblings = Vec::with_capacity(before_count + after_count);
        for _ in 0..before_count {
            siblings.push(
                rt.spawn(StartStopCountingActor {
                    started: started.clone(),
                    stopped: stopped.clone(),
                    handled: handled.clone(),
                })
                .unwrap(),
            );
        }

        let _panic_addr = rt
            .spawn(PanicOnStartWithStopReport {
                handled: panic_handled.clone(),
                stopped: panic_stopped.clone(),
            })
            .unwrap();

        for _ in 0..after_count {
            siblings.push(
                rt.spawn(StartStopCountingActor {
                    started: started.clone(),
                    stopped: stopped.clone(),
                    handled: handled.clone(),
                })
                .unwrap(),
            );
        }
        tick_n(&mut host, 3);

        assert_eq!(started.load(Ordering::SeqCst), siblings.len());
        assert_eq!(panic_handled.load(Ordering::SeqCst), 0);
        assert_eq!(panic_stopped.load(Ordering::SeqCst), 0);

        for _ in 0..msgs_each {
            for &addr in &siblings {
                rt.send_to(addr, Work).unwrap();
            }
        }
        tick_n(&mut host, (msgs_each / 64) + 5);
        assert_eq!(handled.load(Ordering::SeqCst), siblings.len() * msgs_each);

        for &addr in &siblings {
            rt.stop_actor(addr).unwrap();
        }
        tick_n(&mut host, 3);
        assert_eq!(stopped.load(Ordering::SeqCst), siblings.len());
    }
}

#[test]
fn multiple_panics_in_same_tick_preserve_healthy_actors() {
    for (healthy_count, panic_count, msgs_each) in [(2, 2, 1), (6, 4, 70)] {
        let (rt, mut host) = plain_host(RuntimeConfig::default());
        let report_inbox = rt.new_inbox::<WorkCount>().unwrap();
        let report_to = *report_inbox.addr();

        let mut healthy = Vec::with_capacity(healthy_count);
        for _ in 0..healthy_count {
            healthy.push(
                rt.spawn(StopReportingCounter {
                    handled: 0,
                    report_to,
                })
                .unwrap(),
            );
        }

        let mut panickers = Vec::with_capacity(panic_count);
        for _ in 0..panic_count {
            panickers.push(
                rt.spawn(PanicOnWorkNumber {
                    handled: 0,
                    panic_at: 1,
                })
                .unwrap(),
            );
        }
        host.try_tick();

        for _ in 0..msgs_each {
            for &addr in &healthy {
                rt.send_to(addr, Work).unwrap();
            }
            for &addr in &panickers {
                rt.send_to(addr, Work).unwrap();
            }
        }
        tick_n(&mut host, (msgs_each / 64) + 10);

        for &addr in &healthy {
            rt.stop_actor(addr).unwrap();
        }
        tick_n(&mut host, 3);

        let reports = drain_work_counts(&report_inbox);
        assert_eq!(reports.len(), healthy_count);
        assert!(
            reports.iter().all(|&count| count == msgs_each),
            "healthy reports were {reports:?}, expected every actor to process {msgs_each}"
        );
    }
}
