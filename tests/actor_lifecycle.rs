//! Actor Lifecycle Tests — birth, life, death of individual actors.
//!
//! Covers: spawning, on_start, parent-child delegation, graceful stop,
//! panic isolation, dead actor cleanup, watching (ActorExited), and
//! monitoring (Down notifications).

mod common;
use common::*;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
        let _ = ctx.send(child, Forward { value: msg.value, reply_to: msg.reply_to });
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
    last_reason: Arc<std::sync::Mutex<Option<ExitReason>>>,
    last_addr: Arc<std::sync::Mutex<Option<ActorAddress>>>,
}

#[derive(Clone)]
enum WatcherCmd {
    WatchThis(ActorAddress),
    UnwatchThis(ActorAddress),
}

impl ActorInterface for ExitWatcher {
    type Incoming = WatcherCmd;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: WatcherCmd) {
        match msg {
            WatcherCmd::WatchThis(target) => ctx.watch(target),
            WatcherCmd::UnwatchThis(target) => ctx.unwatch(target),
        }
    }
    fn on_actor_exit(&mut self, _ctx: &Ctx, exited: ActorExited) {
        self.exit_count.fetch_add(1, Ordering::SeqCst);
        *self.last_reason.lock().unwrap() = Some(exited.reason);
        *self.last_addr.lock().unwrap() = Some(exited.addr);
    }
}

struct WatcherState {
    exit_count: Arc<AtomicUsize>,
    last_reason: Arc<std::sync::Mutex<Option<ExitReason>>>,
}

impl WatcherState {
    fn count(&self) -> usize {
        self.exit_count.load(Ordering::SeqCst)
    }
    fn last_reason(&self) -> Option<ExitReason> {
        self.last_reason.lock().unwrap().clone()
    }
}

fn new_exit_watcher() -> (ExitWatcher, WatcherState) {
    let exit_count = Arc::new(AtomicUsize::new(0));
    let last_reason = Arc::new(std::sync::Mutex::new(None));
    let last_addr = Arc::new(std::sync::Mutex::new(None));
    let state = WatcherState {
        exit_count: exit_count.clone(),
        last_reason: last_reason.clone(),
    };
    (
        ExitWatcher { exit_count, last_reason, last_addr },
        state,
    )
}

/// Monitors a target and forwards Down to a reply address.
struct MonitorWatcherActor {
    watch_target: ActorAddress,
    reply_to: ActorAddress,
    mref: Option<MonitorRef>,
}

impl ActorInterface for MonitorWatcherActor {
    type Incoming = Down;
    type Response = ();
    fn on_start(&mut self, ctx: &Ctx) {
        self.mref = Some(ctx.monitor(self.watch_target).unwrap());
    }
    fn handle(&mut self, ctx: &Ctx, msg: Down) {
        ctx.send(self.reply_to, msg).unwrap();
    }
}

/// Demonitors on Ping.
struct DemonitorActor {
    watch_target: ActorAddress,
    mref: Option<MonitorRef>,
}

impl ActorInterface for DemonitorActor {
    type Incoming = Ping;
    type Response = ();
    fn on_start(&mut self, ctx: &Ctx) {
        self.mref = Some(ctx.monitor(self.watch_target).unwrap());
    }
    fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
        if let Some(mref) = self.mref.take() {
            ctx.demonitor(mref);
        }
    }
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

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();

    // Spawn one tracked actor + 4 more sharing the same counters
    let addr = rt.spawn(LifecycleActor {
        started: started.clone(),
        stopped: stopped.clone(),
        handled: handled.clone(),
    }).unwrap();
    for _ in 0..4 {
        rt.spawn(LifecycleActor {
            started: started.clone(),
            stopped: stopped.clone(),
            handled: handled.clone(),
        }).unwrap();
    }

    // First tick: all 5 on_start fire, no messages processed yet
    rt.tick();
    assert_eq!(started.load(Ordering::Relaxed), 5, "on_start per instance");
    assert_eq!(handled.load(Ordering::Relaxed), 0, "no messages before first send");

    // Send 3 Increments to a CounterActor to verify state accumulation
    let counter_addr = rt.spawn(CounterActor { count: 0 }).unwrap();
    let count_inbox = rt.new_inbox::<Count>().unwrap();
    for _ in 0..3 {
        rt.send_to(counter_addr, Increment { reply_to: *count_inbox.addr() }).unwrap();
    }
    let replies = tick_and_drain(&rt, &count_inbox, 10);
    assert_eq!(replies, vec![Count(1), Count(2), Count(3)], "state accumulates");

    // on_start must not fire again on subsequent ticks
    rt.tick();
    rt.tick();
    assert_eq!(started.load(Ordering::Relaxed), 5, "on_start not repeated");

    // Verify the first actor still responds normally
    rt.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();
    let reply = tick_until_recv(&rt, &inbox, 10);
    assert!(reply.is_some(), "actor handles messages after on_start");
}

/// Delegation chains: parent spawns child, child spawns grandchild, fan-out
/// distributes work. Spawn+send interleaving in a single handler works.
#[test]
fn parent_child_delegation_and_spawn_chains() {
    let rt = std_runtime(RuntimeConfig {
        max_actors: 2000,
        ..Default::default()
    });

    // Act 1: DelegatorActor spawns child, forwards value 7 → Done(14)
    let delegator = rt.spawn(DelegatorActor).unwrap();
    let inbox = rt.new_inbox::<Done>().unwrap();
    rt.send_to(delegator, Forward { value: 7, reply_to: *inbox.addr() }).unwrap();
    let reply = tick_until_recv(&rt, &inbox, 20);
    assert_eq!(reply, Some(Done(14)), "delegator child doubles value");

    // Act 2: Chain of depth 20
    let chain = rt.spawn(ChainActor).unwrap();
    rt.send_to(chain, ChainMsg { remaining: 20, depth: 0, reply_to: *inbox.addr() }).unwrap();
    let reply = tick_until_recv(&rt, &inbox, 200);
    assert_eq!(reply, Some(Done(20)), "chain reaches depth 20");

    // Act 3: Fan-out to 20 children
    let fan = rt.spawn(FanOutActor).unwrap();
    rt.send_to(fan, FanOut { count: 20, reply_to: *inbox.addr() }).unwrap();
    let replies = tick_and_drain(&rt, &inbox, 50);
    assert_eq!(replies.len(), 20, "all 20 fan-out children reply");
}

/// The full graceful-stop story: self-stop with on_stop, farewell messages,
/// external stop ordering vs pending messages, mid-mailbox stop trigger.
#[test]
fn graceful_stop_lifecycle() {
    // --- Part A: SelfStopActor ---
    let stopped = Arc::new(AtomicUsize::new(0));
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Done>().unwrap();

    let addr = rt.spawn(SelfStopActor {
        count: 0,
        stop_after: 3,
        stopped: stopped.clone(),
    }).unwrap();

    for i in 0..5 {
        let _ = rt.send_to(addr, Forward { value: i, reply_to: *inbox.addr() });
    }
    tick_n(&rt, 10);

    let mut replies = Vec::new();
    while let Some(Done(v)) = inbox.try_recv() {
        replies.push(v);
    }
    assert_eq!(replies.len(), 3, "only 3 messages processed before self-stop");
    assert_eq!(stopped.load(Ordering::Relaxed), 1, "on_stop fired");
    assert!(rt.send_to(addr, Forward { value: 99, reply_to: *inbox.addr() }).is_err(),
        "send to stopped actor fails");

    // --- Part B: FarewellActor sends farewell in on_stop ---
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let addr = rt.spawn(FarewellActor { farewell_to: *inbox.addr() }).unwrap();
    rt.tick();
    rt.stop_actor(addr).unwrap();
    tick_n(&rt, 5);
    assert_eq!(inbox.try_recv(), Some(Pong), "farewell message delivered from on_stop");

    // --- Part C: External stop after pending messages ---
    let stopped = Arc::new(AtomicUsize::new(0));
    let handled = Arc::new(AtomicUsize::new(0));
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let addr = rt.spawn(LifecycleActor {
        started: Arc::new(AtomicUsize::new(0)),
        stopped: stopped.clone(),
        handled: handled.clone(),
    }).unwrap();
    for _ in 0..10 {
        let _ = rt.send_to(addr, Ping { reply_to: *inbox.addr() });
    }
    rt.stop_actor(addr).unwrap();
    tick_n(&rt, 10);
    assert_eq!(handled.load(Ordering::Relaxed), 10, "all pending messages processed before stop");
    assert_eq!(stopped.load(Ordering::Relaxed), 1, "on_stop fires after messages");

    // --- Part D: External stop before messages → 0 processed ---
    let handled = Arc::new(AtomicUsize::new(0));
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let addr = rt.spawn(LifecycleActor {
        started: Arc::new(AtomicUsize::new(0)),
        stopped: Arc::new(AtomicUsize::new(0)),
        handled: handled.clone(),
    }).unwrap();
    rt.tick(); // on_start
    rt.stop_actor(addr).unwrap();
    for _ in 0..5 {
        let _ = rt.send_to(addr, Ping { reply_to: *inbox.addr() });
    }
    tick_n(&rt, 10);
    assert_eq!(handled.load(Ordering::Relaxed), 0, "stop before messages prevents processing");

    // --- Part E: Mid-mailbox stop trigger ---
    let processed = Arc::new(AtomicUsize::new(0));
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(StopOnTrigger(processed.clone())).unwrap();
    rt.tick();
    rt.send_to(addr, Trigger(false)).unwrap();
    rt.send_to(addr, Trigger(false)).unwrap();
    rt.send_to(addr, Trigger(true)).unwrap(); // stop trigger
    rt.send_to(addr, Trigger(false)).unwrap();
    rt.send_to(addr, Trigger(false)).unwrap();
    tick_n(&rt, 5);
    assert_eq!(processed.load(Ordering::Relaxed), 3,
        "only messages up to and including stop trigger processed");
    assert!(rt.send_to(addr, Trigger(false)).is_err());
}

/// Panics are caught: healthy siblings survive, panicked actors are poisoned
/// and cleaned from stats/address map, mid-batch panic discards remaining,
/// child spawned before parent panic survives, message sent before panic is
/// delivered, bulk cleanup, on_start panic also poisons.
#[test]
fn panic_isolation_and_cleanup() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let count_inbox = rt.new_inbox::<Count>().unwrap();

    // Spawn a healthy counter, a PanicActor, and a PanicOnStartActor
    let good = rt.spawn(CounterActor { count: 0 }).unwrap();
    let bad = rt.spawn(PanicActor).unwrap();
    let bad_start_handled = Arc::new(AtomicUsize::new(0));
    let bad_start = rt.spawn(PanicOnStartActor { handled: bad_start_handled.clone() }).unwrap();

    // Trigger panics
    rt.send_to(bad, PanicMsg).unwrap();
    let _ = rt.send_to(bad_start, Ping { reply_to: *inbox.addr() });
    tick_n(&rt, 10);

    // Healthy actor still works
    rt.send_to(good, Increment { reply_to: *count_inbox.addr() }).unwrap();
    rt.send_to(good, Increment { reply_to: *count_inbox.addr() }).unwrap();
    let replies = tick_and_drain(&rt, &count_inbox, 10);
    assert_eq!(replies, vec![Count(1), Count(2)], "healthy actor unaffected by peer panics");

    // Poisoned actors are cleaned from address map
    assert!(rt.send_to(bad, PanicMsg).is_err(), "send to cleaned-up actor fails");
    assert_eq!(bad_start_handled.load(Ordering::Relaxed), 0, "on_start panic prevents messages");

    // Stats track panics vs stops separately
    let stats = rt.stats();
    let panics: u64 = stats.workers.iter().map(|w| w.panics).sum();
    assert!(panics >= 2, "at least 2 panics recorded (PanicActor + PanicOnStartActor)");

    // --- Mid-batch panic discards remaining ---
    let counter = Arc::new(AtomicUsize::new(0));
    let rt = std_runtime(RuntimeConfig::default());
    let dummy = rt.new_inbox::<Pong>().unwrap();
    let addr = rt.spawn(PanicAfterNActor { remaining_good: 2, counter: counter.clone() }).unwrap();
    for _ in 0..5 {
        rt.send_to(addr, Ping { reply_to: *dummy.addr() }).unwrap();
    }
    tick_n(&rt, 20);
    assert_eq!(counter.load(Ordering::SeqCst), 2, "only messages before panic processed");

    // --- Child spawned before parent panic survives ---
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Done>().unwrap();
    let parent = rt.spawn(SpawnThenPanicActor).unwrap();
    rt.send_to(parent, Forward { value: 5, reply_to: *inbox.addr() }).unwrap();
    let reply = tick_until_recv(&rt, &inbox, 30);
    assert_eq!(reply, Some(Done(10)), "child survives parent panic");

    // --- Message sent before panic is delivered ---
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let addr = rt.spawn(SendThenPanicActor).unwrap();
    rt.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();
    let reply = tick_until_recv(&rt, &inbox, 20);
    assert!(reply.is_some(), "message sent before panic still delivered");

    // --- Bulk cleanup: 20 panicking actors all cleaned ---
    let rt = std_runtime(RuntimeConfig::default());
    let mut addrs = Vec::new();
    for _ in 0..20 {
        addrs.push(rt.spawn(PanicActor).unwrap());
    }
    for &addr in &addrs {
        let _ = rt.send_to(addr, PanicMsg);
    }
    tick_n(&rt, 10);
    let stats = rt.stats();
    assert_eq!(stats.workers[0].num_actors, 0, "all poisoned actors cleaned up");
}

/// Watch API contract: watchers are notified on death, unwatch cancels,
/// double-watch is idempotent, multiple watchers all notified,
/// runtime-level watch works.
#[test]
fn watch_notification_contract() {
    let rt = std_runtime(RuntimeConfig::default());

    // Spawn target + 3 watchers + 1 that unwatches
    let target = rt.spawn(PanicActor).unwrap();
    let (w1, s1) = new_exit_watcher();
    let (w2, s2) = new_exit_watcher();
    let (w3, s3) = new_exit_watcher();
    let (w4, s4) = new_exit_watcher(); // will unwatch

    let w1_addr = rt.spawn(w1).unwrap();
    let w2_addr = rt.spawn(w2).unwrap();
    let w3_addr = rt.spawn(w3).unwrap();
    let w4_addr = rt.spawn(w4).unwrap();

    // All watch the target
    rt.send_to(w1_addr, WatcherCmd::WatchThis(target)).unwrap();
    rt.send_to(w2_addr, WatcherCmd::WatchThis(target)).unwrap();
    rt.send_to(w3_addr, WatcherCmd::WatchThis(target)).unwrap();
    rt.send_to(w4_addr, WatcherCmd::WatchThis(target)).unwrap();
    tick_n(&rt, 3);

    // w2 double-watches (idempotent test)
    rt.send_to(w2_addr, WatcherCmd::WatchThis(target)).unwrap();
    tick_n(&rt, 3);

    // w4 unwatches
    rt.send_to(w4_addr, WatcherCmd::UnwatchThis(target)).unwrap();
    tick_n(&rt, 3);

    // Kill target
    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&rt, 5);

    assert_eq!(s1.count(), 1, "watcher 1 notified");
    assert_eq!(s2.count(), 1, "double-watch still only one notification");
    assert_eq!(s3.count(), 1, "watcher 3 notified");
    assert_eq!(s4.count(), 0, "unwatched watcher not notified");
    assert_eq!(s1.last_reason(), Some(ExitReason::Panicked));

    // --- Runtime-level watch ---
    let rt = std_runtime(RuntimeConfig::default());
    let target = rt.spawn(PanicActor).unwrap();
    let (w, s) = new_exit_watcher();
    let w_addr = rt.spawn(w).unwrap();
    tick_n(&rt, 2);
    rt.watch(w_addr, target);
    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&rt, 5);
    assert_eq!(s.count(), 1, "runtime-level watch delivers notification");
}

/// Watch edge cases: watcher dies before target (no crash), self-watch (no
/// crash), watcher reacts to death by spawning a replacement.
#[test]
fn watch_edge_cases() {
    // Watcher dies before target — no crash
    let rt = std_runtime(RuntimeConfig::default());
    let target = rt.spawn(PanicActor).unwrap();
    let target2 = rt.spawn(PanicActor).unwrap();
    rt.watch(target2, target);
    tick_n(&rt, 3);
    rt.send_to(target2, PanicMsg).unwrap(); // kill watcher first
    tick_n(&rt, 5);
    rt.send_to(target, PanicMsg).unwrap(); // kill target — no crash
    tick_n(&rt, 5);

    // Self-watch — no crash
    let rt = std_runtime(RuntimeConfig::default());
    let (w, _s) = new_exit_watcher();
    let addr = rt.spawn(w).unwrap();
    rt.send_to(addr, WatcherCmd::WatchThis(addr)).unwrap();
    tick_n(&rt, 5);

    // Watcher reacts to death by spawning replacement
    let rt = std_runtime(RuntimeConfig::default());
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
    let sup = rt.spawn(SupervisorWatcher { spawned_count: spawned.clone() }).unwrap();
    rt.send_to(sup, SupCmd::WatchThis(target)).unwrap();
    tick_n(&rt, 3);
    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&rt, 5);
    assert_eq!(spawned.load(Ordering::SeqCst), 1, "watcher spawned replacement");
}

/// Monitor API contract: Down on stop (Normal) and panic (Panicked), multiple
/// monitors, demonitor cancels, dead watcher cleanup, stacked monitors,
/// external inbox, handle_down dispatch.
#[test]
fn monitor_death_notification_contract() {
    let rt = std_runtime(RuntimeConfig::default());

    // --- Stop → Down(Normal) ---
    let inbox = rt.new_inbox::<Down>().unwrap();
    let target = rt.spawn(PingPongActor).unwrap();
    rt.spawn(MonitorWatcherActor {
        watch_target: target,
        reply_to: *inbox.addr(),
        mref: None,
    }).unwrap();
    rt.tick();
    rt.stop_actor(target).unwrap();
    tick_n(&rt, 3);
    let down = inbox.try_recv().expect("Down on graceful stop");
    assert_eq!(down.addr, target);
    assert_eq!(down.reason, StopReason::Normal);

    // --- Panic → Down(Panicked) ---
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Down>().unwrap();
    let target = rt.spawn(PanicActor).unwrap();
    rt.spawn(MonitorWatcherActor {
        watch_target: target,
        reply_to: *inbox.addr(),
        mref: None,
    }).unwrap();
    rt.tick();
    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&rt, 3);
    let down = inbox.try_recv().expect("Down on panic");
    assert_eq!(down.reason, StopReason::Panicked);

    // --- Multiple monitors ---
    let rt = std_runtime(RuntimeConfig::default());
    let inbox1 = rt.new_inbox::<Down>().unwrap();
    let inbox2 = rt.new_inbox::<Down>().unwrap();
    let target = rt.spawn(PingPongActor).unwrap();
    rt.spawn(MonitorWatcherActor {
        watch_target: target, reply_to: *inbox1.addr(), mref: None,
    }).unwrap();
    rt.spawn(MonitorWatcherActor {
        watch_target: target, reply_to: *inbox2.addr(), mref: None,
    }).unwrap();
    rt.tick();
    rt.stop_actor(target).unwrap();
    tick_n(&rt, 3);
    assert!(inbox1.try_recv().is_some(), "watcher 1 notified");
    assert!(inbox2.try_recv().is_some(), "watcher 2 notified");

    // --- Demonitor cancels ---
    let rt = std_runtime(RuntimeConfig::default());
    let down_inbox = rt.new_inbox::<Down>().unwrap();
    let target = rt.spawn(PingPongActor).unwrap();
    let watcher = rt.spawn(DemonitorActor { watch_target: target, mref: None }).unwrap();
    rt.tick();
    rt.send_to(watcher, Ping { reply_to: ActorAddress::default() }).unwrap();
    rt.tick(); // demonitor
    rt.stop_actor(target).unwrap();
    tick_n(&rt, 3);
    assert!(down_inbox.try_recv().is_none(), "demonitored: no Down delivered");

    // --- Dead watcher cleaned up ---
    let rt = std_runtime(RuntimeConfig::default());
    let target = rt.spawn(PingPongActor).unwrap();
    let watcher = rt.spawn(MonitorWatcherActor {
        watch_target: target,
        reply_to: ActorAddress::default(),
        mref: None,
    }).unwrap();
    rt.tick();
    rt.stop_actor(watcher).unwrap();
    rt.tick(); // watcher dies
    rt.stop_actor(target).unwrap();
    tick_n(&rt, 3); // target dies — no crash trying to deliver to dead watcher

    // --- Stacked monitors produce multiple notifications ---
    struct DoubleMonitor {
        target: ActorAddress,
        reply_to: ActorAddress,
    }
    impl ActorInterface for DoubleMonitor {
        type Incoming = Down;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            ctx.monitor(self.target).unwrap();
            ctx.monitor(self.target).unwrap();
        }
        fn handle(&mut self, ctx: &Ctx, msg: Down) {
            ctx.send(self.reply_to, msg).unwrap();
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Down>().unwrap();
    let target = rt.spawn(PingPongActor).unwrap();
    rt.spawn(DoubleMonitor { target, reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    rt.stop_actor(target).unwrap();
    tick_n(&rt, 3);
    assert!(inbox.try_recv().is_some(), "first Down from stacked monitor");
    assert!(inbox.try_recv().is_some(), "second Down from stacked monitor");
    assert!(inbox.try_recv().is_none(), "no more");

    // --- handle_down dispatch ---
    struct MonitoringTracker {
        target: ActorAddress,
        downs: Vec<Down>,
        inbox: ActorAddress,
    }
    impl ActorInterface for MonitoringTracker {
        type Incoming = Ping;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            ctx.monitor(self.target).unwrap();
        }
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let _ = ctx.send(self.inbox, Count(self.downs.len()));
        }
        fn handle_down(&mut self, _ctx: &Ctx, down: Down) {
            self.downs.push(down);
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Count>().unwrap();
    let target = rt.spawn(PanicActor).unwrap();
    let tracker = rt.spawn(MonitoringTracker {
        target,
        downs: vec![],
        inbox: *inbox.addr(),
    }).unwrap();
    rt.tick();
    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&rt, 3);
    rt.send_to(tracker, Ping { reply_to: ActorAddress::default() }).unwrap();
    rt.tick();
    assert_eq!(inbox.try_recv(), Some(Count(1)), "handle_down received exactly one Down");

    // --- When Incoming=Down, handle_down is NOT called ---
    struct DownAsIncoming {
        target: ActorAddress,
        inbox: ActorAddress,
    }
    impl ActorInterface for DownAsIncoming {
        type Incoming = Down;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            ctx.monitor(self.target).unwrap();
        }
        fn handle(&mut self, ctx: &Ctx, msg: Down) {
            let _ = ctx.send(self.inbox, msg);
        }
        fn handle_down(&mut self, _ctx: &Ctx, _down: Down) {
            panic!("handle_down must not be called when Incoming=Down");
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Down>().unwrap();
    let target = rt.spawn(PanicActor).unwrap();
    rt.spawn(DownAsIncoming { target, inbox: *inbox.addr() }).unwrap();
    rt.tick();
    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&rt, 3);
    let received = inbox.try_recv().expect("Down delivered via handle(), not handle_down");
    assert_eq!(received.reason, StopReason::Panicked);
}
