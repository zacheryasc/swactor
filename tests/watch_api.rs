use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use swactor::actor::{ActorAddress, ActorExited, ActorInterface, ExitReason};
use swactor::runtime::{Ctx, Runtime, RuntimeConfig};

// ── Actors ──────────────────────────────────────────────────────────────────

/// An actor that panics when it receives PanicMsg.
struct PanicOnCommand;

#[derive(Clone)]
struct PanicMsg;

impl ActorInterface for PanicOnCommand {
    type Incoming = PanicMsg;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: PanicMsg) {
        panic!("deliberate panic for test");
    }
}

/// An actor that watches targets and counts exit notifications.
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
            WatcherCmd::WatchThis(target) => {
                ctx.watch(target);
            }
            WatcherCmd::UnwatchThis(target) => {
                ctx.unwatch(target);
            }
        }
    }

    fn on_actor_exit(&mut self, _ctx: &Ctx, exited: ActorExited) {
        self.exit_count.fetch_add(1, Ordering::SeqCst);
        *self.last_reason.lock().unwrap() = Some(exited.reason);
        *self.last_addr.lock().unwrap() = Some(exited.addr);
    }
}

impl ExitWatcher {
    fn new() -> (Self, WatcherState) {
        let exit_count = Arc::new(AtomicUsize::new(0));
        let last_reason = Arc::new(std::sync::Mutex::new(None));
        let last_addr = Arc::new(std::sync::Mutex::new(None));
        let state = WatcherState {
            exit_count: exit_count.clone(),
            last_reason: last_reason.clone(),
            last_addr: last_addr.clone(),
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
}

/// Shared state for inspecting what ExitWatcher observed.
struct WatcherState {
    exit_count: Arc<AtomicUsize>,
    last_reason: Arc<std::sync::Mutex<Option<ExitReason>>>,
    last_addr: Arc<std::sync::Mutex<Option<ActorAddress>>>,
}

impl WatcherState {
    fn count(&self) -> usize {
        self.exit_count.load(Ordering::SeqCst)
    }
    fn last_reason(&self) -> Option<ExitReason> {
        self.last_reason.lock().unwrap().clone()
    }
    fn last_addr(&self) -> Option<ActorAddress> {
        *self.last_addr.lock().unwrap()
    }
}

/// A silent actor that does nothing (for targets that shouldn't panic).
struct Sleeper;

#[derive(Clone)]
struct Noop;

impl ActorInterface for Sleeper {
    type Incoming = Noop;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: Noop) {}
}

// ── Helper ──────────────────────────────────────────────────────────────────

fn tick_n(rt: &Runtime, n: usize) {
    for _ in 0..n {
        rt.tick();
    }
}

fn single_thread_config() -> RuntimeConfig {
    RuntimeConfig {
        num_threads: 1,
        ..RuntimeConfig::default()
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

/// Given a watcher and a target actor,
/// when the target panics,
/// then the watcher's on_actor_exit fires with ExitReason::Panicked.
#[test]
fn watch_receives_notification_on_panic() {
    let rt = Runtime::new(single_thread_config());
    let (watcher_actor, state) = ExitWatcher::new();

    let target = rt.spawn(PanicOnCommand).unwrap();
    let watcher = rt.spawn(watcher_actor).unwrap();

    // Tell watcher to watch the target
    rt.send_to(watcher, WatcherCmd::WatchThis(target)).unwrap();
    tick_n(&rt, 3);

    // Kill the target
    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&rt, 5);

    assert_eq!(state.count(), 1, "watcher should have received exactly one ActorExited");
    assert_eq!(state.last_reason(), Some(ExitReason::Panicked));
    assert_eq!(state.last_addr(), Some(target));
}

/// Given a watcher that watches then unwatches a target,
/// when the target panics,
/// then the watcher receives NO notification.
#[test]
fn unwatch_prevents_notification() {
    let rt = Runtime::new(single_thread_config());
    let (watcher_actor, state) = ExitWatcher::new();

    let target = rt.spawn(PanicOnCommand).unwrap();
    let watcher = rt.spawn(watcher_actor).unwrap();

    // Watch
    rt.send_to(watcher, WatcherCmd::WatchThis(target)).unwrap();
    tick_n(&rt, 3);

    // Unwatch
    rt.send_to(watcher, WatcherCmd::UnwatchThis(target)).unwrap();
    tick_n(&rt, 3);

    // Kill target
    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&rt, 5);

    assert_eq!(state.count(), 0, "after unwatch, no notification should be delivered");
}

/// Given a watch on an address that was never spawned,
/// then the watcher receives ActorExited { reason: Stopped }.
#[test]
fn watch_nonexistent_actor_delivers_stopped() {
    let rt = Runtime::new(single_thread_config());
    let (watcher_actor, state) = ExitWatcher::new();

    let watcher = rt.spawn(watcher_actor).unwrap();

    let nonexistent = ActorAddress::new_random();
    rt.send_to(watcher, WatcherCmd::WatchThis(nonexistent)).unwrap();
    tick_n(&rt, 5);

    assert_eq!(state.count(), 1, "should receive ActorExited for non-existent target");
    assert_eq!(state.last_reason(), Some(ExitReason::Stopped));
    assert_eq!(state.last_addr(), Some(nonexistent));
}

/// Given a watcher that dies before the target,
/// when the target subsequently panics,
/// then there is no panic or leak.
#[test]
fn watcher_dies_before_target_no_panic() {
    let rt = Runtime::new(single_thread_config());

    let target = rt.spawn(PanicOnCommand).unwrap();
    let (watcher_actor, _state) = ExitWatcher::new();
    let watcher = rt.spawn(watcher_actor).unwrap();

    // Watch
    rt.send_to(watcher, WatcherCmd::WatchThis(target)).unwrap();
    tick_n(&rt, 3);

    // Kill the watcher first (send it a type-mismatched panic msg directly)
    // Actually, ExitWatcher doesn't panic. Use Runtime-level watch + PanicOnCommand.
    let rt2 = Runtime::new(single_thread_config());
    let target2 = rt2.spawn(PanicOnCommand).unwrap();
    let watcher2 = rt2.spawn(PanicOnCommand).unwrap();

    use swactor::actor::ContextInner;
    rt2.watch(watcher2, target2);
    tick_n(&rt2, 3);

    // Kill watcher first
    rt2.send_to(watcher2, PanicMsg).unwrap();
    tick_n(&rt2, 5);

    // Kill target — should not crash
    rt2.send_to(target2, PanicMsg).unwrap();
    tick_n(&rt2, 5);

    // If we got here, no crash.
}

/// Given a watcher that calls watch() twice on the same target,
/// when the target panics,
/// then the watcher receives exactly one notification.
#[test]
fn idempotent_watch_delivers_one_notification() {
    let rt = Runtime::new(single_thread_config());
    let (watcher_actor, state) = ExitWatcher::new();

    let target = rt.spawn(PanicOnCommand).unwrap();
    let watcher = rt.spawn(watcher_actor).unwrap();

    // Watch twice
    rt.send_to(watcher, WatcherCmd::WatchThis(target)).unwrap();
    tick_n(&rt, 3);
    rt.send_to(watcher, WatcherCmd::WatchThis(target)).unwrap();
    tick_n(&rt, 3);

    // Kill target
    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&rt, 5);

    assert_eq!(state.count(), 1, "double watch should produce exactly one notification");
}

/// Given multiple watchers on the same target,
/// when the target panics,
/// then all watchers receive the notification.
#[test]
fn multiple_watchers_all_notified() {
    let rt = Runtime::new(single_thread_config());
    let (w1_actor, s1) = ExitWatcher::new();
    let (w2_actor, s2) = ExitWatcher::new();
    let (w3_actor, s3) = ExitWatcher::new();

    let target = rt.spawn(PanicOnCommand).unwrap();
    let w1 = rt.spawn(w1_actor).unwrap();
    let w2 = rt.spawn(w2_actor).unwrap();
    let w3 = rt.spawn(w3_actor).unwrap();

    rt.send_to(w1, WatcherCmd::WatchThis(target)).unwrap();
    rt.send_to(w2, WatcherCmd::WatchThis(target)).unwrap();
    rt.send_to(w3, WatcherCmd::WatchThis(target)).unwrap();
    tick_n(&rt, 3);

    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&rt, 5);

    assert_eq!(s1.count(), 1, "watcher 1 should be notified");
    assert_eq!(s2.count(), 1, "watcher 2 should be notified");
    assert_eq!(s3.count(), 1, "watcher 3 should be notified");
}

/// Self-watch doesn't crash the runtime.
#[test]
fn self_watch_does_not_crash() {
    let rt = Runtime::new(single_thread_config());
    let (watcher_actor, _state) = ExitWatcher::new();

    let actor = rt.spawn(watcher_actor).unwrap();
    rt.send_to(actor, WatcherCmd::WatchThis(actor)).unwrap();
    tick_n(&rt, 5);

    // No crash = pass
}

/// Runtime-level watch (outside actor context) delivers notification.
#[test]
fn runtime_level_watch_delivers_notification() {
    let rt = Runtime::new(single_thread_config());
    let (watcher_actor, state) = ExitWatcher::new();

    let target = rt.spawn(PanicOnCommand).unwrap();
    let watcher = rt.spawn(watcher_actor).unwrap();
    tick_n(&rt, 2); // ensure both spawned

    use swactor::actor::ContextInner;
    rt.watch(watcher, target);

    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&rt, 5);

    assert_eq!(state.count(), 1, "runtime-level watch should deliver notification");
    assert_eq!(state.last_reason(), Some(ExitReason::Panicked));
}

/// Runtime-level watch on non-existent address delivers Stopped.
#[test]
fn runtime_level_watch_nonexistent_delivers_stopped() {
    let rt = Runtime::new(single_thread_config());
    let (watcher_actor, state) = ExitWatcher::new();

    let watcher = rt.spawn(watcher_actor).unwrap();
    tick_n(&rt, 2);

    let fake = ActorAddress::new_random();
    use swactor::actor::ContextInner;
    rt.watch(watcher, fake);

    tick_n(&rt, 5);

    assert_eq!(state.count(), 1, "watching non-existent from runtime should deliver Stopped");
    assert_eq!(state.last_reason(), Some(ExitReason::Stopped));
}

/// Given a watcher watching target via on_actor_exit,
/// when target panics,
/// then the watcher can react by spawning a replacement (supervision pattern).
#[test]
fn watcher_can_react_to_death_by_spawning() {
    let rt = Runtime::new(single_thread_config());
    let spawned = Arc::new(AtomicUsize::new(0));

    struct Supervisor {
        spawned_count: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    enum SupervisorMsg {
        WatchThis(ActorAddress),
    }

    impl ActorInterface for Supervisor {
        type Incoming = SupervisorMsg;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: SupervisorMsg) {
            match msg {
                SupervisorMsg::WatchThis(target) => ctx.watch(target),
            }
        }

        fn on_actor_exit(&mut self, ctx: &Ctx, _exited: ActorExited) {
            // React: spawn a replacement
            let replacement = ctx.spawn(Sleeper).unwrap();
            let _ = replacement;
            self.spawned_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    let target = rt.spawn(PanicOnCommand).unwrap();
    let sup = rt.spawn(Supervisor { spawned_count: spawned.clone() }).unwrap();

    rt.send_to(sup, SupervisorMsg::WatchThis(target)).unwrap();
    tick_n(&rt, 3);

    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&rt, 5);

    assert_eq!(spawned.load(Ordering::SeqCst), 1, "supervisor should have spawned a replacement");
}
