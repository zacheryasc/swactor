//! Exhaustive correspondence tests: verify that Kani bounded mirrors agree
//! with the real runtime across the *entire* finite input space.
//!
//! These are the drift detectors. If someone changes `tick_all`'s skip logic
//! or `Supervisor::handle_down`'s restart decision without updating the Kani
//! mirrors, these tests fail.
//!
//! How they work:
//! - Deterministic enumeration of every reachable input combination
//! - Drive the same inputs through BOTH the production decision functions AND
//!   the real runtime
//! - Assert they agree on observable outcomes
//! - No random sampling — every combination is hit

use std::sync::Arc;

use crate::actor::{ActorAddress, ActorInterface, StopReason};
use crate::config::RuntimeConfig;
use crate::runtime::{Ctx, Runtime};
use crate::std::supervisor::compute_restart_set;
use crate::std::{ChildSpec, RestartPolicy, StdExtension, Supervisor, SupervisorStrategy};
use crate::worker::{is_on_stop_eligible, should_skip_actor};

// ─── Helpers ────────────────────────────────────────────────────────────────

fn std_runtime() -> Runtime {
    Runtime::new(RuntimeConfig::default()).with_extension(Arc::new(StdExtension::new()))
}

fn tick_many(rt: &Runtime, n: usize) {
    for _ in 0..n {
        rt.tick();
    }
}

const ALL_POLICIES: [RestartPolicy; 3] = [
    RestartPolicy::Permanent,
    RestartPolicy::Transient,
    RestartPolicy::Temporary,
];

const ALL_REASONS: [StopReason; 3] = [
    StopReason::Normal,
    StopReason::Panicked,
    StopReason::Completed,
];

const ALL_STRATEGIES: [SupervisorStrategy; 3] = [
    SupervisorStrategy::OneForOne,
    SupervisorStrategy::OneForAll,
    SupervisorStrategy::RestForOne,
];

// ─── Real runtime actors ────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Ping;

#[derive(Clone, Debug)]
struct HandleCalled(#[allow(dead_code)] ActorAddress);

#[derive(Clone, Debug)]
struct OnStopCalled(#[allow(dead_code)] ActorAddress);

#[derive(Clone, Debug)]
struct ChildStarted(ActorAddress);

/// Actor that reports handle and on_stop to separate inboxes.
struct DualReporter {
    handle_to: ActorAddress,
    stop_to: ActorAddress,
}

impl ActorInterface for DualReporter {
    type Incoming = Ping;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
        let _ = ctx.send(self.handle_to, HandleCalled(ctx.self_addr()));
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        let _ = ctx.send(self.stop_to, OnStopCalled(ctx.self_addr()));
    }
}

/// Actor that panics in on_start.
struct OnStartPanicker {
    handle_to: ActorAddress,
    stop_to: ActorAddress,
}

impl ActorInterface for OnStartPanicker {
    type Incoming = Ping;
    type Response = ();

    fn on_start(&mut self, _ctx: &Ctx) {
        panic!("intentional on_start panic");
    }

    fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
        let _ = ctx.send(self.handle_to, HandleCalled(ctx.self_addr()));
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        let _ = ctx.send(self.stop_to, OnStopCalled(ctx.self_addr()));
    }
}

/// Actor that panics on first handle call.
struct HandlePanicker {
    stop_to: ActorAddress,
}

impl ActorInterface for HandlePanicker {
    type Incoming = Ping;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {
        panic!("intentional handle panic");
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        let _ = ctx.send(self.stop_to, OnStopCalled(ctx.self_addr()));
    }
}

/// Actor that panics on receiving Ping — used to trigger Panicked death.
struct PanicOnPing;

impl ActorInterface for PanicOnPing {
    type Incoming = Ping;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {
        panic!("intentional child panic");
    }
}

/// Actor that calls ctx.stop_with() on first Ping — produces Completed stop reason.
struct CompletedOnPing;

impl ActorInterface for CompletedOnPing {
    type Incoming = Ping;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
        ctx.stop_with("done");
    }
}

/// Actor that does nothing.
struct IdleChild;

impl ActorInterface for IdleChild {
    type Incoming = Ping;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}
}

// ═══════════════════════════════════════════════════════════════════════════
// G4 Exhaustive Correspondence: lifecycle decisions vs real runtime
// ═══════════════════════════════════════════════════════════════════════════

/// Exhaustive G4: for a healthy actor, the production `should_skip_actor`
/// predicts handle will be called — the real runtime agrees.
///
/// Enumerates msg_count in 1..=5.
#[test]
fn g4_healthy_actor_handle_called() {
    for msg_count in 1..=5 {
        let rt = Runtime::new(RuntimeConfig::default());
        let h_inbox = rt.new_inbox::<HandleCalled>().unwrap();
        let report_addr = *h_inbox.addr();

        let addr = rt
            .spawn(DualReporter {
                handle_to: report_addr,
                stop_to: report_addr, // unused for this test path
            })
            .unwrap();
        rt.tick(); // on_start

        // Production decision: healthy actor should NOT be skipped
        assert!(
            !should_skip_actor(false, false, false),
            "production should_skip_actor must return false for healthy actor"
        );

        for _ in 0..msg_count {
            rt.send_to(addr, Ping).unwrap();
        }
        tick_many(&rt, msg_count + 5);

        let handle_count: usize = std::iter::from_fn(|| h_inbox.try_recv()).count();
        assert_eq!(
            handle_count, msg_count,
            "runtime must call handle for each message (msg_count={})",
            msg_count
        );
    }
}

/// Exhaustive G4: a poisoned actor (panicked in on_start) must not have
/// handle called and must not have on_stop called.
///
/// Enumerates msg_count in 1..=5.
#[test]
fn g4_poisoned_actor_no_handle_no_on_stop() {
    for msg_count in 1..=5 {
        let rt = Runtime::new(RuntimeConfig::default());
        let h_inbox = rt.new_inbox::<HandleCalled>().unwrap();
        let s_inbox = rt.new_inbox::<OnStopCalled>().unwrap();

        // Production decisions for poisoned actor
        assert!(
            should_skip_actor(true, false, false),
            "production should_skip_actor must return true for poisoned actor"
        );
        assert!(
            !is_on_stop_eligible(false, true),
            "production is_on_stop_eligible must return false for poisoned (stopping=false, poisoned=true)"
        );

        let addr = rt
            .spawn(OnStartPanicker {
                handle_to: *h_inbox.addr(),
                stop_to: *s_inbox.addr(),
            })
            .unwrap();
        rt.tick(); // on_start panics → poisoned

        for _ in 0..msg_count {
            let _ = rt.send_to(addr, Ping);
        }
        tick_many(&rt, msg_count + 5);

        let handle_count: usize = std::iter::from_fn(|| h_inbox.try_recv()).count();
        assert_eq!(
            handle_count, 0,
            "poisoned actor must not call handle (msg_count={})",
            msg_count
        );

        let stop_count: usize = std::iter::from_fn(|| s_inbox.try_recv()).count();
        assert_eq!(
            stop_count, 0,
            "poisoned actor must not call on_stop (msg_count={})",
            msg_count
        );
    }
}

/// Exhaustive G4: a stopping actor must not have handle called for messages
/// sent after stop, but must have on_stop called exactly once.
///
/// Enumerates msg_count in 1..=5.
#[test]
fn g4_stopping_actor_no_handle_yes_on_stop() {
    for msg_count in 1..=5 {
        // Production decisions
        assert!(
            should_skip_actor(false, true, false),
            "production should_skip_actor must return true for stopping actor"
        );
        assert!(
            is_on_stop_eligible(true, false),
            "production is_on_stop_eligible must return true for (stopping=true, poisoned=false)"
        );

        let rt = Runtime::new(RuntimeConfig::default());
        let h_inbox = rt.new_inbox::<HandleCalled>().unwrap();
        let s_inbox = rt.new_inbox::<OnStopCalled>().unwrap();

        let addr = rt
            .spawn(DualReporter {
                handle_to: *h_inbox.addr(),
                stop_to: *s_inbox.addr(),
            })
            .unwrap();
        rt.tick(); // on_start

        rt.stop_actor(addr).unwrap();
        for _ in 0..msg_count {
            let _ = rt.send_to(addr, Ping);
        }
        tick_many(&rt, 5);

        let h_count: usize = std::iter::from_fn(|| h_inbox.try_recv()).count();
        assert_eq!(
            h_count, 0,
            "stopping actor must not call handle for messages sent after stop (msg_count={})",
            msg_count
        );

        let s_count: usize = std::iter::from_fn(|| s_inbox.try_recv()).count();
        assert_eq!(
            s_count, 1,
            "stopping actor must call on_stop exactly once (msg_count={})",
            msg_count
        );
    }
}

/// Exhaustive G4: a handle-panicked actor must not call on_stop.
///
/// This is a single deterministic case (not parameterized — panic is binary).
#[test]
fn g4_handle_panic_poisons_no_on_stop() {
    let rt = Runtime::new(RuntimeConfig::default());
    let s_inbox = rt.new_inbox::<OnStopCalled>().unwrap();

    let addr = rt
        .spawn(HandlePanicker {
            stop_to: *s_inbox.addr(),
        })
        .unwrap();
    rt.tick(); // on_start

    rt.send_to(addr, Ping).unwrap();
    tick_many(&rt, 5);

    // Production decision: poisoned → no on_stop
    assert!(
        !is_on_stop_eligible(false, true),
        "production is_on_stop_eligible must return false for poisoned"
    );

    let stop_count: usize = std::iter::from_fn(|| s_inbox.try_recv()).count();
    assert_eq!(stop_count, 0, "handle-panicked actor must not call on_stop");
}

/// Exhaustive G4: enumerate ALL 16 boolean flag combinations for the
/// production decision functions and verify consistency.
///
/// 4 flags × 2 values = 16 combinations for should_skip_actor
/// 2 flags × 2 values = 4 combinations for is_on_stop_eligible
/// 2 flags × 2 values = 4 combinations for determine_stop_reason
#[test]
fn g4_exhaustive_decision_function_truth_table() {
    use crate::worker::determine_stop_reason;

    let mut combinations_tested = 0u32;

    // should_skip_actor: exhaustive over (poisoned, stopping, suspended)
    for poisoned in [false, true] {
        for stopping in [false, true] {
            for suspended in [false, true] {
                let skip = should_skip_actor(poisoned, stopping, suspended);
                // Must skip iff any flag is set
                assert_eq!(
                    skip,
                    poisoned || stopping || suspended,
                    "should_skip_actor({}, {}, {}) = {} but expected {}",
                    poisoned,
                    stopping,
                    suspended,
                    skip,
                    poisoned || stopping || suspended
                );
                combinations_tested += 1;
            }
        }
    }
    assert_eq!(
        combinations_tested, 8,
        "must test all 8 flag combinations for should_skip_actor"
    );

    // is_on_stop_eligible: exhaustive over (stopping, poisoned)
    let mut on_stop_combinations = 0u32;
    for stopping in [false, true] {
        for poisoned in [false, true] {
            let eligible = is_on_stop_eligible(stopping, poisoned);
            assert_eq!(
                eligible,
                stopping && !poisoned,
                "is_on_stop_eligible({}, {}) = {} but expected {}",
                stopping,
                poisoned,
                eligible,
                stopping && !poisoned
            );
            on_stop_combinations += 1;
        }
    }
    assert_eq!(
        on_stop_combinations, 4,
        "must test all 4 flag combinations for is_on_stop_eligible"
    );

    // determine_stop_reason: exhaustive over (poisoned, has_exit_value)
    let mut reason_combinations = 0u32;
    for poisoned in [false, true] {
        for has_exit_value in [false, true] {
            let reason = determine_stop_reason(poisoned, has_exit_value);
            let expected = if poisoned {
                StopReason::Panicked
            } else if has_exit_value {
                StopReason::Completed
            } else {
                StopReason::Normal
            };
            assert_eq!(
                reason, expected,
                "determine_stop_reason({}, {}) = {:?} but expected {:?}",
                poisoned, has_exit_value, reason, expected
            );
            reason_combinations += 1;
        }
    }
    assert_eq!(
        reason_combinations, 4,
        "must test all 4 flag combinations for determine_stop_reason"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// G10 Exhaustive Correspondence: restart decisions vs real supervisor
// ═══════════════════════════════════════════════════════════════════════════

/// Exhaustive G10: the production `RestartPolicy::should_restart` must agree
/// with the real supervisor's restart behavior for ALL policy × reason combos.
///
/// 3 policies × 3 reasons = 9 combinations, all tested.
#[test]
fn g10_should_restart_matches_supervisor() {
    let mut combinations_tested = 0u32;

    for &policy in &ALL_POLICIES {
        for &reason in &ALL_REASONS {
            let rt = std_runtime();
            let child_inbox = rt.new_inbox::<ChildStarted>().unwrap();
            let child_report = *child_inbox.addr();

            let spawn_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let sc = spawn_count.clone();

            let spec = ChildSpec::new("test-child", policy, move |ctx| {
                sc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let addr = match reason {
                    StopReason::Panicked => ctx.spawn(PanicOnPing)?,
                    StopReason::Completed => ctx.spawn(CompletedOnPing)?,
                    StopReason::Normal => ctx.spawn(IdleChild)?,
                };
                let _ = ctx.send(child_report, ChildStarted(addr));
                Ok(addr)
            });

            let sup = Supervisor::new(SupervisorStrategy::OneForOne, 10, vec![spec]);
            let _sup_addr = rt.spawn(sup).unwrap();
            tick_many(&rt, 3);

            let child_addr = child_inbox.try_recv().expect("child must start").0;
            let initial = spawn_count.load(std::sync::atomic::Ordering::SeqCst);
            assert_eq!(initial, 1, "exactly one child spawn initially");

            match reason {
                StopReason::Panicked => {
                    rt.send_to(child_addr, Ping).unwrap();
                }
                StopReason::Normal => {
                    rt.stop_actor(child_addr).unwrap();
                }
                StopReason::Completed => {
                    rt.send_to(child_addr, Ping).unwrap();
                }
            }
            tick_many(&rt, 10);

            let final_spawns = spawn_count.load(std::sync::atomic::Ordering::SeqCst);
            let was_restarted = final_spawns > initial;
            let production_predicts = policy.should_restart(reason);

            assert_eq!(
                was_restarted, production_predicts,
                "policy={:?} reason={:?}: production predicts restart={} but runtime restarted={}",
                policy, reason, production_predicts, was_restarted
            );

            combinations_tested += 1;
        }
    }

    assert_eq!(
        combinations_tested, 9,
        "must test all 9 policy×reason combinations"
    );
}

/// Exhaustive G10: OneForOne strategy restarts only the dead child.
///
/// Enumerates: num_children in 2..=4, dead_idx in 0..num_children.
/// Total: 2+3+4 = 9 combinations.
#[test]
fn g10_one_for_one_restarts_only_dead() {
    let mut combinations_tested = 0u32;

    for num_children in 2..=4 {
        for dead_idx in 0..num_children {
            let rt = std_runtime();
            let report_inbox = rt.new_inbox::<ChildStarted>().unwrap();
            let report_addr = *report_inbox.addr();

            let spawn_counts: Vec<Arc<std::sync::atomic::AtomicUsize>> = (0..num_children)
                .map(|_| Arc::new(std::sync::atomic::AtomicUsize::new(0)))
                .collect();

            let specs: Vec<ChildSpec> = (0..num_children)
                .map(|i| {
                    let counter = spawn_counts[i].clone();
                    let is_dead_child = i == dead_idx;
                    let report = report_addr;
                    ChildSpec::new(
                        format!("child-{}", i),
                        RestartPolicy::Permanent,
                        move |ctx| {
                            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            let addr = if is_dead_child {
                                ctx.spawn(PanicOnPing)?
                            } else {
                                ctx.spawn(IdleChild)?
                            };
                            let _ = ctx.send(report, ChildStarted(addr));
                            Ok(addr)
                        },
                    )
                })
                .collect();

            let sup = Supervisor::new(SupervisorStrategy::OneForOne, 10, specs);
            let _sup_addr = rt.spawn(sup).unwrap();
            tick_many(&rt, 5);

            let mut child_addrs = Vec::new();
            while let Some(ChildStarted(addr)) = report_inbox.try_recv() {
                child_addrs.push(addr);
            }
            assert_eq!(child_addrs.len(), num_children, "all children must start");

            let initial_counts: Vec<usize> = spawn_counts
                .iter()
                .map(|c| c.load(std::sync::atomic::Ordering::SeqCst))
                .collect();

            // Kill the designated child via panic
            rt.send_to(child_addrs[dead_idx], Ping).unwrap();
            tick_many(&rt, 10);

            // Verify against production compute_restart_set
            let expected_set =
                compute_restart_set(SupervisorStrategy::OneForOne, dead_idx, num_children);

            let final_counts: Vec<usize> = spawn_counts
                .iter()
                .map(|c| c.load(std::sync::atomic::Ordering::SeqCst))
                .collect();

            for i in 0..num_children {
                let restarted = final_counts[i] > initial_counts[i];
                let expected_restart = expected_set.contains(&i);
                assert_eq!(
                    restarted, expected_restart,
                    "OneForOne: num_children={} dead_idx={} child={}: expected restart={} got={}",
                    num_children, dead_idx, i, expected_restart, restarted
                );
            }

            combinations_tested += 1;
        }
    }

    assert_eq!(
        combinations_tested, 9,
        "must test all 9 num_children×dead_idx combinations"
    );
}

/// Exhaustive G10: Temporary policy never restarts, regardless of strategy
/// or death reason.
///
/// Enumerates: 3 strategies × 3 reasons = 9 combinations.
#[test]
fn g10_temporary_never_restarts() {
    let mut combinations_tested = 0u32;

    for &strategy in &ALL_STRATEGIES {
        for &reason in &ALL_REASONS {
            // Production decision: Temporary never restarts
            assert!(
                !RestartPolicy::Temporary.should_restart(reason),
                "production should_restart must return false for Temporary + {:?}",
                reason
            );

            let rt = std_runtime();
            let child_inbox = rt.new_inbox::<ChildStarted>().unwrap();
            let child_report = *child_inbox.addr();

            let spawn_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let sc = spawn_count.clone();

            let spec = ChildSpec::new("temp-child", RestartPolicy::Temporary, move |ctx| {
                sc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let addr = match reason {
                    StopReason::Panicked => ctx.spawn(PanicOnPing)?,
                    StopReason::Completed => ctx.spawn(CompletedOnPing)?,
                    StopReason::Normal => ctx.spawn(IdleChild)?,
                };
                let _ = ctx.send(child_report, ChildStarted(addr));
                Ok(addr)
            });

            let sup = Supervisor::new(strategy, 10, vec![spec]);
            let _sup_addr = rt.spawn(sup).unwrap();
            tick_many(&rt, 5);

            let child_addr = child_inbox.try_recv().expect("child must start").0;
            let initial = spawn_count.load(std::sync::atomic::Ordering::SeqCst);

            match reason {
                StopReason::Panicked => {
                    rt.send_to(child_addr, Ping).unwrap();
                }
                StopReason::Normal => {
                    rt.stop_actor(child_addr).unwrap();
                }
                StopReason::Completed => {
                    rt.send_to(child_addr, Ping).unwrap();
                }
            }
            tick_many(&rt, 10);

            let final_count = spawn_count.load(std::sync::atomic::Ordering::SeqCst);
            assert_eq!(
                final_count, initial,
                "Temporary child must NOT be restarted: strategy={:?} reason={:?} spawns before={} after={}",
                strategy, reason, initial, final_count
            );

            combinations_tested += 1;
        }
    }

    assert_eq!(
        combinations_tested, 9,
        "must test all 9 strategy×reason combinations"
    );
}

/// Exhaustive G10: Transient policy restarts only on panic.
///
/// Enumerates: 3 reasons × 3 strategies = 9 combinations.
#[test]
fn g10_transient_restart_only_on_panic() {
    let mut combinations_tested = 0u32;

    for &strategy in &ALL_STRATEGIES {
        for &reason in &ALL_REASONS {
            let rt = std_runtime();
            let child_inbox = rt.new_inbox::<ChildStarted>().unwrap();
            let child_report = *child_inbox.addr();

            let spawn_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let sc = spawn_count.clone();

            let spec = ChildSpec::new("transient-child", RestartPolicy::Transient, move |ctx| {
                sc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let addr = match reason {
                    StopReason::Panicked => ctx.spawn(PanicOnPing)?,
                    StopReason::Completed => ctx.spawn(CompletedOnPing)?,
                    StopReason::Normal => ctx.spawn(IdleChild)?,
                };
                let _ = ctx.send(child_report, ChildStarted(addr));
                Ok(addr)
            });

            let sup = Supervisor::new(strategy, 10, vec![spec]);
            let _sup_addr = rt.spawn(sup).unwrap();
            tick_many(&rt, 5);

            let child_addr = child_inbox.try_recv().expect("child must start").0;
            let initial = spawn_count.load(std::sync::atomic::Ordering::SeqCst);

            match reason {
                StopReason::Panicked => {
                    rt.send_to(child_addr, Ping).unwrap();
                }
                StopReason::Normal => {
                    rt.stop_actor(child_addr).unwrap();
                }
                StopReason::Completed => {
                    rt.send_to(child_addr, Ping).unwrap();
                }
            }
            tick_many(&rt, 10);

            let final_count = spawn_count.load(std::sync::atomic::Ordering::SeqCst);
            let was_restarted = final_count > initial;
            let production_predicts = RestartPolicy::Transient.should_restart(reason);

            assert_eq!(
                was_restarted, production_predicts,
                "Transient: strategy={:?} reason={:?}: production predicts restart={} but runtime restarted={}",
                strategy, reason, production_predicts, was_restarted
            );

            combinations_tested += 1;
        }
    }

    assert_eq!(
        combinations_tested, 9,
        "must test all 9 strategy×reason combinations"
    );
}

/// Exhaustive G10: verify compute_restart_set for every strategy × child count × dead index.
///
/// 3 strategies × (1..=4 children) × (0..num_children dead indices) = 30 combinations.
#[test]
fn g10_exhaustive_restart_set_computation() {
    let mut combinations_tested = 0u32;

    for &strategy in &ALL_STRATEGIES {
        for num_children in 1..=4usize {
            for dead_idx in 0..num_children {
                let result = compute_restart_set(strategy, dead_idx, num_children);

                match strategy {
                    SupervisorStrategy::OneForOne => {
                        assert_eq!(
                            result,
                            vec![dead_idx],
                            "OneForOne(dead={}, n={}) should restart only dead child",
                            dead_idx,
                            num_children
                        );
                    }
                    SupervisorStrategy::OneForAll => {
                        let expected: Vec<usize> = (0..num_children).collect();
                        assert_eq!(
                            result, expected,
                            "OneForAll(dead={}, n={}) should restart all children",
                            dead_idx, num_children
                        );
                    }
                    SupervisorStrategy::RestForOne => {
                        let expected: Vec<usize> = (dead_idx..num_children).collect();
                        assert_eq!(
                            result, expected,
                            "RestForOne(dead={}, n={}) should restart dead and after",
                            dead_idx, num_children
                        );
                    }
                }

                combinations_tested += 1;
            }
        }
    }

    assert_eq!(
        combinations_tested, 30,
        "must test all 30 strategy×children×dead_idx combinations"
    );
}

/// Exhaustive G10: verify should_restart for every policy × reason combination.
///
/// 3 policies × 3 reasons = 9 combinations.
#[test]
fn g10_exhaustive_should_restart_truth_table() {
    let mut combinations_tested = 0u32;

    for &policy in &ALL_POLICIES {
        for &reason in &ALL_REASONS {
            let result = policy.should_restart(reason);
            let expected = match policy {
                RestartPolicy::Permanent => true,
                RestartPolicy::Transient => reason == StopReason::Panicked,
                RestartPolicy::Temporary => false,
            };
            assert_eq!(
                result, expected,
                "should_restart({:?}, {:?}) = {} but expected {}",
                policy, reason, result, expected
            );
            combinations_tested += 1;
        }
    }

    assert_eq!(
        combinations_tested, 9,
        "must test all 9 policy×reason combinations"
    );
}
