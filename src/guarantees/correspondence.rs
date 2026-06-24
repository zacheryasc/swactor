//! Exhaustive correspondence tests for core runtime lifecycle decisions.
//!
//! These tests enumerate finite input spaces for production decision functions and
//! compare them to observable runtime behavior. Pruned alpha std features are not
//! part of the beta guarantee set.

use crate::actor::{ActorAddress, ActorInterface, StopReason};
use crate::config::RuntimeConfig;
use crate::runtime::{Ctx, Runtime};
use crate::worker::{is_on_stop_eligible, should_skip_actor};

fn tick_many(rt: &Runtime, n: usize) {
    for _ in 0..n {
        rt.tick();
    }
}

#[derive(Clone, Debug)]
struct Ping;

#[derive(Clone, Debug)]
struct HandleCalled(#[allow(dead_code)] ActorAddress);

#[derive(Clone, Debug)]
struct OnStopCalled(#[allow(dead_code)] ActorAddress);

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

/// Exhaustive G4: for a healthy actor, the production `should_skip_actor`
/// predicts handle will be called — the real runtime agrees.
#[test]
fn g4_healthy_actor_handle_called() {
    for msg_count in 1..=5 {
        let rt = Runtime::new(RuntimeConfig::default());
        let h_inbox = rt.new_inbox::<HandleCalled>().unwrap();
        let report_addr = *h_inbox.addr();

        let addr = rt
            .spawn(DualReporter {
                handle_to: report_addr,
                stop_to: report_addr,
            })
            .unwrap();
        rt.tick();

        assert!(!should_skip_actor(false, false, false));

        for _ in 0..msg_count {
            rt.send_to(addr, Ping).unwrap();
        }
        tick_many(&rt, msg_count + 5);

        let handle_count = std::iter::from_fn(|| h_inbox.try_recv()).count();
        assert_eq!(handle_count, msg_count);
    }
}

/// Exhaustive G4: a poisoned actor must not have handle or on_stop called.
#[test]
fn g4_poisoned_actor_no_handle_no_on_stop() {
    for msg_count in 1..=5 {
        let rt = Runtime::new(RuntimeConfig::default());
        let h_inbox = rt.new_inbox::<HandleCalled>().unwrap();
        let s_inbox = rt.new_inbox::<OnStopCalled>().unwrap();

        assert!(should_skip_actor(true, false, false));
        assert!(!is_on_stop_eligible(false, true));

        let addr = rt
            .spawn(OnStartPanicker {
                handle_to: *h_inbox.addr(),
                stop_to: *s_inbox.addr(),
            })
            .unwrap();
        rt.tick();

        for _ in 0..msg_count {
            let _ = rt.send_to(addr, Ping);
        }
        tick_many(&rt, msg_count + 5);

        assert_eq!(std::iter::from_fn(|| h_inbox.try_recv()).count(), 0);
        assert_eq!(std::iter::from_fn(|| s_inbox.try_recv()).count(), 0);
    }
}

/// Exhaustive G4: a stopping actor skips later messages and calls on_stop once.
#[test]
fn g4_stopping_actor_no_handle_yes_on_stop() {
    for msg_count in 1..=5 {
        assert!(should_skip_actor(false, true, false));
        assert!(is_on_stop_eligible(true, false));

        let rt = Runtime::new(RuntimeConfig::default());
        let h_inbox = rt.new_inbox::<HandleCalled>().unwrap();
        let s_inbox = rt.new_inbox::<OnStopCalled>().unwrap();

        let addr = rt
            .spawn(DualReporter {
                handle_to: *h_inbox.addr(),
                stop_to: *s_inbox.addr(),
            })
            .unwrap();
        rt.tick();

        rt.stop_actor(addr).unwrap();
        for _ in 0..msg_count {
            let _ = rt.send_to(addr, Ping);
        }
        tick_many(&rt, 5);

        assert_eq!(std::iter::from_fn(|| h_inbox.try_recv()).count(), 0);
        assert_eq!(std::iter::from_fn(|| s_inbox.try_recv()).count(), 1);
    }
}

/// Exhaustive G4: a handle-panicked actor must not call on_stop.
#[test]
fn g4_handle_panic_poisons_no_on_stop() {
    let rt = Runtime::new(RuntimeConfig::default());
    let s_inbox = rt.new_inbox::<OnStopCalled>().unwrap();

    let addr = rt
        .spawn(HandlePanicker {
            stop_to: *s_inbox.addr(),
        })
        .unwrap();
    rt.tick();

    rt.send_to(addr, Ping).unwrap();
    tick_many(&rt, 5);

    assert!(!is_on_stop_eligible(false, true));
    assert_eq!(std::iter::from_fn(|| s_inbox.try_recv()).count(), 0);
}

/// Exhaustively enumerate core lifecycle decision function truth tables.
#[test]
fn g4_exhaustive_decision_function_truth_table() {
    use crate::worker::determine_stop_reason;

    let mut skip_combinations = 0;
    for poisoned in [false, true] {
        for stopping in [false, true] {
            for suspended in [false, true] {
                assert_eq!(
                    should_skip_actor(poisoned, stopping, suspended),
                    poisoned || stopping || suspended
                );
                skip_combinations += 1;
            }
        }
    }
    assert_eq!(skip_combinations, 8);

    let mut on_stop_combinations = 0;
    for stopping in [false, true] {
        for poisoned in [false, true] {
            assert_eq!(is_on_stop_eligible(stopping, poisoned), stopping && !poisoned);
            on_stop_combinations += 1;
        }
    }
    assert_eq!(on_stop_combinations, 4);

    let mut reason_combinations = 0;
    for poisoned in [false, true] {
        for has_exit_value in [false, true] {
            let expected = if poisoned {
                StopReason::Panicked
            } else if has_exit_value {
                StopReason::Completed
            } else {
                StopReason::Normal
            };
            assert_eq!(determine_stop_reason(poisoned, has_exit_value), expected);
            reason_combinations += 1;
        }
    }
    assert_eq!(reason_combinations, 4);
}
