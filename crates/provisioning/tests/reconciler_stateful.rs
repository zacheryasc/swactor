//! Stateful property tests for the cluster reconciler over the kit's
//! reference `FakeBackend`. The harness, oracle, and generator live in
//! `common`; this file is a thin client pinning named guarantees.

pub mod common;

use std::time::{Duration, UNIX_EPOCH};

use common::{
    BootEvent, Harness, Input, RecordingExecutor, Reply, RunOrder, check_invariants, gen_trace,
    group, group_with_role, run_trace, sanitized, shape,
};
use provisioning::*;

// ── plain deterministic tests ─────────────────────────────────────────

#[test]
fn replayed_traces_are_identical() {
    for seed in 0..32 {
        let trace = gen_trace(seed, 48);
        let first = run_trace(seed, &trace, false, RunOrder::Fifo);
        let second = run_trace(seed, &trace, false, RunOrder::Fifo);
        assert_eq!(*first.state(), *second.state(), "seed {seed}");
        assert_eq!(first.backend.calls(), second.backend.calls(), "seed {seed}");
    }
}

#[test]
fn happy_path_converges() {
    let mut harness = Harness::new_with_backend(
        0,
        shape(1, vec![group("g0", 1)]),
        common::FakeBackend::default(),
    );
    harness.step(Input::Run); // dispatch create lease
    harness.step(Input::Run); // execute create, dispatch bootstrap start
    harness.step(Input::Run); // execute bootstrap start, session active
    harness.step(Input::Boot(BootEvent::Joined));
    harness.step(Input::Boot(BootEvent::Closed));
    harness.step(Input::Run); // bootstrap convergence accepted
    assert!(harness.driver.is_converged());
    assert!(
        harness
            .backend
            .calls()
            .iter()
            .any(|effect| { matches!(effect.command, NodeManagerCommand::CreateLease(_)) })
    );
}

#[test]
fn ambiguous_create_is_adopted_and_converges() {
    let mut harness = Harness::new_with_backend(
        0,
        shape(1, vec![group("g0", 1)]),
        common::FakeBackend::default(),
    );
    harness.step(Input::Reply(Reply::Ambiguous("create timed out")));
    harness.step(Input::Run); // create fails ambiguously, backoff starts
    harness.step(Input::Tick(Duration::from_secs(10))); // retry/adopt
    harness.fair_tail();
    assert!(harness.driver.is_converged());
}

#[test]
fn shape_shrink_mid_lifecycle_converges() {
    let mut harness = Harness::new_with_backend(
        0,
        shape(1, vec![group("g0", 2)]),
        common::FakeBackend::default(),
    );
    harness.step(Input::Run);
    harness.step(Input::Run);
    harness.step(Input::Boot(BootEvent::Joined));
    // Node g0-1 may be mid-bootstrap when the shape shrinks to one node.
    harness.step(Input::Shape(shape(2, vec![group("g0", 1)])));
    harness.fair_tail();
    assert!(harness.driver.is_converged());
    assert_eq!(harness.state().nodes.len(), 1);
    assert!(
        harness
            .state()
            .nodes
            .contains_key(&LogicalNodeId("g0-0".to_owned()))
    );
}

#[test]
fn same_generation_same_content_is_accepted() {
    let mut driver = ClusterDriver::new(shape(1, vec![group("g0", 1)]), RetryPolicy::default())
        .expect("driver builds");
    let identical = driver.desired().clone();
    assert!(driver.update_desired(identical).is_ok());
    let mut changed = driver.desired().clone();
    changed.groups[0].count = 2;
    assert!(driver.update_desired(changed).is_err());
}

#[test]
fn deadline_expires_exactly_at_deadline() {
    let mut harness = Harness::new_with_backend(
        0,
        shape(1, vec![group("g0", 1)]),
        common::FakeBackend::default(),
    );
    harness.settle(); // create dispatched at the epoch
    let timeout = RetryPolicy::default().operation_timeout;
    let node = harness.state().nodes.values().next().expect("node exists");
    let pending = node.pending.as_ref().expect("create is pending");
    assert_eq!(pending.deadline, UNIX_EPOCH + timeout);

    // One tick before the deadline: still pending, nothing expired.
    harness.step(Input::Tick(timeout - Duration::from_secs(1)));
    let node = harness.state().nodes.values().next().expect("node exists");
    assert!(
        node.pending.is_some(),
        "operation expired before its deadline"
    );
    assert_eq!(node.retry.ambiguous_operation, None);

    // Exactly at the deadline: expired, classified ambiguous, never ran.
    harness.step(Input::Tick(Duration::from_secs(1)));
    let node = harness.state().nodes.values().next().expect("node exists");
    assert!(
        node.pending.is_none(),
        "operation did not expire at its deadline"
    );
    assert_eq!(
        node.retry.ambiguous_operation,
        Some(OperationKind::CreateLease)
    );
    assert!(
        harness.backend.calls().is_empty(),
        "expired operation must not reach the backend"
    );
}

#[test]
fn clock_extremes_do_not_panic_or_corrupt_state() {
    // Near the end of representable time the operation timeout saturates
    // (deadline collapses to `now`, i.e. immediately due) while retry
    // backoffs still fit; the machine must keep making progress without
    // panicking and without corrupting state.
    let mut now = UNIX_EPOCH + Duration::from_secs(i64::MAX as u64 - 100);
    let step = Duration::from_secs(2);
    let mut driver = ClusterDriver::new(shape(1, vec![group("g0", 1)]), RetryPolicy::default())
        .expect("driver builds");
    let mut executor = RecordingExecutor::default();
    let mut guard = 0;
    while executor.submitted < 8 {
        guard += 1;
        assert!(
            guard <= 64,
            "driver stopped making progress at clock extremes"
        );
        driver.trigger_if_due(now);
        driver
            .drive_until_blocked(now, &mut executor)
            .expect("drive near the end of time");
        for operation in driver.pending_operations_due(now) {
            assert!(driver.operation_timed_out(&operation, "extreme clock", now));
        }
        check_invariants(driver.state(), &driver.desired().expand().expect("expands"))
            .unwrap_or_else(|violation| panic!("invariant broken at clock extreme: {violation}"));
        now = now
            .checked_add(step)
            .expect("probe clock still representable");
        if let Some(requeue) = driver.requeue_at()
            && requeue > now
        {
            now = requeue
                .checked_add(step)
                .expect("requeue still representable");
        }
    }
    assert_eq!(executor.submitted, 8);
    let policy = RetryPolicy::default();
    assert_eq!(policy.delay_for_failure(u32::MAX), policy.max_delay);
}

#[test]
fn attempt_allocator_exhaustion_is_reported() {
    let observed = ClusterState {
        next_attempt_id: u64::MAX,
        ..ClusterState::default()
    };
    let error = reconcile(&observed, &shape(1, vec![group("g0", 1)]), UNIX_EPOCH)
        .expect_err("allocator must be exhausted");
    assert!(
        error.reason.contains("exhausted"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn latest_desired_wins() {
    for seed in 0..16 {
        let trace = sanitized(&gen_trace(seed, 32));
        let mut harness = Harness::new_with_backend(
            seed,
            shape(1, vec![group("g0", 1)]),
            common::FakeBackend::default(),
        );
        for input in &trace {
            harness.step(input.clone());
        }
        // A late shape change at a higher generation must win: the final
        // state converges to it, never to any earlier generation.
        let generation = harness.driver.desired().generation + 1;
        harness.step(Input::Shape(shape(
            generation,
            vec![group_with_role("g0", 2, "worker-late")],
        )));
        harness.fair_tail();
        assert!(harness.driver.is_converged(), "seed {seed}");
        assert_eq!(harness.driver.state().observed_generation, generation);
        assert_eq!(harness.state().nodes.len(), 2, "seed {seed}");
        for node in harness.state().nodes.values() {
            assert_eq!(
                node.record.desired.role,
                RoleId("worker-late".to_owned()),
                "seed {seed}"
            );
        }
    }
}

#[test]
fn run_order_confluence() {
    for seed in 0..16 {
        let trace = sanitized(&gen_trace(seed, 48));
        let fifo = run_trace(seed, &trace, true, RunOrder::Fifo);
        let lifo = run_trace(seed, &trace, true, RunOrder::Lifo);
        assert_eq!(*fifo.state(), *lifo.state(), "seed {seed}");
    }
}

// ── stateful property tests ───────────────────────────────────────────

const SEEDS: u64 = 256;
const TRACE_LEN: usize = 64;

#[test]
fn adversarial_traces_hold_invariants() {
    for seed in 0..SEEDS {
        let trace = gen_trace(seed, TRACE_LEN);
        common::assert_trace(common::FakeBackend::default, seed, &trace, false);
    }
}

#[test]
fn fair_traces_converge() {
    for seed in 0..SEEDS {
        let trace = gen_trace(seed, TRACE_LEN);
        common::assert_trace(common::FakeBackend::default, seed, &trace, true);
    }
}
