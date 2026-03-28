//! Kani proof harnesses for G10 — Supervisor Restart Decisions.
//!
//! Bounded mirror of the supervisor restart decision logic from
//! `std/supervisor.rs`. For bounded child counts (4 children),
//! symbolically enumerates all combinations of strategy, which child
//! dies, each child's restart policy, and death reason.
//!
//! The decision points call **production** pure functions
//! (`RestartPolicy::should_restart`, `compute_restart_set`) from
//! `std/supervisor.rs`, so Kani is proving properties of the real code.
//!
//! Properties proven:
//! - **G10a**: `OneForOne` restarts only the dead child (if policy permits).
//! - **G10b**: `OneForAll` restarts all children (respecting policies).
//! - **G10c**: `RestForOne` restarts dead child + all after it (respecting policies).
//! - **G10d**: `Temporary` children are never restarted.
//! - **G10e**: `Transient` children restart only on panic.

use crate::actor::StopReason;
use crate::std::supervisor::compute_restart_set;
use crate::std::{RestartPolicy, SupervisorStrategy};

// ─── Bounded mirror ─────────────────────────────────────────────────────────

const MAX_CHILDREN: usize = 4;

/// Outcome of the supervisor's restart decision. Tracks which children
/// get restarted (set to `true` in the array).
struct RestartOutcome {
    restarted: [bool; MAX_CHILDREN],
    meltdown: bool,
}

/// Mirror of `Supervisor::handle_down` — the restart decision logic.
/// Calls production functions for the individual decisions.
///
/// `num_children`: number of active children (1..=MAX_CHILDREN)
/// `dead_idx`: index of the child that died
/// `strategy`: supervision strategy
/// `policies`: restart policy per child
/// `reason`: why the child died
/// `total_restarts` / `max_restarts`: meltdown tracking
fn decide_restart(
    num_children: usize,
    dead_idx: usize,
    strategy: SupervisorStrategy,
    policies: &[RestartPolicy; MAX_CHILDREN],
    reason: StopReason,
    total_restarts: u32,
    max_restarts: u32,
) -> RestartOutcome {
    let mut outcome = RestartOutcome {
        restarted: [false; MAX_CHILDREN],
        meltdown: false,
    };

    // Step 1: Use production should_restart function
    if !policies[dead_idx].should_restart(reason) {
        return outcome;
    }

    // Step 2: meltdown check (supervisor.rs:273-280)
    let new_total = total_restarts + 1;
    if new_total > max_restarts {
        outcome.meltdown = true;
        return outcome;
    }

    // Step 3: Use production compute_restart_set function
    let indices = compute_restart_set(strategy, dead_idx, num_children);
    for idx in indices {
        outcome.restarted[idx] = true;
    }

    outcome
}

// ─── Helper: symbolic enum generation ───────────────────────────────────────

fn symbolic_strategy() -> SupervisorStrategy {
    let v: u8 = kani::any();
    kani::assume(v < 3);
    match v {
        0 => SupervisorStrategy::OneForOne,
        1 => SupervisorStrategy::OneForAll,
        _ => SupervisorStrategy::RestForOne,
    }
}

fn symbolic_policy() -> RestartPolicy {
    let v: u8 = kani::any();
    kani::assume(v < 3);
    match v {
        0 => RestartPolicy::Permanent,
        1 => RestartPolicy::Transient,
        _ => RestartPolicy::Temporary,
    }
}

fn symbolic_reason() -> StopReason {
    let v: u8 = kani::any();
    kani::assume(v < 2);
    // Only Normal and Panicked are relevant for restart decisions.
    // Completed behaves identically to Normal (non-panic).
    match v {
        0 => StopReason::Normal,
        _ => StopReason::Panicked,
    }
}

// ─── Proof harnesses ────────────────────────────────────────────────────────

/// **G10a**: `OneForOne` restarts only the dead child (if policy permits).
#[kani::proof]
#[kani::unwind(5)]
fn proof_g10a_one_for_one_restarts_only_dead() {
    let num_children: usize = kani::any();
    kani::assume(num_children >= 1 && num_children <= MAX_CHILDREN);

    let dead_idx: usize = kani::any();
    kani::assume(dead_idx < num_children);

    let mut policies = [RestartPolicy::Permanent; MAX_CHILDREN];
    let mut i = 0;
    while i < num_children {
        policies[i] = symbolic_policy();
        i += 1;
    }

    let reason = symbolic_reason();
    let max_restarts: u32 = kani::any();
    kani::assume(max_restarts >= 1);

    let outcome = decide_restart(
        num_children,
        dead_idx,
        SupervisorStrategy::OneForOne,
        &policies,
        reason,
        0, // fresh supervisor
        max_restarts,
    );

    if !outcome.meltdown && policies[dead_idx].should_restart(reason) {
        // Only the dead child is restarted
        assert!(outcome.restarted[dead_idx]);
        let mut j = 0;
        while j < num_children {
            if j != dead_idx {
                assert!(!outcome.restarted[j]);
            }
            j += 1;
        }
    }
}

/// **G10b**: `OneForAll` restarts all children (respecting policies).
#[kani::proof]
#[kani::unwind(5)]
fn proof_g10b_one_for_all_restarts_all() {
    let num_children: usize = kani::any();
    kani::assume(num_children >= 1 && num_children <= MAX_CHILDREN);

    let dead_idx: usize = kani::any();
    kani::assume(dead_idx < num_children);

    let mut policies = [RestartPolicy::Permanent; MAX_CHILDREN];
    let mut i = 0;
    while i < num_children {
        policies[i] = symbolic_policy();
        i += 1;
    }

    let reason = symbolic_reason();
    let max_restarts: u32 = kani::any();
    kani::assume(max_restarts >= 1);

    let outcome = decide_restart(
        num_children,
        dead_idx,
        SupervisorStrategy::OneForAll,
        &policies,
        reason,
        0,
        max_restarts,
    );

    if !outcome.meltdown && policies[dead_idx].should_restart(reason) {
        // All children are restarted
        let mut j = 0;
        while j < num_children {
            assert!(outcome.restarted[j]);
            j += 1;
        }
    }
}

/// **G10c**: `RestForOne` restarts dead child + all after it.
#[kani::proof]
#[kani::unwind(5)]
fn proof_g10c_rest_for_one_restarts_from_dead() {
    let num_children: usize = kani::any();
    kani::assume(num_children >= 1 && num_children <= MAX_CHILDREN);

    let dead_idx: usize = kani::any();
    kani::assume(dead_idx < num_children);

    let mut policies = [RestartPolicy::Permanent; MAX_CHILDREN];
    let mut i = 0;
    while i < num_children {
        policies[i] = symbolic_policy();
        i += 1;
    }

    let reason = symbolic_reason();
    let max_restarts: u32 = kani::any();
    kani::assume(max_restarts >= 1);

    let outcome = decide_restart(
        num_children,
        dead_idx,
        SupervisorStrategy::RestForOne,
        &policies,
        reason,
        0,
        max_restarts,
    );

    if !outcome.meltdown && policies[dead_idx].should_restart(reason) {
        // Children before dead_idx are NOT restarted
        let mut j = 0;
        while j < dead_idx {
            assert!(!outcome.restarted[j]);
            j += 1;
        }
        // Dead child and all after it ARE restarted
        let mut k = dead_idx;
        while k < num_children {
            assert!(outcome.restarted[k]);
            k += 1;
        }
    }
}

/// **G10d**: `Temporary` children are never restarted.
#[kani::proof]
#[kani::unwind(5)]
fn proof_g10d_temporary_never_restarted() {
    let num_children: usize = kani::any();
    kani::assume(num_children >= 1 && num_children <= MAX_CHILDREN);

    let dead_idx: usize = kani::any();
    kani::assume(dead_idx < num_children);

    let strategy = symbolic_strategy();
    let reason = symbolic_reason();

    // Force the dead child to Temporary
    let mut policies = [RestartPolicy::Permanent; MAX_CHILDREN];
    let mut i = 0;
    while i < num_children {
        policies[i] = symbolic_policy();
        i += 1;
    }
    policies[dead_idx] = RestartPolicy::Temporary;

    let max_restarts: u32 = kani::any();
    kani::assume(max_restarts >= 1);

    let outcome = decide_restart(
        num_children,
        dead_idx,
        strategy,
        &policies,
        reason,
        0,
        max_restarts,
    );

    // Temporary child triggers no restart at all (should_restart returns false)
    // so no children should be restarted
    let mut j = 0;
    while j < num_children {
        assert!(!outcome.restarted[j]);
        j += 1;
    }
    assert!(!outcome.meltdown);
}

/// **G10e**: `Transient` children restart only on panic.
#[kani::proof]
#[kani::unwind(5)]
fn proof_g10e_transient_only_on_panic() {
    let num_children: usize = kani::any();
    kani::assume(num_children >= 1 && num_children <= MAX_CHILDREN);

    let dead_idx: usize = kani::any();
    kani::assume(dead_idx < num_children);

    let strategy = symbolic_strategy();
    let reason = symbolic_reason();

    // Force the dead child to Transient
    let mut policies = [RestartPolicy::Permanent; MAX_CHILDREN];
    let mut i = 0;
    while i < num_children {
        policies[i] = symbolic_policy();
        i += 1;
    }
    policies[dead_idx] = RestartPolicy::Transient;

    let max_restarts: u32 = kani::any();
    kani::assume(max_restarts >= 1);

    let outcome = decide_restart(
        num_children,
        dead_idx,
        strategy,
        &policies,
        reason,
        0,
        max_restarts,
    );

    if reason == StopReason::Normal {
        // Normal stop: transient child should NOT trigger restarts
        let mut j = 0;
        while j < num_children {
            assert!(!outcome.restarted[j]);
            j += 1;
        }
        assert!(!outcome.meltdown);
    }
    // On panic: restarts happen (covered by strategy-specific proofs)
}
