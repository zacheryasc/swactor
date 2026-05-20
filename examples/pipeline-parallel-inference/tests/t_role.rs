//! T-role: pure role-classification tests (TEST_SPEC §3).
//!
//! Surface: `StageRole::for_stage(stage, num_stages)`. The function is a
//! pure value-level classifier: no runtime, no env, no actor needed. These
//! tests are the cheapest possible guard against regressions in the
//! first/middle/last partition that the rest of the N-stage code relies on.
//!
//! The N range here (2..=16) is intentionally wider than the test matrix
//! we actually ship (N up to 5 today): if a future N raises `num_stages`,
//! it should not be the role-computation that breaks.

use pipeline_parallel_inference::stage_actor::StageRole;

/// Stage 0 is always `First`, for every supported `N`. The first stage's
/// identity does not depend on chain length.
#[test]
fn stage_zero_is_first_for_any_num_stages() {
    for n in 2..=8 {
        assert_eq!(
            StageRole::for_stage(0, n),
            StageRole::First,
            "stage 0 of {n} stages should be First",
        );
    }
}

/// The terminal stage (`stage == N - 1`) is always `Last`, for every
/// supported `N`. The last stage's identity does not depend on chain length.
#[test]
fn last_index_is_last_for_any_num_stages() {
    for n in 2..=8 {
        assert_eq!(
            StageRole::for_stage(n - 1, n),
            StageRole::Last,
            "stage {} of {n} stages should be Last",
            n - 1,
        );
    }
}

/// Every interior index in a chain of 3+ stages is `Middle`. This is the
/// "if it's not first and not last, it's middle" invariant the actor
/// dispatch table relies on.
#[test]
fn middle_indices_are_middle() {
    for n in 3..=8 {
        for i in 1..(n - 1) {
            assert_eq!(
                StageRole::for_stage(i, n),
                StageRole::Middle,
                "stage {i} of {n} stages should be Middle",
            );
        }
    }
}

/// A 2-stage chain has no middle stage at all: both valid inputs yield
/// `First` or `Last`. Nothing in the input space classifies as `Middle`
/// when `N == 2`.
#[test]
fn n_stages_two_has_no_middle() {
    assert_eq!(StageRole::for_stage(0, 2), StageRole::First);
    assert_eq!(StageRole::for_stage(1, 2), StageRole::Last);
    // Exhaustive over the valid input space at N=2 — no Middle appears.
    for stage in 0..2 {
        assert_ne!(
            StageRole::for_stage(stage, 2),
            StageRole::Middle,
            "N=2 stage {stage} must not be Middle",
        );
    }
}

/// For every `N ∈ {2..=16}`, exactly one stage is `First`, exactly one is
/// `Last`, and the remaining `N - 2` stages are `Middle`. Cross-checks the
/// three role predicates against each other as a partition.
#[test]
fn role_partition_property() {
    for n in 2..=16u32 {
        let mut first = 0u32;
        let mut middle = 0u32;
        let mut last = 0u32;
        for stage in 0..n {
            match StageRole::for_stage(stage, n) {
                StageRole::First => first += 1,
                StageRole::Middle => middle += 1,
                StageRole::Last => last += 1,
            }
        }
        assert_eq!(first, 1, "N={n}: expected exactly one First, got {first}");
        assert_eq!(last, 1, "N={n}: expected exactly one Last, got {last}");
        assert_eq!(
            middle,
            n - 2,
            "N={n}: expected {} Middle, got {middle}",
            n - 2,
        );
        assert_eq!(
            first + middle + last,
            n,
            "N={n}: role counts must partition the full stage set",
        );
    }
}
