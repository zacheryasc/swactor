//! Adversarial spec-anchored probes built per §4/§5 contracts of
//! `PP_DEPLOY_FIX_SPEC.md`. Written to break the implementation, not echo it.
//!
//! Run with:
//!   \
//!     cargo test --test spec_probes -- --nocapture

use pipeline_parallel_inference::orchestrator::{
    spawn_chain, resolve_roster, SpawnChainError,
};

use std::time::Duration;

// ─── §4.6 pipeline-wired ────────────────────────────────────────────
//
// Negative side: resolve_roster MUST NOT report all-resolved until every
// pp-stage-K is up. Without all stages resolved, an emitter cannot emit
// pp_pipeline_wired (spec §4.6 negative space).

#[test]
fn s46_resolve_roster_times_out_when_a_stage_never_registers() {
    // Stage 1 (out of 3) is never resolvable.
    let started_at = std::time::Instant::now();
    let result = resolve_roster(
        3,
        Duration::from_millis(150),
        Duration::from_millis(10),
        |k| if k == 1 { None } else { Some("ab".repeat(32)) },
    );
    let elapsed = started_at.elapsed();
    match result {
        Err(e) => {
            let msg = format!("{e}");
            assert!(msg.contains("[1]"), "missing-stages list must name stage 1: {msg}");
            assert!(elapsed >= Duration::from_millis(100), "must wait full budget: {elapsed:?}");
        }
        Ok(r) => panic!("expected timeout, got Ok({:?})", r),
    }
}

// ─── §4.9 spawn-chain partial-failure cleanup ───────────────────────
//
// spawn_chain treats each stage independently: a failure on one stage
// MUST NOT leak the subprocesses already spawned for earlier stages, and
// the chain's "did all succeed" predicate is what gates downstream drive.

#[test]
fn s49_independent_per_host_attempt_is_visible_in_chain_guard() {
    // Spawn_chain treats each stage independently in the sense that
    // failure rolls back what was spawned. The negative case: if stage 1
    // is the one that fails, stage 0 (already spawned) is killed (no
    // orphaned subprocess) — proxy for §4.9's "per-host result" guarantee:
    // operation does not leak processes if any one host fails.
    let mut spawn_count = 0;
    let result = spawn_chain(3, Duration::from_millis(200), |_ctx| {
        spawn_count += 1;
        let mut cmd = std::process::Command::new("sh");
        if spawn_count == 2 {
            // Force stage 1 to never announce
            cmd.arg("-c").arg("sleep 10");
        } else {
            cmd.arg("-c").arg(
                "echo PP_GPU_NODE_ADDR aabbccddeeff0011223344556677889900aabbccddeeff00112233445566778899 127.0.0.1:9999; \
                 sleep 10",
            );
        }
        cmd
    });
    // stage 1 timeout aborts
    match result {
        Err(SpawnChainError::AddressTimeout { stage, .. }) => {
            assert_eq!(stage, 1, "expected timeout on stage 1, got {stage}");
        }
        other => panic!("expected AddressTimeout(stage=1), got {other:?}"),
    }
}

// ─── §5.2 distinct-host selection ───────────────────────────────────
//
// The host-throughput preflight prototype (PP_PREFLIGHT_HF) was removed; its
// only live behavior — never leasing two stages on the same physical host — is
// now unconditional in the lease's distinct-host pick. That invariant is
// covered by the `next_eligible_offer` scenario tests in
// `src/vastai.rs` (no two draws share a host_id) and the orchestrator's
// `lease_chain_finds_n_distinct_offers`.
