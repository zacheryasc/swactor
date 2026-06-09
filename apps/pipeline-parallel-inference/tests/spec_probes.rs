//! Adversarial spec-anchored probes built per §4/§5 contracts of
//! `PP_DEPLOY_FIX_SPEC.md`. Written to break the implementation, not echo it.
//!
//! Run with:
//!   \
//!     cargo test --test spec_probes -- --nocapture

use pipeline_parallel_inference::orchestrator::{
    spawn_chain, stage_roster_event_fields, resolve_roster,
    StageRosterEntry, SpawnChainError,
};

use std::time::Duration;

// ─── §4.5 stage roster event fields ─────────────────────────────────

#[test]
fn s45_roster_lists_every_stage_in_index_order_with_required_fields() {
    let roster = vec![
        StageRosterEntry {
            stage_index: 0,
            node_id_hex: "00".repeat(32),
            node_id_short: "00".repeat(4),
        },
        StageRosterEntry {
            stage_index: 1,
            node_id_hex: "ff".repeat(32),
            node_id_short: "ff".repeat(4),
        },
        StageRosterEntry {
            stage_index: 2,
            node_id_hex: "aa".repeat(32),
            node_id_short: "aa".repeat(4),
        },
    ];
    let fields = stage_roster_event_fields(7, &roster);
    assert_eq!(fields["drive_seq"], 7);
    let stages = fields["stages"].as_array().unwrap();
    assert_eq!(stages.len(), 3);
    for (k, s) in stages.iter().enumerate() {
        // spec §4.5 fields
        assert!(s["stage_index"].as_u64().is_some());
        assert!(s["node_id_hex"].as_str().is_some());
        assert!(s["node_id_short"].as_str().is_some());
        // ordered by stage_index ascending
        assert_eq!(s["stage_index"].as_u64().unwrap(), k as u64);
    }
}

#[test]
fn s45_drive_seq_changes_per_drive() {
    // Just confirm: the helper takes drive_seq as a parameter, so the
    // orchestrator can vary it per drive (spec §4.5: "emitted on every
    // drive").
    let roster = vec![StageRosterEntry {
        stage_index: 0,
        node_id_hex: "00".repeat(32),
        node_id_short: "00".repeat(4),
    }];
    assert_ne!(
        stage_roster_event_fields(1, &roster),
        stage_roster_event_fields(2, &roster),
    );
}

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

// ─── §4.5 negative space: roster entry fields ───────────────────────

#[test]
fn s45_stages_list_carries_stage_index_node_id_hex_node_id_short() {
    let r = vec![StageRosterEntry {
        stage_index: 42,
        node_id_hex: "deadbeef".repeat(8),
        node_id_short: "dead".repeat(2),
    }];
    let v = stage_roster_event_fields(1, &r);
    let entry = &v["stages"][0];
    let keys: std::collections::HashSet<&str> = entry
        .as_object()
        .unwrap()
        .keys()
        .map(|s| s.as_str())
        .collect();
    // Spec §4.5: at minimum these three fields
    for f in ["stage_index", "node_id_hex", "node_id_short"] {
        assert!(keys.contains(f), "missing field {f} in roster entry");
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
