//! TESTING_SPEC §8 — Causality and time invariants.
//!
//! All five checks read a bundle the reference scenario produces and
//! assert structural invariants over the record stream: monotonic
//! sequence numbers, non-decreasing wall_ms modulo declared clock
//! jumps, send-precedes-receive matching, snapshot-id uniqueness,
//! and deterministic per-tick executor ordering.

#[path = "common.rs"]
mod common;

use common::{bundle_node_dirs, flatten_events, load_record_files, run_reference_scenario};
use std::collections::{BTreeMap, BTreeSet};

// ── §8.1 — monotonic_seq strictly increasing ───────────────────────

#[test]
fn monotonic_seq_strictly_increasing() {
    let bundle = run_reference_scenario();
    for node_dir in bundle_node_dirs(&bundle.root) {
        let mut per_boot: BTreeMap<i64, Vec<u64>> = BTreeMap::new();
        for rec in records_with_envelope_metadata(&node_dir) {
            per_boot.entry(rec.boot_sequence).or_default().push(rec.monotonic_seq);
        }
        for (boot_seq, mut seq) in per_boot {
            seq.sort();
            let mut prev: Option<u64> = None;
            for s in &seq {
                if let Some(p) = prev {
                    assert!(
                        *s > p,
                        "{}: monotonic_seq not strictly increasing under \
                         boot_sequence {boot_seq} (saw {p} then {s})",
                        node_dir.display()
                    );
                }
                prev = Some(*s);
            }
        }
    }
}

// ── §8.2 — wall_ms non-decreasing modulo clock jumps ───────────────

#[test]
fn wall_ms_non_decreasing() {
    let bundle = run_reference_scenario();
    for node_dir in bundle_node_dirs(&bundle.root) {
        let events = flatten_events(&node_dir);
        let mut prev: Option<i64> = None;
        for rec in events {
            let wall = rec
                .get("wall_ms")
                .and_then(|v| v.as_i64())
                .expect("every record carries wall_ms");
            let is_clock_jump = rec.get("variant").and_then(|v| v.as_str()) == Some("Custom")
                && rec.get("user_kind").and_then(|v| v.as_str()) == Some("clock_jump");
            if let Some(p) = prev {
                if !is_clock_jump {
                    assert!(
                        wall >= p,
                        "{}: wall_ms regressed from {p} to {wall} without clock_jump",
                        node_dir.display()
                    );
                }
            }
            prev = Some(wall);
        }
    }
}

// ── §8.3 — send precedes receive ───────────────────────────────────

#[test]
fn send_precedes_receive() {
    let bundle = run_reference_scenario();
    // Gather every MessageSent across the whole bundle keyed by
    // (sender, peer, kind, size). For every MessageReceived assert
    // that some send earlier in virtual time matches.
    let mut sends: Vec<MessageEvent> = Vec::new();
    let mut receives: Vec<MessageEvent> = Vec::new();
    for node_dir in bundle_node_dirs(&bundle.root) {
        let node_name = node_dir
            .file_name()
            .and_then(|s| s.to_str())
            .expect("node dir name")
            .to_string();
        for rec in flatten_events(&node_dir) {
            let Some(variant) = rec.get("variant").and_then(|v| v.as_str()) else {
                continue;
            };
            if variant != "MessageSent" && variant != "MessageReceived" {
                continue;
            }
            let event = MessageEvent {
                node: node_name.clone(),
                peer: rec
                    .get("peer")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .unwrap_or_default(),
                kind: rec
                    .get("kind")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .unwrap_or_default(),
                size: rec.get("size").and_then(|v| v.as_u64()).unwrap_or(0),
                wall_ms: rec.get("wall_ms").and_then(|v| v.as_i64()).unwrap_or(0),
            };
            if variant == "MessageSent" {
                sends.push(event);
            } else {
                receives.push(event);
            }
        }
    }

    for r in &receives {
        let matched = sends.iter().any(|s| {
            s.peer == r.node
                && s.kind == r.kind
                && s.size == r.size
                && s.wall_ms <= r.wall_ms
        });
        assert!(
            matched,
            "MessageReceived on {} from {} (kind={}, size={}, wall_ms={}) \
             has no causal MessageSent within the bundle",
            r.node, r.peer, r.kind, r.size, r.wall_ms
        );
    }
}

// ── §8.4 — snapshot_id uniqueness ──────────────────────────────────

#[test]
fn snapshot_id_unique() {
    let bundle = run_reference_scenario();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for node_dir in bundle_node_dirs(&bundle.root) {
        for snap in load_record_files(&node_dir, "snapshots") {
            let id = snap
                .get("snapshot_id")
                .and_then(|v| v.as_str())
                .expect("snapshot.snapshot_id is a string")
                .to_string();
            assert!(
                seen.insert(id.clone()),
                "duplicate snapshot_id={id} (across the bundle)"
            );
        }
    }
}

// ── §8.5 — executor tiebreaker determinism ─────────────────────────

#[test]
fn executor_tiebreaker_deterministic() {
    // Construct a scenario in which multiple events fire at the
    // same virtual tick. The scenario is built inline so the test
    // is hermetic; the per-tick resolution order must be identical
    // across two runs of the same scenario.
    let spec = tiebreaker_scenario_toml();
    let b1 = common::run_engine(&spec, 7);
    let b2 = common::run_engine(&spec, 7);

    // Compare the orchestrator's event stream tick-by-tick.
    let orch1 = flatten_events(&b1.root.join("orchestrator"));
    let orch2 = flatten_events(&b2.root.join("orchestrator"));
    assert_eq!(
        orch1.len(),
        orch2.len(),
        "scenario produced different event counts across two runs"
    );
    for (i, (a, b)) in orch1.iter().zip(orch2.iter()).enumerate() {
        assert_eq!(
            a, b,
            "event #{i} diverged across two runs of the same (spec, seed)"
        );
    }
}

// ── Helpers ────────────────────────────────────────────────────────

struct RecordMeta {
    boot_sequence: i64,
    monotonic_seq: u64,
}

#[derive(Debug)]
struct MessageEvent {
    node: String,
    peer: String,
    kind: String,
    size: u64,
    wall_ms: i64,
}

fn records_with_envelope_metadata(node_dir: &std::path::Path) -> Vec<RecordMeta> {
    let mut out = Vec::new();
    for envelope in load_record_files(node_dir, "events") {
        let boot_seq = envelope
            .get("boot_sequence")
            .and_then(|v| v.as_i64())
            .expect("event envelope carries boot_sequence");
        for rec in envelope
            .get("records")
            .and_then(|r| r.as_array())
            .into_iter()
            .flatten()
        {
            let mono = rec
                .get("monotonic_seq")
                .and_then(|v| v.as_u64())
                .expect("record carries monotonic_seq");
            out.push(RecordMeta {
                boot_sequence: boot_seq,
                monotonic_seq: mono,
            });
        }
    }
    for envelope in load_record_files(node_dir, "snapshots") {
        let boot_seq = envelope
            .get("boot_sequence")
            .and_then(|v| v.as_i64())
            .expect("snapshot carries boot_sequence");
        let mono = envelope
            .get("monotonic_seq")
            .and_then(|v| v.as_u64())
            .expect("snapshot carries monotonic_seq");
        out.push(RecordMeta {
            boot_sequence: boot_seq,
            monotonic_seq: mono,
        });
    }
    out
}

fn tiebreaker_scenario_toml() -> String {
    // Three events fire at virtual tick `at_ms=1000` on the same
    // node — partition + heal + restart, all at the same tick.
    // Per SPEC §2.3 the executor resolves them by
    // `(node_id, fiber_id, event_seq)`; the test asserts the same
    // resolution order across two runs.
    r#"
run_id = "tiebreaker-001"
seed = 7
duration_ms = 2000

[[hosts]]
name = "alpha"
role = "stage"
stage_index = 0
start_at_ms = 0
stop_at_ms = 2000

[[hosts]]
name = "beta"
role = "stage"
stage_index = 1
start_at_ms = 0
stop_at_ms = 2000

[[mutations]]
kind = "partition"
at_ms = 1000
edges = [["alpha", "beta"]]

[[mutations]]
kind = "heal"
at_ms = 1000
edges = [["alpha", "beta"]]

[[mutations]]
kind = "restart"
at_ms = 1000
node = "alpha"
"#
    .to_string()
}
