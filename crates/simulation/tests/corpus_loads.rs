//! Stage 2 corpus sanity check.
//!
//! The reference bundle under `tests/parity-bar/fixtures/vastai-n3-1/`
//! is the floor for §6 (schema coverage) and §7 (round-trip).
//! Stage 2's gate runs this test to confirm the fixture is present,
//! has the documented layout, and every JSON record is parseable
//! before any of the engine-side §6/§7 tests are even written.
//!
//! Once Phase 2 lands the real loader (Stage 7), this scenario test
//! is replaced by the loader exercising the bundle in anger. Until
//! then it exists to catch fixture corruption / bit-rot.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn corpus_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/parity-bar/fixtures/vastai-n3-1")
}

fn read_json(path: &Path) -> serde_json::Value {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("parse JSON {}: {e}", path.display()))
}

#[test]
fn manifest_lists_canonical_nodes() {
    let manifest = read_json(&corpus_root().join("MANIFEST.json"));
    let nodes: BTreeSet<&str> = manifest["nodes"]
        .as_array()
        .expect("manifest.nodes is an array")
        .iter()
        .map(|v| v.as_str().expect("node entries are strings"))
        .collect();
    let expected: BTreeSet<&str> =
        ["orchestrator", "stage-0", "stage-1", "stage-2"].into();
    assert_eq!(
        nodes, expected,
        "corpus must list the four canonical nodes (orchestrator + 3 stages)"
    );
}

#[test]
fn every_node_has_boot_snapshots_events_finalize() {
    let root = corpus_root();
    for node in ["orchestrator", "stage-0", "stage-1", "stage-2"] {
        let dir = root.join(node);
        assert!(dir.is_dir(), "node directory missing: {}", dir.display());
        assert!(
            dir.join("boot.json").is_file(),
            "missing boot.json under {}",
            dir.display()
        );
        assert!(
            dir.join("finalize.json").is_file(),
            "missing finalize.json under {}",
            dir.display()
        );
        let snapshots_dir = dir.join("snapshots");
        assert!(
            snapshots_dir.is_dir(),
            "missing snapshots dir under {}",
            dir.display()
        );
        let events_dir = dir.join("events");
        assert!(
            events_dir.is_dir(),
            "missing events dir under {}",
            dir.display()
        );

        let snapshot_count = std::fs::read_dir(&snapshots_dir)
            .expect("snapshots dir readable")
            .count();
        let event_count = std::fs::read_dir(&events_dir)
            .expect("events dir readable")
            .count();
        assert!(
            snapshot_count >= 1,
            "{node}: expected at least one snapshot"
        );
        assert!(event_count >= 1, "{node}: expected at least one event file");
    }
}

#[test]
fn every_json_file_is_parseable() {
    let root = corpus_root();
    let mut count = 0;
    walk(&root, &mut |path| {
        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            let _ = read_json(path);
            count += 1;
        }
    });
    assert!(count >= 12, "expected ≥12 JSON files in corpus, found {count}");
}

#[test]
fn every_event_record_carries_identity_fields() {
    // Every event record envelope must carry the identity fields the
    // §7 round-trip parser is going to expect: node_id, monotonic_seq,
    // and wall_ms (per OBSERVABILITY §3.3).
    let root = corpus_root();
    for node in ["orchestrator", "stage-0", "stage-1", "stage-2"] {
        let events_dir = root.join(node).join("events");
        for entry in std::fs::read_dir(&events_dir).expect("events dir readable") {
            let entry = entry.expect("dir entry");
            let val = read_json(&entry.path());
            let node_id = val["node_id_hex"]
                .as_str()
                .expect("events file has node_id_hex");
            assert!(
                !node_id.is_empty(),
                "{node}: event file has empty node_id_hex"
            );
            let records = val["records"].as_array().expect("records is an array");
            for rec in records {
                assert!(
                    rec["variant"].is_string(),
                    "{node}: every record has a string `variant`"
                );
                assert!(
                    rec["monotonic_seq"].is_u64(),
                    "{node}: every record carries monotonic_seq"
                );
                assert!(
                    rec["wall_ms"].is_number(),
                    "{node}: every record carries wall_ms"
                );
            }
        }
    }
}

#[test]
fn provenance_exists() {
    let p = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/parity-bar/fixtures/PROVENANCE.md");
    assert!(p.is_file(), "PROVENANCE.md missing: {}", p.display());
}

#[test]
fn reference_scenario_describes_three_stages_and_60s_horizon() {
    let scenario_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/parity-bar/fixtures/scenarios/reference.toml");
    let text = std::fs::read_to_string(&scenario_path)
        .expect("reference.toml readable");
    let parsed: toml::Value =
        toml::from_str(&text).expect("reference.toml parses as TOML");
    assert_eq!(
        parsed["duration_ms"].as_integer(),
        Some(60_000),
        "reference scenario is 60s per TESTING_SPEC §6.5"
    );
    let hosts = parsed["hosts"].as_array().expect("hosts array");
    let stages: Vec<_> = hosts
        .iter()
        .filter(|h| h["role"].as_str() == Some("stage"))
        .collect();
    assert_eq!(
        stages.len(),
        3,
        "reference scenario must have 3 stages (matches N=3 corpus)"
    );
}

fn walk(root: &Path, f: &mut dyn FnMut(&Path)) {
    let entries = match std::fs::read_dir(root) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, f);
        } else {
            f(&path);
        }
    }
}
