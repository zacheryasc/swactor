//! TESTING_SPEC §11 — Lifecycle observability.
//!
//! Each spec-declared lifecycle event (`start_at_ms`, `restart_at_ms`,
//! `crash_at_ms`, `stop_at_ms`) must produce its corresponding boot
//! / finalize / `Custom` records in the bundle. Living nodes at
//! end-of-run receive a synthetic clean shutdown so no node ends
//! without either an organic finalize, a crash mark, or an
//! `end_of_run` finalize.

#[path = "common.rs"]
mod common;

use common::{flatten_events, load_record_files, run_engine, run_reference_scenario};
use std::collections::BTreeMap;

// ── §11.1 — Boot record presence ───────────────────────────────────

#[test]
fn start_emits_boot() {
    let bundle = run_reference_scenario();
    for node in ["orchestrator", "stage-0", "stage-1", "stage-2", "collector"] {
        let boot = bundle.root.join(node).join("boot.json");
        assert!(
            boot.is_file(),
            "{node}: expected boot.json under {}",
            boot.display()
        );
        let parsed = read_json(&boot);
        assert_eq!(
            parsed["boot_sequence"].as_i64(),
            Some(0),
            "{node}: first boot must have boot_sequence=0"
        );
    }
}

// ── §11.2 — Restart sequence integrity ─────────────────────────────

#[test]
fn restart_increments_boot_sequence() {
    let spec = restart_scenario();
    let bundle = run_engine(&spec, 17);
    let node_dir = bundle.root.join("alpha");

    // Find every boot.json — `boot.json`, `boot-001.json`, etc.
    // The engine emits one per epoch; the test grabs every file
    // matching `boot*.json` and confirms boot_sequence values are
    // 0, 1, 2 in order.
    let mut boots: Vec<i64> = Vec::new();
    common::walk_files(&node_dir, &mut |p| {
        let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if name.starts_with("boot") && name.ends_with(".json") {
            let v = read_json(p);
            if let Some(seq) = v["boot_sequence"].as_i64() {
                boots.push(seq);
            }
        }
    });
    boots.sort();
    assert_eq!(
        boots,
        vec![0, 1, 2],
        "alpha should boot 3 times (seq=0,1,2) given start_at_ms=0, \
         restart_at_ms=[1000, 2000]; observed {boots:?}"
    );

    // Each epoch except the last must have a finalize.json (the
    // last epoch is still running at end-of-run — covered by §11.5).
    let mut finalizes: Vec<i64> = Vec::new();
    common::walk_files(&node_dir, &mut |p| {
        let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if name.starts_with("finalize") && name.ends_with(".json") {
            let v = read_json(p);
            if let Some(seq) = v["boot_sequence"].as_i64() {
                finalizes.push(seq);
            }
        }
    });
    finalizes.sort();
    assert!(
        finalizes.contains(&0) && finalizes.contains(&1),
        "alpha's first two epochs must have finalize.json; observed {finalizes:?}"
    );
}

// ── §11.3 — Crash distinguishability ───────────────────────────────

#[test]
fn crash_omits_finalize() {
    let spec = crash_scenario();
    let bundle = run_engine(&spec, 23);
    let node_dir = bundle.root.join("crasher");

    assert!(
        node_dir.join("boot.json").is_file(),
        "crasher must produce boot.json"
    );
    assert!(
        !node_dir.join("finalize.json").exists(),
        "crasher must NOT produce finalize.json (it crashed)"
    );

    // The engine must mark the crash with a sim-only `Custom` record
    // so the post-processor can distinguish crash from "incomplete
    // capture".
    let events = flatten_events(&node_dir);
    let crash_marked = events.iter().any(|r| {
        r.get("variant").and_then(|v| v.as_str()) == Some("Custom")
            && r.get("user_kind").and_then(|v| v.as_str()) == Some("crash")
    });
    assert!(
        crash_marked,
        "crashed node must emit a Custom(kind=\"crash\") record"
    );
}

// ── §11.4 — Clean shutdown ─────────────────────────────────────────

#[test]
fn stop_emits_clean_finalize() {
    let spec = stop_scenario();
    let bundle = run_engine(&spec, 29);
    let finalize = bundle.root.join("clean").join("finalize.json");
    assert!(
        finalize.is_file(),
        "clean-stop must produce finalize.json"
    );
    let value = read_json(&finalize);
    assert_eq!(
        value["shutdown_reason"].as_str(),
        Some("clean"),
        "clean-stop's shutdown_reason must be \"clean\""
    );
}

// ── §11.5 — Mid-run finalization ───────────────────────────────────

#[test]
fn end_of_run_finalizes_all_living() {
    // Every node that's still running when virtual time ends must
    // receive a synthetic `end_of_run` finalize.
    let spec = end_of_run_scenario();
    let bundle = run_engine(&spec, 31);

    let mut finalize_reasons: BTreeMap<String, String> = BTreeMap::new();
    let mut crash_marks: BTreeMap<String, bool> = BTreeMap::new();

    for entry in std::fs::read_dir(&bundle.root)
        .expect("bundle root readable")
        .flatten()
    {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(node) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if node == "sim" {
            continue;
        }
        let finalize = path.join("finalize.json");
        if finalize.is_file() {
            let reason = read_json(&finalize)["shutdown_reason"]
                .as_str()
                .expect("finalize.shutdown_reason is a string")
                .to_string();
            finalize_reasons.insert(node.to_string(), reason);
        }
        let events = flatten_events(&path);
        let crashed = events.iter().any(|r| {
            r.get("variant").and_then(|v| v.as_str()) == Some("Custom")
                && r.get("user_kind").and_then(|v| v.as_str()) == Some("crash")
        });
        crash_marks.insert(node.to_string(), crashed);
    }

    for node in finalize_reasons.keys().chain(crash_marks.keys()) {
        let has_organic = finalize_reasons.get(node) == Some(&"clean".to_string());
        let has_end_of_run =
            finalize_reasons.get(node) == Some(&"end_of_run".to_string());
        let crashed = crash_marks.get(node).copied().unwrap_or(false);
        assert!(
            has_organic || has_end_of_run || crashed,
            "node {node} ended the run with neither finalize nor crash mark"
        );
    }
}

// ── Helpers ────────────────────────────────────────────────────────

fn read_json(path: &std::path::Path) -> serde_json::Value {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("parse JSON {}: {e}", path.display()))
}

fn restart_scenario() -> String {
    r#"
run_id = "lifecycle-restart"
seed = 17
duration_ms = 3000

[[hosts]]
name = "alpha"
role = "stage"
stage_index = 0
start_at_ms = 0
restart_at_ms = [1000, 2000]
stop_at_ms = 3000
"#
    .to_string()
}

fn crash_scenario() -> String {
    r#"
run_id = "lifecycle-crash"
seed = 23
duration_ms = 3000

[[hosts]]
name = "crasher"
role = "stage"
stage_index = 0
start_at_ms = 0
crash_at_ms = 1500
"#
    .to_string()
}

fn stop_scenario() -> String {
    r#"
run_id = "lifecycle-stop"
seed = 29
duration_ms = 2000

[[hosts]]
name = "clean"
role = "stage"
stage_index = 0
start_at_ms = 0
stop_at_ms = 1500
"#
    .to_string()
}

fn end_of_run_scenario() -> String {
    r#"
run_id = "lifecycle-end-of-run"
seed = 31
duration_ms = 2000

[[hosts]]
name = "still_running"
role = "stage"
stage_index = 0
start_at_ms = 0
"#
    .to_string()
}
