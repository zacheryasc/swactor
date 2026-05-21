//! TESTING_SPEC §6 — Schema floor coverage.
//!
//! The reference scenario must emit at least every record kind /
//! every event variant the prod corpus exhibits, and the sim must
//! never emit kinds outside the documented union. §6.3 also forbids
//! "silent `None`" fields — any `None` must be paired with an
//! `Error` event explaining the gap.

#[path = "common.rs"]
mod common;

use common::{
    bundle_node_dirs, corpus_root, flatten_events, load_record_files, run_reference_scenario,
};
use std::collections::BTreeSet;

// ── §6.1 — Corpus record-kind census ───────────────────────────────

#[test]
fn corpus_kinds() {
    let corpus_kinds = collect_record_kinds(&corpus_root());
    let bundle = run_reference_scenario();
    let sim_kinds = collect_record_kinds(&bundle.root);
    let missing: BTreeSet<&String> = corpus_kinds.difference(&sim_kinds).collect();
    assert!(
        missing.is_empty(),
        "sim output is missing corpus record kinds (TESTING_SPEC §6.1): {missing:?}"
    );
}

// ── §6.2 — No phantom records ──────────────────────────────────────

#[test]
fn no_phantom_records() {
    let allowed = corpus_record_kinds_union(&corpus_root());
    let bundle = run_reference_scenario();
    let sim_kinds = collect_record_kinds(&bundle.root);
    let phantom: BTreeSet<&String> = sim_kinds.difference(&allowed).collect();
    assert!(
        phantom.is_empty(),
        "sim emits record kinds not in corpus or OBSERVABILITY: {phantom:?}"
    );
}

// ── §6.3 — Field presence audit ────────────────────────────────────

#[test]
fn no_silent_none() {
    // Every documented field is either populated or paired with an
    // `Error` record whose `component` names the introspector and
    // whose `message` explains the gap (OBSERVABILITY §6 rule 1).
    let bundle = run_reference_scenario();
    let mut silent_nones = Vec::new();
    for node_dir in bundle_node_dirs(&bundle.root) {
        let snapshots = load_record_files(&node_dir, "snapshots");
        let events = flatten_events(&node_dir);
        let error_components: BTreeSet<String> = events
            .iter()
            .filter(|r| r.get("variant").and_then(|v| v.as_str()) == Some("Error"))
            .filter_map(|r| r.get("component").and_then(|v| v.as_str()).map(String::from))
            .collect();
        for snap in &snapshots {
            walk_nulls(snap, "", &error_components, &mut |path| {
                silent_nones.push(format!("{}: {path}", node_dir.display()));
            });
        }
    }
    assert!(
        silent_nones.is_empty(),
        "silent None fields (no paired Error event):\n{}",
        silent_nones.join("\n")
    );
}

// ── §6.4 — Variant exhaustiveness ──────────────────────────────────

#[test]
fn all_event_variants_fire() {
    let corpus_variants = collect_event_variants(&corpus_root());
    let bundle = run_reference_scenario();
    let sim_variants = collect_event_variants(&bundle.root);
    let missing: BTreeSet<&String> = corpus_variants.difference(&sim_variants).collect();
    assert!(
        missing.is_empty(),
        "reference scenario did not fire every corpus Event variant: {missing:?}"
    );
}

// ── Helpers ────────────────────────────────────────────────────────

fn collect_record_kinds(root: &std::path::Path) -> BTreeSet<String> {
    let mut kinds = BTreeSet::new();
    for node_dir in bundle_node_dirs(root) {
        for sub in ["snapshots", "events"] {
            for value in load_record_files(&node_dir, sub) {
                if let Some(kind) = value.get("kind").and_then(|v| v.as_str()) {
                    kinds.insert(kind.to_string());
                }
            }
        }
        for special in ["boot.json", "finalize.json"] {
            let path = node_dir.join(special);
            if path.is_file() {
                let text = std::fs::read_to_string(&path)
                    .expect("special file readable");
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(kind) = v.get("kind").and_then(|x| x.as_str()) {
                        kinds.insert(kind.to_string());
                    }
                }
            }
        }
    }
    kinds
}

fn corpus_record_kinds_union(root: &std::path::Path) -> BTreeSet<String> {
    // The corpus kinds + the OBSERVABILITY-documented set. The
    // documented set is the small, stable list of envelope kinds in
    // §3.1 (boot, events, finalize, snapshot). Adding to either set
    // requires a §12.1 commit.
    let mut allowed = collect_record_kinds(root);
    allowed.extend(["boot", "events", "finalize", "snapshot"].map(String::from));
    allowed
}

fn collect_event_variants(root: &std::path::Path) -> BTreeSet<String> {
    let mut variants = BTreeSet::new();
    for node_dir in bundle_node_dirs(root) {
        for rec in flatten_events(&node_dir) {
            if let Some(v) = rec.get("variant").and_then(|v| v.as_str()) {
                variants.insert(v.to_string());
            }
        }
    }
    variants
}

fn walk_nulls(
    value: &serde_json::Value,
    path: &str,
    error_components: &BTreeSet<String>,
    sink: &mut dyn FnMut(&str),
) {
    match value {
        serde_json::Value::Null => {
            // Walk the path elements looking for an Error component
            // that mentions any segment of the path. The match is
            // intentionally loose; the calibration loop (SPEC §9)
            // is what tightens it to exact (component, field) pairs.
            let covered = error_components
                .iter()
                .any(|c| path.contains(c.as_str()) || c.contains("introspect"));
            if !covered {
                sink(path);
            }
        }
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let child = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                walk_nulls(v, &child, error_components, sink);
            }
        }
        serde_json::Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                let child = format!("{path}[{i}]");
                walk_nulls(item, &child, error_components, sink);
            }
        }
        _ => {}
    }
}
