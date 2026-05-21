//! TESTING_SPEC §3 — Replay isomorphism.
//!
//! The recording must carry every piece of state needed to drive a
//! replay; the replay must reproduce the original bundle byte for
//! byte, even after the sim-only `sim/` subtree has been stripped;
//! and a real prod-origin bundle must replay through the same code
//! path.

#[path = "common.rs"]
mod common;

use common::{
    corpus_root, reference_scenario_text, run_engine, run_reference_scenario, sha256_tree,
};

// ── §3.1 — Self-replay ─────────────────────────────────────────────

#[test]
fn self_replay_identical() {
    // Run → record → replay → recording. The two bundles must be
    // byte-identical: any difference proves the original recording
    // dropped state SPEC §7.1 demands it carry.
    let primary = run_reference_scenario();
    let replayed = replay_bundle(&primary.root);
    assert_eq!(
        sha256_tree(&primary.root),
        sha256_tree(&replayed.root),
        "self-replay must produce a byte-identical bundle (SPEC §7.1)"
    );
}

// ── §3.2 — Mutation fidelity ──────────────────────────────────────

#[test]
fn mutations_preserved() {
    // The reference scenario declares partition / heal / restart
    // mutations at fixed `at_ms`. Each must appear in the bundle as
    // a `Custom` event on the recording stream SPEC §4.1/§5.3
    // designates, in declaration order, with virtual-time `wall_ms`
    // matching the spec exactly.
    let bundle = run_reference_scenario();
    let mutations = collect_mutation_events(&bundle.root);
    let expected: &[(&str, u64)] = &[
        ("partition", 20_000),
        ("heal", 35_000),
        ("restart", 45_000),
    ];
    assert_eq!(
        mutations.len(),
        expected.len(),
        "exactly {} mutation events expected, observed {} — {:?}",
        expected.len(),
        mutations.len(),
        mutations
    );
    for ((kind, at_ms), (obs_kind, obs_at)) in expected.iter().zip(mutations.iter()) {
        assert_eq!(obs_kind, kind, "mutation order/kind mismatch");
        assert_eq!(
            obs_at, at_ms,
            "mutation {kind} should fire at wall_ms={at_ms}, observed {obs_at}"
        );
    }
}

// ── §3.3 — Recording self-sufficiency under prod shape ────────────

#[test]
fn replay_from_prod_shape_only() {
    // Strip the `sim/` subtree (the sim-only fields) and replay
    // from what's left. The replay must succeed and produce a
    // bundle whose non-`sim/` files hash identically to the original.
    let primary = run_reference_scenario();
    let stripped = clone_without_sim(&primary.root);
    let replayed = replay_bundle(&stripped);

    let original_no_sim = sha256_tree_excluding_sim(&primary.root);
    let replayed_no_sim = sha256_tree_excluding_sim(&replayed.root);
    assert_eq!(
        original_no_sim, replayed_no_sim,
        "replay must reproduce non-sim/ files even when sim/ is absent (SPEC §7.1)"
    );
}

// ── §3.4 — Prod-bundle replay (corpus-anchored) ───────────────────

#[test]
fn vastai_n3_replays() {
    // The locked vastai-n3 corpus must replay through the same
    // loader. The replay output is not required to be byte-identical
    // to the corpus (calibration concern, out of scope per §15); it
    // must merely complete and produce a structurally valid bundle.
    let corpus = corpus_root();
    let replayed = replay_bundle(&corpus);

    // §7.3-style minimal structural check: every node in the corpus
    // has a matching directory in the replayed bundle, each
    // containing `boot.json`.
    for node in ["orchestrator", "stage-0", "stage-1", "stage-2"] {
        let node_dir = replayed.root.join(node);
        assert!(
            node_dir.is_dir(),
            "replayed bundle missing node directory: {}",
            node_dir.display()
        );
        assert!(
            node_dir.join("boot.json").is_file(),
            "replayed {node} missing boot.json"
        );
    }
}

// ── Helpers ────────────────────────────────────────────────────────

fn replay_bundle(bundle_path: &std::path::Path) -> simulation::Bundle {
    // Re-run the engine in replay mode by appending a `[replay]`
    // table pointing at `bundle_path` to the reference scenario text;
    // `run_to_tempdir` recognises the table and routes the spec
    // through `simulation::replay::load_replay_spec`.
    let spec_text = reference_scenario_text();
    let augmented = format!("{spec_text}\n[replay]\nbundle = {:?}\n", bundle_path.display());
    run_engine(&augmented, 42)
}

fn collect_mutation_events(bundle_root: &std::path::Path) -> Vec<(String, u64)> {
    // Mutations are written to the orchestrator's event stream as
    // `Custom { user_kind: "partition" | "heal" | "restart", ... }`.
    let orch_events = common::flatten_events(&bundle_root.join("orchestrator"));
    orch_events
        .into_iter()
        .filter_map(|rec| {
            let variant = rec.get("variant").and_then(|v| v.as_str())?;
            if variant != "Custom" {
                return None;
            }
            let kind = rec.get("user_kind").and_then(|v| v.as_str())?;
            if !matches!(kind, "partition" | "heal" | "restart") {
                return None;
            }
            let at = rec.get("wall_ms").and_then(|v| v.as_u64())?;
            Some((kind.to_string(), at))
        })
        .collect()
}

fn clone_without_sim(bundle_root: &std::path::Path) -> std::path::PathBuf {
    let dest = bundle_root.with_extension("stripped");
    std::fs::create_dir_all(&dest)
        .unwrap_or_else(|e| panic!("create_dir_all {}: {e}", dest.display()));
    common::walk_files(bundle_root, &mut |path| {
        let rel = path
            .strip_prefix(bundle_root)
            .expect("walk yields paths under bundle_root");
        if rel.starts_with("sim") {
            return;
        }
        let dst = dest.join(rel);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)
                .unwrap_or_else(|e| panic!("create_dir_all {}: {e}", parent.display()));
        }
        std::fs::copy(path, &dst)
            .unwrap_or_else(|e| panic!("copy {} → {}: {e}", path.display(), dst.display()));
    });
    dest
}

fn sha256_tree_excluding_sim(root: &std::path::Path) -> std::collections::BTreeMap<std::path::PathBuf, String> {
    sha256_tree(root)
        .into_iter()
        .filter(|(rel, _)| !rel.starts_with("sim"))
        .collect()
}
