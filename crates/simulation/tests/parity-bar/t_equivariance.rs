//! TESTING_SPEC §9 — Equivariance.
//!
//! Three rewrites of the reference scenario that should be
//! indistinguishable to the engine: node-id rename, host-listing
//! permutation, and same-tick mutation reordering. All three must
//! produce byte-identical bundles after applying the appropriate
//! rename map.

#[path = "common.rs"]
mod common;

use common::{reference_scenario_text, run_engine, sha256_tree};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Drop the `sim/` subtree from a hash tree before comparison.
/// SPEC §6.4: "The sim/ subtree is sim-only and is ignored by the
/// parity diff." The rename / ordering checks land below the same
/// exclusion — sim/spec.toml necessarily encodes the *renamed* host
/// names so comparing it under rename would force the engine to
/// rename strings it has no way to recognise.
fn drop_sim_subtree(
    tree: BTreeMap<PathBuf, String>,
) -> BTreeMap<PathBuf, String> {
    tree.into_iter()
        .filter(|(p, _)| !p.starts_with(Path::new("sim")))
        .collect()
}

// ── §9.1 — Node-id rename invariance ───────────────────────────────

#[test]
fn rename_invariance() {
    let base_spec = reference_scenario_text();
    let rename = BTreeMap::from([
        ("orchestrator", "x"),
        ("stage-0", "alpha"),
        ("stage-1", "beta"),
        ("stage-2", "gamma"),
        ("collector", "z"),
    ]);
    let renamed_spec = apply_text_rename(&base_spec, &rename);

    let b_base = run_engine(&base_spec, 42);
    let b_renamed = run_engine(&renamed_spec, 42);

    let base_tree = drop_sim_subtree(sha256_tree(&b_base.root));
    let renamed_tree = drop_sim_subtree(sha256_tree(&b_renamed.root));
    let base_aligned = apply_path_rename(base_tree, &rename);

    assert_eq!(
        base_aligned,
        renamed_tree,
        "rename-equivariance: bundles diverged after path rename"
    );
}

// ── §9.2 — Spec ordering invariance ────────────────────────────────

#[test]
fn spec_ordering_invariance() {
    let base = reference_scenario_text();
    let permuted = reverse_host_blocks(&base);
    assert_ne!(
        base.trim(),
        permuted.trim(),
        "test harness bug: permutation should change the spec text"
    );
    let b1 = run_engine(&base, 42);
    let b2 = run_engine(&permuted, 42);
    assert_eq!(
        drop_sim_subtree(sha256_tree(&b1.root)),
        drop_sim_subtree(sha256_tree(&b2.root)),
        "spec ordering must not affect the bundle"
    );
}

// ── §9.3 — Same-tick mutation reordering ───────────────────────────

#[test]
fn same_tick_mutation_reorder() {
    let order_a = same_tick_scenario(&[("partition", true), ("partition", false)]);
    let order_b = same_tick_scenario(&[("partition", false), ("partition", true)]);
    let b1 = run_engine(&order_a, 11);
    let b2 = run_engine(&order_b, 11);
    assert_eq!(
        drop_sim_subtree(sha256_tree(&b1.root)),
        drop_sim_subtree(sha256_tree(&b2.root)),
        "same-tick mutations declared in different orders must produce \
         byte-identical bundles"
    );
}

// ── Helpers ────────────────────────────────────────────────────────

fn apply_text_rename(text: &str, rename: &BTreeMap<&str, &str>) -> String {
    let mut out = text.to_string();
    // Use distinctive placeholders to avoid renaming "stage-0" inside
    // the result of a previous rename to "stage-00".
    for (i, (from, _)) in rename.iter().enumerate() {
        let placeholder = format!("__RENAME_{i}__");
        out = out.replace(from, &placeholder);
    }
    for (i, (_, to)) in rename.iter().enumerate() {
        let placeholder = format!("__RENAME_{i}__");
        out = out.replace(&placeholder, to);
    }
    out
}

fn apply_path_rename(
    tree: BTreeMap<PathBuf, String>,
    rename: &BTreeMap<&str, &str>,
) -> BTreeMap<PathBuf, String> {
    let mut out = BTreeMap::new();
    for (rel, hash) in tree {
        let s = rel.to_string_lossy().to_string();
        let mut renamed = s.clone();
        for (i, (from, _)) in rename.iter().enumerate() {
            let placeholder = format!("__RENAME_{i}__");
            renamed = renamed.replace(from, &placeholder);
        }
        for (i, (_, to)) in rename.iter().enumerate() {
            let placeholder = format!("__RENAME_{i}__");
            renamed = renamed.replace(&placeholder, to);
        }
        out.insert(PathBuf::from(renamed), hash);
    }
    out
}

fn reverse_host_blocks(text: &str) -> String {
    // Naively flip the order of `[[hosts]]` blocks in the TOML.
    // Other blocks (`[host_defaults]`, `[[links]]`, `[[mutations]]`,
    // top-level keys) are emitted unchanged.
    let mut lines: Vec<&str> = text.lines().collect();
    let mut blocks: Vec<(usize, usize)> = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        if lines[i].trim() == "[[hosts]]" {
            let start = i;
            let mut j = i + 1;
            while j < lines.len()
                && !lines[j].trim_start().starts_with('[')
            {
                j += 1;
            }
            blocks.push((start, j));
            i = j;
        } else {
            i += 1;
        }
    }
    // Reverse the host blocks in place.
    let mut block_contents: Vec<Vec<&str>> = blocks
        .iter()
        .map(|(s, e)| lines[*s..*e].to_vec())
        .collect();
    block_contents.reverse();
    let mut out_lines: Vec<String> = Vec::new();
    let mut block_iter = block_contents.into_iter();
    let mut consumed_until = 0;
    for (s, e) in &blocks {
        for raw in &lines[consumed_until..*s] {
            out_lines.push((*raw).to_string());
        }
        let block = block_iter.next().expect("blocks aligned");
        for raw in block {
            out_lines.push(raw.to_string());
        }
        consumed_until = *e;
    }
    for raw in &lines[consumed_until..] {
        out_lines.push((*raw).to_string());
    }
    out_lines.join("\n") + "\n"
}

fn same_tick_scenario(mutations: &[(&str, bool)]) -> String {
    let mut s = String::from(
        r#"
run_id = "equivariance-001"
seed = 11
duration_ms = 3000

[[hosts]]
name = "alpha"
role = "stage"
stage_index = 0
start_at_ms = 0
stop_at_ms = 3000

[[hosts]]
name = "beta"
role = "stage"
stage_index = 1
start_at_ms = 0
stop_at_ms = 3000

[[hosts]]
name = "gamma"
role = "stage"
stage_index = 2
start_at_ms = 0
stop_at_ms = 3000
"#,
    );
    for (kind, first_pair) in mutations {
        let (a, b) = if *first_pair {
            ("alpha", "beta")
        } else {
            ("beta", "gamma")
        };
        s.push_str(&format!(
            r#"
[[mutations]]
kind = "{kind}"
at_ms = 1500
edges = [["{a}", "{b}"]]
"#
        ));
    }
    s
}
