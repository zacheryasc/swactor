//! TESTING_SPEC §5 — Same-binary invariant.
//!
//! The sim under load-bearing transports must be the production
//! transports linked against the sim facade. Replacement / forked
//! transports defeat the purpose of the sim. The three §5 checks
//! cover this from three angles: resolved-dep equality (§5.1),
//! absence of `sim_*` source forks (§5.2), and symbol-set overlap
//! across the two binaries (§5.3).

#[path = "common.rs"]
mod common;

use common::workspace_root;
use std::collections::BTreeSet;
use std::process::Command;

// ── §5.1 — Shared load-bearing dependencies ────────────────────────

#[test]
fn shared_load_bearing() {
    // `cargo metadata` for the prod binary and the sim driver must
    // list the load-bearing transport crates at *identical resolved
    // versions*. The list is closed in TESTING_SPEC §5.1; adding to
    // it requires a §12.1 commit.
    const LOAD_BEARING: &[&str] = &[
        "iroh",
        "iroh-relay",
        "quinn",
        "quinn-proto",
        "swactor",
        "distribution",
        "postcard",
    ];

    let metadata = run_cargo_metadata();
    let mut versions: std::collections::BTreeMap<&str, BTreeSet<String>> =
        std::collections::BTreeMap::new();
    for pkg in metadata["packages"].as_array().expect("metadata.packages is an array") {
        let name = match pkg["name"].as_str() {
            Some(s) => s,
            None => continue,
        };
        if let Some(slot) = LOAD_BEARING.iter().find(|c| **c == name) {
            let version = pkg["version"]
                .as_str()
                .expect("package version is a string")
                .to_string();
            versions.entry(slot).or_default().insert(version);
        }
    }

    // Each load-bearing crate must resolve to exactly one version
    // and must be present at all.
    let mut missing = Vec::new();
    let mut ambiguous = Vec::new();
    for crate_name in LOAD_BEARING {
        match versions.get(crate_name) {
            None => missing.push(*crate_name),
            Some(s) if s.len() != 1 => ambiguous.push((*crate_name, s.clone())),
            _ => {}
        }
    }
    assert!(
        missing.is_empty(),
        "missing load-bearing crates from workspace dep graph: {missing:?}"
    );
    assert!(
        ambiguous.is_empty(),
        "load-bearing crates resolved to multiple versions: {ambiguous:?}"
    );
}

// ── §5.2 — No sim-only forks of transport code ─────────────────────

#[test]
fn no_transport_forks() {
    // The sim has zero source files matching the §5.2 patterns.
    // We sweep the workspace tree (skipping `target/`) and assert
    // no candidate paths exist.
    let banned_components: &[&str] = &["sim_swim", "sim_iroh"];
    let banned_filenames: &[&str] = &[
        "swim_sim.rs",
        "iroh_sim.rs",
        "quinn_sim.rs",
    ];
    let mut hits: Vec<String> = Vec::new();
    common::walk_files(&workspace_root(), &mut |path| {
        let s = path.to_string_lossy().replace('\\', "/");
        if s.contains("/target/") || s.contains("/.git/") {
            return;
        }
        for c in banned_components {
            if s.contains(&format!("/{c}/")) {
                hits.push(s.to_string());
                return;
            }
        }
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if banned_filenames.contains(&name) {
                hits.push(s.to_string());
            }
        }
    });
    assert!(
        hits.is_empty(),
        "sim-only transport forks present (TESTING_SPEC §5.2):\n{}",
        hits.join("\n")
    );
}

// ── Helpers ────────────────────────────────────────────────────────

fn run_cargo_metadata() -> serde_json::Value {
    // §5.1 reads "must list … in both dependency graphs", which is
    // unambiguous about wanting the resolved graph rather than just
    // the workspace member list. Dropping `--no-deps` lets the
    // load-bearing crates (iroh, quinn, postcard, ...) appear via
    // transitive deps without forcing them into workspace.members.
    let output = Command::new(cargo_bin())
        .args(["metadata", "--format-version=1"])
        .current_dir(workspace_root())
        .output()
        .expect("cargo metadata must run");
    assert!(
        output.status.success(),
        "cargo metadata exited {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("cargo metadata produces JSON")
}

fn cargo_bin() -> String {
    option_env!("CARGO")
        .map(str::to_string)
        .unwrap_or_else(|| "cargo".to_string())
}
