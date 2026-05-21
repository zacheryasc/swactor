//! Shared helpers for the parity-bar tests.
//!
//! Every `t_*.rs` in this directory pulls this in with
//!     #[path = "common.rs"] mod common;
//!
//! Cargo only treats files registered as `[[test]]` entries in
//! `crates/simulation/Cargo.toml` as standalone test binaries; this
//! file is not registered, so it lives purely as a shared source
//! module pulled in by `#[path]`.
//!
//! The helpers here exist so individual tests can:
//!
//! * locate the locked reference scenario (TESTING_SPEC §6.5) and
//!   the locked corpus bundle (TESTING_SPEC §1.1) without each test
//!   re-deriving the path;
//!
//! * read every regular file under a bundle in a stable order and
//!   compute SHA-256 over its bytes (the load-bearing primitive of
//!   §2 and §3 — "every regular file has the same SHA-256");
//!
//! * recover gracefully when the engine returns
//!   [`simulation::SimError::NotImplemented`] — the engine surface
//!   keeps this sentinel as the documented "engine not wired in"
//!   error so `cargo xtask parity-bar --phase 1` can recognise
//!   pre-engine failures during a clean rebuild from scratch.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

// ── Paths ──────────────────────────────────────────────────────────

pub fn simulation_crate_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

pub fn workspace_root() -> PathBuf {
    let mut p = simulation_crate_root();
    p.pop(); // strip "simulation"
    p.pop(); // strip "crates"
    p
}

pub fn reference_scenario_path() -> PathBuf {
    simulation_crate_root().join("tests/parity-bar/fixtures/scenarios/reference.toml")
}

pub fn reference_scenario_text() -> String {
    let path = reference_scenario_path();
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read reference scenario {}: {e}", path.display()))
}

pub fn corpus_root() -> PathBuf {
    simulation_crate_root().join("tests/parity-bar/fixtures/vastai-n3-1")
}

// ── Phase-1 sentinel ───────────────────────────────────────────────
//
// The literal text below must match `simulation::SimError::NotImplemented`'s
// Display impl. xtask `--phase 1` greps cargo's failure output for it.

pub const PHASE1_SENTINEL: &str = "simulation engine not implemented yet";

/// Run the engine for `(spec_text, seed)`. Returns the bundle on
/// success. The `NotImplemented` sentinel is mapped onto a panic
/// that carries [`PHASE1_SENTINEL`] so a clean-build rerun with
/// `cargo xtask parity-bar --phase 1` can recognise pre-engine
/// failures; other engine errors panic with their `Display` form so
/// real failures surface clearly.
pub fn run_engine(spec_text: &str, seed: u64) -> simulation::Bundle {
    match simulation::run_to_tempdir(spec_text, seed) {
        Ok(b) => b,
        Err(simulation::SimError::NotImplemented) => panic!("{PHASE1_SENTINEL}"),
        Err(other) => panic!("simulation engine returned error: {other}"),
    }
}

/// Run the reference scenario at the locked seed. Most §6/§7/§8/§11
/// tests share this entry point so they all exercise the same
/// recording for cross-check.
pub fn run_reference_scenario() -> simulation::Bundle {
    run_engine(&reference_scenario_text(), 42)
}

// ── Hashing / file walking ─────────────────────────────────────────

/// SHA-256 of a single file, lower-hex.
pub fn sha256_file(path: &Path) -> String {
    use sha2::{Digest, Sha256};
    let bytes = std::fs::read(path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let mut h = Sha256::new();
    h.update(&bytes);
    format!("{:x}", h.finalize())
}

/// Map of `relative path → SHA-256` for every regular file under
/// `root`. `BTreeMap` iteration is sorted so callers can compare with
/// `==` to assert directory-tree equality.
pub fn sha256_tree(root: &Path) -> BTreeMap<PathBuf, String> {
    let mut out = BTreeMap::new();
    walk_files(root, &mut |path| {
        let rel = path
            .strip_prefix(root)
            .expect("walk_files yields paths under root")
            .to_path_buf();
        out.insert(rel, sha256_file(path));
    });
    out
}

/// Visit every regular file under `root`, sorted by directory entry
/// name at each level for determinism.
pub fn walk_files(root: &Path, f: &mut dyn FnMut(&Path)) {
    let entries = match std::fs::read_dir(root) {
        Ok(e) => e,
        Err(_) => return,
    };
    let mut entries: Vec<_> = entries.flatten().collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            walk_files(&path, f);
        } else if path.is_file() {
            f(&path);
        }
    }
}

// ── Bundle access helpers ──────────────────────────────────────────

/// List every node directory at the top level of a bundle, sorted.
pub fn bundle_node_dirs(bundle_root: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let entries = std::fs::read_dir(bundle_root)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", bundle_root.display()));
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() && path.file_name().and_then(|s| s.to_str()) != Some("sim") {
            dirs.push(path);
        }
    }
    dirs.sort();
    dirs
}

/// Parse every JSON file under `node_dir/sub` (sorted) into a flat
/// vector of records. Returns the parsed values in the order the
/// engine emitted them.
pub fn load_record_files(node_dir: &Path, sub: &str) -> Vec<serde_json::Value> {
    let dir = node_dir.join(sub);
    let mut files: Vec<_> = match std::fs::read_dir(&dir) {
        Ok(e) => e.flatten().collect(),
        Err(_) => return Vec::new(),
    };
    files.sort_by_key(|f| f.file_name());
    files
        .into_iter()
        .map(|entry| {
            let text = std::fs::read_to_string(entry.path())
                .unwrap_or_else(|e| panic!("read {}: {e}", entry.path().display()));
            serde_json::from_str::<serde_json::Value>(&text)
                .unwrap_or_else(|e| panic!("parse {}: {e}", entry.path().display()))
        })
        .collect()
}

/// Convenience: flatten every event record across `node_dir/events/*.json`
/// into a single Vec preserving emit order.
pub fn flatten_events(node_dir: &Path) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    for envelope in load_record_files(node_dir, "events") {
        if let Some(arr) = envelope.get("records").and_then(|v| v.as_array()) {
            out.extend(arr.iter().cloned());
        }
    }
    out
}
