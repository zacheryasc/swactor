//! Divergence detector (SPEC §2.5, TESTING_SPEC §2.5).
//!
//! Runs the engine twice with the same `(spec, seed)` — once clean,
//! once with the sim-facade poison flag engaged — and reports the
//! first regular file whose contents diverge. The report carries a
//! structured `Error` envelope identifying the divergence and an
//! exit code (non-zero) so a CLI driver can surface the disagreement
//! to a caller.
//!
//! The divergence detector exists so the parity-bar's §2.5 check can
//! prove that any byte-level engine perturbation is detectable. With
//! a clean engine and a clean poison flag the detector reports
//! `Identical` and the test asserts that path; with poison engaged
//! it reports `Diverged` and the test asserts the structured Error
//! is emitted and the exit code would be non-zero.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::sim_backend::poison::PoisonGuard;
use crate::{run_to_tempdir, SimError};

/// Outcome of a divergence check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Outcome {
    /// The two runs produced byte-identical bundles.
    Identical,
    /// At least one regular file's contents diverged.
    Diverged {
        first_divergent_path: String,
        clean_sha256: String,
        poisoned_sha256: String,
        error_event: ErrorEvent,
    },
}

/// Structured `Error` envelope per OBSERVABILITY §3.3 — the
/// divergence detector emits one of these naming the first divergent
/// record (file). The shape mirrors the schema recogniser's accepted
/// envelope fields so a downstream parser can read it without
/// changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEvent {
    pub schema_version: u32,
    pub variant: String,
    pub component: String,
    pub message: String,
    pub wall_ms: u64,
}

/// Final report. `exit_code` is what a CLI driver should propagate to
/// its OS exit (zero iff `Identical`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DivergenceReport {
    pub outcome: Outcome,
    pub exit_code: i32,
}

impl DivergenceReport {
    pub fn is_diverged(&self) -> bool {
        matches!(self.outcome, Outcome::Diverged { .. })
    }
}

/// Run the engine twice and report whether the second run produces
/// the same bundle. If `poison_second_run` is true the second run
/// runs under a `PoisonGuard` (the §2.5 test-only RNG perturbation);
/// otherwise the second run is byte-identical to the first and the
/// detector should report `Identical`. Returns an error only if a
/// run itself failed to start; a *detected* divergence is a
/// successful detector outcome.
pub fn check(spec_text: &str, seed: u64, poison_second_run: bool) -> Result<DivergenceReport, SimError> {
    let clean = run_to_tempdir(spec_text, seed)?;
    let clean_tree = sha256_tree(&clean.root);

    let second = if poison_second_run {
        let _guard = PoisonGuard::engage();
        run_to_tempdir(spec_text, seed)?
    } else {
        run_to_tempdir(spec_text, seed)?
    };
    let poisoned_tree = sha256_tree(&second.root);

    if clean_tree == poisoned_tree {
        return Ok(DivergenceReport {
            outcome: Outcome::Identical,
            exit_code: 0,
        });
    }

    let mut all_keys: Vec<PathBuf> = clean_tree.keys().cloned().collect();
    for k in poisoned_tree.keys() {
        if !all_keys.contains(k) {
            all_keys.push(k.clone());
        }
    }
    all_keys.sort();
    let (first_path, clean_hash, poisoned_hash) = all_keys
        .into_iter()
        .find_map(|path| {
            let c = clean_tree.get(&path).cloned().unwrap_or_default();
            let p = poisoned_tree.get(&path).cloned().unwrap_or_default();
            if c != p {
                Some((path, c, p))
            } else {
                None
            }
        })
        .expect("trees differ but no file differs — sha256_tree bug");

    let first_divergent_path = first_path.to_string_lossy().to_string();
    let error_event = ErrorEvent {
        schema_version: crate::engine::SCHEMA_VERSION,
        variant: "Error".into(),
        component: "divergence_detector".into(),
        message: format!(
            "byte-level divergence at {first_divergent_path} \
             (clean=sha256:{clean_hash}, poisoned=sha256:{poisoned_hash})"
        ),
        wall_ms: 0,
    };

    Ok(DivergenceReport {
        outcome: Outcome::Diverged {
            first_divergent_path,
            clean_sha256: clean_hash,
            poisoned_sha256: poisoned_hash,
            error_event,
        },
        exit_code: 2,
    })
}

fn sha256_tree(root: &std::path::Path) -> BTreeMap<PathBuf, String> {
    let mut out = BTreeMap::new();
    walk_files(root, &mut |path| {
        let rel = path
            .strip_prefix(root)
            .expect("walk yields paths under root")
            .to_path_buf();
        let bytes = match crate::sim_backend::bundle::read_text(path) {
            Ok(text) => text.into_bytes(),
            Err(_) => Vec::new(),
        };
        let mut h = Sha256::new();
        h.update(&bytes);
        out.insert(rel, format!("{:x}", h.finalize()));
    });
    out
}

fn walk_files(root: &std::path::Path, f: &mut dyn FnMut(&std::path::Path)) {
    // Route through `sim_backend::bundle` (the allowlisted facade
    // subtree) — the banned-API lint scopes the simulation crate.
    let subdirs = match crate::sim_backend::bundle::list_subdirs(root) {
        Ok(s) => s,
        Err(_) => return,
    };
    let files = match crate::sim_backend::bundle::list_files_with_prefix(root, "") {
        Ok(s) => s,
        Err(_) => Vec::new(),
    };
    let mut paths: Vec<PathBuf> = subdirs.into_iter().map(|s| root.join(s)).collect();
    paths.extend(files);
    paths.sort();
    paths.dedup();
    for path in paths {
        if path.is_dir() {
            walk_files(&path, f);
        } else if path.is_file() {
            f(&path);
        }
    }
}
