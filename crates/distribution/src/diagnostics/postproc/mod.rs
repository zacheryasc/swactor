//! Post-processor for finalized diagnostics bundles
//! (`DIAGNOSTICS_PLAN.md` A.3).
//!
//! Given a `swactor-diag-collector`-produced tarball, build human-
//! readable views: a one-page Markdown summary, an N×N reachability
//! matrix as TSV, and per-peer-pair event timelines as TSV. Plus a
//! `diff` mode that highlights what changed between two bundles.
//!
//! The library API (used by the `swactor-diag-postproc` binary and by
//! tests) is:
//!
//! - [`Bundle::parse_path`] / [`Bundle::parse_bytes`] — load a tarball
//!   into a structured in-memory form.
//! - [`render_summary`] — produce `summary.md`.
//! - [`render_reachability_tsv`] — produce `reachability.tsv`.
//! - [`render_timeline_tsv`] — produce `timeline-{a}-to-{b}.tsv` for
//!   a single peer pair.
//! - [`render_diff`] — produce a textual diff of two bundles.
//! - [`Outputs::write_all_to`] — wraps the four renderers and writes
//!   the full output set into a directory.
//!
//! Anything that touches the on-disk tarball lives behind the
//! `collector` Cargo feature, which is also where the underlying
//! `tar` + `flate2` deps live.

mod parse;
mod render;

pub use parse::{Bundle, NodeData, ParseError, PostprocManifest, PostprocManifestNode};
pub use render::{
    PerPeerDialRollup, per_peer_dial_rollup, render_diff, render_reachability_tsv,
    render_summary, render_timeline_tsv,
};

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Convenience bundle of the four renderer outputs.
///
/// The binary writes these to disk; tests assert on them directly.
#[derive(Debug)]
pub struct Outputs {
    /// `summary.md`.
    pub summary_md: String,
    /// `reachability.tsv`.
    pub reachability_tsv: String,
    /// One `timeline-{a}-to-{b}.tsv` per ordered pair of node labels
    /// present in the bundle's manifest.
    pub timelines: Vec<(String, String)>,
}

impl Outputs {
    /// Compute all renderer outputs from a parsed bundle.
    pub fn from_bundle(bundle: &Bundle) -> Self {
        let summary_md = render_summary(bundle);
        let reachability_tsv = render_reachability_tsv(bundle);
        let labels: Vec<&str> = bundle.labels_in_order().collect();
        let mut timelines = Vec::new();
        for a in &labels {
            for b in &labels {
                if a == b {
                    continue;
                }
                let tsv = render_timeline_tsv(bundle, a, b);
                let name = format!("timeline-{a}-to-{b}.tsv");
                timelines.push((name, tsv));
            }
        }
        Self {
            summary_md,
            reachability_tsv,
            timelines,
        }
    }

    /// Write `summary.md`, `reachability.tsv`, and every
    /// `timeline-{a}-to-{b}.tsv` into `out_dir`. Creates the directory
    /// if it does not exist. Returns the list of paths written.
    pub fn write_all_to(&self, out_dir: &Path) -> io::Result<Vec<PathBuf>> {
        fs::create_dir_all(out_dir)?;
        let mut written = Vec::with_capacity(2 + self.timelines.len());
        let summary_path = out_dir.join("summary.md");
        fs::write(&summary_path, &self.summary_md)?;
        written.push(summary_path);
        let reach_path = out_dir.join("reachability.tsv");
        fs::write(&reach_path, &self.reachability_tsv)?;
        written.push(reach_path);
        // Deduplicate timeline filenames if labels happened to repeat
        // (the manifest disambiguates collisions, but defensively).
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for (name, body) in &self.timelines {
            if !seen.insert(name) {
                continue;
            }
            let p = out_dir.join(name);
            fs::write(&p, body)?;
            written.push(p);
        }
        Ok(written)
    }
}
