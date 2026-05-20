//! Persistence + sequencing state for a running collector.
//!
//! Owns the on-disk layout under `{root}/{run_id}/{node_id}/` and the
//! per-(run, node, kind) sequence counters used to make filenames
//! collision-free across concurrent posts.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use serde_json::Value;

use super::protocol::{Hints, RecordKind};

/// How long `/diag/finalize` waits between marking nodes for
/// snapshot_now and assembling the tarball, by default.
///
/// Per `DIAGNOSTICS_PLAN.md` T1.7: "Waits up to ~5s for stragglers."
pub const DEFAULT_FINALIZE_WAIT: Duration = Duration::from_secs(5);

/// Hard cap on a single POST body. Keeps a malformed or malicious
/// node from filling the collector's memory before we even see the
/// `node_id` header. 32 MiB is generous for an event batch or
/// snapshot but small enough that a single bad actor can't OOM us.
pub const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

/// Process-wide collector state. Cloneable as `Arc<CollectorState>`
/// so axum handlers can share it.
pub struct CollectorState {
    root: PathBuf,
    finalize_wait: Duration,
    /// Per-(run, node, kind) monotonic counter. Filenames are
    /// `{kind}-{seq}.json` with `seq` zero-padded to 6 digits so
    /// lexical sort matches numeric sort up to ~1M records.
    seqs: Mutex<HashMap<SeqKey, u64>>,
    /// Per-run accounting maintained as records arrive — drives the
    /// `MANIFEST.json` produced at finalize without re-scanning disk.
    runs: Mutex<HashMap<String, RunStats>>,
    /// Per-(run, node) hints queued to be returned on that node's
    /// next POST. Cleared on read so each hint fires once. T1.4
    /// pull-trigger.
    pending_hints: Mutex<HashMap<HintKey, Hints>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct HintKey {
    run_id: String,
    node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SeqKey {
    run_id: String,
    node_id: String,
    kind: RecordKind,
}

#[derive(Debug, Clone, Default)]
pub struct RunStats {
    pub run_start_collector_ms: Option<u64>,
    pub run_end_collector_ms: Option<u64>,
    pub finalize_received: bool,
    pub nodes: HashMap<String, NodeStats>,
}

#[derive(Debug, Clone, Default)]
pub struct NodeStats {
    pub boot_recorded: bool,
    pub event_batches: u64,
    pub snapshots: u64,
    pub finalize_recorded: bool,
    /// Cached identity payload (parsed from the most recent boot
    /// record) — lets the bundle assembler name the directory and
    /// fill in role/stage_index without re-reading boot.json.
    pub identity: Option<Value>,
}

impl CollectorState {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            finalize_wait: DEFAULT_FINALIZE_WAIT,
            seqs: Mutex::new(HashMap::new()),
            runs: Mutex::new(HashMap::new()),
            pending_hints: Mutex::new(HashMap::new()),
        }
    }

    /// Override the finalize wait window — tests use a millisecond
    /// budget to keep the suite snappy. Production defaults to
    /// [`DEFAULT_FINALIZE_WAIT`].
    pub fn with_finalize_wait(mut self, d: Duration) -> Self {
        self.finalize_wait = d;
        self
    }

    pub fn finalize_wait(&self) -> Duration {
        self.finalize_wait
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn run_dir(&self, run_id: &str) -> PathBuf {
        self.root.join(sanitize_path_component(run_id))
    }

    pub fn node_dir(&self, run_id: &str, node_id: &str) -> PathBuf {
        self.run_dir(run_id).join(sanitize_path_component(node_id))
    }

    pub fn bundles_dir(&self) -> PathBuf {
        self.root.join("bundles")
    }

    pub fn bundle_path(&self, run_id: &str) -> PathBuf {
        self.bundles_dir()
            .join(format!("{}.tar.gz", sanitize_path_component(run_id)))
    }

    /// Persist a single record and update per-run accounting. Returns
    /// the on-disk path that was written.
    pub fn persist(
        &self,
        run_id: &str,
        node_id: &str,
        kind: RecordKind,
        recv_ms: u64,
        body: &Value,
    ) -> io::Result<PathBuf> {
        let dir = self.node_dir(run_id, node_id);
        std::fs::create_dir_all(&dir)?;
        let seq = self.next_seq(run_id, node_id, kind);
        let filename = format!("{}-{:06}.json", kind.as_str(), seq);
        let path = dir.join(filename);
        let bytes = serde_json::to_vec_pretty(body)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        std::fs::write(&path, bytes)?;
        self.update_stats(run_id, node_id, kind, recv_ms, body);
        Ok(path)
    }

    fn next_seq(&self, run_id: &str, node_id: &str, kind: RecordKind) -> u64 {
        let mut seqs = self.seqs.lock().expect("collector seq mutex poisoned");
        let key = SeqKey {
            run_id: run_id.to_string(),
            node_id: node_id.to_string(),
            kind,
        };
        let entry = seqs.entry(key).or_insert(0);
        *entry += 1;
        *entry
    }

    fn update_stats(
        &self,
        run_id: &str,
        node_id: &str,
        kind: RecordKind,
        recv_ms: u64,
        body: &Value,
    ) {
        let mut runs = self.runs.lock().expect("collector runs mutex poisoned");
        let run = runs.entry(run_id.to_string()).or_default();
        run.run_start_collector_ms = Some(
            run.run_start_collector_ms
                .map(|prev| prev.min(recv_ms))
                .unwrap_or(recv_ms),
        );
        run.run_end_collector_ms = Some(
            run.run_end_collector_ms
                .map(|prev| prev.max(recv_ms))
                .unwrap_or(recv_ms),
        );
        let node = run.nodes.entry(node_id.to_string()).or_default();
        match kind {
            RecordKind::Boot => {
                node.boot_recorded = true;
                node.identity = Some(body.clone());
            }
            RecordKind::Events => {
                node.event_batches += 1;
            }
            RecordKind::Snapshot => {
                node.snapshots += 1;
            }
            RecordKind::Finalize => {
                node.finalize_recorded = true;
                run.finalize_received = true;
            }
        }
    }

    /// Read-only access to per-run accounting. Cloned so callers can
    /// release the lock immediately.
    pub fn run_stats(&self, run_id: &str) -> Option<RunStats> {
        self.runs
            .lock()
            .expect("collector runs mutex poisoned")
            .get(run_id)
            .cloned()
    }

    /// Queue `snapshot_now` to be returned on every known node's next
    /// POST under this run (T1.4 pull-trigger fan-out, used by the
    /// `/diag/finalize` handler).
    pub fn mark_run_for_snapshot_now(&self, run_id: &str) {
        let node_ids: Vec<String> = {
            let runs = self.runs.lock().expect("collector runs mutex poisoned");
            match runs.get(run_id) {
                Some(run) => run.nodes.keys().cloned().collect(),
                None => Vec::new(),
            }
        };
        let mut hints = self
            .pending_hints
            .lock()
            .expect("collector hints mutex poisoned");
        for node_id in node_ids {
            hints
                .entry(HintKey {
                    run_id: run_id.to_string(),
                    node_id,
                })
                .or_default()
                .snapshot_now = true;
        }
    }

    /// Pull (and clear) any pending hints for this (run, node). Called
    /// from the ingest path so each hint fires once.
    pub fn take_pending_hints(&self, run_id: &str, node_id: &str) -> Option<Hints> {
        self.pending_hints
            .lock()
            .expect("collector hints mutex poisoned")
            .remove(&HintKey {
                run_id: run_id.to_string(),
                node_id: node_id.to_string(),
            })
            .filter(|h| !h.is_empty())
    }
}

/// Reject path traversal in headers — keeps malicious or buggy nodes
/// from writing outside `{root}/{run}/{node}/`. We only allow
/// alphanumerics, dash, underscore, and dot; anything else is
/// replaced with `_`. Empty input becomes `_`.
fn sanitize_path_component(s: &str) -> String {
    if s.is_empty() {
        return "_".to_string();
    }
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    // Reject "." and ".." outright — even if they slip through char
    // filtering, they'd be interpreted as directory references.
    if out == "." || out == ".." {
        return "_".to_string();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_rejects_traversal_and_separators() {
        assert_eq!(sanitize_path_component(""), "_");
        assert_eq!(sanitize_path_component("."), "_");
        assert_eq!(sanitize_path_component(".."), "_");
        assert_eq!(sanitize_path_component("../etc"), ".._etc");
        assert_eq!(sanitize_path_component("good-id_1.2"), "good-id_1.2");
        assert_eq!(sanitize_path_component("a/b"), "a_b");
        assert_eq!(sanitize_path_component("a\\b\0c"), "a_b_c");
    }
}
