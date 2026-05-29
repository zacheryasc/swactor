//! Persistence + sequencing state for a running collector.
//!
//! Owns the on-disk layout under `{root}/{run_id}/{node_id}/` and the
//! per-(run, node, kind) sequence counters used to make filenames
//! collision-free across concurrent posts.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::broadcast;

use super::protocol::{Hints, LiveRecord, RecordKind};

/// Default fan-out capacity for the live SSE broadcast — overridable
/// via `SWACTOR_DIAG_STREAM_CAPACITY`. 1024 is ~70s of buffer at the
/// ~14 rec/s typical of an 11-stage cluster; slow subscribers see
/// `Lagged` rather than backpressuring the ingest path.
pub const DEFAULT_STREAM_CAPACITY: usize = 1024;

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
    /// Coverage 2.5 — bundle serve hardening under run-id reuse.
    /// Records the in-memory node count captured each time the
    /// canonical tarball is written by `bundle::assemble`. On serve,
    /// `download_bundle` compares this against the current
    /// `run_stats(run_id).nodes.len()`; if staging has grown past
    /// the canonical's snapshot the canonical is stale and the
    /// handler rebuilds from current staging. This is the
    /// "node-count heuristic" the spec names.
    canonical_node_counts: Mutex<HashMap<String, usize>>,
    /// Fan-out of every persisted record to live SSE subscribers.
    /// Lossy: when a subscriber falls behind the channel's capacity
    /// it observes `Lagged` and resumes from the next send. The
    /// catch-up path is `GET /diag/bundle/{run_id}`.
    live_tx: broadcast::Sender<Arc<LiveRecord>>,
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
    /// Count of records from the independent vastai monitoring layer.
    /// These nodes carry no swactor boot/identity, so the bundle
    /// assembler names them from their synthetic node id.
    pub vastai_records: u64,
    /// Cached identity payload (parsed from the most recent boot
    /// record) — lets the bundle assembler name the directory and
    /// fill in role/stage_index without re-reading boot.json.
    pub identity: Option<Value>,
}

impl CollectorState {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let cap = std::env::var("SWACTOR_DIAG_STREAM_CAPACITY")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_STREAM_CAPACITY);
        let (live_tx, _) = broadcast::channel(cap);
        Self {
            root: root.into(),
            finalize_wait: DEFAULT_FINALIZE_WAIT,
            seqs: Mutex::new(HashMap::new()),
            runs: Mutex::new(HashMap::new()),
            pending_hints: Mutex::new(HashMap::new()),
            canonical_node_counts: Mutex::new(HashMap::new()),
            live_tx,
        }
    }

    /// Coverage 2.5: record the node-count snapshot captured when the
    /// canonical tarball was last written for `run_id`. Called by
    /// `bundle::assemble` right after the tarball lands on disk so
    /// the serve handler can compare against current staging to
    /// detect canonical staleness.
    pub fn record_canonical_node_count(&self, run_id: &str, count: usize) {
        let mut m = self
            .canonical_node_counts
            .lock()
            .expect("canonical_node_counts mutex poisoned");
        m.insert(run_id.to_string(), count);
    }

    /// Coverage 2.5: return the canonical's last-recorded node count
    /// for `run_id`, or `None` if no canonical has been written yet
    /// (or this collector process never wrote one).
    pub fn canonical_node_count(&self, run_id: &str) -> Option<usize> {
        self.canonical_node_counts
            .lock()
            .expect("canonical_node_counts mutex poisoned")
            .get(run_id)
            .copied()
    }

    /// Override the finalize wait window — tests use a millisecond
    /// budget to keep the suite snappy. Production defaults to
    /// [`DEFAULT_FINALIZE_WAIT`].
    pub fn with_finalize_wait(mut self, d: Duration) -> Self {
        self.finalize_wait = d;
        self
    }

    /// Override the live-broadcast capacity. Production reads
    /// `SWACTOR_DIAG_STREAM_CAPACITY` in [`Self::new`]; tests use
    /// this builder to exercise the lossy-lagged path without
    /// racing other tests on a shared env var.
    pub fn with_stream_capacity(mut self, cap: usize) -> Self {
        let cap = cap.max(1);
        let (tx, _) = broadcast::channel(cap);
        self.live_tx = tx;
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
        // Fan out to live SSE subscribers. SendError (zero receivers)
        // is steady state; ignore.
        let _ = self.live_tx.send(Arc::new(LiveRecord {
            run_id: run_id.to_string(),
            node_id: node_id.to_string(),
            kind,
            recv_ms,
            seq,
            body: body.clone(),
        }));
        Ok(path)
    }

    /// Subscribe to the live fan-out of persisted records. Each
    /// receiver gets every record sent after subscription; if the
    /// receiver falls behind the channel capacity it observes
    /// `RecvError::Lagged(n)` and continues from the next send.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<LiveRecord>> {
        self.live_tx.subscribe()
    }

    /// Snapshot of all known runs and their accounting, sorted by
    /// `run_id`. Used by `GET /diag/runs`.
    pub fn run_summaries(&self) -> Vec<(String, RunStats)> {
        let runs = self.runs.lock().expect("collector runs mutex poisoned");
        let mut out: Vec<(String, RunStats)> =
            runs.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
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
            RecordKind::VastaiInstance
            | RecordKind::VastaiSample
            | RecordKind::VastaiLogs
            | RecordKind::VastaiLifecycle => {
                node.vastai_records += 1;
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
