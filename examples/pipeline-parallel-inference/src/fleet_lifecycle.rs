//! The bootstrap → joined lifecycle for launched nodes (the reusable
//! "spawn swactor onto a new node" abstraction).
//!
//! The orchestrator launches N stage nodes; each one either fails to load or
//! loads and joins the cluster. We want to *see* that loading happen — its
//! stdout/stderr in the Fleet view — and transition seamlessly to live
//! telemetry on join. [`FleetLifecycle`] is one logical Fleet entry per launched
//! node, created at launch time keyed by a stable handle (the stage index)
//! *before* the node has a swactor id, driven by three input sources the
//! orchestrator already owns:
//!
//! * **Bootstrap logs** — the launcher child's stdout/stderr (local), or vast.ai
//!   `status_msg`/`disk_usage` progress + the post-running container log tail
//!   ([`record_bootstrap`](FleetLifecycle::record_bootstrap)). Shown as
//!   `proc.bootstrap.*` while the node is loading.
//! * **The join seam** — when `pp-stage-K` resolves to a node id, the same entry
//!   flips `Bootstrapping → Live` and binds the node's live datastream
//!   ([`bind_live`](FleetLifecycle::bind_live)). One continuous row.
//! * **Lease metadata** — contract id / cost / status the orchestrator holds
//!   from leasing, attached directly ([`set_lease`](FleetLifecycle::set_lease)).
//!
//! The live telemetry itself comes from the datastream: the entry's bound node
//! id indexes the dashboard's [`FleetView`], which the `datastream-sink` actor
//! folds frames into. [`render`](FleetLifecycle::render) merges launch state +
//! bootstrap logs + lease metadata + datastream telemetry into the one Fleet
//! model the dashboard serves.

use std::sync::{Arc, Mutex};

use dashboard::datastream_source::FleetView;
use distribution::datastream::frame::{Frame, StreamId};

/// How many trailing bootstrap lines to keep per node. The Fleet row shows the
/// last line; the tail bounds memory for a chatty image pull.
const BOOTSTRAP_TAIL: usize = 200;

/// Orchestrator-side lease facts for a launched node — knowledge the
/// orchestrator already holds from leasing, attached to the row directly rather
/// than shipped from the node.
#[derive(Debug, Clone, Default)]
pub struct LeaseMeta {
    pub contract_id: u64,
    pub dph: f64,
    pub status: String,
    pub region: String,
    pub gpu: String,
}

/// The lifecycle state of one launched node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaunchState {
    /// Loading swactor — not yet trusted/joined. Telemetry is bootstrap logs.
    Bootstrapping,
    /// Joined: the node's own datastream frames are arriving over the cluster.
    Live,
    /// The join deadline passed with no membership. Last bootstrap logs are kept
    /// as the failure evidence.
    Failed,
}

impl LaunchState {
    fn as_str(self) -> &'static str {
        match self {
            LaunchState::Bootstrapping => "bootstrapping",
            LaunchState::Live => "live",
            LaunchState::Failed => "failed",
        }
    }
}

/// One Fleet entry per launched node, keyed by stage index.
struct LaunchEntry {
    stage: u32,
    state: LaunchState,
    /// The node's swactor id (hex), known only once it joins (`bind_live`).
    node_id: Option<String>,
    /// Trailing bootstrap output (newest last), `(is_stderr, line)`.
    bootstrap: Vec<(bool, String)>,
    lease: Option<LeaseMeta>,
}

impl LaunchEntry {
    fn last_bootstrap(&self) -> String {
        self.bootstrap
            .last()
            .map(|(_, l)| l.clone())
            .unwrap_or_default()
    }
}

/// The per-launch Fleet model: launch entries (orchestrator-owned) merged with
/// live datastream telemetry (the shared [`FleetView`] the sink folds into).
pub struct FleetLifecycle {
    fleet: Arc<Mutex<FleetView>>,
    entries: Mutex<Vec<LaunchEntry>>,
    /// The Fleet dashboard cache this lifecycle renders into.
    cache: Arc<Mutex<Option<String>>>,
}

impl FleetLifecycle {
    /// Register `num_stages` launch entries, all `Bootstrapping`. `fleet` is the
    /// datastream aggregate the sink folds into; `cache` is the Fleet plugin's
    /// cache string this lifecycle keeps refreshed.
    pub fn new(
        fleet: Arc<Mutex<FleetView>>,
        cache: Arc<Mutex<Option<String>>>,
        num_stages: u32,
    ) -> Self {
        let entries = (0..num_stages)
            .map(|stage| LaunchEntry {
                stage,
                state: LaunchState::Bootstrapping,
                node_id: None,
                bootstrap: Vec::new(),
                lease: None,
            })
            .collect();
        let lifecycle = Self {
            fleet,
            entries: Mutex::new(entries),
            cache,
        };
        lifecycle.refresh();
        lifecycle
    }

    /// Fold one delivered datastream frame into the bound node's telemetry and
    /// refresh. Called by the orchestrator's `datastream-sink` actor for every
    /// frame a worker ships over the cluster.
    pub fn ingest(&self, stream: &StreamId, frame: &Frame) {
        self.fleet.lock().unwrap().ingest(stream, frame);
        self.refresh();
    }

    /// Append a bootstrap-log line for `stage` (the launcher child's output).
    pub fn record_bootstrap(&self, stage: u32, is_stderr: bool, line: &str) {
        {
            let mut entries = self.entries.lock().unwrap();
            if let Some(e) = entries.iter_mut().find(|e| e.stage == stage) {
                e.bootstrap.push((is_stderr, line.to_string()));
                if e.bootstrap.len() > BOOTSTRAP_TAIL {
                    let drop = e.bootstrap.len() - BOOTSTRAP_TAIL;
                    e.bootstrap.drain(0..drop);
                }
            }
        }
        self.refresh();
    }

    /// Flip `stage` to `Live` and bind it to the joined node's id, so the row's
    /// telemetry now comes from the node's live datastream. The seam.
    pub fn bind_live(&self, stage: u32, node_id_hex: &str) {
        {
            let mut entries = self.entries.lock().unwrap();
            if let Some(e) = entries.iter_mut().find(|e| e.stage == stage) {
                e.node_id = Some(node_id_hex.to_string());
                e.state = LaunchState::Live;
            }
        }
        self.refresh();
    }

    /// Has `stage` joined (flipped to live)? Used by the vast.ai bootstrap-log
    /// poller to stop once the node's own datastream takes over.
    pub fn stage_is_live(&self, stage: u32) -> bool {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .find(|e| e.stage == stage)
            .map(|e| e.state == LaunchState::Live)
            .unwrap_or(false)
    }

    /// Attach lease metadata to `stage`'s row.
    pub fn set_lease(&self, stage: u32, lease: LeaseMeta) {
        {
            let mut entries = self.entries.lock().unwrap();
            if let Some(e) = entries.iter_mut().find(|e| e.stage == stage) {
                e.lease = Some(lease);
            }
        }
        self.refresh();
    }

    /// Flip every still-`Bootstrapping` entry to `Failed` — the join deadline
    /// passed. Their last bootstrap logs remain as the failure evidence.
    pub fn fail_remaining_bootstrapping(&self) {
        {
            let mut entries = self.entries.lock().unwrap();
            for e in entries.iter_mut() {
                if e.state == LaunchState::Bootstrapping {
                    e.state = LaunchState::Failed;
                }
            }
        }
        self.refresh();
    }

    /// Re-render the Fleet model into the cache from the current launch entries
    /// and live datastream telemetry. Cheap; called on every state change and
    /// can be called periodically to pick up fresh telemetry.
    pub fn refresh(&self) {
        let entries = self.entries.lock().unwrap();
        let fleet = self.fleet.lock().unwrap();

        let mut nodes: Vec<serde_json::Value> = Vec::with_capacity(entries.len());
        let mut live_count = 0usize;
        let mut all_converged = true;

        for e in entries.iter() {
            // Start from the node's live datastream telemetry when it has joined
            // and is streaming; otherwise an empty object (bootstrap/failed).
            let live_telemetry = e
                .node_id
                .as_deref()
                .filter(|id| fleet.is_live(id))
                .and_then(|id| fleet.node_metrics(id));
            let is_streaming = live_telemetry.is_some();
            let mut row = live_telemetry.unwrap_or_else(|| serde_json::json!({}));

            if e.state == LaunchState::Live && is_streaming {
                live_count += 1;
                if row.get("converged").and_then(|v| v.as_bool()) != Some(true) {
                    all_converged = false;
                }
            } else {
                all_converged = false;
            }

            let lease = e.lease.clone().unwrap_or_default();
            let short = e
                .node_id
                .as_deref()
                .map(|id| id.chars().take(8).collect::<String>())
                .unwrap_or_else(|| format!("stage {}", e.stage));
            // The orchestrator owns role/region (a node is generic on the wire).
            let region = if lease.region.is_empty() {
                "local".to_string()
            } else {
                lease.region.clone()
            };
            // The bootstrap tail is the "last line" until live telemetry has a
            // process line of its own.
            let last_proc = row
                .get("last_proc")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| e.last_bootstrap());

            let obj = row.as_object_mut().expect("row is a json object");
            obj.insert("id".into(), serde_json::json!(e.node_id.clone().unwrap_or_default()));
            obj.insert("short".into(), serde_json::json!(short));
            obj.insert("stage".into(), serde_json::json!(e.stage));
            obj.insert("state".into(), serde_json::json!(e.state.as_str()));
            obj.insert("region".into(), serde_json::json!(region));
            obj.insert("role".into(), serde_json::json!(format!("stage {}", e.stage)));
            obj.insert("contract".into(), serde_json::json!(lease.contract_id));
            obj.insert("dph".into(), serde_json::json!(lease.dph));
            obj.insert("gpu".into(), serde_json::json!(lease.gpu));
            obj.insert("status".into(), serde_json::json!(lease.status));
            obj.insert("last_proc".into(), serde_json::json!(last_proc));
            obj.entry("cpu_pct").or_insert(serde_json::json!(0));
            obj.entry("mem_used_mb").or_insert(serde_json::json!(0));
            obj.entry("mem_total_mb").or_insert(serde_json::json!(0));
            obj.entry("gpu_pct").or_insert(serde_json::json!(0));
            obj.entry("actors_live").or_insert(serde_json::json!(0));
            obj.entry("mailbox_depth").or_insert(serde_json::json!(0));
            obj.entry("relay_connected").or_insert(serde_json::json!(false));
            obj.entry("direct_peers").or_insert(serde_json::json!(0));
            obj.entry("relay_peers").or_insert(serde_json::json!(0));
            obj.entry("alive").or_insert(serde_json::json!(0));
            obj.entry("suspect").or_insert(serde_json::json!(0));
            obj.entry("dead").or_insert(serde_json::json!(0));
            obj.entry("proc_lines").or_insert(serde_json::json!(0));
            obj.entry("selected").or_insert(serde_json::json!(false));

            nodes.push(row);
        }

        // The whole fleet is converged once every launched node is live and its
        // own SWIM view has converged.
        let converged = !entries.is_empty() && live_count == entries.len() && all_converged;
        let model = serde_json::json!({
            "node_count": entries.len(),
            "live_count": live_count,
            "converged": converged,
            "nodes": nodes,
        });
        *self.cache.lock().unwrap() = Some(model.to_string());
    }
}
