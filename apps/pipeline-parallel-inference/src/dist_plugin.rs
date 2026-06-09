//! Orchestrator distribution dashboard plugin.
//!
//! Two cooperating pieces give the orchestrator dashboard its "Distribution"
//! tab (which is otherwise dead — the dashboard ships the nav link but no
//! plugin is registered to back `/plugin/distribution`):
//!
//!   * [`MsgCounts`] — a per-kind / per-peer message + byte tally injected into
//!     the snapshot JSON under `msg_counts`. The engine no longer carries a
//!     message-counting emitter, so the tallies currently stay at zero; the
//!     panel renders the shape and is ready to repopulate if a datastream
//!     message-count channel is wired.
//!
//!   * [`DistDashPlugin`] — a read-only [`DashboardPlugin`] named
//!     `"distribution"`. It serves a cached [`DistributionNodeSnapshot`] (SWIM
//!     members, routing table, location cache, name registry) with the message
//!     tallies injected under `msg_counts`, plus a lean HTML page that draws a
//!     SWIM membership graph and a messages panel.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dashboard::plugin::{DashboardPlugin, PluginResponse};
use distribution::snapshot::DistributionNodeSnapshot;

/// Shared snapshot cell the orchestrator refreshes each tick.
pub type SharedSnapshot = Arc<Mutex<Option<DistributionNodeSnapshot>>>;

/// Cumulative wire-message tallies for this node.
#[derive(Default)]
pub struct MsgCounts {
    sent_total: AtomicU64,
    recv_total: AtomicU64,
    sent_bytes: AtomicU64,
    recv_bytes: AtomicU64,
    by_kind_sent: Mutex<HashMap<String, u64>>,
    by_kind_recv: Mutex<HashMap<String, u64>>,
    /// peer hex → cumulative per-peer message/byte tallies
    by_peer: Mutex<HashMap<String, PeerTally>>,
}

/// Cumulative per-peer tally: message counts and payload bytes, each split by
/// direction. Byte counters are monotonic, so a UI can derive a rate from the
/// delta between two snapshots.
#[derive(Default, Clone, Copy)]
pub struct PeerTally {
    pub sent: u64,
    pub recv: u64,
    pub sent_bytes: u64,
    pub recv_bytes: u64,
}

impl MsgCounts {
    /// Render the tallies as the JSON object injected under `msg_counts`.
    pub fn to_json(&self) -> serde_json::Value {
        let by_peer: serde_json::Map<String, serde_json::Value> = self
            .by_peer
            .lock()
            .unwrap()
            .iter()
            .map(|(k, t)| {
                (
                    k.clone(),
                    serde_json::json!({
                        "sent": t.sent,
                        "recv": t.recv,
                        "sent_bytes": t.sent_bytes,
                        "recv_bytes": t.recv_bytes,
                    }),
                )
            })
            .collect();
        serde_json::json!({
            "sent_total": self.sent_total.load(Ordering::Relaxed),
            "recv_total": self.recv_total.load(Ordering::Relaxed),
            "sent_bytes": self.sent_bytes.load(Ordering::Relaxed),
            "recv_bytes": self.recv_bytes.load(Ordering::Relaxed),
            "by_kind_sent": self.by_kind_sent.lock().unwrap().clone(),
            "by_kind_recv": self.by_kind_recv.lock().unwrap().clone(),
            "by_peer": by_peer,
        })
    }
}

/// Render a distribution snapshot with the message tallies injected under
/// `msg_counts` — the exact JSON shape the distribution UI consumes, served by
/// [`DistDashPlugin`].
pub fn render_dist_json(snap: &DistributionNodeSnapshot, counts: &MsgCounts) -> Option<String> {
    let mut v = serde_json::to_value(snap).ok()?;
    if let serde_json::Value::Object(ref mut m) = v {
        m.insert("msg_counts".to_string(), counts.to_json());
    }
    serde_json::to_string(&v).ok()
}

/// Read-only dashboard plugin backing `/plugin/distribution`.
pub struct DistDashPlugin {
    cached: SharedSnapshot,
    counts: Arc<MsgCounts>,
}

impl DistDashPlugin {
    pub fn new(cached: SharedSnapshot, counts: Arc<MsgCounts>) -> Self {
        Self { cached, counts }
    }

    /// Serialize the cached snapshot with the message tallies injected.
    fn rendered_json(&self) -> Option<String> {
        let guard = self.cached.lock().unwrap();
        let snap = guard.as_ref()?;
        render_dist_json(snap, &self.counts)
    }
}

const DIST_HTML: &str = include_str!("dist_page.html");

impl DashboardPlugin for DistDashPlugin {
    fn name(&self) -> &str {
        "distribution"
    }

    fn snapshot_json(&self) -> Option<String> {
        self.rendered_json()
    }

    fn handle_request(
        &self,
        method: &str,
        path: &str,
        _query: &HashMap<String, String>,
        _body: &[u8],
    ) -> PluginResponse {
        match (method, path) {
            ("GET", "") => {
                PluginResponse::json(self.rendered_json().unwrap_or_else(|| "{}".into()))
            }
            _ => PluginResponse::not_found(),
        }
    }

    fn html_page(&self) -> Option<&str> {
        Some(DIST_HTML)
    }
}
