//! Orchestrator distribution dashboard plugin.
//!
//! Two cooperating pieces give the orchestrator dashboard its "Distribution"
//! tab (which is otherwise dead — the dashboard ships the nav link but no
//! plugin is registered to back `/plugin/distribution`):
//!
//!   * [`MsgCounts`] — cumulative wire-message tallies (totals, bytes, and a
//!     per-kind / per-peer breakdown). The actor transport
//!     (`crate::iroh_transport`) calls [`MsgCounts::note_sent`] /
//!     [`MsgCounts::note_recv`] directly for every application message it
//!     moves, so the messages panel shows live pipeline traffic.
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

pub(crate) fn hex32(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

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
    /// Tally one outbound application message to `peer` (node-id hex).
    pub(crate) fn note_sent(&self, peer: &str, kind: &str, size: u64) {
        self.sent_total.fetch_add(1, Ordering::Relaxed);
        self.sent_bytes.fetch_add(size, Ordering::Relaxed);
        *self
            .by_kind_sent
            .lock()
            .unwrap()
            .entry(kind.to_string())
            .or_insert(0) += 1;
        let mut by_peer = self.by_peer.lock().unwrap();
        let tally = by_peer.entry(peer.to_string()).or_default();
        tally.sent += 1;
        tally.sent_bytes += size;
    }

    /// Tally one inbound application message from `peer` (node-id hex).
    pub(crate) fn note_recv(&self, peer: &str, kind: &str, size: u64) {
        self.recv_total.fetch_add(1, Ordering::Relaxed);
        self.recv_bytes.fetch_add(size, Ordering::Relaxed);
        *self
            .by_kind_recv
            .lock()
            .unwrap()
            .entry(kind.to_string())
            .or_insert(0) += 1;
        let mut by_peer = self.by_peer.lock().unwrap();
        let tally = by_peer.entry(peer.to_string()).or_default();
        tally.recv += 1;
        tally.recv_bytes += size;
    }

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
/// `msg_counts` — the exact JSON shape the distribution UI consumes.
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
