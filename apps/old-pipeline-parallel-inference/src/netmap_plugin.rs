//! Orchestrator "netmap" dashboard plugin — a live connection/bandwidth map.
//!
//! The orchestrator dials every stage, so its egocentric view captures the
//! orchestrator↔stage mesh. This plugin renders that as a directed graph:
//! orchestrator at center, one node per SWIM member, and two arcs per member
//! (orchestrator→peer "sent" and peer→orchestrator "recv"). Each arc is
//! **colored by iroh transport** (direct / relay / mixed / idle) and **weighted
//! by live bandwidth** (bytes/sec, derived client-side from the delta of the
//! cumulative per-peer byte counters between successive snapshots).
//!
//! Two pieces feed it:
//!
//!   * [`SharedSnapshot`] — the cached cluster snapshot (members, names),
//!     **shared** with the distribution plugin.
//!   * [`ConnTracker`] — a per-peer transport map kept fresh by
//!     [`spawn_conn_poller`], which queries `endpoint.remote_info` directly —
//!     a self-contained poll, independent of any telemetry pipeline.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashboard::plugin::{DashboardPlugin, PluginResponse};
use iroh::PublicKey;
use iroh_driver::{ConnType, IrohDriver, conn_type_of};

use crate::dist_plugin::SharedSnapshot;

/// Milliseconds since the Unix epoch.
fn wall_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Parse a 64-char lowercase-hex node id back into raw key bytes. Inverse of the
/// `node_id_hex` / `hex32` formatting used across the snapshot and tally maps.
fn hex_to_32(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(hex.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

/// One peer's current transport, as observed from this node's iroh endpoint.
#[derive(Clone, serde::Serialize)]
pub struct ConnEntry {
    /// `"Direct"` / `"Relay"` / `"Mixed"` / `"None"` (matches [`ConnType`]'s serde form).
    pub r#type: String,
    /// The relay URL backing this peer, when one is known.
    pub relay_url: Option<String>,
}

/// Live per-peer transport map, keyed by peer node-id hex (lowercase).
#[derive(Default)]
pub struct ConnTracker {
    map: Mutex<HashMap<String, ConnEntry>>,
}

impl ConnTracker {
    pub fn set(&self, hex: String, ty: ConnType, relay_url: Option<String>) {
        self.map.lock().unwrap().insert(
            hex,
            ConnEntry {
                r#type: format!("{ty:?}"),
                relay_url,
            },
        );
    }

    /// Drop entries for peers no longer present (departed members).
    pub fn retain_peers(&self, keep: &HashSet<String>) {
        self.map.lock().unwrap().retain(|k, _| keep.contains(k));
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(&*self.map.lock().unwrap()).unwrap_or(serde_json::Value::Null)
    }
}

const NETMAP_HTML: &str = include_str!("netmap_page.html");

/// Read-only dashboard plugin backing `/plugin/netmap`.
pub struct NetmapPlugin {
    cached: SharedSnapshot,
    conn: Arc<ConnTracker>,
}

impl NetmapPlugin {
    pub fn new(cached: SharedSnapshot, conn: Arc<ConnTracker>) -> Self {
        Self { cached, conn }
    }

    /// Serialize the cached snapshot with the transport map and a server
    /// timestamp the frontend uses as the time base for bytes/sec.
    fn rendered_json(&self) -> Option<String> {
        let guard = self.cached.lock().unwrap();
        let snap = guard.as_ref()?;
        let mut v = serde_json::to_value(snap).ok()?;
        if let serde_json::Value::Object(ref mut m) = v {
            m.insert("conn".to_string(), self.conn.to_json());
            m.insert("conn_ts_ms".to_string(), serde_json::json!(wall_ms_now()));
        }
        serde_json::to_string(&v).ok()
    }
}

impl DashboardPlugin for NetmapPlugin {
    fn name(&self) -> &str {
        "netmap"
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
        Some(NETMAP_HTML)
    }
}

/// Spawn the transport poller on the driver's tokio runtime. Every ~1s it reads
/// the current member set from `cached`, asks the iroh endpoint for each peer's
/// `remote_info`, derives the [`ConnType`], and updates `conn`. Runs until `stop`
/// is set. Independent of the diagnostics aggregator.
pub fn spawn_conn_poller(
    driver: &IrohDriver,
    cached: SharedSnapshot,
    conn: Arc<ConnTracker>,
    stop: Arc<AtomicBool>,
) {
    // `Endpoint` is Arc-backed; clone gives the task an owned handle without
    // borrowing the driver.
    let endpoint = driver.endpoint().clone();
    driver.tokio_handle().spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(1000));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        while !stop.load(Ordering::Relaxed) {
            ticker.tick().await;
            // Clone the member hex set, then drop the guard before any `.await`
            // (std Mutex guards must not be held across await points).
            let peers: Vec<String> = {
                let guard = cached.lock().unwrap();
                guard
                    .as_ref()
                    .map(|s| s.members.iter().map(|m| m.node_id.clone()).collect())
                    .unwrap_or_default()
            };
            let mut seen = HashSet::with_capacity(peers.len());
            for hex in peers {
                let Some(bytes) = hex_to_32(&hex) else {
                    continue;
                };
                let Ok(pk) = PublicKey::from_bytes(&bytes) else {
                    continue;
                };
                seen.insert(hex.clone());
                match endpoint.remote_info(pk).await {
                    Some(info) => {
                        let relay_url = info.addrs().find_map(|ai| match ai.addr() {
                            iroh::TransportAddr::Relay(url) => Some(url.to_string()),
                            _ => None,
                        });
                        conn.set(hex, conn_type_of(&info), relay_url);
                    }
                    // Known member, but iroh has no connection info yet.
                    None => conn.set(hex, ConnType::None, None),
                }
            }
            conn.retain_peers(&seen);
        }
    });
}
