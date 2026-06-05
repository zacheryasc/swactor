//! Distribution plugin adapter for the dashboard plugin system.
//!
//! Wraps a cached `DistributionNodeSnapshot` into a `DashboardPlugin` that the
//! dashboard can poll for snapshots and serve the distribution page.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dashboard::plugin::{DashboardPlugin, PluginResponse};
use dashboard::JoinPeerInfo;
use distribution::snapshot::DistributionNodeSnapshot;

/// HTML page for the distribution plugin. Owned by the `dashboard` crate so the
/// live node and the datastream dashboard serve the identical page.
const DISTRIBUTION_HTML: &str = dashboard::DISTRIBUTION_PAGE_HTML;

/// Dashboard plugin that exposes distribution node snapshots.
pub struct DistributionPlugin {
    cached: Arc<Mutex<Option<DistributionNodeSnapshot>>>,
    join_sender: Option<std::sync::mpsc::Sender<JoinPeerInfo>>,
    /// Node IDs whose join status has been dismissed by the user.
    /// The main loop drains these and calls `clear_join_status` on the driver.
    dismissed_statuses: Arc<Mutex<Vec<[u8; 32]>>>,
}

impl DistributionPlugin {
    pub fn new(
        cached: Arc<Mutex<Option<DistributionNodeSnapshot>>>,
        join_sender: Option<std::sync::mpsc::Sender<JoinPeerInfo>>,
    ) -> Self {
        Self {
            cached,
            join_sender,
            dismissed_statuses: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get a clone of the dismissed-statuses Arc for the main loop to drain.
    pub fn dismissed_statuses(&self) -> Arc<Mutex<Vec<[u8; 32]>>> {
        Arc::clone(&self.dismissed_statuses)
    }
}

impl DashboardPlugin for DistributionPlugin {
    fn name(&self) -> &str {
        "distribution"
    }

    fn snapshot_json(&self) -> Option<String> {
        let guard = self.cached.lock().unwrap();
        let snapshot = guard.as_ref()?;
        serde_json::to_string(snapshot).ok()
    }

    fn handle_request(
        &self,
        method: &str,
        path: &str,
        _query: &HashMap<String, String>,
        body: &[u8],
    ) -> PluginResponse {
        match (method, path) {
            ("GET", "") => {
                let guard = self.cached.lock().unwrap();
                match guard.as_ref() {
                    Some(snapshot) => match serde_json::to_string(snapshot) {
                        Ok(json) => PluginResponse::json(json),
                        Err(_) => PluginResponse::json("{}".into()),
                    },
                    None => PluginResponse::json("{}".into()),
                }
            }
            ("POST", "rejoin") => self.handle_rejoin(body),
            ("POST", "clear_status") => self.handle_clear_status(body),
            _ => PluginResponse::not_found(),
        }
    }

    fn html_page(&self) -> Option<&str> {
        Some(DISTRIBUTION_HTML)
    }
}

impl DistributionPlugin {
    fn handle_rejoin(&self, body: &[u8]) -> PluginResponse {
        let tx = match &self.join_sender {
            Some(tx) => tx,
            None => return PluginResponse::json(r#"{"error":"rejoin not available"}"#.into()),
        };

        // Parse { "node_id": "<hex>" } from body
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => return PluginResponse::json(r#"{"error":"invalid json"}"#.into()),
        };
        let node_id_hex = match parsed.get("node_id").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return PluginResponse::json(r#"{"error":"missing node_id"}"#.into()),
        };

        // Parse hex node_id into [u8; 32]
        let bytes = match parse_hex_node_id(node_id_hex) {
            Some(b) => b,
            None => return PluginResponse::json(r#"{"error":"invalid node_id hex"}"#.into()),
        };

        // Look up relay_url from cached snapshot
        let relay_url = {
            let guard = self.cached.lock().unwrap();
            guard.as_ref().and_then(|snap| {
                snap.members.iter()
                    .find(|m| m.node_id == node_id_hex)
                    .and_then(|m| m.relay_url.clone())
            })
        };

        match tx.send(JoinPeerInfo { node_id: bytes, relay_url, direct_addrs: vec![] }) {
            Ok(()) => PluginResponse::json(r#"{"ok":true}"#.into()),
            Err(_) => PluginResponse::json(r#"{"error":"channel closed"}"#.into()),
        }
    }

    fn handle_clear_status(&self, body: &[u8]) -> PluginResponse {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => return PluginResponse::json(r#"{"error":"invalid json"}"#.into()),
        };
        let node_id_hex = match parsed.get("node_id").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return PluginResponse::json(r#"{"error":"missing node_id"}"#.into()),
        };
        let bytes = match parse_hex_node_id(node_id_hex) {
            Some(b) => b,
            None => return PluginResponse::json(r#"{"error":"invalid node_id hex"}"#.into()),
        };
        self.dismissed_statuses.lock().unwrap().push(bytes);
        PluginResponse::json(r#"{"ok":true}"#.into())
    }
}

fn parse_hex_node_id(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}
