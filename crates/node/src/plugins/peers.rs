//! Peers plugin adapter for the dashboard plugin system.
//!
//! Moves peer management from the dashboard's server.rs into a plugin that
//! handles add/remove/sync/list operations via `PeerAllowList`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dashboard::plugin::{DashboardPlugin, PluginResponse};
use dashboard::JoinPeerInfo;
use swactor_transport::NodeId;
use swactor_transport::identity::{base58_decode, hex_decode};
use distribution::peer_auth::PeerAllowList;

/// Dashboard plugin that exposes peer management operations.
pub struct PeersPlugin {
    peer_auth: Arc<Mutex<PeerAllowList>>,
    join_sender: Option<std::sync::mpsc::Sender<JoinPeerInfo>>,
}

impl PeersPlugin {
    pub fn new(
        peer_auth: Arc<Mutex<PeerAllowList>>,
        join_sender: Option<std::sync::mpsc::Sender<JoinPeerInfo>>,
    ) -> Self {
        Self {
            peer_auth,
            join_sender,
        }
    }
}

impl DashboardPlugin for PeersPlugin {
    fn name(&self) -> &str {
        "peers"
    }

    fn snapshot_json(&self) -> Option<String> {
        let list = self.peer_auth.lock().unwrap();
        let is_open = list.is_open();
        let peers: Vec<serde_json::Value> = list
            .list_peers()
            .iter()
            .map(|p| {
                serde_json::json!({
                    "node_id": p.node_id,
                    "label": p.label,
                })
            })
            .collect();
        let json = serde_json::json!({
            "mode": if is_open { "open" } else { "allow-list" },
            "peers": peers,
        });
        Some(json.to_string())
    }

    fn handle_request(
        &self,
        method: &str,
        path: &str,
        _query: &HashMap<String, String>,
        body: &[u8],
    ) -> PluginResponse {
        match (method, path) {
            ("GET", "list") => self.handle_list(),
            ("POST", "add") => self.handle_add(body),
            ("POST", "sync") => self.handle_sync(body),
            ("POST", "remove") => self.handle_remove(body),
            _ => PluginResponse::not_found(),
        }
    }

    fn html_page(&self) -> Option<&str> {
        None
    }
}

impl PeersPlugin {
    fn handle_list(&self) -> PluginResponse {
        let list = self.peer_auth.lock().unwrap();
        let is_open = list.is_open();
        let peers: Vec<serde_json::Value> = list
            .list_peers()
            .iter()
            .map(|p| {
                serde_json::json!({
                    "node_id": p.node_id,
                    "label": p.label,
                })
            })
            .collect();
        let json = serde_json::json!({
            "mode": if is_open { "open" } else { "allow-list" },
            "peers": peers,
        });
        PluginResponse::json(json.to_string())
    }

    fn handle_add(&self, body: &[u8]) -> PluginResponse {
        let body_str = match std::str::from_utf8(body) {
            Ok(s) => s,
            Err(_) => return PluginResponse::error(400, "invalid UTF-8"),
        };

        let parsed: serde_json::Value = match serde_json::from_str(body_str) {
            Ok(v) => v,
            Err(e) => return PluginResponse::error(400, format!("invalid JSON: {e}")),
        };

        let raw_node_id = match parsed.get("node_id").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return PluginResponse::error(400, "missing node_id field"),
        };
        let label = parsed
            .get("label")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Parse rich invite code: <base58>#<addr1>,<addr2>@<relay_url>
        // Split on last '@' for relay, then '#' for direct addrs
        let (left, invite_relay_url) = match raw_node_id.rfind('@') {
            Some(idx) => (&raw_node_id[..idx], Some(raw_node_id[idx + 1..].to_string())),
            None => (raw_node_id, None),
        };
        let (node_id_str, invite_addrs_str) = match left.find('#') {
            Some(idx) => (&left[..idx], Some(&left[idx + 1..])),
            None => (left, None),
        };

        // Parse direct addrs from invite code or explicit body field
        let direct_addrs_str = parsed
            .get("direct_addrs")
            .and_then(|v| v.as_str())
            .or(invite_addrs_str);
        let direct_addrs: Vec<std::net::SocketAddr> = direct_addrs_str
            .map(|s| {
                s.split(',')
                    .filter_map(|a| a.trim().parse::<std::net::SocketAddr>().ok())
                    .collect()
            })
            .unwrap_or_default();

        let bytes: [u8; 32] = if let Some(b) = hex_decode(node_id_str) {
            match b.try_into() {
                Ok(arr) => arr,
                Err(_) => {
                    return PluginResponse::error(400, "invalid node_id (hex decoded to wrong length)");
                }
            }
        } else if let Some(arr) = base58_decode(node_id_str) {
            arr
        } else {
            return PluginResponse::error(400, "invalid node_id (expected 64-char hex or base58)");
        };

        // Explicit relay_url field takes precedence, then invite code's @relay
        let relay_url = parsed
            .get("relay_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or(invite_relay_url);

        let node_id = NodeId(bytes);
        let mut list = self.peer_auth.lock().unwrap();
        list.add_peer(node_id, label);
        if let Err(e) = list.save() {
            eprintln!("warning: failed to persist peers.json: {e}");
        }
        drop(list);

        // Trigger a SWIM join for the newly added peer
        let has_direct = !direct_addrs.is_empty();
        let direct_count = direct_addrs.len();
        if let Some(tx) = &self.join_sender {
            let _ = tx.send(JoinPeerInfo {
                node_id: bytes,
                relay_url: relay_url.clone(),
                direct_addrs,
            });
        }

        let has_relay = relay_url.is_some();
        let node_id_hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
        PluginResponse::json(format!(
            r#"{{"ok":true,"has_relay":{has_relay},"has_direct":{has_direct},"direct_count":{direct_count},"node_id":"{node_id_hex}"}}"#
        ))
    }

    fn handle_sync(&self, body: &[u8]) -> PluginResponse {
        let body_str = match std::str::from_utf8(body) {
            Ok(s) => s,
            Err(_) => return PluginResponse::error(400, "invalid UTF-8"),
        };

        let parsed: serde_json::Value = match serde_json::from_str(body_str) {
            Ok(v) => v,
            Err(e) => return PluginResponse::error(400, format!("invalid JSON: {e}")),
        };

        let peers = match parsed.get("peers").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return PluginResponse::error(400, "missing peers array"),
        };

        let mut parsed_peers: Vec<(NodeId, String)> = Vec::new();
        for peer in peers {
            let node_id_str = match peer.get("node_id").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => return PluginResponse::error(400, "peer missing node_id"),
            };
            let label = peer
                .get("label")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let bytes: [u8; 32] = if let Some(b) = hex_decode(node_id_str) {
                match b.try_into() {
                    Ok(arr) => arr,
                    Err(_) => {
                        return PluginResponse::error(
                            400,
                            format!("invalid node_id hex length for {node_id_str}"),
                        );
                    }
                }
            } else if let Some(arr) = base58_decode(node_id_str) {
                arr
            } else {
                return PluginResponse::error(400, format!("invalid node_id: {node_id_str}"));
            };

            parsed_peers.push((NodeId(bytes), label));
        }

        {
            let mut list = self.peer_auth.lock().unwrap();
            for (node_id, label) in &parsed_peers {
                list.add_peer(*node_id, label.clone());
            }
            if let Err(e) = list.save() {
                eprintln!("warning: failed to persist peers.json: {e}");
            }
        }

        // Trigger a SWIM join to the seed peer if specified
        let join_seed = parsed.get("join_seed").and_then(|v| v.as_str());
        if let Some(seed_str) = join_seed {
            let seed_bytes: Option<[u8; 32]> = if let Some(b) = hex_decode(seed_str) {
                b.try_into().ok()
            } else {
                base58_decode(seed_str)
            };

            if let Some(bytes) = seed_bytes {
                let relay_url = peers.iter().find_map(|p| {
                    let nid = p.get("node_id").and_then(|v| v.as_str())?;
                    let peer_bytes: [u8; 32] = if let Some(b) = hex_decode(nid) {
                        b.try_into().ok()?
                    } else {
                        base58_decode(nid)?
                    };
                    if peer_bytes == bytes {
                        p.get("relay_url")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    } else {
                        None
                    }
                });

                if let Some(tx) = &self.join_sender {
                    let _ = tx.send(JoinPeerInfo {
                        node_id: bytes,
                        relay_url,
                        direct_addrs: vec![],
                    });
                }
            }
        }

        let added = parsed_peers.len();
        PluginResponse::json(format!(r#"{{"ok":true,"added":{added}}}"#))
    }

    fn handle_remove(&self, body: &[u8]) -> PluginResponse {
        let body_str = match std::str::from_utf8(body) {
            Ok(s) => s,
            Err(_) => return PluginResponse::error(400, "invalid UTF-8"),
        };

        let parsed: serde_json::Value = match serde_json::from_str(body_str) {
            Ok(v) => v,
            Err(e) => return PluginResponse::error(400, format!("invalid JSON: {e}")),
        };

        let node_id_hex = match parsed.get("node_id").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return PluginResponse::error(400, "missing node_id field"),
        };

        let bytes: Vec<u8> = if let Some(b) = hex_decode(node_id_hex) {
            if b.len() != 32 {
                return PluginResponse::error(400, "invalid node_id (hex decoded to wrong length)");
            }
            b
        } else if let Some(arr) = base58_decode(node_id_hex) {
            arr.to_vec()
        } else {
            return PluginResponse::error(400, "invalid node_id (expected 64-char hex or base58)");
        };

        let node_id = NodeId(bytes.try_into().unwrap());
        let mut list = self.peer_auth.lock().unwrap();
        list.remove_peer(&node_id);
        if let Err(e) = list.save() {
            eprintln!("warning: failed to persist peers.json: {e}");
        }

        PluginResponse::json(r#"{"ok":true}"#.to_string())
    }
}
