//! Datastore plugin adapter for the dashboard plugin system.
//!
//! Wraps `DatastoreBridge` and `DatastoreNodeFactory` into a `DashboardPlugin`
//! that the dashboard can poll for snapshots and dispatch HTTP requests to.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dashboard::plugin::{DashboardPlugin, PluginResponse};

use swactor_datastore::bridge::{DatastoreGroup, DatastoreGroupConfig};
use swactor::runtime::Runtime;

use crate::plugins::datastore::bridge_ops::BridgeOps;

/// HTML page for the datastore plugin.
const DATASTORE_HTML: &str = include_str!("datastore_page.html");

/// Dashboard plugin that exposes datastore metrics, CRUD operations, and
/// lifecycle management through the generic plugin interface.
pub struct DatastorePlugin {
    /// The bridge operations handle — either a live bridge or a factory-only state.
    state: Mutex<DatastoreState>,
}

enum DatastoreState {
    Running(BridgeOps),
    Stopped {
        runtime: Arc<Runtime>,
        default_chunk_size: u32,
    },
}

impl DatastorePlugin {
    /// Create a plugin wrapping an already-running datastore group.
    pub fn from_group(group: &DatastoreGroup, runtime: Arc<Runtime>, default_chunk_size: u32) -> Self {
        Self {
            state: Mutex::new(DatastoreState::Running(BridgeOps::from_group(
                group,
                runtime,
                default_chunk_size,
            ))),
        }
    }

    /// Create a plugin with no running datastore (factory-only mode).
    pub fn stopped(runtime: Arc<Runtime>, default_chunk_size: u32) -> Self {
        Self {
            state: Mutex::new(DatastoreState::Stopped {
                runtime,
                default_chunk_size,
            }),
        }
    }
}

impl DashboardPlugin for DatastorePlugin {
    fn name(&self) -> &str {
        "datastore"
    }

    fn snapshot_json(&self) -> Option<String> {
        let state = self.state.lock().unwrap();
        match &*state {
            DatastoreState::Running(ops) => {
                let snap_json = ops.snapshot_json().unwrap_or_else(|| "null".into());
                Some(format!(
                    r#"{{"is_running":true,"snapshot":{}}}"#,
                    snap_json
                ))
            }
            DatastoreState::Stopped { .. } => {
                Some(r#"{"is_running":false,"snapshot":null}"#.to_string())
            }
        }
    }

    fn handle_request(
        &self,
        method: &str,
        path: &str,
        query: &HashMap<String, String>,
        body: &[u8],
    ) -> PluginResponse {
        match (method, path) {
            ("GET", "list") => {
                let state = self.state.lock().unwrap();
                let ops = match &*state {
                    DatastoreState::Running(ops) => ops,
                    _ => return PluginResponse::error(503, "datastore not running"),
                };
                let scope = match query.get("scope").map(|s| s.as_str()) {
                    Some("local") => ListScope::Local,
                    _ => ListScope::Swarm,
                };
                let name_filter = query.get("name").map(|s| s.as_str());
                match ops.list_objects(name_filter, scope) {
                    Ok(json) => PluginResponse::json(json),
                    Err(e) => PluginResponse::error(500, e),
                }
            }
            ("GET", "get") => {
                let state = self.state.lock().unwrap();
                let ops = match &*state {
                    DatastoreState::Running(ops) => ops,
                    _ => return PluginResponse::error(503, "datastore not running"),
                };
                let hash = match query.get("hash") {
                    Some(h) => h.as_str(),
                    None => return PluginResponse::error(400, "missing ?hash= parameter"),
                };
                match ops.get_object(hash) {
                    Ok(json) => PluginResponse::json(json),
                    Err(e) if e.contains("not found") => PluginResponse::error(404, e),
                    Err(e) => PluginResponse::error(500, e),
                }
            }
            ("GET", "data") => {
                let state = self.state.lock().unwrap();
                let ops = match &*state {
                    DatastoreState::Running(ops) => ops,
                    _ => return PluginResponse::error(503, "datastore not running"),
                };
                let hash = match query.get("hash") {
                    Some(h) => h.as_str(),
                    None => return PluginResponse::error(400, "missing ?hash= parameter"),
                };
                match ops.get_data(hash) {
                    Ok(data) => PluginResponse::Binary {
                        content_type: "application/octet-stream".into(),
                        data,
                    },
                    Err(e) if e.contains("not found") => PluginResponse::error(404, e),
                    Err(e) => PluginResponse::error(500, e),
                }
            }
            ("GET", "status") => {
                let state = self.state.lock().unwrap();
                let ops = match &*state {
                    DatastoreState::Running(ops) => ops,
                    _ => return PluginResponse::error(503, "datastore not running"),
                };
                match ops.node_status() {
                    Ok(json) => PluginResponse::json(json),
                    Err(e) => PluginResponse::error(500, e),
                }
            }
            ("POST", "put") => {
                let state = self.state.lock().unwrap();
                let ops = match &*state {
                    DatastoreState::Running(ops) => ops,
                    _ => return PluginResponse::error(503, "datastore not running"),
                };
                let name = query.get("name").cloned();
                match ops.put_data(body.to_vec(), name) {
                    Ok(json) => PluginResponse::json(json),
                    Err(e) => PluginResponse::error(500, e),
                }
            }
            ("POST", "delete") => {
                let state = self.state.lock().unwrap();
                let ops = match &*state {
                    DatastoreState::Running(ops) => ops,
                    _ => return PluginResponse::error(503, "datastore not running"),
                };
                let hash = match query.get("hash") {
                    Some(h) => h.as_str(),
                    None => return PluginResponse::error(400, "missing ?hash= parameter"),
                };
                match ops.delete_object(hash) {
                    Ok(json) => PluginResponse::json(json),
                    Err(e) if e.contains("not found") => PluginResponse::error(404, e),
                    Err(e) => PluginResponse::error(500, e),
                }
            }
            ("POST", "start") => {
                let mut state = self.state.lock().unwrap();
                if matches!(&*state, DatastoreState::Running(_)) {
                    return PluginResponse::error(409, "datastore already running");
                }
                let (runtime, chunk_size) = match &*state {
                    DatastoreState::Stopped {
                        runtime,
                        default_chunk_size,
                    } => (Arc::clone(runtime), *default_chunk_size),
                    _ => unreachable!(),
                };
                let storage_path = query.get("storage_path").cloned();
                match start_datastore_group(&runtime, chunk_size, storage_path) {
                    Ok(ops) => {
                        *state = DatastoreState::Running(ops);
                        PluginResponse::json(r#"{"ok":true}"#.to_string())
                    }
                    Err(e) => PluginResponse::error(500, e),
                }
            }
            ("POST", "shutdown") => {
                let mut state = self.state.lock().unwrap();
                let (runtime, chunk_size) = match &*state {
                    DatastoreState::Running(ops) => {
                        let _ = ops.shutdown_datastore();
                        (Arc::clone(&ops.runtime), ops.default_chunk_size)
                    }
                    _ => return PluginResponse::error(503, "datastore not running"),
                };
                *state = DatastoreState::Stopped {
                    runtime,
                    default_chunk_size: chunk_size,
                };
                PluginResponse::json(r#"{"ok":true}"#.to_string())
            }
            _ => PluginResponse::not_found(),
        }
    }

    fn html_page(&self) -> Option<&str> {
        Some(DATASTORE_HTML)
    }
}

/// Scope filter for listing objects — mirrors the old `ListScope` from dashboard.
#[derive(Debug, Clone, Copy)]
enum ListScope {
    Local,
    Swarm,
}

/// Start a new datastore group and return bridge operations.
fn start_datastore_group(
    runtime: &Arc<Runtime>,
    chunk_size: u32,
    storage_path: Option<String>,
) -> Result<BridgeOps, String> {
    use swactor::transport::NodeId;
    use std::time::{SystemTime, UNIX_EPOCH};

    // Generate a unique node ID
    let mut bytes = [0u8; 32];
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    for (i, b) in nanos.to_le_bytes().iter().enumerate() {
        bytes[i % 32] ^= *b;
    }
    let pid = std::process::id();
    for (i, b) in pid.to_le_bytes().iter().enumerate() {
        bytes[i + 16] ^= *b;
    }
    let node_id = NodeId(bytes);
    let node_hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();

    let group = DatastoreGroup::spawn(
        Arc::clone(runtime),
        DatastoreGroupConfig {
            node_id,
            node_id_hex: node_hex,
            chunk_size,
            storage_path,
            auth: None,
            gc_interval: 1000,
            disseminate_interval: 50,
        },
    )?;

    Ok(BridgeOps::from_group(&group, Arc::clone(runtime), chunk_size))
}

/// Internal module that extracts the bridge operations from `DatastoreBridge`.
///
/// Instead of depending on the `DatastoreStatsProvider` trait (which we're
/// removing from the dashboard), we replicate the operations directly using
/// the datastore actor addresses and runtime.
mod bridge_ops {
    use super::*;
    use std::collections::BTreeMap;
    use std::thread;
    use std::time::{Duration, Instant};

    use swactor::actor::ActorAddress;
    use swactor::runtime::Inbox;
    use swactor_datastore::chunking::reassemble_blob;
    use swactor_datastore::messages::{DatastoreNodeMsg, DatastoreResponse, MetadataMsg};
    use swactor_datastore::metrics::DatastoreMetrics;
    use swactor_datastore::types::ContentHash;

    const POLL_TIMEOUT: Duration = Duration::from_secs(5);
    const POLL_INTERVAL: Duration = Duration::from_millis(1);

    fn poll_response(
        inbox: &Inbox<DatastoreResponse>,
        timeout: Duration,
    ) -> Option<DatastoreResponse> {
        let start = Instant::now();
        loop {
            if let Some(resp) = inbox.try_recv() {
                return Some(resp);
            }
            if start.elapsed() > timeout {
                return None;
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn entry_to_json(entry: &swactor_datastore::types::ObjectEntry) -> serde_json::Value {
        let node_hex: String = entry.node_id.0.iter().map(|b| format!("{b:02x}")).collect();
        serde_json::json!({
            "content_hash": entry.content_hash.to_hex(),
            "name": entry.name,
            "node_id": node_hex,
            "tags": entry.tags,
            "size_bytes": entry.size_bytes,
            "created_at": entry.created_at,
        })
    }

    fn manifest_to_json(manifest: &swactor_datastore::types::ObjectManifest) -> serde_json::Value {
        let chunks: Vec<serde_json::Value> = manifest
            .chunks
            .iter()
            .map(|c| {
                serde_json::json!({
                    "hash": c.hash.to_hex(),
                    "offset": c.offset,
                    "size": c.size,
                })
            })
            .collect();
        serde_json::json!({
            "content_hash": manifest.content_hash.to_hex(),
            "chunks": chunks,
            "total_size": manifest.total_size,
            "chunk_size": manifest.chunk_size,
            "content_type": manifest.content_type,
        })
    }

    fn entries_to_json(entries: &[swactor_datastore::types::ObjectEntry]) -> Vec<serde_json::Value> {
        entries.iter().map(entry_to_json).collect()
    }

    pub struct BridgeOps {
        metrics: Arc<DatastoreMetrics>,
        pub runtime: Arc<Runtime>,
        datastore_addr: ActorAddress,
        metadata_addr: ActorAddress,
        pub default_chunk_size: u32,
    }

    impl BridgeOps {
        pub fn from_group(
            group: &DatastoreGroup,
            runtime: Arc<Runtime>,
            default_chunk_size: u32,
        ) -> Self {
            let datastore_addr = group.datastore_addr();
            let metadata_addr = group.metadata_addr();
            let metrics = Arc::new(DatastoreMetrics::new());

            Self {
                metrics,
                runtime,
                datastore_addr,
                metadata_addr,
                default_chunk_size,
            }
        }

        pub fn snapshot_json(&self) -> Option<String> {
            let snap = self.metrics.snapshot();
            serde_json::to_string(&snap).ok()
        }

        pub fn list_objects(
            &self,
            name_filter: Option<&str>,
            scope: super::ListScope,
        ) -> Result<String, String> {
            let inbox = self
                .runtime
                .new_inbox::<DatastoreResponse>()
                .map_err(|e| format!("failed to create inbox: {e}"))?;

            match scope {
                super::ListScope::Local => {
                    let _ = self.runtime.send_to(
                        self.datastore_addr,
                        DatastoreNodeMsg::List {
                            name_filter: name_filter.map(|s| s.to_string()),
                            all: false,
                            reply_to: *inbox.addr(),
                        },
                    );
                }
                super::ListScope::Swarm => {
                    let _ = self.runtime.send_to(
                        self.metadata_addr,
                        MetadataMsg::ListLocal {
                            name_filter: name_filter.map(|s| s.to_string()),
                            reply_to: *inbox.addr(),
                        },
                    );
                }
            }

            match poll_response(&inbox, POLL_TIMEOUT) {
                Some(DatastoreResponse::ListOk { entries }) => {
                    let json =
                        serde_json::json!({ "entries": entries_to_json(&entries) }).to_string();
                    Ok(json)
                }
                Some(DatastoreResponse::Error { reason }) => Err(reason),
                _ => Err("timeout".into()),
            }
        }

        pub fn get_object(&self, hash: &str) -> Result<String, String> {
            let content_hash = ContentHash::from_hex(hash)
                .ok_or_else(|| "invalid content hash hex".to_string())?;

            let inbox = self
                .runtime
                .new_inbox::<DatastoreResponse>()
                .map_err(|e| format!("failed to create inbox: {e}"))?;

            let _ = self.runtime.send_to(
                self.datastore_addr,
                DatastoreNodeMsg::Get {
                    content_hash,
                    reply_to: *inbox.addr(),
                },
            );

            match poll_response(&inbox, POLL_TIMEOUT) {
                Some(DatastoreResponse::GetOk { entry, manifest }) => {
                    self.metrics.record_get(&content_hash.to_hex());
                    let json = serde_json::json!({
                        "entry": entry_to_json(&entry),
                        "manifest": manifest_to_json(&manifest),
                    })
                    .to_string();
                    Ok(json)
                }
                Some(DatastoreResponse::NotFound) => Err("not found".into()),
                Some(DatastoreResponse::Error { reason }) => Err(reason),
                _ => Err("timeout".into()),
            }
        }

        pub fn get_data(&self, hash: &str) -> Result<Vec<u8>, String> {
            let content_hash = ContentHash::from_hex(hash)
                .ok_or_else(|| "invalid content hash hex".to_string())?;

            let inbox = self
                .runtime
                .new_inbox::<DatastoreResponse>()
                .map_err(|e| format!("failed to create inbox: {e}"))?;

            let _ = self.runtime.send_to(
                self.datastore_addr,
                DatastoreNodeMsg::Get {
                    content_hash,
                    reply_to: *inbox.addr(),
                },
            );

            self.metrics.record_get(&content_hash.to_hex());

            let manifest = match poll_response(&inbox, POLL_TIMEOUT) {
                Some(DatastoreResponse::GetOk { manifest, .. }) => manifest,
                Some(DatastoreResponse::NotFound) => return Err("not found".into()),
                Some(DatastoreResponse::Error { reason }) => return Err(reason),
                _ => return Err("timeout".into()),
            };

            let mut chunk_data = Vec::new();
            for chunk_ref in &manifest.chunks {
                let chunk_inbox = self
                    .runtime
                    .new_inbox::<DatastoreResponse>()
                    .map_err(|e| format!("failed to create inbox: {e}"))?;

                let _ = self.runtime.send_to(
                    self.datastore_addr,
                    DatastoreNodeMsg::ReadChunk {
                        hash: chunk_ref.hash,
                        reply_to: *chunk_inbox.addr(),
                    },
                );

                match poll_response(&chunk_inbox, POLL_TIMEOUT) {
                    Some(DatastoreResponse::ChunkOk { hash, data }) => {
                        chunk_data.push((hash, data));
                    }
                    _ => return Err("failed to read chunk".into()),
                }
            }

            reassemble_blob(&manifest, &chunk_data)
                .map_err(|e| format!("reassembly failed: {e:?}"))
        }

        pub fn put_data(&self, data: Vec<u8>, name: Option<String>) -> Result<String, String> {
            let body_len = data.len();

            let inbox = self
                .runtime
                .new_inbox::<DatastoreResponse>()
                .map_err(|e| format!("failed to create inbox: {e}"))?;

            let _ = self.runtime.send_to(
                self.datastore_addr,
                DatastoreNodeMsg::Put {
                    data,
                    name: name.clone(),
                    tags: BTreeMap::new(),
                    reply_to: *inbox.addr(),
                },
            );

            match poll_response(&inbox, POLL_TIMEOUT) {
                Some(DatastoreResponse::PutOk { content_hash }) => {
                    let hex = content_hash.to_hex();
                    self.metrics
                        .record_put(&hex, name.as_deref(), body_len as u64);
                    let json = serde_json::json!({ "content_hash": hex }).to_string();
                    Ok(json)
                }
                Some(DatastoreResponse::Error { reason }) => Err(reason),
                _ => Err("timeout waiting for put response".into()),
            }
        }

        pub fn delete_object(&self, hash: &str) -> Result<String, String> {
            let content_hash = ContentHash::from_hex(hash)
                .ok_or_else(|| "invalid content hash hex".to_string())?;

            let inbox = self
                .runtime
                .new_inbox::<DatastoreResponse>()
                .map_err(|e| format!("failed to create inbox: {e}"))?;

            let _ = self.runtime.send_to(
                self.datastore_addr,
                DatastoreNodeMsg::Delete {
                    content_hash,
                    reply_to: *inbox.addr(),
                },
            );

            match poll_response(&inbox, POLL_TIMEOUT) {
                Some(DatastoreResponse::DeleteOk { content_hash }) => {
                    let hex = content_hash.to_hex();
                    self.metrics.record_delete(&hex, 0);
                    let json = serde_json::json!({ "content_hash": hex }).to_string();
                    Ok(json)
                }
                Some(DatastoreResponse::NotFound) => Err("not found".into()),
                Some(DatastoreResponse::Error { reason }) => Err(reason),
                _ => Err("timeout".into()),
            }
        }

        pub fn node_status(&self) -> Result<String, String> {
            let inbox = self
                .runtime
                .new_inbox::<DatastoreResponse>()
                .map_err(|e| format!("failed to create inbox: {e}"))?;

            let _ = self.runtime.send_to(
                self.datastore_addr,
                DatastoreNodeMsg::Status {
                    reply_to: *inbox.addr(),
                },
            );

            match poll_response(&inbox, POLL_TIMEOUT) {
                Some(DatastoreResponse::NodeStatus { node_id }) => {
                    let hex: String = node_id.0.iter().map(|b| format!("{b:02x}")).collect();
                    let json = serde_json::json!({ "node_id": hex }).to_string();
                    Ok(json)
                }
                _ => Err("timeout".into()),
            }
        }

        pub fn shutdown_datastore(&self) -> Result<(), String> {
            Ok(())
        }
    }
}
