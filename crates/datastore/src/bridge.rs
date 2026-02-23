//! Bridge between the runtime dashboard's `DatastoreStatsProvider` trait and
//! the datastore actor system. Allows the dashboard to perform CRUD operations
//! and lifecycle management without depending on `swactor-datastore` types.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use swactor::actor::ActorAddress;
use swactor::runtime::{Inbox, Runtime};
use swactor_std::RuntimeNaming;

use distribution::types::NodeId;
use dashboard::datastore_collector::{
    DatastoreFactory, DatastoreStatsProvider, ListScope,
};

use crate::actors::{BlobStoreActor, DatastoreNode, GatewayActor, MetadataActor};
use crate::auth::{AccessControlList, AuthzEngine};
use crate::chunking::reassemble_blob;
use crate::messages::{DatastoreNodeMsg, DatastoreResponse, GatewayMsg, MetadataMsg};
use crate::metrics::DatastoreMetrics;
use crate::storage::{FilesystemBackend, InMemoryBackend};
use crate::types::{ContentHash, DatastoreConfig};

const POLL_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(1);

fn poll_response(inbox: &Inbox<DatastoreResponse>, timeout: Duration) -> Option<DatastoreResponse> {
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

fn entry_to_json(entry: &crate::types::ObjectEntry) -> serde_json::Value {
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

fn manifest_to_json(manifest: &crate::types::ObjectManifest) -> serde_json::Value {
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

fn entries_to_json(entries: &[crate::types::ObjectEntry]) -> Vec<serde_json::Value> {
    entries.iter().map(entry_to_json).collect()
}

/// Bridges the dashboard trait to the datastore actor system.
pub struct DatastoreBridge {
    metrics: Arc<DatastoreMetrics>,
    runtime: Arc<Runtime>,
    datastore_addr: ActorAddress,
    metadata_addr: ActorAddress,
    #[allow(dead_code)]
    blob_store_addr: ActorAddress,
}

impl DatastoreBridge {
    pub fn new(
        metrics: Arc<DatastoreMetrics>,
        runtime: Arc<Runtime>,
        datastore_addr: ActorAddress,
        metadata_addr: ActorAddress,
        blob_store_addr: ActorAddress,
    ) -> Self {
        Self {
            metrics,
            runtime,
            datastore_addr,
            metadata_addr,
            blob_store_addr,
        }
    }
}

impl DatastoreStatsProvider for DatastoreBridge {
    fn snapshot_json(&self) -> Option<String> {
        let snap = self.metrics.snapshot();
        serde_json::to_string(&snap).ok()
    }

    fn is_running(&self) -> bool {
        true
    }

    fn list_objects(&self, name_filter: Option<&str>, scope: ListScope) -> Result<String, String> {
        let inbox = self.runtime.new_inbox::<DatastoreResponse>()
            .map_err(|e| format!("failed to create inbox: {e}"))?;

        match scope {
            ListScope::Local => {
                let _ = self.runtime.send_to(
                    self.datastore_addr,
                    DatastoreNodeMsg::List {
                        name_filter: name_filter.map(|s| s.to_string()),
                        all: false,
                        reply_to: *inbox.addr(),
                    },
                );
            }
            ListScope::Swarm => {
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
                let json = serde_json::json!({ "entries": entries_to_json(&entries) }).to_string();
                Ok(json)
            }
            Some(DatastoreResponse::Error { reason }) => Err(reason),
            _ => Err("timeout".into()),
        }
    }

    fn get_object(&self, hash: &str) -> Result<String, String> {
        let content_hash = ContentHash::from_hex(hash)
            .ok_or_else(|| "invalid content hash hex".to_string())?;

        let inbox = self.runtime.new_inbox::<DatastoreResponse>()
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

    fn get_data(&self, hash: &str) -> Result<Vec<u8>, String> {
        let content_hash = ContentHash::from_hex(hash)
            .ok_or_else(|| "invalid content hash hex".to_string())?;

        // Get manifest
        let inbox = self.runtime.new_inbox::<DatastoreResponse>()
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

        // Read chunks
        let mut chunk_data = Vec::new();
        for chunk_ref in &manifest.chunks {
            let chunk_inbox = self.runtime.new_inbox::<DatastoreResponse>()
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

    fn put_data(&self, data: Vec<u8>, name: Option<String>) -> Result<String, String> {
        let body_len = data.len();

        let inbox = self.runtime.new_inbox::<DatastoreResponse>()
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
                self.metrics.record_put(&hex, name.as_deref(), body_len as u64);
                let json = serde_json::json!({ "content_hash": hex }).to_string();
                Ok(json)
            }
            Some(DatastoreResponse::Error { reason }) => Err(reason),
            _ => Err("timeout waiting for put response".into()),
        }
    }

    fn delete_object(&self, hash: &str) -> Result<String, String> {
        let content_hash = ContentHash::from_hex(hash)
            .ok_or_else(|| "invalid content hash hex".to_string())?;

        let inbox = self.runtime.new_inbox::<DatastoreResponse>()
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

    fn node_status(&self) -> Result<String, String> {
        let inbox = self.runtime.new_inbox::<DatastoreResponse>()
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

    fn shutdown_datastore(&self) -> Result<(), String> {
        // We can't actually stop the actors from here without a runtime handle,
        // but we can signal shutdown. The caller (server handler) clears the
        // provider reference which effectively disables the datastore.
        Ok(())
    }
}

/// Factory that can spawn a new set of datastore actors on a shared runtime.
pub struct DatastoreNodeFactory {
    runtime: Arc<Runtime>,
    default_chunk_size: u32,
}

impl DatastoreNodeFactory {
    pub fn new(runtime: Arc<Runtime>, default_chunk_size: u32) -> Self {
        Self {
            runtime,
            default_chunk_size,
        }
    }
}

impl DatastoreFactory for DatastoreNodeFactory {
    fn start_datastore(
        &self,
        storage_path: Option<String>,
    ) -> Result<Arc<dyn DatastoreStatsProvider>, String> {
        // Generate a unique node ID
        let node_id = generate_node_id();
        let node_hex: String = node_id.0.iter().map(|b| format!("{b:02x}")).collect();

        let group = DatastoreGroup::spawn(
            Arc::clone(&self.runtime),
            DatastoreGroupConfig {
                node_id,
                node_id_hex: node_hex,
                chunk_size: self.default_chunk_size,
                storage_path,
                auth: None,
                gc_interval: 1000,
                disseminate_interval: 50,
            },
        )?;

        Ok(group.bridge().clone())
    }
}

// ─── DatastoreGroup ─────────────────────────────────────────────────────────

/// Configuration for spawning a complete datastore actor group.
pub struct DatastoreGroupConfig {
    pub node_id: NodeId,
    pub node_id_hex: String,
    pub chunk_size: u32,
    pub storage_path: Option<String>,
    pub auth: Option<DatastoreAuthConfig>,
    pub gc_interval: u64,
    pub disseminate_interval: u64,
}

/// Auth configuration for the datastore gateway.
pub struct DatastoreAuthConfig {
    pub auth_dir: PathBuf,
}

/// Owns the full lifecycle of a datastore actor group: BlobStore, Metadata,
/// DatastoreNode, and optional GatewayActor.
pub struct DatastoreGroup {
    datastore_addr: ActorAddress,
    metadata_addr: ActorAddress,
    gateway_addr: Option<ActorAddress>,
    bridge: Arc<dyn DatastoreStatsProvider>,
    gc_interval: u64,
    disseminate_interval: u64,
    runtime: Arc<Runtime>,
}

impl DatastoreGroup {
    /// Spawn all datastore actors, wire them together, and register names.
    pub fn spawn(runtime: Arc<Runtime>, config: DatastoreGroupConfig) -> Result<Self, String> {
        let ds_config = DatastoreConfig {
            chunk_size: config.chunk_size,
            storage_path: config
                .storage_path
                .as_ref()
                .map(|s| s.into())
                .unwrap_or_else(|| "datastore".into()),
            ..Default::default()
        };

        let backend: Box<dyn crate::StorageBackend> = match &config.storage_path {
            Some(path) => {
                let p = PathBuf::from(path);
                std::fs::create_dir_all(&p)
                    .map_err(|e| format!("failed to create storage directory: {e}"))?;
                Box::new(FilesystemBackend::new(p))
            }
            None => Box::new(InMemoryBackend::new()),
        };

        let blob_store_addr = runtime
            .spawn(BlobStoreActor::new(backend))
            .map_err(|e| format!("failed to spawn BlobStoreActor: {e}"))?;
        let _ = runtime.register_name("BlobStore", blob_store_addr);

        let mut metadata = MetadataActor::new(config.node_id, &ds_config);
        metadata.set_blob_store(blob_store_addr);
        let metadata_addr = runtime
            .spawn(metadata)
            .map_err(|e| format!("failed to spawn MetadataActor: {e}"))?;
        let _ = runtime.register_name("Metadata", metadata_addr);

        let datastore_node =
            DatastoreNode::new(config.node_id, blob_store_addr, metadata_addr, ds_config);
        let datastore_addr = runtime
            .spawn(datastore_node)
            .map_err(|e| format!("failed to spawn DatastoreNode: {e}"))?;
        let _ = runtime.register_name("Datastore", datastore_addr);

        // Spawn GatewayActor if auth is configured
        let gateway_addr = if let Some(auth_cfg) = &config.auth {
            std::fs::create_dir_all(&auth_cfg.auth_dir)
                .map_err(|e| format!("failed to create auth directory: {e}"))?;
            let acl_path = auth_cfg.auth_dir.join("acl.json");
            let acl = AccessControlList::load_or_create(&acl_path, config.node_id)
                .map_err(|e| format!("failed to load/create ACL: {e}"))?;
            let engine = AuthzEngine::new(acl);
            let gateway = GatewayActor::new(engine, datastore_addr, Some(acl_path));
            let addr = runtime
                .spawn(gateway)
                .map_err(|e| format!("failed to spawn GatewayActor: {e}"))?;
            let _ = runtime.register_name("Gateway", addr);
            eprintln!("Auth: enabled (owner {})", &config.node_id_hex[..16]);
            Some(addr)
        } else {
            eprintln!("Auth: disabled");
            None
        };

        let metrics = Arc::new(DatastoreMetrics::new());
        metrics.set_node_id(config.node_id_hex.clone());

        let bridge: Arc<dyn DatastoreStatsProvider> = Arc::new(DatastoreBridge::new(
            metrics,
            Arc::clone(&runtime),
            datastore_addr,
            metadata_addr,
            blob_store_addr,
        ));

        if config.storage_path.is_some() {
            eprintln!(
                "Datastore: persistent ({})",
                config.storage_path.as_ref().unwrap()
            );
        } else {
            eprintln!("Datastore: in-memory");
        }

        Ok(Self {
            datastore_addr,
            metadata_addr,
            gateway_addr,
            bridge,
            gc_interval: config.gc_interval,
            disseminate_interval: config.disseminate_interval,
            runtime,
        })
    }

    /// Send periodic ticks to the datastore actors based on the current round.
    pub fn tick(&self, round: u64) {
        if round % self.gc_interval == 0 {
            let _ = self.runtime.send_to(self.metadata_addr, MetadataMsg::GcTick);
            if let Some(gw) = self.gateway_addr {
                let _ = self.runtime.send_to(gw, GatewayMsg::NonceGcTick);
            }
        }
        if round % self.disseminate_interval == 0 {
            let _ = self
                .runtime
                .send_to(self.metadata_addr, MetadataMsg::DisseminateTick);
        }
    }

    /// Access the bridge (as a trait object for the dashboard).
    pub fn bridge(&self) -> &Arc<dyn DatastoreStatsProvider> {
        &self.bridge
    }

    /// Configure stream support: sends ConfigureStreams to DatastoreNode and
    /// spawns a StreamListener actor.
    pub fn configure_streams(
        &self,
        stream_manager: ActorAddress,
        tokio_handle: tokio::runtime::Handle,
    ) {
        let _ = self.runtime.send_to(
            self.datastore_addr,
            DatastoreNodeMsg::ConfigureStreams {
                stream_manager,
                tokio_handle,
                runtime: Arc::clone(&self.runtime),
            },
        );

        // Spawn StreamListener
        use crate::actors::stream_listener::StreamListener;
        use swactor_std::RuntimeNaming;
        if let Ok(addr) = self.runtime.spawn(StreamListener::new(self.datastore_addr, stream_manager)) {
            let _ = self.runtime.register_name("StreamListener", addr);
        }
    }
}

fn generate_node_id() -> NodeId {
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
    NodeId(bytes)
}
