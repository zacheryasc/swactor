//! Datastore actor group lifecycle management.
//!
//! `DatastoreGroup` owns the full lifecycle of a datastore actor group:
//! BlobStore, Metadata, DatastoreNode, and optional GatewayActor.

use std::path::PathBuf;
use std::sync::Arc;

use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;
use swactor::std::RuntimeNaming;

use swactor::transport::NodeId;

use crate::actors::{BlobStoreActor, DatastoreNode, GatewayActor, MetadataActor};
use crate::auth::{AccessControlList, AuthzEngine};
use crate::messages::{DatastoreNodeMsg, GatewayMsg, MetadataMsg};
use crate::metrics::DatastoreMetrics;
use crate::storage::{FilesystemBackend, InMemoryBackend};
use crate::types::DatastoreConfig;

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
    metrics: Arc<DatastoreMetrics>,
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
            metrics,
            gc_interval: config.gc_interval,
            disseminate_interval: config.disseminate_interval,
            runtime,
        })
    }

    /// Send periodic ticks to the datastore actors based on the current round.
    pub fn tick(&self, round: u64) {
        if round.is_multiple_of(self.gc_interval) {
            let _ = self.runtime.send_to(self.metadata_addr, MetadataMsg::GcTick);
            if let Some(gw) = self.gateway_addr {
                let _ = self.runtime.send_to(gw, GatewayMsg::NonceGcTick);
            }
        }
        if round.is_multiple_of(self.disseminate_interval) {
            let _ = self
                .runtime
                .send_to(self.metadata_addr, MetadataMsg::DisseminateTick);
        }
    }

    /// Access the metrics accumulator.
    pub fn metrics(&self) -> &Arc<DatastoreMetrics> {
        &self.metrics
    }

    /// Address of the DatastoreNode actor.
    pub fn datastore_addr(&self) -> ActorAddress {
        self.datastore_addr
    }

    /// Address of the MetadataActor.
    pub fn metadata_addr(&self) -> ActorAddress {
        self.metadata_addr
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
        use swactor::std::RuntimeNaming;
        if let Ok(addr) = self.runtime.spawn(StreamListener::new(self.datastore_addr, stream_manager)) {
            let _ = self.runtime.register_name("StreamListener", addr);
        }
    }
}
