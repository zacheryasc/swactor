//! Shared test harness for datastore actor tests.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;

use swactor::actor::{ActorAddress, Message};
use swactor::runtime::{Inbox, Runtime, RuntimeConfig};
use swactor_std::StdExtension;

use swactor_datastore::chunking::chunk_blob;
use swactor_datastore::messages::{BlobStoreMsg, DatastoreResponse, MetadataMsg};
use swactor_datastore::storage::InMemoryBackend;
use swactor_datastore::types::{ChunkRef, ContentHash, DatastoreConfig, ObjectEntry, ObjectManifest};
use swactor_datastore::{BlobStoreActor, DatastoreNode, MetadataActor, TransferActor};

use distribution::types::NodeId;

/// Create a single-threaded runtime with StdExtension.
pub fn test_runtime() -> Runtime {
    Runtime::new(RuntimeConfig::default()).with_extension(Arc::new(StdExtension::new()))
}

/// Tick exactly `n` times.
pub fn tick_n(rt: &Runtime, n: usize) {
    for _ in 0..n {
        rt.tick();
    }
}

/// Tick up to `max` times, returning as soon as `inbox` has a message.
pub fn tick_until_recv<M: Message>(rt: &Runtime, inbox: &Inbox<M>, max: usize) -> Option<M> {
    for _ in 0..max {
        rt.tick();
        if let Some(msg) = inbox.try_recv() {
            return Some(msg);
        }
    }
    None
}

/// Tick `n` times, then drain all messages from the inbox.
pub fn tick_and_drain<M: Message>(rt: &Runtime, inbox: &Inbox<M>, ticks: usize) -> Vec<M> {
    for _ in 0..ticks {
        rt.tick();
    }
    std::iter::from_fn(|| inbox.try_recv()).collect()
}

/// Spawn a BlobStoreActor backed by InMemoryBackend.
pub fn spawn_blob_store(rt: &Runtime) -> ActorAddress {
    rt.spawn(BlobStoreActor::new(Box::new(InMemoryBackend::new())))
        .unwrap()
}

/// Spawn a TransferActor wired to the given BlobStoreActor.
pub fn spawn_transfer(rt: &Runtime, blob_store_addr: ActorAddress) -> ActorAddress {
    rt.spawn(TransferActor::new(blob_store_addr)).unwrap()
}

/// Spawn a MetadataActor with the given node_id and default config.
pub fn spawn_metadata(rt: &Runtime, node_id: NodeId) -> ActorAddress {
    let config = DatastoreConfig::default();
    rt.spawn(MetadataActor::new(node_id, &config)).unwrap()
}

/// A test node ID.
pub fn test_node_id() -> NodeId {
    NodeId([0x42; 32])
}

/// Create a simple ObjectEntry for testing.
pub fn make_entry(data: &[u8], name: Option<&str>) -> ObjectEntry {
    ObjectEntry {
        content_hash: ContentHash::of(data),
        name: name.map(|s| s.to_string()),
        node_id: test_node_id(),
        tags: BTreeMap::new(),
        size_bytes: data.len() as u64,
        created_at: 0,
    }
}

/// Create a single-chunk ObjectManifest for testing.
pub fn make_manifest(data: &[u8]) -> ObjectManifest {
    let content_hash = ContentHash::of(data);
    ObjectManifest {
        content_hash,
        chunks: vec![ChunkRef {
            hash: content_hash,
            offset: 0,
            size: data.len() as u32,
        }],
        total_size: data.len() as u64,
        chunk_size: data.len() as u32,
        content_type: None,
    }
}

/// Full lifecycle harness: spawns both BlobStore and Metadata actors.
pub struct DatastoreHarness {
    pub rt: Runtime,
    pub blob_store: ActorAddress,
    pub metadata: ActorAddress,
    pub inbox: Inbox<DatastoreResponse>,
    pub node_id: NodeId,
}

impl DatastoreHarness {
    pub fn new() -> Self {
        let rt = test_runtime();
        let blob_store = spawn_blob_store(&rt);
        let node_id = test_node_id();
        let metadata = spawn_metadata(&rt, node_id);
        let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();

        // Let actors initialize.
        tick_n(&rt, 2);

        Self {
            rt,
            blob_store,
            metadata,
            inbox,
            node_id,
        }
    }

    pub fn reply_addr(&self) -> ActorAddress {
        *self.inbox.addr()
    }

    /// Store a blob: chunk it, store chunks via BlobStoreActor, store metadata via MetadataActor.
    pub fn put_blob(
        &self,
        data: &[u8],
        name: Option<&str>,
    ) -> ContentHash {
        let (content_hash, manifest, chunks) = chunk_blob(data, 1_048_576);

        // Store all chunks.
        for (hash, chunk_data) in &chunks {
            self.rt
                .send_to(
                    self.blob_store,
                    BlobStoreMsg::WriteChunk {
                        hash: *hash,
                        data: chunk_data.clone(),
                        reply_to: self.reply_addr(),
                    },
                )
                .unwrap();
        }
        // Let chunk writes complete.
        tick_n(&self.rt, 3);
        // Drain chunk stored responses.
        let _ = tick_and_drain(&self.rt, &self.inbox, 2);

        // Store manifest via BlobStoreActor.
        self.rt
            .send_to(
                self.blob_store,
                BlobStoreMsg::WriteManifest {
                    manifest: manifest.clone(),
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        tick_n(&self.rt, 2);
        let _ = tick_and_drain(&self.rt, &self.inbox, 1);

        // Store metadata via MetadataActor.
        let entry = ObjectEntry {
            content_hash,
            name: name.map(|s| s.to_string()),
            node_id: self.node_id,
            tags: BTreeMap::new(),
            size_bytes: data.len() as u64,
            created_at: 0,
        };
        self.rt
            .send_to(
                self.metadata,
                MetadataMsg::PutObject {
                    entry,
                    manifest,
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        tick_n(&self.rt, 2);
        // Drain PutOk.
        let responses = tick_and_drain(&self.rt, &self.inbox, 1);
        assert!(
            responses.iter().any(|r| matches!(r, DatastoreResponse::PutOk { .. })),
            "expected PutOk response"
        );

        content_hash
    }

    /// Read blob data back through the actor system.
    pub fn get_blob(&self, content_hash: &ContentHash) -> Option<Vec<u8>> {
        // Ask metadata actor for the manifest.
        self.rt
            .send_to(
                self.metadata,
                MetadataMsg::GetObject {
                    content_hash: *content_hash,
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();

        let resp = tick_until_recv(&self.rt, &self.inbox, 10)?;
        let manifest = match resp {
            DatastoreResponse::GetOk { manifest, .. } => manifest,
            DatastoreResponse::NotFound => return None,
            other => panic!("unexpected response: {other:?}"),
        };

        // Read each chunk.
        let mut data = Vec::new();
        for chunk_ref in &manifest.chunks {
            self.rt
                .send_to(
                    self.blob_store,
                    BlobStoreMsg::ReadChunk {
                        hash: chunk_ref.hash,
                        reply_to: self.reply_addr(),
                    },
                )
                .unwrap();
            let resp = tick_until_recv(&self.rt, &self.inbox, 10).unwrap();
            match resp {
                DatastoreResponse::ChunkOk { data: chunk_data, .. } => {
                    data.extend_from_slice(&chunk_data);
                }
                other => panic!("unexpected chunk response: {other:?}"),
            }
        }
        Some(data)
    }

    /// Delete an object by content hash.
    pub fn delete_blob(&self, content_hash: &ContentHash) -> DatastoreResponse {
        self.rt
            .send_to(
                self.metadata,
                MetadataMsg::DeleteObject {
                    content_hash: *content_hash,
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        tick_until_recv(&self.rt, &self.inbox, 10).unwrap()
    }

    /// List local objects with optional name filter.
    pub fn list_local(&self, name_filter: Option<&str>) -> Vec<ObjectEntry> {
        self.rt
            .send_to(
                self.metadata,
                MetadataMsg::ListLocal {
                    name_filter: name_filter.map(|s| s.to_string()),
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        let resp = tick_until_recv(&self.rt, &self.inbox, 10).unwrap();
        match resp {
            DatastoreResponse::ListOk { entries } => entries,
            other => panic!("unexpected list response: {other:?}"),
        }
    }
}

/// Spawn a MetadataActor with blob_store_addr wired up before spawning.
pub fn spawn_metadata_with_config(
    rt: &Runtime,
    node_id: NodeId,
    config: &DatastoreConfig,
    blob_store: ActorAddress,
) -> ActorAddress {
    let mut meta = MetadataActor::new(node_id, config);
    meta.set_blob_store(blob_store);
    rt.spawn(meta).unwrap()
}

// ─── GcHarness ──────────────────────────────────────────────────────────────

/// Test harness for garbage collection scenarios.
/// Uses gc_interval=3, chunk_size=64 so GC triggers every 3 ticks
/// and 200-byte blobs produce multiple chunks.
pub struct GcHarness {
    pub rt: Runtime,
    pub blob_store: ActorAddress,
    pub metadata: ActorAddress,
    pub inbox: Inbox<DatastoreResponse>,
    pub node_id: NodeId,
    pub chunk_size: u32,
}

impl GcHarness {
    pub fn new() -> Self {
        let rt = test_runtime();
        let blob_store = spawn_blob_store(&rt);
        let node_id = test_node_id();

        let mut config = DatastoreConfig::default();
        config.gc_interval = 3;
        config.chunk_size = 64;

        let metadata = spawn_metadata_with_config(&rt, node_id, &config, blob_store);
        let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();

        tick_n(&rt, 2);

        Self {
            rt,
            blob_store,
            metadata,
            inbox,
            node_id,
            chunk_size: 64,
        }
    }

    pub fn reply_addr(&self) -> ActorAddress {
        *self.inbox.addr()
    }

    /// Chunk a blob, store all chunks in BlobStore, store entry+manifest in Metadata.
    pub fn put_blob(&self, data: &[u8], name: Option<&str>) -> ContentHash {
        let (content_hash, manifest, chunks) = chunk_blob(data, self.chunk_size);

        // Store all chunks.
        for (hash, chunk_data) in &chunks {
            self.rt
                .send_to(
                    self.blob_store,
                    BlobStoreMsg::WriteChunk {
                        hash: *hash,
                        data: chunk_data.clone(),
                        reply_to: self.reply_addr(),
                    },
                )
                .unwrap();
        }
        tick_n(&self.rt, 3);
        let _ = tick_and_drain(&self.rt, &self.inbox, 2);

        // Store manifest via BlobStoreActor.
        self.rt
            .send_to(
                self.blob_store,
                BlobStoreMsg::WriteManifest {
                    manifest: manifest.clone(),
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        tick_n(&self.rt, 2);
        let _ = tick_and_drain(&self.rt, &self.inbox, 1);

        // Store metadata via MetadataActor.
        let entry = ObjectEntry {
            content_hash,
            name: name.map(|s| s.to_string()),
            node_id: self.node_id,
            tags: BTreeMap::new(),
            size_bytes: data.len() as u64,
            created_at: 0,
        };
        self.rt
            .send_to(
                self.metadata,
                MetadataMsg::PutObject {
                    entry,
                    manifest,
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        tick_n(&self.rt, 2);
        let responses = tick_and_drain(&self.rt, &self.inbox, 1);
        assert!(
            responses
                .iter()
                .any(|r| matches!(r, DatastoreResponse::PutOk { .. })),
            "expected PutOk response"
        );

        content_hash
    }

    /// Delete an object by content hash.
    pub fn delete_blob(&self, content_hash: &ContentHash) {
        self.rt
            .send_to(
                self.metadata,
                MetadataMsg::DeleteObject {
                    content_hash: *content_hash,
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        let resp = tick_until_recv(&self.rt, &self.inbox, 10).unwrap();
        assert!(
            matches!(resp, DatastoreResponse::DeleteOk { .. }),
            "expected DeleteOk, got {resp:?}"
        );
    }

    /// Send `n` GcTick messages, ticking the runtime between each to allow
    /// the two-hop flow: GcTick → MetadataActor → GcUnreferenced → BlobStoreActor.
    pub fn gc_ticks(&self, n: usize) {
        for _ in 0..n {
            self.rt
                .send_to(self.metadata, MetadataMsg::GcTick)
                .unwrap();
            tick_n(&self.rt, 3);
        }
    }

    /// Query BlobStore for all chunk hashes.
    pub fn list_chunks(&self) -> Vec<ContentHash> {
        self.rt
            .send_to(
                self.blob_store,
                BlobStoreMsg::ListChunks {
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        let resp = tick_until_recv(&self.rt, &self.inbox, 10).unwrap();
        match resp {
            DatastoreResponse::ChunkList { hashes } => hashes,
            other => panic!("expected ChunkList, got {other:?}"),
        }
    }

    /// Check whether a specific chunk exists in BlobStore.
    pub fn has_chunk(&self, hash: &ContentHash) -> bool {
        self.rt
            .send_to(
                self.blob_store,
                BlobStoreMsg::HasChunk {
                    hash: *hash,
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        let resp = tick_until_recv(&self.rt, &self.inbox, 10).unwrap();
        match resp {
            DatastoreResponse::Bool(b) => b,
            other => panic!("expected Bool, got {other:?}"),
        }
    }
}

// ─── NodeHarness (DatastoreNode coordinator) ────────────────────────────────

/// Spawn a DatastoreNode wired to the given BlobStore and Metadata actors.
pub fn spawn_datastore_node(
    rt: &Runtime,
    node_id: NodeId,
    blob_store: ActorAddress,
    metadata: ActorAddress,
) -> ActorAddress {
    let mut config = DatastoreConfig::default();
    config.chunk_size = 64; // Small chunks for multi-chunk testing
    rt.spawn(DatastoreNode::new(node_id, blob_store, metadata, config))
        .unwrap()
}

/// Test harness that routes all commands through the DatastoreNode coordinator.
pub struct NodeHarness {
    pub rt: Runtime,
    pub node: ActorAddress,
    pub blob_store: ActorAddress,
    pub inbox: Inbox<DatastoreResponse>,
    pub node_id: NodeId,
}

impl NodeHarness {
    pub fn new() -> Self {
        let rt = test_runtime();
        let blob_store = spawn_blob_store(&rt);
        let node_id = test_node_id();
        let metadata = spawn_metadata(&rt, node_id);
        let node = spawn_datastore_node(&rt, node_id, blob_store, metadata);
        let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();

        // Let actors initialize.
        tick_n(&rt, 2);

        Self {
            rt,
            node,
            blob_store,
            inbox,
            node_id,
        }
    }

    pub fn reply_addr(&self) -> ActorAddress {
        *self.inbox.addr()
    }
}

// ─── MultiNodeHarness (distributed simulation) ──────────────────────────────

/// A simulated node in a multi-node cluster.
pub struct SimNode {
    pub blob_store: ActorAddress,
    pub metadata: ActorAddress,
    pub node_id: NodeId,
}

/// Multi-node simulation harness. All nodes share a single Runtime so actor
/// addresses are globally unique and cross-node messaging works via `ctx.send()`.
pub struct MultiNodeHarness {
    pub rt: Runtime,
    pub nodes: Vec<SimNode>,
    pub inbox: Inbox<DatastoreResponse>,
    pub chunk_size: u32,
}

impl MultiNodeHarness {
    /// Create a cluster of `n` nodes, all wired as peers.
    pub fn new(n: usize) -> Self {
        let rt = test_runtime();
        let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
        let chunk_size = 64u32;

        let mut nodes = Vec::with_capacity(n);
        for i in 0..n {
            let node_id = NodeId([(i + 1) as u8; 32]);
            let blob_store = spawn_blob_store(&rt);
            let mut config = DatastoreConfig::default();
            config.gc_interval = 3;
            config.chunk_size = chunk_size;
            let metadata = spawn_metadata_with_config(&rt, node_id, &config, blob_store);
            nodes.push(SimNode {
                blob_store,
                metadata,
                node_id,
            });
        }

        // Wire all MetadataActors as peers of each other.
        for i in 0..n {
            let peers: Vec<ActorAddress> = (0..n)
                .filter(|&j| j != i)
                .map(|j| nodes[j].metadata)
                .collect();
            rt.send_to(nodes[i].metadata, MetadataMsg::SetPeers { peers })
                .unwrap();
        }

        tick_n(&rt, 2);

        Self {
            rt,
            nodes,
            inbox,
            chunk_size,
        }
    }

    pub fn reply_addr(&self) -> ActorAddress {
        *self.inbox.addr()
    }

    /// Store a blob on a specific node: chunk it, store chunks, store metadata.
    pub fn put_on(&self, node_idx: usize, data: &[u8], name: Option<&str>) -> ContentHash {
        let node = &self.nodes[node_idx];
        let (content_hash, manifest, chunks) = chunk_blob(data, self.chunk_size);

        for (hash, chunk_data) in &chunks {
            self.rt
                .send_to(
                    node.blob_store,
                    BlobStoreMsg::WriteChunk {
                        hash: *hash,
                        data: chunk_data.clone(),
                        reply_to: self.reply_addr(),
                    },
                )
                .unwrap();
        }
        tick_n(&self.rt, 3);
        let _ = tick_and_drain(&self.rt, &self.inbox, 2);

        self.rt
            .send_to(
                node.blob_store,
                BlobStoreMsg::WriteManifest {
                    manifest: manifest.clone(),
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        tick_n(&self.rt, 2);
        let _ = tick_and_drain(&self.rt, &self.inbox, 1);

        let entry = ObjectEntry {
            content_hash,
            name: name.map(|s| s.to_string()),
            node_id: node.node_id,
            tags: BTreeMap::new(),
            size_bytes: data.len() as u64,
            created_at: 0,
        };
        self.rt
            .send_to(
                node.metadata,
                MetadataMsg::PutObject {
                    entry,
                    manifest,
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        tick_n(&self.rt, 2);
        let responses = tick_and_drain(&self.rt, &self.inbox, 1);
        assert!(
            responses
                .iter()
                .any(|r| matches!(r, DatastoreResponse::PutOk { .. })),
            "expected PutOk response"
        );

        content_hash
    }

    /// Query metadata on a specific node via GetObject.
    pub fn get_from(&self, node_idx: usize, content_hash: ContentHash) -> Option<(ObjectEntry, ObjectManifest)> {
        let node = &self.nodes[node_idx];
        self.rt
            .send_to(
                node.metadata,
                MetadataMsg::GetObject {
                    content_hash,
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        let resp = tick_until_recv(&self.rt, &self.inbox, 10)?;
        match resp {
            DatastoreResponse::GetOk { entry, manifest } => Some((entry, manifest)),
            DatastoreResponse::NotFound => None,
            other => panic!("unexpected response: {other:?}"),
        }
    }

    /// Send DisseminateTick to all MetadataActors and tick the runtime.
    pub fn disseminate_all(&self) {
        for node in &self.nodes {
            self.rt
                .send_to(node.metadata, MetadataMsg::DisseminateTick)
                .unwrap();
        }
        // Tick enough for: DisseminateTick → MetadataActor → HandleStoreObject → peer MetadataActor
        tick_n(&self.rt, 5);
    }

    /// List objects on a specific node with optional name filter.
    pub fn list_on(&self, node_idx: usize, name_filter: Option<&str>) -> Vec<ObjectEntry> {
        let node = &self.nodes[node_idx];
        self.rt
            .send_to(
                node.metadata,
                MetadataMsg::ListLocal {
                    name_filter: name_filter.map(|s| s.to_string()),
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        let resp = tick_until_recv(&self.rt, &self.inbox, 10).unwrap();
        match resp {
            DatastoreResponse::ListOk { entries } => entries,
            other => panic!("unexpected list response: {other:?}"),
        }
    }

    /// Delete an object on a specific node.
    pub fn delete_on(&self, node_idx: usize, content_hash: &ContentHash) {
        let node = &self.nodes[node_idx];
        self.rt
            .send_to(
                node.metadata,
                MetadataMsg::DeleteObject {
                    content_hash: *content_hash,
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        let resp = tick_until_recv(&self.rt, &self.inbox, 10).unwrap();
        assert!(
            matches!(resp, DatastoreResponse::DeleteOk { .. }),
            "expected DeleteOk, got {resp:?}"
        );
    }

    /// Query BlobStore for all chunk hashes on a specific node.
    pub fn list_chunks_on(&self, node_idx: usize) -> Vec<ContentHash> {
        let node = &self.nodes[node_idx];
        self.rt
            .send_to(
                node.blob_store,
                BlobStoreMsg::ListChunks {
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        let resp = tick_until_recv(&self.rt, &self.inbox, 10).unwrap();
        match resp {
            DatastoreResponse::ChunkList { hashes } => hashes,
            other => panic!("expected ChunkList, got {other:?}"),
        }
    }

    /// Read a chunk from a specific node's BlobStore.
    pub fn read_chunk_from(&self, node_idx: usize, hash: ContentHash) -> Option<Vec<u8>> {
        let node = &self.nodes[node_idx];
        self.rt
            .send_to(
                node.blob_store,
                BlobStoreMsg::ReadChunk {
                    hash,
                    reply_to: self.reply_addr(),
                },
            )
            .unwrap();
        let resp = tick_until_recv(&self.rt, &self.inbox, 10).unwrap();
        match resp {
            DatastoreResponse::ChunkOk { data, .. } => Some(data),
            DatastoreResponse::NotFound => None,
            other => panic!("unexpected chunk response: {other:?}"),
        }
    }

    /// Send GcTick messages to a specific node's MetadataActor.
    pub fn gc_ticks_on(&self, node_idx: usize, n: usize) {
        let node = &self.nodes[node_idx];
        for _ in 0..n {
            self.rt
                .send_to(node.metadata, MetadataMsg::GcTick)
                .unwrap();
            tick_n(&self.rt, 3);
        }
    }
}
