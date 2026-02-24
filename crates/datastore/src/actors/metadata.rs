//! MetadataActor — object metadata index with DHT overlay.
//!
//! Owns:
//! - Local object index: `HashMap<ContentHash, ObjectEntry>` keyed by content hash
//! - Dissemination queue for DHT replication (reuses `ClusterRegistry` pattern)
//!
//! The metadata DHT is a separate Kademlia overlay from the actor directory.
//! Objects are keyed by `blake3(blob_bytes)` — the content hash of the entire blob.

use std::collections::{HashMap, HashSet};

use swactor::actor::{ActorAddress, ActorInterface, Ctx};

use swactor::transport::NodeId;

use crate::messages::{BlobStoreMsg, DatastoreResponse, MetadataMsg};
use crate::types::{ContentHash, DatastoreConfig, ObjectEntry, ObjectManifest};

// ─── Dissemination entry (reuses registry.rs pattern) ───────────────────────

#[derive(Debug, Clone)]
struct DisseminationEntry {
    entry: ObjectEntry,
    manifest: Option<ObjectManifest>,
    remaining: usize,
}

// ─── MetadataActor ──────────────────────────────────────────────────────────

/// Manages the object metadata index for a single node.
pub struct MetadataActor {
    /// This node's identity.
    node_id: NodeId,
    /// Local object index: content_hash → ObjectEntry.
    entries: HashMap<ContentHash, ObjectEntry>,
    /// Local manifest cache: content_hash → ObjectManifest.
    manifests: HashMap<ContentHash, ObjectManifest>,
    /// Pending entries to disseminate to DHT peers.
    dissemination: Vec<DisseminationEntry>,
    /// Tick counter for periodic GC.
    tick_count: u64,
    /// Configuration.
    gc_interval: u64,
    /// Dissemination multiplier (Λ) — same role as in SWIM.
    dissemination_lambda: usize,
    /// Address of the local BlobStoreActor (for forwarding manifest writes).
    blob_store_addr: Option<ActorAddress>,
    /// Addresses of peer MetadataActors for epidemic dissemination.
    peers: Vec<ActorAddress>,
}

impl MetadataActor {
    pub fn new(node_id: NodeId, config: &DatastoreConfig) -> Self {
        Self {
            node_id,
            entries: HashMap::new(),
            manifests: HashMap::new(),
            dissemination: Vec::new(),
            tick_count: 0,
            gc_interval: config.gc_interval,
            dissemination_lambda: 3,
            blob_store_addr: None,
            peers: Vec::new(),
        }
    }

    /// Set the address of the co-located BlobStoreActor.
    pub fn set_blob_store(&mut self, addr: ActorAddress) {
        self.blob_store_addr = Some(addr);
    }

    fn transmit_budget(&self, cluster_size: usize) -> usize {
        let n = cluster_size.max(2) as f64;
        let log_n = n.log2().ceil() as usize;
        self.dissemination_lambda * log_n.max(1)
    }

    fn enqueue(&mut self, entry: ObjectEntry, manifest: Option<ObjectManifest>, cluster_size: usize) {
        let budget = self.transmit_budget(cluster_size);

        // Replace existing entry for same content hash if present.
        if let Some(existing) = self
            .dissemination
            .iter_mut()
            .find(|e| e.entry.content_hash == entry.content_hash)
        {
            existing.entry = entry;
            if manifest.is_some() {
                existing.manifest = manifest;
            }
            existing.remaining = budget;
            return;
        }

        self.dissemination.push(DisseminationEntry {
            entry,
            manifest,
            remaining: budget,
        });
    }

    /// Take pending entries for dissemination, up to `max_count`.
    /// Returns `(ObjectEntry, Option<ObjectManifest>)` pairs.
    pub fn take_pending(&mut self, max_count: usize) -> Vec<(ObjectEntry, Option<ObjectManifest>)> {
        let count = max_count.min(self.dissemination.len());
        let mut result = Vec::with_capacity(count);

        for entry in self.dissemination.iter_mut().take(count) {
            result.push((entry.entry.clone(), entry.manifest.clone()));
            entry.remaining = entry.remaining.saturating_sub(1);
        }

        // Evict exhausted entries.
        self.dissemination.retain(|e| e.remaining > 0);

        result
    }

    /// Periodic GC: build referenced chunk set from all manifests and send
    /// `GcUnreferenced` to BlobStoreActor to delete orphaned chunks.
    fn gc_tick(&mut self, ctx: &Ctx) {
        self.tick_count += 1;
        if !self.tick_count.is_multiple_of(self.gc_interval) {
            return;
        }

        let blob_store_addr = match self.blob_store_addr {
            Some(addr) => addr,
            None => return,
        };

        let mut referenced = HashSet::new();
        for manifest in self.manifests.values() {
            for chunk_ref in &manifest.chunks {
                referenced.insert(chunk_ref.hash);
            }
        }

        let _ = ctx.send(blob_store_addr, BlobStoreMsg::GcUnreferenced { referenced });
    }

    // ─── Message handlers ───────────────────────────────────────────────

    fn handle_put_object(
        &mut self,
        ctx: &Ctx,
        entry: ObjectEntry,
        manifest: ObjectManifest,
        reply_to: ActorAddress,
    ) {
        let content_hash = entry.content_hash;

        // Store manifest locally.
        self.manifests.insert(content_hash, manifest.clone());

        // Insert entry keyed by content hash.
        let mut entry = entry;
        entry.node_id = self.node_id;
        self.entries.insert(content_hash, entry.clone());

        // Persist entry to disk via BlobStoreActor.
        if let Some(addr) = self.blob_store_addr {
            let _ = ctx.send(addr, BlobStoreMsg::WriteEntry { entry: entry.clone() });
        }

        // Enqueue for DHT dissemination (include manifest for peer replication).
        self.enqueue(entry, Some(manifest), 3);

        let _ = ctx.send(
            reply_to,
            DatastoreResponse::PutOk { content_hash },
        );
    }

    fn handle_get_object(&self, ctx: &Ctx, content_hash: ContentHash, reply_to: ActorAddress) {
        match self.entries.get(&content_hash) {
            Some(entry) => {
                if let Some(manifest) = self.manifests.get(&content_hash) {
                    let _ = ctx.send(
                        reply_to,
                        DatastoreResponse::GetOk {
                            entry: entry.clone(),
                            manifest: manifest.clone(),
                        },
                    );
                } else {
                    let _ = ctx.send(
                        reply_to,
                        DatastoreResponse::Error {
                            reason: format!("manifest not found for content hash: {content_hash}"),
                        },
                    );
                }
            }
            None => {
                let _ = ctx.send(reply_to, DatastoreResponse::NotFound);
            }
        }
    }

    fn handle_delete_object(&mut self, ctx: &Ctx, content_hash: ContentHash, reply_to: ActorAddress) {
        if self.entries.remove(&content_hash).is_some() {
            self.manifests.remove(&content_hash);
            // Delete persisted entry from disk.
            if let Some(addr) = self.blob_store_addr {
                let _ = ctx.send(addr, BlobStoreMsg::DeleteEntry { hash: content_hash });
            }
            let _ = ctx.send(reply_to, DatastoreResponse::DeleteOk { content_hash });
        } else {
            let _ = ctx.send(reply_to, DatastoreResponse::NotFound);
        }
    }

    fn handle_list_local(
        &self,
        ctx: &Ctx,
        name_filter: Option<String>,
        reply_to: ActorAddress,
    ) {
        let entries: Vec<ObjectEntry> = self
            .entries
            .values()
            .filter(|e| {
                match (&name_filter, &e.name) {
                    (Some(filter), Some(name)) => name.contains(filter.as_str()),
                    (Some(_), None) => false,
                    (None, _) => true,
                }
            })
            .cloned()
            .collect();

        let _ = ctx.send(reply_to, DatastoreResponse::ListOk { entries });
    }

    fn handle_list_swarm(
        &self,
        ctx: &Ctx,
        name_filter: Option<String>,
        reply_to: ActorAddress,
    ) {
        // Delegates to local index. Swarm-wide fan-out to peer MetadataActors
        // will be wired when networking integration is added.
        self.handle_list_local(ctx, name_filter, reply_to);
    }

    fn handle_find_object(
        &self,
        ctx: &Ctx,
        _from: NodeId,
        content_hash: ContentHash,
        reply_to: ActorAddress,
    ) {
        match self.entries.get(&content_hash) {
            Some(entry) => {
                let _ = ctx.send(
                    reply_to,
                    DatastoreResponse::GetOk {
                        entry: entry.clone(),
                        manifest: self
                            .manifests
                            .get(&content_hash)
                            .cloned()
                            .unwrap_or_else(|| ObjectManifest {
                                content_hash,
                                chunks: vec![],
                                total_size: entry.size_bytes,
                                chunk_size: 0,
                                content_type: None,
                            }),
                    },
                );
            }
            None => {
                let _ = ctx.send(reply_to, DatastoreResponse::NotFound);
            }
        }
    }

    fn handle_set_peers(&mut self, peers: Vec<ActorAddress>) {
        self.peers = peers;
    }

    fn handle_disseminate_tick(&mut self, ctx: &Ctx) {
        if self.peers.is_empty() {
            return;
        }
        let pending = self.take_pending(10);
        for (entry, manifest) in pending {
            for &peer in &self.peers {
                let _ = ctx.send(peer, MetadataMsg::HandleStoreObject {
                    entry: entry.clone(),
                    manifest: manifest.clone(),
                });
            }
        }
    }

    fn handle_store_object(&mut self, ctx: &Ctx, entry: ObjectEntry, manifest: Option<ObjectManifest>) {
        // Insert if absent — content-addressed entries don't conflict.
        let content_hash = entry.content_hash;
        if !self.entries.contains_key(&content_hash) {
            if let Some(ref m) = manifest {
                self.manifests.insert(content_hash, m.clone());
            }
            self.entries.insert(content_hash, entry.clone());
            // Persist entry to disk via BlobStoreActor.
            if let Some(addr) = self.blob_store_addr {
                let _ = ctx.send(addr, BlobStoreMsg::WriteEntry { entry: entry.clone() });
            }
            self.enqueue(entry, manifest, 3);
        }
    }

    fn handle_bulk_load(&mut self, entries: Vec<(ObjectEntry, ObjectManifest)>) {
        for (entry, manifest) in entries {
            let hash = entry.content_hash;
            self.entries.insert(hash, entry);
            self.manifests.insert(hash, manifest);
        }
    }
}

impl ActorInterface for MetadataActor {
    type Incoming = MetadataMsg;
    type Response = DatastoreResponse;

    fn handle(&mut self, ctx: &Ctx, msg: MetadataMsg) {
        match msg {
            MetadataMsg::PutObject {
                entry,
                manifest,
                reply_to,
            } => self.handle_put_object(ctx, entry, manifest, reply_to),
            MetadataMsg::GetObject { content_hash, reply_to } => {
                self.handle_get_object(ctx, content_hash, reply_to)
            }
            MetadataMsg::DeleteObject { content_hash, reply_to } => {
                self.handle_delete_object(ctx, content_hash, reply_to)
            }
            MetadataMsg::ListLocal { name_filter, reply_to } => {
                self.handle_list_local(ctx, name_filter, reply_to)
            }
            MetadataMsg::ListSwarm { name_filter, reply_to } => {
                self.handle_list_swarm(ctx, name_filter, reply_to)
            }
            MetadataMsg::HandleFindObject {
                from,
                content_hash,
                reply_to,
            } => self.handle_find_object(ctx, from, content_hash, reply_to),
            MetadataMsg::HandleStoreObject { entry, manifest } => {
                self.handle_store_object(ctx, entry, manifest)
            }
            MetadataMsg::SetPeers { peers } => self.handle_set_peers(peers),
            MetadataMsg::DisseminateTick => self.handle_disseminate_tick(ctx),
            MetadataMsg::GcTick => self.gc_tick(ctx),
            MetadataMsg::BulkLoad { entries } => self.handle_bulk_load(entries),
        }
    }
}
