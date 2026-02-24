//! TransferActor — ephemeral actor for downloading an object from a remote node.
//!
//! One TransferActor is spawned per download. It walks the manifest's chunk list,
//! requests each chunk from the source node's BlobStoreActor, forwards received
//! chunks to the local BlobStoreActor for persistence, and replies to the
//! original requester when all chunks are received (or on failure).
//!
//! Sequential chunk fetching for MVP (parallel fetching planned for later).
//! Self-terminates via `ctx.stop_self()` on completion, failure, or cancel.

use std::collections::HashSet;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};

use swactor::transport::NodeId;

use crate::messages::{DatastoreResponse, TransferMsg};
use crate::types::{ContentHash, ObjectManifest, TransferStatus};

/// Ephemeral actor that manages a single object download.
pub struct TransferActor {
    /// The manifest describing which chunks to download.
    manifest: Option<ObjectManifest>,
    /// The remote node to fetch chunks from.
    source_node: Option<NodeId>,
    /// Address to send the final result to.
    reply_to: Option<ActorAddress>,
    /// Address of the local BlobStoreActor for storing received chunks.
    blob_store_addr: ActorAddress,
    /// Chunk hashes still pending download.
    pending: HashSet<ContentHash>,
    /// Chunk hashes successfully received and stored.
    received: HashSet<ContentHash>,
    /// Current status of the transfer.
    status: TransferStatus,
    /// Number of retry attempts per chunk.
    max_retries: usize,
    /// Tracks which chunks have been retried and how many times.
    retry_counts: std::collections::HashMap<ContentHash, usize>,
}

impl TransferActor {
    /// Create a new transfer actor.
    ///
    /// `blob_store_addr` is the address of the local BlobStoreActor where
    /// downloaded chunks will be persisted.
    pub fn new(blob_store_addr: ActorAddress) -> Self {
        Self {
            manifest: None,
            source_node: None,
            reply_to: None,
            blob_store_addr,
            pending: HashSet::new(),
            received: HashSet::new(),
            status: TransferStatus::Downloading {
                chunks_received: 0,
                chunks_total: 0,
            },
            max_retries: 1,
            retry_counts: std::collections::HashMap::new(),
        }
    }

    fn handle_start_download(
        &mut self,
        _ctx: &Ctx,
        manifest: ObjectManifest,
        source_node: NodeId,
        reply_to: ActorAddress,
    ) {
        let total = manifest.chunks.len();
        self.pending = manifest.chunks.iter().map(|c| c.hash).collect();
        self.manifest = Some(manifest);
        self.source_node = Some(source_node);
        self.reply_to = Some(reply_to);
        self.status = TransferStatus::Downloading {
            chunks_received: 0,
            chunks_total: total,
        };

        // Chunks are fed externally via ChunkReceived/ChunkFailed messages.
        // In production, a network adapter (or DatastoreNode) reads chunks from
        // the remote BlobStoreActor and forwards them here. In simulation, the
        // test harness plays this role.
    }

    fn handle_chunk_received(
        &mut self,
        ctx: &Ctx,
        hash: ContentHash,
        data: Vec<u8>,
    ) {
        if !self.pending.remove(&hash) {
            return; // Duplicate or unexpected chunk.
        }

        // Forward chunk to local BlobStoreActor for persistence.
        let _ = ctx.send(
            self.blob_store_addr,
            crate::messages::BlobStoreMsg::WriteChunk {
                hash,
                data,
                reply_to: ctx.self_addr(),
            },
        );

        self.received.insert(hash);

        let total = self.received.len() + self.pending.len();
        self.status = TransferStatus::Downloading {
            chunks_received: self.received.len(),
            chunks_total: total,
        };

        // Check if all chunks are received.
        if self.pending.is_empty() {
            self.status = TransferStatus::Complete;
            if let (Some(manifest), Some(reply_to)) = (&self.manifest, self.reply_to) {
                let _ = ctx.send(
                    reply_to,
                    DatastoreResponse::TransferComplete {
                        content_hash: manifest.content_hash,
                    },
                );
            }
            ctx.stop_self();
        }
    }

    fn handle_chunk_failed(
        &mut self,
        ctx: &Ctx,
        hash: ContentHash,
        reason: String,
    ) {
        let retries = self.retry_counts.entry(hash).or_insert(0);
        if *retries < self.max_retries {
            *retries += 1;
            // Retry is tracked; the external driver (network adapter or test
            // harness) is expected to re-send the chunk on retry.
            return;
        }

        // Exhausted retries — fail the whole transfer.
        self.status = TransferStatus::Failed {
            reason: reason.clone(),
        };
        if let Some(reply_to) = self.reply_to {
            let _ = ctx.send(reply_to, DatastoreResponse::TransferFailed { reason });
        }
        ctx.stop_self();
    }

    fn handle_cancel(&mut self, ctx: &Ctx) {
        self.status = TransferStatus::Cancelled;
        ctx.stop_self();
    }
}

impl ActorInterface for TransferActor {
    type Incoming = TransferMsg;
    type Response = DatastoreResponse;

    fn handle(&mut self, ctx: &Ctx, msg: TransferMsg) {
        match msg {
            TransferMsg::StartDownload {
                manifest,
                source_node,
                reply_to,
            } => self.handle_start_download(ctx, manifest, source_node, reply_to),
            TransferMsg::ChunkReceived { hash, data } => {
                self.handle_chunk_received(ctx, hash, data)
            }
            TransferMsg::ChunkFailed { hash, reason } => {
                self.handle_chunk_failed(ctx, hash, reason)
            }
            TransferMsg::Cancel => self.handle_cancel(ctx),
        }
    }
}
