//! DatastoreNode — coordinator/facade actor for the datastore stack.
//!
//! Encapsulates the internal actor topology (BlobStoreActor, MetadataActor)
//! behind a single address. Callers send high-level commands (Put, Get,
//! Delete, List, Status) and receive responses. Also routes incoming network
//! protocol messages to the appropriate internal actors.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::Runtime;

use distribution::types::NodeId;

use crate::actors::stream_downloader::StreamDownloader;
use crate::actors::stream_server::StreamServer;
use crate::chunking::chunk_blob;
use crate::messages::{
    BlobStoreMsg, DatastoreNodeMsg, DatastoreResponse, MetadataMsg,
};
use crate::types::{ContentHash, DatastoreConfig, ObjectEntry, ObjectManifest};

/// Progress from a partially-completed stream download, used for resume.
struct PartialDownload {
    _source_node: [u8; 32],
    chunks_completed: u64,
}

/// Top-level coordinator actor for the datastore.
///
/// Pure router/facade — delegates all work to BlobStoreActor and MetadataActor.
/// Callers interact with a single address instead of knowing about internal actors.
pub struct DatastoreNode {
    node_id: NodeId,
    blob_store: ActorAddress,
    metadata: ActorAddress,
    config: DatastoreConfig,
    // Stream support (configured lazily via ConfigureStreams)
    runtime: Option<Arc<Runtime>>,
    tokio_handle: Option<tokio::runtime::Handle>,
    stream_manager: Option<ActorAddress>,
    // Resume state for interrupted stream downloads
    partial_downloads: HashMap<ContentHash, PartialDownload>,
}

impl DatastoreNode {
    pub fn new(
        node_id: NodeId,
        blob_store: ActorAddress,
        metadata: ActorAddress,
        config: DatastoreConfig,
    ) -> Self {
        Self {
            node_id,
            blob_store,
            metadata,
            config,
            runtime: None,
            tokio_handle: None,
            stream_manager: None,
            partial_downloads: HashMap::new(),
        }
    }

    fn handle_put(
        &self,
        ctx: &Ctx,
        data: Vec<u8>,
        name: Option<String>,
        tags: BTreeMap<String, String>,
        reply_to: ActorAddress,
    ) {
        let (content_hash, manifest, chunks) = chunk_blob(&data, self.config.chunk_size);

        // Fire-and-forget chunk writes to BlobStoreActor.
        // reply_to: self — ChunkStored responses are silently dropped (type mismatch).
        for (hash, chunk_data) in chunks {
            let _ = ctx.send(
                self.blob_store,
                BlobStoreMsg::WriteChunk {
                    hash,
                    data: chunk_data,
                    reply_to: ctx.self_addr(),
                },
            );
        }

        // Fire-and-forget manifest write to BlobStoreActor.
        let _ = ctx.send(
            self.blob_store,
            BlobStoreMsg::WriteManifest {
                manifest: manifest.clone(),
                reply_to: ctx.self_addr(),
            },
        );

        // Build ObjectEntry and send to MetadataActor with caller's reply_to.
        let entry = ObjectEntry {
            content_hash,
            name,
            node_id: self.node_id,
            tags,
            size_bytes: data.len() as u64,
            created_at: 0,
        };

        let _ = ctx.send(
            self.metadata,
            MetadataMsg::PutObject {
                entry,
                manifest,
                reply_to,
            },
        );
    }

    fn handle_get(&self, ctx: &Ctx, content_hash: ContentHash, reply_to: ActorAddress) {
        let _ = ctx.send(
            self.metadata,
            MetadataMsg::GetObject {
                content_hash,
                reply_to,
            },
        );
    }

    fn handle_read_chunk(&self, ctx: &Ctx, hash: ContentHash, reply_to: ActorAddress) {
        let _ = ctx.send(
            self.blob_store,
            BlobStoreMsg::ReadChunk { hash, reply_to },
        );
    }

    fn handle_delete(&self, ctx: &Ctx, content_hash: ContentHash, reply_to: ActorAddress) {
        let _ = ctx.send(
            self.metadata,
            MetadataMsg::DeleteObject {
                content_hash,
                reply_to,
            },
        );
    }

    fn handle_list(
        &self,
        ctx: &Ctx,
        name_filter: Option<String>,
        all: bool,
        reply_to: ActorAddress,
    ) {
        if all {
            let _ = ctx.send(
                self.metadata,
                MetadataMsg::ListSwarm {
                    name_filter,
                    reply_to,
                },
            );
        } else {
            let _ = ctx.send(
                self.metadata,
                MetadataMsg::ListLocal {
                    name_filter,
                    reply_to,
                },
            );
        }
    }

    fn handle_status(&self, ctx: &Ctx, reply_to: ActorAddress) {
        let _ = ctx.send(
            reply_to,
            DatastoreResponse::NodeStatus {
                node_id: self.node_id,
            },
        );
    }

    fn handle_incoming_get_chunk(
        &self,
        ctx: &Ctx,
        request: crate::messages::GetChunkRequest,
        reply_to: ActorAddress,
    ) {
        let _ = ctx.send(
            self.blob_store,
            BlobStoreMsg::ReadChunk {
                hash: request.hash,
                reply_to,
            },
        );
    }

    fn handle_incoming_get_manifest(
        &self,
        ctx: &Ctx,
        request: crate::messages::GetManifestRequest,
        reply_to: ActorAddress,
    ) {
        let _ = ctx.send(
            self.blob_store,
            BlobStoreMsg::ReadManifest {
                hash: request.hash,
                reply_to,
            },
        );
    }

    fn handle_incoming_store_object(
        &self,
        ctx: &Ctx,
        request: crate::messages::StoreObjectRequest,
    ) {
        let _ = ctx.send(
            self.metadata,
            MetadataMsg::HandleStoreObject {
                entry: request.entry,
                manifest: None,
            },
        );
    }

    fn handle_incoming_find_object(
        &self,
        ctx: &Ctx,
        request: crate::messages::FindObjectRequest,
        reply_to: ActorAddress,
    ) {
        let _ = ctx.send(
            self.metadata,
            MetadataMsg::HandleFindObject {
                from: request.from,
                content_hash: request.content_hash,
                reply_to,
            },
        );
    }

    fn handle_incoming_list_objects(
        &self,
        ctx: &Ctx,
        request: crate::messages::ListObjectsRequest,
        reply_to: ActorAddress,
    ) {
        let _ = ctx.send(
            self.metadata,
            MetadataMsg::ListLocal {
                name_filter: request.name_filter,
                reply_to,
            },
        );
    }

    // ── Stream-based transfer handlers ───────────────────────────────────

    fn handle_configure_streams(
        &mut self,
        stream_manager: ActorAddress,
        tokio_handle: tokio::runtime::Handle,
        runtime: Arc<Runtime>,
    ) {
        self.stream_manager = Some(stream_manager);
        self.tokio_handle = Some(tokio_handle);
        self.runtime = Some(runtime);
    }

    fn handle_download_via_stream(
        &self,
        ctx: &Ctx,
        content_hash: ContentHash,
        source_node: [u8; 32],
        reply_to: ActorAddress,
    ) {
        let (stream_manager, tokio_handle, runtime) =
            match (&self.stream_manager, &self.tokio_handle, &self.runtime) {
                (Some(sm), Some(th), Some(rt)) => (*sm, th.clone(), Arc::clone(rt)),
                _ => {
                    let _ = ctx.send(
                        reply_to,
                        DatastoreResponse::TransferFailed {
                            reason: "stream support not configured".into(),
                        },
                    );
                    return;
                }
            };

        // Check for partial progress from a previous attempt
        let skip_chunks = self
            .partial_downloads
            .get(&content_hash)
            .map(|p| p.chunks_completed)
            .unwrap_or(0);

        let downloader = StreamDownloader::new(
            content_hash,
            source_node,
            ctx.self_addr(),
            self.blob_store,
            reply_to,
            stream_manager,
            tokio_handle,
            runtime,
            skip_chunks,
        );
        let _ = ctx.spawn(downloader);
    }

    fn handle_stream_offer(
        &self,
        ctx: &Ctx,
        stream_id: swactor_streams::types::StreamId,
        content_hash: ContentHash,
        _from_node: [u8; 32],
        stream_manager: ActorAddress,
        resume_from_chunk: u64,
    ) {
        let (tokio_handle, runtime) = match (&self.tokio_handle, &self.runtime) {
            (Some(th), Some(rt)) => (th.clone(), Arc::clone(rt)),
            _ => return,
        };

        let server = StreamServer::new(
            stream_id,
            content_hash,
            self.blob_store,
            stream_manager,
            tokio_handle,
            runtime,
            resume_from_chunk,
        );
        let _ = ctx.spawn(server);
    }

    fn handle_stream_download_complete(
        &mut self,
        ctx: &Ctx,
        content_hash: ContentHash,
        manifest: ObjectManifest,
        reply_to: ActorAddress,
    ) {
        // Clear any partial progress now that download is complete
        self.partial_downloads.remove(&content_hash);

        let entry = ObjectEntry {
            content_hash,
            name: None,
            node_id: self.node_id,
            tags: BTreeMap::new(),
            size_bytes: manifest.total_size,
            created_at: 0,
        };

        let _ = ctx.send(
            self.metadata,
            MetadataMsg::PutObject {
                entry,
                manifest,
                reply_to,
            },
        );
    }

    fn handle_stream_download_failed(
        &mut self,
        ctx: &Ctx,
        content_hash: ContentHash,
        reason: String,
        chunks_completed: u64,
        source_node: [u8; 32],
        reply_to: ActorAddress,
    ) {
        // Store partial progress so next attempt can resume
        if chunks_completed > 0 {
            self.partial_downloads.insert(
                content_hash,
                PartialDownload {
                    _source_node: source_node,
                    chunks_completed,
                },
            );
        }
        let _ = ctx.send(reply_to, DatastoreResponse::TransferFailed { reason });
    }
}

impl ActorInterface for DatastoreNode {
    type Incoming = DatastoreNodeMsg;
    type Response = DatastoreResponse;

    fn handle(&mut self, ctx: &Ctx, msg: DatastoreNodeMsg) {
        match msg {
            DatastoreNodeMsg::Put {
                data,
                name,
                tags,
                reply_to,
            } => self.handle_put(ctx, data, name, tags, reply_to),
            DatastoreNodeMsg::Get {
                content_hash,
                reply_to,
            } => self.handle_get(ctx, content_hash, reply_to),
            DatastoreNodeMsg::Delete {
                content_hash,
                reply_to,
            } => self.handle_delete(ctx, content_hash, reply_to),
            DatastoreNodeMsg::List {
                name_filter,
                all,
                reply_to,
            } => self.handle_list(ctx, name_filter, all, reply_to),
            DatastoreNodeMsg::Status { reply_to } => self.handle_status(ctx, reply_to),
            DatastoreNodeMsg::ReadChunk { hash, reply_to } => {
                self.handle_read_chunk(ctx, hash, reply_to)
            }
            DatastoreNodeMsg::IncomingGetChunk { request, reply_to } => {
                self.handle_incoming_get_chunk(ctx, request, reply_to)
            }
            DatastoreNodeMsg::IncomingGetManifest { request, reply_to } => {
                self.handle_incoming_get_manifest(ctx, request, reply_to)
            }
            DatastoreNodeMsg::IncomingStoreObject { request } => {
                self.handle_incoming_store_object(ctx, request)
            }
            DatastoreNodeMsg::IncomingFindObject { request, reply_to } => {
                self.handle_incoming_find_object(ctx, request, reply_to)
            }
            DatastoreNodeMsg::IncomingListObjects { request, reply_to } => {
                self.handle_incoming_list_objects(ctx, request, reply_to)
            }
            DatastoreNodeMsg::DownloadViaStream {
                content_hash,
                source_node,
                reply_to,
            } => self.handle_download_via_stream(ctx, content_hash, source_node, reply_to),
            DatastoreNodeMsg::HandleStreamOffer {
                stream_id,
                content_hash,
                from_node,
                stream_manager,
                resume_from_chunk,
            } => self.handle_stream_offer(ctx, stream_id, content_hash, from_node, stream_manager, resume_from_chunk),
            DatastoreNodeMsg::StreamDownloadComplete {
                content_hash,
                manifest,
                reply_to,
            } => self.handle_stream_download_complete(ctx, content_hash, manifest, reply_to),
            DatastoreNodeMsg::StreamDownloadFailed {
                content_hash,
                reason,
                chunks_completed,
                reply_to,
            } => self.handle_stream_download_failed(ctx, content_hash, reason, chunks_completed, [0; 32], reply_to),
            DatastoreNodeMsg::ConfigureStreams {
                stream_manager,
                tokio_handle,
                runtime,
            } => self.handle_configure_streams(stream_manager, tokio_handle, runtime),
        }
    }
}
