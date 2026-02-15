//! DatastoreNode — coordinator/facade actor for the datastore stack.
//!
//! Encapsulates the internal actor topology (BlobStoreActor, MetadataActor)
//! behind a single address. Callers send high-level commands (Put, Get,
//! Delete, List, Status) and receive responses. Also routes incoming network
//! protocol messages to the appropriate internal actors.

use std::collections::BTreeMap;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};

use distribution::types::NodeId;

use crate::chunking::chunk_blob;
use crate::messages::{
    BlobStoreMsg, DatastoreNodeMsg, DatastoreResponse, MetadataMsg,
};
use crate::types::{ContentHash, DatastoreConfig, ObjectEntry};

/// Top-level coordinator actor for the datastore.
///
/// Pure router/facade — delegates all work to BlobStoreActor and MetadataActor.
/// Callers interact with a single address instead of knowing about internal actors.
pub struct DatastoreNode {
    node_id: NodeId,
    blob_store: ActorAddress,
    metadata: ActorAddress,
    config: DatastoreConfig,
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
        }
    }
}
