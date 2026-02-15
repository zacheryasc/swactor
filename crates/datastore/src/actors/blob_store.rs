//! BlobStoreActor — content-addressed chunk and manifest storage.
//!
//! Delegates all I/O through a `StorageBackend` trait, allowing pluggable
//! backends (filesystem for MVP, IndexedDB for browser, etc.).

use std::collections::HashSet;

use swactor::actor::{ActorInterface, Ctx};

use crate::messages::{BlobStoreMsg, DatastoreResponse};
use crate::storage::StorageBackend;
use crate::types::{ContentHash, ObjectManifest};

/// Manages chunk and manifest storage via a pluggable backend.
pub struct BlobStoreActor {
    backend: Box<dyn StorageBackend>,
}

impl BlobStoreActor {
    pub fn new(backend: Box<dyn StorageBackend>) -> Self {
        Self { backend }
    }

    fn handle_write_chunk(
        &mut self,
        ctx: &Ctx,
        hash: ContentHash,
        data: Vec<u8>,
        reply_to: swactor::actor::ActorAddress,
    ) {
        match self.backend.write_chunk(&hash, &data) {
            Ok(()) => {
                let _ = ctx.send(reply_to, DatastoreResponse::ChunkStored { hash });
            }
            Err(e) => {
                let _ = ctx.send(
                    reply_to,
                    DatastoreResponse::Error {
                        reason: format!("write chunk failed: {e}"),
                    },
                );
            }
        }
    }

    fn handle_read_chunk(
        &self,
        ctx: &Ctx,
        hash: ContentHash,
        reply_to: swactor::actor::ActorAddress,
    ) {
        match self.backend.read_chunk(&hash) {
            Ok(Some(data)) => {
                let _ = ctx.send(reply_to, DatastoreResponse::ChunkOk { hash, data });
            }
            Ok(None) => {
                let _ = ctx.send(reply_to, DatastoreResponse::NotFound);
            }
            Err(e) => {
                let _ = ctx.send(
                    reply_to,
                    DatastoreResponse::Error {
                        reason: format!("read chunk failed: {e}"),
                    },
                );
            }
        }
    }

    fn handle_delete_chunk(&mut self, hash: ContentHash) {
        let _ = self.backend.delete_chunk(&hash);
    }

    fn handle_has_chunk(
        &self,
        ctx: &Ctx,
        hash: ContentHash,
        reply_to: swactor::actor::ActorAddress,
    ) {
        let exists = self.backend.has_chunk(&hash);
        let _ = ctx.send(reply_to, DatastoreResponse::Bool(exists));
    }

    fn handle_list_chunks(&self, ctx: &Ctx, reply_to: swactor::actor::ActorAddress) {
        let hashes = self.backend.list_chunks();
        let _ = ctx.send(reply_to, DatastoreResponse::ChunkList { hashes });
    }

    fn handle_gc_unreferenced(&mut self, referenced: HashSet<ContentHash>) {
        let all_chunks = self.backend.list_chunks();
        for hash in all_chunks {
            if !referenced.contains(&hash) {
                let _ = self.backend.delete_chunk(&hash);
            }
        }
    }

    fn handle_write_manifest(
        &mut self,
        ctx: &Ctx,
        manifest: ObjectManifest,
        reply_to: swactor::actor::ActorAddress,
    ) {
        let hash = manifest.content_hash;
        match self.backend.write_manifest(&manifest) {
            Ok(()) => {
                let _ = ctx.send(reply_to, DatastoreResponse::ManifestStored { hash });
            }
            Err(e) => {
                let _ = ctx.send(
                    reply_to,
                    DatastoreResponse::Error {
                        reason: format!("write manifest failed: {e}"),
                    },
                );
            }
        }
    }

    fn handle_read_manifest(
        &self,
        ctx: &Ctx,
        hash: ContentHash,
        reply_to: swactor::actor::ActorAddress,
    ) {
        match self.backend.read_manifest(&hash) {
            Ok(Some(manifest)) => {
                let _ = ctx.send(reply_to, DatastoreResponse::ManifestOk { manifest });
            }
            Ok(None) => {
                let _ = ctx.send(reply_to, DatastoreResponse::NotFound);
            }
            Err(e) => {
                let _ = ctx.send(
                    reply_to,
                    DatastoreResponse::Error {
                        reason: format!("read manifest failed: {e}"),
                    },
                );
            }
        }
    }
}

impl ActorInterface for BlobStoreActor {
    type Incoming = BlobStoreMsg;
    type Response = DatastoreResponse;

    fn handle(&mut self, ctx: &Ctx, msg: BlobStoreMsg) {
        match msg {
            BlobStoreMsg::WriteChunk {
                hash,
                data,
                reply_to,
            } => self.handle_write_chunk(ctx, hash, data, reply_to),
            BlobStoreMsg::ReadChunk { hash, reply_to } => {
                self.handle_read_chunk(ctx, hash, reply_to)
            }
            BlobStoreMsg::DeleteChunk { hash } => self.handle_delete_chunk(hash),
            BlobStoreMsg::HasChunk { hash, reply_to } => {
                self.handle_has_chunk(ctx, hash, reply_to)
            }
            BlobStoreMsg::ListChunks { reply_to } => {
                self.handle_list_chunks(ctx, reply_to)
            }
            BlobStoreMsg::GcUnreferenced { referenced } => {
                self.handle_gc_unreferenced(referenced)
            }
            BlobStoreMsg::WriteManifest {
                manifest,
                reply_to,
            } => self.handle_write_manifest(ctx, manifest, reply_to),
            BlobStoreMsg::ReadManifest { hash, reply_to } => {
                self.handle_read_manifest(ctx, hash, reply_to)
            }
        }
    }
}
