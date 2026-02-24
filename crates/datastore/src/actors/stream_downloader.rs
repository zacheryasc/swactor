//! StreamDownloader — opens a stream to a remote node and downloads a blob.
//!
//! Lifecycle:
//! 1. on_start: sends Open to StreamManager
//! 2. StreamReady: spawns a tokio task for I/O, then stops self
//! 3. tokio task: recv_blob, write chunks to BlobStore, notify DatastoreNode

use std::sync::Arc;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::Runtime;

use crate::streams::messages::{StreamManagerMsg, StreamNotification};
use crate::streams::types::{StreamConfig, StreamMode};

use crate::blob_transfer::{encode_metadata, recv_blob, BlobTransferMetadata};
use crate::messages::{BlobStoreMsg, DatastoreNodeMsg};
use crate::types::ContentHash;

pub struct StreamDownloader {
    content_hash: ContentHash,
    source_node: [u8; 32],
    datastore_node: ActorAddress,
    blob_store: ActorAddress,
    reply_to: ActorAddress,
    stream_manager: ActorAddress,
    tokio_handle: tokio::runtime::Handle,
    runtime: Arc<Runtime>,
    skip_chunks: u64,
}

impl StreamDownloader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        content_hash: ContentHash,
        source_node: [u8; 32],
        datastore_node: ActorAddress,
        blob_store: ActorAddress,
        reply_to: ActorAddress,
        stream_manager: ActorAddress,
        tokio_handle: tokio::runtime::Handle,
        runtime: Arc<Runtime>,
        skip_chunks: u64,
    ) -> Self {
        Self {
            content_hash,
            source_node,
            datastore_node,
            blob_store,
            reply_to,
            stream_manager,
            tokio_handle,
            runtime,
            skip_chunks,
        }
    }
}

impl ActorInterface for StreamDownloader {
    type Incoming = StreamNotification;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let resume = if self.skip_chunks > 0 {
            Some(self.skip_chunks)
        } else {
            None
        };
        let meta = BlobTransferMetadata {
            content_hash: self.content_hash,
            resume_from_chunk: resume,
        };
        let config = StreamConfig {
            metadata: encode_metadata(&meta),
            stripe_count: 1, // blob transfer is sequential — one stripe avoids empty-stripe Closed races
            ..Default::default()
        };
        let _ = ctx.send(
            self.stream_manager,
            StreamManagerMsg::Open {
                target_node: self.source_node,
                mode: StreamMode::BlobTransfer,
                config,
                reply_to: ctx.self_addr(),
            },
        );
    }

    fn handle(&mut self, ctx: &Ctx, msg: StreamNotification) {
        match msg {
            StreamNotification::StreamReady { handle, .. } => {
                let stream_handle = match handle.take() {
                    Some(h) => h,
                    None => return,
                };

                let runtime = Arc::clone(&self.runtime);
                let blob_store = self.blob_store;
                let datastore_node = self.datastore_node;
                let reply_to = self.reply_to;
                let content_hash = self.content_hash;
                let skip_chunks = self.skip_chunks;

                self.tokio_handle.spawn(async move {
                    let (_send, mut recv) = (stream_handle.send, stream_handle.recv);

                    match recv_blob(&mut recv, skip_chunks).await {
                        Ok(received) => {
                            // Write chunks to BlobStore (fire-and-forget)
                            for (hash, data) in &received.chunks {
                                let _ = runtime.send_to(
                                    blob_store,
                                    BlobStoreMsg::WriteChunk {
                                        hash: *hash,
                                        data: data.clone(),
                                        reply_to: datastore_node, // response ignored
                                    },
                                );
                            }

                            // Write manifest to BlobStore (fire-and-forget)
                            let _ = runtime.send_to(
                                blob_store,
                                BlobStoreMsg::WriteManifest {
                                    manifest: received.manifest.clone(),
                                    reply_to: datastore_node, // response ignored
                                },
                            );

                            // Notify DatastoreNode of completion
                            let _ = runtime.send_to(
                                datastore_node,
                                DatastoreNodeMsg::StreamDownloadComplete {
                                    content_hash,
                                    manifest: received.manifest,
                                    reply_to,
                                },
                            );
                        }
                        Err(e) => {
                            let _ = runtime.send_to(
                                datastore_node,
                                DatastoreNodeMsg::StreamDownloadFailed {
                                    content_hash,
                                    reason: e.to_string(),
                                    chunks_completed: skip_chunks,
                                    reply_to,
                                },
                            );
                        }
                    }
                });

                ctx.stop_self();
            }
            StreamNotification::StreamFailed { error, .. } => {
                let _ = ctx.send(
                    self.datastore_node,
                    DatastoreNodeMsg::StreamDownloadFailed {
                        content_hash: self.content_hash,
                        reason: error.to_string(),
                        chunks_completed: self.skip_chunks,
                        reply_to: self.reply_to,
                    },
                );
                ctx.stop_self();
            }
            _ => {}
        }
    }
}
