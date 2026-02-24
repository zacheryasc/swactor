//! StreamServer — accepts an incoming stream and serves blob data.
//!
//! Lifecycle:
//! 1. on_start: sends Accept to StreamManager
//! 2. StreamReady: spawns a tokio task for I/O, then stops self
//! 3. tokio task: reads manifest from BlobStore, streams each chunk on-demand

use std::sync::Arc;
use std::time::Duration;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::Runtime;

use crate::streams::messages::{StreamManagerMsg, StreamNotification};
use crate::streams::types::StreamId;

use crate::blob_transfer::{poll_inbox, send_blob, BlobTransferError};
use crate::messages::{BlobStoreMsg, DatastoreResponse};
use crate::types::ContentHash;

const INBOX_TIMEOUT: Duration = Duration::from_secs(10);

pub struct StreamServer {
    stream_id: StreamId,
    content_hash: ContentHash,
    blob_store: ActorAddress,
    stream_manager: ActorAddress,
    tokio_handle: tokio::runtime::Handle,
    runtime: Arc<Runtime>,
    skip_chunks: u64,
}

impl StreamServer {
    pub fn new(
        stream_id: StreamId,
        content_hash: ContentHash,
        blob_store: ActorAddress,
        stream_manager: ActorAddress,
        tokio_handle: tokio::runtime::Handle,
        runtime: Arc<Runtime>,
        skip_chunks: u64,
    ) -> Self {
        Self {
            stream_id,
            content_hash,
            blob_store,
            stream_manager,
            tokio_handle,
            runtime,
            skip_chunks,
        }
    }
}

impl ActorInterface for StreamServer {
    type Incoming = StreamNotification;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let _ = ctx.send(
            self.stream_manager,
            StreamManagerMsg::Accept {
                stream_id: self.stream_id,
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
                let content_hash = self.content_hash;
                let skip_chunks = self.skip_chunks;

                self.tokio_handle.spawn(async move {
                    let (mut send, _recv) = (stream_handle.send, stream_handle.recv);

                    // Read manifest from BlobStore
                    let manifest_inbox = match runtime.new_inbox::<DatastoreResponse>() {
                        Ok(inbox) => inbox,
                        Err(_) => return,
                    };
                    let _ = runtime.send_to(
                        blob_store,
                        BlobStoreMsg::ReadManifest {
                            hash: content_hash,
                            reply_to: *manifest_inbox.addr(),
                        },
                    );

                    let manifest = match poll_inbox(&manifest_inbox, INBOX_TIMEOUT).await {
                        Some(DatastoreResponse::ManifestOk { manifest }) => manifest,
                        other => {
                            // Manifest not found or timeout — close stream
                            eprintln!("StreamServer: manifest read failed: {other:?}");
                            let _ = send.close();
                            return;
                        }
                    };

                    // Stream each chunk on-demand (one at a time)
                    let rt = Arc::clone(&runtime);
                    let bs = blob_store;
                    let result = send_blob(&mut send, &manifest, |chunk_hash| {
                        let rt = Arc::clone(&rt);
                        async move {
                            let chunk_inbox = rt
                                .new_inbox::<DatastoreResponse>()
                                .map_err(|e| {
                                    BlobTransferError::Storage(format!(
                                        "failed to create inbox: {e}"
                                    ))
                                })?;
                            let _ = rt.send_to(
                                bs,
                                BlobStoreMsg::ReadChunk {
                                    hash: chunk_hash,
                                    reply_to: *chunk_inbox.addr(),
                                },
                            );
                            match poll_inbox(&chunk_inbox, INBOX_TIMEOUT).await {
                                Some(DatastoreResponse::ChunkOk { data, .. }) => Ok(data),
                                _ => Err(BlobTransferError::Storage(
                                    "chunk not found or timeout".into(),
                                )),
                            }
                        }
                    }, skip_chunks)
                    .await;

                    if let Err(e) = result {
                        eprintln!("StreamServer: send_blob failed: {e}");
                    }
                });

                ctx.stop_self();
            }
            StreamNotification::StreamFailed { .. } => {
                ctx.stop_self();
            }
            _ => {}
        }
    }
}
