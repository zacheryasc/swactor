//! StreamListener — listens for incoming BlobTransfer stream offers and
//! forwards them to DatastoreNode for handling.

use swactor::actor::{ActorAddress, ActorInterface, Ctx};

use crate::streams::messages::{StreamManagerMsg, StreamNotification};
use crate::streams::types::StreamMode;

use crate::blob_transfer::parse_metadata;
use crate::messages::DatastoreNodeMsg;

pub struct StreamListener {
    datastore_node: ActorAddress,
    stream_manager: ActorAddress,
}

impl StreamListener {
    pub fn new(datastore_node: ActorAddress, stream_manager: ActorAddress) -> Self {
        Self {
            datastore_node,
            stream_manager,
        }
    }
}

impl ActorInterface for StreamListener {
    type Incoming = StreamNotification;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let _ = ctx.send(
            self.stream_manager,
            StreamManagerMsg::Listen {
                mode: StreamMode::BlobTransfer,
                listener: ctx.self_addr(),
            },
        );
    }

    fn handle(&mut self, ctx: &Ctx, msg: StreamNotification) {
        match msg {
            StreamNotification::StreamOffer {
                stream_id,
                metadata,
                from_node,
                ..
            } => {
                // Parse metadata (supports both legacy 32-byte and new versioned format)
                let meta = match parse_metadata(&metadata) {
                    Some(m) => m,
                    None => {
                        // Reject malformed offer
                        let _ = ctx.send(self.stream_manager, StreamManagerMsg::Reject { stream_id });
                        return;
                    }
                };

                let stream_manager = self.stream_manager;

                let _ = ctx.send(
                    self.datastore_node,
                    DatastoreNodeMsg::HandleStreamOffer {
                        stream_id,
                        content_hash: meta.content_hash,
                        from_node,
                        stream_manager,
                        resume_from_chunk: meta.resume_from_chunk.unwrap_or(0),
                    },
                );
            }
            // Ignore other notifications
            _ => {}
        }
    }
}
