//! Legacy `DatastreamSink` for [`DatastreamFrame`] actor messages.
//!
//! New live transports should prefer catalog-aware [`crate::DatastreamEvent`]
//! streams. This actor remains as the counterpart to
//! [`ClusterFrameSink`](super::emit::ClusterFrameSink): a node ships positioned
//! telemetry as legacy `DatastreamFrame` actor messages over regular Swactor
//! transport, and this actor receives them, decodes each back into a
//! `(StreamId, Frame)` delivery, and hands it to a caller-supplied fold.
//!
//! It deliberately knows nothing about any view layer. The actor owns an opaque
//! callback so binaries can wire decoded deliveries into whichever fold they
//! need. Malformed payloads are dropped silently.

use swactor::actor::ActorInterface;
use swactor::runtime::Ctx;

use crate::frame::{Frame, StreamId};
use super::wire::{DatastreamFrame, decode_delivery};

/// Receives legacy [`DatastreamFrame`] cluster messages and folds each decoded
/// delivery through `on_frame`. Spawn it, then publish its address under
/// [`DATASTREAM_SINK_NAME`] so legacy emitters can resolve and ship to it.
pub struct DatastreamSink {
    on_frame: Box<dyn FnMut(StreamId, Frame) + Send>,
}

/// The cluster name a [`DatastreamSink`] is published under. Emitters resolve
/// this to fill their `ClusterFrameSink` destination.
pub const DATASTREAM_SINK_NAME: &str = "datastream-sink";

impl DatastreamSink {
    /// Build a sink that folds every decoded delivery through `on_frame`.
    pub fn new(on_frame: impl FnMut(StreamId, Frame) + Send + 'static) -> Self {
        Self {
            on_frame: Box::new(on_frame),
        }
    }
}

impl ActorInterface for DatastreamSink {
    type Incoming = DatastreamFrame;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, msg: DatastreamFrame) {
        // Best-effort: a malformed datagram is dropped, never panics the sink.
        if let Ok((stream, frame)) = decode_delivery(&msg.payload) {
            (self.on_frame)(stream, frame);
        }
    }
}
