//! Transport-port contracts for ring-backed edge byte streams.
//!
//! This is the *entire* contract a byte transport must satisfy for the
//! data-plane to drive it: open a writer for one edge, and report inbound
//! stream events. The iroh-driver crate provides the concrete implementation
//! over its QUIC endpoint; everything semantic — edges, rings, lifecycle,
//! object parsing — lives in this crate and consumes this port.

use crate::ids::{EdgeId, StreamId};

/// One observed edge-byte transport event, in transport vocabulary only.
///
/// Streams are unidirectional byte streams tagged with an edge id (the wire
/// preamble). The transport knows nothing about edges beyond passing the tag
/// through.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireEvent {
    /// A new inbound stream arrived, tagged for `edge_id`.
    StreamArrived {
        edge_id: EdgeId,
        stream_id: StreamId,
    },
    /// Bytes were read from an inbound stream.
    BytesRead {
        edge_id: EdgeId,
        stream_id: StreamId,
        bytes: Vec<u8>,
    },
    /// An inbound stream ended cleanly.
    StreamEnded {
        edge_id: EdgeId,
        stream_id: StreamId,
    },
    /// A stream or connection-level fault. `None` ids mean the fault could
    /// not be attributed to a specific edge or stream.
    StreamFault {
        edge_id: Option<EdgeId>,
        stream_id: Option<StreamId>,
        reason: WireFault,
    },
}

/// Why an edge byte stream faulted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireFault {
    ReadError,
    WriteError,
    ProtocolError,
}

/// Cloneable-ish handle for writing opaque bytes to one edge's peer.
pub trait EdgeWriter {
    fn send(&self, bytes: Vec<u8>) -> Result<(), String>;
}

/// Byte transport port the data-plane edge runtime drives.
///
/// `PeerAddr` is the transport's own peer-address notion (e.g. an iroh
/// `EndpointAddr`); the data-plane treats it as opaque.
pub trait EdgeTransport {
    type Writer: EdgeWriter;
    type PeerAddr: Clone;

    /// Open (or continue) the writer pumping bytes to `edge_id`'s peer.
    fn open_writer(
        &mut self,
        edge_id: EdgeId,
        peer: &Self::PeerAddr,
    ) -> Result<Self::Writer, String>;

    /// Drain all transport events observed since the last call.
    fn drain_events(&mut self) -> Vec<WireEvent>;
}
