//! Core data model: the framed, channel-multiplexed stream (spec §4).
//!
//! A stream is identified by the producing node and lifetime. Frames carry a
//! stream-local numeric channel id plus the mux-assigned position and opaque
//! payload bytes. Channel names, payload content kinds, and display metadata live
//! in catalog descriptors that consumers receive before or alongside frames.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A position assigned by a node's mux (spec §5.2).
///
/// Positions are **monotonic** and **gap-free** within a single node's stream:
/// the mux never reuses one and never skips one in its numbering. A position
/// that is assigned but never delivered surfaces downstream as a missing
/// position — a detectable gap (spec §5.3, §7.5).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct Position(pub u64);

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Stream-local numeric channel id.
///
/// `ChannelId(0)` is reserved for the datastream frame-timing sidecar. Every
/// other id is allocated by the stream owner and is meaningful only with the
/// corresponding [`StreamId`]. Consumers resolve frames by `(stream, channel)`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct ChannelId(pub u32);

impl fmt::Display for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Payload content kind without schema details. Used for subscription filters.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChannelContentKind {
    Bytes,
    TextStream,
    JsonRecord,
}

/// How consumers should decode/display payload bytes for a channel.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChannelContent {
    Bytes,
    TextStream,
    JsonRecord { schema: Option<String> },
}

impl ChannelContent {
    pub fn kind(&self) -> ChannelContentKind {
        match self {
            ChannelContent::Bytes => ChannelContentKind::Bytes,
            ChannelContent::TextStream => ChannelContentKind::TextStream,
            ChannelContent::JsonRecord { .. } => ChannelContentKind::JsonRecord,
        }
    }
}

/// Where a stream originates from in the current process topology.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamOrigin {
    Orchestrator,
    Bootstrap,
    RemoteNode,
}

/// The stable identity of a node that produces a stream (spec §4.4, §8.1).
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(Arc<str>);

impl NodeId {
    /// Construct a node id from any string-like value.
    pub fn new(id: impl AsRef<str>) -> Self {
        NodeId(Arc::from(id.as_ref()))
    }

    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        NodeId::new(s)
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        NodeId::new(s)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({:?})", &self.0)
    }
}

impl Serialize for NodeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(NodeId::new)
    }
}

/// A lifetime discriminator distinguishing a node's incarnations (spec §8.4).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct Lifetime(pub u64);

/// Identifies exactly one stored stream: a node plus the life it was produced in.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct StreamId {
    /// Which node produced the stream.
    pub node: NodeId,
    /// Which life of that node.
    pub life: Lifetime,
}

impl StreamId {
    /// Construct a stream id from a node and a lifetime.
    pub fn new(node: impl Into<NodeId>, life: Lifetime) -> Self {
        StreamId {
            node: node.into(),
            life,
        }
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.node, self.life.0)
    }
}

/// Stream metadata declared by the stream owner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDescriptor {
    pub stream: StreamId,
    pub label: Option<String>,
    pub origin: StreamOrigin,
}

/// Channel metadata declared by the stream owner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelDescriptor {
    pub stream: StreamId,
    pub id: ChannelId,
    pub name: String,
    pub label: Option<String>,
    pub content: ChannelContent,
}

/// Globally resolved channel identity: a stream plus that stream's numeric lane.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct ChannelRef {
    pub stream: StreamId,
    pub channel: ChannelId,
}

/// A delivered frame with its stream-local channel resolved to a [`ChannelRef`].
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct FrameDelivery {
    pub channel: ChannelRef,
    pub position: Position,
    pub payload: Vec<u8>,
}

/// Catalog and frame events delivered to subscribers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatastreamEvent {
    StreamDeclared(StreamDescriptor),
    ChannelDeclared(ChannelDescriptor),
    Frame(FrameDelivery),
    StreamEnded(StreamId),
}

/// Source filter used by subscribers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceFilter {
    All,
    Origin(StreamOrigin),
    Node(NodeId),
    Stream(StreamId),
}

/// Channel filter used by subscribers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChannelFilter {
    All,
    Name(String),
    Prefix(String),
    Content(ChannelContentKind),
}

/// A subscription request for catalog metadata and future frame events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionRequest {
    pub sources: SourceFilter,
    pub channels: ChannelFilter,
}

impl SubscriptionRequest {
    pub fn all() -> Self {
        Self {
            sources: SourceFilter::All,
            channels: ChannelFilter::All,
        }
    }
}

/// The unit the mux emits (spec §4.1): bytes tagged with a channel and a position.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frame {
    /// The stream-local lane these bytes belong to.
    pub channel: ChannelId,
    /// The mux-assigned position within the node's stream.
    pub position: Position,
    /// The opaque payload bytes.
    pub payload: Vec<u8>,
}

impl Frame {
    /// Assemble a frame from its parts.
    pub fn new(channel: ChannelId, position: Position, payload: Vec<u8>) -> Self {
        Frame {
            channel,
            position,
            payload,
        }
    }
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dbg = f.debug_struct("Frame");
        dbg.field("channel", &self.channel)
            .field("position", &self.position);
        match std::str::from_utf8(&self.payload) {
            Ok(text) => dbg.field("payload", &text),
            Err(_) => dbg.field("payload", &format_args!("<{} bytes>", self.payload.len())),
        };
        dbg.finish()
    }
}
