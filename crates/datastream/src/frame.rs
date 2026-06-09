//! Core data model: the framed, channel-multiplexed stream (spec §4).
//!
//! These are the values that ride the seams a test observes (testing spec
//! §2): a [`Frame`] crosses the mux→transport boundary, and a [`StreamId`]
//! keys the reconstructed stream at ingest. Everything here is a plain
//! value type with no behavior — the behavior lives in the mux, ingest,
//! store, and views.

use std::fmt;
use std::sync::Arc;

/// A position assigned by a node's mux (spec §5.2).
///
/// Positions are **monotonic** and **gap-free** within a single node's
/// stream: the mux never reuses one and never skips one in its numbering.
/// A position that is assigned but never delivered surfaces downstream as
/// a missing position — a detectable gap (spec §5.3, §7.5).
///
/// Across nodes, positions are **not** comparable (spec §4.3): they order
/// frames within one node only.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Position(pub u64);

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The stable identity of a channel — the named lane a frame's bytes
/// belong to (spec §4.2, §3).
///
/// It is an opaque token: the pipe (mux, transport, ingest, store) never
/// interprets it. Only a *view* resolves it, through the catalog
/// ([`crate::catalog`]), into a codec. A token with no
/// registered codec is still carried and stored whole, then decoded later
/// (spec §6.3) — which is why this type is open (any string) rather than a
/// closed enum: a new channel is a new id, no pipe code changes.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChannelId(Arc<str>);

impl ChannelId {
    /// Construct a channel id from any string-like value.
    pub fn new(id: impl AsRef<str>) -> Self {
        ChannelId(Arc::from(id.as_ref()))
    }

    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ChannelId {
    fn from(s: &str) -> Self {
        ChannelId::new(s)
    }
}

impl From<String> for ChannelId {
    fn from(s: String) -> Self {
        ChannelId::new(s)
    }
}

impl fmt::Display for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChannelId({:?})", &self.0)
    }
}

/// The stable identity of a node that produces a stream (spec §4.4, §8.1).
///
/// Opaque to the pipe. In a deployment this is whatever durable id the
/// system already assigns a machine (e.g. its public key); tests use
/// readable names. A frame records the producing *node*, never a producer
/// identity within it (spec §4.4).
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

/// A lifetime discriminator distinguishing a node's incarnations (spec
/// §8.4). A node that dies and is re-rented starts a new lifetime, so its
/// fresh stream does not collide with or append to its prior life.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct Lifetime(pub u64);

/// Identifies exactly one stored stream: a node plus the life it was
/// produced in (spec §8.4).
///
/// This is the ingest key. Two streams with the same [`NodeId`] but
/// different [`Lifetime`] are different streams and MUST NOT merge — that
/// is what lets a re-incarnated node not append to its prior life.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct StreamId {
    /// Which node produced the stream.
    pub node: NodeId,
    /// Which life of that node.
    pub life: Lifetime,
}

impl StreamId {
    /// Construct a stream id from a node and a lifetime.
    pub fn new(node: impl Into<NodeId>, life: Lifetime) -> Self {
        StreamId { node: node.into(), life }
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.node, self.life.0)
    }
}

/// The unit the mux emits (spec §4.1): bytes tagged with a channel and a
/// position.
///
/// The `payload` is **opaque** to everything between the producer and a
/// view — the mux, the transport, and storage treat it as bytes and never
/// interpret it (spec §4.1). A typed event and a log line are the same
/// kind of thing here: bytes on a channel.
///
/// Per the `// USER:` annotation on spec §4.1/§5.2 there is no per-frame
/// wall-clock timestamp: frames are ordered and correlated by position
/// alone.
#[derive(Clone, PartialEq, Eq)]
pub struct Frame {
    /// The lane these bytes belong to.
    pub channel: ChannelId,
    /// The mux-assigned position within the node's stream.
    pub position: Position,
    /// The opaque payload bytes.
    pub payload: Vec<u8>,
}

impl Frame {
    /// Assemble a frame from its parts.
    pub fn new(channel: impl Into<ChannelId>, position: Position, payload: Vec<u8>) -> Self {
        Frame { channel: channel.into(), position, payload }
    }
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Render the payload as text when it is valid UTF-8 (log lines,
        // JSON records both are) so debug output is readable; fall back to
        // a byte count for genuinely binary payloads.
        let mut dbg = f.debug_struct("Frame");
        dbg.field("channel", &self.channel).field("position", &self.position);
        match std::str::from_utf8(&self.payload) {
            Ok(text) => dbg.field("payload", &text),
            Err(_) => dbg.field("payload", &format_args!("<{} bytes>", self.payload.len())),
        };
        dbg.finish()
    }
}
