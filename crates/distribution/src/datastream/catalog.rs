//! The channel catalog (spec §6): the kinds of observation the datastream
//! carries, as channels, plus the codecs that interpret typed channels.
//!
//! This is the schema contract between producers and views. The *pipe*
//! never consults it — only producers (to tag bytes) and views (to decode)
//! do. Adding a channel here, or teaching a view a new codec, changes
//! nothing in the mux, transport, ingest, or store (spec §6.3, §9.3).
//!
//! Typed channels use JSON as their codec. JSON is forgiving by design:
//! decoding ignores unknown fields and `#[serde(default)]` fills missing
//! ones, so a producer and a consumer can evolve a record independently
//! (spec §6.3 version skew). A typed channel decodes to a record; a
//! raw-text channel's "codec" is the identity and a view treats it as
//! lines (spec §4.2).

use serde::{Deserialize, Serialize};

use super::frame::ChannelId;

// ── Typed channel ids (spec §6.1) ──────────────────────────────────────
//
// Each is a stable token. They are `&'static str` constants, not an enum,
// so that "unknown channel" is simply "an id with no entry here" and a
// newer producer's channel still lands whole in the store (spec §6.3).

/// Identity / boot — emitted first in a stream; identifies the node and
/// its context so the consumer can attribute the stream. Event-driven.
pub const IDENTITY: &str = "identity";
/// Host / resource samples — periodic snapshots of machine resources.
pub const HOST_RESOURCE: &str = "host.resource";
/// Transport internals — the node's connectivity to peers and the relay.
pub const TRANSPORT_INTERNALS: &str = "transport.internals";
/// Membership / liveness — the node's view of which peers are alive,
/// suspect, or dead, and transitions thereof. Event-driven.
pub const MEMBERSHIP: &str = "membership";
/// Runtime stats — the node's own actor-runtime metrics. Periodic.
pub const RUNTIME_STATS: &str = "runtime.stats";
/// Provider / lifecycle / cost — coarse lifecycle and cost facts about the
/// node as a rented resource.
pub const LIFECYCLE_COST: &str = "provider.lifecycle";

/// Which standard stream a span of process output came from (spec §6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcStream {
    Stdout,
    Stderr,
}

impl ProcStream {
    fn as_str(self) -> &'static str {
        match self {
            ProcStream::Stdout => "stdout",
            ProcStream::Stderr => "stderr",
        }
    }
}

/// A raw-text process-output channel (spec §6.2). This is a *family*
/// parameterized by process label and stream, so managing a new process
/// introduces channels without defining new channel *types*:
/// `proc.<label>.stdout` / `proc.<label>.stderr`.
pub fn process_output(label: &str, stream: ProcStream) -> ChannelId {
    ChannelId::new(format!("proc.{label}.{}", stream.as_str()))
}

/// How a view should treat a channel's bytes, decided by the catalog at
/// read time. An id the catalog does not know is [`ChannelKind::Opaque`]
/// and degrades to raw bytes (spec §6.3, §9.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelKind {
    /// Decodes to a structured record under a JSON codec.
    Typed,
    /// Opaque text; a view treats it as lines.
    Text,
    /// Unknown to this consumer; retained and shown as raw bytes.
    Opaque,
}

/// Classify a channel id. Known typed ids and the `proc.*` text family are
/// recognized; everything else is [`ChannelKind::Opaque`].
pub fn classify(channel: &ChannelId) -> ChannelKind {
    let id = channel.as_str();
    match id {
        IDENTITY | HOST_RESOURCE | TRANSPORT_INTERNALS | MEMBERSHIP | RUNTIME_STATS
        | LIFECYCLE_COST => ChannelKind::Typed,
        _ if id.starts_with("proc.") => ChannelKind::Text,
        _ => ChannelKind::Opaque,
    }
}

// ── Typed records (realistic shapes for spec §6.1 channels) ────────────
//
// These define the bytes a producer actually emits and a metric view
// actually decodes. They carry `#[serde(default)]` so a missing field
// decodes to a default (version skew, spec §6.3). Unknown fields are
// ignored by serde_json on decode for the same reason.

/// A typed channel record: a record knows its own channel and round-trips
/// through the JSON codec. `decode(encode(r)) == r` is the codec contract
/// (testing spec §3, §5).
pub trait Record: Serialize + for<'de> Deserialize<'de> + Sized {
    /// The channel this record is carried on.
    const CHANNEL: &'static str;

    /// The channel id this record is carried on.
    fn channel() -> ChannelId {
        ChannelId::new(Self::CHANNEL)
    }

    /// Encode this record to its opaque payload bytes.
    fn encode(&self) -> Vec<u8> {
        // Records are plain data; JSON serialization of them cannot fail.
        serde_json::to_vec(self).expect("record serializes to JSON")
    }

    /// Decode a payload back into the record. Fails (gracefully) if the
    /// bytes are not this record's shape — a view degrades to raw bytes
    /// (spec §9.3) rather than propagating the error.
    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

/// Identity / boot record (spec §6.1). A node is generic — its job is resolved
/// orchestrator-side by SWIM name, so the stream carries only the node id and
/// the lifetime it belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityRecord {
    pub node: String,
    /// The lifetime this stream belongs to (spec §8.4), echoed in-band so a
    /// view can confirm attribution.
    #[serde(default)]
    pub life: u64,
}

/// Host / resource sample (spec §6.1). The metric projection (spec §9.2)
/// decodes a series of these.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceSample {
    #[serde(default)]
    pub cpu_pct: f32,
    #[serde(default)]
    pub mem_used_mb: u32,
    #[serde(default)]
    pub mem_total_mb: u32,
    #[serde(default)]
    pub gpu_pct: f32,
    #[serde(default)]
    pub disk_used_gb: u32,
    #[serde(default)]
    pub net_rx_kbps: u32,
    #[serde(default)]
    pub net_tx_kbps: u32,
}

/// Transport-internals record (spec §6.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransportInternals {
    #[serde(default)]
    pub relay_connected: bool,
    #[serde(default)]
    pub direct_peers: u32,
    #[serde(default)]
    pub relay_peers: u32,
    #[serde(default)]
    pub rtt_ms_p50: u32,
}

/// Membership / liveness transition (spec §6.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipTransition {
    pub peer: String,
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub reason: String,
}

/// Runtime-stats record (spec §6.1).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeStats {
    #[serde(default)]
    pub actors_live: u32,
    #[serde(default)]
    pub mailbox_depth: u32,
    #[serde(default)]
    pub scheduled_tasks: u32,
}

/// Provider / lifecycle / cost record (spec §6.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LifecycleCost {
    pub phase: String,
    #[serde(default)]
    pub cost_usd_per_hr: f32,
    #[serde(default)]
    pub uptime_s: u64,
}

impl Record for IdentityRecord {
    const CHANNEL: &'static str = IDENTITY;
}
impl Record for ResourceSample {
    const CHANNEL: &'static str = HOST_RESOURCE;
}
impl Record for TransportInternals {
    const CHANNEL: &'static str = TRANSPORT_INTERNALS;
}
impl Record for MembershipTransition {
    const CHANNEL: &'static str = MEMBERSHIP;
}
impl Record for RuntimeStats {
    const CHANNEL: &'static str = RUNTIME_STATS;
}
impl Record for LifecycleCost {
    const CHANNEL: &'static str = LIFECYCLE_COST;
}
