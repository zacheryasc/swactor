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
/// Distribution-subsystem state — location cache, directory, registry, gossip
/// probes, and peer-auth. Periodic, consolidated; the parts of a node's
/// distribution view the `membership`/`identity` channels do not already carry.
pub const DIST_STATE: &str = "dist.state";
/// Datastore steady metrics — object/byte totals, op tallies, in-flight
/// transfers. Periodic; the recent-events timeline rides [`DATASTORE_EVENTS`].
pub const DATASTORE_STATE: &str = "datastore.state";
/// Per-actor runtime detail — one row per live actor, behind the aggregate
/// [`RUNTIME_STATS`]. Periodic.
pub const RUNTIME_ACTORS: &str = "runtime.actors";
/// Datastore operation events — a raw-text channel carrying one line per
/// recorded op (`<kind> <hash> <size>`), the `proc.*` model applied to ops.
pub const DATASTORE_EVENTS: &str = "datastore.events";

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

/// The datastore's text event channel (spec §6.2). A single channel, tailed by
/// a view to rebuild the recent-operations timeline — the streaming counterpart
/// of the old fixed-size event ring.
pub fn datastore_event() -> ChannelId {
    ChannelId::new(DATASTORE_EVENTS)
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
        | LIFECYCLE_COST | DIST_STATE | DATASTORE_STATE | RUNTIME_ACTORS => ChannelKind::Typed,
        DATASTORE_EVENTS => ChannelKind::Text,
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityRecord {
    pub node: String,
    /// The lifetime this stream belongs to (spec §8.4), echoed in-band so a
    /// view can confirm attribution.
    #[serde(default)]
    pub life: u64,
    /// Human-friendly node name (e.g. "swift-falcon"). Empty until assigned.
    #[serde(default)]
    pub node_name: String,
    /// The node's listen / endpoint address. Empty until the endpoint binds.
    #[serde(default)]
    pub listen_addr: String,
    /// This node's own relay URL when it runs an embedded relay. Empty otherwise.
    #[serde(default)]
    pub relay_url: String,
    /// Build version string (e.g. "branch @ hash"). Empty if unknown.
    #[serde(default)]
    pub version: String,
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

/// Distribution-subsystem state record. A periodic, consolidated view of the
/// cache / directory / registry / gossip / peer-auth state a node observes.
/// Members and their liveness ride the `membership` channel and node identity
/// rides `identity`, so they are deliberately absent here (no duplication).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DistributionState {
    #[serde(default)]
    pub cache_size: u32,
    #[serde(default)]
    pub cache_entries: Vec<CacheEntryRec>,
    #[serde(default)]
    pub directory_route_count: u32,
    #[serde(default)]
    pub registry_size: u32,
    #[serde(default)]
    pub registry_tombstones: u32,
    #[serde(default)]
    pub registry_entries: Vec<RegistryEntryRec>,
    #[serde(default)]
    pub recent_probe_targets: Vec<String>,
    /// "open" or "allow-list".
    #[serde(default)]
    pub peer_auth_mode: String,
    /// Authorized peers in allow-list mode; 0 in open mode.
    #[serde(default)]
    pub authorized_peer_count: u32,
}

/// One location-cache entry: which node an actor address resolves to.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEntryRec {
    #[serde(default)]
    pub actor_addr: String,
    #[serde(default)]
    pub node_id: String,
}

/// One cluster-registry entry (a named, signed actor location), possibly a tombstone.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryEntryRec {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub actor_addr: String,
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub tombstone: bool,
}

/// Datastore steady metrics. Periodic; the operation timeline rides the
/// event-driven [`DATASTORE_EVENTS`] text channel, so a quiet datastore that is
/// still serving reads keeps reporting accurate totals without event spam.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatastoreState {
    #[serde(default)]
    pub object_count: u64,
    #[serde(default)]
    pub total_bytes: u64,
    #[serde(default)]
    pub put_ops: u64,
    #[serde(default)]
    pub get_ops: u64,
    #[serde(default)]
    pub delete_ops: u64,
    #[serde(default)]
    pub objects: Vec<ObjectRec>,
    #[serde(default)]
    pub active_transfers: Vec<TransferRec>,
}

/// One stored object's summary (for the dashboard's object table).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRec {
    #[serde(default)]
    pub hash: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub size_bytes: u64,
}

/// One in-flight chunk transfer's progress.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferRec {
    #[serde(default)]
    pub hash: String,
    #[serde(default)]
    pub chunks_received: u64,
    #[serde(default)]
    pub chunks_total: u64,
}

/// Per-actor runtime detail — the rows behind the aggregate [`RuntimeStats`].
/// Periodic, on its own channel so a consumer that only wants the summary never
/// pays to decode the full table.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorRuntimeDetail {
    #[serde(default)]
    pub actors: Vec<ActorRec>,
}

/// One live actor's stats — a serde projection of the runtime's per-actor view.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorRec {
    #[serde(default)]
    pub address: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub mailbox_depth: u32,
    #[serde(default)]
    pub messages_processed: u64,
    #[serde(default)]
    pub last_msg_type: String,
    #[serde(default)]
    pub poisoned: bool,
    #[serde(default)]
    pub message_type_counts: Vec<(String, u64)>,
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
impl Record for DistributionState {
    const CHANNEL: &'static str = DIST_STATE;
}
impl Record for DatastoreState {
    const CHANNEL: &'static str = DATASTORE_STATE;
}
impl Record for ActorRuntimeDetail {
    const CHANNEL: &'static str = RUNTIME_ACTORS;
}
