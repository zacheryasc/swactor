//! Point-in-time snapshot of a node's local view (T1.4).
//!
//! A snapshot is the full *local* dump of identity + reachability log
//! + event tail since the last snapshot, plus (in later tiers) iroh
//! internals, SWIM internals, and host context. Snapshots are the
//! unit the post-processor stitches together across nodes.

use serde::{Deserialize, Serialize};

use crate::diagnostics::event::{ConnType, EventRecord};
use crate::diagnostics::identity::Identity;
use crate::diagnostics::reachability::PeerReachability;

/// Why a particular snapshot was taken. Drives differing handling in
/// the post-processor — e.g. transition snapshots are prioritized when
/// rendering the one-pager (`DIAGNOSTICS_PLAN.md` A.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data")]
pub enum SnapshotTrigger {
    /// Fired by the periodic timer (default 5s).
    Periodic,
    /// Fired by a local SWIM transition. The string is an event id
    /// (or arbitrary correlation tag).
    Transition(String),
    /// Fired because the collector returned `{"hints":
    /// {"snapshot_now": true}}` in a response.
    OnDemand,
}

/// A full snapshot record as it lands on the collector.
///
/// `snapshot_id` is opaque per-process and unique across the process
/// lifetime; useful for de-duplicating retries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub identity: Identity,
    pub run_id: String,
    pub snapshot_id: String,
    pub wall_ms: u64,
    pub monotonic_seq: u64,
    pub trigger: SnapshotTrigger,
    pub body: SnapshotBody,
}

/// Tier-1 / tier-2 / tier-3 fields collapse into one struct with
/// `Option` slots so unimplemented tiers stay `None`. The wire form
/// only writes the fields that are populated, keeping bundles small.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotBody {
    /// Per-remote-peer reachability log (T1.2). Always present in
    /// tier-1 snapshots; empty before any peers are observed.
    pub reachability: Vec<PeerReachability>,
    /// Event tail since the previous snapshot.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<EventRecord>,
    /// Tier-2 iroh-internal scrape (T2.1 + T2.2 + T2.3). `None` when
    /// no introspector is installed; populated by the transport-side
    /// implementation (`diagnostics::iroh_introspect`) on iroh builds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iroh: Option<Tier2IrohState>,
    /// Tier-2 SWIM internal scrape (T2.6). `None` when no SWIM
    /// introspector is installed; populated by
    /// `diagnostics::swim_introspect` whenever a `SwimNode` is wired
    /// up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swim: Option<Tier2SwimState>,
    /// Tier-3 host context (T3.1 + T3.2). `None` when no host
    /// introspector is installed; populated by
    /// `diagnostics::host_introspect`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<Tier3HostState>,
    /// Tier-3 outbound probe state (T3.3). `None` when no probe
    /// introspector is installed; populated by
    /// `diagnostics::probes::ProbeScheduler`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probes: Option<Tier3ProbeState>,
    /// Tier-3 vast.ai-side context (T3.4). `None` when no vastai
    /// introspector is installed; populated by
    /// `diagnostics::vastai_context::VastaiContext`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vastai: Option<Tier3VastaiContext>,
    /// Tier-3 process resource snapshot (T3.5). `None` when no process
    /// introspector is installed; populated by
    /// `diagnostics::process_stats::ProcessStats`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process: Option<Tier3ProcessStats>,
    /// Local name→address registry view. `None` when no registry
    /// introspector is installed; populated by
    /// `diagnostics::registry_introspect::RegistryIntrospect` from
    /// the local `ClusterRegistry`. Lets the post-processor answer
    /// "did this node ever register `pp-entry`?" without inferring it
    /// from gossip-receive events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry: Option<Tier2Registry>,
    /// Relay-side server view (spec §1). Populated only by relay
    /// binaries — node-role and orchestrator-role snapshots leave it
    /// `None`. Carries end-of-run totals (active sessions, opens,
    /// closes, bytes, breakdown by close reason).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_server: Option<Tier3RelayServer>,
    /// Tier-3 subprocesses owned by this node (spec §4). Populated
    /// only when a [`SubprocessIntrospector`] has been installed.
    /// Generic over the calling use case: the introspector knows
    /// about (label, PID, parent PID); decisions about *which*
    /// subprocesses to register live in the calling crate. The
    /// existing `process_stats` block remains for the *parent* process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subprocess: Option<Tier3SubprocessState>,
}

/// Iroh-internal snapshot fields (`DIAGNOSTICS_PLAN.md` T2.1 + T2.2 + T2.3).
///
/// Populated by an [`IrohIntrospector`] installed on the aggregator.
/// All values are best-effort: fields the running iroh version does
/// not expose appear as `None`, with the gaps enumerated in
/// `api_gaps` so the post-processor can render them explicitly
/// rather than silently treat missing data as zero.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier2IrohState {
    /// This node's home relay URL at the moment of capture.
    /// `None` if iroh has not picked one yet (e.g. RelayMode::Disabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home_relay_url: Option<String>,
    /// Per-remote-peer iroh-side view (T2.1).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub peers: Vec<Tier2Peer>,
    /// Flat dump of `iroh-metrics` counters and gauges (T2.3). Post-
    /// processor computes deltas across snapshots.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics: Vec<MetricSample>,
    /// Per-peer summary of this node's connection cache lifecycle
    /// (T2.4). Entries are only present for peers we have at some
    /// point dialed or successfully connected to — peers we only
    /// *received from* (and never sent to) won't show up here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connection_cache: Vec<Tier2ConnectionCache>,
    /// Fields the current iroh version does not expose, listed once
    /// per snapshot so the bundle reader does not confuse "absent"
    /// with "zero." Matches the `iroh_api_missing` event kinds.
    ///
    /// Computed from observed per-peer field population each scrape:
    /// a candidate field name is included iff no scraped peer carried
    /// a natively-sourced value for it. Bumping iroh to a version that
    /// populates a previously-missing field causes the gap to vanish
    /// from this list without further code changes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub api_gaps: Vec<String>,
    /// Version of the `iroh` crate this binary was linked against,
    /// taken from `Cargo.lock` at build time. Tier-2 carries it on
    /// every snapshot so the bundle reader does not need to scan the
    /// event stream to know what iroh version ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iroh_version: Option<String>,
    /// State of this node's tunnel to its home relay (spec §2). This
    /// is the answer to "is my tunnel up right now," kept separate
    /// from per-peer connection state — a peer connection going dead
    /// does not by itself prove the underlying relay tunnel died.
    /// `None` when no relay introspector has populated it yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_session: Option<Tier2RelaySession>,
    /// Wall-clock millis at the moment the introspector last
    /// refreshed its cache.
    #[serde(default)]
    pub scraped_at_ms: u64,
}

/// State of a node's tunnel to its home relay
/// (`N3_OBSERVABILITY_UPGRADE_SPEC.md` §2).
///
/// The discriminator pattern: `status_source` says where `status` came
/// from. `"iroh"` means we read it natively from the transport
/// library; `"derived"` means we inferred it from address-watcher
/// state. When `status` is `"unknown"`, the reader knows we genuinely
/// couldn't ask — versus an `"unknown"` that means "the tunnel is in
/// an unknown sub-state." The spec is explicit: a bundle reader must
/// never have to guess which of those is meant.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier2RelaySession {
    /// Relay URL the node is currently using. `None` when iroh has
    /// not picked (or no longer holds) a home relay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_url: Option<String>,
    /// One of `"connected"`, `"connecting"`, `"disconnected"`, or
    /// `"unknown"`. Strings so the wire stays forgiving when iroh
    /// adds new states.
    pub status: String,
    /// `"iroh"` when the value came from a native iroh API,
    /// `"derived"` when the introspector synthesized it from other
    /// signals (e.g. presence of a home-relay URL in `watch_addr()`).
    pub status_source: String,
    /// Wall-clock millis of the most recent transition between two
    /// distinct `status` values. `None` until at least one transition
    /// has been observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_changed_at_ms: Option<u64>,
    /// Wall-clock millis at which the current status was first
    /// entered. Equals `status_changed_at_ms` after the first change;
    /// equals the introspector's first observation otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_entered_at_ms: Option<u64>,
    /// Last moment the node successfully sent bytes over the tunnel.
    /// `None` when the linked iroh version does not expose this and
    /// the introspector has no other way to know.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_send_at_ms: Option<u64>,
    /// Last moment the node received bytes over the tunnel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_recv_at_ms: Option<u64>,
    /// Lifetime byte counters in each direction over the tunnel.
    /// `None` when not exposed; see `api_gaps`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx_bytes_total: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rx_bytes_total: Option<u64>,
}

/// Per-peer iroh-side view (`DIAGNOSTICS_PLAN.md` T2.1). Fields that
/// iroh exposes are populated directly; the rest stay `None` and are
/// listed in [`Tier2IrohState::api_gaps`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tier2Peer {
    pub peer_node_id_hex: String,
    /// `Direct` if any active IP addr exists, `Relay` if any active
    /// relay addr exists, `Mixed` if both, `None` if iroh has no active
    /// path. `None` is *not* the same as "iroh hasn't heard of this
    /// peer" — that case yields a peer entry whose vectors are empty
    /// and `conn_type` is `None`. The corresponding `conn_type_source`
    /// disambiguates whether the value came from iroh natively or was
    /// derived from address-usage signal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conn_type: Option<ConnType>,
    /// Source of `conn_type` for this peer. `"iroh"` when iroh's
    /// `RemoteInfo` exposes a connection-type field directly,
    /// `"derived"` when synthesized from address usage. Absent only
    /// when `conn_type` itself is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conn_type_source: Option<String>,
    /// Latency in milliseconds reported by iroh's `RemoteInfo`. `None`
    /// when the linked iroh version does not expose it; in that case
    /// the canonical field name appears in [`Tier2IrohState::api_gaps`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    /// Wall-clock millis of the last time iroh used this peer's
    /// connection. `None` when the linked iroh version does not expose
    /// it; see `api_gaps`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_ms: Option<u64>,
    /// Wall-clock millis of the last time iroh received from this peer.
    /// `None` when the linked iroh version does not expose it; see
    /// `api_gaps`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_received_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub direct_addresses: Vec<TransportAddrWire>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relay_urls: Vec<TransportAddrWire>,
    /// Per-address provenance strings (e.g. which discovery method
    /// produced each entry). `None` when the linked iroh version does
    /// not expose it; see `api_gaps`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addr_sources: Option<Vec<String>>,
}

impl Tier2IrohState {
    /// Canonical field-name list for *per-peer* fields the bundle
    /// reader may expect iroh to populate. Used by
    /// [`Self::compute_api_gaps`] to derive the runtime gap list from
    /// observed peer slots.
    pub const CANDIDATE_PEER_FIELDS: &'static [&'static str] = &[
        "RemoteInfo.conn_type",
        "RemoteInfo.latency_ms",
        "RemoteInfo.last_used_ms",
        "RemoteInfo.last_received_ms",
        "TransportAddrInfo.source",
    ];

    /// Canonical field-name list for *relay-tunnel* fields the bundle
    /// reader may expect iroh to populate. Computed against the
    /// observed [`Tier2RelaySession`] (spec §2 cross-references §6 —
    /// when the linked iroh doesn't expose tunnel state natively, the
    /// field is reported as `unknown` + derived, and its canonical
    /// name lands in `api_gaps`).
    pub const CANDIDATE_RELAY_FIELDS: &'static [&'static str] = &[
        "RelayTunnel.status",
        "RelayTunnel.last_send_at_ms",
        "RelayTunnel.last_recv_at_ms",
        "RelayTunnel.tx_bytes_total",
        "RelayTunnel.rx_bytes_total",
    ];

    /// Compute the list of API gaps for a set of peers just scraped
    /// from iroh. Backwards-compatible name for callers that only
    /// have peer data; prefer [`Self::compute_api_gaps_full`] when
    /// the relay session is also available.
    pub fn compute_api_gaps(peers: &[Tier2Peer]) -> Vec<String> {
        Self::compute_api_gaps_full(peers, None)
    }

    /// Compute the list of API gaps for a scrape, considering both
    /// per-peer fields and the relay-tunnel state.
    ///
    /// A candidate appears in the result iff the corresponding
    /// observation slot is not natively populated. For `conn_type`
    /// "native" means `conn_type_source == "iroh"`; for relay-tunnel
    /// status, "native" means `status_source == "iroh"`; for the
    /// pure `Option` fields, "native" means `Some(_)`. When nothing
    /// has been scraped at all, every candidate stays in the gap
    /// list — the bundle reader has no evidence iroh exposes
    /// anything.
    pub fn compute_api_gaps_full(
        peers: &[Tier2Peer],
        relay: Option<&Tier2RelaySession>,
    ) -> Vec<String> {
        let mut out: Vec<String> = Self::CANDIDATE_PEER_FIELDS
            .iter()
            .filter(|name| !peers.iter().any(|p| Self::peer_populates_field(p, name)))
            .map(|s| (*s).to_string())
            .collect();
        for name in Self::CANDIDATE_RELAY_FIELDS {
            let populated = relay
                .map(|r| Self::relay_populates_field(r, name))
                .unwrap_or(false);
            if !populated {
                out.push((*name).to_string());
            }
        }
        out
    }

    fn peer_populates_field(peer: &Tier2Peer, field: &str) -> bool {
        match field {
            "RemoteInfo.conn_type" => peer.conn_type_source.as_deref() == Some("iroh"),
            "RemoteInfo.latency_ms" => peer.latency_ms.is_some(),
            "RemoteInfo.last_used_ms" => peer.last_used_ms.is_some(),
            "RemoteInfo.last_received_ms" => peer.last_received_ms.is_some(),
            "TransportAddrInfo.source" => peer.addr_sources.is_some(),
            _ => false,
        }
    }

    fn relay_populates_field(relay: &Tier2RelaySession, field: &str) -> bool {
        match field {
            "RelayTunnel.status" => relay.status_source == "iroh",
            "RelayTunnel.last_send_at_ms" => relay.last_send_at_ms.is_some(),
            "RelayTunnel.last_recv_at_ms" => relay.last_recv_at_ms.is_some(),
            "RelayTunnel.tx_bytes_total" => relay.tx_bytes_total.is_some(),
            "RelayTunnel.rx_bytes_total" => relay.rx_bytes_total.is_some(),
            _ => false,
        }
    }
}

/// Per-peer connection-cache aggregate (`DIAGNOSTICS_PLAN.md` T2.4).
///
/// One entry per peer that this node has tried to connect to. The
/// fields capture the cache lifetime so the post-processor can answer
/// "when did we last successfully send to peer P? when was the cache
/// invalidated and why?" without having to fold the
/// `ConnectionCacheHit/Miss/Invalidated` event stream itself.
///
/// `generation` mirrors the value carried in the per-event
/// [`Event::ConnectionCacheHit`](crate::diagnostics::Event::ConnectionCacheHit) /
/// [`Event::ConnectionCacheInvalidated`](crate::diagnostics::Event::ConnectionCacheInvalidated)
/// payloads so the bundle reader can stitch a cache aggregate to its
/// per-touch events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tier2ConnectionCache {
    pub peer_node_id_hex: String,
    /// Monotonically increasing per-peer. Bumped every time the
    /// driver inserts a fresh `iroh::Connection` for this peer.
    pub generation: u64,
    /// Wall-clock millis at which the *current* cached connection
    /// was inserted. `None` if no connection has ever been cached
    /// (i.e. only misses so far).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_successful_send_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_reason: Option<String>,
    /// What iroh thought the connection type was the last time we
    /// touched this peer's cache. Lifted from the latest tier-2 peer
    /// scrape at snapshot time, so reads "current" rather than
    /// "at literal moment of last use." Good enough for the
    /// post-processor: it correlates with the per-touch events on the
    /// event stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_conn_type_at_last_use: Option<ConnType>,
}

/// A transport address (relay URL or IP socket) with iroh's view of
/// whether it is currently in use. `usage` is `"active"` or
/// `"inactive"`, mirroring iroh's `TransportAddrUsage`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportAddrWire {
    pub addr: String,
    pub usage: String,
}

/// A single iroh-metrics counter or gauge captured into the snapshot.
/// Untouched units; post-processor decides what to do with deltas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSample {
    pub group: String,
    pub name: String,
    pub value: MetricValueWire,
}

/// Wire form of `iroh_metrics::MetricValue`. Decoupled so the
/// snapshot schema does not break when iroh-metrics gains variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum MetricValueWire {
    Counter(u64),
    Gauge(i64),
    Histogram { count: u64, sum: f64 },
}

/// Anything that knows how to read iroh's internal state into a
/// [`Tier2IrohState`]. Installed on the aggregator via
/// [`crate::diagnostics::Aggregator::set_iroh_introspector`].
///
/// Production wires up `crate::diagnostics::iroh_introspect::IrohIntrospect`
/// (only available when the `iroh` feature is on). Tests can use any
/// implementation that fits the assertion they want to make — this
/// trait is intentionally tiny.
pub trait IrohIntrospector: Send + Sync {
    fn capture(&self) -> Tier2IrohState;
}

/// SWIM-internal snapshot fields (`DIAGNOSTICS_PLAN.md` T2.6).
///
/// Self-describing — the configured timeouts and gossip parameters
/// land in [`Self::config`] so the bundle reader does not need to
/// guess what the protocol was tuned to during the run.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier2SwimState {
    /// Configured timeouts + fanouts at the top so a partial bundle
    /// read still tells you what the protocol was set up for.
    pub config: Tier2SwimConfig,
    /// Hex-encoded `NodeId` of the node owning this snapshot. Lets
    /// the post-processor disambiguate self vs. peers when the
    /// surrounding identity block is unavailable.
    pub self_node_id_hex: String,
    /// Local SWIM incarnation at scrape time. Bumped every time we
    /// refute a suspicion against ourselves.
    pub self_incarnation: u64,
    /// Local generation of this node's gossiped metadata (relay URL
    /// + name). Cluster-wide metadata version comparisons happen by
    /// pairing this with each peer's [`Tier2SwimPeer::metadata_version_seen`].
    pub metadata_local_version: u64,
    /// Per-peer SWIM-side view.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub peers: Vec<Tier2SwimPeer>,
    /// Bounded ring buffer of recently *received* SWIM messages
    /// (oldest first). Cap is documented in
    /// [`Tier2SwimState::RECENT_MESSAGES_CAP`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_messages: Vec<Tier2SwimMessage>,
    /// Wall-clock millis at the moment the introspector built this
    /// snapshot.
    #[serde(default)]
    pub scraped_at_ms: u64,
}

impl Tier2SwimState {
    /// Capacity of the recent-messages ring buffer
    /// (`DIAGNOSTICS_PLAN.md` T2.6: "~64").
    pub const RECENT_MESSAGES_CAP: usize = 64;
}

/// Configured SWIM timeouts and fanouts (`DIAGNOSTICS_PLAN.md` T2.6).
///
/// Captured once at introspector install time — these fields do not
/// change at runtime in this implementation. Unit suffixes are
/// explicit (`_ticks`, `_k`) so the bundle reader is never left
/// wondering whether a value is in ms or in protocol ticks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier2SwimConfig {
    pub probe_interval_ticks: u64,
    pub probe_timeout_ticks: u64,
    pub suspicion_timeout_ticks: u64,
    pub indirect_probes_k: u32,
    pub dead_reprobe_interval_ticks: u64,
    /// Λ (lambda) gossip-fanout multiplier — each membership update
    /// is piggybacked `Λ * ceil(log2(n))` times.
    pub gossip_fanout_lambda: u32,
    /// Max number of piggybacked membership updates per outgoing
    /// message. The `max_piggyback` knob on `SwimNode`.
    pub max_piggyback: u32,
    /// `"periodic"` or `"reactive(<safety_sweep_interval>)"`.
    pub probe_mode: String,
}

/// Per-peer SWIM block (`DIAGNOSTICS_PLAN.md` T2.6).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tier2SwimPeer {
    pub peer_node_id_hex: String,
    pub state: crate::diagnostics::event::PeerState,
    pub incarnation: u64,
    /// Wall-clock millis when this peer was last sent a SWIM ping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_ping_sent_at_ms: Option<u64>,
    /// Wall-clock millis when this peer last acked us.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_ack_received_at_ms: Option<u64>,
    /// Wall-clock millis when this peer last pinged us.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_ping_received_at_ms: Option<u64>,
    /// Wall-clock millis when this peer's suspect timer last
    /// started. `None` if the peer is currently Alive or Dead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspect_started_at_ms: Option<u64>,
    /// Latest seen metadata generation for this peer (read off the
    /// dissemination layer's per-node store).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_version_seen: Option<u64>,
}

/// A single received SWIM message envelope captured into the recent-
/// messages ring buffer.
///
/// `kind` is `"ping"`, `"ack"`, `"ping_req"`, `"indirect_ack"`,
/// `"join_request"`, or `"join_response"`. Strings rather than an
/// enum so adding a new SWIM message type does not break the schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tier2SwimMessage {
    pub kind: String,
    pub peer_node_id_hex: String,
    pub at_ms: u64,
    /// Protocol sequence number when the message carries one
    /// (pings/acks). Absent on join messages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
}

/// SWIM-side analogue of [`IrohIntrospector`]. Installed on the
/// aggregator via [`crate::diagnostics::Aggregator::set_swim_introspector`].
/// Production wires up `crate::diagnostics::swim_introspect::SwimIntrospect`.
pub trait SwimIntrospector: Send + Sync {
    fn capture(&self) -> Tier2SwimState;
}

/// Local name registry view (name → actor address). The post-processor
/// uses this to verify name-publication independent of gossip — every
/// snapshot from a node that owns a name carries it here, so absence
/// at scrape time means the node never called `register_name` (vs.
/// "called it but gossip never propagated").
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier2Registry {
    /// One entry per known name (live or tombstoned).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<Tier2RegistryEntry>,
    /// Cached count of tombstone entries. Redundant with iterating
    /// `entries`, but cheap and lets the post-processor render the
    /// "N live, M tombstone" summary without a scan.
    pub tombstone_count: u64,
    /// Monotonic logical clock from the local registry at scrape time.
    /// Lets the post-processor order two snapshots from the same node
    /// even when wall-clock samples collide.
    pub clock: u64,
    /// Wall-clock millis at the moment the introspector built this
    /// snapshot.
    #[serde(default)]
    pub scraped_at_ms: u64,
}

/// One name in the registry as the local node sees it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tier2RegistryEntry {
    pub name: String,
    /// Hex-encoded `ActorAddress`. 32-byte address rendered as 64 hex
    /// chars; matches the format used for `peer_node_id_hex`.
    pub actor_addr_hex: String,
    /// Hex-encoded `NodeId` of the node that owns this binding. Equal
    /// to `Tier2Registry`'s containing identity when the local node
    /// owns the name; different when the entry was learned via gossip.
    pub owner_node_id_hex: String,
    /// Per-name dissemination generation. Bumped each time the owner
    /// re-registers under the same name.
    pub generation: u64,
    /// Logical timestamp from the local registry's clock at the moment
    /// this entry was inserted/updated. Not wall-clock; useful only for
    /// ordering relative to other entries from the *same* node.
    #[serde(default)]
    pub logical_timestamp: u64,
    /// `true` for unregistered names that are still being gossiped as
    /// tombstones. Lets the post-processor distinguish "never seen"
    /// from "seen and revoked."
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_tombstone: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// Registry-side analogue of [`SwimIntrospector`]. Installed on the
/// aggregator via [`crate::diagnostics::Aggregator::set_registry_introspector`].
/// Production wires up
/// `crate::diagnostics::registry_introspect::RegistryIntrospect`.
pub trait RegistryIntrospector: Send + Sync {
    fn capture(&self) -> Tier2Registry;
}

/// Host-side snapshot fields (`DIAGNOSTICS_PLAN.md` T3.1 + T3.2).
///
/// `network` and `dns` are independently refreshed at ~30s cadence —
/// the host scrape walks /proc, the DNS scrape resolves known relay
/// URLs. Each value falls back to `None` (or an empty vector) on
/// non-Linux hosts or when a required capability is missing, rather
/// than erroring; gaps are surfaced through one-time `Error` events
/// on the event stream.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier3HostState {
    /// Most recent host-network scrape. `None` if the scraper has not
    /// yet completed its first refresh, or if the host is non-Linux.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<Tier3HostNetwork>,
    /// One entry per known relay URL. Empty until the introspector is
    /// told about any URLs (via
    /// [`crate::diagnostics::host_introspect::HostIntrospect::add_dns_target`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dns: Vec<Tier3DnsResolution>,
    /// Wall-clock millis at the moment this capture was produced.
    #[serde(default)]
    pub scraped_at_ms: u64,
}

/// Host network scrape (`DIAGNOSTICS_PLAN.md` T3.1).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier3HostNetwork {
    /// All visible interfaces with their address sets and link state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub interfaces: Vec<Tier3Interface>,
    /// Parsed default routes (both v4 and v6 when present).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub default_routes: Vec<Tier3Route>,
    /// UDP socket table snapshot (`/proc/net/udp` + `udp6`). Local and
    /// remote addresses are pre-decoded into human-readable form.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub udp_sockets: Vec<Tier3UdpSocket>,
    /// Best-effort count of conntrack rows. `None` when the host does
    /// not expose `/proc/sys/net/netfilter/nf_conntrack_count` or the
    /// process lacks `CAP_NET_ADMIN`-equivalent permission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conntrack_count: Option<u64>,
    /// Reading of `/proc/sys/net/ipv6/conf/all/disable_ipv6`. `Some(true)`
    /// when IPv6 is enabled, `Some(false)` when explicitly disabled,
    /// `None` when the file could not be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ipv6_enabled: Option<bool>,
    /// Nameservers listed in `/etc/resolv.conf`, in declaration order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolv_conf_nameservers: Vec<String>,
    /// Kernel UDP counters from `/proc/net/snmp` (spec §11).
    /// `None` on non-Linux, when the file could not be read, or when
    /// the kernel did not expose the row we expected. Bundle reader
    /// must treat absent as "we couldn't ask", never as zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub udp_kernel_stats: Option<Tier3UdpKernelStats>,
    /// Wall-clock millis at the moment of this scrape.
    #[serde(default)]
    pub refreshed_at_ms: u64,
}

/// UDP-layer kernel counters parsed from `/proc/net/snmp` (spec §11).
///
/// All fields are best-effort `Option<u64>`. A field that the kernel's
/// `Udp:` row does not include stays `None` — the bundle reader can
/// then distinguish "kernel didn't expose this counter" from "kernel
/// reported zero." Deltas across consecutive snapshots tell the
/// investigator whether packet loss was happening at the UDP layer
/// (send/receive errors rising) or above it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier3UdpKernelStats {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_datagrams: Option<u64>,
    /// Datagrams that arrived with no listening socket. Rising values
    /// here on the receiver mean the path got through but nothing was
    /// bound to consume it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_ports: Option<u64>,
    /// Packets discarded because of a checksum or framing error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_errors: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub out_datagrams: Option<u64>,
    /// Receiver-side socket buffer overflows — the kernel had no room
    /// to queue the packet for the application.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rcvbuf_errors: Option<u64>,
    /// Sender-side socket buffer overflows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sndbuf_errors: Option<u64>,
}

/// A single network interface as seen by the host scrape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tier3Interface {
    pub name: String,
    /// IP addresses bound to the interface (v4 and v6 mixed). The
    /// post-processor can demux by parsing each string.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addresses: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtu: Option<u32>,
    pub up: bool,
    /// Per-interface kernel counters from `/proc/net/dev` (spec §11).
    /// `None` when the row was unreadable or unavailable; never
    /// silently zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counters: Option<Tier3InterfaceCounters>,
}

/// Per-interface byte/packet/drop/error counters from `/proc/net/dev`.
///
/// Same best-effort honesty as [`Tier3UdpKernelStats`]: every counter
/// is `u64` and the whole block is wrapped in `Option` upstream.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier3InterfaceCounters {
    pub rx_bytes: u64,
    pub rx_packets: u64,
    pub rx_errors: u64,
    pub rx_dropped: u64,
    pub tx_bytes: u64,
    pub tx_packets: u64,
    pub tx_errors: u64,
    pub tx_dropped: u64,
}

/// A default-route entry from `/proc/net/route` or `/proc/net/ipv6_route`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tier3Route {
    /// `"v4"` or `"v6"` — string to keep the schema forgiving.
    pub family: String,
    /// Destination prefix (`"0.0.0.0/0"`, `"::/0"`, or a more specific
    /// prefix where the kernel exposes one).
    pub destination: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway: Option<String>,
    pub interface: String,
}

/// A single row from `/proc/net/udp` (or `udp6`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tier3UdpSocket {
    pub local_addr: String,
    pub remote_addr: String,
    /// Hex socket state string (kernel field as-is — `"07"` for
    /// `TCP_CLOSE`, etc.). Kept as a string so the wire format does not
    /// pretend to interpret kernel internals.
    pub state: String,
    pub inode: u64,
}

/// A single DNS resolution result (`DIAGNOSTICS_PLAN.md` T3.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tier3DnsResolution {
    pub hostname: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub a_records: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aaaa_records: Vec<String>,
    /// Not exposed by `std::net::ToSocketAddrs`. Reserved for a future
    /// implementation that uses a real DNS lib; always `None` today.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u32>,
    /// First nameserver from `/etc/resolv.conf`, if available, so the
    /// bundle reader can correlate "different nameservers, different
    /// answers" across nodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolver_used: Option<String>,
    /// Wall-clock millis at the moment of this resolution.
    pub resolved_at_ms: u64,
    /// Last error from the resolver, if the most recent attempt
    /// failed. `None` on success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Host-side analogue of [`IrohIntrospector`] / [`SwimIntrospector`].
/// Installed on the aggregator via
/// [`crate::diagnostics::Aggregator::set_host_introspector`]. Production
/// wires up `crate::diagnostics::host_introspect::HostIntrospect`.
pub trait HostIntrospector: Send + Sync {
    fn capture(&self) -> Tier3HostState;
}

/// Tier-3 outbound reachability probes (`DIAGNOSTICS_PLAN.md` T3.3).
///
/// One entry per registered target. The post-processor uses these to
/// distinguish "iroh can't reach the relay" from "this host can't reach
/// the relay at all" — if the raw UDP probe round-trips but iroh
/// reports `Dead`, the bug is iroh-side; if both fail, the bug is
/// environment-side.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier3ProbeState {
    /// Per-target probe state. Sorted by `(kind, target)` so two
    /// snapshots from the same node line up under `diff`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub probes: Vec<Tier3Probe>,
    /// Wall-clock millis at the moment this capture was produced.
    #[serde(default)]
    pub scraped_at_ms: u64,
}

/// A single probe target's most recent attempt + summary counters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tier3Probe {
    /// Free-form target label (e.g. `"collector-udp-echo"` or the
    /// relay hostname). Stable across attempts.
    pub target: String,
    /// What was probed. One of `"udp_echo"`, `"udp_relay"`, `"stun"`.
    /// Strings so adding a new probe type doesn't break the schema.
    pub kind: String,
    /// Resolved socket address used for the most recent attempt, or
    /// `None` if the target string was never resolved (e.g. DNS error).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_addr: Option<String>,
    /// Wall-clock millis when the last attempt started. `0` until the
    /// first refresh.
    #[serde(default)]
    pub last_attempted_at_ms: u64,
    /// Outcome of the last attempt. One of `"ok"`, `"timeout"`,
    /// `"refused"`, `"error"`, or `"unresolved"`. Strings so new
    /// outcomes don't churn the schema.
    pub last_outcome: String,
    /// RTT of the last successful attempt, in ms. `None` if the last
    /// attempt did not return.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_rtt_ms: Option<u64>,
    /// Last error string, if the most recent attempt failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Total number of attempts (success or failure) over this
    /// introspector's lifetime.
    pub attempts: u64,
    /// Total number of attempts that round-tripped successfully.
    pub successes: u64,
}

/// Probe-side analogue of [`HostIntrospector`]. Installed on the
/// aggregator via [`crate::diagnostics::Aggregator::set_probe_introspector`].
/// Production wires up `crate::diagnostics::probes::ProbeScheduler`.
pub trait ProbeIntrospector: Send + Sync {
    fn capture(&self) -> Tier3ProbeState;
}

/// Vast.ai-side context captured once at boot (`DIAGNOSTICS_PLAN.md` T3.4).
///
/// Best-effort, env-var-only — no calls to the vast.ai API. Empty on
/// hosts that aren't vast.ai workers, which is the common case for
/// the orchestrator.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier3VastaiContext {
    /// `$CONTAINER_ID` if set; `None` otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>,
    /// `$HOSTNAME` if set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    /// All env vars matching the `VAST_*`, `VASTAI_*`, `CONTAINER_*`,
    /// `CUDA_*`, `NVIDIA_*` prefixes at boot time, sorted by key. Keys
    /// known to carry secrets (`*_TOKEN`, `*_KEY`, `*_PASSWORD`) are
    /// dropped before they reach the wire — the captured set is
    /// metadata, not credentials.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env_vars: Vec<(String, String)>,
    /// Wall-clock millis at the moment this process started capturing.
    /// Useful for diffing against the container start time on hosts
    /// where vast.ai exposes one.
    #[serde(default)]
    pub process_start_ms: u64,
    /// Wall-clock millis at the moment of the original capture. Equals
    /// `process_start_ms` today; future captures may refresh subsets
    /// of the snapshot.
    #[serde(default)]
    pub captured_at_ms: u64,
}

/// Vastai-context analogue of [`HostIntrospector`]. Installed on the
/// aggregator via [`crate::diagnostics::Aggregator::set_vastai_introspector`].
/// Production wires up `crate::diagnostics::vastai_context::VastaiContext`.
pub trait VastaiIntrospector: Send + Sync {
    fn capture(&self) -> Tier3VastaiContext;
}

/// Tier-3 process resource snapshot (`DIAGNOSTICS_PLAN.md` T3.5).
///
/// Cheap to collect, occasionally decisive — a tokio worker stalled on
/// a sync call delays SWIM probes enough to look like network failure.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier3ProcessStats {
    /// Resident set size in bytes, parsed from `/proc/self/status`
    /// (`VmRSS`). `None` on non-Linux or when the file is unreadable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rss_bytes: Option<u64>,
    /// Virtual memory size in bytes (`VmSize`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vm_size_bytes: Option<u64>,
    /// Open file descriptor count, derived from `/proc/self/fd`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_fd_count: Option<u64>,
    /// CPU time in milliseconds since process start (utime + stime),
    /// parsed from `/proc/self/stat`. `None` on non-Linux.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_ms: Option<u64>,
    /// Best-effort tokio runtime stats. `None` when capture is called
    /// from outside a tokio context, when the runtime is single-threaded,
    /// or when no stable metrics are exposed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokio: Option<Tier3TokioStats>,
    /// Wall-clock millis at the moment of this scrape.
    #[serde(default)]
    pub captured_at_ms: u64,
}

/// What we can capture from `tokio::runtime::Handle` without enabling
/// the `tokio_unstable` cfg. Stable surface today is the runtime
/// flavor; everything else (worker count, alive task count, blocking
/// pool size) requires `tokio_unstable` and so stays absent.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier3TokioStats {
    /// One of `"current_thread"` or `"multi_thread"`.
    pub flavor: String,
}

/// Process-stats analogue of [`HostIntrospector`]. Installed on the
/// aggregator via [`crate::diagnostics::Aggregator::set_process_introspector`].
/// Production wires up `crate::diagnostics::process_stats::ProcessStats`.
pub trait ProcessIntrospector: Send + Sync {
    fn capture(&self) -> Tier3ProcessStats;
}

/// Relay-side observability totals (spec §1).
///
/// Populated only by relay binaries (role `"relay"`). The bundle
/// reader sees one such block per snapshot from each relay that opted
/// into observability. End-of-run totals answer "how busy was the
/// relay, what closed the most sessions, and how many bytes
/// transited?" without needing an external metrics store.
///
/// Per-session detail lives on the event stream as
/// [`crate::diagnostics::Event::RelaySessionOpened`] /
/// [`crate::diagnostics::Event::RelaySessionClosed`] — the snapshot
/// is the current-value view; events are the lifecycle view.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier3RelayServer {
    /// Sessions the relay considers open right now.
    pub active_sessions: u64,
    /// Total sessions opened over this relay's lifetime in the run.
    pub total_opens: u64,
    /// Total sessions closed over this relay's lifetime in the run.
    pub total_closes: u64,
    /// Bytes received from clients across all sessions, summed.
    pub bytes_rx_total: u64,
    /// Bytes sent to clients across all sessions, summed.
    pub bytes_tx_total: u64,
    /// Count of closes broken down by `close_reason`. Sorted by reason
    /// for stable rendering. An empty vec means no closes observed (or
    /// the relay couldn't classify them).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub closes_by_reason: Vec<(String, u64)>,
    /// Wall-clock millis at the moment of this scrape.
    #[serde(default)]
    pub scraped_at_ms: u64,
}

/// Relay-server analogue of [`HostIntrospector`] / [`ProcessIntrospector`].
/// Installed on a relay binary's aggregator via
/// [`crate::diagnostics::Aggregator::set_relay_server_introspector`].
/// Production wires up `crate::diagnostics::relay_observability::RelayObservability`.
pub trait RelayServerIntrospector: Send + Sync {
    fn capture(&self) -> Tier3RelayServer;
}

/// Tier-3 subprocess snapshot block (spec §4).
///
/// One [`Tier3Subprocess`] entry per subprocess the owning actor
/// registered with the [`SubprocessIntrospector`] — generic over the
/// use case: the introspector only knows about a label, a PID, and a
/// parent PID. Deciding which subprocesses to track is the *calling
/// crate's* responsibility, not the introspector's.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier3SubprocessState {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subprocesses: Vec<Tier3Subprocess>,
    /// Wall-clock millis at the moment of this scrape.
    #[serde(default)]
    pub scraped_at_ms: u64,
}

/// Per-subprocess entry (spec §4 behavior contract).
///
/// All resource fields are `Option<u64>` so the bundle reader can
/// always tell "we couldn't read /proc" from "the process is using
/// zero bytes." The status discriminator is a string for forward
/// compatibility — adding a new state (e.g. `"zombie"`) does not
/// break the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tier3Subprocess {
    /// Caller-supplied label. The introspector never invents one —
    /// the calling crate decides whether this is `"pp-worker"`,
    /// `"helper-script"`, etc.
    pub label: String,
    pub pid: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_pid: Option<u32>,
    /// `"running"`, `"exited"`, or `"unknown"`. Strings so the wire
    /// stays forgiving when new states (e.g. `"zombie"`) are added.
    pub status: String,
    /// Wall-clock millis when the subprocess was registered with
    /// the introspector. Distinct from kernel-side start time —
    /// this is the actor's view of "we asked it to run."
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spawn_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_at_ms: Option<u64>,
    /// Process exit code, if the subprocess exited normally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Terminating signal number, if the subprocess was killed by
    /// a signal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_signal: Option<i32>,
    /// Resident set size in bytes, from `/proc/<pid>/status`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rss_bytes: Option<u64>,
    /// Virtual memory size in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vm_size_bytes: Option<u64>,
    /// Count of entries under `/proc/<pid>/fd`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_fd_count: Option<u64>,
    /// CPU time in milliseconds since this subprocess started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_ms: Option<u64>,
    /// Truncated `/proc/<pid>/cmdline` (first 256 bytes), joined by
    /// spaces. `None` when the file is unreadable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cmdline: Option<String>,
}

/// Subprocess-side analogue of [`HostIntrospector`] / [`ProcessIntrospector`].
/// Installed on the aggregator via
/// [`crate::diagnostics::Aggregator::set_subprocess_introspector`].
///
/// Generic over the use case (spec §4 explicit requirement): the
/// trait surface is one method that returns a [`Tier3SubprocessState`].
/// Tests can install any implementation that fits their assertion;
/// production wires up
/// `crate::diagnostics::subprocess_introspect::SubprocessIntrospect`.
pub trait SubprocessIntrospector: Send + Sync {
    fn capture(&self) -> Tier3SubprocessState;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::event::PeerState;
    use crate::diagnostics::identity::Role;
    use crate::diagnostics::reachability::PeerReachability;
    use crate::types::NodeId;

    #[test]
    fn snapshot_roundtrips_through_json() {
        let id = Identity::new(NodeId([1u8; 32]), Role::stage(), "run-z").with_stage(0, 2);
        let mut peer = PeerReachability::new("ff".repeat(32));
        peer.current_swim_opinion = PeerState::Alive;
        let snap = Snapshot {
            identity: id,
            run_id: "run-z".into(),
            snapshot_id: "snap-1".into(),
            wall_ms: 1_700_000_000_000,
            monotonic_seq: 5,
            trigger: SnapshotTrigger::Periodic,
            body: SnapshotBody {
                reachability: vec![peer],
                events: vec![],
                iroh: None,
                swim: None,
                host: None,
                probes: None,
                vastai: None,
                process: None,
                registry: None,
                relay_server: None,
                subprocess: None,
            },
        };
        let s = serde_json::to_string(&snap).unwrap();
        let back: Snapshot = serde_json::from_str(&s).unwrap();
        assert_eq!(back.snapshot_id, "snap-1");
        assert_eq!(back.body.reachability.len(), 1);
        assert!(matches!(back.trigger, SnapshotTrigger::Periodic));
    }
}
