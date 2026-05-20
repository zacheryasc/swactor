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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub api_gaps: Vec<String>,
    /// Wall-clock millis at the moment the introspector last
    /// refreshed its cache.
    #[serde(default)]
    pub scraped_at_ms: u64,
}

/// Per-peer iroh-side view (`DIAGNOSTICS_PLAN.md` T2.1). Fields that
/// iroh exposes are populated directly; the rest stay `None` and are
/// listed in [`Tier2IrohState::api_gaps`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tier2Peer {
    pub peer_node_id_hex: String,
    /// Derived from address usage when iroh doesn't expose a direct
    /// `conn_type`. `Direct` if any active IP addr exists, `Relay` if
    /// any active relay addr exists, `Mixed` if both, `None` if iroh
    /// has no active path. `None` is *not* the same as "iroh hasn't
    /// heard of this peer" — that case yields a peer entry whose
    /// vectors are empty and `conn_type` is `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conn_type: Option<ConnType>,
    /// Not exposed by iroh 0.96; reported in `api_gaps`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    /// Not exposed by iroh 0.96; reported in `api_gaps`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_ms: Option<u64>,
    /// Not exposed by iroh 0.96; reported in `api_gaps`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_received_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub direct_addresses: Vec<TransportAddrWire>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relay_urls: Vec<TransportAddrWire>,
    /// Not exposed by iroh 0.96; reported in `api_gaps`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addr_sources: Option<Vec<String>>,
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
    /// Wall-clock millis at the moment of this scrape.
    #[serde(default)]
    pub refreshed_at_ms: u64,
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
            },
        };
        let s = serde_json::to_string(&snap).unwrap();
        let back: Snapshot = serde_json::from_str(&s).unwrap();
        assert_eq!(back.snapshot_id, "snap-1");
        assert_eq!(back.body.reachability.len(), 1);
        assert!(matches!(back.trigger, SnapshotTrigger::Periodic));
    }
}
