//! Shared support for the datastream tests (testing spec §2, §3).
//!
//! Two things live here, both deliberately separate from the system under
//! test so a test never asserts the code against itself:
//!
//! * the **payload library** — realistic record shapes and log lines, the
//!   bytes the pipe will actually carry (testing spec §5: "never
//!   placeholder text"); and
//! * the **reference model** — the spec's rules restated as small, total
//!   functions over sequences (testing spec §3). It is the trusted oracle:
//!   every test's `expected` is derived from it, never captured from a run.
//!   It is written naively on purpose (collect, sort, scan) so a reader can
//!   confirm it against the spec by eye, while the pipe computes the same
//!   answers the long way.
//!
//! This module is `#[path]`-included into more than one test binary, so
//! some items are unused in some of them.
#![allow(dead_code)]

use datastream::mux::Mux;
use datastream::store::GapSpan;
pub use datastream::transport::Delivery;
use datastream::{ChannelId, Frame, Position, Record, StreamId};

pub mod schema {
    use datastream::{ChannelId, Record};
    use serde::{Deserialize, Serialize};

    pub const IDENTITY: &str = "identity";
    pub const HOST_RESOURCE: &str = "host.resource";
    pub const TRANSPORT_INTERNALS: &str = "transport.internals";
    pub const MEMBERSHIP: &str = "membership";
    pub const RUNTIME_STATS: &str = "runtime.stats";
    pub const DIST_STATE: &str = "dist.state";
    pub const RUNTIME_ACTORS: &str = "runtime.actors";
    pub const RUNTIME_WORKERS: &str = "runtime.workers";
    pub const DATASTREAM_HEALTH: &str = "datastream.health";

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ProcStream {
        Stdout,
        Stderr,
    }

    impl ProcStream {
        pub fn as_str(self) -> &'static str {
            match self {
                ProcStream::Stdout => "stdout",
                ProcStream::Stderr => "stderr",
            }
        }
    }

    pub fn process_output(label: &str, stream: ProcStream) -> ChannelId {
        ChannelId::new(format!("proc.{label}.{}", stream.as_str()))
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct IdentityRecord {
        pub node: String,
        #[serde(default)]
        pub life: u64,
        #[serde(default)]
        pub node_name: String,
        #[serde(default)]
        pub listen_addr: String,
        #[serde(default)]
        pub relay_url: String,
        #[serde(default)]
        pub version: String,
    }

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

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct MembershipTransition {
        pub peer: String,
        pub from: String,
        pub to: String,
        #[serde(default)]
        pub reason: String,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct RuntimeStats {
        #[serde(default)]
        pub actors_live: u32,
        #[serde(default)]
        pub mailbox_depth: u32,
        #[serde(default)]
        pub scheduled_tasks: u32,
    }

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
        #[serde(default)]
        pub peer_auth_mode: String,
        #[serde(default)]
        pub authorized_peer_count: u32,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct CacheEntryRec {
        #[serde(default)]
        pub actor_addr: String,
        #[serde(default)]
        pub node_id: String,
    }

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

    #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WorkerCounters {
        #[serde(default)]
        pub num_workers: u32,
        #[serde(default)]
        pub scheduled_tasks: u32,
        #[serde(default)]
        pub local_sends: u64,
        #[serde(default)]
        pub cross_sends: u64,
        #[serde(default)]
        pub inbox_sends: u64,
        #[serde(default)]
        pub type_mismatches: u64,
        #[serde(default)]
        pub panics: u64,
        #[serde(default)]
        pub messages_dropped: u64,
        #[serde(default)]
        pub restarts: u64,
        #[serde(default)]
        pub stops: u64,
        #[serde(default)]
        pub messages_processed: u64,
        #[serde(default)]
        pub tick_p50_us: u64,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct DatastreamHealth {
        #[serde(default)]
        pub assigned: u64,
        #[serde(default)]
        pub dropped: u64,
        #[serde(default)]
        pub loss_rate_ppm: u32,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ActorRuntimeDetail {
        #[serde(default)]
        pub actors: Vec<ActorRec>,
    }

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
    impl Record for DistributionState {
        const CHANNEL: &'static str = DIST_STATE;
    }
    impl Record for ActorRuntimeDetail {
        const CHANNEL: &'static str = RUNTIME_ACTORS;
    }
    impl Record for WorkerCounters {
        const CHANNEL: &'static str = RUNTIME_WORKERS;
    }
    impl Record for DatastreamHealth {
        const CHANNEL: &'static str = DATASTREAM_HEALTH;
    }
}

use schema::*;

/// An in-process node (testing spec §2 — the faked machine boundary): a
/// *real* mux plus the producers that feed it. Producers push realistic
/// bytes in at the producer seam; [`Node::sent`] takes the mux's ordered
/// output stream — the value on the mux→transport seam. Only the machine
/// boundary is faked; the mux is the production code.
pub struct Node {
    stream: StreamId,
    mux: Mux,
}

impl Node {
    /// Stand up a node for a given stream (node identity + lifetime).
    pub fn new(stream: StreamId) -> Self {
        Node {
            mux: Mux::unbounded(stream.clone()),
            stream,
        }
    }

    /// The stream this node produces.
    pub fn stream_id(&self) -> &StreamId {
        &self.stream
    }

    /// A producer emits a typed record on its own channel (spec §6.1).
    pub fn emit<R: Record>(&self, record: &R) -> Position {
        self.mux.submit(R::channel(), record.encode())
    }

    /// A producer emits a line of raw process output (spec §6.2).
    pub fn emit_text(&self, label: &str, stream: ProcStream, line: &str) -> Position {
        self.mux
            .submit(process_output(label, stream), line.as_bytes().to_vec())
    }

    /// A producer emits bytes on a channel the consumer may not know
    /// (spec §6.3) — opaque to everything until a view learns the channel.
    pub fn emit_opaque(&self, channel: &str, bytes: &[u8]) -> Position {
        self.mux.submit(ChannelId::new(channel), bytes.to_vec())
    }

    /// Take the node's ordered output stream (drains the mux).
    pub fn sent(&self) -> Vec<Frame> {
        self.mux.drain()
    }
}

/// Realistic payloads, drawn on by both the verified vectors (testing spec
/// §5) and the deployment scenario (testing spec §8). Nothing here is
/// placeholder text.
pub mod payloads {
    use super::*;

    /// A boot/identity record for a node, including the descriptive fields a
    /// node fills once known (name, listen addr, embedded relay, build version).
    pub fn identity(node: &str, life: u64) -> IdentityRecord {
        IdentityRecord {
            node: node.to_string(),
            life,
            node_name: format!("swift-{node}"),
            listen_addr: format!("{node}.iroh:4242"),
            relay_url: "https://relay.example:4443/".to_string(),
            version: "ds-inference @ abc1234".to_string(),
        }
    }

    /// A consolidated distribution-subsystem state; `tick` nudges the values so
    /// a series is not constant.
    pub fn dist_state(tick: u64) -> DistributionState {
        DistributionState {
            cache_size: 3 + (tick % 4) as u32,
            cache_entries: vec![CacheEntryRec {
                actor_addr: format!("actor-{}", tick % 5),
                node_id: format!("node-{}", tick % 3),
            }],
            directory_route_count: 5 + (tick % 7) as u32,
            registry_size: 8 + (tick % 3) as u32,
            registry_tombstones: (tick % 2) as u32,
            registry_entries: vec![RegistryEntryRec {
                name: format!("svc-{}", tick % 4),
                actor_addr: format!("actor-{}", tick % 5),
                node_id: format!("node-{}", tick % 3),
                tombstone: tick.is_multiple_of(2),
            }],
            recent_probe_targets: vec![format!("peer-{}", tick % 6)],
            peer_auth_mode: if tick.is_multiple_of(2) {
                "open".into()
            } else {
                "allow-list".into()
            },
            authorized_peer_count: (tick % 5) as u32,
        }
    }

    /// A per-actor runtime-detail record (the real actor table).
    pub fn actor_detail(tick: u64) -> ActorRuntimeDetail {
        ActorRuntimeDetail {
            actors: vec![
                ActorRec {
                    address: format!("{:064x}", tick),
                    name: "SwimActor".to_string(),
                    mailbox_depth: (tick % 5) as u32,
                    messages_processed: 100 + tick,
                    last_msg_type: "swactor_dist::Ping".to_string(),
                    poisoned: false,
                    message_type_counts: vec![("swactor_dist::Ping".to_string(), 40 + tick)],
                },
                ActorRec {
                    address: format!("{:064x}", tick + 1),
                    name: "RegistryActor".to_string(),
                    mailbox_depth: 0,
                    messages_processed: 10 + tick % 3,
                    last_msg_type: "Tick".to_string(),
                    poisoned: false,
                    message_type_counts: vec![("Tick".to_string(), 10 + tick % 3)],
                },
            ],
        }
    }

    /// A plausible resource sample; `tick` nudges the values so a series is
    /// not constant.
    pub fn resource(tick: u64) -> ResourceSample {
        ResourceSample {
            cpu_pct: 12.5 + (tick % 7) as f32 * 3.0,
            mem_used_mb: 2048 + (tick % 5) as u32 * 128,
            mem_total_mb: 16384,
            gpu_pct: (tick % 4) as f32 * 25.0,
            disk_used_gb: 40 + (tick % 3) as u32,
            net_rx_kbps: 900 + (tick % 11) as u32 * 30,
            net_tx_kbps: 300 + (tick % 13) as u32 * 20,
        }
    }

    /// A transport-internals snapshot.
    pub fn transport(tick: u64) -> TransportInternals {
        TransportInternals {
            relay_connected: !tick.is_multiple_of(9),
            direct_peers: 2 + (tick % 3) as u32,
            relay_peers: 1,
            rtt_ms_p50: 18 + (tick % 5) as u32 * 4,
        }
    }

    /// A membership transition between two peers' states.
    pub fn membership(peer: &str, from: &str, to: &str) -> MembershipTransition {
        MembershipTransition {
            peer: peer.to_string(),
            from: from.to_string(),
            to: to.to_string(),
            reason: "probe timeout".to_string(),
        }
    }

    /// A runtime-stats record.
    pub fn runtime(tick: u64) -> RuntimeStats {
        RuntimeStats {
            actors_live: 30 + (tick % 6) as u32,
            mailbox_depth: (tick % 17) as u32,
            scheduled_tasks: 4 + (tick % 3) as u32,
        }
    }

    /// Aggregated worker-runtime counters (the `runtime.workers` channel).
    pub fn worker_counters(tick: u64) -> WorkerCounters {
        WorkerCounters {
            num_workers: 4,
            scheduled_tasks: 4 + (tick % 3) as u32,
            local_sends: 100 + tick,
            cross_sends: 20 + tick,
            inbox_sends: tick,
            messages_processed: 1000 + tick * 7,
            tick_p50_us: 50 + tick,
            ..Default::default()
        }
    }

    /// Datastream self-health (the `datastream.health` channel). `assigned`
    /// tracks the seed directly so a scenario can pick a frame out by its value.
    pub fn datastream_health(tick: u64) -> DatastreamHealth {
        DatastreamHealth {
            assigned: tick,
            dropped: tick % 4,
            loss_rate_ppm: (tick % 4) as u32,
        }
    }

    /// A realistic line of process output (without trailing newline).
    pub fn log_line(label: &str, tick: u64) -> String {
        format!(
            "[{label}] step {tick} loss=0.{:03} lr=3e-4",
            250 - (tick % 200)
        )
    }
}

/// The reference model: the spec's rules as plain total functions over
/// sequences. No transport, no storage, no concurrency, no time.
pub mod reference {
    use super::*;

    /// The frames a consumer was delivered for one stream, in arrival
    /// order — the raw material reconstruction works over.
    pub fn delivered_frames(stream: &StreamId, deliveries: &[Delivery]) -> Vec<Frame> {
        deliveries
            .iter()
            .filter(|d| &d.stream == stream)
            .map(|d| d.frame.clone())
            .collect()
    }

    /// Reconstruction (spec §8.1, testing spec §3): "keep the frames that
    /// were delivered, in position order." Duplicates of a position
    /// collapse to one (the carrier may not fabricate content, spec §9, so
    /// a repeat carries identical bytes).
    pub fn reconstruct(delivered: &[Frame]) -> Vec<Frame> {
        let mut frames: Vec<Frame> = Vec::new();
        for f in delivered {
            if !frames.iter().any(|seen| seen.position == f.position) {
                frames.push(f.clone());
            }
        }
        frames.sort_by_key(|f| f.position);
        frames
    }

    /// The surfaced gaps as spans: the **interior** runs of positions missing
    /// between the first and last delivered position (spec §7.5, §8). A
    /// consumer can only detect gaps it has bracketing frames for; positions
    /// lost after the last delivered frame are invisible and manifest as the
    /// stream ending (spec §7.4 node death = truncation, not a gap).
    ///
    /// Walks the sorted delivered positions — O(frames), never the gap size —
    /// so the oracle agrees with the store on the cheap path even across a
    /// near-`u64::MAX` gap.
    pub fn gap_spans(delivered: &[Frame]) -> Vec<GapSpan> {
        let recon = reconstruct(delivered);
        let mut spans = Vec::new();
        let mut prev: Option<u64> = None;
        for f in &recon {
            let p = f.position.0;
            if let Some(q) = prev
                && p > q + 1
            {
                spans.push(GapSpan {
                    start: q + 1,
                    end: p - 1,
                });
            }
            prev = Some(p);
        }
        spans
    }

    /// One structural item on the merged timeline (testing spec §3: "all
    /// stored frames in position order, channels interleaved"). This is the
    /// *structure* of the merged log — order and surfaced gaps — decoupled
    /// from how each payload is rendered for display, which is a separate
    /// §9.3 concern the tests assert on its own.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum TimelineItem {
        Frame { position: u64, channel: String },
        Gap { start: u64, end: u64 },
    }

    /// The merged log oracle (spec §9.2): every delivered frame in position
    /// order, channels interleaved, with an interior gap surfaced wherever a
    /// position is missing. Written naively so it is obviously the spec.
    pub fn merged_log(delivered: &[Frame]) -> Vec<TimelineItem> {
        let frames = reconstruct(delivered);
        let mut out = Vec::new();
        let mut prev: Option<u64> = None;
        for f in &frames {
            let pos = f.position.0;
            if let Some(p) = prev
                && pos > p + 1
            {
                out.push(TimelineItem::Gap {
                    start: p + 1,
                    end: pos - 1,
                });
            }
            out.push(TimelineItem::Frame {
                position: pos,
                channel: f.channel.to_string(),
            });
            prev = Some(pos);
        }
        out
    }
}

/// Convenience: build a frame on a typed channel from a record.
pub fn typed_frame<R: Record>(record: &R, position: u64) -> Frame {
    Frame::new(R::channel(), Position(position), record.encode())
}

/// Convenience: build a frame on a raw-text process-output channel.
pub fn text_frame(label: &str, stream: ProcStream, line: &str, position: u64) -> Frame {
    Frame::new(
        process_output(label, stream),
        Position(position),
        line.as_bytes().to_vec(),
    )
}

/// Convenience: a frame on a channel id the consumer does not know — an
/// opaque channel (spec §6.3).
pub fn opaque_frame(id: &str, payload: &[u8], position: u64) -> Frame {
    Frame::new(ChannelId::new(id), Position(position), payload.to_vec())
}
