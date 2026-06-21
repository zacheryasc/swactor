//! Fleet telemetry over the swactor cluster transport.
//!
//! Every node in the demo — the orchestrator and each stage `pp-worker` —
//! frames its *own* `identity` + `host.resource` records (plus
//! runtime/transport/membership) onto its per-node datastream and ships them as
//! [`DatastreamFrame`](datastream::wire::DatastreamFrame) actor
//! messages over the regular swactor transport to the orchestrator's
//! [`DatastreamSink`](datastream::DatastreamSink) actor (published
//! under [`DATASTREAM_SINK_NAME`](datastream::DATASTREAM_SINK_NAME)).
//! The orchestrator folds them into an in-process `FleetView` and renders the
//! Fleet tab. No dedicated channel: telemetry rides the same iroh/SWIM transport
//! as everything else.

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use datastream::catalog::{
    CacheEntryRec, DistributionState, IdentityRecord, MembershipTransition, RegistryEntryRec,
    RuntimeStats as DsRuntimeStats, WorkerCounters,
};
use datastream::emit::{
    ClusterFrameSink, DatastreamEmitter, EmitterConfig, TickInput,
};
use distribution::registry::RegistrySnapshot;
use distribution::swim::member_list::MemberEntry;
use distribution::swim::telemetry::ObservedTransition;
use distribution::types::{MemberState, NodeId};
use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;

/// How often a node ships a periodic fleet sample. The fleet table animates at
/// roughly this cadence; the membership tracker still diffs every tick so peer
/// transitions surface promptly regardless.
pub const FLEET_TICK_INTERVAL: Duration = Duration::from_secs(3);

/// A node's fleet emitter: a per-node [`DatastreamEmitter`] whose sink is a
/// [`ClusterFrameSink`] shipping to the orchestrator's `datastream-sink` actor.
/// Built once per node, then [`tick`](Self::tick)ed from the node's main loop
/// with its live membership. The sink destination is late-bound through
/// `sink_slot`: the caller fills it once the name resolves; until then frames
/// drop and the bounded mux absorbs the gap.
pub struct FleetEmitter {
    emitter: DatastreamEmitter,
}

impl FleetEmitter {
    /// Build an emitter shipping over the cluster to the `datastream-sink`
    /// resolved into `sink_slot`, seeding the identity frame with `name` (e.g.
    /// `"pp-stage-0"`) and `listen_addr`.
    pub fn new(
        rt: Arc<Runtime>,
        sink_slot: Arc<OnceLock<ActorAddress>>,
        node_hex: &str,
        life: u64,
        name: &str,
        listen_addr: &str,
    ) -> Self {
        let sink = ClusterFrameSink::new(rt, sink_slot);
        let mut emitter = DatastreamEmitter::new(
            EmitterConfig {
                node_hex: node_hex.to_string(),
                life,
                mux_capacity: 1024,
            },
            Box::new(sink),
        );
        // Supersede the minimal boot identity with the node's name + address so
        // the Fleet table labels the row instead of showing a bare hex id.
        emitter.update_identity(&IdentityRecord {
            node: node_hex.to_string(),
            life,
            node_name: name.to_string(),
            listen_addr: listen_addr.to_string(),
            ..Default::default()
        });
        // Membership is driven from the SWIM observer (real transitions, with a
        // cause), so the emitter's reason-less member-list diff is turned off; the
        // caller drains transitions via [`submit_membership`](Self::submit_membership).
        emitter.use_external_membership();
        Self { emitter }
    }

    /// Ship one periodic sample: host resource + runtime + transport (with the
    /// stage's real SWIM `rtt_ms_p50`) + datastream health. Membership, dist.state,
    /// and worker counters are submitted separately before this drains the mux.
    /// Call on the [`FLEET_TICK_INTERVAL`] cadence from the node's main loop.
    pub fn tick(
        &mut self,
        members: &[(String, String)],
        runtime: DsRuntimeStats,
        relay_connected: bool,
        relay_peers: u32,
        rtt_ms_p50: u32,
    ) {
        self.emitter.tick(
            TickInput {
                members,
                runtime,
                relay_connected,
                relay_peers,
                rtt_ms_p50,
            },
            true,
        );
    }

    /// Emit one membership transition from the SWIM observer (carries a real
    /// `reason`). Submit before [`tick`](Self::tick) so it drains this cycle.
    pub fn submit_membership(&self, transition: &MembershipTransition) {
        self.emitter.submit_membership(transition);
    }

    /// Emit the consolidated distribution-subsystem state (registry / location
    /// cache / probe targets). Submit before [`tick`](Self::tick).
    pub fn submit_dist_state(&self, state: &DistributionState) {
        self.emitter.submit_dist_state(state);
    }

    /// Emit the aggregated worker-runtime counters. Submit before [`tick`](Self::tick).
    pub fn submit_worker_counters(&self, counters: &WorkerCounters) {
        self.emitter.submit_worker_counters(counters);
    }
}

/// Hex-encode raw id bytes the way every other id on the stream is encoded.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn member_state_str(state: MemberState) -> &'static str {
    match state {
        MemberState::Alive => "alive",
        MemberState::Suspect => "suspect",
        MemberState::Dead => "dead",
    }
}

/// Convert a SWIM-observer transition into the wire record (with its real cause).
pub fn membership_transition(t: &ObservedTransition) -> MembershipTransition {
    MembershipTransition {
        peer: hex(&t.peer.0),
        from: t.from.map(member_state_str).unwrap_or("unknown").to_string(),
        to: member_state_str(t.to).to_string(),
        reason: t.reason.to_string(),
    }
}

/// Build the consolidated `dist.state` record from a stage's live subsystem
/// mirrors — the same shape the standalone node assembles. The demo runs open
/// peer-auth, so `peer_auth_mode` is `"open"` and `authorized_peer_count` is 0.
pub fn build_dist_state(
    registry: &RegistrySnapshot,
    cache: &[(ActorAddress, NodeId)],
    recent_targets: &[NodeId],
    directory_route_count: u32,
) -> DistributionState {
    DistributionState {
        cache_size: cache.len() as u32,
        cache_entries: cache
            .iter()
            .map(|(addr, host)| CacheEntryRec {
                actor_addr: hex(&addr.0),
                node_id: hex(&host.0),
            })
            .collect(),
        directory_route_count,
        registry_size: registry.size as u32,
        registry_tombstones: registry.tombstones as u32,
        registry_entries: registry
            .entries
            .iter()
            .map(|e| RegistryEntryRec {
                name: e.name.clone(),
                actor_addr: hex(&e.actor_addr.0),
                node_id: hex(&e.node_id.0),
                tombstone: e.tombstone,
            })
            .collect(),
        recent_probe_targets: recent_targets.iter().map(|t| hex(&t.0)).collect(),
        peer_auth_mode: "open".to_string(),
        authorized_peer_count: 0,
    }
}

/// Aggregate the runtime's per-worker counters + tick timing into the
/// `runtime.workers` record (the deep slice behind the thin `runtime.stats`).
pub fn worker_counters(rt: &Runtime) -> WorkerCounters {
    let rs = rt.stats();
    let mut wc = WorkerCounters {
        num_workers: rs.workers.len() as u32,
        ..Default::default()
    };
    for w in &rs.workers {
        wc.scheduled_tasks += w.num_actors as u32;
        wc.local_sends += w.local_sends;
        wc.cross_sends += w.cross_sends;
        wc.inbox_sends += w.inbox_sends;
        wc.type_mismatches += w.type_mismatches;
        wc.panics += w.panics;
        wc.messages_dropped += w.messages_dropped;
        wc.restarts += w.restarts;
        wc.stops += w.stops;
        wc.messages_processed += w.messages_processed;
    }
    let mut tick_us: Vec<u64> = rs
        .tick_timings
        .iter()
        .flatten()
        .map(|t| t.phase_us.iter().sum())
        .collect();
    tick_us.sort_unstable();
    wc.tick_p50_us = tick_us.get(tick_us.len() / 2).copied().unwrap_or(0);
    wc
}

/// A cheap "has the fleet cadence elapsed" gate, so a 20 ms main loop only
/// builds + ships a sample every [`FLEET_TICK_INTERVAL`].
pub struct FleetTimer {
    last: Instant,
}

impl FleetTimer {
    pub fn new() -> Self {
        // Bias the first sample to fire promptly after boot.
        Self {
            last: Instant::now() - FLEET_TICK_INTERVAL,
        }
    }

    /// `true` (and resets) when at least [`FLEET_TICK_INTERVAL`] has passed.
    pub fn due(&mut self) -> bool {
        if self.last.elapsed() >= FLEET_TICK_INTERVAL {
            self.last = Instant::now();
            true
        } else {
            false
        }
    }
}

impl Default for FleetTimer {
    fn default() -> Self {
        Self::new()
    }
}

/// Project SWIM [`MemberEntry`]s into the `(node_hex, state)` pairs the emitter's
/// membership tracker diffs.
pub fn members_to_pairs(members: &[MemberEntry]) -> Vec<(String, String)> {
    members
        .iter()
        .map(|e| {
            let id: String = e.node_id.0.iter().map(|b| format!("{:02x}", b)).collect();
            let state = match e.state {
                MemberState::Alive => "alive",
                MemberState::Suspect => "suspect",
                MemberState::Dead => "dead",
            }
            .to_string();
            (id, state)
        })
        .collect()
}

/// Snapshot the runtime's live actor metrics into the datastream's
/// [`DsRuntimeStats`] (the same projection the standalone node uses).
pub fn runtime_stats(rt: &Runtime) -> DsRuntimeStats {
    let rs = rt.stats();
    DsRuntimeStats {
        actors_live: rs.actors.len() as u32,
        mailbox_depth: rs.workers.iter().map(|w| w.mailbox_depth as u32).sum(),
        scheduled_tasks: rs.workers.iter().map(|w| w.num_actors as u32).sum(),
    }
}
