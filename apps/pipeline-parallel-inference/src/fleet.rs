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

use datastream::catalog::{IdentityRecord, RuntimeStats as DsRuntimeStats};
use datastream::emit::{
    ClusterFrameSink, DatastreamEmitter, EmitterConfig, TickInput,
};
use distribution::swim::member_list::MemberEntry;
use distribution::types::MemberState;
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
        let emitter = DatastreamEmitter::new(
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
        Self { emitter }
    }

    /// Ship one periodic sample: host resource + runtime + transport, plus any
    /// membership transitions since the last tick. Call on the
    /// [`FLEET_TICK_INTERVAL`] cadence from the node's main loop.
    pub fn tick(
        &mut self,
        members: &[(String, String)],
        runtime: DsRuntimeStats,
        relay_connected: bool,
        relay_peers: u32,
    ) {
        self.emitter.tick(
            TickInput {
                members,
                runtime,
                relay_connected,
                relay_peers,
            },
            true,
        );
    }
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
