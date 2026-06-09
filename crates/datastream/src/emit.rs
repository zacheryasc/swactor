//! The shared per-node telemetry emitter.
//!
//! Both the generic `swactor` node and the pipeline-parallel GPU worker emit
//! the same datastream through this one type. A caller extracts only the few
//! node-specific values each tick (SWIM members, runtime counts, relay flags)
//! and hands them in via [`TickInput`]; the emitter owns the mux, the host/CPU
//! sampler, and the membership differ, and ships every assembled [`Frame`]
//! through a pluggable [`FrameSink`].
//!
//! The emitter is transport-agnostic: it never depends on iroh. A node ships
//! over the swactor cluster ([`ClusterFrameSink`]); a raw demo or test ships
//! over UDP or collects in memory. Swapping the sink does not change a byte of
//! emission logic.

use std::sync::Arc;
use std::sync::OnceLock;

use swactor::actor::ActorAddress;
use swactor::process_observer::ProcessOutputObserver;
use swactor::runtime::Runtime;

use super::catalog::{
    self, ActorRuntimeDetail, DatastoreState, DistributionState, IdentityRecord, ProcStream,
    Record, RuntimeStats, TransportInternals,
};
use super::frame::{Frame, Lifetime, NodeId, StreamId};
use super::mux::Mux;
use super::source::{self, CpuSampler, MembershipTracker};
use super::wire::{encode_delivery, DatastreamFrame};

/// Where assembled frames go once the mux has ordered them. A sink is the only
/// place transport lives; the emitter knows nothing about it.
pub trait FrameSink: Send {
    /// Ship one ordered frame for `stream`. Best-effort: a sink may drop.
    fn ship(&mut self, stream: &StreamId, frame: &Frame);
}

/// Static identity a node needs to build its emitter.
pub struct EmitterConfig {
    pub node_hex: String,
    pub life: u64,
    pub mux_capacity: usize,
}

/// The node-specific values a caller extracts each tick. Everything else the
/// emitter samples itself.
pub struct TickInput<'a> {
    /// `(node_id, swim_state)` for every known peer, keyed by stable id.
    pub members: &'a [(String, String)],
    /// Actor-runtime metrics this tick.
    pub runtime: RuntimeStats,
    pub relay_connected: bool,
    pub relay_peers: u32,
}

/// Forwards managed-process output into a node's mux as `proc.<label>.*` text
/// frames, so every process the node spawns is captured with no per-spawn
/// wiring. Installed on the runtime via `set_process_output_observer`.
struct MuxProcObserver {
    mux: Arc<Mux>,
}

impl ProcessOutputObserver for MuxProcObserver {
    fn on_output(&self, label: &str, is_stderr: bool, data: &[u8]) {
        let stream = if is_stderr {
            ProcStream::Stderr
        } else {
            ProcStream::Stdout
        };
        self.mux
            .submit(catalog::process_output(label, stream), data.to_vec());
    }
}

/// The one per-node emitter. Owns ordering (its [`Mux`]) and the periodic
/// samplers; ships through its [`FrameSink`].
pub struct DatastreamEmitter {
    stream_id: StreamId,
    mux: Arc<Mux>,
    cpu: CpuSampler,
    membership: MembershipTracker,
    sink: Box<dyn FrameSink>,
}

impl DatastreamEmitter {
    /// Build a node's emitter: a mux keyed by its stream id, with the identity
    /// frame emitted first so the consumer can attribute the stream.
    pub fn new(cfg: EmitterConfig, sink: Box<dyn FrameSink>) -> Self {
        let stream_id = StreamId::new(NodeId::new(&cfg.node_hex), Lifetime(cfg.life));
        let mux = Arc::new(Mux::new(stream_id.clone(), cfg.mux_capacity));
        mux.submit(
            catalog::IDENTITY,
            source::identity_record(&cfg.node_hex, cfg.life).encode(),
        );
        Self {
            stream_id,
            mux,
            cpu: CpuSampler::new(),
            membership: MembershipTracker::new(),
            sink,
        }
    }

    /// The node's mux, for producers (e.g. a raw demo) that submit directly.
    pub fn mux(&self) -> &Arc<Mux> {
        &self.mux
    }

    /// An observer that taps managed-process output onto this node's stream.
    /// Register it on the runtime with `Runtime::set_process_output_observer`.
    pub fn process_observer(&self) -> Arc<dyn ProcessOutputObserver> {
        Arc::new(MuxProcObserver {
            mux: self.mux.clone(),
        })
    }

    /// One main-loop iteration: when `sample_periodic`, submit the
    /// host/runtime/transport records; every call diff membership and submit
    /// transitions; then drain the mux and ship every ordered frame.
    pub fn tick(&mut self, input: TickInput, sample_periodic: bool) {
        if sample_periodic {
            self.mux.submit(
                catalog::HOST_RESOURCE,
                source::read_host_resource(&mut self.cpu).encode(),
            );
            self.mux.submit(catalog::RUNTIME_STATS, input.runtime.encode());
            let transport = TransportInternals {
                relay_connected: input.relay_connected,
                direct_peers: input
                    .members
                    .iter()
                    .filter(|(_, s)| s == "alive")
                    .count() as u32,
                relay_peers: input.relay_peers,
                rtt_ms_p50: 0,
            };
            self.mux
                .submit(catalog::TRANSPORT_INTERNALS, transport.encode());
        }

        for transition in self.membership.diff(input.members) {
            self.mux.submit(catalog::MEMBERSHIP, transition.encode());
        }

        for frame in self.mux.drain() {
            self.sink.ship(&self.stream_id, &frame);
        }
    }

    /// Submit the consolidated distribution-subsystem state. Periodic; the node
    /// builds this from its actor mirrors each refresh. Drained by the next
    /// [`tick`](Self::tick).
    pub fn submit_dist_state(&self, state: &DistributionState) {
        self.mux.submit(catalog::DIST_STATE, state.encode());
    }

    /// Submit the consolidated datastore steady metrics. Periodic.
    pub fn submit_datastore_state(&self, state: &DatastoreState) {
        self.mux.submit(catalog::DATASTORE_STATE, state.encode());
    }

    /// Submit the per-actor runtime detail table. Periodic.
    pub fn submit_actor_detail(&self, detail: &ActorRuntimeDetail) {
        self.mux.submit(catalog::RUNTIME_ACTORS, detail.encode());
    }

    /// Re-emit the identity record once late-bound fields (name, listen addr,
    /// relay URL, version) are known. "Latest wins" on the consumer, so this
    /// supersedes the minimal boot identity emitted in [`new`](Self::new).
    pub fn update_identity(&self, identity: &IdentityRecord) {
        self.mux.submit(catalog::IDENTITY, identity.encode());
    }

    /// A cheap, cloneable handle for submitting event-driven frames onto this
    /// node's stream from any thread — the same shared-mux path as the process
    /// observer, for sources (e.g. the datastore) whose events fire on their own
    /// worker threads rather than in the main loop.
    pub fn event_sink(&self) -> DatastreamEventSink {
        DatastreamEventSink {
            mux: self.mux.clone(),
        }
    }
}

/// A thread-safe submit handle (see [`DatastreamEmitter::event_sink`]). Holds a
/// clone of the node's mux; submitted frames drain in the node's main loop.
#[derive(Clone)]
pub struct DatastreamEventSink {
    mux: Arc<Mux>,
}

impl DatastreamEventSink {
    /// Record one datastore operation as a `datastore.events` text line (JSON,
    /// one object per line — the text channel carries structured op records the
    /// consumer tails to rebuild the recent-operations timeline).
    pub fn datastore_event(
        &self,
        timestamp_ms: u64,
        kind: &str,
        hash: &str,
        name: Option<&str>,
        size_bytes: u64,
    ) {
        let line = serde_json::json!({
            "timestamp_ms": timestamp_ms,
            "kind": kind,
            "hash": hash,
            "name": name,
            "size_bytes": size_bytes,
        })
        .to_string();
        self.mux
            .submit(catalog::datastore_event(), line.into_bytes());
    }
}

/// A sink that drops everything. Used by a node before it knows where to ship
/// (e.g. a standalone node with no orchestrator), so the mux still drains and
/// stays bounded.
pub struct NoopSink;

impl FrameSink for NoopSink {
    fn ship(&mut self, _stream: &StreamId, _frame: &Frame) {}
}

/// Ships frames over the swactor cluster to the orchestrator's `datastream-sink`
/// actor, reusing the exact `register_name`/`resolve_name` + transport-router
/// path the application already uses. The destination is late-bound through a
/// shared `OnceLock`: until the sink resolves (the orchestrator may not have
/// joined yet) frames are dropped, and the mux's bounded buffer absorbs the
/// gap — the same tolerance as a lazily re-resolved collector.
pub struct ClusterFrameSink {
    rt: Arc<Runtime>,
    sink: Arc<OnceLock<ActorAddress>>,
}

impl ClusterFrameSink {
    pub fn new(rt: Arc<Runtime>, sink: Arc<OnceLock<ActorAddress>>) -> Self {
        Self { rt, sink }
    }
}

impl FrameSink for ClusterFrameSink {
    fn ship(&mut self, stream: &StreamId, frame: &Frame) {
        if let Some(addr) = self.sink.get() {
            let _ = self.rt.send_to(
                *addr,
                DatastreamFrame {
                    payload: encode_delivery(stream, frame),
                },
            );
        }
    }
}
