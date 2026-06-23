//! Generic datastream emission helpers.
//!
//! This module owns only integration mechanics: a per-node mux, process-output
//! observer plumbing, generic record/text/byte submission, and sinks that ship
//! ordered frames. The records and channel names belong to the crates that own
//! those domains.

use std::sync::Arc;
use std::sync::OnceLock;

use swactor::actor::ActorAddress;
use swactor::process_observer::ProcessOutputObserver;
use swactor::runtime::Runtime;

use super::frame::{ChannelId, Frame, Lifetime, NodeId, Position, StreamId};
use super::mux::Mux;
use super::record::Record;
use super::wire::{DatastreamFrame, encode_delivery};

/// Where assembled frames go once the mux has ordered them. A sink is the only
/// place transport lives; the emitter knows nothing about it.
pub trait FrameSink: Send {
    /// Ship one ordered frame for `stream`. Best-effort: a sink may drop.
    fn ship(&mut self, stream: &StreamId, frame: &Frame);
}

/// Static identity a node needs to build its mux.
pub struct EmitterConfig {
    pub node_hex: String,
    pub life: u64,
    pub mux_capacity: usize,
}

/// Forwards managed-process output into a node's mux using a caller-owned
/// channel mapping.
struct MuxProcObserver {
    mux: Arc<Mux>,
    channel_for: Arc<dyn Fn(&str, bool) -> ChannelId + Send + Sync>,
}

impl ProcessOutputObserver for MuxProcObserver {
    fn on_output(&self, label: &str, is_stderr: bool, data: &[u8]) {
        self.mux
            .submit((self.channel_for)(label, is_stderr), data.to_vec());
    }
}

/// A per-node emitter. Owns ordering (its [`Mux`]) and ships through its
/// [`FrameSink`]. It does not know any domain-specific record type.
pub struct DatastreamEmitter {
    stream_id: StreamId,
    mux: Arc<Mux>,
    sink: Box<dyn FrameSink>,
}

impl DatastreamEmitter {
    /// Build a node's emitter: a mux keyed by its stream id.
    pub fn new(cfg: EmitterConfig, sink: Box<dyn FrameSink>) -> Self {
        let stream_id = StreamId::new(NodeId::new(&cfg.node_hex), Lifetime(cfg.life));
        let mux = Arc::new(Mux::new(stream_id.clone(), cfg.mux_capacity));
        Self {
            stream_id,
            mux,
            sink,
        }
    }

    /// The stream this emitter produces.
    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    /// The node's mux, for producers that submit directly.
    pub fn mux(&self) -> &Arc<Mux> {
        &self.mux
    }

    /// Number of positions assigned by the mux.
    pub fn assigned(&self) -> u64 {
        self.mux.assigned()
    }

    /// Number of frames dropped by the mux on overflow.
    pub fn dropped(&self) -> u64 {
        self.mux.dropped()
    }

    /// Submit a typed record defined by the caller's crate.
    pub fn submit_record<R: Record>(&self, record: &R) -> Position {
        self.mux.submit(R::channel(), record.encode())
    }

    /// Submit UTF-8/text bytes on a caller-owned channel.
    pub fn submit_text(&self, channel: impl Into<ChannelId>, text: impl AsRef<[u8]>) -> Position {
        self.mux.submit(channel, text.as_ref().to_vec())
    }

    /// Submit arbitrary bytes on a caller-owned channel.
    pub fn submit_bytes(&self, channel: impl Into<ChannelId>, bytes: Vec<u8>) -> Position {
        self.mux.submit(channel, bytes)
    }

    /// An observer that taps managed-process output onto this node's stream.
    /// The caller supplies the channel naming convention.
    pub fn process_observer_with<F>(&self, channel_for: F) -> Arc<dyn ProcessOutputObserver>
    where
        F: Fn(&str, bool) -> ChannelId + Send + Sync + 'static,
    {
        Arc::new(MuxProcObserver {
            mux: self.mux.clone(),
            channel_for: Arc::new(channel_for),
        })
    }

    /// Drain the mux and ship every ordered frame.
    pub fn tick(&mut self) {
        for frame in self.mux.drain() {
            self.sink.ship(&self.stream_id, &frame);
        }
    }

    /// A cheap, cloneable handle for submitting event-driven frames onto this
    /// node's stream from any thread.
    pub fn event_sink(&self) -> DatastreamEventSink {
        DatastreamEventSink {
            mux: self.mux.clone(),
        }
    }
}

/// A thread-safe submit handle. Holds a clone of the node's mux; submitted
/// frames drain in the node's main loop.
#[derive(Clone)]
pub struct DatastreamEventSink {
    mux: Arc<Mux>,
}

impl DatastreamEventSink {
    pub fn submit_record<R: Record>(&self, record: &R) -> Position {
        self.mux.submit(R::channel(), record.encode())
    }

    pub fn submit_text(&self, channel: impl Into<ChannelId>, text: impl AsRef<[u8]>) -> Position {
        self.mux.submit(channel, text.as_ref().to_vec())
    }

    pub fn submit_bytes(&self, channel: impl Into<ChannelId>, bytes: Vec<u8>) -> Position {
        self.mux.submit(channel, bytes)
    }
}

/// A sink that drops everything. Used when a node has no collector yet, so the
/// mux can still drain and stay bounded.
pub struct NoopSink;

impl FrameSink for NoopSink {
    fn ship(&mut self, _stream: &StreamId, _frame: &Frame) {}
}

/// Ships frames over the swactor cluster to the orchestrator's `datastream-sink`
/// actor, reusing the exact `register_name`/`resolve_name` + transport-router
/// path the application already uses.
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
