//! Generic datastream emission helpers.

use std::sync::Arc;
use std::sync::OnceLock;

use swactor::actor::ActorAddress;
use swactor::process_observer::ProcessOutputObserver;
use swactor::runtime::Runtime;

use super::frame::{ChannelId, Frame, Lifetime, NodeId, Position, StreamId};
use super::mux::Mux;
use super::record::Record;
use super::wire::{DatastreamFrame, encode_delivery};

/// Where assembled frames go once the mux has ordered them.
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

/// Legacy per-node emitter retained while runtime callsites move to
/// [`crate::DatastreamEndpoint`]. New code should register channels on the
/// endpoint and submit through [`crate::DatastreamProducer`].
pub struct DatastreamEmitter {
    stream_id: StreamId,
    mux: Arc<Mux>,
    sink: Box<dyn FrameSink>,
}

impl DatastreamEmitter {
    pub fn new(cfg: EmitterConfig, sink: Box<dyn FrameSink>) -> Self {
        let stream_id = StreamId::new(NodeId::new(&cfg.node_hex), Lifetime(cfg.life));
        let mux = Arc::new(Mux::new(stream_id.clone(), cfg.mux_capacity));
        Self {
            stream_id,
            mux,
            sink,
        }
    }

    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    pub fn mux(&self) -> &Arc<Mux> {
        &self.mux
    }

    pub fn assigned(&self) -> u64 {
        self.mux.assigned()
    }

    pub fn dropped(&self) -> u64 {
        self.mux.dropped()
    }

    pub fn set_frame_timing_enabled(&self, enabled: bool) {
        self.mux.set_frame_timing_enabled(enabled);
    }

    pub fn frame_timing_enabled(&self) -> bool {
        self.mux.frame_timing_enabled()
    }

    pub fn submit_record<R: Record>(&self, channel: ChannelId, record: &R) -> Position {
        self.mux.submit(channel, record.encode())
    }

    pub fn submit_text(&self, channel: ChannelId, text: impl AsRef<[u8]>) -> Position {
        self.mux.submit(channel, text.as_ref().to_vec())
    }

    pub fn submit_bytes(&self, channel: ChannelId, bytes: Vec<u8>) -> Position {
        self.mux.submit(channel, bytes)
    }

    pub fn process_observer_with<F>(&self, channel_for: F) -> Arc<dyn ProcessOutputObserver>
    where
        F: Fn(&str, bool) -> ChannelId + Send + Sync + 'static,
    {
        Arc::new(MuxProcObserver {
            mux: self.mux.clone(),
            channel_for: Arc::new(channel_for),
        })
    }

    pub fn tick(&mut self) {
        for frame in self.mux.drain() {
            self.sink.ship(&self.stream_id, &frame);
        }
    }

    pub fn event_sink(&self) -> DatastreamEventSink {
        DatastreamEventSink {
            mux: self.mux.clone(),
        }
    }
}

/// A thread-safe submit handle. Legacy; prefer [`crate::DatastreamProducer`].
#[derive(Clone)]
pub struct DatastreamEventSink {
    mux: Arc<Mux>,
}

impl DatastreamEventSink {
    pub fn submit_record<R: Record>(&self, channel: ChannelId, record: &R) -> Position {
        self.mux.submit(channel, record.encode())
    }

    pub fn submit_text(&self, channel: ChannelId, text: impl AsRef<[u8]>) -> Position {
        self.mux.submit(channel, text.as_ref().to_vec())
    }

    pub fn submit_bytes(&self, channel: ChannelId, bytes: Vec<u8>) -> Position {
        self.mux.submit(channel, bytes)
    }
}

/// A sink that drops everything.
pub struct NoopSink;

impl FrameSink for NoopSink {
    fn ship(&mut self, _stream: &StreamId, _frame: &Frame) {}
}

/// Legacy swactor frame sink retained until MVP runtime cutover removes it.
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
