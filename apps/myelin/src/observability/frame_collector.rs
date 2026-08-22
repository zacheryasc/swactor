//! Frame collection and load-progress extraction for the control loop.
//!
//! [`FrameCollector`] wraps the mpsc channel that buffers telemetry frames
//! drained from the iroh driver. It exposes drain methods that forward
//! queued frames to sinks (dashboard/archive) via closures; the control loop
//! never names [`Frame`] or [`TelemetryEvent`] directly — frames reach sinks
//! only through these closures.

use iroh::EndpointAddr;
use iroh_driver::telemetry_transport::PullCollectorConfig;
use iroh_driver::{IrohDriver, PullCollectorHandle, TelemetryQuicHeader, spawn_pull_collector};
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, mpsc};
use swactor_engine::EngineHandle;
use telemetry::frame::{ChannelRef, Frame, StreamId, TelemetryEvent};
use telemetry::{
    DeliveryFanout, StreamDescriptor, SubscriptionRequest, TelemetrySnapshot, TelemetrySubscription,
};

use crate::observability::orch_telemetry::DashboardSupport;

/// A telemetry frame queued from a remote node, with its resolved channel name.
#[derive(Clone, Debug)]
struct CollectedTelemetryFrame {
    stream: StreamId,
    descriptor: Option<StreamDescriptor>,
    channel_name: String,
    frame: Frame,
}

/// Buffers telemetry frames drained from the driver and forwards them to sinks.
pub(crate) struct FrameCollector {
    tx: mpsc::Sender<CollectedTelemetryFrame>,
    rx: mpsc::Receiver<CollectedTelemetryFrame>,
    pull_fanout: Arc<DeliveryFanout>,
    pull_subscription: TelemetrySubscription,
    pull_header_tx: mpsc::Sender<TelemetryQuicHeader>,
    pull_header_rx: mpsc::Receiver<TelemetryQuicHeader>,
    pull_channels: Mutex<BTreeMap<ChannelRef, String>>,
    pull_streams: Mutex<BTreeMap<StreamId, StreamDescriptor>>,
    pull_stream_owners: Mutex<BTreeMap<StreamId, (u64, u64)>>,
    pull_collectors: Mutex<BTreeMap<(u64, u64), PullCollectorHandle>>,
}

impl FrameCollector {
    pub(crate) fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        let pull_fanout = Arc::new(DeliveryFanout::new(4096));
        let pull_subscription = pull_fanout.subscribe_all(
            "myelin-daemon",
            TelemetrySnapshot {
                streams: Vec::new(),
                channels: Vec::new(),
            },
        );
        let (pull_header_tx, pull_header_rx) = mpsc::channel();
        Self {
            tx,
            rx,
            pull_fanout,
            pull_subscription,
            pull_header_tx,
            pull_header_rx,
            pull_channels: Mutex::new(BTreeMap::new()),
            pull_streams: Mutex::new(BTreeMap::new()),
            pull_stream_owners: Mutex::new(BTreeMap::new()),
            pull_collectors: Mutex::new(BTreeMap::new()),
        }
    }

    /// Dial a bootstrapped node and retain its live telemetry subscription.
    pub(crate) fn subscribe_node(
        &self,
        engine: &EngineHandle,
        endpoint: iroh::Endpoint,
        peer: EndpointAddr,
        run_id: u64,
        node_id: u64,
    ) {
        let mut flow_id = [0_u8; 16];
        flow_id[..8].copy_from_slice(&run_id.to_le_bytes());
        flow_id[8..].copy_from_slice(&node_id.to_le_bytes());
        let collector = spawn_pull_collector(
            engine,
            PullCollectorConfig {
                endpoint,
                peer,
                flow_id,
                token: Vec::new(),
                request: SubscriptionRequest::all(),
                fanout: Arc::clone(&self.pull_fanout),
            },
            self.pull_header_tx.clone(),
        );
        if let Some(previous) = self
            .pull_collectors
            .lock()
            .insert((run_id, node_id), collector)
        {
            previous.cancel();
        }
    }
    /// Stop retaining and reconnecting a telemetry subscription for a terminal node.
    pub(crate) fn unsubscribe_node(&self, run_id: u64, node_id: u64) {
        if let Some(collector) = self.pull_collectors.lock().remove(&(run_id, node_id)) {
            collector.cancel();
        }
        let mut ended = BTreeSet::new();
        self.pull_stream_owners.lock().retain(|stream, owner| {
            if *owner == (run_id, node_id) {
                ended.insert(stream.clone());
                false
            } else {
                true
            }
        });
        if !ended.is_empty() {
            self.pull_streams
                .lock()
                .retain(|stream, _| !ended.contains(stream));
            self.pull_channels
                .lock()
                .retain(|channel, _| !ended.contains(&channel.stream));
        }
    }

    fn pump_pulls(&self) {
        while let Ok(header) = self.pull_header_rx.try_recv() {
            self.pull_streams
                .lock()
                .insert(header.stream.stream.clone(), header.stream.clone());
            let mut run_id = [0_u8; 8];
            run_id.copy_from_slice(&header.flow_id[..8]);
            let mut node_id = [0_u8; 8];
            node_id.copy_from_slice(&header.flow_id[8..]);
            self.pull_stream_owners.lock().insert(
                header.stream.stream.clone(),
                (u64::from_le_bytes(run_id), u64::from_le_bytes(node_id)),
            );
            let mut channels = self.pull_channels.lock();
            for descriptor in header.channels {
                channels.insert(
                    ChannelRef {
                        stream: descriptor.stream,
                        channel: descriptor.id,
                    },
                    descriptor.name,
                );
            }
        }
        for event in self.pull_subscription.drain_available() {
            match event {
                TelemetryEvent::StreamDeclared(descriptor) => {
                    self.pull_streams
                        .lock()
                        .insert(descriptor.stream.clone(), descriptor);
                }
                TelemetryEvent::ChannelDeclared(descriptor) => {
                    self.pull_channels.lock().insert(
                        ChannelRef {
                            stream: descriptor.stream,
                            channel: descriptor.id,
                        },
                        descriptor.name,
                    );
                }
                TelemetryEvent::Frame(delivery) => {
                    let stream = delivery.channel.stream;
                    let channel_name = self
                        .pull_channels
                        .lock()
                        .get(&ChannelRef {
                            stream: stream.clone(),
                            channel: delivery.channel.channel,
                        })
                        .cloned()
                        .unwrap_or_else(|| format!("channel#{}", delivery.channel.channel.0));
                    let descriptor = self.pull_streams.lock().get(&stream).cloned();
                    let _ = self.tx.send(CollectedTelemetryFrame {
                        stream,
                        descriptor,
                        channel_name,
                        frame: Frame::new(
                            delivery.channel.channel,
                            delivery.position,
                            delivery.payload,
                        ),
                    });
                }
                TelemetryEvent::StreamEnded(stream) => {
                    self.pull_streams.lock().remove(&stream);
                    self.pull_stream_owners.lock().remove(&stream);
                    self.pull_channels
                        .lock()
                        .retain(|channel, _| channel.stream != stream);
                }
            }
        }
    }

    /// Drain push-compatible and collector-initiated telemetry into the queue.
    pub(crate) fn pump(&self, driver: &IrohDriver) {
        drain_telemetry_connections(driver, &self.tx);
        self.pump_pulls();
    }

    /// Drain queued frames, forwarding each via the closure. No progress extraction.
    pub(crate) fn drain<F>(&self, forward: F)
    where
        F: FnMut(&StreamId, Option<&StreamDescriptor>, &str, &Frame),
    {
        let mut forward = forward;
        while let Ok(collected) = self.rx.try_recv() {
            forward(
                &collected.stream,
                collected.descriptor.as_ref(),
                &collected.channel_name,
                &collected.frame,
            );
        }
    }
}

fn drain_telemetry_connections(
    driver: &IrohDriver,
    frame_tx: &mpsc::Sender<CollectedTelemetryFrame>,
) {
    for read in driver.drain_telemetry_reads() {
        let mut streams = BTreeMap::from([(
            read.header.stream.stream.clone(),
            read.header.stream.clone(),
        )]);
        let mut channels = read
            .header
            .channels
            .iter()
            .map(|descriptor| {
                (
                    ChannelRef {
                        stream: descriptor.stream.clone(),
                        channel: descriptor.id,
                    },
                    descriptor.name.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        for event in read.events {
            match event {
                TelemetryEvent::StreamDeclared(descriptor) => {
                    streams.insert(descriptor.stream.clone(), descriptor);
                }
                TelemetryEvent::ChannelDeclared(descriptor) => {
                    channels.insert(
                        ChannelRef {
                            stream: descriptor.stream.clone(),
                            channel: descriptor.id,
                        },
                        descriptor.name,
                    );
                }
                TelemetryEvent::Frame(delivery) => {
                    let stream = delivery.channel.stream;
                    let channel_name = channels
                        .get(&ChannelRef {
                            stream: stream.clone(),
                            channel: delivery.channel.channel,
                        })
                        .cloned()
                        .unwrap_or_else(|| format!("channel#{}", delivery.channel.channel.0));
                    let frame = Frame::new(
                        delivery.channel.channel,
                        delivery.position,
                        delivery.payload,
                    );
                    if frame_tx
                        .send(CollectedTelemetryFrame {
                            descriptor: streams.get(&stream).cloned(),
                            stream,
                            channel_name,
                            frame,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                TelemetryEvent::StreamEnded(stream) => {
                    streams.remove(&stream);
                }
            }
        }
    }
}

/// Publish a frame to the dashboard, if one is attached. Used by the producer
/// flush path as well as the control-loop drain closures.
pub(crate) fn ingest_dashboard_frame(
    dashboard: Option<&DashboardSupport>,
    stream: &StreamId,
    channel: &str,
    frame: &Frame,
    descriptor: Option<&StreamDescriptor>,
) {
    if let Some(dashboard) = dashboard {
        dashboard.publish_frame(stream, descriptor, channel, frame);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use telemetry::frame::{FrameDelivery, Lifetime, NodeId, Position, StreamOrigin};
    use telemetry::{ChannelContent, ChannelDescriptor, ChannelId};

    #[test]
    fn pulled_frame_preserves_remote_stream_metadata() {
        let collector = FrameCollector::new();
        let stream = StreamId::new(NodeId::new("node-7"), Lifetime(3));
        let descriptor = StreamDescriptor {
            stream: stream.clone(),
            label: Some("worker seven".to_owned()),
            origin: StreamOrigin::RemoteNode,
        };
        let channel = ChannelDescriptor {
            stream: stream.clone(),
            id: ChannelId(9),
            name: "host.net".to_owned(),
            label: None,
            content: ChannelContent::JsonRecord { schema: None },
        };
        collector
            .pull_header_tx
            .send(TelemetryQuicHeader::new(
                [7; 16],
                Vec::new(),
                descriptor.clone(),
                vec![channel.clone()],
            ))
            .unwrap();
        collector
            .pull_fanout
            .publish(TelemetryEvent::Frame(FrameDelivery {
                channel: ChannelRef {
                    stream: stream.clone(),
                    channel: channel.id,
                },
                position: Position(11),
                payload: br#"{"rx":1}"#.to_vec(),
            }));

        collector.pump_pulls();
        let mut observed = None;
        collector.drain(|stream, descriptor, channel, frame| {
            observed = Some((
                stream.clone(),
                descriptor.cloned(),
                channel.to_owned(),
                frame.clone(),
            ));
        });

        let (observed_stream, observed_descriptor, observed_channel, observed_frame) =
            observed.expect("pulled frame");
        assert_eq!(observed_stream, stream);
        assert_eq!(observed_descriptor, Some(descriptor));
        assert_eq!(observed_channel, "host.net");
        assert_eq!(observed_frame.position, Position(11));
        assert_eq!(observed_frame.payload, br#"{"rx":1}"#);
        collector
            .pull_fanout
            .publish(TelemetryEvent::StreamEnded(stream.clone()));
        collector.pump_pulls();
        assert!(!collector.pull_streams.lock().contains_key(&stream));
        assert!(
            collector
                .pull_channels
                .lock()
                .keys()
                .all(|channel| channel.stream != stream),
            "ended stream retained channel descriptors"
        );
    }

    #[test]
    fn node_unsubscribe_releases_abrupt_stream_metadata() {
        let collector = FrameCollector::new();
        let stream = StreamId::new(NodeId::new("node-9"), Lifetime(4));
        let descriptor = StreamDescriptor {
            stream: stream.clone(),
            label: Some("worker nine".to_owned()),
            origin: StreamOrigin::RemoteNode,
        };
        let channel = ChannelDescriptor {
            stream: stream.clone(),
            id: ChannelId(3),
            name: "runtime.actors".to_owned(),
            label: None,
            content: ChannelContent::JsonRecord { schema: None },
        };
        let mut flow_id = [0_u8; 16];
        flow_id[..8].copy_from_slice(&5_u64.to_le_bytes());
        flow_id[8..].copy_from_slice(&9_u64.to_le_bytes());
        collector
            .pull_header_tx
            .send(TelemetryQuicHeader::new(
                flow_id,
                Vec::new(),
                descriptor,
                vec![channel],
            ))
            .unwrap();
        collector.pump_pulls();
        assert!(collector.pull_streams.lock().contains_key(&stream));
        assert!(
            collector
                .pull_channels
                .lock()
                .keys()
                .any(|channel| channel.stream == stream)
        );

        collector.unsubscribe_node(5, 9);

        assert!(!collector.pull_streams.lock().contains_key(&stream));
        assert!(
            collector
                .pull_channels
                .lock()
                .keys()
                .all(|channel| channel.stream != stream),
            "abrupt node stop retained channel descriptors"
        );
    }
}
