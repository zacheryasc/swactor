//! Frame collection and load-progress extraction for the control loop.
//!
//! [`FrameCollector`] wraps the mpsc channel that buffers telemetry frames
//! drained from the iroh driver. It exposes drain methods that forward
//! queued frames to sinks (dashboard/archive) via closures; the control loop
//! never names [`Frame`] or [`TelemetryEvent`] directly — frames reach sinks
//! only through these closures.

use iroh::{EndpointAddr, PublicKey};
use iroh_driver::telemetry_transport::PullCollectorConfig;
use iroh_driver::{IrohDriver, PullCollectorHandle, TelemetryQuicHeader, spawn_pull_collector};
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::{Arc, mpsc};
use swactor_engine::EngineHandle;
use telemetry::frame::{ChannelRef, Frame, Lifetime, NodeId, Position, StreamId, TelemetryEvent};
use telemetry::{
    ChannelContent, ChannelDescriptor, DeliveryFanout, StreamDescriptor, SubscriptionRequest,
    TelemetrySnapshot, TelemetrySubscription,
};

use crate::observability::orch_telemetry::DashboardSupport;

/// A telemetry frame queued from a remote node, with its resolved channel name.
#[derive(Clone, Debug)]
struct CollectedTelemetryFrame {
    stream: StreamId,
    descriptor: Option<StreamDescriptor>,
    channel_name: String,
    channel_content: ChannelContent,
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
    pull_channels: Mutex<BTreeMap<ChannelRef, ChannelDescriptor>>,
    pull_streams: Mutex<BTreeMap<StreamId, StreamDescriptor>>,
    pull_stream_owners: Mutex<BTreeMap<StreamId, (u64, u64)>>,
    pull_positions: Mutex<BTreeMap<StreamId, Position>>,
    pull_collectors: Mutex<BTreeMap<(u64, u64), (PublicKey, PullCollectorHandle)>>,
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
            pull_positions: Mutex::new(BTreeMap::new()),
            pull_collectors: Mutex::new(BTreeMap::new()),
        }
    }

    /// Recover exact stream cursors before reconnecting retained worker nodes.
    /// The archive, not the prior process's receive queue, is the durable boundary.
    pub(crate) fn restore_archive(&self, path: Option<&Path>) -> Result<(), String> {
        let Some(path) = path else {
            return Ok(());
        };
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(format!(
                    "read telemetry cursors {}: {error}",
                    path.display()
                ));
            }
        };
        #[derive(serde::Deserialize)]
        struct ArchivedPosition {
            source: String,
            stream: String,
            position: u64,
        }
        let mut positions = BTreeMap::new();
        for (index, line) in BufReader::new(file).lines().enumerate() {
            let line = line
                .map_err(|error| format!("read telemetry cursors {}: {error}", path.display()))?;
            let record: ArchivedPosition = serde_json::from_str(&line).map_err(|error| {
                format!(
                    "read telemetry cursor {}:{}: {error}",
                    path.display(),
                    index + 1
                )
            })?;
            if record.source != "node" {
                continue;
            }
            let (node, lifetime) = record
                .stream
                .rsplit_once('#')
                .ok_or_else(|| format!("invalid archived telemetry stream {}", record.stream))?;
            let lifetime = lifetime.parse::<u64>().map_err(|error| {
                format!(
                    "invalid archived telemetry stream {}: {error}",
                    record.stream
                )
            })?;
            let stream = StreamId::new(NodeId::new(node), Lifetime(lifetime));
            let position = positions.entry(stream).or_insert(Position(record.position));
            *position = (*position).max(Position(record.position));
        }
        self.pull_positions.lock().extend(positions);
        Ok(())
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
        let mut collectors = self.pull_collectors.lock();
        if collectors
            .get(&(run_id, node_id))
            .is_some_and(|(peer_id, handle)| {
                *peer_id == peer.id && !handle.is_cancelled() && !handle.is_finished()
            })
        {
            return;
        }
        if let Some((_, previous)) = collectors.remove(&(run_id, node_id)) {
            previous.cancel();
        }
        let peer_id = peer.id;
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
        collectors.insert((run_id, node_id), (peer_id, collector));
    }
    /// Stop retaining and reconnecting a telemetry subscription for a terminal node.
    pub(crate) fn unsubscribe_node(&self, run_id: u64, node_id: u64) {
        if let Some((_, collector)) = self.pull_collectors.lock().remove(&(run_id, node_id)) {
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
                let channel = ChannelRef {
                    stream: descriptor.stream.clone(),
                    channel: descriptor.id,
                };
                channels.insert(channel, descriptor);
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
                    let channel = ChannelRef {
                        stream: descriptor.stream.clone(),
                        channel: descriptor.id,
                    };
                    self.pull_channels.lock().insert(channel, descriptor);
                }
                TelemetryEvent::Frame(delivery) => {
                    let stream = delivery.channel.stream;
                    {
                        let mut positions = self.pull_positions.lock();
                        if positions
                            .get(&stream)
                            .is_some_and(|position| delivery.position <= *position)
                        {
                            continue;
                        }
                        positions.insert(stream.clone(), delivery.position);
                    }
                    let channel = self
                        .pull_channels
                        .lock()
                        .get(&ChannelRef {
                            stream: stream.clone(),
                            channel: delivery.channel.channel,
                        })
                        .cloned();
                    let (channel_name, channel_content) = channel
                        .map(|descriptor| (descriptor.name, descriptor.content))
                        .unwrap_or_else(|| {
                            (
                                format!("channel#{}", delivery.channel.channel.0),
                                ChannelContent::Bytes,
                            )
                        });
                    let descriptor = self.pull_streams.lock().get(&stream).cloned();
                    let _ = self.tx.send(CollectedTelemetryFrame {
                        stream,
                        descriptor,
                        channel_name,
                        channel_content,
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
        F: FnMut(&StreamId, Option<&StreamDescriptor>, &str, &ChannelContent, &Frame),
    {
        let mut forward = forward;
        while let Ok(collected) = self.rx.try_recv() {
            forward(
                &collected.stream,
                collected.descriptor.as_ref(),
                &collected.channel_name,
                &collected.channel_content,
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
                    descriptor.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        for event in read.events {
            match event {
                TelemetryEvent::StreamDeclared(descriptor) => {
                    streams.insert(descriptor.stream.clone(), descriptor);
                }
                TelemetryEvent::ChannelDeclared(descriptor) => {
                    let channel = ChannelRef {
                        stream: descriptor.stream.clone(),
                        channel: descriptor.id,
                    };
                    channels.insert(channel, descriptor);
                }
                TelemetryEvent::Frame(delivery) => {
                    let stream = delivery.channel.stream;
                    let channel = channels
                        .get(&ChannelRef {
                            stream: stream.clone(),
                            channel: delivery.channel.channel,
                        })
                        .cloned();
                    let (channel_name, channel_content) = channel
                        .map(|descriptor| (descriptor.name, descriptor.content))
                        .unwrap_or_else(|| {
                            (
                                format!("channel#{}", delivery.channel.channel.0),
                                ChannelContent::Bytes,
                            )
                        });
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
                            channel_content,
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
    content: &ChannelContent,
    frame: &Frame,
    descriptor: Option<&StreamDescriptor>,
) {
    if let Some(dashboard) = dashboard {
        dashboard.publish_frame(stream, descriptor, channel, content, frame);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use telemetry::frame::{FrameDelivery, Lifetime, NodeId, Position, StreamOrigin};
    use telemetry::{ChannelContent, ChannelDescriptor, ChannelId};

    #[test]
    fn restored_archive_skips_replay_but_accepts_fresh_producer_positions() {
        use crate::observability::frame_archive::FrameArchive;

        let log = tempfile::NamedTempFile::new().expect("archive");
        let retained = StreamId::new(NodeId::new("7"), Lifetime(11));
        let restarted = StreamId::new(NodeId::new("7"), Lifetime(12));
        let channel = ChannelId(1);
        {
            let mut archive = FrameArchive::open_with_label(log.path(), "test").unwrap();
            for position in 0..2 {
                archive
                    .record(
                        "node",
                        &retained,
                        "runtime.log",
                        &ChannelContent::TextStream,
                        &Frame::new(channel, Position(position), vec![position as u8]),
                    )
                    .unwrap();
            }
        }
        let collector = FrameCollector::new();
        collector
            .restore_archive(Some(log.path()))
            .expect("restore durable cursor");
        for (stream, position) in [
            (retained.clone(), 0),
            (retained.clone(), 1),
            (retained.clone(), 2),
            (retained.clone(), 2),
            (retained.clone(), 3),
            (restarted.clone(), 0),
        ] {
            collector
                .pull_fanout
                .publish(TelemetryEvent::Frame(FrameDelivery {
                    channel: ChannelRef { stream, channel },
                    position: Position(position),
                    payload: vec![position as u8],
                }));
        }
        collector.pump_pulls();
        let mut observed = Vec::new();
        collector.drain(|stream, _, _, _, frame| {
            observed.push((stream.clone(), frame.position, frame.payload.clone()));
        });
        assert_eq!(
            observed,
            vec![
                (retained.clone(), Position(2), vec![2]),
                (retained, Position(3), vec![3]),
                (restarted, Position(0), vec![0]),
            ]
        );
    }

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
            content: ChannelContent::MessagePackRecord { schema: None },
        };
        let payload =
            telemetry::encode_record(&serde_json::json!({"rx": 1})).expect("encode test payload");
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
                payload: payload.clone(),
            }));

        collector.pump_pulls();
        let mut observed = None;
        collector.drain(|stream, descriptor, channel, content, frame| {
            observed = Some((
                stream.clone(),
                descriptor.cloned(),
                channel.to_owned(),
                content.clone(),
                frame.clone(),
            ));
        });

        let (
            observed_stream,
            observed_descriptor,
            observed_channel,
            observed_content,
            observed_frame,
        ) = observed.expect("pulled frame");
        assert_eq!(observed_stream, stream);
        assert_eq!(observed_descriptor, Some(descriptor));
        assert_eq!(observed_channel, "host.net");
        assert_eq!(observed_content, channel.content);
        assert_eq!(observed_frame.position, Position(11));
        assert_eq!(observed_frame.payload, payload);
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
            content: ChannelContent::MessagePackRecord { schema: None },
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
