//! Remote telemetry subscriptions owned by the dashboard integration.

use crate::{DashboardHandle, FrameEvent, StreamEvent};
use iroh::EndpointAddr;
use iroh_driver::telemetry_transport::PullCollectorConfig;
use iroh_driver::{PullCollectorHandle, TelemetryQuicHeader, spawn_pull_collector};
use std::collections::BTreeMap;
use std::sync::{Arc, mpsc};
use std::time::Duration;
use swactor_engine::{EngineHandle, TaskSpawner};
use telemetry::frame::TelemetryEvent;
use telemetry::{
    ChannelRef, DeliveryFanout, Position, StreamOrigin, SubscriptionRequest, TelemetryEndpoint,
    TelemetrySnapshot, TelemetrySubscription,
};

enum Command {
    Subscribe(EndpointAddr, u64, u64),
    Unsubscribe(u64, u64),
    ObserveLocal(TelemetrySubscription),
}

/// Dropping the integration stops every remote subscription and its publishing task.
pub struct RemoteTelemetry {
    commands: tokio::sync::mpsc::UnboundedSender<Command>,
}

impl RemoteTelemetry {
    pub fn new(
        tasks: &TaskSpawner,
        engine: &EngineHandle,
        endpoint: iroh::Endpoint,
        dashboard: DashboardHandle,
    ) -> Self {
        let (commands, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let fanout = Arc::new(DeliveryFanout::new(16_384));
        let subscription = fanout.subscribe_all(
            "dashboard",
            TelemetrySnapshot {
                streams: Vec::new(),
                channels: Vec::new(),
            },
        );
        let (headers_tx, headers_rx) = mpsc::channel::<TelemetryQuicHeader>();
        let subscription_tasks = tasks.clone();
        let engine = engine.clone();
        tasks.spawn(async move {
            let mut collectors: BTreeMap<(u64, u64), (EndpointAddr, PullCollectorHandle)> = BTreeMap::new();
            let mut streams = BTreeMap::new();
            let mut channels = BTreeMap::new();
            let mut positions = BTreeMap::<_, Position>::new();
            let mut local = Vec::new();
            let mut pending = Vec::new();
            loop {
                tokio::select! {
                    command = requests.recv() => match command {
                        None => break,
                        Some(Command::Unsubscribe(run, node)) => { collectors.remove(&(run, node)); }
                        Some(Command::ObserveLocal(subscription)) => {
                            for stream in &subscription.snapshot().streams {
                                streams.insert(stream.stream.clone(), stream.clone());
                            }
                            for channel in &subscription.snapshot().channels {
                                channels.insert(ChannelRef { stream: channel.stream.clone(), channel: channel.id }, channel.clone());
                            }
                            local.push(subscription);
                        }
                        Some(Command::Subscribe(peer, run, node)) => {
                            if collectors.get(&(run, node)).is_some_and(|(previous, handle)| previous == &peer && !handle.is_finished()) { continue; }
                            collectors.remove(&(run, node));
                            let mut flow_id = [0; 16];
                            flow_id[..8].copy_from_slice(&run.to_le_bytes());
                            flow_id[8..].copy_from_slice(&node.to_le_bytes());
                            let handle = spawn_pull_collector(&subscription_tasks, &engine, PullCollectorConfig {
                                endpoint: endpoint.clone(), peer: peer.clone(), flow_id, token: Vec::new(), request: SubscriptionRequest::all(), fanout: Arc::clone(&fanout),
                            }, headers_tx.clone());
                            collectors.insert((run, node), (peer, handle));
                        }
                    },
                    _ = engine.timer(Duration::from_millis(10)) => {}
                }
                let mut events = std::mem::take(&mut pending);
                events.extend(subscription.drain_available());
                for subscription in &local {
                    events.extend(subscription.drain_available());
                }
                while let Ok(header) = headers_rx.try_recv() {
                    streams.insert(header.stream.stream.clone(), header.stream);
                    for channel in header.channels {
                        channels.insert(ChannelRef { stream: channel.stream.clone(), channel: channel.id }, channel);
                    }
                }
                for event in events {
                    match event {
                        TelemetryEvent::StreamDeclared(stream) => { streams.insert(stream.stream.clone(), stream); }
                        TelemetryEvent::ChannelDeclared(channel) => { channels.insert(ChannelRef { stream: channel.stream.clone(), channel: channel.id }, channel); }
                        TelemetryEvent::StreamEnded(stream) => {
                            streams.remove(&stream);
                            channels.retain(|key, _| key.stream != stream);
                        }
                        TelemetryEvent::Frame(frame) => {
                            if positions.get(&frame.channel.stream).is_some_and(|position| frame.position <= *position) { continue; }
                            let Some(channel) = channels.get(&frame.channel) else {
                                pending.push(TelemetryEvent::Frame(frame));
                                continue;
                            };
                            let stream = &frame.channel.stream;
                            let descriptor = streams.get(stream);
                            let origin = descriptor.map(|descriptor| match descriptor.origin {
                                StreamOrigin::Orchestrator => "orchestrator",
                                StreamOrigin::Bootstrap => "bootstrap",
                                StreamOrigin::RemoteNode => "remote-node",
                            }.to_owned());
                            dashboard.publish(FrameEvent {
                                stream: StreamEvent { node: stream.node.to_string(), life: stream.life.0, origin, label: descriptor.and_then(|descriptor| descriptor.label.clone()) },
                                channel: channel.name.clone(), position: frame.position.0, payload: frame.payload, content: channel.content.clone(),
                            });
                            positions.insert(frame.channel.stream, frame.position);
                        }
                    }
                }
            }
        });
        Self { commands }
    }

    pub fn observe_local(&self, endpoint: &TelemetryEndpoint) {
        let subscription =
            endpoint.subscribe_retained("dashboard-local", SubscriptionRequest::all());
        let _ = self.commands.send(Command::ObserveLocal(subscription));
    }

    pub fn subscribe(&self, peer: EndpointAddr, run_id: u64, node_id: u64) {
        let _ = self
            .commands
            .send(Command::Subscribe(peer, run_id, node_id));
    }

    pub fn unsubscribe(&self, run_id: u64, node_id: u64) {
        let _ = self.commands.send(Command::Unsubscribe(run_id, node_id));
    }
}
