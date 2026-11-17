//! Harness-owned direct telemetry capture. Durable JSONL is the replay boundary.

use super::HarnessExecution;
use super::frame_archive::FrameArchive;
use iroh::{EndpointAddr, RelayMode};
use iroh_driver::telemetry_transport::PullCollectorConfig;
use iroh_driver::{
    IrohDriver, IrohDriverConfig, PullCollectorHandle, TelemetryQuicHeader, spawn_pull_collector,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, Runtime};
use swactor_engine::{ActorTimer, EngineHandle};
use telemetry::frame::{Frame, TelemetryEvent};
use telemetry::{
    ChannelDescriptor, ChannelRef, DeliveryFanout, StreamDescriptor, StreamId, StreamOrigin,
    SubscriptionRequest, TelemetrySnapshot, TelemetrySubscription,
};

struct CaptureState {
    archive: FrameArchive,
    subscription: TelemetrySubscription,
    headers: mpsc::Receiver<TelemetryQuicHeader>,
    streams: BTreeMap<StreamId, StreamDescriptor>,
    channels: BTreeMap<ChannelRef, ChannelDescriptor>,
    pending: Vec<TelemetryEvent>,
    error: Option<String>,
    orchestrator: Option<String>,
}

impl CaptureState {
    fn drain_headers(&mut self) {
        while let Ok(header) = self.headers.try_recv() {
            if header.stream.origin == StreamOrigin::Orchestrator {
                self.orchestrator = Some(header.stream.stream.to_string());
            }
            self.streams
                .insert(header.stream.stream.clone(), header.stream);
            for channel in header.channels {
                self.channels.insert(
                    ChannelRef {
                        stream: channel.stream.clone(),
                        channel: channel.id,
                    },
                    channel,
                );
            }
        }
    }

    fn flush(&mut self) -> Result<(), String> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        self.drain_headers();
        let mut events = std::mem::take(&mut self.pending);
        events.extend(self.subscription.drain_available());
        for event in events {
            match event {
                TelemetryEvent::StreamDeclared(stream) => {
                    if stream.origin == StreamOrigin::Orchestrator {
                        self.orchestrator = Some(stream.stream.to_string());
                    }
                    self.streams.insert(stream.stream.clone(), stream);
                }
                TelemetryEvent::ChannelDeclared(channel) => {
                    self.channels.insert(
                        ChannelRef {
                            stream: channel.stream.clone(),
                            channel: channel.id,
                        },
                        channel,
                    );
                }
                TelemetryEvent::StreamEnded(_) => {}
                TelemetryEvent::Frame(frame) => {
                    // Header and event ports are independent; preserve a delivery
                    // if its header arrives between this drain and the event poll.
                    self.drain_headers();
                    let Some(channel) = self.channels.get(&frame.channel) else {
                        self.pending.push(TelemetryEvent::Frame(frame));
                        continue;
                    };
                    let source = if self
                        .streams
                        .get(&frame.channel.stream)
                        .is_some_and(|stream| stream.origin == StreamOrigin::Orchestrator)
                    {
                        "orchestrator"
                    } else {
                        "node"
                    };
                    if let Err(error) = self.archive.record(
                        source,
                        &frame.channel.stream,
                        &channel.name,
                        &channel.content,
                        &Frame::new(frame.channel.channel, frame.position, frame.payload),
                    ) {
                        self.error = Some(error.clone());
                        return Err(error);
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct CaptureTick;

struct CapturePulse {
    runtime: Runtime,
    engine: EngineHandle,
    wake: tokio::sync::mpsc::Sender<()>,
    timer: Option<ActorTimer>,
}

impl ActorInterface for CapturePulse {
    type Incoming = CaptureTick;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        self.handle(ctx, CaptureTick);
    }

    fn handle(&mut self, ctx: &Ctx, _: CaptureTick) {
        if self.wake.is_closed() {
            ctx.stop_self();
            return;
        }
        let _ = self.wake.try_send(());
        self.timer = Some(self.engine.send_after(
            Duration::from_millis(10),
            self.runtime.create_sender(),
            ctx.self_addr(),
            CaptureTick,
        ));
    }

    fn on_stop(&mut self, _: &Ctx) {
        if let Some(timer) = self.timer.take() {
            timer.cancel();
        }
    }
}

/// Owns transport, subscriptions, and the writer until the fixture is torn down.
/// Reopening the same archive restores exact stream cursors; a new producer
/// lifetime remains distinct even when its positions restart at zero.
pub(super) struct TelemetryCapture {
    state: Arc<Mutex<CaptureState>>,
    collectors: Mutex<BTreeMap<(String, u64, u64), (EndpointAddr, PullCollectorHandle)>>,
    fanout: Arc<DeliveryFanout>,
    headers: mpsc::Sender<TelemetryQuicHeader>,
    pulse: ActorAddress,
    driver: IrohDriver,
    context: HarnessExecution,
}

impl TelemetryCapture {
    pub(super) fn start(
        path: &Path,
        relay_mode: RelayMode,
        context: &HarnessExecution,
    ) -> Result<Self, String> {
        let archive = FrameArchive::open_with_label(path, "harness telemetry")?;
        let driver = IrohDriver::with_engine(
            context.tasks.clone(),
            context.engine.clone(),
            IrohDriverConfig {
                secret_key: None,
                relay_mode,
                bind_port: None,
                node: Default::default(),
                peer_auth: None,
                additional_alpns: Vec::new(),
            },
        )
        .map_err(|error| format!("capture endpoint: {error}"))?;
        let fanout = Arc::new(DeliveryFanout::new(65_536));
        let subscription = fanout.subscribe_all(
            "e2e-evidence",
            TelemetrySnapshot {
                streams: Vec::new(),
                channels: Vec::new(),
            },
        );
        let (headers, header_rx) = mpsc::channel();
        let state = Arc::new(Mutex::new(CaptureState {
            archive,
            subscription,
            headers: header_rx,
            streams: BTreeMap::new(),
            channels: BTreeMap::new(),
            pending: Vec::new(),
            error: None,
            orchestrator: None,
        }));
        let (wake, mut requests) = tokio::sync::mpsc::channel(1);
        let writer = Arc::clone(&state);
        let pulse = match context.runtime.spawn(CapturePulse {
            runtime: context.runtime.clone(),
            engine: context.engine.clone(),
            wake,
            timer: None,
        }) {
            Ok(pulse) => pulse,
            Err(error) => {
                driver.shutdown_bounded(Duration::from_secs(1));
                return Err(format!("start capture writer pulse: {error}"));
            }
        };
        context.tasks.spawn(async move {
            while requests.recv().await.is_some() {
                let Ok(mut writer) = writer.lock() else {
                    break;
                };
                if writer.flush().is_err() {
                    break;
                }
            }
        });
        Ok(Self {
            state,
            collectors: Mutex::new(BTreeMap::new()),
            fanout,
            headers,
            pulse,
            driver,
            context: context.clone(),
        })
    }

    pub(super) fn subscribe(
        &self,
        source: &str,
        peer: EndpointAddr,
        run_id: u64,
        node_id: u64,
    ) -> Result<(), String> {
        self.flush()?;
        let mut collectors = self
            .collectors
            .lock()
            .map_err(|_| "capture subscriptions poisoned")?;
        let key = (source.to_owned(), run_id, node_id);
        if collectors
            .get(&key)
            .is_some_and(|(previous, handle)| previous == &peer && !handle.is_finished())
        {
            return Ok(());
        }
        collectors.remove(&key);
        let mut flow_id = [0; 16];
        flow_id[..8].copy_from_slice(&run_id.to_le_bytes());
        flow_id[8..].copy_from_slice(&node_id.to_le_bytes());
        let handle = spawn_pull_collector(
            &self.context.tasks,
            &self.context.engine,
            PullCollectorConfig {
                endpoint: self.driver.endpoint(),
                peer: peer.clone(),
                flow_id,
                token: Vec::new(),
                request: SubscriptionRequest::all(),
                fanout: Arc::clone(&self.fanout),
            },
            self.headers.clone(),
        );
        collectors.insert(key, (peer, handle));
        Ok(())
    }

    pub(super) fn flush(&self) -> Result<(), String> {
        self.state
            .lock()
            .map_err(|_| "capture writer poisoned")?
            .flush()
    }

    pub(super) fn orchestrator_stream(&self) -> Result<String, String> {
        let mut state = self.state.lock().map_err(|_| "capture writer poisoned")?;
        state.flush()?;
        state
            .orchestrator
            .clone()
            .ok_or_else(|| "orchestrator telemetry has not connected".to_owned())
    }
}

impl Drop for TelemetryCapture {
    fn drop(&mut self) {
        if let Ok(mut collectors) = self.collectors.lock() {
            collectors.clear();
        }
        let _ = self.context.runtime.stop_actor(self.pulse);
        self.driver.shutdown_bounded(Duration::from_secs(1));
        let _ = self.flush();
    }
}
