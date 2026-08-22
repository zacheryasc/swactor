//! Process-local telemetry endpoint.
//!
//! The endpoint is the stream owner: it allocates numeric channel ids, stores
//! stream/channel metadata, orders producer frames through the mux, and fans
//! catalog-aware events to subscribers.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_channel::{
    Receiver, RecvError, RecvTimeoutError, Sender, TryRecvError, TrySendError, bounded,
};

use serde::{Deserialize, Serialize};
use swactor::process_observer::ProcessOutputObserver;
use swactor::stats::{ActorSnapshot, StatsHook};

use crate::emit::ProcessChannelRouter;
use crate::frame::{
    ChannelContent, ChannelDescriptor, ChannelFilter, ChannelId, ChannelRef, FrameDelivery,
    SourceFilter, StreamDescriptor, StreamId, StreamOrigin, SubscriptionRequest, TelemetryEvent,
};
use crate::mux::Mux;
use crate::record::Record;
use crate::transport::Delivery;

const DEFAULT_MUX_CAPACITY: usize = 4096;
const DEFAULT_SUBSCRIBER_CAPACITY: usize = 1024;
const DEFAULT_STATS_CHANNEL: &str = "runtime.actors";

/// Stable handle identifying a local telemetry subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionId(pub u64);

/// Drain statistics for one endpoint tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EndpointTick {
    /// Events drained from the endpoint mux.
    pub drained: usize,
    /// Event copies successfully enqueued to subscribers.
    pub delivered: usize,
    /// Event copies dropped because a subscriber queue was full or closed.
    pub dropped_for_subscribers: usize,
    /// Number of subscribers present when the batch was published.
    pub subscribers: usize,
}

/// Snapshot of one subscriber's local fanout state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriberSnapshot {
    pub id: SubscriptionId,
    pub name: String,
    pub dropped: u64,
}

/// Current catalog snapshot delivered at subscription time, filtered by request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetrySnapshot {
    pub streams: Vec<StreamDescriptor>,
    pub channels: Vec<ChannelDescriptor>,
}

/// Channel registration failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelRegistrationError {
    ConflictingName { name: String },
}

/// Bounded future-event subscription.
pub struct TelemetrySubscription {
    id: SubscriptionId,
    name: String,
    request: SubscriptionRequest,
    snapshot: TelemetrySnapshot,
    rx: Receiver<TelemetryEvent>,
}

impl TelemetrySubscription {
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn request(&self) -> &SubscriptionRequest {
        &self.request
    }

    pub fn snapshot(&self) -> &TelemetrySnapshot {
        &self.snapshot
    }

    pub fn try_recv(&self) -> Result<TelemetryEvent, TryRecvError> {
        self.rx.try_recv()
    }

    pub fn recv(&self) -> Result<TelemetryEvent, RecvError> {
        self.rx.recv()
    }

    pub fn recv_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<TelemetryEvent, RecvTimeoutError> {
        self.rx.recv_timeout(timeout)
    }

    pub fn drain_available(&self) -> Vec<TelemetryEvent> {
        let mut out = Vec::new();
        while let Ok(event) = self.rx.try_recv() {
            out.push(event);
        }
        out
    }
}

struct SubscriberSlot {
    name: String,
    tx: Sender<TelemetryEvent>,
    dropped: u64,
}

struct FanoutTarget {
    id: SubscriptionId,
    tx: Sender<TelemetryEvent>,
}

struct FanoutReport {
    id: SubscriptionId,
    dropped: u64,
    disconnected: bool,
}

struct FanoutState {
    next_id: u64,
    subscribers: BTreeMap<SubscriptionId, SubscriberSlot>,
}

/// Local fanout; future events are broadcast to every subscriber without request filtering.
pub struct DeliveryFanout {
    default_capacity: usize,
    state: Mutex<FanoutState>,
}

impl DeliveryFanout {
    pub fn new(default_capacity: usize) -> Self {
        Self {
            default_capacity: default_capacity.max(1),
            state: Mutex::new(FanoutState {
                next_id: 1,
                subscribers: BTreeMap::new(),
            }),
        }
    }

    pub fn subscribe_all(
        &self,
        name: impl Into<String>,
        snapshot: TelemetrySnapshot,
    ) -> TelemetrySubscription {
        self.subscribe(name, SubscriptionRequest::all(), snapshot)
    }

    pub fn subscribe(
        &self,
        name: impl Into<String>,
        request: SubscriptionRequest,
        snapshot: TelemetrySnapshot,
    ) -> TelemetrySubscription {
        self.subscribe_with_capacity(name, request, snapshot, self.default_capacity)
    }

    pub fn subscribe_with_capacity(
        &self,
        name: impl Into<String>,
        request: SubscriptionRequest,
        snapshot: TelemetrySnapshot,
        capacity: usize,
    ) -> TelemetrySubscription {
        let name = name.into();
        let (tx, rx) = bounded(capacity.max(1));
        let mut state = self.state.lock().expect("telemetry fanout poisoned");
        let id = SubscriptionId(state.next_id);
        state.next_id = state.next_id.wrapping_add(1).max(1);
        state.subscribers.insert(
            id,
            SubscriberSlot {
                name: name.clone(),
                tx,
                dropped: 0,
            },
        );
        TelemetrySubscription {
            id,
            name,
            request,
            snapshot,
            rx,
        }
    }

    pub fn subscriber_count(&self) -> usize {
        self.state
            .lock()
            .expect("telemetry fanout poisoned")
            .subscribers
            .len()
    }

    pub fn subscriber_snapshots(&self) -> Vec<SubscriberSnapshot> {
        self.state
            .lock()
            .expect("telemetry fanout poisoned")
            .subscribers
            .iter()
            .map(|(id, slot)| SubscriberSnapshot {
                id: *id,
                name: slot.name.clone(),
                dropped: slot.dropped,
            })
            .collect()
    }

    pub fn publish(&self, event: TelemetryEvent) -> EndpointTick {
        self.publish_batch(std::iter::once(event))
    }

    pub fn publish_batch(&self, events: impl IntoIterator<Item = TelemetryEvent>) -> EndpointTick {
        let events: Vec<TelemetryEvent> = events.into_iter().collect();
        if events.is_empty() {
            return EndpointTick::default();
        }

        // Snapshot sender handles while holding the subscriber map lock, then
        // deliver outside the lock so large batches or slow subscribers do not
        // block subscribe/snapshot control-plane operations.
        let (targets, subscribers) = {
            let state = self.state.lock().expect("telemetry fanout poisoned");
            let subscribers = state.subscribers.len();
            let targets = state
                .subscribers
                .iter()
                .map(|(id, slot)| FanoutTarget {
                    id: *id,
                    tx: slot.tx.clone(),
                })
                .collect::<Vec<_>>();
            (targets, subscribers)
        };

        if targets.is_empty() {
            return EndpointTick {
                drained: events.len(),
                subscribers: 0,
                ..EndpointTick::default()
            };
        }

        let mut delivered = 0;
        let mut reports = Vec::new();
        for target in targets {
            let mut dropped = 0;
            let mut disconnected = false;
            for event in &events {
                match target.tx.try_send(event.clone()) {
                    Ok(()) => delivered += 1,
                    Err(TrySendError::Full(_)) => dropped += 1,
                    Err(TrySendError::Disconnected(_)) => {
                        dropped += 1;
                        disconnected = true;
                    }
                }
            }
            if dropped > 0 || disconnected {
                reports.push(FanoutReport {
                    id: target.id,
                    dropped,
                    disconnected,
                });
            }
        }

        let dropped_for_subscribers = reports.iter().map(|report| report.dropped).sum::<u64>();
        if !reports.is_empty() {
            let mut state = self.state.lock().expect("telemetry fanout poisoned");
            for report in &reports {
                if let Some(slot) = state.subscribers.get_mut(&report.id) {
                    slot.dropped = slot.dropped.saturating_add(report.dropped);
                }
            }
            for report in &reports {
                if report.disconnected {
                    state.subscribers.remove(&report.id);
                }
            }
        }

        EndpointTick {
            drained: events.len(),
            delivered,
            dropped_for_subscribers: usize::try_from(dropped_for_subscribers).unwrap_or(usize::MAX),
            subscribers,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CatalogSnapshot {
    pub streams: BTreeMap<StreamId, StreamDescriptor>,
    pub channels: BTreeMap<ChannelRef, ChannelDescriptor>,
}

impl CatalogSnapshot {
    /// Apply a subscription request to the initial metadata snapshot only.
    pub fn telemetry_snapshot(&self, request: &SubscriptionRequest) -> TelemetrySnapshot {
        let channels: Vec<ChannelDescriptor> = self
            .channels
            .values()
            .filter(|descriptor| descriptor_matches_request(descriptor, request, self))
            .cloned()
            .collect();
        let mut streams = Vec::new();
        for descriptor in self.streams.values() {
            if !source_matches(&descriptor.stream, Some(descriptor), &request.sources) {
                continue;
            }
            if matches!(request.channels, ChannelFilter::All)
                || channels
                    .iter()
                    .any(|channel| channel.stream == descriptor.stream)
            {
                streams.push(descriptor.clone());
            }
        }
        TelemetrySnapshot { streams, channels }
    }

    pub fn descriptor_for(&self, channel: &ChannelRef) -> Option<&ChannelDescriptor> {
        self.channels.get(channel)
    }

    fn stream_descriptor(&self, stream: &StreamId) -> Option<&StreamDescriptor> {
        self.streams.get(stream)
    }
}

struct ChannelCatalogState {
    stream: StreamDescriptor,
    by_id: BTreeMap<ChannelId, ChannelDescriptor>,
    by_name: BTreeMap<String, ChannelId>,
    next_channel: u32,
}

impl ChannelCatalogState {
    fn new(stream: StreamDescriptor) -> Self {
        Self {
            stream,
            by_id: BTreeMap::new(),
            by_name: BTreeMap::new(),
            next_channel: 1,
        }
    }

    fn snapshot(&self) -> CatalogSnapshot {
        let mut streams = BTreeMap::new();
        streams.insert(self.stream.stream.clone(), self.stream.clone());
        let channels = self
            .by_id
            .iter()
            .map(|(id, desc)| {
                (
                    ChannelRef {
                        stream: self.stream.stream.clone(),
                        channel: *id,
                    },
                    desc.clone(),
                )
            })
            .collect();
        CatalogSnapshot { streams, channels }
    }

    fn try_register_channel(
        &mut self,
        name: String,
        content: ChannelContent,
    ) -> Result<Option<ChannelDescriptor>, ChannelRegistrationError> {
        if let Some(id) = self.by_name.get(&name).copied() {
            let existing = self
                .by_id
                .get(&id)
                .expect("channel name and id maps stay in sync");
            if existing.content == content {
                return Ok(None);
            }
            return Err(ChannelRegistrationError::ConflictingName { name });
        }
        let id = ChannelId(self.next_channel);
        self.next_channel = self.next_channel.wrapping_add(1).max(1);
        let descriptor = ChannelDescriptor {
            stream: self.stream.stream.clone(),
            id,
            name: name.clone(),
            label: None,
            content,
        };
        self.by_name.insert(name, id);
        self.by_id.insert(id, descriptor.clone());
        Ok(Some(descriptor))
    }

    fn id_for_name(&self, name: &str) -> Option<ChannelId> {
        self.by_name.get(name).copied()
    }
}

/// Process-local telemetry endpoint.
pub struct TelemetryEndpoint {
    stream: StreamId,
    mux: Arc<Mux>,
    catalog: Arc<Mutex<ChannelCatalogState>>,
    fanout: Arc<DeliveryFanout>,
    drained: AtomicU64,
    bitbucketed: AtomicU64,
}

impl TelemetryEndpoint {
    pub fn new(stream: StreamId) -> Self {
        Self::with_capacity(stream, DEFAULT_MUX_CAPACITY, DEFAULT_SUBSCRIBER_CAPACITY)
    }

    pub fn with_capacity(
        stream: StreamId,
        mux_capacity: usize,
        subscriber_capacity: usize,
    ) -> Self {
        Self::with_descriptor(
            StreamDescriptor {
                stream,
                label: None,
                origin: StreamOrigin::RemoteNode,
            },
            mux_capacity,
            subscriber_capacity,
        )
    }

    pub fn with_descriptor(
        descriptor: StreamDescriptor,
        mux_capacity: usize,
        subscriber_capacity: usize,
    ) -> Self {
        let stream = descriptor.stream.clone();
        let mux = Arc::new(Mux::new(stream.clone(), mux_capacity.max(1)));
        Self {
            stream,
            mux,
            catalog: Arc::new(Mutex::new(ChannelCatalogState::new(descriptor))),
            fanout: Arc::new(DeliveryFanout::new(subscriber_capacity)),
            drained: AtomicU64::new(0),
            bitbucketed: AtomicU64::new(0),
        }
    }

    pub fn stream_id(&self) -> &StreamId {
        &self.stream
    }

    pub fn mux(&self) -> &Arc<Mux> {
        &self.mux
    }

    pub fn catalog_snapshot(&self) -> CatalogSnapshot {
        self.catalog
            .lock()
            .expect("telemetry catalog poisoned")
            .snapshot()
    }

    pub fn producer(&self) -> TelemetryProducer {
        TelemetryProducer {
            mux: Arc::clone(&self.mux),
            catalog: Arc::clone(&self.catalog),
            fanout: Arc::clone(&self.fanout),
        }
    }

    pub fn try_register_channel(
        &self,
        name: impl Into<String>,
        content: ChannelContent,
    ) -> Result<ChannelId, ChannelRegistrationError> {
        register_channel(&self.catalog, &self.fanout, name.into(), content)
    }

    pub fn register_channel(&self, name: impl Into<String>, content: ChannelContent) -> ChannelId {
        self.try_register_channel(name, content.clone())
            .unwrap_or_else(|err| match err {
                ChannelRegistrationError::ConflictingName { name } => {
                    panic!("conflicting telemetry channel registration for {name}")
                }
            })
    }

    pub fn register_record<R: Record>(&self) -> ChannelId {
        self.register_channel(
            R::CHANNEL,
            ChannelContent::JsonRecord {
                schema: Some(R::CHANNEL.to_owned()),
            },
        )
    }

    pub fn subscribe(
        &self,
        name: impl Into<String>,
        request: SubscriptionRequest,
    ) -> TelemetrySubscription {
        let snapshot = self.catalog_snapshot().telemetry_snapshot(&request);
        self.fanout.subscribe(name, request, snapshot)
    }

    pub fn subscribe_all(&self, name: impl Into<String>) -> TelemetrySubscription {
        self.subscribe(name, SubscriptionRequest::all())
    }

    pub fn subscribe_all_with_capacity(
        &self,
        name: impl Into<String>,
        capacity: usize,
    ) -> TelemetrySubscription {
        let request = SubscriptionRequest::all();
        let snapshot = self.catalog_snapshot().telemetry_snapshot(&request);
        self.fanout
            .subscribe_with_capacity(name, request, snapshot, capacity)
    }

    pub fn subscriber_count(&self) -> usize {
        self.fanout.subscriber_count()
    }

    pub fn subscriber_snapshots(&self) -> Vec<SubscriberSnapshot> {
        self.fanout.subscriber_snapshots()
    }

    /// Drain the mux once and broadcast future frame events; with no subscribers, drained frames are bitbucketed.
    pub fn tick(&self) -> EndpointTick {
        let events = self.drain_events();
        if events.is_empty() {
            return EndpointTick::default();
        }
        self.publish_events(events)
    }

    fn drain_events(&self) -> Vec<TelemetryEvent> {
        self.mux
            .drain()
            .into_iter()
            .map(|frame| {
                TelemetryEvent::Frame(FrameDelivery {
                    channel: ChannelRef {
                        stream: self.stream.clone(),
                        channel: frame.channel,
                    },
                    position: frame.position,
                    payload: frame.payload,
                })
            })
            .collect()
    }

    fn publish_events(&self, events: Vec<TelemetryEvent>) -> EndpointTick {
        let drained = events.len();
        self.drained.fetch_add(drained as u64, Ordering::Relaxed);
        let tick = self.fanout.publish_batch(events);
        if tick.subscribers == 0 {
            self.bitbucketed
                .fetch_add(drained as u64, Ordering::Relaxed);
        }
        tick
    }

    pub fn assigned(&self) -> u64 {
        self.mux.assigned()
    }

    pub fn mux_dropped(&self) -> u64 {
        self.mux.dropped()
    }

    pub fn drained(&self) -> u64 {
        self.drained.load(Ordering::Relaxed)
    }

    pub fn bitbucketed(&self) -> u64 {
        self.bitbucketed.load(Ordering::Relaxed)
    }
}

/// Cloneable producer handle for code that emits telemetry.
#[derive(Clone)]
pub struct TelemetryProducer {
    mux: Arc<Mux>,
    catalog: Arc<Mutex<ChannelCatalogState>>,
    fanout: Arc<DeliveryFanout>,
}

impl TelemetryProducer {
    pub fn stream_id(&self) -> &StreamId {
        self.mux.stream_id()
    }

    pub fn try_register_channel(
        &self,
        name: impl Into<String>,
        content: ChannelContent,
    ) -> Result<ChannelId, ChannelRegistrationError> {
        register_channel(&self.catalog, &self.fanout, name.into(), content)
    }

    pub fn register_channel(&self, name: impl Into<String>, content: ChannelContent) -> ChannelId {
        self.try_register_channel(name, content.clone())
            .unwrap_or_else(|err| match err {
                ChannelRegistrationError::ConflictingName { name } => {
                    panic!("conflicting telemetry channel registration for {name}")
                }
            })
    }

    pub fn register_record<R: Record>(&self) -> ChannelId {
        self.register_channel(
            R::CHANNEL,
            ChannelContent::JsonRecord {
                schema: Some(R::CHANNEL.to_owned()),
            },
        )
    }

    pub fn channel_id_for_name(&self, name: &str) -> Option<ChannelId> {
        self.catalog
            .lock()
            .expect("telemetry catalog poisoned")
            .id_for_name(name)
    }

    pub fn submit_record<R: Record>(&self, channel: ChannelId, record: &R) -> bool {
        self.mux.submit(channel, record.encode())
    }

    pub fn submit_text(&self, channel: ChannelId, text: impl AsRef<[u8]>) -> bool {
        self.mux.submit(channel, text.as_ref().to_vec())
    }

    pub fn submit_bytes(&self, channel: ChannelId, bytes: Vec<u8>) -> bool {
        self.mux.submit(channel, bytes)
    }

    pub fn submit_text_owned(&self, channel: ChannelId, text: String) -> bool {
        self.mux.submit(channel, text.into_bytes())
    }

    pub fn process_observer_with<F>(&self, channel_for: F) -> Arc<dyn ProcessOutputObserver>
    where
        F: Fn(&str, bool) -> ChannelId + Send + Sync + 'static,
    {
        Arc::new(TelemetryProcessObserver {
            producer: self.clone(),
            channel_for: Arc::new(channel_for),
        })
    }

    pub fn stats_hook(&self) -> Arc<dyn StatsHook> {
        let channel = self.register_channel(
            DEFAULT_STATS_CHANNEL,
            ChannelContent::JsonRecord {
                schema: Some(DEFAULT_STATS_CHANNEL.to_owned()),
            },
        );
        self.stats_hook_on(channel)
    }

    pub fn stats_hook_on(&self, channel: ChannelId) -> Arc<dyn StatsHook> {
        Arc::new(TelemetryStatsHook {
            producer: self.clone(),
            channel,
        })
    }
}

fn register_channel(
    catalog: &Arc<Mutex<ChannelCatalogState>>,
    fanout: &Arc<DeliveryFanout>,
    name: String,
    content: ChannelContent,
) -> Result<ChannelId, ChannelRegistrationError> {
    // New channel declarations publish a future event immediately; existing-name
    // reuse only returns the prior id.
    let (id, event) = {
        let mut catalog = catalog.lock().expect("telemetry catalog poisoned");
        match catalog.try_register_channel(name.clone(), content)? {
            Some(descriptor) => (
                descriptor.id,
                Some(TelemetryEvent::ChannelDeclared(descriptor)),
            ),
            None => {
                let id = catalog
                    .id_for_name(&name)
                    .expect("duplicate channel name remains registered");
                (id, None)
            }
        }
    };
    if let Some(event) = event {
        let _ = fanout.publish(event);
    }
    Ok(id)
}

/// Legacy/custom process-output observer adapter that submits stdout/stderr chunks as frames.
pub struct TelemetryProcessObserver {
    producer: TelemetryProducer,
    channel_for: Arc<ProcessChannelRouter>,
}

impl ProcessOutputObserver for TelemetryProcessObserver {
    fn on_output(&self, label: &str, is_stderr: bool, data: &[u8]) {
        self.producer
            .submit_bytes((self.channel_for)(label, is_stderr), data.to_vec());
    }
}

/// Runtime stats hook that submits one JSON record per productive worker tick.
pub struct TelemetryStatsHook {
    producer: TelemetryProducer,
    channel: ChannelId,
}

impl StatsHook for TelemetryStatsHook {
    fn on_tick(&self, worker_id: usize, snapshots: &[ActorSnapshot]) {
        let payload = RuntimeActorStatsRecord::from_snapshots(worker_id, snapshots);
        let bytes = serde_json::to_vec(&payload).expect("runtime stats record serializes");
        self.producer.submit_bytes(self.channel, bytes);
    }
}

#[derive(Debug, Serialize)]
struct RuntimeActorStatsRecord<'a> {
    worker_id: usize,
    actors: Vec<RuntimeActorSnapshotRecord<'a>>,
}

impl<'a> RuntimeActorStatsRecord<'a> {
    fn from_snapshots(worker_id: usize, snapshots: &'a [ActorSnapshot]) -> Self {
        Self {
            worker_id,
            actors: snapshots
                .iter()
                .map(|snapshot| RuntimeActorSnapshotRecord {
                    address: snapshot.address.to_full_hex(),
                    mailbox_depth: snapshot.mailbox_depth,
                    last_msg_type: snapshot.last_msg_type,
                    actor_type: snapshot.actor_type,
                    message_type: snapshot.message_type,
                    messages_processed: snapshot.messages_processed,
                    poisoned: snapshot.poisoned,
                    message_type_counts: snapshot
                        .message_type_counts
                        .iter()
                        .map(|(ty, count)| RuntimeMessageTypeCount { ty, count: *count })
                        .collect(),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
struct RuntimeActorSnapshotRecord<'a> {
    address: String,
    mailbox_depth: usize,
    last_msg_type: Option<&'static str>,
    actor_type: Option<&'static str>,
    message_type: Option<&'static str>,
    messages_processed: u64,
    poisoned: bool,
    message_type_counts: Vec<RuntimeMessageTypeCount<'a>>,
}

#[derive(Debug, Serialize)]
struct RuntimeMessageTypeCount<'a> {
    ty: &'a str,
    count: u64,
}

fn descriptor_matches_request(
    descriptor: &ChannelDescriptor,
    request: &SubscriptionRequest,
    catalog: &CatalogSnapshot,
) -> bool {
    let stream_descriptor = catalog.stream_descriptor(&descriptor.stream);
    source_matches(&descriptor.stream, stream_descriptor, &request.sources)
        && channel_matches(descriptor, &request.channels)
}

fn source_matches(
    stream: &StreamId,
    descriptor: Option<&StreamDescriptor>,
    filter: &SourceFilter,
) -> bool {
    match filter {
        SourceFilter::All => true,
        SourceFilter::Origin(origin) => descriptor
            .map(|descriptor| descriptor.origin == *origin)
            .unwrap_or(false),
        SourceFilter::Node(node) => &stream.node == node,
        SourceFilter::Stream(target) => stream == target,
    }
}

fn channel_matches(descriptor: &ChannelDescriptor, filter: &ChannelFilter) -> bool {
    match filter {
        ChannelFilter::All => true,
        ChannelFilter::Name(name) => descriptor.name == *name,
        ChannelFilter::Prefix(prefix) => descriptor.name.starts_with(prefix),
        ChannelFilter::Content(kind) => descriptor.content.kind() == *kind,
    }
}

/// Convert a live event back to the legacy transport delivery shape when a
/// stored-stream test or transitional adapter needs it.
pub fn frame_event_to_delivery(event: TelemetryEvent) -> Option<Delivery> {
    match event {
        TelemetryEvent::Frame(delivery) => Some(Delivery::new(
            delivery.channel.stream,
            crate::frame::Frame::new(
                delivery.channel.channel,
                delivery.position,
                delivery.payload,
            ),
        )),
        _ => None,
    }
}
