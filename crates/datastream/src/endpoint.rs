//! Process-local datastream endpoint.
//!
//! This is the sidecar a process creates next to a swactor runtime. It is not
//! stored in core runtime state: producers get a cheap [`DatastreamProducer`],
//! frames are ordered by the local [`Mux`], and subscribers receive future
//! deliveries through bounded queues. With no subscribers, draining is a
//! bitbucket.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use serde::Serialize;
use swactor::process_observer::ProcessOutputObserver;
use swactor::stats::{ActorSnapshot, StatsHook};

use crate::frame::{ChannelId, Position, StreamId};
use crate::mux::Mux;
use crate::record::Record;
use crate::transport::Delivery;

const DEFAULT_MUX_CAPACITY: usize = 4096;
const DEFAULT_SUBSCRIBER_CAPACITY: usize = 1024;
const DEFAULT_STATS_CHANNEL: &str = "runtime.actors";

/// Stable handle identifying a local datastream subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionId(pub u64);

/// Drain statistics for one endpoint tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EndpointTick {
    /// Frames drained from the endpoint mux.
    pub drained: usize,
    /// Delivery copies successfully enqueued to subscribers.
    pub delivered: usize,
    /// Delivery copies dropped because a subscriber queue was full or closed.
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

/// Bounded future-frame subscription.
pub struct DatastreamSubscription {
    id: SubscriptionId,
    name: String,
    rx: mpsc::Receiver<Delivery>,
}

impl DatastreamSubscription {
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn try_recv(&self) -> Result<Delivery, mpsc::TryRecvError> {
        self.rx.try_recv()
    }

    pub fn recv(&self) -> Result<Delivery, mpsc::RecvError> {
        self.rx.recv()
    }

    pub fn recv_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Delivery, mpsc::RecvTimeoutError> {
        self.rx.recv_timeout(timeout)
    }

    pub fn drain_available(&self) -> Vec<Delivery> {
        let mut out = Vec::new();
        while let Ok(delivery) = self.rx.try_recv() {
            out.push(delivery);
        }
        out
    }
}

struct SubscriberSlot {
    name: String,
    tx: mpsc::SyncSender<Delivery>,
    dropped: u64,
}

struct FanoutState {
    next_id: u64,
    subscribers: BTreeMap<SubscriptionId, SubscriberSlot>,
}

/// Local delivery fanout used by endpoints and collectors.
///
/// Publishing with zero subscribers is intentionally a drop. This gives the
/// default runtime behavior requested for v0: telemetry can be produced and
/// drained without retaining historical frames, and subscriptions receive only
/// future deliveries.
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

    pub fn subscribe_all(&self, name: impl Into<String>) -> DatastreamSubscription {
        self.subscribe_all_with_capacity(name, self.default_capacity)
    }

    pub fn subscribe_all_with_capacity(
        &self,
        name: impl Into<String>,
        capacity: usize,
    ) -> DatastreamSubscription {
        let name = name.into();
        let (tx, rx) = mpsc::sync_channel(capacity.max(1));
        let mut state = self.state.lock().expect("datastream fanout poisoned");
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
        DatastreamSubscription { id, name, rx }
    }

    pub fn subscriber_count(&self) -> usize {
        self.state
            .lock()
            .expect("datastream fanout poisoned")
            .subscribers
            .len()
    }

    pub fn subscriber_snapshots(&self) -> Vec<SubscriberSnapshot> {
        self.state
            .lock()
            .expect("datastream fanout poisoned")
            .subscribers
            .iter()
            .map(|(id, slot)| SubscriberSnapshot {
                id: *id,
                name: slot.name.clone(),
                dropped: slot.dropped,
            })
            .collect()
    }

    pub fn publish(&self, delivery: Delivery) -> EndpointTick {
        self.publish_batch(std::iter::once(delivery))
    }

    pub fn publish_batch(&self, deliveries: impl IntoIterator<Item = Delivery>) -> EndpointTick {
        let deliveries: Vec<Delivery> = deliveries.into_iter().collect();
        if deliveries.is_empty() {
            return EndpointTick::default();
        }

        let mut state = self.state.lock().expect("datastream fanout poisoned");
        let subscribers = state.subscribers.len();
        if subscribers == 0 {
            return EndpointTick {
                drained: deliveries.len(),
                subscribers: 0,
                ..EndpointTick::default()
            };
        }

        let mut delivered = 0;
        let mut dropped = 0;
        let mut disconnected = Vec::new();
        for (id, slot) in state.subscribers.iter_mut() {
            for delivery in &deliveries {
                match slot.tx.try_send(delivery.clone()) {
                    Ok(()) => delivered += 1,
                    Err(mpsc::TrySendError::Full(_)) => {
                        slot.dropped = slot.dropped.saturating_add(1);
                        dropped += 1;
                    }
                    Err(mpsc::TrySendError::Disconnected(_)) => {
                        slot.dropped = slot.dropped.saturating_add(1);
                        dropped += 1;
                        disconnected.push(*id);
                        break;
                    }
                }
            }
        }
        for id in disconnected {
            state.subscribers.remove(&id);
        }

        EndpointTick {
            drained: deliveries.len(),
            delivered,
            dropped_for_subscribers: dropped,
            subscribers,
        }
    }
}

/// Process-local datastream endpoint.
pub struct DatastreamEndpoint {
    stream: StreamId,
    mux: Arc<Mux>,
    fanout: DeliveryFanout,
    drained: AtomicU64,
    bitbucketed: AtomicU64,
}

impl DatastreamEndpoint {
    pub fn new(stream: StreamId) -> Self {
        Self::with_capacity(stream, DEFAULT_MUX_CAPACITY, DEFAULT_SUBSCRIBER_CAPACITY)
    }

    pub fn with_capacity(
        stream: StreamId,
        mux_capacity: usize,
        subscriber_capacity: usize,
    ) -> Self {
        let mux = Arc::new(Mux::new(stream.clone(), mux_capacity.max(1)));
        Self {
            stream,
            mux,
            fanout: DeliveryFanout::new(subscriber_capacity),
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

    /// Enable or disable optional sidecar timing samples for newly submitted
    /// frames from this endpoint's producers.
    pub fn set_frame_timing_enabled(&self, enabled: bool) {
        self.mux.set_frame_timing_enabled(enabled);
    }

    /// Whether this endpoint currently emits sidecar frame timing samples.
    pub fn frame_timing_enabled(&self) -> bool {
        self.mux.frame_timing_enabled()
    }

    pub fn producer(&self) -> DatastreamProducer {
        DatastreamProducer {
            mux: Arc::clone(&self.mux),
        }
    }

    pub fn subscribe_all(&self, name: impl Into<String>) -> DatastreamSubscription {
        self.fanout.subscribe_all(name)
    }

    pub fn subscribe_all_with_capacity(
        &self,
        name: impl Into<String>,
        capacity: usize,
    ) -> DatastreamSubscription {
        self.fanout.subscribe_all_with_capacity(name, capacity)
    }

    pub fn subscriber_count(&self) -> usize {
        self.fanout.subscriber_count()
    }

    pub fn subscriber_snapshots(&self) -> Vec<SubscriberSnapshot> {
        self.fanout.subscriber_snapshots()
    }

    /// Drain the mux and fan out the resulting future deliveries.
    pub fn tick(&self) -> EndpointTick {
        let frames = self.mux.drain();
        if frames.is_empty() {
            return EndpointTick::default();
        }
        let drained = frames.len();
        self.drained.fetch_add(drained as u64, Ordering::Relaxed);
        let deliveries = frames
            .into_iter()
            .map(|frame| Delivery::new(self.stream.clone(), frame));
        let tick = self.fanout.publish_batch(deliveries);
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
pub struct DatastreamProducer {
    mux: Arc<Mux>,
}

impl DatastreamProducer {
    pub fn stream_id(&self) -> &StreamId {
        self.mux.stream_id()
    }

    pub fn submit_record<R: Record>(&self, record: &R) -> Position {
        self.mux.submit(R::channel(), record.encode())
    }

    pub fn submit_text(&self, channel: impl Into<ChannelId>, text: impl AsRef<[u8]>) -> Position {
        self.mux.submit(channel, text.as_ref().to_vec())
    }

    pub fn submit_bytes(&self, channel: impl Into<ChannelId>, bytes: Vec<u8>) -> Position {
        self.mux.submit(channel, bytes)
    }

    /// Enable or disable optional sidecar timing samples for this producer's mux.
    pub fn set_frame_timing_enabled(&self, enabled: bool) {
        self.mux.set_frame_timing_enabled(enabled);
    }

    /// Whether this producer's mux currently emits sidecar frame timing samples.
    pub fn frame_timing_enabled(&self) -> bool {
        self.mux.frame_timing_enabled()
    }

    pub fn process_observer_with<F>(&self, channel_for: F) -> Arc<dyn ProcessOutputObserver>
    where
        F: Fn(&str, bool) -> ChannelId + Send + Sync + 'static,
    {
        Arc::new(DatastreamProcessObserver {
            producer: self.clone(),
            channel_for: Arc::new(channel_for),
        })
    }

    pub fn stats_hook(&self) -> Arc<dyn StatsHook> {
        self.stats_hook_on(DEFAULT_STATS_CHANNEL)
    }

    pub fn stats_hook_on(&self, channel: impl Into<ChannelId>) -> Arc<dyn StatsHook> {
        Arc::new(DatastreamStatsHook {
            producer: self.clone(),
            channel: channel.into(),
        })
    }
}

/// Managed-process output observer that submits stdout/stderr chunks as frames.
pub struct DatastreamProcessObserver {
    producer: DatastreamProducer,
    channel_for: Arc<dyn Fn(&str, bool) -> ChannelId + Send + Sync>,
}

impl ProcessOutputObserver for DatastreamProcessObserver {
    fn on_output(&self, label: &str, is_stderr: bool, data: &[u8]) {
        self.producer
            .submit_bytes((self.channel_for)(label, is_stderr), data.to_vec());
    }
}

/// Runtime stats hook that submits one JSON record per productive worker tick.
pub struct DatastreamStatsHook {
    producer: DatastreamProducer,
    channel: ChannelId,
}

impl StatsHook for DatastreamStatsHook {
    fn on_tick(&self, worker_id: usize, snapshots: &[ActorSnapshot]) {
        let payload = RuntimeActorStatsRecord::from_snapshots(worker_id, snapshots);
        let bytes = serde_json::to_vec(&payload).expect("runtime stats record serializes");
        self.producer.submit_bytes(self.channel.clone(), bytes);
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
                    address: snapshot.address.to_string(),
                    mailbox_depth: snapshot.mailbox_depth,
                    last_msg_type: snapshot.last_msg_type,
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
    messages_processed: u64,
    poisoned: bool,
    message_type_counts: Vec<RuntimeMessageTypeCount<'a>>,
}

#[derive(Debug, Serialize)]
struct RuntimeMessageTypeCount<'a> {
    ty: &'a str,
    count: u64,
}
