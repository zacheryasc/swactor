use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use proptest::prelude::*;
use telemetry::frame::{Frame, FrameDelivery, TelemetryEvent};
use telemetry::ingest::Consumer;
use telemetry::transport::Delivery;
use telemetry::wire::{WireError, decode_delivery, encode_delivery};
use telemetry::{
    ChannelContent, ChannelDescriptor, ChannelId, ChannelRegistrationError, Lifetime, Position,
    StreamId, TelemetryEndpoint, TelemetrySubscription,
};

#[derive(Clone, Debug)]
enum Action {
    RegisterFresh {
        suffix: String,
        kind: u8,
    },
    RegisterSame {
        slot: usize,
    },
    Submit {
        channel: usize,
        tail: Vec<u8>,
    },
    Tick,
    Subscribe {
        capacity: usize,
    },
    DrainSubscriber {
        slot: usize,
    },
    DropSubscriber {
        slot: usize,
    },
    IngestNew {
        stream: u8,
        position: u16,
        channel: u16,
        tail: Vec<u8>,
    },
    IngestDuplicate {
        slot: usize,
    },
    InspectCatalog,
    InspectStore,
}

fn action_strategy() -> impl Strategy<Value = Action> {
    prop_oneof![
        2 => (any::<String>(), any::<u8>())
            .prop_map(|(suffix, kind)| Action::RegisterFresh { suffix, kind }),
        1 => any::<usize>().prop_map(|slot| Action::RegisterSame { slot }),
        4 => (any::<usize>(), prop::collection::vec(any::<u8>(), 0..129))
            .prop_map(|(channel, tail)| Action::Submit { channel, tail }),
        3 => Just(Action::Tick),
        2 => any::<usize>().prop_map(|capacity| Action::Subscribe { capacity }),
        2 => any::<usize>().prop_map(|slot| Action::DrainSubscriber { slot }),
        1 => any::<usize>().prop_map(|slot| Action::DropSubscriber { slot }),
        3 => (
            any::<u8>(),
            any::<u16>(),
            any::<u16>(),
            prop::collection::vec(any::<u8>(), 0..129),
        )
            .prop_map(|(stream, position, channel, tail)| Action::IngestNew {
                stream,
                position,
                channel,
                tail,
            }),
        2 => any::<usize>().prop_map(|slot| Action::IngestDuplicate { slot }),
        1 => Just(Action::InspectCatalog),
        1 => Just(Action::InspectStore),
    ]
}

#[derive(Clone, Debug)]
struct Submission {
    channel: ChannelId,
    payload: Vec<u8>,
    accepted: bool,
    assigned_ordinal: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum EventKey {
    Channel(ChannelId),
    Frame(u64),
}

struct SubscriberLedger {
    subscription: TelemetrySubscription,
    capacity: usize,
    created_at_publication: u64,
    last_live_publication: Option<u64>,
    seen: HashSet<EventKey>,
    last_dropped: u64,
    observations: Vec<EventKey>,
}

#[derive(Clone, Copy, Default)]
struct CounterSnapshot {
    assigned: u64,
    drained: u64,
    mux_dropped: u64,
    bitbucketed: u64,
}

struct ObservationLedger {
    actions: Vec<String>,
    channels: Vec<ChannelDescriptor>,
    submissions: BTreeMap<u64, Submission>,
    pending: VecDeque<u64>,
    assigned: Vec<u64>,
    rejected: HashSet<u64>,
    observed_positions: HashMap<u64, Position>,
    position_offset: Option<i128>,
    publications: HashMap<EventKey, u64>,
    next_publication: u64,
    subscribers: Vec<Option<SubscriberLedger>>,
    deliveries: Vec<(StreamId, Frame)>,
    next_payload_id: u64,
    expected_bitbucketed: u64,
    previous_counters: CounterSnapshot,
}

impl ObservationLedger {
    fn new() -> Self {
        Self {
            actions: Vec::new(),
            channels: Vec::new(),
            submissions: BTreeMap::new(),
            pending: VecDeque::new(),
            assigned: Vec::new(),
            rejected: HashSet::new(),
            observed_positions: HashMap::new(),
            position_offset: None,
            publications: HashMap::new(),
            next_publication: 0,
            subscribers: Vec::new(),
            deliveries: Vec::new(),
            next_payload_id: 0,
            expected_bitbucketed: 0,
            previous_counters: CounterSnapshot::default(),
        }
    }

    fn publish(&mut self, key: EventKey) {
        assert!(
            self.publications
                .insert(key, self.next_publication)
                .is_none(),
            "an endpoint event was published twice"
        );
        self.next_publication += 1;
    }
}

struct Harness {
    endpoint: TelemetryEndpoint,
    consumer: Consumer,
    ledger: ObservationLedger,
    next_channel_name: u64,
}

impl Harness {
    fn new(mux_capacity: usize) -> Self {
        let endpoint = TelemetryEndpoint::with_capacity(
            StreamId::new("fuzz-endpoint", Lifetime(1)),
            mux_capacity,
            16,
        );
        let mut harness = Self {
            endpoint,
            consumer: Consumer::new(),
            ledger: ObservationLedger::new(),
            next_channel_name: 0,
        };
        harness.register_fresh("initial".to_owned(), 0);
        harness.ledger.actions.clear();
        harness
    }

    fn content(kind: u8, suffix: &str) -> ChannelContent {
        match kind % 3 {
            0 => ChannelContent::Bytes,
            1 => ChannelContent::TextStream,
            _ => ChannelContent::JsonRecord {
                schema: Some(suffix.to_owned()),
            },
        }
    }

    fn payload(&mut self, tail: Vec<u8>) -> (u64, Vec<u8>) {
        let id = self.ledger.next_payload_id;
        self.ledger.next_payload_id += 1;
        let mut payload = Vec::with_capacity(8 + tail.len());
        payload.extend_from_slice(&id.to_be_bytes());
        payload.extend_from_slice(&tail);
        (id, payload)
    }

    fn payload_id(payload: &[u8]) -> u64 {
        let prefix: [u8; 8] = payload[..8]
            .try_into()
            .expect("generated payload always carries an identity prefix");
        u64::from_be_bytes(prefix)
    }

    fn register_fresh(&mut self, suffix: String, kind: u8) {
        let name = format!("channel-{}-{suffix}", self.next_channel_name);
        self.next_channel_name += 1;
        let content = Self::content(kind, &suffix);
        let id = self
            .endpoint
            .try_register_channel(name.clone(), content.clone())
            .expect("fresh generated channel name");
        let descriptor = self
            .endpoint
            .catalog_snapshot()
            .channels
            .values()
            .find(|descriptor| descriptor.id == id)
            .cloned()
            .expect("new channel appears in catalog");
        assert_eq!(descriptor.name, name);
        assert_eq!(descriptor.content, content);
        self.ledger.channels.push(descriptor);
        self.ledger.publish(EventKey::Channel(id));
        self.ledger.actions.push("RegisterFreshChannel".into());
    }

    fn register_same(&mut self, slot: usize) {
        let descriptor = self.ledger.channels[slot % self.ledger.channels.len()].clone();
        let before = self.endpoint.catalog_snapshot().channels;
        let id = self
            .endpoint
            .try_register_channel(descriptor.name.clone(), descriptor.content.clone())
            .expect("identical registration is legal");
        assert_eq!(id, descriptor.id);
        assert_eq!(self.endpoint.catalog_snapshot().channels, before);
        self.ledger.actions.push("RegisterSameChannelAgain".into());
    }

    fn submit(&mut self, channel_slot: usize, tail: Vec<u8>) {
        let channel = self.ledger.channels[channel_slot % self.ledger.channels.len()].id;
        let (id, payload) = self.payload(tail);
        let accepted = self
            .endpoint
            .producer()
            .submit_bytes(channel, payload.clone());
        assert!(
            self.ledger
                .submissions
                .insert(
                    id,
                    Submission {
                        channel,
                        payload,
                        accepted,
                        assigned_ordinal: None,
                    },
                )
                .is_none()
        );
        if accepted {
            self.ledger.pending.push_back(id);
        } else {
            self.ledger.rejected.insert(id);
        }
        self.ledger
            .actions
            .push(format!("SubmitRegisteredPayload({accepted})"));
    }

    fn tick(&mut self) {
        let subscriberless = self.endpoint.subscriber_count() == 0;
        let assigned_now: Vec<u64> = self.ledger.pending.drain(..).collect();
        let stats = self.endpoint.tick();
        assert_eq!(stats.drained, assigned_now.len());
        if subscriberless {
            self.ledger.expected_bitbucketed += stats.drained as u64;
        }
        for id in assigned_now {
            let ordinal = self.ledger.assigned.len() as u64;
            let submission = self.ledger.submissions.get_mut(&id).unwrap();
            assert!(submission.assigned_ordinal.replace(ordinal).is_none());
            self.ledger.assigned.push(id);
            self.ledger.publish(EventKey::Frame(id));
        }
        self.ledger.actions.push("Tick".into());
    }

    fn subscribe(&mut self, raw_capacity: usize) {
        let capacity = raw_capacity % 16 + 1;
        let name = format!("subscriber-{}", self.ledger.subscribers.len());
        let subscription = self.endpoint.subscribe_all_with_capacity(name, capacity);
        let snapshot_channels: HashSet<ChannelId> = subscription
            .snapshot()
            .channels
            .iter()
            .map(|descriptor| descriptor.id)
            .collect();
        let catalog_channels: HashSet<ChannelId> = self
            .ledger
            .channels
            .iter()
            .map(|descriptor| descriptor.id)
            .collect();
        assert_eq!(snapshot_channels, catalog_channels);
        self.ledger.subscribers.push(Some(SubscriberLedger {
            subscription,
            capacity,
            created_at_publication: self.ledger.next_publication,
            last_live_publication: None,
            seen: HashSet::new(),
            last_dropped: 0,
            observations: Vec::new(),
        }));
        self.ledger.actions.push("Subscribe".into());
    }

    fn live_subscriber_slots(&self) -> Vec<usize> {
        self.ledger
            .subscribers
            .iter()
            .enumerate()
            .filter_map(|(slot, subscriber)| subscriber.as_ref().map(|_| slot))
            .collect()
    }

    fn drain_subscriber(&mut self, requested_slot: usize) {
        let live = self.live_subscriber_slots();
        if live.is_empty() {
            self.subscribe(requested_slot);
            return;
        }
        let slot = live[requested_slot % live.len()];
        let events = self.ledger.subscribers[slot]
            .as_ref()
            .unwrap()
            .subscription
            .drain_available();
        let capacity = self.ledger.subscribers[slot].as_ref().unwrap().capacity;
        assert!(events.len() <= capacity);

        for event in events {
            let key = match event {
                TelemetryEvent::ChannelDeclared(descriptor) => {
                    let original = self
                        .ledger
                        .channels
                        .iter()
                        .find(|known| known.id == descriptor.id)
                        .expect("received declaration exists in catalog history");
                    assert_eq!(&descriptor, original);
                    EventKey::Channel(descriptor.id)
                }
                TelemetryEvent::Frame(delivery) => {
                    self.check_delivery(&delivery);
                    EventKey::Frame(Self::payload_id(&delivery.payload))
                }
                other => panic!("endpoint generated unexpected event: {other:?}"),
            };
            let publication = *self
                .ledger
                .publications
                .get(&key)
                .expect("received event was previously published");
            let subscriber = self.ledger.subscribers[slot].as_mut().unwrap();
            assert!(
                subscriber.seen.insert(key.clone()),
                "subscriber saw a duplicate"
            );
            if publication >= subscriber.created_at_publication {
                if let Some(previous) = subscriber.last_live_publication {
                    assert!(
                        publication > previous,
                        "future events preserve publication order"
                    );
                }
                subscriber.last_live_publication = Some(publication);
            }
            subscriber.observations.push(key);
        }
        self.ledger.actions.push("DrainSubscriber".into());
    }

    fn check_delivery(&mut self, delivery: &FrameDelivery) {
        assert_eq!(delivery.channel.stream, self.endpoint.stream_id().clone());
        let id = Self::payload_id(&delivery.payload);
        let submission = self
            .ledger
            .submissions
            .get(&id)
            .expect("observed frame corresponds to a submission");
        assert!(submission.accepted, "rejected submission appeared");
        assert_eq!(delivery.channel.channel, submission.channel);
        assert_eq!(delivery.payload, submission.payload);
        let ordinal = submission
            .assigned_ordinal
            .expect("observed frame was assigned during an earlier tick");
        let offset = i128::from(delivery.position.0) - i128::from(ordinal);
        match self.ledger.position_offset {
            Some(expected) => assert_eq!(offset, expected, "positions are gap-free across drains"),
            None => self.ledger.position_offset = Some(offset),
        }
        if let Some(previous) = self.ledger.observed_positions.insert(id, delivery.position) {
            assert_eq!(previous, delivery.position);
        }
    }

    fn drop_subscriber(&mut self, requested_slot: usize) {
        let live = self.live_subscriber_slots();
        if live.is_empty() {
            self.subscribe(requested_slot);
            return;
        }
        let slot = live[requested_slot % live.len()];
        self.ledger.subscribers[slot].take();
        self.ledger.actions.push("DropSubscriber".into());
    }

    fn stream(raw: u8) -> StreamId {
        match raw % 4 {
            0 => StreamId::new("shared-node", Lifetime(1)),
            1 => StreamId::new("shared-node", Lifetime(2)),
            2 => StreamId::new("other-node", Lifetime(1)),
            _ => StreamId::new("other-node", Lifetime(9)),
        }
    }

    fn fresh_store_position(&self, stream: &StreamId, raw: u16) -> Position {
        let occupied: HashSet<Position> = self
            .ledger
            .deliveries
            .iter()
            .filter(|(known_stream, _)| known_stream == stream)
            .map(|(_, frame)| frame.position)
            .collect();
        let mut position = u64::from(raw);
        while occupied.contains(&Position(position)) {
            position += 1;
        }
        Position(position)
    }

    fn ingest_new(&mut self, raw_stream: u8, raw_position: u16, raw_channel: u16, tail: Vec<u8>) {
        let stream = Self::stream(raw_stream);
        let position = self.fresh_store_position(&stream, raw_position);
        let (_, payload) = self.payload(tail);
        let frame = Frame {
            channel: ChannelId(10_000 + u32::from(raw_channel)),
            position,
            payload,
        };
        assert!(
            self.consumer
                .accept(Delivery::new(stream.clone(), frame.clone()))
        );
        self.ledger.deliveries.push((stream, frame));
        self.ledger.actions.push("IngestFrame".into());
    }

    fn ingest_duplicate(&mut self, requested_slot: usize) {
        if self.ledger.deliveries.is_empty() {
            self.ingest_new(0, 0, 0, Vec::new());
            return;
        }
        let (stream, frame) =
            self.ledger.deliveries[requested_slot % self.ledger.deliveries.len()].clone();
        assert!(
            !self
                .consumer
                .accept(Delivery::new(stream.clone(), frame.clone()))
        );
        self.ledger.deliveries.push((stream, frame));
        self.ledger.actions.push("IngestDuplicate".into());
    }

    fn execute(&mut self, action: Action) {
        let action_debug = format!("{action:?}");
        self.ledger.actions.push(format!("Attempt:{action_debug}"));
        match action {
            Action::RegisterFresh { suffix, kind } => self.register_fresh(suffix, kind),
            Action::RegisterSame { slot } => self.register_same(slot),
            Action::Submit { channel, tail } => self.submit(channel, tail),
            Action::Tick => self.tick(),
            Action::Subscribe { capacity } => self.subscribe(capacity),
            Action::DrainSubscriber { slot } => self.drain_subscriber(slot),
            Action::DropSubscriber { slot } => self.drop_subscriber(slot),
            Action::IngestNew {
                stream,
                position,
                channel,
                tail,
            } => self.ingest_new(stream, position, channel, tail),
            Action::IngestDuplicate { slot } => self.ingest_duplicate(slot),
            Action::InspectCatalog => self.ledger.actions.push("InspectCatalog".into()),
            Action::InspectStore => self.ledger.actions.push("InspectStore".into()),
        }
        self.check_invariants(&action_debug);
    }

    fn check_catalog(&self) {
        let snapshot = self.endpoint.catalog_snapshot();
        assert_eq!(snapshot.channels.len(), self.ledger.channels.len());
        let by_id: HashMap<ChannelId, &ChannelDescriptor> = snapshot
            .channels
            .values()
            .map(|descriptor| (descriptor.id, descriptor))
            .collect();
        let by_name: HashMap<&str, &ChannelDescriptor> = snapshot
            .channels
            .values()
            .map(|descriptor| (descriptor.name.as_str(), descriptor))
            .collect();
        assert_eq!(
            by_id.len(),
            snapshot.channels.len(),
            "channel ids are unique"
        );
        assert_eq!(
            by_name.len(),
            snapshot.channels.len(),
            "channel names are unique"
        );
        for expected in &self.ledger.channels {
            assert_eq!(by_id.get(&expected.id).copied(), Some(expected));
            assert_eq!(by_name.get(expected.name.as_str()).copied(), Some(expected));
        }
    }

    fn expected_store(&self) -> BTreeMap<StreamId, BTreeMap<Position, Frame>> {
        let mut expected = BTreeMap::<StreamId, BTreeMap<Position, Frame>>::new();
        for (stream, frame) in &self.ledger.deliveries {
            expected
                .entry(stream.clone())
                .or_default()
                .entry(frame.position)
                .or_insert_with(|| frame.clone());
        }
        expected
    }

    fn check_store(&self) {
        let expected = self.expected_store();
        let actual_streams: HashSet<StreamId> =
            self.consumer.store().stream_ids().cloned().collect();
        let expected_streams: HashSet<StreamId> = expected.keys().cloned().collect();
        assert_eq!(actual_streams, expected_streams);

        for (stream, expected_frames) in expected {
            let stored = self
                .consumer
                .store()
                .stream(&stream)
                .expect("expected stream exists");
            let actual = stored.to_vec();
            let actual_positions: Vec<Position> =
                actual.iter().map(|frame| frame.position).collect();
            assert!(actual_positions.windows(2).all(|pair| pair[0] < pair[1]));
            assert_eq!(
                actual_positions.len(),
                actual_positions.iter().collect::<HashSet<_>>().len()
            );
            assert_eq!(
                actual,
                expected_frames.values().cloned().collect::<Vec<_>>(),
                "first delivery wins and iteration follows position order"
            );

            let expected_gaps: Vec<(u64, u64)> = actual_positions
                .windows(2)
                .filter_map(|pair| {
                    (pair[1].0 > pair[0].0 + 1).then_some((pair[0].0 + 1, pair[1].0 - 1))
                })
                .collect();
            let actual_gaps: Vec<(u64, u64)> = stored
                .gap_spans()
                .into_iter()
                .map(|gap| (gap.start, gap.end))
                .collect();
            assert_eq!(actual_gaps, expected_gaps);
        }
    }

    fn counter_snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            assigned: self.endpoint.assigned(),
            drained: self.endpoint.drained(),
            mux_dropped: self.endpoint.mux_dropped(),
            bitbucketed: self.endpoint.bitbucketed(),
        }
    }

    fn check_invariants(&mut self, action: &str) {
        self.check_catalog();
        self.check_store();

        assert!(
            self.ledger
                .assigned
                .iter()
                .all(|id| self.ledger.submissions[id].accepted)
        );
        assert_eq!(
            self.ledger.assigned.len(),
            self.ledger.assigned.iter().collect::<HashSet<_>>().len(),
            "accepted submission assigned at most once after {action}"
        );
        assert!(
            self.ledger
                .rejected
                .is_disjoint(&self.ledger.assigned.iter().copied().collect())
        );

        let counters = self.counter_snapshot();
        assert!(counters.assigned >= self.ledger.previous_counters.assigned);
        assert!(counters.drained >= self.ledger.previous_counters.drained);
        assert!(counters.mux_dropped >= self.ledger.previous_counters.mux_dropped);
        assert!(counters.bitbucketed >= self.ledger.previous_counters.bitbucketed);
        assert_eq!(counters.assigned, self.ledger.assigned.len() as u64);
        assert_eq!(counters.drained, self.ledger.assigned.len() as u64);
        assert_eq!(counters.mux_dropped, self.ledger.rejected.len() as u64);
        assert_eq!(counters.bitbucketed, self.ledger.expected_bitbucketed);
        self.ledger.previous_counters = counters;

        let dropped_by_id: HashMap<_, _> = self
            .endpoint
            .subscriber_snapshots()
            .into_iter()
            .map(|snapshot| (snapshot.id, snapshot.dropped))
            .collect();
        for subscriber in self.ledger.subscribers.iter_mut().flatten() {
            let dropped = dropped_by_id[&subscriber.subscription.id()];
            assert!(dropped >= subscriber.last_dropped);
            subscriber.last_dropped = dropped;
        }
    }

    fn finish(&mut self) {
        self.tick();
        for slot in self.live_subscriber_slots() {
            self.drain_subscriber(slot);
            self.check_invariants("final drain");
        }
        self.check_invariants("trace completion");
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 16,
        max_shrink_iters: 20_000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn long_legal_action_sequences_never_violate_telemetry_invariants(
        mux_capacity in 1usize..17,
        actions in prop::collection::vec(action_strategy(), 256..1025),
    ) {
        let mut harness = Harness::new(mux_capacity);
        for action in actions {
            harness.execute(action);
        }
        harness.finish();
    }
}

#[test]
fn conflicting_registration_fails_without_catalog_or_fanout_mutation() {
    let endpoint = TelemetryEndpoint::with_capacity(StreamId::new("node", Lifetime(1)), 4, 8);
    let id = endpoint
        .try_register_channel("same", ChannelContent::Bytes)
        .unwrap();
    let subscriber = endpoint.subscribe_all_with_capacity("observer", 8);
    let before = endpoint.catalog_snapshot().channels;

    let error = endpoint
        .try_register_channel("same", ChannelContent::TextStream)
        .unwrap_err();
    assert!(matches!(
        error,
        ChannelRegistrationError::ConflictingName { .. }
    ));
    assert_eq!(endpoint.catalog_snapshot().channels, before);
    assert!(
        endpoint
            .catalog_snapshot()
            .channels
            .values()
            .any(|descriptor| descriptor.id == id)
    );
    assert!(subscriber.try_recv().is_err());
}

#[test]
fn mux_rejection_never_appears_or_consumes_a_position() {
    let endpoint = TelemetryEndpoint::with_capacity(StreamId::new("node", Lifetime(1)), 1, 8);
    let channel = endpoint.register_channel("bytes", ChannelContent::Bytes);
    let subscriber = endpoint.subscribe_all_with_capacity("observer", 8);

    assert!(
        endpoint
            .producer()
            .submit_bytes(channel, b"accepted-0".to_vec())
    );
    assert!(
        !endpoint
            .producer()
            .submit_bytes(channel, b"rejected".to_vec())
    );
    endpoint.tick();
    assert!(
        endpoint
            .producer()
            .submit_bytes(channel, b"accepted-1".to_vec())
    );
    endpoint.tick();

    let frames: Vec<FrameDelivery> = subscriber
        .drain_available()
        .into_iter()
        .filter_map(|event| match event {
            TelemetryEvent::Frame(frame) => Some(frame),
            _ => None,
        })
        .collect();
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].payload, b"accepted-0");
    assert_eq!(frames[1].payload, b"accepted-1");
    assert_eq!(frames[1].position.0, frames[0].position.0 + 1);
    assert_eq!(endpoint.mux_dropped(), 1);
}

#[test]
fn full_and_disconnected_subscribers_do_not_affect_others() {
    let endpoint = TelemetryEndpoint::with_capacity(StreamId::new("node", Lifetime(1)), 8, 8);
    let channel = endpoint.register_channel("bytes", ChannelContent::Bytes);
    let slow = endpoint.subscribe_all_with_capacity("slow", 1);
    let fast = endpoint.subscribe_all_with_capacity("fast", 8);
    let slow_id = slow.id();
    let fast_id = fast.id();

    assert!(endpoint.producer().submit_bytes(channel, b"one".to_vec()));
    assert!(endpoint.producer().submit_bytes(channel, b"two".to_vec()));
    let tick = endpoint.tick();
    assert_eq!(tick.dropped_for_subscribers, 1);

    assert_eq!(slow.drain_available().len(), 1);
    assert_eq!(fast.drain_available().len(), 2);
    let dropped: HashMap<_, _> = endpoint
        .subscriber_snapshots()
        .into_iter()
        .map(|snapshot| (snapshot.id, snapshot.dropped))
        .collect();
    assert_eq!(dropped[&slow_id], 1);
    assert_eq!(dropped[&fast_id], 0);

    drop(slow);
    assert!(
        endpoint
            .producer()
            .submit_bytes(channel, b"after-drop".to_vec())
    );
    endpoint.tick();
    let event = fast
        .try_recv()
        .expect("live subscriber continues after peer drop");
    assert!(matches!(event, TelemetryEvent::Frame(frame) if frame.payload == b"after-drop"));
    assert_eq!(endpoint.subscriber_count(), 1);
}

#[test]
fn conflicting_duplicate_ingest_preserves_first_frame_and_other_streams() {
    let mut consumer = Consumer::new();
    let first_stream = StreamId::new("same-node", Lifetime(1));
    let second_stream = StreamId::new("same-node", Lifetime(2));
    let first = Frame {
        channel: ChannelId(999),
        position: Position(7),
        payload: vec![0, 1, 2, 255],
    };
    let conflicting = Frame {
        payload: b"replacement".to_vec(),
        ..first.clone()
    };

    assert!(consumer.accept(Delivery::new(first_stream.clone(), first.clone())));
    assert!(!consumer.accept(Delivery::new(first_stream.clone(), conflicting)));
    assert!(consumer.accept(Delivery::new(second_stream.clone(), first.clone())));
    assert_eq!(
        consumer.store().stream(&first_stream).unwrap().to_vec(),
        vec![first.clone()]
    );
    assert_eq!(
        consumer.store().stream(&second_stream).unwrap().to_vec(),
        vec![first]
    );
}

#[test]
fn invalid_actions_leave_all_ledger_invariants_intact() {
    let mut harness = Harness::new(1);
    harness.subscribe(1);

    harness.submit(0, b"accepted".to_vec());
    harness.submit(0, b"rejected".to_vec());
    assert_eq!(harness.endpoint.mux_dropped(), 1);
    harness.check_invariants("full mux rejection");

    let descriptor = harness.ledger.channels[0].clone();
    let before_catalog = harness.endpoint.catalog_snapshot().channels;
    assert!(matches!(
        harness
            .endpoint
            .try_register_channel(descriptor.name, ChannelContent::TextStream),
        Err(ChannelRegistrationError::ConflictingName { .. })
    ));
    assert_eq!(harness.endpoint.catalog_snapshot().channels, before_catalog);
    harness.check_invariants("conflicting registration");

    harness.register_fresh("fills-subscriber".into(), 0);
    harness.register_fresh("drops-for-subscriber".into(), 0);
    harness.check_invariants("full subscriber publication");

    harness.ingest_new(0, 9, 7, b"first".to_vec());
    let (stream, first) = harness.ledger.deliveries.last().unwrap().clone();
    let conflicting = Frame {
        payload: b"conflicting".to_vec(),
        ..first
    };
    assert!(!harness.consumer.accept(Delivery::new(stream, conflicting)));
    harness.check_invariants("conflicting duplicate ingest");
}

fn sample_delivery_bytes() -> Vec<u8> {
    encode_delivery(
        &StreamId::new("node", Lifetime(9)),
        &Frame {
            channel: ChannelId(4),
            position: Position(12),
            payload: vec![0, 1, 2, 255],
        },
    )
}

#[test]
fn telemetry_wire_rejects_every_truncation_and_trailing_bytes() {
    let encoded = sample_delivery_bytes();
    for length in 0..encoded.len() {
        assert!(
            matches!(
                decode_delivery(&encoded[..length]),
                Err(WireError::Truncated | WireError::BadLength)
            ),
            "accepted truncation at {length}"
        );
    }
    let mut trailing = encoded;
    trailing.push(0);
    assert_eq!(decode_delivery(&trailing), Err(WireError::TrailingBytes));
}

#[test]
fn telemetry_wire_rejects_invalid_lengths_and_utf8() {
    let mut invalid_length = sample_delivery_bytes();
    invalid_length[..4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        decode_delivery(&invalid_length),
        Err(WireError::Truncated | WireError::BadLength)
    ));

    let mut invalid_utf8 = sample_delivery_bytes();
    assert_eq!(u32::from_le_bytes(invalid_utf8[..4].try_into().unwrap()), 4);
    invalid_utf8[4] = 0xff;
    assert_eq!(decode_delivery(&invalid_utf8), Err(WireError::NotUtf8));

    let (stream, frame) = decode_delivery(&sample_delivery_bytes()).unwrap();
    assert_eq!(stream, StreamId::new("node", Lifetime(9)));
    assert_eq!(frame.payload, vec![0, 1, 2, 255]);
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 512,
        max_shrink_iters: 10_000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn arbitrary_wire_bytes_decode_or_fail_without_panicking(
        bytes in prop::collection::vec(any::<u8>(), 0..2048),
    ) {
        if let Ok((stream, frame)) = decode_delivery(&bytes) {
            let canonical = encode_delivery(&stream, &frame);
            prop_assert_eq!(decode_delivery(&canonical).unwrap(), (stream, frame));
        }
    }
}
