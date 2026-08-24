use std::collections::HashSet;
use std::sync::{Arc, Barrier};
use std::thread::{self, JoinHandle};

use telemetry::frame::{Frame, TelemetryEvent};
use telemetry::ingest::Consumer;
use telemetry::transport::Delivery;
use telemetry::wire::{decode_delivery, encode_delivery};
use telemetry::{
    ChannelContent, ChannelId, Lifetime, Mux, Position, StreamId, TelemetryEndpoint,
    TelemetrySubscription,
};

use crate::{SetupPolicy, WorkUnits, Workload};

const PAYLOAD_SIZES: [usize; 3] = [32, 1024, 65_536];
const BATCH_SIZES: [usize; 3] = [1, 64, 1024];
const STORE_BATCH: usize = 1024;
const CONCURRENT_TOTAL: usize = 4096;
const CONCURRENT_PAYLOAD_SIZE: usize = 32;

#[derive(Clone, Copy, Debug)]
pub enum StoreKind {
    Ordered,
    Reordered,
    Duplicate,
    Gaps,
}

impl StoreKind {
    fn label(self) -> &'static str {
        match self {
            Self::Ordered => "ordered",
            Self::Reordered => "reordered",
            Self::Duplicate => "duplicate",
            Self::Gaps => "gaps",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum WireOperation {
    Encode,
    Decode,
}

impl WireOperation {
    fn label(self) -> &'static str {
        match self {
            Self::Encode => "encode",
            Self::Decode => "decode",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum TelemetryWorkload {
    Mux {
        payload_size: usize,
        batch: usize,
        reject_full: bool,
    },
    Fanout {
        payload_size: usize,
        batch: usize,
        subscribers: usize,
        slow_subscriber: bool,
    },
    Store(StoreKind),
    Wire {
        operation: WireOperation,
        payload_size: usize,
    },
    Concurrent {
        producers: usize,
    },
}

pub struct MuxState {
    mux: Mux,
    payload: Vec<u8>,
    batch: usize,
    reject_full: bool,
}

pub struct FanoutState {
    endpoint: TelemetryEndpoint,
    channel: ChannelId,
    subscriptions: Vec<TelemetrySubscription>,
    payload: Vec<u8>,
    batch: usize,
    slow_subscriber: bool,
}

pub struct StoreState {
    consumer: Consumer,
    stream: StreamId,
    deliveries: Vec<Delivery>,
    kind: StoreKind,
}

pub struct WireState {
    stream: StreamId,
    frame: Frame,
    encoded: Vec<u8>,
}

pub struct ConcurrentState {
    endpoint: TelemetryEndpoint,
    start: Arc<Barrier>,
    handles: Vec<JoinHandle<usize>>,
    total: usize,
}

pub enum TelemetryState {
    Mux(MuxState),
    Fanout(FanoutState),
    Store(StoreState),
    Wire(WireState),
    Concurrent(ConcurrentState),
}

pub enum TelemetryOutput {
    Mux {
        accepted: usize,
        rejected: usize,
        dropped: u64,
        frames: Vec<Frame>,
    },
    Fanout {
        accepted: usize,
        drained: usize,
        dropped_for_subscribers: usize,
        events: Vec<Vec<TelemetryEvent>>,
        drop_counts: Vec<u64>,
        bitbucketed: u64,
    },
    Store {
        attempted: usize,
        accepted: usize,
        frames: Vec<Frame>,
        gaps: Vec<(u64, u64)>,
    },
    WireEncoded(Vec<u8>),
    WireDecoded(StreamId, Frame),
    Concurrent {
        accepted: usize,
        frames: Vec<Frame>,
        dropped: u64,
    },
}

fn deterministic_payload(size: usize) -> Vec<u8> {
    (0..size)
        .map(|index| ((index.wrapping_mul(29) + 11) % 251) as u8)
        .collect()
}

fn marked_payload(size: usize, marker: u64) -> Vec<u8> {
    let mut payload = deterministic_payload(size.max(8));
    payload[..8].copy_from_slice(&marker.to_le_bytes());
    payload.truncate(size.max(8));
    payload
}

fn marker(payload: &[u8]) -> u64 {
    u64::from_le_bytes(payload[..8].try_into().unwrap())
}

fn contiguous(positions: &[u64]) -> bool {
    positions.windows(2).all(|pair| pair[1] == pair[0] + 1)
}

fn setup_mux(payload_size: usize, batch: usize, reject_full: bool) -> MuxState {
    let mux = Mux::new(StreamId::new("bench-mux", Lifetime(1)), batch.max(1));
    let payload = deterministic_payload(payload_size);
    if reject_full {
        for _ in 0..batch {
            assert!(mux.submit(ChannelId(1), payload.clone()));
        }
    }
    MuxState {
        mux,
        payload,
        batch,
        reject_full,
    }
}

fn setup_fanout(
    payload_size: usize,
    batch: usize,
    subscribers: usize,
    slow_subscriber: bool,
) -> FanoutState {
    let endpoint = TelemetryEndpoint::with_capacity(
        StreamId::new("bench-endpoint", Lifetime(1)),
        batch,
        batch,
    );
    let channel = endpoint.register_channel("payload", ChannelContent::Bytes);
    let subscriptions = if slow_subscriber {
        vec![
            endpoint.subscribe_all_with_capacity("slow", 1),
            endpoint.subscribe_all_with_capacity("fast", batch),
        ]
    } else {
        (0..subscribers)
            .map(|index| endpoint.subscribe_all_with_capacity(format!("subscriber-{index}"), batch))
            .collect()
    };
    FanoutState {
        endpoint,
        channel,
        subscriptions,
        payload: deterministic_payload(payload_size),
        batch,
        slow_subscriber,
    }
}

fn store_deliveries(kind: StoreKind) -> (StreamId, Vec<Delivery>) {
    let stream = StreamId::new("bench-store", Lifetime(1));
    let positions: Vec<u64> = match kind {
        StoreKind::Ordered | StoreKind::Duplicate => (0..STORE_BATCH as u64).collect(),
        StoreKind::Reordered => (0..STORE_BATCH as u64).rev().collect(),
        StoreKind::Gaps => (0..STORE_BATCH as u64)
            .map(|position| position * 3)
            .collect(),
    };
    let deliveries = positions
        .into_iter()
        .map(|position| {
            Delivery::new(
                stream.clone(),
                Frame {
                    channel: ChannelId(1),
                    position: Position(position),
                    payload: marked_payload(1024, position),
                },
            )
        })
        .collect();
    (stream, deliveries)
}

fn setup_store(kind: StoreKind) -> StoreState {
    let (stream, deliveries) = store_deliveries(kind);
    let mut consumer = Consumer::new();
    if matches!(kind, StoreKind::Gaps) {
        for delivery in &deliveries {
            assert!(consumer.accept(delivery.clone()));
        }
    }
    StoreState {
        consumer,
        stream,
        deliveries,
        kind,
    }
}

fn setup_wire(payload_size: usize) -> WireState {
    let stream = StreamId::new("bench-wire", Lifetime(7));
    let frame = Frame {
        channel: ChannelId(3),
        position: Position(11),
        payload: deterministic_payload(payload_size),
    };
    let encoded = encode_delivery(&stream, &frame);
    WireState {
        stream,
        frame,
        encoded,
    }
}

fn setup_concurrent(producers: usize) -> ConcurrentState {
    let endpoint = TelemetryEndpoint::with_capacity(
        StreamId::new("bench-concurrent", Lifetime(1)),
        CONCURRENT_TOTAL,
        1,
    );
    let start = Arc::new(Barrier::new(producers + 1));
    let per_producer = CONCURRENT_TOTAL / producers;
    let mut handles = Vec::with_capacity(producers);
    for producer_index in 0..producers {
        let producer = endpoint.producer();
        let start = Arc::clone(&start);
        let payloads: Vec<Vec<u8>> = (0..per_producer)
            .map(|index| {
                marked_payload(
                    CONCURRENT_PAYLOAD_SIZE,
                    (producer_index * per_producer + index) as u64,
                )
            })
            .collect();
        handles.push(thread::spawn(move || {
            start.wait();
            payloads
                .into_iter()
                .map(|payload| usize::from(producer.submit_bytes(ChannelId(1), payload)))
                .sum()
        }));
    }
    ConcurrentState {
        endpoint,
        start,
        handles,
        total: per_producer * producers,
    }
}

fn execute_mux(state: &mut MuxState) -> TelemetryOutput {
    if state.reject_full {
        let rejected = (0..state.batch)
            .filter(|_| !state.mux.submit(ChannelId(1), state.payload.clone()))
            .count();
        return TelemetryOutput::Mux {
            accepted: 0,
            rejected,
            dropped: state.mux.dropped(),
            frames: Vec::new(),
        };
    }

    let accepted = (0..state.batch)
        .filter(|_| state.mux.submit(ChannelId(1), state.payload.clone()))
        .count();
    TelemetryOutput::Mux {
        accepted,
        rejected: state.batch - accepted,
        dropped: state.mux.dropped(),
        frames: state.mux.drain(),
    }
}

fn execute_fanout(state: &mut FanoutState) -> TelemetryOutput {
    let producer = state.endpoint.producer();
    let accepted = (0..state.batch)
        .filter(|_| producer.submit_bytes(state.channel, state.payload.clone()))
        .count();
    let tick = state.endpoint.tick();
    let events = state
        .subscriptions
        .iter()
        .map(TelemetrySubscription::drain_available)
        .collect();
    let drop_counts = state
        .endpoint
        .subscriber_snapshots()
        .into_iter()
        .map(|snapshot| snapshot.dropped)
        .collect();
    TelemetryOutput::Fanout {
        accepted,
        drained: tick.drained,
        dropped_for_subscribers: tick.dropped_for_subscribers,
        events,
        drop_counts,
        bitbucketed: state.endpoint.bitbucketed(),
    }
}

fn execute_store(state: &mut StoreState) -> TelemetryOutput {
    if matches!(state.kind, StoreKind::Gaps) {
        let stored = state.consumer.store().stream(&state.stream).unwrap();
        return TelemetryOutput::Store {
            attempted: 0,
            accepted: 0,
            frames: stored.to_vec(),
            gaps: stored
                .gap_spans()
                .into_iter()
                .map(|gap| (gap.start, gap.end))
                .collect(),
        };
    }

    let mut attempted = 0;
    let mut accepted = 0;
    for delivery in &state.deliveries {
        attempted += 1;
        accepted += usize::from(state.consumer.accept(delivery.clone()));
        if matches!(state.kind, StoreKind::Duplicate) {
            attempted += 1;
            accepted += usize::from(state.consumer.accept(delivery.clone()));
        }
    }
    let stored = state.consumer.store().stream(&state.stream).unwrap();
    TelemetryOutput::Store {
        attempted,
        accepted,
        frames: stored.to_vec(),
        gaps: Vec::new(),
    }
}

fn execute_wire(state: &WireState, operation: WireOperation) -> TelemetryOutput {
    match operation {
        WireOperation::Encode => {
            TelemetryOutput::WireEncoded(encode_delivery(&state.stream, &state.frame))
        }
        WireOperation::Decode => {
            let (stream, frame) = decode_delivery(&state.encoded).unwrap();
            TelemetryOutput::WireDecoded(stream, frame)
        }
    }
}

fn execute_concurrent(state: &mut ConcurrentState) -> TelemetryOutput {
    state.start.wait();
    let accepted = state
        .handles
        .drain(..)
        .map(|handle| handle.join().unwrap())
        .sum();
    TelemetryOutput::Concurrent {
        accepted,
        frames: state.endpoint.mux().drain(),
        dropped: state.endpoint.mux_dropped(),
    }
}

impl Workload for TelemetryWorkload {
    type State = TelemetryState;
    type Output = TelemetryOutput;

    fn name(&self) -> String {
        match self {
            Self::Mux {
                payload_size,
                batch,
                reject_full,
            } => format!(
                "telemetry/mux/{}/{payload_size}b/batch-{batch}",
                if *reject_full {
                    "reject-full"
                } else {
                    "submit-drain"
                }
            ),
            Self::Fanout {
                payload_size,
                batch,
                subscribers,
                slow_subscriber,
            } => format!(
                "telemetry/fanout/{}/{payload_size}b/batch-{batch}",
                if *slow_subscriber {
                    "slow-plus-fast".to_owned()
                } else {
                    format!("subscribers-{subscribers}")
                }
            ),
            Self::Store(kind) => format!("telemetry/store/{}", kind.label()),
            Self::Wire {
                operation,
                payload_size,
            } => format!("telemetry/wire/{}/{}b", operation.label(), payload_size),
            Self::Concurrent { producers } => {
                format!("telemetry/mux/concurrent/producers-{producers}/total-{CONCURRENT_TOTAL}")
            }
        }
    }

    fn setup(&self) -> Self::State {
        match self {
            Self::Mux {
                payload_size,
                batch,
                reject_full,
            } => TelemetryState::Mux(setup_mux(*payload_size, *batch, *reject_full)),
            Self::Fanout {
                payload_size,
                batch,
                subscribers,
                slow_subscriber,
            } => TelemetryState::Fanout(setup_fanout(
                *payload_size,
                *batch,
                *subscribers,
                *slow_subscriber,
            )),
            Self::Store(kind) => TelemetryState::Store(setup_store(*kind)),
            Self::Wire { payload_size, .. } => TelemetryState::Wire(setup_wire(*payload_size)),
            Self::Concurrent { producers } => {
                TelemetryState::Concurrent(setup_concurrent(*producers))
            }
        }
    }

    fn execute(&self, state: &mut Self::State) -> Self::Output {
        match (self, state) {
            (Self::Mux { .. }, TelemetryState::Mux(state)) => execute_mux(state),
            (Self::Fanout { .. }, TelemetryState::Fanout(state)) => execute_fanout(state),
            (Self::Store(_), TelemetryState::Store(state)) => execute_store(state),
            (Self::Wire { operation, .. }, TelemetryState::Wire(state)) => {
                execute_wire(state, *operation)
            }
            (Self::Concurrent { .. }, TelemetryState::Concurrent(state)) => {
                execute_concurrent(state)
            }
            _ => panic!("telemetry workload and state mismatch"),
        }
    }

    fn verify(&self, state: &Self::State, output: &Self::Output) {
        match (state, output) {
            (
                TelemetryState::Mux(state),
                TelemetryOutput::Mux {
                    accepted,
                    rejected,
                    dropped,
                    frames,
                },
            ) => {
                if state.reject_full {
                    assert_eq!(*accepted, 0);
                    assert_eq!(*rejected, state.batch);
                    assert_eq!(*dropped, state.batch as u64);
                    assert!(frames.is_empty());
                } else {
                    assert_eq!(*accepted, state.batch);
                    assert_eq!(*rejected, 0);
                    assert_eq!(*dropped, 0);
                    assert_eq!(frames.len(), state.batch);
                    let positions: Vec<u64> = frames.iter().map(|frame| frame.position.0).collect();
                    assert!(contiguous(&positions));
                    assert!(frames.iter().all(|frame| frame.payload == state.payload));
                }
            }
            (
                TelemetryState::Fanout(state),
                TelemetryOutput::Fanout {
                    accepted,
                    drained,
                    dropped_for_subscribers,
                    events,
                    drop_counts,
                    bitbucketed,
                },
            ) => {
                assert_eq!(*accepted, state.batch);
                assert_eq!(*drained, state.batch);
                assert_eq!(events.len(), state.subscriptions.len());
                let expected_dropped = if state.slow_subscriber {
                    state.batch.saturating_sub(1)
                } else {
                    0
                };
                assert_eq!(*dropped_for_subscribers, expected_dropped);
                assert_eq!(
                    drop_counts.iter().copied().sum::<u64>(),
                    expected_dropped as u64
                );
                for (index, events) in events.iter().enumerate() {
                    let frames: Vec<_> = events
                        .iter()
                        .map(|event| match event {
                            TelemetryEvent::Frame(frame) => frame,
                            other => panic!("unexpected fanout event: {other:?}"),
                        })
                        .collect();
                    let expected = if state.slow_subscriber && index == 0 {
                        usize::from(state.batch > 0)
                    } else {
                        state.batch
                    };
                    assert_eq!(frames.len(), expected);
                    let positions: Vec<u64> = frames.iter().map(|frame| frame.position.0).collect();
                    assert!(contiguous(&positions));
                    assert!(frames.iter().all(|frame| frame.payload == state.payload));
                }
                assert_eq!(
                    *bitbucketed,
                    if state.subscriptions.is_empty() {
                        state.batch as u64
                    } else {
                        0
                    }
                );
            }
            (
                TelemetryState::Store(state),
                TelemetryOutput::Store {
                    attempted,
                    accepted,
                    frames,
                    gaps,
                },
            ) => {
                assert_eq!(frames.len(), STORE_BATCH);
                let positions: Vec<u64> = frames.iter().map(|frame| frame.position.0).collect();
                assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
                assert!(
                    frames
                        .iter()
                        .all(|frame| marker(&frame.payload) == frame.position.0)
                );
                if matches!(state.kind, StoreKind::Gaps) {
                    assert_eq!(*attempted, 0);
                    assert_eq!(*accepted, 0);
                    assert_eq!(gaps.len(), STORE_BATCH - 1);
                    assert!(gaps.iter().all(|(start, end)| *end == *start + 1));
                } else {
                    assert_eq!(
                        *attempted,
                        if matches!(state.kind, StoreKind::Duplicate) {
                            STORE_BATCH * 2
                        } else {
                            STORE_BATCH
                        }
                    );
                    assert_eq!(*accepted, STORE_BATCH);
                    assert!(gaps.is_empty());
                    assert!(contiguous(&positions));
                }
            }
            (TelemetryState::Wire(state), TelemetryOutput::WireEncoded(encoded)) => {
                assert_eq!(encoded, &state.encoded)
            }
            (TelemetryState::Wire(state), TelemetryOutput::WireDecoded(stream, frame)) => {
                assert_eq!(stream, &state.stream);
                assert_eq!(frame, &state.frame);
            }
            (
                TelemetryState::Concurrent(state),
                TelemetryOutput::Concurrent {
                    accepted,
                    frames,
                    dropped,
                },
            ) => {
                assert_eq!(*accepted, state.total);
                assert_eq!(*dropped, 0);
                assert_eq!(frames.len(), state.total);
                let positions: Vec<u64> = frames.iter().map(|frame| frame.position.0).collect();
                let payload_ids: HashSet<u64> =
                    frames.iter().map(|frame| marker(&frame.payload)).collect();
                assert!(contiguous(&positions));
                assert_eq!(payload_ids.len(), state.total);
                assert_eq!(
                    frames
                        .iter()
                        .map(|frame| frame.payload.len())
                        .sum::<usize>(),
                    state.total * CONCURRENT_PAYLOAD_SIZE
                );
            }
            _ => panic!("telemetry workload, state, and output mismatch"),
        }
    }

    fn units(&self) -> WorkUnits {
        match self {
            Self::Mux {
                payload_size,
                batch,
                reject_full,
            } => {
                if *reject_full {
                    WorkUnits::Operations(*batch as u64)
                } else {
                    WorkUnits::Bytes((*payload_size * *batch) as u64)
                }
            }
            Self::Fanout {
                payload_size,
                batch,
                subscribers,
                slow_subscriber,
            } => {
                let copies = if *slow_subscriber {
                    2
                } else {
                    (*subscribers).max(1)
                };
                WorkUnits::Bytes((*payload_size * *batch * copies) as u64)
            }
            Self::Store(kind) => WorkUnits::Frames(if matches!(kind, StoreKind::Duplicate) {
                (STORE_BATCH * 2) as u64
            } else {
                STORE_BATCH as u64
            }),
            Self::Wire { payload_size, .. } => WorkUnits::Bytes(*payload_size as u64),
            Self::Concurrent { .. } => WorkUnits::Operations(CONCURRENT_TOTAL as u64),
        }
    }

    fn setup_policy(&self) -> SetupPolicy {
        if matches!(self, Self::Concurrent { .. }) {
            SetupPolicy::PerExecution
        } else {
            SetupPolicy::Batched
        }
    }
}

pub fn workloads() -> Vec<TelemetryWorkload> {
    let mut workloads = Vec::new();
    for payload_size in PAYLOAD_SIZES {
        for batch in BATCH_SIZES {
            workloads.push(TelemetryWorkload::Mux {
                payload_size,
                batch,
                reject_full: false,
            });
        }
        workloads.push(TelemetryWorkload::Mux {
            payload_size,
            batch: 64,
            reject_full: true,
        });
    }

    for subscribers in [0, 1, 8, 64] {
        workloads.push(TelemetryWorkload::Fanout {
            payload_size: 1024,
            batch: 64,
            subscribers,
            slow_subscriber: false,
        });
    }
    workloads.push(TelemetryWorkload::Fanout {
        payload_size: 1024,
        batch: 64,
        subscribers: 2,
        slow_subscriber: true,
    });

    workloads.extend([
        TelemetryWorkload::Store(StoreKind::Ordered),
        TelemetryWorkload::Store(StoreKind::Reordered),
        TelemetryWorkload::Store(StoreKind::Duplicate),
        TelemetryWorkload::Store(StoreKind::Gaps),
    ]);

    for payload_size in PAYLOAD_SIZES {
        workloads.push(TelemetryWorkload::Wire {
            operation: WireOperation::Encode,
            payload_size,
        });
        workloads.push(TelemetryWorkload::Wire {
            operation: WireOperation::Decode,
            payload_size,
        });
    }

    for producers in [1, 2, 4, 8] {
        workloads.push(TelemetryWorkload::Concurrent { producers });
    }
    workloads
}
