use std::collections::HashSet;
use std::sync::{Arc, Barrier, mpsc};
use std::thread::{self, JoinHandle};

use serde::{Deserialize, Serialize};

use iroh_driver::telemetry_transport::{
    decode_event_records, encode_event_batch, encode_event_record,
};
use swactor::config::RuntimeConfig;
use swactor::runtime::RuntimeParts;
use swactor_engine::{Engine, EngineHandle, TokioBackend, TokioConfig};

use telemetry::frame::{ChannelRef, Frame, FrameDelivery, TelemetryEvent};
use telemetry::ingest::Consumer;
use telemetry::transport::Delivery;
use telemetry::wire::{decode_delivery, encode_delivery};
use telemetry::{
    ChannelContent, ChannelId, Lifetime, Mux, NodeId, Position, StreamDescriptor, StreamId,
    StreamOrigin, TelemetryEndpoint, TelemetrySubscription,
};

use crate::{SetupPolicy, WorkUnits, Workload};

const PAYLOAD_SIZES: [usize; 3] = [32, 1024, 65_536];
const BATCH_SIZES: [usize; 3] = [1, 64, 1024];
const STORE_BATCH: usize = 1024;
const CONCURRENT_TOTAL: usize = 4096;
const CONCURRENT_PAYLOAD_SIZE: usize = 32;
const ENGINE_WORKERS: usize = 4;
const ENGINE_PRODUCERS: usize = 8;
const ENGINE_CHANNELS: usize = 256;
const ENGINE_TOTAL: usize = 16_384;
const PAYLOAD_CODEC_TOTAL: usize = 4096;
const COMPRESSION_CHUNK_BYTES: usize = 256 * 1024;

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
pub enum PayloadCodec {
    Json,
    Cbor,
    MessagePack,
}

impl PayloadCodec {
    fn label(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Cbor => "cbor",
            Self::MessagePack => "messagepack",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum PayloadCodecOperation {
    Encode,
    Decode,
}

impl PayloadCodecOperation {
    fn label(self) -> &'static str {
        match self {
            Self::Encode => "encode",
            Self::Decode => "decode",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum CompressionCodec {
    Lz4,
    ZstdFast,
    Zstd,
}

impl CompressionCodec {
    fn label(self) -> &'static str {
        match self {
            Self::Lz4 => "lz4",
            Self::ZstdFast => "zstd-fast",
            Self::Zstd => "zstd",
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
    WireBatch,
    Concurrent {
        producers: usize,
    },
    EngineConcurrent,
    PayloadCodec {
        codec: PayloadCodec,
        operation: PayloadCodecOperation,
    },
    Compression(CompressionCodec),
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

pub struct WireBatchState {
    descriptor: StreamDescriptor,
    events: Vec<TelemetryEvent>,
    payload_bytes: usize,
}

pub struct ConcurrentState {
    endpoint: TelemetryEndpoint,
    start: Arc<Barrier>,
    handles: Vec<JoinHandle<usize>>,
    total: usize,
}

pub struct EngineConcurrentState {
    _engine: Engine,
    handle: EngineHandle,
    endpoint: Arc<TelemetryEndpoint>,
    subscriptions: Vec<TelemetrySubscription>,
    submissions: Vec<Vec<(ChannelId, Vec<u8>)>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MyelinBenchmarkDetail {
    channel: usize,
    message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MyelinBenchmarkRecord {
    schema_version: u32,
    event_type: String,
    producer_sequence: u64,
    node_id: u64,
    status: String,
    detail: MyelinBenchmarkDetail,
}

pub struct PayloadCodecState {
    codec: PayloadCodec,
    records: Vec<MyelinBenchmarkRecord>,
    encoded: Vec<Vec<u8>>,
}

pub struct CompressionState {
    codec: CompressionCodec,
    chunks: Vec<Vec<u8>>,
    raw_bytes: usize,
}

pub enum TelemetryState {
    Mux(MuxState),
    Fanout(FanoutState),
    Store(StoreState),
    Wire(WireState),
    WireBatch(WireBatchState),
    Concurrent(ConcurrentState),
    EngineConcurrent(EngineConcurrentState),
    PayloadCodec(PayloadCodecState),
    Compression(CompressionState),
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
    WireBatch(Vec<u8>),
    Concurrent {
        accepted: usize,
        frames: Vec<Frame>,
        dropped: u64,
    },
    EngineConcurrent {
        accepted: usize,
        drained: usize,
        delivered: usize,
        events: Vec<Vec<TelemetryEvent>>,
        dropped: u64,
    },
    PayloadEncoded(Vec<Vec<u8>>),
    PayloadDecoded(Vec<MyelinBenchmarkRecord>),
    Compressed(Vec<Vec<u8>>),
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

fn myelin_record(sequence: u64, channel: usize) -> MyelinBenchmarkRecord {
    let event_type = match channel % 4 {
        0 => "HostCpuSample",
        1 => "RuntimeActorActivity",
        2 => "WorkerStep",
        _ => "ProvisionLogLine",
    };
    MyelinBenchmarkRecord {
        schema_version: 1,
        event_type: event_type.to_owned(),
        producer_sequence: sequence,
        node_id: sequence % 64,
        status: "running".to_owned(),
        detail: MyelinBenchmarkDetail {
            channel,
            message: "telemetry benchmark workload".to_owned(),
        },
    }
}

fn encode_payload(codec: PayloadCodec, record: &MyelinBenchmarkRecord) -> Vec<u8> {
    match codec {
        PayloadCodec::Json => serde_json::to_vec(record).expect("encode benchmark JSON"),
        PayloadCodec::Cbor => {
            let mut bytes = Vec::new();
            ciborium::ser::into_writer(record, &mut bytes).expect("encode benchmark CBOR");
            bytes
        }
        PayloadCodec::MessagePack => {
            rmp_serde::to_vec_named(record).expect("encode benchmark MessagePack")
        }
    }
}

fn decode_payload(codec: PayloadCodec, payload: &[u8]) -> MyelinBenchmarkRecord {
    match codec {
        PayloadCodec::Json => serde_json::from_slice(payload).expect("decode benchmark JSON"),
        PayloadCodec::Cbor => ciborium::de::from_reader(payload).expect("decode benchmark CBOR"),
        PayloadCodec::MessagePack => {
            rmp_serde::from_slice(payload).expect("decode benchmark MessagePack")
        }
    }
}

fn myelin_payload(codec: PayloadCodec, sequence: u64, channel: usize) -> Vec<u8> {
    encode_payload(codec, &myelin_record(sequence, channel))
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

fn setup_engine_concurrent() -> EngineConcurrentState {
    let endpoint = Arc::new(TelemetryEndpoint::with_capacity(
        StreamId::new("bench-engine", Lifetime(1)),
        ENGINE_TOTAL,
        ENGINE_TOTAL,
    ));
    let channel_families = [
        "host.cpu",
        "host.gpu",
        "host.memory",
        "host.net",
        "host.storage",
        "runtime.actors",
        "myelin.worker.step",
        "myelin.provisioning.logs",
    ];
    let channels = (0..ENGINE_CHANNELS)
        .map(|index| {
            let family = channel_families[index % channel_families.len()];
            endpoint.register_channel(
                format!("{family}.{}", index / channel_families.len()),
                ChannelContent::MessagePackRecord {
                    schema: Some(family.to_owned()),
                },
            )
        })
        .collect::<Vec<_>>();
    let subscriptions = ["dashboard", "archive"]
        .into_iter()
        .map(|name| endpoint.subscribe_all_with_capacity(name, ENGINE_TOTAL))
        .collect();
    let per_producer = ENGINE_TOTAL / ENGINE_PRODUCERS;
    let submissions = (0..ENGINE_PRODUCERS)
        .map(|producer_index| {
            (0..per_producer)
                .map(|index| {
                    let marker = (producer_index * per_producer + index) as u64;
                    (
                        channels[marker as usize % channels.len()],
                        myelin_payload(
                            PayloadCodec::MessagePack,
                            marker,
                            marker as usize % channels.len(),
                        ),
                    )
                })
                .collect()
        })
        .collect();
    let parts = RuntimeParts::new(RuntimeConfig {
        worker_count: ENGINE_WORKERS,
        ..RuntimeConfig::default()
    });
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig {
            worker_threads: ENGINE_WORKERS,
            ..TokioConfig::default()
        })
        .expect("benchmark Tokio backend"),
    )
    .expect("benchmark engine");
    let handle = engine.handle();
    EngineConcurrentState {
        _engine: engine,
        handle,
        endpoint,
        subscriptions,
        submissions,
    }
}

fn setup_wire_batch_with_codec(codec: PayloadCodec) -> WireBatchState {
    let descriptor = StreamDescriptor {
        stream: StreamId::new(NodeId::new("myelin-benchmark-node"), Lifetime(7)),
        label: Some("myelin worker node".to_owned()),
        origin: StreamOrigin::RemoteNode,
    };
    let events = (0..ENGINE_TOTAL)
        .map(|sequence| {
            let channel = sequence % ENGINE_CHANNELS;
            TelemetryEvent::Frame(FrameDelivery {
                channel: ChannelRef {
                    stream: descriptor.stream.clone(),
                    channel: ChannelId((channel + 1) as u32),
                },
                position: Position(sequence as u64),
                payload: myelin_payload(codec, sequence as u64, channel),
            })
        })
        .collect::<Vec<_>>();
    let payload_bytes = events
        .iter()
        .map(|event| match event {
            TelemetryEvent::Frame(frame) => frame.payload.len(),
            _ => 0,
        })
        .sum();
    WireBatchState {
        descriptor,
        events,
        payload_bytes,
    }
}

fn setup_wire_batch() -> WireBatchState {
    setup_wire_batch_with_codec(PayloadCodec::MessagePack)
}

fn setup_compression(codec: CompressionCodec) -> CompressionState {
    let state = setup_wire_batch();
    let mut chunks = vec![Vec::with_capacity(COMPRESSION_CHUNK_BYTES)];
    let mut record = Vec::new();
    for event in &state.events {
        record.clear();
        encode_event_record(event, &mut record).expect("encode raw telemetry event");
        if !chunks.last().expect("compression chunk").is_empty()
            && chunks.last().expect("compression chunk").len() + record.len()
                > COMPRESSION_CHUNK_BYTES
        {
            chunks.push(Vec::with_capacity(COMPRESSION_CHUNK_BYTES));
        }
        chunks
            .last_mut()
            .expect("compression chunk")
            .extend_from_slice(&record);
    }
    let raw_bytes = chunks.iter().map(Vec::len).sum();
    CompressionState {
        codec,
        chunks,
        raw_bytes,
    }
}

fn setup_payload_codec(codec: PayloadCodec) -> PayloadCodecState {
    let records = (0..PAYLOAD_CODEC_TOTAL)
        .map(|sequence| myelin_record(sequence as u64, sequence % ENGINE_CHANNELS))
        .collect::<Vec<_>>();
    let encoded = records
        .iter()
        .map(|record| encode_payload(codec, record))
        .collect();
    PayloadCodecState {
        codec,
        records,
        encoded,
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

fn execute_engine_concurrent(state: &mut EngineConcurrentState) -> TelemetryOutput {
    let (done_tx, done_rx) = mpsc::channel();
    for submissions in std::mem::take(&mut state.submissions) {
        let done_tx = done_tx.clone();
        let producer = state.endpoint.producer();
        state.handle.spawn(async move {
            let accepted: usize = submissions
                .into_iter()
                .map(|(channel, payload)| usize::from(producer.submit_bytes(channel, payload)))
                .sum();
            let _ = done_tx.send(accepted);
        });
    }
    drop(done_tx);
    let accepted = done_rx.iter().sum();
    let tick = state.endpoint.tick();
    let events = state
        .subscriptions
        .iter()
        .map(TelemetrySubscription::drain_available)
        .collect();
    TelemetryOutput::EngineConcurrent {
        accepted,
        drained: tick.drained,
        delivered: tick.delivered,
        events,
        dropped: state.endpoint.mux_dropped(),
    }
}

fn execute_wire_batch(state: &WireBatchState) -> TelemetryOutput {
    let mut encoded = Vec::with_capacity(state.payload_bytes);
    encode_event_batch(&state.events, &mut encoded).expect("encode telemetry subscription events");
    TelemetryOutput::WireBatch(encoded)
}

fn execute_payload_codec(
    state: &PayloadCodecState,
    codec: PayloadCodec,
    operation: PayloadCodecOperation,
) -> TelemetryOutput {
    match operation {
        PayloadCodecOperation::Encode => TelemetryOutput::PayloadEncoded(
            state
                .records
                .iter()
                .map(|record| encode_payload(codec, record))
                .collect(),
        ),
        PayloadCodecOperation::Decode => TelemetryOutput::PayloadDecoded(
            state
                .encoded
                .iter()
                .map(|payload| decode_payload(codec, payload))
                .collect(),
        ),
    }
}

fn execute_compression(state: &CompressionState) -> TelemetryOutput {
    let compressed = match state.codec {
        CompressionCodec::Lz4 => state
            .chunks
            .iter()
            .map(|raw| lz4_flex::block::compress(raw))
            .collect(),
        CompressionCodec::ZstdFast | CompressionCodec::Zstd => {
            let level = if matches!(state.codec, CompressionCodec::ZstdFast) {
                -5
            } else {
                1
            };
            let mut compressor =
                zstd::bulk::Compressor::new(level).expect("create benchmark zstd compressor");
            state
                .chunks
                .iter()
                .map(|raw| {
                    compressor
                        .compress(raw)
                        .expect("compress telemetry batch with zstd")
                })
                .collect()
        }
    };
    TelemetryOutput::Compressed(compressed)
}

pub fn representative_wire_sizes() -> (usize, usize) {
    let state = setup_wire_batch();
    let TelemetryOutput::WireBatch(encoded) = execute_wire_batch(&state) else {
        unreachable!("wire batch execution returns wire bytes");
    };
    (state.payload_bytes, encoded.len())
}

pub fn representative_wire_codec_sizes() -> [(&'static str, usize, usize); 3] {
    [
        PayloadCodec::Json,
        PayloadCodec::Cbor,
        PayloadCodec::MessagePack,
    ]
    .map(|codec| {
        let state = setup_wire_batch_with_codec(codec);
        let TelemetryOutput::WireBatch(encoded) = execute_wire_batch(&state) else {
            unreachable!("wire batch execution returns wire bytes");
        };
        (codec.label(), state.payload_bytes, encoded.len())
    })
}

pub fn representative_compression_sizes() -> [(&'static str, usize, usize); 3] {
    [
        CompressionCodec::Lz4,
        CompressionCodec::ZstdFast,
        CompressionCodec::Zstd,
    ]
    .map(|codec| {
        let state = setup_compression(codec);
        let TelemetryOutput::Compressed(compressed) = execute_compression(&state) else {
            unreachable!("compression execution returns bytes");
        };
        let compressed_bytes = compressed.iter().map(Vec::len).sum();
        (codec.label(), state.raw_bytes, compressed_bytes)
    })
}

pub fn representative_payload_codec_sizes() -> [(&'static str, usize); 3] {
    [
        PayloadCodec::Json,
        PayloadCodec::Cbor,
        PayloadCodec::MessagePack,
    ]
    .map(|codec| {
        let state = setup_payload_codec(codec);
        (
            codec.label(),
            state.encoded.iter().map(Vec::len).sum::<usize>(),
        )
    })
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
            Self::WireBatch => format!(
                "telemetry/wire/subscription-batch/channels-{ENGINE_CHANNELS}/frames-{ENGINE_TOTAL}"
            ),
            Self::Concurrent { producers } => {
                format!("telemetry/mux/concurrent/producers-{producers}/total-{CONCURRENT_TOTAL}")
            }
            Self::EngineConcurrent => format!(
                "telemetry/engine/multithread/channels-{ENGINE_CHANNELS}/producers-{ENGINE_PRODUCERS}/total-{ENGINE_TOTAL}"
            ),
            Self::PayloadCodec { codec, operation } => format!(
                "telemetry/payload-codec/{}/{}/records-{PAYLOAD_CODEC_TOTAL}",
                codec.label(),
                operation.label(),
            ),
            Self::Compression(codec) => format!(
                "telemetry/wire/compression/{}/frames-{ENGINE_TOTAL}",
                codec.label()
            ),
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
            Self::WireBatch => TelemetryState::WireBatch(setup_wire_batch()),
            Self::Concurrent { producers } => {
                TelemetryState::Concurrent(setup_concurrent(*producers))
            }
            Self::EngineConcurrent => TelemetryState::EngineConcurrent(setup_engine_concurrent()),
            Self::PayloadCodec { codec, .. } => {
                TelemetryState::PayloadCodec(setup_payload_codec(*codec))
            }
            Self::Compression(codec) => TelemetryState::Compression(setup_compression(*codec)),
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
            (Self::WireBatch, TelemetryState::WireBatch(state)) => execute_wire_batch(state),
            (Self::Concurrent { .. }, TelemetryState::Concurrent(state)) => {
                execute_concurrent(state)
            }
            (Self::EngineConcurrent, TelemetryState::EngineConcurrent(state)) => {
                execute_engine_concurrent(state)
            }
            (Self::PayloadCodec { codec, operation }, TelemetryState::PayloadCodec(state)) => {
                execute_payload_codec(state, *codec, *operation)
            }
            (Self::Compression(_), TelemetryState::Compression(state)) => {
                execute_compression(state)
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
            (TelemetryState::WireBatch(state), TelemetryOutput::WireBatch(encoded)) => {
                let decoded = decode_event_records(encoded, &state.descriptor)
                    .expect("decode telemetry subscription events");
                assert_eq!(decoded, state.events);
                assert!(encoded.len() < state.payload_bytes / 4);
            }
            (
                TelemetryState::EngineConcurrent(_),
                TelemetryOutput::EngineConcurrent {
                    accepted,
                    drained,
                    delivered,
                    events,
                    dropped,
                },
            ) => {
                assert_eq!(*accepted, ENGINE_TOTAL);
                assert_eq!(*drained, ENGINE_TOTAL);
                assert_eq!(*delivered, ENGINE_TOTAL * 2);
                assert_eq!(*dropped, 0);
                assert_eq!(events.len(), 2);
                assert_eq!(events[0], events[1]);
                assert_eq!(events[0].len(), ENGINE_TOTAL);
                let frames = events[0]
                    .iter()
                    .map(|event| match event {
                        TelemetryEvent::Frame(frame) => frame,
                        other => panic!("unexpected engine event: {other:?}"),
                    })
                    .collect::<Vec<_>>();
                let positions = frames
                    .iter()
                    .map(|frame| frame.position.0)
                    .collect::<Vec<_>>();
                let payloads = frames
                    .iter()
                    .map(|frame| frame.payload.as_slice())
                    .collect::<HashSet<_>>();
                let channel_ids = frames
                    .iter()
                    .map(|frame| frame.channel.channel)
                    .collect::<HashSet<_>>();
                assert!(contiguous(&positions));
                assert_eq!(payloads.len(), ENGINE_TOTAL);
                assert_eq!(channel_ids.len(), ENGINE_CHANNELS);
                assert!(frames.iter().all(|frame| {
                    decode_payload(PayloadCodec::MessagePack, &frame.payload).producer_sequence
                        < ENGINE_TOTAL as u64
                }));
            }
            (TelemetryState::PayloadCodec(state), TelemetryOutput::PayloadEncoded(encoded)) => {
                assert_eq!(encoded.len(), state.records.len());
                let decoded = encoded
                    .iter()
                    .map(|payload| decode_payload(state.codec, payload))
                    .collect::<Vec<_>>();
                assert_eq!(decoded, state.records);
            }
            (TelemetryState::PayloadCodec(state), TelemetryOutput::PayloadDecoded(decoded)) => {
                assert_eq!(decoded, &state.records)
            }
            (TelemetryState::Compression(state), TelemetryOutput::Compressed(compressed)) => {
                assert_eq!(compressed.len(), state.chunks.len());
                for (compressed, raw) in compressed.iter().zip(&state.chunks) {
                    let decoded = match state.codec {
                        CompressionCodec::Lz4 => lz4_flex::block::decompress(compressed, raw.len())
                            .expect("decompress benchmark LZ4"),
                        CompressionCodec::ZstdFast | CompressionCodec::Zstd => {
                            zstd::bulk::decompress(compressed, raw.len())
                                .expect("decompress benchmark zstd")
                        }
                    };
                    assert_eq!(&decoded, raw);
                }
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
            Self::WireBatch => WorkUnits::Frames(ENGINE_TOTAL as u64),
            Self::Concurrent { .. } => WorkUnits::Operations(CONCURRENT_TOTAL as u64),
            Self::EngineConcurrent => WorkUnits::Frames(ENGINE_TOTAL as u64),
            Self::PayloadCodec { .. } => WorkUnits::Frames(PAYLOAD_CODEC_TOTAL as u64),
            Self::Compression(_) => {
                WorkUnits::Bytes(setup_compression(CompressionCodec::Lz4).raw_bytes as u64)
            }
        }
    }

    fn setup_policy(&self) -> SetupPolicy {
        if matches!(self, Self::Concurrent { .. } | Self::EngineConcurrent) {
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
    workloads.push(TelemetryWorkload::WireBatch);

    for codec in [
        PayloadCodec::Json,
        PayloadCodec::Cbor,
        PayloadCodec::MessagePack,
    ] {
        for operation in [PayloadCodecOperation::Encode, PayloadCodecOperation::Decode] {
            workloads.push(TelemetryWorkload::PayloadCodec { codec, operation });
        }
    }
    workloads.push(TelemetryWorkload::Compression(CompressionCodec::Lz4));
    workloads.push(TelemetryWorkload::Compression(CompressionCodec::ZstdFast));
    workloads.push(TelemetryWorkload::Compression(CompressionCodec::Zstd));

    for producers in [1, 2, 4, 8] {
        workloads.push(TelemetryWorkload::Concurrent { producers });
    }
    workloads.push(TelemetryWorkload::EngineConcurrent);
    workloads
}
