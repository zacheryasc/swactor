use std::sync::Arc;
use std::thread;

use telemetry::frame::Frame;
use telemetry::ingest::Consumer;
use telemetry::mux::Mux;
use telemetry::transport::{Delivery, Reorder, ScriptedTransport, StreamScript};
use telemetry::views::{self, Body, LogEntry};
use telemetry::wire::{decode_delivery, encode_delivery};
use telemetry::{ChannelId, ChannelKind, ChannelRegistry, Lifetime, NodeId, Position, Record, StreamId};
use serde::{Deserialize, Serialize};

const RESOURCE_CHANNEL: ChannelId = ChannelId(1);
const LOG_CHANNEL: ChannelId = ChannelId(2);
const MEMBERSHIP_CHANNEL: ChannelId = ChannelId(3);
const OPAQUE_CHANNEL: ChannelId = ChannelId(99);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ResourceSample {
    cpu_pct: f32,
    mem_mb: u32,
}

impl Record for ResourceSample {
    const CHANNEL: &'static str = "host.resource";
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MembershipTransition {
    peer: String,
    from: String,
    to: String,
}

impl Record for MembershipTransition {
    const CHANNEL: &'static str = "membership";
}

fn stream() -> StreamId {
    StreamId::new(NodeId::new("node-alpha"), Lifetime(1))
}

fn resource(tick: u32) -> ResourceSample {
    ResourceSample {
        cpu_pct: 12.5 + tick as f32,
        mem_mb: 1024 + tick,
    }
}

#[test]
fn record_codecs_round_trip_without_global_catalog() {
    let sample = resource(7);
    let decoded = ResourceSample::decode(&sample.encode()).expect("resource decodes");
    assert_eq!(decoded, sample);
    assert_eq!(ResourceSample::channel_name(), "host.resource");
}

#[test]
fn mux_assigns_positions_when_drained() {
    let mux = Mux::unbounded(stream());

    for i in 0..64u64 {
        assert!(mux.submit(RESOURCE_CHANNEL, resource(i as u32).encode()));
    }

    assert_eq!(mux.assigned(), 0);
    assert_eq!(mux.dropped(), 0);
    let mut positions: Vec<u64> = mux.drain().iter().map(|frame| frame.position.0).collect();
    positions.sort_unstable();
    assert_eq!(positions, (0..64).collect::<Vec<_>>());
    assert_eq!(mux.assigned(), 64);
}

#[test]
fn mux_concurrent_producers_assign_unique_positions_on_drain() {
    let mux = Arc::new(Mux::unbounded(stream()));

    let mut threads = Vec::new();
    for producer in 0..4u8 {
        let mux = Arc::clone(&mux);
        threads.push(thread::spawn(move || {
            let mut accepted = 0;
            for seq in 0..32u8 {
                if mux.submit(LOG_CHANNEL, vec![producer, seq]) {
                    accepted += 1;
                }
            }
            accepted
        }));
    }
    let accepted: usize = threads
        .into_iter()
        .map(|thread| thread.join().expect("producer thread completes"))
        .sum();

    assert_eq!(accepted, 128);
    assert_eq!(mux.assigned(), 0);
    let frames = mux.drain();
    assert_eq!(frames.len(), 128);
    let mut positions: Vec<u64> = frames.iter().map(|frame| frame.position.0).collect();
    positions.sort_unstable();
    assert_eq!(positions, (0..128).collect::<Vec<_>>());
    assert_eq!(mux.assigned(), 128);
}

#[test]
fn mux_full_queue_drops_without_consuming_position() {
    let mux = Mux::new(stream(), 1);

    assert!(mux.submit(LOG_CHANNEL, b"first".to_vec()));
    assert!(!mux.submit(LOG_CHANNEL, b"second".to_vec()));
    assert_eq!(mux.dropped(), 1);
    assert_eq!(mux.assigned(), 0);

    let frames = mux.drain();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].channel, LOG_CHANNEL);
    assert_eq!(frames[0].position, Position(0));
    assert_eq!(frames[0].payload, b"first");
    assert_eq!(mux.assigned(), 1);
}

#[test]
fn mux_one_submit_one_drained_frame_without_timing_sidecar() {
    let mux = Mux::unbounded(stream());

    assert!(mux.submit(RESOURCE_CHANNEL, resource(0).encode()));
    let frames = mux.drain();

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].channel, RESOURCE_CHANNEL);
    assert_eq!(frames[0].position, Position(0));
    assert_eq!(frames[0].payload, resource(0).encode());
}

#[test]
fn wire_envelope_round_trips_numeric_channel_payload_and_position() {
    let stream = StreamId::new(NodeId::new("node-ünïcode-Ω"), Lifetime(7));
    let cases = vec![
        Frame::new(RESOURCE_CHANNEL, Position(0), resource(2).encode()),
        Frame::new(LOG_CHANNEL, Position(1), b"loss=0.0312 lr=3e-4".to_vec()),
        Frame::new(OPAQUE_CHANNEL, Position(2), vec![0, 255, 1, 254, 128]),
    ];

    for frame in &cases {
        let bytes = encode_delivery(&stream, frame);
        let (out_stream, out_frame) = decode_delivery(&bytes).expect("decodes");
        assert_eq!(out_stream, stream);
        assert_eq!(&out_frame, frame);
    }
}

#[test]
fn scripted_transport_and_ingest_preserve_delivered_frames_and_surface_gaps() {
    let stream = stream();
    let sent = vec![
        Frame::new(RESOURCE_CHANNEL, Position(0), resource(0).encode()),
        Frame::new(LOG_CHANNEL, Position(1), b"hello".to_vec()),
        Frame::new(MEMBERSHIP_CHANNEL, Position(2), b"peer moved".to_vec()),
        Frame::new(LOG_CHANNEL, Position(3), b"bye".to_vec()),
    ];
    let deliveries = ScriptedTransport::carry(
        &stream,
        &sent,
        &StreamScript::dropping([1]).with_reorder(Reorder::Reversed),
    );
    let mut consumer = Consumer::new();
    consumer.ingest(deliveries);
    let stored = consumer.store().stream(&stream).expect("stored stream");

    assert_eq!(
        stored.to_vec(),
        vec![sent[0].clone(), sent[2].clone(), sent[3].clone()]
    );
    assert_eq!(stored.gap_spans()[0].start, 1);
    assert_eq!(stored.gap_spans()[0].end, 1);
}

#[test]
fn caller_registry_decodes_by_channel_name_and_unknowns_degrade_to_raw() {
    let registry = ChannelRegistry::new()
        .with_record::<ResourceSample>()
        .with_text_prefix("proc.");
    let sample = resource(3);

    match views::decode_body_with(ResourceSample::channel_name(), &sample.encode(), &registry) {
        Body::Record(value) => assert_eq!(value["mem_mb"], 1027),
        other => panic!("resource should decode as record, got {other:?}"),
    }
    match views::decode_body_with("proc.trainer.stdout", b"line", &registry) {
        Body::Text(text) => assert_eq!(text, "line"),
        other => panic!("text should decode, got {other:?}"),
    }
    assert_eq!(
        registry.classify_name("v2.gpu.thermals"),
        ChannelKind::Opaque
    );
    assert_eq!(
        views::decode_body(&OPAQUE_CHANNEL, &[1, 2, 3]),
        Body::Raw(vec![1, 2, 3])
    );
}

#[test]
fn metric_series_on_decodes_only_the_requested_numeric_channel() {
    let stream = stream();
    let mut consumer = Consumer::new();
    consumer.ingest([
        Delivery::new(
            stream.clone(),
            Frame::new(RESOURCE_CHANNEL, Position(0), resource(0).encode()),
        ),
        Delivery::new(
            stream.clone(),
            Frame::new(LOG_CHANNEL, Position(1), b"log".to_vec()),
        ),
        Delivery::new(
            stream.clone(),
            Frame::new(RESOURCE_CHANNEL, Position(2), resource(2).encode()),
        ),
    ]);
    let stored = consumer.store().stream(&stream).expect("stored stream");

    let series = views::metric_series_on::<ResourceSample>(stored, RESOURCE_CHANNEL);
    assert_eq!(
        series,
        vec![(Position(0), resource(0)), (Position(2), resource(2))]
    );
}

#[test]
fn merged_log_with_resolver_decodes_known_names_and_keeps_gaps() {
    let stream = stream();
    let mut consumer = Consumer::new();
    consumer.ingest([
        Delivery::new(
            stream.clone(),
            Frame::new(RESOURCE_CHANNEL, Position(0), resource(0).encode()),
        ),
        Delivery::new(
            stream.clone(),
            Frame::new(LOG_CHANNEL, Position(2), b"hello".to_vec()),
        ),
    ]);
    let stored = consumer.store().stream(&stream).expect("stored stream");
    let registry = ChannelRegistry::new()
        .with_record::<ResourceSample>()
        .with_text_channel("proc.trainer.stdout");

    let entries = views::merged_log_with_names(stored, &registry, |channel| match channel {
        RESOURCE_CHANNEL => Some(ResourceSample::channel_name().to_owned()),
        LOG_CHANNEL => Some("proc.trainer.stdout".to_owned()),
        _ => None,
    });

    assert!(matches!(entries[0], LogEntry::Frame(_)));
    assert!(matches!(entries[1], LogEntry::Gap(_)));
    match &entries[2] {
        LogEntry::Frame(frame) => assert_eq!(frame.body, Body::Text("hello".to_owned())),
        other => panic!("expected frame, got {other:?}"),
    }
}
