//! Datastream verification harness (see `DATASTREAM_TESTING_SPEC.md`).
//!
//! The tests climb the ladder of testing spec §4: Kind I verified vectors
//! (one rule), Kind II seam tests (one boundary), Kind III the full mock
//! (the assembled pipe), Kind IV a deployment simulation. Every expected
//! answer is derived from the reference model in [`support::reference`],
//! never captured from the system under test (testing spec §3). The real-
//! I/O checks of testing spec §9–§10 live in `t_datastream_realio.rs`.

#[path = "datastream_support/mod.rs"]
mod support;

use serde::{Deserialize, Serialize};
use std::sync::Arc;

use datastream::frame::{ChannelId, Frame, Lifetime, NodeId, Position, StreamId};
use datastream::ingest::Consumer;
use datastream::mux::Mux;
use datastream::store::{GapSpan, StoredStream};
use datastream::transport::{Delivery, Reorder, ScriptedTransport, StreamScript};
use datastream::views::{self, Body, LogEntry};
use datastream::wire::{WireError, decode_delivery, encode_delivery};
use datastream::{ChannelKind, ChannelRegistry, Record};

use support::reference::TimelineItem;
use support::schema::{
    self as catalog, ActorRuntimeDetail, DatastreamHealth, DistributionState, IdentityRecord,
    ProcStream, ResourceSample,
};
use support::{Node, payloads, reference};

/// The frames of `sent` that survive dropping `dropped`, in send order —
/// the scenario's delivered set, derived without running the pipe.
fn surviving(sent: &[Frame], dropped: &[u64]) -> Vec<Frame> {
    let drop: std::collections::BTreeSet<u64> = dropped.iter().copied().collect();
    sent.iter()
        .filter(|f| !drop.contains(&f.position.0))
        .cloned()
        .collect()
}

/// A node that has emitted a realistic spread of channels: identity, two
/// resource samples, a transport snapshot, a membership transition, runtime
/// stats, two lines of process output, and a worker-counters record.
fn busy_node(stream: &StreamId) -> Vec<Frame> {
    let node = Node::new(stream.clone());
    node.emit(&payloads::identity(stream.node.as_str(), stream.life.0)); // 0
    node.emit(&payloads::resource(0)); // 1
    node.emit_text(
        "trainer",
        ProcStream::Stdout,
        &payloads::log_line("trainer", 0),
    ); // 2
    node.emit(&payloads::transport(1)); // 3
    node.emit(&payloads::membership("node-beta", "alive", "suspect")); // 4
    node.emit(&payloads::resource(1)); // 5
    node.emit(&payloads::runtime(2)); // 6
    node.emit_text("trainer", ProcStream::Stderr, "WARN cuda oom, retrying"); // 7
    node.emit(&payloads::worker_counters(8)); // 8
    node.emit(&payloads::resource(2)); // 9
    node.sent()
}

/// Carry a node's sent frames through the scripted transport and ingest the
/// result, returning the consumer (whose store the views read).
fn consume(stream: &StreamId, sent: &[Frame], script: StreamScript) -> Consumer {
    let delivered = ScriptedTransport::carry(stream, sent, &script);
    let mut consumer = Consumer::new();
    consumer.ingest(delivered);
    consumer
}

/// Project a view's merged log down to its structural skeleton — order and
/// surfaced gaps — so it can be compared to the reference oracle.
fn structure(entries: &[LogEntry]) -> Vec<TimelineItem> {
    entries
        .iter()
        .map(|e| match e {
            LogEntry::Frame(mf) => TimelineItem::Frame {
                position: mf.position.0,
                channel: mf.channel.to_string(),
            },
            LogEntry::Gap(span) => TimelineItem::Gap {
                start: span.start,
                end: span.end,
            },
        })
        .collect()
}

fn test_stream() -> StreamId {
    StreamId::new(NodeId::new("node-alpha"), Lifetime(1))
}

fn test_registry() -> ChannelRegistry {
    ChannelRegistry::new()
        .with_record::<IdentityRecord>()
        .with_record::<ResourceSample>()
        .with_record::<support::schema::TransportInternals>()
        .with_record::<support::schema::MembershipTransition>()
        .with_record::<support::schema::RuntimeStats>()
        .with_record::<DistributionState>()
        .with_record::<ActorRuntimeDetail>()
        .with_record::<support::schema::WorkerCounters>()
        .with_record::<DatastreamHealth>()
        .with_text_prefix("proc.")
}

// ── Kind I — verified vectors (testing spec §5) ────────────────────────
//
// Each targets a single rule in isolation and asserts a *relationship*
// (round-trip, byte-equality) rather than a captured literal, so the
// vector cannot be authored wrong and survives a refactor.

/// The codec round-trips: decoding what was encoded yields the original
/// record (testing spec §3, §5), for every typed channel in the catalog
/// (spec §6.1), on realistic payloads.
#[test]
fn codec_round_trips_every_typed_channel() {
    assert_round_trip(&payloads::identity("node-alpha", 1));
    assert_round_trip(&payloads::resource(3));
    assert_round_trip(&payloads::transport(4));
    assert_round_trip(&payloads::membership("node-beta", "alive", "suspect"));
    assert_round_trip(&payloads::runtime(5));
    assert_round_trip(&payloads::dist_state(6));
    assert_round_trip(&payloads::actor_detail(8));
    assert_round_trip(&payloads::worker_counters(9));
    assert_round_trip(&payloads::datastream_health(10));
}

fn assert_round_trip<R: Record + PartialEq + std::fmt::Debug>(record: &R) {
    let decoded = R::decode(&record.encode()).expect("round-trips");
    assert_eq!(&decoded, record, "decode(encode(r)) must equal r");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ExternalPluginRecord {
    #[serde(default)]
    value: u64,
    #[serde(default)]
    label: String,
}

impl Record for ExternalPluginRecord {
    const CHANNEL: &'static str = "external.plugin.sample";
}

#[test]
fn external_record_full_pipe_round_trips_without_datastream_catalog() {
    let stream = test_stream();
    let mux = Mux::unbounded(stream.clone());
    let record = ExternalPluginRecord {
        value: 42,
        label: "owned outside datastream".into(),
    };
    let pos = mux.submit(ExternalPluginRecord::channel(), record.encode());
    let sent = mux.drain();

    let consumer = consume(&stream, &sent, StreamScript::perfect());
    let stored = consumer.store().stream(&stream).expect("stream stored");
    let series = views::metric_series::<ExternalPluginRecord>(stored);

    assert_eq!(pos, Position(0));
    assert_eq!(series, vec![(Position(0), record.clone())]);

    let registry = ChannelRegistry::new().with_record::<ExternalPluginRecord>();
    let body = views::decode_body_with(
        &ExternalPluginRecord::channel(),
        &record.encode(),
        &registry,
    );
    match body {
        Body::Record(value) => assert_eq!(value["value"], 42),
        other => panic!("caller registry should decode external record, got {other:?}"),
    }
}

/// A typed channel's codec tolerates version skew (spec §6.3): a record
/// written by a newer producer (extra field) and one written by an older
/// producer (missing field) both still decode.
#[test]
fn typed_codec_tolerates_version_skew() {
    // Newer producer: an extra, unknown field. The consumer ignores it.
    let with_extra = br#"{"node":"node-x","role":"worker","region":"eu-west-1",
        "life":2,"future_field":{"nested":true}}"#;
    let decoded = IdentityRecord::decode(with_extra).expect("unknown field ignored");
    assert_eq!(decoded.node, "node-x");
    assert_eq!(decoded.life, 2);

    // Older producer: a sample that predates several fields. The missing
    // ones decode to their defaults rather than failing.
    let sparse = br#"{"cpu_pct":42.0}"#;
    let decoded = ResourceSample::decode(sparse).expect("missing fields default");
    assert_eq!(decoded.cpu_pct, 42.0);
    assert_eq!(decoded.mem_used_mb, 0);
    assert_eq!(decoded.net_tx_kbps, 0);

    // A consolidated record evolves the same way: an unknown panel is ignored,
    // and a record predating the per-entry vectors decodes them as empty — so a
    // newer fleet view and an older node stay compatible (spec §6.3).
    let dist_skew = br#"{"cache_size":4,"unknown_panel":[1,2,3]}"#;
    let decoded = DistributionState::decode(dist_skew).expect("dist.state tolerates skew");
    assert_eq!(decoded.cache_size, 4);
    assert!(decoded.registry_entries.is_empty());
    assert!(decoded.recent_probe_targets.is_empty());
    assert_eq!(decoded.directory_route_count, 0);
}

/// The transport envelope round-trips exactly (testing spec §9): a decoded
/// delivery equals the one encoded — payload bytes byte-identical, channel
/// and position intact — across realistic, unicode, empty, and binary
/// payloads.
#[test]
fn wire_envelope_round_trips() {
    let stream = StreamId::new(NodeId::new("node-ünïcode-Ω"), Lifetime(7));
    let cases = vec![
        support::typed_frame(&payloads::resource(2), 0),
        support::text_frame("trainer", ProcStream::Stdout, "loss=0.0312 lr=3e-4", 1),
        // Empty payload — a real edge (a channel may emit a zero-length span).
        Frame::new(
            ChannelId::new("proc.trainer.stderr"),
            Position(2),
            Vec::new(),
        ),
        // Binary payload on an opaque channel — bytes the consumer cannot read.
        support::opaque_frame("sensor.raw", &[0u8, 255, 1, 254, 128, 0, 0, 7], 3),
        // Unicode channel id and payload.
        Frame::new(
            ChannelId::new("proc.café.stdout"),
            Position(4),
            "café ☕".as_bytes().to_vec(),
        ),
    ];

    for frame in &cases {
        let bytes = encode_delivery(&stream, frame);
        let (out_stream, out_frame) = decode_delivery(&bytes).expect("decodes");
        assert_eq!(out_stream, stream, "stream id intact");
        assert_eq!(
            &out_frame, frame,
            "frame intact (channel, position, payload bytes)"
        );
        assert_eq!(out_frame.payload, frame.payload, "payload byte-identical");
    }
}

/// The cluster transport wraps each delivery in a `DatastreamFrame` whose codec
/// is the identity — the on-wire bytes are the delivery envelope verbatim. A node
/// ships these to the orchestrator's `datastream-sink`, so the codec must neither
/// inflate nor mangle the payload: encode∘decode is the identity, and a real
/// delivery survives the round-trip back to the same `(stream, frame)`.
#[test]
fn datastream_frame_codec_is_identity_and_carries_a_delivery() {
    use datastream::wire::{DatastreamFrame, DatastreamFrameCodec};
    use swactor_transport::Codec;

    let stream = StreamId::new(NodeId::new("pp-stage-2"), Lifetime(42));
    let frame = support::text_frame("pp-worker", ProcStream::Stdout, "stage 2 ready", 0);

    // A node builds the cluster message from the same bytes the raw path uses.
    let msg = DatastreamFrame {
        payload: encode_delivery(&stream, &frame),
    };

    let codec = DatastreamFrameCodec;
    let wire = codec.encode(&msg).expect("encode");
    assert_eq!(
        wire, msg.payload,
        "codec must not inflate the envelope bytes"
    );
    let back = codec.decode(&wire).expect("decode");
    assert_eq!(
        back.payload, msg.payload,
        "codec round-trips byte-identical"
    );

    // The carried delivery decodes back to exactly what was shipped.
    let (out_stream, out_frame) = decode_delivery(&back.payload).expect("delivery decodes");
    assert_eq!(out_stream, stream);
    assert_eq!(out_frame, frame);
}

/// A best-effort carrier can hand the consumer anything; a truncated or
/// malformed envelope must fail gracefully (a `WireError`), never panic.
#[test]
fn wire_envelope_rejects_malformed_buffers_without_panicking() {
    let stream = StreamId::new(NodeId::new("node-a"), Lifetime(1));
    let frame = support::typed_frame(&payloads::resource(1), 9);
    let good = encode_delivery(&stream, &frame);

    // Every proper prefix is incomplete and must be rejected as an error.
    for cut in 0..good.len() {
        match decode_delivery(&good[..cut]) {
            Err(_) => {}
            Ok(_) => panic!("truncated buffer of {cut} bytes decoded as if whole"),
        }
    }
    // The full buffer still decodes.
    assert!(decode_delivery(&good).is_ok());

    // A length prefix that overruns the buffer is a clean error.
    let mut lying = Vec::new();
    lying.extend_from_slice(&u32::MAX.to_le_bytes()); // claims 4 GiB of node id
    assert_eq!(decode_delivery(&lying), Err(WireError::Truncated));

    // One datagram carries exactly one frame: trailing bytes are rejected,
    // so a malformed concatenation cannot be silently half-read.
    let mut trailing = good.clone();
    trailing.push(0xFF);
    assert_eq!(decode_delivery(&trailing), Err(WireError::TrailingBytes));
}

/// Opaque retention is byte-exact (spec §6.3, §8.3): a channel the
/// consumer does not recognize classifies as opaque, and its bytes survive
/// the envelope unchanged — the relationship "stored bytes equal submitted
/// bytes" (testing spec §5).
#[test]
fn opaque_channel_classifies_and_preserves_bytes() {
    let id = ChannelId::new("v2.gpu.thermals"); // not in the caller registry
    assert_eq!(test_registry().classify_channel(&id), ChannelKind::Opaque);

    let payload: Vec<u8> = (0u8..=255).cycle().take(1000).collect();
    let frame = Frame::new(id, Position(42), payload.clone());
    let stream = StreamId::new(NodeId::new("node-z"), Lifetime(3));

    let (_, out) = decode_delivery(&encode_delivery(&stream, &frame)).expect("decodes");
    assert_eq!(
        out.payload, payload,
        "opaque bytes retained whole, byte-identical"
    );
}

/// A caller-owned registry classifies known channels correctly and treats the
/// process-output family (spec §6.2) as text without enumerating labels.
#[test]
fn caller_registry_classifies_known_and_text_family() {
    let registry = test_registry();
    assert_eq!(
        registry.classify_channel(&ChannelId::new(catalog::IDENTITY)),
        ChannelKind::Typed
    );
    assert_eq!(
        registry.classify_channel(&ChannelId::new(catalog::HOST_RESOURCE)),
        ChannelKind::Typed
    );
    // The consolidated records are typed channels too.
    assert_eq!(
        registry.classify_channel(&ChannelId::new(catalog::DIST_STATE)),
        ChannelKind::Typed
    );
    assert_eq!(
        registry.classify_channel(&ChannelId::new(catalog::RUNTIME_ACTORS)),
        ChannelKind::Typed
    );
    // A process introduced at runtime gets text channels for free.
    let out = catalog::process_output("inference-server", ProcStream::Stdout);
    assert_eq!(out.as_str(), "proc.inference-server.stdout");
    assert_eq!(registry.classify_channel(&out), ChannelKind::Text);
}

/// A record carries its own channel (spec §6.1 fixed identity), and that
/// channel classifies as typed.
#[test]
fn records_name_their_own_typed_channel() {
    assert_eq!(IdentityRecord::channel().as_str(), catalog::IDENTITY);
    assert_eq!(ResourceSample::channel().as_str(), catalog::HOST_RESOURCE);
    assert_eq!(
        test_registry().classify_channel(&IdentityRecord::channel()),
        ChannelKind::Typed
    );
    // An identity record round-trips through its typed codec (spec §6.1).
    let r = payloads::identity("n", 7);
    assert_eq!(IdentityRecord::decode(&r.encode()).unwrap(), r);
}

// ── The mux: position authority (spec §5) ──────────────────────────────

/// Kind I (testing spec §5) — position assignment is monotonic and
/// gap-free (spec §5.2): with no overflow, K submissions number `0..K`
/// exactly, and `submit` returns each position in order.
#[test]
fn mux_numbers_monotonic_and_gap_free() {
    let mux = Mux::unbounded(test_stream());
    let k = 64u64;
    for i in 0..k {
        let pos = mux.submit(catalog::HOST_RESOURCE, payloads::resource(i).encode());
        assert_eq!(
            pos,
            Position(i),
            "submit returns the next position, in order"
        );
    }
    assert_eq!(
        mux.assigned(),
        k,
        "assigned == number of submissions (never skips)"
    );
    assert_eq!(mux.dropped(), 0, "no overflow, nothing dropped");

    let positions: Vec<u64> = mux.drain().iter().map(|f| f.position.0).collect();
    assert_eq!(
        positions,
        (0..k).collect::<Vec<_>>(),
        "emitted positions are 0..k, gap-free"
    );
}

/// Kind II (testing spec §6) — across the mux seam, every submission
/// appears as exactly one frame, byte-identical, in one interleaved order.
/// A typed event and a log line share the single timeline (spec §5.1).
#[test]
fn mux_seam_preserves_every_submission_byte_identical() {
    let mux = Mux::unbounded(test_stream());

    // A realistic interleaving of typed records and raw process output —
    // the same kind of thing on one stream (spec §4.2).
    let submissions: Vec<(ChannelId, Vec<u8>)> = vec![
        (
            catalog::IDENTITY.into(),
            payloads::identity("node-alpha", 1).encode(),
        ),
        (
            catalog::process_output("trainer", ProcStream::Stdout),
            payloads::log_line("trainer", 0).into_bytes(),
        ),
        (
            catalog::HOST_RESOURCE.into(),
            payloads::resource(1).encode(),
        ),
        (
            catalog::MEMBERSHIP.into(),
            payloads::membership("node-beta", "alive", "suspect").encode(),
        ),
        (
            catalog::process_output("trainer", ProcStream::Stderr),
            b"WARN cuda oom, retrying".to_vec(),
        ),
        (catalog::RUNTIME_STATS.into(), payloads::runtime(2).encode()),
    ];

    for (channel, payload) in &submissions {
        mux.submit(channel.clone(), payload.clone());
    }

    let frames = mux.drain();
    assert_eq!(
        frames.len(),
        submissions.len(),
        "exactly one frame per submission — none lost"
    );
    for (i, (frame, (channel, payload))) in frames.iter().zip(&submissions).enumerate() {
        assert_eq!(
            frame.position,
            Position(i as u64),
            "interleaved in submission order, gap-free"
        );
        assert_eq!(
            &frame.channel, channel,
            "channel tag preserved crossing the seam"
        );
        assert_eq!(
            &frame.payload, payload,
            "payload bytes byte-identical crossing the seam"
        );
    }
}

/// Spec §5.3 — on overflow the mux drops the frame, but its position is
/// already spent, so the loss surfaces as a missing position (a detectable
/// gap), never a silent renumber. The reference-model gap oracle confirms
/// the interior gap.
#[test]
fn mux_overflow_drops_surface_as_a_gap_not_a_renumber() {
    let mux = Mux::new(test_stream(), 2); // tiny buffer

    mux.submit(catalog::HOST_RESOURCE, payloads::resource(0).encode()); // pos 0 -> buffered
    mux.submit(catalog::HOST_RESOURCE, payloads::resource(1).encode()); // pos 1 -> buffered
    mux.submit(catalog::HOST_RESOURCE, payloads::resource(2).encode()); // pos 2 -> OVERFLOW, dropped
    let first = mux.drain(); // empties the buffer
    mux.submit(catalog::HOST_RESOURCE, payloads::resource(3).encode()); // pos 3 -> buffered
    let second = mux.drain();

    assert_eq!(
        mux.assigned(),
        4,
        "every submission consumed a position — numbering never skips"
    );
    assert_eq!(
        mux.dropped(),
        1,
        "exactly the overflowing frame was dropped"
    );

    let mut emitted: Vec<Frame> = first;
    emitted.extend(second);
    let positions: Vec<u64> = emitted.iter().map(|f| f.position.0).collect();
    assert_eq!(
        positions,
        vec![0, 1, 3],
        "dropped position 2 is simply absent, others not renumbered"
    );

    // The dropped position reads as an interior gap, exactly what the
    // consumer will later surface (spec §7.5).
    assert_eq!(
        reference::gap_spans(&emitted),
        vec![GapSpan { start: 2, end: 2 }],
        "the drop is a detectable gap at position 2"
    );
}

/// Spec §5.3 — the mux serializes concurrent producers into one order:
/// every position is assigned exactly once across threads, with no
/// duplicate and no gap. This is the single-ordering-authority guarantee.
#[test]
fn mux_serializes_concurrent_producers_without_collision() {
    let mux = Arc::new(Mux::unbounded(test_stream()));
    let threads = 8u64;
    let per_thread = 500u64;

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let mux = Arc::clone(&mux);
            std::thread::spawn(move || {
                let mut mine = Vec::with_capacity(per_thread as usize);
                for i in 0..per_thread {
                    // Each thread is a distinct producer writing real bytes.
                    let pos = mux.submit(
                        catalog::RUNTIME_STATS,
                        payloads::runtime(t * 1000 + i).encode(),
                    );
                    mine.push(pos.0);
                }
                mine
            })
        })
        .collect();

    let mut assigned: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    let total = threads * per_thread;
    assert_eq!(mux.assigned(), total);
    assert_eq!(mux.dropped(), 0, "unbounded mux drops nothing");

    assigned.sort_unstable();
    assert_eq!(
        assigned,
        (0..total).collect::<Vec<_>>(),
        "each position assigned exactly once"
    );

    // The buffered frames carry the same complete set of positions.
    let mut emitted: Vec<u64> = mux.drain().iter().map(|f| f.position.0).collect();
    emitted.sort_unstable();
    assert_eq!(
        emitted,
        (0..total).collect::<Vec<_>>(),
        "no frame lost, no position duplicated"
    );
}

// ── Transport seam + ingest + store (spec §7, §8) ──────────────────────

/// Build a realistic single-node stream the way a node would: drive the
/// real mux with a mix of typed records, raw process output, and a channel
/// the consumer does not know, then take its output.
fn realistic_stream(stream: &StreamId) -> Vec<Frame> {
    let mux = Mux::unbounded(stream.clone());
    mux.submit(
        catalog::IDENTITY,
        payloads::identity(stream.node.as_str(), stream.life.0).encode(),
    );
    mux.submit(catalog::HOST_RESOURCE, payloads::resource(0).encode());
    // A channel this consumer cannot decode — must still be retained whole.
    mux.submit(
        ChannelId::new("v2.gpu.thermals"),
        vec![0xDE, 0xAD, 0xBE, 0xEF],
    );
    mux.submit(
        catalog::process_output("trainer", ProcStream::Stdout),
        payloads::log_line("trainer", 0).into_bytes(),
    );
    mux.submit(catalog::HOST_RESOURCE, payloads::resource(1).encode());
    mux.submit(
        catalog::MEMBERSHIP,
        payloads::membership("node-beta", "alive", "suspect").encode(),
    );
    mux.drain()
}

/// Kind II (testing spec §6) — across ingest, every delivered frame appears
/// in the stored stream byte-identical, an undecodable channel is retained
/// whole, and the gap left by a dropped position is surfaced. Both sides of
/// the seam are checked against the reference model.
#[test]
fn ingest_seam_stores_every_delivered_frame_and_surfaces_gaps() {
    let stream = test_stream();
    let sent = realistic_stream(&stream); // positions 0..6

    // The carrier drops one resource sample (interior loss) and scrambles
    // arrival order within windows — both inside the envelope (spec §9).
    let script = StreamScript::dropping([4]).with_reorder(Reorder::Windows(3));
    let delivered = ScriptedTransport::carry(&stream, &sent, &script);

    let mut consumer = Consumer::new();
    consumer.ingest(delivered.clone());
    let stored = consumer.store().stream(&stream).expect("stream stored");

    // Derive the trusted answer from the reference model, never from the run.
    let delivered_frames = reference::delivered_frames(&stream, &delivered);
    assert_eq!(
        stored.to_vec(),
        reference::reconstruct(&delivered_frames),
        "stored stream equals the reference reconstruction (every delivered frame, in position order)"
    );
    assert_eq!(
        stored.gap_spans(),
        reference::gap_spans(&delivered_frames),
        "the dropped position is surfaced as a gap"
    );
    assert_eq!(
        stored.gap_spans(),
        vec![GapSpan { start: 4, end: 4 }],
        "specifically position 4 is missing"
    );

    // The undecodable channel landed whole, byte-identical (spec §8.3).
    let opaque = stored.at(Position(2)).expect("opaque frame retained");
    assert_eq!(opaque.channel, ChannelId::new("v2.gpu.thermals"));
    assert_eq!(
        opaque.payload,
        vec![0xDE, 0xAD, 0xBE, 0xEF],
        "opaque bytes retained whole"
    );
    assert_eq!(
        test_registry().classify_channel(&opaque.channel),
        ChannelKind::Opaque
    );
}

/// Spec §7.5 — the consumer reconstructs by position, not by arrival: even
/// when the carrier delivers a stream fully reversed, the stored stream is
/// in position order and identical to the in-order case.
#[test]
fn ingest_reconstructs_by_position_not_arrival_order() {
    let stream = test_stream();
    let sent = realistic_stream(&stream);

    let reversed = ScriptedTransport::carry(
        &stream,
        &sent,
        &StreamScript::perfect().with_reorder(Reorder::Reversed),
    );
    // The carrier really did reverse arrival: first delivered is last sent.
    assert_eq!(
        reversed.first().unwrap().frame.position,
        sent.last().unwrap().position
    );

    let mut consumer = Consumer::new();
    consumer.ingest(reversed);
    let stored = consumer.store().stream(&stream).unwrap();

    assert_eq!(
        stored.to_vec(),
        sent,
        "reconstruction restores the sent order from reversed arrival"
    );
}

/// Spec §9 (no fabrication) — a position delivered twice collapses to one
/// stored frame; ingest is idempotent and append-only.
#[test]
fn ingest_is_idempotent_on_duplicate_positions() {
    let stream = test_stream();
    let frame = support::typed_frame(&payloads::resource(0), 0);

    let mut consumer = Consumer::new();
    consumer.accept(Delivery::new(stream.clone(), frame.clone()));
    let was_new = consumer.accept(Delivery::new(stream.clone(), frame.clone()));

    assert!(
        !was_new,
        "a repeated position is not recorded a second time"
    );
    assert_eq!(
        consumer.store().stream(&stream).unwrap().len(),
        1,
        "exactly one frame stored"
    );
}

/// Spec §8.4 — a node identity reused across lifetimes does not merge: each
/// life is its own stored stream, even though both begin at position 0 with
/// an identity frame.
#[test]
fn reincarnated_node_does_not_merge_with_prior_life() {
    let node = "node-worker-7";
    let life1 = StreamId::new(NodeId::new(node), Lifetime(1));
    let life2 = StreamId::new(NodeId::new(node), Lifetime(2)); // same node, new life
    let other = StreamId::new(NodeId::new("node-worker-8"), Lifetime(1));

    let s1 = realistic_stream(&life1);
    let s2 = realistic_stream(&life2);
    let s3 = realistic_stream(&other);

    // Interleaved on one wire; ingest must route by stream id alone.
    let delivered = ScriptedTransport::carry_all(&[
        (life1.clone(), s1.clone(), StreamScript::perfect()),
        (life2.clone(), s2.clone(), StreamScript::perfect()),
        (other.clone(), s3.clone(), StreamScript::perfect()),
    ]);

    let mut consumer = Consumer::new();
    consumer.ingest(delivered);
    let store = consumer.store();

    assert_eq!(store.len(), 3, "three distinct streams, none merged");
    assert_eq!(store.stream(&life1).unwrap().to_vec(), s1);
    assert_eq!(store.stream(&life2).unwrap().to_vec(), s2);
    assert_eq!(store.stream(&other).unwrap().to_vec(), s3);

    // Both lives have a position-0 identity frame; they did not collide.
    let id1 = store.stream(&life1).unwrap().at(Position(0)).unwrap();
    let id2 = store.stream(&life2).unwrap().at(Position(0)).unwrap();
    assert_ne!(
        id1.payload, id2.payload,
        "each life's identity frame is its own"
    );
    assert_eq!(
        IdentityRecord::decode(&id1.payload).unwrap().life,
        1,
        "life-1 identity attributes to lifetime 1"
    );
    assert_eq!(IdentityRecord::decode(&id2.payload).unwrap().life, 2);
}

// ── Views: read-time projections (spec §9) ─────────────────────────────

/// Kind II (testing spec §6) — the merged log view (spec §9.2) is every
/// stored frame in position order, channels interleaved, with the dropped
/// position surfaced as a gap. Its structure equals the reference oracle.
#[test]
fn merged_log_matches_oracle_with_surfaced_gap() {
    let stream = test_stream();
    let sent = realistic_stream(&stream); // positions 0..6, mixed channels
    let script = StreamScript::dropping([3]).with_reorder(Reorder::Reversed);
    let delivered = ScriptedTransport::carry(&stream, &sent, &script);

    let mut consumer = Consumer::new();
    consumer.ingest(delivered.clone());
    let stored = consumer.store().stream(&stream).unwrap();

    let log = views::merged_log(stored);
    let expected = reference::merged_log(&reference::delivered_frames(&stream, &delivered));
    assert_eq!(
        structure(&log),
        expected,
        "merged log = timeline in position order with gap surfaced"
    );

    // The gap sits between the bracketing frames, not at the end.
    assert!(
        log.iter()
            .any(|e| matches!(e, LogEntry::Gap(g) if g.start == 3 && g.end == 3)),
        "position 3 is surfaced as an interior gap"
    );
}

/// Kind I (testing spec §5) — a view decodes each channel via its codec and
/// degrades to raw bytes over anything it cannot decode (spec §9.3): a typed
/// channel decodes to a record, a text channel to lines, an unknown channel
/// to bytes, and a typed channel carrying garbage degrades rather than
/// failing.
#[test]
fn view_decodes_each_channel_and_degrades_gracefully() {
    let registry = test_registry();
    // Typed channel → structured record.
    let resource = payloads::resource(2);
    match views::decode_body_with(&ResourceSample::channel(), &resource.encode(), &registry) {
        Body::Record(value) => {
            assert!(
                value.get("cpu_pct").is_some(),
                "typed channel decodes to its record fields"
            );
        }
        other => panic!("typed channel should decode to a record, got {other:?}"),
    }

    // Raw-text channel → its line.
    let line = payloads::log_line("trainer", 7);
    let text_channel = catalog::process_output("trainer", ProcStream::Stdout);
    assert_eq!(
        views::decode_body_with(&text_channel, line.as_bytes(), &registry),
        Body::Text(line.clone())
    );

    // Unknown channel → raw bytes (graceful, spec §6.3/§9.3).
    let opaque = ChannelId::new("v2.gpu.thermals");
    assert_eq!(
        views::decode_body_with(&opaque, &[0xDE, 0xAD], &registry),
        Body::Raw(vec![0xDE, 0xAD])
    );

    // Typed channel, garbage bytes → degrades to raw, never panics or drops.
    let garbage = b"not-json{oops".to_vec();
    assert_eq!(
        views::decode_body_with(&ResourceSample::channel(), &garbage, &registry),
        Body::Raw(garbage.clone()),
        "a typed channel that fails to parse degrades to bytes (spec §9.3)"
    );
}

/// Kind I (testing spec §5) — the metric projection (spec §9.2) decodes one
/// typed channel into a time series, in position order, leaving the other
/// channels untouched. Expected is the records that were emitted.
#[test]
fn metric_projection_decodes_one_typed_channel_into_a_series() {
    let stream = test_stream();
    let mux = Mux::unbounded(stream.clone());
    mux.submit(
        catalog::IDENTITY,
        payloads::identity("node-alpha", 1).encode(),
    );

    let mut expected: Vec<(Position, ResourceSample)> = Vec::new();
    for i in 0..5 {
        let sample = payloads::resource(i);
        let pos = mux.submit(catalog::HOST_RESOURCE, sample.encode());
        expected.push((pos, sample));
        // Interleave an unrelated channel — the projection must ignore it.
        mux.submit(
            catalog::MEMBERSHIP,
            payloads::membership("p", "alive", "dead").encode(),
        );
    }
    let sent = mux.drain();

    let consumer = consume(&stream, &sent, StreamScript::perfect());
    let series = views::metric_series::<ResourceSample>(consumer.store().stream(&stream).unwrap());

    assert_eq!(
        series, expected,
        "the resource series is exactly the samples emitted, in order"
    );
}

/// Kind I (testing spec §5) — the migrated consolidated records (`dist.state`,
/// `runtime.actors`) each survive the pipe unchanged: every channel projects
/// back into the exact series emitted, even interleaved with unrelated channels.
#[test]
fn metric_projection_recovers_each_consolidated_record() {
    let stream = test_stream();
    let mux = Mux::unbounded(stream.clone());

    let mut dist: Vec<(Position, DistributionState)> = Vec::new();
    let mut actors: Vec<(Position, ActorRuntimeDetail)> = Vec::new();
    for i in 0..5 {
        let d = payloads::dist_state(i);
        dist.push((mux.submit(catalog::DIST_STATE, d.encode()), d));
        let a = payloads::actor_detail(i);
        actors.push((mux.submit(catalog::RUNTIME_ACTORS, a.encode()), a));
        // Unrelated channels each projection must step over.
        mux.submit(catalog::HOST_RESOURCE, payloads::resource(i).encode());
        mux.submit(
            catalog::MEMBERSHIP,
            payloads::membership("p", "alive", "dead").encode(),
        );
    }
    let sent = mux.drain();
    let consumer = consume(&stream, &sent, StreamScript::perfect());
    let stored = consumer.store().stream(&stream).unwrap();

    assert_eq!(views::metric_series::<DistributionState>(stored), dist);
    assert_eq!(views::metric_series::<ActorRuntimeDetail>(stored), actors);
}

/// Kind I (testing spec §5) — tail, grep, and filter are windowed /
/// predicate views over the stream (spec §9.2).
#[test]
fn tail_grep_and_filter_restrict_the_stream() {
    let stream = test_stream();
    let sent = realistic_stream(&stream); // id, resource, opaque, proc, resource, membership
    let consumer = consume(&stream, &sent, StreamScript::perfect());
    let stored = consumer.store().stream(&stream).unwrap();

    // Tail: the last two frames by position.
    let last_two = views::tail(stored, 2);
    assert_eq!(
        last_two,
        stored.to_vec()[4..].to_vec(),
        "tail(2) is the final two frames in order"
    );

    // Grep: only the membership transition mentions "suspect".
    let hits = views::grep(stored, "suspect");
    assert_eq!(hits.len(), 1, "exactly one frame matches the needle");
    assert_eq!(hits[0].channel, ChannelId::new(catalog::MEMBERSHIP));

    // Filter: restrict to a single channel.
    let resources = views::filter(stored, |f| f.channel == ResourceSample::channel());
    assert_eq!(resources.len(), 2, "two resource samples in the stream");
    assert!(
        resources
            .iter()
            .all(|f| f.channel == ResourceSample::channel())
    );
}

/// Boundary — views over an empty stream are empty, and a single-frame
/// stream has no gaps. Views must not panic at the edges.
#[test]
fn views_handle_empty_and_singleton_streams() {
    let empty = StoredStream::new();
    assert!(views::merged_log(&empty).is_empty());
    assert!(views::tail(&empty, 5).is_empty());
    assert!(views::metric_series::<ResourceSample>(&empty).is_empty());
    assert_eq!(views::replay(&empty).count(), 0);

    let mut one = StoredStream::new();
    one.record(support::typed_frame(&payloads::resource(0), 0));
    let log = views::merged_log(&one);
    assert_eq!(log.len(), 1, "one frame, one entry");
    assert!(
        matches!(log[0], LogEntry::Frame(_)),
        "no spurious gap around a lone frame"
    );
}

/// Resource safety (spec §7.4/§7.5) — surfacing a gap costs O(stored frames),
/// never O(gap size). A long consumer outage, or a single wild position from
/// a corrupt best-effort datagram, can leave the store bracketing an enormous
/// interior gap. Surfacing it must be one span computed from the bracketing
/// frames, not an enumeration of the missing range (which would hang / OOM
/// the single consumer). The reference oracle agrees on the same cheap path.
#[test]
fn gap_surfacing_is_bounded_by_frame_count_not_gap_size() {
    // The largest interior gap a u64 position space admits.
    let mut extreme = StoredStream::new();
    extreme.record(support::typed_frame(&payloads::resource(0), 0));
    let high = Frame::new(
        ResourceSample::channel(),
        Position(u64::MAX),
        payloads::resource(1).encode(),
    );
    extreme.record(high.clone());

    // Were this O(gap size), the next line would never return.
    assert_eq!(
        extreme.gap_spans(),
        vec![GapSpan {
            start: 1,
            end: u64::MAX - 1
        }],
        "one span covers the whole interior gap"
    );
    assert_eq!(
        reference::gap_spans(&extreme.to_vec()),
        extreme.gap_spans(),
        "oracle agrees on the cheap path"
    );

    // The merged log over the same store is also O(frames): two frames with
    // a single gap span between them, not a billion entries.
    let log = views::merged_log(&extreme);
    assert_eq!(log.len(), 3, "two frames and one gap span");
    assert!(
        matches!(log[1], LogEntry::Gap(_)),
        "the gap sits between the bracketing frames"
    );

    // A realistic long outage (millions of dropped positions) is just as cheap.
    let mut outage = StoredStream::new();
    outage.record(support::typed_frame(&payloads::resource(0), 0));
    for p in 5_000_000u64..5_000_003 {
        outage.record(support::typed_frame(&payloads::resource(p), p));
    }
    assert_eq!(
        outage.gap_spans(),
        vec![GapSpan {
            start: 1,
            end: 4_999_999
        }],
        "the outage is one surfaced span, the stream resumes after it"
    );
}

// ── Kind III — the full mock (testing spec §7) ─────────────────────────
//
// Each test assembles the ENTIRE pipe — producers → real mux → scripted
// transport seam → real ingest → store → merged-log view — and drives it
// with one verified vector, checking the view (the consumer-facing end)
// against the reference oracle. The oracle's `expected` is derived from the
// scenario (emitted frames + the script's drops), never from the run, and
// is blind to delivery order — which is precisely the property the pipe
// must satisfy.

/// Adversarial vector — reordering as deep as the envelope allows. Delivered
/// fully reversed, the assembled pipe still reconstructs the exact timeline:
/// reconstruction does not assume arrival order (spec §7.5).
#[test]
fn full_mock_survives_deepest_reorder() {
    let id = StreamId::new(NodeId::new("node-mock-1"), Lifetime(1));
    let sent = busy_node(&id);

    let script = StreamScript::perfect().with_reorder(Reorder::Reversed);
    let delivered = ScriptedTransport::carry(&id, &sent, &script);

    let mut consumer = Consumer::new();
    consumer.ingest(delivered);
    let stored = consumer.store().stream(&id).unwrap();
    let log = views::merged_log(stored);

    assert_eq!(
        structure(&log),
        reference::merged_log(&surviving(&sent, &[])),
        "merged log equals the oracle despite the deepest reorder"
    );
    assert!(stored.gap_spans().is_empty(), "nothing dropped — no gaps");
    assert_eq!(
        stored.to_vec(),
        sent,
        "full timeline restored from fully reversed arrival"
    );
}

/// Adversarial vector — total loss of a span. A whole contiguous run of
/// positions never arrives (and survivors are reordered too); the view
/// surfaces one gap and the stream resumes after it, without replaying lost
/// history (spec §7.4, §7.5).
#[test]
fn full_mock_surfaces_total_span_loss_and_resumes() {
    let id = StreamId::new(NodeId::new("node-mock-2"), Lifetime(1));
    let sent = busy_node(&id); // positions 0..10
    let lost = [4u64, 5, 6, 7]; // an entire span vanishes

    let script = StreamScript::dropping(lost).with_reorder(Reorder::Windows(3));
    let delivered = ScriptedTransport::carry(&id, &sent, &script);

    let mut consumer = Consumer::new();
    consumer.ingest(delivered);
    let stored = consumer.store().stream(&id).unwrap();

    assert_eq!(
        structure(&views::merged_log(stored)),
        reference::merged_log(&surviving(&sent, &lost)),
        "view equals the oracle with the span surfaced as a gap"
    );
    assert_eq!(
        stored.gap_spans(),
        vec![GapSpan { start: 4, end: 7 }],
        "the lost span is one surfaced gap, not silent concatenation"
    );

    // The stream resumes with the original post-gap frames — nothing from
    // the lost span reappears, and history is not replayed.
    assert!(
        stored.frames().all(|f| !lost.contains(&f.position.0)),
        "lost span absent"
    );
    let after: Vec<Frame> = stored
        .frames()
        .filter(|f| f.position.0 >= 8)
        .cloned()
        .collect();
    let original_after: Vec<Frame> = sent.iter().filter(|f| f.position.0 >= 8).cloned().collect();
    assert_eq!(
        after, original_after,
        "resumes at position 8 with the originals, in order"
    );
}

/// Adversarial vector — a channel the consumer cannot decode. Its bytes are
/// a perfectly valid record, but on a channel the catalog does not know:
/// the pipe stores it whole, a view degrades it to raw bytes now, and it
/// decodes later once the channel is learned (spec §6.3, §8.3).
#[test]
fn full_mock_stores_undecodable_channel_and_decodes_it_later() {
    let id = StreamId::new(NodeId::new("node-mock-3"), Lifetime(1));
    let node = Node::new(id.clone());
    node.emit(&payloads::identity("node-mock-3", 1)); // 0
    // A future host-metric channel this consumer has never heard of, whose
    // payload happens to be a valid resource sample.
    let future = payloads::resource(3);
    let future_channel = "v2.future.host_metric";
    node.emit_opaque(future_channel, &future.encode()); // 1
    node.emit(&payloads::runtime(1)); // 2
    let sent = node.sent();

    let delivered = ScriptedTransport::carry(&id, &sent, &StreamScript::perfect());
    let mut consumer = Consumer::new();
    consumer.ingest(delivered);
    let stored = consumer.store().stream(&id).unwrap();

    // Now: the view cannot decode it, so it degrades to raw bytes (§9.3),
    // but the frame is present and whole.
    let log = views::merged_log(stored);
    let entry = log
        .iter()
        .find_map(|e| match e {
            LogEntry::Frame(mf) if mf.position == Position(1) => Some(mf),
            _ => None,
        })
        .expect("the unknown-channel frame is on the timeline");
    assert!(
        matches!(entry.body, Body::Raw(_)),
        "unknown channel degrades to raw bytes now"
    );

    let raw = stored.at(Position(1)).unwrap();
    assert_eq!(
        raw.channel,
        ChannelId::new(future_channel),
        "retained on its own channel"
    );

    // Later: once the channel is learned, the stored bytes decode to the
    // original record — nothing was lost at ingest.
    let decoded = ResourceSample::decode(&raw.payload).expect("decodes once the channel is known");
    assert_eq!(
        decoded, future,
        "the opaque bytes were the record all along"
    );
}

/// Adversarial vector — a node identity reused across lifetimes. Two lives
/// of one node flow into the one consumer (one delivered reversed); at the
/// view end they are two separate timelines that never merge (spec §8.4).
#[test]
fn full_mock_reused_identity_does_not_merge_at_the_view() {
    let node = "node-recycled";
    let life1 = StreamId::new(NodeId::new(node), Lifetime(1));
    let life2 = StreamId::new(NodeId::new(node), Lifetime(2));

    let s1 = busy_node(&life1);
    let s2 = busy_node(&life2);

    let delivered = ScriptedTransport::carry_all(&[
        (life1.clone(), s1.clone(), StreamScript::perfect()),
        (
            life2.clone(),
            s2.clone(),
            StreamScript::perfect().with_reorder(Reorder::Reversed),
        ),
    ]);

    let mut consumer = Consumer::new();
    consumer.ingest(delivered);
    let store = consumer.store();
    assert_eq!(store.len(), 2, "two lives, two streams — never merged");

    let log1 = views::merged_log(store.stream(&life1).unwrap());
    let log2 = views::merged_log(store.stream(&life2).unwrap());
    assert_eq!(
        structure(&log1),
        reference::merged_log(&surviving(&s1, &[]))
    );
    assert_eq!(
        structure(&log2),
        reference::merged_log(&surviving(&s2, &[]))
    );

    // Each life's position-0 identity frame is its own, correctly attributed.
    let id1 = IdentityRecord::decode(
        &store
            .stream(&life1)
            .unwrap()
            .at(Position(0))
            .unwrap()
            .payload,
    )
    .unwrap();
    let id2 = IdentityRecord::decode(
        &store
            .stream(&life2)
            .unwrap()
            .at(Position(0))
            .unwrap()
            .payload,
    )
    .unwrap();
    assert_eq!(
        (id1.life, id2.life),
        (1, 2),
        "identities attribute to their own lifetimes"
    );
}

/// Envelope coverage (testing spec §9) — an explicit arrival permutation,
/// distinct from full reversal: the carrier delivers evens then odds. The
/// carrier applies exactly that permutation (no fabrication or loss), and
/// the pipe reconstructs the send order regardless.
#[test]
fn full_mock_reconstructs_under_explicit_permutation() {
    let id = StreamId::new(NodeId::new("node-perm"), Lifetime(1));
    let sent = busy_node(&id);
    let n = sent.len();

    // Deliver all even indices first, then all odd ones.
    let perm: Vec<usize> = (0..n)
        .filter(|i| i.is_multiple_of(2))
        .chain((0..n).filter(|i| !i.is_multiple_of(2)))
        .collect();
    let script = StreamScript::perfect().with_reorder(Reorder::Permutation(perm.clone()));
    let delivered = ScriptedTransport::carry(&id, &sent, &script);

    // The carrier delivered exactly the scripted permutation of positions.
    let arrival: Vec<u64> = delivered.iter().map(|d| d.frame.position.0).collect();
    let expected_arrival: Vec<u64> = perm.iter().map(|&i| sent[i].position.0).collect();
    assert_eq!(
        arrival, expected_arrival,
        "delivered in the scripted permutation, nothing added or lost"
    );

    let mut consumer = Consumer::new();
    consumer.ingest(delivered);
    let stored = consumer.store().stream(&id).unwrap();
    assert_eq!(
        stored.to_vec(),
        sent,
        "reconstruction restores send order from the permutation"
    );
    assert_eq!(
        structure(&views::merged_log(stored)),
        reference::merged_log(&surviving(&sent, &[])),
        "merged log equals the oracle"
    );
}

// ── Kind IV — the deployment simulation (testing spec §8) ──────────────

/// Record the virtual tick at which a position was emitted (positions are
/// contiguous from 0, so the tick vector is indexed by position).
fn at_tick(ticks: &mut Vec<u64>, tick: u64, pos: Position) {
    assert_eq!(
        pos.0 as usize,
        ticks.len(),
        "positions emitted contiguously"
    );
    ticks.push(tick);
}

/// The positions a node emitted during an outage window — the frames lost
/// while the consumer was absent (spec §7.4).
fn emitted_during(tick_of: &[u64], outage_ticks: &[u64]) -> Vec<u64> {
    tick_of
        .iter()
        .enumerate()
        .filter(|(_, t)| outage_ticks.contains(t))
        .map(|(pos, _)| pos as u64)
        .collect()
}

/// A scenario shaped like a real run (testing spec §8): three nodes boot and
/// emit on real channels at real cadence over a virtual clock; the carrier
/// drops one frame; the consumer is absent for a span; one node dies; one
/// finalizes. Every fault is in the §9 envelope. The end-to-end answer is
/// the full merged-log view over the consumer's store, **derived from the
/// scenario by the reference model** — so the expected log already carries
/// the right gaps, the dead node already ends at its last delivered frame,
/// and the outage span is already absent. The simulation passes only if the
/// assembled pipe reproduces every node's log exactly.
#[test]
fn kind_iv_deployment_simulation() {
    const OUTAGE: [u64; 3] = [4, 5, 6]; // ticks the consumer is absent

    // ── Node A: lives the whole run and finalizes at t8 ────────────────
    let id_a = StreamId::new(NodeId::new("node-a"), Lifetime(1));
    let a = Node::new(id_a.clone());
    let mut a_ticks = Vec::new();
    at_tick(&mut a_ticks, 0, a.emit(&payloads::identity("node-a", 1)));
    for t in 1..=5 {
        at_tick(&mut a_ticks, t, a.emit(&payloads::resource(t)));
    }
    at_tick(&mut a_ticks, 7, a.emit(&payloads::resource(7))); // resumes after the outage
    // Finalize frame: a datastream.health record whose `assigned` carries the seed,
    // so the surviving last frame is identifiable by value below.
    at_tick(&mut a_ticks, 8, a.emit(&payloads::datastream_health(7200))); // finalize
    let sent_a = a.sent();

    // ── Node B: lives, and also emits membership transitions ───────────
    let id_b = StreamId::new(NodeId::new("node-b"), Lifetime(1));
    let b = Node::new(id_b.clone());
    let mut b_ticks = Vec::new();
    at_tick(&mut b_ticks, 0, b.emit(&payloads::identity("node-b", 1)));
    at_tick(&mut b_ticks, 1, b.emit(&payloads::resource(1)));
    at_tick(&mut b_ticks, 2, b.emit(&payloads::resource(2)));
    at_tick(
        &mut b_ticks,
        2,
        b.emit(&payloads::membership("node-c", "alive", "suspect")),
    );
    at_tick(&mut b_ticks, 3, b.emit(&payloads::resource(3)));
    at_tick(&mut b_ticks, 4, b.emit(&payloads::resource(4)));
    at_tick(
        &mut b_ticks,
        4,
        b.emit(&payloads::membership("node-c", "suspect", "dead")),
    );
    at_tick(&mut b_ticks, 5, b.emit(&payloads::resource(5)));
    at_tick(&mut b_ticks, 7, b.emit(&payloads::resource(7)));
    at_tick(&mut b_ticks, 8, b.emit(&payloads::runtime(8)));
    let sent_b = b.sent();

    // ── Node C: dies at t6 (emits nothing after t5) ────────────────────
    let id_c = StreamId::new(NodeId::new("node-c"), Lifetime(1));
    let c = Node::new(id_c.clone());
    let mut c_ticks = Vec::new();
    at_tick(&mut c_ticks, 0, c.emit(&payloads::identity("node-c", 1)));
    for t in 1..=5 {
        at_tick(&mut c_ticks, t, c.emit(&payloads::resource(t)));
    }
    // t6: dies. Nothing more is emitted.
    let sent_c = c.sent();

    // ── Faults, all inside the §9 envelope ─────────────────────────────
    // The carrier drops A's t3 resource frame (a known single drop).
    let mut drop_a = emitted_during(&a_ticks, &OUTAGE);
    drop_a.push(3);
    let drop_b = emitted_during(&b_ticks, &OUTAGE);
    let drop_c = emitted_during(&c_ticks, &OUTAGE);

    // Each node's surviving frames may also arrive reordered, at varying
    // depth — reconstruction must not care.
    let delivered = ScriptedTransport::carry_all(&[
        (
            id_a.clone(),
            sent_a.clone(),
            StreamScript::dropping(drop_a.clone()),
        ),
        (
            id_b.clone(),
            sent_b.clone(),
            StreamScript::dropping(drop_b.clone()).with_reorder(Reorder::Windows(3)),
        ),
        (
            id_c.clone(),
            sent_c.clone(),
            StreamScript::dropping(drop_c.clone()).with_reorder(Reorder::Reversed),
        ),
    ]);

    let mut consumer = Consumer::new();
    consumer.ingest(delivered);
    let store = consumer.store();

    // ── The end-to-end answer: each node's full merged-log view equals the
    //    reference model's, derived from the scenario alone. ─────────────
    assert_eq!(
        store.len(),
        3,
        "three independent streams; positions not comparable across them"
    );
    for (id, sent, dropped) in [
        (&id_a, &sent_a, &drop_a),
        (&id_b, &sent_b, &drop_b),
        (&id_c, &sent_c, &drop_c),
    ] {
        let log = views::merged_log(store.stream(id).unwrap());
        assert_eq!(
            structure(&log),
            reference::merged_log(&surviving(sent, dropped)),
            "merged log for {id} matches the oracle derived from the scenario"
        );
    }

    // ── Targeted reads of the scenario's signature properties ──────────
    let a_stored = store.stream(&id_a).unwrap();
    let b_stored = store.stream(&id_b).unwrap();
    let c_stored = store.stream(&id_c).unwrap();

    // A: the carrier drop (t3) and the outage (t4–5) coalesce into one
    // surfaced gap, then the stream resumes — without replaying history.
    assert_eq!(
        a_stored.gap_spans(),
        vec![GapSpan { start: 3, end: 5 }],
        "A: one surfaced gap"
    );
    let finalize = a_stored.frames().last().unwrap();
    assert_eq!(
        DatastreamHealth::decode(&finalize.payload)
            .unwrap()
            .assigned,
        7200,
        "A's run ends with the finalize frame, delivered after the outage"
    );

    // B: its outage span (t4–5 emissions) is one surfaced gap; it resumes.
    assert_eq!(b_stored.gap_spans().len(), 1, "B: a single outage gap");
    assert!(
        b_stored.frames().count() > 5,
        "B resumed and kept producing after the outage"
    );

    // C: the dead node's stream ends at its last delivered frame — its
    // outage-lost t4–5 frames are trailing loss (truncation), NOT a gap.
    assert!(
        c_stored.gap_spans().is_empty(),
        "C: dead node truncates, no trailing gap"
    );
    assert_eq!(
        c_stored.frames().last().unwrap().position,
        Position(3),
        "C ends at its last delivered position (t3)"
    );
}
