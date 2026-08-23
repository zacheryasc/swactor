//! Byte-ring guarantees (behavior invariants, not implementation shape).
//!
//! Written before the implementation: every test here is failing until
//! `byte_ring` lands. Each test names the property it defends from the
//! protocol list in `src/byte_ring.rs`.
#![cfg(target_os = "linux")]

use data_plane::arena::{ArenaConfig, ArenaManager, NodeId};
use data_plane::byte_ring::{
    self, ByteRingSpec, FlowError, HeaderError, RecordKind, Role, attach, install,
};

fn arena() -> ArenaManager {
    ArenaManager::boot(ArenaConfig {
        node_id: NodeId(10),
        reservation_ceiling: 1 << 20,
        base_alignment: 64,
    })
    .expect("test arena must boot")
}

fn spec(capacity: u64, generation: u64) -> ByteRingSpec {
    ByteRingSpec {
        capacity,
        generation,
        alignment: 64,
        request_id: 1,
    }
}

fn installed(capacity: u64, generation: u64) -> (ArenaManager, byte_ring::RingHandle) {
    let mut arena = arena();
    let handle = install(&mut arena, spec(capacity, generation)).expect("ring must install");
    (arena, handle)
}

fn scribble_u64(arena: &ArenaManager, handle: &byte_ring::RingHandle, off: u64, value: u64) {
    arena
        .write_arena(handle.offset + off, &value.to_le_bytes())
        .expect("scribble");
}

/// Endpoints must be thread-safe enough to hand one side to another thread
/// (property P1's concurrency test depends on this).
#[test]
fn endpoints_are_send() {
    fn assert_send<T: Send>() {}
    assert_send::<byte_ring::Endpoint>();
}

// ─── install ─────────────────────────────────────────────────────────────────

/// B10-style: after `install` returns, the header is fully written.
#[test]
fn install_writes_valid_header_and_zeroed_data() {
    use data_plane::byte_ring::{
        OFF_CAPACITY, OFF_COMMIT, OFF_CONSUME, OFF_GENERATION, OFF_MAGIC, OFF_VERSION, RING_MAGIC,
        RING_VERSION,
    };

    let (arena, handle) = installed(4096, 7);
    assert_eq!(handle.capacity, 4096);
    assert_eq!(handle.generation, 7);

    let page = arena.read_arena(handle.offset, 128).expect("read header");
    let u32_at = |off: usize| u32::from_le_bytes(page[off..off + 4].try_into().unwrap());
    let u16_at = |off: usize| u16::from_le_bytes(page[off..off + 2].try_into().unwrap());
    let u64_at = |off: usize| u64::from_le_bytes(page[off..off + 8].try_into().unwrap());

    assert_eq!(u32_at(OFF_MAGIC as usize), RING_MAGIC);
    assert_eq!(u16_at(OFF_VERSION as usize), RING_VERSION);
    assert_eq!(u64_at(OFF_CAPACITY as usize), 4096);
    assert_eq!(u64_at(OFF_GENERATION as usize), 7);
    assert_eq!(u64_at(OFF_COMMIT as usize), 0);
    assert_eq!(u64_at(OFF_CONSUME as usize), 0);
    assert!(page[40..128].iter().all(|&b| b == 0), "reserved bytes");
    let data = arena
        .read_arena(handle.offset + 128, 64)
        .expect("read data");
    assert!(data.iter().all(|&b| b == 0), "fresh data region is zero");
}

#[test]
fn install_rejects_bad_specs() {
    let mut arena = arena();
    assert!(matches!(
        install(&mut arena, spec(0, 1)).unwrap_err(),
        byte_ring::InstallError::ZeroCapacity
    ));
    assert!(matches!(
        install(&mut arena, spec(64, 0)).unwrap_err(),
        byte_ring::InstallError::ZeroGeneration
    ));
    assert!(matches!(
        install(&mut arena, spec(1 << 21, 1)).unwrap_err(),
        byte_ring::InstallError::LeaseRejected(_)
    ));
}

// ─── attach: untrusted-header containment (P5) ──────────────────────────────

type CorruptHeaderCase = (Option<(u64, u64)>, (u64, u64), HeaderError);

#[test]
fn attach_rejects_corrupt_headers_without_trusting_them() {
    let cases: Vec<CorruptHeaderCase> = vec![
        // (optional pre-scribble, (field, value), expected)
        (
            None,
            (byte_ring::OFF_MAGIC, 0xDEAD_BEEF),
            HeaderError::BadMagic { found: 0xDEAD_BEEF },
        ),
        (
            None,
            (byte_ring::OFF_VERSION, 2),
            HeaderError::UnsupportedVersion {
                found: 2,
                supported: 1,
            },
        ),
        (
            None,
            (byte_ring::OFF_CAPACITY, 999),
            HeaderError::CapacityMismatch {
                header: 999,
                handle: 512,
            },
        ),
        (
            None,
            (byte_ring::OFF_GENERATION, 4),
            HeaderError::GenerationMismatch {
                header: 4,
                handle: 3,
            },
        ),
        (
            Some((byte_ring::OFF_CONSUME, 10)),
            (byte_ring::OFF_COMMIT, 5),
            HeaderError::CommitBelowConsume {
                commit: 5,
                consume: 10,
            },
        ),
        (
            None,
            (byte_ring::OFF_COMMIT, 600),
            HeaderError::ReadableExceedsCapacity {
                commit: 600,
                consume: 0,
                capacity: 512,
            },
        ),
    ];
    for (pre, (field, value), expected) in cases {
        let (fresh_arena, fresh_handle) = installed(512, 3);
        if let Some((pre_field, pre_value)) = pre {
            scribble_u64(&fresh_arena, &fresh_handle, pre_field, pre_value);
        }
        scribble_u64(&fresh_arena, &fresh_handle, field, value);
        let error = attach(&fresh_arena, fresh_handle, Role::Producer)
            .expect_err("corrupt header must fail attach");
        assert_eq!(
            error,
            byte_ring::AttachError::Header(expected),
            "field {field} = {value}"
        );
    }
}

// ─── movement: exactly-once, in order (P1, P2) ───────────────────────────────

#[test]
fn basic_round_trip_preserves_bytes() {
    let (arena, handle) = installed(4096, 1);
    let mut producer = attach(&arena, handle, Role::Producer).expect("producer");
    let mut consumer = attach(&arena, handle, Role::Consumer).expect("consumer");

    producer
        .send_record(RecordKind::Data, b"weights-bytes")
        .expect("send");
    let received = consumer.recv_record().expect("recv");
    assert_eq!(
        received,
        Some((RecordKind::Data, b"weights-bytes".to_vec()))
    );
    assert_eq!(consumer.recv_record().expect("recv empty"), None);
}

#[test]
fn wraparound_preserves_stream_exactly() {
    // Capacity 128 forces many wraps; move 16 KiB through in odd chunks.
    let (arena, handle) = installed(128, 1);
    let mut producer = attach(&arena, handle, Role::Producer).expect("producer");
    let mut consumer = attach(&arena, handle, Role::Consumer).expect("consumer");

    let expected: Vec<u8> = (0..16_u32 * 1024).map(|i| (i % 251) as u8).collect();
    let mut sent = 0;
    let mut received = Vec::new();
    // Chunk sizes chosen to straddle the capacity and force partial state;
    // every chunk must satisfy 5-byte record header + chunk <= 128.
    let chunks = [3usize, 123, 1, 100, 65, 64, 122, 17];
    let mut chunk_index = 0;
    while received.len() < expected.len() {
        let chunk = chunks[chunk_index % chunks.len()];
        chunk_index += 1;
        let end = (sent + chunk).min(expected.len());
        // Producer sends what fits, drains when the ring is full.
        while producer
            .send_record(RecordKind::Data, &expected[sent..end])
            .is_err()
        {
            match consumer.recv_record().expect("recv") {
                Some((RecordKind::Data, bytes)) => received.extend_from_slice(&bytes),
                Some((kind, _)) => panic!("unexpected record kind {kind:?}"),
                None => panic!("deadlock: producer blocked, consumer empty"),
            }
        }
        sent = end;
        while let Some((RecordKind::Data, bytes)) = consumer.recv_record().expect("recv") {
            received.extend_from_slice(&bytes);
        }
    }
    assert_eq!(received, expected, "byte stream must survive wraparound");
}

#[test]
fn concurrent_producer_consumer_delivers_exactly_once() {
    use std::thread::scope;

    let (arena, handle) = installed(1024, 1);
    let expected: Vec<(RecordKind, Vec<u8>)> = {
        let mut rng = 0x5357_4752_1111_u64; // xorshift, deterministic
        let mut records = Vec::new();
        let mut total = 0;
        while total < 8 * 1024 {
            let len = ((rng & 0xFF) as usize).max(1);
            let bytes: Vec<u8> = (0..len).map(|i| ((total + i) % 251) as u8).collect();
            total += len;
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            records.push((RecordKind::Data, bytes));
        }
        records.push((RecordKind::Eof, Vec::new()));
        records
    };

    let producer_handle = handle;
    let consumer_handle = handle;
    let producer_records = &expected;
    let received: parking_lot::Mutex<Vec<(RecordKind, Vec<u8>)>> =
        parking_lot::Mutex::new(Vec::new());
    let mut producer = attach(&arena, producer_handle, Role::Producer).expect("producer");
    let mut consumer = attach(&arena, consumer_handle, Role::Consumer).expect("consumer");

    scope(|s| {
        let producer_thread = s.spawn(move || {
            for (kind, bytes) in producer_records {
                // Retry until space frees up: the consumer runs concurrently.
                while producer.send_record(*kind, bytes).is_err() {}
            }
        });
        let consumer_records = &received;
        let consumer_thread = s.spawn(move || {
            loop {
                match consumer.recv_record().expect("recv") {
                    Some((RecordKind::Eof, _)) => break,
                    Some(record) => consumer_records.lock().push(record),
                    None => continue, // spin: sleep discipline is the binding slice's concern
                }
            }
        });
        producer_thread.join().expect("producer thread");
        consumer_thread.join().expect("consumer thread");
    });
    let got = received.into_inner();
    assert_eq!(
        got,
        expected[..expected.len() - 1],
        "records must arrive exactly once, in order, under concurrency"
    );
}

// ─── backpressure (P7) ───────────────────────────────────────────────────────

#[test]
fn reserve_reports_exact_free_space_and_recovers() {
    let (arena, handle) = installed(128, 1);
    let mut producer = attach(&arena, handle, Role::Producer).expect("producer");
    let mut consumer = attach(&arena, handle, Role::Consumer).expect("consumer");

    let first = producer.reserve(100).expect("reserve fits");
    producer.write(&first, &[7; 100]).expect("write");
    producer.commit(first).expect("commit");

    assert_eq!(
        producer.reserve(50).unwrap_err(),
        FlowError::InsufficientSpace {
            requested: 50,
            free: 28
        }
    );

    // Bytes are readable, intact, and consuming frees the space again.
    assert_eq!(consumer.readable().expect("readable"), 100);
    let bytes = consumer.read(100).expect("read");
    assert!(bytes.iter().all(|&b| b == 7));
    consumer.consume(100).expect("consume");
    assert_eq!(producer.reserve(50).expect("reserve recovers").len, 50);
}

// ─── completion semantics (P9) ───────────────────────────────────────────────

#[test]
fn records_delimit_completion_distinctly() {
    let (arena, handle) = installed(4096, 1);
    let mut producer = attach(&arena, handle, Role::Producer).expect("producer");
    let mut consumer = attach(&arena, handle, Role::Consumer).expect("consumer");

    producer.send_record(RecordKind::Data, b"a").expect("send");
    producer.send_record(RecordKind::Data, b"b").expect("send");
    producer.send_record(RecordKind::Eof, b"").expect("send");

    assert_eq!(
        consumer.recv_record().expect("recv"),
        Some((RecordKind::Data, b"a".to_vec()))
    );
    assert_eq!(
        consumer.recv_record().expect("recv"),
        Some((RecordKind::Data, b"b".to_vec()))
    );
    assert_eq!(
        consumer.recv_record().expect("recv"),
        Some((RecordKind::Eof, Vec::new()))
    );
    assert_eq!(consumer.recv_record().expect("post-eof"), None);

    // Fault is a distinct terminal, not a second EOF.
    producer
        .send_record(RecordKind::Fault, b"reason")
        .expect("send");
    assert_eq!(
        consumer.recv_record().expect("recv fault"),
        Some((RecordKind::Fault, b"reason".to_vec()))
    );
}

#[test]
fn torn_records_stay_invisible_until_committed() {
    let (arena, handle) = installed(512, 1);
    let mut producer = attach(&arena, handle, Role::Producer).expect("producer");
    let mut consumer = attach(&arena, handle, Role::Consumer).expect("consumer");

    // Frame the record manually so only the commit is withheld.
    let mut framed = Vec::with_capacity(5 + 64);
    framed.push(1_u8); // RecordKind::Data
    framed.extend_from_slice(&64_u32.to_le_bytes());
    framed.extend_from_slice(&[9; 64]);
    let reservation = producer.reserve(69).expect("reserve");
    producer.write(&reservation, &framed).expect("write");
    // Deliberately do NOT commit.
    assert_eq!(consumer.readable().expect("readable"), 0);
    assert_eq!(consumer.recv_record().expect("torn invisible"), None);

    producer.commit(reservation).expect("commit");
    assert_eq!(consumer.readable().expect("readable"), 69);
    let (kind, bytes) = consumer.recv_record().expect("recv").unwrap();
    assert_eq!(kind, RecordKind::Data);
    assert!(bytes.iter().all(|&b| b == 9));
}

// ─── generation fence (P10) ──────────────────────────────────────────────────

#[test]
fn stale_reservations_are_rejected_after_generation_change() {
    let (arena, handle) = installed(512, 1);
    let mut producer = attach(&arena, handle, Role::Producer).expect("producer");

    let reservation = producer.reserve(32).expect("reserve");
    // The ring is replaced under us: generation moves on.
    scribble_u64(&arena, &handle, byte_ring::OFF_GENERATION, 2);
    assert_eq!(
        producer.commit(reservation).unwrap_err(),
        FlowError::StaleReservation {
            reservation: 1,
            ring: 2
        }
    );
}

// ─── role enforcement (P3) ───────────────────────────────────────────────────

#[test]
fn wrong_role_operations_are_rejected() {
    let (arena, handle) = installed(512, 1);
    let mut producer = attach(&arena, handle, Role::Producer).expect("producer");
    let mut consumer = attach(&arena, handle, Role::Consumer).expect("consumer");

    assert_eq!(
        consumer.reserve(8).unwrap_err(),
        FlowError::RoleViolation {
            operation: "reserve",
            role: Role::Consumer
        }
    );
    assert_eq!(
        consumer
            .commit(byte_ring::Reservation {
                start: 0,
                len: 0,
                generation: 1,
            })
            .unwrap_err(),
        FlowError::RoleViolation {
            operation: "commit",
            role: Role::Consumer
        }
    );
    assert_eq!(
        producer.consume(8).unwrap_err(),
        FlowError::RoleViolation {
            operation: "consume",
            role: Role::Producer
        }
    );
    assert_eq!(
        producer.readable().unwrap_err(),
        FlowError::RoleViolation {
            operation: "readable",
            role: Role::Producer
        }
    );
}

// ─── mid-protocol revalidation (P5) ──────────────────────────────────────────

#[test]
fn operations_revalidate_cursors_and_never_panic() {
    let (arena, handle) = installed(512, 1);
    let producer = attach(&arena, handle, Role::Producer).expect("producer");
    let consumer = attach(&arena, handle, Role::Consumer).expect("consumer");

    // Corrupt the cursors mid-protocol (consume beyond commit).
    scribble_u64(&arena, &handle, byte_ring::OFF_CONSUME, 5);
    assert_eq!(
        producer.reserve(8).unwrap_err(),
        FlowError::Corrupt(HeaderError::CommitBelowConsume {
            commit: 0,
            consume: 5
        })
    );
    assert_eq!(
        consumer.readable().unwrap_err(),
        FlowError::Corrupt(HeaderError::CommitBelowConsume {
            commit: 0,
            consume: 5
        })
    );

    // An impossible readable span is rejected at attach, not crashed on.
    let (arena2, handle2) = installed(512, 1);
    scribble_u64(&arena2, &handle2, byte_ring::OFF_COMMIT, 600);
    assert_eq!(
        attach(&arena2, handle2, Role::Consumer).unwrap_err(),
        byte_ring::AttachError::Header(HeaderError::ReadableExceedsCapacity {
            commit: 600,
            consume: 0,
            capacity: 512
        })
    );
}

#[test]
fn pinned_record_blocks_capacity_until_release() {
    let (arena, handle) = installed(32, 1);
    let mut producer = attach(&arena, handle, Role::Producer).expect("producer");
    let mut consumer = attach(&arena, handle, Role::Consumer).expect("consumer");

    producer
        .send_record(RecordKind::Data, &[7; 20])
        .expect("send");
    let view = consumer
        .peek_record()
        .expect("peek")
        .expect("record must be visible");
    assert_eq!(view.kind(), RecordKind::Data);
    assert_eq!(view.len(), 20);
    assert!(view.spans().1.is_empty());
    assert_eq!(
        producer.reserve(8).unwrap_err(),
        FlowError::InsufficientSpace {
            requested: 8,
            free: 7,
        }
    );

    drop(view);
    assert_eq!(producer.reserve(8).expect("capacity released").len, 8);
}

#[test]
fn pinned_record_exposes_wrapped_payload_as_two_spans() {
    let (arena, handle) = installed(32, 1);
    let mut producer = attach(&arena, handle, Role::Producer).expect("producer");
    let mut consumer = attach(&arena, handle, Role::Consumer).expect("consumer");

    producer
        .send_record(RecordKind::Data, &[1; 18])
        .expect("first");
    assert_eq!(
        consumer.recv_record().expect("consume first"),
        Some((RecordKind::Data, vec![1; 18]))
    );
    let expected: Vec<u8> = (0..15).collect();
    producer
        .send_record(RecordKind::Data, &expected)
        .expect("wrapped record");

    let view = consumer
        .peek_record()
        .expect("peek")
        .expect("wrapped record visible");
    let (first, second) = view.spans();
    assert!(!first.is_empty());
    assert!(!second.is_empty());
    let observed: Vec<u8> = first.iter().chain(second).copied().collect();
    assert_eq!(observed, expected);
}

#[test]
fn writable_record_is_invisible_until_commit() {
    let (arena, handle) = installed(64, 1);
    let mut producer = attach(&arena, handle, Role::Producer).expect("producer");
    let mut consumer = attach(&arena, handle, Role::Consumer).expect("consumer");

    let mut reservation = producer
        .reserve_record(RecordKind::Data, 17)
        .expect("reserve record");
    let (first, second) = reservation.spans_mut();
    for (index, byte) in first.iter_mut().chain(second).enumerate() {
        *byte = index as u8;
    }
    assert!(consumer.peek_record().expect("peek uncommitted").is_none());
    reservation.commit().expect("commit");

    let view = consumer
        .peek_record()
        .expect("peek")
        .expect("committed record");
    let observed: Vec<u8> = view
        .spans()
        .0
        .iter()
        .chain(view.spans().1)
        .copied()
        .collect();
    assert_eq!(observed, (0..17).collect::<Vec<_>>());
}
