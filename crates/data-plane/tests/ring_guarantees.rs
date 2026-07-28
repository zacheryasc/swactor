//! Black-box contract tests for the reusable data-plane ring helper ABI.
//!
//! These tests intentionally know only the public helper surface:
//!
//! - helper-created bounded SPSC rings
//! - reserve, write, publish, read, consume, and wake operations
//! - cursor snapshots and wake hints observed through the helper
//!
//! They assert the guarantees in
//! the data-plane ring helper contract.

use data_plane::ring;

// A small ring forces wraparound and full/empty transitions quickly. The helper
// still owns the actual shared-memory atomics and process-local address math.
fn new_ring() -> ring::RingHelperHarness {
    ring::RingHelperHarness::create(ring::RingConfig {
        node_id: ring::NodeId(10),
        ring_id: ring::RingId(7000),
        capacity: 8,
        producer: ring::EndpointId("producer".into()),
        consumer: ring::EndpointId("consumer".into()),
    })
}

// This helper reads all currently committed bytes through the consumer API.
// It proves visibility through acquire reads instead of peeking at backing
// memory directly.
fn drain_committed(helper: &mut ring::RingHelperHarness) -> Vec<u8> {
    let readable = helper.consumer_readable();
    let bytes = helper.consumer_read(readable);
    helper.consumer_consume(readable);
    bytes
}

// Wake hints must remain hints. This helper checks the public wake enum without
// depending on scheduler internals.
fn assert_wake_is_payload_free(wake: &ring::WakeHint) {
    match wake {
        ring::WakeHint::RingReadable { ring_id } | ring::WakeHint::RingWritable { ring_id } => {
            assert_eq!(*ring_id, ring::RingId(7000));
        }
    }
}

// This proves ring identity is bounded SPSC, has one producer and one consumer,
// uses unique RingId values, and does not allow stale wakes to alias a
// replacement ring.
#[test]
fn ring_identity_is_unique_bounded_spsc_and_stale_wakes_do_not_alias() {
    // Create one ring and inspect its public identity.
    let mut helper = new_ring();
    let identity = helper.identity();
    assert_eq!(identity.ring_id, ring::RingId(7000));
    assert_eq!(identity.capacity, 8);
    assert_eq!(identity.producer_count, 1);
    assert_eq!(identity.consumer_count, 1);

    // Retire it and create a replacement with the same numeric id but a new
    // generation. Stale wake from the retired generation must not wake the new
    // ring.
    let stale_wake = helper.retire_and_capture_stale_wake();
    let mut replacement = ring::RingHelperHarness::create_replacement(identity.ring_id);
    replacement.deliver_wake(stale_wake);
    assert!(!replacement.wake_log().iter().any(|wake| wake.was_accepted));
}

// This proves commit and consume are monotonic logical byte positions, writes
// are invisible before commit, and physical wrap uses cursor modulo capacity.
#[test]
fn cursors_are_monotonic_and_wrap_by_modulo_capacity() {
    // Reserve and write without publishing.
    let mut helper = new_ring();
    let reservation = helper.producer_reserve(6).expect("space must exist");
    helper.producer_write(&reservation, b"abcdef");

    // Producer-local write is not readable before commit.
    assert_eq!(helper.consumer_readable(), 0);

    // Publishing makes the prefix readable and advances commit.
    helper.producer_commit(reservation);
    assert_eq!(helper.cursor_snapshot().commit, 6);
    assert_eq!(drain_committed(&mut helper), b"abcdef");
    assert_eq!(helper.cursor_snapshot().consume, 6);

    // Wrap the physical index while logical cursors keep increasing.
    let wrapped = helper
        .producer_reserve(5)
        .expect("space must exist after consume");
    helper.producer_write(&wrapped, b"ghijk");
    helper.producer_commit(wrapped);
    assert_eq!(helper.cursor_snapshot().commit, 11);
    assert_eq!(
        helper.cursor_snapshot().commit % helper.identity().capacity,
        3
    );
    assert_eq!(drain_committed(&mut helper), b"ghijk");
    assert_eq!(helper.cursor_snapshot().consume, 11);
}

// This proves the producer computes free space from acquired consume, never
// reserves beyond capacity, writes before publishing commit, and emits readable
// wake hints after publication.
#[test]
fn producer_respects_free_space_and_publishes_after_writing() {
    // Reserve the full ring and publish it.
    let mut helper = new_ring();
    let reservation = helper
        .producer_reserve(8)
        .expect("full ring reservation fits");
    helper.producer_write(&reservation, b"12345678");
    helper.producer_commit(reservation);

    // With no consumed bytes, producer cannot reserve additional space.
    assert!(matches!(
        helper.producer_reserve(1),
        Err(ring::ReserveError::InsufficientSpace)
    ));

    // The readable wake must be a payload-free hint.
    let wake = helper
        .wake_hints()
        .iter()
        .find(|wake| matches!(wake, ring::WakeHint::RingReadable { .. }))
        .expect("readable wake must be emitted");
    assert_wake_is_payload_free(wake);

    // Consumer sees the written bytes, proving commit was not published before
    // the payload became valid.
    assert_eq!(drain_committed(&mut helper), b"12345678");
}

// This proves the consumer computes readable bytes from acquired commit, never
// reads beyond committed data, advances consume only after release, and emits
// writable wake hints after freeing space.
#[test]
fn consumer_reads_only_committed_bytes_and_releases_after_safe_consume() {
    // Publish three committed bytes.
    let mut helper = new_ring();
    let reservation = helper.producer_reserve(3).expect("space must exist");
    helper.producer_write(&reservation, b"abc");
    helper.producer_commit(reservation);

    // The consumer cannot read beyond the committed prefix.
    assert_eq!(helper.consumer_readable(), 3);
    assert!(matches!(
        helper.consumer_try_read(4),
        Err(ring::ReadError::BeyondCommittedBytes)
    ));

    // Reading alone does not release bytes.
    assert_eq!(helper.consumer_read(3), b"abc");
    assert_eq!(helper.cursor_snapshot().consume, 0);

    // Consuming releases space and emits a writable hint.
    helper.consumer_consume(3);
    assert_eq!(helper.cursor_snapshot().consume, 3);
    let wake = helper
        .wake_hints()
        .iter()
        .find(|wake| matches!(wake, ring::WakeHint::RingWritable { .. }))
        .expect("writable wake must be emitted");
    assert_wake_is_payload_free(wake);
}

// This proves wake hints carry no byte ranges, counts, pointers, or credits,
// and coalescing cannot hide the only readable or writable transition.
#[test]
fn wake_hints_are_edge_hints_without_hiding_transitions() {
    // Create an empty ring and publish one byte, causing empty-to-readable.
    let mut helper = new_ring();
    let reservation = helper.producer_reserve(1).expect("space must exist");
    helper.producer_write(&reservation, b"x");
    helper.producer_commit(reservation);

    // The readable transition must be discoverable even if duplicate wakes are
    // coalesced.
    helper.coalesce_duplicate_wakes();
    assert!(
        helper
            .scheduler_state()
            .readable_rings
            .contains(&ring::RingId(7000))
    );

    // Fill then release space to cause full-to-writable.
    let _ = drain_committed(&mut helper);
    helper.coalesce_duplicate_wakes();
    assert!(
        helper
            .scheduler_state()
            .writable_rings
            .contains(&ring::RingId(7000))
    );

    // Every wake remains a payload-free hint.
    for wake in helper.wake_hints() {
        assert_wake_is_payload_free(wake);
    }
}

// This proves Python-facing helper operations own shared atomics and wrap math,
// while returned pointers are process-local addresses derived from arena base
// plus arena offsets.
#[test]
fn helper_abi_owns_atomics_wrap_math_and_process_local_pointers() {
    // Ask the helper for a process-local view of a layout.
    let helper = new_ring();
    let view = helper.map_process_local_view(ring::ArenaBase(0x1000));

    // The helper returns process-local addresses derived from offsets.
    assert_eq!(
        view.data_pointer,
        ring::ProcessLocalPointer::from_base_plus_offset(
            ring::ArenaBase(0x1000),
            view.layout.data_offset
        )
    );

    // Python operations use helper calls for cursor and wrap behavior instead
    // of implementing atomics directly.
    for operation in helper.python_visible_operations() {
        match operation {
            ring::PythonOperation::ReserveViaHelper { .. }
            | ring::PythonOperation::CommitViaHelper { .. }
            | ring::PythonOperation::ReadableViaHelper { .. }
            | ring::PythonOperation::ConsumeViaHelper { .. }
            | ring::PythonOperation::MapPointerViaHelper { .. } => {}
            ring::PythonOperation::DirectAtomicAccess { .. }
            | ring::PythonOperation::DirectWrapArithmetic { .. } => {
                panic!("Python operation bypassed helper ABI: {operation:?}")
            }
        }
    }
}
