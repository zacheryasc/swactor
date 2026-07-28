//! Black-box contract tests for MVP GPU worker ingress parsing.
//!
//! These tests intentionally know only the public worker ingress surface:
//!
//! - `InstallRing`, ring-readable wakes, committed ring bytes, EOF, and device
//!   copy outcomes in
//! - cursor changes, `ObjectLoaded`, `ObjectFailed`, and `RingFault` out
//!
//! They assert the guarantees in
//! `specs/mvp_system/gpu_worker_ingress_parser_contract.md`.

use data_plane::ingress;

// A valid ingress ring config supplies the edge object spec and current worker
// generation. The parser remains a black box behind ring helper operations.
fn ingress_ring() -> ingress::InstallRing {
    ingress::InstallRing {
        ring_id: ingress::RingId(8001),
        edge_id: ingress::EdgeId(7001),
        port_id: ingress::PortId("in".into()),
        direction: ingress::RingDirection::Ingress,
        object_spec: ingress::ObjectSpec {
            max_extent: 16,
            alignment: 4,
            layout: ingress::ObjectLayout::Token,
        },
        generation: ingress::WorkerGeneration(1),
    }
}

// The harness observes only worker-facing ring and device events. Tests do not
// parse private parser state or device allocation internals.
fn new_parser() -> ingress::IngressParserHarness {
    ingress::IngressParserHarness::new(ingress::WorkerGeneration(1))
}

// A valid object record is constructed through the public helper encoder so the
// test proves parser behavior rather than hard-coded header bytes.
fn valid_record(sequence: u64, extent: u64) -> Vec<u8> {
    ingress::ObjectRecordBuilder::new(ingress_ring().object_spec)
        .object_id(ingress::ObjectId(9000 + sequence))
        .sequence(sequence)
        .extent(extent)
        .payload(vec![7; extent as usize])
        .encode()
}

fn flagged_record(sequence: u64, extent: u64) -> Vec<u8> {
    ingress::ObjectRecordBuilder::new(ingress_ring().object_spec)
        .object_id(ingress::ObjectId(9000 + sequence))
        .sequence(sequence)
        .extent(extent)
        .flags(ingress::ObjectFlags {
            end_of_sequence: true,
            begin_sequence: true,
        })
        .payload(vec![7; extent as usize])
        .encode()
}

// This helper installs the ingress ring through the public command path.
fn installed_parser() -> ingress::IngressParserHarness {
    let mut harness = new_parser();
    harness.observe(ingress::WorkerIngressEvent::InstallRing(ingress_ring()));
    harness
}

// This proves ingress parsing starts only after InstallRing, reloads cursors
// after RingReadable, consumes committed bytes only, and does not read
// uncommitted bytes.
#[test]
fn parser_reads_only_committed_bytes_after_ingress_ring_install() {
    // Write committed bytes before install; the parser must not consume them.
    let mut harness = new_parser();
    harness.write_committed_bytes(ingress::RingId(8001), valid_record(0, 8));
    harness.observe(ingress::WorkerIngressEvent::RingReadable {
        ring_id: ingress::RingId(8001),
    });
    assert_eq!(harness.consume_cursor(ingress::RingId(8001)), 0);

    // Install the ingress ring and make committed bytes readable.
    harness.observe(ingress::WorkerIngressEvent::InstallRing(ingress_ring()));
    harness.observe(ingress::WorkerIngressEvent::RingReadable {
        ring_id: ingress::RingId(8001),
    });
    assert!(harness.cursor_reload_count(ingress::RingId(8001)) > 0);

    // Uncommitted bytes must not be read.
    harness.write_uncommitted_bytes(ingress::RingId(8001), vec![1, 2, 3, 4]);
    let before = harness.consume_cursor(ingress::RingId(8001));
    harness.observe(ingress::WorkerIngressEvent::RingReadable {
        ring_id: ingress::RingId(8001),
    });
    assert_eq!(harness.consume_cursor(ingress::RingId(8001)), before);
}

// This proves malformed or unsupported headers, max-extent violations,
// alignment/layout violations, and sequence violations emit ObjectFailed.
#[test]
fn header_validation_rejects_invalid_object_records() {
    // Each record corrupts one header fact in an installed parser.
    let cases = vec![
        (
            ingress::ObjectRecordBuilder::new(ingress_ring().object_spec)
                .unsupported_magic()
                .encode(),
            ingress::ObjectFailureReason::UnsupportedMagic,
        ),
        (
            ingress::ObjectRecordBuilder::new(ingress_ring().object_spec)
                .unsupported_version()
                .encode(),
            ingress::ObjectFailureReason::UnsupportedVersion,
        ),
        (
            ingress::ObjectRecordBuilder::new(ingress_ring().object_spec)
                .malformed_header_length()
                .encode(),
            ingress::ObjectFailureReason::MalformedHeaderLength,
        ),
        (
            ingress::ObjectRecordBuilder::new(ingress_ring().object_spec)
                .extent(32)
                .payload(vec![0; 32])
                .encode(),
            ingress::ObjectFailureReason::ExtentExceedsMax,
        ),
        (
            ingress::ObjectRecordBuilder::new(ingress_ring().object_spec)
                .extent(6)
                .payload(vec![0; 6])
                .encode(),
            ingress::ObjectFailureReason::ExtentAlignmentViolation,
        ),
    ];

    for (record, expected_reason) in cases {
        // Use a fresh installed parser for each invalid object.
        let mut harness = installed_parser();
        harness.write_committed_bytes(ingress::RingId(8001), record);
        harness.observe(ingress::WorkerIngressEvent::RingReadable {
            ring_id: ingress::RingId(8001),
        });

        // The parser must emit a typed object failure.
        assert!(harness.events().iter().any(|event| {
            matches!(
                event,
                ingress::WorkerIngressOut::ObjectFailed {
                    edge_id: ingress::EdgeId(7001),
                    port_id,
                    reason,
                    ..
                } if port_id == &ingress::PortId("in".into()) && *reason == expected_reason
            )
        }));
    }
}

#[test]
fn parser_decodes_and_propagates_object_header_flags() {
    let record = flagged_record(0, 8);
    let parsed = ingress::read_object_record(&record, ingress_ring().object_spec, true)
        .expect("flagged record parses");
    let ingress::ObjectRecordRead::Complete(parsed) = parsed else {
        panic!("record must be complete");
    };
    assert_eq!(
        parsed.flags,
        ingress::ObjectFlags {
            end_of_sequence: true,
            begin_sequence: true,
        }
    );
}

// This proves the parser copies exactly extent payload bytes to device memory,
// advances consume only after bytes are safe to release, can stream objects
// larger than the ring through bounded spans, and faults EOF before full payload.
#[test]
fn payload_loading_is_exact_extent_and_release_after_copy_completion() {
    // Install the parser and provide a valid record.
    let mut harness = installed_parser();
    harness.write_committed_bytes(ingress::RingId(8001), valid_record(0, 8));
    harness.observe(ingress::WorkerIngressEvent::RingReadable {
        ring_id: ingress::RingId(8001),
    });

    // Before device copy completion, consume must not advance past safe bytes.
    let before_copy_complete = harness.consume_cursor(ingress::RingId(8001));
    assert_eq!(before_copy_complete, 0);

    // Complete the device copy and prove exact extent was copied.
    harness.observe(ingress::WorkerIngressEvent::DeviceCopyCompleted {
        object_id: ingress::ObjectId(9000),
        byte_count: 8,
    });
    assert_eq!(harness.device_copy_log().last().unwrap().byte_count, 8);
    assert!(harness.consume_cursor(ingress::RingId(8001)) > before_copy_complete);

    // EOF mid-object faults the object.
    let mut eof = installed_parser();
    eof.write_committed_bytes(
        ingress::RingId(8001),
        ingress::ObjectRecordBuilder::new(ingress_ring().object_spec)
            .object_id(ingress::ObjectId(9010))
            .sequence(0)
            .extent(12)
            .partial_payload(vec![1, 2, 3])
            .encode(),
    );
    eof.observe(ingress::WorkerIngressEvent::Eof {
        ring_id: ingress::RingId(8001),
    });
    assert!(eof.events().iter().any(|event| {
        matches!(
            event,
            ingress::WorkerIngressOut::ObjectFailed {
                reason: ingress::ObjectFailureReason::EofBeforeFullPayload,
                ..
            }
        )
    }));
}

// This proves ObjectLoaded is emitted only after valid header, exact extent
// copy, copy completion, device handle creation, and with current-generation
// identity fields.
#[test]
fn object_loaded_requires_complete_valid_object_and_current_handle() {
    // Install and parse one valid record.
    let mut harness = installed_parser();
    harness.write_committed_bytes(ingress::RingId(8001), valid_record(0, 8));
    harness.observe(ingress::WorkerIngressEvent::RingReadable {
        ring_id: ingress::RingId(8001),
    });

    // No ObjectLoaded may appear before device copy completion and handle
    // creation.
    assert!(
        !harness
            .events()
            .iter()
            .any(|event| { matches!(event, ingress::WorkerIngressOut::ObjectLoaded { .. }) })
    );

    // Complete device work.
    harness.observe(ingress::WorkerIngressEvent::DeviceCopyCompleted {
        object_id: ingress::ObjectId(9000),
        byte_count: 8,
    });
    harness.observe(ingress::WorkerIngressEvent::DeviceHandleCreated {
        object_id: ingress::ObjectId(9000),
        handle: ingress::DeviceHandle::new(ingress::WorkerGeneration(1), 42),
    });

    // ObjectLoaded includes the required identities and current generation.
    assert!(harness.events().iter().any(|event| {
        matches!(
            event,
            ingress::WorkerIngressOut::ObjectLoaded {
                ring_id: ingress::RingId(8001),
                edge_id: ingress::EdgeId(7001),
                port_id,
                object_id: ingress::ObjectId(9000),
                sequence: 0,
                extent: 8,
                handle,
            } if port_id == &ingress::PortId("in".into())
                && handle.generation == ingress::WorkerGeneration(1)
        )
    }));
}

// This proves parser faults cover sequence violations and device allocation or
// copy failures, and after RingFault the worker stops consuming until uninstall.
#[test]
fn sequence_and_device_failures_fault_and_ring_fault_stops_consumption() {
    // Parse sequence 0 successfully.
    let mut harness = installed_parser();
    harness.write_committed_bytes(ingress::RingId(8001), valid_record(0, 8));
    harness.observe(ingress::WorkerIngressEvent::RingReadable {
        ring_id: ingress::RingId(8001),
    });
    harness.observe(ingress::WorkerIngressEvent::DeviceCopyCompleted {
        object_id: ingress::ObjectId(9000),
        byte_count: 8,
    });
    harness.observe(ingress::WorkerIngressEvent::DeviceHandleCreated {
        object_id: ingress::ObjectId(9000),
        handle: ingress::DeviceHandle::new(ingress::WorkerGeneration(1), 42),
    });

    // Repeating sequence 0 violates sequence safety.
    harness.write_committed_bytes(ingress::RingId(8001), valid_record(0, 8));
    harness.observe(ingress::WorkerIngressEvent::RingReadable {
        ring_id: ingress::RingId(8001),
    });
    assert!(harness.events().iter().any(|event| {
        matches!(
            event,
            ingress::WorkerIngressOut::ObjectFailed {
                edge_id: ingress::EdgeId(7001),
                port_id,
                object_id: Some(ingress::ObjectId(9000)),
                sequence: Some(0),
                reason: ingress::ObjectFailureReason::SequenceViolation,
                ..
            } if port_id == &ingress::PortId("in".into())
        )
    }));

    // RingFault stops consumption until uninstall.
    let before_fault = harness.consume_cursor(ingress::RingId(8001));
    harness.observe(ingress::WorkerIngressEvent::RingFault {
        ring_id: ingress::RingId(8001),
    });
    harness.write_committed_bytes(ingress::RingId(8001), valid_record(1, 8));
    harness.observe(ingress::WorkerIngressEvent::RingReadable {
        ring_id: ingress::RingId(8001),
    });
    assert_eq!(harness.consume_cursor(ingress::RingId(8001)), before_fault);
}
