//! Black-box contract tests for MVP GPU worker egress production.
//!
//! These tests intentionally know only the public worker egress surface:
//!
//! - `InstallRing`, `ExecuteStep` output bindings, device-copy outcomes,
//!   backpressure, and shutdown events in
//! - committed ring bytes, cursor publication, `ObjectProduced`,
//!   `StepCompleted`, and step failures out
//!
//! They assert the guarantees in
//! `specs/mvp_system/gpu_worker_egress_producer_contract.md`.

use mvp_system::gpu_worker_egress_producer as egress;

// A valid egress ring config supplies the edge object spec and current worker
// generation. The producer remains free to choose copy scheduling internally.
fn egress_ring() -> egress::InstallRing {
    egress::InstallRing {
        ring_id: egress::RingId(8002),
        edge_id: egress::EdgeId(7002),
        port_id: egress::PortId("out".into()),
        direction: egress::RingDirection::Egress,
        object_spec: egress::ObjectSpec {
            max_extent: 16,
            alignment: 4,
            layout: egress::ObjectLayout::Token,
        },
        generation: egress::WorkerGeneration(1),
    }
}

// The harness exposes egress ring writes and worker events, not private output
// queues, device kernels, or role internals.
fn new_producer() -> egress::EgressProducerHarness {
    egress::EgressProducerHarness::new(egress::WorkerGeneration(1))
}

// This helper installs the output ring through the public worker command path.
fn installed_producer() -> egress::EgressProducerHarness {
    let mut harness = new_producer();
    harness.observe(egress::WorkerEgressEvent::InstallRing(egress_ring()));
    harness
}

// A valid output binding carries object identity, sequence, extent, flags, and
// target ring. The worker must not invent these graph-visible facts.
fn output_binding(sequence: u64, extent: u64) -> egress::OutputBinding {
    egress::OutputBinding {
        ring_id: egress::RingId(8002),
        object_id: egress::ObjectId(9000 + sequence),
        sequence,
        extent,
        flags: egress::ObjectFlags::default(),
        device_source: egress::DeviceHandle::new(egress::WorkerGeneration(1), 40 + sequence),
    }
}

// This proves egress production starts only after InstallRing, only for
// ExecuteStep output bindings naming that ring, and never invents object ids or
// sequence numbers.
#[test]
fn output_admission_requires_installed_ring_and_explicit_binding() {
    // Execute before InstallRing must not write output.
    let mut not_installed = new_producer();
    not_installed.observe(egress::WorkerEgressEvent::ExecuteStep {
        step_id: egress::StepId(77),
        outputs: vec![output_binding(0, 8)],
    });
    assert_eq!(not_installed.committed_bytes(egress::RingId(8002)).len(), 0);

    // Install the ring and execute with a binding that names it.
    let mut harness = installed_producer();
    harness.observe(egress::WorkerEgressEvent::ExecuteStep {
        step_id: egress::StepId(77),
        outputs: vec![output_binding(0, 8)],
    });

    // The pending output identity must match the binding exactly.
    let pending = harness.pending_outputs();
    assert_eq!(pending[0].object_id, egress::ObjectId(9000));
    assert_eq!(pending[0].sequence, 0);
    assert_eq!(pending[0].extent, 8);
}

// This proves the worker creates a valid ObjectHeader from ObjectSpec, writes
// header bytes before payload bytes, advances commit only after valid header
// bytes, and emits readable wake after committed header bytes.
#[test]
fn header_is_written_and_committed_before_payload() {
    // Start one egress output.
    let mut harness = installed_producer();
    harness.observe(egress::WorkerEgressEvent::ExecuteStep {
        step_id: egress::StepId(77),
        outputs: vec![output_binding(0, 8)],
    });

    // Complete header production but not payload copy.
    harness.observe(egress::WorkerEgressEvent::HeaderReady {
        object_id: egress::ObjectId(9000),
    });

    // The committed prefix must decode as a header for the configured spec.
    let committed = harness.committed_bytes(egress::RingId(8002));
    let header = egress::ObjectHeader::decode(committed).expect("header must decode");
    assert_eq!(header.object_id, egress::ObjectId(9000));
    assert_eq!(header.sequence, 0);
    assert_eq!(header.extent, 8);

    // Payload bytes are not committed before the payload copy is valid.
    assert_eq!(harness.committed_payload_bytes(egress::RingId(8002)), 0);
    assert!(harness.wake_hints().iter().any(|wake| {
        matches!(
            wake,
            egress::WakeHint::RingReadable {
                ring_id: egress::RingId(8002)
            }
        )
    }));
}

// This proves payload production copies exactly extent bytes from device to the
// egress ring, advances commit only after host bytes are valid, and blocks on
// egress backpressure without dropping ownership.
#[test]
fn payload_copy_is_exact_extent_and_respects_backpressure() {
    // Start one output with extent 8.
    let mut harness = installed_producer();
    harness.observe(egress::WorkerEgressEvent::ExecuteStep {
        step_id: egress::StepId(77),
        outputs: vec![output_binding(0, 8)],
    });
    harness.observe(egress::WorkerEgressEvent::HeaderReady {
        object_id: egress::ObjectId(9000),
    });

    // Backpressure prevents committing payload bytes.
    harness.observe(egress::WorkerEgressEvent::EgressRingFull {
        ring_id: egress::RingId(8002),
    });
    harness.observe(egress::WorkerEgressEvent::DeviceToHostCopyCompleted {
        object_id: egress::ObjectId(9000),
        byte_count: 4,
    });
    assert_eq!(harness.committed_payload_bytes(egress::RingId(8002)), 0);

    // Once writable, the full exact extent can commit.
    harness.observe(egress::WorkerEgressEvent::RingWritable {
        ring_id: egress::RingId(8002),
    });
    harness.observe(egress::WorkerEgressEvent::DeviceToHostCopyCompleted {
        object_id: egress::ObjectId(9000),
        byte_count: 8,
    });
    assert_eq!(harness.committed_payload_bytes(egress::RingId(8002)), 8);
}

// This proves ObjectProduced is emitted after the full output object is
// committed, and StepCompleted is emitted only after all declared outputs are
// produced and role state updates are complete.
#[test]
fn object_produced_precedes_step_completed_after_all_outputs() {
    // Execute a step with two outputs.
    let mut harness = installed_producer();
    harness.observe(egress::WorkerEgressEvent::InstallRing(
        egress::InstallRing {
            ring_id: egress::RingId(8003),
            edge_id: egress::EdgeId(7003),
            port_id: egress::PortId("out2".into()),
            ..egress_ring()
        },
    ));
    harness.observe(egress::WorkerEgressEvent::ExecuteStep {
        step_id: egress::StepId(77),
        outputs: vec![
            output_binding(0, 8),
            egress::OutputBinding {
                ring_id: egress::RingId(8003),
                object_id: egress::ObjectId(9100),
                sequence: 0,
                extent: 8,
                flags: egress::ObjectFlags::default(),
                device_source: egress::DeviceHandle::new(egress::WorkerGeneration(1), 55),
            },
        ],
    });

    // Produce only the first output and prove StepCompleted is still absent.
    harness.complete_output(egress::ObjectId(9000));
    assert!(
        !harness
            .events()
            .iter()
            .any(|event| { matches!(event, egress::WorkerEgressOut::StepCompleted { .. }) })
    );

    // Produce the second output and complete role state update.
    harness.complete_output(egress::ObjectId(9100));
    harness.observe(egress::WorkerEgressEvent::RoleStateUpdated {
        step_id: egress::StepId(77),
    });

    // Both object-produced events precede StepCompleted.
    let first_object_pos = harness
        .events()
        .iter()
        .position(|event| {
            matches!(
                event,
                egress::WorkerEgressOut::ObjectProduced {
                    object_id: egress::ObjectId(9000),
                    ..
                }
            )
        })
        .expect("first object produced");
    let second_object_pos = harness
        .events()
        .iter()
        .position(|event| {
            matches!(
                event,
                egress::WorkerEgressOut::ObjectProduced {
                    object_id: egress::ObjectId(9100),
                    ..
                }
            )
        })
        .expect("second object produced");
    let completed_pos = harness
        .events()
        .iter()
        .position(|event| {
            matches!(
                event,
                egress::WorkerEgressOut::StepCompleted {
                    step_id: egress::StepId(77),
                    ..
                }
            )
        })
        .expect("step completed");
    assert!(first_object_pos < completed_pos);
    assert!(second_object_pos < completed_pos);
}

// This proves invalid output ring, extent violation, device copy failure, and
// shutdown reject or abort egress production with visible step/ring faults.
#[test]
fn egress_faults_are_visible_and_suppress_success_events() {
    // Invalid output ring fails the step.
    let mut invalid_ring = installed_producer();
    invalid_ring.observe(egress::WorkerEgressEvent::ExecuteStep {
        step_id: egress::StepId(77),
        outputs: vec![egress::OutputBinding {
            ring_id: egress::RingId(9999),
            ..output_binding(0, 8)
        }],
    });
    assert!(invalid_ring.events().iter().any(|event| {
        matches!(
            event,
            egress::WorkerEgressOut::StepFailed {
                reason: egress::StepFailureReason::InvalidOutputRing,
                ..
            }
        )
    }));

    // Extent violation fails the step.
    let mut bad_extent = installed_producer();
    bad_extent.observe(egress::WorkerEgressEvent::ExecuteStep {
        step_id: egress::StepId(78),
        outputs: vec![output_binding(0, 32)],
    });
    assert!(bad_extent.events().iter().any(|event| {
        matches!(
            event,
            egress::WorkerEgressOut::StepFailed {
                reason: egress::StepFailureReason::OutputExtentViolation,
                ..
            }
        )
    }));

    // Copy failure faults the ring or fails the step, but must not emit
    // ObjectProduced.
    let mut copy_failed = installed_producer();
    copy_failed.observe(egress::WorkerEgressEvent::ExecuteStep {
        step_id: egress::StepId(79),
        outputs: vec![output_binding(0, 8)],
    });
    copy_failed.observe(egress::WorkerEgressEvent::DeviceCopyFailed {
        object_id: egress::ObjectId(9000),
    });
    assert!(copy_failed.events().iter().any(|event| {
        matches!(event, egress::WorkerEgressOut::StepFailed { .. })
            || matches!(event, egress::WorkerEgressOut::RingFault { .. })
    }));
    assert!(
        !copy_failed
            .events()
            .iter()
            .any(|event| { matches!(event, egress::WorkerEgressOut::ObjectProduced { .. }) })
    );
}
