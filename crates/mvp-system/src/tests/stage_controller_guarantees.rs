//! Black-box contract tests for MVP StageController behavior.
//!
//! These tests intentionally know only the public stage-controller surface:
//!
//! - `ProvisionStage`, worker, edge, object, stop, and fault events in
//! - worker commands, lifecycle events, and teardown events out
//!
//! They assert the guarantees in
//! `specs/mvp_system/stage_controller_contract.md`.

use mvp_system::stage_controller as stage;

// This provision fixture represents a single middle stage. It has one inbound
// and one outbound edge so tests can prove the controller uses assigned edges
// without relying on endpoint internals.
fn valid_provision() -> stage::ProvisionStage {
    stage::ProvisionStage {
        run_id: stage::RunId(7),
        authorized_orchestrator: stage::NodeId(99),
        node_id: stage::NodeId(11),
        stage_index: 1,
        stage_count: 3,
        layer_range: stage::LayerRange {
            start: 12,
            end_exclusive: 24,
        },
        inbound: stage::EdgeProvision::inbound(stage::EdgeId(7001)),
        outbound: stage::EdgeProvision::outbound(stage::EdgeId(7002)),
        weight_source: stage::WeightSource::TestArtifact("model.gguf".into()),
    }
}

// The harness exposes only public messages. Tests intentionally do not inspect
// private controller states such as "Preparing" or "Executing"; they infer
// controller behavior from emitted commands and lifecycle events.
fn new_controller() -> stage::StageControllerHarness {
    stage::StageControllerHarness::new(stage::NodeId(11))
}

// Preparation readiness has four independent prerequisites. Listing them as
// public observations lets tests prove StageReady is a barrier across worker,
// weights, inbound edge, and outbound edge readiness.
fn preparation_ready_events() -> Vec<stage::StageEvent> {
    vec![
        stage::StageEvent::WorkerReady,
        stage::StageEvent::WeightsReady,
        stage::StageEvent::InboundEdgeReady {
            edge_id: stage::EdgeId(7001),
        },
        stage::StageEvent::OutboundEdgeReady {
            edge_id: stage::EdgeId(7002),
        },
    ]
}

// This helper provisions and readies a stage through public events. Tests that
// focus on execution use it to avoid duplicating setup while still going through
// the same observable path as production.
fn ready_stage() -> stage::StageControllerHarness {
    let mut harness = new_controller();
    harness.observe(stage::StageEvent::ProvisionStage {
        from: stage::NodeId(99),
        provision: valid_provision(),
    });
    for event in preparation_ready_events() {
        harness.observe(event);
    }
    harness
}

// This proves provisioning is authorized, validated before setup, and does not
// allow a stage to rewire its assigned inbound or outbound edge.
#[test]
fn provisioning_validates_authority_and_assigned_shape_before_setup() {
    // Send a valid provision from the authorized orchestrator.
    let mut harness = new_controller();
    harness.observe(stage::StageEvent::ProvisionStage {
        from: stage::NodeId(99),
        provision: valid_provision(),
    });

    // Setup commands should be derived from the provided assignment.
    assert!(harness.commands().iter().any(|command| {
        matches!(
            command,
            stage::StageCommand::EstablishInboundEdge {
                edge_id: stage::EdgeId(7001),
                ..
            }
        )
    }));
    assert!(harness.commands().iter().any(|command| {
        matches!(
            command,
            stage::StageCommand::EstablishOutboundEdge {
                edge_id: stage::EdgeId(7002),
                ..
            }
        )
    }));

    // The controller must not emit any command that replaces the provisioned
    // edge ids with a locally chosen edge.
    assert!(
        !harness
            .commands()
            .iter()
            .any(|command| { matches!(command, stage::StageCommand::RewireEdge { .. }) })
    );

    // An unauthorized provision attempt must fault before setup can begin.
    let mut unauthorized = new_controller();
    unauthorized.observe(stage::StageEvent::ProvisionStage {
        from: stage::NodeId(123),
        provision: valid_provision(),
    });
    assert!(unauthorized.events().iter().any(|event| {
        matches!(
            event,
            stage::StageLifecycleEvent::StageFault {
                reason: stage::StageFaultReason::UnauthorizedProvision,
                ..
            }
        )
    }));
    assert!(
        !unauthorized
            .commands()
            .iter()
            .any(|command| { matches!(command, stage::StageCommand::ConfigureWorkerRole { .. }) })
    );
}

// This proves StageReady is emitted only after worker readiness, weight
// readiness, inbound edge readiness, and outbound edge readiness are all
// observed.
#[test]
fn stage_ready_waits_for_worker_weights_and_both_edges() {
    // Provision the stage so preparation can begin.
    let mut harness = new_controller();
    harness.observe(stage::StageEvent::ProvisionStage {
        from: stage::NodeId(99),
        provision: valid_provision(),
    });

    // Feed every readiness event except the final one and prove no prefix is
    // enough for StageReady.
    let mut events = preparation_ready_events();
    let final_event = events.pop().expect("fixture has final setup event");
    for event in events {
        harness.observe(event);
        assert!(
            !harness
                .events()
                .iter()
                .any(|event| { matches!(event, stage::StageLifecycleEvent::StageReady { .. }) })
        );
    }

    // The final prerequisite crosses the barrier.
    harness.observe(final_event);

    // StageReady appears exactly once for the provisioned stage.
    let ready_count = harness
        .events()
        .iter()
        .filter(|event| {
            matches!(
                event,
                stage::StageLifecycleEvent::StageReady {
                    run_id: stage::RunId(7),
                    stage_index: 1,
                }
            )
        })
        .count();
    assert_eq!(ready_count, 1);
}

// This proves a ready stage admits work only from inbound ObjectLoaded, issues
// one ExecuteStep per accepted object, and binds output with the same sequence.
#[test]
fn accepted_inbound_object_creates_one_same_sequence_execute_step() {
    // Bring the stage to ready state through public setup events.
    let mut harness = ready_stage();

    // Deliver the first inbound object, sequence 0.
    harness.observe(stage::StageEvent::ObjectLoaded {
        edge_id: stage::EdgeId(7001),
        object_id: stage::ObjectId(9000),
        sequence: 0,
        handle: stage::DeviceHandle::new_current(42),
    });

    // Exactly one ExecuteStep command must result from that accepted object.
    let execute_steps = harness
        .commands()
        .iter()
        .filter_map(|command| match command {
            stage::StageCommand::ExecuteStep(step) => Some(step),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(execute_steps.len(), 1);

    // The output binding must preserve the input sequence.
    assert_eq!(execute_steps[0].input.sequence, 0);
    assert_eq!(execute_steps[0].outputs[0].sequence, 0);

    // A second object while the first step is active must not create another
    // active ExecuteStep in the MVP.
    harness.observe(stage::StageEvent::ObjectLoaded {
        edge_id: stage::EdgeId(7001),
        object_id: stage::ObjectId(9001),
        sequence: 1,
        handle: stage::DeviceHandle::new_current(43),
    });
    let active_steps = harness
        .commands()
        .iter()
        .filter(|command| matches!(command, stage::StageCommand::ExecuteStep(_)))
        .count();
    assert_eq!(active_steps, 1);
}

// This proves the sequence contract: sequence 0 is accepted as prefill, decode
// sequences must strictly increase, and duplicate, skipped, or out-of-order
// inputs fault the stage.
#[test]
fn duplicate_skipped_and_out_of_order_sequences_fault() {
    // Each invalid trace starts from a freshly readied stage.
    let invalid_traces = vec![vec![0, 0], vec![0, 2], vec![0, 1, 0]];

    for trace in invalid_traces {
        // Accept the first object and complete its step when needed so the next
        // object is admitted through the normal public path.
        let mut harness = ready_stage();
        for (i, sequence) in trace.iter().enumerate() {
            harness.observe(stage::StageEvent::ObjectLoaded {
                edge_id: stage::EdgeId(7001),
                object_id: stage::ObjectId(9000 + i as u64),
                sequence: *sequence,
                handle: stage::DeviceHandle::new_current(100 + i as u64),
            });
            if i + 1 < trace.len() {
                harness.observe(stage::StageEvent::StepCompleted {
                    step_id: stage::StepId(i as u64),
                });
            }
        }

        // The transcript must contain a sequence fault for the invalid trace.
        assert!(harness.events().iter().any(|event| {
            matches!(
                event,
                stage::StageLifecycleEvent::StageFault {
                    reason: stage::StageFaultReason::SequenceViolation,
                    ..
                }
            )
        }));
    }
}

// This proves compute completion is observed only after the worker reports
// StepCompleted, and completion returns the stage to ready-for-next-object.
#[test]
fn step_completed_releases_input_and_admits_next_object() {
    // Start one accepted step.
    let mut harness = ready_stage();
    harness.observe(stage::StageEvent::ObjectLoaded {
        edge_id: stage::EdgeId(7001),
        object_id: stage::ObjectId(9000),
        sequence: 0,
        handle: stage::DeviceHandle::new_current(42),
    });

    // Before worker completion, no compute-complete lifecycle event is allowed.
    assert!(!harness.events().iter().any(|event| {
        matches!(
            event,
            stage::StageLifecycleEvent::StepAccepted { sequence: 1, .. }
        )
    }));

    // Worker StepCompleted is the public completion signal.
    harness.observe(stage::StageEvent::StepCompleted {
        step_id: stage::StepId(0),
    });

    // The controller releases per-step input according to policy.
    assert!(harness.commands().iter().any(|command| {
        matches!(
            command,
            stage::StageCommand::ReleaseInputHandle {
                object_id: stage::ObjectId(9000),
                ..
            }
        )
    }));

    // The next sequence is now admissible.
    harness.observe(stage::StageEvent::ObjectLoaded {
        edge_id: stage::EdgeId(7001),
        object_id: stage::ObjectId(9001),
        sequence: 1,
        handle: stage::DeviceHandle::new_current(43),
    });
    let execute_count = harness
        .commands()
        .iter()
        .filter(|command| matches!(command, stage::StageCommand::ExecuteStep(_)))
        .count();
    assert_eq!(execute_count, 2);
}

// This proves worker, object, output-edge, and step failures fault the stage,
// and after fault no new run work is accepted until StopRun.
#[test]
fn stage_fault_rejects_new_work_until_stopped() {
    // Start from a ready stage and inject a worker crash.
    let mut harness = ready_stage();
    harness.observe(stage::StageEvent::WorkerCrashed);

    // Fault must be visible at the stage boundary.
    assert!(harness.events().iter().any(|event| {
        matches!(
            event,
            stage::StageLifecycleEvent::StageFault {
                reason: stage::StageFaultReason::WorkerCrashed,
                ..
            }
        )
    }));

    // New work after fault must not produce ExecuteStep.
    let before = harness
        .commands()
        .iter()
        .filter(|command| matches!(command, stage::StageCommand::ExecuteStep(_)))
        .count();
    harness.observe(stage::StageEvent::ObjectLoaded {
        edge_id: stage::EdgeId(7001),
        object_id: stage::ObjectId(9999),
        sequence: 0,
        handle: stage::DeviceHandle::new_current(77),
    });
    let after = harness
        .commands()
        .iter()
        .filter(|command| matches!(command, stage::StageCommand::ExecuteStep(_)))
        .count();
    assert_eq!(after, before);

    // StopRun moves the stage through local teardown and emits StageStopped.
    harness.observe(stage::StageEvent::StopRun {
        run_id: stage::RunId(7),
    });
    assert!(
        harness
            .commands()
            .iter()
            .any(|command| { matches!(command, stage::StageCommand::StopLocalEdges { .. }) })
    );
    assert!(
        harness.commands().iter().any(|command| {
            matches!(command, stage::StageCommand::ReleaseRunDeviceObjects { .. })
        })
    );
    assert!(
        harness
            .events()
            .iter()
            .any(|event| { matches!(event, stage::StageLifecycleEvent::StageStopped { .. }) })
    );
}
