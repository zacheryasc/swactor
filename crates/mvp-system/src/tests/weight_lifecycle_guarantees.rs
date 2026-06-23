//! Black-box contract tests for MVP stage-local weight lifecycle.
//!
//! These tests intentionally know only the public weight-work surface:
//!
//! - `ProvisionStage` assignment in
//! - artifact, parse, allocation, binding, and cache outcomes in
//! - `WeightsReady`, `StageReady`, and `StageFault` out
//!
//! They assert the guarantees in
//! `specs/mvp_system/weight_lifecycle_contract.md`.

use mvp_system::weight_lifecycle as weights;

// A valid assignment gives the stage exactly one layer range and one source.
// Tests vary only source or failure outcome so the assignment contract remains
// visible.
fn valid_assignment() -> weights::WeightAssignment {
    weights::WeightAssignment {
        run_id: weights::RunId(7),
        stage_index: 1,
        plan_layer_range: weights::LayerRange {
            start: 12,
            end_exclusive: 24,
        },
        assigned_layer_range: weights::LayerRange {
            start: 12,
            end_exclusive: 24,
        },
        source: weights::WeightSource::WholeGguf {
            uri: "test://model.gguf".into(),
        },
    }
}

// Weight loading is intentionally opaque. The harness accepts public loader and
// worker outcomes and records only stage-visible events and commands.
fn new_weight_harness() -> weights::WeightLifecycleHarness {
    weights::WeightLifecycleHarness::new(weights::NodeId(11))
}

// The success facts represent the observable prerequisites for WeightsReady:
// artifact bytes exist, the assigned layer range is valid, and the worker has
// loaded or bound that range.
fn successful_load_events() -> Vec<weights::WeightEvent> {
    vec![
        weights::WeightEvent::ArtifactAvailable {
            bytes: weights::ArtifactBytes::Local,
        },
        weights::WeightEvent::LayerRangeValidated,
        weights::WeightEvent::WorkerRangeBound,
    ]
}

// Each failure case maps one public loader or worker failure to the stable
// stage fault reason expected at the control boundary.
fn failure_cases() -> Vec<(weights::WeightEvent, weights::StageFaultReason)> {
    vec![
        (
            weights::WeightEvent::DownloadFailed,
            weights::StageFaultReason::WeightDownloadFailed,
        ),
        (
            weights::WeightEvent::ParseFailed,
            weights::StageFaultReason::WeightParseFailed,
        ),
        (
            weights::WeightEvent::DeviceAllocationFailed,
            weights::StageFaultReason::DeviceAllocationFailed,
        ),
        (
            weights::WeightEvent::BindingFailed,
            weights::StageFaultReason::WeightBindingFailed,
        ),
        (
            weights::WeightEvent::InvalidLayerRange,
            weights::StageFaultReason::InvalidLayerRange,
        ),
    ]
}

// This proves a stage receives weight source and exactly one assigned layer
// range from provisioning, validates it against the plan, and does not claim
// graph-visible ownership outside that range.
#[test]
fn assignment_is_stage_local_and_range_limited() {
    // Start weight work from the provisioned assignment.
    let mut harness = new_weight_harness();
    harness.observe(weights::WeightEvent::Provisioned(valid_assignment()));

    // The load command may use the physical source, but its graph-visible layer
    // range must be the assigned range.
    for command in harness.commands() {
        if let weights::WeightCommand::LoadOrBindRange { range, .. } = command {
            assert_eq!(
                *range,
                weights::LayerRange {
                    start: 12,
                    end_exclusive: 24,
                }
            );
        }
    }

    // There must be no command claiming ownership of neighboring layers.
    assert!(!harness.commands().iter().any(|command| {
        matches!(
            command,
            weights::WeightCommand::AdvertiseLoadedLayerRange {
                range,
                ..
            } if range.start < 12 || range.end_exclusive > 24
        )
    }));
}

// This proves whole GGUF download, shard download, and cache use are physical
// mechanisms with the same public outcome: WeightsReady or StageFault.
#[test]
fn supported_physical_sources_have_same_visible_success_contract() {
    // Exercise every supported source without asserting how bytes are obtained.
    let sources = vec![
        weights::WeightSource::WholeGguf {
            uri: "test://model.gguf".into(),
        },
        weights::WeightSource::ShardSet {
            uris: vec!["test://model.layers.12-24.gguf".into()],
        },
        weights::WeightSource::CachedArtifact {
            cache_key: "model:layers:12-24".into(),
        },
    ];

    for source in sources {
        // Install the source in an otherwise valid assignment.
        let mut assignment = valid_assignment();
        assignment.source = source;
        let mut harness = new_weight_harness();
        harness.observe(weights::WeightEvent::Provisioned(assignment));

        // Drive the same public success facts for every source.
        for event in successful_load_events() {
            harness.observe(event);
        }

        // The system-visible success outcome is WeightsReady.
        assert!(harness.events().iter().any(|event| {
            matches!(
                event,
                weights::WeightLifecycleEvent::WeightsReady {
                    run_id: weights::RunId(7),
                    stage_index: 1,
                }
            )
        }));
    }
}

// This proves WeightsReady requires artifact availability, layer validation,
// and worker bind/load completion, and that WeightsReady precedes StageReady.
#[test]
fn weights_ready_requires_all_weight_facts_and_precedes_stage_ready() {
    // Start from a valid assignment.
    let mut harness = new_weight_harness();
    harness.observe(weights::WeightEvent::Provisioned(valid_assignment()));

    // Feed every success fact except the final one and prove no prefix is
    // sufficient for WeightsReady.
    let mut events = successful_load_events();
    let final_event = events.pop().expect("fixture has final weight event");
    for event in events {
        harness.observe(event);
        assert!(
            !harness.events().iter().any(|event| {
                matches!(event, weights::WeightLifecycleEvent::WeightsReady { .. })
            })
        );
    }

    // The final weight prerequisite emits WeightsReady.
    harness.observe(final_event);
    let weights_ready_pos = harness
        .events()
        .iter()
        .position(|event| matches!(event, weights::WeightLifecycleEvent::WeightsReady { .. }))
        .expect("WeightsReady must be emitted");

    // StageReady may occur only after the StageController observes WeightsReady
    // and the other local setup prerequisites.
    harness.observe(weights::WeightEvent::OtherStagePrerequisitesReady);
    let stage_ready_pos = harness
        .events()
        .iter()
        .position(|event| matches!(event, weights::WeightLifecycleEvent::StageReady { .. }))
        .expect("StageReady must be emitted after prerequisites");
    assert!(weights_ready_pos < stage_ready_pos);
}

// This proves every weight failure source faults the stage and suppresses both
// WeightsReady and StageReady.
#[test]
fn weight_failures_emit_stage_fault_without_readiness() {
    // Each failure source gets an isolated attempt.
    for (failure_event, expected_reason) in failure_cases() {
        let mut harness = new_weight_harness();
        harness.observe(weights::WeightEvent::Provisioned(valid_assignment()));

        // Deliver the public failure outcome from weight work.
        harness.observe(failure_event);

        // The stable fault reason must be observable.
        assert!(harness.events().iter().any(|event| {
            matches!(
                event,
                weights::WeightLifecycleEvent::StageFault {
                    reason,
                    ..
                } if *reason == expected_reason
            )
        }));

        // Readiness cannot also be emitted after a faulted weight attempt.
        assert!(!harness.events().iter().any(|event| {
            matches!(event, weights::WeightLifecycleEvent::WeightsReady { .. })
                || matches!(event, weights::WeightLifecycleEvent::StageReady { .. })
        }));
    }
}
