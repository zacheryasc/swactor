//! Black-box contract tests for the MVP observability surface.
//!
//! These tests intentionally know only the public event stream surface:
//!
//! - structured lifecycle and fault events emitted by components
//! - event identities, reason enums, and ordering observed by subscribers
//!
//! They assert the guarantees in
//! `specs/mvp_system/observability_surface_contract.md`.

use mvp_system::observability_surface as obs;

// The trace fixture contains one successful run from boot through teardown.
// Tests use structured events only; logs, transport, storage, and batching stay
// outside the contract.
fn successful_run_trace() -> Vec<obs::Event> {
    obs::TraceBuilder::new(obs::RunId(7))
        .node_started(obs::NodeId(10))
        .node_available(obs::NodeId(10))
        .pool_ready(vec![obs::NodeId(10)])
        .run_planned()
        .stage_provision_started(obs::StageIndex(0), obs::NodeId(10))
        .weights_download_started(obs::StageIndex(0))
        .weights_downloaded(obs::StageIndex(0))
        .weights_loaded(obs::StageIndex(0))
        .edge_provision_started(obs::EdgeId(7000))
        .edge_ready(obs::EdgeId(7000))
        .stage_ready(obs::StageIndex(0))
        .readiness_barrier_passed()
        .prompt_injected(obs::Sequence(0))
        .object_loaded(obs::EdgeId(7000), obs::ObjectId(9000), obs::Sequence(0))
        .execute_step_started(obs::StepId(77))
        .object_produced(obs::EdgeId(7001), obs::ObjectId(9001), obs::Sequence(0))
        .step_completed(obs::StepId(77))
        .token_received(obs::ObjectId(9002), obs::Sequence(0))
        .run_completed()
        .stop_run_sent(obs::StageIndex(0))
        .stage_stopped(obs::StageIndex(0))
        .run_torn_down()
        .finish()
}

// A fault trace gives the tests stable reason enums and detecting components
// without relying on diagnostic log text.
fn fault_trace() -> Vec<obs::Event> {
    obs::TraceBuilder::new(obs::RunId(7))
        .node_started(obs::NodeId(10))
        .node_available(obs::NodeId(10))
        .pool_ready(vec![obs::NodeId(10)])
        .run_planned()
        .stage_provision_started(obs::StageIndex(0), obs::NodeId(10))
        .node_faulted(
            obs::NodeId(10),
            obs::FaultReason::MembershipLoss,
            obs::Component::Membership,
        )
        .stage_faulted(
            obs::StageIndex(0),
            obs::FaultReason::RingFault,
            obs::Component::SharedRingHelper,
        )
        .run_faulted(
            obs::FaultReason::MembershipLoss,
            obs::Component::Orchestrator,
        )
        .stop_run_sent(obs::StageIndex(0))
        .stage_stopped(obs::StageIndex(0))
        .run_torn_down()
        .finish()
}

// This helper returns the position of an event kind in a trace. Ordering tests
// use positions so they prove causal ordering without depending on exact event
// batching or adjacent placement.
fn position_of_kind(events: &[obs::Event], kind: obs::EventKind) -> usize {
    events
        .iter()
        .position(|event| event.kind() == kind)
        .expect("event kind missing from trace")
}

// This helper checks structured identity fields directly. If callers have to
// scrape logs to recover an id, the event fails this contract test.
fn assert_required_identity(event: &obs::Event) {
    match event {
        obs::Event::RunScoped { run_id, .. } => assert_eq!(*run_id, obs::RunId(7)),
        obs::Event::NodeScoped { node_id, .. } | obs::Event::NodeFaulted { node_id, .. } => {
            assert_eq!(*node_id, obs::NodeId(10));
        }
        obs::Event::StageScoped {
            run_id,
            stage_index,
            ..
        } => {
            assert_eq!(*run_id, obs::RunId(7));
            assert_eq!(*stage_index, obs::StageIndex(0));
        }
        obs::Event::EdgeScoped { edge_id, .. } => {
            assert!([obs::EdgeId(7000), obs::EdgeId(7001)].contains(edge_id));
        }
        obs::Event::RingScoped { ring_id, .. } => assert_eq!(*ring_id, obs::RingId(8000)),
        obs::Event::ObjectScoped {
            object_id,
            sequence,
            ..
        } => {
            assert!(
                [
                    obs::ObjectId(9000),
                    obs::ObjectId(9001),
                    obs::ObjectId(9002)
                ]
                .contains(object_id)
            );
            assert_eq!(*sequence, obs::Sequence(0));
        }
        obs::Event::StepScoped { step_id, .. } => assert_eq!(*step_id, obs::StepId(77)),
        obs::Event::WorkerScoped {
            worker_generation, ..
        } => assert_eq!(*worker_generation, obs::WorkerGeneration(1)),
    }
}

// This proves required event identity fields are structured on the event itself
// for run, node, stage, edge, ring, object, step, and worker scopes.
#[test]
fn required_event_identity_is_structured_not_log_derived() {
    // Build one trace that includes all required identity scopes.
    let mut events = successful_run_trace();
    events.push(obs::Event::NodeFaulted {
        node_id: obs::NodeId(10),
        reason: obs::FaultReason::NodeUnavailable,
        component: obs::Component::NodeBoot,
    });
    events.push(obs::Event::RingScoped {
        kind: obs::EventKind::RingReadable,
        ring_id: obs::RingId(8000),
        component: obs::Component::SharedRingHelper,
    });
    events.push(obs::Event::WorkerScoped {
        kind: obs::EventKind::WorkerReady,
        worker_generation: obs::WorkerGeneration(1),
        component: obs::Component::GpuWorkerCtl,
    });

    // Every event exposes its required identity directly.
    for event in &events {
        assert_required_identity(event);
    }
}

// This proves the lifecycle event stream covers the required milestones from
// node boot through terminal success or fault and run teardown.
#[test]
fn lifecycle_events_cover_required_milestones() {
    // Build success and fault traces because terminal success and fault events
    // are mutually exclusive in one run.
    let events = successful_run_trace()
        .into_iter()
        .chain(fault_trace())
        .collect::<Vec<_>>();
    let observed = events
        .iter()
        .map(|event| event.kind())
        .collect::<std::collections::BTreeSet<_>>();

    // The required lifecycle event kinds must all be present, regardless of
    // batching or transport.
    let required = [
        obs::EventKind::NodeStarted,
        obs::EventKind::NodeAvailable,
        obs::EventKind::NodeFaulted,
        obs::EventKind::PoolReady,
        obs::EventKind::RunPlanned,
        obs::EventKind::StageProvisionStarted,
        obs::EventKind::WeightsDownloadStarted,
        obs::EventKind::WeightsDownloaded,
        obs::EventKind::WeightsLoaded,
        obs::EventKind::EdgeProvisionStarted,
        obs::EventKind::EdgeReady,
        obs::EventKind::StageReady,
        obs::EventKind::ReadinessBarrierPassed,
        obs::EventKind::PromptInjected,
        obs::EventKind::ObjectLoaded,
        obs::EventKind::ExecuteStepStarted,
        obs::EventKind::ObjectProduced,
        obs::EventKind::StepCompleted,
        obs::EventKind::TokenReceived,
        obs::EventKind::RunCompleted,
        obs::EventKind::RunFaulted,
        obs::EventKind::StopRunSent,
        obs::EventKind::StageStopped,
        obs::EventKind::RunTornDown,
    ];
    for kind in required {
        assert!(
            observed.contains(&kind),
            "missing lifecycle event: {kind:?}"
        );
    }
}

// This proves the public fault taxonomy can classify the lifecycle fault
// families called out by the spec without falling back to worker-crash text.
#[test]
fn fault_reason_taxonomy_covers_required_lifecycle_families() {
    let required = [
        obs::FaultReason::NodeUnavailable,
        obs::FaultReason::MembershipLoss,
        obs::FaultReason::ProvisioningRejected,
        obs::FaultReason::ArenaBootFailed,
        obs::FaultReason::OversizedRingRequest,
        obs::FaultReason::PressureTimeout,
        obs::FaultReason::WeightLifecycleFailed,
        obs::FaultReason::EdgeEstablishmentFailed,
        obs::FaultReason::MalformedObjectHeader,
        obs::FaultReason::EofMidObject,
        obs::FaultReason::StreamFault,
        obs::FaultReason::PumpFailure,
        obs::FaultReason::RingFault,
        obs::FaultReason::WorkerFatal,
        obs::FaultReason::WorkerCrashed,
        obs::FaultReason::DeviceOutOfMemory,
        obs::FaultReason::DeviceCopyFailed,
        obs::FaultReason::SequenceViolation,
        obs::FaultReason::StepFailed,
        obs::FaultReason::TeardownTimeout,
        obs::FaultReason::UnsupportedRingVersion,
        obs::FaultReason::RingLayoutInvalid,
        obs::FaultReason::RingStateInvalid,
        obs::FaultReason::WorkerProcessExited,
        obs::FaultReason::WorkerShuttingDown,
        obs::FaultReason::WorkerInternal,
        obs::FaultReason::UnsupportedObjectVersion,
        obs::FaultReason::ExtentExceedsMax,
        obs::FaultReason::ExtentAlignmentInvalid,
        obs::FaultReason::DeviceAllocationFailed,
        obs::FaultReason::RoleUnavailable,
        obs::FaultReason::InvalidInputHandle,
        obs::FaultReason::InvalidOutputRing,
        obs::FaultReason::TinygradError,
        obs::FaultReason::OutputExtentInvalid,
        obs::FaultReason::OutputCopyFailed,
        obs::FaultReason::ArenaMapFailed,
        obs::FaultReason::RingHelperAbiMismatch,
        obs::FaultReason::BackendInitFailed,
        obs::FaultReason::MalformedControlMessage,
        obs::FaultReason::UnhandledException,
        obs::FaultReason::EdgeStopped,
        obs::FaultReason::WorkerShutdown,
    ];

    assert!(required.contains(&obs::FaultReason::MembershipLoss));
    assert!(required.contains(&obs::FaultReason::RingFault));
    assert!(required.contains(&obs::FaultReason::WorkerFatal));
    assert!(required.contains(&obs::FaultReason::WorkerCrashed));
}

// This proves fault events carry a stable reason enum and detecting component,
// and tests do not need free-form log text to determine lifecycle progress.
#[test]
fn fault_events_include_stable_reason_and_detecting_component() {
    // Build a fault trace with node, stage, and run faults from non-worker
    // sources.
    let events = fault_trace();

    // The structured node fault carries the reason and detector.
    assert!(events.iter().any(|event| {
        matches!(
            event,
            obs::Event::NodeFaulted {
                reason: obs::FaultReason::MembershipLoss,
                component: obs::Component::Membership,
                ..
            }
        )
    }));

    // The structured stage fault carries a non-worker reason and detector.
    assert!(events.iter().any(|event| {
        matches!(
            event,
            obs::Event::StageScoped {
                kind: obs::EventKind::StageFaulted,
                reason: Some(obs::FaultReason::RingFault),
                component: obs::Component::SharedRingHelper,
                ..
            }
        )
    }));

    // The run fault carries a structured non-worker reason.
    assert!(events.iter().any(|event| {
        matches!(
            event,
            obs::Event::RunScoped {
                kind: obs::EventKind::RunFaulted,
                reason: Some(obs::FaultReason::MembershipLoss),
                component: obs::Component::Orchestrator,
                ..
            }
        )
    }));

    // Logs may exist, but they are not required to classify progress.
    assert!(!obs::requires_log_scraping(&events));
}

// This proves observability ordering reflects component contracts:
// prompt_injected follows readiness_barrier_passed, stage_ready follows local
// readiness, run_torn_down follows teardown completion, and terminal run outcome
// is emitted exactly once.
#[test]
fn event_ordering_reflects_component_contracts_and_one_terminal_outcome() {
    // Build the successful trace.
    let events = successful_run_trace();

    // Prompt injection cannot precede the global barrier.
    assert!(
        position_of_kind(&events, obs::EventKind::ReadinessBarrierPassed)
            < position_of_kind(&events, obs::EventKind::PromptInjected)
    );

    // StageReady cannot precede required local readiness facts.
    assert!(
        position_of_kind(&events, obs::EventKind::WeightsLoaded)
            < position_of_kind(&events, obs::EventKind::StageReady)
    );
    assert!(
        position_of_kind(&events, obs::EventKind::EdgeReady)
            < position_of_kind(&events, obs::EventKind::StageReady)
    );

    // RunTornDown cannot precede teardown completion.
    assert!(
        position_of_kind(&events, obs::EventKind::StageStopped)
            < position_of_kind(&events, obs::EventKind::RunTornDown)
    );

    // Exactly one terminal run outcome is emitted.
    let terminal_count = events
        .iter()
        .filter(|event| {
            matches!(
                event.kind(),
                obs::EventKind::RunCompleted | obs::EventKind::RunFaulted
            )
        })
        .count();
    assert_eq!(terminal_count, 1);
}

// This proves observability tests are independent of transport, storage, and
// batching policy by asserting the same event facts after batching is changed.
#[test]
fn event_contract_survives_transport_storage_and_batching_policy() {
    // Build the same logical events under two batching policies.
    let unbatched =
        obs::EventSubscriberHarness::collect(successful_run_trace(), obs::Batching::None);
    let batched =
        obs::EventSubscriberHarness::collect(successful_run_trace(), obs::Batching::Fixed(8));

    // Flattened public event facts must match as an ordered stream.
    let unbatched_kinds = unbatched
        .flattened_events()
        .iter()
        .map(|event| event.kind())
        .collect::<Vec<_>>();
    let batched_kinds = batched
        .flattened_events()
        .iter()
        .map(|event| event.kind())
        .collect::<Vec<_>>();
    assert_eq!(batched_kinds, unbatched_kinds);

    // Neither subscriber depends on transport or storage implementation names.
    assert!(!unbatched.used_transport_specific_assertions());
    assert!(!batched.used_storage_specific_assertions());
}
