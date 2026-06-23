//! Black-box contract tests for MVP node boot lifecycle.
//!
//! These tests intentionally know only the public node-boot surface:
//!
//! - `BootHarness::launch(config)`
//! - public resource outcomes delivered into the harness
//! - lifecycle events and provisioning admission observed from the harness
//!
//! They assert the guarantees in
//! `specs/mvp_system/node_boot_lifecycle_contract.md`.

use mvp_system::node_boot_lifecycle as boot;

// A complete boot config lets the tests focus on ordering and failure behavior.
// The concrete process, transport, arena, worker, and SWIM implementations are
// outside this contract; the harness exposes only their observable outcomes.
fn valid_boot_config() -> boot::BootConfig {
    boot::BootConfig {
        node_id: boot::NodeId(10),
        intended_pool_id: boot::PoolId("mvp-test-pool".into()),
        worker_policy: boot::WorkerStartPolicy::StartBeforeAvailable,
    }
}

// The required facts are deliberately listed as public resource outcomes. That
// makes the proof about the boot barrier rather than about any private boot FSM
// state or the order in which an implementation happens to initialize resources.
fn required_readiness_facts() -> Vec<boot::ResourceOutcome> {
    vec![
        boot::ResourceOutcome::RustProcessAlive,
        boot::ResourceOutcome::RuntimeAcceptingControl,
        boot::ResourceOutcome::StableNodeIdKnown(boot::NodeId(10)),
        boot::ResourceOutcome::ArenaMapped,
        boot::ResourceOutcome::GpuWorkerReady,
        boot::ResourceOutcome::TransportEndpointBound,
        boot::ResourceOutcome::SwimJoiningPool,
        boot::ResourceOutcome::ProvisioningReceiverOpen,
    ]
}

// Fault injection is one-per-resource so each failure must be explained by the
// resource that failed. This prevents a broad "boot failed" bucket from hiding
// which readiness fact was not satisfied.
fn boot_fault_cases() -> Vec<(boot::ResourceOutcome, boot::BootFaultKind)> {
    vec![
        (
            boot::ResourceOutcome::ArenaFault,
            boot::BootFaultKind::ArenaConstructionFailed,
        ),
        (
            boot::ResourceOutcome::GpuWorkerFault,
            boot::BootFaultKind::WorkerStartupFailed,
        ),
        (
            boot::ResourceOutcome::TransportEndpointFault,
            boot::BootFaultKind::TransportEndpointFailed,
        ),
        (
            boot::ResourceOutcome::InvalidNodeIdentity,
            boot::BootFaultKind::InvalidNodeIdentity,
        ),
    ]
}

// This proves NodeAvailable is emitted only after every required local boot
// resource is ready, and that a boot attempt resolves to one availability event
// rather than a partial intermediate state.
#[test]
fn node_available_waits_for_all_required_readiness_facts() {
    // Launch starts one public boot attempt in the intended pool.
    let mut harness = boot::BootHarness::launch(valid_boot_config());

    // Drive all but the final readiness fact and prove no prefix is sufficient.
    let mut facts = required_readiness_facts();
    let final_fact = facts.pop().expect("fixture has a final fact");
    for fact in facts {
        harness.observe(fact);
        assert!(
            !harness
                .events()
                .contains(&boot::LifecycleEvent::NodeAvailable {
                    node_id: boot::NodeId(10),
                })
        );
    }

    // Once the final required fact arrives, availability becomes observable.
    harness.observe(final_fact);

    // Count the public event, not a private state bit; duplicate availability
    // would make the boot attempt ambiguous to the orchestrator.
    let available_count = harness
        .events()
        .iter()
        .filter(|event| {
            matches!(
                event,
                boot::LifecycleEvent::NodeAvailable {
                    node_id
                } if *node_id == boot::NodeId(10)
            )
        })
        .count();
    assert_eq!(available_count, 1);
}

// This proves run provisioning cannot race ahead of node availability. The
// orchestrator can use NodeAvailable as the public admission point without
// inspecting node-local boot state.
#[test]
fn provisioning_is_rejected_until_node_available_is_observed() {
    // Launch the node and create a provision message that would be valid after
    // boot finishes.
    let mut harness = boot::BootHarness::launch(valid_boot_config());
    let provision = boot::ProvisionRequest::for_node(boot::NodeId(10));

    // Before readiness, provisioning must be rejected at the public boundary.
    assert_eq!(
        harness.try_accept_provisioning(provision.clone()),
        Err(boot::ProvisioningAdmissionRejection::NodeNotAvailable)
    );

    // Complete readiness through observable facts only.
    for fact in required_readiness_facts() {
        harness.observe(fact);
    }

    // After NodeAvailable, the same request may enter run setup.
    assert_eq!(harness.try_accept_provisioning(provision), Ok(()));
}

// This proves NodeAvailable is not overloaded with graph or weight meaning. A
// booted node is eligible for orchestration, but it has not selected itself for
// any stage, edge, layer range, role, or weight assignment.
#[test]
fn node_available_contains_no_run_assignment_or_weight_claims() {
    // Complete node boot through the same public readiness facts as production.
    let mut harness = boot::BootHarness::launch(valid_boot_config());
    for fact in required_readiness_facts() {
        harness.observe(fact);
    }

    // Inspect every emitted boot command. The proof is negative: boot may emit
    // lifecycle facts, but it must not emit graph ownership commands.
    for command in harness.commands() {
        assert!(!matches!(command, boot::BootCommand::LoadWeights { .. }));
        assert!(!matches!(command, boot::BootCommand::ConfigureRole { .. }));
        assert!(!matches!(command, boot::BootCommand::EstablishEdge { .. }));
        assert!(!matches!(command, boot::BootCommand::AssignStage { .. }));
    }
}

// This proves every boot-blocking resource failure emits a typed boot fault and
// keeps the node out of the candidate run pool.
#[test]
fn boot_resource_failure_faults_without_node_availability() {
    // Each failure case runs in a fresh boot attempt so outcomes cannot mask
    // each other.
    for (failed_resource, expected_kind) in boot_fault_cases() {
        let mut harness = boot::BootHarness::launch(valid_boot_config());

        // Deliver the failed public resource outcome.
        harness.observe(failed_resource);

        // The emitted fault must carry the stable reason enum for operators and
        // tests; logs are not part of the contract.
        assert!(
            harness
                .events()
                .contains(&boot::LifecycleEvent::NodeFaulted {
                    node_id: boot::NodeId(10),
                    kind: expected_kind,
                })
        );

        // A faulted boot attempt must not also become available.
        assert!(
            !harness
                .events()
                .iter()
                .any(|event| { matches!(event, boot::LifecycleEvent::NodeAvailable { .. }) })
        );

        // The orchestrator-facing eligibility check must agree with the
        // lifecycle transcript.
        assert!(!harness.is_candidate_eligible(boot::NodeId(10)));
    }
}

// This proves node boot authority is local-resource authority only. The node
// may report boot facts, but stage, edge, layer, and object-spec assignment
// remain absent until the orchestrator provisions a committed plan.
#[test]
fn boot_never_self_assigns_run_topology() {
    // Complete a successful boot attempt.
    let mut harness = boot::BootHarness::launch(valid_boot_config());
    for fact in required_readiness_facts() {
        harness.observe(fact);
    }

    // Walk every emitted command and require it to stay in the boot domain.
    for command in harness.commands() {
        match command {
            boot::BootCommand::AdvertiseLifecycle { .. }
            | boot::BootCommand::JoinMembership { .. }
            | boot::BootCommand::OpenProvisioningInbox { .. } => {}
            boot::BootCommand::LoadWeights { .. }
            | boot::BootCommand::ConfigureRole { .. }
            | boot::BootCommand::EstablishEdge { .. }
            | boot::BootCommand::AssignStage { .. }
            | boot::BootCommand::AssignLayerRange { .. }
            | boot::BootCommand::AssignEdge { .. }
            | boot::BootCommand::AssignObjectSpec { .. } => {
                panic!("boot emitted graph assignment command: {command:?}")
            }
        }
    }
}
