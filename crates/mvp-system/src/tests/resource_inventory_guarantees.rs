//! Black-box contract tests for MVP resource inventory.
//!
//! These tests intentionally know only the public inventory/planner input
//! surface:
//!
//! - orchestrator-owned inventory entries
//! - candidate pool and placement input derived from that inventory
//! - planner acceptance or typed rejection
//!
//! They assert the guarantees in
//! `specs/mvp_system/resource_inventory_contract.md`.

use mvp_system::node::resource_inventory as inventory;

// The inventory fixture has more nodes than the placement needs. That proves
// the planner may choose among known inventory entries but may not invent hidden
// nodes or accept nodes that the orchestrator did not provide.
fn inventory_entries() -> Vec<inventory::InventoryEntry> {
    vec![
        inventory::InventoryEntry::ready(inventory::NodeId(10), inventory::GpuClass::TestSmall),
        inventory::InventoryEntry::ready(inventory::NodeId(11), inventory::GpuClass::TestSmall),
        inventory::InventoryEntry::ready(inventory::NodeId(12), inventory::GpuClass::TestSmall),
        inventory::InventoryEntry::ready(inventory::NodeId(13), inventory::GpuClass::TestSmall),
    ]
}

// The fixed placement input names stage ownership without giving the planner
// permission to derive any other topology. The plan still owns the resulting
// stage records.
fn fixed_placement() -> inventory::PlacementInput {
    inventory::PlacementInput::FixedLinear(vec![
        inventory::StagePlacement {
            stage_index: 0,
            node_id: inventory::NodeId(10),
        },
        inventory::StagePlacement {
            stage_index: 1,
            node_id: inventory::NodeId(11),
        },
        inventory::StagePlacement {
            stage_index: 2,
            node_id: inventory::NodeId(12),
        },
    ])
}

// This helper builds the public planning request from inventory facts. Tests
// mutate only the fact under examination so a rejection can be attributed to a
// specific inventory contract violation.
fn planning_request() -> inventory::PlanningRequest {
    inventory::PlanningRequest {
        run_id: inventory::RunId(7),
        stage_count: 3,
        entries: inventory_entries(),
        placement: fixed_placement(),
    }
}

// Node reports after boot are inventory inputs, not placement negotiations.
// This helper gives tests one post-boot report they can send and then prove did
// not rewrite the committed planning input.
fn post_boot_health_report(node_id: inventory::NodeId) -> inventory::NodeReport {
    inventory::NodeReport::BootHealth {
        node_id,
        health: inventory::BootHealth::Ready,
    }
}

// This proves inventory formation is orchestrator-owned and available before
// planning: every entry is tied to a known node id, and node reports do not
// negotiate placement after boot.
#[test]
fn inventory_entries_are_known_before_planning_and_not_negotiated_by_nodes() {
    // Create the orchestrator-owned inventory and snapshot the public entries.
    let mut harness = inventory::InventoryHarness::new(inventory_entries());
    let before_report = harness.entries().to_vec();

    // Let a node report boot health after the inventory already exists.
    harness.observe_node_report(post_boot_health_report(inventory::NodeId(10)));

    // Boot health may update readiness metadata, but it must not rewrite the
    // node set or add placement facts.
    let after_report = harness.entries().to_vec();
    let before_nodes = before_report
        .iter()
        .map(|entry| entry.node_id)
        .collect::<std::collections::BTreeSet<_>>();
    let after_nodes = after_report
        .iter()
        .map(|entry| entry.node_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(after_nodes, before_nodes);
    assert!(!harness.commands().iter().any(|command| {
        matches!(
            command,
            inventory::InventoryCommand::AcceptPlacementNegotiation { .. }
        )
    }));
}

// This proves the planner may place stages only onto nodes present in the
// orchestrator-owned inventory, and the emitted RunPlan is the sole placement
// result.
#[test]
fn planned_stage_nodes_are_subset_of_inventory_nodes() {
    // Build a valid public planning request from inventory entries.
    let request = planning_request();
    let inventory_nodes = request
        .entries
        .iter()
        .map(|entry| entry.node_id)
        .collect::<std::collections::BTreeSet<_>>();

    // Plan through the public inventory/planner boundary.
    let plan = inventory::plan_from_inventory(request).expect("valid inventory must plan");

    // Prove every planned stage node came from the inventory snapshot.
    for stage in &plan.stages {
        assert!(
            inventory_nodes.contains(&stage.node_id),
            "stage used node outside inventory: {:?}",
            stage.node_id
        );
    }
}

// This proves unknown nodes, duplicate stage assignments, and missing stage
// assignments reject as typed planning-input errors.
#[test]
fn invalid_inventory_placement_rejects_with_typed_errors() {
    // Each case corrupts one public placement fact.
    let cases = vec![
        (
            inventory::PlacementInput::FixedLinear(vec![
                inventory::StagePlacement {
                    stage_index: 0,
                    node_id: inventory::NodeId(10),
                },
                inventory::StagePlacement {
                    stage_index: 1,
                    node_id: inventory::NodeId(99),
                },
                inventory::StagePlacement {
                    stage_index: 2,
                    node_id: inventory::NodeId(12),
                },
            ]),
            inventory::InventoryRejectionKind::UnknownNode {
                node_id: inventory::NodeId(99),
            },
        ),
        (
            inventory::PlacementInput::FixedLinear(vec![
                inventory::StagePlacement {
                    stage_index: 0,
                    node_id: inventory::NodeId(10),
                },
                inventory::StagePlacement {
                    stage_index: 0,
                    node_id: inventory::NodeId(11),
                },
                inventory::StagePlacement {
                    stage_index: 2,
                    node_id: inventory::NodeId(12),
                },
            ]),
            inventory::InventoryRejectionKind::DuplicateStage { stage_index: 0 },
        ),
        (
            inventory::PlacementInput::FixedLinear(vec![
                inventory::StagePlacement {
                    stage_index: 0,
                    node_id: inventory::NodeId(10),
                },
                inventory::StagePlacement {
                    stage_index: 2,
                    node_id: inventory::NodeId(12),
                },
            ]),
            inventory::InventoryRejectionKind::MissingStage { stage_index: 1 },
        ),
    ];

    for (placement, expected_kind) in cases {
        // Replace only the placement fact in an otherwise valid request.
        let mut request = planning_request();
        request.placement = placement;

        // The rejection must be typed and no RunPlan may be emitted.
        let rejection = inventory::plan_from_inventory(request)
            .expect_err("invalid inventory placement must reject");
        assert_eq!(rejection.kind, expected_kind);
    }
}

// This proves planning does not mutate the candidate pool and does not derive
// hidden nodes outside the inventory.
#[test]
fn planning_preserves_candidate_pool_and_emits_no_hidden_nodes() {
    // Snapshot the public inventory before planning.
    let request = planning_request();
    let original_entries = request.entries.clone();
    let original_nodes = original_entries
        .iter()
        .map(|entry| entry.node_id)
        .collect::<std::collections::BTreeSet<_>>();

    // Plan using a cloned request so the caller-owned facts remain inspectable.
    let plan = inventory::plan_from_inventory(request.clone()).expect("valid inventory must plan");

    // The caller's candidate facts must be unchanged.
    assert_eq!(request.entries, original_entries);

    // Every node mentioned by the plan must be explainable by the original
    // inventory snapshot.
    let planned_nodes = plan
        .stages
        .iter()
        .map(|stage| stage.node_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert!(planned_nodes.is_subset(&original_nodes));
}

// This proves inventory authority ends at planner input. After provisioning,
// stages consume their RunPlan-derived assignment and do not reinterpret
// inventory reports.
#[test]
fn provisioned_stages_do_not_reinterpret_inventory() {
    // Commit a plan and provision its stages.
    let plan =
        inventory::plan_from_inventory(planning_request()).expect("valid inventory must plan");
    let mut harness = inventory::InventoryHarness::new(inventory_entries());
    harness.commit_plan(plan.clone());
    harness.provision_stages();

    // Send a later inventory report that would be dangerous if treated as a
    // graph rewrite.
    harness.observe_node_report(inventory::NodeReport::CapacityChanged {
        node_id: inventory::NodeId(11),
        gpu_class: inventory::GpuClass::TestLarge,
    });

    // No provisioned stage may be asked to reinterpret its assignment.
    assert!(!harness.commands().iter().any(|command| {
        matches!(
            command,
            inventory::InventoryCommand::RewriteStagePlacement { .. }
        )
    }));

    // The committed plan remains the only graph-visible placement fact.
    assert_eq!(harness.committed_plan(), Some(&plan));
}
