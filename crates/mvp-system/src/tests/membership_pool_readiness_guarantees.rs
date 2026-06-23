//! Black-box contract tests for MVP membership and pool readiness.
//!
//! These tests intentionally know only the public readiness-gate surface:
//!
//! - a fixed orchestrator-owned candidate pool
//! - membership, availability, and identity observations
//! - emitted pool readiness, planning, and run fault events
//!
//! They assert the guarantees in
//! `specs/mvp_system/membership_pool_readiness_contract.md`.

use mvp_system::membership_pool_readiness as membership;

// Three nodes are enough to prove all-node quantification without hiding behind
// a single-node special case. The orchestrator owns this pool; SWIM only reports
// liveness facts about it.
fn candidate_pool() -> Vec<membership::NodeId> {
    vec![
        membership::NodeId(10),
        membership::NodeId(11),
        membership::NodeId(12),
    ]
}

// The convergence window is short but non-zero so tests can prove PoolReady is
// not emitted at the instant the last fact arrives.
fn readiness_config() -> membership::ReadinessConfig {
    membership::ReadinessConfig {
        pool_id: membership::PoolId("mvp-test-pool".into()),
        convergence_window_ms: 500,
    }
}

// This helper provides every required public fact for one node. Tests use it to
// build complete and deliberately incomplete pool views without observing any
// private readiness bookkeeping.
fn report_node_ready(gate: &mut membership::ReadinessGateHarness, node_id: membership::NodeId) {
    gate.observe(membership::Observation::NodeKnown { node_id });
    gate.observe(membership::Observation::SwimLive { node_id });
    gate.observe(membership::Observation::NodeAvailable { node_id });
    gate.observe(membership::Observation::DataPlaneIdentityReady { node_id });
}

// The test harness records public events as a transcript. This helper counts a
// specific event so duplicate readiness or duplicate faults are visible at the
// contract boundary.
fn count_pool_ready(events: &[membership::ReadinessEvent]) -> usize {
    events
        .iter()
        .filter(|event| matches!(event, membership::ReadinessEvent::PoolReady { .. }))
        .count()
}

// This proves PoolReady requires every candidate node, every required fact, no
// suspect or faulted candidates, and a stable convergence window.
#[test]
fn pool_ready_requires_complete_stable_candidate_pool() {
    // Build the readiness gate with an orchestrator-owned candidate pool.
    let pool = candidate_pool();
    let mut gate = membership::ReadinessGateHarness::new(readiness_config(), pool.clone());

    // Report complete facts for every node except the final candidate.
    report_node_ready(&mut gate, pool[0]);
    report_node_ready(&mut gate, pool[1]);
    gate.advance_time_ms(1_000);

    // A partial pool must not become ready, even after time passes.
    assert_eq!(count_pool_ready(gate.events()), 0);

    // Report the final candidate, then prove the convergence window still
    // matters by advancing less than the configured duration.
    report_node_ready(&mut gate, pool[2]);
    gate.advance_time_ms(499);
    assert_eq!(count_pool_ready(gate.events()), 0);

    // After the window, PoolReady must describe exactly the intended pool.
    gate.advance_time_ms(1);
    let ready = gate
        .events()
        .iter()
        .find_map(|event| match event {
            membership::ReadinessEvent::PoolReady { pool } => Some(pool),
            _ => None,
        })
        .expect("complete stable pool must emit PoolReady");

    // Compare as sets so ordering is not part of the behavioral contract.
    let observed = ready
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let expected = pool
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(observed, expected);
}

// This proves suspect, faulted, missing, or identity-less candidates prevent
// PoolReady rather than being silently ignored.
#[test]
fn unavailable_candidate_prevents_pool_ready() {
    // Each case withholds or poisons one readiness fact for node 12.
    let cases = [
        membership::Observation::SwimSuspect {
            node_id: membership::NodeId(12),
        },
        membership::Observation::NodeFaulted {
            node_id: membership::NodeId(12),
        },
        membership::Observation::DataPlaneIdentityMissing {
            node_id: membership::NodeId(12),
        },
    ];

    for poisoned_fact in cases {
        // Start each run from a fresh gate so one poisoned fact is responsible
        // for the absence of readiness.
        let pool = candidate_pool();
        let mut gate = membership::ReadinessGateHarness::new(readiness_config(), pool.clone());

        // Make every candidate otherwise known and available.
        for node_id in &pool {
            report_node_ready(&mut gate, *node_id);
        }

        // Poison the final candidate and wait past convergence.
        gate.observe(poisoned_fact.clone());
        gate.advance_time_ms(1_000);

        // PoolReady must be absent because one required candidate is no longer
        // available to the intended pool.
        assert_eq!(count_pool_ready(gate.events()), 0);
    }
}

// This proves run planning is gated by PoolReady. SWIM observations alone do
// not start placement, and loss before a committed RunPlan prevents planning
// from racing ahead.
#[test]
fn planning_starts_only_after_pool_ready_and_stops_if_readiness_is_lost() {
    // Create a gate and ask the orchestrator to plan before readiness.
    let pool = candidate_pool();
    let mut gate = membership::ReadinessGateHarness::new(readiness_config(), pool.clone());
    gate.request_run_planning(membership::RunRequest::new(membership::RunId(7)));

    // With no PoolReady event, there must be no planning command.
    assert!(
        !gate.commands().iter().any(|command| {
            matches!(command, membership::ReadinessCommand::StartPlanning { .. })
        })
    );

    // Satisfy readiness, then immediately lose a required node before plan
    // commit. The policy may wait or abort, but it must not commit placement.
    for node_id in &pool {
        report_node_ready(&mut gate, *node_id);
    }
    gate.advance_time_ms(500);
    gate.observe(membership::Observation::SwimLost { node_id: pool[1] });

    // No plan commitment command may be emitted from an unstable pool view.
    assert!(
        !gate.commands().iter().any(|command| {
            matches!(command, membership::ReadinessCommand::CommitRunPlan { .. })
        })
    );
}

// This proves membership loss after provisioning is a run fault, not active
// re-placement. The MVP run keeps its committed topology until teardown.
#[test]
fn required_node_loss_after_provisioning_faults_without_replacement() {
    // Drive the pool to ready and mark a run as provisioned from that pool.
    let pool = candidate_pool();
    let mut gate = membership::ReadinessGateHarness::new(readiness_config(), pool.clone());
    for node_id in &pool {
        report_node_ready(&mut gate, *node_id);
    }
    gate.advance_time_ms(500);
    gate.observe(membership::Observation::RunProvisioned {
        run_id: membership::RunId(7),
        required_nodes: pool.clone(),
    });

    // Lose one required node during the active run.
    gate.observe(membership::Observation::SwimLost { node_id: pool[1] });

    // The run must fault with a membership reason.
    assert!(
        gate.events()
            .contains(&membership::ReadinessEvent::RunFaulted {
                run_id: membership::RunId(7),
                reason: membership::RunFaultReason::RequiredNodeLost { node_id: pool[1] },
            })
    );

    // Re-placement would violate the committed-plan authority boundary.
    assert!(!gate.commands().iter().any(|command| {
        matches!(
            command,
            membership::ReadinessCommand::RecomputePlacement { .. }
        )
    }));
}

// This proves SWIM authority is limited to membership and liveness. It may
// report facts, but it must not assign stages, edges, layers, or object specs.
#[test]
fn swim_observations_do_not_create_graph_assignments() {
    // Feed a complete, stable membership view.
    let pool = candidate_pool();
    let mut gate = membership::ReadinessGateHarness::new(readiness_config(), pool.clone());
    for node_id in &pool {
        report_node_ready(&mut gate, *node_id);
    }
    gate.advance_time_ms(500);

    // Walk emitted commands and reject any graph-assignment side effect.
    for command in gate.commands() {
        match command {
            membership::ReadinessCommand::EmitPoolReady { .. }
            | membership::ReadinessCommand::StartPlanning { .. }
            | membership::ReadinessCommand::WaitForStability { .. }
            | membership::ReadinessCommand::AbortPendingRun { .. } => {}
            membership::ReadinessCommand::CommitRunPlan { .. }
            | membership::ReadinessCommand::RecomputePlacement { .. }
            | membership::ReadinessCommand::AssignStage { .. }
            | membership::ReadinessCommand::AssignEdge { .. }
            | membership::ReadinessCommand::AssignLayerRange { .. }
            | membership::ReadinessCommand::AssignObjectSpec { .. } => {
                panic!("membership gate emitted graph assignment: {command:?}")
            }
        }
    }
}
