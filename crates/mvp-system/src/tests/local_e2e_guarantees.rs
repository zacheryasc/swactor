//! Local end-to-end behavior guarantee for the MVP system lifecycle.
//!
//! This test composes the crate's MVP contract harnesses through one in-process
//! mock environment. It deliberately avoids Docker, real SWIM, real iroh, GGUF,
//! and CUDA while still driving the run through planning, provisioning,
//! readiness, prompt injection, stage execution, completion, and teardown.

use mvp_system::observability::lifecycle as obs;
use mvp_system::orchestration::engine_builder as engine;

use super::local_mock::{
    LocalMockCluster, LocalMockConfig, LocalMockOutcome, assert_happy_path_lifecycle,
    assert_terminal_fault, assert_terminal_success, assert_topology_surface,
};

#[test]
fn local_mock_two_stage_pipeline_completes_and_tears_down() {
    let mut cluster = LocalMockCluster::two_stage();
    let outcome = cluster.run_prompt("hello world");

    assert_happy_path_lifecycle(&outcome);
    assert_topology_surface(&outcome);
    assert_engine_builder_surface(&outcome);
    assert_terminal_success(&outcome);
}

#[test]
fn local_mock_pipeline_topologies_complete() {
    let cases = [
        (
            LocalMockConfig {
                stage_count: 1,
                max_tokens: 4,
                eos_after_sequence: 0,
            },
            vec![0],
        ),
        (
            LocalMockConfig {
                stage_count: 2,
                max_tokens: 4,
                eos_after_sequence: 1,
            },
            vec![0, 1],
        ),
        (
            LocalMockConfig {
                stage_count: 3,
                max_tokens: 3,
                eos_after_sequence: 99,
            },
            vec![0, 1, 2],
        ),
    ];

    for (config, expected_sequences) in cases {
        let mut cluster = LocalMockCluster::with_config(config);
        let outcome = cluster.run_prompt("hello world");

        assert_eq!(outcome.injected_sequences, expected_sequences);
        assert_topology_surface(&outcome);
        assert_engine_builder_surface(&outcome);
        assert_terminal_success(&outcome);
    }
}

#[test]
fn local_mock_readiness_barrier_gates_prompt_injection() {
    let mut cluster = LocalMockCluster::with_config(LocalMockConfig {
        stage_count: 3,
        max_tokens: 2,
        eos_after_sequence: 0,
    });
    let outcome = cluster.run_prompt_with_delayed_stage_ready("hello world", 1);

    assert_topology_surface(&outcome);
    assert_terminal_success(&outcome);
    assert!(
        stage_position(&outcome, obs::EventKind::StageReady, 1)
            < first_position(&outcome, obs::EventKind::ReadinessBarrierPassed),
        "delayed stage must become ready before the readiness barrier passes"
    );
    assert!(
        last_position(&outcome, obs::EventKind::StageReady)
            < first_position(&outcome, obs::EventKind::PromptInjected),
        "prompt injection must wait for every stage_ready event"
    );
}

#[test]
fn local_mock_terminal_modes_teardown_everything() {
    let mut max_token_cluster = LocalMockCluster::with_config(LocalMockConfig {
        stage_count: 2,
        max_tokens: 3,
        eos_after_sequence: 99,
    });
    let max_token_outcome = max_token_cluster.run_prompt("hello world");
    assert_eq!(max_token_outcome.injected_sequences, vec![0, 1, 2]);
    assert_terminal_success(&max_token_outcome);

    let mut provisioning_fault_cluster = LocalMockCluster::with_config(LocalMockConfig {
        stage_count: 3,
        max_tokens: 4,
        eos_after_sequence: 1,
    });
    let provisioning_fault = provisioning_fault_cluster.run_with_unauthorized_provision("hello", 2);
    assert_eq!(
        count_kind(&provisioning_fault, obs::EventKind::StageFaulted),
        1
    );
    assert_eq!(
        count_kind(&provisioning_fault, obs::EventKind::PromptInjected),
        0
    );
    assert_terminal_fault(&provisioning_fault);

    let mut execution_fault_cluster = LocalMockCluster::with_config(LocalMockConfig {
        stage_count: 3,
        max_tokens: 4,
        eos_after_sequence: 1,
    });
    let execution_fault =
        execution_fault_cluster.run_with_worker_crash_during_execution("hello", 1, 0);
    assert_eq!(
        count_kind(&execution_fault, obs::EventKind::PromptInjected),
        1
    );
    assert_eq!(
        count_kind(&execution_fault, obs::EventKind::TokenReceived),
        0
    );
    assert_terminal_fault(&execution_fault);
}

#[test]
fn local_mock_rejects_invalid_cross_stage_events() {
    let mut wrong_edge_cluster = LocalMockCluster::with_config(LocalMockConfig {
        stage_count: 2,
        max_tokens: 2,
        eos_after_sequence: 0,
    });
    let wrong_edge = wrong_edge_cluster.run_wrong_edge_object_then_prompt("hello", 0);
    assert_eq!(
        count_kind(&wrong_edge, obs::EventKind::ObjectLoaded),
        wrong_edge.stage_count * wrong_edge.injected_sequences.len() + 1,
        "wrong-edge object is observed but must not add an execution step"
    );
    assert_eq!(
        count_kind(&wrong_edge, obs::EventKind::ExecuteStepStarted),
        wrong_edge.stage_count * wrong_edge.injected_sequences.len()
    );
    assert_terminal_success(&wrong_edge);

    let mut sequence_violation_cluster = LocalMockCluster::with_config(LocalMockConfig {
        stage_count: 2,
        max_tokens: 2,
        eos_after_sequence: 0,
    });
    let sequence_violation = sequence_violation_cluster.run_with_sequence_violation("hello", 0, 0);
    assert_eq!(
        count_kind(&sequence_violation, obs::EventKind::ExecuteStepStarted),
        0,
        "out-of-order first object must fault before compute admission"
    );
    assert_eq!(
        count_kind(&sequence_violation, obs::EventKind::TokenReceived),
        0
    );
    assert_terminal_fault(&sequence_violation);
}

fn assert_engine_builder_surface(outcome: &LocalMockOutcome) {
    assert!(
        outcome
            .engine_events
            .iter()
            .any(|event| matches!(event, engine::EngineEvent::PoolAcquired { .. })),
        "local mock integration must be built from a neutral engine pool"
    );
    assert!(
        outcome
            .engine_events
            .iter()
            .any(|event| matches!(event, engine::EngineEvent::ClusterConverged { .. })),
        "local mock integration must pass through the builder convergence barrier"
    );
    assert!(
        outcome
            .engine_events
            .iter()
            .any(|event| matches!(event, engine::EngineEvent::EngineReady { .. })),
        "local mock integration must return an engine-ready handle before workload IO"
    );
    let assigned_stages = outcome
        .engine_events
        .iter()
        .filter(|event| {
            matches!(
                event,
                engine::EngineEvent::RoleAssigned {
                    role: engine::RoleKind::StageWorker { .. },
                    ..
                }
            )
        })
        .count();
    assert_eq!(assigned_stages, outcome.stage_count);
}

fn count_kind(outcome: &LocalMockOutcome, kind: obs::EventKind) -> usize {
    outcome
        .trace
        .iter()
        .filter(|event| event.kind() == kind)
        .count()
}

fn first_position(outcome: &LocalMockOutcome, kind: obs::EventKind) -> usize {
    outcome
        .trace
        .iter()
        .position(|event| event.kind() == kind)
        .unwrap_or_else(|| panic!("missing event kind {kind:?}"))
}

fn last_position(outcome: &LocalMockOutcome, kind: obs::EventKind) -> usize {
    outcome
        .trace
        .iter()
        .rposition(|event| event.kind() == kind)
        .unwrap_or_else(|| panic!("missing event kind {kind:?}"))
}

fn stage_position(outcome: &LocalMockOutcome, kind: obs::EventKind, stage_index: u32) -> usize {
    outcome
        .trace
        .iter()
        .position(|event| match event {
            obs::Event::StageScoped {
                kind: event_kind,
                stage_index: event_stage_index,
                ..
            } => *event_kind == kind && event_stage_index.0 == stage_index,
            _ => false,
        })
        .unwrap_or_else(|| panic!("missing stage event {kind:?} for stage {stage_index}"))
}
