use mvp_system::observability_surface as obs;

use super::mock_transport::MockObjectKind;

use super::environment::LocalMockOutcome;

pub fn assert_happy_path_lifecycle(outcome: &LocalMockOutcome) {
    assert_eq!(count_kind(outcome, obs::EventKind::PoolReady), 1);
    assert_eq!(count_kind(outcome, obs::EventKind::RunPlanned), 1);
    assert_eq!(
        count_kind(outcome, obs::EventKind::StageProvisionStarted),
        outcome.stage_count
    );
    assert_eq!(
        count_kind(outcome, obs::EventKind::WeightsLoaded),
        outcome.stage_count
    );
    assert_eq!(
        count_kind(outcome, obs::EventKind::StageReady),
        outcome.stage_count
    );
    assert_eq!(
        count_kind(outcome, obs::EventKind::ReadinessBarrierPassed),
        1
    );
    assert_eq!(outcome.injected_sequences, vec![0, 1]);
    assert_eq!(
        sequences_for_kind(outcome, obs::EventKind::PromptInjected),
        outcome.injected_sequences
    );
    assert_eq!(
        sequences_for_kind(outcome, obs::EventKind::TokenReceived),
        outcome.injected_sequences
    );
    assert_eq!(
        count_kind(outcome, obs::EventKind::ExecuteStepStarted),
        outcome.stage_count * outcome.injected_sequences.len()
    );
    assert_eq!(
        count_kind(outcome, obs::EventKind::StepCompleted),
        outcome.stage_count * outcome.injected_sequences.len()
    );
    assert_eq!(
        outcome.transport_delivery_count,
        (outcome.stage_count + 1) * outcome.injected_sequences.len()
    );

    assert_order(
        outcome,
        obs::EventKind::PoolReady,
        obs::EventKind::RunPlanned,
    );
    assert_order(
        outcome,
        obs::EventKind::RunPlanned,
        obs::EventKind::StageProvisionStarted,
    );
    assert!(
        last_position(outcome, obs::EventKind::StageReady)
            < first_position(outcome, obs::EventKind::ReadinessBarrierPassed),
        "readiness barrier must wait for every stage_ready"
    );
    assert_order(
        outcome,
        obs::EventKind::ReadinessBarrierPassed,
        obs::EventKind::PromptInjected,
    );

    for sequence in &outcome.injected_sequences {
        assert_eq!(
            object_sequence_count(outcome, obs::EventKind::ObjectLoaded, *sequence),
            outcome.stage_count,
            "each stage must load sequence {sequence} once"
        );
        assert_eq!(
            object_sequence_count(outcome, obs::EventKind::ObjectProduced, *sequence),
            outcome.stage_count,
            "each stage must produce sequence {sequence} once"
        );
        assert_eq!(
            object_sequence_count(outcome, obs::EventKind::TokenReceived, *sequence),
            1,
            "orchestrator must receive sequence {sequence} once"
        );
    }
}

pub fn assert_topology_surface(outcome: &LocalMockOutcome) {
    let sequence_count = outcome.injected_sequences.len();
    assert_eq!(
        outcome.transport_delivery_count,
        (outcome.stage_count + 1) * sequence_count
    );
    assert_eq!(
        count_kind(outcome, obs::EventKind::ExecuteStepStarted),
        outcome.stage_count * sequence_count
    );
    assert_eq!(
        count_kind(outcome, obs::EventKind::StepCompleted),
        outcome.stage_count * sequence_count
    );

    for sequence in &outcome.injected_sequences {
        let deliveries = outcome
            .transport_deliveries
            .iter()
            .filter(|delivery| delivery.sequence == *sequence)
            .collect::<Vec<_>>();
        assert_eq!(
            deliveries.len(),
            outcome.stage_count + 1,
            "sequence {sequence} must cross token-in, every stage output, and token-out"
        );
        assert_eq!(deliveries[0].kind, MockObjectKind::Token);
        assert_eq!(deliveries.last().unwrap().kind, MockObjectKind::Token);
        assert_eq!(
            deliveries
                .iter()
                .map(|delivery| delivery.edge_id)
                .collect::<Vec<_>>(),
            outcome.edge_chain,
            "sequence {sequence} must follow the planned linear edge chain"
        );
        assert_eq!(
            deliveries
                .iter()
                .filter(|delivery| delivery.kind == MockObjectKind::Activation)
                .count(),
            outcome.stage_count.saturating_sub(1),
            "only inter-stage edges carry activations"
        );
    }
}

pub fn assert_terminal_fault(outcome: &LocalMockOutcome) {
    assert_eq!(count_kind(outcome, obs::EventKind::RunCompleted), 0);
    assert_eq!(count_kind(outcome, obs::EventKind::RunFaulted), 1);
    assert_eq!(count_kind(outcome, obs::EventKind::RunTornDown), 1);
    assert_eq!(
        count_kind(outcome, obs::EventKind::StopRunSent),
        outcome.stage_count
    );
    assert_eq!(
        count_kind(outcome, obs::EventKind::StageStopped),
        outcome.stage_count
    );
    assert_resources_released(outcome);
    assert_only_teardown_after_terminal(outcome, obs::EventKind::RunFaulted);
}

pub fn assert_terminal_success(outcome: &LocalMockOutcome) {
    assert_eq!(count_kind(outcome, obs::EventKind::RunCompleted), 1);
    assert_eq!(count_kind(outcome, obs::EventKind::RunFaulted), 0);
    assert_eq!(count_kind(outcome, obs::EventKind::RunTornDown), 1);
    assert_eq!(
        count_kind(outcome, obs::EventKind::StopRunSent),
        outcome.stage_count
    );
    assert_eq!(
        count_kind(outcome, obs::EventKind::StageStopped),
        outcome.stage_count
    );
    assert_order(
        outcome,
        obs::EventKind::RunCompleted,
        obs::EventKind::StopRunSent,
    );
    assert!(
        last_position(outcome, obs::EventKind::StageStopped)
            < first_position(outcome, obs::EventKind::RunTornDown),
        "run_torn_down must wait for stage_stopped events"
    );

    for stage_index in 0..outcome.stage_count as u32 {
        assert!(
            stage_position(outcome, obs::EventKind::StopRunSent, stage_index)
                < stage_position(outcome, obs::EventKind::StageStopped, stage_index),
            "stage {stage_index} must stop after StopRun"
        );
    }

    assert_resources_released(outcome);
    assert_only_teardown_after_terminal(outcome, obs::EventKind::RunCompleted);
}

fn assert_resources_released(outcome: &LocalMockOutcome) {
    assert_eq!(outcome.live_edges, 0, "teardown must release mock edges");
    assert_eq!(outcome.live_rings, 0, "teardown must release mock rings");
    assert_eq!(
        outcome.live_stage_runs, 0,
        "teardown must release mock stage run state"
    );
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

fn assert_order(outcome: &LocalMockOutcome, before: obs::EventKind, after: obs::EventKind) {
    assert!(
        first_position(outcome, before) < first_position(outcome, after),
        "{before:?} must occur before {after:?}"
    );
}

fn sequences_for_kind(outcome: &LocalMockOutcome, kind: obs::EventKind) -> Vec<u64> {
    outcome
        .trace
        .iter()
        .filter_map(|event| match event {
            obs::Event::ObjectScoped {
                kind: event_kind,
                sequence,
                ..
            } if *event_kind == kind => Some(sequence.0),
            _ => None,
        })
        .collect()
}

fn object_sequence_count(outcome: &LocalMockOutcome, kind: obs::EventKind, sequence: u64) -> usize {
    outcome
        .trace
        .iter()
        .filter(|event| match event {
            obs::Event::ObjectScoped {
                kind: event_kind,
                sequence: event_sequence,
                ..
            } => *event_kind == kind && event_sequence.0 == sequence,
            _ => false,
        })
        .count()
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

fn assert_only_teardown_after_terminal(outcome: &LocalMockOutcome, terminal: obs::EventKind) {
    let terminal_position = first_position(outcome, terminal);
    for event in &outcome.trace[terminal_position + 1..] {
        assert!(
            matches!(
                event.kind(),
                obs::EventKind::StopRunSent
                    | obs::EventKind::StageStopped
                    | obs::EventKind::RunTornDown
            ),
            "non-teardown event after terminal outcome: {event:?}"
        );
    }
}
