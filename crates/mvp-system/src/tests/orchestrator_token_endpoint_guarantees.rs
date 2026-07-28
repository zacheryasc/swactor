//! Black-box contract tests for MVP orchestrator token endpoints.
//!
//! These tests intentionally know only the public token-endpoint surface:
//!
//! - committed token edges and endpoint formation in
//! - readiness, prompt, token, and fault events in
//! - token writes, lifecycle events, and run faults out
//!
//! They assert the guarantees in
//! `specs/mvp_system/orchestrator_token_endpoint_contract.md`.

use mvp_system::orchestration::token_endpoint as token;

// The endpoint plan names the orchestrator and both token edges. Tests use the
// committed plan as input and do not assume how endpoints allocate local rings
// or actors.
fn token_plan() -> token::TokenEndpointPlan {
    token::TokenEndpointPlan {
        run_id: token::RunId(7),
        orchestrator_node_id: token::NodeId(99),
        token_in_edge: token::EdgePlan::token_in(
            token::EdgeId(7000),
            token::NodeId(99),
            token::NodeId(10),
        ),
        token_out_edge: token::EdgePlan::token_out(
            token::EdgeId(7003),
            token::NodeId(12),
            token::NodeId(99),
        ),
        token_spec: token::ObjectSpec::test_tokens(),
        max_tokens: 4,
    }
}

// The harness exposes only endpoint-visible inputs and outputs. It does not
// reveal pump tasks, local actor addresses, or ring internals.
fn new_endpoint_harness() -> token::TokenEndpointHarness {
    token::TokenEndpointHarness::new(token_plan())
}

// This helper drives the global readiness barrier as the orchestrator would see
// it. Prompt-injection tests use it to prove the endpoint does not start early.
fn pass_global_barrier(harness: &mut token::TokenEndpointHarness) {
    harness.observe(token::EndpointEvent::TokenInReady {
        edge_id: token::EdgeId(7000),
    });
    harness.observe(token::EndpointEvent::TokenOutReady {
        edge_id: token::EdgeId(7003),
    });
    harness.observe(token::EndpointEvent::AllStagesReady);
    harness.observe(token::EndpointEvent::ReadinessBarrierPassed);
}

// This proves endpoint formation is derived from the committed plan, uses the
// same edge semantics as stage endpoints, and keeps a stable orchestrator node
// identity even when co-located with GPU nodes.
#[test]
fn token_endpoints_are_created_from_committed_plan_with_stable_node_id() {
    // Create endpoints from the committed token plan.
    let harness = new_endpoint_harness();

    // Token-in and token-out endpoint creation must reference the plan edges.
    assert!(harness.commands().iter().any(|command| {
        matches!(
            command,
            token::EndpointCommand::CreateTokenInProducer {
                edge_id: token::EdgeId(7000),
                orchestrator_node_id: token::NodeId(99),
                ..
            }
        )
    }));
    assert!(harness.commands().iter().any(|command| {
        matches!(
            command,
            token::EndpointCommand::CreateTokenOutConsumer {
                edge_id: token::EdgeId(7003),
                orchestrator_node_id: token::NodeId(99),
                ..
            }
        )
    }));

    // Co-location must not change the edge shape or remove the orchestrator
    // endpoint identity.
    for endpoint in harness.local_endpoints() {
        assert_eq!(endpoint.orchestrator_node_id, token::NodeId(99));
        assert!(matches!(
            endpoint.edge_semantics,
            token::EdgeSemantics::SingleProducerSingleConsumer
        ));
    }
}

// This proves prompt injection waits for the global readiness barrier, writes
// token object sequence 0, conforms to the token ObjectSpec, and is the only
// start signal.
#[test]
fn prompt_injection_is_barrier_gated_sequence_zero_token_object() {
    // Build endpoints but do not pass readiness.
    let mut harness = new_endpoint_harness();
    harness.request_prompt_injection(vec![101, 102, 103]);

    // No prompt object may be written before the barrier.
    assert!(
        !harness
            .commands()
            .iter()
            .any(|command| { matches!(command, token::EndpointCommand::WriteTokenObject { .. }) })
    );

    // Passing the global barrier permits the first prompt write.
    pass_global_barrier(&mut harness);

    // The first token-in object must be sequence 0 and match the token spec.
    let writes = harness
        .commands()
        .iter()
        .filter_map(|command| match command {
            token::EndpointCommand::WriteTokenObject(write) => Some(write),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].sequence, 0);
    assert_eq!(writes[0].object_spec, token::ObjectSpec::test_tokens());

    // Prompt injection is the start signal; there must not be a separate
    // broadcast start command.
    assert!(
        !harness
            .commands()
            .iter()
            .any(|command| { matches!(command, token::EndpointCommand::BroadcastStart { .. }) })
    );
}

// This proves token-out is consumed in sequence order and sequence k + 1 is
// injected only after the orchestrator consumes token sequence k.
#[test]
fn token_feedback_controls_next_injection() {
    // Start with prompt sequence 0 injected.
    let mut harness = new_endpoint_harness();
    harness.request_prompt_injection(vec![101, 102, 103]);
    pass_global_barrier(&mut harness);
    assert_eq!(harness.injected_sequences(), vec![0]);

    // Consuming token sequence 0 authorizes sequence 1.
    harness.observe(token::EndpointEvent::TokenObjectReceived {
        edge_id: token::EdgeId(7003),
        object_id: token::ObjectId(9000),
        sequence: 0,
        token_id: 201,
        eos: false,
    });
    assert_eq!(harness.injected_sequences(), vec![0, 1]);

    // Time and readiness events alone do not authorize another decode step.
    harness.advance_time_ms(100);
    assert_eq!(harness.injected_sequences(), vec![0, 1]);

    // An out-of-order token faults the run rather than skipping ahead.
    harness.observe(token::EndpointEvent::TokenObjectReceived {
        edge_id: token::EdgeId(7003),
        object_id: token::ObjectId(9002),
        sequence: 3,
        token_id: 203,
        eos: false,
    });
    assert!(harness.events().iter().any(|event| {
        matches!(
            event,
            token::EndpointLifecycleEvent::RunFaulted {
                reason: token::RunFaultReason::TokenSequenceViolation,
                ..
            }
        )
    }));
}

// This proves EOS and max_tokens both stop further token-in injection.
#[test]
fn eos_and_max_tokens_stop_injection() {
    // EOS stops immediately after the consumed token.
    let mut eos_harness = new_endpoint_harness();
    eos_harness.request_prompt_injection(vec![101]);
    pass_global_barrier(&mut eos_harness);
    eos_harness.observe(token::EndpointEvent::TokenObjectReceived {
        edge_id: token::EdgeId(7003),
        object_id: token::ObjectId(9000),
        sequence: 0,
        token_id: 2,
        eos: true,
    });
    assert_eq!(eos_harness.injected_sequences(), vec![0]);

    // max_tokens stops after the configured number of injections.
    let mut max_harness = new_endpoint_harness();
    max_harness.request_prompt_injection(vec![101]);
    pass_global_barrier(&mut max_harness);
    for sequence in 0..4 {
        max_harness.observe(token::EndpointEvent::TokenObjectReceived {
            edge_id: token::EdgeId(7003),
            object_id: token::ObjectId(9000 + sequence),
            sequence,
            token_id: 300 + sequence as u32,
            eos: false,
        });
    }
    assert_eq!(max_harness.injected_sequences(), vec![0, 1, 2, 3]);
}

// This proves token endpoint, malformed object, and sequence faults are
// surfaced as run faults, while teardown failure contributes to teardown
// failure rather than being lost in logs.
#[test]
fn token_endpoint_failures_fault_the_run_or_teardown() {
    // Endpoint fault during active run faults the run.
    let mut harness = new_endpoint_harness();
    harness.request_prompt_injection(vec![101]);
    pass_global_barrier(&mut harness);
    harness.observe(token::EndpointEvent::EndpointFault {
        edge_id: token::EdgeId(7000),
        direction: token::EndpointDirection::TokenIn,
    });
    assert!(harness.events().iter().any(|event| {
        matches!(
            event,
            token::EndpointLifecycleEvent::RunFaulted {
                reason: token::RunFaultReason::TokenEndpointFault,
                ..
            }
        )
    }));

    // Malformed token objects fault the run at the token boundary.
    let mut malformed = new_endpoint_harness();
    malformed.request_prompt_injection(vec![101]);
    pass_global_barrier(&mut malformed);
    malformed.observe(token::EndpointEvent::MalformedTokenObject {
        edge_id: token::EdgeId(7003),
        object_id: token::ObjectId(9999),
    });
    assert!(malformed.events().iter().any(|event| {
        matches!(
            event,
            token::EndpointLifecycleEvent::RunFaulted {
                reason: token::RunFaultReason::MalformedTokenObject,
                ..
            }
        )
    }));

    // Teardown failure must be visible as teardown failure.
    malformed.observe(token::EndpointEvent::TeardownFailed {
        edge_id: token::EdgeId(7003),
    });
    assert!(
        malformed
            .events()
            .iter()
            .any(|event| { matches!(event, token::EndpointLifecycleEvent::TeardownFailed { .. }) })
    );
}
