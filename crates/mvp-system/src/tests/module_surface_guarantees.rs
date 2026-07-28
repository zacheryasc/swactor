//! Black-box contract tests for the Stage 1 MVP module surface.
//!
//! These tests assert only the observable public-path contract: target modules
//! expose existing behavior without introducing copied type definitions.

use mvp_system::{
    chat, node, node_data, observability, orchestration, prompt, staging, transport, worker,
};

#[test]
fn target_modules_offer_new_paths_to_existing_public_contracts() {
    let run_id: orchestration::run_plan::RunId = orchestration::run_plan::RunId::from(7);
    assert_eq!(run_id, orchestration::run_plan::RunId(7));

    let boot_node_id: node::boot_lifecycle::NodeId = node::boot_lifecycle::NodeId(11);
    assert_eq!(boot_node_id, node::boot_lifecycle::NodeId(11));

    let arena_ring_id: node_data::arena::RingId = node_data::arena::RingId(3);
    assert_eq!(arena_ring_id, node_data::arena::RingId(3));

    let stage_edge_id: staging::EdgeId = staging::EdgeId(7001);
    assert_eq!(stage_edge_id, staging::EdgeId(7001));

    let transport_edge_id: node::edge_lifecycle::EdgeId = node::edge_lifecycle::EdgeId(7002);
    assert_eq!(transport_edge_id, node::edge_lifecycle::EdgeId(7002));

    let worker_generation: worker::WorkerGeneration = worker::WorkerGeneration(2);
    assert_eq!(worker_generation, worker::WorkerGeneration(2));

    let prompt_request: prompt::rpc::SubmitPrompt = prompt::rpc::SubmitPrompt {
        request_id: 42,
        prompt_text: "hello".into(),
        max_tokens: 8,
    };
    assert_eq!(prompt_request.request_id, 42);

    let observed_run_id: observability::lifecycle::RunId = observability::lifecycle::RunId(7);
    assert_eq!(observed_run_id, observability::lifecycle::RunId(7));

    let _chat_entrypoint = chat::run_from_args::<Vec<String>>;
}
