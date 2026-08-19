//! Behavior guarantees for the `node` module.
//!
//! These tests use the engine's deterministic stepping backend to verify actor
//! message routing without constructing or driving a runtime directly.

use crate::node_actor::{NodeAgentActor, NodeAgentMsg, NodeAgentReport};
use crate::orchestration::actor::OrchestratorMsg;
use iroh::{EndpointAddr, SecretKey};
use myelin::staging as stage;
use swactor::actor::ActorAddress;
use swactor::config::RuntimeConfig;
use swactor::runtime::RuntimeParts;
use swactor_engine::{Engine, SteppingBackend};

#[test]
fn node_agent_runtime_loaded_reports_orchestrator() {
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let backend = SteppingBackend::new();
    let _engine = Engine::new(parts, backend.clone()).expect("stepping engine");
    let orchestrator_inbox = runtime
        .new_inbox::<OrchestratorMsg>()
        .expect("orchestrator inbox");
    let orchestrator = *orchestrator_inbox.addr();
    let node_actor = ActorAddress::new_random();
    let endpoint = EndpointAddr::new(SecretKey::from_bytes(&[9; 32]).public());
    let actor = runtime
        .spawn(NodeAgentActor::new(stage::NodeId(11), orchestrator, None))
        .expect("spawn node agent");

    runtime
        .send_to(
            actor,
            NodeAgentMsg::RuntimeLoaded {
                run_id: 7,
                node_id: 11,
                stage_index: 3,
                endpoint: endpoint.clone(),
                node_actor,
                readiness_id: 99,
            },
        )
        .expect("send runtime loaded");
    backend.step();

    assert_eq!(
        orchestrator_inbox.try_recv(),
        Some(OrchestratorMsg::ObserveNodeRuntimeReady {
            run_id: 7,
            node_id: 11,
            stage_index: 3,
            endpoint,
            node_actor,
            readiness_id: 99,
        })
    );
}

#[test]
fn node_agent_runtime_ready_ack_reports_worker_loop() {
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let backend = SteppingBackend::new();
    let _engine = Engine::new(parts, backend.clone()).expect("stepping engine");
    let orchestrator_inbox = runtime
        .new_inbox::<OrchestratorMsg>()
        .expect("orchestrator inbox");
    let reports = runtime
        .new_inbox::<NodeAgentReport>()
        .expect("node report inbox");
    let actor = runtime
        .spawn(NodeAgentActor::new(
            stage::NodeId(11),
            *orchestrator_inbox.addr(),
            Some(*reports.addr()),
        ))
        .expect("spawn node agent");

    runtime
        .send_to(
            actor,
            NodeAgentMsg::RuntimeReadyAck {
                run_id: 7,
                node_id: 11,
                stage_index: 3,
                readiness_id: 99,
            },
        )
        .expect("send runtime ready ack");
    backend.step();

    assert_eq!(
        reports.try_recv(),
        Some(NodeAgentReport::RuntimeReadyAck {
            run_id: 7,
            node_id: 11,
            stage_index: 3,
            readiness_id: 99,
        })
    );
    assert_eq!(reports.try_recv(), None);
}
