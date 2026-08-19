//! Proves the job runner runs through swactor **inside Myelin's real node
//! composition**: a swactor `Engine` over Tokio owns the core runtime; a real
//! `IrohDriver` is bound through the engine handle; the production distribution
//! stack is built on the same runtime with the job wire codecs registered; an
//! orchestrator FSM actor and a node job actor drive a real command via
//! `swactor-process` over the actor plane, with workspace/output bytes traveling
//! as chunked actor messages. No SSH for the job.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use swactor::actor::Message;
use swactor::runtime::Inbox;
use swactor_engine::{Engine, TokioBackend, TokioConfig};
use swactor_job_runner::{
    register_job_codecs, Job, JobDone, JobState, NodeJobActor, OrchestratorJobActor,
    OrchestratorJobMsg, Workspace,
};

use distribution::node::DistributedNodeConfig;
use iroh::RelayMode;
use iroh_driver::{IrohDriver, IrohDriverConfig};

use crate::orchestration::distribution_stack::DistributionRuntimeStack;

const POLL: Duration = Duration::from_millis(15);
const DEADLINE: Duration = Duration::from_secs(20);

fn recv_within<T: Message>(inbox: &Inbox<T>, deadline: Duration) -> Option<T> {
    let started = Instant::now();
    loop {
        if let Some(v) = inbox.try_recv() {
            return Some(v);
        }
        if started.elapsed() >= deadline {
            return None;
        }
        std::thread::sleep(POLL);
    }
}

fn build_composition() -> (Engine, IrohDriver, DistributionRuntimeStack) {
    let (parts, runtime, codec, transport_router) =
        DistributionRuntimeStack::build_runtime(|c| register_job_codecs(c), None);
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).expect("tokio backend"))
        .expect("engine");
    let mut driver = IrohDriver::with_engine(
        engine.handle(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: RelayMode::Disabled,
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![],
        },
    )
    .expect("iroh driver");
    let stack = DistributionRuntimeStack::new_from_runtime(
        runtime.clone(),
        codec,
        transport_router,
        driver.node_id(),
        DistributedNodeConfig::default(),
        engine.handle(),
    );
    driver.enable_actor_bridge(
        stack.runtime.clone(),
        stack.codec.clone(),
        stack.actor_bridge_routes(),
        stack.actors.swim,
        stack.relay_mirror.clone(),
        stack.route_view.clone(),
        stack.outbox.clone(),
    );
    stack.spawn_protocol_ticker(POLL);
    driver.install_actor_bridge_pump(POLL);
    (engine, driver, stack)
}

#[test]
fn job_runs_through_swactor_inside_myelin_composition() {
    let (engine, _driver, stack) = build_composition();
    let runtime = stack.runtime.clone();
    let sender = runtime.create_sender();

    let ws = tempfile::tempdir().expect("ws");
    std::fs::write(ws.path().join("seed.txt"), "seed-value").expect("seed");
    let node_workdir = tempfile::tempdir().expect("node workdir");
    let landing = tempfile::tempdir().expect("landing");

    let done = runtime.new_inbox::<JobDone>().expect("done inbox");
    let orch = runtime
        .spawn(OrchestratorJobActor::new(*done.addr(), landing.path().to_path_buf()))
        .expect("spawn orchestrator");
    let node = runtime
        .spawn(NodeJobActor::new(orch, node_workdir.path().to_path_buf(), sender, 0))
        .expect("spawn node");

    let job = Job {
        name: "myelin-probe".to_owned(),
        setup: Some("echo setup-ok > setup_done.txt".to_owned()),
        run: "echo hello-from-myelin-swactor > greeting.txt".to_owned(),
        workspace: Some(Workspace { workdir: ws.path().to_path_buf(), exclude: vec![] }),
        outputs: vec![
            "greeting.txt".to_owned(),
            "setup_done.txt".to_owned(),
            "seed.txt".to_owned(),
        ],
        env: BTreeMap::new(),
    };
    runtime
        .send_to(orch, OrchestratorJobMsg::Submit { job, node_actor: node })
        .expect("submit");

    let result = recv_within(&done, DEADLINE);
    drop(engine);
    let done = result.expect("job did not reach a terminal state");
    assert_eq!(done.state, JobState::Completed, "expected COMPLETED, got {:?}", done);
    assert_eq!(done.exit_code, Some(0));

    let greeting = std::fs::read_to_string(landing.path().join("greeting.txt"))
        .expect("collected greeting.txt");
    assert!(greeting.contains("hello-from-myelin-swactor"), "greeting: {greeting}");
    let seed = std::fs::read_to_string(landing.path().join("seed.txt")).expect("collected seed.txt");
    assert_eq!(seed, "seed-value", "workspace materialized + collected through swactor");
}
