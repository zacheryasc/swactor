//! Real-iroh two-node proof: an orchestrator composition and a worker
//! composition, each with its own `Engine` + `IrohDriver` + distribution stack,
//! connected over **actual iroh**. The worker registers its `NodeJobActor` in the
//! directory (the production `driver.register_actor` + `register_local_actor`
//! path); the orchestrator joins, the directory converges, and the orchestrator
//! drives the job across iroh — control, workspace bytes, output bytes, and the
//! supervised-process exit all cross the iroh actor plane.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use swactor::actor::Message;
use swactor::runtime::Inbox;
use swactor_engine::{Engine, TokioBackend, TokioConfig};
use swactor_job_runner::{
    Job, JobDone, JobState, NodeJobActor, OrchestratorJobActor, OrchestratorJobMsg, Workspace,
    register_job_codecs,
};

use distribution::node::DistributedNodeConfig;
use iroh::RelayMode;
use iroh_driver::{IrohDriver, IrohDriverConfig};

use crate::orchestration::distribution_stack::DistributionRuntimeStack;

const POLL: Duration = Duration::from_millis(25);
const CONVERGE_DEADLINE: Duration = Duration::from_secs(30);
const JOB_DEADLINE: Duration = Duration::from_secs(30);

fn build_composition() -> (Engine, IrohDriver, DistributionRuntimeStack) {
    let (parts, runtime, codec, transport_router) =
        DistributionRuntimeStack::build_runtime(|c| register_job_codecs(c), None);
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).expect("tokio backend"),
    )
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
fn job_runs_across_two_nodes_over_real_iroh() {
    let node_workdir = tempfile::tempdir().expect("node workdir");
    let landing = tempfile::tempdir().expect("landing");
    let ws = tempfile::tempdir().expect("ws");
    std::fs::write(ws.path().join("seed.txt"), "seed-value").expect("seed");

    // Worker composition (B): spawn + register the NodeJobActor so peers can
    // route to it through the converged directory.
    let (engine_b, driver_b, stack_b) = build_composition();
    let sender_b = stack_b.runtime.create_sender();
    let done_b_echo = stack_b
        .runtime
        .new_inbox::<JobDone>()
        .expect("worker echo inbox");
    // Placeholder orchestrator address: the real one is on A; the node only
    // needs it once the orchestrator submits. We point the node at A's
    // orchestrator after it exists (address is fixed below), but the node actor
    // captures the address at construction — so spawn it after A's orchestrator.
    let (_engine_a, _driver_a, stack_a) = build_composition();

    let done = stack_a
        .runtime
        .new_inbox::<JobDone>()
        .expect("orchestrator done inbox");
    let orch = stack_a
        .runtime
        .spawn(OrchestratorJobActor::new(
            *done.addr(),
            landing.path().to_path_buf(),
        ))
        .expect("spawn orchestrator on A");

    let job_actor = stack_b
        .runtime
        .spawn(NodeJobActor::new(
            orch,
            node_workdir.path().to_path_buf(),
            sender_b,
            0,
        ))
        .expect("spawn node job actor on B");
    stack_b.register_local_actor(driver_b.register_actor(job_actor, 1));

    // Orchestrator (A) joins the worker (B) over iroh and waits for the directory
    // to converge: A's route view must learn job_actor → B.
    driver_a_join(&stack_a, &_driver_a, &driver_b);
    stack_a.register_local_actor(_driver_a.register_actor(orch, 1));

    let converged = wait_until(CONVERGE_DEADLINE, || {
        stack_a.route_view.read().unwrap().contains_key(&job_actor)
    });
    assert!(
        converged,
        "directory did not converge: A never learned the worker's NodeJobActor"
    );

    let job = Job {
        name: "iroh-probe".to_owned(),
        setup: Some("echo setup-ok > setup_done.txt".to_owned()),
        run: "echo hello-over-iroh > greeting.txt".to_owned(),
        workspace: Some(Workspace {
            workdir: ws.path().to_path_buf(),
            exclude: vec![],
        }),
        outputs: vec![
            "greeting.txt".to_owned(),
            "setup_done.txt".to_owned(),
            "seed.txt".to_owned(),
        ],
        env: BTreeMap::new(),
    };
    stack_a
        .runtime
        .send_to(
            orch,
            OrchestratorJobMsg::Submit {
                job,
                node_actor: job_actor,
            },
        )
        .expect("submit");
    let mut outcome = None;
    let started = Instant::now();
    while started.elapsed() < JOB_DEADLINE {
        if let Some(d) = done.try_recv() {
            outcome = Some(d);
            break;
        }
        std::thread::sleep(POLL);
    }
    drop(_engine_a);
    drop(engine_b);
    let done = outcome.expect("job did not complete across iroh within deadline");
    assert_eq!(
        done.state,
        JobState::Completed,
        "expected COMPLETED over iroh, got {:?}",
        done
    );
    assert_eq!(done.exit_code, Some(0));
    let greeting =
        std::fs::read_to_string(landing.path().join("greeting.txt")).expect("collected greeting");
    assert!(greeting.contains("hello-over-iroh"), "greeting: {greeting}");
    let seed = std::fs::read_to_string(landing.path().join("seed.txt")).expect("collected seed");
    assert_eq!(seed, "seed-value", "workspace crossed iroh through swactor");
}

fn driver_a_join(
    _stack_a: &DistributionRuntimeStack,
    driver_a: &IrohDriver,
    driver_b: &IrohDriver,
) {
    driver_a.join(std::slice::from_ref(&driver_b.endpoint_addr()));
}

#[allow(clippy::disallowed_methods)]
fn wait_until(deadline: Duration, mut check: impl FnMut() -> bool) -> bool {
    let started = Instant::now();
    loop {
        if check() {
            return true;
        }
        if started.elapsed() >= deadline {
            return false;
        }
        std::thread::sleep(POLL);
    }
}
