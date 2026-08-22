//! Real-iroh two-node proof: an orchestrator composition and a worker
//! composition, each with its own `Engine` + `IrohDriver` + distribution stack,
//! connected over **actual iroh**. The worker registers its `NodeJobActor` in the
//! directory (the production `driver.register_actor` + `register_local_actor`
//! path); the orchestrator joins, the directory converges, and the orchestrator
//! drives the job across iroh — control, workspace bytes, output bytes, and the
//! supervised-process exit all cross the iroh actor plane.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use swactor_job_runner::{
    Job, JobDone, JobState, NodeJobActor, OrchestratorJobActor, OrchestratorJobMsg, Workspace,
};

use iroh_driver::IrohDriver;

use crate::orchestration::distribution_stack::DistributionRuntimeStack;

use crate::tests::harness::build_iroh_composition;

const POLL: Duration = Duration::from_millis(25);
const CONVERGE_DEADLINE: Duration = Duration::from_secs(30);
const JOB_DEADLINE: Duration = Duration::from_secs(30);

#[test]
fn job_runs_across_two_nodes_over_real_iroh() {
    let node_workdir = tempfile::tempdir().expect("node workdir");
    let landing = tempfile::tempdir().expect("landing");
    let ws = tempfile::tempdir().expect("ws");
    std::fs::write(ws.path().join("seed.txt"), "seed-value").expect("seed");

    // Worker composition (B): spawn + register the NodeJobActor so peers can
    // route to it through the converged directory.
    let (engine_b, driver_b, stack_b) = build_iroh_composition(POLL);
    let sender_b = stack_b.runtime.create_sender();
    // Placeholder orchestrator address: the real one is on A; the node only
    // needs it once the orchestrator submits. We point the node at A's
    // orchestrator after it exists (address is fixed below), but the node actor
    // captures the address at construction — so spawn it after A's orchestrator.
    let (_engine_a, _driver_a, stack_a) = build_iroh_composition(POLL);

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

    // Both control directions must be routable before Submit. A learning the
    // worker route does not imply B has already learned the orchestrator route;
    // submitting at that one-way boundary loses the first node event.
    driver_a_join(&stack_a, &_driver_a, &driver_b);
    stack_a.register_local_actor(_driver_a.register_actor(orch, 1));

    let converged = wait_until(CONVERGE_DEADLINE, || {
        stack_a.route_owner(job_actor).is_some() && stack_b.route_owner(orch).is_some()
    });
    assert!(
        converged,
        "directory did not converge bidirectionally for orchestrator and worker actors"
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
