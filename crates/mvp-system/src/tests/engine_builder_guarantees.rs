//! Black-box contract tests for the pool-based engine builder.
//!
//! The builder contract is topology/lifecycle only: it acquires a neutral node
//! pool, launches the same node image, waits for readiness/convergence, lets a
//! planner assign roles, and stays agnostic to workload input semantics.

use mvp_system::engine_builder as engine;
use mvp_system::engine_builder::WorkloadAdapter;

fn model() -> engine::ModelSpec {
    engine::ModelSpec::mvp_tiny_open_llm_fixture()
}

fn image() -> engine::NodeImageSpec {
    engine::NodeImageSpec::new("mvp-node:cuda").worker_runtime(
        engine::WorkerRuntimeSpec::TinygradCuda {
            worker_script: "/opt/mvp/mvp_tinygrad_worker.py".to_owned(),
            device_env: "CUDA".to_owned(),
        },
    )
}

fn full_pool() -> engine::StaticPoolProvider {
    engine::StaticPoolProvider::new(vec![
        engine::NodeLease::new(
            "coordinator",
            engine::NodeId(900),
            [engine::NodeCapability::Coordinator],
        )
        .resources(engine::ResourceFacts::cpu_only(2, 2 << 30)),
        engine::NodeLease::new(
            "worker-0",
            engine::NodeId(11),
            [engine::NodeCapability::Worker],
        )
        .resources(engine::ResourceFacts::cuda(1, 8 << 30, 4, 8 << 30)),
        engine::NodeLease::new(
            "worker-1",
            engine::NodeId(12),
            [engine::NodeCapability::Worker],
        )
        .resources(engine::ResourceFacts::cuda(1, 8 << 30, 4, 8 << 30)),
    ])
}

#[test]
fn builder_launches_pool_converges_and_assigns_planned_roles() {
    let cluster = engine::ClusterBuilder::new("cluster-a", model())
        .run_id(77)
        .image(image())
        .pool_provider(full_pool())
        .launcher(engine::StaticNodeLauncher)
        .planner(engine::FixedLinearPipelinePlanner::new(2))
        .launch()
        .expect("launch cluster");

    let summaries = cluster.node_summaries();
    let coordinator = summaries
        .iter()
        .find(|node| node.node_id == engine::NodeId(900))
        .expect("coordinator summary");
    assert_eq!(coordinator.roles, vec![engine::RoleKind::Coordinator]);
    let worker0 = summaries
        .iter()
        .find(|node| node.node_id == engine::NodeId(11))
        .expect("worker 0 summary");
    assert_eq!(
        worker0.roles,
        vec![engine::RoleKind::StageWorker { stage_index: 0 }]
    );
    let worker1 = summaries
        .iter()
        .find(|node| node.node_id == engine::NodeId(12))
        .expect("worker 1 summary");
    assert_eq!(
        worker1.roles,
        vec![engine::RoleKind::StageWorker { stage_index: 1 }]
    );

    let plan = cluster.role_plan();
    assert_eq!(plan.run_plan.stages.len(), 2);
    assert_eq!(plan.run_plan.edges.len(), 3);
    assert_eq!(plan.run_plan.stages[0].layer_start, 0);
    assert_eq!(plan.run_plan.stages[0].layer_end_exclusive, 2);
    assert_eq!(plan.run_plan.stages[1].layer_start, 2);
    assert_eq!(plan.run_plan.stages[1].layer_end_exclusive, 4);
    assert!(
        cluster
            .events()
            .contains(&engine::EngineEvent::EngineReady {
                cluster_id: "cluster-a".to_owned()
            })
    );

    let shutdown_events = cluster.shutdown().expect("shutdown cluster");
    assert!(
        shutdown_events.contains(&engine::EngineEvent::ShutdownComplete {
            cluster_id: "cluster-a".to_owned()
        })
    );
}

#[test]
fn planner_rejects_when_pool_cannot_supply_stage_workers() {
    let short_pool = engine::StaticPoolProvider::new(vec![
        engine::NodeLease::new(
            "coordinator",
            engine::NodeId(900),
            [engine::NodeCapability::Coordinator],
        ),
        engine::NodeLease::new(
            "worker-0",
            engine::NodeId(11),
            [engine::NodeCapability::Worker],
        ),
        engine::NodeLease::new(
            "observer",
            engine::NodeId(901),
            [engine::NodeCapability::Coordinator],
        ),
    ]);

    let result = engine::ClusterBuilder::new("cluster-short", model())
        .run_id(77)
        .image(image())
        .pool_provider(short_pool)
        .launcher(engine::StaticNodeLauncher)
        .planner(engine::FixedLinearPipelinePlanner::new(2))
        .launch();

    match result {
        Err(engine::EngineBuildError::Planning(engine::PlanningError::InsufficientWorkers {
            required,
            available,
        })) => {
            assert_eq!(required, 2);
            assert_eq!(available, 1);
        }
        Ok(_) => panic!("expected insufficient worker planning error, got launched cluster"),
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

struct ProbeWorkload;

impl engine::WorkloadAdapter for ProbeWorkload {
    type Input = Vec<&'static str>;
    type Output = usize;
    type Error = std::convert::Infallible;

    fn submit(
        &self,
        _cluster: &mut engine::ClusterHandle,
        input: Self::Input,
    ) -> Result<Self::Output, Self::Error> {
        Ok(input.len())
    }
}

#[test]
fn workload_input_semantics_live_outside_the_cluster_builder() {
    let mut cluster = engine::ClusterBuilder::new("cluster-opaque", model())
        .run_id(77)
        .image(image())
        .pool_provider(full_pool())
        .launcher(engine::StaticNodeLauncher)
        .planner(engine::FixedLinearPipelinePlanner::new(2))
        .launch()
        .expect("launch cluster");

    let observed = ProbeWorkload
        .submit(&mut cluster, vec!["not", "tokens"])
        .expect("submit probe workload");
    assert_eq!(observed, 2);

    cluster.shutdown().expect("shutdown cluster");
}

#[test]
fn runtime_node_builds_the_reusable_iroh_swactor_stack() {
    let mut node = engine::RuntimeNode::start_default().expect("start runtime node");

    let orchestrator = node
        .spawn_orchestrator_actor(
            mvp_system::orchestrator_run_fsm::RunConfig {
                run_id: mvp_system::orchestrator_run_fsm::RunId(77),
                max_tokens: 1,
                prompt: vec![1, 2, 3],
            },
            None,
        )
        .expect("spawn orchestrator actor");
    let worker = node
        .spawn_node_agent_actor(mvp_system::stage_controller::NodeId(11), orchestrator, None)
        .expect("spawn node agent actor");

    node.wait_for_routes(&[orchestrator, worker], std::time::Duration::from_secs(1))
        .expect("local actor routes");
    assert_eq!(node.node_id().0, *node.endpoint_addr().id.as_bytes());
}
