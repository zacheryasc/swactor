use std::collections::{BTreeMap, BTreeSet};

use crate::run_fsm as fsm;
use crate::run_plan as plan;
use data_plane::edge_actor;
use mvp_system::observability::lifecycle as obs;
use mvp_system::orchestration::engine_builder as engine;
use mvp_system::staging as stage;

use super::mock_node::MockNode;
use super::mock_transport::{Delivery, MockObject, MockObjectKind, MockTransport};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalMockConfig {
    pub stage_count: u32,
    pub max_tokens: u32,
    pub eos_after_sequence: u64,
}

impl Default for LocalMockConfig {
    fn default() -> Self {
        Self {
            stage_count: 2,
            max_tokens: 4,
            eos_after_sequence: 1,
        }
    }
}

pub struct LocalMockCluster {
    run_id: plan::RunId,
    orchestrator_node_id: plan::NodeId,
    max_tokens: u32,
    engine_events: Vec<engine::EngineEvent>,
    plan: plan::RunPlan,
    nodes: BTreeMap<u32, MockNode>,
    orchestrator: Option<fsm::OrchestratorHarness>,
    orchestrator_command_cursor: usize,
    orchestrator_event_cursor: usize,
    trace: Vec<obs::Event>,
    transport: MockTransport,
    resources: ResourceTracker,
    observed_edges: BTreeSet<plan::EdgeId>,
    object_allocators: BTreeMap<plan::EdgeId, edge_actor::ObjectIdAllocator>,
    scenario: LocalMockScenario,
}

#[derive(Clone, Debug)]
pub struct LocalMockOutcome {
    pub trace: Vec<obs::Event>,
    pub engine_events: Vec<engine::EngineEvent>,
    pub injected_sequences: Vec<u64>,
    pub stage_count: usize,
    pub live_edges: usize,
    pub live_rings: usize,
    pub live_stage_runs: usize,
    pub transport_delivery_count: usize,
    pub transport_deliveries: Vec<Delivery>,
    pub edge_chain: Vec<plan::EdgeId>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum LocalMockScenario {
    #[default]
    Happy,
    UnauthorizedProvision {
        stage_index: u32,
    },
    WorkerCrashDuringExecution {
        stage_index: u32,
        sequence: u64,
    },
    SequenceViolation {
        stage_index: u32,
        sequence: u64,
    },
}

#[derive(Default)]
struct ResourceTracker {
    live_edges: BTreeSet<u64>,
    live_rings: BTreeSet<u64>,
    live_stage_runs: BTreeSet<u32>,
}

impl ResourceTracker {
    fn provision_edge(&mut self, edge_id: plan::EdgeId) {
        self.live_edges.insert(edge_id.0);
        self.live_rings.insert(20_000 + edge_id.0);
    }

    fn provision_stage_run(&mut self, stage_index: u32) {
        self.live_stage_runs.insert(stage_index);
    }

    fn release_stage_run(&mut self, stage_index: u32) {
        self.live_stage_runs.remove(&stage_index);
    }

    fn release_all_edges(&mut self) {
        self.live_edges.clear();
        self.live_rings.clear();
    }
}

fn mock_pool(orchestrator_node_id: plan::NodeId, stage_count: u32) -> engine::StaticPoolProvider {
    let mut leases = Vec::with_capacity(stage_count as usize + 1);
    leases.push(
        engine::NodeLease::new(
            "mock-coordinator",
            engine::NodeId(orchestrator_node_id.0),
            [engine::NodeCapability::Coordinator],
        )
        .resources(engine::ResourceFacts::cpu_only(2, 2 << 30)),
    );
    for stage_index in 0..stage_count {
        leases.push(
            engine::NodeLease::new(
                format!("mock-worker-{stage_index}"),
                engine::NodeId(11 + u64::from(stage_index)),
                [engine::NodeCapability::Worker],
            )
            .resources(engine::ResourceFacts::cpu_only(2, 2 << 30)),
        );
    }
    engine::StaticPoolProvider::new(leases)
}

impl LocalMockCluster {
    pub fn two_stage() -> Self {
        Self::with_config(LocalMockConfig::default())
    }

    pub fn with_config(config: LocalMockConfig) -> Self {
        assert!(
            config.stage_count > 0,
            "local mock needs at least one stage"
        );
        assert!(config.max_tokens > 0, "local mock needs at least one token");

        let run_id = plan::RunId(77);
        let orchestrator_node_id = plan::NodeId(900);
        let engine_cluster = engine::ClusterBuilder::new(
            "local-mock",
            engine::ModelSpec::pipelined_causal_llm(
                "mock-gguf",
                engine::ModelArtifact::TestTinyLlm {
                    path: "local-mock://mock-gguf".to_owned(),
                },
                config.stage_count * 2,
                8,
                engine::DTypeFamily::BFloat,
                2,
                8,
                99,
                plan::TokenizerSource::EmbeddedGguf,
            ),
        )
        .run_id(run_id.0)
        .image(
            engine::NodeImageSpec::new("local-mock-node")
                .worker_runtime(engine::WorkerRuntimeSpec::DumbProcess),
        )
        .pool_provider(mock_pool(orchestrator_node_id, config.stage_count))
        .launcher(engine::StaticNodeLauncher)
        .planner(
            engine::FixedLinearPipelinePlanner::new(config.stage_count).runtime(
                plan::RuntimeConfig {
                    max_tokens: config.max_tokens,
                    prompt: plan::PromptSource::Inline("local mock prompt".to_owned()),
                    sampling: plan::SamplingPolicy {
                        temperature_millis: 0,
                        top_k: 1,
                    },
                    token_output_policy: plan::TokenOutputPolicy::EmitAll,
                },
            ),
        )
        .launch()
        .expect("local mock engine builder must launch");
        let engine_events = engine_cluster.events().to_vec();
        let role_plan = engine_cluster.role_plan().clone();
        engine_cluster
            .shutdown()
            .expect("local mock engine builder must shutdown");
        let plan = role_plan.run_plan;
        let nodes = plan
            .stages
            .iter()
            .map(|stage| {
                (
                    stage.stage_index,
                    MockNode::new(
                        stage.stage_index,
                        stage.node_id,
                        stage.stage_count,
                        config.eos_after_sequence,
                    ),
                )
            })
            .collect();
        Self {
            run_id,
            orchestrator_node_id,
            engine_events,
            max_tokens: config.max_tokens,
            plan,
            nodes,
            orchestrator: None,
            orchestrator_command_cursor: 0,
            orchestrator_event_cursor: 0,
            trace: Vec::new(),
            transport: MockTransport::default(),
            resources: ResourceTracker::default(),
            observed_edges: BTreeSet::new(),
            object_allocators: BTreeMap::new(),
            scenario: LocalMockScenario::Happy,
        }
    }

    pub fn run_prompt(&mut self, prompt: &str) -> LocalMockOutcome {
        self.run_prompt_in_scenario(prompt, LocalMockScenario::Happy)
    }

    pub fn run_prompt_with_delayed_stage_ready(
        &mut self,
        prompt: &str,
        delayed_stage_index: u32,
    ) -> LocalMockOutcome {
        self.start_run(prompt, LocalMockScenario::Happy);
        self.process_orchestrator_commands_with_delayed_ready(Some(delayed_stage_index));
        assert_eq!(
            self.count_kind(obs::EventKind::PromptInjected),
            0,
            "prompt injection must be gated while one stage is not ready"
        );
        self.mark_stage_ready(delayed_stage_index);
        self.process_orchestrator_commands();
        self.finish_outcome()
    }

    pub fn run_with_unauthorized_provision(
        &mut self,
        prompt: &str,
        stage_index: u32,
    ) -> LocalMockOutcome {
        self.run_prompt_in_scenario(
            prompt,
            LocalMockScenario::UnauthorizedProvision { stage_index },
        )
    }

    pub fn run_with_worker_crash_during_execution(
        &mut self,
        prompt: &str,
        stage_index: u32,
        sequence: u64,
    ) -> LocalMockOutcome {
        self.run_prompt_in_scenario(
            prompt,
            LocalMockScenario::WorkerCrashDuringExecution {
                stage_index,
                sequence,
            },
        )
    }

    pub fn run_with_sequence_violation(
        &mut self,
        prompt: &str,
        stage_index: u32,
        sequence: u64,
    ) -> LocalMockOutcome {
        self.run_prompt_in_scenario(
            prompt,
            LocalMockScenario::SequenceViolation {
                stage_index,
                sequence,
            },
        )
    }

    pub fn run_wrong_edge_object_then_prompt(
        &mut self,
        prompt: &str,
        stage_index: u32,
    ) -> LocalMockOutcome {
        self.start_run(prompt, LocalMockScenario::Happy);
        self.process_orchestrator_commands_until(|command| {
            matches!(
                command,
                fsm::RunCommand::CreateTokenInEndpoint { .. }
                    | fsm::RunCommand::CreateTokenOutEndpoint { .. }
            )
        });
        assert!(
            !self.inject_wrong_edge_object(stage_index),
            "wrong-edge object must not reach ExecuteStep"
        );
        self.process_orchestrator_commands();
        self.finish_outcome()
    }

    fn run_prompt_in_scenario(
        &mut self,
        prompt: &str,
        scenario: LocalMockScenario,
    ) -> LocalMockOutcome {
        self.start_run(prompt, scenario);
        self.process_orchestrator_commands();
        self.finish_outcome()
    }

    fn start_run(&mut self, prompt: &str, scenario: LocalMockScenario) {
        self.trace.clear();
        self.transport = MockTransport::default();
        self.resources = ResourceTracker::default();
        self.observed_edges.clear();
        self.object_allocators.clear();
        self.orchestrator_command_cursor = 0;
        self.orchestrator_event_cursor = 0;
        self.scenario = scenario;
        self.orchestrator = Some(fsm::OrchestratorHarness::new(fsm::RunConfig {
            run_id: fsm::RunId(self.run_id.0),
            max_tokens: u64::from(self.max_tokens),
            prompt: tokenize(prompt),
        }));

        let stage_node_ids: Vec<_> = self.plan.stages.iter().map(|stage| stage.node_id).collect();
        for node_id in &stage_node_ids {
            self.push_node(obs::EventKind::NodeStarted, *node_id);
            self.push_node(obs::EventKind::NodeAvailable, *node_id);
        }

        self.push_run(obs::EventKind::PoolReady, obs::Component::Membership);
        self.orchestrator_mut().observe(fsm::RunEvent::PoolReady {
            nodes: stage_node_ids
                .iter()
                .map(|node_id| fsm::NodeId(node_id.0))
                .collect(),
        });

        self.push_run(obs::EventKind::RunPlanned, obs::Component::Orchestrator);
        let fsm_plan = self.fsm_plan();
        self.orchestrator_mut()
            .observe(fsm::RunEvent::PlanAvailable(fsm_plan));
    }

    fn process_orchestrator_commands(&mut self) {
        self.process_orchestrator_commands_with_delayed_ready(None);
    }

    fn process_orchestrator_commands_with_delayed_ready(&mut self, delayed_stage: Option<u32>) {
        self.process_orchestrator_commands_until_with_delayed_ready(|_| false, delayed_stage);
    }

    fn process_orchestrator_commands_until<F>(&mut self, should_pause: F)
    where
        F: FnMut(&fsm::RunCommand) -> bool,
    {
        self.process_orchestrator_commands_until_with_delayed_ready(should_pause, None);
    }

    fn process_orchestrator_commands_until_with_delayed_ready<F>(
        &mut self,
        mut should_pause: F,
        delayed_stage: Option<u32>,
    ) where
        F: FnMut(&fsm::RunCommand) -> bool,
    {
        loop {
            self.drain_orchestrator_lifecycle();
            let Some(command) = self.peek_orchestrator_command() else {
                break;
            };
            if should_pause(&command) {
                break;
            }
            let command = self
                .next_orchestrator_command()
                .expect("peeked command must still be present");
            self.process_orchestrator_command(command, delayed_stage);
        }
        self.drain_orchestrator_lifecycle();
    }

    fn peek_orchestrator_command(&self) -> Option<fsm::RunCommand> {
        self.orchestrator
            .as_ref()?
            .commands()
            .get(self.orchestrator_command_cursor)
            .cloned()
    }

    fn next_orchestrator_command(&mut self) -> Option<fsm::RunCommand> {
        let command = self.peek_orchestrator_command()?;
        self.orchestrator_command_cursor += 1;
        Some(command)
    }

    fn process_orchestrator_command(
        &mut self,
        command: fsm::RunCommand,
        delayed_stage: Option<u32>,
    ) {
        if self.terminal_observed()
            && !matches!(
                command,
                fsm::RunCommand::StopRun { .. } | fsm::RunCommand::TearDownTokenEndpoints { .. }
            )
        {
            return;
        }

        match command {
            fsm::RunCommand::ProvisionStage { provision } => {
                self.provision_stage(provision.stage_index, delayed_stage);
            }
            fsm::RunCommand::CreateTokenInEndpoint { .. } => {
                self.orchestrator_mut()
                    .observe(fsm::RunEvent::TokenInEndpointReady);
            }
            fsm::RunCommand::CreateTokenOutEndpoint { .. } => {
                self.orchestrator_mut()
                    .observe(fsm::RunEvent::TokenOutEndpointReady);
            }
            fsm::RunCommand::InjectTokenObject { object, .. } => self.inject_token_object(object),
            fsm::RunCommand::StopRun { stage_index, .. } => self.stop_stage(stage_index),
            fsm::RunCommand::TearDownTokenEndpoints { .. } => {
                self.resources.release_all_edges();
                self.orchestrator_mut()
                    .observe(fsm::RunEvent::TokenEndpointsStopped);
            }
        }
    }

    fn provision_stage(&mut self, stage_index: u32, delayed_stage: Option<u32>) {
        let provision = plan::derive_stage_provision(&self.plan, stage_index)
            .expect("orchestrator must provision planned stages");
        self.resources.provision_stage_run(stage_index);
        self.push_stage(
            obs::EventKind::StageProvisionStarted,
            stage_index,
            obs::Component::Orchestrator,
        );
        self.push_stage(
            obs::EventKind::WeightsDownloadStarted,
            stage_index,
            obs::Component::WeightLifecycle,
        );
        self.push_stage(
            obs::EventKind::WeightsDownloaded,
            stage_index,
            obs::Component::WeightLifecycle,
        );
        self.push_stage(
            obs::EventKind::WeightsLoaded,
            stage_index,
            obs::Component::WeightLifecycle,
        );
        self.record_edge_ready(provision.inbound.edge_id);
        self.record_edge_ready(provision.outbound.edge_id);

        let stage_provision = self.to_stage_provision(provision);
        let lifecycle_events = {
            let node = self
                .nodes
                .get_mut(&stage_index)
                .expect("mock node must exist for stage");
            if matches!(self.scenario, LocalMockScenario::UnauthorizedProvision { stage_index: target } if target == stage_index)
            {
                node.provision_from_wrong_orchestrator(stage_provision);
            } else {
                node.provision(stage::NodeId(self.orchestrator_node_id.0), stage_provision);
                if delayed_stage != Some(stage_index) {
                    node.mark_ready();
                }
            }
            node.drain_lifecycle_events()
        };
        for event in lifecycle_events {
            self.handle_stage_lifecycle_event(event);
        }
    }

    fn mark_stage_ready(&mut self, stage_index: u32) {
        let lifecycle_events = {
            let node = self
                .nodes
                .get_mut(&stage_index)
                .expect("mock node must exist for stage");
            node.mark_ready();
            node.drain_lifecycle_events()
        };
        for event in lifecycle_events {
            self.handle_stage_lifecycle_event(event);
        }
    }

    fn record_edge_ready(&mut self, edge_id: plan::EdgeId) {
        if self.observed_edges.insert(edge_id) {
            self.resources.provision_edge(edge_id);
            self.push_edge(obs::EventKind::EdgeProvisionStarted, edge_id);
            self.push_edge(obs::EventKind::EdgeReady, edge_id);
        }
    }

    fn inject_token_object(&mut self, object: fsm::TokenObjectInjection) {
        debug_assert!(matches!(
            object.payload,
            fsm::TokenObjectPayload::Prompt { .. } | fsm::TokenObjectPayload::Decode { .. }
        ));
        if !self
            .trace
            .iter()
            .any(|event| event.kind() == obs::EventKind::ReadinessBarrierPassed)
        {
            self.push_run(
                obs::EventKind::ReadinessBarrierPassed,
                obs::Component::Orchestrator,
            );
        }
        let token_in_edge = self.edge_by_kind(plan::EdgeKind::TokenIn).edge_id;
        let object_id = self.allocate_object_id(token_in_edge);
        self.push_object(
            obs::EventKind::PromptInjected,
            object_id,
            object.sequence,
            obs::Component::TokenEndpoint,
        );
        let token_id = match object.payload {
            fsm::TokenObjectPayload::Prompt { .. } => None,
            fsm::TokenObjectPayload::Decode { token_id, .. } => Some(token_id),
        };
        let object = self.transport.deliver(MockObject {
            edge_id: token_in_edge,
            object_id,
            sequence: object.sequence,
            kind: MockObjectKind::Token,
            token_id,
            eos: false,
        });
        self.route_object_through_stages(object);
    }

    fn route_object_through_stages(&mut self, object: MockObject) {
        let mut current = object;
        let stage_indices: Vec<_> = self
            .plan
            .stages
            .iter()
            .map(|stage| stage.stage_index)
            .collect();
        for stage_index in stage_indices {
            let mut stage_object = current;
            if self.scenario
                == (LocalMockScenario::SequenceViolation {
                    stage_index,
                    sequence: stage_object.sequence,
                })
            {
                stage_object.sequence += 1;
            }
            self.push_object(
                obs::EventKind::ObjectLoaded,
                stage_object.object_id,
                stage_object.sequence,
                obs::Component::GpuWorkerCtl,
            );

            if self.scenario
                == (LocalMockScenario::WorkerCrashDuringExecution {
                    stage_index,
                    sequence: stage_object.sequence,
                })
            {
                let lifecycle_events = {
                    let node = self
                        .nodes
                        .get_mut(&stage_index)
                        .expect("mock node must exist for stage");
                    node.crash_worker();
                    node.drain_lifecycle_events()
                };
                for event in lifecycle_events {
                    self.handle_stage_lifecycle_event(event);
                }
                return;
            }

            let execution = {
                let node = self
                    .nodes
                    .get_mut(&stage_index)
                    .expect("mock node must exist for stage");
                let execution = node.execute_loaded_object(stage_object);
                let lifecycle_events = node.drain_lifecycle_events();
                (execution, lifecycle_events)
            };
            for event in execution.1 {
                self.handle_stage_lifecycle_event(event);
            }
            let Some(execution) = execution.0 else {
                return;
            };
            self.push_step(obs::EventKind::ExecuteStepStarted, execution.step_id);
            self.push_object(
                obs::EventKind::ObjectProduced,
                execution.produced.object_id,
                execution.produced.sequence,
                obs::Component::GpuWorkerCtl,
            );
            self.push_step(obs::EventKind::StepCompleted, execution.step_id);
            let delivered = self.transport.deliver(execution.produced);
            let expected_edge = self
                .plan
                .stages
                .iter()
                .find(|stage| stage.stage_index == stage_index)
                .expect("mock stage must exist")
                .outbound_edge;
            debug_assert_eq!(delivered.edge_id, expected_edge);
            current = delivered;
        }
        self.push_object(
            obs::EventKind::TokenReceived,
            current.object_id,
            current.sequence,
            obs::Component::TokenEndpoint,
        );
        self.orchestrator_mut()
            .observe(fsm::RunEvent::TokenReceived {
                sequence: current.sequence,
                token_id: current.token_id.expect("last stage must produce token"),
                eos: current.eos,
            });
        self.drain_orchestrator_lifecycle();
    }

    fn inject_wrong_edge_object(&mut self, stage_index: u32) -> bool {
        let stage = self
            .plan
            .stages
            .iter()
            .find(|stage| stage.stage_index == stage_index)
            .expect("mock stage must exist");
        let wrong_edge = stage.outbound_edge;
        let object_id = self.allocate_object_id(wrong_edge);
        let object = MockObject {
            edge_id: wrong_edge,
            object_id,
            sequence: 0,
            kind: MockObjectKind::Token,
            token_id: None,
            eos: false,
        };
        self.push_object(
            obs::EventKind::ObjectLoaded,
            object.object_id,
            object.sequence,
            obs::Component::GpuWorkerCtl,
        );
        let (execution, lifecycle_events) = {
            let node = self
                .nodes
                .get_mut(&stage_index)
                .expect("mock node must exist for stage");
            let execution = node.execute_loaded_object(object);
            let lifecycle_events = node.drain_lifecycle_events();
            (execution, lifecycle_events)
        };
        for event in lifecycle_events {
            self.handle_stage_lifecycle_event(event);
        }
        execution.is_some()
    }

    fn stop_stage(&mut self, stage_index: u32) {
        self.push_stage(
            obs::EventKind::StopRunSent,
            stage_index,
            obs::Component::Orchestrator,
        );
        let lifecycle_events = {
            let node = self
                .nodes
                .get_mut(&stage_index)
                .expect("mock node must exist for stage");
            node.stop(self.run_id);
            node.drain_lifecycle_events()
        };
        for event in lifecycle_events {
            self.handle_stage_lifecycle_event(event);
        }
    }

    fn handle_stage_lifecycle_event(&mut self, event: stage::StageLifecycleEvent) {
        match event {
            stage::StageLifecycleEvent::StageReady {
                run_id,
                stage_index,
            } => {
                self.push_stage(
                    obs::EventKind::StageReady,
                    stage_index,
                    obs::Component::StageController,
                );
                self.orchestrator_mut().observe(fsm::RunEvent::StageReady {
                    run_id: fsm::RunId(run_id.0),
                    stage_index,
                });
            }
            stage::StageLifecycleEvent::StageStopped {
                run_id,
                stage_index,
            } => {
                self.resources.release_stage_run(stage_index);
                self.push_stage(
                    obs::EventKind::StageStopped,
                    stage_index,
                    obs::Component::StageController,
                );
                self.orchestrator_mut()
                    .observe(fsm::RunEvent::StageStopped {
                        run_id: fsm::RunId(run_id.0),
                        stage_index,
                    });
            }
            stage::StageLifecycleEvent::StageFault {
                run_id,
                stage_index,
                ..
            } => {
                self.push_stage(
                    obs::EventKind::StageFaulted,
                    stage_index,
                    obs::Component::StageController,
                );
                self.orchestrator_mut().observe(fsm::RunEvent::StageFault {
                    run_id: fsm::RunId(run_id.0),
                    stage_index,
                    reason: fsm::StageFaultReason::WorkerCrashed,
                });
            }
            stage::StageLifecycleEvent::StepAccepted { .. } => {}
        }
        self.drain_orchestrator_lifecycle();
    }

    fn drain_orchestrator_lifecycle(&mut self) {
        let Some(orchestrator) = &self.orchestrator else {
            return;
        };
        let events = orchestrator.events()[self.orchestrator_event_cursor..].to_vec();
        self.orchestrator_event_cursor = orchestrator.events().len();
        for event in events {
            match event {
                fsm::LifecycleEvent::RunCompleted { .. } => {
                    self.push_run(obs::EventKind::RunCompleted, obs::Component::Orchestrator);
                }
                fsm::LifecycleEvent::RunFaulted { .. }
                | fsm::LifecycleEvent::RunRejected { .. }
                | fsm::LifecycleEvent::RunOperatorStopped { .. } => {
                    self.push_run(obs::EventKind::RunFaulted, obs::Component::Orchestrator);
                }
                fsm::LifecycleEvent::RunTornDown { .. } => {
                    self.push_run(obs::EventKind::RunTornDown, obs::Component::Orchestrator);
                }
            }
        }
    }

    fn fsm_plan(&self) -> fsm::RunPlan {
        fsm::RunPlan::test_linear(
            fsm::RunId(self.run_id.0),
            self.plan
                .stages
                .iter()
                .map(|stage| fsm::StageRef {
                    stage_index: stage.stage_index,
                    node_id: fsm::NodeId(stage.node_id.0),
                })
                .collect(),
        )
    }

    fn to_stage_provision(&self, provision: plan::ProvisionStage) -> stage::ProvisionStage {
        stage::ProvisionStage {
            run_id: stage::RunId(provision.run_id.0),
            authorized_orchestrator: stage::NodeId(self.orchestrator_node_id.0),
            node_id: stage::NodeId(provision.node_id.0),
            stage_index: provision.stage_index,
            stage_count: provision.stage_count,
            layer_range: stage::LayerRange {
                start: provision.layer_start,
                end_exclusive: provision.layer_end_exclusive,
            },
            inbound: stage::EdgeProvision::inbound(stage::EdgeId(provision.inbound.edge_id.0)),
            outbound: stage::EdgeProvision::outbound(stage::EdgeId(provision.outbound.edge_id.0)),
            weight_source: stage::WeightSource::new(
                provision.model.model_id,
                provision.gguf_source,
                provision.tokenizer,
            ),
            shard_plan: None,
        }
    }

    fn edge_by_kind(&self, kind: plan::EdgeKind) -> &plan::EdgePlan {
        self.plan
            .edges
            .iter()
            .find(|edge| edge.kind == kind)
            .expect("mock plan must contain requested edge")
    }

    fn edge_chain(&self) -> Vec<plan::EdgeId> {
        let mut stages = self.plan.stages.iter().collect::<Vec<_>>();
        stages.sort_by_key(|stage| stage.stage_index);
        let mut chain = Vec::with_capacity(stages.len() + 1);
        if let Some(first) = stages.first() {
            chain.push(first.inbound_edge);
        }
        chain.extend(stages.into_iter().map(|stage| stage.outbound_edge));
        chain
    }

    fn allocate_object_id(&mut self, edge_id: plan::EdgeId) -> u64 {
        self.object_allocators
            .entry(edge_id)
            .or_insert_with(|| edge_actor::ObjectIdAllocator::new(edge_actor::EdgeId(edge_id.0)))
            .alloc()
            .object_id
            .0
    }

    fn finish_outcome(&self) -> LocalMockOutcome {
        LocalMockOutcome {
            engine_events: self.engine_events.clone(),
            trace: self.trace.clone(),
            injected_sequences: self
                .orchestrator
                .as_ref()
                .map(|orchestrator| orchestrator.injected_sequences())
                .unwrap_or_default(),
            stage_count: self.plan.stages.len(),
            live_edges: self.resources.live_edges.len(),
            live_rings: self.resources.live_rings.len(),
            live_stage_runs: self.resources.live_stage_runs.len(),
            transport_delivery_count: self.transport.deliveries().len(),
            transport_deliveries: self.transport.deliveries().to_vec(),
            edge_chain: self.edge_chain(),
        }
    }

    fn terminal_observed(&self) -> bool {
        self.trace.iter().any(|event| {
            matches!(
                event.kind(),
                obs::EventKind::RunCompleted | obs::EventKind::RunFaulted
            )
        })
    }

    fn count_kind(&self, kind: obs::EventKind) -> usize {
        self.trace
            .iter()
            .filter(|event| event.kind() == kind)
            .count()
    }

    fn orchestrator_mut(&mut self) -> &mut fsm::OrchestratorHarness {
        self.orchestrator
            .as_mut()
            .expect("run_prompt must initialize orchestrator")
    }

    fn push_run(&mut self, kind: obs::EventKind, component: obs::Component) {
        self.trace.push(obs::Event::RunScoped {
            kind,
            run_id: obs::RunId(self.run_id.0),
            reason: None,
            component,
        });
    }

    fn push_node(&mut self, kind: obs::EventKind, node_id: plan::NodeId) {
        self.trace.push(obs::Event::NodeScoped {
            kind,
            node_id: obs::NodeId(node_id.0),
            component: obs::Component::NodeBoot,
        });
    }

    fn push_stage(&mut self, kind: obs::EventKind, stage_index: u32, component: obs::Component) {
        self.trace.push(obs::Event::StageScoped {
            kind,
            run_id: obs::RunId(self.run_id.0),
            stage_index: obs::StageIndex(stage_index),
            reason: None,
            component,
        });
    }

    fn push_edge(&mut self, kind: obs::EventKind, edge_id: plan::EdgeId) {
        self.trace.push(obs::Event::EdgeScoped {
            kind,
            edge_id: obs::EdgeId(edge_id.0),
            component: obs::Component::EdgeEstablisher,
        });
    }

    fn push_object(
        &mut self,
        kind: obs::EventKind,
        object_id: u64,
        sequence: u64,
        component: obs::Component,
    ) {
        self.trace.push(obs::Event::ObjectScoped {
            kind,
            object_id: obs::ObjectId(object_id),
            sequence: obs::Sequence(sequence),
            component,
        });
    }

    fn push_step(&mut self, kind: obs::EventKind, step_id: u64) {
        self.trace.push(obs::Event::StepScoped {
            kind,
            step_id: obs::StepId(step_id),
            component: obs::Component::StageController,
        });
    }
}

fn tokenize(prompt: &str) -> Vec<u32> {
    let tokens: Vec<u32> = prompt
        .split_whitespace()
        .enumerate()
        .map(|(index, _)| index as u32 + 1)
        .collect();
    if tokens.is_empty() { vec![0] } else { tokens }
}
