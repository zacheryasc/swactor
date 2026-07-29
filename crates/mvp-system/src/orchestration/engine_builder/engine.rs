use crate::run_plan::RunId;

use super::error::EngineBuildError;
use super::events::EngineEvent;
use super::launcher::{LaunchedNode, NodeControl, NodeFacts, NodeLaunchSpec, StaticNodeLauncher};
use super::model::ModelSpec;
use super::node_image::NodeImageSpec;
use super::planner::{FixedLinearPipelinePlanner, RoleAssignmentPlan, RolePlannerInput};
use super::pool::{PoolRequest, StaticPoolProvider};
use super::roles::RoleAssignment;

pub struct ClusterBuilder {
    cluster_id: String,
    run_id: RunId,
    model: ModelSpec,
    pool_provider: Option<StaticPoolProvider>,
    launcher: Option<StaticNodeLauncher>,
    planner: Option<FixedLinearPipelinePlanner>,
}

impl ClusterBuilder {
    pub fn new(cluster_id: impl Into<String>, model: ModelSpec) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            run_id: RunId(1),
            model,
            pool_provider: None,
            launcher: None,
            planner: None,
        }
    }

    pub fn run_id(mut self, run_id: impl Into<RunId>) -> Self {
        self.run_id = run_id.into();
        self
    }

    pub fn image(self, _image: NodeImageSpec) -> Self {
        self
    }

    pub fn pool_provider(mut self, provider: StaticPoolProvider) -> Self {
        self.pool_provider = Some(provider);
        self
    }

    pub fn launcher(mut self, launcher: StaticNodeLauncher) -> Self {
        self.launcher = Some(launcher);
        self
    }

    pub fn planner(mut self, planner: FixedLinearPipelinePlanner) -> Self {
        self.planner = Some(planner);
        self
    }

    pub fn launch(mut self) -> Result<ClusterHandle, EngineBuildError> {
        let pool_provider = self
            .pool_provider
            .take()
            .ok_or(EngineBuildError::MissingComponent("pool_provider"))?;
        let launcher = self
            .launcher
            .take()
            .ok_or(EngineBuildError::MissingComponent("launcher"))?;
        let planner = self
            .planner
            .take()
            .ok_or(EngineBuildError::MissingComponent("planner"))?;

        let mut events = Vec::new();
        let leases = pool_provider.acquire_pool(PoolRequest {
            min_nodes: planner.required_node_count(),
        })?;
        if leases.is_empty() {
            return Err(EngineBuildError::EmptyPool);
        }
        events.push(EngineEvent::PoolAcquired {
            node_count: leases.len(),
        });

        let mut nodes = Vec::with_capacity(leases.len());
        let mut iter = leases.into_iter();
        let coordinator_lease = iter.next().ok_or(EngineBuildError::EmptyPool)?;
        let mut coordinator = launcher.launch_node(&coordinator_lease, NodeLaunchSpec);
        events.push(EngineEvent::NodeLaunched {
            node_id: coordinator.lease.logical_node_id,
            coordinator: true,
        });
        let coordinator_facts = coordinator.control.wait_boot_ready()?;
        events.push(EngineEvent::NodeBootReady {
            node_id: coordinator_facts.node_id,
        });
        nodes.push(EngineNode::new(coordinator, coordinator_facts));

        for lease in iter {
            let mut node = launcher.launch_node(&lease, NodeLaunchSpec);
            events.push(EngineEvent::NodeLaunched {
                node_id: node.lease.logical_node_id,
                coordinator: false,
            });
            let facts = node.control.wait_boot_ready()?;
            events.push(EngineEvent::NodeBootReady {
                node_id: facts.node_id,
            });
            nodes.push(EngineNode::new(node, facts));
        }

        let expected_alive = nodes.len();
        for node in &mut nodes {
            node.control.wait_cluster_converged(expected_alive)?;
        }
        events.push(EngineEvent::ClusterConverged {
            node_count: expected_alive,
        });

        let plan = planner.plan(RolePlannerInput {
            run_id: self.run_id,
            model: self.model,
            nodes: nodes.iter().map(|node| node.facts.clone()).collect(),
        })?;
        events.push(EngineEvent::RolesPlanned {
            stage_count: plan.stages.len(),
        });

        assign_role(
            &mut nodes,
            RoleAssignment::Coordinator(plan.coordinator.clone()),
            &mut events,
        )?;
        for stage in &plan.stages {
            assign_role(
                &mut nodes,
                RoleAssignment::StageWorker(stage.clone()),
                &mut events,
            )?;
        }
        events.push(EngineEvent::EngineReady {
            cluster_id: self.cluster_id.clone(),
        });

        Ok(ClusterHandle {
            cluster_id: self.cluster_id,
            nodes,
            plan,
            events,
        })
    }
}

pub struct ClusterHandle {
    cluster_id: String,
    nodes: Vec<EngineNode>,
    plan: RoleAssignmentPlan,
    events: Vec<EngineEvent>,
}

impl ClusterHandle {
    pub fn role_plan(&self) -> &RoleAssignmentPlan {
        &self.plan
    }

    pub fn events(&self) -> &[EngineEvent] {
        &self.events
    }

    pub fn shutdown(mut self) -> Result<Vec<EngineEvent>, EngineBuildError> {
        for node in &mut self.nodes {
            node.control.shutdown()?;
            self.events.push(EngineEvent::NodeStopped {
                node_id: node.facts.node_id,
            });
        }
        self.events.push(EngineEvent::ShutdownComplete {
            cluster_id: self.cluster_id,
        });
        Ok(self.events)
    }
}
struct EngineNode {
    facts: NodeFacts,
    roles: Vec<RoleAssignment>,
    control: Box<dyn NodeControl>,
}

impl EngineNode {
    fn new(launched: LaunchedNode, facts: NodeFacts) -> Self {
        Self {
            facts,
            roles: Vec::new(),
            control: launched.control,
        }
    }
}

fn assign_role(
    nodes: &mut [EngineNode],
    assignment: RoleAssignment,
    events: &mut Vec<EngineEvent>,
) -> Result<(), EngineBuildError> {
    let node_id = assignment.node_id();
    let node = nodes
        .iter_mut()
        .find(|node| node.facts.node_id == node_id)
        .ok_or(EngineBuildError::RoleTargetMissing { node_id: node_id.0 })?;
    node.control.assign_role(assignment.clone())?;
    let role = assignment.kind();
    node.roles.push(assignment);
    events.push(EngineEvent::RoleAssigned { node_id, role });
    Ok(())
}
