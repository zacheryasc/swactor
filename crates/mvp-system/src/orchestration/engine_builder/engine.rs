use crate::run_plan::RunId;

use super::error::EngineBuildError;
use super::planner::{
    FixedLinearPipelinePlanner, RoleAssignment, RoleAssignmentPlan, RoleKind, RolePlannerInput,
};
use super::pool::{ModelSpec, NodeFacts, StaticPoolProvider};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EngineEvent {
    PoolAcquired,
    ClusterConverged,
    RoleAssigned(RoleKind),
    EngineReady,
}

fn launch_node(facts: &NodeFacts) -> StaticNodeControl {
    StaticNodeControl {
        facts: facts.clone(),
        booted: false,
        stopped: false,
        assigned_roles: Vec::new(),
    }
}

struct StaticNodeControl {
    facts: NodeFacts,
    booted: bool,
    stopped: bool,
    assigned_roles: Vec<RoleAssignment>,
}

impl StaticNodeControl {
    fn node_id(&self) -> u64 {
        self.facts.node_id.0
    }
}

impl StaticNodeControl {
    fn wait_boot_ready(&mut self) -> Result<NodeFacts, EngineBuildError> {
        if self.stopped {
            return Err(EngineBuildError::Stopped {
                node_id: self.node_id(),
            });
        }
        self.booted = true;
        Ok(self.facts.clone())
    }

    fn wait_cluster_converged(&mut self, expected_alive: usize) -> Result<(), EngineBuildError> {
        if self.stopped {
            return Err(EngineBuildError::Stopped {
                node_id: self.node_id(),
            });
        }
        if !self.booted {
            return Err(EngineBuildError::NotBooted {
                node_id: self.node_id(),
            });
        }
        if expected_alive == 0 {
            return Err(EngineBuildError::Backend(
                "expected_alive must be greater than zero",
            ));
        }
        Ok(())
    }

    fn assign_role(&mut self, assignment: RoleAssignment) -> Result<(), EngineBuildError> {
        if self.stopped {
            return Err(EngineBuildError::Stopped {
                node_id: self.node_id(),
            });
        }
        if !self.booted {
            return Err(EngineBuildError::NotBooted {
                node_id: self.node_id(),
            });
        }
        let role_node_id = assignment.node_id().0;
        if role_node_id != self.node_id() {
            return Err(EngineBuildError::RoleNodeMismatch {
                node_id: self.node_id(),
                role_node_id,
            });
        }
        self.assigned_roles.push(assignment);
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), EngineBuildError> {
        if self.stopped {
            return Ok(());
        }
        self.stopped = true;
        Ok(())
    }
}

pub(crate) struct ClusterBuilder {
    cluster_id: String,
    run_id: RunId,
    model: ModelSpec,
    pool_provider: Option<StaticPoolProvider>,
    planner: Option<FixedLinearPipelinePlanner>,
}

impl ClusterBuilder {
    pub(crate) fn new(cluster_id: impl Into<String>, model: ModelSpec) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            run_id: RunId(1),
            model,
            pool_provider: None,
            planner: None,
        }
    }

    pub(crate) fn run_id(mut self, run_id: impl Into<RunId>) -> Self {
        self.run_id = run_id.into();
        self
    }

    pub(crate) fn pool_provider(mut self, provider: StaticPoolProvider) -> Self {
        self.pool_provider = Some(provider);
        self
    }

    pub(crate) fn planner(mut self, planner: FixedLinearPipelinePlanner) -> Self {
        self.planner = Some(planner);
        self
    }

    pub(crate) fn launch(mut self) -> Result<ClusterHandle, EngineBuildError> {
        let pool_provider = self
            .pool_provider
            .take()
            .ok_or(EngineBuildError::MissingComponent("pool_provider"))?;
        let planner = self
            .planner
            .take()
            .ok_or(EngineBuildError::MissingComponent("planner"))?;

        let mut events = Vec::new();
        let leases = pool_provider.acquire_pool(planner.required_node_count())?;
        if leases.is_empty() {
            return Err(EngineBuildError::EmptyPool);
        }
        events.push(EngineEvent::PoolAcquired);

        let mut nodes = Vec::with_capacity(leases.len());
        let mut iter = leases.into_iter();
        let coordinator_lease = iter.next().ok_or(EngineBuildError::EmptyPool)?;
        let mut coordinator = launch_node(&coordinator_lease);
        let coordinator_facts = coordinator.wait_boot_ready()?;
        nodes.push(EngineNode::new(coordinator, coordinator_facts));

        for lease in iter {
            let mut node = launch_node(&lease);
            let facts = node.wait_boot_ready()?;
            nodes.push(EngineNode::new(node, facts));
        }

        let expected_alive = nodes.len();
        for node in &mut nodes {
            node.control.wait_cluster_converged(expected_alive)?;
        }
        events.push(EngineEvent::ClusterConverged);

        let plan = planner.plan(RolePlannerInput {
            run_id: self.run_id,
            model: self.model,
            nodes: nodes.iter().map(|node| node.facts.clone()).collect(),
        })?;

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
        events.push(EngineEvent::EngineReady);

        Ok(ClusterHandle {
            nodes,
            plan,
            events,
        })
    }
}

pub(crate) struct ClusterHandle {
    nodes: Vec<EngineNode>,
    plan: RoleAssignmentPlan,
    events: Vec<EngineEvent>,
}

impl ClusterHandle {
    pub(crate) fn role_plan(&self) -> &RoleAssignmentPlan {
        &self.plan
    }

    pub(crate) fn events(&self) -> &[EngineEvent] {
        &self.events
    }

    pub(crate) fn shutdown(mut self) -> Result<(), EngineBuildError> {
        for node in &mut self.nodes {
            node.control.shutdown()?;
        }
        Ok(())
    }
}
struct EngineNode {
    facts: NodeFacts,
    roles: Vec<RoleAssignment>,
    control: StaticNodeControl,
}

impl EngineNode {
    fn new(control: StaticNodeControl, facts: NodeFacts) -> Self {
        Self {
            facts,
            roles: Vec::new(),
            control,
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
    events.push(EngineEvent::RoleAssigned(role));
    Ok(())
}
