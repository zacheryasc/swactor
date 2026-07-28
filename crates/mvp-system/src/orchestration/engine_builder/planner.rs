use std::collections::BTreeSet;

use crate::orchestration::run_plan::{self, NodeId, RunId};

use super::error::PlanningError;
use super::launcher::NodeFacts;
use super::model::ModelSpec;
use super::pool::NodeCapability;
use super::roles::{CoordinatorAssignment, StageAssignment};

pub trait RolePlanner: Send + Sync {
    fn required_node_count(&self) -> usize;
    fn plan(&self, input: RolePlannerInput) -> Result<RoleAssignmentPlan, PlanningError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RolePlannerInput {
    pub cluster_id: String,
    pub run_id: RunId,
    pub model: ModelSpec,
    pub nodes: Vec<NodeFacts>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoleAssignmentPlan {
    pub coordinator: CoordinatorAssignment,
    pub stages: Vec<StageAssignment>,
    pub run_plan: run_plan::RunPlan,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixedLinearPipelinePlanner {
    pub stage_count: u32,
    pub runtime: run_plan::RuntimeConfig,
    pub activation_ring: run_plan::RingSpec,
    pub token_ring: run_plan::RingSpec,
}

impl FixedLinearPipelinePlanner {
    pub fn new(stage_count: u32) -> Self {
        Self {
            stage_count,
            runtime: run_plan::RuntimeConfig::test_default(),
            activation_ring: run_plan::RingSpec::test_default_activation(),
            token_ring: run_plan::RingSpec::test_default_token(),
        }
    }

    pub fn runtime(mut self, runtime: run_plan::RuntimeConfig) -> Self {
        self.runtime = runtime;
        self
    }
}

impl RolePlanner for FixedLinearPipelinePlanner {
    fn required_node_count(&self) -> usize {
        self.stage_count as usize + 1
    }

    fn plan(&self, input: RolePlannerInput) -> Result<RoleAssignmentPlan, PlanningError> {
        reject_duplicate_nodes(&input.nodes)?;
        let coordinator = input
            .nodes
            .iter()
            .find(|node| node.capabilities.contains(&NodeCapability::Coordinator))
            .ok_or(PlanningError::NoCoordinatorCandidate)?;
        let workers = input
            .nodes
            .iter()
            .filter(|node| {
                node.node_id != coordinator.node_id
                    && node.capabilities.contains(&NodeCapability::Worker)
            })
            .collect::<Vec<_>>();
        let required = self.stage_count as usize;
        if workers.len() < required {
            return Err(PlanningError::InsufficientWorkers {
                required,
                available: workers.len(),
            });
        }

        let placements = workers
            .iter()
            .take(required)
            .enumerate()
            .map(|(stage_index, node)| run_plan::StagePlacement {
                stage_index: stage_index as u32,
                node_id: node.node_id,
            })
            .collect::<Vec<_>>();
        let candidate_pool = workers.iter().map(|node| node.node_id).collect::<Vec<_>>();
        let run_plan = run_plan::plan_run(run_plan::PlannerInput {
            run_id: input.run_id,
            orchestrator_node_id: coordinator.node_id,
            model: input.model.to_run_plan_facts(),
            runtime: self.runtime.clone(),
            candidate_pool,
            stage_count: self.stage_count,
            placement: run_plan::PlacementInput::FixedLinear(placements),
            activation_ring: self.activation_ring,
            token_ring: self.token_ring,
        })
        .map_err(|err| PlanningError::ModelRejected(err.kind()))?;

        let mut stages = Vec::with_capacity(self.stage_count as usize);
        for stage_index in 0..self.stage_count {
            let provision = run_plan::derive_stage_provision(&run_plan, stage_index)
                .map_err(PlanningError::StageProjection)?;
            stages.push(StageAssignment {
                cluster_id: input.cluster_id.clone(),
                provision,
            });
        }

        Ok(RoleAssignmentPlan {
            coordinator: CoordinatorAssignment {
                cluster_id: input.cluster_id,
                node_id: coordinator.node_id,
                model: input.model,
            },
            stages,
            run_plan,
        })
    }
}

fn reject_duplicate_nodes(nodes: &[NodeFacts]) -> Result<(), PlanningError> {
    let mut seen = BTreeSet::<NodeId>::new();
    for node in nodes {
        if !seen.insert(node.node_id) {
            return Err(PlanningError::DuplicateNodeId {
                node_id: node.node_id.0,
            });
        }
    }
    Ok(())
}
