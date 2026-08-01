use std::collections::BTreeSet;

use crate::run_plan::{self, NodeId, RunId};

use super::error::EngineBuildError;
use super::pool::{ModelSpec, NodeCapability, NodeFacts};
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CoordinatorAssignment {
    pub node_id: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StageAssignment {
    pub provision: run_plan::ProvisionStage,
}

impl StageAssignment {
    pub(crate) fn node_id(&self) -> NodeId {
        self.provision.node_id
    }

    pub(crate) fn stage_index(&self) -> u32 {
        self.provision.stage_index
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RoleAssignment {
    Coordinator(CoordinatorAssignment),
    StageWorker(StageAssignment),
}

impl RoleAssignment {
    pub(crate) fn node_id(&self) -> NodeId {
        match self {
            Self::Coordinator(assignment) => assignment.node_id,
            Self::StageWorker(assignment) => assignment.node_id(),
        }
    }

    pub(crate) fn kind(&self) -> RoleKind {
        match self {
            Self::Coordinator(_) => RoleKind::Coordinator,
            Self::StageWorker(assignment) => RoleKind::StageWorker {
                stage_index: assignment.stage_index(),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RoleKind {
    Coordinator,
    StageWorker { stage_index: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RolePlannerInput {
    pub run_id: RunId,
    pub model: ModelSpec,
    pub nodes: Vec<NodeFacts>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RoleAssignmentPlan {
    pub coordinator: CoordinatorAssignment,
    pub stages: Vec<StageAssignment>,
    pub run_plan: run_plan::RunPlan,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FixedLinearPipelinePlanner {
    pub stage_count: u32,
    pub runtime: run_plan::RuntimeConfig,
    pub activation_ring: run_plan::RingSpec,
    pub token_ring: run_plan::RingSpec,
}

impl FixedLinearPipelinePlanner {
    pub(crate) fn new(stage_count: u32) -> Self {
        Self {
            stage_count,
            runtime: run_plan::RuntimeConfig {
                max_tokens: 4,
                sampling: run_plan::SamplingPolicy {
                    temperature_millis: 0,
                    top_k: 1,
                },
            },
            activation_ring: run_plan::RingSpec {
                data_capacity: 1 << 20,
                alignment: 64,
                direction: run_plan::RingDirection::Egress,
                host_pinning: run_plan::HostPinning::Pageable,
                wake_coalescing: run_plan::WakeCoalescing::PendingBit,
            },
            token_ring: run_plan::RingSpec {
                data_capacity: 4096,
                alignment: 8,
                direction: run_plan::RingDirection::Egress,
                host_pinning: run_plan::HostPinning::Pageable,
                wake_coalescing: run_plan::WakeCoalescing::PendingBit,
            },
        }
    }

    pub(crate) fn runtime(mut self, runtime: run_plan::RuntimeConfig) -> Self {
        self.runtime = runtime;
        self
    }
}

impl FixedLinearPipelinePlanner {
    pub(crate) fn required_node_count(&self) -> usize {
        self.stage_count as usize + 1
    }

    pub(crate) fn plan(
        &self,
        input: RolePlannerInput,
    ) -> Result<RoleAssignmentPlan, EngineBuildError> {
        reject_duplicate_nodes(&input.nodes)?;
        let coordinator = input
            .nodes
            .iter()
            .find(|node| node.capabilities.contains(&NodeCapability::Coordinator))
            .ok_or(EngineBuildError::NoCoordinatorCandidate)?;
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
            return Err(EngineBuildError::InsufficientWorkers {
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
        .map_err(|err| EngineBuildError::ModelRejected(err.kind()))?;

        let mut stages = Vec::with_capacity(self.stage_count as usize);
        for stage_index in 0..self.stage_count {
            let provision = run_plan::derive_stage_provision(&run_plan, stage_index)
                .map_err(EngineBuildError::StageProjection)?;
            stages.push(StageAssignment { provision });
        }

        Ok(RoleAssignmentPlan {
            coordinator: CoordinatorAssignment {
                node_id: coordinator.node_id,
            },
            stages,
            run_plan,
        })
    }
}

fn reject_duplicate_nodes(nodes: &[NodeFacts]) -> Result<(), EngineBuildError> {
    let mut seen = BTreeSet::<NodeId>::new();
    for node in nodes {
        if !seen.insert(node.node_id) {
            return Err(EngineBuildError::DuplicateNodeId {
                node_id: node.node_id.0,
            });
        }
    }
    Ok(())
}
