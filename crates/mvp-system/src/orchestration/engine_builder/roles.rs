use crate::run_plan::{self, NodeId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatorAssignment {
    pub node_id: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageAssignment {
    pub provision: run_plan::ProvisionStage,
}

impl StageAssignment {
    pub fn node_id(&self) -> NodeId {
        self.provision.node_id
    }

    pub fn stage_index(&self) -> u32 {
        self.provision.stage_index
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoleAssignment {
    Coordinator(CoordinatorAssignment),
    StageWorker(StageAssignment),
}

impl RoleAssignment {
    pub fn node_id(&self) -> NodeId {
        match self {
            Self::Coordinator(assignment) => assignment.node_id,
            Self::StageWorker(assignment) => assignment.node_id(),
        }
    }

    pub fn kind(&self) -> RoleKind {
        match self {
            Self::Coordinator(_) => RoleKind::Coordinator,
            Self::StageWorker(assignment) => RoleKind::StageWorker {
                stage_index: assignment.stage_index(),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoleKind {
    Coordinator,
    StageWorker { stage_index: u32 },
}
