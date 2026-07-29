use std::fmt;

use crate::run_plan;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EngineBuildError {
    MissingComponent(&'static str),
    EmptyPool,
    RoleTargetMissing { node_id: u64 },
    Pool(PoolError),
    Node(NodeControlError),
    Planning(PlanningError),
}

impl fmt::Display for EngineBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingComponent(name) => write!(f, "missing engine builder component: {name}"),
            Self::EmptyPool => write!(f, "pool provider returned no nodes"),
            Self::RoleTargetMissing { node_id } => {
                write!(f, "role assignment targeted unknown node {node_id}")
            }
            Self::Pool(err) => err.fmt(f),
            Self::Node(err) => err.fmt(f),
            Self::Planning(err) => err.fmt(f),
        }
    }
}

impl From<PoolError> for EngineBuildError {
    fn from(value: PoolError) -> Self {
        Self::Pool(value)
    }
}

impl From<NodeControlError> for EngineBuildError {
    fn from(value: NodeControlError) -> Self {
        Self::Node(value)
    }
}

impl From<PlanningError> for EngineBuildError {
    fn from(value: PlanningError) -> Self {
        Self::Planning(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PoolError {
    InsufficientNodes { requested: usize, available: usize },
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientNodes {
                requested,
                available,
            } => write!(
                f,
                "pool has {available} matching nodes, but {requested} were requested"
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeControlError {
    NotBooted { node_id: u64 },
    Stopped { node_id: u64 },
    RoleNodeMismatch { node_id: u64, role_node_id: u64 },
    Backend(&'static str),
}

impl fmt::Display for NodeControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotBooted { node_id } => write!(f, "node {node_id} is not boot-ready"),
            Self::Stopped { node_id } => write!(f, "node {node_id} is already stopped"),
            Self::RoleNodeMismatch {
                node_id,
                role_node_id,
            } => write!(
                f,
                "node {node_id} cannot accept role targeted at node {role_node_id}"
            ),
            Self::Backend(message) => write!(f, "node control failed: {message}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanningError {
    DuplicateNodeId { node_id: u64 },
    NoCoordinatorCandidate,
    InsufficientWorkers { required: usize, available: usize },
    ModelRejected(run_plan::PlanRejectionKind),
    StageProjection(run_plan::ProjectionRejection),
}

impl fmt::Display for PlanningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateNodeId { node_id } => {
                write!(f, "planner input contained duplicate node id {node_id}")
            }
            Self::NoCoordinatorCandidate => write!(f, "no coordinator-capable node available"),
            Self::InsufficientWorkers {
                required,
                available,
            } => write!(
                f,
                "planner needs {required} worker nodes, but only {available} are available"
            ),
            Self::ModelRejected(kind) => write!(f, "model/run planner rejected input: {kind:?}"),
            Self::StageProjection(kind) => {
                write!(f, "stage provision projection failed: {kind:?}")
            }
        }
    }
}
