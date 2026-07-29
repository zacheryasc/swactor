use crate::run_plan;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EngineBuildError {
    MissingComponent(&'static str),
    EmptyPool,
    RoleTargetMissing { node_id: u64 },
    InsufficientNodes { requested: usize, available: usize },
    NotBooted { node_id: u64 },
    Stopped { node_id: u64 },
    RoleNodeMismatch { node_id: u64, role_node_id: u64 },
    Backend(&'static str),
    DuplicateNodeId { node_id: u64 },
    NoCoordinatorCandidate,
    InsufficientWorkers { required: usize, available: usize },
    ModelRejected(run_plan::PlanRejectionKind),
    StageProjection(run_plan::ProjectionRejection),
}
