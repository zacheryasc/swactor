//! Pool-based engine/node builder primitives.
//!
//! This module owns topology construction: acquire a role-neutral node pool,
//! launch the same node image everywhere, wait for node/cluster readiness, map a
//! model onto discovered nodes, assign roles, and return a live cluster handle.
//! Workload semantics stay outside this module; see [`WorkloadAdapter`].

pub mod engine;
pub mod error;
pub mod events;
pub mod launcher;
pub mod model;
pub mod node_image;
pub mod planner;
pub mod pool;
pub mod roles;
pub mod runtime_stack;
pub mod workload;

pub use crate::orchestration::run_plan::{NodeId, RunId};
pub use engine::{ClusterBuilder, ClusterHandle, NodeSummary};
pub use error::{EngineBuildError, LaunchError, NodeControlError, PlanningError, PoolError};
pub use events::EngineEvent;
pub use launcher::{
    CoordinatorJoinSpec, LaunchedNode, NodeControl, NodeFacts, NodeLaunchSpec, NodeLauncher,
    StaticNodeLauncher,
};
pub use model::{DTypeFamily, ModelArchitecture, ModelArtifact, ModelSpec};
pub use node_image::{NodeImageSpec, WorkerRuntimeSpec};
pub use planner::{FixedLinearPipelinePlanner, RoleAssignmentPlan, RolePlanner, RolePlannerInput};
pub use pool::{
    LaunchTarget, NodeCapability, NodeLease, PoolProvider, PoolRequest, ResourceFacts,
    ResourceRequest, StaticPoolProvider,
};
pub use roles::{CoordinatorAssignment, RoleAssignment, RoleKind, StageAssignment};
pub use runtime_stack::{RuntimeNode, RuntimeNodeConfig, RuntimeNodeError};
pub use workload::WorkloadAdapter;
