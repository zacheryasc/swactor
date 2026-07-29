#![allow(dead_code)]

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

pub use crate::run_plan::NodeId;
pub use engine::{ClusterBuilder, ClusterHandle};
pub use error::{EngineBuildError, PlanningError};
pub use events::EngineEvent;
pub use launcher::StaticNodeLauncher;
pub use model::{DTypeFamily, ModelArtifact, ModelSpec};
pub use node_image::{NodeImageSpec, WorkerRuntimeSpec};
pub use planner::FixedLinearPipelinePlanner;
pub use pool::{NodeCapability, NodeLease, ResourceFacts, StaticPoolProvider};
pub use roles::RoleKind;
pub use runtime_stack::RuntimeNode;
pub use workload::WorkloadAdapter;
