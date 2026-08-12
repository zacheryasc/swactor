//! Pool-based engine/node builder primitives.
//!
//! This module owns topology construction: acquire a role-neutral node pool,
//! launch the same node image everywhere, wait for node/cluster readiness, map a
//! model onto discovered nodes, assign roles, and return a live cluster handle.
//! Workload semantics stay outside this module; see [`WorkloadAdapter`].

pub(crate) mod engine;
pub(crate) mod error;
pub(crate) mod planner;
pub(crate) mod pool;

pub(crate) use crate::run_plan::{DTypeFamily, NodeId};
pub(crate) use engine::{ClusterBuilder, EngineEvent};
pub(crate) use planner::{FixedLinearPipelinePlanner, RoleKind};
pub(crate) use pool::{ModelSpec, NodeCapability, NodeFacts, StaticPoolProvider};
