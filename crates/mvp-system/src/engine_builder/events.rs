use crate::run_plan::NodeId;

use super::roles::RoleKind;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EngineEvent {
    PoolAcquired { node_count: usize },
    NodeLaunched { node_id: NodeId, seed: bool },
    NodeBootReady { node_id: NodeId },
    ClusterConverged { node_count: usize },
    RolesPlanned { stage_count: usize },
    RoleAssigned { node_id: NodeId, role: RoleKind },
    EngineReady { cluster_id: String },
    NodeStopped { node_id: NodeId },
    ShutdownComplete { cluster_id: String },
}
