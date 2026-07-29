use std::collections::BTreeSet;

use crate::run_plan::NodeId;

use super::error::PoolError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolRequest {
    pub min_nodes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeLease {
    pub logical_node_id: NodeId,
    pub capabilities: BTreeSet<NodeCapability>,
}

impl NodeLease {
    pub fn new(
        _lease_id: impl Into<String>,
        logical_node_id: NodeId,
        capabilities: impl IntoIterator<Item = NodeCapability>,
    ) -> Self {
        Self {
            logical_node_id,
            capabilities: capabilities.into_iter().collect(),
        }
    }

    pub fn resources(self, _expected_resources: ResourceFacts) -> Self {
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeCapability {
    Coordinator,
    Worker,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResourceFacts;

impl ResourceFacts {
    pub fn cpu_only(_cpu_cores: u32, _ram_bytes: u64) -> Self {
        Self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StaticPoolProvider {
    leases: Vec<NodeLease>,
}

impl StaticPoolProvider {
    pub fn new(leases: Vec<NodeLease>) -> Self {
        Self { leases }
    }
}

impl StaticPoolProvider {
    pub fn acquire_pool(&self, request: PoolRequest) -> Result<Vec<NodeLease>, PoolError> {
        if self.leases.len() < request.min_nodes {
            return Err(PoolError::InsufficientNodes {
                requested: request.min_nodes,
                available: self.leases.len(),
            });
        }
        Ok(self.leases.clone())
    }
}
