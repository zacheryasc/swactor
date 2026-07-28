use std::collections::BTreeSet;

use crate::orchestration::run_plan::NodeId;

use super::error::PoolError;
use super::node_image::NodeImageSpec;

pub trait PoolProvider: Send + Sync {
    fn acquire_pool(&self, request: PoolRequest) -> Result<Vec<NodeLease>, PoolError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolRequest {
    pub cluster_id: String,
    pub min_nodes: usize,
    pub image: NodeImageSpec,
    pub required_resources: ResourceRequest,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ResourceRequest {
    pub min_gpu_count: u32,
    pub min_gpu_memory_bytes: u64,
    pub require_cuda: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeLease {
    pub lease_id: String,
    pub logical_node_id: NodeId,
    pub launch_target: LaunchTarget,
    pub expected_resources: ResourceFacts,
    pub capabilities: BTreeSet<NodeCapability>,
}

impl NodeLease {
    pub fn new(
        lease_id: impl Into<String>,
        logical_node_id: NodeId,
        capabilities: impl IntoIterator<Item = NodeCapability>,
    ) -> Self {
        Self {
            lease_id: lease_id.into(),
            logical_node_id,
            launch_target: LaunchTarget::InProcess,
            expected_resources: ResourceFacts::default(),
            capabilities: capabilities.into_iter().collect(),
        }
    }

    pub fn launch_target(mut self, launch_target: LaunchTarget) -> Self {
        self.launch_target = launch_target;
        self
    }

    pub fn resources(mut self, expected_resources: ResourceFacts) -> Self {
        self.expected_resources = expected_resources;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LaunchTarget {
    InProcess,
    LocalProcess { program: String, args: Vec<String> },
    DockerContainer { name: String },
    RemoteHost { label: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeCapability {
    Coordinator,
    Worker,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceFacts {
    pub gpu_count: u32,
    pub gpu_memory_bytes: u64,
    pub cpu_cores: u32,
    pub ram_bytes: u64,
    pub cuda_available: bool,
}

impl ResourceFacts {
    pub fn cpu_only(cpu_cores: u32, ram_bytes: u64) -> Self {
        Self {
            gpu_count: 0,
            gpu_memory_bytes: 0,
            cpu_cores,
            ram_bytes,
            cuda_available: false,
        }
    }

    pub fn cuda(gpu_count: u32, gpu_memory_bytes: u64, cpu_cores: u32, ram_bytes: u64) -> Self {
        Self {
            gpu_count,
            gpu_memory_bytes,
            cpu_cores,
            ram_bytes,
            cuda_available: true,
        }
    }

    fn satisfies(&self, request: &ResourceRequest) -> bool {
        self.gpu_count >= request.min_gpu_count
            && self.gpu_memory_bytes >= request.min_gpu_memory_bytes
            && (!request.require_cuda || self.cuda_available)
    }
}

impl Default for ResourceFacts {
    fn default() -> Self {
        Self::cpu_only(1, 512 * 1024 * 1024)
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

    pub fn leases(&self) -> &[NodeLease] {
        &self.leases
    }
}

impl PoolProvider for StaticPoolProvider {
    fn acquire_pool(&self, request: PoolRequest) -> Result<Vec<NodeLease>, PoolError> {
        let matching = self
            .leases
            .iter()
            .filter(|lease| {
                lease
                    .expected_resources
                    .satisfies(&request.required_resources)
            })
            .cloned()
            .collect::<Vec<_>>();
        if matching.len() < request.min_nodes {
            return Err(PoolError::InsufficientNodes {
                requested: request.min_nodes,
                available: matching.len(),
            });
        }
        Ok(matching)
    }
}
