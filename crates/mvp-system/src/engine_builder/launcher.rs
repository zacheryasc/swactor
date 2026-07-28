use std::collections::{BTreeMap, BTreeSet};

use crate::orchestration::run_plan::NodeId;

use super::error::{LaunchError, NodeControlError};
use super::node_image::NodeImageSpec;
use super::pool::{NodeCapability, NodeLease, ResourceFacts};
use super::roles::RoleAssignment;

pub trait NodeLauncher: Send + Sync {
    fn launch_node(
        &self,
        lease: &NodeLease,
        spec: NodeLaunchSpec,
    ) -> Result<LaunchedNode, LaunchError>;
}

pub trait NodeControl: Send {
    fn wait_boot_ready(&mut self) -> Result<NodeFacts, NodeControlError>;
    fn wait_cluster_converged(&mut self, expected_alive: usize) -> Result<(), NodeControlError>;
    fn assign_role(&mut self, assignment: RoleAssignment) -> Result<(), NodeControlError>;
    fn shutdown(&mut self) -> Result<(), NodeControlError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeLaunchSpec {
    pub cluster_id: String,
    pub image: NodeImageSpec,
    pub coordinator: Option<CoordinatorJoinSpec>,
    pub is_coordinator: bool,
    pub env: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatorJoinSpec {
    pub endpoint: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeFacts {
    pub node_id: NodeId,
    pub coordinator_endpoint: Option<String>,
    pub resources: ResourceFacts,
    pub capabilities: BTreeSet<NodeCapability>,
}

impl NodeFacts {
    pub fn from_lease(lease: &NodeLease, coordinator_endpoint: Option<String>) -> Self {
        Self {
            node_id: lease.logical_node_id,
            coordinator_endpoint,
            resources: lease.expected_resources.clone(),
            capabilities: lease.capabilities.clone(),
        }
    }
}

pub struct LaunchedNode {
    pub lease: NodeLease,
    pub control: Box<dyn NodeControl>,
}

#[derive(Clone, Debug, Default)]
pub struct StaticNodeLauncher;

impl NodeLauncher for StaticNodeLauncher {
    fn launch_node(
        &self,
        lease: &NodeLease,
        spec: NodeLaunchSpec,
    ) -> Result<LaunchedNode, LaunchError> {
        let endpoint = format!(
            "static://{}/node/{}",
            spec.cluster_id, lease.logical_node_id.0
        );
        let facts = NodeFacts::from_lease(lease, Some(endpoint));
        Ok(LaunchedNode {
            lease: lease.clone(),
            control: Box::new(StaticNodeControl {
                facts,
                booted: false,
                stopped: false,
                assigned_roles: Vec::new(),
            }),
        })
    }
}

struct StaticNodeControl {
    facts: NodeFacts,
    booted: bool,
    stopped: bool,
    assigned_roles: Vec<RoleAssignment>,
}

impl StaticNodeControl {
    fn node_id(&self) -> u64 {
        self.facts.node_id.0
    }
}

impl NodeControl for StaticNodeControl {
    fn wait_boot_ready(&mut self) -> Result<NodeFacts, NodeControlError> {
        if self.stopped {
            return Err(NodeControlError::Stopped {
                node_id: self.node_id(),
            });
        }
        self.booted = true;
        Ok(self.facts.clone())
    }

    fn wait_cluster_converged(&mut self, expected_alive: usize) -> Result<(), NodeControlError> {
        if self.stopped {
            return Err(NodeControlError::Stopped {
                node_id: self.node_id(),
            });
        }
        if !self.booted {
            return Err(NodeControlError::NotBooted {
                node_id: self.node_id(),
            });
        }
        if expected_alive == 0 {
            return Err(NodeControlError::Backend(
                "expected_alive must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }

    fn assign_role(&mut self, assignment: RoleAssignment) -> Result<(), NodeControlError> {
        if self.stopped {
            return Err(NodeControlError::Stopped {
                node_id: self.node_id(),
            });
        }
        if !self.booted {
            return Err(NodeControlError::NotBooted {
                node_id: self.node_id(),
            });
        }
        let role_node_id = assignment.node_id().0;
        if role_node_id != self.node_id() {
            return Err(NodeControlError::RoleNodeMismatch {
                node_id: self.node_id(),
                role_node_id,
            });
        }
        self.assigned_roles.push(assignment);
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), NodeControlError> {
        if self.stopped {
            return Ok(());
        }
        self.stopped = true;
        Ok(())
    }
}
