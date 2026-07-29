use std::collections::BTreeSet;

use crate::run_plan::NodeId;

use super::error::NodeControlError;
use super::pool::{NodeCapability, NodeLease};
use super::roles::RoleAssignment;

pub trait NodeControl: Send {
    fn wait_boot_ready(&mut self) -> Result<NodeFacts, NodeControlError>;
    fn wait_cluster_converged(&mut self, expected_alive: usize) -> Result<(), NodeControlError>;
    fn assign_role(&mut self, assignment: RoleAssignment) -> Result<(), NodeControlError>;
    fn shutdown(&mut self) -> Result<(), NodeControlError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeLaunchSpec;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeFacts {
    pub node_id: NodeId,
    pub capabilities: BTreeSet<NodeCapability>,
}

impl NodeFacts {
    pub fn from_lease(lease: &NodeLease) -> Self {
        Self {
            node_id: lease.logical_node_id,
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

impl StaticNodeLauncher {
    pub fn launch_node(&self, lease: &NodeLease, _spec: NodeLaunchSpec) -> LaunchedNode {
        let facts = NodeFacts::from_lease(lease);
        LaunchedNode {
            lease: lease.clone(),
            control: Box::new(StaticNodeControl {
                facts,
                booted: false,
                stopped: false,
                assigned_roles: Vec::new(),
            }),
        }
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
                "expected_alive must be greater than zero",
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
