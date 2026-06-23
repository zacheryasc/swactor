#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolId(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerStartPolicy {
    StartBeforeAvailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootConfig {
    pub node_id: NodeId,
    pub intended_pool_id: PoolId,
    pub worker_policy: WorkerStartPolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResourceFact {
    RustProcessAlive,
    RuntimeAcceptingControl,
    StableNodeIdKnown,
    ArenaMapped,
    GpuWorkerReady,
    TransportEndpointBound,
    SwimJoiningPool,
    ProvisioningReceiverOpen,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResourceOutcome {
    RustProcessAlive,
    RuntimeAcceptingControl,
    StableNodeIdKnown(NodeId),
    ArenaMapped,
    GpuWorkerReady,
    TransportEndpointBound,
    SwimJoiningPool,
    ProvisioningReceiverOpen,
    ArenaFault,
    GpuWorkerFault,
    TransportEndpointFault,
    InvalidNodeIdentity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootFaultKind {
    ArenaConstructionFailed,
    WorkerStartupFailed,
    TransportEndpointFailed,
    InvalidNodeIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecycleEvent {
    NodeAvailable {
        node_id: NodeId,
    },
    NodeFaulted {
        node_id: NodeId,
        kind: BootFaultKind,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootCommand {
    AdvertiseLifecycle { node_id: NodeId },
    JoinMembership { pool_id: PoolId },
    OpenProvisioningInbox { node_id: NodeId },
    LoadWeights { node_id: NodeId },
    ConfigureRole { node_id: NodeId },
    EstablishEdge { node_id: NodeId },
    AssignStage { node_id: NodeId },
    AssignLayerRange { node_id: NodeId },
    AssignEdge { node_id: NodeId },
    AssignObjectSpec { node_id: NodeId },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionRequest {
    pub node_id: NodeId,
}

impl ProvisionRequest {
    pub fn for_node(node_id: NodeId) -> Self {
        Self { node_id }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProvisioningAdmissionRejection {
    NodeNotAvailable,
    WrongNode,
}

#[cfg(test)]
pub struct BootHarness {
    config: BootConfig,
    facts: std::collections::BTreeSet<ResourceFact>,
    events: Vec<LifecycleEvent>,
    commands: Vec<BootCommand>,
    available: bool,
    faulted: bool,
}

#[cfg(test)]
impl BootHarness {
    pub fn launch(config: BootConfig) -> Self {
        let commands = vec![
            BootCommand::AdvertiseLifecycle {
                node_id: config.node_id,
            },
            BootCommand::JoinMembership {
                pool_id: config.intended_pool_id.clone(),
            },
            BootCommand::OpenProvisioningInbox {
                node_id: config.node_id,
            },
        ];
        Self {
            config,
            facts: std::collections::BTreeSet::new(),
            events: Vec::new(),
            commands,
            available: false,
            faulted: false,
        }
    }

    pub fn observe(&mut self, outcome: ResourceOutcome) {
        if self.available || self.faulted {
            return;
        }
        match outcome {
            ResourceOutcome::RustProcessAlive => self.insert_fact(ResourceFact::RustProcessAlive),
            ResourceOutcome::RuntimeAcceptingControl => {
                self.insert_fact(ResourceFact::RuntimeAcceptingControl)
            }
            ResourceOutcome::StableNodeIdKnown(node_id) if node_id == self.config.node_id => {
                self.insert_fact(ResourceFact::StableNodeIdKnown)
            }
            ResourceOutcome::StableNodeIdKnown(_) | ResourceOutcome::InvalidNodeIdentity => {
                self.fault(BootFaultKind::InvalidNodeIdentity)
            }
            ResourceOutcome::ArenaMapped => self.insert_fact(ResourceFact::ArenaMapped),
            ResourceOutcome::GpuWorkerReady => self.insert_fact(ResourceFact::GpuWorkerReady),
            ResourceOutcome::TransportEndpointBound => {
                self.insert_fact(ResourceFact::TransportEndpointBound)
            }
            ResourceOutcome::SwimJoiningPool => self.insert_fact(ResourceFact::SwimJoiningPool),
            ResourceOutcome::ProvisioningReceiverOpen => {
                self.insert_fact(ResourceFact::ProvisioningReceiverOpen)
            }
            ResourceOutcome::ArenaFault => self.fault(BootFaultKind::ArenaConstructionFailed),
            ResourceOutcome::GpuWorkerFault => self.fault(BootFaultKind::WorkerStartupFailed),
            ResourceOutcome::TransportEndpointFault => {
                self.fault(BootFaultKind::TransportEndpointFailed)
            }
        }
        self.maybe_available();
    }

    pub fn events(&self) -> &[LifecycleEvent] {
        &self.events
    }

    pub fn commands(&self) -> &[BootCommand] {
        &self.commands
    }

    pub fn is_candidate_eligible(&self, node_id: NodeId) -> bool {
        node_id == self.config.node_id && self.available && !self.faulted
    }

    pub fn try_accept_provisioning(
        &self,
        request: ProvisionRequest,
    ) -> Result<(), ProvisioningAdmissionRejection> {
        if request.node_id != self.config.node_id {
            return Err(ProvisioningAdmissionRejection::WrongNode);
        }
        if self.available && !self.faulted {
            Ok(())
        } else {
            Err(ProvisioningAdmissionRejection::NodeNotAvailable)
        }
    }

    fn insert_fact(&mut self, fact: ResourceFact) {
        self.facts.insert(fact);
    }

    fn maybe_available(&mut self) {
        if self.available || self.faulted {
            return;
        }
        let required = [
            ResourceFact::RustProcessAlive,
            ResourceFact::RuntimeAcceptingControl,
            ResourceFact::StableNodeIdKnown,
            ResourceFact::ArenaMapped,
            ResourceFact::GpuWorkerReady,
            ResourceFact::TransportEndpointBound,
            ResourceFact::SwimJoiningPool,
            ResourceFact::ProvisioningReceiverOpen,
        ];
        if required.iter().all(|fact| self.facts.contains(fact)) {
            self.available = true;
            self.events.push(LifecycleEvent::NodeAvailable {
                node_id: self.config.node_id,
            });
        }
    }

    fn fault(&mut self, kind: BootFaultKind) {
        if self.faulted || self.available {
            return;
        }
        self.faulted = true;
        self.events.push(LifecycleEvent::NodeFaulted {
            node_id: self.config.node_id,
            kind,
        });
    }
}
