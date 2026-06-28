#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolId(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadinessConfig {
    pub pool_id: PoolId,
    pub convergence_window_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunRequest {
    pub run_id: RunId,
}

impl RunRequest {
    pub fn new(run_id: RunId) -> Self {
        Self { run_id }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Observation {
    NodeKnown {
        node_id: NodeId,
    },
    SwimLive {
        node_id: NodeId,
    },
    NodeAvailable {
        node_id: NodeId,
    },
    DataPlaneIdentityReady {
        node_id: NodeId,
    },
    SwimSuspect {
        node_id: NodeId,
    },
    NodeFaulted {
        node_id: NodeId,
    },
    DataPlaneIdentityMissing {
        node_id: NodeId,
    },
    SwimLost {
        node_id: NodeId,
    },
    RunProvisioned {
        run_id: RunId,
        required_nodes: Vec<NodeId>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunFaultReason {
    RequiredNodeLost { node_id: NodeId },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadinessEvent {
    PoolReady {
        pool: Vec<NodeId>,
    },
    RunFaulted {
        run_id: RunId,
        reason: RunFaultReason,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadinessCommand {
    EmitPoolReady { pool: Vec<NodeId> },
    StartPlanning { run_id: RunId, pool: Vec<NodeId> },
    WaitForStability { pool_id: PoolId },
    AbortPendingRun { run_id: RunId },
    CommitRunPlan { run_id: RunId },
    RecomputePlacement { run_id: RunId },
    AssignStage { node_id: NodeId },
    AssignEdge { node_id: NodeId },
    AssignLayerRange { node_id: NodeId },
    AssignObjectSpec { node_id: NodeId },
}

#[cfg(test)]
#[derive(Clone, Default)]
struct NodeFacts {
    known: bool,
    live: bool,
    available: bool,
    identity_ready: bool,
    poisoned: bool,
}

#[cfg(test)]
pub struct ReadinessGateHarness {
    config: ReadinessConfig,
    candidates: Vec<NodeId>,
    facts: std::collections::BTreeMap<NodeId, NodeFacts>,
    events: Vec<ReadinessEvent>,
    commands: Vec<ReadinessCommand>,
    now_ms: u64,
    stable_since_ms: Option<u64>,
    emitted_ready: bool,
    pending_run: Option<RunRequest>,
    provisioned_run: Option<(RunId, Vec<NodeId>)>,
}

#[cfg(test)]
impl ReadinessGateHarness {
    pub fn new(config: ReadinessConfig, candidates: Vec<NodeId>) -> Self {
        let facts = candidates
            .iter()
            .copied()
            .map(|node_id| (node_id, NodeFacts::default()))
            .collect();
        Self {
            config,
            candidates,
            facts,
            events: Vec::new(),
            commands: Vec::new(),
            now_ms: 0,
            stable_since_ms: None,
            emitted_ready: false,
            pending_run: None,
            provisioned_run: None,
        }
    }

    pub fn observe(&mut self, observation: Observation) {
        match observation {
            Observation::NodeKnown { node_id } => self.fact(node_id).known = true,
            Observation::SwimLive { node_id } => self.fact(node_id).live = true,
            Observation::NodeAvailable { node_id } => self.fact(node_id).available = true,
            Observation::DataPlaneIdentityReady { node_id } => {
                self.fact(node_id).identity_ready = true
            }
            Observation::SwimSuspect { node_id }
            | Observation::NodeFaulted { node_id }
            | Observation::DataPlaneIdentityMissing { node_id } => {
                self.fact(node_id).poisoned = true
            }
            Observation::SwimLost { node_id } => {
                self.fact(node_id).live = false;
                self.fact(node_id).poisoned = true;
                if let Some((run_id, required)) = &self.provisioned_run {
                    if required.contains(&node_id)
                        && !self.events.iter().any(|event| {
                            matches!(event, ReadinessEvent::RunFaulted { run_id: seen, .. } if seen == run_id)
                        })
                    {
                        self.events.push(ReadinessEvent::RunFaulted {
                            run_id: *run_id,
                            reason: RunFaultReason::RequiredNodeLost { node_id },
                        });
                    }
                } else if let Some(pending) = &self.pending_run {
                    self.commands.push(ReadinessCommand::AbortPendingRun {
                        run_id: pending.run_id,
                    });
                }
            }
            Observation::RunProvisioned {
                run_id,
                required_nodes,
            } => {
                self.provisioned_run = Some((run_id, required_nodes));
            }
        }
        self.reset_stability_if_needed();
        self.maybe_ready();
    }

    pub fn advance_time_ms(&mut self, delta: u64) {
        self.now_ms = self.now_ms.saturating_add(delta);
        self.maybe_ready();
    }

    pub fn request_run_planning(&mut self, request: RunRequest) {
        self.pending_run = Some(request);
        self.maybe_ready();
    }

    pub fn events(&self) -> &[ReadinessEvent] {
        &self.events
    }

    pub fn commands(&self) -> &[ReadinessCommand] {
        &self.commands
    }

    fn fact(&mut self, node_id: NodeId) -> &mut NodeFacts {
        self.facts.entry(node_id).or_default()
    }

    fn complete_now(&self) -> bool {
        self.candidates.iter().all(|node_id| {
            self.facts.get(node_id).is_some_and(|facts| {
                facts.known
                    && facts.live
                    && facts.available
                    && facts.identity_ready
                    && !facts.poisoned
            })
        })
    }

    fn reset_stability_if_needed(&mut self) {
        if self.complete_now() {
            if self.stable_since_ms.is_none() {
                self.stable_since_ms = Some(self.now_ms);
                self.commands.push(ReadinessCommand::WaitForStability {
                    pool_id: self.config.pool_id.clone(),
                });
            }
        } else {
            self.stable_since_ms = None;
        }
    }

    fn maybe_ready(&mut self) {
        if self.emitted_ready || !self.complete_now() {
            return;
        }
        let Some(stable_since) = self.stable_since_ms else {
            return;
        };
        if self.now_ms.saturating_sub(stable_since) < self.config.convergence_window_ms {
            return;
        }
        self.emitted_ready = true;
        let pool = self.candidates.clone();
        self.events
            .push(ReadinessEvent::PoolReady { pool: pool.clone() });
        self.commands
            .push(ReadinessCommand::EmitPoolReady { pool: pool.clone() });
        if let Some(request) = &self.pending_run {
            self.commands.push(ReadinessCommand::StartPlanning {
                run_id: request.run_id,
                pool,
            });
        }
    }
}
