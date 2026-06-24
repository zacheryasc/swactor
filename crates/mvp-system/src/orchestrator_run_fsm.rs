#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StageRef {
    pub stage_index: u32,
    pub node_id: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunPlan {
    pub run_id: RunId,
    pub stages: Vec<StageRef>,
}

impl RunPlan {
    pub fn test_linear(run_id: RunId, stages: Vec<StageRef>) -> Self {
        Self { run_id, stages }
    }

    pub fn stage_nodes(&self) -> Vec<NodeId> {
        self.stages.iter().map(|stage| stage.node_id).collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunConfig {
    pub run_id: RunId,
    pub max_tokens: u64,
    pub prompt: Vec<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StageFaultReason {
    WorkerCrashed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndpointKind {
    TokenIn,
    TokenOut,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeoutKind {
    Execution,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunEvent {
    PoolReady {
        nodes: Vec<NodeId>,
    },
    PlanAvailable(RunPlan),
    StageReady {
        run_id: RunId,
        stage_index: u32,
    },
    TokenInEndpointReady,
    TokenOutEndpointReady,
    TokenReceived {
        sequence: u64,
        token_id: u32,
        eos: bool,
    },
    StageFault {
        run_id: RunId,
        stage_index: u32,
        reason: StageFaultReason,
    },
    EndpointFault {
        run_id: RunId,
        endpoint: EndpointKind,
    },
    Timeout {
        run_id: RunId,
        kind: TimeoutKind,
    },
    StageStopped {
        run_id: RunId,
        stage_index: u32,
    },
    TokenEndpointsStopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunFaultReason {
    StageFault {
        stage_index: u32,
        reason: StageFaultReason,
    },
    EndpointFault {
        endpoint: EndpointKind,
    },
    Timeout {
        kind: TimeoutKind,
    },
    UnknownStageReady {
        stage_index: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecycleEvent {
    RunRejected {
        run_id: RunId,
        reason: RunFaultReason,
    },
    RunFaulted {
        run_id: RunId,
        reason: RunFaultReason,
    },
    RunCompleted {
        run_id: RunId,
    },
    RunTornDown {
        run_id: RunId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageProvision {
    pub run_id: RunId,
    pub stage_index: u32,
    pub node_id: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunCommand {
    ProvisionStage {
        provision: StageProvision,
    },
    CreateTokenInEndpoint {
        run_id: RunId,
    },
    CreateTokenOutEndpoint {
        run_id: RunId,
    },
    InjectPrompt {
        run_id: RunId,
        sequence: u64,
        prompt: Vec<u32>,
    },
    BroadcastStart {
        run_id: RunId,
    },
    StopRun {
        run_id: RunId,
        stage_index: u32,
    },
    TearDownTokenEndpoints {
        run_id: RunId,
    },
}

pub type OrchestratorHarness = OrchestratorRun;

pub struct OrchestratorRun {
    config: RunConfig,
    plan: Option<RunPlan>,
    pool_ready: bool,
    provisioned: bool,
    token_in_ready: bool,
    token_out_ready: bool,
    ready_stages: std::collections::BTreeSet<u32>,
    injected_sequences: Vec<u64>,
    expected_token_sequence: u64,
    events: Vec<LifecycleEvent>,
    commands: Vec<RunCommand>,
    terminal: bool,
    teardown_started: bool,
    stopped_stages: std::collections::BTreeSet<u32>,
    token_endpoints_stopped: bool,
}

impl OrchestratorRun {
    pub fn new(config: RunConfig) -> Self {
        Self {
            config,
            plan: None,
            pool_ready: false,
            provisioned: false,
            token_in_ready: false,
            token_out_ready: false,
            ready_stages: std::collections::BTreeSet::new(),
            injected_sequences: Vec::new(),
            expected_token_sequence: 0,
            events: Vec::new(),
            commands: Vec::new(),
            terminal: false,
            teardown_started: false,
            stopped_stages: std::collections::BTreeSet::new(),
            token_endpoints_stopped: false,
        }
    }

    pub fn observe(&mut self, event: RunEvent) {
        match event {
            RunEvent::PoolReady { .. } => {
                self.pool_ready = true;
                self.maybe_provision();
            }
            RunEvent::PlanAvailable(plan) => {
                self.plan = Some(plan);
                self.maybe_provision();
            }
            RunEvent::StageReady {
                run_id,
                stage_index,
            } => {
                if run_id != self.config.run_id || !self.plan_has_stage(stage_index) {
                    self.fault(RunFaultReason::UnknownStageReady { stage_index });
                    return;
                }
                self.ready_stages.insert(stage_index);
                self.maybe_inject_initial();
            }
            RunEvent::TokenInEndpointReady => {
                self.token_in_ready = true;
                self.maybe_inject_initial();
            }
            RunEvent::TokenOutEndpointReady => {
                self.token_out_ready = true;
                self.maybe_inject_initial();
            }
            RunEvent::TokenReceived { sequence, eos, .. } => {
                if self.terminal {
                    return;
                }
                if sequence != self.expected_token_sequence {
                    return;
                }
                self.expected_token_sequence += 1;
                if eos {
                    self.complete();
                } else if (self.injected_sequences.len() as u64) < self.config.max_tokens {
                    self.inject(sequence + 1);
                } else {
                    self.complete();
                }
            }
            RunEvent::StageFault {
                run_id,
                stage_index,
                reason,
            } if run_id == self.config.run_id => {
                self.fault(RunFaultReason::StageFault {
                    stage_index,
                    reason,
                });
            }
            RunEvent::EndpointFault { run_id, endpoint } if run_id == self.config.run_id => {
                self.fault(RunFaultReason::EndpointFault { endpoint });
            }
            RunEvent::Timeout { run_id, kind } if run_id == self.config.run_id => {
                self.fault(RunFaultReason::Timeout { kind });
            }
            RunEvent::StageStopped {
                run_id,
                stage_index,
            } if run_id == self.config.run_id => {
                self.stopped_stages.insert(stage_index);
                self.maybe_torn_down();
            }
            RunEvent::TokenEndpointsStopped => {
                self.token_endpoints_stopped = true;
                self.maybe_torn_down();
            }
            RunEvent::StageFault { .. }
            | RunEvent::EndpointFault { .. }
            | RunEvent::Timeout { .. }
            | RunEvent::StageStopped { .. } => {}
        }
    }

    pub fn advance_time_ms(&mut self, _delta: u64) {}

    pub fn commands(&self) -> &[RunCommand] {
        &self.commands
    }

    pub fn events(&self) -> &[LifecycleEvent] {
        &self.events
    }

    pub fn injected_sequences(&self) -> Vec<u64> {
        self.injected_sequences.clone()
    }

    fn maybe_provision(&mut self) {
        if !self.pool_ready || self.provisioned {
            return;
        }
        let Some(plan) = &self.plan else {
            return;
        };
        self.provisioned = true;
        for stage in &plan.stages {
            self.commands.push(RunCommand::ProvisionStage {
                provision: StageProvision {
                    run_id: plan.run_id,
                    stage_index: stage.stage_index,
                    node_id: stage.node_id,
                },
            });
        }
        self.commands.push(RunCommand::CreateTokenInEndpoint {
            run_id: self.config.run_id,
        });
        self.commands.push(RunCommand::CreateTokenOutEndpoint {
            run_id: self.config.run_id,
        });
    }

    fn maybe_inject_initial(&mut self) {
        if self.terminal || !self.provisioned || !self.injected_sequences.is_empty() {
            return;
        }
        if self.token_in_ready && self.token_out_ready && self.all_stages_ready() {
            self.inject(0);
        }
    }

    fn inject(&mut self, sequence: u64) {
        if self.terminal {
            return;
        }
        self.injected_sequences.push(sequence);
        self.commands.push(RunCommand::InjectPrompt {
            run_id: self.config.run_id,
            sequence,
            prompt: if sequence == 0 {
                self.config.prompt.clone()
            } else {
                Vec::new()
            },
        });
    }

    fn complete(&mut self) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        self.events.push(LifecycleEvent::RunCompleted {
            run_id: self.config.run_id,
        });
        self.start_teardown();
    }

    fn fault(&mut self, reason: RunFaultReason) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        self.events.push(LifecycleEvent::RunFaulted {
            run_id: self.config.run_id,
            reason,
        });
        self.start_teardown();
    }

    fn start_teardown(&mut self) {
        if self.teardown_started {
            return;
        }
        self.teardown_started = true;
        if let Some(plan) = &self.plan {
            for stage in &plan.stages {
                self.commands.push(RunCommand::StopRun {
                    run_id: self.config.run_id,
                    stage_index: stage.stage_index,
                });
            }
        }
        self.commands.push(RunCommand::TearDownTokenEndpoints {
            run_id: self.config.run_id,
        });
    }

    fn maybe_torn_down(&mut self) {
        if !self.teardown_started || !self.token_endpoints_stopped {
            return;
        }
        if !self.all_stages_stopped() {
            return;
        }
        if !self
            .events
            .iter()
            .any(|event| matches!(event, LifecycleEvent::RunTornDown { .. }))
        {
            self.events.push(LifecycleEvent::RunTornDown {
                run_id: self.config.run_id,
            });
        }
    }

    fn plan_has_stage(&self, stage_index: u32) -> bool {
        self.plan.as_ref().is_some_and(|plan| {
            plan.stages
                .iter()
                .any(|stage| stage.stage_index == stage_index)
        })
    }

    fn all_stages_ready(&self) -> bool {
        self.plan.as_ref().is_some_and(|plan| {
            plan.stages
                .iter()
                .all(|stage| self.ready_stages.contains(&stage.stage_index))
        })
    }

    fn all_stages_stopped(&self) -> bool {
        self.plan.as_ref().is_some_and(|plan| {
            plan.stages
                .iter()
                .all(|stage| self.stopped_stages.contains(&stage.stage_index))
        })
    }
}
