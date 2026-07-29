#![allow(dead_code)]

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct RunId(pub(crate) u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct NodeId(pub(crate) u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StageRef {
    pub stage_index: u32,
    pub node_id: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RunPlan {
    pub run_id: RunId,
    pub stages: Vec<StageRef>,
}

impl RunPlan {
    pub(crate) fn test_linear(run_id: RunId, stages: Vec<StageRef>) -> Self {
        Self { run_id, stages }
    }

    pub(crate) fn stage_nodes(&self) -> Vec<NodeId> {
        self.stages.iter().map(|stage| stage.node_id).collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RunConfig {
    pub run_id: RunId,
    pub max_tokens: u64,
    pub prompt: Vec<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SamplingData {
    pub source_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TokenObjectPayload {
    Prompt {
        tokens: Vec<u32>,
    },
    Decode {
        token_id: u32,
        sampling: SamplingData,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TokenObjectInjection {
    pub sequence: u64,
    pub payload: TokenObjectPayload,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StageFaultReason {
    WorkerCrashed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EndpointKind {
    TokenIn,
    TokenOut,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RunEvent {
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
    OperatorStop {
        run_id: RunId,
    },
    MembershipLost {
        run_id: RunId,
        node_id: NodeId,
    },
    StageStopped {
        run_id: RunId,
        stage_index: u32,
    },
    TokenEndpointsStopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RunFaultReason {
    StageFault {
        stage_index: u32,
        reason: StageFaultReason,
    },
    EndpointFault {
        endpoint: EndpointKind,
    },
    MembershipLost {
        node_id: NodeId,
    },
    UnknownStageReady {
        stage_index: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LifecycleEvent {
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
    RunOperatorStopped {
        run_id: RunId,
    },
    RunTornDown {
        run_id: RunId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StageProvision {
    pub run_id: RunId,
    pub stage_index: u32,
    pub node_id: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RunCommand {
    ProvisionStage {
        provision: StageProvision,
    },
    CreateTokenInEndpoint {
        run_id: RunId,
    },
    CreateTokenOutEndpoint {
        run_id: RunId,
    },
    InjectTokenObject {
        run_id: RunId,
        object: TokenObjectInjection,
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

pub(crate) struct OrchestratorRun {
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
    pub(crate) fn new(config: RunConfig) -> Self {
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

    pub(crate) fn observe(&mut self, event: RunEvent) {
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
            } => self.stage_ready(run_id, stage_index),
            RunEvent::TokenInEndpointReady => {
                self.token_in_ready = true;
                self.maybe_inject_initial();
            }
            RunEvent::TokenOutEndpointReady => {
                self.token_out_ready = true;
                self.maybe_inject_initial();
            }
            RunEvent::TokenReceived {
                sequence,
                token_id,
                eos,
            } => self.token_received(sequence, token_id, eos),
            RunEvent::StageFault { run_id, .. }
            | RunEvent::EndpointFault { run_id, .. }
            | RunEvent::OperatorStop { run_id }
            | RunEvent::MembershipLost { run_id, .. }
            | RunEvent::StageStopped { run_id, .. }
                if run_id != self.config.run_id => {}
            RunEvent::StageFault {
                stage_index,
                reason,
                ..
            } => {
                self.fault(RunFaultReason::StageFault {
                    stage_index,
                    reason,
                });
            }
            RunEvent::EndpointFault { endpoint, .. } => {
                self.fault(RunFaultReason::EndpointFault { endpoint });
            }
            RunEvent::OperatorStop { .. } => self.operator_stop(),
            RunEvent::MembershipLost { node_id, .. } => {
                self.fault(RunFaultReason::MembershipLost { node_id });
            }
            RunEvent::StageStopped { stage_index, .. } => {
                self.stopped_stages.insert(stage_index);
                self.maybe_torn_down();
            }
            RunEvent::TokenEndpointsStopped => {
                self.token_endpoints_stopped = true;
                self.maybe_torn_down();
            }
        }
    }

    fn stage_ready(&mut self, run_id: RunId, stage_index: u32) {
        if run_id != self.config.run_id || !self.plan_has_stage(stage_index) {
            self.fault(RunFaultReason::UnknownStageReady { stage_index });
            return;
        }
        self.ready_stages.insert(stage_index);
        self.maybe_inject_initial();
    }

    fn token_received(&mut self, sequence: u64, token_id: u32, eos: bool) {
        if self.terminal || sequence != self.expected_token_sequence {
            return;
        }
        self.expected_token_sequence += 1;
        if eos {
            self.complete();
        } else if (self.injected_sequences.len() as u64) < self.config.max_tokens {
            self.inject_decode(sequence + 1, token_id, sequence);
        } else {
            self.complete();
        }
    }

    pub(crate) fn advance_time_ms(&mut self, _delta: u64) {}

    pub(crate) fn commands(&self) -> &[RunCommand] {
        &self.commands
    }

    pub(crate) fn events(&self) -> &[LifecycleEvent] {
        &self.events
    }

    pub(crate) fn injected_sequences(&self) -> Vec<u64> {
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
            self.inject_prompt(0);
        }
    }

    fn inject_prompt(&mut self, sequence: u64) {
        self.inject(TokenObjectInjection {
            sequence,
            payload: TokenObjectPayload::Prompt {
                tokens: self.config.prompt.clone(),
            },
        });
    }

    fn inject_decode(&mut self, sequence: u64, token_id: u32, source_sequence: u64) {
        self.inject(TokenObjectInjection {
            sequence,
            payload: TokenObjectPayload::Decode {
                token_id,
                sampling: SamplingData { source_sequence },
            },
        });
    }

    fn inject(&mut self, object: TokenObjectInjection) {
        if self.terminal {
            return;
        }
        self.injected_sequences.push(object.sequence);
        self.commands.push(RunCommand::InjectTokenObject {
            run_id: self.config.run_id,
            object,
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
    fn operator_stop(&mut self) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        self.events.push(LifecycleEvent::RunOperatorStopped {
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
        self.mark_torn_down();
    }

    fn mark_torn_down(&mut self) {
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
