#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EdgeId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectSpec {
    pub max_extent: u64,
    pub alignment: u64,
}

impl ObjectSpec {
    pub fn test_tokens() -> Self {
        Self {
            max_extent: 64,
            alignment: 4,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndpointDirection {
    TokenIn,
    TokenOut,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeSemantics {
    SingleProducerSingleConsumer,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgePlan {
    pub edge_id: EdgeId,
    pub producer_node_id: NodeId,
    pub consumer_node_id: NodeId,
    pub direction: EndpointDirection,
}

impl EdgePlan {
    pub fn token_in(
        edge_id: EdgeId,
        orchestrator_node_id: NodeId,
        first_stage_node: NodeId,
    ) -> Self {
        Self {
            edge_id,
            producer_node_id: orchestrator_node_id,
            consumer_node_id: first_stage_node,
            direction: EndpointDirection::TokenIn,
        }
    }

    pub fn token_out(
        edge_id: EdgeId,
        last_stage_node: NodeId,
        orchestrator_node_id: NodeId,
    ) -> Self {
        Self {
            edge_id,
            producer_node_id: last_stage_node,
            consumer_node_id: orchestrator_node_id,
            direction: EndpointDirection::TokenOut,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenEndpointPlan {
    pub run_id: RunId,
    pub orchestrator_node_id: NodeId,
    pub token_in_edge: EdgePlan,
    pub token_out_edge: EdgePlan,
    pub token_spec: ObjectSpec,
    pub max_tokens: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalEndpoint {
    pub edge_id: EdgeId,
    pub orchestrator_node_id: NodeId,
    pub edge_semantics: EdgeSemantics,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenObjectWrite {
    pub run_id: RunId,
    pub edge_id: EdgeId,
    pub sequence: u64,
    pub tokens: Vec<u32>,
    pub object_spec: ObjectSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndpointCommand {
    CreateTokenInProducer {
        edge_id: EdgeId,
        orchestrator_node_id: NodeId,
    },
    CreateTokenOutConsumer {
        edge_id: EdgeId,
        orchestrator_node_id: NodeId,
    },
    WriteTokenObject(TokenObjectWrite),
    BroadcastStart {
        run_id: RunId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndpointEvent {
    TokenInReady {
        edge_id: EdgeId,
    },
    TokenOutReady {
        edge_id: EdgeId,
    },
    AllStagesReady,
    ReadinessBarrierPassed,
    TokenObjectReceived {
        edge_id: EdgeId,
        object_id: ObjectId,
        sequence: u64,
        token_id: u32,
        eos: bool,
    },
    EndpointFault {
        edge_id: EdgeId,
        direction: EndpointDirection,
    },
    MalformedTokenObject {
        edge_id: EdgeId,
        object_id: ObjectId,
    },
    TeardownFailed {
        edge_id: EdgeId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunFaultReason {
    TokenSequenceViolation,
    TokenEndpointFault,
    MalformedTokenObject,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndpointLifecycleEvent {
    RunFaulted {
        run_id: RunId,
        reason: RunFaultReason,
    },
    TeardownFailed {
        run_id: RunId,
        edge_id: EdgeId,
    },
}

#[cfg(test)]
pub struct TokenEndpointHarness {
    plan: TokenEndpointPlan,
    commands: Vec<EndpointCommand>,
    events: Vec<EndpointLifecycleEvent>,
    local_endpoints: Vec<LocalEndpoint>,
    prompt: Option<Vec<u32>>,
    token_in_ready: bool,
    token_out_ready: bool,
    stages_ready: bool,
    barrier_passed: bool,
    injected_sequences: Vec<u64>,
    expected_token_sequence: u64,
    terminal: bool,
}

#[cfg(test)]
impl TokenEndpointHarness {
    pub fn new(plan: TokenEndpointPlan) -> Self {
        let commands = vec![
            EndpointCommand::CreateTokenInProducer {
                edge_id: plan.token_in_edge.edge_id,
                orchestrator_node_id: plan.orchestrator_node_id,
            },
            EndpointCommand::CreateTokenOutConsumer {
                edge_id: plan.token_out_edge.edge_id,
                orchestrator_node_id: plan.orchestrator_node_id,
            },
        ];
        let local_endpoints = vec![
            LocalEndpoint {
                edge_id: plan.token_in_edge.edge_id,
                orchestrator_node_id: plan.orchestrator_node_id,
                edge_semantics: EdgeSemantics::SingleProducerSingleConsumer,
            },
            LocalEndpoint {
                edge_id: plan.token_out_edge.edge_id,
                orchestrator_node_id: plan.orchestrator_node_id,
                edge_semantics: EdgeSemantics::SingleProducerSingleConsumer,
            },
        ];
        Self {
            plan,
            commands,
            events: Vec::new(),
            local_endpoints,
            prompt: None,
            token_in_ready: false,
            token_out_ready: false,
            stages_ready: false,
            barrier_passed: false,
            injected_sequences: Vec::new(),
            expected_token_sequence: 0,
            terminal: false,
        }
    }

    pub fn commands(&self) -> &[EndpointCommand] {
        &self.commands
    }

    pub fn events(&self) -> &[EndpointLifecycleEvent] {
        &self.events
    }

    pub fn local_endpoints(&self) -> &[LocalEndpoint] {
        &self.local_endpoints
    }

    pub fn injected_sequences(&self) -> Vec<u64> {
        self.injected_sequences.clone()
    }

    pub fn request_prompt_injection(&mut self, prompt: Vec<u32>) {
        self.prompt = Some(prompt);
        self.maybe_inject_initial();
    }

    pub fn advance_time_ms(&mut self, _delta: u64) {}

    pub fn observe(&mut self, event: EndpointEvent) {
        match event {
            EndpointEvent::TokenInReady { edge_id }
                if edge_id == self.plan.token_in_edge.edge_id =>
            {
                self.token_in_ready = true
            }
            EndpointEvent::TokenOutReady { edge_id }
                if edge_id == self.plan.token_out_edge.edge_id =>
            {
                self.token_out_ready = true
            }
            EndpointEvent::AllStagesReady => self.stages_ready = true,
            EndpointEvent::ReadinessBarrierPassed => self.barrier_passed = true,
            EndpointEvent::TokenObjectReceived { sequence, eos, .. } => {
                if self.terminal {
                    return;
                }
                if sequence != self.expected_token_sequence {
                    self.fault(RunFaultReason::TokenSequenceViolation);
                    return;
                }
                self.expected_token_sequence += 1;
                if eos || self.injected_sequences.len() as u64 >= self.plan.max_tokens {
                    self.terminal = true;
                } else {
                    self.inject_sequence(sequence + 1, vec![0]);
                }
            }
            EndpointEvent::EndpointFault { .. } => self.fault(RunFaultReason::TokenEndpointFault),
            EndpointEvent::MalformedTokenObject { .. } => {
                self.fault(RunFaultReason::MalformedTokenObject)
            }
            EndpointEvent::TeardownFailed { edge_id } => {
                self.events.push(EndpointLifecycleEvent::TeardownFailed {
                    run_id: self.plan.run_id,
                    edge_id,
                });
            }
            EndpointEvent::TokenInReady { .. } | EndpointEvent::TokenOutReady { .. } => {}
        }
        self.maybe_inject_initial();
    }

    fn maybe_inject_initial(&mut self) {
        if self.injected_sequences.is_empty()
            && self.prompt.is_some()
            && self.token_in_ready
            && self.token_out_ready
            && self.stages_ready
            && self.barrier_passed
        {
            let prompt = self.prompt.clone().unwrap_or_default();
            self.inject_sequence(0, prompt);
        }
    }

    fn inject_sequence(&mut self, sequence: u64, tokens: Vec<u32>) {
        if self.terminal || self.injected_sequences.len() as u64 >= self.plan.max_tokens {
            return;
        }
        self.injected_sequences.push(sequence);
        self.commands
            .push(EndpointCommand::WriteTokenObject(TokenObjectWrite {
                run_id: self.plan.run_id,
                edge_id: self.plan.token_in_edge.edge_id,
                sequence,
                tokens,
                object_spec: self.plan.token_spec,
            }));
    }

    fn fault(&mut self, reason: RunFaultReason) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        self.events.push(EndpointLifecycleEvent::RunFaulted {
            run_id: self.plan.run_id,
            reason,
        });
    }
}
