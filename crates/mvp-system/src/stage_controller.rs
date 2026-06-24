#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EdgeId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StepId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceHandle {
    pub generation: u64,
    pub id: u64,
}

impl DeviceHandle {
    pub fn new_current(id: u64) -> Self {
        Self { generation: 1, id }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LayerRange {
    pub start: u32,
    pub end_exclusive: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WeightSource {
    TestArtifact(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeDirection {
    Inbound,
    Outbound,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EdgeProvision {
    pub edge_id: EdgeId,
    pub direction: EdgeDirection,
}

impl EdgeProvision {
    pub fn inbound(edge_id: EdgeId) -> Self {
        Self {
            edge_id,
            direction: EdgeDirection::Inbound,
        }
    }

    pub fn outbound(edge_id: EdgeId) -> Self {
        Self {
            edge_id,
            direction: EdgeDirection::Outbound,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionStage {
    pub run_id: RunId,
    pub authorized_orchestrator: NodeId,
    pub node_id: NodeId,
    pub stage_index: u32,
    pub stage_count: u32,
    pub layer_range: LayerRange,
    pub inbound: EdgeProvision,
    pub outbound: EdgeProvision,
    pub weight_source: WeightSource,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StageEvent {
    ProvisionStage {
        from: NodeId,
        provision: ProvisionStage,
    },
    WorkerReady,
    WeightsReady,
    InboundEdgeReady {
        edge_id: EdgeId,
    },
    OutboundEdgeReady {
        edge_id: EdgeId,
    },
    ObjectLoaded {
        edge_id: EdgeId,
        object_id: ObjectId,
        sequence: u64,
        handle: DeviceHandle,
    },
    StepCompleted {
        step_id: StepId,
    },
    WorkerCrashed,
    StopRun {
        run_id: RunId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StageFaultReason {
    UnauthorizedProvision,
    SequenceViolation,
    WorkerCrashed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StageLifecycleEvent {
    StageReady {
        run_id: RunId,
        stage_index: u32,
    },
    StepAccepted {
        run_id: RunId,
        stage_index: u32,
        sequence: u64,
    },
    StageFault {
        run_id: RunId,
        stage_index: u32,
        reason: StageFaultReason,
    },
    StageStopped {
        run_id: RunId,
        stage_index: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepInput {
    pub edge_id: EdgeId,
    pub object_id: ObjectId,
    pub sequence: u64,
    pub handle: DeviceHandle,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputBinding {
    pub edge_id: EdgeId,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecuteStep {
    pub step_id: StepId,
    pub input: StepInput,
    pub outputs: Vec<OutputBinding>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StageCommand {
    EstablishInboundEdge {
        edge_id: EdgeId,
    },
    EstablishOutboundEdge {
        edge_id: EdgeId,
    },
    ConfigureWorkerRole {
        run_id: RunId,
        stage_index: u32,
        layer_range: LayerRange,
    },
    LoadWeights {
        source: WeightSource,
        range: LayerRange,
    },
    RewireEdge {
        edge_id: EdgeId,
    },
    ExecuteStep(ExecuteStep),
    ReleaseInputHandle {
        object_id: ObjectId,
        handle: DeviceHandle,
    },
    StopLocalEdges {
        run_id: RunId,
    },
    ReleaseRunDeviceObjects {
        run_id: RunId,
    },
}

pub type StageControllerHarness = StageController;

pub struct StageController {
    local_node_id: NodeId,
    provision: Option<ProvisionStage>,
    worker_ready: bool,
    weights_ready: bool,
    inbound_ready: bool,
    outbound_ready: bool,
    stage_ready_emitted: bool,
    busy: bool,
    expected_sequence: u64,
    active_input: Option<StepInput>,
    commands: Vec<StageCommand>,
    events: Vec<StageLifecycleEvent>,
    faulted: bool,
    stopped: bool,
}

impl StageController {
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            provision: None,
            worker_ready: false,
            weights_ready: false,
            inbound_ready: false,
            outbound_ready: false,
            stage_ready_emitted: false,
            busy: false,
            expected_sequence: 0,
            active_input: None,
            commands: Vec::new(),
            events: Vec::new(),
            faulted: false,
            stopped: false,
        }
    }

    pub fn observe(&mut self, event: StageEvent) {
        match event {
            StageEvent::ProvisionStage { from, provision } => self.provision(from, provision),
            StageEvent::WorkerReady => self.worker_ready = true,
            StageEvent::WeightsReady => self.weights_ready = true,
            StageEvent::InboundEdgeReady { edge_id } => {
                if self
                    .provision
                    .as_ref()
                    .is_some_and(|p| p.inbound.edge_id == edge_id)
                {
                    self.inbound_ready = true;
                }
            }
            StageEvent::OutboundEdgeReady { edge_id } => {
                if self
                    .provision
                    .as_ref()
                    .is_some_and(|p| p.outbound.edge_id == edge_id)
                {
                    self.outbound_ready = true;
                }
            }
            StageEvent::ObjectLoaded {
                edge_id,
                object_id,
                sequence,
                handle,
            } => self.object_loaded(edge_id, object_id, sequence, handle),
            StageEvent::StepCompleted { .. } => self.step_completed(),
            StageEvent::WorkerCrashed => self.fault(StageFaultReason::WorkerCrashed),
            StageEvent::StopRun { run_id } => self.stop(run_id),
        }
        self.maybe_stage_ready();
    }

    pub fn commands(&self) -> &[StageCommand] {
        &self.commands
    }

    pub fn events(&self) -> &[StageLifecycleEvent] {
        &self.events
    }

    fn provision(&mut self, from: NodeId, provision: ProvisionStage) {
        if from != provision.authorized_orchestrator || provision.node_id != self.local_node_id {
            self.provision = Some(provision.clone());
            self.fault(StageFaultReason::UnauthorizedProvision);
            return;
        }
        self.commands.push(StageCommand::EstablishInboundEdge {
            edge_id: provision.inbound.edge_id,
        });
        self.commands.push(StageCommand::EstablishOutboundEdge {
            edge_id: provision.outbound.edge_id,
        });
        self.commands.push(StageCommand::ConfigureWorkerRole {
            run_id: provision.run_id,
            stage_index: provision.stage_index,
            layer_range: provision.layer_range,
        });
        self.commands.push(StageCommand::LoadWeights {
            source: provision.weight_source.clone(),
            range: provision.layer_range,
        });
        self.provision = Some(provision);
    }

    fn maybe_stage_ready(&mut self) {
        if self.stage_ready_emitted || self.faulted || self.stopped {
            return;
        }
        if self.worker_ready && self.weights_ready && self.inbound_ready && self.outbound_ready {
            if let Some(provision) = &self.provision {
                self.stage_ready_emitted = true;
                self.events.push(StageLifecycleEvent::StageReady {
                    run_id: provision.run_id,
                    stage_index: provision.stage_index,
                });
            }
        }
    }

    fn object_loaded(
        &mut self,
        edge_id: EdgeId,
        object_id: ObjectId,
        sequence: u64,
        handle: DeviceHandle,
    ) {
        if self.faulted || self.stopped || !self.stage_ready_emitted || self.busy {
            return;
        }
        let Some(provision) = &self.provision else {
            return;
        };
        if edge_id != provision.inbound.edge_id {
            return;
        }
        if sequence != self.expected_sequence {
            self.fault(StageFaultReason::SequenceViolation);
            return;
        }
        let input = StepInput {
            edge_id,
            object_id,
            sequence,
            handle,
        };
        let step = ExecuteStep {
            step_id: StepId(sequence),
            input: input.clone(),
            outputs: vec![OutputBinding {
                edge_id: provision.outbound.edge_id,
                sequence,
            }],
        };
        self.busy = true;
        self.active_input = Some(input);
        self.commands.push(StageCommand::ExecuteStep(step));
        self.events.push(StageLifecycleEvent::StepAccepted {
            run_id: provision.run_id,
            stage_index: provision.stage_index,
            sequence,
        });
    }

    fn step_completed(&mut self) {
        if self.faulted || self.stopped || !self.busy {
            return;
        }
        if let Some(input) = self.active_input.take() {
            self.commands.push(StageCommand::ReleaseInputHandle {
                object_id: input.object_id,
                handle: input.handle,
            });
            self.expected_sequence = input.sequence + 1;
        }
        self.busy = false;
    }

    fn fault(&mut self, reason: StageFaultReason) {
        if self.faulted || self.stopped {
            return;
        }
        self.faulted = true;
        let (run_id, stage_index) = self
            .provision
            .as_ref()
            .map(|p| (p.run_id, p.stage_index))
            .unwrap_or((RunId(0), 0));
        self.events.push(StageLifecycleEvent::StageFault {
            run_id,
            stage_index,
            reason,
        });
    }

    fn stop(&mut self, run_id: RunId) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        self.commands.push(StageCommand::StopLocalEdges { run_id });
        self.commands
            .push(StageCommand::ReleaseRunDeviceObjects { run_id });
        let stage_index = self.provision.as_ref().map_or(0, |p| p.stage_index);
        self.events.push(StageLifecycleEvent::StageStopped {
            run_id,
            stage_index,
        });
    }
}
