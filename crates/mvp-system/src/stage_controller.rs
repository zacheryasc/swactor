use crate::run_plan::{GgufSource, TokenizerSource};

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
pub struct WeightSource {
    pub model_id: String,
    pub gguf_source: GgufSource,
    pub tokenizer: TokenizerSource,
}

impl WeightSource {
    pub fn new(
        model_id: impl Into<String>,
        gguf_source: GgufSource,
        tokenizer: TokenizerSource,
    ) -> Self {
        Self {
            model_id: model_id.into(),
            gguf_source,
            tokenizer,
        }
    }

    pub fn embedded_gguf(model_id: impl Into<String>, path: impl Into<String>) -> Self {
        Self::new(
            model_id,
            GgufSource::LocalPath(path.into()),
            TokenizerSource::EmbeddedGguf,
        )
    }
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
    StepFailed {
        step_id: StepId,
    },
    ObjectFailed {
        edge_id: EdgeId,
        object_id: Option<ObjectId>,
    },
    OutputFault {
        edge_id: EdgeId,
    },
    EdgeFault {
        edge_id: EdgeId,
    },
    StopRun {
        run_id: RunId,
    },
    LocalEdgesStopped {
        run_id: RunId,
    },
    WorkerRingsQuiesced {
        run_id: RunId,
    },
    DeviceObjectsReleased {
        run_id: RunId,
    },
    WorkerRoleReset {
        run_id: RunId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StageFaultReason {
    UnauthorizedProvision,
    SequenceViolation,
    WorkerCrashed,
    StepFailed,
    ObjectFailed,
    OutputFault,
    EdgeFault,
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
    stopping_run: Option<RunId>,
    local_edges_stopped: bool,
    worker_rings_quiesced: bool,
    release_reset_requested: bool,
    device_objects_released: bool,
    worker_role_reset: bool,
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
            stopping_run: None,
            local_edges_stopped: false,
            worker_rings_quiesced: false,
            release_reset_requested: false,
            device_objects_released: false,
            worker_role_reset: false,
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
            StageEvent::StepFailed { .. } => self.fault(StageFaultReason::StepFailed),
            StageEvent::ObjectFailed { .. } => self.fault(StageFaultReason::ObjectFailed),
            StageEvent::OutputFault { .. } => self.fault(StageFaultReason::OutputFault),
            StageEvent::EdgeFault { .. } => self.fault(StageFaultReason::EdgeFault),
            StageEvent::WorkerCrashed => self.fault(StageFaultReason::WorkerCrashed),
            StageEvent::StopRun { run_id } => self.stop(run_id),
            StageEvent::LocalEdgesStopped { run_id } => self.local_edges_stopped(run_id),
            StageEvent::WorkerRingsQuiesced { run_id } => self.worker_rings_quiesced(run_id),
            StageEvent::DeviceObjectsReleased { run_id } => self.device_objects_released(run_id),
            StageEvent::WorkerRoleReset { run_id } => self.worker_role_reset(run_id),
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
        if self.stage_ready_emitted || self.faulted || self.stopped || self.stopping_run.is_some() {
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
        if self.faulted
            || self.stopped
            || self.stopping_run.is_some()
            || !self.stage_ready_emitted
            || self.busy
        {
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
        if self.faulted || self.stopped || self.stopping_run.is_some() {
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
        if self.stopping_run.is_none() {
            self.stopping_run = Some(run_id);
            self.commands.push(StageCommand::StopLocalEdges { run_id });
        }
        self.maybe_request_release_and_reset();
        self.maybe_stage_stopped();
    }

    fn local_edges_stopped(&mut self, run_id: RunId) {
        if self.stopping_run == Some(run_id) && !self.stopped {
            self.local_edges_stopped = true;
            self.maybe_request_release_and_reset();
            self.maybe_stage_stopped();
        }
    }

    fn worker_rings_quiesced(&mut self, run_id: RunId) {
        if self.stopping_run == Some(run_id) && !self.stopped {
            self.worker_rings_quiesced = true;
            self.maybe_request_release_and_reset();
            self.maybe_stage_stopped();
        }
    }

    fn device_objects_released(&mut self, run_id: RunId) {
        if self.stopping_run == Some(run_id) && !self.stopped {
            self.device_objects_released = true;
            self.maybe_stage_stopped();
        }
    }

    fn worker_role_reset(&mut self, run_id: RunId) {
        if self.stopping_run == Some(run_id) && !self.stopped {
            self.worker_role_reset = true;
            self.maybe_stage_stopped();
        }
    }

    fn maybe_request_release_and_reset(&mut self) {
        if self.release_reset_requested || !self.local_edges_stopped || !self.worker_rings_quiesced
        {
            return;
        }
        let Some(run_id) = self.stopping_run else {
            return;
        };
        self.release_reset_requested = true;
        self.commands
            .push(StageCommand::ReleaseRunDeviceObjects { run_id });
    }

    fn maybe_stage_stopped(&mut self) {
        if self.stopped
            || !self.local_edges_stopped
            || !self.worker_rings_quiesced
            || !self.device_objects_released
            || !self.worker_role_reset
        {
            return;
        }
        let Some(run_id) = self.stopping_run else {
            return;
        };
        self.stopped = true;
        let stage_index = self.provision.as_ref().map_or(0, |p| p.stage_index);
        self.events.push(StageLifecycleEvent::StageStopped {
            run_id,
            stage_index,
        });
    }
}
