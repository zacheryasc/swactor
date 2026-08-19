#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct RunId(pub(crate) u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct NodeId(pub(crate) u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LayerRange {
    pub start: u32,
    pub end_exclusive: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WeightSource {
    WholeGguf { uri: String },
    ShardSet { uris: Vec<String> },
    CachedArtifact { cache_key: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WeightAssignment {
    pub run_id: RunId,
    pub stage_index: u32,
    pub plan_layer_range: LayerRange,
    pub assigned_layer_range: LayerRange,
    pub source: WeightSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArtifactBytes {
    Local,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WeightEvent {
    Provisioned(WeightAssignment),
    ArtifactAvailable { bytes: ArtifactBytes },
    LayerRangeValidated,
    WorkerRangeBound,
    OtherStagePrerequisitesReady,
    DownloadFailed,
    ParseFailed,
    DeviceAllocationFailed,
    BindingFailed,
    InvalidLayerRange,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StageFaultReason {
    WeightDownloadFailed,
    WeightParseFailed,
    DeviceAllocationFailed,
    WeightBindingFailed,
    InvalidLayerRange,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WeightLifecycleEvent {
    WeightsReady {
        run_id: RunId,
        stage_index: u32,
    },
    StageReady {
        run_id: RunId,
        stage_index: u32,
    },
    StageFault {
        run_id: RunId,
        stage_index: u32,
        reason: StageFaultReason,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WeightCommand {
    LoadOrBindRange {
        source: WeightSource,
        range: LayerRange,
    },
}

#[cfg(test)]
pub(crate) struct WeightLifecycleHarness {
    _node_id: NodeId,
    assignment: Option<WeightAssignment>,
    artifact: bool,
    validated: bool,
    bound: bool,
    other_stage_prereqs: bool,
    weights_ready: bool,
    faulted: bool,
    commands: Vec<WeightCommand>,
    events: Vec<WeightLifecycleEvent>,
}

#[cfg(test)]
impl WeightLifecycleHarness {
    pub(crate) fn new(node_id: NodeId) -> Self {
        Self {
            _node_id: node_id,
            assignment: None,
            artifact: false,
            validated: false,
            bound: false,
            other_stage_prereqs: false,
            weights_ready: false,
            faulted: false,
            commands: Vec::new(),
            events: Vec::new(),
        }
    }

    pub(crate) fn observe(&mut self, event: WeightEvent) {
        match event {
            WeightEvent::Provisioned(assignment) => {
                self.commands.push(WeightCommand::LoadOrBindRange {
                    source: assignment.source.clone(),
                    range: assignment.assigned_layer_range,
                });
                if assignment.assigned_layer_range != assignment.plan_layer_range
                    || assignment.assigned_layer_range.start
                        >= assignment.assigned_layer_range.end_exclusive
                {
                    self.assignment = Some(assignment);
                    self.fault(StageFaultReason::InvalidLayerRange);
                } else {
                    self.assignment = Some(assignment);
                }
            }
            WeightEvent::ArtifactAvailable { .. } => self.artifact = true,
            WeightEvent::LayerRangeValidated => self.validated = true,
            WeightEvent::WorkerRangeBound => self.bound = true,
            WeightEvent::OtherStagePrerequisitesReady => self.other_stage_prereqs = true,
            WeightEvent::DownloadFailed => self.fault(StageFaultReason::WeightDownloadFailed),
            WeightEvent::ParseFailed => self.fault(StageFaultReason::WeightParseFailed),
            WeightEvent::DeviceAllocationFailed => {
                self.fault(StageFaultReason::DeviceAllocationFailed)
            }
            WeightEvent::BindingFailed => self.fault(StageFaultReason::WeightBindingFailed),
            WeightEvent::InvalidLayerRange => self.fault(StageFaultReason::InvalidLayerRange),
        }
        self.maybe_weights_ready();
        self.maybe_stage_ready();
    }

    pub(crate) fn commands(&self) -> &[WeightCommand] {
        &self.commands
    }

    pub(crate) fn events(&self) -> &[WeightLifecycleEvent] {
        &self.events
    }

    fn maybe_weights_ready(&mut self) {
        if self.faulted || self.weights_ready || self.assignment.is_none() {
            return;
        }
        if self.artifact && self.validated && self.bound {
            self.weights_ready = true;
            let assignment = self.assignment.as_ref().unwrap();
            self.events.push(WeightLifecycleEvent::WeightsReady {
                run_id: assignment.run_id,
                stage_index: assignment.stage_index,
            });
        }
    }

    fn maybe_stage_ready(&mut self) {
        if self.faulted || !self.weights_ready || !self.other_stage_prereqs {
            return;
        }
        let assignment = self.assignment.as_ref().unwrap();
        if !self
            .events
            .iter()
            .any(|event| matches!(event, WeightLifecycleEvent::StageReady { .. }))
        {
            self.events.push(WeightLifecycleEvent::StageReady {
                run_id: assignment.run_id,
                stage_index: assignment.stage_index,
            });
        }
    }

    fn fault(&mut self, reason: StageFaultReason) {
        if self.faulted {
            return;
        }
        self.faulted = true;
        let (run_id, stage_index) = self
            .assignment
            .as_ref()
            .map(|assignment| (assignment.run_id, assignment.stage_index))
            .unwrap_or((RunId(0), 0));
        self.events.push(WeightLifecycleEvent::StageFault {
            run_id,
            stage_index,
            reason,
        });
    }
}
