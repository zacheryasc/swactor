#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StageIndex(pub u32);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EdgeId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RingId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sequence(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StepId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerGeneration(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EventKind {
    NodeStarted,
    NodeAvailable,
    PoolReady,
    RunPlanned,
    StageProvisionStarted,
    WeightsDownloadStarted,
    WeightsDownloaded,
    WeightsLoaded,
    EdgeProvisionStarted,
    EdgeReady,
    StageReady,
    ReadinessBarrierPassed,
    PromptInjected,
    ObjectLoaded,
    ExecuteStepStarted,
    ObjectProduced,
    StepCompleted,
    TokenReceived,
    RunCompleted,
    RunFaulted,
    StopRunSent,
    StageStopped,
    RunTornDown,
    StageFaulted,
    RingReadable,
    WorkerReady,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Component {
    NodeBoot,
    Membership,
    Orchestrator,
    StageController,
    WeightLifecycle,
    EdgeEstablisher,
    GpuWorkerCtl,
    SharedRingHelper,
    TokenEndpoint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultReason {
    WorkerCrashed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    RunScoped {
        kind: EventKind,
        run_id: RunId,
        reason: Option<FaultReason>,
        component: Component,
    },
    NodeScoped {
        kind: EventKind,
        node_id: NodeId,
        component: Component,
    },
    StageScoped {
        kind: EventKind,
        run_id: RunId,
        stage_index: StageIndex,
        reason: Option<FaultReason>,
        component: Component,
    },
    EdgeScoped {
        kind: EventKind,
        edge_id: EdgeId,
        component: Component,
    },
    RingScoped {
        kind: EventKind,
        ring_id: RingId,
        component: Component,
    },
    ObjectScoped {
        kind: EventKind,
        object_id: ObjectId,
        sequence: Sequence,
        component: Component,
    },
    StepScoped {
        kind: EventKind,
        step_id: StepId,
        component: Component,
    },
    WorkerScoped {
        kind: EventKind,
        worker_generation: WorkerGeneration,
        component: Component,
    },
}

impl Event {
    pub fn kind(&self) -> EventKind {
        match self {
            Event::RunScoped { kind, .. }
            | Event::NodeScoped { kind, .. }
            | Event::StageScoped { kind, .. }
            | Event::EdgeScoped { kind, .. }
            | Event::RingScoped { kind, .. }
            | Event::ObjectScoped { kind, .. }
            | Event::StepScoped { kind, .. }
            | Event::WorkerScoped { kind, .. } => *kind,
        }
    }
}

pub struct TraceBuilder {
    run_id: RunId,
    events: Vec<Event>,
}

impl TraceBuilder {
    pub fn new(run_id: RunId) -> Self {
        Self {
            run_id,
            events: Vec::new(),
        }
    }

    pub fn node_started(mut self, node_id: NodeId) -> Self {
        self.events.push(Event::NodeScoped {
            kind: EventKind::NodeStarted,
            node_id,
            component: Component::NodeBoot,
        });
        self
    }

    pub fn node_available(mut self, node_id: NodeId) -> Self {
        self.events.push(Event::NodeScoped {
            kind: EventKind::NodeAvailable,
            node_id,
            component: Component::NodeBoot,
        });
        self
    }

    pub fn pool_ready(mut self, _nodes: Vec<NodeId>) -> Self {
        self.events.push(Event::RunScoped {
            kind: EventKind::PoolReady,
            run_id: self.run_id,
            reason: None,
            component: Component::Membership,
        });
        self
    }

    pub fn run_planned(mut self) -> Self {
        self.events.push(Event::RunScoped {
            kind: EventKind::RunPlanned,
            run_id: self.run_id,
            reason: None,
            component: Component::Orchestrator,
        });
        self
    }

    pub fn stage_provision_started(mut self, stage_index: StageIndex, _node_id: NodeId) -> Self {
        self.stage(
            EventKind::StageProvisionStarted,
            stage_index,
            None,
            Component::Orchestrator,
        );
        self
    }

    pub fn weights_download_started(mut self, stage_index: StageIndex) -> Self {
        self.stage(
            EventKind::WeightsDownloadStarted,
            stage_index,
            None,
            Component::WeightLifecycle,
        );
        self
    }

    pub fn weights_downloaded(mut self, stage_index: StageIndex) -> Self {
        self.stage(
            EventKind::WeightsDownloaded,
            stage_index,
            None,
            Component::WeightLifecycle,
        );
        self
    }

    pub fn weights_loaded(mut self, stage_index: StageIndex) -> Self {
        self.stage(
            EventKind::WeightsLoaded,
            stage_index,
            None,
            Component::WeightLifecycle,
        );
        self
    }

    pub fn edge_provision_started(mut self, edge_id: EdgeId) -> Self {
        self.edge(EventKind::EdgeProvisionStarted, edge_id);
        self
    }

    pub fn edge_ready(mut self, edge_id: EdgeId) -> Self {
        self.edge(EventKind::EdgeReady, edge_id);
        self
    }

    pub fn stage_ready(mut self, stage_index: StageIndex) -> Self {
        self.stage(
            EventKind::StageReady,
            stage_index,
            None,
            Component::StageController,
        );
        self
    }

    pub fn readiness_barrier_passed(mut self) -> Self {
        self.run(
            EventKind::ReadinessBarrierPassed,
            None,
            Component::Orchestrator,
        );
        self
    }

    pub fn prompt_injected(mut self, sequence: Sequence) -> Self {
        self.events.push(Event::ObjectScoped {
            kind: EventKind::PromptInjected,
            object_id: ObjectId(9000),
            sequence,
            component: Component::TokenEndpoint,
        });
        self
    }

    pub fn object_loaded(
        mut self,
        _edge_id: EdgeId,
        object_id: ObjectId,
        sequence: Sequence,
    ) -> Self {
        self.events.push(Event::ObjectScoped {
            kind: EventKind::ObjectLoaded,
            object_id,
            sequence,
            component: Component::GpuWorkerCtl,
        });
        self
    }

    pub fn execute_step_started(mut self, step_id: StepId) -> Self {
        self.events.push(Event::StepScoped {
            kind: EventKind::ExecuteStepStarted,
            step_id,
            component: Component::StageController,
        });
        self
    }

    pub fn object_produced(
        mut self,
        _edge_id: EdgeId,
        object_id: ObjectId,
        sequence: Sequence,
    ) -> Self {
        self.events.push(Event::ObjectScoped {
            kind: EventKind::ObjectProduced,
            object_id,
            sequence,
            component: Component::GpuWorkerCtl,
        });
        self
    }

    pub fn step_completed(mut self, step_id: StepId) -> Self {
        self.events.push(Event::StepScoped {
            kind: EventKind::StepCompleted,
            step_id,
            component: Component::StageController,
        });
        self
    }

    pub fn token_received(mut self, object_id: ObjectId, sequence: Sequence) -> Self {
        self.events.push(Event::ObjectScoped {
            kind: EventKind::TokenReceived,
            object_id,
            sequence,
            component: Component::TokenEndpoint,
        });
        self
    }

    pub fn run_completed(mut self) -> Self {
        self.run(EventKind::RunCompleted, None, Component::Orchestrator);
        self
    }

    pub fn stage_faulted(
        mut self,
        stage_index: StageIndex,
        reason: FaultReason,
        component: Component,
    ) -> Self {
        self.stage(
            EventKind::StageFaulted,
            stage_index,
            Some(reason),
            component,
        );
        self
    }

    pub fn run_faulted(mut self, reason: FaultReason, component: Component) -> Self {
        self.run(EventKind::RunFaulted, Some(reason), component);
        self
    }

    pub fn stop_run_sent(mut self, stage_index: StageIndex) -> Self {
        self.stage(
            EventKind::StopRunSent,
            stage_index,
            None,
            Component::Orchestrator,
        );
        self
    }

    pub fn stage_stopped(mut self, stage_index: StageIndex) -> Self {
        self.stage(
            EventKind::StageStopped,
            stage_index,
            None,
            Component::StageController,
        );
        self
    }

    pub fn run_torn_down(mut self) -> Self {
        self.run(EventKind::RunTornDown, None, Component::Orchestrator);
        self
    }

    pub fn finish(self) -> Vec<Event> {
        self.events
    }

    fn run(&mut self, kind: EventKind, reason: Option<FaultReason>, component: Component) {
        self.events.push(Event::RunScoped {
            kind,
            run_id: self.run_id,
            reason,
            component,
        });
    }

    fn stage(
        &mut self,
        kind: EventKind,
        stage_index: StageIndex,
        reason: Option<FaultReason>,
        component: Component,
    ) {
        self.events.push(Event::StageScoped {
            kind,
            run_id: self.run_id,
            stage_index,
            reason,
            component,
        });
    }

    fn edge(&mut self, kind: EventKind, edge_id: EdgeId) {
        self.events.push(Event::EdgeScoped {
            kind,
            edge_id,
            component: Component::EdgeEstablisher,
        });
    }
}

pub fn requires_log_scraping(_events: &[Event]) -> bool {
    false
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Batching {
    None,
    Fixed(usize),
}

#[cfg(test)]
pub struct EventSubscriberHarness {
    events: Vec<Event>,
    _batching: Batching,
}

#[cfg(test)]
impl EventSubscriberHarness {
    pub fn collect(events: Vec<Event>, batching: Batching) -> Self {
        Self {
            events,
            _batching: batching,
        }
    }

    pub fn flattened_events(&self) -> &[Event] {
        &self.events
    }

    pub fn used_transport_specific_assertions(&self) -> bool {
        false
    }

    pub fn used_storage_specific_assertions(&self) -> bool {
        false
    }
}
