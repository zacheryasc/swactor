#![allow(dead_code)]

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct RunId(pub(crate) u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct NodeId(pub(crate) u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct StageIndex(pub(crate) u32);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct EdgeId(pub(crate) u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct RingId(pub(crate) u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct ObjectId(pub(crate) u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct Sequence(pub(crate) u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct StepId(pub(crate) u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct WorkerGeneration(pub(crate) u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) enum EventKind {
    NodeStarted,
    NodeAvailable,
    NodeFaulted,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Component {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum FaultReason {
    NodeUnavailable,
    MembershipLoss,
    ProvisioningRejected,
    ArenaBootFailed,
    OversizedRingRequest,
    WeightLifecycleFailed,
    EdgeEstablishmentFailed,
    MalformedObjectHeader,
    EofMidObject,
    StreamFault,
    PumpFailure,
    RingFault,
    WorkerFatal,
    WorkerCrashed,
    DeviceOutOfMemory,
    DeviceCopyFailed,
    SequenceViolation,
    StepFailed,
    UnsupportedRingVersion,
    RingLayoutInvalid,
    RingStateInvalid,
    WorkerProcessExited,
    WorkerShuttingDown,
    WorkerInternal,
    UnsupportedObjectVersion,
    ExtentExceedsMax,
    ExtentAlignmentInvalid,
    DeviceAllocationFailed,
    RoleUnavailable,
    InvalidInputHandle,
    InvalidOutputRing,
    TinygradError,
    OutputExtentInvalid,
    OutputCopyFailed,
    ArenaMapFailed,
    RingHelperAbiMismatch,
    BackendInitFailed,
    MalformedControlMessage,
    UnhandledException,
    EdgeStopped,
    WorkerShutdown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Event {
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
    NodeFaulted {
        node_id: NodeId,
        reason: FaultReason,
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
    pub(crate) fn kind(&self) -> EventKind {
        match self {
            Event::RunScoped { kind, .. }
            | Event::NodeScoped { kind, .. }
            | Event::StageScoped { kind, .. }
            | Event::EdgeScoped { kind, .. }
            | Event::RingScoped { kind, .. }
            | Event::ObjectScoped { kind, .. }
            | Event::StepScoped { kind, .. }
            | Event::WorkerScoped { kind, .. } => *kind,
            Event::NodeFaulted { .. } => EventKind::NodeFaulted,
        }
    }
}

pub(crate) struct TraceBuilder {
    run_id: RunId,
    events: Vec<Event>,
}

impl TraceBuilder {
    pub(crate) fn new(run_id: RunId) -> Self {
        Self {
            run_id,
            events: Vec::new(),
        }
    }

    pub(crate) fn node_started(mut self, node_id: NodeId) -> Self {
        self.events.push(Event::NodeScoped {
            kind: EventKind::NodeStarted,
            node_id,
            component: Component::NodeBoot,
        });
        self
    }

    pub(crate) fn node_available(mut self, node_id: NodeId) -> Self {
        self.events.push(Event::NodeScoped {
            kind: EventKind::NodeAvailable,
            node_id,
            component: Component::NodeBoot,
        });
        self
    }

    pub(crate) fn node_faulted(
        mut self,
        node_id: NodeId,
        reason: FaultReason,
        component: Component,
    ) -> Self {
        self.events.push(Event::NodeFaulted {
            node_id,
            reason,
            component,
        });
        self
    }

    pub(crate) fn pool_ready(mut self, _nodes: Vec<NodeId>) -> Self {
        self.events.push(Event::RunScoped {
            kind: EventKind::PoolReady,
            run_id: self.run_id,
            reason: None,
            component: Component::Membership,
        });
        self
    }

    pub(crate) fn run_planned(mut self) -> Self {
        self.events.push(Event::RunScoped {
            kind: EventKind::RunPlanned,
            run_id: self.run_id,
            reason: None,
            component: Component::Orchestrator,
        });
        self
    }

    pub(crate) fn stage_provision_started(
        mut self,
        stage_index: StageIndex,
        _node_id: NodeId,
    ) -> Self {
        self.stage(
            EventKind::StageProvisionStarted,
            stage_index,
            None,
            Component::Orchestrator,
        );
        self
    }

    pub(crate) fn weights_download_started(mut self, stage_index: StageIndex) -> Self {
        self.stage(
            EventKind::WeightsDownloadStarted,
            stage_index,
            None,
            Component::WeightLifecycle,
        );
        self
    }

    pub(crate) fn weights_downloaded(mut self, stage_index: StageIndex) -> Self {
        self.stage(
            EventKind::WeightsDownloaded,
            stage_index,
            None,
            Component::WeightLifecycle,
        );
        self
    }

    pub(crate) fn weights_loaded(mut self, stage_index: StageIndex) -> Self {
        self.stage(
            EventKind::WeightsLoaded,
            stage_index,
            None,
            Component::WeightLifecycle,
        );
        self
    }

    pub(crate) fn edge_provision_started(mut self, edge_id: EdgeId) -> Self {
        self.edge(EventKind::EdgeProvisionStarted, edge_id);
        self
    }

    pub(crate) fn edge_ready(mut self, edge_id: EdgeId) -> Self {
        self.edge(EventKind::EdgeReady, edge_id);
        self
    }

    pub(crate) fn stage_ready(mut self, stage_index: StageIndex) -> Self {
        self.stage(
            EventKind::StageReady,
            stage_index,
            None,
            Component::StageController,
        );
        self
    }

    pub(crate) fn readiness_barrier_passed(mut self) -> Self {
        self.run(
            EventKind::ReadinessBarrierPassed,
            None,
            Component::Orchestrator,
        );
        self
    }

    pub(crate) fn prompt_injected(mut self, sequence: Sequence) -> Self {
        self.events.push(Event::ObjectScoped {
            kind: EventKind::PromptInjected,
            object_id: ObjectId(9000),
            sequence,
            component: Component::TokenEndpoint,
        });
        self
    }

    pub(crate) fn object_loaded(
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

    pub(crate) fn execute_step_started(mut self, step_id: StepId) -> Self {
        self.events.push(Event::StepScoped {
            kind: EventKind::ExecuteStepStarted,
            step_id,
            component: Component::StageController,
        });
        self
    }

    pub(crate) fn object_produced(
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

    pub(crate) fn step_completed(mut self, step_id: StepId) -> Self {
        self.events.push(Event::StepScoped {
            kind: EventKind::StepCompleted,
            step_id,
            component: Component::StageController,
        });
        self
    }

    pub(crate) fn token_received(mut self, object_id: ObjectId, sequence: Sequence) -> Self {
        self.events.push(Event::ObjectScoped {
            kind: EventKind::TokenReceived,
            object_id,
            sequence,
            component: Component::TokenEndpoint,
        });
        self
    }

    pub(crate) fn run_completed(mut self) -> Self {
        self.run(EventKind::RunCompleted, None, Component::Orchestrator);
        self
    }

    pub(crate) fn stage_faulted(
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

    pub(crate) fn run_faulted(mut self, reason: FaultReason, component: Component) -> Self {
        self.run(EventKind::RunFaulted, Some(reason), component);
        self
    }

    pub(crate) fn stop_run_sent(mut self, stage_index: StageIndex) -> Self {
        self.stage(
            EventKind::StopRunSent,
            stage_index,
            None,
            Component::Orchestrator,
        );
        self
    }

    pub(crate) fn stage_stopped(mut self, stage_index: StageIndex) -> Self {
        self.stage(
            EventKind::StageStopped,
            stage_index,
            None,
            Component::StageController,
        );
        self
    }

    pub(crate) fn run_torn_down(mut self) -> Self {
        self.run(EventKind::RunTornDown, None, Component::Orchestrator);
        self
    }

    pub(crate) fn finish(self) -> Vec<Event> {
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

pub(crate) fn requires_log_scraping(_events: &[Event]) -> bool {
    false
}
