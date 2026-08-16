use std::collections::BTreeMap;

pub use crate::ids::{ActorAddress, EdgeId, LeaseRequestId, NodeId, RingId, RunId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RingDirection {
    Egress,
    Ingress,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectKind {
    Activation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DType {
    F16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectSpec {
    pub kind: ObjectKind,
    pub dtype: DType,
    pub max_extent_bytes: u64,
}

impl ObjectSpec {
    pub const fn test_activation() -> Self {
        Self {
            kind: ObjectKind::Activation,
            dtype: DType::F16,
            max_extent_bytes: 4096,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingSpec {
    pub header_bytes: u64,
    pub data_bytes: u64,
    pub alignment: u64,
}

impl RingSpec {
    pub const fn test_activation() -> Self {
        Self {
            header_bytes: 128,
            data_bytes: 4096,
            alignment: 64,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingLayout {
    pub start_offset: u64,
    pub header_offset: u64,
    pub data_offset: u64,
    pub end_offset: u64,
    pub data_bytes: u64,
    pub alignment: u64,
}

impl RingLayout {
    pub const fn test_layout(start_offset: u64) -> Self {
        Self {
            start_offset,
            header_offset: start_offset,
            data_offset: start_offset + 128,
            end_offset: start_offset + 128 + 4096,
            data_bytes: 4096,
            alignment: 64,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuiescenceProof {
    verified: bool,
}

impl QuiescenceProof {
    pub const fn verified() -> Self {
        Self { verified: true }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionTx {
    pub run_id: RunId,
    pub edge_id: EdgeId,
    pub local_node_id: NodeId,
    pub consumer_node_id: NodeId,
    pub object_spec: ObjectSpec,
    pub ring_spec: RingSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionRx {
    pub run_id: RunId,
    pub edge_id: EdgeId,
    pub local_node_id: NodeId,
    pub object_spec: ObjectSpec,
    pub ring_spec: RingSpec,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RingLeaseRejection {
    CannotFit,
    ArenaShuttingDown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamFaultReason {
    ReadError,
    WriteError,
    ProtocolError,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RingFaultReason {
    WorkerRejectedRing,
    WorkerCrashed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EdgeEvent {
    ProvisionTx(ProvisionTx),
    ProvisionRx(ProvisionRx),
    RingLeased {
        request_id: LeaseRequestId,
        ring_id: RingId,
        layout: RingLayout,
    },
    RingLeaseRejected {
        request_id: LeaseRequestId,
        reason: RingLeaseRejection,
    },
    RingInstalled {
        edge_id: EdgeId,
        ring_id: RingId,
    },
    DriverEdgeReady {
        edge_id: EdgeId,
    },
    StreamFault {
        edge_id: EdgeId,
        reason: StreamFaultReason,
    },
    RingFault {
        edge_id: EdgeId,
        ring_id: RingId,
        reason: RingFaultReason,
    },
    StopEdge {
        edge_id: EdgeId,
    },
    PumpStopped {
        edge_id: EdgeId,
        ring_id: RingId,
    },
    RingQuiesced {
        ring_id: RingId,
    },
    QuiescenceProven {
        ring_id: RingId,
    },
    Stopped {
        edge_id: EdgeId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EdgeCommand {
    LeaseRing {
        request_id: LeaseRequestId,
        edge_id: EdgeId,
        direction: RingDirection,
        ring_spec: RingSpec,
    },
    CancelQueuedLease {
        request_id: LeaseRequestId,
        edge_id: EdgeId,
    },
    InstallWorkerRing {
        edge_id: EdgeId,
        ring_id: RingId,
        direction: RingDirection,
        layout: RingLayout,
        object_spec: ObjectSpec,
        ring_spec: RingSpec,
    },
    UninstallWorkerRing {
        edge_id: EdgeId,
        ring_id: RingId,
    },
    EstablishSend {
        edge_id: EdgeId,
        consumer_node_id: NodeId,
        layout: RingLayout,
    },
    EstablishRecv {
        edge_id: EdgeId,
        layout: RingLayout,
    },
    StopPump {
        edge_id: EdgeId,
        ring_id: RingId,
    },
    ReleaseArenaLease {
        ring_id: RingId,
        proof: QuiescenceProof,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeFaultReason {
    RingLeaseRejected(RingLeaseRejection),
    RingFault(RingFaultReason),
    StreamFault(StreamFaultReason),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EdgeLifecycleEvent {
    EdgeReady {
        edge_id: EdgeId,
        local_edge_actor: ActorAddress,
    },
    EdgeFaulted {
        edge_id: EdgeId,
        reason: EdgeFaultReason,
    },
    EdgeStopped {
        edge_id: EdgeId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalEdgeRecord {
    pub edge_id: EdgeId,
    pub direction: RingDirection,
    pub state: EdgeProvisionState,
    pub lease_request_id: Option<LeaseRequestId>,
    pub ring_id: Option<RingId>,
    pub peer_node_id: Option<NodeId>,
    pub local_edge_actor: ActorAddress,
    pub remote_actor_address: Option<ActorAddress>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeProvisionState {
    WaitingForLease,
    WaitingForWorkerRing,
    WaitingForDriver,
    Ready,
    Stopping,
    Stopped,
    Failed,
}

pub struct EdgeEstablisher {
    state: EdgeEstablisherState,
}

impl EdgeEstablisher {
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            state: EdgeEstablisherState::new(local_node_id),
        }
    }

    pub fn observe(&mut self, event: EdgeEvent) {
        self.state.observe(event);
    }

    pub fn commands(&self) -> &[EdgeCommand] {
        &self.state.commands
    }

    pub fn events(&self) -> &[EdgeLifecycleEvent] {
        &self.state.events
    }

    pub fn local_record(&self, edge_id: EdgeId) -> Option<LocalEdgeRecord> {
        self.state.records.get(&edge_id).map(EdgeRecord::snapshot)
    }
}

struct EdgeEstablisherState {
    local_node_id: NodeId,
    next_request_id: u64,
    next_actor_id: u64,
    records: BTreeMap<EdgeId, EdgeRecord>,
    commands: Vec<EdgeCommand>,
    events: Vec<EdgeLifecycleEvent>,
}

impl EdgeEstablisherState {
    fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            next_request_id: 1,
            next_actor_id: 1,
            records: BTreeMap::new(),
            commands: Vec::new(),
            events: Vec::new(),
        }
    }

    fn observe(&mut self, event: EdgeEvent) {
        match event {
            EdgeEvent::ProvisionTx(provision) => self.provision_tx(provision),
            EdgeEvent::ProvisionRx(provision) => self.provision_rx(provision),
            EdgeEvent::RingLeased {
                request_id,
                ring_id,
                layout,
            } => self.ring_leased(request_id, ring_id, layout),
            EdgeEvent::RingLeaseRejected { request_id, reason } => {
                self.ring_lease_rejected(request_id, reason);
            }
            EdgeEvent::RingInstalled { edge_id, ring_id } => self.ring_installed(edge_id, ring_id),
            EdgeEvent::DriverEdgeReady { edge_id } => self.driver_edge_ready(edge_id),
            EdgeEvent::StreamFault { edge_id, reason } => {
                self.fault_edge(edge_id, EdgeFaultReason::StreamFault(reason));
            }
            EdgeEvent::RingFault {
                edge_id,
                ring_id,
                reason,
            } => self.ring_fault(edge_id, ring_id, reason),
            EdgeEvent::StopEdge { edge_id } => self.stop_edge(edge_id),
            EdgeEvent::PumpStopped { edge_id, ring_id } => self.pump_stopped(edge_id, ring_id),
            EdgeEvent::RingQuiesced { ring_id } => self.ring_quiesced(ring_id),
            EdgeEvent::QuiescenceProven { ring_id } => self.quiescence_proven(ring_id),
            EdgeEvent::Stopped { edge_id } => self.mark_stopped(edge_id),
        }
    }

    fn provision_tx(&mut self, provision: ProvisionTx) {
        if provision.local_node_id != self.local_node_id {
            return;
        }

        let request_id = self.next_request_id();
        let actor = self.next_actor_address();
        let edge_id = provision.edge_id;
        let ring_spec = provision.ring_spec;
        self.records
            .insert(edge_id, EdgeRecord::new_tx(provision, request_id, actor));
        self.commands.push(EdgeCommand::LeaseRing {
            request_id,
            edge_id,
            direction: RingDirection::Egress,
            ring_spec,
        });
    }

    fn provision_rx(&mut self, provision: ProvisionRx) {
        if provision.local_node_id != self.local_node_id {
            return;
        }

        let request_id = self.next_request_id();
        let actor = self.next_actor_address();
        let edge_id = provision.edge_id;
        let ring_spec = provision.ring_spec;
        self.records
            .insert(edge_id, EdgeRecord::new_rx(provision, request_id, actor));
        self.commands.push(EdgeCommand::LeaseRing {
            request_id,
            edge_id,
            direction: RingDirection::Ingress,
            ring_spec,
        });
    }

    fn ring_leased(&mut self, request_id: LeaseRequestId, ring_id: RingId, layout: RingLayout) {
        let Some(edge_id) = self.edge_for_request(request_id) else {
            return;
        };

        let Some(record) = self.records.get_mut(&edge_id) else {
            return;
        };

        if record.state != EdgeProvisionState::WaitingForLease {
            self.commands.push(EdgeCommand::ReleaseArenaLease {
                ring_id,
                proof: QuiescenceProof::verified(),
            });
            return;
        }

        record.ring_id = Some(ring_id);
        record.layout = Some(layout);
        record.worker_ring_requested = true;
        record.state = EdgeProvisionState::WaitingForWorkerRing;
        self.commands.push(EdgeCommand::InstallWorkerRing {
            edge_id,
            ring_id,
            direction: record.direction,
            layout,
            object_spec: record.object_spec,
            ring_spec: record.ring_spec,
        });
    }

    fn ring_lease_rejected(&mut self, request_id: LeaseRequestId, reason: RingLeaseRejection) {
        let Some(edge_id) = self.edge_for_waiting_request(request_id) else {
            return;
        };
        let Some(record) = self.records.get_mut(&edge_id) else {
            return;
        };

        record.state = EdgeProvisionState::Failed;
        self.events.push(EdgeLifecycleEvent::EdgeFaulted {
            edge_id,
            reason: EdgeFaultReason::RingLeaseRejected(reason),
        });
    }

    fn ring_installed(&mut self, edge_id: EdgeId, ring_id: RingId) {
        let Some(record) = self.records.get_mut(&edge_id) else {
            return;
        };
        if record.state != EdgeProvisionState::WaitingForWorkerRing
            || record.ring_id != Some(ring_id)
        {
            return;
        }
        let Some(layout) = record.layout else {
            return;
        };

        record.worker_installed = true;
        record.driver_established = true;
        record.state = EdgeProvisionState::WaitingForDriver;
        match record.direction {
            RingDirection::Egress => {
                if let Some(consumer_node_id) = record.peer_node_id {
                    self.commands.push(EdgeCommand::EstablishSend {
                        edge_id,
                        consumer_node_id,
                        layout,
                    });
                }
            }
            RingDirection::Ingress => {
                self.commands
                    .push(EdgeCommand::EstablishRecv { edge_id, layout });
            }
        }
    }

    fn driver_edge_ready(&mut self, edge_id: EdgeId) {
        let Some(record) = self.records.get_mut(&edge_id) else {
            return;
        };
        if record.state != EdgeProvisionState::WaitingForDriver {
            return;
        }

        record.state = EdgeProvisionState::Ready;
        self.events.push(EdgeLifecycleEvent::EdgeReady {
            edge_id,
            local_edge_actor: record.local_edge_actor,
        });
    }

    fn ring_fault(&mut self, edge_id: EdgeId, ring_id: RingId, reason: RingFaultReason) {
        let Some(record) = self.records.get(&edge_id) else {
            return;
        };
        if record.ring_id != Some(ring_id) {
            return;
        }
        self.fault_edge(edge_id, EdgeFaultReason::RingFault(reason));
    }

    fn fault_edge(&mut self, edge_id: EdgeId, reason: EdgeFaultReason) {
        let Some(record) = self.records.get(&edge_id) else {
            return;
        };
        if matches!(
            record.state,
            EdgeProvisionState::Stopping | EdgeProvisionState::Stopped | EdgeProvisionState::Failed
        ) {
            return;
        }

        self.events
            .push(EdgeLifecycleEvent::EdgeFaulted { edge_id, reason });
        self.start_stopping(edge_id, true);
    }

    fn stop_edge(&mut self, edge_id: EdgeId) {
        self.start_stopping(edge_id, true);
    }

    fn start_stopping(&mut self, edge_id: EdgeId, cancel_lease: bool) {
        let Some(record) = self.records.get_mut(&edge_id) else {
            return;
        };
        if matches!(
            record.state,
            EdgeProvisionState::Stopping | EdgeProvisionState::Stopped
        ) {
            return;
        }

        let request_id = record.lease_request_id;
        let ring_id = record.ring_id;
        let driver_established = record.driver_established;
        let worker_ring_cleanup_required = record.worker_ring_cleanup_required();
        record.state = EdgeProvisionState::Stopping;

        if cancel_lease {
            if let Some(request_id) = request_id {
                self.commands.push(EdgeCommand::CancelQueuedLease {
                    request_id,
                    edge_id,
                });
            }
        }

        let Some(ring_id) = ring_id else {
            self.mark_stopped_with_event(edge_id);
            return;
        };

        if driver_established {
            self.commands
                .push(EdgeCommand::StopPump { edge_id, ring_id });
        }
        if worker_ring_cleanup_required {
            self.commands
                .push(EdgeCommand::UninstallWorkerRing { edge_id, ring_id });
        }
        if !driver_established && !worker_ring_cleanup_required {
            self.release_ring(edge_id, ring_id);
        }
    }

    fn pump_stopped(&mut self, edge_id: EdgeId, ring_id: RingId) {
        let Some(record) = self.records.get_mut(&edge_id) else {
            return;
        };
        if record.state != EdgeProvisionState::Stopping || record.ring_id != Some(ring_id) {
            return;
        }
        record.pump_stopped = true;
        self.release_after_teardown_proofs(edge_id, ring_id);
    }

    fn ring_quiesced(&mut self, ring_id: RingId) {
        let Some(edge_id) = self.edge_for_ring(ring_id) else {
            return;
        };
        let Some(record) = self.records.get_mut(&edge_id) else {
            return;
        };
        if record.state != EdgeProvisionState::Stopping {
            return;
        }
        record.worker_ring_quiesced = true;
        self.release_after_teardown_proofs(edge_id, ring_id);
    }

    fn quiescence_proven(&mut self, ring_id: RingId) {
        let Some(edge_id) = self.edge_for_ring(ring_id) else {
            return;
        };
        let Some(record) = self.records.get_mut(&edge_id) else {
            return;
        };
        if record.state != EdgeProvisionState::Stopping {
            return;
        }
        record.quiescence_proven = true;
        self.release_after_teardown_proofs(edge_id, ring_id);
    }

    fn release_after_teardown_proofs(&mut self, edge_id: EdgeId, ring_id: RingId) {
        let Some(record) = self.records.get(&edge_id) else {
            return;
        };
        if record.state != EdgeProvisionState::Stopping
            || record.ring_id != Some(ring_id)
            || !record.teardown_quiesced()
        {
            return;
        }
        self.release_ring(edge_id, ring_id);
    }

    fn release_ring(&mut self, edge_id: EdgeId, ring_id: RingId) {
        self.commands.push(EdgeCommand::ReleaseArenaLease {
            ring_id,
            proof: QuiescenceProof::verified(),
        });
        self.mark_stopped_with_event(edge_id);
    }

    fn mark_stopped_with_event(&mut self, edge_id: EdgeId) {
        let Some(record) = self.records.get_mut(&edge_id) else {
            return;
        };
        if record.state == EdgeProvisionState::Stopped {
            return;
        }
        record.state = EdgeProvisionState::Stopped;
        self.events
            .push(EdgeLifecycleEvent::EdgeStopped { edge_id });
    }

    fn mark_stopped(&mut self, edge_id: EdgeId) {
        let Some(record) = self.records.get_mut(&edge_id) else {
            return;
        };
        record.state = EdgeProvisionState::Stopped;
    }

    fn edge_for_request(&self, request_id: LeaseRequestId) -> Option<EdgeId> {
        self.records.iter().find_map(|(edge_id, record)| {
            (record.lease_request_id == Some(request_id)).then_some(*edge_id)
        })
    }

    fn edge_for_waiting_request(&self, request_id: LeaseRequestId) -> Option<EdgeId> {
        self.records.iter().find_map(|(edge_id, record)| {
            (record.state == EdgeProvisionState::WaitingForLease
                && record.lease_request_id == Some(request_id))
            .then_some(*edge_id)
        })
    }

    fn edge_for_ring(&self, ring_id: RingId) -> Option<EdgeId> {
        self.records
            .iter()
            .find_map(|(edge_id, record)| (record.ring_id == Some(ring_id)).then_some(*edge_id))
    }

    fn next_request_id(&mut self) -> LeaseRequestId {
        let request_id = LeaseRequestId(self.next_request_id);
        self.next_request_id += 1;
        request_id
    }

    fn next_actor_address(&mut self) -> ActorAddress {
        let actor = ActorAddress(self.next_actor_id);
        self.next_actor_id += 1;
        actor
    }
}

struct EdgeRecord {
    run_id: RunId,
    edge_id: EdgeId,
    direction: RingDirection,
    state: EdgeProvisionState,
    lease_request_id: Option<LeaseRequestId>,
    ring_id: Option<RingId>,
    layout: Option<RingLayout>,
    peer_node_id: Option<NodeId>,
    local_edge_actor: ActorAddress,
    remote_actor_address: Option<ActorAddress>,
    object_spec: ObjectSpec,
    ring_spec: RingSpec,
    worker_ring_requested: bool,
    worker_installed: bool,
    driver_established: bool,
    pump_stopped: bool,
    worker_ring_quiesced: bool,
    quiescence_proven: bool,
}

impl EdgeRecord {
    fn new_tx(provision: ProvisionTx, request_id: LeaseRequestId, actor: ActorAddress) -> Self {
        Self {
            run_id: provision.run_id,
            edge_id: provision.edge_id,
            direction: RingDirection::Egress,
            state: EdgeProvisionState::WaitingForLease,
            lease_request_id: Some(request_id),
            ring_id: None,
            layout: None,
            peer_node_id: Some(provision.consumer_node_id),
            local_edge_actor: actor,
            remote_actor_address: None,
            object_spec: provision.object_spec,
            ring_spec: provision.ring_spec,
            worker_ring_requested: false,
            worker_installed: false,
            driver_established: false,
            pump_stopped: false,
            worker_ring_quiesced: false,
            quiescence_proven: false,
        }
    }

    fn new_rx(provision: ProvisionRx, request_id: LeaseRequestId, actor: ActorAddress) -> Self {
        Self {
            run_id: provision.run_id,
            edge_id: provision.edge_id,
            direction: RingDirection::Ingress,
            state: EdgeProvisionState::WaitingForLease,
            lease_request_id: Some(request_id),
            ring_id: None,
            layout: None,
            peer_node_id: None,
            local_edge_actor: actor,
            remote_actor_address: None,
            object_spec: provision.object_spec,
            ring_spec: provision.ring_spec,
            worker_ring_requested: false,
            worker_installed: false,
            driver_established: false,
            pump_stopped: false,
            worker_ring_quiesced: false,
            quiescence_proven: false,
        }
    }

    fn worker_ring_cleanup_required(&self) -> bool {
        self.worker_ring_requested || self.worker_installed
    }

    fn teardown_quiesced(&self) -> bool {
        (!self.driver_established || self.pump_stopped)
            && (!self.worker_ring_cleanup_required() || self.worker_ring_quiesced)
            && self.quiescence_proven
    }

    fn snapshot(&self) -> LocalEdgeRecord {
        let _ = self.run_id;
        LocalEdgeRecord {
            edge_id: self.edge_id,
            direction: self.direction,
            state: self.state,
            lease_request_id: self.lease_request_id,
            ring_id: self.ring_id,
            peer_node_id: self.peer_node_id,
            local_edge_actor: self.local_edge_actor,
            remote_actor_address: self.remote_actor_address,
        }
    }
}

pub struct EdgeEstablisherHarness {
    establisher: EdgeEstablisher,
}

impl EdgeEstablisherHarness {
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            establisher: EdgeEstablisher::new(local_node_id),
        }
    }

    pub fn observe(&mut self, event: EdgeEvent) {
        self.establisher.observe(event);
    }

    pub fn commands(&self) -> &[EdgeCommand] {
        self.establisher.commands()
    }

    pub fn events(&self) -> &[EdgeLifecycleEvent] {
        self.establisher.events()
    }

    pub fn local_record(&self, edge_id: EdgeId) -> Option<LocalEdgeRecord> {
        self.establisher.local_record(edge_id)
    }
}
