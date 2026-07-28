//! Swactor-facing data-plane actor API.
//!
//! The actor owns lifecycle decisions for logical wire edge endpoints and their
//! bound node-local worker rings. Runtime-specific actors execute the effect
//! messages and report observations back.

use std::collections::{BTreeMap, BTreeSet};

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::Ctx;

use crate::edge_lifecycle as lifecycle;
use crate::object_record;

pub use lifecycle::{
    DType, EdgeFaultReason, EdgeId, LeaseRequestId, NodeId, ObjectKind, ObjectSpec,
    QuiescenceProof, RingDirection, RingId, RingLayout, RingLeaseRejection, RingSpec, RunId,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortId(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeKind {
    TokenIn,
    Activation,
    TokenOut,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeEndpointDirection {
    Inbound,
    Outbound,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerEndpoint(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportBinding {
    Remote { endpoint: Option<PeerEndpoint> },
    LocalOnly,
}

impl TransportBinding {
    fn requires_transport(&self) -> bool {
        matches!(self, Self::Remote { .. })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerRingBinding {
    Required,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataPlaneRunConfig {
    pub run_id: RunId,
    pub local_node_id: NodeId,
    pub arena_actor: ActorAddress,
    pub worker_actor: ActorAddress,
    pub transport_actor: ActorAddress,
    pub report_sink: ActorAddress,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireEdgeEndpoint {
    pub run_id: RunId,
    pub edge_id: EdgeId,
    pub direction: EdgeEndpointDirection,
    pub kind: EdgeKind,
    pub local_node_id: NodeId,
    pub peer_node_id: Option<NodeId>,
    pub peer_endpoint: Option<PeerEndpoint>,
    pub local_role_port: PortId,
    pub object_spec: ObjectSpec,
    pub ring_spec: RingSpec,
    pub object_record_spec: object_record::ObjectSpec,
    pub transport: TransportBinding,
    pub worker_ring: WorkerRingBinding,
}

impl WireEdgeEndpoint {
    fn provision_event(&self) -> Option<lifecycle::EdgeEvent> {
        match self.direction {
            EdgeEndpointDirection::Inbound => {
                Some(lifecycle::EdgeEvent::ProvisionRx(lifecycle::ProvisionRx {
                    run_id: self.run_id,
                    edge_id: self.edge_id,
                    local_node_id: self.local_node_id,
                    object_spec: self.object_spec,
                    ring_spec: self.ring_spec,
                }))
            }
            EdgeEndpointDirection::Outbound => {
                Some(lifecycle::EdgeEvent::ProvisionTx(lifecycle::ProvisionTx {
                    run_id: self.run_id,
                    edge_id: self.edge_id,
                    local_node_id: self.local_node_id,
                    consumer_node_id: self.peer_node_id?,
                    object_spec: self.object_spec,
                    ring_spec: self.ring_spec,
                }))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataPlaneNodeMsg {
    ConfigureRun(DataPlaneRunConfig),
    ProvisionWireEdgeEndpoint(WireEdgeEndpoint),
    Arena(ArenaObservation),
    Worker(WorkerObservation),
    Transport(TransportObservation),
    StopEdge { edge_id: EdgeId },
    StopRun { run_id: RunId },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArenaObservation {
    RingLeased {
        request_id: LeaseRequestId,
        ring_id: RingId,
        layout: RingLayout,
    },
    RingLeaseRejected {
        request_id: LeaseRequestId,
        reason: RingLeaseRejection,
    },
    RingReleased {
        ring_id: RingId,
    },
    RingReleaseRejected {
        ring_id: RingId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerObservation {
    WorkerReady,
    RingInstalled {
        edge_id: EdgeId,
        ring_id: RingId,
    },
    RingFaulted {
        edge_id: EdgeId,
        ring_id: RingId,
        reason: lifecycle::RingFaultReason,
    },
    RingQuiesced {
        ring_id: RingId,
    },
    QuiescenceProven {
        ring_id: RingId,
    },
    ObjectLoaded {
        edge_id: EdgeId,
        ring_id: RingId,
        object_id: object_record::ObjectId,
        sequence: u64,
        extent: u64,
        handle: DeviceHandle,
    },
    ObjectProduced {
        edge_id: EdgeId,
        ring_id: RingId,
        object_id: object_record::ObjectId,
        sequence: u64,
        extent: u64,
    },
    ObjectFailed {
        edge_id: EdgeId,
        ring_id: RingId,
        reason: object_record::ObjectFailureReason,
    },
    WorkerFaulted,
    WorkerStopped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportObservation {
    EdgeReady {
        edge_id: EdgeId,
    },
    BytesAvailable {
        edge_id: EdgeId,
        stream_id: StreamId,
        byte_count: u64,
    },
    StreamClosed {
        edge_id: EdgeId,
    },
    StreamFaulted {
        edge_id: EdgeId,
        reason: lifecycle::StreamFaultReason,
    },
    PumpStopped {
        edge_id: EdgeId,
        ring_id: RingId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataPlaneArenaMsg {
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
    ReleaseRing {
        ring_id: RingId,
        proof: QuiescenceProof,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataPlaneWorkerMsg {
    InstallRing {
        edge_id: EdgeId,
        ring_id: RingId,
        direction: RingDirection,
        layout: RingLayout,
        object_spec: ObjectSpec,
        ring_spec: RingSpec,
        role_port: PortId,
    },
    UninstallRing {
        edge_id: EdgeId,
        ring_id: RingId,
    },
    NotifyRingReadable {
        ring_id: RingId,
    },
    NotifyRingWritable {
        ring_id: RingId,
    },
    LoadObjectFromRing {
        edge_id: EdgeId,
        ring_id: RingId,
        object_id: object_record::ObjectId,
        sequence: u64,
        extent: u64,
    },
    ExecuteStep {
        edge_id: EdgeId,
        sequence: u64,
    },
    ReleaseDeviceObject {
        handle: DeviceHandle,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataPlaneTransportMsg {
    EstablishSend {
        edge_id: EdgeId,
        consumer_node_id: NodeId,
        ring_id: RingId,
        layout: RingLayout,
    },
    EstablishRecv {
        edge_id: EdgeId,
        ring_id: RingId,
        layout: RingLayout,
    },
    WriteWireObject {
        edge_id: EdgeId,
        object_id: object_record::ObjectId,
        sequence: u64,
        extent: u64,
    },
    StopWirePump {
        edge_id: EdgeId,
        ring_id: RingId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataPlaneReportMsg {
    InboundEdgeReady {
        edge_id: EdgeId,
    },
    OutboundEdgeReady {
        edge_id: EdgeId,
    },
    ObjectLoaded {
        edge_id: EdgeId,
        object_id: object_record::ObjectId,
        sequence: u64,
        extent: u64,
        handle: DeviceHandle,
    },
    ObjectProduced {
        edge_id: EdgeId,
        object_id: object_record::ObjectId,
        sequence: u64,
        extent: u64,
    },
    EdgeFaulted {
        edge_id: EdgeId,
        reason: EdgeFaultReason,
    },
    EdgeStopped {
        edge_id: EdgeId,
    },
    LocalEdgesStopped {
        run_id: RunId,
    },
    WorkerDataPlaneFaulted {
        reason: WorkerDataPlaneFaultReason,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerDataPlaneFaultReason {
    WorkerFaulted,
    WorkerStopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerGeneration(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceHandle {
    pub generation: WorkerGeneration,
    pub id: u64,
}

impl DeviceHandle {
    pub const fn new(generation: WorkerGeneration, id: u64) -> Self {
        Self { generation, id }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
struct EdgeBinding {
    direction: EdgeEndpointDirection,
    role_port_index: usize,
    transport: TransportBinding,
}

pub struct DataPlaneNodeActor {
    run: Option<DataPlaneRunConfig>,
    lifecycle: lifecycle::EdgeEstablisher,
    command_cursor: usize,
    event_cursor: usize,
    bindings: BTreeMap<EdgeId, EdgeBinding>,
    stopped_edges: BTreeSet<EdgeId>,
    role_ports: Vec<PortId>,
    rings: BTreeMap<RingId, EdgeId>,
    pending_reports: Vec<DataPlaneReportMsg>,
    stopping_run: Option<RunId>,
    local_edges_stopped_reported: bool,
}

impl DataPlaneNodeActor {
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            run: None,
            lifecycle: lifecycle::EdgeEstablisher::new(local_node_id),
            command_cursor: 0,
            event_cursor: 0,
            bindings: BTreeMap::new(),
            stopped_edges: BTreeSet::new(),
            role_ports: Vec::new(),
            rings: BTreeMap::new(),
            pending_reports: Vec::new(),
            stopping_run: None,
            local_edges_stopped_reported: false,
        }
    }

    fn configure(&mut self, config: DataPlaneRunConfig) {
        self.lifecycle = lifecycle::EdgeEstablisher::new(config.local_node_id);
        self.command_cursor = 0;
        self.event_cursor = 0;
        self.bindings.clear();
        self.role_ports.clear();
        self.stopped_edges.clear();
        self.rings.clear();
        self.pending_reports.clear();
        self.stopping_run = None;
        self.local_edges_stopped_reported = false;
        self.run = Some(config);
    }

    fn observe(&mut self, msg: DataPlaneNodeMsg) {
        match msg {
            DataPlaneNodeMsg::ConfigureRun(config) => self.configure(config),
            DataPlaneNodeMsg::ProvisionWireEdgeEndpoint(endpoint) => {
                if let Some(event) = endpoint.provision_event() {
                    let role_port_index = self.role_ports.len();
                    self.role_ports.push(endpoint.local_role_port.clone());
                    self.bindings.insert(
                        endpoint.edge_id,
                        EdgeBinding {
                            direction: endpoint.direction,
                            role_port_index,
                            transport: endpoint.transport.clone(),
                        },
                    );
                    self.lifecycle.observe(event);
                }
            }
            DataPlaneNodeMsg::Arena(observation) => match observation {
                ArenaObservation::RingLeased {
                    request_id,
                    ring_id,
                    layout,
                } => self.lifecycle.observe(lifecycle::EdgeEvent::RingLeased {
                    request_id,
                    ring_id,
                    layout,
                }),
                ArenaObservation::RingLeaseRejected { request_id, reason } => self
                    .lifecycle
                    .observe(lifecycle::EdgeEvent::RingLeaseRejected { request_id, reason }),
                ArenaObservation::RingReleased { .. }
                | ArenaObservation::RingReleaseRejected { .. } => {}
            },
            DataPlaneNodeMsg::Worker(observation) => match observation {
                WorkerObservation::WorkerReady => {}
                WorkerObservation::RingInstalled { edge_id, ring_id } => {
                    self.rings.insert(ring_id, edge_id);
                    self.lifecycle
                        .observe(lifecycle::EdgeEvent::RingInstalled { edge_id, ring_id });
                }
                WorkerObservation::RingFaulted {
                    edge_id,
                    ring_id,
                    reason,
                } => self.lifecycle.observe(lifecycle::EdgeEvent::RingFault {
                    edge_id,
                    ring_id,
                    reason,
                }),
                WorkerObservation::RingQuiesced { ring_id } => self
                    .lifecycle
                    .observe(lifecycle::EdgeEvent::RingQuiesced { ring_id }),
                WorkerObservation::QuiescenceProven { ring_id } => self
                    .lifecycle
                    .observe(lifecycle::EdgeEvent::QuiescenceProven { ring_id }),
                WorkerObservation::ObjectLoaded {
                    edge_id,
                    object_id,
                    sequence,
                    extent,
                    handle,
                    ..
                } => self.report_object_loaded(edge_id, object_id, sequence, extent, handle),
                WorkerObservation::ObjectProduced {
                    edge_id,
                    object_id,
                    sequence,
                    extent,
                    ..
                } => self.report_object_produced(edge_id, object_id, sequence, extent),
                WorkerObservation::ObjectFailed {
                    edge_id, ring_id, ..
                } => self.lifecycle.observe(lifecycle::EdgeEvent::RingFault {
                    edge_id,
                    ring_id,
                    reason: lifecycle::RingFaultReason::WorkerRejectedRing,
                }),
                WorkerObservation::WorkerFaulted => {
                    self.report_worker_fault(WorkerDataPlaneFaultReason::WorkerFaulted)
                }
                WorkerObservation::WorkerStopped => {
                    self.report_worker_fault(WorkerDataPlaneFaultReason::WorkerStopped)
                }
            },
            DataPlaneNodeMsg::Transport(observation) => match observation {
                TransportObservation::EdgeReady { edge_id } => self
                    .lifecycle
                    .observe(lifecycle::EdgeEvent::DriverEdgeReady { edge_id }),
                TransportObservation::BytesAvailable { .. }
                | TransportObservation::StreamClosed { .. } => {}
                TransportObservation::StreamFaulted { edge_id, reason } => self
                    .lifecycle
                    .observe(lifecycle::EdgeEvent::StreamFault { edge_id, reason }),
                TransportObservation::PumpStopped { edge_id, ring_id } => self
                    .lifecycle
                    .observe(lifecycle::EdgeEvent::PumpStopped { edge_id, ring_id }),
            },
            DataPlaneNodeMsg::StopEdge { edge_id } => self
                .lifecycle
                .observe(lifecycle::EdgeEvent::StopEdge { edge_id }),
            DataPlaneNodeMsg::StopRun { run_id } => {
                self.stopping_run = Some(run_id);
                self.local_edges_stopped_reported = false;
                let edge_ids = self.bindings.keys().copied().collect::<Vec<_>>();
                for edge_id in edge_ids {
                    self.lifecycle
                        .observe(lifecycle::EdgeEvent::StopEdge { edge_id });
                }
                self.maybe_report_local_edges_stopped();
            }
        }
    }

    fn ring_for_edge(&self, edge_id: EdgeId) -> Option<RingId> {
        self.rings
            .iter()
            .find_map(|(ring_id, seen_edge)| (*seen_edge == edge_id).then_some(*ring_id))
    }

    fn role_port_for(&self, edge_id: EdgeId) -> PortId {
        self.bindings
            .get(&edge_id)
            .and_then(|binding| self.role_ports.get(binding.role_port_index))
            .cloned()
            .unwrap_or_else(|| PortId(String::new()))
    }

    fn report_object_loaded(
        &mut self,
        edge_id: EdgeId,
        object_id: object_record::ObjectId,
        sequence: u64,
        extent: u64,
        handle: DeviceHandle,
    ) {
        self.pending_reports.push(DataPlaneReportMsg::ObjectLoaded {
            edge_id,
            object_id,
            sequence,
            extent,
            handle,
        });
    }

    fn report_object_produced(
        &mut self,
        edge_id: EdgeId,
        object_id: object_record::ObjectId,
        sequence: u64,
        extent: u64,
    ) {
        self.pending_reports
            .push(DataPlaneReportMsg::ObjectProduced {
                edge_id,
                object_id,
                sequence,
                extent,
            });
    }

    fn report_worker_fault(&mut self, reason: WorkerDataPlaneFaultReason) {
        self.pending_reports
            .push(DataPlaneReportMsg::WorkerDataPlaneFaulted { reason });
    }

    fn report_local_edges_stopped(&mut self, run_id: RunId) {
        self.pending_reports
            .push(DataPlaneReportMsg::LocalEdgesStopped { run_id });
    }

    fn maybe_report_local_edges_stopped(&mut self) {
        let Some(run_id) = self.stopping_run else {
            return;
        };
        if self.local_edges_stopped_reported {
            return;
        }
        if self
            .bindings
            .keys()
            .all(|edge_id| self.stopped_edges.contains(edge_id))
        {
            self.local_edges_stopped_reported = true;
            self.report_local_edges_stopped(run_id);
        }
    }

    fn drain(&mut self, ctx: &Ctx) {
        let Some(run) = self.run.clone() else {
            return;
        };

        loop {
            let mut progressed = false;
            let mut immediate_ready = Vec::new();

            while self.command_cursor < self.lifecycle.commands().len() {
                let command = self.lifecycle.commands()[self.command_cursor].clone();
                self.command_cursor += 1;
                progressed = true;
                match command {
                    lifecycle::EdgeCommand::LeaseRing {
                        request_id,
                        edge_id,
                        direction,
                        ring_spec,
                    } => {
                        let _ = ctx.send(
                            run.arena_actor,
                            DataPlaneArenaMsg::LeaseRing {
                                request_id,
                                edge_id,
                                direction,
                                ring_spec,
                            },
                        );
                    }
                    lifecycle::EdgeCommand::CancelQueuedLease {
                        request_id,
                        edge_id,
                    } => {
                        let _ = ctx.send(
                            run.arena_actor,
                            DataPlaneArenaMsg::CancelQueuedLease {
                                request_id,
                                edge_id,
                            },
                        );
                    }
                    lifecycle::EdgeCommand::InstallWorkerRing {
                        edge_id,
                        ring_id,
                        direction,
                        layout,
                        object_spec,
                        ring_spec,
                    } => {
                        let _ = ctx.send(
                            run.worker_actor,
                            DataPlaneWorkerMsg::InstallRing {
                                edge_id,
                                ring_id,
                                direction,
                                layout,
                                object_spec,
                                ring_spec,
                                role_port: self.role_port_for(edge_id),
                            },
                        );
                    }
                    lifecycle::EdgeCommand::UninstallWorkerRing { edge_id, ring_id } => {
                        let _ = ctx.send(
                            run.worker_actor,
                            DataPlaneWorkerMsg::UninstallRing { edge_id, ring_id },
                        );
                    }
                    lifecycle::EdgeCommand::EstablishSend {
                        edge_id,
                        consumer_node_id,
                        layout,
                    } => {
                        let ring_id = self.ring_for_edge(edge_id).unwrap_or(RingId(0));
                        if self
                            .bindings
                            .get(&edge_id)
                            .is_some_and(|binding| binding.transport.requires_transport())
                        {
                            let _ = ctx.send(
                                run.transport_actor,
                                DataPlaneTransportMsg::EstablishSend {
                                    edge_id,
                                    consumer_node_id,
                                    ring_id,
                                    layout,
                                },
                            );
                        } else {
                            immediate_ready.push(edge_id);
                        }
                    }
                    lifecycle::EdgeCommand::EstablishRecv { edge_id, layout } => {
                        let ring_id = self.ring_for_edge(edge_id).unwrap_or(RingId(0));
                        if self
                            .bindings
                            .get(&edge_id)
                            .is_some_and(|binding| binding.transport.requires_transport())
                        {
                            let _ = ctx.send(
                                run.transport_actor,
                                DataPlaneTransportMsg::EstablishRecv {
                                    edge_id,
                                    ring_id,
                                    layout,
                                },
                            );
                        } else {
                            immediate_ready.push(edge_id);
                        }
                    }
                    lifecycle::EdgeCommand::StopPump { edge_id, ring_id } => {
                        let _ = ctx.send(
                            run.transport_actor,
                            DataPlaneTransportMsg::StopWirePump { edge_id, ring_id },
                        );
                    }
                    lifecycle::EdgeCommand::ReleaseArenaLease { ring_id, proof } => {
                        let _ = ctx.send(
                            run.arena_actor,
                            DataPlaneArenaMsg::ReleaseRing { ring_id, proof },
                        );
                    }
                }
            }

            for edge_id in immediate_ready {
                self.lifecycle
                    .observe(lifecycle::EdgeEvent::DriverEdgeReady { edge_id });
            }

            while self.event_cursor < self.lifecycle.events().len() {
                let event = self.lifecycle.events()[self.event_cursor].clone();
                self.event_cursor += 1;
                progressed = true;
                match event {
                    lifecycle::EdgeLifecycleEvent::EdgeReady { edge_id, .. } => {
                        let report =
                            match self.bindings.get(&edge_id).map(|binding| binding.direction) {
                                Some(EdgeEndpointDirection::Inbound) => {
                                    DataPlaneReportMsg::InboundEdgeReady { edge_id }
                                }
                                Some(EdgeEndpointDirection::Outbound) => {
                                    DataPlaneReportMsg::OutboundEdgeReady { edge_id }
                                }
                                None => continue,
                            };
                        let _ = ctx.send(run.report_sink, report);
                    }
                    lifecycle::EdgeLifecycleEvent::EdgeFaulted { edge_id, reason } => {
                        let _ = ctx.send(
                            run.report_sink,
                            DataPlaneReportMsg::EdgeFaulted { edge_id, reason },
                        );
                    }
                    lifecycle::EdgeLifecycleEvent::EdgeStopped { edge_id } => {
                        self.stopped_edges.insert(edge_id);
                        let _ =
                            ctx.send(run.report_sink, DataPlaneReportMsg::EdgeStopped { edge_id });
                        self.maybe_report_local_edges_stopped();
                    }
                }
            }

            for report in self.pending_reports.drain(..) {
                progressed = true;
                let _ = ctx.send(run.report_sink, report);
            }

            if !progressed {
                break;
            }
        }
    }
}

impl ActorInterface for DataPlaneNodeActor {
    type Incoming = DataPlaneNodeMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        self.observe(msg);
        self.drain(ctx);
    }
}
