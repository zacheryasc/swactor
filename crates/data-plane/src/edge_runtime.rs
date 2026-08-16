//! Edge runtime: the composition engine for wire edges.
//!
//! [`EdgeRuntime`] merges what used to be three separately-owned pieces:
//!
//! - the pure edge lifecycle protocol ([`crate::edge_lifecycle`]),
//! - the wire bookkeeping formerly living in iroh-driver's `driver_pumps`
//!   (which edges have send/recv pumps, mapping inbound streams to edges),
//! - and the per-tick application glue (draining transport events, buffering
//!   ingress streams, parsing object records into arena rings, executing
//!   lifecycle commands).
//!
//! The runtime is driven synchronously from [`EdgeRuntime::poll`]. All
//! effects go through narrow ports: the byte transport is any
//! [`EdgeTransport`](crate::edge_wire::EdgeTransport) (iroh-driver provides
//! one), worker ring effects go through [`WorkerPort`] (the application
//! implements it over its GPU worker), and the arena is the in-crate
//! [`ArenaManager`]. Observations — telemetry-shaped facts and lifecycle
//! transitions — accumulate and are taken by the application after each
//! poll.

use std::collections::BTreeMap;
use std::time::Instant;

use crate::arena::{
    ArenaEvent, ArenaManager, ArenaRequest, LeaseRing, QuiescenceProof as ArenaQuiescenceProof,
    RingLayout as ArenaRingLayout, RingSpec as ArenaRingSpec,
};
use crate::edge_lifecycle::{
    EdgeCommand, EdgeEstablisher, EdgeEvent, EdgeFaultReason, EdgeLifecycleEvent, NodeId,
    ObjectSpec, ProvisionRx, ProvisionTx, RingDirection, RingLeaseRejection,
    StreamFaultReason,
};
use crate::edge_wire::EdgeTransport;
use crate::ids::{EdgeId, LeaseRequestId, RingId, StreamId};
use crate::object_record::{self, ObjectRecord};

/// Worker effects the runtime drives during edge provisioning and ingress.
pub trait WorkerPort {
    /// Install a leased ring into the worker for `edge_id`; `direction`
    /// decides input/output placement. `layout` is the arena lease layout.
    fn install_ring(
        &mut self,
        edge_id: EdgeId,
        ring_id: RingId,
        direction: RingDirection,
        layout: &ArenaRingLayout,
        object_spec: &ObjectSpec,
    ) -> Result<(), String>;

    /// Uninstall (quiesce) a worker ring.
    fn uninstall_ring(&mut self, ring_id: RingId) -> Result<(), String>;

    /// Load one complete ingress object from `ring_id` and return its device
    /// handle. `spec` is the object parse spec used on the wire.
    fn load_object(
        &mut self,
        edge_id: EdgeId,
        ring_id: RingId,
        record: &ObjectRecord,
        spec: &object_record::ObjectSpec,
    ) -> Result<LoadedObject, String>;
}

/// A worker-loaded ingress object: identity plus its device handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoadedObject {
    pub object_id: u64,
    pub sequence: u64,
    pub handle_generation: u64,
    pub handle_id: u64,
}

/// One structured observation of runtime progress. The application maps
/// these to telemetry and node-agent messages; the runtime emits them and
/// forgets them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Observation {
    StreamArrived {
        edge_id: EdgeId,
        stream_id: StreamId,
    },
    BytesRead {
        edge_id: EdgeId,
        stream_id: StreamId,
        byte_count: usize,
    },
    /// One complete object record was parsed from an ingress stream and
    /// written into the edge's ring.
    IngressRingWrite {
        edge_id: EdgeId,
        ring_id: RingId,
        stream_id: StreamId,
        object_id: u64,
        sequence: u64,
        extent: u64,
        begin_sequence: bool,
        end_of_sequence: bool,
        record_bytes: usize,
        buffered_bytes: usize,
        write_ms: u64,
    },
    /// One ingress object was loaded by the worker and now has a handle.
    ObjectLoaded {
        edge_id: EdgeId,
        ring_id: RingId,
        stream_id: StreamId,
        object: LoadedObject,
        load_ms: u64,
    },
    /// An ingress object failed to parse or load.
    ObjectFailed {
        edge_id: EdgeId,
        object_id: Option<u64>,
    },
    EdgeReady {
        edge_id: EdgeId,
        direction: RingDirection,
    },
    EdgeFaulted {
        edge_id: EdgeId,
        reason: EdgeFaultReason,
    },
    EdgeStopped {
        edge_id: EdgeId,
    },
}

struct InboundEdge {
    edge_id: EdgeId,
    ring_id: Option<RingId>,
    /// Wire parse spec for objects arriving on this edge.
    parse_spec: object_record::ObjectSpec,
    buffers: BTreeMap<StreamId, Vec<u8>>,
}

struct OutboundEdge<P, W> {
    edge_id: EdgeId,
    peer: P,
    ring_id: Option<RingId>,
    writer: Option<W>,
    next_object_id: u64,
}

/// One complete object record drained from an ingress stream buffer.
struct IngressRecord {
    bytes: Vec<u8>,
    record: ObjectRecord,
}

pub struct EdgeRuntime<T: EdgeTransport> {
    establisher: EdgeEstablisher,
    command_cursor: usize,
    lifecycle_cursor: usize,
    /// Wire bookkeeping (formerly iroh-driver `driver_pumps::Driver`).
    send_rings: BTreeMap<EdgeId, RingId>,
    recv_specs: BTreeMap<EdgeId, RingId>,
    pending_streams: BTreeMap<EdgeId, StreamId>,
    recv_rings: BTreeMap<EdgeId, RingId>,
    inbound: Option<InboundEdge>,
    outbound: Option<OutboundEdge<T::PeerAddr, T::Writer>>,
    object_handles: BTreeMap<(EdgeId, u64), LoadedObject>,
    observations: Vec<Observation>,
}

impl<T: EdgeTransport> EdgeRuntime<T> {
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            establisher: EdgeEstablisher::new(local_node_id),
            command_cursor: 0,
            lifecycle_cursor: 0,
            send_rings: BTreeMap::new(),
            recv_specs: BTreeMap::new(),
            pending_streams: BTreeMap::new(),
            recv_rings: BTreeMap::new(),
            inbound: None,
            outbound: None,
            object_handles: BTreeMap::new(),
            observations: Vec::new(),
        }
    }

    /// Provision the node's (single) inbound edge and begin its lifecycle.
    ///
    /// `parse_spec` is the object-record parse spec for this edge's wire
    /// format; the application derives it from its plan.
    pub fn establish_inbound(
        &mut self,
        provision: ProvisionRx,
        parse_spec: object_record::ObjectSpec,
    ) {
        self.inbound = Some(InboundEdge {
            edge_id: provision.edge_id,
            ring_id: None,
            parse_spec,
            buffers: BTreeMap::new(),
        });
        self.establisher.observe(EdgeEvent::ProvisionRx(provision));
    }

    /// Provision the node's (single) outbound edge toward `peer` and begin
    /// its lifecycle.
    pub fn establish_outbound(&mut self, provision: ProvisionTx, peer: T::PeerAddr) {
        self.outbound = Some(OutboundEdge {
            edge_id: provision.edge_id,
            peer,
            ring_id: None,
            writer: None,
            next_object_id: 1,
        });
        self.establisher.observe(EdgeEvent::ProvisionTx(provision));
    }

    /// One tick: drain transport events, ingest inbound bytes, and run the
    /// lifecycle/workflow fixpoint to quiescence.
    ///
    /// Returns `Err` on any fault the previous composition treated as fatal
    /// (object parse/load failure, worker effect failure, writer failure, or
    /// an edge fault). Observations emitted up to and including the fault
    /// remain retrievable via [`Self::take_observations`].
    pub fn poll(
        &mut self,
        transport: &mut T,
        arena: &mut ArenaManager,
        worker: &mut dyn WorkerPort,
    ) -> Result<(), String> {
        // Run the workflow first: provisions established since the last
        // tick (or earlier in this tick) lease and install their rings
        // before ingress bytes are parsed against them.
        self.drive(transport, arena, worker)?;
        for event in transport.drain_events() {
            match event {
                crate::edge_wire::WireEvent::StreamArrived { edge_id, stream_id } => {
                    self.incoming_stream(edge_id, stream_id);
                    self.observations.push(Observation::StreamArrived { edge_id, stream_id });
                }
                crate::edge_wire::WireEvent::BytesRead { edge_id, stream_id, bytes } => {
                    self.observations.push(Observation::BytesRead {
                        edge_id,
                        stream_id,
                        byte_count: bytes.len(),
                    });
                    self.ingress_bytes(arena, worker, edge_id, stream_id, bytes)?;
                }
                crate::edge_wire::WireEvent::StreamEnded { edge_id, stream_id } => {
                    if let Some(inbound) = self
                        .inbound
                        .as_mut()
                        .filter(|edge| edge.edge_id == edge_id)
                    {
                        inbound.buffers.remove(&stream_id);
                    }
                }
                crate::edge_wire::WireEvent::StreamFault {
                    edge_id: Some(edge_id),
                    ..
                } => {
                    self.read_error(edge_id);
                }
                crate::edge_wire::WireEvent::StreamFault { edge_id: None, .. } => {}
            }
        }
        self.drive(transport, arena, worker)
    }

    /// Drain all observations accumulated since the last call.
    pub fn take_observations(&mut self) -> Vec<Observation> {
        std::mem::take(&mut self.observations)
    }

    /// The device handle for one loaded ingress object.
    pub fn loaded_object(&self, edge_id: EdgeId, object_id: u64) -> Option<&LoadedObject> {
        self.object_handles.get(&(edge_id, object_id))
    }

    /// The outbound edge's leased ring, once installed.
    pub fn outbound_ring_id(&self) -> Option<RingId> {
        self.outbound.as_ref().and_then(|edge| edge.ring_id)
    }

    /// The outbound edge's byte writer, once the send pump is established.
    pub fn outbound_writer(&self) -> Option<&T::Writer> {
        self.outbound.as_ref().and_then(|edge| edge.writer.as_ref())
    }

    /// Allocate the next output object id for the outbound edge.
    pub fn alloc_output_object_id(&mut self) -> Result<u64, String> {
        self.outbound
            .as_mut()
            .map(|edge| {
                let id = edge.next_object_id;
                edge.next_object_id = edge.next_object_id.saturating_add(1);
                id
            })
            .ok_or_else(|| "outbound edge missing".to_owned())
    }

    /// The edge id of the provisioned inbound edge.
    pub fn inbound_edge_id(&self) -> Option<EdgeId> {
        self.inbound.as_ref().map(|edge| edge.edge_id)
    }

    /// The edge id of the provisioned outbound edge.
    pub fn outbound_edge_id(&self) -> Option<EdgeId> {
        self.outbound.as_ref().map(|edge| edge.edge_id)
    }

    // ─── Ingress ────────────────────────────────────────────────────────────

    /// Buffer stream bytes on the inbound edge, parse complete object
    /// records, write them into the edge's ring, and load them on the
    /// worker.
    fn ingress_bytes(
        &mut self,
        arena: &mut ArenaManager,
        worker: &mut dyn WorkerPort,
        edge_id: EdgeId,
        stream_id: StreamId,
        bytes: Vec<u8>,
    ) -> Result<(), String> {
        let (parse_spec, records, buffered_bytes) = {
            let Some(inbound) = &mut self.inbound else {
                return Ok(());
            };
            if inbound.edge_id != edge_id {
                return Ok(());
            }
            let buffer = inbound.buffers.entry(stream_id).or_default();
            buffer.extend_from_slice(&bytes);
            let buffered_bytes = buffer.len();
            let mut records = Vec::new();
            loop {
                match take_complete_record(buffer, inbound.parse_spec) {
                    Ok(Some(record)) => records.push(record),
                    Ok(None) => break,
                    Err(e) => {
                        self.observations.push(Observation::ObjectFailed {
                            edge_id,
                            object_id: None,
                        });
                        return Err(e);
                    }
                }
            }
            (inbound.parse_spec, records, buffered_bytes)
        };
        for IngressRecord { bytes, record } in records {
            let ring_id = self
                .inbound
                .as_ref()
                .and_then(|edge| edge.ring_id)
                .ok_or_else(|| "inbound ring missing".to_owned())?;
            let write_started = Instant::now();
            {
                let lease = arena
                    .lookup_lease(ring_id)
                    .ok_or_else(|| format!("inbound ring {} lease missing", ring_id.0))?;
                let data_offset = lease.layout.data_offset;
                arena
                    .write_arena(data_offset, &bytes)
                    .map_err(|e| format!("write ingress ring: {e}"))?;
            }
            self.observations.push(Observation::IngressRingWrite {
                edge_id,
                ring_id,
                stream_id,
                object_id: record.object_id.0,
                sequence: record.sequence,
                extent: record.extent,
                begin_sequence: record.flags.begin_sequence,
                end_of_sequence: record.flags.end_of_sequence,
                record_bytes: bytes.len(),
                buffered_bytes,
                write_ms: elapsed_ms(write_started),
            });
            let load_started = Instant::now();
            let loaded = match worker.load_object(edge_id, ring_id, &record, &parse_spec) {
                Ok(loaded) => loaded,
                Err(e) => {
                    self.observations.push(Observation::ObjectFailed {
                        edge_id,
                        object_id: Some(record.object_id.0),
                    });
                    return Err(e);
                }
            };
            self.observations.push(Observation::ObjectLoaded {
                edge_id,
                ring_id,
                stream_id,
                object: loaded,
                load_ms: elapsed_ms(load_started),
            });
            self.object_handles
                .insert((edge_id, loaded.object_id), loaded);
        }
        Ok(())
    }

    // ─── Workflow fixpoint ──────────────────────────────────────────────────

    /// Run command execution and lifecycle-event draining until nothing
    /// progresses.
    fn drive(
        &mut self,
        transport: &mut T,
        arena: &mut ArenaManager,
        worker: &mut dyn WorkerPort,
    ) -> Result<(), String> {
        loop {
            let progressed = self.execute_commands(transport, arena, worker)?
                || self.drain_lifecycle()?;
            if !progressed {
                break;
            }
        }
        Ok(())
    }

    /// Execute every not-yet-executed establisher command against the arena,
    /// worker, and transport.
    fn execute_commands(
        &mut self,
        transport: &mut T,
        arena: &mut ArenaManager,
        worker: &mut dyn WorkerPort,
    ) -> Result<bool, String> {
        let mut progressed = false;
        while self.command_cursor < self.establisher.commands().len() {
            let command = self.establisher.commands()[self.command_cursor].clone();
            self.command_cursor += 1;
            progressed = true;
            self.execute_command(command, transport, arena, worker)?;
        }
        Ok(progressed)
    }

    fn execute_command(
        &mut self,
        command: EdgeCommand,
        transport: &mut T,
        arena: &mut ArenaManager,
        worker: &mut dyn WorkerPort,
    ) -> Result<(), String> {
        match command {
            EdgeCommand::LeaseRing {
                request_id,
                ring_spec,
                ..
            } => {
                let events = arena.request(ArenaRequest::LeaseRing(LeaseRing {
                    request_id: LeaseRequestId(request_id.0),
                    ring_spec: ArenaRingSpec {
                        header_bytes: ring_spec.header_bytes,
                        data_bytes: ring_spec.data_bytes,
                        alignment: ring_spec.alignment,
                    },
                }));
                for event in events {
                    match event {
                        ArenaEvent::RingLeased { lease } => {
                            self.establisher.observe(EdgeEvent::RingLeased {
                                request_id: LeaseRequestId(lease.request_id.0),
                                ring_id: RingId(lease.ring_id.0),
                                layout: edge_layout(&lease.layout),
                            });
                        }
                        ArenaEvent::RingLeaseRejected { request_id, reason } => {
                            let reason = match reason {
                                crate::arena::RingLeaseRejection::CannotFitWithinCeiling => {
                                    RingLeaseRejection::CannotFit
                                }
                                crate::arena::RingLeaseRejection::ArenaShuttingDown => {
                                    RingLeaseRejection::ArenaShuttingDown
                                }
                            };
                            self.establisher
                                .observe(EdgeEvent::RingLeaseRejected {
                                    request_id: LeaseRequestId(request_id.0),
                                    reason,
                                });
                        }
                        ArenaEvent::RingLeaseQueued { .. }
                        | ArenaEvent::RingReleased { .. }
                        | ArenaEvent::RingReleaseRejected { .. }
                        | ArenaEvent::CancelledFreshLeaseReleased { .. } => {}
                    }
                }
            }
            EdgeCommand::InstallWorkerRing {
                edge_id,
                ring_id,
                direction,
                object_spec,
                ..
            } => {
                let layout = arena
                    .lookup_lease(RingId(ring_id.0))
                    .map(|lease| lease.layout.clone())
                    .ok_or_else(|| format!("ring {} lease missing", ring_id.0))?;
                worker.install_ring(edge_id, ring_id, direction, &layout, &object_spec)?;
                match direction {
                    RingDirection::Ingress => {
                        if let Some(inbound) = self
                            .inbound
                            .as_mut()
                            .filter(|edge| edge.edge_id == edge_id)
                        {
                            inbound.ring_id = Some(ring_id);
                        }
                    }
                    RingDirection::Egress => {
                        if let Some(outbound) = self
                            .outbound
                            .as_mut()
                            .filter(|edge| edge.edge_id == edge_id)
                        {
                            outbound.ring_id = Some(ring_id);
                        }
                    }
                }
                self.establisher
                    .observe(EdgeEvent::RingInstalled { edge_id, ring_id });
            }
            EdgeCommand::EstablishSend { edge_id, .. } => {
                let ring_id = self.edge_ring(edge_id)?;
                let peer = self
                    .outbound
                    .as_ref()
                    .filter(|outbound| outbound.edge_id == edge_id)
                    .map(|outbound| outbound.peer.clone())
                    .ok_or_else(|| "outbound edge missing".to_owned())?;
                let writer = transport.open_writer(edge_id, &peer)?;
                if let Some(outbound) = self.outbound.as_mut().filter(|edge| edge.edge_id == edge_id)
                {
                    outbound.writer = Some(writer);
                }
                self.establish_send_wire(edge_id, ring_id);
            }
            EdgeCommand::EstablishRecv { edge_id, .. } => {
                let ring_id = self.edge_ring(edge_id)?;
                self.establish_recv_wire(edge_id, ring_id);
            }
            EdgeCommand::CancelQueuedLease { request_id, .. } => {
                let _ = arena.request(ArenaRequest::CancelLease {
                    request_id: LeaseRequestId(request_id.0),
                });
            }
            EdgeCommand::StopPump { edge_id, .. } => {
                self.stop_wire(edge_id);
            }
            EdgeCommand::UninstallWorkerRing { ring_id, .. } => {
                worker.uninstall_ring(ring_id)?;
                self.establisher
                    .observe(EdgeEvent::RingQuiesced { ring_id });
            }
            EdgeCommand::ReleaseArenaLease { ring_id, proof } => {
                let proof = if proof == crate::edge_lifecycle::QuiescenceProof::verified() {
                    ArenaQuiescenceProof::verified()
                } else {
                    ArenaQuiescenceProof::missing()
                };
                let _ = arena.request(ArenaRequest::ReleaseRing {
                    ring_id: RingId(ring_id.0),
                    proof,
                });
            }
        }
        Ok(())
    }

    fn edge_ring(&self, edge_id: EdgeId) -> Result<RingId, String> {
        self.establisher
            .local_record(edge_id)
            .and_then(|record| record.ring_id)
            .ok_or_else(|| format!("edge {} ring missing", edge_id.0))
    }

    /// Drain establisher lifecycle events into observations. An edge fault
    /// is reported and then treated as fatal (mirrors the previous
    /// composition, which halted the tick after reporting).
    fn drain_lifecycle(&mut self) -> Result<bool, String> {
        let mut progressed = false;
        while self.lifecycle_cursor < self.establisher.events().len() {
            let event = self.establisher.events()[self.lifecycle_cursor].clone();
            self.lifecycle_cursor += 1;
            progressed = true;
            match event {
                EdgeLifecycleEvent::EdgeReady { edge_id, .. } => {
                    let direction = self
                        .establisher
                        .local_record(edge_id)
                        .map(|record| record.direction)
                        .unwrap_or(RingDirection::Ingress);
                    self.observations
                        .push(Observation::EdgeReady { edge_id, direction });
                }
                EdgeLifecycleEvent::EdgeFaulted { edge_id, reason } => {
                    self.observations
                        .push(Observation::EdgeFaulted { edge_id, reason });
                    return Err(format!("edge {} faulted: {reason:?}", edge_id.0));
                }
                EdgeLifecycleEvent::EdgeStopped { edge_id } => {
                    self.observations
                        .push(Observation::EdgeStopped { edge_id });
                }
            }
        }
        Ok(progressed)
    }

    // ─── Wire bookkeeping (formerly iroh-driver `driver_pumps::Driver`) ─────

    fn establish_send_wire(&mut self, edge_id: EdgeId, ring_id: RingId) {
        self.send_rings.insert(edge_id, ring_id);
        self.establisher
            .observe(EdgeEvent::DriverEdgeReady { edge_id });
    }

    fn establish_recv_wire(&mut self, edge_id: EdgeId, ring_id: RingId) {
        self.recv_specs.insert(edge_id, ring_id);
        if let Some(stream_id) = self.pending_streams.remove(&edge_id) {
            self.spawn_recv(edge_id, stream_id);
        }
    }

    fn incoming_stream(&mut self, edge_id: EdgeId, stream_id: StreamId) {
        if self.recv_specs.contains_key(&edge_id) {
            self.spawn_recv(edge_id, stream_id);
        } else {
            self.pending_streams.insert(edge_id, stream_id);
        }
    }

    fn spawn_recv(&mut self, edge_id: EdgeId, stream_id: StreamId) {
        let Some(ring_id) = self.recv_specs.get(&edge_id).copied() else {
            self.pending_streams.insert(edge_id, stream_id);
            return;
        };
        self.recv_rings.insert(edge_id, ring_id);
        self.establisher
            .observe(EdgeEvent::DriverEdgeReady { edge_id });
    }

    fn read_error(&mut self, edge_id: EdgeId) {
        self.establisher.observe(EdgeEvent::StreamFault {
            edge_id,
            reason: StreamFaultReason::ReadError,
        });
    }

    fn stop_wire(&mut self, edge_id: EdgeId) {
        let ring_id = self
            .send_rings
            .get(&edge_id)
            .copied()
            .or_else(|| self.recv_rings.get(&edge_id).copied())
            .or_else(|| self.recv_specs.get(&edge_id).copied())
            .unwrap_or(RingId(0));

        self.send_rings.remove(&edge_id);
        self.recv_rings.remove(&edge_id);
        self.recv_specs.remove(&edge_id);
        self.pending_streams.remove(&edge_id);
        self.establisher
            .observe(EdgeEvent::PumpStopped { edge_id, ring_id });
    }
}

/// Parse one complete object record off the front of `buffer`, draining its
/// bytes; `Ok(None)` means more bytes are needed.
fn take_complete_record(
    buffer: &mut Vec<u8>,
    spec: object_record::ObjectSpec,
) -> Result<Option<IngressRecord>, String> {
    match object_record::read_object_record(buffer, spec, false) {
        Ok(object_record::ObjectRecordRead::Incomplete) => Ok(None),
        Ok(object_record::ObjectRecordRead::Complete(record)) => {
            let bytes = buffer.drain(..record.total_len).collect();
            Ok(Some(IngressRecord { bytes, record }))
        }
        Err(reason) => Err(format!("invalid ingress record: {reason:?}")),
    }
}

fn edge_layout(layout: &ArenaRingLayout) -> crate::edge_lifecycle::RingLayout {
    crate::edge_lifecycle::RingLayout {
        start_offset: layout.start_offset,
        header_offset: layout.header_offset,
        data_offset: layout.data_offset,
        end_offset: layout.end_offset,
        data_bytes: layout.data_bytes,
        alignment: layout.alignment,
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    started
        .elapsed()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

