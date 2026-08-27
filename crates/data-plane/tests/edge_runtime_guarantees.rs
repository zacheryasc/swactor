//! Black-box contract tests for the data-plane edge runtime.
//!
//! These tests know only the public `EdgeRuntime` surface driven over a mock
//! byte transport and a mock worker port, with the real arena manager. They
//! assert the composition guarantees:
//!
//! - provisioning an edge reaches `EdgeReady` through the real lifecycle
//!   (lease → worker ring → transport establishment),
//! - inbound streams arriving before recv establishment are held pending,
//! - complete ingress records are parsed, written to the edge's ring, and
//!   surfaced as loaded objects,
//! - outbound edges open the transport writer and allocate object ids,
//! - ingress faults are fatal and reported as observations.
use parking_lot::Mutex;
use std::sync::Arc;

use data_plane::arena::{ArenaConfig, ArenaManager};
use data_plane::edge_lifecycle::{
    DType, NodeId, ObjectKind, ObjectSpec as EdgeObjectSpec, ProvisionRx, ProvisionTx, RingSpec,
};
use data_plane::edge_runtime::{EdgeRuntime, LoadedObject, Observation, WorkerPort};
use data_plane::edge_wire::{EdgeTransport, EdgeWriter, WireEvent};
use data_plane::ids::{EdgeId, RingId, StreamId};
use data_plane::object_record::{
    ObjectFlags, ObjectId, ObjectLayout, ObjectRecord, ObjectRecordBuilder, ObjectSpec as ParseSpec,
};

// ─── Mock transport ─────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct MockWriter {
    sent: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl EdgeWriter for MockWriter {
    fn send(&self, bytes: Vec<u8>) -> Result<(), String> {
        self.sent.lock().push(bytes);
        Ok(())
    }
}

struct MockTransport {
    events: Vec<WireEvent>,
    opened: Vec<EdgeId>,
    writer: MockWriter,
}

impl EdgeTransport for MockTransport {
    type Writer = MockWriter;
    type PeerAddr = ();

    fn open_writer(&mut self, edge_id: EdgeId, _peer: &()) -> Result<Self::Writer, String> {
        self.opened.push(edge_id);
        Ok(self.writer.clone())
    }

    fn drain_events(&mut self) -> Vec<WireEvent> {
        std::mem::take(&mut self.events)
    }
}

// ─── Mock worker ────────────────────────────────────────────────────────────

#[derive(Default)]
struct MockWorker {
    installed: Vec<(EdgeId, RingId)>,
    uninstalled: Vec<RingId>,
    fail_load: bool,
}

impl WorkerPort for MockWorker {
    fn install_ring(
        &mut self,
        edge_id: EdgeId,
        ring_id: RingId,
        _direction: data_plane::edge_lifecycle::RingDirection,
        _layout: &data_plane::arena::RingLayout,
        _object_spec: &EdgeObjectSpec,
    ) -> Result<(), String> {
        self.installed.push((edge_id, ring_id));
        Ok(())
    }

    fn uninstall_ring(&mut self, ring_id: RingId) -> Result<(), String> {
        self.uninstalled.push(ring_id);
        Ok(())
    }

    fn load_object(
        &mut self,
        _edge_id: EdgeId,
        _ring_id: RingId,
        record: &ObjectRecord,
        _spec: &ParseSpec,
    ) -> Result<LoadedObject, String> {
        if self.fail_load {
            return Err("mock load failure".to_owned());
        }
        Ok(LoadedObject {
            object_id: record.object_id.0,
            sequence: record.sequence,
            handle_generation: 3,
            handle_id: 4242,
        })
    }
}

// ─── Fixtures ───────────────────────────────────────────────────────────────

fn boot_arena() -> ArenaManager {
    ArenaManager::boot(ArenaConfig {
        node_id: NodeId(10),
        reservation_ceiling: 1 << 20,
        base_alignment: 64,
    })
    .expect("boot arena")
}

fn parse_spec() -> ParseSpec {
    ParseSpec {
        max_extent: 4096,
        alignment: 16,
        layout: ObjectLayout::Token,
    }
}

fn provision_rx(edge_id: u64) -> ProvisionRx {
    ProvisionRx {
        execution_id: data_plane::ids::ExecutionId(1),
        edge_id: EdgeId(edge_id),
        local_node_id: NodeId(10),
        object_spec: EdgeObjectSpec {
            kind: ObjectKind::Activation,
            dtype: DType::F16,
            max_extent_bytes: 4096,
        },
        ring_spec: RingSpec {
            header_bytes: 0,
            data_bytes: 8192,
            alignment: 64,
        },
    }
}

fn provision_tx(edge_id: u64) -> ProvisionTx {
    ProvisionTx {
        execution_id: data_plane::ids::ExecutionId(1),
        edge_id: EdgeId(edge_id),
        local_node_id: NodeId(10),
        consumer_node_id: NodeId(11),
        object_spec: EdgeObjectSpec {
            kind: ObjectKind::Activation,
            dtype: DType::F16,
            max_extent_bytes: 4096,
        },
        ring_spec: RingSpec {
            header_bytes: 0,
            data_bytes: 8192,
            alignment: 64,
        },
    }
}

fn record_bytes(object_id: u64, sequence: u64) -> Vec<u8> {
    ObjectRecordBuilder::new(parse_spec())
        .object_id(ObjectId(object_id))
        .sequence(sequence)
        .payload(vec![7_u8; 64])
        .flags(ObjectFlags::default())
        .encode()
}

fn arena_runtime(
    events: Vec<WireEvent>,
) -> (
    EdgeRuntime<MockTransport>,
    MockTransport,
    ArenaManager,
    MockWorker,
) {
    let runtime = EdgeRuntime::new(NodeId(10));
    let transport = MockTransport {
        events,
        opened: Vec::new(),
        writer: MockWriter::default(),
    };
    (runtime, transport, boot_arena(), MockWorker::default())
}

fn find_observation(
    observations: &[Observation],
    predicate: impl Fn(&Observation) -> bool,
) -> Option<&Observation> {
    observations.iter().find(|obs| predicate(obs))
}

// ─── Contracts ──────────────────────────────────────────────────────────────

// A fully provisioned inbound edge must reach Ready through the real
// lifecycle: ring leased, worker ring installed, then — because the inbound
// stream already arrived — transport readiness observed and EdgeReady fired.
#[test]
fn inbound_edge_reaches_ready_and_delivers_objects() {
    let (mut runtime, mut transport, mut arena, mut worker) = arena_runtime(vec![
        WireEvent::StreamArrived {
            edge_id: EdgeId(7001),
            stream_id: StreamId(1),
        },
        WireEvent::BytesRead {
            edge_id: EdgeId(7001),
            stream_id: StreamId(1),
            bytes: record_bytes(9001, 1),
        },
    ]);
    runtime.establish_inbound(provision_rx(7001), parse_spec());

    runtime
        .poll(&mut transport, &mut arena, &mut worker)
        .expect("poll");
    let observations = runtime.take_observations();

    assert_eq!(worker.installed.len(), 1, "worker ring must be installed");
    let Observation::EdgeReady { direction, .. } = find_observation(&observations, |obs| {
        matches!(obs, Observation::EdgeReady { .. })
    })
    .expect("edge ready observation") else {
        unreachable!()
    };
    assert_eq!(
        *direction,
        data_plane::edge_lifecycle::RingDirection::Ingress
    );

    let Observation::ObjectLoaded { object, .. } = find_observation(&observations, |obs| {
        matches!(obs, Observation::ObjectLoaded { .. })
    })
    .expect("object loaded observation") else {
        unreachable!()
    };
    assert_eq!(object.object_id, 9001);
    assert_eq!(object.sequence, 1);

    // The loaded object is retrievable by identity for compute admission.
    let loaded = runtime
        .loaded_object(EdgeId(7001), 9001)
        .expect("loaded object handle");
    assert_eq!(loaded.handle_id, 4242);

    // The record bytes were written into the leased ingress ring.
    let ring_id = worker.installed[0].1;
    let lease = arena.lookup_lease(ring_id).expect("ingress lease");
    let written = arena
        .read_arena(lease.layout.data_offset, 40 + 64)
        .expect("read ring");
    assert_eq!(written, record_bytes(9001, 1));
}

// An outbound edge must open the transport writer, become ready, and hand
// out monotonically increasing output object ids.
#[test]
fn outbound_edge_opens_writer_and_allocates_object_ids() {
    let (mut runtime, mut transport, mut arena, mut worker) = arena_runtime(Vec::new());
    runtime.establish_outbound(provision_tx(7002), ());

    runtime
        .poll(&mut transport, &mut arena, &mut worker)
        .expect("poll");
    let observations = runtime.take_observations();

    assert!(
        find_observation(&observations, |obs| matches!(
            obs,
            Observation::EdgeReady { .. }
        ))
        .is_some()
    );
    assert_eq!(transport.opened, vec![EdgeId(7002)]);
    assert_eq!(runtime.outbound_ring_id().map(|ring| ring.0), Some(1));
    assert!(runtime.outbound_writer().is_some());
    assert_eq!(runtime.alloc_output_object_id(), Ok(1));
    assert_eq!(runtime.alloc_output_object_id(), Ok(2));
}

// Streams that arrive before recv establishment are held pending; the edge
// still becomes Ready once establishment completes.
#[test]
fn early_stream_waits_for_recv_establishment() {
    let (mut runtime, mut transport, mut arena, mut worker) =
        arena_runtime(vec![WireEvent::StreamArrived {
            edge_id: EdgeId(7003),
            stream_id: StreamId(9),
        }]);
    runtime.establish_inbound(provision_rx(7003), parse_spec());

    runtime
        .poll(&mut transport, &mut arena, &mut worker)
        .expect("poll");
    let observations = runtime.take_observations();
    assert!(
        find_observation(&observations, |obs| matches!(
            obs,
            Observation::EdgeReady { .. }
        ))
        .is_some()
    );
}

// A malformed ingress record must fault: ObjectFailed observation with no
// object id, and a fatal poll error.
#[test]
fn malformed_ingress_record_is_fatal_and_reported() {
    let garbage = vec![0xDE; 64];
    let (mut runtime, mut transport, mut arena, mut worker) =
        arena_runtime(vec![WireEvent::BytesRead {
            edge_id: EdgeId(7004),
            stream_id: StreamId(1),
            bytes: garbage,
        }]);
    runtime.establish_inbound(provision_rx(7004), parse_spec());

    let result = runtime.poll(&mut transport, &mut arena, &mut worker);
    assert!(result.is_err(), "malformed record must be fatal");
    let observations = runtime.take_observations();
    assert!(
        find_observation(&observations, |obs| {
            matches!(
                obs,
                Observation::ObjectFailed {
                    object_id: None,
                    ..
                }
            )
        })
        .is_some()
    );
}

// A worker load failure must surface ObjectFailed with the object id and
// remain fatal.
#[test]
fn worker_load_failure_reports_object_and_is_fatal() {
    let (mut runtime, mut transport, mut arena, mut worker) =
        arena_runtime(vec![WireEvent::BytesRead {
            edge_id: EdgeId(7005),
            stream_id: StreamId(1),
            bytes: record_bytes(9002, 1),
        }]);
    worker.fail_load = true;
    runtime.establish_inbound(provision_rx(7005), parse_spec());

    let result = runtime.poll(&mut transport, &mut arena, &mut worker);
    assert!(result.is_err(), "load failure must be fatal");
    let observations = runtime.take_observations();
    assert!(
        find_observation(&observations, |obs| {
            matches!(
                obs,
                Observation::ObjectFailed {
                    object_id: Some(9002),
                    ..
                }
            )
        })
        .is_some()
    );
}
