use data_plane::actor as dp;
use data_plane::object_record;
use swactor::config::RuntimeConfig;
use swactor::runtime::{RuntimeParts, SingleThreadRuntime};

fn object_spec() -> dp::ObjectSpec {
    dp::ObjectSpec {
        kind: dp::ObjectKind::Activation,
        dtype: dp::DType::F16,
        max_extent_bytes: 4096,
    }
}

fn record_spec() -> object_record::ObjectSpec {
    object_record::ObjectSpec {
        max_extent: 4096,
        alignment: 4,
        layout: object_record::ObjectLayout::Token,
    }
}

fn ring_spec() -> dp::RingSpec {
    dp::RingSpec {
        header_bytes: 128,
        data_bytes: 4096,
        alignment: 64,
    }
}

fn layout() -> dp::RingLayout {
    dp::RingLayout {
        start_offset: 0,
        header_offset: 0,
        data_offset: 128,
        end_offset: 4224,
        data_bytes: 4096,
        alignment: 64,
    }
}

fn inbound_endpoint() -> dp::WireEdgeEndpoint {
    dp::WireEdgeEndpoint {
        run_id: dp::RunId(55),
        edge_id: dp::EdgeId(7001),
        direction: dp::EdgeEndpointDirection::Inbound,
        kind: dp::EdgeKind::Activation,
        local_node_id: dp::NodeId(10),
        peer_node_id: Some(dp::NodeId(9)),
        peer_endpoint: Some(dp::PeerEndpoint("node-9".to_owned())),
        local_role_port: dp::PortId("input".to_owned()),
        object_spec: object_spec(),
        ring_spec: ring_spec(),
        object_record_spec: record_spec(),
        transport: dp::TransportBinding::Remote {
            endpoint: Some(dp::PeerEndpoint("node-9".to_owned())),
        },
        worker_ring: dp::WorkerRingBinding::Required,
    }
}

#[test]
fn inbound_wire_edge_establishes_through_arena_worker_transport_then_reports_ready() {
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let mut host = SingleThreadRuntime::new(parts);
    let arena = runtime
        .new_inbox::<dp::DataPlaneArenaMsg>()
        .expect("arena inbox");
    let worker = runtime
        .new_inbox::<dp::DataPlaneWorkerMsg>()
        .expect("worker inbox");
    let transport = runtime
        .new_inbox::<dp::DataPlaneTransportMsg>()
        .expect("transport inbox");
    let reports = runtime
        .new_inbox::<dp::DataPlaneReportMsg>()
        .expect("report inbox");
    let actor = runtime
        .spawn(dp::DataPlaneNodeActor::new(dp::NodeId(10)))
        .expect("spawn data-plane actor");

    runtime
        .send_to(
            actor,
            dp::DataPlaneNodeMsg::ConfigureRun(dp::DataPlaneRunConfig {
                run_id: dp::RunId(55),
                local_node_id: dp::NodeId(10),
                arena_actor: *arena.addr(),
                worker_actor: *worker.addr(),
                transport_actor: *transport.addr(),
                report_sink: *reports.addr(),
            }),
        )
        .expect("configure run");
    runtime
        .send_to(
            actor,
            dp::DataPlaneNodeMsg::ProvisionWireEdgeEndpoint(inbound_endpoint()),
        )
        .expect("provision inbound");
    host.tick();

    assert_eq!(
        arena.try_recv(),
        Some(dp::DataPlaneArenaMsg::LeaseRing {
            request_id: dp::LeaseRequestId(1),
            edge_id: dp::EdgeId(7001),
            direction: dp::RingDirection::Ingress,
            ring_spec: ring_spec(),
        })
    );

    runtime
        .send_to(
            actor,
            dp::DataPlaneNodeMsg::Arena(dp::ArenaObservation::RingLeased {
                request_id: dp::LeaseRequestId(1),
                ring_id: dp::RingId(8001),
                layout: layout(),
            }),
        )
        .expect("ring leased");
    host.tick();

    assert_eq!(
        worker.try_recv(),
        Some(dp::DataPlaneWorkerMsg::InstallRing {
            edge_id: dp::EdgeId(7001),
            ring_id: dp::RingId(8001),
            direction: dp::RingDirection::Ingress,
            layout: layout(),
            object_spec: object_spec(),
            ring_spec: ring_spec(),
            role_port: dp::PortId("input".to_owned()),
        })
    );

    runtime
        .send_to(
            actor,
            dp::DataPlaneNodeMsg::Worker(dp::WorkerObservation::RingInstalled {
                edge_id: dp::EdgeId(7001),
                ring_id: dp::RingId(8001),
            }),
        )
        .expect("worker installed");
    host.tick();

    assert_eq!(
        transport.try_recv(),
        Some(dp::DataPlaneTransportMsg::EstablishRecv {
            edge_id: dp::EdgeId(7001),
            ring_id: dp::RingId(8001),
            layout: layout(),
        })
    );

    runtime
        .send_to(
            actor,
            dp::DataPlaneNodeMsg::Transport(dp::TransportObservation::EdgeReady {
                edge_id: dp::EdgeId(7001),
            }),
        )
        .expect("transport ready");
    host.tick();

    assert_eq!(
        reports.try_recv(),
        Some(dp::DataPlaneReportMsg::InboundEdgeReady {
            edge_id: dp::EdgeId(7001)
        })
    );
}

#[test]
fn object_loaded_observation_is_reported_as_coarse_data_plane_outcome() {
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let mut host = SingleThreadRuntime::new(parts);
    let arena = runtime
        .new_inbox::<dp::DataPlaneArenaMsg>()
        .expect("arena inbox");
    let worker = runtime
        .new_inbox::<dp::DataPlaneWorkerMsg>()
        .expect("worker inbox");
    let transport = runtime
        .new_inbox::<dp::DataPlaneTransportMsg>()
        .expect("transport inbox");
    let reports = runtime
        .new_inbox::<dp::DataPlaneReportMsg>()
        .expect("report inbox");
    let actor = runtime
        .spawn(dp::DataPlaneNodeActor::new(dp::NodeId(10)))
        .expect("spawn data-plane actor");

    runtime
        .send_to(
            actor,
            dp::DataPlaneNodeMsg::ConfigureRun(dp::DataPlaneRunConfig {
                run_id: dp::RunId(55),
                local_node_id: dp::NodeId(10),
                arena_actor: *arena.addr(),
                worker_actor: *worker.addr(),
                transport_actor: *transport.addr(),
                report_sink: *reports.addr(),
            }),
        )
        .expect("configure run");
    runtime
        .send_to(
            actor,
            dp::DataPlaneNodeMsg::Worker(dp::WorkerObservation::ObjectLoaded {
                edge_id: dp::EdgeId(7001),
                ring_id: dp::RingId(8001),
                object_id: object_record::ObjectId(9000),
                sequence: 7,
                extent: 16,
                handle: dp::DeviceHandle::new(dp::WorkerGeneration(3), 42),
            }),
        )
        .expect("object loaded");
    host.tick();

    assert_eq!(
        reports.try_recv(),
        Some(dp::DataPlaneReportMsg::ObjectLoaded {
            edge_id: dp::EdgeId(7001),
            object_id: object_record::ObjectId(9000),
            sequence: 7,
            extent: 16,
            handle: dp::DeviceHandle::new(dp::WorkerGeneration(3), 42),
        })
    );
}
