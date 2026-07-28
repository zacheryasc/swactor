use data_plane::actor as dp;
use mvp_system::node::actor::NodeAgentMsg;
use mvp_system::node::data_plane_bridge::MvpDataPlaneReportSinkActor;
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;

fn spawn_bridge(
    runtime: &Runtime,
) -> (
    swactor::actor::ActorAddress,
    swactor::runtime::Inbox<NodeAgentMsg>,
) {
    let node_messages = runtime
        .new_inbox::<NodeAgentMsg>()
        .expect("node-agent inbox");
    let bridge = runtime
        .spawn(MvpDataPlaneReportSinkActor::new(*node_messages.addr()))
        .expect("spawn bridge");
    (bridge, node_messages)
}

#[test]
fn data_plane_reports_drive_mvp_stage_readiness_and_object_visibility() {
    let runtime = Runtime::new(RuntimeConfig::default());
    let (bridge, node_messages) = spawn_bridge(&runtime);

    runtime
        .send_to(
            bridge,
            dp::DataPlaneReportMsg::InboundEdgeReady {
                edge_id: dp::EdgeId(7001),
            },
        )
        .expect("send inbound ready");
    runtime
        .send_to(
            bridge,
            dp::DataPlaneReportMsg::ObjectLoaded {
                edge_id: dp::EdgeId(7001),
                object_id: data_plane::object_record::ObjectId(9000),
                sequence: 3,
                extent: 8,
                handle: dp::DeviceHandle::new(dp::WorkerGeneration(2), 44),
            },
        )
        .expect("send object loaded");
    runtime.tick();

    assert_eq!(
        node_messages.try_recv(),
        Some(NodeAgentMsg::MarkInboundEdgeReady { edge_id: 7001 })
    );
    assert_eq!(
        node_messages.try_recv(),
        Some(NodeAgentMsg::ObjectLoaded {
            edge_id: 7001,
            object_id: 9000,
            sequence: 3,
            handle_generation: 2,
            handle_id: 44,
        })
    );
}

#[test]
fn data_plane_fault_and_stop_reports_map_to_mvp_lifecycle_messages() {
    let runtime = Runtime::new(RuntimeConfig::default());
    let (bridge, node_messages) = spawn_bridge(&runtime);

    runtime
        .send_to(
            bridge,
            dp::DataPlaneReportMsg::EdgeFaulted {
                edge_id: dp::EdgeId(7001),
                reason: dp::EdgeFaultReason::StreamFault(
                    data_plane::edge_lifecycle::StreamFaultReason::ProtocolError,
                ),
            },
        )
        .expect("send edge fault");
    runtime
        .send_to(
            bridge,
            dp::DataPlaneReportMsg::LocalEdgesStopped {
                run_id: dp::RunId(55),
            },
        )
        .expect("send stopped");
    runtime.tick();

    assert_eq!(node_messages.try_recv(), Some(NodeAgentMsg::WorkerCrashed));
    assert_eq!(
        node_messages.try_recv(),
        Some(NodeAgentMsg::LocalEdgesStopped { run_id: 55 })
    );
}
