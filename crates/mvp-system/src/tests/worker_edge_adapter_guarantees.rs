use mvp_system::actors::node_agent::{
    NodeAgentActor, NodeAgentMsg, NodeAgentReport, StageCommandWire, StageEdgeKindWire,
    StageInboundEdgeWire, StageObjectSpecWire, StageOutboundEdgeWire, StageProvisionWire,
    StageRingSpecWire,
};
use mvp_system::gpu_worker_ingress_parser as ingress;
use mvp_system::run_plan::{self, GgufSource, TokenizerSource};
use mvp_system::stage_controller as stage;
use swactor::actor::ActorAddress;
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;

fn inbound_edge() -> StageInboundEdgeWire {
    StageInboundEdgeWire {
        edge_id: 7001,
        kind: StageEdgeKindWire::TokenIn,
        object_spec: StageObjectSpecWire {
            max_extent: 2048,
            alignment: 4,
        },
        ring_spec: StageRingSpecWire {
            data_capacity: 2088,
            alignment: 64,
        },
    }
}

fn outbound_edge() -> StageOutboundEdgeWire {
    StageOutboundEdgeWire {
        edge_id: 7002,
        kind: StageEdgeKindWire::Activation,
        consumer_node_id: 43,
        consumer_endpoint: None,
        object_spec: StageObjectSpecWire {
            max_extent: 589_824,
            alignment: 128,
        },
        ring_spec: StageRingSpecWire {
            data_capacity: 589_864,
            alignment: 256,
        },
    }
}

fn provision_wire() -> StageProvisionWire {
    StageProvisionWire {
        run_id: 55,
        authorized_orchestrator: 9,
        node_id: 11,
        stage_index: 1,
        stage_count: 3,
        layer_start: 10,
        layer_end_exclusive: 20,
        inbound_edge_id: 7001,
        outbound_edge_id: 7002,
        inbound_edge: Some(inbound_edge()),
        outbound_edge: Some(outbound_edge()),
        model_id: "smollm2-135m-q4".to_owned(),
        gguf_source: GgufSource::LocalPath("/models/smollm.gguf".to_owned()),
        tokenizer: TokenizerSource::EmbeddedGguf,
        stage_shard_plan: None,
    }
}

fn plan_fixture(stage_count: u32) -> run_plan::RunPlan {
    let placements = (0..stage_count)
        .map(|stage_index| run_plan::StagePlacement {
            stage_index,
            node_id: run_plan::NodeId(11 + u64::from(stage_index)),
        })
        .collect::<Vec<_>>();
    let candidate_pool = placements
        .iter()
        .map(|placement| placement.node_id)
        .collect::<Vec<_>>();

    run_plan::plan_run(run_plan::PlannerInput {
        run_id: run_plan::RunId(55),
        orchestrator_node_id: run_plan::NodeId(1),
        model: run_plan::ModelFacts {
            model_id: "fixture-model".to_owned(),
            gguf_source: GgufSource::LocalPath("/models/fixture.gguf".to_owned()),
            num_layers: 7,
            hidden_dim: 13,
            dtype_family: run_plan::DTypeFamily::BFloat,
            dtype_width_bytes: 2,
            max_seq_len: 32,
            eos_token_id: 2,
            tokenizer: TokenizerSource::EmbeddedGguf,
        },
        runtime: run_plan::RuntimeConfig::test_default(),
        candidate_pool,
        stage_count,
        placement: run_plan::PlacementInput::FixedLinear(placements),
        activation_ring: run_plan::RingSpec {
            data_capacity: 4096,
            alignment: 64,
            direction: run_plan::RingDirection::Egress,
            host_pinning: run_plan::HostPinning::Pageable,
            wake_coalescing: run_plan::WakeCoalescing::PendingBit,
        },
        token_ring: run_plan::RingSpec {
            data_capacity: 4096,
            alignment: 64,
            direction: run_plan::RingDirection::Egress,
            host_pinning: run_plan::HostPinning::Pageable,
            wake_coalescing: run_plan::WakeCoalescing::PendingBit,
        },
    })
    .expect("plan fixture builds")
}

fn provision_wire_from_plan(plan: &run_plan::RunPlan, stage_index: u32) -> StageProvisionWire {
    let provision = run_plan::derive_stage_provision(plan, stage_index)
        .expect("stage provision derives from plan");
    StageProvisionWire {
        run_id: provision.run_id.0,
        authorized_orchestrator: 9,
        node_id: provision.node_id.0,
        stage_index: provision.stage_index,
        stage_count: provision.stage_count,
        layer_start: provision.layer_start,
        layer_end_exclusive: provision.layer_end_exclusive,
        inbound_edge_id: provision.inbound.edge_id.0,
        outbound_edge_id: provision.outbound.edge_id.0,
        inbound_edge: Some(StageInboundEdgeWire {
            edge_id: provision.inbound.edge_id.0,
            kind: edge_kind_wire(provision.inbound.kind),
            object_spec: object_spec_wire(provision.inbound.object_spec),
            ring_spec: ring_spec_wire(provision.inbound.ring_spec),
        }),
        outbound_edge: Some(StageOutboundEdgeWire {
            edge_id: provision.outbound.edge_id.0,
            kind: edge_kind_wire(provision.outbound.kind),
            consumer_node_id: provision.outbound.consumer_node_id.0,
            consumer_endpoint: None,
            object_spec: object_spec_wire(provision.outbound.object_spec),
            ring_spec: ring_spec_wire(provision.outbound.ring_spec),
        }),
        model_id: provision.model.model_id,
        gguf_source: provision.gguf_source,
        tokenizer: provision.tokenizer,
        stage_shard_plan: None,
    }
}

fn edge_kind_wire(kind: run_plan::EdgeKind) -> StageEdgeKindWire {
    match kind {
        run_plan::EdgeKind::TokenIn => StageEdgeKindWire::TokenIn,
        run_plan::EdgeKind::Activation => StageEdgeKindWire::Activation,
        run_plan::EdgeKind::TokenOut => StageEdgeKindWire::TokenOut,
    }
}

fn object_spec_wire(spec: run_plan::ObjectSpec) -> StageObjectSpecWire {
    StageObjectSpecWire {
        max_extent: spec.max_extent,
        alignment: spec.alignment,
    }
}

fn ring_spec_wire(spec: run_plan::RingSpec) -> StageRingSpecWire {
    StageRingSpecWire {
        data_capacity: spec.data_capacity,
        alignment: spec.alignment,
    }
}

fn token_spec() -> ingress::ObjectSpec {
    ingress::ObjectSpec {
        max_extent: 16,
        alignment: 4,
        layout: ingress::ObjectLayout::Token,
    }
}

fn record(sequence: u64, extent: u64) -> Vec<u8> {
    ingress::ObjectRecordBuilder::new(token_spec())
        .object_id(ingress::ObjectId(9000 + sequence))
        .sequence(sequence)
        .extent(extent)
        .payload(vec![sequence as u8; extent as usize])
        .encode()
}

#[test]
fn stage_provision_wire_round_trips_edge_object_and_ring_facts() {
    let wire = provision_wire();

    let encoded = serde_json::to_string(&NodeAgentMsg::ProvisionStage(wire.clone()))
        .expect("serialize provision wire");
    let decoded: NodeAgentMsg = serde_json::from_str(&encoded).expect("deserialize provision wire");

    let NodeAgentMsg::ProvisionStage(decoded) = decoded else {
        panic!("decoded message must remain a provision");
    };
    assert_eq!(decoded.inbound_edge, Some(inbound_edge()));
    assert_eq!(decoded.outbound_edge, Some(outbound_edge()));
}

#[test]
fn node_agent_establish_edge_commands_preserve_provisioned_runtime_facts() {
    let runtime = Runtime::new(RuntimeConfig::default());
    let reports = runtime
        .new_inbox::<NodeAgentReport>()
        .expect("node report inbox");
    let actor = runtime
        .spawn(NodeAgentActor::new(
            stage::NodeId(11),
            ActorAddress::new_random(),
            Some(*reports.addr()),
        ))
        .expect("spawn node agent");

    runtime
        .send_to(actor, NodeAgentMsg::ProvisionStage(provision_wire()))
        .expect("send provision");
    runtime.tick();

    let commands = [
        reports.try_recv().expect("inbound command"),
        reports.try_recv().expect("outbound command"),
        reports.try_recv().expect("configure command"),
        reports.try_recv().expect("load command"),
    ];

    assert!(commands.iter().any(|report| {
        matches!(
            report,
            NodeAgentReport::Command(StageCommandWire::EstablishInboundEdge { edge_id: 7001, edge })
                if edge == &inbound_edge()
        )
    }));
    assert!(commands.iter().any(|report| {
        matches!(
            report,
            NodeAgentReport::Command(StageCommandWire::EstablishOutboundEdge { edge_id: 7002, edge })
                if edge == &outbound_edge()
        )
    }));
}

#[test]
fn node_agent_commands_preserve_plan_derived_single_stage_edge_contract() {
    let plan = plan_fixture(1);
    let wire = provision_wire_from_plan(&plan, 0);
    assert_eq!(wire.stage_count, 1);
    assert_eq!(wire.layer_start, 0);
    assert_eq!(wire.layer_end_exclusive, plan.model.num_layers);
    assert_eq!(
        wire.inbound_edge.as_ref().expect("inbound edge").kind,
        StageEdgeKindWire::TokenIn
    );
    assert_eq!(
        wire.outbound_edge.as_ref().expect("outbound edge").kind,
        StageEdgeKindWire::TokenOut
    );

    let runtime = Runtime::new(RuntimeConfig::default());
    let reports = runtime
        .new_inbox::<NodeAgentReport>()
        .expect("node report inbox");
    let actor = runtime
        .spawn(NodeAgentActor::new(
            stage::NodeId(wire.node_id),
            ActorAddress::new_random(),
            Some(*reports.addr()),
        ))
        .expect("spawn node agent");

    runtime
        .send_to(actor, NodeAgentMsg::ProvisionStage(wire.clone()))
        .expect("send provision");
    runtime.tick();

    let commands = [
        reports.try_recv().expect("inbound command"),
        reports.try_recv().expect("outbound command"),
        reports.try_recv().expect("configure command"),
        reports.try_recv().expect("load command"),
    ];
    assert!(commands.iter().any(|report| {
        matches!(
            report,
            NodeAgentReport::Command(StageCommandWire::EstablishInboundEdge { edge_id, edge })
                if *edge_id == wire.inbound_edge_id
                    && edge.kind == StageEdgeKindWire::TokenIn
                    && edge.object_spec.max_extent == 128
                    && edge.object_spec.alignment == 4
        )
    }));
    assert!(commands.iter().any(|report| {
        matches!(
            report,
            NodeAgentReport::Command(StageCommandWire::EstablishOutboundEdge { edge_id, edge })
                if *edge_id == wire.outbound_edge_id
                    && edge.kind == StageEdgeKindWire::TokenOut
                    && edge.object_spec.max_extent == 128
                    && edge.object_spec.alignment == 4
        )
    }));
}

#[test]
fn mo01_reader_uses_total_len_to_split_records_and_faults_truncated_eof() {
    let first = record(0, 8);
    let second = record(1, 4);
    let mut joined = first.clone();
    joined.extend_from_slice(&second);

    let parsed = ingress::read_object_record(&joined, token_spec(), false)
        .expect("joined stream starts with a complete record");
    let ingress::ObjectRecordRead::Complete(first_record) = parsed else {
        panic!("first record must be complete");
    };
    assert_eq!(first_record.object_id, ingress::ObjectId(9000));
    assert_eq!(first_record.sequence, 0);
    assert_eq!(first_record.extent, 8);
    assert_eq!(first_record.total_len, first.len());

    let parsed_second =
        ingress::read_object_record(&joined[first_record.total_len..], token_spec(), true)
            .expect("split cursor points at the next complete record");
    let ingress::ObjectRecordRead::Complete(second_record) = parsed_second else {
        panic!("second record must be complete");
    };
    assert_eq!(second_record.object_id, ingress::ObjectId(9001));
    assert_eq!(second_record.sequence, 1);
    assert_eq!(second_record.extent, 4);
    assert_eq!(second_record.total_len, second.len());

    assert_eq!(
        ingress::read_object_record(&joined[..ingress::HEADER_LEN - 1], token_spec(), false),
        Ok(ingress::ObjectRecordRead::Incomplete)
    );
    assert_eq!(
        ingress::read_object_record(&joined[..ingress::HEADER_LEN - 1], token_spec(), true),
        Err(ingress::ObjectFailureReason::EofBeforeFullPayload)
    );
}
