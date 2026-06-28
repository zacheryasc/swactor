use datastream::{ChannelKind, Record};
use mvp_system::observability_surface as obs;
use mvp_system::provisioning::{
    ProvisionEvent, ProvisionEventKind, ProvisionLogLine, ProvisionLogStream,
};
use mvp_system::telemetry::{
    self, MvpLifecycleRecord, MvpProvisionEventRecord, MvpProvisionLogRecord,
};

#[test]
fn mvp_lifecycle_record_round_trips_on_owned_datastream_channel() {
    let record = MvpLifecycleRecord::new(obs::Event::StageScoped {
        kind: obs::EventKind::StageFaulted,
        run_id: obs::RunId(7),
        stage_index: obs::StageIndex(2),
        reason: Some(obs::FaultReason::WorkerCrashed),
        component: obs::Component::StageController,
    });

    assert_eq!(MvpLifecycleRecord::CHANNEL, telemetry::MVP_LIFECYCLE);
    assert_eq!(MvpLifecycleRecord::channel().as_str(), "mvp.lifecycle");
    assert_eq!(record.kind(), obs::EventKind::StageFaulted);
    assert_eq!(
        MvpLifecycleRecord::decode(&record.encode()).unwrap(),
        record
    );
}

#[test]
fn mvp_channel_registry_marks_lifecycle_payloads_as_typed() {
    let registry = telemetry::channel_registry();

    assert_eq!(
        registry.classify_channel(&MvpLifecycleRecord::channel()),
        ChannelKind::Typed
    );
}

#[test]
fn provisioning_records_round_trip_on_owned_datastream_channels() {
    let event = MvpProvisionEventRecord::new(ProvisionEvent {
        run_id: 77,
        node_id: 11,
        kind: ProvisionEventKind::NodeLive,
        message: None,
    });
    let log = MvpProvisionLogRecord::new(ProvisionLogLine {
        run_id: 77,
        node_id: 11,
        stream: ProvisionLogStream::Stdout,
        line: "{\"type\":\"ready\"}".to_owned(),
    });

    assert_eq!(
        MvpProvisionEventRecord::CHANNEL,
        telemetry::MVP_PROVISIONING_EVENTS
    );
    assert_eq!(
        MvpProvisionLogRecord::CHANNEL,
        telemetry::MVP_PROVISIONING_LOGS
    );
    assert_eq!(
        MvpProvisionEventRecord::decode(&event.encode()).unwrap(),
        event
    );
    assert_eq!(MvpProvisionLogRecord::decode(&log.encode()).unwrap(), log);
    assert_eq!(
        telemetry::mvp_provision_log_channel(11, ProvisionLogStream::Stdout).as_str(),
        "mvp.provisioning.logs.node.11.stdout"
    );
}

#[test]
fn mvp_channel_registry_marks_provisioning_payloads_as_typed() {
    let registry = telemetry::channel_registry();

    assert_eq!(
        registry.classify_channel(&MvpProvisionEventRecord::channel()),
        ChannelKind::Typed
    );
    assert_eq!(
        registry.classify_channel(&MvpProvisionLogRecord::channel()),
        ChannelKind::Typed
    );
}
