use datastream::{ChannelId, ChannelKind, Frame, Lifetime, Record, StreamId};
use mvp_system::observability::frame_archive::FrameArchive;
use mvp_system::observability::observability_surface as obs;
use mvp_system::observability::telemetry::{
    self, MvpLifecycleRecord, MvpProvisionEventRecord, MvpProvisionLogRecord,
};
use mvp_system::orchestration::provisioning::{
    ProvisionEvent, ProvisionEventKind, ProvisionLogLine, ProvisionLogStream,
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
    assert_eq!(MvpLifecycleRecord::channel_name(), "mvp.lifecycle");
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
        registry.classify_name(MvpLifecycleRecord::channel_name()),
        ChannelKind::Typed
    );
}

#[test]
fn provisioning_records_round_trip_on_owned_datastream_channels() {
    let event = MvpProvisionEventRecord::new(ProvisionEvent {
        run_id: 77,
        node_id: 11,
        kind: ProvisionEventKind::NodeLive,
        provider: Some("docker".to_owned()),
        message: None,
    });
    let event_without_provider = MvpProvisionEventRecord::new(ProvisionEvent {
        run_id: 77,
        node_id: 12,
        kind: ProvisionEventKind::ProvisionStart,
        provider: None,
        message: Some("queued".to_owned()),
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
        MvpProvisionEventRecord::decode(&event_without_provider.encode()).unwrap(),
        event_without_provider
    );
    assert_eq!(
        telemetry::mvp_provision_log_channel(11, ProvisionLogStream::Stdout).as_str(),
        "mvp.provisioning.logs.node.11.stdout"
    );
}

#[test]
fn mvp_channel_registry_marks_provisioning_payloads_as_typed() {
    let registry = telemetry::channel_registry();

    assert_eq!(
        registry.classify_name(MvpProvisionEventRecord::channel_name()),
        ChannelKind::Typed
    );
    assert_eq!(
        registry.classify_name(MvpProvisionLogRecord::channel_name()),
        ChannelKind::Typed
    );
}

#[test]
fn frame_archive_writes_jsonl_records_for_text_and_binary_payloads() {
    static NEXT_TEMP_FILE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    let suffix = NEXT_TEMP_FILE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "mvp-observability-frame-archive-test-{}-{suffix}.jsonl",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);

    let stream = StreamId::new("test-node", Lifetime(42));
    let mut archive = FrameArchive::open(&path).expect("frame archive opens");
    archive
        .record(
            "orchestrator",
            &stream,
            "stdout",
            &Frame::new(
                ChannelId(1),
                datastream::Position(7),
                b"hello \xce\xbb".to_vec(),
            ),
        )
        .expect("text frame archives");
    archive
        .record(
            "orchestrator",
            &stream,
            "stderr",
            &Frame::new(
                ChannelId(2),
                datastream::Position(8),
                vec![0xff, 0x00, b'A'],
            ),
        )
        .expect("binary frame archives");
    drop(archive);

    let contents = std::fs::read_to_string(&path).expect("read frame archive jsonl");
    let records = contents
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("archive line is json"))
        .collect::<Vec<_>>();
    let _ = std::fs::remove_file(&path);

    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["arrival_seq"], serde_json::json!(0));
    assert!(
        records[0]["arrival_unix_ms"]
            .as_u64()
            .is_some_and(|value| value > 0)
    );
    assert_eq!(records[0]["source"], serde_json::json!("orchestrator"));
    assert_eq!(records[0]["stream"], serde_json::json!("test-node#42"));
    assert_eq!(records[0]["channel"], serde_json::json!("stdout"));
    assert_eq!(records[0]["channel_id"], serde_json::json!(1));
    assert_eq!(records[0]["position"], serde_json::json!(7));
    assert_eq!(
        records[0]["payload"],
        serde_json::json!({"encoding": "utf8", "value": "hello λ"})
    );

    assert_eq!(records[1]["arrival_seq"], serde_json::json!(1));
    assert_eq!(records[1]["source"], serde_json::json!("orchestrator"));
    assert_eq!(records[1]["stream"], serde_json::json!("test-node#42"));
    assert_eq!(records[1]["channel"], serde_json::json!("stderr"));
    assert_eq!(records[1]["channel_id"], serde_json::json!(2));
    assert_eq!(records[1]["position"], serde_json::json!(8));
    assert_eq!(
        records[1]["payload"],
        serde_json::json!({"encoding": "bytes", "value": [255, 0, 65]})
    );
}
