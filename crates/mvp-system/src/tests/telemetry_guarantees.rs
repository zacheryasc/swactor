use datastream::{ChannelKind, Record};
use mvp_system::observability_surface as obs;
use mvp_system::telemetry::{self, MvpLifecycleRecord};

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
