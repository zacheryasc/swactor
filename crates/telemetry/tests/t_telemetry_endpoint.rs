use std::time::Duration;

use telemetry::{
    ChannelContent, ChannelContentKind, ChannelFilter, ChannelId, TelemetryEndpoint,
    Lifetime, NodeId, Position, Record, SourceFilter, StreamId, SubscriptionRequest,
};
use telemetry::frame::{TelemetryEvent, FrameDelivery};
use serde_json::Value;
use swactor::actor::ActorAddress;
use swactor::stats::ActorSnapshot;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct RuntimeRecord {
    value: u64,
}

impl Record for RuntimeRecord {
    const CHANNEL: &'static str = "runtime.record";
}

fn stream() -> StreamId {
    StreamId::new(NodeId::new("node-endpoint"), Lifetime(7))
}

fn endpoint() -> TelemetryEndpoint {
    TelemetryEndpoint::with_capacity(stream(), 64, 8)
}

fn frame_event(event: &TelemetryEvent) -> &FrameDelivery {
    match event {
        TelemetryEvent::Frame(delivery) => delivery,
        other => panic!("expected frame event, got {other:?}"),
    }
}

#[test]
fn endpoint_without_subscribers_drains_to_bitbucket() {
    let endpoint = endpoint();
    let producer = endpoint.producer();
    let log = producer.register_channel("runtime.log", ChannelContent::TextStream);

    producer.submit_text(log, "before");
    let tick = endpoint.tick();

    assert_eq!(tick.drained, 1);
    assert_eq!(tick.subscribers, 0);
    assert_eq!(tick.delivered, 0);
    assert_eq!(endpoint.assigned(), 1);
    assert_eq!(endpoint.drained(), 1);
    assert_eq!(endpoint.bitbucketed(), 1);
}

#[test]
fn channel_registration_allocates_numeric_ids() {
    let endpoint = endpoint();

    let stdout = endpoint.register_channel("stdout", ChannelContent::TextStream);
    let stderr = endpoint.register_channel("stderr", ChannelContent::TextStream);
    let runtime = endpoint.register_record::<RuntimeRecord>();

    assert_eq!(stdout, ChannelId(1));
    assert_eq!(stderr, ChannelId(2));
    assert_eq!(runtime, ChannelId(3));
    let catalog = endpoint.catalog_snapshot();
    let removed_timing_name = ["telemetry", "frame_time"].join(".");
    assert!(
        !catalog
            .channels
            .values()
            .any(|descriptor| descriptor.name == removed_timing_name)
    );
}

#[test]
fn duplicate_channel_registration_rejects_conflicting_content() {
    let endpoint = endpoint();

    let first = endpoint.register_channel("stdout", ChannelContent::TextStream);
    let duplicate = endpoint.register_channel("stdout", ChannelContent::TextStream);
    let conflict = endpoint.try_register_channel(
        "stdout",
        ChannelContent::JsonRecord {
            schema: Some("stdout.json".to_owned()),
        },
    );

    assert_eq!(first, duplicate);
    assert!(conflict.is_err());
}

#[test]
fn subscription_snapshot_contains_stream_and_channel_metadata() {
    let endpoint = endpoint();
    let producer = endpoint.producer();
    let stdout = producer.register_channel("stdout", ChannelContent::TextStream);
    let runtime = producer.register_record::<RuntimeRecord>();

    let subscription = endpoint.subscribe_all("dashboard");

    assert_eq!(subscription.snapshot().streams.len(), 1);
    assert_eq!(subscription.snapshot().streams[0].stream, stream());
    assert!(
        subscription
            .snapshot()
            .channels
            .iter()
            .any(|descriptor| descriptor.id == stdout && descriptor.name == "stdout")
    );
    assert!(subscription
        .snapshot()
        .channels
        .iter()
        .any(|descriptor| descriptor.id == runtime && descriptor.name == RuntimeRecord::CHANNEL));
}

#[test]
fn subscription_receives_only_future_matching_frames() {
    let endpoint = endpoint();
    let producer = endpoint.producer();
    let log = producer.register_channel("runtime.log", ChannelContent::TextStream);

    producer.submit_text(log, "pre-subscription");
    endpoint.tick();

    let subscription = endpoint.subscribe_all("dashboard");
    producer.submit_text(log, "visible");
    endpoint.tick();

    let events = subscription.drain_available();
    assert_eq!(events.len(), 1);
    let delivery = frame_event(&events[0]);
    assert_eq!(delivery.channel.stream, stream());
    assert_eq!(delivery.channel.channel, log);
    assert_eq!(delivery.position, Position(1));
    assert_eq!(delivery.payload, b"visible");
}

#[test]
fn subscription_snapshot_filters_but_future_fanout_broadcasts() {
    let endpoint = endpoint();
    let producer = endpoint.producer();
    let stdout = producer.register_channel("stdout", ChannelContent::TextStream);
    let json = producer.register_record::<RuntimeRecord>();
    let text_subscription = endpoint.subscribe(
        "text",
        SubscriptionRequest {
            sources: SourceFilter::All,
            channels: ChannelFilter::Content(ChannelContentKind::TextStream),
        },
    );

    assert_eq!(text_subscription.snapshot().channels.len(), 1);
    assert_eq!(text_subscription.snapshot().channels[0].id, stdout);

    producer.submit_text(stdout, "line");
    producer.submit_record(json, &RuntimeRecord { value: 5 });
    endpoint.tick();

    let events = text_subscription.drain_available();
    assert_eq!(events.len(), 2);
    let channels: Vec<ChannelId> = events
        .iter()
        .map(|event| frame_event(event).channel.channel)
        .collect();
    assert_eq!(channels, vec![stdout, json]);
}

#[test]
fn endpoint_fans_out_ordered_frames_to_multiple_subscribers() {
    let endpoint = endpoint();
    let producer = endpoint.producer();
    let log = producer.register_channel("runtime.log", ChannelContent::TextStream);
    let left = endpoint.subscribe_all("left");
    let right = endpoint.subscribe_all("right");

    for n in 0..3 {
        producer.submit_text(log, format!("line-{n}"));
    }
    let tick = endpoint.tick();

    assert_eq!(tick.drained, 3);
    assert_eq!(tick.subscribers, 2);
    assert_eq!(tick.delivered, 6);
    assert_eq!(positions(&left.drain_available()), vec![0, 1, 2]);
    assert_eq!(positions(&right.drain_available()), vec![0, 1, 2]);
}

#[test]
fn slow_subscriber_drops_without_blocking_fast_subscriber() {
    let endpoint = endpoint();
    let producer = endpoint.producer();
    let log = producer.register_channel("runtime.log", ChannelContent::TextStream);
    let slow = endpoint.subscribe_all_with_capacity("slow", 1);
    let fast = endpoint.subscribe_all_with_capacity("fast", 8);

    for n in 0..4 {
        producer.submit_text(log, format!("line-{n}"));
    }
    let tick = endpoint.tick();

    assert_eq!(tick.drained, 4);
    assert_eq!(tick.delivered, 5);
    assert_eq!(tick.dropped_for_subscribers, 3);
    assert_eq!(positions(&slow.drain_available()), vec![0]);
    assert_eq!(positions(&fast.drain_available()), vec![0, 1, 2, 3]);
    let slow_snapshot = endpoint
        .subscriber_snapshots()
        .into_iter()
        .find(|snapshot| snapshot.name == "slow")
        .expect("slow subscriber snapshot");
    assert_eq!(slow_snapshot.dropped, 3);
}

#[test]
fn channel_declared_is_broadcast_to_filtered_subscribers() {
    let endpoint = endpoint();
    let subscription = endpoint.subscribe(
        "text-only",
        SubscriptionRequest {
            sources: SourceFilter::All,
            channels: ChannelFilter::Content(ChannelContentKind::TextStream),
        },
    );

    let channel = endpoint.register_channel(
        "runtime.json",
        ChannelContent::JsonRecord {
            schema: Some("runtime.json".to_owned()),
        },
    );

    let events = subscription.drain_available();
    assert_eq!(events.len(), 1);
    match &events[0] {
        TelemetryEvent::ChannelDeclared(descriptor) => {
            assert_eq!(descriptor.id, channel);
            assert_eq!(descriptor.name, "runtime.json");
        }
        other => panic!("expected channel declaration, got {other:?}"),
    }
}

#[test]
fn submit_text_owned_queues_owned_string() {
    let endpoint = endpoint();
    let producer = endpoint.producer();
    let log = producer.register_channel("runtime.log", ChannelContent::TextStream);
    let sub = endpoint.subscribe_all("test");

    assert!(producer.submit_text_owned(log, String::from("hello")));
    let tick = endpoint.tick();

    assert_eq!(tick.drained, 1);
    assert_eq!(tick.delivered, 1);
    let event = sub.recv_timeout(Duration::from_millis(50)).unwrap();
    let delivery = frame_event(&event);
    assert_eq!(delivery.channel.channel, log);
    assert_eq!(delivery.payload, b"hello");
}

#[test]
fn process_observer_adapter_submits_configured_channels() {
    let endpoint = endpoint();
    let producer = endpoint.producer();
    let stdout = producer.register_channel("proc.trainer.stdout", ChannelContent::TextStream);
    let stderr = producer.register_channel("proc.trainer.stderr", ChannelContent::TextStream);
    let observer = producer.process_observer_with(
        move |_label, is_stderr| {
            if is_stderr { stderr } else { stdout }
        },
    );
    let sub = endpoint.subscribe_all("test");

    observer.on_output("trainer", false, b"hello\n");
    observer.on_output("trainer", true, b"warn\n");
    endpoint.tick();

    let events = sub.drain_available();
    assert_eq!(events.len(), 2);
    assert_eq!(frame_event(&events[0]).channel.channel, stdout);
    assert_eq!(frame_event(&events[0]).payload, b"hello\n");
    assert_eq!(frame_event(&events[1]).channel.channel, stderr);
    assert_eq!(frame_event(&events[1]).payload, b"warn\n");
}

#[test]
fn stats_hook_adapter_submits_worker_snapshot_json() {
    let endpoint = endpoint();
    let producer = endpoint.producer();
    let runtime = producer.register_channel(
        "runtime.actors",
        ChannelContent::JsonRecord {
            schema: Some("runtime.actors".to_owned()),
        },
    );
    let hook = producer.stats_hook_on(runtime);
    let sub = endpoint.subscribe_all("test");
    let actor = ActorAddress::new_random();
    let snapshots = [ActorSnapshot {
        address: actor,
        mailbox_depth: 3,
        last_msg_type: Some("Ping"),
        actor_type: Some("TestActor"),
        message_type: Some("Ping"),
        messages_processed: 5,
        poisoned: false,
        message_type_counts: vec![("Ping", 5)],
    }];

    hook.on_tick(2, &snapshots);
    endpoint.tick();

    let event = sub.recv_timeout(Duration::from_millis(50)).unwrap();
    let delivery = frame_event(&event);
    assert_eq!(delivery.channel.channel, runtime);
    let json: Value = serde_json::from_slice(&delivery.payload).unwrap();
    assert_eq!(json["worker_id"], 2);
    assert_eq!(json["actors"][0]["address"], actor.to_full_hex());
    assert_eq!(json["actors"][0]["mailbox_depth"], 3);
    assert_eq!(json["actors"][0]["last_msg_type"], "Ping");
    assert_eq!(json["actors"][0]["messages_processed"], 5);
    assert_eq!(json["actors"][0]["message_type_counts"][0]["ty"], "Ping");
    assert_eq!(json["actors"][0]["actor_type"], "TestActor");
    assert_eq!(json["actors"][0]["message_type"], "Ping");
}

fn positions(events: &[TelemetryEvent]) -> Vec<u64> {
    let mut positions: Vec<u64> = events
        .iter()
        .map(|event| frame_event(event).position.0)
        .collect();
    positions.sort_unstable();
    positions
}
