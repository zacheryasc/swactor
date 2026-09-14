use std::time::Duration;

use serde_json::Value;
use swactor::actor::ActorAddress;
use swactor::stats::{ActorSnapshot, StatsSnapshotKind};
use telemetry::frame::{FrameDelivery, TelemetryEvent};
use telemetry::{
    ChannelContent, ChannelContentKind, ChannelFilter, ChannelId, Lifetime, NodeId, Record,
    SourceFilter, StreamId, SubscriptionRequest, TelemetryEndpoint, decode_record_value,
};

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
    assert!(subscription.snapshot().channels.iter().any(|descriptor| {
        descriptor.id == runtime
            && descriptor.name == RuntimeRecord::CHANNEL
            && descriptor.content
                == ChannelContent::MessagePackRecord {
                    schema: Some(RuntimeRecord::CHANNEL.to_owned()),
                }
    }));
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
    let record = events
        .iter()
        .map(frame_event)
        .find(|delivery| delivery.channel.channel == json)
        .expect("record frame");
    assert_eq!(
        RuntimeRecord::decode(&record.payload).unwrap(),
        RuntimeRecord { value: 5 }
    );
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
    let left_positions = positions(&left.drain_available());
    let right_positions = positions(&right.drain_available());
    assert_eq!(left_positions, right_positions);
    assert_eq!(left_positions.len(), 3);
    assert!(left_positions.windows(2).all(|pair| pair[1] == pair[0] + 1));
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
fn stats_hook_adapter_submits_worker_snapshot_messagepack() {
    let endpoint = endpoint();
    let producer = endpoint.producer();
    let runtime = producer.register_channel(
        "runtime.actors",
        ChannelContent::MessagePackRecord {
            schema: Some("runtime.actors".to_owned()),
        },
    );
    let hook = producer.stats_hook_on(runtime);
    let sub = endpoint.subscribe_all("test");
    let actor = ActorAddress::new_random();
    let snapshots = [ActorSnapshot {
        address: actor,
        mailbox_depth: 3,
        mailbox_max_depth: 4,
        last_msg_type: Some("Ping"),
        actor_type: Some("TestActor"),
        message_type: Some("Ping"),
        messages_processed: 5,
        poisoned: false,
        message_type_counts: vec![("Ping", 5)],
    }];

    hook.on_snapshot(2, &snapshots, StatsSnapshotKind::CENSUS);
    endpoint.tick();

    let events = sub.drain_available();
    assert_eq!(events.len(), 2, "started vital plus recovery census");
    let delivery = events
        .iter()
        .map(frame_event)
        .find(|delivery| {
            decode_record_value(&delivery.payload).is_ok_and(|value| value["kind"] == "census")
        })
        .expect("census frame");
    assert_eq!(delivery.channel.channel, runtime);
    let json: Value = decode_record_value(&delivery.payload).unwrap();
    assert_eq!(json["worker_id"], 2);
    assert_eq!(json["generation"], 7);
    assert_eq!(json["actors"][0]["address"], actor.to_full_hex());
    assert_eq!(json["actors"][0]["mailbox_depth"], 3);
    assert_eq!(json["actors"][0]["last_msg_type"], "Ping");
    assert_eq!(json["actors"][0]["messages_processed"], 5);
    assert_eq!(json["actors"][0]["message_type_counts"][0]["ty"], "Ping");
    assert_eq!(json["actors"][0]["actor_type"], "TestActor");
    assert_eq!(json["actors"][0]["message_type"], "Ping");
}

#[test]
fn retained_subscription_recovers_actor_startup_before_live_activity() {
    let endpoint =
        TelemetryEndpoint::with_capacity(stream(), 64, 1).with_retention(32, 1024 * 1024);
    let producer = endpoint.producer();
    let hook = producer.stats_hook();
    let mut snapshots = (1..=13)
        .map(|index| ActorSnapshot {
            address: ActorAddress([index; 32]),
            mailbox_depth: 0,
            mailbox_max_depth: 0,
            last_msg_type: Some("Ping"),
            actor_type: Some("StartupActor"),
            message_type: Some("Ping"),
            messages_processed: 1,
            poisoned: false,
            message_type_counts: vec![("Ping", 1)],
        })
        .collect::<Vec<_>>();
    hook.on_snapshot(0, &snapshots, StatsSnapshotKind::CENSUS);
    endpoint.tick();

    // The node has already ticked before runtimeReady makes it discoverable.
    // Fourteen startup records must not compete with the single live queue slot.
    let subscription = endpoint.subscribe_retained("late-supervisor", SubscriptionRequest::all());
    for snapshot in &mut snapshots {
        snapshot.messages_processed += 1;
    }
    hook.on_snapshot(0, &snapshots, StatsSnapshotKind::ACTIVITY);
    endpoint.tick();

    let events = subscription.drain_available();
    let records = events
        .iter()
        .map(frame_event)
        .map(|frame| decode_record_value(&frame.payload).expect("actor record"))
        .collect::<Vec<_>>();
    assert_eq!(
        records
            .iter()
            .map(|record| record["sequence"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        (1..=15).collect::<Vec<_>>()
    );
    assert!(records.iter().all(|record| record["generation"] == 7));
    assert_eq!(records[0]["event"], "started");
    assert_eq!(records[13]["kind"], "census");
    assert_eq!(records[14]["kind"], "activity");
    assert_eq!(
        events
            .iter()
            .map(|event| frame_event(event).position.0)
            .collect::<Vec<_>>(),
        (0..15).collect::<Vec<_>>()
    );
    assert_eq!(endpoint.bitbucketed(), 0);
    drop(subscription);
    assert_eq!(endpoint.subscriber_count(), 0);
}

#[test]
fn retained_subscription_bounds_leave_original_positions_visible() {
    let endpoint = TelemetryEndpoint::with_capacity(stream(), 8, 1).with_retention(2, 5);
    let producer = endpoint.producer();
    let channel = producer.register_channel("bounded", ChannelContent::Bytes);
    for value in ["a", "b", "c"] {
        producer.submit_text(channel, value);
    }
    endpoint.tick();
    let first = endpoint.subscribe_retained("frame-bound", SubscriptionRequest::all());
    assert_eq!(positions(&first.drain_available()), vec![1, 2]);
    drop(first);

    producer.submit_text(channel, "12345");
    endpoint.tick();
    let second = endpoint.subscribe_retained("byte-bound", SubscriptionRequest::all());
    let events = second.drain_available();
    assert_eq!(positions(&events), vec![3]);
    assert_eq!(frame_event(&events[0]).payload, b"12345");
    drop(second);

    producer.submit_text(channel, "oversize");
    producer.submit_text(channel, "d");
    endpoint.tick();
    let third = endpoint.subscribe_retained("after-overflow", SubscriptionRequest::all());
    let events = third.drain_available();
    assert_eq!(positions(&events), vec![5]);
    assert_eq!(frame_event(&events[0]).payload, b"d");
}

fn positions(events: &[TelemetryEvent]) -> Vec<u64> {
    let mut positions: Vec<u64> = events
        .iter()
        .map(|event| frame_event(event).position.0)
        .collect();
    positions.sort_unstable();
    positions
}
