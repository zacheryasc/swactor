use std::time::Duration;

use datastream::{
    ChannelId, DatastreamEndpoint, DeliveryFanout, FRAME_TIME_CHANNEL, Frame, FrameTimeSample,
    Lifetime, NodeId, Position, Record, StreamId,
};
use serde_json::Value;
use swactor::actor::ActorAddress;
use swactor::stats::ActorSnapshot;

fn stream() -> StreamId {
    StreamId::new(NodeId::new("node-endpoint"), Lifetime(7))
}

#[test]
fn endpoint_without_subscribers_drains_to_bitbucket() {
    let endpoint = DatastreamEndpoint::with_capacity(stream(), 8, 4);
    let producer = endpoint.producer();
    producer.set_frame_timing_enabled(false);

    producer.submit_text("runtime.log", "before");
    let tick = endpoint.tick();

    assert_eq!(tick.drained, 1);
    assert_eq!(tick.subscribers, 0);
    assert_eq!(tick.delivered, 0);
    assert_eq!(endpoint.assigned(), 1);
    assert_eq!(endpoint.drained(), 1);
    assert_eq!(endpoint.bitbucketed(), 1);
}

#[test]
fn subscription_receives_only_future_frames() {
    let endpoint = DatastreamEndpoint::with_capacity(stream(), 8, 4);
    let producer = endpoint.producer();
    producer.set_frame_timing_enabled(false);

    producer.submit_text("runtime.log", "pre-subscription");
    endpoint.tick();

    let subscription = endpoint.subscribe_all("dashboard");
    producer.submit_text("runtime.log", "visible");
    endpoint.tick();

    let deliveries = subscription.drain_available();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].stream, stream());
    assert_eq!(deliveries[0].frame.position, Position(1));
    assert_eq!(deliveries[0].frame.channel, ChannelId::new("runtime.log"));
    assert_eq!(deliveries[0].frame.payload, b"visible");
}

#[test]
fn endpoint_fans_out_ordered_frames_to_multiple_subscribers() {
    let endpoint = DatastreamEndpoint::with_capacity(stream(), 8, 4);
    let producer = endpoint.producer();
    producer.set_frame_timing_enabled(false);
    let left = endpoint.subscribe_all("left");
    let right = endpoint.subscribe_all("right");

    for n in 0..3 {
        producer.submit_text("runtime.log", format!("line-{n}"));
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
    let endpoint = DatastreamEndpoint::with_capacity(stream(), 8, 8);
    let producer = endpoint.producer();
    producer.set_frame_timing_enabled(false);
    let slow = endpoint.subscribe_all_with_capacity("slow", 1);
    let fast = endpoint.subscribe_all_with_capacity("fast", 8);

    for n in 0..4 {
        producer.submit_text("runtime.log", format!("line-{n}"));
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
fn delivery_fanout_can_publish_collector_deliveries_without_a_mux() {
    let fanout = DeliveryFanout::new(4);
    let sub = fanout.subscribe_all("dashboard");
    let delivery = datastream::Delivery::new(
        stream(),
        Frame::new("remote.lifecycle", Position(9), b"ready".to_vec()),
    );

    let tick = fanout.publish(delivery.clone());

    assert_eq!(tick.drained, 1);
    assert_eq!(tick.delivered, 1);
    assert_eq!(
        sub.recv_timeout(Duration::from_millis(50)).unwrap(),
        delivery
    );
}

#[test]
fn endpoint_producer_timing_sidecars_are_fanned_out_when_enabled() {
    let endpoint = DatastreamEndpoint::with_capacity(stream(), 8, 4);
    let producer = endpoint.producer();
    let sub = endpoint.subscribe_all("test");

    producer.set_frame_timing_enabled(true);
    let data_position = producer.submit_text("runtime.log", "visible");
    let tick = endpoint.tick();

    assert!(endpoint.frame_timing_enabled());
    assert!(producer.frame_timing_enabled());
    assert_eq!(data_position, Position(0));
    assert_eq!(tick.drained, 2);
    assert_eq!(tick.delivered, 2);

    let deliveries = sub.drain_available();
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[0].stream, stream());
    assert_eq!(deliveries[0].frame.position, Position(0));
    assert_eq!(deliveries[0].frame.channel, ChannelId::new("runtime.log"));
    assert_eq!(deliveries[0].frame.payload, b"visible");
    assert_eq!(deliveries[1].stream, stream());
    assert_eq!(deliveries[1].frame.position, Position(1));
    assert_eq!(
        deliveries[1].frame.channel,
        ChannelId::new(FRAME_TIME_CHANNEL)
    );

    let sample =
        FrameTimeSample::decode(&deliveries[1].frame.payload).expect("timing sidecar decodes");
    assert_eq!(sample.target_position, data_position.0);
    assert!(
        sample.created_at_unix_ns > 0,
        "sidecar records a concrete creation timestamp"
    );
}

#[test]
fn process_observer_adapter_submits_configured_channels() {
    let endpoint = DatastreamEndpoint::with_capacity(stream(), 8, 4);
    let producer = endpoint.producer();
    producer.set_frame_timing_enabled(false);
    let observer = producer.process_observer_with(|label, is_stderr| {
        ChannelId::new(format!(
            "proc.{label}.{}",
            if is_stderr { "stderr" } else { "stdout" }
        ))
    });
    let sub = endpoint.subscribe_all("test");

    observer.on_output("trainer", false, b"hello\n");
    observer.on_output("trainer", true, b"warn\n");
    endpoint.tick();

    let deliveries = sub.drain_available();
    assert_eq!(deliveries.len(), 2);
    assert_eq!(
        deliveries[0].frame.channel,
        ChannelId::new("proc.trainer.stdout")
    );
    assert_eq!(deliveries[0].frame.payload, b"hello\n");
    assert_eq!(
        deliveries[1].frame.channel,
        ChannelId::new("proc.trainer.stderr")
    );
    assert_eq!(deliveries[1].frame.payload, b"warn\n");
}

#[test]
fn stats_hook_adapter_submits_worker_snapshot_json() {
    let endpoint = DatastreamEndpoint::with_capacity(stream(), 8, 4);
    let producer = endpoint.producer();
    let hook = producer.stats_hook_on("runtime.actors");
    let sub = endpoint.subscribe_all("test");
    let actor = ActorAddress::new_random();
    let snapshots = [ActorSnapshot {
        address: actor,
        mailbox_depth: 3,
        last_msg_type: Some("Ping"),
        messages_processed: 5,
        poisoned: false,
        message_type_counts: vec![("Ping", 5)],
    }];

    hook.on_tick(2, &snapshots);
    endpoint.tick();

    let delivery = sub.recv_timeout(Duration::from_millis(50)).unwrap();
    assert_eq!(delivery.frame.channel, ChannelId::new("runtime.actors"));
    let json: Value = serde_json::from_slice(&delivery.frame.payload).unwrap();
    assert_eq!(json["worker_id"], 2);
    assert_eq!(json["actors"][0]["address"], actor.to_string());
    assert_eq!(json["actors"][0]["mailbox_depth"], 3);
    assert_eq!(json["actors"][0]["last_msg_type"], "Ping");
    assert_eq!(json["actors"][0]["messages_processed"], 5);
    assert_eq!(json["actors"][0]["message_type_counts"][0]["ty"], "Ping");
}

fn positions(deliveries: &[datastream::Delivery]) -> Vec<u64> {
    deliveries
        .iter()
        .map(|delivery| delivery.frame.position.0)
        .collect()
}
