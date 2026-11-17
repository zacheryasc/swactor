use std::sync::mpsc;

use protocol_telemetry::consumer::{TransitionDecoder, registry};
use protocol_telemetry::{
    ActorType, Outcome, Phase, Protocol, Reason, TransitionEmitter, TransitionRecord,
};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::RuntimeParts;
use swactor_engine::{Engine, SteppingBackend};
use telemetry::frame::{FrameDelivery, TelemetryEvent};
use telemetry::{
    ChannelContent, Lifetime, NodeId, Position, Record, StreamId, SubscriptionRequest,
    TelemetryEndpoint,
};

fn stream(node: &str) -> StreamId {
    StreamId::new(NodeId::new(node), Lifetime(7))
}

fn transition(previous: Phase, next: Phase, reason: Reason) -> TransitionRecord {
    TransitionRecord {
        actor_type: ActorType::HostStreamBindingActor,
        actor: ActorAddress([0x8a; 32]),
        protocol: Protocol::HostStream,
        operation: Some([0xf1; 16]),
        generation: Some(23),
        incarnation: Some(1948),
        previous,
        next,
        reason,
        outcome: None,
    }
}

#[test]
fn retained_and_live_transitions_share_the_pipe_order_across_producers() {
    let endpoint =
        TelemetryEndpoint::with_capacity(stream("worker"), 8, 8).with_retention(3, 16_384);
    let producer = endpoint.producer();
    let stream_emitter = TransitionEmitter::register(&producer);
    let session_emitter = TransitionEmitter::register(&producer);
    let text = producer.register_channel("stdout", ChannelContent::TextStream);

    stream_emitter.emit(transition(
        Phase::Created,
        Phase::Resolving,
        Reason::Started,
    ));
    assert!(producer.submit_text(text, "reader waiting"));
    let opened = transition(
        Phase::AwaitingTransport,
        Phase::Open,
        Reason::TransportReady,
    );
    stream_emitter.emit(opened);
    endpoint.tick();

    let forwarded = TransitionRecord {
        actor_type: ActorType::ChildDataPlaneSessionActor,
        actor: ActorAddress([0x7b; 32]),
        protocol: Protocol::ChildSession,
        previous: Phase::Opening,
        next: Phase::Forwarding,
        reason: Reason::Forwarded,
        ..opened
    };
    session_emitter.emit(forwarded);
    endpoint.tick();

    // Failure collection begins after the first transition has been evicted.
    let subscription = endpoint.subscribe_retained("failure-artifact", SubscriptionRequest::all());
    let mut decoder = TransitionDecoder::from_snapshot(subscription.snapshot());
    let retained = subscription.drain_available();
    let positions: Vec<_> = retained
        .iter()
        .filter_map(|event| match event {
            TelemetryEvent::Frame(frame) => Some(frame.position.0),
            _ => None,
        })
        .collect();
    assert_eq!(positions, [1, 2, 3]);
    let transitions: Vec<_> = retained
        .iter()
        .filter_map(|event| decoder.observe(event).expect("valid retained event"))
        .collect();
    assert_eq!(
        transitions
            .iter()
            .map(|event| (event.position, event.record))
            .collect::<Vec<_>>(),
        [(Position(2), opened), (Position(3), forwarded)],
    );

    let completed = TransitionRecord {
        previous: Phase::Forwarding,
        next: Phase::Finished,
        reason: Reason::Completed,
        outcome: Some(Outcome::Success),
        ..forwarded
    };
    session_emitter.clone().emit(completed);
    endpoint.tick();
    let live = subscription.try_recv().expect("live transition");
    let observed = decoder.observe(&live).unwrap().unwrap();
    assert_eq!(
        (observed.position, observed.record),
        (Position(4), completed)
    );
    assert!(
        subscription.try_recv().is_err(),
        "replay/live boundary must not duplicate records"
    );
}

#[test]
fn decoder_resolves_stream_local_channels_and_preserves_malformed_evidence() {
    let left = TelemetryEndpoint::new(stream("left"));
    let emitter = TransitionEmitter::register(&left.producer());
    let left_subscription = left.subscribe_all("artifact");
    let mut decoder = TransitionDecoder::from_snapshot(left_subscription.snapshot());

    let right = TelemetryEndpoint::new(stream("right"));
    let right_producer = right.producer();
    let text = right_producer.register_channel("unrelated", ChannelContent::TextStream);
    let right_subscription = right.subscribe_all("artifact");
    for descriptor in &right_subscription.snapshot().channels {
        decoder.register(descriptor);
    }
    let record = transition(Phase::Open, Phase::Closing, Reason::CloseRequested);
    assert!(right_producer.submit_bytes(text, record.encode()));
    right.tick();
    // A valid transition-shaped payload on another stream's lane is not evidence.
    assert!(
        decoder
            .observe(&right_subscription.try_recv().unwrap())
            .unwrap()
            .is_none()
    );

    emitter.emit(record);
    left.tick();
    let observed = decoder
        .observe(&left_subscription.try_recv().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(observed.record, record);
    assert_eq!(observed.channel.stream, stream("left"));

    let invalid = TelemetryEvent::Frame(FrameDelivery {
        channel: observed.channel.clone(),
        position: Position(99),
        payload: vec![0xc1],
    });
    let error = decoder
        .observe(&invalid)
        .expect_err("corrupt transition must be reported");
    assert_eq!(error.channel, observed.channel);
    assert_eq!(error.position, Position(99));

    // Ending one stream retires only its own channel interpretation.
    decoder
        .observe(&TelemetryEvent::StreamEnded(stream("left")))
        .unwrap();
    assert!(decoder.observe(&invalid).unwrap().is_none());
}

#[test]
fn late_catalog_registration_and_artifact_rendering_preserve_full_identity() {
    let endpoint = TelemetryEndpoint::new(stream("render"));
    let subscription = endpoint.subscribe_all("artifact");
    let mut decoder = TransitionDecoder::from_snapshot(subscription.snapshot());
    let emitter = TransitionEmitter::register(&endpoint.producer());
    let record = TransitionRecord {
        next: Phase::Closed,
        outcome: Some(Outcome::PeerLost),
        ..transition(Phase::Closing, Phase::Closed, Reason::PeerTerminated)
    };
    emitter.emit(record);
    endpoint.tick();
    let mut observed = None;
    for event in subscription.drain_available() {
        if let Some(transition) = decoder.observe(&event).unwrap() {
            observed = Some(transition);
        }
    }
    let observed = observed.expect("catalog must enable subsequent decoding");
    assert_eq!(observed.record, record);
    let rendered = observed.to_string();
    assert!(rendered.contains(&record.actor.to_full_hex()));
    assert!(rendered.contains(&"f1".repeat(16)));
    // Existing generic artifact views also decode the same named-field record.
    let body = telemetry::views::decode_body_with(
        TransitionRecord::CHANNEL,
        &record.encode(),
        &registry(),
    );
    match body {
        telemetry::views::Body::Record(value) => {
            assert_eq!(value["incarnation"].as_u64(), Some(1948));
            assert_eq!(value["generation"].as_u64(), Some(23));
            assert_eq!(value["outcome"].as_str(), Some("peer_lost"));
        }
        other => panic!("expected typed artifact body, got {other:?}"),
    }
}

struct StreamOpenOperationActor {
    phase: Phase,
    emitter: TransitionEmitter,
    completion: Option<mpsc::Sender<Outcome>>,
    observation: mpsc::Sender<Phase>,
}

#[derive(Clone)]
enum OperationMessage {
    Complete,
    Observe,
}

impl ActorInterface for StreamOpenOperationActor {
    type Incoming = OperationMessage;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, message: OperationMessage) {
        match message {
            OperationMessage::Complete => {
                let Some(completion) = self.completion.take() else {
                    return;
                };
                let previous = self.phase;
                self.phase = Phase::Finished;
                self.emitter.emit(TransitionRecord {
                    actor_type: ActorType::StreamOpenOperationActor,
                    actor: ctx.self_addr(),
                    protocol: Protocol::StreamOpenOperation,
                    previous,
                    next: self.phase,
                    reason: Reason::Completed,
                    outcome: Some(Outcome::Success),
                    ..transition(previous, self.phase, Reason::Completed)
                });
                completion
                    .send(Outcome::Success)
                    .expect("completion receiver");
            }
            OperationMessage::Observe => {
                self.observation.send(self.phase).expect("phase receiver");
            }
        }
    }
}

#[test]
fn full_telemetry_queue_does_not_gate_actor_completion_or_terminal_progress() {
    let endpoint = TelemetryEndpoint::with_capacity(stream("busy"), 1, 8).with_retention(8, 16_384);
    let producer = endpoint.producer();
    let emitter = TransitionEmitter::register(&producer);
    let text = producer.register_channel("stdout", ChannelContent::TextStream);
    assert!(producer.submit_text(text, "occupy the entire mux"));

    let parts = RuntimeParts::new(RuntimeConfig {
        worker_count: 1,
        ..RuntimeConfig::default()
    });
    let runtime = parts.runtime().clone();
    let (completed_tx, completed_rx) = mpsc::channel();
    let (observed_tx, observed_rx) = mpsc::channel();
    let actor = runtime
        .spawn(StreamOpenOperationActor {
            phase: Phase::Opening,
            emitter,
            completion: Some(completed_tx),
            observation: observed_tx,
        })
        .unwrap();
    let backend = SteppingBackend::new();
    let _engine = Engine::new(parts, backend.clone()).unwrap();
    runtime.send_to(actor, OperationMessage::Complete).unwrap();
    runtime.send_to(actor, OperationMessage::Complete).unwrap();
    runtime.send_to(actor, OperationMessage::Observe).unwrap();
    for _ in 0..30 {
        backend.step();
    }

    assert_eq!(completed_rx.try_recv(), Ok(Outcome::Success));
    assert!(
        completed_rx.try_recv().is_err(),
        "terminal completion occurs once"
    );
    assert_eq!(observed_rx.try_recv(), Ok(Phase::Finished));
    assert_eq!(
        endpoint.mux_dropped(),
        1,
        "the completion's evidence was actually rejected"
    );
    endpoint.tick();
    let artifact = endpoint.subscribe_retained("failure", SubscriptionRequest::all());
    let mut decoder = TransitionDecoder::from_snapshot(artifact.snapshot());
    assert!(
        artifact
            .drain_available()
            .iter()
            .all(|event| decoder.observe(event).unwrap().is_none())
    );
}
