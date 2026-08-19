//! Distribution observability contracts.
//!
//! These tests are intentionally narrower than protocol tests: they pin public record/snapshot
//! shapes and assert telemetry remains observational rather than protocol-driving.
//!
//! Behavioral/correctness guarantees:
//! - Distribution telemetry records and snapshot shapes are stable wire contracts.
//! - Telemetry is observational: it reports protocol/cache/registry state without feeding back
//!   into behavior.
//! - Transition causes and recent probe targets are externally visible when promised.

mod telemetry_records {
    //! Distribution-owned telemetry records keep their channels and JSON payloads stable through
    //! the mux.

    use distribution::telemetry::{
        CacheEntryRec, DIST_STATE, DistributionState, MEMBERSHIP, MembershipTransition,
        RegistryEntryRec, SWIM_PROBES, SwimProbeEvent, TRANSPORT_INTERNALS, TransportInternals,
    };
    use telemetry::frame::{Lifetime, NodeId, StreamId};
    use telemetry::{ChannelId, Mux, Position, Record};

    #[test]
    fn distribution_state_record_round_trips_from_owner_crate() {
        let state = DistributionState {
            cache_size: 2,
            cache_entries: vec![CacheEntryRec {
                actor_addr: "actor-a".into(),
                node_id: "node-a".into(),
            }],
            directory_route_count: 7,
            registry_size: 3,
            registry_tombstones: 1,
            registry_entries: vec![RegistryEntryRec {
                name: "svc".into(),
                actor_addr: "actor-b".into(),
                node_id: "node-b".into(),
                tombstone: false,
            }],
            recent_probe_targets: vec!["node-c".into()],
            peer_auth_mode: "allow-list".into(),
            authorized_peer_count: 4,
        };

        assert_eq!(DistributionState::CHANNEL, DIST_STATE);
        assert_eq!(DistributionState::decode(&state.encode()).unwrap(), state);
    }

    #[test]
    fn distribution_emits_owned_channel_through_telemetry_mux() {
        let stream = StreamId::new(NodeId::new("dist-node"), Lifetime(1));
        let mux = Mux::unbounded(stream);
        let state = DistributionState {
            registry_size: 9,
            ..Default::default()
        };

        let channel = ChannelId(1);
        assert!(mux.submit(channel, state.encode()));
        let frames = mux.drain();

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].channel, channel);
        assert_eq!(frames[0].position, Position(0));
        assert_eq!(
            DistributionState::decode(&frames[0].payload)
                .unwrap()
                .registry_size,
            9
        );
    }

    #[test]
    fn transport_internals_record_round_trips_probe_rtt() {
        let internals = TransportInternals {
            relay_connected: true,
            direct_peers: 2,
            relay_peers: 1,
            rtt_ms_p50: 405,
        };

        assert_eq!(TransportInternals::CHANNEL, TRANSPORT_INTERNALS);
        assert_eq!(
            TransportInternals::decode(&internals.encode()).unwrap(),
            internals
        );
    }

    #[test]
    fn membership_transition_record_round_trips_from_owner_crate() {
        let transition = MembershipTransition {
            peer: "peer-a".into(),
            from: "alive".into(),
            to: "suspect".into(),
            reason: "probe timeout".into(),
            last_ack_age_ms: Some(15_000),
            consecutive_timeouts: 2,
            recent_probe_targets: vec!["peer-a".into(), "peer-b".into()],
            member_state: Some("Suspect".into()),
        };

        assert_eq!(MembershipTransition::CHANNEL, MEMBERSHIP);
        assert_eq!(
            MembershipTransition::decode(&transition.encode()).unwrap(),
            transition
        );
    }

    #[test]
    fn swim_probe_event_record_round_trips_from_owner_crate() {
        let event = SwimProbeEvent {
            event: "timed_out".into(),
            target: "peer-a".into(),
            sequence: 9,
            kind: "direct".into(),
            rtt_ms: None,
            budget_ms: Some(15_000),
            budget_ticks: Some(15_000),
            last_ack_age_ms: Some(45_000),
            consecutive_timeouts: 3,
            recent_probe_targets: vec!["peer-a".into()],
            member_state: Some("Suspect".into()),
            local_phase: "weights_loaded_wait".into(),
            probe_interval_ms: 200,
            probe_timeout_ms: 15_000,
            indirect_probes: 2,
            suspicion_timeout_ms: 45_000,
            dead_reprobe_interval_ms: 1_000,
            probe_mode: "Periodic".into(),
            lifeguard_enabled: false,
        };

        assert_eq!(SwimProbeEvent::CHANNEL, SWIM_PROBES);
        assert_eq!(SwimProbeEvent::decode(&event.encode()).unwrap(), event);
    }
}

mod snapshot_and_swim_telemetry {
    //! Snapshot and SWIM telemetry readouts: public empty-state shape, recent probe targets, and
    //! transition causes.

    use distribution::snapshot::DistributionNodeSnapshot;
    use distribution::swim::node::{SwimObservation, SwimObserver};
    use distribution::swim::telemetry::SwimTelemetry;
    use distribution::types::{MemberState, NodeId};
    use std::time::Duration;

    fn id(byte: u8) -> NodeId {
        NodeId([byte; 32])
    }

    #[test]
    fn empty_distribution_snapshot_keeps_the_public_wire_shape_stable() {
        // Correctness: external distribution consumers can deserialize the retained
        // snapshot shape, and the empty constructor reports explicit zero/open defaults.
        let snapshot = DistributionNodeSnapshot::empty(id(0xAB));
        let json = serde_json::to_string(&snapshot).unwrap();
        let decoded: DistributionNodeSnapshot = serde_json::from_str(&json).unwrap();

        assert_eq!(
            decoded.node_id,
            "abababababababababababababababababababababababababababababababab"
        );
        assert_eq!(decoded.peer_auth_mode, "open");
        assert_eq!(decoded.alive_count, 0);
        assert!(decoded.members.is_empty());
        assert!(decoded.registry_entries.is_empty());
        assert!(decoded.authorized_peer_count.is_none());
    }

    #[test]
    fn swim_telemetry_reports_probe_targets_and_transition_causes_without_state_feedback() {
        // Correctness: telemetry is an observer side channel. It records recent probes
        // and transition causes for readers, and draining transitions only affects the
        // telemetry buffer, not protocol state.
        let telemetry = SwimTelemetry::new();
        let peer = id(4);

        telemetry.observe(SwimObservation::ProbeSent {
            target: peer,
            sequence: 1,
            kind: "direct",
        });
        telemetry.observe(SwimObservation::Transition {
            peer,
            from: Some(MemberState::Alive),
            to: MemberState::Suspect,
            reason: "probe-timeout",
        });

        assert_eq!(telemetry.rtt_ms_p50(), 0);
        assert_eq!(telemetry.recent_targets(), vec![peer]);
        assert_eq!(telemetry.last_reasons().get(&peer), Some(&"probe-timeout"));

        let drained = telemetry.drain_transitions();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].peer, peer);
        assert_eq!(drained[0].from, Some(MemberState::Alive));
        assert_eq!(drained[0].to, MemberState::Suspect);
        assert_eq!(drained[0].reason, "probe-timeout");
        assert_eq!(drained[0].consecutive_timeouts, 0);
        assert!(drained[0].last_ack_age.is_none());
        assert!(telemetry.drain_transitions().is_empty());
        assert_eq!(telemetry.recent_targets(), vec![peer]);
    }

    #[test]
    fn swim_telemetry_keeps_recent_probe_targets_bounded_and_ordered() {
        let telemetry = SwimTelemetry::new();

        for byte in 0..17 {
            telemetry.observe(SwimObservation::ProbeSent {
                target: id(byte),
                sequence: byte as u64,
                kind: "direct",
            });
        }

        let expected = (1..17).map(id).collect::<Vec<_>>();
        assert_eq!(telemetry.recent_targets(), expected);
    }

    #[test]
    fn swim_telemetry_records_probe_events_and_timeout_state_without_fabricating_rtt() {
        let telemetry = SwimTelemetry::new();
        let peer = id(8);

        telemetry.observe(SwimObservation::ProbeSent {
            target: peer,
            sequence: 1,
            kind: "direct",
        });
        std::thread::sleep(Duration::from_millis(1));
        telemetry.observe(SwimObservation::ProbeAcked {
            target: peer,
            sequence: 1,
            kind: "direct",
        });
        telemetry.observe(SwimObservation::ProbeSent {
            target: peer,
            sequence: 2,
            kind: "direct",
        });
        telemetry.observe(SwimObservation::ProbeTimedOut {
            target: peer,
            sequence: 2,
            kind: "direct",
            budget_ticks: 15_000,
        });

        let events = telemetry.drain_probe_events();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].event, "sent");
        assert_eq!(events[1].event, "acked");
        assert!(events[1].rtt_ms.is_some());
        assert_eq!(events[2].event, "sent");
        assert_eq!(events[3].event, "timed_out");
        assert_eq!(events[3].budget_ms, Some(15_000));
        assert_eq!(events[3].rtt_ms, None);
        assert_eq!(events[3].consecutive_timeouts, 1);
        assert!(telemetry.drain_probe_events().is_empty());
    }
}
