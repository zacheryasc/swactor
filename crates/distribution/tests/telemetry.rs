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

mod datastream_records {
    //! Distribution-owned datastream records keep their channels and JSON payloads stable through
    //! the mux.

    use datastream::frame::{Lifetime, NodeId, StreamId};
    use datastream::{Mux, Record};
    use distribution::telemetry::{
        CacheEntryRec, DIST_STATE, DistributionState, MembershipTransition, RegistryEntryRec,
    };

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
    fn distribution_emits_owned_channel_through_datastream_mux() {
        let stream = StreamId::new(NodeId::new("dist-node"), Lifetime(1));
        let mux = Mux::unbounded(stream);
        let state = DistributionState {
            registry_size: 9,
            ..Default::default()
        };

        let pos = mux.submit(DistributionState::channel(), state.encode());
        let frames = mux.drain();

        assert_eq!(pos.0, 0);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].channel.as_str(), DIST_STATE);
        assert_eq!(
            DistributionState::decode(&frames[0].payload)
                .unwrap()
                .registry_size,
            9
        );
    }

    #[test]
    fn membership_transition_record_round_trips_from_owner_crate() {
        let transition = MembershipTransition {
            peer: "peer-a".into(),
            from: "alive".into(),
            to: "suspect".into(),
            reason: "probe timeout".into(),
        };

        assert_eq!(
            MembershipTransition::decode(&transition.encode()).unwrap(),
            transition
        );
    }
}

mod snapshot_and_swim_telemetry {
    //! Snapshot and SWIM telemetry readouts: public empty-state shape, recent probe targets, and
    //! transition causes.

    use distribution::snapshot::DistributionNodeSnapshot;
    use distribution::swim::node::{SwimObservation, SwimObserver};
    use distribution::swim::telemetry::SwimTelemetry;
    use distribution::types::{MemberState, NodeId};

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
        assert!(telemetry.drain_transitions().is_empty());
    }
}
