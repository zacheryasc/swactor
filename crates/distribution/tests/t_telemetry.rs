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
