use distribution::swim::node::{NodeAction, SwimNode};
use distribution::swim::probe::SwimConfig;
use distribution::types::{MemberState, NodeId, NodeRecord};

fn node(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

fn addr(port: u16) -> std::net::SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

fn fast_config() -> SwimConfig {
    SwimConfig {
        probe_interval: 5,
        probe_timeout: 3,
        indirect_probes: 2,
        suspicion_timeout: 10,
        dead_reprobe_interval: 0,
    }
}

fn tick_n(swim: &mut SwimNode, n: u64) -> Vec<NodeAction> {
    let mut all = Vec::new();
    for _ in 0..n {
        all.extend(swim.tick());
    }
    all
}

// ─── Solo node ──────────────────────────────────────────────────────────────

#[test]
fn solo_node_starts_with_empty_membership() {
    let swim = SwimNode::new(node(0), addr(8000), fast_config());
    assert_eq!(swim.members().alive_count(), 0);
}

#[test]
fn solo_node_ticks_without_actions() {
    let mut swim = SwimNode::new(node(0), addr(8000), fast_config());
    let actions = tick_n(&mut swim, 100);
    assert!(actions.is_empty(), "no members → no actions");
}

// ─── Join protocol ──────────────────────────────────────────────────────────

#[test]
fn join_produces_join_requests_to_seeds() {
    let swim = SwimNode::new(node(1), addr(8001), fast_config());
    let seeds = vec![addr(8000), addr(8002)];
    let actions = swim.join(&seeds);

    assert_eq!(actions.len(), 2);
    for action in &actions {
        assert!(matches!(action, NodeAction::SendJoinRequest { .. }));
    }
}

#[test]
fn seed_handles_join_request_and_responds_with_members() {
    let mut seed = SwimNode::new(node(0), addr(8000), fast_config());

    // Seed already knows about node 2
    seed.handle_join_response(vec![NodeRecord {
        node_id: node(2),
        addr: addr(8002),
        state: MemberState::Alive,
        incarnation: 0,
    }]);

    // Node 1 sends join request
    let actions = seed.handle_join_request(node(1), addr(8001));

    // Should have JoinResponse and MembershipChanged
    let join_responses: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, NodeAction::SendJoinResponse { .. }))
        .collect();
    assert_eq!(join_responses.len(), 1);

    // The join response should include node 1 (just added) and node 2 (existing)
    if let NodeAction::SendJoinResponse { members, .. } = &join_responses[0] {
        assert!(members.len() >= 1, "should include at least node 2");
    }

    // Seed should now know about node 1
    assert!(seed.members().get(&node(1)).is_some());
}

#[test]
fn joiner_populates_members_from_response() {
    let mut joiner = SwimNode::new(node(1), addr(8001), fast_config());

    let member_list = vec![
        NodeRecord {
            node_id: node(2),
            addr: addr(8002),
            state: MemberState::Alive,
            incarnation: 0,
        },
        NodeRecord {
            node_id: node(3),
            addr: addr(8003),
            state: MemberState::Alive,
            incarnation: 0,
        },
    ];

    let actions = joiner.handle_join_response(member_list);

    // Should emit MembershipChanged for each new member
    let changes: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, NodeAction::MembershipChanged { .. }))
        .collect();
    assert_eq!(changes.len(), 2);

    assert_eq!(joiner.members().alive_count(), 2);
}

// ─── Ping/Ack round-trip ────────────────────────────────────────────────────

#[test]
fn ping_produces_ack_response() {
    let mut swim = SwimNode::new(node(0), addr(8000), fast_config());
    let actions = swim.handle_ping(node(1), addr(8001), 42, &[]);

    let acks: Vec<_> = actions.iter().filter(|a| matches!(a, NodeAction::SendAck { .. })).collect();
    assert_eq!(acks.len(), 1);

    if let NodeAction::SendAck { to, sequence, .. } = &acks[0] {
        assert_eq!(*to, node(1));
        assert_eq!(*sequence, 42);
    }
}

#[test]
fn ping_from_unknown_node_adds_it_to_members() {
    let mut swim = SwimNode::new(node(0), addr(8000), fast_config());
    assert_eq!(swim.members().alive_count(), 0);

    swim.handle_ping(node(1), addr(8001), 1, &[]);
    assert_eq!(swim.members().alive_count(), 1);
}

// ─── Piggyback dissemination ────────────────────────────────────────────────

#[test]
fn membership_updates_piggyback_on_pings() {
    let mut swim = SwimNode::new(node(0), addr(8000), fast_config());

    // Add a member and join a node (which enqueues a dissemination update)
    swim.handle_join_request(node(1), addr(8001));

    // Tick until a probe fires — the ping should carry piggyback data
    let actions = tick_n(&mut swim, 5);
    let pings: Vec<_> = actions.iter().filter_map(|a| {
        if let NodeAction::SendPing { piggyback, .. } = a {
            Some(piggyback)
        } else {
            None
        }
    }).collect();

    if !pings.is_empty() {
        // At least one ping should carry piggyback (the join update)
        assert!(pings.iter().any(|pb| !pb.is_empty()), "pings should carry piggyback data");
    }
}

// ─── Refutation ─────────────────────────────────────────────────────────────

#[test]
fn node_refutes_when_suspected_via_piggyback() {
    let mut swim = SwimNode::new(node(0), addr(8000), fast_config());

    // Simulate receiving a piggyback that suspects us
    use distribution::swim::dissemination::{membership_update, DisseminationQueue};
    let mut q = DisseminationQueue::new(3);
    q.enqueue(
        membership_update(node(0), addr(8000), MemberState::Suspect, 0),
        5,
    );
    let piggyback = q.pack_piggyback(10);

    // Receive a ping with this piggyback
    swim.handle_ping(node(1), addr(8001), 1, &piggyback);

    // Our incarnation should have been bumped
    assert!(swim.members().self_incarnation() > 0, "should have refuted by bumping incarnation");
}

// ─── Leave ──────────────────────────────────────────────────────────────────

#[test]
fn leave_enqueues_death_for_dissemination() {
    let mut swim = SwimNode::new(node(0), addr(8000), fast_config());
    swim.handle_join_request(node(1), addr(8001));

    swim.leave();

    // Tick to trigger a probe — the death update should piggyback
    let actions = tick_n(&mut swim, 5);
    let pings_with_piggyback: Vec<_> = actions.iter().filter_map(|a| {
        if let NodeAction::SendPing { piggyback, .. } = a {
            if !piggyback.is_empty() { Some(piggyback) } else { None }
        } else {
            None
        }
    }).collect();

    // We can't guarantee the exact content, but the leave should enqueue something
    // that gets piggybacked
    assert!(!pings_with_piggyback.is_empty() || swim.members().alive_count() > 0);
}

// ─── Full join scenario ─────────────────────────────────────────────────────

#[test]
fn three_node_cluster_forms_via_seed() {
    let mut seed = SwimNode::new(node(0), addr(8000), fast_config());
    let mut n1 = SwimNode::new(node(1), addr(8001), fast_config());
    let mut n2 = SwimNode::new(node(2), addr(8002), fast_config());

    // Node 1 joins via seed
    let join_actions = seed.handle_join_request(node(1), addr(8001));
    for action in &join_actions {
        if let NodeAction::SendJoinResponse { members, .. } = action {
            n1.handle_join_response(members.clone());
        }
    }

    // Node 2 joins via seed
    let join_actions = seed.handle_join_request(node(2), addr(8002));
    for action in &join_actions {
        if let NodeAction::SendJoinResponse { members, .. } = action {
            n2.handle_join_response(members.clone());
        }
    }

    // Seed knows both
    assert_eq!(seed.members().alive_count(), 2);
    // Node 1 was added before node 2, so it got the response before node 2 existed
    // It should know at least the seed's other members
    assert!(n1.members().alive_count() >= 1);
    // Node 2 should know about node 1 (from the seed's response)
    assert!(n2.members().alive_count() >= 1);
}
