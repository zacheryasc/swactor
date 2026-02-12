use std::net::SocketAddr;

use distribution::swim::member_list::MemberList;
use distribution::swim::probe::{SwimAction, SwimConfig, SwimEvent, SwimProbe};
use distribution::types::{MemberState, NodeId};

fn node(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

fn tick_n(probe: &mut SwimProbe, members: &mut MemberList, n: u64) -> Vec<SwimAction> {
    let mut all_actions = Vec::new();
    for _ in 0..n {
        all_actions.extend(probe.step(SwimEvent::Tick, members));
    }
    all_actions
}

// ─── MemberList tests ───────────────────────────────────────────────────────

#[test]
fn member_list_apply_new_node() {
    let mut ml = MemberList::new(node(0));
    let changed = ml.apply(node(1), addr(8001), MemberState::Alive, 0);
    assert!(changed);
    assert_eq!(ml.alive_count(), 1);
}

#[test]
fn member_list_ignores_self() {
    let mut ml = MemberList::new(node(0));
    let changed = ml.apply(node(0), addr(8000), MemberState::Alive, 0);
    assert!(!changed);
    assert_eq!(ml.len(), 0);
}

#[test]
fn member_list_higher_incarnation_wins() {
    let mut ml = MemberList::new(node(0));
    ml.apply(node(1), addr(8001), MemberState::Alive, 5);

    // Lower incarnation ignored
    let changed = ml.apply(node(1), addr(8001), MemberState::Dead, 3);
    assert!(!changed);
    assert_eq!(ml.get(&node(1)).unwrap().state, MemberState::Alive);

    // Higher incarnation overrides
    let changed = ml.apply(node(1), addr(8001), MemberState::Dead, 6);
    assert!(changed);
    assert_eq!(ml.get(&node(1)).unwrap().state, MemberState::Dead);
}

#[test]
fn member_list_same_incarnation_higher_priority_wins() {
    let mut ml = MemberList::new(node(0));
    ml.apply(node(1), addr(8001), MemberState::Alive, 0);

    // Suspect overrides Alive at same incarnation
    let changed = ml.apply(node(1), addr(8001), MemberState::Suspect, 0);
    assert!(changed);
    assert_eq!(ml.get(&node(1)).unwrap().state, MemberState::Suspect);

    // Alive does NOT override Suspect at same incarnation
    let changed = ml.apply(node(1), addr(8001), MemberState::Alive, 0);
    assert!(!changed);
    assert_eq!(ml.get(&node(1)).unwrap().state, MemberState::Suspect);
}

#[test]
fn member_list_suspect_and_declare_dead() {
    let mut ml = MemberList::new(node(0));
    ml.apply(node(1), addr(8001), MemberState::Alive, 0);

    assert!(ml.suspect(node(1)));
    assert_eq!(ml.get(&node(1)).unwrap().state, MemberState::Suspect);

    assert!(ml.declare_dead(node(1)));
    assert_eq!(ml.get(&node(1)).unwrap().state, MemberState::Dead);
    assert_eq!(ml.alive_count(), 0);
}

#[test]
fn member_list_refute_bumps_incarnation() {
    let mut ml = MemberList::new(node(0));
    assert_eq!(ml.self_incarnation(), 0);
    ml.refute();
    assert_eq!(ml.self_incarnation(), 1);
}

// ─── Probe state machine tests ─────────────────────────────────────────────

#[test]
fn probe_sends_ping_after_interval() {
    let config = SwimConfig {
        probe_interval: 5,
        probe_timeout: 3,
        indirect_probes: 2,
        suspicion_timeout: 20,
    };
    let mut probe = SwimProbe::new(config);
    let mut members = MemberList::new(node(0));
    members.apply(node(1), addr(8001), MemberState::Alive, 0);

    // Ticks 1-4: nothing happens
    let actions = tick_n(&mut probe, &mut members, 4);
    assert!(actions.iter().all(|a| !matches!(a, SwimAction::SendPing { .. })));

    // Tick 5: probe fires
    let actions = tick_n(&mut probe, &mut members, 1);
    let pings: Vec<_> = actions.iter().filter(|a| matches!(a, SwimAction::SendPing { .. })).collect();
    assert_eq!(pings.len(), 1);
}

#[test]
fn probe_ack_completes_cycle() {
    let config = SwimConfig {
        probe_interval: 5,
        probe_timeout: 3,
        indirect_probes: 2,
        suspicion_timeout: 20,
    };
    let mut probe = SwimProbe::new(config);
    let mut members = MemberList::new(node(0));
    members.apply(node(1), addr(8001), MemberState::Alive, 0);

    // Trigger probe
    tick_n(&mut probe, &mut members, 5);

    // Ack arrives — should complete without suspicion
    let actions = probe.step(
        SwimEvent::AckReceived { from: node(1), sequence: 1 },
        &mut members,
    );
    // No suspect or dead actions
    assert!(actions.iter().all(|a| !matches!(a, SwimAction::Suspect(_) | SwimAction::DeclareDead(_))));

    // Verify probe is idle — next probe after interval
    let actions = tick_n(&mut probe, &mut members, 5);
    let pings: Vec<_> = actions.iter().filter(|a| matches!(a, SwimAction::SendPing { .. })).collect();
    assert_eq!(pings.len(), 1, "second probe cycle should fire");
}

#[test]
fn probe_timeout_triggers_indirect_probes() {
    let config = SwimConfig {
        probe_interval: 5,
        probe_timeout: 3,
        indirect_probes: 2,
        suspicion_timeout: 20,
    };
    let mut probe = SwimProbe::new(config);
    let mut members = MemberList::new(node(0));
    members.apply(node(1), addr(8001), MemberState::Alive, 0);
    members.apply(node(2), addr(8002), MemberState::Alive, 0);
    members.apply(node(3), addr(8003), MemberState::Alive, 0);

    // Fire probe
    tick_n(&mut probe, &mut members, 5);

    // Wait for timeout without ack
    let actions = tick_n(&mut probe, &mut members, 3);
    let ping_reqs: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, SwimAction::SendPingReq { .. }))
        .collect();
    assert!(!ping_reqs.is_empty(), "should send indirect probes after timeout");
}

#[test]
fn no_ack_at_all_causes_suspicion() {
    let config = SwimConfig {
        probe_interval: 5,
        probe_timeout: 3,
        indirect_probes: 2,
        suspicion_timeout: 20,
    };
    let mut probe = SwimProbe::new(config);
    let mut members = MemberList::new(node(0));
    members.apply(node(1), addr(8001), MemberState::Alive, 0);

    // Fire probe
    tick_n(&mut probe, &mut members, 5);

    // Wait for direct timeout
    tick_n(&mut probe, &mut members, 3);

    // Wait for indirect timeout — no relays available (only 1 member),
    // so after indirect timeout the target should be suspected
    let actions = tick_n(&mut probe, &mut members, 3);
    let suspects: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, SwimAction::Suspect(_)))
        .collect();
    assert!(!suspects.is_empty(), "should suspect unresponsive node");
}

#[test]
fn suspicion_timeout_causes_death_declaration() {
    let config = SwimConfig {
        probe_interval: 5,
        probe_timeout: 3,
        indirect_probes: 0,
        suspicion_timeout: 10,
    };
    let mut probe = SwimProbe::new(config);
    let mut members = MemberList::new(node(0));
    members.apply(node(1), addr(8001), MemberState::Alive, 0);

    // Fire probe, let it timeout fully (direct + indirect)
    tick_n(&mut probe, &mut members, 5); // ping sent
    tick_n(&mut probe, &mut members, 3); // direct timeout → indirect phase
    let actions = tick_n(&mut probe, &mut members, 3); // indirect timeout → suspect

    // Apply the suspect action to member list
    for action in &actions {
        if let SwimAction::Suspect(id) = action {
            members.suspect(*id);
        }
    }

    // Wait for suspicion timeout
    let actions = tick_n(&mut probe, &mut members, 10);
    let deaths: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, SwimAction::DeclareDead(_)))
        .collect();
    assert!(!deaths.is_empty(), "should declare dead after suspicion timeout");
}

#[test]
fn indirect_ack_rescues_suspected_node() {
    let config = SwimConfig {
        probe_interval: 5,
        probe_timeout: 3,
        indirect_probes: 2,
        suspicion_timeout: 20,
    };
    let mut probe = SwimProbe::new(config);
    let mut members = MemberList::new(node(0));
    members.apply(node(1), addr(8001), MemberState::Alive, 0);
    members.apply(node(2), addr(8002), MemberState::Alive, 0);

    // Fire probe (assume target is node 1)
    let actions = tick_n(&mut probe, &mut members, 5);
    let target = match &actions[0] {
        SwimAction::SendPing { to, sequence, .. } => (*to, *sequence),
        _ => panic!("expected SendPing"),
    };

    // Direct timeout → indirect probes
    tick_n(&mut probe, &mut members, 3);

    // Indirect ack arrives from a relay
    let actions = probe.step(
        SwimEvent::IndirectAckReceived { target: target.0, sequence: target.1 },
        &mut members,
    );

    // Should NOT suspect the node
    assert!(actions.iter().all(|a| !matches!(a, SwimAction::Suspect(_))));

    // And the next probe cycle should start normally
    let actions = tick_n(&mut probe, &mut members, 5);
    assert!(actions.iter().any(|a| matches!(a, SwimAction::SendPing { .. })));
}

#[test]
fn probe_with_no_members_is_idle() {
    let config = SwimConfig::default();
    let mut probe = SwimProbe::new(config);
    let mut members = MemberList::new(node(0));

    // Many ticks with no members — nothing should happen
    let actions = tick_n(&mut probe, &mut members, 100);
    assert!(actions.is_empty());
}
