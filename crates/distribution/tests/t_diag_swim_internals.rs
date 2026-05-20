//! Tier-2 SWIM-internal scrape, end to end (`DIAGNOSTICS_PLAN.md` T2.6 + T2.7).
//!
//! Boots two iroh drivers with `RelayMode::Disabled`, installs a full
//! diagnostics aggregator wired to an [`InMemorySink`], lets them join
//! and pump until each side sees the other Alive, then asserts that
//! the snapshot's tier-2 `swim` block carries:
//!
//! - self-describing protocol config (probe interval, suspicion
//!   timeout, gossip fanout, ...),
//! - a per-peer SWIM entry for the other node, with state Alive and
//!   at least one ping/ack timestamp populated,
//! - a non-empty recent-messages ring buffer that holds the ping/ack
//!   exchange we just observed.
//!
//! T2.7 — discovery-resolve event emission — is exercised by the
//! second scenario, which forces a bare-key dial against a
//! deterministically-keyed unreachable peer and asserts that the
//! aggregator's sink saw the `discovery_resolve_started` /
//! `discovery_resolve_completed` Custom event pair.

#![cfg(feature = "iroh")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::iroh::*;
use distribution::diagnostics::{
    Aggregator, Event, Identity, InMemorySink, Role, SnapshotTrigger,
};
use distribution::diagnostics::iroh_introspect::IntrospectConfig;
use distribution::iroh_driver::IrohDriver;
use distribution::types::NodeId;
use iroh::{EndpointAddr, PublicKey, SecretKey};

fn install_full_diagnostics(
    driver: &mut IrohDriver,
    run_id: &str,
) -> (Arc<InMemorySink>, Arc<Aggregator<Arc<InMemorySink>>>) {
    let sink = Arc::new(InMemorySink::new());
    let identity = Identity::new(driver.node_id(), Role::stage(), run_id);
    let aggregator = Arc::new(Aggregator::new(identity, sink.clone()));
    driver.install_diagnostics_with_config(
        aggregator.clone(),
        IntrospectConfig {
            scrape_interval: Duration::from_millis(50),
        },
    );
    (sink, aggregator)
}

fn node_hex(id: &NodeId) -> String {
    let mut s = String::with_capacity(64);
    for b in id.0 {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

#[test]
fn two_node_cluster_produces_tier2_swim_snapshot_block() {
    let mut a = make_driver();
    let mut b = make_driver();

    let (_sink_a, agg_a) = install_full_diagnostics(&mut a, "run-tier2-swim");
    let (_sink_b, agg_b) = install_full_diagnostics(&mut b, "run-tier2-swim");

    let b_addr = b.endpoint_addr();
    let a_id = a.node_id();
    let b_id = b.node_id();
    a.join(&[b_addr]);

    let converged = pump_until_pair(
        &mut a,
        &mut b,
        Duration::from_secs(5),
        |a, b| {
            let a_key = PublicKey::from_bytes(&a.node_id().0).unwrap();
            let b_key = PublicKey::from_bytes(&b.node_id().0).unwrap();
            sees_alive(a, &b_key) && sees_alive(b, &a_key)
        },
    );
    assert!(converged, "nodes did not converge within 5s");

    // Drive a couple more pumps so the membership round-trip (ping/ack)
    // and the message ring buffer pick up at least one observation
    // beyond the join handshake. Twenty ticks at the default pace is
    // enough to land a ping in each direction even when the suspect
    // timer hasn't started.
    for _ in 0..20 {
        pump_one(&mut a);
        pump_one(&mut b);
    }

    let snap_a = agg_a.snapshot(SnapshotTrigger::OnDemand);
    let snap_b = agg_b.snapshot(SnapshotTrigger::OnDemand);

    a.shutdown();
    b.shutdown();

    let swim_a = snap_a
        .body
        .swim
        .as_ref()
        .expect("A's snapshot must include the tier-2 SWIM block");
    let swim_b = snap_b
        .body
        .swim
        .as_ref()
        .expect("B's snapshot must include the tier-2 SWIM block");

    // Config block is self-describing — probe parameters land in the
    // snapshot itself so the bundle reader doesn't have to assume
    // anything about the running version.
    for (label, swim) in [("A", swim_a), ("B", swim_b)] {
        assert!(
            swim.config.probe_interval_ticks > 0,
            "{label}: probe_interval_ticks should be > 0; got {}",
            swim.config.probe_interval_ticks,
        );
        assert!(
            swim.config.suspicion_timeout_ticks > 0,
            "{label}: suspicion_timeout_ticks should be > 0",
        );
        assert!(
            swim.config.gossip_fanout_lambda > 0,
            "{label}: gossip_fanout_lambda should be > 0",
        );
        assert!(
            !swim.config.probe_mode.is_empty(),
            "{label}: probe_mode should be a non-empty string",
        );
    }

    // Self id hex reflects the node owning the snapshot.
    assert_eq!(swim_a.self_node_id_hex, node_hex(&a_id));
    assert_eq!(swim_b.self_node_id_hex, node_hex(&b_id));

    // Each side has a SWIM entry for the other.
    let b_hex = node_hex(&b_id);
    let a_hex = node_hex(&a_id);
    let a_view_of_b = swim_a
        .peers
        .iter()
        .find(|p| p.peer_node_id_hex == b_hex)
        .expect("A's tier-2 SWIM peers must include B");
    let b_view_of_a = swim_b
        .peers
        .iter()
        .find(|p| p.peer_node_id_hex == a_hex)
        .expect("B's tier-2 SWIM peers must include A");

    use distribution::diagnostics::PeerState;
    assert_eq!(
        a_view_of_b.state,
        PeerState::Alive,
        "A should see B as Alive; got {:?}",
        a_view_of_b.state,
    );
    assert_eq!(
        b_view_of_a.state,
        PeerState::Alive,
        "B should see A as Alive; got {:?}",
        b_view_of_a.state,
    );

    // At least one of the ping/ack timestamps should be populated on
    // each side after a converged exchange. Which specific one fires
    // first depends on probe scheduling, so just require *some*
    // observation.
    let touched = |p: &distribution::diagnostics::Tier2SwimPeer| {
        p.last_ping_sent_at_ms.is_some()
            || p.last_ack_received_at_ms.is_some()
            || p.last_ping_received_at_ms.is_some()
    };
    assert!(
        touched(a_view_of_b),
        "A's peer entry for B should have at least one ping/ack timestamp; got {a_view_of_b:#?}",
    );
    assert!(
        touched(b_view_of_a),
        "B's peer entry for A should have at least one ping/ack timestamp; got {b_view_of_a:#?}",
    );

    // The recent-messages ring buffer picks up at least one
    // ping/ack/join observation per side. The exact mix depends on
    // who probes first.
    let known_kinds = ["ping", "ack", "ping_req", "indirect_ack", "join_request", "join_response"];
    let saw_known = |swim: &distribution::diagnostics::Tier2SwimState| {
        swim.recent_messages
            .iter()
            .any(|m| known_kinds.contains(&m.kind.as_str()))
    };
    assert!(
        saw_known(swim_a),
        "A's recent_messages should contain a known SWIM kind; got {:?}",
        swim_a.recent_messages.iter().map(|m| &m.kind).collect::<Vec<_>>(),
    );
    assert!(
        saw_known(swim_b),
        "B's recent_messages should contain a known SWIM kind; got {:?}",
        swim_b.recent_messages.iter().map(|m| &m.kind).collect::<Vec<_>>(),
    );
}

#[test]
fn bare_key_dial_emits_discovery_resolve_events() {
    // Force a bare-key dial: build an `EndpointAddr` from a
    // deterministic key with no relay and no direct addresses. With
    // `RelayMode::Disabled` iroh has nothing to discover and the dial
    // will fail — but the diagnostics layer should still emit the
    // discovery_resolve_started / discovery_resolve_completed Custom
    // events around the attempt.
    let mut a = make_driver();
    let (sink, _agg) = install_full_diagnostics(&mut a, "run-tier2-discovery");

    let unreachable_secret = SecretKey::from_bytes(&[0xcd; 32]);
    let unreachable_key = unreachable_secret.public();
    let unreachable_addr = EndpointAddr::new(unreachable_key);
    a.join(&[unreachable_addr]);

    // Pump for a short while so the join task actually issues at
    // least one dial attempt. We're not waiting for success — just
    // for the event pair to appear in the sink.
    let start = std::time::Instant::now();
    let saw_pair = loop {
        pump_one(&mut a);
        let records = sink.records();
        let started = records.iter().any(|r| match &r.event {
            Event::Custom { kind, .. } => kind == "discovery_resolve_started",
            _ => false,
        });
        let completed = records.iter().any(|r| match &r.event {
            Event::Custom { kind, .. } => kind == "discovery_resolve_completed",
            _ => false,
        });
        if started && completed {
            break true;
        }
        if start.elapsed() > Duration::from_secs(15) {
            break false;
        }
        std::thread::sleep(Duration::from_millis(20));
    };

    a.shutdown();

    assert!(
        saw_pair,
        "bare-key dial should emit both discovery_resolve_started and \
         discovery_resolve_completed Custom events; got {:#?}",
        sink.records()
            .iter()
            .filter_map(|r| match &r.event {
                Event::Custom { kind, .. } => Some(kind.clone()),
                _ => None,
            })
            .collect::<Vec<_>>(),
    );
}
