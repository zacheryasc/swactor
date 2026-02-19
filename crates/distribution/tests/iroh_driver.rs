//! Integration tests for the iroh-based P2P driver.
//!
//! These tests verify that IrohDriver can:
//! - Create endpoints with matching identities
//! - Form clusters via join
//! - Detect membership changes through SWIM
//! - Reject unauthorized peers
//!
//! Requires the `iroh` feature.

#![cfg(feature = "iroh")]

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::iroh::*;
use distribution::peer_auth::PeerAllowList;
use iroh::PublicKey;

// ─── Identity tests ─────────────────────────────────────────────────────

#[test]
fn iroh_driver_creates_with_unique_identity() {
    let mut d1 = make_driver();
    let mut d2 = make_driver();
    assert_ne!(d1.node_id(), d2.node_id());
    d1.shutdown();
    d2.shutdown();
}

#[test]
fn iroh_driver_snapshot_contains_node_id() {
    let mut driver = make_driver();
    let snap = driver.snapshot();
    assert!(!snap.node_id.is_empty());
    assert_eq!(snap.members.len(), 0);
    driver.shutdown();
}

#[test]
fn iroh_driver_identity_matches_iroh_endpoint() {
    let mut driver = make_driver();
    let node_id = driver.node_id();
    let snap = driver.snapshot();
    let expected_hex: String = node_id.0.iter().map(|b| format!("{:02x}", b)).collect();
    assert_eq!(snap.node_id, expected_hex);
    driver.shutdown();
}

// ─── Join integration tests ─────────────────────────────────────────────

#[test]
fn two_nodes_form_cluster_via_join() {
    let mut node_a = make_driver();
    let mut node_b = make_driver();

    let b_addr = node_b.endpoint_addr();
    node_a.join(&[b_addr]);

    let converged = pump_until_pair(
        &mut node_a,
        &mut node_b,
        Duration::from_secs(5),
        |a, b| {
            let a_key = PublicKey::from_bytes(&a.node_id().0).unwrap();
            let b_key = PublicKey::from_bytes(&b.node_id().0).unwrap();
            sees_alive(a, &b_key) && sees_alive(b, &a_key)
        },
    );

    assert!(converged, "nodes did not converge within timeout");
    assert_eq!(node_a.snapshot().alive_count, 1, "node_a should see 1 alive peer");
    assert_eq!(node_b.snapshot().alive_count, 1, "node_b should see 1 alive peer");

    node_a.shutdown();
    node_b.shutdown();
}

#[test]
fn two_nodes_form_cluster_via_mutual_join() {
    let mut node_a = make_driver();
    let mut node_b = make_driver();

    let b_addr = node_b.endpoint_addr();
    let a_addr = node_a.endpoint_addr();

    node_a.join(&[b_addr]);
    node_b.join(&[a_addr]);

    let converged = pump_until_pair(
        &mut node_a,
        &mut node_b,
        Duration::from_secs(5),
        |a, b| {
            let a_key = PublicKey::from_bytes(&a.node_id().0).unwrap();
            let b_key = PublicKey::from_bytes(&b.node_id().0).unwrap();
            sees_alive(a, &b_key) && sees_alive(b, &a_key)
        },
    );

    assert!(converged, "nodes did not converge within timeout (mutual join)");
    assert_eq!(node_a.snapshot().alive_count, 1);
    assert_eq!(node_b.snapshot().alive_count, 1);

    node_a.shutdown();
    node_b.shutdown();
}

#[test]
fn two_nodes_form_cluster_with_peer_auth() {
    let auth_a = Arc::new(Mutex::new(PeerAllowList::open()));
    let auth_b = Arc::new(Mutex::new(PeerAllowList::open()));

    let mut node_a = make_driver_with_auth(auth_a.clone());
    let mut node_b = make_driver_with_auth(auth_b.clone());

    let a_id = node_a.node_id();
    let b_id = node_b.node_id();
    let b_addr = node_b.endpoint_addr();

    // Switch to restrictive mode by adding each other
    auth_a.lock().unwrap().add_peer(b_id, "node-b".into());
    auth_b.lock().unwrap().add_peer(a_id, "node-a".into());

    node_a.join(&[b_addr]);

    let converged = pump_until_pair(
        &mut node_a,
        &mut node_b,
        Duration::from_secs(5),
        |a, b| {
            let a_key = PublicKey::from_bytes(&a.node_id().0).unwrap();
            let b_key = PublicKey::from_bytes(&b.node_id().0).unwrap();
            sees_alive(a, &b_key) && sees_alive(b, &a_key)
        },
    );

    assert!(converged, "nodes with peer auth did not converge within timeout");
    assert_eq!(node_a.snapshot().alive_count, 1);
    assert_eq!(node_b.snapshot().alive_count, 1);

    node_a.shutdown();
    node_b.shutdown();
}

#[test]
fn peer_auth_prevents_unauthorized_join() {
    let mut node_a = make_driver();

    // Node B has auth with only a dummy peer — node_a is NOT authorized
    let auth_b = Arc::new(Mutex::new(PeerAllowList::open()));
    let dummy_id = distribution::types::NodeId([0xAA; 32]);
    auth_b.lock().unwrap().add_peer(dummy_id, "dummy".into());
    let mut node_b = make_driver_with_auth(auth_b);

    let b_addr = node_b.endpoint_addr();
    node_a.join(&[b_addr]);

    let converged = pump_until_pair(
        &mut node_a,
        &mut node_b,
        Duration::from_secs(3),
        |_a, b| b.snapshot().alive_count > 0,
    );

    assert!(!converged, "unauthorized peer should NOT have joined");
    assert_eq!(node_b.snapshot().alive_count, 0, "node_b should have no alive peers");

    node_a.shutdown();
    node_b.shutdown();
}

// ─── Multi-node tests ───────────────────────────────────────────────────

#[test]
fn three_nodes_converge_via_star_join() {
    let mut cluster = IrohTestCluster::star(3);

    let converged = cluster.pump_until(Duration::from_secs(10), |drivers| {
        drivers.iter().all(|d| d.snapshot().alive_count == 2)
    });

    assert!(converged, "3-node star did not converge");
    cluster.shutdown();
}
