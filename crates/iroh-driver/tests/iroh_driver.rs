//! Integration tests for the iroh-based P2P driver.
//!
//! These tests verify that IrohDriver can:
//! - Create endpoints with matching identities
//! - Form clusters via join
//! - Detect membership changes through SWIM
//! - Reject unauthorized peers
//!
//! Runs in the `iroh-driver` crate, where iroh support is always available.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::iroh::*;
use distribution::peer_auth::PeerAllowList;
use iroh::PublicKey;
use parking_lot::Mutex;

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
fn iroh_driver_reports_listen_addr_and_no_routes() {
    // A freshly created driver knows where it listens and, having learned no
    // peers, has converged on an empty directory route view.
    let mut driver = make_driver();
    assert!(!driver.listen_addr().is_empty());
    assert_eq!(driver.directory_route_count(), 0);
    driver.shutdown();
}

#[test]
fn endpoint_addr_includes_home_relay() {
    let (relay_url, _relay_guard) = spawn_test_relay();
    let expected_relay_url = relay_url.to_string();
    let mut node = make_driver_with_relay(relay_url.clone());
    let start = Instant::now();
    let (relay_advertised, observed_relay_url) = loop {
        let endpoint = node.endpoint_addr();
        let current_relay_url = endpoint.relay_urls().next().map(|url| url.to_string());
        if current_relay_url.as_deref() == Some(expected_relay_url.as_str()) {
            break (true, current_relay_url);
        }
        if start.elapsed() >= Duration::from_secs(5) {
            break (false, current_relay_url);
        }
        std::thread::sleep(Duration::from_millis(10));
    };

    node.shutdown();

    assert!(
        relay_advertised,
        "advertised endpoint did not include home relay {expected_relay_url} within timeout; last relay URL: {observed_relay_url:?}"
    );
}

// ─── Join integration tests ─────────────────────────────────────────────

#[test]
fn two_nodes_form_cluster_via_join() {
    let mut node_a = make_driver();
    let mut node_b = make_driver();

    let b_addr = node_b.endpoint_addr();
    node_a.join(&[b_addr]);

    let converged = pump_until_pair(&mut node_a, &mut node_b, Duration::from_secs(5), |a, b| {
        let a_key = PublicKey::from_bytes(&a.node_id().0).unwrap();
        let b_key = PublicKey::from_bytes(&b.node_id().0).unwrap();
        sees_alive(a, &b_key) && sees_alive(b, &a_key)
    });

    assert!(converged, "nodes did not converge within timeout");
    assert_eq!(node_a.alive_count(), 1, "node_a should see 1 alive peer");
    assert_eq!(node_b.alive_count(), 1, "node_b should see 1 alive peer");

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

    let converged = pump_until_pair(&mut node_a, &mut node_b, Duration::from_secs(5), |a, b| {
        let a_key = PublicKey::from_bytes(&a.node_id().0).unwrap();
        let b_key = PublicKey::from_bytes(&b.node_id().0).unwrap();
        sees_alive(a, &b_key) && sees_alive(b, &a_key)
    });

    assert!(
        converged,
        "nodes did not converge within timeout (mutual join)"
    );
    assert_eq!(node_a.alive_count(), 1);
    assert_eq!(node_b.alive_count(), 1);

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
    auth_a.lock().add_peer(b_id, "node-b".into());
    auth_b.lock().add_peer(a_id, "node-a".into());

    node_a.join(&[b_addr]);

    let converged = pump_until_pair(&mut node_a, &mut node_b, Duration::from_secs(5), |a, b| {
        let a_key = PublicKey::from_bytes(&a.node_id().0).unwrap();
        let b_key = PublicKey::from_bytes(&b.node_id().0).unwrap();
        sees_alive(a, &b_key) && sees_alive(b, &a_key)
    });

    assert!(
        converged,
        "nodes with peer auth did not converge within timeout"
    );
    assert_eq!(node_a.alive_count(), 1);
    assert_eq!(node_b.alive_count(), 1);

    node_a.shutdown();
    node_b.shutdown();
}

#[test]
fn peer_auth_prevents_unauthorized_join() {
    let mut node_a = make_driver();

    // Node B has auth with only a dummy peer — node_a is NOT authorized
    let auth_b = Arc::new(Mutex::new(PeerAllowList::open()));
    let dummy_id = distribution::types::NodeId([0xAA; 32]);
    auth_b.lock().add_peer(dummy_id, "dummy".into());
    let mut node_b = make_driver_with_auth(auth_b);

    let b_addr = node_b.endpoint_addr();
    node_a.join(&[b_addr]);

    let converged = pump_until_pair(&mut node_a, &mut node_b, Duration::from_secs(3), |_a, b| {
        b.alive_count() > 0
    });

    assert!(!converged, "unauthorized peer should NOT have joined");
    assert_eq!(node_b.alive_count(), 0, "node_b should have no alive peers");

    node_a.shutdown();
    node_b.shutdown();
}

// ─── Multi-node tests ───────────────────────────────────────────────────

#[test]
fn three_nodes_converge_via_star_join() {
    let mut cluster = IrohTestCluster::star(3);

    let converged = cluster.pump_until(Duration::from_secs(10), |nodes| {
        nodes.iter().all(|d| d.alive_count() == 2)
    });

    assert!(converged, "3-node star did not converge");
    cluster.shutdown();
}

// ─── Goal 2 (real-QUIC) — genuine death is detected ──────────────────────

#[test]
fn goal2_shutdown_node_is_detected_dead_by_survivors() {
    // BEHAVIORAL_TEST_SPEC Goal 2 over real QUIC: shut one node down for real and
    // poll until the survivors converge on it being Dead — a genuine probe
    // timeout, not an injected death. Observed only through the membership view.
    let mut cluster = IrohTestCluster::star(3);
    let converged = cluster.pump_until(Duration::from_secs(10), |nodes| {
        nodes.iter().all(|d| d.alive_count() == 2)
    });
    assert!(converged, "precondition: 3-node star must converge");

    let dead = 2usize;
    let dead_key = cluster.key(dead);
    cluster.shutdown_one(dead);

    // The survivors' probes to the dead node now truly fail; poll until both
    // converge on it being Dead.
    let detected = cluster.pump_until(Duration::from_secs(30), |drivers| {
        drivers
            .iter()
            .enumerate()
            .all(|(i, d)| i == dead || sees_dead(d, &dead_key))
    });
    assert!(detected, "survivors must detect the shut-down node as Dead");

    // Tear down the two survivors (the third is already shut down).
    cluster[0].shutdown();
    cluster[1].shutdown();
}

// ─── Capability binding (ENGINE_SPEC.md) ──────────────────────

#[test]
fn driver_rejects_engine_without_io() {
    // The SteppingBackend advertises tasks + timers + blocking but NOT io.
    // The driver requires tasks + timers + io, so construction must fail
    // before any endpoint is bound or background work starts.
    use distribution::node::DistributedNodeConfig;
    use iroh::RelayMode;
    use iroh_driver::{IrohDriver, IrohDriverConfig};
    use swactor::config::RuntimeConfig;
    use swactor::runtime::RuntimeParts;
    use swactor_engine::{Engine, SteppingBackend};

    let parts = RuntimeParts::new(RuntimeConfig::default());
    let engine = Engine::new(parts, SteppingBackend::default()).expect("stepping engine");
    let result = IrohDriver::with_engine(
        engine.handle(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: RelayMode::Disabled,
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![],
        },
    );
    assert!(
        result.is_err(),
        "driver must reject an engine that lacks the io capability"
    );
}
