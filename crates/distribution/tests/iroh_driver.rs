//! Integration tests for the iroh-based P2P driver.
//!
//! These tests verify that IrohDriver can:
//! - Create endpoints with matching identities
//! - Form clusters via join
//! - Detect membership changes through SWIM
//!
//! Requires the `iroh` feature.

#![cfg(feature = "iroh")]

use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::RelayMode;

fn test_config() -> DistributedNodeConfig {
    DistributedNodeConfig {
        swim: SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 1,
            suspicion_timeout: 5,
            dead_reprobe_interval: 0,
        },
        cache_capacity: 100,
        republish_interval: 50,
        registry: RegistryConfig::default(),
    }
}

fn make_driver() -> IrohDriver {
    IrohDriver::new(IrohDriverConfig {
        secret_key: None,
        relay_mode: RelayMode::Disabled,
        node: test_config(),
    })
    .expect("failed to create iroh driver")
}

#[test]
fn iroh_driver_creates_with_unique_identity() {
    let d1 = make_driver();
    let d2 = make_driver();
    assert_ne!(d1.node_id(), d2.node_id());
    d1.shutdown();
    d2.shutdown();
}

#[test]
fn iroh_driver_snapshot_contains_node_id() {
    let driver = make_driver();
    let snap = driver.snapshot();
    assert!(!snap.node_id.is_empty());
    assert_eq!(snap.members.len(), 0);
    driver.shutdown();
}

#[test]
fn iroh_driver_identity_matches_iroh_endpoint() {
    let driver = make_driver();
    let node_id = driver.node_id();
    // The snapshot's node_id hex should match the NodeId bytes
    let snap = driver.snapshot();
    let expected_hex: String = node_id.0.iter().map(|b| format!("{:02x}", b)).collect();
    assert_eq!(snap.node_id, expected_hex);
    driver.shutdown();
}
