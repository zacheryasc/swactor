//! Deploy lifecycle simulation test.
//!
//! Exercises the same verification flow that `cargo xtask deploy` runs
//! (health check, node_id collection, rolling redeploy, convergence)
//! against the local docker-compose cluster — no real SSH needed.
//!
//! Run with: `cargo test -p docker-tests -- --ignored deploy_lifecycle`

use std::collections::HashSet;
use std::time::Duration;

use docker_tests::*;

#[test]
#[ignore]
fn deploy_lifecycle_simulation() {
    // Phase 1: Start cluster (simulates: build + deploy containers)
    let mut cluster = ClusterHandle::start();

    // Phase 2: Health check — all nodes respond to /api/distribution
    for &port in &DASHBOARD_PORTS {
        let snap = poll_distribution(port);
        assert!(snap.is_some(), "node on port {port} not responding");
    }

    // Phase 3: Convergence — all nodes see 4 peers alive
    wait_for_convergence(&DASHBOARD_PORTS, 4, Duration::from_secs(30))
        .expect("initial convergence failed");

    // Phase 4: Collect node IDs (deploy script does this via GET /api/distribution)
    let node_ids: Vec<String> = DASHBOARD_PORTS
        .iter()
        .map(|&port| get_node_id(port).expect("missing node_id"))
        .collect();
    let unique: HashSet<&String> = node_ids.iter().collect();
    assert_eq!(unique.len(), 5, "expected 5 unique node IDs, got {}", unique.len());

    // Phase 5: Simulate rolling redeploy — recreate one node, verify it rejoins
    redeploy_node("node-3");
    wait_for_convergence(&DASHBOARD_PORTS, 4, Duration::from_secs(45))
        .expect("convergence after redeploy failed");

    // Phase 6: Final status report — all metrics healthy
    for &port in &DASHBOARD_PORTS {
        let snap = poll_distribution(port).unwrap();
        assert!(snap.alive_count >= 4,
            "port {port}: expected alive_count >= 4, got {}", snap.alive_count);
        assert!(snap.directory_route_count >= 2,
            "port {port}: expected directory_route_count >= 2, got {}", snap.directory_route_count);
    }

    cluster.stop();
}
