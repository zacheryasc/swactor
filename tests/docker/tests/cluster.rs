//! Docker cluster integration tests.
//!
//! These tests mirror the simulation tests in
//! `crates/simulation/tests/distribution_sim.rs` but run against real
//! Docker containers communicating over iroh/QUIC.
//!
//! Run with: `cargo test -p docker-tests -- --ignored`
//! Requires: Docker with compose v2

use std::time::Duration;

use docker_tests::*;

// ────────────────────────────────────────────────────────────────────────────
// Test 1: A 5-node cluster converges its membership view
// Mirrors: distribution_sim::cluster_of_five_converges
// ────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn cluster_of_five_converges() {
    // Given: 5 nodes started via docker compose
    let mut cluster = ClusterHandle::start();

    // When: we wait for convergence
    let result = wait_for_convergence(
        &DASHBOARD_PORTS,
        4, // each node sees at least 4 alive (self + 3 peers minimum)
        Duration::from_secs(30),
    );

    // Then: all 5 nodes report healthy membership
    match result {
        Ok(()) => {
            // Verify each node's snapshot looks reasonable
            for (i, &port) in DASHBOARD_PORTS.iter().enumerate() {
                let snap = poll_distribution(port)
                    .unwrap_or_else(|| panic!("node {} (port {}) unreachable after convergence", i, port));
                assert!(
                    snap.alive_count >= 4,
                    "node {} should see >= 4 alive members, got {}",
                    i,
                    snap.alive_count
                );
                assert!(
                    snap.routing_table_size >= 3,
                    "node {} should have >= 3 routing table entries, got {}",
                    i,
                    snap.routing_table_size
                );
            }
        }
        Err(diag) => {
            cluster.stop();
            panic!("cluster of 5 did not converge: {diag}");
        }
    }

    cluster.stop();
}

// ────────────────────────────────────────────────────────────────────────────
// Test 2: A killed node is eventually detected by survivors
// Mirrors: distribution_sim::node_death_is_detected
// ────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn node_death_is_detected() {
    // Given: a converged 5-node cluster
    let mut cluster = ClusterHandle::start();
    wait_for_convergence(&DASHBOARD_PORTS, 4, Duration::from_secs(30))
        .expect("cluster did not converge before kill test");

    // When: we kill node-3
    kill_node("node-3");

    // Then: surviving nodes detect the death within 30s
    // Survivors are: seed(9091), node-2(9092), node-4(9094), node-5(9095)
    let survivor_ports = [9091, 9092, 9094, 9095];
    let result = wait_for_death_detection(
        &survivor_ports,
        4, // should see at most 4 alive (down from 5)
        Duration::from_secs(30),
    );

    match result {
        Ok(()) => {
            // Verify at least one survivor sees the dead node
            let any_sees_dead = survivor_ports.iter().any(|&port| {
                poll_distribution(port)
                    .map(|snap| snap.dead_count >= 1)
                    .unwrap_or(false)
            });
            assert!(any_sees_dead, "at least one survivor should see a dead member");
        }
        Err(diag) => {
            cluster.stop();
            panic!("node death was not detected: {diag}");
        }
    }

    cluster.stop();
}

// ────────────────────────────────────────────────────────────────────────────
// Test 3: A killed node can rejoin the cluster
// Mirrors: distribution_sim::killed_node_rejoins
// ────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn killed_node_rejoins() {
    // Given: a converged cluster with node-3 killed and detected dead
    let mut cluster = ClusterHandle::start();
    wait_for_convergence(&DASHBOARD_PORTS, 4, Duration::from_secs(30))
        .expect("cluster did not converge before rejoin test");

    kill_node("node-3");
    let survivor_ports = [9091, 9092, 9094, 9095];
    wait_for_death_detection(&survivor_ports, 4, Duration::from_secs(30))
        .expect("node death not detected before rejoin");

    // When: we restart node-3
    restart_node("node-3");

    // Then: node-3 rejoins and learns about cluster members
    // Give the restarted node time to re-join and be discovered
    let result = wait_for_convergence(
        &[9093], // node-3's dashboard
        1,       // at minimum, it should know about at least 1 peer
        Duration::from_secs(30),
    );

    match result {
        Ok(()) => {
            let snap = poll_distribution(9093).expect("node-3 unreachable after rejoin");
            assert!(
                snap.alive_count >= 1,
                "rejoined node should see >= 1 alive member, got {}",
                snap.alive_count
            );
        }
        Err(diag) => {
            cluster.stop();
            panic!("killed node did not rejoin: {diag}");
        }
    }

    cluster.stop();
}

// ────────────────────────────────────────────────────────────────────────────
// Test 4: Actors are resolvable across the cluster
// Mirrors: distribution_sim::actors_resolvable_across_cluster
// ────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn actors_resolvable_across_cluster() {
    // Given: a converged 5-node cluster, each with 2 registered actors
    let mut cluster = ClusterHandle::start();
    wait_for_convergence(&DASHBOARD_PORTS, 4, Duration::from_secs(30))
        .expect("cluster did not converge before actor resolution test");

    // When: we query each node's snapshot
    let mut total_directory_entries = 0;
    let mut total_cache_size = 0;

    for (i, &port) in DASHBOARD_PORTS.iter().enumerate() {
        let snap = poll_distribution(port)
            .unwrap_or_else(|| panic!("node {} unreachable", i));

        // Then: each node has registered its own 2 actors in the directory
        assert!(
            snap.directory_entry_count >= 2,
            "node {} should have >= 2 directory entries, got {}",
            i,
            snap.directory_entry_count
        );

        total_directory_entries += snap.directory_entry_count;
        total_cache_size += snap.cache_size;
    }

    // Total actors across cluster should be 10 (5 nodes * 2 actors)
    assert!(
        total_directory_entries >= 10,
        "total directory entries across cluster should be >= 10, got {}",
        total_directory_entries
    );

    // At least some nodes should have cached locations for remote actors
    assert!(
        total_cache_size >= 5,
        "total cache entries across cluster should be >= 5 (each node caches its own 2), got {}",
        total_cache_size
    );

    cluster.stop();
}
