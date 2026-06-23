//! LAN cluster integration tests — nodes across two physical machines.
//!
//! These tests run a 5-node cluster split across devuan-hpz (192.168.1.106)
//! and thinkpad (192.168.1.102) communicating over a real LAN.
//!
//! Run with: `cargo test -p docker-tests -- --ignored lan_`
//! Requires: Docker on both machines, SSH access to thinkpad

use std::time::Duration;

use docker_tests::*;

// ────────────────────────────────────────────────────────────────────────────
// Test 1: Cross-machine cluster converges
// ────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn lan_cluster_converges() {
    // Given: 5 nodes split across two physical machines on a LAN
    let mut cluster = LanClusterHandle::start();

    // When: we wait for convergence
    let result = wait_for_lan_convergence(
        &LAN_ENDPOINTS,
        4, // each node sees at least 4 alive
        Duration::from_secs(30),
    );

    // Then: all 5 nodes discover each other across the LAN
    match result {
        Ok(()) => {
            for &(host, port) in &LAN_ENDPOINTS {
                let snap = poll_distribution_at(host, port)
                    .unwrap_or_else(|| panic!("{host}:{port} unreachable after convergence"));
                assert!(
                    snap.alive_count >= 4,
                    "{host}:{port} should see >= 4 alive, got {}",
                    snap.alive_count
                );
            }
        }
        Err(diag) => {
            cluster.stop();
            panic!("LAN cluster did not converge: {diag}");
        }
    }

    cluster.stop();
}

// ────────────────────────────────────────────────────────────────────────────
// Test 2: Death of a remote node is detected across the LAN
// ────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn lan_remote_node_death_detected() {
    // Given: a converged LAN cluster
    let mut cluster = LanClusterHandle::start();
    wait_for_lan_convergence(&LAN_ENDPOINTS, 4, Duration::from_secs(30))
        .expect("LAN cluster did not converge before kill test");

    // When: we kill node-3 on the thinkpad
    kill_remote_node("node-3");

    // Then: surviving nodes detect the death
    // Survivors: hpz seed(9091), hpz node-2(9092), thinkpad node-4(9094), thinkpad node-5(9095)
    let survivor_endpoints = [
        ("127.0.0.1", 9091_u16),
        ("127.0.0.1", 9092),
        (LAN_THINKPAD_IP, 9094),
        (LAN_THINKPAD_IP, 9095),
    ];
    let result = wait_for_death_detection_at(&survivor_endpoints, 4, Duration::from_secs(30));

    match result {
        Ok(()) => {
            let any_sees_dead = survivor_endpoints.iter().any(|&(host, port)| {
                poll_distribution_at(host, port)
                    .map(|snap| snap.dead_count >= 1)
                    .unwrap_or(false)
            });
            assert!(
                any_sees_dead,
                "at least one survivor should see a dead member"
            );
        }
        Err(diag) => {
            cluster.stop();
            panic!("remote node death not detected: {diag}");
        }
    }

    cluster.stop();
}

// ────────────────────────────────────────────────────────────────────────────
// Test 3: A killed remote node can rejoin across the LAN
// ────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn lan_killed_remote_node_rejoins() {
    // Given: a converged cluster with node-3 killed and detected dead
    let mut cluster = LanClusterHandle::start();
    wait_for_lan_convergence(&LAN_ENDPOINTS, 4, Duration::from_secs(30))
        .expect("LAN cluster did not converge before rejoin test");

    kill_remote_node("node-3");
    let survivor_endpoints = [
        ("127.0.0.1", 9091_u16),
        ("127.0.0.1", 9092),
        (LAN_THINKPAD_IP, 9094),
        (LAN_THINKPAD_IP, 9095),
    ];
    wait_for_death_detection_at(&survivor_endpoints, 4, Duration::from_secs(30))
        .expect("node death not detected before rejoin");

    // When: we restart node-3 on the thinkpad
    restart_remote_node("node-3");

    // Then: node-3 rejoins the cluster across the LAN
    let result = wait_for_lan_convergence(&[(LAN_THINKPAD_IP, 9093)], 1, Duration::from_secs(30));

    match result {
        Ok(()) => {
            let snap = poll_distribution_at(LAN_THINKPAD_IP, 9093)
                .expect("node-3 unreachable after rejoin");
            assert!(
                snap.alive_count >= 1,
                "rejoined node should see >= 1 alive, got {}",
                snap.alive_count
            );
        }
        Err(diag) => {
            cluster.stop();
            panic!("killed remote node did not rejoin: {diag}");
        }
    }

    cluster.stop();
}

// ────────────────────────────────────────────────────────────────────────────
// Test 4: Actors are resolvable across machines
// ────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn lan_actors_resolvable_cross_machine() {
    // Given: a converged 5-node LAN cluster, each with 2 registered actors
    let mut cluster = LanClusterHandle::start();
    wait_for_lan_convergence(&LAN_ENDPOINTS, 4, Duration::from_secs(30))
        .expect("LAN cluster did not converge before actor resolution test");

    // When: we query each node's snapshot
    let mut total_directory_routes = 0;
    let mut total_cache_size = 0;

    for &(host, port) in &LAN_ENDPOINTS {
        let snap =
            poll_distribution_at(host, port).unwrap_or_else(|| panic!("{host}:{port} unreachable"));

        // Then: the directory has converged so each node knows >= its own 2 actors.
        assert!(
            snap.directory_route_count >= 2,
            "{host}:{port} should know >= 2 directory routes, got {}",
            snap.directory_route_count
        );

        total_directory_routes += snap.directory_route_count;
        total_cache_size += snap.cache_size;
    }

    // 5 nodes * 2 actors = 10 actors; every converged node knows all of them.
    assert!(
        total_directory_routes >= 10,
        "total directory routes should be >= 10, got {}",
        total_directory_routes
    );

    // Nodes should cache remote actor locations (including cross-machine)
    assert!(
        total_cache_size >= 5,
        "total cache entries should be >= 5, got {}",
        total_cache_size
    );

    cluster.stop();
}
