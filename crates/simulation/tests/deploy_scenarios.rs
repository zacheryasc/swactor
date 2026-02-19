//! Deployment topology simulation scenarios — NAT, relay, firewall, staggered join.
//!
//! These tests model real deployment topologies (home NAT + cloud VPS) to catch
//! transport-level issues before hitting the real internet.

use simulation::distribution::properties::{
    analyze, check_convergence, check_group_convergence, check_membership_accuracy,
    check_staggered_join, check_zero_convergence,
};
use simulation::distribution::sim::{
    run_simulation, DistributionSimConfig, NetworkFault, NetworkTopology, NodeLocation, Partition,
    SimAction,
};

fn default_config() -> DistributionSimConfig {
    DistributionSimConfig {
        actors_per_node: 0,
        ..Default::default()
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 1. Home/cloud topology converges via relay
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn home_cloud_topology_converges_via_relay() {
    // Topology: 3 nodes — 1 Public (relay), 2 Nat("home")
    // Mirrors: hpz + thinkpad + docean
    let config = DistributionSimConfig {
        name: "home-cloud-relay".into(),
        num_nodes: 3,
        num_rounds: 80,
        ticks_per_round: 3,
        topology: Some(NetworkTopology {
            locations: vec![
                NodeLocation::Public,
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Nat { group: "home".into() },
            ],
            relay_nodes: vec![0],
        }),
        ..default_config()
    };

    let trace = run_simulation(config);
    let metrics = analyze(&trace);

    let result = check_membership_accuracy(&metrics, 1.0);
    assert!(
        result.passed,
        "all 3 nodes should converge via relay: {}",
        result.actual
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 2. Multi-site NAT communicates via relay
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn multi_site_nat_communicates_via_relay() {
    // Topology: 5 nodes — 1 Public relay, 2 Nat("home"), 2 Nat("office")
    let config = DistributionSimConfig {
        name: "multi-site-nat".into(),
        num_nodes: 5,
        num_rounds: 100,
        ticks_per_round: 3,
        topology: Some(NetworkTopology {
            locations: vec![
                NodeLocation::Public,
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Nat { group: "office".into() },
                NodeLocation::Nat { group: "office".into() },
            ],
            relay_nodes: vec![0],
        }),
        ..default_config()
    };

    let trace = run_simulation(config);
    let metrics = analyze(&trace);

    let result = check_membership_accuracy(&metrics, 1.0);
    assert!(
        result.passed,
        "all 5 nodes should converge via relay: {}",
        result.actual
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 3. Relay death partitions NAT groups
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn relay_death_partitions_nat_groups() {
    // Topology: 5 nodes — 1 Public relay, 2 Nat("home"), 2 Nat("office")
    // Kill relay at round 30
    let config = DistributionSimConfig {
        name: "relay-death".into(),
        num_nodes: 5,
        num_rounds: 100,
        ticks_per_round: 3,
        topology: Some(NetworkTopology {
            locations: vec![
                NodeLocation::Public,
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Nat { group: "office".into() },
                NodeLocation::Nat { group: "office".into() },
            ],
            relay_nodes: vec![0],
        }),
        kill_schedule: vec![(30, 0)],
        ..default_config()
    };

    let trace = run_simulation(config);

    // Each NAT group should still see its own peers
    let home_result = check_group_convergence(&trace, &[1, 2], 50, 0);
    assert!(
        home_result.passed,
        "home group should maintain internal connectivity: {}",
        home_result.actual
    );

    let office_result = check_group_convergence(&trace, &[3, 4], 50, 0);
    assert!(
        office_result.passed,
        "office group should maintain internal connectivity: {}",
        office_result.actual
    );

    // Cross-group connectivity lost: home nodes should not see office nodes.
    // After relay death and SWIM timeout, each group's member_count should drop.
    let last_round = trace.snapshots_per_round.last().unwrap();
    for &idx in &[1, 2] {
        let snap = &last_round[idx].1;
        assert!(
            snap.is_alive && snap.member_count < 4,
            "home node {} should see < 4 members without relay, got {}",
            idx,
            snap.member_count
        );
    }
    for &idx in &[3, 4] {
        let snap = &last_round[idx].1;
        assert!(
            snap.is_alive && snap.member_count < 4,
            "office node {} should see < 4 members without relay, got {}",
            idx,
            snap.member_count
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 4. Rolling redeploy with reintroduction
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn rolling_redeploy_with_reintroduction() {
    // Topology: 3 nodes — 1 Public, 2 Nat("home")
    // Kill node 1 at round 20, revive at round 40, re-introduce via Join at 45
    let config = DistributionSimConfig {
        name: "rolling-redeploy".into(),
        num_nodes: 3,
        num_rounds: 100,
        ticks_per_round: 3,
        topology: Some(NetworkTopology {
            locations: vec![
                NodeLocation::Public,
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Nat { group: "home".into() },
            ],
            relay_nodes: vec![0],
        }),
        kill_schedule: vec![(20, 1)],
        revive_schedule: vec![(40, 1)],
        action_schedule: vec![
            (45, SimAction::Join { node_idx: 1, seed_idx: 0 }),
        ],
        ..default_config()
    };

    let trace = run_simulation(config);

    // Revived node should rejoin and see >= 1 member
    let last_round = trace.snapshots_per_round.last().unwrap();
    let revived_snap = &last_round[1].1;
    assert!(
        revived_snap.is_alive && revived_snap.member_count >= 1,
        "revived node should rejoin and see >= 1 member, got alive={} members={}",
        revived_snap.is_alive,
        revived_snap.member_count
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 5. Staggered startup — seed first, others join later
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn staggered_startup_seed_first() {
    // Topology: 4 nodes — 1 Public (seed), 3 Nat (mixed groups)
    // All non-seed nodes deferred, joined at rounds 10, 20, 30
    let config = DistributionSimConfig {
        name: "staggered-startup".into(),
        num_nodes: 4,
        num_rounds: 100,
        ticks_per_round: 3,
        topology: Some(NetworkTopology {
            locations: vec![
                NodeLocation::Public,
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Nat { group: "office".into() },
                NodeLocation::Nat { group: "home".into() },
            ],
            relay_nodes: vec![0],
        }),
        deferred_join: vec![1, 2, 3],
        action_schedule: vec![
            (10, SimAction::Join { node_idx: 1, seed_idx: 0 }),
            (20, SimAction::Join { node_idx: 2, seed_idx: 0 }),
            (30, SimAction::Join { node_idx: 3, seed_idx: 0 }),
        ],
        ..default_config()
    };

    let trace = run_simulation(config);

    let result = check_staggered_join(&trace, &[0, 1, 2, 3], 4, 100);
    assert!(
        result.passed,
        "all 4 nodes should be joined by round 100: {}",
        result.actual
    );

    let metrics = analyze(&trace);
    let acc = check_membership_accuracy(&metrics, 0.75);
    assert!(
        acc.passed,
        "staggered cluster should converge: {}",
        acc.actual
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 6. Firewalled node isolated — others converge
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn firewalled_node_isolated_others_converge() {
    // Topology: 5 nodes — 1 Public, 3 Nat, 1 Firewalled (deferred, never joins)
    let config = DistributionSimConfig {
        name: "firewalled-isolated".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        topology: Some(NetworkTopology {
            locations: vec![
                NodeLocation::Public,
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Firewalled,
            ],
            relay_nodes: vec![0],
        }),
        deferred_join: vec![4], // Firewalled node never joins
        ..default_config()
    };

    let trace = run_simulation(config);

    // 4 non-firewalled nodes should converge
    let last_round = trace.snapshots_per_round.last().unwrap();
    let connected_count = (0..4)
        .filter(|&idx| {
            let snap = &last_round[idx].1;
            snap.is_alive && snap.member_count >= 3
        })
        .count();
    assert!(
        connected_count >= 4,
        "4 non-firewalled nodes should all see >= 3 members, only {} do",
        connected_count
    );

    // Firewalled node sees 0 peers (it was deferred and never joined)
    let fw_snap = &last_round[4].1;
    assert!(
        fw_snap.is_alive && fw_snap.member_count == 0,
        "firewalled node should see 0 peers, got {}",
        fw_snap.member_count
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 7. Controller-driven peer introduction
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn controller_driven_peer_introduction() {
    // Topology: 4 nodes — all Public, all deferred (no automatic seed join)
    // Controller introduces all 6 pairs at round 10 via SimAction::Introduce
    // Models the deploy script's POST /api/peers/add flow
    let config = DistributionSimConfig {
        name: "controller-introduction".into(),
        num_nodes: 4,
        num_rounds: 80,
        ticks_per_round: 3,
        topology: Some(NetworkTopology {
            locations: vec![
                NodeLocation::Public,
                NodeLocation::Public,
                NodeLocation::Public,
                NodeLocation::Public,
            ],
            relay_nodes: vec![],
        }),
        deferred_join: vec![0, 1, 2, 3],
        action_schedule: vec![
            // All 6 pairs: (0,1), (0,2), (0,3), (1,2), (1,3), (2,3)
            (10, SimAction::Introduce { node_a: 0, node_b: 1 }),
            (10, SimAction::Introduce { node_a: 0, node_b: 2 }),
            (10, SimAction::Introduce { node_a: 0, node_b: 3 }),
            (10, SimAction::Introduce { node_a: 1, node_b: 2 }),
            (10, SimAction::Introduce { node_a: 1, node_b: 3 }),
            (10, SimAction::Introduce { node_a: 2, node_b: 3 }),
        ],
        ..default_config()
    };

    let trace = run_simulation(config);
    let metrics = analyze(&trace);

    let result = check_membership_accuracy(&metrics, 1.0);
    assert!(
        result.passed,
        "all 4 nodes should converge via controller introduction: {}",
        result.actual
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 8. Deploy auth race — two-pass introduction recovers
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn deploy_auth_race_recovery_via_two_pass() {
    // Models the deploy auth race condition:
    // - 3 nodes, all deferred (no automatic join)
    // - Round 5: 100% drop rate (models auth rejection window — peer A connects
    //   to B but B hasn't added A yet, so the response is dropped)
    // - Round 5: Introduce all pairs (membership exchanged but responses dropped)
    // - Round 10: drop rate cleared (auth race resolved — all allow-lists populated)
    // - Round 15: Re-introduce all pairs (models deploy retry / second pass)
    //
    // The second-pass introduction succeeds because all peers are now authorized.
    let config = DistributionSimConfig {
        name: "deploy-auth-race".into(),
        num_nodes: 3,
        num_rounds: 60,
        ticks_per_round: 3,
        deferred_join: vec![0, 1, 2],
        network_faults: vec![
            NetworkFault::SetDropRate { round: 5, rate: 1.0 },
            NetworkFault::SetDropRate { round: 10, rate: 0.0 },
        ],
        action_schedule: vec![
            // First pass: introductions during drop window (simulates auth race)
            (5, SimAction::Introduce { node_a: 0, node_b: 1 }),
            (5, SimAction::Introduce { node_a: 0, node_b: 2 }),
            (5, SimAction::Introduce { node_a: 1, node_b: 2 }),
            // Second pass: re-introduce after auth race resolves
            (15, SimAction::Introduce { node_a: 0, node_b: 1 }),
            (15, SimAction::Introduce { node_a: 0, node_b: 2 }),
            (15, SimAction::Introduce { node_a: 1, node_b: 2 }),
        ],
        ..default_config()
    };

    let trace = run_simulation(config);
    let metrics = analyze(&trace);

    // Zero-convergence check: should NOT be stuck at 0
    let zero_check = check_zero_convergence(&trace, 20);
    assert!(
        zero_check.passed,
        "cluster should recover from auth race via two-pass: {}",
        zero_check.actual
    );

    // Full membership accuracy after recovery
    let acc = check_membership_accuracy(&metrics, 1.0);
    assert!(
        acc.passed,
        "all 3 nodes should see each other after two-pass introduction: {}",
        acc.actual
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 9. Degenerate controller actions do not degrade convergence
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn degenerate_controller_actions_do_not_degrade_convergence() {
    // Class: Controller-initiated actions that produce degenerate/no-op protocol messages.
    // Two simulations with identical 4-node NAT topology (1 public relay + 3 NAT).
    // The "clean" run has only the 6 necessary Introduce pairs.
    // The "degenerate" run prepends self-joins, self-introductions, and appends
    // redundant re-introductions of already-connected pairs.

    let nat_topology = || NetworkTopology {
        locations: vec![
            NodeLocation::Public,
            NodeLocation::Nat { group: "home".into() },
            NodeLocation::Nat { group: "home".into() },
            NodeLocation::Nat { group: "home".into() },
        ],
        relay_nodes: vec![0],
    };

    // Clean run: only the 6 necessary Introduce pairs at round 10
    let clean_config = DistributionSimConfig {
        name: "degenerate-clean".into(),
        num_nodes: 4,
        num_rounds: 80,
        ticks_per_round: 3,
        topology: Some(nat_topology()),
        deferred_join: vec![0, 1, 2, 3],
        action_schedule: vec![
            (10, SimAction::Introduce { node_a: 0, node_b: 1 }),
            (10, SimAction::Introduce { node_a: 0, node_b: 2 }),
            (10, SimAction::Introduce { node_a: 0, node_b: 3 }),
            (10, SimAction::Introduce { node_a: 1, node_b: 2 }),
            (10, SimAction::Introduce { node_a: 1, node_b: 3 }),
            (10, SimAction::Introduce { node_a: 2, node_b: 3 }),
        ],
        ..default_config()
    };

    // Degenerate run: self-joins, self-introductions, then necessary pairs, then redundant re-introductions
    let degenerate_config = DistributionSimConfig {
        name: "degenerate-noisy".into(),
        num_nodes: 4,
        num_rounds: 80,
        ticks_per_round: 3,
        topology: Some(nat_topology()),
        deferred_join: vec![0, 1, 2, 3],
        action_schedule: vec![
            // Self-joins (degenerate: "Connecting to ourself")
            (5, SimAction::Join { node_idx: 0, seed_idx: 0 }),
            (5, SimAction::Join { node_idx: 1, seed_idx: 1 }),
            (5, SimAction::Join { node_idx: 2, seed_idx: 2 }),
            (5, SimAction::Join { node_idx: 3, seed_idx: 3 }),
            // Self-introductions (degenerate: introduce node to itself)
            (7, SimAction::Introduce { node_a: 1, node_b: 1 }),
            (7, SimAction::Introduce { node_a: 2, node_b: 2 }),
            // Necessary pairs at round 10
            (10, SimAction::Introduce { node_a: 0, node_b: 1 }),
            (10, SimAction::Introduce { node_a: 0, node_b: 2 }),
            (10, SimAction::Introduce { node_a: 0, node_b: 3 }),
            (10, SimAction::Introduce { node_a: 1, node_b: 2 }),
            (10, SimAction::Introduce { node_a: 1, node_b: 3 }),
            (10, SimAction::Introduce { node_a: 2, node_b: 3 }),
            // Redundant re-introductions of already-connected pairs
            (15, SimAction::Introduce { node_a: 0, node_b: 1 }),
            (15, SimAction::Introduce { node_a: 0, node_b: 2 }),
            (15, SimAction::Introduce { node_a: 1, node_b: 3 }),
        ],
        ..default_config()
    };

    let clean_trace = run_simulation(clean_config);
    let clean_metrics = analyze(&clean_trace);
    let degenerate_trace = run_simulation(degenerate_config);
    let degenerate_metrics = analyze(&degenerate_trace);

    // Degenerate run converges to 100% accuracy (self-join doesn't corrupt state)
    let degen_acc = check_membership_accuracy(&degenerate_metrics, 1.0);
    assert!(
        degen_acc.passed,
        "degenerate run should converge to 100% accuracy: {}",
        degen_acc.actual
    );

    // Degenerate run doesn't cause zero-convergence (no node stuck at 0)
    let degen_zero = check_zero_convergence(&degenerate_trace, 20);
    assert!(
        degen_zero.passed,
        "degenerate run should not cause zero-convergence: {}",
        degen_zero.actual
    );

    // Clean run converges (sanity baseline)
    let clean_acc = check_membership_accuracy(&clean_metrics, 1.0);
    assert!(
        clean_acc.passed,
        "clean run should converge: {}",
        clean_acc.actual
    );

    // Convergence speed gap ≤ 10 rounds
    let clean_conv = clean_metrics.join_convergence_round.unwrap_or(80);
    let degen_conv = degenerate_metrics.join_convergence_round.unwrap_or(80);
    let gap = if degen_conv > clean_conv {
        degen_conv - clean_conv
    } else {
        0
    };
    assert!(
        gap <= 10,
        "degenerate actions should not delay convergence by >10 rounds: clean={}, degenerate={}, gap={}",
        clean_conv, degen_conv, gap
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 10. Relay dependency failure prevents cross-group convergence
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn relay_dependency_failure_prevents_cross_group_convergence() {
    // Class: Relay dependency failures that silently prevent convergence.
    // 5 nodes (1 public relay, 2 NAT "home", 2 NAT "office"), all deferred.
    // Models: port 3340 blocked by firewall — NAT nodes can't reach the relay.
    // Link faults block all NAT→relay traffic at round 1. Introductions at
    // round 5 still attempt to connect, but messages to/from relay are dropped.
    // Same-group LAN peers can still reach each other directly.
    let config = DistributionSimConfig {
        name: "relay-dependency-failure".into(),
        num_nodes: 5,
        num_rounds: 100,
        ticks_per_round: 3,
        topology: Some(NetworkTopology {
            locations: vec![
                NodeLocation::Public,
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Nat { group: "office".into() },
                NodeLocation::Nat { group: "office".into() },
            ],
            relay_nodes: vec![0],
        }),
        deferred_join: vec![0, 1, 2, 3, 4],
        network_faults: vec![
            // Block all NAT↔relay links (bidirectional) — models firewall blocking port 3340
            NetworkFault::LinkFault { round: 1, from: 0, to: 1, rate: 1.0, bidirectional: true },
            NetworkFault::LinkFault { round: 1, from: 0, to: 2, rate: 1.0, bidirectional: true },
            NetworkFault::LinkFault { round: 1, from: 0, to: 3, rate: 1.0, bidirectional: true },
            NetworkFault::LinkFault { round: 1, from: 0, to: 4, rate: 1.0, bidirectional: true },
        ],
        action_schedule: vec![
            // Attempt introductions despite firewall (deploy script doesn't know port is blocked)
            (5, SimAction::Introduce { node_a: 0, node_b: 1 }),
            (5, SimAction::Introduce { node_a: 0, node_b: 2 }),
            (5, SimAction::Introduce { node_a: 0, node_b: 3 }),
            (5, SimAction::Introduce { node_a: 0, node_b: 4 }),
            (5, SimAction::Introduce { node_a: 1, node_b: 2 }),
            (5, SimAction::Introduce { node_a: 1, node_b: 3 }),
            (5, SimAction::Introduce { node_a: 1, node_b: 4 }),
            (5, SimAction::Introduce { node_a: 2, node_b: 3 }),
            (5, SimAction::Introduce { node_a: 2, node_b: 4 }),
            (5, SimAction::Introduce { node_a: 3, node_b: 4 }),
        ],
        ..default_config()
    };

    let trace = run_simulation(config);
    let metrics = analyze(&trace);

    // Home group (nodes 1,2) converges via LAN despite relay failure
    let home_conv = check_group_convergence(&trace, &[1, 2], 30, 0);
    assert!(
        home_conv.passed,
        "home group should converge via LAN despite relay failure: {}",
        home_conv.actual
    );

    // Office group (nodes 3,4) converges via LAN despite relay failure
    let office_conv = check_group_convergence(&trace, &[3, 4], 30, 0);
    assert!(
        office_conv.passed,
        "office group should converge via LAN despite relay failure: {}",
        office_conv.actual
    );

    // Each NAT node sees ≥ 1 member (at least same-group LAN peer)
    let last_round = trace.snapshots_per_round.last().unwrap();
    for &idx in &[1, 2, 3, 4] {
        let snap = &last_round[idx].1;
        assert!(
            snap.is_alive && snap.member_count >= 1,
            "NAT node {} should see >= 1 member (LAN peer), got {}",
            idx,
            snap.member_count
        );
    }

    // Full cluster accuracy < 1.0 (cross-group connectivity degraded, not silently "fine")
    assert!(
        metrics.membership_accuracy < 1.0,
        "full cluster should NOT show 100% accuracy with relay failure, got {:.3}",
        metrics.membership_accuracy
    );

    // Not zero-convergence (failure is detectable, not silent death)
    let zero_check = check_zero_convergence(&trace, 20);
    assert!(
        zero_check.passed,
        "relay failure should not cause total convergence death: {}",
        zero_check.actual
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 11. Introduction strategy equivalence under NAT topology
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn introduction_strategy_equivalence_under_nat_topology() {
    // Class: Introduction strategy equivalence under topology constraints.
    // Same 5-node mixed-NAT topology (1 public relay, 2 "home" NAT, 2 "office" NAT),
    // three strategies: Star, Full mesh, Chain.

    let nat_topology = || NetworkTopology {
        locations: vec![
            NodeLocation::Public,
            NodeLocation::Nat { group: "home".into() },
            NodeLocation::Nat { group: "home".into() },
            NodeLocation::Nat { group: "office".into() },
            NodeLocation::Nat { group: "office".into() },
        ],
        relay_nodes: vec![0],
    };

    // Star: All 4 NAT nodes Join via seed (node 0)
    let star_config = DistributionSimConfig {
        name: "strategy-star".into(),
        num_nodes: 5,
        num_rounds: 100,
        ticks_per_round: 3,
        topology: Some(nat_topology()),
        deferred_join: vec![1, 2, 3, 4],
        action_schedule: vec![
            (10, SimAction::Join { node_idx: 1, seed_idx: 0 }),
            (10, SimAction::Join { node_idx: 2, seed_idx: 0 }),
            (10, SimAction::Join { node_idx: 3, seed_idx: 0 }),
            (10, SimAction::Join { node_idx: 4, seed_idx: 0 }),
        ],
        ..default_config()
    };

    // Full mesh: All 10 pairs Introduced at round 10
    let mesh_config = DistributionSimConfig {
        name: "strategy-mesh".into(),
        num_nodes: 5,
        num_rounds: 100,
        ticks_per_round: 3,
        topology: Some(nat_topology()),
        deferred_join: vec![0, 1, 2, 3, 4],
        action_schedule: vec![
            (10, SimAction::Introduce { node_a: 0, node_b: 1 }),
            (10, SimAction::Introduce { node_a: 0, node_b: 2 }),
            (10, SimAction::Introduce { node_a: 0, node_b: 3 }),
            (10, SimAction::Introduce { node_a: 0, node_b: 4 }),
            (10, SimAction::Introduce { node_a: 1, node_b: 2 }),
            (10, SimAction::Introduce { node_a: 1, node_b: 3 }),
            (10, SimAction::Introduce { node_a: 1, node_b: 4 }),
            (10, SimAction::Introduce { node_a: 2, node_b: 3 }),
            (10, SimAction::Introduce { node_a: 2, node_b: 4 }),
            (10, SimAction::Introduce { node_a: 3, node_b: 4 }),
        ],
        ..default_config()
    };

    // Chain: Linear introductions staggered every 5 rounds
    let chain_config = DistributionSimConfig {
        name: "strategy-chain".into(),
        num_nodes: 5,
        num_rounds: 100,
        ticks_per_round: 3,
        topology: Some(nat_topology()),
        deferred_join: vec![0, 1, 2, 3, 4],
        action_schedule: vec![
            (10, SimAction::Introduce { node_a: 0, node_b: 1 }),
            (15, SimAction::Introduce { node_a: 1, node_b: 2 }),
            (20, SimAction::Introduce { node_a: 2, node_b: 3 }),
            (25, SimAction::Introduce { node_a: 3, node_b: 4 }),
        ],
        ..default_config()
    };

    let star_trace = run_simulation(star_config);
    let star_metrics = analyze(&star_trace);
    let mesh_trace = run_simulation(mesh_config);
    let mesh_metrics = analyze(&mesh_trace);
    let chain_trace = run_simulation(chain_config);
    let chain_metrics = analyze(&chain_trace);

    let star_acc = star_metrics.membership_accuracy;
    let mesh_acc = mesh_metrics.membership_accuracy;
    let chain_acc = chain_metrics.membership_accuracy;

    // All three strategies reach ≥ 75% membership accuracy
    assert!(
        star_acc >= 0.75,
        "star strategy should reach >= 75% accuracy, got {:.3}",
        star_acc
    );
    assert!(
        mesh_acc >= 0.75,
        "mesh strategy should reach >= 75% accuracy, got {:.3}",
        mesh_acc
    );
    assert!(
        chain_acc >= 0.75,
        "chain strategy should reach >= 75% accuracy, got {:.3}",
        chain_acc
    );

    // No strategy produces zero-convergence
    let star_zero = check_zero_convergence(&star_trace, 30);
    assert!(
        star_zero.passed,
        "star strategy should not produce zero-convergence: {}",
        star_zero.actual
    );
    let mesh_zero = check_zero_convergence(&mesh_trace, 30);
    assert!(
        mesh_zero.passed,
        "mesh strategy should not produce zero-convergence: {}",
        mesh_zero.actual
    );
    let chain_zero = check_zero_convergence(&chain_trace, 30);
    assert!(
        chain_zero.passed,
        "chain strategy should not produce zero-convergence: {}",
        chain_zero.actual
    );

    // Accuracy spread across strategies ≤ 0.5
    let accuracies = [star_acc, mesh_acc, chain_acc];
    let min_acc = accuracies.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_acc = accuracies.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let spread = max_acc - min_acc;
    assert!(
        spread <= 0.5,
        "accuracy spread across strategies should be <= 0.5, got {:.3} (star={:.3}, mesh={:.3}, chain={:.3})",
        spread, star_acc, mesh_acc, chain_acc
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 12. Mid-deploy compound fault recovery
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn mid_deploy_compound_fault_recovery() {
    // Class: Mid-deploy fault recovery patterns.
    // 5 nodes, all deferred, compound faults during join window:
    // - Round 5: 80% drop rate + first introduction attempt (most messages lost)
    // - Round 10: Seed (node 0) killed
    // - Round 15: Partition home/office groups
    // - Round 20: Seed revived
    // - Round 25: All faults healed (drop=0, partition healed)
    // - Round 30: Full re-introduction of all 10 pairs (deploy retry)
    let config = DistributionSimConfig {
        name: "compound-fault-recovery".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        topology: Some(NetworkTopology {
            locations: vec![
                NodeLocation::Public,
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Nat { group: "office".into() },
                NodeLocation::Nat { group: "office".into() },
            ],
            relay_nodes: vec![0],
        }),
        deferred_join: vec![0, 1, 2, 3, 4],
        network_faults: vec![
            NetworkFault::SetDropRate { round: 5, rate: 0.8 },
            NetworkFault::Partition {
                round: 15,
                partition: Partition {
                    side_a: vec![1, 2],
                    side_b: vec![3, 4],
                    asymmetric: false,
                },
            },
            NetworkFault::SetDropRate { round: 25, rate: 0.0 },
            NetworkFault::Heal { round: 25 },
        ],
        kill_schedule: vec![(10, 0)],
        revive_schedule: vec![(20, 0)],
        action_schedule: vec![
            // First introduction attempt during 80% drop window
            (5, SimAction::Introduce { node_a: 0, node_b: 1 }),
            (5, SimAction::Introduce { node_a: 0, node_b: 2 }),
            (5, SimAction::Introduce { node_a: 0, node_b: 3 }),
            (5, SimAction::Introduce { node_a: 0, node_b: 4 }),
            (5, SimAction::Introduce { node_a: 1, node_b: 2 }),
            (5, SimAction::Introduce { node_a: 1, node_b: 3 }),
            (5, SimAction::Introduce { node_a: 1, node_b: 4 }),
            (5, SimAction::Introduce { node_a: 2, node_b: 3 }),
            (5, SimAction::Introduce { node_a: 2, node_b: 4 }),
            (5, SimAction::Introduce { node_a: 3, node_b: 4 }),
            // Full re-introduction after all faults healed (deploy retry)
            (30, SimAction::Introduce { node_a: 0, node_b: 1 }),
            (30, SimAction::Introduce { node_a: 0, node_b: 2 }),
            (30, SimAction::Introduce { node_a: 0, node_b: 3 }),
            (30, SimAction::Introduce { node_a: 0, node_b: 4 }),
            (30, SimAction::Introduce { node_a: 1, node_b: 2 }),
            (30, SimAction::Introduce { node_a: 1, node_b: 3 }),
            (30, SimAction::Introduce { node_a: 1, node_b: 4 }),
            (30, SimAction::Introduce { node_a: 2, node_b: 3 }),
            (30, SimAction::Introduce { node_a: 2, node_b: 4 }),
            (30, SimAction::Introduce { node_a: 3, node_b: 4 }),
        ],
        ..default_config()
    };

    let trace = run_simulation(config);
    let metrics = analyze(&trace);

    // Not stuck at zero after recovery (primary "permanent degradation" check)
    let zero_check = check_zero_convergence(&trace, 35);
    assert!(
        zero_check.passed,
        "cluster should not be stuck at zero after compound fault recovery: {}",
        zero_check.actual
    );

    // ≥ 75% accuracy after fault recovery
    let acc = check_membership_accuracy(&metrics, 0.75);
    assert!(
        acc.passed,
        "cluster should reach >= 75% accuracy after fault recovery: {}",
        acc.actual
    );

    // All 5 nodes alive at end
    let last_round = trace.snapshots_per_round.last().unwrap();
    let alive_count = last_round.iter().filter(|(_, s)| s.is_alive).count();
    assert_eq!(
        alive_count, 5,
        "all 5 nodes should be alive at end, got {}",
        alive_count
    );

    // Global convergence (tolerance=1) achieved by round 60
    let global_conv = check_convergence(&trace, 60, 1);
    assert!(
        global_conv.passed,
        "global convergence should be achieved by round 60: {}",
        global_conv.actual
    );

    // Both home and office groups converge independently by round 60
    let home_conv = check_group_convergence(&trace, &[1, 2], 60, 0);
    assert!(
        home_conv.passed,
        "home group should converge by round 60: {}",
        home_conv.actual
    );
    let office_conv = check_group_convergence(&trace, &[3, 4], 60, 0);
    assert!(
        office_conv.passed,
        "office group should converge by round 60: {}",
        office_conv.actual
    );
}

// ════════════════════════════════════════════════════════════════════════════
// Bug-class regression: verify tests catch the actual deploy bugs
// ════════════════════════════════════════════════════════════════════════════

// ────────────────────────────────────────────────────────────────────────────
// 13. Bug replay: deploy script sends join_seed to the seed node itself
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn bug_replay_self_join_only_deploy_fails_to_converge() {
    // Replicates the real bug: deploy script sent `join_seed` to the seed node
    // itself (iroh rejected "Connecting to ourself"). The bug meant the seed
    // never learned about other nodes — it only tried to join itself.
    //
    // This test models a "buggy deploy" where the controller ONLY sends
    // self-joins (the bug) and never sends the correct cross-node introductions.
    // The test passes if the buggy deploy FAILS to converge — proving that
    // test 9's assertions would catch this class of bug.
    let config = DistributionSimConfig {
        name: "bug-replay-self-join".into(),
        num_nodes: 3,
        num_rounds: 60,
        ticks_per_round: 3,
        topology: Some(NetworkTopology {
            locations: vec![
                NodeLocation::Public,
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Nat { group: "home".into() },
            ],
            relay_nodes: vec![0],
        }),
        deferred_join: vec![0, 1, 2],
        action_schedule: vec![
            // Buggy deploy: only self-joins, never cross-node introductions
            (5, SimAction::Join { node_idx: 0, seed_idx: 0 }),
            (5, SimAction::Join { node_idx: 1, seed_idx: 1 }),
            (5, SimAction::Join { node_idx: 2, seed_idx: 2 }),
        ],
        ..default_config()
    };

    let trace = run_simulation(config);
    let metrics = analyze(&trace);

    // The buggy deploy MUST fail: nodes stuck at 0 members
    let zero_check = check_zero_convergence(&trace, 10);
    assert!(
        !zero_check.passed,
        "buggy self-join-only deploy should produce zero-convergence (stuck at 0 members), \
         but somehow nodes converged: {}",
        zero_check.actual
    );

    // Accuracy must be 0 — no node sees any other
    assert!(
        metrics.membership_accuracy < 0.5,
        "buggy self-join-only deploy should have < 50% accuracy, got {:.3}",
        metrics.membership_accuracy
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 14. Bug replay: firewall blocks relay port — NAT nodes silently isolated
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn bug_replay_firewall_blocks_relay_port_silent_isolation() {
    // Replicates the real bug: port 3340 blocked by firewall. All NAT nodes
    // couldn't reach the relay, so the cluster was stuck at 0 peers.
    //
    // This models a deploy where the firewall rule blocks the relay port BEFORE
    // any connections are established. NAT nodes attempt to join via the relay
    // but all traffic is dropped. The test passes if this scenario FAILS to
    // achieve full convergence — proving that test 10's assertions would
    // detect this failure mode.
    let config = DistributionSimConfig {
        name: "bug-replay-firewall".into(),
        num_nodes: 3,
        num_rounds: 60,
        ticks_per_round: 3,
        topology: Some(NetworkTopology {
            locations: vec![
                NodeLocation::Public,
                NodeLocation::Nat { group: "home".into() },
                NodeLocation::Nat { group: "home".into() },
            ],
            relay_nodes: vec![0],
        }),
        deferred_join: vec![0, 1, 2],
        network_faults: vec![
            // Firewall blocks all NAT↔relay traffic for the ENTIRE simulation
            NetworkFault::LinkFault { round: 1, from: 0, to: 1, rate: 1.0, bidirectional: true },
            NetworkFault::LinkFault { round: 1, from: 0, to: 2, rate: 1.0, bidirectional: true },
        ],
        action_schedule: vec![
            // Deploy script tries to introduce all pairs, but relay traffic is blocked
            (5, SimAction::Introduce { node_a: 0, node_b: 1 }),
            (5, SimAction::Introduce { node_a: 0, node_b: 2 }),
            (5, SimAction::Introduce { node_a: 1, node_b: 2 }),
        ],
        ..default_config()
    };

    let trace = run_simulation(config);
    let metrics = analyze(&trace);

    // Full accuracy MUST NOT be achieved — relay is the only path for NAT→Public,
    // and it's blocked. NAT nodes in the same group can still talk via LAN,
    // but nobody can reach the public relay node.
    assert!(
        metrics.membership_accuracy < 1.0,
        "firewall-blocked deploy should NOT achieve 100% accuracy, got {:.3}",
        metrics.membership_accuracy
    );

    // The LAN peers (nodes 1,2) should still see each other
    let home_conv = check_group_convergence(&trace, &[1, 2], 20, 0);
    assert!(
        home_conv.passed,
        "same-group NAT nodes should still converge via LAN: {}",
        home_conv.actual
    );

    // But relay node should be isolated — member_count == 0
    let last_round = trace.snapshots_per_round.last().unwrap();
    let relay_snap = &last_round[0].1;
    assert!(
        relay_snap.is_alive && relay_snap.member_count == 0,
        "relay node should be isolated with 0 members (firewall blocks all NAT traffic), got {}",
        relay_snap.member_count
    );
}
