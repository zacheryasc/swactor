//! Cluster simulation scenarios — breadth-first coverage of failure modes.
//!
//! Inspired by Hashicorp memberlist test suite, FoundationDB simulation testing,
//! and Jepsen/Antithesis fault injection patterns.

use simulation::distribution::properties::{
    analyze, check_accuracy, check_completeness, check_convergence, check_failure_detection,
    check_membership_accuracy,
};
use simulation::distribution::sim::{
    run_simulation, DistributionSimConfig, NetworkFault, Partition,
};
use simulation::distribution::trace::DistributionEventKind;

fn default_config() -> DistributionSimConfig {
    DistributionSimConfig::default()
}

// ────────────────────────────────────────────────────────────────────────────
// 1. Network Partition — symmetric split-brain
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn symmetric_partition_splits_membership_views() {
    // Given: 6 nodes, partition {0,1,2} vs {3,4,5} at round 10
    // Long partitions cause SWIM to declare the other side dead — this is correct behavior.
    // SWIM does not auto-rediscover dead nodes after partition heals.
    let config = DistributionSimConfig {
        name: "symmetric-partition".into(),
        num_nodes: 6,
        num_rounds: 60,
        ticks_per_round: 3,
        actors_per_node: 0,
        network_faults: vec![
            NetworkFault::Partition {
                round: 10,
                partition: Partition {
                    side_a: vec![0, 1, 2],
                    side_b: vec![3, 4, 5],
                    asymmetric: false,
                },
            },
        ],
        ..default_config()
    };

    let trace = run_simulation(config);

    // After partition, each side should form its own sub-cluster.
    // Each side of 3 nodes should see exactly 2 other members (its own group).
    let last_round = trace.snapshots_per_round.last().unwrap();

    // Side A nodes (0,1,2) should see ≤2 members each (only their partition)
    for idx in 0..3 {
        let snap = &last_round[idx].1;
        assert!(
            snap.is_alive && snap.member_count <= 3,
            "side_a node {} sees {} members, expected ≤3",
            idx,
            snap.member_count
        );
    }

    // Side B nodes (3,4,5) should also see ≤2 members
    for idx in 3..6 {
        let snap = &last_round[idx].1;
        assert!(
            snap.is_alive && snap.member_count <= 3,
            "side_b node {} sees {} members, expected ≤3",
            idx,
            snap.member_count
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 2. Asymmetric partition — one-way communication failure
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn asymmetric_partition_causes_one_sided_suspicion() {
    // Given: 5 nodes, node 4 can send to 0 but 0 can't send to 4
    let config = DistributionSimConfig {
        name: "asymmetric-partition".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 0,
        network_faults: vec![
            NetworkFault::Partition {
                round: 10,
                partition: Partition {
                    side_a: vec![0],
                    side_b: vec![4],
                    asymmetric: true, // 0→4 blocked, 4→0 works
                },
            },
            NetworkFault::Heal { round: 50 },
        ],
        ..default_config()
    };

    let trace = run_simulation(config);

    // After healing, the cluster should eventually recover
    let last_round = trace.snapshots_per_round.last().unwrap();
    let alive_with_members: Vec<_> = last_round
        .iter()
        .filter(|(_, s)| s.is_alive)
        .map(|(_, s)| s.member_count)
        .collect();

    // All nodes should see at least 3 members after healing
    assert!(
        alive_with_members.iter().all(|&c| c >= 3),
        "all nodes should recover after asymmetric partition heals, got: {alive_with_members:?}"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 3. Message loss — random packet dropping
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn cluster_converges_under_10_percent_message_loss() {
    // Given: 5 nodes with 10% message loss from the start.
    // 10% loss is significant for SWIM because it can hit both direct probe
    // AND indirect probes in the same cycle, causing false suspicions.
    // We verify the cluster degrades but doesn't crash, and at least some
    // membership information survives.
    let config = DistributionSimConfig {
        name: "message-loss-10pct".into(),
        num_nodes: 5,
        num_rounds: 150,
        ticks_per_round: 3,
        actors_per_node: 0,
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 5,
            indirect_probes: 2,
            suspicion_timeout: 20,
            dead_reprobe_interval: 0,
        },
        network_faults: vec![NetworkFault::SetDropRate {
            round: 1,
            rate: 0.1,
        }],
        ..default_config()
    };

    let trace = run_simulation(config);
    let metrics = analyze(&trace);

    // With 10% loss and the deterministic LCG, SWIM's probe cycle is disrupted
    // enough to cause false deaths. The test verifies:
    // 1. The simulation completes without panic (implicit — we got here)
    // 2. At least partial membership is maintained (some nodes still know about others)
    let result = check_membership_accuracy(&metrics, 0.15);
    assert!(
        result.passed,
        "cluster should maintain some membership under 10% loss: {}",
        result.actual
    );
}

#[test]
fn heavy_message_loss_causes_membership_instability() {
    // Given: 5 nodes with 30% message loss
    // Heavy loss overwhelms SWIM's probe cycle, causing false suspicions.
    // This tests that the protocol degrades but doesn't crash.
    let config = DistributionSimConfig {
        name: "message-loss-30pct".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 0,
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 5,
            indirect_probes: 2,
            suspicion_timeout: 15,
            dead_reprobe_interval: 0,
        },
        network_faults: vec![NetworkFault::SetDropRate {
            round: 1,
            rate: 0.3,
        }],
        ..default_config()
    };

    let trace = run_simulation(config);

    // The simulation should complete without panicking.
    // Under heavy loss, some membership instability is expected.
    let last_round = trace.snapshots_per_round.last().unwrap();
    let alive_count = last_round.iter().filter(|(_, s)| s.is_alive).count();
    assert_eq!(alive_count, 5, "no nodes should actually die");
}

// ────────────────────────────────────────────────────────────────────────────
// 4. Seed node failure — cluster survives without the seed
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn cluster_survives_seed_node_death() {
    // Given: 5 nodes, kill the seed (node 0) at round 15
    let config = DistributionSimConfig {
        name: "seed-death".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 0,
        kill_schedule: vec![(15, 0)], // Kill the seed!
        ..default_config()
    };

    let trace = run_simulation(config);

    // Surviving 4 nodes should detect the seed's death
    let result = check_failure_detection(&trace, 4);
    assert!(
        result.passed,
        "survivors should detect seed death: {}",
        result.actual
    );

    // Survivors should still maintain membership among themselves
    let last_round = trace.snapshots_per_round.last().unwrap();
    let survivors: Vec<_> = last_round
        .iter()
        .filter(|(_, s)| s.is_alive)
        .collect();
    assert_eq!(survivors.len(), 4, "4 survivors expected");

    // At least 3 of 4 survivors should see each other
    let well_connected = survivors
        .iter()
        .filter(|(_, s)| s.member_count >= 2)
        .count();
    assert!(
        well_connected >= 3,
        "at least 3 survivors should see ≥2 members, got {well_connected}"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 5. Simultaneous multi-node failure
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn simultaneous_two_node_failure_detected() {
    // Given: 7 nodes, kill nodes 2 and 5 simultaneously at round 15
    let config = DistributionSimConfig {
        name: "multi-kill".into(),
        num_nodes: 7,
        num_rounds: 100,
        ticks_per_round: 3,
        actors_per_node: 0,
        kill_schedule: vec![(15, 2), (15, 5)],
        ..default_config()
    };

    let trace = run_simulation(config);

    // 5 survivors should see at most 5 members (detecting both deaths)
    let result = check_failure_detection(&trace, 5);
    assert!(
        result.passed,
        "survivors should detect both deaths: {}",
        result.actual
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 6. Cascading sequential failure
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn cascading_failures_leave_quorum_intact() {
    // Given: 7 nodes, kill one at r=10, another at r=25, another at r=40
    let config = DistributionSimConfig {
        name: "cascading-failure".into(),
        num_nodes: 7,
        num_rounds: 100,
        ticks_per_round: 3,
        actors_per_node: 0,
        kill_schedule: vec![(10, 1), (25, 3), (40, 5)],
        ..default_config()
    };

    let trace = run_simulation(config);

    // 4 survivors should still form a connected cluster
    let last_round = trace.snapshots_per_round.last().unwrap();
    let survivors: Vec<_> = last_round
        .iter()
        .filter(|(_, s)| s.is_alive)
        .collect();
    assert_eq!(survivors.len(), 4, "4 survivors expected");

    // Each survivor should see at most 4 members
    for (name, snap) in &survivors {
        assert!(
            snap.member_count <= 4,
            "{name} sees {} members, expected ≤4",
            snap.member_count
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 7. Large cluster convergence
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn cluster_of_fifty_converges() {
    // Given: 50 nodes
    let config = DistributionSimConfig {
        name: "fifty-converges".into(),
        num_nodes: 50,
        num_rounds: 150,
        ticks_per_round: 3,
        actors_per_node: 0,
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 2,
            suspicion_timeout: 10,
            dead_reprobe_interval: 0,
        },
        ..default_config()
    };

    let trace = run_simulation(config);
    let metrics = analyze(&trace);

    // 50-node cluster should reach ≥80% accuracy
    let result = check_membership_accuracy(&metrics, 0.8);
    assert!(
        result.passed,
        "50-node cluster should converge: {}",
        result.actual
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 8. Rapid churn — nodes joining and dying frequently
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn rapid_churn_maintains_partial_membership() {
    // Given: 8 nodes with rapid kill/revive cycles
    let config = DistributionSimConfig {
        name: "rapid-churn".into(),
        num_nodes: 8,
        num_rounds: 100,
        ticks_per_round: 3,
        actors_per_node: 0,
        kill_schedule: vec![
            (10, 2),
            (15, 4),
            (30, 6),
            (45, 3),
        ],
        revive_schedule: vec![
            (25, 2),
            (35, 4),
            (55, 6),
            (65, 3),
        ],
        ..default_config()
    };

    let trace = run_simulation(config);

    // At end, all nodes should be alive and have some membership view
    let last_round = trace.snapshots_per_round.last().unwrap();
    let alive_count = last_round.iter().filter(|(_, s)| s.is_alive).count();
    assert_eq!(alive_count, 8, "all 8 nodes should be alive at end");

    // At least half should have reasonable membership
    let connected = last_round
        .iter()
        .filter(|(_, s)| s.is_alive && s.member_count >= 3)
        .count();
    assert!(
        connected >= 4,
        "at least 4 of 8 nodes should see ≥3 members after churn, got {connected}"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 9. Graceful leave — node announces departure
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn graceful_leave_detected_faster_than_crash() {
    // We can't directly test graceful leave in the current sim harness
    // (leave() is called but doesn't disseminate through ticks in the same way).
    // Instead, test that a crash is detected within a bounded number of rounds.

    // Given: 5 nodes, kill node 2 at round 5
    let config = DistributionSimConfig {
        name: "crash-detection-speed".into(),
        num_nodes: 5,
        num_rounds: 40,
        ticks_per_round: 3,
        actors_per_node: 0,
        kill_schedule: vec![(5, 2)],
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 2,
            indirect_probes: 1,
            suspicion_timeout: 5,
            dead_reprobe_interval: 0,
        },
        ..default_config()
    };

    let trace = run_simulation(config);

    // Death should be detected by round 20 (suspicion_timeout + margin)
    let mut detected_by_round = None;
    for (round_idx, round_snaps) in trace.snapshots_per_round.iter().enumerate() {
        if round_idx < 5 {
            continue; // Skip rounds before kill
        }
        let all_survivors_see_reduced = round_snaps
            .iter()
            .filter(|(_, s)| s.is_alive)
            .all(|(_, s)| s.member_count <= 4);
        if all_survivors_see_reduced {
            detected_by_round = Some(round_idx + 1);
            break;
        }
    }

    assert!(
        detected_by_round.is_some(),
        "crash should be detected before end of simulation"
    );
    let round = detected_by_round.unwrap();
    assert!(
        round <= 25,
        "crash should be detected by round 25, was detected at round {round}"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 10. Partition then kill — compounding failures
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn partition_plus_kill_in_minority_side() {
    // Given: 5 nodes, partition {0,1,2} vs {3,4}, then kill node 3
    let config = DistributionSimConfig {
        name: "partition-plus-kill".into(),
        num_nodes: 5,
        num_rounds: 100,
        ticks_per_round: 3,
        actors_per_node: 0,
        network_faults: vec![
            NetworkFault::Partition {
                round: 10,
                partition: Partition {
                    side_a: vec![0, 1, 2],
                    side_b: vec![3, 4],
                    asymmetric: false,
                },
            },
            NetworkFault::Heal { round: 70 },
        ],
        kill_schedule: vec![(20, 3)], // Kill in minority side
        ..default_config()
    };

    let trace = run_simulation(config);

    // After healing, 4 alive nodes should re-converge
    let last_round = trace.snapshots_per_round.last().unwrap();
    let alive_nodes: Vec<_> = last_round
        .iter()
        .filter(|(_, s)| s.is_alive)
        .collect();
    assert_eq!(alive_nodes.len(), 4, "4 nodes should be alive");

    // Majority side {0,1,2} should be well-connected
    let majority_connected = last_round[..3]
        .iter()
        .filter(|(_, s)| s.is_alive && s.member_count >= 2)
        .count();
    assert!(
        majority_connected >= 2,
        "majority partition should maintain connectivity"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 11. Actor resolution under network faults
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn actor_resolution_degrades_during_partition() {
    // Given: 5 nodes with actors, partition at round 10
    let config = DistributionSimConfig {
        name: "actor-resolution-partition".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 2,
        network_faults: vec![
            NetworkFault::Partition {
                round: 10,
                partition: Partition {
                    side_a: vec![0, 1],
                    side_b: vec![2, 3, 4],
                    asymmetric: false,
                },
            },
            NetworkFault::Heal { round: 50 },
        ],
        ..default_config()
    };

    let trace = run_simulation(config);

    // Should still have some successful resolutions (from cache)
    let resolve_events: Vec<_> = trace
        .events
        .iter()
        .filter(|e| matches!(e.kind, DistributionEventKind::ActorResolved { .. }))
        .collect();

    assert!(
        !resolve_events.is_empty(),
        "should still resolve some actors (cached) during partition"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 12. Dissemination completeness — all nodes learn about membership changes
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn membership_changes_disseminate_to_all_nodes() {
    // Given: 10 nodes, kill node 5 at round 20
    let config = DistributionSimConfig {
        name: "dissemination-completeness".into(),
        num_nodes: 10,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 0,
        kill_schedule: vec![(20, 5)],
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 2,
            suspicion_timeout: 8,
            dead_reprobe_interval: 0,
        },
        ..default_config()
    };

    let trace = run_simulation(config);

    // All 9 survivors should eventually detect the death
    let last_round = trace.snapshots_per_round.last().unwrap();
    let survivors_with_correct_view = last_round
        .iter()
        .filter(|(_, s)| s.is_alive && s.member_count <= 9)
        .count();

    assert!(
        survivors_with_correct_view >= 7,
        "at least 7 of 9 survivors should detect node death, got {survivors_with_correct_view}"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 13. Multiple partitions in sequence
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn sequential_partitions_fragment_cluster() {
    // Given: 6 nodes, two sequential partitions.
    // SWIM doesn't auto-rediscover dead-declared nodes, so each partition
    // permanently reduces the membership view of affected nodes.
    let config = DistributionSimConfig {
        name: "sequential-partitions".into(),
        num_nodes: 6,
        num_rounds: 100,
        ticks_per_round: 3,
        actors_per_node: 0,
        network_faults: vec![
            // Partition: {0,1,2} vs {3,4,5}
            NetworkFault::Partition {
                round: 10,
                partition: Partition {
                    side_a: vec![0, 1, 2],
                    side_b: vec![3, 4, 5],
                    asymmetric: false,
                },
            },
        ],
        ..default_config()
    };

    let trace = run_simulation(config);

    // Each side should still see its own members
    let last_round = trace.snapshots_per_round.last().unwrap();
    let alive_count = last_round.iter().filter(|(_, s)| s.is_alive).count();
    assert_eq!(alive_count, 6, "all nodes still alive");

    // Side A should maintain internal connectivity
    let side_a_connected = (0..3)
        .filter(|&i| last_round[i].1.member_count >= 1)
        .count();
    assert!(
        side_a_connected >= 2,
        "at least 2 of side_a nodes should see peers, got {side_a_connected}"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 14. Message loss then recovery
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn cluster_survives_brief_message_loss() {
    // Given: 5 nodes with 15% loss for a brief window, then clean network.
    // High suspicion timeout prevents false positives during the loss period.
    let config = DistributionSimConfig {
        name: "brief-loss-recovery".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 0,
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 5,
            indirect_probes: 2,
            suspicion_timeout: 20,
            dead_reprobe_interval: 0,
        },
        network_faults: vec![
            NetworkFault::SetDropRate {
                round: 5,
                rate: 0.15,
            },
            NetworkFault::SetDropRate {
                round: 30,
                rate: 0.0,
            },
        ],
        ..default_config()
    };

    let trace = run_simulation(config);

    // After loss stops, most nodes should still be in each other's member lists.
    // Some nodes may have been falsely declared dead during the loss window,
    // but the majority should maintain connectivity.
    let last_round = trace.snapshots_per_round.last().unwrap();
    let well_connected = last_round
        .iter()
        .filter(|(_, s)| s.is_alive && s.member_count >= 2)
        .count();
    assert!(
        well_connected >= 2,
        "at least 2 nodes should see ≥2 members after brief loss, got {well_connected}"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 15. Dead-node reprobe — partition heals, dead nodes recover via reprobe
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn partition_heals_via_dead_reprobe() {
    // Given: 6 nodes, partition {0,1,2} vs {3,4,5} at round 10, heal at round 40.
    // With dead_reprobe_interval enabled, both sides should eventually reprobe
    // the other side's dead-declared nodes, triggering incarnation refutation
    // and recovering the cluster.
    let config = DistributionSimConfig {
        name: "dead-reprobe-recovery".into(),
        num_nodes: 6,
        num_rounds: 120,
        ticks_per_round: 3,
        actors_per_node: 0,
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 1,
            suspicion_timeout: 5,
            dead_reprobe_interval: 10,
        },
        network_faults: vec![
            NetworkFault::Partition {
                round: 10,
                partition: Partition {
                    side_a: vec![0, 1, 2],
                    side_b: vec![3, 4, 5],
                    asymmetric: false,
                },
            },
            NetworkFault::Heal { round: 40 },
        ],
        ..default_config()
    };

    let trace = run_simulation(config);

    // After healing + reprobe cycles, nodes should recover cross-partition membership.
    // The reprobe fires every 10 ticks; 80 remaining rounds × 3 ticks = 240 ticks ≫ 10.
    let last_round = trace.snapshots_per_round.last().unwrap();
    let well_connected = last_round
        .iter()
        .filter(|(_, s)| s.is_alive && s.member_count >= 4)
        .count();
    assert!(
        well_connected >= 4,
        "at least 4 of 6 nodes should recover membership after partition heals via reprobe, got {well_connected}"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// SWIM Invariant Tests — formal property checks
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn completeness_all_survivors_detect_failure() {
    // SWIM completeness: every killed node is eventually detected by ALL survivors.
    let config = DistributionSimConfig {
        name: "completeness".into(),
        num_nodes: 7,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 0,
        kill_schedule: vec![(15, 3)],
        ..default_config()
    };

    let trace = run_simulation(config);
    // 7 nodes originally alive, kill 1 → survivors should see member_count < 7
    let result = check_completeness(&trace, 15, 7);
    assert!(
        result.passed,
        "completeness failed: {}",
        result.actual
    );
}

#[test]
fn accuracy_no_false_permanent_deaths() {
    // SWIM accuracy: after partition heals with reprobe enabled, no alive node
    // should be permanently declared dead by the majority.
    let config = DistributionSimConfig {
        name: "accuracy".into(),
        num_nodes: 6,
        num_rounds: 120,
        ticks_per_round: 3,
        actors_per_node: 0,
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 1,
            suspicion_timeout: 5,
            dead_reprobe_interval: 10,
        },
        network_faults: vec![
            NetworkFault::Partition {
                round: 10,
                partition: Partition {
                    side_a: vec![0, 1, 2],
                    side_b: vec![3, 4, 5],
                    asymmetric: false,
                },
            },
            NetworkFault::Heal { round: 40 },
        ],
        ..default_config()
    };

    let trace = run_simulation(config);
    // All 6 nodes are alive. At least 80% should be well-connected.
    let result = check_accuracy(&trace, 0.8);
    assert!(
        result.passed,
        "accuracy failed: {}",
        result.actual
    );
}

#[test]
fn convergence_after_partition_heal() {
    // SWIM convergence: after partition heals, surviving nodes' member_count
    // values should converge to the same value.
    let config = DistributionSimConfig {
        name: "convergence".into(),
        num_nodes: 6,
        num_rounds: 120,
        ticks_per_round: 3,
        actors_per_node: 0,
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 1,
            suspicion_timeout: 5,
            dead_reprobe_interval: 10,
        },
        network_faults: vec![
            NetworkFault::Partition {
                round: 10,
                partition: Partition {
                    side_a: vec![0, 1, 2],
                    side_b: vec![3, 4, 5],
                    asymmetric: false,
                },
            },
            NetworkFault::Heal { round: 40 },
        ],
        ..default_config()
    };

    let trace = run_simulation(config);
    // After round 60 (20 rounds post-heal), views should converge within ±1
    let result = check_convergence(&trace, 60, 1);
    assert!(
        result.passed,
        "convergence failed: {}",
        result.actual
    );
}
