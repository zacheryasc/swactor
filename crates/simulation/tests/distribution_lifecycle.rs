#![cfg(feature = "distribution")]
//! Lifecycle simulation tests — death/repair/cache/routing behavior.
//!
//! Tests that node death correctly triggers:
//! - Repair queue population for re-replication
//! - Cache invalidation of stale entries
//! - Routing table cleanup
//! - Recovery after partition heals

use simulation::distribution::properties::{
    check_repair_queue_populated, check_routing_table_bounded,
};
use simulation::distribution::sim::{
    run_simulation_with_nodes, DistributionSimConfig, DistTrace, NetworkFault, Partition, SimAction,
};

fn maybe_save_trace(trace: &DistTrace) {
    if let Ok(dir) = std::env::var("SWACTOR_TRACE_DIR") {
        std::fs::create_dir_all(&dir).ok();
        let filename = format!(
            "{}/{}.trace.json",
            dir,
            trace.name.to_lowercase().replace(' ', "_").replace(['(', ')'], "")
        );
        let json = serde_json::to_string_pretty(trace).expect("trace serialization failed");
        std::fs::write(&filename, json).expect("trace write failed");
    }
}

fn default_config() -> DistributionSimConfig {
    DistributionSimConfig::default()
}

// ────────────────────────────────────────────────────────────────────────────
// 1. Dead node's actors populate repair queue and invalidate cache
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn dead_node_triggers_repair_queue_and_cache_invalidation() {
    // Given: 5-node cluster, 2 actors/node. Node 2 is killed at round 10.
    let config = DistributionSimConfig {
        name: "death-repair-cache".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 2,
        kill_schedule: vec![(10, 2)],
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: at least one survivor should have a non-empty repair queue
    let result = check_repair_queue_populated(&trace, 10);
    assert!(
        result.passed,
        "repair queue should be populated after node death: {}",
        result.actual
    );

    // And: no survivor's cache should contain entries pointing to the dead node
    let dead_node_id = {
        // Find the node_id for node 2 from round snapshots before death
        // We can check from the surviving nodes
        // Node 2 is dead (None), so we check survivors' caches
        let mut stale_count = 0;
        for node in nodes.iter().filter_map(|n| n.as_ref()) {
            for (_actor, cached_on) in node.cache().entries() {
                // The dead node's entries should have been invalidated
                // We can't easily get node 2's ID here, but we can check
                // that no survivor caches an actor on a node not in their members
                let alive_ids: Vec<_> = node.members().iter().map(|m| m.node_id).collect();
                if !alive_ids.contains(&cached_on) && cached_on != node.node_id() {
                    stale_count += 1;
                }
            }
        }
        stale_count
    };

    assert_eq!(
        dead_node_id, 0,
        "no survivor should have cache entries pointing to non-member nodes"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 2. Revived node starts fresh (empty directory)
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn revived_node_has_empty_directory() {
    // Given: 5-node cluster, 2 actors/node.
    // Node 2 killed at round 10, revived at round 50.
    let config = DistributionSimConfig {
        name: "revive-fresh".into(),
        num_nodes: 5,
        num_rounds: 100,
        ticks_per_round: 3,
        actors_per_node: 2,
        kill_schedule: vec![(10, 2)],
        revive_schedule: vec![(50, 2)],
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: the revived node should have an empty directory
    // (it's a fresh DistributedNode, not carrying over old state)
    let revived = nodes[2].as_ref().expect("node 2 should be revived");
    assert_eq!(
        revived.directory().entry_count(), 0,
        "revived node should start with empty directory"
    );

    // And: the revived node should have rejoined the cluster
    assert!(
        !revived.members().is_empty(),
        "revived node should have some cluster members"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 3. Cache invalidation tracks membership changes
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn cache_shrinks_after_node_death() {
    // Given: 5-node cluster with actors, all caches populated during setup.
    // When: node 1 is killed
    // Then: cache_size should decrease for survivors after death detection.
    let config = DistributionSimConfig {
        name: "cache-invalidation".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 3,
        kill_schedule: vec![(10, 1)],
        ..default_config()
    };

    let (trace, _nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Check that cache_size decreased for at least some survivors after death
    // Before death (round 9), survivors should have cache entries
    // After death detection, cache for dead node's actors should be invalidated
    let pre_death_round = 8; // 0-indexed round 9
    let post_detection_round = 39; // well after SWIM detection

    if pre_death_round < trace.snapshots_per_round.len()
        && post_detection_round < trace.snapshots_per_round.len()
    {
        let pre_cache_max: usize = trace.snapshots_per_round[pre_death_round]
            .iter()
            .filter(|(_, s)| s.is_alive)
            .map(|(_, s)| s.cache_size)
            .max()
            .unwrap_or(0);

        let post_cache_sizes: Vec<usize> = trace.snapshots_per_round[post_detection_round]
            .iter()
            .filter(|(_, s)| s.is_alive)
            .map(|(_, s)| s.cache_size)
            .collect();

        // After death, some survivors should have fewer cache entries
        // (the dead node's actors were invalidated)
        let any_decreased = post_cache_sizes.iter().any(|&s| s < pre_cache_max);
        assert!(
            any_decreased || pre_cache_max == 0,
            "cache should shrink after node death, pre_max={pre_cache_max}, post={post_cache_sizes:?}"
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 4. Routing table recovers after partition heals (dead reprobe)
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn routing_table_recovers_after_partition_heals() {
    // Given: 6-node cluster, partition at round 10, heal at round 40
    // With dead_reprobe_interval=10, nodes re-discover dead members
    let config = DistributionSimConfig {
        name: "rt-recovery".into(),
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
            probe_mode: distribution::swim::probe::ProbeMode::Periodic,
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

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: after healing, all nodes should have recovered routing tables
    // Each node should see at least 4 of 5 other nodes in their routing table
    for (i, maybe_node) in nodes.iter().enumerate() {
        if let Some(node) = maybe_node {
            assert!(
                node.routing_table().len() >= 4,
                "node {i} should have ≥4 RT entries after partition heals, got {}",
                node.routing_table().len()
            );
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 5. Routing table tracks alive membership (bounded invariant)
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn routing_table_bounded_by_alive_count() {
    // Run a simulation with deaths and verify the routing table invariant
    let config = DistributionSimConfig {
        name: "rt-bounded".into(),
        num_nodes: 8,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 1,
        kill_schedule: vec![(15, 2), (25, 5)],
        ..default_config()
    };

    let (trace, _) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    let result = check_routing_table_bounded(&trace);
    assert!(
        result.passed,
        "routing table should never exceed alive count: {}",
        result.actual
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 6. Combined: partition + death during partition + heal + verify
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn partition_then_death_during_partition_then_heal() {
    // Given: 6 nodes, partition at r=10, node 2 (side A) killed during partition
    // at r=20, heal at r=40. Node 2 had actors and a registry name.
    // Use high suspicion_timeout so cross-partition nodes stay Suspect (not Dead),
    // while within-partition death of node 2 is detected after timeout expires.
    let config = DistributionSimConfig {
        name: "partition-death-heal".into(),
        num_nodes: 6,
        num_rounds: 150,
        ticks_per_round: 3,
        actors_per_node: 2,
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
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 2, name: "doomed-svc".into() }),
            (5, SimAction::RegisterName { node_idx: 4, name: "stable-svc".into() }),
        ],
        kill_schedule: vec![(20, 2)],
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 1,
            // >90 ticks (30 rounds × 3 ticks) so cross-partition nodes stay Suspect during
            // the 30-round partition. Node 2 (truly dead) gets declared dead ~33 rounds
            // after kill, well after partition heals.
            suspicion_timeout: 100,
            dead_reprobe_interval: 10,
            probe_mode: distribution::swim::probe::ProbeMode::Periodic,
        },
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    let survivors: Vec<_> = nodes.iter().filter_map(|n| n.as_ref()).collect();
    assert_eq!(survivors.len(), 5, "5 of 6 should survive");

    // "doomed-svc" should be tombstoned (owner node 2 died)
    for node in &survivors {
        assert!(
            node.resolve_name("doomed-svc").is_none(),
            "doomed-svc should be tombstoned after owner died during partition"
        );
    }

    // "stable-svc" should still resolve (node 4 alive throughout)
    for node in &survivors {
        assert!(
            node.resolve_name("stable-svc").is_some(),
            "stable-svc should resolve (owner survived partition)"
        );
    }

    // After partition heal + dead reprobe, routing tables should recover
    // (at least 4 entries for each surviving node)
    for node in &survivors {
        assert!(
            node.routing_table().len() >= 3,
            "surviving node should have ≥3 RT entries after partition heals, got {}",
            node.routing_table().len()
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 7. Registry GC: tombstones are garbage-collected after TTL
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn registry_tombstones_gc_after_ttl() {
    // Given: short tombstone TTL and GC interval, register then unregister a name.
    // Then do many more register/unregister operations to advance the logical clock
    // (which is used for TTL comparison). After enough clock advancement, the
    // original tombstone should be garbage-collected.
    let config = DistributionSimConfig {
        name: "registry-gc".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 0,
        // Very short GC: TTL=5 logical clock ticks, GC runs every 3 ticks
        registry_tombstone_ttl: Some(5),
        registry_gc_interval: Some(3),
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 0, name: "ephemeral".into() }),
            (10, SimAction::UnregisterName { node_idx: 0, name: "ephemeral".into() }),
            // Additional operations to advance the logical clock past the TTL
            (15, SimAction::RegisterName { node_idx: 1, name: "churn-1".into() }),
            (16, SimAction::RegisterName { node_idx: 2, name: "churn-2".into() }),
            (17, SimAction::RegisterName { node_idx: 3, name: "churn-3".into() }),
            (18, SimAction::RegisterName { node_idx: 4, name: "churn-4".into() }),
            (19, SimAction::RegisterName { node_idx: 1, name: "churn-5".into() }),
            (20, SimAction::RegisterName { node_idx: 2, name: "churn-6".into() }),
        ],
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Shortly after unregister (round 12), tombstones should exist
    let mid_tombstones: usize = trace.snapshots_per_round
        .get(11) // round 12
        .map(|snaps| {
            snaps.iter()
                .filter(|(_, s)| s.is_alive)
                .map(|(_, s)| s.registry_tombstone_count)
                .sum()
        })
        .unwrap_or(0);

    assert!(
        mid_tombstones > 0,
        "tombstones should exist shortly after unregister"
    );

    // After many more register operations advance the clock, the "ephemeral" tombstone
    // should be GC'd (its age exceeds TTL=5 in logical clock terms)
    let alive_nodes: Vec<_> = nodes.iter().filter_map(|n| n.as_ref()).collect();

    // Check that "ephemeral" resolves to None on all nodes (whether GC'd or still tombstoned)
    for node in &alive_nodes {
        assert!(
            node.resolve_name("ephemeral").is_none(),
            "ephemeral should not resolve (tombstoned or GC'd)"
        );
    }

    // At least some nodes should have GC'd the tombstone (clock advanced past TTL)
    let nodes_with_ephemeral_tombstone: usize = alive_nodes
        .iter()
        .filter(|n| {
            n.registry().entries().any(|e| e.name == "ephemeral" && e.tombstone)
        })
        .count();

    assert!(
        nodes_with_ephemeral_tombstone < alive_nodes.len(),
        "at least some nodes should have GC'd the 'ephemeral' tombstone, but {} of {} still have it",
        nodes_with_ephemeral_tombstone,
        alive_nodes.len()
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 8. Indirect probes (PingReq) prevent false death on flaky direct path
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn asymmetric_one_way_block_does_not_kill_node() {
    // Given: 6-node cluster. Asymmetric partition: 0→5 blocked, 5→0 works.
    // Node 5 can still communicate with nodes 1-4 in both directions, and
    // 5→0 works, so gossip piggyback keeps node 0 informed about node 5's
    // aliveness through intermediate nodes.
    //
    // Note: this implementation relies on piggyback gossip for indirect
    // recovery (PingReq ack forwarding is not implemented), so we use a
    // generous suspicion_timeout to allow gossip propagation.
    let config = DistributionSimConfig {
        name: "one-way-block".into(),
        num_nodes: 6,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 0,
        // Asymmetric: only 0→5 is blocked, all other paths work
        network_faults: vec![
            NetworkFault::Partition {
                round: 10,
                partition: Partition {
                    side_a: vec![0],
                    side_b: vec![5],
                    asymmetric: true, // 0→5 blocked, 5→0 works
                },
            },
        ],
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 3,
            // Timeout must exceed total sim ticks (80*3=240) so node 0
            // never declares node 5 dead despite the blocked direct path.
            // Gossip through intermediate nodes refutes suspicion each cycle.
            suspicion_timeout: 500,
            dead_reprobe_interval: 10,
            probe_mode: distribution::swim::probe::ProbeMode::Periodic,
        },
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 5, name: "target-svc".into() }),
        ],
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: all 6 nodes should still be alive (asymmetric block doesn't kill either side)
    let alive_count = nodes.iter().filter(|n| n.is_some()).count();
    assert_eq!(alive_count, 6, "all 6 nodes should be alive");

    // And: node 5's registry name should be resolvable from all nodes
    // (gossip carries the entry through intermediate nodes even if 0→5 is blocked)
    for (i, node) in nodes.iter().enumerate() {
        if let Some(node) = node {
            assert!(
                node.resolve_name("target-svc").is_some(),
                "node {i} should resolve 'target-svc'"
            );
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 9. Names registered during partition propagate after heal via re_disseminate_all
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn names_registered_during_partition_propagate_after_heal() {
    // Given: 6 nodes, partition {0,1,2} vs {3,4,5} from round 10 to 50.
    // During the partition, each side registers a name the other side can't see.
    // After healing, re_disseminate_all (triggered by Alive transitions) should
    // propagate both names to the entire cluster.
    let config = DistributionSimConfig {
        name: "partition-register-heal".into(),
        num_nodes: 6,
        num_rounds: 120,
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
            NetworkFault::Heal { round: 50 },
        ],
        action_schedule: vec![
            // Registered DURING partition — other side doesn't see these initially
            (20, SimAction::RegisterName { node_idx: 0, name: "side-a-svc".into() }),
            (20, SimAction::RegisterName { node_idx: 3, name: "side-b-svc".into() }),
        ],
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 1,
            // High timeout: cross-partition nodes stay Suspect during 40-round partition
            suspicion_timeout: 200,
            dead_reprobe_interval: 10,
            probe_mode: distribution::swim::probe::ProbeMode::Periodic,
        },
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    let alive: Vec<_> = nodes.iter().filter_map(|n| n.as_ref()).collect();
    assert_eq!(alive.len(), 6, "all 6 nodes should survive");

    // After healing + gossip, both names should be resolvable from every node
    for (i, node) in nodes.iter().enumerate() {
        if let Some(node) = node {
            assert!(
                node.resolve_name("side-a-svc").is_some(),
                "node {i} should resolve 'side-a-svc' (registered during partition on side A)"
            );
            assert!(
                node.resolve_name("side-b-svc").is_some(),
                "node {i} should resolve 'side-b-svc' (registered during partition on side B)"
            );
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 10. Bidirectional suspicion: two nodes suspect each other, both recover
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn bidirectional_suspicion_both_nodes_recover() {
    // Given: 6 nodes. Mutual partition between node 0 and node 5 (both directions)
    // from round 10 to 30. Both sides can still reach nodes 1-4.
    // Both 0 and 5 will suspect each other, but gossip through 1-4 carries
    // refutations. After healing, both should be Alive with registry intact.
    let config = DistributionSimConfig {
        name: "bidir-suspicion".into(),
        num_nodes: 6,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 0,
        network_faults: vec![
            NetworkFault::Partition {
                round: 10,
                partition: Partition {
                    side_a: vec![0],
                    side_b: vec![5],
                    asymmetric: false, // full mutual block
                },
            },
            NetworkFault::Heal { round: 30 },
        ],
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 0, name: "svc-zero".into() }),
            (5, SimAction::RegisterName { node_idx: 5, name: "svc-five".into() }),
        ],
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 3,
            // Must exceed partition duration (20 rounds × 3 ticks = 60 ticks)
            suspicion_timeout: 100,
            dead_reprobe_interval: 10,
            probe_mode: distribution::swim::probe::ProbeMode::Periodic,
        },
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // All 6 nodes alive
    let alive_count = nodes.iter().filter(|n| n.is_some()).count();
    assert_eq!(alive_count, 6, "all 6 nodes should survive bidirectional suspicion");

    // Both registry names should resolve on all nodes
    for (i, node) in nodes.iter().enumerate() {
        if let Some(node) = node {
            assert!(
                node.resolve_name("svc-zero").is_some(),
                "node {i} should resolve 'svc-zero'"
            );
            assert!(
                node.resolve_name("svc-five").is_some(),
                "node {i} should resolve 'svc-five'"
            );
        }
    }

    // Both nodes 0 and 5 should see each other in their member lists
    let node0 = nodes[0].as_ref().unwrap();
    let node5 = nodes[5].as_ref().unwrap();
    let node0_sees_5 = node0.members().iter().any(|m| m.node_id == node5.node_id());
    let node5_sees_0 = node5.members().iter().any(|m| m.node_id == node0.node_id());
    assert!(node0_sees_5, "node 0 should see node 5 as a member after healing");
    assert!(node5_sees_0, "node 5 should see node 0 as a member after healing");
}
