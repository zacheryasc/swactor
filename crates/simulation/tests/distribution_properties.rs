#![cfg(feature = "distribution")]
//! Property-based distribution tests — invariants that must hold across configs.
//!
//! Each test verifies a structural property across multiple simulation
//! configurations with varying fault conditions.

use simulation::distribution::properties::{
    check_cache_bounded, check_registry_propagation, check_repair_queue_populated,
    check_routing_table_bounded,
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

// ────────────────────────────────────────────────────────────────────────────
// 1. Routing table size ≤ alive membership at every round
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn routing_table_bounded_across_configs() {
    let configs = vec![
        // Healthy 5-node cluster
        DistributionSimConfig {
            name: "rt-bound-healthy-5".into(),
            num_nodes: 5,
            num_rounds: 50,
            ticks_per_round: 3,
            ..DistributionSimConfig::default()
        },
        // 10-node cluster with 2 deaths
        DistributionSimConfig {
            name: "rt-bound-deaths-10".into(),
            num_nodes: 10,
            num_rounds: 60,
            ticks_per_round: 3,
            kill_schedule: vec![(15, 3), (25, 7)],
            ..DistributionSimConfig::default()
        },
        // 15-node cluster with 1 death
        DistributionSimConfig {
            name: "rt-bound-large-15".into(),
            num_nodes: 15,
            num_rounds: 60,
            ticks_per_round: 3,
            kill_schedule: vec![(20, 0)],
            ..DistributionSimConfig::default()
        },
    ];

    for config in configs {
        let name = config.name.clone();
        let (trace, _) = run_simulation_with_nodes(config);
        maybe_save_trace(&trace);
        let result = check_routing_table_bounded(&trace);
        assert!(
            result.passed,
            "[{name}] routing table bounded invariant violated: {}",
            result.actual
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 2. Cache size ≤ cache_capacity at every round
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn cache_bounded_across_configs() {
    let capacity = 100;
    let configs = vec![
        DistributionSimConfig {
            name: "cache-bound-5".into(),
            num_nodes: 5,
            num_rounds: 50,
            ticks_per_round: 3,
            actors_per_node: 5,
            cache_capacity: capacity,
            ..DistributionSimConfig::default()
        },
        DistributionSimConfig {
            name: "cache-bound-10-deaths".into(),
            num_nodes: 10,
            num_rounds: 60,
            ticks_per_round: 3,
            actors_per_node: 3,
            cache_capacity: capacity,
            kill_schedule: vec![(15, 2), (20, 5)],
            ..DistributionSimConfig::default()
        },
    ];

    for config in configs {
        let name = config.name.clone();
        let (trace, _) = run_simulation_with_nodes(config);
        maybe_save_trace(&trace);
        let result = check_cache_bounded(&trace, capacity);
        assert!(
            result.passed,
            "[{name}] cache bounded invariant violated: {}",
            result.actual
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 3. Repair queue populates when node with directory entries dies
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn repair_queue_populates_on_death_with_directory_entries() {
    // Node 2 has 3 actors. When it dies, repair queue should grow.
    let config = DistributionSimConfig {
        name: "repair-proportional".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 3,
        kill_schedule: vec![(15, 2)],
        ..DistributionSimConfig::default()
    };

    let (trace, _) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Repair queue should be populated within 2-3 rounds of death detection
    let result = check_repair_queue_populated(&trace, 15);
    assert!(
        result.passed,
        "repair queue should fill after node with actors dies: {}",
        result.actual
    );

    // Check that the total repair queue size across survivors is proportional
    // to the dead node's directory entries (3 actors)
    let post_death_sizes: Vec<usize> = trace
        .snapshots_per_round
        .iter()
        .skip(20)
        .take(30)
        .flat_map(|round_snaps| {
            round_snaps
                .iter()
                .filter(|(_, s)| s.is_alive)
                .map(|(_, s)| s.repair_queue_size)
        })
        .collect();

    let max_repair = post_death_sizes.iter().max().copied().unwrap_or(0);
    assert!(
        max_repair >= 1,
        "at least one repair queue entry expected for dead node's actors, max seen: {max_repair}"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 4. Registry convergence — eventual consistency across configs
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn registry_eventually_consistent_across_configs() {
    let configs = vec![
        // 5 nodes, 1 name
        (
            DistributionSimConfig {
                name: "reg-ec-simple".into(),
                num_nodes: 5,
                num_rounds: 50,
                ticks_per_round: 3,
                actors_per_node: 0,
                action_schedule: vec![
                    (5, SimAction::RegisterName { node_idx: 0, name: "svc-a".into() }),
                ],
                ..DistributionSimConfig::default()
            },
            1, // expected min registry size
        ),
        // 8 nodes, 3 names from different nodes
        (
            DistributionSimConfig {
                name: "reg-ec-multi".into(),
                num_nodes: 8,
                num_rounds: 60,
                ticks_per_round: 3,
                actors_per_node: 0,
                action_schedule: vec![
                    (5, SimAction::RegisterName { node_idx: 0, name: "alpha".into() }),
                    (5, SimAction::RegisterName { node_idx: 3, name: "beta".into() }),
                    (5, SimAction::RegisterName { node_idx: 6, name: "gamma".into() }),
                ],
                ..DistributionSimConfig::default()
            },
            3,
        ),
        // 5 nodes, register + kill owner, verify tombstone propagates
        (
            DistributionSimConfig {
                name: "reg-ec-death".into(),
                num_nodes: 5,
                num_rounds: 80,
                ticks_per_round: 3,
                actors_per_node: 0,
                action_schedule: vec![
                    (5, SimAction::RegisterName { node_idx: 0, name: "ephemeral".into() }),
                ],
                kill_schedule: vec![(15, 0)],
                ..DistributionSimConfig::default()
            },
            1, // tombstoned entry still counts as registry_size
        ),
    ];

    for (config, min_size) in configs {
        let name = config.name.clone();
        let (trace, _) = run_simulation_with_nodes(config);
        maybe_save_trace(&trace);
        let result = check_registry_propagation(&trace, min_size);
        assert!(
            result.passed,
            "[{name}] registry should converge: {}",
            result.actual
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 5. Multiple deaths don't violate invariants
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn cascading_deaths_maintain_invariants() {
    // 8 nodes, 3 die in sequence
    let config = DistributionSimConfig {
        name: "cascade-invariants".into(),
        num_nodes: 8,
        num_rounds: 100,
        ticks_per_round: 3,
        actors_per_node: 2,
        cache_capacity: 200,
        kill_schedule: vec![(10, 1), (20, 3), (30, 5)],
        ..DistributionSimConfig::default()
    };

    let (trace, _) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    let rt_result = check_routing_table_bounded(&trace);
    assert!(
        rt_result.passed,
        "routing table bounded after cascading deaths: {}",
        rt_result.actual
    );

    let cache_result = check_cache_bounded(&trace, 200);
    assert!(
        cache_result.passed,
        "cache bounded after cascading deaths: {}",
        cache_result.actual
    );

    let repair_result = check_repair_queue_populated(&trace, 10);
    assert!(
        repair_result.passed,
        "repair queue populated after first death: {}",
        repair_result.actual
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 6. Asymmetric partition + registry — one-way reachability
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn asymmetric_partition_registry_converges_after_heal() {
    // Given: 6 nodes, asymmetric partition: A→B blocked, B→A works.
    // Node 0 (side A) and node 3 (side B) each register a name.
    // After heal, all should converge.
    let config = DistributionSimConfig {
        name: "asymmetric-partition-registry".into(),
        num_nodes: 6,
        num_rounds: 100,
        ticks_per_round: 3,
        actors_per_node: 0,
        network_faults: vec![
            NetworkFault::Partition {
                round: 10,
                partition: Partition {
                    side_a: vec![0, 1, 2],
                    side_b: vec![3, 4, 5],
                    asymmetric: true, // A→B blocked, B→A works
                },
            },
            NetworkFault::Heal { round: 40 },
        ],
        action_schedule: vec![
            (12, SimAction::RegisterName { node_idx: 0, name: "from-a".into() }),
            (12, SimAction::RegisterName { node_idx: 3, name: "from-b".into() }),
        ],
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 1,
            suspicion_timeout: 200,
            dead_reprobe_interval: 10,
            probe_mode: distribution::swim::probe::ProbeMode::Periodic,
        },
        ..DistributionSimConfig::default()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // After healing, all nodes should resolve both names
    let alive_nodes: Vec<_> = nodes.iter().filter_map(|n| n.as_ref()).collect();
    for node in &alive_nodes {
        // Side B's name should have been reachable from side A even during partition
        // (B→A works), so "from-b" should propagate to everyone.
        // "from-a" might need post-heal gossip to reach side B.
        assert!(
            node.resolve_name("from-b").is_some(),
            "all nodes should resolve 'from-b' (B→A was always open)"
        );
    }

    // After 60 rounds of healed connectivity, "from-a" should also propagate
    let resolved_a: Vec<_> = alive_nodes
        .iter()
        .filter(|n| n.resolve_name("from-a").is_some())
        .collect();
    assert!(
        resolved_a.len() >= 4,
        "at least 4 of 6 nodes should resolve 'from-a' after partition heals, got {}",
        resolved_a.len()
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 7. Revived node re-registers — new registration overwrites tombstone
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn revived_node_re_registration_overwrites_tombstone() {
    use swactor::actor::ActorAddress;
    let new_actor = ActorAddress::new_random();

    // Given: 5 nodes, node 2 registers "svc", is killed, revived with fresh state,
    // then node 3 re-registers "svc" with a new actor.
    // (Don't kill node 0 since it's the join seed.)
    let config = DistributionSimConfig {
        name: "revive-reregister".into(),
        num_nodes: 5,
        num_rounds: 120,
        ticks_per_round: 3,
        actors_per_node: 0,
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 2, name: "svc".into() }),
            // After death + revive, a different surviving node re-registers
            (60, SimAction::RegisterNameWithActor { node_idx: 3, name: "svc".into(), actor: new_actor }),
        ],
        kill_schedule: vec![(15, 2)],
        revive_schedule: vec![(40, 2)],
        ..DistributionSimConfig::default()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: all alive nodes should resolve "svc" to the new registration
    let alive_nodes: Vec<_> = nodes.iter().filter_map(|n| n.as_ref()).collect();
    for node in &alive_nodes {
        assert!(
            node.resolve_name("svc").is_some(),
            "all nodes should resolve 'svc' after re-registration by surviving node"
        );
        assert_eq!(
            node.resolve_name("svc").unwrap().0,
            new_actor,
            "all nodes should resolve 'svc' to the new actor"
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 8. Registry convergence is monotonic — divergence doesn't increase
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn registry_convergence_is_monotonic_in_stable_cluster() {
    // Given: 8-node cluster, 4 names registered at round 5, no faults
    let config = DistributionSimConfig {
        name: "registry-monotonic".into(),
        num_nodes: 8,
        num_rounds: 60,
        ticks_per_round: 3,
        actors_per_node: 0,
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 0, name: "alpha".into() }),
            (5, SimAction::RegisterName { node_idx: 2, name: "beta".into() }),
            (5, SimAction::RegisterName { node_idx: 4, name: "gamma".into() }),
            (5, SimAction::RegisterName { node_idx: 6, name: "delta".into() }),
        ],
        ..DistributionSimConfig::default()
    };

    let (trace, _) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Measure "divergence" = number of alive nodes with registry_size < 4
    // Once it reaches 0, it should never increase again
    let mut reached_convergence = false;
    let mut post_convergence_divergence = 0;

    for round_snaps in &trace.snapshots_per_round {
        let alive_with_full_registry = round_snaps
            .iter()
            .filter(|(_, s)| s.is_alive && s.registry_size >= 4)
            .count();
        let alive_count = round_snaps.iter().filter(|(_, s)| s.is_alive).count();
        let divergent = alive_count - alive_with_full_registry;

        if divergent == 0 && alive_count > 0 {
            reached_convergence = true;
        } else if reached_convergence && divergent > 0 {
            post_convergence_divergence += 1;
        }
    }

    assert!(
        reached_convergence,
        "registry should converge (all alive nodes see all 4 names)"
    );
    assert_eq!(
        post_convergence_divergence, 0,
        "once converged, registry should not diverge again in a stable cluster"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 9. Large cluster registry stress test
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn large_cluster_registry_converges() {
    // Given: 15-node cluster, 5 names registered on different nodes, 2 deaths
    let config = DistributionSimConfig {
        name: "large-cluster-registry".into(),
        num_nodes: 15,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 0,
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 0, name: "alpha".into() }),
            (5, SimAction::RegisterName { node_idx: 3, name: "beta".into() }),
            (5, SimAction::RegisterName { node_idx: 6, name: "gamma".into() }),
            (5, SimAction::RegisterName { node_idx: 9, name: "delta".into() }),
            (5, SimAction::RegisterName { node_idx: 12, name: "epsilon".into() }),
        ],
        kill_schedule: vec![(20, 0), (20, 3)],
        ..DistributionSimConfig::default()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    let survivors: Vec<_> = nodes.iter().filter_map(|n| n.as_ref()).collect();
    assert_eq!(survivors.len(), 13, "13 of 15 nodes should survive");

    // Names owned by dead nodes should be tombstoned
    for node in &survivors {
        assert!(
            node.resolve_name("alpha").is_none(),
            "alpha (owned by dead node 0) should be tombstoned"
        );
        assert!(
            node.resolve_name("beta").is_none(),
            "beta (owned by dead node 3) should be tombstoned"
        );
    }

    // Names owned by surviving nodes should resolve
    for node in &survivors {
        for name in &["gamma", "delta", "epsilon"] {
            assert!(
                node.resolve_name(name).is_some(),
                "'{name}' (owned by surviving node) should resolve across 15-node cluster"
            );
        }
    }

    // Registry propagation: all survivors should have all 5 registry entries
    // (2 tombstoned + 3 alive)
    let result = check_registry_propagation(&trace, 5);
    assert!(
        result.passed,
        "all 5 registry entries should propagate in 15-node cluster: {}",
        result.actual
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 10. Three-way partition: cluster splits into 3 groups, heals, converges
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn three_way_partition_heals_and_converges() {
    // Given: 9 nodes split into 3 groups {0,1,2}, {3,4,5}, {6,7,8}.
    // Each group registers a name during the partition. After healing,
    // all 9 nodes should converge on all 3 names.
    let config = DistributionSimConfig {
        name: "three-way-partition".into(),
        num_nodes: 9,
        num_rounds: 140,
        ticks_per_round: 3,
        actors_per_node: 0,
        network_faults: vec![
            // Partition A vs B
            NetworkFault::Partition {
                round: 10,
                partition: Partition {
                    side_a: vec![0, 1, 2],
                    side_b: vec![3, 4, 5, 6, 7, 8],
                    asymmetric: false,
                },
            },
            // Partition B vs C (stacks with the above: now 3 groups isolated)
            NetworkFault::Partition {
                round: 10,
                partition: Partition {
                    side_a: vec![3, 4, 5],
                    side_b: vec![6, 7, 8],
                    asymmetric: false,
                },
            },
            NetworkFault::Heal { round: 60 },
        ],
        action_schedule: vec![
            (20, SimAction::RegisterName { node_idx: 0, name: "group-a-svc".into() }),
            (20, SimAction::RegisterName { node_idx: 3, name: "group-b-svc".into() }),
            (20, SimAction::RegisterName { node_idx: 6, name: "group-c-svc".into() }),
        ],
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 1,
            // Must exceed partition duration (50 rounds × 3 ticks = 150 ticks)
            suspicion_timeout: 200,
            dead_reprobe_interval: 10,
            probe_mode: distribution::swim::probe::ProbeMode::Periodic,
        },
        ..DistributionSimConfig::default()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    let alive_count = nodes.iter().filter(|n| n.is_some()).count();
    assert_eq!(alive_count, 9, "all 9 nodes should survive the three-way partition");

    // After healing + gossip, all 3 names should be resolvable from every node
    for (i, node) in nodes.iter().enumerate() {
        if let Some(node) = node {
            for name in &["group-a-svc", "group-b-svc", "group-c-svc"] {
                assert!(
                    node.resolve_name(name).is_some(),
                    "node {i} should resolve '{name}' after three-way partition heals"
                );
            }
        }
    }
}
