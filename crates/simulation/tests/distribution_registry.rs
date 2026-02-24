#![cfg(feature = "distribution")]
//! Registry simulation tests — cluster registry CRDT behavior under gossip.
//!
//! Tests that registry names propagate, converge, and resolve correctly
//! across the cluster under various fault conditions.

use simulation::distribution::properties::{
    check_registry_propagation, check_registry_tombstones,
};
use simulation::distribution::sim::{
    run_simulation_with_nodes, DistributionSimConfig, DistTrace, NetworkFault, Partition,
    SimAction,
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
    DistributionSimConfig {
        actors_per_node: 0, // Registry tests don't need actors
        ..DistributionSimConfig::default()
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 1. Registry name converges across 5-node cluster via gossip piggyback
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn registry_name_converges_across_cluster() {
    // Given: 5-node cluster, node 0 registers "counter" at round 5
    let config = DistributionSimConfig {
        name: "registry-convergence".into(),
        num_nodes: 5,
        num_rounds: 50,
        ticks_per_round: 3,
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 0, name: "counter".into() }),
        ],
        ..default_config()
    };

    // When: we run the simulation
    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: all alive nodes should have the registry entry
    let result = check_registry_propagation(&trace, 1);
    assert!(
        result.passed,
        "all nodes should see the 'counter' name: {}",
        result.actual
    );

    // And: all nodes should resolve "counter" to the same actor
    let resolutions: Vec<_> = nodes
        .iter()
        .filter_map(|n| n.as_ref())
        .map(|n| n.resolve_name("counter"))
        .collect();

    assert!(
        resolutions.iter().all(|r| r.is_some()),
        "all nodes should resolve 'counter', got: {resolutions:?}"
    );

    // All should agree on the same actor address
    let first = resolutions[0].unwrap().0;
    assert!(
        resolutions.iter().all(|r| r.unwrap().0 == first),
        "all nodes should agree on the same actor for 'counter'"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 2. Split-brain naming — two sides register same name during partition
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn split_brain_naming_converges_after_partition_heals() {
    // Given: 6-node cluster, partition {0,1,2} vs {3,4,5} at round 10
    // Node 0 registers "leader" at round 12, node 3 registers "leader" at round 12
    // Heal at round 40
    let config = DistributionSimConfig {
        name: "split-brain-registry".into(),
        num_nodes: 6,
        num_rounds: 100,
        ticks_per_round: 3,
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
            (12, SimAction::RegisterName { node_idx: 0, name: "leader".into() }),
            (12, SimAction::RegisterName { node_idx: 3, name: "leader".into() }),
        ],
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 1,
            // High enough that no node reaches Dead during the 30-round partition.
            // Nodes go Suspect → back to Alive when partition heals, triggering
            // re_disseminate_all which propagates both sides' registry entries.
            suspicion_timeout: 200,
            dead_reprobe_interval: 10,
        },
        ..default_config()
    };

    // When: we run the simulation
    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: all alive nodes should resolve "leader" to the same value (LWW winner)
    let resolutions: Vec<_> = nodes
        .iter()
        .filter_map(|n| n.as_ref())
        .map(|n| n.resolve_name("leader"))
        .collect();

    // All should resolve to Some (the LWW winner)
    let resolved: Vec<_> = resolutions.iter().filter_map(|r| r.as_ref()).collect();
    assert!(
        resolved.len() >= 4,
        "at least 4 of 6 nodes should resolve 'leader', got {} out of {}",
        resolved.len(),
        resolutions.len()
    );

    // All resolving nodes should agree on the same actor
    if resolved.len() >= 2 {
        let first_actor = resolved[0].0;
        let agree = resolved.iter().all(|r| r.0 == first_actor);
        assert!(
            agree,
            "all nodes resolving 'leader' should agree on the same actor (LWW winner)"
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 3. Tombstone propagation on node death
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn tombstone_propagates_when_name_owner_dies() {
    // Given: 5-node cluster, node 0 registers "svc" at round 5, killed at round 15
    let config = DistributionSimConfig {
        name: "tombstone-propagation".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 0, name: "svc".into() }),
        ],
        kill_schedule: vec![(15, 0)],
        ..default_config()
    };

    // When: we run the simulation
    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: surviving nodes should have tombstoned "svc"
    let result = check_registry_tombstones(&trace, 1);
    assert!(
        result.passed,
        "survivors should have tombstones after node death: {}",
        result.actual
    );

    // And: resolve_name("svc") should return None on all survivors
    let resolutions: Vec<_> = nodes
        .iter()
        .filter_map(|n| n.as_ref())
        .map(|n| n.resolve_name("svc"))
        .collect();
    assert!(
        resolutions.iter().all(|r| r.is_none()),
        "all survivors should resolve 'svc' to None after owner dies, got: {resolutions:?}"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 4. Rapid re-registration converges to latest value
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn rapid_re_registration_converges_to_latest() {
    // Given: 5-node cluster, node 0 registers "svc" three times rapidly
    use swactor::actor::ActorAddress;
    let actor_a = ActorAddress::new_random();
    let actor_b = ActorAddress::new_random();
    let actor_c = ActorAddress::new_random();

    let config = DistributionSimConfig {
        name: "rapid-reregister".into(),
        num_nodes: 5,
        num_rounds: 60,
        ticks_per_round: 3,
        action_schedule: vec![
            (5, SimAction::RegisterNameWithActor { node_idx: 0, name: "svc".into(), actor: actor_a }),
            (6, SimAction::RegisterNameWithActor { node_idx: 0, name: "svc".into(), actor: actor_b }),
            (7, SimAction::RegisterNameWithActor { node_idx: 0, name: "svc".into(), actor: actor_c }),
        ],
        ..default_config()
    };

    // When: we run the simulation
    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: all nodes should resolve "svc" to actor_c (the latest registration)
    let resolutions: Vec<_> = nodes
        .iter()
        .filter_map(|n| n.as_ref())
        .map(|n| n.resolve_name("svc"))
        .collect();

    assert!(
        resolutions.iter().all(|r| r.is_some()),
        "all nodes should resolve 'svc'"
    );

    assert!(
        resolutions.iter().all(|r| r.unwrap().0 == actor_c),
        "all nodes should converge to the latest registration (actor_c)"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 5. Simultaneous registration of same name on different nodes
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn simultaneous_registration_converges_deterministically() {
    // Given: 5-node cluster, node 0 and node 3 both register "mutex" at round 5
    use swactor::actor::ActorAddress;
    let actor_a = ActorAddress::new_random();
    let actor_b = ActorAddress::new_random();

    let config = DistributionSimConfig {
        name: "simultaneous-register".into(),
        num_nodes: 5,
        num_rounds: 60,
        ticks_per_round: 3,
        action_schedule: vec![
            (5, SimAction::RegisterNameWithActor { node_idx: 0, name: "mutex".into(), actor: actor_a }),
            (5, SimAction::RegisterNameWithActor { node_idx: 3, name: "mutex".into(), actor: actor_b }),
        ],
        ..default_config()
    };

    // When: we run the simulation
    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: all nodes should agree on one winner
    let resolutions: Vec<_> = nodes
        .iter()
        .filter_map(|n| n.as_ref())
        .map(|n| n.resolve_name("mutex"))
        .collect();

    assert!(
        resolutions.iter().all(|r| r.is_some()),
        "all nodes should resolve 'mutex'"
    );

    // All should agree on the same actor (whichever won the LWW tiebreaker)
    let first_actor = resolutions[0].unwrap().0;
    assert!(
        resolutions.iter().all(|r| r.unwrap().0 == first_actor),
        "all nodes should agree on the LWW winner for 'mutex'"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 6. Multiple names propagate correctly
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn multiple_names_from_different_nodes_all_propagate() {
    // Given: 5-node cluster, each node registers a unique name
    let config = DistributionSimConfig {
        name: "multi-name-propagation".into(),
        num_nodes: 5,
        num_rounds: 60,
        ticks_per_round: 3,
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 0, name: "svc-0".into() }),
            (5, SimAction::RegisterName { node_idx: 1, name: "svc-1".into() }),
            (5, SimAction::RegisterName { node_idx: 2, name: "svc-2".into() }),
            (5, SimAction::RegisterName { node_idx: 3, name: "svc-3".into() }),
            (5, SimAction::RegisterName { node_idx: 4, name: "svc-4".into() }),
        ],
        ..default_config()
    };

    // When: we run the simulation
    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: all 5 nodes should have all 5 registry entries
    let result = check_registry_propagation(&trace, 5);
    assert!(
        result.passed,
        "all nodes should have all 5 registry entries: {}",
        result.actual
    );

    // And: each node should resolve all 5 names
    for node in nodes.iter().filter_map(|n| n.as_ref()) {
        for i in 0..5 {
            let name = format!("svc-{i}");
            assert!(
                node.resolve_name(&name).is_some(),
                "every node should resolve '{name}'"
            );
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 7. Explicit unregister propagates to all nodes
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn explicit_unregister_propagates_to_all_nodes() {
    // Given: 5-node cluster, node 0 registers "svc" at round 5, unregisters at round 15
    let config = DistributionSimConfig {
        name: "explicit-unregister".into(),
        num_nodes: 5,
        num_rounds: 60,
        ticks_per_round: 3,
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 0, name: "svc".into() }),
            (15, SimAction::UnregisterName { node_idx: 0, name: "svc".into() }),
        ],
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: all nodes should resolve "svc" to None (tombstoned)
    for (i, node) in nodes.iter().filter_map(|n| n.as_ref()).enumerate() {
        assert!(
            node.resolve_name("svc").is_none(),
            "node {i} should resolve 'svc' to None after unregister"
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 8. Re-registration after tombstone overwrites the tombstone
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn re_registration_after_tombstone_succeeds() {
    // Given: node 0 registers "svc", then it's killed (tombstoned),
    // then node 1 re-registers "svc" with a new actor
    use swactor::actor::ActorAddress;
    let new_actor = ActorAddress::new_random();

    let config = DistributionSimConfig {
        name: "re-register-after-tombstone".into(),
        num_nodes: 5,
        num_rounds: 100,
        ticks_per_round: 3,
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 0, name: "svc".into() }),
            // Node 1 re-registers "svc" well after node 0 dies and tombstone propagates
            (50, SimAction::RegisterNameWithActor { node_idx: 1, name: "svc".into(), actor: new_actor }),
        ],
        kill_schedule: vec![(15, 0)],
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: all survivors should resolve "svc" to the new actor from node 1
    let resolutions: Vec<_> = nodes
        .iter()
        .filter_map(|n| n.as_ref())
        .map(|n| n.resolve_name("svc"))
        .collect();

    assert!(
        resolutions.iter().all(|r| r.is_some()),
        "all survivors should resolve 'svc' to the new registration, got: {resolutions:?}"
    );

    assert!(
        resolutions.iter().all(|r| r.unwrap().0 == new_actor),
        "all survivors should resolve 'svc' to the new actor"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 9. Multiple names from same node, kill node, all tombstoned
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn all_names_tombstoned_when_owner_dies() {
    // Given: node 0 registers 3 names, then is killed
    let config = DistributionSimConfig {
        name: "multi-name-tombstone".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 0, name: "alpha".into() }),
            (5, SimAction::RegisterName { node_idx: 0, name: "beta".into() }),
            (5, SimAction::RegisterName { node_idx: 0, name: "gamma".into() }),
        ],
        kill_schedule: vec![(15, 0)],
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: all survivors should resolve all 3 names to None
    for node in nodes.iter().filter_map(|n| n.as_ref()) {
        for name in &["alpha", "beta", "gamma"] {
            assert!(
                node.resolve_name(name).is_none(),
                "all names should be tombstoned after owner dies, but '{}' still resolves",
                name
            );
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 10. Graceful leave tombstones the leaving node's names
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn graceful_leave_tombstones_registry_names() {
    // Given: node 0 registers "svc", then does a graceful leave
    let config = DistributionSimConfig {
        name: "graceful-leave-registry".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 0, name: "svc".into() }),
            (20, SimAction::GracefulLeave { node_idx: 0 }),
        ],
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: all survivors should resolve "svc" to None (tombstoned via death notification)
    let resolutions: Vec<_> = nodes
        .iter()
        .filter_map(|n| n.as_ref())
        .map(|n| n.resolve_name("svc"))
        .collect();

    assert!(
        resolutions.iter().all(|r| r.is_none()),
        "all survivors should resolve 'svc' to None after graceful leave, got: {resolutions:?}"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 11. Piggyback contention — kills + registrations compete for bandwidth
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn piggyback_contention_both_propagate() {
    // Given: 10-node cluster, kill 2 nodes + register 3 names simultaneously
    // Both membership death updates and registry entries share piggyback bandwidth
    let config = DistributionSimConfig {
        name: "piggyback-contention".into(),
        num_nodes: 10,
        num_rounds: 100,
        ticks_per_round: 3,
        actors_per_node: 0,
        action_schedule: vec![
            (10, SimAction::RegisterName { node_idx: 0, name: "alpha".into() }),
            (10, SimAction::RegisterName { node_idx: 3, name: "beta".into() }),
            (10, SimAction::RegisterName { node_idx: 6, name: "gamma".into() }),
        ],
        kill_schedule: vec![(10, 2), (10, 5)],
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: all survivors should have all 3 registry names
    let result = check_registry_propagation(&trace, 3);
    assert!(
        result.passed,
        "all 3 names should propagate despite contention with death updates: {}",
        result.actual
    );

    // And: all survivors should resolve all 3 names
    for node in nodes.iter().filter_map(|n| n.as_ref()) {
        for name in &["alpha", "beta", "gamma"] {
            assert!(
                node.resolve_name(name).is_some(),
                "survivor should resolve '{}' despite piggyback contention",
                name
            );
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 12. Registry convergence under message loss
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn registry_converges_despite_message_loss() {
    // Given: 5-node cluster with a loss window during name registration,
    // then clean connectivity for convergence.
    let config = DistributionSimConfig {
        name: "registry-under-loss".into(),
        num_nodes: 5,
        num_rounds: 100,
        ticks_per_round: 3,
        registry_dissemination_lambda: Some(5),
        network_faults: vec![
            // Loss window: 30% drops during registration phase
            NetworkFault::SetDropRate { round: 3, rate: 0.30 },
            // Restore clean connectivity, forcing convergence via Alive transitions
            NetworkFault::SetDropRate { round: 30, rate: 0.0 },
        ],
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 0, name: "alpha".into() }),
            (5, SimAction::RegisterName { node_idx: 2, name: "beta".into() }),
            (5, SimAction::RegisterName { node_idx: 4, name: "gamma".into() }),
        ],
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 3,
            // High timeout prevents false deaths during the loss window.
            suspicion_timeout: 200,
            dead_reprobe_interval: 15,
        },
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: all surviving nodes should resolve all 3 names despite packet loss
    let alive_nodes: Vec<_> = nodes.iter().filter_map(|n| n.as_ref()).collect();
    for node in &alive_nodes {
        for name in &["alpha", "beta", "gamma"] {
            assert!(
                node.resolve_name(name).is_some(),
                "all nodes should resolve '{}' despite 20% message loss",
                name
            );
        }
    }

    // All should agree on the same actor for each name
    for name in &["alpha", "beta", "gamma"] {
        let first = alive_nodes[0].resolve_name(name).unwrap().0;
        assert!(
            alive_nodes.iter().all(|n| n.resolve_name(name).unwrap().0 == first),
            "all nodes should agree on actor for '{}' despite message loss",
            name
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 13. Multiple name-owning nodes killed simultaneously
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn simultaneous_kill_of_multiple_name_owners() {
    // Given: 7-node cluster, nodes 0-2 each own a name, all 3 killed at round 15
    let config = DistributionSimConfig {
        name: "multi-owner-kill".into(),
        num_nodes: 7,
        num_rounds: 80,
        ticks_per_round: 3,
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 0, name: "svc-a".into() }),
            (5, SimAction::RegisterName { node_idx: 1, name: "svc-b".into() }),
            (5, SimAction::RegisterName { node_idx: 2, name: "svc-c".into() }),
        ],
        kill_schedule: vec![(15, 0), (15, 1), (15, 2)],
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: all 4 survivors should resolve all 3 names to None (tombstoned)
    let survivors: Vec<_> = nodes.iter().filter_map(|n| n.as_ref()).collect();
    assert_eq!(survivors.len(), 4, "4 of 7 nodes should survive");

    for node in &survivors {
        for name in &["svc-a", "svc-b", "svc-c"] {
            assert!(
                node.resolve_name(name).is_none(),
                "survivor should resolve '{}' to None after owner died",
                name
            );
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 14. Name owner suspected but recovers — registry entry survives
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn suspected_name_owner_recovers_and_registry_survives() {
    // Given: 5-node cluster, node 0 registers "svc",
    // then a brief partition isolates node 0 (becomes Suspect, recovers before Dead)
    let config = DistributionSimConfig {
        name: "suspect-recovery-registry".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 0, name: "svc".into() }),
        ],
        // Brief partition: isolate node 0 for 10 rounds (not long enough to reach Dead)
        network_faults: vec![
            NetworkFault::Partition {
                round: 15,
                partition: Partition {
                    side_a: vec![0],
                    side_b: vec![1, 2, 3, 4],
                    asymmetric: false,
                },
            },
            NetworkFault::Heal { round: 25 },
        ],
        swim: distribution::swim::probe::SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 1,
            // Node stays Suspect during the 10-round partition (30 ticks < 200)
            suspicion_timeout: 200,
            dead_reprobe_interval: 10,
        },
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    // Then: all nodes should still resolve "svc" (node 0 never died, only suspected)
    let resolutions: Vec<_> = nodes
        .iter()
        .filter_map(|n| n.as_ref())
        .map(|n| n.resolve_name("svc"))
        .collect();

    assert!(
        resolutions.iter().all(|r| r.is_some()),
        "all nodes should resolve 'svc' after owner recovers from Suspect, got: {resolutions:?}"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 15. Rapid churn: interleaved kills and registrations
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn registry_correct_under_rapid_churn() {
    // Given: 8-node cluster with interleaved kills and registrations
    let config = DistributionSimConfig {
        name: "rapid-churn-registry".into(),
        num_nodes: 8,
        num_rounds: 100,
        ticks_per_round: 3,
        action_schedule: vec![
            (5, SimAction::RegisterName { node_idx: 0, name: "svc-0".into() }),
            (5, SimAction::RegisterName { node_idx: 1, name: "svc-1".into() }),
            (8, SimAction::RegisterName { node_idx: 4, name: "svc-4".into() }),
            // Node 3 registers AFTER nodes 0 and 1 are killed
            (20, SimAction::RegisterName { node_idx: 3, name: "svc-3".into() }),
            // Node 5 takes over "svc-0" after original owner dies
            (30, SimAction::RegisterName { node_idx: 5, name: "svc-0".into() }),
        ],
        kill_schedule: vec![(10, 0), (10, 1), (25, 2)],
        ..default_config()
    };

    let (trace, nodes) = run_simulation_with_nodes(config);
    maybe_save_trace(&trace);

    let survivors: Vec<_> = nodes.iter().filter_map(|n| n.as_ref()).collect();
    assert_eq!(survivors.len(), 5, "5 of 8 nodes should survive");

    // svc-0 should resolve to node 5's re-registration (not tombstoned)
    for node in &survivors {
        assert!(
            node.resolve_name("svc-0").is_some(),
            "svc-0 should resolve (re-registered by node 5 after owner death)"
        );
    }

    // svc-1 should be tombstoned (node 1 died, no re-registration)
    for node in &survivors {
        assert!(
            node.resolve_name("svc-1").is_none(),
            "svc-1 should be tombstoned (owner died, not re-registered)"
        );
    }

    // svc-3 should resolve (registered after kills, node 3 alive)
    for node in &survivors {
        assert!(
            node.resolve_name("svc-3").is_some(),
            "svc-3 should resolve (registered after kills)"
        );
    }

    // svc-4 should resolve (node 4 alive throughout)
    for node in &survivors {
        assert!(
            node.resolve_name("svc-4").is_some(),
            "svc-4 should resolve (owner alive throughout)"
        );
    }
}
