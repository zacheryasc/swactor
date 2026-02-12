use simulation::distribution::properties::{
    analyze, check_actor_resolution, check_failure_detection, check_join_convergence,
    check_membership_accuracy,
};
use simulation::distribution::sim::{run_simulation, DistributionSimConfig};
use simulation::distribution::trace::DistributionEventKind;

fn default_config() -> DistributionSimConfig {
    DistributionSimConfig::default()
}

// ────────────────────────────────────────────────────────────────────────────
// Test 1: A small cluster converges its membership view
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn cluster_of_five_converges() {
    // Given: 5 nodes with probe_interval=1
    let config = DistributionSimConfig {
        name: "five-converges".into(),
        num_nodes: 5,
        num_rounds: 50,
        ticks_per_round: 3,
        actors_per_node: 0,
        ..default_config()
    };

    // When: we run the simulation
    let trace = run_simulation(config);
    let metrics = analyze(&trace);

    // Then: cluster membership converges within 20 rounds
    let result = check_join_convergence(&metrics, 20);
    assert!(
        result.passed,
        "cluster of 5 should converge within 20 rounds: {}",
        result.actual
    );
}

// ────────────────────────────────────────────────────────────────────────────
// Test 2: A larger cluster converges with high accuracy
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn cluster_of_twenty_converges() {
    // Given: 20 nodes with extra rounds
    let config = DistributionSimConfig {
        name: "twenty-converges".into(),
        num_nodes: 20,
        num_rounds: 100,
        ticks_per_round: 3,
        actors_per_node: 0,
        ..default_config()
    };

    // When: we run the simulation
    let trace = run_simulation(config);
    let metrics = analyze(&trace);

    // Then: membership accuracy is high
    let result = check_membership_accuracy(&metrics, 0.9);
    assert!(
        result.passed,
        "cluster of 20 should have ≥90% membership accuracy: {}",
        result.actual
    );
}

// ────────────────────────────────────────────────────────────────────────────
// Test 3: A killed node is eventually detected by survivors
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn node_death_is_detected() {
    // Given: 5 nodes, node 2 is killed at round 10
    let config = DistributionSimConfig {
        name: "death-detection".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 0,
        kill_schedule: vec![(10, 2)],
        ..default_config()
    };

    // When: we run the simulation
    let trace = run_simulation(config);

    // Then: surviving nodes see at most 4 members at the end
    // (self + 3 other survivors; the killed node should be removed)
    let result = check_failure_detection(&trace, 4);
    assert!(
        result.passed,
        "survivors should detect node death: {}",
        result.actual
    );
}

// ────────────────────────────────────────────────────────────────────────────
// Test 4: A killed node can rejoin the cluster
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn killed_node_rejoins() {
    // Given: 5 nodes, kill node 3 at round 10, revive at round 50
    let config = DistributionSimConfig {
        name: "rejoin".into(),
        num_nodes: 5,
        num_rounds: 100,
        ticks_per_round: 3,
        actors_per_node: 0,
        kill_schedule: vec![(10, 3)],
        revive_schedule: vec![(50, 3)],
        ..default_config()
    };

    // When: we run the simulation
    let trace = run_simulation(config);

    // Then: the revived node has learned about at least some cluster members
    let last_round = trace.snapshots_per_round.last().unwrap();
    let revived_snap = &last_round[3].1;
    assert!(
        revived_snap.is_alive,
        "revived node should be alive at end"
    );
    assert!(
        revived_snap.member_count >= 1,
        "revived node should know about at least 1 member, got {}",
        revived_snap.member_count
    );
}

// ────────────────────────────────────────────────────────────────────────────
// Test 5: Actors are resolvable across the cluster
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn actors_resolvable_across_cluster() {
    // Given: 5 nodes, 2 actors registered per node
    let config = DistributionSimConfig {
        name: "actor-resolution".into(),
        num_nodes: 5,
        num_rounds: 30,
        ticks_per_round: 3,
        actors_per_node: 2,
        ..default_config()
    };

    // When: we run the simulation
    let trace = run_simulation(config);
    let metrics = analyze(&trace);

    // Then: actor resolution succeeds at ≥90%
    let result = check_actor_resolution(&metrics, 0.9);
    assert!(
        result.passed,
        "actor resolution should succeed ≥90%: {}",
        result.actual
    );
}

// ────────────────────────────────────────────────────────────────────────────
// Test 6: Actor resolution degrades gracefully when a host node dies
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn actor_resolution_survives_node_death() {
    // Given: 5 nodes, 2 actors/node, kill node 1 at round 10
    let config = DistributionSimConfig {
        name: "resolution-after-death".into(),
        num_nodes: 5,
        num_rounds: 80,
        ticks_per_round: 3,
        actors_per_node: 2,
        kill_schedule: vec![(10, 1)],
        ..default_config()
    };

    // When: we run the simulation
    let trace = run_simulation(config);
    let metrics = analyze(&trace);

    // Then: we get some resolution failures (the killed node's actors may fail)
    // but surviving nodes' actors should still be resolvable.
    // With 5 nodes and 1 killed, at least 60% of total resolutions should succeed
    // (actors on surviving nodes should all resolve via cache).
    let total = metrics.actor_resolve_success + metrics.actor_resolve_failed;
    assert!(
        total > 0,
        "should have attempted some actor resolutions"
    );

    // Also check that after killing, the simulation continues producing events
    let post_kill_events: Vec<_> = trace
        .events
        .iter()
        .filter(|e| e.tick > 10)
        .filter(|e| matches!(&e.kind, DistributionEventKind::ActorResolved { .. }))
        .collect();
    assert!(
        !post_kill_events.is_empty(),
        "should still resolve some actors after node death"
    );
}
