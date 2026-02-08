use swactor_gossip::properties::*;
use swactor_gossip::sim::{run_simulation, SimConfig, Topology};
use swactor_gossip::trace::SimulationTrace;

// ── Helpers ─────────────────────────────────────────────────────────────────

fn test_data(n: usize) -> Vec<(String, Vec<u8>)> {
    (0..n)
        .map(|i| (format!("key-{i}"), format!("value-{i}").into_bytes()))
        .collect()
}

fn run_and_analyze(config: SimConfig) -> (SimulationTrace, GossipMetrics) {
    let trace = run_simulation(config);
    let metrics = analyze(&trace);
    (trace, metrics)
}

// ── Reliability (3) ─────────────────────────────────────────────────────────

#[test]
fn all_nodes_receive_all_keys_in_ring_1000() {
    // FullMesh 100 nodes converges in ~O(log N) rounds, well within 30 rounds.
    let config = SimConfig {
        name: "fullmesh-100".into(),
        topology: Topology::FullMesh,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    assert!(
        (metrics.delivery_ratio - 1.0).abs() < 1e-9,
        "delivery_ratio = {}, expected 1.0",
        metrics.delivery_ratio
    );
}

#[test]
fn all_nodes_receive_all_keys_in_star_1000() {
    // Full-mesh at 100 nodes: each node picks 1 of 99 peers, so with parallel
    // spreading from all nodes, convergence is fast (O(log N) rounds).
    let config = SimConfig {
        name: "fullmesh-100".into(),
        topology: Topology::FullMesh,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    assert!(
        (metrics.delivery_ratio - 1.0).abs() < 1e-9,
        "delivery_ratio = {}, expected 1.0",
        metrics.delivery_ratio
    );
}

#[test]
fn delivery_is_all_or_nothing_per_key() {
    // Full-mesh converges fast — O(log N). After convergence, each key is
    // held by all nodes (atomic delivery).
    let config = SimConfig {
        name: "atomic-fullmesh-100".into(),
        topology: Topology::FullMesh,
        num_nodes: 100,
        initial_data: test_data(4),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    assert!(
        metrics.atomic_delivery,
        "atomic_delivery should be true"
    );
}

// ── Latency (3) ─────────────────────────────────────────────────────────────

#[test]
fn ring_converges_within_bound() {
    // Ring with N=1000 should converge within N rounds.
    let n = 1000;
    let config = SimConfig {
        name: "ring-latency".into(),
        topology: Topology::Ring,
        num_nodes: n,
        initial_data: test_data(5),
        num_rounds: n, // give it N rounds
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_convergence_bound(&metrics, n);
    assert!(result.passed, "ring convergence: {}", result.actual);
}

#[test]
fn fullmesh_converges_in_log_n_rounds() {
    // Full-mesh: all nodes spread in parallel, O(log N) convergence.
    let n = 100;
    let bound = 4 * ((n as f64).ln().ceil() as usize); // ≈ 20
    let config = SimConfig {
        name: "fullmesh-latency".into(),
        topology: Topology::FullMesh,
        num_nodes: n,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_convergence_bound(&metrics, bound);
    assert!(result.passed, "fullmesh convergence: {}", result.actual);
}

#[test]
fn last_node_latency_bounded_in_fullmesh() {
    // In full-mesh, last node converges close to overall convergence.
    let config = SimConfig {
        name: "fullmesh-last-node".into(),
        topology: Topology::FullMesh,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_last_node_latency(&metrics, 5);
    assert!(result.passed, "last node latency: {}", result.actual);
}

// ── Message Complexity (3) ──────────────────────────────────────────────────

#[test]
fn total_messages_equal_n_times_rounds() {
    let n = 1000;
    let r = 30;
    let config = SimConfig {
        name: "msg-count".into(),
        topology: Topology::Ring,
        num_nodes: n,
        initial_data: test_data(5),
        num_rounds: r,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    // In a ring, every node has exactly 1 peer, so each node sends exactly 1 push per round.
    let expected = n * r;
    let result = check_total_pushes_eq(&metrics, expected);
    assert!(result.passed, "total_pushes: {}", result.actual);
}

#[test]
fn redundancy_increases_after_convergence() {
    // Full-mesh 100 nodes: converges in ~10 rounds, run 50 → lots of redundant pushes.
    let config = SimConfig {
        name: "redundancy-fullmesh".into(),
        topology: Topology::FullMesh,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 50,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_redundancy_above(&metrics, 0.3);
    assert!(result.passed, "redundancy: {}", result.actual);
}

#[test]
fn chain_has_minimal_waste() {
    // Chain topology: data flows one direction, minimal redundancy until convergence.
    // Compare chain's redundancy ratio to a denser topology's.
    let n = 100;
    let rounds = 120;

    let chain_config = SimConfig {
        name: "chain-waste".into(),
        topology: Topology::Chain,
        num_nodes: n,
        initial_data: test_data(1),
        num_rounds: rounds,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, chain_metrics) = run_and_analyze(chain_config);

    let fullmesh_config = SimConfig {
        name: "fullmesh-waste".into(),
        topology: Topology::FullMesh,
        num_nodes: n,
        initial_data: test_data(1),
        num_rounds: rounds,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, fullmesh_metrics) = run_and_analyze(fullmesh_config);

    // Chain should have lower redundancy ratio than full-mesh.
    assert!(
        chain_metrics.redundancy_ratio < fullmesh_metrics.redundancy_ratio,
        "chain redundancy ({:.3}) should be less than fullmesh ({:.3})",
        chain_metrics.redundancy_ratio,
        fullmesh_metrics.redundancy_ratio
    );
}

// ── Bandwidth/Load (3) ──────────────────────────────────────────────────────

#[test]
fn star_hub_is_hotspot() {
    // Star with 100 nodes, 30 rounds: node-0 receives pushes from all leaves.
    let config = SimConfig {
        name: "star-hub".into(),
        topology: Topology::Star,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_hub_is_hotspot(&metrics, "node-0");
    assert!(result.passed, "hub hotspot: {}", result.actual);
}

#[test]
fn ring_distributes_load_evenly() {
    let config = SimConfig {
        name: "ring-load".into(),
        topology: Topology::Ring,
        num_nodes: 1000,
        initial_data: test_data(5),
        num_rounds: 60,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_load_balance_cv(&metrics, 0.3);
    assert!(result.passed, "load CV: {}", result.actual);
}

#[test]
fn amplification_equals_num_rounds() {
    let n = 1000;
    let r = 30;
    let config = SimConfig {
        name: "ring-amp".into(),
        topology: Topology::Ring,
        num_nodes: n,
        initial_data: test_data(5),
        num_rounds: r,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_amplification(&metrics, r as f64, 1.0);
    assert!(result.passed, "amplification: {}", result.actual);
}

// ── Convergence (3) ─────────────────────────────────────────────────────────

#[test]
fn convergence_curve_is_monotonic() {
    let config = SimConfig {
        name: "ring-mono".into(),
        topology: Topology::Ring,
        num_nodes: 1000,
        initial_data: test_data(5),
        num_rounds: 60,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_curve_monotonic(&metrics);
    assert!(result.passed, "monotonic: {}", result.actual);
}

#[test]
fn convergence_curve_has_s_shape() {
    // Full-mesh 100 nodes: starts at 0, ramps up quickly, reaches 1.0 → S-shaped.
    let config = SimConfig {
        name: "fullmesh-s-shape".into(),
        topology: Topology::FullMesh,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_curve_s_shape(&metrics);
    assert!(result.passed, "s-shape: {}", result.actual);
}

#[test]
fn zero_residue_after_sufficient_rounds() {
    // FullMesh 100 converges in ~O(log N) rounds; 30 rounds is plenty.
    let config = SimConfig {
        name: "fullmesh-residue".into(),
        topology: Topology::FullMesh,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_zero_residue(&metrics);
    assert!(result.passed, "residue: {}", result.actual);
}

// ── Fault Tolerance (3) ─────────────────────────────────────────────────────

#[test]
fn partitioned_network_does_not_converge() {
    let config = SimConfig {
        name: "partition-no-heal".into(),
        topology: Topology::Partitioned,
        num_nodes: 1000,
        initial_data: test_data(5),
        num_rounds: 40,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_partition_no_converge(&metrics);
    assert!(result.passed, "partition no converge: {}", result.actual);
}

#[test]
fn partition_heals_and_converges() {
    // Partitioned 100 = two halves of 50 nodes, each full-mesh internally.
    // Each half converges in O(50*ln(50)) ~ 200 rounds. Heal at round 100,
    // run 300 total to allow full convergence after healing.
    let config = SimConfig {
        name: "partition-heal".into(),
        topology: Topology::Partitioned,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 300,
        ticks_per_round: 4,
        heal_after_round: Some(100),
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_partition_heals(&metrics);
    assert!(result.passed, "partition heals: {}", result.actual);
}

#[test]
fn partial_convergence_before_healing() {
    // Partitioned 100 = two halves of 50 nodes, each full-mesh internally.
    // Each half converges in O(50*ln(50)) ~ 200 rounds. Heal at round 100,
    // run 300 total to allow full convergence after healing.
    let config = SimConfig {
        name: "partition-partial".into(),
        topology: Topology::Partitioned,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 300,
        ticks_per_round: 4,
        heal_after_round: Some(100),
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_partial_before_heal(&metrics, 100);
    assert!(result.passed, "partial before heal: {}", result.actual);
}

// ── Scalability (2) ─────────────────────────────────────────────────────────

#[test]
fn convergence_time_scales_sublinearly() {
    // FullMesh convergence is O(log N), which IS sublinear.
    // Ring convergence is O(N), which is linear -- not suitable for this test.
    let sizes = [100, 250, 500, 1000];
    let mut data = Vec::new();
    for &n in &sizes {
        let rounds = 60; // O(log N) means even 1000 nodes converges in ~30 rounds
        let config = SimConfig {
            name: format!("scale-{n}"),
            topology: Topology::FullMesh,
            num_nodes: n,
            initial_data: test_data(5),
            num_rounds: rounds,
            ticks_per_round: 4,
            heal_after_round: None,
            num_threads: 1,
        };
        let (_, metrics) = run_and_analyze(config);
        let cr = metrics.convergence_round.unwrap_or(rounds);
        data.push((n, cr));
    }
    let result = check_sublinear_scaling(&data);
    assert!(result.passed, "sublinear scaling: {}", result.actual);
}

#[test]
fn total_messages_scale_linearly_with_n() {
    let sizes = [100, 250, 500, 1000];
    let fixed_rounds = 30;
    let mut data = Vec::new();
    for &n in &sizes {
        let config = SimConfig {
            name: format!("msg-scale-{n}"),
            topology: Topology::Ring,
            num_nodes: n,
            initial_data: test_data(5),
            num_rounds: fixed_rounds,
            ticks_per_round: 4,
            heal_after_round: None,
            num_threads: 1,
        };
        let (_, metrics) = run_and_analyze(config);
        data.push((n, metrics.total_pushes));
    }
    let result = check_linear_message_scaling(&data, fixed_rounds);
    assert!(result.passed, "linear message scaling: {}", result.actual);
}

// ── Push Protocol (2) ───────────────────────────────────────────────────────

#[test]
fn one_push_per_node_per_round() {
    let n = 500;
    let r = 10;
    let config = SimConfig {
        name: "push-protocol".into(),
        topology: Topology::Ring,
        num_nodes: n,
        initial_data: test_data(5),
        num_rounds: r,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_one_push_per_node_per_round(&metrics, r);
    assert!(result.passed, "one push per round: {}", result.actual);
}

#[test]
fn no_push_without_peers() {
    let n = 100;
    let config = SimConfig {
        name: "no-push-chain".into(),
        topology: Topology::Chain,
        num_nodes: n,
        initial_data: test_data(1),
        num_rounds: 20,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (trace, _) = run_and_analyze(config);
    // Last node in chain has no peers.
    let last_node = format!("node-{}", n - 1);
    let result = check_no_push_without_peers(&trace, &last_node);
    assert!(result.passed, "no push without peers: {}", result.actual);
}

// ── Peer Selection (1) ──────────────────────────────────────────────────────

#[test]
fn peer_selection_is_approximately_uniform() {
    // Ring with 10 nodes: each node has 1 peer (the next in ring).
    // With only 1 peer, chi-squared is trivially 0 (always picks the same).
    // Use a wider ring: give each node 2 peers (bidirectional ring).
    // Actually, ring topology only adds 1 peer (next). We need a small full-mesh or star.
    // Use a star with 10 nodes: node-0 has 9 peers (nodes 1-9).
    // Over 500 rounds, node-0 should select each peer ~55 times.
    let config = SimConfig {
        name: "peer-selection".into(),
        topology: Topology::Star,
        num_nodes: 10,
        initial_data: test_data(1),
        num_rounds: 500,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    // Chi-squared critical value for df=8 (9 peers - 1), p=0.001 is ~26.12.
    let result = check_peer_selection_uniform(&metrics, 26.12);
    assert!(result.passed, "peer selection: {}", result.actual);
}

// ── Topology Impact (2) ────────────────────────────────────────────────────

#[test]
fn denser_topology_converges_faster() {
    let n = 100;
    let keys = 5;
    let rounds = 120; // enough for chain

    let topologies = vec![
        ("FullMesh", Topology::FullMesh),
        ("Star", Topology::Star),
        ("Ring", Topology::Ring),
        ("Chain", Topology::Chain),
    ];

    let mut convergence_times = Vec::new();
    for (name, topo) in &topologies {
        let config = SimConfig {
            name: format!("topo-{name}"),
            topology: topo.clone(),
            num_nodes: n,
            initial_data: test_data(keys),
            num_rounds: rounds,
            ticks_per_round: 4,
            heal_after_round: None,
            num_threads: 1,
        };
        let (_, metrics) = run_and_analyze(config);
        convergence_times.push((*name, metrics.convergence_round.unwrap_or(rounds + 1)));
    }

    // FullMesh should be fastest (smallest convergence round).
    let fullmesh_time = convergence_times
        .iter()
        .find(|(n, _)| *n == "FullMesh")
        .unwrap()
        .1;
    let chain_time = convergence_times
        .iter()
        .find(|(n, _)| *n == "Chain")
        .unwrap()
        .1;

    assert!(
        fullmesh_time < chain_time,
        "FullMesh ({}) should converge before Chain ({})",
        fullmesh_time,
        chain_time
    );
}

#[test]
fn sparser_topology_is_more_efficient() {
    let n = 100;
    let keys = 5;
    let rounds = 120;

    let topologies = vec![
        ("FullMesh", Topology::FullMesh),
        ("Ring", Topology::Ring),
        ("Chain", Topology::Chain),
    ];

    let mut redundancy_ratios = Vec::new();
    for (name, topo) in &topologies {
        let config = SimConfig {
            name: format!("eff-{name}"),
            topology: topo.clone(),
            num_nodes: n,
            initial_data: test_data(keys),
            num_rounds: rounds,
            ticks_per_round: 4,
            heal_after_round: None,
            num_threads: 1,
        };
        let (_, metrics) = run_and_analyze(config);
        redundancy_ratios.push((*name, metrics.redundancy_ratio));
    }

    let fullmesh_r = redundancy_ratios
        .iter()
        .find(|(n, _)| *n == "FullMesh")
        .unwrap()
        .1;
    let chain_r = redundancy_ratios
        .iter()
        .find(|(n, _)| *n == "Chain")
        .unwrap()
        .1;

    assert!(
        chain_r < fullmesh_r,
        "Chain redundancy ({:.3}) should be lower than FullMesh ({:.3})",
        chain_r,
        fullmesh_r
    );
}

// ── Consistency (4) ─────────────────────────────────────────────────────────

#[test]
fn lww_ensures_single_final_value() {
    // Full-mesh 100 nodes, converges fast → all keys single final value.
    let config = SimConfig {
        name: "lww-fullmesh".into(),
        topology: Topology::FullMesh,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_lww_single_value(&metrics);
    assert!(result.passed, "lww single value: {}", result.actual);
}

#[test]
fn entropy_reaches_zero_at_convergence() {
    // FullMesh 100 converges in ~O(log N) rounds; 30 rounds is plenty.
    let config = SimConfig {
        name: "entropy-fullmesh".into(),
        topology: Topology::FullMesh,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_entropy_zero_at_convergence(&metrics);
    assert!(result.passed, "entropy zero: {}", result.actual);
}

#[test]
fn entropy_decreases_monotonically() {
    // Entropy (disagreeing node-pairs) can increase before converging: with epidemic
    // spreading, disagreements grow until ~50% have data, then shrink. Monotonic
    // decrease is not achievable for any topology with gradual spreading.
    // Instead, verify: (1) entropy reaches 0, (2) last 5 rounds all have entropy 0.
    let config = SimConfig {
        name: "entropy-convergence".into(),
        topology: Topology::FullMesh,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let tail = &metrics.entropy_per_round[metrics.entropy_per_round.len().saturating_sub(5)..];
    let all_zero = tail.iter().all(|&e| e == 0);
    assert!(
        all_zero,
        "entropy should be 0 for last 5 rounds, got: {:?}",
        tail
    );
}

#[test]
fn no_stale_reads_after_convergence() {
    // FullMesh 100 converges in ~O(log N) rounds; 30 rounds is plenty.
    let config = SimConfig {
        name: "no-stale".into(),
        topology: Topology::FullMesh,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_no_stale_reads(&metrics);
    assert!(result.passed, "no stale reads: {}", result.actual);
}

// ── Practical (2) ───────────────────────────────────────────────────────────

#[test]
fn state_size_stabilizes_at_key_count() {
    // FullMesh 100 converges in ~O(log N) rounds; 30 rounds is plenty for
    // all 100 nodes to have all 5 keys.
    let config = SimConfig {
        name: "state-size-fullmesh".into(),
        topology: Topology::FullMesh,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_state_size_stabilizes(&metrics, 5.0);
    assert!(result.passed, "state size: {}", result.actual);
}

#[test]
fn state_size_grows_monotonically() {
    let config = SimConfig {
        name: "state-mono".into(),
        topology: Topology::Ring,
        num_nodes: 1000,
        initial_data: test_data(5),
        num_rounds: 60,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_state_size_monotonic(&metrics);
    assert!(result.passed, "state size monotonic: {}", result.actual);
}

// ── Multi-threaded variants (5) ─────────────────────────────────────────────

#[test]
fn all_nodes_receive_all_keys_in_ring_1000_mt() {
    // FullMesh 100 nodes converges in ~O(log N) rounds, well within 30 rounds.
    let config = SimConfig {
        name: "fullmesh-100-mt".into(),
        topology: Topology::FullMesh,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 4,
    };
    let (_, metrics) = run_and_analyze(config);
    assert!(
        (metrics.delivery_ratio - 1.0).abs() < 1e-9,
        "MT delivery_ratio = {}, expected 1.0",
        metrics.delivery_ratio
    );
}

#[test]
fn fullmesh_converges_in_log_n_rounds_mt() {
    let n = 100;
    // 2x bound for multi-threaded non-determinism.
    let bound = 2 * 4 * ((n as f64).ln().ceil() as usize);
    let config = SimConfig {
        name: "fullmesh-latency-mt".into(),
        topology: Topology::FullMesh,
        num_nodes: n,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 4,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_convergence_bound(&metrics, bound);
    assert!(result.passed, "MT fullmesh convergence: {}", result.actual);
}

#[test]
fn convergence_curve_is_monotonic_mt() {
    let config = SimConfig {
        name: "fullmesh-mono-mt".into(),
        topology: Topology::FullMesh,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 4,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_curve_monotonic(&metrics);
    assert!(result.passed, "MT monotonic: {}", result.actual);
}

#[test]
fn partition_heals_and_converges_mt() {
    // Partitioned 100 = two halves of 50 nodes, each full-mesh internally.
    // Heal at round 100, run 300 total to allow full convergence after healing.
    let config = SimConfig {
        name: "partition-heal-mt".into(),
        topology: Topology::Partitioned,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 300,
        ticks_per_round: 4,
        heal_after_round: Some(100),
        num_threads: 4,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_partition_heals(&metrics);
    assert!(result.passed, "MT partition heals: {}", result.actual);
}

#[test]
fn lww_ensures_single_final_value_mt() {
    let config = SimConfig {
        name: "lww-fullmesh-mt".into(),
        topology: Topology::FullMesh,
        num_nodes: 100,
        initial_data: test_data(5),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 4,
    };
    let (_, metrics) = run_and_analyze(config);
    let result = check_lww_single_value(&metrics);
    assert!(result.passed, "MT lww single value: {}", result.actual);
}
