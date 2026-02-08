use std::fs;

use swactor_gossip::properties::*;
use swactor_gossip::property_report::*;
use swactor_gossip::sim::{run_simulation, SimConfig, Topology};

fn main() {
    println!("=== Gossip Protocol Property Verification Report ===\n");

    let mut sections = Vec::new();
    let mut convergence_overlays = Vec::new();
    let mut scaling_points_st = Vec::new();
    let mut scaling_points_mt = Vec::new();
    let mut thread_comparisons = Vec::new();

    // ── Section 1: Reliability ──────────────────────────────────────────
    {
        println!("[1/12] Reliability...");
        let mut scenarios = Vec::new();

        let config = base_config_with("FullMesh 100", Topology::FullMesh, 100, 5, 30, 1);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        convergence_overlays.push(("FullMesh 100".into(), metrics.convergence_curve.clone()));
        scenarios.push(ScenarioReport {
            name: "FullMesh 100 nodes, 5 keys, 30 rounds".into(),
            description: "Single-threaded full-mesh topology".into(),
            metrics: metrics.clone(),
            results: vec![
                check_delivery_ratio(&metrics, 1.0),
                check_atomic_delivery(&metrics),
            ],
        });

        let config = base_config_with("Ring 100", Topology::Ring, 100, 5, 120, 1);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        convergence_overlays.push(("Ring 100".into(), metrics.convergence_curve.clone()));
        scenarios.push(ScenarioReport {
            name: "Ring 100 nodes, 5 keys, 120 rounds".into(),
            description: "Single-threaded ring topology (needs ~N rounds)".into(),
            metrics: metrics.clone(),
            results: vec![check_delivery_ratio(&metrics, 1.0)],
        });

        sections.push(ReportSection {
            title: "Reliability".into(),
            explanation: "Verifies that all nodes eventually receive all keys. Delivery ratio should be 1.0 and delivery should be atomic per key.".into(),
            scenarios,
        });
    }

    // ── Section 2: Latency ──────────────────────────────────────────────
    {
        println!("[2/12] Latency...");
        let mut scenarios = Vec::new();

        let n = 100;
        let bound = 4 * ((n as f64).ln().ceil() as usize);
        let config = base_config_with("FullMesh Latency", Topology::FullMesh, n, 5, 30, 1);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        scenarios.push(ScenarioReport {
            name: "FullMesh 100 nodes - O(log N) convergence".into(),
            description: format!("Should converge within 4*ln(N) = {} rounds", bound),
            metrics: metrics.clone(),
            results: vec![
                check_convergence_bound(&metrics, bound),
                check_last_node_latency(&metrics, 5),
            ],
        });

        let config = base_config_with("Ring Latency", Topology::Ring, 200, 5, 220, 1);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        scenarios.push(ScenarioReport {
            name: "Ring 200 nodes - O(N) convergence".into(),
            description: "Ring should converge within N rounds".into(),
            metrics: metrics.clone(),
            results: vec![check_convergence_bound(&metrics, 200)],
        });

        sections.push(ReportSection {
            title: "Latency".into(),
            explanation: "Measures convergence speed across topologies. FullMesh converges in O(log N), ring in O(N).".into(),
            scenarios,
        });
    }

    // ── Section 3: Message Complexity ───────────────────────────────────
    {
        println!("[3/12] Message Complexity...");
        let mut scenarios = Vec::new();

        let config = base_config_with("Ring MsgCount", Topology::Ring, 500, 5, 30, 1);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        scenarios.push(ScenarioReport {
            name: "Ring 500 nodes, 30 rounds".into(),
            description: "Each node sends exactly 1 push per round in ring".into(),
            metrics: metrics.clone(),
            results: vec![
                check_total_pushes_eq(&metrics, 500 * 30),
                check_redundancy_above(&metrics, 0.0),
            ],
        });

        let config = base_config_with("FullMesh Redundancy", Topology::FullMesh, 100, 5, 50, 1);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        scenarios.push(ScenarioReport {
            name: "FullMesh 100 nodes, 50 rounds".into(),
            description: "After convergence, most pushes are redundant".into(),
            metrics: metrics.clone(),
            results: vec![check_redundancy_above(&metrics, 0.3)],
        });

        sections.push(ReportSection {
            title: "Message Complexity".into(),
            explanation: "Analyzes message overhead: total pushes, useful vs redundant, and per-topology efficiency.".into(),
            scenarios,
        });
    }

    // ── Section 4: Bandwidth/Load ───────────────────────────────────────
    {
        println!("[4/12] Bandwidth/Load...");
        let mut scenarios = Vec::new();

        let config = base_config_with("Star Hub", Topology::Star, 100, 5, 30, 1);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        scenarios.push(ScenarioReport {
            name: "Star 100 nodes - hub hotspot".into(),
            description: "Hub node-0 should receive the most pushes".into(),
            metrics: metrics.clone(),
            results: vec![check_hub_is_hotspot(&metrics, "node-0")],
        });

        let config = base_config_with("Ring Load", Topology::Ring, 500, 5, 60, 1);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        scenarios.push(ScenarioReport {
            name: "Ring 500 nodes - load balance".into(),
            description: "Ring should distribute load evenly across nodes".into(),
            metrics: metrics.clone(),
            results: vec![check_load_balance_cv(&metrics, 0.3)],
        });

        sections.push(ReportSection {
            title: "Bandwidth/Load".into(),
            explanation: "Examines how push traffic is distributed across nodes. Star topologies create hotspots at the hub.".into(),
            scenarios,
        });
    }

    // ── Section 5: Convergence ──────────────────────────────────────────
    {
        println!("[5/12] Convergence...");
        let mut scenarios = Vec::new();

        let config = base_config_with("FullMesh Conv", Topology::FullMesh, 100, 5, 30, 1);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        scenarios.push(ScenarioReport {
            name: "FullMesh 100 nodes - convergence curve".into(),
            description: "Convergence curve should be monotonic with zero residue".into(),
            metrics: metrics.clone(),
            results: vec![
                check_curve_monotonic(&metrics),
                check_curve_s_shape(&metrics),
                check_zero_residue(&metrics),
            ],
        });

        sections.push(ReportSection {
            title: "Convergence".into(),
            explanation: "Verifies convergence curve properties: monotonicity, S-shape for dense topologies, and zero residue.".into(),
            scenarios,
        });
    }

    // ── Section 6: Fault Tolerance ──────────────────────────────────────
    {
        println!("[6/12] Fault Tolerance...");
        let mut scenarios = Vec::new();

        let mut config = base_config_with("Partition NoHeal", Topology::Partitioned, 100, 5, 40, 1);
        config.heal_after_round = None;
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        scenarios.push(ScenarioReport {
            name: "Partitioned 100 nodes, no heal".into(),
            description: "Partitioned network cannot fully converge".into(),
            metrics: metrics.clone(),
            results: vec![check_partition_no_converge(&metrics)],
        });

        let mut config = base_config_with("Partition Heal", Topology::Partitioned, 100, 5, 300, 1);
        config.heal_after_round = Some(100);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        scenarios.push(ScenarioReport {
            name: "Partitioned 100 nodes, heal at round 100".into(),
            description: "After healing, full convergence should be achieved".into(),
            metrics: metrics.clone(),
            results: vec![
                check_partition_heals(&metrics),
                check_partial_before_heal(&metrics, 100),
            ],
        });

        sections.push(ReportSection {
            title: "Fault Tolerance".into(),
            explanation: "Tests behavior under network partitions and recovery after healing.".into(),
            scenarios,
        });
    }

    // ── Section 7: Push Protocol ────────────────────────────────────────
    {
        println!("[7/12] Push Protocol...");
        let mut scenarios = Vec::new();

        let config = base_config_with("Push Proto", Topology::Ring, 500, 5, 10, 1);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        scenarios.push(ScenarioReport {
            name: "Ring 500 nodes, 10 rounds".into(),
            description: "One push per node per round".into(),
            metrics: metrics.clone(),
            results: vec![check_one_push_per_node_per_round(&metrics, 10)],
        });

        let config = base_config_with("No Push Chain", Topology::Chain, 100, 1, 20, 1);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        let last_node = format!("node-{}", 99);
        scenarios.push(ScenarioReport {
            name: "Chain 100 nodes - last node".into(),
            description: "Last node in chain has no peers, should never push".into(),
            metrics: metrics.clone(),
            results: vec![check_no_push_without_peers(&trace, &last_node)],
        });

        sections.push(ReportSection {
            title: "Push Protocol".into(),
            explanation: "Verifies the push protocol mechanics: exactly one push per node per round, no pushes without peers.".into(),
            scenarios,
        });
    }

    // ── Section 8: Peer Selection ───────────────────────────────────────
    {
        println!("[8/12] Peer Selection...");
        let config = base_config_with("Peer Select", Topology::Star, 10, 1, 500, 1);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        sections.push(ReportSection {
            title: "Peer Selection".into(),
            explanation: "Verifies that peer selection is approximately uniform using chi-squared test.".into(),
            scenarios: vec![ScenarioReport {
                name: "Star 10 nodes, 500 rounds".into(),
                description: "Node-0 has 9 peers, should select each approximately uniformly".into(),
                metrics: metrics.clone(),
                results: vec![check_peer_selection_uniform(&metrics, 26.12)],
            }],
        });
    }

    // ── Section 9: Topology Impact ──────────────────────────────────────
    {
        println!("[9/12] Topology Impact...");
        let mut scenarios = Vec::new();

        let topos = [
            ("FullMesh", Topology::FullMesh, 30),
            ("Star", Topology::Star, 40),
            ("Ring", Topology::Ring, 120),
            ("Chain", Topology::Chain, 120),
        ];
        for (name, topo, rounds) in &topos {
            let config = base_config_with(
                &format!("Topo-{name}"),
                topo.clone(),
                100,
                5,
                *rounds,
                1,
            );
            let trace = run_simulation(config);
            let metrics = analyze(&trace);
            scenarios.push(ScenarioReport {
                name: format!("{name} 100 nodes"),
                description: format!("Convergence round: {:?}", metrics.convergence_round),
                metrics,
                results: vec![],
            });
        }

        sections.push(ReportSection {
            title: "Topology Impact".into(),
            explanation: "Compares convergence speed and efficiency across topologies. Denser topologies converge faster but with more redundancy.".into(),
            scenarios,
        });
    }

    // ── Section 10: Consistency ─────────────────────────────────────────
    {
        println!("[10/12] Consistency...");
        let mut scenarios = Vec::new();

        let config = base_config_with("LWW FullMesh", Topology::FullMesh, 100, 5, 30, 1);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        scenarios.push(ScenarioReport {
            name: "FullMesh 100 nodes - LWW consistency".into(),
            description: "All keys should have exactly 1 distinct final value".into(),
            metrics: metrics.clone(),
            results: vec![
                check_lww_single_value(&metrics),
                check_no_stale_reads(&metrics),
            ],
        });

        let config = base_config_with("Entropy FullMesh", Topology::FullMesh, 100, 5, 30, 1);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        scenarios.push(ScenarioReport {
            name: "FullMesh 100 nodes - entropy".into(),
            description: "Entropy should reach zero at convergence".into(),
            metrics: metrics.clone(),
            results: vec![check_entropy_zero_at_convergence(&metrics)],
        });

        sections.push(ReportSection {
            title: "Consistency".into(),
            explanation: "Verifies LWW consistency: single final value per key, monotonically decreasing entropy, no stale reads post-convergence.".into(),
            scenarios,
        });
    }

    // ── Section 11: Practical ───────────────────────────────────────────
    {
        println!("[11/12] Practical...");
        let config = base_config_with("State Size", Topology::FullMesh, 100, 5, 30, 1);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        sections.push(ReportSection {
            title: "Practical".into(),
            explanation: "Verifies practical properties: state size stabilizes at key count and grows monotonically.".into(),
            scenarios: vec![ScenarioReport {
                name: "FullMesh 100 nodes, 5 keys".into(),
                description: "State size should stabilize at 5.0 and never decrease".into(),
                metrics: metrics.clone(),
                results: vec![
                    check_state_size_stabilizes(&metrics, 5.0),
                    check_state_size_monotonic(&metrics),
                ],
            }],
        });
    }

    // ── Section 12: Multi-threaded ──────────────────────────────────────
    {
        println!("[12/12] Multi-threaded scenarios...");
        let mut scenarios = Vec::new();

        let config = base_config_with("FullMesh MT", Topology::FullMesh, 100, 5, 30, 4);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        scenarios.push(ScenarioReport {
            name: "FullMesh 100 nodes, 4 threads".into(),
            description: "Multi-threaded full-mesh should still converge".into(),
            metrics: metrics.clone(),
            results: vec![
                check_delivery_ratio(&metrics, 1.0),
                check_curve_monotonic(&metrics),
            ],
        });

        let n = 100;
        let bound = 2 * 4 * ((n as f64).ln().ceil() as usize);
        let config = base_config_with("FullMesh MT Latency", Topology::FullMesh, n, 5, 30, 4);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        scenarios.push(ScenarioReport {
            name: "FullMesh 100 nodes, 4 threads - latency".into(),
            description: format!("Multi-threaded fullmesh, bound = {}", bound),
            metrics: metrics.clone(),
            results: vec![
                check_delivery_ratio(&metrics, 1.0),
                check_convergence_bound(&metrics, bound),
            ],
        });

        let mut config = base_config_with("Partition Heal MT", Topology::Partitioned, 100, 5, 300, 4);
        config.heal_after_round = Some(100);
        let trace = run_simulation(config);
        let metrics = analyze(&trace);
        scenarios.push(ScenarioReport {
            name: "Partition heal, 4 threads".into(),
            description: "Multi-threaded partition healing".into(),
            metrics: metrics.clone(),
            results: vec![check_partition_heals(&metrics)],
        });

        sections.push(ReportSection {
            title: "Multi-threaded".into(),
            explanation: "Verifies that gossip properties hold under concurrent multi-threaded scheduling with non-deterministic message ordering.".into(),
            scenarios,
        });
    }

    // ── Scaling series (single-threaded) ────────────────────────────────
    {
        println!("Scaling series (single-threaded)...");
        for &n in &[50, 100, 200, 500] {
            let rounds = 60; // FullMesh converges in O(log N)
            let config = base_config_with(
                &format!("Scale ST N={n}"),
                Topology::FullMesh,
                n,
                5,
                rounds,
                1,
            );
            let trace = run_simulation(config);
            let metrics = analyze(&trace);
            scaling_points_st.push(ScalingPoint {
                n,
                convergence_round: metrics.convergence_round,
                total_pushes: metrics.total_pushes,
                label: format!("ST N={n}"),
            });
        }
    }

    // ── Scaling series (multi-threaded) ─────────────────────────────────
    {
        println!("Scaling series (multi-threaded)...");
        for &n in &[50, 100, 200, 500] {
            let rounds = 60;
            let config = base_config_with(
                &format!("Scale MT N={n}"),
                Topology::FullMesh,
                n,
                5,
                rounds,
                4,
            );
            let trace = run_simulation(config);
            let metrics = analyze(&trace);
            scaling_points_mt.push(ScalingPoint {
                n,
                convergence_round: metrics.convergence_round,
                total_pushes: metrics.total_pushes,
                label: format!("MT N={n}"),
            });
        }
    }

    // ── Thread-mode comparison ──────────────────────────────────────────
    {
        println!("Thread-mode comparison...");
        for &threads in &[1, 2, 4] {
            let config = base_config_with(
                &format!("FullMesh 200 {threads}T"),
                Topology::FullMesh,
                200,
                5,
                30,
                threads,
            );
            let trace = run_simulation(config);
            let metrics = analyze(&trace);
            thread_comparisons.push(ThreadComparison {
                label: "FullMesh 200 nodes".into(),
                num_threads: threads,
                convergence_round: metrics.convergence_round,
                total_pushes: metrics.total_pushes,
            });
        }
    }

    // ── Generate report ─────────────────────────────────────────────────
    let report_data = PropertyReportData {
        sections,
        scaling_points_st,
        scaling_points_mt,
        thread_comparison: thread_comparisons,
        convergence_overlays,
    };

    let html = generate_property_report(&report_data);
    let path = "gossip_properties_report.html";
    fs::write(path, &html).expect("failed to write report");
    println!("\nWrote {} ({} bytes)", path, html.len());
}

fn base_config_with(
    name: &str,
    topology: Topology,
    num_nodes: usize,
    num_keys: usize,
    num_rounds: usize,
    num_threads: usize,
) -> SimConfig {
    let mut config = base_config(name, topology, num_nodes, num_keys);
    config.num_rounds = num_rounds;
    config.num_threads = num_threads;
    config
}
