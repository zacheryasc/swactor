use std::fs;

use swactor_gossip::report::generate_html_report;
use swactor_gossip::sim::{run_simulation, SimConfig, Topology};

fn main() {
    let scenarios = vec![
        SimConfig {
            name: "Ring (5 nodes)".into(),
            topology: Topology::Ring,
            num_nodes: 5,
            initial_data: test_data(3),
            num_rounds: 15,
            ticks_per_round: 4,
            heal_after_round: None,
            num_threads: 1,
        },
        SimConfig {
            name: "Star (7 nodes)".into(),
            topology: Topology::Star,
            num_nodes: 7,
            initial_data: test_data(3),
            num_rounds: 10,
            ticks_per_round: 4,
            heal_after_round: None,
            num_threads: 1,
        },
        SimConfig {
            name: "Full Mesh (5 nodes)".into(),
            topology: Topology::FullMesh,
            num_nodes: 5,
            initial_data: test_data(3),
            num_rounds: 8,
            ticks_per_round: 4,
            heal_after_round: None,
            num_threads: 1,
        },
        SimConfig {
            name: "Chain (8 nodes)".into(),
            topology: Topology::Chain,
            num_nodes: 8,
            initial_data: test_data(3),
            num_rounds: 20,
            ticks_per_round: 4,
            heal_after_round: None,
            num_threads: 1,
        },
        SimConfig {
            name: "Partition & Heal (6 nodes)".into(),
            topology: Topology::Partitioned,
            num_nodes: 6,
            initial_data: test_data(3),
            num_rounds: 20,
            ticks_per_round: 4,
            heal_after_round: Some(10),
            num_threads: 1,
        },
    ];

    for config in scenarios {
        let filename = format!(
            "gossip_report_{}.html",
            config.name.to_lowercase().replace(' ', "_").replace(['(', ')'], "")
        );
        println!("Running scenario: {} ...", config.name);
        let trace = run_simulation(config);
        let html = generate_html_report(&trace);
        fs::write(&filename, &html).expect("failed to write report");
        println!("  -> wrote {filename} ({} bytes)", html.len());
    }
    println!("Done.");
}

fn test_data(n: usize) -> Vec<(String, Vec<u8>)> {
    (0..n)
        .map(|i| (format!("key-{i}"), format!("value-{i}").into_bytes()))
        .collect()
}
