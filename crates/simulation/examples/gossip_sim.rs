use simulation::gossip::sim::{run_simulation, GossipSimConfig};
use simulation::gossip::properties::{analyze, check_delivery_ratio, check_convergence_bound};
use simulation::topology::Topology;

fn main() {
    let config = GossipSimConfig {
        name: "ring-10-example".into(),
        topology: Topology::Ring,
        num_nodes: 10,
        initial_data: (0..3)
            .map(|i| (format!("key-{i}"), format!("value-{i}").into_bytes()))
            .collect(),
        num_rounds: 30,
        ticks_per_round: 3,
        heal_after_round: None,
        num_threads: 1,
    };

    println!("Running gossip simulation: {}", config.name);
    println!(
        "  topology={:?}  nodes={}  rounds={}  keys={}",
        config.topology,
        config.num_nodes,
        config.num_rounds,
        config.initial_data.len(),
    );

    let trace = run_simulation(config);
    let metrics = analyze(&trace);

    println!("\nResults:");
    println!("  delivery ratio:    {:.1}%", metrics.delivery_ratio * 100.0);
    println!("  atomic delivery:   {}", metrics.atomic_delivery);
    println!(
        "  convergence round: {}",
        metrics
            .convergence_round
            .map(|r| r.to_string())
            .unwrap_or_else(|| "never".into())
    );
    println!("  total pushes:      {}", metrics.total_pushes);
    println!("  redundant pushes:  {}", metrics.redundant_pushes);
    println!("  load balance CV:   {:.3}", metrics.load_balance_cv);

    let dr = check_delivery_ratio(&metrics, 1.0);
    let cr = check_convergence_bound(&metrics, 20);
    println!("\nProperty checks:");
    println!("  delivery >= 100%:   {}", if dr.passed { "PASS" } else { "FAIL" });
    println!("  converge <= 20 rds: {}", if cr.passed { "PASS" } else { "FAIL" });
}
