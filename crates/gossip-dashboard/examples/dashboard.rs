use gossip_dashboard::{DashboardConfig, config::SimFileConfig, run_with_dashboard, save_trace};
use swactor_gossip::sim::{SimConfig, Topology};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let (config, dash) = if let Some(path) = args.get(1) {
        let file_config = SimFileConfig::load(path).expect("failed to load config file");
        file_config.into_sim_config()
    } else {
        let config = SimConfig {
            name: "Ring-10 Demo".to_string(),
            topology: Topology::Ring,
            num_nodes: 10,
            initial_data: vec![
                ("color".into(), b"blue".to_vec()),
                ("version".into(), b"1".to_vec()),
                ("status".into(), b"active".to_vec()),
            ],
            num_rounds: 15,
            ticks_per_round: 5,
            heal_after_round: None,
            num_threads: 2,
        };
        let dash = DashboardConfig { port: 8080 };
        (config, dash)
    };

    eprintln!("Starting gossip dashboard at http://localhost:{}", dash.port);
    eprintln!("Open in your browser to see the simulation live.");

    let trace = run_with_dashboard(config, dash);

    let path = "demo.trace.json";
    save_trace(&trace, path).expect("failed to save trace");
    eprintln!("Trace saved to {path}");
    eprintln!(
        "Replay with: cargo run -p gossip-dashboard --example replay -- {path}"
    );
}
