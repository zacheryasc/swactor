use std::path::PathBuf;

use simulation_dashboard::config::SimFileConfig;
use simulation_dashboard::save_trace;
use simulation::gossip::sim::run_simulation;

const CONFIGS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/configs");

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let (out_dir, configs) = match args.len() {
        // generate_traces              → traces/ + all bundled configs
        1 => ("traces".to_string(), collect_configs(CONFIGS_DIR)),
        // generate_traces <out-dir>    → custom dir + all bundled configs
        2 if !args[1].ends_with(".toml") => (args[1].clone(), collect_configs(CONFIGS_DIR)),
        // generate_traces <out-dir> <config.toml ...>
        n if n >= 3 => (args[1].clone(), args[2..].iter().map(PathBuf::from).collect()),
        _ => {
            eprintln!("Usage:");
            eprintln!("  generate_traces                          # all configs -> traces/");
            eprintln!("  generate_traces <out-dir>                # all configs -> out-dir/");
            eprintln!("  generate_traces <out-dir> <config.toml> [more.toml ...]");
            std::process::exit(1);
        }
    };

    if configs.is_empty() {
        eprintln!("No .toml configs found in {CONFIGS_DIR}");
        std::process::exit(1);
    }

    std::fs::create_dir_all(&out_dir).expect("failed to create output directory");

    for path in &configs {
        let path_str = path.to_string_lossy();
        let file_config = SimFileConfig::load(&path_str)
            .unwrap_or_else(|e| panic!("failed to load {path_str}: {e}"));
        let config = file_config.into_sim_config();

        eprintln!("Running: {} ...", config.name);
        let trace = run_simulation(config);

        let filename = format!(
            "{}/{}.trace.json",
            out_dir,
            trace.name.to_lowercase().replace(' ', "_").replace(['(', ')'], "")
        );
        save_trace(&trace, &filename).expect("failed to save trace");
        eprintln!(
            "  -> {} ({} nodes, {} events)",
            filename,
            trace.node_names.len(),
            trace.events.len()
        );
    }
    eprintln!("Done. View with: cargo run -p simulation-dashboard --example replay -- {out_dir}");
}

fn collect_configs(dir: &str) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("cannot read configs dir {dir}: {e}"))
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "toml"))
        .collect();
    paths.sort();
    paths
}
