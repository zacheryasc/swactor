#![cfg(not(test))]

use std::process::ExitCode;
use swactor::config::RuntimeConfig;
use swactor::runtime::RuntimeParts;
use swactor_engine::{Engine, TokioBackend, TokioConfig};
use xtask::{check_telemetry_isolation, demo, print_usage, run_lint, run_tests};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("check-telemetry-isolation") => {
            if check_telemetry_isolation() {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
        Some("lint") if args.next().is_none() => run_lint(),
        Some("test") if args.next().is_none() => run_tests(),
        Some("demo") => {
            let parts = RuntimeParts::new(RuntimeConfig::default());
            let runtime = parts.runtime().clone();
            let backend = match TokioBackend::new(TokioConfig::default()) {
                Ok(backend) => backend,
                Err(error) => {
                    eprintln!("demo: backend: {error}");
                    return ExitCode::FAILURE;
                }
            };
            let engine = match Engine::new(parts, backend) {
                Ok(engine) => engine,
                Err(error) => {
                    eprintln!("demo: engine: {error}");
                    return ExitCode::FAILURE;
                }
            };
            let tasks = engine.task_spawner();
            demo::run(&args.collect::<Vec<_>>(), runtime, engine, tasks)
        }
        Some("help" | "--help" | "-h") | None => {
            print_usage();
            ExitCode::SUCCESS
        }
        _ => {
            print_usage();
            ExitCode::from(1)
        }
    }
}
