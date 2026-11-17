#![cfg(not(test))]

use std::process::ExitCode;
use swactor_engine::{Engine, TokioBackend, TokioConfig};

fn main() -> ExitCode {
    let result = (|| {
        let (parts, run) = myelin_e2e_fuzz::fixture_providers::prepare_orchestrator(
            std::env::args().skip(1).collect(),
        )?;
        let engine = Engine::new(
            parts,
            TokioBackend::new(TokioConfig::default()).map_err(|error| error.to_string())?,
        )
        .map_err(|error| format!("create fixture orchestrator engine: {error}"))?;
        let tasks = engine.task_spawner();
        run(engine, tasks)
    })();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("myelin-fixture-orchestrator: {error}");
            ExitCode::from(1)
        }
    }
}
