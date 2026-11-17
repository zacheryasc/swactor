use std::process::ExitCode;
use swactor_engine::{Engine, TokioBackend, TokioConfig};

fn main() -> ExitCode {
    if let Some(exit) = myelin::run_worker_helper_from_env() {
        return exit;
    }
    let result = (|| {
        let (parts, run) = myelin::prepare_worker_node()?;
        let engine = Engine::new(
            parts,
            TokioBackend::new(TokioConfig::default()).map_err(|error| error.to_string())?,
        )
        .map_err(|error| format!("create engine: {error}"))?;
        let tasks = engine.task_spawner();
        run(engine, tasks)
    })();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("myelin-worker: {error}");
            ExitCode::from(1)
        }
    }
}
