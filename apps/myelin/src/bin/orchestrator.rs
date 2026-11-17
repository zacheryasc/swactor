use std::process::ExitCode;
use swactor_engine::{Engine, TokioBackend, TokioConfig};

fn main() -> ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let result = if args.as_slice() == [myelin::ORCHESTRATOR_WORKER_MODE_ARG] {
        if let Some(exit) = myelin::run_worker_helper_from_env() {
            return exit;
        }
        (|| {
            let (parts, run) = myelin::prepare_worker_node()?;
            let engine = Engine::new(
                parts,
                TokioBackend::new(TokioConfig::default()).map_err(|error| error.to_string())?,
            )
            .map_err(|error| format!("create engine: {error}"))?;
            let tasks = engine.task_spawner();
            run(engine, tasks)
        })()
    } else {
        (|| {
            let (parts, run) = myelin::prepare_orchestrator_with_options(args, true, None)?;
            let engine = Engine::new(
                parts,
                TokioBackend::new(TokioConfig::default()).map_err(|error| error.to_string())?,
            )
            .map_err(|error| format!("create engine: {error}"))?;
            let tasks = engine.task_spawner();
            run(engine, tasks)
        })()
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("myelin-orchestrator: {error}");
            ExitCode::from(1)
        }
    }
}
