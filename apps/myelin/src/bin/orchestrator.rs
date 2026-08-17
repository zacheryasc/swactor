use std::process::ExitCode;

fn main() -> ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.as_slice() == [myelin::ORCHESTRATOR_WORKER_MODE_ARG] {
        return myelin::run_worker_node_from_env();
    }

    match myelin::run_orchestrator_from_args(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("myelin-orchestrator: {error}");
            ExitCode::from(1)
        }
    }
}
