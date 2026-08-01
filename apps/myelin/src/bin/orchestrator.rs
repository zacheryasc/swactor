use std::process::ExitCode;

fn main() -> ExitCode {
    match myelin::run_orchestrator_from_args(std::env::args().skip(1)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("myelin-orchestrator: {error}");
            ExitCode::from(1)
        }
    }
}
