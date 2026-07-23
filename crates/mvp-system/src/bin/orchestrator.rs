use std::process::ExitCode;

fn main() -> ExitCode {
    match mvp_system::orchestrator_app::run_from_args(std::env::args().skip(1)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mvp-orchestrator: {error}");
            ExitCode::from(1)
        }
    }
}
