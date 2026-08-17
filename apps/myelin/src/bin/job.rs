use std::process::ExitCode;

fn main() -> ExitCode {
    myelin::run_job_serve_from_args(std::env::args().skip(1))
}
