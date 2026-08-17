use std::process::ExitCode;

fn main() -> ExitCode {
    myelin::run_job_worker_from_args(std::env::args().skip(1))
}
