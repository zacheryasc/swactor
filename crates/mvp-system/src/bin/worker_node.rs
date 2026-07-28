use std::process::ExitCode;

fn main() -> ExitCode {
    mvp_system::node::worker_node_runtime::run_from_env()
}
