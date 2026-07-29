//! MVP node-local runtime public surface.
//!
//! Worker-node runtime behavior lives behind this module boundary; binaries
//! only wire entrypoints into it.

mod worker_node_runtime;

pub(super) fn run_worker_node_from_env() -> std::process::ExitCode {
    worker_node_runtime::run_from_env()
}
