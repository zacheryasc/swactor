// Engine boundary enforcement: disallowed scheduling, time, and core-driving
// methods are hard errors in this crate (ENGINE_SPEC.md §2).
#![deny(clippy::disallowed_methods)]
#![recursion_limit = "256"]

#[cfg(test)]
extern crate self as myelin;

const DEFAULT_PIPELINE_CACHED_MODEL_FILE: &str = "SmolLM2-135M-Instruct.Q4_0.gguf";
#[doc(hidden)]
pub const ORCHESTRATOR_WORKER_MODE_ARG: &str = "--myelin-worker-node";
pub fn run_orchestrator_from_args<I>(args: I) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    orchestration::app::run_with_options(args, true, None)
}

pub fn run_worker_node_from_env() -> std::process::ExitCode {
    node::worker_node_runtime::run_from_env()
}

pub mod contextual_process;
mod data_namespace;

#[path = "staging/gguf_common.rs"]
mod gguf_common;
#[path = "staging/gguf_shard.rs"]
mod gguf_shard;
#[path = "node/actor.rs"]
mod node_actor;
#[path = "orchestration/node_provisioning.rs"]
mod node_provisioning;
#[path = "orchestration/provisioning.rs"]
mod provisioning;
#[path = "orchestration/run_fsm.rs"]
mod run_fsm;

#[path = "orchestration/run_plan.rs"]
mod run_plan;

mod codecs;
mod node;
mod observability;
mod orchestration;
mod staging;

#[cfg(test)]
mod tests;
