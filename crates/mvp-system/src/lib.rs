#![recursion_limit = "256"]

#[cfg(test)]
extern crate self as mvp_system;

const DEFAULT_PIPELINE_CACHED_MODEL_FILE: &str = "SmolLM2-135M-Instruct.Q4_0.gguf";
const DEFAULT_PIPELINE_CACHED_MODEL_REPO: &str = "QuantFactory/SmolLM2-135M-Instruct-GGUF";
const DEFAULT_PIPELINE_CACHED_MODEL_ID: &str = "smollm2-135m-instruct-q4";
const DEFAULT_PIPELINE_CACHED_MODEL_MAX_CONTEXT: u32 = 256;

pub fn run_chat_from_args<I>(args: I) -> std::process::ExitCode
where
    I: IntoIterator<Item = String>,
{
    chat::runtime::run_from_args(args)
}

pub fn run_orchestrator_from_args<I>(args: I) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    orchestration::app::run_with_options(args, true, None)
}

pub fn run_worker_node_from_env() -> std::process::ExitCode {
    node::worker_node_runtime::run_from_env()
}

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

mod chat;
mod node;
mod observability;
mod orchestration;
mod prompt;
mod staging;
mod codecs;

#[cfg(test)]
mod tests;
