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
    chat::run_from_args(args)
}

pub fn run_orchestrator_from_args<I>(args: I) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    orchestration::run_from_args(args)
}

pub fn run_worker_node_from_env() -> std::process::ExitCode {
    node::run_worker_node_from_env()
}

fn run_orchestrator_in_process_from_args<I>(
    args: I,
    stop_rx: std::sync::mpsc::Receiver<()>,
) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    orchestration::run_in_process_from_args(args, stop_rx)
}

#[path = "transport/driver_pumps.rs"]
mod driver_pumps;
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
mod transport;

#[cfg(test)]
mod tests;
