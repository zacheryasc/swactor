// Engine boundary enforcement: disallowed scheduling, time, and core-driving
// methods are hard errors in this crate (ENGINE_SPEC.md §2).
#![deny(clippy::disallowed_methods)]
#![recursion_limit = "256"]

#[cfg(test)]
extern crate self as myelin;

// TODO: Remove defaults referring to specific named models like this, if we need a specific named model, it shall be declared and imported elsewhere
const DEFAULT_PIPELINE_CACHED_MODEL_FILE: &str = "SmolLM2-135M-Instruct.Q4_0.gguf";
#[doc(hidden)]
pub const ORCHESTRATOR_WORKER_MODE_ARG: &str = "--myelin-worker-node";
pub use node::worker_node_runtime::{
    prepare_worker_node, run_helper_from_env as run_worker_helper_from_env,
};
pub use orchestration::app::prepare_with_options as prepare_orchestrator_with_options;
pub use orchestration::app::{
    ProviderComposition, ProviderContext,
    prepare_with_providers as prepare_orchestrator_with_providers,
};

/// Operational provider implementations available to alternate composition roots.
pub mod providers {
    pub use crate::orchestration::app::{derive_ssh_public_key, ensure_vastai_account_ssh_key};
    pub use crate::orchestration::manual_control::OfferSearchRequestExt;
    pub use crate::orchestration::provider_adapters::ssh_bootstrap::{
        BootstrapEnvSource, DEPLOYED_WORKER_BIN_PATH, SshArtifactBootstrapLauncher,
        SshBootstrapHandle, SshBootstrapLauncher, SshCommandBootstrapLauncher, SshEndpoint,
    };
    pub use crate::orchestration::provider_adapters::vastai::{
        CleanupObserver, OwnershipRecorder, ProvisionRequest, SnapshotObserver, ToolsVastAiLeaseClient, VastAiLeaseClient,
        VastAiProviderMonitor, VastAiProvisioningConfig, VastAiProvisioningPlugin, VastAiTaskPort,
    };
    pub use crate::orchestration::provider_adapters::vastai::webhooks::{
        VastAiWebhookConfig, VastAiWebhookSubscription, VastAiWebhooks,
    };
    pub use crate::provisioning::{
        LocalDockerPlugin, LocalProcessPlugin, docker_container_is_absent,
        docker_container_is_running,
    };
}

pub mod contextual_process;
mod data_namespace;

// TODO: Why the explicit paths?
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
mod orchestration;
mod staging;

#[cfg(test)]
mod tests;
