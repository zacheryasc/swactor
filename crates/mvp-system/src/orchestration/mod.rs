//! MVP run orchestration public surface.
//!
//! This module owns run planning, membership/readiness coordination, deployment
//! dispatch, run authority, prompt admission, and teardown behavior. Provider
//! adapters live in `provider_adapters` so provider-neutral orchestration logic
//! stays separate from local/Docker/VastAI implementation details.

pub mod actor;
pub mod app;
pub mod config;
pub mod distribution_stack;
pub mod engine_builder;
pub mod membership_readiness;
pub mod node_provisioning;
pub mod provisioning;
pub mod resource_inventory;
pub mod run_fsm;
pub mod run_plan;
pub mod token_endpoint;

pub mod provider_adapters {
    pub mod docker_cluster;
    pub mod relay;
    pub mod vastai;
}

pub use app::*;
pub use node_provisioning::{ProviderKind, ProviderPlugin};
pub use run_fsm::{OrchestratorRun, RunConfig, RunId};
pub use run_plan::{GgufSource, TokenizerSource};
