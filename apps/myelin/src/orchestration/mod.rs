//! Myelin run orchestration public surface.
//!
//! This module owns run planning, membership/readiness coordination, deployment
//! dispatch, run authority, prompt admission, and teardown behavior. Provider
//! adapters live in `provider_adapters` so provider-neutral orchestration logic
//! stays separate from local/Docker/VastAI implementation details.

pub(crate) mod actor;
pub(crate) mod app;
pub(crate) mod cluster_reconciler;
pub(crate) mod config;
pub(crate) mod daemon;
pub(crate) mod distribution_stack;
#[cfg(test)]
pub(crate) mod engine_builder;
pub(crate) mod job_reconciler;

pub(crate) mod node_image;
pub(crate) mod provider_adapters {
    pub(crate) mod relay;
    pub(super) mod vastai;
}
