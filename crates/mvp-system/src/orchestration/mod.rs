//! MVP run orchestration public surface.
//!
//! This module owns run planning, membership/readiness coordination, deployment
//! dispatch, run authority, prompt admission, and teardown behavior. Provider
//! adapters live in `provider_adapters` so provider-neutral orchestration logic
//! stays separate from local/Docker/VastAI implementation details.

pub mod actor;
pub(crate) mod app;
pub mod config;
pub mod distribution_stack;
#[cfg(test)]
pub mod engine_builder;

pub mod provider_adapters {
    pub mod relay;
    pub(super) mod vastai;
}
