//! MVP run orchestration public surface.
//!
//! This module owns run planning, membership/readiness coordination, deployment
//! dispatch, run authority, prompt admission, and teardown behavior. Provider
//! adapters live in `provider_adapters` so provider-neutral orchestration logic
//! stays separate from local/Docker/VastAI implementation details.

pub mod actor;
mod app;
pub mod config;
pub mod distribution_stack;
#[cfg(test)]
pub mod engine_builder;

pub mod provider_adapters {
    pub mod relay;
    pub(super) mod vastai;
}

pub(super) fn run_from_args<I>(args: I) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    app::run_from_args(args)
}

pub(super) fn run_in_process_from_args<I>(
    args: I,
    stop_rx: std::sync::mpsc::Receiver<()>,
) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    app::run_in_process_from_args(args, stop_rx)
}
