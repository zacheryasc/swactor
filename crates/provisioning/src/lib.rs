//! Reusable provider-neutral provisioning contracts.
//!
//! This crate owns lease, boot, destroy, bootstrap-session, and provider plugin
//! state machines that do not depend on GGUF, prompts, stage execution, Docker,
//! VastAI, or MVP runtime policy. Concrete provider adapters live in application
//! crates and implement these contracts.

pub mod node;
pub mod plugin;

pub use node::*;
