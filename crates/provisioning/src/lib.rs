//! Reusable provider-neutral provisioning contracts.
//!
//! This crate owns provider-neutral lifecycle facts and commands together with
//! the level-triggered reconciler and identity-aware executor contracts. Concrete
//! provider adapters live in application crates and implement the executor
//! backend contract.

pub mod executor;
pub mod node;
pub mod plugin;
pub mod reconciler;

pub use executor::*;
pub use node::*;
pub use reconciler::*;
