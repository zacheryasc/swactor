#![allow(dead_code)]

//! MVP stage control, shard planning, and weight lifecycle public surface.

#[cfg(test)]
pub mod actor;
pub mod control;
pub mod gguf_metadata;
#[cfg(test)]
pub mod shard_fetch;
#[cfg(test)]
pub mod shard_weight_lifecycle;
#[cfg(test)]
pub mod weight_lifecycle;
#[cfg(test)]
pub mod weight_shards;

pub use control::*;
