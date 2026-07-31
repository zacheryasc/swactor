
//! MVP stage control, shard planning, and weight lifecycle public surface.

pub(crate) mod control;
pub(crate) mod gguf_metadata;
#[cfg(test)]
pub(crate) mod weight_lifecycle;

pub(crate) use control::*;
