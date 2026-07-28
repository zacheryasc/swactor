//! MVP stage control, shard planning, and weight lifecycle public surface.

pub mod control;
pub mod gguf_metadata;
pub mod gguf_shard;
pub mod shard_fetch;
pub mod shard_weight_lifecycle;
pub mod weight_lifecycle;
pub mod weight_shards;

pub mod actor {
    pub use crate::actors::stage_controller::*;
}
pub use control::*;
