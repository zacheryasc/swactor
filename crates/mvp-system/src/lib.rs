#![recursion_limit = "256"]

#[cfg(test)]
extern crate self as mvp_system;

pub mod chat;
pub mod node;
pub mod node_data;
pub mod observability;
pub mod orchestration;
pub mod prompt;
pub mod staging;
pub mod transport;
pub mod worker;

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "tests/driver_pumps_guarantees.rs"]
mod driver_pumps_guarantees;

#[cfg(test)]
#[path = "tests/shard_fetch_guarantees.rs"]
mod shard_fetch_guarantees;
#[cfg(test)]
#[path = "tests/shard_weight_lifecycle_guarantees.rs"]
mod shard_weight_lifecycle_guarantees;
#[cfg(test)]
#[path = "tests/weight_shards_guarantees.rs"]
mod weight_shards_guarantees;
