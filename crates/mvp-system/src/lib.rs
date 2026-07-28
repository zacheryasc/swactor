#![recursion_limit = "256"]

#[cfg(test)]
extern crate self as mvp_system;

mod actors;
mod arena_manager;
mod benchmark_observability;
mod bootstrap_datastream;
pub mod chat;
mod config;
mod dashboard_view;
mod driver_pumps;
mod edge_establisher;
mod endpoint_advertisement;
mod engine_builder;
mod gpu_worker_ingress_parser;
pub mod node;
mod node_boot_lifecycle;
pub mod node_data;
mod node_image;
pub mod observability;
mod observability_surface;
pub mod orchestration;
pub mod prompt;
mod prompt_rpc;
mod resource_inventory;
pub mod staging;
mod telemetry;
pub mod transport;
mod tx_rx_edge_actor;
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
