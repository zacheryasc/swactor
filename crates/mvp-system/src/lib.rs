#[cfg(test)]
extern crate self as mvp_system;

pub mod actors;
pub mod arena_manager;
pub mod benchmark_observability;
pub mod bootstrap_datastream;
pub mod config;
pub mod dashboard_view;
pub mod device_bridge;
pub mod distribution_stack;
pub mod docker_cluster_provisioning;
pub mod driver_pumps;
pub mod edge_establisher;
pub mod endpoint_advertisement;
pub mod engine_builder;
pub mod gguf_metadata;
pub mod gpu_worker_ctl;
pub mod gpu_worker_egress_producer;
pub mod gpu_worker_ingress_parser;
pub mod gpu_worker_process_adapter;
pub mod membership_pool_readiness;
pub mod mvp_chat;
pub mod node_boot_lifecycle;
pub mod node_image;
pub mod node_provisioning;
pub mod observability_surface;
pub mod orchestrator_app;
pub mod orchestrator_run_fsm;
pub mod orchestrator_token_endpoint;
pub mod prompt_rpc;
pub mod provisioning;
pub mod relay_provisioning;
pub mod resource_inventory;
pub mod run_plan;
pub mod shard_fetch;
pub mod shard_weight_lifecycle;
pub mod shared_ring_helper_abi;
pub mod stage_controller;
pub mod telemetry;
pub mod tx_rx_edge_actor;
pub mod vastai_offer_preview;
pub mod vastai_provisioning;
pub mod weight_lifecycle;
pub mod weight_shards;

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
