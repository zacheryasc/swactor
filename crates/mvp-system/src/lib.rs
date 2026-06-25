#[cfg(test)]
extern crate self as mvp_system;

pub mod actors;
pub mod arena_manager;
#[cfg(feature = "local-e2e")]
pub mod dashboard_view;
pub mod device_bridge;
pub mod distribution_stack;
pub mod driver_pumps;
pub mod edge_establisher;
pub mod engine_builder;
pub mod gpu_worker_ctl;
pub mod gpu_worker_egress_producer;
pub mod gpu_worker_ingress_parser;
pub mod gpu_worker_process_adapter;
pub mod membership_pool_readiness;
pub mod node_boot_lifecycle;
pub mod observability_surface;
pub mod orchestrator_run_fsm;
pub mod orchestrator_token_endpoint;
pub mod provisioning;
pub mod resource_inventory;
pub mod run_plan;
pub mod shared_ring_helper_abi;
pub mod stage_controller;
pub mod telemetry;
pub mod tx_rx_edge_actor;
pub mod weight_lifecycle;

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "tests/driver_pumps_guarantees.rs"]
mod driver_pumps_guarantees;
