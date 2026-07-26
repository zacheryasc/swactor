//! Peripheral vast.ai provisioning utility.
//!
//! This crate is intentionally outside `crates/`: swactor itself stays a
//! general-purpose actor runtime, while this utility rents, monitors, and tears
//! down vast.ai machines for apps that choose to use it.

pub mod client;
pub mod config;
pub mod filters;
pub mod lease;
pub mod logs;
pub mod monitor;
pub mod pricing;
pub mod provision;
pub mod search;
pub mod state;
pub mod teardown;
pub mod types;

pub use client::VastClient;
pub use lease::{confirm_lease, provision_fleet};
pub use logs::{fetch_logs, request_logs};
pub use monitor::{wait_for_running, wait_for_running_with_policy};
pub use pricing::CostModel;
pub use provision::create_instance;
pub use search::{plan_distinct_host_first_wave, select_offer_pool, select_offer_pool_with_policy};
pub use teardown::{
    destroy_all_instances, destroy_instance, destroy_instance_with_retry, list_instances_by_label,
};
pub use types::{
    ContractRef, CreateInstanceRequest, FleetState, InstanceInfo, LabeledInstance, LifecyclePolicy,
    Offer, ProvisionRequest, ProvisionedFleet, ProvisionedInstance, RunningInstance,
    SelectionPolicy, VastAiFailureClass, classify_vastai_error,
};
