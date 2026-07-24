use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Identifier for a rented vast.ai instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceInfo {
    pub contract_id: u64,
}

/// A vast.ai offer (GPU rental option) returned by the bundle search endpoint.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Offer {
    pub id: u64,
    pub gpu_name: String,
    pub dph_total: f64,
    #[serde(default)]
    pub gpu_ram: Option<f64>,
    #[serde(default)]
    pub geolocation: Option<String>,
    /// Inbound bandwidth price ($/TB). vast.ai bills Docker image pulls here.
    #[serde(default, rename = "internet_down_cost_per_tb")]
    pub inet_down_cost_per_tb: f64,
    /// Outbound bandwidth price ($/TB), surfaced so callers can account for it.
    #[serde(default, rename = "internet_up_cost_per_tb")]
    pub inet_up_cost_per_tb: f64,
    /// Marketplace host that owns the machine. Used for host-level blacklist and
    /// distinct-host provisioning.
    #[serde(default)]
    pub host_id: Option<u64>,
    /// vast.ai host verification state: `verified`, `unverified`, or
    /// `deverified`.
    #[serde(default)]
    pub verification: Option<String>,
}

/// Connection details for a running instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningInstance {
    pub ip: String,
    pub port: u16,
}

/// SSH endpoint + identity of a held instance, discovered by label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabeledInstance {
    pub contract_id: u64,
    /// vast.ai SSH proxy host (e.g. `ssh5.vast.ai`); empty if not yet assigned.
    pub ssh_host: String,
    pub ssh_port: u16,
    pub public_ipaddr: String,
    pub actual_status: String,
}

/// Offer selection knobs. Apps choose policy; this crate applies it.
#[derive(Debug, Clone)]
pub struct SelectionPolicy {
    pub gpu_name: Option<String>,
    pub min_gpu_ram_mb: Option<u64>,
    pub min_reliability: f64,
    pub require_verified: bool,
    pub min_down_mbps: f64,
    pub min_up_mbps: Option<f64>,
    pub max_dph_total: Option<f64>,
    pub blacklist_hosts: Vec<u64>,
    pub drop_cheap_frac: f64,
    pub image_size_gb: Option<f64>,
}

impl Default for SelectionPolicy {
    fn default() -> Self {
        Self {
            gpu_name: None,
            min_gpu_ram_mb: None,
            min_reliability: 0.95,
            require_verified: false,
            min_down_mbps: 100.0,
            min_up_mbps: None,
            max_dph_total: None,
            blacklist_hosts: vec![59017],
            drop_cheap_frac: 0.30,
            image_size_gb: None,
        }
    }
}

/// Provisioning and monitoring retry/timing knobs.
#[derive(Debug, Clone)]
pub struct LifecyclePolicy {
    pub lease_pace: Duration,
    pub poll_interval: Duration,
}

impl Default for LifecyclePolicy {
    fn default() -> Self {
        Self {
            lease_pace: Duration::from_millis(600),
            poll_interval: Duration::from_secs(10),
        }
    }
}

/// Generic vast.ai create-instance payload.
#[derive(Debug, Clone)]
pub struct CreateInstanceRequest {
    pub offer_id: u64,
    pub image: String,
    pub disk_gb: u32,
    pub label: Option<String>,
    pub env: BTreeMap<String, String>,
    /// Command passed to vast.ai's `onstart`; `None` omits the field.
    pub onstart: Option<String>,
}

/// Generic N-instance provision request. Apps decide what env each machine runs.
#[derive(Debug, Clone)]
pub struct ProvisionRequest {
    pub count: u32,
    pub image: String,
    pub label: Option<String>,
    pub disk_gb: u32,
    /// Env applied to every instance.
    pub env: BTreeMap<String, String>,
    /// Per-index env overlays, merged after `env`.
    pub per_instance_env: Vec<BTreeMap<String, String>>,
    pub onstart: Option<String>,
    pub selection: SelectionPolicy,
    pub lifecycle: LifecyclePolicy,
    /// Whether to print the lease plan and ask on TTY before spending money.
    pub confirm_lease: bool,
}

/// A successfully-provisioned instance with the offer facts used to pick it.
#[derive(Debug, Clone, PartialEq)]
pub struct ProvisionedInstance {
    pub index: u32,
    pub contract_id: u64,
    pub offer_id: u64,
    pub host_id: Option<u64>,
    pub gpu_name: String,
    pub gpu_ram: Option<f64>,
    pub dph_total: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProvisionedFleet {
    pub label: Option<String>,
    pub instances: Vec<ProvisionedInstance>,
}

/// Generic held-fleet handle file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FleetState {
    pub label: String,
    pub image: String,
    pub contracts: Vec<ContractRef>,
    pub created_at: u64,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContractRef {
    pub id: u64,
    pub index: u32,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SearchResponse {
    pub offers: Vec<Offer>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateResponse {
    pub new_contract: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InstanceResponse {
    pub instances: InstanceStatus,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InstanceStatus {
    pub actual_status: Option<String>,
    pub intended_status: Option<String>,
    #[serde(default)]
    pub status_msg: Option<String>,
    #[serde(default)]
    pub public_ipaddr: Option<String>,
    #[serde(default)]
    pub ssh_port: Option<u16>,
    #[serde(default)]
    pub disk_usage: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InstanceListResponse {
    pub instances: Vec<InstanceListEntry>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InstanceListEntry {
    pub id: u64,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub actual_status: Option<String>,
    #[serde(default)]
    pub ssh_host: Option<String>,
    #[serde(default)]
    pub ssh_port: Option<u16>,
    #[serde(default)]
    pub public_ipaddr: Option<String>,
}

impl From<InstanceListEntry> for LabeledInstance {
    fn from(e: InstanceListEntry) -> Self {
        Self {
            contract_id: e.id,
            ssh_host: e.ssh_host.unwrap_or_default(),
            ssh_port: e.ssh_port.unwrap_or(0),
            public_ipaddr: e.public_ipaddr.unwrap_or_default(),
            actual_status: e.actual_status.unwrap_or_else(|| "unknown".to_string()),
        }
    }
}
