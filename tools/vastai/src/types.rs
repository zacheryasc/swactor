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
    /// CUDA compute capability encoded as vast.ai reports it (`890` = 8.9).
    #[serde(default)]
    pub compute_cap: u64,
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
    /// Historical host reliability score in `[0, 1]`.
    #[serde(default)]
    pub reliability2: Option<f64>,
    /// Measured inbound and outbound bandwidth in Mbps.
    #[serde(default)]
    pub inet_down: Option<f64>,
    #[serde(default)]
    pub inet_up: Option<f64>,
}

/// Connection details for a running instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningInstance {
    pub ip: String,
    pub port: u16,
}

/// Provider status snapshot for one Vast.ai contract.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderInstanceStatus {
    pub actual_status: String,
    pub intended_status: String,
    pub status_msg: Option<String>,
    pub public_ipaddr: Option<String>,
    pub ssh_port: Option<u16>,
    pub disk_usage: Option<f64>,
}

impl ProviderInstanceStatus {
    pub fn ssh_endpoint(&self) -> Option<RunningInstance> {
        let ip = self
            .public_ipaddr
            .as_deref()
            .filter(|ip| !ip.trim().is_empty())?;
        let port = self.ssh_port.filter(|port| *port > 0)?;
        Some(RunningInstance {
            ip: ip.to_owned(),
            port,
        })
    }
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
    /// Minimum CUDA compute capability encoded as vast.ai reports it (`700` = 7.0).
    pub min_compute_cap: Option<u64>,
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
            min_compute_cap: Some(700),
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
    /// Maximum time to stay in one non-running provider state before replacing the lease.
    pub state_timeout: Duration,
}

impl Default for LifecyclePolicy {
    fn default() -> Self {
        Self {
            lease_pace: Duration::from_millis(600),
            poll_interval: Duration::from_secs(10),
            state_timeout: Duration::from_secs(300),
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
    /// Preferred offer for single-instance requests after an app-level shared
    /// first-wave planner has already coordinated distinct hosts.
    pub preferred_offer_id: Option<u64>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VastAiFailureClass {
    ConnectionRefused,
    PublicKeyDenied,
    EndpointMissing,
    ProviderLoadingTimeout,
    VanishedOffer,
    Other,
}

impl VastAiFailureClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ConnectionRefused => "connection_refused",
            Self::PublicKeyDenied => "publickey_denied",
            Self::EndpointMissing => "endpoint_missing",
            Self::ProviderLoadingTimeout => "provider_loading_timeout",
            Self::VanishedOffer => "vanished_offer",
            Self::Other => "other",
        }
    }
}

pub fn classify_vastai_error(raw: &str) -> VastAiFailureClass {
    let lower = raw.to_ascii_lowercase();
    if lower.contains("connection refused") || lower.contains("os error 111") {
        return VastAiFailureClass::ConnectionRefused;
    }
    if lower.contains("permission denied (publickey")
        || lower.contains("publickey denied")
        || lower.contains("public key denied")
        || lower.contains("no supported authentication methods")
    {
        return VastAiFailureClass::PublicKeyDenied;
    }
    if lower.contains("no ssh host")
        || lower.contains("no ssh port")
        || lower.contains("no ssh endpoint")
        || lower.contains("endpoint missing")
        || lower.contains("ssh missing")
    {
        return VastAiFailureClass::EndpointMissing;
    }
    if lower.contains("stuck in status loading")
        || lower.contains("provider loading timeout")
        || (lower.contains("status loading") && lower.contains("timeout"))
    {
        return VastAiFailureClass::ProviderLoadingTimeout;
    }
    if lower.contains("no_such_ask")
        || lower.contains("no such ask")
        || lower.contains("vanished offer")
        || (lower.contains("404") && lower.contains("/asks/"))
    {
        return VastAiFailureClass::VanishedOffer;
    }
    VastAiFailureClass::Other
}

#[cfg(test)]
mod failure_class_tests {
    use super::*;

    #[test]
    fn representative_vastai_errors_classify_to_stable_failure_classes() {
        for (raw, class) in [
            (
                "ssh: connect to host ssh5.vast.ai port 22017: Connection refused",
                VastAiFailureClass::ConnectionRefused,
            ),
            (
                "Permission denied (publickey).",
                VastAiFailureClass::PublicKeyDenied,
            ),
            (
                "vastai contract 123 has no SSH host",
                VastAiFailureClass::EndpointMissing,
            ),
            (
                "instance 123 stuck in status loading for 300s",
                VastAiFailureClass::ProviderLoadingTimeout,
            ),
            (
                "create_instance HTTP 400: {\"error\":\"no_such_ask\"}",
                VastAiFailureClass::VanishedOffer,
            ),
        ] {
            assert_eq!(classify_vastai_error(raw), class, "{raw}");
            assert_ne!(class.as_str(), "other");
        }
    }
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

impl From<InstanceStatus> for ProviderInstanceStatus {
    fn from(status: InstanceStatus) -> Self {
        Self {
            actual_status: status
                .actual_status
                .unwrap_or_else(|| "unknown".to_string()),
            intended_status: status
                .intended_status
                .unwrap_or_else(|| "unknown".to_string()),
            status_msg: status.status_msg,
            public_ipaddr: status.public_ipaddr,
            ssh_port: status.ssh_port,
            disk_usage: status.disk_usage,
        }
    }
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
