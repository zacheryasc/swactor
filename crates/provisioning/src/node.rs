//! Provider-neutral node lease, bootstrap, and destroy contracts.
//!
//! `reconciler` owns lifecycle observation and effect selection. This module
//! retains the fact, command, provider, and bootstrap-session vocabulary.

use std::collections::BTreeMap;
use std::time::SystemTime;

use crate::plugin::ProviderMount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RunId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LogicalNodeId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeGroupId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RoleId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProviderLeaseId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SwactorId(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BootstrapSessionId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DatastreamStreamId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProviderKind(pub String);

impl ProviderKind {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DesiredNodeShape {
    pub image: String,
    pub disk_gb: u32,
    pub gpu_name: Option<String>,
    pub min_gpu_ram_mb: Option<u64>,
    pub min_down_mbps: Option<f64>,
    pub min_up_mbps: Option<f64>,
    pub min_reliability: Option<f64>,
    pub require_verified: bool,
    pub provider_labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootSpec {
    pub ssh_user: String,
    pub verify_commands: Vec<String>,
    pub start_swactor_command: String,
    pub stdout_sources: Vec<String>,
    pub stderr_sources: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<ProviderMount>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmJoinTemplate {
    pub orch_swactor_addr: String,
    pub join_token_ref: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmJoinSpec {
    pub orch_swactor_addr: String,
    pub join_token_ref: String,
    pub expected_logical_node_id: LogicalNodeId,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunNodeGroupSpec {
    pub run_id: RunId,
    pub group_id: NodeGroupId,
    pub role: RoleId,
    pub count: u32,
    pub provider: ProviderKind,
    pub shape: DesiredNodeShape,
    pub boot: BootSpec,
    pub swarm_join: SwarmJoinTemplate,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogicalNodeSpec {
    pub run_id: RunId,
    pub logical_node_id: LogicalNodeId,
    pub group_id: NodeGroupId,
    pub role: RoleId,
    pub provider: ProviderKind,
    pub shape: DesiredNodeShape,
    pub boot: BootSpec,
    pub swarm_join: SwarmJoinSpec,
}

pub fn expand_node_group(group: &RunNodeGroupSpec) -> Vec<LogicalNodeSpec> {
    (0..group.count)
        .map(|index| {
            let logical_node_id = LogicalNodeId(format!("{}-{index}", group.group_id.0));
            LogicalNodeSpec {
                run_id: group.run_id.clone(),
                logical_node_id: logical_node_id.clone(),
                group_id: group.group_id.clone(),
                role: group.role.clone(),
                provider: group.provider.clone(),
                shape: group.shape.clone(),
                boot: group.boot.clone(),
                swarm_join: SwarmJoinSpec {
                    orch_swactor_addr: group.swarm_join.orch_swactor_addr.clone(),
                    join_token_ref: group.swarm_join.join_token_ref.clone(),
                    expected_logical_node_id: logical_node_id,
                },
            }
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStage {
    New,
    LeaseRequested,
    LeaseCreated,
    EndpointKnown,
    BootstrapRunning,
    SwactorJoined,
    HandedOff,
    Dormant,
    Failed,
    Destroyed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BootstrapStage {
    Created,
    SshConnecting,
    SshReady,
    StdoutStreaming,
    BootChecking,
    SwactorStarting,
    WaitingForSwactorJoin,
    Converged,
    Closed,
    SshConnectFailed,
    BootCheckFailed,
    StartFailed,
    SwactorJoinFailed,
    StreamError,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestroyHandle {
    pub provider: ProviderKind,
    pub lease_id: ProviderLeaseId,
    pub provider_contract_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseFacts {
    pub provider: ProviderKind,
    pub lease_id: ProviderLeaseId,
    pub provider_contract_id: String,
    pub offer_id: Option<String>,
    pub destroy_handle: DestroyHandle,
    pub provider_metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SshEndpoint {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub auth_ref: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapFacts {
    pub session_id: BootstrapSessionId,
    pub last_stage: BootstrapStage,
    pub last_stdout_seq: Option<u64>,
    pub last_stderr_seq: Option<u64>,
    pub last_observed_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwactorFacts {
    pub swactor_id: SwactorId,
    pub joined_at: SystemTime,
    pub handed_off_at: Option<SystemTime>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub logical_node_id: LogicalNodeId,
    pub run_id: RunId,
    pub group_id: NodeGroupId,
    pub role: RoleId,
    pub desired: LogicalNodeSpec,
    pub stage: NodeStage,
    pub ready: bool,
    pub lease: Option<LeaseFacts>,
    pub connection: Option<SshEndpoint>,
    pub bootstrap: Option<BootstrapFacts>,
    pub swactor: Option<SwactorFacts>,
    pub failed_reason: Option<String>,
    pub failed_at: Option<SystemTime>,
    pub destroyed_at: Option<SystemTime>,
}

impl NodeRecord {
    pub fn from_spec(spec: LogicalNodeSpec) -> Self {
        Self {
            logical_node_id: spec.logical_node_id.clone(),
            run_id: spec.run_id.clone(),
            group_id: spec.group_id.clone(),
            role: spec.role.clone(),
            desired: spec,
            stage: NodeStage::New,
            ready: false,
            lease: None,
            connection: None,
            bootstrap: None,
            swactor: None,
            failed_reason: None,
            failed_at: None,
            destroyed_at: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateLeaseRequest {
    pub spec: LogicalNodeSpec,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateLeaseResult {
    pub lease: LeaseFacts,
    pub endpoint: Option<SshEndpoint>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapObservation {
    pub stage: BootstrapStage,
    pub last_stdout_seq: Option<u64>,
    pub last_stderr_seq: Option<u64>,
    pub marker: Option<String>,
}

impl BootstrapObservation {
    pub fn stage(stage: BootstrapStage) -> Self {
        Self {
            stage,
            last_stdout_seq: None,
            last_stderr_seq: None,
            marker: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum NodeManagerCommand {
    CreateLease(CreateLeaseRequest),
    LookupEndpoint(LeaseFacts),
    StartBootstrap(BootstrapSessionSpec),
    BootstrapConvergenceObserved {
        session_id: BootstrapSessionId,
        swactor_id: SwactorId,
    },
    CancelBootstrap {
        session_id: BootstrapSessionId,
    },
    DestroyLease(DestroyHandle),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BootstrapSessionSpec {
    pub run_id: RunId,
    pub logical_node_id: LogicalNodeId,
    pub lease_id: ProviderLeaseId,
    pub ssh: SshEndpoint,
    pub boot: BootSpec,
    pub swarm_join: SwarmJoinSpec,
    pub datastream: DatastreamStreamId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_kind_is_an_opaque_provider_identifier() {
        let provider = ProviderKind::new("example-provider");

        assert_eq!(provider.as_str(), "example-provider");
    }
}
