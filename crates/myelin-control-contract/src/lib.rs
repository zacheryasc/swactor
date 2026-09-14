//! Versioned wire contracts shared by Myelin control-plane producers and clients.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u32 = 1;
pub const MAX_PROVISION_COUNT: u32 = 8;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Versioned<T> {
    pub schema_version: u32,
    pub payload: T,
}

impl<T> Versioned<T> {
    pub const fn new(payload: T) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            payload,
        }
    }

    pub fn into_payload(self) -> Result<T, String> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(format!(
                "unsupported control schema version {} (expected {})",
                self.schema_version, SCHEMA_VERSION
            ));
        }
        Ok(self.payload)
    }
}

/// Per-execution cursor, never a collector-arrival ordering across processes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextualEventCursor {
    pub request_id: String,
    pub after_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextualEventsRequest {
    pub cursors: Vec<ContextualEventCursor>,
    #[serde(default)]
    pub wait_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextualEventsAckRequest {
    pub execution_incarnation: String,
    /// Exclusive cursor after the complete terminal event history.
    pub through_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlChangesRequest {
    pub generation: Option<String>,
    pub after_revision: Option<u64>,
    #[serde(default)]
    pub wait_ms: u64,
}

/// Identity of the exact binary deployment requested from a provider and
/// reported independently by a live worker.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentIdentity {
    pub artifact_digest: String,
    pub deployment_generation: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderReadinessKind {
    Unconfigured,
    Validating,
    Ready,
    ConfigurationError,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderReadiness {
    pub name: String,
    pub provisioning_mode: String,
    pub runtime_image: String,
    pub kind: ProviderReadinessKind,
    pub error: Option<String>,
}

impl ProviderReadiness {
    pub fn ready() -> Self {
        Self::ready_for("test", "test-image")
    }

    pub fn ready_for(name: impl Into<String>, runtime_image: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            provisioning_mode: "real".to_owned(),
            runtime_image: runtime_image.into(),
            kind: ProviderReadinessKind::Ready,
            error: None,
        }
    }

    pub fn unconfigured_for(
        name: impl Into<String>,
        runtime_image: impl Into<String>,
        error: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            provisioning_mode: "real".to_owned(),
            runtime_image: runtime_image.into(),
            kind: ProviderReadinessKind::Unconfigured,
            error: Some(error.into()),
        }
    }

    pub fn with_provisioning_mode(mut self, mode: impl Into<String>) -> Self {
        self.provisioning_mode = mode.into();
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandKind {
    Provision,
    Kill,
    Migrated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandState {
    Persisting,
    Running,
    Succeeded,
    Failed,
}

impl CommandState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandRecord {
    pub command_id: String,
    pub kind: CommandKind,
    pub state: CommandState,
    pub node_ids: Vec<u64>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodePhase {
    Requested,
    Creating,
    Bootstrapping,
    Joining,
    Acknowledging,
    Running,
    KillRequested,
    Stopping,
    StopFailed,
    Stopped,
    Orphan,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRuntimeStatus {
    pub run_id: u64,
    pub attempt_id: u64,
    pub endpoint: String,
    pub node_actor: [u8; 32],
    pub swim_node_id: [u8; 32],
    pub stage_index: u32,
    pub readiness_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment_generation: Option<String>,
}

impl NodeRuntimeStatus {
    pub fn deployment_identity(&self) -> Option<DeploymentIdentity> {
        Some(DeploymentIdentity {
            artifact_digest: self.artifact_digest.clone()?,
            deployment_generation: self.deployment_generation.clone()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStatus {
    pub logical_node_id: u64,
    pub selected_offer_id: Option<u64>,
    pub provider_ref: Option<String>,
    pub phase: NodePhase,
    pub runtime: Option<NodeRuntimeStatus>,
    pub last_error: Option<String>,
    pub last_seen_unix_ms: u64,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderConfigurationRequest {
    pub api_key: Option<String>,
    pub ssh_identity: Option<String>,
    pub bootstrap_command: Option<String>,
}

impl std::fmt::Debug for ProviderConfigurationRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderConfigurationRequest")
            .field("api_key", &self.api_key.as_ref().map(|_| "[redacted]"))
            .field("ssh_identity", &self.ssh_identity)
            .field("bootstrap_command", &self.bootstrap_command)
            .finish()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct OfferSearchRequest {
    pub gpu_model: Option<String>,
    pub min_gpu_ram_mb: Option<u64>,
    pub min_compute_cap: Option<u64>,
    pub min_reliability: Option<f64>,
    pub require_verified: Option<bool>,
    pub min_download_mbps: Option<f64>,
    pub min_upload_mbps: Option<f64>,
    pub max_hourly_price: Option<f64>,
    #[serde(default)]
    pub blacklist_hosts: Vec<u64>,
    pub count: Option<u32>,
}

impl OfferSearchRequest {
    pub fn validate(&self) -> Result<(), String> {
        let finite_non_negative = [
            ("min_reliability", self.min_reliability),
            ("min_download_mbps", self.min_download_mbps),
            ("min_upload_mbps", self.min_upload_mbps),
            ("max_hourly_price", self.max_hourly_price),
        ];
        for (name, value) in finite_non_negative {
            if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
                return Err(format!("{name} must be finite and non-negative"));
            }
        }
        if self.min_reliability.is_some_and(|value| value > 1.0) {
            return Err("min_reliability must not exceed 1".to_owned());
        }
        let count = self.count.unwrap_or(1);
        if count == 0 || count > MAX_PROVISION_COUNT {
            return Err(format!(
                "offer count must be between 1 and {MAX_PROVISION_COUNT}"
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Offer {
    pub offer_id: u64,
    pub host_id: Option<u64>,
    pub gpu_model: String,
    pub gpu_ram_mb: Option<f64>,
    pub compute_cap: u64,
    pub verification: Option<String>,
    pub reliability: Option<f64>,
    pub download_mbps: Option<f64>,
    pub upload_mbps: Option<f64>,
    pub location: Option<String>,
    pub hourly_price: f64,
    pub download_cost_per_tb: f64,
    pub upload_cost_per_tb: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OfferSearchResults {
    pub search_id: u64,
    pub offers: Vec<Offer>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionRequest {
    pub command_id: String,
    #[serde(default = "default_one")]
    pub count: u32,
    #[serde(default)]
    pub selected_offer_ids: Vec<u64>,
    #[serde(default)]
    pub search_id: Option<u64>,
    #[serde(default)]
    pub image: Option<String>,
}

const fn default_one() -> u32 {
    1
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KillRequest {
    pub command_id: String,
    pub logical_node_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManualReadModel {
    pub provider: ProviderReadiness,
    pub commands: Vec<CommandRecord>,
    pub nodes: Vec<NodeStatus>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FleetReadModel {
    pub provider: ProviderReadiness,
    pub nodes: Vec<NodeStatus>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ControlReply {
    Accepted(CommandRecord),
    Provider(ProviderReadiness),
    Status(ManualReadModel),
    FleetStatus(FleetReadModel),
    Offers(OfferSearchResults),
    Flushed,
    Rejected(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextualProgramFile {
    pub namespace_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextualProcessSpec {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub working_dir: Option<String>,
    pub label: Option<String>,
    pub execution_id: String,
    #[serde(default)]
    pub read_prefixes: Vec<String>,
    #[serde(default)]
    pub write_prefixes: Vec<String>,
    pub attach_timeout_ms: u64,
    #[serde(default)]
    pub staged_program: Option<ContextualProgramFile>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextualSpawnRequest {
    pub logical_node_id: u64,
    pub request_id: String,
    #[serde(flatten)]
    pub spec: ContextualProcessSpec,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextualStopRequest {
    pub control_request_id: String,
    pub kill_after_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionIdentity {
    pub execution_id: u64,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ContextualExitStatus {
    Code(i32),
    Signal(i32),
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveContextualExecution {
    pub request_id: String,
    pub process: String,
    pub identity: ExecutionIdentity,
    #[serde(deserialize_with = "required_option")]
    pub started_pid: Option<u32>,
    pub context_ready: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextualProcessEventKind {
    Spawned {
        process: String,
        identity: ExecutionIdentity,
    },
    SpawnRejected {
        error: String,
    },
    ProcessStarted {
        pid: u32,
    },
    ContextReady,
    Stdout {
        bytes: Vec<u8>,
    },
    Stderr {
        bytes: Vec<u8>,
    },
    SpawnFailed {
        error: String,
    },
    BootstrapFailed {
        error: String,
    },
    Exited {
        status: ContextualExitStatus,
    },
    ProcessError {
        error: String,
    },
    StopAccepted {
        process: String,
    },
    StopRejected {
        error: String,
    },
    LiveExecutions {
        executions: Vec<LiveContextualExecution>,
        #[serde(deserialize_with = "required_option")]
        resources: Option<WorkerResourceSnapshot>,
    },
    ControlUnavailable {
        error: String,
    },
}

impl ContextualProcessEventKind {
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::SpawnRejected { .. }
                | Self::SpawnFailed { .. }
                | Self::Exited { .. }
                | Self::ProcessError { .. }
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextualProcessEvent {
    pub request_id: String,
    pub logical_node_id: u64,
    pub event: ContextualProcessEventKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextualEventRecord {
    pub sequence: u64,
    pub observation: ContextualProcessEvent,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextualExecutionView {
    pub request_id: String,
    pub execution_incarnation: String,
    pub logical_node_id: u64,
    #[serde(deserialize_with = "required_option")]
    pub process: Option<String>,
    pub terminal: bool,
    pub truncated_before: u64,
    pub next_sequence: u64,
    pub events: Vec<ContextualEventRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextualEventsBatch {
    pub executions: Vec<ContextualExecutionView>,
    pub missing: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextualEventsAcknowledgement {
    pub request_id: String,
    pub execution_incarnation: String,
    pub through_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlRevision {
    pub generation: String,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextualControlReply {
    Event { observation: ContextualProcessEvent },
    Events { execution: ContextualExecutionView },
    EventsBatch(ContextualEventsBatch),
    Acknowledged(ContextualEventsAcknowledgement),
    ControlRevision(ControlRevision),
    Rejected { error: String },
}

/// Successful exact-node health observation. Unlike execution event history,
/// health never accepts a partial, rejected, or unversioned response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextualHealthReply {
    pub schema_version: u32,
    #[serde(rename = "type")]
    pub reply_type: ContextualHealthReplyType,
    pub observation: ContextualHealthObservation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextualHealthReplyType {
    Event,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextualHealthObservation {
    pub logical_node_id: u64,
    pub request_id: String,
    pub event: ContextualHealthEvent,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextualHealthEvent {
    LiveExecutions {
        executions: Vec<HealthExecution>,
        resources: WorkerResourceSnapshot,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthExecution {
    pub request_id: String,
    pub process: String,
    pub execution_id: u64,
    pub generation: u64,
    #[serde(deserialize_with = "required_option")]
    pub started_pid: Option<u32>,
    pub context_ready: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceActor {
    pub address: String,
    pub actor_type: String,
    pub worker_id: u64,
    pub mailbox_depth: u64,
    pub poisoned: bool,
    pub stopping: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerResourceSnapshot {
    pub schema_version: u32,
    pub request_id: String,
    pub logical_node_id: u64,
    pub generation: u64,
    pub sample_sequence: u64,
    pub iroh_node_id: String,
    #[serde(deserialize_with = "required_option")]
    pub artifact_digest: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub deployment_generation: Option<String>,
    pub actors: Vec<ResourceActor>,
    pub arena: ResourceArena,
    pub collection_elapsed_us: u64,
    pub transport: ResourceTransport,
}

impl WorkerResourceSnapshot {
    pub fn deployment_identity(&self) -> Option<DeploymentIdentity> {
        Some(DeploymentIdentity {
            artifact_digest: self.artifact_digest.clone()?,
            deployment_generation: self.deployment_generation.clone()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceArena {
    pub seq: u64,
    pub sample_unix_ms: u64,
    pub capacity_bytes: u64,
    pub live_bytes: u64,
    pub free_bytes: u64,
    pub active_leases: u64,
    pub pending_leases: u64,
    pub largest_free_range_bytes: u64,
    pub allocation_failures_total: u64,
    pub release_failures_total: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceTransport {
    pub pending_sinks: u64,
    pub pending_inbound: u64,
    pub active_controls: u64,
    pub source_probes: u64,
    pub sink_probes: u64,
    pub local_incarnations: u64,
    pub retired_incarnations: u64,
}

impl ContextualHealthReply {
    pub fn validate(&self, node: u64, request_id: &str) -> Result<(), String> {
        let ContextualHealthEvent::LiveExecutions {
            executions,
            resources,
        } = &self.observation.event;
        if self.schema_version != SCHEMA_VERSION
            || resources.schema_version != SCHEMA_VERSION
            || self.observation.logical_node_id != node
            || resources.logical_node_id != node
            || self.observation.request_id != request_id
            || resources.request_id != request_id
            || request_id.is_empty()
            || resources.iroh_node_id.is_empty()
            || resources.sample_sequence == 0
            || resources.arena.seq != resources.sample_sequence
            || executions
                .iter()
                .any(|execution| execution.request_id.is_empty() || execution.process.is_empty())
        {
            return Err(format!(
                "invalid contextual health identity for node {node}, request {request_id}"
            ));
        }
        let mut addresses = std::collections::BTreeSet::new();
        if resources.actors.iter().any(|actor| {
            actor.address.is_empty()
                || actor.actor_type.is_empty()
                || !addresses.insert(&actor.address)
        }) {
            return Err(format!(
                "invalid or duplicate resource actor for node {node}"
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedBlobsRequest {
    pub schema_version: u32,
    pub request_id: String,
    pub paths: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedBlob {
    pub source: String,
    #[serde(deserialize_with = "required_option")]
    pub binding: Option<String>,
    pub source_node: [u8; 32],
    pub length: u64,
    pub revision: u64,
}

fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetainedNonBlob {
    Absent,
    QuiescentStream { revision: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedBlobsReply {
    pub schema_version: u32,
    pub request_id: String,
    pub blobs: BTreeMap<String, RetainedBlob>,
    pub non_blobs: BTreeMap<String, RetainedNonBlob>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceCleanupRequest {
    pub schema_version: u32,
    pub request_id: String,
    pub paths: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceCleanupQuiescence {
    pub revision: u64,
    pub active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceCleanupPath {
    #[serde(rename = "type")]
    pub record_type: String,
    pub path: String,
    pub attempted: bool,
    pub absent: bool,
    #[serde(deserialize_with = "required_option")]
    pub quiescence: Option<NamespaceCleanupQuiescence>,
    #[serde(deserialize_with = "required_option")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceCleanupReply {
    pub schema_version: u32,
    pub request_id: String,
    pub paths: Vec<NamespaceCleanupPath>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceIdentity {
    pub campaign_id: String,
    pub deployment: DeploymentIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceFailure {
    pub kind: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum EvidenceOutcome {
    Running,
    Passed,
    Failed { error: EvidenceFailure },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence<T> {
    pub kind: String,
    pub identity: EvidenceIdentity,
    pub outcome: EvidenceOutcome,
    pub payload: T,
}

pub type VersionedEvidence<T> = Versioned<Evidence<T>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use serde::de::DeserializeOwned;
    use serde_json::json;

    use super::*;

    fn round_trip_versioned<T>(payload: T)
    where
        T: Debug + PartialEq + Serialize + DeserializeOwned,
    {
        let expected = Versioned::new(payload);
        let encoded = serde_json::to_vec(&expected).unwrap();
        let decoded: Versioned<T> = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, expected);
        assert_eq!(decoded.schema_version, SCHEMA_VERSION);
    }

    fn deployment() -> DeploymentIdentity {
        DeploymentIdentity {
            artifact_digest: format!("sha256:{}", "a".repeat(64)),
            deployment_generation: "deployment-7".to_owned(),
        }
    }

    fn runtime() -> NodeRuntimeStatus {
        let deployment = deployment();
        NodeRuntimeStatus {
            run_id: 11,
            attempt_id: 3,
            endpoint: "127.0.0.1:9000".to_owned(),
            node_actor: [1; 32],
            swim_node_id: [2; 32],
            stage_index: 4,
            readiness_id: 5,
            artifact_digest: Some(deployment.artifact_digest),
            deployment_generation: Some(deployment.deployment_generation),
        }
    }

    fn node() -> NodeStatus {
        NodeStatus {
            logical_node_id: 7,
            selected_offer_id: Some(70),
            provider_ref: Some("contract-70".to_owned()),
            phase: NodePhase::Running,
            runtime: Some(runtime()),
            last_error: None,
            last_seen_unix_ms: 123,
        }
    }

    fn provider() -> ProviderReadiness {
        ProviderReadiness::ready_for("vastai", "runtime/image:latest")
    }

    fn offer() -> Offer {
        Offer {
            offer_id: 70,
            host_id: Some(700),
            gpu_model: "gpu".to_owned(),
            gpu_ram_mb: Some(24_576.0),
            compute_cap: 89,
            verification: Some("verified".to_owned()),
            reliability: Some(0.99),
            download_mbps: Some(500.0),
            upload_mbps: Some(250.0),
            location: Some("test".to_owned()),
            hourly_price: 0.25,
            download_cost_per_tb: 0.0,
            upload_cost_per_tb: 0.0,
        }
    }

    fn command() -> CommandRecord {
        CommandRecord {
            command_id: "command-1".to_owned(),
            kind: CommandKind::Provision,
            state: CommandState::Succeeded,
            node_ids: vec![7],
            error: None,
        }
    }

    fn resources() -> WorkerResourceSnapshot {
        let deployment = deployment();
        WorkerResourceSnapshot {
            schema_version: SCHEMA_VERSION,
            request_id: "health-7".to_owned(),
            logical_node_id: 7,
            generation: 9,
            sample_sequence: 10,
            iroh_node_id: "iroh-7".to_owned(),
            artifact_digest: Some(deployment.artifact_digest),
            deployment_generation: Some(deployment.deployment_generation),
            actors: vec![ResourceActor {
                address: "actor-1".to_owned(),
                actor_type: "Worker".to_owned(),
                worker_id: 1,
                mailbox_depth: 0,
                poisoned: false,
                stopping: false,
            }],
            arena: ResourceArena {
                seq: 10,
                sample_unix_ms: 456,
                capacity_bytes: 4096,
                live_bytes: 1024,
                free_bytes: 3072,
                active_leases: 1,
                pending_leases: 0,
                largest_free_range_bytes: 2048,
                allocation_failures_total: 0,
                release_failures_total: 0,
            },
            collection_elapsed_us: 50,
            transport: ResourceTransport {
                pending_sinks: 0,
                pending_inbound: 0,
                active_controls: 1,
                source_probes: 1,
                sink_probes: 1,
                local_incarnations: 1,
                retired_incarnations: 0,
            },
        }
    }

    fn process_event(event: ContextualProcessEventKind) -> ContextualProcessEvent {
        ContextualProcessEvent {
            request_id: "execution-1".to_owned(),
            logical_node_id: 7,
            event,
        }
    }

    fn execution_view(event: ContextualProcessEventKind) -> ContextualExecutionView {
        ContextualExecutionView {
            request_id: "execution-1".to_owned(),
            execution_incarnation: "incarnation-1".to_owned(),
            logical_node_id: 7,
            process: Some("actor-1".to_owned()),
            terminal: event.is_terminal(),
            truncated_before: 0,
            next_sequence: 2,
            events: vec![ContextualEventRecord {
                sequence: 1,
                observation: process_event(event),
            }],
        }
    }

    #[test]
    fn versioned_contract_requires_a_supported_explicit_schema() {
        round_trip_versioned(KillRequest {
            command_id: "kill-1".to_owned(),
            logical_node_id: 7,
        });

        assert!(
            serde_json::from_value::<Versioned<KillRequest>>(json!({
                "payload": {"command_id": "kill-1", "logical_node_id": 7}
            }))
            .is_err()
        );
        let incompatible: Versioned<KillRequest> = serde_json::from_value(json!({
            "schema_version": SCHEMA_VERSION + 1,
            "payload": {"command_id": "kill-1", "logical_node_id": 7}
        }))
        .unwrap();
        assert!(
            incompatible
                .into_payload()
                .unwrap_err()
                .contains("unsupported")
        );
    }

    #[test]
    fn every_control_contract_round_trips_through_the_shared_version() {
        round_trip_versioned(ControlChangesRequest {
            generation: Some("control-1".to_owned()),
            after_revision: Some(8),
            wait_ms: 100,
        });
        round_trip_versioned(ContextualEventsRequest {
            cursors: vec![ContextualEventCursor {
                request_id: "execution-1".to_owned(),
                after_sequence: 4,
            }],
            wait_ms: 100,
        });
        round_trip_versioned(ContextualEventsAckRequest {
            execution_incarnation: "incarnation-1".to_owned(),
            through_sequence: 5,
        });
        round_trip_versioned(ProviderConfigurationRequest {
            api_key: Some("secret".to_owned()),
            ssh_identity: Some("/tmp/id".to_owned()),
            bootstrap_command: Some("bootstrap".to_owned()),
        });
        round_trip_versioned(OfferSearchRequest {
            gpu_model: Some("gpu".to_owned()),
            min_gpu_ram_mb: Some(16_384),
            min_compute_cap: Some(80),
            min_reliability: Some(0.95),
            require_verified: Some(true),
            min_download_mbps: Some(100.0),
            min_upload_mbps: Some(50.0),
            max_hourly_price: Some(0.50),
            blacklist_hosts: vec![1],
            count: Some(5),
        });
        round_trip_versioned(ProvisionRequest {
            command_id: "provision-1".to_owned(),
            count: 1,
            selected_offer_ids: vec![70],
            search_id: Some(9),
            image: Some("runtime/image:latest".to_owned()),
        });
        round_trip_versioned(ContextualSpawnRequest {
            logical_node_id: 7,
            request_id: "execution-1".to_owned(),
            spec: ContextualProcessSpec {
                command: "python3".to_owned(),
                args: vec!["program.py".to_owned()],
                env: BTreeMap::from([("KEY".to_owned(), "value".to_owned())]),
                working_dir: Some("/work".to_owned()),
                label: Some("process-1".to_owned()),
                execution_id: "execution-1".to_owned(),
                read_prefixes: vec!["/models".to_owned()],
                write_prefixes: vec!["/cases/1".to_owned()],
                attach_timeout_ms: 1_000,
                staged_program: Some(ContextualProgramFile {
                    namespace_path: "/programs/1.py".to_owned(),
                }),
            },
        });
        round_trip_versioned(ContextualStopRequest {
            control_request_id: "stop-1".to_owned(),
            kill_after_ms: Some(500),
        });
        round_trip_versioned(RetainedBlobsRequest {
            schema_version: SCHEMA_VERSION,
            request_id: "retained-1".to_owned(),
            paths: vec!["/cases/1/blob".to_owned()],
        });
        round_trip_versioned(NamespaceCleanupRequest {
            schema_version: SCHEMA_VERSION,
            request_id: "cleanup-1".to_owned(),
            paths: vec!["/cases/1/blob".to_owned()],
        });

        let read_model = ManualReadModel {
            provider: provider(),
            commands: vec![command()],
            nodes: vec![node()],
        };
        let replies = vec![
            ControlReply::Accepted(command()),
            ControlReply::Provider(provider()),
            ControlReply::Status(read_model),
            ControlReply::FleetStatus(FleetReadModel {
                provider: provider(),
                nodes: vec![node()],
            }),
            ControlReply::Offers(OfferSearchResults {
                search_id: 9,
                offers: vec![offer()],
            }),
            ControlReply::Flushed,
            ControlReply::Rejected("rejected".to_owned()),
        ];
        for reply in replies {
            round_trip_versioned(reply);
        }
    }

    #[test]
    fn every_contextual_event_and_reply_round_trips() {
        let identity = ExecutionIdentity {
            execution_id: 12,
            generation: 3,
        };
        let events = vec![
            ContextualProcessEventKind::Spawned {
                process: "actor-1".to_owned(),
                identity,
            },
            ContextualProcessEventKind::SpawnRejected {
                error: "duplicate".to_owned(),
            },
            ContextualProcessEventKind::ProcessStarted { pid: 123 },
            ContextualProcessEventKind::ContextReady,
            ContextualProcessEventKind::Stdout {
                bytes: b"stdout".to_vec(),
            },
            ContextualProcessEventKind::Stderr {
                bytes: b"stderr".to_vec(),
            },
            ContextualProcessEventKind::SpawnFailed {
                error: "spawn".to_owned(),
            },
            ContextualProcessEventKind::BootstrapFailed {
                error: "bootstrap".to_owned(),
            },
            ContextualProcessEventKind::Exited {
                status: ContextualExitStatus::Code(0),
            },
            ContextualProcessEventKind::ProcessError {
                error: "process".to_owned(),
            },
            ContextualProcessEventKind::StopAccepted {
                process: "actor-1".to_owned(),
            },
            ContextualProcessEventKind::StopRejected {
                error: "stopped".to_owned(),
            },
            ContextualProcessEventKind::LiveExecutions {
                executions: vec![LiveContextualExecution {
                    request_id: "execution-1".to_owned(),
                    process: "actor-1".to_owned(),
                    identity,
                    started_pid: Some(123),
                    context_ready: true,
                }],
                resources: Some(resources()),
            },
            ContextualProcessEventKind::ControlUnavailable {
                error: "unavailable".to_owned(),
            },
        ];
        for event in events {
            round_trip_versioned(process_event(event));
        }

        let view = execution_view(ContextualProcessEventKind::Exited {
            status: ContextualExitStatus::Signal(15),
        });
        let replies = vec![
            ContextualControlReply::Event {
                observation: process_event(ContextualProcessEventKind::ContextReady),
            },
            ContextualControlReply::Events {
                execution: view.clone(),
            },
            ContextualControlReply::EventsBatch(ContextualEventsBatch {
                executions: vec![view],
                missing: vec!["missing-1".to_owned()],
            }),
            ContextualControlReply::Acknowledged(ContextualEventsAcknowledgement {
                request_id: "execution-1".to_owned(),
                execution_incarnation: "incarnation-1".to_owned(),
                through_sequence: 1,
            }),
            ContextualControlReply::ControlRevision(ControlRevision {
                generation: "control-1".to_owned(),
                revision: 8,
            }),
            ContextualControlReply::Rejected {
                error: "rejected".to_owned(),
            },
        ];
        for reply in replies {
            round_trip_versioned(reply);
        }
    }

    #[test]
    fn telemetry_and_typed_evidence_round_trip() {
        let resource_snapshot = resources();
        assert_eq!(resource_snapshot.deployment_identity(), Some(deployment()));
        round_trip_versioned(ContextualHealthReply {
            schema_version: SCHEMA_VERSION,
            reply_type: ContextualHealthReplyType::Event,
            observation: ContextualHealthObservation {
                logical_node_id: 7,
                request_id: "health-7".to_owned(),
                event: ContextualHealthEvent::LiveExecutions {
                    executions: vec![HealthExecution {
                        request_id: "execution-1".to_owned(),
                        process: "actor-1".to_owned(),
                        execution_id: 12,
                        generation: 3,
                        started_pid: Some(123),
                        context_ready: true,
                    }],
                    resources: resource_snapshot,
                },
            },
        });
        round_trip_versioned(RetainedBlobsReply {
            schema_version: SCHEMA_VERSION,
            request_id: "retained-1".to_owned(),
            blobs: BTreeMap::from([(
                "/cases/1/blob".to_owned(),
                RetainedBlob {
                    source: "source-1".to_owned(),
                    binding: Some("binding-1".to_owned()),
                    source_node: [3; 32],
                    length: 3,
                    revision: 4,
                },
            )]),
            non_blobs: BTreeMap::from([(
                "/cases/1/stream".to_owned(),
                RetainedNonBlob::QuiescentStream { revision: 5 },
            )]),
        });
        round_trip_versioned(NamespaceCleanupReply {
            schema_version: SCHEMA_VERSION,
            request_id: "cleanup-1".to_owned(),
            paths: vec![NamespaceCleanupPath {
                record_type: "path_cleanup".to_owned(),
                path: "/cases/1/stream".to_owned(),
                attempted: true,
                absent: true,
                quiescence: Some(NamespaceCleanupQuiescence {
                    revision: 5,
                    active: false,
                }),
                error: None,
            }],
        });

        for outcome in [
            EvidenceOutcome::Running,
            EvidenceOutcome::Passed,
            EvidenceOutcome::Failed {
                error: EvidenceFailure {
                    kind: "control_collection".to_owned(),
                    message: "missing execution".to_owned(),
                },
            },
        ] {
            let evidence: VersionedEvidence<ContextualEventsBatch> = Versioned::new(Evidence {
                kind: "contextual-events".to_owned(),
                identity: EvidenceIdentity {
                    campaign_id: "campaign-1".to_owned(),
                    deployment: deployment(),
                },
                outcome,
                payload: ContextualEventsBatch {
                    executions: vec![execution_view(ContextualProcessEventKind::ContextReady)],
                    missing: Vec::new(),
                },
            });
            let encoded = serde_json::to_vec(&evidence).unwrap();
            let decoded: VersionedEvidence<ContextualEventsBatch> =
                serde_json::from_slice(&encoded).unwrap();
            assert_eq!(decoded, evidence);
        }
    }

    #[test]
    fn deployment_fields_preserve_legacy_node_json_and_provider_debug_is_redacted() {
        let legacy: NodeRuntimeStatus = serde_json::from_value(json!({
            "run_id": 11,
            "attempt_id": 3,
            "endpoint": "127.0.0.1:9000",
            "node_actor": vec![1_u8; 32],
            "swim_node_id": vec![2_u8; 32],
            "stage_index": 4,
            "readiness_id": 5
        }))
        .unwrap();
        assert_eq!(legacy.deployment_identity(), None);
        let encoded = serde_json::to_value(&legacy).unwrap();
        assert!(encoded.get("artifact_digest").is_none());
        assert!(encoded.get("deployment_generation").is_none());

        let request = ProviderConfigurationRequest {
            api_key: Some("credential-marker".to_owned()),
            ssh_identity: None,
            bootstrap_command: None,
        };
        let debug = format!("{request:?}");
        assert!(!debug.contains("credential-marker"));
        assert!(debug.contains("[redacted]"));
    }
}
