//! Node-local provisioning and bootstrap state machines for the MVP node
//! provisioning spec.
//!
//! The module is intentionally in-process and deterministic. `NodeManager` is the
//! actor core: it owns one node record and emits commands for provider and
//! bootstrap effects. `BootstrapSession` is the transient pre-swactor SSH core.
//! Tests can drive both without Vast.ai, Docker, or real SSH.

use std::collections::{BTreeMap, VecDeque};
use std::time::SystemTime;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ProviderKind {
    Mock,
    Docker,
    VastAi,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapTimeoutPolicy {
    pub ssh_connect_secs: u64,
    pub boot_check_secs: u64,
    pub swactor_join_secs: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootSpec {
    pub ssh_user: String,
    pub verify_commands: Vec<String>,
    pub start_swactor_command: String,
    pub stdout_sources: Vec<String>,
    pub stderr_sources: Vec<String>,
    pub timeout_policy: BootstrapTimeoutPolicy,
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
                provider: group.provider,
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
    SshTimeout,
    BootCheckFailed,
    StartFailed,
    JoinTimeout,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderError {
    pub reason: String,
}

impl ProviderError {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

pub trait ProviderPlugin {
    fn create_lease(
        &mut self,
        request: CreateLeaseRequest,
    ) -> Result<CreateLeaseResult, ProviderError>;

    fn lookup_endpoint(&mut self, lease: &LeaseFacts)
    -> Result<Option<SshEndpoint>, ProviderError>;

    fn destroy_lease(&mut self, handle: &DestroyHandle) -> Result<(), ProviderError>;
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
pub enum NodeManagerMsg {
    Start(LogicalNodeSpec),
    LeaseCreated(CreateLeaseResult),
    LeaseFailed(String),
    EndpointKnown(SshEndpoint),
    EndpointFailed(String),
    BootstrapObserved(BootstrapObservation),
    BootstrapFailed(String),
    BootstrapClosed,
    SwactorJoined {
        logical_node_id: LogicalNodeId,
        swactor_id: SwactorId,
    },
    Destroy,
    LeaseDestroyed,
    DestroyFailed(String),
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeManagerError {
    pub reason: String,
}

impl NodeManagerError {
    fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NodeManager {
    record: Option<NodeRecord>,
    active_bootstrap: Option<BootstrapSessionId>,
    next_bootstrap_session_id: u64,
    lease_destroyed: bool,
}

impl Default for NodeManager {
    fn default() -> Self {
        Self {
            record: None,
            active_bootstrap: None,
            next_bootstrap_session_id: 1,
            lease_destroyed: false,
        }
    }
}

impl NodeManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self) -> Option<&NodeRecord> {
        self.record.as_ref()
    }

    pub fn is_ready(&self) -> bool {
        self.record.as_ref().is_some_and(|record| record.ready)
    }

    pub fn active_bootstrap(&self) -> Option<BootstrapSessionId> {
        self.active_bootstrap
    }

    pub fn handle(
        &mut self,
        msg: NodeManagerMsg,
    ) -> Result<Vec<NodeManagerCommand>, NodeManagerError> {
        match msg {
            NodeManagerMsg::Start(spec) => self.start(spec),
            NodeManagerMsg::LeaseCreated(result) => self.lease_created(result),
            NodeManagerMsg::LeaseFailed(reason) => self.fail(reason),
            NodeManagerMsg::EndpointKnown(endpoint) => self.endpoint_known(endpoint),
            NodeManagerMsg::EndpointFailed(reason) => self.fail(reason),
            NodeManagerMsg::BootstrapObserved(observation) => self.bootstrap_observed(observation),
            NodeManagerMsg::BootstrapFailed(reason) => self.fail(reason),
            NodeManagerMsg::BootstrapClosed => self.bootstrap_closed(),
            NodeManagerMsg::SwactorJoined {
                logical_node_id,
                swactor_id,
            } => self.swactor_joined(logical_node_id, swactor_id),
            NodeManagerMsg::Destroy => self.destroy(),
            NodeManagerMsg::LeaseDestroyed => self.lease_destroyed(),
            NodeManagerMsg::DestroyFailed(reason) => self.destroy_failed(reason),
        }
    }

    fn start(
        &mut self,
        spec: LogicalNodeSpec,
    ) -> Result<Vec<NodeManagerCommand>, NodeManagerError> {
        if self.record.is_some() {
            return Err(NodeManagerError::new("node manager already started"));
        }
        let mut record = NodeRecord::from_spec(spec.clone());
        record.stage = NodeStage::LeaseRequested;
        self.record = Some(record);
        Ok(vec![NodeManagerCommand::CreateLease(CreateLeaseRequest {
            spec,
        })])
    }

    fn lease_created(
        &mut self,
        result: CreateLeaseResult,
    ) -> Result<Vec<NodeManagerCommand>, NodeManagerError> {
        let record = self.record_mut()?;
        record.lease = Some(result.lease.clone());
        record.stage = NodeStage::LeaseCreated;
        if let Some(endpoint) = result.endpoint {
            self.begin_bootstrap(endpoint)
        } else {
            Ok(vec![NodeManagerCommand::LookupEndpoint(result.lease)])
        }
    }

    fn endpoint_known(
        &mut self,
        endpoint: SshEndpoint,
    ) -> Result<Vec<NodeManagerCommand>, NodeManagerError> {
        if self.require_record()?.lease.is_none() {
            return Err(NodeManagerError::new("endpoint cannot arrive before lease"));
        }
        self.begin_bootstrap(endpoint)
    }

    fn begin_bootstrap(
        &mut self,
        endpoint: SshEndpoint,
    ) -> Result<Vec<NodeManagerCommand>, NodeManagerError> {
        if self.active_bootstrap.is_some() {
            return Err(NodeManagerError::new("bootstrap already active"));
        }
        let session_id = BootstrapSessionId(self.next_bootstrap_session_id);
        self.next_bootstrap_session_id = self.next_bootstrap_session_id.wrapping_add(1).max(1);
        let (run_id, logical_node_id, lease_id, boot, swarm_join, timeout_policy) = {
            let record = self.record_mut()?;
            let lease_id = record
                .lease
                .as_ref()
                .ok_or_else(|| NodeManagerError::new("bootstrap requires known lease"))?
                .lease_id
                .clone();
            record.connection = Some(endpoint.clone());
            record.stage = NodeStage::EndpointKnown;
            record.bootstrap = Some(BootstrapFacts {
                session_id,
                last_stage: BootstrapStage::Created,
                last_stdout_seq: None,
                last_stderr_seq: None,
                last_observed_at: SystemTime::now(),
            });
            record.stage = NodeStage::BootstrapRunning;
            (
                record.run_id.clone(),
                record.logical_node_id.clone(),
                lease_id,
                record.desired.boot.clone(),
                record.desired.swarm_join.clone(),
                record.desired.boot.timeout_policy,
            )
        };
        self.active_bootstrap = Some(session_id);
        Ok(vec![NodeManagerCommand::StartBootstrap(
            BootstrapSessionSpec {
                datastream: DatastreamStreamId(format!(
                    "run/{}/node/{}/bootstrap",
                    run_id.0, logical_node_id.0
                )),
                run_id,
                logical_node_id,
                lease_id,
                ssh: endpoint,
                boot,
                swarm_join,
                timeout_policy,
            },
        )])
    }

    fn bootstrap_observed(
        &mut self,
        observation: BootstrapObservation,
    ) -> Result<Vec<NodeManagerCommand>, NodeManagerError> {
        if self.active_bootstrap.is_none() {
            return Err(NodeManagerError::new(
                "bootstrap observation without active session",
            ));
        }
        let record = self.record_mut()?;
        let facts = record
            .bootstrap
            .as_mut()
            .ok_or_else(|| NodeManagerError::new("missing bootstrap facts"))?;
        facts.last_stage = observation.stage;
        facts.last_observed_at = SystemTime::now();
        if observation.last_stdout_seq.is_some() {
            facts.last_stdout_seq = observation.last_stdout_seq;
        }
        if observation.last_stderr_seq.is_some() {
            facts.last_stderr_seq = observation.last_stderr_seq;
        }
        Ok(Vec::new())
    }

    fn swactor_joined(
        &mut self,
        logical_node_id: LogicalNodeId,
        swactor_id: SwactorId,
    ) -> Result<Vec<NodeManagerCommand>, NodeManagerError> {
        let expected = self.require_record()?.logical_node_id.clone();
        if logical_node_id != expected {
            return Err(NodeManagerError::new(format!(
                "swactor join for {}, expected {}",
                logical_node_id.0, expected.0
            )));
        }
        let session_id = self
            .active_bootstrap
            .ok_or_else(|| NodeManagerError::new("swactor join without active bootstrap"))?;
        let record = self.record_mut()?;
        record.swactor = Some(SwactorFacts {
            swactor_id: swactor_id.clone(),
            joined_at: SystemTime::now(),
            handed_off_at: None,
        });
        record.stage = NodeStage::SwactorJoined;
        Ok(vec![NodeManagerCommand::BootstrapConvergenceObserved {
            session_id,
            swactor_id,
        }])
    }

    fn bootstrap_closed(&mut self) -> Result<Vec<NodeManagerCommand>, NodeManagerError> {
        let record = self.record_mut()?;
        if record.stage != NodeStage::SwactorJoined {
            return Err(NodeManagerError::new(
                "bootstrap closed before swactor convergence",
            ));
        }
        let swactor = record
            .swactor
            .as_mut()
            .ok_or_else(|| NodeManagerError::new("handoff requires swactor facts"))?;
        swactor.handed_off_at = Some(SystemTime::now());
        record.stage = NodeStage::HandedOff;
        record.ready = true;
        record.stage = NodeStage::Dormant;
        self.active_bootstrap = None;
        Ok(Vec::new())
    }

    fn destroy(&mut self) -> Result<Vec<NodeManagerCommand>, NodeManagerError> {
        let mut commands = Vec::new();
        let active_bootstrap = self.active_bootstrap.take();
        let lease_already_destroyed = self.lease_destroyed;
        let lease_command = {
            let record = self.record_mut()?;
            record.ready = false;
            if let Some(session_id) = active_bootstrap {
                commands.push(NodeManagerCommand::CancelBootstrap { session_id });
            }
            if lease_already_destroyed {
                None
            } else {
                record
                    .lease
                    .as_ref()
                    .map(|lease| NodeManagerCommand::DestroyLease(lease.destroy_handle.clone()))
            }
        };
        if let Some(command) = lease_command {
            commands.push(command);
        } else {
            let record = self.record_mut()?;
            record.stage = NodeStage::Destroyed;
            record.destroyed_at = Some(SystemTime::now());
        }
        Ok(commands)
    }

    fn lease_destroyed(&mut self) -> Result<Vec<NodeManagerCommand>, NodeManagerError> {
        let record = self.record_mut()?;
        record.stage = NodeStage::Destroyed;
        record.ready = false;
        record.destroyed_at = Some(SystemTime::now());
        self.lease_destroyed = true;
        Ok(Vec::new())
    }

    fn destroy_failed(
        &mut self,
        reason: String,
    ) -> Result<Vec<NodeManagerCommand>, NodeManagerError> {
        self.fail(format!("destroy: {reason}"))
    }

    fn fail(&mut self, reason: String) -> Result<Vec<NodeManagerCommand>, NodeManagerError> {
        let record = self.record_mut()?;
        record.stage = NodeStage::Failed;
        record.ready = false;
        record.failed_reason = Some(reason);
        Ok(Vec::new())
    }

    fn require_record(&self) -> Result<&NodeRecord, NodeManagerError> {
        self.record
            .as_ref()
            .ok_or_else(|| NodeManagerError::new("node manager not started"))
    }

    fn record_mut(&mut self) -> Result<&mut NodeRecord, NodeManagerError> {
        self.record
            .as_mut()
            .ok_or_else(|| NodeManagerError::new("node manager not started"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BootstrapLogSource {
    SshBootstrap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BootstrapLogStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapLogRecord {
    pub run_id: RunId,
    pub logical_node_id: LogicalNodeId,
    pub lease_id: ProviderLeaseId,
    pub source: BootstrapLogSource,
    pub stream: BootstrapLogStream,
    pub seq: u64,
    pub timestamp: SystemTime,
    pub line: String,
}

pub trait BootstrapDatastreamSink {
    fn record(&mut self, record: BootstrapLogRecord);
    fn flush(&mut self);
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct InMemoryBootstrapDatastream {
    records: Vec<BootstrapLogRecord>,
    flush_count: usize,
}

impl InMemoryBootstrapDatastream {
    pub fn records(&self) -> &[BootstrapLogRecord] {
        &self.records
    }

    pub fn flush_count(&self) -> usize {
        self.flush_count
    }
}

impl BootstrapDatastreamSink for InMemoryBootstrapDatastream {
    fn record(&mut self, record: BootstrapLogRecord) {
        self.records.push(record);
    }

    fn flush(&mut self) {
        self.flush_count += 1;
    }
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
    pub timeout_policy: BootstrapTimeoutPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapSessionEvent {
    Observed(BootstrapObservation),
    Failed(String),
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MockBootstrapScript {
    pub ssh_ok: bool,
    pub verify_ok: bool,
    pub start_ok: bool,
    pub records: Vec<(BootstrapLogStream, String)>,
}

impl MockBootstrapScript {
    pub fn successful(records: Vec<(BootstrapLogStream, String)>) -> Self {
        Self {
            ssh_ok: true,
            verify_ok: true,
            start_ok: true,
            records,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BootstrapSession {
    spec: BootstrapSessionSpec,
    stage: BootstrapStage,
    next_seq: u64,
    last_stdout_seq: Option<u64>,
    last_stderr_seq: Option<u64>,
    closed: bool,
}

impl BootstrapSession {
    pub fn new(spec: BootstrapSessionSpec) -> Self {
        Self {
            spec,
            stage: BootstrapStage::Created,
            next_seq: 1,
            last_stdout_seq: None,
            last_stderr_seq: None,
            closed: false,
        }
    }

    pub fn stage(&self) -> BootstrapStage {
        self.stage
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn start(
        &mut self,
        script: &MockBootstrapScript,
        sink: &mut dyn BootstrapDatastreamSink,
    ) -> Vec<BootstrapSessionEvent> {
        let mut events = Vec::new();
        self.stage = BootstrapStage::SshConnecting;
        if !script.ssh_ok {
            self.stage = BootstrapStage::SshTimeout;
            return vec![BootstrapSessionEvent::Failed("ssh timeout".into())];
        }
        self.stage = BootstrapStage::SshReady;
        events.push(BootstrapSessionEvent::Observed(
            BootstrapObservation::stage(BootstrapStage::SshReady),
        ));

        self.stage = BootstrapStage::StdoutStreaming;
        for (stream, line) in &script.records {
            let seq = self.next_seq;
            self.next_seq += 1;
            match stream {
                BootstrapLogStream::Stdout => self.last_stdout_seq = Some(seq),
                BootstrapLogStream::Stderr => self.last_stderr_seq = Some(seq),
            }
            sink.record(BootstrapLogRecord {
                run_id: self.spec.run_id.clone(),
                logical_node_id: self.spec.logical_node_id.clone(),
                lease_id: self.spec.lease_id.clone(),
                source: BootstrapLogSource::SshBootstrap,
                stream: *stream,
                seq,
                timestamp: SystemTime::now(),
                line: line.clone(),
            });
        }
        events.push(BootstrapSessionEvent::Observed(BootstrapObservation {
            stage: BootstrapStage::StdoutStreaming,
            last_stdout_seq: self.last_stdout_seq,
            last_stderr_seq: self.last_stderr_seq,
            marker: None,
        }));

        self.stage = BootstrapStage::BootChecking;
        events.push(BootstrapSessionEvent::Observed(
            BootstrapObservation::stage(BootstrapStage::BootChecking),
        ));
        if !script.verify_ok {
            self.stage = BootstrapStage::BootCheckFailed;
            events.push(BootstrapSessionEvent::Failed("boot check failed".into()));
            return events;
        }

        self.stage = BootstrapStage::SwactorStarting;
        events.push(BootstrapSessionEvent::Observed(
            BootstrapObservation::stage(BootstrapStage::SwactorStarting),
        ));
        if !script.start_ok {
            self.stage = BootstrapStage::StartFailed;
            events.push(BootstrapSessionEvent::Failed("swactor start failed".into()));
            return events;
        }

        self.stage = BootstrapStage::WaitingForSwactorJoin;
        events.push(BootstrapSessionEvent::Observed(
            BootstrapObservation::stage(BootstrapStage::WaitingForSwactorJoin),
        ));
        events
    }

    pub fn convergence_observed(
        &mut self,
        _swactor_id: SwactorId,
        sink: &mut dyn BootstrapDatastreamSink,
    ) -> Vec<BootstrapSessionEvent> {
        self.stage = BootstrapStage::Converged;
        sink.flush();
        self.closed = true;
        self.stage = BootstrapStage::Closed;
        vec![
            BootstrapSessionEvent::Observed(BootstrapObservation::stage(BootstrapStage::Converged)),
            BootstrapSessionEvent::Closed,
        ]
    }

    pub fn join_timeout(&mut self) -> Vec<BootstrapSessionEvent> {
        self.stage = BootstrapStage::JoinTimeout;
        vec![BootstrapSessionEvent::Failed("join timeout".into())]
    }

    pub fn cancel(&mut self) -> Vec<BootstrapSessionEvent> {
        self.stage = BootstrapStage::Cancelled;
        self.closed = true;
        vec![BootstrapSessionEvent::Closed]
    }
}

#[derive(Default, Debug, Clone)]
pub struct MockProviderPlugin {
    next_contract_id: u64,
    create_results: VecDeque<Result<CreateLeaseResult, ProviderError>>,
    endpoint_results: VecDeque<Result<Option<SshEndpoint>, ProviderError>>,
    create_requests: Vec<CreateLeaseRequest>,
    lookup_requests: Vec<ProviderLeaseId>,
    destroyed_handles: Vec<DestroyHandle>,
    destroy_failures: BTreeMap<ProviderLeaseId, String>,
}

impl MockProviderPlugin {
    pub fn new() -> Self {
        Self {
            next_contract_id: 1,
            ..Self::default()
        }
    }

    pub fn queue_create_result(&mut self, result: Result<CreateLeaseResult, ProviderError>) {
        self.create_results.push_back(result);
    }

    pub fn queue_endpoint_result(&mut self, result: Result<Option<SshEndpoint>, ProviderError>) {
        self.endpoint_results.push_back(result);
    }

    pub fn fail_destroy(&mut self, lease_id: ProviderLeaseId, reason: impl Into<String>) {
        self.destroy_failures.insert(lease_id, reason.into());
    }

    pub fn create_requests(&self) -> &[CreateLeaseRequest] {
        &self.create_requests
    }

    pub fn lookup_requests(&self) -> &[ProviderLeaseId] {
        &self.lookup_requests
    }

    pub fn destroyed_handles(&self) -> &[DestroyHandle] {
        &self.destroyed_handles
    }

    pub fn result_with_endpoint(id: u64, endpoint: Option<SshEndpoint>) -> CreateLeaseResult {
        let lease_id = ProviderLeaseId(format!("mock:{id}"));
        CreateLeaseResult {
            lease: LeaseFacts {
                provider: ProviderKind::Mock,
                lease_id: lease_id.clone(),
                provider_contract_id: id.to_string(),
                offer_id: Some(format!("offer-{id}")),
                destroy_handle: DestroyHandle {
                    provider: ProviderKind::Mock,
                    lease_id,
                    provider_contract_id: id.to_string(),
                },
                provider_metadata: BTreeMap::new(),
            },
            endpoint,
        }
    }

    fn default_endpoint(id: u64) -> SshEndpoint {
        SshEndpoint {
            host: "127.0.0.1".into(),
            port: 22000 + id as u16,
            user: "root".into(),
            auth_ref: format!("mock-auth-{id}"),
        }
    }
}

impl ProviderPlugin for MockProviderPlugin {
    fn create_lease(
        &mut self,
        request: CreateLeaseRequest,
    ) -> Result<CreateLeaseResult, ProviderError> {
        self.create_requests.push(request);
        if let Some(result) = self.create_results.pop_front() {
            return result;
        }
        let id = self.next_contract_id;
        self.next_contract_id = self.next_contract_id.wrapping_add(1).max(1);
        Ok(Self::result_with_endpoint(
            id,
            Some(Self::default_endpoint(id)),
        ))
    }

    fn lookup_endpoint(
        &mut self,
        lease: &LeaseFacts,
    ) -> Result<Option<SshEndpoint>, ProviderError> {
        self.lookup_requests.push(lease.lease_id.clone());
        if let Some(result) = self.endpoint_results.pop_front() {
            return result;
        }
        Ok(Some(Self::default_endpoint(
            lease.provider_contract_id.parse().unwrap_or(1),
        )))
    }

    fn destroy_lease(&mut self, handle: &DestroyHandle) -> Result<(), ProviderError> {
        if let Some(reason) = self.destroy_failures.get(&handle.lease_id) {
            return Err(ProviderError::new(reason.clone()));
        }
        self.destroyed_handles.push(handle.clone());
        Ok(())
    }
}
