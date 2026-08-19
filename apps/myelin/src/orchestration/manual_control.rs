//! Manual node-control protocol and pure transition core.
//!
//! The core owns durable command/node state and emits explicit effects. It never
//! performs provider, filesystem, network, or process work itself.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;
use swactor::runtime::{Ctx, ExternalSender, Runtime};
use swactor_engine::EngineHandle;
use swactor_transport::{CodecRegistry, JsonCodec, NetworkMessage};
use swactor_vastai::OfferBrowseCriteria;

use crate::node_actor::NodeAgentMsg;
use crate::orchestration::actor::OrchestratorMsg;
use crate::orchestration::daemon::{
    ClusterSnapshot, RuntimeFacts, SnapshotNode, StateDir, unix_ms_now,
};
use crate::provisioning::{NodeProvisionSpec, PluginNodeHandle, PluginSink, ProvisionPlugin};

pub(crate) const MAX_PROVISION_COUNT: u32 = 8;
pub(crate) const CONTROL_REGISTRY_NAME: &str = "myelin.manual-control";
pub(crate) const SELECTED_OFFER_ID_ENV: &str = "MYELIN_SELECTED_OFFER_ID";
pub(crate) const READ_MODEL_COMMAND_LIMIT: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProviderReadinessKind {
    Unconfigured,
    Validating,
    Ready,
    ConfigurationError,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProviderReadiness {
    pub name: String,
    pub provisioning_mode: String,
    pub kind: ProviderReadinessKind,
    pub error: Option<String>,
}

impl ProviderReadiness {
    #[cfg(test)]
    pub(crate) fn ready() -> Self {
        Self::ready_for("test")
    }

    pub(crate) fn ready_for(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            provisioning_mode: "real".to_owned(),
            kind: ProviderReadinessKind::Ready,
            error: None,
        }
    }

    pub(crate) fn unconfigured_for(name: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            provisioning_mode: "real".to_owned(),
            kind: ProviderReadinessKind::Unconfigured,
            error: Some(error.into()),
        }
    }

    pub(crate) fn with_provisioning_mode(mut self, mode: impl Into<String>) -> Self {
        self.provisioning_mode = mode.into();
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CommandKind {
    Provision,
    Kill,
    /// Schema-v1 accepted-command IDs had no durable kind or result.
    Migrated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CommandState {
    Persisting,
    Running,
    Succeeded,
    Failed,
}

impl CommandState {
    pub(crate) fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CommandRecord {
    pub command_id: String,
    pub kind: CommandKind,
    pub state: CommandState,
    pub node_ids: Vec<u64>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NodePhase {
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

/// Runtime correction input. `api_key` is intentionally absent from Debug and
/// never enters durable state or a response DTO.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ProviderConfigurationRequest {
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
pub(crate) struct OfferSearchRequest {
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
    pub(crate) fn browse_criteria(&self) -> OfferBrowseCriteria {
        OfferBrowseCriteria {
            gpu_name_contains: self.gpu_model.clone(),
            min_gpu_ram_mb: self.min_gpu_ram_mb,
            min_compute_cap: self.min_compute_cap,
            min_reliability: self.min_reliability,
            require_verified: self.require_verified.unwrap_or(false),
            min_down_mbps: self.min_download_mbps,
            min_up_mbps: self.min_upload_mbps,
            max_dph_total: self.max_hourly_price,
            blacklist_hosts: self.blacklist_hosts.clone(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
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
pub(crate) struct OfferDto {
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProvisionRequest {
    pub command_id: String,
    #[serde(default = "default_one")]
    pub count: u32,
    #[serde(default)]
    pub selected_offer_ids: Vec<u64>,
}

const fn default_one() -> u32 {
    1
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct KillRequest {
    pub command_id: String,
    pub logical_node_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RejoinHello {
    pub run_id: u64,
    pub logical_node_id: u64,
    pub attempt_id: u64,
    pub selected_offer_id: Option<u64>,
    pub endpoint: String,
    pub swim_node_id: distribution::types::NodeId,
    pub stage_index: u32,
    pub node_actor: ActorAddress,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RejoinBinding {
    pub orchestrator_actor: ActorAddress,
    pub control_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ManualReadModel {
    pub provider: ProviderReadiness,
    pub commands: Vec<CommandRecord>,
    pub nodes: Vec<SnapshotNode>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum EffectKind {
    Create,
    StartBootstrap,
    CompleteBootstrap,
    Stop,
    Recover,
}

#[derive(Clone, Debug)]
pub(crate) enum ManualAction {
    Persist {
        generation: u64,
        snapshot: ClusterSnapshot,
    },
    Create {
        node_id: u64,
        spec: NodeProvisionSpec,
        selected_offer_id: Option<u64>,
    },
    Recover {
        node_id: u64,
        spec: NodeProvisionSpec,
    },
    StartBootstrap {
        node_id: u64,
    },
    CompleteBootstrap {
        node_id: u64,
    },
    Stop {
        node_id: u64,
        cancel_bootstrap: bool,
    },
    SendRuntimeReadyAck {
        node_id: u64,
        facts: RuntimeFacts,
    },
    SendRejoinReply {
        reply_to: ActorAddress,
        binding: RejoinBinding,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum EffectOutcome {
    Created { provider_ref: String },
    BootstrapStarted,
    BootstrapCompleted,
    Stopped,
    Recovered { provider_ref: Option<String> },
}

#[derive(Clone, Debug)]
enum AfterPersist {
    None,
    CreateMany(Vec<u64>),
    StartBootstrap(u64),
    CompleteBootstrap(u64),
    Stop(u64),
    SendRuntimeReadyAck(u64),
    SendRejoinReply {
        reply_to: ActorAddress,
        binding: RejoinBinding,
    },
}

#[derive(Clone, Debug)]
struct PendingPersist {
    generation: u64,
    snapshot: ClusterSnapshot,
    after: AfterPersist,
}

/// Pure authority for command deduplication, node transitions, persistence
/// barriers, and one-provider-effect-per-node admission.
pub(crate) struct ManualControl {
    snapshot: ClusterSnapshot,
    provider: ProviderReadiness,
    actions: VecDeque<ManualAction>,
    pending_persists: VecDeque<PendingPersist>,
    persistence_in_flight: Option<u64>,
    next_persist_generation: u64,
    in_flight: BTreeMap<u64, EffectKind>,
}

impl ManualControl {
    pub(crate) fn new(snapshot: ClusterSnapshot, provider: ProviderReadiness) -> Self {
        Self {
            snapshot,
            provider,
            actions: VecDeque::new(),
            pending_persists: VecDeque::new(),
            persistence_in_flight: None,
            next_persist_generation: 1,
            in_flight: BTreeMap::new(),
        }
    }

    pub(crate) fn snapshot(&self) -> &ClusterSnapshot {
        &self.snapshot
    }
    fn latest_persist_generation(&self) -> Option<u64> {
        self.pending_persists
            .back()
            .map(|pending| pending.generation)
    }
    fn persistence_idle(&self) -> bool {
        self.pending_persists.is_empty()
    }

    pub(crate) fn provider(&self) -> &ProviderReadiness {
        &self.provider
    }

    pub(crate) fn set_provider_validating(&mut self) {
        self.provider.kind = ProviderReadinessKind::Validating;
        self.provider.error = None;
    }

    pub(crate) fn set_provider_validation(&mut self, result: Result<(), String>) {
        match result {
            Ok(()) => {
                self.provider.kind = ProviderReadinessKind::Ready;
                self.provider.error = None;
            }
            Err(error) => {
                self.provider.kind = ProviderReadinessKind::ConfigurationError;
                self.provider.error = Some(error);
            }
        }
    }

    pub(crate) fn begin_recovery(&mut self) {
        if self.provider.kind != ProviderReadinessKind::Ready {
            return;
        }
        let recoverable = self
            .snapshot
            .nodes
            .iter()
            .filter(|node| {
                !matches!(node.phase, NodePhase::Stopped | NodePhase::Orphan)
                    && node.spec.is_some()
                    && !self.in_flight.contains_key(&node.logical_node_id)
            })
            .filter_map(|node| node.spec.clone().map(|spec| (node.logical_node_id, spec)))
            .collect::<Vec<_>>();
        for (node_id, spec) in recoverable {
            if self.dispatch(node_id, EffectKind::Recover).is_ok() {
                self.actions
                    .push_back(ManualAction::Recover { node_id, spec });
            }
        }
    }

    pub(crate) fn read_model(&self) -> ManualReadModel {
        let mut commands = self.snapshot.commands.values().cloned().collect::<Vec<_>>();
        if commands.len() > READ_MODEL_COMMAND_LIMIT {
            commands.drain(..commands.len() - READ_MODEL_COMMAND_LIMIT);
        }
        ManualReadModel {
            provider: self.provider.clone(),
            commands,
            nodes: self.snapshot.nodes.clone(),
        }
    }

    pub(crate) fn request_provision<F>(
        &mut self,
        request: ProvisionRequest,
        mut build_spec: F,
    ) -> Result<&CommandRecord, String>
    where
        F: FnMut(u64) -> Result<NodeProvisionSpec, String>,
    {
        let command_id = validate_command_id(&request.command_id)?;
        if self.snapshot.commands.contains_key(command_id) {
            return Ok(self
                .snapshot
                .commands
                .get(command_id)
                .expect("checked command exists"));
        }
        if request.count == 0 || request.count > MAX_PROVISION_COUNT {
            return Err(format!(
                "provision count must be between 1 and {MAX_PROVISION_COUNT}"
            ));
        }
        if !request.selected_offer_ids.is_empty()
            && request.selected_offer_ids.len() != request.count as usize
        {
            return Err("selected offer count must equal provision count".to_owned());
        }
        if self.provider.kind != ProviderReadinessKind::Ready {
            self.snapshot.commands.insert(
                command_id.to_owned(),
                CommandRecord {
                    command_id: command_id.to_owned(),
                    kind: CommandKind::Provision,
                    state: CommandState::Failed,
                    node_ids: Vec::new(),
                    error: Some(
                        self.provider
                            .error
                            .clone()
                            .unwrap_or_else(|| "provider is not ready".to_owned()),
                    ),
                },
            );
            self.queue_persist(AfterPersist::None);
            return Ok(self
                .snapshot
                .commands
                .get(command_id)
                .expect("inserted command exists"));
        }
        let start_id = self.snapshot.next_node_id;
        let mut intents = Vec::with_capacity(request.count as usize);
        for offset in 0..request.count {
            let node_id = start_id
                .checked_add(u64::from(offset))
                .ok_or_else(|| "logical node id space exhausted".to_owned())?;
            let selected_offer_id = request.selected_offer_ids.get(offset as usize).copied();
            let mut spec = build_spec(node_id)?;
            if let Some(offer_id) = selected_offer_id {
                spec.env
                    .push((SELECTED_OFFER_ID_ENV.to_owned(), offer_id.to_string()));
            }
            intents.push((node_id, spec, selected_offer_id));
        }

        let node_ids = intents
            .iter()
            .map(|(node_id, _, _)| *node_id)
            .collect::<Vec<_>>();
        self.snapshot.commands.insert(
            command_id.to_owned(),
            CommandRecord {
                command_id: command_id.to_owned(),
                kind: CommandKind::Provision,
                state: CommandState::Persisting,
                node_ids: node_ids.clone(),
                error: None,
            },
        );

        for (node_id, spec, selected_offer_id) in intents {
            let allocated = self.snapshot.allocate_node_id();
            debug_assert_eq!(allocated, node_id);
            self.snapshot.upsert_node(SnapshotNode {
                logical_node_id: node_id,
                spec: Some(spec),
                selected_offer_id,
                provider_ref: None,
                phase: NodePhase::Requested,
                runtime: None,
                last_error: None,
                last_seen_unix_ms: unix_ms_now(),
            });
        }
        self.queue_persist(AfterPersist::CreateMany(node_ids));
        Ok(self
            .snapshot
            .commands
            .get(command_id)
            .expect("inserted command exists"))
    }

    pub(crate) fn request_kill(&mut self, request: KillRequest) -> Result<&CommandRecord, String> {
        let command_id = validate_command_id(&request.command_id)?;
        if self.snapshot.commands.contains_key(command_id) {
            return Ok(self
                .snapshot
                .commands
                .get(command_id)
                .expect("checked command exists"));
        }
        let node = self
            .snapshot
            .node_mut(request.logical_node_id)
            .ok_or_else(|| format!("managed node {} does not exist", request.logical_node_id))?;
        if node.phase == NodePhase::Orphan {
            return Err(format!(
                "node {} is unmanaged and cannot be killed",
                request.logical_node_id
            ));
        }
        let already_stopped = node.phase == NodePhase::Stopped;
        if !already_stopped {
            if node.phase != NodePhase::Running && node.last_error.is_none() {
                node.last_error = Some("provision cancelled by Kill".to_owned());
            }
            node.phase = NodePhase::KillRequested;
            node.last_seen_unix_ms = unix_ms_now();
        }
        self.snapshot.commands.insert(
            command_id.to_owned(),
            CommandRecord {
                command_id: command_id.to_owned(),
                kind: CommandKind::Kill,
                state: if already_stopped {
                    CommandState::Succeeded
                } else {
                    CommandState::Persisting
                },
                node_ids: vec![request.logical_node_id],
                error: None,
            },
        );
        let wait_for_in_flight = self.in_flight.contains_key(&request.logical_node_id);
        self.queue_persist(if already_stopped || wait_for_in_flight {
            AfterPersist::None
        } else {
            AfterPersist::Stop(request.logical_node_id)
        });
        Ok(self
            .snapshot
            .commands
            .get(command_id)
            .expect("inserted command exists"))
    }

    pub(crate) fn persisted(
        &mut self,
        generation: u64,
        result: Result<(), String>,
    ) -> Result<(), String> {
        if self.persistence_in_flight != Some(generation) {
            return Err(format!("stale persistence completion {generation}"));
        }
        let pending = self
            .pending_persists
            .pop_front()
            .expect("in-flight persistence has queue entry");
        self.persistence_in_flight = None;
        if let Err(error) = result {
            self.fail_all_nonterminal(format!("persist control state: {error}"));
            self.pending_persists.clear();
            return Ok(());
        }
        self.after_persist(pending.after)?;
        self.start_next_persist();
        Ok(())
    }

    pub(crate) fn effect_finished(
        &mut self,
        node_id: u64,
        kind: EffectKind,
        result: Result<EffectOutcome, String>,
    ) -> Result<(), String> {
        if self.in_flight.remove(&node_id) != Some(kind) {
            return Err(format!(
                "node {node_id} completed {kind:?} without matching in-flight effect"
            ));
        }
        match kind {
            EffectKind::Create => self.created(node_id, result),
            EffectKind::StartBootstrap => self.bootstrap_started(node_id, result),
            EffectKind::CompleteBootstrap => self.bootstrap_completed(node_id, result),
            EffectKind::Stop => self.stopped(node_id, result),
            EffectKind::Recover => self.recovered(node_id, result),
        }
    }

    pub(crate) fn runtime_ready(
        &mut self,
        node_id: u64,
        facts: RuntimeFacts,
    ) -> Result<(), String> {
        let node = self
            .snapshot
            .node_mut(node_id)
            .ok_or_else(|| format!("runtime-ready for unknown node {node_id}"))?;
        if node.phase != NodePhase::Joining {
            return Err(format!(
                "runtime-ready is invalid for node {node_id} in {:?}",
                node.phase
            ));
        }
        let spec = node
            .spec
            .as_ref()
            .ok_or_else(|| format!("node {node_id} has no provision intent"))?;
        if facts.run_id != spec.run_id || facts.attempt_id != spec.attempt_id {
            return Err(format!(
                "runtime-ready identity mismatch for node {node_id}: expected run/attempt {}/{}, got {}/{}",
                spec.run_id, spec.attempt_id, facts.run_id, facts.attempt_id
            ));
        }
        node.runtime = Some(facts);
        node.last_seen_unix_ms = unix_ms_now();
        self.queue_persist(AfterPersist::None);
        Ok(())
    }

    pub(crate) fn join_barrier_satisfied(&mut self, node_id: u64) -> Result<(), String> {
        let node = self
            .snapshot
            .node_mut(node_id)
            .ok_or_else(|| format!("join barrier for unknown node {node_id}"))?;
        if node.phase != NodePhase::Joining || node.runtime.is_none() {
            return Err(format!(
                "join barrier is invalid for node {node_id} in {:?}",
                node.phase
            ));
        }
        node.phase = NodePhase::Acknowledging;
        node.last_seen_unix_ms = unix_ms_now();
        self.queue_persist(AfterPersist::SendRuntimeReadyAck(node_id));
        Ok(())
    }

    pub(crate) fn node_ack(&mut self, node_id: u64, readiness_id: u64) -> Result<(), String> {
        let node = self
            .snapshot
            .node(node_id)
            .ok_or_else(|| format!("node ACK for unknown node {node_id}"))?;
        if node.phase != NodePhase::Acknowledging
            || node.runtime.as_ref().map(|facts| facts.readiness_id) != Some(readiness_id)
        {
            return Err(format!(
                "node ACK does not match node {node_id} acknowledging attempt"
            ));
        }
        self.queue_persist(AfterPersist::CompleteBootstrap(node_id));
        Ok(())
    }

    pub(crate) fn terminal_failure(&mut self, node_id: u64, error: String) -> Result<(), String> {
        let node = self
            .snapshot
            .node_mut(node_id)
            .ok_or_else(|| format!("terminal failure for unknown node {node_id}"))?;
        if matches!(node.phase, NodePhase::Stopped | NodePhase::Orphan) {
            return Ok(());
        }
        node.phase = NodePhase::KillRequested;
        node.last_error = Some(error);
        node.last_seen_unix_ms = unix_ms_now();
        self.queue_persist(if self.in_flight.contains_key(&node_id) {
            AfterPersist::None
        } else {
            AfterPersist::Stop(node_id)
        });
        Ok(())
    }

    pub(crate) fn rejoin(
        &mut self,
        hello: &RejoinHello,
        orchestrator_actor: ActorAddress,
        control_generation: u64,
        reply_to: ActorAddress,
    ) -> Result<RejoinBinding, String> {
        let node = self
            .snapshot
            .node_mut(hello.logical_node_id)
            .ok_or_else(|| format!("rejoin for unknown node {}", hello.logical_node_id))?;
        let spec = node
            .spec
            .as_ref()
            .ok_or_else(|| format!("rejoin node {} has no intent", hello.logical_node_id))?;
        if spec.run_id != hello.run_id
            || spec.attempt_id != hello.attempt_id
            || node.selected_offer_id != hello.selected_offer_id
        {
            return Err(format!(
                "rejoin identity mismatch for node {}",
                hello.logical_node_id
            ));
        }
        if matches!(
            node.phase,
            NodePhase::KillRequested
                | NodePhase::Stopping
                | NodePhase::StopFailed
                | NodePhase::Stopped
        ) {
            return Err(format!(
                "rejoin rejected for node {} in phase {:?}",
                hello.logical_node_id, node.phase
            ));
        }
        node.runtime = Some(RuntimeFacts {
            run_id: hello.run_id,
            attempt_id: hello.attempt_id,
            endpoint: hello.endpoint.clone(),
            node_actor: hello.node_actor,
            swim_node_id: hello.swim_node_id,
            stage_index: hello.stage_index,
            readiness_id: hello.attempt_id,
        });
        node.phase = NodePhase::Running;
        node.last_error = None;
        node.last_seen_unix_ms = unix_ms_now();
        let binding = RejoinBinding {
            orchestrator_actor,
            control_generation,
        };
        self.queue_persist(AfterPersist::SendRejoinReply {
            reply_to,
            binding: binding.clone(),
        });
        Ok(binding)
    }

    pub(crate) fn take_actions(&mut self) -> impl Iterator<Item = ManualAction> + '_ {
        self.actions.drain(..)
    }

    fn queue_persist(&mut self, after: AfterPersist) {
        let generation = self.next_persist_generation;
        self.next_persist_generation = self.next_persist_generation.wrapping_add(1).max(1);
        self.pending_persists.push_back(PendingPersist {
            generation,
            snapshot: self.snapshot.clone(),
            after,
        });
        self.start_next_persist();
    }

    fn start_next_persist(&mut self) {
        if self.persistence_in_flight.is_some() {
            return;
        }
        let Some(pending) = self.pending_persists.front() else {
            return;
        };
        self.persistence_in_flight = Some(pending.generation);
        self.actions.push_back(ManualAction::Persist {
            generation: pending.generation,
            snapshot: pending.snapshot.clone(),
        });
    }

    fn after_persist(&mut self, after: AfterPersist) -> Result<(), String> {
        match after {
            AfterPersist::None => Ok(()),
            AfterPersist::CreateMany(node_ids) => {
                for node_id in node_ids {
                    let phase = self.snapshot.node(node_id).map(|node| node.phase);
                    if phase == Some(NodePhase::Requested) {
                        self.dispatch_create(node_id)?;
                    } else if phase == Some(NodePhase::KillRequested) {
                        if let Some(node) = self.snapshot.node_mut(node_id) {
                            node.phase = NodePhase::Stopped;
                            node.last_error = Some("provision cancelled by Kill".to_owned());
                        }
                        self.succeed_commands(node_id, CommandKind::Kill);
                        self.fail_commands(
                            node_id,
                            CommandKind::Provision,
                            "provision cancelled by Kill".to_owned(),
                        );
                        self.queue_persist(AfterPersist::None);
                    }
                }
                Ok(())
            }
            AfterPersist::StartBootstrap(node_id) => {
                if self
                    .snapshot
                    .node(node_id)
                    .is_none_or(|node| node.phase != NodePhase::Bootstrapping)
                {
                    return Ok(());
                }
                self.dispatch(node_id, EffectKind::StartBootstrap)?;
                self.actions
                    .push_back(ManualAction::StartBootstrap { node_id });
                Ok(())
            }
            AfterPersist::CompleteBootstrap(node_id) => {
                if self
                    .snapshot
                    .node(node_id)
                    .is_none_or(|node| node.phase != NodePhase::Acknowledging)
                {
                    return Ok(());
                }
                self.dispatch(node_id, EffectKind::CompleteBootstrap)?;
                self.actions
                    .push_back(ManualAction::CompleteBootstrap { node_id });
                Ok(())
            }
            AfterPersist::Stop(node_id) => self.dispatch_stop(node_id),
            AfterPersist::SendRuntimeReadyAck(node_id) => {
                let Some(node) = self.snapshot.node(node_id) else {
                    return Ok(());
                };
                if node.phase != NodePhase::Acknowledging {
                    return Ok(());
                }
                let facts = node
                    .runtime
                    .clone()
                    .ok_or_else(|| format!("node {node_id} has no runtime-ready facts"))?;
                self.actions
                    .push_back(ManualAction::SendRuntimeReadyAck { node_id, facts });
                Ok(())
            }
            AfterPersist::SendRejoinReply { reply_to, binding } => {
                self.actions
                    .push_back(ManualAction::SendRejoinReply { reply_to, binding });
                Ok(())
            }
        }
    }

    fn dispatch_create(&mut self, node_id: u64) -> Result<(), String> {
        let node = self
            .snapshot
            .node_mut(node_id)
            .ok_or_else(|| format!("create for unknown node {node_id}"))?;
        if node.phase != NodePhase::Requested {
            return Err(format!(
                "create is invalid for node {node_id} in {:?}",
                node.phase
            ));
        }
        node.phase = NodePhase::Creating;
        node.last_seen_unix_ms = unix_ms_now();
        let spec = node
            .spec
            .clone()
            .ok_or_else(|| format!("node {node_id} has no provision intent"))?;
        let selected_offer_id = node.selected_offer_id;
        self.dispatch(node_id, EffectKind::Create)?;
        self.set_commands_running(node_id);
        self.actions.push_back(ManualAction::Create {
            node_id,
            spec,
            selected_offer_id,
        });
        Ok(())
    }

    fn dispatch_stop(&mut self, node_id: u64) -> Result<(), String> {
        if self.in_flight.contains_key(&node_id) {
            return Ok(());
        }
        let node = self
            .snapshot
            .node_mut(node_id)
            .ok_or_else(|| format!("stop for unknown node {node_id}"))?;
        if node.phase == NodePhase::Stopped {
            return Ok(());
        }
        if !matches!(node.phase, NodePhase::KillRequested | NodePhase::StopFailed) {
            return Err(format!(
                "stop is invalid for node {node_id} in {:?}",
                node.phase
            ));
        }
        let cancel_bootstrap = node.provider_ref.is_some();
        node.phase = NodePhase::Stopping;
        node.last_seen_unix_ms = unix_ms_now();
        self.dispatch(node_id, EffectKind::Stop)?;
        self.set_commands_running(node_id);
        self.actions.push_back(ManualAction::Stop {
            node_id,
            cancel_bootstrap,
        });
        Ok(())
    }

    fn dispatch(&mut self, node_id: u64, kind: EffectKind) -> Result<(), String> {
        if let Some(existing) = self.in_flight.insert(node_id, kind) {
            self.in_flight.insert(node_id, existing);
            return Err(format!(
                "node {node_id} already has {existing:?} in flight; cannot dispatch {kind:?}"
            ));
        }
        Ok(())
    }

    fn created(
        &mut self,
        node_id: u64,
        result: Result<EffectOutcome, String>,
    ) -> Result<(), String> {
        let kill_requested = self
            .snapshot
            .node(node_id)
            .is_some_and(|node| node.phase == NodePhase::KillRequested);
        match result {
            Ok(EffectOutcome::Created { provider_ref }) => {
                let node = self.snapshot.node_mut(node_id).expect("effect node exists");
                node.provider_ref = Some(provider_ref);
                node.phase = if kill_requested {
                    NodePhase::KillRequested
                } else {
                    NodePhase::Bootstrapping
                };
                node.last_seen_unix_ms = unix_ms_now();
                self.queue_persist(if kill_requested {
                    AfterPersist::Stop(node_id)
                } else {
                    AfterPersist::StartBootstrap(node_id)
                });
                Ok(())
            }
            Ok(other) => Err(format!("create returned unexpected outcome {other:?}")),
            Err(error) => {
                self.fail_provision(node_id, error);
                self.succeed_commands(node_id, CommandKind::Kill);
                self.queue_persist(AfterPersist::None);
                Ok(())
            }
        }
    }

    fn bootstrap_started(
        &mut self,
        node_id: u64,
        result: Result<EffectOutcome, String>,
    ) -> Result<(), String> {
        match result {
            Ok(EffectOutcome::BootstrapStarted) => {
                let node = self.snapshot.node_mut(node_id).expect("effect node exists");
                if node.phase == NodePhase::KillRequested {
                    self.queue_persist(AfterPersist::Stop(node_id));
                } else {
                    node.phase = NodePhase::Joining;
                    node.last_seen_unix_ms = unix_ms_now();
                    self.queue_persist(AfterPersist::None);
                }
                Ok(())
            }
            Ok(other) => Err(format!(
                "bootstrap start returned unexpected outcome {other:?}"
            )),
            Err(error) => {
                self.mark_provision_failure_for_cleanup(node_id, error);
                self.queue_persist(AfterPersist::Stop(node_id));
                Ok(())
            }
        }
    }

    fn bootstrap_completed(
        &mut self,
        node_id: u64,
        result: Result<EffectOutcome, String>,
    ) -> Result<(), String> {
        match result {
            Ok(EffectOutcome::BootstrapCompleted) => {
                let node = self.snapshot.node_mut(node_id).expect("effect node exists");
                if node.phase == NodePhase::KillRequested {
                    self.queue_persist(AfterPersist::Stop(node_id));
                } else {
                    node.phase = NodePhase::Running;
                    node.last_error = None;
                    node.last_seen_unix_ms = unix_ms_now();
                    self.succeed_commands(node_id, CommandKind::Provision);
                    self.queue_persist(AfterPersist::None);
                }
                Ok(())
            }
            Ok(other) => Err(format!(
                "bootstrap completion returned unexpected outcome {other:?}"
            )),
            Err(error) => {
                self.mark_provision_failure_for_cleanup(node_id, error);
                self.queue_persist(AfterPersist::Stop(node_id));
                Ok(())
            }
        }
    }

    fn recovered(
        &mut self,
        node_id: u64,
        result: Result<EffectOutcome, String>,
    ) -> Result<(), String> {
        match result {
            Ok(EffectOutcome::Recovered { provider_ref }) => {
                let Some(provider_ref) = provider_ref else {
                    let phase = self
                        .snapshot
                        .node(node_id)
                        .map(|node| node.phase)
                        .expect("effect node exists");
                    match phase {
                        NodePhase::Requested | NodePhase::Creating => {
                            let node = self.snapshot.node_mut(node_id).expect("effect node exists");
                            node.phase = NodePhase::Requested;
                            node.provider_ref = None;
                            node.last_error = None;
                            node.last_seen_unix_ms = unix_ms_now();
                            self.queue_persist(AfterPersist::CreateMany(vec![node_id]));
                        }
                        NodePhase::KillRequested | NodePhase::Stopping | NodePhase::StopFailed => {
                            let provision_error = self
                                .snapshot
                                .node(node_id)
                                .and_then(|node| node.last_error.clone());
                            let node = self.snapshot.node_mut(node_id).expect("effect node exists");
                            node.phase = NodePhase::Stopped;
                            node.provider_ref = None;
                            node.last_seen_unix_ms = unix_ms_now();
                            self.succeed_commands(node_id, CommandKind::Kill);
                            if let Some(error) = provision_error {
                                self.fail_commands(node_id, CommandKind::Provision, error);
                            }
                            self.queue_persist(AfterPersist::None);
                        }
                        _ => {
                            self.fail_provision(
                                node_id,
                                "persisted node is absent from the provider".to_owned(),
                            );
                            self.succeed_commands(node_id, CommandKind::Kill);
                            self.queue_persist(AfterPersist::None);
                        }
                    }
                    return Ok(());
                };
                let node = self.snapshot.node_mut(node_id).expect("effect node exists");
                node.provider_ref = Some(provider_ref);
                node.last_seen_unix_ms = unix_ms_now();
                let after = match node.phase {
                    NodePhase::Requested | NodePhase::Creating | NodePhase::Bootstrapping => {
                        node.phase = NodePhase::Bootstrapping;
                        AfterPersist::StartBootstrap(node_id)
                    }
                    NodePhase::KillRequested | NodePhase::Stopping | NodePhase::StopFailed => {
                        node.phase = NodePhase::KillRequested;
                        AfterPersist::Stop(node_id)
                    }
                    NodePhase::Joining
                    | NodePhase::Acknowledging
                    | NodePhase::Running
                    | NodePhase::Stopped
                    | NodePhase::Orphan => AfterPersist::None,
                };
                self.queue_persist(after);
                Ok(())
            }
            Ok(other) => Err(format!("recovery returned unexpected outcome {other:?}")),
            Err(error) => {
                if let Some(node) = self.snapshot.node_mut(node_id) {
                    node.last_error = Some(format!("provider adoption failed: {error}"));
                }
                self.queue_persist(AfterPersist::None);
                Ok(())
            }
        }
    }

    fn stopped(
        &mut self,
        node_id: u64,
        result: Result<EffectOutcome, String>,
    ) -> Result<(), String> {
        match result {
            Ok(EffectOutcome::Stopped) => {
                let provision_error = self
                    .snapshot
                    .node(node_id)
                    .and_then(|node| node.last_error.clone());
                let node = self.snapshot.node_mut(node_id).expect("effect node exists");
                node.phase = NodePhase::Stopped;
                node.last_seen_unix_ms = unix_ms_now();
                self.succeed_commands(node_id, CommandKind::Kill);
                if let Some(error) = provision_error {
                    self.fail_commands(node_id, CommandKind::Provision, error);
                }
                self.queue_persist(AfterPersist::None);
                Ok(())
            }
            Ok(other) => Err(format!("stop returned unexpected outcome {other:?}")),
            Err(error) => {
                let node = self.snapshot.node_mut(node_id).expect("effect node exists");
                node.phase = NodePhase::StopFailed;
                node.last_error = Some(error.clone());
                node.last_seen_unix_ms = unix_ms_now();
                self.fail_commands(node_id, CommandKind::Kill, error.clone());
                self.fail_commands(
                    node_id,
                    CommandKind::Provision,
                    format!("resource cleanup failed: {error}"),
                );
                self.queue_persist(AfterPersist::None);
                Ok(())
            }
        }
    }

    fn fail_provision(&mut self, node_id: u64, error: String) {
        if let Some(node) = self.snapshot.node_mut(node_id) {
            node.phase = NodePhase::Stopped;
            node.last_error = Some(error.clone());
            node.last_seen_unix_ms = unix_ms_now();
        }
        self.fail_commands(node_id, CommandKind::Provision, error);
    }

    fn mark_provision_failure_for_cleanup(&mut self, node_id: u64, error: String) {
        if let Some(node) = self.snapshot.node_mut(node_id) {
            node.phase = NodePhase::KillRequested;
            node.last_error = Some(error);
            node.last_seen_unix_ms = unix_ms_now();
        }
    }

    fn set_commands_running(&mut self, node_id: u64) {
        for command in self.snapshot.commands.values_mut().filter(|command| {
            command.node_ids.contains(&node_id) && command.state == CommandState::Persisting
        }) {
            command.state = CommandState::Running;
        }
    }

    fn succeed_commands(&mut self, node_id: u64, kind: CommandKind) {
        let candidates = self
            .snapshot
            .commands
            .iter()
            .filter(|(_, command)| {
                command.kind == kind
                    && command.node_ids.contains(&node_id)
                    && !command.state.is_terminal()
            })
            .map(|(command_id, command)| (command_id.clone(), command.node_ids.clone()))
            .collect::<Vec<_>>();
        for (command_id, node_ids) in candidates {
            let all_terminal = node_ids.iter().all(|candidate| {
                self.snapshot
                    .node(*candidate)
                    .is_some_and(|node| match kind {
                        CommandKind::Provision => node.phase == NodePhase::Running,
                        CommandKind::Kill => node.phase == NodePhase::Stopped,
                        CommandKind::Migrated => false,
                    })
            });
            if all_terminal {
                let command = self
                    .snapshot
                    .commands
                    .get_mut(&command_id)
                    .expect("candidate command still exists");
                command.state = CommandState::Succeeded;
                command.error = None;
            }
        }
    }

    fn fail_commands(&mut self, node_id: u64, kind: CommandKind, error: String) {
        for command in self.snapshot.commands.values_mut().filter(|command| {
            command.kind == kind
                && command.node_ids.contains(&node_id)
                && !command.state.is_terminal()
        }) {
            command.state = CommandState::Failed;
            command.error = Some(error.clone());
        }
    }

    fn fail_all_nonterminal(&mut self, error: String) {
        for command in self
            .snapshot
            .commands
            .values_mut()
            .filter(|command| !command.state.is_terminal())
        {
            command.state = CommandState::Failed;
            command.error = Some(error.clone());
        }
        for node in &mut self.snapshot.nodes {
            if node.phase != NodePhase::Orphan && node.phase != NodePhase::Stopped {
                node.last_error = Some(error.clone());
            }
        }
        self.in_flight.clear();
    }
}

fn validate_command_id(command_id: &str) -> Result<&str, String> {
    let trimmed = command_id.trim();
    if trimmed.is_empty() {
        return Err("command_id must not be empty".to_owned());
    }
    if trimmed.len() > 128 {
        return Err("command_id must not exceed 128 bytes".to_owned());
    }
    if trimmed != command_id {
        return Err("command_id must not contain leading or trailing whitespace".to_owned());
    }
    Ok(trimmed)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum ManualControlMsg {
    Provision {
        request: ProvisionRequest,
        reply_to: Option<ActorAddress>,
    },
    Kill {
        request: KillRequest,
        reply_to: Option<ActorAddress>,
    },
    Configure {
        request: ProviderConfigurationRequest,
        reply_to: Option<ActorAddress>,
    },
    ProviderValidated {
        error: Option<String>,
        reply_to: Option<ActorAddress>,
    },
    SearchOffers {
        request: OfferSearchRequest,
        reply_to: ActorAddress,
    },
    OfferSearchFinished {
        reply_to: ActorAddress,
        result: Result<Vec<OfferDto>, String>,
    },
    Query {
        reply_to: ActorAddress,
    },
    Flush {
        reply_to: ActorAddress,
    },
    PersistenceFinished {
        generation: u64,
        error: Option<String>,
    },
    EffectFinished {
        node_id: u64,
        kind: EffectKind,
        outcome: Option<EffectOutcome>,
        error: Option<String>,
    },
    JoinBarrierSatisfied {
        node_id: u64,
    },
    Rejoin {
        hello: RejoinHello,
        reply_to: ActorAddress,
    },
    ProviderTerminalFailure {
        node_id: u64,
        error: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum ManualControlReply {
    Accepted(CommandRecord),
    Provider(ProviderReadiness),
    Status(ManualReadModel),
    Offers(Vec<OfferDto>),
    Rejoined(RejoinBinding),
    Flushed,
    Rejected(String),
}

pub(crate) type ProviderFactory =
    Arc<dyn Fn() -> Result<Box<dyn ProvisionPlugin>, String> + Send + Sync>;
pub(crate) type SpecBuilder =
    Arc<dyn Fn(u64, ActorAddress) -> Result<NodeProvisionSpec, String> + Send + Sync>;
pub(crate) type ConfigValidator =
    Arc<dyn Fn(ProviderConfigurationRequest) -> Result<(), String> + Send + Sync>;
pub(crate) type OfferSearcher =
    Arc<dyn Fn(OfferSearchRequest) -> Result<Vec<OfferDto>, String> + Send + Sync>;
fn refresh_recovery_routing(
    mut persisted: NodeProvisionSpec,
    current: NodeProvisionSpec,
) -> NodeProvisionSpec {
    const ROUTING_KEYS: &[&str] = &[
        "MYELIN_COORDINATOR_ENDPOINT",
        "MYELIN_ORCHESTRATOR_ACTOR",
        "MYELIN_IROH_RELAY_MODE",
        "MYELIN_IROH_RELAY_URL",
        "MYELIN_IROH_ENDPOINT_ADDR_MASK",
    ];
    persisted
        .env
        .retain(|(key, _)| !ROUTING_KEYS.contains(&key.as_str()));
    persisted.env.extend(
        current
            .env
            .into_iter()
            .filter(|(key, _)| ROUTING_KEYS.contains(&key.as_str())),
    );
    persisted
}

impl NetworkMessage for ManualControlReply {
    fn type_tag() -> &'static str {
        "myelin::ManualControlReply"
    }
}

pub(crate) fn register_codecs(registry: &mut CodecRegistry) {
    registry.register::<ManualControlReply, _>(JsonCodec::<ManualControlReply>::default());
}

struct NodeLane {
    plugin: Box<dyn ProvisionPlugin>,
    handle: PluginNodeHandle,
}

type SharedLane = Arc<Mutex<Option<NodeLane>>>;

/// Engine-backed adapter owned by the orchestrator actor. All methods called
/// from actor handlers are transition-only; filesystem and provider work is
/// scheduled on the engine and reports back as `ManualControlMsg`.
pub(crate) struct ManualActorControl {
    core: ManualControl,
    engine: EngineHandle,
    runtime: Runtime,
    sender: ExternalSender,
    state_dir: StateDir,
    sink: PluginSink,
    provider_factory: ProviderFactory,
    spec_builder: SpecBuilder,
    config_validator: Option<ConfigValidator>,
    offer_searcher: Option<OfferSearcher>,
    lanes: BTreeMap<u64, SharedLane>,
    persistence_queue: VecDeque<(u64, ClusterSnapshot)>,
    persistence_in_flight: bool,
    pending_command_replies: BTreeMap<u64, (ActorAddress, String)>,
    flush_waiters: Vec<ActorAddress>,
    control_generation: u64,
}

impl ManualActorControl {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        core: ManualControl,
        engine: EngineHandle,
        runtime: Runtime,
        state_dir: StateDir,
        sink: PluginSink,
        provider_factory: ProviderFactory,
        spec_builder: SpecBuilder,
        config_validator: Option<ConfigValidator>,
        offer_searcher: Option<OfferSearcher>,
        control_generation: u64,
    ) -> Self {
        let sender = runtime.create_sender();
        Self {
            core,
            engine,
            runtime,
            sender,
            state_dir,
            sink,
            provider_factory,
            spec_builder,
            config_validator,
            offer_searcher,
            lanes: BTreeMap::new(),
            persistence_queue: VecDeque::new(),
            persistence_in_flight: false,
            pending_command_replies: BTreeMap::new(),
            flush_waiters: Vec::new(),
            control_generation,
        }
    }

    pub(crate) fn read_model(&self) -> ManualReadModel {
        self.core.read_model()
    }

    pub(crate) fn start(&mut self, actor: ActorAddress) {
        self.core.begin_recovery();
        self.dispatch_actions(actor);
    }

    pub(crate) fn handle(&mut self, ctx: &Ctx, msg: ManualControlMsg) {
        match msg {
            ManualControlMsg::Provision { request, reply_to } => {
                let command_id = request.command_id.clone();
                let already_durable = self.core.snapshot().commands.contains_key(&command_id);
                let builder = Arc::clone(&self.spec_builder);
                let result = self
                    .core
                    .request_provision(request, |node_id| builder(node_id, ctx.self_addr()))
                    .cloned();
                if already_durable || result.is_err() {
                    send_command_reply(ctx, reply_to, result);
                } else if let Some(reply_to) = reply_to {
                    let generation = self
                        .core
                        .latest_persist_generation()
                        .expect("new provision command queues persistence");
                    self.pending_command_replies
                        .insert(generation, (reply_to, command_id));
                }
            }
            ManualControlMsg::Kill { request, reply_to } => {
                let command_id = request.command_id.clone();
                let already_durable = self.core.snapshot().commands.contains_key(&command_id);
                let result = self.core.request_kill(request).cloned();
                if already_durable || result.is_err() {
                    send_command_reply(ctx, reply_to, result);
                } else if let Some(reply_to) = reply_to {
                    let generation = self
                        .core
                        .latest_persist_generation()
                        .expect("new kill command queues persistence");
                    self.pending_command_replies
                        .insert(generation, (reply_to, command_id));
                }
            }
            ManualControlMsg::Configure { request, reply_to } => {
                self.core.set_provider_validating();
                let Some(validator) = self.config_validator.clone() else {
                    self.core.set_provider_validation(Err(
                        "runtime provider configuration is unsupported".to_owned(),
                    ));
                    if let Some(reply_to) = reply_to {
                        let _ = ctx.send(
                            reply_to,
                            ManualControlReply::Provider(self.core.provider().clone()),
                        );
                    }
                    return;
                };
                let sender = self.sender.clone();
                let actor = ctx.self_addr();
                self.engine.spawn_blocking(move || {
                    let error = validator(request).err();
                    let _ = sender.send_to(
                        actor,
                        OrchestratorMsg::Manual(ManualControlMsg::ProviderValidated {
                            error,
                            reply_to,
                        }),
                    );
                });
            }
            ManualControlMsg::ProviderValidated { error, reply_to } => {
                self.core.set_provider_validation(error.map_or(Ok(()), Err));
                if self.core.provider().kind == ProviderReadinessKind::Ready {
                    self.core.begin_recovery();
                }
                if let Some(reply_to) = reply_to {
                    let _ = ctx.send(
                        reply_to,
                        ManualControlReply::Provider(self.core.provider().clone()),
                    );
                }
            }
            ManualControlMsg::SearchOffers { request, reply_to } => {
                if let Err(error) = request.validate() {
                    let _ = ctx.send(reply_to, ManualControlReply::Rejected(error));
                } else if self.core.provider().kind != ProviderReadinessKind::Ready {
                    let _ = ctx.send(
                        reply_to,
                        ManualControlReply::Rejected(
                            self.core
                                .provider()
                                .error
                                .clone()
                                .unwrap_or_else(|| "provider is not ready".to_owned()),
                        ),
                    );
                } else if let Some(searcher) = self.offer_searcher.clone() {
                    let sender = self.sender.clone();
                    let actor = ctx.self_addr();
                    self.engine.spawn_blocking(move || {
                        let result = searcher(request);
                        let _ = sender.send_to(
                            actor,
                            OrchestratorMsg::Manual(ManualControlMsg::OfferSearchFinished {
                                reply_to,
                                result,
                            }),
                        );
                    });
                } else {
                    let _ = ctx.send(
                        reply_to,
                        ManualControlReply::Rejected(
                            "offer search is available only for Vast.ai".to_owned(),
                        ),
                    );
                }
            }
            ManualControlMsg::OfferSearchFinished { reply_to, result } => {
                let reply = match result {
                    Ok(offers) => ManualControlReply::Offers(offers),
                    Err(error) => ManualControlReply::Rejected(error),
                };
                let _ = ctx.send(reply_to, reply);
            }
            ManualControlMsg::Query { reply_to } => {
                let _ = ctx.send(reply_to, ManualControlReply::Status(self.core.read_model()));
            }
            ManualControlMsg::PersistenceFinished { generation, error } => {
                self.persistence_in_flight = false;
                let persisted = self
                    .core
                    .persisted(generation, error.clone().map_or(Ok(()), Err));
                if let Some((reply_to, command_id)) =
                    self.pending_command_replies.remove(&generation)
                {
                    let reply = match persisted {
                        Ok(()) => self
                            .core
                            .snapshot()
                            .commands
                            .get(&command_id)
                            .cloned()
                            .map(ManualControlReply::Accepted)
                            .unwrap_or_else(|| {
                                ManualControlReply::Rejected(format!(
                                    "persisted command {command_id} is absent"
                                ))
                            }),
                        Err(error) => ManualControlReply::Rejected(error),
                    };
                    let _ = ctx.send(reply_to, reply);
                }
            }
            ManualControlMsg::EffectFinished {
                node_id,
                kind,
                outcome,
                error,
            } => {
                let result = match (outcome, error) {
                    (Some(outcome), None) => Ok(outcome),
                    (_, Some(error)) => Err(error),
                    (None, None) => Err("provider effect returned no outcome".to_owned()),
                };
                let _ = self.core.effect_finished(node_id, kind, result);
            }
            ManualControlMsg::JoinBarrierSatisfied { node_id } => {
                let _ = self.core.join_barrier_satisfied(node_id);
            }
            ManualControlMsg::Rejoin { hello, reply_to } => {
                if let Err(error) =
                    self.core
                        .rejoin(&hello, ctx.self_addr(), self.control_generation, reply_to)
                {
                    let _ = ctx.send(reply_to, ManualControlReply::Rejected(error));
                }
            }
            ManualControlMsg::Flush { reply_to } => {
                self.flush_waiters.push(reply_to);
            }
            ManualControlMsg::ProviderTerminalFailure { node_id, error } => {
                let _ = self.core.terminal_failure(node_id, error);
            }
        }
        self.dispatch_actions(ctx.self_addr());
        self.finish_flush_waiters(ctx);
    }

    pub(crate) fn observe_runtime_ready(
        &mut self,
        actor: ActorAddress,
        node_id: u64,
        facts: RuntimeFacts,
    ) {
        let _ = self.core.runtime_ready(node_id, facts);
        self.dispatch_actions(actor);
    }

    pub(crate) fn observe_node_ack(
        &mut self,
        actor: ActorAddress,
        node_id: u64,
        readiness_id: u64,
    ) {
        let _ = self.core.node_ack(node_id, readiness_id);
        self.dispatch_actions(actor);
    }

    fn dispatch_actions(&mut self, actor: ActorAddress) {
        let actions = self.core.take_actions().collect::<Vec<_>>();
        for action in actions {
            match action {
                ManualAction::Persist {
                    generation,
                    snapshot,
                } => {
                    self.persistence_queue.push_back((generation, snapshot));
                }
                ManualAction::Create {
                    node_id,
                    spec,
                    selected_offer_id,
                } => {
                    let lane = Arc::new(Mutex::new(None));
                    self.lanes.insert(node_id, Arc::clone(&lane));
                    let factory = Arc::clone(&self.provider_factory);
                    let sink = self.sink.clone();
                    self.spawn_effect(actor, node_id, EffectKind::Create, move || {
                        let mut plugin = factory()?;
                        let provider_ref = plugin.provider_ref_for(&spec);
                        let handle =
                            plugin.create_node_selected(spec.clone(), sink, selected_offer_id)?;
                        *lane.lock() = Some(NodeLane { plugin, handle });
                        Ok(EffectOutcome::Created { provider_ref })
                    });
                }
                ManualAction::Recover { node_id, spec } => {
                    let lane = Arc::new(Mutex::new(None));
                    self.lanes.insert(node_id, Arc::clone(&lane));
                    let factory = Arc::clone(&self.provider_factory);
                    let sink = self.sink.clone();
                    let spec_builder = Arc::clone(&self.spec_builder);
                    let phase = self.core.snapshot().node(node_id).map(|node| node.phase);
                    self.spawn_effect(actor, node_id, EffectKind::Recover, move || {
                        let current_spec = spec_builder(node_id, actor)?;
                        let spec = refresh_recovery_routing(spec, current_spec);
                        let mut plugin = factory()?;
                        let mut adopted = plugin.adopt_by_spec(&spec, sink.clone())?;
                        if adopted.is_none() && phase == Some(NodePhase::Bootstrapping) {
                            adopted = plugin.prepare_missing_bootstrap(&spec, sink)?;
                        }
                        let provider_ref = adopted.as_ref().map(|node| node.provider_ref.clone());
                        if let Some(adopted) = adopted {
                            *lane.lock() = Some(NodeLane {
                                plugin,
                                handle: adopted.handle,
                            });
                        }
                        Ok(EffectOutcome::Recovered { provider_ref })
                    });
                }
                ManualAction::StartBootstrap { node_id } => {
                    let lane = self.lanes.get(&node_id).cloned();
                    self.spawn_effect(actor, node_id, EffectKind::StartBootstrap, move || {
                        let lane =
                            lane.ok_or_else(|| format!("node {node_id} has no provider lane"))?;
                        let mut guard = lane.lock();
                        let lane = guard.as_mut().ok_or_else(|| {
                            format!("node {node_id} provider lane is uninitialized")
                        })?;
                        lane.plugin.start_bootstrap(&lane.handle)?;
                        Ok(EffectOutcome::BootstrapStarted)
                    });
                }
                ManualAction::CompleteBootstrap { node_id } => {
                    let lane = self.lanes.get(&node_id).cloned();
                    self.spawn_effect(actor, node_id, EffectKind::CompleteBootstrap, move || {
                        let lane =
                            lane.ok_or_else(|| format!("node {node_id} has no provider lane"))?;
                        let mut guard = lane.lock();
                        let lane = guard.as_mut().ok_or_else(|| {
                            format!("node {node_id} provider lane is uninitialized")
                        })?;
                        lane.plugin.complete_bootstrap(&lane.handle)?;
                        Ok(EffectOutcome::BootstrapCompleted)
                    });
                }
                ManualAction::Stop {
                    node_id,
                    cancel_bootstrap,
                } => {
                    let lane = self.lanes.get(&node_id).cloned();
                    let factory = Arc::clone(&self.provider_factory);
                    let sink = self.sink.clone();
                    let spec = self
                        .core
                        .snapshot()
                        .node(node_id)
                        .and_then(|node| node.spec.clone());
                    self.spawn_effect(actor, node_id, EffectKind::Stop, move || {
                        if let Some(lane) = lane {
                            let mut guard = lane.lock();
                            let lane = guard.as_mut().ok_or_else(|| {
                                format!("node {node_id} provider lane is uninitialized")
                            })?;
                            if cancel_bootstrap {
                                lane.plugin.cancel_bootstrap(&lane.handle)?;
                            }
                            lane.plugin.stop_node(&lane.handle)?;
                        } else {
                            let spec = spec
                                .ok_or_else(|| format!("node {node_id} has no durable intent"))?;
                            let mut plugin = factory()?;
                            let _ = plugin.stop_by_spec(&spec, sink)?;
                        }
                        Ok(EffectOutcome::Stopped)
                    });
                }
                ManualAction::SendRuntimeReadyAck { node_id, facts } => {
                    let _ = self.runtime.send_to(
                        facts.node_actor,
                        NodeAgentMsg::RuntimeReadyAck {
                            run_id: facts.run_id,
                            node_id,
                            stage_index: facts.stage_index,
                            readiness_id: facts.readiness_id,
                        },
                    );
                }
                ManualAction::SendRejoinReply { reply_to, binding } => {
                    let _ = self
                        .runtime
                        .send_to(reply_to, ManualControlReply::Rejoined(binding));
                }
            }
        }
        self.start_next_persistence(actor);
    }

    fn start_next_persistence(&mut self, actor: ActorAddress) {
        if self.persistence_in_flight {
            return;
        }
        let Some((generation, snapshot)) = self.persistence_queue.pop_front() else {
            return;
        };
        self.persistence_in_flight = true;
        let state_dir = self.state_dir.clone();
        let sender = self.sender.clone();
        self.engine.spawn_blocking(move || {
            let error = state_dir.save_snapshot(&snapshot).err();
            let _ = sender.send_to(
                actor,
                OrchestratorMsg::Manual(ManualControlMsg::PersistenceFinished {
                    generation,
                    error,
                }),
            );
        });
    }
    fn finish_flush_waiters(&mut self, ctx: &Ctx) {
        if self.persistence_in_flight
            || !self.persistence_queue.is_empty()
            || !self.core.persistence_idle()
        {
            return;
        }
        for reply_to in self.flush_waiters.drain(..) {
            let _ = ctx.send(reply_to, ManualControlReply::Flushed);
        }
    }

    fn spawn_effect<F>(&self, actor: ActorAddress, node_id: u64, kind: EffectKind, work: F)
    where
        F: FnOnce() -> Result<EffectOutcome, String> + Send + 'static,
    {
        let sender = self.sender.clone();
        self.engine.spawn_blocking(move || {
            let (outcome, error) = match work() {
                Ok(outcome) => (Some(outcome), None),
                Err(error) => (None, Some(error)),
            };
            let _ = sender.send_to(
                actor,
                OrchestratorMsg::Manual(ManualControlMsg::EffectFinished {
                    node_id,
                    kind,
                    outcome,
                    error,
                }),
            );
        });
    }
}

fn send_command_reply(
    ctx: &Ctx,
    reply_to: Option<ActorAddress>,
    result: Result<CommandRecord, String>,
) {
    let Some(reply_to) = reply_to else {
        return;
    };
    let reply = result
        .map(ManualControlReply::Accepted)
        .unwrap_or_else(ManualControlReply::Rejected);
    let _ = ctx.send(reply_to, reply);
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, VecDeque};

    use distribution::types::NodeId as DistNodeId;
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn offer_browsing_does_not_inherit_provisioning_defaults() {
        let request = OfferSearchRequest {
            gpu_model: Some("4090".to_owned()),
            ..OfferSearchRequest::default()
        };

        let criteria = request.browse_criteria();

        assert_eq!(criteria.gpu_name_contains.as_deref(), Some("4090"));
        assert_eq!(
            criteria,
            OfferBrowseCriteria {
                gpu_name_contains: Some("4090".to_owned()),
                ..OfferBrowseCriteria::default()
            }
        );
    }

    fn spec(node_id: u64) -> NodeProvisionSpec {
        NodeProvisionSpec {
            run_id: 7,
            node_id,
            attempt_id: 0,
            stage_index: Some(0),
            image: "test-image".to_owned(),
            env: Vec::new(),
            args: Vec::new(),
            mounts: Vec::new(),
        }
    }

    fn ready_core() -> ManualControl {
        ManualControl::new(
            ClusterSnapshot::fresh(7, "test"),
            ProviderReadiness::ready(),
        )
    }

    fn facts(node_id: u64, readiness_id: u64) -> RuntimeFacts {
        RuntimeFacts {
            run_id: 7,
            attempt_id: 0,
            endpoint: format!("endpoint-{node_id}"),
            node_actor: ActorAddress::default(),
            swim_node_id: DistNodeId([node_id as u8; 32]),
            stage_index: 0,
            readiness_id,
        }
    }

    /// Stub persistence: acknowledge every snapshot in FIFO order and return
    /// only externally observable provider/network effects.
    fn settle_persistence(core: &mut ManualControl) -> Vec<ManualAction> {
        let mut effects = Vec::new();
        loop {
            let actions = core.take_actions().collect::<Vec<_>>();
            if actions.is_empty() {
                break;
            }
            for action in actions {
                match action {
                    ManualAction::Persist { generation, .. } => {
                        core.persisted(generation, Ok(())).unwrap();
                    }
                    effect => effects.push(effect),
                }
            }
        }
        effects
    }

    fn request_one(core: &mut ManualControl, command_id: &str, offer: Option<u64>) {
        core.request_provision(
            ProvisionRequest {
                command_id: command_id.to_owned(),
                count: 1,
                selected_offer_ids: offer.into_iter().collect(),
            },
            |node_id| Ok(spec(node_id)),
        )
        .unwrap();
    }

    #[test]
    fn provider_readiness_rejects_without_allocating_and_can_be_corrected() {
        let mut core = ManualControl::new(
            ClusterSnapshot::fresh(7, "test"),
            ProviderReadiness::unconfigured_for("vastai", "missing key"),
        );
        request_one(&mut core, "not-ready", Some(44));
        settle_persistence(&mut core);
        assert!(core.snapshot().nodes.is_empty());
        assert_eq!(
            core.snapshot().commands["not-ready"].state,
            CommandState::Failed
        );

        core.set_provider_validating();
        assert_eq!(core.provider().kind, ProviderReadinessKind::Validating);
        assert_eq!(core.provider().name, "vastai");
        core.set_provider_validation(Ok(()));
        assert_eq!(core.provider().name, "vastai");
        request_one(&mut core, "ready", Some(44));
        let effects = settle_persistence(&mut core);
        assert!(matches!(
            effects.as_slice(),
            [ManualAction::Create {
                node_id: 1,
                selected_offer_id: Some(44),
                ..
            }]
        ));
    }

    #[test]
    fn full_provision_lifecycle_requires_every_persistence_barrier() {
        let mut core = ready_core();
        request_one(&mut core, "provision", Some(9001));
        let effects = settle_persistence(&mut core);
        assert!(matches!(effects.as_slice(), [ManualAction::Create { .. }]));

        core.effect_finished(
            1,
            EffectKind::Create,
            Ok(EffectOutcome::Created {
                provider_ref: "contract-11".to_owned(),
            }),
        )
        .unwrap();
        assert!(matches!(
            settle_persistence(&mut core).as_slice(),
            [ManualAction::StartBootstrap { node_id: 1 }]
        ));
        core.effect_finished(
            1,
            EffectKind::StartBootstrap,
            Ok(EffectOutcome::BootstrapStarted),
        )
        .unwrap();
        assert!(settle_persistence(&mut core).is_empty());
        assert_eq!(core.snapshot().node(1).unwrap().phase, NodePhase::Joining);

        core.runtime_ready(1, facts(1, 55)).unwrap();
        assert!(settle_persistence(&mut core).is_empty());
        core.join_barrier_satisfied(1).unwrap();
        assert!(matches!(
            settle_persistence(&mut core).as_slice(),
            [ManualAction::SendRuntimeReadyAck { node_id: 1, .. }]
        ));
        core.node_ack(1, 55).unwrap();
        assert!(matches!(
            settle_persistence(&mut core).as_slice(),
            [ManualAction::CompleteBootstrap { node_id: 1 }]
        ));
        core.effect_finished(
            1,
            EffectKind::CompleteBootstrap,
            Ok(EffectOutcome::BootstrapCompleted),
        )
        .unwrap();
        assert!(settle_persistence(&mut core).is_empty());
        assert_eq!(core.snapshot().node(1).unwrap().phase, NodePhase::Running);
        assert_eq!(
            core.snapshot().commands["provision"].state,
            CommandState::Succeeded
        );
    }

    #[test]
    fn kill_during_create_waits_for_and_stops_the_exact_result() {
        let mut core = ready_core();
        request_one(&mut core, "provision", Some(77));
        assert!(matches!(
            settle_persistence(&mut core).as_slice(),
            [ManualAction::Create { .. }]
        ));

        core.request_kill(KillRequest {
            command_id: "kill".to_owned(),
            logical_node_id: 1,
        })
        .unwrap();
        assert!(settle_persistence(&mut core).is_empty());
        core.effect_finished(
            1,
            EffectKind::Create,
            Ok(EffectOutcome::Created {
                provider_ref: "exact-77".to_owned(),
            }),
        )
        .unwrap();
        assert!(matches!(
            settle_persistence(&mut core).as_slice(),
            [ManualAction::Stop { node_id: 1, .. }]
        ));
        core.effect_finished(1, EffectKind::Stop, Ok(EffectOutcome::Stopped))
            .unwrap();
        settle_persistence(&mut core);
        assert_eq!(core.snapshot().node(1).unwrap().phase, NodePhase::Stopped);
        assert_eq!(
            core.snapshot().commands["kill"].state,
            CommandState::Succeeded
        );
        assert_eq!(
            core.snapshot().commands["provision"].state,
            CommandState::Failed
        );
    }

    #[test]
    fn duplicate_ids_never_dispatch_a_second_effect() {
        let mut core = ready_core();
        request_one(&mut core, "same", None);
        let first = settle_persistence(&mut core);
        request_one(&mut core, "same", None);
        let duplicate = settle_persistence(&mut core);
        assert_eq!(first.len(), 1);
        assert!(duplicate.is_empty());
        assert_eq!(core.snapshot().nodes.len(), 1);
        assert_eq!(core.snapshot().next_node_id, 2);
    }

    #[test]
    fn stop_failure_is_visible_and_new_command_retries_same_node() {
        let mut core = ready_core();
        request_one(&mut core, "provision", None);
        settle_persistence(&mut core);
        core.request_kill(KillRequest {
            command_id: "kill-1".to_owned(),
            logical_node_id: 1,
        })
        .unwrap();
        settle_persistence(&mut core);
        core.effect_finished(
            1,
            EffectKind::Create,
            Ok(EffectOutcome::Created {
                provider_ref: "resource-1".to_owned(),
            }),
        )
        .unwrap();
        settle_persistence(&mut core);
        core.effect_finished(1, EffectKind::Stop, Err("teardown unavailable".to_owned()))
            .unwrap();
        settle_persistence(&mut core);
        assert_eq!(
            core.snapshot().node(1).unwrap().phase,
            NodePhase::StopFailed
        );

        core.request_kill(KillRequest {
            command_id: "kill-2".to_owned(),
            logical_node_id: 1,
        })
        .unwrap();
        assert!(matches!(
            settle_persistence(&mut core).as_slice(),
            [ManualAction::Stop { node_id: 1, .. }]
        ));
    }

    #[test]
    fn recovery_adopts_or_stops_without_issuing_create() {
        for phase in [
            NodePhase::Creating,
            NodePhase::Bootstrapping,
            NodePhase::Joining,
            NodePhase::Acknowledging,
            NodePhase::Running,
            NodePhase::KillRequested,
            NodePhase::Stopping,
            NodePhase::StopFailed,
        ] {
            let mut snapshot = ClusterSnapshot::fresh(7, "test");
            snapshot.next_node_id = 2;
            snapshot.upsert_node(SnapshotNode {
                logical_node_id: 1,
                spec: Some(spec(1)),
                selected_offer_id: Some(99),
                provider_ref: Some("resource-1".to_owned()),
                phase,
                runtime: None,
                last_error: None,
                last_seen_unix_ms: 0,
            });
            let mut core = ManualControl::new(snapshot, ProviderReadiness::ready());
            core.begin_recovery();
            assert!(matches!(
                core.take_actions().collect::<Vec<_>>().as_slice(),
                [ManualAction::Recover { node_id: 1, .. }]
            ));
            core.effect_finished(
                1,
                EffectKind::Recover,
                Ok(EffectOutcome::Recovered {
                    provider_ref: Some("resource-1".to_owned()),
                }),
            )
            .unwrap();
            let effects = settle_persistence(&mut core);
            assert!(
                effects
                    .iter()
                    .all(|effect| !matches!(effect, ManualAction::Create { .. }))
            );
            if matches!(phase, NodePhase::Creating | NodePhase::Bootstrapping) {
                assert!(matches!(
                    effects.as_slice(),
                    [ManualAction::StartBootstrap { node_id: 1 }]
                ));
            }
            if matches!(
                phase,
                NodePhase::KillRequested | NodePhase::Stopping | NodePhase::StopFailed
            ) {
                assert!(matches!(
                    effects.as_slice(),
                    [ManualAction::Stop { node_id: 1, .. }]
                ));
            }
        }
    }

    #[test]
    fn missing_resource_during_create_resumes_idempotent_create() {
        let mut snapshot = ClusterSnapshot::fresh(7, "test");
        snapshot.upsert_node(SnapshotNode {
            logical_node_id: 1,
            spec: Some(spec(1)),
            selected_offer_id: None,
            provider_ref: Some("missing".to_owned()),
            phase: NodePhase::Creating,
            runtime: None,
            last_error: None,
            last_seen_unix_ms: 0,
        });
        let mut core = ManualControl::new(snapshot, ProviderReadiness::ready());
        core.begin_recovery();
        core.take_actions().for_each(drop);
        core.effect_finished(
            1,
            EffectKind::Recover,
            Ok(EffectOutcome::Recovered { provider_ref: None }),
        )
        .unwrap();
        let effects = settle_persistence(&mut core);
        assert!(matches!(
            effects.as_slice(),
            [ManualAction::Create { node_id: 1, .. }]
        ));
        assert_eq!(core.snapshot().node(1).unwrap().phase, NodePhase::Creating);
    }

    #[test]
    fn missing_resource_during_stop_completes_stop_idempotently() {
        let mut snapshot = ClusterSnapshot::fresh(7, "test");
        snapshot.upsert_node(SnapshotNode {
            logical_node_id: 1,
            spec: Some(spec(1)),
            selected_offer_id: None,
            provider_ref: Some("missing".to_owned()),
            phase: NodePhase::Stopping,
            runtime: None,
            last_error: None,
            last_seen_unix_ms: 0,
        });
        let mut core = ManualControl::new(snapshot, ProviderReadiness::ready());
        core.begin_recovery();
        core.take_actions().for_each(drop);
        core.effect_finished(
            1,
            EffectKind::Recover,
            Ok(EffectOutcome::Recovered { provider_ref: None }),
        )
        .unwrap();
        assert!(settle_persistence(&mut core).is_empty());
        assert_eq!(core.snapshot().node(1).unwrap().phase, NodePhase::Stopped);
    }

    #[test]
    fn rejoin_rebinds_to_current_address_and_rejects_wrong_attempt() {
        let mut snapshot = ClusterSnapshot::fresh(7, "test");
        snapshot.upsert_node(SnapshotNode {
            logical_node_id: 1,
            spec: Some(spec(1)),
            selected_offer_id: None,
            provider_ref: Some("resource".to_owned()),
            phase: NodePhase::Joining,
            runtime: None,
            last_error: None,
            last_seen_unix_ms: 0,
        });
        let mut core = ManualControl::new(snapshot, ProviderReadiness::ready());
        let current = ActorAddress([9; 32]);
        let binding = core
            .rejoin(
                &RejoinHello {
                    run_id: 7,
                    logical_node_id: 1,
                    attempt_id: 0,
                    selected_offer_id: None,
                    endpoint: "endpoint".to_owned(),
                    swim_node_id: DistNodeId([7; 32]),
                    stage_index: 0,
                    node_actor: ActorAddress([8; 32]),
                },
                current,
                12,
                ActorAddress([6; 32]),
            )
            .unwrap();
        assert_eq!(binding.orchestrator_actor, current);
        assert_eq!(binding.control_generation, 12);
        let actions = core.take_actions().collect::<Vec<_>>();
        let [ManualAction::Persist { generation, .. }] = actions.as_slice() else {
            panic!("rejoin must persist before replying: {actions:?}");
        };
        core.persisted(*generation, Ok(())).unwrap();
        assert!(matches!(
            core.take_actions().collect::<Vec<_>>().as_slice(),
            [ManualAction::SendRejoinReply {
                reply_to,
                binding: persisted_binding,
            }] if *reply_to == ActorAddress([6; 32]) && persisted_binding == &binding
        ));
        let node = core.snapshot().node(1).unwrap();
        assert_eq!(node.phase, NodePhase::Running);
        assert_eq!(
            node.runtime.as_ref().unwrap().node_actor,
            ActorAddress([8; 32])
        );
        assert!(
            core.rejoin(
                &RejoinHello {
                    run_id: 7,
                    logical_node_id: 1,
                    attempt_id: 1,
                    selected_offer_id: None,
                    endpoint: "endpoint".to_owned(),
                    swim_node_id: DistNodeId([7; 32]),
                    stage_index: 0,
                    node_actor: ActorAddress([8; 32]),
                },
                current,
                12,
                ActorAddress([6; 32]),
            )
            .is_err()
        );
    }

    #[derive(Clone, Debug)]
    struct PendingEffect {
        node_id: u64,
        kind: EffectKind,
    }

    fn absorb_actions(
        core: &mut ManualControl,
        pending: &mut VecDeque<PendingEffect>,
        active: &mut BTreeMap<u64, EffectKind>,
    ) {
        loop {
            let actions = core.take_actions().collect::<Vec<_>>();
            if actions.is_empty() {
                break;
            }
            for action in actions {
                match action {
                    ManualAction::Persist { generation, .. } => {
                        let _ = core.persisted(generation, Ok(()));
                    }
                    ManualAction::Create { node_id, .. } => {
                        assert!(active.insert(node_id, EffectKind::Create).is_none());
                        pending.push_back(PendingEffect {
                            node_id,
                            kind: EffectKind::Create,
                        });
                    }
                    ManualAction::Recover { node_id, .. } => {
                        assert!(active.insert(node_id, EffectKind::Recover).is_none());
                        pending.push_back(PendingEffect {
                            node_id,
                            kind: EffectKind::Recover,
                        });
                    }
                    ManualAction::StartBootstrap { node_id } => {
                        assert!(active.insert(node_id, EffectKind::StartBootstrap).is_none());
                        pending.push_back(PendingEffect {
                            node_id,
                            kind: EffectKind::StartBootstrap,
                        });
                    }
                    ManualAction::CompleteBootstrap { node_id } => {
                        assert!(
                            active
                                .insert(node_id, EffectKind::CompleteBootstrap)
                                .is_none()
                        );
                        pending.push_back(PendingEffect {
                            node_id,
                            kind: EffectKind::CompleteBootstrap,
                        });
                    }
                    ManualAction::Stop { node_id, .. } => {
                        assert!(active.insert(node_id, EffectKind::Stop).is_none());
                        pending.push_back(PendingEffect {
                            node_id,
                            kind: EffectKind::Stop,
                        });
                    }
                    ManualAction::SendRuntimeReadyAck { .. }
                    | ManualAction::SendRejoinReply { .. } => {}
                }
            }
        }
        assert_eq!(&*active, &core.in_flight);
    }

    fn assert_invariants(core: &ManualControl) {
        let mut ids = BTreeSet::new();
        for node in core
            .snapshot()
            .nodes
            .iter()
            .filter(|node| node.logical_node_id != 0)
        {
            assert!(ids.insert(node.logical_node_id));
            assert!(node.logical_node_id < core.snapshot().next_node_id);
            if node.phase == NodePhase::Stopped {
                assert!(!core.in_flight.contains_key(&node.logical_node_id));
            }
            if node.selected_offer_id.is_some() {
                assert!(node.spec.is_some());
            }
        }
        for (command_id, command) in &core.snapshot().commands {
            assert_eq!(command_id, &command.command_id);
            assert!(!command.command_id.trim().is_empty());
            assert!(command.node_ids.iter().all(|id| ids.contains(id)));
        }
        assert!(core.in_flight.keys().all(|node_id| ids.contains(node_id)));
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 2048,
            max_shrink_iters: 20_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn aggressive_random_event_stream_preserves_control_invariants(
            operations in prop::collection::vec(any::<u8>(), 1..768)
        ) {
            let mut core = ready_core();
            let mut pending = VecDeque::new();
            let mut active = BTreeMap::new();
            let mut next_command = 0_u64;
            absorb_actions(&mut core, &mut pending, &mut active);

            for operation in operations {
                let node_ids = core
                    .snapshot()
                    .nodes
                    .iter()
                    .filter(|node| node.logical_node_id != 0)
                    .map(|node| node.logical_node_id)
                    .collect::<Vec<_>>();
                let selected_node = (!node_ids.is_empty())
                    .then(|| node_ids[usize::from(operation) % node_ids.len()]);
                match operation % 12 {
                    0 | 1 => {
                        let command_id = format!("provision-{next_command}");
                        next_command += 1;
                        let count = u32::from(operation % 3) + 1;
                        let _ = core.request_provision(
                            ProvisionRequest {
                                command_id,
                                count,
                                selected_offer_ids: (0..count)
                                    .map(|offset| 10_000 + u64::from(offset))
                                    .collect(),
                            },
                            |node_id| Ok(spec(node_id)),
                        );
                    }
                    2 => {
                        if let Some(node_id) = selected_node {
                            let _ = core.request_kill(KillRequest {
                                command_id: format!("kill-{next_command}"),
                                logical_node_id: node_id,
                            });
                            next_command += 1;
                        }
                    }
                    3 | 4 => {
                        if let Some(effect) = pending.pop_front() {
                            active.remove(&effect.node_id);
                            let outcome = match effect.kind {
                                EffectKind::Create => EffectOutcome::Created {
                                    provider_ref: format!("resource-{}", effect.node_id),
                                },
                                EffectKind::StartBootstrap => EffectOutcome::BootstrapStarted,
                                EffectKind::CompleteBootstrap => EffectOutcome::BootstrapCompleted,
                                EffectKind::Stop => EffectOutcome::Stopped,
                                EffectKind::Recover => EffectOutcome::Recovered {
                                    provider_ref: Some(format!("resource-{}", effect.node_id)),
                                },
                            };
                            let result = if operation % 4 == 0 {
                                Err(format!("injected-{}", operation))
                            } else {
                                Ok(outcome)
                            };
                            let _ = core.effect_finished(effect.node_id, effect.kind, result);
                        }
                    }
                    5 => {
                        if let Some(node_id) = selected_node {
                            let _ = core.runtime_ready(node_id, facts(node_id, u64::from(operation)));
                        }
                    }
                    6 => {
                        if let Some(node_id) = selected_node {
                            let _ = core.join_barrier_satisfied(node_id);
                        }
                    }
                    7 => {
                        if let Some(node_id) = selected_node {
                            let readiness = core
                                .snapshot()
                                .node(node_id)
                                .and_then(|node| node.runtime.as_ref())
                                .map_or(u64::MAX, |facts| facts.readiness_id);
                            let _ = core.node_ack(node_id, readiness);
                        }
                    }
                    8 => {
                        if let Some(node_id) = selected_node {
                            let _ = core.terminal_failure(
                                node_id,
                                format!("terminal-{}", operation),
                            );
                        }
                    }
                    9 => {
                        core.set_provider_validating();
                        core.set_provider_validation(if operation & 0x80 == 0 {
                            Ok(())
                        } else {
                            Err("invalid config".to_owned())
                        });
                    }
                    10 => {
                        core.begin_recovery();
                    }
                    _ => {
                        if let Some(existing) = core.snapshot().commands.keys().next().cloned() {
                            let _ = core.request_provision(
                                ProvisionRequest {
                                    command_id: existing,
                                    count: 1,
                                    selected_offer_ids: Vec::new(),
                                },
                                |node_id| Ok(spec(node_id)),
                            );
                        }
                    }
                }
                absorb_actions(&mut core, &mut pending, &mut active);
                assert_invariants(&core);
            }
        }
    }

    fn drive_rental_free_system(core: &mut ManualControl, resources: &mut BTreeSet<u64>) {
        for _ in 0..10_000 {
            let actions = core.take_actions().collect::<Vec<_>>();
            if actions.is_empty() {
                let joining = core
                    .snapshot()
                    .nodes
                    .iter()
                    .filter(|node| node.phase == NodePhase::Joining && node.runtime.is_none())
                    .map(|node| node.logical_node_id)
                    .collect::<Vec<_>>();
                if joining.is_empty() {
                    return;
                }
                for node_id in joining {
                    core.runtime_ready(node_id, facts(node_id, node_id + 100))
                        .unwrap();
                    core.join_barrier_satisfied(node_id).unwrap();
                }
                continue;
            }
            for action in actions {
                match action {
                    ManualAction::Persist {
                        generation,
                        snapshot,
                    } => {
                        let encoded = serde_json::to_vec(&snapshot).unwrap();
                        let decoded: ClusterSnapshot = serde_json::from_slice(&encoded).unwrap();
                        assert_eq!(
                            decoded.schema_version,
                            crate::orchestration::daemon::SNAPSHOT_SCHEMA_VERSION
                        );
                        assert_eq!(decoded.next_node_id, snapshot.next_node_id);
                        core.persisted(generation, Ok(())).unwrap();
                    }
                    ManualAction::Create {
                        node_id,
                        spec,
                        selected_offer_id,
                    } => {
                        if let Some(offer_id) = selected_offer_id {
                            assert!(spec.env.iter().any(|(name, value)| {
                                name == SELECTED_OFFER_ID_ENV && value == &offer_id.to_string()
                            }));
                        }
                        assert!(resources.insert(node_id));
                        core.effect_finished(
                            node_id,
                            EffectKind::Create,
                            Ok(EffectOutcome::Created {
                                provider_ref: format!("resource-{node_id}"),
                            }),
                        )
                        .unwrap();
                    }
                    ManualAction::Recover { node_id, .. } => {
                        let provider_ref = resources
                            .contains(&node_id)
                            .then(|| format!("resource-{node_id}"));
                        core.effect_finished(
                            node_id,
                            EffectKind::Recover,
                            Ok(EffectOutcome::Recovered { provider_ref }),
                        )
                        .unwrap();
                    }
                    ManualAction::StartBootstrap { node_id } => {
                        core.effect_finished(
                            node_id,
                            EffectKind::StartBootstrap,
                            Ok(EffectOutcome::BootstrapStarted),
                        )
                        .unwrap();
                    }
                    ManualAction::CompleteBootstrap { node_id } => {
                        core.effect_finished(
                            node_id,
                            EffectKind::CompleteBootstrap,
                            Ok(EffectOutcome::BootstrapCompleted),
                        )
                        .unwrap();
                    }
                    ManualAction::Stop { node_id, .. } => {
                        resources.remove(&node_id);
                        core.effect_finished(node_id, EffectKind::Stop, Ok(EffectOutcome::Stopped))
                            .unwrap();
                    }
                    ManualAction::SendRuntimeReadyAck { node_id, facts } => {
                        core.node_ack(node_id, facts.readiness_id).unwrap();
                    }
                    ManualAction::SendRejoinReply { .. } => {}
                }
            }
        }
        panic!("rental-free control harness did not quiesce");
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            max_shrink_iters: 10_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn rental_free_end_to_end_sequences_converge(
            operations in prop::collection::vec((any::<u8>(), any::<u8>()), 1..256)
        ) {
            let mut core = ready_core();
            let mut resources = BTreeSet::new();
            let mut command_serial = 0_u64;

            for (kind, selector) in operations {
                match kind % 6 {
                    0 | 1 => {
                        let count = u32::from(selector % 2) + 1;
                        let command_id = format!("provision-{command_serial}");
                        command_serial += 1;
                        core.request_provision(
                            ProvisionRequest {
                                command_id,
                                count,
                                selected_offer_ids: (0..count)
                                    .map(|offset| 100_000 + command_serial * 8 + u64::from(offset))
                                    .collect(),
                            },
                            |node_id| Ok(spec(node_id)),
                        )
                        .unwrap();
                    }
                    2 => {
                        let live = core
                            .snapshot()
                            .nodes
                            .iter()
                            .filter(|node| node.phase != NodePhase::Stopped)
                            .map(|node| node.logical_node_id)
                            .collect::<Vec<_>>();
                        if !live.is_empty() {
                            core.request_kill(KillRequest {
                                command_id: format!("kill-{command_serial}"),
                                logical_node_id: live[usize::from(selector) % live.len()],
                            })
                            .unwrap();
                            command_serial += 1;
                        }
                    }
                    3 => {
                        drive_rental_free_system(&mut core, &mut resources);
                        let encoded = serde_json::to_vec(core.snapshot()).unwrap();
                        let snapshot: ClusterSnapshot = serde_json::from_slice(&encoded).unwrap();
                        core = ManualControl::new(snapshot, ProviderReadiness::ready());
                        core.begin_recovery();
                    }
                    4 => {
                        core.set_provider_validating();
                        core.set_provider_validation(Err("injected invalid config".to_owned()));
                        core.request_provision(
                            ProvisionRequest {
                                command_id: format!("rejected-{command_serial}"),
                                count: 1,
                                selected_offer_ids: vec![200_000 + command_serial],
                            },
                            |node_id| Ok(spec(node_id)),
                        )
                        .unwrap();
                        command_serial += 1;
                        core.set_provider_validation(Ok(()));
                    }
                    _ => {
                        if let Some(existing) = core.snapshot().commands.keys().next().cloned() {
                            let before = core.snapshot().next_node_id;
                            core.request_provision(
                                ProvisionRequest {
                                    command_id: existing,
                                    count: 1,
                                    selected_offer_ids: vec![u64::from(selector)],
                                },
                                |node_id| Ok(spec(node_id)),
                            )
                            .unwrap();
                            prop_assert_eq!(core.snapshot().next_node_id, before);
                        }
                    }
                }
                drive_rental_free_system(&mut core, &mut resources);
                assert_invariants(&core);
            }

            let live = core
                .snapshot()
                .nodes
                .iter()
                .filter(|node| node.phase != NodePhase::Stopped)
                .map(|node| node.logical_node_id)
                .collect::<Vec<_>>();
            for node_id in live {
                core.request_kill(KillRequest {
                    command_id: format!("cleanup-{node_id}"),
                    logical_node_id: node_id,
                })
                .unwrap();
            }
            drive_rental_free_system(&mut core, &mut resources);
            prop_assert!(resources.is_empty());
            prop_assert!(core.snapshot().nodes.iter().all(|node| node.phase == NodePhase::Stopped));
            prop_assert!(core.snapshot().commands.values().all(|command| command.state.is_terminal()));
        }
    }
}
