//! Level-triggered cluster reconciliation over the provider-neutral node lifecycle.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::{Duration, SystemTime};

use crate::executor::{EffectError, EffectFailureDisposition};
use serde::{Deserialize, Serialize};

use crate::node::{
    BootstrapFacts, BootstrapObservation, BootstrapSessionId, BootstrapStage, CreateLeaseResult,
    LogicalNodeId, LogicalNodeSpec, NodeManagerCommand, NodeRecord, NodeStage, RunId,
    RunNodeGroupSpec, SshEndpoint, SwactorFacts, SwactorId, expand_node_group,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClusterShape {
    pub run_id: RunId,
    pub generation: u64,
    pub groups: Vec<RunNodeGroupSpec>,
}

impl ClusterShape {
    pub fn expand(&self) -> Result<BTreeMap<LogicalNodeId, LogicalNodeSpec>, ShapeError> {
        let mut group_ids = BTreeSet::new();
        let mut nodes = BTreeMap::new();

        for group in &self.groups {
            if group.run_id != self.run_id {
                return Err(ShapeError::new(format!(
                    "group {} belongs to run {}, expected {}",
                    group.group_id.0, group.run_id.0, self.run_id.0
                )));
            }
            if !group_ids.insert(group.group_id.clone()) {
                return Err(ShapeError::new(format!(
                    "duplicate node group {}",
                    group.group_id.0
                )));
            }
            for (field, value) in [
                ("min_down_mbps", group.shape.min_down_mbps),
                ("min_up_mbps", group.shape.min_up_mbps),
                ("min_reliability", group.shape.min_reliability),
            ] {
                if value.is_some_and(|value| !value.is_finite()) {
                    return Err(ShapeError::new(format!(
                        "node group {} has non-finite {field}",
                        group.group_id.0
                    )));
                }
            }
            for node in expand_node_group(group) {
                let id = node.logical_node_id.clone();
                if nodes.insert(id.clone(), node).is_some() {
                    return Err(ShapeError::new(format!("duplicate logical node {}", id.0)));
                }
            }
        }

        Ok(nodes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShapeError {
    pub reason: String,
}

impl ShapeError {
    fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl fmt::Display for ShapeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.reason)
    }
}

impl std::error::Error for ShapeError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeAttemptId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OperationId {
    pub attempt: NodeAttemptId,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeIntent {
    Active,
    Deleting,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationKind {
    CreateLease,
    LookupEndpoint,
    StartBootstrap,
    BootstrapConvergenceObserved,
    CancelBootstrap,
    DestroyLease,
}

impl OperationKind {
    pub(crate) fn for_command(command: &NodeManagerCommand) -> Self {
        match command {
            NodeManagerCommand::CreateLease(_) => Self::CreateLease,
            NodeManagerCommand::LookupEndpoint(_) => Self::LookupEndpoint,
            NodeManagerCommand::StartBootstrap(_) => Self::StartBootstrap,
            NodeManagerCommand::BootstrapConvergenceObserved { .. } => {
                Self::BootstrapConvergenceObserved
            }
            NodeManagerCommand::CancelBootstrap { .. } => Self::CancelBootstrap,
            NodeManagerCommand::DestroyLease(_) => Self::DestroyLease,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingOperation {
    pub id: OperationId,
    pub kind: OperationKind,
    pub deadline: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedOperation {
    pub node: LogicalNodeId,
    pub operation: OperationId,
    pub kind: OperationKind,
    pub deadline: SystemTime,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryState {
    pub consecutive_failures: u32,
    pub next_effect_at: Option<SystemTime>,
    pub restart_at: Option<SystemTime>,
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ambiguous_operation: Option<OperationKind>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ManagedNode {
    pub attempt: NodeAttemptId,
    pub intent: NodeIntent,
    pub record: NodeRecord,
    pub active_bootstrap: Option<BootstrapSessionId>,
    pub pending: Option<PendingOperation>,
    pub next_operation_sequence: u64,
    pub retry: RetryState,
}

impl ManagedNode {
    pub fn new(attempt: NodeAttemptId, desired: LogicalNodeSpec) -> Self {
        Self {
            attempt,
            intent: NodeIntent::Active,
            record: NodeRecord::from_spec(desired),
            active_bootstrap: None,
            pending: None,
            next_operation_sequence: 1,
            retry: RetryState::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClusterState {
    pub observed_generation: u64,
    pub next_attempt_id: u64,
    pub nodes: BTreeMap<LogicalNodeId, ManagedNode>,
}

impl Default for ClusterState {
    fn default() -> Self {
        Self {
            observed_generation: 0,
            next_attempt_id: 1,
            nodes: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReconcilePlan {
    pub actions: Vec<NodeAction>,
    pub observed_generation: u64,
    pub requeue_at: Option<SystemTime>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum NodeAction {
    Insert {
        attempt: NodeAttemptId,
        desired: LogicalNodeSpec,
    },
    BeginDelete {
        node: LogicalNodeId,
        expected_attempt: NodeAttemptId,
    },
    MarkDestroyed {
        node: LogicalNodeId,
        expected_attempt: NodeAttemptId,
    },
    Restart {
        node: LogicalNodeId,
        expected_attempt: NodeAttemptId,
        new_attempt: NodeAttemptId,
        desired: LogicalNodeSpec,
    },
    Reap {
        node: LogicalNodeId,
        expected_attempt: NodeAttemptId,
    },
    Dispatch(PlannedEffect),
}

impl NodeAction {
    pub fn node(&self) -> &LogicalNodeId {
        match self {
            Self::Insert { desired, .. } => &desired.logical_node_id,
            Self::BeginDelete { node, .. }
            | Self::MarkDestroyed { node, .. }
            | Self::Restart { node, .. }
            | Self::Reap { node, .. } => node,
            Self::Dispatch(effect) => &effect.node,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlannedEffect {
    pub node: LogicalNodeId,
    pub operation: OperationId,
    pub command: NodeManagerCommand,
}

/// Non-blocking submission boundary. Implementations queue engine-hosted work;
/// provider I/O must not execute inline in `submit`.
pub trait EffectExecutor {
    type SubmitError: fmt::Display;

    fn submit(&mut self, effect: &PlannedEffect) -> Result<(), Self::SubmitError>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct NodeDecision {
    pub action: Option<NodeDecisionAction>,
    pub requeue_at: Option<SystemTime>,
}

impl NodeDecision {
    fn action(action: NodeDecisionAction) -> Self {
        Self {
            action: Some(action),
            requeue_at: None,
        }
    }

    fn wait(requeue_at: Option<SystemTime>) -> Self {
        Self {
            action: None,
            requeue_at,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum NodeDecisionAction {
    BeginDelete,
    MarkDestroyed,
    Restart(LogicalNodeSpec),
    Reap,
    Dispatch(NodeManagerCommand),
}

pub fn reconcile(
    observed: &ClusterState,
    desired: &ClusterShape,
    now: SystemTime,
) -> Result<ReconcilePlan, ShapeError> {
    let desired_nodes = desired.expand()?;
    let keys: BTreeSet<_> = observed
        .nodes
        .keys()
        .chain(desired_nodes.keys())
        .cloned()
        .collect();
    let mut next_attempt = observed.next_attempt_id;
    let mut actions = Vec::new();
    let mut requeue_at = None;

    for id in keys {
        match (observed.nodes.get(&id), desired_nodes.get(&id)) {
            (None, Some(spec)) => {
                let attempt = allocate_attempt(&mut next_attempt)?;
                actions.push(NodeAction::Insert {
                    attempt,
                    desired: spec.clone(),
                });
            }
            (Some(node), desired) => {
                let decision = reconcile_node(node, desired, now);
                requeue_at = earliest(requeue_at, decision.requeue_at);
                let Some(action) = decision.action else {
                    continue;
                };
                let action = match action {
                    NodeDecisionAction::BeginDelete => NodeAction::BeginDelete {
                        node: id,
                        expected_attempt: node.attempt,
                    },
                    NodeDecisionAction::MarkDestroyed => NodeAction::MarkDestroyed {
                        node: id,
                        expected_attempt: node.attempt,
                    },
                    NodeDecisionAction::Restart(spec) => NodeAction::Restart {
                        node: id,
                        expected_attempt: node.attempt,
                        new_attempt: allocate_attempt(&mut next_attempt)?,
                        desired: spec,
                    },
                    NodeDecisionAction::Reap => NodeAction::Reap {
                        node: id,
                        expected_attempt: node.attempt,
                    },
                    NodeDecisionAction::Dispatch(command) => NodeAction::Dispatch(PlannedEffect {
                        node: id,
                        operation: OperationId {
                            attempt: node.attempt,
                            sequence: node.next_operation_sequence,
                        },
                        command,
                    }),
                };
                actions.push(action);
            }
            (None, None) => unreachable!("union key must exist in one map"),
        }
    }

    Ok(ReconcilePlan {
        actions,
        observed_generation: desired.generation,
        requeue_at,
    })
}

fn allocate_attempt(next: &mut u64) -> Result<NodeAttemptId, ShapeError> {
    let attempt = NodeAttemptId(*next);
    *next = next
        .checked_add(1)
        .ok_or_else(|| ShapeError::new("node attempt allocator exhausted"))?;
    Ok(attempt)
}

pub fn reconcile_node(
    node: &ManagedNode,
    desired: Option<&LogicalNodeSpec>,
    now: SystemTime,
) -> NodeDecision {
    if node.record.stage == NodeStage::Destroyed {
        return match desired {
            None => NodeDecision::action(NodeDecisionAction::Reap),
            Some(spec) => {
                if let Some(restart_at) = node.retry.restart_at
                    && now < restart_at
                {
                    NodeDecision::wait(Some(restart_at))
                } else {
                    NodeDecision::action(NodeDecisionAction::Restart(spec.clone()))
                }
            }
        };
    }

    if node.intent == NodeIntent::Active
        && (desired.is_none()
            || desired.is_some_and(|spec| spec != &node.record.desired)
            || node.record.stage == NodeStage::Failed)
    {
        return NodeDecision::action(NodeDecisionAction::BeginDelete);
    }

    if let Some(pending) = &node.pending {
        return NodeDecision::wait(Some(pending.deadline));
    }

    if node.intent == NodeIntent::Deleting {
        if let Some(next_effect_at) = node.retry.next_effect_at
            && now < next_effect_at
        {
            return NodeDecision::wait(Some(next_effect_at));
        }
        match node.retry.ambiguous_operation {
            Some(OperationKind::CreateLease) => {
                return NodeDecision::action(NodeDecisionAction::Dispatch(
                    NodeManagerCommand::CreateLease(crate::node::CreateLeaseRequest {
                        spec: node.record.desired.clone(),
                    }),
                ));
            }
            Some(OperationKind::StartBootstrap) => {
                if let Some(command) = start_bootstrap_command(&node.record) {
                    return NodeDecision::action(NodeDecisionAction::Dispatch(command));
                }
            }
            _ => {}
        }
        if node.active_bootstrap.is_none() && node.record.lease.is_none() {
            return NodeDecision::action(NodeDecisionAction::MarkDestroyed);
        }
        if let Some(session_id) = node.active_bootstrap {
            return NodeDecision::action(NodeDecisionAction::Dispatch(
                NodeManagerCommand::CancelBootstrap { session_id },
            ));
        }
        if let Some(lease) = &node.record.lease {
            return NodeDecision::action(NodeDecisionAction::Dispatch(
                NodeManagerCommand::DestroyLease(lease.destroy_handle.clone()),
            ));
        }
        return NodeDecision::action(NodeDecisionAction::MarkDestroyed);
    }

    if let Some(next_effect_at) = node.retry.next_effect_at
        && now < next_effect_at
    {
        return NodeDecision::wait(Some(next_effect_at));
    }

    if node.record.ready && matches!(node.record.stage, NodeStage::HandedOff | NodeStage::Dormant) {
        return NodeDecision::wait(None);
    }

    if node.record.lease.is_none() {
        return NodeDecision::action(NodeDecisionAction::Dispatch(
            NodeManagerCommand::CreateLease(crate::node::CreateLeaseRequest {
                spec: node.record.desired.clone(),
            }),
        ));
    }

    if node.record.connection.is_none() {
        return NodeDecision::action(NodeDecisionAction::Dispatch(
            NodeManagerCommand::LookupEndpoint(
                node.record.lease.clone().expect("lease checked above"),
            ),
        ));
    }

    if node.record.stage == NodeStage::SwactorJoined {
        return match (node.active_bootstrap, node.record.swactor.as_ref()) {
            (Some(session_id), Some(swactor)) => NodeDecision::action(
                NodeDecisionAction::Dispatch(NodeManagerCommand::BootstrapConvergenceObserved {
                    session_id,
                    swactor_id: swactor.swactor_id.clone(),
                }),
            ),
            _ => NodeDecision::wait(None),
        };
    }

    if node.active_bootstrap.is_some() || node.record.stage == NodeStage::BootstrapRunning {
        return NodeDecision::wait(None);
    }

    let command =
        start_bootstrap_command(&node.record).expect("lease and connection checked above");
    NodeDecision::action(NodeDecisionAction::Dispatch(command))
}

fn start_bootstrap_command(record: &NodeRecord) -> Option<NodeManagerCommand> {
    let lease_id = record.lease.as_ref()?.lease_id.clone();
    let ssh = record.connection.clone()?;
    Some(NodeManagerCommand::StartBootstrap(
        crate::node::BootstrapSessionSpec {
            telemetry: crate::node::TelemetryStreamId(format!(
                "run/{}/node/{}/bootstrap",
                record.run_id.0, record.logical_node_id.0
            )),
            run_id: record.run_id.clone(),
            logical_node_id: record.logical_node_id.clone(),
            lease_id,
            ssh,
            boot: record.desired.boot.clone(),
            swarm_join: record.desired.swarm_join.clone(),
        },
    ))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub jitter: Duration,
    pub operation_timeout: Duration,
    pub endpoint_probe_interval: Duration,
}

impl RetryPolicy {
    pub fn delay_for_failure(&self, consecutive_failures: u32) -> Duration {
        let exponent = consecutive_failures.saturating_sub(1).min(31);
        let multiplier = 1_u32 << exponent;
        self.initial_delay
            .checked_mul(multiplier)
            .unwrap_or(self.max_delay)
            .min(self.max_delay)
    }
}

fn sampled_retry_delay(
    policy: &RetryPolicy,
    attempt: NodeAttemptId,
    consecutive_failures: u32,
) -> Duration {
    let base = policy.delay_for_failure(consecutive_failures);
    let jitter_nanos = policy.jitter.as_nanos();
    if jitter_nanos == 0 {
        return base;
    }
    let mixed = attempt
        .0
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(u64::from(consecutive_failures))
        .rotate_left(17);
    let sampled_nanos = u128::from(mixed) % (jitter_nanos + 1);
    let sampled = Duration::new(
        (sampled_nanos / 1_000_000_000) as u64,
        (sampled_nanos % 1_000_000_000) as u32,
    );
    base.checked_add(sampled).unwrap_or(Duration::MAX)
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            jitter: Duration::ZERO,
            operation_timeout: Duration::from_secs(120),
            endpoint_probe_interval: Duration::from_secs(2),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationOutcome {
    LeaseCreated(Box<CreateLeaseResult>),
    EndpointLookup(Option<SshEndpoint>),
    BootstrapStarted { session_id: BootstrapSessionId },
    BootstrapConvergenceAccepted,
    BootstrapCancelled,
    LeaseDestroyed,
}

impl OperationOutcome {
    pub(crate) fn kind(&self) -> OperationKind {
        match self {
            Self::LeaseCreated(_) => OperationKind::CreateLease,
            Self::EndpointLookup(_) => OperationKind::LookupEndpoint,
            Self::BootstrapStarted { .. } => OperationKind::StartBootstrap,
            Self::BootstrapConvergenceAccepted => OperationKind::BootstrapConvergenceObserved,
            Self::BootstrapCancelled => OperationKind::CancelBootstrap,
            Self::LeaseDestroyed => OperationKind::DestroyLease,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutorResult {
    pub node: LogicalNodeId,
    pub operation: OperationId,
    pub result: Result<OperationOutcome, EffectError>,
}

// Keeping outcomes inline avoids allocating on every executor result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeObservation {
    OperationSucceeded {
        operation: OperationId,
        outcome: OperationOutcome,
    },
    OperationFailed {
        operation: OperationId,
        error: EffectError,
    },
    BootstrapObserved {
        session_id: BootstrapSessionId,
        observation: BootstrapObservation,
    },
    BootstrapFailed {
        session_id: BootstrapSessionId,
        reason: String,
    },
    SwactorJoined {
        session_id: BootstrapSessionId,
        swactor_id: SwactorId,
    },
    BootstrapClosed {
        session_id: BootstrapSessionId,
    },
}

pub fn observe(
    node: &mut ManagedNode,
    observation: NodeObservation,
    now: SystemTime,
    retry: &RetryPolicy,
) {
    if node.record.stage == NodeStage::Failed
        && matches!(
            &observation,
            NodeObservation::BootstrapObserved { .. }
                | NodeObservation::BootstrapFailed { .. }
                | NodeObservation::SwactorJoined { .. }
                | NodeObservation::BootstrapClosed { .. }
        )
    {
        return;
    }
    match observation {
        NodeObservation::OperationSucceeded { operation, outcome } => {
            let Some(pending) = node.pending.as_ref() else {
                return;
            };
            if pending.id != operation || pending.kind != outcome.kind() {
                return;
            }
            node.pending = None;
            node.retry.next_effect_at = None;
            node.retry.ambiguous_operation = None;
            match outcome {
                OperationOutcome::LeaseCreated(result) => {
                    node.record.lease = Some(result.lease);
                    if let Some(endpoint) = result.endpoint {
                        node.record.connection = Some(endpoint);
                        node.record.stage = NodeStage::EndpointKnown;
                    } else {
                        node.record.stage = NodeStage::LeaseCreated;
                    }
                }
                OperationOutcome::EndpointLookup(Some(endpoint)) => {
                    node.record.connection = Some(endpoint);
                    node.record.stage = NodeStage::EndpointKnown;
                }
                OperationOutcome::EndpointLookup(None) => {
                    node.retry.next_effect_at = now.checked_add(retry.endpoint_probe_interval);
                }
                OperationOutcome::BootstrapStarted { session_id } => {
                    node.active_bootstrap = Some(session_id);
                    node.record.bootstrap = Some(BootstrapFacts {
                        session_id,
                        last_stage: BootstrapStage::Created,
                        last_stdout_seq: None,
                        last_stderr_seq: None,
                        last_observed_at: now,
                    });
                    node.record.stage = NodeStage::BootstrapRunning;
                }
                OperationOutcome::BootstrapConvergenceAccepted => {}
                OperationOutcome::BootstrapCancelled => {
                    if let Some(facts) = node.record.bootstrap.as_mut() {
                        facts.last_stage = BootstrapStage::Cancelled;
                        facts.last_observed_at = now;
                    }
                    node.active_bootstrap = None;
                }
                OperationOutcome::LeaseDestroyed => {
                    node.record.lease = None;
                    node.record.connection = None;
                }
            }
        }
        NodeObservation::OperationFailed { operation, error } => {
            let Some(pending) = node.pending.as_ref() else {
                return;
            };
            if pending.id != operation {
                return;
            }
            let kind = pending.kind;
            node.pending = None;
            record_failure(node, kind, error, now, retry);
        }
        NodeObservation::BootstrapObserved {
            session_id,
            observation,
        } => {
            if node.active_bootstrap != Some(session_id) {
                return;
            }
            let stage = observation.stage;
            let facts = node.record.bootstrap.get_or_insert(BootstrapFacts {
                session_id,
                last_stage: stage,
                last_stdout_seq: None,
                last_stderr_seq: None,
                last_observed_at: now,
            });
            facts.last_stage = stage;
            facts.last_observed_at = now;
            if observation.last_stdout_seq.is_some() {
                facts.last_stdout_seq = observation.last_stdout_seq;
            }
            if observation.last_stderr_seq.is_some() {
                facts.last_stderr_seq = observation.last_stderr_seq;
            }
            if is_bootstrap_failure(stage) {
                mark_attempt_failed(node, format!("bootstrap stage {stage:?}"), now, retry);
            } else if matches!(stage, BootstrapStage::Converged | BootstrapStage::Closed) {
                finish_bootstrap(node, now, retry);
            } else if stage == BootstrapStage::Cancelled {
                node.active_bootstrap = None;
            }
        }
        NodeObservation::BootstrapFailed { session_id, reason } => {
            if node.active_bootstrap == Some(session_id) {
                mark_attempt_failed(node, reason, now, retry);
            }
        }
        NodeObservation::SwactorJoined {
            session_id,
            swactor_id,
        } => {
            if node.active_bootstrap != Some(session_id) {
                return;
            }
            node.record.swactor = Some(SwactorFacts {
                swactor_id,
                joined_at: now,
                handed_off_at: None,
            });
            node.record.stage = NodeStage::SwactorJoined;
        }
        NodeObservation::BootstrapClosed { session_id } => {
            if node.active_bootstrap == Some(session_id) {
                finish_bootstrap(node, now, retry);
            }
        }
    }
}

fn observation_matches(node: &ManagedNode, observation: &NodeObservation) -> bool {
    match observation {
        NodeObservation::OperationSucceeded { operation, outcome } => node
            .pending
            .as_ref()
            .is_some_and(|pending| pending.id == *operation && pending.kind == outcome.kind()),
        NodeObservation::OperationFailed { operation, .. } => node
            .pending
            .as_ref()
            .is_some_and(|pending| pending.id == *operation),
        NodeObservation::BootstrapObserved { session_id, .. }
        | NodeObservation::BootstrapFailed { session_id, .. }
        | NodeObservation::SwactorJoined { session_id, .. }
        | NodeObservation::BootstrapClosed { session_id } => {
            node.record.stage != NodeStage::Failed && node.active_bootstrap == Some(*session_id)
        }
    }
}

fn record_failure(
    node: &mut ManagedNode,
    kind: OperationKind,
    error: EffectError,
    now: SystemTime,
    retry: &RetryPolicy,
) {
    let EffectError {
        reason,
        disposition,
    } = error;
    let ambiguous_operation = (disposition == EffectFailureDisposition::Ambiguous
        && matches!(
            kind,
            OperationKind::CreateLease | OperationKind::StartBootstrap
        ))
    .then_some(kind);
    node.retry.ambiguous_operation = ambiguous_operation;
    if kind == OperationKind::BootstrapConvergenceObserved
        || (kind == OperationKind::StartBootstrap
            && disposition == EffectFailureDisposition::Definite)
    {
        mark_attempt_failed(node, reason, now, retry);
        return;
    }
    node.retry.consecutive_failures = node.retry.consecutive_failures.saturating_add(1);
    node.retry.last_error = Some(reason);
    if node.intent == NodeIntent::Deleting
        && ambiguous_operation.is_none()
        && !matches!(
            kind,
            OperationKind::CancelBootstrap | OperationKind::DestroyLease
        )
    {
        node.retry.next_effect_at = None;
        return;
    }
    node.retry.next_effect_at = now.checked_add(sampled_retry_delay(
        retry,
        node.attempt,
        node.retry.consecutive_failures,
    ));
}

fn mark_attempt_failed(
    node: &mut ManagedNode,
    reason: String,
    now: SystemTime,
    retry: &RetryPolicy,
) {
    node.retry.consecutive_failures = node.retry.consecutive_failures.saturating_add(1);
    node.retry.last_error = Some(reason.clone());
    node.retry.next_effect_at = None;
    node.retry.ambiguous_operation = None;
    node.retry.restart_at = now.checked_add(sampled_retry_delay(
        retry,
        node.attempt,
        node.retry.consecutive_failures,
    ));
    node.record.stage = NodeStage::Failed;
    node.record.ready = false;
    node.record.failed_reason = Some(reason);
    node.record.failed_at = Some(now);
}

fn finish_bootstrap(node: &mut ManagedNode, now: SystemTime, retry: &RetryPolicy) {
    if node.record.swactor.is_none() {
        mark_attempt_failed(
            node,
            "bootstrap closed before swactor convergence".to_owned(),
            now,
            retry,
        );
        return;
    }
    if let Some(swactor) = node.record.swactor.as_mut() {
        swactor.handed_off_at = Some(now);
    }
    node.active_bootstrap = None;
    node.record.stage = NodeStage::Dormant;
    node.record.ready = node.intent == NodeIntent::Active;
    node.retry.consecutive_failures = 0;
    node.retry.next_effect_at = None;
    node.retry.restart_at = None;
    node.retry.last_error = None;
    node.retry.ambiguous_operation = None;
}

fn is_bootstrap_failure(stage: BootstrapStage) -> bool {
    matches!(
        stage,
        BootstrapStage::SshConnectFailed
            | BootstrapStage::BootCheckFailed
            | BootstrapStage::StartFailed
            | BootstrapStage::SwactorJoinFailed
            | BootstrapStage::StreamError
    )
}

fn earliest(left: Option<SystemTime>, right: Option<SystemTime>) -> Option<SystemTime> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DriverError {
    Shape(ShapeError),
    RunChanged { expected: RunId, supplied: RunId },
    GenerationRegressed { current: u64, supplied: u64 },
    ShapeChangedWithoutGeneration { generation: u64 },
    ReentrantPass,
    AttemptAllocatorMismatch,
    OperationSequenceExhausted,
}

impl fmt::Display for DriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shape(error) => error.fmt(formatter),
            Self::RunChanged { expected, supplied } => write!(
                formatter,
                "driver run changed from {} to {}",
                expected.0, supplied.0
            ),
            Self::GenerationRegressed { current, supplied } => write!(
                formatter,
                "desired generation regressed from {current} to {supplied}"
            ),
            Self::ShapeChangedWithoutGeneration { generation } => write!(
                formatter,
                "desired shape changed without advancing generation {generation}"
            ),
            Self::ReentrantPass => formatter.write_str("reentrant reconcile pass"),
            Self::AttemptAllocatorMismatch => formatter.write_str("attempt allocator mismatch"),
            Self::OperationSequenceExhausted => formatter.write_str("operation sequence exhausted"),
        }
    }
}

impl std::error::Error for DriverError {}

impl From<ShapeError> for DriverError {
    fn from(value: ShapeError) -> Self {
        Self::Shape(value)
    }
}

#[derive(Clone, Debug)]
pub struct ClusterDriver {
    state: ClusterState,
    desired: ClusterShape,
    expanded_desired: BTreeMap<LogicalNodeId, LogicalNodeSpec>,
    retry: RetryPolicy,
    queued: bool,
    processing: bool,
    dirty: bool,
    requeue_at: Option<SystemTime>,
}

impl ClusterDriver {
    pub fn new(desired: ClusterShape, retry: RetryPolicy) -> Result<Self, DriverError> {
        let expanded_desired = desired.expand()?;
        Ok(Self {
            state: ClusterState::default(),
            desired,
            expanded_desired,
            retry,
            queued: true,
            processing: false,
            dirty: false,
            requeue_at: None,
        })
    }

    pub fn state(&self) -> &ClusterState {
        &self.state
    }

    pub fn desired(&self) -> &ClusterShape {
        &self.desired
    }

    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry
    }

    pub fn is_queued(&self) -> bool {
        self.queued
    }

    pub fn requeue_at(&self) -> Option<SystemTime> {
        self.requeue_at
    }

    pub fn trigger(&mut self) {
        if self.processing {
            self.dirty = true;
        } else {
            self.queued = true;
        }
    }

    pub fn update_desired(&mut self, desired: ClusterShape) -> Result<(), DriverError> {
        if desired.run_id != self.desired.run_id {
            return Err(DriverError::RunChanged {
                expected: self.desired.run_id.clone(),
                supplied: desired.run_id,
            });
        }
        if desired.generation < self.desired.generation {
            return Err(DriverError::GenerationRegressed {
                current: self.desired.generation,
                supplied: desired.generation,
            });
        }
        let expanded = desired.expand()?;
        if desired.generation == self.desired.generation && desired != self.desired {
            return Err(DriverError::ShapeChangedWithoutGeneration {
                generation: desired.generation,
            });
        }
        self.desired = desired;
        self.expanded_desired = expanded;
        self.trigger();
        Ok(())
    }

    pub fn trigger_if_due(&mut self, now: SystemTime) -> bool {
        if self.requeue_at.is_some_and(|deadline| deadline <= now) {
            self.requeue_at = None;
            self.trigger();
            true
        } else {
            false
        }
    }

    pub fn pending_operations_due(&self, now: SystemTime) -> Vec<PlannedOperation> {
        self.state
            .nodes
            .iter()
            .filter_map(|(node, managed)| {
                let pending = managed.pending.as_ref()?;
                (pending.deadline <= now).then(|| PlannedOperation {
                    node: node.clone(),
                    operation: pending.id,
                    kind: pending.kind,
                    deadline: pending.deadline,
                })
            })
            .collect()
    }

    /// Folds a timeout only after the executor has stopped the operation or
    /// classified an ambiguous outcome according to its idempotency contract.
    pub fn operation_timed_out(
        &mut self,
        operation: &PlannedOperation,
        reason: impl Into<String>,
        now: SystemTime,
    ) -> bool {
        self.apply_observation(
            &operation.node,
            operation.operation.attempt,
            NodeObservation::OperationFailed {
                operation: operation.operation,
                error: EffectError::ambiguous(reason),
            },
            now,
        )
    }

    pub fn apply_executor_result(&mut self, result: ExecutorResult, now: SystemTime) -> bool {
        let observation = match result.result {
            Ok(outcome) => NodeObservation::OperationSucceeded {
                operation: result.operation,
                outcome,
            },
            Err(error) => NodeObservation::OperationFailed {
                operation: result.operation,
                error,
            },
        };
        self.apply_observation(&result.node, result.operation.attempt, observation, now)
    }
    pub fn apply_observation(
        &mut self,
        node: &LogicalNodeId,
        attempt: NodeAttemptId,
        observation: NodeObservation,
        now: SystemTime,
    ) -> bool {
        let Some(managed) = self.state.nodes.get_mut(node) else {
            return false;
        };
        if managed.attempt != attempt || !observation_matches(managed, &observation) {
            return false;
        }
        observe(managed, observation, now, &self.retry);
        self.trigger();
        true
    }

    pub fn submission_failed(
        &mut self,
        effect: &PlannedEffect,
        reason: impl Into<String>,
        now: SystemTime,
    ) -> bool {
        self.apply_observation(
            &effect.node,
            effect.operation.attempt,
            NodeObservation::OperationFailed {
                operation: effect.operation,
                error: EffectError::definite(reason),
            },
            now,
        )
    }

    pub fn drive_next<E>(&mut self, now: SystemTime, executor: &mut E) -> Result<usize, DriverError>
    where
        E: EffectExecutor,
    {
        if self.processing {
            return Err(DriverError::ReentrantPass);
        }
        if !self.queued {
            return Ok(0);
        }

        self.queued = false;
        self.processing = true;
        let pass = self.run_pass(now);
        let result = match pass {
            Ok(effects) => {
                let mut submitted = 0_usize;
                for effect in effects {
                    match executor.submit(&effect) {
                        Ok(()) => submitted = submitted.saturating_add(1),
                        Err(error) => {
                            self.submission_failed(
                                &effect,
                                format!("executor submission failed: {error}"),
                                now,
                            );
                        }
                    }
                }
                Ok(submitted)
            }
            Err(error) => Err(error),
        };
        self.processing = false;
        if self.dirty {
            self.dirty = false;
            self.queued = true;
        }
        result
    }

    pub fn drive_until_blocked<E>(
        &mut self,
        now: SystemTime,
        executor: &mut E,
    ) -> Result<usize, DriverError>
    where
        E: EffectExecutor,
    {
        let mut submitted = 0_usize;
        while self.queued {
            submitted = submitted.saturating_add(self.drive_next(now, executor)?);
        }
        Ok(submitted)
    }

    pub fn is_converged(&self) -> bool {
        if self.state.observed_generation != self.desired.generation
            || self.state.nodes.len() != self.expanded_desired.len()
        {
            return false;
        }
        self.expanded_desired.iter().all(|(id, desired)| {
            self.state.nodes.get(id).is_some_and(|node| {
                node.intent == NodeIntent::Active
                    && node.record.desired == *desired
                    && node.record.ready
                    && node.pending.is_none()
            })
        })
    }

    fn run_pass(&mut self, now: SystemTime) -> Result<Vec<PlannedEffect>, DriverError> {
        let plan = reconcile(&self.state, &self.desired, now)?;
        let mut effects = Vec::new();
        self.requeue_at = plan.requeue_at;

        let mut allocated_actions_valid = true;
        for action in plan.actions {
            if let Some(allocated_attempt) = allocated_attempt(&action)
                && (!allocated_actions_valid || allocated_attempt.0 != self.state.next_attempt_id)
            {
                allocated_actions_valid = false;
                self.dirty = true;
                continue;
            }
            if let Some(effect) = self.apply_action(action, now)? {
                effects.push(effect);
            }
        }
        self.state.observed_generation = plan.observed_generation;
        Ok(effects)
    }

    fn apply_action(
        &mut self,
        action: NodeAction,
        now: SystemTime,
    ) -> Result<Option<PlannedEffect>, DriverError> {
        match action {
            NodeAction::Insert { attempt, desired } => {
                if attempt.0 != self.state.next_attempt_id
                    || self.state.nodes.contains_key(&desired.logical_node_id)
                {
                    self.dirty = true;
                    return Ok(None);
                }
                self.state.next_attempt_id = self
                    .state
                    .next_attempt_id
                    .checked_add(1)
                    .ok_or(DriverError::AttemptAllocatorMismatch)?;
                self.state.nodes.insert(
                    desired.logical_node_id.clone(),
                    ManagedNode::new(attempt, desired),
                );
                self.dirty = true;
                Ok(None)
            }
            NodeAction::BeginDelete {
                node,
                expected_attempt,
            } => {
                let Some(managed) = self.current_node_mut(&node, expected_attempt) else {
                    self.dirty = true;
                    return Ok(None);
                };
                managed.intent = NodeIntent::Deleting;
                managed.record.ready = false;
                if managed.retry.ambiguous_operation.is_none() {
                    managed.retry.next_effect_at = None;
                }
                self.dirty = true;
                Ok(None)
            }
            NodeAction::MarkDestroyed {
                node,
                expected_attempt,
            } => {
                let Some(managed) = self.current_node_mut(&node, expected_attempt) else {
                    self.dirty = true;
                    return Ok(None);
                };
                if managed.pending.is_some()
                    || managed.active_bootstrap.is_some()
                    || managed.record.lease.is_some()
                {
                    self.dirty = true;
                    return Ok(None);
                }
                managed.record.stage = NodeStage::Destroyed;
                managed.record.ready = false;
                managed.record.destroyed_at = Some(now);
                self.dirty = true;
                Ok(None)
            }
            NodeAction::Restart {
                node,
                expected_attempt,
                new_attempt,
                desired,
            } => {
                if new_attempt.0 != self.state.next_attempt_id {
                    self.dirty = true;
                    return Ok(None);
                }
                let Some(old) = self.state.nodes.get(&node) else {
                    self.dirty = true;
                    return Ok(None);
                };
                if old.attempt != expected_attempt || old.record.stage != NodeStage::Destroyed {
                    self.dirty = true;
                    return Ok(None);
                }
                let failures = old.retry.consecutive_failures;
                self.state.next_attempt_id = self
                    .state
                    .next_attempt_id
                    .checked_add(1)
                    .ok_or(DriverError::AttemptAllocatorMismatch)?;
                let mut replacement = ManagedNode::new(new_attempt, desired);
                replacement.retry.consecutive_failures = failures;
                self.state.nodes.insert(node, replacement);
                self.dirty = true;
                Ok(None)
            }
            NodeAction::Reap {
                node,
                expected_attempt,
            } => {
                if self.state.nodes.get(&node).is_some_and(|managed| {
                    managed.attempt == expected_attempt
                        && managed.record.stage == NodeStage::Destroyed
                }) {
                    self.state.nodes.remove(&node);
                } else {
                    self.dirty = true;
                }
                Ok(None)
            }
            NodeAction::Dispatch(effect) => {
                let timeout = self.retry.operation_timeout;
                let Some(managed) = self.current_node_mut(&effect.node, effect.operation.attempt)
                else {
                    self.dirty = true;
                    return Ok(None);
                };
                if managed.pending.is_some()
                    || managed.next_operation_sequence != effect.operation.sequence
                {
                    self.dirty = true;
                    return Ok(None);
                }
                let deadline = now.checked_add(timeout).unwrap_or(now);
                let next_sequence = managed
                    .next_operation_sequence
                    .checked_add(1)
                    .ok_or(DriverError::OperationSequenceExhausted)?;
                managed.pending = Some(PendingOperation {
                    id: effect.operation,
                    kind: OperationKind::for_command(&effect.command),
                    deadline,
                });
                managed.next_operation_sequence = next_sequence;
                if matches!(effect.command, NodeManagerCommand::CreateLease(_)) {
                    managed.record.stage = NodeStage::LeaseRequested;
                }
                self.requeue_at = earliest(self.requeue_at, Some(deadline));
                Ok(Some(effect))
            }
        }
    }

    fn current_node_mut(
        &mut self,
        id: &LogicalNodeId,
        attempt: NodeAttemptId,
    ) -> Option<&mut ManagedNode> {
        self.state
            .nodes
            .get_mut(id)
            .filter(|node| node.attempt == attempt)
    }
}

fn allocated_attempt(action: &NodeAction) -> Option<NodeAttemptId> {
    match action {
        NodeAction::Insert { attempt, .. } => Some(*attempt),
        NodeAction::Restart { new_attempt, .. } => Some(*new_attempt),
        _ => None,
    }
}
