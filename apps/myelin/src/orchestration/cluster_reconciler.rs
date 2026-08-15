//! Myelin integration for the provider-neutral cluster reconciler.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, SystemTime};

use provisioning::{
    BlockingEffectSpawner, BootstrapSessionId, ClusterDriver, ClusterShape, CreateLeaseResult,
    DestroyHandle, DriverError, EffectBackend, EffectError, ExecutorOperationStatus,
    IdempotentEffectExecutor, LeaseFacts, LogicalNodeId, NodeAttemptId, NodeIntent,
    NodeManagerCommand, NodeObservation, NodeStage, OperationOutcome, PlannedEffect,
    ProviderLeaseId, RetryPolicy, SshEndpoint, SwactorId,
};
use swactor_engine::EngineHandle;

use crate::provisioning::{
    NodeProvisionSpec, PluginNodeHandle, PluginObservation, PluginObservationSink, PluginSink,
    ProvisionPlugin,
};

const PERIODIC_RECONCILE: Duration = Duration::from_secs(30);
const ATTEMPT_ENV: &str = "MYELIN_NODE_ATTEMPT_ID";

pub(crate) struct ReconcilerNodeBinding {
    pub logical_node_id: LogicalNodeId,
    pub provision: NodeProvisionSpec,
    pub plugin: Box<dyn ProvisionPlugin>,
}

struct LiveNode {
    attempt: NodeAttemptId,
    handle: PluginNodeHandle,
    lease: LeaseFacts,
    endpoint: SshEndpoint,
    failure_sink: Arc<AttemptObservationSink>,
    bootstrap_started: bool,
}

struct StagedNodeEffects {
    template: NodeProvisionSpec,
    plugin: Box<dyn ProvisionPlugin>,
}

struct NodeEffects {
    template: NodeProvisionSpec,
    plugin: Box<dyn ProvisionPlugin>,
    live: Option<LiveNode>,
    staged: Option<StagedNodeEffects>,
}

#[derive(Clone, Debug)]
struct TaggedFailure {
    node: LogicalNodeId,
    attempt: NodeAttemptId,
    reason: String,
}

enum FailureGate {
    Pending(Vec<String>),
    Armed,
    Discarded,
}

struct AttemptObservationSink {
    downstream: PluginSink,
    failures: Sender<TaggedFailure>,
    node: LogicalNodeId,
    attempt: NodeAttemptId,
    gate: Mutex<FailureGate>,
}

impl AttemptObservationSink {
    fn arm(&self) {
        let mut gate = self
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let FailureGate::Pending(reasons) = std::mem::replace(&mut *gate, FailureGate::Armed)
        else {
            return;
        };
        for reason in reasons {
            self.send_failure(reason);
        }
    }

    fn discard(&self) {
        *self
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = FailureGate::Discarded;
    }

    fn record_failure(&self, reason: String) {
        let mut gate = self
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *gate {
            FailureGate::Pending(reasons) => reasons.push(reason),
            FailureGate::Armed => self.send_failure(reason),
            FailureGate::Discarded => {}
        }
    }

    fn send_failure(&self, reason: String) {
        let _ = self.failures.send(TaggedFailure {
            node: self.node.clone(),
            attempt: self.attempt,
            reason,
        });
    }
}

impl PluginObservationSink for AttemptObservationSink {
    fn observe(&self, observation: PluginObservation) {
        let reason = match &observation {
            PluginObservation::Failed { reason, .. } => Some(reason.clone()),
            PluginObservation::Exited {
                node_id, status, ..
            } => Some(format!(
                "node {node_id} exited during bootstrap: {status:?}"
            )),
            PluginObservation::StdoutLine { .. }
            | PluginObservation::StderrLine { .. }
            | PluginObservation::TelemetryFrame { .. }
            | PluginObservation::ProviderLine { .. } => None,
        };
        self.downstream.observe(observation);
        if let Some(reason) = reason {
            self.record_failure(reason);
        }
    }
}

pub(crate) struct MyelinEffectBackend {
    nodes: RwLock<BTreeMap<LogicalNodeId, Arc<Mutex<NodeEffects>>>>,
    sink: PluginSink,
    failure_tx: Sender<TaggedFailure>,
}

impl MyelinEffectBackend {
    fn new(
        bindings: Vec<ReconcilerNodeBinding>,
        sink: PluginSink,
        failure_tx: Sender<TaggedFailure>,
    ) -> Result<(Self, BTreeMap<u64, LogicalNodeId>), String> {
        let mut nodes = BTreeMap::new();
        let mut by_external_id = BTreeMap::new();
        for binding in bindings {
            if nodes.contains_key(&binding.logical_node_id) {
                return Err(format!(
                    "duplicate reconciler binding {}",
                    binding.logical_node_id.0
                ));
            }
            if by_external_id
                .insert(binding.provision.node_id, binding.logical_node_id.clone())
                .is_some()
            {
                return Err(format!(
                    "duplicate provision node id {}",
                    binding.provision.node_id
                ));
            }
            nodes.insert(
                binding.logical_node_id,
                Arc::new(Mutex::new(NodeEffects {
                    template: binding.provision,
                    plugin: binding.plugin,
                    live: None,
                    staged: None,
                })),
            );
        }
        Ok((
            Self {
                nodes: RwLock::new(nodes),
                sink,
                failure_tx,
            },
            by_external_id,
        ))
    }

    fn node(&self, id: &LogicalNodeId) -> Result<Arc<Mutex<NodeEffects>>, EffectError> {
        lock_nodes_read(&self.nodes)
            .get(id)
            .cloned()
            .ok_or_else(|| EffectError::definite(format!("no effect binding for node {}", id.0)))
    }

    fn has_node(&self, id: &LogicalNodeId) -> bool {
        lock_nodes_read(&self.nodes).contains_key(id)
    }

    fn register_binding(&self, binding: ReconcilerNodeBinding) -> (u64, LogicalNodeId) {
        let external_id = binding.provision.node_id;
        let logical_id = binding.logical_node_id;
        let staged = StagedNodeEffects {
            template: binding.provision,
            plugin: binding.plugin,
        };
        let mut nodes = lock_nodes_write(&self.nodes);
        if let Some(effects) = nodes.get(&logical_id) {
            lock_node(effects).staged = Some(staged);
        } else {
            nodes.insert(
                logical_id.clone(),
                Arc::new(Mutex::new(NodeEffects {
                    template: staged.template,
                    plugin: staged.plugin,
                    live: None,
                    staged: None,
                })),
            );
        }
        (external_id, logical_id)
    }

    fn stop_all(&self) -> Result<(), String> {
        let mut first_error = None;
        let nodes = lock_nodes_read(&self.nodes)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for effects in nodes {
            let mut effects = lock_node(&effects);
            let Some(live) = effects.live.take() else {
                continue;
            };
            live.failure_sink.discard();
            if let Err(error) = effects.plugin.stop_node(&live.handle) {
                effects.live = Some(live);
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn create_or_adopt(
        &self,
        effect: &PlannedEffect,
        request: &provisioning::CreateLeaseRequest,
    ) -> Result<OperationOutcome, EffectError> {
        let effects = self.node(&effect.node)?;
        let mut effects = lock_node(&effects);
        if let Some(live) = &effects.live {
            if live.attempt == effect.operation.attempt {
                return Ok(OperationOutcome::LeaseCreated(CreateLeaseResult {
                    lease: live.lease.clone(),
                    endpoint: Some(live.endpoint.clone()),
                }));
            }
            return Err(EffectError::ambiguous(format!(
                "node {} still owns attempt {} while creating attempt {}",
                effect.node.0, live.attempt.0, effect.operation.attempt.0
            )));
        }

        if let Some(staged) = effects.staged.take() {
            effects.template = staged.template;
            effects.plugin = staged.plugin;
        }

        let mut spec = effects.template.clone();
        spec.run_id = request.spec.run_id.0;
        spec.attempt_id = effect.operation.attempt.0;
        spec.image = request.spec.shape.image.clone();
        spec.env = request.spec.boot.env.clone();
        spec.args = request.spec.boot.args.clone();
        spec.mounts = request.spec.boot.mounts.clone();
        spec.env.retain(|(name, _)| name != ATTEMPT_ENV);
        spec.env
            .push((ATTEMPT_ENV.to_owned(), spec.attempt_id.to_string()));
        let failure_sink = Arc::new(AttemptObservationSink {
            downstream: self.sink.clone(),
            failures: self.failure_tx.clone(),
            node: effect.node.clone(),
            attempt: effect.operation.attempt,
            gate: Mutex::new(FailureGate::Pending(Vec::new())),
        });
        let attempt_sink = PluginSink::new(failure_sink.clone());
        let handle = match effects.plugin.create_node(spec, attempt_sink) {
            Ok(handle) => handle,
            Err(error) => {
                failure_sink.discard();
                return Err(EffectError::ambiguous(error));
            }
        };

        let provider = request.spec.provider.clone();
        let lease_id = ProviderLeaseId(format!(
            "run-{}-node-{}-attempt-{}",
            request.spec.run_id.0, effect.node.0, effect.operation.attempt.0
        ));
        let provider_contract_id = lease_id.0.clone();
        let lease = LeaseFacts {
            provider: provider.clone(),
            lease_id: lease_id.clone(),
            provider_contract_id: provider_contract_id.clone(),
            offer_id: None,
            destroy_handle: DestroyHandle {
                provider,
                lease_id,
                provider_contract_id,
            },
            provider_metadata: BTreeMap::from([
                ("logical_node_id".to_owned(), effect.node.0.clone()),
                ("attempt".to_owned(), effect.operation.attempt.0.to_string()),
            ]),
        };
        let endpoint = SshEndpoint {
            host: "managed-by-myelin-plugin".to_owned(),
            port: 0,
            user: request.spec.boot.ssh_user.clone(),
            auth_ref: "managed-by-myelin-plugin".to_owned(),
        };
        effects.live = Some(LiveNode {
            attempt: effect.operation.attempt,
            handle,
            lease: lease.clone(),
            endpoint: endpoint.clone(),
            failure_sink: Arc::clone(&failure_sink),
            bootstrap_started: false,
        });
        failure_sink.arm();
        Ok(OperationOutcome::LeaseCreated(CreateLeaseResult {
            lease,
            endpoint: Some(endpoint),
        }))
    }
}

impl EffectBackend for MyelinEffectBackend {
    fn execute(&self, effect: &PlannedEffect) -> Result<OperationOutcome, EffectError> {
        match &effect.command {
            NodeManagerCommand::CreateLease(request) => self.create_or_adopt(effect, request),
            NodeManagerCommand::LookupEndpoint(_) => {
                let node = self.node(&effect.node)?;
                let effects = lock_node(&node);
                let endpoint = effects
                    .live
                    .as_ref()
                    .filter(|live| live.attempt == effect.operation.attempt)
                    .map(|live| live.endpoint.clone());
                Ok(OperationOutcome::EndpointLookup(endpoint))
            }
            NodeManagerCommand::StartBootstrap(_) => {
                let effects = self.node(&effect.node)?;
                let mut effects = lock_node(&effects);
                let (handle, bootstrap_started) = {
                    let live = effects.live.as_ref().ok_or_else(|| {
                        EffectError::definite(format!("node {} has no live lease", effect.node.0))
                    })?;
                    if live.attempt != effect.operation.attempt {
                        return Err(EffectError::definite(
                            "bootstrap attempt does not own lease",
                        ));
                    }
                    (live.handle.clone(), live.bootstrap_started)
                };
                if !bootstrap_started {
                    effects
                        .plugin
                        .start_bootstrap(&handle)
                        .map_err(EffectError::definite)?;
                    let live = effects
                        .live
                        .as_mut()
                        .expect("live lease retained while bootstrap starts");
                    live.bootstrap_started = true;
                }
                Ok(OperationOutcome::BootstrapStarted {
                    session_id: BootstrapSessionId(effect.operation.attempt.0),
                })
            }
            NodeManagerCommand::BootstrapConvergenceObserved { session_id, .. } => {
                if *session_id != BootstrapSessionId(effect.operation.attempt.0) {
                    return Err(EffectError::definite("bootstrap session/attempt mismatch"));
                }
                let effects = self.node(&effect.node)?;
                let mut effects = lock_node(&effects);
                let handle = effects
                    .live
                    .as_ref()
                    .filter(|live| live.attempt == effect.operation.attempt)
                    .map(|live| live.handle.clone())
                    .ok_or_else(|| EffectError::definite("bootstrap lease is absent"))?;
                effects
                    .plugin
                    .complete_bootstrap(&handle)
                    .map_err(EffectError::definite)?;
                if let Some(live) = effects.live.as_ref() {
                    live.failure_sink.discard();
                }
                Ok(OperationOutcome::BootstrapConvergenceAccepted)
            }
            NodeManagerCommand::CancelBootstrap { session_id } => {
                if *session_id != BootstrapSessionId(effect.operation.attempt.0) {
                    return Err(EffectError::definite("bootstrap session/attempt mismatch"));
                }
                let effects = self.node(&effect.node)?;
                let mut effects = lock_node(&effects);
                if let Some(handle) = effects
                    .live
                    .as_ref()
                    .filter(|live| live.attempt == effect.operation.attempt)
                    .map(|live| live.handle.clone())
                {
                    effects
                        .plugin
                        .cancel_bootstrap(&handle)
                        .map_err(EffectError::definite)?;
                    if let Some(live) = effects.live.as_ref() {
                        live.failure_sink.discard();
                    }
                }
                Ok(OperationOutcome::BootstrapCancelled)
            }
            NodeManagerCommand::DestroyLease(_) => {
                let effects = self.node(&effect.node)?;
                let mut effects = lock_node(&effects);
                let Some(live) = effects.live.take() else {
                    return Ok(OperationOutcome::LeaseDestroyed);
                };
                if live.attempt != effect.operation.attempt {
                    effects.live = Some(live);
                    return Err(EffectError::definite("destroy attempt does not own lease"));
                }
                live.failure_sink.discard();
                if let Err(error) = effects.plugin.stop_node(&live.handle) {
                    effects.live = Some(live);
                    return Err(EffectError::definite(error));
                }
                Ok(OperationOutcome::LeaseDestroyed)
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct EngineEffectSpawner {
    engine: EngineHandle,
}

impl EngineEffectSpawner {
    fn new(engine: EngineHandle) -> Self {
        Self { engine }
    }
}

impl BlockingEffectSpawner for EngineEffectSpawner {
    type SpawnError = String;

    fn spawn_blocking(
        &self,
        work: provisioning::BlockingEffectWork,
    ) -> Result<(), Self::SpawnError> {
        if !self.engine.capabilities().blocking {
            return Err("engine blocking work capability is unavailable".to_owned());
        }
        self.engine.spawn_blocking(work);
        Ok(())
    }
}

enum ControllerWake {
    Deadline(SystemTime),
    Periodic,
}

pub(crate) struct ProvisionedClusterGuard {
    driver: ClusterDriver,
    executor: IdempotentEffectExecutor<MyelinEffectBackend, EngineEffectSpawner>,
    external_nodes: BTreeMap<u64, LogicalNodeId>,
    failure_rx: Receiver<TaggedFailure>,
    deferred_failures: VecDeque<TaggedFailure>,
    engine: EngineHandle,
    wake_tx: Sender<ControllerWake>,
    wake_rx: Receiver<ControllerWake>,
    scheduled_deadline: Option<SystemTime>,
    stopped: bool,
}

impl ProvisionedClusterGuard {
    pub(crate) fn new(
        desired: ClusterShape,
        bindings: Vec<ReconcilerNodeBinding>,
        retry: RetryPolicy,
        engine: EngineHandle,
        sink: PluginSink,
    ) -> Result<Self, String> {
        let expanded = desired.expand().map_err(|error| error.to_string())?;
        let binding_ids = bindings
            .iter()
            .map(|binding| binding.logical_node_id.clone())
            .collect::<BTreeSet<_>>();
        for logical_id in expanded.keys() {
            if !binding_ids.contains(logical_id) {
                return Err(format!(
                    "no effect binding for desired node {}",
                    logical_id.0
                ));
            }
        }
        for logical_id in &binding_ids {
            if !expanded.contains_key(logical_id) {
                return Err(format!(
                    "binding {} is absent from desired shape",
                    logical_id.0
                ));
            }
        }
        let (failure_tx, failure_rx) = mpsc::channel();
        let (backend, external_nodes) = MyelinEffectBackend::new(bindings, sink, failure_tx)?;
        let executor =
            IdempotentEffectExecutor::new(backend, EngineEffectSpawner::new(engine.clone()));
        let driver = ClusterDriver::new(desired, retry).map_err(|error| error.to_string())?;
        let (wake_tx, wake_rx) = mpsc::channel();
        spawn_periodic_wake(&engine, wake_tx.clone());
        Ok(Self {
            driver,
            executor,
            external_nodes,
            failure_rx,
            deferred_failures: VecDeque::new(),
            engine,
            wake_tx,
            wake_rx,
            scheduled_deadline: None,
            stopped: false,
        })
    }

    pub(crate) fn update_desired(
        &mut self,
        desired: ClusterShape,
        bindings: Vec<ReconcilerNodeBinding>,
    ) -> Result<(), String> {
        let expanded = desired.expand().map_err(|error| error.to_string())?;
        let mut new_external_ids = BTreeSet::new();
        let mut new_logical_ids = BTreeSet::new();
        for binding in &bindings {
            if !expanded.contains_key(&binding.logical_node_id) {
                return Err(format!(
                    "binding {} is absent from desired shape",
                    binding.logical_node_id.0
                ));
            }
            if self
                .external_nodes
                .get(&binding.provision.node_id)
                .is_some_and(|logical_id| logical_id != &binding.logical_node_id)
                || !new_external_ids.insert(binding.provision.node_id)
            {
                return Err(format!(
                    "duplicate provision node id {}",
                    binding.provision.node_id
                ));
            }
            if !new_logical_ids.insert(binding.logical_node_id.clone()) {
                return Err(format!(
                    "duplicate reconciler binding {}",
                    binding.logical_node_id.0
                ));
            }
        }
        for (logical_id, desired_node) in &expanded {
            let needs_binding = match self.driver.state().nodes.get(logical_id) {
                Some(current) => current.record.desired != *desired_node,
                None => true,
            } || !self.executor.backend().has_node(logical_id);
            if needs_binding && !new_logical_ids.contains(logical_id) {
                return Err(format!(
                    "desired node {} requires a replacement effect binding",
                    logical_id.0
                ));
            }
        }
        self.driver
            .update_desired(desired)
            .map_err(|error| error.to_string())?;
        for binding in bindings {
            let (external_id, logical_id) = self.executor.backend().register_binding(binding);
            self.external_nodes
                .retain(|_, existing| existing != &logical_id);
            self.external_nodes.insert(external_id, logical_id);
        }
        Ok(())
    }

    pub(crate) fn is_converged(&self) -> bool {
        self.driver.is_converged()
    }

    pub(crate) fn current_attempt(&self, external_node_id: u64) -> Option<NodeAttemptId> {
        let logical = self.external_nodes.get(&external_node_id)?;
        self.driver
            .state()
            .nodes
            .get(logical)
            .map(|node| node.attempt)
    }

    pub(crate) fn awaiting_runtime(&self) -> bool {
        !self.driver.state().nodes.is_empty()
            && self.driver.state().nodes.values().all(|node| {
                (node.intent == NodeIntent::Active && node.record.ready && node.pending.is_none())
                    || (node.intent == NodeIntent::Active
                        && node.record.stage == NodeStage::BootstrapRunning
                        && node.active_bootstrap.is_some()
                        && node.pending.is_none())
            })
    }

    pub(crate) fn observe_runtime_ready(
        &mut self,
        external_node_id: u64,
        attempt: NodeAttemptId,
        swactor_id: SwactorId,
        now: SystemTime,
    ) -> bool {
        let Some(logical) = self.external_nodes.get(&external_node_id) else {
            return false;
        };
        self.driver.apply_observation(
            logical,
            attempt,
            NodeObservation::SwactorJoined {
                session_id: BootstrapSessionId(attempt.0),
                swactor_id,
            },
            now,
        )
    }

    pub(crate) fn poll(&mut self, now: SystemTime) -> Result<usize, DriverError> {
        self.drain_wakes(now);
        self.drain_executor_results(now);
        self.drain_failures(now);
        if self.classify_due_operations(now) {
            self.drain_executor_results(now);
        }
        self.driver.trigger_if_due(now);
        let submitted = self.driver.drive_until_blocked(now, &mut self.executor)?;
        self.schedule_deadline(now);
        Ok(submitted)
    }

    fn begin_shutdown(&mut self) -> Result<(), String> {
        if self.stopped {
            return Ok(());
        }
        let desired = ClusterShape {
            run_id: self.driver.desired().run_id.clone(),
            generation: self.driver.desired().generation.saturating_add(1),
            groups: Vec::new(),
        };
        self.update_desired(desired, Vec::new())
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.driver.state().nodes.is_empty()
    }

    // Synchronous orchestration waits while all provider work remains engine-hosted.
    #[allow(clippy::disallowed_methods)]
    pub(crate) fn stop(&mut self) -> Result<(), String> {
        self.begin_shutdown()?;
        while !self.is_stopped() {
            self.poll(SystemTime::now())
                .map_err(|error| error.to_string())?;
            std::thread::sleep(Duration::from_millis(10));
        }
        self.executor.backend().stop_all()?;
        self.stopped = true;
        Ok(())
    }

    fn drain_wakes(&mut self, now: SystemTime) {
        loop {
            match self.wake_rx.try_recv() {
                Ok(ControllerWake::Periodic) => self.driver.trigger(),
                Ok(ControllerWake::Deadline(deadline)) => {
                    if self.scheduled_deadline == Some(deadline) {
                        self.scheduled_deadline = None;
                    }
                    self.driver.trigger_if_due(now);
                }
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
            }
        }
    }

    fn drain_executor_results(&mut self, now: SystemTime) {
        for result in self.executor.drain_results() {
            let close = matches!(
                result.result,
                Ok(OperationOutcome::BootstrapConvergenceAccepted)
            )
            .then_some((result.node.clone(), result.operation.attempt));
            if self.driver.apply_executor_result(result, now)
                && let Some((node, attempt)) = close
            {
                self.driver.apply_observation(
                    &node,
                    attempt,
                    NodeObservation::BootstrapClosed {
                        session_id: BootstrapSessionId(attempt.0),
                    },
                    now,
                );
            }
        }
    }

    fn drain_failures(&mut self, now: SystemTime) {
        while let Ok(failure) = self.failure_rx.try_recv() {
            self.deferred_failures.push_back(failure);
        }
        let mut remaining = VecDeque::new();
        while let Some(failure) = self.deferred_failures.pop_front() {
            let Some(node) = self.driver.state().nodes.get(&failure.node) else {
                continue;
            };
            if node.attempt != failure.attempt || node.intent == NodeIntent::Deleting {
                continue;
            }
            let Some(session_id) = node.active_bootstrap else {
                if node.pending.is_some()
                    || matches!(
                        node.record.stage,
                        NodeStage::LeaseCreated
                            | NodeStage::EndpointKnown
                            | NodeStage::BootstrapRunning
                    )
                {
                    remaining.push_back(failure);
                }
                continue;
            };
            self.driver.apply_observation(
                &failure.node,
                failure.attempt,
                NodeObservation::BootstrapFailed {
                    session_id,
                    reason: failure.reason,
                },
                now,
            );
        }
        self.deferred_failures = remaining;
    }

    fn classify_due_operations(&mut self, now: SystemTime) -> bool {
        let mut expired = false;
        for operation in self.driver.pending_operations_due(now) {
            match self.executor.operation_status(operation.operation) {
                ExecutorOperationStatus::Unknown => {
                    self.driver.operation_timed_out(
                        &operation,
                        "executor lost pending operation",
                        now,
                    );
                }
                ExecutorOperationStatus::InFlight => {
                    expired |= self.executor.expire(
                        operation.operation,
                        "executor operation timed out with an ambiguous outcome",
                    );
                }
                ExecutorOperationStatus::Completed => {}
            }
        }
        expired
    }

    fn schedule_deadline(&mut self, now: SystemTime) {
        let deadline = self.driver.requeue_at();
        if deadline.is_some_and(|deadline| deadline <= now)
            && self
                .driver
                .pending_operations_due(now)
                .iter()
                .any(|operation| {
                    self.executor.operation_status(operation.operation)
                        == ExecutorOperationStatus::InFlight
                })
        {
            self.scheduled_deadline = None;
            return;
        }
        if deadline.is_none() || deadline == self.scheduled_deadline {
            return;
        }
        let deadline = deadline.expect("checked deadline");
        self.scheduled_deadline = Some(deadline);
        let delay = deadline.duration_since(now).unwrap_or(Duration::ZERO);
        let timer = self.engine.timer(delay);
        let wake = self.wake_tx.clone();
        self.engine.spawn(async move {
            timer.await;
            let _ = wake.send(ControllerWake::Deadline(deadline));
        });
    }
}

impl Drop for ProvisionedClusterGuard {
    fn drop(&mut self) {
        if !self.stopped {
            let _ = self.executor.backend().stop_all();
        }
    }
}

fn spawn_periodic_wake(engine: &EngineHandle, wake: Sender<ControllerWake>) {
    let mut interval = engine.interval(PERIODIC_RECONCILE);
    engine.spawn(async move {
        loop {
            (&mut interval).await;
            if wake.send(ControllerWake::Periodic).is_err() {
                return;
            }
        }
    });
}

fn lock_node(node: &Mutex<NodeEffects>) -> MutexGuard<'_, NodeEffects> {
    node.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_nodes_read(
    nodes: &RwLock<BTreeMap<LogicalNodeId, Arc<Mutex<NodeEffects>>>>,
) -> RwLockReadGuard<'_, BTreeMap<LogicalNodeId, Arc<Mutex<NodeEffects>>>> {
    nodes
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_nodes_write(
    nodes: &RwLock<BTreeMap<LogicalNodeId, Arc<Mutex<NodeEffects>>>>,
) -> RwLockWriteGuard<'_, BTreeMap<LogicalNodeId, Arc<Mutex<NodeEffects>>>> {
    nodes
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::provisioning::ProviderMount;
    use ::provisioning::{
        BootSpec, BootstrapSessionSpec, ClusterShape, CreateLeaseRequest, TelemetryStreamId,
        DesiredNodeShape, LogicalNodeSpec, NodeGroupId, OperationId, ProviderKind, RetryPolicy,
        RoleId, RunId, RunNodeGroupSpec, SwarmJoinSpec, SwarmJoinTemplate,
    };

    use super::*;

    #[derive(Default)]
    struct NullSink;

    impl PluginObservationSink for NullSink {
        fn observe(&self, _observation: PluginObservation) {}
    }

    #[test]
    fn attempt_failures_are_published_only_while_the_start_is_live() {
        let (failure_tx, failure_rx) = mpsc::channel();
        let sink = AttemptObservationSink {
            downstream: PluginSink::new(Arc::new(NullSink)),
            failures: failure_tx,
            node: LogicalNodeId("node-7-0".to_owned()),
            attempt: NodeAttemptId(9),
            gate: Mutex::new(FailureGate::Pending(Vec::new())),
        };
        let failed = |reason: &str| PluginObservation::Failed {
            run_id: 5,
            node_id: 7,
            reason: reason.to_owned(),
        };

        sink.observe(failed("before start returned"));
        assert!(matches!(failure_rx.try_recv(), Err(TryRecvError::Empty)));
        sink.arm();
        assert_eq!(failure_rx.recv().unwrap().reason, "before start returned");
        sink.observe(failed("while live"));
        assert_eq!(failure_rx.recv().unwrap().reason, "while live");
        sink.discard();
        sink.observe(failed("after cleanup"));
        assert!(matches!(failure_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[derive(Default)]
    struct PluginStats {
        creates: AtomicUsize,
        starts: AtomicUsize,
        start_failures: AtomicUsize,
        completes: AtomicUsize,
        cancels: AtomicUsize,
        stops: AtomicUsize,
        stop_failures: AtomicUsize,
        specs: Mutex<Vec<NodeProvisionSpec>>,
    }

    struct FakePlugin {
        stats: Arc<PluginStats>,
    }

    impl ProvisionPlugin for FakePlugin {
        fn create_node(
            &mut self,
            spec: NodeProvisionSpec,
            sink: PluginSink,
        ) -> Result<PluginNodeHandle, String> {
            self.stats.creates.fetch_add(1, Ordering::SeqCst);
            self.stats.specs.lock().unwrap().push(spec.clone());
            sink.observe(PluginObservation::ProviderLine {
                run_id: spec.run_id,
                node_id: spec.node_id,
                line: "created".to_owned(),
            });
            Ok(PluginNodeHandle {
                id: spec.attempt_id,
                provider_process_id: None,
            })
        }

        fn start_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
            self.stats.starts.fetch_add(1, Ordering::SeqCst);
            if self
                .stats
                .start_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    (remaining > 0).then(|| remaining - 1)
                })
                .is_ok()
            {
                Err("bootstrap start failed".to_owned())
            } else {
                Ok(())
            }
        }

        fn cancel_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
            self.stats.cancels.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn complete_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
            self.stats.completes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn stop_node(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
            self.stats.stops.fetch_add(1, Ordering::SeqCst);
            if self
                .stats
                .stop_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    (remaining > 0).then(|| remaining - 1)
                })
                .is_ok()
            {
                Err("node cleanup failed".to_owned())
            } else {
                Ok(())
            }
        }
    }

    struct BlockingCreatePlugin {
        stats: Arc<PluginStats>,
        entered: Sender<()>,
        release: Receiver<()>,
    }

    impl ProvisionPlugin for BlockingCreatePlugin {
        fn create_node(
            &mut self,
            spec: NodeProvisionSpec,
            _sink: PluginSink,
        ) -> Result<PluginNodeHandle, String> {
            self.stats.creates.fetch_add(1, Ordering::SeqCst);
            let _ = self.entered.send(());
            self.release
                .recv()
                .map_err(|_| "blocking test release closed".to_owned())?;
            Ok(PluginNodeHandle {
                id: spec.attempt_id,
                provider_process_id: None,
            })
        }

        fn start_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
            self.stats.starts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn cancel_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
            self.stats.cancels.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn complete_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
            self.stats.completes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn stop_node(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
            self.stats.stops.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn desired() -> LogicalNodeSpec {
        let node = LogicalNodeId("node-7-0".to_owned());
        LogicalNodeSpec {
            run_id: RunId(5),
            logical_node_id: node.clone(),
            group_id: NodeGroupId("node-7".to_owned()),
            role: RoleId("worker".to_owned()),
            provider: ProviderKind::new("fake"),
            shape: DesiredNodeShape {
                image: "node:v2".to_owned(),
                disk_gb: 1,
                gpu_name: None,
                min_gpu_ram_mb: None,
                min_down_mbps: None,
                min_up_mbps: None,
                min_reliability: None,
                require_verified: false,
                provider_labels: BTreeMap::new(),
            },
            boot: BootSpec {
                ssh_user: "root".to_owned(),
                verify_commands: Vec::new(),
                start_swactor_command: "swactor".to_owned(),
                stdout_sources: Vec::new(),
                stderr_sources: Vec::new(),
                env: vec![("MYELIN_RUN_ID".to_owned(), "5".to_owned())],
                args: vec!["--worker".to_owned()],
                mounts: vec![ProviderMount {
                    host_path: "/model".to_owned(),
                    container_path: "/model".to_owned(),
                    readonly: true,
                }],
            },
            swarm_join: SwarmJoinSpec {
                orch_swactor_addr: "orchestrator".to_owned(),
                join_token_ref: "token".to_owned(),
                expected_logical_node_id: node,
            },
        }
    }

    fn effect(sequence: u64, command: NodeManagerCommand) -> PlannedEffect {
        PlannedEffect {
            node: LogicalNodeId("node-7-0".to_owned()),
            operation: OperationId {
                attempt: NodeAttemptId(9),
                sequence,
            },
            command,
        }
    }

    #[test]
    fn adapter_creates_and_starts_once_and_adopts_repeated_effects() {
        let stats = Arc::new(PluginStats::default());
        let binding = ReconcilerNodeBinding {
            logical_node_id: LogicalNodeId("node-7-0".to_owned()),
            provision: NodeProvisionSpec {
                run_id: 5,
                node_id: 7,
                attempt_id: 0,
                stage_index: Some(0),
                image: "node:v1".to_owned(),
                env: Vec::new(),
                args: Vec::new(),
                mounts: Vec::new(),
            },
            plugin: Box::new(FakePlugin {
                stats: Arc::clone(&stats),
            }),
        };
        let downstream = PluginSink::new(Arc::new(NullSink));
        let (failure_tx, _failure_rx) = mpsc::channel();
        let (backend, _) = MyelinEffectBackend::new(vec![binding], downstream, failure_tx).unwrap();
        let desired = desired();

        let create = effect(
            1,
            NodeManagerCommand::CreateLease(CreateLeaseRequest {
                spec: desired.clone(),
            }),
        );
        let created = backend.execute(&create).unwrap();
        assert!(matches!(created, OperationOutcome::LeaseCreated(_)));
        let adopted = backend.execute(&create).unwrap();
        assert_eq!(adopted, created);
        assert_eq!(stats.creates.load(Ordering::SeqCst), 1);
        assert_eq!(stats.starts.load(Ordering::SeqCst), 0);

        let start = effect(
            2,
            NodeManagerCommand::StartBootstrap(BootstrapSessionSpec {
                run_id: desired.run_id.clone(),
                logical_node_id: desired.logical_node_id.clone(),
                lease_id: ProviderLeaseId("lease".to_owned()),
                ssh: SshEndpoint {
                    host: "host".to_owned(),
                    port: 22,
                    user: "root".to_owned(),
                    auth_ref: "key".to_owned(),
                },
                boot: desired.boot.clone(),
                swarm_join: desired.swarm_join.clone(),
                telemetry: TelemetryStreamId("bootstrap".to_owned()),
            }),
        );
        assert!(matches!(
            backend.execute(&start).unwrap(),
            OperationOutcome::BootstrapStarted {
                session_id: BootstrapSessionId(9)
            }
        ));
        assert!(matches!(
            backend.execute(&start).unwrap(),
            OperationOutcome::BootstrapStarted {
                session_id: BootstrapSessionId(9)
            }
        ));
        assert_eq!(stats.starts.load(Ordering::SeqCst), 1);
        let launched = stats.specs.lock().unwrap();
        assert_eq!(launched[0].attempt_id, 9);
        assert_eq!(launched[0].image, "node:v2");
        assert!(
            launched[0]
                .env
                .contains(&(ATTEMPT_ENV.to_owned(), "9".to_owned()))
        );
        drop(launched);

        let convergence = effect(
            3,
            NodeManagerCommand::BootstrapConvergenceObserved {
                session_id: BootstrapSessionId(9),
                swactor_id: SwactorId("joined".to_owned()),
            },
        );
        backend.execute(&convergence).unwrap();
        assert_eq!(stats.completes.load(Ordering::SeqCst), 1);

        let cancel = effect(
            4,
            NodeManagerCommand::CancelBootstrap {
                session_id: BootstrapSessionId(9),
            },
        );
        backend.execute(&cancel).unwrap();
        assert_eq!(stats.cancels.load(Ordering::SeqCst), 1);

        let destroy = effect(
            5,
            NodeManagerCommand::DestroyLease(match created {
                OperationOutcome::LeaseCreated(result) => result.lease.destroy_handle,
                _ => unreachable!(),
            }),
        );
        backend.execute(&destroy).unwrap();
        backend.execute(&destroy).unwrap();
        assert_eq!(stats.stops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn returned_bootstrap_start_failure_is_definite_and_phase_correlated() {
        let stats = Arc::new(PluginStats::default());
        stats.start_failures.store(1, Ordering::SeqCst);
        let binding = ReconcilerNodeBinding {
            logical_node_id: LogicalNodeId("node-7-0".to_owned()),
            provision: NodeProvisionSpec {
                run_id: 5,
                node_id: 7,
                attempt_id: 0,
                stage_index: Some(0),
                image: "node:v1".to_owned(),
                env: Vec::new(),
                args: Vec::new(),
                mounts: Vec::new(),
            },
            plugin: Box::new(FakePlugin {
                stats: Arc::clone(&stats),
            }),
        };
        let (failure_tx, _failure_rx) = mpsc::channel();
        let (backend, _) = MyelinEffectBackend::new(
            vec![binding],
            PluginSink::new(Arc::new(NullSink)),
            failure_tx,
        )
        .unwrap();
        let desired = desired();
        let create = effect(
            1,
            NodeManagerCommand::CreateLease(CreateLeaseRequest {
                spec: desired.clone(),
            }),
        );
        backend.execute(&create).unwrap();
        let start = effect(
            2,
            NodeManagerCommand::StartBootstrap(BootstrapSessionSpec {
                run_id: desired.run_id.clone(),
                logical_node_id: desired.logical_node_id.clone(),
                lease_id: ProviderLeaseId("lease".to_owned()),
                ssh: SshEndpoint {
                    host: "host".to_owned(),
                    port: 22,
                    user: "root".to_owned(),
                    auth_ref: "key".to_owned(),
                },
                boot: desired.boot,
                swarm_join: desired.swarm_join,
                telemetry: TelemetryStreamId("bootstrap".to_owned()),
            }),
        );

        let error = backend.execute(&start).unwrap_err();
        assert_eq!(
            error.disposition,
            ::provisioning::EffectFailureDisposition::Definite
        );
        assert_eq!(error.reason, "bootstrap start failed");
        assert_eq!(stats.creates.load(Ordering::SeqCst), 1);
        assert_eq!(stats.starts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn replacement_binding_activates_only_after_old_lease_cleanup() {
        let old_stats = Arc::new(PluginStats::default());
        let new_stats = Arc::new(PluginStats::default());
        let binding = |stats: &Arc<PluginStats>, node_id| ReconcilerNodeBinding {
            logical_node_id: LogicalNodeId("node-7-0".to_owned()),
            provision: NodeProvisionSpec {
                run_id: 5,
                node_id,
                attempt_id: 0,
                stage_index: Some(0),
                image: "node:v1".to_owned(),
                env: Vec::new(),
                args: Vec::new(),
                mounts: Vec::new(),
            },
            plugin: Box::new(FakePlugin {
                stats: Arc::clone(stats),
            }),
        };
        let (failure_tx, _failure_rx) = mpsc::channel();
        let (backend, _) = MyelinEffectBackend::new(
            vec![binding(&old_stats, 7)],
            PluginSink::new(Arc::new(NullSink)),
            failure_tx,
        )
        .unwrap();
        let request = CreateLeaseRequest { spec: desired() };
        let create = effect(1, NodeManagerCommand::CreateLease(request.clone()));
        let created = backend.execute(&create).unwrap();
        backend.register_binding(binding(&new_stats, 8));

        assert_eq!(backend.execute(&create).unwrap(), created);
        assert_eq!(old_stats.creates.load(Ordering::SeqCst), 1);
        assert_eq!(new_stats.creates.load(Ordering::SeqCst), 0);

        let destroy = effect(
            2,
            NodeManagerCommand::DestroyLease(match created {
                OperationOutcome::LeaseCreated(result) => result.lease.destroy_handle,
                _ => unreachable!(),
            }),
        );
        backend.execute(&destroy).unwrap();
        let mut replacement = effect(3, NodeManagerCommand::CreateLease(request));
        replacement.operation.attempt = NodeAttemptId(10);
        backend.execute(&replacement).unwrap();

        assert_eq!(old_stats.stops.load(Ordering::SeqCst), 1);
        assert_eq!(old_stats.creates.load(Ordering::SeqCst), 1);
        assert_eq!(new_stats.creates.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn failed_node_cleanup_keeps_lease_owned_for_retry() {
        let stats = Arc::new(PluginStats::default());
        stats.stop_failures.store(2, Ordering::SeqCst);
        let binding = ReconcilerNodeBinding {
            logical_node_id: LogicalNodeId("node-7-0".to_owned()),
            provision: NodeProvisionSpec {
                run_id: 5,
                node_id: 7,
                attempt_id: 0,
                stage_index: Some(0),
                image: "node:v1".to_owned(),
                env: Vec::new(),
                args: Vec::new(),
                mounts: Vec::new(),
            },
            plugin: Box::new(FakePlugin {
                stats: Arc::clone(&stats),
            }),
        };
        let (failure_tx, _failure_rx) = mpsc::channel();
        let (backend, _) = MyelinEffectBackend::new(
            vec![binding],
            PluginSink::new(Arc::new(NullSink)),
            failure_tx,
        )
        .unwrap();
        let create = effect(
            1,
            NodeManagerCommand::CreateLease(CreateLeaseRequest { spec: desired() }),
        );
        let created = backend.execute(&create).unwrap();
        let destroy = effect(
            2,
            NodeManagerCommand::DestroyLease(match created {
                OperationOutcome::LeaseCreated(result) => result.lease.destroy_handle,
                _ => unreachable!(),
            }),
        );

        let error = backend.execute(&destroy).unwrap_err();
        assert_eq!(
            error.disposition,
            ::provisioning::EffectFailureDisposition::Definite
        );
        assert_eq!(error.reason, "node cleanup failed");
        assert_eq!(backend.stop_all().unwrap_err(), "node cleanup failed");
        backend.stop_all().unwrap();
        assert_eq!(stats.stops.load(Ordering::SeqCst), 3);
    }

    #[test]
    #[allow(clippy::disallowed_methods)]
    fn engine_hosted_controller_converges_and_cleans_up_end_to_end() {
        let stats = Arc::new(PluginStats::default());
        let desired_node = desired();
        let shape = ClusterShape {
            run_id: desired_node.run_id.clone(),
            generation: 1,
            groups: vec![RunNodeGroupSpec {
                run_id: desired_node.run_id.clone(),
                group_id: desired_node.group_id.clone(),
                role: desired_node.role.clone(),
                count: 1,
                provider: desired_node.provider.clone(),
                shape: desired_node.shape.clone(),
                boot: desired_node.boot.clone(),
                swarm_join: SwarmJoinTemplate {
                    orch_swactor_addr: desired_node.swarm_join.orch_swactor_addr.clone(),
                    join_token_ref: desired_node.swarm_join.join_token_ref.clone(),
                },
            }],
        };
        let binding = ReconcilerNodeBinding {
            logical_node_id: desired_node.logical_node_id,
            provision: NodeProvisionSpec {
                run_id: 5,
                node_id: 7,
                attempt_id: 0,
                stage_index: Some(0),
                image: "node:v1".to_owned(),
                env: Vec::new(),
                args: Vec::new(),
                mounts: Vec::new(),
            },
            plugin: Box::new(FakePlugin {
                stats: Arc::clone(&stats),
            }),
        };
        let parts = swactor::runtime::RuntimeParts::new(swactor::config::RuntimeConfig::default());
        let backend =
            swactor_engine::TokioBackend::new(swactor_engine::TokioConfig::default()).unwrap();
        let engine = swactor_engine::Engine::new(parts, backend).unwrap();
        let sink = PluginSink::new(Arc::new(NullSink));
        let mut cluster = ProvisionedClusterGuard::new(
            shape,
            vec![binding],
            RetryPolicy::default(),
            engine.handle(),
            sink,
        )
        .unwrap();

        for _ in 0..200 {
            cluster.poll(SystemTime::now()).unwrap();
            if cluster.awaiting_runtime() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(cluster.awaiting_runtime());
        let attempt = cluster.current_attempt(7).unwrap();
        assert!(cluster.observe_runtime_ready(
            7,
            attempt,
            SwactorId("worker-7".to_owned()),
            SystemTime::now(),
        ));

        for _ in 0..200 {
            cluster.poll(SystemTime::now()).unwrap();
            if cluster.is_converged() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(cluster.is_converged());
        assert_eq!(stats.starts.load(Ordering::SeqCst), 1);
        assert_eq!(stats.creates.load(Ordering::SeqCst), 1);
        assert_eq!(stats.completes.load(Ordering::SeqCst), 1);
        let scaled_stats = Arc::new(PluginStats::default());
        let scaled = desired();
        cluster
            .update_desired(
                ClusterShape {
                    run_id: scaled.run_id.clone(),
                    generation: 2,
                    groups: vec![RunNodeGroupSpec {
                        run_id: scaled.run_id,
                        group_id: scaled.group_id,
                        role: scaled.role,
                        count: 2,
                        provider: scaled.provider,
                        shape: scaled.shape,
                        boot: scaled.boot,
                        swarm_join: SwarmJoinTemplate {
                            orch_swactor_addr: scaled.swarm_join.orch_swactor_addr,
                            join_token_ref: scaled.swarm_join.join_token_ref,
                        },
                    }],
                },
                vec![ReconcilerNodeBinding {
                    logical_node_id: LogicalNodeId("node-7-1".to_owned()),
                    provision: NodeProvisionSpec {
                        run_id: 5,
                        node_id: 8,
                        attempt_id: 0,
                        stage_index: Some(1),
                        image: "node:v1".to_owned(),
                        env: Vec::new(),
                        args: Vec::new(),
                        mounts: Vec::new(),
                    },
                    plugin: Box::new(FakePlugin {
                        stats: Arc::clone(&scaled_stats),
                    }),
                }],
            )
            .unwrap();
        for _ in 0..200 {
            cluster.poll(SystemTime::now()).unwrap();
            if cluster.awaiting_runtime() && cluster.current_attempt(8).is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let scaled_attempt = cluster.current_attempt(8).unwrap();
        assert!(cluster.observe_runtime_ready(
            8,
            scaled_attempt,
            SwactorId("worker-8".to_owned()),
            SystemTime::now(),
        ));
        for _ in 0..200 {
            cluster.poll(SystemTime::now()).unwrap();

            if cluster.is_converged() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(cluster.is_converged());
        assert_eq!(scaled_stats.starts.load(Ordering::SeqCst), 1);
        assert_eq!(scaled_stats.creates.load(Ordering::SeqCst), 1);
        assert_eq!(scaled_stats.completes.load(Ordering::SeqCst), 1);

        cluster.stop().unwrap();
        assert!(cluster.is_stopped());
        assert_eq!(stats.stops.load(Ordering::SeqCst), 1);
        assert_eq!(scaled_stats.stops.load(Ordering::SeqCst), 1);
    }
    #[test]
    #[allow(clippy::disallowed_methods)]
    fn ambiguous_create_timeout_retries_by_adoption_and_discards_late_success() {
        let stats = Arc::new(PluginStats::default());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let desired_node = desired();
        let shape = ClusterShape {
            run_id: desired_node.run_id.clone(),
            generation: 1,
            groups: vec![RunNodeGroupSpec {
                run_id: desired_node.run_id.clone(),
                group_id: desired_node.group_id.clone(),
                role: desired_node.role.clone(),
                count: 1,
                provider: desired_node.provider.clone(),
                shape: desired_node.shape.clone(),
                boot: desired_node.boot.clone(),
                swarm_join: SwarmJoinTemplate {
                    orch_swactor_addr: desired_node.swarm_join.orch_swactor_addr.clone(),
                    join_token_ref: desired_node.swarm_join.join_token_ref.clone(),
                },
            }],
        };
        let binding = ReconcilerNodeBinding {
            logical_node_id: desired_node.logical_node_id,
            provision: NodeProvisionSpec {
                run_id: 5,
                node_id: 7,
                attempt_id: 0,
                stage_index: Some(0),
                image: "node:v1".to_owned(),
                env: Vec::new(),
                args: Vec::new(),
                mounts: Vec::new(),
            },
            plugin: Box::new(BlockingCreatePlugin {
                stats: Arc::clone(&stats),
                entered: entered_tx,
                release: release_rx,
            }),
        };
        let parts = swactor::runtime::RuntimeParts::new(swactor::config::RuntimeConfig::default());
        let backend =
            swactor_engine::TokioBackend::new(swactor_engine::TokioConfig::default()).unwrap();
        let engine = swactor_engine::Engine::new(parts, backend).unwrap();
        let mut cluster = ProvisionedClusterGuard::new(
            shape,
            vec![binding],
            RetryPolicy {
                operation_timeout: Duration::from_secs(1),
                ..RetryPolicy::default()
            },
            engine.handle(),
            PluginSink::new(Arc::new(NullSink)),
        )
        .unwrap();

        let mut entered = false;
        for _ in 0..200 {
            cluster.poll(SystemTime::now()).unwrap();
            if entered_rx.try_recv().is_ok() {
                entered = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(entered, "lease creation did not enter the backend");

        cluster
            .poll(SystemTime::now() + Duration::from_secs(2))
            .unwrap();
        let managed = cluster.driver.state().nodes.values().next().unwrap();
        assert_eq!(managed.intent, NodeIntent::Active);
        assert!(managed.pending.is_none());
        assert!(
            managed
                .retry
                .last_error
                .as_deref()
                .is_some_and(|reason| reason.contains("ambiguous outcome"))
        );

        release_tx.send(()).unwrap();
        let retry_now = SystemTime::now() + Duration::from_secs(5);
        for _ in 0..200 {
            cluster.poll(retry_now).unwrap();
            if cluster.awaiting_runtime() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(cluster.awaiting_runtime());
        assert_eq!(stats.creates.load(Ordering::SeqCst), 1);
        assert_eq!(stats.starts.load(Ordering::SeqCst), 1);

        let attempt = cluster.current_attempt(7).unwrap();
        assert!(cluster.observe_runtime_ready(
            7,
            attempt,
            SwactorId("worker-7".to_owned()),
            retry_now,
        ));
        for _ in 0..200 {
            cluster.poll(retry_now).unwrap();
            if cluster.is_converged() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(cluster.is_converged());
        assert_eq!(stats.completes.load(Ordering::SeqCst), 1);

        cluster.stop().unwrap();
        assert_eq!(stats.stops.load(Ordering::SeqCst), 1);
    }
}
