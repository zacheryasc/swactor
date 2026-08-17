//! The provisioning test kit: a reusable conformance suite for the
//! reconciler crate and any `ProvisionPlugin` implementation.
//!
//! Style: stateful property-based testing (an Erlang-QuickCheck-style
//! command-sequence test without the framework). A trace is an explicit
//! `Vec<Input>`; the harness folds it into a real `ClusterDriver` plus a
//! real `IdempotentEffectExecutor` over a backend, settling the machine
//! to quiescence after every input and checking guarantees. The oracle
//! is the invariant checker only — there is no reference model.
//! Failures panic with the seed and a shrunk minimal trace; paste that
//! trace into a plain `#[test]` to pin a regression.
//!
//! Conformance levels (one battery, three backends):
//! - `FakeBackend`: the reference in-memory substrate (fastest).
//! - `PluginBackendAdapter` over an in-memory `TestablePlugin`.
//! - `PluginBackendAdapter` over a real process-spawning plugin.
//!
//! Guarantee index (each item names its enforcement site):
//! - identity: attempts unique, ordered, never reused (oracle)
//! - correlation: pending ops belong to the current attempt (oracle)
//! - dead nodes hold nothing: Destroyed => no lease/session/pending (oracle)
//! - deleting nodes are not ready (oracle)
//! - session coherence: active session => bootstrap facts (oracle)
//! - readiness implies full facts and desired match (oracle)
//! - attempt-fact ownership: lease/session facts never leak across
//!   attempts (oracle; fixture identities are attempt-encoded)
//! - resource conservation: a converged machine leaks nothing
//!   (`HarnessedBackend::leaked`, checked at convergence)
//! - quiescence: a forced pass on a quiescent machine is a no-op, a
//!   converged machine requeues nothing, and results are drained
//!   (harness, after every settle)
//! - monotonic generation: observed_generation never regresses (harness)
//! - replay determinism: identical traces yield identical state (test)
//! - fair convergence: fault-free tails converge in bounded rounds and
//!   bounded replacement attempts (harness fair tail)
//! - latest-desired-wins: a late shape change converges to it (test)
//! - run-order confluence: FIFO vs LIFO work execution converge to the
//!   same state (test)
//! - deadline boundary: operations expire exactly at their deadline (test)
//! - clock extremes: saturated arithmetic never panics (test)
//! - allocator exhaustion: reported as an error, not a spin (test)
//!
//! Note on backend calls for retired attempts: an effect dispatched
//! before expiry may legitimately complete at the provider after its
//! attempt is retired (real providers have latency). The executor's
//! identity/adoption contract plus the driver's stale-result rejection
//! make that safe; what must never happen — old-attempt facts surviving
//! into a live attempt — is the attempt-fact-ownership oracle line.

// Shared across test binaries; each binary uses a different subset.
#![allow(dead_code)]

use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use provisioning::plugin::*;
use provisioning::*;

// ── fixtures ──────────────────────────────────────────────────────────

pub fn group_with_role(id: &str, count: u32, role: &str) -> RunNodeGroupSpec {
    RunNodeGroupSpec {
        run_id: RunId(7),
        group_id: NodeGroupId(id.to_owned()),
        role: RoleId(role.to_owned()),
        count,
        provider: ProviderKind::new("mock"),
        shape: DesiredNodeShape {
            image: "node:v1".to_owned(),
            disk_gb: 20,
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
            verify_commands: vec!["true".to_owned()],
            start_swactor_command: "swactor".to_owned(),
            stdout_sources: Vec::new(),
            stderr_sources: Vec::new(),
            env: Vec::new(),
            args: Vec::new(),
            mounts: Vec::new(),
        },
        swarm_join: SwarmJoinTemplate {
            orch_swactor_addr: "127.0.0.1:9000".to_owned(),
            join_token_ref: "token".to_owned(),
        },
    }
}

pub fn group(id: &str, count: u32) -> RunNodeGroupSpec {
    group_with_role(id, count, "worker")
}

pub fn shape(generation: u64, groups: Vec<RunNodeGroupSpec>) -> ClusterShape {
    ClusterShape {
        run_id: RunId(7),
        generation,
        groups,
    }
}

pub fn ssh_endpoint() -> SshEndpoint {
    SshEndpoint {
        host: "127.0.0.1".to_owned(),
        port: 22,
        user: "root".to_owned(),
        auth_ref: "test-key".to_owned(),
    }
}

/// Lease identity encodes the owning attempt, so fact leakage across
/// attempts is detectable in pure state.
pub fn lease_result(attempt: NodeAttemptId, endpoint: bool) -> CreateLeaseResult {
    let provider = ProviderKind::new("mock");
    let lease_id = ProviderLeaseId(format!("lease-{}", attempt.0));
    CreateLeaseResult {
        lease: LeaseFacts {
            provider: provider.clone(),
            lease_id: lease_id.clone(),
            provider_contract_id: format!("contract-{}", attempt.0),
            offer_id: None,
            destroy_handle: DestroyHandle {
                provider,
                lease_id,
                provider_contract_id: format!("contract-{}", attempt.0),
            },
            provider_metadata: BTreeMap::new(),
        },
        endpoint: endpoint.then(ssh_endpoint),
    }
}

/// Bootstrap session identity encodes the owning attempt (sessions are
/// `attempt * 1_000_000 + sequence`, sequences start at 1).
pub const SESSION_SEQ_SPACE: u64 = 1_000_000;

pub fn session_id_for(operation: OperationId) -> BootstrapSessionId {
    assert!(
        operation.sequence < SESSION_SEQ_SPACE,
        "session id encoding exhausted"
    );
    BootstrapSessionId(operation.attempt.0 * SESSION_SEQ_SPACE + operation.sequence)
}

#[derive(Default)]
pub struct RecordingExecutor {
    pub submitted: usize,
}

impl EffectExecutor for RecordingExecutor {
    type SubmitError = Infallible;

    fn submit(&mut self, _effect: &PlannedEffect) -> Result<(), Self::SubmitError> {
        self.submitted += 1;
        Ok(())
    }
}

pub struct NullSink;

impl PluginObservationSink for NullSink {
    fn observe(&self, _observation: PluginObservation) {}
}

pub fn null_sink() -> PluginSink {
    PluginSink::new(Arc::new(NullSink))
}

/// A `NodeProvisionSpec` for a concrete attempt, for direct plugin calls.
pub fn plugin_spec(attempt: u64) -> NodeProvisionSpec {
    NodeProvisionSpec {
        run_id: 7,
        node_id: attempt,
        attempt_id: attempt,
        stage_index: None,
        image: "kit-node".to_owned(),
        env: Vec::new(),
        args: Vec::new(),
        mounts: Vec::new(),
    }
}

// ── backend contract ──────────────────────────────────────────────────

/// Scripted answer for the next backend call. An empty script means
/// `Succeed`. `NoEndpoint` only affects lease creation (forces the
/// endpoint-probe path); every other kind treats it as `Succeed`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Reply {
    Succeed,
    NoEndpoint,
    Definite(&'static str),
    Ambiguous(&'static str),
    Panic,
}

/// A backend the harness can drive: scripted faults, a healed (fair)
/// mode, and a resource-conservation probe.
pub trait HarnessedBackend: EffectBackend + Clone {
    /// Script the next fault. `Reply::Succeed` means "no fault".
    fn script(&mut self, reply: Reply);
    /// Enter the fault-free mode (fair tail).
    fn heal(&mut self);
    /// Resources alive but not owned by any live lease (leaks).
    fn leaked(&self) -> Vec<String>;
}

// ── reference backend: fake substrate ─────────────────────────────────

#[derive(Default)]
struct BackendShared {
    scripted: Mutex<VecDeque<Reply>>,
    calls: Mutex<Vec<PlannedEffect>>,
}

#[derive(Clone, Default)]
pub struct FakeBackend {
    shared: Arc<BackendShared>,
}

impl FakeBackend {
    pub fn calls(&self) -> Vec<PlannedEffect> {
        self.shared.calls.lock().clone()
    }
}

impl HarnessedBackend for FakeBackend {
    fn script(&mut self, reply: Reply) {
        self.shared.scripted.lock().push_back(reply);
    }

    fn heal(&mut self) {
        self.shared.scripted.lock().clear();
    }

    fn leaked(&self) -> Vec<String> {
        Vec::new()
    }
}

impl EffectBackend for FakeBackend {
    fn execute(&self, effect: &PlannedEffect) -> Result<OperationOutcome, EffectError> {
        self.shared.calls.lock().push(effect.clone());
        let reply = self
            .shared
            .scripted
            .lock()
            .pop_front()
            .unwrap_or(Reply::Succeed);
        match reply {
            Reply::Definite(reason) => return Err(EffectError::definite(reason)),
            Reply::Ambiguous(reason) => return Err(EffectError::ambiguous(reason)),
            Reply::Panic => panic!("scripted backend panic"),
            Reply::Succeed | Reply::NoEndpoint => {}
        }
        let endpoint = !matches!(reply, Reply::NoEndpoint);
        Ok(match &effect.command {
            NodeManagerCommand::CreateLease(_) => {
                OperationOutcome::LeaseCreated(lease_result(effect.operation.attempt, endpoint))
            }
            NodeManagerCommand::LookupEndpoint(_) => {
                OperationOutcome::EndpointLookup(Some(ssh_endpoint()))
            }
            NodeManagerCommand::StartBootstrap(_) => OperationOutcome::BootstrapStarted {
                session_id: session_id_for(effect.operation),
            },
            NodeManagerCommand::BootstrapConvergenceObserved { .. } => {
                OperationOutcome::BootstrapConvergenceAccepted
            }
            NodeManagerCommand::CancelBootstrap { .. } => OperationOutcome::BootstrapCancelled,
            NodeManagerCommand::DestroyLease(_) => OperationOutcome::LeaseDestroyed,
        })
    }
}

// ── plugin-level kit ──────────────────────────────────────────────────

/// Fault a `TestablePlugin` can inject into its own behavior.
/// The error string a `TestablePlugin` returns for `Fault::Ambiguous`.
/// The `PluginBackendAdapter` reclassifies it as an ambiguous effect
/// error; every other plugin error is definite.
pub const AMBIGUOUS_FAULT_MARKER: &str = "kit-ambiguous: work may have happened";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// Fail cleanly: the call errors and nothing is created.
    Definite,
    /// Fail ambiguously: the resource is created but the call errors.
    /// A retry with the same attempt must adopt the resource, not
    /// create a second one. The plugin reports this by returning
    /// `AMBIGUOUS_FAULT_MARKER` from the call; the adapter reclassifies
    /// that error as an ambiguous effect error so the driver records
    /// `ambiguous_operation` and adopts on retry.
    Ambiguous,
    /// Panic inside the plugin call.
    Panic,
    /// Clear all injected faults.
    Heal,
}

/// A `ProvisionPlugin` the kit can drive and probe. All probe methods
/// use interior mutability so the plugin can live behind the adapter.
pub trait TestablePlugin: ProvisionPlugin {
    /// Queue the next fault (`Fault::Heal` clears all).
    fn apply_fault(&self, fault: Fault);
    /// Resources alive but not owned by `live_handles`.
    fn leaked_resources(&self, live_handles: &[u64]) -> Vec<String>;
    /// Total resources ever created (adoption must not increment this).
    fn resources_created(&self) -> usize;
}

struct LiveLease {
    handle: PluginNodeHandle,
    endpoint: Option<SshEndpoint>,
}

struct AdapterShared<P> {
    plugin: P,
    live: BTreeMap<u64, LiveLease>,
    withhold_endpoint: bool,
}

fn classify_plugin_error(error: String) -> EffectError {
    if error == AMBIGUOUS_FAULT_MARKER {
        EffectError::ambiguous(error)
    } else {
        EffectError::definite(error)
    }
}

/// The kit's `EffectBackend` over a `ProvisionPlugin`: maps
/// `NodeManagerCommand`s to plugin calls, records live leases per
/// attempt, and synthesizes lease/session identities using the oracle's
/// attempt-encoded conventions. Create adopts: an existing live lease
/// for the same attempt is returned instead of calling the plugin
/// again (mirrors provider-side idempotency keys).
pub struct PluginBackendAdapter<P> {
    shared: Arc<Mutex<AdapterShared<P>>>,
}

impl<P: TestablePlugin + 'static> PluginBackendAdapter<P> {
    pub fn new(plugin: P) -> Self {
        Self {
            shared: Arc::new(Mutex::new(AdapterShared {
                plugin,
                live: BTreeMap::new(),
                withhold_endpoint: false,
            })),
        }
    }
}

impl<P: TestablePlugin + 'static> Clone for PluginBackendAdapter<P> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<P: TestablePlugin + 'static> HarnessedBackend for PluginBackendAdapter<P> {
    fn script(&mut self, reply: Reply) {
        let mut shared = self.shared.lock();
        match reply {
            Reply::Succeed => {}
            Reply::NoEndpoint => shared.withhold_endpoint = true,
            Reply::Definite(_) => shared.plugin.apply_fault(Fault::Definite),
            Reply::Ambiguous(_) => shared.plugin.apply_fault(Fault::Ambiguous),
            Reply::Panic => shared.plugin.apply_fault(Fault::Panic),
        }
    }

    fn heal(&mut self) {
        let mut shared = self.shared.lock();
        shared.plugin.apply_fault(Fault::Heal);
        shared.withhold_endpoint = false;
    }

    fn leaked(&self) -> Vec<String> {
        let shared = self.shared.lock();
        let live: Vec<u64> = shared.live.values().map(|lease| lease.handle.id).collect();
        shared.plugin.leaked_resources(&live)
    }
}

impl<P: TestablePlugin + 'static> EffectBackend for PluginBackendAdapter<P> {
    fn execute(&self, effect: &PlannedEffect) -> Result<OperationOutcome, EffectError> {
        let attempt = effect.operation.attempt.0;
        let mut shared = self.shared.lock();
        match &effect.command {
            NodeManagerCommand::CreateLease(request) => {
                if let Some(lease) = shared.live.get(&attempt) {
                    // Adoption: the resource for this attempt already
                    // exists; return it without touching the plugin.
                    let endpoint = lease.endpoint.clone();
                    return Ok(OperationOutcome::LeaseCreated(lease_result(
                        effect.operation.attempt,
                        endpoint.is_some(),
                    )));
                }
                let spec = NodeProvisionSpec {
                    run_id: request.spec.run_id.0,
                    node_id: attempt,
                    attempt_id: attempt,
                    stage_index: None,
                    image: request.spec.shape.image.clone(),
                    env: Vec::new(),
                    args: Vec::new(),
                    mounts: Vec::new(),
                };
                let handle = shared
                    .plugin
                    .create_node(spec, null_sink())
                    .map_err(classify_plugin_error)?;
                let endpoint = (!shared.withhold_endpoint).then(ssh_endpoint);
                shared.withhold_endpoint = false;
                shared.live.insert(
                    attempt,
                    LiveLease {
                        handle,
                        endpoint: endpoint.clone(),
                    },
                );
                Ok(OperationOutcome::LeaseCreated(lease_result(
                    effect.operation.attempt,
                    endpoint.is_some(),
                )))
            }
            NodeManagerCommand::LookupEndpoint(_) => {
                // A provider resolves the node's address on each probe;
                // a lease created without an endpoint gets one here
                // (possibly after some None probes in a faulty world,
                // but the healthy tail always resolves).
                if !shared.live.contains_key(&attempt) {
                    return Ok(OperationOutcome::EndpointLookup(None));
                }
                Ok(OperationOutcome::EndpointLookup(Some(ssh_endpoint())))
            }
            NodeManagerCommand::StartBootstrap(_) => {
                let handle = shared
                    .live
                    .get(&attempt)
                    .map(|lease| lease.handle.clone())
                    .ok_or_else(|| EffectError::definite("no live lease for bootstrap"))?;
                shared
                    .plugin
                    .start_bootstrap(&handle)
                    .map_err(EffectError::definite)?;
                Ok(OperationOutcome::BootstrapStarted {
                    session_id: session_id_for(effect.operation),
                })
            }
            NodeManagerCommand::BootstrapConvergenceObserved { .. } => {
                let handle = shared
                    .live
                    .get(&attempt)
                    .map(|lease| lease.handle.clone())
                    .ok_or_else(|| EffectError::definite("bootstrap lease is absent"))?;
                shared
                    .plugin
                    .complete_bootstrap(&handle)
                    .map_err(EffectError::definite)?;
                Ok(OperationOutcome::BootstrapConvergenceAccepted)
            }
            NodeManagerCommand::CancelBootstrap { .. } => {
                if let Some(lease) = shared.live.get(&attempt) {
                    let handle = lease.handle.clone();
                    shared
                        .plugin
                        .cancel_bootstrap(&handle)
                        .map_err(EffectError::definite)?;
                }
                Ok(OperationOutcome::BootstrapCancelled)
            }
            NodeManagerCommand::DestroyLease(_) => {
                let Some(lease) = shared.live.remove(&attempt) else {
                    return Ok(OperationOutcome::LeaseDestroyed);
                };
                shared
                    .plugin
                    .stop_node(&lease.handle)
                    .map_err(classify_plugin_error)?;
                Ok(OperationOutcome::LeaseDestroyed)
            }
        }
    }
}

// ── reference in-memory plugin ────────────────────────────────────────

#[derive(Default)]
struct FakePluginShared {
    faults: VecDeque<Fault>,
    /// Attempts with a live resource; the handle id is the attempt.
    resources: BTreeSet<u64>,
    created: usize,
}

/// The kit's reference `TestablePlugin`: resources are map entries,
/// faults are queue entries. `Fault::Ambiguous` inserts the resource
/// and fails; a retry for the same attempt adopts it.
#[derive(Clone, Default)]
pub struct FakePlugin {
    shared: Arc<Mutex<FakePluginShared>>,
}

impl TestablePlugin for FakePlugin {
    fn apply_fault(&self, fault: Fault) {
        let mut shared = self.shared.lock();
        match fault {
            Fault::Heal => shared.faults.clear(),
            other => shared.faults.push_back(other),
        }
    }

    fn leaked_resources(&self, live_handles: &[u64]) -> Vec<String> {
        let shared = self.shared.lock();
        shared
            .resources
            .iter()
            .filter(|attempt| !live_handles.contains(attempt))
            .map(|attempt| format!("resource-{attempt}"))
            .collect()
    }

    fn resources_created(&self) -> usize {
        self.shared.lock().created
    }
}

impl ProvisionPlugin for FakePlugin {
    fn create_node(
        &mut self,
        spec: NodeProvisionSpec,
        _sink: PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        let mut shared = self.shared.lock();
        let attempt = spec.attempt_id;
        if shared.resources.contains(&attempt) {
            return Ok(PluginNodeHandle {
                id: attempt,
                provider_process_id: None,
            });
        }
        let fault = shared.faults.pop_front();
        if matches!(fault, Some(Fault::Panic)) {
            panic!("scripted plugin panic");
        }
        if matches!(fault, Some(Fault::Definite)) {
            return Err("scripted definite failure".to_owned());
        }
        shared.resources.insert(attempt);
        shared.created += 1;
        if matches!(fault, Some(Fault::Ambiguous)) {
            return Err(AMBIGUOUS_FAULT_MARKER.to_owned());
        }
        Ok(PluginNodeHandle {
            id: attempt,
            provider_process_id: None,
        })
    }

    fn start_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
        Ok(())
    }

    fn cancel_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
        Ok(())
    }

    fn complete_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
        Ok(())
    }

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        self.shared.lock().resources.remove(&handle.id);
        Ok(())
    }
}

// ── input alphabet ────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BootEvent {
    Joined,
    Closed,
    Failed,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Input {
    /// Advance the clock. The only source of time.
    Tick(Duration),
    /// Update the desired shape. The generator guarantees legal
    /// generations (strictly increasing), so this never errors.
    Shape(ClusterShape),
    /// Script the backend's next reply.
    Reply(Reply),
    /// Execute all dispatched-but-unexecuted backend work.
    Run,
    /// Deliver an external bootstrap observation to every node with an
    /// active bootstrap session, in sorted node order.
    Boot(BootEvent),
}

impl Input {
    pub fn describe(&self) -> String {
        match self {
            Input::Tick(duration) => format!("tick({duration:?})"),
            Input::Shape(shape) => {
                let first = shape.groups.first();
                format!(
                    "shape(gen={}, count={}, role={})",
                    shape.generation,
                    first.map_or(0, |g| g.count),
                    first.map_or("-", |g| g.role.0.as_str()),
                )
            }
            Input::Reply(reply) => format!("reply({reply:?})"),
            Input::Run => "run".to_owned(),
            Input::Boot(event) => format!("boot({event:?})"),
        }
    }
}

pub fn describe_trace(trace: &[Input]) -> String {
    trace
        .iter()
        .enumerate()
        .map(|(index, input)| format!("  [{index}] {}\n", input.describe()))
        .collect()
}

// ── oracle ────────────────────────────────────────────────────────────

/// Guarantees that must hold in every reachable state, checked after
/// every settle. `desired` is the expanded desired shape the driver is
/// converging toward.
pub fn check_invariants(
    state: &ClusterState,
    desired: &BTreeMap<LogicalNodeId, LogicalNodeSpec>,
) -> Result<(), String> {
    let mut attempts = BTreeSet::new();
    for (id, node) in &state.nodes {
        if !attempts.insert(node.attempt) {
            return Err(format!("attempt {} reused across nodes", node.attempt.0));
        }
        if node.attempt.0 >= state.next_attempt_id {
            return Err(format!(
                "node {id:?}: attempt {} not below allocator {}",
                node.attempt.0, state.next_attempt_id
            ));
        }
        if let Some(pending) = &node.pending {
            if pending.id.attempt != node.attempt {
                return Err(format!(
                    "node {id:?}: pending operation {}:{} does not belong to attempt {}",
                    pending.id.attempt.0, pending.id.sequence, node.attempt.0
                ));
            }
            if pending.id.sequence >= node.next_operation_sequence {
                return Err(format!(
                    "node {id:?}: pending sequence {} not below next {}",
                    pending.id.sequence, node.next_operation_sequence
                ));
            }
        }
        if node.intent == NodeIntent::Deleting && node.record.ready {
            return Err(format!("node {id:?}: Deleting but ready"));
        }
        if node.record.stage == NodeStage::Destroyed {
            if node.record.lease.is_some() {
                return Err(format!("node {id:?}: Destroyed with live lease"));
            }
            if node.active_bootstrap.is_some() {
                return Err(format!("node {id:?}: Destroyed with active bootstrap"));
            }
            if node.pending.is_some() {
                return Err(format!("node {id:?}: Destroyed with pending operation"));
            }
        }
        if node.active_bootstrap.is_some() && node.record.bootstrap.is_none() {
            return Err(format!(
                "node {id:?}: active session without bootstrap facts"
            ));
        }
        // Attempt-fact ownership: fixture identities are attempt-encoded,
        // so a fact from another attempt is detectable here.
        if let Some(lease) = &node.record.lease
            && lease.lease_id.0 != format!("lease-{}", node.attempt.0)
        {
            return Err(format!(
                "node {id:?}: lease {} leaked from another attempt",
                lease.lease_id.0
            ));
        }
        for session in [
            node.active_bootstrap,
            node.record.bootstrap.as_ref().map(|f| f.session_id),
        ]
        .into_iter()
        .flatten()
        {
            let owning_attempt = session.0 / SESSION_SEQ_SPACE;
            let sequence = session.0 % SESSION_SEQ_SPACE;
            if owning_attempt != node.attempt.0 || sequence == 0 {
                return Err(format!(
                    "node {id:?}: bootstrap session {} leaked from another attempt",
                    session.0
                ));
            }
        }
        // Readiness implies full facts and agreement with desired.
        if node.record.ready {
            if node.intent != NodeIntent::Active {
                return Err(format!("node {id:?}: ready but intent {:?}", node.intent));
            }
            if node.record.lease.is_none() || node.record.connection.is_none() {
                return Err(format!("node {id:?}: ready without live lease/connection"));
            }
            if node
                .record
                .swactor
                .as_ref()
                .is_none_or(|swactor| swactor.handed_off_at.is_none())
            {
                return Err(format!(
                    "node {id:?}: ready without completed swactor handoff"
                ));
            }
            match desired.get(id) {
                Some(spec) if node.record.desired == *spec => {}
                _ => {
                    return Err(format!(
                        "node {id:?}: ready but does not match the desired shape"
                    ));
                }
            }
        }
    }
    Ok(())
}

// ── spawner ───────────────────────────────────────────────────────────

/// Order in which a deferred batch of blocking work is executed; used by
/// the confluence guarantee (permuting independent deliveries within a
/// round must not change the converged state).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum RunOrder {
    #[default]
    Fifo,
    Lifo,
}

/// Defers all blocking work: dispatched effects queue up and execute only
/// when the harness runs them (`Input::Run`). This makes in-flight effects
/// that outlive deadlines reachable.
#[derive(Clone, Default)]
pub struct DeferredSpawner {
    work: Arc<Mutex<Vec<BlockingEffectWork>>>,
    order: RunOrder,
}

impl DeferredSpawner {
    pub fn run_all(&self) {
        loop {
            let mut work = std::mem::take(&mut *self.work.lock());
            if work.is_empty() {
                return;
            }
            if self.order == RunOrder::Lifo {
                work.reverse();
            }
            for operation in work {
                operation();
            }
        }
    }
}

impl DeferredSpawner {
    /// Drops all queued-but-never-run work. `spawn_effect` closures
    /// capture the spawner (to promote queued adoption work), so a
    /// queue-based spawner holds a closure→spawner→queue Arc cycle;
    /// clearing the queue breaks it and releases the backend.
    pub fn clear(&self) {
        self.work.lock().clear();
    }
}

impl BlockingEffectSpawner for DeferredSpawner {
    type SpawnError = Infallible;

    fn spawn_blocking(&self, work: BlockingEffectWork) -> Result<(), Self::SpawnError> {
        self.work.lock().push(work);
        Ok(())
    }
}

// ── harness ───────────────────────────────────────────────────────────

pub const SETTLE_LIMIT: usize = 1_000;
pub const FAIR_ROUNDS: usize = 64;
pub const FAIR_TICK: Duration = Duration::from_secs(3_600);

pub struct Harness<B: HarnessedBackend> {
    pub driver: ClusterDriver,
    pub executor: IdempotentEffectExecutor<B, DeferredSpawner>,
    pub backend: B,
    spawner: DeferredSpawner,
    now: SystemTime,
    seed: u64,
    trace: Vec<Input>,
    desired_expanded: BTreeMap<LogicalNodeId, LogicalNodeSpec>,
    last_observed_generation: Option<u64>,
}

impl<B: HarnessedBackend> Harness<B> {
    pub fn new_with_backend(seed: u64, initial: ClusterShape, backend: B) -> Self {
        Self::new_ordered(seed, initial, backend, RunOrder::Fifo)
    }

    pub fn new_ordered(seed: u64, initial: ClusterShape, backend: B, order: RunOrder) -> Self {
        let desired_expanded = initial.expand().expect("initial shape expands");
        let spawner = DeferredSpawner {
            work: Arc::new(Mutex::new(Vec::new())),
            order,
        };
        Self {
            driver: ClusterDriver::new(initial, RetryPolicy::default()).unwrap(),
            executor: IdempotentEffectExecutor::new(backend.clone(), spawner.clone()),
            backend,
            spawner,
            now: UNIX_EPOCH,
            seed,
            trace: Vec::new(),
            desired_expanded,
            last_observed_generation: None,
        }
    }

    pub fn step(&mut self, input: Input) {
        self.trace.push(input.clone());
        match input {
            Input::Tick(duration) => {
                self.now = self
                    .now
                    .checked_add(duration)
                    .expect("harness clock overflow");
            }
            Input::Shape(shape) => {
                let expanded = shape.expand().expect("generator produced an invalid shape");
                self.driver
                    .update_desired(shape)
                    .expect("generator produced an illegal shape");
                self.desired_expanded = expanded;
            }
            Input::Reply(reply) => self.backend.script(reply),
            Input::Run => self.spawner.run_all(),
            Input::Boot(event) => {
                self.deliver_boot(event);
            }
        }
        self.settle();
        self.check();
    }

    pub fn settle(&mut self) {
        for iteration in 0..SETTLE_LIMIT {
            let mut progress = false;
            let results = self.executor.drain_results();
            for result in results {
                self.driver.apply_executor_result(result, self.now);
                progress = true;
            }
            for operation in self.driver.pending_operations_due(self.now) {
                if self
                    .executor
                    .expire(operation.operation, "deadline elapsed in harness")
                {
                    progress = true;
                }
            }
            if self.driver.trigger_if_due(self.now) {
                progress = true;
            }
            let submitted = self
                .driver
                .drive_until_blocked(self.now, &mut self.executor)
                .expect("drive failed in harness");
            progress |= submitted > 0;
            if !progress {
                break;
            }
            if iteration + 1 == SETTLE_LIMIT {
                self.fail(&format!(
                    "machine did not reach quiescence within {SETTLE_LIMIT} settle iterations (livelock)"
                ));
            }
        }
        self.after_settle();
    }

    /// Machine-level guarantees, checked at every quiescent point.
    fn after_settle(&mut self) {
        // No orphaned external work: results are drained at quiescence.
        if !self.executor.drain_results().is_empty() {
            self.fail("quiescent machine left executor results undrained");
        }
        // Monotonic generation.
        let generation = self.driver.state().observed_generation;
        if let Some(previous) = self.last_observed_generation
            && generation < previous
        {
            self.fail("observed_generation regressed");
        }
        self.last_observed_generation = Some(generation);
        // Pass idempotency and quiescence: a forced pass over a
        // quiescent machine dispatches nothing, mutates nothing, and
        // keeps the requeue deadline; a converged machine has no
        // requeue deadline at all.
        let before = self.driver.state().clone();
        let requeue_before = self.driver.requeue_at();
        self.driver.trigger();
        let submitted = self
            .driver
            .drive_next(self.now, &mut self.executor)
            .expect("forced pass failed");
        if submitted != 0 {
            self.fail("quiescent machine dispatched effects on a forced pass");
        }
        if *self.driver.state() != before {
            self.fail("forced pass mutated quiescent state");
        }
        if self.driver.requeue_at() != requeue_before {
            self.fail("forced pass changed the requeue deadline");
        }
        if self.driver.is_converged() {
            if self.driver.requeue_at().is_some() {
                self.fail("converged machine scheduled a requeue");
            }
            // Resource conservation: convergence owns every live resource.
            let leaked = self.backend.leaked();
            if !leaked.is_empty() {
                self.fail(&format!("converged machine leaked resources: {leaked:?}"));
            }
        }
    }

    fn deliver_boot(&mut self, event: BootEvent) {
        let targets: Vec<(LogicalNodeId, NodeAttemptId, BootstrapSessionId)> = self
            .driver
            .state()
            .nodes
            .iter()
            .filter_map(|(id, node)| {
                node.active_bootstrap
                    .map(|session| (id.clone(), node.attempt, session))
            })
            .collect();
        for (id, attempt, session) in targets {
            let observation = match event {
                BootEvent::Joined => NodeObservation::SwactorJoined {
                    session_id: session,
                    swactor_id: SwactorId(format!("sw-{}-{}", id.0, attempt.0)),
                },
                BootEvent::Closed => NodeObservation::BootstrapClosed {
                    session_id: session,
                },
                BootEvent::Failed => NodeObservation::BootstrapFailed {
                    session_id: session,
                    reason: "scripted bootstrap failure".to_owned(),
                },
            };
            self.driver
                .apply_observation(&id, attempt, observation, self.now);
        }
    }

    /// Fair scheduler: stop injecting faults, deliver bootstrap
    /// completion, run dispatched work, advance past every deadline.
    /// A healthy machine must converge within `FAIR_ROUNDS` rounds and
    /// must not exceed a bounded replacement-attempt budget (unbounded
    /// replacement under a fault-free tail is an infinite retry loop).
    pub fn fair_tail(&mut self) {
        self.backend.heal();
        let attempts_before = self.driver.state().next_attempt_id;
        let budget = (self.driver.state().nodes.len() as u64).saturating_mul(2) + 2;
        for round in 0..FAIR_ROUNDS {
            self.deliver_boot(BootEvent::Joined);
            self.settle();
            self.deliver_boot(BootEvent::Closed);
            self.settle();
            self.spawner.run_all();
            self.now = self
                .now
                .checked_add(FAIR_TICK)
                .expect("harness clock overflow");
            self.settle();
            self.check();
            if self.driver.is_converged() {
                return;
            }
            let allocated = self.driver.state().next_attempt_id - attempts_before;
            if allocated > budget {
                self.fail(&format!(
                    "fair tail allocated {allocated} replacement attempts (budget {budget}): unbounded retry"
                ));
            }
            if round + 1 == FAIR_ROUNDS {
                self.fail(&format!(
                    "fair trace did not converge within {FAIR_ROUNDS} rounds"
                ));
            }
        }
    }

    fn check(&self) {
        if let Err(violation) = check_invariants(self.driver.state(), &self.desired_expanded) {
            self.fail(&format!("invariant violated: {violation}"));
        }
    }

    fn fail(&self, message: &str) -> ! {
        panic!(
            "\n[{message}]\nseed: {}\ntrace ({} inputs):\n{}",
            self.seed,
            self.trace.len(),
            describe_trace(&self.trace),
        );
    }

    pub fn state(&self) -> &ClusterState {
        self.driver.state()
    }
}

impl<B: HarnessedBackend> Drop for Harness<B> {
    fn drop(&mut self) {
        // Break the spawner's closure cycle for never-run work so the
        // backend (and anything it owns, e.g. child processes) releases.
        self.spawner.clear();
    }
}

// ── deterministic generator ───────────────────────────────────────────

pub struct Rng(u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

pub struct GenCtx {
    generation: u64,
    role_alt: bool,
}

fn gen_input(ctx: &mut GenCtx, rng: &mut Rng) -> Input {
    match rng.next() % 100 {
        0..=39 => Input::Tick(Duration::from_secs(
            [1, 10, 61, 130, 3_600][(rng.next() % 5) as usize],
        )),
        40..=61 => Input::Run,
        62..=79 => {
            let reply = match rng.next() % 100 {
                0..=59 => Reply::Succeed,
                60..=74 => Reply::NoEndpoint,
                75..=84 => Reply::Definite("scripted definite failure"),
                85..=94 => Reply::Ambiguous("scripted ambiguous failure"),
                _ => Reply::Panic,
            };
            Input::Reply(reply)
        }
        80..=86 => Input::Boot(match rng.next() % 3 {
            0 => BootEvent::Joined,
            1 => BootEvent::Closed,
            _ => BootEvent::Failed,
        }),
        _ => {
            ctx.generation += 1;
            ctx.role_alt ^= rng.next().is_multiple_of(2);
            let count = (rng.next() % 4) as u32;
            let role = if ctx.role_alt { "worker-alt" } else { "worker" };
            Input::Shape(shape(
                ctx.generation,
                vec![group_with_role("g0", count, role)],
            ))
        }
    }
}

pub fn gen_trace(seed: u64, len: usize) -> Vec<Input> {
    let mut rng = Rng(seed);
    let mut ctx = GenCtx {
        generation: 1,
        role_alt: false,
    };
    (0..len).map(|_| gen_input(&mut ctx, &mut rng)).collect()
}

/// Replaces scripted faults with `Succeed`, for metamorphic comparisons
/// where outcome assignment must not depend on execution order.
pub fn sanitized(trace: &[Input]) -> Vec<Input> {
    trace
        .iter()
        .map(|input| match input {
            Input::Reply(_) => Input::Reply(Reply::Succeed),
            other => other.clone(),
        })
        .collect()
}

// ── execution and shrinking ───────────────────────────────────────────

pub fn run_trace_with<B: HarnessedBackend>(
    make: impl Fn() -> B,
    seed: u64,
    trace: &[Input],
    fair: bool,
    order: RunOrder,
) -> Harness<B> {
    let mut harness = Harness::new_ordered(seed, shape(1, vec![group("g0", 1)]), make(), order);
    for input in trace {
        harness.step(input.clone());
    }
    if fair {
        harness.fair_tail();
        if !harness.driver.is_converged() {
            harness.fail("fair trace did not converge");
        }
    }
    harness
}

pub fn run_trace(seed: u64, trace: &[Input], fair: bool, order: RunOrder) -> Harness<FakeBackend> {
    run_trace_with(FakeBackend::default, seed, trace, fair, order)
}

fn still_fails<B: HarnessedBackend>(
    make: impl Fn() -> B + Clone,
    seed: u64,
    trace: &[Input],
    fair: bool,
) -> bool {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_trace_with(make.clone(), seed, trace, fair, RunOrder::Fifo)
    }))
    .is_err()
}

/// Naive shrinker: halve suffixes, then drop single inputs until a
/// fixpoint. Re-runs with the same seed; legality of remaining shapes is
/// preserved because generations are strictly increasing as data.
fn shrink<B: HarnessedBackend>(
    make: impl Fn() -> B + Clone,
    seed: u64,
    trace: &[Input],
    fair: bool,
) -> Vec<Input> {
    let mut current = trace.to_vec();
    loop {
        let mut reduced = false;
        while current.len() > 1 {
            let half = current[..current.len() / 2].to_vec();
            if still_fails(make.clone(), seed, &half, fair) {
                current = half;
                reduced = true;
            } else {
                break;
            }
        }
        for index in 0..current.len() {
            let mut candidate = current.clone();
            candidate.remove(index);
            if still_fails(make.clone(), seed, &candidate, fair) {
                current = candidate;
                reduced = true;
                break;
            }
        }
        if !reduced {
            return current;
        }
    }
}

/// Runs a trace, shrinking and reporting on failure. Used by the seeded
/// loops; the default panic hook is silenced while shrinking.
pub fn assert_trace<B: HarnessedBackend>(
    make: impl Fn() -> B + Clone,
    seed: u64,
    trace: &[Input],
    fair: bool,
) {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_trace_with(make.clone(), seed, trace, fair, RunOrder::Fifo)
    }));
    if outcome.is_ok() {
        return;
    }
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let minimal = shrink(make, seed, trace, fair);
    std::panic::set_hook(previous_hook);
    panic!(
        "\nseed: {seed}\nminimal failing trace ({} inputs):\n{}",
        minimal.len(),
        describe_trace(&minimal),
    );
}

/// The full battery: adversarial invariants plus fair convergence, over
/// a fresh backend per run.
pub fn run_trace_battery<B: HarnessedBackend>(
    make: impl Fn() -> B + Clone,
    seeds: u64,
    len: usize,
) {
    for seed in 0..seeds {
        let trace = gen_trace(seed, len);
        assert_trace(make.clone(), seed, &trace, false);
        assert_trace(make.clone(), seed, &trace, true);
    }
}

// ── plugin contracts ──────────────────────────────────────────────────

/// Seam-level contracts every `TestablePlugin` must satisfy, checked by
/// direct plugin calls (no driver involved):
/// - create is idempotent per attempt (retry adopts, never re-creates),
/// - a definite failure creates nothing,
/// - an ambiguous failure creates the resource; retry adopts it,
/// - stop releases the resource (nothing leaks),
/// - bootstrap lifecycle calls succeed on a live handle.
pub fn assert_plugin_contracts<P: TestablePlugin>(plugin: &mut P) {
    let sink = null_sink();
    let before = plugin.resources_created();

    // Create twice for the same attempt: one resource, same handle.
    let first = plugin
        .create_node(plugin_spec(11), sink.clone())
        .expect("create succeeds");
    let second = plugin
        .create_node(plugin_spec(11), sink.clone())
        .expect("create adopts");
    assert_eq!(first.id, second.id, "retry must adopt, not re-create");
    assert_eq!(
        plugin.resources_created(),
        before + 1,
        "adoption must not create a second resource"
    );

    // Definite failure creates nothing.
    plugin.apply_fault(Fault::Definite);
    let failed = plugin.create_node(plugin_spec(12), sink.clone());
    assert!(failed.is_err(), "definite fault must fail");
    assert_eq!(
        plugin.resources_created(),
        before + 1,
        "definite failure must not create a resource"
    );
    plugin.apply_fault(Fault::Heal);

    // Ambiguous failure creates the resource; retry adopts it.
    plugin.apply_fault(Fault::Ambiguous);
    let ambiguous = plugin.create_node(plugin_spec(13), sink.clone());
    assert!(ambiguous.is_err(), "ambiguous fault must fail");
    assert_eq!(
        plugin.resources_created(),
        before + 2,
        "ambiguous failure must have created the resource"
    );
    plugin.apply_fault(Fault::Heal);
    let adopted = plugin
        .create_node(plugin_spec(13), sink.clone())
        .expect("retry after ambiguity must adopt");
    assert_eq!(
        plugin.resources_created(),
        before + 2,
        "adoption must not create a second resource"
    );
    assert!(
        plugin.leaked_resources(&[first.id, adopted.id]).is_empty(),
        "owned resources are not leaks"
    );

    // Bootstrap lifecycle on a live handle.
    plugin
        .start_bootstrap(&first)
        .expect("start_bootstrap succeeds");
    plugin
        .complete_bootstrap(&first)
        .expect("complete_bootstrap succeeds");
    plugin
        .cancel_bootstrap(&first)
        .expect("cancel_bootstrap succeeds");

    // Stop releases everything.
    plugin.stop_node(&first).expect("stop succeeds");
    plugin.stop_node(&adopted).expect("stop succeeds");
    assert!(
        plugin.leaked_resources(&[]).is_empty(),
        "stopped resources must be released"
    );
}
