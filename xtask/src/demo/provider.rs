//! The demo provider seam: real node-role children behind the reconciler's
//! effect executor.
//!
//! `DemoProvider` is the demo's `ProvisionPlugin` — create spawns a
//! `swactor-process` actor whose child re-execs this binary in node role
//! (`--demo-node`), stop goes through the real process-actor command path.
//! `DemoBackend` adapts the plugin to `NodeManagerCommand` effects, mirroring
//! the conformance kit's adapter semantics (attempt-keyed identity, adoption
//! on retry).
//!
//! Actor spawning needs an actor context, so spawn requests travel over a
//! channel to the supervisor actor; the plugin call blocks for the reply on
//! an executor blocking thread.
//!
//! Children run with stdio null, so the supervisor learns a node's iroh
//! identity and liveness from the wire announce (see `node.rs`): the
//! [`AnnounceActor`] routes it to the owning bootstrap actor and records
//! `last_announce_ms` in the [`NodeManager`] registry.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::ExternalSender;
use swactor_engine::ActorCompletion;
use swactor_process::{ExitStatus, ProcessOutput};

use provisioning::executor::{EffectBackend, EffectError};
use provisioning::node::{
    BootstrapSessionId, CreateLeaseResult, DestroyHandle, LeaseFacts, NodeManagerCommand,
    ProviderKind, ProviderLeaseId, SshEndpoint,
};
use provisioning::plugin::{NodeProvisionSpec, PluginNodeHandle, PluginSink, ProvisionPlugin};
use provisioning::reconciler::{OperationId, OperationOutcome, PlannedEffect};
use telemetry::{ChannelContent, StreamDescriptor, TelemetryEndpoint, TelemetryProducer};

/// One provisioned node's runtime facts.
#[derive(Clone)]
pub struct NodeRuntime {
    pub attempt: u64,
    pub logical_node: String,
    /// The bootstrap actor that owns this attempt's lifecycle.
    pub bootstrap: ActorAddress,
    pub pid: Option<u32>,
    pub exited: Option<ExitStatus>,
    pub spawn_failed: Option<String>,
    /// Wall-clock ms of the last wire announce from the node (None until
    /// the first announce). The node re-announces every heartbeat period,
    /// so staleness here means the control-plane path is dead.
    pub last_announce_ms: Option<u64>,
    /// Serde-serialized `iroh::EndpointAddr` from the node's announce —
    /// what the supervisor dials for telemetry pulls and demo edges.
    pub endpoint_addr: Option<String>,
}

/// Spawn request from the plugin (blocking thread) to the supervisor actor.
#[derive(Clone)]
pub struct SpawnNodeRequest {
    pub attempt: u64,
    pub logical_node: String,
    pub reply: ActorCompletion<Result<NodeRuntime, String>>,
}

/// Shared node registry and actor route for provisioning requests.
#[derive(Clone, Default)]
pub struct NodeManager {
    inner: Arc<Mutex<NodeManagerInner>>,
}

#[derive(Clone)]
struct SpawnRoute {
    sender: ExternalSender,
    supervisor: Arc<std::sync::OnceLock<ActorAddress>>,
}

#[derive(Default)]
struct NodeManagerInner {
    nodes: BTreeMap<u64, NodeRuntime>,
    spawn_route: Option<SpawnRoute>,
    exit_waiters: BTreeMap<u64, Vec<ActorCompletion<()>>>,
}

impl NodeManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_spawn_actor(
        &self,
        sender: ExternalSender,
        supervisor: Arc<std::sync::OnceLock<ActorAddress>>,
    ) {
        self.inner.lock().expect("node manager").spawn_route =
            Some(SpawnRoute { sender, supervisor });
    }

    pub fn request_spawn(&self, attempt: u64, logical_node: String) -> Result<NodeRuntime, String> {
        let reply = ActorCompletion::new();
        let route = self
            .inner
            .lock()
            .expect("node manager")
            .spawn_route
            .clone()
            .ok_or_else(|| "supervisor actor route not installed".to_owned())?;
        let supervisor = route
            .supervisor
            .get()
            .copied()
            .ok_or_else(|| "supervisor actor not ready".to_owned())?;
        route
            .sender
            .send_to(
                supervisor,
                crate::demo::feed::SupervisorMsg::Spawn(SpawnNodeRequest {
                    attempt,
                    logical_node,
                    reply: reply.clone(),
                }),
            )
            .map_err(|_| "supervisor actor gone".to_owned())?;
        reply.wait()
    }

    pub fn register(&self, runtime: NodeRuntime) {
        self.inner
            .lock()
            .expect("node manager")
            .nodes
            .insert(runtime.attempt, runtime);
    }

    pub fn get(&self, attempt: u64) -> Option<NodeRuntime> {
        self.inner
            .lock()
            .expect("node manager")
            .nodes
            .get(&attempt)
            .cloned()
    }

    /// All registered runtimes (snapshot for liveness sweeps).
    pub fn nodes(&self) -> Vec<NodeRuntime> {
        self.inner
            .lock()
            .expect("node manager")
            .nodes
            .values()
            .cloned()
            .collect()
    }

    pub fn find_by_stream_node(&self, node: &str) -> Option<NodeRuntime> {
        self.inner
            .lock()
            .expect("node manager")
            .nodes
            .values()
            .find(|runtime| runtime.logical_node == node)
            .cloned()
    }

    fn update(&self, attempt: u64, mutate: impl FnOnce(&mut NodeRuntime)) {
        if let Some(runtime) = self
            .inner
            .lock()
            .expect("node manager")
            .nodes
            .get_mut(&attempt)
        {
            mutate(runtime);
        }
    }

    pub fn set_pid(&self, attempt: u64, pid: u32) {
        self.update(attempt, |runtime| runtime.pid = Some(pid));
    }

    pub fn set_exited(&self, attempt: u64, status: ExitStatus) {
        let waiters = {
            let mut inner = self.inner.lock().expect("node manager");
            if let Some(runtime) = inner.nodes.get_mut(&attempt) {
                runtime.exited = Some(status);
            }
            inner.exit_waiters.remove(&attempt).unwrap_or_default()
        };
        for waiter in waiters {
            let _ = waiter.complete(());
        }
    }

    pub fn watch_exit(&self, attempt: u64) -> ActorCompletion<()> {
        let completion = ActorCompletion::new();
        let already_exited = {
            let mut inner = self.inner.lock().expect("node manager");
            if inner
                .nodes
                .get(&attempt)
                .is_some_and(|runtime| runtime.exited.is_some())
            {
                true
            } else {
                inner
                    .exit_waiters
                    .entry(attempt)
                    .or_default()
                    .push(completion.clone());
                false
            }
        };
        if already_exited {
            let _ = completion.complete(());
        }
        completion
    }

    pub fn set_spawn_failed(&self, attempt: u64, reason: String) {
        self.update(attempt, |runtime| runtime.spawn_failed = Some(reason));
    }

    pub fn set_announce(&self, attempt: u64, at_ms: u64) {
        self.update(attempt, |runtime| runtime.last_announce_ms = Some(at_ms));
    }

    pub fn set_endpoint(&self, attempt: u64, endpoint_addr_json: String) {
        self.update(attempt, |runtime| {
            runtime.endpoint_addr = Some(endpoint_addr_json)
        });
    }
    pub fn remove(&self, attempt: u64) {
        self.inner
            .lock()
            .expect("node manager")
            .nodes
            .remove(&attempt);
    }
}

/// The demo `ProvisionPlugin`: resources are node-role child processes.
pub struct DemoProvider {
    manager: NodeManager,
}

impl DemoProvider {
    pub fn new(manager: NodeManager) -> Self {
        Self { manager }
    }
}

impl ProvisionPlugin for DemoProvider {
    fn create_node(
        &mut self,
        spec: NodeProvisionSpec,
        _sink: PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        let attempt = spec.attempt_id;
        if let Some(runtime) = self.manager.get(attempt) {
            // Adoption: the child for this attempt already exists.
            return Ok(PluginNodeHandle {
                id: attempt,
                provider_process_id: runtime.pid,
            });
        }
        let logical_node = spec
            .env
            .iter()
            .find(|(key, _)| key == "DEMO_LOGICAL_NODE")
            .map(|(_, value)| value.clone())
            .ok_or_else(|| "spec missing DEMO_LOGICAL_NODE".to_owned())?;
        let runtime = self.manager.request_spawn(attempt, logical_node)?;
        Ok(PluginNodeHandle {
            id: attempt,
            provider_process_id: runtime.pid,
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
        // The backend path sends the process Stop command before calling
        // here; direct plugin-level stop just drops the registration.
        self.manager.remove(handle.id);
        Ok(())
    }
}

/// Per-node telemetry: one endpoint/producer per provisioned node so each
/// lands on its own dashboard stream (one fleet card per node). Carries the
/// stream descriptor's origin/label so the dashboard can classify the stream
/// (the frame path itself has no catalog).
pub struct NodeTelemetry {
    pub endpoint: TelemetryEndpoint,
    pub producer: TelemetryProducer,
    pub origin: &'static str,
    pub label: String,
}

impl NodeTelemetry {
    pub fn new(logical_node: &str, life: u64) -> Self {
        let stream = telemetry::frame::StreamId::new(
            telemetry::frame::NodeId::new(logical_node),
            telemetry::frame::Lifetime(life),
        );
        let endpoint = TelemetryEndpoint::with_descriptor(
            StreamDescriptor {
                stream,
                label: Some(format!("demo node {logical_node}")),
                origin: telemetry::frame::StreamOrigin::RemoteNode,
            },
            256,
            16,
        );
        let producer = endpoint.producer();
        Self {
            endpoint,
            producer,
            origin: "remote_node",
            label: format!("demo node {logical_node}"),
        }
    }
}

/// Relay actor: folds process-actor lifecycle reports into the registry.
pub struct NodeRelayActor {
    manager: NodeManager,
    attempt: u64,
}

impl NodeRelayActor {
    pub fn new(manager: NodeManager, attempt: u64) -> Self {
        Self { manager, attempt }
    }
}

impl ActorInterface for NodeRelayActor {
    type Incoming = ProcessOutput;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, output: ProcessOutput) {
        match output {
            ProcessOutput::Stdout(_) | ProcessOutput::Stderr(_) => {}
            ProcessOutput::Started { pid } => self.manager.set_pid(self.attempt, pid),
            ProcessOutput::Exited { status } => self.manager.set_exited(self.attempt, status),
            ProcessOutput::SpawnFailed { error } => {
                eprintln!("demo node {}: spawn failed: {error}", self.attempt);
                self.manager
                    .set_spawn_failed(self.attempt, error.to_string());
            }
            ProcessOutput::Error { error } => {
                eprintln!("demo node {}: process actor error: {error}", self.attempt);
            }
        }
    }
}

/// Supervisor-side announce relay. The iroh actor bridge decodes inbound
/// [`NodeAnnounce`] gossip frames and routes them here by wire tag; this
/// actor correlates by attempt token and forwards to the owning bootstrap
/// actor ([`provisioning::BootstrapMsg::Announce`]) — the handoff from
/// bootstrap to control plane. Announces for unknown attempts (deregistered
/// lease, stale container from a dead attempt) are dropped with a log line.
pub struct AnnounceActor {
    manager: NodeManager,
    sender: ExternalSender,
}

impl AnnounceActor {
    pub fn new(manager: NodeManager, sender: ExternalSender) -> Self {
        Self { manager, sender }
    }
}

impl ActorInterface for AnnounceActor {
    type Incoming = crate::demo::node::NodeAnnounce;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, announce: crate::demo::node::NodeAnnounce) {
        self.manager.set_announce(announce.attempt, announce.at_ms);
        self.manager
            .set_endpoint(announce.attempt, announce.endpoint_addr_json.clone());
        let Some(runtime) = self.manager.get(announce.attempt) else {
            let key = &announce.key_hex;
            eprintln!(
                "demo: announce for unknown attempt {} (key {}…) dropped",
                announce.attempt,
                &key[..8.min(key.len())]
            );
            return;
        };
        let identity = provisioning::NodeIdentity {
            attempt: announce.attempt,
            logical_node: announce.logical_node,
            key_hex: announce.key_hex,
            transport_addr: announce.endpoint_addr_json,
        };
        let _ = self.sender.send_to(
            runtime.bootstrap,
            provisioning::BootstrapMsg::Announce(identity),
        );
    }
}

/// Demo lease/session identity (attempt-encoded, kit conventions).
pub fn demo_lease(attempt: u64) -> LeaseFacts {
    let provider = ProviderKind::new("demo");
    let lease_id = ProviderLeaseId(format!("demo-lease-{attempt}"));
    LeaseFacts {
        destroy_handle: DestroyHandle {
            provider: provider.clone(),
            lease_id: lease_id.clone(),
            provider_contract_id: format!("demo-contract-{attempt}"),
        },
        provider,
        lease_id,
        provider_contract_id: format!("demo-contract-{attempt}"),
        offer_id: None,
        provider_metadata: BTreeMap::new(),
    }
}

pub fn demo_endpoint() -> SshEndpoint {
    SshEndpoint {
        host: "127.0.0.1".to_owned(),
        port: 0,
        user: "demo".to_owned(),
        auth_ref: "demo-key".to_owned(),
    }
}

/// Session identity: one bootstrap session per attempt (production
/// convention — the supervisor's close path addresses it the same way).
pub fn session_id_for(operation: OperationId) -> BootstrapSessionId {
    BootstrapSessionId(operation.attempt.0)
}

/// The demo `EffectBackend`: routes effects through the plugin.
pub struct DemoBackend {
    /// The plugin lives behind a mutex: `EffectBackend::execute` is `&self`
    /// while `ProvisionPlugin` methods are `&mut self` (kit adapter shape).
    pub plugin: Arc<Mutex<DemoProvider>>,
    pub manager: NodeManager,
    pub sender: ExternalSender,
}

impl DemoBackend {
    fn handle_for(&self, attempt: u64) -> PluginNodeHandle {
        let pid = self.manager.get(attempt).and_then(|r| r.pid);
        PluginNodeHandle {
            id: attempt,
            provider_process_id: pid,
        }
    }
}

impl EffectBackend for DemoBackend {
    fn execute(&self, effect: &PlannedEffect) -> Result<OperationOutcome, EffectError> {
        let attempt = effect.operation.attempt.0;
        match &effect.command {
            NodeManagerCommand::CreateLease(request) => {
                let spec = NodeProvisionSpec {
                    run_id: request.spec.run_id.0,
                    node_id: attempt,
                    attempt_id: attempt,
                    stage_index: None,
                    image: request.spec.shape.image.clone(),
                    env: vec![(
                        "DEMO_LOGICAL_NODE".to_owned(),
                        request.spec.logical_node_id.0.clone(),
                    )],
                    args: Vec::new(),
                    mounts: Vec::new(),
                };
                self.plugin
                    .lock()
                    .expect("demo provider")
                    .create_node(spec, null_sink())
                    .map_err(EffectError::definite)?;
                Ok(OperationOutcome::LeaseCreated(Box::new(
                    CreateLeaseResult {
                        lease: demo_lease(attempt),
                        endpoint: Some(demo_endpoint()),
                    },
                )))
            }
            NodeManagerCommand::LookupEndpoint(_) => {
                Ok(OperationOutcome::EndpointLookup(Some(demo_endpoint())))
            }
            NodeManagerCommand::StartBootstrap(_) => {
                let handle = self.handle_for(attempt);
                self.plugin
                    .lock()
                    .expect("demo provider")
                    .start_bootstrap(&handle)
                    .map_err(EffectError::definite)?;
                Ok(OperationOutcome::BootstrapStarted {
                    session_id: session_id_for(effect.operation),
                })
            }
            NodeManagerCommand::BootstrapConvergenceObserved { .. } => {
                let handle = self.handle_for(attempt);
                self.plugin
                    .lock()
                    .expect("demo provider")
                    .complete_bootstrap(&handle)
                    .map_err(EffectError::definite)?;
                Ok(OperationOutcome::BootstrapConvergenceAccepted)
            }
            NodeManagerCommand::CancelBootstrap { .. } => {
                let handle = self.handle_for(attempt);
                self.plugin
                    .lock()
                    .expect("demo provider")
                    .cancel_bootstrap(&handle)
                    .map_err(EffectError::definite)?;
                Ok(OperationOutcome::BootstrapCancelled)
            }
            NodeManagerCommand::DestroyLease(_) => {
                let handle = self.handle_for(attempt);
                // stop_node needs the sender; route it explicitly.
                stop_node_with_sender(&self.plugin, &self.manager, &handle, &self.sender)
                    .map_err(EffectError::definite)?;
                Ok(OperationOutcome::LeaseDestroyed)
            }
        }
    }
}

fn stop_node_with_sender(
    plugin: &Arc<Mutex<DemoProvider>>,
    manager: &NodeManager,
    handle: &PluginNodeHandle,
    sender: &ExternalSender,
) -> Result<(), String> {
    if let Some(runtime) = manager.get(handle.id)
        && runtime.pid.is_some()
        && runtime.exited.is_none()
    {
        let exited = manager.watch_exit(handle.id);
        sender
            .send_to(
                runtime.bootstrap,
                provisioning::BootstrapMsg::Stop {
                    kill_after: Some(Duration::from_secs(1)),
                },
            )
            .map_err(|_| "bootstrap actor stopped before process shutdown".to_owned())?;
        exited.wait();
    }
    plugin.lock().expect("demo provider").stop_node(handle)
}

fn null_sink() -> PluginSink {
    struct NullSink;
    impl provisioning::plugin::PluginObservationSink for NullSink {
        fn observe(&self, _observation: provisioning::plugin::PluginObservation) {}
    }
    PluginSink::new(Arc::new(NullSink))
}

/// Register per-node supplementary channels; returns the `node.status`
/// channel id for the supervisor's liveness heartbeats.
pub fn register_node_channels(producer: &TelemetryProducer) -> telemetry::ChannelId {
    producer.register_channel(
        "node.status",
        ChannelContent::JsonRecord {
            schema: Some("demo.node.status.v1".to_owned()),
        },
    )
}

pub fn unix_ms(now: SystemTime) -> u64 {
    now.duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod properties {
    use proptest::prelude::*;
    use swactor::config::RuntimeConfig;
    use swactor::runtime::RuntimeParts;
    use swactor_engine::{Engine, SteppingBackend};

    use super::*;

    #[derive(Clone, Debug)]
    enum RelayAction {
        Started(u16),
        ExitedCode(u8),
        ExitedSignal(u8),
        SpawnFailed(u8),
        Error,
    }

    fn relay_actions() -> impl Strategy<Value = Vec<RelayAction>> {
        prop::collection::vec(
            prop_oneof![
                3 => any::<u16>().prop_map(RelayAction::Started),
                2 => any::<u8>().prop_map(RelayAction::ExitedCode),
                2 => any::<u8>().prop_map(RelayAction::ExitedSignal),
                1 => any::<u8>().prop_map(RelayAction::SpawnFailed),
                1 => Just(RelayAction::Error),
            ],
            0..=32,
        )
    }

    fn output(action: &RelayAction) -> ProcessOutput {
        match *action {
            RelayAction::Started(pid) => ProcessOutput::Started {
                pid: u32::from(pid) + 1,
            },
            RelayAction::ExitedCode(code) => ProcessOutput::Exited {
                status: ExitStatus::Code(i32::from(code)),
            },
            RelayAction::ExitedSignal(signal) => ProcessOutput::Exited {
                status: ExitStatus::Signal(i32::from(signal) + 1),
            },
            RelayAction::SpawnFailed(code) => ProcessOutput::SpawnFailed {
                error: format!("spawn-{code}"),
            },
            RelayAction::Error => ProcessOutput::Error {
                error: "scripted process error".to_owned(),
            },
        }
    }

    fn drive(backend: &SteppingBackend) {
        for _ in 0..64 {
            backend.step();
        }
    }

    fn check_relay_invariants(
        observed: &NodeRuntime,
        expected_pid: Option<u32>,
        expected_exit: &Option<ExitStatus>,
        expected_spawn_failure: &Option<String>,
        final_actors: usize,
        worker_panics: u64,
    ) -> Result<(), String> {
        if observed.pid != expected_pid {
            return Err(format!(
                "pid mismatch: observed={:?} expected={expected_pid:?}",
                observed.pid
            ));
        }
        if observed.exited.as_ref() != expected_exit.as_ref() {
            return Err(format!(
                "exit mismatch: observed={:?} expected={expected_exit:?}",
                observed.exited
            ));
        }
        if observed.spawn_failed.as_ref() != expected_spawn_failure.as_ref() {
            return Err(format!(
                "spawn failure mismatch: observed={:?} expected={expected_spawn_failure:?}",
                observed.spawn_failed
            ));
        }
        if final_actors != 0 {
            return Err(format!(
                "process relay did not return to baseline: observed={final_actors}"
            ));
        }
        if worker_panics != 0 {
            return Err(format!(
                "process relay worker panicked {worker_panics} time(s)"
            ));
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            max_shrink_iters: 2_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn generated_process_reports_complete_exit_watchers_once_and_preserve_last_state(
            mut actions in relay_actions(),
            watcher_count in 0_usize..=8,
            fallback_exit in any::<u8>(),
        ) {
            if !actions.iter().any(|action| {
                matches!(action, RelayAction::ExitedCode(_) | RelayAction::ExitedSignal(_))
            }) {
                let terminal = RelayAction::ExitedCode(fallback_exit);
                if let Some(last) = actions.get_mut(31) {
                    *last = terminal;
                } else {
                    actions.push(terminal);
                }
            }
            let parts = RuntimeParts::new(RuntimeConfig {
                worker_count: 1,
                ..RuntimeConfig::default()
            });
            let runtime = parts.runtime().clone();
            let backend = SteppingBackend::new();
            let _engine =
                Engine::new(parts, backend.clone()).expect("demo process stepping engine");
            let manager = NodeManager::new();
            let attempt = 41;
            manager.register(NodeRuntime {
                attempt,
                logical_node: "generated-node".to_owned(),
                bootstrap: ActorAddress::default(),
                pid: None,
                exited: None,
                spawn_failed: None,
                last_announce_ms: None,
                endpoint_addr: None,
            });
            let relay = runtime
                .spawn(NodeRelayActor::new(manager.clone(), attempt))
                .expect("spawn demo process relay");
            let watchers = (0..watcher_count)
                .map(|_| manager.watch_exit(attempt))
                .collect::<Vec<_>>();

            let mut expected_pid = None;
            let mut expected_exit = None;
            let mut expected_spawn_failure = None;
            for action in &actions {
                match action {
                    RelayAction::Started(pid) => expected_pid = Some(u32::from(*pid) + 1),
                    RelayAction::ExitedCode(code) => {
                        expected_exit = Some(ExitStatus::Code(i32::from(*code)));
                    }
                    RelayAction::ExitedSignal(signal) => {
                        expected_exit = Some(ExitStatus::Signal(i32::from(*signal) + 1));
                    }
                    RelayAction::SpawnFailed(code) => {
                        expected_spawn_failure = Some(format!("spawn-{code}"));
                    }
                    RelayAction::Error => {}
                }
                runtime
                    .send_to(relay, output(action))
                    .expect("send demo process report");
            }
            drive(&backend);

            for watcher in watchers {
                prop_assert!(
                    watcher.complete(()).is_err(),
                    "exit watcher remained pending after fixed drive budget; actions={actions:?} \
                     census={:?}",
                    runtime.stats()
                );
                watcher.wait();
            }
            let late_watcher = manager.watch_exit(attempt);
            prop_assert!(
                late_watcher.complete(()).is_err(),
                "late exit watcher did not complete immediately; actions={actions:?} census={:?}",
                runtime.stats()
            );
            late_watcher.wait();
            let observed = manager.get(attempt).expect("registered demo node");

            runtime.stop_actor(relay).expect("stop demo process relay");
            drive(&backend);
            let stats = runtime.stats();
            let worker_panics = stats
                .workers
                .iter()
                .map(|worker| worker.panics)
                .sum::<u64>();
            prop_assert!(
                check_relay_invariants(
                    &observed,
                    expected_pid,
                    &expected_exit,
                    &expected_spawn_failure,
                    stats.actors.len(),
                    worker_panics,
                )
                .is_ok(),
                "demo process relay invariant failed; actions={actions:?} \
                 expected_pid={expected_pid:?} expected_exit={expected_exit:?} \
                 expected_spawn_failure={expected_spawn_failure:?} observed_pid={:?} \
                 observed_exit={:?} observed_spawn_failure={:?} census={stats:?}",
                observed.pid,
                observed.exited,
                observed.spawn_failed,
            );
        }
    }

    #[test]
    fn process_relay_oracle_rejects_lost_exit() {
        let observed = NodeRuntime {
            attempt: 7,
            logical_node: "fault-sensitive-node".to_owned(),
            bootstrap: ActorAddress::default(),
            pid: Some(9),
            exited: None,
            spawn_failed: None,
            last_announce_ms: None,
            endpoint_addr: None,
        };
        let rejected =
            check_relay_invariants(&observed, Some(9), &Some(ExitStatus::Code(0)), &None, 0, 0);
        assert!(
            rejected.is_err(),
            "provider property oracle accepted a controlled lost-exit defect"
        );
    }
}
