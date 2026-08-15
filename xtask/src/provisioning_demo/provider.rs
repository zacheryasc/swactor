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
//! Because the process actor supervises children with stdio null, each child
//! publishes its swactor node key and heartbeats to a per-attempt key file
//! (`DEMO_NODE_KEY_FILE`); the supervisor reads it to learn the child's iroh
//! identity and liveness.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::ExternalSender;
use swactor_process::{ExitStatus, ProcessOutput};

use provisioning::executor::{EffectBackend, EffectError};
use provisioning::node::{
    BootstrapSessionId, CreateLeaseResult, DestroyHandle, LeaseFacts, NodeManagerCommand,
    ProviderKind, ProviderLeaseId, SshEndpoint,
};
use provisioning::reconciler::{OperationId, OperationOutcome, PlannedEffect};
use provisioning::plugin::{NodeProvisionSpec, PluginNodeHandle, PluginSink, ProvisionPlugin};
use telemetry::{ChannelContent, StreamDescriptor, TelemetryEndpoint, TelemetryProducer};

/// How long a blocking plugin call waits for the supervisor actor.
pub const BACKEND_WAIT: Duration = Duration::from_secs(20);

/// One provisioned node's runtime facts.
#[derive(Clone)]
pub struct NodeRuntime {
    pub attempt: u64,
    pub logical_node: String,
    pub process_actor: ActorAddress,
    pub key_file: PathBuf,
    pub pid: Option<u32>,
    pub exited: Option<ExitStatus>,
    pub spawn_failed: Option<String>,
}

/// Spawn request from the plugin (blocking thread) to the supervisor actor.
#[derive(Clone)]
pub struct SpawnNodeRequest {
    pub attempt: u64,
    pub logical_node: String,
    pub key_file: PathBuf,
    pub reply: std::sync::mpsc::Sender<Result<NodeRuntime, String>>,
}

/// Shared node registry + spawn queue: hub between executor blocking threads,
/// relay actors, and the supervisor actor.
#[derive(Clone, Default)]
pub struct NodeManager {
    inner: Arc<Mutex<NodeManagerInner>>,
}

#[derive(Default)]
struct NodeManagerInner {
    nodes: BTreeMap<u64, NodeRuntime>,
    spawn_tx: Option<std::sync::mpsc::Sender<SpawnNodeRequest>>,
}

impl NodeManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_spawn_channel(&self, sender: std::sync::mpsc::Sender<SpawnNodeRequest>) {
        self.inner.lock().expect("node manager").spawn_tx = Some(sender);
    }

    pub fn request_spawn(
        &self,
        attempt: u64,
        logical_node: String,
        key_file: PathBuf,
    ) -> Result<NodeRuntime, String> {
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        let request = SpawnNodeRequest {
            attempt,
            logical_node,
            key_file,
            reply: reply_tx,
        };
        {
            let inner = self.inner.lock().expect("node manager");
            let sender = inner
                .spawn_tx
                .as_ref()
                .ok_or_else(|| "supervisor spawn channel not installed".to_owned())?;
            sender
                .send(request)
                .map_err(|_| "supervisor actor gone".to_owned())?;
        }
        reply_rx
            .recv_timeout(BACKEND_WAIT)
            .map_err(|_| "timed out waiting for node spawn".to_owned())?
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
        self.update(attempt, |runtime| runtime.exited = Some(status));
    }

    pub fn set_spawn_failed(&self, attempt: u64, reason: String) {
        self.update(attempt, |runtime| runtime.spawn_failed = Some(reason));
    }

    pub fn remove(&self, attempt: u64) {
        self.inner.lock().expect("node manager").nodes.remove(&attempt);
    }

}

/// The demo `ProvisionPlugin`: resources are node-role child processes.
pub struct DemoProvider {
    manager: NodeManager,
    keys_dir: PathBuf,
}

impl DemoProvider {
    pub fn new(manager: NodeManager, keys_dir: PathBuf) -> Self {
        Self { manager, keys_dir }
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
        let key_file = self.keys_dir.join(format!("node-{attempt}.key"));
        let runtime = self
            .manager
            .request_spawn(attempt, logical_node, key_file)?;
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
/// lands on its own dashboard stream (one fleet card per node).
pub struct NodeTelemetry {
    pub endpoint: TelemetryEndpoint,
    pub producer: TelemetryProducer,
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
        Self { endpoint, producer }
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

fn runtime_exited(manager: &NodeManager, attempt: u64) -> bool {
    manager.get(attempt).is_some_and(|runtime| runtime.exited.is_some())
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
                Ok(OperationOutcome::LeaseCreated(CreateLeaseResult {
                    lease: demo_lease(attempt),
                    endpoint: Some(demo_endpoint()),
                }))
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
    if let Some(runtime) = manager.get(handle.id) {
        if runtime.pid.is_some() && runtime.exited.is_none() {
            let _ = swactor_process::send_process_command(
                sender,
                runtime.process_actor,
                swactor_process::ProcessCommand::Stop {
                    kill_after: Some(Duration::from_secs(1)),
                },
            );
        }
        let deadline = std::time::Instant::now() + BACKEND_WAIT;
        while !runtime_exited(manager, handle.id) {
            if std::time::Instant::now() > deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
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
