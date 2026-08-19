//! The provisioning supervisor: an actor that owns the `ClusterDriver`, the
//! demo provider, the desired shape, and the 250ms tick.
//!
//! Each tick mirrors the production `ClusterReconciler` poll semantics:
//! drain executor results (closing bootstrap sessions after convergence),
//! classify due operations, requeue, drive until blocked — then feeds the
//! real world back in (wire announces, child exits), emits
//! `prov.reconciler.*` telemetry, and drains every telemetry endpoint into
//! the dashboard.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use provisioning::executor::{
    BlockingEffectSpawner, BlockingEffectWork, ExecutorOperationStatus, IdempotentEffectExecutor,
};
use provisioning::node::{
    BootstrapObservation, BootstrapStage, NodeGroupId, NodeStage, RoleId, RunId, SwactorId,
};
use provisioning::reconciler::{ClusterDriver, EffectExecutor};
use provisioning::reconciler::{
    ClusterShape, NodeObservation, OperationOutcome, PlannedEffect, RetryPolicy,
};
use serde_json::json;
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor_engine::{ActorCompletion, BlockingWorkSender, EngineHandle};
use telemetry::{ChannelContent, StreamDescriptor, TelemetryEndpoint, TelemetryProducer};

use crate::demo::edge;
use crate::demo::edge::{EdgeAck, EdgePumpCmd, EdgePumpMessage, EdgeSession};
use crate::demo::provider::{
    DemoBackend, NodeManager, NodeTelemetry, register_node_channels, unix_ms,
};

/// Supervisor telemetry: channels + name resolution for the dashboard path.
pub struct SupervisorTelemetry {
    pub endpoint: TelemetryEndpoint,
    pub producer: TelemetryProducer,
    /// Descriptor metadata mirrored onto every published frame so the
    /// dashboard can classify the stream without a catalog.
    pub origin: &'static str,
    pub label: &'static str,
    names: BTreeMap<telemetry::ChannelId, String>,
}

impl SupervisorTelemetry {
    pub fn new(node: &str) -> Self {
        let stream = telemetry::frame::StreamId::new(
            telemetry::frame::NodeId::new(node),
            telemetry::frame::Lifetime(1),
        );
        let endpoint = TelemetryEndpoint::with_descriptor(
            StreamDescriptor {
                stream,
                label: Some("provisioning supervisor".to_owned()),
                origin: telemetry::frame::StreamOrigin::Orchestrator,
            },
            512,
            16,
        );
        let producer = endpoint.producer();
        Self {
            endpoint,
            producer,
            origin: "orchestrator",
            label: "provisioning supervisor",
            names: BTreeMap::new(),
        }
    }
    pub fn register(&mut self, name: &str) -> telemetry::ChannelId {
        let id = self.endpoint.register_channel(
            name,
            ChannelContent::JsonRecord {
                schema: Some("demo.prov.v1".to_owned()),
            },
        );
        self.names.insert(id, name.to_owned());
        id
    }
}

#[derive(Clone)]
pub struct EngineSpawner {
    blocking: BlockingWorkSender,
}

impl EngineSpawner {
    pub fn new(engine: &EngineHandle) -> Self {
        Self {
            blocking: engine.blocking_work_sender(),
        }
    }
}

impl BlockingEffectSpawner for EngineSpawner {
    type SpawnError = String;

    fn spawn_blocking(&self, work: BlockingEffectWork) -> Result<(), Self::SpawnError> {
        self.blocking
            .submit(work)
            .map_err(|_| "engine stopped before demo effect submission".to_owned())
    }
}

/// Wrapper that records every dispatched effect for the command feed.
struct FeedExecutor<'a> {
    inner: &'a mut IdempotentEffectExecutor<DemoBackend, EngineSpawner>,
    dispatched: Vec<String>,
}

impl EffectExecutor for FeedExecutor<'_> {
    type SubmitError = provisioning::executor::ExecutorSubmitError;

    fn submit(&mut self, effect: &PlannedEffect) -> Result<(), Self::SubmitError> {
        use provisioning::node::NodeManagerCommand;
        let command = match &effect.command {
            NodeManagerCommand::CreateLease(_) => "CreateLease".to_owned(),
            NodeManagerCommand::LookupEndpoint(_) => "LookupEndpoint".to_owned(),
            NodeManagerCommand::StartBootstrap(_) => "StartBootstrap".to_owned(),
            NodeManagerCommand::BootstrapConvergenceObserved { .. } => {
                "BootstrapConvergenceObserved".to_owned()
            }
            NodeManagerCommand::CancelBootstrap { .. } => "CancelBootstrap".to_owned(),
            NodeManagerCommand::DestroyLease(_) => "DestroyLease".to_owned(),
        };
        self.dispatched
            .push(format!("{} → {}", effect.node.0, command));
        self.inner.submit(effect)
    }
}

#[derive(Clone)]
pub enum SupervisorMsg {
    Tick,
    Control(dashboard::control::ControlCommand),
    Spawn(crate::demo::provider::SpawnNodeRequest),
    /// Event from a per-node bootstrap actor.
    Bootstrap(provisioning::BootstrapEvent),
    /// A remote node's telemetry pull stream registered its header (stream
    /// descriptor + channel catalog), associated with its logical node and
    /// provision attempt. Frames are fused onto the logical node's stream.
    NodeStream {
        header: iroh_driver::TelemetryQuicHeader,
        logical_node: String,
        attempt: u64,
    },
    /// Control-plane ack from a node's edge agent (edge provisioning).
    EdgeAck(EdgeAck),
    /// State + feed update from the edge pump thread (sole session owner).
    EdgeUpdate(crate::demo::edge::EdgePumpUpdate),
    Shutdown {
        completion: ActorCompletion<()>,
    },
}

/// Metadata for one remote node telemetry pull, keyed by the node's hex
/// transport key: channel catalog + which logical node stream its frames
/// are fused onto.
#[derive(Clone, Default)]
struct RemoteStreamMeta {
    logical_node: String,
    attempt: u64,
    channels: BTreeMap<telemetry::ChannelId, String>,
}

/// Per-node telemetry handle kept while the attempt is live.
struct NodeStreams {
    telemetry: NodeTelemetry,
    status_channel: telemetry::ChannelId,
}

struct ShutdownState {
    completion: ActorCompletion<()>,
    ticks: u32,
    settled_ticks: u32,
}

/// The provisioning supervisor actor.
pub struct SupervisorActor {
    pub driver: ClusterDriver,
    pub executor: IdempotentEffectExecutor<DemoBackend, EngineSpawner>,
    pub manager: NodeManager,
    pub driver_handle: std::sync::Arc<crate::demo::DemoDriverHandle>,
    pub telemetry: SupervisorTelemetry,
    pub events_channel: telemetry::ChannelId,
    pub snapshot_channel: telemetry::ChannelId,
    pub sender: swactor::runtime::ExternalSender,
    /// Bootstrap kind registry (spec.kind → logic).
    registry: provisioning::BootstrapRegistry,
    /// Pull collector fired by bootstrap actors on Bootstrapped.
    collector: std::sync::Arc<dyn provisioning::NodeTelemetryCollector>,
    /// Engine handle for bootstrap actor probe intervals.
    engine: EngineHandle,
    /// Subscription draining the remote-stream fanout into the dashboard.
    remote_sub: telemetry::TelemetrySubscription,
    remote_streams: BTreeMap<String, RemoteStreamMeta>,
    /// Bootstrap reports parked until the reconciler opens the attempt's
    /// bootstrap session (early joins / failures).
    pending_reports: BTreeMap<u64, provisioning::BootstrapEvent>,
    /// Desired shape slots: singleton groups, one per logical node.
    slots: Vec<String>,
    slot_seq: u64,
    run_id: RunId,
    nodes: BTreeMap<u64, NodeStreams>,
    node_life: u64,
    last_stages: BTreeMap<String, (NodeStage, Option<BootstrapStage>)>,
    pub dashboard: dashboard::DashboardHandle,
    status_tick: u64,
    launch: crate::demo::LaunchStyle,
    /// Actor that owns and advances all edge sessions.
    edge_actor: ActorAddress,
    /// Last reported edge-state mirror from the pump (snapshot data).
    edge_states: Vec<serde_json::Value>,
    next_edge_id: u64,
    shutdown: Option<ShutdownState>,
}

impl SupervisorActor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        driver: ClusterDriver,
        executor: IdempotentEffectExecutor<DemoBackend, EngineSpawner>,
        manager: NodeManager,
        driver_handle: std::sync::Arc<crate::demo::DemoDriverHandle>,
        mut telemetry: SupervisorTelemetry,
        dashboard: dashboard::DashboardHandle,
        sender: swactor::runtime::ExternalSender,
        registry: provisioning::BootstrapRegistry,
        collector: std::sync::Arc<dyn provisioning::NodeTelemetryCollector>,
        engine: EngineHandle,
        remote_sub: telemetry::TelemetrySubscription,
        initial_slots: Vec<String>,
        run_id: RunId,
        launch: crate::demo::LaunchStyle,
        edge_actor: ActorAddress,
    ) -> Self {
        let events_channel = telemetry.register("prov.reconciler.events");
        let snapshot_channel = telemetry.register("prov.reconciler.snapshot");
        Self {
            driver,
            executor,
            manager,
            driver_handle,
            telemetry,
            events_channel,
            snapshot_channel,
            sender,
            registry,
            collector,
            engine,
            remote_sub,
            remote_streams: BTreeMap::new(),
            pending_reports: BTreeMap::new(),
            nodes: BTreeMap::new(),
            run_id,
            slot_seq: initial_slots.len() as u64,
            slots: initial_slots,
            node_life: 0,
            last_stages: BTreeMap::new(),
            dashboard,
            status_tick: 0,
            launch,
            edge_actor,
            edge_states: Vec::new(),
            next_edge_id: 0,
            shutdown: None,
        }
    }

    fn schedule_tick(&self, ctx: &Ctx) {
        self.engine.send_after(
            crate::demo::TICK,
            self.sender.clone(),
            ctx.self_addr(),
            SupervisorMsg::Tick,
        );
    }

    fn advance_shutdown(&mut self, ctx: &Ctx) -> bool {
        let converged = self.driver.is_converged();
        let Some(shutdown) = self.shutdown.as_mut() else {
            return false;
        };
        shutdown.ticks = shutdown.ticks.saturating_add(1);
        if converged {
            shutdown.settled_ticks = shutdown.settled_ticks.saturating_add(1);
        } else {
            shutdown.settled_ticks = 0;
        }
        if shutdown.settled_ticks < 8 && shutdown.ticks < 80 {
            return false;
        }
        let shutdown = self
            .shutdown
            .take()
            .expect("shutdown state was present while completing");
        assert!(
            shutdown.completion.complete(()).is_ok(),
            "demo supervisor completed shutdown twice"
        );
        ctx.stop_self();
        true
    }

    fn desired_shape(&self, generation: u64) -> ClusterShape {
        ClusterShape {
            run_id: self.run_id.clone(),
            generation,
            groups: self.slots.iter().map(|slot| slot_group(slot)).collect(),
        }
    }

    fn emit_event(&mut self, kind: &str, node: &str, detail: String) {
        let payload = json!({
            "at_ms": unix_ms(SystemTime::now()),
            "kind": kind,
            "node": node,
            "detail": detail,
        });
        let bytes = serde_json::to_vec(&payload).expect("event serializes");
        self.telemetry
            .producer
            .submit_bytes(self.events_channel, bytes);
    }

    /// Handle a spawn request from the provider (runs in actor context):
    /// spawn the per-attempt bootstrap actor through the registry — the
    /// supervision never launches nodes directly.
    fn spawn_node(&mut self, ctx: &Ctx, request: crate::demo::provider::SpawnNodeRequest) {
        let attempt = request.attempt;

        // Supervisor-authored lifecycle stream (kept alive independent of
        // the node: it reports death even when the node cannot).
        self.node_life += 1;
        let telemetry = NodeTelemetry::new(&request.logical_node, self.node_life);
        let status_channel = register_node_channels(&telemetry.producer);

        let (kind, argv, mut env) = match &self.launch {
            crate::demo::LaunchStyle::Process { exe } => (
                "process",
                vec![
                    exe.to_string_lossy().to_string(),
                    "demo".to_owned(),
                    "--demo-node".to_owned(),
                    self.driver_handle.supervisor_addr_json.clone(),
                    "--demo-attempt".to_owned(),
                    attempt.to_string(),
                ],
                Vec::new(),
            ),
            crate::demo::LaunchStyle::Docker(docker) => (
                "docker",
                vec![
                    "docker".to_owned(),
                    "run".to_owned(),
                    "--rm".to_owned(),
                    "--name".to_owned(),
                    crate::demo::docker::container_name(attempt),
                    "--label".to_owned(),
                    format!("{}=1", crate::demo::docker::SWEEP_LABEL),
                    "--label".to_owned(),
                    format!("{}={}", crate::demo::docker::RUN_LABEL, docker.run_token),
                    "--network".to_owned(),
                    docker.network.clone(),
                    docker.image.clone(),
                    "demo".to_owned(),
                    "--demo-node".to_owned(),
                    docker.supervisor_addr_json.clone(),
                    "--demo-attempt".to_owned(),
                    attempt.to_string(),
                ],
                Vec::new(),
            ),
        };
        env.push(("DEMO_NODE_ID".to_owned(), request.logical_node.clone()));

        let spec = provisioning::NodeLaunchSpec {
            kind: kind.to_owned(),
            attempt,
            logical_node: request.logical_node.clone(),
            argv,
            env,
            workdir: None,
            label: Some(request.logical_node.clone()),
        };

        let reporter: provisioning::BootstrapReporter = {
            let sender = self.sender.clone();
            let supervisor = ctx.self_addr();
            std::sync::Arc::new(move |event| {
                let _ = sender.send_to(supervisor, SupervisorMsg::Bootstrap(event));
            })
        };
        let config = provisioning::BootstrapConfig {
            reporter,
            sender: self.sender.clone(),
            collector: Some(std::sync::Arc::clone(&self.collector)),
            probe_period: provisioning::DEFAULT_PROBE_PERIOD,
            spec: spec.clone(),
        };
        let logic = match self.registry.create(&spec) {
            Ok(logic) => logic,
            Err(error) => {
                assert!(
                    request
                        .reply
                        .complete(Err(format!("bootstrap logic: {error}")))
                        .is_ok(),
                    "spawn request completed twice"
                );
                return;
            }
        };
        match provisioning::spawn_bootstrap_actor(ctx, &self.engine, logic, config) {
            Ok(bootstrap) => {
                self.nodes.insert(
                    attempt,
                    NodeStreams {
                        telemetry,
                        status_channel,
                    },
                );
                let runtime = crate::demo::provider::NodeRuntime {
                    attempt,
                    logical_node: request.logical_node.clone(),
                    bootstrap,
                    pid: None,
                    exited: None,
                    spawn_failed: None,
                    last_announce_ms: None,
                    endpoint_addr: None,
                };
                assert!(
                    request.reply.complete(Ok(runtime)).is_ok(),
                    "spawn request completed twice"
                );
            }
            Err(error) => {
                assert!(
                    request
                        .reply
                        .complete(Err(format!("spawn bootstrap actor: {error}")))
                        .is_ok(),
                    "spawn request completed twice"
                );
            }
        }
    }

    /// Feed bootstrap-progress observations into the driver. Join detection
    /// and exit classification moved into the per-node bootstrap actors
    fn observe_world(&mut self, now: SystemTime) {
        let node_ids: Vec<String> = self
            .driver
            .state()
            .nodes
            .keys()
            .map(|id| id.0.clone())
            .collect();
        for node_id in node_ids {
            let Some(managed) = self
                .driver
                .state()
                .nodes
                .get(&provisioning::node::LogicalNodeId(node_id.clone()))
            else {
                continue;
            };
            let attempt = managed.attempt;
            let Some(session_id) = managed.active_bootstrap else {
                continue;
            };
            let Some(runtime) = self.manager.get(attempt.0) else {
                continue;
            };
            // Stage evidence: the supervised child started (process pid /
            // docker CLI pid observed) means the node runtime is coming up
            // and we are waiting for its control-plane announce; before
            // that the lease exists but the foreign process is not up yet.
            let stage_seen = if runtime.pid.is_some() {
                BootstrapStage::WaitingForSwactorJoin
            } else {
                BootstrapStage::SshReady
            };
            self.driver.apply_observation(
                &provisioning::node::LogicalNodeId(node_id.clone()),
                attempt,
                NodeObservation::BootstrapObserved {
                    session_id,
                    observation: BootstrapObservation::stage(stage_seen),
                },
                now,
            );
        }
    }

    /// Fold bootstrap-actor events into the reconciler. A report that
    /// arrives before the reconciler has an active bootstrap session for
    /// the attempt (the actor starts at spawn, `StartBootstrap` comes
    /// later) is parked and re-delivered on the next tick — the join signal
    /// is exactly-once, so dropping an early one wedges the session.
    fn handle_bootstrap_event(&mut self, now: SystemTime, event: provisioning::BootstrapEvent) {
        let attempt = match &event {
            provisioning::BootstrapEvent::Bootstrapped(identity) => identity.attempt,
            provisioning::BootstrapEvent::Failed { attempt, .. }
            | provisioning::BootstrapEvent::Exited { attempt, .. } => *attempt,
        };
        if !self.deliver_bootstrap_report(now, event.clone()) {
            self.pending_reports.insert(attempt, event);
        }
    }

    /// Deliver one bootstrap report to the reconciler driver. Returns false
    /// when the attempt has no active bootstrap session yet.
    fn deliver_bootstrap_report(
        &mut self,
        now: SystemTime,
        event: provisioning::BootstrapEvent,
    ) -> bool {
        match event {
            provisioning::BootstrapEvent::Bootstrapped(identity) => {
                let Some((node_id, session_id)) = self.node_for_attempt(identity.attempt) else {
                    return false;
                };
                let key_prefix = &identity.key_hex[..8.min(identity.key_hex.len())];
                self.emit_event(
                    "observation",
                    &node_id,
                    format!("swactor join confirmed (key {key_prefix}…)"),
                );
                self.driver.apply_observation(
                    &provisioning::node::LogicalNodeId(node_id),
                    provisioning::NodeAttemptId(identity.attempt),
                    NodeObservation::SwactorJoined {
                        session_id,
                        swactor_id: SwactorId(identity.key_hex),
                    },
                    now,
                );
            }
            provisioning::BootstrapEvent::Failed { attempt, reason } => {
                let Some((node_id, session_id)) = self.node_for_attempt(attempt) else {
                    return false;
                };
                self.emit_event("observation", &node_id, reason.clone());
                self.driver.apply_observation(
                    &provisioning::node::LogicalNodeId(node_id),
                    provisioning::NodeAttemptId(attempt),
                    NodeObservation::BootstrapFailed { session_id, reason },
                    now,
                );
            }
            provisioning::BootstrapEvent::Exited { attempt, reason } => {
                // An exit while a session is still open is a bootstrap
                // failure (death before join); otherwise it is a plain
                // death observation. Either way its edges are dead: the
                // node process is gone.
                if let Some((node_id, session_id)) = self.node_for_attempt(attempt) {
                    self.emit_event("observation", &node_id, format!("node runtime: {reason}"));
                    self.driver.apply_observation(
                        &provisioning::node::LogicalNodeId(node_id),
                        provisioning::NodeAttemptId(attempt),
                        NodeObservation::BootstrapFailed {
                            session_id,
                            reason: format!("node process exited: {reason}"),
                        },
                        now,
                    );
                }
                // Publish the terminal status now: once the reconciler
                // destroys the lease the runtime is deregistered, and the
                // Fleet Control table would otherwise keep its last
                // "running" state forever. Derive the logical name from the
                // lifecycle stream itself — the registry entry can already
                // be gone (lease destroy races the bootstrap probe).
                if let Some(streams) = self.nodes.get(&attempt) {
                    let logical = streams
                        .telemetry
                        .endpoint
                        .stream_id()
                        .node
                        .as_str()
                        .to_owned();
                    let pid = self.manager.get(attempt).and_then(|r| r.pid);
                    let payload = json!({
                        "at_ms": unix_ms(now),
                        "node": logical,
                        "alive": false,
                        "pid": pid,
                        "event": "exited",
                    });
                    if let Ok(bytes) = serde_json::to_vec(&payload) {
                        streams
                            .telemetry
                            .producer
                            .submit_bytes(streams.status_channel, bytes);
                    }
                }
                // Death-replacement is shape logic: `replace_dead_ready_nodes`
                // reads the exit from the shared registry.
            }
        }
        true
    }

    /// Re-deliver parked bootstrap reports whose session has appeared.
    fn deliver_pending_reports(&mut self, now: SystemTime) {
        let attempts: Vec<u64> = self.pending_reports.keys().copied().collect();
        for attempt in attempts {
            let Some(event) = self.pending_reports.get(&attempt).cloned() else {
                continue;
            };
            if self.deliver_bootstrap_report(now, event) {
                self.pending_reports.remove(&attempt);
            }
        }
    }

    /// Resolve a driver node (id + active bootstrap session) by attempt.
    fn node_for_attempt(
        &self,
        attempt: u64,
    ) -> Option<(String, provisioning::node::BootstrapSessionId)> {
        self.driver
            .state()
            .nodes
            .iter()
            .find(|(_, managed)| managed.attempt.0 == attempt)
            .and_then(|(id, managed)| {
                managed
                    .active_bootstrap
                    .map(|session| (id.0.clone(), session))
            })
    }

    /// Register a remote node telemetry stream header (pull side).
    fn register_remote_stream(
        &mut self,
        header: iroh_driver::TelemetryQuicHeader,
        logical_node: String,
        attempt: u64,
    ) {
        let key = header.stream.stream.node.as_str().to_string();
        let meta = self.remote_streams.entry(key.clone()).or_default();
        meta.logical_node = logical_node.clone();
        meta.attempt = attempt;
        for channel in &header.channels {
            meta.channels.insert(channel.id, channel.name.clone());
        }
        println!(
            "demo: fusing telemetry of node {logical_node} (stream {key}, {} channels)",
            meta.channels.len()
        );
    }
    /// Drain remote-node telemetry frames into the dashboard, fusing them
    /// onto the logical node's stream: one card per node, carrying both the
    /// supervisor-authored lifecycle channels and the node's real runtime
    /// channels (runtime.actors, node.beat, node.status).
    fn flush_remote_streams(&mut self) {
        for event in self.remote_sub.drain_available() {
            match event {
                telemetry::frame::TelemetryEvent::Frame(delivery) => {
                    let key = delivery.channel.stream.node.as_str().to_string();
                    let Some(meta) = self.remote_streams.get(&key).cloned() else {
                        continue;
                    };
                    let Some(node_stream) = self.nodes.get(&meta.attempt) else {
                        continue;
                    };
                    // Fuse: republish on the logical node's stream.
                    let target_stream = node_stream.telemetry.endpoint.stream_id().clone();
                    let origin = node_stream.telemetry.origin;
                    let label = node_stream.telemetry.label.clone();
                    let channel_name = meta
                        .channels
                        .get(&delivery.channel.channel)
                        .cloned()
                        .unwrap_or_else(|| format!("channel#{}", delivery.channel.channel.0));
                    let frame = telemetry::frame::Frame {
                        channel: delivery.channel.channel,
                        position: delivery.position,
                        payload: delivery.payload,
                    };
                    publish_frame(
                        &self.dashboard,
                        &target_stream,
                        &channel_name,
                        &frame,
                        origin,
                        &label,
                    );
                }
                telemetry::frame::TelemetryEvent::ChannelDeclared(descriptor) => {
                    let key = descriptor.stream.node.as_str().to_string();
                    self.remote_streams
                        .entry(key)
                        .or_default()
                        .channels
                        .insert(descriptor.id, descriptor.name);
                }
                _ => {}
            }
        }
    }

    /// Replace ready nodes whose child has died (shape-native replacement:
    /// retire the dead singleton slot and add a fresh one).
    fn replace_dead_ready_nodes(&mut self, now: SystemTime) {
        let state = self.driver.state().clone();
        let mut replacements: Vec<(String, String)> = Vec::new();
        for (id, managed) in &state.nodes {
            if managed.record.ready
                && managed.intent == provisioning::reconciler::NodeIntent::Active
            {
                let Some(runtime) = self.manager.get(managed.attempt.0) else {
                    continue;
                };
                if runtime.exited.is_some() {
                    self.slot_seq += 1;
                    let fresh = format!("node-{}", self.slot_seq);
                    replacements.push((id.0.clone(), fresh));
                }
            }
        }
        if replacements.is_empty() {
            return;
        }
        for (dead, fresh) in &replacements {
            self.emit_event(
                "control",
                dead,
                format!("runtime death; replacing as {fresh}"),
            );
            self.slots.retain(|slot| slot_group_id(slot) != *dead);
            self.slots.push(fresh.clone());
        }
        let generation = self.driver.desired().generation.saturating_add(1);
        if let Err(error) = self.driver.update_desired(self.desired_shape(generation)) {
            eprintln!("demo: replace update_desired failed: {error}");
        }
        let _ = now;
    }

    /// One poll pass mirroring the production reconciler loop.
    fn poll(&mut self, now: SystemTime) {
        // 1. Drain executor results; close bootstrap after convergence.
        for result in self.executor.drain_results() {
            let close = matches!(
                result.result,
                Ok(OperationOutcome::BootstrapConvergenceAccepted)
            )
            .then_some((result.node.clone(), result.operation.attempt));
            let detail = match &result.result {
                Ok(outcome) => format!("{outcome:?}"),
                Err(error) => format!("failed: {}", error.reason),
            };
            let node = result.node.0.clone();
            let applied = self.driver.apply_executor_result(result, now);
            self.emit_event(
                "result",
                &node,
                format!("{detail}{}", if applied { "" } else { " (stale)" }),
            );
            if applied && let Some((node, attempt)) = close {
                self.driver.apply_observation(
                    &node.clone(),
                    attempt,
                    NodeObservation::BootstrapClosed {
                        session_id: provisioning::node::BootstrapSessionId(attempt.0),
                    },
                    now,
                );
            }
        }

        // 2. Classify due operations.
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
                    self.executor.expire(
                        operation.operation,
                        "executor operation timed out with an ambiguous outcome",
                    );
                }
                ExecutorOperationStatus::Completed => {}
            }
        }

        // 3. Drive the state machine, recording dispatched commands.
        self.driver.trigger_if_due(now);
        let mut feed = FeedExecutor {
            inner: &mut self.executor,
            dispatched: Vec::new(),
        };
        match self.driver.drive_until_blocked(now, &mut feed) {
            Ok(_) => {
                for line in feed.dispatched {
                    self.emit_event("command", "", line);
                }
            }
            Err(error) => {
                self.emit_event("error", "", format!("{error}"));
            }
        }
    }
    /// Per-node liveness heartbeat on the `node.status` channel so fleet
    /// cards and the control view stay live between lifecycle transitions.
    fn emit_node_status(&mut self, now: SystemTime) {
        self.status_tick = self.status_tick.wrapping_add(1);
        if self.status_tick % 4 != 0 {
            return; // 250ms ticks → heartbeat every second
        }
        let attempts: Vec<u64> = self.nodes.keys().copied().collect();
        for attempt in attempts {
            let Some(streams) = self.nodes.get(&attempt) else {
                continue;
            };
            let Some(runtime) = self.manager.get(attempt) else {
                continue;
            };
            // Wire liveness: the node re-announces every heartbeat period,
            // so the age of the last announce is the control-plane
            // heartbeat. `null` until the first announce.
            let heartbeat_ms_ago = runtime
                .last_announce_ms
                .map(|ms| unix_ms(now).saturating_sub(ms));
            let payload = json!({
                "at_ms": unix_ms(now),
                "node": runtime.logical_node,
                "alive": runtime.exited.is_none(),
                "pid": runtime.pid,
                // Process state for the Fleet Control table (same shape the
                // process lifecycle mirror used to publish).
                "event": if runtime.exited.is_some() { "exited" } else { "started" },
                "heartbeat_ms_ago": heartbeat_ms_ago,
            });
            let bytes = serde_json::to_vec(&payload).expect("status serializes");
            streams
                .telemetry
                .producer
                .submit_bytes(streams.status_channel, bytes);
        }
    }

    /// Emit stage transitions and the snapshot.
    fn emit_feed(&mut self, now: SystemTime) {
        let state = self.driver.state().clone();
        let mut nodes_json = Vec::new();
        let mut ready_count = 0_u64;
        for (id, managed) in &state.nodes {
            let bootstrap = managed
                .record
                .bootstrap
                .as_ref()
                .map(|facts| facts.last_stage);
            let current = (managed.record.stage, bootstrap);
            let fmt_boot = |stage: Option<BootstrapStage>| {
                stage
                    .map(|stage| format!("{stage:?}"))
                    .unwrap_or_else(|| "-".to_owned())
            };
            if let Some(previous) = self.last_stages.get(&id.0) {
                if previous.0 != current.0 || previous.1 != current.1 {
                    self.emit_event(
                        "transition",
                        &id.0,
                        format!(
                            "{:?} ({}) → {:?} ({})",
                            previous.0,
                            fmt_boot(previous.1),
                            current.0,
                            fmt_boot(current.1),
                        ),
                    );
                }
            } else if managed.record.stage != NodeStage::New {
                self.emit_event(
                    "transition",
                    &id.0,
                    format!("New → {:?} ({})", current.0, fmt_boot(current.1)),
                );
            }
            self.last_stages.insert(id.0.clone(), current);
            if managed.record.ready {
                ready_count += 1;
            }
            let runtime_pid = self.manager.get(managed.attempt.0).and_then(|r| r.pid);
            nodes_json.push(json!({
                "id": id.0,
                "intent": format!("{:?}", managed.intent),
                "stage": format!("{:?}", managed.record.stage),
                "bootstrap": fmt_boot(current.1),
                "attempt": managed.attempt.0,
                "ready": managed.record.ready,
                "pid": runtime_pid,
                "failure": managed.record.failed_reason,
            }));
        }
        let edges_json = self.edge_states.clone();

        let snapshot = json!({
            "at_ms": unix_ms(now),
            "desired": self.slots.len(),
            "ready": ready_count,
            "generation": self.driver.desired().generation,
            "converged": self.driver.is_converged(),
            "nodes": nodes_json,
            "edges": edges_json,
        });
        let bytes = serde_json::to_vec(&snapshot).expect("snapshot serializes");
        self.telemetry
            .producer
            .submit_bytes(self.snapshot_channel, bytes);
    }

    /// Drain every telemetry endpoint into the dashboard.
    fn flush_telemetry(&mut self) {
        let supervisor_stream = self.telemetry.endpoint.stream_id().clone();
        for frame in self.telemetry.endpoint.mux().drain() {
            let channel = self
                .telemetry
                .names
                .get(&frame.channel)
                .cloned()
                .unwrap_or_else(|| format!("channel#{}", frame.channel.0));
            publish_frame(
                &self.dashboard,
                &supervisor_stream,
                &channel,
                &frame,
                self.telemetry.origin,
                self.telemetry.label,
            );
        }
        let attempts: Vec<u64> = self.nodes.keys().copied().collect();
        for attempt in attempts {
            let Some(streams) = self.nodes.get(&attempt) else {
                continue;
            };
            let stream = streams.telemetry.endpoint.stream_id().clone();
            let catalog = streams.telemetry.endpoint.catalog_snapshot();
            for frame in streams.telemetry.endpoint.mux().drain() {
                let channel = catalog
                    .channels
                    .get(&telemetry::frame::ChannelRef {
                        stream: stream.clone(),
                        channel: frame.channel,
                    })
                    .map(|descriptor| descriptor.name.clone())
                    .unwrap_or_else(|| format!("channel#{}", frame.channel.0));
                publish_frame(
                    &self.dashboard,
                    &stream,
                    &channel,
                    &frame,
                    streams.telemetry.origin,
                    &streams.telemetry.label,
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn publish_frame(
    dashboard: &dashboard::DashboardHandle,
    stream: &telemetry::frame::StreamId,
    channel: &str,
    frame: &telemetry::frame::Frame,
    origin: &str,
    label: &str,
) {
    dashboard.publish(dashboard::FrameEvent {
        stream: dashboard::StreamEvent {
            node: stream.node.as_str().to_string(),
            life: stream.life.0,
            origin: Some(origin.to_owned()),
            label: Some(label.to_owned()),
        },
        channel: channel.to_owned(),
        position: frame.position.0,
        payload: frame.payload.clone(),
    });
}

impl ActorInterface for SupervisorActor {
    type Incoming = SupervisorMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        self.schedule_tick(ctx);
    }

    fn handle(&mut self, ctx: &Ctx, msg: SupervisorMsg) {
        match msg {
            SupervisorMsg::Tick => {
                let now = SystemTime::now();
                self.deliver_pending_reports(now);
                self.poll(now);
                self.observe_world(now);
                self.replace_dead_ready_nodes(now);
                self.poll(now);
                self.sweep_dead_edges();
                self.emit_node_status(now);
                self.emit_feed(now);
                self.flush_telemetry();
                self.flush_remote_streams();
                if !self.advance_shutdown(ctx) {
                    self.schedule_tick(ctx);
                }
            }
            SupervisorMsg::Control(command) => self.handle_control(command),
            SupervisorMsg::Spawn(request) => self.spawn_node(ctx, request),
            SupervisorMsg::Bootstrap(event) => {
                let now = SystemTime::now();
                self.handle_bootstrap_event(now, event);
                self.poll(now);
            }
            SupervisorMsg::NodeStream {
                header,
                logical_node,
                attempt,
            } => self.register_remote_stream(header, logical_node, attempt),
            SupervisorMsg::EdgeAck(ack) => {
                let _ = self
                    .sender
                    .send_to(self.edge_actor, EdgePumpMessage::new(EdgePumpCmd::Ack(ack)));
            }
            SupervisorMsg::EdgeUpdate(update) => {
                self.edge_states = update.states;
                for (node, detail) in update.feed {
                    self.emit_event("edge", &node, detail);
                }
            }
            SupervisorMsg::Shutdown { completion } => {
                if self.shutdown.is_none() {
                    self.slots.clear();
                    let generation = self.driver.desired().generation.saturating_add(1);
                    if let Err(error) = self.driver.update_desired(self.desired_shape(generation)) {
                        eprintln!("demo: shutdown update_desired failed: {error}");
                    }
                    let _ = self
                        .sender
                        .send_to(self.edge_actor, EdgePumpMessage::new(EdgePumpCmd::DropAll));
                    self.emit_event("control", "", "shutdown: desired → empty".to_owned());
                    self.shutdown = Some(ShutdownState {
                        completion,
                        ticks: 0,
                        settled_ticks: 0,
                    });
                }
            }
        }
    }
}

impl SupervisorActor {
    fn handle_control(&mut self, command: dashboard::control::ControlCommand) {
        match command {
            dashboard::control::ControlCommand::Kill { node, .. } => {
                match self.manager.find_by_stream_node(&node) {
                    Some(runtime) => {
                        self.emit_event(
                            "control",
                            &node,
                            format!("kill requested (pid {:?})", runtime.pid),
                        );
                        let _ = self.sender.send_to(
                            runtime.bootstrap,
                            provisioning::BootstrapMsg::Stop {
                                kill_after: Some(Duration::ZERO),
                            },
                        );
                    }
                    None => self.emit_event("control", &node, "kill: unknown node".to_owned()),
                }
            }
            dashboard::control::ControlCommand::Remove { count, .. } => {
                let removed = self.slots.len().min(count as usize);
                if removed == 0 {
                    self.emit_event("control", "", "remove: nothing to remove".to_owned());
                    return;
                }
                for _ in 0..removed {
                    self.slots.pop();
                }
                self.emit_event("control", "", format!("remove -{removed}"));
                let generation = self.driver.desired().generation.saturating_add(1);
                if let Err(error) = self.driver.update_desired(self.desired_shape(generation)) {
                    eprintln!("demo: remove update_desired failed: {error}");
                }
            }
            dashboard::control::ControlCommand::Provision { count, .. } => {
                if count == 0 {
                    return;
                }
                self.slot_seq += count as u64;
                let start = self.slot_seq - count as u64 + 1;
                for seq in start..=self.slot_seq {
                    self.slots.push(format!("node-{seq}"));
                }
                self.emit_event("control", "", format!("provision +{count}"));
                let generation = self.driver.desired().generation.saturating_add(1);
                if let Err(error) = self.driver.update_desired(self.desired_shape(generation)) {
                    eprintln!("demo: provision update_desired failed: {error}");
                }
            }
            dashboard::control::ControlCommand::EstablishEdge { node, .. } => {
                self.establish_edge(&node);
            }
        }
    }

    // ─── Data-plane edges ──────────────────────────────────────────────────

    /// Establish (or replace) the supervisor→node edge from the dashboard.
    /// Only control-plane facts gate this: the node must be registered,
    /// alive, and have a fresh announce (endpoint addr + liveness).
    fn establish_edge(&mut self, node: &str) {
        let Some(runtime) = self.manager.find_by_stream_node(node) else {
            self.emit_event("edge", node, "edge: unknown node".to_owned());
            return;
        };
        if runtime.exited.is_some() {
            self.emit_event("edge", node, "edge: node not running".to_owned());
            return;
        }
        let Some(addr_json) = runtime.endpoint_addr.clone() else {
            self.emit_event(
                "edge",
                node,
                "edge: node endpoint unknown (no announce yet)".to_owned(),
            );
            return;
        };
        // Announce freshness is control-plane liveness: a stale announce
        // means the dial would target a dead endpoint.
        if let Some(last) = runtime.last_announce_ms {
            let age = unix_ms(SystemTime::now()).saturating_sub(last);
            if age > (2 * crate::demo::HEARTBEAT_PERIOD).as_millis() as u64 + 2000 {
                self.emit_event(
                    "edge",
                    node,
                    format!("edge: announce stale ({age}ms); refusing dial",),
                );
                return;
            }
        }
        let Ok(peer) = serde_json::from_str::<iroh::EndpointAddr>(&addr_json) else {
            self.emit_event("edge", node, "edge: node endpoint unparseable".to_owned());
            return;
        };
        // One live session per node: a new edge replaces the old one (the
        // pump owns the sessions; replacement happens on its thread).
        self.next_edge_id += 1;
        let edge_id = data_plane::ids::EdgeId(self.next_edge_id);
        let session = EdgeSession::new(edge_id, runtime.attempt, node.to_owned(), peer.clone());
        let provision = session.provision();
        if self
            .edge_states
            .iter()
            .any(|state| state.get("node").and_then(|v| v.as_str()) == Some(node))
        {
            self.emit_event(
                "edge",
                node,
                "previous edge torn down (replaced)".to_owned(),
            );
        }
        if self
            .sender
            .send_to(
                self.edge_actor,
                EdgePumpMessage::new(EdgePumpCmd::Establish(Box::new(session))),
            )
            .is_err()
        {
            self.emit_event("edge", node, "edge: pump gone".to_owned());
            return;
        }
        if let Ok(bytes) = serde_json::to_vec(&provision) {
            self.driver_handle.driver.send_tagged_gossip(
                peer,
                edge::EDGE_PROVISION_TAG.as_bytes(),
                bytes,
            );
        }
        self.emit_event(
            "edge",
            node,
            format!("edge {}: provision sent (outbound provisioning)", edge_id.0),
        );
    }

    /// Tell the pump which node attempts are still live; it tears down
    /// sessions for anything else (exit observed or registry entry gone).
    fn sweep_dead_edges(&mut self) {
        if self.edge_states.is_empty() {
            return;
        }
        let live: Vec<u64> = self
            .manager
            .nodes()
            .into_iter()
            .filter(|runtime| runtime.exited.is_none())
            .map(|runtime| runtime.attempt)
            .collect();
        let _ = self.sender.send_to(
            self.edge_actor,
            EdgePumpMessage::new(EdgePumpCmd::LiveAttempts(live)),
        );
    }
}

fn slot_group(slot: &str) -> provisioning::node::RunNodeGroupSpec {
    let mut group = demo_group(slot, 1);
    group.group_id = NodeGroupId(slot.to_owned());
    group
}

fn slot_group_id(slot: &str) -> String {
    format!("{slot}-0")
}

pub fn demo_group(id: &str, count: u32) -> provisioning::node::RunNodeGroupSpec {
    provisioning::node::RunNodeGroupSpec {
        run_id: RunId(1),
        group_id: NodeGroupId(id.to_owned()),
        role: RoleId("worker".to_owned()),
        count,
        provider: provisioning::node::ProviderKind::new("demo"),
        shape: provisioning::node::DesiredNodeShape {
            image: "demo-node".to_owned(),
            disk_gb: 1,
            gpu_name: None,
            min_gpu_ram_mb: None,
            min_down_mbps: None,
            min_up_mbps: None,
            min_reliability: None,
            require_verified: false,
            provider_labels: BTreeMap::new(),
        },
        boot: provisioning::node::BootSpec {
            ssh_user: "demo".to_owned(),
            verify_commands: vec!["true".to_owned()],
            start_swactor_command: "xtask demo".to_owned(),
            stdout_sources: Vec::new(),
            stderr_sources: Vec::new(),
            env: Vec::new(),
            args: Vec::new(),
            mounts: Vec::new(),
        },
        swarm_join: provisioning::node::SwarmJoinTemplate {
            orch_swactor_addr: "127.0.0.1:1".to_owned(),
            join_token_ref: "demo".to_owned(),
        },
    }
}

/// Marker helpers used by tests and the module glue.
pub fn initial_slots(count: u64) -> Vec<String> {
    (0..count).map(|index| format!("node-{index}")).collect()
}

pub fn demo_retry_policy() -> RetryPolicy {
    RetryPolicy {
        initial_delay: Duration::from_millis(500),
        max_delay: Duration::from_secs(2),
        jitter: Duration::ZERO,
        operation_timeout: Duration::from_secs(10),
        endpoint_probe_interval: Duration::from_secs(1),
    }
}

#[cfg(test)]
mod properties {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::{Duration, SystemTime};

    use proptest::prelude::*;
    use provisioning::bootstrap::{
        BootstrapLogic, LogicProbe, NodeIdentity, NodeLaunchSpec, NodeTelemetryCollector,
    };
    use provisioning::plugin::{
        NodeProvisionSpec, PluginObservation, PluginObservationSink, PluginSink, ProvisionPlugin,
    };
    use swactor::config::RuntimeConfig;
    use swactor::runtime::RuntimeParts;
    use swactor_engine::{ActorCompletion, Engine, SteppingBackend};
    use swactor_process::{ExitStatus, ProcessOutput};

    use super::*;
    use crate::demo::provider::{
        AnnounceActor, DemoProvider, NodeRelayActor, NodeRuntime, SpawnNodeRequest,
    };

    #[derive(Clone, Copy, Debug)]
    enum SupervisorAction {
        Spawn { attempt: u16, succeeds: bool },
        Started { attempt: u16, pid: u16 },
        Announce { attempt: u16 },
        Heartbeat { attempt: u16 },
        ChildExit { attempt: u16, code: u8 },
        Control { kind: u8, attempt: u16 },
        ProviderBlock,
        Shutdown,
    }

    fn supervisor_actions() -> impl Strategy<Value = Vec<SupervisorAction>> {
        let generated = prop::collection::vec(
            prop_oneof![
                4 => (0_u16..=3, any::<bool>()).prop_map(|(attempt, succeeds)| {
                    SupervisorAction::Spawn { attempt, succeeds }
                }),
                2 => (0_u16..=3, any::<u16>())
                    .prop_map(|(attempt, pid)| SupervisorAction::Started { attempt, pid }),
                3 => (0_u16..=3)
                    .prop_map(|attempt| SupervisorAction::Announce { attempt }),
                3 => (0_u16..=3)
                    .prop_map(|attempt| SupervisorAction::Heartbeat { attempt }),
                2 => (0_u16..=3, any::<u8>())
                    .prop_map(|(attempt, code)| SupervisorAction::ChildExit { attempt, code }),
                3 => (any::<u8>(), 0_u16..=3)
                    .prop_map(|(kind, attempt)| SupervisorAction::Control { kind, attempt }),
            ],
            0..=22,
        );
        generated.prop_map(|mut actions| {
            actions.extend([
                SupervisorAction::Spawn {
                    attempt: 60_000,
                    succeeds: true,
                },
                SupervisorAction::Spawn {
                    attempt: 60_001,
                    succeeds: false,
                },
                SupervisorAction::Started {
                    attempt: 60_000,
                    pid: 41,
                },
                SupervisorAction::Announce { attempt: 60_000 },
                SupervisorAction::Heartbeat { attempt: 60_000 },
                SupervisorAction::ProviderBlock,
                SupervisorAction::Control {
                    kind: 0,
                    attempt: 60_000,
                },
                SupervisorAction::ChildExit {
                    attempt: 60_000,
                    code: 0,
                },
                SupervisorAction::Shutdown,
                SupervisorAction::Shutdown,
            ]);
            actions
        })
    }

    #[derive(Clone, Default)]
    struct Evidence {
        starts: Arc<Mutex<BTreeMap<u64, usize>>>,
        terminations: Arc<Mutex<BTreeMap<u64, usize>>>,
        relays: Arc<Mutex<BTreeMap<u64, ActorAddress>>>,
        collections: Arc<Mutex<BTreeMap<u64, usize>>>,
    }

    struct GeneratedLogic {
        spec: NodeLaunchSpec,
        manager: NodeManager,
        evidence: Evidence,
    }

    impl BootstrapLogic for GeneratedLogic {
        fn start(
            &mut self,
            ctx: &Ctx,
            owner: ActorAddress,
            _sender: &swactor::runtime::ExternalSender,
        ) -> Result<(), String> {
            let relay = ctx
                .spawn(NodeRelayActor::new(self.manager.clone(), self.spec.attempt))
                .map_err(|error| format!("spawn generated process relay: {error}"))?;
            let replaced = self
                .evidence
                .relays
                .lock()
                .expect("generated relay evidence")
                .insert(self.spec.attempt, relay);
            if replaced.is_some() {
                return Err(format!(
                    "duplicate actor set for attempt {}",
                    self.spec.attempt
                ));
            }
            *self
                .evidence
                .starts
                .lock()
                .expect("generated start evidence")
                .entry(self.spec.attempt)
                .or_default() += 1;
            self.manager.register(NodeRuntime {
                attempt: self.spec.attempt,
                logical_node: self.spec.logical_node.clone(),
                bootstrap: owner,
                pid: None,
                exited: None,
                spawn_failed: None,
                last_announce_ms: None,
                endpoint_addr: None,
            });
            Ok(())
        }

        fn probe(&mut self, _now: SystemTime) -> LogicProbe {
            let Some(runtime) = self.manager.get(self.spec.attempt) else {
                return LogicProbe::Exited("generated runtime removed".to_owned());
            };
            if let Some(reason) = runtime.spawn_failed {
                return LogicProbe::Failed(reason);
            }
            if let Some(status) = runtime.exited {
                return LogicProbe::Exited(format!("{status:?}"));
            }
            LogicProbe::Pending
        }

        fn terminate(
            &mut self,
            _sender: &swactor::runtime::ExternalSender,
            _kill_after: Option<Duration>,
        ) {
            *self
                .evidence
                .terminations
                .lock()
                .expect("generated termination evidence")
                .entry(self.spec.attempt)
                .or_default() += 1;
        }
    }

    impl NodeTelemetryCollector for Evidence {
        fn collect(&self, identity: &NodeIdentity) {
            *self
                .collections
                .lock()
                .expect("generated collection evidence")
                .entry(identity.attempt)
                .or_default() += 1;
        }
    }

    fn drive(backend: &SteppingBackend, steps: usize) {
        for _ in 0..steps {
            backend.step();
        }
    }

    fn null_sink() -> PluginSink {
        struct NullSink;
        impl PluginObservationSink for NullSink {
            fn observe(&self, _observation: PluginObservation) {}
        }
        PluginSink::new(Arc::new(NullSink))
    }

    fn provision_spec(attempt: u64) -> NodeProvisionSpec {
        NodeProvisionSpec {
            run_id: 1,
            node_id: attempt,
            attempt_id: attempt,
            stage_index: None,
            image: "generated-demo-node".to_owned(),
            env: vec![("DEMO_LOGICAL_NODE".to_owned(), format!("node-{attempt}"))],
            args: Vec::new(),
            mounts: Vec::new(),
        }
    }

    fn control_command(kind: u8, attempt: u64, index: usize) -> dashboard::control::ControlCommand {
        let command_id = format!("generated-command-{index}");
        if kind % 2 == 0 {
            dashboard::control::ControlCommand::Kill {
                command_id,
                node: format!("node-{attempt}"),
            }
        } else {
            dashboard::control::ControlCommand::EstablishEdge {
                command_id,
                node: format!("node-{attempt}"),
            }
        }
    }

    fn check_supervisor_invariants(
        identities: &[u64],
        starts: &BTreeMap<u64, usize>,
        relays: &BTreeMap<u64, ActorAddress>,
        collections: &BTreeMap<u64, usize>,
        replies: &[(u64, bool)],
        final_actors: usize,
        worker_panics: u64,
    ) -> Result<(), String> {
        let unique = identities.iter().copied().collect::<BTreeSet<_>>();
        if unique.len() != identities.len() {
            return Err(format!("duplicate manager identity: {identities:?}"));
        }
        for attempt in &unique {
            if starts.get(attempt) != Some(&1) {
                return Err(format!(
                    "attempt {attempt} started {:?} times",
                    starts.get(attempt)
                ));
            }
            if !relays.contains_key(attempt) {
                return Err(format!("attempt {attempt} has no process relay actor"));
            }
            if collections.get(attempt).copied().unwrap_or(0) > 1 {
                return Err(format!(
                    "attempt {attempt} collected telemetry more than once"
                ));
            }
        }
        if collections.get(&60_000) != Some(&1) {
            return Err(format!(
                "announce/heartbeat did not collect canonical identity exactly once: {:?}",
                collections.get(&60_000)
            ));
        }
        if !replies.iter().any(|(_, success)| *success)
            || !replies.iter().any(|(_, success)| !*success)
        {
            return Err(format!(
                "spawn replies did not cover success and failure: {replies:?}"
            ));
        }
        if final_actors != 0 {
            return Err(format!(
                "supervisor resources did not return to baseline: {final_actors}"
            ));
        }
        if worker_panics != 0 {
            return Err(format!(
                "supervisor worker panicked {worker_panics} time(s)"
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
        fn generated_supervisor_transitions_are_once_only_nonblocking_and_clean(
            actions in supervisor_actions(),
        ) {
            let mut config = RuntimeConfig::default();
            config.worker_count = 1;
            let parts = RuntimeParts::new(config);
            let runtime = parts.runtime().clone();
            let sender = runtime.create_sender();
            let backend = SteppingBackend::new();
            let engine =
                Engine::new(parts, backend.clone()).expect("one-worker supervisor stepping engine");
            let manager = NodeManager::new();
            let evidence = Evidence::default();
            let outcomes = Arc::new(Mutex::new(BTreeMap::<u64, bool>::new()));

            let mut registry = provisioning::BootstrapRegistry::new();
            registry.register("process", {
                let manager = manager.clone();
                let evidence = evidence.clone();
                let outcomes = Arc::clone(&outcomes);
                Arc::new(move |spec| {
                    if !outcomes
                        .lock()
                        .expect("generated provider outcomes")
                        .get(&spec.attempt)
                        .copied()
                        .unwrap_or(true)
                    {
                        return Err(format!("generated provider failure {}", spec.attempt));
                    }
                    Ok(Box::new(GeneratedLogic {
                        spec: spec.clone(),
                        manager: manager.clone(),
                        evidence: evidence.clone(),
                    }) as Box<dyn BootstrapLogic>)
                })
            });
            let telemetry = SupervisorTelemetry::new("generated-supervisor");
            let fanout = Arc::new(telemetry::DeliveryFanout::new(32));
            let remote_sub = fanout.subscribe_all(
                "generated-dashboard",
                telemetry::TelemetrySnapshot {
                    streams: Vec::new(),
                    channels: Vec::new(),
                },
            );
            let io_driver = crate::demo::shared_test_driver();
            let driver_handle = Arc::new(crate::demo::DemoDriverHandle {
                supervisor_addr_json: serde_json::to_string(&io_driver.endpoint_addr())
                    .expect("serialize generated supervisor endpoint"),
                driver: io_driver,
            });
            let shape = ClusterShape {
                run_id: RunId(1),
                generation: 1,
                groups: Vec::new(),
            };
            let driver =
                ClusterDriver::new(shape, demo_retry_policy()).expect("generated cluster driver");
            let plugin = Arc::new(Mutex::new(DemoProvider::new(manager.clone())));
            let executor = IdempotentEffectExecutor::new(
                DemoBackend {
                    plugin: Arc::clone(&plugin),
                    manager: manager.clone(),
                    sender: sender.clone(),
                },
                EngineSpawner::new(&engine.handle()),
            );
            let dashboard = dashboard::DashboardHandle::new(dashboard::DashboardConfig {
                port: 0,
                ..dashboard::DashboardConfig::default()
            });
            let edge_inbox = runtime
                .new_inbox::<EdgePumpMessage>()
                .expect("create generated edge inbox");
            let announce = runtime
                .spawn(AnnounceActor::new(manager.clone(), sender.clone()))
                .expect("spawn generated announce actor");
            let supervisor = runtime
                .spawn(SupervisorActor::new(
                    driver,
                    executor,
                    manager.clone(),
                    driver_handle,
                    telemetry,
                    dashboard,
                    sender.clone(),
                    registry,
                    Arc::new(evidence.clone()),
                    engine.handle(),
                    remote_sub,
                    Vec::new(),
                    RunId(1),
                    crate::demo::LaunchStyle::Process {
                        exe: "generated-demo-node".into(),
                    },
                    *edge_inbox.addr(),
                ))
                .expect("spawn generated supervisor actor");
            drive(&backend, 8);

            let mut replies = Vec::new();
            let mut shutdown = None;
            let mut provider_gate: Option<mpsc::SyncSender<()>> = None;
            for (index, action) in actions.iter().enumerate() {
                match *action {
                    SupervisorAction::Spawn { attempt, succeeds } => {
                        let attempt = u64::from(attempt);
                        if manager.get(attempt).is_some() {
                            let actors_before = runtime.stats().actors.len();
                            let handle = plugin
                                .lock()
                                .expect("generated demo provider")
                                .create_node(provision_spec(attempt), null_sink())
                                .expect("adopt generated demo node");
                            prop_assert_eq!(
                                handle.id,
                                attempt,
                                "adopted wrong identity; actions={:?}",
                                actions
                            );
                            prop_assert_eq!(
                                runtime.stats().actors.len(),
                                actors_before,
                                "provider adoption duplicated actor resources; actions={:?} \
                                 attempt={} census={:?}",
                                actions,
                                attempt,
                                runtime.stats()
                            );
                            continue;
                        }
                        outcomes
                            .lock()
                            .expect("generated provider outcomes")
                            .insert(attempt, succeeds);
                        let actors_before = runtime.stats().actors.len();
                        let reply = ActorCompletion::new();
                        runtime
                            .send_to(
                                supervisor,
                                SupervisorMsg::Spawn(SpawnNodeRequest {
                                    attempt,
                                    logical_node: format!("node-{attempt}"),
                                    reply: reply.clone(),
                                }),
                            )
                            .expect("send generated spawn request");
                        drive(&backend, 12);
                        prop_assert!(
                            reply.complete(Err("spawn completion probe".to_owned())).is_err(),
                            "spawn request did not complete within fixed budget; \
                             actions={:?} attempt={} census={:?}",
                            actions,
                            attempt,
                            runtime.stats()
                        );
                        let result = reply.wait();
                        replies.push((attempt, result.is_ok()));
                        match result {
                            Ok(runtime_node) => {
                                prop_assert!(
                                    succeeds,
                                    "provider unexpectedly succeeded; actions={:?}",
                                    actions
                                );
                                runtime
                                    .send_to(runtime_node.bootstrap, provisioning::BootstrapMsg::Start)
                                    .expect("start generated bootstrap actor");
                                drive(&backend, 12);
                                prop_assert!(
                                    manager.get(attempt).is_some(),
                                    "successful provider did not register identity; actions={:?} \
                                     attempt={} replies={:?}",
                                    actions,
                                    attempt,
                                    replies
                                );
                                prop_assert_eq!(
                                    runtime.stats().actors.len(),
                                    actors_before + 2,
                                    "identity did not own exactly bootstrap+relay resources; \
                                     actions={:?} attempt={} census={:?}",
                                    actions,
                                    attempt,
                                    runtime.stats()
                                );
                            }
                            Err(_) => {
                                prop_assert!(
                                    !succeeds,
                                    "provider unexpectedly failed; actions={:?}",
                                    actions
                                );
                                prop_assert_eq!(
                                    runtime.stats().actors.len(),
                                    actors_before,
                                    "failed provider leaked actor resources; actions={:?} \
                                     attempt={} census={:?}",
                                    actions,
                                    attempt,
                                    runtime.stats()
                                );
                            }
                        }
                    }
                    SupervisorAction::Started { attempt, pid } => {
                        if let Some(relay) = evidence
                            .relays
                            .lock()
                            .expect("generated relay evidence")
                            .get(&u64::from(attempt))
                            .copied()
                        {
                            let _ = runtime.send_to(
                                relay,
                                ProcessOutput::Started {
                                    pid: u32::from(pid) + 1,
                                },
                            );
                            drive(&backend, 4);
                        }
                    }
                    SupervisorAction::Announce { attempt }
                    | SupervisorAction::Heartbeat { attempt } => {
                        let attempt = u64::from(attempt);
                        let _ = runtime.send_to(
                            announce,
                            crate::demo::node::NodeAnnounce {
                                attempt,
                                logical_node: format!("node-{attempt}"),
                                key_hex: format!("{attempt:016x}"),
                                endpoint_addr_json: serde_json::to_string(
                                    &crate::demo::shared_test_driver().endpoint_addr(),
                                )
                                .expect("serialize generated announce endpoint"),
                                at_ms: index as u64 + 1,
                            },
                        );
                        drive(&backend, 8);
                    }
                    SupervisorAction::ChildExit { attempt, code } => {
                        let attempt = u64::from(attempt);
                        if let Some(relay) = evidence
                            .relays
                            .lock()
                            .expect("generated relay evidence")
                            .get(&attempt)
                            .copied()
                        {
                            let _ = runtime.send_to(
                                relay,
                                ProcessOutput::Exited {
                                    status: ExitStatus::Code(i32::from(code)),
                                },
                            );
                            drive(&backend, 4);
                        }
                        if let Some(runtime_node) = manager.get(attempt) {
                            let _ = runtime.send_to(
                                runtime_node.bootstrap,
                                provisioning::BootstrapMsg::Probe,
                            );
                            drive(&backend, 8);
                        }
                    }
                    SupervisorAction::Control { kind, attempt } => {
                        let attempt = u64::from(attempt);
                        let terminations_before = evidence
                            .terminations
                            .lock()
                            .expect("generated termination evidence")
                            .get(&attempt)
                            .copied()
                            .unwrap_or(0);
                        runtime
                            .send_to(
                                supervisor,
                                SupervisorMsg::Control(control_command(kind, attempt, index)),
                            )
                            .expect("send generated dashboard control");
                        drive(&backend, 8);
                        if let Some(release) = provider_gate.take() {
                            let terminations_after = evidence
                                .terminations
                                .lock()
                                .expect("generated termination evidence")
                                .get(&attempt)
                                .copied()
                                .unwrap_or(0);
                            prop_assert!(
                                terminations_after > terminations_before,
                                "blocking provider work prevented supervisor control progress; \
                                 actions={:?} attempt={} terminations={:?} census={:?}",
                                actions,
                                attempt,
                                evidence.terminations,
                                runtime.stats()
                            );
                            release
                                .send(())
                                .expect("release generated blocking provider work");
                        }
                    }
                    SupervisorAction::ProviderBlock => {
                        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
                        let (release_tx, release_rx) = mpsc::sync_channel(1);
                        EngineSpawner::new(&engine.handle())
                            .spawn_blocking(Box::new(move || {
                                let _ = entered_tx.send(());
                                let _ = release_rx.recv();
                            }))
                            .expect("spawn generated blocking provider work");
                        entered_rx
                            .recv_timeout(Duration::from_secs(2))
                            .expect("blocking provider work did not enter within hard budget");
                        provider_gate = Some(release_tx);
                    }
                    SupervisorAction::Shutdown => {
                        let completion = shutdown.get_or_insert_with(ActorCompletion::new);
                        runtime
                            .send_to(
                                supervisor,
                                SupervisorMsg::Shutdown {
                                    completion: completion.clone(),
                                },
                            )
                            .expect("send generated supervisor shutdown");
                        drive(&backend, 4);
                    }
                }
            }
            prop_assert!(
                provider_gate.is_none(),
                "provider gate was not released; actions={:?}",
                actions
            );

            let canonical = manager
                .get(60_000)
                .expect("canonical generated identity remains registered through shutdown");
            prop_assert_eq!(
                canonical.pid,
                Some(42),
                "provider Started report was lost; actions={:?} replies={:?} \
                 census={:?}",
                actions,
                replies,
                runtime.stats()
            );
            prop_assert_eq!(
                canonical.exited.as_ref(),
                Some(&ExitStatus::Code(0)),
                "child exit report was lost; actions={:?} replies={:?} census={:?}",
                actions,
                replies,
                runtime.stats()
            );
            prop_assert!(
                canonical.last_announce_ms.is_some() && canonical.endpoint_addr.is_some(),
                "announce/heartbeat facts were lost; actions={:?} replies={:?} \
                 last_announce={:?} endpoint={:?} census={:?}",
                actions,
                replies,
                canonical.last_announce_ms,
                canonical.endpoint_addr,
                runtime.stats()
            );

            let identities = manager
                .nodes()
                .into_iter()
                .map(|node| node.attempt)
                .collect::<Vec<_>>();
            let starts = evidence
                .starts
                .lock()
                .expect("generated start evidence")
                .clone();
            let relays = evidence
                .relays
                .lock()
                .expect("generated relay evidence")
                .clone();
            let collections = evidence
                .collections
                .lock()
                .expect("generated collection evidence")
                .clone();

            for runtime_node in manager.nodes() {
                manager.set_exited(runtime_node.attempt, ExitStatus::Code(0));
                let _ = runtime.send_to(
                    runtime_node.bootstrap,
                    provisioning::BootstrapMsg::Stop {
                        kill_after: Some(Duration::ZERO),
                    },
                );
                let _ = runtime.send_to(
                    runtime_node.bootstrap,
                    provisioning::BootstrapMsg::Probe,
                );
                manager.remove(runtime_node.attempt);
            }
            for relay in relays.values().copied() {
                runtime
                    .stop_actor(relay)
                    .expect("stop generated process relay");
            }
            runtime
                .stop_actor(announce)
                .expect("stop generated announce actor");
            drive(&backend, 32);

            for _ in 0..12 {
                backend.advance_time(crate::demo::TICK);
                drive(&backend, 16);
            }
            let shutdown = shutdown.expect("bounded action suffix installs shutdown");
            prop_assert!(
                shutdown.complete(()).is_err(),
                "supervisor shutdown did not complete within fixed budget; \
                 actions={:?} identities={:?} replies={:?} census={:?}",
                actions,
                identities,
                replies,
                runtime.stats()
            );
            shutdown.wait();
            prop_assert!(
                shutdown.complete(()).is_err(),
                "supervisor shutdown completion accepted a second terminal value; actions={:?}",
                actions
            );

            let final_stats = runtime.stats();
            let worker_panics = final_stats
                .workers
                .iter()
                .map(|worker| worker.panics)
                .sum::<u64>();
            prop_assert!(
                check_supervisor_invariants(
                    &identities,
                    &starts,
                    &relays,
                    &collections,
                    &replies,
                    final_stats.actors.len(),
                    worker_panics,
                )
                .is_ok(),
                "supervisor invariant failed; actions={:?} identities={:?} \
                 starts={:?} relays={:?} collections={:?} \
                 replies={:?} census={:?}",
                actions,
                identities,
                starts,
                relays,
                collections,
                replies,
                final_stats
            );
        }
    }

    #[test]
    fn supervisor_transition_oracle_rejects_duplicate_identity_resources() {
        let starts = BTreeMap::from([(7, 2)]);
        let relays = BTreeMap::from([(7, ActorAddress::default())]);
        let collections = BTreeMap::from([(60_000, 1)]);
        let rejected = check_supervisor_invariants(
            &[7],
            &starts,
            &relays,
            &collections,
            &[(7, true), (8, false)],
            0,
            0,
        );
        assert!(
            rejected.is_err(),
            "supervisor property oracle accepted a controlled duplicate resource set"
        );
    }
}
