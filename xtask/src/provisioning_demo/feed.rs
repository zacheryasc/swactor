//! The provisioning supervisor: an actor that owns the `ClusterDriver`, the
//! demo provider, the desired shape, and the 250ms tick.
//!
//! Each tick mirrors the production `ClusterReconciler` poll semantics:
//! drain executor results (closing bootstrap sessions after convergence),
//! classify due operations, requeue, drive until blocked — then feeds the
//! real world back in (key-file observations, iroh join checks, child exits),
//! emits `prov.reconciler.*` telemetry, and drains every telemetry endpoint
//! into the dashboard.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use serde_json::json;
use swactor::actor::{ActorInterface, Ctx};
use swactor_engine::EngineHandle;
use swactor_process::{spawn_local_process, ProcessOutputConfig, ProcessSpec};

use provisioning::executor::{
    BlockingEffectSpawner, BlockingEffectWork, ExecutorOperationStatus,
    IdempotentEffectExecutor,
};
use provisioning::node::{
    BootstrapObservation, BootstrapStage, NodeGroupId,
    NodeStage, RoleId, RunId, SwactorId,
};
use provisioning::reconciler::{
    ClusterShape, NodeObservation, RetryPolicy, PlannedEffect, OperationOutcome,
};
use provisioning::reconciler::{ClusterDriver, EffectExecutor};
use telemetry::{ChannelContent, StreamDescriptor, TelemetryEndpoint, TelemetryProducer};

use crate::provisioning_demo::node::read_key_report;
use crate::provisioning_demo::provider::{
    register_node_channels, unix_ms, DemoBackend, NodeManager, NodeRelayActor,
    NodeTelemetry,
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

/// Engine-backed spawner for executor blocking work.
#[derive(Clone)]
pub struct EngineSpawner {
    engine: EngineHandle,
}

impl EngineSpawner {
    pub fn new(engine: EngineHandle) -> Self {
        Self { engine }
    }
}

impl BlockingEffectSpawner for EngineSpawner {
    type SpawnError = std::convert::Infallible;

    fn spawn_blocking(&self, work: BlockingEffectWork) -> Result<(), Self::SpawnError> {
        self.engine.spawn_blocking(move || work());
        Ok(())
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
    Spawn(crate::provisioning_demo::provider::SpawnNodeRequest),
    /// Drain the cluster: desired → empty, stop every child, flag when done.
    Shutdown {
        drained: std::sync::Arc<std::sync::atomic::AtomicBool>,
    },
}

/// Per-node telemetry handle kept while the attempt is live.
struct NodeStreams {
    telemetry: NodeTelemetry,
    status_channel: telemetry::ChannelId,
}

/// The provisioning supervisor actor.
pub struct SupervisorActor {
    pub driver: ClusterDriver,
    pub executor: IdempotentEffectExecutor<DemoBackend, EngineSpawner>,
    pub manager: NodeManager,
    pub driver_handle: std::sync::Arc<crate::provisioning_demo::DemoDriverHandle>,
    pub telemetry: SupervisorTelemetry,
    pub events_channel: telemetry::ChannelId,
    pub snapshot_channel: telemetry::ChannelId,
    pub sender: swactor::runtime::ExternalSender,
    /// Desired shape slots: singleton groups, one per logical node.
    slots: Vec<String>,
    slot_seq: u64,
    run_id: RunId,
    nodes: BTreeMap<u64, NodeStreams>,
    node_life: u64,
    last_stages: BTreeMap<String, (NodeStage, Option<BootstrapStage>)>,
    pub dashboard: dashboard::DashboardHandle,
    status_tick: u64,
    exe: std::path::PathBuf,
}

impl SupervisorActor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        driver: ClusterDriver,
        executor: IdempotentEffectExecutor<DemoBackend, EngineSpawner>,
        manager: NodeManager,
        driver_handle: std::sync::Arc<crate::provisioning_demo::DemoDriverHandle>,
        mut telemetry: SupervisorTelemetry,
        dashboard: dashboard::DashboardHandle,
        sender: swactor::runtime::ExternalSender,
        initial_slots: Vec<String>,
        run_id: RunId,
        exe: std::path::PathBuf,
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
            nodes: BTreeMap::new(),
            run_id,
            slot_seq: initial_slots.len() as u64,
            slots: initial_slots,
            node_life: 0,
            last_stages: BTreeMap::new(),
            dashboard,
            status_tick: 0,
            exe,
        }
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
        self.telemetry.producer.submit_bytes(self.events_channel, bytes);
    }

    /// Handle a spawn request from the provider (runs in actor context).
    fn spawn_node(&mut self, ctx: &Ctx, request: crate::provisioning_demo::provider::SpawnNodeRequest) {
        let attempt = request.attempt;
        let relay = match ctx.spawn(NodeRelayActor::new(self.manager.clone(), attempt)) {
            Ok(addr) => addr,
            Err(error) => {
                let _ = request.reply.send(Err(format!("spawn relay actor: {error}")));
                return;
            }
        };

        let spec = ProcessSpec {
            command: self.exe.to_string_lossy().to_string(),
            args: vec![
                "provisioning-reconciler-demo".to_owned(),
                "--demo-node".to_owned(),
                self.driver_handle.supervisor_addr_json.clone(),
            ],
            env: [
                (
                    "DEMO_NODE_KEY_FILE".to_owned(),
                    request.key_file.to_string_lossy().to_string(),
                ),
                ("DEMO_NODE_ID".to_owned(), request.logical_node.clone()),
            ]
            .into_iter()
            .collect(),
            working_dir: None,
            label: Some(request.logical_node.clone()),
        };

        self.node_life += 1;
        let telemetry = NodeTelemetry::new(&request.logical_node, self.node_life);
        let status_channel = register_node_channels(&telemetry.producer);
        // The lifecycle channel is registered by the process crate with the
        // sanitized label; mirror it for name resolution during drain.
        let output =
            ProcessOutputConfig::telemetry_mirror(relay, telemetry.producer.clone());
        match spawn_local_process(ctx, &self.sender, spec, output) {
            Ok(process_actor) => {
                self.nodes.insert(
                    attempt,
                    NodeStreams {
                        telemetry,
                        status_channel,
                    },
                );
                self.manager.register(
                    crate::provisioning_demo::provider::NodeRuntime {
                        attempt,
                        logical_node: request.logical_node.clone(),
                        process_actor,
                        key_file: request.key_file.clone(),
                        pid: None,
                        exited: None,
                        spawn_failed: None,
                    },
                );
                let _ = request.reply.send(Ok(
                    crate::provisioning_demo::provider::NodeRuntime {
                        attempt,
                        logical_node: request.logical_node,
                        process_actor,
                        key_file: request.key_file,
                        pid: None,
                        exited: None,
                        spawn_failed: None,
                    },
                ));
            }
            Err(error) => {
                let _ = request.reply.send(Err(format!("spawn process actor: {error}")));
            }
        }
    }

    /// Feed real-world observations into the driver.
    fn observe_world(&mut self, now: SystemTime) {
        let node_ids: Vec<String> = self
            .driver
            .state()
            .nodes
            .keys()
            .map(|id| id.0.clone())
            .collect();
        for node_id in node_ids {
            let Some(managed) = self.driver.state().nodes.get(&provisioning::node::LogicalNodeId(node_id.clone())) else {
                continue;
            };
            let attempt = managed.attempt;
            let stage = managed.record.stage;
            let active_bootstrap = managed.active_bootstrap;
            let runtime = match self.manager.get(attempt.0) {
                Some(runtime) => runtime,
                None => continue,
            };

            // Child exit or spawn failure: fail the attempt while it is
            // still bootstrapping so the reconciler retries with a fresh
            // lease instead of wedging at SshReady forever.
            if runtime.exited.is_some() || runtime.spawn_failed.is_some() {
                if stage != NodeStage::Failed && active_bootstrap.is_some() {
                    let reason = if let Some(status) = &runtime.exited {
                        format!("node process exited: {status:?}")
                    } else {
                        format!(
                            "node process failed to spawn: {}",
                            runtime.spawn_failed.as_deref().unwrap_or("unknown")
                        )
                    };
                    self.emit_event("observation", &node_id, reason.clone());
                    self.driver.apply_observation(
                        &provisioning::node::LogicalNodeId(node_id.clone()),
                        attempt,
                        NodeObservation::BootstrapFailed {
                            session_id: active_bootstrap.expect("checked above"),
                            reason,
                        },
                        now,
                    );
                }
                continue;
            }

            // Bootstrap progression from the key file and iroh join state.
            if let Some(session_id) = active_bootstrap {
                let report = read_key_report(&runtime.key_file);
                let mut stage_seen = BootstrapStage::SshReady;
                if let Some(report) = &report {
                    let connected = report
                        .node_hex
                        .parse_key()
                        .is_some_and(|key| self.driver_handle.has_active_connection(key));
                    if connected {
                        let heartbeat_age = unix_ms(now).saturating_sub(report.last_seen_ms);
                        self.emit_event(
                            "observation",
                            &node_id,
                            format!(
                                "swactor join confirmed (key {}…, heartbeat {}ms old)",
                                &report.node_hex[..8.min(report.node_hex.len())],
                                heartbeat_age
                            ),
                        );
                        let swactor_id = SwactorId(report.node_hex.clone());
                        self.driver.apply_observation(
                            &provisioning::node::LogicalNodeId(node_id.clone()),
                            attempt,
                            NodeObservation::SwactorJoined {
                                session_id,
                                swactor_id,
                            },
                            now,
                        );
                        continue;
                    }
                    stage_seen = BootstrapStage::WaitingForSwactorJoin;
                }
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
    }

    /// Replace ready nodes whose child has died (shape-native replacement:
    /// retire the dead singleton slot and add a fresh one).
    fn replace_dead_ready_nodes(&mut self, now: SystemTime) {
        let state = self.driver.state().clone();
        let mut replacements: Vec<(String, String)> = Vec::new();
        for (id, managed) in &state.nodes {
            if managed.record.ready && managed.intent == provisioning::reconciler::NodeIntent::Active
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
            self.emit_event("control", dead, format!("runtime death; replacing as {fresh}"));
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
            let report = read_key_report(&runtime.key_file);
            let heartbeat_ms_ago = report
                .as_ref()
                .map(|r| unix_ms(now).saturating_sub(r.last_seen_ms))
                .unwrap_or(u64::MAX);
            let payload = json!({
                "at_ms": unix_ms(now),
                "node": runtime.logical_node,
                "alive": runtime.exited.is_none(),
                "pid": runtime.pid,
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

        let snapshot = json!({
            "at_ms": unix_ms(now),
            "desired": self.slots.len(),
            "ready": ready_count,
            "generation": self.driver.desired().generation,
            "converged": self.driver.is_converged(),
            "nodes": nodes_json,
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

    fn handle(&mut self, ctx: &Ctx, msg: SupervisorMsg) {
        match msg {
            SupervisorMsg::Tick => {
                let now = SystemTime::now();
                self.poll(now);
                self.observe_world(now);
                self.replace_dead_ready_nodes(now);
                self.poll(now);
                self.emit_node_status(now);
                self.emit_feed(now);
                self.flush_telemetry();
            }
            SupervisorMsg::Control(command) => self.handle_control(command),
            SupervisorMsg::Spawn(request) => self.spawn_node(ctx, request),
            SupervisorMsg::Shutdown { drained } => {
                self.slots.clear();
                let generation = self.driver.desired().generation.saturating_add(1);
                if let Err(error) = self.driver.update_desired(self.desired_shape(generation)) {
                    eprintln!("demo: shutdown update_desired failed: {error}");
                }
                self.emit_event("control", "", "shutdown: desired → empty".to_owned());
                drained.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }
}

impl SupervisorActor {
    fn handle_control(&mut self, command: dashboard::control::ControlCommand) {
        match command {
            dashboard::control::ControlCommand::Kill { node } => {
                match self.manager.find_by_stream_node(&node) {
                    Some(runtime) => {
                        self.emit_event(
                            "control",
                            &node,
                            format!("kill requested (pid {:?})", runtime.pid),
                        );
                        let _ = swactor_process::send_process_command(
                            &self.sender,
                            runtime.process_actor,
                            swactor_process::ProcessCommand::Stop {
                                kill_after: Some(Duration::ZERO),
                            },
                        );
                    }
                    None => self.emit_event("control", &node, "kill: unknown node".to_owned()),
                }
            }
            dashboard::control::ControlCommand::Remove { count } => {
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
            dashboard::control::ControlCommand::Provision { count } => {
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
        }
    }
}

/// Parse a hex node key into a transport NodeId.
trait ParseKey {
    fn parse_key(&self) -> Option<swactor_transport::NodeId>;
}

impl ParseKey for String {
    fn parse_key(&self) -> Option<swactor_transport::NodeId> {
        let bytes = swactor_transport::hex_decode(self)?;
        let array: [u8; 32] = bytes.try_into().ok()?;
        Some(swactor_transport::NodeId(array))
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
            start_swactor_command: "xtask provisioning-reconciler-demo".to_owned(),
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

