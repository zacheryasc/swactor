use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitCode, Stdio};
use std::sync::{
    Arc,
    mpsc::{self, Receiver, Sender},
};
use std::thread;
use std::time::{Duration, Instant};

use dashboard::swactor::{RUNTIME_ACTORS, RUNTIME_STATS, RUNTIME_WORKERS};
use datastream::frame::{ChannelId, Frame, Position};
use datastream::{
    DatastreamEndpoint, DatastreamProducer, DatastreamSubscription, Lifetime, NodeId, StreamId,
};
use distribution::node::DistributedNodeConfig;
use iroh::EndpointAddr;
use iroh_driver::{IrohDriver, IrohDriverConfig};
use mvp_system::actors::node_agent::{
    NodeAgentActor, NodeAgentMsg, NodeAgentReport, StageCommandWire, StageProvisionWire,
};
use mvp_system::actors::orchestrator::{
    LifecycleEventWire, OrchestratorActor, OrchestratorMsg, OrchestratorReport, RunCommandWire,
    StageRefWire,
};
use mvp_system::actors::register_mvp_actor_codecs;
use mvp_system::arena_manager as arena;
use mvp_system::dashboard_view::MvpClusterDashboardView;
use mvp_system::distribution_stack::DistributionRuntimeStack;
use mvp_system::docker_cluster_provisioning as docker_provision;
use mvp_system::driver_pumps as driver_model;
use mvp_system::edge_establisher as edge;
use mvp_system::engine_builder as engine;
use mvp_system::gpu_worker_ctl as worker_ctl;
use mvp_system::gpu_worker_ingress_parser as ingress;
use mvp_system::node_provisioning as node_provision;
use mvp_system::observability_surface as obs;
use mvp_system::orchestrator_run_fsm as fsm;
use mvp_system::provisioning::{NodeProvisionSpec, ProvisionLogStream};
use mvp_system::run_plan as plan;
use mvp_system::stage_controller as stage;
use mvp_system::tx_rx_edge_actor as edge_actor;
use serde::Deserialize;
use serde_json::{Value, json};
use swactor::actor::ActorAddress;
use tokio::sync::mpsc as tokio_mpsc;

const RUN_ID: u64 = 77;
const ORCHESTRATOR_LOGICAL_NODE_ID: u64 = 900;
const NODE0_LOGICAL_ID: u64 = 11;
const NODE1_LOGICAL_ID: u64 = 12;
const MAX_TOKENS: u64 = 1;
const DEFAULT_PROMPT: &str = "ping";
const DEFAULT_DOCKER_IMAGE: &str = "swactor-mvp-local-e2e-cluster:latest";
const EDGE_ALPN: &[u8] = b"swactor/edge/1";
const OBJECT_MAX_EXTENT: u64 = 16;
const OBJECT_ALIGNMENT: u64 = 4;
const ARENA_BYTES: usize = 16 * 1024;
const RING_BYTES: usize = 4096;
const EDGE_READY_TIMEOUT: Duration = Duration::from_secs(20);
const DEFAULT_RUNTIME_SNAPSHOT_INTERVAL: Duration = Duration::from_millis(500);

struct MvpDashboard {
    url: String,
    endpoint: DatastreamEndpoint,
    producer: DatastreamProducer,
    subscription: DatastreamSubscription,
    handle: dashboard::DashboardHandle,
    runtime_position: u64,
    runtime_snapshot_interval: Duration,
    last_runtime_snapshot: Option<Instant>,
}

impl MvpDashboard {
    fn start_from_env() -> Result<Self, String> {
        let mut config = dashboard::DashboardConfig::default();
        if let Some(port) = std::env::var_os("MVP_DASHBOARD_PORT") {
            let port = port
                .to_string_lossy()
                .parse::<u16>()
                .map_err(|e| format!("invalid MVP_DASHBOARD_PORT: {e}"))?;
            config.port = port;
        }
        let runtime_snapshot_interval = runtime_snapshot_interval_from_env()?;
        let url = format!("http://127.0.0.1:{}/view/datastream/live", config.port);
        let handle = dashboard::start_dashboard(config.clone());
        handle.register_view(Arc::new(MvpClusterDashboardView::new()));
        handle.start_http_standalone();
        let endpoint = DatastreamEndpoint::with_capacity(
            StreamId::new(NodeId::new("mvp-system-orch"), Lifetime(1)),
            4096,
            config.frame_buffer,
        );
        let producer = endpoint.producer();
        let subscription = endpoint.subscribe_all("dashboard");
        Ok(Self {
            url,
            endpoint,
            producer,
            subscription,
            handle,
            runtime_position: 0,
            runtime_snapshot_interval,
            last_runtime_snapshot: None,
        })
    }

    fn url(&self) -> &str {
        &self.url
    }

    fn drain(&mut self) {
        self.endpoint.tick();
        for delivery in self.subscription.drain_available() {
            self.handle.ingest(&delivery.stream, &delivery.frame);
        }
    }

    fn record_event(&mut self, event: obs::Event) {
        let record = mvp_system::telemetry::MvpLifecycleRecord::new(event);
        self.producer.submit_record(&record);
        self.drain();
    }

    fn record_provision_log(&mut self, line: mvp_system::provisioning::ProvisionLogLine) {
        let channel = mvp_system::telemetry::mvp_provision_log_channel(line.node_id, line.stream);
        let record = mvp_system::telemetry::MvpProvisionLogRecord::new(line);
        let payload = serde_json::to_vec(&record).expect("serialize provisioning log record");
        self.producer.submit_bytes(channel, payload);
        self.drain();
    }

    fn publish_runtime_snapshot_throttled(&mut self, stack: &DistributionRuntimeStack) {
        let due = self.last_runtime_snapshot.map_or(true, |last| {
            last.elapsed() >= self.runtime_snapshot_interval
        });
        if self.runtime_snapshot_interval.is_zero() || due {
            self.publish_runtime_snapshot(stack);
        }
    }

    fn publish_runtime_snapshot(&mut self, stack: &DistributionRuntimeStack) {
        let stats = stack.runtime.stats();
        let actors = stats
            .actors
            .iter()
            .map(|(address, worker_id)| json!([address.to_string(), worker_id]))
            .collect::<Vec<_>>();
        let total_mailbox_depth = stats
            .workers
            .iter()
            .map(|worker| worker.mailbox_depth)
            .sum::<usize>();

        self.ingest_runtime_json(
            RUNTIME_STATS,
            json!({
                "num_workers": stats.num_workers,
                "uptime_ms": stats.uptime_ms,
                "actors_live": stats.actors.len(),
                "mailbox_depth": total_mailbox_depth,
                "actors": actors,
                "workers": &stats.workers,
                "actor_details": &stats.actor_details,
                "tick_timings": &stats.tick_timings,
            }),
        );
        self.ingest_runtime_json(RUNTIME_WORKERS, json!({ "workers": &stats.workers }));
        if !stats.actor_details.is_empty() {
            self.ingest_runtime_json(RUNTIME_ACTORS, json!({ "actors": &stats.actor_details }));
        }
        self.last_runtime_snapshot = Some(Instant::now());
    }

    fn ingest_runtime_json(&mut self, channel: &str, value: Value) {
        let payload = serde_json::to_vec(&value).expect("serialize runtime dashboard frame");
        let frame = Frame::new(
            ChannelId::new(channel),
            Position(self.runtime_position),
            payload,
        );
        self.runtime_position = self.runtime_position.wrapping_add(1);
        self.handle.ingest(self.endpoint.stream_id(), &frame);
    }
}

fn runtime_snapshot_interval_from_env() -> Result<Duration, String> {
    let Some(value) = std::env::var_os("MVP_RUNTIME_SNAPSHOT_MS") else {
        return Ok(DEFAULT_RUNTIME_SNAPSHOT_INTERVAL);
    };
    let millis = value
        .to_string_lossy()
        .parse::<u64>()
        .map_err(|e| format!("invalid MVP_RUNTIME_SNAPSHOT_MS: {e}"))?;
    Ok(Duration::from_millis(millis))
}

fn main() -> ExitCode {
    let args = std::env::args().collect::<Vec<_>>();
    let result = if args.iter().any(|arg| arg == "--role=node") {
        run_node_role(&args)
    } else if std::env::var_os("MVP_DASHBOARD").is_some() {
        run_supervisor_dashboard_loop()
    } else {
        let prompt = parse_optional_arg(&args, "--prompt")
            .unwrap_or(DEFAULT_PROMPT)
            .to_owned();
        run_supervisor_once(RUN_ID, None, true, prompt)
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mvp-local-e2e-cluster: {error}");
            ExitCode::from(1)
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct NodeStdoutLine {
    #[serde(rename = "type")]
    kind: String,
    stage_index: Option<u32>,
    event: Option<String>,
    endpoint: Option<EndpointAddr>,
    node_actor: Option<ActorAddress>,
    logical_node_id: Option<u64>,
}

type LocalDockerNodeProvisioner = docker_provision::DockerNodeProvisioner<
    LocalE2eDockerCli,
    LocalE2eBootstrapFactory,
    LocalE2eBootstrapDatastream,
>;

struct ProvisionedDockerNode {
    node_id: u64,
    stage_index: u32,
    endpoint: EndpointAddr,
    node_actor: ActorAddress,
    provider_process_id: Option<u32>,
    provisioner: LocalDockerNodeProvisioner,
    events: Receiver<LocalDockerNodeEvent>,
}

#[derive(Default)]
struct ProvisionStats {
    node_live_count: usize,
    stdout_line_count: usize,
    stderr_line_count: usize,
}

#[derive(Clone, Debug)]
enum LocalDockerNodeEvent {
    Stdout(String),
    Stderr(String),
    Exited(Option<i32>),
}

#[derive(Clone, Debug)]
enum DriverIngressEvent {
    StreamArrived {
        edge_id: u64,
        stream_id: u64,
    },
    BytesRead {
        edge_id: u64,
        stream_id: u64,
        bytes: Vec<u8>,
    },
}

#[derive(Clone)]
struct SendPumpHandle {
    tx: tokio_mpsc::UnboundedSender<Vec<u8>>,
}

impl SendPumpHandle {
    fn send(&self, record: Vec<u8>) -> Result<(), String> {
        self.tx
            .send(record)
            .map_err(|_| "edge sender task stopped".to_owned())
    }
}

struct DriverRuntime {
    tx: Sender<DriverIngressEvent>,
    rx: Receiver<DriverIngressEvent>,
    next_stream_id: u64,
    connection_count: usize,
}

impl DriverRuntime {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            tx,
            rx,
            next_stream_id: 1,
            connection_count: 0,
        }
    }

    fn poll_iroh(&mut self, driver: &IrohDriver) {
        for (_node, conn) in driver.drain_other_connections() {
            self.connection_count += 1;
            spawn_recv_pump(
                driver.tokio_handle(),
                conn,
                self.tx.clone(),
                self.next_stream_id,
            );
            self.next_stream_id += 1;
        }
    }

    fn try_recv(&self) -> Option<DriverIngressEvent> {
        self.rx.try_recv().ok()
    }
}

fn record_dashboard_event(dashboard: &mut Option<&mut MvpDashboard>, event: obs::Event) {
    if let Some(dashboard) = dashboard.as_deref_mut() {
        dashboard.record_event(event);
    }
}
fn record_dashboard_provision_log(
    dashboard: &mut Option<&mut MvpDashboard>,
    run_id: u64,
    node_id: u64,
    stream: ProvisionLogStream,
    line: &str,
) {
    if let Some(dashboard) = dashboard.as_deref_mut() {
        dashboard.record_provision_log(mvp_system::provisioning::ProvisionLogLine {
            run_id,
            node_id,
            stream,
            line: line.to_owned(),
        });
    }
}

fn drain_dashboard(dashboard: &mut Option<&mut MvpDashboard>) {
    if let Some(dashboard) = dashboard.as_deref_mut() {
        dashboard.drain();
    }
}

fn publish_runtime_snapshot(
    dashboard: &mut Option<&mut MvpDashboard>,
    stack: &DistributionRuntimeStack,
) {
    if let Some(dashboard) = dashboard.as_deref_mut() {
        dashboard.publish_runtime_snapshot(stack);
    }
}

fn publish_runtime_snapshot_throttled(
    dashboard: &mut Option<&mut MvpDashboard>,
    stack: &DistributionRuntimeStack,
) {
    if let Some(dashboard) = dashboard.as_deref_mut() {
        dashboard.publish_runtime_snapshot_throttled(stack);
    }
}

fn run_event(run_id: u64, kind: obs::EventKind) -> obs::Event {
    obs::Event::RunScoped {
        kind,
        run_id: obs::RunId(run_id),
        reason: None,
        component: obs::Component::Orchestrator,
    }
}

fn run_fault_event(run_id: u64) -> obs::Event {
    obs::Event::RunScoped {
        kind: obs::EventKind::RunFaulted,
        run_id: obs::RunId(run_id),
        reason: None,
        component: obs::Component::Orchestrator,
    }
}

fn node_event(node_id: u64, kind: obs::EventKind) -> obs::Event {
    obs::Event::NodeScoped {
        kind,
        node_id: obs::NodeId(node_id),
        component: obs::Component::NodeBoot,
    }
}

fn stage_event(run_id: u64, stage_index: u32, kind: obs::EventKind) -> obs::Event {
    obs::Event::StageScoped {
        kind,
        run_id: obs::RunId(run_id),
        stage_index: obs::StageIndex(stage_index),
        reason: None,
        component: obs::Component::StageController,
    }
}

fn object_event(run_id: u64, object_id: u64, sequence: u64, kind: obs::EventKind) -> obs::Event {
    let _ = run_id;
    obs::Event::ObjectScoped {
        kind,
        object_id: obs::ObjectId(object_id),
        sequence: obs::Sequence(sequence),
        component: obs::Component::TokenEndpoint,
    }
}

fn run_supervisor_dashboard_loop() -> Result<(), String> {
    let mut dashboard = MvpDashboard::start_from_env()?;
    eprintln!("mvp-local-e2e-cluster: dashboard {}", dashboard.url());
    eprintln!(
        "mvp-local-e2e-cluster: MVP_DASHBOARD=1, repeating Docker cluster scenario until Ctrl+C"
    );
    let mut run_id = RUN_ID;
    loop {
        match run_supervisor_once(
            run_id,
            Some(&mut dashboard),
            false,
            DEFAULT_PROMPT.to_owned(),
        ) {
            Ok(()) => eprintln!("mvp-local-e2e-cluster: run {run_id} ok"),
            Err(error) => eprintln!("mvp-local-e2e-cluster: run {run_id} failed: {error}"),
        }
        run_id = run_id.saturating_add(1);
        thread::sleep(Duration::from_secs(1));
    }
}

fn run_supervisor_once(
    run_id: u64,
    mut dashboard: Option<&mut MvpDashboard>,
    print_summary: bool,
    prompt_text: String,
) -> Result<(), String> {
    let tokio = tokio::runtime::Runtime::new().map_err(|e| format!("tokio runtime: {e}"))?;
    let mut driver = new_driver(tokio.handle().clone())?;
    let mut driver_runtime = DriverRuntime::new();
    let stack = DistributionRuntimeStack::new_with_codecs(
        driver.node_id(),
        DistributedNodeConfig::default(),
        register_mvp_actor_codecs,
    );
    driver.enable_actor_bridge(
        stack.runtime.clone(),
        stack.codec.clone(),
        stack.actor_bridge_routes(),
        stack.actors.swim,
        stack.relay_mirror.clone(),
        stack.route_view.clone(),
    );

    let prompt_tokens = tokenize_prompt(&prompt_text);
    let orchestrator_report = stack
        .runtime
        .new_inbox::<OrchestratorReport>()
        .map_err(|e| format!("orchestrator report inbox: {e}"))?;
    let orchestrator_addr = stack
        .runtime
        .spawn(OrchestratorActor::new(
            fsm::RunConfig {
                run_id: fsm::RunId(run_id),
                max_tokens: MAX_TOKENS,
                prompt: prompt_tokens.clone(),
            },
            Some(*orchestrator_report.addr()),
        ))
        .map_err(|e| format!("spawn orchestrator actor: {e}"))?;
    stack.register_local_actor(driver.register_actor(orchestrator_addr, 1));
    publish_runtime_snapshot(&mut dashboard, &stack);
    let mut provision_stats = ProvisionStats::default();

    let topology = build_local_engine_topology(run_id)?;
    let run_plan = topology.role_plan.run_plan.clone();
    let stage0 = run_plan
        .stages
        .iter()
        .find(|stage| stage.stage_index == 0)
        .cloned()
        .ok_or_else(|| "engine builder did not assign stage 0".to_owned())?;
    let stage1 = run_plan
        .stages
        .iter()
        .find(|stage| stage.stage_index == 1)
        .cloned()
        .ok_or_else(|| "engine builder did not assign stage 1".to_owned())?;

    let self_endpoint_json = serde_json::to_string(&driver.endpoint_addr())
        .map_err(|e| format!("serialize endpoint addr: {e}"))?;
    let orchestrator_actor_json = serde_json::to_string(&orchestrator_addr)
        .map_err(|e| format!("serialize orchestrator actor: {e}"))?;

    let mut node1 = provision_local_docker_node(
        &mut driver,
        &stack,
        &mut dashboard,
        &mut provision_stats,
        local_docker_spec(
            run_id,
            stage1.node_id.0,
            stage1.stage_index,
            &self_endpoint_json,
            &self_endpoint_json,
            &orchestrator_actor_json,
        ),
    )?;
    let node1_endpoint_json = serde_json::to_string(&node1.endpoint)
        .map_err(|e| format!("serialize node1 endpoint: {e}"))?;
    let mut node0 = match provision_local_docker_node(
        &mut driver,
        &stack,
        &mut dashboard,
        &mut provision_stats,
        local_docker_spec(
            run_id,
            stage0.node_id.0,
            stage0.stage_index,
            &node1_endpoint_json,
            &self_endpoint_json,
            &orchestrator_actor_json,
        ),
    ) {
        Ok(node) => node,
        Err(error) => {
            let _ = stop_provisioned_nodes(&mut [&mut node1], &mut driver, &stack, &mut dashboard);
            return Err(error);
        }
    };

    for stage in [&stage1, &stage0] {
        record_dashboard_event(
            &mut dashboard,
            node_event(stage.node_id.0, obs::EventKind::NodeStarted),
        );
        record_dashboard_event(
            &mut dashboard,
            node_event(stage.node_id.0, obs::EventKind::NodeAvailable),
        );
    }

    let started_at = Instant::now();
    wait_for_routes(
        &mut driver,
        &stack,
        &[node0.node_actor, node1.node_actor],
        Duration::from_secs(20),
    )?;

    let token_in_sender = spawn_send_pump(
        driver.tokio_handle(),
        driver.endpoint().clone(),
        node0.endpoint.clone(),
        stage0.inbound_edge.0,
    )?;
    let mut token_in_object_allocator =
        edge_actor::ObjectIdAllocator::new(edge_actor::EdgeId(stage0.inbound_edge.0));

    stack
        .runtime
        .send_to(
            orchestrator_addr,
            OrchestratorMsg::ObservePoolReady {
                nodes: run_plan
                    .stages
                    .iter()
                    .map(|stage| stage.node_id.0)
                    .collect(),
            },
        )
        .map_err(|e| format!("observe pool ready: {e}"))?;
    record_dashboard_event(&mut dashboard, run_event(run_id, obs::EventKind::PoolReady));
    stack
        .runtime
        .send_to(
            orchestrator_addr,
            OrchestratorMsg::ObservePlanAvailable {
                run_id,
                stages: run_plan
                    .stages
                    .iter()
                    .map(|stage| StageRefWire {
                        stage_index: stage.stage_index,
                        node_id: stage.node_id.0,
                    })
                    .collect(),
            },
        )
        .map_err(|e| format!("observe plan: {e}"))?;
    record_dashboard_event(
        &mut dashboard,
        run_event(run_id, obs::EventKind::RunPlanned),
    );
    stack
        .runtime
        .send_to(
            orchestrator_addr,
            OrchestratorMsg::ObserveTokenInEndpointReady,
        )
        .map_err(|e| format!("observe token-in endpoint: {e}"))?;
    stack
        .runtime
        .send_to(
            orchestrator_addr,
            OrchestratorMsg::ObserveTokenOutEndpointReady,
        )
        .map_err(|e| format!("observe token-out endpoint: {e}"))?;
    record_dashboard_event(
        &mut dashboard,
        run_event(run_id, obs::EventKind::ReadinessBarrierPassed),
    );

    let mut injected = false;
    let mut completed = false;
    let mut torn_down = false;
    let mut stage_ready_count = 0usize;
    let mut token_received = false;
    let mut sent_stop_to_node0 = false;
    let mut sent_stop_to_node1 = false;
    let mut response_tokens = Vec::<u32>::new();
    let mut edge_stream_count = 0usize;
    let mut token_out_streams = HashMap::<u64, Vec<u8>>::new();

    while started_at.elapsed() < Duration::from_secs(30) {
        pump_network(&mut driver, &stack);
        driver_runtime.poll_iroh(&driver);
        while let Some(event) = driver_runtime.try_recv() {
            match event {
                DriverIngressEvent::StreamArrived { edge_id, .. } => {
                    if edge_id == stage1.outbound_edge.0 {
                        edge_stream_count += 1;
                    }
                }
                DriverIngressEvent::BytesRead {
                    edge_id,
                    stream_id,
                    bytes,
                } => {
                    if edge_id != stage1.outbound_edge.0 {
                        continue;
                    }
                    let records = {
                        let buffer = token_out_streams.entry(stream_id).or_default();
                        buffer.extend_from_slice(&bytes);
                        let mut records = Vec::new();
                        while let Some(record) = take_complete_ingress_record(buffer)? {
                            records.push(record);
                        }
                        records
                    };
                    for record in records {
                        let metadata = decode_ingress_record(&record)?;
                        let words = object_record_words(&metadata, &record)?;
                        if let Some(token_id) = words.first().copied() {
                            response_tokens.push(token_id);
                            token_received = true;
                            stack
                                .runtime
                                .send_to(
                                    orchestrator_addr,
                                    OrchestratorMsg::ObserveTokenReceived {
                                        sequence: metadata.sequence,
                                        token_id,
                                        eos: true,
                                    },
                                )
                                .map_err(|e| format!("observe token received: {e}"))?;
                            record_dashboard_event(
                                &mut dashboard,
                                object_event(
                                    run_id,
                                    metadata.object_id.0,
                                    metadata.sequence,
                                    obs::EventKind::TokenReceived,
                                ),
                            );
                        }
                    }
                }
            }
        }
        stage_ready_count += drain_provisioned_node_events(
            &mut [&mut node0, &mut node1],
            run_id,
            &mut dashboard,
            &mut provision_stats,
        )?;
        drain_dashboard(&mut dashboard);
        publish_runtime_snapshot_throttled(&mut dashboard, &stack);

        while let Some(report) = orchestrator_report.try_recv() {
            match report {
                OrchestratorReport::Command(command) => match command {
                    RunCommandWire::ProvisionStage { stage_index, .. } => {
                        let provision = stage_provision_wire(&run_plan, stage_index)?;
                        let target = if stage_index == 0 {
                            node0.node_actor
                        } else {
                            node1.node_actor
                        };
                        stack
                            .runtime
                            .send_to(target, NodeAgentMsg::ProvisionStage(provision))
                            .map_err(|e| format!("send provision to stage {stage_index}: {e}"))?;
                        record_dashboard_event(
                            &mut dashboard,
                            stage_event(run_id, stage_index, obs::EventKind::StageProvisionStarted),
                        );
                    }
                    RunCommandWire::InjectTokenObject {
                        sequence, payload, ..
                    } => {
                        injected = true;
                        let tokens = match payload {
                            mvp_system::actors::orchestrator::TokenObjectPayloadWire::Prompt {
                                tokens,
                            } => tokens,
                            mvp_system::actors::orchestrator::TokenObjectPayloadWire::Decode {
                                token_id,
                                ..
                            } => vec![token_id],
                        };
                        let prompt_object = token_in_object_allocator.alloc();
                        let record = object_record(prompt_object.object_id.0, sequence, &tokens);
                        token_in_sender.send(record)?;
                        record_dashboard_event(
                            &mut dashboard,
                            object_event(
                                run_id,
                                prompt_object.object_id.0,
                                sequence,
                                obs::EventKind::PromptInjected,
                            ),
                        );
                    }
                    RunCommandWire::StopRun {
                        stage_index,
                        run_id,
                    } => {
                        let target = if stage_index == 0 {
                            sent_stop_to_node0 = true;
                            node0.node_actor
                        } else {
                            sent_stop_to_node1 = true;
                            node1.node_actor
                        };
                        stack
                            .runtime
                            .send_to(target, NodeAgentMsg::StopRun { run_id })
                            .map_err(|e| format!("send stop to stage {stage_index}: {e}"))?;
                        record_dashboard_event(
                            &mut dashboard,
                            stage_event(run_id, stage_index, obs::EventKind::StopRunSent),
                        );
                    }
                    RunCommandWire::TearDownTokenEndpoints { .. } => {
                        stack
                            .runtime
                            .send_to(
                                orchestrator_addr,
                                OrchestratorMsg::ObserveTokenEndpointsStopped,
                            )
                            .map_err(|e| format!("observe token endpoints stopped: {e}"))?;
                    }
                    RunCommandWire::CreateTokenInEndpoint { .. }
                    | RunCommandWire::CreateTokenOutEndpoint { .. } => {}
                },
                OrchestratorReport::Lifecycle(event) => match event {
                    LifecycleEventWire::RunCompleted { run_id } => {
                        record_dashboard_event(
                            &mut dashboard,
                            run_event(run_id, obs::EventKind::RunCompleted),
                        );
                        completed = true;
                    }
                    LifecycleEventWire::RunTornDown { run_id } => {
                        record_dashboard_event(
                            &mut dashboard,
                            run_event(run_id, obs::EventKind::RunTornDown),
                        );
                        torn_down = true;
                    }
                    LifecycleEventWire::RunRejected { run_id }
                    | LifecycleEventWire::RunFaulted { run_id }
                    | LifecycleEventWire::RunOperatorStopped { run_id } => {
                        record_dashboard_event(&mut dashboard, run_fault_event(run_id));
                        return Err(format!("run failed: {event:?}"));
                    }
                },
                OrchestratorReport::Snapshot { .. } => {}
            }
        }

        if injected
            && token_received
            && completed
            && torn_down
            && sent_stop_to_node0
            && sent_stop_to_node1
        {
            stop_provisioned_nodes(
                &mut [&mut node0, &mut node1],
                &mut driver,
                &stack,
                &mut dashboard,
            )?;
            let builder_stage_assignments = topology
                .events
                .iter()
                .filter(|event| {
                    matches!(
                        event,
                        engine::EngineEvent::RoleAssigned {
                            role: engine::RoleKind::StageWorker { .. },
                            ..
                        }
                    )
                })
                .count();
            let response_text = detokenize_response(&response_tokens);
            let summary = json!({
                "ok": true,
                "actor_plane": "iroh-swactor",
                "data_plane": "iroh-quic-persistent-edge-streams",
                "edge_protocol": "edge-id-preamble-mo01-object-records",
                "node_local_data_plane": "arena-backed-rings-json-metadata-only",
                "processes": {
                    "orchestrator": std::process::id(),
                    "node0": node0.provider_process_id,
                    "node1": node1.provider_process_id,
                },
                "node0_endpoint": node0.endpoint,
                "node1_endpoint": node1.endpoint,
                "node0_logical_id": node0.node_id,
                "node1_logical_id": node1.node_id,
                "node0_stage_index": node0.stage_index,
                "node1_stage_index": node1.stage_index,
                "worker_processes": "docker-tinygrad-cpu-worker-per-node",
                "docker_image": docker_image(),
                "prompt_text": prompt_text,
                "prompt_tokens": prompt_tokens,
                "response_tokens": response_tokens,
                "response_text": response_text,
                "tinygrad_device": "CPU",
                "injected_prompt_observed": injected,
                "token_received_observed": token_received,
                "run_completed_observed": completed,
                "run_torn_down_observed": torn_down,
                "stop_sent_to_all_nodes": sent_stop_to_node0 && sent_stop_to_node1,
                "engine_builder_pattern": "host-coordinator-static-topology-docker-workers",
                "engine_builder_event_count": topology.events.len(),
                "engine_builder_node_count": topology.node_summaries.len(),
                "engine_builder_stage_assignments": builder_stage_assignments,
                "node_route_count": stack.route_view.read().map(|view| view.len()).unwrap_or_default(),
                "stage_ready_stdout_count": stage_ready_count,
                "provisioned_node_count": 2,
                "provision_node_live_count": provision_stats.node_live_count,
                "provision_stdout_line_count": provision_stats.stdout_line_count,
                "provision_stderr_line_count": provision_stats.stderr_line_count,
                "provision_nodes_stopped": true,
                "edge_stream_object_count": edge_stream_count,
            });
            if print_summary {
                println!("{summary}");
            } else {
                eprintln!("mvp-local-e2e-cluster: run {run_id} summary {summary}");
            }
            return Ok(());
        }

        thread::sleep(Duration::from_millis(10));
    }

    record_dashboard_event(&mut dashboard, run_fault_event(run_id));
    let _ = stop_provisioned_nodes(
        &mut [&mut node0, &mut node1],
        &mut driver,
        &stack,
        &mut dashboard,
    );
    Err(format!(
        "timed out: injected={injected} token_received={token_received} completed={completed} torn_down={torn_down} stop0={sent_stop_to_node0} stop1={sent_stop_to_node1}"
    ))
}

fn run_node_role(args: &[String]) -> Result<(), String> {
    let logical_node_id = parse_arg(args, "--logical-node-id")?
        .parse::<u64>()
        .map_err(|e| format!("logical node id: {e}"))?;
    let stage_index = parse_arg(args, "--stage-index")?
        .parse::<u32>()
        .map_err(|e| format!("stage index: {e}"))?;
    let coordinator: EndpointAddr =
        serde_json::from_str(parse_arg(args, "--coordinator-endpoint")?)
            .map_err(|e| format!("coordinator endpoint json: {e}"))?;
    let downstream_endpoint: EndpointAddr =
        serde_json::from_str(parse_arg(args, "--outbound-endpoint")?)
            .map_err(|e| format!("outbound endpoint json: {e}"))?;
    let orchestrator_addr: ActorAddress =
        serde_json::from_str(parse_arg(args, "--orchestrator-actor")?)
            .map_err(|e| format!("orchestrator actor json: {e}"))?;

    let tokio = tokio::runtime::Runtime::new().map_err(|e| format!("node tokio runtime: {e}"))?;
    let mut driver = new_driver(tokio.handle().clone())?;
    driver.join(&[coordinator]);
    let mut driver_runtime = DriverRuntime::new();
    let stack = DistributionRuntimeStack::new_with_codecs(
        driver.node_id(),
        DistributedNodeConfig::default(),
        register_mvp_actor_codecs,
    );
    driver.enable_actor_bridge(
        stack.runtime.clone(),
        stack.codec.clone(),
        stack.actor_bridge_routes(),
        stack.actors.swim,
        stack.relay_mirror.clone(),
        stack.route_view.clone(),
    );

    let node_report = stack
        .runtime
        .new_inbox::<NodeAgentReport>()
        .map_err(|e| format!("node report inbox: {e}"))?;
    let node_actor = stack
        .runtime
        .spawn(NodeAgentActor::new(
            stage::NodeId(logical_node_id),
            orchestrator_addr,
            Some(*node_report.addr()),
        ))
        .map_err(|e| format!("spawn node agent: {e}"))?;
    stack.register_local_actor(driver.register_actor(node_actor, 1));

    let mut arena_manager = arena::ArenaManager::boot(arena::ArenaConfig {
        node_id: arena::NodeId(logical_node_id),
        reservation_ceiling: ARENA_BYTES as u64,
        base_alignment: 64,
    })
    .map_err(|e| format!("boot ArenaManager: {e:?}"))?;
    let mut worker = GpuWorkerRuntime::spawn(stage_index, arena_manager.arena_fd())?;
    let mut edge_establisher = edge::EdgeEstablisher::new(edge::NodeId(logical_node_id));
    let mut tx_edge_actor: Option<edge_actor::TxEdgeActor> = None;
    let mut rx_edge_actor: Option<edge_actor::RxEdgeActor> = None;
    let mut driver_fsm = driver_model::Driver::new(driver_model::DriverConfig {
        local_node_id: driver_model::NodeId(logical_node_id),
        alpn: driver_model::Alpn(String::from_utf8_lossy(EDGE_ALPN).into_owned()),
    });
    let mut edge_command_cursor = 0usize;
    let mut edge_event_cursor = 0usize;
    let mut driver_event_cursor = 0usize;
    eprintln!(
        "{}",
        json!({
            "type": "boot_stream",
            "stream": "stderr",
            "logical_node_id": logical_node_id,
            "stage_index": stage_index,
        })
    );
    println!(
        "{}",
        json!({
            "type": "ready",
            "role": "node",
            "endpoint": driver.endpoint_addr(),
            "node_actor": node_actor,
            "logical_node_id": logical_node_id,
            "stage_index": stage_index,
        })
    );
    std::io::stdout()
        .flush()
        .map_err(|e| format!("flush node ready: {e}"))?;

    let mut pending_commands = VecDeque::new();
    let mut object_handles = HashMap::<edge_actor::ObjectKey, LoadedObject>::new();
    let mut ingress_streams = HashMap::<u64, Vec<u8>>::new();
    let mut outbound_sender: Option<SendPumpHandle> = None;
    let mut outbound_object_allocator: Option<edge_actor::ObjectIdAllocator> = None;
    let mut inbound_edge_id = None;
    let mut outbound_edge_id = None;
    let started_at = Instant::now();
    let shutdown_rx = spawn_shutdown_listener();
    let mut inbound_ring_id = None;
    let mut outbound_ring_id = None;

    loop {
        pump_network(&mut driver, &stack);
        driver_runtime.poll_iroh(&driver);
        while let Some(event) = driver_runtime.try_recv() {
            let (edge_id, stream_id, bytes) = match event {
                DriverIngressEvent::StreamArrived { edge_id, stream_id } => {
                    driver_fsm.observe(driver_model::DriverEvent::IncomingUniStream {
                        edge_id: driver_model::EdgeId(edge_id),
                        stream_id: driver_model::StreamId(stream_id),
                    });
                    drive_edge_workflow(
                        &mut edge_establisher,
                        &mut edge_command_cursor,
                        &mut edge_event_cursor,
                        &mut driver_event_cursor,
                        &mut arena_manager,
                        &mut worker,
                        &mut driver_fsm,
                        &mut tx_edge_actor,
                        &mut rx_edge_actor,
                        &mut outbound_sender,
                        &mut inbound_edge_id,
                        &mut outbound_edge_id,
                        &mut inbound_ring_id,
                        &mut outbound_ring_id,
                        &stack.runtime,
                        node_actor,
                        driver.tokio_handle(),
                        driver.endpoint().clone(),
                        downstream_endpoint.clone(),
                    )?;
                    continue;
                }
                DriverIngressEvent::BytesRead {
                    edge_id,
                    stream_id,
                    bytes,
                } => (edge_id, stream_id, bytes),
            };
            if Some(edge_id) != inbound_edge_id {
                continue;
            }
            let records = {
                let buffer = ingress_streams.entry(stream_id).or_default();
                buffer.extend_from_slice(&bytes);
                let mut records = Vec::new();
                while let Some(record) = take_complete_ingress_record(buffer)? {
                    records.push(record);
                }
                records
            };
            for record in records {
                let ring_id = inbound_ring_id.ok_or_else(|| "inbound ring missing".to_owned())?;
                let lease = arena_manager
                    .lookup_lease(arena::RingId(ring_id))
                    .ok_or_else(|| format!("inbound ring {ring_id} lease missing"))?;
                arena_manager
                    .write_arena(lease.layout.data_offset, &record)
                    .map_err(|e| format!("write ingress ring: {e}"))?;
                let loaded = worker.ring_readable(ring_id, edge_id)?;
                let object_key = edge_actor::ObjectKey::new(
                    edge_actor::EdgeId(edge_id),
                    edge_actor::ObjectId(loaded.object_id),
                );
                object_handles.insert(object_key, loaded.clone());
                stack
                    .runtime
                    .send_to(
                        node_actor,
                        NodeAgentMsg::ObjectLoaded {
                            edge_id,
                            object_id: loaded.object_id,
                            sequence: loaded.sequence,
                            handle_generation: loaded.handle_generation,
                            handle_id: loaded.handle_id,
                        },
                    )
                    .map_err(|e| format!("report object loaded: {e}"))?;
                println!(
                    "{}",
                    json!({
                        "type":"node_lifecycle",
                        "stage_index":stage_index,
                        "event":"ObjectLoaded",
                        "object_id":loaded.object_id,
                        "sequence":loaded.sequence,
                    })
                );
                std::io::stdout()
                    .flush()
                    .map_err(|e| format!("flush object loaded stdout: {e}"))?;
            }
        }

        while let Some(report) = node_report.try_recv() {
            match report {
                NodeAgentReport::Command(command) => pending_commands.push_back(command),
                NodeAgentReport::Lifecycle(event) => {
                    println!(
                        "{}",
                        json!({
                            "type":"node_lifecycle",
                            "stage_index":stage_index,
                            "event":format!("{event:?}"),
                        })
                    );
                    std::io::stdout()
                        .flush()
                        .map_err(|e| format!("flush lifecycle stdout: {e}"))?;
                }
                NodeAgentReport::Snapshot { .. } => {}
            }
        }

        if let Some(index) = pending_commands.iter().position(readiness_command) {
            let command = pending_commands.remove(index).unwrap();
            handle_node_command(
                command,
                &stack.runtime,
                node_actor,
                &mut worker,
                &mut outbound_sender,
                &mut outbound_object_allocator,
                &mut inbound_edge_id,
                &mut outbound_edge_id,
                &mut object_handles,
                &mut inbound_ring_id,
                &mut outbound_ring_id,
                &mut edge_establisher,
                &mut edge_command_cursor,
                &mut edge_event_cursor,
                &mut driver_event_cursor,
                &mut arena_manager,
                &mut driver_fsm,
                &mut tx_edge_actor,
                &mut rx_edge_actor,
                stage_index,
                driver.tokio_handle(),
                driver.endpoint().clone(),
                downstream_endpoint.clone(),
            )?;
        } else if let Some(command) = pending_commands.pop_front() {
            handle_node_command(
                command,
                &stack.runtime,
                node_actor,
                &mut worker,
                &mut outbound_sender,
                &mut outbound_object_allocator,
                &mut inbound_edge_id,
                &mut outbound_edge_id,
                &mut object_handles,
                &mut inbound_ring_id,
                &mut outbound_ring_id,
                &mut edge_establisher,
                &mut edge_command_cursor,
                &mut edge_event_cursor,
                &mut driver_event_cursor,
                &mut arena_manager,
                &mut driver_fsm,
                &mut tx_edge_actor,
                &mut rx_edge_actor,
                stage_index,
                driver.tokio_handle(),
                driver.endpoint().clone(),
                downstream_endpoint.clone(),
            )?;
        }

        if shutdown_rx.try_recv().is_ok() {
            worker.shutdown().ok();
            return Ok(());
        }

        if started_at.elapsed() > Duration::from_secs(60) {
            return Err("node role timed out".to_owned());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn new_driver(handle: tokio::runtime::Handle) -> Result<IrohDriver, String> {
    IrohDriver::with_handle(
        handle,
        IrohDriverConfig {
            secret_key: None,
            relay_mode: iroh::RelayMode::Disabled,
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![EDGE_ALPN.to_vec()],
        },
    )
    .map_err(|e| format!("create iroh driver: {e}"))
}

fn pump_network(driver: &mut IrohDriver, stack: &DistributionRuntimeStack) {
    stack.tick_protocol_actors(Instant::now());
    driver.pump_inbound_to_actors();
    stack.pump_runtime_once();
    driver.drain_outbox(&stack.outbox);
}

fn wait_for_routes(
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    actors: &[ActorAddress],
    timeout: Duration,
) -> Result<(), String> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        pump_network(driver, stack);
        let ready = stack
            .route_view
            .read()
            .map(|view| actors.iter().all(|actor| view.contains_key(actor)))
            .unwrap_or(false);
        if ready {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err("directory routes for node actors did not converge".to_owned())
}

struct LocalEngineTopology {
    role_plan: engine::RoleAssignmentPlan,
    events: Vec<engine::EngineEvent>,
    node_summaries: Vec<engine::NodeSummary>,
}

fn build_local_engine_topology(run_id: u64) -> Result<LocalEngineTopology, String> {
    let cluster = engine::ClusterBuilder::new(
        "local-e2e-cluster",
        engine::ModelSpec::pipelined_causal_llm(
            "local-e2e-cluster-tinygrad-cpu-fixture",
            engine::ModelArtifact::TestTinyLlm {
                path: "docker-cpu://local-e2e-cluster-tinygrad-cpu-fixture".to_owned(),
            },
            4,
            8,
            engine::DTypeFamily::BFloat,
            2,
            8,
            99,
            plan::TokenizerSource::EmbeddedGguf,
        ),
    )
    .run_id(run_id)
    .image(engine::NodeImageSpec::new(docker_image()).worker_runtime(
        engine::WorkerRuntimeSpec::External {
            name: "tinygrad-cpu".to_owned(),
        },
    ))
    .pool_provider(engine::StaticPoolProvider::new(vec![
        engine::NodeLease::new(
            "orchestrator",
            engine::NodeId(ORCHESTRATOR_LOGICAL_NODE_ID),
            [engine::NodeCapability::Coordinator],
        )
        .resources(engine::ResourceFacts::cpu_only(2, 2 << 30)),
        engine::NodeLease::new(
            "node0",
            engine::NodeId(NODE0_LOGICAL_ID),
            [engine::NodeCapability::Worker],
        )
        .resources(engine::ResourceFacts::cpu_only(2, 2 << 30)),
        engine::NodeLease::new(
            "node1",
            engine::NodeId(NODE1_LOGICAL_ID),
            [engine::NodeCapability::Worker],
        )
        .resources(engine::ResourceFacts::cpu_only(2, 2 << 30)),
    ]))
    .launcher(engine::StaticNodeLauncher)
    .planner(
        engine::FixedLinearPipelinePlanner::new(2).runtime(plan::RuntimeConfig {
            max_tokens: MAX_TOKENS as u32,
            prompt: plan::PromptSource::Inline(DEFAULT_PROMPT.to_owned()),
            sampling: plan::SamplingPolicy {
                temperature_millis: 0,
                top_k: 1,
            },
            token_output_policy: plan::TokenOutputPolicy::EmitAll,
        }),
    )
    .launch()
    .map_err(|e| format!("local engine builder launch: {e}"))?;
    let role_plan = cluster.role_plan().clone();
    let events = cluster.events().to_vec();
    let node_summaries = cluster.node_summaries();
    cluster
        .shutdown()
        .map_err(|e| format!("local engine builder shutdown: {e}"))?;
    Ok(LocalEngineTopology {
        role_plan,
        events,
        node_summaries,
    })
}

fn stage_provision_wire(
    plan: &plan::RunPlan,
    stage_index: u32,
) -> Result<StageProvisionWire, String> {
    let provision = plan::derive_stage_provision(plan, stage_index)
        .map_err(|e| format!("derive stage provision {stage_index}: {e:?}"))?;
    Ok(StageProvisionWire {
        run_id: provision.run_id.0,
        authorized_orchestrator: ORCHESTRATOR_LOGICAL_NODE_ID,
        node_id: provision.node_id.0,
        stage_index: provision.stage_index,
        stage_count: provision.stage_count,
        layer_start: provision.layer_start,
        layer_end_exclusive: provision.layer_end_exclusive,
        inbound_edge_id: provision.inbound.edge_id.0,
        outbound_edge_id: provision.outbound.edge_id.0,
        weight_artifact: "local-e2e-cluster-tinygrad-cpu-fixture".to_owned(),
    })
}

fn handle_node_command(
    command: StageCommandWire,
    runtime: &swactor::runtime::Runtime,
    node_actor: ActorAddress,
    worker: &mut GpuWorkerRuntime,
    outbound_sender: &mut Option<SendPumpHandle>,
    outbound_object_allocator: &mut Option<edge_actor::ObjectIdAllocator>,
    inbound_edge_id: &mut Option<u64>,
    outbound_edge_id: &mut Option<u64>,
    object_handles: &mut HashMap<edge_actor::ObjectKey, LoadedObject>,
    inbound_ring_id: &mut Option<u64>,
    outbound_ring_id: &mut Option<u64>,
    edge_establisher: &mut edge::EdgeEstablisher,
    edge_command_cursor: &mut usize,
    edge_event_cursor: &mut usize,
    driver_event_cursor: &mut usize,
    arena_manager: &mut arena::ArenaManager,
    driver_fsm: &mut driver_model::Driver,
    tx_edge_actor: &mut Option<edge_actor::TxEdgeActor>,
    rx_edge_actor: &mut Option<edge_actor::RxEdgeActor>,
    local_stage_index: u32,
    handle: tokio::runtime::Handle,
    endpoint: iroh::Endpoint,
    downstream_endpoint: EndpointAddr,
) -> Result<(), String> {
    match command {
        StageCommandWire::EstablishInboundEdge { edge_id } => {
            *inbound_edge_id = Some(edge_id);
            *rx_edge_actor = Some(edge_actor::RxEdgeActor::new(edge_actor::RxConfig {
                edge_id: edge_actor::EdgeId(edge_id),
                role_port: edge_actor::PortId("input".to_owned()),
            }));
            edge_establisher.observe(edge::EdgeEvent::ProvisionRx(edge::ProvisionRx {
                run_id: edge::RunId(RUN_ID),
                edge_id: edge::EdgeId(edge_id),
                local_node_id: edge::NodeId(u64::from(local_stage_index) + 11),
                object_spec: edge::ObjectSpec::test_activation(),
                ring_spec: edge_ring_spec(),
            }));
            drive_edge_workflow(
                edge_establisher,
                edge_command_cursor,
                edge_event_cursor,
                driver_event_cursor,
                arena_manager,
                worker,
                driver_fsm,
                tx_edge_actor,
                rx_edge_actor,
                outbound_sender,
                inbound_edge_id,
                outbound_edge_id,
                inbound_ring_id,
                outbound_ring_id,
                runtime,
                node_actor,
                handle,
                endpoint,
                downstream_endpoint,
            )
        }
        StageCommandWire::EstablishOutboundEdge { edge_id } => {
            *outbound_edge_id = Some(edge_id);
            *outbound_object_allocator = Some(edge_actor::ObjectIdAllocator::new(
                edge_actor::EdgeId(edge_id),
            ));
            *tx_edge_actor = Some(edge_actor::TxEdgeActor::new(edge_actor::TxConfig {
                edge_id: edge_actor::EdgeId(edge_id),
                role_port: edge_actor::PortId("output".to_owned()),
            }));
            edge_establisher.observe(edge::EdgeEvent::ProvisionTx(edge::ProvisionTx {
                run_id: edge::RunId(RUN_ID),
                edge_id: edge::EdgeId(edge_id),
                local_node_id: edge::NodeId(u64::from(local_stage_index) + 11),
                consumer_node_id: edge::NodeId(if local_stage_index == 0 {
                    NODE1_LOGICAL_ID
                } else {
                    ORCHESTRATOR_LOGICAL_NODE_ID
                }),
                object_spec: edge::ObjectSpec::test_activation(),
                ring_spec: edge_ring_spec(),
            }));
            drive_edge_workflow(
                edge_establisher,
                edge_command_cursor,
                edge_event_cursor,
                driver_event_cursor,
                arena_manager,
                worker,
                driver_fsm,
                tx_edge_actor,
                rx_edge_actor,
                outbound_sender,
                inbound_edge_id,
                outbound_edge_id,
                inbound_ring_id,
                outbound_ring_id,
                runtime,
                node_actor,
                handle,
                endpoint,
                downstream_endpoint,
            )
        }
        StageCommandWire::ConfigureWorkerRole {
            run_id,
            stage_index,
            layer_start,
            layer_end_exclusive,
        } => {
            worker.configure_role(run_id, stage_index, layer_start, layer_end_exclusive)?;
            runtime
                .send_to(node_actor, NodeAgentMsg::MarkWorkerReady)
                .map_err(|e| format!("mark worker ready: {e}"))
        }
        StageCommandWire::LoadWeights { .. } => runtime
            .send_to(node_actor, NodeAgentMsg::MarkWeightsReady)
            .map_err(|e| format!("mark weights ready: {e}")),
        StageCommandWire::ExecuteStep {
            step_id,
            input_edge_id,
            object_id,
            sequence,
            ..
        } => {
            let input_key = edge_actor::ObjectKey::new(
                edge_actor::EdgeId(input_edge_id),
                edge_actor::ObjectId(object_id),
            );
            let loaded = object_handles
                .get(&input_key)
                .cloned()
                .ok_or_else(|| format!("object {input_key:?} has no loaded device handle"))?;
            if loaded.sequence != sequence {
                return Err(format!(
                    "object {input_key:?} sequence {} does not match command sequence {sequence}",
                    loaded.sequence
                ));
            }
            if loaded.edge_id != input_edge_id {
                return Err(format!(
                    "object {input_key:?} was loaded from edge {}, not command edge {input_edge_id}",
                    loaded.edge_id
                ));
            }
            let output_ring = outbound_ring_id.ok_or_else(|| "outbound ring missing".to_owned())?;
            let output_key = outbound_object_allocator
                .as_mut()
                .ok_or_else(|| "outbound object allocator missing".to_owned())?
                .alloc();
            let output_object_id = output_key.object_id.0;
            let committed_bytes = worker.execute_step(
                u64::from(local_stage_index) + 1,
                step_id,
                object_id,
                loaded.sequence,
                loaded.handle_id,
                output_ring,
                output_object_id,
                loaded.sequence,
                local_stage_index == 1,
            )?;
            let lease = arena_manager
                .lookup_lease(arena::RingId(output_ring))
                .ok_or_else(|| format!("outbound ring {output_ring} lease missing"))?;
            let record = arena_manager
                .read_arena(lease.layout.data_offset, committed_bytes)
                .map_err(|e| format!("read egress ring: {e}"))?;
            driver_fsm.observe(driver_model::DriverEvent::EgressBytesCommitted {
                edge_id: driver_model::EdgeId(
                    outbound_edge_id.ok_or_else(|| "outbound edge missing".to_owned())?,
                ),
                bytes: record.clone(),
            });
            driver_fsm.observe(driver_model::DriverEvent::RingReadable {
                ring_id: driver_model::RingId(output_ring),
            });
            if let Some(tx_actor) = tx_edge_actor.as_mut() {
                tx_actor.observe(edge_actor::TxEvent::ObjectProduced {
                    edge_id: edge_actor::EdgeId(
                        outbound_edge_id.ok_or_else(|| "outbound edge missing".to_owned())?,
                    ),
                    object_id: output_key.object_id,
                    sequence: loaded.sequence,
                });
            }
            let sender = outbound_sender
                .as_ref()
                .ok_or_else(|| "outbound edge sender missing".to_owned())?;
            sender.send(record)?;
            runtime
                .send_to(node_actor, NodeAgentMsg::StepCompleted { step_id })
                .map_err(|e| format!("mark step completed: {e}"))
        }
        StageCommandWire::ReleaseInputHandle { handle_id, .. } => {
            worker.release_device_object(handle_id)
        }
        StageCommandWire::StopLocalEdges { run_id } => {
            runtime
                .send_to(node_actor, NodeAgentMsg::LocalEdgesStopped { run_id })
                .map_err(|e| format!("mark local edges stopped: {e}"))?;
            runtime
                .send_to(node_actor, NodeAgentMsg::WorkerRingsQuiesced { run_id })
                .map_err(|e| format!("mark worker rings quiesced: {e}"))
        }
        StageCommandWire::ReleaseRunDeviceObjects { run_id } => {
            runtime
                .send_to(node_actor, NodeAgentMsg::DeviceObjectsReleased { run_id })
                .map_err(|e| format!("mark device objects released: {e}"))?;
            runtime
                .send_to(node_actor, NodeAgentMsg::WorkerRoleReset { run_id })
                .map_err(|e| format!("mark worker role reset: {e}"))
        }
        StageCommandWire::RewireEdge { .. } => Ok(()),
    }
}

fn readiness_command(command: &StageCommandWire) -> bool {
    matches!(
        command,
        StageCommandWire::EstablishInboundEdge { .. }
            | StageCommandWire::EstablishOutboundEdge { .. }
            | StageCommandWire::ConfigureWorkerRole { .. }
            | StageCommandWire::LoadWeights { .. }
    )
}

#[derive(Clone, Debug)]
struct LoadedObject {
    object_id: u64,
    edge_id: u64,
    sequence: u64,
    handle_generation: u64,
    handle_id: u64,
}
fn edge_ring_spec() -> edge::RingSpec {
    edge::RingSpec {
        header_bytes: 128,
        data_bytes: RING_BYTES as u64,
        alignment: 64,
    }
}

fn drive_edge_workflow(
    edge_establisher: &mut edge::EdgeEstablisher,
    edge_command_cursor: &mut usize,
    edge_event_cursor: &mut usize,
    driver_event_cursor: &mut usize,
    arena_manager: &mut arena::ArenaManager,
    worker: &mut GpuWorkerRuntime,
    driver_fsm: &mut driver_model::Driver,
    tx_edge_actor: &mut Option<edge_actor::TxEdgeActor>,
    rx_edge_actor: &mut Option<edge_actor::RxEdgeActor>,
    outbound_sender: &mut Option<SendPumpHandle>,
    inbound_edge_id: &mut Option<u64>,
    outbound_edge_id: &mut Option<u64>,
    inbound_ring_id: &mut Option<u64>,
    outbound_ring_id: &mut Option<u64>,
    runtime: &swactor::runtime::Runtime,
    node_actor: ActorAddress,
    handle: tokio::runtime::Handle,
    endpoint: iroh::Endpoint,
    downstream_endpoint: EndpointAddr,
) -> Result<(), String> {
    loop {
        let mut progressed = false;

        while *edge_command_cursor < edge_establisher.commands().len() {
            let command = edge_establisher.commands()[*edge_command_cursor].clone();
            *edge_command_cursor += 1;
            progressed = true;
            match command {
                edge::EdgeCommand::LeaseRing {
                    request_id,
                    direction,
                    ring_spec,
                    ..
                } => {
                    let events =
                        arena_manager.request(arena::ArenaRequest::LeaseRing(arena::LeaseRing {
                            request_id: arena::LeaseRequestId(request_id.0),
                            ring_spec: arena::RingSpec {
                                header_bytes: ring_spec.header_bytes,
                                data_bytes: ring_spec.data_bytes,
                                alignment: ring_spec.alignment,
                            },
                        }));
                    for event in events {
                        match event {
                            arena::ArenaEvent::RingLeased { lease } => {
                                let edge_layout = edge::RingLayout {
                                    start_offset: lease.layout.start_offset,
                                    header_offset: lease.layout.header_offset,
                                    data_offset: lease.layout.data_offset,
                                    end_offset: lease.layout.end_offset,
                                    data_bytes: lease.layout.data_bytes,
                                    alignment: lease.layout.alignment,
                                };
                                edge_establisher.observe(edge::EdgeEvent::RingLeased {
                                    request_id: edge::LeaseRequestId(lease.request_id.0),
                                    ring_id: edge::RingId(lease.ring_id.0),
                                    layout: edge_layout,
                                });
                            }
                            arena::ArenaEvent::RingLeaseRejected { request_id, reason } => {
                                let reason = match reason {
                                    arena::RingLeaseRejection::CannotFitWithinCeiling => {
                                        edge::RingLeaseRejection::CannotFit
                                    }
                                    arena::RingLeaseRejection::ArenaShuttingDown => {
                                        edge::RingLeaseRejection::ArenaShuttingDown
                                    }
                                };
                                edge_establisher.observe(edge::EdgeEvent::RingLeaseRejected {
                                    request_id: edge::LeaseRequestId(request_id.0),
                                    reason,
                                });
                            }
                            arena::ArenaEvent::RingLeaseQueued { .. }
                            | arena::ArenaEvent::RingReleased { .. }
                            | arena::ArenaEvent::RingReleaseRejected { .. }
                            | arena::ArenaEvent::CancelledFreshLeaseReleased { .. } => {}
                        }
                    }
                    if matches!(direction, edge::RingDirection::Ingress) {
                        // The eventual ring id is learned from the install command.
                    }
                }
                edge::EdgeCommand::InstallWorkerRing {
                    edge_id,
                    ring_id,
                    direction,
                    ..
                } => {
                    let lease = arena_manager
                        .lookup_lease(arena::RingId(ring_id.0))
                        .ok_or_else(|| format!("ring {} lease missing", ring_id.0))?
                        .clone();
                    let (port, direction_name) = match direction {
                        edge::RingDirection::Ingress => {
                            *inbound_ring_id = Some(ring_id.0);
                            ("input", "ingress")
                        }
                        edge::RingDirection::Egress => {
                            *outbound_ring_id = Some(ring_id.0);
                            ("output", "egress")
                        }
                    };
                    worker.install_ring(
                        ring_id.0,
                        edge_id.0,
                        port,
                        direction_name,
                        lease.layout,
                    )?;
                    edge_establisher.observe(edge::EdgeEvent::RingInstalled { edge_id, ring_id });
                }
                edge::EdgeCommand::EstablishSend {
                    edge_id,
                    consumer_node_id,
                    ..
                } => {
                    let record = edge_establisher
                        .local_record(edge_id)
                        .ok_or_else(|| format!("edge {} record missing", edge_id.0))?;
                    let ring_id = record
                        .ring_id
                        .ok_or_else(|| format!("edge {} ring missing", edge_id.0))?;
                    driver_fsm.observe(driver_model::DriverEvent::EstablishSend(
                        driver_model::EstablishSend {
                            edge_id: driver_model::EdgeId(edge_id.0),
                            peer_node_id: driver_model::NodeId(consumer_node_id.0),
                            layout: driver_model::RingLayout {
                                ring_id: driver_model::RingId(ring_id.0),
                                byte_capacity: RING_BYTES,
                                direction: driver_model::RingDirection::Egress,
                            },
                        },
                    ));
                    *outbound_sender = Some(spawn_send_pump(
                        handle.clone(),
                        endpoint.clone(),
                        downstream_endpoint.clone(),
                        edge_id.0,
                    )?);
                }
                edge::EdgeCommand::EstablishRecv { edge_id, .. } => {
                    let record = edge_establisher
                        .local_record(edge_id)
                        .ok_or_else(|| format!("edge {} record missing", edge_id.0))?;
                    let ring_id = record
                        .ring_id
                        .ok_or_else(|| format!("edge {} ring missing", edge_id.0))?;
                    driver_fsm.observe(driver_model::DriverEvent::EstablishRecv(
                        driver_model::EstablishRecv {
                            edge_id: driver_model::EdgeId(edge_id.0),
                            layout: driver_model::RingLayout {
                                ring_id: driver_model::RingId(ring_id.0),
                                byte_capacity: RING_BYTES,
                                direction: driver_model::RingDirection::Ingress,
                            },
                        },
                    ));
                }
                edge::EdgeCommand::CancelQueuedLease { request_id, .. } => {
                    let _ = arena_manager.request(arena::ArenaRequest::CancelLease {
                        request_id: arena::LeaseRequestId(request_id.0),
                    });
                }
                edge::EdgeCommand::StopPump { edge_id, .. } => {
                    driver_fsm.observe(driver_model::DriverEvent::StopEdge {
                        edge_id: driver_model::EdgeId(edge_id.0),
                    });
                }
                edge::EdgeCommand::UninstallWorkerRing { ring_id, .. } => {
                    edge_establisher.observe(edge::EdgeEvent::RingQuiesced { ring_id });
                }
                edge::EdgeCommand::ReleaseArenaLease { ring_id, proof } => {
                    let proof = if proof == edge::QuiescenceProof::verified() {
                        arena::QuiescenceProof::verified()
                    } else {
                        arena::QuiescenceProof::missing()
                    };
                    for event in arena_manager.request(arena::ArenaRequest::ReleaseRing {
                        ring_id: arena::RingId(ring_id.0),
                        proof,
                    }) {
                        if let arena::ArenaEvent::RingReleased { .. } = event {
                            edge_establisher.observe(edge::EdgeEvent::Stopped {
                                edge_id: edge::EdgeId(0),
                            });
                        }
                    }
                }
            }
        }

        while *driver_event_cursor < driver_fsm.events().len() {
            let event = driver_fsm.events()[*driver_event_cursor].clone();
            *driver_event_cursor += 1;
            progressed = true;
            match event {
                driver_model::DriverEventOut::DriverEdgeReady { edge_id } => {
                    edge_establisher.observe(edge::EdgeEvent::DriverEdgeReady {
                        edge_id: edge::EdgeId(edge_id.0),
                    });
                }
                driver_model::DriverEventOut::StreamFault { edge_id, reason } => {
                    let reason = match reason {
                        driver_model::StreamFaultReason::ReadError => {
                            edge::StreamFaultReason::ReadError
                        }
                        driver_model::StreamFaultReason::WriteError => {
                            edge::StreamFaultReason::WriteError
                        }
                        driver_model::StreamFaultReason::ProtocolError => {
                            edge::StreamFaultReason::ProtocolError
                        }
                    };
                    edge_establisher.observe(edge::EdgeEvent::StreamFault {
                        edge_id: edge::EdgeId(edge_id.0),
                        reason,
                    });
                }
                driver_model::DriverEventOut::PumpStopped { edge_id, ring_id } => {
                    edge_establisher.observe(edge::EdgeEvent::PumpStopped {
                        edge_id: edge::EdgeId(edge_id.0),
                        ring_id: edge::RingId(ring_id.0),
                    });
                }
                driver_model::DriverEventOut::StreamClosed { .. } => {}
            }
        }

        while *edge_event_cursor < edge_establisher.events().len() {
            let event = edge_establisher.events()[*edge_event_cursor].clone();
            *edge_event_cursor += 1;
            progressed = true;
            match event {
                edge::EdgeLifecycleEvent::EdgeReady { edge_id, .. } => {
                    if Some(edge_id.0) == *inbound_edge_id {
                        if let Some(rx_actor) = rx_edge_actor.as_mut() {
                            rx_actor.observe(edge_actor::RxEvent::EdgeReady {
                                edge_id: edge_actor::EdgeId(edge_id.0),
                            });
                        }
                        runtime
                            .send_to(
                                node_actor,
                                NodeAgentMsg::MarkInboundEdgeReady { edge_id: edge_id.0 },
                            )
                            .map_err(|e| format!("mark inbound ready: {e}"))?;
                    }
                    if Some(edge_id.0) == *outbound_edge_id {
                        if let Some(tx_actor) = tx_edge_actor.as_mut() {
                            tx_actor.observe(edge_actor::TxEvent::EdgeReady {
                                edge_id: edge_actor::EdgeId(edge_id.0),
                            });
                        }
                        runtime
                            .send_to(
                                node_actor,
                                NodeAgentMsg::MarkOutboundEdgeReady { edge_id: edge_id.0 },
                            )
                            .map_err(|e| format!("mark outbound ready: {e}"))?;
                    }
                }
                edge::EdgeLifecycleEvent::EdgeFaulted { .. }
                | edge::EdgeLifecycleEvent::EdgeStopped { .. } => {}
            }
        }

        if !progressed {
            break;
        }
    }
    Ok(())
}

struct GpuWorkerRuntime {
    ctl: worker_ctl::GpuWorkerCtl,
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    generation: u64,
}

impl GpuWorkerRuntime {
    fn spawn(stage_index: u32, arena_fd: i32) -> Result<Self, String> {
        let mut ctl = worker_ctl::GpuWorkerCtl::new(worker_ctl::WorkerConfig {
            node_id: worker_ctl::NodeId(u64::from(stage_index) + 1),
            arena_env: worker_ctl::ArenaEnv {
                arena_fd,
                arena_bytes: ARENA_BYTES as u64,
            },
            initialization_timeout_ms: 10_000,
        });
        ctl.observe(worker_ctl::WorkerCtlEvent::StartWorker);

        let mut child = Command::new("python3")
            .arg(tinygrad_worker_path())
            .env("SWACTOR_ARENA_FD", arena_fd.to_string())
            .env("SWACTOR_ARENA_BYTES", ARENA_BYTES.to_string())
            .env("SWACTOR_STAGE_INDEX", stage_index.to_string())
            .env("DEV", "CPU")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| format!("spawn tinygrad CPU worker: {e}"))?;
        let process_id = worker_ctl::ProcessId(u64::from(child.id()));
        ctl.observe(worker_ctl::WorkerCtlEvent::ProcessStarted { pid: process_id });
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "worker stdin missing".to_owned())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "worker stdout missing".to_owned())?;
        let mut worker = Self {
            ctl,
            child,
            stdin,
            stdout: BufReader::new(stdout),
            generation: 1,
        };
        worker.initialize()?;
        Ok(worker)
    }

    fn initialize(&mut self) -> Result<(), String> {
        let value = self.command(
            json!({
                "type":"InitializeWorker",
                "worker_generation": self.generation,
                "arena_ceiling": ARENA_BYTES,
                "required_ring_helper_abi": 1,
                "backend": {"device":"CPU"},
            }),
            "WorkerReady",
        )?;
        let generation = value
            .get("worker_generation")
            .or_else(|| value.get("generation"))
            .and_then(|value| value.as_u64())
            .unwrap_or(self.generation);
        self.generation = generation;
        self.ctl.observe(worker_ctl::WorkerCtlEvent::WorkerReady {
            generation: worker_ctl::WorkerGeneration(generation),
        });
        Ok(())
    }

    fn install_ring(
        &mut self,
        ring_id: u64,
        edge_id: u64,
        port_id: &str,
        direction: &str,
        layout: arena::RingLayout,
    ) -> Result<(), String> {
        self.ctl.observe(worker_ctl::WorkerCtlEvent::ActorCommand(
            worker_ctl::ActorCommand::InstallRing {
                generation: worker_ctl::WorkerGeneration(self.generation),
                ring_id: worker_ctl::RingId(ring_id),
            },
        ));
        self.command(
            json!({
                "type":"InstallRing",
                "ring_id":ring_id,
                "edge_id":edge_id,
                "port_id":port_id,
                "direction":direction,
                "layout": {
                    "ring_id": ring_id,
                    "arena_offset": layout.start_offset,
                    "header_offset": layout.header_offset,
                    "data_offset": layout.data_offset,
                    "data_capacity": layout.data_bytes,
                    "alignment": layout.alignment,
                },
                "object_spec": {
                    "kind":"token_or_activation",
                    "max_extent": OBJECT_MAX_EXTENT,
                    "dtype_family":"i32",
                    "dtype_width_bytes":4,
                    "layout":"linear",
                    "alignment": OBJECT_ALIGNMENT,
                    "sequence_policy":"strict_increasing",
                }
            }),
            "RingInstalled",
        )?;
        self.ctl.observe(worker_ctl::WorkerCtlEvent::StdoutEvent(
            worker_ctl::WorkerEvent::RingInstalled {
                ring_id: worker_ctl::RingId(ring_id),
            },
        ));
        Ok(())
    }

    fn configure_role(
        &mut self,
        run_id: u64,
        stage_index: u32,
        layer_start: u32,
        layer_end_exclusive: u32,
    ) -> Result<(), String> {
        self.command(
            json!({
                "type":"ConfigureRole",
                "role_id":stage_index + 1,
                "config": {
                    "run_id":run_id,
                    "stage_index":stage_index,
                    "layer_start":layer_start,
                    "layer_end_exclusive":layer_end_exclusive,
                    "model_id":"local-e2e-cluster-tinygrad-cpu-fixture",
                    "gguf_source":"docker-cpu://local-e2e-cluster-tinygrad-cpu-fixture",
                }
            }),
            "RoleConfigured",
        )
        .map(|_| ())
    }

    fn ring_readable(&mut self, ring_id: u64, edge_id: u64) -> Result<LoadedObject, String> {
        let value = self.command(
            json!({"type":"RingReadable","ring_id":ring_id}),
            "ObjectLoaded",
        )?;
        let loaded = LoadedObject {
            object_id: value
                .get("object_id")
                .and_then(|value| value.as_u64())
                .ok_or_else(|| format!("ObjectLoaded missing object_id: {value}"))?,
            edge_id,
            sequence: value
                .get("sequence")
                .and_then(|value| value.as_u64())
                .ok_or_else(|| format!("ObjectLoaded missing sequence: {value}"))?,
            handle_generation: value
                .get("device_handle")
                .or_else(|| value.get("handle"))
                .and_then(|value| {
                    value
                        .get("worker_generation")
                        .or_else(|| value.get("generation"))
                })
                .and_then(|value| value.as_u64())
                .ok_or_else(|| format!("ObjectLoaded missing handle generation: {value}"))?,
            handle_id: value
                .get("device_handle")
                .or_else(|| value.get("handle"))
                .and_then(|value| value.get("id"))
                .and_then(|value| value.as_u64())
                .ok_or_else(|| format!("ObjectLoaded missing handle id: {value}"))?,
        };
        self.ctl.observe(worker_ctl::WorkerCtlEvent::StdoutEvent(
            worker_ctl::WorkerEvent::ObjectLoaded {
                object_id: worker_ctl::ObjectId(loaded.object_id),
                sequence: loaded.sequence,
            },
        ));
        Ok(loaded)
    }

    fn execute_step(
        &mut self,
        role_id: u64,
        step_id: u64,
        input_object_id: u64,
        input_sequence: u64,
        input_handle: u64,
        output_ring_id: u64,
        output_object_id: u64,
        output_sequence: u64,
        final_stage: bool,
    ) -> Result<usize, String> {
        self.ctl.observe(worker_ctl::WorkerCtlEvent::ActorCommand(
            worker_ctl::ActorCommand::ExecuteStep {
                generation: worker_ctl::WorkerGeneration(self.generation),
                step_id: worker_ctl::StepId(step_id),
                input: worker_ctl::DeviceHandle {
                    generation: worker_ctl::WorkerGeneration(self.generation),
                    id: input_handle,
                },
            },
        ));
        let produced = self.command(
            json!({
                "type":"ExecuteStep",
                "role_id":role_id,
                "step_id":step_id,
                "inputs":[{
                    "port_id":"input",
                    "object_id":input_object_id,
                    "sequence":input_sequence,
                    "device_handle":{"worker_generation":self.generation,"id":input_handle},
                }],
                "outputs":[{
                    "port_id":"output",
                    "ring_id":output_ring_id,
                    "object_id":output_object_id,
                    "sequence":output_sequence,
                    "extent":4,
                    "flags": if final_stage { 1 } else { 0 },
                }],
                "runtime":{"final_stage":final_stage},
                "release_inputs_after":false,
            }),
            "ObjectProduced",
        )?;
        let produced_object_id = produced
            .get("object_id")
            .and_then(|value| value.as_u64())
            .unwrap_or(output_object_id);
        let produced_sequence = produced
            .get("sequence")
            .and_then(|value| value.as_u64())
            .unwrap_or(output_sequence);
        self.ctl.observe(worker_ctl::WorkerCtlEvent::StdoutEvent(
            worker_ctl::WorkerEvent::ObjectProduced {
                object_id: worker_ctl::ObjectId(produced_object_id),
                sequence: produced_sequence,
            },
        ));
        self.expect_event("StepCompleted")?;
        self.ctl.observe(worker_ctl::WorkerCtlEvent::StdoutEvent(
            worker_ctl::WorkerEvent::StepCompleted {
                step_id: worker_ctl::StepId(step_id),
            },
        ));
        produced
            .get("committed_bytes")
            .and_then(|value| value.as_u64())
            .map(|value| value as usize)
            .ok_or_else(|| format!("ObjectProduced missing committed_bytes: {produced}"))
    }

    fn release_device_object(&mut self, handle: u64) -> Result<(), String> {
        self.ctl.observe(worker_ctl::WorkerCtlEvent::ActorCommand(
            worker_ctl::ActorCommand::ReleaseDeviceObject {
                generation: worker_ctl::WorkerGeneration(self.generation),
                handle: worker_ctl::DeviceHandle {
                    generation: worker_ctl::WorkerGeneration(self.generation),
                    id: handle,
                },
            },
        ));
        self.command(
            json!({"type":"ReleaseDeviceObject","device_handle":{"worker_generation":self.generation,"id":handle}}),
            "DeviceObjectReleased",
        )
        .map(|_| ())
    }

    fn shutdown(&mut self) -> Result<(), String> {
        self.ctl
            .observe(worker_ctl::WorkerCtlEvent::ShutdownRequested);
        self.command(
            json!({"type":"ShutdownWorker","mode":"Graceful"}),
            "WorkerStopped",
        )
        .map(|_| ())
    }

    fn expect_event(&mut self, expected: &str) -> Result<serde_json::Value, String> {
        let mut line = String::new();
        self.stdout
            .read_line(&mut line)
            .map_err(|e| format!("read worker stdout: {e}"))?;
        let value: serde_json::Value = serde_json::from_str(&line)
            .map_err(|e| format!("parse worker stdout {line:?}: {e}"))?;
        if value.get("type").and_then(|value| value.as_str()) == Some(expected) {
            Ok(value)
        } else {
            Err(format!("worker emitted {value}, expected {expected}"))
        }
    }

    fn command(
        &mut self,
        command: serde_json::Value,
        expected: &str,
    ) -> Result<serde_json::Value, String> {
        if command.get("payload").is_some()
            || command.get("bytes").is_some()
            || command.get("data").is_some()
        {
            return Err(format!(
                "worker command illegally carried payload bytes: {command}"
            ));
        }
        writeln!(self.stdin, "{command}").map_err(|e| format!("write worker command: {e}"))?;
        self.stdin
            .flush()
            .map_err(|e| format!("flush worker stdin: {e}"))?;
        self.expect_event(expected)
    }
}

impl Drop for GpuWorkerRuntime {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn local_docker_spec(
    run_id: u64,
    node_id: u64,
    stage_index: u32,
    outbound_endpoint_json: &str,
    coordinator_endpoint_json: &str,
    orchestrator_actor_json: &str,
) -> NodeProvisionSpec {
    NodeProvisionSpec {
        run_id,
        node_id,
        stage_index: Some(stage_index),
        image: docker_image(),
        env: vec![
            ("DEV".to_owned(), "CPU".to_owned()),
            ("PYTHONDONTWRITEBYTECODE".to_owned(), "1".to_owned()),
            (
                "MVP_TINYGRAD_WORKER".to_owned(),
                "/workspace/crates/mvp-system/tests/local_e2e_cluster/tinygrad_cpu_worker.py"
                    .to_owned(),
            ),
        ],
        args: vec![
            "--role=node".to_owned(),
            "--logical-node-id".to_owned(),
            node_id.to_string(),
            "--stage-index".to_owned(),
            stage_index.to_string(),
            "--outbound-endpoint".to_owned(),
            outbound_endpoint_json.to_owned(),
            "--coordinator-endpoint".to_owned(),
            coordinator_endpoint_json.to_owned(),
            "--orchestrator-actor".to_owned(),
            orchestrator_actor_json.to_owned(),
        ],
    }
}

struct LocalE2eContainer {
    container_name: String,
    stdin: ChildStdin,
}

struct LocalE2eDockerCli {
    spec: NodeProvisionSpec,
    events: Sender<LocalDockerNodeEvent>,
    nodes: HashMap<String, LocalE2eContainer>,
    provider_process_id: Option<u32>,
}

impl LocalE2eDockerCli {
    fn new(spec: NodeProvisionSpec, events: Sender<LocalDockerNodeEvent>) -> Self {
        Self {
            spec,
            events,
            nodes: HashMap::new(),
            provider_process_id: None,
        }
    }

    fn provider_process_id(&self) -> Option<u32> {
        self.provider_process_id
    }
}

impl docker_provision::DockerCli for LocalE2eDockerCli {
    fn run_container(
        &mut self,
        request: docker_provision::DockerRunRequest,
    ) -> Result<docker_provision::DockerRunResult, docker_provision::DockerCliError> {
        let mut env = request.env.clone();
        for (key, value) in &self.spec.env {
            env.insert(key.clone(), value.clone());
        }

        let mut command = Command::new("docker");
        command
            .arg("run")
            .arg("--rm")
            .arg("--add-host")
            .arg("host.docker.internal:host-gateway")
            .arg("--name")
            .arg(&request.container_name)
            .arg("-i");
        for (key, value) in &request.labels {
            command.arg("--label").arg(format!("{key}={value}"));
        }
        for (key, value) in &env {
            command.arg("-e").arg(format!("{key}={value}"));
        }
        command.arg(&self.spec.image);
        for arg in &self.spec.args {
            command.arg(arg);
        }

        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                docker_provision::DockerCliError::new(format!(
                    "spawn Docker node {}: {e}",
                    self.spec.node_id
                ))
            })?;

        let provider_process_id = child.id();
        self.provider_process_id = Some(provider_process_id);
        let stdin = child.stdin.take().ok_or_else(|| {
            docker_provision::DockerCliError::new(format!(
                "Docker node {} stdin missing",
                self.spec.node_id
            ))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            docker_provision::DockerCliError::new(format!(
                "Docker node {} stdout missing",
                self.spec.node_id
            ))
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            docker_provision::DockerCliError::new(format!(
                "Docker node {} stderr missing",
                self.spec.node_id
            ))
        })?;

        self.nodes.insert(
            request.container_name.clone(),
            LocalE2eContainer {
                container_name: request.container_name.clone(),
                stdin,
            },
        );

        spawn_local_docker_reader(stdout, self.events.clone(), true);
        spawn_local_docker_reader(stderr, self.events.clone(), false);
        let events = self.events.clone();
        thread::spawn(move || match child.wait() {
            Ok(status) => {
                let _ = events.send(LocalDockerNodeEvent::Exited(status.code()));
            }
            Err(error) => {
                let _ = events.send(LocalDockerNodeEvent::Stderr(format!(
                    "wait Docker node: {error}"
                )));
                let _ = events.send(LocalDockerNodeEvent::Exited(None));
            }
        });

        Ok(docker_provision::DockerRunResult {
            container_id: request.container_name,
        })
    }

    fn inspect_ssh_endpoint(
        &mut self,
        container_id: &str,
    ) -> Result<Option<node_provision::SshEndpoint>, docker_provision::DockerCliError> {
        Ok(Some(node_provision::SshEndpoint {
            host: "127.0.0.1".to_owned(),
            port: 0,
            user: "local-e2e".to_owned(),
            auth_ref: format!("local-docker:{container_id}"),
        }))
    }

    fn remove_force(&mut self, container_id: &str) -> Result<(), docker_provision::DockerCliError> {
        let Some(mut node) = self.nodes.remove(container_id) else {
            return Ok(());
        };
        let _ = writeln!(node.stdin, "shutdown");
        let _ = node.stdin.flush();
        let status = Command::new("docker")
            .arg("stop")
            .arg("-t")
            .arg("2")
            .arg(&node.container_name)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|e| {
                docker_provision::DockerCliError::new(format!(
                    "docker stop {}: {e}",
                    node.container_name
                ))
            })?;
        if status.success() {
            Ok(())
        } else {
            Err(docker_provision::DockerCliError::new(format!(
                "docker stop {} exited with {status}",
                node.container_name
            )))
        }
    }
}

#[derive(Default)]
struct LocalE2eBootstrapFactory;

impl docker_provision::SshBootstrapClientFactory for LocalE2eBootstrapFactory {
    type Client = LocalE2eBootstrapClient;

    fn client_for(&mut self, _spec: &node_provision::BootstrapSessionSpec) -> Self::Client {
        LocalE2eBootstrapClient
    }
}

struct LocalE2eBootstrapClient;

impl docker_provision::BootstrapSshClient for LocalE2eBootstrapClient {
    fn connect(
        &mut self,
        _endpoint: &node_provision::SshEndpoint,
    ) -> Result<(), docker_provision::BootstrapSshError> {
        Ok(())
    }

    fn probe_stdout(&mut self) -> Result<(), docker_provision::BootstrapSshError> {
        Ok(())
    }

    fn read_bootstrap_logs(
        &mut self,
        _stdout_sources: &[String],
        _stderr_sources: &[String],
    ) -> Result<
        Vec<(node_provision::BootstrapLogStream, String)>,
        docker_provision::BootstrapSshError,
    > {
        Ok(Vec::new())
    }

    fn run_verify_commands(
        &mut self,
        _commands: &[String],
    ) -> Result<(), docker_provision::BootstrapSshError> {
        Ok(())
    }

    fn start_swactor(
        &mut self,
        _command: &str,
        _join: &node_provision::SwarmJoinSpec,
    ) -> Result<(), docker_provision::BootstrapSshError> {
        Ok(())
    }

    fn close(&mut self) {}
}

#[derive(Default)]
struct LocalE2eBootstrapDatastream;

impl node_provision::BootstrapDatastreamSink for LocalE2eBootstrapDatastream {
    fn record(&mut self, _record: node_provision::BootstrapLogRecord) {}

    fn flush(&mut self) {}
}

fn spawn_local_docker_reader(
    stream: impl std::io::Read + Send + 'static,
    events: Sender<LocalDockerNodeEvent>,
    stdout: bool,
) {
    thread::spawn(move || {
        let reader = BufReader::new(stream);
        for next in reader.lines() {
            let Ok(line) = next else {
                break;
            };
            let event = if stdout {
                LocalDockerNodeEvent::Stdout(line)
            } else {
                LocalDockerNodeEvent::Stderr(line)
            };
            if events.send(event).is_err() {
                break;
            }
        }
    });
}

fn logical_node_spec_from_local(spec: &NodeProvisionSpec) -> node_provision::LogicalNodeSpec {
    let logical_node_id = node_provision::LogicalNodeId(spec.node_id.to_string());
    node_provision::LogicalNodeSpec {
        run_id: node_provision::RunId(spec.run_id),
        logical_node_id: logical_node_id.clone(),
        group_id: node_provision::NodeGroupId("local-e2e".to_owned()),
        role: node_provision::RoleId("stage-worker".to_owned()),
        provider: node_provision::ProviderKind::Docker,
        shape: node_provision::DesiredNodeShape {
            image: spec.image.clone(),
            disk_gb: 0,
            gpu_name: None,
            min_gpu_ram_mb: None,
            min_down_mbps: None,
            min_up_mbps: None,
            min_reliability: None,
            require_verified: false,
            provider_labels: BTreeMap::from([
                ("mvp.local_e2e".to_owned(), "true".to_owned()),
                ("mvp.node_id".to_owned(), spec.node_id.to_string()),
            ]),
        },
        boot: node_provision::BootSpec {
            ssh_user: "local-e2e".to_owned(),
            verify_commands: Vec::new(),
            start_swactor_command: spec.args.join(" "),
            stdout_sources: Vec::new(),
            stderr_sources: Vec::new(),
            timeout_policy: node_provision::BootstrapTimeoutPolicy {
                ssh_connect_secs: 1,
                boot_check_secs: 1,
                swactor_join_secs: 30,
            },
        },
        swarm_join: node_provision::SwarmJoinSpec {
            orch_swactor_addr: "local-e2e".to_owned(),
            join_token_ref: "local-e2e".to_owned(),
            expected_logical_node_id: logical_node_id,
        },
    }
}

fn provision_local_docker_node(
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    dashboard: &mut Option<&mut MvpDashboard>,
    stats: &mut ProvisionStats,
    spec: NodeProvisionSpec,
) -> Result<ProvisionedDockerNode, String> {
    let expected_run_id = spec.run_id;
    let expected_node_id = spec.node_id;
    let expected_stage_index = spec.stage_index.unwrap_or_default();
    let logical_spec = logical_node_spec_from_local(&spec);
    let (events_tx, events_rx) = mpsc::channel();
    let cli = LocalE2eDockerCli::new(spec, events_tx);
    let provider = docker_provision::DockerProvider::new(cli);
    let mut provisioner = docker_provision::DockerNodeProvisioner::new(
        provider,
        LocalE2eBootstrapFactory,
        LocalE2eBootstrapDatastream,
    );
    provisioner
        .start(logical_spec)
        .map_err(|e| format!("start Docker node {expected_node_id}: {e:?}"))?;
    let provider_process_id = provisioner.provider().cli().provider_process_id();

    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(30) {
        pump_network(driver, stack);
        drain_dashboard(dashboard);
        publish_runtime_snapshot_throttled(dashboard, stack);
        while let Ok(event) = events_rx.try_recv() {
            match event {
                LocalDockerNodeEvent::Stdout(line) => {
                    handle_local_node_stdout(
                        expected_run_id,
                        expected_node_id,
                        &line,
                        dashboard,
                        stats,
                    );
                    if let Some((endpoint, node_actor, stage_index)) =
                        ready_node_from_stdout(expected_node_id, expected_stage_index, &line)?
                    {
                        provisioner
                            .observe_swactor_join(node_provision::SwactorId(format!(
                                "local-e2e-node-{expected_node_id}"
                            )))
                            .map_err(|e| {
                                format!("complete Docker node {expected_node_id} handoff: {e:?}")
                            })?;
                        stats.node_live_count += 1;
                        return Ok(ProvisionedDockerNode {
                            node_id: expected_node_id,
                            stage_index,
                            endpoint,
                            node_actor,
                            provider_process_id,
                            provisioner,
                            events: events_rx,
                        });
                    }
                }
                LocalDockerNodeEvent::Stderr(line) => {
                    handle_local_node_stderr(
                        expected_run_id,
                        expected_node_id,
                        &line,
                        dashboard,
                        stats,
                    );
                }
                LocalDockerNodeEvent::Exited(status) => {
                    return Err(format!(
                        "node {expected_node_id} process exited before ready: {status:?}"
                    ));
                }
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    Err(format!("timed out provisioning node {expected_node_id}"))
}

fn ready_node_from_stdout(
    expected_node_id: u64,
    default_stage_index: u32,
    line: &str,
) -> Result<Option<(EndpointAddr, ActorAddress, u32)>, String> {
    let Ok(line) = serde_json::from_str::<NodeStdoutLine>(line) else {
        return Ok(None);
    };
    if line.kind != "ready" {
        return Ok(None);
    }
    if line.logical_node_id != Some(expected_node_id) {
        return Ok(None);
    }
    let endpoint = line
        .endpoint
        .ok_or_else(|| format!("ready line for node {expected_node_id} missing endpoint"))?;
    let node_actor = line
        .node_actor
        .ok_or_else(|| format!("ready line for node {expected_node_id} missing node actor"))?;
    Ok(Some((
        endpoint,
        node_actor,
        line.stage_index.unwrap_or(default_stage_index),
    )))
}

fn drain_provisioned_node_events(
    nodes: &mut [&mut ProvisionedDockerNode],
    run_id: u64,
    dashboard: &mut Option<&mut MvpDashboard>,
    stats: &mut ProvisionStats,
) -> Result<usize, String> {
    let mut stage_ready_count = 0usize;
    for node in nodes.iter_mut() {
        while let Ok(event) = node.events.try_recv() {
            match event {
                LocalDockerNodeEvent::Stdout(line) => {
                    stage_ready_count +=
                        handle_local_node_stdout(run_id, node.node_id, &line, dashboard, stats);
                }
                LocalDockerNodeEvent::Stderr(line) => {
                    handle_local_node_stderr(run_id, node.node_id, &line, dashboard, stats);
                }
                LocalDockerNodeEvent::Exited(status) => {
                    return Err(format!(
                        "node {} process exited before stop: {status:?}",
                        node.node_id
                    ));
                }
            }
        }
    }
    Ok(stage_ready_count)
}

fn handle_local_node_stdout(
    run_id: u64,
    node_id: u64,
    line: &str,
    dashboard: &mut Option<&mut MvpDashboard>,
    stats: &mut ProvisionStats,
) -> usize {
    stats.stdout_line_count += 1;
    record_dashboard_provision_log(dashboard, run_id, node_id, ProvisionLogStream::Stdout, line);
    record_stage_ready_from_stdout(run_id, ProvisionLogStream::Stdout, line, dashboard)
}

fn handle_local_node_stderr(
    run_id: u64,
    node_id: u64,
    line: &str,
    dashboard: &mut Option<&mut MvpDashboard>,
    stats: &mut ProvisionStats,
) {
    stats.stderr_line_count += 1;
    record_dashboard_provision_log(dashboard, run_id, node_id, ProvisionLogStream::Stderr, line);
}

fn record_stage_ready_from_stdout(
    run_id: u64,
    stream: ProvisionLogStream,
    line: &str,
    dashboard: &mut Option<&mut MvpDashboard>,
) -> usize {
    if stream != ProvisionLogStream::Stdout {
        return 0;
    }
    let Ok(line) = serde_json::from_str::<NodeStdoutLine>(line) else {
        return 0;
    };
    if line.kind != "node_lifecycle" {
        return 0;
    }
    let Some(stage_index) = line.stage_index else {
        return 0;
    };
    if line
        .event
        .as_deref()
        .is_some_and(|event| event.contains("StageReady"))
    {
        record_dashboard_event(
            dashboard,
            stage_event(run_id, stage_index, obs::EventKind::StageReady),
        );
        1
    } else {
        0
    }
}

fn stop_provisioned_nodes(
    nodes: &mut [&mut ProvisionedDockerNode],
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    dashboard: &mut Option<&mut MvpDashboard>,
) -> Result<(), String> {
    for node in nodes.iter_mut().rev() {
        node.provisioner
            .stop()
            .map_err(|e| format!("stop node {}: {e:?}", node.node_id))?;
    }
    pump_network(driver, stack);
    drain_dashboard(dashboard);
    publish_runtime_snapshot(dashboard, stack);
    Ok(())
}

fn spawn_send_pump(
    handle: tokio::runtime::Handle,
    endpoint: iroh::Endpoint,
    peer: EndpointAddr,
    edge_id: u64,
) -> Result<SendPumpHandle, String> {
    let (tx, mut rx) = tokio_mpsc::unbounded_channel::<Vec<u8>>();
    let (ready_tx, ready_rx) = mpsc::channel::<Result<(), String>>();
    handle.spawn(async move {
        let result: Result<(), String> = async {
            let conn = endpoint
                .connect(peer, EDGE_ALPN)
                .await
                .map_err(|e| format!("connect edge {edge_id}: {e}"))?;
            let mut send = conn
                .open_uni()
                .await
                .map_err(|e| format!("open edge stream {edge_id}: {e}"))?;
            send.write_all(&driver_model::encode_edge_preamble(driver_model::EdgeId(
                edge_id,
            )))
            .await
            .map_err(|e| format!("write edge preamble {edge_id}: {e}"))?;
            let _ = ready_tx.send(Ok(()));
            while let Some(record) = rx.recv().await {
                send.write_all(&record)
                    .await
                    .map_err(|e| format!("write edge record {edge_id}: {e}"))?;
            }
            send.finish()
                .map_err(|e| format!("finish edge stream {edge_id}: {e}"))?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            let _ = ready_tx.send(Err(error));
        }
    });
    ready_rx
        .recv_timeout(EDGE_READY_TIMEOUT)
        .map_err(|e| format!("edge {edge_id} sender did not become ready: {e}"))??;
    Ok(SendPumpHandle { tx })
}

fn spawn_recv_pump(
    handle: tokio::runtime::Handle,
    conn: iroh::endpoint::Connection,
    tx: Sender<DriverIngressEvent>,
    stream_id: u64,
) {
    handle.spawn(async move {
        let mut next_uni_stream_id = stream_id << 32;
        while let Ok(mut recv) = conn.accept_uni().await {
            next_uni_stream_id += 1;
            let current_stream_id = next_uni_stream_id;
            let mut preamble = [0u8; 8];
            if recv.read_exact(&mut preamble).await.is_err() {
                continue;
            }
            let edge_id = u64::from_le_bytes(preamble);
            if tx
                .send(DriverIngressEvent::StreamArrived {
                    edge_id,
                    stream_id: current_stream_id,
                })
                .is_err()
            {
                break;
            }
            let mut chunk = vec![0u8; 4096];
            loop {
                match recv.read(&mut chunk).await {
                    Ok(Some(0)) | Ok(None) => break,
                    Ok(Some(n)) => {
                        if tx
                            .send(DriverIngressEvent::BytesRead {
                                edge_id,
                                stream_id: current_stream_id,
                                bytes: chunk[..n].to_vec(),
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    });
}

fn object_record_spec() -> ingress::ObjectSpec {
    ingress::ObjectSpec {
        max_extent: OBJECT_MAX_EXTENT,
        alignment: OBJECT_ALIGNMENT,
        layout: ingress::ObjectLayout::Token,
    }
}

fn object_record(object_id: u64, sequence: u64, words: &[u32]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(words.len() * 4);
    for word in words {
        payload.extend_from_slice(&(*word as i32).to_le_bytes());
    }
    ingress::ObjectRecordBuilder::new(object_record_spec())
        .object_id(ingress::ObjectId(object_id))
        .sequence(sequence)
        .payload(payload)
        .encode()
}

fn take_complete_ingress_record(buffer: &mut Vec<u8>) -> Result<Option<Vec<u8>>, String> {
    let record = match ingress::read_object_record(buffer, object_record_spec(), false)
        .map_err(|reason| format!("invalid object record: {reason:?}"))?
    {
        ingress::ObjectRecordRead::Incomplete => return Ok(None),
        ingress::ObjectRecordRead::Complete(record) => record,
    };
    Ok(Some(buffer.drain(..record.total_len).collect()))
}

fn decode_ingress_record(record: &[u8]) -> Result<ingress::ObjectRecord, String> {
    match ingress::read_object_record(record, object_record_spec(), true)
        .map_err(|reason| format!("invalid object record: {reason:?}"))?
    {
        ingress::ObjectRecordRead::Incomplete => Err("object record incomplete".to_owned()),
        ingress::ObjectRecordRead::Complete(record) => Ok(record),
    }
}

fn object_record_words(
    metadata: &ingress::ObjectRecord,
    record: &[u8],
) -> Result<Vec<u32>, String> {
    let payload = metadata
        .payload(record)
        .ok_or_else(|| format!("object {} payload truncated", metadata.object_id.0))?;
    if payload.len() % 4 != 0 {
        return Err(format!(
            "object {} payload length {} is not word-aligned",
            metadata.object_id.0,
            payload.len()
        ));
    }
    Ok(payload
        .chunks_exact(4)
        .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
        .collect())
}

fn spawn_shutdown_listener() -> Receiver<()> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines().map_while(Result::ok) {
            if line.trim() == "shutdown" {
                let _ = tx.send(());
                break;
            }
        }
    });
    rx
}

fn parse_arg<'a>(args: &'a [String], name: &str) -> Result<&'a str, String> {
    let index = args
        .iter()
        .position(|arg| arg == name)
        .ok_or_else(|| format!("missing {name}"))?;
    args.get(index + 1)
        .map(String::as_str)
        .ok_or_else(|| format!("missing value for {name}"))
}

fn parse_optional_arg<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    let index = args.iter().position(|arg| arg == name)?;
    args.get(index + 1).map(String::as_str)
}

fn docker_image() -> String {
    std::env::var("MVP_LOCAL_E2E_CLUSTER_IMAGE").unwrap_or_else(|_| DEFAULT_DOCKER_IMAGE.to_owned())
}

fn tinygrad_worker_path() -> PathBuf {
    std::env::var("MVP_TINYGRAD_WORKER")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from("crates/mvp-system/tests/local_e2e_cluster/tinygrad_cpu_worker.py")
        })
}

fn tokenize_prompt(prompt: &str) -> Vec<u32> {
    let mut tokens = prompt
        .split_whitespace()
        .map(|word| match word.to_ascii_lowercase().as_str() {
            "ping" => 2,
            "hello" => 3,
            "local" => 4,
            "cluster" => 5,
            "pong" => 6,
            "world" => 7,
            "ok" => 8,
            other => 20 + (other.bytes().fold(0u32, |acc, byte| acc + u32::from(byte)) % 100),
        })
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        tokens.push(3);
    }
    tokens
}

fn detokenize_response(tokens: &[u32]) -> String {
    tokens
        .iter()
        .map(|token| match *token {
            1 => "<eos>".to_owned(),
            2 => "ping".to_owned(),
            3 => "hello".to_owned(),
            4 => "local".to_owned(),
            5 => "cluster".to_owned(),
            6 => "pong".to_owned(),
            7 => "world".to_owned(),
            8 => "ok".to_owned(),
            other => format!("<tok{other}>"),
        })
        .collect::<Vec<_>>()
        .join(" ")
}
