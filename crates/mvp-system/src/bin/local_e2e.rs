use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitCode, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

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
use mvp_system::distribution_stack::DistributionRuntimeStack;
use mvp_system::driver_pumps;
use mvp_system::engine_builder as engine;
use mvp_system::orchestrator_run_fsm as fsm;
use mvp_system::run_plan as plan;
use mvp_system::stage_controller as stage;
use mvp_system::tx_rx_edge_actor as edge_actor;
use serde::{Deserialize, Serialize};
use serde_json::json;
use swactor::actor::ActorAddress;
use swactor::runtime::ExternalSender;

const RUN_ID: u64 = 77;
const ORCHESTRATOR_LOGICAL_NODE_ID: u64 = 900;
const NODE0_LOGICAL_ID: u64 = 11;
const NODE1_LOGICAL_ID: u64 = 12;
const MAX_TOKENS: u64 = 1;

fn main() -> ExitCode {
    let args = std::env::args().collect::<Vec<_>>();
    let result = if args.iter().any(|arg| arg == "--role=node") {
        run_node_role(&args)
    } else {
        run_supervisor_once(RUN_ID, true)
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mvp-local-e2e: {error}");
            ExitCode::from(1)
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EdgeFrame {
    edge_id: u64,
    object_id: u64,
    sequence: u64,
    kind: String,
    token_id: Option<u32>,
    eos: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct NodeReady {
    #[serde(rename = "type")]
    kind: String,
    endpoint: EndpointAddr,
    node_actor: ActorAddress,
    token_in_addr: SocketAddr,
    logical_node_id: u64,
    stage_index: u32,
}

#[derive(Clone, Debug, Deserialize)]
struct NodeStdoutLine {
    #[serde(rename = "type")]
    kind: String,
    event: Option<String>,
}

fn run_supervisor_once(run_id: u64, print_summary: bool) -> Result<(), String> {
    let _tokio = tokio::runtime::Runtime::new().map_err(|e| format!("tokio runtime: {e}"))?;
    let mut driver = new_driver(_tokio.handle().clone())?;
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
                prompt: vec![1, 2, 3],
            },
            Some(*orchestrator_report.addr()),
        ))
        .map_err(|e| format!("spawn orchestrator actor: {e}"))?;
    stack.register_local_actor(driver.register_actor(orchestrator_addr, 1));

    let token_out_listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|e| format!("bind orchestrator token-out listener: {e}"))?;
    let token_out_addr = token_out_listener
        .local_addr()
        .map_err(|e| format!("token-out local addr: {e}"))?;
    spawn_token_out_reader(
        token_out_listener,
        stack.runtime.create_sender(),
        orchestrator_addr,
    );

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

    let mut node1 = spawn_node_process(
        stage1.node_id.0,
        stage1.stage_index,
        stage1.inbound_edge.0,
        token_out_addr,
        &self_endpoint_json,
        &orchestrator_actor_json,
    )?;
    let mut node0 = spawn_node_process(
        stage0.node_id.0,
        stage0.stage_index,
        stage0.inbound_edge.0,
        node1.ready.token_in_addr,
        &self_endpoint_json,
        &orchestrator_actor_json,
    )?;

    let started_at = Instant::now();
    wait_for_routes(
        &mut driver,
        &stack,
        &[node0.ready.node_actor, node1.ready.node_actor],
        Duration::from_secs(20),
    )?;

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

    let mut injected = false;
    let mut completed = false;
    let mut torn_down = false;
    let mut stage_ready_count = 0usize;
    let mut token_received = false;
    let mut sent_stop_to_node0 = false;
    let mut sent_stop_to_node1 = false;

    let mut token_in_object_allocator =
        edge_actor::ObjectIdAllocator::new(edge_actor::EdgeId(stage0.inbound_edge.0));
    while started_at.elapsed() < Duration::from_secs(30) {
        pump_network(&mut driver, &stack);
        stage_ready_count += drain_node_stdout(&node0.stdout_rx);
        stage_ready_count += drain_node_stdout(&node1.stdout_rx);

        while let Some(report) = orchestrator_report.try_recv() {
            match report {
                OrchestratorReport::Command(command) => match command {
                    RunCommandWire::ProvisionStage { stage_index, .. } => {
                        let provision = stage_provision_wire(&run_plan, stage_index)?;
                        let target = if stage_index == 0 {
                            node0.ready.node_actor
                        } else {
                            node1.ready.node_actor
                        };
                        stack
                            .runtime
                            .send_to(target, NodeAgentMsg::ProvisionStage(provision))
                            .map_err(|e| format!("send provision to stage {stage_index}: {e}"))?;
                    }
                    RunCommandWire::InjectPrompt {
                        sequence, prompt, ..
                    } => {
                        injected = true;

                        let prompt_object = token_in_object_allocator.alloc();
                        write_edge_frame(
                            node0.ready.token_in_addr,
                            EdgeFrame {
                                edge_id: stage0.inbound_edge.0,
                                object_id: prompt_object.object_id.0,
                                sequence,
                                kind: "token".to_owned(),
                                token_id: prompt.first().copied(),
                                eos: false,
                            },
                        )?;
                    }
                    RunCommandWire::StopRun {
                        stage_index,
                        run_id,
                    } => {
                        let target = if stage_index == 0 {
                            sent_stop_to_node0 = true;
                            node0.ready.node_actor
                        } else {
                            sent_stop_to_node1 = true;
                            node1.ready.node_actor
                        };
                        stack
                            .runtime
                            .send_to(target, NodeAgentMsg::StopRun { run_id })
                            .map_err(|e| format!("send stop to stage {stage_index}: {e}"))?;
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
                    | RunCommandWire::CreateTokenOutEndpoint { .. }
                    | RunCommandWire::BroadcastStart { .. } => {}
                },
                OrchestratorReport::Lifecycle(event) => match event {
                    LifecycleEventWire::RunCompleted { .. } => {
                        token_received = true;
                        completed = true;
                    }
                    LifecycleEventWire::RunTornDown { .. } => {
                        torn_down = true;
                    }
                    LifecycleEventWire::RunRejected { .. }
                    | LifecycleEventWire::RunFaulted { .. } => {
                        return Err(format!("run failed: {event:?}"));
                    }
                },
                OrchestratorReport::Snapshot { .. } => {}
            }
        }

        token_received |= completed;

        if injected && completed && torn_down && sent_stop_to_node0 && sent_stop_to_node1 {
            shutdown_node(&mut node0);
            shutdown_node(&mut node1);
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
            let summary = json!({
                "ok": true,
                "actor_plane": "iroh-swactor",
                "processes": {
                    "orchestrator": std::process::id(),
                    "node0": node0.child.id(),
                    "node1": node1.child.id(),
                },
                "node0_endpoint": node0.ready.endpoint,
                "node1_endpoint": node1.ready.endpoint,
                "node0_logical_id": node0.ready.logical_node_id,
                "node1_logical_id": node1.ready.logical_node_id,
                "node0_stage_index": node0.ready.stage_index,
                "node1_stage_index": node1.ready.stage_index,
                "data_plane": "tcp-loopback-streams",
                "worker_processes": "mvp-dumb-worker-per-node",
                "injected_prompt_observed": injected,
                "token_received_observed": token_received,
                "run_completed_observed": completed,
                "run_torn_down_observed": torn_down,
                "stop_sent_to_all_nodes": sent_stop_to_node0 && sent_stop_to_node1,
                "engine_builder_pattern": "pool-first-static-launcher",
                "engine_builder_event_count": topology.events.len(),
                "engine_builder_node_count": topology.node_summaries.len(),
                "engine_builder_stage_assignments": builder_stage_assignments,
                "node_route_count": stack.route_view.read().map(|view| view.len()).unwrap_or_default(),
                "stage_ready_stdout_count": stage_ready_count,
            });
            if print_summary {
                println!("{summary}");
            } else {
                eprintln!("mvp-local-e2e: run {run_id} summary {summary}");
            }
            return Ok(());
        }

        thread::sleep(Duration::from_millis(10));
    }

    shutdown_node(&mut node0);
    shutdown_node(&mut node1);
    Err(format!(
        "timed out: injected={injected} completed={completed} torn_down={torn_down} stop0={sent_stop_to_node0} stop1={sent_stop_to_node1}"
    ))
}

fn run_node_role(args: &[String]) -> Result<(), String> {
    let logical_node_id = parse_arg(args, "--logical-node-id")?
        .parse::<u64>()
        .map_err(|e| format!("logical node id: {e}"))?;
    let stage_index = parse_arg(args, "--stage-index")?
        .parse::<u32>()
        .map_err(|e| format!("stage index: {e}"))?;
    let inbound_edge_id = parse_arg(args, "--inbound-edge-id")?
        .parse::<u64>()
        .map_err(|e| format!("inbound edge id: {e}"))?;
    let outbound_addr = parse_arg(args, "--outbound-addr")?
        .parse::<SocketAddr>()
        .map_err(|e| format!("outbound addr: {e}"))?;
    let coordinator: EndpointAddr =
        serde_json::from_str(parse_arg(args, "--coordinator-endpoint")?)
            .map_err(|e| format!("coordinator endpoint json: {e}"))?;
    let orchestrator_addr: ActorAddress =
        serde_json::from_str(parse_arg(args, "--orchestrator-actor")?)
            .map_err(|e| format!("orchestrator actor json: {e}"))?;

    let _tokio = tokio::runtime::Runtime::new().map_err(|e| format!("tokio runtime: {e}"))?;
    let mut driver = new_driver(_tokio.handle().clone())?;
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
    driver.join(&[coordinator]);

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
        .map_err(|e| format!("spawn node actor: {e}"))?;
    stack.register_local_actor(driver.register_actor(node_actor, 1));

    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|e| format!("bind node inbound edge listener: {e}"))?;
    let token_in_addr = listener
        .local_addr()
        .map_err(|e| format!("node inbound local addr: {e}"))?;
    spawn_inbound_edge_reader(
        listener,
        inbound_edge_id,
        stack.runtime.create_sender(),
        node_actor,
    );

    println!(
        "{}",
        json!({
            "type": "ready",
            "role": "node",
            "endpoint": driver.endpoint_addr(),
            "node_actor": node_actor,
            "token_in_addr": token_in_addr,
            "logical_node_id": logical_node_id,
            "stage_index": stage_index,
        })
    );
    std::io::stdout()
        .flush()
        .map_err(|e| format!("flush ready: {e}"))?;

    let (stdin_tx, stdin_rx) = mpsc::channel::<serde_json::Value>();
    thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines().map_while(Result::ok) {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) {
                let _ = stdin_tx.send(value);
            }
        }
    });

    let mut worker = WorkerProc::spawn()?;
    let mut pending_commands = VecDeque::new();
    let mut outbound_stream: Option<TcpStream> = None;
    let mut outbound_object_allocator: Option<edge_actor::ObjectIdAllocator> = None;
    let started_at = Instant::now();

    loop {
        if let Ok(value) = stdin_rx.try_recv() {
            if value.get("type").and_then(|value| value.as_str()) == Some("shutdown") {
                let _ = worker.shutdown();
                return Ok(());
            }
        }

        pump_network(&mut driver, &stack);

        while let Some(report) = node_report.try_recv() {
            match report {
                NodeAgentReport::Command(command) => pending_commands.push_back(command),
                NodeAgentReport::Lifecycle(event) => {
                    println!(
                        "{}",
                        json!({
                            "type": "node_lifecycle",
                            "stage_index": stage_index,
                            "event": format!("{event:?}"),
                        })
                    );
                    let _ = std::io::stdout().flush();
                }
                NodeAgentReport::Snapshot { .. } => {}
            }
        }

        let route_to_orchestrator_ready = stack
            .route_view
            .read()
            .map(|view| view.contains_key(&orchestrator_addr))
            .unwrap_or(false);
        let mut deferred = VecDeque::new();
        while let Some(command) = pending_commands.pop_front() {
            if !route_to_orchestrator_ready && readiness_command(&command) {
                deferred.push_back(command);
                continue;
            }
            handle_node_command(
                command,
                &stack.runtime,
                node_actor,
                &mut worker,
                outbound_addr,
                &mut outbound_stream,
                &mut outbound_object_allocator,
                stage_index,
            )?;
        }
        pending_commands = deferred;

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
            additional_alpns: vec![],
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
        "local-process-e2e",
        engine::ModelSpec::pipelined_causal_llm(
            "local-e2e-fixture",
            engine::ModelArtifact::TestTinyLlm {
                path: "local-process://local-e2e-fixture".to_owned(),
            },
            4,
            8,
            engine::DTypeFamily::BFloat,
            2,
            8,
            99,
        ),
    )
    .run_id(run_id)
    .image(
        engine::NodeImageSpec::new("mvp-local-e2e")
            .worker_runtime(engine::WorkerRuntimeSpec::DumbProcess),
    )
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
        weight_artifact: "local-e2e-fixture".to_owned(),
    })
}

fn write_edge_frame(addr: SocketAddr, frame: EdgeFrame) -> Result<(), String> {
    let mut stream = TcpStream::connect(addr).map_err(|e| format!("connect edge {addr}: {e}"))?;
    stream
        .write_all(&driver_pumps::encode_edge_preamble(driver_pumps::EdgeId(
            frame.edge_id,
        )))
        .map_err(|e| format!("write edge preamble: {e}"))?;
    write_json_frame(&mut stream, &frame)
}

fn write_json_frame(stream: &mut TcpStream, frame: &EdgeFrame) -> Result<(), String> {
    serde_json::to_writer(&mut *stream, frame).map_err(|e| format!("serialize edge frame: {e}"))?;
    stream
        .write_all(b"\n")
        .map_err(|e| format!("write edge frame newline: {e}"))?;
    stream.flush().map_err(|e| format!("flush edge frame: {e}"))
}

fn spawn_inbound_edge_reader(
    listener: TcpListener,
    expected_edge_id: u64,
    sender: ExternalSender,
    node_actor: ActorAddress,
) {
    thread::spawn(move || {
        for incoming in listener.incoming() {
            let Ok(mut stream) = incoming else { continue };
            let mut preamble = [0u8; 8];
            if std::io::Read::read_exact(&mut stream, &mut preamble).is_err() {
                continue;
            }
            if preamble != expected_edge_id.to_le_bytes() {
                continue;
            }
            let mut reader = BufReader::new(stream);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => break,
                    Ok(_) => {
                        if let Ok(frame) = serde_json::from_str::<EdgeFrame>(&line) {
                            let _ = sender.send_to(
                                node_actor,
                                NodeAgentMsg::ObjectLoaded {
                                    edge_id: frame.edge_id,
                                    object_id: frame.object_id,
                                    sequence: frame.sequence,
                                    handle_generation: 1,
                                    handle_id: frame.object_id,
                                },
                            );
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    });
}

fn spawn_token_out_reader(
    listener: TcpListener,
    sender: ExternalSender,
    orchestrator_addr: ActorAddress,
) {
    thread::spawn(move || {
        for incoming in listener.incoming() {
            let Ok(mut stream) = incoming else { continue };
            let mut preamble = [0u8; 8];
            if std::io::Read::read_exact(&mut stream, &mut preamble).is_err() {
                continue;
            }
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            if reader.read_line(&mut line).is_ok()
                && let Ok(frame) = serde_json::from_str::<EdgeFrame>(&line)
            {
                let _ = sender.send_to(
                    orchestrator_addr,
                    OrchestratorMsg::ObserveTokenReceived {
                        sequence: frame.sequence,
                        token_id: frame.token_id.unwrap_or(99),
                        eos: frame.eos,
                    },
                );
            }
        }
    });
}

fn handle_node_command(
    command: StageCommandWire,
    runtime: &swactor::runtime::Runtime,
    node_actor: ActorAddress,
    worker: &mut WorkerProc,
    outbound_addr: SocketAddr,
    outbound_stream: &mut Option<TcpStream>,
    outbound_object_allocator: &mut Option<edge_actor::ObjectIdAllocator>,
    local_stage_index: u32,
) -> Result<(), String> {
    match command {
        StageCommandWire::EstablishInboundEdge { edge_id } => runtime
            .send_to(node_actor, NodeAgentMsg::MarkInboundEdgeReady { edge_id })
            .map_err(|e| format!("mark inbound ready: {e}")),
        StageCommandWire::EstablishOutboundEdge { edge_id } => {
            let mut stream = TcpStream::connect(outbound_addr)
                .map_err(|e| format!("connect outbound edge {edge_id} to {outbound_addr}: {e}"))?;
            stream
                .write_all(&driver_pumps::encode_edge_preamble(driver_pumps::EdgeId(
                    edge_id,
                )))
                .map_err(|e| format!("write outbound preamble: {e}"))?;
            *outbound_stream = Some(stream);
            *outbound_object_allocator = Some(edge_actor::ObjectIdAllocator::new(
                edge_actor::EdgeId(edge_id),
            ));
            runtime
                .send_to(node_actor, NodeAgentMsg::MarkOutboundEdgeReady { edge_id })
                .map_err(|e| format!("mark outbound ready: {e}"))
        }
        StageCommandWire::ConfigureWorkerRole { .. } => {
            worker.initialize()?;
            runtime
                .send_to(node_actor, NodeAgentMsg::MarkWorkerReady)
                .map_err(|e| format!("mark worker ready: {e}"))
        }
        StageCommandWire::LoadWeights { .. } => runtime
            .send_to(node_actor, NodeAgentMsg::MarkWeightsReady)
            .map_err(|e| format!("mark weights ready: {e}")),
        StageCommandWire::ExecuteStep {
            step_id,
            sequence,
            output_edge_ids,
            ..
        } => {
            worker.execute_step(step_id)?;
            runtime
                .send_to(node_actor, NodeAgentMsg::StepCompleted { step_id })
                .map_err(|e| format!("mark step completed: {e}"))?;
            let output_edge_id = output_edge_ids.first().copied().unwrap_or(0);
            let output_key = outbound_object_allocator
                .as_mut()
                .ok_or_else(|| "outbound object allocator missing for ExecuteStep".to_owned())?
                .alloc();
            let stream = outbound_stream
                .as_mut()
                .ok_or_else(|| "outbound stream missing for ExecuteStep".to_owned())?;
            let final_stage = local_stage_index == 1;
            write_json_frame(
                stream,
                &EdgeFrame {
                    edge_id: output_edge_id,
                    object_id: output_key.object_id.0,
                    sequence,
                    kind: if final_stage {
                        "token".to_owned()
                    } else {
                        "activation".to_owned()
                    },
                    token_id: final_stage.then_some(99),
                    eos: final_stage,
                },
            )
        }
        StageCommandWire::ReleaseInputHandle { .. }
        | StageCommandWire::StopLocalEdges { .. }
        | StageCommandWire::ReleaseRunDeviceObjects { .. }
        | StageCommandWire::RewireEdge { .. } => Ok(()),
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

struct WorkerProc {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl WorkerProc {
    fn spawn() -> Result<Self, String> {
        let mut child = Command::new(worker_bin_path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("spawn dumb worker: {e}"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "worker stdin missing".to_owned())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "worker stdout missing".to_owned())?;
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    fn initialize(&mut self) -> Result<(), String> {
        self.command(
            json!({"type":"InitializeWorker","helper_abi_version":1}),
            "WorkerReady",
        )
    }

    fn execute_step(&mut self, step_id: u64) -> Result<(), String> {
        self.command(
            json!({"type":"ExecuteStep","step_id":step_id}),
            "StepCompleted",
        )
    }

    fn shutdown(&mut self) -> Result<(), String> {
        self.command(json!({"type":"ShutdownWorker"}), "WorkerStopped")
    }

    fn command(&mut self, command: serde_json::Value, expected: &str) -> Result<(), String> {
        writeln!(self.stdin, "{command}").map_err(|e| format!("write worker command: {e}"))?;
        self.stdin
            .flush()
            .map_err(|e| format!("flush worker stdin: {e}"))?;
        let mut line = String::new();
        self.stdout
            .read_line(&mut line)
            .map_err(|e| format!("read worker stdout: {e}"))?;
        let value: serde_json::Value = serde_json::from_str(&line)
            .map_err(|e| format!("parse worker stdout {line:?}: {e}"))?;
        if value.get("type").and_then(|value| value.as_str()) == Some(expected) {
            Ok(())
        } else {
            Err(format!("worker emitted {value}, expected {expected}"))
        }
    }
}

impl Drop for WorkerProc {
    fn drop(&mut self) {
        let _ = self.child.try_wait();
    }
}

struct NodeChild {
    child: Child,
    stdin: ChildStdin,
    stdout_rx: Receiver<NodeStdoutLine>,
    ready: NodeReady,
}

fn spawn_node_process(
    logical_node_id: u64,
    stage_index: u32,
    inbound_edge_id: u64,
    outbound_addr: SocketAddr,
    coordinator_endpoint_json: &str,
    orchestrator_actor_json: &str,
) -> Result<NodeChild, String> {
    let mut child = Command::new(std::env::current_exe().map_err(|e| format!("current exe: {e}"))?)
        .arg("--role=node")
        .arg("--logical-node-id")
        .arg(logical_node_id.to_string())
        .arg("--stage-index")
        .arg(stage_index.to_string())
        .arg("--inbound-edge-id")
        .arg(inbound_edge_id.to_string())
        .arg("--outbound-addr")
        .arg(outbound_addr.to_string())
        .arg("--coordinator-endpoint")
        .arg(coordinator_endpoint_json)
        .arg("--orchestrator-actor")
        .arg(orchestrator_actor_json)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn node {stage_index}: {e}"))?;

    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| "node stdin missing".to_owned())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "node stdout missing".to_owned())?;
    let mut reader = BufReader::new(stdout);
    let mut ready_line = String::new();
    reader
        .read_line(&mut ready_line)
        .map_err(|e| format!("read node ready: {e}"))?;
    let ready: NodeReady = serde_json::from_str(&ready_line)
        .map_err(|e| format!("parse node ready {ready_line:?}: {e}"))?;
    if ready.kind != "ready" {
        return Err(format!("node first line was not ready: {ready_line}"));
    }

    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in reader.lines().map_while(Result::ok) {
            if let Ok(value) = serde_json::from_str::<NodeStdoutLine>(&line) {
                let _ = tx.send(value);
            }
        }
    });

    Ok(NodeChild {
        child,
        stdin,
        stdout_rx: rx,
        ready,
    })
}

fn shutdown_node(node: &mut NodeChild) {
    let _ = writeln!(node.stdin, "{}", json!({"type":"shutdown"}));
    let _ = node.stdin.flush();
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(3) {
        if matches!(node.child.try_wait(), Ok(Some(_))) {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let _ = node.child.kill();
    let _ = node.child.wait();
}

fn drain_node_stdout(rx: &Receiver<NodeStdoutLine>) -> usize {
    let mut stage_ready_count = 0;
    while let Ok(line) = rx.try_recv() {
        if line.kind != "node_lifecycle" {
            continue;
        }
        let Some(event) = line.event.as_deref() else {
            continue;
        };
        if event.contains("StageReady") {
            stage_ready_count += 1;
        }
    }
    stage_ready_count
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

fn worker_bin_path() -> PathBuf {
    if let Ok(path) = std::env::var("MVP_DUMB_WORKER_BIN") {
        return PathBuf::from(path);
    }
    let mut path = std::env::current_exe().expect("current exe");
    path.set_file_name(format!("mvp-dumb-worker{}", std::env::consts::EXE_SUFFIX));
    path
}
