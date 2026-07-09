use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitCode, Stdio};
use std::sync::{
    Arc, OnceLock,
    mpsc::{self, Receiver},
};
use std::thread;
use std::time::{Duration, Instant};

use datastream::ChannelId;
use datastream::emit::{
    ClusterFrameSink, DatastreamEmitter, DatastreamEventSink, EmitterConfig, FrameSink, NoopSink,
};

use distribution::node::DistributedNodeConfig;
use distribution::types::{MemberState, NodeId as DistNodeId};
use iroh::EndpointAddr;
use iroh_driver::{IrohDriver, IrohDriverConfig};
use mvp_system::actors::node_agent::{
    NodeAgentActor, NodeAgentMsg, NodeAgentReport, StageCommandWire,
};
use mvp_system::actors::register_mvp_actor_codecs;
use mvp_system::arena_manager as arena;
use mvp_system::distribution_stack::DistributionRuntimeStack;
use mvp_system::prompt_rpc::PromptEvent;
use mvp_system::relay_provisioning::relay_runtime_config_from_env;
use mvp_system::run_plan::{GgufSource, TokenizerSource};
use mvp_system::stage_controller as stage;
use parking_lot::Mutex;
use serde_json::{Value, json};
use swactor::actor::ActorAddress;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

const DEFAULT_WORKER_SCRIPT: &str = "/usr/local/share/mvp/tinygrad_worker.py";
const DEFAULT_DEVICE: &str = "CUDA";
const DEFAULT_HF_REPO: &str = "bartowski/Llama-3.2-1B-Instruct-GGUF";
const DEFAULT_HF_FILE: &str = "Llama-3.2-1B-Instruct-Q4_K_M.gguf";
const DEFAULT_MODEL_ID: &str = "llama-3.2-1b-instruct-q4";
const DEFAULT_ARENA_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_ARENA_ALIGNMENT: u64 = 64;
const PUMP_INTERVAL: Duration = Duration::from_millis(10);
const RUNTIME_READY_RETRY_INITIAL: Duration = Duration::from_millis(100);
const RUNTIME_READY_RETRY_MAX: Duration = Duration::from_secs(2);
const NODE_BOOTSTRAP_CHANNEL: &str = "mvp.node.bootstrap";
const NODE_RUNTIME_CHANNEL: &str = "mvp.node.runtime";
const NODE_STAGE_CHANNEL: &str = "mvp.node.stage";
const NODE_WORKER_CHANNEL: &str = "mvp.node.worker";
const NODE_PROMPT_CHANNEL: &str = "mvp.node.prompt";
const NODE_SHUTDOWN_CHANNEL: &str = "mvp.node.shutdown";

fn node_event_payload(
    config: &DeploymentConfig,
    phase: &str,
    status: &str,
    detail: Value,
) -> Value {
    json!({
        "type":"NodeEvent",
        "phase":phase,
        "status":status,
        "run_id":config.run_id,
        "node_id":config.logical_node_id,
        "stage_index":config.stage_index,
        "detail":detail,
    })
}

fn emit_stdio_node_event(
    config: &DeploymentConfig,
    channel: &str,
    phase: &str,
    status: &str,
    detail: Value,
) -> Result<(), String> {
    println!(
        "{}",
        json!({
            "mvp_stdio_event":1,
            "kind":"datastream_frame",
            "channel":channel,
            "payload":node_event_payload(config, phase, status, detail),
        })
    );
    std::io::stdout()
        .flush()
        .map_err(|e| format!("flush stdio node event: {e}"))
}

fn emit_node_event(
    datastream: &mut DatastreamEmitter,
    config: &DeploymentConfig,
    channel: &str,
    phase: &str,
    status: &str,
    detail: Value,
) {
    datastream.submit_text(
        ChannelId::new(channel),
        node_event_payload(config, phase, status, detail).to_string(),
    );
    datastream.tick();
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "type")]
enum DebugJoinRequestWire {
    JoinEndpoint { endpoint: EndpointAddr },
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "type")]
enum DebugJoinResponseWire {
    JoinQueued {
        peer_node_id: String,
        has_relay: bool,
        direct_addr_count: usize,
    },
    JoinRejected {
        error: String,
        detail: String,
    },
}

enum DebugJoinCommand {
    JoinEndpoint {
        endpoint: EndpointAddr,
        reply: tokio::sync::oneshot::Sender<DebugJoinResponseWire>,
    },
}

enum DebugJoinClientError {
    Cli(String),
    Runtime(String),
}

fn debug_join_client_main(args: Vec<String>) -> ExitCode {
    match run_debug_join_client(args) {
        Ok(response) => {
            let queued = matches!(response, DebugJoinResponseWire::JoinQueued { .. });
            match serde_json::to_string(&response) {
                Ok(line) => println!("{line}"),
                Err(error) => {
                    eprintln!("mvp-worker-node debug-join: serialize response: {error}");
                    return ExitCode::from(1);
                }
            }
            if queued {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
        Err(DebugJoinClientError::Cli(error)) => {
            eprintln!("mvp-worker-node debug-join: {error}");
            ExitCode::from(2)
        }
        Err(DebugJoinClientError::Runtime(error)) => {
            eprintln!("mvp-worker-node debug-join: {error}");
            ExitCode::from(1)
        }
    }
}

fn run_debug_join_client(args: Vec<String>) -> Result<DebugJoinResponseWire, DebugJoinClientError> {
    let mut socket = None;
    let mut endpoint_json = None;
    let mut read_endpoint_stdin = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--socket" => {
                socket = Some(PathBuf::from(iter.next().ok_or_else(|| {
                    DebugJoinClientError::Cli("--socket requires a path".to_owned())
                })?));
            }
            "--endpoint-json" => {
                endpoint_json = Some(iter.next().ok_or_else(|| {
                    DebugJoinClientError::Cli("--endpoint-json requires JSON".to_owned())
                })?);
            }
            "--endpoint-json-stdin" => read_endpoint_stdin = true,
            other => {
                return Err(DebugJoinClientError::Cli(format!(
                    "unknown argument {other:?}; usage: debug-join --socket <path> (--endpoint-json <json> | --endpoint-json-stdin)"
                )));
            }
        }
    }
    let socket = socket.ok_or_else(|| {
        DebugJoinClientError::Cli(
            "missing --socket <path>; usage: debug-join --socket <path> (--endpoint-json <json> | --endpoint-json-stdin)".to_owned(),
        )
    })?;
    let endpoint_json = match (endpoint_json, read_endpoint_stdin) {
        (Some(_), true) => {
            return Err(DebugJoinClientError::Cli(
                "use either --endpoint-json or --endpoint-json-stdin, not both".to_owned(),
            ));
        }
        (Some(json), false) => json,
        (None, true) => {
            let mut json = String::new();
            std::io::stdin()
                .read_to_string(&mut json)
                .map_err(|e| DebugJoinClientError::Runtime(format!("read endpoint stdin: {e}")))?;
            json
        }
        (None, false) => {
            return Err(DebugJoinClientError::Cli(
                "missing endpoint JSON; use --endpoint-json <json> or --endpoint-json-stdin"
                    .to_owned(),
            ));
        }
    };
    let endpoint = serde_json::from_str::<EndpointAddr>(&endpoint_json)
        .map_err(|e| DebugJoinClientError::Cli(format!("parse endpoint JSON: {e}")))?;
    let request = debug_join_request_line(endpoint).map_err(DebugJoinClientError::Runtime)?;
    let mut stream = std::os::unix::net::UnixStream::connect(&socket)
        .map_err(|e| DebugJoinClientError::Runtime(format!("connect {}: {e}", socket.display())))?;
    stream
        .write_all(request.as_bytes())
        .map_err(|e| DebugJoinClientError::Runtime(format!("write request: {e}")))?;
    stream
        .flush()
        .map_err(|e| DebugJoinClientError::Runtime(format!("flush request: {e}")))?;
    let mut response_line = String::new();
    BufReader::new(stream)
        .read_line(&mut response_line)
        .map_err(|e| DebugJoinClientError::Runtime(format!("read response: {e}")))?;
    if response_line.trim().is_empty() {
        return Err(DebugJoinClientError::Runtime(
            "debug join socket closed without response".to_owned(),
        ));
    }
    serde_json::from_str::<DebugJoinResponseWire>(&response_line)
        .map_err(|e| DebugJoinClientError::Runtime(format!("parse response JSON: {e}")))
}

fn debug_join_request_line(endpoint: EndpointAddr) -> Result<String, String> {
    serde_json::to_string(&DebugJoinRequestWire::JoinEndpoint { endpoint })
        .map(|mut line| {
            line.push('\n');
            line
        })
        .map_err(|e| format!("serialize debug join request: {e}"))
}

fn spawn_debug_join_listener(
    handle: tokio::runtime::Handle,
    path: PathBuf,
) -> Result<tokio::sync::mpsc::UnboundedReceiver<DebugJoinCommand>, String> {
    use std::os::unix::fs::PermissionsExt;

    match fs::remove_file(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!(
                "remove stale debug join socket {}: {error}",
                path.display()
            ));
        }
    }
    let listener = {
        let _guard = handle.enter();
        tokio::net::UnixListener::bind(&path)
            .map_err(|e| format!("bind debug join socket {}: {e}", path.display()))?
    };
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
        .map_err(|e| format!("chmod debug join socket {}: {e}", path.display()))?;
    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel::<DebugJoinCommand>();
    handle.spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let command_tx = command_tx.clone();
                    tokio::spawn(async move {
                        handle_debug_join_stream(stream, command_tx).await;
                    });
                }
                Err(error) => {
                    eprintln!("mvp-worker-node debug join listener stopped: {error}");
                    break;
                }
            }
        }
    });
    Ok(command_rx)
}

async fn handle_debug_join_stream(
    stream: tokio::net::UnixStream,
    command_tx: tokio::sync::mpsc::UnboundedSender<DebugJoinCommand>,
) {
    let mut reader = tokio::io::BufReader::new(stream);
    let mut line = String::new();
    let response = match reader.read_line(&mut line).await {
        Ok(0) => DebugJoinResponseWire::JoinRejected {
            error: "MalformedCommand".to_owned(),
            detail: "empty request".to_owned(),
        },
        Ok(_) => match parse_debug_join_request(&line) {
            Ok(DebugJoinRequestWire::JoinEndpoint { endpoint }) => {
                let (reply, response_rx) = tokio::sync::oneshot::channel();
                if command_tx
                    .send(DebugJoinCommand::JoinEndpoint { endpoint, reply })
                    .is_err()
                {
                    DebugJoinResponseWire::JoinRejected {
                        error: "CommandQueueClosed".to_owned(),
                        detail: "worker main loop is not accepting debug join commands".to_owned(),
                    }
                } else {
                    response_rx
                        .await
                        .unwrap_or_else(|error| DebugJoinResponseWire::JoinRejected {
                            error: "CommandCancelled".to_owned(),
                            detail: error.to_string(),
                        })
                }
            }
            Err(response) => response,
        },
        Err(error) => DebugJoinResponseWire::JoinRejected {
            error: "MalformedCommand".to_owned(),
            detail: format!("read request: {error}"),
        },
    };
    let mut stream = reader.into_inner();
    if let Ok(line) = serde_json::to_string(&response) {
        let _ = stream.write_all(line.as_bytes()).await;
        let _ = stream.write_all(b"\n").await;
        let _ = stream.flush().await;
    }
}

fn parse_debug_join_request(raw: &str) -> Result<DebugJoinRequestWire, DebugJoinResponseWire> {
    let value =
        serde_json::from_str::<Value>(raw).map_err(|e| DebugJoinResponseWire::JoinRejected {
            error: "MalformedCommand".to_owned(),
            detail: e.to_string(),
        })?;
    let endpoint_decode_error = value.get("type").and_then(Value::as_str) == Some("JoinEndpoint")
        && value.get("endpoint").is_some();
    serde_json::from_value::<DebugJoinRequestWire>(value).map_err(|e| {
        DebugJoinResponseWire::JoinRejected {
            error: if endpoint_decode_error {
                "MalformedEndpoint"
            } else {
                "MalformedCommand"
            }
            .to_owned(),
            detail: e.to_string(),
        }
    })
}

fn drain_debug_join_commands(
    debug_join_rx: &mut Option<tokio::sync::mpsc::UnboundedReceiver<DebugJoinCommand>>,
    driver: &mut IrohDriver,
    config: &DeploymentConfig,
    datastream: &mut DatastreamEmitter,
) {
    let Some(rx) = debug_join_rx else {
        return;
    };
    while let Ok(command) = rx.try_recv() {
        match command {
            DebugJoinCommand::JoinEndpoint { endpoint, reply } => {
                let peer_node_id = endpoint.id.to_string();
                let has_relay = endpoint.relay_urls().next().is_some();
                let direct_addr_count = endpoint.ip_addrs().count();
                driver.join(std::slice::from_ref(&endpoint));
                emit_node_event(
                    datastream,
                    config,
                    NODE_RUNTIME_CHANNEL,
                    "debug_join",
                    "queued",
                    json!({
                        "peer_node_id":peer_node_id,
                        "has_relay":has_relay,
                        "direct_addr_count":direct_addr_count,
                    }),
                );
                let _ = reply.send(DebugJoinResponseWire::JoinQueued {
                    peer_node_id,
                    has_relay,
                    direct_addr_count,
                });
            }
        }
    }
}

fn spawn_host_gpu_sampler(handle: tokio::runtime::Handle, sink: DatastreamEventSink) {
    handle.spawn(async move {
        let mut seq = 0_u64;
        let mut interval = tokio::time::interval(datastream::hardware::gpu::GPU_SAMPLE_INTERVAL);

        loop {
            interval.tick().await;

            let sample_seq = seq;
            let sample = match tokio::task::spawn_blocking(move || {
                datastream::hardware::gpu::sample(sample_seq)
            })
            .await
            {
                Ok(sample) => sample,
                Err(error) => datastream::hardware::gpu::HostGpuSample::error(
                    sample_seq,
                    format!("gpu sampler task failed: {error}"),
                ),
            };

            seq = seq.saturating_add(1);
            sink.submit_record(&sample);
        }
    });
}

fn spawn_host_cpu_sampler(
    handle: tokio::runtime::Handle,
    sink: DatastreamEventSink,
    watched_pids: Vec<u32>,
) {
    handle.spawn(async move {
        let mut seq = 0_u64;
        let mut sampler = datastream::hardware::cpu::CpuSampler::new(watched_pids);
        let mut interval = tokio::time::interval(datastream::hardware::cpu::CPU_SAMPLE_INTERVAL);

        loop {
            interval.tick().await;

            let sample = sampler.sample(seq);
            seq = seq.saturating_add(1);
            sink.submit_record(&sample);
        }
    });
}
fn spawn_host_net_sampler(handle: tokio::runtime::Handle, sink: DatastreamEventSink) {
    handle.spawn(async move {
        let mut seq = 0_u64;
        let mut interval =
            tokio::time::interval(datastream::hardware::net::HOST_NET_SAMPLE_INTERVAL);

        loop {
            interval.tick().await;

            let sample_seq = seq;
            let sample = match tokio::task::spawn_blocking(move || {
                datastream::hardware::net::sample(sample_seq)
            })
            .await
            {
                Ok(sample) => sample,
                Err(error) => datastream::hardware::net::HostNetSample::error(
                    sample_seq,
                    format!("network sampler task failed: {error}"),
                ),
            };

            seq = seq.saturating_add(1);
            sink.submit_record(&sample);
        }
    });
}

fn spawn_arena_sampler(
    handle: tokio::runtime::Handle,
    sink: DatastreamEventSink,
    arena_manager: Arc<Mutex<arena::ArenaManager>>,
) {
    handle.spawn(async move {
        let mut seq = 0_u64;
        let mut interval = tokio::time::interval(arena::ARENA_SAMPLE_INTERVAL);

        loop {
            interval.tick().await;

            let sample = arena_manager.lock().sample(seq);
            seq = seq.saturating_add(1);
            sink.submit_record(&sample);
        }
    });
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.first().map(String::as_str) == Some("debug-join") {
        args.remove(0);
        return debug_join_client_main(args);
    }
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mvp-worker-node: {error}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), String> {
    let config = DeploymentConfig::from_env()?;
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "config",
        "ready",
        json!({
            "worker_script":&config.worker_script,
            "device":&config.device,
            "model_id":&config.model_id,
            "has_coordinator_endpoint":config.coordinator_endpoint.is_some(),
            "has_orchestrator_actor":config.orchestrator_actor.is_some(),
            "has_datastream_sink_actor":config.datastream_sink_actor.is_some(),
            "self_test_enabled":config.self_test_prompt.is_some(),
            "arena_bytes":config.arena_bytes,
            "arena_alignment":config.arena_alignment,
            "debug_join_socket":config.debug_join_socket.as_deref().unwrap_or("disabled"),
        }),
    )?;
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "process",
        "started",
        json!({"binary":"mvp-worker-node","pid":std::process::id()}),
    )?;

    let tokio = match tokio::runtime::Runtime::new() {
        Ok(runtime) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "tokio_runtime",
                "ready",
                json!({"runtime":"tokio"}),
            )?;
            runtime
        }
        Err(error) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "tokio_runtime",
                "failed",
                json!({"error":error.to_string()}),
            )?;
            return Err(format!("tokio runtime: {error}"));
        }
    };
    let mut driver = match IrohDriver::with_handle(
        tokio.handle().clone(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: config.relay_mode.clone(),
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![],
        },
    ) {
        Ok(driver) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "iroh_driver",
                "ready",
                json!({"endpoint":driver.endpoint_addr(),"relay_mode":format!("{:?}", config.relay_mode)}),
            )?;
            driver
        }
        Err(error) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "iroh_driver",
                "failed",
                json!({"error":error.to_string()}),
            )?;
            return Err(format!("create iroh driver: {error}"));
        }
    };
    if let Some(coordinator) = &config.coordinator_endpoint {
        driver.join(std::slice::from_ref(coordinator));
        emit_stdio_node_event(
            &config,
            NODE_BOOTSTRAP_CHANNEL,
            "coordinator_join",
            "started",
            json!({"endpoint":coordinator}),
        )?;
    } else {
        emit_stdio_node_event(
            &config,
            NODE_BOOTSTRAP_CHANNEL,
            "coordinator_join",
            "skipped",
            json!({"reason":"MVP_COORDINATOR_ENDPOINT not set","mode":"standalone"}),
        )?;
    }

    let stack = DistributionRuntimeStack::new_with_codecs(
        driver.node_id(),
        DistributedNodeConfig::default(),
        |registry| {
            register_mvp_actor_codecs(registry);
            datastream::wire::register_datastream_codec(registry);
        },
    );
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "distribution_stack",
        "ready",
        json!({"actors":"initialized","route_view":"initialized","swim":"initialized","outbox":"initialized"}),
    )?;
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "codecs",
        "ready",
        json!({"registered":["node_agent","orchestrator","provisioner","prompt_rpc","datastream"]}),
    )?;
    driver.enable_actor_bridge(
        stack.runtime.clone(),
        stack.codec.clone(),
        stack.actor_bridge_routes(),
        stack.actors.swim,
        stack.relay_mirror.clone(),
        stack.route_view.clone(),
    );
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "actor_bridge",
        "ready",
        json!({"transport":"iroh","routes":"attached"}),
    )?;

    let arena_manager = match arena::ArenaManager::boot(arena::ArenaConfig {
        node_id: arena::NodeId(config.logical_node_id),
        reservation_ceiling: config.arena_bytes,
        base_alignment: config.arena_alignment,
    }) {
        Ok(manager) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "arena_manager",
                "ready",
                json!({
                    "arena_bytes":config.arena_bytes,
                    "arena_alignment":config.arena_alignment,
                }),
            )?;
            Arc::new(Mutex::new(manager))
        }
        Err(error) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "arena_manager",
                "failed",
                json!({"error":format!("{error:?}")}),
            )?;
            return Err(format!("boot arena manager: {error:?}"));
        }
    };

    let mut datastream = node_datastream(&config, &stack);
    spawn_host_gpu_sampler(tokio.handle().clone(), datastream.event_sink());
    spawn_host_net_sampler(tokio.handle().clone(), datastream.event_sink());
    spawn_arena_sampler(
        tokio.handle().clone(),
        datastream.event_sink(),
        Arc::clone(&arena_manager),
    );
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "datastream_emitter",
        "ready",
        config.datastream_sink_detail(),
    )?;
    let mut debug_join_rx = match &config.debug_join_socket {
        Some(path) => {
            match spawn_debug_join_listener(tokio.handle().clone(), PathBuf::from(path)) {
                Ok(rx) => {
                    emit_node_event(
                        &mut datastream,
                        &config,
                        NODE_RUNTIME_CHANNEL,
                        "debug_join_socket",
                        "ready",
                        json!({"socket":path}),
                    );
                    Some(rx)
                }
                Err(error) => {
                    emit_node_event(
                        &mut datastream,
                        &config,
                        NODE_RUNTIME_CHANNEL,
                        "debug_join_socket",
                        "failed",
                        json!({"socket":path,"error":error}),
                    );
                    return Err(format!("bind debug join socket {}: {error}", path));
                }
            }
        }
        None => {
            emit_node_event(
                &mut datastream,
                &config,
                NODE_RUNTIME_CHANNEL,
                "debug_join_socket",
                "skipped",
                json!({"reason":"MVP_DEBUG_JOIN_SOCKET=disabled"}),
            );
            None
        }
    };

    let reports = match stack.runtime.new_inbox::<NodeAgentReport>() {
        Ok(inbox) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "node_report_inbox",
                "ready",
                json!({"actor":inbox.addr()}),
            )?;
            inbox
        }
        Err(error) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "node_report_inbox",
                "failed",
                json!({"error":error.to_string()}),
            )?;
            return Err(format!("node report inbox: {error}"));
        }
    };
    let orchestrator = config.orchestrator_actor.ok_or_else(|| {
        "MVP_ORCHESTRATOR_ACTOR is required for runtime readiness signaling".to_owned()
    })?;
    let orchestrator_source = "env";
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "orchestrator_actor",
        "ready",
        json!({"actor":orchestrator,"source":orchestrator_source}),
    )?;
    let node_agent = NodeAgentActor::new(
        stage::NodeId(config.logical_node_id),
        orchestrator,
        Some(*reports.addr()),
    );
    let node_actor = match stack.runtime.spawn(node_agent) {
        Ok(actor) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "node_agent",
                "ready",
                json!({"node_actor":actor,"source":"generated"}),
            )?;
            actor
        }
        Err(error) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "node_agent",
                "failed",
                json!({"error":error.to_string(),"source":"generated"}),
            )?;
            return Err(format!("spawn node agent: {error}"));
        }
    };
    stack.register_local_actor(driver.register_actor(node_actor, 1));
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "node_actor_registration",
        "ready",
        json!({"node_actor":node_actor,"network_reachable":true}),
    )?;

    emit_stdio_node_event(
        &config,
        NODE_WORKER_CHANNEL,
        "worker_process",
        "started",
        json!({
            "program":"python3",
            "script":&config.worker_script,
            "device":&config.device,
            "stdin":"piped",
            "stdout":"piped",
            "stderr":"piped",
        }),
    )?;
    let mut worker = match TinygradWorker::spawn(&config) {
        Ok(worker) => worker,
        Err(error) => {
            emit_stdio_node_event(
                &config,
                NODE_WORKER_CHANNEL,
                "worker_process",
                "failed",
                json!({"error":error}),
            )?;
            return Err(error);
        }
    };
    spawn_host_cpu_sampler(
        tokio.handle().clone(),
        datastream.event_sink(),
        vec![std::process::id(), worker.pid()],
    );
    emit_stdio_node_event(
        &config,
        NODE_WORKER_CHANNEL,
        "worker_initialize",
        "started",
        json!({"command":"InitializeWorker","helper_abi_version":1,"device":&config.device}),
    )?;
    let mut initial_pump = || {};
    match worker.initialize(&config.device, &config, &mut datastream, &mut initial_pump) {
        Ok(()) => emit_stdio_node_event(
            &config,
            NODE_WORKER_CHANNEL,
            "worker_initialize",
            "ready",
            json!({"worker_event_type":"WorkerReady"}),
        )?,
        Err(error) => {
            emit_stdio_node_event(
                &config,
                NODE_WORKER_CHANNEL,
                "worker_initialize",
                "failed",
                json!({"error":error}),
            )?;
            return Err(error);
        }
    }
    let mut pending_runtime_ready =
        PendingRuntimeReady::new(&config, driver.endpoint_addr(), node_actor);

    let ready = json!({
        "type":"ready",
        "role":"node",
        "endpoint": driver.endpoint_addr(),
        "node_actor": node_actor,
        "logical_node_id": config.logical_node_id,
        "stage_index": config.stage_index,
    });
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "runtime_ready_local",
        "ready",
        json!({
            "endpoint":driver.endpoint_addr(),
            "node_actor":node_actor,
            "logical_node_id":config.logical_node_id,
            "stage_index":config.stage_index,
            "readiness_id":pending_runtime_ready.readiness_id,
        }),
    )?;

    if let Some(prompt) = &config.self_test_prompt {
        run_self_test(
            &mut worker,
            &config,
            prompt,
            &mut datastream,
            &mut driver,
            &stack,
        )?;
    }

    let shutdown_rx = spawn_stdin_shutdown_listener();
    emit_node_event(
        &mut datastream,
        &config,
        NODE_RUNTIME_CHANNEL,
        "stdin_shutdown_listener",
        "ready",
        json!({"command":"shutdown"}),
    );
    emit_node_event(
        &mut datastream,
        &config,
        NODE_RUNTIME_CHANNEL,
        "main_loop",
        "started",
        json!({
            "poll_interval_ms":PUMP_INTERVAL.as_millis(),
            "checks":["network","datastream","node_reports","stdin_shutdown","worker_health"],
        }),
    );
    loop {
        pump_network(&mut driver, &stack);
        drain_debug_join_commands(&mut debug_join_rx, &mut driver, &config, &mut datastream);
        datastream.tick();
        worker.drain_stderr(&config, &mut datastream);
        while let Some(report) = reports.try_recv() {
            match handle_node_report(
                report,
                &config,
                &stack,
                &mut driver,
                node_actor,
                &mut worker,
                &mut datastream,
            )? {
                NodeReportOutcome::None => {}
                NodeReportOutcome::RuntimeReadyAck {
                    run_id,
                    node_id,
                    stage_index,
                    readiness_id,
                } => {
                    if pending_runtime_ready.observe_ack(run_id, node_id, stage_index, readiness_id)
                    {
                        emit_node_event(
                            &mut datastream,
                            &config,
                            NODE_BOOTSTRAP_CHANNEL,
                            "runtime_ready_ack",
                            "ready",
                            json!({
                                "readiness_id":readiness_id,
                                "attempts":pending_runtime_ready.attempts,
                                "endpoint":&pending_runtime_ready.endpoint,
                                "node_actor":pending_runtime_ready.node_actor,
                            }),
                        );
                        datastream.submit_text(ChannelId::new("mvp.node.ready"), ready.to_string());
                        emit_node_event(
                            &mut datastream,
                            &config,
                            NODE_BOOTSTRAP_CHANNEL,
                            "datastream_handoff",
                            "ready",
                            json!({"from":"runtime_ready_ack","to":"cluster_datastream","channel":"mvp.node.ready"}),
                        );
                    }
                }
            }
        }
        if !pending_runtime_ready.swim_logged && pending_runtime_ready.swim_ready(&stack) {
            emit_node_event(
                &mut datastream,
                &config,
                NODE_RUNTIME_CHANNEL,
                "coordinator_swim",
                "ready",
                json!({
                    "coordinator":pending_runtime_ready
                        .coordinator
                        .map(|node| format!("{node:?}"))
                        .unwrap_or_else(|| "standalone".to_owned()),
                    "readiness_id":pending_runtime_ready.readiness_id,
                }),
            );
            pending_runtime_ready.swim_logged = true;
        }
        if !pending_runtime_ready.acked && pending_runtime_ready.maybe_send(&stack, node_actor)? {
            emit_node_event(
                &mut datastream,
                &config,
                NODE_RUNTIME_CHANNEL,
                "runtime_ready_signal",
                "sent",
                json!({
                    "readiness_id":pending_runtime_ready.readiness_id,
                    "attempts":pending_runtime_ready.attempts,
                    "next_backoff_ms":pending_runtime_ready.backoff.as_millis(),
                }),
            );
        }
        if shutdown_rx.try_recv().is_ok() {
            emit_node_event(
                &mut datastream,
                &config,
                NODE_SHUTDOWN_CHANNEL,
                "shutdown",
                "started",
                json!({"source":"stdin","command":"shutdown"}),
            );
            let mut pump = || pump_network(&mut driver, &stack);
            match worker.shutdown(&config, &mut datastream, &mut pump) {
                Ok(()) => {
                    emit_node_event(
                        &mut datastream,
                        &config,
                        NODE_SHUTDOWN_CHANNEL,
                        "worker_shutdown",
                        "ready",
                        json!({"worker_event_type":"WorkerStopped"}),
                    );
                    emit_node_event(
                        &mut datastream,
                        &config,
                        NODE_SHUTDOWN_CHANNEL,
                        "node_exit",
                        "ready",
                        json!({"result":"ok"}),
                    );
                }
                Err(error) => emit_node_event(
                    &mut datastream,
                    &config,
                    NODE_SHUTDOWN_CHANNEL,
                    "worker_shutdown",
                    "failed",
                    json!({"error":error}),
                ),
            }
            return Ok(());
        }
        if let Some(status) = worker.try_wait()? {
            emit_node_event(
                &mut datastream,
                &config,
                NODE_SHUTDOWN_CHANNEL,
                "worker_process",
                "failed",
                json!({"exit_status":status.to_string()}),
            );
            let _ = stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::WorkerCrashed);
            return Err(format!("tinygrad helper exited with {status}"));
        }
        thread::sleep(PUMP_INTERVAL);
    }
}

fn pump_network(driver: &mut IrohDriver, stack: &DistributionRuntimeStack) {
    stack.tick_protocol_actors(Instant::now());
    driver.pump_inbound_to_actors();
    stack.pump_runtime_once();
    driver.drain_outbox(&stack.outbox);
}

fn node_datastream(
    config: &DeploymentConfig,
    stack: &DistributionRuntimeStack,
) -> DatastreamEmitter {
    let mut sinks: Vec<Box<dyn FrameSink>> = Vec::new();
    if let Some(actor) = config.datastream_sink_actor {
        let sink_addr = Arc::new(OnceLock::new());
        let _ = sink_addr.set(actor);
        sinks.push(Box::new(ClusterFrameSink::new(
            stack.runtime.clone(),
            sink_addr,
        )));
    }
    if let Some(path) = &config.datastream_frame_log {
        match JsonlFrameSink::open(path) {
            Ok(sink) => {
                sinks.push(Box::new(sink));
            }
            Err(_error) => {}
        }
    }
    if sinks.is_empty() {
        sinks.push(Box::new(NoopSink));
    }
    let sink: Box<dyn FrameSink> = if sinks.len() == 1 {
        sinks.pop().expect("one sink")
    } else {
        Box::new(TeeFrameSink { sinks })
    };
    DatastreamEmitter::new(
        EmitterConfig {
            node_hex: config.logical_node_id.to_string(),
            life: config.run_id,
            mux_capacity: 256,
        },
        sink,
    )
}

struct TeeFrameSink {
    sinks: Vec<Box<dyn FrameSink>>,
}

impl FrameSink for TeeFrameSink {
    fn ship(&mut self, stream: &datastream::StreamId, frame: &datastream::Frame) {
        for sink in &mut self.sinks {
            sink.ship(stream, frame);
        }
    }
}

struct JsonlFrameSink {
    file: File,
}

impl JsonlFrameSink {
    fn open(path: &str) -> std::io::Result<Self> {
        Ok(Self {
            file: OpenOptions::new().create(true).append(true).open(path)?,
        })
    }
}

impl FrameSink for JsonlFrameSink {
    fn ship(&mut self, stream: &datastream::StreamId, frame: &datastream::Frame) {
        let record = json!({
            "stream":stream.to_string(),
            "channel":frame.channel.as_str(),
            "position":frame.position.0,
            "payload":String::from_utf8_lossy(&frame.payload),
        });
        let _ = serde_json::to_writer(&mut self.file, &record);
        let _ = writeln!(self.file);
        let _ = self.file.flush();
    }
}

enum NodeReportOutcome {
    None,
    RuntimeReadyAck {
        run_id: u64,
        node_id: u64,
        stage_index: u32,
        readiness_id: u64,
    },
}

struct PendingRuntimeReady {
    run_id: u64,
    node_id: u64,
    stage_index: u32,
    endpoint: EndpointAddr,
    node_actor: ActorAddress,
    coordinator: Option<DistNodeId>,
    readiness_id: u64,
    attempts: u32,
    next_attempt_at: Instant,
    backoff: Duration,
    acked: bool,
    swim_logged: bool,
}

impl PendingRuntimeReady {
    fn new(config: &DeploymentConfig, endpoint: EndpointAddr, node_actor: ActorAddress) -> Self {
        Self {
            run_id: config.run_id,
            node_id: config.logical_node_id,
            stage_index: config.stage_index,
            endpoint,
            node_actor,
            coordinator: config
                .coordinator_endpoint
                .as_ref()
                .map(|endpoint| DistNodeId(*endpoint.id.as_bytes())),
            readiness_id: 1,
            attempts: 0,
            next_attempt_at: Instant::now(),
            backoff: RUNTIME_READY_RETRY_INITIAL,
            acked: false,
            swim_logged: false,
        }
    }

    fn swim_ready(&self, stack: &DistributionRuntimeStack) -> bool {
        let Some(coordinator) = self.coordinator else {
            return true;
        };
        stack.member_state(coordinator) == Some(MemberState::Alive)
    }

    fn observe_ack(
        &mut self,
        run_id: u64,
        node_id: u64,
        stage_index: u32,
        readiness_id: u64,
    ) -> bool {
        if self.acked
            || self.run_id != run_id
            || self.node_id != node_id
            || self.stage_index != stage_index
            || self.readiness_id != readiness_id
        {
            return false;
        }
        self.acked = true;
        true
    }

    fn maybe_send(
        &mut self,
        stack: &DistributionRuntimeStack,
        node_actor: ActorAddress,
    ) -> Result<bool, String> {
        if self.acked || !self.swim_ready(stack) {
            return Ok(false);
        }
        let now = Instant::now();
        if now < self.next_attempt_at {
            return Ok(false);
        }
        stack
            .runtime
            .send_to(
                node_actor,
                NodeAgentMsg::RuntimeLoaded {
                    run_id: self.run_id,
                    node_id: self.node_id,
                    stage_index: self.stage_index,
                    endpoint: self.endpoint.clone(),
                    node_actor: self.node_actor,
                    readiness_id: self.readiness_id,
                },
            )
            .map_err(|error| format!("signal runtime loaded: {error}"))?;
        self.attempts = self.attempts.saturating_add(1);
        self.next_attempt_at = now + self.backoff;
        self.backoff = self
            .backoff
            .checked_mul(2)
            .unwrap_or(RUNTIME_READY_RETRY_MAX)
            .min(RUNTIME_READY_RETRY_MAX);
        Ok(true)
    }
}

fn handle_node_report(
    report: NodeAgentReport,
    config: &DeploymentConfig,
    stack: &DistributionRuntimeStack,
    driver: &mut IrohDriver,
    node_actor: ActorAddress,
    worker: &mut TinygradWorker,
    datastream: &mut DatastreamEmitter,
) -> Result<NodeReportOutcome, String> {
    let kind = match &report {
        NodeAgentReport::Command(_) => "Command",
        NodeAgentReport::Lifecycle(_) => "Lifecycle",
        NodeAgentReport::PromptRequested { .. } => "PromptRequested",
        NodeAgentReport::RuntimeReadyAck { .. } => "RuntimeReadyAck",
        NodeAgentReport::Snapshot { .. } => "Snapshot",
    };
    emit_node_event(
        datastream,
        config,
        NODE_RUNTIME_CHANNEL,
        "node_report",
        "observed",
        json!({"kind":kind}),
    );
    match report {
        NodeAgentReport::Command(command) => {
            handle_stage_command(
                command, config, stack, driver, node_actor, worker, datastream,
            )?;
            Ok(NodeReportOutcome::None)
        }
        NodeAgentReport::Lifecycle(event) => {
            let event = format!("{event:?}");
            datastream.submit_text(
                ChannelId::new("mvp.node.lifecycle"),
                json!({"type":"node_lifecycle","event":event}).to_string(),
            );
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "lifecycle",
                "observed",
                json!({"event":event}),
            );
            Ok(NodeReportOutcome::None)
        }
        NodeAgentReport::PromptRequested {
            request_id,
            prompt,
            max_tokens,
            reply_to,
        } => {
            handle_prompt_request(
                request_id, prompt, max_tokens, reply_to, config, stack, driver, worker, datastream,
            )?;
            Ok(NodeReportOutcome::None)
        }
        NodeAgentReport::RuntimeReadyAck {
            run_id,
            node_id,
            stage_index,
            readiness_id,
        } => Ok(NodeReportOutcome::RuntimeReadyAck {
            run_id,
            node_id,
            stage_index,
            readiness_id,
        }),
        NodeAgentReport::Snapshot { .. } => Ok(NodeReportOutcome::None),
    }
}

fn handle_prompt_request(
    request_id: u64,
    prompt: String,
    max_tokens: u32,
    reply_to: ActorAddress,
    config: &DeploymentConfig,
    stack: &DistributionRuntimeStack,
    driver: &mut IrohDriver,
    worker: &mut TinygradWorker,
    datastream: &mut DatastreamEmitter,
) -> Result<(), String> {
    let started = Instant::now();
    emit_node_event(
        datastream,
        config,
        NODE_PROMPT_CHANNEL,
        "prompt_requested",
        "started",
        json!({"request_id":request_id,"max_tokens":max_tokens,"reply_to":reply_to,"prompt_bytes":prompt.len()}),
    );
    emit_node_event(
        datastream,
        config,
        NODE_PROMPT_CHANNEL,
        "infer_prompt",
        "started",
        json!({"request_id":request_id,"command":"InferPrompt","max_tokens":max_tokens}),
    );
    let mut pump = || pump_network(driver, stack);
    match worker.infer_prompt(
        request_id, &prompt, max_tokens, config, datastream, &mut pump,
    ) {
        Ok(result) => {
            let text = result
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let text_bytes = text.len();
            let worker_result_payload_bytes = result.to_string().len();
            let prompt_tokens = result
                .get("prompt_tokens")
                .and_then(Value::as_array)
                .map_or(0, |tokens| tokens.len() as u32);
            let tokens_generated = result
                .get("generated_tokens")
                .and_then(Value::as_array)
                .map_or(0, |tokens| tokens.len() as u32);
            let elapsed_ms = result
                .get("elapsed_ms")
                .and_then(Value::as_u64)
                .unwrap_or_else(|| started.elapsed().as_millis() as u64);
            emit_node_event(
                datastream,
                config,
                NODE_PROMPT_CHANNEL,
                "infer_prompt",
                "ready",
                json!({"request_id":request_id,"worker_event_type":"PromptCompleted","prompt_tokens":prompt_tokens,"tokens_generated":tokens_generated,"elapsed_ms":elapsed_ms,"text_bytes":text_bytes,"worker_result_payload_bytes":worker_result_payload_bytes}),
            );
            if !text.is_empty() {
                match stack.runtime.send_to(
                    reply_to,
                    PromptEvent::TextDelta {
                        request_id,
                        text: text.clone(),
                    },
                ) {
                    Ok(()) => emit_node_event(
                        datastream,
                        config,
                        NODE_PROMPT_CHANNEL,
                        "prompt_response",
                        "ready",
                        json!({"request_id":request_id,"event":"TextDelta","bytes":text_bytes,"reply_to":reply_to}),
                    ),
                    Err(error) => {
                        emit_node_event(
                            datastream,
                            config,
                            NODE_PROMPT_CHANNEL,
                            "prompt_response",
                            "failed",
                            json!({"request_id":request_id,"event":"TextDelta","error":error.to_string()}),
                        );
                        return Err(format!("send prompt text delta: {error}"));
                    }
                }
            }
            match stack.runtime.send_to(
                reply_to,
                PromptEvent::Done {
                    request_id,
                    final_text: text,
                    tokens_generated,
                    elapsed_ms,
                },
            ) {
                Ok(()) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_PROMPT_CHANNEL,
                        "prompt_response",
                        "ready",
                        json!({"request_id":request_id,"event":"Done","tokens_generated":tokens_generated,"elapsed_ms":elapsed_ms,"final_text_bytes":text_bytes,"reply_to":reply_to}),
                    );
                    Ok(())
                }
                Err(error) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_PROMPT_CHANNEL,
                        "prompt_response",
                        "failed",
                        json!({"request_id":request_id,"event":"Done","error":error.to_string()}),
                    );
                    Err(format!("send prompt done: {error}"))
                }
            }
        }
        Err(error) => {
            emit_node_event(
                datastream,
                config,
                NODE_PROMPT_CHANNEL,
                "infer_prompt",
                "failed",
                json!({"request_id":request_id,"error":error}),
            );
            match stack.runtime.send_to(
                reply_to,
                PromptEvent::Fault {
                    request_id,
                    error: error.clone(),
                },
            ) {
                Ok(()) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_PROMPT_CHANNEL,
                        "prompt_response",
                        "ready",
                        json!({"request_id":request_id,"event":"Fault","reply_to":reply_to}),
                    );
                    Ok(())
                }
                Err(send_error) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_PROMPT_CHANNEL,
                        "prompt_response",
                        "failed",
                        json!({"request_id":request_id,"event":"Fault","error":send_error.to_string()}),
                    );
                    Err(format!("send prompt fault: {send_error}"))
                }
            }
        }
    }
}

fn handle_stage_command(
    command: StageCommandWire,
    config: &DeploymentConfig,
    stack: &DistributionRuntimeStack,
    driver: &mut IrohDriver,
    node_actor: ActorAddress,
    worker: &mut TinygradWorker,
    datastream: &mut DatastreamEmitter,
) -> Result<(), String> {
    match command {
        StageCommandWire::ConfigureWorkerRole {
            run_id,
            stage_index,
            layer_start,
            layer_end_exclusive,
        } => {
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "configure_worker_role",
                "started",
                json!({"run_id":run_id,"stage_index":stage_index,"layer_range":{"start":layer_start,"end_exclusive":layer_end_exclusive}}),
            );
            let mut pump = || pump_network(driver, stack);
            match worker.configure_role(
                run_id,
                stage_index,
                layer_start,
                layer_end_exclusive,
                config,
                datastream,
                &mut pump,
            ) {
                Ok(()) => emit_node_event(
                    datastream,
                    config,
                    NODE_STAGE_CHANNEL,
                    "configure_worker_role",
                    "ready",
                    json!({"worker_event_type":"RoleConfigured"}),
                ),
                Err(error) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_STAGE_CHANNEL,
                        "configure_worker_role",
                        "failed",
                        json!({"error":error}),
                    );
                    return Err(error);
                }
            }
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::MarkWorkerReady)
                .map_err(|e| format!("mark worker ready: {e}"))
        }
        StageCommandWire::LoadWeights {
            model_id,
            gguf_source,
            tokenizer,
            layer_start,
            layer_end_exclusive,
        } => {
            let gguf_source_kind = match &gguf_source {
                GgufSource::LocalPath(_) => "local_path",
                GgufSource::HuggingFaceGguf { .. } => "huggingface",
            };
            let tokenizer_kind = match &tokenizer {
                TokenizerSource::EmbeddedGguf => "gguf",
                TokenizerSource::LocalPath(_) => "local_path",
            };
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "load_weights",
                "started",
                json!({"model_id":&model_id,"gguf_source":gguf_source_kind,"tokenizer":tokenizer_kind,"layer_range":{"start":layer_start,"end_exclusive":layer_end_exclusive}}),
            );
            let mut pump = || pump_network(driver, stack);
            match worker.load_weights(
                model_id.clone(),
                gguf_source,
                tokenizer,
                layer_start,
                layer_end_exclusive,
                config,
                datastream,
                &mut pump,
            ) {
                Ok(()) => emit_node_event(
                    datastream,
                    config,
                    NODE_STAGE_CHANNEL,
                    "load_weights",
                    "ready",
                    json!({"worker_event_type":"WeightsLoaded","model_id":model_id}),
                ),
                Err(error) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_STAGE_CHANNEL,
                        "load_weights",
                        "failed",
                        json!({"error":error}),
                    );
                    return Err(error);
                }
            }
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::MarkWeightsReady)
                .map_err(|e| format!("mark weights ready: {e}"))
        }
        StageCommandWire::StopLocalEdges { run_id } => {
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::LocalEdgesStopped { run_id })
                .map_err(|e| format!("mark local edges stopped: {e}"))?;
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::WorkerRingsQuiesced { run_id })
                .map_err(|e| format!("mark worker rings quiesced: {e}"))?;
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "local_edges",
                "ready",
                json!({"run_id":run_id,"sent":["LocalEdgesStopped","WorkerRingsQuiesced"]}),
            );
            Ok(())
        }
        StageCommandWire::ReleaseRunDeviceObjects { run_id } => {
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::DeviceObjectsReleased { run_id })
                .map_err(|e| format!("mark device objects released: {e}"))?;
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::WorkerRoleReset { run_id })
                .map_err(|e| format!("mark worker role reset: {e}"))?;
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "device_objects",
                "ready",
                json!({"run_id":run_id,"sent":["DeviceObjectsReleased","WorkerRoleReset"]}),
            );
            Ok(())
        }
        StageCommandWire::EstablishInboundEdge { edge_id } => {
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::MarkInboundEdgeReady { edge_id })
                .map_err(|e| format!("mark inbound edge ready: {e}"))?;
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "inbound_edge",
                "ready",
                json!({"edge_id":format!("{edge_id:?}")}),
            );
            Ok(())
        }
        StageCommandWire::EstablishOutboundEdge { edge_id } => {
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::MarkOutboundEdgeReady { edge_id })
                .map_err(|e| format!("mark outbound edge ready: {e}"))?;
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "outbound_edge",
                "ready",
                json!({"edge_id":format!("{edge_id:?}")}),
            );
            Ok(())
        }
        StageCommandWire::RewireEdge { .. } => {
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "rewire_edge",
                "skipped",
                json!({"reason":"not implemented in mvp-worker-node image path"}),
            );
            Ok(())
        }
        StageCommandWire::ReleaseInputHandle { .. } => {
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "release_input_handle",
                "skipped",
                json!({"reason":"not implemented in mvp-worker-node image path"}),
            );
            Ok(())
        }
        StageCommandWire::ExecuteStep { .. } => {
            let error = "mvp-worker-node image path does not carry ring payload commands yet";
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "execute_step",
                "failed",
                json!({"error":error}),
            );
            Err(format!("{error}: {command:?}"))
        }
    }
}

fn run_self_test(
    worker: &mut TinygradWorker,
    config: &DeploymentConfig,
    prompt: &str,
    datastream: &mut DatastreamEmitter,
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
) -> Result<(), String> {
    emit_node_event(
        datastream,
        config,
        NODE_RUNTIME_CHANNEL,
        "self_test",
        "started",
        json!({"prompt_bytes":prompt.len()}),
    );
    let mut pump = || pump_network(driver, stack);
    worker.configure_role(
        config.run_id,
        config.stage_index,
        0,
        config.self_test_layer_end,
        config,
        datastream,
        &mut pump,
    )?;
    worker.load_weights(
        config.model_id.clone(),
        config.gguf_source.clone(),
        config.tokenizer.clone(),
        0,
        config.self_test_layer_end,
        config,
        datastream,
        &mut pump,
    )?;
    let result = worker.infer_prompt(
        0,
        prompt,
        config.self_test_max_tokens,
        config,
        datastream,
        &mut pump,
    )?;
    let record = json!({"type":"self_test_completed","prompt_bytes":prompt.len(),"result":result});
    datastream.submit_text(ChannelId::new("mvp.node.self_test"), record.to_string());
    emit_node_event(
        datastream,
        config,
        NODE_RUNTIME_CHANNEL,
        "self_test",
        "ready",
        json!({"prompt_bytes":prompt.len()}),
    );
    Ok(())
}

#[derive(Clone)]
struct DeploymentConfig {
    run_id: u64,
    logical_node_id: u64,
    stage_index: u32,
    coordinator_endpoint: Option<EndpointAddr>,
    orchestrator_actor: Option<ActorAddress>,
    datastream_sink_actor: Option<ActorAddress>,
    datastream_frame_log: Option<String>,
    debug_join_socket: Option<String>,
    relay_mode: iroh::RelayMode,
    worker_script: String,
    device: String,
    model_id: String,
    gguf_source: GgufSource,
    tokenizer: TokenizerSource,
    self_test_prompt: Option<String>,
    self_test_layer_end: u32,
    self_test_max_tokens: u32,
    arena_bytes: u64,
    arena_alignment: u64,
}

impl DeploymentConfig {
    fn from_env() -> Result<Self, String> {
        let run_id = env_u64("MVP_RUN_ID", 1)?;
        let logical_node_id = env_u64("MVP_LOGICAL_NODE_ID", 1)?;
        let relay = relay_runtime_config_from_env(run_id)?;
        let debug_join_socket = match env_optional("MVP_DEBUG_JOIN_SOCKET").as_deref() {
            Some("disabled") => None,
            Some(path) => Some(path.to_owned()),
            None => Some(
                std::env::temp_dir()
                    .join(format!(
                        "mvp-node-debug-join-{run_id}-{logical_node_id}.sock"
                    ))
                    .to_string_lossy()
                    .into_owned(),
            ),
        };
        Ok(Self {
            run_id,
            logical_node_id,
            stage_index: env_u32("MVP_STAGE_INDEX", 0)?,
            coordinator_endpoint: env_json("MVP_COORDINATOR_ENDPOINT")?,
            orchestrator_actor: env_json("MVP_ORCHESTRATOR_ACTOR")?,
            datastream_sink_actor: env_json("MVP_DATASTREAM_SINK_ACTOR")?,
            datastream_frame_log: env_optional("MVP_DATASTREAM_FRAME_LOG"),
            debug_join_socket,
            relay_mode: relay.mode,
            worker_script: env_string("MVP_TINYGRAD_WORKER", DEFAULT_WORKER_SCRIPT),
            device: env_string("DEV", DEFAULT_DEVICE),
            model_id: env_string("MVP_MODEL_ID", DEFAULT_MODEL_ID),
            gguf_source: gguf_source_from_env(),
            tokenizer: tokenizer_from_env(),
            self_test_prompt: env_optional("MVP_NODE_SELF_TEST_PROMPT"),
            self_test_layer_end: env_u32("MVP_SELF_TEST_LAYER_END", 16)?,
            self_test_max_tokens: env_u32("MVP_SELF_TEST_MAX_TOKENS", 1)?,
            arena_bytes: env_u64("MVP_ARENA_BYTES", DEFAULT_ARENA_BYTES)?,
            arena_alignment: env_u64("MVP_ARENA_ALIGNMENT", DEFAULT_ARENA_ALIGNMENT)?,
        })
    }

    fn datastream_sink_detail(&self) -> Value {
        let sink = match (
            self.datastream_sink_actor.is_some(),
            self.datastream_frame_log.is_some(),
        ) {
            (true, true) => "tee",
            (true, false) => "cluster_actor",
            (false, true) => "frame_log",
            (false, false) => "noop",
        };
        json!({
            "sink":sink,
            "cluster_actor":self.datastream_sink_actor,
            "frame_log":self.datastream_frame_log,
        })
    }
}

struct TinygradWorker {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr_rx: Receiver<String>,
}

impl TinygradWorker {
    fn spawn(config: &DeploymentConfig) -> Result<Self, String> {
        let mut child = Command::new("python3")
            .arg(&config.worker_script)
            .env("DEV", &config.device)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn tinygrad helper {}: {e}", config.worker_script))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "tinygrad helper stdin missing".to_owned())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "tinygrad helper stdout missing".to_owned())?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| "tinygrad helper stderr missing".to_owned())?;
        let (stderr_tx, stderr_rx) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if stderr_tx.send(line).is_err() {
                    break;
                }
            }
        });
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr_rx,
        })
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn initialize(
        &mut self,
        device: &str,
        config: &DeploymentConfig,
        datastream: &mut DatastreamEmitter,
        pump: &mut dyn FnMut(),
    ) -> Result<(), String> {
        self.command(
            json!({"type":"InitializeWorker","helper_abi_version":1,"backend":{"device":device}}),
            "WorkerReady",
            config,
            datastream,
            ChannelId::new("mvp.worker.initialize"),
            pump,
        )
        .map(|_| ())
    }

    fn configure_role(
        &mut self,
        run_id: u64,
        stage_index: u32,
        layer_start: u32,
        layer_end_exclusive: u32,
        config: &DeploymentConfig,
        datastream: &mut DatastreamEmitter,
        pump: &mut dyn FnMut(),
    ) -> Result<(), String> {
        self.command(
            json!({
                "type":"ConfigureRole",
                "role_id":stage_index + 1,
                "config":{
                    "run_id":run_id,
                    "stage_index":stage_index,
                    "layer_start":layer_start,
                    "layer_end_exclusive":layer_end_exclusive,
                }
            }),
            "RoleConfigured",
            config,
            datastream,
            ChannelId::new("mvp.worker.role"),
            pump,
        )
        .map(|_| ())
    }

    fn load_weights(
        &mut self,
        model_id: String,
        gguf_source: GgufSource,
        tokenizer: TokenizerSource,
        layer_start: u32,
        layer_end_exclusive: u32,
        config: &DeploymentConfig,
        datastream: &mut DatastreamEmitter,
        pump: &mut dyn FnMut(),
    ) -> Result<(), String> {
        self.command(
            json!({
                "type":"LoadWeights",
                "model_id":model_id,
                "gguf_source":gguf_source,
                "tokenizer":tokenizer,
                "layer_start":layer_start,
                "layer_end_exclusive":layer_end_exclusive,
            }),
            "WeightsLoaded",
            config,
            datastream,
            ChannelId::new("mvp.worker.weights"),
            pump,
        )
        .map(|_| ())
    }

    fn infer_prompt(
        &mut self,
        request_id: u64,
        prompt: &str,
        max_tokens: u32,
        config: &DeploymentConfig,
        datastream: &mut DatastreamEmitter,
        pump: &mut dyn FnMut(),
    ) -> Result<Value, String> {
        self.command(
            json!({"type":"InferPrompt","request_id":request_id,"prompt":prompt,"max_tokens":max_tokens}),
            "PromptCompleted",
            config,
            datastream,
            ChannelId::new("mvp.worker.prompt"),
            pump,
        )
    }

    fn shutdown(
        &mut self,
        config: &DeploymentConfig,
        datastream: &mut DatastreamEmitter,
        pump: &mut dyn FnMut(),
    ) -> Result<(), String> {
        self.command(
            json!({"type":"ShutdownWorker"}),
            "WorkerStopped",
            config,
            datastream,
            ChannelId::new("mvp.worker.shutdown"),
            pump,
        )
        .map(|_| ())
    }

    fn try_wait(&mut self) -> Result<Option<std::process::ExitStatus>, String> {
        self.child
            .try_wait()
            .map_err(|e| format!("poll tinygrad helper: {e}"))
    }

    fn drain_stderr(&mut self, config: &DeploymentConfig, datastream: &mut DatastreamEmitter) {
        let mut emitted = false;
        while let Ok(line) = self.stderr_rx.try_recv() {
            let payload =
                node_event_payload(config, "worker_stderr", "observed", json!({"line":line}));
            datastream.submit_text(ChannelId::new("mvp.worker.stderr"), payload.to_string());
            emitted = true;
        }
        if emitted {
            datastream.tick();
        }
    }

    fn command(
        &mut self,
        command: Value,
        expected: &str,
        config: &DeploymentConfig,
        datastream: &mut DatastreamEmitter,
        channel: ChannelId,
        pump: &mut dyn FnMut(),
    ) -> Result<Value, String> {
        let command_type = command
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let command_text = command.to_string();
        let command_bytes = command_text.len() + 1;
        emit_node_event(
            datastream,
            config,
            NODE_WORKER_CHANNEL,
            "worker_command_write",
            "started",
            json!({"command_type":command_type.as_str(),"expected_event_type":expected,"command_bytes":command_bytes}),
        );
        if let Err(error) = writeln!(self.stdin, "{command_text}") {
            emit_node_event(
                datastream,
                config,
                NODE_WORKER_CHANNEL,
                "worker_command_write",
                "failed",
                json!({"command_type":command_type.as_str(),"expected_event_type":expected,"command_bytes":command_bytes,"error":error.to_string()}),
            );
            return Err(format!("write helper command: {error}"));
        }
        if let Err(error) = self.stdin.flush() {
            emit_node_event(
                datastream,
                config,
                NODE_WORKER_CHANNEL,
                "worker_command_write",
                "failed",
                json!({"command_type":command_type.as_str(),"expected_event_type":expected,"command_bytes":command_bytes,"error":error.to_string()}),
            );
            return Err(format!("flush helper command: {error}"));
        }
        emit_node_event(
            datastream,
            config,
            NODE_WORKER_CHANNEL,
            "worker_command_write",
            "ready",
            json!({"command_type":command_type.as_str(),"expected_event_type":expected,"command_bytes":command_bytes}),
        );
        self.expect_event(expected, config, datastream, channel, pump)
    }

    fn expect_event(
        &mut self,
        expected: &str,
        config: &DeploymentConfig,
        datastream: &mut DatastreamEmitter,
        channel: ChannelId,
        pump: &mut dyn FnMut(),
    ) -> Result<Value, String> {
        loop {
            let mut line = String::new();
            emit_node_event(
                datastream,
                config,
                NODE_WORKER_CHANNEL,
                "worker_stdout_read",
                "started",
                json!({"expected_event_type":expected,"channel":channel.as_str()}),
            );
            let n = match self.stdout.read_line(&mut line) {
                Ok(n) => n,
                Err(error) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_WORKER_CHANNEL,
                        "worker_stdout_read",
                        "failed",
                        json!({"expected_event_type":expected,"channel":channel.as_str(),"error":error.to_string()}),
                    );
                    return Err(format!("read helper stdout: {error}"));
                }
            };
            self.drain_stderr(config, datastream);
            if n == 0 {
                emit_node_event(
                    datastream,
                    config,
                    NODE_WORKER_CHANNEL,
                    "worker_stdout_read",
                    "failed",
                    json!({"expected_event_type":expected,"channel":channel.as_str(),"line_bytes":0,"error":"stdout closed"}),
                );
                return Err(format!(
                    "tinygrad helper stdout closed while waiting for {expected}"
                ));
            }
            emit_node_event(
                datastream,
                config,
                NODE_WORKER_CHANNEL,
                "worker_stdout_read",
                "ready",
                json!({"expected_event_type":expected,"channel":channel.as_str(),"line_bytes":n}),
            );
            emit_node_event(
                datastream,
                config,
                NODE_WORKER_CHANNEL,
                "worker_stdout_parse",
                "started",
                json!({"expected_event_type":expected,"channel":channel.as_str(),"line_bytes":n}),
            );
            let value: Value = match serde_json::from_str(&line) {
                Ok(value) => value,
                Err(error) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_WORKER_CHANNEL,
                        "worker_stdout_parse",
                        "failed",
                        json!({"expected_event_type":expected,"channel":channel.as_str(),"line_bytes":n,"error":error.to_string()}),
                    );
                    return Err(format!("parse helper stdout {line:?}: {error}"));
                }
            };
            let worker_event_type = value
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            emit_node_event(
                datastream,
                config,
                NODE_WORKER_CHANNEL,
                "worker_stdout_parse",
                "ready",
                json!({"expected_event_type":expected,"channel":channel.as_str(),"line_bytes":n,"worker_event_type":worker_event_type}),
            );
            datastream.submit_text(channel.clone(), value.to_string());
            datastream.tick();
            pump();
            if value.get("type").and_then(Value::as_str) == Some(expected) {
                return Ok(value);
            }
            emit_node_event(
                datastream,
                config,
                NODE_WORKER_CHANNEL,
                "worker_event",
                "observed",
                json!({"command_waiting_for":expected,"worker_event_type":worker_event_type,"event":value}),
            );
        }
    }
}

impl Drop for TinygradWorker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_stdin_shutdown_listener() -> Receiver<()> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines().map_while(Result::ok) {
            if line.trim().eq_ignore_ascii_case("shutdown") {
                let _ = tx.send(());
                break;
            }
        }
    });
    rx
}

fn env_string(name: &str, default: &str) -> String {
    env_optional(name).unwrap_or_else(|| default.to_owned())
}

fn env_optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn env_u64(name: &str, default: u64) -> Result<u64, String> {
    match env_optional(name) {
        Some(value) => value
            .parse::<u64>()
            .map_err(|e| format!("invalid {name}={value:?}: {e}")),
        None => Ok(default),
    }
}

fn env_u32(name: &str, default: u32) -> Result<u32, String> {
    match env_optional(name) {
        Some(value) => value
            .parse::<u32>()
            .map_err(|e| format!("invalid {name}={value:?}: {e}")),
        None => Ok(default),
    }
}

fn env_json<T>(name: &str) -> Result<Option<T>, String>
where
    T: serde::de::DeserializeOwned,
{
    env_optional(name)
        .map(|value| serde_json::from_str(&value).map_err(|e| format!("invalid {name} JSON: {e}")))
        .transpose()
}

fn gguf_source_from_env() -> GgufSource {
    if let Some(path) = env_optional("MVP_GGUF_LOCAL_PATH") {
        return GgufSource::LocalPath(path);
    }
    GgufSource::HuggingFaceGguf {
        repo: env_string("MVP_GGUF_REPO", DEFAULT_HF_REPO),
        file: env_string("MVP_GGUF_FILE", DEFAULT_HF_FILE),
        revision: env_optional("MVP_GGUF_REVISION"),
    }
}

fn tokenizer_from_env() -> TokenizerSource {
    env_optional("MVP_TOKENIZER_LOCAL_PATH")
        .map(TokenizerSource::LocalPath)
        .unwrap_or(TokenizerSource::EmbeddedGguf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use distribution::swim::actor::MembershipChanged;
    use mvp_system::actors::orchestrator::OrchestratorMsg;

    fn endpoint(seed: u8) -> EndpointAddr {
        EndpointAddr::new(iroh::SecretKey::from_bytes(&[seed; 32]).public())
    }

    fn test_config(coordinator_endpoint: Option<EndpointAddr>) -> DeploymentConfig {
        DeploymentConfig {
            run_id: 7,
            logical_node_id: 11,
            stage_index: 3,
            coordinator_endpoint,
            orchestrator_actor: Some(ActorAddress::new_random()),
            datastream_sink_actor: None,
            datastream_frame_log: None,
            debug_join_socket: None,
            relay_mode: iroh::RelayMode::Disabled,
            worker_script: DEFAULT_WORKER_SCRIPT.to_owned(),
            device: DEFAULT_DEVICE.to_owned(),
            model_id: DEFAULT_MODEL_ID.to_owned(),
            gguf_source: GgufSource::LocalPath("/tmp/model.gguf".to_owned()),
            tokenizer: TokenizerSource::EmbeddedGguf,
            self_test_prompt: None,
            self_test_layer_end: 16,
            self_test_max_tokens: 1,
            arena_bytes: DEFAULT_ARENA_BYTES,
            arena_alignment: DEFAULT_ARENA_ALIGNMENT,
        }
    }

    fn test_stack() -> DistributionRuntimeStack {
        DistributionRuntimeStack::new(DistNodeId([1; 32]), DistributedNodeConfig::default())
    }

    #[test]
    fn debug_join_client_serializes_endpoint_from_stdin() {
        let secret = iroh::SecretKey::from_bytes(&[7; 32]);
        let endpoint = EndpointAddr::new(secret.public()).with_relay_url(
            "http://relay.example.com"
                .parse::<iroh::RelayUrl>()
                .unwrap(),
        );

        let line = debug_join_request_line(endpoint).expect("serialize debug join request");
        let request: DebugJoinRequestWire =
            serde_json::from_str(&line).expect("deserialize debug join request");

        match request {
            DebugJoinRequestWire::JoinEndpoint { endpoint } => {
                assert_eq!(
                    endpoint.relay_urls().next().map(ToString::to_string),
                    Some("http://relay.example.com/".to_owned())
                );
            }
        }
    }

    #[test]
    fn debug_join_listener_queues_join_endpoint() {
        let root = std::env::temp_dir().join(format!(
            "mvp-worker-debug-join-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        std::fs::create_dir(&root).expect("create debug join test temp dir");
        let socket_path = root.join("debug-join.sock");
        let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
        let mut commands = spawn_debug_join_listener(runtime.handle().clone(), socket_path.clone())
            .expect("spawn debug join listener");

        let secret = iroh::SecretKey::from_bytes(&[8; 32]);
        let endpoint = EndpointAddr::new(secret.public()).with_relay_url(
            "http://relay.example.com"
                .parse::<iroh::RelayUrl>()
                .unwrap(),
        );
        let request = DebugJoinRequestWire::JoinEndpoint { endpoint };
        let mut request_line =
            serde_json::to_string(&request).expect("serialize debug join request");
        request_line.push('\n');

        runtime.block_on(async {
            let mut stream = tokio::net::UnixStream::connect(&socket_path)
                .await
                .expect("connect to debug join listener");
            stream
                .write_all(request_line.as_bytes())
                .await
                .expect("write debug join request");
            stream.flush().await.expect("flush debug join request");

            let DebugJoinCommand::JoinEndpoint { endpoint, reply } =
                commands.recv().await.expect("receive debug join command");
            assert_eq!(
                endpoint.relay_urls().next().map(ToString::to_string),
                Some("http://relay.example.com/".to_owned())
            );
            let peer_node_id = endpoint.id.to_string();
            assert!(
                reply
                    .send(DebugJoinResponseWire::JoinQueued {
                        peer_node_id,
                        has_relay: true,
                        direct_addr_count: 0,
                    })
                    .is_ok()
            );

            let mut reader = tokio::io::BufReader::new(stream);
            let mut response_line = String::new();
            reader
                .read_line(&mut response_line)
                .await
                .expect("read debug join response");
            let response: DebugJoinResponseWire =
                serde_json::from_str(&response_line).expect("deserialize debug join response");
            match response {
                DebugJoinResponseWire::JoinQueued { has_relay, .. } => {
                    assert!(has_relay);
                }
                DebugJoinResponseWire::JoinRejected { error, detail } => {
                    panic!("debug join was rejected: {error}: {detail}");
                }
            }
        });

        drop(commands);
        let _ = std::fs::remove_file(&socket_path);
        std::fs::remove_dir(&root).expect("remove debug join test temp dir");
    }

    #[test]
    fn runtime_ready_retry_waits_for_swim() {
        let stack = test_stack();
        let node_actor = ActorAddress::new_random();
        let mut pending = PendingRuntimeReady::new(&test_config(None), endpoint(3), node_actor);
        pending.coordinator = Some(DistNodeId([2; 32]));

        assert_eq!(pending.maybe_send(&stack, node_actor), Ok(false));
        assert_eq!(pending.attempts, 0);
    }

    #[test]
    fn runtime_ready_retry_stops_after_matching_ack() {
        let stack = test_stack();
        let node_actor = ActorAddress::new_random();
        let mut pending = PendingRuntimeReady::new(&test_config(None), endpoint(4), node_actor);

        assert!(pending.observe_ack(
            pending.run_id,
            pending.node_id,
            pending.stage_index,
            pending.readiness_id,
        ));
        assert!(pending.acked);
        assert_eq!(pending.maybe_send(&stack, node_actor), Ok(false));
    }

    #[test]
    fn runtime_ready_retry_backoff_caps() {
        let stack = test_stack();
        let orchestrator_inbox = stack
            .runtime
            .new_inbox::<OrchestratorMsg>()
            .expect("orchestrator inbox");
        let node_actor = stack
            .runtime
            .spawn(NodeAgentActor::new(
                stage::NodeId(11),
                *orchestrator_inbox.addr(),
                None,
            ))
            .expect("spawn node agent");
        let coordinator = DistNodeId([2; 32]);
        stack
            .runtime
            .send_to(
                stack.actors.membership_fanout,
                MembershipChanged {
                    node_id: coordinator,
                    state: MemberState::Alive,
                    incarnation: 1,
                },
            )
            .expect("send membership change");
        stack.pump_runtime_once();

        let mut pending = PendingRuntimeReady::new(&test_config(None), endpoint(5), node_actor);
        pending.coordinator = Some(coordinator);
        for expected_attempts in 1..=4 {
            pending.next_attempt_at = Instant::now();
            assert!(
                pending
                    .maybe_send(&stack, node_actor)
                    .expect("runtime ready send")
            );
            assert_eq!(pending.attempts, expected_attempts);
            assert!(pending.backoff <= RUNTIME_READY_RETRY_MAX);
        }
    }
}
