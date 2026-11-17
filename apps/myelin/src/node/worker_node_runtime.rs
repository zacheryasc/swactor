//! Worker-node runtime behavior behind the `myelin::node` boundary.
//!
//! The `myelin-worker` binary constructs the engine and grants task authority.
//! This module prepares runtime parts and owns reusable node-local behavior:
//! helper commands, telemetry, debug join, staging, and worker coordination.

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, ExitCode, Stdio};
use std::sync::{
    Arc,
    mpsc::{self, Receiver, Sender, TryRecvError},
};

#[cfg(test)]
use telemetry::decode_record_value;
#[cfg(test)]
use telemetry::frame::TelemetryEvent;
use telemetry::{
    ChannelContent, ChannelId, Lifetime, NodeId, Record, StreamDescriptor, StreamId, StreamOrigin,
    TelemetryEndpoint, TelemetryProducer, encode_record,
};

use crate::codecs::register_myelin_actor_codecs;
use crate::contextual_process::{
    ContextualProcessController, ContextualProgramMaterializer, MyelinChildRouteRegistrar,
    MyelinContextualProcessConfig, build_contextual_process_spawner,
};
use crate::data_namespace::install_namespace_client;
use crate::gguf_shard::{StageShardPlan, materialize_stage_shard_http, validate_stage_shard_cache};
use crate::node::prompt_wire::{PromptEvent, TokenizerEvent};
use crate::node::telemetry::allocate_telemetry_lifetime;
use crate::node_actor::{
    NodeAgentActor, NodeAgentMsg, NodeAgentReport, StageCommandWire, StageInboundEdgeWire,
    StageObjectSpecWire, StageOutboundEdgeWire,
};
use crate::orchestration::actor::OrchestratorMsg;
use crate::orchestration::distribution_stack::DistributionRuntimeStack;
use crate::orchestration::manual_control::{
    CONTROL_REGISTRY_NAME, ManualControlMsg, ManualControlReply, RejoinHello, SELECTED_OFFER_ID_ENV,
};
use crate::orchestration::provider_adapters::relay::relay_runtime_config_from_env;
use crate::run_plan::{GgufSource, TokenizerSource};
use crate::staging::control as stage;
use data_plane::arena;
use data_plane::blob_transfer::{BlobTransferReceiver, BlobTransferSender};
use data_plane::edge_lifecycle as edge;
use data_plane::edge_runtime;
use data_plane::host::HostRouteRegistrar;
use data_plane::object_record as ingress;
use distribution::node::DistributedNodeConfig;
use distribution::telemetry::TransportMembershipTransition;
use distribution::types::{MemberState, NodeId as DistNodeId};
use iroh::{EndpointAddr, RelayMode};
use iroh_driver::{
    EDGE_ALPN, IrohBlobTransferReceiver, IrohBlobTransferSender, IrohDriver, IrohDriverConfig,
    IrohEdgePort, PeerConnector, TELEMETRY_ALPN, spawn_pull_server,
};
use iroh_driver::{EndpointAddrMask, MVP_IROH_ENDPOINT_ADDR_MASK_ENV, advertised_endpoint};
use myelin_control_contract::DeploymentIdentity;
use parking_lot::Mutex;
use serde_json::{Value, json};
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, ExternalSender, Inbox, Runtime, RuntimeParts};
use swactor::stats::{ActorSnapshot, StatsHook, StatsSnapshotKind};
use swactor_engine::{ActorCompletion, BlockingWorkSender, Engine, EngineHandle, TaskSpawner};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

const DEFAULT_WORKER_SCRIPT: &str = "/usr/local/share/myelin/tinygrad_worker.py";
const DEFAULT_DEVICE: &str = "CUDA";
const DEFAULT_HF_REPO: &str = "bartowski/Llama-3.2-1B-Instruct-GGUF";
const DEFAULT_HF_FILE: &str = "Llama-3.2-1B-Instruct-Q4_K_M.gguf";
const DEFAULT_MODEL_ID: &str = "llama-3.2-1b-instruct-q4";
const DEFAULT_ARENA_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_ARENA_ALIGNMENT: u64 = 64;
const NODE_BOOTSTRAP_CHANNEL: &str = "myelin.node.bootstrap";
const NODE_RUNTIME_CHANNEL: &str = "myelin.node.runtime";
const NODE_STAGE_CHANNEL: &str = "myelin.node.stage";
const NODE_WORKER_CHANNEL: &str = "myelin.node.worker";
const NODE_PROMPT_CHANNEL: &str = "myelin.node.prompt";
const NODE_SHUTDOWN_CHANNEL: &str = "myelin.node.shutdown";
const NODE_SAMPLER_CHANNEL: &str = "myelin.node.sampler";

fn node_event_payload(
    config: &DeploymentConfig,
    phase: &str,
    status: &str,
    detail: Value,
) -> Value {
    json!({
        "type":"NodeEvent",
        "event_type":"NodeEvent",
        "event_name":phase,
        "phase":phase,
        "status":status,
        "run_id":config.run_id,
        "node_id":config.logical_node_id,
        "stage_index":config.stage_index,
        "detail":detail,
    })
}

fn emit_stdio_telemetry_frame(channel: &str, payload: &Value) -> Result<(), String> {
    println!(
        "{}",
        json!({
            "myelin_stdio_event":1,
            "kind":"telemetry_frame",
            "channel":channel,
            "payload":payload,
        })
    );
    std::io::stdout()
        .flush()
        .map_err(|e| format!("flush stdio telemetry frame: {e}"))
}

fn emit_stdio_node_event(
    config: &DeploymentConfig,
    channel: &str,
    phase: &str,
    status: &str,
    detail: Value,
) -> Result<(), String> {
    let payload = node_event_payload(config, phase, status, detail);
    emit_stdio_telemetry_frame(channel, &payload)
}

fn emit_node_event(
    telemetry: &mut NodeTelemetry,
    config: &DeploymentConfig,
    channel: &str,
    phase: &str,
    status: &str,
    detail: Value,
) {
    let channel = telemetry.channel_by_name(channel);
    let payload = node_event_payload(config, phase, status, detail);
    telemetry.submit_value(channel, &payload);
    telemetry.tick();
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct DebugActorSnapshot {
    address: String,
    mailbox_depth: usize,
    actor_type: String,
    poisoned: bool,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
struct DebugRuntimeStats {
    actors: Vec<DebugActorSnapshot>,
}

#[derive(Clone, Default)]
struct RuntimeStatsInspector {
    latest: Arc<Mutex<BTreeMap<usize, Vec<DebugActorSnapshot>>>>,
}

impl RuntimeStatsInspector {
    fn snapshot(&self) -> DebugRuntimeStats {
        let mut actors = self
            .latest
            .lock()
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        actors.sort_by(|left, right| left.address.cmp(&right.address));
        DebugRuntimeStats { actors }
    }
}

struct InspectableStatsHook {
    inner: Arc<dyn StatsHook>,
    inspector: RuntimeStatsInspector,
}

impl StatsHook for InspectableStatsHook {
    fn on_snapshot(&self, worker_id: usize, snapshots: &[ActorSnapshot], kind: StatsSnapshotKind) {
        self.inspector.latest.lock().insert(
            worker_id,
            snapshots
                .iter()
                .map(|snapshot| DebugActorSnapshot {
                    address: snapshot.address.to_full_hex(),
                    mailbox_depth: snapshot.mailbox_depth,
                    actor_type: snapshot.actor_type.unwrap_or("<unknown>").to_owned(),
                    poisoned: snapshot.poisoned,
                })
                .collect(),
        );
        self.inner.on_snapshot(worker_id, snapshots, kind);
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "type")]
enum DebugJoinRequestWire {
    JoinEndpoint {
        endpoint: EndpointAddr,
        #[serde(default)]
        orchestrator_actor: Option<ActorAddress>,
    },
    RuntimeStats,
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
    RuntimeStats {
        stats: DebugRuntimeStats,
    },
}

enum DebugJoinCommand {
    JoinEndpoint {
        endpoint: EndpointAddr,
        orchestrator_actor: Option<ActorAddress>,
        reply: tokio::sync::oneshot::Sender<DebugJoinResponseWire>,
    },
    RuntimeStats {
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
                    eprintln!("myelin-worker debug-join: serialize response: {error}");
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
            eprintln!("myelin-worker debug-join: {error}");
            ExitCode::from(2)
        }
        Err(DebugJoinClientError::Runtime(error)) => {
            eprintln!("myelin-worker debug-join: {error}");
            ExitCode::from(1)
        }
    }
}

fn run_debug_join_client(args: Vec<String>) -> Result<DebugJoinResponseWire, DebugJoinClientError> {
    let mut socket = None;
    let mut endpoint_json = None;
    let mut orchestrator_actor_json = None;
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
            "--orchestrator-actor-json" => {
                orchestrator_actor_json = Some(iter.next().ok_or_else(|| {
                    DebugJoinClientError::Cli("--orchestrator-actor-json requires JSON".to_owned())
                })?);
            }
            other => {
                return Err(DebugJoinClientError::Cli(format!(
                    "unknown argument {other:?}; usage: debug-join --socket <path> (--endpoint-json <json> | --endpoint-json-stdin) [--orchestrator-actor-json <json>]"
                )));
            }
        }
    }
    let socket = socket.ok_or_else(|| {
        DebugJoinClientError::Cli(
            "missing --socket <path>; usage: debug-join --socket <path> (--endpoint-json <json> | --endpoint-json-stdin) [--orchestrator-actor-json <json>]".to_owned(),
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
    let orchestrator_actor = orchestrator_actor_json
        .as_deref()
        .map(serde_json::from_str::<ActorAddress>)
        .transpose()
        .map_err(|error| {
            DebugJoinClientError::Cli(format!("parse orchestrator actor JSON: {error}"))
        })?;
    send_debug_join_request(&socket, endpoint, orchestrator_actor)
        .map_err(DebugJoinClientError::Runtime)
}

pub(crate) fn request_debug_join(
    socket: &Path,
    endpoint: EndpointAddr,
    orchestrator_actor: ActorAddress,
) -> Result<(), String> {
    match send_debug_join_request(socket, endpoint, Some(orchestrator_actor))? {
        DebugJoinResponseWire::JoinQueued { .. } => Ok(()),
        DebugJoinResponseWire::JoinRejected { error, detail } => {
            Err(format!("worker join rejected: {error}: {detail}"))
        }
        DebugJoinResponseWire::RuntimeStats { .. } => {
            Err("worker join returned runtime stats unexpectedly".to_owned())
        }
    }
}

fn send_debug_join_request(
    socket: &Path,
    endpoint: EndpointAddr,
    orchestrator_actor: Option<ActorAddress>,
) -> Result<DebugJoinResponseWire, String> {
    let request = debug_join_request_line(endpoint, orchestrator_actor)?;
    let mut stream = std::os::unix::net::UnixStream::connect(socket)
        .map_err(|error| format!("connect {}: {error}", socket.display()))?;
    let mut request = request.as_bytes();
    while !request.is_empty() {
        match stream.write(request) {
            Ok(0) => return Err("debug join socket closed while writing".to_owned()),
            Ok(written) => request = &request[written..],
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(format!("write debug join request: {error}")),
        }
    }
    let mut response = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let read = match stream.read(&mut buffer) {
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(format!("read debug join response: {error}")),
        };
        if read == 0 {
            break;
        }
        let end = buffer[..read].iter().position(|byte| *byte == b'\n');
        response.extend_from_slice(&buffer[..end.unwrap_or(read)]);
        if response.len() > 64 * 1024 {
            return Err("debug join response exceeds 64 KiB".to_owned());
        }
        if end.is_some() {
            break;
        }
    }
    let response_line = String::from_utf8(response).map_err(|error| error.to_string())?;
    if response_line.trim().is_empty() {
        return Err("debug join socket closed without response".to_owned());
    }
    serde_json::from_str::<DebugJoinResponseWire>(&response_line)
        .map_err(|e| format!("parse response JSON: {e}"))
}

fn debug_join_request_line(
    endpoint: EndpointAddr,
    orchestrator_actor: Option<ActorAddress>,
) -> Result<String, String> {
    serde_json::to_string(&DebugJoinRequestWire::JoinEndpoint {
        endpoint,
        orchestrator_actor,
    })
    .map(|mut line| {
        line.push('\n');
        line
    })
    .map_err(|e| format!("serialize debug join request: {e}"))
}

fn spawn_debug_join_listener(
    tasks: TaskSpawner,
    path: PathBuf,
    wake: RuntimeWake,
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
    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel::<DebugJoinCommand>();
    swactor_process::spawn_unix_stream_listener(tasks, &path, move |stream| {
        let command_tx = command_tx.clone();
        let wake = wake.clone();
        async move {
            handle_debug_join_stream(stream, command_tx, wake).await;
        }
    })
    .map_err(|error| format!("bind debug join socket {}: {error}", path.display()))?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
        .map_err(|error| format!("chmod debug join socket {}: {error}", path.display()))?;
    Ok(command_rx)
}

async fn handle_debug_join_stream(
    stream: tokio::net::UnixStream,
    command_tx: tokio::sync::mpsc::UnboundedSender<DebugJoinCommand>,
    wake: RuntimeWake,
) {
    let mut reader = tokio::io::BufReader::new(stream);
    let mut line = String::new();
    let response = match reader.read_line(&mut line).await {
        Ok(0) => DebugJoinResponseWire::JoinRejected {
            error: "MalformedCommand".to_owned(),
            detail: "empty request".to_owned(),
        },
        Ok(_) => match parse_debug_join_request(&line) {
            Ok(DebugJoinRequestWire::JoinEndpoint {
                endpoint,
                orchestrator_actor,
            }) => {
                let (reply, response_rx) = tokio::sync::oneshot::channel();
                if command_tx
                    .send(DebugJoinCommand::JoinEndpoint {
                        endpoint,
                        orchestrator_actor,
                        reply,
                    })
                    .is_err()
                {
                    DebugJoinResponseWire::JoinRejected {
                        error: "CommandQueueClosed".to_owned(),
                        detail: "worker main loop is not accepting debug join commands".to_owned(),
                    }
                } else {
                    wake.send(NodeRuntimeMsg::Progress);
                    response_rx
                        .await
                        .unwrap_or_else(|error| DebugJoinResponseWire::JoinRejected {
                            error: "CommandCancelled".to_owned(),
                            detail: error.to_string(),
                        })
                }
            }
            Ok(DebugJoinRequestWire::RuntimeStats) => {
                let (reply, response_rx) = tokio::sync::oneshot::channel();
                if command_tx
                    .send(DebugJoinCommand::RuntimeStats { reply })
                    .is_err()
                {
                    DebugJoinResponseWire::JoinRejected {
                        error: "CommandQueueClosed".to_owned(),
                        detail: "worker main loop is not accepting debug commands".to_owned(),
                    }
                } else {
                    wake.send(NodeRuntimeMsg::Progress);
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
    peers: &PeerConnector,
    config: &DeploymentConfig,
    telemetry: &mut NodeTelemetry,
    pending_control_rejoin: &mut PendingControlRejoin,
    runtime_stats: &RuntimeStatsInspector,
) {
    let Some(rx) = debug_join_rx else {
        return;
    };
    while let Ok(command) = rx.try_recv() {
        match command {
            DebugJoinCommand::JoinEndpoint {
                endpoint,
                orchestrator_actor,
                reply,
            } => {
                let peer_node_id = endpoint.id.to_string();
                let has_relay = endpoint.relay_urls().next().is_some();
                let direct_addr_count = endpoint.ip_addrs().count();
                peers.connect(endpoint.clone());
                if let Some(orchestrator_actor) = orchestrator_actor {
                    pending_control_rejoin.set_recovery_actor(orchestrator_actor);
                }
                emit_node_event(
                    telemetry,
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
            DebugJoinCommand::RuntimeStats { reply } => {
                let _ = reply.send(DebugJoinResponseWire::RuntimeStats {
                    stats: runtime_stats.snapshot(),
                });
            }
        }
    }
}

#[derive(Clone, Copy)]
struct SamplerHealthContext {
    run_id: u64,
    node_id: u64,
    stage_index: u32,
}

impl SamplerHealthContext {
    fn from_config(config: &DeploymentConfig) -> Self {
        Self {
            run_id: config.run_id,
            node_id: config.logical_node_id,
            stage_index: config.stage_index,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SamplerHealthState {
    Waiting,
    Ready,
    Failed,
}

fn sampler_health_payload(
    context: SamplerHealthContext,
    sampler: &str,
    sample_channel: &str,
    status: &str,
    detail: Value,
) -> Value {
    json!({
        "type":"SamplerHealth",
        "event_type":"SamplerHealth",
        "event_name":"host_sampler_health",
        "schema":"myelin.node.sampler.health.v1",
        "run_id":context.run_id,
        "node_id":context.node_id,
        "stage_index":context.stage_index,
        "phase":"host_sampler_health",
        "status":status,
        "sampler":sampler,
        "sample_channel":sample_channel,
        "detail":detail,
    })
}

fn submit_sampler_health(
    producer: &TelemetryProducer,
    channel: ChannelId,
    context: SamplerHealthContext,
    sampler: &str,
    sample_channel: &str,
    status: &str,
    detail: Value,
) {
    let payload = sampler_health_payload(context, sampler, sample_channel, status, detail);
    producer.submit_bytes(
        channel,
        encode_record(&payload).expect("serialize sampler health telemetry"),
    );
}

fn submit_sampler_started(
    producer: &TelemetryProducer,
    health_channel: ChannelId,
    context: SamplerHealthContext,
    sampler: &str,
    sample_channel: &str,
) {
    submit_sampler_health(
        producer,
        health_channel,
        context,
        sampler,
        sample_channel,
        "started",
        json!({"state":"started","trigger":"work_progress"}),
    );
    submit_sampler_health(
        producer,
        health_channel,
        context,
        sampler,
        sample_channel,
        "waiting",
        json!({"state":"no_sample_yet","trigger":"work_progress"}),
    );
}

fn submit_sampler_sample_health(
    producer: &TelemetryProducer,
    health_channel: ChannelId,
    context: SamplerHealthContext,
    identity: (&str, &str),
    seq: u64,
    error: Option<&str>,
    state: &Mutex<SamplerHealthState>,
) {
    let (sampler, sample_channel) = identity;
    let mut state = state.lock();
    let (status, detail, next) = match (*state, error) {
        (SamplerHealthState::Waiting, None) => (
            "ready",
            json!({"state":"sample_observed","sample_seq":seq}),
            SamplerHealthState::Ready,
        ),
        (SamplerHealthState::Failed, None) => (
            "recovered",
            json!({"state":"sample_observed","sample_seq":seq}),
            SamplerHealthState::Ready,
        ),
        (SamplerHealthState::Waiting | SamplerHealthState::Ready, Some(error)) => (
            "failed",
            json!({"state":"error","sample_seq":seq,"error":error}),
            SamplerHealthState::Failed,
        ),
        (SamplerHealthState::Ready, None) | (SamplerHealthState::Failed, Some(_)) => return,
    };
    *state = next;
    drop(state);
    submit_sampler_health(
        producer,
        health_channel,
        context,
        sampler,
        sample_channel,
        status,
        detail,
    );
}

struct BlockingSamplerConfig<Sample> {
    endpoint: Arc<TelemetryEndpoint>,
    channel: ChannelId,
    health_channel: ChannelId,
    health_context: SamplerHealthContext,
    sampler: &'static str,
    sample_channel: &'static str,
    error_of: fn(&Sample) -> Option<&str>,
}

fn spawn_blocking_sampler_task<State, Sample>(
    tasks: TaskSpawner,
    engine: EngineHandle,
    state: State,
    sample_fn: fn(State, u64) -> (State, Sample),
    config: BlockingSamplerConfig<Sample>,
) -> tokio::sync::mpsc::Sender<()>
where
    State: Send + 'static,
    Sample: Record + Send + 'static,
{
    let BlockingSamplerConfig {
        endpoint,
        channel,
        health_channel,
        health_context,
        sampler,
        sample_channel,
        error_of,
    } = config;
    let producer = endpoint.producer();
    submit_sampler_started(
        &producer,
        health_channel,
        health_context,
        sampler,
        sample_channel,
    );
    let (progress, mut observations) = tokio::sync::mpsc::channel(1);
    let _ = progress.try_send(());
    tasks.spawn(async move {
        let blocking = engine.blocking_work_sender();
        let health_state = Mutex::new(SamplerHealthState::Waiting);
        let mut state = state;
        let mut seq = 0_u64;
        while observations.recv().await.is_some() {
            let (reply, result) = tokio::sync::oneshot::channel();
            if blocking
                .submit(Box::new(move || {
                    let _ = reply.send(sample_fn(state, seq));
                }))
                .is_err()
            {
                return;
            }
            let Ok((next_state, sample)) = result.await else {
                return;
            };
            state = next_state;
            submit_sampler_sample_health(
                &producer,
                health_channel,
                health_context,
                (sampler, sample_channel),
                seq,
                error_of(&sample),
                &health_state,
            );
            producer.submit_record(channel, &sample);
            endpoint.tick();
            seq = seq.saturating_add(1);
        }
    });
    progress
}

fn sample_host_gpu((): (), seq: u64) -> ((), telemetry::hardware::gpu::HostGpuSample) {
    ((), telemetry::hardware::gpu::sample(seq))
}

fn spawn_host_gpu_sampler(
    tasks: TaskSpawner,
    engine: EngineHandle,
    endpoint: Arc<TelemetryEndpoint>,
    channel: ChannelId,
    health_channel: ChannelId,
    health_context: SamplerHealthContext,
) -> tokio::sync::mpsc::Sender<()> {
    spawn_blocking_sampler_task(
        tasks,
        engine,
        (),
        sample_host_gpu,
        BlockingSamplerConfig {
            endpoint,
            channel,
            health_channel,
            health_context,
            sampler: "gpu",
            sample_channel: telemetry::hardware::gpu::HOST_GPU_CHANNEL,
            error_of: |sample| sample.error.as_deref(),
        },
    )
}

fn sample_host_memory((): (), seq: u64) -> ((), telemetry::hardware::memory::HostMemorySample) {
    ((), telemetry::hardware::memory::sample(seq))
}

fn spawn_host_memory_sampler(
    tasks: TaskSpawner,
    engine: EngineHandle,
    endpoint: Arc<TelemetryEndpoint>,
    channel: ChannelId,
    health_channel: ChannelId,
    health_context: SamplerHealthContext,
) -> tokio::sync::mpsc::Sender<()> {
    spawn_blocking_sampler_task(
        tasks,
        engine,
        (),
        sample_host_memory,
        BlockingSamplerConfig {
            endpoint,
            channel,
            health_channel,
            health_context,
            sampler: "memory",
            sample_channel: telemetry::hardware::memory::HOST_MEMORY_CHANNEL,
            error_of: |sample| sample.error.as_deref(),
        },
    )
}

fn sample_host_cpu(
    mut sampler: telemetry::hardware::cpu::CpuSampler,
    seq: u64,
) -> (
    telemetry::hardware::cpu::CpuSampler,
    telemetry::hardware::cpu::HostCpuSample,
) {
    let sample = sampler.sample(seq);
    (sampler, sample)
}

fn spawn_host_cpu_sampler(
    tasks: TaskSpawner,
    engine: EngineHandle,
    endpoint: Arc<TelemetryEndpoint>,
    channel: ChannelId,
    health_channel: ChannelId,
    health_context: SamplerHealthContext,
    watched_pids: Vec<u32>,
) -> tokio::sync::mpsc::Sender<()> {
    spawn_blocking_sampler_task(
        tasks,
        engine,
        telemetry::hardware::cpu::CpuSampler::new(watched_pids),
        sample_host_cpu,
        BlockingSamplerConfig {
            endpoint,
            channel,
            health_channel,
            health_context,
            sampler: "cpu",
            sample_channel: telemetry::hardware::cpu::HOST_CPU_CHANNEL,
            error_of: |sample| sample.error.as_deref(),
        },
    )
}

fn sample_host_net((): (), seq: u64) -> ((), telemetry::hardware::net::HostNetSample) {
    ((), telemetry::hardware::net::sample(seq))
}

fn spawn_host_net_sampler(
    tasks: TaskSpawner,
    engine: EngineHandle,
    endpoint: Arc<TelemetryEndpoint>,
    channel: ChannelId,
    health_channel: ChannelId,
    health_context: SamplerHealthContext,
) -> tokio::sync::mpsc::Sender<()> {
    spawn_blocking_sampler_task(
        tasks,
        engine,
        (),
        sample_host_net,
        BlockingSamplerConfig {
            endpoint,
            channel,
            health_channel,
            health_context,
            sampler: "net",
            sample_channel: telemetry::hardware::net::HOST_NET_CHANNEL,
            error_of: |sample| sample.error.as_deref(),
        },
    )
}

fn sample_host_storage((): (), seq: u64) -> ((), telemetry::hardware::storage::HostStorageSample) {
    ((), telemetry::hardware::storage::sample(seq))
}

fn spawn_host_storage_sampler(
    tasks: TaskSpawner,
    engine: EngineHandle,
    endpoint: Arc<TelemetryEndpoint>,
    channel: ChannelId,
    health_channel: ChannelId,
    health_context: SamplerHealthContext,
) -> tokio::sync::mpsc::Sender<()> {
    spawn_blocking_sampler_task(
        tasks,
        engine,
        (),
        sample_host_storage,
        BlockingSamplerConfig {
            endpoint,
            channel,
            health_channel,
            health_context,
            sampler: "storage",
            sample_channel: telemetry::hardware::storage::HOST_STORAGE_CHANNEL,
            error_of: |sample| sample.error.as_deref(),
        },
    )
}

struct ArenaSampler {
    producer: TelemetryProducer,
    channel: ChannelId,
    arena_manager: Arc<Mutex<arena::ArenaManager>>,
    seq: u64,
}

impl ArenaSampler {
    fn observe_progress(&mut self) {
        let sample: arena::ArenaSample = self.arena_manager.lock().sample(self.seq).into();
        self.seq = self.seq.saturating_add(1);
        self.producer.submit_record(self.channel, &sample);
    }
}

struct LoadedObject {
    object_id: u64,
    sequence: u64,
    handle_generation: u64,
    handle_id: u64,
}

struct ExecuteStepRequest {
    role_id: u64,
    step_id: u64,
    input_object_id: u64,
    input_sequence: u64,
    input_handle_id: u64,
    output_ring_id: u64,
    output_object_id: u64,
    output_sequence: u64,
    final_stage: bool,
    output_spec: StageObjectSpecWire,
}

/// All edge logic — lifecycle, wire bookkeeping, ingress parsing, arena and
/// worker orchestration — lives in the data-plane edge runtime. This wrapper
/// only supplies myelin effects (tinygrad worker port, telemetry, node-agent
/// reporting) around it.
struct WorkerEdgeRuntime {
    runtime: edge_runtime::EdgeRuntime<IrohEdgePort>,
    inbound_edge: Option<StageInboundEdgeWire>,
    outbound_edge: Option<StageOutboundEdgeWire>,
}

/// Worker-ring effects the data-plane edge runtime drives over the tinygrad
/// worker.
struct TinygradRingPort<'a> {
    worker: &'a mut TinygradWorker,
    config: &'a DeploymentConfig,
    telemetry: &'a mut NodeTelemetry,
    inbound: Option<StageInboundEdgeWire>,
    outbound: Option<StageOutboundEdgeWire>,
}

impl edge_runtime::WorkerPort for TinygradRingPort<'_> {
    fn install_ring(
        &mut self,
        edge_id: edge::EdgeId,
        ring_id: edge::RingId,
        direction: edge::RingDirection,
        layout: &arena::RingLayout,
        object_spec: &edge::ObjectSpec,
    ) -> Result<(), String> {
        let (port, direction_name, wire_spec) = match direction {
            edge::RingDirection::Ingress => (
                "input",
                "ingress",
                self.inbound.as_ref().map(|edge| edge.object_spec),
            ),
            edge::RingDirection::Egress => (
                "output",
                "egress",
                self.outbound.as_ref().map(|edge| edge.object_spec),
            ),
        };
        let wire_spec = wire_spec.unwrap_or(StageObjectSpecWire {
            max_extent: object_spec.max_extent_bytes,
            alignment: 4,
        });
        self.worker.install_ring(
            InstallRingRequest {
                ring_id: ring_id.0,
                edge_id: edge_id.0,
                port,
                direction: direction_name,
                layout: layout.clone(),
                object_spec: wire_spec,
            },
            self.config,
            self.telemetry,
        )
    }

    fn uninstall_ring(&mut self, ring_id: edge::RingId) -> Result<(), String> {
        self.worker
            .uninstall_ring(ring_id.0, self.config, self.telemetry)
    }

    fn load_object(
        &mut self,
        edge_id: edge::EdgeId,
        ring_id: edge::RingId,
        _record: &ingress::ObjectRecord,
        spec: &ingress::ObjectSpec,
    ) -> Result<edge_runtime::LoadedObject, String> {
        let wire_spec = StageObjectSpecWire {
            max_extent: spec.max_extent,
            alignment: spec.alignment.min(u64::from(u32::MAX)) as u32,
        };
        let loaded = self.worker.ring_readable(
            ring_id.0,
            edge_id.0,
            wire_spec,
            self.config,
            self.telemetry,
        )?;
        Ok(edge_runtime::LoadedObject {
            object_id: loaded.object_id,
            sequence: loaded.sequence,
            handle_generation: loaded.handle_generation,
            handle_id: loaded.handle_id,
        })
    }
}

struct WorkerRuntimeContext<'a> {
    config: &'a DeploymentConfig,
    stack: &'a DistributionRuntimeStack,
    driver: &'a mut IrohEdgePort,
    node_actor: ActorAddress,
    worker: &'a mut TinygradWorker,
    arena_manager: &'a Arc<Mutex<arena::ArenaManager>>,
    telemetry: &'a mut NodeTelemetry,
}

struct EdgeStepRequest {
    step_id: u64,
    input_edge_id: u64,
    object_id: u64,
    sequence: u64,
}

struct PromptRequest {
    request_id: u64,
    prompt: String,
    max_tokens: u32,
    reply_to: ActorAddress,
}

struct EncodePromptRequest {
    request_id: u64,
    prompt: String,
    reply_to: ActorAddress,
}

struct DecodeTokensRequest {
    request_id: u64,
    tokens: Vec<u32>,
    reply_to: ActorAddress,
}

struct WorkerStageServices<'a> {
    config: &'a DeploymentConfig,
    stack: &'a DistributionRuntimeStack,
    node_actor: ActorAddress,
    arena_manager: &'a Arc<Mutex<arena::ArenaManager>>,
}

impl WorkerEdgeRuntime {
    fn new(local_node_id: u64) -> Self {
        Self {
            runtime: edge_runtime::EdgeRuntime::new(edge::NodeId(local_node_id)),
            inbound_edge: None,
            outbound_edge: None,
        }
    }

    /// Run one data-plane edge-runtime tick, then map its observations to
    /// telemetry and node-agent messages. The poll error (if any) propagates
    /// after the observations are reported, mirroring the previous
    /// composition's report-then-halt behavior.
    fn poll_and_report(&mut self, context: &mut WorkerRuntimeContext<'_>) -> Result<(), String> {
        let config = context.config;
        let stack = context.stack;
        let driver = &mut *context.driver;
        let node_actor = context.node_actor;
        let worker = &mut *context.worker;
        let arena_manager = context.arena_manager;
        let telemetry = &mut *context.telemetry;
        let result = {
            let mut arena = arena_manager.lock();
            let mut port = TinygradRingPort {
                worker,
                config,
                telemetry,
                inbound: self.inbound_edge.clone(),
                outbound: self.outbound_edge.clone(),
            };
            self.runtime.poll(driver, &mut arena, &mut port)
        };
        self.report(stack, node_actor, config, telemetry)?;
        result
    }

    /// Map drained runtime observations to telemetry events and node-agent
    /// messages.
    fn report(
        &mut self,
        stack: &DistributionRuntimeStack,
        node_actor: ActorAddress,
        config: &DeploymentConfig,
        telemetry: &mut NodeTelemetry,
    ) -> Result<(), String> {
        let node_stage = |ds: &mut NodeTelemetry, phase: &str, status: &str, detail: Value| {
            emit_node_event(ds, config, NODE_STAGE_CHANNEL, phase, status, detail)
        };
        for observation in self.runtime.take_observations() {
            match observation {
                edge_runtime::Observation::StreamArrived { edge_id, stream_id } => {
                    node_stage(
                        telemetry,
                        "iroh_edge_stream_arrived",
                        "observed",
                        json!({"edge_id":edge_id.0,"stream_id":stream_id.0}),
                    );
                }
                edge_runtime::Observation::BytesRead {
                    edge_id,
                    stream_id,
                    byte_count,
                } => {
                    node_stage(
                        telemetry,
                        "iroh_edge_bytes_read",
                        "observed",
                        json!({"edge_id":edge_id.0,"stream_id":stream_id.0,"bytes":byte_count}),
                    );
                }
                edge_runtime::Observation::IngressRingWrite {
                    edge_id,
                    ring_id,
                    stream_id,
                    object_id,
                    sequence,
                    extent,
                    begin_sequence,
                    end_of_sequence,
                    record_bytes,
                    buffered_bytes,
                    ..
                } => {
                    let edge_kind = self
                        .inbound_edge
                        .as_ref()
                        .map(|edge| format!("{:?}", edge.kind))
                        .unwrap_or_else(|| "unknown".to_owned());
                    node_stage(
                        telemetry,
                        "ingress_ring_write",
                        "ready",
                        json!({
                            "edge_id":edge_id.0,
                            "edge_kind":edge_kind,
                            "ring_id":ring_id.0,
                            "stream_id":stream_id.0,
                            "object_id":object_id,
                            "sequence":sequence,
                            "extent":extent,
                            "begin_sequence":begin_sequence,
                            "end_of_sequence":end_of_sequence,
                            "record_bytes":record_bytes,
                            "ingress_buffer_bytes":buffered_bytes,
                        }),
                    );
                }
                edge_runtime::Observation::ObjectLoaded {
                    edge_id,
                    ring_id,
                    stream_id,
                    object,
                    ..
                } => {
                    let edge_kind = self
                        .inbound_edge
                        .as_ref()
                        .map(|edge| format!("{:?}", edge.kind))
                        .unwrap_or_else(|| "unknown".to_owned());
                    node_stage(
                        telemetry,
                        "object_loaded",
                        "ready",
                        json!({
                            "edge_id":edge_id.0,
                            "edge_kind":edge_kind,
                            "ring_id":ring_id.0,
                            "stream_id":stream_id.0,
                            "object_id":object.object_id,
                            "sequence":object.sequence,
                            "handle_generation":object.handle_generation,
                            "handle_id":object.handle_id,
                        }),
                    );
                    stack
                        .runtime
                        .send_to(
                            node_actor,
                            NodeAgentMsg::ObjectLoaded {
                                edge_id: edge_id.0,
                                object_id: object.object_id,
                                sequence: object.sequence,
                                handle_generation: object.handle_generation,
                                handle_id: object.handle_id,
                            },
                        )
                        .map_err(|e| format!("report object loaded: {e}"))?;
                }
                edge_runtime::Observation::ObjectFailed { edge_id, object_id } => {
                    let _ = stack.runtime.send_to(
                        node_actor,
                        NodeAgentMsg::ObjectFailed {
                            edge_id: edge_id.0,
                            object_id,
                        },
                    );
                }
                edge_runtime::Observation::EdgeReady { edge_id, .. } => {
                    if self
                        .inbound_edge
                        .as_ref()
                        .is_some_and(|edge| edge.edge_id == edge_id.0)
                    {
                        stack
                            .runtime
                            .send_to(
                                node_actor,
                                NodeAgentMsg::MarkInboundEdgeReady { edge_id: edge_id.0 },
                            )
                            .map_err(|e| format!("mark inbound ready: {e}"))?;
                    }
                    if self
                        .outbound_edge
                        .as_ref()
                        .is_some_and(|edge| edge.edge_id == edge_id.0)
                    {
                        stack
                            .runtime
                            .send_to(
                                node_actor,
                                NodeAgentMsg::MarkOutboundEdgeReady { edge_id: edge_id.0 },
                            )
                            .map_err(|e| format!("mark outbound ready: {e}"))?;
                    }
                }
                edge_runtime::Observation::EdgeFaulted { edge_id, .. } => {
                    stack
                        .runtime
                        .send_to(node_actor, NodeAgentMsg::EdgeFault { edge_id: edge_id.0 })
                        .map_err(|e| format!("report edge fault: {e}"))?;
                }
                edge_runtime::Observation::EdgeStopped { .. } => {}
            }
        }
        Ok(())
    }

    fn poll_iroh(&mut self, context: &mut WorkerRuntimeContext<'_>) -> Result<(), String> {
        self.poll_and_report(context)
    }

    fn establish_inbound(
        &mut self,
        edge_wire: StageInboundEdgeWire,
        context: &mut WorkerRuntimeContext<'_>,
    ) -> Result<(), String> {
        let config = context.config;
        let parse_spec = ingress::ObjectSpec {
            max_extent: edge_wire.object_spec.max_extent,
            alignment: u64::from(edge_wire.object_spec.alignment),
            layout: ingress::ObjectLayout::Token,
        };
        self.runtime.establish_inbound(
            edge::ProvisionRx {
                execution_id: edge::ExecutionId(config.run_id),
                edge_id: edge::EdgeId(edge_wire.edge_id),
                local_node_id: edge::NodeId(config.logical_node_id),
                object_spec: edge::ObjectSpec {
                    kind: edge::ObjectKind::Activation,
                    dtype: edge::DType::F16,
                    max_extent_bytes: edge_wire.object_spec.max_extent,
                },
                ring_spec: edge::RingSpec {
                    header_bytes: 0,
                    data_bytes: edge_wire.ring_spec.data_capacity,
                    alignment: u64::from(edge_wire.ring_spec.alignment),
                },
            },
            parse_spec,
        );
        self.inbound_edge = Some(edge_wire);
        self.poll_and_report(context)
    }

    fn establish_outbound(
        &mut self,
        edge_wire: StageOutboundEdgeWire,
        context: &mut WorkerRuntimeContext<'_>,
    ) -> Result<(), String> {
        let config = context.config;
        let stack = context.stack;
        let node_actor = context.node_actor;
        if edge_wire.consumer_endpoint.is_none() {
            stack
                .runtime
                .send_to(
                    node_actor,
                    NodeAgentMsg::MarkOutboundEdgeReady {
                        edge_id: edge_wire.edge_id,
                    },
                )
                .map_err(|e| format!("mark outbound edge ready: {e}"))?;
            self.outbound_edge = Some(edge_wire);
            return Ok(());
        }
        let peer = edge_wire
            .consumer_endpoint
            .clone()
            .expect("consumer endpoint presence checked above");
        self.runtime.establish_outbound(
            edge::ProvisionTx {
                execution_id: edge::ExecutionId(config.run_id),
                edge_id: edge::EdgeId(edge_wire.edge_id),
                local_node_id: edge::NodeId(config.logical_node_id),
                consumer_node_id: edge::NodeId(edge_wire.consumer_node_id),
                object_spec: edge::ObjectSpec {
                    kind: edge::ObjectKind::Activation,
                    dtype: edge::DType::F16,
                    max_extent_bytes: edge_wire.object_spec.max_extent,
                },
                ring_spec: edge::RingSpec {
                    header_bytes: 0,
                    data_bytes: edge_wire.ring_spec.data_capacity,
                    alignment: u64::from(edge_wire.ring_spec.alignment),
                },
            },
            peer,
        );
        self.outbound_edge = Some(edge_wire);
        self.poll_and_report(context)
    }

    fn execute_step(
        &mut self,
        request: EdgeStepRequest,
        context: &mut WorkerRuntimeContext<'_>,
    ) -> Result<(), String> {
        let EdgeStepRequest {
            step_id,
            input_edge_id,
            object_id,
            sequence,
        } = request;
        let config = context.config;
        let stack = context.stack;
        let node_actor = context.node_actor;
        let worker = &mut *context.worker;
        let arena_manager = context.arena_manager;
        let telemetry = &mut *context.telemetry;
        let node_stage = |ds: &mut NodeTelemetry, phase: &str, status: &str, detail: Value| {
            emit_node_event(ds, config, NODE_STAGE_CHANNEL, phase, status, detail)
        };
        let loaded = self
            .runtime
            .loaded_object(edge::EdgeId(input_edge_id), object_id)
            .cloned()
            .ok_or_else(|| {
                format!("object (edge {input_edge_id}, id {object_id}) has no loaded device handle")
            })?;
        if loaded.sequence != sequence {
            return Err(format!(
                "object (edge {input_edge_id}, id {object_id}) sequence {} does not match command sequence {sequence}",
                loaded.sequence
            ));
        }
        let outbound = self
            .outbound_edge
            .clone()
            .ok_or_else(|| "outbound edge missing".to_owned())?;
        let output_ring_id = self
            .runtime
            .outbound_ring_id()
            .ok_or_else(|| "outbound ring missing".to_owned())?
            .0;
        let output_object_id = self.runtime.alloc_output_object_id()?;
        let final_stage = matches!(
            outbound.kind,
            crate::node_actor::StageEdgeKindWire::TokenOut
        );

        let committed_bytes = match worker.execute_step(
            ExecuteStepRequest {
                role_id: u64::from(config.stage_index) + 1,
                step_id,
                input_object_id: object_id,
                input_sequence: sequence,
                input_handle_id: loaded.handle_id,
                output_ring_id,
                output_object_id,
                output_sequence: sequence,
                final_stage,
                output_spec: outbound.object_spec,
            },
            config,
            telemetry,
        ) {
            Ok(bytes) => bytes,
            Err(e) => {
                let _ = stack
                    .runtime
                    .send_to(node_actor, NodeAgentMsg::StepFailed { step_id });
                return Err(e);
            }
        };

        let record = {
            let arena = arena_manager.lock();
            let lease = arena
                .lookup_lease(arena::RingId(output_ring_id))
                .ok_or_else(|| format!("outbound ring {output_ring_id} lease missing"))?;
            arena.read_arena(lease.layout.data_offset, committed_bytes)
        };
        let record = match record {
            Ok(record) => record,
            Err(e) => {
                let _ = stack.runtime.send_to(
                    node_actor,
                    NodeAgentMsg::OutputFault {
                        edge_id: outbound.edge_id,
                    },
                );
                return Err(format!("read egress ring: {e}"));
            }
        };

        let record_bytes = record.len();
        node_stage(
            telemetry,
            "egress_ring_read",
            "ready",
            json!({
                "edge_id":outbound.edge_id,
                "edge_kind":format!("{:?}", outbound.kind),
                "ring_id":output_ring_id,
                "step_id":step_id,
                "input_object_id":object_id,
                "input_edge_id":input_edge_id,
                "sequence":sequence,
                "output_object_id":output_object_id,
                "output_sequence":sequence,
                "record_bytes":record_bytes,
                "committed_bytes":committed_bytes,
                "final_stage":final_stage,
            }),
        );

        let sender = self
            .runtime
            .outbound_writer()
            .ok_or_else(|| "outbound edge sender missing".to_owned())?;
        if let Err(e) = sender.send(record) {
            let _ = stack.runtime.send_to(
                node_actor,
                NodeAgentMsg::OutputFault {
                    edge_id: outbound.edge_id,
                },
            );
            return Err(e);
        }

        node_stage(
            telemetry,
            "iroh_edge_bytes_sent",
            "ready",
            json!({
                "edge_id":outbound.edge_id,
                "edge_kind":format!("{:?}", outbound.kind),
                "step_id":step_id,
                "object_id":output_object_id,
                "sequence":sequence,
                "bytes":record_bytes,
                "record_bytes":record_bytes,
            }),
        );
        stack
            .runtime
            .send_to(node_actor, NodeAgentMsg::StepCompleted { step_id })
            .map_err(|e| format!("mark step completed: {e}"))
    }

    fn release_input_handle(
        &mut self,
        handle_id: u64,
        worker: &mut TinygradWorker,
        config: &DeploymentConfig,
        telemetry: &mut NodeTelemetry,
        _driver: &mut IrohEdgePort,
        _stack: &DistributionRuntimeStack,
    ) -> Result<(), String> {
        worker.release_device_object(handle_id, config, telemetry)
    }
}

fn value_u64(value: &Value, field: &str) -> Result<u64, String> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("helper event missing numeric {field}: {value}"))
}

pub fn run_helper_from_env() -> Option<ExitCode> {
    let mut args = std::env::args().skip(1);
    Some(match args.next().as_deref() {
        Some("debug-join") => debug_join_client_main(args.collect()),
        Some("stage-shard-fetcher") => match run_stage_shard_fetcher() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                let event = json!({"type":"StageShardFetchFailed","error":error});
                println!("{event}");
                ExitCode::from(1)
            }
        },
        _ => return None,
    })
}

#[derive(serde::Deserialize, serde::Serialize)]
struct StageShardFetchRequest {
    plan: StageShardPlan,
    output_path: PathBuf,
}

fn run_stage_shard_fetcher() -> Result<(), String> {
    let mut input = String::new();
    std::io::stdin()
        .read_to_string(&mut input)
        .map_err(|e| format!("read stage shard fetch request: {e}"))?;
    let request: StageShardFetchRequest = serde_json::from_str(&input)
        .map_err(|e| format!("parse stage shard fetch request: {e}"))?;
    materialize_stage_shard_http(&request.plan, &request.output_path, |event| {
        println!("{event}");
        let _ = std::io::stdout().flush();
    })
}

fn publish_bootstrap_exec(config: &DeploymentConfig) -> Result<(), String> {
    let Some(raw_fd) = env_optional("MYELIN_BOOTSTRAP_READY_FD") else {
        return Ok(());
    };
    #[cfg(not(target_os = "linux"))]
    {
        let _ = config;
        Err(format!(
            "bootstrap readiness descriptor {raw_fd} requires Linux"
        ))
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::FromRawFd;

        let fd = raw_fd
            .parse::<i32>()
            .map_err(|error| format!("invalid MYELIN_BOOTSTRAP_READY_FD: {error}"))?;
        if fd <= 2 {
            return Err("bootstrap readiness requires a dedicated inherited descriptor".to_owned());
        }
        // SAFETY: F_GETFD only checks the descriptor; it borrows no Rust value.
        if unsafe { libc::fcntl(fd, libc::F_GETFD) } < 0 {
            return Err(format!(
                "inspect bootstrap readiness descriptor: {}",
                std::io::Error::last_os_error()
            ));
        }
        // SAFETY: the launcher transfers this live descriptor exclusively to
        // this startup path, before the worker creates its runtime threads.
        let mut ready = unsafe { fs::File::from_raw_fd(fd) };
        let deployment = config
            .deployment
            .as_ref()
            .ok_or_else(|| "bootstrap readiness requires a deployment identity".to_owned())?;
        let process_start_ticks = config
            .process_start_ticks
            .ok_or_else(|| "bootstrap readiness requires a process identity".to_owned())?;
        #[derive(serde::Serialize)]
        struct ExecReceipt<'a> {
            pid: u32,
            process_start_ticks: u64,
            deployment_generation: &'a str,
        }
        serde_json::to_writer(
            &mut ready,
            &ExecReceipt {
                pid: std::process::id(),
                process_start_ticks,
                deployment_generation: &deployment.deployment_generation,
            },
        )
        .map_err(|error| format!("write bootstrap exec receipt: {error}"))?;
        ready
            .write_all(b"\n")
            .map_err(|error| format!("finish bootstrap exec receipt: {error}"))
    }
}

/// Prepare worker runtime parts before the binary constructs its execution engine.
///
/// The returned continuation receives task authority explicitly; it never mints
/// a capability and keeps the engine alive through terminal runtime completion.
pub fn prepare_worker_node() -> Result<
    (
        RuntimeParts,
        impl FnOnce(Engine, TaskSpawner) -> Result<(), String>,
    ),
    String,
> {
    let config = DeploymentConfig::from_env()?;
    publish_bootstrap_exec(&config)?;
    let boot = |phase: &str, status: &str, detail: Value| {
        emit_stdio_node_event(&config, NODE_BOOTSTRAP_CHANNEL, phase, status, detail)
    };
    boot(
        "config",
        "ready",
        json!({
            "worker_script":&config.worker_script,
            "device":&config.device,
            "model_id":&config.model_id,
            "has_coordinator_endpoint":config.coordinator_endpoint.is_some(),
            "has_orchestrator_actor":config.orchestrator_actor.is_some(),
            "self_test_enabled":config.self_test_prompt.is_some(),
            "arena_bytes":config.arena_bytes,
            "arena_alignment":config.arena_alignment,
            "debug_join_socket":config.debug_join_socket.as_deref().unwrap_or("disabled"),
        }),
    )?;
    boot(
        "process",
        "started",
        json!({"binary":"myelin-worker","pid":std::process::id()}),
    )?;

    // Telemetry must exist before the runtime: its stats hook is wired in
    // during runtime construction.
    let mut telemetry = NodeTelemetry::new(&config)?;

    // Build the core swactor runtime parts, clone the routing handle needed by
    // integrations, then hand the workers to the engine. The engine owns both
    // core progression and the Tokio substrate (it schedules all background
    // work); components retain only cheap Runtime handles (ENGINE_SPEC.md).
    let runtime_stats = RuntimeStatsInspector::default();
    let telemetry_stats_hook = telemetry.producer.stats_hook();
    let worker_stats_hook: Arc<dyn StatsHook> = if config.debug_join_socket.is_some() {
        Arc::new(InspectableStatsHook {
            inner: telemetry_stats_hook,
            inspector: runtime_stats.clone(),
        })
    } else {
        telemetry_stats_hook
    };
    let (parts, runtime, codec, transport_router) = DistributionRuntimeStack::build_runtime(
        |registry| {
            register_myelin_actor_codecs(registry);
            telemetry::wire::register_telemetry_codec(registry);
        },
        Some(worker_stats_hook),
    );
    Ok((parts, move |engine: Engine, tasks: TaskSpawner| {
        let boot = |phase: &str, status: &str, detail: Value| {
            emit_stdio_node_event(&config, NODE_BOOTSTRAP_CHANNEL, phase, status, detail)
        };
        let worker_evt = |phase: &str, status: &str, detail: Value| {
            emit_stdio_node_event(&config, NODE_WORKER_CHANNEL, phase, status, detail)
        };
        boot(
            "engine",
            "ready",
            json!({"backend":"tokio","owns":"core+substrate"}),
        )?;
        let mut driver = match IrohDriver::with_engine(
            tasks.clone(),
            engine.handle(),
            IrohDriverConfig {
                secret_key: None,
                relay_mode: config.relay_mode.clone(),
                bind_port: None,
                node: DistributedNodeConfig::default(),
                peer_auth: None,
                additional_alpns: vec![EDGE_ALPN.to_vec(), TELEMETRY_ALPN.to_vec()],
            },
        ) {
            Ok(driver) => driver,
            Err(error) => {
                boot("iroh_driver", "failed", json!({"error":error.to_string()}))?;
                return Err(format!("create iroh driver: {error}"));
            }
        };
        // Bootstrap supervisors pull telemetry from the node endpoint. Keep these
        // ALPN connections available to the node loop instead of the legacy push
        // reader path.
        driver.retain_telemetry_connections();
        let coordinator_uses_relay = config
            .coordinator_endpoint
            .as_ref()
            .is_some_and(|endpoint| endpoint.relay_urls().next().is_some());
        if coordinator_uses_relay && !matches!(config.relay_mode, RelayMode::Disabled) {
            boot(
                "iroh_relay",
                "waiting",
                json!({"relay_mode":format!("{:?}", config.relay_mode)}),
            )?;
            let endpoint = driver.wait_for_relay_endpoint()?;
            boot(
                "iroh_relay",
                "ready",
                json!({"relay_url":endpoint.relay_urls().next().map(ToString::to_string)}),
            )?;
        }
        let advertised_self_endpoint =
            advertised_endpoint(driver.endpoint_addr(), config.endpoint_addr_mask)?;
        boot(
            "iroh_driver",
            "ready",
            json!({"endpoint":advertised_self_endpoint.clone(),"has_relay":advertised_self_endpoint.relay_urls().next().is_some(),"direct_addr_count":advertised_self_endpoint.ip_addrs().count(),"relay_mode":format!("{:?}", config.relay_mode),"endpoint_addr_mask":config.endpoint_addr_mask.as_str()}),
        )?;
        if let Some(coordinator) = &config.coordinator_endpoint {
            driver.join(std::slice::from_ref(coordinator));
            boot(
                "coordinator_join",
                "started",
                json!({"endpoint":coordinator,"has_relay":coordinator.relay_urls().next().is_some(),"direct_addr_count":coordinator.ip_addrs().count()}),
            )?;
        } else {
            boot(
                "coordinator_join",
                "skipped",
                json!({"reason":"MYELIN_COORDINATOR_ENDPOINT not set","mode":"standalone"}),
            )?;
        }

        let stack = DistributionRuntimeStack::new_from_runtime(
            runtime.clone(),
            codec,
            transport_router,
            driver.node_id(),
            DistributedNodeConfig::default(),
            engine.handle(),
        );
        boot(
            "distribution_stack",
            "ready",
            json!({"actors":"initialized","route_view":"initialized","membership":"initialized","outbox":"initialized"}),
        )?;
        boot(
            "codecs",
            "ready",
            json!({"registered":["node_agent","orchestrator","provisioner","prompt_rpc","telemetry"]}),
        )?;
        driver.enable_actor_bridge(iroh_driver::ActorBridgeConfig {
            runtime: stack.runtime.clone(),
            codec: stack.codec.clone(),
            routes: stack.actor_bridge_routes(),
            swim: stack.actors.swim,
            relay_mirror: stack.relay_mirror.clone(),
            route_view: stack.route_view.clone(),
            outbox: stack.outbox.clone(),
        });
        // Distribution protocols own their progression; Myelin supplies no cadence.
        driver.install_actor_bridge();
        boot(
            "actor_bridge",
            "ready",
            json!({"transport":"iroh","routes":"attached","progress":"transport_observations"}),
        )?;

        let arena_manager = match arena::ArenaManager::boot(arena::ArenaConfig {
            node_id: arena::NodeId(config.logical_node_id),
            reservation_ceiling: config.arena_bytes,
            base_alignment: config.arena_alignment,
        }) {
            Ok(manager) => {
                boot(
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
                boot(
                    "arena_manager",
                    "failed",
                    json!({"error":format!("{error:?}")}),
                )?;
                return Err(format!("boot arena manager: {error:?}"));
            }
        };
        let arena_fd = arena_manager.lock().arena_fd();

        let node_runtime = |ds: &mut NodeTelemetry, phase: &str, status: &str, detail: Value| {
            emit_node_event(ds, &config, NODE_RUNTIME_CHANNEL, phase, status, detail)
        };
        // Telemetry leaves this node exclusively through pull subscriptions served
        // by the external transport owner on `TELEMETRY_ALPN`; no publisher actor.
        let sampler_health_channel = telemetry.channel_by_name(NODE_SAMPLER_CHANNEL);
        let sampler_health_context = SamplerHealthContext::from_config(&config);
        let report_wake_target = Arc::new(Mutex::new(None));
        let runtime_wake = RuntimeWake {
            sender: stack.runtime.create_sender(),
            target: Arc::clone(&report_wake_target),
        };
        telemetry
            .resource_samples
            .lock()
            .push(spawn_host_gpu_sampler(
                tasks.clone(),
                engine.handle(),
                Arc::clone(&telemetry.endpoint),
                telemetry.channels.host_gpu,
                sampler_health_channel,
                sampler_health_context,
            ));
        telemetry
            .resource_samples
            .lock()
            .push(spawn_host_memory_sampler(
                tasks.clone(),
                engine.handle(),
                Arc::clone(&telemetry.endpoint),
                telemetry.channels.host_memory,
                sampler_health_channel,
                sampler_health_context,
            ));
        telemetry
            .resource_samples
            .lock()
            .push(spawn_host_net_sampler(
                tasks.clone(),
                engine.handle(),
                Arc::clone(&telemetry.endpoint),
                telemetry.channels.host_net,
                sampler_health_channel,
                sampler_health_context,
            ));
        telemetry
            .resource_samples
            .lock()
            .push(spawn_host_storage_sampler(
                tasks.clone(),
                engine.handle(),
                Arc::clone(&telemetry.endpoint),
                telemetry.channels.host_storage,
                sampler_health_channel,
                sampler_health_context,
            ));
        telemetry.arena_sampler = Some(ArenaSampler {
            producer: telemetry.producer.clone(),
            channel: telemetry.channels.arena,
            arena_manager: Arc::clone(&arena_manager),
            seq: 0,
        });
        telemetry.observe_work();
        let debug_join_rx = match &config.debug_join_socket {
            Some(path) => match spawn_debug_join_listener(
                tasks.clone(),
                PathBuf::from(path),
                runtime_wake.clone(),
            ) {
                Ok(rx) => {
                    node_runtime(
                        &mut telemetry,
                        "debug_join_socket",
                        "ready",
                        json!({"socket":path}),
                    );
                    Some(rx)
                }
                Err(error) => {
                    node_runtime(
                        &mut telemetry,
                        "debug_join_socket",
                        "failed",
                        json!({"socket":path,"error":error}),
                    );
                    return Err(format!("bind debug join socket {}: {error}", path));
                }
            },
            None => {
                node_runtime(
                    &mut telemetry,
                    "debug_join_socket",
                    "skipped",
                    json!({"reason":"MYELIN_DEBUG_JOIN_SOCKET=disabled"}),
                );
                None
            }
        };

        let reports = match stack.runtime.new_inbox::<NodeAgentReport>() {
            Ok(inbox) => {
                boot("node_report_inbox", "ready", json!({"actor":inbox.addr()}))?;
                inbox
            }
            Err(error) => {
                boot(
                    "node_report_inbox",
                    "failed",
                    json!({"error":error.to_string()}),
                )?;
                return Err(format!("node report inbox: {error}"));
            }
        };
        let report_forwarder = stack
            .runtime
            .spawn(NodeReportWakeForwarder {
                destination: *reports.addr(),
                target: Arc::clone(&report_wake_target),
            })
            .map_err(|error| format!("spawn node report wake forwarder: {error}"))?;
        let route_target = Arc::clone(&report_wake_target);
        let route_sender = stack.runtime.create_sender();
        let route_wake: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            if let Some(target) = *route_target.lock() {
                let _ = route_sender.send_to(target, NodeRuntimeMsg::RoutingChanged);
            }
        });
        stack
            .runtime
            .send_to(
                stack.actors.directory,
                distribution::directory_actor::DirectoryIn::WatchRoutes {
                    changed: Arc::downgrade(&route_wake),
                },
            )
            .map_err(|error| format!("watch runtime ready routes: {error}"))?;
        stack
            .runtime
            .send_to(
                stack.actors.registry,
                distribution::registry_actor::RegistryIn::WatchName {
                    name: CONTROL_REGISTRY_NAME.to_owned(),
                    changed: Arc::downgrade(&route_wake),
                },
            )
            .map_err(|error| format!("watch control registry: {error}"))?;
        stack.watch_membership(Arc::downgrade(&route_wake));
        // The synchronous entrypoint retains the transport watch until its runtime
        // actor completes; teardown cancels the watch even on startup failure.
        let _route_watch = driver.connection_observer().watch_connections(route_wake);
        let rejoin_replies = stack
            .runtime
            .new_inbox::<ManualControlReply>()
            .map_err(|error| format!("rejoin reply inbox: {error}"))?;
        let rejoin_forwarder = stack
            .runtime
            .spawn(RejoinReplyForwarder {
                destination: *rejoin_replies.addr(),
                wake: runtime_wake.clone(),
            })
            .map_err(|error| format!("spawn rejoin reply observer: {error}"))?;
        let orchestrator = config.orchestrator_actor.ok_or_else(|| {
            "MYELIN_ORCHESTRATOR_ACTOR is required for runtime readiness signaling".to_owned()
        })?;
        let orchestrator_source = "env";
        boot(
            "orchestrator_actor",
            "ready",
            json!({"actor":orchestrator,"source":orchestrator_source}),
        )?;
        let mut contextual_blob_receiver = None;
        let contextual_services = if config.agent_only {
            let namespace = install_namespace_client(&stack, &driver)?;
            let blob_receiver = Arc::new(IrohBlobTransferReceiver::new(
                driver.endpoint_addr(),
                driver.edge_events_handle(),
            ));
            contextual_blob_receiver = Some(Arc::clone(&blob_receiver));
            let transfer_receiver: Arc<dyn BlobTransferReceiver> = blob_receiver;
            let source_sender: Arc<dyn BlobTransferSender> = Arc::new(IrohBlobTransferSender::new(
                driver.edge_connector(),
                &engine.handle(),
                stack.runtime.clone(),
            ));
            let routes: Arc<dyn HostRouteRegistrar> = Arc::new(MyelinChildRouteRegistrar::new(
                stack.route_view.clone(),
                stack.pinned_routes.clone(),
                stack.route_binder.clone(),
                stack.runtime.clone(),
                stack.actors.directory,
                driver.connection_observer(),
            ));
            let spawner = Arc::new(build_contextual_process_spawner(
                MyelinContextualProcessConfig {
                    runtime: stack.runtime.clone(),
                    engine: engine.handle(),
                    arena_bytes: config.arena_bytes,
                    arena_alignment: config.arena_alignment,
                    namespace: Some(namespace.client.clone()),
                    transfer_receiver: Some(Arc::clone(&transfer_receiver)),
                    source_sender: Some(source_sender),
                    source_publisher: Some(namespace.source_publisher),
                    route_registrar: Arc::clone(&routes),
                    stream_transport: Some(driver.stream_transport()),
                    host_endpoint: driver.endpoint_addr(),
                },
            )?);
            let materializer = ContextualProgramMaterializer {
                namespace_proxy: namespace.client.proxy(),
                receiver: transfer_receiver,
                routes,
                sender: stack.runtime.create_sender(),
                root: std::env::temp_dir().join("myelin-contextual"),
            };
            Some((spawner, materializer))
        } else {
            None
        };
        let contextual_controller = contextual_services
            .as_ref()
            .map(|(spawner, materializer)| {
                stack.runtime.spawn(
                    ContextualProcessController::new(
                        stack.runtime.clone(),
                        config.logical_node_id,
                        Arc::clone(spawner),
                        stack.runtime.create_sender(),
                        Arc::clone(&materializer.routes),
                    )?
                    .with_program_materializer(materializer.clone())
                    .with_resource_probe(
                        crate::contextual_process::ContextualResourceProbe {
                            runtime: stack.runtime.clone(),
                            engine: engine.handle(),
                            arena: Arc::clone(&arena_manager),
                            stream_transport: driver.stream_transport().resource_probe(),
                            generation: telemetry.producer.stream_id().life.0,
                            iroh_node_id: driver.node_id().to_string(),
                            deployment: config.deployment.clone(),
                        },
                    ),
                )
            })
            .transpose()
            .map_err(|error| format!("spawn contextual process controller: {error}"))?;
        let mut node_agent = NodeAgentActor::new(
            stage::NodeId(config.logical_node_id),
            orchestrator,
            Some(report_forwarder),
        )
        .with_directory_actor(stack.actors.directory)
        .with_peer_connector(driver.peer_connector());
        if let Some(controller) = contextual_controller {
            node_agent = node_agent.with_contextual_controller(controller);
        }
        let node_actor = match stack.runtime.spawn(node_agent) {
            Ok(actor) => {
                boot(
                    "node_agent",
                    "ready",
                    json!({"node_actor":actor,"source":"generated"}),
                )?;
                actor
            }
            Err(error) => {
                boot(
                    "node_agent",
                    "failed",
                    json!({"error":error.to_string(),"source":"generated"}),
                )?;
                return Err(format!("spawn node agent: {error}"));
            }
        };
        stack.register_local_actor(driver.register_actor(node_actor, 1));
        // Agent-only nodes do not launch the legacy Tinygrad child, but they still
        // need host CPU telemetry. The sampler's host totals are independent of
        // the watched process list; watch the node process itself for process detail.
        if config.agent_only {
            telemetry
                .resource_samples
                .lock()
                .push(spawn_host_cpu_sampler(
                    tasks.clone(),
                    engine.handle(),
                    Arc::clone(&telemetry.endpoint),
                    telemetry.channels.host_cpu,
                    sampler_health_channel,
                    sampler_health_context,
                    vec![std::process::id()],
                ));
        }
        stack.register_local_actor(driver.register_actor(rejoin_forwarder, 1));
        let pending_control_rejoin = PendingControlRejoin::new(
            &config,
            &advertised_self_endpoint,
            driver.node_id(),
            node_actor,
            orchestrator,
            rejoin_forwarder,
        )?;
        boot(
            "network_registration",
            "ready",
            json!({"node_actor":node_actor,"network_reachable":true}),
        )?;
        let peers = driver.peer_connector();
        let mut edge_port = driver.edge_port();
        let _transport = spawn_worker_transport(
            tasks.clone(),
            driver,
            Arc::clone(&telemetry.endpoint),
            runtime_wake.clone(),
            contextual_blob_receiver,
            stack.runtime.clone(),
        );

        if config.agent_only {
            boot(
                "agent_mode",
                "ready",
                json!({"framework":"none","workloads":"contextual_processes"}),
            )?;
            let pending_runtime_ready =
                PendingRuntimeReady::new(&config, advertised_self_endpoint.clone(), node_actor);
            let ready = json!({
                "type":"ready",
                "endpoint":advertised_self_endpoint,
                "node_actor":node_actor,
                "logical_node_id":config.logical_node_id,
                "stage_index":config.stage_index,
            });
            boot(
                "runtime_ready_local",
                "ready",
                json!({
                    "endpoint":advertised_self_endpoint,
                    "node_actor":node_actor,
                    "logical_node_id":config.logical_node_id,
                    "stage_index":config.stage_index,
                    "readiness_id":pending_runtime_ready.readiness_id,
                }),
            )?;
            node_runtime(
                &mut telemetry,
                "main_loop",
                "started",
                json!({
                    "mode":"agent_only",
                    "checks":["network","telemetry","node_reports","stdin_shutdown"],
                }),
            );
            let actor_runtime = stack.runtime.clone();
            let sender = actor_runtime.create_sender();
            let exit_on_stdin_eof = config.exit_on_stdin_eof;
            let completion = ActorCompletion::new();
            let runtime_actor = actor_runtime
                .spawn(AgentNodeRuntimeActor {
                    effects: AgentNodeRuntimeLive {
                        config,
                        telemetry,
                        peers,
                        stack,
                        debug_join_rx,
                        runtime_stats: runtime_stats.clone(),
                        pending_control_rejoin,
                        rejoin_replies,
                    },
                    reports,
                    pending_runtime_ready,
                    ready,
                    node_actor,
                    completion: completion.clone(),
                })
                .map_err(|error| format!("spawn agent node runtime actor: {error}"))?;
            *report_wake_target.lock() = Some(runtime_actor);
            let _ = sender.send_to(runtime_actor, NodeRuntimeMsg::RoutingChanged);
            let stop_actor = actor_runtime
                .spawn(StdinStopForwarder {
                    sender: sender.clone(),
                    target: runtime_actor,
                })
                .map_err(|error| format!("spawn stdin stop forwarder: {error}"))?;
            spawn_stdin_shutdown_listener(
                &engine.handle().blocking_work_sender(),
                exit_on_stdin_eof,
                sender,
                stop_actor,
            );
            return completion.wait();
        }

        worker_evt(
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
        let mut worker = match TinygradWorker::spawn(
            &config,
            arena_fd,
            runtime.clone(),
            engine.handle(),
            runtime_wake.clone(),
        ) {
            Ok(worker) => worker,
            Err(error) => {
                worker_evt("worker_process", "failed", json!({"error":error}))?;
                return Err(error);
            }
        };
        telemetry
            .resource_samples
            .lock()
            .push(spawn_host_cpu_sampler(
                tasks.clone(),
                engine.handle(),
                Arc::clone(&telemetry.endpoint),
                telemetry.channels.host_cpu,
                sampler_health_channel,
                sampler_health_context,
                vec![std::process::id(), worker.child.id()],
            ));
        worker_evt(
            "worker_initialize",
            "started",
            json!({"command":"InitializeWorker","helper_abi_version":1,"device":&config.device}),
        )?;
        match worker.initialize(&config.device, &config, &mut telemetry) {
            Ok(()) => worker_evt(
                "worker_initialize",
                "ready",
                json!({"worker_event_type":"WorkerReady"}),
            )?,
            Err(error) => {
                worker_evt("worker_initialize", "failed", json!({"error":error}))?;
                return Err(error);
            }
        }
        let edge_runtime = WorkerEdgeRuntime::new(config.logical_node_id);
        let pending_runtime_ready =
            PendingRuntimeReady::new(&config, advertised_self_endpoint.clone(), node_actor);

        let ready = json!({
            "type":"ready",
            "role":"node",
            "endpoint": advertised_self_endpoint.clone(),
            "node_actor": node_actor,
            "logical_node_id": config.logical_node_id,
            "stage_index": config.stage_index,
        });
        boot(
            "runtime_ready_local",
            "ready",
            json!({
                "endpoint":advertised_self_endpoint.clone(),
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
                &mut telemetry,
                &mut edge_port,
                &stack,
            )?;
        }

        node_runtime(
            &mut telemetry,
            "stdin_shutdown_listener",
            "ready",
            json!({"command":"shutdown"}),
        );
        node_runtime(
            &mut telemetry,
            "main_loop",
            "started",
            json!({
                "checks":["network","edge_streams","telemetry","node_reports","stdin_shutdown","worker_health"],
            }),
        );
        let actor_runtime = stack.runtime.clone();
        let sender = actor_runtime.create_sender();
        let exit_on_stdin_eof = config.exit_on_stdin_eof;
        let completion = ActorCompletion::new();
        let runtime_actor = actor_runtime
            .spawn(WorkerNodeRuntimeActor {
                effects: WorkerNodeRuntimeLive {
                    config,
                    telemetry,
                    driver: edge_port,
                    peers,
                    stack,
                    debug_join_rx,
                    runtime_stats: runtime_stats.clone(),
                    pending_control_rejoin,
                    rejoin_replies,
                    worker,
                    edge_runtime,
                    arena_manager,
                },
                reports,
                pending_runtime_ready,
                ready,
                node_actor,
                completion: completion.clone(),
            })
            .map_err(|error| format!("spawn worker node runtime actor: {error}"))?;
        *report_wake_target.lock() = Some(runtime_actor);
        let _ = sender.send_to(runtime_actor, NodeRuntimeMsg::RoutingChanged);
        let stop_actor = actor_runtime
            .spawn(StdinStopForwarder {
                sender: sender.clone(),
                target: runtime_actor,
            })
            .map_err(|error| format!("spawn stdin stop forwarder: {error}"))?;
        spawn_stdin_shutdown_listener(
            &engine.handle().blocking_work_sender(),
            exit_on_stdin_eof,
            sender,
            stop_actor,
        );
        completion.wait()
    }))
}
#[derive(Clone)]
struct RuntimeWake {
    sender: ExternalSender,
    target: Arc<Mutex<Option<ActorAddress>>>,
}

impl RuntimeWake {
    fn send(&self, message: NodeRuntimeMsg) {
        if let Some(target) = *self.target.lock() {
            let _ = self.sender.send_to(target, message);
        }
    }
}

struct RejoinReplyForwarder {
    destination: ActorAddress,
    wake: RuntimeWake,
}

impl ActorInterface for RejoinReplyForwarder {
    type Incoming = ManualControlReply;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, reply: Self::Incoming) {
        let _ = ctx.send(self.destination, reply);
        self.wake.send(NodeRuntimeMsg::Progress);
    }
}

struct HelperProcessForwarder {
    observation: Arc<Mutex<Option<swactor_process::ProcessExitObservation>>>,
    wake: RuntimeWake,
}

impl ActorInterface for HelperProcessForwarder {
    type Incoming = swactor_process::ProcessExitObservation;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, observation: Self::Incoming) {
        *self.observation.lock() = Some(observation);
        self.wake.send(NodeRuntimeMsg::Progress);
        ctx.stop_self();
    }
}

#[derive(Clone, Copy)]
enum NodeRuntimeMsg {
    Progress,
    RoutingChanged,
    Shutdown,
}

struct NodeReportWakeForwarder {
    destination: ActorAddress,
    target: Arc<Mutex<Option<ActorAddress>>>,
}

impl ActorInterface for NodeReportWakeForwarder {
    type Incoming = NodeAgentReport;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, report: NodeAgentReport) {
        let _ = ctx.send(self.destination, report);
        if let Some(target) = *self.target.lock() {
            let _ = ctx.send(target, NodeRuntimeMsg::Progress);
        }
    }
}

struct StdinStopForwarder {
    sender: ExternalSender,
    target: ActorAddress,
}

impl ActorInterface for StdinStopForwarder {
    type Incoming = swactor_process::ProcessStopSignal;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, _message: Self::Incoming) {
        let _ = self.sender.send_to(self.target, NodeRuntimeMsg::Shutdown);
        ctx.stop_self();
    }
}

trait AgentNodeRuntimeEffects: Send + 'static {
    fn tick_before_reports(&mut self, node_actor: ActorAddress) -> Result<(), String>;

    fn publish_runtime_ready(&mut self, pending: &PendingRuntimeReady, ready: &Value);

    fn tick_after_reports(
        &mut self,
        pending: &mut PendingRuntimeReady,
        node_actor: ActorAddress,
    ) -> Result<(), String>;

    fn shutdown(&mut self);
    fn routing_changed(&mut self) {}

    fn record_finish(&mut self, _result: &Result<(), String>) {}
}

struct AgentNodeRuntimeLive {
    config: DeploymentConfig,
    telemetry: NodeTelemetry,
    peers: PeerConnector,
    stack: DistributionRuntimeStack,
    debug_join_rx: Option<tokio::sync::mpsc::UnboundedReceiver<DebugJoinCommand>>,
    runtime_stats: RuntimeStatsInspector,
    pending_control_rejoin: PendingControlRejoin,
    rejoin_replies: Inbox<ManualControlReply>,
}

impl AgentNodeRuntimeEffects for AgentNodeRuntimeLive {
    fn routing_changed(&mut self) {
        self.pending_control_rejoin.pending_actor = None;
    }

    fn tick_before_reports(&mut self, node_actor: ActorAddress) -> Result<(), String> {
        emit_membership_telemetry(&mut self.telemetry, &self.stack);
        drain_debug_join_commands(
            &mut self.debug_join_rx,
            &self.peers,
            &self.config,
            &mut self.telemetry,
            &mut self.pending_control_rejoin,
            &self.runtime_stats,
        );
        self.pending_control_rejoin
            .drive(&self.stack, node_actor, &self.rejoin_replies)?;
        self.telemetry.observe_work();
        self.telemetry.tick();
        Ok(())
    }

    fn publish_runtime_ready(&mut self, pending: &PendingRuntimeReady, ready: &Value) {
        emit_node_event(
            &mut self.telemetry,
            &self.config,
            NODE_BOOTSTRAP_CHANNEL,
            "runtime_ready_ack",
            "ready",
            json!({
                "readiness_id":pending.readiness_id,
                "attempts":pending.attempts,
                "endpoint":&pending.endpoint,
                "node_actor":pending.node_actor,
            }),
        );
        self.telemetry
            .submit_value(self.telemetry.channels.node_ready, ready);
    }

    fn tick_after_reports(
        &mut self,
        pending: &mut PendingRuntimeReady,
        node_actor: ActorAddress,
    ) -> Result<(), String> {
        if !pending.membership_logged && pending.membership_ready(&self.stack) {
            emit_node_event(
                &mut self.telemetry,
                &self.config,
                NODE_RUNTIME_CHANNEL,
                "coordinator_membership",
                "ready",
                json!({
                    "coordinator":pending
                        .coordinator
                        .map(|node| format!("{node:?}"))
                        .unwrap_or_else(|| "standalone".to_owned()),
                    "readiness_id":pending.readiness_id,
                }),
            );
            pending.membership_logged = true;
        }
        if !pending.acked && pending.maybe_send(&self.stack, node_actor)? {
            emit_node_event(
                &mut self.telemetry,
                &self.config,
                NODE_RUNTIME_CHANNEL,
                "runtime_ready_signal",
                "sent",
                json!({
                    "readiness_id":pending.readiness_id,
                    "attempts":pending.attempts,
                }),
            );
        }
        Ok(())
    }

    fn shutdown(&mut self) {
        emit_node_event(
            &mut self.telemetry,
            &self.config,
            NODE_SHUTDOWN_CHANNEL,
            "node_exit",
            "ready",
            json!({"result":"ok","mode":"agent_only"}),
        );
    }

    fn record_finish(&mut self, result: &Result<(), String>) {
        if let Err(error) = result {
            emit_node_event(
                &mut self.telemetry,
                &self.config,
                NODE_SHUTDOWN_CHANNEL,
                "node_runtime",
                "failed",
                json!({"error":error}),
            );
        }
    }
}

struct AgentNodeRuntimeActor<E = AgentNodeRuntimeLive> {
    effects: E,
    reports: Inbox<NodeAgentReport>,
    pending_runtime_ready: PendingRuntimeReady,
    ready: Value,
    node_actor: ActorAddress,
    completion: ActorCompletion<Result<(), String>>,
}

impl<E: AgentNodeRuntimeEffects> AgentNodeRuntimeActor<E> {
    fn tick(&mut self) -> Result<(), String> {
        self.effects.tick_before_reports(self.node_actor)?;
        while let Some(report) = self.reports.try_recv() {
            if let NodeAgentReport::RuntimeReadyAck {
                run_id,
                node_id,
                stage_index,
                readiness_id,
            } = report
                && self.pending_runtime_ready.observe_ack(
                    run_id,
                    node_id,
                    stage_index,
                    readiness_id,
                )
            {
                self.effects
                    .publish_runtime_ready(&self.pending_runtime_ready, &self.ready);
            }
        }
        self.effects
            .tick_after_reports(&mut self.pending_runtime_ready, self.node_actor)
    }

    fn finish(&mut self, ctx: &Ctx, result: Result<(), String>) {
        self.effects.record_finish(&result);
        assert!(
            self.completion.complete(result).is_ok(),
            "agent node runtime completed twice"
        );
        ctx.stop_self();
    }
}

impl<E: AgentNodeRuntimeEffects> ActorInterface for AgentNodeRuntimeActor<E> {
    type Incoming = NodeRuntimeMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let _ = ctx.send(ctx.self_addr(), NodeRuntimeMsg::Progress);
    }

    fn handle(&mut self, ctx: &Ctx, message: Self::Incoming) {
        match message {
            NodeRuntimeMsg::Progress | NodeRuntimeMsg::RoutingChanged => {
                if matches!(message, NodeRuntimeMsg::RoutingChanged) {
                    self.pending_runtime_ready.sent = false;
                    self.effects.routing_changed();
                }
                if let Err(error) = self.tick() {
                    self.finish(ctx, Err(error));
                }
            }
            NodeRuntimeMsg::Shutdown => {
                self.effects.shutdown();
                self.finish(ctx, Ok(()));
            }
        }
    }
}

trait WorkerNodeRuntimeEffects: Send + 'static {
    fn tick_before_reports(&mut self, node_actor: ActorAddress) -> Result<(), String>;

    fn handle_report(
        &mut self,
        report: NodeAgentReport,
        node_actor: ActorAddress,
    ) -> Result<NodeReportOutcome, String>;

    fn publish_runtime_ready(&mut self, pending: &PendingRuntimeReady, ready: &Value);

    fn tick_after_reports(
        &mut self,
        pending: &mut PendingRuntimeReady,
        node_actor: ActorAddress,
    ) -> Result<(), String>;

    fn shutdown(&mut self) -> Result<(), String>;
    fn routing_changed(&mut self) {}

    fn record_finish(&mut self, _result: &Result<(), String>) {}
}

struct WorkerNodeRuntimeLive {
    config: DeploymentConfig,
    telemetry: NodeTelemetry,
    driver: IrohEdgePort,
    peers: PeerConnector,
    stack: DistributionRuntimeStack,
    debug_join_rx: Option<tokio::sync::mpsc::UnboundedReceiver<DebugJoinCommand>>,
    runtime_stats: RuntimeStatsInspector,
    pending_control_rejoin: PendingControlRejoin,
    rejoin_replies: Inbox<ManualControlReply>,
    worker: TinygradWorker,
    edge_runtime: WorkerEdgeRuntime,
    arena_manager: Arc<Mutex<arena::ArenaManager>>,
}

impl WorkerNodeRuntimeEffects for WorkerNodeRuntimeLive {
    fn routing_changed(&mut self) {
        self.pending_control_rejoin.pending_actor = None;
    }

    fn tick_before_reports(&mut self, node_actor: ActorAddress) -> Result<(), String> {
        emit_membership_telemetry(&mut self.telemetry, &self.stack);
        drain_debug_join_commands(
            &mut self.debug_join_rx,
            &self.peers,
            &self.config,
            &mut self.telemetry,
            &mut self.pending_control_rejoin,
            &self.runtime_stats,
        );
        self.pending_control_rejoin
            .drive(&self.stack, node_actor, &self.rejoin_replies)?;
        self.telemetry.observe_work();
        self.telemetry.tick();
        drain_worker_stderr(&self.worker.stderr_rx, &self.config, &mut self.telemetry);
        self.edge_runtime.poll_iroh(&mut WorkerRuntimeContext {
            config: &self.config,
            stack: &self.stack,
            driver: &mut self.driver,
            node_actor,
            worker: &mut self.worker,
            arena_manager: &self.arena_manager,
            telemetry: &mut self.telemetry,
        })
    }

    fn handle_report(
        &mut self,
        report: NodeAgentReport,
        node_actor: ActorAddress,
    ) -> Result<NodeReportOutcome, String> {
        handle_node_report(
            report,
            WorkerStageServices {
                config: &self.config,
                stack: &self.stack,
                node_actor,
                arena_manager: &self.arena_manager,
            },
            &mut self.driver,
            &mut self.worker,
            &mut self.edge_runtime,
            &mut self.telemetry,
        )
    }

    fn publish_runtime_ready(&mut self, pending: &PendingRuntimeReady, ready: &Value) {
        emit_node_event(
            &mut self.telemetry,
            &self.config,
            NODE_BOOTSTRAP_CHANNEL,
            "runtime_ready_ack",
            "ready",
            json!({
                "readiness_id":pending.readiness_id,
                "attempts":pending.attempts,
                "ready":ready,
            }),
        );
        emit_node_event(
            &mut self.telemetry,
            &self.config,
            NODE_BOOTSTRAP_CHANNEL,
            "telemetry_handoff",
            "ready",
            json!({"from":"runtime_ready_ack","to":"cluster_telemetry","channel":"myelin.node.ready"}),
        );
    }

    fn tick_after_reports(
        &mut self,
        pending: &mut PendingRuntimeReady,
        node_actor: ActorAddress,
    ) -> Result<(), String> {
        if !pending.membership_logged && pending.membership_ready(&self.stack) {
            emit_node_event(
                &mut self.telemetry,
                &self.config,
                NODE_RUNTIME_CHANNEL,
                "coordinator_membership",
                "ready",
                json!({
                    "coordinator":pending
                        .coordinator
                        .map(|node| format!("{node:?}"))
                        .unwrap_or_else(|| "standalone".to_owned()),
                    "readiness_id":pending.readiness_id,
                }),
            );
            pending.membership_logged = true;
        }
        if !pending.acked && pending.maybe_send(&self.stack, node_actor)? {
            emit_node_event(
                &mut self.telemetry,
                &self.config,
                NODE_RUNTIME_CHANNEL,
                "runtime_ready_signal",
                "sent",
                json!({
                    "readiness_id":pending.readiness_id,
                    "attempts":pending.attempts,
                }),
            );
        }
        if let Some(status) = self.worker.observe_exit()? {
            emit_node_event(
                &mut self.telemetry,
                &self.config,
                NODE_SHUTDOWN_CHANNEL,
                "worker_process",
                "failed",
                json!({"exit_status":status.to_string()}),
            );
            let _ = self.stack.runtime.send_to(
                node_actor,
                NodeAgentMsg::WorkerCrashed {
                    reason: Some(format!("tinygrad helper exited with {status}")),
                },
            );
            return Err(format!("tinygrad helper exited with {status}"));
        }
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), String> {
        emit_node_event(
            &mut self.telemetry,
            &self.config,
            NODE_SHUTDOWN_CHANNEL,
            "shutdown",
            "started",
            json!({"source":"stdin","command":"shutdown"}),
        );
        let result = self.worker.shutdown(&self.config, &mut self.telemetry);
        match &result {
            Ok(()) => {
                emit_node_event(
                    &mut self.telemetry,
                    &self.config,
                    NODE_SHUTDOWN_CHANNEL,
                    "worker_shutdown",
                    "ready",
                    json!({"worker_event_type":"WorkerStopped"}),
                );
                emit_node_event(
                    &mut self.telemetry,
                    &self.config,
                    NODE_SHUTDOWN_CHANNEL,
                    "node_exit",
                    "ready",
                    json!({"result":"ok"}),
                );
            }
            Err(error) => emit_node_event(
                &mut self.telemetry,
                &self.config,
                NODE_SHUTDOWN_CHANNEL,
                "worker_shutdown",
                "failed",
                json!({"error":error}),
            ),
        }
        result
    }
}

struct WorkerNodeRuntimeActor<E = WorkerNodeRuntimeLive> {
    effects: E,
    reports: Inbox<NodeAgentReport>,
    pending_runtime_ready: PendingRuntimeReady,
    ready: Value,
    node_actor: ActorAddress,
    completion: ActorCompletion<Result<(), String>>,
}

impl<E: WorkerNodeRuntimeEffects> WorkerNodeRuntimeActor<E> {
    fn tick(&mut self) -> Result<(), String> {
        self.effects.tick_before_reports(self.node_actor)?;
        while let Some(report) = self.reports.try_recv() {
            match self.effects.handle_report(report, self.node_actor)? {
                NodeReportOutcome::None => {}
                NodeReportOutcome::RuntimeReadyAck {
                    run_id,
                    node_id,
                    stage_index,
                    readiness_id,
                } => {
                    if self.pending_runtime_ready.observe_ack(
                        run_id,
                        node_id,
                        stage_index,
                        readiness_id,
                    ) {
                        self.effects
                            .publish_runtime_ready(&self.pending_runtime_ready, &self.ready);
                    }
                }
            }
        }
        self.effects
            .tick_after_reports(&mut self.pending_runtime_ready, self.node_actor)
    }

    fn finish(&mut self, ctx: &Ctx, result: Result<(), String>) {
        self.effects.record_finish(&result);
        assert!(
            self.completion.complete(result).is_ok(),
            "worker node runtime completed twice"
        );
        ctx.stop_self();
    }
}

impl<E: WorkerNodeRuntimeEffects> ActorInterface for WorkerNodeRuntimeActor<E> {
    type Incoming = NodeRuntimeMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let _ = ctx.send(ctx.self_addr(), NodeRuntimeMsg::Progress);
    }

    fn handle(&mut self, ctx: &Ctx, message: Self::Incoming) {
        match message {
            NodeRuntimeMsg::Progress | NodeRuntimeMsg::RoutingChanged => {
                if matches!(message, NodeRuntimeMsg::RoutingChanged) {
                    self.pending_runtime_ready.sent = false;
                    self.effects.routing_changed();
                }
                if let Err(error) = self.tick() {
                    self.finish(ctx, Err(error));
                }
            }
            NodeRuntimeMsg::Shutdown => {
                let result = self.effects.shutdown();
                self.finish(ctx, result);
            }
        }
    }
}

fn emit_membership_telemetry(telemetry: &mut NodeTelemetry, stack: &DistributionRuntimeStack) {
    for transition in stack.drain_membership_transitions() {
        let record = stack.membership_transition(&transition);
        telemetry
            .producer
            .submit_record(telemetry.channels.membership, &record);
    }
}

#[derive(Clone, Copy)]
struct TelemetryChannelSet {
    node_ready: ChannelId,
    node_lifecycle: ChannelId,
    node_self_test: ChannelId,
    worker_stderr: ChannelId,
    host_cpu: ChannelId,
    host_gpu: ChannelId,
    host_memory: ChannelId,
    membership: ChannelId,
    host_net: ChannelId,
    host_storage: ChannelId,
    arena: ChannelId,
}

struct NodeTelemetry {
    endpoint: Arc<TelemetryEndpoint>,
    producer: TelemetryProducer,
    channels: TelemetryChannelSet,
    resource_samples: Arc<Mutex<Vec<tokio::sync::mpsc::Sender<()>>>>,
    arena_sampler: Option<ArenaSampler>,
    by_name: BTreeMap<String, ChannelId>,
    by_id: BTreeMap<ChannelId, String>,
}

impl NodeTelemetry {
    fn new(config: &DeploymentConfig) -> Result<Self, String> {
        // Lifetimes are ordered generations, not random readiness identities.
        // Persist the next one before any producer can emit startup records.
        let state_root = env_optional("XDG_STATE_HOME")
            .map(PathBuf::from)
            .or_else(|| env_optional("HOME").map(|home| PathBuf::from(home).join(".local/state")))
            .ok_or_else(|| "telemetry requires XDG_STATE_HOME or HOME for node state".to_owned())?
            .join("myelin/telemetry");
        let lifetime =
            allocate_telemetry_lifetime(&state_root, config.logical_node_id, config.run_id)?;
        let stream = StreamId::new(
            NodeId::new(config.logical_node_id.to_string()),
            Lifetime(lifetime),
        );
        let endpoint = Arc::new(
            TelemetryEndpoint::with_descriptor(
                StreamDescriptor {
                    stream: stream.clone(),
                    label: Some("myelin worker node".to_owned()),
                    origin: StreamOrigin::RemoteNode,
                },
                16_384,
                16_384,
            )
            .with_retention(16_384, 16 * 1024 * 1024),
        );
        let producer = endpoint.producer();
        let mut by_name = BTreeMap::new();
        let mut by_id = BTreeMap::new();

        for name in [
            NODE_BOOTSTRAP_CHANNEL,
            NODE_RUNTIME_CHANNEL,
            NODE_STAGE_CHANNEL,
            NODE_WORKER_CHANNEL,
            NODE_PROMPT_CHANNEL,
            NODE_SHUTDOWN_CHANNEL,
            NODE_SAMPLER_CHANNEL,
            "myelin.worker.initialize",
            "myelin.worker.role",
            "myelin.worker.weights",
            "myelin.worker.prompt",
            "myelin.worker.tokenizer",
            "myelin.worker.ring",
            "myelin.worker.ingress",
            "myelin.worker.step",
            "myelin.worker.device_object",
            "myelin.worker.shutdown",
        ] {
            register_messagepack_channel(&producer, &mut by_name, &mut by_id, name);
        }

        let channels = TelemetryChannelSet {
            node_ready: register_messagepack_channel(
                &producer,
                &mut by_name,
                &mut by_id,
                "myelin.node.ready",
            ),
            node_lifecycle: register_messagepack_channel(
                &producer,
                &mut by_name,
                &mut by_id,
                "myelin.node.lifecycle",
            ),
            node_self_test: register_messagepack_channel(
                &producer,
                &mut by_name,
                &mut by_id,
                "myelin.node.self_test",
            ),
            worker_stderr: register_messagepack_channel(
                &producer,
                &mut by_name,
                &mut by_id,
                "myelin.worker.stderr",
            ),
            host_cpu: register_record_channel::<telemetry::hardware::cpu::HostCpuSample>(
                &producer,
                &mut by_name,
                &mut by_id,
            ),
            host_gpu: register_record_channel::<telemetry::hardware::gpu::HostGpuSample>(
                &producer,
                &mut by_name,
                &mut by_id,
            ),
            host_memory: register_record_channel::<telemetry::hardware::memory::HostMemorySample>(
                &producer,
                &mut by_name,
                &mut by_id,
            ),
            host_net: register_record_channel::<telemetry::hardware::net::HostNetSample>(
                &producer,
                &mut by_name,
                &mut by_id,
            ),
            host_storage: register_record_channel::<telemetry::hardware::storage::HostStorageSample>(
                &producer,
                &mut by_name,
                &mut by_id,
            ),
            arena: register_record_channel::<arena::ArenaSample>(
                &producer,
                &mut by_name,
                &mut by_id,
            ),
            membership: register_record_channel::<TransportMembershipTransition>(
                &producer,
                &mut by_name,
                &mut by_id,
            ),
        };

        Ok(Self {
            endpoint,
            producer,
            channels,
            by_name,
            by_id,
            resource_samples: Arc::new(Mutex::new(Vec::new())),
            arena_sampler: None,
        })
    }

    fn channel_by_name(&mut self, name: &str) -> ChannelId {
        if let Some(id) = self.by_name.get(name).copied() {
            return id;
        }
        register_messagepack_channel(&self.producer, &mut self.by_name, &mut self.by_id, name)
    }

    fn submit_value(&self, channel: ChannelId, value: &Value) {
        let payload = encode_record(value).expect("serialize node telemetry record");
        self.producer.submit_bytes(channel, payload);
    }

    fn tick(&mut self) {
        self.endpoint.tick();
    }

    fn observe_work(&mut self) {
        for sampler in self.resource_samples.lock().iter() {
            let _ = sampler.try_send(());
        }
        if let Some(sampler) = &mut self.arena_sampler {
            sampler.observe_progress();
        }
    }
}

/// Own the live driver and telemetry connection tasks outside all actors.
fn spawn_worker_transport(
    tasks: TaskSpawner,
    driver: IrohDriver,
    endpoint: Arc<TelemetryEndpoint>,
    wake: RuntimeWake,
    blob_receiver: Option<Arc<IrohBlobTransferReceiver>>,
    runtime: Runtime,
) -> WorkerTransport {
    let mut ingress = driver.ingress_changed();
    let mut edges = driver.edge_events_changed();
    let (cancel, mut cancelled) = tokio::sync::oneshot::channel();
    let completion = ActorCompletion::new();
    let finished = completion.clone();
    let connection_tasks = tasks.clone();
    tasks.spawn(async move {
        loop {
            if let Some(receiver) = &blob_receiver {
                receiver.drain(&runtime);
            }
            for (_node, connection) in driver.drain_accepted_for_alpn(TELEMETRY_ALPN) {
                spawn_pull_server(&connection_tasks, connection, Arc::clone(&endpoint));
            }
            tokio::select! {
                _ = &mut cancelled => break,
                changed = ingress.changed() => {
                    if changed.is_err() { break; }
                }
                changed = edges.changed() => {
                    if changed.is_err() { break; }
                    wake.send(NodeRuntimeMsg::Progress);
                }
            }
        }
        driver.close().await;
        let _ = finished.complete(());
    });
    WorkerTransport {
        cancel: Some(cancel),
        completion,
    }
}

struct WorkerTransport {
    cancel: Option<tokio::sync::oneshot::Sender<()>>,
    completion: ActorCompletion<()>,
}

impl Drop for WorkerTransport {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        self.completion.wait();
    }
}

fn register_messagepack_channel(
    producer: &TelemetryProducer,
    by_name: &mut BTreeMap<String, ChannelId>,
    by_id: &mut BTreeMap<ChannelId, String>,
    name: &str,
) -> ChannelId {
    let id = producer.register_channel(
        name,
        ChannelContent::MessagePackRecord {
            schema: Some(name.to_owned()),
        },
    );
    by_name.insert(name.to_owned(), id);
    by_id.insert(id, name.to_owned());
    id
}

fn register_record_channel<R: Record>(
    producer: &TelemetryProducer,
    by_name: &mut BTreeMap<String, ChannelId>,
    by_id: &mut BTreeMap<ChannelId, String>,
) -> ChannelId {
    let id = producer.register_record::<R>();
    by_name.insert(R::CHANNEL.to_owned(), id);
    by_id.insert(id, R::CHANNEL.to_owned());
    id
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

struct PendingControlRejoin {
    hello: RejoinHello,
    last_bound_actor: ActorAddress,
    pending_actor: Option<ActorAddress>,
    recovery_actor: Option<ActorAddress>,
    reply_to: ActorAddress,
    next_request_id: u64,
    pending_request_id: Option<u64>,
}

impl PendingControlRejoin {
    fn new(
        config: &DeploymentConfig,
        endpoint: &EndpointAddr,
        swim_node_id: DistNodeId,
        node_actor: ActorAddress,
        orchestrator_actor: ActorAddress,
        reply_to: ActorAddress,
    ) -> Result<Self, String> {
        Ok(Self {
            hello: RejoinHello {
                run_id: config.run_id,
                logical_node_id: config.logical_node_id,
                attempt_id: config.attempt_id,
                readiness_id: config.readiness_id,
                selected_offer_id: config.selected_offer_id,
                endpoint: serde_json::to_string(endpoint)
                    .map_err(|error| format!("serialize rejoin endpoint: {error}"))?,
                swim_node_id,
                stage_index: config.stage_index,
                node_actor,
                artifact_digest: config
                    .deployment
                    .as_ref()
                    .map(|identity| identity.artifact_digest.clone()),
                deployment_generation: config
                    .deployment
                    .as_ref()
                    .map(|identity| identity.deployment_generation.clone()),
            },
            last_bound_actor: orchestrator_actor,
            pending_actor: None,
            recovery_actor: None,
            reply_to,
            next_request_id: 0,
            pending_request_id: None,
        })
    }

    fn set_recovery_actor(&mut self, actor: ActorAddress) {
        self.recovery_actor = Some(actor);
        self.pending_actor = None;
    }

    fn current_actor(&self, stack: &DistributionRuntimeStack) -> Option<ActorAddress> {
        self.recovery_actor.or_else(|| {
            stack
                .registry_view
                .read()
                .ok()?
                .entries
                .iter()
                .find(|entry| entry.name == CONTROL_REGISTRY_NAME && !entry.tombstone)
                .map(|entry| entry.actor_addr)
        })
    }

    fn drive(
        &mut self,
        stack: &DistributionRuntimeStack,
        node_actor: ActorAddress,
        replies: &Inbox<ManualControlReply>,
    ) -> Result<(), String> {
        while let Some(reply) = replies.try_recv() {
            let ManualControlReply::RejoinResolved { request_id, result } = reply else {
                continue;
            };
            if self.pending_request_id != Some(request_id)
                || self.pending_actor.is_none()
                || self.current_actor(stack) != self.pending_actor
            {
                continue;
            }
            let binding = result.map_err(|reason| format!("control rejoin rejected: {reason}"))?;
            if self.pending_actor != Some(binding.orchestrator_actor) {
                continue;
            }
            stack
                .runtime
                .send_to(
                    node_actor,
                    NodeAgentMsg::RebindOrchestrator {
                        orchestrator_actor: binding.orchestrator_actor,
                        control_generation: binding.control_generation,
                    },
                )
                .map_err(|error| format!("apply orchestrator rebind: {error}"))?;
            self.last_bound_actor = binding.orchestrator_actor;
            self.pending_actor = None;
            self.pending_request_id = None;
        }

        let Some(orchestrator_actor) = self.current_actor(stack) else {
            return Ok(());
        };
        if orchestrator_actor == self.last_bound_actor {
            self.pending_actor = None;
            return Ok(());
        }
        if self.pending_actor == Some(orchestrator_actor) {
            return Ok(());
        }
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or_else(|| "control rejoin request identity exhausted".to_owned())?;
        if stack
            .runtime
            .send_to(
                orchestrator_actor,
                OrchestratorMsg::Manual(ManualControlMsg::Rejoin {
                    request_id: self.next_request_id,
                    hello: self.hello.clone(),
                    reply_to: self.reply_to,
                }),
            )
            .is_ok()
        {
            self.pending_actor = Some(orchestrator_actor);
            self.pending_request_id = Some(self.next_request_id);
        } else {
            self.pending_actor = None;
            self.pending_request_id = None;
        }
        Ok(())
    }
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
    sent: bool,
    acked: bool,
    membership_logged: bool,
    deployment: Option<DeploymentIdentity>,
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
            readiness_id: config.readiness_id,
            attempts: 0,
            sent: false,
            acked: false,
            membership_logged: false,
            deployment: config.deployment.clone(),
        }
    }

    fn membership_ready(&self, stack: &DistributionRuntimeStack) -> bool {
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
        if self.acked || !self.membership_ready(stack) {
            return Ok(false);
        }
        if self.sent {
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
                    artifact_digest: self
                        .deployment
                        .as_ref()
                        .map(|identity| identity.artifact_digest.clone()),
                    deployment_generation: self
                        .deployment
                        .as_ref()
                        .map(|identity| identity.deployment_generation.clone()),
                },
            )
            .map_err(|error| format!("signal runtime loaded: {error}"))?;
        self.attempts = self.attempts.saturating_add(1);
        self.sent = true;
        Ok(true)
    }
}

fn handle_node_report(
    report: NodeAgentReport,
    services: WorkerStageServices<'_>,
    driver: &mut IrohEdgePort,
    worker: &mut TinygradWorker,
    edge_runtime: &mut WorkerEdgeRuntime,
    telemetry: &mut NodeTelemetry,
) -> Result<NodeReportOutcome, String> {
    let WorkerStageServices {
        config,
        stack,
        node_actor,
        arena_manager,
    } = services;
    let node_stage = |ds: &mut NodeTelemetry, phase: &str, status: &str, detail: Value| {
        emit_node_event(ds, config, NODE_STAGE_CHANNEL, phase, status, detail)
    };
    let kind = match &report {
        NodeAgentReport::Command(_) => "Command",
        NodeAgentReport::Lifecycle(_) => "Lifecycle",
        NodeAgentReport::PromptRequested { .. } => "PromptRequested",
        NodeAgentReport::EncodePromptRequested { .. } => "EncodePromptRequested",
        NodeAgentReport::DecodeTokensRequested { .. } => "DecodeTokensRequested",
        NodeAgentReport::RuntimeReadyAck { .. } => "RuntimeReadyAck",
        NodeAgentReport::Snapshot { .. } => "Snapshot",
    };
    emit_node_event(
        telemetry,
        config,
        NODE_RUNTIME_CHANNEL,
        "node_report",
        "observed",
        json!({"kind":kind}),
    );
    match report {
        NodeAgentReport::Command(command) => {
            handle_stage_command(
                *command,
                WorkerStageServices {
                    config,
                    stack,
                    node_actor,
                    arena_manager,
                },
                driver,
                worker,
                edge_runtime,
                telemetry,
            )?;
            Ok(NodeReportOutcome::None)
        }
        NodeAgentReport::Lifecycle(event) => {
            let event = format!("{event:?}");
            let payload = json!({"type":"node_lifecycle","event":event});
            telemetry.submit_value(telemetry.channels.node_lifecycle, &payload);
            node_stage(telemetry, "lifecycle", "observed", json!({"event":event}));
            Ok(NodeReportOutcome::None)
        }
        NodeAgentReport::PromptRequested {
            request_id,
            prompt,
            max_tokens,
            reply_to,
        } => {
            handle_prompt_request(
                PromptRequest {
                    request_id,
                    prompt,
                    max_tokens,
                    reply_to,
                },
                config,
                stack,
                worker,
                telemetry,
            )?;
            Ok(NodeReportOutcome::None)
        }
        NodeAgentReport::EncodePromptRequested {
            request_id,
            prompt,
            reply_to,
        } => {
            handle_encode_prompt_request(
                EncodePromptRequest {
                    request_id,
                    prompt,
                    reply_to,
                },
                config,
                stack,
                worker,
                telemetry,
            )?;
            Ok(NodeReportOutcome::None)
        }
        NodeAgentReport::DecodeTokensRequested {
            request_id,
            tokens,
            reply_to,
        } => {
            handle_decode_tokens_request(
                DecodeTokensRequest {
                    request_id,
                    tokens,
                    reply_to,
                },
                config,
                stack,
                worker,
                telemetry,
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
    request: PromptRequest,
    config: &DeploymentConfig,
    stack: &DistributionRuntimeStack,
    worker: &mut TinygradWorker,
    telemetry: &mut NodeTelemetry,
) -> Result<(), String> {
    let PromptRequest {
        request_id,
        prompt,
        max_tokens,
        reply_to,
    } = request;
    let node_prompt = |ds: &mut NodeTelemetry, phase: &str, status: &str, detail: Value| {
        emit_node_event(ds, config, NODE_PROMPT_CHANNEL, phase, status, detail)
    };

    node_prompt(
        telemetry,
        "prompt_requested",
        "started",
        json!({"request_id":request_id,"max_tokens":max_tokens,"reply_to":reply_to,"prompt_bytes":prompt.len()}),
    );
    node_prompt(
        telemetry,
        "infer_prompt",
        "started",
        json!({"request_id":request_id,"command":"InferPrompt","max_tokens":max_tokens}),
    );
    match worker.infer_prompt(request_id, &prompt, max_tokens, config, telemetry) {
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
            node_prompt(
                telemetry,
                "infer_prompt",
                "ready",
                json!({"request_id":request_id,"worker_event_type":"PromptCompleted","prompt_tokens":prompt_tokens,"tokens_generated":tokens_generated,"text_bytes":text_bytes,"worker_result_payload_bytes":worker_result_payload_bytes}),
            );
            if !text.is_empty() {
                match stack.runtime.send_to(
                    reply_to,
                    PromptEvent::TextDelta {
                        request_id,
                        text: text.clone(),
                    },
                ) {
                    Ok(()) => node_prompt(
                        telemetry,
                        "prompt_response",
                        "ready",
                        json!({"request_id":request_id,"event":"TextDelta","bytes":text_bytes,"reply_to":reply_to}),
                    ),
                    Err(error) => {
                        node_prompt(
                            telemetry,
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
                },
            ) {
                Ok(()) => {
                    node_prompt(
                        telemetry,
                        "prompt_response",
                        "ready",
                        json!({"request_id":request_id,"event":"Done","tokens_generated":tokens_generated,"final_text_bytes":text_bytes,"reply_to":reply_to}),
                    );
                    Ok(())
                }
                Err(error) => {
                    node_prompt(
                        telemetry,
                        "prompt_response",
                        "failed",
                        json!({"request_id":request_id,"event":"Done","error":error.to_string()}),
                    );
                    Err(format!("send prompt done: {error}"))
                }
            }
        }
        Err(error) => {
            node_prompt(
                telemetry,
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
                    node_prompt(
                        telemetry,
                        "prompt_response",
                        "ready",
                        json!({"request_id":request_id,"event":"Fault","reply_to":reply_to}),
                    );
                    Ok(())
                }
                Err(send_error) => {
                    node_prompt(
                        telemetry,
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

fn handle_encode_prompt_request(
    request: EncodePromptRequest,
    config: &DeploymentConfig,
    stack: &DistributionRuntimeStack,
    worker: &mut TinygradWorker,
    telemetry: &mut NodeTelemetry,
) -> Result<(), String> {
    let EncodePromptRequest {
        request_id,
        prompt,
        reply_to,
    } = request;
    let node_prompt = |ds: &mut NodeTelemetry, phase: &str, status: &str, detail: Value| {
        emit_node_event(ds, config, NODE_PROMPT_CHANNEL, phase, status, detail)
    };
    node_prompt(
        telemetry,
        "encode_prompt",
        "started",
        json!({"request_id":request_id,"prompt_bytes":prompt.len(),"reply_to":reply_to}),
    );
    let event = match worker.encode_prompt(request_id, &prompt, config, telemetry) {
        Ok(tokens) => {
            node_prompt(
                telemetry,
                "encode_prompt",
                "ready",
                json!({"request_id":request_id,"tokens":tokens.len(),"reply_to":reply_to}),
            );
            TokenizerEvent::PromptEncoded { request_id, tokens }
        }
        Err(error) => {
            node_prompt(
                telemetry,
                "encode_prompt",
                "failed",
                json!({"request_id":request_id,"error":error,"reply_to":reply_to}),
            );
            TokenizerEvent::Fault { request_id, error }
        }
    };
    stack
        .runtime
        .send_to(reply_to, event)
        .map_err(|e| format!("send tokenizer encode response: {e}"))
}

fn handle_decode_tokens_request(
    request: DecodeTokensRequest,
    config: &DeploymentConfig,
    stack: &DistributionRuntimeStack,
    worker: &mut TinygradWorker,
    telemetry: &mut NodeTelemetry,
) -> Result<(), String> {
    let DecodeTokensRequest {
        request_id,
        tokens,
        reply_to,
    } = request;
    let node_prompt = |ds: &mut NodeTelemetry, phase: &str, status: &str, detail: Value| {
        emit_node_event(ds, config, NODE_PROMPT_CHANNEL, phase, status, detail)
    };
    node_prompt(
        telemetry,
        "decode_tokens",
        "started",
        json!({"request_id":request_id,"tokens":tokens.len(),"reply_to":reply_to}),
    );
    let event = match worker.decode_tokens(request_id, &tokens, config, telemetry) {
        Ok(text) => {
            node_prompt(
                telemetry,
                "decode_tokens",
                "ready",
                json!({"request_id":request_id,"tokens":tokens.len(),"text_bytes":text.len(),"reply_to":reply_to}),
            );
            TokenizerEvent::TokensDecoded { request_id, text }
        }
        Err(error) => {
            node_prompt(
                telemetry,
                "decode_tokens",
                "failed",
                json!({"request_id":request_id,"error":error,"reply_to":reply_to}),
            );
            TokenizerEvent::Fault { request_id, error }
        }
    };
    stack
        .runtime
        .send_to(reply_to, event)
        .map_err(|e| format!("send tokenizer decode response: {e}"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StageShardProcessStream {
    Stdout,
    Stderr,
}

#[derive(Clone)]
enum StageShardFetchMsg {
    Start,
    ProcessLine {
        stream: StageShardProcessStream,
        line: String,
    },
    ReaderError {
        stream: StageShardProcessStream,
        error: String,
    },
    ReaderClosed {
        stream: StageShardProcessStream,
    },
    ProcessExited(std::process::ExitStatus),
    ProcessFailed(String),
}

struct StageShardFetchOutcome {
    events: Vec<Value>,
    result: Result<PathBuf, String>,
}

struct StageShardFetchActor {
    request_json: Vec<u8>,
    output_path: PathBuf,
    completion: ActorCompletion<StageShardFetchOutcome>,
    sender: ExternalSender,
    engine: EngineHandle,
    child: Option<swactor_process::SupervisedChild>,
    process_relay: Option<ActorAddress>,
    stdout_closed: bool,
    stderr_closed: bool,
    ready_path: Option<PathBuf>,
    exit_status: Option<std::process::ExitStatus>,
    events: Vec<Value>,
    finished: bool,
}

impl StageShardFetchActor {
    fn new(
        request_json: Vec<u8>,
        output_path: PathBuf,
        completion: ActorCompletion<StageShardFetchOutcome>,
        sender: ExternalSender,
        engine: EngineHandle,
    ) -> Self {
        Self {
            request_json,
            output_path,
            completion,
            sender,
            engine,
            child: None,
            process_relay: None,
            stdout_closed: true,
            stderr_closed: true,
            ready_path: None,
            exit_status: None,
            events: Vec::new(),
            finished: false,
        }
    }

    fn start_fetch(&mut self, ctx: &Ctx) {
        if self.finished || self.child.is_some() {
            return;
        }
        let exe = match std::env::current_exe() {
            Ok(exe) => exe,
            Err(error) => {
                self.fail(ctx, format!("locate worker node executable: {error}"));
                return;
            }
        };
        let relay = match ctx.spawn(StageShardProcessForwarder {
            target: ctx.self_addr(),
        }) {
            Ok(relay) => relay,
            Err(error) => {
                self.fail(ctx, format!("spawn stage shard observer: {error}"));
                return;
            }
        };
        self.process_relay = Some(relay);
        self.stdout_closed = false;
        self.stderr_closed = false;
        match swactor_process::SupervisedChild::spawn(
            &self.engine.blocking_work_sender(),
            Command::new(exe).arg("stage-shard-fetcher"),
            Some(Arc::from(self.request_json.clone())),
            self.sender.clone(),
            relay,
            1,
        ) {
            Ok(child) => self.child = Some(child),
            Err(error) => self.fail(ctx, format!("spawn stage shard fetcher: {error}")),
        }
    }

    fn handle_process_exit(&mut self, ctx: &Ctx, status: std::process::ExitStatus) {
        if self.finished || self.exit_status.is_some() {
            return;
        }
        self.exit_status = Some(status);
        self.maybe_finish(ctx);
    }

    fn handle_line(&mut self, _ctx: &Ctx, stream: StageShardProcessStream, line: String) {
        if line.is_empty() || self.finished {
            return;
        }
        let event = match stream {
            StageShardProcessStream::Stdout => match serde_json::from_str::<Value>(&line) {
                Ok(value) => value,
                Err(error) => {
                    json!({"type":"StageShardFetchOutputParseFailed","line":line,"error":error.to_string()})
                }
            },
            StageShardProcessStream::Stderr => json!({"type":"StageShardFetchStderr","line":line}),
        };
        if event.get("type").and_then(Value::as_str) == Some("StageShardReady") {
            self.ready_path = event
                .get("path")
                .and_then(Value::as_str)
                .map(PathBuf::from)
                .or_else(|| Some(self.output_path.clone()));
        }
        self.events.push(event);
    }

    fn handle_reader_error(&mut self, ctx: &Ctx, stream: StageShardProcessStream, error: String) {
        self.handle_line(ctx, stream, format!("reader error: {error}"));
    }

    fn handle_reader_closed(&mut self, ctx: &Ctx, stream: StageShardProcessStream) {
        match stream {
            StageShardProcessStream::Stdout => self.stdout_closed = true,
            StageShardProcessStream::Stderr => self.stderr_closed = true,
        }
        self.maybe_finish(ctx);
    }

    fn maybe_finish(&mut self, ctx: &Ctx) {
        if self.finished || self.exit_status.is_none() || !self.stdout_closed || !self.stderr_closed
        {
            return;
        }
        self.release_process(ctx);
        let status = self.exit_status.take().expect("exit status checked");
        if status.success() {
            let path = self
                .ready_path
                .clone()
                .unwrap_or_else(|| self.output_path.clone());
            if path.is_file() {
                self.finished = true;
                assert!(
                    self.completion
                        .complete(StageShardFetchOutcome {
                            events: std::mem::take(&mut self.events),
                            result: Ok(path),
                        })
                        .is_ok(),
                    "stage shard fetch completed twice"
                );
                ctx.stop_self();
                return;
            }
            self.fail(
                ctx,
                format!(
                    "stage shard fetcher exited successfully but {} is missing",
                    path.display()
                ),
            );
            return;
        }
        self.fail(ctx, format!("stage shard fetcher exited with {status}"));
    }

    fn fail(&mut self, ctx: &Ctx, error: String) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.release_process(ctx);
        assert!(
            self.completion
                .complete(StageShardFetchOutcome {
                    events: std::mem::take(&mut self.events),
                    result: Err(error),
                })
                .is_ok(),
            "stage shard fetch completed twice"
        );
        ctx.stop_self();
    }

    fn release_process(&mut self, ctx: &Ctx) {
        // The supervisor reaps the child group and joins both readers on drop.
        self.child = None;
        if let Some(relay) = self.process_relay.take() {
            let _ = ctx.stop_actor(relay);
        }
    }
}

impl ActorInterface for StageShardFetchActor {
    type Incoming = StageShardFetchMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        match msg {
            StageShardFetchMsg::Start => self.start_fetch(ctx),
            StageShardFetchMsg::ProcessLine { stream, line } => self.handle_line(ctx, stream, line),
            StageShardFetchMsg::ReaderError { stream, error } => {
                self.handle_reader_error(ctx, stream, error)
            }
            StageShardFetchMsg::ReaderClosed { stream } => self.handle_reader_closed(ctx, stream),
            StageShardFetchMsg::ProcessExited(status) => self.handle_process_exit(ctx, status),
            StageShardFetchMsg::ProcessFailed(error) => self.fail(ctx, error),
        }
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        self.release_process(ctx);
    }
}

struct StageShardProcessForwarder {
    target: ActorAddress,
}

impl ActorInterface for StageShardProcessForwarder {
    type Incoming = swactor_process::SupervisedProcessObservation;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, event: Self::Incoming) {
        use swactor_process::{
            ProcessStream, ProcessStreamObservation, SupervisedProcessObservation,
        };
        let stream_kind = |stream| match stream {
            ProcessStream::Stdout => StageShardProcessStream::Stdout,
            ProcessStream::Stderr => StageShardProcessStream::Stderr,
        };
        let message = match event {
            SupervisedProcessObservation::Stream(ProcessStreamObservation::Line {
                stream,
                line,
            }) => StageShardFetchMsg::ProcessLine {
                stream: stream_kind(stream),
                line,
            },
            SupervisedProcessObservation::Stream(ProcessStreamObservation::Error {
                stream,
                error,
            }) => StageShardFetchMsg::ReaderError {
                stream: stream_kind(stream),
                error,
            },
            SupervisedProcessObservation::Stream(ProcessStreamObservation::Closed { stream }) => {
                StageShardFetchMsg::ReaderClosed {
                    stream: stream_kind(stream),
                }
            }
            SupervisedProcessObservation::Exited {
                operation: 1,
                result,
            } => {
                ctx.stop_self();
                match result {
                    Ok(status) => StageShardFetchMsg::ProcessExited(status),
                    Err(error) => StageShardFetchMsg::ProcessFailed(error),
                }
            }
            SupervisedProcessObservation::Exited { .. } => return,
        };
        let _ = ctx.send(self.target, message);
    }
}

fn publish_stage_shard_fetch_event(
    telemetry: &mut NodeTelemetry,
    config: &DeploymentConfig,
    event: &Value,
) -> Result<(), String> {
    let channel = telemetry.channel_by_name("myelin.worker.weights");
    let payload = node_event_payload(config, "stage_shard_fetch", "event", event.clone());
    telemetry.submit_value(channel, &payload);
    emit_stdio_telemetry_frame("myelin.worker.weights", &payload)
        .map_err(|e| format!("emit stage shard fetch telemetry frame: {e}"))?;
    telemetry.tick();
    Ok(())
}

fn materialize_stage_shard_with_process(
    plan: &StageShardPlan,
    config: &DeploymentConfig,
    telemetry: &mut NodeTelemetry,
    _driver: &mut IrohEdgePort,
    stack: &DistributionRuntimeStack,
) -> Result<PathBuf, String> {
    let output_path = std::env::var("MYELIN_MODEL_CACHE_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/var/cache/myelin-models"))
        .join(plan.cache_file_name());
    if output_path.is_file() {
        match validate_stage_shard_cache(&output_path, plan) {
            Ok(()) => {
                let event = json!({
                    "type":"StageShardCacheReady",
                    "stage_index":plan.stage_index,
                    "path":output_path,
                    "cache_hit":true,
                });
                publish_stage_shard_fetch_event(telemetry, config, &event)?;
                return Ok(output_path);
            }
            Err(error) => {
                let event = json!({
                    "type":"StageShardCacheInvalid",
                    "stage_index":plan.stage_index,
                    "path":output_path,
                    "cache_hit":false,
                    "error":error,
                });
                publish_stage_shard_fetch_event(telemetry, config, &event)?;
                std::fs::remove_file(&output_path).map_err(|remove_error| {
                    format!(
                        "remove invalid stage shard cache {}: {remove_error}",
                        output_path.display()
                    )
                })?;
            }
        }
    }

    let request = StageShardFetchRequest {
        plan: plan.clone(),
        output_path: output_path.clone(),
    };
    let request_json = serde_json::to_vec(&request)
        .map_err(|e| format!("serialize stage shard fetch request: {e}"))?;
    let completion = ActorCompletion::new();
    let actor = stack
        .runtime
        .spawn(StageShardFetchActor::new(
            request_json,
            output_path,
            completion.clone(),
            stack.runtime.create_sender(),
            stack.engine.clone(),
        ))
        .map_err(|e| format!("spawn stage shard fetch actor: {e}"))?;
    stack
        .runtime
        .send_to(actor, StageShardFetchMsg::Start)
        .map_err(|e| format!("start stage shard fetch actor: {e}"))?;
    let outcome = completion.wait();
    for event in outcome.events {
        publish_stage_shard_fetch_event(telemetry, config, &event)?;
    }
    outcome.result
}

fn handle_stage_command(
    command: StageCommandWire,
    services: WorkerStageServices<'_>,
    driver: &mut IrohEdgePort,
    worker: &mut TinygradWorker,
    edge_runtime: &mut WorkerEdgeRuntime,
    telemetry: &mut NodeTelemetry,
) -> Result<(), String> {
    let WorkerStageServices {
        config,
        stack,
        node_actor,
        arena_manager,
    } = services;
    let node_stage = |ds: &mut NodeTelemetry, phase: &str, status: &str, detail: Value| {
        emit_node_event(ds, config, NODE_STAGE_CHANNEL, phase, status, detail)
    };
    match command {
        StageCommandWire::ConfigureWorkerRole {
            run_id,
            stage_index,
            layer_start,
            layer_end_exclusive,
        } => {
            node_stage(
                telemetry,
                "configure_worker_role",
                "started",
                json!({"run_id":run_id,"stage_index":stage_index,"layer_range":{"start":layer_start,"end_exclusive":layer_end_exclusive}}),
            );
            match worker.configure_role(
                run_id,
                stage_index,
                layer_start,
                layer_end_exclusive,
                config,
                telemetry,
            ) {
                Ok(()) => node_stage(
                    telemetry,
                    "configure_worker_role",
                    "ready",
                    json!({"worker_event_type":"RoleConfigured"}),
                ),
                Err(error) => {
                    node_stage(
                        telemetry,
                        "configure_worker_role",
                        "failed",
                        json!({"error":error}),
                    );
                    let _ = stack.runtime.send_to(
                        node_actor,
                        NodeAgentMsg::WorkerCrashed {
                            reason: Some(error.clone()),
                        },
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
            stage_shard_plan,
        } => {
            let gguf_source_kind = match &gguf_source {
                GgufSource::LocalPath(_) => "local_path",
                GgufSource::HuggingFaceGguf { .. } => "huggingface",
            };
            let tokenizer_kind = match &tokenizer {
                TokenizerSource::EmbeddedGguf => "gguf",
                TokenizerSource::LocalPath(_) => "local_path",
            };
            let (resolved_gguf_source, using_stage_shard) =
                if let Some(stage_plan) = stage_shard_plan {
                    match materialize_stage_shard_with_process(
                        &stage_plan,
                        config,
                        telemetry,
                        driver,
                        stack,
                    ) {
                        Ok(local_path) => (
                            GgufSource::LocalPath(local_path.to_string_lossy().into_owned()),
                            true,
                        ),
                        Err(error) => {
                            node_stage(
                                telemetry,
                                "load_weights",
                                "failed",
                                json!({"error":error,"stage_shard":true}),
                            );
                            let _ = stack.runtime.send_to(
                                node_actor,
                                NodeAgentMsg::WorkerCrashed {
                                    reason: Some(error.clone()),
                                },
                            );
                            return Err(error);
                        }
                    }
                } else {
                    (gguf_source, false)
                };
            node_stage(
                telemetry,
                "load_weights",
                "started",
                json!({"model_id":&model_id,"gguf_source":gguf_source_kind,"tokenizer":tokenizer_kind,"stage_shard":using_stage_shard,"layer_range":{"start":layer_start,"end_exclusive":layer_end_exclusive}}),
            );
            match worker.load_weights(
                LoadWeightsRequest {
                    model_id: model_id.clone(),
                    gguf_source: resolved_gguf_source,
                    tokenizer,
                    layer_start,
                    layer_end_exclusive,
                },
                config,
                telemetry,
            ) {
                Ok(()) => node_stage(
                    telemetry,
                    "load_weights",
                    "ready",
                    json!({"worker_event_type":"WeightsLoaded","model_id":model_id}),
                ),
                Err(error) => {
                    node_stage(telemetry, "load_weights", "failed", json!({"error":error}));
                    let _ = stack.runtime.send_to(
                        node_actor,
                        NodeAgentMsg::WorkerCrashed {
                            reason: Some(error.clone()),
                        },
                    );
                    return Err(error);
                }
            }
            stack
                .runtime
                .send_to(
                    node_actor,
                    NodeAgentMsg::MarkWeightsReady {
                        run_id: config.run_id,
                        node_id: config.logical_node_id,
                        stage_index: config.stage_index,
                    },
                )
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
            node_stage(
                telemetry,
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
            node_stage(
                telemetry,
                "device_objects",
                "ready",
                json!({"run_id":run_id,"sent":["DeviceObjectsReleased","WorkerRoleReset"]}),
            );
            Ok(())
        }
        StageCommandWire::EstablishInboundEdge { edge_id, edge } => {
            node_stage(
                telemetry,
                "inbound_edge",
                "started",
                json!({"edge_id":edge_id,"kind":format!("{:?}", edge.kind),"ring_data_capacity":edge.ring_spec.data_capacity}),
            );
            edge_runtime.establish_inbound(
                edge,
                &mut WorkerRuntimeContext {
                    config,
                    stack,
                    driver,
                    node_actor,
                    worker,
                    arena_manager,
                    telemetry,
                },
            )?;
            node_stage(
                telemetry,
                "inbound_edge",
                "ready",
                json!({"edge_id":edge_id}),
            );
            Ok(())
        }
        StageCommandWire::EstablishOutboundEdge { edge_id, edge } => {
            node_stage(
                telemetry,
                "outbound_edge",
                "started",
                json!({"edge_id":edge_id,"kind":format!("{:?}", edge.kind),"consumer_node_id":edge.consumer_node_id,"has_consumer_endpoint":edge.consumer_endpoint.is_some()}),
            );
            edge_runtime.establish_outbound(
                edge,
                &mut WorkerRuntimeContext {
                    config,
                    stack,
                    driver,
                    node_actor,
                    worker,
                    arena_manager,
                    telemetry,
                },
            )?;
            node_stage(
                telemetry,
                "outbound_edge",
                "ready",
                json!({"edge_id":edge_id}),
            );
            Ok(())
        }
        StageCommandWire::ReleaseInputHandle { handle_id, .. } => {
            edge_runtime.release_input_handle(handle_id, worker, config, telemetry, driver, stack)
        }
        StageCommandWire::ExecuteStep {
            step_id,
            input_edge_id,
            object_id,
            sequence,
            ..
        } => {
            node_stage(
                telemetry,
                "execute_step",
                "started",
                json!({"step_id":step_id,"input_edge_id":input_edge_id,"object_id":object_id,"sequence":sequence}),
            );
            edge_runtime.execute_step(
                EdgeStepRequest {
                    step_id,
                    input_edge_id,
                    object_id,
                    sequence,
                },
                &mut WorkerRuntimeContext {
                    config,
                    stack,
                    driver,
                    node_actor,
                    worker,
                    arena_manager,
                    telemetry,
                },
            )?;
            node_stage(
                telemetry,
                "execute_step",
                "ready",
                json!({"step_id":step_id}),
            );
            Ok(())
        }
    }
}

fn run_self_test(
    worker: &mut TinygradWorker,
    config: &DeploymentConfig,
    prompt: &str,
    telemetry: &mut NodeTelemetry,
    _driver: &mut IrohEdgePort,
    _stack: &DistributionRuntimeStack,
) -> Result<(), String> {
    emit_node_event(
        telemetry,
        config,
        NODE_RUNTIME_CHANNEL,
        "self_test",
        "started",
        json!({"prompt_bytes":prompt.len()}),
    );
    worker.configure_role(
        config.run_id,
        config.stage_index,
        0,
        config.self_test_layer_end,
        config,
        telemetry,
    )?;
    worker.load_weights(
        LoadWeightsRequest {
            model_id: config.model_id.clone(),
            gguf_source: config.gguf_source.clone(),
            tokenizer: config.tokenizer.clone(),
            layer_start: 0,
            layer_end_exclusive: config.self_test_layer_end,
        },
        config,
        telemetry,
    )?;
    let result = worker.infer_prompt(0, prompt, config.self_test_max_tokens, config, telemetry)?;
    let record = json!({"type":"self_test_completed","prompt_bytes":prompt.len(),"result":result});
    telemetry.submit_value(telemetry.channels.node_self_test, &record);
    emit_node_event(
        telemetry,
        config,
        NODE_RUNTIME_CHANNEL,
        "self_test",
        "ready",
        json!({"prompt_bytes":prompt.len()}),
    );
    Ok(())
}

fn env_optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[derive(Clone)]
struct DeploymentConfig {
    run_id: u64,
    logical_node_id: u64,
    attempt_id: u64,
    readiness_id: u64,
    process_start_ticks: Option<u64>,
    selected_offer_id: Option<u64>,
    stage_index: u32,
    coordinator_endpoint: Option<EndpointAddr>,
    orchestrator_actor: Option<ActorAddress>,
    deployment: Option<DeploymentIdentity>,
    debug_join_socket: Option<String>,
    relay_mode: iroh::RelayMode,
    endpoint_addr_mask: EndpointAddrMask,
    agent_only: bool,
    exit_on_stdin_eof: bool,
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
        macro_rules! env_parse {
            ($name:expr, $default:expr) => {
                match env_optional($name) {
                    Some(value) => value
                        .parse()
                        .map_err(|e| format!("invalid {}={value:?}: {e}", $name)),
                    None => Ok($default),
                }
            };
        }
        let run_id = env_parse!("MYELIN_RUN_ID", 1)?;
        let logical_node_id = env_parse!("MYELIN_LOGICAL_NODE_ID", 1)?;
        let attempt_id = env_parse!("MYELIN_NODE_ATTEMPT_ID", 1)?;
        let deployment = match (
            env_optional("MYELIN_ARTIFACT_DIGEST"),
            env_optional("MYELIN_DEPLOYMENT_GENERATION"),
        ) {
            (None, None) => None,
            (Some(artifact_digest), Some(deployment_generation)) => Some(DeploymentIdentity {
                artifact_digest,
                deployment_generation,
            }),
            (Some(_), None) => {
                return Err(
                    "MYELIN_ARTIFACT_DIGEST requires MYELIN_DEPLOYMENT_GENERATION".to_owned(),
                );
            }
            (None, Some(_)) => {
                return Err(
                    "MYELIN_DEPLOYMENT_GENERATION requires MYELIN_ARTIFACT_DIGEST".to_owned(),
                );
            }
        };
        let (readiness_id, process_start_ticks) = if let Some(identity) = &deployment {
            let stat = fs::read_to_string("/proc/self/stat")
                .map_err(|error| format!("read deployment process identity: {error}"))?;
            let start_ticks = stat
                .rsplit_once(')')
                .and_then(|(_, fields)| fields.split_whitespace().nth(19))
                .ok_or_else(|| "process stat omitted start_ticks".to_owned())?;
            (
                crate::orchestration::daemon::deployment_readiness_id(&format!(
                    "{}:{start_ticks}:{}",
                    std::process::id(),
                    identity.deployment_generation,
                )),
                Some(
                    start_ticks
                        .parse::<u64>()
                        .map_err(|error| format!("parse deployment process identity: {error}"))?,
                ),
            )
        } else {
            (attempt_id, None)
        };
        let relay = relay_runtime_config_from_env(run_id)?;
        let debug_join_socket = match env_optional("MYELIN_DEBUG_JOIN_SOCKET").as_deref() {
            Some("disabled") => None,
            Some(path) => Some(path.to_owned()),
            None => Some(
                std::env::temp_dir()
                    .join(format!(
                        "myelin-node-debug-join-{run_id}-{logical_node_id}.sock"
                    ))
                    .to_string_lossy()
                    .into_owned(),
            ),
        };
        let provider = env_optional("MYELIN_NODE_PROVIDER").unwrap_or_else(|| "process".to_owned());
        let default_device = if provider == "process" {
            "CPU"
        } else {
            DEFAULT_DEVICE
        };
        Ok(Self {
            run_id,
            logical_node_id,
            attempt_id,
            readiness_id,
            process_start_ticks,
            selected_offer_id: env_optional(SELECTED_OFFER_ID_ENV)
                .map(|value| {
                    value.parse().map_err(|error| {
                        format!("invalid {SELECTED_OFFER_ID_ENV}={value:?}: {error}")
                    })
                })
                .transpose()?,
            stage_index: env_parse!("MYELIN_STAGE_INDEX", 0)?,
            coordinator_endpoint: env_optional("MYELIN_COORDINATOR_ENDPOINT")
                .map(|value| {
                    serde_json::from_str::<EndpointAddr>(&value)
                        .map_err(|e| format!("invalid MYELIN_COORDINATOR_ENDPOINT JSON: {e}"))
                })
                .transpose()?,
            orchestrator_actor: env_optional("MYELIN_ORCHESTRATOR_ACTOR")
                .map(|value| {
                    serde_json::from_str::<ActorAddress>(&value)
                        .map_err(|e| format!("invalid MYELIN_ORCHESTRATOR_ACTOR JSON: {e}"))
                })
                .transpose()?,
            deployment,
            debug_join_socket,
            relay_mode: relay.mode,
            endpoint_addr_mask: env_optional(MVP_IROH_ENDPOINT_ADDR_MASK_ENV)
                .as_deref()
                .map(EndpointAddrMask::parse)
                .transpose()?
                .unwrap_or_default(),
            agent_only: env_optional("MYELIN_AGENT_ONLY")
                .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on")),
            // Provider workers outlive the orchestrator process and rejoin it after restart.
            exit_on_stdin_eof: false,
            worker_script: env_optional("MYELIN_TINYGRAD_WORKER")
                .unwrap_or_else(|| DEFAULT_WORKER_SCRIPT.to_owned()),
            device: env_optional("DEV").unwrap_or_else(|| default_device.to_owned()),
            model_id: env_optional("MYELIN_MODEL_ID")
                .unwrap_or_else(|| DEFAULT_MODEL_ID.to_owned()),
            gguf_source: if let Some(path) = env_optional("MYELIN_GGUF_LOCAL_PATH") {
                GgufSource::LocalPath(path)
            } else {
                GgufSource::HuggingFaceGguf {
                    repo: env_optional("MYELIN_GGUF_REPO")
                        .unwrap_or_else(|| DEFAULT_HF_REPO.to_owned()),
                    file: env_optional("MYELIN_GGUF_FILE")
                        .unwrap_or_else(|| DEFAULT_HF_FILE.to_owned()),
                    revision: env_optional("MYELIN_GGUF_REVISION"),
                }
            },
            tokenizer: env_optional("MYELIN_TOKENIZER_LOCAL_PATH")
                .map(TokenizerSource::LocalPath)
                .unwrap_or(TokenizerSource::EmbeddedGguf),
            self_test_prompt: env_optional("MYELIN_NODE_SELF_TEST_PROMPT"),
            self_test_layer_end: env_parse!("MYELIN_SELF_TEST_LAYER_END", 16)?,
            self_test_max_tokens: env_parse!("MYELIN_SELF_TEST_MAX_TOKENS", 1)?,
            arena_bytes: env_parse!("MYELIN_ARENA_BYTES", DEFAULT_ARENA_BYTES)?,
            arena_alignment: env_parse!("MYELIN_ARENA_ALIGNMENT", DEFAULT_ARENA_ALIGNMENT)?,
        })
    }
}

#[derive(Debug)]
enum HelperStdoutEvent {
    Line(String),
    Closed,
    ReadError(String),
}

fn spawn_helper_stdout_reader<R: Read + Send + 'static>(
    blocking: &BlockingWorkSender,
    reader: R,
    tx: Sender<HelperStdoutEvent>,
    sender: ExternalSender,
    wake: Arc<Mutex<Option<ActorAddress>>>,
) -> Result<swactor_engine::BlockingWorkHandle<()>, String> {
    use std::io::BufRead;
    blocking
        .submit_joinable(move || {
            let publish = |event| {
                let sent = tx.send(event).is_ok();
                if let Some(actor) = *wake.lock() {
                    let _ = sender.send_to(actor, HelperOutputReady);
                }
                sent
            };
            for line in std::io::BufReader::new(reader).lines() {
                match line {
                    Ok(line) => {
                        if !publish(HelperStdoutEvent::Line(line)) {
                            return;
                        }
                    }
                    Err(error) => {
                        publish(HelperStdoutEvent::ReadError(error.to_string()));
                        return;
                    }
                }
            }
            publish(HelperStdoutEvent::Closed);
        })
        .map_err(|error| format!("spawn helper stdout reader: {error}"))
}

fn spawn_helper_stderr_reader<R: Read + Send + 'static>(
    blocking: &BlockingWorkSender,
    reader: R,
    tx: Sender<String>,
    sender: ExternalSender,
    wake: Arc<Mutex<Option<ActorAddress>>>,
) -> Result<swactor_engine::BlockingWorkHandle<()>, String> {
    use std::io::BufRead;
    blocking
        .submit_joinable(move || {
            for line in std::io::BufReader::new(reader).lines() {
                let (line, failed) = match line {
                    Ok(line) => (line, false),
                    Err(error) => (format!("read helper stderr: {error}"), true),
                };
                if tx.send(line).is_err() {
                    return;
                }
                if let Some(actor) = *wake.lock() {
                    let _ = sender.send_to(actor, NodeRuntimeMsg::Progress);
                }
                if failed {
                    return;
                }
            }
        })
        .map_err(|error| format!("spawn helper stderr reader: {error}"))
}

fn drain_worker_stderr(
    stderr_rx: &Arc<Mutex<Receiver<String>>>,
    config: &DeploymentConfig,
    telemetry: &mut NodeTelemetry,
) {
    let mut emitted = false;
    while let Ok(line) = stderr_rx.lock().try_recv() {
        let payload = node_event_payload(config, "worker_stderr", "observed", json!({"line":line}));
        telemetry.submit_value(telemetry.channels.worker_stderr, &payload);
        emitted = true;
    }
    if emitted {
        telemetry.tick();
    }
}

#[derive(Clone, Copy)]
struct HelperOutputReady;

struct HelperWaitOutcome {
    result: Result<Value, String>,
    lines: Vec<String>,
    stderr_lines: Vec<String>,
}

struct HelperWaitActor {
    stdout_rx: Arc<Mutex<Receiver<HelperStdoutEvent>>>,
    stderr_rx: Option<Arc<Mutex<Receiver<String>>>>,
    expected: String,
    command_id: u64,
    wake: Arc<Mutex<Option<ActorAddress>>>,
    resource_samples: Arc<Mutex<Vec<tokio::sync::mpsc::Sender<()>>>>,
    completion: ActorCompletion<HelperWaitOutcome>,
    lines: Vec<String>,
    stderr_lines: Vec<String>,
}

impl HelperWaitActor {
    fn drain_stderr(&mut self) {
        let Some(stderr_rx) = &self.stderr_rx else {
            return;
        };
        while let Ok(line) = stderr_rx.lock().try_recv() {
            self.stderr_lines.push(line);
        }
    }

    fn finish(&mut self, ctx: &Ctx, result: Result<Value, String>) {
        self.drain_stderr();
        *self.wake.lock() = None;
        assert!(
            self.completion
                .complete(HelperWaitOutcome {
                    result,
                    lines: std::mem::take(&mut self.lines),
                    stderr_lines: std::mem::take(&mut self.stderr_lines),
                })
                .is_ok(),
            "helper wait completed twice"
        );
        ctx.stop_self();
    }

    fn receive_output(&mut self, ctx: &Ctx) {
        self.drain_stderr();
        loop {
            let observation = self.stdout_rx.lock().try_recv();
            match observation {
                Ok(HelperStdoutEvent::Line(line)) => {
                    let parsed = serde_json::from_str::<Value>(&line);
                    self.lines.push(line.clone());
                    let value = match parsed {
                        Ok(value) => value,
                        Err(error) => {
                            self.finish(ctx, Err(format!("parse helper stdout {line:?}: {error}")));
                            return;
                        }
                    };
                    for sampler in self.resource_samples.lock().iter() {
                        let _ = sampler.try_send(());
                    }
                    if value.get("type").and_then(Value::as_str) == Some("WorkerFatal") {
                        self.finish(ctx, Err(format!("worker fatal: {value}")));
                        return;
                    }
                    if value.get("type").and_then(Value::as_str) == Some(self.expected.as_str())
                        && value.get("command_id").and_then(Value::as_u64) == Some(self.command_id)
                    {
                        self.finish(ctx, Ok(value));
                        return;
                    }
                }
                Ok(HelperStdoutEvent::Closed) => {
                    self.finish(
                        ctx,
                        Err(format!(
                            "tinygrad helper stdout closed while waiting for {}",
                            self.expected
                        )),
                    );
                    return;
                }
                Ok(HelperStdoutEvent::ReadError(error)) => {
                    self.finish(ctx, Err(format!("read helper stdout: {error}")));
                    return;
                }
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Disconnected) => {
                    self.finish(
                        ctx,
                        Err(format!(
                            "tinygrad helper stdout reader disconnected while waiting for {}",
                            self.expected
                        )),
                    );
                    return;
                }
            }
        }
    }
}

impl ActorInterface for HelperWaitActor {
    type Incoming = HelperOutputReady;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        *self.wake.lock() = Some(ctx.self_addr());
        self.receive_output(ctx);
    }

    fn handle(&mut self, ctx: &Ctx, _message: Self::Incoming) {
        self.receive_output(ctx);
    }
}

struct HelperWaitRequest<'a> {
    stdout_rx: Arc<Mutex<Receiver<HelperStdoutEvent>>>,
    stderr_rx: Option<Arc<Mutex<Receiver<String>>>>,
    expected: &'a str,
    command_type: &'a str,
    command_id: u64,
    config: &'a DeploymentConfig,
    channel: ChannelId,
    channel_name: &'a str,
    wake: Arc<Mutex<Option<ActorAddress>>>,
}

fn wait_for_helper_event(
    runtime: &Runtime,
    telemetry: &mut NodeTelemetry,
    request: HelperWaitRequest<'_>,
) -> Result<Value, String> {
    let HelperWaitRequest {
        stdout_rx,
        stderr_rx,
        expected,
        command_type,
        command_id,
        config,
        channel,
        channel_name,
        wake,
    } = request;
    emit_node_event(
        telemetry,
        config,
        NODE_WORKER_CHANNEL,
        "worker_stdout_read",
        "started",
        json!({"command_type":command_type,"expected_event_type":expected,"channel":channel_name}),
    );
    let completion = ActorCompletion::new();
    runtime
        .spawn(HelperWaitActor {
            stdout_rx,
            stderr_rx,
            expected: expected.to_owned(),
            command_id,
            wake,
            resource_samples: Arc::clone(&telemetry.resource_samples),
            completion: completion.clone(),
            lines: Vec::new(),
            stderr_lines: Vec::new(),
        })
        .map_err(|error| format!("spawn helper wait actor: {error}"))?;
    let outcome = completion.wait();
    for line in outcome.stderr_lines {
        let payload = node_event_payload(config, "worker_stderr", "observed", json!({"line":line}));
        telemetry.submit_value(telemetry.channels.worker_stderr, &payload);
    }
    for line in outcome.lines {
        let line_bytes = line.len();
        emit_node_event(
            telemetry,
            config,
            NODE_WORKER_CHANNEL,
            "worker_stdout_read",
            "ready",
            json!({"command_type":command_type,"expected_event_type":expected,"channel":channel_name,"line_bytes":line_bytes}),
        );
        let value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(error) => {
                emit_node_event(
                    telemetry,
                    config,
                    NODE_WORKER_CHANNEL,
                    "worker_stdout_parse",
                    "failed",
                    json!({"command_type":command_type,"expected_event_type":expected,"channel":channel_name,"line_bytes":line_bytes,"error":error.to_string()}),
                );
                continue;
            }
        };
        let worker_event_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        emit_node_event(
            telemetry,
            config,
            NODE_WORKER_CHANNEL,
            "worker_stdout_parse",
            "ready",
            json!({"command_type":command_type,"expected_event_type":expected,"channel":channel_name,"line_bytes":line_bytes,"worker_event_type":worker_event_type}),
        );
        telemetry.submit_value(channel, &value);
        emit_stdio_telemetry_frame(channel_name, &value)
            .map_err(|error| format!("emit worker stdio telemetry frame: {error}"))?;
        if worker_event_type != expected {
            emit_node_event(
                telemetry,
                config,
                NODE_WORKER_CHANNEL,
                "worker_event",
                "observed",
                json!({"command_type":command_type,"command_waiting_for":expected,"worker_event_type":worker_event_type,"event":value}),
            );
        }
    }
    if let Err(error) = &outcome.result {
        emit_node_event(
            telemetry,
            config,
            NODE_WORKER_CHANNEL,
            "worker_stdout_read",
            "failed",
            json!({"command_type":command_type,"expected_event_type":expected,"channel":channel_name,"error":error}),
        );
    }
    outcome.result
}

struct LoadWeightsRequest {
    model_id: String,
    gguf_source: GgufSource,
    tokenizer: TokenizerSource,
    layer_start: u32,
    layer_end_exclusive: u32,
}

struct InstallRingRequest<'a> {
    ring_id: u64,
    edge_id: u64,
    port: &'a str,
    direction: &'a str,
    layout: arena::RingLayout,
    object_spec: StageObjectSpecWire,
}

struct TinygradWorker {
    child: Child,
    stdin: ChildStdin,
    stdout_rx: Arc<Mutex<Receiver<HelperStdoutEvent>>>,
    stderr_rx: Arc<Mutex<Receiver<String>>>,
    runtime: Runtime,
    stdout_wake: Arc<Mutex<Option<ActorAddress>>>,
    readers: Vec<swactor_engine::BlockingWorkHandle<()>>,
    exit_watch: Option<swactor_process::ProcessWatch>,
    exit_relay: Option<ActorAddress>,
    exit_observation: Arc<Mutex<Option<swactor_process::ProcessExitObservation>>>,
    command_id: u64,
}

impl TinygradWorker {
    fn spawn(
        config: &DeploymentConfig,
        arena_fd: std::os::fd::RawFd,
        runtime: Runtime,
        engine: EngineHandle,
        wake: RuntimeWake,
    ) -> Result<Self, String> {
        let mut child = swactor_process::command_spawn(
            Command::new("python3")
                .arg(&config.worker_script)
                .env("DEV", &config.device)
                .env("MYELIN_RUN_ID", config.run_id.to_string())
                .env("MYELIN_LOGICAL_NODE_ID", config.logical_node_id.to_string())
                .env("MYELIN_STAGE_INDEX", config.stage_index.to_string())
                .env("MYELIN_ARENA_FD", arena_fd.to_string())
                .env("MYELIN_ARENA_BYTES", config.arena_bytes.to_string())
                .env(
                    "MVP_IROH_ENDPOINT_ADDR_MASK",
                    config.endpoint_addr_mask.as_str(),
                )
                .env("MYELIN_IROH_RELAY_MODE", format!("{:?}", config.relay_mode))
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped()),
        )
        .map_err(|e| format!("spawn tinygrad helper {}: {e}", config.worker_script))?;
        let stdin = child.stdin.take().expect("helper stdin is piped");
        let stdout = child.stdout.take().expect("helper stdout is piped");
        let stderr = child.stderr.take().expect("helper stderr is piped");
        let (stdout_tx, stdout_rx) = mpsc::channel();
        let (stderr_tx, stderr_rx) = mpsc::channel();
        let mut worker = Self {
            child,
            stdin,
            stdout_rx: Arc::new(Mutex::new(stdout_rx)),
            stderr_rx: Arc::new(Mutex::new(stderr_rx)),
            runtime,
            stdout_wake: Arc::new(Mutex::new(None)),
            readers: Vec::new(),
            exit_watch: None,
            exit_relay: None,
            exit_observation: Arc::new(Mutex::new(None)),
            command_id: 0,
        };
        worker.readers.push(spawn_helper_stdout_reader(
            &engine.blocking_work_sender(),
            stdout,
            stdout_tx,
            worker.runtime.create_sender(),
            Arc::clone(&worker.stdout_wake),
        )?);
        worker.readers.push(spawn_helper_stderr_reader(
            &engine.blocking_work_sender(),
            stderr,
            stderr_tx,
            worker.runtime.create_sender(),
            Arc::clone(&wake.target),
        )?);
        let relay = worker
            .runtime
            .spawn(HelperProcessForwarder {
                observation: Arc::clone(&worker.exit_observation),
                wake,
            })
            .map_err(|error| format!("spawn helper exit observer: {error}"))?;
        worker.exit_relay = Some(relay);
        worker.exit_watch = Some(
            swactor_process::watch_process(
                &engine.blocking_work_sender(),
                swactor_process::ProcessIdentity {
                    pid: worker.child.id(),
                    environment: Vec::new(),
                    process_group_leader: false,
                },
                None,
                worker.runtime.create_sender(),
                relay,
            )
            .map_err(|error| format!("watch helper exit: {error}"))?,
        );
        Ok(worker)
    }

    fn initialize(
        &mut self,
        device: &str,
        config: &DeploymentConfig,
        telemetry: &mut NodeTelemetry,
    ) -> Result<(), String> {
        self.command(
            json!({"type":"InitializeWorker","helper_abi_version":1,"backend":{"device":device}}),
            "WorkerReady",
            config,
            telemetry,
            "myelin.worker.initialize",
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
        telemetry: &mut NodeTelemetry,
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
            telemetry,
            "myelin.worker.role",
        )
        .map(|_| ())
    }

    fn load_weights(
        &mut self,
        request: LoadWeightsRequest,
        config: &DeploymentConfig,
        telemetry: &mut NodeTelemetry,
    ) -> Result<(), String> {
        let LoadWeightsRequest {
            model_id,
            gguf_source,
            tokenizer,
            layer_start,
            layer_end_exclusive,
        } = request;
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
            telemetry,
            "myelin.worker.weights",
        )
        .map(|_| ())
    }

    fn infer_prompt(
        &mut self,
        request_id: u64,
        prompt: &str,
        max_tokens: u32,
        config: &DeploymentConfig,
        telemetry: &mut NodeTelemetry,
    ) -> Result<Value, String> {
        self.command(
            json!({"type":"InferPrompt","request_id":request_id,"prompt":prompt,"max_tokens":max_tokens}),
            "PromptCompleted",
            config,
            telemetry,
            "myelin.worker.prompt",
        )
    }

    fn encode_prompt(
        &mut self,
        request_id: u64,
        prompt: &str,
        config: &DeploymentConfig,
        telemetry: &mut NodeTelemetry,
    ) -> Result<Vec<u32>, String> {
        let result = self.command(
            json!({"type":"EncodePrompt","request_id":request_id,"prompt":prompt}),
            "PromptEncoded",
            config,
            telemetry,
            "myelin.worker.tokenizer",
        )?;
        result
            .get("tokens")
            .and_then(Value::as_array)
            .ok_or_else(|| "PromptEncoded missing tokens".to_owned())?
            .iter()
            .map(|value| {
                let token = value
                    .as_u64()
                    .ok_or_else(|| format!("PromptEncoded token is not u64: {value}"))?;
                u32::try_from(token)
                    .map_err(|_| format!("PromptEncoded token exceeds u32: {token}"))
            })
            .collect()
    }

    fn decode_tokens(
        &mut self,
        request_id: u64,
        tokens: &[u32],
        config: &DeploymentConfig,
        telemetry: &mut NodeTelemetry,
    ) -> Result<String, String> {
        let result = self.command(
            json!({"type":"DecodeTokens","request_id":request_id,"tokens":tokens}),
            "TokensDecoded",
            config,
            telemetry,
            "myelin.worker.tokenizer",
        )?;
        result
            .get("text")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| "TokensDecoded missing text".to_owned())
    }

    fn install_ring(
        &mut self,
        request: InstallRingRequest<'_>,
        config: &DeploymentConfig,
        telemetry: &mut NodeTelemetry,
    ) -> Result<(), String> {
        let InstallRingRequest {
            ring_id,
            edge_id,
            port,
            direction,
            layout,
            object_spec,
        } = request;
        self.command(
            json!({
                "type":"InstallRing",
                "ring_id":ring_id,
                "edge_id":edge_id,
                "port":port,
                "direction":direction,
                "layout":{
                    "start_offset":layout.start_offset,
                    "header_offset":layout.header_offset,
                    "data_offset":layout.data_offset,
                    "end_offset":layout.end_offset,
                    "data_bytes":layout.data_bytes,
                    "alignment":layout.alignment,
                },
                "object_spec":{
                    "max_extent":object_spec.max_extent,
                    "alignment":object_spec.alignment,
                },
            }),
            "RingInstalled",
            config,
            telemetry,
            "myelin.worker.ring",
        )
        .map(|_| ())
    }

    fn uninstall_ring(
        &mut self,
        ring_id: u64,
        config: &DeploymentConfig,
        telemetry: &mut NodeTelemetry,
    ) -> Result<(), String> {
        self.command(
            json!({"type":"UninstallRing","ring_id":ring_id}),
            "RingUninstalled",
            config,
            telemetry,
            "myelin.worker.ring",
        )
        .map(|_| ())
    }

    fn ring_readable(
        &mut self,
        ring_id: u64,
        edge_id: u64,
        object_spec: StageObjectSpecWire,
        config: &DeploymentConfig,
        telemetry: &mut NodeTelemetry,
    ) -> Result<LoadedObject, String> {
        let event = self.command(
            json!({
                "type":"RingReadable",
                "ring_id":ring_id,
                "edge_id":edge_id,
                "object_spec":{
                    "max_extent":object_spec.max_extent,
                    "alignment":object_spec.alignment,
                },
            }),
            "ObjectLoaded",
            config,
            telemetry,
            "myelin.worker.ingress",
        )?;
        Ok(LoadedObject {
            object_id: value_u64(&event, "object_id")?,
            sequence: value_u64(&event, "sequence")?,
            handle_generation: value_u64(&event, "handle_generation")?,
            handle_id: value_u64(&event, "handle_id")?,
        })
    }

    fn execute_step(
        &mut self,
        request: ExecuteStepRequest,
        config: &DeploymentConfig,
        telemetry: &mut NodeTelemetry,
    ) -> Result<usize, String> {
        let ExecuteStepRequest {
            role_id,
            step_id,
            input_object_id,
            input_sequence,
            input_handle_id,
            output_ring_id,
            output_object_id,
            output_sequence,
            final_stage,
            output_spec,
        } = request;
        let event = self.command(
            json!({
                "type":"ExecuteStep",
                "role_id":role_id,
                "step_id":step_id,
                "input_object_id":input_object_id,
                "input_sequence":input_sequence,
                "input_handle_id":input_handle_id,
                "output_ring_id":output_ring_id,
                "output_object_id":output_object_id,
                "output_sequence":output_sequence,
                "final_stage":final_stage,
                "output_spec":{
                    "max_extent":output_spec.max_extent,
                    "alignment":output_spec.alignment,
                },
            }),
            "StepExecuted",
            config,
            telemetry,
            "myelin.worker.step",
        )?;
        usize::try_from(value_u64(&event, "committed_bytes")?)
            .map_err(|_| "StepExecuted committed_bytes does not fit usize".to_owned())
    }

    fn release_device_object(
        &mut self,
        handle_id: u64,
        config: &DeploymentConfig,
        telemetry: &mut NodeTelemetry,
    ) -> Result<(), String> {
        self.command(
            json!({"type":"ReleaseDeviceObject","handle_id":handle_id}),
            "DeviceObjectReleased",
            config,
            telemetry,
            "myelin.worker.device_object",
        )
        .map(|_| ())
    }

    fn shutdown(
        &mut self,
        config: &DeploymentConfig,
        telemetry: &mut NodeTelemetry,
    ) -> Result<(), String> {
        self.command(
            json!({"type":"ShutdownWorker"}),
            "WorkerStopped",
            config,
            telemetry,
            "myelin.worker.shutdown",
        )?;
        let status = swactor_process::child_wait(&mut self.child)
            .map_err(|error| format!("reap stopped tinygrad helper: {error}"))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("tinygrad helper shutdown exited with {status}"))
        }
    }

    fn observe_exit(&mut self) -> Result<Option<std::process::ExitStatus>, String> {
        let Some(observation) = self.exit_observation.lock().take() else {
            return Ok(None);
        };
        if let Some(error) = observation.error {
            return Err(format!("observe tinygrad helper exit: {error}"));
        }
        swactor_process::child_wait(&mut self.child)
            .map(Some)
            .map_err(|error| format!("reap tinygrad helper: {error}"))
    }

    fn command(
        &mut self,
        mut command: Value,
        expected: &str,
        config: &DeploymentConfig,
        telemetry: &mut NodeTelemetry,
        channel_name: &str,
    ) -> Result<Value, String> {
        self.command_id = self
            .command_id
            .checked_add(1)
            .ok_or_else(|| "helper command identity exhausted".to_owned())?;
        command["command_id"] = json!(self.command_id);
        let node_worker = |ds: &mut NodeTelemetry, phase: &str, status: &str, detail: Value| {
            emit_node_event(ds, config, NODE_WORKER_CHANNEL, phase, status, detail)
        };
        let channel = telemetry.channel_by_name(channel_name);
        let command_type = command
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let command_text = command.to_string();
        let command_bytes = command_text.len() + 1;
        node_worker(
            telemetry,
            "worker_command_write",
            "started",
            json!({"command_type":command_type.as_str(),"expected_event_type":expected,"command_bytes":command_bytes}),
        );
        if let Err(error) = writeln!(self.stdin, "{command_text}") {
            node_worker(
                telemetry,
                "worker_command_write",
                "failed",
                json!({"command_type":command_type.as_str(),"expected_event_type":expected,"command_bytes":command_bytes,"error":error.to_string()}),
            );
            return Err(format!("write helper command: {error}"));
        }
        if let Err(error) = self.stdin.flush() {
            node_worker(
                telemetry,
                "worker_command_write",
                "failed",
                json!({"command_type":command_type.as_str(),"expected_event_type":expected,"command_bytes":command_bytes,"error":error.to_string()}),
            );
            return Err(format!("flush helper command: {error}"));
        }
        node_worker(
            telemetry,
            "worker_command_write",
            "ready",
            json!({"command_type":command_type.as_str(),"expected_event_type":expected,"command_bytes":command_bytes}),
        );
        self.expect_event(
            expected,
            command_type.as_str(),
            config,
            telemetry,
            channel,
            channel_name,
        )
    }

    fn expect_event(
        &mut self,
        expected: &str,
        command_type: &str,
        config: &DeploymentConfig,
        telemetry: &mut NodeTelemetry,
        channel: ChannelId,
        channel_name: &str,
    ) -> Result<Value, String> {
        wait_for_helper_event(
            &self.runtime,
            telemetry,
            HelperWaitRequest {
                stdout_rx: Arc::clone(&self.stdout_rx),
                stderr_rx: Some(Arc::clone(&self.stderr_rx)),
                expected,
                command_type,
                command_id: self.command_id,
                config,
                channel,
                channel_name,
                wake: Arc::clone(&self.stdout_wake),
            },
        )
    }
}

impl Drop for TinygradWorker {
    fn drop(&mut self) {
        let _ = swactor_process::child_kill(&mut self.child);
        let _ = swactor_process::child_wait(&mut self.child);
        self.exit_watch = None;
        if let Some(relay) = self.exit_relay.take() {
            let _ = self.runtime.stop_actor(relay);
        }
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }
}

fn spawn_stdin_shutdown_listener(
    blocking: &BlockingWorkSender,
    exit_on_eof: bool,
    sender: ExternalSender,
    actor: ActorAddress,
) {
    swactor_process::spawn_stdin_command_wait(blocking, "shutdown", exit_on_eof, sender, actor);
}

#[cfg(test)]
mod control_flow_properties {
    use std::os::unix::process::ExitStatusExt;
    use std::sync::{Arc, mpsc};

    use iroh::{EndpointAddr, SecretKey};
    use parking_lot::Mutex;
    use proptest::prelude::*;
    use swactor::actor::ActorAddress;
    use swactor::config::RuntimeConfig;
    use swactor::runtime::{Runtime, RuntimeParts};
    use swactor_engine::{ActorCompletion, Engine, SteppingBackend, TokioBackend, TokioConfig};

    use super::*;
    use crate::node_actor::StageLifecycleWire;
    use crate::tests::fuzz_support::{actor_census, drive_steps};

    const DRIVE_PER_ACTION: usize = 16;
    const FINAL_DRIVE_BUDGET: usize = 256;

    #[test]
    fn telemetry_lifetimes_survive_restarts_and_serialize_concurrent_boots() {
        let state = tempfile::tempdir().expect("telemetry state");
        let run_id = 1 << 62;
        assert_eq!(
            allocate_telemetry_lifetime(state.path(), 7, run_id).unwrap(),
            run_id + 1
        );
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            TokioBackend::new(TokioConfig::default()).expect("Tokio backend"),
        )
        .expect("engine");
        let blocking = engine.handle().blocking_work_sender();
        let handles = (0..4)
            .map(|_| {
                let path = state.path().to_owned();
                blocking
                    .submit_joinable(move || allocate_telemetry_lifetime(&path, 7, run_id).unwrap())
                    .expect("submit lifetime allocation")
            })
            .collect::<Vec<_>>();
        let mut generations = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        generations.sort_unstable();
        assert_eq!(generations, (run_id + 2..=run_id + 5).collect::<Vec<_>>());
        // A new process opening the same state cannot regress with a lower run id.
        assert_eq!(
            allocate_telemetry_lifetime(state.path(), 7, 1).unwrap(),
            run_id + 6
        );
        assert_eq!(
            allocate_telemetry_lifetime(state.path(), 7, run_id + 100).unwrap(),
            run_id + 101
        );
        assert!(allocate_telemetry_lifetime(state.path(), 7, u64::MAX).is_err());
        assert_eq!(
            allocate_telemetry_lifetime(state.path(), 7, 1).unwrap(),
            run_id + 102
        );
    }

    #[test]
    fn sampler_health_emits_only_state_transitions() {
        let endpoint = TelemetryEndpoint::with_descriptor(
            StreamDescriptor {
                stream: StreamId::new(NodeId::new("sampler-health-test"), Lifetime(1)),
                label: None,
                origin: StreamOrigin::RemoteNode,
            },
            16,
            16,
        );
        let producer = endpoint.producer();
        let health_channel = producer.register_channel(
            NODE_SAMPLER_CHANNEL,
            ChannelContent::MessagePackRecord {
                schema: Some(NODE_SAMPLER_CHANNEL.to_owned()),
            },
        );
        let subscription = endpoint.subscribe_all("sampler-health-transitions");
        let context = SamplerHealthContext {
            run_id: 1,
            node_id: 2,
            stage_index: 0,
        };
        let state = Mutex::new(SamplerHealthState::Waiting);

        submit_sampler_started(&producer, health_channel, context, "cpu", "host.cpu");
        submit_sampler_sample_health(
            &producer,
            health_channel,
            context,
            ("cpu", "host.cpu"),
            0,
            None,
            &state,
        );
        submit_sampler_sample_health(
            &producer,
            health_channel,
            context,
            ("cpu", "host.cpu"),
            1,
            None,
            &state,
        );
        submit_sampler_sample_health(
            &producer,
            health_channel,
            context,
            ("cpu", "host.cpu"),
            2,
            Some("unavailable"),
            &state,
        );
        submit_sampler_sample_health(
            &producer,
            health_channel,
            context,
            ("cpu", "host.cpu"),
            3,
            Some("still unavailable"),
            &state,
        );
        submit_sampler_sample_health(
            &producer,
            health_channel,
            context,
            ("cpu", "host.cpu"),
            4,
            None,
            &state,
        );
        endpoint.tick();

        let statuses = subscription
            .drain_available()
            .into_iter()
            .filter_map(|event| match event {
                TelemetryEvent::Frame(delivery) if delivery.channel.channel == health_channel => {
                    decode_record_value(&delivery.payload)
                        .ok()
                        .and_then(|value| value["status"].as_str().map(str::to_owned))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            statuses,
            ["started", "waiting", "ready", "failed", "recovered"]
        );
    }

    #[test]
    fn agent_cpu_sampler_emits_decodable_host_stats() {
        let parts = RuntimeParts::new(RuntimeConfig {
            worker_count: 1,
            ..RuntimeConfig::default()
        });
        let _runtime = parts.runtime().clone();
        let engine = Engine::new(
            parts,
            TokioBackend::new(TokioConfig::default()).expect("Tokio backend"),
        )
        .expect("engine");
        let endpoint = Arc::new(TelemetryEndpoint::with_descriptor(
            StreamDescriptor {
                stream: StreamId::new(NodeId::new("agent-sampler-test"), Lifetime(1)),
                label: None,
                origin: StreamOrigin::RemoteNode,
            },
            8,
            8,
        ));
        let producer = endpoint.producer();
        let sample_channel = producer.register_record::<telemetry::hardware::cpu::HostCpuSample>();
        let health_channel = producer.register_channel(
            NODE_SAMPLER_CHANNEL,
            ChannelContent::MessagePackRecord {
                schema: Some(NODE_SAMPLER_CHANNEL.to_owned()),
            },
        );
        let subscription = endpoint.subscribe_all("agent-cpu-sampler-test");

        let _progress = spawn_host_cpu_sampler(
            engine.task_spawner(),
            engine.handle(),
            Arc::clone(&endpoint),
            sample_channel,
            health_channel,
            SamplerHealthContext {
                run_id: 1,
                node_id: 1,
                stage_index: 0,
            },
            vec![std::process::id()],
        );

        loop {
            if let TelemetryEvent::Frame(delivery) = subscription.recv().expect("sample event")
                && delivery.channel.channel == sample_channel
            {
                let sample = telemetry::hardware::cpu::HostCpuSample::decode(&delivery.payload)
                    .expect("host CPU sample");
                assert_eq!(sample.seq, 0);
                assert!(
                    sample
                        .host
                        .as_ref()
                        .is_some_and(|host| host.logical_cpus > 0)
                );
                break;
            }
        }
    }

    fn check_runtime_clean(
        runtime: &Runtime,
        backend: &SteppingBackend,
        baseline_actors: usize,
        baseline_tasks: usize,
    ) -> Result<(), String> {
        let stats = runtime.stats();
        let poisoned = stats
            .actor_details
            .iter()
            .filter(|actor| actor.poisoned)
            .count();
        let panics = stats
            .workers
            .iter()
            .map(|worker| worker.panics)
            .sum::<u64>();
        let mailbox_depth = stats
            .workers
            .iter()
            .map(|worker| worker.mailbox_depth)
            .sum::<usize>()
            + stats
                .actor_details
                .iter()
                .map(|actor| actor.mailbox_depth)
                .sum::<usize>();
        let actors = stats.actors.len();
        let tasks = backend.pending_task_count();
        if poisoned != 0
            || panics != 0
            || mailbox_depth != 0
            || actors != baseline_actors
            || tasks != baseline_tasks
        {
            Err(format!(
                "poisoned={poisoned} panics={panics} mailbox={mailbox_depth} \
                 actors={actors}/{baseline_actors} tasks={tasks}/{baseline_tasks}\n{}",
                actor_census(runtime),
            ))
        } else {
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    enum RuntimeAction {
        Tick,
        CurrentReadinessAck,
        DuplicateReadinessAck,
        StaleReadinessAck(u8),
        Snapshot,
        Lifecycle(u8),
        WorkerExit,
        Shutdown,
        DuplicateShutdown,
    }

    fn runtime_actions() -> impl Strategy<Value = Vec<RuntimeAction>> {
        prop::collection::vec(
            prop_oneof![
                4 => Just(RuntimeAction::Tick),
                3 => Just(RuntimeAction::CurrentReadinessAck),
                2 => Just(RuntimeAction::DuplicateReadinessAck),
                3 => any::<u8>().prop_map(RuntimeAction::StaleReadinessAck),
                2 => Just(RuntimeAction::Snapshot),
                2 => any::<u8>().prop_map(RuntimeAction::Lifecycle),
                2 => Just(RuntimeAction::WorkerExit),
                2 => Just(RuntimeAction::Shutdown),
                2 => Just(RuntimeAction::DuplicateShutdown),
            ],
            0..=32,
        )
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    struct RuntimeEvidence {
        ticks: usize,
        readiness: usize,
        snapshots: usize,
        lifecycle: usize,
        shutdowns: usize,
        finishes: usize,
        failed_finishes: usize,
    }

    struct TestAgentRuntimeEffects(Arc<Mutex<RuntimeEvidence>>);

    impl AgentNodeRuntimeEffects for TestAgentRuntimeEffects {
        fn tick_before_reports(&mut self, _node_actor: ActorAddress) -> Result<(), String> {
            self.0.lock().ticks += 1;
            Ok(())
        }

        fn publish_runtime_ready(&mut self, _pending: &PendingRuntimeReady, _ready: &Value) {
            self.0.lock().readiness += 1;
        }

        fn tick_after_reports(
            &mut self,
            _pending: &mut PendingRuntimeReady,
            _node_actor: ActorAddress,
        ) -> Result<(), String> {
            Ok(())
        }

        fn shutdown(&mut self) {
            self.0.lock().shutdowns += 1;
        }

        fn record_finish(&mut self, result: &Result<(), String>) {
            let mut evidence = self.0.lock();
            evidence.finishes += 1;
            evidence.failed_finishes += result.is_err() as usize;
        }
    }

    struct TestWorkerRuntimeEffects {
        evidence: Arc<Mutex<RuntimeEvidence>>,
        fail_next_tick: Arc<Mutex<bool>>,
    }

    impl WorkerNodeRuntimeEffects for TestWorkerRuntimeEffects {
        fn tick_before_reports(&mut self, _node_actor: ActorAddress) -> Result<(), String> {
            self.evidence.lock().ticks += 1;
            Ok(())
        }

        fn handle_report(
            &mut self,
            report: NodeAgentReport,
            _node_actor: ActorAddress,
        ) -> Result<NodeReportOutcome, String> {
            match report {
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
                NodeAgentReport::Snapshot { .. } => {
                    self.evidence.lock().snapshots += 1;
                    Ok(NodeReportOutcome::None)
                }
                NodeAgentReport::Lifecycle(_) => {
                    self.evidence.lock().lifecycle += 1;
                    Ok(NodeReportOutcome::None)
                }
                _ => Ok(NodeReportOutcome::None),
            }
        }

        fn publish_runtime_ready(&mut self, _pending: &PendingRuntimeReady, _ready: &Value) {
            self.evidence.lock().readiness += 1;
        }

        fn tick_after_reports(
            &mut self,
            _pending: &mut PendingRuntimeReady,
            _node_actor: ActorAddress,
        ) -> Result<(), String> {
            if std::mem::take(&mut *self.fail_next_tick.lock()) {
                Err("scripted worker process exit".to_owned())
            } else {
                Ok(())
            }
        }

        fn shutdown(&mut self) -> Result<(), String> {
            self.evidence.lock().shutdowns += 1;
            Ok(())
        }

        fn record_finish(&mut self, result: &Result<(), String>) {
            let mut evidence = self.evidence.lock();
            evidence.finishes += 1;
            evidence.failed_finishes += result.is_err() as usize;
        }
    }

    #[derive(Clone, Debug, Default)]
    struct ExpectedRuntimeEvidence {
        agent_readiness: usize,
        worker_readiness: usize,
        snapshots: usize,
        lifecycle: usize,
        agent_shutdowns: usize,
        worker_shutdowns: usize,
        worker_failed: bool,
    }

    fn check_runtime_evidence(
        expected: &ExpectedRuntimeEvidence,
        agent: &RuntimeEvidence,
        worker: &RuntimeEvidence,
    ) -> Result<(), String> {
        let valid = agent.readiness == expected.agent_readiness
            && worker.readiness == expected.worker_readiness
            && worker.snapshots == expected.snapshots
            && worker.lifecycle == expected.lifecycle
            && agent.shutdowns == expected.agent_shutdowns
            && worker.shutdowns == expected.worker_shutdowns
            && agent.finishes == 1
            && agent.failed_finishes == 0
            && worker.finishes == 1
            && worker.failed_finishes == expected.worker_failed as usize
            && agent.ticks > 0
            && worker.ticks > 0;
        if valid {
            Ok(())
        } else {
            Err(format!(
                "expected={expected:?}, agent={agent:?}, worker={worker:?}"
            ))
        }
    }

    fn pending_runtime_ready(node_actor: ActorAddress) -> PendingRuntimeReady {
        PendingRuntimeReady {
            run_id: 7,
            node_id: 11,
            stage_index: 3,
            endpoint: EndpointAddr::new(SecretKey::from_bytes(&[9; 32]).public()),
            node_actor,
            coordinator: None,
            readiness_id: 99,
            attempts: 0,
            sent: false,
            acked: false,
            membership_logged: false,
            deployment: None,
        }
    }

    fn readiness_report(stale: Option<u8>) -> NodeAgentReport {
        let (mut run_id, mut node_id, mut stage_index, mut readiness_id) = (7, 11, 3, 99);
        if let Some(kind) = stale {
            match kind % 4 {
                0 => run_id += 1,
                1 => node_id += 1,
                2 => stage_index += 1,
                _ => readiness_id += 1,
            }
        }
        NodeAgentReport::RuntimeReadyAck {
            run_id,
            node_id,
            stage_index,
            readiness_id,
        }
    }

    fn send_report_and_tick(
        runtime: &Runtime,
        report_to: ActorAddress,
        actor: ActorAddress,
        report: NodeAgentReport,
    ) {
        let _ = runtime.send_to(report_to, report);
        let _ = runtime.send_to(actor, NodeRuntimeMsg::Progress);
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            max_shrink_iters: 2_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn runtime_actors_generated_transitions_complete_once_on_one_worker(
            actions in runtime_actions()
        ) {
            let parts = RuntimeParts::new(RuntimeConfig {
                worker_count: 1,
                ..RuntimeConfig::default()
            });
            let runtime = parts.runtime().clone();
            let backend = SteppingBackend::new();
            let _engine = Engine::new(parts, backend.clone()).expect("one-worker stepping engine");
            let baseline_actors = runtime.stats().actors.len();
            let baseline_tasks = backend.pending_task_count();
            let agent_reports = runtime.new_inbox::<NodeAgentReport>().expect("agent reports");
            let worker_reports = runtime.new_inbox::<NodeAgentReport>().expect("worker reports");
            let agent_reports_addr = *agent_reports.addr();
            let worker_reports_addr = *worker_reports.addr();
            let agent_node = ActorAddress([4; 32]);
            let worker_node = ActorAddress([5; 32]);
            let agent_completion = ActorCompletion::new();
            let worker_completion = ActorCompletion::new();
            let agent_evidence = Arc::new(Mutex::new(RuntimeEvidence::default()));
            let worker_evidence = Arc::new(Mutex::new(RuntimeEvidence::default()));
            let fail_next_worker_tick = Arc::new(Mutex::new(false));
            let agent_actor = runtime
                .spawn(AgentNodeRuntimeActor {
                    effects: TestAgentRuntimeEffects(Arc::clone(&agent_evidence)),
                    reports: agent_reports,
                    pending_runtime_ready: pending_runtime_ready(agent_node),
                    ready: json!({"type":"ready","identity":99}),
                    node_actor: agent_node,
                    completion: agent_completion.clone(),
                })
                .expect("spawn actual agent runtime actor");
            let worker_actor = runtime
                .spawn(WorkerNodeRuntimeActor {
                    effects: TestWorkerRuntimeEffects {
                        evidence: Arc::clone(&worker_evidence),
                        fail_next_tick: Arc::clone(&fail_next_worker_tick),
                    },
                    reports: worker_reports,
                    pending_runtime_ready: pending_runtime_ready(worker_node),
                    ready: json!({"type":"ready","identity":99}),
                    node_actor: worker_node,
                    completion: worker_completion.clone(),
                })
                .expect("spawn actual worker runtime actor");
            let agent_stop = runtime
                .spawn(StdinStopForwarder {
                    sender: runtime.create_sender(),
                    target: agent_actor,
                })
                .expect("spawn agent stdin forwarder");
            let worker_stop = runtime
                .spawn(StdinStopForwarder {
                    sender: runtime.create_sender(),
                    target: worker_actor,
                })
                .expect("spawn worker stdin forwarder");
            drive_steps(&backend, DRIVE_PER_ACTION);

            let mut expected = ExpectedRuntimeEvidence::default();
            let (mut agent_live, mut worker_live) = (true, true);
            let (mut agent_acked, mut worker_acked) = (false, false);
            for action in &actions {
                match action {
                    RuntimeAction::Tick => {
                        if agent_live {
                            let _ = runtime.send_to(agent_actor, NodeRuntimeMsg::Progress);
                        }
                        if worker_live {
                            let _ = runtime.send_to(worker_actor, NodeRuntimeMsg::Progress);
                        }
                    }
                    RuntimeAction::CurrentReadinessAck
                    | RuntimeAction::DuplicateReadinessAck => {
                        if agent_live {
                            send_report_and_tick(
                                &runtime,
                                agent_reports_addr,
                                agent_actor,
                                readiness_report(None),
                            );
                            if !agent_acked {
                                agent_acked = true;
                                expected.agent_readiness = 1;
                            }
                        }
                        if worker_live {
                            send_report_and_tick(
                                &runtime,
                                worker_reports_addr,
                                worker_actor,
                                readiness_report(None),
                            );
                            if !worker_acked {
                                worker_acked = true;
                                expected.worker_readiness = 1;
                            }
                        }
                    }
                    RuntimeAction::StaleReadinessAck(kind) => {
                        if agent_live {
                            send_report_and_tick(
                                &runtime,
                                agent_reports_addr,
                                agent_actor,
                                readiness_report(Some(*kind)),
                            );
                        }
                        if worker_live {
                            send_report_and_tick(
                                &runtime,
                                worker_reports_addr,
                                worker_actor,
                                readiness_report(Some(*kind)),
                            );
                        }
                    }
                    RuntimeAction::Snapshot => {
                        let report = NodeAgentReport::Snapshot {
                            commands: Vec::new(),
                            events: Vec::new(),
                        };
                        if agent_live {
                            send_report_and_tick(
                                &runtime,
                                agent_reports_addr,
                                agent_actor,
                                report.clone(),
                            );
                        }
                        if worker_live {
                            send_report_and_tick(
                                &runtime,
                                worker_reports_addr,
                                worker_actor,
                                report,
                            );
                            expected.snapshots += 1;
                        }
                    }
                    RuntimeAction::Lifecycle(value) => {
                        let report = NodeAgentReport::Lifecycle(
                            StageLifecycleWire::StageReady {
                                run_id: 7,
                                stage_index: u32::from(*value),
                            },
                        );
                        if agent_live {
                            send_report_and_tick(
                                &runtime,
                                agent_reports_addr,
                                agent_actor,
                                report.clone(),
                            );
                        }
                        if worker_live {
                            send_report_and_tick(
                                &runtime,
                                worker_reports_addr,
                                worker_actor,
                                report,
                            );
                            expected.lifecycle += 1;
                        }
                    }
                    RuntimeAction::WorkerExit if worker_live => {
                        *fail_next_worker_tick.lock() = true;
                        let _ = runtime.send_to(worker_actor, NodeRuntimeMsg::Progress);
                        worker_live = false;
                        expected.worker_failed = true;
                    }
                    RuntimeAction::Shutdown => {
                        if agent_live {
                            let _ =
                                runtime.send_to(agent_stop, swactor_process::ProcessStopSignal);
                            agent_live = false;
                            expected.agent_shutdowns = 1;
                        }
                        if worker_live {
                            let _ =
                                runtime.send_to(worker_stop, swactor_process::ProcessStopSignal);
                            worker_live = false;
                            expected.worker_shutdowns = 1;
                        }
                    }
                    RuntimeAction::DuplicateShutdown => {
                        let _ = runtime.send_to(agent_actor, NodeRuntimeMsg::Shutdown);
                        let _ = runtime.send_to(agent_actor, NodeRuntimeMsg::Shutdown);
                        let _ = runtime.send_to(worker_actor, NodeRuntimeMsg::Shutdown);
                        let _ = runtime.send_to(worker_actor, NodeRuntimeMsg::Shutdown);
                        if agent_live {
                            agent_live = false;
                            expected.agent_shutdowns = 1;
                        }
                        if worker_live {
                            worker_live = false;
                            expected.worker_shutdowns = 1;
                        }
                    }
                    RuntimeAction::WorkerExit => {}
                }
                drive_steps(&backend, DRIVE_PER_ACTION);
            }

            let _ = runtime.send_to(agent_stop, swactor_process::ProcessStopSignal);
            let _ = runtime.send_to(worker_stop, swactor_process::ProcessStopSignal);
            if agent_live {
                expected.agent_shutdowns = 1;
            }
            if worker_live {
                expected.worker_shutdowns = 1;
            }
            drive_steps(&backend, FINAL_DRIVE_BUDGET);

            let before_wait =
                check_runtime_clean(&runtime, &backend, baseline_actors, baseline_tasks);
            prop_assert!(
                before_wait.is_ok(),
                "runtime did not converge within fixed budget: {:?}; actions={:?}; census=\n{}",
                before_wait,
                actions,
                actor_census(&runtime),
            );

            let agent_result = agent_completion.wait();
            let worker_result = worker_completion.wait();
            let agent_observed = agent_evidence.lock().clone();
            let worker_observed = worker_evidence.lock().clone();
            let evidence =
                check_runtime_evidence(&expected, &agent_observed, &worker_observed);
            prop_assert!(
                evidence.is_ok(),
                "runtime invariant failed: {:?}; actions={:?}; expected={:?}; agent={:?}; \
                 worker={:?}; replies=({:?}, {:?}); census=\n{}",
                evidence,
                actions,
                expected,
                agent_observed,
                worker_observed,
                agent_result,
                worker_result,
                actor_census(&runtime),
            );
            prop_assert!(agent_result.is_ok(), "actions={:?}, agent_reply={:?}", actions, agent_result);
            prop_assert_eq!(
                worker_result.is_err(),
                expected.worker_failed,
                "actions={:?}, worker_reply={:?}, census=\n{}",
                actions,
                worker_result,
                actor_census(&runtime),
            );
            let clean =
                check_runtime_clean(&runtime, &backend, baseline_actors, baseline_tasks);
            prop_assert!(
                clean.is_ok(),
                "runtime residue: {:?}; actions={:?}; replies=({:?}, {:?}); census=\n{}",
                clean,
                actions,
                agent_result,
                worker_result,
                actor_census(&runtime),
            );
        }
    }

    #[test]
    fn runtime_invariant_checker_rejects_duplicate_readiness_publication() {
        let expected = ExpectedRuntimeEvidence {
            agent_readiness: 1,
            agent_shutdowns: 1,
            worker_shutdowns: 1,
            ..ExpectedRuntimeEvidence::default()
        };
        let agent = RuntimeEvidence {
            ticks: 1,
            readiness: 2,
            shutdowns: 1,
            finishes: 1,
            ..RuntimeEvidence::default()
        };
        let worker = RuntimeEvidence {
            ticks: 1,
            shutdowns: 1,
            finishes: 1,
            ..RuntimeEvidence::default()
        };
        assert!(check_runtime_evidence(&expected, &agent, &worker).is_err());
    }

    #[derive(Clone, Debug)]
    enum HelperAction {
        Other,
        Expected(u8),
        Malformed,
        Fatal,
        ReadError,
        Closed,
        Stderr(u8),
    }

    fn helper_actions() -> impl Strategy<Value = Vec<HelperAction>> {
        prop::collection::vec(
            prop_oneof![
                3 => Just(HelperAction::Other),
                2 => any::<u8>().prop_map(HelperAction::Expected),
                2 => Just(HelperAction::Malformed),
                2 => Just(HelperAction::Fatal),
                1 => Just(HelperAction::ReadError),
                1 => Just(HelperAction::Closed),
                1 => any::<u8>().prop_map(HelperAction::Stderr),
            ],
            0..=32,
        )
    }
    fn expected_helper_success(actions: &[HelperAction]) -> bool {
        for action in actions {
            match action {
                HelperAction::Expected(_) => return true,
                HelperAction::Malformed
                | HelperAction::Fatal
                | HelperAction::ReadError
                | HelperAction::Closed => return false,
                HelperAction::Other | HelperAction::Stderr(_) => {}
            }
        }
        false
    }

    fn check_helper_classification(
        actions: &[HelperAction],
        observed_success: bool,
    ) -> Result<(), String> {
        let expected = expected_helper_success(actions);
        if observed_success == expected {
            Ok(())
        } else {
            Err(format!(
                "helper success={observed_success}, expected={expected}, actions={actions:?}"
            ))
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            max_shrink_iters: 2_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn helper_wait_generated_terminal_sequences_complete_once_on_one_worker(
            actions in helper_actions(),
            extra_ticks in 0_usize..=8,
        ) {
            let parts = RuntimeParts::new(RuntimeConfig {
                worker_count: 1,
                ..RuntimeConfig::default()
            });
            let runtime = parts.runtime().clone();
            let backend = SteppingBackend::new();
            let _engine = Engine::new(parts, backend.clone()).expect("one-worker stepping engine");
            let baseline_actors = runtime.stats().actors.len();
            let baseline_tasks = backend.pending_task_count();
            let (stdout_tx, stdout_rx) = mpsc::channel();
            let (stderr_tx, stderr_rx) = mpsc::channel();

            for action in &actions {
                match *action {
                    HelperAction::Other => {
                        stdout_tx
                            .send(HelperStdoutEvent::Line(
                                serde_json::json!({"type":"Progress","command_id":1}).to_string(),
                            ))
                            .expect("queue helper progress");
                    }
                    HelperAction::Expected(value) => {
                        stdout_tx
                            .send(HelperStdoutEvent::Line(
                                serde_json::json!({"type":"Expected","value":value,"command_id":1}).to_string(),
                            ))
                            .expect("queue expected helper event");
                    }
                    HelperAction::Malformed => {
                        stdout_tx
                            .send(HelperStdoutEvent::Line("{broken".to_owned()))
                            .expect("queue malformed helper event");
                    }
                    HelperAction::Fatal => {
                        stdout_tx
                            .send(HelperStdoutEvent::Line(
                                serde_json::json!({"type":"WorkerFatal","reason":"scripted","command_id":1})
                                    .to_string(),
                            ))
                            .expect("queue fatal helper event");
                    }
                    HelperAction::ReadError => {
                        stdout_tx
                            .send(HelperStdoutEvent::ReadError("scripted read error".to_owned()))
                            .expect("queue helper read error");
                    }
                    HelperAction::Closed => {
                        stdout_tx
                            .send(HelperStdoutEvent::Closed)
                            .expect("queue helper close");
                    }
                    HelperAction::Stderr(value) => {
                        stderr_tx
                            .send(format!("stderr-{value}"))
                            .expect("queue helper stderr");
                    }
                }
            }
            drop(stdout_tx);
            drop(stderr_tx);

            let completion = ActorCompletion::new();
            let actor = runtime
                .spawn(HelperWaitActor {
                    stdout_rx: Arc::new(Mutex::new(stdout_rx)),
                    stderr_rx: Some(Arc::new(Mutex::new(stderr_rx))),
                    expected: "Expected".to_owned(),
                    command_id: 1,
                    wake: Arc::new(Mutex::new(None)),
                    resource_samples: Arc::new(Mutex::new(Vec::new())),
                    completion: completion.clone(),
                    lines: Vec::new(),
                    stderr_lines: Vec::new(),
                })
                .expect("spawn helper wait actor");
            for _ in 0..extra_ticks {
                let _ = runtime.send_to(actor, HelperOutputReady);
            }
            drive_steps(&backend, FINAL_DRIVE_BUDGET);

            let before_wait =
                check_runtime_clean(&runtime, &backend, baseline_actors, baseline_tasks);
            prop_assert!(
                before_wait.is_ok(),
                "helper did not converge within fixed budget: {:?}; actions={:?}; census=\n{}",
                before_wait,
                actions,
                actor_census(&runtime),
            );

            let outcome = completion.wait();
            let classification =
                check_helper_classification(&actions, outcome.result.is_ok());
            prop_assert!(
                classification.is_ok(),
                "helper invariant failed: {:?}; actions={:?}; result={:?}; lines={:?}; \
                 stderr={:?}; census=\n{}",
                classification,
                actions,
                outcome.result,
                outcome.lines,
                outcome.stderr_lines,
                actor_census(&runtime),
            );
            prop_assert!(
                outcome.lines.len() <= actions.len(),
                "actions={:?}, lines={:?}, census=\n{}",
                actions,
                outcome.lines,
                actor_census(&runtime),
            );
            prop_assert!(
                outcome.stderr_lines.len() <= actions.len(),
                "actions={:?}, stderr={:?}, census=\n{}",
                actions,
                outcome.stderr_lines,
                actor_census(&runtime),
            );
            let clean =
                check_runtime_clean(&runtime, &backend, baseline_actors, baseline_tasks);
            prop_assert!(
                clean.is_ok(),
                "helper residue: {:?}; actions={:?}; result={:?}; census=\n{}",
                clean,
                actions,
                outcome.result,
                actor_census(&runtime),
            );
        }
    }
    #[test]
    fn helper_invariant_checker_rejects_expected_output_after_terminal_error() {
        for actions in [
            vec![HelperAction::Fatal, HelperAction::Expected(1)],
            vec![HelperAction::Closed, HelperAction::Expected(1)],
        ] {
            assert!(!expected_helper_success(&actions));
            assert!(check_helper_classification(&actions, true).is_err());
        }
    }

    #[test]
    fn helper_wait_requires_current_command_completion_after_output_wake() {
        let parts = RuntimeParts::new(RuntimeConfig {
            worker_count: 1,
            ..RuntimeConfig::default()
        });
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let _engine = Engine::new(parts, backend.clone()).expect("stepping engine");
        let (stdout_tx, stdout_rx) = mpsc::channel();
        let completion = ActorCompletion::new();
        let actor = runtime
            .spawn(HelperWaitActor {
                stdout_rx: Arc::new(Mutex::new(stdout_rx)),
                stderr_rx: None,
                expected: "Expected".to_owned(),
                command_id: 2,
                wake: Arc::new(Mutex::new(None)),
                resource_samples: Arc::new(Mutex::new(Vec::new())),
                completion: completion.clone(),
                lines: Vec::new(),
                stderr_lines: Vec::new(),
            })
            .expect("helper wait actor");
        drive_steps(&backend, DRIVE_PER_ACTION);
        for command_id in [1, 2] {
            stdout_tx
                .send(HelperStdoutEvent::Line(
                    json!({"type":"Expected","command_id":command_id}).to_string(),
                ))
                .expect("helper output");
        }
        runtime
            .send_to(actor, HelperOutputReady)
            .expect("output observation");
        drive_steps(&backend, FINAL_DRIVE_BUDGET);
        assert!(
            runtime
                .stats()
                .actors
                .iter()
                .all(|(address, _)| *address != actor)
        );
        let result = completion.wait().result.expect("current command completed");
        assert_eq!(result["command_id"], 2);
    }

    #[derive(Clone, Debug)]
    enum StageAction {
        Output(u8),
        MalformedOutput,
        ReadyOutput,
        ReadyMissing,
        Stderr(u8),
        ReaderError(bool),
        CloseStdout,
        CloseStderr,
        ExitSuccess,
        ExitFailure,
    }

    fn stage_actions() -> impl Strategy<Value = Vec<StageAction>> {
        prop::collection::vec(
            prop_oneof![
                3 => any::<u8>().prop_map(StageAction::Output),
                2 => Just(StageAction::MalformedOutput),
                2 => Just(StageAction::ReadyOutput),
                2 => Just(StageAction::ReadyMissing),
                2 => any::<u8>().prop_map(StageAction::Stderr),
                1 => any::<bool>().prop_map(StageAction::ReaderError),
                2 => Just(StageAction::CloseStdout),
                2 => Just(StageAction::CloseStderr),
                2 => Just(StageAction::ExitSuccess),
                2 => Just(StageAction::ExitFailure),
            ],
            0..=32,
        )
    }

    #[derive(Clone, Debug)]
    struct StageModel {
        stdout_closed: bool,
        stderr_closed: bool,
        exit_success: Option<bool>,
        ready_exists: Option<bool>,
        events: usize,
        finished: bool,
        success: bool,
    }

    impl StageModel {
        fn new() -> Self {
            Self {
                stdout_closed: false,
                stderr_closed: false,
                exit_success: None,
                ready_exists: None,
                events: 0,
                finished: false,
                success: false,
            }
        }

        fn observe(&mut self, action: &StageAction) {
            if self.finished {
                return;
            }
            match action {
                StageAction::Output(_) | StageAction::MalformedOutput | StageAction::Stderr(_) => {
                    self.events += 1;
                }
                StageAction::ReadyOutput => {
                    self.ready_exists = Some(true);
                    self.events += 1;
                }
                StageAction::ReadyMissing => {
                    self.ready_exists = Some(false);
                    self.events += 1;
                }
                StageAction::ReaderError(_) => self.events += 1,
                StageAction::CloseStdout => self.stdout_closed = true,
                StageAction::CloseStderr => self.stderr_closed = true,
                StageAction::ExitSuccess if self.exit_success.is_none() => {
                    self.exit_success = Some(true);
                }
                StageAction::ExitFailure if self.exit_success.is_none() => {
                    self.exit_success = Some(false);
                }
                StageAction::ExitSuccess | StageAction::ExitFailure => {}
            }
            if let Some(exit_success) = self.exit_success
                && self.stdout_closed
                && self.stderr_closed
            {
                self.finished = true;
                self.success = exit_success && self.ready_exists.unwrap_or(true);
            }
        }
    }

    fn check_stage_outcome(
        model: &StageModel,
        observed_success: bool,
        observed_events: usize,
    ) -> Result<(), String> {
        if observed_success == model.success && observed_events == model.events {
            Ok(())
        } else {
            Err(format!(
                "success={observed_success}/{} events={observed_events}/{} model={model:?}",
                model.success, model.events
            ))
        }
    }

    fn stage_message(
        action: &StageAction,
        output_path: &Path,
        missing_path: &Path,
    ) -> StageShardFetchMsg {
        match action {
            StageAction::Output(value) => StageShardFetchMsg::ProcessLine {
                stream: StageShardProcessStream::Stdout,
                line: json!({"type":"StageShardProgress","value":value}).to_string(),
            },
            StageAction::MalformedOutput => StageShardFetchMsg::ProcessLine {
                stream: StageShardProcessStream::Stdout,
                line: "{broken".to_owned(),
            },
            StageAction::ReadyOutput => StageShardFetchMsg::ProcessLine {
                stream: StageShardProcessStream::Stdout,
                line: json!({"type":"StageShardReady","path":output_path}).to_string(),
            },
            StageAction::ReadyMissing => StageShardFetchMsg::ProcessLine {
                stream: StageShardProcessStream::Stdout,
                line: json!({"type":"StageShardReady","path":missing_path}).to_string(),
            },
            StageAction::Stderr(value) => StageShardFetchMsg::ProcessLine {
                stream: StageShardProcessStream::Stderr,
                line: format!("stderr-{value}"),
            },
            StageAction::ReaderError(stdout) => StageShardFetchMsg::ReaderError {
                stream: if *stdout {
                    StageShardProcessStream::Stdout
                } else {
                    StageShardProcessStream::Stderr
                },
                error: "scripted reader error".to_owned(),
            },
            StageAction::CloseStdout => StageShardFetchMsg::ReaderClosed {
                stream: StageShardProcessStream::Stdout,
            },
            StageAction::CloseStderr => StageShardFetchMsg::ReaderClosed {
                stream: StageShardProcessStream::Stderr,
            },
            StageAction::ExitSuccess => {
                StageShardFetchMsg::ProcessExited(std::process::ExitStatus::from_raw(0))
            }
            StageAction::ExitFailure => {
                StageShardFetchMsg::ProcessExited(std::process::ExitStatus::from_raw(1 << 8))
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            max_shrink_iters: 2_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn stage_fetch_generated_observations_complete_once_on_one_worker(
            actions in stage_actions()
        ) {
            let parts = RuntimeParts::new(RuntimeConfig {
                worker_count: 1,
                ..RuntimeConfig::default()
            });
            let runtime = parts.runtime().clone();
            let backend = SteppingBackend::new();
            let engine = Engine::new(parts, backend.clone()).expect("one-worker stepping engine");
            let baseline_actors = runtime.stats().actors.len();
            let baseline_tasks = backend.pending_task_count();
            let output = tempfile::NamedTempFile::new().expect("stage output file");
            let output_path = output.path().to_path_buf();
            let missing_path = output_path.with_extension("missing");
            let completion = ActorCompletion::new();
            let mut stage_actor = StageShardFetchActor::new(
                Vec::new(),
                output_path.clone(),
                completion.clone(),
                runtime.create_sender(),
                engine.handle(),
            );
            stage_actor.stdout_closed = false;
            stage_actor.stderr_closed = false;
            let actor = runtime.spawn(stage_actor).expect("spawn actual stage fetch actor");
            let mut model = StageModel::new();

            for action in &actions {
                let _ =
                    runtime.send_to(actor, stage_message(action, &output_path, &missing_path));
                model.observe(action);
                drive_steps(&backend, DRIVE_PER_ACTION);
            }
            for terminal in [
                StageAction::CloseStdout,
                StageAction::CloseStderr,
                StageAction::ExitSuccess,
            ] {
                if !model.finished {
                    let _ = runtime.send_to(
                        actor,
                        stage_message(&terminal, &output_path, &missing_path),
                    );
                    model.observe(&terminal);
                    drive_steps(&backend, DRIVE_PER_ACTION);
                }
            }
            drive_steps(&backend, FINAL_DRIVE_BUDGET);

            let before_wait =
                check_runtime_clean(&runtime, &backend, baseline_actors, baseline_tasks);
            prop_assert!(
                before_wait.is_ok(),
                "stage did not converge within fixed budget: {:?}; actions={:?}; model={:?}; census=\n{}",
                before_wait,
                actions,
                model,
                actor_census(&runtime),
            );

            let outcome = completion.wait();
            let checked =
                check_stage_outcome(&model, outcome.result.is_ok(), outcome.events.len());
            prop_assert!(
                checked.is_ok(),
                "stage invariant failed: {:?}; actions={:?}; model={:?}; result={:?}; \
                 events={:?}; census=\n{}",
                checked,
                actions,
                model,
                outcome.result,
                outcome.events,
                actor_census(&runtime),
            );
            let clean =
                check_runtime_clean(&runtime, &backend, baseline_actors, baseline_tasks);
            prop_assert!(
                clean.is_ok(),
                "stage residue: {:?}; actions={:?}; result={:?}; events={:?}; census=\n{}",
                clean,
                actions,
                outcome.result,
                outcome.events,
                actor_census(&runtime),
            );
        }
    }

    #[test]
    fn stage_invariant_checker_rejects_wrong_terminal_classification() {
        let model = StageModel {
            stdout_closed: true,
            stderr_closed: true,
            exit_success: Some(false),
            ready_exists: Some(true),
            events: 1,
            finished: true,
            success: false,
        };
        assert!(check_stage_outcome(&model, true, 1).is_err());
    }
}
