//! Worker-node runtime behavior behind the `mvp_system::node` boundary.
//!
//! The `mvp-worker-node` binary remains a thin entrypoint wrapper; this module
//! owns the reusable node-local runtime, helper-command, datastream archive,
//! debug-join, staging, transport, and worker coordination behavior.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, ExitCode, Stdio};
use std::sync::{
    Arc,
    mpsc::{self, Receiver, RecvTimeoutError, Sender},
};
use std::thread;
use std::time::{Duration, Instant};

use datastream::{
    ChannelContent, ChannelId, DATASTREAM_PUBLISHER_NAME, DatastreamEndpoint, DatastreamEvent,
    DatastreamProducer, DatastreamPublisherActor, DatastreamSubscribe, DatastreamSubscription,
    Lifetime, NodeId, Record, StreamDescriptor, StreamId, StreamOrigin,
};

use crate::driver_pumps as driver_model;
use crate::gguf_shard::{StageShardPlan, materialize_stage_shard_http, validate_stage_shard_cache};
use crate::node_actor::{
    NodeAgentActor, NodeAgentMsg, NodeAgentReport, StageCommandWire, StageInboundEdgeWire,
    StageObjectSpecWire, StageOutboundEdgeWire,
};
use crate::observability::benchmark;
use crate::orchestration::distribution_stack::DistributionRuntimeStack;
use crate::orchestration::provider_adapters::relay::relay_runtime_config_from_env;
use crate::prompt::rpc::{PromptEvent, TokenizerEvent};
use crate::run_plan::{GgufSource, TokenizerSource};
use crate::staging::control as stage;
use crate::transport::codec_registry::register_mvp_actor_codecs;
use crate::transport::endpoint_advertisement::{
    EndpointAddrMask, MVP_IROH_ENDPOINT_ADDR_MASK_ENV, advertised_endpoint,
};
use data_plane::arena;
use data_plane::edge_lifecycle as edge;
use data_plane::ingress;
use distribution::node::DistributedNodeConfig;
use distribution::swim::telemetry::ObservedProbeEvent;
use distribution::telemetry::{MembershipTransition, SwimProbeEvent};
use distribution::types::{MemberState, NodeId as DistNodeId};
use iroh::EndpointAddr;
use iroh_driver::{
    DATASTREAM_ALPN, DatastreamPublishHandle, DatastreamQuicHeader, EDGE_ALPN, EdgeSendHandle,
    EdgeTransportEvent, IrohDriver, IrohDriverConfig,
};
use parking_lot::Mutex;
use serde_json::{Value, json};
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, ExternalSender};
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
const NODE_SAMPLER_CHANNEL: &str = "mvp.node.sampler";
const WORKER_COMMAND_WAIT_TELEMETRY_INTERVAL: Duration = Duration::from_secs(1);

fn worker_benchmark_stamp(run_id: u64, node_id: u64) -> Value {
    let mut benchmark = benchmark::stamp("mvp-worker-node");
    let pid = benchmark
        .get("producer_process_id")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| u64::from(std::process::id()));
    let producer_instance_id = format!("mvp-worker-node:{run_id}:node-{node_id}:pid-{pid}");
    if let Some(object) = benchmark.as_object_mut() {
        object.insert(
            "producer_instance_id".to_owned(),
            json!(producer_instance_id),
        );
    }
    benchmark
}

fn node_event_payload(
    config: &DeploymentConfig,
    phase: &str,
    status: &str,
    detail: Value,
) -> Value {
    let benchmark = worker_benchmark_stamp(config.run_id, config.logical_node_id);
    json!({
        "schema_version": benchmark["schema_version"].clone(),
        "type":"NodeEvent",
        "event_type":"NodeEvent",
        "event_name":phase,
        "phase":phase,
        "status":status,
        "run_id":config.run_id,
        "node_id":config.logical_node_id,
        "stage_index":config.stage_index,
        "producer_component":benchmark["producer_component"].clone(),
        "producer_instance_id":benchmark["producer_instance_id"].clone(),
        "producer_process_id":benchmark["producer_process_id"].clone(),
        "producer_sequence":benchmark["producer_sequence"].clone(),
        "wall_clock_unix_ms":benchmark["wall_clock_unix_ms"].clone(),
        "monotonic_ms":benchmark["monotonic_ms"].clone(),
        "clock_source":benchmark["clock_source"].clone(),
        "span_id":format!("mvp-worker-node:{}:{}:{}:{phase}", config.run_id, config.logical_node_id, benchmark["producer_sequence"]),
        "parent_span_id":Value::Null,
        "benchmark":benchmark,
        "detail":detail,
    })
}

fn emit_stdio_datastream_frame(channel: &str, payload: &Value) -> Result<(), String> {
    println!(
        "{}",
        json!({
            "mvp_stdio_event":1,
            "kind":"datastream_frame",
            "channel":channel,
            "payload":payload,
        })
    );
    std::io::stdout()
        .flush()
        .map_err(|e| format!("flush stdio datastream frame: {e}"))
}

fn emit_stdio_node_event(
    config: &DeploymentConfig,
    channel: &str,
    phase: &str,
    status: &str,
    detail: Value,
) -> Result<(), String> {
    let payload = node_event_payload(config, phase, status, detail);
    emit_stdio_datastream_frame(channel, &payload)
}

fn emit_node_event(
    datastream: &mut NodeDatastream,
    config: &DeploymentConfig,
    channel: &str,
    phase: &str,
    status: &str,
    detail: Value,
) {
    let channel = datastream.channel_by_name(channel);
    datastream.submit_text(
        channel,
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
    datastream: &mut NodeDatastream,
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

fn sampler_health_payload(
    context: SamplerHealthContext,
    sampler: &str,
    sample_channel: &str,
    status: &str,
    detail: Value,
) -> Value {
    let benchmark = worker_benchmark_stamp(context.run_id, context.node_id);
    json!({
        "schema_version":benchmark["schema_version"].clone(),
        "type":"SamplerHealth",
        "event_type":"SamplerHealth",
        "event_name":"host_sampler_health",
        "schema":"mvp.node.sampler.health.v1",
        "run_id":context.run_id,
        "node_id":context.node_id,
        "stage_index":context.stage_index,
        "producer_component":benchmark["producer_component"].clone(),
        "producer_instance_id":benchmark["producer_instance_id"].clone(),
        "producer_process_id":benchmark["producer_process_id"].clone(),
        "producer_sequence":benchmark["producer_sequence"].clone(),
        "wall_clock_unix_ms":benchmark["wall_clock_unix_ms"].clone(),
        "monotonic_ms":benchmark["monotonic_ms"].clone(),
        "clock_source":benchmark["clock_source"].clone(),
        "span_id":format!("mvp-worker-node:{}:{}:{}:host_sampler_health", context.run_id, context.node_id, benchmark["producer_sequence"]),
        "parent_span_id":Value::Null,
        "phase":"host_sampler_health",
        "status":status,
        "sampler":sampler,
        "sample_channel":sample_channel,
        "detail":detail,
        "benchmark":benchmark,
    })
}

fn submit_sampler_health(
    producer: &DatastreamProducer,
    channel: ChannelId,
    context: SamplerHealthContext,
    sampler: &str,
    sample_channel: &str,
    status: &str,
    detail: Value,
) {
    producer.submit_text(
        channel,
        sampler_health_payload(context, sampler, sample_channel, status, detail).to_string(),
    );
}

fn submit_sampler_started(
    producer: &DatastreamProducer,
    health_channel: ChannelId,
    context: SamplerHealthContext,
    sampler: &str,
    sample_channel: &str,
    interval: Duration,
) {
    submit_sampler_health(
        producer,
        health_channel,
        context,
        sampler,
        sample_channel,
        "started",
        json!({"state":"started","sample_interval_ms":duration_ms_u64(interval)}),
    );
    submit_sampler_health(
        producer,
        health_channel,
        context,
        sampler,
        sample_channel,
        "waiting",
        json!({"state":"no_sample_yet","sample_interval_ms":duration_ms_u64(interval)}),
    );
}

fn submit_sampler_sample_health(
    producer: &DatastreamProducer,
    health_channel: ChannelId,
    context: SamplerHealthContext,
    sampler: &str,
    sample_channel: &str,
    seq: u64,
    error: Option<&str>,
) {
    match error {
        Some(error) => submit_sampler_health(
            producer,
            health_channel,
            context,
            sampler,
            sample_channel,
            "failed",
            json!({"state":"error","sample_seq":seq,"error":error}),
        ),
        None => submit_sampler_health(
            producer,
            health_channel,
            context,
            sampler,
            sample_channel,
            "ready",
            json!({"state":"sample_observed","sample_seq":seq}),
        ),
    }
}

fn spawn_host_gpu_sampler(
    handle: tokio::runtime::Handle,
    producer: DatastreamProducer,
    channel: ChannelId,
    health_channel: ChannelId,
    health_context: SamplerHealthContext,
) {
    handle.spawn(async move {
        let sample_channel = datastream::hardware::gpu::HOST_GPU_CHANNEL;
        submit_sampler_started(
            &producer,
            health_channel,
            health_context,
            "gpu",
            sample_channel,
            datastream::hardware::gpu::GPU_SAMPLE_INTERVAL,
        );
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

            submit_sampler_sample_health(
                &producer,
                health_channel,
                health_context,
                "gpu",
                sample_channel,
                sample_seq,
                sample.error.as_deref(),
            );
            seq = seq.saturating_add(1);
            producer.submit_record(channel, &sample);
        }
    });
}

fn spawn_host_cpu_sampler(
    handle: tokio::runtime::Handle,
    producer: DatastreamProducer,
    channel: ChannelId,
    health_channel: ChannelId,
    health_context: SamplerHealthContext,
    watched_pids: Vec<u32>,
) {
    handle.spawn(async move {
        let sample_channel = datastream::hardware::cpu::HOST_CPU_CHANNEL;
        submit_sampler_started(
            &producer,
            health_channel,
            health_context,
            "cpu",
            sample_channel,
            datastream::hardware::cpu::CPU_SAMPLE_INTERVAL,
        );
        let mut seq = 0_u64;
        let mut sampler = datastream::hardware::cpu::CpuSampler::new(watched_pids);
        let mut interval = tokio::time::interval(datastream::hardware::cpu::CPU_SAMPLE_INTERVAL);

        loop {
            interval.tick().await;

            let sample = sampler.sample(seq);
            submit_sampler_sample_health(
                &producer,
                health_channel,
                health_context,
                "cpu",
                sample_channel,
                seq,
                sample.error.as_deref(),
            );
            seq = seq.saturating_add(1);
            producer.submit_record(channel, &sample);
        }
    });
}
fn spawn_host_net_sampler(
    handle: tokio::runtime::Handle,
    producer: DatastreamProducer,
    channel: ChannelId,
    health_channel: ChannelId,
    health_context: SamplerHealthContext,
) {
    handle.spawn(async move {
        let sample_channel = datastream::hardware::net::HOST_NET_CHANNEL;
        submit_sampler_started(
            &producer,
            health_channel,
            health_context,
            "net",
            sample_channel,
            datastream::hardware::net::HOST_NET_SAMPLE_INTERVAL,
        );
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

            submit_sampler_sample_health(
                &producer,
                health_channel,
                health_context,
                "net",
                sample_channel,
                sample_seq,
                sample.error.as_deref(),
            );
            seq = seq.saturating_add(1);
            producer.submit_record(channel, &sample);
        }
    });
}

fn spawn_arena_sampler(
    handle: tokio::runtime::Handle,
    producer: DatastreamProducer,
    channel: ChannelId,
    arena_manager: Arc<Mutex<arena::ArenaManager>>,
) {
    handle.spawn(async move {
        let mut seq = 0_u64;
        let mut interval = tokio::time::interval(arena::ARENA_SAMPLE_INTERVAL);

        loop {
            interval.tick().await;

            let sample: arena::ArenaSample = arena_manager.lock().sample(seq).into();
            seq = seq.saturating_add(1);
            producer.submit_record(channel, &sample);
        }
    });
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ObjectKey {
    edge_id: u64,
    object_id: u64,
}

#[derive(Clone, Debug)]
struct LoadedObject {
    object_id: u64,
    sequence: u64,
    handle_generation: u64,
    handle_id: u64,
}

struct WorkerEdgeRuntime {
    establisher: edge::EdgeEstablisher,
    driver_model: driver_model::Driver,
    edge_command_cursor: usize,
    edge_event_cursor: usize,
    driver_event_cursor: usize,
    inbound_edge: Option<StageInboundEdgeWire>,
    outbound_edge: Option<StageOutboundEdgeWire>,
    inbound_ring_id: Option<u64>,
    outbound_ring_id: Option<u64>,
    outbound_sender: Option<EdgeSendHandle>,
    next_output_object_id: u64,
    object_handles: BTreeMap<ObjectKey, LoadedObject>,
    ingress_streams: BTreeMap<u64, Vec<u8>>,
}

impl WorkerEdgeRuntime {
    fn new(local_node_id: u64) -> Self {
        Self {
            establisher: edge::EdgeEstablisher::new(edge::NodeId(local_node_id)),
            driver_model: driver_model::Driver::new(driver_model::DriverConfig {
                local_node_id: driver_model::NodeId(local_node_id),
                alpn: driver_model::Alpn(String::from_utf8_lossy(EDGE_ALPN).into_owned()),
            }),
            edge_command_cursor: 0,
            edge_event_cursor: 0,
            driver_event_cursor: 0,
            inbound_edge: None,
            outbound_edge: None,
            inbound_ring_id: None,
            outbound_ring_id: None,
            outbound_sender: None,
            next_output_object_id: 1,
            object_handles: BTreeMap::new(),
            ingress_streams: BTreeMap::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn poll_iroh(
        &mut self,
        driver: &mut IrohDriver,
        stack: &DistributionRuntimeStack,
        node_actor: ActorAddress,
        worker: &mut TinygradWorker,
        arena_manager: &Arc<Mutex<arena::ArenaManager>>,
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
    ) -> Result<(), String> {
        driver.pump_edge_ingress();
        for event in driver.drain_edge_events() {
            match event {
                EdgeTransportEvent::StreamArrived {
                    edge_id, stream_id, ..
                } => {
                    self.driver_model
                        .observe(driver_model::DriverEvent::IncomingUniStream {
                            edge_id: driver_model::EdgeId(edge_id),
                            stream_id: driver_model::StreamId(stream_id),
                        });
                    emit_node_event(
                        datastream,
                        config,
                        NODE_STAGE_CHANNEL,
                        "iroh_edge_stream_arrived",
                        "observed",
                        json!({"edge_id":edge_id,"stream_id":stream_id}),
                    );
                    self.drive_edge_workflow(
                        stack,
                        node_actor,
                        worker,
                        arena_manager,
                        config,
                        datastream,
                        driver,
                    )?;
                }
                EdgeTransportEvent::BytesRead {
                    edge_id,
                    stream_id,
                    bytes,
                    ..
                } => {
                    let byte_count = bytes.len();
                    emit_node_event(
                        datastream,
                        config,
                        NODE_STAGE_CHANNEL,
                        "iroh_edge_bytes_read",
                        "observed",
                        json!({"edge_id":edge_id,"stream_id":stream_id,"bytes":byte_count}),
                    );
                    self.ingest_stream_bytes(
                        edge_id,
                        stream_id,
                        bytes,
                        stack,
                        node_actor,
                        worker,
                        arena_manager,
                        config,
                        datastream,
                        driver,
                    )?;
                }
                EdgeTransportEvent::StreamEnded { .. } => {}
                EdgeTransportEvent::StreamFault {
                    edge_id: Some(edge_id),
                    ..
                } => {
                    self.driver_model
                        .observe(driver_model::DriverEvent::ReadError {
                            edge_id: driver_model::EdgeId(edge_id),
                        });
                    self.drive_edge_workflow(
                        stack,
                        node_actor,
                        worker,
                        arena_manager,
                        config,
                        datastream,
                        driver,
                    )?;
                }
                EdgeTransportEvent::StreamFault { edge_id: None, .. } => {}
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn establish_inbound(
        &mut self,
        edge: StageInboundEdgeWire,
        stack: &DistributionRuntimeStack,
        node_actor: ActorAddress,
        worker: &mut TinygradWorker,
        arena_manager: &Arc<Mutex<arena::ArenaManager>>,
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
        driver: &mut IrohDriver,
    ) -> Result<(), String> {
        self.inbound_edge = Some(edge.clone());
        self.establisher
            .observe(edge::EdgeEvent::ProvisionRx(edge::ProvisionRx {
                run_id: edge::RunId(config.run_id),
                edge_id: edge::EdgeId(edge.edge_id),
                local_node_id: edge::NodeId(config.logical_node_id),
                object_spec: edge::ObjectSpec {
                    kind: edge::ObjectKind::Activation,
                    dtype: edge::DType::F16,
                    max_extent_bytes: edge.object_spec.max_extent,
                },
                ring_spec: edge::RingSpec {
                    header_bytes: 0,
                    data_bytes: edge.ring_spec.data_capacity,
                    alignment: u64::from(edge.ring_spec.alignment),
                },
            }));
        self.drive_edge_workflow(
            stack,
            node_actor,
            worker,
            arena_manager,
            config,
            datastream,
            driver,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn establish_outbound(
        &mut self,
        edge: StageOutboundEdgeWire,
        stack: &DistributionRuntimeStack,
        node_actor: ActorAddress,
        worker: &mut TinygradWorker,
        arena_manager: &Arc<Mutex<arena::ArenaManager>>,
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
        driver: &mut IrohDriver,
    ) -> Result<(), String> {
        if edge.consumer_endpoint.is_none() {
            stack
                .runtime
                .send_to(
                    node_actor,
                    NodeAgentMsg::MarkOutboundEdgeReady {
                        edge_id: edge.edge_id,
                    },
                )
                .map_err(|e| format!("mark outbound edge ready: {e}"))?;
            self.outbound_edge = Some(edge);
            return Ok(());
        }
        self.outbound_edge = Some(edge.clone());
        self.establisher
            .observe(edge::EdgeEvent::ProvisionTx(edge::ProvisionTx {
                run_id: edge::RunId(config.run_id),
                edge_id: edge::EdgeId(edge.edge_id),
                local_node_id: edge::NodeId(config.logical_node_id),
                consumer_node_id: edge::NodeId(edge.consumer_node_id),
                object_spec: edge::ObjectSpec {
                    kind: edge::ObjectKind::Activation,
                    dtype: edge::DType::F16,
                    max_extent_bytes: edge.object_spec.max_extent,
                },
                ring_spec: edge::RingSpec {
                    header_bytes: 0,
                    data_bytes: edge.ring_spec.data_capacity,
                    alignment: u64::from(edge.ring_spec.alignment),
                },
            }));
        self.drive_edge_workflow(
            stack,
            node_actor,
            worker,
            arena_manager,
            config,
            datastream,
            driver,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_step(
        &mut self,
        step_id: u64,
        input_edge_id: u64,
        object_id: u64,
        sequence: u64,
        stack: &DistributionRuntimeStack,
        node_actor: ActorAddress,
        worker: &mut TinygradWorker,
        arena_manager: &Arc<Mutex<arena::ArenaManager>>,
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
        driver: &mut IrohDriver,
    ) -> Result<(), String> {
        let input_key = ObjectKey {
            edge_id: input_edge_id,
            object_id,
        };
        let loaded = self
            .object_handles
            .get(&input_key)
            .cloned()
            .ok_or_else(|| format!("object {input_key:?} has no loaded device handle"))?;
        if loaded.sequence != sequence {
            return Err(format!(
                "object {input_key:?} sequence {} does not match command sequence {sequence}",
                loaded.sequence
            ));
        }
        let outbound = self
            .outbound_edge
            .clone()
            .ok_or_else(|| "outbound edge missing".to_owned())?;
        let output_ring_id = self
            .outbound_ring_id
            .ok_or_else(|| "outbound ring missing".to_owned())?;
        let output_object_id = self.next_output_object_id;
        self.next_output_object_id = self.next_output_object_id.saturating_add(1);
        let final_stage = matches!(
            outbound.kind,
            crate::node_actor::StageEdgeKindWire::TokenOut
        );
        let step_started = Instant::now();
        let mut pump = || pump_network(driver, stack);
        let committed_bytes = worker.execute_step(
            u64::from(config.stage_index) + 1,
            step_id,
            object_id,
            sequence,
            loaded.handle_id,
            output_ring_id,
            output_object_id,
            sequence,
            final_stage,
            outbound.object_spec,
            config,
            datastream,
            &mut pump,
        )?;
        let helper_execute_ms = duration_ms_u64(step_started.elapsed());
        let egress_read_started = Instant::now();
        let record = {
            let arena = arena_manager.lock();
            let lease = arena
                .lookup_lease(arena::RingId(output_ring_id))
                .ok_or_else(|| format!("outbound ring {output_ring_id} lease missing"))?;
            arena
                .read_arena(lease.layout.data_offset, committed_bytes)
                .map_err(|e| format!("read egress ring: {e}"))?
        };
        let egress_read_ms = duration_ms_u64(egress_read_started.elapsed());
        let record_bytes = record.len();
        emit_node_event(
            datastream,
            config,
            NODE_STAGE_CHANNEL,
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
                "helper_execute_ms":helper_execute_ms,
                "egress_ring_read_ms":egress_read_ms,
            }),
        );
        self.driver_model
            .observe(driver_model::DriverEvent::EgressBytesCommitted {
                edge_id: driver_model::EdgeId(outbound.edge_id),
                bytes: record.clone(),
            });
        self.driver_model
            .observe(driver_model::DriverEvent::RingReadable {
                ring_id: driver_model::RingId(output_ring_id),
            });
        let sender = self
            .outbound_sender
            .as_ref()
            .ok_or_else(|| "outbound edge sender missing".to_owned())?;
        let edge_send_started = Instant::now();
        sender.send(record)?;
        let edge_send_ms = duration_ms_u64(edge_send_started.elapsed());
        emit_node_event(
            datastream,
            config,
            NODE_STAGE_CHANNEL,
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
                "send_ms":edge_send_ms,
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
        datastream: &mut NodeDatastream,
        driver: &mut IrohDriver,
        stack: &DistributionRuntimeStack,
    ) -> Result<(), String> {
        let mut pump = || pump_network(driver, stack);
        worker.release_device_object(handle_id, config, datastream, &mut pump)
    }

    #[allow(clippy::too_many_arguments)]
    fn ingest_stream_bytes(
        &mut self,
        edge_id: u64,
        stream_id: u64,
        bytes: Vec<u8>,
        stack: &DistributionRuntimeStack,
        node_actor: ActorAddress,
        worker: &mut TinygradWorker,
        arena_manager: &Arc<Mutex<arena::ArenaManager>>,
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
        driver: &mut IrohDriver,
    ) -> Result<(), String> {
        let Some(inbound) = self.inbound_edge.clone() else {
            return Ok(());
        };
        if inbound.edge_id != edge_id {
            return Ok(());
        }
        let (records, buffered_bytes) = {
            let buffer = self.ingress_streams.entry(stream_id).or_default();
            buffer.extend_from_slice(&bytes);
            let buffered_bytes = buffer.len();
            let mut records = Vec::new();
            while let Some(record) = take_complete_ingress_record(buffer, inbound.object_spec)? {
                records.push(record);
            }
            (records, buffered_bytes)
        };
        for record in records {
            let ring_id = self
                .inbound_ring_id
                .ok_or_else(|| "inbound ring missing".to_owned())?;
            let ring_write_started = Instant::now();
            {
                let arena = arena_manager.lock();
                let lease = arena
                    .lookup_lease(arena::RingId(ring_id))
                    .ok_or_else(|| format!("inbound ring {ring_id} lease missing"))?;
                arena
                    .write_arena(lease.layout.data_offset, &record.bytes)
                    .map_err(|e| format!("write ingress ring: {e}"))?;
            }
            let ingress_ring_write_ms = duration_ms_u64(ring_write_started.elapsed());
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "ingress_ring_write",
                "ready",
                json!({
                    "edge_id":edge_id,
                    "edge_kind":format!("{:?}", inbound.kind),
                    "ring_id":ring_id,
                    "stream_id":stream_id,
                    "object_id":record.object_id,
                    "sequence":record.sequence,
                    "extent":record.extent,
                    "begin_sequence":record.begin_sequence,
                    "end_of_sequence":record.end_of_sequence,
                    "record_bytes":record.bytes.len(),
                    "ingress_buffer_bytes":buffered_bytes,
                    "ingress_ring_write_ms":ingress_ring_write_ms,
                }),
            );
            let object_load_started = Instant::now();
            let loaded = worker.ring_readable(
                ring_id,
                edge_id,
                inbound.object_spec,
                config,
                datastream,
                &mut || {},
            )?;
            let object_load_ms = duration_ms_u64(object_load_started.elapsed());
            let key = ObjectKey {
                edge_id,
                object_id: loaded.object_id,
            };
            self.object_handles.insert(key, loaded.clone());
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "object_loaded",
                "ready",
                json!({
                    "edge_id":edge_id,
                    "edge_kind":format!("{:?}", inbound.kind),
                    "ring_id":ring_id,
                    "stream_id":stream_id,
                    "object_id":loaded.object_id,
                    "sequence":loaded.sequence,
                    "handle_generation":loaded.handle_generation,
                    "handle_id":loaded.handle_id,
                    "object_load_ms":object_load_ms,
                }),
            );
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
        }
        self.drive_edge_workflow(
            stack,
            node_actor,
            worker,
            arena_manager,
            config,
            datastream,
            driver,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn drive_edge_workflow(
        &mut self,
        stack: &DistributionRuntimeStack,
        node_actor: ActorAddress,
        worker: &mut TinygradWorker,
        arena_manager: &Arc<Mutex<arena::ArenaManager>>,
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
        driver: &mut IrohDriver,
    ) -> Result<(), String> {
        loop {
            let progressed =
                self.drain_edge_commands(worker, arena_manager, config, datastream, driver)?
                    || self.drain_driver_events()
                    || self.drain_edge_events(stack, node_actor)?;
            if !progressed {
                break;
            }
        }
        Ok(())
    }

    fn drain_edge_commands(
        &mut self,
        worker: &mut TinygradWorker,
        arena_manager: &Arc<Mutex<arena::ArenaManager>>,
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
        driver: &mut IrohDriver,
    ) -> Result<bool, String> {
        let mut progressed = false;
        while self.edge_command_cursor < self.establisher.commands().len() {
            let command = self.establisher.commands()[self.edge_command_cursor].clone();
            self.edge_command_cursor += 1;
            progressed = true;
            match command {
                edge::EdgeCommand::LeaseRing {
                    request_id,
                    ring_spec,
                    ..
                } => {
                    let events = arena_manager.lock().request(arena::ArenaRequest::LeaseRing(
                        arena::LeaseRing {
                            request_id: arena::LeaseRequestId(request_id.0),
                            ring_spec: arena::RingSpec {
                                header_bytes: ring_spec.header_bytes,
                                data_bytes: ring_spec.data_bytes,
                                alignment: ring_spec.alignment,
                            },
                        },
                    ));
                    for event in events {
                        match event {
                            arena::ArenaEvent::RingLeased { lease } => {
                                self.establisher.observe(edge::EdgeEvent::RingLeased {
                                    request_id: edge::LeaseRequestId(lease.request_id.0),
                                    ring_id: edge::RingId(lease.ring_id.0),
                                    layout: edge::RingLayout {
                                        start_offset: lease.layout.start_offset,
                                        header_offset: lease.layout.header_offset,
                                        data_offset: lease.layout.data_offset,
                                        end_offset: lease.layout.end_offset,
                                        data_bytes: lease.layout.data_bytes,
                                        alignment: lease.layout.alignment,
                                    },
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
                                self.establisher
                                    .observe(edge::EdgeEvent::RingLeaseRejected {
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
                }
                edge::EdgeCommand::InstallWorkerRing {
                    edge_id,
                    ring_id,
                    direction,
                    object_spec,
                    ..
                } => {
                    let lease = arena_manager
                        .lock()
                        .lookup_lease(arena::RingId(ring_id.0))
                        .ok_or_else(|| format!("ring {} lease missing", ring_id.0))?
                        .clone();
                    let (port, direction_name, wire_spec) = match direction {
                        edge::RingDirection::Ingress => {
                            self.inbound_ring_id = Some(ring_id.0);
                            let spec = self
                                .inbound_edge
                                .as_ref()
                                .map(|edge| edge.object_spec)
                                .unwrap_or(StageObjectSpecWire {
                                    max_extent: object_spec.max_extent_bytes,
                                    alignment: 4,
                                });
                            ("input", "ingress", spec)
                        }
                        edge::RingDirection::Egress => {
                            self.outbound_ring_id = Some(ring_id.0);
                            let spec = self
                                .outbound_edge
                                .as_ref()
                                .map(|edge| edge.object_spec)
                                .unwrap_or(StageObjectSpecWire {
                                    max_extent: object_spec.max_extent_bytes,
                                    alignment: 4,
                                });
                            ("output", "egress", spec)
                        }
                    };
                    worker.install_ring(
                        ring_id.0,
                        edge_id.0,
                        port,
                        direction_name,
                        lease.layout,
                        wire_spec,
                        config,
                        datastream,
                        &mut || {},
                    )?;
                    self.establisher
                        .observe(edge::EdgeEvent::RingInstalled { edge_id, ring_id });
                }
                edge::EdgeCommand::EstablishSend {
                    edge_id,
                    consumer_node_id,
                    ..
                } => {
                    let outbound = self
                        .outbound_edge
                        .as_ref()
                        .ok_or_else(|| "outbound edge missing".to_owned())?;
                    let peer = outbound
                        .consumer_endpoint
                        .clone()
                        .ok_or_else(|| "outbound consumer endpoint missing".to_owned())?;
                    let record = self
                        .establisher
                        .local_record(edge_id)
                        .ok_or_else(|| format!("edge {} record missing", edge_id.0))?;
                    let ring_id = record
                        .ring_id
                        .ok_or_else(|| format!("edge {} ring missing", edge_id.0))?;
                    let ring_capacity = outbound.ring_spec.data_capacity as usize;
                    self.driver_model
                        .observe(driver_model::DriverEvent::EstablishSend(
                            driver_model::EstablishSend {
                                edge_id: driver_model::EdgeId(edge_id.0),
                                peer_node_id: driver_model::NodeId(consumer_node_id.0),
                                layout: driver_model::RingLayout {
                                    ring_id: driver_model::RingId(ring_id.0),
                                    byte_capacity: ring_capacity,
                                    direction: driver_model::RingDirection::Egress,
                                },
                            },
                        ));
                    self.outbound_sender = Some(driver.spawn_edge_send_pump(peer, edge_id.0)?);
                }
                edge::EdgeCommand::EstablishRecv { edge_id, .. } => {
                    let record = self
                        .establisher
                        .local_record(edge_id)
                        .ok_or_else(|| format!("edge {} record missing", edge_id.0))?;
                    let ring_id = record
                        .ring_id
                        .ok_or_else(|| format!("edge {} ring missing", edge_id.0))?;
                    let ring_capacity = self
                        .inbound_edge
                        .as_ref()
                        .map(|edge| edge.ring_spec.data_capacity as usize)
                        .unwrap_or(4096);
                    self.driver_model
                        .observe(driver_model::DriverEvent::EstablishRecv(
                            driver_model::EstablishRecv {
                                edge_id: driver_model::EdgeId(edge_id.0),
                                layout: driver_model::RingLayout {
                                    ring_id: driver_model::RingId(ring_id.0),
                                    byte_capacity: ring_capacity,
                                    direction: driver_model::RingDirection::Ingress,
                                },
                            },
                        ));
                }
                edge::EdgeCommand::CancelQueuedLease { request_id, .. } => {
                    let _ = arena_manager
                        .lock()
                        .request(arena::ArenaRequest::CancelLease {
                            request_id: arena::LeaseRequestId(request_id.0),
                        });
                }
                edge::EdgeCommand::StopPump { edge_id, .. } => {
                    self.driver_model
                        .observe(driver_model::DriverEvent::StopEdge {
                            edge_id: driver_model::EdgeId(edge_id.0),
                        });
                }
                edge::EdgeCommand::UninstallWorkerRing { ring_id, .. } => {
                    let mut pump = || {};
                    worker.uninstall_ring(ring_id.0, config, datastream, &mut pump)?;
                    self.establisher
                        .observe(edge::EdgeEvent::RingQuiesced { ring_id });
                }
                edge::EdgeCommand::ReleaseArenaLease { ring_id, proof } => {
                    let proof = if proof == edge::QuiescenceProof::verified() {
                        arena::QuiescenceProof::verified()
                    } else {
                        arena::QuiescenceProof::missing()
                    };
                    let _ = arena_manager
                        .lock()
                        .request(arena::ArenaRequest::ReleaseRing {
                            ring_id: arena::RingId(ring_id.0),
                            proof,
                        });
                }
            }
        }
        Ok(progressed)
    }

    fn drain_driver_events(&mut self) -> bool {
        let mut progressed = false;
        while self.driver_event_cursor < self.driver_model.events().len() {
            let event = self.driver_model.events()[self.driver_event_cursor].clone();
            self.driver_event_cursor += 1;
            progressed = true;
            match event {
                driver_model::DriverEventOut::DriverEdgeReady { edge_id } => {
                    self.establisher.observe(edge::EdgeEvent::DriverEdgeReady {
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
                    self.establisher.observe(edge::EdgeEvent::StreamFault {
                        edge_id: edge::EdgeId(edge_id.0),
                        reason,
                    });
                }
                driver_model::DriverEventOut::PumpStopped { edge_id, ring_id } => {
                    self.establisher.observe(edge::EdgeEvent::PumpStopped {
                        edge_id: edge::EdgeId(edge_id.0),
                        ring_id: edge::RingId(ring_id.0),
                    });
                }
                driver_model::DriverEventOut::StreamClosed { .. } => {}
            }
        }
        progressed
    }

    fn drain_edge_events(
        &mut self,
        stack: &DistributionRuntimeStack,
        node_actor: ActorAddress,
    ) -> Result<bool, String> {
        let mut progressed = false;
        while self.edge_event_cursor < self.establisher.events().len() {
            let event = self.establisher.events()[self.edge_event_cursor].clone();
            self.edge_event_cursor += 1;
            progressed = true;
            match event {
                edge::EdgeLifecycleEvent::EdgeReady { edge_id, .. } => {
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
                edge::EdgeLifecycleEvent::EdgeFaulted { edge_id, reason } => {
                    stack
                        .runtime
                        .send_to(
                            node_actor,
                            NodeAgentMsg::WorkerCrashed {
                                reason: Some(format!("edge {} faulted: {reason:?}", edge_id.0)),
                            },
                        )
                        .map_err(|e| format!("mark worker crashed after edge fault: {e}"))?;
                    return Err(format!("edge {} faulted: {reason:?}", edge_id.0));
                }
                edge::EdgeLifecycleEvent::EdgeStopped { .. } => {}
            }
        }
        Ok(progressed)
    }
}

fn duration_ms_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

struct IngressRecordBytes {
    bytes: Vec<u8>,
    object_id: u64,
    sequence: u64,
    extent: u64,
    begin_sequence: bool,
    end_of_sequence: bool,
}

fn take_complete_ingress_record(
    buffer: &mut Vec<u8>,
    spec: StageObjectSpecWire,
) -> Result<Option<IngressRecordBytes>, String> {
    let record = match ingress::read_object_record(
        buffer,
        ingress::ObjectSpec {
            max_extent: spec.max_extent,
            alignment: u64::from(spec.alignment),
            layout: ingress::ObjectLayout::Token,
        },
        false,
    )
    .map_err(|reason| format!("invalid object record: {reason:?}"))?
    {
        ingress::ObjectRecordRead::Incomplete => return Ok(None),
        ingress::ObjectRecordRead::Complete(record) => record,
    };
    let bytes = buffer.drain(..record.total_len).collect();
    Ok(Some(IngressRecordBytes {
        bytes,
        object_id: record.object_id.0,
        sequence: record.sequence,
        extent: record.extent,
        begin_sequence: record.flags.begin_sequence,
        end_of_sequence: record.flags.end_of_sequence,
    }))
}

fn value_u64(value: &Value, field: &str) -> Result<u64, String> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("helper event missing numeric {field}: {value}"))
}

pub(crate) fn run_from_env() -> ExitCode {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("debug-join") => debug_join_client_main(args.collect()),
        Some("stage-shard-fetcher") => match run_stage_shard_fetcher() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                let event = json!({"type":"StageShardFetchFailed","error":error});
                println!("{event}");
                ExitCode::from(1)
            }
        },
        _ => match run() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("mvp-worker-node: {error}");
                ExitCode::from(1)
            }
        },
    }
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
            additional_alpns: vec![EDGE_ALPN.to_vec(), DATASTREAM_ALPN.to_vec()],
        },
    ) {
        Ok(driver) => driver,
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
    let advertised_self_endpoint =
        advertised_endpoint(driver.endpoint_addr(), config.endpoint_addr_mask)?;
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "iroh_driver",
        "ready",
        json!({"endpoint":advertised_self_endpoint.clone(),"has_relay":advertised_self_endpoint.relay_urls().next().is_some(),"direct_addr_count":advertised_self_endpoint.ip_addrs().count(),"relay_mode":format!("{:?}", config.relay_mode),"endpoint_addr_mask":config.endpoint_addr_mask.as_str()}),
    )?;
    if let Some(coordinator) = &config.coordinator_endpoint {
        driver.join(std::slice::from_ref(coordinator));
        emit_stdio_node_event(
            &config,
            NODE_BOOTSTRAP_CHANNEL,
            "coordinator_join",
            "started",
            json!({"endpoint":coordinator,"has_relay":coordinator.relay_urls().next().is_some(),"direct_addr_count":coordinator.ip_addrs().count()}),
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
    let arena_fd = arena_manager.lock().arena_fd();

    let mut datastream = NodeDatastream::new(&config);
    let datastream_transport = driver.datastream_publish_handle();
    let datastream_publisher = match stack
        .runtime
        .spawn(datastream.publisher_actor(datastream_transport))
    {
        Ok(actor) => actor,
        Err(error) => {
            emit_node_event(
                &mut datastream,
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "datastream_publisher",
                "failed",
                json!({"error":error.to_string()}),
            );
            return Err(format!("spawn datastream publisher: {error}"));
        }
    };
    stack.register_local_actor(driver.register_actor(datastream_publisher, 1));
    let sampler_health_channel = datastream.channel_by_name(NODE_SAMPLER_CHANNEL);
    let sampler_health_context = SamplerHealthContext::from_config(&config);
    spawn_host_gpu_sampler(
        tokio.handle().clone(),
        datastream.producer.clone(),
        datastream.channels.host_gpu,
        sampler_health_channel,
        sampler_health_context,
    );
    spawn_host_net_sampler(
        tokio.handle().clone(),
        datastream.producer.clone(),
        datastream.channels.host_net,
        sampler_health_channel,
        sampler_health_context,
    );
    spawn_arena_sampler(
        tokio.handle().clone(),
        datastream.producer.clone(),
        datastream.channels.arena,
        Arc::clone(&arena_manager),
    );
    emit_node_event(
        &mut datastream,
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "datastream_publisher",
        "ready",
        json!({"actor":datastream_publisher,"name":DATASTREAM_PUBLISHER_NAME,"subscription_transport":"iroh"}),
    );
    let worker_synthetic_id = format!(
        "mvp-worker-node-{}-{}-datastream-preflight",
        config.logical_node_id, config.stage_index
    );
    for (phase, status) in [
        ("DatastreamProducerConfigured", "configured"),
        ("DatastreamProducerConnected", "ready"),
        ("DatastreamSyntheticEventSent", "sent"),
        ("DatastreamSyntheticEventObserved", "observed"),
    ] {
        emit_node_event(
            &mut datastream,
            &config,
            NODE_BOOTSTRAP_CHANNEL,
            phase,
            status,
            json!({
                "producer":"mvp-worker-node",
                "producer_class":"rust-worker-node",
                "synthetic_id":worker_synthetic_id,
                "datastream_endpoint":{
                    "role":"worker-node-iroh-publisher",
                    "transport":"iroh-datastream",
                    "endpoint_addr_mask":config.endpoint_addr_mask.as_str(),
                    "relay_mode":format!("{:?}", config.relay_mode),
                },
            }),
        );
    }
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
    let mut worker = match TinygradWorker::spawn(&config, arena_fd) {
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
        datastream.producer.clone(),
        datastream.channels.host_cpu,
        sampler_health_channel,
        sampler_health_context,
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
    let mut edge_runtime = WorkerEdgeRuntime::new(config.logical_node_id);
    let mut pending_runtime_ready = PendingRuntimeReady::new(
        &config,
        advertised_self_endpoint.clone(),
        node_actor,
        datastream_publisher,
    );

    let ready = json!({
        "type":"ready",
        "role":"node",
        "endpoint": advertised_self_endpoint.clone(),
        "node_actor": node_actor,
        "datastream_publisher": datastream_publisher,
        "logical_node_id": config.logical_node_id,
        "stage_index": config.stage_index,
    });
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
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
            "checks":["network","edge_streams","datastream","node_reports","stdin_shutdown","worker_health"],
        }),
    );
    loop {
        pump_network(&mut driver, &stack);
        emit_swim_telemetry(&mut datastream, &stack, "main_loop");
        drain_debug_join_commands(&mut debug_join_rx, &mut driver, &config, &mut datastream);
        datastream.tick();
        worker.drain_stderr(&config, &mut datastream);
        edge_runtime.poll_iroh(
            &mut driver,
            &stack,
            node_actor,
            &mut worker,
            &arena_manager,
            &config,
            &mut datastream,
        )?;
        while let Some(report) = reports.try_recv() {
            match handle_node_report(
                report,
                &config,
                &stack,
                &mut driver,
                node_actor,
                &mut worker,
                &mut edge_runtime,
                &arena_manager,
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
                        datastream.submit_text(datastream.channels.node_ready, ready.to_string());
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
            let _ = stack.runtime.send_to(
                node_actor,
                NodeAgentMsg::WorkerCrashed {
                    reason: Some(format!("tinygrad helper exited with {status}")),
                },
            );
            pump_network(&mut driver, &stack);
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

fn emit_swim_telemetry(
    datastream: &mut NodeDatastream,
    stack: &DistributionRuntimeStack,
    local_phase: &str,
) {
    for transition in stack.drain_swim_transitions() {
        let peer = format!("{:?}", transition.peer);
        let from = transition.from.map(|state| format!("{:?}", state));
        let to = format!("{:?}", transition.to);
        let member_state = stack
            .member_state(transition.peer)
            .map(|state| format!("{:?}", state));
        let record = MembershipTransition {
            peer,
            from: from.unwrap_or_default(),
            to,
            reason: transition.reason.to_owned(),
            last_ack_age_ms: transition.last_ack_age.map(duration_ms_u64),
            consecutive_timeouts: transition.consecutive_timeouts,
            recent_probe_targets: swim_recent_probe_targets(stack),
            member_state,
        };
        datastream
            .producer
            .submit_record(datastream.channels.membership, &record);
    }
    for event in stack.drain_swim_probe_events() {
        let record = swim_probe_event_record(stack, event, local_phase);
        datastream
            .producer
            .submit_record(datastream.channels.swim_probes, &record);
    }
}

fn swim_probe_event_record(
    stack: &DistributionRuntimeStack,
    event: ObservedProbeEvent,
    local_phase: &str,
) -> SwimProbeEvent {
    let config = &stack.swim_config;
    let budget_ms = event.budget_ms;
    SwimProbeEvent {
        event: event.event.to_owned(),
        target: format!("{:?}", event.target),
        sequence: event.sequence,
        kind: event.kind.to_owned(),
        rtt_ms: event.rtt_ms,
        budget_ms,
        budget_ticks: budget_ms,
        last_ack_age_ms: event.last_ack_age.map(duration_ms_u64),
        consecutive_timeouts: event.consecutive_timeouts,
        recent_probe_targets: swim_recent_probe_targets(stack),
        member_state: stack
            .member_state(event.target)
            .map(|state| format!("{:?}", state)),
        local_phase: local_phase.to_owned(),
        probe_interval_ms: duration_ms_u64(config.probe_interval),
        probe_timeout_ms: duration_ms_u64(config.probe_timeout),
        indirect_probes: u32::try_from(config.indirect_probes).unwrap_or(u32::MAX),
        suspicion_timeout_ms: duration_ms_u64(config.suspicion_timeout),
        dead_reprobe_interval_ms: duration_ms_u64(config.dead_reprobe_interval),
        probe_mode: format!("{:?}", config.probe_mode),
        lifeguard_enabled: config.lifeguard.is_some(),
    }
}

fn swim_recent_probe_targets(stack: &DistributionRuntimeStack) -> Vec<String> {
    stack
        .swim_telemetry
        .recent_targets()
        .into_iter()
        .map(|node_id| format!("{:?}", node_id))
        .collect()
}

#[derive(Clone, Copy)]
struct DatastreamChannelSet {
    node_ready: ChannelId,
    node_lifecycle: ChannelId,
    node_self_test: ChannelId,
    worker_stderr: ChannelId,
    host_cpu: ChannelId,
    host_gpu: ChannelId,
    membership: ChannelId,
    swim_probes: ChannelId,
    host_net: ChannelId,
    arena: ChannelId,
}

struct NodeDatastream {
    endpoint: Arc<DatastreamEndpoint>,
    producer: DatastreamProducer,
    channels: DatastreamChannelSet,
    by_name: BTreeMap<String, ChannelId>,
    by_id: BTreeMap<ChannelId, String>,
    archive: Option<DatastreamArchive>,
}

impl NodeDatastream {
    fn new(config: &DeploymentConfig) -> Self {
        let stream = StreamId::new(
            NodeId::new(config.logical_node_id.to_string()),
            Lifetime(config.run_id),
        );
        let endpoint = Arc::new(DatastreamEndpoint::with_descriptor(
            StreamDescriptor {
                stream: stream.clone(),
                label: Some("mvp worker node".to_owned()),
                origin: StreamOrigin::RemoteNode,
            },
            256,
            1024,
        ));
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
            "mvp.worker.initialize",
            "mvp.worker.role",
            "mvp.worker.weights",
            "mvp.worker.prompt",
            "mvp.worker.tokenizer",
            "mvp.worker.ring",
            "mvp.worker.ingress",
            "mvp.worker.step",
            "mvp.worker.device_object",
            "mvp.worker.shutdown",
        ] {
            register_json_channel(&producer, &mut by_name, &mut by_id, name);
        }

        let channels = DatastreamChannelSet {
            node_ready: register_json_channel(
                &producer,
                &mut by_name,
                &mut by_id,
                "mvp.node.ready",
            ),
            node_lifecycle: register_json_channel(
                &producer,
                &mut by_name,
                &mut by_id,
                "mvp.node.lifecycle",
            ),
            node_self_test: register_json_channel(
                &producer,
                &mut by_name,
                &mut by_id,
                "mvp.node.self_test",
            ),
            worker_stderr: register_json_channel(
                &producer,
                &mut by_name,
                &mut by_id,
                "mvp.worker.stderr",
            ),
            host_cpu: register_record_channel::<datastream::hardware::cpu::HostCpuSample>(
                &producer,
                &mut by_name,
                &mut by_id,
            ),
            host_gpu: register_record_channel::<datastream::hardware::gpu::HostGpuSample>(
                &producer,
                &mut by_name,
                &mut by_id,
            ),
            host_net: register_record_channel::<datastream::hardware::net::HostNetSample>(
                &producer,
                &mut by_name,
                &mut by_id,
            ),
            arena: register_record_channel::<arena::ArenaSample>(
                &producer,
                &mut by_name,
                &mut by_id,
            ),
            membership: register_record_channel::<MembershipTransition>(
                &producer,
                &mut by_name,
                &mut by_id,
            ),
            swim_probes: register_record_channel::<SwimProbeEvent>(
                &producer,
                &mut by_name,
                &mut by_id,
            ),
        };
        let archive = config.datastream_frame_log.as_deref().and_then(|path| {
            DatastreamArchive::open(path, endpoint.subscribe_all("frame-log")).ok()
        });

        Self {
            endpoint,
            producer,
            channels,
            by_name,
            by_id,
            archive,
        }
    }

    fn channel_by_name(&mut self, name: &str) -> ChannelId {
        if let Some(id) = self.by_name.get(name).copied() {
            return id;
        }
        register_json_channel(&self.producer, &mut self.by_name, &mut self.by_id, name)
    }

    fn submit_text(&self, channel: ChannelId, text: impl AsRef<[u8]>) {
        self.producer.submit_text(channel, text);
    }

    fn tick(&mut self) {
        self.endpoint.tick();
        if let Some(archive) = &mut self.archive {
            archive.drain(&self.by_id);
        }
    }

    fn publisher_actor(&self, transport: DatastreamPublishHandle) -> DatastreamPublisherActor {
        DatastreamPublisherActor::new(
            Arc::clone(&self.endpoint),
            move |subscribe: DatastreamSubscribe, subscription: DatastreamSubscription| {
                let Ok(header) = DatastreamQuicHeader::from_snapshot(
                    subscribe.flow_id,
                    subscribe.token,
                    subscription.snapshot(),
                ) else {
                    return;
                };
                transport.publish_subscription(
                    subscribe.collector,
                    header,
                    subscription,
                    Duration::from_millis(10),
                );
            },
        )
    }
}

fn register_json_channel(
    producer: &DatastreamProducer,
    by_name: &mut BTreeMap<String, ChannelId>,
    by_id: &mut BTreeMap<ChannelId, String>,
    name: &str,
) -> ChannelId {
    let id = producer.register_channel(
        name,
        ChannelContent::JsonRecord {
            schema: Some(name.to_owned()),
        },
    );
    by_name.insert(name.to_owned(), id);
    by_id.insert(id, name.to_owned());
    id
}

fn register_record_channel<R: Record>(
    producer: &DatastreamProducer,
    by_name: &mut BTreeMap<String, ChannelId>,
    by_id: &mut BTreeMap<ChannelId, String>,
) -> ChannelId {
    let id = producer.register_record::<R>();
    by_name.insert(R::CHANNEL.to_owned(), id);
    by_id.insert(id, R::CHANNEL.to_owned());
    id
}

struct DatastreamArchive {
    file: File,
    subscription: DatastreamSubscription,
}

impl DatastreamArchive {
    fn open(path: &str, subscription: DatastreamSubscription) -> std::io::Result<Self> {
        Ok(Self {
            file: OpenOptions::new().create(true).append(true).open(path)?,
            subscription,
        })
    }

    fn drain(&mut self, channel_names: &BTreeMap<ChannelId, String>) {
        for event in self.subscription.drain_available() {
            if let DatastreamEvent::Frame(frame) = event {
                let channel = channel_names
                    .get(&frame.channel.channel)
                    .cloned()
                    .unwrap_or_else(|| format!("channel#{}", frame.channel.channel.0));
                let record = json!({
                    "stream":frame.channel.stream.to_string(),
                    "channel":channel,
                    "channel_id":frame.channel.channel.0,
                    "position":frame.position.0,
                    "payload":String::from_utf8_lossy(&frame.payload),
                });
                let _ = serde_json::to_writer(&mut self.file, &record);
                let _ = writeln!(self.file);
            }
        }
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
    datastream_publisher: ActorAddress,
    coordinator: Option<DistNodeId>,
    readiness_id: u64,
    attempts: u32,
    next_attempt_at: Instant,
    backoff: Duration,
    acked: bool,
    swim_logged: bool,
}

impl PendingRuntimeReady {
    fn new(
        config: &DeploymentConfig,
        endpoint: EndpointAddr,
        node_actor: ActorAddress,
        datastream_publisher: ActorAddress,
    ) -> Self {
        Self {
            run_id: config.run_id,
            node_id: config.logical_node_id,
            stage_index: config.stage_index,
            endpoint,
            node_actor,
            datastream_publisher,
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
                    datastream_publisher: self.datastream_publisher,
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
    edge_runtime: &mut WorkerEdgeRuntime,
    arena_manager: &Arc<Mutex<arena::ArenaManager>>,
    datastream: &mut NodeDatastream,
) -> Result<NodeReportOutcome, String> {
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
                command,
                config,
                stack,
                driver,
                node_actor,
                worker,
                edge_runtime,
                arena_manager,
                datastream,
            )?;
            Ok(NodeReportOutcome::None)
        }
        NodeAgentReport::Lifecycle(event) => {
            let event = format!("{event:?}");
            datastream.submit_text(
                datastream.channels.node_lifecycle,
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
        NodeAgentReport::EncodePromptRequested {
            request_id,
            prompt,
            reply_to,
        } => {
            handle_encode_prompt_request(
                request_id, prompt, reply_to, config, stack, driver, worker, datastream,
            )?;
            Ok(NodeReportOutcome::None)
        }
        NodeAgentReport::DecodeTokensRequested {
            request_id,
            tokens,
            reply_to,
        } => {
            handle_decode_tokens_request(
                request_id, tokens, reply_to, config, stack, driver, worker, datastream,
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
    datastream: &mut NodeDatastream,
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

fn handle_encode_prompt_request(
    request_id: u64,
    prompt: String,
    reply_to: ActorAddress,
    config: &DeploymentConfig,
    stack: &DistributionRuntimeStack,
    driver: &mut IrohDriver,
    worker: &mut TinygradWorker,
    datastream: &mut NodeDatastream,
) -> Result<(), String> {
    emit_node_event(
        datastream,
        config,
        NODE_PROMPT_CHANNEL,
        "encode_prompt",
        "started",
        json!({"request_id":request_id,"prompt_bytes":prompt.len(),"reply_to":reply_to}),
    );
    let mut pump = || pump_network(driver, stack);
    let event = match worker.encode_prompt(request_id, &prompt, config, datastream, &mut pump) {
        Ok(tokens) => {
            emit_node_event(
                datastream,
                config,
                NODE_PROMPT_CHANNEL,
                "encode_prompt",
                "ready",
                json!({"request_id":request_id,"tokens":tokens.len(),"reply_to":reply_to}),
            );
            TokenizerEvent::PromptEncoded { request_id, tokens }
        }
        Err(error) => {
            emit_node_event(
                datastream,
                config,
                NODE_PROMPT_CHANNEL,
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
    request_id: u64,
    tokens: Vec<u32>,
    reply_to: ActorAddress,
    config: &DeploymentConfig,
    stack: &DistributionRuntimeStack,
    driver: &mut IrohDriver,
    worker: &mut TinygradWorker,
    datastream: &mut NodeDatastream,
) -> Result<(), String> {
    emit_node_event(
        datastream,
        config,
        NODE_PROMPT_CHANNEL,
        "decode_tokens",
        "started",
        json!({"request_id":request_id,"tokens":tokens.len(),"reply_to":reply_to}),
    );
    let mut pump = || pump_network(driver, stack);
    let event = match worker.decode_tokens(request_id, &tokens, config, datastream, &mut pump) {
        Ok(text) => {
            emit_node_event(
                datastream,
                config,
                NODE_PROMPT_CHANNEL,
                "decode_tokens",
                "ready",
                json!({"request_id":request_id,"tokens":tokens.len(),"text_bytes":text.len(),"reply_to":reply_to}),
            );
            TokenizerEvent::TokensDecoded { request_id, text }
        }
        Err(error) => {
            emit_node_event(
                datastream,
                config,
                NODE_PROMPT_CHANNEL,
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
    PollChild,
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
}

#[derive(Clone, Debug)]
enum StageShardFetchReport {
    Progress(Value),
    Done(PathBuf),
    Failed(String),
}

struct StageShardFetchActor {
    request_json: Vec<u8>,
    output_path: PathBuf,
    report_to: ActorAddress,
    sender: ExternalSender,
    child: Option<Child>,
    stdout_reader: Option<thread::JoinHandle<()>>,
    stderr_reader: Option<thread::JoinHandle<()>>,
    stdout_closed: bool,
    stderr_closed: bool,
    ready_path: Option<PathBuf>,
    exit_status: Option<std::process::ExitStatus>,
    finished: bool,
}

impl StageShardFetchActor {
    fn new(
        request_json: Vec<u8>,
        output_path: PathBuf,
        report_to: ActorAddress,
        sender: ExternalSender,
    ) -> Self {
        Self {
            request_json,
            output_path,
            report_to,
            sender,
            child: None,
            stdout_reader: None,
            stderr_reader: None,
            stdout_closed: true,
            stderr_closed: true,
            ready_path: None,
            exit_status: None,
            finished: false,
        }
    }

    fn start_fetch(&mut self, ctx: &Ctx) {
        if self.finished {
            return;
        }
        let exe = match std::env::current_exe() {
            Ok(exe) => exe,
            Err(error) => {
                self.fail(ctx, format!("locate worker node executable: {error}"));
                return;
            }
        };
        let mut child = match Command::new(exe)
            .arg("stage-shard-fetcher")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                self.fail(ctx, format!("spawn stage shard fetcher: {error}"));
                return;
            }
        };
        match child.stdin.take() {
            Some(mut stdin) => {
                if let Err(error) = stdin.write_all(&self.request_json) {
                    let mut child = Some(child);
                    stop_stage_shard_child(&mut child);
                    self.fail(ctx, format!("write stage shard fetch request: {error}"));
                    return;
                }
            }
            None => {
                let mut child = Some(child);
                stop_stage_shard_child(&mut child);
                self.fail(ctx, "stage shard fetcher stdin missing".to_owned());
                return;
            }
        }

        self.stdout_closed = false;
        self.stderr_closed = false;
        self.ready_path = None;
        self.exit_status = None;
        if let Some(stdout) = child.stdout.take() {
            self.stdout_reader = Some(spawn_stage_shard_reader(
                StageShardProcessStream::Stdout,
                stdout,
                self.sender.clone(),
                ctx.self_addr(),
            ));
        } else {
            self.stdout_closed = true;
        }
        if let Some(stderr) = child.stderr.take() {
            self.stderr_reader = Some(spawn_stage_shard_reader(
                StageShardProcessStream::Stderr,
                stderr,
                self.sender.clone(),
                ctx.self_addr(),
            ));
        } else {
            self.stderr_closed = true;
        }
        self.child = Some(child);
        schedule_stage_shard_message(
            self.sender.clone(),
            ctx.self_addr(),
            StageShardFetchMsg::PollChild,
            PUMP_INTERVAL,
        );
    }

    fn poll_child(&mut self, ctx: &Ctx) {
        if self.finished {
            return;
        }
        let Some(child) = self.child.as_mut() else {
            return;
        };
        match child.try_wait() {
            Ok(Some(status)) => {
                self.child = None;
                self.exit_status = Some(status);
                self.maybe_finish(ctx);
            }
            Ok(None) => schedule_stage_shard_message(
                self.sender.clone(),
                ctx.self_addr(),
                StageShardFetchMsg::PollChild,
                PUMP_INTERVAL,
            ),
            Err(error) => {
                self.child = None;
                self.fail(ctx, format!("poll stage shard fetcher: {error}"));
            }
        }
    }

    fn handle_line(&mut self, ctx: &Ctx, stream: StageShardProcessStream, line: String) {
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
        let _ = ctx.send(self.report_to, StageShardFetchReport::Progress(event));
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
        self.join_readers();
        let status = self.exit_status.take().expect("exit status checked");
        if status.success() {
            let path = self
                .ready_path
                .clone()
                .unwrap_or_else(|| self.output_path.clone());
            if path.is_file() {
                self.finished = true;
                let _ = ctx.send(self.report_to, StageShardFetchReport::Done(path));
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
        stop_stage_shard_child(&mut self.child);
        self.join_readers();
        let _ = ctx.send(self.report_to, StageShardFetchReport::Failed(error));
        ctx.stop_self();
    }

    fn join_readers(&mut self) {
        if let Some(reader) = self.stdout_reader.take() {
            let _ = reader.join();
        }
        if let Some(reader) = self.stderr_reader.take() {
            let _ = reader.join();
        }
        self.stdout_closed = true;
        self.stderr_closed = true;
    }
}

impl ActorInterface for StageShardFetchActor {
    type Incoming = StageShardFetchMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        match msg {
            StageShardFetchMsg::Start => self.start_fetch(ctx),
            StageShardFetchMsg::PollChild => self.poll_child(ctx),
            StageShardFetchMsg::ProcessLine { stream, line } => self.handle_line(ctx, stream, line),
            StageShardFetchMsg::ReaderError { stream, error } => {
                self.handle_reader_error(ctx, stream, error)
            }
            StageShardFetchMsg::ReaderClosed { stream } => self.handle_reader_closed(ctx, stream),
        }
    }

    fn on_stop(&mut self, _ctx: &Ctx) {
        stop_stage_shard_child(&mut self.child);
        self.join_readers();
    }
}

fn stop_stage_shard_child(child: &mut Option<Child>) {
    let Some(mut child) = child.take() else {
        return;
    };
    let _ = child.kill();
    let _ = child.wait();
}

fn stage_shard_cache_path(plan: &StageShardPlan) -> PathBuf {
    let root = std::env::var("MVP_MODEL_CACHE_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/var/cache/mvp-models"));
    root.join(plan.cache_file_name())
}

fn spawn_stage_shard_reader<R: Read + Send + 'static>(
    stream: StageShardProcessStream,
    reader: R,
    sender: ExternalSender,
    actor: ActorAddress,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    let _ = sender.send_to(
                        actor,
                        StageShardFetchMsg::ProcessLine {
                            stream,
                            line: line.trim_end_matches(['\r', '\n']).to_owned(),
                        },
                    );
                }
                Err(error) => {
                    let _ = sender.send_to(
                        actor,
                        StageShardFetchMsg::ReaderError {
                            stream,
                            error: error.to_string(),
                        },
                    );
                    break;
                }
            }
        }
        let _ = sender.send_to(actor, StageShardFetchMsg::ReaderClosed { stream });
    })
}

fn schedule_stage_shard_message(
    sender: ExternalSender,
    actor: ActorAddress,
    msg: StageShardFetchMsg,
    delay: Duration,
) {
    thread::spawn(move || {
        thread::sleep(delay);
        let _ = sender.send_to(actor, msg);
    });
}

fn publish_stage_shard_fetch_event(
    datastream: &mut NodeDatastream,
    config: &DeploymentConfig,
    event: &Value,
) -> Result<(), String> {
    let channel = datastream.channel_by_name("mvp.worker.weights");
    let payload = node_event_payload(config, "stage_shard_fetch", "event", event.clone());
    datastream.submit_text(channel, payload.to_string());
    emit_stdio_datastream_frame("mvp.worker.weights", &payload)
        .map_err(|e| format!("emit stage shard fetch datastream frame: {e}"))?;
    datastream.tick();
    Ok(())
}

fn materialize_stage_shard_with_process(
    plan: &StageShardPlan,
    config: &DeploymentConfig,
    datastream: &mut NodeDatastream,
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
) -> Result<PathBuf, String> {
    let output_path = stage_shard_cache_path(plan);
    if output_path.is_file() {
        match validate_stage_shard_cache(&output_path, plan) {
            Ok(()) => {
                let event = json!({
                    "type":"StageShardCacheReady",
                    "stage_index":plan.stage_index,
                    "path":output_path,
                    "cache_hit":true,
                });
                publish_stage_shard_fetch_event(datastream, config, &event)?;
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
                publish_stage_shard_fetch_event(datastream, config, &event)?;
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
    let reports = stack
        .runtime
        .new_inbox::<StageShardFetchReport>()
        .map_err(|e| format!("stage shard fetch report inbox: {e}"))?;
    let actor = stack
        .runtime
        .spawn(StageShardFetchActor::new(
            request_json,
            output_path,
            *reports.addr(),
            stack.runtime.create_sender(),
        ))
        .map_err(|e| format!("spawn stage shard fetch actor: {e}"))?;
    stack
        .runtime
        .send_to(actor, StageShardFetchMsg::Start)
        .map_err(|e| format!("start stage shard fetch actor: {e}"))?;

    loop {
        pump_network(driver, stack);
        while let Some(report) = reports.try_recv() {
            match report {
                StageShardFetchReport::Progress(event) => {
                    if let Err(error) = publish_stage_shard_fetch_event(datastream, config, &event)
                    {
                        let _ = stack.runtime.stop_actor(actor);
                        return Err(error);
                    }
                }
                StageShardFetchReport::Done(path) => return Ok(path),
                StageShardFetchReport::Failed(error) => return Err(error),
            }
        }
        datastream.tick();
        thread::sleep(PUMP_INTERVAL);
    }
}

fn handle_stage_command(
    command: StageCommandWire,
    config: &DeploymentConfig,
    stack: &DistributionRuntimeStack,
    driver: &mut IrohDriver,
    node_actor: ActorAddress,
    worker: &mut TinygradWorker,
    edge_runtime: &mut WorkerEdgeRuntime,
    arena_manager: &Arc<Mutex<arena::ArenaManager>>,
    datastream: &mut NodeDatastream,
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
                    let _ = stack.runtime.send_to(
                        node_actor,
                        NodeAgentMsg::WorkerCrashed {
                            reason: Some(error.clone()),
                        },
                    );
                    pump();
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
                        datastream,
                        driver,
                        stack,
                    ) {
                        Ok(local_path) => (
                            GgufSource::LocalPath(local_path.to_string_lossy().into_owned()),
                            true,
                        ),
                        Err(error) => {
                            emit_node_event(
                                datastream,
                                config,
                                NODE_STAGE_CHANNEL,
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
                            pump_network(driver, stack);
                            return Err(error);
                        }
                    }
                } else {
                    (gguf_source, false)
                };
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "load_weights",
                "started",
                json!({"model_id":&model_id,"gguf_source":gguf_source_kind,"tokenizer":tokenizer_kind,"stage_shard":using_stage_shard,"layer_range":{"start":layer_start,"end_exclusive":layer_end_exclusive}}),
            );
            let mut pump = || pump_network(driver, stack);
            match worker.load_weights(
                model_id.clone(),
                resolved_gguf_source,
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
                    let _ = stack.runtime.send_to(
                        node_actor,
                        NodeAgentMsg::WorkerCrashed {
                            reason: Some(error.clone()),
                        },
                    );
                    pump();
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
        StageCommandWire::EstablishInboundEdge { edge_id, edge } => {
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "inbound_edge",
                "started",
                json!({"edge_id":edge_id,"kind":format!("{:?}", edge.kind),"ring_data_capacity":edge.ring_spec.data_capacity}),
            );
            edge_runtime.establish_inbound(
                edge,
                stack,
                node_actor,
                worker,
                arena_manager,
                config,
                datastream,
                driver,
            )?;
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "inbound_edge",
                "ready",
                json!({"edge_id":edge_id}),
            );
            Ok(())
        }
        StageCommandWire::EstablishOutboundEdge { edge_id, edge } => {
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "outbound_edge",
                "started",
                json!({"edge_id":edge_id,"kind":format!("{:?}", edge.kind),"consumer_node_id":edge.consumer_node_id,"has_consumer_endpoint":edge.consumer_endpoint.is_some()}),
            );
            edge_runtime.establish_outbound(
                edge,
                stack,
                node_actor,
                worker,
                arena_manager,
                config,
                datastream,
                driver,
            )?;
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "outbound_edge",
                "ready",
                json!({"edge_id":edge_id}),
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
        StageCommandWire::ReleaseInputHandle { handle_id, .. } => {
            edge_runtime.release_input_handle(handle_id, worker, config, datastream, driver, stack)
        }
        StageCommandWire::ExecuteStep {
            step_id,
            input_edge_id,
            object_id,
            sequence,
            ..
        } => {
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "execute_step",
                "started",
                json!({"step_id":step_id,"input_edge_id":input_edge_id,"object_id":object_id,"sequence":sequence}),
            );
            edge_runtime.execute_step(
                step_id,
                input_edge_id,
                object_id,
                sequence,
                stack,
                node_actor,
                worker,
                arena_manager,
                config,
                datastream,
                driver,
            )?;
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
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
    datastream: &mut NodeDatastream,
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
    datastream.submit_text(datastream.channels.node_self_test, record.to_string());
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
    datastream_frame_log: Option<String>,
    debug_join_socket: Option<String>,
    relay_mode: iroh::RelayMode,
    endpoint_addr_mask: EndpointAddrMask,
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
        let run_id = env_parse!("MVP_RUN_ID", 1)?;
        let logical_node_id = env_parse!("MVP_LOGICAL_NODE_ID", 1)?;
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
        let provider = env_optional("MVP_NODE_PROVIDER").unwrap_or_else(|| "process".to_owned());
        let default_device = if provider == "process" {
            "CPU"
        } else {
            DEFAULT_DEVICE
        };
        Ok(Self {
            run_id,
            logical_node_id,
            stage_index: env_parse!("MVP_STAGE_INDEX", 0)?,
            coordinator_endpoint: env_optional("MVP_COORDINATOR_ENDPOINT")
                .map(|value| {
                    serde_json::from_str::<EndpointAddr>(&value)
                        .map_err(|e| format!("invalid MVP_COORDINATOR_ENDPOINT JSON: {e}"))
                })
                .transpose()?,
            orchestrator_actor: env_optional("MVP_ORCHESTRATOR_ACTOR")
                .map(|value| {
                    serde_json::from_str::<ActorAddress>(&value)
                        .map_err(|e| format!("invalid MVP_ORCHESTRATOR_ACTOR JSON: {e}"))
                })
                .transpose()?,
            datastream_frame_log: env_optional("MVP_DATASTREAM_FRAME_LOG"),
            debug_join_socket,
            relay_mode: relay.mode,
            endpoint_addr_mask: env_optional(MVP_IROH_ENDPOINT_ADDR_MASK_ENV)
                .as_deref()
                .map(EndpointAddrMask::parse)
                .transpose()?
                .unwrap_or_default(),
            worker_script: env_optional("MVP_TINYGRAD_WORKER")
                .unwrap_or_else(|| DEFAULT_WORKER_SCRIPT.to_owned()),
            device: env_optional("DEV").unwrap_or_else(|| default_device.to_owned()),
            model_id: env_optional("MVP_MODEL_ID").unwrap_or_else(|| DEFAULT_MODEL_ID.to_owned()),
            gguf_source: if let Some(path) = env_optional("MVP_GGUF_LOCAL_PATH") {
                GgufSource::LocalPath(path)
            } else {
                GgufSource::HuggingFaceGguf {
                    repo: env_optional("MVP_GGUF_REPO")
                        .unwrap_or_else(|| DEFAULT_HF_REPO.to_owned()),
                    file: env_optional("MVP_GGUF_FILE")
                        .unwrap_or_else(|| DEFAULT_HF_FILE.to_owned()),
                    revision: env_optional("MVP_GGUF_REVISION"),
                }
            },
            tokenizer: env_optional("MVP_TOKENIZER_LOCAL_PATH")
                .map(TokenizerSource::LocalPath)
                .unwrap_or(TokenizerSource::EmbeddedGguf),
            self_test_prompt: env_optional("MVP_NODE_SELF_TEST_PROMPT"),
            self_test_layer_end: env_parse!("MVP_SELF_TEST_LAYER_END", 16)?,
            self_test_max_tokens: env_parse!("MVP_SELF_TEST_MAX_TOKENS", 1)?,
            arena_bytes: env_parse!("MVP_ARENA_BYTES", DEFAULT_ARENA_BYTES)?,
            arena_alignment: env_parse!("MVP_ARENA_ALIGNMENT", DEFAULT_ARENA_ALIGNMENT)?,
        })
    }
}

#[derive(Debug)]
enum HelperStdoutEvent {
    Line(String),
    Closed,
    ReadError(String),
}

#[derive(Clone, Copy)]
struct HelperCommandWaitConfig {
    poll_interval: Duration,
    telemetry_interval: Duration,
}

impl HelperCommandWaitConfig {
    fn production() -> Self {
        Self {
            poll_interval: PUMP_INTERVAL,
            telemetry_interval: WORKER_COMMAND_WAIT_TELEMETRY_INTERVAL,
        }
    }
}

fn spawn_helper_stdout_reader<R: Read + Send + 'static>(reader: R, tx: Sender<HelperStdoutEvent>) {
    thread::spawn(move || {
        let mut reader = BufReader::new(reader);
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = tx.send(HelperStdoutEvent::Closed);
                    break;
                }
                Ok(_) => {
                    if tx.send(HelperStdoutEvent::Line(line)).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    let _ = tx.send(HelperStdoutEvent::ReadError(error.to_string()));
                    break;
                }
            }
        }
    });
}

fn drain_worker_stderr(
    stderr_rx: &Receiver<String>,
    config: &DeploymentConfig,
    datastream: &mut NodeDatastream,
) {
    let mut emitted = false;
    while let Ok(line) = stderr_rx.try_recv() {
        let payload = node_event_payload(config, "worker_stderr", "observed", json!({"line":line}));
        datastream.submit_text(datastream.channels.worker_stderr, payload.to_string());
        emitted = true;
    }
    if emitted {
        datastream.tick();
    }
}

#[allow(clippy::too_many_arguments)]
fn wait_for_helper_event(
    stdout_rx: &Receiver<HelperStdoutEvent>,
    stderr_rx: Option<&Receiver<String>>,
    expected: &str,
    command_type: &str,
    config: &DeploymentConfig,
    datastream: &mut NodeDatastream,
    channel: ChannelId,
    channel_name: &str,
    wait_config: HelperCommandWaitConfig,
    pump: &mut dyn FnMut(),
) -> Result<Value, String> {
    emit_node_event(
        datastream,
        config,
        NODE_WORKER_CHANNEL,
        "worker_stdout_read",
        "started",
        json!({"command_type":command_type,"expected_event_type":expected,"channel":channel_name}),
    );
    let wait_started = Instant::now();
    let mut wait_cycles = 0_u64;
    let mut next_telemetry_at = wait_started;
    loop {
        match stdout_rx.recv_timeout(wait_config.poll_interval) {
            Ok(HelperStdoutEvent::Line(line)) => {
                if let Some(stderr_rx) = stderr_rx {
                    drain_worker_stderr(stderr_rx, config, datastream);
                }
                let line_bytes = line.len();
                emit_node_event(
                    datastream,
                    config,
                    NODE_WORKER_CHANNEL,
                    "worker_stdout_read",
                    "ready",
                    json!({"command_type":command_type,"expected_event_type":expected,"channel":channel_name,"line_bytes":line_bytes}),
                );
                emit_node_event(
                    datastream,
                    config,
                    NODE_WORKER_CHANNEL,
                    "worker_stdout_parse",
                    "started",
                    json!({"command_type":command_type,"expected_event_type":expected,"channel":channel_name,"line_bytes":line_bytes}),
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
                            json!({"command_type":command_type,"expected_event_type":expected,"channel":channel_name,"line_bytes":line_bytes,"error":error.to_string()}),
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
                    json!({"command_type":command_type,"expected_event_type":expected,"channel":channel_name,"line_bytes":line_bytes,"worker_event_type":worker_event_type}),
                );
                datastream.submit_text(channel, value.to_string());
                emit_stdio_datastream_frame(channel_name, &value)
                    .map_err(|e| format!("emit worker stdio datastream frame: {e}"))?;
                datastream.tick();
                pump();
                if worker_event_type == "WorkerFatal" {
                    return Err(format!("worker fatal: {value}"));
                }
                if value.get("type").and_then(Value::as_str) == Some(expected) {
                    return Ok(value);
                }
                emit_node_event(
                    datastream,
                    config,
                    NODE_WORKER_CHANNEL,
                    "worker_event",
                    "observed",
                    json!({"command_type":command_type,"command_waiting_for":expected,"worker_event_type":worker_event_type,"event":value}),
                );
            }
            Ok(HelperStdoutEvent::Closed) => {
                if let Some(stderr_rx) = stderr_rx {
                    drain_worker_stderr(stderr_rx, config, datastream);
                }
                emit_node_event(
                    datastream,
                    config,
                    NODE_WORKER_CHANNEL,
                    "worker_stdout_read",
                    "failed",
                    json!({"command_type":command_type,"expected_event_type":expected,"channel":channel_name,"line_bytes":0,"error":"stdout closed"}),
                );
                return Err(format!(
                    "tinygrad helper stdout closed while waiting for {expected}"
                ));
            }
            Ok(HelperStdoutEvent::ReadError(error)) => {
                if let Some(stderr_rx) = stderr_rx {
                    drain_worker_stderr(stderr_rx, config, datastream);
                }
                emit_node_event(
                    datastream,
                    config,
                    NODE_WORKER_CHANNEL,
                    "worker_stdout_read",
                    "failed",
                    json!({"command_type":command_type,"expected_event_type":expected,"channel":channel_name,"error":error}),
                );
                return Err(format!("read helper stdout: {error}"));
            }
            Err(RecvTimeoutError::Timeout) => {
                wait_cycles = wait_cycles.saturating_add(1);
                pump();
                if let Some(stderr_rx) = stderr_rx {
                    drain_worker_stderr(stderr_rx, config, datastream);
                }
                let now = Instant::now();
                if now >= next_telemetry_at {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_WORKER_CHANNEL,
                        "worker_command_wait",
                        "waiting",
                        json!({
                            "command_type":command_type,
                            "expected_event_type":expected,
                            "channel":channel_name,
                            "state":"busy_waiting_for_helper_stdout",
                            "elapsed_ms":duration_ms_u64(now.saturating_duration_since(wait_started)),
                            "wait_cycles":wait_cycles,
                            "poll_interval_ms":duration_ms_u64(wait_config.poll_interval),
                        }),
                    );
                    next_telemetry_at = now
                        .checked_add(wait_config.telemetry_interval)
                        .unwrap_or(now);
                }
                datastream.tick();
            }
            Err(RecvTimeoutError::Disconnected) => {
                emit_node_event(
                    datastream,
                    config,
                    NODE_WORKER_CHANNEL,
                    "worker_stdout_read",
                    "failed",
                    json!({"command_type":command_type,"expected_event_type":expected,"channel":channel_name,"line_bytes":0,"error":"stdout reader disconnected"}),
                );
                return Err(format!(
                    "tinygrad helper stdout reader disconnected while waiting for {expected}"
                ));
            }
        }
    }
}

struct TinygradWorker {
    child: Child,
    stdin: ChildStdin,
    stdout_rx: Receiver<HelperStdoutEvent>,
    stderr_rx: Receiver<String>,
}

impl TinygradWorker {
    fn spawn(config: &DeploymentConfig, arena_fd: std::os::fd::RawFd) -> Result<Self, String> {
        let mut child = Command::new("python3")
            .arg(&config.worker_script)
            .env("DEV", &config.device)
            .env("MVP_RUN_ID", config.run_id.to_string())
            .env("MVP_LOGICAL_NODE_ID", config.logical_node_id.to_string())
            .env("MVP_STAGE_INDEX", config.stage_index.to_string())
            .env("MVP_ARENA_FD", arena_fd.to_string())
            .env("MVP_ARENA_BYTES", config.arena_bytes.to_string())
            .env(
                "MVP_DATASTREAM_ENDPOINT_ID",
                format!(
                    "worker-node-{}-stage-{}-stdio-bridge",
                    config.logical_node_id, config.stage_index
                ),
            )
            .env(
                "MVP_BENCHMARK_PRODUCER_INSTANCE",
                format!(
                    "tinygrad-worker:{}:{}",
                    config.logical_node_id, config.stage_index
                ),
            )
            .env(
                "MVP_IROH_ENDPOINT_ADDR_MASK",
                config.endpoint_addr_mask.as_str(),
            )
            .env("MVP_IROH_RELAY_MODE", format!("{:?}", config.relay_mode))
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
        let (stdout_tx, stdout_rx) = mpsc::channel();
        spawn_helper_stdout_reader(stdout, stdout_tx);
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
            stdout_rx,
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
        datastream: &mut NodeDatastream,
        pump: &mut dyn FnMut(),
    ) -> Result<(), String> {
        self.command(
            json!({"type":"InitializeWorker","helper_abi_version":1,"backend":{"device":device}}),
            "WorkerReady",
            config,
            datastream,
            "mvp.worker.initialize",
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
        datastream: &mut NodeDatastream,
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
            "mvp.worker.role",
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
        datastream: &mut NodeDatastream,
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
            "mvp.worker.weights",
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
        datastream: &mut NodeDatastream,
        pump: &mut dyn FnMut(),
    ) -> Result<Value, String> {
        self.command(
            json!({"type":"InferPrompt","request_id":request_id,"prompt":prompt,"max_tokens":max_tokens}),
            "PromptCompleted",
            config,
            datastream,
            "mvp.worker.prompt",
            pump,
        )
    }

    fn encode_prompt(
        &mut self,
        request_id: u64,
        prompt: &str,
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
        pump: &mut dyn FnMut(),
    ) -> Result<Vec<u32>, String> {
        let result = self.command(
            json!({"type":"EncodePrompt","request_id":request_id,"prompt":prompt}),
            "PromptEncoded",
            config,
            datastream,
            "mvp.worker.tokenizer",
            pump,
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
        datastream: &mut NodeDatastream,
        pump: &mut dyn FnMut(),
    ) -> Result<String, String> {
        let result = self.command(
            json!({"type":"DecodeTokens","request_id":request_id,"tokens":tokens}),
            "TokensDecoded",
            config,
            datastream,
            "mvp.worker.tokenizer",
            pump,
        )?;
        result
            .get("text")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| "TokensDecoded missing text".to_owned())
    }

    fn install_ring(
        &mut self,
        ring_id: u64,
        edge_id: u64,
        port: &str,
        direction: &str,
        layout: arena::RingLayout,
        object_spec: StageObjectSpecWire,
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
        pump: &mut dyn FnMut(),
    ) -> Result<(), String> {
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
            datastream,
            "mvp.worker.ring",
            pump,
        )
        .map(|_| ())
    }

    fn uninstall_ring(
        &mut self,
        ring_id: u64,
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
        pump: &mut dyn FnMut(),
    ) -> Result<(), String> {
        self.command(
            json!({"type":"UninstallRing","ring_id":ring_id}),
            "RingUninstalled",
            config,
            datastream,
            "mvp.worker.ring",
            pump,
        )
        .map(|_| ())
    }

    fn ring_readable(
        &mut self,
        ring_id: u64,
        edge_id: u64,
        object_spec: StageObjectSpecWire,
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
        pump: &mut dyn FnMut(),
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
            datastream,
            "mvp.worker.ingress",
            pump,
        )?;
        Ok(LoadedObject {
            object_id: value_u64(&event, "object_id")?,
            sequence: value_u64(&event, "sequence")?,
            handle_generation: value_u64(&event, "handle_generation")?,
            handle_id: value_u64(&event, "handle_id")?,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_step(
        &mut self,
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
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
        pump: &mut dyn FnMut(),
    ) -> Result<usize, String> {
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
            datastream,
            "mvp.worker.step",
            pump,
        )?;
        usize::try_from(value_u64(&event, "committed_bytes")?)
            .map_err(|_| "StepExecuted committed_bytes does not fit usize".to_owned())
    }

    fn release_device_object(
        &mut self,
        handle_id: u64,
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
        pump: &mut dyn FnMut(),
    ) -> Result<(), String> {
        self.command(
            json!({"type":"ReleaseDeviceObject","handle_id":handle_id}),
            "DeviceObjectReleased",
            config,
            datastream,
            "mvp.worker.device_object",
            pump,
        )
        .map(|_| ())
    }

    fn shutdown(
        &mut self,
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
        pump: &mut dyn FnMut(),
    ) -> Result<(), String> {
        self.command(
            json!({"type":"ShutdownWorker"}),
            "WorkerStopped",
            config,
            datastream,
            "mvp.worker.shutdown",
            pump,
        )
        .map(|_| ())
    }

    fn try_wait(&mut self) -> Result<Option<std::process::ExitStatus>, String> {
        self.child
            .try_wait()
            .map_err(|e| format!("poll tinygrad helper: {e}"))
    }

    fn drain_stderr(&mut self, config: &DeploymentConfig, datastream: &mut NodeDatastream) {
        drain_worker_stderr(&self.stderr_rx, config, datastream);
    }

    fn command(
        &mut self,
        command: Value,
        expected: &str,
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
        channel_name: &str,
        pump: &mut dyn FnMut(),
    ) -> Result<Value, String> {
        let channel = datastream.channel_by_name(channel_name);
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
        self.expect_event(
            expected,
            command_type.as_str(),
            config,
            datastream,
            channel,
            channel_name,
            pump,
        )
    }

    fn expect_event(
        &mut self,
        expected: &str,
        command_type: &str,
        config: &DeploymentConfig,
        datastream: &mut NodeDatastream,
        channel: ChannelId,
        channel_name: &str,
        pump: &mut dyn FnMut(),
    ) -> Result<Value, String> {
        wait_for_helper_event(
            &self.stdout_rx,
            Some(&self.stderr_rx),
            expected,
            command_type,
            config,
            datastream,
            channel,
            channel_name,
            HelperCommandWaitConfig::production(),
            pump,
        )
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

fn env_optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}
