use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufRead, BufReader, IsTerminal, Write};
use std::net::{Shutdown, TcpStream};
#[cfg(all(target_os = "linux", not(test)))]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use datastream::{
    ChannelContent, ChannelId, DatastreamEndpoint, DatastreamProducer, Frame, Lifetime, NodeId,
    StreamDescriptor, StreamId, StreamOrigin,
};
use serde::Deserialize;
use serde_json::{Value, json};
#[cfg(target_os = "linux")]
use signal_hook::consts::signal::{SIGINT, SIGTERM};
#[cfg(target_os = "linux")]
use signal_hook::iterator::Signals;

use crate::chat::node_image::{
    NodeImageProgressEvent, NodeImageProgressEventKind, NodeImageProgressSink, NodeImageRequest,
    prepare_node_image_with_progress,
};
use crate::node_provisioning::{ProviderKind, provider_kind};
use crate::observability::{benchmark, frame_archive::FrameArchive};
use crate::orchestration::config::ResolvedVastAiConfig;
use crate::prompt::rpc::{PromptEvent, SubmitPrompt, write_json_line};
use crate::{
    DEFAULT_PIPELINE_CACHED_MODEL_FILE, DEFAULT_PIPELINE_CACHED_MODEL_ID,
    DEFAULT_PIPELINE_CACHED_MODEL_MAX_CONTEXT, DEFAULT_PIPELINE_CACHED_MODEL_REPO,
};
use iroh_driver::EndpointAddrMask;

const DEFAULT_CONFIG_PATH: &str = ".config/config.toml";
const DEFAULT_RPC_ADDR: &str = "127.0.0.1:19777";
const BASE_NODE_IMAGE: &str = "myelin-node-base:cuda12.6";
const REPO_MODEL_CACHE_DIR: &str = ".model-cache";
const DEFAULT_MAX_TOKENS: u32 = 64;
const ORCH_SHUTDOWN_GRACE_MS: u64 = 5_000;
const VASTAI_ORCH_SHUTDOWN_GRACE_MS: u64 = 180_000;
const MYELIN_CHAT_GPU_RUN_ENV: &str = "MYELIN_CHAT_GPU_RUN";
const MYELIN_CHAT_USAGE: &str = "\
USAGE: cargo myelin-chat [OPTIONS]

OPTIONS:
  --gpu                         Run the local GPU path: in-process orchestrator plus DEV=CUDA worker selection
  --process | --docker | --vastai
                                Select the runtime provider
  --config <path>               Load config overlay
  --pipeline-stages|--pipeline-parallel <count>
  --relay-mode <mode>           Relay mode: default or disabled
  --relay-url <url>             Custom relay URL passed to myelin-orchestrator
  --endpoint-addr-mask <mask>   Endpoint address mask: full or relay-only
  --cached-model[=<path>]       Use discovered or explicit cached GGUF model (default for --process)
  --dump-logs[=<path>]          Write datastream frame log
  --run-id <id>                 Override run id
  --skip-rebuild                Reuse existing Cargo artifacts
  --yes, -y                     Approve Vast.ai lease prompts
  --help, -h                    Print this help";
const ORCH_SHUTDOWN_POLL_MS: u64 = 50;
const CHAT_LIFECYCLE_CHANNEL: &str = "myelin.chat.lifecycle";
const CHAT_RUNTIME_CHANNEL: &str = "myelin.chat.runtime";
const CHAT_PROMPT_CHANNEL: &str = "myelin.chat.prompt";
const CHAT_COMPONENT_CHANNEL: &str = "myelin.chat.component";
const CHAT_BENCHMARK_CHANNEL: &str = "myelin.chat.benchmark";

#[derive(Debug)]
enum PromptInput {
    Line(String),
    Closed,
    StopRequested,
}

static STOP_REQUESTED: AtomicBool = AtomicBool::new(false);
static PROMPT_STOP_TX: Mutex<Option<mpsc::Sender<PromptInput>>> = Mutex::new(None);

pub(crate) fn run_from_args<I>(args: I) -> ExitCode
where
    I: IntoIterator<Item = String>,
{
    match install_signal_handlers().and_then(|()| run(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("myelin-chat: {error}");
            ExitCode::from(1)
        }
    }
}

struct RuntimeEnvGuard {
    name: &'static str,
    original: Option<std::ffi::OsString>,
}

impl RuntimeEnvGuard {
    fn apply_gpu_defaults(gpu_run: bool) -> Option<Self> {
        if !gpu_run || std::env::var_os("DEV").is_some() {
            return None;
        }
        let guard = Self {
            name: "DEV",
            original: None,
        };
        unsafe { std::env::set_var(guard.name, "CUDA") };
        Some(guard)
    }
}

impl Drop for RuntimeEnvGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(value) => unsafe { std::env::set_var(self.name, value) },
            None => unsafe { std::env::remove_var(self.name) },
        }
    }
}

// blocking user-stdin thread is process control, out of scope (ENGINE_SPEC.md §2)
#[allow(clippy::disallowed_methods)]
fn run<I>(args: I) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    let provided_args = args.into_iter().collect::<Vec<_>>();
    if provided_args
        .iter()
        .any(|arg| matches!(arg.as_str(), "--help" | "-h" | "help"))
    {
        println!("{MYELIN_CHAT_USAGE}");
        return Ok(());
    }
    let config = Config::from_args(provided_args)?;
    let _gpu_env = RuntimeEnvGuard::apply_gpu_defaults(config.gpu_run);
    let mut progress = ChatDatastream::new(config.run_id, config.datastream_frame_log.clone())?;
    progress.emit(
        CHAT_LIFECYCLE_CHANNEL,
        "config",
        "ready",
        json!({
            "provider": config.provider.as_str(),
            "pipeline_stages": config.pipeline_stages,
            "max_tokens": config.max_tokens,
            "cached_model": config.cached_model.as_ref().map(|model| model.host_path.to_string_lossy().to_string()),
            "dump_logs": config.datastream_frame_log.as_ref().map(|path| path.to_string_lossy().to_string()),
            "gpu_run": config.gpu_run,
        }),
    );
    progress.emit_benchmark_envelope(&config);
    progress.emit_endpoint_config_snapshot(&config);
    confirm_vastai_if_needed(&config)?;
    let prepare_runtime_started = Instant::now();
    progress.emit(
        CHAT_RUNTIME_CHANNEL,
        "prepare_runtime",
        "started",
        json!({"provider": config.provider.as_str()}),
    );
    let image_ref = match prepare_runtime_with_progress(
        &config,
        prepare_node_image_with_progress,
        Some(&mut progress),
    ) {
        Ok(image_ref) => {
            progress.emit(
                    CHAT_RUNTIME_CHANNEL,
                    "prepare_runtime",
                    "ready",
                    json!({"image_ref": image_ref, "elapsed_ms": prepare_runtime_started.elapsed().as_millis()}),
                );
            image_ref
        }
        Err(error) => {
            progress.emit(
                    CHAT_RUNTIME_CHANNEL,
                    "prepare_runtime",
                    "failed",
                    json!({"error": error, "elapsed_ms": prepare_runtime_started.elapsed().as_millis()}),
                );
            progress.archive_pending()?;
            return Err(error);
        }
    };
    progress.emit(
        CHAT_COMPONENT_CHANNEL,
        "orchestrator_process_spawn",
        "started",
        json!({
            "mode": config.orchestrator_launch_mode(),
            "binary": config.orch_bin.to_string_lossy(),
        }),
    );
    let mut orch = match OrchHandle::spawn(&config, &image_ref) {
        Ok(orch) => {
            progress.emit(
                CHAT_COMPONENT_CHANNEL,
                "orchestrator_process_spawn",
                "ready",
                json!({
                    "mode": config.orchestrator_launch_mode(),
                    "binary": config.orch_bin.to_string_lossy(),
                }),
            );
            progress.emit(
                CHAT_COMPONENT_CHANNEL,
                "orchestrator_process",
                "started",
                json!({
                    "mode": config.orchestrator_launch_mode(),
                    "binary": config.orch_bin.to_string_lossy(),
                }),
            );
            orch
        }
        Err(error) => {
            progress.emit(
                CHAT_COMPONENT_CHANNEL,
                "orchestrator_process_spawn",
                "failed",
                json!({
                    "mode": config.orchestrator_launch_mode(),
                    "binary": config.orch_bin.to_string_lossy(),
                    "error": error,
                }),
            );
            progress.emit(
                CHAT_COMPONENT_CHANNEL,
                "orchestrator_process",
                "failed",
                json!({"mode": config.orchestrator_launch_mode(), "error": error}),
            );
            progress.archive_pending()?;
            return Err(error);
        }
    };
    progress.emit(
        CHAT_RUNTIME_CHANNEL,
        "prompt_rpc_wait",
        "started",
        json!({"addr": config.rpc_addr}),
    );
    let rpc_addr = match orch.wait_ready(config.rpc_addr.clone()) {
        Ok(addr) => {
            progress.emit(
                CHAT_RUNTIME_CHANNEL,
                "prompt_rpc_wait",
                "ready",
                json!({"addr": addr}),
            );
            progress.emit(
                CHAT_RUNTIME_CHANNEL,
                "prompt_rpc",
                "ready",
                json!({"addr": addr}),
            );
            addr
        }
        Err(error) if STOP_REQUESTED.load(Ordering::SeqCst) => {
            progress.emit(
                CHAT_RUNTIME_CHANNEL,
                "prompt_rpc_wait",
                "failed",
                json!({"addr": config.rpc_addr, "error": error}),
            );
            progress.emit(
                CHAT_LIFECYCLE_CHANNEL,
                "shutdown",
                "requested",
                json!({"reason": "interrupted_before_ready"}),
            );
            orch.shutdown();
            progress.emit(
                CHAT_COMPONENT_CHANNEL,
                "orchestrator_process",
                "stopped",
                json!({"reason": "interrupted_before_ready"}),
            );
            progress.archive_pending()?;
            return Ok(());
        }
        Err(error) => {
            progress.emit(
                CHAT_RUNTIME_CHANNEL,
                "prompt_rpc_wait",
                "failed",
                json!({"addr": config.rpc_addr, "error": error}),
            );
            progress.emit(
                CHAT_RUNTIME_CHANNEL,
                "prompt_rpc",
                "failed",
                json!({"error": error}),
            );
            orch.shutdown();
            progress.emit(
                CHAT_COMPONENT_CHANNEL,
                "orchestrator_process",
                "stopped",
                json!({"reason": "startup_failed"}),
            );
            progress.archive_pending()?;
            return Err(error);
        }
    };
    let (prompt_tx, input_rx) = mpsc::channel();
    if STOP_REQUESTED.load(Ordering::SeqCst) {
        let _ = prompt_tx.send(PromptInput::StopRequested);
    }
    if let Ok(mut stop_tx) = PROMPT_STOP_TX.lock() {
        *stop_tx = Some(prompt_tx.clone());
    }
    thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            match line {
                Ok(line) => {
                    if prompt_tx.send(PromptInput::Line(line)).is_err() {
                        return;
                    }
                }
                Err(_) => {
                    let _ = prompt_tx.send(PromptInput::Closed);
                    return;
                }
            }
        }
        let _ = prompt_tx.send(PromptInput::Closed);
    });
    let result = run_chat_loop_with_input_and_progress(
        &rpc_addr,
        config.max_tokens,
        input_rx,
        Some(&mut progress),
    );
    progress.emit(
        CHAT_LIFECYCLE_CHANNEL,
        "shutdown",
        "requested",
        json!({"reason": "prompt_loop_exited", "ok": result.is_ok()}),
    );
    orch.shutdown();
    progress.emit(
        CHAT_COMPONENT_CHANNEL,
        "orchestrator_process",
        "stopped",
        json!({"reason": "shutdown_requested"}),
    );
    progress.archive_pending()?;
    result
}

struct Config {
    orch_bin: PathBuf,
    worker_bin: PathBuf,
    rpc_addr: String,
    node_image: String,
    provider: ProviderKind,
    image_tag: Option<String>,
    cached_model: Option<CachedModelConfig>,
    datastream_frame_log: Option<PathBuf>,
    run_id: u64,
    vastai_yes: bool,
    vastai: Option<ResolvedVastAiConfig>,
    model: ChatModelConfig,
    pipeline_stages: u32,
    max_tokens: u32,
    skip_rebuild: bool,
    gpu_run: bool,
    relay_mode: Option<String>,
    relay_url: Option<String>,
    endpoint_addr_mask: EndpointAddrMask,
}

struct ChatDatastream {
    stream: StreamId,
    run_id: u64,
    endpoint: DatastreamEndpoint,
    producer: DatastreamProducer,
    channels: BTreeMap<String, ChannelId>,
    channel_names: BTreeMap<ChannelId, String>,
    archive_path: Option<PathBuf>,
    pending: Vec<(String, StreamId, String, Frame)>,
}

impl ChatDatastream {
    fn new(run_id: u64, archive_path: Option<PathBuf>) -> Result<Self, String> {
        let stream = StreamId::new(NodeId::new("myelin-chat"), Lifetime(run_id));
        let endpoint = DatastreamEndpoint::with_descriptor(
            StreamDescriptor {
                stream: stream.clone(),
                label: Some("myelin chat".to_owned()),
                origin: StreamOrigin::Orchestrator,
            },
            1024,
            256,
        );
        let producer = endpoint.producer();
        let mut out = Self {
            stream,
            run_id,
            endpoint,
            producer,
            channels: BTreeMap::new(),
            channel_names: BTreeMap::new(),
            archive_path,
            pending: Vec::new(),
        };
        for name in [
            CHAT_LIFECYCLE_CHANNEL,
            CHAT_RUNTIME_CHANNEL,
            CHAT_PROMPT_CHANNEL,
            CHAT_COMPONENT_CHANNEL,
            CHAT_BENCHMARK_CHANNEL,
        ] {
            out.channel_by_name(name);
        }
        Ok(out)
    }

    fn channel_by_name(&mut self, name: &str) -> ChannelId {
        if let Some(id) = self.channels.get(name).copied() {
            return id;
        }
        let id = self.producer.register_channel(
            name,
            ChannelContent::JsonRecord {
                schema: Some(name.to_owned()),
            },
        );
        self.channels.insert(name.to_owned(), id);
        self.channel_names.insert(id, name.to_owned());
        id
    }

    fn emit(&mut self, channel: &str, phase: &str, status: &str, detail: Value) {
        let id = self.channel_by_name(channel);
        let benchmark = benchmark::stamp("myelin-chat");
        let payload = serde_json::to_vec(&json!({
            "schema_version": benchmark["schema_version"].clone(),
            "type": "ChatProgress",
            "event_type": "ChatProgress",
            "event_name": phase,
            "phase": phase,
            "status": status,
            "run_id": self.run_id,
            "producer_component": benchmark["producer_component"].clone(),
            "producer_instance_id": benchmark["producer_instance_id"].clone(),
            "producer_process_id": benchmark["producer_process_id"].clone(),
            "producer_sequence": benchmark["producer_sequence"].clone(),
            "wall_clock_unix_ms": benchmark["wall_clock_unix_ms"].clone(),
            "monotonic_ms": benchmark["monotonic_ms"].clone(),
            "clock_source": benchmark["clock_source"].clone(),
            "span_id": format!("myelin-chat:{}:{}:{phase}", self.run_id, benchmark["producer_sequence"]),
            "parent_span_id": Value::Null,
            "benchmark": benchmark,
            "detail": detail,
        }))
        .expect("serialize myelin-chat progress event");
        self.producer.submit_bytes(id, payload);
        self.flush();
    }

    fn emit_benchmark_envelope(&mut self, config: &Config) {
        let id = self.channel_by_name(CHAT_BENCHMARK_CHANNEL);
        let benchmark = benchmark::stamp("myelin-chat");
        let payload = serde_json::to_vec(&json!({
            "schema_version": benchmark["schema_version"].clone(),
            "type": "BenchmarkRunEnvelope",
            "event_type": "BenchmarkRunEnvelope",
            "event_name": "run_envelope",
            "phase": "run_envelope",
            "status": "ready",
            "run_id": self.run_id,
            "producer_component": benchmark["producer_component"].clone(),
            "producer_instance_id": benchmark["producer_instance_id"].clone(),
            "producer_process_id": benchmark["producer_process_id"].clone(),
            "producer_sequence": benchmark["producer_sequence"].clone(),
            "wall_clock_unix_ms": benchmark["wall_clock_unix_ms"].clone(),
            "monotonic_ms": benchmark["monotonic_ms"].clone(),
            "clock_source": benchmark["clock_source"].clone(),
            "span_id": format!("myelin-chat:{}:{}:run_envelope", self.run_id, benchmark["producer_sequence"]),
            "parent_span_id": Value::Null,
            "benchmark": benchmark,
            "detail": {
                "scenario": "myelin-chat",
                "detail_level": "benchmark_observability_v1",
                "workload": {
                    "mode": "stdin_prompt_corpus",
                    "max_tokens": config.max_tokens,
                    "prompt_corpus": "external_or_stdin",
                },
                "model": {
                    "id": config.model.id.as_deref(),
                    "gguf_local_path": config.model.gguf_local_path.as_deref(),
                    "gguf_repo": config.model.gguf_repo.as_deref(),
                    "gguf_file": config.model.gguf_file.as_deref(),
                    "gguf_revision": config.model.gguf_revision.as_deref(),
                    "tokenizer_local_path": config.model.tokenizer_local_path.as_deref(),
                    "max_context": config.model.max_context,
                },
                "runtime": {
                    "provider": config.provider.as_str(),
                    "pipeline_stages": config.pipeline_stages,
                    "orchestrator_launch_mode": config.orchestrator_launch_mode(),
                    "gpu_run": config.gpu_run,
                    "relay_mode": config.relay_mode.as_deref(),
                    "relay_configured": config.relay_url.is_some(),
                    "endpoint_addr_mask": config.endpoint_addr_mask.as_str(),
                },
                "provider": {
                    "kind": config.provider.as_str(),
                    "node_image": &config.node_image,
                    "image_tag": config.image_tag.as_deref(),
                    "cached_model": config.cached_model.as_ref().map(|model| model.host_path.to_string_lossy().to_string()),
                    "vastai": config.vastai.as_ref().map(|vastai| json!({
                        "image": &vastai.image,
                        "relay_configured": !vastai.relay_url.is_empty(),
                        "bootstrap_command_configured": !vastai.bootstrap_command.is_empty(),
                        "gpu_name": vastai.gpu_name.as_deref(),
                        "min_gpu_ram_mb": vastai.min_gpu_ram_mb,
                        "min_down_mbps": vastai.min_down_mbps,
                        "min_up_mbps": vastai.min_up_mbps,
                        "max_dph_total": vastai.max_dph_total,
                        "min_reliability": vastai.min_reliability,
                        "require_verified": vastai.require_verified,
                        "blacklist_hosts": &vastai.blacklist_hosts,
                        "disk_gb": vastai.disk_gb,
                        "has_onstart": vastai.onstart.is_some(),
                        "has_ssh_identity": vastai.ssh_identity.is_some(),
                    })),
                },
            },
        }))
        .expect("serialize myelin-chat benchmark envelope");
        self.producer.submit_bytes(id, payload);
        self.flush();
    }

    fn emit_endpoint_config_snapshot(&mut self, config: &Config) {
        let endpoint = json!({
            "role": "chat-frame-archive",
            "transport": "datastream-frame-log",
            "configured": config.datastream_frame_log.is_some(),
            "archive_path": config.datastream_frame_log.as_ref().map(|path| path.to_string_lossy().to_string()),
        });
        let runtime_endpoint = json!({
            "provider": config.provider.as_str(),
            "relay_mode": config.relay_mode.as_deref(),
            "relay_configured": config.relay_url.is_some(),
            "endpoint_addr_mask": config.endpoint_addr_mask.as_str(),
        });
        let synthetic_id = format!("myelin-chat-{}-datastream-preflight", self.run_id);
        for (phase, status) in [
            ("DatastreamProducerConfigured", "configured"),
            ("DatastreamProducerConnected", "ready"),
            ("DatastreamSyntheticEventSent", "sent"),
            ("DatastreamSyntheticEventObserved", "observed"),
        ] {
            self.emit(
                CHAT_BENCHMARK_CHANNEL,
                phase,
                status,
                json!({
                    "producer": "myelin-chat",
                    "producer_class": "rust-chat",
                    "synthetic_id": synthetic_id,
                    "datastream_endpoint": endpoint,
                    "runtime_endpoint": runtime_endpoint,
                }),
            );
        }
        self.emit(
            CHAT_BENCHMARK_CHANNEL,
            "endpoint_config_snapshot",
            "ready",
            json!({
                "producer": "myelin-chat",
                "expected_producers": ["myelin-chat", "myelin-orchestrator", "myelin-worker", "tinygrad-worker"],
                "datastream_endpoint": endpoint,
                "runtime_endpoint": runtime_endpoint,
                "connectivity_preflight": {
                    "status": "configured",
                    "canonical_datastream_required": true,
                },
            }),
        );
    }

    fn flush(&mut self) {
        let stream = self.stream.clone();
        for frame in self.endpoint.mux().drain() {
            let channel = self
                .channel_names
                .get(&frame.channel)
                .cloned()
                .unwrap_or_else(|| format!("channel#{}", frame.channel.0));
            self.pending
                .push(("myelin-chat".to_owned(), stream.clone(), channel, frame));
        }
    }

    fn archive_pending(&mut self) -> Result<(), String> {
        let Some(path) = self.archive_path.as_deref() else {
            self.pending.clear();
            return Ok(());
        };
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut archive = FrameArchive::open_with_label(path, "myelin-chat datastream frame log")?;
        for (source, stream, channel, frame) in self.pending.drain(..) {
            archive.record(&source, &stream, &channel, &frame)?;
        }
        Ok(())
    }
}

impl NodeImageProgressSink for ChatDatastream {
    fn emit(&mut self, event: NodeImageProgressEvent) {
        let mut detail = serde_json::Map::new();
        if let Some(command_label) = event.command_label {
            detail.insert("command_label".to_owned(), json!(command_label));
        }
        if let Some(image_ref) = event.image_ref {
            detail.insert("image_ref".to_owned(), json!(image_ref));
        }
        if let Some(elapsed_ms) = event.elapsed_ms {
            detail.insert("elapsed_ms".to_owned(), json!(elapsed_ms));
        }

        let (phase, status) = match event.kind {
            NodeImageProgressEventKind::ImageReference { role, image_ref } => {
                detail.insert("event".to_owned(), json!("image_ref"));
                detail.insert("role".to_owned(), json!(role));
                detail.insert("image_ref".to_owned(), json!(image_ref));
                ("prepare_node_image", "image_ref")
            }
            NodeImageProgressEventKind::CommandStarted { program, args } => {
                detail.insert("event".to_owned(), json!("command_start"));
                detail.insert("program".to_owned(), json!(program));
                detail.insert("args".to_owned(), json!(args));
                ("node_image_command", "started")
            }
            NodeImageProgressEventKind::CommandStdout { line } => {
                detail.insert("event".to_owned(), json!("stdout"));
                detail.insert("stream".to_owned(), json!("stdout"));
                detail.insert("line".to_owned(), json!(line));
                ("node_image_command", "stdout")
            }
            NodeImageProgressEventKind::CommandStderr { line } => {
                detail.insert("event".to_owned(), json!("stderr"));
                detail.insert("stream".to_owned(), json!("stderr"));
                detail.insert("line".to_owned(), json!(line));
                ("node_image_command", "stderr")
            }
            NodeImageProgressEventKind::CommandExited {
                status: command_status,
                code,
                success,
            } => {
                detail.insert("event".to_owned(), json!("command_exit"));
                detail.insert("command_status".to_owned(), json!(command_status));
                detail.insert("exit_code".to_owned(), json!(code));
                detail.insert("success".to_owned(), json!(success));
                if let Some(elapsed_ms) = detail.get("elapsed_ms").cloned() {
                    detail.insert("duration_ms".to_owned(), elapsed_ms);
                }
                (
                    "node_image_command",
                    if success { "exited" } else { "failed" },
                )
            }
        };
        self.emit(CHAT_RUNTIME_CHANNEL, phase, status, Value::Object(detail));
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ChatTomlConfig {
    provider: ChatProviderConfig,
    runtime: ChatRuntimeConfig,
    observability: ChatObservabilityConfig,
    image: ChatImageConfig,
    vastai: ChatVastAiConfig,
    model: ChatModelConfig,
    relay: ChatRelayConfig,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ChatProviderConfig {
    kind: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ChatRuntimeConfig {
    pipeline_stages: Option<u32>,
    max_tokens: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ChatRelayConfig {
    mode: Option<String>,
    url: Option<String>,
    endpoint_addr_mask: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ChatObservabilityConfig {
    dump_logs: Option<bool>,
    dump_log_path: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ChatImageConfig {
    node: Option<String>,
    tag: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ChatVastAiConfig {
    relay_url: Option<String>,
    bootstrap_command: Option<String>,
    gpu_name: Option<String>,
    min_gpu_ram_mb: Option<u64>,
    min_down_mbps: Option<f64>,
    min_up_mbps: Option<f64>,
    max_dph_total: Option<f64>,
    min_reliability: Option<f64>,
    require_verified: Option<bool>,
    blacklist_hosts: Vec<u64>,
    disk_gb: Option<u32>,
    onstart: Option<String>,
    ssh_identity: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ChatModelConfig {
    id: Option<String>,
    gguf_local_path: Option<String>,
    gguf_repo: Option<String>,
    gguf_file: Option<String>,
    gguf_revision: Option<String>,
    tokenizer_local_path: Option<String>,
    max_context: Option<u32>,
}

fn load_chat_config(path: Option<&Path>) -> Result<ChatTomlConfig, String> {
    Ok(match path {
        Some(path) => {
            let text = fs::read_to_string(path)
                .map_err(|e| format!("read config {}: {e}", path.display()))?;
            toml::from_str::<ChatTomlConfig>(&text)
                .map_err(|e| format!("parse config {}: {e}", path.display()))?
        }
        None => {
            let default = Path::new(DEFAULT_CONFIG_PATH);
            if !default.is_file() {
                ChatTomlConfig::default()
            } else {
                let text = fs::read_to_string(default)
                    .map_err(|e| format!("read config {}: {e}", default.display()))?;
                toml::from_str::<ChatTomlConfig>(&text)
                    .map_err(|e| format!("parse config {}: {e}", default.display()))?
            }
        }
    })
}

impl Config {
    fn from_args<I>(provided_args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = String>,
    {
        let args = ParsedArgs::parse(provided_args)?;
        let toml = load_chat_config(args.config_path.as_deref())?;
        let provider = provider_from_sources(args.provider.clone(), toml.provider.kind.as_deref())?;
        let node_image = first_non_empty([toml.image.node.clone()]).unwrap_or_default();
        if provider != provider_kind::process() && node_image.is_empty() {
            return Err("node image is required for docker or vastai provider".to_owned());
        }
        let pipeline_stages = Self::pipeline_stages(&args, &toml)?;
        let max_tokens = Self::max_tokens(&toml)?;
        let gpu_run = args.gpu || env_flag(MYELIN_CHAT_GPU_RUN_ENV, false);
        let endpoint_addr_mask = Self::endpoint_addr_mask(&args, &toml)?;
        let (relay_mode, relay_url) = Self::relay_settings(&args, &toml, endpoint_addr_mask)?;
        let cached_model = match Self::cached_model_source(&args, &provider) {
            None => None,
            Some(CachedModelSource::Path(path)) => Some(CachedModelConfig::from_path(path)?),
            Some(CachedModelSource::Discover) => match CachedModelConfig::discover() {
                Ok(config) => Some(config),
                Err(error) if args.cached_model.is_some() => return Err(error),
                Err(_) => None,
            },
        };
        let model = Self::model_config(&provider, &toml, cached_model.as_ref())?;
        let datastream_frame_log = Self::datastream_frame_log(&args, &toml);
        let vastai = if provider == provider_kind::vastai() {
            Some(resolve_vastai_config(&toml.vastai, &node_image)?)
        } else {
            None
        };

        Ok(Self {
            orch_bin: artifact_root().join("target/debug/myelin-orchestrator"),
            worker_bin: default_worker_bin(),
            rpc_addr: DEFAULT_RPC_ADDR.to_owned(),
            node_image,
            provider,
            image_tag: first_non_empty([toml.image.tag.clone()]),
            cached_model,
            datastream_frame_log,
            run_id: args.run_id.unwrap_or(1),
            vastai_yes: args.vastai_yes,
            pipeline_stages,
            max_tokens,
            model,
            vastai,
            skip_rebuild: args.skip_rebuild,
            gpu_run,
            relay_mode,
            relay_url,
            endpoint_addr_mask,
        })
    }

    fn pipeline_stages(args: &ParsedArgs, toml: &ChatTomlConfig) -> Result<u32, String> {
        let pipeline_stages = args
            .pipeline_stages
            .or(toml.runtime.pipeline_stages)
            .unwrap_or(1);
        if pipeline_stages == 0 {
            return Err("--pipeline-stages must be greater than 0".to_owned());
        }
        Ok(pipeline_stages)
    }

    fn max_tokens(toml: &ChatTomlConfig) -> Result<u32, String> {
        let max_tokens = toml.runtime.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS);
        if max_tokens == 0 {
            return Err("[runtime].max_tokens must be greater than 0".to_owned());
        }
        Ok(max_tokens)
    }

    fn endpoint_addr_mask(
        args: &ParsedArgs,
        toml: &ChatTomlConfig,
    ) -> Result<EndpointAddrMask, String> {
        match first_non_empty([
            args.endpoint_addr_mask.clone(),
            toml.relay.endpoint_addr_mask.clone(),
        ]) {
            Some(mask) => EndpointAddrMask::parse(&mask),
            None => Ok(EndpointAddrMask::Full),
        }
    }

    fn relay_settings(
        args: &ParsedArgs,
        toml: &ChatTomlConfig,
        endpoint_addr_mask: EndpointAddrMask,
    ) -> Result<(Option<String>, Option<String>), String> {
        let relay_mode = first_non_empty([args.relay_mode.clone(), toml.relay.mode.clone()]);
        let mut relay_url = first_non_empty([args.relay_url.clone(), toml.relay.url.clone()]);
        if endpoint_addr_mask.requires_relay() && relay_url.is_none() {
            relay_url = first_non_empty([toml.vastai.relay_url.clone()]);
        }
        if endpoint_addr_mask.requires_relay() && relay_url.is_none() {
            return Err("relay-only endpoint address mask requires [relay].url, --relay-url, or [vastai].relay_url".to_owned());
        }
        let relay_mode = relay_mode.or_else(|| relay_url.as_ref().map(|_| "default".to_owned()));
        Ok((relay_mode, relay_url))
    }

    /// Resolve the cached-model source. The process provider defaults to
    /// best-effort discovery of `.model-cache/` so `cargo myelin-chat` runs a
    /// cached GGUF model without explicit flags. Explicit `--cached-model` is
    /// always honored (and stays strict); the default degrades gracefully to
    /// the normal download path when no cached model is present.
    fn cached_model_source(
        args: &ParsedArgs,
        provider: &ProviderKind,
    ) -> Option<CachedModelSource> {
        match &args.cached_model {
            Some(source) => Some(source.clone()),
            None if provider == &provider_kind::process() => Some(CachedModelSource::Discover),
            None => None,
        }
    }

    fn model_config(
        provider: &ProviderKind,
        toml: &ChatTomlConfig,
        cached_model: Option<&CachedModelConfig>,
    ) -> Result<ChatModelConfig, String> {
        if provider != &provider_kind::vastai() {
            return Ok(toml.model.clone());
        }
        match cached_model {
            Some(cached_model) => {
                vastai_model_config_for_cached_model(toml.model.clone(), cached_model)
            }
            None => Ok(toml.model.clone()),
        }
    }

    fn datastream_frame_log(args: &ParsedArgs, toml: &ChatTomlConfig) -> Option<PathBuf> {
        if args.dump_logs {
            return Some(
                args.dump_log_path
                    .clone()
                    .unwrap_or_else(|| PathBuf::from("myelin-chat.log")),
            );
        }
        if !toml.observability.dump_logs.unwrap_or(false) {
            return None;
        }
        Some(
            first_non_empty([toml.observability.dump_log_path.clone()])
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("myelin-chat.log")),
        )
    }

    // The orchestrator launch spec is still pending. These flags are the current adapter;
    // adjust this mapping when the approved orchestrator launch contract is finalized.
    fn orchestrator_cli_args(&self, image_ref: &str) -> Vec<String> {
        macro_rules! push_opt {
            ($args:ident, $option:expr, $flag:expr, |$value:ident| $arg:expr) => {
                if let Some($value) = $option {
                    $args.extend([$flag.to_owned(), $arg]);
                }
            };
        }

        let mut args = vec![
            "--provider".to_owned(),
            self.provider.as_str().to_owned(),
            "--image".to_owned(),
            image_ref.to_owned(),
            "--rpc-bind".to_owned(),
            self.rpc_addr.clone(),
            "--max-tokens".to_owned(),
            self.max_tokens.to_string(),
            "--run-id".to_owned(),
            self.run_id.to_string(),
            "--pipeline-stages".to_owned(),
            self.pipeline_stages.to_string(),
            "--dashboard".to_owned(),
        ];
        push_opt!(args, &self.model.id, "--model-id", |model_id| model_id
            .clone());
        push_opt!(
            args,
            &self.model.gguf_local_path,
            "--gguf-local-path",
            |path| path.clone()
        );
        push_opt!(args, &self.model.gguf_repo, "--gguf-repo", |repo| repo
            .clone());
        push_opt!(args, &self.model.gguf_file, "--gguf-file", |file| file
            .clone());
        push_opt!(
            args,
            &self.model.gguf_revision,
            "--gguf-revision",
            |revision| { revision.clone() }
        );
        push_opt!(
            args,
            &self.model.tokenizer_local_path,
            "--tokenizer-local-path",
            |path| path.clone()
        );
        push_opt!(
            args,
            self.model.max_context,
            "--max-context",
            |max_context| { max_context.to_string() }
        );
        if self.provider == provider_kind::process() {
            args.extend([
                "--worker-bin".to_owned(),
                self.worker_bin.to_string_lossy().to_string(),
            ]);
        }
        if let Some(cached_model) = &self.cached_model {
            args.extend([
                "--cached-model-host-path".to_owned(),
                cached_model.host_path.to_string_lossy().to_string(),
            ]);
        }
        push_opt!(
            args,
            &self.datastream_frame_log,
            "--datastream-frame-log",
            |path| { path.to_string_lossy().to_string() }
        );
        push_opt!(args, &self.relay_mode, "--relay-mode", |mode| mode.clone());
        push_opt!(args, &self.relay_url, "--relay-url", |url| url.clone());
        if self.endpoint_addr_mask != EndpointAddrMask::Full {
            args.extend([
                "--endpoint-addr-mask".to_owned(),
                self.endpoint_addr_mask.as_str().to_owned(),
            ]);
        }
        if let Some(vastai) = &self.vastai {
            args.extend([
                "--vastai-bootstrap-command".to_owned(),
                vastai.bootstrap_command.clone(),
                "--no-vastai-confirm-lease".to_owned(),
            ]);
            push_opt!(args, vastai.disk_gb, "--vastai-disk-gb", |disk_gb| disk_gb
                .to_string());
            push_opt!(args, &vastai.gpu_name, "--vastai-gpu-name", |gpu_name| {
                gpu_name.clone()
            });
            push_opt!(
                args,
                vastai.min_gpu_ram_mb,
                "--vastai-min-gpu-ram-mb",
                |min_gpu_ram_mb| min_gpu_ram_mb.to_string()
            );
            push_opt!(
                args,
                vastai.min_down_mbps,
                "--vastai-min-down-mbps",
                |min_down_mbps| min_down_mbps.to_string()
            );
            push_opt!(
                args,
                vastai.min_up_mbps,
                "--vastai-min-up-mbps",
                |min_up_mbps| { min_up_mbps.to_string() }
            );
            push_opt!(
                args,
                vastai.max_dph_total,
                "--vastai-max-dph-total",
                |max_dph_total| { max_dph_total.to_string() }
            );
            push_opt!(
                args,
                vastai.min_reliability,
                "--vastai-min-reliability",
                |min_reliability| min_reliability.to_string()
            );
            if let Some(require_verified) = vastai.require_verified {
                args.push(if require_verified {
                    "--vastai-require-verified".to_owned()
                } else {
                    "--no-vastai-require-verified".to_owned()
                });
            }
            for host_id in &vastai.blacklist_hosts {
                args.extend(["--vastai-blacklist-host".to_owned(), host_id.to_string()]);
            }
            push_opt!(args, &vastai.onstart, "--vastai-onstart", |onstart| onstart
                .clone());
            push_opt!(
                args,
                &vastai.ssh_identity,
                "--vastai-ssh-identity",
                |ssh_identity| { ssh_identity.clone() }
            );
        }
        args
    }

    fn orchestrator_launch_mode(&self) -> &'static str {
        if self.gpu_run {
            "in_process_actor"
        } else {
            "process_binary"
        }
    }
}

#[derive(Default, Debug)]
struct ParsedArgs {
    provider: Option<ProviderKind>,
    vastai_yes: bool,
    config_path: Option<PathBuf>,
    pipeline_stages: Option<u32>,
    dump_logs: bool,
    dump_log_path: Option<PathBuf>,
    run_id: Option<u64>,
    skip_rebuild: bool,
    cached_model: Option<CachedModelSource>,
    help: bool,
    gpu: bool,
    relay_mode: Option<String>,
    relay_url: Option<String>,
    endpoint_addr_mask: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CachedModelSource {
    Discover,
    Path(PathBuf),
}

const PROVIDER_SELECTOR_CONFLICT: &str =
    "conflicting provider selectors; use exactly one of --process, --docker, or --vastai";

impl ParsedArgs {
    fn set_provider_selector(&mut self, provider: ProviderKind) -> Result<(), String> {
        if self.provider.is_some() {
            return Err(PROVIDER_SELECTOR_CONFLICT.to_owned());
        }
        self.provider = Some(provider);
        Ok(())
    }

    fn apply_provider_arg(&mut self, arg: &str) -> Result<bool, String> {
        match arg {
            "--help" | "-h" | "help" => self.help = true,
            "--gpu" => self.gpu = true,
            "--vastai" => self.set_provider_selector(provider_kind::vastai())?,
            "--process" => self.set_provider_selector(provider_kind::process())?,
            "--docker" => self.set_provider_selector(provider_kind::docker())?,
            "--yes" | "-y" => self.vastai_yes = true,
            "--dump-logs" => self.dump_logs = true,
            "--cached-model" => self.cached_model = Some(CachedModelSource::Discover),
            "--skip-rebuild" => self.skip_rebuild = true,
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn apply_config_arg<I>(&mut self, arg: &str, args: &mut I) -> Result<bool, String>
    where
        I: Iterator<Item = String>,
    {
        match arg {
            "--config" => self.config_path = Some(PathBuf::from(next_arg(args, "--config")?)),
            "--pipeline-stages" | "--pipeline-parallel" => {
                if self.pipeline_stages.is_some() {
                    return Err("pipeline stage count was provided more than once".to_owned());
                }
                self.pipeline_stages = Some(parse_pipeline_stages_value(args, arg)?);
            }
            "--relay-mode" => self.relay_mode = Some(next_arg(args, "--relay-mode")?),
            "--relay-url" => self.relay_url = Some(next_arg(args, "--relay-url")?),
            "--endpoint-addr-mask" => {
                self.endpoint_addr_mask = Some(next_arg(args, "--endpoint-addr-mask")?)
            }
            "--run-id" => {
                let run_id: u64 = parse_next(args, "--run-id")?;
                if run_id == 0 {
                    return Err("--run-id must be greater than 0".to_owned());
                }
                self.run_id = Some(run_id);
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn apply_assignment_arg(&mut self, arg: &str) -> Result<bool, String> {
        if let Some(path) = arg.strip_prefix("--dump-logs=") {
            if path.is_empty() {
                return Err("--dump-logs path must not be empty".to_owned());
            }
            self.dump_logs = true;
            self.dump_log_path = Some(PathBuf::from(path));
            return Ok(true);
        }
        if let Some(path) = arg.strip_prefix("--cached-model=") {
            if path.is_empty() {
                return Err("--cached-model path must not be empty".to_owned());
            }
            self.cached_model = Some(CachedModelSource::Path(PathBuf::from(path)));
            return Ok(true);
        }
        Ok(false)
    }

    fn parse<I>(provided_args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = String>,
    {
        let mut parsed = Self::default();
        let mut args = provided_args.into_iter().peekable();
        while let Some(arg) = args.next() {
            if parsed.apply_provider_arg(&arg)?
                || parsed.apply_config_arg(&arg, &mut args)?
                || parsed.apply_assignment_arg(&arg)?
            {
                continue;
            }
            return Err(format!("unsupported myelin-chat argument {arg:?}"));
        }
        Ok(parsed)
    }
}

fn vastai_model_config_for_cached_model(
    mut model: ChatModelConfig,
    cached_model: &CachedModelConfig,
) -> Result<ChatModelConfig, String> {
    let file_name = cached_model
        .host_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            format!(
                "cached model path {} does not have a UTF-8 file name",
                cached_model.host_path.display()
            )
        })?;
    if file_name == DEFAULT_PIPELINE_CACHED_MODEL_FILE {
        model.id = Some(DEFAULT_PIPELINE_CACHED_MODEL_ID.to_owned());
        model.gguf_local_path = None;
        model.gguf_repo = Some(DEFAULT_PIPELINE_CACHED_MODEL_REPO.to_owned());
        model.gguf_file = Some(DEFAULT_PIPELINE_CACHED_MODEL_FILE.to_owned());
        model.gguf_revision = None;
        model.max_context = Some(DEFAULT_PIPELINE_CACHED_MODEL_MAX_CONTEXT);
        return Ok(model);
    }
    if model.gguf_file.as_deref() == Some(file_name) {
        model.gguf_local_path = None;
        return Ok(model);
    }
    Err(format!(
        "VastAI cached model {} does not match configured remote GGUF {}; use --cached-model=<matching .gguf> or configure [model].gguf_repo and [model].gguf_file for that cache",
        cached_model.host_path.display(),
        model.gguf_file.as_deref().unwrap_or("<unset>")
    ))
}

fn resolve_vastai_config(
    file: &ChatVastAiConfig,
    node_image: &str,
) -> Result<ResolvedVastAiConfig, String> {
    ResolvedVastAiConfig {
        api_key: first_non_empty([env_optional("VAST_API_KEY")]).unwrap_or_default(),
        relay_url: first_non_empty([file.relay_url.clone()]).unwrap_or_default(),
        image: node_image.to_owned(),
        bootstrap_command: first_non_empty([file.bootstrap_command.clone()]).unwrap_or_default(),
        disk_gb: file.disk_gb,
        gpu_name: first_non_empty([file.gpu_name.clone()]),
        min_gpu_ram_mb: file.min_gpu_ram_mb,
        min_down_mbps: file.min_down_mbps,
        min_up_mbps: file.min_up_mbps,
        max_dph_total: file.max_dph_total,
        min_reliability: file.min_reliability,
        require_verified: file.require_verified,
        blacklist_hosts: file.blacklist_hosts.clone(),
        onstart: first_non_empty([file.onstart.clone()]),
        ssh_identity: first_non_empty([file.ssh_identity.clone()]),
    }
    .validate()
}

fn first_non_empty<const N: usize>(values: [Option<String>; N]) -> Option<String> {
    values
        .into_iter()
        .flatten()
        .map(|value| value.trim().to_owned())
        .find(|value| !value.is_empty())
}

fn confirm_vastai_if_needed(config: &Config) -> Result<(), String> {
    if config.vastai.is_none() {
        return Ok(());
    }
    if config.vastai_yes {
        return Ok(());
    }
    if !io::stdin().is_terminal() {
        return Err("Vast.ai rental requires --yes when stdin is not a terminal".to_owned());
    }
    if prompt_vastai_approval()? {
        Ok(())
    } else {
        Err("Vast.ai rental declined".to_owned())
    }
}

fn prompt_vastai_approval() -> Result<bool, String> {
    #[cfg(test)]
    {
        let mut input = std::io::Cursor::new(Vec::<u8>::new());
        let mut output = io::sink();
        ask_vastai_approval(&mut input, &mut output)
    }
    #[cfg(not(test))]
    {
        let stdin = io::stdin();
        let mut input = stdin.lock();
        let mut output = io::stdout();
        ask_vastai_approval(&mut input, &mut output)
    }
}

fn ask_vastai_approval<R, W>(input: &mut R, output: &mut W) -> Result<bool, String>
where
    R: BufRead,
    W: Write,
{
    write!(output, "Rent 1 Vast.ai node? [y/N]: ")
        .map_err(|e| format!("write Vast.ai approval prompt: {e}"))?;
    output
        .flush()
        .map_err(|e| format!("flush Vast.ai approval prompt: {e}"))?;
    let mut line = String::new();
    input
        .read_line(&mut line)
        .map_err(|e| format!("read Vast.ai approval: {e}"))?;
    Ok(matches!(
        line.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

enum OrchHandle {
    Process(OrchChild),
    InProcess(InProcessOrch),
}

impl OrchHandle {
    fn spawn(config: &Config, image_ref: &str) -> Result<Self, String> {
        if config.gpu_run {
            InProcessOrch::spawn(config, image_ref).map(Self::InProcess)
        } else {
            OrchChild::spawn(config, image_ref).map(Self::Process)
        }
    }

    fn wait_ready(&mut self, rpc_addr: String) -> Result<String, String> {
        match self {
            Self::Process(orch) => orch.wait_ready(rpc_addr),
            Self::InProcess(orch) => orch.wait_ready(rpc_addr),
        }
    }

    fn shutdown(&mut self) {
        match self {
            Self::Process(orch) => orch.shutdown(),
            Self::InProcess(orch) => orch.shutdown(),
        }
    }
}

struct InProcessOrch {
    stop_tx: Option<mpsc::Sender<()>>,
    thread: Option<thread::JoinHandle<Result<(), String>>>,
    cleaned: bool,
}

// synchronous process-control readiness sequencing; the engine drives all background work (ENGINE_SPEC.md §2)
#[allow(clippy::disallowed_methods)]
fn wait_for_rpc_ready(
    rpc_addr: &str,
    mut check_dead: impl FnMut() -> Result<(), String>,
) -> Result<String, String> {
    loop {
        if STOP_REQUESTED.load(Ordering::SeqCst) {
            return Err("interrupted before orchestrator became ready".to_owned());
        }
        match TcpStream::connect(rpc_addr) {
            Ok(stream) => {
                let _ = stream.shutdown(Shutdown::Both);
                return Ok(rpc_addr.to_owned());
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::AddrNotAvailable
                ) => {}
            Err(error) => return Err(format!("connect prompt RPC {rpc_addr}: {error}")),
        }
        check_dead()?;
        thread::sleep(Duration::from_millis(100));
    }
}

impl InProcessOrch {
    // spawns the orchestrator process; process control, out of scope (ENGINE_SPEC.md §2)
    #[allow(clippy::disallowed_methods)]
    fn spawn(config: &Config, image_ref: &str) -> Result<Self, String> {
        let args = config.orchestrator_cli_args(image_ref);
        let (stop_tx, stop_rx) = mpsc::channel();
        let thread = thread::spawn(move || {
            crate::orchestration::app::run_with_options(args, false, Some(stop_rx))
        });
        Ok(Self {
            stop_tx: Some(stop_tx),
            thread: Some(thread),
            cleaned: false,
        })
    }

    fn wait_ready(&mut self, rpc_addr: String) -> Result<String, String> {
        wait_for_rpc_ready(&rpc_addr, || {
            if let Some(result) = self.take_finished_result() {
                let reason = match result {
                    Ok(()) => "completed successfully".to_owned(),
                    Err(error) => error,
                };
                return Err(format!(
                    "in-process orchestrator exited before prompt RPC ready: {reason}"
                ));
            }
            Ok(())
        })
    }

    // synchronous process-control readiness sequencing; the engine drives all background work (ENGINE_SPEC.md §2)
    #[allow(clippy::disallowed_methods)]
    fn shutdown(&mut self) {
        if self.cleaned {
            return;
        }
        self.cleaned = true;
        let _ = self.stop_tx.take().map(|tx| tx.send(()));
        let grace = Duration::from_millis(ORCH_SHUTDOWN_GRACE_MS);
        let poll = Duration::from_millis(ORCH_SHUTDOWN_POLL_MS);
        let started = Instant::now();
        while started.elapsed() < grace {
            if self.take_finished_result().is_some() {
                return;
            }
            thread::sleep(poll);
        }
    }

    fn take_finished_result(&mut self) -> Option<Result<(), String>> {
        if !self
            .thread
            .as_ref()
            .is_some_and(|thread| thread.is_finished())
        {
            return None;
        }
        let thread = self.thread.take()?;
        Some(match thread.join() {
            Ok(result) => result,
            Err(_) => Err("in-process orchestrator thread panicked".to_owned()),
        })
    }
}

impl Drop for InProcessOrch {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct OrchChild {
    child: Child,
    cleaned: bool,
    shutdown_grace: Duration,
}

impl OrchChild {
    fn spawn(config: &Config, image_ref: &str) -> Result<Self, String> {
        let mut command = Command::new(&config.orch_bin);
        command
            .args(config.orchestrator_cli_args(image_ref))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(vastai) = &config.vastai {
            command.env("VAST_API_KEY", &vastai.api_key);
        }
        #[cfg(all(target_os = "linux", not(test)))]
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        let child = command
            .spawn()
            .map_err(|e| format!("spawn {}: {e}", config.orch_bin.display()))?;
        Ok(Self {
            child,
            cleaned: false,
            shutdown_grace: if config.provider == provider_kind::vastai() {
                Duration::from_millis(VASTAI_ORCH_SHUTDOWN_GRACE_MS)
            } else {
                Duration::from_millis(ORCH_SHUTDOWN_GRACE_MS)
            },
        })
    }

    fn wait_ready(&mut self, rpc_addr: String) -> Result<String, String> {
        wait_for_rpc_ready(&rpc_addr, || {
            if let Some(status) = self
                .child
                .try_wait()
                .map_err(|e| format!("poll orchestrator: {e}"))?
            {
                return Err(format!(
                    "orchestrator exited before prompt RPC ready: {status}"
                ));
            }
            Ok(())
        })
    }

    // The orchestrator shutdown spec is still pending. Replace this with the approved
    // shutdown contract when it is finalized; do not add private stdin commands here.
    // synchronous process-control readiness sequencing; the engine drives all background work (ENGINE_SPEC.md §2)
    #[allow(clippy::disallowed_methods)]
    fn shutdown(&mut self) {
        if self.cleaned {
            return;
        }
        self.cleaned = true;
        if matches!(self.child.try_wait(), Ok(Some(_))) {
            return;
        }

        #[cfg(target_os = "linux")]
        let _ = signal_orch_process_group(&self.child, libc::SIGTERM);

        let grace = self.shutdown_grace;
        let poll = Duration::from_millis(ORCH_SHUTDOWN_POLL_MS);
        let started = Instant::now();
        while started.elapsed() < grace {
            match self.child.try_wait() {
                Ok(Some(_)) => {
                    let _ = self.child.wait();
                    return;
                }
                Ok(None) | Err(_) => thread::sleep(poll),
            }
        }

        #[cfg(target_os = "linux")]
        {
            if signal_orch_process_group(&self.child, libc::SIGKILL).is_err() {
                let _ = self.child.kill();
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

impl Drop for OrchChild {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(target_os = "linux")]
fn signal_orch_process_group(child: &Child, signal: libc::c_int) -> io::Result<()> {
    let result = unsafe { libc::kill(-(child.id() as libc::pid_t), signal) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn ensure_binary_with_progress(
    progress: &mut Option<&mut ChatDatastream>,
    phase: &str,
    mode: &str,
    verbose: bool,
    skip_rebuild: bool,
    bin: &Path,
    label: &str,
    cargo_args: &[&str],
) -> Result<(), String> {
    let started = Instant::now();
    emit_chat_progress(
        progress,
        CHAT_RUNTIME_CHANNEL,
        phase,
        "started",
        if verbose {
            json!({"mode": mode, "command_label": phase})
        } else {
            json!({"mode": mode})
        },
    );
    match ensure_runtime_binary(skip_rebuild, bin, label, cargo_args) {
        Ok(()) => {
            emit_chat_progress(
                progress,
                CHAT_RUNTIME_CHANNEL,
                phase,
                "ready",
                if verbose {
                    json!({"mode": mode, "command_label": phase, "elapsed_ms": started.elapsed().as_millis()})
                } else {
                    json!({"mode": mode})
                },
            );
            Ok(())
        }
        Err(error) => {
            emit_chat_progress(
                progress,
                CHAT_RUNTIME_CHANNEL,
                phase,
                "failed",
                if verbose {
                    json!({"mode": mode, "command_label": phase, "elapsed_ms": started.elapsed().as_millis(), "error": error.as_str()})
                } else {
                    json!({"mode": mode, "error": error.as_str()})
                },
            );
            Err(error)
        }
    }
}

fn prepare_runtime_with_progress<F>(
    config: &Config,
    mut prepare_node_image_fn: F,
    progress: Option<&mut ChatDatastream>,
) -> Result<String, String>
where
    F: FnMut(NodeImageRequest, Option<&mut dyn NodeImageProgressSink>) -> Result<String, String>,
{
    let mut progress = progress;
    let binary_mode = if config.skip_rebuild {
        "existing_artifact"
    } else {
        "cargo_build"
    };
    if config.gpu_run {
        emit_chat_progress(
            &mut progress,
            CHAT_RUNTIME_CHANNEL,
            "ensure_orchestrator_actor",
            "started",
            json!({"mode": config.orchestrator_launch_mode()}),
        );
        emit_chat_progress(
            &mut progress,
            CHAT_RUNTIME_CHANNEL,
            "ensure_orchestrator_actor",
            "ready",
            json!({"mode": config.orchestrator_launch_mode()}),
        );
    } else {
        ensure_binary_with_progress(
            &mut progress,
            "ensure_orch_binary",
            binary_mode,
            true,
            config.skip_rebuild,
            &config.orch_bin,
            "myelin-orchestrator",
            &[
                "build",
                "--quiet",
                "-p",
                "myelin",
                "--features",
                "dashboard",
                "--bin",
                "myelin-orchestrator",
            ],
        )?;
    }

    if config.provider == provider_kind::process() {
        ensure_binary_with_progress(
            &mut progress,
            "ensure_worker_binary",
            binary_mode,
            false,
            config.skip_rebuild,
            &config.worker_bin,
            "myelin-worker",
            &["build", "--quiet", "-p", "myelin", "--bin", "myelin-worker"],
        )?;
        emit_chat_progress(
            &mut progress,
            CHAT_RUNTIME_CHANNEL,
            "prepare_node_image",
            "skipped",
            json!({"provider": config.provider.as_str(), "command_label": "prepare_node_image", "reason": "process_provider"}),
        );
        return Ok(config.node_image.clone());
    }

    if config.skip_rebuild {
        if config.provider == provider_kind::vastai() {
            emit_chat_progress(
                &mut progress,
                CHAT_RUNTIME_CHANNEL,
                "ensure_worker_binary",
                "skipped",
                json!({"mode": binary_mode, "command_label": "ensure_worker_binary", "reason": "vastai_remote_image"}),
            );
            emit_chat_progress(
                &mut progress,
                CHAT_RUNTIME_CHANNEL,
                "prepare_node_image",
                "skipped",
                json!({"provider": config.provider.as_str(), "command_label": "prepare_node_image", "reason": "skip_rebuild"}),
            );
            return Ok(config.node_image.clone());
        }
        ensure_binary_with_progress(
            &mut progress,
            "ensure_worker_binary",
            binary_mode,
            true,
            config.skip_rebuild,
            &config.worker_bin,
            "myelin-worker",
            &["build", "--quiet", "-p", "myelin", "--bin", "myelin-worker"],
        )?;
        emit_chat_progress(
            &mut progress,
            CHAT_RUNTIME_CHANNEL,
            "prepare_node_image",
            "skipped",
            json!({"provider": config.provider.as_str(), "command_label": "prepare_node_image", "reason": "skip_rebuild"}),
        );
        return Ok(config.node_image.clone());
    }

    let prepare_node_image_started = Instant::now();
    emit_chat_progress(
        &mut progress,
        CHAT_RUNTIME_CHANNEL,
        "prepare_node_image",
        "started",
        json!({"provider": config.provider.as_str(), "command_label": "prepare_node_image", "image_tag": config.image_tag.as_deref()}),
    );
    let node_bin = default_worker_bin();
    let requires_registry_image = config.provider == provider_kind::vastai();
    let prepared = {
        let command_progress = progress
            .as_deref_mut()
            .map(|sink| sink as &mut dyn NodeImageProgressSink);
        match prepare_node_image_fn(
            NodeImageRequest {
                requested_image: config.node_image.clone(),
                base_image: BASE_NODE_IMAGE.to_owned(),
                node_bin,
                requires_registry_image,
                extra_tag: config.image_tag.clone(),
                force_refresh: false,
                enabled: true,
            },
            command_progress,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                emit_chat_progress(
                    &mut progress,
                    CHAT_RUNTIME_CHANNEL,
                    "prepare_node_image",
                    "failed",
                    json!({"provider": config.provider.as_str(), "command_label": "prepare_node_image", "elapsed_ms": prepare_node_image_started.elapsed().as_millis(), "error": error.as_str()}),
                );
                return Err(error);
            }
        }
    };
    emit_chat_progress(
        &mut progress,
        CHAT_RUNTIME_CHANNEL,
        "prepare_node_image",
        "ready",
        json!({"provider": config.provider.as_str(), "command_label": "prepare_node_image", "image_ref": &prepared, "elapsed_ms": prepare_node_image_started.elapsed().as_millis()}),
    );
    Ok(prepared)
}

fn run_chat_loop_with_input_and_progress(
    addr: &str,
    max_tokens: u32,
    input_rx: mpsc::Receiver<PromptInput>,
    progress: Option<&mut ChatDatastream>,
) -> Result<(), String> {
    let mut progress = progress;
    emit_chat_progress(
        &mut progress,
        CHAT_RUNTIME_CHANNEL,
        "prompt_rpc",
        "connecting",
        json!({"addr": addr}),
    );
    let mut stream = match TcpStream::connect(addr) {
        Ok(stream) => {
            emit_chat_progress(
                &mut progress,
                CHAT_RUNTIME_CHANNEL,
                "prompt_rpc",
                "connected",
                json!({"addr": addr}),
            );
            stream
        }
        Err(error) => {
            emit_chat_progress(
                &mut progress,
                CHAT_RUNTIME_CHANNEL,
                "prompt_rpc",
                "failed",
                json!({"addr": addr, "error": error.to_string()}),
            );
            return Err(format!("connect prompt RPC {addr}: {error}"));
        }
    };
    let reader = match stream.try_clone() {
        Ok(stream) => BufReader::new(stream),
        Err(error) => {
            emit_chat_progress(
                &mut progress,
                CHAT_RUNTIME_CHANNEL,
                "prompt_rpc_clone",
                "failed",
                json!({"error": error.to_string()}),
            );
            return Err(format!("clone prompt RPC stream: {error}"));
        }
    };
    let mut output = io::stdout();
    run_chat_session_with_output_and_progress(
        &mut stream,
        reader,
        input_rx,
        max_tokens,
        &mut output,
        progress,
    )
}

#[cfg(test)]
fn run_chat_session_with_output(
    writer: &mut impl Write,
    reader: impl BufRead,
    input_rx: mpsc::Receiver<PromptInput>,
    max_tokens: u32,
    output: &mut impl Write,
) -> Result<(), String> {
    run_chat_session_with_output_and_progress(writer, reader, input_rx, max_tokens, output, None)
}

fn emit_chat_progress(
    progress: &mut Option<&mut ChatDatastream>,
    channel: &str,
    phase: &str,
    status: &str,
    detail: Value,
) {
    if let Some(progress) = progress.as_deref_mut() {
        progress.emit(channel, phase, status, detail);
    }
}

fn run_chat_session_with_output_and_progress(
    writer: &mut impl Write,
    mut reader: impl BufRead,
    input_rx: mpsc::Receiver<PromptInput>,
    max_tokens: u32,
    output: &mut impl Write,
    progress: Option<&mut ChatDatastream>,
) -> Result<(), String> {
    let mut progress = progress;
    let mut next_request_id = 1_u64;
    let mut next_prompt_index = 1_u64;
    let prompt_exited = |progress: &mut Option<&mut ChatDatastream>, reason: &str| {
        emit_chat_progress(
            progress,
            CHAT_PROMPT_CHANNEL,
            "prompt_loop",
            "exited",
            json!({"reason": reason}),
        )
    };

    loop {
        if STOP_REQUESTED.load(Ordering::SeqCst) {
            prompt_exited(&mut progress, "stop_requested");
            return Ok(());
        }
        emit_chat_progress(
            &mut progress,
            CHAT_PROMPT_CHANNEL,
            "waiting_for_prompt",
            "started",
            json!({"next_request_id": next_request_id, "next_prompt_index": next_prompt_index}),
        );
        write!(output, "prompt:> ").map_err(|e| format!("write prompt: {e}"))?;
        output.flush().map_err(|e| format!("flush prompt: {e}"))?;
        let prompt = match input_rx.recv() {
            Ok(PromptInput::Line(line)) => line.trim_end().to_owned(),
            Ok(PromptInput::Closed) | Err(_) => {
                prompt_exited(&mut progress, "input_closed");
                return Ok(());
            }
            Ok(PromptInput::StopRequested) => {
                prompt_exited(&mut progress, "stop_requested");
                return Ok(());
            }
        };
        if prompt.trim().is_empty() {
            continue;
        }

        let request_id = next_request_id;
        next_request_id = next_request_id.wrapping_add(1).max(1);
        let prompt_index = next_prompt_index;
        next_prompt_index = next_prompt_index.wrapping_add(1).max(1);
        let prompt_hash = blake3::hash(prompt.as_bytes()).to_hex().to_string();
        emit_chat_progress(
            &mut progress,
            CHAT_PROMPT_CHANNEL,
            "prompt_submitted",
            "ready",
            json!({"request_id": request_id, "prompt_index": prompt_index, "prompt_hash": &prompt_hash, "prompt_bytes": prompt.len(), "max_tokens": max_tokens}),
        );
        write_json_line(
            writer,
            &SubmitPrompt {
                request_id,
                prompt_text: prompt,
                max_tokens,
            },
        )?;
        writeln!(output, "decoding...").map_err(|e| format!("write decoding marker: {e}"))?;
        emit_chat_progress(
            &mut progress,
            CHAT_PROMPT_CHANNEL,
            "decoding",
            "started",
            json!({"request_id": request_id, "prompt_index": prompt_index, "prompt_hash": &prompt_hash}),
        );
        let mut response_started = false;

        loop {
            if STOP_REQUESTED.load(Ordering::SeqCst) {
                prompt_exited(&mut progress, "stop_requested");
                return Ok(());
            }
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    emit_chat_progress(
                        &mut progress,
                        CHAT_PROMPT_CHANNEL,
                        "prompt_rpc",
                        "failed",
                        json!({"request_id": request_id, "error": "prompt RPC closed"}),
                    );
                    return Err("prompt RPC closed".to_owned());
                }
                Ok(_) => {}
                Err(error) => {
                    emit_chat_progress(
                        &mut progress,
                        CHAT_PROMPT_CHANNEL,
                        "prompt_rpc",
                        "failed",
                        json!({"request_id": request_id, "error": error.to_string()}),
                    );
                    return Err(format!("read prompt RPC event: {error}"));
                }
            }
            let event = match serde_json::from_str::<PromptEvent>(&line) {
                Ok(event) => event,
                Err(error) => {
                    emit_chat_progress(
                        &mut progress,
                        CHAT_PROMPT_CHANNEL,
                        "prompt_event_parse",
                        "failed",
                        json!({"request_id": request_id, "error": error.to_string()}),
                    );
                    return Err(format!("parse prompt RPC event: {error}"));
                }
            };
            let seen = event.request_id();
            if seen != request_id {
                emit_chat_progress(
                    &mut progress,
                    CHAT_PROMPT_CHANNEL,
                    "prompt_request_id",
                    "failed",
                    json!({"expected": request_id, "observed": seen}),
                );
                return Err(format!(
                    "prompt RPC protocol error: response request_id {seen} does not match active request_id {request_id}"
                ));
            }
            match event {
                PromptEvent::TextDelta { text, .. } => {
                    if !response_started {
                        write!(output, "Response: ")
                            .map_err(|e| format!("write response prefix: {e}"))?;
                        response_started = true;
                    }
                    write!(output, "{text}").map_err(|e| format!("write response text: {e}"))?;
                    output
                        .flush()
                        .map_err(|e| format!("flush response text: {e}"))?;
                    emit_chat_progress(
                        &mut progress,
                        CHAT_PROMPT_CHANNEL,
                        "response_text",
                        "observed",
                        json!({"request_id": request_id, "prompt_index": prompt_index, "prompt_hash": &prompt_hash, "text_bytes": text.len()}),
                    );
                }
                PromptEvent::Done {
                    final_text,
                    tokens_generated,
                    elapsed_ms,
                    ..
                } => {
                    if response_started {
                        writeln!(output).map_err(|e| format!("write response terminator: {e}"))?;
                    } else {
                        writeln!(output, "Response: ")
                            .map_err(|e| format!("write empty response: {e}"))?;
                    }
                    emit_chat_progress(
                        &mut progress,
                        CHAT_PROMPT_CHANNEL,
                        "request_completed",
                        "ready",
                        json!({
                            "request_id": request_id,
                            "prompt_index": prompt_index,
                            "prompt_hash": &prompt_hash,
                            "response_started": response_started,
                            "tokens_generated": tokens_generated,
                            "elapsed_ms": elapsed_ms,
                            "final_text_bytes": final_text.len(),
                        }),
                    );
                    break;
                }
                PromptEvent::Fault { error, .. } => {
                    writeln!(output, "error: {error}")
                        .map_err(|e| format!("write prompt fault: {e}"))?;
                    emit_chat_progress(
                        &mut progress,
                        CHAT_PROMPT_CHANNEL,
                        "request_faulted",
                        "ready",
                        json!({"request_id": request_id, "prompt_index": prompt_index, "prompt_hash": &prompt_hash, "error": error}),
                    );
                    break;
                }
            }
        }
    }
}

fn default_worker_bin() -> PathBuf {
    artifact_root().join("target/debug/myelin-worker")
}

fn ensure_runtime_binary(
    skip_rebuild: bool,
    path: &Path,
    label: &str,
    cargo_args: &[&str],
) -> Result<(), String> {
    if skip_rebuild {
        let metadata = fs::metadata(path)
            .map_err(|e| format!("missing required {label} artifact {}: {e}", path.display()))?;
        if !metadata.is_file() {
            return Err(format!(
                "missing required {label} artifact {}; not a file",
                path.display()
            ));
        }
        return Ok(());
    }

    let status = Command::new("cargo")
        .args(cargo_args)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|e| format!("run build {label}: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("build {label} failed with {status}"))
    }
}

// top-level OS signal handling is process control, out of scope (ENGINE_SPEC.md §2)
#[allow(clippy::disallowed_methods)]
fn install_signal_handlers() -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        let mut signals =
            Signals::new([SIGINT, SIGTERM]).map_err(|e| format!("install signal handlers: {e}"))?;
        thread::spawn(move || {
            for _ in signals.forever() {
                STOP_REQUESTED.store(true, Ordering::SeqCst);
                if let Ok(stop_tx) = PROMPT_STOP_TX.lock() {
                    if let Some(tx) = stop_tx.as_ref() {
                        let _ = tx.send(PromptInput::StopRequested);
                    }
                }
            }
        });
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct CachedModelConfig {
    host_path: PathBuf,
}

impl CachedModelConfig {
    fn from_path(path: PathBuf) -> Result<Self, String> {
        let metadata = fs::metadata(&path)
            .map_err(|e| format!("stat cached model {}: {e}", path.display()))?;
        if !is_accepted_cached_model_file(&path, &metadata) {
            return Err(format!(
                "cached model {} must be a regular .gguf file",
                path.display()
            ));
        }
        let host_path = path
            .canonicalize()
            .map_err(|e| format!("resolve cached model {}: {e}", path.display()))?;
        Ok(Self { host_path })
    }

    fn discover() -> Result<Self, String> {
        let cache_dir = PathBuf::from(REPO_MODEL_CACHE_DIR);
        let entries = fs::read_dir(&cache_dir)
            .map_err(|e| format!("discover cached model in {}: {e}", cache_dir.display()))?;
        let mut candidates = Vec::new();
        for entry in entries {
            let entry = entry
                .map_err(|e| format!("read cached model entry in {}: {e}", cache_dir.display()))?;
            let path = entry.path();
            let metadata = entry
                .metadata()
                .map_err(|e| format!("stat cached model candidate {}: {e}", path.display()))?;
            if is_accepted_cached_model_file(&path, &metadata) {
                candidates.push(path);
            }
        }
        candidates.sort_by(|left, right| left.file_name().cmp(&right.file_name()));
        let requested = candidates.into_iter().next().ok_or_else(|| {
            format!(
                "discover cached model in {}: no usable cached model files found",
                cache_dir.display()
            )
        })?;
        let host_path = requested
            .canonicalize()
            .map_err(|e| format!("resolve cached model {}: {e}", requested.display()))?;
        Ok(Self { host_path })
    }
}

fn is_accepted_cached_model_file(path: &Path, metadata: &fs::Metadata) -> bool {
    metadata.is_file()
        && path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("gguf"))
}

fn artifact_root() -> PathBuf {
    std::env::current_dir().expect("current directory is available")
}

fn provider_from_sources(
    cli_provider: Option<ProviderKind>,
    toml_provider: Option<&str>,
) -> Result<ProviderKind, String> {
    if let Some(provider) = cli_provider {
        return Ok(provider);
    }
    if let Some(value) = toml_provider {
        return match value.trim() {
            "process" => Ok(provider_kind::process()),
            "docker" => Ok(provider_kind::docker()),
            "vastai" => Ok(provider_kind::vastai()),
            other => Err(format!(
                "unsupported provider {other:?}; use process, docker, or vastai"
            )),
        };
    }
    Ok(provider_kind::process())
}

fn env_optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value after {name}"))
}

fn parse_next<T>(args: &mut impl Iterator<Item = String>, name: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = next_arg(args, name)?;
    value
        .parse::<T>()
        .map_err(|e| format!("invalid {name}={value:?}: {e}"))
}

fn env_flag(name: &str, default: bool) -> bool {
    match env_optional(name) {
        Some(value) => !matches!(
            value.to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        ),
        None => default,
    }
}

fn parse_pipeline_stages_value(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<u32, String> {
    let value: u32 = parse_next(args, name)?;
    if value == 0 {
        return Err(format!("{name} must be greater than 0"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ffi::OsString;
    use std::io::{Cursor, Read};
    use std::path::Path;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    static PROCESS_STATE_LOCK: Mutex<()> = Mutex::new(());
    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    const PROCESS_ENV_KEYS: &[&str] = &[
        "VAST_API_KEY",
        "MYELIN_PIPELINE_STAGES",
        "MYELIN_RUNTIME_CONFIG",
        "MYELIN_CHAT_GPU_RUN",
        "DEV",
    ];

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, AtomicOrdering::SeqCst);
            let path = std::env::temp_dir().join(format!(
                "myelin-chat-test-{}-{}-{}",
                std::process::id(),
                id,
                label
            ));
            if path.exists() {
                fs::remove_dir_all(&path).expect("remove stale temp dir");
            }
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct RestoreProcessState {
        saved_env: Vec<(&'static str, Option<OsString>)>,
        saved_cwd: PathBuf,
    }

    impl Drop for RestoreProcessState {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(&self.saved_cwd);
            for (key, value) in &self.saved_env {
                match value {
                    Some(value) => unsafe { std::env::set_var(key, value) },
                    None => unsafe { std::env::remove_var(key) },
                }
            }
        }
    }

    fn with_process_state<T>(
        settings: &[(&'static str, Option<&str>)],
        cwd: Option<&Path>,
        test: impl FnOnce() -> T,
    ) -> T {
        let _lock = PROCESS_STATE_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let saved_env = PROCESS_ENV_KEYS
            .iter()
            .map(|key| (*key, std::env::var_os(key)))
            .collect::<Vec<_>>();
        for key in PROCESS_ENV_KEYS {
            unsafe { std::env::remove_var(key) };
        }
        for (key, value) in settings {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
        let saved_cwd = std::env::current_dir().expect("current directory");
        if let Some(cwd) = cwd {
            std::env::set_current_dir(cwd).expect("set test current directory");
        }
        let _restore = RestoreProcessState {
            saved_env,
            saved_cwd,
        };
        test()
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn write_config(dir: &TempDir, name: &str, text: &str) -> PathBuf {
        let path = dir.path().join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create config parent");
        }
        fs::write(&path, text).expect("write config");
        path
    }

    fn channel_lines(lines: &[&str]) -> mpsc::Receiver<PromptInput> {
        let (tx, rx) = mpsc::channel();
        for line in lines {
            tx.send(PromptInput::Line((*line).to_owned()))
                .expect("send input line");
        }
        drop(tx);
        rx
    }

    fn event_reader(events: &[PromptEvent]) -> Cursor<Vec<u8>> {
        let mut bytes = Vec::new();
        for event in events {
            serde_json::to_writer(&mut bytes, event).expect("serialize prompt event");
            bytes.push(b'\n');
        }
        Cursor::new(bytes)
    }

    fn done(request_id: u64) -> PromptEvent {
        PromptEvent::Done {
            request_id,
            final_text: String::new(),
            tokens_generated: 0,
            elapsed_ms: 0,
        }
    }

    fn submitted_prompts(bytes: &[u8]) -> Vec<SubmitPrompt> {
        String::from_utf8(bytes.to_vec())
            .expect("submitted prompts are UTF-8")
            .lines()
            .map(|line| serde_json::from_str(line).expect("submitted prompt JSON"))
            .collect()
    }

    #[test]
    fn parsed_args_accepts_public_flags() {
        let parsed = ParsedArgs::parse(strings(&[
            "--gpu",
            "--docker",
            "--yes",
            "--config",
            "chat.toml",
            "--pipeline-stages",
            "3",
            "--dump-logs=logs.ndjson",
            "--cached-model",
            "--skip-rebuild",
        ]))
        .expect("public args parse");

        assert_eq!(parsed.provider, Some(provider_kind::docker()));
        assert!(parsed.vastai_yes);
        assert_eq!(parsed.config_path, Some(PathBuf::from("chat.toml")));
        assert_eq!(parsed.pipeline_stages, Some(3));
        assert!(parsed.dump_logs);
        assert_eq!(parsed.dump_log_path, Some(PathBuf::from("logs.ndjson")));
        assert_eq!(parsed.cached_model, Some(CachedModelSource::Discover));
        assert!(parsed.skip_rebuild);
        assert!(parsed.gpu);

        let help = ParsedArgs::parse(strings(&["--help"])).expect("help parses");
        assert!(help.help);
        let short_help = ParsedArgs::parse(strings(&["-h"])).expect("short help parses");
        assert!(short_help.help);

        let alias = ParsedArgs::parse(strings(&["--vastai", "--pipeline-parallel", "4"]))
            .expect("pipeline-parallel alias parses");
        assert_eq!(alias.provider, Some(provider_kind::vastai()));
        assert_eq!(alias.pipeline_stages, Some(4));
    }

    #[test]
    fn parsed_args_rejects_conflicts_and_pruned_inputs() {
        for args in [
            vec!["--process", "--docker"],
            vec!["-N", "2"],
            vec!["--pipeline-stages", "0"],
            vec!["--pipeline-stages", "many"],
            vec!["--config"],
            vec!["--dump-logs", "logs.ndjson"],
            vec!["--dump-logs="],
            vec!["--cached-model", "/tmp/model.gguf"],
            vec!["--cached-model="],
            vec!["--"],
        ] {
            assert!(
                ParsedArgs::parse(strings(&args)).is_err(),
                "args should fail: {args:?}"
            );
        }
    }

    #[test]
    fn config_resolution_uses_defaults_toml_and_cli_precedence() {
        let temp = TempDir::new("config-resolution");

        with_process_state(
            &[
                ("MYELIN_PIPELINE_STAGES", Some("9")),
                ("MYELIN_RUNTIME_CONFIG", Some("local")),
            ],
            Some(temp.path()),
            || {
                let defaults = Config::from_args(Vec::<String>::new()).expect("defaults resolve");
                assert_eq!(defaults.provider, provider_kind::process());
                assert_eq!(defaults.pipeline_stages, 1);
                assert_eq!(defaults.run_id, 1);
                assert!(defaults.datastream_frame_log.is_none());
                assert!(defaults.cached_model.is_none());
                assert!(defaults.vastai.is_none());
                assert!(!defaults.skip_rebuild);

                let config_path = write_config(
                    &temp,
                    "chat.toml",
                    r#"
[provider]
kind = "docker"

[runtime]
pipeline_stages = 2

[observability]
dump_logs = true
dump_log_path = "toml.log"

[image]
node = "docker.io/acme/node:toml"
tag = " alias "
"#,
                );
                let config_arg = config_path.to_string_lossy().into_owned();
                let config = Config::from_args(strings(&[
                    "--config",
                    config_arg.as_str(),
                    "--process",
                    "--pipeline-stages",
                    "4",
                    "--dump-logs=cli.log",
                ]))
                .expect("config resolves");

                assert_eq!(config.provider, provider_kind::process());
                assert_eq!(config.pipeline_stages, 4);
                assert_eq!(config.datastream_frame_log, Some(PathBuf::from("cli.log")));
                assert_eq!(config.node_image, "docker.io/acme/node:toml");
                assert_eq!(config.image_tag, Some("alias".to_owned()));
            },
        );
    }

    #[test]
    fn config_defaults_use_cached_model_for_process_when_present() {
        let temp = TempDir::new("cached-model-default");

        with_process_state(&[], Some(temp.path()), || {
            let cache_dir = temp.path().join(REPO_MODEL_CACHE_DIR);
            fs::create_dir_all(&cache_dir).expect("create model cache dir");
            fs::write(
                cache_dir.join(DEFAULT_PIPELINE_CACHED_MODEL_FILE),
                Vec::<u8>::new(),
            )
            .expect("seed cached model file");

            let defaults = Config::from_args(Vec::<String>::new()).expect("defaults resolve");

            assert_eq!(defaults.provider, provider_kind::process());
            let cached_model = defaults
                .cached_model
                .expect("process provider discovers a cached model by default");
            assert!(
                cached_model
                    .host_path
                    .ends_with(DEFAULT_PIPELINE_CACHED_MODEL_FILE),
                "discovered the seeded cached model"
            );
        });
    }

    #[test]
    fn config_max_tokens_drives_orchestrator_args_and_submit_prompt() {
        let temp = TempDir::new("config-max-tokens");
        let config_path = write_config(
            &temp,
            "chat.toml",
            r#"
[runtime]
max_tokens = 12
"#,
        );

        with_process_state(&[], Some(temp.path()), || {
            let config_arg = config_path.to_string_lossy().into_owned();
            let config = Config::from_args(strings(&["--config", config_arg.as_str()]))
                .expect("max_tokens config resolves");
            assert_eq!(config.max_tokens, 12);

            let args = config.orchestrator_cli_args("resolved-image");
            let max_tokens_arg = args
                .windows(2)
                .find(|pair| pair[0] == "--max-tokens")
                .map(|pair| pair[1].as_str());
            assert_eq!(max_tokens_arg, Some("12"), "{args:?}");

            let mut rpc_writer = Vec::new();
            let reader = event_reader(&[done(1)]);
            let input = channel_lines(&["hello"]);
            let mut output = Vec::new();
            run_chat_session_with_output(
                &mut rpc_writer,
                reader,
                input,
                config.max_tokens,
                &mut output,
            )
            .expect("prompt loop completes");

            assert_eq!(
                submitted_prompts(&rpc_writer),
                vec![SubmitPrompt {
                    request_id: 1,
                    prompt_text: "hello".to_owned(),
                    max_tokens: 12,
                }]
            );
        });
    }

    #[test]
    fn config_rejects_zero_max_tokens() {
        let temp = TempDir::new("config-zero-max-tokens");
        let config_path = write_config(
            &temp,
            "chat.toml",
            r#"
[runtime]
max_tokens = 0
"#,
        );

        with_process_state(&[], Some(temp.path()), || {
            let config_arg = config_path.to_string_lossy().into_owned();
            assert!(Config::from_args(strings(&["--config", config_arg.as_str()])).is_err());
        });
    }

    #[test]
    fn config_rejects_out_of_spec_sections() {
        let temp = TempDir::new("config-strict-surface");
        let config_path = write_config(
            &temp,
            "chat.toml",
            r#"
[prompt]
max_tokens = 7
"#,
        );

        with_process_state(&[], Some(temp.path()), || {
            let config_arg = config_path.to_string_lossy().into_owned();
            assert!(Config::from_args(strings(&["--config", config_arg.as_str()])).is_err());
        });
    }
    #[test]
    fn config_rejects_invalid_pipeline_provider_and_missing_images() {
        let temp = TempDir::new("config-rejections");

        with_process_state(&[], Some(temp.path()), || {
            let zero_pipeline = write_config(
                &temp,
                "zero-pipeline.toml",
                r#"
[runtime]
pipeline_stages = 0
"#,
            );
            let zero_pipeline_arg = zero_pipeline.to_string_lossy().into_owned();
            assert!(Config::from_args(strings(&["--config", zero_pipeline_arg.as_str()])).is_err());

            let invalid_provider = write_config(
                &temp,
                "invalid-provider.toml",
                r#"
[provider]
kind = "mock"
"#,
            );
            let invalid_provider_arg = invalid_provider.to_string_lossy().into_owned();
            assert!(
                Config::from_args(strings(&["--config", invalid_provider_arg.as_str()])).is_err()
            );

            assert!(Config::from_args(strings(&["--docker"])).is_err());
            assert!(Config::from_args(strings(&["--vastai"])).is_err());
        });
    }

    #[test]
    fn prompt_loop_exits_cleanly_and_ignores_empty_prompts() {
        let mut rpc_writer = Vec::new();
        let reader = event_reader(&[]);
        let input = channel_lines(&["", "   "]);
        let mut output = Vec::new();

        run_chat_session_with_output(&mut rpc_writer, reader, input, 7, &mut output)
            .expect("prompt loop exits");

        assert!(rpc_writer.is_empty());
        let output = String::from_utf8(output).expect("output is UTF-8");
        assert_eq!(output.matches("prompt:> ").count(), 3, "{output:?}");
        assert!(!output.contains("decoding..."), "{output:?}");
    }

    #[test]
    fn prompt_loop_submits_prompts_streams_text_and_increments_request_ids() {
        let mut rpc_writer = Vec::new();
        let reader = event_reader(&[
            PromptEvent::TextDelta {
                request_id: 1,
                text: "hi".to_owned(),
            },
            done(1),
            PromptEvent::TextDelta {
                request_id: 2,
                text: "bye".to_owned(),
            },
            done(2),
        ]);
        let input = channel_lines(&["hello\n", "again"]);
        let mut output = Vec::new();

        run_chat_session_with_output(&mut rpc_writer, reader, input, 7, &mut output)
            .expect("prompt loop completes");

        assert_eq!(
            submitted_prompts(&rpc_writer),
            vec![
                SubmitPrompt {
                    request_id: 1,
                    prompt_text: "hello".to_owned(),
                    max_tokens: 7,
                },
                SubmitPrompt {
                    request_id: 2,
                    prompt_text: "again".to_owned(),
                    max_tokens: 7,
                },
            ]
        );
        assert_eq!(
            String::from_utf8(output).expect("output is UTF-8"),
            "prompt:> decoding...\nResponse: hi\nprompt:> decoding...\nResponse: bye\nprompt:> "
        );
    }

    #[test]
    fn prompt_loop_rejects_mismatched_response_request_id() {
        let mut rpc_writer = Vec::new();
        let reader = event_reader(&[PromptEvent::TextDelta {
            request_id: 99,
            text: "wrong".to_owned(),
        }]);
        let input = channel_lines(&["hello"]);
        let mut output = Vec::new();

        let error = run_chat_session_with_output(&mut rpc_writer, reader, input, 7, &mut output)
            .expect_err("mismatched request id fails");

        assert!(error.contains("prompt RPC protocol error"), "{error}");
        let prompts = submitted_prompts(&rpc_writer);
        assert_eq!(prompts.len(), 1);
        assert_eq!(prompts[0].request_id, 1);
    }

    #[test]
    fn prompt_loop_fault_is_expected_prompt_result() {
        let mut rpc_writer = Vec::new();
        let reader = event_reader(&[PromptEvent::Fault {
            request_id: 1,
            error: "boom".to_owned(),
        }]);
        let input = channel_lines(&["bad"]);
        let mut output = Vec::new();

        run_chat_session_with_output(&mut rpc_writer, reader, input, 7, &mut output)
            .expect("fault is a prompt result");

        assert_eq!(submitted_prompts(&rpc_writer).len(), 1);
        let output = String::from_utf8(output).expect("output is UTF-8");
        assert!(output.contains("error: boom\n"), "{output:?}");
    }

    struct FailingBufRead;

    impl Read for FailingBufRead {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::Other, "reader failed"))
        }
    }

    impl BufRead for FailingBufRead {
        fn fill_buf(&mut self) -> io::Result<&[u8]> {
            Err(io::Error::new(io::ErrorKind::Other, "reader failed"))
        }

        fn consume(&mut self, _amt: usize) {}
    }

    #[test]
    fn prompt_loop_reports_prompt_rpc_errors() {
        let mut rpc_writer = Vec::new();
        let input = channel_lines(&["hello"]);
        let mut output = Vec::new();
        let error = run_chat_session_with_output(
            &mut rpc_writer,
            Cursor::new(Vec::new()),
            input,
            7,
            &mut output,
        )
        .expect_err("closed RPC fails");
        assert!(error.contains("prompt RPC closed"), "{error}");

        let mut rpc_writer = Vec::new();
        let input = channel_lines(&["hello"]);
        let mut output = Vec::new();
        let error = run_chat_session_with_output(
            &mut rpc_writer,
            Cursor::new(b"not-json\n".to_vec()),
            input,
            7,
            &mut output,
        )
        .expect_err("malformed event fails");
        assert!(error.contains("parse prompt RPC event"), "{error}");

        let mut rpc_writer = Vec::new();
        let input = channel_lines(&["hello"]);
        let mut output = Vec::new();
        let error =
            run_chat_session_with_output(&mut rpc_writer, FailingBufRead, input, 7, &mut output)
                .expect_err("read error fails");
        assert!(
            error.contains("read prompt RPC event: reader failed"),
            "{error}"
        );
    }
}
