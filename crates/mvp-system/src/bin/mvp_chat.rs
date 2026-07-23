use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
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

use mvp_system::benchmark_observability;
use mvp_system::config as chat_config;
use mvp_system::config::ResolvedVastAiConfig;
use mvp_system::node_image::{
    NodeImageProvider, NodeImageRequest, PreparedNodeImage, prepare_node_image,
};
use mvp_system::node_provisioning::ProviderKind;
use mvp_system::prompt_rpc::{PromptEvent, SubmitPrompt, write_json_line};

const DEFAULT_RPC_ADDR: &str = "127.0.0.1:19777";
const BASE_NODE_IMAGE: &str = "swactor-mvp-node-base:cuda12.6";
const REPO_MODEL_CACHE_DIR: &str = ".model-cache";
const DEFAULT_MAX_TOKENS: u32 = 64;
const ORCH_SHUTDOWN_GRACE_MS: u64 = 5_000;
const MVP_CHAT_GPU_RUN_ENV: &str = "MVP_CHAT_GPU_RUN";
const MVP_CHAT_USAGE: &str = "\
USAGE: cargo mvp-chat [OPTIONS]

OPTIONS:
  --gpu                         Run the local GPU path: in-process orchestrator plus DEV=CUDA worker selection
  --process | --docker | --vastai
                                Select the runtime provider
  --config <path>               Load config overlay
  --pipeline-stages <count>     Number of pipeline stages
  --cached-model[=<path>]       Use discovered or explicit cached GGUF model
  --dump-logs[=<path>]          Write datastream frame log
  --run-id <id>                 Override run id
  --skip-rebuild                Reuse existing Cargo artifacts
  --yes, -y                     Approve Vast.ai lease prompts
  --help, -h                    Print this help";
const ORCH_SHUTDOWN_POLL_MS: u64 = 50;
const CHAT_LIFECYCLE_CHANNEL: &str = "mvp.chat.lifecycle";
const CHAT_RUNTIME_CHANNEL: &str = "mvp.chat.runtime";
const CHAT_PROMPT_CHANNEL: &str = "mvp.chat.prompt";
const CHAT_COMPONENT_CHANNEL: &str = "mvp.chat.component";

#[derive(Debug)]
enum PromptInput {
    Line(String),
    Closed,
    StopRequested,
}

static STOP_REQUESTED: AtomicBool = AtomicBool::new(false);
static PROMPT_STOP_TX: Mutex<Option<mpsc::Sender<PromptInput>>> = Mutex::new(None);

pub fn run_from_args<I>(args: I) -> ExitCode
where
    I: IntoIterator<Item = String>,
{
    if let Err(error) = install_signal_handlers() {
        eprintln!("mvp-chat: {error}");
        return ExitCode::from(1);
    }
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mvp-chat: {error}");
            ExitCode::from(1)
        }
    }
}

fn print_usage() {
    println!("{MVP_CHAT_USAGE}");
}

fn is_help_request(args: &[String]) -> bool {
    args.iter()
        .any(|arg| matches!(arg.as_str(), "--help" | "-h" | "help"))
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

fn run<I>(args: I) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    let provided_args = args.into_iter().collect::<Vec<_>>();
    if is_help_request(&provided_args) {
        print_usage();
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
    confirm_vastai_if_needed(&config)?;
    progress.emit(
        CHAT_RUNTIME_CHANNEL,
        "prepare_runtime",
        "started",
        json!({"provider": config.provider.as_str()}),
    );
    let image_ref =
        match prepare_runtime_with_progress(&config, prepare_node_image, Some(&mut progress)) {
            Ok(image_ref) => {
                progress.emit(
                    CHAT_RUNTIME_CHANNEL,
                    "prepare_runtime",
                    "ready",
                    json!({"image_ref": image_ref}),
                );
                image_ref
            }
            Err(error) => {
                progress.emit(
                    CHAT_RUNTIME_CHANNEL,
                    "prepare_runtime",
                    "failed",
                    json!({"error": error}),
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
    let result = run_chat_loop_with_progress(&rpc_addr, config.max_tokens, Some(&mut progress));
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
    pipeline_stages: u32,
    max_tokens: u32,
    skip_rebuild: bool,
    gpu_run: bool,
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
        let stream = StreamId::new(NodeId::new("mvp-chat"), Lifetime(run_id));
        let endpoint = DatastreamEndpoint::with_descriptor(
            StreamDescriptor {
                stream: stream.clone(),
                label: Some("mvp chat".to_owned()),
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
        let payload = serde_json::to_vec(&json!({
            "type": "ChatProgress",
            "phase": phase,
            "status": status,
            "run_id": self.run_id,
            "benchmark": benchmark_observability::stamp("mvp-chat"),
            "detail": detail,
        }))
        .expect("serialize mvp-chat progress event");
        self.producer.submit_bytes(id, payload);
        self.flush();
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
                .push(("mvp-chat".to_owned(), stream.clone(), channel, frame));
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
        let mut archive = ChatFrameArchive::open(path)?;
        for (source, stream, channel, frame) in self.pending.drain(..) {
            archive.record(&source, &stream, &channel, &frame)?;
        }
        Ok(())
    }
}

struct ChatFrameArchive {
    file: File,
    next_seq: u64,
}

impl ChatFrameArchive {
    fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "create mvp-chat datastream frame log dir {}: {e}",
                    parent.display()
                )
            })?;
        }
        let next_seq = match File::open(path) {
            Ok(file) => BufReader::new(file).lines().count() as u64,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => {
                return Err(format!(
                    "read mvp-chat datastream frame log {}: {error}",
                    path.display()
                ));
            }
        };
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("open mvp-chat datastream frame log {}: {e}", path.display()))?;
        Ok(Self { file, next_seq })
    }

    fn record(
        &mut self,
        source: &str,
        stream: &StreamId,
        channel: &str,
        frame: &Frame,
    ) -> Result<(), String> {
        let payload = match std::str::from_utf8(&frame.payload) {
            Ok(text) => json!({"encoding": "utf8", "value": text}),
            Err(_) => json!({"encoding": "bytes", "value": frame.payload}),
        };
        let record = json!({
            "arrival_seq": self.next_seq,
            "arrival_unix_ms": benchmark_observability::unix_ms_now(),
            "source": source,
            "stream": stream.to_string(),
            "channel": channel,
            "channel_id": frame.channel.0,
            "position": frame.position.0,
            "payload": payload,
        });
        self.next_seq += 1;
        let mut line = serde_json::to_vec(&record)
            .map_err(|e| format!("serialize mvp-chat frame log: {e}"))?;
        line.push(b'\n');
        self.file
            .write_all(&line)
            .map_err(|e| format!("write mvp-chat frame log: {e}"))?;
        self.file
            .flush()
            .map_err(|e| format!("flush mvp-chat frame log: {e}"))
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
    min_reliability: Option<f64>,
    require_verified: Option<bool>,
    disk_gb: Option<u32>,
    onstart: Option<String>,
    ssh_identity: Option<String>,
}

#[derive(Clone, Debug)]
struct LoadedChatTomlConfig {
    overlay: ChatTomlConfig,
}

fn load_chat_config(path: Option<&Path>) -> Result<LoadedChatTomlConfig, String> {
    let overlay = match path {
        Some(path) => {
            let text = fs::read_to_string(path)
                .map_err(|e| format!("read config {}: {e}", path.display()))?;
            toml::from_str::<ChatTomlConfig>(&text)
                .map_err(|e| format!("parse config {}: {e}", path.display()))?
        }
        None => {
            let default = Path::new(chat_config::DEFAULT_CONFIG_PATH);
            if !default.is_file() {
                ChatTomlConfig::default()
            } else {
                let text = fs::read_to_string(default)
                    .map_err(|e| format!("read config {}: {e}", default.display()))?;
                toml::from_str::<ChatTomlConfig>(&text)
                    .map_err(|e| format!("parse config {}: {e}", default.display()))?
            }
        }
    };
    Ok(LoadedChatTomlConfig { overlay })
}

impl Config {
    fn from_args<I>(provided_args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = String>,
    {
        let args = ParsedArgs::parse(provided_args)?;
        let loaded = load_chat_config(args.config_path.as_deref())?;
        let toml = loaded.overlay;
        let provider = provider_from_sources(args.provider, toml.provider.kind.as_deref())?;
        let node_image = first_non_empty([toml.image.node.clone()]).unwrap_or_default();
        if provider != ProviderKind::Process && node_image.is_empty() {
            return Err("node image is required for docker or vastai provider".to_owned());
        }
        let pipeline_stages = args
            .pipeline_stages
            .or(toml.runtime.pipeline_stages)
            .unwrap_or(1);
        if pipeline_stages == 0 {
            return Err("--pipeline-stages must be greater than 0".to_owned());
        }
        let max_tokens = toml.runtime.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS);
        if max_tokens == 0 {
            return Err("[runtime].max_tokens must be greater than 0".to_owned());
        }
        let gpu_run = args.gpu || env_flag(MVP_CHAT_GPU_RUN_ENV, false);
        let cached_model_source = match args.cached_model {
            Some(source) => Some(source),
            None if gpu_run && provider == ProviderKind::Process => {
                Some(CachedModelSource::Discover)
            }
            None => None,
        };
        let cached_model = cached_model_source
            .map(CachedModelConfig::from_source)
            .transpose()?;
        let datastream_frame_log = if args.dump_logs {
            Some(
                args.dump_log_path
                    .unwrap_or_else(|| PathBuf::from("mvp-chat.log")),
            )
        } else if toml.observability.dump_logs.unwrap_or(false) {
            Some(
                first_non_empty([toml.observability.dump_log_path.clone()])
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("mvp-chat.log")),
            )
        } else {
            None
        };
        let vastai = if provider == ProviderKind::VastAi {
            Some(resolve_vastai_config(&toml.vastai, &node_image)?)
        } else {
            None
        };

        Ok(Self {
            orch_bin: default_orch_bin()?,
            worker_bin: node_bin_for_current_profile()?,
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
            vastai,
            skip_rebuild: args.skip_rebuild,
            gpu_run,
        })
    }

    // The orchestrator launch spec is still pending. These flags are the current adapter;
    // adjust this mapping when the approved orchestrator launch contract is finalized.
    fn orchestrator_cli_args(&self, image_ref: &str) -> Vec<String> {
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
            "--no-dashboard".to_owned(),
        ];
        if self.provider == ProviderKind::Process {
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
        if let Some(path) = &self.datastream_frame_log {
            args.extend([
                "--datastream-frame-log".to_owned(),
                path.to_string_lossy().to_string(),
            ]);
        }
        if let Some(vastai) = &self.vastai {
            args.extend([
                "--vastai-api-key".to_owned(),
                vastai.api_key.clone(),
                "--vastai-bootstrap-command".to_owned(),
                vastai.bootstrap_command.clone(),
                "--no-vastai-confirm-lease".to_owned(),
            ]);
            if let Some(disk_gb) = vastai.disk_gb {
                args.extend(["--vastai-disk-gb".to_owned(), disk_gb.to_string()]);
            }
            if let Some(gpu_name) = &vastai.gpu_name {
                args.extend(["--vastai-gpu-name".to_owned(), gpu_name.clone()]);
            }
            if let Some(min_gpu_ram_mb) = vastai.min_gpu_ram_mb {
                args.extend([
                    "--vastai-min-gpu-ram-mb".to_owned(),
                    min_gpu_ram_mb.to_string(),
                ]);
            }
            if let Some(min_down_mbps) = vastai.min_down_mbps {
                args.extend([
                    "--vastai-min-down-mbps".to_owned(),
                    min_down_mbps.to_string(),
                ]);
            }
            if let Some(min_up_mbps) = vastai.min_up_mbps {
                args.extend(["--vastai-min-up-mbps".to_owned(), min_up_mbps.to_string()]);
            }
            if let Some(min_reliability) = vastai.min_reliability {
                args.extend([
                    "--vastai-min-reliability".to_owned(),
                    min_reliability.to_string(),
                ]);
            }
            if let Some(require_verified) = vastai.require_verified {
                args.push(if require_verified {
                    "--vastai-require-verified".to_owned()
                } else {
                    "--no-vastai-require-verified".to_owned()
                });
            }
            if let Some(onstart) = &vastai.onstart {
                args.extend(["--vastai-onstart".to_owned(), onstart.clone()]);
            }
            if let Some(ssh_identity) = &vastai.ssh_identity {
                args.extend(["--vastai-ssh-identity".to_owned(), ssh_identity.clone()]);
            }
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

    fn parse<I>(provided_args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = String>,
    {
        let mut parsed = Self::default();
        let mut args = provided_args.into_iter().peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" | "help" => parsed.help = true,
                "--gpu" => parsed.gpu = true,
                "--vastai" => parsed.set_provider_selector(ProviderKind::VastAi)?,
                "--process" => parsed.set_provider_selector(ProviderKind::Process)?,
                "--docker" => parsed.set_provider_selector(ProviderKind::Docker)?,
                "--yes" | "-y" => parsed.vastai_yes = true,
                "--config" => {
                    parsed.config_path = Some(PathBuf::from(next_arg(&mut args, "--config")?))
                }
                "--pipeline-stages" => {
                    parsed.pipeline_stages =
                        Some(parse_pipeline_stages_value(&mut args, arg.as_str())?)
                }
                "--run-id" => {
                    let run_id: u64 = parse_next(&mut args, "--run-id")?;
                    if run_id == 0 {
                        return Err("--run-id must be greater than 0".to_owned());
                    }
                    parsed.run_id = Some(run_id);
                }
                "--dump-logs" => {
                    parsed.dump_logs = true;
                }
                value if value.starts_with("--dump-logs=") => {
                    let path = value.strip_prefix("--dump-logs=").expect("prefix checked");
                    if path.is_empty() {
                        return Err("--dump-logs path must not be empty".to_owned());
                    }
                    parsed.dump_logs = true;
                    parsed.dump_log_path = Some(PathBuf::from(path));
                }
                "--cached-model" => {
                    parsed.cached_model = Some(CachedModelSource::Discover);
                }
                value if value.starts_with("--cached-model=") => {
                    let path = value
                        .strip_prefix("--cached-model=")
                        .expect("prefix checked");
                    if path.is_empty() {
                        return Err("--cached-model path must not be empty".to_owned());
                    }
                    parsed.cached_model = Some(CachedModelSource::Path(PathBuf::from(path)));
                }
                "--skip-rebuild" => parsed.skip_rebuild = true,
                other => return Err(format!("unsupported mvp-chat argument {other:?}")),
            }
        }
        Ok(parsed)
    }
}

fn resolve_vastai_config(
    file: &ChatVastAiConfig,
    node_image: &str,
) -> Result<ResolvedVastAiConfig, String> {
    ResolvedVastAiConfig {
        api_key: first_non_empty([env_optional("VASTAI_API_KEY")]).unwrap_or_default(),
        relay_url: first_non_empty([file.relay_url.clone()]).unwrap_or_default(),
        image: node_image.to_owned(),
        bootstrap_command: first_non_empty([file.bootstrap_command.clone()]).unwrap_or_default(),
        disk_gb: file.disk_gb,
        gpu_name: first_non_empty([file.gpu_name.clone()]),
        min_gpu_ram_mb: file.min_gpu_ram_mb,
        min_down_mbps: file.min_down_mbps,
        min_up_mbps: file.min_up_mbps,
        min_reliability: file.min_reliability,
        require_verified: file.require_verified,
        onstart: first_non_empty([file.onstart.clone()]),
        ssh_identity: first_non_empty([file.ssh_identity.clone()]),
    }
    .validate()
}

fn first_non_empty<const N: usize>(values: [Option<String>; N]) -> Option<String> {
    values.into_iter().find_map(chat_config::normalize_optional)
}

fn confirm_vastai_if_needed(config: &Config) -> Result<(), String> {
    let mut approval = StdinVastAiApproval;
    confirm_vastai_if_needed_with_approval(config, &mut approval)
}

trait VastAiApproval {
    fn stdin_is_terminal(&self) -> bool;
    fn ask(&mut self) -> Result<bool, String>;
}

struct StdinVastAiApproval;

impl VastAiApproval for StdinVastAiApproval {
    fn stdin_is_terminal(&self) -> bool {
        io::stdin().is_terminal()
    }

    fn ask(&mut self) -> Result<bool, String> {
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
}

fn confirm_vastai_if_needed_with_approval<A>(
    config: &Config,
    approval: &mut A,
) -> Result<(), String>
where
    A: VastAiApproval,
{
    if config.vastai.is_none() {
        return Ok(());
    }
    if config.vastai_yes {
        return Ok(());
    }
    if !approval.stdin_is_terminal() {
        return Err("Vast.ai rental requires --yes when stdin is not a terminal".to_owned());
    }
    if approval.ask()? {
        Ok(())
    } else {
        Err("Vast.ai rental declined".to_owned())
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
    Ok(parse_approval(&line))
}

fn parse_approval(input: &str) -> bool {
    matches!(input.trim().to_ascii_lowercase().as_str(), "y" | "yes")
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

impl InProcessOrch {
    fn spawn(config: &Config, image_ref: &str) -> Result<Self, String> {
        let args = config.orchestrator_cli_args(image_ref);
        let (stop_tx, stop_rx) = mpsc::channel();
        let thread = thread::spawn(move || {
            mvp_system::orchestrator_app::run_in_process_from_args(args, stop_rx)
        });
        Ok(Self {
            stop_tx: Some(stop_tx),
            thread: Some(thread),
            cleaned: false,
        })
    }

    fn wait_ready(&mut self, rpc_addr: String) -> Result<String, String> {
        loop {
            if STOP_REQUESTED.load(Ordering::SeqCst) {
                return Err("interrupted before orchestrator became ready".to_owned());
            }
            match TcpStream::connect(&rpc_addr) {
                Ok(stream) => {
                    let _ = stream.shutdown(Shutdown::Both);
                    return Ok(rpc_addr);
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
            if let Some(result) = self.take_finished_result() {
                return Err(format!(
                    "in-process orchestrator exited before prompt RPC ready: {}",
                    render_orch_thread_result(result)
                ));
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

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

fn render_orch_thread_result(result: Result<(), String>) -> String {
    match result {
        Ok(()) => "completed successfully".to_owned(),
        Err(error) => error,
    }
}

struct OrchChild {
    child: Child,
    cleaned: bool,
}

impl OrchChild {
    fn spawn(config: &Config, image_ref: &str) -> Result<Self, String> {
        let mut command = Command::new(&config.orch_bin);
        command
            .args(config.orchestrator_cli_args(image_ref))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
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
        })
    }

    fn wait_ready(&mut self, rpc_addr: String) -> Result<String, String> {
        loop {
            if STOP_REQUESTED.load(Ordering::SeqCst) {
                return Err("interrupted before orchestrator became ready".to_owned());
            }
            match TcpStream::connect(&rpc_addr) {
                Ok(stream) => {
                    let _ = stream.shutdown(Shutdown::Both);
                    return Ok(rpc_addr);
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
            if let Some(status) = self
                .child
                .try_wait()
                .map_err(|e| format!("poll orchestrator: {e}"))?
            {
                return Err(format!(
                    "orchestrator exited before prompt RPC ready: {status}"
                ));
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    // The orchestrator shutdown spec is still pending. Replace this with the approved
    // shutdown contract when it is finalized; do not add private stdin commands here.
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

        let grace = Duration::from_millis(ORCH_SHUTDOWN_GRACE_MS);
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

type PrepareNodeImageFn = fn(NodeImageRequest) -> Result<PreparedNodeImage, String>;

#[allow(dead_code)]
fn prepare_runtime(config: &Config) -> Result<String, String> {
    prepare_runtime_with(config, prepare_node_image)
}

#[allow(dead_code)]
fn prepare_runtime_with(
    config: &Config,
    prepare_node_image_fn: PrepareNodeImageFn,
) -> Result<String, String> {
    prepare_runtime_with_progress(config, prepare_node_image_fn, None)
}

fn prepare_runtime_with_progress(
    config: &Config,
    prepare_node_image_fn: PrepareNodeImageFn,
    progress: Option<&mut ChatDatastream>,
) -> Result<String, String> {
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
        emit_chat_progress(
            &mut progress,
            CHAT_RUNTIME_CHANNEL,
            "ensure_orch_binary",
            "started",
            json!({"mode": binary_mode}),
        );
        match ensure_orch_binary(config) {
            Ok(()) => emit_chat_progress(
                &mut progress,
                CHAT_RUNTIME_CHANNEL,
                "ensure_orch_binary",
                "ready",
                json!({"mode": binary_mode}),
            ),
            Err(error) => {
                emit_chat_progress(
                    &mut progress,
                    CHAT_RUNTIME_CHANNEL,
                    "ensure_orch_binary",
                    "failed",
                    json!({"mode": binary_mode, "error": error.as_str()}),
                );
                return Err(error);
            }
        }
    }

    if config.provider == ProviderKind::Process {
        emit_chat_progress(
            &mut progress,
            CHAT_RUNTIME_CHANNEL,
            "ensure_worker_binary",
            "started",
            json!({"mode": binary_mode}),
        );
        match ensure_worker_binary(config) {
            Ok(()) => emit_chat_progress(
                &mut progress,
                CHAT_RUNTIME_CHANNEL,
                "ensure_worker_binary",
                "ready",
                json!({"mode": binary_mode}),
            ),
            Err(error) => {
                emit_chat_progress(
                    &mut progress,
                    CHAT_RUNTIME_CHANNEL,
                    "ensure_worker_binary",
                    "failed",
                    json!({"mode": binary_mode, "error": error.as_str()}),
                );
                return Err(error);
            }
        }
        emit_chat_progress(
            &mut progress,
            CHAT_RUNTIME_CHANNEL,
            "prepare_node_image",
            "skipped",
            json!({"provider": config.provider.as_str(), "reason": "process_provider"}),
        );
        return Ok(config.node_image.clone());
    }

    if config.skip_rebuild {
        emit_chat_progress(
            &mut progress,
            CHAT_RUNTIME_CHANNEL,
            "ensure_worker_binary",
            "started",
            json!({"mode": binary_mode}),
        );
        match ensure_worker_binary(config) {
            Ok(()) => emit_chat_progress(
                &mut progress,
                CHAT_RUNTIME_CHANNEL,
                "ensure_worker_binary",
                "ready",
                json!({"mode": binary_mode}),
            ),
            Err(error) => {
                emit_chat_progress(
                    &mut progress,
                    CHAT_RUNTIME_CHANNEL,
                    "ensure_worker_binary",
                    "failed",
                    json!({"mode": binary_mode, "error": error.as_str()}),
                );
                return Err(error);
            }
        }
        emit_chat_progress(
            &mut progress,
            CHAT_RUNTIME_CHANNEL,
            "prepare_node_image",
            "skipped",
            json!({"provider": config.provider.as_str(), "reason": "skip_rebuild"}),
        );
        return Ok(config.node_image.clone());
    }

    emit_chat_progress(
        &mut progress,
        CHAT_RUNTIME_CHANNEL,
        "prepare_node_image",
        "started",
        json!({"provider": config.provider.as_str()}),
    );
    let node_bin = match node_bin_for_current_profile() {
        Ok(path) => path,
        Err(error) => {
            emit_chat_progress(
                &mut progress,
                CHAT_RUNTIME_CHANNEL,
                "prepare_node_image",
                "failed",
                json!({"provider": config.provider.as_str(), "error": error.as_str()}),
            );
            return Err(error);
        }
    };
    let provider = match node_image_provider(config.provider) {
        Ok(provider) => provider,
        Err(error) => {
            emit_chat_progress(
                &mut progress,
                CHAT_RUNTIME_CHANNEL,
                "prepare_node_image",
                "failed",
                json!({"provider": config.provider.as_str(), "error": error.as_str()}),
            );
            return Err(error);
        }
    };
    let prepared = match prepare_node_image_fn(NodeImageRequest {
        requested_image: config.node_image.clone(),
        base_image: BASE_NODE_IMAGE.to_owned(),
        node_bin,
        provider,
        extra_tag: config.image_tag.clone(),
        push: false,
        force_refresh: false,
        enabled: true,
    }) {
        Ok(prepared) => prepared,
        Err(error) => {
            emit_chat_progress(
                &mut progress,
                CHAT_RUNTIME_CHANNEL,
                "prepare_node_image",
                "failed",
                json!({"provider": config.provider.as_str(), "error": error.as_str()}),
            );
            return Err(error);
        }
    };
    emit_chat_progress(
        &mut progress,
        CHAT_RUNTIME_CHANNEL,
        "prepare_node_image",
        "ready",
        json!({"provider": config.provider.as_str(), "image_ref": prepared.image_ref}),
    );
    Ok(prepared.image_ref)
}

fn stdin_prompt_events() -> mpsc::Receiver<PromptInput> {
    let (tx, rx) = mpsc::channel();
    if STOP_REQUESTED.load(Ordering::SeqCst) {
        let _ = tx.send(PromptInput::StopRequested);
    }
    if let Ok(mut stop_tx) = PROMPT_STOP_TX.lock() {
        *stop_tx = Some(tx.clone());
    }
    thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            match line {
                Ok(line) => {
                    if tx.send(PromptInput::Line(line)).is_err() {
                        return;
                    }
                }
                Err(_) => {
                    let _ = tx.send(PromptInput::Closed);
                    return;
                }
            }
        }
        let _ = tx.send(PromptInput::Closed);
    });
    rx
}

fn run_chat_loop_with_progress(
    addr: &str,
    max_tokens: u32,
    progress: Option<&mut ChatDatastream>,
) -> Result<(), String> {
    run_chat_loop_with_input_and_progress(addr, max_tokens, stdin_prompt_events(), progress)
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
    run_chat_session_with_progress(&mut stream, reader, input_rx, max_tokens, progress)
}

#[cfg(test)]
fn run_chat_session_with_output<R, W, O>(
    writer: &mut W,
    reader: R,
    input_rx: mpsc::Receiver<PromptInput>,
    max_tokens: u32,
    output: &mut O,
) -> Result<(), String>
where
    R: BufRead,
    W: Write,
    O: Write,
{
    run_chat_session_with_output_and_progress(writer, reader, input_rx, max_tokens, output, None)
}

fn run_chat_session_with_progress<R, W>(
    writer: &mut W,
    reader: R,
    input_rx: mpsc::Receiver<PromptInput>,
    max_tokens: u32,
    progress: Option<&mut ChatDatastream>,
) -> Result<(), String>
where
    R: BufRead,
    W: Write,
{
    let mut output = io::stdout();
    run_chat_session_with_output_and_progress(
        writer,
        reader,
        input_rx,
        max_tokens,
        &mut output,
        progress,
    )
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

fn run_chat_session_with_output_and_progress<R, W, O>(
    writer: &mut W,
    mut reader: R,
    input_rx: mpsc::Receiver<PromptInput>,
    max_tokens: u32,
    output: &mut O,
    progress: Option<&mut ChatDatastream>,
) -> Result<(), String>
where
    R: BufRead,
    W: Write,
    O: Write,
{
    let mut progress = progress;
    let mut next_request_id = 1_u64;

    loop {
        if STOP_REQUESTED.load(Ordering::SeqCst) {
            emit_chat_progress(
                &mut progress,
                CHAT_PROMPT_CHANNEL,
                "prompt_loop",
                "exited",
                json!({"reason": "stop_requested"}),
            );
            return Ok(());
        }
        emit_chat_progress(
            &mut progress,
            CHAT_PROMPT_CHANNEL,
            "waiting_for_prompt",
            "started",
            json!({"next_request_id": next_request_id}),
        );
        write!(output, "prompt:> ").map_err(|e| format!("write prompt: {e}"))?;
        output.flush().map_err(|e| format!("flush prompt: {e}"))?;
        let prompt = match input_rx.recv() {
            Ok(PromptInput::Line(line)) => line.trim_end().to_owned(),
            Ok(PromptInput::Closed) | Err(_) => {
                emit_chat_progress(
                    &mut progress,
                    CHAT_PROMPT_CHANNEL,
                    "prompt_loop",
                    "exited",
                    json!({"reason": "input_closed"}),
                );
                return Ok(());
            }
            Ok(PromptInput::StopRequested) => {
                emit_chat_progress(
                    &mut progress,
                    CHAT_PROMPT_CHANNEL,
                    "prompt_loop",
                    "exited",
                    json!({"reason": "stop_requested"}),
                );
                return Ok(());
            }
        };
        if prompt.trim().is_empty() {
            continue;
        }

        let request_id = next_request_id;
        next_request_id = next_request_id.wrapping_add(1).max(1);
        emit_chat_progress(
            &mut progress,
            CHAT_PROMPT_CHANNEL,
            "prompt_submitted",
            "ready",
            json!({"request_id": request_id, "prompt_bytes": prompt.len(), "max_tokens": max_tokens}),
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
            json!({"request_id": request_id}),
        );
        let mut response_started = false;

        loop {
            if STOP_REQUESTED.load(Ordering::SeqCst) {
                emit_chat_progress(
                    &mut progress,
                    CHAT_PROMPT_CHANNEL,
                    "prompt_loop",
                    "exited",
                    json!({"reason": "stop_requested"}),
                );
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
                        json!({"request_id": request_id, "text_bytes": text.len()}),
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
                        json!({"request_id": request_id, "error": error}),
                    );
                    break;
                }
            }
        }
    }
}

fn default_orch_bin() -> Result<PathBuf, String> {
    Ok(artifact_root().join("target/debug/mvp-orchestrator"))
}

fn node_bin_for_current_profile() -> Result<PathBuf, String> {
    Ok(artifact_root().join("target/debug/mvp-worker-node"))
}

fn cargo_command() -> &'static str {
    "cargo"
}

fn ensure_orch_binary(config: &Config) -> Result<(), String> {
    if config.skip_rebuild {
        return ensure_existing_artifact(&config.orch_bin, "mvp-orchestrator");
    }
    run_status(
        cargo_command(),
        &[
            "build",
            "--quiet",
            "-p",
            "mvp-system",
            "--bin",
            "mvp-orchestrator",
        ],
        "build mvp-orchestrator",
    )
}

fn ensure_worker_binary(config: &Config) -> Result<(), String> {
    if config.skip_rebuild {
        return ensure_existing_artifact(&config.worker_bin, "mvp-worker-node");
    }
    run_status(
        cargo_command(),
        &[
            "build",
            "--quiet",
            "-p",
            "mvp-system",
            "--bin",
            "mvp-worker-node",
        ],
        "build mvp-worker-node",
    )
}

fn ensure_existing_artifact(path: &PathBuf, label: &str) -> Result<(), String> {
    let metadata = fs::metadata(path)
        .map_err(|e| format!("missing required {label} artifact {}: {e}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!(
            "missing required {label} artifact {}; not a file",
            path.display()
        ));
    }
    Ok(())
}

fn run_status(program: &str, args: &[&str], label: &str) -> Result<(), String> {
    let status = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|e| format!("run {label}: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{label} failed with {status}"))
    }
}

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
    fn from_source(source: CachedModelSource) -> Result<Self, String> {
        match source {
            CachedModelSource::Discover => Self::discover(),
            CachedModelSource::Path(path) => Self::from_path(path),
        }
    }

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
            "process" => Ok(ProviderKind::Process),
            "docker" => Ok(ProviderKind::Docker),
            "vastai" => Ok(ProviderKind::VastAi),
            other => Err(format!(
                "unsupported provider {other:?}; use process, docker, or vastai"
            )),
        };
    }
    Ok(ProviderKind::Process)
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

fn env_optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn node_image_provider(provider: ProviderKind) -> Result<NodeImageProvider, String> {
    match provider {
        ProviderKind::Docker => Ok(NodeImageProvider::Docker),
        ProviderKind::VastAi => Ok(NodeImageProvider::VastAi),
        ProviderKind::Process => Err("process provider does not use node images".to_owned()),
        ProviderKind::Mock => Err("mvp-chat does not support mock provider".to_owned()),
    }
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

fn main() -> ExitCode {
    run_from_args(std::env::args().skip(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ffi::{OsStr, OsString};
    use std::io::{Cursor, Read};
    #[cfg(target_os = "linux")]
    use std::os::unix::process::CommandExt;
    use std::path::Path;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    static PROCESS_STATE_LOCK: Mutex<()> = Mutex::new(());
    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    const PROCESS_ENV_KEYS: &[&str] = &[
        "VASTAI_API_KEY",
        "MVP_PIPELINE_STAGES",
        "MVP_RUNTIME_CONFIG",
        "MVP_CHAT_GPU_RUN",
        "DEV",
    ];

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, AtomicOrdering::SeqCst);
            let path = std::env::temp_dir().join(format!(
                "mvp-chat-test-{}-{}-{}",
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

    fn base_config(provider: ProviderKind) -> Config {
        Config {
            orch_bin: PathBuf::from("/tmp/mvp-orchestrator"),
            worker_bin: PathBuf::from("/tmp/mvp-worker-node"),
            rpc_addr: DEFAULT_RPC_ADDR.to_owned(),
            node_image: "docker.io/acme/node:latest".to_owned(),
            provider,
            image_tag: None,
            cached_model: None,
            datastream_frame_log: None,
            run_id: 1,
            vastai_yes: false,
            vastai: None,
            pipeline_stages: 1,
            max_tokens: DEFAULT_MAX_TOKENS,
            skip_rebuild: true,
            gpu_run: false,
        }
    }

    fn valid_vastai() -> ResolvedVastAiConfig {
        ResolvedVastAiConfig {
            api_key: "secret".to_owned(),
            relay_url: "https://relay.example".to_owned(),
            image: "docker.io/acme/node:latest".to_owned(),
            bootstrap_command: "boot".to_owned(),
            disk_gb: None,
            gpu_name: None,
            min_gpu_ram_mb: None,
            min_down_mbps: None,
            min_up_mbps: None,
            min_reliability: None,
            require_verified: None,
            onstart: None,
            ssh_identity: None,
        }
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
    fn benchmark_observability_chat_progress_records_include_run_id_and_stamp() {
        let temp = TempDir::new("chat-progress-archive");
        let archive_path = temp.path().join("frames.ndjson");
        let mut progress = ChatDatastream::new(77, Some(archive_path.clone()))
            .expect("chat datastream constructs");

        progress.emit(
            CHAT_RUNTIME_CHANNEL,
            "unit_phase",
            "ready",
            serde_json::json!({"ok": true}),
        );
        progress.archive_pending().expect("archive pending frames");

        let archive = fs::read_to_string(&archive_path).expect("read archive");
        let line = archive.lines().next().expect("archive line");
        let outer: serde_json::Value = serde_json::from_str(line).expect("outer archive JSON");
        let inner_text = outer
            .get("payload")
            .and_then(|payload| payload.get("value"))
            .and_then(serde_json::Value::as_str)
            .expect("inner event text");
        let inner: serde_json::Value = serde_json::from_str(inner_text).expect("inner event JSON");

        assert_eq!(
            inner.get("type").and_then(serde_json::Value::as_str),
            Some("ChatProgress")
        );
        assert_eq!(
            inner.get("run_id").and_then(serde_json::Value::as_u64),
            Some(77)
        );
        assert_eq!(
            inner
                .get("benchmark")
                .and_then(|benchmark| benchmark.get("schema"))
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );
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

        assert_eq!(parsed.provider, Some(ProviderKind::Docker));
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
    }

    #[test]
    fn config_gpu_flag_selects_in_process_gpu_run() {
        let temp = TempDir::new("gpu-flag-config");
        let cache = temp.path().join(REPO_MODEL_CACHE_DIR);
        fs::create_dir_all(&cache).expect("create model cache");
        let cached_path = cache.join("default.gguf");
        fs::write(&cached_path, b"cached model").expect("write cached model");
        with_process_state(&[], Some(temp.path()), || {
            let config = Config::from_args(strings(&["--gpu", "--skip-rebuild"]))
                .expect("gpu config resolves");
            assert!(config.gpu_run);
            assert_eq!(config.orchestrator_launch_mode(), "in_process_actor");
            assert_eq!(
                config
                    .cached_model
                    .as_ref()
                    .map(|model| model.host_path.clone()),
                Some(cached_path.canonicalize().expect("canonical cached model"))
            );
        });
    }

    #[test]
    fn benchmark_observability_parsed_args_accepts_run_id_and_forwards_to_orchestrator() {
        let parsed = ParsedArgs::parse(strings(&["--run-id", "123"])).expect("run id parses");
        assert_eq!(parsed.run_id, Some(123));

        let temp = TempDir::new("run-id-config");
        with_process_state(&[], Some(temp.path()), || {
            let config =
                Config::from_args(strings(&["--run-id", "123"])).expect("config resolves run id");
            assert_eq!(config.run_id, 123);
            let args = config.orchestrator_cli_args("resolved-image");
            let run_id_arg = args
                .windows(2)
                .find(|pair| pair[0] == "--run-id")
                .map(|pair| pair[1].as_str());
            assert_eq!(run_id_arg, Some("123"), "{args:?}");
        });
    }

    #[test]
    fn benchmark_observability_parsed_args_rejects_zero_run_id() {
        let error =
            ParsedArgs::parse(strings(&["--run-id", "0"])).expect_err("zero run id should fail");
        assert_eq!(error, "--run-id must be greater than 0");
    }

    #[test]
    fn parsed_args_accepts_cached_model_path() {
        let parsed = ParsedArgs::parse(strings(&["--cached-model=/tmp/model.gguf"]))
            .expect("cached model path parses");

        assert_eq!(
            parsed.cached_model,
            Some(CachedModelSource::Path(PathBuf::from("/tmp/model.gguf")))
        );
    }

    #[test]
    fn parsed_args_accepts_cached_model_equals_path_with_dash_prefix() {
        let parsed = ParsedArgs::parse(strings(&["--cached-model=-model.gguf"]))
            .expect("cached model path parses");

        assert_eq!(
            parsed.cached_model,
            Some(CachedModelSource::Path(PathBuf::from("-model.gguf")))
        );
    }

    #[test]
    fn parsed_args_accepts_dump_logs_equals_path_with_dash_prefix() {
        let parsed = ParsedArgs::parse(strings(&["--dump-logs=-logs.ndjson"]))
            .expect("dump log path parses");

        assert!(parsed.dump_logs);
        assert_eq!(parsed.dump_log_path, Some(PathBuf::from("-logs.ndjson")));
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
                ("MVP_PIPELINE_STAGES", Some("9")),
                ("MVP_RUNTIME_CONFIG", Some("local")),
            ],
            Some(temp.path()),
            || {
                let defaults = Config::from_args(Vec::<String>::new()).expect("defaults resolve");
                assert_eq!(defaults.provider, ProviderKind::Process);
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

                assert_eq!(config.provider, ProviderKind::Process);
                assert_eq!(config.pipeline_stages, 4);
                assert_eq!(config.datastream_frame_log, Some(PathBuf::from("cli.log")));
                assert_eq!(config.node_image, "docker.io/acme/node:toml");
                assert_eq!(config.image_tag, Some("alias".to_owned()));
            },
        );
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
    fn vastai_config_requires_secret_relay_bootstrap_and_remote_image() {
        let missing_secret = TempDir::new("vastai-missing-secret");
        let missing_secret_config = write_config(
            &missing_secret,
            "chat.toml",
            r#"
[provider]
kind = "vastai"

[image]
node = "docker.io/acme/node:latest"

[vastai]
relay_url = "https://relay.example"
bootstrap_command = "boot"
"#,
        );
        with_process_state(&[], Some(missing_secret.path()), || {
            let config_arg = missing_secret_config.to_string_lossy().into_owned();
            assert!(Config::from_args(strings(&["--config", config_arg.as_str()])).is_err());
        });

        let missing_relay = TempDir::new("vastai-missing-relay");
        let missing_relay_config = write_config(
            &missing_relay,
            "chat.toml",
            r#"
[provider]
kind = "vastai"

[image]
node = "docker.io/acme/node:latest"

[vastai]
bootstrap_command = "boot"
"#,
        );
        with_process_state(
            &[("VASTAI_API_KEY", Some("secret"))],
            Some(missing_relay.path()),
            || {
                let config_arg = missing_relay_config.to_string_lossy().into_owned();
                assert!(Config::from_args(strings(&["--config", config_arg.as_str()])).is_err());
            },
        );

        let local_image = TempDir::new("vastai-local-image");
        let local_image_config = write_config(
            &local_image,
            "chat.toml",
            r#"
[provider]
kind = "vastai"

[image]
node = "local-node:latest"

[vastai]
relay_url = "https://relay.example"
bootstrap_command = "boot"
"#,
        );
        with_process_state(
            &[("VASTAI_API_KEY", Some("secret"))],
            Some(local_image.path()),
            || {
                let config_arg = local_image_config.to_string_lossy().into_owned();
                assert!(Config::from_args(strings(&["--config", config_arg.as_str()])).is_err());
            },
        );

        let valid = TempDir::new("vastai-valid");
        let valid_config = write_config(
            &valid,
            "chat.toml",
            r#"
[provider]
kind = "vastai"

[image]
node = "docker.io/acme/node:latest"

[vastai]
relay_url = "https://relay.example"
bootstrap_command = "boot"
"#,
        );
        with_process_state(
            &[("VASTAI_API_KEY", Some("secret"))],
            Some(valid.path()),
            || {
                let config_arg = valid_config.to_string_lossy().into_owned();
                let config = Config::from_args(strings(&["--config", config_arg.as_str()]))
                    .expect("valid Vast.ai config resolves");
                let vastai = config.vastai.as_ref().expect("resolved Vast.ai config");
                assert_eq!(vastai.api_key, "secret");
                assert_eq!(vastai.relay_url, "https://relay.example");
                assert_eq!(vastai.bootstrap_command, "boot");
                assert_eq!(vastai.image, "docker.io/acme/node:latest");
            },
        );
    }

    struct MockApproval {
        terminal: bool,
        answer: Result<bool, String>,
    }

    impl VastAiApproval for MockApproval {
        fn stdin_is_terminal(&self) -> bool {
            self.terminal
        }

        fn ask(&mut self) -> Result<bool, String> {
            self.answer.clone()
        }
    }

    #[test]
    fn parse_approval_accepts_only_yes_variants() {
        for value in ["y", "Y", " yes \n", "YeS"] {
            assert!(parse_approval(value), "{value:?} should approve");
        }
        for value in ["", "n", "no", "yep", " yes please"] {
            assert!(!parse_approval(value), "{value:?} should decline");
        }
    }

    #[test]
    fn vastai_approval_is_used_only_when_required() {
        let process = base_config(ProviderKind::Process);
        let mut approval = MockApproval {
            terminal: false,
            answer: Err("should not ask".to_owned()),
        };
        confirm_vastai_if_needed_with_approval(&process, &mut approval)
            .expect("non-Vast.ai skips approval");

        let mut yes_config = base_config(ProviderKind::VastAi);
        yes_config.vastai = Some(valid_vastai());
        yes_config.vastai_yes = true;
        let mut approval = MockApproval {
            terminal: false,
            answer: Err("should not ask".to_owned()),
        };
        confirm_vastai_if_needed_with_approval(&yes_config, &mut approval)
            .expect("--yes skips approval prompt");

        let mut non_terminal = base_config(ProviderKind::VastAi);
        non_terminal.vastai = Some(valid_vastai());
        let mut approval = MockApproval {
            terminal: false,
            answer: Err("should not ask".to_owned()),
        };
        assert!(confirm_vastai_if_needed_with_approval(&non_terminal, &mut approval).is_err());

        let mut accepted = base_config(ProviderKind::VastAi);
        accepted.vastai = Some(valid_vastai());
        let mut approval = MockApproval {
            terminal: true,
            answer: Ok(true),
        };
        confirm_vastai_if_needed_with_approval(&accepted, &mut approval)
            .expect("interactive approval accepts");

        let mut declined = base_config(ProviderKind::VastAi);
        declined.vastai = Some(valid_vastai());
        let mut approval = MockApproval {
            terminal: true,
            answer: Ok(false),
        };
        assert!(confirm_vastai_if_needed_with_approval(&declined, &mut approval).is_err());
    }

    #[test]
    fn cached_model_discovery_selects_first_sorted_gguf_file() {
        let temp = TempDir::new("cached-model-selects");
        let cache_dir = temp.path().join(".model-cache");
        fs::create_dir_all(&cache_dir).expect("create cache dir");
        fs::write(cache_dir.join("z.gguf"), b"z").expect("write z model");
        fs::write(cache_dir.join("a.gguf"), b"a").expect("write a model");
        fs::write(cache_dir.join("ignored.txt"), b"ignored").expect("write ignored file");
        fs::create_dir(cache_dir.join("0.gguf")).expect("create ignored directory");

        with_process_state(&[], Some(temp.path()), || {
            let cached = CachedModelConfig::discover().expect("cached model discovered");
            assert_eq!(cached.host_path.file_name(), Some(OsStr::new("a.gguf")));
        });
    }

    #[test]
    fn cached_model_discovery_errors_when_no_usable_model_exists() {
        let missing = TempDir::new("cached-model-missing");
        with_process_state(&[], Some(missing.path()), || {
            assert!(CachedModelConfig::discover().is_err());
        });

        let empty = TempDir::new("cached-model-empty");
        let cache_dir = empty.path().join(".model-cache");
        fs::create_dir_all(&cache_dir).expect("create cache dir");
        fs::write(cache_dir.join("ignored.txt"), b"ignored").expect("write ignored file");
        fs::create_dir(cache_dir.join("not-a-file.gguf")).expect("create ignored directory");
        with_process_state(&[], Some(empty.path()), || {
            assert!(CachedModelConfig::discover().is_err());
        });
    }

    #[test]
    fn cached_model_path_resolves_regular_gguf_file() {
        let temp = TempDir::new("cached-model-path");
        let model = temp.path().join("chosen.gguf");
        fs::write(&model, b"model").expect("write chosen model");
        let model_arg = model.to_string_lossy().into_owned();

        with_process_state(&[], Some(temp.path()), || {
            let parsed = ParsedArgs::parse(strings(&[&format!("--cached-model={model_arg}")]))
                .expect("cached model path parses");
            assert_eq!(
                parsed.cached_model,
                Some(CachedModelSource::Path(PathBuf::from(model_arg.as_str())))
            );

            let config = Config::from_args(strings(&[&format!("--cached-model={model_arg}")]))
                .expect("cached model path resolves");
            assert_eq!(
                config.cached_model.unwrap().host_path.file_name(),
                Some(OsStr::new("chosen.gguf"))
            );

            let upper_model = temp.path().join("upper.GGUF");
            fs::write(&upper_model, b"model").expect("write uppercase model");
            let upper = CachedModelConfig::from_path(upper_model)
                .expect("uppercase cached model extension resolves");
            assert_eq!(upper.host_path.file_name(), Some(OsStr::new("upper.GGUF")));
        });
    }

    fn panic_prepare_node_image(_: NodeImageRequest) -> Result<PreparedNodeImage, String> {
        panic!("image preparer must not be called when --skip-rebuild is set")
    }

    #[test]
    fn skip_rebuild_requires_existing_artifacts_and_skips_image_preparation() {
        let temp = TempDir::new("skip-rebuild");
        let orch_bin = temp.path().join("mvp-orchestrator");
        let worker_bin = temp.path().join("mvp-worker-node");
        let mut config = base_config(ProviderKind::Docker);
        config.skip_rebuild = true;
        config.orch_bin = orch_bin.clone();
        config.worker_bin = worker_bin.clone();
        config.node_image = "docker.io/acme/node:latest".to_owned();

        assert!(prepare_runtime_with(&config, panic_prepare_node_image).is_err());

        fs::write(&orch_bin, b"orch").expect("write orchestrator artifact");
        assert!(prepare_runtime_with(&config, panic_prepare_node_image).is_err());

        fs::write(&worker_bin, b"worker").expect("write worker artifact");
        let image_ref = prepare_runtime_with(&config, panic_prepare_node_image)
            .expect("skip rebuild uses existing artifacts");
        assert_eq!(image_ref, "docker.io/acme/node:latest");
    }

    #[test]
    fn artifact_roots_use_current_directory() {
        let temp = TempDir::new("artifact-root");

        with_process_state(&[], Some(temp.path()), || {
            let path = default_orch_bin().expect("default orchestrator path resolves");
            assert!(path.starts_with(temp.path()), "{path:?}");
            assert!(path.ends_with("target/debug/mvp-orchestrator"), "{path:?}");
        });
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn orch_child_shutdown_sends_sigterm_to_process_group() {
        let temp = TempDir::new("orch-shutdown");
        let flag_path = temp.path().join("term.flag");
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                "trap 'echo term > \"$1\"; exit 0' TERM; while true; do sleep 1; done",
                "sh",
            ])
            .arg(&flag_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        let child = command.spawn().expect("spawn signal test child");
        thread::sleep(Duration::from_millis(100));
        let mut orch = OrchChild {
            child,
            cleaned: false,
        };

        orch.shutdown();

        assert!(flag_path.exists(), "SIGTERM trap should write flag");
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
