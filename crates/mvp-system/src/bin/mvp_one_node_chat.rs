use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, ExitCode, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, SystemTime};

use mvp_system::config as chat_config;
use mvp_system::config::ResolvedVastAiConfig;
use mvp_system::node_image::{NodeImageProvider, NodeImageRequest, prepare_node_image};
use mvp_system::node_provisioning::ProviderKind;
use mvp_system::prompt_rpc::{PromptEvent, SubmitPrompt, write_json_line};
use mvp_system::vastai_offer_preview::{OfferPreview, OfferPreviewer, VastAiOfferPreviewer};
use serde_json::Value;

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

const DEFAULT_RPC_ADDR: &str = "127.0.0.1:19777";
const DEFAULT_NODE_IMAGE: &str = "swactor-mvp-node:latest";
const BASE_NODE_IMAGE: &str = "swactor-mvp-node-base:cuda12.6";
const MVP_RUNTIME_CONFIG_ENV: &str = "MVP_RUNTIME_CONFIG";
const CACHED_MODEL_HOST_ENV: &str = "MVP_CACHED_MODEL_HOST_PATH";
const DEFAULT_CACHED_MODEL_FILE: &str = "Llama-3.2-1B-Instruct-Q4_K_M.gguf";
const REPO_MODEL_CACHE_DIR: &str = ".model-cache";
const DEFAULT_MAX_TOKENS: u32 = 64;
const ORCH_REBUILD_INPUTS: &[&str] = &[
    "Cargo.lock",
    "Cargo.toml",
    "src",
    "crates/datastream/Cargo.toml",
    "crates/datastream/src",
    "crates/dashboard/Cargo.toml",
    "crates/dashboard/src",
    "crates/distribution/Cargo.toml",
    "crates/distribution/src",
    "crates/iroh-driver/Cargo.toml",
    "crates/iroh-driver/src",
    "crates/mvp-system/Cargo.toml",
    "crates/mvp-system/src",
    "crates/transport/Cargo.toml",
    "crates/transport/src",
    "tools/vastai/Cargo.toml",
    "tools/vastai/src",
];
const CHAT_READ_TIMEOUT: Duration = Duration::from_millis(100);

static STOP_REQUESTED: AtomicBool = AtomicBool::new(false);
static STOP_ACKNOWLEDGED: AtomicBool = AtomicBool::new(false);

pub fn run_from_args<I>(args: I) -> ExitCode
where
    I: IntoIterator<Item = String>,
{
    install_signal_handlers();
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mvp-one-node-chat: {error}");
            ExitCode::from(1)
        }
    }
}

fn run<I>(args: I) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    let mut config = Config::from_args(args)?;
    confirm_vastai_if_needed(&config, VastAiOfferPreviewer)?;
    let image_ref = prepare_runtime(&config)?;
    let frame_log = configure_progress_frame_log(&mut config)?;
    if let Some(path) = &config.datastream_frame_log {
        eprintln!(
            "mvp-one-node-chat: dumping datastream frames to {}",
            display_user_path(path)
        );
    }
    let mut progress = StartupProgress::new(&frame_log);
    let mut orch = OrchChild::spawn(&config, &image_ref)?;
    let rpc_addr = match orch.wait_ready(config.rpc_addr.clone(), &mut progress) {
        Ok(addr) => addr,
        Err(_) if STOP_REQUESTED.load(Ordering::SeqCst) => {
            acknowledge_stop();
            let _ = orch.shutdown(true);
            report_interrupt_shutdown();
            return Ok(());
        }
        Err(error) => {
            progress.poll();
            return Err(progress.failure_summary().unwrap_or(error));
        }
    };
    progress.poll();
    println!("model successfully loaded.");
    let result = run_chat_loop(&rpc_addr, config.max_tokens);
    let interrupted = STOP_REQUESTED.load(Ordering::SeqCst);
    let _ = orch.shutdown(interrupted);
    if interrupted {
        report_interrupt_shutdown();
    }
    result
}

struct Config {
    orch_bin: PathBuf,
    orch_args: Vec<String>,
    rpc_addr: String,
    node_image: String,
    config_profile: RuntimeConfigProfile,
    provider: ProviderKind,
    relay_mode: iroh::RelayMode,
    relay_url: Option<String>,
    max_tokens: u32,
    dashboard: bool,
    build_image: bool,
    image_tag: Option<String>,
    push_image: bool,
    force_image_refresh: bool,
    cached_model: Option<CachedModelConfig>,
    datastream_frame_log: Option<PathBuf>,
    vastai_yes: bool,
    vastai: Option<ResolvedVastAiConfig>,
    model_id: Option<String>,
    gguf_repo: Option<String>,
    gguf_file: Option<String>,
    gguf_revision: Option<String>,
    max_context: Option<u32>,
}

impl Config {
    fn from_args<I>(provided_args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = String>,
    {
        let args = ParsedArgs::parse(provided_args)?;
        let loaded = chat_config::Config::load(args.config_path.as_deref())?;
        let file = loaded.config;
        let profile = if args.vastai {
            RuntimeConfigProfile::Deploy
        } else {
            RuntimeConfigProfile::from_env()?
        };
        let provider = if args.vastai {
            ProviderKind::VastAi
        } else {
            provider_from_env(profile)?
        };
        let relay_mode = relay_mode_from_sources(file.relay.mode.as_deref())?;
        let relay_url = first_non_empty([
            env_optional("MVP_IROH_RELAY_URL"),
            env_optional("SWACTOR_IROH_RELAY_URL"),
            file.relay.url.clone(),
        ]);
        let node_image = first_non_empty([
            args.image,
            env_optional("MVP_NODE_IMAGE"),
            if args.vastai {
                file.vastai.image.clone()
            } else {
                None
            },
            file.image.node.clone(),
            Some(DEFAULT_NODE_IMAGE.to_owned()),
        ])
        .expect("default image is non-empty");
        let max_tokens = args
            .max_tokens
            .or(env_u32_optional("MVP_PROMPT_MAX_TOKENS")?)
            .or(file.prompt.max_tokens)
            .unwrap_or(DEFAULT_MAX_TOKENS);
        let dashboard = args
            .dashboard
            .or(env_bool_optional("MVP_DASHBOARD")?)
            .or(file.prompt.dashboard)
            .unwrap_or(true);
        let build_image = args
            .build_image
            .or(env_bool_optional("MVP_BUILD_NODE_IMAGE")?)
            .or(file.image.build)
            .unwrap_or(true);
        let push_image = args
            .push_image
            .or(env_bool_optional("MVP_PUSH_NODE_IMAGE")?)
            .or(file.image.push)
            .unwrap_or(false);
        let force_image_refresh = args
            .force_image_refresh
            .or(env_bool_optional("MVP_FORCE_NODE_IMAGE_REFRESH")?)
            .or(file.image.force_refresh)
            .unwrap_or(false);
        let image_tag = first_non_empty([
            args.image_tag,
            env_optional("MVP_NODE_IMAGE_TAG"),
            file.image.tag.clone(),
        ]);
        let rpc_addr = first_non_empty([
            args.rpc_addr,
            env_optional("MVP_PROMPT_RPC_ADDR"),
            env_optional("MVP_PROMPT_RPC_BIND"),
            file.prompt.rpc_addr.clone(),
            Some(DEFAULT_RPC_ADDR.to_owned()),
        ])
        .expect("default RPC address is non-empty");
        let datastream_frame_log = match (args.dump_logs, args.datastream_frame_log) {
            (true, Some(_)) => {
                return Err("--dump-logs cannot be combined with --datastream-frame-log; use one datastream log destination".to_owned());
            }
            (true, None) => Some(PathBuf::from("mvp-chat.log")),
            (false, explicit) => {
                explicit.or_else(|| env_optional("MVP_DATASTREAM_FRAME_LOG").map(PathBuf::from))
            }
        };
        let model_id = first_non_empty([env_optional("MVP_MODEL_ID"), file.model.id.clone()]);
        let gguf_repo =
            first_non_empty([env_optional("MVP_GGUF_REPO"), file.model.gguf_repo.clone()]);
        let gguf_file =
            first_non_empty([env_optional("MVP_GGUF_FILE"), file.model.gguf_file.clone()]);
        let gguf_revision = first_non_empty([
            env_optional("MVP_GGUF_REVISION"),
            file.model.gguf_revision.clone(),
        ]);
        let max_context = env_u32_optional("MVP_MAX_CONTEXT")?.or(file.model.max_context);
        let vastai = if args.vastai {
            Some(resolve_vastai_config(
                &file.vastai,
                &node_image,
                relay_url.clone(),
            )?)
        } else {
            None
        };

        Ok(Self {
            orch_bin: args.orch_bin.unwrap_or(default_orch_bin()?),
            orch_args: args.orch_args,
            rpc_addr,
            node_image,
            config_profile: profile,
            provider,
            relay_mode,
            relay_url,
            max_tokens,
            dashboard,
            build_image,
            image_tag,
            push_image,
            force_image_refresh,
            cached_model: args.cached_model,
            datastream_frame_log,
            vastai_yes: args.vastai_yes,
            model_id,
            gguf_repo,
            gguf_file,
            gguf_revision,
            max_context,
            vastai,
        })
    }
}

#[derive(Default)]
struct ParsedArgs {
    vastai: bool,
    vastai_yes: bool,
    config_path: Option<PathBuf>,
    orch_bin: Option<PathBuf>,
    rpc_addr: Option<String>,
    image: Option<String>,
    max_tokens: Option<u32>,
    datastream_frame_log: Option<PathBuf>,
    dump_logs: bool,
    dashboard: Option<bool>,
    build_image: Option<bool>,
    image_tag: Option<String>,
    push_image: Option<bool>,
    force_image_refresh: Option<bool>,
    cached_model: Option<CachedModelConfig>,
    orch_args: Vec<String>,
}

impl ParsedArgs {
    fn parse<I>(provided_args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = String>,
    {
        let mut parsed = Self::default();
        let mut args = provided_args.into_iter().peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--vastai" => parsed.vastai = true,
                "--yes" | "-y" => parsed.vastai_yes = true,
                "--config" => {
                    parsed.config_path = Some(PathBuf::from(next_arg(&mut args, "--config")?))
                }
                "--orch-bin" => {
                    parsed.orch_bin = Some(PathBuf::from(next_arg(&mut args, "--orch-bin")?))
                }
                "--addr" => parsed.rpc_addr = Some(next_arg(&mut args, "--addr")?),
                "--image" => parsed.image = Some(next_arg(&mut args, "--image")?),
                "--max-tokens" => parsed.max_tokens = Some(parse_next(&mut args, "--max-tokens")?),
                "--datastream-frame-log" => {
                    parsed.datastream_frame_log = Some(PathBuf::from(next_arg(
                        &mut args,
                        "--datastream-frame-log",
                    )?));
                }
                "--dump-logs" => parsed.dump_logs = true,
                "--dashboard" => parsed.dashboard = Some(true),
                "--no-dashboard" => parsed.dashboard = Some(false),
                "--no-build-image" => parsed.build_image = Some(false),
                "--image-tag" => parsed.image_tag = Some(next_arg(&mut args, "--image-tag")?),
                "--push-image" => parsed.push_image = Some(true),
                "--no-push-image" => parsed.push_image = Some(false),
                "--force-image-refresh" => parsed.force_image_refresh = Some(true),
                "--cached-model" => {
                    let path = args.next_if(|value| !value.starts_with("--"));
                    parsed.cached_model = Some(CachedModelConfig::from_arg(path)?);
                }
                "--" => {
                    parsed.orch_args.extend(args);
                    break;
                }
                other => parsed.orch_args.push(other.to_owned()),
            }
        }
        Ok(parsed)
    }
}

fn resolve_vastai_config(
    file: &chat_config::VastAiConfig,
    node_image: &str,
    relay_url: Option<String>,
) -> Result<ResolvedVastAiConfig, String> {
    ResolvedVastAiConfig {
        api_key: first_non_empty([
            env_optional("MVP_VASTAI_API_KEY"),
            env_optional("VAST_API_KEY"),
            file.api_key.clone(),
        ])
        .unwrap_or_default(),
        relay_url: relay_url.unwrap_or_default(),
        image: node_image.to_owned(),
        bootstrap_command: first_non_empty([
            env_optional("MVP_VASTAI_BOOTSTRAP_COMMAND"),
            file.bootstrap_command.clone(),
        ])
        .unwrap_or_default(),
        disk_gb: env_u32_optional("MVP_VASTAI_DISK_GB")?.or(file.disk_gb),
        gpu_name: first_non_empty([env_optional("MVP_VASTAI_GPU_NAME"), file.gpu_name.clone()]),
        min_gpu_ram_mb: env_u64_optional("MVP_VASTAI_MIN_GPU_RAM_MB")?.or(file.min_gpu_ram_mb),
        min_down_mbps: env_f64_optional("MVP_VASTAI_MIN_DOWN_MBPS")?.or(file.min_down_mbps),
        min_up_mbps: env_f64_optional("MVP_VASTAI_MIN_UP_MBPS")?.or(file.min_up_mbps),
        min_reliability: env_f64_optional("MVP_VASTAI_MIN_RELIABILITY")?.or(file.min_reliability),
        require_verified: env_bool_optional("MVP_VASTAI_REQUIRE_VERIFIED")?
            .or(file.require_verified),
        onstart: first_non_empty([env_optional("MVP_VASTAI_ONSTART"), file.onstart.clone()]),
        ssh_identity: first_non_empty([
            env_optional("MVP_VASTAI_SSH_IDENTITY"),
            file.ssh_identity.clone(),
        ]),
    }
    .validate()
}

fn first_non_empty<const N: usize>(values: [Option<String>; N]) -> Option<String> {
    values.into_iter().find_map(chat_config::normalize_optional)
}

fn confirm_vastai_if_needed<P>(config: &Config, previewer: P) -> Result<(), String>
where
    P: OfferPreviewer,
{
    let Some(vastai) = &config.vastai else {
        return Ok(());
    };
    eprintln!("mvp-one-node-chat: checking Vast.ai offers...");
    let preview = previewer.preview(&vastai.api_key, &vastai.selection_policy())?;
    print_offer_preview(&preview);
    if config.vastai_yes {
        eprintln!("mvp-one-node-chat: --yes supplied; skipping Vast.ai rental prompt");
        return Ok(());
    }
    if !io::stdin().is_terminal() {
        return Err("Vast.ai rental requires --yes when stdin is not a terminal".to_owned());
    }
    if ask_vastai_approval()? {
        Ok(())
    } else {
        Err("Vast.ai rental declined".to_owned())
    }
}

fn print_offer_preview(preview: &OfferPreview) {
    let ram = preview
        .gpu_ram_mb
        .map(|mb| format!(", {mb} MB VRAM"))
        .unwrap_or_default();
    eprintln!(
        "mvp-one-node-chat: best Vast.ai offer {}{} at ${:.3}/hr",
        preview.gpu_name, ram, preview.dollars_per_hour
    );
}

fn ask_vastai_approval() -> Result<bool, String> {
    eprint!("Rent 1 Vast.ai node? [y/N]: ");
    io::stderr()
        .flush()
        .map_err(|e| format!("flush Vast.ai approval prompt: {e}"))?;
    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .map_err(|e| format!("read Vast.ai approval: {e}"))?;
    Ok(parse_approval(&line))
}

fn parse_approval(input: &str) -> bool {
    matches!(input.trim().to_ascii_lowercase().as_str(), "y" | "yes")
}

struct FrameLogConfig {
    path: PathBuf,
    start_offset: u64,
    remove_on_drop: bool,
}

fn configure_progress_frame_log(config: &mut Config) -> Result<FrameLogConfig, String> {
    if let Some(path) = &config.datastream_frame_log {
        let start_offset = fs::metadata(path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        return Ok(FrameLogConfig {
            path: path.clone(),
            start_offset,
            remove_on_drop: false,
        });
    }

    let path = PathBuf::from("target")
        .join("mvp-chat")
        .join(format!("startup-{}.frames.jsonl", std::process::id()));
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("create startup frame log dir {}: {e}", parent.display()))?;
    }
    File::create(&path).map_err(|e| format!("create startup frame log {}: {e}", path.display()))?;
    config.datastream_frame_log = Some(path.clone());
    Ok(FrameLogConfig {
        path,
        start_offset: 0,
        remove_on_drop: true,
    })
}

impl Drop for FrameLogConfig {
    fn drop(&mut self) {
        if self.remove_on_drop {
            let _ = fs::remove_file(&self.path);
        }
    }
}

struct StartupProgress {
    path: PathBuf,
    offset: u64,
    partial: String,
    printed: HashSet<String>,
    last_error_line: Option<String>,
    last_failure: Option<String>,
    last_download_bucket: Option<u64>,
}

impl StartupProgress {
    fn new(frame_log: &FrameLogConfig) -> Self {
        Self {
            path: frame_log.path.clone(),
            offset: frame_log.start_offset,
            partial: String::new(),
            printed: HashSet::new(),
            last_error_line: None,
            last_failure: None,
            last_download_bucket: None,
        }
    }

    fn poll(&mut self) {
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(_) => return,
        };
        if file.seek(SeekFrom::Start(self.offset)).is_err() {
            return;
        }
        let mut chunk = String::new();
        if file.read_to_string(&mut chunk).is_err() || chunk.is_empty() {
            return;
        }
        self.offset += chunk.as_bytes().len() as u64;
        self.partial.push_str(&chunk);
        while let Some(newline) = self.partial.find('\n') {
            let line: String = self.partial.drain(..=newline).collect();
            let line = line.trim();
            if !line.is_empty() {
                self.observe_archive_line(line);
            }
        }
    }

    fn failure_summary(&self) -> Option<String> {
        if let Some(line) = &self.last_error_line {
            return Some(format!("node provisioning failed: {line}"));
        }
        self.last_failure.clone()
    }

    fn observe_archive_line(&mut self, line: &str) {
        let Ok(record) = serde_json::from_str::<Value>(line) else {
            return;
        };
        let Some(channel) = record.get("channel").and_then(Value::as_str) else {
            return;
        };
        let Some(payload_text) = record
            .get("payload")
            .and_then(|payload| payload.get("value"))
            .and_then(Value::as_str)
        else {
            return;
        };
        let Ok(payload) = serde_json::from_str::<Value>(payload_text) else {
            return;
        };

        if channel == "mvp.orch.bootstrap" {
            self.observe_bootstrap(&payload);
        } else if channel == "mvp.provisioning.events" {
            self.observe_provision_event(&payload);
        } else if channel == "mvp.worker.weights" {
            self.observe_worker_weights(&payload);
        } else if channel.starts_with("mvp.provisioning.logs.") {
            self.observe_provision_log(&payload);
        }
    }

    fn observe_bootstrap(&mut self, payload: &Value) {
        let phase = payload.get("phase").and_then(Value::as_str).unwrap_or("");
        let status = payload.get("status").and_then(Value::as_str).unwrap_or("");
        if status == "failed" {
            let error = payload
                .get("detail")
                .and_then(|detail| detail.get("error"))
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            self.last_failure = Some(format!("{phase} failed: {error}"));
            return;
        }
        match (phase, status) {
            ("provider_start", "started") => {
                self.print_once("starting_docker_node", "starting docker node")
            }
            ("node_runtime_ready", "started") => {
                self.print_once("node_runtime_ready_started", "waiting for node runtime")
            }
            ("node_runtime_ready", "ready") => {
                self.print_once("node_runtime_ready", "node runtime ready")
            }
            ("stage_provision", "started") => {
                self.print_once("stage_provision", "configuring model stage")
            }
            ("weights_loaded", "started") => {
                self.print_once("weights_loaded_started", "loading model")
            }
            ("prompt_rpc", "ready") | ("prompt_loop", "ready") => {
                self.print_once("prompt_ready", "prompt RPC ready")
            }
            _ => {}
        }
    }

    fn observe_provision_event(&mut self, payload: &Value) {
        let Some(event) = payload.get("event") else {
            return;
        };
        match event.get("kind").and_then(Value::as_str).unwrap_or("") {
            "ProvisionStart" => self.print_once("starting_docker_node", "starting docker node"),
            "NodeLive" => self.print_once("node_runtime_ready", "node runtime ready"),
            "ProvisionFailed" => {
                let message = event
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("provisioning failed");
                self.last_failure = Some(format!("node provisioning failed: {message}"));
            }
            _ => {}
        }
    }

    fn observe_worker_weights(&mut self, payload: &Value) {
        match payload.get("type").and_then(Value::as_str).unwrap_or("") {
            "GgufDownloadStarted" => {
                self.print_once("download_started", "downloading model weights")
            }
            "GgufDownloadProgress" => {
                let done = payload
                    .get("bytes_done")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let total = payload
                    .get("bytes_total")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                if total == 0 {
                    return;
                }
                let pct = done.saturating_mul(100).saturating_div(total).min(100);
                let bucket = pct / 10;
                if self.last_download_bucket != Some(bucket) {
                    self.last_download_bucket = Some(bucket);
                    eprintln!("mvp-one-node-chat: downloading model weights {pct}%");
                }
            }
            _ => {}
        }
    }

    fn observe_provision_log(&mut self, payload: &Value) {
        let Some(line) = payload
            .get("line")
            .and_then(|line| line.get("line"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|line| !line.is_empty())
        else {
            return;
        };
        if line.starts_with("docker:") || line.contains("Error response") || line.contains("error")
        {
            self.last_error_line = Some(line.to_owned());
        }
    }

    fn print_once(&mut self, key: &str, message: &str) {
        if self.printed.insert(key.to_owned()) {
            eprintln!("mvp-one-node-chat: {message}");
        }
    }
}

struct OrchChild {
    child: Child,
    stdin: Option<ChildStdin>,
    cleaned: bool,
}

impl OrchChild {
    fn spawn(config: &Config, image_ref: &str) -> Result<Self, String> {
        let mut command = Command::new(&config.orch_bin);
        command
            .args(&config.orch_args)
            .env("MVP_NODE_PROVIDER", config.provider.as_str())
            .env(MVP_RUNTIME_CONFIG_ENV, config.config_profile.as_str())
            .env("MVP_NODE_IMAGE", image_ref)
            .env(
                "MVP_IROH_RELAY_MODE",
                relay_mode_env_value(&config.relay_mode),
            )
            .env("MVP_PROMPT_RPC_BIND", &config.rpc_addr)
            .env("MVP_PROMPT_RPC_ADDR", &config.rpc_addr)
            .env("MVP_PROMPT_MAX_TOKENS", config.max_tokens.to_string())
            .env("MVP_DASHBOARD", if config.dashboard { "1" } else { "0" })
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(relay_url) = &config.relay_url {
            command.env("MVP_IROH_RELAY_URL", relay_url);
        }
        if let Some(vastai) = &config.vastai {
            command
                .env("MVP_VASTAI_API_KEY", &vastai.api_key)
                .env("MVP_VASTAI_BOOTSTRAP_COMMAND", &vastai.bootstrap_command)
                .env("MVP_VASTAI_CONFIRM_LEASE", "0");
            if let Some(disk_gb) = vastai.disk_gb {
                command.env("MVP_VASTAI_DISK_GB", disk_gb.to_string());
            }
            if let Some(gpu_name) = &vastai.gpu_name {
                command.env("MVP_VASTAI_GPU_NAME", gpu_name);
            }
            if let Some(min_gpu_ram_mb) = vastai.min_gpu_ram_mb {
                command.env("MVP_VASTAI_MIN_GPU_RAM_MB", min_gpu_ram_mb.to_string());
            }
            if let Some(min_down_mbps) = vastai.min_down_mbps {
                command.env("MVP_VASTAI_MIN_DOWN_MBPS", min_down_mbps.to_string());
            }
            if let Some(min_up_mbps) = vastai.min_up_mbps {
                command.env("MVP_VASTAI_MIN_UP_MBPS", min_up_mbps.to_string());
            }
            if let Some(min_reliability) = vastai.min_reliability {
                command.env("MVP_VASTAI_MIN_RELIABILITY", min_reliability.to_string());
            }
            if let Some(require_verified) = vastai.require_verified {
                command.env(
                    "MVP_VASTAI_REQUIRE_VERIFIED",
                    if require_verified { "1" } else { "0" },
                );
            }
            if let Some(onstart) = &vastai.onstart {
                command.env("MVP_VASTAI_ONSTART", onstart);
            }
            if let Some(ssh_identity) = &vastai.ssh_identity {
                command.env("MVP_VASTAI_SSH_IDENTITY", ssh_identity);
            }
        }
        if let Some(model_id) = &config.model_id {
            command.env("MVP_MODEL_ID", model_id);
        }
        if let Some(repo) = &config.gguf_repo {
            command.env("MVP_GGUF_REPO", repo);
        }
        if let Some(file) = &config.gguf_file {
            command.env("MVP_GGUF_FILE", file);
        }
        if let Some(revision) = &config.gguf_revision {
            command.env("MVP_GGUF_REVISION", revision);
        }
        if let Some(max_context) = config.max_context {
            command.env("MVP_MAX_CONTEXT", max_context.to_string());
        }
        if let Some(cached_model) = &config.cached_model {
            command.env(CACHED_MODEL_HOST_ENV, &cached_model.host_path);
        }
        if let Some(path) = &config.datastream_frame_log {
            command.env("MVP_DATASTREAM_FRAME_LOG", path);
        }
        #[cfg(target_os = "linux")]
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        let display_orch = display_user_path(&config.orch_bin);
        let mut child = command
            .spawn()
            .map_err(|e| format!("spawn {display_orch}: {e}"))?;
        let stdin = child.stdin.take();
        Ok(Self {
            child,
            stdin,
            cleaned: false,
        })
    }

    fn wait_ready(
        &mut self,
        rpc_addr: String,
        progress: &mut StartupProgress,
    ) -> Result<String, String> {
        loop {
            progress.poll();
            if STOP_REQUESTED.load(Ordering::SeqCst) {
                acknowledge_stop();
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
                progress.poll();
                if let Some(failure) = progress.failure_summary() {
                    return Err(failure);
                }
                return Err(format!(
                    "orchestrator exited before prompt RPC ready: {status}"
                ));
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn shutdown(&mut self, _interrupt: bool) -> bool {
        if self.cleaned {
            return false;
        }
        self.cleaned = true;
        if let Some(mut stdin) = self.stdin.take() {
            let _ = writeln!(stdin, "shutdown");
            let _ = stdin.flush();
        }
        let _ = self.child.wait();
        false
    }
}

impl Drop for OrchChild {
    fn drop(&mut self) {
        let _ = self.shutdown(false);
    }
}

fn prepare_runtime(config: &Config) -> Result<String, String> {
    ensure_orch_binary(config)?;
    if !config.build_image {
        eprintln!("mvp-one-node-chat: skipping node image preparation (--no-build-image)");
        return Ok(config.node_image.clone());
    }
    if let Some(cached_model) = &config.cached_model {
        eprintln!(
            "mvp-one-node-chat: using cached model {}",
            cached_model.display_path.display()
        );
    }
    let prepared = prepare_node_image(NodeImageRequest {
        requested_image: config.node_image.clone(),
        base_image: BASE_NODE_IMAGE.to_owned(),
        node_bin: node_bin_for_current_profile()?,
        provider: node_image_provider(config.provider)?,
        extra_tag: config.image_tag.clone(),
        push: config.push_image,
        force_refresh: config.force_image_refresh,
        enabled: true,
    })?;
    eprintln!(
        "mvp-one-node-chat: using node image {} ({})",
        prepared.image_ref, prepared.tag
    );
    Ok(prepared.image_ref)
}

fn run_chat_loop(addr: &str, max_tokens: u32) -> Result<(), String> {
    let mut stream =
        TcpStream::connect(addr).map_err(|e| format!("connect prompt RPC {addr}: {e}"))?;
    stream
        .set_read_timeout(Some(CHAT_READ_TIMEOUT))
        .map_err(|e| format!("set prompt RPC read timeout: {e}"))?;
    let mut reader = BufReader::new(
        stream
            .try_clone()
            .map_err(|e| format!("clone prompt RPC stream: {e}"))?,
    );
    let (input_tx, input_rx) = mpsc::channel::<String>();
    thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines().map_while(Result::ok) {
            if input_tx.send(line).is_err() {
                break;
            }
        }
    });
    let mut next_request_id = 1_u64;
    eprintln!(
        "mvp-one-node-chat: Ctrl-C cleans up the orchestrator and provider node; /exit exits cleanly"
    );

    loop {
        if STOP_REQUESTED.load(Ordering::SeqCst) {
            acknowledge_stop();
            return Ok(());
        }
        print!("prompt:> ");
        io::stdout()
            .flush()
            .map_err(|e| format!("flush prompt: {e}"))?;
        let prompt = loop {
            if STOP_REQUESTED.load(Ordering::SeqCst) {
                acknowledge_stop();
                return Ok(());
            }
            match input_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(line) => break line.trim_end().to_owned(),
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
            }
        };
        if prompt.eq_ignore_ascii_case("/quit") || prompt.eq_ignore_ascii_case("/exit") {
            return Ok(());
        }
        if prompt.trim().is_empty() {
            continue;
        }

        let request_id = next_request_id;
        next_request_id = next_request_id.wrapping_add(1).max(1);
        write_json_line(
            &mut stream,
            &SubmitPrompt {
                request_id,
                prompt_text: prompt,
                max_tokens,
            },
        )?;
        println!("decoding...");
        let mut response_started = false;

        loop {
            if STOP_REQUESTED.load(Ordering::SeqCst) {
                acknowledge_stop();
                return Ok(());
            }
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => return Err("prompt RPC closed".to_owned()),
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                Err(error) => return Err(format!("read prompt event: {error}")),
            }
            let event = serde_json::from_str::<PromptEvent>(&line)
                .map_err(|e| format!("parse prompt event: {e}"))?;
            match event {
                PromptEvent::TextDelta {
                    request_id: seen,
                    text,
                } if seen == request_id => {
                    if !response_started {
                        print!("Response: ");
                        response_started = true;
                    }
                    print!("{text}");
                    io::stdout()
                        .flush()
                        .map_err(|e| format!("flush response text: {e}"))?;
                }
                PromptEvent::Done {
                    request_id: seen, ..
                } if seen == request_id => {
                    if response_started {
                        println!();
                    } else {
                        println!("Response: ");
                    }
                    break;
                }
                PromptEvent::Fault {
                    request_id: seen,
                    error,
                } if seen == request_id => {
                    eprintln!("error: {error}");
                    break;
                }
                _ => {}
            }
        }
    }
}

fn default_orch_bin() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os("MVP_ORCH_BIN") {
        return Ok(PathBuf::from(path));
    }
    let mut path = std::env::current_exe().map_err(|e| format!("current exe: {e}"))?;
    path.set_file_name("mvp-orchestrator");
    Ok(path)
}

fn node_bin_for_current_profile() -> Result<PathBuf, String> {
    let mut path = std::env::current_exe().map_err(|e| format!("current exe: {e}"))?;
    path.set_file_name("mvp-worker-node");
    let cwd = std::env::current_dir().map_err(|e| format!("current dir: {e}"))?;
    if let Ok(relative) = path.strip_prefix(&cwd) {
        Ok(relative.to_path_buf())
    } else {
        Ok(path)
    }
}

fn cargo_command() -> &'static str {
    "cargo"
}

fn ensure_orch_binary(config: &Config) -> Result<(), String> {
    let default_orch = default_orch_bin()?;
    if config.orch_bin != default_orch {
        if config.orch_bin.is_file() {
            let display_orch = display_workspace_path(&workspace_root(), &config.orch_bin);
            eprintln!(
                "mvp-one-node-chat: using custom orchestrator binary {display_orch}; skipping cargo build"
            );
            return Ok(());
        }
        let display_orch = display_workspace_path(&workspace_root(), &config.orch_bin);
        return Err(format!(
            "custom orchestrator binary {display_orch} does not exist"
        ));
    }

    let root = workspace_root();
    let rebuild_needed = orch_rebuild_needed(&config.orch_bin, &root, ORCH_REBUILD_INPUTS)?;
    let dashboard_feature_stale =
        config.dashboard && orch_local_e2e_marker_stale(&config.orch_bin, &root)?;
    if !rebuild_needed && !dashboard_feature_stale {
        eprintln!("mvp-one-node-chat: mvp-orchestrator is up to date; skipping cargo build");
        return Ok(());
    }

    run_status(
        cargo_command(),
        &[
            "build",
            "--quiet",
            "-p",
            "mvp-system",
            "--features",
            "local-e2e",
            "--bin",
            "mvp-orchestrator",
        ],
        "build mvp-orchestrator",
    )?;
    write_orch_local_e2e_marker(&config.orch_bin, &root)
}

fn orch_rebuild_needed(bin: &Path, root: &Path, inputs: &[&str]) -> Result<bool, String> {
    if !bin.is_file() {
        return Ok(true);
    }
    let bin_mtime = modified_time(root, bin)?;
    for input in inputs {
        let path = root.join(input);
        if latest_mtime(root, &path)? > bin_mtime {
            return Ok(true);
        }
    }
    Ok(false)
}

fn orch_local_e2e_marker(bin: &Path) -> PathBuf {
    let mut marker = bin.to_path_buf();
    let file_name = bin
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("mvp-orchestrator");
    marker.set_file_name(format!("{file_name}.local-e2e"));
    marker
}

fn orch_local_e2e_marker_stale(bin: &Path, root: &Path) -> Result<bool, String> {
    if !bin.is_file() {
        return Ok(true);
    }
    let marker = orch_local_e2e_marker(bin);
    if !marker.is_file() {
        return Ok(true);
    }
    Ok(modified_time(root, &marker)? < modified_time(root, bin)?)
}

fn write_orch_local_e2e_marker(bin: &Path, root: &Path) -> Result<(), String> {
    let marker = orch_local_e2e_marker(bin);
    let display = display_workspace_path(root, &marker);
    fs::write(&marker, b"local-e2e\n").map_err(|e| format!("write {display}: {e}"))
}

fn latest_mtime(root: &Path, path: &Path) -> Result<SystemTime, String> {
    let display = display_workspace_path(root, path);
    let metadata = fs::metadata(path).map_err(|e| format!("stat {display}: {e}"))?;
    let mut latest = metadata
        .modified()
        .map_err(|e| format!("modified time {display}: {e}"))?;
    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|e| format!("read dir {display}: {e}"))? {
            let entry = entry.map_err(|e| format!("read dir entry {display}: {e}"))?;
            let entry_mtime = latest_mtime(root, &entry.path())?;
            if entry_mtime > latest {
                latest = entry_mtime;
            }
        }
    }
    Ok(latest)
}

fn modified_time(root: &Path, path: &Path) -> Result<SystemTime, String> {
    let display = display_workspace_path(root, path);
    fs::metadata(path)
        .map_err(|e| format!("stat {display}: {e}"))?
        .modified()
        .map_err(|e| format!("modified time {display}: {e}"))
}

fn display_workspace_path(root: &Path, path: &Path) -> String {
    match path.strip_prefix(root) {
        Ok(relative) if relative.as_os_str().is_empty() => ".".to_owned(),
        Ok(relative) => format!("./{}", relative.display()),
        Err(_) => path.display().to_string(),
    }
}

fn display_user_path(path: &Path) -> String {
    let root = workspace_root();
    if let Ok(relative) = path.strip_prefix(&root) {
        if relative.as_os_str().is_empty() {
            return ".".to_owned();
        }
        return format!("./{}", relative.display());
    }
    if let Ok(cwd) = std::env::current_dir() {
        if let Ok(relative) = path.strip_prefix(&cwd) {
            if relative.as_os_str().is_empty() {
                return ".".to_owned();
            }
            return format!("./{}", relative.display());
        }
    }
    path.display().to_string()
}

fn run_status(program: &str, args: &[&str], label: &str) -> Result<(), String> {
    eprintln!("mvp-one-node-chat: {label}");
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

extern "C" fn request_stop(_: libc::c_int) {
    STOP_REQUESTED.store(true, Ordering::SeqCst);
}

fn install_signal_handlers() {
    #[cfg(target_os = "linux")]
    unsafe {
        libc::signal(libc::SIGINT, request_stop as *const () as usize);
        libc::signal(libc::SIGTERM, request_stop as *const () as usize);
    }
}

fn acknowledge_stop() {
    if !STOP_ACKNOWLEDGED.swap(true, Ordering::SeqCst) {
        eprintln!("mvp-one-node-chat: Ctrl-C received; stopping runtime...");
    }
}

fn report_interrupt_shutdown() {
    eprintln!("mvp-one-node-chat: runtime stopped");
}

#[derive(Clone, Debug)]
struct CachedModelConfig {
    host_path: PathBuf,
    display_path: PathBuf,
}

impl CachedModelConfig {
    fn from_arg(path: Option<String>) -> Result<Self, String> {
        let requested = path
            .map(PathBuf::from)
            .unwrap_or_else(default_cached_model_path);
        let host_path = requested
            .canonicalize()
            .map_err(|e| format!("resolve --cached-model path {}: {e}", requested.display()))?;
        let metadata = fs::metadata(&host_path)
            .map_err(|e| format!("stat cached model {}: {e}", requested.display()))?;
        if !metadata.is_file() {
            return Err(format!(
                "--cached-model must point at a file: {}",
                requested.display()
            ));
        }
        Ok(Self {
            host_path,
            display_path: requested,
        })
    }
}

fn default_cached_model_path() -> PathBuf {
    PathBuf::from(".")
        .join(REPO_MODEL_CACHE_DIR)
        .join(DEFAULT_CACHED_MODEL_FILE)
}

fn workspace_root() -> PathBuf {
    let git_root = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .stdin(Stdio::null())
        .output();
    if let Ok(output) = git_root {
        if output.status.success() {
            return PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
        }
    }
    std::env::current_dir().expect("current directory is available")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuntimeConfigProfile {
    Local,
    Deploy,
}

impl RuntimeConfigProfile {
    fn from_env() -> Result<Self, String> {
        match env_optional(MVP_RUNTIME_CONFIG_ENV).as_deref() {
            None | Some("local") => Ok(Self::Local),
            Some("deploy") => Ok(Self::Deploy),
            Some(other) => Err(format!(
                "unsupported {MVP_RUNTIME_CONFIG_ENV}={other:?}; use local or deploy"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Deploy => "deploy",
        }
    }

    fn default_provider(self) -> ProviderKind {
        match self {
            Self::Local => ProviderKind::Docker,
            Self::Deploy => ProviderKind::VastAi,
        }
    }
}

fn provider_from_env(config_profile: RuntimeConfigProfile) -> Result<ProviderKind, String> {
    match env_optional("MVP_NODE_PROVIDER").or_else(|| env_optional("MVP_PROVIDER")) {
        Some(value) => ProviderKind::parse_deploy(&value),
        None => Ok(config_profile.default_provider()),
    }
}

fn env_optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn relay_mode_from_sources(config_value: Option<&str>) -> Result<iroh::RelayMode, String> {
    match env_optional("MVP_IROH_RELAY_MODE")
        .as_deref()
        .or(config_value)
        .unwrap_or("default")
    {
        "disabled" => Ok(iroh::RelayMode::Disabled),
        "default" => Ok(iroh::RelayMode::Default),
        other => Err(format!(
            "unsupported relay mode {other:?}; use disabled or default"
        )),
    }
}

fn relay_mode_env_value(mode: &iroh::RelayMode) -> &'static str {
    match mode {
        iroh::RelayMode::Disabled => "disabled",
        _ => "default",
    }
}

fn node_image_provider(provider: ProviderKind) -> Result<NodeImageProvider, String> {
    match provider {
        ProviderKind::Docker => Ok(NodeImageProvider::Docker),
        ProviderKind::VastAi => Ok(NodeImageProvider::VastAi),
        ProviderKind::Mock => Err("mvp-one-node-chat does not support mock provider".to_owned()),
    }
}

fn env_bool_optional(name: &str) -> Result<Option<bool>, String> {
    match env_optional(name) {
        None => Ok(None),
        Some(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(Some(true)),
            "0" | "false" | "no" | "off" => Ok(Some(false)),
            _ => Err(format!(
                "invalid {name}={value:?}; use 1/0, true/false, yes/no, or on/off"
            )),
        },
    }
}

fn env_u64_optional(name: &str) -> Result<Option<u64>, String> {
    env_parse_optional(name)
}

fn env_u32_optional(name: &str) -> Result<Option<u32>, String> {
    env_parse_optional(name)
}

fn env_f64_optional(name: &str) -> Result<Option<f64>, String> {
    env_parse_optional(name)
}

fn env_parse_optional<T>(name: &str) -> Result<Option<T>, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match env_optional(name) {
        Some(value) => value
            .parse::<T>()
            .map(Some)
            .map_err(|e| format!("invalid {name}={value:?}: {e}")),
        None => Ok(None),
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

fn main() -> ExitCode {
    run_from_args(std::env::args().skip(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::time::Instant;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempWorkspace {
        root: PathBuf,
    }

    impl TempWorkspace {
        fn new(name: &str) -> Self {
            let counter = TEMP_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "mvp-one-node-chat-{name}-{}-{counter}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("create temp workspace");
            Self { root }
        }

        fn path(&self, relative: &str) -> PathBuf {
            self.root.join(relative)
        }

        fn write(&self, relative: &str, contents: &[u8]) -> PathBuf {
            let path = self.path(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("create temp parent directory");
            }
            fs::write(&path, contents).expect("write temp file");
            path
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn write_file_newer_than(path: &Path, contents: &[u8], older_than: SystemTime) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create temp parent directory");
        }

        let started = Instant::now();
        loop {
            fs::write(path, contents).expect("write temp file");
            let mtime = fs::metadata(path)
                .expect("stat temp file")
                .modified()
                .expect("read temp file mtime");
            if mtime > older_than {
                return;
            }
            assert!(
                started.elapsed() <= Duration::from_secs(3),
                "filesystem did not record a newer mtime for {}",
                path.display()
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    struct FakeOfferPreviewer;

    impl OfferPreviewer for FakeOfferPreviewer {
        fn preview(
            &self,
            _api_key: &str,
            _policy: &mvp_system::vastai_offer_preview::SelectionPolicy,
        ) -> Result<OfferPreview, String> {
            Ok(OfferPreview {
                offer_id: 7,
                host_id: Some(8),
                gpu_name: "RTX 4090".to_owned(),
                gpu_ram_mb: Some(24_000),
                dollars_per_hour: 0.42,
            })
        }
    }

    fn valid_vastai_config() -> ResolvedVastAiConfig {
        ResolvedVastAiConfig {
            api_key: "vast-key".to_owned(),
            relay_url: "https://relay.example.com".to_owned(),
            image: "ghcr.io/swactor/mvp-node:latest".to_owned(),
            bootstrap_command: "/usr/local/bin/mvp-node".to_owned(),
            disk_gb: Some(80),
            gpu_name: Some("RTX 4090".to_owned()),
            min_gpu_ram_mb: Some(16_000),
            min_down_mbps: Some(100.0),
            min_up_mbps: Some(25.0),
            min_reliability: Some(0.98),
            require_verified: Some(true),
            onstart: None,
            ssh_identity: Some("~/.ssh/swactor_vastai_ed25519".to_owned()),
        }
    }

    fn vastai_yes_chat_config() -> Config {
        Config {
            orch_bin: PathBuf::from("mvp-orchestrator"),
            orch_args: Vec::new(),
            rpc_addr: DEFAULT_RPC_ADDR.to_owned(),
            node_image: "ghcr.io/swactor/mvp-node:latest".to_owned(),
            config_profile: RuntimeConfigProfile::Deploy,
            provider: ProviderKind::VastAi,
            relay_mode: iroh::RelayMode::Default,
            relay_url: Some("https://relay.example.com".to_owned()),
            max_tokens: 128,
            dashboard: false,
            build_image: false,
            image_tag: Some("trial".to_owned()),
            push_image: true,
            force_image_refresh: false,
            cached_model: None,
            datastream_frame_log: None,
            vastai_yes: true,
            model_id: None,
            gguf_repo: None,
            gguf_file: None,
            gguf_revision: None,
            max_context: None,
            vastai: Some(valid_vastai_config()),
        }
    }

    #[test]
    fn approval_parser_accepts_only_y_or_yes() {
        for (input, expected) in [
            ("y", true),
            ("Y", true),
            (" yes ", true),
            ("YES", true),
            ("", false),
            ("n", false),
            ("no", false),
            ("yeah", false),
            ("yep", false),
            ("yes please", false),
        ] {
            assert_eq!(parse_approval(input), expected, "approval input {input:?}");
        }
    }

    #[test]
    fn parsed_args_handles_vastai_yes_and_config_path() {
        let parsed = ParsedArgs::parse(
            [
                "--vastai",
                "--yes",
                "--config",
                "/tmp/mvp-chat-config.toml",
                "--",
                "--orchestrator-flag",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("vastai flags parse");

        assert!(parsed.vastai);
        assert!(parsed.vastai_yes);
        assert_eq!(
            parsed.config_path.as_deref(),
            Some(Path::new("/tmp/mvp-chat-config.toml"))
        );
        assert_eq!(parsed.orch_args, vec!["--orchestrator-flag"]);
    }

    #[test]
    fn parsed_args_and_config_dump_logs_select_default_log_file() {
        let parsed = ParsedArgs::parse(["--dump-logs"].into_iter().map(str::to_owned))
            .expect("--dump-logs parses");

        assert!(parsed.dump_logs);

        let config = Config::from_args(
            ["--dump-logs", "--orch-bin", "/tmp/mvp-orchestrator"]
                .into_iter()
                .map(str::to_owned),
        )
        .expect("--dump-logs resolves chat config");

        assert_eq!(
            config.datastream_frame_log.as_deref(),
            Some(Path::new("mvp-chat.log"))
        );
    }

    #[test]
    fn dump_logs_conflicts_with_explicit_datastream_frame_log() {
        let result = Config::from_args(
            [
                "--dump-logs",
                "--datastream-frame-log",
                "/tmp/frames.jsonl",
                "--orch-bin",
                "/tmp/mvp-orchestrator",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        let error = match result {
            Ok(_) => panic!("conflicting datastream log destinations must fail"),
            Err(error) => error,
        };

        assert_eq!(
            error,
            "--dump-logs cannot be combined with --datastream-frame-log; use one datastream log destination"
        );
    }

    #[test]
    fn confirm_vastai_if_needed_accepts_yes_without_terminal_approval() {
        let config = vastai_yes_chat_config();

        confirm_vastai_if_needed(&config, FakeOfferPreviewer)
            .expect("--yes accepts the previewed Vast.ai rental");
    }

    #[test]
    fn orch_rebuild_missing_binary_requires_rebuild() {
        let workspace = TempWorkspace::new("missing-binary");
        workspace.write("src/main.rs", b"fn main() {}\n");
        let bin = workspace.path("target/debug/mvp-orchestrator");

        let needed = orch_rebuild_needed(&bin, &workspace.root, &["src/main.rs"])
            .expect("missing binary check succeeds");

        assert!(needed, "missing orchestrator binary must trigger rebuild");
    }

    #[test]
    fn orch_rebuild_binary_newer_than_input_skips_rebuild() {
        let workspace = TempWorkspace::new("fresh-binary");
        let input = workspace.write("src/main.rs", b"fn main() {}\n");
        let input_mtime = modified_time(&workspace.root, &input).expect("read input mtime");
        let bin = workspace.path("target/debug/mvp-orchestrator");
        write_file_newer_than(&bin, b"orchestrator binary\n", input_mtime);

        let needed = orch_rebuild_needed(&bin, &workspace.root, &["src/main.rs"])
            .expect("fresh binary check succeeds");

        assert!(
            !needed,
            "binary newer than every tracked input must skip rebuild"
        );
    }

    #[test]
    fn orch_rebuild_nested_directory_input_newer_than_binary_requires_rebuild() {
        let workspace = TempWorkspace::new("nested-newer-input");
        let nested_input = workspace.write("src/nested/orchestrator.rs", b"old source\n");
        let src_mtime =
            latest_mtime(&workspace.root, &workspace.path("src")).expect("read source tree mtime");
        let bin = workspace.path("target/debug/mvp-orchestrator");
        write_file_newer_than(&bin, b"orchestrator binary\n", src_mtime);
        let bin_mtime = modified_time(&workspace.root, &bin).expect("read binary mtime");
        write_file_newer_than(&nested_input, b"new source\n", bin_mtime);

        let needed = orch_rebuild_needed(&bin, &workspace.root, &["src"])
            .expect("stale binary check succeeds");

        assert!(
            needed,
            "newer file inside a tracked directory must trigger rebuild"
        );
    }
}
