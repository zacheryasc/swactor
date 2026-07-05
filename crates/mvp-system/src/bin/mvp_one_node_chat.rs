use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, ExitCode, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use mvp_system::node_image::{NodeImageProvider, NodeImageRequest, prepare_node_image};
use mvp_system::node_provisioning::ProviderKind;
use mvp_system::prompt_rpc::{PromptEvent, SubmitPrompt, write_json_line};
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
const DEFAULT_TIMEOUT_MS: u64 = 120_000;
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
const ORCH_READY_TIMEOUT: Duration = Duration::from_secs(1_200);
const ORCH_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const INTERRUPT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const FORCE_KILL_DELAY: Duration = Duration::from_millis(500);
const CHAT_READ_TIMEOUT: Duration = Duration::from_millis(100);

static STOP_REQUESTED: AtomicBool = AtomicBool::new(false);
static STOP_ACKNOWLEDGED: AtomicBool = AtomicBool::new(false);

fn main() -> ExitCode {
    install_signal_handlers();
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mvp-one-node-chat: {error}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), String> {
    let mut config = Config::from_args()?;
    let image_ref = prepare_runtime(&config)?;
    let frame_log = configure_progress_frame_log(&mut config)?;
    let mut progress = StartupProgress::new(&frame_log);
    let mut orch = OrchChild::spawn(&config, &image_ref)?;
    let rpc_addr = match orch.wait_ready(config.rpc_addr.clone(), &mut progress) {
        Ok(addr) => addr,
        Err(_) if STOP_REQUESTED.load(Ordering::SeqCst) => {
            acknowledge_stop();
            let forced = orch.shutdown(true);
            report_interrupt_shutdown(forced);
            return Ok(());
        }
        Err(error) => {
            progress.poll();
            return Err(progress.failure_summary().unwrap_or(error));
        }
    };
    progress.poll();
    println!("model successfully loaded.");
    let result = run_chat_loop(&rpc_addr, config.max_tokens, config.timeout_ms);
    let interrupted = STOP_REQUESTED.load(Ordering::SeqCst);
    let forced = orch.shutdown(interrupted);
    if interrupted {
        report_interrupt_shutdown(forced);
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
    max_tokens: u32,
    timeout_ms: u64,
    dashboard: bool,
    build_image: bool,
    image_tag: Option<String>,
    push_image: bool,
    force_image_refresh: bool,
    cached_model: Option<CachedModelConfig>,
    datastream_frame_log: Option<PathBuf>,
}
impl Config {
    fn from_args() -> Result<Self, String> {
        let config_profile = RuntimeConfigProfile::from_env()?;
        let mut config = Self {
            orch_bin: default_orch_bin()?,
            orch_args: Vec::new(),
            rpc_addr: std::env::var("MVP_PROMPT_RPC_ADDR")
                .or_else(|_| std::env::var("MVP_PROMPT_RPC_BIND"))
                .unwrap_or_else(|_| DEFAULT_RPC_ADDR.to_owned()),
            node_image: std::env::var("MVP_NODE_IMAGE")
                .unwrap_or_else(|_| DEFAULT_NODE_IMAGE.to_owned()),
            config_profile,
            provider: provider_from_env(config_profile)?,
            relay_mode: relay_mode_from_env()?,
            max_tokens: env_u32("MVP_PROMPT_MAX_TOKENS", DEFAULT_MAX_TOKENS)?,
            timeout_ms: env_u64("MVP_PROMPT_TIMEOUT_MS", DEFAULT_TIMEOUT_MS)?,
            dashboard: env_bool("MVP_DASHBOARD", true)?,
            build_image: env_bool("MVP_BUILD_NODE_IMAGE", true)?,
            image_tag: std::env::var("MVP_NODE_IMAGE_TAG")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            push_image: env_bool("MVP_PUSH_NODE_IMAGE", false)?,
            force_image_refresh: env_bool("MVP_FORCE_NODE_IMAGE_REFRESH", false)?,
            cached_model: None,
            datastream_frame_log: env_optional("MVP_DATASTREAM_FRAME_LOG").map(PathBuf::from),
        };

        let mut args = std::env::args().skip(1).peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--orch-bin" => config.orch_bin = PathBuf::from(next_arg(&mut args, "--orch-bin")?),
                "--addr" => config.rpc_addr = next_arg(&mut args, "--addr")?,
                "--image" => {
                    config.node_image = next_arg(&mut args, "--image")?;
                }
                "--max-tokens" => config.max_tokens = parse_next(&mut args, "--max-tokens")?,
                "--timeout-ms" => config.timeout_ms = parse_next(&mut args, "--timeout-ms")?,
                "--datastream-frame-log" => {
                    config.datastream_frame_log = Some(PathBuf::from(next_arg(
                        &mut args,
                        "--datastream-frame-log",
                    )?));
                }
                "--dashboard" => config.dashboard = true,
                "--no-dashboard" => config.dashboard = false,
                "--no-build-image" => config.build_image = false,
                "--image-tag" => config.image_tag = Some(next_arg(&mut args, "--image-tag")?),
                "--push-image" => config.push_image = true,
                "--no-push-image" => config.push_image = false,
                "--force-image-refresh" => config.force_image_refresh = true,
                "--cached-model" => {
                    let path = args.next_if(|value| !value.starts_with("--"));
                    config.cached_model = Some(CachedModelConfig::from_arg(path)?);
                }
                "--" => {
                    config.orch_args.extend(args);
                    break;
                }
                other => config.orch_args.push(other.to_owned()),
            }
        }
        Ok(config)
    }
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
            .env("MVP_PROMPT_TIMEOUT_MS", config.timeout_ms.to_string())
            .env("MVP_DASHBOARD", if config.dashboard { "1" } else { "0" })
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
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
        let start = Instant::now();
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
            if start.elapsed() > ORCH_READY_TIMEOUT {
                progress.poll();
                if let Some(failure) = progress.failure_summary() {
                    return Err(failure);
                }
                return Err(format!(
                    "timed out waiting for orchestrator prompt RPC at {rpc_addr}"
                ));
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn shutdown(&mut self, interrupt: bool) -> bool {
        if self.cleaned {
            return false;
        }
        self.cleaned = true;
        if let Some(mut stdin) = self.stdin.take() {
            let _ = writeln!(stdin, "shutdown");
            let _ = stdin.flush();
        }
        let timeout = if interrupt {
            INTERRUPT_SHUTDOWN_TIMEOUT
        } else {
            ORCH_SHUTDOWN_TIMEOUT
        };
        let start = Instant::now();
        while start.elapsed() < timeout {
            match self.child.try_wait() {
                Ok(Some(_)) => return false,
                Ok(None) => thread::sleep(Duration::from_millis(100)),
                Err(_) => break,
            }
        }
        terminate_process_group(self.child.id(), FORCE_KILL_DELAY);
        let _ = self.child.wait();
        true
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

fn run_chat_loop(addr: &str, max_tokens: u32, timeout_ms: u64) -> Result<(), String> {
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
                timeout_ms,
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
    path.set_file_name("mvp-orch-one-node");
    Ok(path)
}

fn node_bin_for_current_profile() -> Result<PathBuf, String> {
    let mut path = std::env::current_exe().map_err(|e| format!("current exe: {e}"))?;
    path.set_file_name("mvp-node");
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
        eprintln!("mvp-one-node-chat: mvp-orch-one-node is up to date; skipping cargo build");
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
            "mvp-orch-one-node",
        ],
        "build mvp-orch-one-node",
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
        .unwrap_or("mvp-orch-one-node");
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

fn report_interrupt_shutdown(forced: bool) {
    if forced {
        eprintln!("mvp-one-node-chat: runtime did not stop in 5s; force-killed");
    } else {
        eprintln!("mvp-one-node-chat: runtime stopped");
    }
}

fn terminate_process_group(pid: u32, kill_after: Duration) {
    #[cfg(target_os = "linux")]
    unsafe {
        let pgid = -(pid as libc::pid_t);
        let _ = libc::kill(pgid, libc::SIGTERM);
        thread::sleep(kill_after);
        let _ = libc::kill(pgid, libc::SIGKILL);
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (pid, kill_after);
    }
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

fn relay_mode_from_env() -> Result<iroh::RelayMode, String> {
    match env_optional("MVP_IROH_RELAY_MODE")
        .as_deref()
        .unwrap_or("default")
    {
        "disabled" => Ok(iroh::RelayMode::Disabled),
        "default" => Ok(iroh::RelayMode::Default),
        other => Err(format!(
            "unsupported MVP_IROH_RELAY_MODE={other:?}; use disabled or default"
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

fn env_bool(name: &str, default: bool) -> Result<bool, String> {
    match std::env::var(name).ok().filter(|value| !value.is_empty()) {
        None => Ok(default),
        Some(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(format!(
                "invalid {name}={value:?}; use 1/0, true/false, yes/no, or on/off"
            )),
        },
    }
}

fn env_u64(name: &str, default: u64) -> Result<u64, String> {
    match std::env::var(name).ok().filter(|value| !value.is_empty()) {
        Some(value) => value
            .parse::<u64>()
            .map_err(|e| format!("invalid {name}={value:?}: {e}")),
        None => Ok(default),
    }
}

fn env_u32(name: &str, default: u32) -> Result<u32, String> {
    match std::env::var(name).ok().filter(|value| !value.is_empty()) {
        Some(value) => value
            .parse::<u32>()
            .map_err(|e| format!("invalid {name}={value:?}: {e}")),
        None => Ok(default),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

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

    #[test]
    fn orch_rebuild_missing_binary_requires_rebuild() {
        let workspace = TempWorkspace::new("missing-binary");
        workspace.write("src/main.rs", b"fn main() {}\n");
        let bin = workspace.path("target/debug/mvp-orch-one-node");

        let needed = orch_rebuild_needed(&bin, &workspace.root, &["src/main.rs"])
            .expect("missing binary check succeeds");

        assert!(needed, "missing orchestrator binary must trigger rebuild");
    }

    #[test]
    fn orch_rebuild_binary_newer_than_input_skips_rebuild() {
        let workspace = TempWorkspace::new("fresh-binary");
        let input = workspace.write("src/main.rs", b"fn main() {}\n");
        let input_mtime = modified_time(&workspace.root, &input).expect("read input mtime");
        let bin = workspace.path("target/debug/mvp-orch-one-node");
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
        let bin = workspace.path("target/debug/mvp-orch-one-node");
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
