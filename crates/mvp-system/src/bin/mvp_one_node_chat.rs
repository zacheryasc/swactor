use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, ExitCode, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use mvp_system::prompt_rpc::{PromptEvent, SubmitPrompt, write_json_line};
use serde_json::Value;

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

const DEFAULT_RPC_ADDR: &str = "127.0.0.1:19777";
const DEFAULT_NODE_IMAGE: &str = "swactor-mvp-node:latest";
const BASE_NODE_IMAGE: &str = "swactor-mvp-node-base:cuda12.6";
const DEFAULT_MAX_TOKENS: u32 = 64;
const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const ORCH_READY_TIMEOUT: Duration = Duration::from_secs(1_200);
const ORCH_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const CHAT_READ_TIMEOUT: Duration = Duration::from_millis(100);

static STOP_REQUESTED: AtomicBool = AtomicBool::new(false);

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
    let config = Config::from_args()?;
    prepare_runtime(&config)?;
    let mut orch = OrchChild::spawn(&config)?;
    let rpc_addr = orch.wait_ready(config.rpc_addr.clone())?;
    eprintln!("mvp-one-node-chat: prompt loop ready at {rpc_addr}");
    let result = run_chat_loop(&rpc_addr, config.max_tokens, config.timeout_ms);
    orch.shutdown();
    result
}

struct Config {
    orch_bin: PathBuf,
    orch_args: Vec<String>,
    rpc_addr: String,
    node_image: String,
    max_tokens: u32,
    timeout_ms: u64,
    dashboard: bool,
    build_image: bool,
}

impl Config {
    fn from_args() -> Result<Self, String> {
        let mut config = Self {
            orch_bin: default_orch_bin()?,
            orch_args: Vec::new(),
            rpc_addr: std::env::var("MVP_PROMPT_RPC_ADDR")
                .or_else(|_| std::env::var("MVP_PROMPT_RPC_BIND"))
                .unwrap_or_else(|_| DEFAULT_RPC_ADDR.to_owned()),
            node_image: std::env::var("MVP_NODE_IMAGE")
                .unwrap_or_else(|_| DEFAULT_NODE_IMAGE.to_owned()),
            max_tokens: env_u32("MVP_PROMPT_MAX_TOKENS", DEFAULT_MAX_TOKENS)?,
            timeout_ms: env_u64("MVP_PROMPT_TIMEOUT_MS", DEFAULT_TIMEOUT_MS)?,
            dashboard: env_bool("MVP_DASHBOARD", true)?,
            build_image: env_bool("MVP_BUILD_NODE_IMAGE", true)?,
        };

        let mut args = std::env::args().skip(1).peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--orch-bin" => config.orch_bin = PathBuf::from(next_arg(&mut args, "--orch-bin")?),
                "--addr" => config.rpc_addr = next_arg(&mut args, "--addr")?,
                "--image" => {
                    let image = next_arg(&mut args, "--image")?;
                    config.node_image = image.clone();
                    config.orch_args.push("--image".to_owned());
                    config.orch_args.push(image);
                }
                "--max-tokens" => config.max_tokens = parse_next(&mut args, "--max-tokens")?,
                "--timeout-ms" => config.timeout_ms = parse_next(&mut args, "--timeout-ms")?,
                "--dashboard" => config.dashboard = true,
                "--no-dashboard" => config.dashboard = false,
                "--no-build-image" => config.build_image = false,
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

struct OrchChild {
    child: Child,
    stdin: Option<ChildStdin>,
    ready_rx: mpsc::Receiver<String>,
    cleaned: bool,
}

impl OrchChild {
    fn spawn(config: &Config) -> Result<Self, String> {
        let mut command = Command::new(&config.orch_bin);
        command
            .args(&config.orch_args)
            .env("MVP_NODE_IMAGE", &config.node_image)
            .env("MVP_PROMPT_RPC_BIND", &config.rpc_addr)
            .env("MVP_PROMPT_RPC_ADDR", &config.rpc_addr)
            .env("MVP_PROMPT_MAX_TOKENS", config.max_tokens.to_string())
            .env("MVP_PROMPT_TIMEOUT_MS", config.timeout_ms.to_string())
            .env("MVP_DASHBOARD", if config.dashboard { "1" } else { "0" })
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
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
        let mut child = command
            .spawn()
            .map_err(|e| format!("spawn {}: {e}", config.orch_bin.display()))?;
        let stdin = child.stdin.take();
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "orchestrator stdout missing".to_owned())?;
        let (ready_tx, ready_rx) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                eprintln!("mvp-orch-one-node: {line}");
                if let Ok(value) = serde_json::from_str::<Value>(&line)
                    && value.get("type").and_then(Value::as_str) == Some("prompt_loop_ready")
                    && let Some(addr) = value.get("addr").and_then(Value::as_str)
                {
                    let _ = ready_tx.send(addr.to_owned());
                }
            }
        });
        Ok(Self {
            child,
            stdin,
            ready_rx,
            cleaned: false,
        })
    }

    fn wait_ready(&mut self, fallback_addr: String) -> Result<String, String> {
        let start = Instant::now();
        loop {
            if STOP_REQUESTED.load(Ordering::SeqCst) {
                return Err("interrupted before orchestrator became ready".to_owned());
            }
            if let Ok(addr) = self.ready_rx.recv_timeout(Duration::from_millis(100)) {
                return Ok(addr);
            }
            if let Some(status) = self
                .child
                .try_wait()
                .map_err(|e| format!("poll orchestrator: {e}"))?
            {
                return Err(format!(
                    "orchestrator exited before prompt loop ready: {status}"
                ));
            }
            if start.elapsed() > ORCH_READY_TIMEOUT {
                return Err(format!(
                    "timed out waiting for orchestrator prompt loop; try connecting to {fallback_addr} if it is still booting"
                ));
            }
        }
    }

    fn shutdown(&mut self) {
        if self.cleaned {
            return;
        }
        self.cleaned = true;
        if let Some(stdin) = self.stdin.as_mut() {
            let _ = writeln!(stdin, "shutdown");
            let _ = stdin.flush();
        }
        let start = Instant::now();
        while start.elapsed() < ORCH_SHUTDOWN_TIMEOUT {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => thread::sleep(Duration::from_millis(100)),
                Err(_) => break,
            }
        }
        terminate_process_group(self.child.id());
        let _ = self.child.wait();
    }
}

impl Drop for OrchChild {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn prepare_runtime(config: &Config) -> Result<(), String> {
    run_status(
        cargo_command(),
        &[
            "build",
            "-p",
            "mvp-system",
            "--features",
            "local-e2e",
            "--bin",
            "mvp-orch-one-node",
        ],
        "build mvp-orch-one-node",
    )?;
    if !config.build_image {
        eprintln!("mvp-one-node-chat: skipping node image rebuild (--no-build-image)");
        return Ok(());
    }
    run_status(
        cargo_command(),
        &["build", "-p", "mvp-system", "--bin", "mvp-node"],
        "build mvp-node",
    )?;
    if !docker_image_exists(BASE_NODE_IMAGE) {
        run_status(
            "docker",
            &[
                "build",
                "-f",
                "apps/mvp-node/Dockerfile.base",
                "-t",
                BASE_NODE_IMAGE,
                ".",
            ],
            "build mvp node base image",
        )?;
    }
    let node_bin = node_bin_for_current_profile()?;
    run_status(
        "docker",
        &[
            "build",
            "-f",
            "apps/mvp-node/Dockerfile",
            "--build-arg",
            &format!("BASE_IMAGE={BASE_NODE_IMAGE}"),
            "--build-arg",
            &format!("MVP_NODE_BIN={}", node_bin.display()),
            "-t",
            &config.node_image,
            ".",
        ],
        "build mvp node image",
    )
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
        "mvp-one-node-chat: Ctrl-C cleans up the orchestrator and Docker node; /exit exits cleanly"
    );

    loop {
        if STOP_REQUESTED.load(Ordering::SeqCst) {
            return Ok(());
        }
        print!("> ");
        io::stdout()
            .flush()
            .map_err(|e| format!("flush prompt: {e}"))?;
        let prompt = loop {
            if STOP_REQUESTED.load(Ordering::SeqCst) {
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

        loop {
            if STOP_REQUESTED.load(Ordering::SeqCst) {
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
                    print!("{text}");
                    io::stdout()
                        .flush()
                        .map_err(|e| format!("flush response text: {e}"))?;
                }
                PromptEvent::Done {
                    request_id: seen,
                    tokens_generated,
                    elapsed_ms,
                    ..
                } if seen == request_id => {
                    println!();
                    eprintln!(
                        "mvp-one-node-chat: done request={} tokens={} elapsed_ms={}",
                        seen, tokens_generated, elapsed_ms
                    );
                    break;
                }
                PromptEvent::Fault {
                    request_id: seen,
                    error,
                } if seen == request_id => {
                    eprintln!("mvp-one-node-chat: fault request={seen}: {error}");
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

fn docker_image_exists(image: &str) -> bool {
    Command::new("docker")
        .args(["image", "inspect", image])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
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

fn terminate_process_group(pid: u32) {
    #[cfg(target_os = "linux")]
    unsafe {
        let pgid = -(pid as libc::pid_t);
        let _ = libc::kill(pgid, libc::SIGTERM);
        thread::sleep(Duration::from_secs(2));
        let _ = libc::kill(pgid, libc::SIGKILL);
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
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
