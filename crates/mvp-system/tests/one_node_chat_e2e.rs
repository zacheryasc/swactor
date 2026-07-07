use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

const TEST_WATCHDOG: Duration = Duration::from_secs(1_800);
const PROMPT_WATCHDOG: Duration = Duration::from_secs(600);
const SHUTDOWN_WATCHDOG: Duration = Duration::from_secs(60);
const DASHBOARD_ADDR: &str = "127.0.0.1:9090";
const DEFAULT_CONTAINER: &str = "mvp-orchestrator-1-1";

#[test]
fn one_node_chat_docker_cuda_e2e() {
    let root = workspace_root();
    require_docker(&root);

    let mut command = Command::new("cargo");
    command
        .current_dir(&root)
        .args(["mvp-chat"])
        .env("MVP_RUNTIME_CONFIG", "local")
        .env("MVP_IROH_RELAY_MODE", "disabled")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
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

    let mut child = command.spawn().expect("spawn cargo mvp-chat");
    let mut stdin = child.stdin.take().expect("cargo mvp-chat stdin");
    let stdout = Arc::new(Mutex::new(String::new()));
    let stderr = Arc::new(Mutex::new(String::new()));
    let stdout_reader = spawn_capture(child.stdout.take().expect("stdout"), Arc::clone(&stdout));
    let stderr_reader = spawn_capture(child.stderr.take().expect("stderr"), Arc::clone(&stderr));

    let mut result = run_full_flow(&mut child, &mut stdin, &stdout);
    if result.is_err() {
        request_child_interrupt(&child);
        let _ = wait_child(&mut child, SHUTDOWN_WATCHDOG);
        let _ = child.kill();
        let _ = child.wait();
    }
    let _ = stdout_reader.join();
    let _ = stderr_reader.join();
    if result.is_ok() {
        result = assert_no_lower_layer_terminal_leaks(&stdout, &stderr);
    }
    assert_container_removed(&root, DEFAULT_CONTAINER);

    if let Err(error) = result {
        panic!(
            "{error}\nstdout:\n{}\nstderr:\n{}\ndashboard frames:\n{}",
            snapshot(&stdout),
            snapshot(&stderr),
            dashboard_snapshot().unwrap_or_else(|err| format!("<dashboard unavailable: {err}>"))
        );
    }
}

fn run_full_flow(
    child: &mut Child,
    stdin: &mut impl Write,
    stdout: &Arc<Mutex<String>>,
) -> Result<(), String> {
    wait_for_child_or(TEST_WATCHDOG, child, dashboard_responding)
        .map_err(|e| format!("dashboard API not live: {e}"))?;
    wait_for_child_or(TEST_WATCHDOG, child, || {
        dashboard_has_channel_or_payload("mvp.provisioning.logs", "mvp-entrypoint")
    })
    .map_err(|e| format!("provisioning frames not visible in dashboard: {e}"))?;
    wait_for_child_or(TEST_WATCHDOG, child, || {
        dashboard_has_frame("mvp.worker.weights", "GgufDownloadProgress")
    })
    .map_err(|e| format!("GGUF download progress not visible in dashboard: {e}"))?;
    wait_for_child_or(TEST_WATCHDOG, child, || {
        dashboard_has_frame("mvp.worker.weights", "WeightsLoaded")
    })
    .map_err(|e| format!("WeightsLoaded not visible in dashboard: {e}"))?;
    wait_for_child_or(TEST_WATCHDOG, child, || prompt_visible(stdout))
        .map_err(|e| format!("chat prompt not visible: {e}"))?;

    writeln!(stdin, "hello from full cargo mvp-chat e2e")
        .map_err(|e| format!("write prompt: {e}"))?;
    stdin.flush().map_err(|e| format!("flush prompt: {e}"))?;
    wait_for_child_or(PROMPT_WATCHDOG, child, || {
        stdout_contains(stdout, "decoding...")
    })
    .map_err(|e| format!("prompt was not submitted to chat loop: {e}"))?;
    wait_for_child_or(PROMPT_WATCHDOG, child, || response_text_visible(stdout))
        .map_err(|e| format!("decoded response text not visible: {e}"))?;
    wait_for_child_or(PROMPT_WATCHDOG, child, || {
        dashboard_has_frame("mvp.worker.prompt", "PromptCompleted")
    })
    .map_err(|e| format!("prompt result not visible in dashboard: {e}"))?;
    wait_for_child_or(PROMPT_WATCHDOG, child, dashboard_has_orch_prompt_lifecycle)
        .map_err(|e| format!("orchestrator prompt lifecycle not visible in dashboard: {e}"))?;
    wait_for_child_or(PROMPT_WATCHDOG, child, || prompt_count(stdout) >= 2)
        .map_err(|e| format!("chat prompt did not return after response: {e}"))?;
    request_child_interrupt(child);
    let status = wait_child(child, SHUTDOWN_WATCHDOG)
        .ok_or_else(|| "cargo mvp-chat did not exit after Ctrl-C".to_owned())?;
    if status.success() || status.code() == Some(130) || status.signal_name() == Some("SIGINT") {
        Ok(())
    } else {
        Err(format!("cargo mvp-chat exited with {status}"))
    }
}

fn wait_for_child_or(
    watchdog: Duration,
    child: &mut Child,
    mut predicate: impl FnMut() -> bool,
) -> Result<(), String> {
    let start = Instant::now();
    while start.elapsed() < watchdog {
        if predicate() {
            return Ok(());
        }
        if let Some(status) = child.try_wait().map_err(|e| format!("poll child: {e}"))? {
            return Err(format!("child exited early with {status}"));
        }
        thread::sleep(Duration::from_millis(250));
    }
    Err("test watchdog".to_owned())
}

fn spawn_capture(
    mut reader: impl Read + Send + 'static,
    out: Arc<Mutex<String>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut buf = [0_u8; 8192];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => out
                    .lock()
                    .expect("capture mutex")
                    .push_str(&String::from_utf8_lossy(&buf[..n])),
                Err(_) => break,
            }
        }
    })
}

fn dashboard_responding() -> bool {
    dashboard_frames().is_ok()
}

fn dashboard_has_channel_or_payload(channel_substr: &str, payload_substr: &str) -> bool {
    dashboard_frames()
        .map(|frames| {
            frames.iter().any(|frame| {
                frame.channel.contains(channel_substr) || frame.payload.contains(payload_substr)
            })
        })
        .unwrap_or(false)
}

fn dashboard_has_frame(channel_substr: &str, payload_substr: &str) -> bool {
    dashboard_frames()
        .map(|frames| {
            frames.iter().any(|frame| {
                frame.channel.contains(channel_substr) && frame.payload.contains(payload_substr)
            })
        })
        .unwrap_or(false)
}

fn dashboard_has_orch_prompt_lifecycle() -> bool {
    dashboard_frames()
        .map(|frames| {
            let events = frames
                .iter()
                .filter_map(orch_prompt_observation)
                .collect::<Vec<_>>();

            events.iter().any(|prompt_work| {
                prompt_work.phase == "prompt_work"
                    && prompt_work.status == "observed"
                    && events.iter().any(|event| {
                        same_prompt(prompt_work, event)
                            && event.phase == "node_prompt_send"
                            && event.status == "ready"
                    })
                    && events.iter().any(|event| {
                        same_prompt(prompt_work, event)
                            && event.phase == "node_prompt_event"
                            && event.status == "observed"
                            && event.detail_event.as_deref() == Some("Done")
                    })
                    && events.iter().any(|event| {
                        same_prompt(prompt_work, event)
                            && event.phase == "prompt_complete"
                            && event.status == "ready"
                            && event.detail_event.as_deref() == Some("Done")
                    })
            })
        })
        .unwrap_or(false)
}

fn orch_prompt_observation(frame: &SeenFrame) -> Option<OrchPromptObservation> {
    if frame.channel != "mvp.orch.prompt" {
        return None;
    }

    let value = serde_json::from_str::<Value>(&frame.payload).ok()?;
    if value.get("type").and_then(Value::as_str)? != "OrchPromptEvent" {
        return None;
    }
    let detail = value.get("detail")?;

    Some(OrchPromptObservation {
        phase: value.get("phase").and_then(Value::as_str)?.to_owned(),
        status: value.get("status").and_then(Value::as_str)?.to_owned(),
        run_id: value.get("run_id").and_then(Value::as_u64)?,
        node_id: value.get("node_id").and_then(Value::as_u64)?,
        request_id: value.get("request_id").and_then(Value::as_u64)?,
        detail_event: detail
            .get("event")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

fn same_prompt(left: &OrchPromptObservation, right: &OrchPromptObservation) -> bool {
    left.run_id == right.run_id
        && left.node_id == right.node_id
        && left.request_id == right.request_id
}

#[derive(Debug)]
struct OrchPromptObservation {
    phase: String,
    status: String,
    run_id: u64,
    node_id: u64,
    request_id: u64,
    detail_event: Option<String>,
}

#[derive(Debug)]
struct SeenFrame {
    channel: String,
    payload: String,
}

fn dashboard_frames() -> Result<Vec<SeenFrame>, String> {
    let response = http_get("/api/frames")?;
    let (_, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| "HTTP response missing body".to_owned())?;
    let values = serde_json::from_str::<Vec<Value>>(body)
        .map_err(|e| format!("parse dashboard frames JSON: {e}; body={body:?}"))?;
    Ok(values
        .into_iter()
        .map(|value| SeenFrame {
            channel: value
                .get("channel")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            payload: decode_payload(value.get("payload")).unwrap_or_default(),
        })
        .collect())
}

fn dashboard_snapshot() -> Result<String, String> {
    let mut frames = dashboard_frames()?;
    let keep = frames.len().saturating_sub(40);
    frames.drain(0..keep);
    Ok(frames
        .into_iter()
        .map(|frame| format!("{} {}", frame.channel, frame.payload))
        .collect::<Vec<_>>()
        .join("\n"))
}

fn decode_payload(value: Option<&Value>) -> Option<String> {
    let bytes = value?
        .as_array()?
        .iter()
        .map(|byte| byte.as_u64().map(|n| n as u8))
        .collect::<Option<Vec<_>>>()?;
    Some(String::from_utf8_lossy(&bytes).to_string())
}

fn http_get(path: &str) -> Result<String, String> {
    let mut stream = TcpStream::connect(DASHBOARD_ADDR)
        .map_err(|e| format!("connect dashboard {DASHBOARD_ADDR}: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|e| format!("set read watchdog: {e}"))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .map_err(|e| format!("write HTTP request: {e}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| format!("read HTTP response: {e}"))?;
    if response.starts_with("HTTP/1.1 200") {
        Ok(response)
    } else {
        Err(format!("non-200 dashboard response: {response:?}"))
    }
}

fn stdout_contains(stdout: &Arc<Mutex<String>>, needle: &str) -> bool {
    snapshot(stdout).contains(needle)
}

fn prompt_visible(stdout: &Arc<Mutex<String>>) -> bool {
    snapshot(stdout).contains("prompt:> ")
}

fn prompt_count(stdout: &Arc<Mutex<String>>) -> usize {
    snapshot(stdout).matches("prompt:> ").count()
}

fn response_text_visible(stdout: &Arc<Mutex<String>>) -> bool {
    snapshot(stdout)
        .split("Response: ")
        .skip(1)
        .any(|text| !text.lines().next().unwrap_or_default().trim().is_empty())
}

fn assert_no_lower_layer_terminal_leaks(
    stdout: &Arc<Mutex<String>>,
    stderr: &Arc<Mutex<String>>,
) -> Result<(), String> {
    let leaks = lower_layer_leak_lines("stdout", &snapshot(stdout))
        .into_iter()
        .chain(lower_layer_leak_lines("stderr", &snapshot(stderr)))
        .collect::<Vec<_>>();
    if leaks.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "lower-layer runtime output leaked to terminal:\n{}",
            leaks.join("\n")
        ))
    }
}

fn lower_layer_leak_lines(stream: &str, output: &str) -> Vec<String> {
    output
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim_start();
            let leaked = trimmed.contains("prompt_loop_ready")
                || trimmed.contains("dashboard_ready")
                || trimmed.starts_with("mvp-orchestrator:")
                || trimmed.starts_with("mvp-worker-node:")
                || trimmed.starts_with("mvp_tinygrad_worker:");
            leaked.then(|| format!("{stream}: {line}"))
        })
        .collect()
}
fn snapshot(buf: &Arc<Mutex<String>>) -> String {
    buf.lock().expect("capture mutex").clone()
}

fn wait_child(child: &mut Child, watchdog: Duration) -> Option<std::process::ExitStatus> {
    let start = Instant::now();
    while start.elapsed() < watchdog {
        if let Some(status) = child.try_wait().expect("poll child") {
            return Some(status);
        }
        thread::sleep(Duration::from_millis(100));
    }
    None
}

fn request_child_interrupt(child: &Child) {
    #[cfg(target_os = "linux")]
    unsafe {
        let _ = libc::kill(-(child.id() as libc::pid_t), libc::SIGINT);
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = child;
    }
}

trait ExitStatusSignalName {
    fn signal_name(&self) -> Option<&'static str>;
}

impl ExitStatusSignalName for std::process::ExitStatus {
    fn signal_name(&self) -> Option<&'static str> {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::process::ExitStatusExt;
            match self.signal() {
                Some(libc::SIGINT) => Some("SIGINT"),
                Some(libc::SIGTERM) => Some("SIGTERM"),
                _ => None,
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = self;
            None
        }
    }
}

fn require_docker(root: &std::path::Path) {
    let version = Command::new("docker")
        .current_dir(root)
        .arg("version")
        .output()
        .expect("run docker version");
    assert!(
        version.status.success(),
        "docker is not available\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&version.stdout),
        String::from_utf8_lossy(&version.stderr)
    );
}

fn assert_container_removed(root: &std::path::Path, container: &str) {
    let output = Command::new("docker")
        .current_dir(root)
        .args([
            "ps",
            "-a",
            "--filter",
            &format!("name=^{container}$"),
            "--format",
            "{{.Names}}",
        ])
        .output()
        .expect("run docker ps");
    assert!(
        output.status.success(),
        "docker ps failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).trim().is_empty(),
        "container {container} still exists"
    );
}

fn workspace_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .to_path_buf()
}
