use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

const TEST_TIMEOUT: Duration = Duration::from_secs(1_800);
const PROMPT_TIMEOUT: Duration = Duration::from_secs(600);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60);
const DASHBOARD_ADDR: &str = "127.0.0.1:9090";
const DEFAULT_CONTAINER: &str = "mvp-orch-one-node-1-1";

#[test]
fn one_node_chat_docker_cuda_e2e() {
    let root = workspace_root();
    require_docker(&root);

    let mut command = Command::new("cargo");
    command
        .current_dir(&root)
        .args(["mvp-chat"])
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

    let result = run_full_flow(&mut child, &mut stdin, &stdout, &stderr);
    if result.is_err() {
        request_child_interrupt(&child);
        let _ = wait_child(&mut child, SHUTDOWN_TIMEOUT);
        let _ = child.kill();
        let _ = child.wait();
    }
    let _ = stdout_reader.join();
    let _ = stderr_reader.join();
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
    stderr: &Arc<Mutex<String>>,
) -> Result<(), String> {
    wait_for_child_or(TEST_TIMEOUT, child, || {
        stderr_contains(stderr, "dashboard_ready")
    })
    .map_err(|e| format!("dashboard_ready not observed: {e}"))?;
    wait_for_child_or(TEST_TIMEOUT, child, dashboard_responding)
        .map_err(|e| format!("dashboard API not live: {e}"))?;
    wait_for_child_or(TEST_TIMEOUT, child, || {
        dashboard_has_channel_or_payload("mvp.provisioning.logs", "mvp-entrypoint")
    })
    .map_err(|e| format!("provisioning frames not visible in dashboard: {e}"))?;
    wait_for_child_or(TEST_TIMEOUT, child, || {
        dashboard_has_frame("mvp.worker.weights", "GgufDownloadProgress")
    })
    .map_err(|e| format!("GGUF download progress not visible in dashboard: {e}"))?;
    wait_for_child_or(TEST_TIMEOUT, child, || {
        dashboard_has_frame("mvp.worker.weights", "WeightsLoaded")
    })
    .map_err(|e| format!("WeightsLoaded not visible in dashboard: {e}"))?;
    wait_for_child_or(TEST_TIMEOUT, child, || {
        stderr_contains(stderr, "prompt_loop_ready")
    })
    .map_err(|e| format!("prompt_loop_ready not observed: {e}"))?;

    writeln!(stdin, "hello from full cargo mvp-chat e2e")
        .map_err(|e| format!("write prompt: {e}"))?;
    stdin.flush().map_err(|e| format!("flush prompt: {e}"))?;
    wait_for_child_or(PROMPT_TIMEOUT, child, || stdout_contains(stdout, ">"))
        .map_err(|e| format!("prompt marker not visible: {e}"))?;
    wait_for_child_or(PROMPT_TIMEOUT, child, || response_text_visible(stdout))
        .map_err(|e| format!("decoded response text not visible: {e}"))?;
    wait_for_child_or(PROMPT_TIMEOUT, child, || {
        stderr_contains(stderr, "done request=")
    })
    .map_err(|e| format!("prompt did not complete: {e}"))?;
    wait_for_child_or(PROMPT_TIMEOUT, child, || {
        dashboard_has_frame("mvp.worker.prompt", "PromptCompleted")
    })
    .map_err(|e| format!("prompt result not visible in dashboard: {e}"))?;

    request_child_interrupt(child);
    let status = wait_child(child, SHUTDOWN_TIMEOUT)
        .ok_or_else(|| "cargo mvp-chat did not exit after Ctrl-C".to_owned())?;
    if status.success() || status.code() == Some(130) || status.signal_name() == Some("SIGINT") {
        Ok(())
    } else {
        Err(format!("cargo mvp-chat exited with {status}"))
    }
}

fn wait_for_child_or(
    timeout: Duration,
    child: &mut Child,
    mut predicate: impl FnMut() -> bool,
) -> Result<(), String> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if predicate() {
            return Ok(());
        }
        if let Some(status) = child.try_wait().map_err(|e| format!("poll child: {e}"))? {
            return Err(format!("child exited early with {status}"));
        }
        thread::sleep(Duration::from_millis(250));
    }
    Err("timed out".to_owned())
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
        .map_err(|e| format!("set read timeout: {e}"))?;
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

fn response_text_visible(stdout: &Arc<Mutex<String>>) -> bool {
    let text = snapshot(stdout);
    text.lines()
        .any(|line| line.trim_start_matches('>').trim().len() > 8)
}

fn stderr_contains(stderr: &Arc<Mutex<String>>, needle: &str) -> bool {
    snapshot(stderr).contains(needle)
}

fn snapshot(buf: &Arc<Mutex<String>>) -> String {
    buf.lock().expect("capture mutex").clone()
}

fn wait_child(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    let start = Instant::now();
    while start.elapsed() < timeout {
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
