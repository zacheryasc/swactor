//! T-docker: Stage 11 pre-deploy gate — bring an N-stage cluster up
//! inside Docker containers, drive one inference request through it,
//! and verify clean teardown.
//!
//! TEST_SPEC §13b. Mirrors §13's happy path and at least one failure
//! scenario, but with each `pp-worker` running inside its own
//! container instead of as a host process.
//!
//! Every test in this file is `#[ignore]`d and requires a working
//! Docker daemon plus the ability to build a small CPU image. Run
//! with:
//!
//! ```text
//! cargo test -p pipeline-parallel-inference --test t_docker -- --ignored
//! ```
//!
//! Each test uses a unique container-name prefix so concurrent
//! invocations cannot collide on container names. The happy-path test
//! is the canonical gate; the idempotency test runs the harness twice
//! to surface state leaked across runs; the failure test kills a
//! container mid-decode to prove the harness fails fast.

use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

/// Process-wide lock that serialises every Docker-touching test. `cargo
/// test --test t_docker -- --ignored` is invoked with `--test-threads 1`
/// in the documented gate, but a developer may forget and the shared
/// docker daemon does not survive overlapping image builds + container
/// spawns. Holding this mutex around each test keeps the gate
/// reproducible regardless of test-thread count.
fn docker_serial_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn docker_e2e_script() -> PathBuf {
    crate_dir().join("scripts").join("docker-e2e.sh")
}

fn docker_gpu_node_shim() -> PathBuf {
    crate_dir().join("scripts").join("docker-gpu-node.sh")
}

/// Skip the test (with a printed reason) when docker is unreachable.
/// Returns `true` if the caller should proceed, `false` to early-return.
fn require_docker() -> bool {
    match Command::new("docker").arg("info").output() {
        Ok(out) if out.status.success() => true,
        Ok(_) => {
            eprintln!("t_docker: skipping — `docker info` failed (no daemon?)");
            false
        }
        Err(e) => {
            eprintln!("t_docker: skipping — docker not available: {e}");
            false
        }
    }
}

/// Skip a real-mode test unless a CUDA GPU is reachable through Docker.
/// The slim runtime image has no host compiler, so tinygrad's CPU backend
/// can't JIT — real inference needs a GPU attached via `docker run --gpus`.
/// We probe the same way the run will: launch the CUDA base image with
/// `--gpus all` and check `nvidia-smi` succeeds. Returns `true` to proceed.
fn require_cuda_gpu() -> bool {
    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--gpus",
            "all",
            "nvidia/cuda:12.6.3-base-ubuntu24.04",
            "nvidia-smi",
            "-L",
        ])
        .output();
    match out {
        Ok(o) if o.status.success() && !o.stdout.is_empty() => true,
        _ => {
            eprintln!("t_docker: skipping — no CUDA GPU reachable via `docker run --gpus all`");
            false
        }
    }
}

fn remove_containers_with_prefix(prefix: &str) {
    let filter = format!("name=^{prefix}-[0-9]+$");
    let listing = Command::new("docker")
        .args(["ps", "-aq", "--filter", &filter])
        .output();
    let ids = match listing {
        Ok(out) => String::from_utf8_lossy(&out.stdout).trim().to_string(),
        Err(_) => return,
    };
    if ids.is_empty() {
        return;
    }
    for id in ids.split_whitespace() {
        let _ = Command::new("docker")
            .args(["rm", "-f", id])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// Block until every container matching the prefix is gone from
/// `docker ps -a`. `docker run --rm` removes its container
/// asynchronously after the process exits, so a prior test's last
/// container can linger for hundreds of milliseconds; a leftover-check
/// that fires before the daemon catches up sees a false positive.
/// On timeout, force-remove and continue rather than panicking — the
/// test that called us still has to make its own assertion, and a
/// stuck container in `docker ps` is more useful as a delete + warn
/// than as a swallowed test failure.
fn wait_until_prefix_drains(prefix: &str, timeout: Duration) {
    let start = Instant::now();
    loop {
        if list_containers_with_prefix(prefix).is_empty() {
            return;
        }
        if start.elapsed() >= timeout {
            eprintln!(
                "t_docker: containers with prefix {prefix:?} did not drain within {:?}; \
                 force-removing",
                timeout
            );
            remove_containers_with_prefix(prefix);
            return;
        }
        thread::sleep(Duration::from_millis(150));
    }
}

/// Poll for "no leftover stage containers" with a short grace window.
/// A clean `pp-orchestrator` exit triggers `docker run --rm` teardown on
/// each shim, but the daemon-side delete is not synchronous with the
/// CLI's exit, so we give the daemon a moment to catch up before we
/// call the run dirty.
fn assert_no_leftovers_eventually(prefix: &str, timeout: Duration) {
    let start = Instant::now();
    loop {
        let leftover = list_containers_with_prefix(prefix);
        if leftover.is_empty() {
            return;
        }
        if start.elapsed() >= timeout {
            panic!("stage containers remained after run: {leftover:?}");
        }
        thread::sleep(Duration::from_millis(150));
    }
}

fn list_containers_with_prefix(prefix: &str) -> Vec<String> {
    let filter = format!("name=^{prefix}-[0-9]+$");
    let out = match Command::new("docker")
        .args(["ps", "-aq", "--filter", &filter])
        .output()
    {
        Ok(o) => o,
        Err(_) => return Vec::new(),
    };
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .map(|s| s.to_string())
        .collect()
}

/// Wait for at least `expected` containers matching `prefix` to be
/// `running`. Returns the running container names on success, or
/// `None` on timeout.
fn wait_for_running_containers(
    prefix: &str,
    expected: usize,
    timeout: Duration,
) -> Option<Vec<String>> {
    let filter_running = format!("name=^{prefix}-[0-9]+$");
    let start = Instant::now();
    loop {
        let out = Command::new("docker")
            .args([
                "ps",
                "--filter",
                &filter_running,
                "--filter",
                "status=running",
                "--format",
                "{{.Names}}",
            ])
            .output()
            .ok()?;
        let names: Vec<String> = String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(|s| s.to_string())
            .collect();
        if names.len() >= expected {
            return Some(names);
        }
        if start.elapsed() >= timeout {
            return None;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Run `docker-e2e.sh N` and return its `(status, stdout, stderr)`.
///
/// Stdout and stderr are written to files in a unique tempdir rather than
/// captured in-process via `Command::output`. Direct pipe capture forces
/// the entire script's output (including every container's chatty
/// per-tick log line) through a kernel pipe whose write side is held by
/// the orchestrator and a transitive `docker run` CLI per container.
/// Under back-to-back load the cumulative pipe pressure intermittently
/// stalls the SWIM gossip-piggyback registration of `pp-stage-0` long
/// enough for the second run to time out resolving it (observed even
/// with a 120s timeout). Redirecting to files makes the second run as
/// reliable as the manual `scripts/docker-e2e.sh` invocation — neither
/// holds the pipe back.
fn run_docker_e2e(num_stages: u32, prefix: &str, skip_image_build: bool) -> ScriptOutcome {
    run_docker_e2e_env(num_stages, prefix, skip_image_build, &[])
}

fn run_docker_e2e_env(
    num_stages: u32,
    prefix: &str,
    skip_image_build: bool,
    extra_env: &[(&str, &str)],
) -> ScriptOutcome {
    cargo_build_release_once();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let logdir = std::env::temp_dir().join(format!("pp-e2e-{prefix}-{stamp}"));
    std::fs::create_dir_all(&logdir).expect("create temp log dir");
    let stdout_path = logdir.join("stdout.log");
    let stderr_path = logdir.join("stderr.log");
    let stdout_file = std::fs::File::create(&stdout_path).expect("create stdout log");
    let stderr_file = std::fs::File::create(&stderr_path).expect("create stderr log");
    let mut cmd = Command::new(docker_e2e_script());
    cmd.arg(num_stages.to_string())
        .env("PP_CONTAINER_PREFIX", prefix)
        .env("PP_SKIP_BUILD", "1")
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file));
    if skip_image_build {
        cmd.env("PP_SKIP_IMAGE_BUILD", "1");
    }
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    let status = cmd.status().expect("run docker-e2e.sh");
    let stdout = std::fs::read_to_string(&stdout_path).unwrap_or_default();
    let stderr = std::fs::read_to_string(&stderr_path).unwrap_or_default();
    let _ = std::fs::remove_dir_all(&logdir);
    ScriptOutcome {
        status,
        stdout,
        stderr,
    }
}

struct ScriptOutcome {
    status: std::process::ExitStatus,
    stdout: String,
    stderr: String,
}

impl ScriptOutcome {
    fn require_success(&self, context: &str) {
        if self.status.success() {
            return;
        }
        // On failure, surface every diagnostic the orchestrator and
        // its container children emitted: name-registration lines,
        // resolution attempts, cluster convergence, premature exits.
        // Without these the reader gets just the test-side panic text
        // and nothing about what went wrong in the cluster.
        let registry: Vec<&str> = self
            .stderr
            .lines()
            .filter(|l| {
                l.contains("registered ")
                    || l.contains("resolved ")
                    || l.contains("failed to resolve")
                    || l.contains("cluster did not converge")
                    || l.contains("cluster converged")
                    || l.contains("exited prematurely")
            })
            .collect();
        panic!(
            "docker-e2e.sh ({context}) exited {:?}\n\
             --- stdout ---\n{}\n\
             --- stderr registry events ---\n{}\n\
             --- stderr (tail 120) ---\n{}",
            self.status,
            self.stdout,
            registry.join("\n"),
            tail_lines(&self.stderr, 120),
        );
    }

    fn require_response(&self) -> String {
        let header = "=== pipeline-parallel Inference Response ===";
        let footer = "============================================";
        let mut lines = self.stdout.lines();
        let hdr_idx = lines.position(|l| l == header).unwrap_or_else(|| {
            panic!(
                "stdout missing response header banner\n--- stdout ---\n{}",
                self.stdout
            )
        });
        let _ = hdr_idx;
        let body: Vec<&str> = self
            .stdout
            .lines()
            .skip_while(|l| *l != header)
            .skip(1)
            .take_while(|l| *l != footer)
            .collect();
        assert!(
            !body.is_empty() && body.iter().any(|l| !l.is_empty()),
            "response banner present but body empty\n--- stdout ---\n{}",
            self.stdout
        );
        body.join("\n")
    }
}

fn tail_lines(s: &str, n: usize) -> String {
    let v: Vec<&str> = s.lines().collect();
    let start = v.len().saturating_sub(n);
    v[start..].join("\n")
}

/// Build the release binaries exactly once per test process. Each `#[ignore]`
/// test in this file goes through `docker-e2e.sh` with `PP_SKIP_BUILD=1`, so
/// without this the artefacts would be missing. Using a `OnceLock` keeps the
/// tests independent of run order: whichever test fires first does the
/// `cargo build`, the rest reuse the artefacts.
fn cargo_build_release_once() {
    use std::sync::OnceLock;
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        let status = Command::new("cargo")
            .arg("build")
            .arg("--manifest-path")
            .arg(crate_dir().join("Cargo.toml"))
            .arg("--release")
            .arg("--bin")
            .arg("pp-worker")
            .arg("--bin")
            .arg("pp-orchestrator")
            .status()
            .expect("invoke cargo build");
        assert!(status.success(), "cargo build --release failed");
    });
}

/// RAII guard that removes every container whose name starts with the
/// configured prefix when dropped. Belt-and-suspenders: even if a test
/// panics mid-run, we never leave containers behind to fail the next
/// test or annoy the developer running the suite.
///
/// `new` blocks until the prefix is drained — a previous test or a
/// developer's manual run can leave a container that is still in
/// `Created` / `Removal` and would otherwise haunt this test.
struct PrefixCleanup<'a> {
    prefix: &'a str,
}

impl<'a> PrefixCleanup<'a> {
    fn new(prefix: &'a str) -> Self {
        remove_containers_with_prefix(prefix);
        wait_until_prefix_drains(prefix, Duration::from_secs(15));
        Self { prefix }
    }
}

impl Drop for PrefixCleanup<'_> {
    fn drop(&mut self) {
        remove_containers_with_prefix(self.prefix);
        wait_until_prefix_drains(self.prefix, Duration::from_secs(10));
    }
}

// ─── §13b.1 happy path ───────────────────────────────────────────────

#[test]
#[ignore]
fn docker_e2e_three_stage_cluster_returns_response() {
    let _serial = docker_serial_lock();
    if !require_docker() {
        return;
    }
    let prefix = "pp-e2e-happy";
    let _cleanup = PrefixCleanup::new(prefix);

    let outcome = run_docker_e2e(3, prefix, false);
    outcome.require_success("happy-path N=3");
    let response = outcome.require_response();
    assert!(
        !response.trim().is_empty(),
        "expected non-empty response text"
    );

    assert_no_leftovers_eventually(prefix, Duration::from_secs(10));
}

#[test]
#[ignore]
fn docker_e2e_re_running_command_twice_both_pass() {
    let _serial = docker_serial_lock();
    if !require_docker() {
        return;
    }
    let prefix = "pp-e2e-idem";
    let _cleanup = PrefixCleanup::new(prefix);

    let first = run_docker_e2e(3, prefix, false);
    first.require_success("first run");
    let _ = first.require_response();
    assert_no_leftovers_eventually(prefix, Duration::from_secs(10));

    // Small settle between runs. `assert_no_leftovers_eventually`
    // already waits for docker to remove the prior containers, but
    // the kernel keeps the prior orchestrator's UDP sockets around
    // for a moment after the process exits; giving them time to clear
    // keeps the second run from racing the kernel for ephemeral ports
    // when iroh re-opens its endpoint.
    thread::sleep(Duration::from_secs(2));

    // Second run reuses the image and the cargo artefacts.
    let second = run_docker_e2e(3, prefix, true);
    second.require_success("second run");
    let _ = second.require_response();
    assert_no_leftovers_eventually(prefix, Duration::from_secs(10));
}

// ─── §13b.3 real CUDA pipeline at 12 nodes ───────────────────────────
//
// The full pipeline, for real: 12 containers, each loading its own model
// shard on a GPU via tinygrad/NVRTC, converging, and pushing a "hello
// world" prompt all the way through to a non-empty completion. This is the
// counterpart to the happy-path stub test — same orchestration, but real
// weights and real inference at 12 nodes.
//
// Gated three ways: `#[ignore]` (like every test here), an explicit
// `PP_REAL_E2E` opt-in, and a CUDA-GPU probe. Real workers fetch a model
// shard and JIT CUDA kernels, so they reach `ready` far slower than the
// stub — the wiring/response windows are widened via env. Run with:
//
// ```text
// PP_REAL_E2E=1 cargo test -p pipeline-parallel-inference --test t_docker \
//     -- --ignored real_e2e_twelve_stage
// ```
#[test]
#[ignore]
fn real_e2e_twelve_stage_pipeline_returns_response() {
    let _serial = docker_serial_lock();
    if std::env::var("PP_REAL_E2E").is_err() {
        eprintln!("t_docker: skipping real 12-node e2e — set PP_REAL_E2E=1 to run");
        return;
    }
    if !require_docker() || !require_cuda_gpu() {
        return;
    }
    let prefix = "pp-e2e-real12";
    let _cleanup = PrefixCleanup::new(prefix);

    let outcome = run_docker_e2e_env(
        12,
        prefix,
        false,
        &[
            ("PP_REAL", "1"),
            ("PP_PROMPT", "hello world"),
            ("PP_MAX_TOKENS", "8"),
            ("PP_PIPELINE_WIRED_TIMEOUT_SECS", "900"),
            ("PP_AWAIT_RESPONSE_TIMEOUT_SECS", "900"),
        ],
    );
    outcome.require_success("real N=12");
    let response = outcome.require_response();
    assert!(
        !response.trim().is_empty(),
        "expected a non-empty completion from the 12-stage real pipeline"
    );

    assert_no_leftovers_eventually(prefix, Duration::from_secs(20));
}

// ─── §13b.2 failure path ─────────────────────────────────────────────

#[test]
#[ignore]
fn docker_e2e_premature_container_exit_fails_fast() {
    let _serial = docker_serial_lock();
    if !require_docker() {
        return;
    }
    let prefix = "pp-e2e-fail";
    let _cleanup = PrefixCleanup::new(prefix);
    cargo_build_release_once();

    // Build the layered image inline (heavy base, then thin code layer) so
    // the failure test does not depend on a prior happy-path run having
    // built it. Stub mode is a runtime toggle (PP_WORKER_STUB=1 below).
    let workspace = crate_dir()
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf();
    let base_tag = "swactor-pp-base:cuda12.6";
    let base_build = Command::new("docker")
        .args(["build", "-f"])
        .arg(crate_dir().join("Dockerfile.base"))
        .arg("-t")
        .arg(base_tag)
        .arg(&workspace)
        .status()
        .expect("docker build (base)");
    assert!(base_build.success(), "docker build (base) failed");
    let image_tag = "swactor-pp-gpu:t_docker-fail";
    let build = Command::new("docker")
        .args(["build", "-f"])
        .arg(crate_dir().join("Dockerfile"))
        .arg("--build-arg")
        .arg(format!("BASE_IMAGE={base_tag}"))
        .arg("-t")
        .arg(image_tag)
        .arg(&workspace)
        .status()
        .expect("docker build (code)");
    assert!(build.success(), "docker build (code) failed");

    let smoke_bin = crate_dir().join("target/release/pp-orchestrator");
    let worker_py = crate_dir().join("pp_tinygrad_worker.py");
    assert!(smoke_bin.exists() && worker_py.exists());

    let mut child = Command::new(&smoke_bin)
        .arg("--seed")
        .arg("--num-stages")
        .arg("3")
        .arg("--gpu-node")
        .arg(docker_gpu_node_shim())
        .arg("--worker")
        .arg(&worker_py)
        .arg("--prompt")
        .arg("Say hello")
        .arg("--max-tokens")
        .arg("64")
        .env("PP_WORKER_STUB", "1")
        .env("PP_IMAGE", image_tag)
        .env("PP_CONTAINER_PREFIX", prefix)
        .env("PP_DEV", "CPU")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn pp-orchestrator with docker shim");

    // Drain the child's stderr (and stdout) into in-memory buffers via
    // background reader threads. Without this any pp-orchestrator / shim
    // diagnostic message is swallowed and the test gives the reader
    // nothing actionable on failure.
    let stderr_buf = spawn_stream_collector(child.stderr.take().expect("child stderr piped"));
    let stdout_buf = spawn_stream_collector(child.stdout.take().expect("child stdout piped"));

    // Wait for all 3 stage containers to be running, then kill the middle one.
    let running = match wait_for_running_containers(prefix, 3, Duration::from_secs(180)) {
        Some(running) => running,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "3 stage containers did not start within 180s\n\
                 --- pp-orchestrator stderr (tail) ---\n{}\n\
                 --- pp-orchestrator stdout (tail) ---\n{}",
                tail_lines(&stderr_buf.snapshot(), 80),
                tail_lines(&stdout_buf.snapshot(), 40),
            );
        }
    };
    // Container names are pp-<prefix>-{stage}; pick the middle one (-1).
    let victim = format!("{prefix}-1");
    assert!(
        running.iter().any(|n| n == &victim),
        "expected {victim} in running set, got {running:?}"
    );
    let kill = Command::new("docker")
        .args(["kill", &victim])
        .status()
        .expect("docker kill");
    assert!(kill.success(), "docker kill {victim} failed");

    // The orchestrator must surface this as a non-zero exit within
    // the kill detection window (try_wait inside await_response sees
    // the docker-shim child exit promptly).
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        match child.try_wait().expect("try_wait pp-orchestrator") {
            Some(s) => break s,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "pp-orchestrator did not exit within 60s of killing a container\n\
                     --- pp-orchestrator stderr (tail) ---\n{}\n\
                     --- pp-orchestrator stdout (tail) ---\n{}",
                    tail_lines(&stderr_buf.snapshot(), 80),
                    tail_lines(&stdout_buf.snapshot(), 40),
                );
            }
            None => std::thread::sleep(Duration::from_millis(100)),
        }
    };
    assert!(
        !status.success(),
        "expected non-zero exit after container kill, got {status:?}\n\
         --- pp-orchestrator stderr (tail) ---\n{}",
        tail_lines(&stderr_buf.snapshot(), 80),
    );

    // No stage container may survive the failure path. The shim's
    // `docker run --rm` cleans up the victim and the surviving
    // containers exit when their parent shim process dies. Poll for a
    // few seconds because `--rm` removes asynchronously after exit.
    assert_no_leftovers_eventually(prefix, Duration::from_secs(15));
}

/// Background reader for a child's stdout/stderr pipe. Writes the
/// stream to a thread-safe buffer in 4 KiB chunks so we can dump the
/// tail on a test panic without blocking on the read.
struct StreamBuf {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl StreamBuf {
    fn snapshot(&self) -> String {
        let guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        String::from_utf8_lossy(&guard).to_string()
    }
}

fn spawn_stream_collector<R: Read + Send + 'static>(mut reader: R) -> StreamBuf {
    let inner = Arc::new(Mutex::new(Vec::<u8>::new()));
    let inner_for_thread = Arc::clone(&inner);
    thread::spawn(move || {
        let mut chunk = [0u8; 4096];
        loop {
            match reader.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    let mut guard = inner_for_thread
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    guard.extend_from_slice(&chunk[..n]);
                }
                Err(_) => break,
            }
        }
    });
    StreamBuf { inner }
}
