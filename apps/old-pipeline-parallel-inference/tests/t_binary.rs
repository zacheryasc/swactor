//! T-binary: drive the actual `pp-orchestrator --seed` and `pp-worker`
//! binaries as child processes. TEST_SPEC §13 (stub workers) and §14
//! (real tinygrad workers, `#[ignore]`).
//!
//! Every test in this file is `#[ignore]`d. Each one spawns at least
//! `N + 1` real processes (one orchestrator, `N` stages, and each stage's
//! Python worker) so even the fastest case is too heavy to run in the
//! default `cargo test` guard. They are the deploy-readiness gate; run
//! them with:
//!
//! ```text
//! cargo test -p pipeline-parallel-inference --test t_binary -- --ignored
//! ```

use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStderr, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const ORCHESTRATOR_BIN: &str = env!("CARGO_BIN_EXE_pp-orchestrator");
const WORKER_BIN: &str = env!("CARGO_BIN_EXE_pp-worker");

/// Default per-test budget for an N=5 stub-mode happy path: cluster build +
/// SWIM convergence + sequential worker boots + a short decode loop. Tests
/// that intentionally push past convergence (slow-boot, kill scenarios)
/// override this locally.
const HAPPY_PATH_TIMEOUT: Duration = Duration::from_secs(180);

fn worker_script() -> String {
    format!("{}/pp_tinygrad_worker.py", env!("CARGO_MANIFEST_DIR"))
}

/// A thread-safe sink for stdout / stderr lines. The drainer threads append
/// every line they read; tests `lines()` to read a snapshot. The vector
/// preserves arrival order so timing-sensitive assertions (e.g. "pp-entry
/// registered after all pp-stage-X") can compare positions.
#[derive(Clone, Default)]
struct LogBuffer {
    lines: Arc<Mutex<Vec<String>>>,
}

impl LogBuffer {
    fn push(&self, line: String) {
        self.lines.lock().unwrap().push(line);
    }

    fn lines(&self) -> Vec<String> {
        self.lines.lock().unwrap().clone()
    }

    fn joined(&self) -> String {
        self.lines().join("\n")
    }

    fn first_index_containing(&self, needle: &str) -> Option<usize> {
        self.lines().iter().position(|l| l.contains(needle))
    }
}

fn drain_stdout(stdout: ChildStdout, label: &'static str, buf: LogBuffer) {
    thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            eprintln!("[{label} OUT] {line}");
            buf.push(line);
        }
    });
}

fn drain_stderr(stderr: ChildStderr, label: &'static str, buf: LogBuffer) {
    thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            eprintln!("[{label} ERR] {line}");
            buf.push(line);
        }
    });
}

/// Wait for `child` to exit, polling every 100ms. Kills the child and
/// returns `None` if the timeout elapses; the caller is then responsible
/// for finishing the kill (`child.wait()`).
fn wait_with_timeout(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(s)) => return Some(s),
            Ok(None) => {
                if start.elapsed() > timeout {
                    return None;
                }
                thread::sleep(Duration::from_millis(100));
            }
            Err(e) => panic!("try_wait error: {e}"),
        }
    }
}

/// List direct child pids of `pid` by reading `/proc/<pid>/task/<pid>/children`.
/// Returns an empty list if the file is missing or unreadable. Sorted ascending
/// so `pids[i]` corresponds to the i-th-spawned stage (Linux assigns rising
/// pids and `spawn_chain` is sequential).
fn child_pids(pid: u32) -> Vec<u32> {
    let path = format!("/proc/{pid}/task/{pid}/children");
    let mut v: Vec<u32> = std::fs::read_to_string(&path)
        .ok()
        .map(|s| {
            s.split_whitespace()
                .filter_map(|p| p.parse::<u32>().ok())
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// True iff the kernel still has `pid` in a non-zombie state. Used after
/// the orchestrator has been waited on, or while inspecting orphaned
/// grandchildren. A `Z (zombie)` entry counts as dead because the
/// process is finished — it just has not been `wait()`ed on yet, which
/// happens lazily when whatever subreaper inherits the orphan claims it.
fn pid_alive(pid: u32) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/status")) {
        Ok(s) => !s
            .lines()
            .any(|l| l.starts_with("State:") && l.contains('Z')),
        Err(_) => false,
    }
}

/// Wait until `pred()` returns true or `timeout` elapses. Returns true on
/// success.
fn wait_until(timeout: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if pred() {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    pred()
}

/// Wait for the orchestrator's `/proc/<pid>` child-list to contain at least
/// `expected` pids. Returns the sorted list when it does, or `None` on timeout.
fn wait_for_n_children(parent_pid: u32, expected: usize, timeout: Duration) -> Option<Vec<u32>> {
    let mut last: Vec<u32> = Vec::new();
    if wait_until(timeout, || {
        last = child_pids(parent_pid);
        last.len() >= expected
    }) {
        Some(child_pids(parent_pid))
    } else {
        None
    }
}

/// Extract the response text from `pp-orchestrator`'s stdout. The orchestrator
/// prints the response between two banner lines:
///
/// ```text
/// === pipeline-parallel Inference Response ===
/// <text>
/// ============================================
/// ```
///
/// Children's `print!` output from the chain spawner can be interleaved with
/// the banners on stdout. We collect every line between the markers and the
/// caller's assertion is responsible for narrowing further (the only line
/// the orchestrator's `println!` produces between them is the response text
/// itself, so the body is the response plus zero or more spurious child
/// stdout lines).
fn extract_response(lines: &[String]) -> Option<String> {
    const HEADER: &str = "=== pipeline-parallel Inference Response ===";
    const FOOTER: &str = "============================================";
    let header_idx = lines.iter().position(|l| l == HEADER)?;
    let footer_idx = lines
        .iter()
        .skip(header_idx + 1)
        .position(|l| l == FOOTER)?;
    let body: Vec<&str> = lines[header_idx + 1..header_idx + 1 + footer_idx]
        .iter()
        .map(|s| s.as_str())
        .collect();
    Some(body.join("\n"))
}

#[derive(Clone, Debug, Default)]
struct SmokeRunOpts {
    num_stages: u32,
    prompt: String,
    max_tokens: u32,
    stub: bool,
    /// Stage index that should sleep `boot_delay_secs` before initialising.
    boot_delay_stage: Option<u32>,
    boot_delay_secs: Option<u32>,
}

/// Spawn `pp-orchestrator --seed` with the given options. Returns the spawned
/// process plus a shared log buffer that captures every stdout / stderr
/// line from `pp-orchestrator` AND every `pp-worker` child (children inherit
/// the orchestrator's stderr fd, so their messages land in the same buffer).
fn spawn_smoke_run(opts: &SmokeRunOpts) -> (Child, LogBuffer, LogBuffer) {
    assert!(opts.num_stages >= 2);
    let worker = worker_script();
    let mut cmd = Command::new(ORCHESTRATOR_BIN);
    cmd.arg("--seed")
        .arg("--num-stages")
        .arg(opts.num_stages.to_string())
        .arg("--prompt")
        .arg(&opts.prompt)
        .arg("--max-tokens")
        .arg(opts.max_tokens.to_string())
        .arg("--gpu-node")
        .arg(WORKER_BIN)
        .arg("--worker")
        .arg(&worker)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if opts.stub {
        cmd.env("PP_WORKER_STUB", "1");
    } else {
        cmd.env_remove("PP_WORKER_STUB");
    }
    if let (Some(s), Some(d)) = (opts.boot_delay_stage, opts.boot_delay_secs) {
        cmd.env("PP_BOOT_DELAY_STAGE", s.to_string())
            .env("PP_BOOT_DELAY_SECS", d.to_string());
    } else {
        cmd.env_remove("PP_BOOT_DELAY_STAGE");
        cmd.env_remove("PP_BOOT_DELAY_SECS");
    }

    let mut child = cmd.spawn().expect("spawn pp-orchestrator");
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let stdout_buf = LogBuffer::default();
    let stderr_buf = LogBuffer::default();
    drain_stdout(stdout, "pp-orchestrator", stdout_buf.clone());
    drain_stderr(stderr, "pp-orchestrator", stderr_buf.clone());
    (child, stdout_buf, stderr_buf)
}

/// One-shot stub-mode happy-path runner. Spawns at `num_stages`, waits for
/// completion, panics on timeout, and returns `(exit_status, stdout_lines,
/// stderr_lines, pre_exit_pids)`. The pre-exit pids are the stage children's
/// pids captured while the orchestrator was still alive — used by callers
/// to verify post-exit cleanup.
fn run_to_completion(opts: &SmokeRunOpts) -> RunOutcome {
    let (mut smoke, stdout, stderr) = spawn_smoke_run(opts);
    let smoke_pid = smoke.id();
    let n = opts.num_stages as usize;
    let pre_exit_pids =
        wait_for_n_children(smoke_pid, n, Duration::from_secs(60)).unwrap_or_else(|| {
            let _ = smoke.kill();
            let _ = smoke.wait();
            panic!("pp-orchestrator did not spawn {n} pp-worker children within 60s")
        });

    let status = wait_with_timeout(&mut smoke, HAPPY_PATH_TIMEOUT).unwrap_or_else(|| {
        let _ = smoke.kill();
        let _ = smoke.wait();
        panic!(
            "pp-orchestrator did not exit within {:?}",
            HAPPY_PATH_TIMEOUT
        );
    });
    RunOutcome {
        status,
        stdout: stdout.lines(),
        stderr: stderr.lines(),
        pre_exit_pids,
    }
}

struct RunOutcome {
    status: std::process::ExitStatus,
    stdout: Vec<String>,
    stderr: Vec<String>,
    pre_exit_pids: Vec<u32>,
}

impl RunOutcome {
    fn require_success(&self) {
        assert!(
            self.status.success(),
            "pp-orchestrator exited with {:?}\n--- stdout ---\n{}\n--- stderr (last 40 lines) ---\n{}",
            self.status,
            self.stdout.join("\n"),
            self.stderr
                .iter()
                .rev()
                .take(40)
                .rev()
                .cloned()
                .collect::<Vec<_>>()
                .join("\n"),
        );
    }

    fn require_response_non_empty(&self) -> String {
        let response = extract_response(&self.stdout).unwrap_or_else(|| {
            panic!(
                "pp-orchestrator stdout missing response banner; got:\n{}",
                self.stdout.join("\n")
            )
        });
        assert!(
            !response.trim().is_empty(),
            "response text must be non-empty, got {response:?}"
        );
        response
    }

    fn require_no_orphans(&self) {
        for pid in &self.pre_exit_pids {
            assert!(
                !pid_alive(*pid),
                "pp-worker child pid {pid} still alive after pp-orchestrator exit"
            );
        }
    }
}

/// Default stub-mode happy-path options. `max_tokens=4` keeps the decode
/// loop short; the response text is deterministic per stub-worker seed.
fn stub_opts(num_stages: u32) -> SmokeRunOpts {
    SmokeRunOpts {
        num_stages,
        prompt: "Say hello".into(),
        max_tokens: 4,
        stub: true,
        boot_delay_stage: None,
        boot_delay_secs: None,
    }
}

/// SIGKILL a pid via `kill -9`. Returns once the kernel has accepted the
/// signal (the target may not yet have been reaped).
fn sigkill(pid: u32) {
    let status = Command::new("kill")
        .arg("-9")
        .arg(pid.to_string())
        .status()
        .expect("kill -9");
    assert!(status.success(), "kill -9 {pid} failed: {status:?}");
}

/// Stub-worker accumulated text format: `tokens: [<id> <id> ...]`. The Last
/// stage builds this in `StageActor::detokenize_stub`; matching it here lets
/// the §13.1 token-count test extract the count without parsing freeform text.
fn stub_token_count(response: &str) -> Option<usize> {
    let inner = response.lines().find_map(|l| {
        l.trim()
            .strip_prefix("tokens: [")
            .and_then(|s| s.strip_suffix("]"))
    })?;
    Some(inner.split_whitespace().count())
}

// ───────────────────────────────────────────────────────────────────────
// §13.1 — Happy paths
// ───────────────────────────────────────────────────────────────────────

fn happy_path_at(num_stages: u32) {
    let outcome = run_to_completion(&stub_opts(num_stages));
    outcome.require_success();
    outcome.require_response_non_empty();
    outcome.require_no_orphans();
}

#[test]
#[ignore]
fn binary_e2e_n_stub_workers_returns_response_2() {
    happy_path_at(2);
}

#[test]
#[ignore]
fn binary_e2e_n_stub_workers_returns_response_3() {
    happy_path_at(3);
}

#[test]
#[ignore]
fn binary_e2e_n_stub_workers_returns_response_5() {
    happy_path_at(5);
}

#[test]
#[ignore]
fn binary_e2e_response_contains_accumulated_token_count() {
    let opts = SmokeRunOpts {
        num_stages: 5,
        prompt: "Say hello".into(),
        max_tokens: 6,
        stub: true,
        ..SmokeRunOpts::default()
    };
    let outcome = run_to_completion(&opts);
    outcome.require_success();
    let response = outcome.require_response_non_empty();
    let count = stub_token_count(&response).unwrap_or_else(|| {
        panic!("response does not match stub `tokens: [...]` format:\n{response}")
    });
    assert_eq!(
        count, opts.max_tokens as usize,
        "stub response should contain exactly max_tokens ids; got {count} in:\n{response}"
    );
}

#[test]
#[ignore]
fn binary_e2e_orchestrator_registers_pp_orchestrator_name() {
    let outcome = run_to_completion(&stub_opts(3));
    outcome.require_success();
    let stderr = outcome.stderr.join("\n");
    assert!(
        stderr.contains("registered pp-orchestrator"),
        "expected `registered pp-orchestrator` in stderr; got:\n{stderr}"
    );
    // The last stage cannot have sent its InferenceResponse without first
    // resolving pp-orchestrator on the iroh side. The response banner on
    // stdout is the user-visible confirmation that the round-trip closed.
    outcome.require_response_non_empty();
}

#[test]
#[ignore]
fn binary_e2e_all_stages_register_pp_stage_index_names() {
    let outcome = run_to_completion(&stub_opts(4));
    outcome.require_success();
    let stderr = outcome.stderr.join("\n");
    for i in 0..4 {
        let needle = format!("registered pp-stage-{i}");
        assert!(
            stderr.contains(&needle),
            "expected `{needle}` in stderr; full stderr:\n{stderr}"
        );
    }
}

// ───────────────────────────────────────────────────────────────────────
// §13.2 — Failure / cleanup paths
// ───────────────────────────────────────────────────────────────────────

/// Spawn `pp-orchestrator` at `num_stages`, wait until every stage child is
/// visible, kill `stage_to_kill`, and assert the orchestrator exits
/// non-zero with no surviving stage children.
fn kill_stage_and_expect_failure(num_stages: u32, stage_to_kill: u32) {
    assert!(stage_to_kill < num_stages);
    let opts = stub_opts(num_stages);
    let (mut smoke, _stdout, _stderr) = spawn_smoke_run(&opts);
    let smoke_pid = smoke.id();
    let pids = wait_for_n_children(smoke_pid, num_stages as usize, Duration::from_secs(60))
        .unwrap_or_else(|| {
            let _ = smoke.kill();
            let _ = smoke.wait();
            panic!("pp-orchestrator did not spawn {num_stages} children within 60s");
        });
    let victim = pids[stage_to_kill as usize];

    sigkill(victim);

    let exit = wait_with_timeout(&mut smoke, Duration::from_secs(120)).unwrap_or_else(|| {
        let _ = smoke.kill();
        let _ = smoke.wait();
        panic!("pp-orchestrator did not exit within 120s after killing stage {stage_to_kill}");
    });
    assert!(
        !exit.success(),
        "pp-orchestrator should fail when stage {stage_to_kill} (pid {victim}) is killed, got {exit:?}"
    );
    for pid in &pids {
        assert!(
            !pid_alive(*pid),
            "stage child pid {pid} still alive after pp-orchestrator exit"
        );
    }
}

#[test]
#[ignore]
fn binary_e2e_first_stage_killed_orchestrator_exits_nonzero_2() {
    kill_stage_and_expect_failure(2, 0);
}

#[test]
#[ignore]
fn binary_e2e_first_stage_killed_orchestrator_exits_nonzero_3() {
    kill_stage_and_expect_failure(3, 0);
}

#[test]
#[ignore]
fn binary_e2e_first_stage_killed_orchestrator_exits_nonzero_5() {
    kill_stage_and_expect_failure(5, 0);
}

#[test]
#[ignore]
fn binary_e2e_middle_stage_killed_orchestrator_exits_nonzero() {
    kill_stage_and_expect_failure(5, 2);
}

#[test]
#[ignore]
fn binary_e2e_last_stage_killed_orchestrator_exits_nonzero_2() {
    kill_stage_and_expect_failure(2, 1);
}

#[test]
#[ignore]
fn binary_e2e_last_stage_killed_orchestrator_exits_nonzero_3() {
    kill_stage_and_expect_failure(3, 2);
}

#[test]
#[ignore]
fn binary_e2e_last_stage_killed_orchestrator_exits_nonzero_5() {
    kill_stage_and_expect_failure(5, 4);
}

#[test]
#[ignore]
fn binary_e2e_no_orphaned_processes_after_clean_exit_3() {
    let outcome = run_to_completion(&stub_opts(3));
    outcome.require_success();
    outcome.require_no_orphans();
}

#[test]
#[ignore]
fn binary_e2e_no_orphaned_processes_after_failed_exit_3() {
    // Reuse the kill helper, which already asserts no orphans. Asserting
    // separately here would just re-run the same scenario.
    kill_stage_and_expect_failure(3, 1);
}

#[test]
#[ignore]
fn binary_e2e_orchestrator_sigkilled_children_die_within_timeout() {
    // SIGKILL `pp-orchestrator` itself once its children are up. The kernel
    // delivers `SIGTERM` to each pp-worker (via PR_SET_PDEATHSIG, set in
    // pp-worker's main), and each pp-worker then dies — which also
    // closes its Python worker's stdin, making the worker exit on EOF.
    let opts = stub_opts(3);
    let (mut smoke, _stdout, _stderr) = spawn_smoke_run(&opts);
    let smoke_pid = smoke.id();
    let pids = wait_for_n_children(smoke_pid, 3, Duration::from_secs(60)).unwrap_or_else(|| {
        let _ = smoke.kill();
        let _ = smoke.wait();
        panic!("pp-orchestrator did not spawn 3 children within 60s");
    });

    sigkill(smoke_pid);
    let _ = smoke.wait();

    let cleaned_up = wait_until(Duration::from_secs(10), || {
        pids.iter().all(|p| !pid_alive(*p))
    });
    assert!(
        cleaned_up,
        "pp-worker children {pids:?} still alive 10s after pp-orchestrator SIGKILL; \
         per-pid alive states: {:?}",
        pids.iter().map(|p| (p, pid_alive(*p))).collect::<Vec<_>>()
    );
}

// ───────────────────────────────────────────────────────────────────────
// §13.3 — Boot-order edge cases
// ───────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn binary_e2e_orchestrator_can_resolve_pp_entry_after_n_stages_register() {
    let outcome = run_to_completion(&stub_opts(5));
    outcome.require_success();
    let stderr_buf = LogBuffer::default();
    for l in &outcome.stderr {
        stderr_buf.push(l.clone());
    }
    // pp-entry is registered by stage 0 only *after* it has resolved its
    // next neighbour (which in turn requires stage 1's pp-stage-1 to have
    // been registered, and so on). Walk the log: the line that announces
    // `registered pp-entry` must appear *after* every `registered
    // pp-stage-{i}` line for i in 0..N.
    let entry_idx = stderr_buf
        .first_index_containing("registered pp-entry")
        .expect("missing `registered pp-entry` in stderr");
    for i in 0..5 {
        let needle = format!("registered pp-stage-{i}");
        let idx = stderr_buf
            .first_index_containing(&needle)
            .unwrap_or_else(|| panic!("missing `{needle}` in stderr"));
        assert!(
            idx < entry_idx,
            "expected `{needle}` to be registered before pp-entry (got {idx} vs {entry_idx})"
        );
    }
}

#[test]
#[ignore]
fn binary_e2e_pp_orchestrator_handles_slow_middle_stage_boot() {
    let opts = SmokeRunOpts {
        num_stages: 4,
        prompt: "Say hello".into(),
        max_tokens: 4,
        stub: true,
        boot_delay_stage: Some(2),
        boot_delay_secs: Some(30),
    };
    let outcome = run_to_completion(&opts);
    outcome.require_success();
    outcome.require_response_non_empty();
    outcome.require_no_orphans();
}

#[test]
#[ignore]
fn binary_e2e_pp_orchestrator_handles_slow_last_stage_boot() {
    let opts = SmokeRunOpts {
        num_stages: 4,
        prompt: "Say hello".into(),
        max_tokens: 4,
        stub: true,
        boot_delay_stage: Some(3),
        boot_delay_secs: Some(30),
    };
    let outcome = run_to_completion(&opts);
    outcome.require_success();
    outcome.require_response_non_empty();
    outcome.require_no_orphans();
}

// ───────────────────────────────────────────────────────────────────────
// §14 — Real-tinygrad binary E2E (gated, very slow)
// ───────────────────────────────────────────────────────────────────────
//
// Requirements:
// * `python3` with `tinygrad` importable (a `.venv/bin/python` works if
//   exported via `WORKER_CMD`).
// * `clang` on PATH — tinygrad's CPU backend compiles kernels with it.
// * `~/.cache/tinygrad/downloads/` containing `llama3.2:1b`, or network to
//   fetch it on first run.
//
// Run explicitly:
//   `cargo test -p pipeline-parallel-inference --test t_binary \
//       binary_e2e_real_tinygrad -- --ignored --nocapture --test-threads=1`

/// §14 tests require `numpy`, `tinygrad`, `clang`, and the `llama3.2:1b`
/// GGUF to be reachable from the worker process. None of that is guaranteed
/// even with `--ignored`, so each §14 test opts in via this env var. Set
/// `PP_TINYGRAD_E2E=1` to actually run them.
fn skip_unless_tinygrad_opted_in() -> bool {
    if std::env::var("PP_TINYGRAD_E2E").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping: §14 real-tinygrad tests are off by default; \
             set PP_TINYGRAD_E2E=1 to run them"
        );
        return true;
    }
    false
}

fn real_tinygrad_at(num_stages: u32) {
    if skip_unless_tinygrad_opted_in() {
        return;
    }
    let opts = SmokeRunOpts {
        num_stages,
        prompt: "Say hello".into(),
        max_tokens: 4,
        stub: false,
        ..SmokeRunOpts::default()
    };
    let (mut smoke, stdout, stderr) = spawn_smoke_run(&opts);
    let smoke_pid = smoke.id();
    let pre_exit_pids =
        wait_for_n_children(smoke_pid, num_stages as usize, Duration::from_secs(120))
            .unwrap_or_else(|| {
                let _ = smoke.kill();
                let _ = smoke.wait();
                panic!("pp-orchestrator did not spawn {num_stages} children within 120s");
            });
    let status = wait_with_timeout(&mut smoke, Duration::from_secs(1200)).unwrap_or_else(|| {
        let _ = smoke.kill();
        let _ = smoke.wait();
        panic!("pp-orchestrator did not finish within 20m");
    });
    let stdout_lines = stdout.lines();
    let stderr_joined = stderr.joined();
    assert!(
        status.success(),
        "pp-orchestrator exited {status:?}\n--- stdout ---\n{}\n--- stderr (tail) ---\n{}",
        stdout_lines.join("\n"),
        stderr_joined
            .lines()
            .rev()
            .take(40)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n"),
    );
    let response = extract_response(&stdout_lines).unwrap_or_else(|| {
        panic!(
            "missing response banner; stdout:\n{}",
            stdout_lines.join("\n")
        )
    });
    assert!(
        !response.trim().is_empty(),
        "real-tinygrad response must be non-empty, got {response:?}"
    );
    for pid in pre_exit_pids {
        assert!(
            !pid_alive(pid),
            "pp-worker child pid {pid} still alive after pp-orchestrator exit"
        );
    }
}

#[test]
#[ignore]
fn binary_e2e_real_tinygrad_two_stage_returns_response() {
    real_tinygrad_at(2);
}

#[test]
#[ignore]
fn binary_e2e_real_tinygrad_three_stage_returns_response() {
    real_tinygrad_at(3);
}

#[test]
#[ignore]
fn binary_e2e_real_tinygrad_four_stage_returns_response() {
    real_tinygrad_at(4);
}

/// §14: the printed response at `N=3` should match what
/// `Transformer.generate()` produces single-node. The full equivalence
/// machinery lives in `t_integration.rs::§12`; this test is the same
/// assertion escalated through the binary. Skipped unless
/// `PP_SINGLE_NODE_REFERENCE` is set to the precomputed reference text.
#[test]
#[ignore]
fn binary_e2e_real_tinygrad_response_matches_single_node_for_say_hello() {
    if skip_unless_tinygrad_opted_in() {
        return;
    }
    let reference = match std::env::var("PP_SINGLE_NODE_REFERENCE") {
        Ok(s) if !s.trim().is_empty() => s,
        _ => {
            eprintln!(
                "skipping: PP_SINGLE_NODE_REFERENCE is not set. Run the §12 \
                 single-node reference script and pass the output text via \
                 PP_SINGLE_NODE_REFERENCE=\"...\""
            );
            return;
        }
    };
    let opts = SmokeRunOpts {
        num_stages: 3,
        prompt: "Say hello".into(),
        max_tokens: 4,
        stub: false,
        ..SmokeRunOpts::default()
    };
    let (mut smoke, stdout, _stderr) = spawn_smoke_run(&opts);
    let status = wait_with_timeout(&mut smoke, Duration::from_secs(1200)).unwrap_or_else(|| {
        let _ = smoke.kill();
        let _ = smoke.wait();
        panic!("pp-orchestrator did not finish within 20m");
    });
    assert!(status.success(), "pp-orchestrator exited {status:?}");
    let response = extract_response(&stdout.lines()).expect("missing response banner");
    assert_eq!(
        response.trim(),
        reference.trim(),
        "binary response at N=3 should match single-node generate() output"
    );
}
