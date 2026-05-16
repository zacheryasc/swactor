//! T-binary: drive the actual `pp-smoke-run --seed` and `pp-gpu-node`
//! binaries as child processes. TEST_SPEC §10.
//!
//! Mirrors `single-gpu-inference/tests/t_binary.rs`'s shape: spawn the
//! orchestrator binary, let it spawn the two stage children itself, scrape
//! its stdout for the response block, and verify exit status + that no
//! grandchild `pp-gpu-node` processes survive the test.
//!
//! - `binary_e2e_two_stub_workers_returns_hello_response` — fast, both
//!   stages run the stub worker; asserts non-empty response and exit 0.
//! - `binary_e2e_two_stub_workers_cleans_up_on_failure` — kills one
//!   `pp-gpu-node` mid-run, asserts non-zero exit and no orphaned
//!   grandchildren.
//! - `binary_e2e_two_tinygrad_workers_returns_response` — slow
//!   (`#[ignore]`): real tinygrad workers on the host. Requires clang
//!   and the `llama3.2:1b` GGUF available to tinygrad's fetcher.

use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const SMOKE_RUN_BIN: &str = env!("CARGO_BIN_EXE_pp-smoke-run");
const GPU_NODE_BIN: &str = env!("CARGO_BIN_EXE_pp-gpu-node");

fn worker_script() -> String {
    format!("{}/pp_tinygrad_worker.py", env!("CARGO_MANIFEST_DIR"))
}

/// Read `pp-smoke-run`'s stdout in a background thread, forward every line to
/// the test's stderr, and ship the accumulated text back through a channel so
/// the main thread can scan for response markers after the process exits.
fn drain_stdout(stdout: ChildStdout, label: &'static str) -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let reader = BufReader::new(stdout);
        let mut accum = String::new();
        for line in reader.lines() {
            match line {
                Ok(l) => {
                    eprintln!("[{label}] {l}");
                    accum.push_str(&l);
                    accum.push('\n');
                }
                Err(_) => break,
            }
        }
        let _ = tx.send(accum);
    });
    rx
}

/// Wait for `child` to exit, polling every 100ms. Returns the exit status, or
/// kills the process and panics on timeout. The caller is responsible for
/// `child.wait()` afterwards via `wait_with_output` or similar.
fn wait_with_timeout(child: &mut Child, timeout: Duration) -> std::process::ExitStatus {
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(s)) => return s,
            Ok(None) => {
                if start.elapsed() > timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("child did not exit within {:?}", timeout);
                }
                thread::sleep(Duration::from_millis(100));
            }
            Err(e) => panic!("try_wait error: {e}"),
        }
    }
}

/// Linux-only: list direct child pids of `pid` by reading
/// `/proc/<pid>/task/<pid>/children`. Returns an empty list if the file is
/// missing (process already gone) or unreadable.
fn child_pids(pid: u32) -> Vec<u32> {
    let path = format!("/proc/{pid}/task/{pid}/children");
    std::fs::read_to_string(&path)
        .ok()
        .map(|s| {
            s.split_whitespace()
                .filter_map(|p| p.parse::<u32>().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// True iff `/proc/<pid>` still exists. Adequate for "did this pid get reaped?"
/// on Linux; we only call it after the parent has been waited on, so kernel
/// reaping has already happened if the pid is going to disappear.
fn pid_alive(pid: u32) -> bool {
    std::fs::metadata(format!("/proc/{pid}")).is_ok()
}

/// Wait until `pred()` returns true or `timeout` elapses. Returns true on
/// success. Used to wait for grandchildren to appear / disappear.
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

/// Extract the response text from `pp-smoke-run`'s stdout. The orchestrator
/// prints the response between two banner lines:
///
/// ```text
/// === pipeline-parallel Inference Response ===
/// <text>
/// ============================================
/// ```
fn extract_response(stdout: &str) -> Option<String> {
    const HEADER: &str = "=== pipeline-parallel Inference Response ===";
    const FOOTER: &str = "============================================";
    let mut lines = stdout.lines();
    while let Some(line) = lines.next() {
        if line == HEADER {
            let mut body = Vec::new();
            for next in lines.by_ref() {
                if next == FOOTER {
                    return Some(body.join("\n"));
                }
                body.push(next);
            }
            return None; // header without footer → malformed
        }
    }
    None
}

/// Spawn `pp-smoke-run --seed` with the given prompt, max_tokens, and stub
/// flag. Returns the spawned process and a receiver for its stdout text.
fn spawn_smoke_run(prompt: &str, max_tokens: u32, stub: bool) -> (Child, mpsc::Receiver<String>) {
    let worker = worker_script();
    let mut cmd = Command::new(SMOKE_RUN_BIN);
    cmd.arg("--seed")
        .arg("--prompt")
        .arg(prompt)
        .arg("--max-tokens")
        .arg(max_tokens.to_string())
        .arg("--gpu-node")
        .arg(GPU_NODE_BIN)
        .arg("--worker")
        .arg(&worker)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    // The orchestrator forwards PP_WORKER_STUB to its pp-gpu-node children,
    // and pp-gpu-node forwards it to the worker. Unset == real mode.
    if stub {
        cmd.env("PP_WORKER_STUB", "1");
    } else {
        cmd.env_remove("PP_WORKER_STUB");
    }
    let mut child = cmd.spawn().expect("spawn pp-smoke-run");
    let stdout = child.stdout.take().expect("piped stdout");
    let rx = drain_stdout(stdout, "pp-smoke-run");
    (child, rx)
}

// ───────────────────────────────────────────────────────────────────────
// §10 — Binary E2E
// ───────────────────────────────────────────────────────────────────────

#[test]
fn binary_e2e_two_stub_workers_returns_hello_response() {
    let (mut smoke, stdout_rx) = spawn_smoke_run("Say hello", 4, true);
    let smoke_pid = smoke.id();

    // Wait until the orchestrator has spawned both stage children, so we can
    // verify the orphan-cleanup invariant after exit. 30s is generous; the
    // children fork within a couple of seconds even on a cold cache.
    let appeared = wait_until(Duration::from_secs(30), || child_pids(smoke_pid).len() >= 2);
    let pre_exit_pids = child_pids(smoke_pid);
    assert!(
        appeared,
        "pp-smoke-run did not spawn both pp-gpu-node children within 30s (saw {pre_exit_pids:?})"
    );

    let status = wait_with_timeout(&mut smoke, Duration::from_secs(180));
    let stdout = stdout_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap_or_default();

    assert!(
        status.success(),
        "pp-smoke-run exited with {status:?}\n--- stdout ---\n{stdout}"
    );

    let response = extract_response(&stdout).unwrap_or_else(|| {
        panic!(
            "pp-smoke-run stdout missing response banner; got:\n{stdout}"
        )
    });
    assert!(
        !response.trim().is_empty(),
        "response text between markers must be non-empty, got {response:?}"
    );

    // Children must be reaped. pp-smoke-run's ChildGuard kills both on the way
    // out, so /proc/<pid> should be gone by the time we observe the parent's
    // exit status.
    for pid in pre_exit_pids {
        assert!(
            !pid_alive(pid),
            "pp-gpu-node child pid {pid} still alive after pp-smoke-run exit"
        );
    }
}

#[test]
fn binary_e2e_two_stub_workers_cleans_up_on_failure() {
    let (mut smoke, stdout_rx) = spawn_smoke_run("Say hello", 4, true);
    let smoke_pid = smoke.id();

    // Wait for both stage children to be visible.
    let appeared = wait_until(Duration::from_secs(30), || child_pids(smoke_pid).len() >= 2);
    let pids = child_pids(smoke_pid);
    if !appeared {
        let _ = smoke.kill();
        let _ = smoke.wait();
        panic!("pp-smoke-run did not spawn both pp-gpu-node children within 30s (saw {pids:?})");
    }
    let kill_victim = pids[0];
    let other = pids[1];

    // SIGKILL one stage. The orchestrator's await_response polls its child
    // handles every iteration; a premature exit triggers a non-zero return
    // and the surviving child gets killed by the ChildGuard on drop.
    let status = Command::new("kill")
        .arg("-9")
        .arg(kill_victim.to_string())
        .status()
        .expect("kill -9");
    assert!(status.success(), "kill -9 {kill_victim} failed: {status:?}");

    let exit = wait_with_timeout(&mut smoke, Duration::from_secs(180));
    let stdout = stdout_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap_or_default();

    assert!(
        !exit.success(),
        "pp-smoke-run should fail when a stage child is killed, got {exit:?}\n--- stdout ---\n{stdout}"
    );

    // Neither grandchild should be alive. The killed one is obviously gone;
    // the other should have been reaped by pp-smoke-run's ChildGuard.
    assert!(
        !pid_alive(kill_victim),
        "killed pp-gpu-node pid {kill_victim} still in /proc"
    );
    assert!(
        !pid_alive(other),
        "orphaned pp-gpu-node pid {other} still alive after pp-smoke-run exit"
    );
}

/// Real-tinygrad variant of the happy path. Spawns the same binaries with the
/// non-stub worker. Requires:
///
/// * `python3` with `tinygrad` importable (a `.venv/bin/python` works if
///   exported via `WORKER_CMD`).
/// * `clang` on PATH — tinygrad's CPU backend compiles kernels with it.
/// * `~/.cache/tinygrad/downloads/` containing `llama3.2:1b`, or network to
///   fetch it on first run.
///
/// Run explicitly:
///   `cargo test --manifest-path examples/pipeline-parallel-inference/Cargo.toml \
///       binary_e2e_two_tinygrad_workers_returns_response -- --ignored --nocapture`
#[test]
#[ignore]
fn binary_e2e_two_tinygrad_workers_returns_response() {
    let (mut smoke, stdout_rx) = spawn_smoke_run("Say hello", 4, false);
    let smoke_pid = smoke.id();

    let appeared = wait_until(Duration::from_secs(60), || child_pids(smoke_pid).len() >= 2);
    let pre_exit_pids = child_pids(smoke_pid);
    if !appeared {
        let _ = smoke.kill();
        let _ = smoke.wait();
        panic!(
            "pp-smoke-run did not spawn both pp-gpu-node children within 60s (saw {pre_exit_pids:?})"
        );
    }

    // Real-mode worker load (GGUF fetch + tinygrad realize) can take minutes
    // on a cold cache; the per-token CPU forward dominates the rest.
    let status = wait_with_timeout(&mut smoke, Duration::from_secs(900));
    let stdout = stdout_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap_or_default();

    assert!(
        status.success(),
        "pp-smoke-run exited with {status:?}\n--- stdout ---\n{stdout}"
    );

    let response = extract_response(&stdout).unwrap_or_else(|| {
        panic!("pp-smoke-run stdout missing response banner; got:\n{stdout}")
    });
    assert!(
        !response.trim().is_empty(),
        "response text between markers must be non-empty, got {response:?}"
    );

    for pid in pre_exit_pids {
        assert!(
            !pid_alive(pid),
            "pp-gpu-node child pid {pid} still alive after pp-smoke-run exit"
        );
    }
}
