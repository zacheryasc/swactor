//! Binary integration test — validates gpu-node and smoke-run work end-to-end.
//!
//! Spawns both binaries as child processes on localhost. gpu-node runs an
//! InferenceActor backed by a Python worker; smoke-run connects, sends an
//! InferenceRequest through the distributed pipeline, and receives the response.
//!
//! This proves the binaries work through actual process boundaries — the same
//! code path used in production, unlike the in-process tests in t_integration.rs.
//!
//! - `binary_e2e_echo_worker` — fast, uses echo_worker.py (canned echo responses).
//! - `binary_e2e_tinygrad` — slow (#[ignore]), uses real tinygrad inference.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// Parse the GPU_NODE_ADDR line from gpu-node's stderr.
/// Format: `GPU_NODE_ADDR <hex_node_id> <ip:port,ip:port,...>`
fn parse_gpu_node_addr(line: &str) -> Option<(String, String)> {
    let line = line.trim();
    if !line.starts_with("GPU_NODE_ADDR ") {
        return None;
    }
    let parts: Vec<&str> = line.splitn(3, ' ').collect();
    if parts.len() < 3 {
        return None;
    }
    Some((parts[1].to_string(), parts[2].to_string()))
}

/// Start gpu-node, wait for it to print its address, run smoke-run, verify response.
fn run_binary_e2e(worker_cmd: &str, worker_script: &str, extra_env: Vec<(&str, &str)>) {
    let gpu_node_bin = env!("CARGO_BIN_EXE_gpu-node");
    let smoke_run_bin = env!("CARGO_BIN_EXE_single-gpu-inference");

    // 1. Start gpu-node without SEED_ADDR (it just listens for connections)
    let mut cmd = Command::new(gpu_node_bin);
    cmd.env("WORKER_CMD", worker_cmd)
        .env("WORKER_SCRIPT", worker_script)
        .env_remove("SEED_ADDR")
        .stderr(Stdio::piped());
    for (k, v) in &extra_env {
        cmd.env(k, v);
    }
    let mut gpu_node = cmd.spawn().expect("failed to spawn gpu-node");

    // 2. Read gpu-node stderr in a background thread to find its address
    //    and keep draining so the pipe buffer doesn't fill up.
    let gpu_stderr = gpu_node.stderr.take().unwrap();
    let (addr_tx, addr_rx) = std::sync::mpsc::channel::<(String, String)>();
    let stderr_thread = std::thread::spawn(move || {
        let reader = BufReader::new(gpu_stderr);
        let mut sent = false;
        for line in reader.lines().flatten() {
            eprintln!("[gpu-node] {}", line);
            if !sent {
                if let Some(addr_info) = parse_gpu_node_addr(&line) {
                    let _ = addr_tx.send(addr_info);
                    sent = true;
                }
            }
        }
    });

    // 3. Wait for gpu-node to print its address
    let (node_id, direct_addrs) = addr_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("gpu-node did not print GPU_NODE_ADDR within 30s");
    eprintln!("gpu-node ready: node_id={node_id}, direct={direct_addrs}");

    // 4. Spawn smoke-run pointing at gpu-node
    let mut smoke_run = Command::new(smoke_run_bin)
        .arg("--seed")
        .arg(&node_id)
        .arg("--seed-direct")
        .arg(&direct_addrs)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn smoke-run");

    // 5. Wait for smoke-run with timeout (poll every 200ms, bail after 120s)
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(120);
    loop {
        match smoke_run.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) => {
                if start.elapsed() > timeout {
                    let _ = smoke_run.kill();
                    let _ = smoke_run.wait();
                    cleanup(&mut gpu_node, stderr_thread);
                    panic!("smoke-run did not exit within {timeout:?}");
                }
                std::thread::sleep(Duration::from_millis(200));
            }
            Err(e) => {
                cleanup(&mut gpu_node, stderr_thread);
                panic!("error waiting for smoke-run: {e}");
            }
        }
    }
    let output = smoke_run.wait_with_output().expect("failed to read smoke-run output");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr_out = String::from_utf8_lossy(&output.stderr);
    eprintln!("=== smoke-run stderr ===\n{stderr_out}");
    eprintln!("=== smoke-run stdout ===\n{stdout}");

    // 6. Clean up gpu-node
    cleanup(&mut gpu_node, stderr_thread);

    // 7. Verify
    assert!(
        output.status.success(),
        "smoke-run exited with {:?}",
        output.status
    );
    assert!(
        stdout.contains("=== Inference Response ==="),
        "stdout should contain response header"
    );

    // Extract and verify the response text
    let response_text: String = stdout
        .lines()
        .skip_while(|l| *l != "=== Inference Response ===")
        .skip(1) // skip the header itself
        .take_while(|l| *l != "==========================")
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !response_text.is_empty(),
        "response text between markers should be non-empty"
    );
    eprintln!("response: {response_text:?}");
}

fn cleanup(gpu_node: &mut Child, stderr_thread: std::thread::JoinHandle<()>) {
    let _ = gpu_node.kill();
    let _ = gpu_node.wait();
    let _ = stderr_thread.join();
}

#[test]
fn binary_e2e_echo_worker() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let script = format!("{manifest_dir}/echo_worker.py");
    run_binary_e2e("python3", &script, vec![]);
}

/// Full binary e2e with real tinygrad inference (~1B GGUF model).
///
/// Requires `.venv` with tinygrad installed and downloads a large model.
/// Run explicitly with:
///   cargo test --package smoke-test binary_e2e_tinygrad -- --ignored
#[test]
#[ignore]
fn binary_e2e_tinygrad() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let python = format!("{manifest_dir}/.venv/bin/python");
    let script = format!("{manifest_dir}/tinygrad_worker.py");
    run_binary_e2e(&python, &script, vec![]);
}
