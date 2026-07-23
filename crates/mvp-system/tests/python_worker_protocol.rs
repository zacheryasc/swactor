#![cfg(target_os = "linux")]

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};

use serde_json::{Value, json};

struct WorkerProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
}

impl WorkerProcess {
    fn spawn() -> Self {
        let script = worker_script();
        let mut child = Command::new("python3")
            .arg(&script)
            .env("DEV", "CPU")
            .env("MVP_RUN_ID", "9")
            .env("MVP_LOGICAL_NODE_ID", "3")
            .env("MVP_STAGE_INDEX", "2")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {}: {e}", script.display()));
        let stdin = child.stdin.take().expect("worker stdin");
        let stdout = BufReader::new(child.stdout.take().expect("worker stdout"));
        Self {
            child,
            stdin,
            stdout,
        }
    }

    fn send_collect_until(&mut self, command: Value, expected_type: &str) -> Vec<Value> {
        writeln!(self.stdin, "{command}").expect("write worker command");
        self.stdin.flush().expect("flush worker command");
        let mut events = Vec::new();
        loop {
            let mut line = String::new();
            let read = self.stdout.read_line(&mut line).expect("read worker event");
            assert_ne!(read, 0, "worker exited before {expected_type}");
            let event: Value = serde_json::from_str(line.trim_end()).expect("worker event JSON");
            let actual_type = event.get("type").and_then(Value::as_str).unwrap_or("");
            assert_ne!(
                actual_type, "WorkerFatal",
                "worker fatal while waiting for {expected_type}: {event}"
            );
            let done = actual_type == expected_type;
            events.push(event);
            if done {
                return events;
            }
        }
    }
}

impl Drop for WorkerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn benchmark_observability_worker_ready_includes_stamps_and_identity() {
    if !tinygrad_available() {
        eprintln!("skipping Python worker protocol check: tinygrad is unavailable");
        return;
    }

    let mut worker = WorkerProcess::spawn();
    let events = worker.send_collect_until(
        json!({"type":"InitializeWorker","helper_abi_version":1,"backend":{"device":"CPU"}}),
        "WorkerReady",
    );

    assert!(
        events
            .iter()
            .any(|event| event.get("type").and_then(Value::as_str) == Some("TinygradImportStarted")),
        "expected tinygrad import milestone in {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| event.get("type").and_then(Value::as_str) == Some("WorkerReady")),
        "expected WorkerReady in {events:?}"
    );
    for event in &events {
        assert_eq!(
            event
                .get("benchmark")
                .and_then(|benchmark| benchmark.get("schema"))
                .and_then(Value::as_u64),
            Some(1),
            "{event}"
        );
        assert_eq!(
            event.get("run_id").and_then(Value::as_u64),
            Some(9),
            "{event}"
        );
        assert_eq!(
            event.get("node_id").and_then(Value::as_u64),
            Some(3),
            "{event}"
        );
        assert_eq!(
            event.get("stage_index").and_then(Value::as_u64),
            Some(2),
            "{event}"
        );
    }
}

fn tinygrad_available() -> bool {
    Command::new("python3")
        .args(["-c", "import tinygrad"])
        .status()
        .is_ok_and(|status| status.success())
}

fn worker_script() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("apps/mvp-node/tinygrad_worker.py")
        .canonicalize()
        .expect("tinygrad worker script exists")
}
