use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn local_e2e_binary_drives_real_local_process_deployment() {
    let output = Command::new(env!("CARGO_BIN_EXE_mvp-local-e2e"))
        .output()
        .expect("run mvp-local-e2e");

    assert!(
        output.status.success(),
        "mvp-local-e2e failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json stdout");
    assert_eq!(value["ok"], true);
    assert_eq!(value["actor_plane"], "iroh-swactor");
    assert_eq!(value["data_plane"], "tcp-loopback-streams");
    assert_eq!(value["worker_processes"], "mvp-dumb-worker-per-node");
    assert_eq!(
        value["engine_builder_pattern"],
        "pool-first-static-launcher"
    );
    assert_eq!(value["engine_builder_node_count"], 3);
    assert_eq!(value["engine_builder_stage_assignments"], 2);
    assert!(
        value["engine_builder_event_count"]
            .as_u64()
            .is_some_and(|count| count >= 10),
        "{value}"
    );
    assert_eq!(value["injected_prompt_observed"], true);
    assert_eq!(value["token_received_observed"], true);
    assert_eq!(value["run_completed_observed"], true);
    assert_eq!(value["run_torn_down_observed"], true);
    assert_eq!(value["stop_sent_to_all_nodes"], true);
    assert_eq!(value["stage_ready_stdout_count"], 2);
    assert!(value["processes"]["node0"].as_u64().is_some(), "{value}");
    assert!(value["processes"]["node1"].as_u64().is_some(), "{value}");
    assert!(
        value["node0_endpoint"]["addrs"]
            .as_array()
            .is_some_and(|addrs| !addrs.is_empty()),
        "{value}"
    );
    assert!(
        value["node1_endpoint"]["addrs"]
            .as_array()
            .is_some_and(|addrs| !addrs.is_empty()),
        "{value}"
    );
}

#[test]
fn dumb_worker_is_a_real_child_process_protocol_endpoint() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_mvp-dumb-worker"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mvp-dumb-worker");

    {
        let stdin = child.stdin.as_mut().expect("worker stdin");
        writeln!(
            stdin,
            "{{\"type\":\"InitializeWorker\",\"helper_abi_version\":1}}"
        )
        .expect("write initialize");
        writeln!(stdin, "{{\"type\":\"ExecuteStep\",\"step_id\":7}}").expect("write execute");
        writeln!(stdin, "{{\"type\":\"ShutdownWorker\"}}").expect("write shutdown");
    }

    let output = child.wait_with_output().expect("worker output");
    assert!(
        output.status.success(),
        "worker failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    assert!(stdout.contains("\"type\":\"WorkerReady\""), "{stdout}");
    assert!(stdout.contains("\"type\":\"StepCompleted\""), "{stdout}");
    assert!(stdout.contains("\"step_id\":7"), "{stdout}");
    assert!(stdout.contains("\"type\":\"WorkerStopped\""), "{stdout}");
}
