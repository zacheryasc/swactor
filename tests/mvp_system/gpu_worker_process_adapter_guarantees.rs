//! Black-box contract tests for the MVP GPU worker process adapter.
//!
//! These tests intentionally know only the public stdin/stdout adapter surface:
//!
//! - command JSON lines written to stdin
//! - stdout JSON event lines and stderr diagnostic lines read back
//! - adapter events and process faults out
//!
//! They assert the guarantees in
//! `specs/mvp_system/gpu_worker_process_adapter_contract.md`.

use mvp_system::gpu_worker_process_adapter as adapter;

// The adapter config supplies arena environment variables and an ABI version.
// Tests do not assume how Python maps the arena or initializes tinygrad.
fn adapter_config() -> adapter::AdapterConfig {
    adapter::AdapterConfig {
        arena_fd: adapter::ArenaFd(3),
        arena_bytes: 4096,
        helper_abi_version: adapter::HelperAbiVersion(1),
    }
}

// The harness exposes fake stdin/stdout/stderr lines at the process boundary.
// It does not expose worker internals or controller routing.
fn new_adapter() -> adapter::ProcessAdapterHarness {
    adapter::ProcessAdapterHarness::new(adapter_config())
}

// Valid control commands carry identities and handles, never payload bytes.
// The command list exercises framing without depending on a specific JSON key
// order.
fn valid_commands() -> Vec<adapter::WorkerCommand> {
    vec![
        adapter::WorkerCommand::InitializeWorker {
            helper_abi_version: adapter::HelperAbiVersion(1),
        },
        adapter::WorkerCommand::InstallRing {
            ring_id: adapter::RingId(8001),
        },
        adapter::WorkerCommand::ExecuteStep {
            step_id: adapter::StepId(9001),
        },
        adapter::WorkerCommand::ReleaseDeviceObject {
            handle: adapter::DeviceHandle(42),
        },
        adapter::WorkerCommand::ShutdownWorker,
    ]
}

// This helper proves payload bytes are absent from JSON values by checking the
// public command/event representation before it crosses the process boundary.
fn assert_no_payload_bytes(value: &adapter::JsonLine) {
    assert!(
        !value.contains_key("payload")
            && !value.contains_key("bytes")
            && !value.contains_key("data"),
        "control JSON carried payload bytes: {value:?}"
    );
}

// This proves commands and events are one JSON object per line, stderr is only
// diagnostics, and payload bytes are forbidden in control JSON.
#[test]
fn control_stream_is_line_framed_json_without_payload_bytes() {
    // Send every valid command through the adapter.
    let mut harness = new_adapter();
    for command in valid_commands() {
        harness.send_command(command);
    }

    // Every stdin write must be exactly one JSON object followed by one newline.
    for line in harness.stdin_lines() {
        assert!(line.ends_with('\n'));
        let parsed = adapter::JsonLine::parse(line).expect("stdin line must parse");
        assert!(parsed.is_object());
        assert_no_payload_bytes(&parsed);
    }

    // Stdout events are also one JSON object per line.
    harness.receive_stdout_line(r#"{"type":"WorkerReady","generation":1}"#);
    let event = harness.events().last().expect("stdout event must parse");
    assert!(matches!(event, adapter::AdapterEvent::WorkerReady { .. }));

    // Stderr alone is diagnostic and does not define lifecycle state.
    harness.receive_stderr_line("loading tinygrad backend");
    assert!(!harness.events().iter().any(|event| {
        matches!(event, adapter::AdapterEvent::WorkerFatal { .. })
    }));
}

// This proves worker initialization reads arena environment, waits for
// InitializeWorker, maps the arena, initializes the helper and backend, and then
// emits WorkerReady; failures emit WorkerFatal if possible and exit non-zero.
#[test]
fn initialization_order_is_env_initialize_map_helper_backend_ready() {
    // Start the worker process with arena environment.
    let mut harness = new_adapter();
    harness.start_worker_process();

    // Before InitializeWorker, no mapping or backend initialization may occur.
    assert!(!harness.worker_actions().iter().any(|action| {
        matches!(action, adapter::WorkerAction::MapArena { .. })
            || matches!(action, adapter::WorkerAction::InitializeBackend { .. })
    }));

    // Send InitializeWorker and observe ordered worker actions.
    harness.send_command(adapter::WorkerCommand::InitializeWorker {
        helper_abi_version: adapter::HelperAbiVersion(1),
    });
    let actions = harness.worker_actions();
    let env_pos = actions
        .iter()
        .position(|action| matches!(action, adapter::WorkerAction::ReadArenaEnvironment { .. }))
        .expect("worker must read arena env");
    let map_pos = actions
        .iter()
        .position(|action| matches!(action, adapter::WorkerAction::MapArena { .. }))
        .expect("worker must map arena");
    let helper_pos = actions
        .iter()
        .position(|action| matches!(action, adapter::WorkerAction::InitializeRingHelper { .. }))
        .expect("worker must init helper");
    let backend_pos = actions
        .iter()
        .position(|action| matches!(action, adapter::WorkerAction::InitializeBackend { .. }))
        .expect("worker must init backend");
    assert!(env_pos < map_pos && map_pos < helper_pos && helper_pos < backend_pos);

    // Successful initialization emits WorkerReady.
    harness.receive_stdout_line(r#"{"type":"WorkerReady","generation":1}"#);
    assert!(harness.events().iter().any(|event| {
        matches!(event, adapter::AdapterEvent::WorkerReady { generation: adapter::WorkerGeneration(1) })
    }));

    // Initialization failure emits WorkerFatal if possible and exits non-zero.
    let mut failed = new_adapter();
    failed.start_worker_process();
    failed.inject_initialization_failure(adapter::InitializationFailure::BackendUnavailable);
    assert!(failed.events().iter().any(|event| {
        matches!(event, adapter::AdapterEvent::WorkerFatal { .. })
    }));
    assert_ne!(failed.exit_status(), Some(adapter::ExitStatus::Code(0)));
}

// This proves invalid JSON, unknown event shapes, and unsupported helper ABI are
// worker/process faults, while stderr output alone is not lifecycle state.
#[test]
fn parsing_and_abi_errors_fault_the_worker_process() {
    // Invalid JSON faults the adapter.
    let mut invalid_json = new_adapter();
    invalid_json.receive_stdout_line("{not-json");
    assert!(invalid_json.events().iter().any(|event| {
        matches!(event, adapter::AdapterEvent::ProcessFault { reason: adapter::ProcessFaultReason::InvalidJson, .. })
    }));

    // Unknown event shape faults the adapter.
    let mut unknown = new_adapter();
    unknown.receive_stdout_line(r#"{"type":"NotAWorkerEvent"}"#);
    assert!(unknown.events().iter().any(|event| {
        matches!(event, adapter::AdapterEvent::ProcessFault { reason: adapter::ProcessFaultReason::UnknownEventShape, .. })
    }));

    // Unsupported helper ABI emits WorkerFatal.
    let mut abi = new_adapter();
    abi.send_command(adapter::WorkerCommand::InitializeWorker {
        helper_abi_version: adapter::HelperAbiVersion(999),
    });
    assert!(abi.events().iter().any(|event| {
        matches!(event, adapter::AdapterEvent::WorkerFatal { reason: adapter::WorkerFatalReason::UnsupportedHelperAbi, .. })
    }));
}

// This proves command discipline: InstallRing, wake hints, ExecuteStep,
// ReleaseDeviceObject, and ShutdownWorker are accepted only as control messages
// and payload-bearing control JSON is rejected.
#[test]
fn command_discipline_rejects_payload_bearing_control_messages() {
    // Valid control commands are accepted and framed.
    let mut harness = new_adapter();
    for command in valid_commands() {
        harness.send_command(command);
    }
    assert_eq!(harness.command_rejections().len(), 0);

    // Payload bytes in a control command are rejected at the adapter boundary.
    harness.send_raw_json_command(r#"{"type":"ExecuteStep","step_id":1,"payload":[1,2,3]}"#);
    assert!(harness.command_rejections().iter().any(|rejection| {
        matches!(rejection.reason, adapter::CommandRejectionReason::PayloadBytesForbidden)
    }));

    // Wake hints reload cursors; they do not carry byte ranges or credits.
    harness.send_command(adapter::WorkerCommand::RingReadable {
        ring_id: adapter::RingId(8001),
    });
    let wake_line = harness.stdin_lines().last().expect("wake command must be written");
    let parsed = adapter::JsonLine::parse(wake_line).expect("wake line must parse");
    assert_no_payload_bytes(&parsed);
    assert!(!parsed.contains_key("range") && !parsed.contains_key("credits"));
}
