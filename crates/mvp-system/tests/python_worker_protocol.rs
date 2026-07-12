#![cfg(target_os = "linux")]

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};

use mvp_system::arena_manager as arena;
use serde_json::{Value, json};

const HEADER_LEN: usize = 40;

struct WorkerProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
}

impl WorkerProcess {
    fn spawn(arena: Option<(&arena::ArenaManager, u64)>) -> Self {
        let script = worker_script();
        let mut command = Command::new("python3");
        command
            .arg(&script)
            .env("MVP_TINYGRAD_TEST_MODE", "1")
            .env("DEV", "CPU")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some((arena, arena_bytes)) = arena {
            command
                .env("MVP_ARENA_FD", arena.arena_fd().to_string())
                .env("MVP_ARENA_BYTES", arena_bytes.to_string());
        }
        let mut child = command
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

    fn send_expect(&mut self, command: Value, expected_type: &str) -> Value {
        writeln!(self.stdin, "{command}").expect("write worker command");
        self.stdin.flush().expect("flush worker command");
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
            if actual_type == expected_type {
                return event;
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
fn test_mode_tokenizer_commands_round_trip_prompt_bytes_and_visible_tokens() {
    let mut worker = WorkerProcess::spawn(None);
    worker.send_expect(
        json!({"type":"InitializeWorker","helper_abi_version":1,"backend":{"device":"CPU"}}),
        "WorkerReady",
    );
    worker.send_expect(
        json!({
            "type":"LoadWeights",
            "model_id":"test-model",
            "gguf_source":{"LocalPath":"/tmp/not-used-in-test-mode.gguf"},
            "tokenizer":{"EmbeddedGguf":{}},
            "layer_start":0,
            "layer_end_exclusive":1
        }),
        "WeightsLoaded",
    );

    let encoded = worker.send_expect(
        json!({"type":"EncodePrompt","request_id":7,"prompt":"Hi!"}),
        "PromptEncoded",
    );
    assert_eq!(encoded.get("request_id").and_then(Value::as_u64), Some(7));
    assert_eq!(
        encoded
            .get("tokens")
            .and_then(Value::as_array)
            .expect("encoded tokens")
            .iter()
            .map(|value| value.as_u64().expect("token is u64"))
            .collect::<Vec<_>>(),
        vec![72, 105, 33]
    );

    let decoded = worker.send_expect(
        json!({"type":"DecodeTokens","request_id":8,"tokens":[72,105,33,6]}),
        "TokensDecoded",
    );
    assert_eq!(decoded.get("request_id").and_then(Value::as_u64), Some(8));
    assert_eq!(
        decoded.get("text").and_then(Value::as_str),
        Some("Hi!<tok:6>")
    );
}

#[test]
fn test_mode_worker_executes_single_and_three_stage_mo01_flow_through_real_arena() {
    let arena_bytes = 16 * 1024;
    let mut arena = arena::ArenaManager::boot(arena::ArenaConfig {
        node_id: arena::NodeId(1),
        reservation_ceiling: arena_bytes,
        base_alignment: 64,
    })
    .expect("arena boots");
    let ingress = lease_ring(&mut arena, 1, 1024, 64);
    let egress = lease_ring(&mut arena, 2, 1024, 64);
    let mut worker = WorkerProcess::spawn(Some((&arena, arena_bytes)));
    worker.send_expect(
        json!({"type":"InitializeWorker","helper_abi_version":1,"backend":{"device":"CPU"}}),
        "WorkerReady",
    );

    let single_token = run_stage(
        &mut worker,
        &arena,
        &ingress,
        &egress,
        StageFixture {
            stage_index: 0,
            layer_start: 0,
            layer_end_exclusive: 7,
            final_stage: true,
        },
        900,
        &[2, 3],
    );
    assert_eq!(
        single_token,
        vec![6],
        "N=1 stage should emit a token record"
    );

    let stage0 = run_stage(
        &mut worker,
        &arena,
        &ingress,
        &egress,
        StageFixture {
            stage_index: 0,
            layer_start: 0,
            layer_end_exclusive: 3,
            final_stage: false,
        },
        901,
        &[2, 3],
    );
    assert_eq!(stage0, vec![8], "stage 0 activation fixture word");

    let stage1 = run_stage(
        &mut worker,
        &arena,
        &ingress,
        &egress,
        StageFixture {
            stage_index: 1,
            layer_start: 3,
            layer_end_exclusive: 5,
            final_stage: false,
        },
        902,
        &stage0,
    );
    assert_eq!(stage1, vec![17], "stage 1 activation fixture word");

    let stage2 = run_stage(
        &mut worker,
        &arena,
        &ingress,
        &egress,
        StageFixture {
            stage_index: 2,
            layer_start: 5,
            layer_end_exclusive: 7,
            final_stage: true,
        },
        903,
        &stage1,
    );
    assert_eq!(stage2, vec![6], "final stage should emit a token record");
}

#[derive(Clone, Copy)]
struct StageFixture {
    stage_index: u32,
    layer_start: u32,
    layer_end_exclusive: u32,
    final_stage: bool,
}

fn run_stage(
    worker: &mut WorkerProcess,
    arena: &arena::ArenaManager,
    ingress: &arena::RingLease,
    egress: &arena::RingLease,
    stage: StageFixture,
    output_object_id: u64,
    input_words: &[u32],
) -> Vec<u32> {
    worker.send_expect(
        json!({
            "type":"ConfigureRole",
            "role_id":1,
            "config":{
                "run_id":1,
                "stage_index":stage.stage_index,
                "layer_start":stage.layer_start,
                "layer_end_exclusive":stage.layer_end_exclusive
            }
        }),
        "RoleConfigured",
    );
    worker.send_expect(
        json!({
            "type":"LoadWeights",
            "model_id":"test-model",
            "gguf_source":{"LocalPath":"/tmp/not-used-in-test-mode.gguf"},
            "tokenizer":{"EmbeddedGguf":{}},
            "layer_start":stage.layer_start,
            "layer_end_exclusive":stage.layer_end_exclusive
        }),
        "WeightsLoaded",
    );
    install_ring(worker, ingress, 1, 10, "ingress", 4096, 4);
    install_ring(worker, egress, 2, 11, "egress", 4096, 4);

    let input_payload = words_payload(input_words);
    arena
        .write_arena(
            ingress.layout.data_offset,
            &object_record(100 + u64::from(stage.stage_index), 0, 0, &input_payload),
        )
        .expect("write ingress record");
    let loaded = worker.send_expect(json!({"type":"RingReadable","ring_id":1}), "ObjectLoaded");
    let handle_id = loaded
        .get("handle_id")
        .and_then(Value::as_u64)
        .expect("handle id");
    worker.send_expect(
        json!({
            "type":"ExecuteStep",
            "role_id":1,
            "step_id":u64::from(stage.stage_index) + 1,
            "input_handle_id":handle_id,
            "input_object_id":100 + u64::from(stage.stage_index),
            "input_sequence":0,
            "output_ring_id":2,
            "output_object_id":output_object_id,
            "output_sequence":0,
            "final_stage":stage.final_stage
        }),
        "StepExecuted",
    );
    let output = arena
        .read_arena(egress.layout.data_offset, HEADER_LEN + 4)
        .expect("read egress record");
    decode_record_words(&output, output_object_id)
}

fn install_ring(
    worker: &mut WorkerProcess,
    lease: &arena::RingLease,
    ring_id: u64,
    edge_id: u64,
    direction: &str,
    max_extent: u64,
    alignment: u32,
) {
    worker.send_expect(
        json!({
            "type":"InstallRing",
            "ring_id":ring_id,
            "edge_id":edge_id,
            "port":direction,
            "direction":direction,
            "layout":{
                "data_offset":lease.layout.data_offset,
                "data_bytes":lease.layout.data_bytes
            },
            "object_spec":{
                "max_extent":max_extent,
                "alignment":alignment
            }
        }),
        "RingInstalled",
    );
}

fn lease_ring(
    arena: &mut arena::ArenaManager,
    request_id: u64,
    data_bytes: u64,
    alignment: u64,
) -> arena::RingLease {
    let events = arena.request(arena::ArenaRequest::LeaseRing(arena::LeaseRing {
        request_id: arena::LeaseRequestId(request_id),
        ring_spec: arena::RingSpec {
            header_bytes: 64,
            data_bytes,
            alignment,
        },
    }));
    match events.into_iter().next().expect("arena event") {
        arena::ArenaEvent::RingLeased { lease } => lease,
        event => panic!("expected ring lease, got {event:?}"),
    }
}

fn object_record(object_id: u64, sequence: u64, flags: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0_u8; HEADER_LEN];
    bytes[0..4].copy_from_slice(b"MO01");
    bytes[4..6].copy_from_slice(&1_u16.to_le_bytes());
    bytes[6..8].copy_from_slice(&(HEADER_LEN as u16).to_le_bytes());
    bytes[8..16].copy_from_slice(&object_id.to_le_bytes());
    bytes[16..24].copy_from_slice(&sequence.to_le_bytes());
    bytes[24..32].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes[32..36].copy_from_slice(&flags.to_le_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

fn words_payload(words: &[u32]) -> Vec<u8> {
    words
        .iter()
        .flat_map(|word| word.to_le_bytes())
        .collect::<Vec<_>>()
}

fn decode_record_words(record: &[u8], expected_object_id: u64) -> Vec<u32> {
    assert!(record.len() >= HEADER_LEN);
    assert_eq!(&record[0..4], b"MO01");
    assert_eq!(u16::from_le_bytes(record[4..6].try_into().unwrap()), 1);
    assert_eq!(
        u16::from_le_bytes(record[6..8].try_into().unwrap()),
        HEADER_LEN as u16
    );
    assert_eq!(
        u64::from_le_bytes(record[8..16].try_into().unwrap()),
        expected_object_id
    );
    let extent = u64::from_le_bytes(record[24..32].try_into().unwrap()) as usize;
    assert_eq!(extent % 4, 0);
    record[HEADER_LEN..HEADER_LEN + extent]
        .chunks_exact(4)
        .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
        .collect()
}

fn worker_script() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("apps/mvp-node/tinygrad_worker.py")
        .canonicalize()
        .expect("tinygrad worker script exists")
}
