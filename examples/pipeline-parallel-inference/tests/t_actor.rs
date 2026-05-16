//! T-actor: Stage0Actor / Stage1Actor + process-bridge component tests.
//!
//! All tests run against the stub-mode pp_tinygrad_worker. No GPU, no real
//! model. Test names match TEST_SPEC §4 verbatim.
//!
//! Worker stub constants we depend on (defined in `pp_tinygrad_worker.py`):
//!   - hidden bytes per token = STUB_HIDDEN_DIM * BYTES_PER_ELEM = 16 * 2 = 32
//!   - token_id range = [0, STUB_VOCAB_SIZE) = [0, 32)
//! Tests assert membership, never specific values, so the actor's contract
//! stays decoupled from the worker's pseudo-hash.

use std::collections::HashMap;
use std::process::Command;
use std::time::{Duration, Instant};

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Inbox, Runtime, RuntimeConfig};

use pipeline_parallel_inference::messages::{
    InferenceRequest, InferenceResponse, NextToken, StageActivation,
};
use pipeline_parallel_inference::stage_actor::{
    Stage0Actor, Stage0Msg, Stage1Actor, Stage1Msg, StageActorStatus, stub_tokenize_prompt,
};
use swactor_process::{ProcessMode, ProcessSpec};

const STUB_BYTES_PER_TOKEN: usize = 16 * 2; // STUB_HIDDEN_DIM * BYTES_PER_ELEM
const STUB_VOCAB_SIZE: u32 = 32;

// ─── Helpers ──────────────────────────────────────────────────────────────

fn worker_spec(stage: u32, num_stages: u32) -> ProcessSpec {
    let mut env = HashMap::new();
    env.insert("STAGE".to_string(), stage.to_string());
    env.insert("NUM_STAGES".to_string(), num_stages.to_string());
    env.insert("PP_WORKER_STUB".to_string(), "1".to_string());
    ProcessSpec {
        command: "python3".into(),
        args: vec![format!("{}/pp_tinygrad_worker.py", env!("CARGO_MANIFEST_DIR"))],
        env,
        working_dir: None,
        mode: ProcessMode::Automated,
        initial_pty_size: None,
        kill_timeout: Some(Duration::from_secs(2)),
        stdin_buffer_limit: None,
    }
}

fn tick_until_recv<M: swactor::actor::Message>(
    rt: &Runtime,
    inbox: &Inbox<M>,
    timeout: Duration,
) -> Option<M> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        rt.tick();
        if let Some(msg) = inbox.try_recv() {
            return Some(msg);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    None
}

fn drain_until_ready(
    rt: &Runtime,
    status_inbox: &Inbox<StageActorStatus>,
    timeout: Duration,
) -> u32 {
    let mut saw_started = false;
    let mut pid: Option<u32> = None;
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(status) = tick_until_recv(rt, status_inbox, Duration::from_millis(100)) {
            match status {
                StageActorStatus::ProcessStarted => saw_started = true,
                StageActorStatus::WorkerReady { pid: p } => {
                    assert!(saw_started, "WorkerReady should come after ProcessStarted");
                    pid = p;
                    break;
                }
                other => panic!("unexpected status during startup: {other:?}"),
            }
        }
    }
    pid.expect("worker should report a pid within timeout")
}

fn is_process_alive(pid: u32) -> bool {
    std::fs::metadata(format!("/proc/{pid}")).is_ok()
}

fn dummy_addr(rt: &Runtime) -> ActorAddress {
    // Some message types require an `ActorAddress` field the actor under
    // test doesn't read — point them at an unread inbox so they're real
    // but inert.
    *rt.new_inbox::<DummySink>().unwrap().addr()
}

#[derive(Clone, Debug)]
struct DummySink;

// A trivial runtime-survival probe; mirrors the single-GPU pattern.
#[derive(Clone)]
struct Ping {
    reply_to: ActorAddress,
}
#[derive(Clone, Debug, PartialEq)]
struct Pong;

struct PongActor;
impl ActorInterface for PongActor {
    type Incoming = Ping;
    type Response = Pong;
    fn handle(&mut self, ctx: &swactor::runtime::Ctx, msg: Ping) {
        let _ = ctx.send(msg.reply_to, Pong);
    }
}

fn assert_runtime_alive(rt: &Runtime) {
    let pong_inbox = rt.new_inbox::<Pong>().unwrap();
    let pong_addr = rt.spawn(PongActor).unwrap();
    rt.send_to(pong_addr, Ping { reply_to: *pong_inbox.addr() })
        .unwrap();
    let pong = tick_until_recv(rt, &pong_inbox, Duration::from_secs(2));
    assert_eq!(pong, Some(Pong), "runtime should still be functional");
}

fn kill_pid(pid: u32) {
    let _ = Command::new("kill")
        .arg("-9")
        .arg(pid.to_string())
        .status();
}

// ═══════════════════════════════════════════════════════════════════════
// Stage 0
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn stage_0_actor_spawns_worker_and_reports_ready() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let sender = rt.create_sender();
    let actor = Stage0Actor::new(worker_spec(0, 2), sender, *activation_inbox.addr())
        .with_status_addr(*status_inbox.addr());
    let _addr = rt.spawn(actor).unwrap();

    let pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));
    assert!(is_process_alive(pid), "stage-0 worker should be running");
}

#[test]
fn inference_request_produces_stage_activation_to_next_address() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let sender = rt.create_sender();
    let actor = Stage0Actor::new(worker_spec(0, 2), sender, *activation_inbox.addr())
        .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    let prompt = "say hello there";
    let expected_token_count = stub_tokenize_prompt(prompt).len() as u32;
    assert!(expected_token_count > 0);

    rt.send_to(
        addr,
        Stage0Msg::Inference(InferenceRequest {
            reply_to: dummy_addr(&rt),
            prompt: prompt.into(),
            max_tokens: 8,
        }),
    )
    .unwrap();

    let activation = tick_until_recv(&rt, &activation_inbox, Duration::from_secs(5))
        .expect("should receive StageActivation from stage-0 actor");

    assert!(activation.is_prefill, "prefill flag must be set on first hop");
    assert_eq!(activation.seq_len, expected_token_count);
    assert_eq!(activation.position, 0);
    assert_eq!(
        activation.hidden.len(),
        expected_token_count as usize * STUB_BYTES_PER_TOKEN,
        "hidden byte length must match seq_len * hidden_dim * 2"
    );
    assert!(
        activation.hidden.iter().any(|&b| b != 0),
        "stub hidden state must be non-trivial"
    );
}

#[test]
fn next_token_produces_stage_activation_for_decode_step() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let sender = rt.create_sender();
    let actor = Stage0Actor::new(worker_spec(0, 2), sender, *activation_inbox.addr())
        .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    rt.send_to(
        addr,
        Stage0Msg::NextToken(NextToken {
            request_id: 0xCAFE,
            token_id: 7,
            position: 11,
            done: false,
        }),
    )
    .unwrap();

    let activation = tick_until_recv(&rt, &activation_inbox, Duration::from_secs(5))
        .expect("should receive decode-step StageActivation");

    assert!(!activation.is_prefill);
    assert_eq!(activation.seq_len, 1);
    assert_eq!(activation.position, 11, "decode position must match NextToken");
    assert_eq!(activation.request_id, 0xCAFE);
    assert_eq!(activation.hidden.len(), STUB_BYTES_PER_TOKEN);
}

#[test]
fn next_token_with_done_produces_no_further_activations() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let sender = rt.create_sender();
    let actor = Stage0Actor::new(worker_spec(0, 2), sender, *activation_inbox.addr())
        .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    rt.send_to(
        addr,
        Stage0Msg::NextToken(NextToken {
            request_id: 1,
            token_id: 7,
            position: 5,
            done: true,
        }),
    )
    .unwrap();

    // Tick for a generous window — long enough that any spuriously emitted
    // activation would have arrived.
    let observed = tick_until_recv(&rt, &activation_inbox, Duration::from_millis(800));
    assert!(
        observed.is_none(),
        "no StageActivation should be emitted after done=true; got {observed:?}"
    );
}

#[test]
fn stage_0_worker_crash_is_handled_without_poisoning_runtime() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let sender = rt.create_sender();
    let actor = Stage0Actor::new(worker_spec(0, 2), sender, *activation_inbox.addr())
        .with_status_addr(*status_inbox.addr());
    let _addr = rt.spawn(actor).unwrap();
    let pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    kill_pid(pid);

    let status = tick_until_recv(&rt, &status_inbox, Duration::from_secs(5))
        .expect("actor should report ProcessExited after kill");
    match status {
        StageActorStatus::ProcessExited { .. } => {}
        other => panic!("expected ProcessExited, got {other:?}"),
    }

    assert_runtime_alive(&rt);
}

#[test]
fn stopping_stage_0_actor_kills_child_worker() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let sender = rt.create_sender();
    let actor = Stage0Actor::new(worker_spec(0, 2), sender, *activation_inbox.addr())
        .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));
    assert!(is_process_alive(pid), "worker alive before stop");

    rt.stop_actor(addr).unwrap();

    let start = Instant::now();
    let timeout = Duration::from_secs(5);
    while start.elapsed() < timeout {
        rt.tick();
        std::thread::sleep(Duration::from_millis(10));
        if !is_process_alive(pid) {
            break;
        }
    }
    assert!(
        !is_process_alive(pid),
        "stage-0 worker (pid {pid}) should be dead after actor stop"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Stage 1
// ═══════════════════════════════════════════════════════════════════════

/// One activation with seq_len=1; hidden bytes are deterministic from `seed`
/// so the worker's sampling is too. Bytes are not all-zero (the stub hash
/// folds them in but its determinism does not require it).
fn make_activation(request_id: u64, position: u32, seed: u8) -> StageActivation {
    let mut hidden = vec![0u8; STUB_BYTES_PER_TOKEN];
    for (i, b) in hidden.iter_mut().enumerate() {
        *b = seed.wrapping_add(i as u8);
    }
    StageActivation {
        request_id,
        position,
        hidden,
        seq_len: 1,
        is_prefill: false,
    }
}

#[test]
fn stage_1_actor_spawns_worker_and_reports_ready() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let next_token_inbox = rt.new_inbox::<NextToken>().unwrap();
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();

    let sender = rt.create_sender();
    let actor = Stage1Actor::new(
        worker_spec(1, 2),
        sender,
        *next_token_inbox.addr(),
        *response_inbox.addr(),
        8,
    )
    .with_status_addr(*status_inbox.addr());
    let _addr = rt.spawn(actor).unwrap();

    let pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));
    assert!(is_process_alive(pid), "stage-1 worker should be running");
}

#[test]
fn stage_activation_produces_next_token_to_prev_address() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let next_token_inbox = rt.new_inbox::<NextToken>().unwrap();
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();

    let sender = rt.create_sender();
    let actor = Stage1Actor::new(
        worker_spec(1, 2),
        sender,
        *next_token_inbox.addr(),
        *response_inbox.addr(),
        100, // max_tokens — large so EOS-by-cap never fires here
    )
    .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    let activation = make_activation(0x2A, 5, 0x11);
    rt.send_to(addr, Stage1Msg::Activation(activation)).unwrap();

    let nt = tick_until_recv(&rt, &next_token_inbox, Duration::from_secs(5))
        .expect("should receive NextToken from stage-1 actor");

    assert_eq!(nt.request_id, 0x2A);
    assert_eq!(nt.position, 6, "next position must be activation.position + seq_len");
    assert!(!nt.done, "single activation under max_tokens=100 must not be done");
    assert!(nt.token_id < STUB_VOCAB_SIZE, "token_id must lie in stub vocab range");
}

#[test]
fn stage_1_accumulates_tokens_and_emits_inference_response_on_eos() {
    // Probe: discover what token the stub produces for a specific activation.
    let probe_token = {
        let rt = Runtime::new(RuntimeConfig::default());
        let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
        let next_token_inbox = rt.new_inbox::<NextToken>().unwrap();
        let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();
        let sender = rt.create_sender();
        let actor = Stage1Actor::new(
            worker_spec(1, 2),
            sender,
            *next_token_inbox.addr(),
            *response_inbox.addr(),
            100,
        )
        .with_status_addr(*status_inbox.addr());
        let addr = rt.spawn(actor).unwrap();
        let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

        rt.send_to(addr, Stage1Msg::Activation(make_activation(1, 0, 0x42)))
            .unwrap();
        let nt = tick_until_recv(&rt, &next_token_inbox, Duration::from_secs(5))
            .expect("probe should observe a NextToken");
        rt.stop_actor(addr).unwrap();
        nt.token_id
    };

    // Real: configure EOS at the observed token, replay the activation,
    // expect done + InferenceResponse.
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let next_token_inbox = rt.new_inbox::<NextToken>().unwrap();
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();
    let sender = rt.create_sender();
    let actor = Stage1Actor::new(
        worker_spec(1, 2),
        sender,
        *next_token_inbox.addr(),
        *response_inbox.addr(),
        100, // max_tokens — well above the EOS-triggering count
    )
    .with_eos_token_id(probe_token)
    .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    rt.send_to(addr, Stage1Msg::Activation(make_activation(1, 0, 0x42)))
        .unwrap();

    let nt = tick_until_recv(&rt, &next_token_inbox, Duration::from_secs(5))
        .expect("should receive NextToken on EOS step");
    assert_eq!(nt.token_id, probe_token);
    assert!(nt.done, "NextToken on EOS must have done=true");

    let response = tick_until_recv(&rt, &response_inbox, Duration::from_secs(5))
        .expect("InferenceResponse must arrive on EOS");
    assert!(
        !response.text.is_empty(),
        "stub detokenization must produce non-empty text"
    );
}

#[test]
fn stage_1_emits_inference_response_when_max_tokens_reached() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let next_token_inbox = rt.new_inbox::<NextToken>().unwrap();
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();

    let cap: u32 = 3;
    let sender = rt.create_sender();
    let actor = Stage1Actor::new(
        worker_spec(1, 2),
        sender,
        *next_token_inbox.addr(),
        *response_inbox.addr(),
        cap,
    )
    .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    // The pipeline protocol is strictly sequential — stage 0 only emits a
    // new activation after the previous NextToken arrives. Mirror that
    // here: send one, await the reply, send the next.
    let mut saw_done = false;
    for i in 0..cap {
        rt.send_to(
            addr,
            Stage1Msg::Activation(make_activation(99, i, 0x10 + i as u8)),
        )
        .unwrap();
        let nt = tick_until_recv(&rt, &next_token_inbox, Duration::from_secs(5))
            .expect("should receive NextToken per activation");
        let expected_done = i + 1 == cap;
        assert_eq!(
            nt.done, expected_done,
            "done should be set exactly on the max_tokens-th NextToken (#{})",
            i + 1
        );
        if nt.done {
            saw_done = true;
        }
    }
    assert!(saw_done);

    let response = tick_until_recv(&rt, &response_inbox, Duration::from_secs(5))
        .expect("InferenceResponse must arrive when max_tokens is hit");
    assert!(!response.text.is_empty());

    // After done, no further NextTokens for new activations.
    rt.send_to(
        addr,
        Stage1Msg::Activation(make_activation(99, cap, 0xAA)),
    )
    .unwrap();
    let trailing = tick_until_recv(&rt, &next_token_inbox, Duration::from_millis(500));
    assert!(
        trailing.is_none(),
        "stage-1 actor should drop further activations after termination, got {trailing:?}"
    );
}

#[test]
fn stage_1_worker_crash_is_handled_without_poisoning_runtime() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let next_token_inbox = rt.new_inbox::<NextToken>().unwrap();
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();

    let sender = rt.create_sender();
    let actor = Stage1Actor::new(
        worker_spec(1, 2),
        sender,
        *next_token_inbox.addr(),
        *response_inbox.addr(),
        8,
    )
    .with_status_addr(*status_inbox.addr());
    let _addr = rt.spawn(actor).unwrap();
    let pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    kill_pid(pid);

    let status = tick_until_recv(&rt, &status_inbox, Duration::from_secs(5))
        .expect("actor should report ProcessExited after kill");
    match status {
        StageActorStatus::ProcessExited { .. } => {}
        other => panic!("expected ProcessExited, got {other:?}"),
    }
    assert_runtime_alive(&rt);
}

#[test]
fn stopping_stage_1_actor_kills_child_worker() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let next_token_inbox = rt.new_inbox::<NextToken>().unwrap();
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();

    let sender = rt.create_sender();
    let actor = Stage1Actor::new(
        worker_spec(1, 2),
        sender,
        *next_token_inbox.addr(),
        *response_inbox.addr(),
        8,
    )
    .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));
    assert!(is_process_alive(pid));

    rt.stop_actor(addr).unwrap();

    let start = Instant::now();
    let timeout = Duration::from_secs(5);
    while start.elapsed() < timeout {
        rt.tick();
        std::thread::sleep(Duration::from_millis(10));
        if !is_process_alive(pid) {
            break;
        }
    }
    assert!(
        !is_process_alive(pid),
        "stage-1 worker (pid {pid}) should be dead after actor stop"
    );
}
