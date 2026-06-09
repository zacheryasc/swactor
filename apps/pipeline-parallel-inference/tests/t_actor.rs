//! T-actor: `StageActor` per-role + process-bridge component tests.
//!
//! All tests run against the stub-mode `pp_tinygrad_worker`. No GPU, no
//! real model. Test names match TEST_SPEC §6 verbatim.
//!
//! Stage 3 landed §6.1 (Common), §6.2 (First, minus defensive drops), and
//! §6.4 (Last, minus defensive drops). Stage 4 adds §6.3 (Middle), the
//! Middle property test, and the defensive-drop tests in §6.2 / §6.4.
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
    StageActor, StageActorStatus, StageMsg, StageRole, stub_tokenize_prompt,
};
use swactor_process::{ProcessMode, ProcessSpec};

const STUB_BYTES_PER_TOKEN: usize = 16 * 2; // STUB_HIDDEN_DIM * BYTES_PER_ELEM
const STUB_VOCAB_SIZE: u32 = 32;

// ─── Helpers ──────────────────────────────────────────────────────────────

/// Build a stub-mode worker spec for `(stage, num_stages)`. Caller picks
/// values matching the role they want to test: `(0, 2)` for First,
/// `(1, 3)` for Middle, `(N-1, N)` for Last.
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

/// Per-role concrete worker spec — keeps tests role-agnostic.
fn role_worker_spec(role: StageRole) -> ProcessSpec {
    match role {
        StageRole::First => worker_spec(0, 2),
        // The smallest configuration that yields a Middle stage.
        StageRole::Middle => worker_spec(1, 3),
        StageRole::Last => worker_spec(1, 2),
    }
}

/// Spawn a `StageActor` of the given role with sentinel routing
/// addresses and the supplied status inbox. The status inbox is required
/// — the `drain_until_ready` helper reads from it.
fn spawn_role_actor(
    rt: &Runtime,
    role: StageRole,
    status_addr: ActorAddress,
    max_tokens: u32,
) -> ActorAddress {
    let placeholder = ActorAddress([0; 32]);
    let sender = rt.create_sender();
    let actor = match role {
        StageRole::First => StageActor::first(role_worker_spec(role), sender, placeholder)
            .with_status_addr(status_addr),
        StageRole::Middle => StageActor::middle(role_worker_spec(role), sender, placeholder)
            .with_status_addr(status_addr),
        StageRole::Last => StageActor::last(
            role_worker_spec(role),
            sender,
            placeholder,
            placeholder,
            max_tokens,
        )
        .with_status_addr(status_addr),
    };
    rt.spawn(actor).unwrap()
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
// §6.1 Common — three lifecycle tests, one per role
// ═══════════════════════════════════════════════════════════════════════
//
// Stable Rust has no built-in `paste`-style identifier concatenation, so
// each per-role test is spelled out at the bottom of this section. The
// shared body lives in a `body_*` helper directly above each cluster of
// three `#[test]` entry points.

fn body_spawns_worker_and_reports_ready(role: StageRole) {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let _addr = spawn_role_actor(&rt, role, *status_inbox.addr(), 8);
    let pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));
    assert!(
        is_process_alive(pid),
        "stage worker ({role:?}) should be running"
    );
}

#[test]
fn stage_actor_spawns_worker_and_reports_ready_first() {
    body_spawns_worker_and_reports_ready(StageRole::First);
}

#[test]
fn stage_actor_spawns_worker_and_reports_ready_middle() {
    body_spawns_worker_and_reports_ready(StageRole::Middle);
}

#[test]
fn stage_actor_spawns_worker_and_reports_ready_last() {
    body_spawns_worker_and_reports_ready(StageRole::Last);
}

fn body_worker_crash_emits_process_exited(role: StageRole) {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let _addr = spawn_role_actor(&rt, role, *status_inbox.addr(), 8);
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
fn stage_actor_worker_crash_emits_process_exited_first() {
    body_worker_crash_emits_process_exited(StageRole::First);
}

#[test]
fn stage_actor_worker_crash_emits_process_exited_middle() {
    body_worker_crash_emits_process_exited(StageRole::Middle);
}

#[test]
fn stage_actor_worker_crash_emits_process_exited_last() {
    body_worker_crash_emits_process_exited(StageRole::Last);
}

fn body_stopping_stage_actor_kills_child_worker(role: StageRole) {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let addr = spawn_role_actor(&rt, role, *status_inbox.addr(), 8);
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
        "worker ({role:?}, pid {pid}) should be dead after actor stop"
    );
}

#[test]
fn stopping_stage_actor_kills_child_worker_first() {
    body_stopping_stage_actor_kills_child_worker(StageRole::First);
}

#[test]
fn stopping_stage_actor_kills_child_worker_middle() {
    body_stopping_stage_actor_kills_child_worker(StageRole::Middle);
}

#[test]
fn stopping_stage_actor_kills_child_worker_last() {
    body_stopping_stage_actor_kills_child_worker(StageRole::Last);
}

/// `SetNeighbors` must redirect outbound traffic. Verified for First by
/// observing that an `InferenceRequest` lands at the original `next`,
/// then at a different `next` after a `SetNeighbors` send. We do not
/// re-verify on Last separately — the actor's SetNeighbors handler is
/// role-agnostic (every Some-valued field overwrites unconditionally),
/// and exercising the same field-update code on multiple roles adds no
/// signal.
#[test]
fn set_neighbors_updates_routing_addresses() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let first_inbox = rt.new_inbox::<StageActivation>().unwrap();
    let second_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let sender = rt.create_sender();
    let actor = StageActor::first(worker_spec(0, 2), sender, *first_inbox.addr())
        .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    // First request lands at the original next address.
    rt.send_to(
        addr,
        StageMsg::Inference(InferenceRequest {
            reply_to: dummy_addr(&rt),
            prompt: "first prompt".into(),
            max_tokens: 4,
        }),
    )
    .unwrap();
    let act_a = tick_until_recv(&rt, &first_inbox, Duration::from_secs(5))
        .expect("first request must produce an activation at the original next");
    assert!(act_a.is_prefill);

    // Repoint next via SetNeighbors. prev_stage and reply_to are unused
    // by First; passing None for them exercises the optional-field
    // contract.
    rt.send_to(
        addr,
        StageMsg::SetNeighbors {
            prev_stage: None,
            next_stage: Some(*second_inbox.addr()),
            reply_to: None,
        },
    )
    .unwrap();

    // Second request must land at the new next, not the old one.
    rt.send_to(
        addr,
        StageMsg::Inference(InferenceRequest {
            reply_to: dummy_addr(&rt),
            prompt: "second prompt".into(),
            max_tokens: 4,
        }),
    )
    .unwrap();
    let act_b = tick_until_recv(&rt, &second_inbox, Duration::from_secs(5))
        .expect("after SetNeighbors, request must produce activation at the new next");
    assert!(act_b.is_prefill);
    let leftover = tick_until_recv(&rt, &first_inbox, Duration::from_millis(400));
    assert!(
        leftover.is_none(),
        "after SetNeighbors, no activation should leak back to the previous next address; got {leftover:?}"
    );
}

/// `Reset` clears per-request state so the actor accepts a fresh request
/// without being respawned. Verified for First: send a request, observe
/// the activation, send `Reset`, then send a *new* request and observe
/// its activation. The two activations carry distinct request ids — the
/// second is produced after Reset cleared the first's pending map.
#[test]
fn reset_clears_per_request_state() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let sender = rt.create_sender();
    let actor = StageActor::first(worker_spec(0, 2), sender, *activation_inbox.addr())
        .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    rt.send_to(
        addr,
        StageMsg::Inference(InferenceRequest {
            reply_to: dummy_addr(&rt),
            prompt: "alpha".into(),
            max_tokens: 4,
        }),
    )
    .unwrap();
    let first = tick_until_recv(&rt, &activation_inbox, Duration::from_secs(5))
        .expect("activation for first request");

    rt.send_to(addr, StageMsg::Reset).unwrap();

    rt.send_to(
        addr,
        StageMsg::Inference(InferenceRequest {
            reply_to: dummy_addr(&rt),
            prompt: "beta".into(),
            max_tokens: 4,
        }),
    )
    .unwrap();
    let second = tick_until_recv(&rt, &activation_inbox, Duration::from_secs(5))
        .expect("activation for second request after Reset");

    assert!(first.is_prefill && second.is_prefill);
    assert_ne!(
        first.request_id, second.request_id,
        "request ids should differ across submissions"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// §6.2 First (Stage 3 — non-defensive-drop subset)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn first_role_inference_request_produces_stage_activation_to_next() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let sender = rt.create_sender();
    let actor = StageActor::first(worker_spec(0, 2), sender, *activation_inbox.addr())
        .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    let prompt = "say hello there";
    let expected_token_count = stub_tokenize_prompt(prompt).len() as u32;
    assert!(expected_token_count > 0);

    rt.send_to(
        addr,
        StageMsg::Inference(InferenceRequest {
            reply_to: dummy_addr(&rt),
            prompt: prompt.into(),
            max_tokens: 8,
        }),
    )
    .unwrap();

    let activation = tick_until_recv(&rt, &activation_inbox, Duration::from_secs(5))
        .expect("first-role actor must emit StageActivation");

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
fn first_role_next_token_produces_stage_activation_for_decode_step() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let sender = rt.create_sender();
    let actor = StageActor::first(worker_spec(0, 2), sender, *activation_inbox.addr())
        .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    rt.send_to(
        addr,
        StageMsg::NextToken(NextToken {
            request_id: 0xCAFE,
            token_id: 7,
            position: 11,
            done: false,
        }),
    )
    .unwrap();

    let activation = tick_until_recv(&rt, &activation_inbox, Duration::from_secs(5))
        .expect("decode-step activation");

    assert!(!activation.is_prefill);
    assert_eq!(activation.seq_len, 1);
    assert_eq!(activation.position, 11, "decode position must match NextToken");
    assert_eq!(activation.request_id, 0xCAFE);
    assert_eq!(activation.hidden.len(), STUB_BYTES_PER_TOKEN);
}

#[test]
fn first_role_next_token_with_done_produces_no_further_activations() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let sender = rt.create_sender();
    let actor = StageActor::first(worker_spec(0, 2), sender, *activation_inbox.addr())
        .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    rt.send_to(
        addr,
        StageMsg::NextToken(NextToken {
            request_id: 1,
            token_id: 7,
            position: 5,
            done: true,
        }),
    )
    .unwrap();

    let observed = tick_until_recv(&rt, &activation_inbox, Duration::from_millis(800));
    assert!(
        observed.is_none(),
        "no StageActivation after done=true; got {observed:?}"
    );
}

/// An `InferenceRequest` arriving before the worker reports ready is
/// dropped silently; subsequent requests post-ready still succeed.
#[test]
fn first_role_request_before_ready_is_dropped_without_panic() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let sender = rt.create_sender();
    let actor = StageActor::first(worker_spec(0, 2), sender, *activation_inbox.addr())
        .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();

    // Submit before WorkerReady. The runtime is freshly created — the
    // worker subprocess has not started, much less printed its ready
    // line, so `ready` is false and the actor must drop the request.
    rt.send_to(
        addr,
        StageMsg::Inference(InferenceRequest {
            reply_to: dummy_addr(&rt),
            prompt: "submitted too early".into(),
            max_tokens: 4,
        }),
    )
    .unwrap();

    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));
    // After becoming ready, no activation from the early request should
    // arrive — pump for long enough that any spuriously-buffered work
    // would have completed.
    let stale = tick_until_recv(&rt, &activation_inbox, Duration::from_millis(500));
    assert!(
        stale.is_none(),
        "request submitted before ready must not produce an activation; got {stale:?}"
    );

    // A post-ready request still works.
    rt.send_to(
        addr,
        StageMsg::Inference(InferenceRequest {
            reply_to: dummy_addr(&rt),
            prompt: "submitted after ready".into(),
            max_tokens: 4,
        }),
    )
    .unwrap();
    let activation = tick_until_recv(&rt, &activation_inbox, Duration::from_secs(5))
        .expect("post-ready request must produce an activation");
    assert!(activation.is_prefill);
}

// ═══════════════════════════════════════════════════════════════════════
// §6.4 Last (Stage 3 — non-defensive-drop subset)
// ═══════════════════════════════════════════════════════════════════════

/// One activation with `seq_len=1`; hidden bytes are deterministic from
/// `seed` so the stub worker's sampling is deterministic too. Bytes are
/// not all-zero (the stub hash folds them in but its determinism does
/// not require it).
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
fn last_role_activation_produces_next_token_to_first() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let next_token_inbox = rt.new_inbox::<NextToken>().unwrap();
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();

    let sender = rt.create_sender();
    let actor = StageActor::last(
        worker_spec(1, 2),
        sender,
        *next_token_inbox.addr(),
        *response_inbox.addr(),
        100,
    )
    .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    let activation = make_activation(0x2A, 5, 0x11);
    rt.send_to(addr, StageMsg::Activation(activation)).unwrap();

    let nt = tick_until_recv(&rt, &next_token_inbox, Duration::from_secs(5))
        .expect("last-role actor must emit NextToken");

    assert_eq!(nt.request_id, 0x2A);
    assert_eq!(nt.position, 6, "next position must be activation.position + seq_len");
    assert!(!nt.done, "single activation under max_tokens=100 must not be done");
    assert!(nt.token_id < STUB_VOCAB_SIZE, "token_id must lie in stub vocab range");
}

#[test]
fn last_role_accumulates_tokens_and_emits_response_on_eos() {
    // Probe: discover what token the stub produces for a specific activation.
    let probe_token = {
        let rt = Runtime::new(RuntimeConfig::default());
        let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
        let next_token_inbox = rt.new_inbox::<NextToken>().unwrap();
        let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();
        let sender = rt.create_sender();
        let actor = StageActor::last(
            worker_spec(1, 2),
            sender,
            *next_token_inbox.addr(),
            *response_inbox.addr(),
            100,
        )
        .with_status_addr(*status_inbox.addr());
        let addr = rt.spawn(actor).unwrap();
        let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

        rt.send_to(addr, StageMsg::Activation(make_activation(1, 0, 0x42)))
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
    let actor = StageActor::last(
        worker_spec(1, 2),
        sender,
        *next_token_inbox.addr(),
        *response_inbox.addr(),
        100,
    )
    .with_eos_token_id(probe_token)
    .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    rt.send_to(addr, StageMsg::Activation(make_activation(1, 0, 0x42)))
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
fn last_role_emits_response_when_max_tokens_reached() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let next_token_inbox = rt.new_inbox::<NextToken>().unwrap();
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();

    let cap: u32 = 3;
    let sender = rt.create_sender();
    let actor = StageActor::last(
        worker_spec(1, 2),
        sender,
        *next_token_inbox.addr(),
        *response_inbox.addr(),
        cap,
    )
    .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    // The pipeline protocol is strictly sequential — the first stage only
    // emits a new activation after the previous NextToken arrives. Mirror
    // that here: send one, await the reply, send the next.
    let mut saw_done = false;
    for i in 0..cap {
        rt.send_to(
            addr,
            StageMsg::Activation(make_activation(99, i, 0x10 + i as u8)),
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
}

/// After the actor finalises a request (`done=true` + `InferenceResponse`),
/// any further `StageActivation` for the same `request_id` must not
/// duplicate the response. (The finished flag stays set; the trailing
/// activation is dropped silently.)
#[test]
fn last_role_terminate_clears_pending_state() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let next_token_inbox = rt.new_inbox::<NextToken>().unwrap();
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();

    let cap: u32 = 3;
    let sender = rt.create_sender();
    let actor = StageActor::last(
        worker_spec(1, 2),
        sender,
        *next_token_inbox.addr(),
        *response_inbox.addr(),
        cap,
    )
    .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    // Drive to termination via max_tokens.
    for i in 0..cap {
        rt.send_to(
            addr,
            StageMsg::Activation(make_activation(7, i, 0x55 + i as u8)),
        )
        .unwrap();
        let _nt = tick_until_recv(&rt, &next_token_inbox, Duration::from_secs(5))
            .expect("NextToken per activation");
    }
    let first_response = tick_until_recv(&rt, &response_inbox, Duration::from_secs(5))
        .expect("first InferenceResponse after termination");
    assert!(!first_response.text.is_empty());

    // Trailing activation with the same request id must not produce
    // another NextToken or another InferenceResponse.
    rt.send_to(
        addr,
        StageMsg::Activation(make_activation(7, cap, 0xFF)),
    )
    .unwrap();
    let trailing_nt = tick_until_recv(&rt, &next_token_inbox, Duration::from_millis(500));
    assert!(
        trailing_nt.is_none(),
        "no NextToken after termination; got {trailing_nt:?}"
    );
    let trailing_resp = tick_until_recv(&rt, &response_inbox, Duration::from_millis(500));
    assert!(
        trailing_resp.is_none(),
        "no duplicate InferenceResponse after termination; got {trailing_resp:?}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// §6.2 First — defensive drop (Stage 4)
// ═══════════════════════════════════════════════════════════════════════

/// First never receives activations on the wire (they flow forward, not
/// back to the entry stage), but the swactor inbox is single-typed and the
/// network may technically deliver one. The actor must drop without
/// emitting an activation onward.
#[test]
fn first_role_drops_activation_messages_defensively() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();
    let next_token_inbox = rt.new_inbox::<NextToken>().unwrap();

    let sender = rt.create_sender();
    let actor = StageActor::first(worker_spec(0, 2), sender, *activation_inbox.addr())
        .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    // Construct a syntactically valid StageActivation. First should never
    // forward it nor crash the worker.
    rt.send_to(
        addr,
        StageMsg::Activation(make_activation(0xF1, 0, 0x33)),
    )
    .unwrap();

    let stray_act = tick_until_recv(&rt, &activation_inbox, Duration::from_millis(500));
    assert!(
        stray_act.is_none(),
        "First must not emit a StageActivation in response to an inbound activation; got {stray_act:?}"
    );
    let stray_nt = tick_until_recv(&rt, &next_token_inbox, Duration::from_millis(100));
    assert!(stray_nt.is_none(), "First emits no NextToken from a stray Activation");

    // Subsequent real request still works — drop did not poison anything.
    rt.send_to(
        addr,
        StageMsg::Inference(InferenceRequest {
            reply_to: dummy_addr(&rt),
            prompt: "still works".into(),
            max_tokens: 4,
        }),
    )
    .unwrap();
    let act = tick_until_recv(&rt, &activation_inbox, Duration::from_secs(5))
        .expect("post-drop InferenceRequest must still produce an activation");
    assert!(act.is_prefill);
}

// ═══════════════════════════════════════════════════════════════════════
// §6.3 Middle (Stage 4)
// ═══════════════════════════════════════════════════════════════════════

/// Activation builder for Middle tests. `seq_len` is arbitrary; hidden
/// length is exactly `seq_len * STUB_BYTES_PER_TOKEN` so the worker's
/// length-check passes.
fn make_middle_activation(
    request_id: u64,
    position: u32,
    seq_len: u32,
    is_prefill: bool,
    seed: u8,
) -> StageActivation {
    let total = seq_len as usize * STUB_BYTES_PER_TOKEN;
    let mut hidden = vec![0u8; total];
    for (i, b) in hidden.iter_mut().enumerate() {
        *b = seed.wrapping_add(i as u8);
    }
    StageActivation {
        request_id,
        position,
        hidden,
        seq_len,
        is_prefill,
    }
}

/// Spawn a `Middle` actor wired to a caller-supplied `next_stage` inbox
/// and return its address. Lets multiple Middle tests share boot.
fn spawn_middle_actor(
    rt: &Runtime,
    next_stage: ActorAddress,
    status_addr: ActorAddress,
) -> ActorAddress {
    let sender = rt.create_sender();
    let actor =
        StageActor::middle(worker_spec(1, 3), sender, next_stage).with_status_addr(status_addr);
    rt.spawn(actor).unwrap()
}

#[test]
fn middle_role_activation_produces_activation_to_next() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let addr = spawn_middle_actor(&rt, *activation_inbox.addr(), *status_inbox.addr());
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    let inbound = make_middle_activation(0xAB, 5, 3, true, 0x21);
    rt.send_to(addr, StageMsg::Activation(inbound.clone())).unwrap();

    let outbound = tick_until_recv(&rt, &activation_inbox, Duration::from_secs(5))
        .expect("middle-role actor must emit an outbound StageActivation");

    assert_eq!(outbound.request_id, inbound.request_id);
    assert_eq!(outbound.position, inbound.position);
    assert_eq!(outbound.seq_len, inbound.seq_len);
    assert_eq!(outbound.is_prefill, inbound.is_prefill);
    assert_eq!(
        outbound.hidden.len(),
        inbound.seq_len as usize * STUB_BYTES_PER_TOKEN,
        "outbound hidden byte length must match seq_len * hidden_dim * 2"
    );
}

/// Middle is a pure pass-through for the four control fields; only
/// `hidden` is allowed to change. Hand-rolled property fuzz: iterate
/// across a handful of distinct `(request_id, position, seq_len,
/// is_prefill)` tuples chosen to span the cases the orchestrator drives
/// (prefill vs decode, varied positions, varied seq lengths).
#[test]
fn middle_role_preserves_control_fields_property() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let addr = spawn_middle_actor(&rt, *activation_inbox.addr(), *status_inbox.addr());
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    // Distinct request ids so pending-fwd entries don't collide. The
    // 4-tuple sweep covers prefill (seq_len > 1) and decode (seq_len = 1)
    // at varied positions, with both is_prefill polarities.
    let cases: &[(u64, u32, u32, bool, u8)] = &[
        (1, 0, 1, true, 0x01),
        (2, 0, 4, true, 0x02),
        (3, 4, 1, false, 0x03),
        (4, 11, 1, false, 0x04),
        (5, 0, 8, true, 0x05),
        (6, 32, 1, false, 0x06),
        (7, 1, 2, true, 0x07),
        (8, 99, 1, false, 0x08),
    ];

    for (rid, position, seq_len, is_prefill, seed) in cases.iter().copied() {
        let inbound = make_middle_activation(rid, position, seq_len, is_prefill, seed);
        rt.send_to(addr, StageMsg::Activation(inbound.clone())).unwrap();

        let outbound = tick_until_recv(&rt, &activation_inbox, Duration::from_secs(5))
            .expect("middle must emit an outbound activation per inbound activation");

        assert_eq!(outbound.request_id, inbound.request_id, "rid changed (in {inbound:?})");
        assert_eq!(outbound.position, inbound.position, "position changed (in {inbound:?})");
        assert_eq!(outbound.seq_len, inbound.seq_len, "seq_len changed (in {inbound:?})");
        assert_eq!(
            outbound.is_prefill, inbound.is_prefill,
            "is_prefill changed (in {inbound:?})"
        );
        assert_eq!(
            outbound.hidden.len(),
            inbound.seq_len as usize * STUB_BYTES_PER_TOKEN,
        );
    }
}

#[test]
fn middle_role_drops_inference_requests_defensively() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let addr = spawn_middle_actor(&rt, *activation_inbox.addr(), *status_inbox.addr());
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    rt.send_to(
        addr,
        StageMsg::Inference(InferenceRequest {
            reply_to: dummy_addr(&rt),
            prompt: "should be dropped".into(),
            max_tokens: 4,
        }),
    )
    .unwrap();

    let stray = tick_until_recv(&rt, &activation_inbox, Duration::from_millis(500));
    assert!(
        stray.is_none(),
        "Middle must not emit an activation in response to InferenceRequest; got {stray:?}"
    );

    // Verify the drop did not poison the actor: a real activation still
    // produces a real outbound.
    rt.send_to(
        addr,
        StageMsg::Activation(make_middle_activation(0xCC, 0, 2, true, 0xAA)),
    )
    .unwrap();
    let outbound = tick_until_recv(&rt, &activation_inbox, Duration::from_secs(5))
        .expect("post-drop activation must still produce an outbound");
    assert_eq!(outbound.request_id, 0xCC);
}

#[test]
fn middle_role_drops_next_tokens_defensively() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();

    let addr = spawn_middle_actor(&rt, *activation_inbox.addr(), *status_inbox.addr());
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    rt.send_to(
        addr,
        StageMsg::NextToken(NextToken {
            request_id: 0x9F,
            token_id: 3,
            position: 7,
            done: false,
        }),
    )
    .unwrap();

    let stray = tick_until_recv(&rt, &activation_inbox, Duration::from_millis(500));
    assert!(
        stray.is_none(),
        "Middle must not emit an activation in response to NextToken; got {stray:?}"
    );
}

/// Middle never reaches the sampler / detokenizer — it has no concept of
/// EOS or max_tokens. Even a stream of activations that would trip EOS on
/// Last produces zero `InferenceResponse`s here.
#[test]
fn middle_role_does_not_emit_inference_response() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let activation_inbox = rt.new_inbox::<StageActivation>().unwrap();
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();

    let sender = rt.create_sender();
    // Set reply_to too — proves Middle won't write to it even when given
    // an address. (Middle's constructor doesn't take reply_to, so we
    // route through SetNeighbors to be thorough.)
    let actor = StageActor::middle(worker_spec(1, 3), sender, *activation_inbox.addr())
        .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));
    rt.send_to(
        addr,
        StageMsg::SetNeighbors {
            prev_stage: None,
            next_stage: None,
            reply_to: Some(*response_inbox.addr()),
        },
    )
    .unwrap();

    // Drive several activations and an InferenceRequest. None should
    // produce an InferenceResponse.
    for i in 0..4u32 {
        rt.send_to(
            addr,
            StageMsg::Activation(make_middle_activation(0xD0 + i as u64, i, 1, false, 0x40 + i as u8)),
        )
        .unwrap();
    }
    rt.send_to(
        addr,
        StageMsg::Inference(InferenceRequest {
            reply_to: *response_inbox.addr(),
            prompt: "should be dropped".into(),
            max_tokens: 4,
        }),
    )
    .unwrap();

    // Pump the runtime long enough to be sure any spuriously-buffered
    // response would have arrived.
    let resp = tick_until_recv(&rt, &response_inbox, Duration::from_millis(800));
    assert!(
        resp.is_none(),
        "Middle must never emit InferenceResponse; got {resp:?}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// §6.4 Last — defensive drops (Stage 4)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn last_role_drops_inference_requests_defensively() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let next_token_inbox = rt.new_inbox::<NextToken>().unwrap();
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();

    let sender = rt.create_sender();
    let actor = StageActor::last(
        worker_spec(1, 2),
        sender,
        *next_token_inbox.addr(),
        *response_inbox.addr(),
        8,
    )
    .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    rt.send_to(
        addr,
        StageMsg::Inference(InferenceRequest {
            reply_to: *response_inbox.addr(),
            prompt: "should be dropped".into(),
            max_tokens: 4,
        }),
    )
    .unwrap();

    let stray_nt = tick_until_recv(&rt, &next_token_inbox, Duration::from_millis(500));
    assert!(
        stray_nt.is_none(),
        "Last must not emit NextToken in response to InferenceRequest; got {stray_nt:?}"
    );
    let stray_resp = tick_until_recv(&rt, &response_inbox, Duration::from_millis(100));
    assert!(
        stray_resp.is_none(),
        "Last must not emit InferenceResponse in response to InferenceRequest; got {stray_resp:?}"
    );

    // Drop did not poison the actor — a real activation still works.
    rt.send_to(addr, StageMsg::Activation(make_activation(1, 0, 0x77)))
        .unwrap();
    let nt = tick_until_recv(&rt, &next_token_inbox, Duration::from_secs(5))
        .expect("post-drop activation must still produce a NextToken");
    assert_eq!(nt.request_id, 1);
}

#[test]
fn last_role_drops_next_tokens_defensively() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();
    let next_token_inbox = rt.new_inbox::<NextToken>().unwrap();
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();

    let sender = rt.create_sender();
    let actor = StageActor::last(
        worker_spec(1, 2),
        sender,
        *next_token_inbox.addr(),
        *response_inbox.addr(),
        8,
    )
    .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();
    let _pid = drain_until_ready(&rt, &status_inbox, Duration::from_secs(5));

    rt.send_to(
        addr,
        StageMsg::NextToken(NextToken {
            request_id: 0xA1,
            token_id: 4,
            position: 6,
            done: false,
        }),
    )
    .unwrap();

    let stray_nt = tick_until_recv(&rt, &next_token_inbox, Duration::from_millis(500));
    assert!(
        stray_nt.is_none(),
        "Last must not echo NextTokens it receives back onto the wire; got {stray_nt:?}"
    );
    let stray_resp = tick_until_recv(&rt, &response_inbox, Duration::from_millis(100));
    assert!(
        stray_resp.is_none(),
        "Last must not emit InferenceResponse in response to NextToken; got {stray_resp:?}"
    );
}
