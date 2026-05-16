//! T-integration: in-process end-to-end pipeline-parallel inference.
//!
//! TEST_SPEC §8. Three swactor `DistributedNode`s in one process — orchestrator,
//! stage 0, stage 1 — connected over real iroh QUIC. Each stage runs the
//! stub-mode `pp_tinygrad_worker.py` (no GPU, no GGUF). The pipeline runs the
//! autoregressive loop end-to-end: orchestrator submits an `InferenceRequest`,
//! stage 0 produces a `StageActivation` for prefill, stage 1 samples a token
//! and sends `NextToken` back, stage 0 produces another activation, and so on
//! until EOS or `max_tokens`. Stage 1 emits the final `InferenceResponse` back
//! to the orchestrator.
//!
//! The same shape the binary uses (see `pp_smoke_run.rs`), minus the child
//! subprocesses — actors live in this test process and addresses are wired up
//! directly without SWIM resolution.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::{PublicKey, RelayMode};

use swactor::actor::ActorAddress;
use swactor::runtime::{Inbox, Runtime, RuntimeConfig};
use swactor::transport::{CodecRegistry, TransportRouter};

use pipeline_parallel_inference::iroh_transport::{
    ActorMessagePump, IrohActorTransport, ACTOR_ALPN,
};
use pipeline_parallel_inference::messages::{
    inference_codec_registry, InferenceRequest, InferenceResponse, NextToken,
};
use pipeline_parallel_inference::stage_actor::{
    Stage0Actor, Stage0Msg, Stage0NextTokenBridge, Stage0RequestBridge,
    Stage1ActivationBridge, Stage1Actor, Stage1Msg, StageActorStatus,
};
use swactor_process::{ProcessMode, ProcessSpec};

// ─── Driver / cluster helpers (mirror t_cluster.rs) ───────────────────────

fn test_node_config() -> DistributedNodeConfig {
    DistributedNodeConfig {
        swim: SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 1,
            suspicion_timeout: 5,
            dead_reprobe_interval: 0,
            ..SwimConfig::default()
        },
        cache_capacity: 100,
        republish_interval: 50,
        registry: RegistryConfig::default(),
        metadata_lambda: 3,
    }
}

fn make_driver() -> IrohDriver {
    IrohDriver::new(IrohDriverConfig {
        secret_key: None,
        relay_mode: RelayMode::Disabled,
        node: test_node_config(),
        peer_auth: None,
        additional_alpns: vec![ACTOR_ALPN.to_vec()],
    })
    .expect("failed to create iroh driver")
}

fn pump_one(driver: &mut IrohDriver) {
    driver.recv();
    driver.tick();
}

fn pubkey_of(driver: &IrohDriver) -> PublicKey {
    PublicKey::from_bytes(&driver.node_id().0).unwrap()
}

fn sees_alive(driver: &IrohDriver, peer_key: &PublicKey) -> bool {
    let snap = driver.snapshot();
    let peer_hex: String = peer_key
        .as_bytes()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect();
    snap.members
        .iter()
        .any(|m| m.node_id == peer_hex && m.state == "alive")
}

fn make_three_node_cluster() -> [IrohDriver; 3] {
    let mut orch = make_driver();
    let mut s0 = make_driver();
    let mut s1 = make_driver();

    let seed = orch.endpoint_addr();
    s0.join(&[seed.clone()]);
    s1.join(&[seed]);

    let orch_key = pubkey_of(&orch);
    let s0_key = pubkey_of(&s0);
    let s1_key = pubkey_of(&s1);

    let start = Instant::now();
    let mut converged = false;
    while start.elapsed() < Duration::from_secs(10) {
        pump_one(&mut orch);
        pump_one(&mut s0);
        pump_one(&mut s1);
        if sees_alive(&orch, &s0_key)
            && sees_alive(&orch, &s1_key)
            && sees_alive(&s0, &orch_key)
            && sees_alive(&s0, &s1_key)
            && sees_alive(&s1, &orch_key)
            && sees_alive(&s1, &s0_key)
        {
            converged = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(converged, "3-node cluster did not converge within 10s");
    [orch, s0, s1]
}

// ─── Worker spec (stub mode) ──────────────────────────────────────────────

fn worker_spec(stage: u32, num_stages: u32) -> ProcessSpec {
    let mut env = HashMap::new();
    env.insert("STAGE".into(), stage.to_string());
    env.insert("NUM_STAGES".into(), num_stages.to_string());
    env.insert("PP_WORKER_STUB".into(), "1".into());
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

fn drain_until_ready(
    rt: &Runtime,
    status_inbox: &Inbox<StageActorStatus>,
    timeout: Duration,
) -> u32 {
    let start = Instant::now();
    while start.elapsed() < timeout {
        rt.tick();
        if let Some(status) = status_inbox.try_recv() {
            if let StageActorStatus::WorkerReady { pid } = status {
                return pid.expect("worker reports a pid");
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("worker did not become ready within {:?}", timeout);
}

fn is_process_alive(pid: u32) -> bool {
    std::fs::metadata(format!("/proc/{pid}")).is_ok()
}

fn kill_pid(pid: u32) {
    let _ = Command::new("kill").arg("-9").arg(pid.to_string()).status();
}

// ─── Pipeline harness ─────────────────────────────────────────────────────

/// Fully wired in-process pipeline: 3 iroh drivers + 3 runtimes + actors.
struct Pipeline {
    orch: IrohDriver,
    s0: IrohDriver,
    s1: IrohDriver,

    rt_orch: Runtime,
    rt_s0: Runtime,
    rt_s1: Runtime,

    codecs: Arc<CodecRegistry>,
    response_inbox: Inbox<InferenceResponse>,
    /// Real-mode only: receives a copy of every `NextToken` Stage 1 emits.
    /// `None` for stub-mode pipelines built via `build_pipeline`.
    token_observer_inbox: Option<Inbox<NextToken>>,
    inbox_addr: ActorAddress,
    request_bridge_addr: ActorAddress,

    s0_actor_addr: ActorAddress,
    s1_actor_addr: ActorAddress,

    s0_pid: u32,
    s1_pid: u32,

    orch_pump: ActorMessagePump,
    s0_pump: ActorMessagePump,
    s1_pump: ActorMessagePump,
}

/// Build the pipeline with a fixed `max_tokens` and an optional EOS token id.
/// All addresses are known up front (single-process test), so neighbour
/// addresses are passed directly into actor constructors and only stage 0's
/// next-stage address uses `SetNextStage` (since the activation bridge is
/// spawned after the actor it routes into).
fn build_pipeline(max_tokens: u32, eos: Option<u32>) -> Pipeline {
    let [orch, s0, s1] = make_three_node_cluster();

    let codecs = Arc::new(inference_codec_registry());

    let mut rt_orch = Runtime::new(RuntimeConfig::default());
    let mut rt_s0 = Runtime::new(RuntimeConfig::default());
    let mut rt_s1 = Runtime::new(RuntimeConfig::default());

    let response_inbox = rt_orch.new_inbox::<InferenceResponse>().unwrap();
    let inbox_addr = *response_inbox.addr();

    let s0_status = rt_s0.new_inbox::<StageActorStatus>().unwrap();
    let s1_status = rt_s1.new_inbox::<StageActorStatus>().unwrap();

    // Stage 0 actor with placeholder next_stage; we set it via SetNextStage
    // once the stage-1 activation bridge address exists.
    let placeholder = ActorAddress([0; 32]);
    let sender_s0 = rt_s0.create_sender();
    let stage0_actor = Stage0Actor::new(worker_spec(0, 2), sender_s0, placeholder)
        .with_status_addr(*s0_status.addr());
    let s0_actor_addr = rt_s0.spawn(stage0_actor).unwrap();

    let request_bridge = Stage0RequestBridge { target: s0_actor_addr };
    let request_bridge_addr = rt_s0.spawn(request_bridge).unwrap();
    let nt_bridge = Stage0NextTokenBridge { target: s0_actor_addr };
    let nt_bridge_addr = rt_s0.spawn(nt_bridge).unwrap();

    // Stage 1 actor receives real prev/reply_to up front.
    let sender_s1 = rt_s1.create_sender();
    let mut stage1_actor = Stage1Actor::new(
        worker_spec(1, 2),
        sender_s1,
        nt_bridge_addr,
        inbox_addr,
        max_tokens,
    )
    .with_status_addr(*s1_status.addr());
    if let Some(eos_id) = eos {
        stage1_actor = stage1_actor.with_eos_token_id(eos_id);
    }
    let s1_actor_addr = rt_s1.spawn(stage1_actor).unwrap();

    let activation_bridge = Stage1ActivationBridge { target: s1_actor_addr };
    let activation_bridge_addr = rt_s1.spawn(activation_bridge).unwrap();

    rt_s0
        .send_to(s0_actor_addr, Stage0Msg::SetNextStage(activation_bridge_addr))
        .unwrap();

    // Wait for both worker subprocesses to report ready.
    let s0_pid = drain_until_ready(&rt_s0, &s0_status, Duration::from_secs(10));
    let s1_pid = drain_until_ready(&rt_s1, &s1_status, Duration::from_secs(10));

    // Install codec registries and transport routers.
    // Orchestrator → s0 (InferenceRequest into stage 0's request bridge).
    let orch_to_s0 = Arc::new(IrohActorTransport::new(
        orch.endpoint().clone(),
        s0.endpoint_addr(),
        orch.tokio_handle(),
    ));
    let router_orch = TransportRouter::new();
    router_orch.add_route(request_bridge_addr, orch_to_s0);
    rt_orch.set_codec_registry(codecs.clone());
    rt_orch.set_transport_router(Arc::new(router_orch));

    // s0 → s1 (StageActivation into stage 1's activation bridge).
    let s0_to_s1 = Arc::new(IrohActorTransport::new(
        s0.endpoint().clone(),
        s1.endpoint_addr(),
        s0.tokio_handle(),
    ));
    let router_s0 = TransportRouter::new();
    router_s0.add_route(activation_bridge_addr, s0_to_s1);
    rt_s0.set_codec_registry(codecs.clone());
    rt_s0.set_transport_router(Arc::new(router_s0));

    // s1 → s0 (NextToken) and s1 → orch (InferenceResponse).
    let s1_to_s0 = Arc::new(IrohActorTransport::new(
        s1.endpoint().clone(),
        s0.endpoint_addr(),
        s1.tokio_handle(),
    ));
    let s1_to_orch = Arc::new(IrohActorTransport::new(
        s1.endpoint().clone(),
        orch.endpoint_addr(),
        s1.tokio_handle(),
    ));
    let router_s1 = TransportRouter::new();
    router_s1.add_route(nt_bridge_addr, s1_to_s0);
    router_s1.add_route(inbox_addr, s1_to_orch);
    rt_s1.set_codec_registry(codecs.clone());
    rt_s1.set_transport_router(Arc::new(router_s1));

    Pipeline {
        orch,
        s0,
        s1,
        rt_orch,
        rt_s0,
        rt_s1,
        codecs,
        response_inbox,
        token_observer_inbox: None,
        inbox_addr,
        request_bridge_addr,
        s0_actor_addr,
        s1_actor_addr,
        s0_pid,
        s1_pid,
        orch_pump: ActorMessagePump::new(),
        s0_pump: ActorMessagePump::new(),
        s1_pump: ActorMessagePump::new(),
    }
}

impl Pipeline {
    /// One pass through every node: drain QUIC, pump actor messages into each
    /// runtime, tick every runtime.
    fn pump(&mut self) {
        self.orch.recv();
        self.orch.tick();
        self.s0.recv();
        self.s0.tick();
        self.s1.recv();
        self.s1.tick();

        self.orch_pump.pump(&self.orch, &self.codecs, &self.rt_orch);
        self.s0_pump.pump(&self.s0, &self.codecs, &self.rt_s0);
        self.s1_pump.pump(&self.s1, &self.codecs, &self.rt_s1);

        self.rt_orch.tick();
        self.rt_s0.tick();
        self.rt_s1.tick();
    }

    /// Pump only the orchestrator side. Used after one of the stages has been
    /// shut down (calling `recv`/`tick` on a dead driver is fine, but pumping
    /// its runtime contributes nothing and clutters the loop).
    fn pump_orch_only(&mut self) {
        self.orch.recv();
        self.orch.tick();
        self.orch_pump.pump(&self.orch, &self.codecs, &self.rt_orch);
        self.rt_orch.tick();
    }

    fn submit(&self, prompt: &str, max_tokens: u32) {
        let req = InferenceRequest {
            reply_to: self.inbox_addr,
            prompt: prompt.into(),
            max_tokens,
        };
        self.rt_orch.send_to(self.request_bridge_addr, req).unwrap();
    }

    fn await_response(&mut self, timeout: Duration) -> Result<InferenceResponse, String> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            self.pump();
            if let Some(r) = self.response_inbox.try_recv() {
                return Ok(r);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(format!(
            "no InferenceResponse within {:.0}s",
            timeout.as_secs_f32()
        ))
    }

    /// Await a response OR detect that a stage's node has gone non-alive in the
    /// orchestrator's SWIM view. The orchestrator's snapshot omits self, so
    /// `alive_count < 2` means at least one stage is no longer reachable.
    fn await_response_or_stage_failure(
        &mut self,
        timeout: Duration,
    ) -> Result<InferenceResponse, String> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            self.pump_orch_only();
            if let Some(r) = self.response_inbox.try_recv() {
                return Ok(r);
            }
            let snap = self.orch.snapshot();
            if snap.alive_count < 2 {
                let dead: Vec<_> = snap
                    .members
                    .iter()
                    .filter(|m| m.state != "alive")
                    .map(|m| format!("{}={}", &m.node_id[..8.min(m.node_id.len())], m.state))
                    .collect();
                return Err(format!(
                    "stage failure detected via SWIM after {:.1}s ({:?})",
                    start.elapsed().as_secs_f32(),
                    dead
                ));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Err(format!(
            "no response and no SWIM failure within {:.0}s",
            timeout.as_secs_f32()
        ))
    }

    fn shutdown(mut self) {
        let _ = self.rt_s0.stop_actor(self.s0_actor_addr);
        let _ = self.rt_s1.stop_actor(self.s1_actor_addr);
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            self.rt_s0.tick();
            self.rt_s1.tick();
            if !is_process_alive(self.s0_pid) && !is_process_alive(self.s1_pid) {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        self.orch.shutdown();
        self.s0.shutdown();
        self.s1.shutdown();
    }
}

/// Parse the stub `detokenize_stub` output `"tokens: [a b c]"` into a vector
/// of token ids. The format is owned by `Stage1Actor::detokenize_stub`.
fn parse_stub_tokens(text: &str) -> Vec<u32> {
    let inside = text
        .trim_start_matches("tokens: [")
        .trim_end_matches(']')
        .trim();
    if inside.is_empty() {
        return vec![];
    }
    inside
        .split_whitespace()
        .map(|s| s.parse::<u32>().expect("stub token id parses as u32"))
        .collect()
}

// ═══════════════════════════════════════════════════════════════════════
// §8 tests
// ═══════════════════════════════════════════════════════════════════════

/// Full pipeline: orchestrator submits one `InferenceRequest`, stage 0 →
/// stage 1 → stage 0 round-trips drive the autoregressive loop with the stub
/// worker, stage 1 emits `InferenceResponse` back to the orchestrator within
/// the test timeout, and the response text is non-empty.
#[test]
fn distributed_pipeline_through_stub_workers_returns_response() {
    let mut pipeline = build_pipeline(4, None);
    pipeline.submit("Say hello", 4);

    let response = pipeline
        .await_response(Duration::from_secs(30))
        .expect("pipeline must produce an InferenceResponse");

    assert!(
        !response.text.is_empty(),
        "response text must be non-empty, got: {:?}",
        response.text
    );
    let tokens = parse_stub_tokens(&response.text);
    assert!(
        !tokens.is_empty(),
        "stub response must contain at least one token id: {:?}",
        response.text
    );

    pipeline.shutdown();
}

/// Configure the stub worker's stage-1 actor with an EOS token id and verify
/// the decode loop terminates with exactly that many accumulated tokens. Done
/// by running the pipeline twice with the same prompt: the first run discovers
/// the deterministic stub token sequence, the second pins EOS to one of those
/// tokens and asserts the response truncates at the matching index.
#[test]
fn decode_loop_terminates_on_stub_eos() {
    const PROMPT: &str = "Say hello once";
    const PROBE_TOKENS: u32 = 5;
    const TARGET_N: usize = 3;

    // First run: no EOS, collect the stub's natural token sequence.
    let probe_tokens = {
        let mut probe = build_pipeline(PROBE_TOKENS, None);
        probe.submit(PROMPT, PROBE_TOKENS);
        let resp = probe
            .await_response(Duration::from_secs(30))
            .expect("probe pipeline must produce a response");
        let tokens = parse_stub_tokens(&resp.text);
        assert_eq!(
            tokens.len(),
            PROBE_TOKENS as usize,
            "probe (no EOS, max_tokens={PROBE_TOKENS}) should yield {PROBE_TOKENS} tokens"
        );
        probe.shutdown();
        tokens
    };

    let eos = probe_tokens[TARGET_N - 1];

    // Second run: same prompt, configured EOS at the target position. Pipeline
    // must terminate after exactly TARGET_N tokens are accumulated.
    let mut real = build_pipeline(PROBE_TOKENS, Some(eos));
    real.submit(PROMPT, PROBE_TOKENS);
    let resp = real
        .await_response(Duration::from_secs(30))
        .expect("EOS pipeline must produce a response");
    let tokens = parse_stub_tokens(&resp.text);
    assert_eq!(
        tokens.len(),
        TARGET_N,
        "EOS at index {} should truncate response to {TARGET_N} tokens, got {tokens:?}",
        TARGET_N - 1
    );
    assert_eq!(
        *tokens.last().unwrap(),
        eos,
        "last accumulated token must be the EOS id"
    );
    real.shutdown();
}

/// With no EOS configured, the decode loop runs until `max_tokens` and stage
/// 1 emits `InferenceResponse` containing exactly `max_tokens` tokens.
#[test]
fn decode_loop_terminates_on_max_tokens() {
    const MAX: u32 = 4;

    let mut pipeline = build_pipeline(MAX, None);
    pipeline.submit("max tokens stop", MAX);

    let response = pipeline
        .await_response(Duration::from_secs(30))
        .expect("max_tokens pipeline must produce a response");
    let tokens = parse_stub_tokens(&response.text);
    assert_eq!(
        tokens.len() as u32,
        MAX,
        "with no EOS, response must contain exactly max_tokens={MAX} tokens, got {tokens:?}"
    );
    pipeline.shutdown();
}

/// Mid-decode failure: kill stage 1's worker process and shut down stage 1's
/// iroh driver while the autoregressive loop is in flight. The orchestrator's
/// await loop must surface a stage failure (via SWIM) within the configured
/// detection window — not hang waiting forever.
#[test]
fn stage_failure_mid_decode_surfaces_as_error_to_orchestrator() {
    // Generous max_tokens so the loop will not finish before we kill stage 1.
    let mut pipeline = build_pipeline(64, None);
    pipeline.submit("never finishes naturally", 64);

    // Let a few rounds happen so we are genuinely mid-decode.
    let pump_start = Instant::now();
    while pump_start.elapsed() < Duration::from_millis(400) {
        pipeline.pump();
        if pipeline.response_inbox.try_recv().is_some() {
            panic!("pipeline returned a response too quickly to test mid-decode failure");
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    // Take down stage 1: kill its worker subprocess AND shut down its iroh
    // node. Killing the worker alone leaves the SWIM node alive (it just stops
    // making progress); shutting down the driver is what makes SWIM mark it
    // dead in the orchestrator's view. Both happen together to simulate a
    // complete stage death.
    kill_pid(pipeline.s1_pid);
    pipeline.s1.shutdown();

    // SwimConfig: probe_timeout=3, suspicion_timeout=5 → dead within ~8-10s.
    // Give 20s of slack for indirect probes and gossip propagation.
    let result = pipeline.await_response_or_stage_failure(Duration::from_secs(20));
    assert!(
        result.is_err(),
        "orchestrator should NOT receive an InferenceResponse after stage 1 dies; got {result:?}"
    );

    // Tear down what is left.
    let _ = pipeline.rt_s0.stop_actor(pipeline.s0_actor_addr);
    let teardown = Instant::now();
    while teardown.elapsed() < Duration::from_secs(2) {
        pipeline.rt_s0.tick();
        if !is_process_alive(pipeline.s0_pid) {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    pipeline.orch.shutdown();
    pipeline.s0.shutdown();
}

// ═══════════════════════════════════════════════════════════════════════
// §9 — Sliced-vs-full equivalence
// ═══════════════════════════════════════════════════════════════════════
//
// These tests prove that the 2-stage sliced pipeline produces the same
// token sequence as a single-process run over the full model on the same
// prompt, with argmax sampling. They:
//
// 1. Spawn a long-lived reference worker (single process, `NUM_STAGES=1`,
//    real GGUF). It serves a new `generate_full` op that runs the same
//    unjitted block iteration as the pipeline but over `model.blk[0..N]`
//    in one shot.
// 2. Build a real-mode 3-node pipeline (workers without `--stub`, stage 0
//    tokenises via the worker's `tokenize` op, stage 1 mirrors each
//    sampled `NextToken` to a test-owned observer inbox).
// 3. For each prompt, submit through the pipeline, collect `max_tokens`
//    sampled token ids via the observer, ask the reference worker for
//    the same prompt, and assert equality.
//
// Both are `#[ignore]`: the GGUF fetch + per-token CPU forward dominate
// wall-clock. Run with `cargo test -- --ignored`.
//
// Slow-test prerequisites mirror `tests/test_worker.py::TestRealTinygradWorker`:
//
//   - clang installed (tinygrad's CPU backend compiles kernels with it).
//   - `llama3.2:1b` already cached under `~/.cache/tinygrad/downloads/`.
//     The first run on a fresh machine downloads ~1 GB; subsequent runs
//     reuse it.

const REAL_READY_TIMEOUT: Duration = Duration::from_secs(180);
const REAL_PROMPT_TIMEOUT: Duration = Duration::from_secs(300);
const EQUIVALENCE_MAX_TOKENS: u32 = 8;

fn real_worker_spec(stage: u32, num_stages: u32) -> ProcessSpec {
    let mut env = HashMap::new();
    env.insert("STAGE".into(), stage.to_string());
    env.insert("NUM_STAGES".into(), num_stages.to_string());
    env.insert("MODEL".into(), "llama3.2:1b".into());
    // Explicitly clear any inherited stub gate; presence of the var (not
    // its value) toggles stub mode on some startup paths.
    env.insert("PP_WORKER_STUB".into(), "".into());
    ProcessSpec {
        command: "python3".into(),
        args: vec![format!("{}/pp_tinygrad_worker.py", env!("CARGO_MANIFEST_DIR"))],
        env,
        working_dir: None,
        mode: ProcessMode::Automated,
        initial_pty_size: None,
        kill_timeout: Some(Duration::from_secs(5)),
        stdin_buffer_limit: None,
    }
}

/// Build a real-mode pipeline: same shape as `build_pipeline` but with
/// real workers, real tokenization at stage 0, and a token observer on
/// stage 1 so tests can capture the sampled token sequence.
fn build_real_pipeline(max_tokens: u32) -> Pipeline {
    let [orch, s0, s1] = make_three_node_cluster();

    let codecs = Arc::new(inference_codec_registry());

    let mut rt_orch = Runtime::new(RuntimeConfig::default());
    let mut rt_s0 = Runtime::new(RuntimeConfig::default());
    let mut rt_s1 = Runtime::new(RuntimeConfig::default());

    let response_inbox = rt_orch.new_inbox::<InferenceResponse>().unwrap();
    let inbox_addr = *response_inbox.addr();

    let s0_status = rt_s0.new_inbox::<StageActorStatus>().unwrap();
    let s1_status = rt_s1.new_inbox::<StageActorStatus>().unwrap();

    let token_observer_inbox = rt_s1.new_inbox::<NextToken>().unwrap();
    let observer_addr = *token_observer_inbox.addr();

    let placeholder = ActorAddress([0; 32]);
    let sender_s0 = rt_s0.create_sender();
    let stage0_actor = Stage0Actor::new(real_worker_spec(0, 2), sender_s0, placeholder)
        .with_status_addr(*s0_status.addr())
        .with_real_tokenization();
    let s0_actor_addr = rt_s0.spawn(stage0_actor).unwrap();

    let request_bridge = Stage0RequestBridge { target: s0_actor_addr };
    let request_bridge_addr = rt_s0.spawn(request_bridge).unwrap();
    let nt_bridge = Stage0NextTokenBridge { target: s0_actor_addr };
    let nt_bridge_addr = rt_s0.spawn(nt_bridge).unwrap();

    let sender_s1 = rt_s1.create_sender();
    let stage1_actor = Stage1Actor::new(
        real_worker_spec(1, 2),
        sender_s1,
        nt_bridge_addr,
        inbox_addr,
        max_tokens,
    )
    .with_status_addr(*s1_status.addr())
    .with_token_observer(observer_addr);
    let s1_actor_addr = rt_s1.spawn(stage1_actor).unwrap();

    let activation_bridge = Stage1ActivationBridge { target: s1_actor_addr };
    let activation_bridge_addr = rt_s1.spawn(activation_bridge).unwrap();

    rt_s0
        .send_to(s0_actor_addr, Stage0Msg::SetNextStage(activation_bridge_addr))
        .unwrap();

    // Real workers can take well over a minute to load the GGUF on a cold
    // tinygrad cache. We need both ready signals before driving the
    // pipeline; pumping the runtimes in parallel keeps either side from
    // starving the other's process bridge.
    let s0_pid = drain_until_ready_pumped(
        &[&rt_s0, &rt_s1],
        &s0_status,
        REAL_READY_TIMEOUT,
        "stage-0 real worker",
    );
    let s1_pid = drain_until_ready_pumped(
        &[&rt_s0, &rt_s1],
        &s1_status,
        REAL_READY_TIMEOUT,
        "stage-1 real worker",
    );

    let orch_to_s0 = Arc::new(IrohActorTransport::new(
        orch.endpoint().clone(),
        s0.endpoint_addr(),
        orch.tokio_handle(),
    ));
    let router_orch = TransportRouter::new();
    router_orch.add_route(request_bridge_addr, orch_to_s0);
    rt_orch.set_codec_registry(codecs.clone());
    rt_orch.set_transport_router(Arc::new(router_orch));

    let s0_to_s1 = Arc::new(IrohActorTransport::new(
        s0.endpoint().clone(),
        s1.endpoint_addr(),
        s0.tokio_handle(),
    ));
    let router_s0 = TransportRouter::new();
    router_s0.add_route(activation_bridge_addr, s0_to_s1);
    rt_s0.set_codec_registry(codecs.clone());
    rt_s0.set_transport_router(Arc::new(router_s0));

    let s1_to_s0 = Arc::new(IrohActorTransport::new(
        s1.endpoint().clone(),
        s0.endpoint_addr(),
        s1.tokio_handle(),
    ));
    let s1_to_orch = Arc::new(IrohActorTransport::new(
        s1.endpoint().clone(),
        orch.endpoint_addr(),
        s1.tokio_handle(),
    ));
    let router_s1 = TransportRouter::new();
    router_s1.add_route(nt_bridge_addr, s1_to_s0);
    router_s1.add_route(inbox_addr, s1_to_orch);
    rt_s1.set_codec_registry(codecs.clone());
    rt_s1.set_transport_router(Arc::new(router_s1));

    Pipeline {
        orch,
        s0,
        s1,
        rt_orch,
        rt_s0,
        rt_s1,
        codecs,
        response_inbox,
        token_observer_inbox: Some(token_observer_inbox),
        inbox_addr,
        request_bridge_addr,
        s0_actor_addr,
        s1_actor_addr,
        s0_pid,
        s1_pid,
        orch_pump: ActorMessagePump::new(),
        s0_pump: ActorMessagePump::new(),
        s1_pump: ActorMessagePump::new(),
    }
}

/// Tick every runtime in `rts` until `inbox` produces a `WorkerReady`,
/// returning the reported pid. Used by `build_real_pipeline` where the
/// model load can take much longer than `drain_until_ready`'s 10s budget
/// — the SwACTOR process bridge consumes stdout on its host runtime, so
/// pumping is required for the ready line to surface.
fn drain_until_ready_pumped(
    rts: &[&Runtime],
    inbox: &Inbox<StageActorStatus>,
    timeout: Duration,
    label: &str,
) -> u32 {
    let start = Instant::now();
    while start.elapsed() < timeout {
        for rt in rts {
            rt.tick();
        }
        if let Some(status) = inbox.try_recv() {
            if let StageActorStatus::WorkerReady { pid } = status {
                return pid.unwrap_or_else(|| {
                    panic!("{label}: WorkerReady with no pid")
                });
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("{label} did not become ready within {:?}", timeout);
}

impl Pipeline {
    /// Drive the pipeline for one prompt and return the `max_tokens`
    /// sampled token ids in order. Asserts that the run terminates on
    /// `done=true` (max_tokens reached) within `timeout`.
    fn run_real_prompt(&mut self, prompt: &str, max_tokens: u32, timeout: Duration) -> Vec<u32> {
        assert!(
            self.token_observer_inbox.is_some(),
            "pipeline has no token observer; built via build_pipeline instead of build_real_pipeline?"
        );

        self.submit(prompt, max_tokens);

        let mut collected: Vec<u32> = Vec::with_capacity(max_tokens as usize);
        let start = Instant::now();
        while collected.len() < max_tokens as usize && start.elapsed() < timeout {
            self.pump();
            if let Some(observer) = self.token_observer_inbox.as_ref() {
                while let Some(nt) = observer.try_recv() {
                    collected.push(nt.token_id);
                    if nt.done {
                        break;
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        // Drain the final InferenceResponse (the stub-text path is fine; we
        // don't read the text). Without this, a residual response in the
        // inbox would leak across prompts when the harness is reused.
        let drain_until = Instant::now() + Duration::from_secs(10);
        while Instant::now() < drain_until {
            self.pump();
            if self.response_inbox.try_recv().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        assert_eq!(
            collected.len(),
            max_tokens as usize,
            "pipeline produced {} tokens for prompt {prompt:?}, expected {max_tokens}",
            collected.len()
        );
        collected
    }

    /// Reset the two stage actors so a fresh prompt can be submitted. The
    /// worker's per-block KV cache is left alone — the next prefill at
    /// `position=0` rewrites the cache positions it needs.
    fn reset_for_next_prompt(&mut self) {
        self.rt_s0.send_to(self.s0_actor_addr, Stage0Msg::Reset).unwrap();
        self.rt_s1.send_to(self.s1_actor_addr, Stage1Msg::Reset).unwrap();
        // Give the runtimes a moment to deliver the Reset before the next
        // submit so the actors don't race their own pending bookkeeping.
        for _ in 0..5 {
            self.pump();
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

// ─── Reference worker (single process, full-model generate) ─────────────

/// A standalone real-mode worker driven directly over stdin/stdout. We do
/// not need the actor/process-bridge infrastructure for the reference —
/// it answers one op (`generate_full`) and is reused across prompts.
struct ReferenceWorker {
    child: Child,
    stdout: BufReader<ChildStdout>,
    stdin: ChildStdin,
    next_rid: u64,
}

impl ReferenceWorker {
    fn spawn() -> Self {
        let mut cmd = Command::new("python3");
        cmd.arg(format!(
            "{}/pp_tinygrad_worker.py",
            env!("CARGO_MANIFEST_DIR")
        ));
        cmd.env("STAGE", "0");
        cmd.env("NUM_STAGES", "1");
        cmd.env("MODEL", "llama3.2:1b");
        cmd.env_remove("PP_WORKER_STUB");
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::inherit());
        let mut child = cmd.spawn().expect("spawn reference worker");
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let stdin = child.stdin.take().unwrap();
        let mut me = Self { child, stdout, stdin, next_rid: 1 };
        let ready = me.read_json();
        assert_eq!(
            ready.get("status").and_then(|v| v.as_str()),
            Some("ready"),
            "reference worker did not report ready: {ready}"
        );
        me
    }

    fn read_json(&mut self) -> serde_json::Value {
        let mut line = String::new();
        // BufRead::read_line blocks until newline or EOF. For a test, a
        // blocking read is acceptable — the worker either replies or the
        // test times out at the cargo level. Catching EOF (read_line
        // returning Ok(0)) makes the panic message more useful when the
        // child has died.
        let n = self
            .stdout
            .read_line(&mut line)
            .expect("reference worker: read stdout");
        if n == 0 {
            panic!("reference worker closed stdout before replying");
        }
        serde_json::from_str(line.trim()).unwrap_or_else(|e| {
            panic!("reference worker: invalid JSON {line:?}: {e}")
        })
    }

    /// Ask the reference worker for the `max_tokens` argmax-sampled
    /// continuations of `prompt`. Drives the worker's `generate_full` op,
    /// which runs the full model in one process — no per-stage slicing.
    fn generate_full(&mut self, prompt: &str, max_tokens: u32) -> Vec<u32> {
        let rid = self.next_rid;
        self.next_rid += 1;
        let req = serde_json::json!({
            "op": "generate_full",
            "request_id": rid,
            "prompt": prompt,
            "max_tokens": max_tokens,
        });
        let line = serde_json::to_string(&req).unwrap();
        self.stdin.write_all(line.as_bytes()).expect("write");
        self.stdin.write_all(b"\n").expect("write newline");
        self.stdin.flush().expect("flush");

        let reply = self.read_json();
        if let Some(err) = reply.get("error").and_then(|v| v.as_str()) {
            panic!("reference generate_full failed: {err}");
        }
        let tokens = reply
            .get("tokens")
            .and_then(|v| v.as_array())
            .unwrap_or_else(|| panic!("reference reply missing tokens: {reply}"));
        tokens
            .iter()
            .map(|t| {
                t.as_u64()
                    .unwrap_or_else(|| panic!("reference token not u64: {t}"))
                    as u32
            })
            .collect()
    }
}

impl Drop for ReferenceWorker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Drive every prompt through both the sliced pipeline and the
/// single-process reference, asserting per-prompt token-id equality up to
/// `EQUIVALENCE_MAX_TOKENS`. Factored so each `#[ignore]` test reads as
/// "what prompts" instead of "what plumbing".
fn assert_pipeline_matches_reference(prompts: &[&str]) {
    assert!(!prompts.is_empty(), "need at least one prompt");

    let mut reference = ReferenceWorker::spawn();
    let mut pipeline = build_real_pipeline(EQUIVALENCE_MAX_TOKENS);

    for (i, prompt) in prompts.iter().enumerate() {
        let ref_tokens = reference.generate_full(prompt, EQUIVALENCE_MAX_TOKENS);
        assert_eq!(
            ref_tokens.len() as u32,
            EQUIVALENCE_MAX_TOKENS,
            "reference returned wrong token count for prompt #{i} {prompt:?}: {ref_tokens:?}"
        );

        let pipeline_tokens =
            pipeline.run_real_prompt(prompt, EQUIVALENCE_MAX_TOKENS, REAL_PROMPT_TIMEOUT);

        assert_eq!(
            pipeline_tokens, ref_tokens,
            "sliced pipeline diverged from full-model reference on prompt #{i} {prompt:?}\n  pipeline:  {pipeline_tokens:?}\n  reference: {ref_tokens:?}"
        );

        if i + 1 < prompts.len() {
            pipeline.reset_for_next_prompt();
        }
    }

    pipeline.shutdown();
    // ReferenceWorker drops here — its `Drop` impl kills the subprocess.
}

/// Single-prompt equivalence: the canonical "Say hello" prompt from SPEC §1
/// must produce the same token ids through the sliced 2-stage pipeline as
/// it does through a full-model `Transformer.generate`-style run.
#[ignore]
#[test]
fn sliced_two_stage_inference_matches_single_node_full_inference_for_say_hello() {
    assert_pipeline_matches_reference(&["Say hello"]);
}

/// Three diverse prompt shapes — single word, full sentence, punctuation —
/// stress position handling and tokenizer behaviour at the layer boundary.
/// Per the plan, any off-by-one in `start_pos` or a misaligned KV cache
/// slice falls out here, not on "Say hello".
#[ignore]
#[test]
fn sliced_two_stage_inference_matches_single_node_for_three_diverse_prompts() {
    assert_pipeline_matches_reference(&[
        "Hello",
        "The quick brown fox jumps over the lazy dog.",
        "Wait... what?!",
    ]);
}
