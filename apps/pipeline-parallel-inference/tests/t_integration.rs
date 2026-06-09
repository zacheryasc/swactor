//! T-integration: in-process end-to-end pipeline-parallel inference.
//!
//! TEST_SPEC §11. One `DistributedNode` per orchestrator + one per pipeline
//! stage, all in the same test process, connected over real iroh QUIC in
//! `RelayMode::Disabled`. Each stage runs the stub-mode `pp_tinygrad_worker.py`
//! (no GPU, no GGUF). The pipeline runs the autoregressive loop end-to-end:
//! orchestrator submits an `InferenceRequest`, stage 0 produces a
//! `StageActivation` for prefill, each middle stage echoes activation control
//! fields forward, the last stage samples a token and sends `NextToken` back
//! to stage 0, and so on until EOS or `max_tokens`. The last stage emits the
//! final `InferenceResponse` back to the orchestrator.
//!
//! The same shape the binary uses (see `pp_orchestrator.rs`), minus the child
//! subprocesses — actors live in this test process and addresses are wired up
//! directly without SWIM resolution.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Process-wide lock that serialises pipeline lifetimes inside this
/// test binary. The cargo default test-thread pool is `num_cpus`, and
/// with each pipeline owning N+1 iroh drivers probing at
/// `probe_interval=1` tick, the resulting fan-out (up to ~80
/// driver-pump loops at the default parallelism) reliably starved
/// SWIM convergence at N=4/5 well past any reasonable cap. A
/// concurrency limit of 1 (full serialisation of pipeline lifetimes)
/// is the only setting that converges deterministically on this
/// box across back-to-back full-suite runs — earlier values (3, 2)
/// still flaked on N=4 cases under load. `Pipeline` holds the guard
/// for its whole lifetime so cluster build *and* post-build
/// inference work both run with exclusive access.
static PIPELINE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn pipeline_lock() -> std::sync::MutexGuard<'static, ()> {
    PIPELINE_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

use distribution::iroh_driver::IrohDriverConfig;
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::{PublicKey, RelayMode};

use swactor::actor::ActorAddress;
use swactor::runtime::{Inbox, Runtime};
use swactor_transport::CodecRegistry;

use pipeline_parallel_inference::cluster::ClusterNode;
use pipeline_parallel_inference::iroh_transport::{
    ActorMessagePump, IrohActorTransport, ACTOR_ALPN,
};
use pipeline_parallel_inference::messages::{
    inference_codec_registry, InferenceRequest, InferenceResponse, NextToken,
};
use pipeline_parallel_inference::stage_actor::{
    ActivationBridge, NextTokenBridge, RequestBridge, StageActor, StageActorStatus, StageMsg,
    StageRole,
};
use swactor_process::{ProcessMode, ProcessSpec};

// ─── Node / cluster helpers ─────────────────────────────────────────────

fn test_node_config() -> DistributedNodeConfig {
    DistributedNodeConfig {
        // SWIM timing matches t_cluster's proven values: the tokio actor-bridge
        // engine drives probes via the test's pump loop, so the original 30ms
        // probe-timeout was too tight for N>=4 localhost convergence under load
        // (probes timed out before the ack was pumped). These are test-only knobs
        // — production uses SwimConfig::default().
        swim: SwimConfig {
            probe_interval: Duration::from_millis(20),
            probe_timeout: Duration::from_millis(200),
            indirect_probes: 1,
            suspicion_timeout: Duration::from_millis(500),
            dead_reprobe_interval: Duration::ZERO,
            ..SwimConfig::default()
        },
        cache_capacity: 100,
        registry: RegistryConfig::default(),
        metadata_lambda: 3,
    }
}

fn make_node() -> ClusterNode {
    ClusterNode::new(
        IrohDriverConfig {
            secret_key: None,
            relay_mode: RelayMode::Disabled,
            node: test_node_config(),
            peer_auth: None,
            additional_alpns: vec![ACTOR_ALPN.to_vec()],
        },
        test_node_config(),
        inference_codec_registry(),
        |_| {},
    )
    .expect("failed to create cluster node")
}

fn pubkey_of(node: &ClusterNode) -> PublicKey {
    PublicKey::from_bytes(&node.node_id().0).unwrap()
}

fn sees_alive(node: &ClusterNode, peer_key: &PublicKey) -> bool {
    let peer = distribution::types::NodeId(*peer_key.as_bytes());
    node.sees_alive(&peer)
}

/// Build a `num_stages + 1`-node cluster: node `[0]` is the orchestrator,
/// nodes `[1..=num_stages]` are pipeline stages `0..num_stages-1`. Every
/// non-orchestrator node joins via the orchestrator's seed address. Returns
/// once every node sees every other node alive, or panics on timeout.
///
/// Callers are expected to already hold the `PIPELINE_LOCK` (acquired
/// by `build_pipeline`/`build_real_pipeline`), so SWIM convergence
/// runs with exclusive access to the localhost network stack.
fn make_cluster(num_stages: u32) -> Vec<ClusterNode> {
    assert!(num_stages >= 2, "pipeline tests require num_stages >= 2");
    let total = num_stages as usize + 1;

    let mut nodes: Vec<ClusterNode> = (0..total).map(|_| make_node()).collect();
    let seed = nodes[0].endpoint_addr();
    for d in nodes.iter_mut().skip(1) {
        d.join(&[seed.clone()]);
    }

    let keys: Vec<PublicKey> = nodes.iter().map(pubkey_of).collect();

    let timeout = Duration::from_secs(60);
    let start = Instant::now();
    let mut converged = false;
    while start.elapsed() < timeout {
        for n in nodes.iter_mut() {
            n.pump_once();
        }
        let all_see_all = nodes.iter().enumerate().all(|(i, n)| {
            keys.iter()
                .enumerate()
                .all(|(j, k)| i == j || sees_alive(n, k))
        });
        if all_see_all {
            converged = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        converged,
        "N={num_stages} cluster did not converge within {}s",
        timeout.as_secs(),
    );
    nodes
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

fn is_process_alive(pid: u32) -> bool {
    std::fs::metadata(format!("/proc/{pid}")).is_ok()
}

fn kill_pid(pid: u32) {
    let _ = Command::new("kill").arg("-9").arg(pid.to_string()).status();
}

// ─── Pipeline harness (N-stage) ───────────────────────────────────────────

/// Fully wired in-process pipeline of `num_stages` stages plus an
/// orchestrator. Indexing: `drivers[0]` and `runtimes[0]` belong to the
/// orchestrator; `drivers[s+1]` / `runtimes[s+1]` belong to stage `s`.
/// `stage_actor_addrs[s]` / `stage_pids[s]` index stages directly.
struct Pipeline {
    num_stages: u32,
    nodes: Vec<ClusterNode>,
    pumps: Vec<ActorMessagePump>,

    codecs: Arc<CodecRegistry>,
    response_inbox: Inbox<InferenceResponse>,
    /// Receives a copy of every `NextToken` the Last stage emits, when the
    /// pipeline was built with `observe_tokens=true`. Lives on the Last
    /// stage's runtime so the token-observer route is purely local.
    token_observer_inbox: Option<Inbox<NextToken>>,
    inbox_addr: ActorAddress,
    request_bridge_addr: ActorAddress,

    stage_actor_addrs: Vec<ActorAddress>,
    stage_pids: Vec<u32>,

    /// Pipeline-lock guard held for the pipeline's whole lifetime then
    /// released on drop. See `PIPELINE_LOCK`.
    _pipeline_lock: std::sync::MutexGuard<'static, ()>,
}

/// Build an N-stage stub-worker pipeline. `max_tokens` caps the decode loop;
/// `eos` optionally pins an EOS token id on the Last stage; `observe_tokens`
/// installs an extra observer inbox on the Last stage's runtime that receives
/// a clone of every `NextToken` it emits.
fn build_pipeline(num_stages: u32, max_tokens: u32, eos: Option<u32>, observe_tokens: bool) -> Pipeline {
    assert!(num_stages >= 2, "pipeline requires num_stages >= 2");
    let last_stage = num_stages - 1;

    let pipeline_guard = pipeline_lock();
    let nodes = make_cluster(num_stages);
    let codecs = Arc::new(inference_codec_registry());

    let pumps: Vec<ActorMessagePump> = (0..=num_stages).map(|_| ActorMessagePump::new()).collect();

    // Orchestrator-side inbox for the final response.
    let response_inbox = nodes[0].rt.new_inbox::<InferenceResponse>().unwrap();
    let inbox_addr = *response_inbox.addr();

    // Per-stage status inbox — `drain_until_ready` blocks until each stage
    // reports its worker pid.
    let status_inboxes: Vec<Inbox<StageActorStatus>> = (0..num_stages)
        .map(|s| nodes[s as usize + 1].rt.new_inbox::<StageActorStatus>().unwrap())
        .collect();

    // Optional NextToken observer on the Last stage's runtime.
    let (token_observer_inbox, observer_addr): (Option<Inbox<NextToken>>, Option<ActorAddress>) = if observe_tokens {
        let inbox = nodes[last_stage as usize + 1].rt.new_inbox::<NextToken>().unwrap();
        let addr = *inbox.addr();
        (Some(inbox), Some(addr))
    } else {
        (None, None)
    };

    let placeholder = ActorAddress([0; 32]);

    // Spawn each stage actor on its node's runtime. The First's `next_stage`
    // address and the Last's `prev_stage` address need addresses from other
    // runtimes that don't exist yet, so we spawn local placeholders and
    // patch the addresses via `SetNeighbors` after every actor exists.
    let mut stage_actor_addrs: Vec<ActorAddress> = Vec::with_capacity(num_stages as usize);
    for s in 0..num_stages {
        let rt = &nodes[s as usize + 1].rt;
        let sender = rt.create_sender();
        let status_addr = *status_inboxes[s as usize].addr();
        let actor_addr = if s == 0 {
            let actor = StageActor::first(worker_spec(s, num_stages), sender, placeholder)
                .with_status_addr(status_addr);
            rt.spawn(actor).unwrap()
        } else if s == last_stage {
            let mut actor = StageActor::last(
                worker_spec(s, num_stages),
                sender,
                placeholder,
                inbox_addr,
                max_tokens,
            )
            .with_status_addr(status_addr);
            if let Some(eos_id) = eos {
                actor = actor.with_eos_token_id(eos_id);
            }
            if let Some(addr) = observer_addr {
                actor = actor.with_token_observer(addr);
            }
            rt.spawn(actor).unwrap()
        } else {
            let actor = StageActor::middle(worker_spec(s, num_stages), sender, placeholder)
                .with_status_addr(status_addr);
            rt.spawn(actor).unwrap()
        };
        stage_actor_addrs.push(actor_addr);
    }

    // Per-stage bridges. Stage 0 carries Request + NextToken bridges (its
    // inbound network types); stages 1..N-1 (Middle) and stage N-1 (Last)
    // each carry an Activation bridge.
    let request_bridge_addr = nodes[1]
        .rt
        .spawn(RequestBridge { target: stage_actor_addrs[0] })
        .unwrap();
    let nt_bridge_addr = nodes[1]
        .rt
        .spawn(NextTokenBridge { target: stage_actor_addrs[0] })
        .unwrap();
    let mut activation_bridge_addrs: Vec<Option<ActorAddress>> = vec![None; num_stages as usize];
    for s in 1..num_stages {
        let rt = &nodes[s as usize + 1].rt;
        let addr = rt
            .spawn(ActivationBridge { target: stage_actor_addrs[s as usize] })
            .unwrap();
        activation_bridge_addrs[s as usize] = Some(addr);
    }

    // Wire neighbours on every stage actor.
    for s in 0..num_stages {
        let rt = &nodes[s as usize + 1].rt;
        let (next, prev) = if s == last_stage {
            (None, Some(nt_bridge_addr))
        } else {
            (
                activation_bridge_addrs[(s + 1) as usize],
                None,
            )
        };
        rt.send_to(
            stage_actor_addrs[s as usize],
            StageMsg::SetNeighbors {
                prev_stage: prev,
                next_stage: next,
                reply_to: None,
            },
        )
        .unwrap();
    }

    // Wait for every worker to come up. The ready line for each stage is
    // delivered locally on that stage's runtime, so per-stage drain is
    // independent.
    let mut stage_pids: Vec<u32> = Vec::with_capacity(num_stages as usize);
    for s in 0..num_stages {
        let rt = &nodes[s as usize + 1].rt;
        let pid = drain_until_ready(rt, &status_inboxes[s as usize], Duration::from_secs(15));
        stage_pids.push(pid);
    }

    // Transport routes. The ClusterNode already wired a `TransportRouter`
    // shared with the protocol actors and pre-installed it on `rt`; we
    // only need to add per-stage app routes to it.
    //
    // orch → stage 0's request bridge:
    let orch_to_first = Arc::new(IrohActorTransport::new(
        nodes[0].driver.endpoint().clone(),
        nodes[1].endpoint_addr(),
        nodes[0].driver.tokio_handle(),
    ));
    nodes[0].transport_router.add_route(request_bridge_addr, orch_to_first);

    // Stage s (0..N-1) → stage s+1's activation bridge.
    for s in 0..num_stages - 1 {
        let rt_idx = s as usize + 1;
        let next_idx = (s + 1) as usize + 1;
        let transport = Arc::new(IrohActorTransport::new(
            nodes[rt_idx].driver.endpoint().clone(),
            nodes[next_idx].endpoint_addr(),
            nodes[rt_idx].driver.tokio_handle(),
        ));
        nodes[rt_idx].transport_router.add_route(
            activation_bridge_addrs[(s + 1) as usize].expect("next stage has activation bridge"),
            transport,
        );
    }

    // Last stage → stage 0's NextToken bridge AND → orchestrator's response
    // inbox.
    let last_idx = last_stage as usize + 1;
    let last_to_first = Arc::new(IrohActorTransport::new(
        nodes[last_idx].driver.endpoint().clone(),
        nodes[1].endpoint_addr(),
        nodes[last_idx].driver.tokio_handle(),
    ));
    let last_to_orch = Arc::new(IrohActorTransport::new(
        nodes[last_idx].driver.endpoint().clone(),
        nodes[0].endpoint_addr(),
        nodes[last_idx].driver.tokio_handle(),
    ));
    nodes[last_idx].transport_router.add_route(nt_bridge_addr, last_to_first);
    nodes[last_idx].transport_router.add_route(inbox_addr, last_to_orch);

    Pipeline {
        num_stages,
        nodes,
        pumps,
        codecs,
        response_inbox,
        token_observer_inbox,
        inbox_addr,
        request_bridge_addr,
        stage_actor_addrs,
        stage_pids,
        _pipeline_lock: pipeline_guard,
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

impl Pipeline {
    /// One pass through every node: drain QUIC, pump actor messages into each
    /// runtime, tick every runtime.
    fn pump(&mut self) {
        for n in &mut self.nodes {
            n.pump_once();
        }
        for (i, pump) in self.pumps.iter().enumerate() {
            pump.pump(&self.nodes[i].driver, &self.codecs, &self.nodes[i].rt);
        }
    }

    /// Pump only the orchestrator side. Used after one of the stages has been
    /// shut down; pumping a dead node's runtime contributes nothing and just
    /// clutters the loop.
    fn pump_orch_only(&mut self) {
        self.nodes[0].pump_once();
        self.pumps[0].pump(&self.nodes[0].driver, &self.codecs, &self.nodes[0].rt);
    }

    fn submit(&self, prompt: &str, max_tokens: u32) {
        let req = InferenceRequest {
            reply_to: self.inbox_addr,
            prompt: prompt.into(),
            max_tokens,
        };
        self.nodes[0]
            .rt
            .send_to(self.request_bridge_addr, req)
            .unwrap();
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
    /// orchestrator's SWIM view. With N stages plus self, the orchestrator's
    /// snapshot (which omits self) lists `num_stages` peers when healthy;
    /// `alive_count < num_stages` means at least one stage is no longer
    /// reachable.
    fn await_response_or_stage_failure(
        &mut self,
        timeout: Duration,
    ) -> Result<InferenceResponse, String> {
        let start = Instant::now();
        let expected = self.num_stages;
        while start.elapsed() < timeout {
            self.pump_orch_only();
            if let Some(r) = self.response_inbox.try_recv() {
                return Ok(r);
            }
            let snap = self.nodes[0].snapshot();
            if (snap.alive_count as u32) < expected {
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
        for (s, addr) in self.stage_actor_addrs.iter().enumerate() {
            let _ = self.nodes[s + 1].rt.stop_actor(*addr);
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            for s in 0..self.num_stages as usize {
                self.nodes[s + 1].rt.tick();
            }
            if self.stage_pids.iter().all(|p| !is_process_alive(*p)) {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        for n in self.nodes.iter_mut() {
            n.driver.shutdown();
        }
    }

    fn stage_pid(&self, stage: u32) -> u32 {
        self.stage_pids[stage as usize]
    }
}

/// Parse the stub `detokenize_stub` output `"tokens: [a b c]"` into a vector
/// of token ids. The format is owned by `StageActor::detokenize_stub`.
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
// §11 — In-process N-stage integration tests
// ═══════════════════════════════════════════════════════════════════════

/// Full pipeline at N stages: orchestrator submits one `InferenceRequest`,
/// the chain drives the autoregressive loop with stub workers, the Last
/// stage emits `InferenceResponse` back to the orchestrator within the
/// test timeout, and the response text is non-empty.
fn n_stage_stub_pipeline_returns_response_case(num_stages: u32) {
    let mut pipeline = build_pipeline(num_stages, 4, None, false);
    pipeline.submit("Say hello", 4);

    let response = pipeline
        .await_response(Duration::from_secs(60))
        .expect("pipeline must produce an InferenceResponse");

    assert!(
        !response.text.is_empty(),
        "N={num_stages}: response text must be non-empty, got: {:?}",
        response.text
    );
    let tokens = parse_stub_tokens(&response.text);
    assert_eq!(
        tokens.len(),
        4,
        "N={num_stages}: stub response must contain max_tokens=4 token ids: {:?}",
        response.text
    );

    pipeline.shutdown();
}

#[test]
fn n_stage_stub_pipeline_returns_response_n_2() {
    n_stage_stub_pipeline_returns_response_case(2);
}

#[test]
fn n_stage_stub_pipeline_returns_response_n_3() {
    n_stage_stub_pipeline_returns_response_case(3);
}

#[test]
fn n_stage_stub_pipeline_returns_response_n_4() {
    n_stage_stub_pipeline_returns_response_case(4);
}

#[test]
fn n_stage_stub_pipeline_returns_response_n_5() {
    n_stage_stub_pipeline_returns_response_case(5);
}

/// Configure the Last stage with an EOS token id and verify the decode loop
/// terminates with exactly that many accumulated tokens. Done by running the
/// pipeline twice with the same prompt: the first run discovers the
/// deterministic stub token sequence at this N, the second pins EOS to one
/// of those tokens and asserts the response truncates at the matching index.
fn decode_loop_terminates_on_stub_eos_case(num_stages: u32) {
    const PROMPT: &str = "Say hello once";
    const PROBE_TOKENS: u32 = 5;
    const TARGET_N: usize = 3;

    let probe_tokens = {
        let mut probe = build_pipeline(num_stages, PROBE_TOKENS, None, false);
        probe.submit(PROMPT, PROBE_TOKENS);
        let resp = probe
            .await_response(Duration::from_secs(60))
            .expect("probe pipeline must produce a response");
        let tokens = parse_stub_tokens(&resp.text);
        assert_eq!(
            tokens.len(),
            PROBE_TOKENS as usize,
            "N={num_stages}: probe (no EOS, max_tokens={PROBE_TOKENS}) should yield {PROBE_TOKENS} tokens"
        );
        probe.shutdown();
        tokens
    };

    let eos = probe_tokens[TARGET_N - 1];

    let mut real = build_pipeline(num_stages, PROBE_TOKENS, Some(eos), false);
    real.submit(PROMPT, PROBE_TOKENS);
    let resp = real
        .await_response(Duration::from_secs(60))
        .expect("EOS pipeline must produce a response");
    let tokens = parse_stub_tokens(&resp.text);
    assert_eq!(
        tokens.len(),
        TARGET_N,
        "N={num_stages}: EOS at index {} should truncate response to {TARGET_N} tokens, got {tokens:?}",
        TARGET_N - 1
    );
    assert_eq!(
        *tokens.last().unwrap(),
        eos,
        "N={num_stages}: last accumulated token must be the EOS id"
    );
    real.shutdown();
}

#[test]
fn decode_loop_terminates_on_stub_eos_n_2() {
    decode_loop_terminates_on_stub_eos_case(2);
}

#[test]
fn decode_loop_terminates_on_stub_eos_n_3() {
    decode_loop_terminates_on_stub_eos_case(3);
}

#[test]
fn decode_loop_terminates_on_stub_eos_n_4() {
    decode_loop_terminates_on_stub_eos_case(4);
}

/// With no EOS configured, the decode loop runs until `max_tokens` and the
/// Last stage emits `InferenceResponse` containing exactly `max_tokens`
/// tokens.
fn decode_loop_terminates_on_max_tokens_case(num_stages: u32) {
    const MAX: u32 = 4;

    let mut pipeline = build_pipeline(num_stages, MAX, None, false);
    pipeline.submit("max tokens stop", MAX);

    let response = pipeline
        .await_response(Duration::from_secs(60))
        .expect("max_tokens pipeline must produce a response");
    let tokens = parse_stub_tokens(&response.text);
    assert_eq!(
        tokens.len() as u32,
        MAX,
        "N={num_stages}: with no EOS, response must contain exactly max_tokens={MAX} tokens, got {tokens:?}"
    );
    pipeline.shutdown();
}

#[test]
fn decode_loop_terminates_on_max_tokens_n_2() {
    decode_loop_terminates_on_max_tokens_case(2);
}

#[test]
fn decode_loop_terminates_on_max_tokens_n_3() {
    decode_loop_terminates_on_max_tokens_case(3);
}

#[test]
fn decode_loop_terminates_on_max_tokens_n_4() {
    decode_loop_terminates_on_max_tokens_case(4);
}

/// Mid-decode failure for the stage at index `victim_stage` in an N=4
/// pipeline. Kills the worker process AND shuts down the stage's iroh
/// driver. The orchestrator's await loop must surface a stage failure
/// (via SWIM) within the detection window — not hang.
fn stage_failure_mid_decode_case(victim_stage: u32) {
    let num_stages: u32 = 4;
    let mut pipeline = build_pipeline(num_stages, 64, None, false);
    pipeline.submit("never finishes naturally", 64);

    // Let a few rounds happen so we are genuinely mid-decode.
    let pump_start = Instant::now();
    while pump_start.elapsed() < Duration::from_millis(400) {
        pipeline.pump();
        if pipeline.response_inbox.try_recv().is_some() {
            panic!(
                "N={num_stages}: pipeline returned a response too quickly to test mid-decode failure"
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    // Kill the victim stage's worker + driver. Worker death alone leaves
    // the SWIM node alive (it just stops doing pipeline work); killing the
    // driver too is what flips it to dead in the orchestrator's view.
    let victim_pid = pipeline.stage_pid(victim_stage);
    kill_pid(victim_pid);
    pipeline.nodes[victim_stage as usize + 1].driver.shutdown();

    // SwimConfig probe/suspicion windows are tight in tests → dead within
    // ~8-10s. Give 20s for indirect probes + gossip propagation.
    let result = pipeline.await_response_or_stage_failure(Duration::from_secs(20));
    assert!(
        result.is_err(),
        "orchestrator should NOT receive an InferenceResponse after stage {victim_stage} dies; got {result:?}",
    );

    // Tear down survivors.
    for s in 0..num_stages {
        if s != victim_stage {
            let _ = pipeline.nodes[s as usize + 1].rt.stop_actor(pipeline.stage_actor_addrs[s as usize]);
        }
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        for s in 0..num_stages {
            if s != victim_stage {
                pipeline.nodes[s as usize + 1].rt.tick();
            }
        }
        let all_dead = (0..num_stages)
            .filter(|s| *s != victim_stage)
            .all(|s| !is_process_alive(pipeline.stage_pid(s)));
        if all_dead {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    for (i, n) in pipeline.nodes.iter_mut().enumerate() {
        // Node at `victim_stage + 1` is already shut down.
        if i != victim_stage as usize + 1 {
            n.driver.shutdown();
        }
    }
}

#[test]
fn first_stage_failure_mid_decode_surfaces_as_error_to_orchestrator() {
    stage_failure_mid_decode_case(0);
}

#[test]
fn middle_stage_failure_mid_decode_surfaces_as_error_to_orchestrator() {
    // N=4 has middle stages 1 and 2; pick 1 (closest to First).
    stage_failure_mid_decode_case(1);
}

#[test]
fn last_stage_failure_mid_decode_surfaces_as_error_to_orchestrator() {
    stage_failure_mid_decode_case(3);
}

/// Every `NextToken` returning to stage 0 carries the same `request_id` as
/// the `StageActivation` that traversed first → middle → middle → last.
/// First's `next_request_id` starts at 1 and increments per outbound
/// activation, so for a single `InferenceRequest` the round-trip rid is
/// stable across every decode step. This test asserts that every observed
/// `NextToken` carries the same rid and that the same rid produces a
/// non-empty response.
#[test]
fn activation_request_id_round_trip_through_chain() {
    const MAX: u32 = 4;
    const NUM_STAGES: u32 = 4;

    let mut pipeline = build_pipeline(NUM_STAGES, MAX, None, true);
    pipeline.submit("rid round trip", MAX);

    // Drive the pipeline until the final response arrives, draining the
    // observer inbox on the way. Two observed tokens are enough to prove
    // "stable across hops"; collect all of them up to max_tokens for a
    // sharper assertion.
    let mut observed: Vec<NextToken> = Vec::with_capacity(MAX as usize);
    let start = Instant::now();
    let mut response: Option<InferenceResponse> = None;
    while response.is_none() && start.elapsed() < Duration::from_secs(60) {
        pipeline.pump();
        if let Some(observer) = pipeline.token_observer_inbox.as_ref() {
            while let Some(nt) = observer.try_recv() {
                observed.push(nt);
            }
        }
        if let Some(r) = pipeline.response_inbox.try_recv() {
            response = Some(r);
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    let response = response.expect("N=4: pipeline must produce a response");
    assert!(!response.text.is_empty(), "N=4: response text must be non-empty");

    // Drain anything that arrived after the response.
    if let Some(observer) = pipeline.token_observer_inbox.as_ref() {
        while let Some(nt) = observer.try_recv() {
            observed.push(nt);
        }
    }

    assert!(
        !observed.is_empty(),
        "N=4: must have observed at least one NextToken from Last"
    );
    let first_rid = observed[0].request_id;
    assert_ne!(first_rid, 0, "N=4: round-trip request_id must be non-zero");
    for (i, nt) in observed.iter().enumerate() {
        assert_eq!(
            nt.request_id, first_rid,
            "N=4: NextToken #{i} has request_id {} (expected {first_rid}); rid must be stable across the full first->middle->middle->last->first round-trip",
            nt.request_id,
        );
    }

    pipeline.shutdown();
}

/// Each `StageActivation` after prefill has `position` exactly one greater
/// than the previous, end-to-end through the chain. The Last stage's
/// `NextToken.position` equals the inbound `activation.position +
/// activation.seq_len`; for decode steps `seq_len == 1`, so successive
/// `NextToken.position`s differ by 1 iff the activation positions did.
/// Observing `NextToken`s is therefore a faithful proxy for the activation
/// position stream.
#[test]
fn activation_position_advances_one_per_decode_step() {
    const MAX: u32 = 4;
    const NUM_STAGES: u32 = 3;
    const PROMPT: &str = "position advances";

    let mut pipeline = build_pipeline(NUM_STAGES, MAX, None, true);
    pipeline.submit(PROMPT, MAX);

    let mut observed: Vec<NextToken> = Vec::with_capacity(MAX as usize);
    let start = Instant::now();
    let mut response: Option<InferenceResponse> = None;
    while response.is_none() && start.elapsed() < Duration::from_secs(60) {
        pipeline.pump();
        if let Some(observer) = pipeline.token_observer_inbox.as_ref() {
            while let Some(nt) = observer.try_recv() {
                observed.push(nt);
            }
        }
        if let Some(r) = pipeline.response_inbox.try_recv() {
            response = Some(r);
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    response.expect("N=3: pipeline must produce a response");
    if let Some(observer) = pipeline.token_observer_inbox.as_ref() {
        while let Some(nt) = observer.try_recv() {
            observed.push(nt);
        }
    }

    assert_eq!(
        observed.len(),
        MAX as usize,
        "N=3: expected {MAX} NextTokens for max_tokens={MAX} run, got {}: {observed:?}",
        observed.len(),
    );

    // First NextToken's position == prompt_len (set by Last from the
    // prefill activation). Subsequent positions increment by 1 per decode
    // step.
    let prompt_len = PROMPT.split_whitespace().count() as u32;
    assert_eq!(
        observed[0].position, prompt_len,
        "N=3: first NextToken position must equal prompt_len ({prompt_len}); got {}",
        observed[0].position,
    );
    for (i, win) in observed.windows(2).enumerate() {
        let prev = &win[0];
        let next = &win[1];
        assert_eq!(
            next.position,
            prev.position + 1,
            "N=3: position must advance by 1 between decode steps #{i} -> #{}; prev={prev:?}, next={next:?}",
            i + 1,
        );
    }

    pipeline.shutdown();
}

// ═══════════════════════════════════════════════════════════════════════
// §12 — Sliced-vs-full equivalence (gated)
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

const REAL_READY_TIMEOUT: Duration = Duration::from_secs(180);
const REAL_PROMPT_TIMEOUT: Duration = Duration::from_secs(300);
const EQUIVALENCE_MAX_TOKENS: u32 = 8;

fn real_worker_spec(stage: u32, num_stages: u32) -> ProcessSpec {
    let mut env = HashMap::new();
    env.insert("STAGE".into(), stage.to_string());
    env.insert("NUM_STAGES".into(), num_stages.to_string());
    env.insert("MODEL".into(), "llama3.2:1b".into());
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

/// Build a real-mode N-stage pipeline with a token observer on the Last
/// stage. Same actor wiring as `build_pipeline` but with the
/// real-tinygrad worker, `with_real_tokenization` on First (synthetic
/// stub ids cannot be embedded against a real GGUF vocab), and a
/// minute-scale ready timeout so the GGUF can load on a cold tinygrad
/// cache.
fn build_real_pipeline(num_stages: u32, max_tokens: u32) -> Pipeline {
    assert!(num_stages >= 2, "real pipeline requires num_stages >= 2");
    let last_stage = num_stages - 1;

    let pipeline_guard = pipeline_lock();
    let nodes = make_cluster(num_stages);
    let codecs = Arc::new(inference_codec_registry());

    let pumps: Vec<ActorMessagePump> = (0..=num_stages).map(|_| ActorMessagePump::new()).collect();

    let response_inbox = nodes[0].rt.new_inbox::<InferenceResponse>().unwrap();
    let inbox_addr = *response_inbox.addr();

    let status_inboxes: Vec<Inbox<StageActorStatus>> = (0..num_stages)
        .map(|s| nodes[s as usize + 1].rt.new_inbox::<StageActorStatus>().unwrap())
        .collect();

    let token_observer_inbox = nodes[last_stage as usize + 1]
        .rt
        .new_inbox::<NextToken>()
        .unwrap();
    let observer_addr = *token_observer_inbox.addr();

    let placeholder = ActorAddress([0; 32]);

    let mut stage_actor_addrs: Vec<ActorAddress> = Vec::with_capacity(num_stages as usize);
    for s in 0..num_stages {
        let rt = &nodes[s as usize + 1].rt;
        let sender = rt.create_sender();
        let status_addr = *status_inboxes[s as usize].addr();
        let actor_addr = if s == 0 {
            let actor = StageActor::first(real_worker_spec(s, num_stages), sender, placeholder)
                .with_status_addr(status_addr)
                .with_real_tokenization();
            rt.spawn(actor).unwrap()
        } else if s == last_stage {
            let actor = StageActor::last(
                real_worker_spec(s, num_stages),
                sender,
                placeholder,
                inbox_addr,
                max_tokens,
            )
            .with_status_addr(status_addr)
            .with_token_observer(observer_addr);
            rt.spawn(actor).unwrap()
        } else {
            let actor = StageActor::middle(real_worker_spec(s, num_stages), sender, placeholder)
                .with_status_addr(status_addr);
            rt.spawn(actor).unwrap()
        };
        stage_actor_addrs.push(actor_addr);
    }

    // Bridges: Request + NextToken on stage 0; Activation on every stage
    // s >= 1.
    let request_bridge_addr = nodes[1]
        .rt
        .spawn(RequestBridge { target: stage_actor_addrs[0] })
        .unwrap();
    let nt_bridge_addr = nodes[1]
        .rt
        .spawn(NextTokenBridge { target: stage_actor_addrs[0] })
        .unwrap();
    let mut activation_bridge_addrs: Vec<Option<ActorAddress>> = vec![None; num_stages as usize];
    for s in 1..num_stages {
        let rt = &nodes[s as usize + 1].rt;
        let addr = rt
            .spawn(ActivationBridge { target: stage_actor_addrs[s as usize] })
            .unwrap();
        activation_bridge_addrs[s as usize] = Some(addr);
    }

    // Wire neighbours: First/Middle → next stage's activation bridge;
    // Last → first stage's NextToken bridge.
    for s in 0..num_stages {
        let rt = &nodes[s as usize + 1].rt;
        let (next, prev) = if s == last_stage {
            (None, Some(nt_bridge_addr))
        } else {
            (activation_bridge_addrs[(s + 1) as usize], None)
        };
        rt.send_to(
            stage_actor_addrs[s as usize],
            StageMsg::SetNeighbors {
                prev_stage: prev,
                next_stage: next,
                reply_to: None,
            },
        )
        .unwrap();
    }

    // Real workers can take well over a minute to load the GGUF on a cold
    // tinygrad cache. Pumping every runtime in parallel keeps any one
    // stage from starving the others' process bridges.
    let rts_refs: Vec<Arc<Runtime>> = nodes.iter().skip(1).map(|n| Arc::clone(&n.rt)).collect();
    let rts_borrows: Vec<&Runtime> = rts_refs.iter().map(|a| a.as_ref()).collect();
    let mut stage_pids: Vec<u32> = Vec::with_capacity(num_stages as usize);
    for s in 0..num_stages {
        let label = match StageRole::for_stage(s, num_stages) {
            StageRole::First => format!("stage-{s} real worker (first)"),
            StageRole::Middle => format!("stage-{s} real worker (middle)"),
            StageRole::Last => format!("stage-{s} real worker (last)"),
        };
        let pid = drain_until_ready_pumped(
            &rts_borrows,
            &status_inboxes[s as usize],
            REAL_READY_TIMEOUT,
            &label,
        );
        stage_pids.push(pid);
    }
    drop(rts_borrows);
    drop(rts_refs);

    // Transport routes (mirror of build_pipeline).
    let orch_to_first = Arc::new(IrohActorTransport::new(
        nodes[0].driver.endpoint().clone(),
        nodes[1].endpoint_addr(),
        nodes[0].driver.tokio_handle(),
    ));
    nodes[0].transport_router.add_route(request_bridge_addr, orch_to_first);

    for s in 0..num_stages - 1 {
        let rt_idx = s as usize + 1;
        let next_idx = (s + 1) as usize + 1;
        let transport = Arc::new(IrohActorTransport::new(
            nodes[rt_idx].driver.endpoint().clone(),
            nodes[next_idx].endpoint_addr(),
            nodes[rt_idx].driver.tokio_handle(),
        ));
        nodes[rt_idx].transport_router.add_route(
            activation_bridge_addrs[(s + 1) as usize].expect("next stage has activation bridge"),
            transport,
        );
    }

    let last_idx = last_stage as usize + 1;
    let last_to_first = Arc::new(IrohActorTransport::new(
        nodes[last_idx].driver.endpoint().clone(),
        nodes[1].endpoint_addr(),
        nodes[last_idx].driver.tokio_handle(),
    ));
    let last_to_orch = Arc::new(IrohActorTransport::new(
        nodes[last_idx].driver.endpoint().clone(),
        nodes[0].endpoint_addr(),
        nodes[last_idx].driver.tokio_handle(),
    ));
    nodes[last_idx].transport_router.add_route(nt_bridge_addr, last_to_first);
    nodes[last_idx].transport_router.add_route(inbox_addr, last_to_orch);

    Pipeline {
        num_stages,
        nodes,
        pumps,
        codecs,
        response_inbox,
        token_observer_inbox: Some(token_observer_inbox),
        inbox_addr,
        request_bridge_addr,
        stage_actor_addrs,
        stage_pids,
        _pipeline_lock: pipeline_guard,
    }
}

/// Tick every runtime in `rts` until `inbox` produces a `WorkerReady`,
/// returning the reported pid. Used by `build_real_pipeline` where the
/// model load can take much longer than `drain_until_ready`'s 10s budget
/// — the swactor process bridge consumes stdout on its host runtime, so
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
                return pid.unwrap_or_else(|| panic!("{label}: WorkerReady with no pid"));
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

        // Drain the final InferenceResponse so a residual response in the
        // inbox doesn't leak across prompts when the harness is reused.
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

    /// Reset every stage actor so a fresh prompt can be submitted. The
    /// worker's per-block KV cache is left alone — the next prefill at
    /// `position=0` rewrites the cache positions it needs.
    fn reset_for_next_prompt(&mut self) {
        for (s, addr) in self.stage_actor_addrs.iter().enumerate() {
            self.nodes[s + 1].rt.send_to(*addr, StageMsg::Reset).unwrap();
        }
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
        cmd.env("NUM_STAGES", "2");
        cmd.env("MODEL", "llama3.2:1b");
        cmd.env_remove("PP_WORKER_STUB");
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::inherit());
        let mut child = cmd.spawn().expect("spawn reference worker");
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let stdin = child.stdin.take().unwrap();
        let mut me = Self {
            child,
            stdout,
            stdin,
            next_rid: 1,
        };
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
        let n = self
            .stdout
            .read_line(&mut line)
            .expect("reference worker: read stdout");
        if n == 0 {
            panic!("reference worker closed stdout before replying");
        }
        serde_json::from_str(line.trim())
            .unwrap_or_else(|e| panic!("reference worker: invalid JSON {line:?}: {e}"))
    }

    /// Ask the reference worker for the `max_tokens` argmax-sampled
    /// continuations of `prompt`.
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
                    .unwrap_or_else(|| panic!("reference token not u64: {t}")) as u32
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

/// The three "diverse" prompts §12 enumerates: a one-word prompt, a
/// well-formed sentence, and a short string with punctuation. Catches
/// per-prompt-shape bugs the single-prompt tests cannot — see TEST_SPEC
/// §12 for why each shape matters.
const DIVERSE_PROMPTS: &[&str] = &[
    "Hello",
    "The quick brown fox jumps over the lazy dog.",
    "Wait... what?!",
];

/// Drive every prompt through both an `num_stages`-stage sliced pipeline
/// and the single-process reference, asserting per-prompt token-id
/// equality up to `EQUIVALENCE_MAX_TOKENS`.
fn assert_pipeline_matches_reference(num_stages: u32, prompts: &[&str]) {
    assert!(!prompts.is_empty(), "need at least one prompt");

    let mut reference = ReferenceWorker::spawn();
    let mut pipeline = build_real_pipeline(num_stages, EQUIVALENCE_MAX_TOKENS);

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
            "N={num_stages}: sliced pipeline diverged from full-model reference on prompt #{i} {prompt:?}\n  pipeline:  {pipeline_tokens:?}\n  reference: {ref_tokens:?}"
        );

        if i + 1 < prompts.len() {
            pipeline.reset_for_next_prompt();
        }
    }

    pipeline.shutdown();
}

#[ignore]
#[test]
fn sliced_two_stage_inference_matches_single_node_for_say_hello() {
    assert_pipeline_matches_reference(2, &["Say hello"]);
}

#[ignore]
#[test]
fn sliced_two_stage_inference_matches_single_node_for_three_diverse_prompts() {
    assert_pipeline_matches_reference(2, DIVERSE_PROMPTS);
}

#[ignore]
#[test]
fn sliced_three_stage_inference_matches_single_node_for_say_hello() {
    assert_pipeline_matches_reference(3, &["Say hello"]);
}

#[ignore]
#[test]
fn sliced_three_stage_inference_matches_single_node_for_three_diverse_prompts() {
    assert_pipeline_matches_reference(3, DIVERSE_PROMPTS);
}

#[ignore]
#[test]
fn sliced_four_stage_inference_matches_single_node_for_say_hello() {
    assert_pipeline_matches_reference(4, &["Say hello"]);
}

/// The N-invariance property: with argmax sampling the token sequence is
/// independent of chain length. Runs the same prompt through pipelines at
/// `N ∈ {2, 3, 4}` and asserts every run yields the same token ids. This
/// is the test that catches silent state corruption a longer chain would
/// introduce — e.g. a Middle stage that subtly mangles `hidden` would pass
/// every individual N-vs-reference test if reference and pipeline mangled
/// it the same way, but the N=2 run (which has no Middle) would diverge.
#[ignore]
#[test]
fn sliced_n_stage_inference_is_invariant_to_n_for_argmax() {
    const PROMPT: &str = "Say hello";

    let mut reference = ReferenceWorker::spawn();
    let ref_tokens = reference.generate_full(PROMPT, EQUIVALENCE_MAX_TOKENS);

    let mut per_n_tokens: Vec<(u32, Vec<u32>)> = Vec::with_capacity(3);
    for &num_stages in &[2u32, 3, 4] {
        let mut pipeline = build_real_pipeline(num_stages, EQUIVALENCE_MAX_TOKENS);
        let tokens = pipeline.run_real_prompt(PROMPT, EQUIVALENCE_MAX_TOKENS, REAL_PROMPT_TIMEOUT);
        pipeline.shutdown();
        per_n_tokens.push((num_stages, tokens));
    }

    for (n, tokens) in &per_n_tokens {
        assert_eq!(
            tokens, &ref_tokens,
            "N={n}: pipeline disagrees with single-node reference\n  pipeline:  {tokens:?}\n  reference: {ref_tokens:?}"
        );
    }

    let (first_n, first_tokens) = &per_n_tokens[0];
    for (n, tokens) in per_n_tokens.iter().skip(1) {
        assert_eq!(
            tokens, first_tokens,
            "N-invariance violated: N={first_n} produced {first_tokens:?} but N={n} produced {tokens:?}"
        );
    }
}
