//! Stage actors — `Stage0Actor` and `Stage1Actor`.
//!
//! Each stage actor owns one Python worker subprocess (via `swactor_process`)
//! and translates pipeline messages into stdin JSON / parses stdout JSON. The
//! ProcessBridge / RequestBridge pattern mirrors the single-GPU example.
//!
//! Stage 0:
//! * On `InferenceRequest`: tokenize the prompt (stub: whitespace split),
//!   send `embed_and_forward` to the worker, then forward the resulting
//!   hidden state to the next stage as `StageActivation { is_prefill: true }`.
//! * On `NextToken { done: false }`: send `decode_step` to the worker, then
//!   forward the resulting hidden state as `StageActivation { is_prefill: false, seq_len: 1 }`.
//! * On `NextToken { done: true }`: no further activations — the decode loop
//!   has terminated.
//!
//! Stage 1:
//! * On `StageActivation`: send `forward_and_sample` to the worker, accumulate
//!   the sampled token, emit `NextToken` to the previous stage, and emit
//!   `InferenceResponse` to `reply_to` when EOS or `max_tokens` is reached.

use std::collections::HashMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::ExternalSender;
use swactor_process::{
    ExitStatus, ProcessCommand, ProcessNotification, ProcessSpec, spawn_local_process,
};

use crate::messages::{InferenceRequest, InferenceResponse, NextToken, StageActivation};

// ─── Common status notifications ──────────────────────────────────────────

/// Lifecycle notifications emitted to an optional observer address. Same
/// shape for both stages so tests share their startup helpers.
#[derive(Clone, Debug)]
pub enum StageActorStatus {
    ProcessStarted,
    WorkerReady { pid: Option<u32> },
    ProcessExited { status: ExitStatus },
}

// ─── Shared worker bookkeeping ────────────────────────────────────────────

/// Tokenize a prompt for the stub worker. Splits on whitespace. The token
/// count returned here is exactly the `seq_len` the actor will emit in the
/// resulting `StageActivation`, so callers (including tests) can predict it
/// directly from the input string.
pub fn stub_tokenize_prompt(prompt: &str) -> Vec<i64> {
    prompt
        .split_whitespace()
        .enumerate()
        .map(|(i, word)| {
            // Sum of bytes, salted by word index, keeps tokens in a small
            // range and ensures distinct words produce distinct ids in
            // typical inputs. Exact mapping is not part of the actor's
            // contract.
            let s: u32 = word.bytes().map(u32::from).sum();
            ((s % 1024) as i64) + (i as i64)
        })
        .collect()
}

fn parse_status_line(val: &serde_json::Value) -> Option<Option<u32>> {
    if val.get("status").and_then(|v| v.as_str()) == Some("ready") {
        let pid = val.get("pid").and_then(|v| v.as_u64()).map(|p| p as u32);
        Some(pid)
    } else {
        None
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Stage 0
// ═══════════════════════════════════════════════════════════════════════

/// Union of messages the `Stage0Actor` accepts internally. Network arrivals
/// (`InferenceRequest`, `NextToken`) and child-process notifications are
/// adapted to this enum by the bridge actors below.
///
/// `SetNextStage` is a one-shot setup message used by the `pp-gpu-node`
/// binary to inject the resolved address of `pp-stage-1` after SWIM
/// gossip has propagated it. Tests construct the actor with the real
/// address up front and never send it.
///
/// `Reset` clears all per-request state (pending worker round-trips). Used
/// by the equivalence tests to drive multiple prompts through a single
/// long-lived pipeline; the worker's per-block KV cache resets implicitly
/// when the next request's prefill rewrites positions `[0, prompt_len)`.
#[derive(Clone, Debug)]
pub enum Stage0Msg {
    Inference(InferenceRequest),
    NextToken(NextToken),
    Process(ProcessNotification),
    SetNextStage(ActorAddress),
    Reset,
}

struct Stage0ProcessBridge {
    target: ActorAddress,
}

impl ActorInterface for Stage0ProcessBridge {
    type Incoming = ProcessNotification;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: ProcessNotification) {
        let _ = ctx.send(self.target, Stage0Msg::Process(msg));
    }
}

/// Routes raw `InferenceRequest` messages from the network into the actor.
pub struct Stage0RequestBridge {
    pub target: ActorAddress,
}

impl ActorInterface for Stage0RequestBridge {
    type Incoming = InferenceRequest;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: InferenceRequest) {
        let _ = ctx.send(self.target, Stage0Msg::Inference(msg));
    }
}

/// Routes raw `NextToken` messages from the network into the actor.
pub struct Stage0NextTokenBridge {
    pub target: ActorAddress,
}

impl ActorInterface for Stage0NextTokenBridge {
    type Incoming = NextToken;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: NextToken) {
        let _ = ctx.send(self.target, Stage0Msg::NextToken(msg));
    }
}

struct Stage0Pending {
    request_id: u64,
    position: u32,
    is_prefill: bool,
}

/// One entry per in-flight `tokenize` round-trip. When the worker replies
/// with the token ids, the actor uses `forward_request_id` as the rid for
/// the follow-up `embed_and_forward` request.
struct Stage0Tokenize {
    forward_request_id: u64,
}

pub struct Stage0Actor {
    spec: ProcessSpec,
    sender: ExternalSender,
    next_stage_addr: ActorAddress,
    status_addr: Option<ActorAddress>,
    /// When true, tokenize the prompt via the worker's `tokenize` op
    /// instead of the in-actor whitespace stub. Required for real-mode
    /// workers since their `embed_and_forward` expects real GGUF vocab
    /// ids, not synthetic ones.
    tokenize_via_worker: bool,

    process_addr: Option<ActorAddress>,
    bridge_addr: Option<ActorAddress>,
    ready: bool,
    process_alive: bool,
    output_buffer: String,

    next_request_id: u64,
    pending_worker: HashMap<u64, Stage0Pending>,
    pending_tokenize: HashMap<u64, Stage0Tokenize>,
}

impl Stage0Actor {
    pub fn new(
        spec: ProcessSpec,
        sender: ExternalSender,
        next_stage_addr: ActorAddress,
    ) -> Self {
        Self {
            spec,
            sender,
            next_stage_addr,
            status_addr: None,
            tokenize_via_worker: false,
            process_addr: None,
            bridge_addr: None,
            ready: false,
            process_alive: false,
            output_buffer: String::new(),
            next_request_id: 1,
            pending_worker: HashMap::new(),
            pending_tokenize: HashMap::new(),
        }
    }

    pub fn with_status_addr(mut self, addr: ActorAddress) -> Self {
        self.status_addr = Some(addr);
        self
    }

    /// Route prompt tokenization through the worker's `tokenize` op. Use
    /// this with real-mode workers; the default (stub) path bypasses the
    /// worker and uses an in-actor whitespace splitter, which produces
    /// synthetic ids that real GGUF vocabularies cannot embed.
    pub fn with_real_tokenization(mut self) -> Self {
        self.tokenize_via_worker = true;
        self
    }

    fn write_to_worker(&self, ctx: &Ctx, json: serde_json::Value) {
        let Some(proc_addr) = self.process_addr else {
            return;
        };
        let mut data = serde_json::to_vec(&json).unwrap_or_default();
        data.push(b'\n');
        let _ = ctx.send(proc_addr, ProcessCommand::WriteStdin { data });
    }

    fn handle_worker_line(&mut self, ctx: &Ctx, line: &str) {
        let Ok(val) = serde_json::from_str::<serde_json::Value>(line) else {
            if !line.is_empty() {
                eprintln!("pp-stage-0 worker: {line}");
            }
            return;
        };

        if let Some(pid) = parse_status_line(&val) {
            self.ready = true;
            if let Some(addr) = self.status_addr {
                let _ = ctx.send(addr, StageActorStatus::WorkerReady { pid });
            }
            return;
        }

        if let Some(err) = val.get("error").and_then(|v| v.as_str()) {
            eprintln!("pp-stage-0 worker error: {err}");
            if let Some(rid) = val.get("request_id").and_then(|v| v.as_u64()) {
                self.pending_worker.remove(&rid);
                self.pending_tokenize.remove(&rid);
            }
            return;
        }

        // Tokenize reply (real-mode path): `{"request_id": rid, "tokens": [..]}`.
        // No `hidden_b64`. Convert tokens into the follow-up `embed_and_forward`
        // request, re-using the forward rid stashed when we sent `tokenize`.
        if let (Some(rid), Some(tokens_val), None) = (
            val.get("request_id").and_then(|v| v.as_u64()),
            val.get("tokens").and_then(|v| v.as_array()),
            val.get("hidden_b64"),
        ) {
            if let Some(pending) = self.pending_tokenize.remove(&rid) {
                let tokens: Vec<i64> =
                    tokens_val.iter().filter_map(|v| v.as_i64()).collect();
                if tokens.is_empty() {
                    eprintln!(
                        "pp-stage-0: tokenize reply for rid {rid} had no usable token ids"
                    );
                    return;
                }
                let forward_rid = pending.forward_request_id;
                self.pending_worker.insert(
                    forward_rid,
                    Stage0Pending {
                        request_id: forward_rid,
                        position: 0,
                        is_prefill: true,
                    },
                );
                self.write_to_worker(
                    ctx,
                    serde_json::json!({
                        "op": "embed_and_forward",
                        "request_id": forward_rid,
                        "tokens": tokens,
                        "position": 0,
                    }),
                );
                return;
            }
        }

        let (Some(rid), Some(hidden_b64), Some(seq_len)) = (
            val.get("request_id").and_then(|v| v.as_u64()),
            val.get("hidden_b64").and_then(|v| v.as_str()),
            val.get("seq_len").and_then(|v| v.as_u64()),
        ) else {
            return;
        };
        let Some(pending) = self.pending_worker.remove(&rid) else {
            return;
        };
        let hidden = match B64.decode(hidden_b64) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("pp-stage-0: base64 decode error for rid {rid}: {e}");
                return;
            }
        };
        let _ = ctx.send(
            self.next_stage_addr,
            StageActivation {
                request_id: pending.request_id,
                position: pending.position,
                hidden,
                seq_len: seq_len as u32,
                is_prefill: pending.is_prefill,
            },
        );
    }
}

impl ActorInterface for Stage0Actor {
    type Incoming = Stage0Msg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let proc_addr = spawn_local_process(ctx, &self.sender, self.spec.clone())
            .expect("failed to spawn stage-0 worker process");
        let bridge = Stage0ProcessBridge { target: ctx.self_addr() };
        let bridge_addr = ctx.spawn(bridge).expect("failed to spawn stage-0 process bridge");
        let _ = ctx.send(proc_addr, ProcessCommand::Subscribe { address: bridge_addr });
        self.process_addr = Some(proc_addr);
        self.bridge_addr = Some(bridge_addr);
    }

    fn handle(&mut self, ctx: &Ctx, msg: Stage0Msg) {
        match msg {
            Stage0Msg::Inference(req) => {
                if !self.ready || !self.process_alive {
                    return;
                }
                if self.tokenize_via_worker {
                    // Real-mode path: ask the worker to tokenize. The
                    // follow-up embed_and_forward is issued from
                    // handle_worker_line when the tokenize reply arrives.
                    let tokenize_rid = self.next_request_id;
                    self.next_request_id += 1;
                    let forward_rid = self.next_request_id;
                    self.next_request_id += 1;
                    self.pending_tokenize.insert(
                        tokenize_rid,
                        Stage0Tokenize { forward_request_id: forward_rid },
                    );
                    self.write_to_worker(
                        ctx,
                        serde_json::json!({
                            "op": "tokenize",
                            "request_id": tokenize_rid,
                            "prompt": req.prompt,
                        }),
                    );
                    return;
                }
                let tokens = stub_tokenize_prompt(&req.prompt);
                if tokens.is_empty() {
                    return;
                }
                let rid = self.next_request_id;
                self.next_request_id += 1;
                let position = 0u32;
                self.pending_worker.insert(
                    rid,
                    Stage0Pending { request_id: rid, position, is_prefill: true },
                );
                self.write_to_worker(
                    ctx,
                    serde_json::json!({
                        "op": "embed_and_forward",
                        "request_id": rid,
                        "tokens": tokens,
                        "position": position,
                    }),
                );
            }
            Stage0Msg::NextToken(nt) => {
                if nt.done {
                    return;
                }
                if !self.ready || !self.process_alive {
                    return;
                }
                self.pending_worker.insert(
                    nt.request_id,
                    Stage0Pending {
                        request_id: nt.request_id,
                        position: nt.position,
                        is_prefill: false,
                    },
                );
                self.write_to_worker(
                    ctx,
                    serde_json::json!({
                        "op": "decode_step",
                        "request_id": nt.request_id,
                        "token_id": nt.token_id,
                        "position": nt.position,
                    }),
                );
            }
            Stage0Msg::SetNextStage(addr) => {
                self.next_stage_addr = addr;
            }
            Stage0Msg::Reset => {
                self.pending_worker.clear();
                self.pending_tokenize.clear();
            }
            Stage0Msg::Process(notif) => match notif {
                ProcessNotification::Started { .. } => {
                    self.process_alive = true;
                    if let Some(addr) = self.status_addr {
                        let _ = ctx.send(addr, StageActorStatus::ProcessStarted);
                    }
                }
                ProcessNotification::Output { data, .. } => {
                    let text = String::from_utf8_lossy(&data);
                    self.output_buffer.push_str(&text);
                    while let Some(pos) = self.output_buffer.find('\n') {
                        let line = self.output_buffer[..pos].to_string();
                        self.output_buffer = self.output_buffer[pos + 1..].to_string();
                        self.handle_worker_line(ctx, line.trim());
                    }
                }
                ProcessNotification::Exited { status, .. } => {
                    self.process_alive = false;
                    self.ready = false;
                    self.pending_worker.clear();
                    self.pending_tokenize.clear();
                    if let Some(addr) = self.status_addr {
                        let _ = ctx.send(addr, StageActorStatus::ProcessExited { status });
                    }
                }
                ProcessNotification::Error { .. } => {
                    self.process_alive = false;
                    self.ready = false;
                }
            },
        }
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        if let Some(proc_addr) = self.process_addr {
            let _ = ctx.send(proc_addr, ProcessCommand::Close);
            let _ = ctx.stop_actor(proc_addr);
        }
        if let Some(bridge_addr) = self.bridge_addr {
            let _ = ctx.stop_actor(bridge_addr);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Stage 1
// ═══════════════════════════════════════════════════════════════════════

/// `SetNeighbors` is a one-shot setup message used by the `pp-gpu-node`
/// binary to inject the addresses of `pp-stage-0` (the prev stage) and
/// the orchestrator's `InferenceResponse` inbox after SWIM gossip
/// propagates them.
///
/// `Reset` clears the accumulated-token buffer and per-request bookkeeping
/// so the actor can serve a second prompt without being respawned. The
/// pp-smoke-run binary still treats every request as one-shot; this is
/// used by the equivalence tests to amortize worker boot across multiple
/// prompts.
#[derive(Clone, Debug)]
pub enum Stage1Msg {
    Activation(StageActivation),
    Process(ProcessNotification),
    SetNeighbors {
        prev_stage: ActorAddress,
        reply_to: ActorAddress,
    },
    Reset,
}

struct Stage1ProcessBridge {
    target: ActorAddress,
}

impl ActorInterface for Stage1ProcessBridge {
    type Incoming = ProcessNotification;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: ProcessNotification) {
        let _ = ctx.send(self.target, Stage1Msg::Process(msg));
    }
}

pub struct Stage1ActivationBridge {
    pub target: ActorAddress,
}

impl ActorInterface for Stage1ActivationBridge {
    type Incoming = StageActivation;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: StageActivation) {
        let _ = ctx.send(self.target, Stage1Msg::Activation(msg));
    }
}

struct Stage1Pending {
    request_id: u64,
    /// Position the worker forwarded at, i.e. `activation.position`. The
    /// next decode step's position is `position + seq_len`.
    next_position: u32,
}

pub struct Stage1Actor {
    spec: ProcessSpec,
    sender: ExternalSender,
    prev_stage_addr: ActorAddress,
    reply_to: ActorAddress,
    max_tokens: u32,
    eos_token_id: Option<u32>,
    status_addr: Option<ActorAddress>,
    /// Optional address that receives a copy of every `NextToken` the
    /// actor emits (in addition to the regular `prev_stage_addr` send).
    /// Used by tests to inspect the sampled token stream without
    /// intercepting the actor-to-actor decode loop.
    token_observer: Option<ActorAddress>,

    process_addr: Option<ActorAddress>,
    bridge_addr: Option<ActorAddress>,
    ready: bool,
    process_alive: bool,
    output_buffer: String,

    pending_worker: HashMap<u64, Stage1Pending>,
    accumulated: Vec<u32>,
    finished: bool,

    /// When true, route the final detokenization through the worker's
    /// `detokenize` op (mirrors `Stage0Actor::tokenize_via_worker`).
    /// Required for real-mode workers so the response is human-readable
    /// text instead of a stringified id array.
    detokenize_via_worker: bool,
    next_request_id: u64,
    /// In-flight detokenize round-trip. We only ever have one terminal
    /// detok per request, so a single `Option` suffices.
    pending_detokenize: Option<u64>,
}

impl Stage1Actor {
    pub fn new(
        spec: ProcessSpec,
        sender: ExternalSender,
        prev_stage_addr: ActorAddress,
        reply_to: ActorAddress,
        max_tokens: u32,
    ) -> Self {
        Self {
            spec,
            sender,
            prev_stage_addr,
            reply_to,
            max_tokens,
            eos_token_id: None,
            status_addr: None,
            token_observer: None,
            process_addr: None,
            bridge_addr: None,
            ready: false,
            process_alive: false,
            output_buffer: String::new(),
            pending_worker: HashMap::new(),
            accumulated: Vec::new(),
            finished: false,
            detokenize_via_worker: false,
            next_request_id: 1,
            pending_detokenize: None,
        }
    }

    pub fn with_status_addr(mut self, addr: ActorAddress) -> Self {
        self.status_addr = Some(addr);
        self
    }

    /// Route the final detokenization through the worker's `detokenize`
    /// op instead of the in-actor stub. Required for real-mode workers so
    /// the `InferenceResponse.text` is human-readable model output rather
    /// than a stringified id array.
    pub fn with_real_detokenization(mut self) -> Self {
        self.detokenize_via_worker = true;
        self
    }

    /// Configure an EOS token id. When the sampled token matches, the
    /// decode loop terminates: `NextToken { done: true }` is sent to the
    /// previous stage and `InferenceResponse` is sent to `reply_to`.
    pub fn with_eos_token_id(mut self, eos: u32) -> Self {
        self.eos_token_id = Some(eos);
        self
    }

    /// Configure an observer that receives a clone of every `NextToken`
    /// produced by this actor. The observer is independent of the prev-
    /// stage route; the actor still sends `NextToken` to `prev_stage_addr`
    /// to drive the decode loop. Intended for tests that want to inspect
    /// the sampled token sequence directly.
    pub fn with_token_observer(mut self, addr: ActorAddress) -> Self {
        self.token_observer = Some(addr);
        self
    }

    fn write_to_worker(&self, ctx: &Ctx, json: serde_json::Value) {
        let Some(proc_addr) = self.process_addr else {
            return;
        };
        let mut data = serde_json::to_vec(&json).unwrap_or_default();
        data.push(b'\n');
        let _ = ctx.send(proc_addr, ProcessCommand::WriteStdin { data });
    }

    fn detokenize_stub(tokens: &[u32]) -> String {
        // No real tokenizer in stub mode. Producing a deterministic,
        // human-readable, *non-empty* string is the only contract the
        // actor tests rely on.
        let body = tokens
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        format!("tokens: [{body}]")
    }

    fn handle_worker_line(&mut self, ctx: &Ctx, line: &str) {
        let Ok(val) = serde_json::from_str::<serde_json::Value>(line) else {
            if !line.is_empty() {
                eprintln!("pp-stage-1 worker: {line}");
            }
            return;
        };

        if let Some(pid) = parse_status_line(&val) {
            self.ready = true;
            if let Some(addr) = self.status_addr {
                let _ = ctx.send(addr, StageActorStatus::WorkerReady { pid });
            }
            return;
        }

        if let Some(err) = val.get("error").and_then(|v| v.as_str()) {
            eprintln!("pp-stage-1 worker error: {err}");
            if let Some(rid) = val.get("request_id").and_then(|v| v.as_u64()) {
                self.pending_worker.remove(&rid);
                if self.pending_detokenize == Some(rid) {
                    self.pending_detokenize = None;
                    let text = Self::detokenize_stub(&self.accumulated);
                    let _ = ctx.send(self.reply_to, InferenceResponse { text });
                }
            }
            return;
        }

        // Detokenize reply: { request_id, text } — the only worker reply
        // that carries a `text` field. Match it before the token-id reply
        // since both share `request_id`.
        if let (Some(rid), Some(text)) = (
            val.get("request_id").and_then(|v| v.as_u64()),
            val.get("text").and_then(|v| v.as_str()),
        ) {
            if self.pending_detokenize == Some(rid) {
                self.pending_detokenize = None;
                let _ = ctx.send(
                    self.reply_to,
                    InferenceResponse { text: text.to_string() },
                );
                return;
            }
        }

        let (Some(rid), Some(token_id)) = (
            val.get("request_id").and_then(|v| v.as_u64()),
            val.get("token_id").and_then(|v| v.as_u64()),
        ) else {
            return;
        };
        let Some(pending) = self.pending_worker.remove(&rid) else {
            return;
        };
        if self.finished {
            return;
        }

        let token_id = token_id as u32;
        self.accumulated.push(token_id);

        let hit_eos = self.eos_token_id == Some(token_id);
        let hit_cap = self.accumulated.len() as u32 >= self.max_tokens;
        let done = hit_eos || hit_cap;

        let next_token = NextToken {
            request_id: pending.request_id,
            token_id,
            position: pending.next_position,
            done,
        };
        let _ = ctx.send(self.prev_stage_addr, next_token.clone());
        if let Some(observer) = self.token_observer {
            let _ = ctx.send(observer, next_token);
        }

        if done {
            self.finished = true;
            if self.detokenize_via_worker {
                let detok_rid = self.next_request_id;
                self.next_request_id += 1;
                self.pending_detokenize = Some(detok_rid);
                let tokens_json: Vec<serde_json::Value> = self
                    .accumulated
                    .iter()
                    .map(|t| serde_json::Value::from(*t))
                    .collect();
                self.write_to_worker(
                    ctx,
                    serde_json::json!({
                        "op": "detokenize",
                        "request_id": detok_rid,
                        "tokens": tokens_json,
                    }),
                );
            } else {
                let text = Self::detokenize_stub(&self.accumulated);
                let _ = ctx.send(self.reply_to, InferenceResponse { text });
            }
        }
    }
}

impl ActorInterface for Stage1Actor {
    type Incoming = Stage1Msg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let proc_addr = spawn_local_process(ctx, &self.sender, self.spec.clone())
            .expect("failed to spawn stage-1 worker process");
        let bridge = Stage1ProcessBridge { target: ctx.self_addr() };
        let bridge_addr = ctx.spawn(bridge).expect("failed to spawn stage-1 process bridge");
        let _ = ctx.send(proc_addr, ProcessCommand::Subscribe { address: bridge_addr });
        self.process_addr = Some(proc_addr);
        self.bridge_addr = Some(bridge_addr);
    }

    fn handle(&mut self, ctx: &Ctx, msg: Stage1Msg) {
        match msg {
            Stage1Msg::Activation(act) => {
                if !self.ready || !self.process_alive || self.finished {
                    return;
                }
                let next_position = act.position.saturating_add(act.seq_len);
                self.pending_worker.insert(
                    act.request_id,
                    Stage1Pending { request_id: act.request_id, next_position },
                );
                let hidden_b64 = B64.encode(&act.hidden);
                self.write_to_worker(
                    ctx,
                    serde_json::json!({
                        "op": "forward_and_sample",
                        "request_id": act.request_id,
                        "hidden_b64": hidden_b64,
                        "position": act.position,
                        "seq_len": act.seq_len,
                    }),
                );
            }
            Stage1Msg::SetNeighbors {
                prev_stage,
                reply_to,
            } => {
                self.prev_stage_addr = prev_stage;
                self.reply_to = reply_to;
            }
            Stage1Msg::Reset => {
                self.accumulated.clear();
                self.finished = false;
                self.pending_worker.clear();
            }
            Stage1Msg::Process(notif) => match notif {
                ProcessNotification::Started { .. } => {
                    self.process_alive = true;
                    if let Some(addr) = self.status_addr {
                        let _ = ctx.send(addr, StageActorStatus::ProcessStarted);
                    }
                }
                ProcessNotification::Output { data, .. } => {
                    let text = String::from_utf8_lossy(&data);
                    self.output_buffer.push_str(&text);
                    while let Some(pos) = self.output_buffer.find('\n') {
                        let line = self.output_buffer[..pos].to_string();
                        self.output_buffer = self.output_buffer[pos + 1..].to_string();
                        self.handle_worker_line(ctx, line.trim());
                    }
                }
                ProcessNotification::Exited { status, .. } => {
                    self.process_alive = false;
                    self.ready = false;
                    self.pending_worker.clear();
                    if let Some(addr) = self.status_addr {
                        let _ = ctx.send(addr, StageActorStatus::ProcessExited { status });
                    }
                }
                ProcessNotification::Error { .. } => {
                    self.process_alive = false;
                    self.ready = false;
                }
            },
        }
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        if let Some(proc_addr) = self.process_addr {
            let _ = ctx.send(proc_addr, ProcessCommand::Close);
            let _ = ctx.stop_actor(proc_addr);
        }
        if let Some(bridge_addr) = self.bridge_addr {
            let _ = ctx.stop_actor(bridge_addr);
        }
    }
}
