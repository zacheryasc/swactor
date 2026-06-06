//! Stage actor — a single `StageActor` parameterised by `StageRole`.
//!
//! Each `StageActor` owns one Python worker subprocess (via
//! `swactor_process`) and translates pipeline messages into stdin JSON /
//! parses stdout JSON. The role is fixed at construction time and decides
//! which incoming messages produce outbound traffic; messages outside the
//! role's set are dropped without panic (defensive drops).
//!
//! `StageRole::First`:
//! * On `Inference`: tokenize the prompt (in-actor whitespace stub by
//!   default; worker `tokenize` op when configured for real mode), send
//!   `embed_and_forward` to the worker, forward the resulting hidden
//!   state to `next_stage_addr` as a `StageActivation { is_prefill: true }`.
//! * On `NextToken { done: false }`: send `decode_step` to the worker,
//!   forward the resulting hidden state as
//!   `StageActivation { is_prefill: false, seq_len: 1 }`.
//! * On `NextToken { done: true }`: no further activations.
//!
//! `StageRole::Middle`: on `Activation`, send `forward_range` to the worker
//! and forward the resulting hidden state to `next_stage_addr` as a fresh
//! `StageActivation` that echoes the inbound `request_id`, `position`,
//! `seq_len`, and `is_prefill`. Middle stages are stateless passes from the
//! orchestrator's perspective; only `hidden` changes across the hop.
//!
//! `StageRole::Last`:
//! * On `Activation`: send `forward_and_sample` to the worker, accumulate
//!   the sampled token, emit `NextToken` to `prev_stage_addr`, and emit
//!   `InferenceResponse` to `reply_to` when EOS or `max_tokens` is reached.
//!
//! The three bridges (`RequestBridge`, `NextTokenBridge`, `ActivationBridge`)
//! are thin adapters that wrap a single network message type into the
//! unified `StageMsg`; they exist because actor inboxes are typed per
//! message and the network arrives one type at a time.

use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;


use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::ExternalSender;
use swactor_process::{
    ExitStatus, OutputStream, ProcessCommand, ProcessNotification, ProcessSpec,
    spawn_local_process,
};

use crate::messages::{InferenceRequest, InferenceResponse, NextToken, StageActivation};

/// Per-process stderr ring buffer cap. Lines beyond this are dropped
/// from the front. Sized to capture a substantial Python traceback
/// plus pre-crash log context without bloating the diagnostic bundle.
pub const STDERR_TAIL_LINES: usize = 256;
/// Per-line truncation cap for the stderr ring buffer. Lines longer
/// than this are truncated at the byte boundary nearest the limit;
/// anything past is dropped silently.
pub const STDERR_LINE_BYTES: usize = 4 * 1024;

// ─── Stage role ───────────────────────────────────────────────────────────

/// Pipeline role of a stage. Derived once at boot from `(STAGE, NUM_STAGES)`
/// and never changes. The `StageActor` branches its per-message logic on
/// the role; the binary uses it to pick which neighbours to resolve.
/// `Middle` only exists for `N >= 3` — a 2-stage chain has a `First` and a
/// `Last` and nothing in between.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StageRole {
    /// `stage == 0`. Owns prompt entry, tokenizer, embed + first range.
    First,
    /// `0 < stage < num_stages - 1`. Owns a middle block range; no
    /// tokenizer, no sampler. Requires `num_stages >= 3`.
    Middle,
    /// `stage == num_stages - 1`. Owns last block range, output head,
    /// sampler, detokenizer.
    Last,
}

impl StageRole {
    /// Classify a stage by `(stage, num_stages)`. `num_stages < 2` or
    /// `stage >= num_stages` is a programmer error; this function panics
    /// rather than silently producing a wrong role, because the binary
    /// validates the env vars before ever calling it.
    pub fn for_stage(stage: u32, num_stages: u32) -> StageRole {
        assert!(
            num_stages >= 2,
            "StageRole::for_stage requires num_stages >= 2 (got {num_stages}); \
             N=1 is not supported by this example",
        );
        assert!(
            stage < num_stages,
            "StageRole::for_stage requires stage < num_stages (got stage={stage}, num_stages={num_stages})",
        );
        if stage == 0 {
            StageRole::First
        } else if stage == num_stages - 1 {
            StageRole::Last
        } else {
            StageRole::Middle
        }
    }
}

// ─── Status / message types ───────────────────────────────────────────────

/// Lifecycle notifications emitted to an optional observer address. Same
/// shape for every role so tests share their startup helpers.
#[derive(Clone, Debug)]
pub enum StageActorStatus {
    ProcessStarted,
    WorkerReady { pid: Option<u32> },
    ProcessExited { status: ExitStatus },
}

/// Union of messages the `StageActor` accepts. Network arrivals
/// (`InferenceRequest`, `NextToken`, `StageActivation`) are adapted into
/// this enum by the bridge actors; `Process` is adapted from the worker
/// subprocess; `SetNeighbors` and `Reset` are control messages.
///
/// `SetNeighbors` is a one-shot setup message used by the `pp-worker`
/// binary to inject the resolved addresses of neighbouring stages and the
/// orchestrator after SWIM gossip has propagated them. Each field is
/// optional; only the fields relevant to the actor's role need to be set.
///
/// `Reset` clears all per-request state so a single long-lived pipeline
/// can serve multiple prompts back-to-back.
#[derive(Clone, Debug)]
pub enum StageMsg {
    Inference(InferenceRequest),
    NextToken(NextToken),
    Activation(StageActivation),
    Process(ProcessNotification),
    SetNeighbors {
        prev_stage: Option<ActorAddress>,
        next_stage: Option<ActorAddress>,
        reply_to: Option<ActorAddress>,
    },
    Reset,
    /// Tear down the running worker subprocess and spawn a fresh one,
    /// re-exec'ing the on-disk worker script so an edited
    /// `pp_tinygrad_worker.py` is picked up without restarting the node.
    /// Driven by a `SIGHUP` to `pp-worker`.
    ReloadWorker,
}

// ─── Bridges (one per inbound network message type) ───────────────────────

/// Routes inbound `InferenceRequest` messages from the network into a
/// `StageActor`. Conventionally only wired on a `StageRole::First` node;
/// targeting a non-First actor is harmless — the actor drops it
/// defensively.
pub struct RequestBridge {
    pub target: ActorAddress,
}

impl ActorInterface for RequestBridge {
    type Incoming = InferenceRequest;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: InferenceRequest) {
        let _ = ctx.send(self.target, StageMsg::Inference(msg));
    }
}

/// Routes inbound `NextToken` messages from the network into a
/// `StageActor`. Conventionally only wired on a `StageRole::First` node.
pub struct NextTokenBridge {
    pub target: ActorAddress,
}

impl ActorInterface for NextTokenBridge {
    type Incoming = NextToken;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: NextToken) {
        let _ = ctx.send(self.target, StageMsg::NextToken(msg));
    }
}

/// Routes inbound `StageActivation` messages from the network into a
/// `StageActor`. Wired on `Middle` and `Last` roles — anything downstream
/// of the first stage in the chain.
pub struct ActivationBridge {
    pub target: ActorAddress,
}

impl ActorInterface for ActivationBridge {
    type Incoming = StageActivation;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: StageActivation) {
        let _ = ctx.send(self.target, StageMsg::Activation(msg));
    }
}

/// Adapts `ProcessNotification`s from the worker subprocess into the
/// actor's `StageMsg::Process` variant. Internal — the binary and tests
/// never construct it directly.
struct ProcessBridge {
    target: ActorAddress,
}

impl ActorInterface for ProcessBridge {
    type Incoming = ProcessNotification;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: ProcessNotification) {
        let _ = ctx.send(self.target, StageMsg::Process(msg));
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────

/// Tokenize a prompt for the stub worker. Splits on whitespace. The token
/// count returned here is exactly the `seq_len` the actor will emit in the
/// resulting `StageActivation`, so callers (including tests) can predict
/// it directly from the input string.
pub fn stub_tokenize_prompt(prompt: &str) -> Vec<i64> {
    prompt
        .split_whitespace()
        .enumerate()
        .map(|(i, word)| {
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

// ─── Stage actor ──────────────────────────────────────────────────────────

const PLACEHOLDER_ADDR: ActorAddress = ActorAddress([0; 32]);

/// Pending forward request (First or Middle): one entry per in-flight
/// `embed_and_forward` / `decode_step` / `forward_range` round-trip. The
/// fields are echoed back on the resulting `StageActivation`.
struct FwdPending {
    request_id: u64,
    position: u32,
    is_prefill: bool,
    seq_len: u32,
}

/// Pending tokenize round-trip (First only). When the worker replies with
/// the token ids the actor uses `forward_request_id` as the rid for the
/// follow-up `embed_and_forward` request.
struct FirstTokenize {
    forward_request_id: u64,
}

/// Pending sample round-trip (Last only).
struct LastPending {
    request_id: u64,
    /// Position the worker forwarded at, i.e. `activation.position`. The
    /// next decode step's position is `position + seq_len`.
    next_position: u32,
}

/// A single stage actor. The role is fixed at construction and chooses
/// which incoming messages are acted on; out-of-role messages are dropped
/// silently.
///
/// Routing addresses (`prev_stage_addr`, `next_stage_addr`, `reply_to`)
/// default to a sentinel and are typically populated post-spawn via
/// `StageMsg::SetNeighbors`. Sending an outbound message to the sentinel
/// is harmless on localhost (the transport router has no route for it and
/// drops the send); in deployments the binary refuses to enter the main
/// pump loop until every role-required neighbour has been resolved.
pub struct StageActor {
    role: StageRole,
    spec: ProcessSpec,
    sender: ExternalSender,

    prev_stage_addr: ActorAddress,
    next_stage_addr: ActorAddress,
    reply_to: ActorAddress,

    status_addr: Option<ActorAddress>,
    /// Optional address that receives a clone of every `NextToken` the
    /// actor emits (in addition to the regular `prev_stage_addr` send).
    /// Used by tests to inspect the sampled token stream without
    /// intercepting the actor-to-actor decode loop. Last-only.
    token_observer: Option<ActorAddress>,

    max_tokens: u32,
    eos_token_id: Option<u32>,
    /// Route tokenization through the worker's `tokenize` op. Required
    /// for real-mode First workers (synthetic stub ids cannot be embedded
    /// against a real GGUF vocabulary).
    tokenize_via_worker: bool,
    /// Route final detokenization through the worker's `detokenize` op so
    /// the response is human-readable text. Real-mode Last only.
    detokenize_via_worker: bool,

    process_addr: Option<ActorAddress>,
    bridge_addr: Option<ActorAddress>,
    ready: bool,
    process_alive: bool,
    output_buffer: String,
    next_request_id: u64,

    pending_fwd: HashMap<u64, FwdPending>,
    pending_first_tokenize: HashMap<u64, FirstTokenize>,
    pending_last: HashMap<u64, LastPending>,
    pending_last_detokenize: Option<u64>,
    accumulated: Vec<u32>,
    finished: bool,

    /// Bounded ring of recent stderr lines from the worker subprocess.
    /// Drained into `Custom("worker_exited")` on exit.
    stderr_tail: VecDeque<String>,
    /// In-progress stderr line being assembled across multiple
    /// `ProcessNotification::Output { is_stderr: true }` chunks. Pushed
    /// to `stderr_tail` on newline.
    stderr_buf: String,
    /// Most recent worker uncaught_exception traceback (if any). Stashed
    /// here so the eventual `worker_exited` event can carry it even if
    /// the event stream truncates the per-line event.
    last_python_traceback: Option<String>,
    /// Wall-clock at the moment the worker subprocess started — used
    /// to compute `uptime_ms` on the worker_exited event.
    process_started_at: Option<Instant>,
    /// OS PID of the spawned worker, learned from
    /// `ProcessNotification::Started`. Stored so the matching
    /// `SubprocessExited` event can carry it and the introspector
    /// can be told which entry to mark exited
    /// (`N3_OBSERVABILITY_UPGRADE_SPEC.md` §4 wiring contract).
    worker_pid: Option<u32>,
}

impl StageActor {
    fn empty(role: StageRole, spec: ProcessSpec, sender: ExternalSender) -> Self {
        Self {
            role,
            spec,
            sender,
            prev_stage_addr: PLACEHOLDER_ADDR,
            next_stage_addr: PLACEHOLDER_ADDR,
            reply_to: PLACEHOLDER_ADDR,
            status_addr: None,
            token_observer: None,
            max_tokens: 0,
            eos_token_id: None,
            tokenize_via_worker: false,
            detokenize_via_worker: false,
            process_addr: None,
            bridge_addr: None,
            ready: false,
            process_alive: false,
            output_buffer: String::new(),
            next_request_id: 1,
            pending_fwd: HashMap::new(),
            pending_first_tokenize: HashMap::new(),
            pending_last: HashMap::new(),
            pending_last_detokenize: None,
            accumulated: Vec::new(),
            finished: false,
            stderr_tail: VecDeque::with_capacity(STDERR_TAIL_LINES),
            stderr_buf: String::new(),
            last_python_traceback: None,
            process_started_at: None,
            worker_pid: None,
        }
    }

    /// Build a first-stage (`StageRole::First`) actor. `next_stage_addr`
    /// is the address that outbound `StageActivation`s are sent to —
    /// usually the next stage's `ActivationBridge`. Wire the real address
    /// post-spawn via `StageMsg::SetNeighbors { next_stage: Some(_), .. }`
    /// when the resolved address is not known at construction time.
    pub fn first(
        spec: ProcessSpec,
        sender: ExternalSender,
        next_stage_addr: ActorAddress,
    ) -> Self {
        let mut a = Self::empty(StageRole::First, spec, sender);
        a.next_stage_addr = next_stage_addr;
        a
    }

    /// Build a middle-stage (`StageRole::Middle`) actor. The Middle role's
    /// `forward_range` behaviour itself lands at Stage 4; at Stage 3 the
    /// actor is constructible and defensively drops every incoming
    /// pipeline message. `next_stage_addr` is the address activations are
    /// forwarded to once Stage 4's behaviour is in place.
    pub fn middle(
        spec: ProcessSpec,
        sender: ExternalSender,
        next_stage_addr: ActorAddress,
    ) -> Self {
        let mut a = Self::empty(StageRole::Middle, spec, sender);
        a.next_stage_addr = next_stage_addr;
        a
    }

    /// Build a last-stage (`StageRole::Last`) actor. `prev_stage_addr` is
    /// the address `NextToken`s are sent to (the first stage's
    /// `NextTokenBridge`). `reply_to` is the orchestrator's inbox for the
    /// final `InferenceResponse`. `max_tokens` caps the decode loop.
    pub fn last(
        spec: ProcessSpec,
        sender: ExternalSender,
        prev_stage_addr: ActorAddress,
        reply_to: ActorAddress,
        max_tokens: u32,
    ) -> Self {
        let mut a = Self::empty(StageRole::Last, spec, sender);
        a.prev_stage_addr = prev_stage_addr;
        a.reply_to = reply_to;
        a.max_tokens = max_tokens;
        a
    }

    /// The role this actor was constructed for. Immutable post-construction.
    pub fn role(&self) -> StageRole {
        self.role
    }

    pub fn with_status_addr(mut self, addr: ActorAddress) -> Self {
        self.status_addr = Some(addr);
        self
    }

    fn push_stderr_line(&mut self, mut line: String) {
        if line.len() > STDERR_LINE_BYTES {
            // Truncate at a UTF-8 char boundary at or below the cap.
            let mut end = STDERR_LINE_BYTES;
            while !line.is_char_boundary(end) {
                end -= 1;
            }
            line.truncate(end);
        }
        if self.stderr_tail.len() >= STDERR_TAIL_LINES {
            self.stderr_tail.pop_front();
        }
        self.stderr_tail.push_back(line);
    }

    fn drain_stderr_tail(&mut self) -> Vec<String> {
        let lines: Vec<String> = self.stderr_tail.drain(..).collect();
        if !self.stderr_buf.is_empty() {
            // Surface any unterminated trailing fragment so a crash that
            // truncates mid-line still produces visible bytes.
            let frag = std::mem::take(&mut self.stderr_buf);
            let mut out = lines;
            out.push(frag);
            out
        } else {
            lines
        }
    }

    /// Route prompt tokenization through the worker's `tokenize` op. Use
    /// this with real-mode First workers; the default (stub) path bypasses
    /// the worker and uses an in-actor whitespace splitter, which produces
    /// synthetic ids that real GGUF vocabularies cannot embed.
    pub fn with_real_tokenization(mut self) -> Self {
        assert_eq!(
            self.role,
            StageRole::First,
            "with_real_tokenization only applies to StageRole::First",
        );
        self.tokenize_via_worker = true;
        self
    }

    /// Route the final detokenization through the worker's `detokenize`
    /// op instead of the in-actor stub. Required for real-mode Last
    /// workers so the `InferenceResponse.text` is human-readable model
    /// output rather than a stringified id array.
    pub fn with_real_detokenization(mut self) -> Self {
        assert_eq!(
            self.role,
            StageRole::Last,
            "with_real_detokenization only applies to StageRole::Last",
        );
        self.detokenize_via_worker = true;
        self
    }

    /// Configure an EOS token id. When the sampled token matches, the
    /// decode loop terminates: `NextToken { done: true }` is sent to the
    /// previous stage and `InferenceResponse` is sent to `reply_to`.
    /// Last-only.
    pub fn with_eos_token_id(mut self, eos: u32) -> Self {
        assert_eq!(
            self.role,
            StageRole::Last,
            "with_eos_token_id only applies to StageRole::Last",
        );
        self.eos_token_id = Some(eos);
        self
    }

    /// Configure an observer that receives a clone of every `NextToken`
    /// produced by this actor. Last-only. The observer is independent of
    /// the prev-stage route; the actor still sends `NextToken` to
    /// `prev_stage_addr` to drive the decode loop. Intended for tests
    /// that want to inspect the sampled token sequence directly.
    pub fn with_token_observer(mut self, addr: ActorAddress) -> Self {
        assert_eq!(
            self.role,
            StageRole::Last,
            "with_token_observer only applies to StageRole::Last",
        );
        self.token_observer = Some(addr);
        self
    }

    fn label(&self) -> &'static str {
        match self.role {
            StageRole::First => "pp-stage(first)",
            StageRole::Middle => "pp-stage(middle)",
            StageRole::Last => "pp-stage(last)",
        }
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

    fn clear_pending_state(&mut self) {
        self.pending_fwd.clear();
        self.pending_first_tokenize.clear();
        self.pending_last.clear();
        self.pending_last_detokenize = None;
    }

    fn handle_worker_line(&mut self, ctx: &Ctx, line: &str) {
        let Ok(val) = serde_json::from_str::<serde_json::Value>(line) else {
            if !line.is_empty() {
                eprintln!("{} worker: {line}", self.label());
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

        // Worker-side lifecycle event: any `{"event": "<kind>", ...}` line
        // already reaches the datastream as `proc.<label>.stdout` text via
        // the runtime's process-output observer, so nothing is re-emitted
        // here. We only stash the traceback off `uncaught_exception` so the
        // abnormal-exit stderr mirror can carry it even if the per-line
        // event is truncated.
        if let Some(event_kind) = val.get("event").and_then(|v| v.as_str()) {
            if event_kind == "uncaught_exception" {
                if let Some(tb) = val.get("traceback").and_then(|v| v.as_str()) {
                    self.last_python_traceback = Some(tb.to_string());
                }
            }
            return;
        }

        if let Some(err) = val.get("error").and_then(|v| v.as_str()) {
            eprintln!("{} worker error: {err}", self.label());
            if let Some(rid) = val.get("request_id").and_then(|v| v.as_u64()) {
                self.pending_fwd.remove(&rid);
                self.pending_first_tokenize.remove(&rid);
                self.pending_last.remove(&rid);
                if self.pending_last_detokenize == Some(rid) {
                    self.pending_last_detokenize = None;
                    // Best-effort stub response so the orchestrator is
                    // not left hanging after a detok error.
                    let text = Self::detokenize_stub(&self.accumulated);
                    let _ = ctx.send(self.reply_to, InferenceResponse { text });
                }
            }
            return;
        }

        // Tokenize reply (First only): request_id + tokens, no hidden_b64.
        if let (Some(rid), Some(tokens_val), None) = (
            val.get("request_id").and_then(|v| v.as_u64()),
            val.get("tokens").and_then(|v| v.as_array()),
            val.get("hidden_b64"),
        ) {
            if let Some(pending) = self.pending_first_tokenize.remove(&rid) {
                let tokens: Vec<i64> =
                    tokens_val.iter().filter_map(|v| v.as_i64()).collect();
                if tokens.is_empty() {
                    eprintln!(
                        "{}: tokenize reply for rid {rid} had no usable token ids",
                        self.label()
                    );
                    return;
                }
                let forward_rid = pending.forward_request_id;
                let seq_len = tokens.len() as u32;
                self.pending_fwd.insert(
                    forward_rid,
                    FwdPending {
                        request_id: forward_rid,
                        position: 0,
                        is_prefill: true,
                        seq_len,
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

        // Detokenize reply (Last only): request_id + text.
        if let (Some(rid), Some(text)) = (
            val.get("request_id").and_then(|v| v.as_u64()),
            val.get("text").and_then(|v| v.as_str()),
        ) {
            if self.pending_last_detokenize == Some(rid) {
                self.pending_last_detokenize = None;
                let _ = ctx.send(
                    self.reply_to,
                    InferenceResponse {
                        text: text.to_string(),
                    },
                );
                return;
            }
        }

        // Forward reply (First and Middle): request_id + hidden_b64 + seq_len.
        if let (Some(rid), Some(hidden_b64), Some(reply_seq_len)) = (
            val.get("request_id").and_then(|v| v.as_u64()),
            val.get("hidden_b64").and_then(|v| v.as_str()),
            val.get("seq_len").and_then(|v| v.as_u64()),
        ) {
            if let Some(pending) = self.pending_fwd.remove(&rid) {
                let hidden = match B64.decode(hidden_b64) {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!(
                            "{}: base64 decode error for rid {rid}: {e}",
                            self.label()
                        );
                        return;
                    }
                };
                let _ = ctx.send(
                    self.next_stage_addr,
                    StageActivation {
                        request_id: pending.request_id,
                        position: pending.position,
                        hidden,
                        seq_len: reply_seq_len as u32,
                        is_prefill: pending.is_prefill,
                    },
                );
                let _ = pending.seq_len; // pending.seq_len kept for symmetry / future asserts
                return;
            }
        }

        // Sample reply (Last only): request_id + token_id.
        if let (Some(rid), Some(token_id)) = (
            val.get("request_id").and_then(|v| v.as_u64()),
            val.get("token_id").and_then(|v| v.as_u64()),
        ) {
            let Some(pending) = self.pending_last.remove(&rid) else {
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
                    self.pending_last_detokenize = Some(detok_rid);
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

    fn handle_inference(&mut self, ctx: &Ctx, req: InferenceRequest) {
        if self.role != StageRole::First {
            return;
        }
        if !self.ready || !self.process_alive {
            return;
        }
        if self.tokenize_via_worker {
            let tokenize_rid = self.next_request_id;
            self.next_request_id += 1;
            let forward_rid = self.next_request_id;
            self.next_request_id += 1;
            self.pending_first_tokenize.insert(
                tokenize_rid,
                FirstTokenize {
                    forward_request_id: forward_rid,
                },
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
        let seq_len = tokens.len() as u32;
        self.pending_fwd.insert(
            rid,
            FwdPending {
                request_id: rid,
                position: 0,
                is_prefill: true,
                seq_len,
            },
        );
        self.write_to_worker(
            ctx,
            serde_json::json!({
                "op": "embed_and_forward",
                "request_id": rid,
                "tokens": tokens,
                "position": 0,
            }),
        );
    }

    fn handle_next_token(&mut self, ctx: &Ctx, nt: NextToken) {
        if self.role != StageRole::First {
            return;
        }
        if nt.done {
            return;
        }
        if !self.ready || !self.process_alive {
            return;
        }
        self.pending_fwd.insert(
            nt.request_id,
            FwdPending {
                request_id: nt.request_id,
                position: nt.position,
                is_prefill: false,
                seq_len: 1,
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

    fn handle_activation(&mut self, ctx: &Ctx, act: StageActivation) {
        match self.role {
            // First never accepts activations — they flow forward, not back
            // to the entry stage. Drop without side-effect.
            StageRole::First => {}
            StageRole::Middle => {
                if !self.ready || !self.process_alive {
                    return;
                }
                // Echo the inbound control fields onto the resulting
                // activation. The worker round-trip only transforms
                // `hidden`; `request_id`, `position`, `seq_len`, and
                // `is_prefill` must pass through unchanged so that
                // (a) the orchestrator can match the eventual
                // `InferenceResponse` to the right request, and
                // (b) Last computes the next decode position from the
                // same `(position, seq_len)` pair the chain has been
                // carrying since First.
                self.pending_fwd.insert(
                    act.request_id,
                    FwdPending {
                        request_id: act.request_id,
                        position: act.position,
                        is_prefill: act.is_prefill,
                        seq_len: act.seq_len,
                    },
                );
                let hidden_b64 = B64.encode(&act.hidden);
                self.write_to_worker(
                    ctx,
                    serde_json::json!({
                        "op": "forward_range",
                        "request_id": act.request_id,
                        "hidden_b64": hidden_b64,
                        "position": act.position,
                        "seq_len": act.seq_len,
                    }),
                );
            }
            StageRole::Last => {
                if !self.ready || !self.process_alive || self.finished {
                    return;
                }
                let next_position = act.position.saturating_add(act.seq_len);
                self.pending_last.insert(
                    act.request_id,
                    LastPending {
                        request_id: act.request_id,
                        next_position,
                    },
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
        }
    }

    fn handle_reset(&mut self) {
        self.clear_pending_state();
        self.accumulated.clear();
        self.finished = false;
    }

    /// Spawn the worker subprocess and its notification bridge, recording
    /// their addresses. Shared by `on_start` and `handle_reload_worker`.
    fn spawn_worker(&mut self, ctx: &Ctx) {
        let proc_addr = spawn_local_process(ctx, &self.sender, self.spec.clone())
            .expect("failed to spawn stage worker process");
        let bridge = ProcessBridge {
            target: ctx.self_addr(),
        };
        let bridge_addr = ctx
            .spawn(bridge)
            .expect("failed to spawn stage process bridge");
        let _ = ctx.send(
            proc_addr,
            ProcessCommand::Subscribe {
                address: bridge_addr,
            },
        );
        self.process_addr = Some(proc_addr);
        self.bridge_addr = Some(bridge_addr);
    }

    /// Hot-reload the worker: gracefully stop the running worker and start a
    /// fresh one. Because the `ProcessSpec` re-execs `python3 <worker
    /// script>`, the replacement picks up an edited on-disk worker file.
    ///
    /// The old process actor is *not* force-stopped — sending `Close` makes
    /// it SIGTERM and reap the child before self-terminating, so the GPU is
    /// freed before the replacement loads (force-stopping could orphan the
    /// child). Any late notification from the old worker is ignored in
    /// `handle`: each `ProcessNotification` carries its origin proc address,
    /// which no longer matches `process_addr` after the swap.
    fn handle_reload_worker(&mut self, ctx: &Ctx) {
        if let Some(proc_addr) = self.process_addr.take() {
            let _ = ctx.send(proc_addr, ProcessCommand::Close);
        }
        if let Some(bridge_addr) = self.bridge_addr.take() {
            let _ = ctx.stop_actor(bridge_addr);
        }
        self.ready = false;
        self.process_alive = false;
        self.clear_pending_state();
        self.output_buffer.clear();
        self.spawn_worker(ctx);
    }
}

impl ActorInterface for StageActor {
    type Incoming = StageMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        self.spawn_worker(ctx);
    }

    fn handle(&mut self, ctx: &Ctx, msg: StageMsg) {
        match msg {
            StageMsg::Inference(req) => self.handle_inference(ctx, req),
            StageMsg::NextToken(nt) => self.handle_next_token(ctx, nt),
            StageMsg::Activation(act) => self.handle_activation(ctx, act),
            StageMsg::SetNeighbors {
                prev_stage,
                next_stage,
                reply_to,
            } => {
                if let Some(addr) = prev_stage {
                    self.prev_stage_addr = addr;
                }
                if let Some(addr) = next_stage {
                    self.next_stage_addr = addr;
                }
                if let Some(addr) = reply_to {
                    self.reply_to = addr;
                }
            }
            StageMsg::Reset => self.handle_reset(),
            StageMsg::ReloadWorker => self.handle_reload_worker(ctx),
            StageMsg::Process(notif) => match notif {
                ProcessNotification::Started { pid, .. } => {
                    self.process_alive = true;
                    self.process_started_at = Some(Instant::now());
                    self.worker_pid = pid;
                    // Fresh process — drop any stale buffered output
                    // from an ancestor invocation so the next exit's
                    // tail reflects this process only. (Stages don't
                    // respawn today, but the contract should be
                    // per-process so the helper is correct if they
                    // ever do.)
                    self.stderr_tail.clear();
                    self.stderr_buf.clear();
                    self.last_python_traceback = None;
                    if let Some(addr) = self.status_addr {
                        let _ = ctx.send(addr, StageActorStatus::ProcessStarted);
                    }
                }
                ProcessNotification::Output { data, stream, .. } => {
                    let text = String::from_utf8_lossy(&data);
                    if stream == OutputStream::Stderr {
                        // Line-buffer stderr into the ring; do NOT feed
                        // the protocol parser. Workers may also emit
                        // diagnostic events on stderr in some setups,
                        // but the contract here is strict: stdout = JSON
                        // protocol, stderr = human-readable logs.
                        self.stderr_buf.push_str(&text);
                        while let Some(pos) = self.stderr_buf.find('\n') {
                            let line = self.stderr_buf[..pos].to_string();
                            self.stderr_buf = self.stderr_buf[pos + 1..].to_string();
                            self.push_stderr_line(line);
                        }
                    } else {
                        self.output_buffer.push_str(&text);
                        while let Some(pos) = self.output_buffer.find('\n') {
                            let line = self.output_buffer[..pos].to_string();
                            self.output_buffer = self.output_buffer[pos + 1..].to_string();
                            self.handle_worker_line(ctx, line.trim());
                        }
                    }
                }
                ProcessNotification::Exited { status, process } => {
                    // Hot-reload guard: a late exit from a worker we already
                    // replaced carries the old proc address; ignoring it
                    // keeps the freshly spawned worker's state intact.
                    if self.process_addr != Some(process) {
                        return;
                    }
                    self.process_alive = false;
                    self.ready = false;
                    self.clear_pending_state();

                    let (exit_code, signal, normal_exit) = match status {
                        ExitStatus::Code(c) => (Some(c), None, c == 0),
                        ExitStatus::Signal(s) => (None, Some(s), false),
                        ExitStatus::Unknown => (None, None, false),
                    };
                    let stderr_tail = self.drain_stderr_tail();
                    let traceback = self.last_python_traceback.take();

                    // Mirror an abnormal worker exit to this process's own
                    // stderr. pp-worker's stderr is captured by the container
                    // log (and by the datastream's process-output observer
                    // when running as a managed child), so this makes a
                    // crashed worker self-diagnosing.
                    if !normal_exit {
                        eprintln!(
                            "pp-worker: worker exited abnormally (code={exit_code:?} signal={signal:?}); stderr tail:"
                        );
                        for line in &stderr_tail {
                            eprintln!("  worker| {line}");
                        }
                        if let Some(tb) = traceback.as_deref() {
                            eprintln!("pp-worker: worker python traceback:\n{tb}");
                        }
                    }

                    self.worker_pid = None;

                    if let Some(addr) = self.status_addr {
                        let _ = ctx.send(addr, StageActorStatus::ProcessExited { status });
                    }
                }
                ProcessNotification::Error { process, .. } => {
                    if self.process_addr != Some(process) {
                        return;
                    }
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
