//! InferenceActor — bridges swactor messaging to a Python child process.
//!
//! Spawns a Python worker (e.g. `echo_worker.py` or `tinygrad_worker.py`) via
//! the process crate and translates `InferenceRequest` messages into stdin JSON,
//! then parses stdout JSON into `InferenceResponse` replies.

use std::collections::VecDeque;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::ExternalSender;
use swactor_process::{
    spawn_local_process, ExitStatus, ProcessCommand, ProcessNotification, ProcessSpec,
};

use crate::messages::{InferenceRequest, InferenceResponse};

// ── Messages ──────────────────────────────────────────────────────────────

/// Union type for messages the InferenceActor can receive.
#[derive(Clone, Debug)]
pub enum InferenceActorMsg {
    /// An inference request from a client.
    Request(InferenceRequest),
    /// A forwarded notification from the child process.
    Process(ProcessNotification),
}

/// Status notifications emitted to an optional observer address.
#[derive(Clone, Debug)]
pub enum InferenceActorStatus {
    ProcessStarted,
    WorkerReady { pid: Option<u32> },
    ProcessExited { status: ExitStatus },
}

// ── ProcessBridge ─────────────────────────────────────────────────────────

/// Receives `ProcessNotification` from the ProcessActor and forwards it
/// wrapped as `InferenceActorMsg::Process` to the InferenceActor.
///
/// Necessary because swactor actors have a single `Incoming` type — the
/// ProcessActor sends `ProcessNotification`, but InferenceActor expects
/// `InferenceActorMsg`.
struct ProcessBridge {
    target: ActorAddress,
}

impl ActorInterface for ProcessBridge {
    type Incoming = ProcessNotification;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: ProcessNotification) {
        let _ = ctx.send(self.target, InferenceActorMsg::Process(msg));
    }
}

// ── RequestBridge ────────────────────────────────────────────────────────

/// Receives `InferenceRequest` from the network and forwards it wrapped as
/// `InferenceActorMsg::Request` to the InferenceActor.
///
/// Necessary because the network codec delivers raw `InferenceRequest`, but
/// InferenceActor expects `InferenceActorMsg`.
pub struct RequestBridge {
    pub target: ActorAddress,
}

impl ActorInterface for RequestBridge {
    type Incoming = InferenceRequest;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: InferenceRequest) {
        let _ = ctx.send(self.target, InferenceActorMsg::Request(msg));
    }
}

// ── InferenceActor ────────────────────────────────────────────────────────

pub struct InferenceActor {
    spec: ProcessSpec,
    sender: ExternalSender,
    process_addr: Option<ActorAddress>,
    bridge_addr: Option<ActorAddress>,
    pending_replies: VecDeque<ActorAddress>,
    ready: bool,
    process_alive: bool,
    worker_pid: Option<u32>,
    status_addr: Option<ActorAddress>,
    output_buffer: String,
}

impl InferenceActor {
    pub fn new(spec: ProcessSpec, sender: ExternalSender) -> Self {
        Self {
            spec,
            sender,
            process_addr: None,
            bridge_addr: None,
            pending_replies: VecDeque::new(),
            ready: false,
            process_alive: false,
            worker_pid: None,
            status_addr: None,
            output_buffer: String::new(),
        }
    }

    /// Set an observer address that receives `InferenceActorStatus` updates.
    pub fn with_status_addr(mut self, addr: ActorAddress) -> Self {
        self.status_addr = Some(addr);
        self
    }

    fn process_output_line(&mut self, ctx: &Ctx, line: &str) {
        let Ok(val) = serde_json::from_str::<serde_json::Value>(line) else {
            // Log non-JSON output (Python tracebacks, error messages, etc.)
            if !line.is_empty() {
                eprintln!("worker: {line}");
            }
            return;
        };

        if val.get("status").and_then(|v| v.as_str()) == Some("ready") {
            self.ready = true;
            let pid = val.get("pid").and_then(|v| v.as_u64()).map(|p| p as u32);
            self.worker_pid = pid;
            if let Some(addr) = self.status_addr {
                let _ = ctx.send(addr, InferenceActorStatus::WorkerReady { pid });
            }
        } else if let Some(response) = val.get("response").and_then(|v| v.as_str()) {
            if let Some(reply_to) = self.pending_replies.pop_front() {
                let _ = ctx.send(reply_to, InferenceResponse { text: response.to_string() });
            }
        } else if let Some(err) = val.get("error").and_then(|v| v.as_str()) {
            eprintln!("worker error: {err}");
        }
    }
}

impl ActorInterface for InferenceActor {
    type Incoming = InferenceActorMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let proc_addr = spawn_local_process(ctx, &self.sender, self.spec.clone())
            .expect("failed to spawn worker process");

        let bridge = ProcessBridge { target: ctx.self_addr() };
        let bridge_addr = ctx.spawn(bridge).expect("failed to spawn process bridge");

        let _ = ctx.send(proc_addr, ProcessCommand::Subscribe { address: bridge_addr });

        self.process_addr = Some(proc_addr);
        self.bridge_addr = Some(bridge_addr);
    }

    fn handle(&mut self, ctx: &Ctx, msg: InferenceActorMsg) {
        match msg {
            InferenceActorMsg::Request(req) => {
                if !self.ready || !self.process_alive {
                    let _ = ctx.send(req.reply_to, InferenceResponse { text: String::new() });
                    return;
                }
                self.pending_replies.push_back(req.reply_to);
                let json = serde_json::json!({
                    "prompt": req.prompt,
                    "max_tokens": req.max_tokens,
                    "temperature": req.temperature,
                });
                let mut data = serde_json::to_vec(&json).unwrap();
                data.push(b'\n');
                if let Some(proc_addr) = self.process_addr {
                    let _ = ctx.send(proc_addr, ProcessCommand::WriteStdin { data });
                }
            }
            InferenceActorMsg::Process(notif) => match notif {
                ProcessNotification::Started { .. } => {
                    self.process_alive = true;
                    if let Some(addr) = self.status_addr {
                        let _ = ctx.send(addr, InferenceActorStatus::ProcessStarted);
                    }
                }
                ProcessNotification::Output { data, .. } => {
                    let text = String::from_utf8_lossy(&data);
                    self.output_buffer.push_str(&text);
                    while let Some(pos) = self.output_buffer.find('\n') {
                        let line = self.output_buffer[..pos].to_string();
                        self.output_buffer = self.output_buffer[pos + 1..].to_string();
                        self.process_output_line(ctx, line.trim());
                    }
                }
                ProcessNotification::Exited { status, .. } => {
                    self.process_alive = false;
                    self.ready = false;
                    // Drain pending requests with empty responses
                    for reply_to in self.pending_replies.drain(..) {
                        let _ = ctx.send(reply_to, InferenceResponse { text: String::new() });
                    }
                    if let Some(addr) = self.status_addr {
                        let _ = ctx.send(addr, InferenceActorStatus::ProcessExited { status });
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
