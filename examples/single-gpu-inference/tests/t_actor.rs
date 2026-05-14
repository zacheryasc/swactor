//! T-actor: InferenceActor + process bridge component tests.
//!
//! Spawns the InferenceActor on a single swactor runtime with `echo_worker.py`
//! as the child process. No networking, no GPU.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use swactor::actor::ActorAddress;
use swactor::runtime::{Inbox, Runtime, RuntimeConfig};

use single_gpu_inference::inference_actor::{InferenceActor, InferenceActorMsg, InferenceActorStatus};
use single_gpu_inference::messages::{InferenceRequest, InferenceResponse};
use swactor_process::{ExitStatus, ProcessMode, ProcessSpec};

// ── Helpers ───────────────────────────────────────────────────────────────

fn echo_worker_spec() -> ProcessSpec {
    ProcessSpec {
        command: "python3".into(),
        args: vec![format!("{}/echo_worker.py", env!("CARGO_MANIFEST_DIR"))],
        env: HashMap::new(),
        working_dir: None,
        mode: ProcessMode::Automated,
        initial_pty_size: None,
        kill_timeout: Some(Duration::from_secs(2)),
        stdin_buffer_limit: None,
    }
}

/// Tick the runtime in a polling loop until the inbox has a message or timeout.
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

/// Spin up an InferenceActor and wait until it reports WorkerReady.
/// Returns (actor address, worker PID).
fn spawn_and_wait_ready(
    rt: &Runtime,
    status_inbox: &Inbox<InferenceActorStatus>,
) -> (ActorAddress, u32) {
    let sender = rt.create_sender();
    let actor = InferenceActor::new(echo_worker_spec(), sender)
        .with_status_addr(*status_inbox.addr());
    let addr = rt.spawn(actor).unwrap();

    let timeout = Duration::from_secs(5);
    let mut got_started = false;
    let mut worker_pid = None;

    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(status) = tick_until_recv(rt, status_inbox, Duration::from_millis(100)) {
            match status {
                InferenceActorStatus::ProcessStarted => got_started = true,
                InferenceActorStatus::WorkerReady { pid } => {
                    assert!(got_started, "WorkerReady should come after ProcessStarted");
                    worker_pid = pid;
                    break;
                }
                other => panic!("unexpected status during startup: {:?}", other),
            }
        }
    }

    let pid = worker_pid.expect("worker should report PID within timeout");
    (addr, pid)
}

fn is_process_alive(pid: u32) -> bool {
    std::fs::metadata(format!("/proc/{}", pid)).is_ok()
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[test]
fn actor_spawns_process_and_receives_started() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<InferenceActorStatus>().unwrap();

    let (_addr, pid) = spawn_and_wait_ready(&rt, &status_inbox);

    // The process should be alive
    assert!(is_process_alive(pid), "worker process should be running");
}

#[test]
fn inference_request_flows_through_process_and_reply_arrives() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<InferenceActorStatus>().unwrap();
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();

    let (addr, _pid) = spawn_and_wait_ready(&rt, &status_inbox);

    // Send an inference request
    rt.send_to(
        addr,
        InferenceActorMsg::Request(InferenceRequest {
            prompt: "Hello, world!".into(),
            max_tokens: 8,
            temperature: 0.7,
            reply_to: *response_inbox.addr(),
        }),
    )
    .unwrap();

    let response = tick_until_recv(&rt, &response_inbox, Duration::from_secs(5))
        .expect("should receive InferenceResponse");

    assert!(
        response.text.contains("Hello, world!"),
        "echo worker should reflect the prompt, got: {:?}",
        response.text
    );
}

#[test]
fn worker_crash_is_handled_without_poisoning_runtime() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<InferenceActorStatus>().unwrap();
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();

    let (addr, _pid) = spawn_and_wait_ready(&rt, &status_inbox);

    // Send a request whose prompt triggers an os._exit(1) in the worker
    rt.send_to(
        addr,
        InferenceActorMsg::Request(InferenceRequest {
            prompt: "__crash__".into(),
            max_tokens: 1,
            temperature: 0.0,
            reply_to: *response_inbox.addr(),
        }),
    )
    .unwrap();

    // The actor should report ProcessExited
    let status = tick_until_recv(&rt, &status_inbox, Duration::from_secs(5))
        .expect("should receive ProcessExited status");
    match status {
        InferenceActorStatus::ProcessExited { status } => {
            assert_ne!(
                status,
                ExitStatus::Code(0),
                "crashed worker should not exit 0"
            );
        }
        other => panic!("expected ProcessExited, got: {:?}", other),
    }

    // Prove the runtime is still alive: spawn a trivial actor and interact with it
    #[derive(Clone)]
    struct Ping {
        reply_to: ActorAddress,
    }
    #[derive(Clone, Debug, PartialEq)]
    struct Pong;

    struct PongActor;
    impl swactor::actor::ActorInterface for PongActor {
        type Incoming = Ping;
        type Response = Pong;
        fn handle(&mut self, ctx: &swactor::runtime::Ctx, msg: Ping) {
            let _ = ctx.send(msg.reply_to, Pong);
        }
    }

    let pong_inbox = rt.new_inbox::<Pong>().unwrap();
    let pong_addr = rt.spawn(PongActor).unwrap();
    rt.send_to(pong_addr, Ping { reply_to: *pong_inbox.addr() }).unwrap();

    let pong = tick_until_recv(&rt, &pong_inbox, Duration::from_secs(2));
    assert_eq!(pong, Some(Pong), "runtime should still be functional after worker crash");
}

#[test]
fn stopping_actor_kills_child_process() {
    let rt = Runtime::new(RuntimeConfig::default());
    let status_inbox = rt.new_inbox::<InferenceActorStatus>().unwrap();

    let (addr, pid) = spawn_and_wait_ready(&rt, &status_inbox);
    assert!(is_process_alive(pid), "worker should be alive before stop");

    // Stop the InferenceActor
    rt.stop_actor(addr).unwrap();

    // Tick until the child process is gone
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
        "worker process (pid {}) should be dead after actor stop",
        pid
    );
}
