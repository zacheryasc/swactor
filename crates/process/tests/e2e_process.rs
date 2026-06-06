//! End-to-end tests: full Runtime + ExternalSender + ProcessActor<LocalDriver>.
//!
//! Spawns real OS processes through the actor system and verifies the
//! complete notification flow.

use std::collections::HashMap;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::{ExternalSender, Inbox, Runtime, RuntimeConfig};

use swactor_process::*;

fn automated_spec(cmd: &str, args: &[&str]) -> ProcessSpec {
    ProcessSpec {
        command: cmd.into(),
        args: args.iter().map(|s| s.to_string()).collect(),
        env: HashMap::new(),
        working_dir: None,
        mode: ProcessMode::Automated,
        initial_pty_size: None,
        kill_timeout: None,
        stdin_buffer_limit: None,
    }
}

/// Tick and collect up to `n` notifications, with a max tick budget.
fn tick_collect(
    rt: &Runtime,
    inbox: &Inbox<ProcessNotification>,
    n: usize,
    max_ticks: usize,
) -> Vec<ProcessNotification> {
    let mut msgs = Vec::new();
    for _ in 0..max_ticks {
        rt.tick();
        // Small sleep to let I/O threads produce events
        std::thread::sleep(std::time::Duration::from_millis(5));
        while let Some(m) = inbox.try_recv() {
            msgs.push(m);
            if msgs.len() >= n {
                return msgs;
            }
        }
    }
    msgs
}

// ── Spawner actor (needed because spawn_local_process requires &Ctx) ────────

#[derive(Clone)]
struct E2eSpawnRequest {
    spec: ProcessSpec,
    subscriber: ActorAddress,
    reply_to: ActorAddress,
    sender: ExternalSender,
}

#[derive(Clone, Debug)]
struct E2eSpawned(ActorAddress);

struct E2eSpawnerActor;

impl ActorInterface for E2eSpawnerActor {
    type Incoming = E2eSpawnRequest;
    type Response = E2eSpawned;

    fn handle(&mut self, ctx: &Ctx, msg: E2eSpawnRequest) {
        let addr = spawn_local_process(ctx, &msg.sender, msg.spec)
            .expect("spawn_local_process failed");
        // Subscribe the notification inbox
        let _ = ctx.send(addr, ProcessCommand::Subscribe { address: msg.subscriber });
        let _ = ctx.send(msg.reply_to, E2eSpawned(addr));
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[test]
fn echo_hello_full_lifecycle() {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    let notif_inbox = rt.new_inbox::<ProcessNotification>().unwrap();

    let spawner_addr = rt.spawn(E2eSpawnerActor).unwrap();
    let reply_inbox = rt.new_inbox::<E2eSpawned>().unwrap();
    rt.tick();

    rt.send_to(
        spawner_addr,
        E2eSpawnRequest {
            spec: automated_spec("echo", &["hello"]),
            subscriber: *notif_inbox.addr(),
            reply_to: *reply_inbox.addr(),
            sender: sender.clone(),
        },
    )
    .unwrap();

    // Tick enough for the spawner to process + the process actor to start
    for _ in 0..10 {
        rt.tick();
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    let spawned = reply_inbox.try_recv().expect("should get spawned address");
    let _proc_addr = spawned.0;

    // Collect notifications: Started, Output("hello\n"), Exited(0)
    let msgs = tick_collect(&rt, &notif_inbox, 3, 200);

    let has_started = msgs.iter().any(|m| matches!(m, ProcessNotification::Started { .. }));
    let has_output = msgs.iter().any(|m| {
        if let ProcessNotification::Output { data, .. } = m {
            String::from_utf8_lossy(data).contains("hello")
        } else {
            false
        }
    });
    let has_exited = msgs.iter().any(|m| {
        matches!(
            m,
            ProcessNotification::Exited {
                status: ExitStatus::Code(0),
                ..
            }
        )
    });

    assert!(has_started, "should receive Started notification, got: {:?}", msgs);
    assert!(has_output, "should receive Output with 'hello', got: {:?}", msgs);
    assert!(has_exited, "should receive Exited(0) notification, got: {:?}", msgs);

    // Verify ordering: Started before Output before Exited
    let started_idx = msgs
        .iter()
        .position(|m| matches!(m, ProcessNotification::Started { .. }))
        .unwrap();
    let output_idx = msgs
        .iter()
        .position(|m| matches!(m, ProcessNotification::Output { .. }))
        .unwrap();
    let exited_idx = msgs
        .iter()
        .position(|m| matches!(m, ProcessNotification::Exited { .. }))
        .unwrap();

    assert!(
        started_idx < output_idx,
        "Started should come before Output"
    );
    assert!(
        output_idx < exited_idx,
        "Output should come before Exited"
    );
}

/// The per-node process-output observer is the mechanism a telemetry node uses
/// to tap *every* managed process with no per-spawn wiring: it installs one
/// observer on its runtime, and any process spawned through the facility hands
/// its output there, labeled by command basename. This is the contract a node's
/// `proc.<label>.*` capture relies on, so it must hold for a real spawn driven
/// only through the public Runtime + `spawn_local_process` API.
#[test]
fn runtime_observer_taps_managed_process_output_by_basename() {
    use std::sync::{Arc, Mutex};
    use swactor::process_observer::ProcessOutputObserver;

    #[derive(Default)]
    struct Recorder {
        // (label, is_stderr, text) for each chunk observed.
        seen: Mutex<Vec<(String, bool, String)>>,
    }
    impl ProcessOutputObserver for Recorder {
        fn on_output(&self, label: &str, is_stderr: bool, data: &[u8]) {
            self.seen.lock().unwrap().push((
                label.to_string(),
                is_stderr,
                String::from_utf8_lossy(data).into_owned(),
            ));
        }
    }

    let rt = Runtime::new(RuntimeConfig::default());
    let recorder = Arc::new(Recorder::default());
    // Install the observer before the first managed process is spawned.
    rt.set_process_output_observer(recorder.clone());

    let sender = rt.create_sender();
    let notif_inbox = rt.new_inbox::<ProcessNotification>().unwrap();
    let spawner_addr = rt.spawn(E2eSpawnerActor).unwrap();
    let reply_inbox = rt.new_inbox::<E2eSpawned>().unwrap();
    rt.tick();

    // A path command so the basename (`echo`) is what labels the output, not the
    // full path — the node keys `proc.<label>.*` on the basename.
    rt.send_to(
        spawner_addr,
        E2eSpawnRequest {
            spec: automated_spec("/bin/echo", &["telemetry-line"]),
            subscriber: *notif_inbox.addr(),
            reply_to: *reply_inbox.addr(),
            sender: sender.clone(),
        },
    )
    .unwrap();

    // Drive until the process has produced output (also drains the inbox).
    let _ = tick_collect(&rt, &notif_inbox, 3, 200);

    let seen = recorder.seen.lock().unwrap().clone();
    let captured = seen
        .iter()
        .any(|(label, is_stderr, text)| label == "echo" && !*is_stderr && text.contains("telemetry-line"));
    assert!(
        captured,
        "observer should capture stdout of the managed process under its command basename, got: {seen:?}"
    );
}

#[test]
fn bad_command_reports_error_e2e() {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    let notif_inbox = rt.new_inbox::<ProcessNotification>().unwrap();

    let spawner_addr = rt.spawn(E2eSpawnerActor).unwrap();
    let reply_inbox = rt.new_inbox::<E2eSpawned>().unwrap();
    rt.tick();

    rt.send_to(
        spawner_addr,
        E2eSpawnRequest {
            spec: automated_spec("/nonexistent/binary/xyz", &[]),
            subscriber: *notif_inbox.addr(),
            reply_to: *reply_inbox.addr(),
            sender: sender.clone(),
        },
    )
    .unwrap();

    for _ in 0..10 {
        rt.tick();
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    let msgs = tick_collect(&rt, &notif_inbox, 1, 200);
    assert!(
        msgs.iter().any(|m| matches!(m, ProcessNotification::Error { .. })),
        "should receive Error notification for bad command, got: {:?}",
        msgs
    );
}
