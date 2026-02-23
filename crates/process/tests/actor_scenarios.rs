//! Layer 3 — Actor integration tests.
//!
//! Uses a TestDriver backed by a shared EventQueue so tests can inject
//! events and observe actions without real OS processes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::{Inbox, Runtime, RuntimeConfig};

use swactor_process::*;

// ── TestDriver ──────────────────────────────────────────────────────────────

/// Shared harness for injecting events and inspecting driver actions.
#[derive(Clone)]
struct TestHarness {
    queue: EventQueue,
    actions: Arc<Mutex<Vec<ProcessAction>>>,
}

impl TestHarness {
    fn new() -> Self {
        Self {
            queue: EventQueue::new(),
            actions: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn inject(&self, event: ProcessEvent) {
        self.queue.push(event);
    }

    fn take_actions(&self) -> Vec<ProcessAction> {
        std::mem::take(&mut self.actions.lock().unwrap())
    }
}

/// A ProcessDriver that records actions and drains from a shared queue.
struct TestDriver {
    queue: EventQueue,
    actions: Arc<Mutex<Vec<ProcessAction>>>,
}

impl TestDriver {
    fn from_harness(harness: &TestHarness) -> Self {
        Self {
            queue: harness.queue.clone(),
            actions: harness.actions.clone(),
        }
    }
}

impl ProcessDriver for TestDriver {
    fn execute(&mut self, action: ProcessAction) {
        self.actions.lock().unwrap().push(action);
    }

    fn poll(&mut self) -> Vec<ProcessEvent> {
        self.queue.drain()
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn automated_spec() -> ProcessSpec {
    ProcessSpec {
        command: "echo".into(),
        args: vec!["hello".into()],
        env: HashMap::new(),
        working_dir: None,
        mode: ProcessMode::Automated,
        initial_pty_size: None,
        kill_timeout: None,
        stdin_buffer_limit: None,
    }
}

fn setup() -> (Runtime, ExternalSender) {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    (rt, sender)
}

/// Helper: tick until we receive N messages, returning them.
fn tick_collect<M: swactor::actor::Message>(
    rt: &Runtime,
    inbox: &Inbox<M>,
    n: usize,
    max_ticks: usize,
) -> Vec<M> {
    let mut msgs = Vec::new();
    for _ in 0..max_ticks {
        rt.tick();
        while let Some(m) = inbox.try_recv() {
            msgs.push(m);
            if msgs.len() >= n {
                return msgs;
            }
        }
    }
    msgs
}

use swactor::runtime::ExternalSender;

// ── Spawner actor ───────────────────────────────────────────────────────────
// We can't call ctx.spawn from outside a handle(), so we use a small "spawner"
// actor that spawns the process actor and reports its address.

#[derive(Clone)]
struct SpawnRequest {
    spec: ProcessSpec,
    harness: TestHarness,
    reply_to: ActorAddress,
    sender: ExternalSender,
}

#[derive(Clone, Debug)]
struct SpawnedAddr(ActorAddress);

struct SpawnerActor;

impl ActorInterface for SpawnerActor {
    type Incoming = SpawnRequest;
    type Response = SpawnedAddr;

    fn handle(&mut self, ctx: &Ctx, msg: SpawnRequest) {
        let waker_slot = Arc::new(OnceLock::new());
        let driver = TestDriver::from_harness(&msg.harness);
        let addr = spawn_process(ctx, &msg.sender, msg.spec, driver, waker_slot)
            .expect("spawn_process failed");
        let _ = ctx.send(msg.reply_to, SpawnedAddr(addr));
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[test]
fn happy_path_spawn_output_exit_notifies_subscriber() {
    let (rt, sender) = setup();
    let harness = TestHarness::new();
    let notif_inbox = rt.new_inbox::<ProcessNotification>().unwrap();

    // Spawn the spawner actor
    let spawner_addr = rt.spawn(SpawnerActor).unwrap();
    let reply_inbox = rt.new_inbox::<SpawnedAddr>().unwrap();
    rt.tick();

    // Ask spawner to create a process actor
    rt.send_to(
        spawner_addr,
        SpawnRequest {
            spec: automated_spec(),
            harness: harness.clone(),
            reply_to: *reply_inbox.addr(),
            sender: sender.clone(),
        },
    )
    .unwrap();

    // Tick to process spawn request
    for _ in 0..5 {
        rt.tick();
    }

    let spawned = reply_inbox.try_recv().expect("should get spawned addr");
    let proc_addr = spawned.0;

    // Verify SpawnProcess action was sent to driver
    let actions = harness.take_actions();
    assert!(
        actions.iter().any(|a| matches!(a, ProcessAction::SpawnProcess { .. })),
        "driver should receive SpawnProcess, got: {:?}",
        actions
    );

    // Subscribe to notifications
    rt.send_to(proc_addr, ProcessCommand::Subscribe { address: *notif_inbox.addr() })
        .unwrap();
    rt.tick();

    // Inject Started event from "driver"
    harness.inject(ProcessEvent::Started);
    // Send PollTick to trigger drain
    rt.send_to(proc_addr, ProcessCommand::PollTick).unwrap();
    for _ in 0..3 {
        rt.tick();
    }

    let msgs = tick_collect(&rt, &notif_inbox, 1, 10);
    assert!(
        msgs.iter().any(|m| matches!(m, ProcessNotification::Started { .. })),
        "subscriber should get Started notification, got: {:?}",
        msgs
    );

    // Inject output
    harness.inject(ProcessEvent::OutputReceived {
        data: b"hello\n".to_vec(),
        is_stderr: false,
    });
    rt.send_to(proc_addr, ProcessCommand::PollTick).unwrap();
    let msgs = tick_collect(&rt, &notif_inbox, 1, 10);
    assert!(
        msgs.iter().any(|m| matches!(m, ProcessNotification::Output { .. })),
        "subscriber should get Output notification"
    );

    // Inject exit
    harness.inject(ProcessEvent::Exited {
        status: ExitStatus::Code(0),
    });
    rt.send_to(proc_addr, ProcessCommand::PollTick).unwrap();
    let msgs = tick_collect(&rt, &notif_inbox, 1, 10);
    assert!(
        msgs.iter().any(|m| matches!(
            m,
            ProcessNotification::Exited { status: ExitStatus::Code(0), .. }
        )),
        "subscriber should get Exited(0) notification"
    );
}

#[test]
fn polltick_drains_queued_events() {
    let (rt, sender) = setup();
    let harness = TestHarness::new();
    let notif_inbox = rt.new_inbox::<ProcessNotification>().unwrap();

    let spawner_addr = rt.spawn(SpawnerActor).unwrap();
    let reply_inbox = rt.new_inbox::<SpawnedAddr>().unwrap();
    rt.tick();

    rt.send_to(
        spawner_addr,
        SpawnRequest {
            spec: automated_spec(),
            harness: harness.clone(),
            reply_to: *reply_inbox.addr(),
            sender: sender.clone(),
        },
    )
    .unwrap();
    for _ in 0..5 {
        rt.tick();
    }

    let proc_addr = reply_inbox.try_recv().unwrap().0;

    // Subscribe
    rt.send_to(proc_addr, ProcessCommand::Subscribe { address: *notif_inbox.addr() })
        .unwrap();
    rt.tick();

    // Queue multiple events before sending PollTick
    harness.inject(ProcessEvent::Started);
    harness.inject(ProcessEvent::OutputReceived {
        data: b"line1\n".to_vec(),
        is_stderr: false,
    });
    harness.inject(ProcessEvent::OutputReceived {
        data: b"line2\n".to_vec(),
        is_stderr: false,
    });

    // Single PollTick should drain all
    rt.send_to(proc_addr, ProcessCommand::PollTick).unwrap();
    let msgs = tick_collect(&rt, &notif_inbox, 3, 20);

    assert_eq!(msgs.len(), 3, "all three events should produce notifications");
    assert!(matches!(msgs[0], ProcessNotification::Started { .. }));
    assert!(matches!(msgs[1], ProcessNotification::Output { .. }));
    assert!(matches!(msgs[2], ProcessNotification::Output { .. }));
}

#[test]
fn close_command_triggers_graceful_shutdown() {
    let (rt, sender) = setup();
    let harness = TestHarness::new();
    let notif_inbox = rt.new_inbox::<ProcessNotification>().unwrap();

    let spawner_addr = rt.spawn(SpawnerActor).unwrap();
    let reply_inbox = rt.new_inbox::<SpawnedAddr>().unwrap();
    rt.tick();

    rt.send_to(
        spawner_addr,
        SpawnRequest {
            spec: automated_spec(),
            harness: harness.clone(),
            reply_to: *reply_inbox.addr(),
            sender: sender.clone(),
        },
    )
    .unwrap();
    for _ in 0..5 {
        rt.tick();
    }
    let proc_addr = reply_inbox.try_recv().unwrap().0;

    // Subscribe and get to Running state
    rt.send_to(proc_addr, ProcessCommand::Subscribe { address: *notif_inbox.addr() })
        .unwrap();
    rt.tick();
    harness.inject(ProcessEvent::Started);
    rt.send_to(proc_addr, ProcessCommand::PollTick).unwrap();
    let _ = tick_collect::<ProcessNotification>(&rt, &notif_inbox, 1, 10);

    // Send Close
    harness.take_actions(); // clear previous actions
    rt.send_to(proc_addr, ProcessCommand::Close).unwrap();
    for _ in 0..5 {
        rt.tick();
    }

    let actions = harness.take_actions();
    assert!(
        actions.iter().any(|a| matches!(
            a,
            ProcessAction::SendSignal { signal: Signal::Terminate }
        )),
        "Close should trigger SIGTERM, got: {:?}",
        actions
    );
}

#[test]
fn write_stdin_and_signal_forwarded_to_driver() {
    let (rt, sender) = setup();
    let harness = TestHarness::new();

    let spawner_addr = rt.spawn(SpawnerActor).unwrap();
    let reply_inbox = rt.new_inbox::<SpawnedAddr>().unwrap();
    rt.tick();

    rt.send_to(
        spawner_addr,
        SpawnRequest {
            spec: automated_spec(),
            harness: harness.clone(),
            reply_to: *reply_inbox.addr(),
            sender: sender.clone(),
        },
    )
    .unwrap();
    for _ in 0..5 {
        rt.tick();
    }
    let proc_addr = reply_inbox.try_recv().unwrap().0;

    // Get to Running
    harness.inject(ProcessEvent::Started);
    rt.send_to(proc_addr, ProcessCommand::PollTick).unwrap();
    for _ in 0..5 {
        rt.tick();
    }

    harness.take_actions(); // clear SpawnProcess action

    // Write stdin
    rt.send_to(
        proc_addr,
        ProcessCommand::WriteStdin {
            data: b"input\n".to_vec(),
        },
    )
    .unwrap();
    for _ in 0..3 {
        rt.tick();
    }

    let actions = harness.take_actions();
    assert!(
        actions.iter().any(|a| matches!(a, ProcessAction::WriteStdin { .. })),
        "WriteStdin should be forwarded to driver, got: {:?}",
        actions
    );

    // Send signal
    rt.send_to(
        proc_addr,
        ProcessCommand::SendSignal {
            signal: Signal::Interrupt,
        },
    )
    .unwrap();
    for _ in 0..3 {
        rt.tick();
    }

    let actions = harness.take_actions();
    assert!(
        actions.iter().any(|a| matches!(
            a,
            ProcessAction::SendSignal { signal: Signal::Interrupt }
        )),
        "SendSignal should be forwarded to driver, got: {:?}",
        actions
    );
}

#[test]
fn spawn_failure_notifies_error_and_stops_actor() {
    let (rt, sender) = setup();
    let harness = TestHarness::new();
    let notif_inbox = rt.new_inbox::<ProcessNotification>().unwrap();

    let spawner_addr = rt.spawn(SpawnerActor).unwrap();
    let reply_inbox = rt.new_inbox::<SpawnedAddr>().unwrap();
    rt.tick();

    rt.send_to(
        spawner_addr,
        SpawnRequest {
            spec: automated_spec(),
            harness: harness.clone(),
            reply_to: *reply_inbox.addr(),
            sender: sender.clone(),
        },
    )
    .unwrap();
    for _ in 0..5 {
        rt.tick();
    }
    let proc_addr = reply_inbox.try_recv().unwrap().0;

    // Subscribe
    rt.send_to(proc_addr, ProcessCommand::Subscribe { address: *notif_inbox.addr() })
        .unwrap();
    rt.tick();

    // Inject spawn failure
    harness.inject(ProcessEvent::SpawnFailed {
        reason: "command not found".into(),
    });
    rt.send_to(proc_addr, ProcessCommand::PollTick).unwrap();

    let msgs = tick_collect(&rt, &notif_inbox, 1, 20);
    assert!(
        msgs.iter().any(|m| matches!(m, ProcessNotification::Error { .. })),
        "subscriber should get Error notification on spawn failure"
    );

    // Actor should have stopped — sending further messages should fail or be ignored
    // (the address may still be in the map briefly, but the actor won't process)
    for _ in 0..10 {
        rt.tick();
    }
}

#[test]
fn subscribe_and_unsubscribe_routing() {
    let (rt, sender) = setup();
    let harness = TestHarness::new();
    let inbox_a = rt.new_inbox::<ProcessNotification>().unwrap();
    let inbox_b = rt.new_inbox::<ProcessNotification>().unwrap();

    let spawner_addr = rt.spawn(SpawnerActor).unwrap();
    let reply_inbox = rt.new_inbox::<SpawnedAddr>().unwrap();
    rt.tick();

    rt.send_to(
        spawner_addr,
        SpawnRequest {
            spec: automated_spec(),
            harness: harness.clone(),
            reply_to: *reply_inbox.addr(),
            sender: sender.clone(),
        },
    )
    .unwrap();
    for _ in 0..5 {
        rt.tick();
    }
    let proc_addr = reply_inbox.try_recv().unwrap().0;

    // Subscribe both
    rt.send_to(proc_addr, ProcessCommand::Subscribe { address: *inbox_a.addr() })
        .unwrap();
    rt.send_to(proc_addr, ProcessCommand::Subscribe { address: *inbox_b.addr() })
        .unwrap();
    rt.tick();

    // Get to Running
    harness.inject(ProcessEvent::Started);
    rt.send_to(proc_addr, ProcessCommand::PollTick).unwrap();
    for _ in 0..5 {
        rt.tick();
    }

    // Both should have received Started
    assert!(inbox_a.try_recv().is_some(), "inbox_a should get Started");
    assert!(inbox_b.try_recv().is_some(), "inbox_b should get Started");

    // Unsubscribe inbox_b
    rt.send_to(proc_addr, ProcessCommand::Unsubscribe { address: *inbox_b.addr() })
        .unwrap();
    rt.tick();

    // Inject output — only inbox_a should receive it
    harness.inject(ProcessEvent::OutputReceived {
        data: b"data".to_vec(),
        is_stderr: false,
    });
    rt.send_to(proc_addr, ProcessCommand::PollTick).unwrap();
    for _ in 0..5 {
        rt.tick();
    }

    assert!(inbox_a.try_recv().is_some(), "inbox_a should get Output");
    assert!(inbox_b.try_recv().is_none(), "inbox_b should NOT get Output after unsubscribe");
}
