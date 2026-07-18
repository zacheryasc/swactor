use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use datastream::{DatastreamEndpoint, DatastreamEvent, Lifetime, NodeId, StreamId};
use serde_json::{Value, json};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::{ExternalSender, Inbox, Runtime, RuntimeConfig};
use swactor_process::{
    ExitStatus, ProcessCommand, ProcessOutput, ProcessOutputConfig, ProcessSpec,
    send_process_command, spawn_local_process,
};

#[derive(Clone)]
struct SpawnRequest {
    spec: ProcessSpec,
    output: ProcessOutputConfig,
    sender: ExternalSender,
    reply_to: ActorAddress,
}

#[derive(Clone, Debug)]
enum SpawnReply {
    Spawned(ActorAddress),
    Failed(String),
}

struct SpawnerActor;

impl ActorInterface for SpawnerActor {
    type Incoming = SpawnRequest;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: SpawnRequest) {
        let reply = match spawn_local_process(ctx, &msg.sender, msg.spec, msg.output) {
            Ok(addr) => SpawnReply::Spawned(addr),
            Err(err) => SpawnReply::Failed(err.to_string()),
        };
        let _ = ctx.send(msg.reply_to, reply);
    }
}

static NEXT_STREAM: AtomicU64 = AtomicU64::new(1);

fn stage2_stream() -> StreamId {
    let id = NEXT_STREAM.fetch_add(1, Ordering::Relaxed);
    StreamId::new(NodeId::new(format!("process-stage2-{id}")), Lifetime(2))
}

fn shell_spec(command: &str, args: Vec<&str>, label: Option<&str>) -> ProcessSpec {
    ProcessSpec {
        command: command.to_owned(),
        args: args.into_iter().map(str::to_owned).collect(),
        env: HashMap::new(),
        working_dir: None,
        label: label.map(str::to_owned),
    }
}

fn drain_outputs(inbox: &Inbox<ProcessOutput>, outputs: &mut Vec<ProcessOutput>) {
    while let Some(output) = inbox.try_recv() {
        outputs.push(output);
    }
}

fn drive_once(
    rt: &Runtime,
    endpoint: Option<&DatastreamEndpoint>,
    upstream: &Inbox<ProcessOutput>,
    outputs: &mut Vec<ProcessOutput>,
) {
    rt.tick();
    std::thread::sleep(Duration::from_millis(5));
    if let Some(endpoint) = endpoint {
        endpoint.tick();
    }
    drain_outputs(upstream, outputs);
}

fn send_spawn(
    rt: &Runtime,
    spawner: ActorAddress,
    sender: &ExternalSender,
    spec: ProcessSpec,
    output: ProcessOutputConfig,
    reply: &Inbox<SpawnReply>,
) {
    rt.send_to(
        spawner,
        SpawnRequest {
            spec,
            output,
            sender: sender.clone(),
            reply_to: *reply.addr(),
        },
    )
    .unwrap();
}

fn drive_until_spawn_reply(
    rt: &Runtime,
    endpoint: Option<&DatastreamEndpoint>,
    upstream: &Inbox<ProcessOutput>,
    outputs: &mut Vec<ProcessOutput>,
    reply: &Inbox<SpawnReply>,
) -> SpawnReply {
    for _ in 0..400 {
        drive_once(rt, endpoint, upstream, outputs);
        if let Some(reply) = reply.try_recv() {
            return reply;
        }
    }
    panic!("spawner did not reply; outputs={outputs:?}");
}

fn drive_until(
    rt: &Runtime,
    endpoint: Option<&DatastreamEndpoint>,
    upstream: &Inbox<ProcessOutput>,
    outputs: &mut Vec<ProcessOutput>,
    mut done: impl FnMut(&[ProcessOutput]) -> bool,
) {
    for _ in 0..800 {
        drive_once(rt, endpoint, upstream, outputs);
        if done(outputs) {
            return;
        }
    }
    panic!("runtime condition was not reached; outputs={outputs:?}");
}

fn expect_spawned(reply: SpawnReply) -> ActorAddress {
    match reply {
        SpawnReply::Spawned(addr) => {
            assert_ne!(addr, ActorAddress::default());
            addr
        }
        SpawnReply::Failed(error) => panic!("spawn helper failed: {error}"),
    }
}

fn expect_failed(reply: SpawnReply) -> String {
    match reply {
        SpawnReply::Spawned(addr) => panic!("spawn helper unexpectedly spawned {addr:?}"),
        SpawnReply::Failed(error) => error,
    }
}

fn is_terminal(output: &ProcessOutput) -> bool {
    matches!(
        output,
        ProcessOutput::SpawnFailed { .. }
            | ProcessOutput::Exited { .. }
            | ProcessOutput::Error { .. }
    )
}

fn terminal_count(outputs: &[ProcessOutput]) -> usize {
    outputs.iter().filter(|output| is_terminal(output)).count()
}

fn has_started(outputs: &[ProcessOutput]) -> bool {
    outputs
        .iter()
        .any(|output| matches!(output, ProcessOutput::Started { pid } if *pid > 0))
}

fn has_exited(outputs: &[ProcessOutput], status: ExitStatus) -> bool {
    outputs.iter().any(|output| {
        matches!(
            output,
            ProcessOutput::Exited { status: observed } if *observed == status
        )
    })
}

fn has_spawn_failed(outputs: &[ProcessOutput]) -> bool {
    outputs
        .iter()
        .any(|output| matches!(output, ProcessOutput::SpawnFailed { .. }))
}

fn assert_no_errors(outputs: &[ProcessOutput]) {
    assert!(
        !outputs
            .iter()
            .any(|output| matches!(output, ProcessOutput::Error { .. })),
        "unexpected process error output: {outputs:?}"
    );
}

fn assert_no_spawn_failed(outputs: &[ProcessOutput]) {
    assert!(
        !has_spawn_failed(outputs),
        "unexpected spawn failure output: {outputs:?}"
    );
}

fn started_index(outputs: &[ProcessOutput]) -> usize {
    outputs
        .iter()
        .position(|output| matches!(output, ProcessOutput::Started { pid } if *pid > 0))
        .unwrap_or_else(|| panic!("missing Started output: {outputs:?}"))
}

fn exited_index(outputs: &[ProcessOutput]) -> usize {
    outputs
        .iter()
        .position(|output| matches!(output, ProcessOutput::Exited { .. }))
        .unwrap_or_else(|| panic!("missing Exited output: {outputs:?}"))
}

fn channel_names(endpoint: &DatastreamEndpoint) -> Vec<String> {
    endpoint
        .catalog_snapshot()
        .channels
        .values()
        .map(|descriptor| descriptor.name.clone())
        .collect()
}

#[test]
fn lifecycle_outputs_are_sent_upstream_and_mirrored_to_datastream() {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    let upstream = rt.new_inbox::<ProcessOutput>().unwrap();
    let reply = rt.new_inbox::<SpawnReply>().unwrap();
    let spawner = rt.spawn(SpawnerActor).unwrap();
    let endpoint = DatastreamEndpoint::new(stage2_stream());
    let subscription = endpoint.subscribe_all("process-stage2-lifecycle");

    send_spawn(
        &rt,
        spawner,
        &sender,
        shell_spec(
            "sh",
            vec!["-c", "echo stdout; echo stderr >&2; exit 0"],
            Some("trainer.0/foo"),
        ),
        ProcessOutputConfig::datastream_mirror(*upstream.addr(), endpoint.producer()),
        &reply,
    );

    let mut outputs = Vec::new();
    let process_addr = expect_spawned(drive_until_spawn_reply(
        &rt,
        Some(&endpoint),
        &upstream,
        &mut outputs,
        &reply,
    ));
    assert_ne!(process_addr, ActorAddress::default());

    let mut datastream_events = subscription.drain_available();
    drive_until(&rt, Some(&endpoint), &upstream, &mut outputs, |outputs| {
        has_exited(outputs, ExitStatus::Code(0))
    });
    for _ in 0..5 {
        drive_once(&rt, Some(&endpoint), &upstream, &mut outputs);
        datastream_events.extend(subscription.drain_available());
    }

    let started = started_index(&outputs);
    let exited = exited_index(&outputs);
    assert!(
        started < exited,
        "Started should be delivered before Exited: {outputs:?}"
    );
    assert_no_spawn_failed(&outputs);
    assert_no_errors(&outputs);

    let names = channel_names(&endpoint);
    assert!(
        names
            .iter()
            .any(|name| name == "proc.trainer_0_foo.lifecycle"),
        "catalog should contain lifecycle channel, got {names:?}"
    );
    assert!(
        names
            .iter()
            .all(|name| !name.contains("stdout") && !name.contains("stderr")),
        "process core should not register stdout/stderr channels: {names:?}"
    );

    let payloads: Vec<Value> = datastream_events
        .iter()
        .filter_map(|event| match event {
            DatastreamEvent::Frame(delivery) => serde_json::from_slice(&delivery.payload).ok(),
            _ => None,
        })
        .collect();
    assert!(
        payloads.iter().any(|payload| {
            payload.get("event") == Some(&Value::String("started".to_owned()))
                && payload
                    .get("pid")
                    .and_then(Value::as_u64)
                    .is_some_and(|pid| pid > 0)
        }),
        "lifecycle mirror should include started JSON, got {payloads:?}"
    );
    assert!(
        payloads.iter().any(|payload| {
            payload.get("event") == Some(&Value::String("exited".to_owned()))
                && payload.get("status") == Some(&json!({"kind": "code", "value": 0}))
        }),
        "lifecycle mirror should include exited JSON, got {payloads:?}"
    );
    assert!(
        payloads.iter().all(|payload| {
            payload.get("event") != Some(&Value::String("stdout".to_owned()))
                && payload.get("event") != Some(&Value::String("stderr".to_owned()))
        }),
        "lifecycle mirror should not include child output events: {payloads:?}"
    );
}

#[test]
fn spawn_failure_maps_to_public_spawn_failed_output() {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    let upstream = rt.new_inbox::<ProcessOutput>().unwrap();
    let reply = rt.new_inbox::<SpawnReply>().unwrap();
    let spawner = rt.spawn(SpawnerActor).unwrap();

    send_spawn(
        &rt,
        spawner,
        &sender,
        shell_spec(
            "/definitely/not/a/real/binary",
            Vec::new(),
            Some("missing-binary"),
        ),
        ProcessOutputConfig::disabled(*upstream.addr()),
        &reply,
    );

    let mut outputs = Vec::new();
    let _process_addr = expect_spawned(drive_until_spawn_reply(
        &rt,
        None,
        &upstream,
        &mut outputs,
        &reply,
    ));
    drive_until(&rt, None, &upstream, &mut outputs, has_spawn_failed);

    let spawn_failures: Vec<&String> = outputs
        .iter()
        .filter_map(|output| match output {
            ProcessOutput::SpawnFailed { error } => Some(error),
            _ => None,
        })
        .collect();
    assert_eq!(
        spawn_failures.len(),
        1,
        "expected exactly one spawn failure output, got {outputs:?}"
    );
    assert!(
        !spawn_failures[0].is_empty(),
        "spawn failure error should not be empty"
    );
    assert!(
        !outputs
            .iter()
            .any(|output| matches!(output, ProcessOutput::Started { .. })),
        "spawn failure should not emit Started: {outputs:?}"
    );
    assert!(
        !outputs
            .iter()
            .any(|output| matches!(output, ProcessOutput::Exited { .. })),
        "spawn failure should not emit Exited: {outputs:?}"
    );
    assert_no_errors(&outputs);
}

#[test]
fn command_basename_is_default_lifecycle_label_source() {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    let upstream = rt.new_inbox::<ProcessOutput>().unwrap();
    let reply = rt.new_inbox::<SpawnReply>().unwrap();
    let spawner = rt.spawn(SpawnerActor).unwrap();
    let endpoint = DatastreamEndpoint::new(stage2_stream());

    send_spawn(
        &rt,
        spawner,
        &sender,
        shell_spec("/bin/sh", vec!["-c", "exit 0"], None),
        ProcessOutputConfig::datastream_mirror(*upstream.addr(), endpoint.producer()),
        &reply,
    );

    let mut outputs = Vec::new();
    let _process_addr = expect_spawned(drive_until_spawn_reply(
        &rt,
        Some(&endpoint),
        &upstream,
        &mut outputs,
        &reply,
    ));
    drive_until(&rt, Some(&endpoint), &upstream, &mut outputs, |outputs| {
        has_exited(outputs, ExitStatus::Code(0))
    });

    let names = channel_names(&endpoint);
    assert!(
        names.iter().any(|name| name == "proc.sh.lifecycle"),
        "catalog should contain basename lifecycle channel, got {names:?}"
    );
    assert!(started_index(&outputs) < exited_index(&outputs));
    assert_no_spawn_failed(&outputs);
    assert_no_errors(&outputs);
}

#[test]
fn explicit_label_overrides_basename_and_duplicate_labels_are_rejected() {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    let upstream = rt.new_inbox::<ProcessOutput>().unwrap();
    let reply = rt.new_inbox::<SpawnReply>().unwrap();
    let spawner = rt.spawn(SpawnerActor).unwrap();
    let endpoint = DatastreamEndpoint::new(stage2_stream());

    send_spawn(
        &rt,
        spawner,
        &sender,
        shell_spec("/bin/sh", vec!["-c", "sleep 2"], Some("trainer.0/foo")),
        ProcessOutputConfig::datastream_mirror(*upstream.addr(), endpoint.producer()),
        &reply,
    );

    let mut outputs = Vec::new();
    let first_addr = expect_spawned(drive_until_spawn_reply(
        &rt,
        Some(&endpoint),
        &upstream,
        &mut outputs,
        &reply,
    ));

    send_spawn(
        &rt,
        spawner,
        &sender,
        shell_spec("/bin/echo", vec!["unused"], Some("trainer.0/foo")),
        ProcessOutputConfig::datastream_mirror(*upstream.addr(), endpoint.producer()),
        &reply,
    );
    let error = expect_failed(drive_until_spawn_reply(
        &rt,
        Some(&endpoint),
        &upstream,
        &mut outputs,
        &reply,
    ));
    assert!(
        error.contains(
            "duplicate process lifecycle datastream channel: proc.trainer_0_foo.lifecycle"
        ),
        "duplicate label error should name lifecycle channel, got {error}"
    );

    send_process_command(
        &sender,
        first_addr,
        ProcessCommand::Stop {
            kill_after: Some(Duration::from_millis(20)),
        },
    )
    .unwrap();
    drive_until(&rt, Some(&endpoint), &upstream, &mut outputs, |outputs| {
        outputs
            .iter()
            .any(|output| matches!(output, ProcessOutput::Exited { .. }))
    });
    assert_no_errors(&outputs);
}

#[test]
fn stop_before_spawn_success_reports_started_then_exited() {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    let upstream = rt.new_inbox::<ProcessOutput>().unwrap();
    let reply = rt.new_inbox::<SpawnReply>().unwrap();
    let spawner = rt.spawn(SpawnerActor).unwrap();

    send_spawn(
        &rt,
        spawner,
        &sender,
        shell_spec("sh", vec!["-c", "sleep 60"], Some("stop-before-start")),
        ProcessOutputConfig::disabled(*upstream.addr()),
        &reply,
    );

    let mut outputs = Vec::new();
    let process_addr = expect_spawned(drive_until_spawn_reply(
        &rt,
        None,
        &upstream,
        &mut outputs,
        &reply,
    ));
    send_process_command(
        &sender,
        process_addr,
        ProcessCommand::Stop {
            kill_after: Some(Duration::from_millis(20)),
        },
    )
    .unwrap();
    drive_until(&rt, None, &upstream, &mut outputs, |outputs| {
        outputs
            .iter()
            .any(|output| matches!(output, ProcessOutput::Exited { .. }))
    });

    assert!(started_index(&outputs) < exited_index(&outputs));
    assert_eq!(
        outputs
            .iter()
            .filter(|output| matches!(output, ProcessOutput::Exited { .. }))
            .count(),
        1,
        "expected exactly one Exited output, got {outputs:?}"
    );
    assert_eq!(
        terminal_count(&outputs),
        1,
        "unexpected terminal outputs: {outputs:?}"
    );
    assert_no_spawn_failed(&outputs);
    assert_no_errors(&outputs);
}

#[test]
fn stop_before_spawn_failure_reports_only_spawn_failed() {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    let upstream = rt.new_inbox::<ProcessOutput>().unwrap();
    let reply = rt.new_inbox::<SpawnReply>().unwrap();
    let spawner = rt.spawn(SpawnerActor).unwrap();

    send_spawn(
        &rt,
        spawner,
        &sender,
        shell_spec(
            "/definitely/not/a/real/binary",
            Vec::new(),
            Some("stop-before-spawn-failure"),
        ),
        ProcessOutputConfig::disabled(*upstream.addr()),
        &reply,
    );

    let mut outputs = Vec::new();
    let process_addr = expect_spawned(drive_until_spawn_reply(
        &rt,
        None,
        &upstream,
        &mut outputs,
        &reply,
    ));
    send_process_command(
        &sender,
        process_addr,
        ProcessCommand::Stop {
            kill_after: Some(Duration::from_millis(20)),
        },
    )
    .unwrap();
    drive_until(&rt, None, &upstream, &mut outputs, has_spawn_failed);

    assert_eq!(
        terminal_count(&outputs),
        1,
        "unexpected terminal outputs: {outputs:?}"
    );
    assert!(
        !has_started(&outputs),
        "spawn failure should not emit Started: {outputs:?}"
    );
    assert!(
        !outputs
            .iter()
            .any(|output| matches!(output, ProcessOutput::Exited { .. })),
        "spawn failure should not emit Exited: {outputs:?}"
    );
    assert_no_errors(&outputs);
}

#[test]
fn stop_running_with_kill_after_escalates_to_kill() {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    let upstream = rt.new_inbox::<ProcessOutput>().unwrap();
    let reply = rt.new_inbox::<SpawnReply>().unwrap();
    let spawner = rt.spawn(SpawnerActor).unwrap();

    send_spawn(
        &rt,
        spawner,
        &sender,
        shell_spec(
            "sh",
            vec!["-c", "trap '' TERM; while true; do sleep 1; done"],
            Some("kill-escalates"),
        ),
        ProcessOutputConfig::disabled(*upstream.addr()),
        &reply,
    );

    let mut outputs = Vec::new();
    let process_addr = expect_spawned(drive_until_spawn_reply(
        &rt,
        None,
        &upstream,
        &mut outputs,
        &reply,
    ));
    drive_until(&rt, None, &upstream, &mut outputs, has_started);
    send_process_command(
        &sender,
        process_addr,
        ProcessCommand::Stop {
            kill_after: Some(Duration::from_millis(20)),
        },
    )
    .unwrap();
    drive_until(&rt, None, &upstream, &mut outputs, |outputs| {
        has_exited(outputs, ExitStatus::Signal(9))
    });

    assert!(
        has_exited(&outputs, ExitStatus::Signal(9)),
        "expected SIGKILL exit: {outputs:?}"
    );
    assert_no_errors(&outputs);
}

#[test]
fn stop_running_without_kill_after_terminates_without_kill_escalation() {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    let upstream = rt.new_inbox::<ProcessOutput>().unwrap();
    let reply = rt.new_inbox::<SpawnReply>().unwrap();
    let spawner = rt.spawn(SpawnerActor).unwrap();

    send_spawn(
        &rt,
        spawner,
        &sender,
        shell_spec(
            "sh",
            vec!["-c", "trap 'exit 0' TERM; while true; do sleep 1; done"],
            Some("terminate-without-kill"),
        ),
        ProcessOutputConfig::disabled(*upstream.addr()),
        &reply,
    );

    let mut outputs = Vec::new();
    let process_addr = expect_spawned(drive_until_spawn_reply(
        &rt,
        None,
        &upstream,
        &mut outputs,
        &reply,
    ));
    drive_until(&rt, None, &upstream, &mut outputs, has_started);
    send_process_command(
        &sender,
        process_addr,
        ProcessCommand::Stop { kill_after: None },
    )
    .unwrap();
    drive_until(&rt, None, &upstream, &mut outputs, |outputs| {
        has_exited(outputs, ExitStatus::Code(0))
    });

    assert!(
        has_exited(&outputs, ExitStatus::Code(0)),
        "expected graceful exit: {outputs:?}"
    );
    assert!(
        !has_exited(&outputs, ExitStatus::Signal(9)),
        "stop without deadline should not escalate to SIGKILL: {outputs:?}"
    );
    assert_no_errors(&outputs);
}

#[test]
fn child_exit_before_kill_deadline_suppresses_kill_escalation() {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    let upstream = rt.new_inbox::<ProcessOutput>().unwrap();
    let reply = rt.new_inbox::<SpawnReply>().unwrap();
    let spawner = rt.spawn(SpawnerActor).unwrap();

    send_spawn(
        &rt,
        spawner,
        &sender,
        shell_spec(
            "sh",
            vec!["-c", "trap 'exit 0' TERM; while true; do sleep 1; done"],
            Some("deadline-suppresses-kill"),
        ),
        ProcessOutputConfig::disabled(*upstream.addr()),
        &reply,
    );

    let mut outputs = Vec::new();
    let process_addr = expect_spawned(drive_until_spawn_reply(
        &rt,
        None,
        &upstream,
        &mut outputs,
        &reply,
    ));
    drive_until(&rt, None, &upstream, &mut outputs, has_started);
    send_process_command(
        &sender,
        process_addr,
        ProcessCommand::Stop {
            kill_after: Some(Duration::from_secs(1)),
        },
    )
    .unwrap();
    drive_until(&rt, None, &upstream, &mut outputs, |outputs| {
        has_exited(outputs, ExitStatus::Code(0))
    });
    for _ in 0..5 {
        drive_once(&rt, None, &upstream, &mut outputs);
    }

    assert!(
        has_exited(&outputs, ExitStatus::Code(0)),
        "expected graceful exit: {outputs:?}"
    );
    assert!(
        !has_exited(&outputs, ExitStatus::Signal(9)),
        "child exit before deadline should suppress SIGKILL: {outputs:?}"
    );
    assert_no_errors(&outputs);
}

#[test]
fn duplicate_stop_while_stopping_is_noop_and_keeps_original_deadline() {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    let upstream = rt.new_inbox::<ProcessOutput>().unwrap();
    let reply = rt.new_inbox::<SpawnReply>().unwrap();
    let spawner = rt.spawn(SpawnerActor).unwrap();

    send_spawn(
        &rt,
        spawner,
        &sender,
        shell_spec(
            "sh",
            vec!["-c", "trap '' TERM; while true; do sleep 1; done"],
            Some("duplicate-stop"),
        ),
        ProcessOutputConfig::disabled(*upstream.addr()),
        &reply,
    );

    let mut outputs = Vec::new();
    let process_addr = expect_spawned(drive_until_spawn_reply(
        &rt,
        None,
        &upstream,
        &mut outputs,
        &reply,
    ));
    drive_until(&rt, None, &upstream, &mut outputs, has_started);

    let first_stop = Instant::now();
    send_process_command(
        &sender,
        process_addr,
        ProcessCommand::Stop {
            kill_after: Some(Duration::from_millis(250)),
        },
    )
    .unwrap();
    send_process_command(
        &sender,
        process_addr,
        ProcessCommand::Stop {
            kill_after: Some(Duration::from_secs(5)),
        },
    )
    .unwrap();

    while first_stop.elapsed() < Duration::from_secs(2) {
        drive_once(&rt, None, &upstream, &mut outputs);
        if has_exited(&outputs, ExitStatus::Signal(9)) {
            break;
        }
    }

    assert!(
        has_exited(&outputs, ExitStatus::Signal(9)),
        "duplicate stop should keep the original kill deadline: {outputs:?}"
    );
    assert!(
        first_stop.elapsed() < Duration::from_secs(2),
        "SIGKILL arrived too late after duplicate stop: {:?}",
        first_stop.elapsed()
    );
    assert_eq!(
        terminal_count(&outputs),
        1,
        "unexpected terminal outputs: {outputs:?}"
    );
    assert_no_errors(&outputs);
}

#[test]
fn stop_after_terminal_output_emits_no_additional_output() {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    let upstream = rt.new_inbox::<ProcessOutput>().unwrap();
    let reply = rt.new_inbox::<SpawnReply>().unwrap();
    let spawner = rt.spawn(SpawnerActor).unwrap();

    send_spawn(
        &rt,
        spawner,
        &sender,
        shell_spec("sh", vec!["-c", "exit 0"], Some("post-terminal-stop")),
        ProcessOutputConfig::disabled(*upstream.addr()),
        &reply,
    );

    let mut outputs = Vec::new();
    let process_addr = expect_spawned(drive_until_spawn_reply(
        &rt,
        None,
        &upstream,
        &mut outputs,
        &reply,
    ));
    drive_until(&rt, None, &upstream, &mut outputs, |outputs| {
        has_exited(outputs, ExitStatus::Code(0))
    });
    let output_len = outputs.len();

    let _ = send_process_command(
        &sender,
        process_addr,
        ProcessCommand::Stop {
            kill_after: Some(Duration::from_millis(10)),
        },
    );
    for _ in 0..10 {
        drive_once(&rt, None, &upstream, &mut outputs);
    }

    assert_eq!(
        outputs.len(),
        output_len,
        "post-terminal stop should not emit more output: {outputs:?}"
    );
    assert_no_errors(&outputs);
}

#[test]
fn lifecycle_mirror_submit_failure_does_not_suppress_upstream_or_emit_error() {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    let upstream = rt.new_inbox::<ProcessOutput>().unwrap();
    let reply = rt.new_inbox::<SpawnReply>().unwrap();
    let spawner = rt.spawn(SpawnerActor).unwrap();
    let endpoint = DatastreamEndpoint::with_capacity(stage2_stream(), 1, 16);

    send_spawn(
        &rt,
        spawner,
        &sender,
        shell_spec("sh", vec!["-c", "exit 0"], Some("mirror-drop")),
        ProcessOutputConfig::datastream_mirror(*upstream.addr(), endpoint.producer()),
        &reply,
    );

    let mut outputs = Vec::new();
    let _process_addr = expect_spawned(drive_until_spawn_reply(
        &rt,
        None,
        &upstream,
        &mut outputs,
        &reply,
    ));
    drive_until(&rt, None, &upstream, &mut outputs, |outputs| {
        has_exited(outputs, ExitStatus::Code(0))
    });
    let dropped = endpoint.mux().dropped();
    endpoint.tick();

    assert!(
        has_started(&outputs),
        "upstream should receive Started: {outputs:?}"
    );
    assert!(
        has_exited(&outputs, ExitStatus::Code(0)),
        "upstream should receive Exited: {outputs:?}"
    );
    assert_no_errors(&outputs);
    assert!(
        dropped > 0,
        "full lifecycle mirror mux should drop at least one frame"
    );
}

#[test]
fn stdout_and_stderr_writes_do_not_affect_lifecycle() {
    let rt = Runtime::new(RuntimeConfig::default());
    let sender = rt.create_sender();
    let upstream = rt.new_inbox::<ProcessOutput>().unwrap();
    let reply = rt.new_inbox::<SpawnReply>().unwrap();
    let spawner = rt.spawn(SpawnerActor).unwrap();

    send_spawn(
        &rt,
        spawner,
        &sender,
        shell_spec(
            "sh",
            vec![
                "-c",
                "for i in $(seq 1 1000); do echo out; echo err >&2; done; exit 0",
            ],
            Some("child-output-is-null"),
        ),
        ProcessOutputConfig::disabled(*upstream.addr()),
        &reply,
    );

    let mut outputs = Vec::new();
    let _process_addr = expect_spawned(drive_until_spawn_reply(
        &rt,
        None,
        &upstream,
        &mut outputs,
        &reply,
    ));
    drive_until(&rt, None, &upstream, &mut outputs, |outputs| {
        has_exited(outputs, ExitStatus::Code(0))
    });

    assert_eq!(
        outputs.len(),
        2,
        "process core should emit only lifecycle outputs: {outputs:?}"
    );
    assert!(started_index(&outputs) < exited_index(&outputs));
    assert!(
        has_exited(&outputs, ExitStatus::Code(0)),
        "expected clean exit: {outputs:?}"
    );
    assert_no_spawn_failed(&outputs);
    assert_no_errors(&outputs);
}
