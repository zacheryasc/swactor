//! In-engine proof that the job runner runs *through swactor*: an orchestrator
//! FSM actor and a node job actor live in one swactor `Engine` over a Tokio
//! substrate; control AND bulk transfer (workspace push / output pull) travel as
//! actor messages; `setup`/`run` execute as supervised processes via
//! `swactor-process`; the exit code is authoritative. No SSH, no shell-out.

use std::collections::BTreeMap;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};

use swactor::actor::{ActorInterface, Ctx, Message};
use swactor::runtime::{RuntimeConfig, RuntimeParts};
use swactor_engine::{Engine, TokioBackend, TokioConfig};
use swactor_job_runner::{
    Job, JobDone, JobEdgeSink, JobState, NodeJobActor, NodeJobCommand, NodeJobEvent,
    OrchestratorJobActor, OrchestratorJobMsg, OutputChunk, Workspace,
};

const POLL: Duration = Duration::from_millis(15);
const DEADLINE: Duration = Duration::from_secs(15);

fn recv_within<T: Message>(inbox: &swactor::runtime::Inbox<T>, deadline: Duration) -> Option<T> {
    let started = Instant::now();
    loop {
        if let Some(v) = inbox.try_recv() {
            return Some(v);
        }
        if started.elapsed() >= deadline {
            return None;
        }
        std::thread::sleep(POLL);
    }
}

struct CommandSink;

impl ActorInterface for CommandSink {
    type Incoming = NodeJobCommand;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: NodeJobCommand) {}
}

fn tar_file(name: &str, contents: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut builder = tar::Builder::new(&mut buf);
    let mut header = tar::Header::new_gnu();
    header.set_size(contents.len() as u64);
    header.set_cksum();
    let mut reader = std::io::Cursor::new(contents.as_bytes());
    builder.append_data(&mut header, name, &mut reader).expect("append tar file");
    builder.finish().expect("finish tar");
    drop(builder);
    buf
}

#[test]
fn job_runs_through_actor_plane_and_swactor_process() {
    let ws = tempfile::tempdir().expect("ws tempdir");
    std::fs::write(ws.path().join("seed.txt"), "seed-value").expect("write seed");
    let node_workdir = tempfile::tempdir().expect("node workdir tempdir");
    let landing = tempfile::tempdir().expect("landing tempdir");

    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let sender = runtime.create_sender();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).expect("tokio backend"))
        .expect("engine");

    let done = runtime.new_inbox::<JobDone>().expect("done inbox");
    let orch = runtime
        .spawn(OrchestratorJobActor::new(*done.addr(), landing.path().to_path_buf()))
        .expect("spawn orchestrator");
    let node = runtime
        .spawn(NodeJobActor::new(orch, node_workdir.path().to_path_buf(), sender, 0))
        .expect("spawn node");

    let job = Job {
        name: "probe".to_owned(),
        setup: Some("echo setup-ok > setup_done.txt".to_owned()),
        run: "echo hello-from-swactor > greeting.txt".to_owned(),
        workspace: Some(Workspace { workdir: ws.path().to_path_buf(), exclude: vec![] }),
        outputs: vec![
            "greeting.txt".to_owned(),
            "setup_done.txt".to_owned(),
            "seed.txt".to_owned(),
        ],
        env: BTreeMap::new(),
    };

    runtime
        .send_to(orch, OrchestratorJobMsg::Submit { job, node_actor: node })
        .expect("submit job");

    let result = recv_within(&done, DEADLINE);
    drop(engine);
    let done = result.expect("job did not reach a terminal state within deadline");
    assert_eq!(done.state, JobState::Completed, "expected COMPLETED, got {:?}", done);
    assert_eq!(done.exit_code, Some(0), "expected exit code 0");

    let greeting = std::fs::read_to_string(landing.path().join("greeting.txt"))
        .expect("collected greeting.txt");
    assert!(greeting.contains("hello-from-swactor"), "greeting content: {greeting}");
    let setup = std::fs::read_to_string(landing.path().join("setup_done.txt"))
        .expect("collected setup_done.txt");
    assert!(setup.contains("setup-ok"), "setup content: {setup}");
    let seed = std::fs::read_to_string(landing.path().join("seed.txt")).expect("collected seed.txt");
    assert_eq!(seed, "seed-value", "workspace materialized + collected through swactor");
}

#[test]
fn workspace_excludes_are_not_materialized() {
    let ws = tempfile::tempdir().expect("ws tempdir");
    std::fs::write(ws.path().join("keep.txt"), "kept").expect("write keep");
    std::fs::write(ws.path().join("secret.txt"), "secret").expect("write secret");
    std::fs::create_dir(ws.path().join("skip_dir")).expect("create skip dir");
    std::fs::write(ws.path().join("skip_dir/hidden.txt"), "hidden").expect("write hidden");
    let node_workdir = tempfile::tempdir().expect("node workdir tempdir");
    let landing = tempfile::tempdir().expect("landing tempdir");

    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let sender = runtime.create_sender();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).expect("tokio backend"))
        .expect("engine");

    let done = runtime.new_inbox::<JobDone>().expect("done inbox");
    let orch = runtime
        .spawn(OrchestratorJobActor::new(*done.addr(), landing.path().to_path_buf()))
        .expect("spawn orchestrator");
    let node = runtime
        .spawn(NodeJobActor::new(orch, node_workdir.path().to_path_buf(), sender, 0))
        .expect("spawn node");

    let job = Job {
        name: "exclude-probe".to_owned(),
        setup: None,
        run: "test ! -e secret.txt && test ! -e skip_dir/hidden.txt && cat keep.txt > result.txt"
            .to_owned(),
        workspace: Some(Workspace {
            workdir: ws.path().to_path_buf(),
            exclude: vec!["/secret.txt".to_owned(), "/skip_dir".to_owned()],
        }),
        outputs: vec!["result.txt".to_owned()],
        env: BTreeMap::new(),
    };

    runtime
        .send_to(orch, OrchestratorJobMsg::Submit { job, node_actor: node })
        .expect("submit job");

    let result = recv_within(&done, DEADLINE);
    drop(engine);
    let done = result.expect("job did not reach terminal state");
    assert_eq!(done.state, JobState::Completed);
    assert_eq!(done.exit_code, Some(0));
    let result = std::fs::read_to_string(landing.path().join("result.txt"))
        .expect("collected result.txt");
    assert_eq!(result, "kept");
}

#[test]
fn nonzero_exit_marks_job_failed_after_best_effort_collect() {
    let ws = tempfile::tempdir().expect("ws tempdir");
    let node_workdir = tempfile::tempdir().expect("node workdir tempdir");
    let landing = tempfile::tempdir().expect("landing tempdir");

    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let sender = runtime.create_sender();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).expect("tokio backend"))
        .expect("engine");

    let done = runtime.new_inbox::<JobDone>().expect("done inbox");
    let orch = runtime
        .spawn(OrchestratorJobActor::new(*done.addr(), landing.path().to_path_buf()))
        .expect("orch");
    let node = runtime
        .spawn(NodeJobActor::new(orch, node_workdir.path().to_path_buf(), sender, 0))
        .expect("node");

    let job = Job {
        name: "fail-probe".to_owned(),
        setup: None,
        run: "echo partial > partial.txt; exit 3".to_owned(),
        workspace: Some(Workspace { workdir: ws.path().to_path_buf(), exclude: vec![] }),
        outputs: vec!["partial.txt".to_owned()],
        env: BTreeMap::new(),
    };
    runtime.send_to(orch, OrchestratorJobMsg::Submit { job, node_actor: node }).expect("submit");

    let result = recv_within(&done, DEADLINE);
    drop(engine);
    let done = result.expect("job did not terminate");
    assert_eq!(done.state, JobState::Failed, "expected FAILED");
    assert_eq!(done.exit_code, Some(3), "expected exit code 3");
    let partial = std::fs::read_to_string(landing.path().join("partial.txt"));
    assert!(partial.is_ok(), "best-effort collect should gather partial.txt");
}

#[test]
fn job_done_waits_for_output_eof_after_outputs_collected_event() {
    let landing = tempfile::tempdir().expect("landing tempdir");

    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).expect("tokio backend"))
        .expect("engine");

    let done = runtime.new_inbox::<JobDone>().expect("done inbox");
    let orch = runtime
        .spawn(OrchestratorJobActor::new(*done.addr(), landing.path().to_path_buf()))
        .expect("orch");
    let node = runtime.spawn(CommandSink).expect("node command sink");

    let job = Job {
        name: "reordered-output".to_owned(),
        setup: None,
        run: "ignored".to_owned(),
        workspace: None,
        outputs: vec!["greeting.txt".to_owned()],
        env: BTreeMap::new(),
    };
    runtime
        .send_to(orch, OrchestratorJobMsg::Submit { job, node_actor: node })
        .expect("submit");
    runtime
        .send_to(
            orch,
            OrchestratorJobMsg::NodeEvent(NodeJobEvent::JobExited { job_id: 0, code: 0 }),
        )
        .expect("job exited");
    runtime
        .send_to(
            orch,
            OrchestratorJobMsg::NodeEvent(NodeJobEvent::OutputsCollected { job_id: 0 }),
        )
        .expect("outputs collected");

    std::thread::sleep(Duration::from_millis(100));
    assert!(
        done.try_recv().is_none(),
        "job completed before output eof arrived"
    );

    runtime
        .send_to(
            orch,
            OrchestratorJobMsg::OutputChunk(OutputChunk {
                job_id: 0,
                name: "greeting.txt".to_owned(),
                seq: 0,
                data: tar_file("greeting.txt", "hello-after-event").to_vec(),
                eof: true,
            }),
        )
        .expect("output chunk");

    let result = recv_within(&done, DEADLINE);
    drop(engine);
    let done = result.expect("job did not complete after output eof");
    assert_eq!(done.state, JobState::Completed);
    assert_eq!(done.exit_code, Some(0));
    let greeting = std::fs::read_to_string(landing.path().join("greeting.txt"))
        .expect("collected greeting after reordered event");
    assert_eq!(greeting, "hello-after-event");
}

// ─── Edge-mode (EDGE_ALPN bulk bytes) actor contract ────────────────────────
//
// The integration layer (job_deploy) drives bulk bytes over EDGE_ALPN and arms
// the node actor with a `JobEdgeSink` (outputs) and a workspace-ready flag. These
// tests exercise that actor-level contract with in-memory mocks, independent of
// iroh, and prove the chunk path is NOT taken when the edge capabilities are set.

/// Records every byte record shipped through a `JobEdgeSink`.
struct RecordingSink(Arc<Mutex<Vec<Vec<u8>>>>);

impl JobEdgeSink for RecordingSink {
    fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), String> {
        self.0.lock().push(bytes);
        Ok(())
    }
}

/// Collects node→orchestrator lifecycle events for assertions.
struct EventSink {
    events: Arc<Mutex<Vec<NodeJobEvent>>>,
}

impl ActorInterface for EventSink {
    type Incoming = OrchestratorJobMsg;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, msg: OrchestratorJobMsg) {
        if let OrchestratorJobMsg::NodeEvent(ev) = msg {
            self.events.lock().push(ev);
        }
    }
}

#[test]
fn node_actor_edge_mode_ships_outputs_through_sink() {
    let workdir = tempfile::tempdir().expect("workdir");
    std::fs::write(workdir.path().join("out.txt"), "edge-output-bytes").expect("write out");

    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let sender = runtime.create_sender();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).expect("tokio"))
        .expect("engine");

    let events = Arc::new(Mutex::new(Vec::new()));
    let orch = runtime.spawn(EventSink { events: events.clone() }).expect("spawn event sink");

    let sent = Arc::new(Mutex::new(Vec::new()));
    let slot: Arc<Mutex<Option<Box<dyn JobEdgeSink>>>> =
        Arc::new(Mutex::new(Some(Box::new(RecordingSink(sent.clone())))));

    let node = runtime
        .spawn(NodeJobActor::new(orch, workdir.path().to_path_buf(), sender, 0).with_output_sink_slot(slot))
        .expect("spawn node");

    runtime
        .send_to(
            node,
            NodeJobCommand::CollectOutputs { job_id: 0, outputs: vec!["out.txt".to_owned()] },
        )
        .expect("collect outputs");

    let started = Instant::now();
    while started.elapsed() < DEADLINE {
        if events.lock().iter().any(|ev| {
            matches!(ev, NodeJobEvent::OutputsCollected { job_id: 0 })
        }) {
            break;
        }
        std::thread::sleep(POLL);
    }
    drop(engine);

    let recorded = sent.lock();
    assert_eq!(recorded.len(), 1, "edge sink received exactly one byte record");
    // The record is a tar containing out.txt; extracting it round-trips the bytes.
    let mut archive = tar::Archive::new(std::io::Cursor::new(&recorded[0]));
    let mut entries = archive.entries().expect("tar entries");
    let entry = entries.next().expect("an entry").expect("entry ok");
    assert_eq!(entry.path().unwrap().to_string_lossy(), "out.txt");

    let collected = events.lock();
    assert!(
        collected.iter().any(|ev| matches!(ev, NodeJobEvent::OutputsCollected { job_id: 0 })),
        "edge collect emitted OutputsCollected, got {:?}", collected
    );
    assert!(
        !collected.iter().any(|ev| matches!(ev, NodeJobEvent::NodeFault { .. })),
        "no fault expected, got {:?}", collected
    );
}

#[test]
fn node_actor_edge_mode_workspace_announces_on_ready_flag() {
    let workdir = tempfile::tempdir().expect("workdir");

    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let sender = runtime.create_sender();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).expect("tokio"))
        .expect("engine");

    let events = Arc::new(Mutex::new(Vec::new()));
    let orch = runtime.spawn(EventSink { events: events.clone() }).expect("spawn event sink");

    let flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let node = runtime
        .spawn(
            NodeJobActor::new(orch, workdir.path().to_path_buf(), sender, 0)
                .with_workspace_ready(flag),
        )
        .expect("spawn node");

    runtime
        .send_to(node, NodeJobCommand::MaterializeWorkspace { job_id: 0 })
        .expect("materialize");

    let started = Instant::now();
    while started.elapsed() < DEADLINE {
        if events.lock().iter().any(|ev| {
            matches!(ev, NodeJobEvent::WorkspaceMaterialized { job_id: 0 })
        }) {
            break;
        }
        std::thread::sleep(POLL);
    }
    drop(engine);

    let collected = events.lock();
    assert!(
        collected
            .iter()
            .any(|ev| matches!(ev, NodeJobEvent::WorkspaceMaterialized { job_id: 0 })),
        "edge workspace emitted WorkspaceMaterialized, got {:?}", collected
    );
}
