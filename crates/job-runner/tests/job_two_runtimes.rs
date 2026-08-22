//! Two-runtime proof: the orchestrator FSM actor and the node job actor live on
//! **separate swactor runtimes**, meshed by the transport seam (codec encode →
//! `TransportRouter` → `Transport` → codec decode → `deliver_raw`) — the same
//! seam iroh realizes in production. Control, workspace bytes, output bytes, and
//! the supervised-process exit all cross the runtime boundary over the actor
//! plane; `setup`/`run` execute via `swactor-process`. No shared filesystem for
//! the job, no SSH.

use std::sync::Arc;
use std::time::{Duration, Instant};

use swactor::runtime::{Runtime, RuntimeConfig, RuntimeParts};
use swactor::std::StdExtension;
use swactor_engine::{Engine, TokioBackend, TokioConfig};
use swactor_transport::{CodecRegistry, CodecRemoteSink, Transport, TransportRouter, WireEnvelope};

use swactor_job_runner::{
    Job, JobDone, JobState, NodeJobActor, OrchestratorJobActor, OrchestratorJobMsg, Workspace,
    register_job_codecs,
};

const POLL: Duration = Duration::from_millis(15);
const DEADLINE: Duration = Duration::from_secs(20);

/// Stands in for the iroh transport: carries a `WireEnvelope` from one runtime
/// to another, decoding via the shared codec and performing the production
/// ingress (`deliver_raw`). This is exactly the seam the iroh driver fills.
struct Link {
    dst: Runtime,
    codec: Arc<CodecRegistry>,
}

impl Transport for Link {
    fn send(&self, wire: WireEnvelope) -> Result<(), swactor::Error> {
        let msg = self.codec.decode(&wire.type_tag, &wire.payload)?;
        self.dst.deliver_raw(wire.dest, msg)
    }
}

fn build_runtime(codec: Arc<CodecRegistry>) -> (RuntimeParts, Runtime, Arc<TransportRouter>) {
    let parts =
        RuntimeParts::new(RuntimeConfig::default()).with_extension(Arc::new(StdExtension::new()));
    let rt = parts.runtime().clone();
    let router = Arc::new(TransportRouter::new());
    rt.set_remote_sink(Arc::new(CodecRemoteSink::new(codec, router.clone())));
    (parts, rt, router)
}

#[test]
fn job_runs_across_two_swactor_runtimes_over_the_actor_plane() {
    let mut codec = CodecRegistry::new();
    register_job_codecs(&mut codec);
    let codec = Arc::new(codec);

    let ws = tempfile::tempdir().expect("ws");
    std::fs::write(ws.path().join("seed.txt"), "seed-value").expect("seed");
    let node_workdir = tempfile::tempdir().expect("node workdir");
    let landing = tempfile::tempdir().expect("landing");

    // Two independent runtimes, each driven by its own engine.
    let (parts_a, rt_a, router_a) = build_runtime(codec.clone());
    let (parts_b, rt_b, router_b) = build_runtime(codec.clone());
    let engine_a = Engine::new(
        parts_a,
        TokioBackend::new(TokioConfig::default()).expect("tokio"),
    )
    .expect("engine A");
    let engine_b = Engine::new(
        parts_b,
        TokioBackend::new(TokioConfig::default()).expect("tokio"),
    )
    .expect("engine B");

    let done = rt_a.new_inbox::<JobDone>().expect("done inbox");
    let orch = rt_a
        .spawn(OrchestratorJobActor::new(
            *done.addr(),
            landing.path().to_path_buf(),
        ))
        .expect("spawn orchestrator on A");
    let node = rt_b
        .spawn(NodeJobActor::new(
            orch,
            node_workdir.path().to_path_buf(),
            rt_b.create_sender(),
            0,
        ))
        .expect("spawn node on B");

    // Cross-runtime routes: A routes the node address → B; B routes the
    // orchestrator address → A. Each Link delivers to wire.dest on the peer.
    router_a.add_route(
        node,
        Arc::new(Link {
            dst: rt_b.clone(),
            codec: codec.clone(),
        }),
    );
    router_b.add_route(
        orch,
        Arc::new(Link {
            dst: rt_a.clone(),
            codec: codec.clone(),
        }),
    );

    let job = Job {
        name: "cross-runtime-probe".to_owned(),
        setup: Some("echo setup-ok > setup_done.txt".to_owned()),
        run: "echo hello-across-runtimes > greeting.txt".to_owned(),
        workspace: Some(Workspace {
            workdir: ws.path().to_path_buf(),
            exclude: vec![],
        }),
        outputs: vec![
            "greeting.txt".to_owned(),
            "setup_done.txt".to_owned(),
            "seed.txt".to_owned(),
        ],
        env: std::collections::BTreeMap::new(),
    };
    rt_a.send_to(
        orch,
        OrchestratorJobMsg::Submit {
            job,
            node_actor: node,
        },
    )
    .expect("submit");

    let started = Instant::now();
    let mut outcome = None;
    while started.elapsed() < DEADLINE {
        if let Some(d) = done.try_recv() {
            outcome = Some(d);
            break;
        }
        std::thread::sleep(POLL);
    }
    drop(engine_a);
    drop(engine_b);
    let done = outcome.expect("job did not reach a terminal state across runtimes");
    assert_eq!(
        done.state,
        JobState::Completed,
        "expected COMPLETED across runtimes, got {:?}",
        done
    );
    assert_eq!(done.exit_code, Some(0));

    let greeting =
        std::fs::read_to_string(landing.path().join("greeting.txt")).expect("collected greeting");
    assert!(
        greeting.contains("hello-across-runtimes"),
        "greeting: {greeting}"
    );
    let seed = std::fs::read_to_string(landing.path().join("seed.txt")).expect("collected seed");
    assert_eq!(
        seed, "seed-value",
        "workspace crossed the runtime boundary through swactor"
    );
}
