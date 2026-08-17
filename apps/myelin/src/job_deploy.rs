//! Deployable job runner over real iroh. Two entrypoints share one module:
//! `run_worker` (the GPU node) and `run_serve` (the operator). Each builds a
//! swactor `Engine` + `IrohDriver` + distribution stack, registers its job actor
//! in the directory, and exchanges its `EndpointAddr` + actor address
//! out-of-band so each side can route to the other over the iroh actor plane.

use parking_lot::Mutex;
use std::env;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;
use swactor_engine::{Engine, EngineHandle, TokioBackend, TokioConfig};
use swactor_job_runner::{
    JobDone, NodeJobActor, OUTPUTS_EDGE_ID, OrchestratorJobActor, OrchestratorJobMsg,
    WORKSPACE_EDGE_ID, register_job_codecs,
};
use swactor_transport::hex_encode;

use data_plane::edge_wire::WireEvent;
use distribution::node::DistributedNodeConfig;
use iroh::{EndpointAddr, RelayMode};
use iroh_driver::{
    EDGE_ALPN, EndpointAddrMask, IrohDriver, IrohDriverConfig, MVP_IROH_ENDPOINT_ADDR_MASK_ENV,
    advertised_endpoint,
};

use crate::orchestration::distribution_stack::DistributionRuntimeStack;

const POLL: Duration = Duration::from_millis(25);
const CONVERGE_DEADLINE: Duration = Duration::from_secs(90);
const JOB_DEADLINE: Duration = Duration::from_secs(60 * 45);
const RELAY_WAIT_DEADLINE: Duration = Duration::from_secs(30);
const MYELIN_IROH_RELAY_MODE_ENV: &str = "MYELIN_IROH_RELAY_MODE";
const MYELIN_IROH_RELAY_URL_ENV: &str = "MYELIN_IROH_RELAY_URL";
const SWACTOR_IROH_RELAY_URL_ENV: &str = "SWACTOR_IROH_RELAY_URL";

/// Out-of-band identity one side publishes so the other can route to it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub endpoint: EndpointAddr,
    pub actor_hex: String,
}

pub(crate) struct JobOrchestratorSession {
    _engine: Engine,
    driver: IrohDriver,
    stack: DistributionRuntimeStack,
    done: swactor::runtime::Inbox<JobDone>,
    orch: ActorAddress,
    identity: NodeIdentity,
    landing: PathBuf,
}

type JobComposition = (Engine, IrohDriver, DistributionRuntimeStack);

pub(crate) fn build_composition() -> Result<JobComposition, String> {
    let (parts, runtime, codec, transport_router) =
        DistributionRuntimeStack::build_runtime(|c| register_job_codecs(c), None);
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).expect("tokio backend"),
    )
    .expect("engine");
    let relay_mode = relay_mode_from_env()?;
    let mut driver = IrohDriver::with_engine(
        engine.handle(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode,
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![EDGE_ALPN.to_vec()],
        },
    )
    .map_err(|e| format!("iroh driver: {e}"))?;
    let stack = DistributionRuntimeStack::new_from_runtime(
        runtime.clone(),
        codec,
        transport_router,
        driver.node_id(),
        DistributedNodeConfig::default(),
        engine.handle(),
    );
    driver.enable_actor_bridge(
        stack.runtime.clone(),
        stack.codec.clone(),
        stack.actor_bridge_routes(),
        stack.actors.swim,
        stack.relay_mirror.clone(),
        stack.route_view.clone(),
        stack.outbox.clone(),
    );
    stack.spawn_protocol_ticker(POLL);
    driver.install_actor_bridge_pump(POLL);
    Ok((engine, driver, stack))
}

fn identity_for(driver: &IrohDriver, actor: ActorAddress) -> Result<NodeIdentity, String> {
    let endpoint = advertised_endpoint_for(driver)?;
    Ok(NodeIdentity {
        endpoint,
        actor_hex: hex_encode(&actor.0),
    })
}

fn advertised_endpoint_for(driver: &IrohDriver) -> Result<EndpointAddr, String> {
    let mask = endpoint_addr_mask_from_env()?;
    if !mask.requires_relay() {
        return advertised_endpoint(driver.endpoint_addr(), mask);
    }

    let started = Instant::now();
    loop {
        let endpoint = driver.endpoint_addr();
        if endpoint.relay_urls().next().is_some() {
            return advertised_endpoint(endpoint, mask);
        }
        if started.elapsed() >= RELAY_WAIT_DEADLINE {
            return Err(format!(
                "relay-only endpoint address mask did not observe a relay URL within {RELAY_WAIT_DEADLINE:?}; last endpoint={endpoint:?}"
            ));
        }
        std::thread::sleep(POLL);
    }
}

fn endpoint_addr_mask_from_env() -> Result<EndpointAddrMask, String> {
    match env_optional(MVP_IROH_ENDPOINT_ADDR_MASK_ENV) {
        Some(mask) => EndpointAddrMask::parse(&mask),
        None => Ok(EndpointAddrMask::Full),
    }
}

fn relay_mode_from_env() -> Result<RelayMode, String> {
    let mode = env_optional(MYELIN_IROH_RELAY_MODE_ENV).map(|value| value.to_ascii_lowercase());
    let relay_url = env_optional(MYELIN_IROH_RELAY_URL_ENV)
        .or_else(|| env_optional(SWACTOR_IROH_RELAY_URL_ENV));
    match mode.as_deref() {
        Some("disabled") => Ok(RelayMode::Disabled),
        None | Some("default") => match relay_url {
            Some(raw) => {
                let relay = raw
                    .parse()
                    .map_err(|e| format!("invalid relay URL {raw:?}: {e}"))?;
                Ok(RelayMode::custom(vec![relay]))
            }
            None => Ok(RelayMode::Default),
        },
        Some(other) => Err(format!(
            "unsupported {MYELIN_IROH_RELAY_MODE_ENV}={other:?}; use disabled or default"
        )),
    }
}

fn env_optional(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

pub(crate) fn parse_actor(hex: &str) -> Result<ActorAddress, String> {
    let bytes = swactor_transport::hex_decode(hex).ok_or_else(|| "bad actor hex".to_string())?;
    if bytes.len() != 32 {
        return Err(format!("actor hex must be 32 bytes, got {}", bytes.len()));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(ActorAddress(arr))
}

/// Worker: connect to the orchestrator, expose a `NodeJobActor`, run jobs it
/// sends over iroh. Prints this node's identity as JSON on stdout, then runs
/// until killed. Bulk bytes travel over EDGE_ALPN: the orchestrator pushes the
/// workspace in (drained + extracted here), and this node ships outputs back.
/// Small commands/lifecycle stay on the actor plane. The engine is held for the
/// process lifetime.
pub fn run_worker(orch_identity_json: String, workdir: PathBuf) -> Result<(), String> {
    let orch: NodeIdentity = serde_json::from_str(&orch_identity_json)
        .map_err(|e| format!("parse orch identity: {e}"))?;
    let orch_actor = parse_actor(&orch.actor_hex)?;
    let orch_endpoint = orch.endpoint.clone();
    let (_engine, driver, stack) = build_composition()?;
    let sender = stack.runtime.create_sender();

    // Edge workspace: the orchestrator pushes the workspace tar over EDGE_ALPN
    // before submitting the job. A background thread drains those bytes,
    // extracts them into `workdir`, and signals readiness; the node actor waits
    // on that flag before announcing the workspace materialized.
    let workspace_ready = Arc::new(AtomicBool::new(false));
    let ws_events = driver.edge_events_handle();
    let ws_workdir = workdir.clone();
    let ws_ready = workspace_ready.clone();
    std::thread::Builder::new()
        .name("job-worker-ws-edge".to_owned())
        .spawn(move || {
            drain_workspace_edge(ws_events, ws_workdir, ws_ready);
        })
        .map_err(|e| format!("spawn workspace edge thread: {e}"))?;

    // Edge outputs: a slot the main thread fills with an EDGE_ALPN sink to the
    // orchestrator once the iroh connection is up. The node actor ships
    // collected outputs through it.
    let output_sink_slot: Arc<Mutex<Option<Box<dyn swactor_job_runner::JobEdgeSink>>>> =
        Arc::new(Mutex::new(None));

    let job_actor = stack
        .runtime
        .spawn(
            NodeJobActor::new(orch_actor, workdir, sender, 0)
                .with_workspace_ready(workspace_ready.clone())
                .with_output_sink_slot(output_sink_slot.clone()),
        )
        .map_err(|e| format!("spawn node job actor: {e}"))?;
    stack.register_local_actor(driver.register_actor(job_actor, 1));
    driver.join(std::slice::from_ref(&orch.endpoint));

    let id = identity_for(&driver, job_actor)?;
    println!(
        "JOB_WORKER_IDENTITY {}",
        serde_json::to_string(&id).map_err(|e| e.to_string())?
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
    eprintln!(
        "job-worker: ready actor={} endpoint={:?}",
        id.actor_hex, id.endpoint
    );

    // Wait for the iroh connection to the orchestrator, then arm the output
    // edge sink so it is ready before a CollectOutputs command can arrive.
    let orch_node = swactor_transport::NodeId(*orch_endpoint.id.as_bytes());
    let conn_started = Instant::now();
    while !driver.has_active_connection(&orch_node) {
        if conn_started.elapsed() >= CONVERGE_DEADLINE {
            break;
        }
        std::thread::sleep(POLL);
    }
    match driver.spawn_edge_send_pump(orch_endpoint.clone(), OUTPUTS_EDGE_ID) {
        Ok(handle) => {
            *output_sink_slot.lock() = Some(Box::new(IrohEdgeSink(handle)));
            eprintln!("job-worker: output edge sink armed");
        }
        Err(e) => eprintln!("job-worker: failed to arm output edge sink: {e}"),
    }

    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

/// Starts the operator-side job actor and publishes enough identity for a
/// provisioned worker to join over iroh.
pub(crate) fn start_orchestrator(landing: PathBuf) -> Result<JobOrchestratorSession, String> {
    let (engine, driver, stack) = build_composition()?;
    let done = stack
        .runtime
        .new_inbox::<JobDone>()
        .map_err(|e| format!("inbox: {e}"))?;
    let orch = stack
        .runtime
        .spawn(OrchestratorJobActor::new(*done.addr(), landing.clone()).with_edge_mode(true))
        .map_err(|e| format!("spawn orchestrator: {e}"))?;
    stack.register_local_actor(driver.register_actor(orch, 1));

    let identity = identity_for(&driver, orch)?;
    Ok(JobOrchestratorSession {
        _engine: engine,
        driver,
        stack,
        done,
        orch,
        identity,
        landing,
    })
}

impl JobOrchestratorSession {
    pub(crate) fn identity_json(&self) -> Result<String, String> {
        serde_json::to_string(&self.identity).map_err(|e| e.to_string())
    }

    pub(crate) fn runtime(&self) -> swactor::runtime::Runtime {
        self.stack.runtime.clone()
    }

    pub(crate) fn engine_handle(&self) -> EngineHandle {
        self.stack.engine.clone()
    }

    pub(crate) fn run_to_completion(
        &mut self,
        job: swactor_job_runner::Job,
        worker: NodeIdentity,
    ) -> Result<JobDone, String> {
        let node_actor = parse_actor(&worker.actor_hex)?;
        self.driver.join(std::slice::from_ref(&worker.endpoint));

        let started = Instant::now();
        while started.elapsed() < CONVERGE_DEADLINE {
            if self
                .stack
                .route_view
                .read()
                .map(|v| v.contains_key(&node_actor))
                .unwrap_or(false)
            {
                break;
            }
            std::thread::sleep(POLL);
        }
        if !self
            .stack
            .route_view
            .read()
            .map(|v| v.contains_key(&node_actor))
            .unwrap_or(false)
        {
            return Err(
                "directory did not converge: orchestrator never learned the worker actor".into(),
            );
        }
        eprintln!("job-orch: directory converged; waiting for iroh connection to worker");
        let worker_node = swactor_transport::NodeId(*worker.endpoint.id.as_bytes());
        let conn_started = Instant::now();
        while !self.driver.has_active_connection(&worker_node) {
            if conn_started.elapsed() >= CONVERGE_DEADLINE {
                eprintln!(
                    "job-orch: no iroh connection to worker after {CONVERGE_DEADLINE:?}; join_statuses={:?}; submitting best-effort",
                    self.driver.join_statuses()
                );
                break;
            }
            std::thread::sleep(POLL);
        }
        if self.driver.has_active_connection(&worker_node) {
            eprintln!(
                "job-orch: iroh connection to worker established after {:?}",
                conn_started.elapsed()
            );
            eprintln!("job-orch: submitting job");
        } else {
            eprintln!("job-orch: submitting job (no confirmed connection)");
        }

        // EDGE: push the workspace tar over EDGE_ALPN before submitting. Small
        // commands/events still travel as actor messages; only bulk bytes move
        // onto the edge transport so they survive relay (NAT) traversal.
        let edge_events = self.driver.edge_events_handle();
        let workspace_bytes =
            swactor_job_runner::pack_workspace(&job).map_err(|e| format!("pack workspace: {e}"))?;
        if !workspace_bytes.is_empty() {
            let pump = self
                .driver
                .spawn_edge_send_pump(worker.endpoint.clone(), WORKSPACE_EDGE_ID)
                .map_err(|e| format!("workspace edge pump: {e}"))?;
            for record in workspace_bytes.chunks(swactor_job_runner::EDGE_RECORD_SIZE) {
                pump.send(record.to_vec())
                    .map_err(|e| format!("workspace edge send: {e}"))?;
            }
            drop(pump); // finish the edge stream → receiver observes end-of-stream
            eprintln!("job-orch: workspace pushed over EDGE_ALPN");
        }

        self.stack
            .runtime
            .send_to(self.orch, OrchestratorJobMsg::Submit { job, node_actor })
            .map_err(|e| format!("submit: {e}"))?;

        // Drive lifecycle (actor messages) while draining the output edge stream.
        let started = Instant::now();
        let mut output_buf: Vec<u8> = Vec::new();
        let mut outputs_ended = false;
        // The orchestrator actor reports `JobDone` exactly once; hold it here
        // while we wait for the output edge stream to land so it is not lost.
        let mut pending_done: Option<JobDone> = None;
        loop {
            // Drain output edge bytes; extract the tar as soon as the stream ends
            // (release the edge-event lock before the potentially slow untar).
            let drained: Vec<WireEvent> = edge_events.lock().drain(..).collect();
            for ev in drained {
                match ev {
                    WireEvent::BytesRead { edge_id, bytes, .. } if edge_id.0 == OUTPUTS_EDGE_ID => {
                        output_buf.extend_from_slice(&bytes);
                    }
                    WireEvent::StreamEnded { edge_id, .. } if edge_id.0 == OUTPUTS_EDGE_ID => {
                        if !output_buf.is_empty() {
                            if let Err(e) =
                                swactor_job_runner::extract_tar(&output_buf, &self.landing)
                            {
                                eprintln!("job-orch: untar edge outputs failed: {e}");
                            }
                            output_buf.clear();
                        }
                        outputs_ended = true;
                    }
                    _ => {}
                }
            }

            if pending_done.is_none() {
                pending_done = self.done.try_recv();
            }

            // A job that ran (exit code observed) collected outputs over edge —
            // wait for that stream to land before returning so the landing dir is
            // populated. A pre-run fault (no exit code) ships no outputs.
            let ready = match &pending_done {
                Some(done) => {
                    let need_outputs = done.exit_code.is_some() && !outputs_ended;
                    !need_outputs || started.elapsed() >= JOB_DEADLINE
                }
                None => false,
            };
            if ready {
                let done = pending_done
                    .take()
                    .expect("pending_done observed Some in ready branch");
                if done.exit_code.is_some() && !outputs_ended {
                    eprintln!("job-orch: output edge stream did not land before deadline");
                }
                return Ok(done);
            }

            if started.elapsed() >= JOB_DEADLINE {
                return Err("job did not complete within deadline".into());
            }
            std::thread::sleep(POLL);
        }
    }
}

/// Orchestrator: expose an `OrchestratorJobActor`, print its identity, read the
/// worker identity from stdin, drive the job to completion over iroh.
pub fn run_serve(job: swactor_job_runner::Job, landing: PathBuf) -> Result<JobDone, String> {
    let mut session = start_orchestrator(landing)?;
    println!("JOB_ORCH_IDENTITY {}", session.identity_json()?);
    let _ = std::io::Write::flush(&mut std::io::stdout());
    eprintln!("job-orch: published identity; waiting for worker identity on stdin...");

    let mut line = String::new();
    BufReader::new(std::io::stdin())
        .read_line(&mut line)
        .map_err(|e| format!("read worker identity: {e}"))?;
    let json = line
        .trim()
        .strip_prefix("JOB_WORKER_IDENTITY ")
        .or_else(|| line.trim().strip_prefix("JOB_ORCH_IDENTITY "))
        .unwrap_or(line.trim());
    let worker: NodeIdentity =
        serde_json::from_str(json).map_err(|e| format!("parse worker identity: {e}"))?;
    session.run_to_completion(job, worker)
}

/// Bridges the job-runner substrate-agnostic [`JobEdgeSink`] to iroh-driver's
/// EDGE_ALPN byte handle, so the node actor can ship outputs over the edge
/// transport without the job-runner crate depending on iroh.
struct IrohEdgeSink(iroh_driver::EdgeSendHandle);

impl swactor_job_runner::JobEdgeSink for IrohEdgeSink {
    fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), String> {
        self.0.send(bytes)
    }
}

/// How long the worker waits for the orchestrator's workspace edge stream before
/// giving up (the node actor faults on its own shorter timeout if this elapses
/// without the readiness flag being set).
const WORKSPACE_EDGE_WAIT: Duration = Duration::from_secs(60 * 30);

/// Drain EDGE_ALPN workspace bytes (edge id `WORKSPACE_EDGE_ID`) the orchestrator
/// pushed, extract the tar into `workdir`, then signal readiness. Runs on a
/// background worker thread; the driver auto-accepts EDGE_ALPN connections and
/// pushes their bytes into the shared event queue drained here.
fn drain_workspace_edge(
    events: Arc<Mutex<Vec<WireEvent>>>,
    workdir: PathBuf,
    ready: Arc<AtomicBool>,
) {
    let mut buf = Vec::new();
    let started = Instant::now();
    loop {
        let drained: Vec<WireEvent> = events.lock().drain(..).collect();
        for ev in drained {
            match ev {
                WireEvent::BytesRead { edge_id, bytes, .. } if edge_id.0 == WORKSPACE_EDGE_ID => {
                    buf.extend_from_slice(&bytes);
                }
                WireEvent::StreamEnded { edge_id, .. } if edge_id.0 == WORKSPACE_EDGE_ID => {
                    if !buf.is_empty() {
                        if let Err(e) = swactor_job_runner::extract_tar(&buf, &workdir) {
                            eprintln!("job-worker: untar workspace failed: {e}");
                        }
                    }
                    ready.store(true, Ordering::Release);
                    return;
                }
                _ => {}
            }
        }
        if started.elapsed() >= WORKSPACE_EDGE_WAIT {
            eprintln!("job-worker: workspace edge stream did not arrive");
            return;
        }
        std::thread::sleep(POLL);
    }
}
