//! Deployable job runner over real iroh. Two entrypoints share one module:
//! `run_worker` (the GPU node) and `run_serve` (the operator). Each builds a
//! swactor `Engine` + `IrohDriver` + distribution stack, registers its job actor
//! in the directory, and exchanges its `EndpointAddr` + actor address
//! out-of-band so each side can route to the other over the iroh actor plane.

use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::env;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, ExternalSender};
use swactor_engine::{ActorCompletion, Engine, EngineHandle, TokioBackend, TokioConfig};
use swactor_job_runner::{
    INFERENCE_RESULTS_EDGE_ID, Job, JobDataPlanePort, JobDone, NodeJobActor, OUTPUTS_EDGE_ID,
    OrchestratorJobActor, OrchestratorJobMsg, WORKSPACE_EDGE_ID, register_job_codecs,
};
use swactor_transport::hex_encode;

use data_plane::blob::BlobMetadata;
use data_plane::edge_wire::WireEvent;
use data_plane::host::BlobSource;
use data_plane::path::{DataPath, JobContext};
use data_plane::protocol::{JobCapability, register_data_plane_codecs};
use distribution::node::DistributedNodeConfig;
use iroh::{EndpointAddr, RelayMode};
use iroh_driver::{
    EDGE_ALPN, EdgeConnector, EdgeSendHandle, EndpointAddrMask, IrohDriver, IrohDriverConfig,
    MVP_IROH_ENDPOINT_ADDR_MASK_ENV, advertised_endpoint,
};
use tokio::io::AsyncReadExt;
use tokio::sync::Notify;

use crate::job_data_plane::{ActorJobDataPlane, MyelinChildRouteRegistrar};
use crate::orchestration::distribution_stack::DistributionRuntimeStack;

const POLL: Duration = Duration::from_millis(25);
const CONVERGE_DEADLINE: Duration = Duration::from_secs(90);
const JOB_DEADLINE: Duration = Duration::from_secs(60 * 45);
const RELAY_WAIT_DEADLINE: Duration = Duration::from_secs(30);
const MYELIN_IROH_RELAY_MODE_ENV: &str = "MYELIN_IROH_RELAY_MODE";
const MYELIN_IROH_RELAY_URL_ENV: &str = "MYELIN_IROH_RELAY_URL";
const SWACTOR_IROH_RELAY_URL_ENV: &str = "SWACTOR_IROH_RELAY_URL";
const JOB_OUTPUT_SOCKET: &str = "inference-results.sock";
const DATA_PLANE_CONNECT_DEADLINE: Duration = Duration::from_secs(30);
const JOB_ARENA_BYTES: u64 = 1 << 20;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct InputBlobAssignment {
    edge_id: u64,
    path: DataPath,
    metadata: BlobMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EmbeddedDataPlaneAssignment {
    result_endpoint: EndpointAddr,
    input_blobs: Vec<InputBlobAssignment>,
}

/// Actor-driven finite-blob ingress plus the retained temporary Unix output
/// stream bridge. Remote bytes remain on `EDGE_ALPN`.
#[derive(Clone)]
pub(crate) struct EmbeddedJobDataPlane {
    input_paths: Arc<Mutex<BTreeMap<u64, DataPath>>>,
    result_sink: Arc<Mutex<Option<EdgeSendHandle>>>,
    result_ready: Arc<Notify>,
    connector: EdgeConnector,
    actor_plane: Arc<ActorJobDataPlane>,
    host_endpoint_json: String,
    output_path: PathBuf,
}

impl EmbeddedJobDataPlane {
    pub(crate) fn start(
        engine: EngineHandle,
        connector: EdgeConnector,
        root: &Path,
        stack: &DistributionRuntimeStack,
        host_endpoint: EndpointAddr,
    ) -> Result<Self, String> {
        std::fs::create_dir_all(root)
            .map_err(|error| format!("create job data-plane root {}: {error}", root.display()))?;
        let output_path = root.join(JOB_OUTPUT_SOCKET);
        remove_stale_socket(&output_path)?;
        let capability = JobCapability::new(ActorAddress::new_random().0);
        let route_registrar = Arc::new(MyelinChildRouteRegistrar::new(
            stack.route_view.clone(),
            stack.pinned_routes.clone(),
            stack.route_binder.clone(),
        ));
        let actor_plane = Arc::new(ActorJobDataPlane::new(
            &stack.runtime,
            JOB_ARENA_BYTES,
            1,
            1,
            capability,
            JobContext {
                run_id: "unconfigured".to_owned(),
                read_prefixes: vec![DataPath::parse("/models").expect("static model prefix")],
                write_prefixes: vec![DataPath::parse("/runs").expect("static run prefix")],
            },
            BTreeMap::<DataPath, BlobSource>::new(),
            Some(route_registrar),
        )?);
        let host_endpoint_json = serde_json::to_string(&host_endpoint)
            .map_err(|error| format!("serialize host data-plane endpoint: {error}"))?;

        let input_paths = Arc::new(Mutex::new(BTreeMap::new()));
        let result_sink: Arc<Mutex<Option<EdgeSendHandle>>> = Arc::new(Mutex::new(None));
        let result_ready = Arc::new(Notify::new());

        let output_slot = Arc::clone(&result_sink);
        let output_ready = Arc::clone(&result_ready);
        swactor_process::spawn_unix_stream_listener(engine, &output_path, move |mut stream| {
            let output_slot = Arc::clone(&output_slot);
            let output_ready = Arc::clone(&output_ready);
            async move {
                let sink = loop {
                    let notified = output_ready.notified();
                    if let Some(sink) = output_slot.lock().take() {
                        break sink;
                    }
                    notified.await;
                };
                let mut bytes = vec![0_u8; 64 * 1024];
                loop {
                    match stream.read(&mut bytes).await {
                        Ok(0) => break,
                        Ok(count) => {
                            if sink.send(bytes[..count].to_vec()).is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                drop(sink);
            }
        })
        .map_err(|error| format!("bind job data-plane output: {error}"))?;

        Ok(Self {
            input_paths,
            result_sink,
            result_ready,
            connector,
            output_path,
            actor_plane,
            host_endpoint_json,
        })
    }

    pub(crate) fn drain_input_events(&self, events: &Arc<Mutex<Vec<WireEvent>>>) {
        let input_paths = self.input_paths.lock().clone();
        let mut events = events.lock();
        let mut remaining = Vec::with_capacity(events.len());
        for event in events.drain(..) {
            match event {
                WireEvent::BytesRead { edge_id, bytes, .. }
                    if input_paths.contains_key(&edge_id.0) =>
                {
                    let path = input_paths[&edge_id.0].clone();
                    let _ = self.actor_plane.push_blob_chunk(path, bytes);
                }
                WireEvent::StreamEnded { edge_id, .. } if input_paths.contains_key(&edge_id.0) => {
                    let path = input_paths[&edge_id.0].clone();
                    let _ = self.actor_plane.finish_blob_source(path);
                }
                WireEvent::StreamFault {
                    edge_id: Some(edge_id),
                    reason,
                    ..
                } if input_paths.contains_key(&edge_id.0) => {
                    let path = input_paths[&edge_id.0].clone();
                    let _ = self
                        .actor_plane
                        .fail_blob_source(path, format!("{reason:?}"));
                }
                WireEvent::StreamArrived { edge_id, .. }
                    if input_paths.contains_key(&edge_id.0) => {}
                event => remaining.push(event),
            }
        }
        events.extend(remaining);
    }
}

impl JobDataPlanePort for EmbeddedJobDataPlane {
    fn configure(
        &self,
        job_id: u64,
        result_peer: &str,
    ) -> Result<BTreeMap<String, String>, String> {
        let assignment = serde_json::from_str::<EmbeddedDataPlaneAssignment>(result_peer)
            .map_err(|error| format!("parse job data-plane assignment: {error}"))?;
        let sink = self.connector.connect(
            assignment.result_endpoint,
            INFERENCE_RESULTS_EDGE_ID,
            DATA_PLANE_CONNECT_DEADLINE,
        )?;
        *self.result_sink.lock() = Some(sink);
        self.result_ready.notify_one();
        self.actor_plane.configure_run(job_id.to_string())?;
        let mut input_paths = BTreeMap::new();
        for input in assignment.input_blobs {
            if input_paths
                .insert(input.edge_id, input.path.clone())
                .is_some()
            {
                return Err(format!(
                    "duplicate input blob edge id {} in job assignment",
                    input.edge_id
                ));
            }
            self.actor_plane.begin_blob_source(
                input.path,
                input.metadata.length,
                input.metadata.digest,
            )?;
        }
        *self.input_paths.lock() = input_paths;
        let mut env = self.actor_plane.handoff_env(&self.host_endpoint_json);
        env.insert(
            "SWACTOR_DATA_PLANE_OUTPUT".to_owned(),
            self.output_path.to_string_lossy().into_owned(),
        );
        Ok(env)
    }

    fn session_ended(&self, _job_id: u64) {
        self.input_paths.lock().clear();
        self.result_sink.lock().take();
        self.actor_plane.close();
    }
}

fn remove_stale_socket(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "remove stale job data-plane socket {}: {error}",
            path.display()
        )),
    }
}

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
    build_composition_with_relay(relay_mode_from_env()?)
}

fn build_composition_with_relay(relay_mode: RelayMode) -> Result<JobComposition, String> {
    let (parts, runtime, codec, transport_router) = DistributionRuntimeStack::build_runtime(
        |c| {
            register_job_codecs(c);
            register_data_plane_codecs(c);
        },
        None,
    );
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).expect("tokio backend"),
    )
    .expect("engine");
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

struct ResolveIdentityActor {
    driver: Option<IrohDriver>,
    actor: ActorAddress,
    mask: EndpointAddrMask,
    started: Instant,
    engine: EngineHandle,
    sender: ExternalSender,
    completion: ActorCompletion<Result<(IrohDriver, NodeIdentity), String>>,
}

#[derive(Clone)]
enum ResolveIdentityMsg {
    Check,
}

impl ResolveIdentityActor {
    fn finish(&mut self, ctx: &swactor::runtime::Ctx, result: Result<NodeIdentity, String>) {
        let result = result.map(|identity| {
            (
                self.driver
                    .take()
                    .expect("identity resolver owns driver until completion"),
                identity,
            )
        });
        assert!(
            self.completion.complete(result).is_ok(),
            "identity resolver completed twice"
        );
        ctx.stop_self();
    }

    fn schedule_check(&self, ctx: &swactor::runtime::Ctx) {
        self.engine.send_after(
            POLL,
            self.sender.clone(),
            ctx.self_addr(),
            ResolveIdentityMsg::Check,
        );
    }
}

impl ActorInterface for ResolveIdentityActor {
    type Incoming = ResolveIdentityMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &swactor::runtime::Ctx) {
        let _ = ctx.send(ctx.self_addr(), ResolveIdentityMsg::Check);
    }

    fn handle(&mut self, ctx: &swactor::runtime::Ctx, _message: Self::Incoming) {
        let driver = self
            .driver
            .as_ref()
            .expect("identity resolver handles messages only while live");
        let endpoint = driver.endpoint_addr();
        if !self.mask.requires_relay() || endpoint.relay_urls().next().is_some() {
            let identity = advertised_endpoint(endpoint, self.mask)
                .map(|endpoint| NodeIdentity {
                    endpoint,
                    actor_hex: hex_encode(&self.actor.0),
                })
                .map_err(|error| error.to_string());
            self.finish(ctx, identity);
        } else if self.started.elapsed() >= RELAY_WAIT_DEADLINE {
            self.finish(
                ctx,
                Err(format!(
                    "relay-only endpoint address mask did not observe a relay URL within {RELAY_WAIT_DEADLINE:?}; last endpoint={endpoint:?}"
                )),
            );
        } else {
            self.schedule_check(ctx);
        }
    }
}

fn resolve_identity(
    driver: IrohDriver,
    actor: ActorAddress,
    stack: &DistributionRuntimeStack,
) -> Result<(IrohDriver, NodeIdentity), String> {
    let completion = ActorCompletion::new();
    stack
        .runtime
        .spawn(ResolveIdentityActor {
            driver: Some(driver),
            actor,
            mask: endpoint_addr_mask_from_env()?,
            started: Instant::now(),
            engine: stack.engine.clone(),
            sender: stack.runtime.create_sender(),
            completion: completion.clone(),
        })
        .map_err(|error| format!("spawn endpoint identity resolver: {error}"))?;
    completion.wait()
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

#[derive(Clone)]
enum WorkerLifecycleMsg {
    CheckConnection,
    Stop,
}

struct WorkerLifecycleActor {
    driver: IrohDriver,
    _stack: DistributionRuntimeStack,
    orchestrator_endpoint: EndpointAddr,
    orchestrator_node: swactor_transport::NodeId,
    output_sink: Arc<Mutex<Option<Box<dyn swactor_job_runner::JobEdgeSink>>>>,
    started: Instant,
    armed: bool,
    engine: EngineHandle,
    sender: ExternalSender,
    completion: ActorCompletion<Result<(), String>>,
}

impl WorkerLifecycleActor {
    fn schedule_check(&self, ctx: &Ctx) {
        self.engine.send_after(
            POLL,
            self.sender.clone(),
            ctx.self_addr(),
            WorkerLifecycleMsg::CheckConnection,
        );
    }
}

impl ActorInterface for WorkerLifecycleActor {
    type Incoming = WorkerLifecycleMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let _ = ctx.send(ctx.self_addr(), WorkerLifecycleMsg::CheckConnection);
    }

    fn handle(&mut self, ctx: &Ctx, message: Self::Incoming) {
        match message {
            WorkerLifecycleMsg::CheckConnection if !self.armed => {
                if self.driver.has_active_connection(&self.orchestrator_node)
                    || self.started.elapsed() >= CONVERGE_DEADLINE
                {
                    match self
                        .driver
                        .spawn_edge_send_pump(self.orchestrator_endpoint.clone(), OUTPUTS_EDGE_ID)
                    {
                        Ok(handle) => {
                            *self.output_sink.lock() = Some(Box::new(IrohEdgeSink(handle)));
                            eprintln!("job-worker: output edge sink armed");
                        }
                        Err(error) => {
                            eprintln!("job-worker: failed to arm output edge sink: {error}")
                        }
                    }
                    self.armed = true;
                } else {
                    self.schedule_check(ctx);
                }
            }
            WorkerLifecycleMsg::CheckConnection => {}
            WorkerLifecycleMsg::Stop => {
                assert!(
                    self.completion.complete(Ok(())).is_ok(),
                    "worker lifecycle completed twice"
                );
                ctx.stop_self();
            }
        }
    }
}

struct WorkerStopForwarder {
    sender: ExternalSender,
    worker: ActorAddress,
}

impl ActorInterface for WorkerStopForwarder {
    type Incoming = ();
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, (): Self::Incoming) {
        let _ = self.sender.send_to(self.worker, WorkerLifecycleMsg::Stop);
        ctx.stop_self();
    }
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
    let (engine, driver, stack) = build_composition()?;
    let sender = stack.runtime.create_sender();

    let workspace_ready = Arc::new(AtomicBool::new(false));
    stack
        .runtime
        .spawn(WorkspaceEdgeActor {
            engine: stack.engine.clone(),
            sender: stack.runtime.create_sender(),
            events: driver.edge_events_handle(),
            workdir: workdir.clone(),
            ready: workspace_ready.clone(),
            buf: Vec::new(),
            started: Instant::now(),
        })
        .map_err(|error| format!("spawn workspace edge actor: {error}"))?;

    let output_sink_slot: Arc<Mutex<Option<Box<dyn swactor_job_runner::JobEdgeSink>>>> =
        Arc::new(Mutex::new(None));
    let job_actor = stack
        .runtime
        .spawn(
            NodeJobActor::new(orch_actor, workdir, sender, 0)
                .with_actor_timers(engine.handle())
                .with_workspace_ready(workspace_ready)
                .with_output_sink_slot(output_sink_slot.clone()),
        )
        .map_err(|e| format!("spawn node job actor: {e}"))?;
    stack.register_local_actor(driver.register_actor(job_actor, 1));
    driver.join(std::slice::from_ref(&orch.endpoint));

    let (driver, id) = resolve_identity(driver, job_actor, &stack)?;
    println!(
        "JOB_WORKER_IDENTITY {}",
        serde_json::to_string(&id).map_err(|e| e.to_string())?
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
    eprintln!(
        "job-worker: ready actor={} endpoint={:?}",
        id.actor_hex, id.endpoint
    );

    let runtime = stack.runtime.clone();
    let completion = ActorCompletion::new();
    let lifecycle = runtime
        .spawn(WorkerLifecycleActor {
            driver,
            _stack: stack,
            orchestrator_node: swactor_transport::NodeId(*orch_endpoint.id.as_bytes()),
            orchestrator_endpoint: orch_endpoint,
            output_sink: output_sink_slot,
            started: Instant::now(),
            armed: false,
            engine: engine.handle(),
            sender: runtime.create_sender(),
            completion: completion.clone(),
        })
        .map_err(|error| format!("spawn job worker lifecycle actor: {error}"))?;
    let stop_forwarder = runtime
        .spawn(WorkerStopForwarder {
            sender: runtime.create_sender(),
            worker: lifecycle,
        })
        .map_err(|error| format!("spawn job worker stop forwarder: {error}"))?;
    #[cfg(target_os = "linux")]
    swactor_process::spawn_os_stop_signal_wait(runtime.create_sender(), stop_forwarder);
    let result = completion.wait();
    drop(engine);
    result
}

/// Starts the operator-side job actor and publishes enough identity for a
/// provisioned worker to join over iroh.
pub(crate) fn start_orchestrator(landing: PathBuf) -> Result<JobOrchestratorSession, String> {
    start_orchestrator_mode(landing, true, None)
}

fn start_orchestrator_mode(
    landing: PathBuf,
    edge_mode: bool,
    relay_override: Option<RelayMode>,
) -> Result<JobOrchestratorSession, String> {
    let (engine, driver, stack) = match relay_override {
        Some(relay_mode) => build_composition_with_relay(relay_mode)?,
        None => build_composition()?,
    };
    let done = stack
        .runtime
        .new_inbox::<JobDone>()
        .map_err(|e| format!("inbox: {e}"))?;
    let orch = stack
        .runtime
        .spawn(OrchestratorJobActor::new(*done.addr(), landing.clone()).with_edge_mode(edge_mode))
        .map_err(|e| format!("spawn orchestrator: {e}"))?;
    stack.register_local_actor(driver.register_actor(orch, 1));

    let (driver, identity) = resolve_identity(driver, orch, &stack)?;
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

pub(crate) struct JobRunStateMachine {
    session: JobOrchestratorSession,
    job: Option<Job>,
    worker: NodeIdentity,
    node_actor: ActorAddress,
    phase: JobRunPhase,
}

enum JobRunPhase {
    Created,
    Directory {
        deadline: Instant,
    },
    Connection {
        started: Instant,
        deadline: Instant,
    },
    Running {
        deadline: Instant,
        output_buf: Vec<u8>,
        outputs_ended: bool,
        pending_done: Option<JobDone>,
    },
    Finished,
}

impl JobRunStateMachine {
    pub(crate) fn new(
        session: JobOrchestratorSession,
        job: Job,
        worker: NodeIdentity,
    ) -> Result<Self, String> {
        let node_actor = parse_actor(&worker.actor_hex)?;
        Ok(Self {
            session,
            job: Some(job),
            worker,
            node_actor,
            phase: JobRunPhase::Created,
        })
    }

    pub(crate) fn start(&mut self, now: Instant) {
        self.session
            .driver
            .join(std::slice::from_ref(&self.worker.endpoint));
        self.phase = JobRunPhase::Directory {
            deadline: now + CONVERGE_DEADLINE,
        };
    }

    pub(crate) fn advance(&mut self, now: Instant) -> Option<Result<JobDone, String>> {
        let phase = std::mem::replace(&mut self.phase, JobRunPhase::Finished);
        match phase {
            JobRunPhase::Created => {
                self.phase = JobRunPhase::Created;
                None
            }
            JobRunPhase::Directory { deadline } => {
                let converged = self
                    .session
                    .stack
                    .route_view
                    .read()
                    .map(|view| view.contains_key(&self.node_actor))
                    .unwrap_or(false);
                if converged {
                    eprintln!(
                        "job-orch: directory converged; waiting for iroh connection to worker"
                    );
                    self.phase = JobRunPhase::Connection {
                        started: now,
                        deadline: now + CONVERGE_DEADLINE,
                    };
                    None
                } else if now >= deadline {
                    Some(Err(
                        "directory did not converge: orchestrator never learned the worker actor"
                            .into(),
                    ))
                } else {
                    self.phase = JobRunPhase::Directory { deadline };
                    None
                }
            }
            JobRunPhase::Connection { started, deadline } => {
                let worker_node = swactor_transport::NodeId(*self.worker.endpoint.id.as_bytes());
                let connected = self.session.driver.has_active_connection(&worker_node);
                if !connected && now < deadline {
                    self.phase = JobRunPhase::Connection { started, deadline };
                    return None;
                }
                if connected {
                    eprintln!(
                        "job-orch: iroh connection to worker established after {:?}",
                        now.saturating_duration_since(started)
                    );
                    eprintln!("job-orch: submitting job");
                } else {
                    eprintln!(
                        "job-orch: no iroh connection to worker after {CONVERGE_DEADLINE:?}; join_statuses={:?}; submitting best-effort",
                        self.session.driver.join_statuses()
                    );
                }
                match self.submit(now) {
                    Ok(()) => None,
                    Err(error) => Some(Err(error)),
                }
            }
            JobRunPhase::Running {
                deadline,
                mut output_buf,
                mut outputs_ended,
                mut pending_done,
            } => {
                let edge_events = self.session.driver.edge_events_handle();
                let drained: Vec<WireEvent> = edge_events.lock().drain(..).collect();
                for event in drained {
                    match event {
                        WireEvent::BytesRead { edge_id, bytes, .. }
                            if edge_id.0 == OUTPUTS_EDGE_ID =>
                        {
                            output_buf.extend_from_slice(&bytes);
                        }
                        WireEvent::StreamEnded { edge_id, .. } if edge_id.0 == OUTPUTS_EDGE_ID => {
                            if !output_buf.is_empty() {
                                if let Err(error) = swactor_job_runner::extract_tar(
                                    &output_buf,
                                    &self.session.landing,
                                ) {
                                    eprintln!("job-orch: untar edge outputs failed: {error}");
                                }
                                output_buf.clear();
                            }
                            outputs_ended = true;
                        }
                        _ => {}
                    }
                }
                if pending_done.is_none() {
                    pending_done = self.session.done.try_recv();
                }
                if let Some(done) = pending_done.as_ref() {
                    let need_outputs = done.exit_code.is_some() && !outputs_ended;
                    if !need_outputs || now >= deadline {
                        let done = pending_done
                            .take()
                            .expect("pending job result was observed");
                        if need_outputs {
                            eprintln!("job-orch: output edge stream did not land before deadline");
                        }
                        return Some(Ok(done));
                    }
                }
                if now >= deadline {
                    return Some(Err("job did not complete within deadline".into()));
                }
                self.phase = JobRunPhase::Running {
                    deadline,
                    output_buf,
                    outputs_ended,
                    pending_done,
                };
                None
            }
            JobRunPhase::Finished => {
                Some(Err("job state machine advanced after completion".into()))
            }
        }
    }

    fn submit(&mut self, now: Instant) -> Result<(), String> {
        let job = self
            .job
            .take()
            .ok_or_else(|| "job was already submitted".to_owned())?;
        let workspace_bytes =
            swactor_job_runner::pack_workspace(&job).map_err(|e| format!("pack workspace: {e}"))?;
        if !workspace_bytes.is_empty() {
            let pump = self
                .session
                .driver
                .spawn_edge_send_pump(self.worker.endpoint.clone(), WORKSPACE_EDGE_ID)
                .map_err(|e| format!("workspace edge pump: {e}"))?;
            for record in workspace_bytes.chunks(swactor_job_runner::EDGE_RECORD_SIZE) {
                pump.send(record.to_vec())
                    .map_err(|e| format!("workspace edge send: {e}"))?;
            }
            drop(pump);
            eprintln!("job-orch: workspace pushed over EDGE_ALPN");
        }
        self.session
            .stack
            .runtime
            .send_to(
                self.session.orch,
                OrchestratorJobMsg::Submit {
                    job,
                    node_actor: self.node_actor,
                },
            )
            .map_err(|e| format!("submit: {e}"))?;
        self.phase = JobRunPhase::Running {
            deadline: now + JOB_DEADLINE,
            output_buf: Vec::new(),
            outputs_ended: false,
            pending_done: None,
        };
        Ok(())
    }
}

#[derive(Clone)]
struct JobRunTick;

struct JobRunActor {
    machine: JobRunStateMachine,
    engine: EngineHandle,
    sender: ExternalSender,
    completion: ActorCompletion<Result<JobDone, String>>,
}

impl JobRunActor {
    fn schedule(&self, ctx: &Ctx) {
        self.engine
            .send_after(POLL, self.sender.clone(), ctx.self_addr(), JobRunTick);
    }
}

impl ActorInterface for JobRunActor {
    type Incoming = JobRunTick;
    type Response = ();
    fn on_start(&mut self, ctx: &Ctx) {
        self.machine.start(Instant::now());
        let _ = ctx.send(ctx.self_addr(), JobRunTick);
    }

    fn handle(&mut self, ctx: &Ctx, _message: Self::Incoming) {
        if let Some(result) = self.machine.advance(Instant::now()) {
            assert!(
                self.completion.complete(result).is_ok(),
                "job lifecycle completed twice"
            );
            ctx.stop_self();
        } else {
            self.schedule(ctx);
        }
    }
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
        self,
        job: Job,
        worker: NodeIdentity,
    ) -> Result<JobDone, String> {
        let runtime = self.stack.runtime.clone();
        let engine = self.stack.engine.clone();
        let completion = ActorCompletion::new();
        runtime
            .spawn(JobRunActor {
                machine: JobRunStateMachine::new(self, job, worker)?,
                engine,
                sender: runtime.create_sender(),
                completion: completion.clone(),
            })
            .map_err(|error| format!("spawn job lifecycle actor: {error}"))?;
        completion.wait()
    }
}
/// Orchestrator: expose an `OrchestratorJobActor`, print its identity, read the
/// worker identity from stdin, drive the job to completion over iroh.
pub fn run_serve(job: swactor_job_runner::Job, landing: PathBuf) -> Result<JobDone, String> {
    let session = start_orchestrator(landing)?;
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

#[derive(Clone)]
struct WorkspaceEdgePoll;

struct WorkspaceEdgeActor {
    engine: EngineHandle,
    sender: ExternalSender,
    events: Arc<Mutex<Vec<WireEvent>>>,
    workdir: PathBuf,
    ready: Arc<AtomicBool>,
    buf: Vec<u8>,
    started: Instant,
}

impl WorkspaceEdgeActor {
    fn schedule(&self, ctx: &Ctx, delay: Duration) {
        self.engine.send_after(
            delay,
            self.sender.clone(),
            ctx.self_addr(),
            WorkspaceEdgePoll,
        );
    }
}

impl ActorInterface for WorkspaceEdgeActor {
    type Incoming = WorkspaceEdgePoll;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        self.schedule(ctx, Duration::ZERO);
    }

    fn handle(&mut self, ctx: &Ctx, _message: Self::Incoming) {
        let drained: Vec<WireEvent> = self.events.lock().drain(..).collect();
        for event in drained {
            match event {
                WireEvent::BytesRead { edge_id, bytes, .. } if edge_id.0 == WORKSPACE_EDGE_ID => {
                    self.buf.extend_from_slice(&bytes);
                }
                WireEvent::StreamEnded { edge_id, .. } if edge_id.0 == WORKSPACE_EDGE_ID => {
                    if !self.buf.is_empty()
                        && let Err(error) =
                            swactor_job_runner::extract_tar(&self.buf, &self.workdir)
                    {
                        eprintln!("job-worker: untar workspace failed: {error}");
                    }
                    self.ready.store(true, Ordering::Release);
                    ctx.stop_self();
                    return;
                }
                _ => {}
            }
        }
        if self.started.elapsed() >= WORKSPACE_EDGE_WAIT {
            eprintln!("job-worker: workspace edge stream did not arrive");
            ctx.stop_self();
            return;
        }
        self.schedule(ctx, POLL);
    }
}
