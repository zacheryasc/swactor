use std::collections::{HashMap, VecDeque};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use data_plane::blob_transfer::{
    BlobTransferEvent, BlobTransferId, BlobTransferOffer, BlobTransferReceiver, BlobTransferSender,
};
use data_plane::host::{HostRouteRegistrar, HostRouteWatch};
use data_plane::namespace::{
    BlobBinding, DataDirectoryOut, NamespaceClient, NamespaceClientIn, NamespaceRequest,
};
use data_plane::source::{BlobSourceIn, BlobSourcePublisher};
use data_plane::stream_transport::StreamTransport;
use distribution::directory_actor::DirectoryIn;
use distribution::transport_bridge::{OutboxRouteBinder, RouteBinder, RouteView};
use distribution::types::NodeId;
use iroh::EndpointAddr;
use iroh_driver::ConnectionObserver;
use myelin_control_contract::{
    ContextualExitStatus, ContextualProcessEvent, ContextualProcessEventKind,
    ContextualProcessSpec, DeploymentIdentity, ExecutionIdentity, LiveContextualExecution,
    ResourceActor, ResourceArena, ResourceTransport, SCHEMA_VERSION, WorkerResourceSnapshot,
};
use swactor::actor::ActorAddress;
use swactor::admin::{ListActorsRequest, ListActorsResponse};
use swactor::runtime::Runtime;
use swactor_engine::{ActorTimer, EngineHandle};
use swactor_process::{ExitStatus, ProcessOutput, ProcessSpec};
use swactor_process_context::{
    BootstrapFailure, ContextualProcessCommand, ContextualProcessOutput,
    ContextualProcessOutputConfig, ContextualProcessSpec as RuntimeContextualProcessSpec,
    ExecutionIdentity as RuntimeExecutionIdentity, send_contextual_process_command,
};
use swactor_process_context::{
    ContextProvisioner, ContextualProcessSpawner, DataPlaneProvisioner, DataPlaneProvisionerConfig,
};

pub struct MyelinChildRouteRegistrar {
    route_view: RouteView,
    pinned_routes: RouteView,
    route_binder: Arc<OutboxRouteBinder>,
    retained_sources: Mutex<HashMap<ActorAddress, (NodeId, u64)>>,
    runtime: Runtime,
    directory: ActorAddress,
    connections: ConnectionObserver,
}

impl MyelinChildRouteRegistrar {
    pub fn new(
        route_view: RouteView,
        pinned_routes: RouteView,
        route_binder: Arc<OutboxRouteBinder>,
        runtime: Runtime,
        directory: ActorAddress,
        connections: ConnectionObserver,
    ) -> Self {
        Self {
            route_view,
            pinned_routes,
            route_binder,
            retained_sources: Mutex::new(HashMap::new()),
            runtime,
            directory,
            connections,
        }
    }

    fn register_route(&self, actor: ActorAddress, node_bytes: [u8; 32]) -> Result<(), String> {
        let node = NodeId(node_bytes);
        self.pinned_routes
            .write()
            .map_err(|_| "pinned route view is poisoned".to_owned())?
            .insert(actor, node);
        self.route_view
            .write()
            .map_err(|_| "route view is poisoned".to_owned())?
            .insert(actor, node);
        self.route_binder.ensure_routable(actor);
        Ok(())
    }

    fn revoke_route(&self, actor: ActorAddress) -> Result<(), String> {
        self.pinned_routes
            .write()
            .map_err(|_| "pinned route view is poisoned".to_owned())?
            .remove(&actor);
        self.route_view
            .write()
            .map_err(|_| "route view is poisoned".to_owned())?
            .remove(&actor);
        self.route_binder.remove_route(&actor);
        Ok(())
    }
}

impl HostRouteRegistrar for MyelinChildRouteRegistrar {
    fn register_child(
        &self,
        child_session: ActorAddress,
        child_node: [u8; 32],
    ) -> Result<(), String> {
        self.register_route(child_session, child_node)
    }

    fn revoke_child(&self, child_session: ActorAddress) -> Result<(), String> {
        self.revoke_route(child_session)
    }

    fn retain_source(&self, source: ActorAddress, source_node: [u8; 32]) -> Result<(), String> {
        let node = NodeId(source_node);
        let mut retained = self
            .retained_sources
            .lock()
            .map_err(|_| "retained source view is poisoned".to_owned())?;
        let fresh = !retained.contains_key(&source);
        let count = retained
            .get(&source)
            .map_or(0, |(_, count)| *count)
            .checked_add(1)
            .ok_or_else(|| "retained source claim count overflowed".to_owned())?;
        if fresh {
            let mut pinned = self
                .pinned_routes
                .write()
                .map_err(|_| "pinned route view is poisoned".to_owned())?;
            let mut routes = self
                .route_view
                .write()
                .map_err(|_| "route view is poisoned".to_owned())?;
            pinned.insert(source, node);
            routes.insert(source, node);
            drop(routes);
            drop(pinned);
            self.route_binder.ensure_routable(source);
        }
        retained.insert(source, (node, count));
        Ok(())
    }

    fn release_source(&self, source: ActorAddress) {
        // Keep the claim count and pin transition under the same lock: a new
        // transfer must not retain between the final decrement and pin removal.
        let Ok(mut retained) = self.retained_sources.lock() else {
            return;
        };
        let Some((_, count)) = retained.get_mut(&source) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            retained.remove(&source);
            // Source actors publish an ordinary distribution-directory claim.
            // Dropping a temporary reader pin must not delete that independently
            // owned route: the namespace authority still needs it to deliver
            // Retire after the last reader releases its binding.
            if let Ok(mut pinned) = self.pinned_routes.write() {
                pinned.remove(&source);
            }
        }
    }

    fn is_routable(&self, actor: ActorAddress) -> bool {
        self.pinned_routes
            .read()
            .is_ok_and(|routes| routes.contains_key(&actor))
            || self
                .route_view
                .read()
                .is_ok_and(|routes| routes.contains_key(&actor))
    }

    fn watch_route(
        &self,
        actor: ActorAddress,
        changed: Arc<dyn Fn() + Send + Sync>,
    ) -> Option<HostRouteWatch> {
        let routes = Arc::clone(&self.route_view);
        let connections = self.connections.clone();
        // Both watchers deliver an initial observation. Compare the exact
        // route/connection generation to coalesce them and unrelated changes.
        let observed = parking_lot::Mutex::new(None);
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let route = routes
                .read()
                .ok()
                .and_then(|routes| routes.get(&actor).copied());
            let current = (route, route.and_then(|node| connections.generation(&node)));
            let mut observed = observed.lock();
            if observed.as_ref() == Some(&current) {
                return;
            }
            *observed = Some(current);
            drop(observed);
            changed();
        });
        let watch = self.connections.watch_connections(Arc::clone(&wake));
        // Subscription precedes the initial read; directory registration also
        // observes current state, closing the publication/registration race.
        let _ = self.runtime.send_to(
            self.directory,
            DirectoryIn::WatchRoutes {
                changed: Arc::downgrade(&wake),
            },
        );
        Some(HostRouteWatch::new((wake, watch)))
    }
}

pub struct MyelinContextualProcessConfig {
    pub runtime: Runtime,
    pub engine: EngineHandle,
    pub arena_bytes: u64,
    pub arena_alignment: u64,
    pub namespace: Option<NamespaceClient>,
    pub transfer_receiver: Option<Arc<dyn BlobTransferReceiver>>,
    pub source_sender: Option<Arc<dyn BlobTransferSender>>,
    pub source_publisher: Option<Arc<dyn BlobSourcePublisher>>,
    /// Shared with uploaded-program materialization for this runtime.
    pub route_registrar: Arc<dyn HostRouteRegistrar>,
    pub stream_transport: Option<Arc<dyn StreamTransport>>,
    pub host_endpoint: EndpointAddr,
}

pub fn build_contextual_process_spawner(
    config: MyelinContextualProcessConfig,
) -> Result<ContextualProcessSpawner, String> {
    let routing = serde_json::to_vec(&config.host_endpoint)
        .map_err(|error| format!("serialize contextual routing material: {error}"))?;
    let provisioner: Arc<dyn ContextProvisioner> =
        Arc::new(DataPlaneProvisioner::new(DataPlaneProvisionerConfig {
            runtime: config.runtime,
            engine: config.engine.clone(),
            arena_bytes: config.arena_bytes,
            arena_alignment: config.arena_alignment,
            namespace: config.namespace,
            transfer_receiver: config.transfer_receiver,
            source_sender: config.source_sender,
            source_publisher: config.source_publisher,
            route_registrar: Some(config.route_registrar),
            stream_transport: config.stream_transport,
            routing: Arc::new(move |_| Ok(routing.clone())),
        })?);
    Ok(ContextualProcessSpawner::new(config.engine, provisioner))
}

fn into_runtime_spec(spec: ContextualProcessSpec) -> Result<RuntimeContextualProcessSpec, String> {
    let parse_paths = |paths: Vec<String>| {
        paths
            .into_iter()
            .map(data_plane::path::DataPath::parse)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())
    };
    Ok(RuntimeContextualProcessSpec {
        process: ProcessSpec {
            command: spec.command,
            args: spec.args,
            env: spec.env.into_iter().collect(),
            working_dir: spec.working_dir.map(PathBuf::from),
            label: spec.label,
        },
        access: data_plane::path::SessionAccess {
            execution_id: spec.execution_id,
            read_prefixes: parse_paths(spec.read_prefixes)?,
            write_prefixes: parse_paths(spec.write_prefixes)?,
        },
        attach_deadline: Duration::from_millis(spec.attach_timeout_ms),
    })
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) enum ContextualNodeCommand {
    Spawn {
        request_id: String,
        spec: ContextualProcessSpec,
        reply_to: ActorAddress,
    },
    Stop {
        request_id: String,
        target_request_id: String,
        kill_after_ms: Option<u64>,
        reply_to: ActorAddress,
    },
    Query {
        request_id: String,
        reply_to: ActorAddress,
    },
}

fn execution_identity(identity: RuntimeExecutionIdentity) -> ExecutionIdentity {
    ExecutionIdentity {
        execution_id: identity.execution_id,
        generation: identity.generation,
    }
}

fn contextual_exit_status(status: ExitStatus) -> ContextualExitStatus {
    match status {
        ExitStatus::Code(code) => ContextualExitStatus::Code(code),
        ExitStatus::Signal(signal) => ContextualExitStatus::Signal(signal),
        ExitStatus::Unknown => ContextualExitStatus::Unknown,
    }
}

#[derive(Clone)]
pub(crate) enum ContextualProcessControllerIn {
    Command(ContextualNodeCommand),
    Observed {
        request_id: String,
        output: ContextualProcessOutput,
    },
    ProgramResolved {
        request_id: String,
        result: Result<BlobBinding, String>,
    },
    ProgramPrepared {
        request_id: String,
        result: Result<PathBuf, String>,
    },
    ResourceCensusObserved {
        sample_sequence: u64,
        response: ListActorsResponse,
    },
    ResourceCensusDeadline {
        sample_sequence: u64,
    },
}

struct ContextualOutputRelay {
    request_id: String,
    controller: ActorAddress,
}

impl swactor::actor::ActorInterface for ContextualOutputRelay {
    type Incoming = ContextualProcessOutput;
    type Response = ();

    fn handle(&mut self, ctx: &swactor::runtime::Ctx<'_>, output: Self::Incoming) {
        let terminal = matches!(
            output,
            ContextualProcessOutput::Process(
                ProcessOutput::SpawnFailed { .. }
                    | ProcessOutput::Exited { .. }
                    | ProcessOutput::Error { .. }
            )
        );
        let _ = ctx.send(
            self.controller,
            ContextualProcessControllerIn::Observed {
                request_id: self.request_id.clone(),
                output,
            },
        );
        if terminal {
            ctx.stop_self();
        }
    }
}

const PROGRAM_TRANSFER_RETRY: Duration = Duration::from_millis(100);
const PROGRAM_TRANSFER_RETRY_LIMIT: u16 = 300;

#[derive(Clone)]
pub(crate) struct ContextualProgramMaterializer {
    pub namespace_proxy: ActorAddress,
    pub receiver: Arc<dyn BlobTransferReceiver>,
    pub routes: Arc<dyn HostRouteRegistrar>,
    pub engine: EngineHandle,
    pub sender: swactor::runtime::ExternalSender,
    pub root: PathBuf,
}

struct PendingProgramSpawn {
    spec: ContextualProcessSpec,
    reply_to: ActorAddress,
}

struct ProgramNamespaceResolver {
    namespace_proxy: ActorAddress,
    source_path: data_plane::path::DataPath,
    request_id: String,
    controller: ActorAddress,
}

impl swactor::actor::ActorInterface for ProgramNamespaceResolver {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &swactor::runtime::Ctx<'_>) {
        if ctx
            .send(
                self.namespace_proxy,
                NamespaceClientIn::Request {
                    request: NamespaceRequest::Resolve {
                        path: self.source_path.clone(),
                    },
                    reply_to: ctx.self_addr(),
                },
            )
            .is_err()
        {
            let _ = ctx.send(
                self.controller,
                ContextualProcessControllerIn::ProgramResolved {
                    request_id: self.request_id.clone(),
                    result: Err("uploaded program namespace is unavailable".to_owned()),
                },
            );
            ctx.stop_self();
        }
    }

    fn handle(&mut self, ctx: &swactor::runtime::Ctx<'_>, reply: Self::Incoming) {
        let result = match reply {
            DataDirectoryOut::Resolved { result, .. } => {
                result.map_err(|error| format!("resolve uploaded program: {error}"))
            }
            other => Err(format!(
                "resolve uploaded program returned unexpected reply {other:?}"
            )),
        };
        let _ = ctx.send(
            self.controller,
            ContextualProcessControllerIn::ProgramResolved {
                request_id: self.request_id.clone(),
                result,
            },
        );
        ctx.stop_self();
    }
}

struct ProgramFileTransfer {
    request_id: String,
    controller: ActorAddress,
    source: ActorAddress,
    length: u64,
    transfer_id: BlobTransferId,
    receiver: Arc<dyn BlobTransferReceiver>,
    routes: Arc<dyn HostRouteRegistrar>,
    engine: EngineHandle,
    sender: swactor::runtime::ExternalSender,
    file: Option<File>,
    path: PathBuf,
    offer: Option<BlobTransferOffer>,
    written: u64,
    source_confirmed: bool,
    route_attempts: u16,
    route_watch: Option<HostRouteWatch>,
    route_retry: Option<ActorTimer>,
}

impl Drop for ProgramFileTransfer {
    fn drop(&mut self) {
        self.stop_route_wait();
        if let Some(offer) = self.offer.take() {
            self.receiver.cancel(&offer);
        }
        if self.file.take().is_some() {
            remove_program_tree(&self.path);
        }
        self.routes.release_source(self.source);
    }
}

impl ProgramFileTransfer {
    fn stop_route_wait(&mut self) {
        self.route_watch = None;
        if let Some(timer) = self.route_retry.take() {
            timer.cancel();
        }
    }

    fn fail(&mut self, ctx: &swactor::runtime::Ctx<'_>, error: impl Into<String>) {
        self.finish(ctx, Err(error.into()));
    }

    fn finish(&mut self, ctx: &swactor::runtime::Ctx<'_>, result: Result<PathBuf, String>) {
        self.stop_route_wait();
        if let Some(offer) = self.offer.take() {
            self.receiver.cancel(&offer);
        }
        self.file.take();
        if result.is_err() {
            remove_program_tree(&self.path);
        }
        let _ = ctx.send(
            self.controller,
            ContextualProcessControllerIn::ProgramPrepared {
                request_id: self.request_id.clone(),
                result,
            },
        );
        ctx.stop_self();
    }

    fn try_start(&mut self, ctx: &swactor::runtime::Ctx<'_>, retry: bool) {
        if retry && self.route_attempts >= PROGRAM_TRANSFER_RETRY_LIMIT {
            self.fail(
                ctx,
                "uploaded program source did not become routable before the transfer deadline",
            );
            return;
        }
        if retry {
            self.route_attempts += 1;
        }
        if self.routes.is_routable(self.source) {
            let offer = self
                .offer
                .as_ref()
                .expect("program transfer retains its offer")
                .clone();
            if ctx
                .send(self.source, BlobSourceIn::BeginTransfer { offer })
                .is_err()
            {
                self.fail(ctx, "route to uploaded program source is unavailable");
                return;
            }
        }
        if !self.source_confirmed && self.route_retry.is_none() {
            self.route_retry = Some(self.engine.send_after(
                PROGRAM_TRANSFER_RETRY,
                self.sender.clone(),
                ctx.self_addr(),
                BlobTransferEvent::RouteRetry,
            ));
        }
    }
}

impl swactor::actor::ActorInterface for ProgramFileTransfer {
    type Incoming = BlobTransferEvent;
    type Response = ();

    fn on_start(&mut self, ctx: &swactor::runtime::Ctx<'_>) {
        if self.length == 0 {
            self.finish(ctx, Ok(self.path.clone()));
            return;
        }
        match self.receiver.open(ctx.self_addr(), self.transfer_id) {
            Ok(offer) => {
                self.offer = Some(offer);
                let sender = self.sender.clone();
                let target = ctx.self_addr();
                self.route_watch = self.routes.watch_route(
                    self.source,
                    Arc::new(move || {
                        let _ = sender.send_to(target, BlobTransferEvent::RouteChanged);
                    }),
                );
                self.try_start(ctx, true);
            }
            Err(error) => self.fail(ctx, format!("open uploaded program transfer: {error}")),
        }
    }

    fn handle(&mut self, ctx: &swactor::runtime::Ctx<'_>, event: Self::Incoming) {
        match event {
            BlobTransferEvent::RouteRetry if !self.source_confirmed => {
                self.route_retry = None;
                self.try_start(ctx, true);
            }
            BlobTransferEvent::RouteChanged if !self.source_confirmed => {
                self.try_start(ctx, false);
            }
            BlobTransferEvent::Chunk { transfer_id, bytes } if transfer_id == self.transfer_id => {
                self.source_confirmed = true;
                self.stop_route_wait();
                let Some(next) = self
                    .written
                    .checked_add(bytes.len() as u64)
                    .filter(|written| *written <= self.length)
                else {
                    self.fail(
                        ctx,
                        format!("uploaded program exceeded declared length {}", self.length),
                    );
                    return;
                };
                if let Err(error) = self
                    .file
                    .as_mut()
                    .expect("live program transfer owns its file")
                    .write_all(&bytes)
                {
                    self.fail(ctx, format!("write uploaded program: {error}"));
                    return;
                }
                self.written = next;
            }
            BlobTransferEvent::Finished { transfer_id } if transfer_id == self.transfer_id => {
                self.source_confirmed = true;
                self.stop_route_wait();
                if self.written != self.length {
                    self.fail(
                        ctx,
                        format!(
                            "uploaded program length mismatch: expected {}, received {}",
                            self.length, self.written
                        ),
                    );
                    return;
                }
                if let Err(error) = self
                    .file
                    .as_mut()
                    .expect("live program transfer owns its file")
                    .flush()
                {
                    self.fail(ctx, format!("flush uploaded program: {error}"));
                    return;
                }
                self.finish(ctx, Ok(self.path.clone()));
            }
            BlobTransferEvent::Failed {
                transfer_id,
                reason,
            } if transfer_id == self.transfer_id => {
                self.fail(ctx, format!("receive uploaded program: {reason}"));
            }
            _ => {}
        }
    }
}

fn remove_program_tree(path: &Path) {
    if let Some(parent) = path.parent() {
        let _ = fs::remove_dir_all(parent);
    }
}

struct LiveExecutionState {
    request_id: String,
    process: ActorAddress,
    identity: RuntimeExecutionIdentity,
    reply_to: ActorAddress,
    started_pid: Option<u32>,
    context_ready: bool,
    staged_program: Option<PathBuf>,
}

#[derive(Clone)]
pub(crate) struct ContextualResourceProbe {
    pub runtime: Runtime,
    pub engine: EngineHandle,
    pub arena: Arc<parking_lot::Mutex<data_plane::arena::ArenaManager>>,
    pub stream_transport: Arc<iroh_driver::IrohStreamTransport>,
    pub generation: u64,
    pub iroh_node_id: String,
    pub deployment: Option<DeploymentIdentity>,
}

struct PendingResourceCensus {
    request_id: String,
    reply_to: ActorAddress,
    executions: Vec<LiveContextualExecution>,
    started: Instant,
    deadline: Instant,
    request: ListActorsRequest,
    timer: ActorTimer,
}

pub(crate) struct ContextualProcessController {
    logical_node_id: u64,
    spawner: Arc<ContextualProcessSpawner>,
    sender: swactor::runtime::ExternalSender,
    materializer: Option<ContextualProgramMaterializer>,
    resource_probe: Option<ContextualResourceProbe>,
    resource_sequence: u64,
    pending_resource_censuses: HashMap<u64, PendingResourceCensus>,
    pending_programs: HashMap<String, PendingProgramSpawn>,
    by_process: HashMap<ActorAddress, LiveExecutionState>,
    process_by_request: HashMap<String, ActorAddress>,
    /// Request IDs that reached a terminal state, newest first. A Spawn
    /// redelivered after completion (client retry or transport redelivery)
    /// must never execute again: the re-spawned child claims a bootstrap
    /// material nobody will send, hangs forever, and leaks its whole actor
    /// assembly past every census.
    finished_requests: VecDeque<String>,
}

/// Upper bound on remembered terminal requests. Campaigns issue hundreds of
/// request IDs; the bound keeps long-lived nodes bounded while covering any
/// realistic redelivery window.
const FINISHED_REQUEST_MEMORY: usize = 4096;
// The HTTP control owner allows two seconds. A local admin census that has not
// completed in this interval is stalled; fail it promptly so retries cannot
// accumulate dozens of orphaned censuses behind one overloaded worker.
const RESOURCE_CENSUS_DEADLINE: Duration = Duration::from_millis(500);

impl ContextualProcessController {
    pub(crate) fn new(
        logical_node_id: u64,
        spawner: Arc<ContextualProcessSpawner>,
        sender: swactor::runtime::ExternalSender,
    ) -> Self {
        Self {
            logical_node_id,
            spawner,
            sender,
            materializer: None,
            resource_probe: None,
            resource_sequence: 0,
            pending_resource_censuses: HashMap::new(),
            pending_programs: HashMap::new(),
            by_process: HashMap::new(),
            process_by_request: HashMap::new(),
            finished_requests: VecDeque::new(),
        }
    }
    pub(crate) fn with_program_materializer(
        mut self,
        materializer: ContextualProgramMaterializer,
    ) -> Self {
        self.materializer = Some(materializer);
        self
    }

    pub(crate) fn with_resource_probe(mut self, probe: ContextualResourceProbe) -> Self {
        self.resource_probe = Some(probe);
        self
    }

    fn emit(
        &self,
        ctx: &swactor::runtime::Ctx<'_>,
        reply_to: ActorAddress,
        request_id: String,
        event: ContextualProcessEventKind,
    ) {
        let _ = ctx.send(
            reply_to,
            crate::orchestration::actor::OrchestratorMsg::ContextualEvent(ContextualProcessEvent {
                request_id,
                logical_node_id: self.logical_node_id,
                event,
            }),
        );
    }

    fn spawn(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: String,
        mut spec: ContextualProcessSpec,
        reply_to: ActorAddress,
    ) {
        if self.process_by_request.contains_key(&request_id)
            || self.pending_programs.contains_key(&request_id)
        {
            self.emit(
                ctx,
                reply_to,
                request_id,
                ContextualProcessEventKind::SpawnRejected {
                    error: "contextual request ID is already live".to_owned(),
                },
            );
            return;
        }
        if self
            .finished_requests
            .iter()
            .any(|finished| finished == &request_id)
        {
            self.emit(
                ctx,
                reply_to,
                request_id,
                ContextualProcessEventKind::SpawnRejected {
                    error: "contextual request ID already completed; spawn redelivery is \
                            rejected instead of re-executed"
                        .to_owned(),
                },
            );
            return;
        }
        let Some(staged_program) = spec.staged_program.take() else {
            self.spawn_ready(ctx, request_id, spec, reply_to, None);
            return;
        };
        let Some(materializer) = self.materializer.clone() else {
            self.emit(
                ctx,
                reply_to,
                request_id,
                ContextualProcessEventKind::SpawnRejected {
                    error: "uploaded program materialization is unavailable on this node"
                        .to_owned(),
                },
            );
            return;
        };
        let source_path = match data_plane::path::DataPath::parse(staged_program.namespace_path) {
            Ok(path) => path,
            Err(error) => {
                self.emit(
                    ctx,
                    reply_to,
                    request_id,
                    ContextualProcessEventKind::SpawnRejected {
                        error: format!("invalid uploaded program path: {error}"),
                    },
                );
                return;
            }
        };
        self.pending_programs
            .insert(request_id.clone(), PendingProgramSpawn { spec, reply_to });
        if let Err(error) = ctx.spawn(ProgramNamespaceResolver {
            namespace_proxy: materializer.namespace_proxy,
            source_path,
            request_id: request_id.clone(),
            controller: ctx.self_addr(),
        }) {
            self.pending_programs.remove(&request_id);
            self.emit(
                ctx,
                reply_to,
                request_id,
                ContextualProcessEventKind::SpawnRejected {
                    error: format!("spawn uploaded program resolver: {error}"),
                },
            );
        }
    }

    fn spawn_ready(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: String,
        spec: ContextualProcessSpec,
        reply_to: ActorAddress,
        staged_program: Option<PathBuf>,
    ) {
        let spec = match into_runtime_spec(spec) {
            Ok(spec) => spec,
            Err(error) => {
                if let Some(path) = staged_program.as_deref() {
                    remove_program_tree(path);
                }
                self.emit(
                    ctx,
                    reply_to,
                    request_id,
                    ContextualProcessEventKind::SpawnRejected { error },
                );
                return;
            }
        };
        let relay = match ctx.spawn(ContextualOutputRelay {
            request_id: request_id.clone(),
            controller: ctx.self_addr(),
        }) {
            Ok(relay) => relay,
            Err(error) => {
                if let Some(path) = staged_program.as_deref() {
                    remove_program_tree(path);
                }
                self.emit(
                    ctx,
                    reply_to,
                    request_id,
                    ContextualProcessEventKind::SpawnRejected {
                        error: format!("spawn contextual output relay: {error}"),
                    },
                );
                return;
            }
        };
        let output = ContextualProcessOutputConfig::disabled(relay);
        match self.spawner.spawn(ctx, &self.sender, spec, output) {
            Ok(spawned) => {
                self.process_by_request
                    .insert(request_id.clone(), spawned.actor);
                self.by_process.insert(
                    spawned.actor,
                    LiveExecutionState {
                        request_id: request_id.clone(),
                        process: spawned.actor,
                        identity: spawned.identity,
                        reply_to,
                        started_pid: None,
                        context_ready: false,
                        staged_program,
                    },
                );
                self.emit(
                    ctx,
                    reply_to,
                    request_id,
                    ContextualProcessEventKind::Spawned {
                        process: spawned.actor.to_full_hex(),
                        identity: execution_identity(spawned.identity),
                    },
                );
            }
            Err(error) => {
                let _ = ctx.stop_actor(relay);
                if let Some(path) = staged_program.as_deref() {
                    remove_program_tree(path);
                }
                self.emit(
                    ctx,
                    reply_to,
                    request_id,
                    ContextualProcessEventKind::SpawnRejected {
                        error: error.to_string(),
                    },
                );
            }
        }
    }

    fn program_resolved(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: String,
        result: Result<BlobBinding, String>,
    ) {
        let Some(pending) = self.pending_programs.get(&request_id) else {
            return;
        };
        let reply_to = pending.reply_to;
        let binding = match result {
            Ok(binding) => binding,
            Err(error) => {
                let pending = self
                    .pending_programs
                    .remove(&request_id)
                    .expect("pending uploaded program exists");
                self.emit(
                    ctx,
                    pending.reply_to,
                    request_id,
                    ContextualProcessEventKind::SpawnRejected { error },
                );
                return;
            }
        };
        let Some(materializer) = self.materializer.clone() else {
            return;
        };
        let path = program_path(&materializer.root, &request_id);
        let file = path
            .parent()
            .ok_or_else(|| "uploaded program path has no parent".to_owned())
            .and_then(|parent| {
                fs::create_dir_all(parent)
                    .map_err(|error| format!("create uploaded program directory: {error}"))
            })
            .and_then(|()| {
                File::create(&path)
                    .map_err(|error| format!("create uploaded program file: {error}"))
            });
        let file = match file {
            Ok(file) => file,
            Err(error) => {
                self.pending_programs.remove(&request_id);
                remove_program_tree(&path);
                self.emit(
                    ctx,
                    reply_to,
                    request_id,
                    ContextualProcessEventKind::SpawnRejected { error },
                );
                return;
            }
        };
        // Namespace resolution carries the exact source node. Pin that route
        // for this transfer just as ordinary blob reads do, instead of waiting
        // for background directory gossip to rediscover the same fact.
        if let Err(error) = materializer
            .routes
            .retain_source(binding.source, binding.source_node)
        {
            self.pending_programs.remove(&request_id);
            drop(file);
            remove_program_tree(&path);
            self.emit(
                ctx,
                reply_to,
                request_id,
                ContextualProcessEventKind::SpawnRejected {
                    error: format!("retain uploaded program source route: {error}"),
                },
            );
            return;
        }
        let random = ActorAddress::new_random();
        let transfer_id = BlobTransferId(u64::from_le_bytes(
            random.0[..8]
                .try_into()
                .expect("actor address contains eight transfer ID bytes"),
        ));
        let transfer = ProgramFileTransfer {
            request_id: request_id.clone(),
            controller: ctx.self_addr(),
            source: binding.source,
            length: binding.length,
            transfer_id,
            receiver: materializer.receiver,
            routes: materializer.routes,
            engine: materializer.engine,
            sender: materializer.sender,
            file: Some(file),
            path: path.clone(),
            offer: None,
            written: 0,
            source_confirmed: false,
            route_attempts: 0,
            route_watch: None,
            route_retry: None,
        };
        if let Err(error) = ctx.spawn(transfer) {
            self.pending_programs.remove(&request_id);
            remove_program_tree(&path);
            self.emit(
                ctx,
                reply_to,
                request_id,
                ContextualProcessEventKind::SpawnRejected {
                    error: format!("spawn uploaded program transfer: {error}"),
                },
            );
        }
    }

    fn program_prepared(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: String,
        result: Result<PathBuf, String>,
    ) {
        let Some(mut pending) = self.pending_programs.remove(&request_id) else {
            if let Ok(path) = result {
                remove_program_tree(&path);
            }
            return;
        };
        let path = match result {
            Ok(path) => path,
            Err(error) => {
                self.emit(
                    ctx,
                    pending.reply_to,
                    request_id,
                    ContextualProcessEventKind::SpawnRejected { error },
                );
                return;
            }
        };
        pending.spec.args.push(path.display().to_string());
        if pending.spec.working_dir.is_none() {
            pending.spec.working_dir = path.parent().map(|parent| parent.display().to_string());
        }
        self.spawn_ready(ctx, request_id, pending.spec, pending.reply_to, Some(path));
    }

    fn stop(
        &self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: String,
        target_request_id: String,
        kill_after_ms: Option<u64>,
        reply_to: ActorAddress,
    ) {
        let event = if let Some(process) = self.process_by_request.get(&target_request_id).copied()
        {
            match send_contextual_process_command(
                &self.sender,
                process,
                ContextualProcessCommand::Stop {
                    kill_after: kill_after_ms.map(Duration::from_millis),
                },
            ) {
                Ok(()) => ContextualProcessEventKind::StopAccepted {
                    process: process.to_full_hex(),
                },
                Err(error) => ContextualProcessEventKind::StopRejected {
                    error: error.to_string(),
                },
            }
        } else {
            ContextualProcessEventKind::StopRejected {
                error: "contextual process is not live on this node".to_owned(),
            }
        };
        self.emit(ctx, reply_to, request_id, event);
    }

    fn query(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: String,
        reply_to: ActorAddress,
    ) {
        let mut executions = self
            .by_process
            .values()
            .map(|execution| LiveContextualExecution {
                request_id: execution.request_id.clone(),
                process: execution.process.to_full_hex(),
                identity: execution_identity(execution.identity),
                started_pid: execution.started_pid,
                context_ready: execution.context_ready,
            })
            .collect::<Vec<_>>();
        executions.sort_by(|left, right| left.request_id.cmp(&right.request_id));
        if let Some(probe) = &self.resource_probe {
            self.resource_sequence = self.resource_sequence.saturating_add(1);
            let sample_sequence = self.resource_sequence;
            let started = probe.engine.now().to_instant();
            let deadline = started + RESOURCE_CENSUS_DEADLINE;
            let request =
                match probe
                    .runtime
                    .admin()
                    .list_actors_to(ctx.self_addr(), move |response| {
                        ContextualProcessControllerIn::ResourceCensusObserved {
                            sample_sequence,
                            response,
                        }
                    }) {
                    Ok(request) => request,
                    Err(error) => {
                        self.emit(
                            ctx,
                            reply_to,
                            request_id,
                            ContextualProcessEventKind::ControlUnavailable {
                                error: format!("request actor census: {error}"),
                            },
                        );
                        return;
                    }
                };
            let timer = probe.engine.send_after(
                deadline.saturating_duration_since(probe.engine.now().to_instant()),
                self.sender.clone(),
                ctx.self_addr(),
                ContextualProcessControllerIn::ResourceCensusDeadline { sample_sequence },
            );
            self.pending_resource_censuses.insert(
                sample_sequence,
                PendingResourceCensus {
                    request_id,
                    reply_to,
                    executions,
                    started,
                    deadline,
                    request,
                    timer,
                },
            );
            return;
        }
        self.emit(
            ctx,
            reply_to,
            request_id,
            ContextualProcessEventKind::LiveExecutions {
                executions,
                resources: None,
            },
        );
    }

    fn resource_census(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        sample_sequence: u64,
        response: Option<ListActorsResponse>,
    ) {
        let Some(pending) = self.pending_resource_censuses.remove(&sample_sequence) else {
            return;
        };
        pending.timer.cancel();
        pending.request.cancel();
        let probe = self
            .resource_probe
            .as_ref()
            .expect("pending resource census has a probe");
        let response = response.filter(|_| probe.engine.now().to_instant() < pending.deadline);
        let event = if let Some(mut response) = response {
            response
                .actors
                .sort_unstable_by_key(|actor| actor.address.0);
            let actors = response
                .actors
                .into_iter()
                .map(|actor| ResourceActor {
                    address: actor.address.to_full_hex(),
                    actor_type: actor.actor_type.to_owned(),
                    worker_id: actor.worker_id as u64,
                    mailbox_depth: actor.mailbox_depth as u64,
                    poisoned: actor.status.poisoned,
                    stopping: actor.status.stopping,
                })
                .collect::<Vec<_>>();
            let arena: data_plane::arena::ArenaSample =
                probe.arena.lock().sample(sample_sequence).into();
            let arena = ResourceArena {
                seq: arena.seq,
                sample_unix_ms: arena.sample_unix_ms,
                capacity_bytes: arena.capacity_bytes,
                live_bytes: arena.live_bytes,
                free_bytes: arena.free_bytes,
                active_leases: arena.active_leases,
                pending_leases: arena.pending_leases,
                largest_free_range_bytes: arena.largest_free_range_bytes,
                allocation_failures_total: arena.allocation_failures_total,
                release_failures_total: arena.release_failures_total,
            };
            let transport = match serde_json::from_value::<ResourceTransport>(
                probe.stream_transport.resource_snapshot(),
            ) {
                Ok(transport) => transport,
                Err(error) => {
                    self.emit(
                        ctx,
                        pending.reply_to,
                        pending.request_id,
                        ContextualProcessEventKind::ControlUnavailable {
                            error: format!("invalid stream transport resource snapshot: {error}"),
                        },
                    );
                    return;
                }
            };
            let resources = WorkerResourceSnapshot {
                schema_version: SCHEMA_VERSION,
                request_id: pending.request_id.clone(),
                logical_node_id: self.logical_node_id,
                generation: probe.generation,
                sample_sequence,
                iroh_node_id: probe.iroh_node_id.clone(),
                artifact_digest: probe
                    .deployment
                    .as_ref()
                    .map(|identity| identity.artifact_digest.clone()),
                deployment_generation: probe
                    .deployment
                    .as_ref()
                    .map(|identity| identity.deployment_generation.clone()),
                actors,
                arena,
                collection_elapsed_us: probe
                    .engine
                    .now()
                    .to_instant()
                    .duration_since(pending.started)
                    .as_micros()
                    .try_into()
                    .unwrap_or(u64::MAX),
                transport,
            };
            if probe.engine.now().to_instant() < pending.deadline {
                ContextualProcessEventKind::LiveExecutions {
                    executions: pending.executions,
                    resources: Some(resources),
                }
            } else {
                ContextualProcessEventKind::ControlUnavailable {
                    error: "fresh worker actor/arena census deadline expired".to_owned(),
                }
            }
        } else {
            ContextualProcessEventKind::ControlUnavailable {
                error: "fresh worker actor/arena census deadline expired".to_owned(),
            }
        };
        self.emit(ctx, pending.reply_to, pending.request_id, event);
    }

    fn observe(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: String,
        output: ContextualProcessOutput,
    ) {
        let Some(process) = self.process_by_request.get(&request_id).copied() else {
            return;
        };
        let (reply_to, event, terminal) = {
            let Some(execution) = self.by_process.get_mut(&process) else {
                return;
            };
            let event = match output {
                ContextualProcessOutput::Process(ProcessOutput::Started { pid }) => {
                    execution.started_pid = Some(pid);
                    ContextualProcessEventKind::ProcessStarted { pid }
                }
                ContextualProcessOutput::Process(ProcessOutput::Stdout(bytes)) => {
                    ContextualProcessEventKind::Stdout { bytes }
                }
                ContextualProcessOutput::Process(ProcessOutput::Stderr(bytes)) => {
                    ContextualProcessEventKind::Stderr { bytes }
                }
                ContextualProcessOutput::Process(ProcessOutput::SpawnFailed { error }) => {
                    ContextualProcessEventKind::SpawnFailed { error }
                }
                ContextualProcessOutput::Process(ProcessOutput::Exited { status }) => {
                    ContextualProcessEventKind::Exited {
                        status: contextual_exit_status(status),
                    }
                }
                ContextualProcessOutput::Process(ProcessOutput::Error { error }) => {
                    ContextualProcessEventKind::ProcessError { error }
                }
                ContextualProcessOutput::ContextReady => {
                    execution.context_ready = true;
                    ContextualProcessEventKind::ContextReady
                }
                ContextualProcessOutput::BootstrapFailed { reason } => {
                    ContextualProcessEventKind::BootstrapFailed {
                        error: bootstrap_failure(&reason),
                    }
                }
            };
            (execution.reply_to, event.clone(), event.is_terminal())
        };
        self.emit(ctx, reply_to, request_id.clone(), event);
        if terminal {
            if let Some(execution) = self.by_process.remove(&process)
                && let Some(path) = execution.staged_program
            {
                remove_program_tree(&path);
            }
            self.process_by_request.remove(&request_id);
            self.finished_requests.push_front(request_id.clone());
            while self.finished_requests.len() > FINISHED_REQUEST_MEMORY {
                self.finished_requests.pop_back();
            }
        }
    }
}

fn program_path(root: &Path, request_id: &str) -> PathBuf {
    let digest = blake3::hash(request_id.as_bytes()).to_hex();
    root.join(digest.as_str()).join("program.py")
}

fn bootstrap_failure(failure: &BootstrapFailure) -> String {
    match failure {
        BootstrapFailure::Provisioning(error)
        | BootstrapFailure::ClaimRejected(error)
        | BootstrapFailure::Attachment(error)
        | BootstrapFailure::ProcessErrorBeforeReady(error)
        | BootstrapFailure::SessionFault(error) => error.clone(),
        BootstrapFailure::StopRequested => "stop requested".to_owned(),
        BootstrapFailure::ChannelClosed => "bootstrap channel closed".to_owned(),
        BootstrapFailure::AttachmentDeadline => "attachment deadline exceeded".to_owned(),
        BootstrapFailure::ProcessExitedBeforeReady => {
            "process exited before context ready".to_owned()
        }
    }
}

impl Drop for ContextualProcessController {
    fn drop(&mut self) {
        for pending in self.pending_resource_censuses.values() {
            pending.timer.cancel();
            pending.request.cancel();
        }
    }
}

impl swactor::actor::ActorInterface for ContextualProcessController {
    type Incoming = ContextualProcessControllerIn;
    type Response = ();

    fn handle(&mut self, ctx: &swactor::runtime::Ctx<'_>, message: Self::Incoming) {
        match message {
            ContextualProcessControllerIn::Command(ContextualNodeCommand::Spawn {
                request_id,
                spec,
                reply_to,
            }) => self.spawn(ctx, request_id, spec, reply_to),
            ContextualProcessControllerIn::Command(ContextualNodeCommand::Stop {
                request_id,
                target_request_id,
                kill_after_ms,
                reply_to,
            }) => self.stop(ctx, request_id, target_request_id, kill_after_ms, reply_to),
            ContextualProcessControllerIn::Command(ContextualNodeCommand::Query {
                request_id,
                reply_to,
            }) => self.query(ctx, request_id, reply_to),
            ContextualProcessControllerIn::Observed { request_id, output } => {
                self.observe(ctx, request_id, output);
            }
            ContextualProcessControllerIn::ProgramResolved { request_id, result } => {
                self.program_resolved(ctx, request_id, result);
            }
            ContextualProcessControllerIn::ProgramPrepared { request_id, result } => {
                self.program_prepared(ctx, request_id, result);
            }
            ContextualProcessControllerIn::ResourceCensusObserved {
                sample_sequence,
                response,
            } => self.resource_census(ctx, sample_sequence, Some(response)),
            ContextualProcessControllerIn::ResourceCensusDeadline { sample_sequence } => {
                self.resource_census(ctx, sample_sequence, None);
            }
        }
    }
}

#[cfg(test)]
mod transfer_wake_tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::Weak;
    use std::sync::atomic::{AtomicBool, Ordering};
    use swactor::runtime::{RuntimeConfig, RuntimeParts};
    use swactor_engine::{Engine, SteppingBackend};

    #[derive(Default)]
    struct Routes {
        ready: AtomicBool,
        changed: Mutex<Option<Weak<dyn Fn() + Send + Sync>>>,
    }

    impl Routes {
        fn wake(&self) {
            let changed = self.changed.lock().as_ref().and_then(Weak::upgrade);
            if let Some(changed) = changed {
                changed();
            }
        }
    }

    impl HostRouteRegistrar for Routes {
        fn register_child(&self, _: ActorAddress, _: [u8; 32]) -> Result<(), String> {
            Ok(())
        }

        fn revoke_child(&self, _: ActorAddress) -> Result<(), String> {
            Ok(())
        }

        fn is_routable(&self, _: ActorAddress) -> bool {
            self.ready.load(Ordering::Acquire)
        }

        fn watch_route(
            &self,
            _: ActorAddress,
            changed: Arc<dyn Fn() + Send + Sync>,
        ) -> Option<HostRouteWatch> {
            *self.changed.lock() = Some(Arc::downgrade(&changed));
            Some(HostRouteWatch::new(changed))
        }
    }

    struct Receiver;

    impl BlobTransferReceiver for Receiver {
        fn open(
            &self,
            destination: ActorAddress,
            transfer_id: BlobTransferId,
        ) -> Result<BlobTransferOffer, String> {
            Ok(BlobTransferOffer {
                transfer_id,
                destination,
                failure_proxy: None,
                transport: Vec::new(),
            })
        }

        fn cancel(&self, _: &BlobTransferOffer) {}
    }

    fn settle(backend: &SteppingBackend) {
        for _ in 0..32 {
            backend.step();
        }
    }

    #[test]
    fn program_route_wakes_preserve_retry_budget_cadence_and_lifetime() {
        let parts = RuntimeParts::new(RuntimeConfig {
            worker_count: 1,
            ..RuntimeConfig::default()
        });
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).unwrap();
        let source = runtime.new_inbox::<BlobSourceIn>().unwrap();
        let controller = runtime
            .new_inbox::<ContextualProcessControllerIn>()
            .unwrap();
        let routes = Arc::new(Routes::default());
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("program");
        let transfer_id = BlobTransferId(17);
        let transfer = runtime
            .spawn(ProgramFileTransfer {
                request_id: "wake-budget".to_owned(),
                controller: *controller.addr(),
                source: *source.addr(),
                length: 3,
                transfer_id,
                receiver: Arc::new(Receiver),
                routes: routes.clone(),
                engine: engine.handle(),
                sender: runtime.create_sender(),
                file: Some(File::create(&path).unwrap()),
                path: path.clone(),
                offer: None,
                written: 0,
                source_confirmed: false,
                route_attempts: 0,
                route_watch: None,
                route_retry: None,
            })
            .unwrap();
        settle(&backend);
        for _ in 0..=PROGRAM_TRANSFER_RETRY_LIMIT {
            routes.wake();
            settle(&backend);
        }
        assert!(source.try_recv().is_none());
        assert!(controller.try_recv().is_none());

        // No virtual time passes: becoming routable must initiate the transfer.
        routes.ready.store(true, Ordering::Release);
        routes.wake();
        settle(&backend);
        assert!(matches!(
            source.try_recv(),
            Some(BlobSourceIn::BeginTransfer { .. })
        ));
        assert!(source.try_recv().is_none());

        // All earlier notifications still leave exactly one fallback re-drive.
        backend.advance_time(PROGRAM_TRANSFER_RETRY);
        settle(&backend);
        assert!(matches!(
            source.try_recv(),
            Some(BlobSourceIn::BeginTransfer { .. })
        ));
        assert!(source.try_recv().is_none());
        runtime
            .send_to(
                transfer,
                BlobTransferEvent::Chunk {
                    transfer_id,
                    bytes: b"bin".to_vec(),
                },
            )
            .unwrap();
        runtime
            .send_to(transfer, BlobTransferEvent::Finished { transfer_id })
            .unwrap();
        settle(&backend);
        assert!(matches!(
            controller.try_recv(),
            Some(ContextualProcessControllerIn::ProgramPrepared { result: Ok(found), .. }) if found == path
        ));
        assert_eq!(fs::read(&path).unwrap(), b"bin");
        assert!(routes.changed.lock().as_ref().unwrap().upgrade().is_none());
        backend.advance_time(PROGRAM_TRANSFER_RETRY);
        settle(&backend);
        assert!(source.try_recv().is_none());
    }
}
