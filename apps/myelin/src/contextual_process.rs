use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use data_plane::blob_transfer::{
    BlobTransferEvent, BlobTransferId, BlobTransferOffer, BlobTransferReceiver, BlobTransferSender,
};
use data_plane::host::HostRouteRegistrar;
use data_plane::namespace::{
    BlobBinding, DataDirectoryOut, NamespaceClient, NamespaceClientIn, NamespaceRequest,
};
use data_plane::source::{BlobSourceIn, BlobSourcePublisher};
use data_plane::stream_transport::StreamTransport;
use distribution::transport_bridge::{OutboxRouteBinder, RouteBinder, RouteView};
use distribution::types::NodeId;
use iroh::EndpointAddr;
use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;
use swactor_engine::EngineHandle;
use swactor_process::{ExitStatus, ProcessOutput, ProcessSpec};
use swactor_process_context::{
    BootstrapFailure, ContextualProcessCommand, ContextualProcessOutput,
    ContextualProcessOutputConfig, ContextualProcessSpec, ExecutionIdentity,
    send_contextual_process_command,
};
use swactor_process_context::{
    ContextProvisioner, ContextualProcessSpawner, DataPlaneProvisioner, DataPlaneProvisionerConfig,
};

pub struct MyelinChildRouteRegistrar {
    route_view: RouteView,
    pinned_routes: RouteView,
    route_binder: Arc<OutboxRouteBinder>,
}

impl MyelinChildRouteRegistrar {
    pub fn new(
        route_view: RouteView,
        pinned_routes: RouteView,
        route_binder: Arc<OutboxRouteBinder>,
    ) -> Self {
        Self {
            route_view,
            pinned_routes,
            route_binder,
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

    fn is_routable(&self, actor: ActorAddress) -> bool {
        self.pinned_routes
            .read()
            .is_ok_and(|routes| routes.contains_key(&actor))
            || self
                .route_view
                .read()
                .is_ok_and(|routes| routes.contains_key(&actor))
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
    pub route_view: RouteView,
    pub pinned_routes: RouteView,
    pub route_binder: Arc<OutboxRouteBinder>,
    pub stream_transport: Option<Arc<dyn StreamTransport>>,
    pub host_endpoint: EndpointAddr,
}

pub fn build_contextual_process_spawner(
    config: MyelinContextualProcessConfig,
) -> Result<ContextualProcessSpawner, String> {
    let routing = serde_json::to_vec(&config.host_endpoint)
        .map_err(|error| format!("serialize contextual routing material: {error}"))?;
    let registrar: Arc<dyn HostRouteRegistrar> = Arc::new(MyelinChildRouteRegistrar::new(
        config.route_view,
        config.pinned_routes,
        config.route_binder,
    ));
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
            route_registrar: Some(registrar),
            stream_transport: config.stream_transport,
            routing: Arc::new(move |_| Ok(routing.clone())),
        })?);
    Ok(ContextualProcessSpawner::new(config.engine, provisioner))
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) struct ContextualProgramFileWire {
    pub namespace_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) struct ContextualProcessSpecWire {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub working_dir: Option<String>,
    pub label: Option<String>,
    pub execution_id: String,
    #[serde(default)]
    pub read_prefixes: Vec<String>,
    #[serde(default)]
    pub write_prefixes: Vec<String>,
    pub attach_timeout_ms: u64,
    #[serde(default)]
    pub staged_program: Option<ContextualProgramFileWire>,
}

impl ContextualProcessSpecWire {
    fn into_spec(self) -> Result<ContextualProcessSpec, String> {
        let parse_paths = |paths: Vec<String>| {
            paths
                .into_iter()
                .map(data_plane::path::DataPath::parse)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())
        };
        Ok(ContextualProcessSpec {
            process: ProcessSpec {
                command: self.command,
                args: self.args,
                env: self.env.into_iter().collect(),
                working_dir: self.working_dir.map(PathBuf::from),
                label: self.label,
            },
            access: data_plane::path::SessionAccess {
                execution_id: self.execution_id,
                read_prefixes: parse_paths(self.read_prefixes)?,
                write_prefixes: parse_paths(self.write_prefixes)?,
            },
            attach_deadline: Duration::from_millis(self.attach_timeout_ms),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) enum ContextualNodeCommand {
    Spawn {
        request_id: String,
        spec: ContextualProcessSpecWire,
        reply_to: ActorAddress,
    },
    Stop {
        request_id: String,
        process: ActorAddress,
        kill_after_ms: Option<u64>,
        reply_to: ActorAddress,
    },
    Query {
        request_id: String,
        reply_to: ActorAddress,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) struct ExecutionIdentityWire {
    pub execution_id: u64,
    pub generation: u64,
}

impl From<ExecutionIdentity> for ExecutionIdentityWire {
    fn from(identity: ExecutionIdentity) -> Self {
        Self {
            execution_id: identity.execution_id,
            generation: identity.generation,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub(crate) enum ContextualExitStatusWire {
    Code(i32),
    Signal(i32),
    Unknown,
}

impl From<ExitStatus> for ContextualExitStatusWire {
    fn from(status: ExitStatus) -> Self {
        match status {
            ExitStatus::Code(code) => Self::Code(code),
            ExitStatus::Signal(signal) => Self::Signal(signal),
            ExitStatus::Unknown => Self::Unknown,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) struct LiveContextualExecutionWire {
    pub request_id: String,
    pub process: ActorAddress,
    pub identity: ExecutionIdentityWire,
    pub started_pid: Option<u32>,
    pub context_ready: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ContextualProcessEventKindWire {
    Spawned {
        process: ActorAddress,
        identity: ExecutionIdentityWire,
    },
    SpawnRejected {
        error: String,
    },
    ProcessStarted {
        pid: u32,
    },
    ContextReady,
    Stdout {
        bytes: Vec<u8>,
    },
    Stderr {
        bytes: Vec<u8>,
    },
    SpawnFailed {
        error: String,
    },
    BootstrapFailed {
        error: String,
    },
    Exited {
        status: ContextualExitStatusWire,
    },
    ProcessError {
        error: String,
    },
    StopAccepted {
        process: ActorAddress,
    },
    StopRejected {
        error: String,
    },
    LiveExecutions {
        executions: Vec<LiveContextualExecutionWire>,
    },
    ControlUnavailable {
        error: String,
    },
}

impl ContextualProcessEventKindWire {
    pub(crate) fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::SpawnRejected { .. }
                | Self::SpawnFailed { .. }
                | Self::BootstrapFailed { .. }
                | Self::Exited { .. }
                | Self::ProcessError { .. }
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) struct ContextualProcessEventWire {
    pub request_id: String,
    pub logical_node_id: u64,
    pub event: ContextualProcessEventKindWire,
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
    spec: ContextualProcessSpecWire,
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
}

impl ProgramFileTransfer {
    fn fail(&mut self, ctx: &swactor::runtime::Ctx<'_>, error: impl Into<String>) {
        self.finish(ctx, Err(error.into()));
    }

    fn finish(&mut self, ctx: &swactor::runtime::Ctx<'_>, result: Result<PathBuf, String>) {
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

    fn try_start(&mut self, ctx: &swactor::runtime::Ctx<'_>) {
        if self.route_attempts >= PROGRAM_TRANSFER_RETRY_LIMIT {
            self.fail(
                ctx,
                "uploaded program source did not become routable before the transfer deadline",
            );
            return;
        }
        self.route_attempts += 1;
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
        if !self.source_confirmed {
            self.engine.send_after(
                PROGRAM_TRANSFER_RETRY,
                self.sender.clone(),
                ctx.self_addr(),
                BlobTransferEvent::RouteRetry,
            );
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
                self.try_start(ctx);
            }
            Err(error) => self.fail(ctx, format!("open uploaded program transfer: {error}")),
        }
    }

    fn handle(&mut self, ctx: &swactor::runtime::Ctx<'_>, event: Self::Incoming) {
        match event {
            BlobTransferEvent::RouteRetry if !self.source_confirmed => self.try_start(ctx),
            BlobTransferEvent::Chunk { transfer_id, bytes } if transfer_id == self.transfer_id => {
                self.source_confirmed = true;
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

struct LiveContextualExecution {
    request_id: String,
    process: ActorAddress,
    identity: ExecutionIdentity,
    reply_to: ActorAddress,
    started_pid: Option<u32>,
    context_ready: bool,
    staged_program: Option<PathBuf>,
}

pub(crate) struct ContextualProcessController {
    logical_node_id: u64,
    spawner: Arc<ContextualProcessSpawner>,
    sender: swactor::runtime::ExternalSender,
    materializer: Option<ContextualProgramMaterializer>,
    pending_programs: HashMap<String, PendingProgramSpawn>,
    by_process: HashMap<ActorAddress, LiveContextualExecution>,
    process_by_request: HashMap<String, ActorAddress>,
}

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
            pending_programs: HashMap::new(),
            by_process: HashMap::new(),
            process_by_request: HashMap::new(),
        }
    }
    pub(crate) fn with_program_materializer(
        mut self,
        materializer: ContextualProgramMaterializer,
    ) -> Self {
        self.materializer = Some(materializer);
        self
    }

    fn emit(
        &self,
        ctx: &swactor::runtime::Ctx<'_>,
        reply_to: ActorAddress,
        request_id: String,
        event: ContextualProcessEventKindWire,
    ) {
        let _ = ctx.send(
            reply_to,
            crate::orchestration::actor::OrchestratorMsg::ContextualEvent(
                ContextualProcessEventWire {
                    request_id,
                    logical_node_id: self.logical_node_id,
                    event,
                },
            ),
        );
    }

    fn spawn(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: String,
        mut spec: ContextualProcessSpecWire,
        reply_to: ActorAddress,
    ) {
        if self.process_by_request.contains_key(&request_id)
            || self.pending_programs.contains_key(&request_id)
        {
            self.emit(
                ctx,
                reply_to,
                request_id,
                ContextualProcessEventKindWire::SpawnRejected {
                    error: "contextual request ID is already live".to_owned(),
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
                ContextualProcessEventKindWire::SpawnRejected {
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
                    ContextualProcessEventKindWire::SpawnRejected {
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
                ContextualProcessEventKindWire::SpawnRejected {
                    error: format!("spawn uploaded program resolver: {error}"),
                },
            );
        }
    }

    fn spawn_ready(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: String,
        spec: ContextualProcessSpecWire,
        reply_to: ActorAddress,
        staged_program: Option<PathBuf>,
    ) {
        let spec = match spec.into_spec() {
            Ok(spec) => spec,
            Err(error) => {
                if let Some(path) = staged_program.as_deref() {
                    remove_program_tree(path);
                }
                self.emit(
                    ctx,
                    reply_to,
                    request_id,
                    ContextualProcessEventKindWire::SpawnRejected { error },
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
                    ContextualProcessEventKindWire::SpawnRejected {
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
                    LiveContextualExecution {
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
                    ContextualProcessEventKindWire::Spawned {
                        process: spawned.actor,
                        identity: spawned.identity.into(),
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
                    ContextualProcessEventKindWire::SpawnRejected {
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
                    ContextualProcessEventKindWire::SpawnRejected { error },
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
                    ContextualProcessEventKindWire::SpawnRejected { error },
                );
                return;
            }
        };
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
        };
        if let Err(error) = ctx.spawn(transfer) {
            self.pending_programs.remove(&request_id);
            remove_program_tree(&path);
            self.emit(
                ctx,
                reply_to,
                request_id,
                ContextualProcessEventKindWire::SpawnRejected {
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
                    ContextualProcessEventKindWire::SpawnRejected { error },
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
        process: ActorAddress,
        kill_after_ms: Option<u64>,
        reply_to: ActorAddress,
    ) {
        let event = if self.by_process.contains_key(&process) {
            match send_contextual_process_command(
                &self.sender,
                process,
                ContextualProcessCommand::Stop {
                    kill_after: kill_after_ms.map(Duration::from_millis),
                },
            ) {
                Ok(()) => ContextualProcessEventKindWire::StopAccepted { process },
                Err(error) => ContextualProcessEventKindWire::StopRejected {
                    error: error.to_string(),
                },
            }
        } else {
            ContextualProcessEventKindWire::StopRejected {
                error: "contextual process is not live on this node".to_owned(),
            }
        };
        self.emit(ctx, reply_to, request_id, event);
    }

    fn query(&self, ctx: &swactor::runtime::Ctx<'_>, request_id: String, reply_to: ActorAddress) {
        let mut executions = self
            .by_process
            .values()
            .map(|execution| LiveContextualExecutionWire {
                request_id: execution.request_id.clone(),
                process: execution.process,
                identity: execution.identity.into(),
                started_pid: execution.started_pid,
                context_ready: execution.context_ready,
            })
            .collect::<Vec<_>>();
        executions.sort_by(|left, right| left.request_id.cmp(&right.request_id));
        self.emit(
            ctx,
            reply_to,
            request_id,
            ContextualProcessEventKindWire::LiveExecutions { executions },
        );
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
                    ContextualProcessEventKindWire::ProcessStarted { pid }
                }
                ContextualProcessOutput::Process(ProcessOutput::Stdout(bytes)) => {
                    ContextualProcessEventKindWire::Stdout { bytes }
                }
                ContextualProcessOutput::Process(ProcessOutput::Stderr(bytes)) => {
                    ContextualProcessEventKindWire::Stderr { bytes }
                }
                ContextualProcessOutput::Process(ProcessOutput::SpawnFailed { error }) => {
                    ContextualProcessEventKindWire::SpawnFailed { error }
                }
                ContextualProcessOutput::Process(ProcessOutput::Exited { status }) => {
                    ContextualProcessEventKindWire::Exited {
                        status: status.into(),
                    }
                }
                ContextualProcessOutput::Process(ProcessOutput::Error { error }) => {
                    ContextualProcessEventKindWire::ProcessError { error }
                }
                ContextualProcessOutput::ContextReady => {
                    execution.context_ready = true;
                    ContextualProcessEventKindWire::ContextReady
                }
                ContextualProcessOutput::BootstrapFailed { reason } => {
                    ContextualProcessEventKindWire::BootstrapFailed {
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
                process,
                kill_after_ms,
                reply_to,
            }) => self.stop(ctx, request_id, process, kill_after_ms, reply_to),
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
        }
    }
}
