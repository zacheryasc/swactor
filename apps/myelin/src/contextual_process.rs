use std::collections::{HashMap, VecDeque};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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
    ResourceActor, ResourceArena, ResourceExecutionOwnership, ResourceTransport, SCHEMA_VERSION,
    WorkerResourceSnapshot,
};
use swactor::actor::ActorAddress;
use swactor::admin::{ListActorsRequest, ListActorsResponse};
use swactor::runtime::Runtime;
use swactor_engine::EngineHandle;
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
    // TODO: Almost always, a mutex is a result of sloppy thinking. Identify all mutexes and lets decide the proper protocol and data structure
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

// TODO: Why these dyn objects? Seems sloppy
// TODO: Also, shouldnt contextual process logic go in that crate and not here?
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
            transitions: None,
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
    Ok(RuntimeContextualProcessSpec::event_driven(
        ProcessSpec {
            command: spec.command,
            args: spec.args,
            env: spec.env.into_iter().collect(),
            working_dir: spec.working_dir.map(PathBuf::from),
            label: spec.label,
        },
        data_plane::path::SessionAccess {
            execution_id: spec.execution_id,
            read_prefixes: parse_paths(spec.read_prefixes)?,
            write_prefixes: parse_paths(spec.write_prefixes)?,
        },
    ))
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
        #[serde(default)]
        force: bool,
        reply_to: ActorAddress,
    },
    Query {
        request_id: String,
        reply_to: ActorAddress,
    },
    Cancel {
        request_id: String,
        reply_to: ActorAddress,
    },
    AcknowledgeEvent {
        delivery_owner: ActorAddress,
        reply_to: ActorAddress,
        sequence: u64,
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
        operation: u64,
        output: ContextualProcessOutput,
    },
    OutputObserverLost {
        request_id: String,
        operation: u64,
    },
    OutputDrained {
        request_id: String,
        operation: u64,
    },
    ProgramResolved {
        request_id: String,
        result: Result<BlobBinding, String>,
        operation: u64,
    },
    ProgramPrepared {
        request_id: String,
        result: Result<PathBuf, String>,
        operation: u64,
    },
    ResourceCensusObserved {
        sample_sequence: u64,
        response: ListActorsResponse,
    },
}

struct ContextualOutputRelay {
    request_id: String,
    operation: u64,
    controller: Option<ActorAddress>,
}

impl swactor::actor::ActorInterface for ContextualOutputRelay {
    type Incoming = ContextualProcessOutput;
    type Response = ();

    fn handle(&mut self, ctx: &swactor::runtime::Ctx<'_>, output: Self::Incoming) {
        let Some(controller) = self.controller else {
            return;
        };
        let terminal = matches!(
            output,
            ContextualProcessOutput::Process(
                ProcessOutput::SpawnFailed { .. }
                    | ProcessOutput::Exited { .. }
                    | ProcessOutput::Error { .. }
            )
        );
        if matches!(
            &output,
            ContextualProcessOutput::Process(ProcessOutput::Stdout(_) | ProcessOutput::Stderr(_))
        ) {
            let _ = ctx.send(
                controller,
                ContextualProcessControllerIn::Observed {
                    request_id: self.request_id.clone(),
                    operation: self.operation,
                    output,
                },
            );
        }
        if terminal {
            let _ = ctx.send(
                controller,
                ContextualProcessControllerIn::OutputDrained {
                    request_id: self.request_id.clone(),
                    operation: self.operation,
                },
            );
            self.controller = None;
            ctx.stop_self();
        }
    }

    fn on_stop(&mut self, ctx: &swactor::runtime::Ctx<'_>) {
        if let Some(controller) = self.controller.take() {
            let _ = ctx.send(
                controller,
                ContextualProcessControllerIn::OutputObserverLost {
                    request_id: self.request_id.clone(),
                    operation: self.operation,
                },
            );
        }
    }
}

#[derive(Clone)]
pub(crate) struct ContextualProgramMaterializer {
    pub namespace_proxy: ActorAddress,
    pub receiver: Arc<dyn BlobTransferReceiver>,
    pub routes: Arc<dyn HostRouteRegistrar>,
    pub sender: swactor::runtime::ExternalSender,
    pub root: PathBuf,
}

struct PendingProgramSpawn {
    spec: ContextualProcessSpec,
    reply_to: ActorAddress,
    operation: u64,
    phase: ProgramLaunchPhase,
    child: ActorAddress,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProgramLaunchPhase {
    Resolving,
    Transferring,
}

struct ProgramNamespaceResolver {
    namespace_proxy: ActorAddress,
    source_path: data_plane::path::DataPath,
    request_id: String,
    controller: ActorAddress,
    operation: u64,
    completed: bool,
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
            self.completed = true;
            let _ = ctx.send(
                self.controller,
                ContextualProcessControllerIn::ProgramResolved {
                    request_id: self.request_id.clone(),
                    result: Err("uploaded program namespace is unavailable".to_owned()),
                    operation: self.operation,
                },
            );
            ctx.stop_self();
        }
    }

    fn handle(&mut self, ctx: &swactor::runtime::Ctx<'_>, reply: Self::Incoming) {
        if self.completed {
            return;
        }
        self.completed = true;
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
                operation: self.operation,
            },
        );
        ctx.stop_self();
    }

    fn on_stop(&mut self, ctx: &swactor::runtime::Ctx<'_>) {
        let _ = ctx.send(
            self.namespace_proxy,
            NamespaceClientIn::Cancel {
                reply_to: ctx.self_addr(),
            },
        );
        if !self.completed {
            self.completed = true;
            let _ = ctx.send(
                self.controller,
                ContextualProcessControllerIn::ProgramResolved {
                    request_id: self.request_id.clone(),
                    operation: self.operation,
                    result: Err("uploaded program resolver stopped before completion".to_owned()),
                },
            );
        }
    }
}

// TODO: I know what this does, but why is it *here* in this file?
// TODO: And it even specifies specific behavior to the blob treansfer? Thats the job of the data-plane
struct ProgramFileTransfer {
    request_id: String,
    controller: ActorAddress,
    operation: u64,
    source: ActorAddress,
    length: u64,
    transfer_id: BlobTransferId,
    receiver: Arc<dyn BlobTransferReceiver>,
    routes: Arc<dyn HostRouteRegistrar>,
    sender: swactor::runtime::ExternalSender,
    file: Option<File>,
    path: PathBuf,
    offer: Option<BlobTransferOffer>,
    written: u64,
    phase: ProgramTransferPhase,
    route_watch: Option<HostRouteWatch>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProgramTransferPhase {
    AwaitingRoute,
    Receiving,
    Completed,
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
    }

    fn fail(&mut self, ctx: &swactor::runtime::Ctx<'_>, error: impl Into<String>) {
        self.finish(ctx, Err(error.into()));
    }

    fn finish(&mut self, ctx: &swactor::runtime::Ctx<'_>, result: Result<PathBuf, String>) {
        if self.phase == ProgramTransferPhase::Completed {
            return;
        }
        self.phase = ProgramTransferPhase::Completed;
        self.stop_route_wait();
        if let Some(offer) = self.offer.take() {
            self.receiver.cancel(&offer);
        }
        self.file.take();
        if result.is_err() {
            remove_program_tree(&self.path);
        }
        let delivered = ctx
            .send(
                self.controller,
                ContextualProcessControllerIn::ProgramPrepared {
                    request_id: self.request_id.clone(),
                    result,
                    operation: self.operation,
                },
            )
            .is_ok();
        if !delivered {
            remove_program_tree(&self.path);
        }
        ctx.stop_self();
    }

    fn route_changed(&mut self, ctx: &swactor::runtime::Ctx<'_>) {
        if self.phase != ProgramTransferPhase::AwaitingRoute {
            return;
        }
        if self.routes.is_routable(self.source) {
            let offer = self
                .offer
                .as_ref()
                .expect("program transfer retains its offer")
                .clone();
            // Failed sends stay pending until a route/connection observation or
            // explicit cancellation; no periodic retry owns this transfer.
            let _ = ctx.send(self.source, BlobSourceIn::BeginTransfer { offer });
        }
    }

    fn chunk(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        transfer_id: BlobTransferId,
        bytes: Vec<u8>,
    ) {
        if transfer_id != self.transfer_id || self.phase == ProgramTransferPhase::Completed {
            return;
        }
        self.phase = ProgramTransferPhase::Receiving;
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

    fn received(&mut self, ctx: &swactor::runtime::Ctx<'_>, transfer_id: BlobTransferId) {
        if transfer_id != self.transfer_id || self.phase == ProgramTransferPhase::Completed {
            return;
        }
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

    fn failed(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        transfer_id: BlobTransferId,
        reason: String,
    ) {
        if transfer_id == self.transfer_id {
            self.fail(ctx, format!("receive uploaded program: {reason}"));
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
                self.route_changed(ctx);
            }
            Err(error) => self.fail(ctx, format!("open uploaded program transfer: {error}")),
        }
    }

    fn handle(&mut self, ctx: &swactor::runtime::Ctx<'_>, event: Self::Incoming) {
        match event {
            BlobTransferEvent::RouteChanged => self.route_changed(ctx),
            BlobTransferEvent::Chunk { transfer_id, bytes } => self.chunk(ctx, transfer_id, bytes),
            BlobTransferEvent::Finished { transfer_id } => self.received(ctx, transfer_id),
            BlobTransferEvent::Failed {
                transfer_id,
                reason,
            } => self.failed(ctx, transfer_id, reason),
            BlobTransferEvent::Cancel => self.fail(ctx, "uploaded program transfer cancelled"),
            BlobTransferEvent::RouteRetry
            | BlobTransferEvent::Allocated(_)
            | BlobTransferEvent::AllocatorFailed(_)
            | BlobTransferEvent::Sealed(_)
            | BlobTransferEvent::Released(_) => {}
        }
    }

    fn on_stop(&mut self, ctx: &swactor::runtime::Ctx<'_>) {
        if self.phase != ProgramTransferPhase::Completed {
            self.finish(
                ctx,
                Err("uploaded program transfer owner stopped".to_owned()),
            );
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
    operation: u64,
    output: ActorAddress,
    output_drained: bool,
    observer_lost: bool,
    pending_terminal: Option<ContextualProcessOutput>,
    reply_to: ActorAddress,
    started_pid: Option<u32>,
    phase: ExecutionPhase,
    staged_program: Option<PathBuf>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExecutionPhase {
    Launching,
    AwaitingContext,
    Ready,
    Stopping,
}

#[derive(Clone)]
pub(crate) struct ContextualResourceProbe {
    pub runtime: Runtime,
    pub engine: EngineHandle,
    pub arena: Arc<parking_lot::Mutex<data_plane::arena::ArenaManager>>,
    pub stream_transport: iroh_driver::IrohStreamResourceProbe,
    pub generation: u64,
    pub iroh_node_id: String,
    pub deployment: Option<DeploymentIdentity>,
}

struct PendingResourceCensus {
    request_id: String,
    reply_to: ActorAddress,
    executions: Vec<LiveContextualExecution>,
    request: ListActorsRequest,
    execution_ownership: ResourceExecutionOwnership,
}

struct PendingStop {
    request_id: String,
    reply_to: ActorAddress,
    process: ActorAddress,
}

struct PendingContextualEventDelivery {
    events: VecDeque<(u64, ContextualProcessEvent)>,
    _route_watch: Option<HostRouteWatch>,
}

#[derive(Clone)]
pub(crate) enum ContextualEventDeliveryIn {
    Enqueue {
        reply_to: ActorAddress,
        event: ContextualProcessEvent,
    },
    RouteChanged {
        reply_to: ActorAddress,
    },
    Acknowledge {
        reply_to: ActorAddress,
        sequence: u64,
    },
    Cancel {
        reply_to: ActorAddress,
        request_id: String,
    },
    Seal,
}

struct ContextualEventDelivery {
    routes: Arc<dyn HostRouteRegistrar>,
    sender: swactor::runtime::ExternalSender,
    deliveries: HashMap<ActorAddress, PendingContextualEventDelivery>,
    next_sequence: u64,
    sealed: bool,
}

impl ContextualEventDelivery {
    fn enqueue(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        reply_to: ActorAddress,
        event: ContextualProcessEvent,
    ) {
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .expect("contextual delivery identity exhausted");
        let delivery = self.deliveries.entry(reply_to).or_insert_with(|| {
            let sender = self.sender.clone();
            let owner = ctx.self_addr();
            let route_watch = self.routes.watch_route(
                reply_to,
                Arc::new(move || {
                    let _ =
                        sender.send_to(owner, ContextualEventDeliveryIn::RouteChanged { reply_to });
                }),
            );
            PendingContextualEventDelivery {
                events: VecDeque::new(),
                _route_watch: route_watch,
            }
        });
        delivery.events.push_back((sequence, event));
        if delivery.events.len() == 1 {
            self.deliver(ctx, reply_to);
        }
    }

    fn deliver(&self, ctx: &swactor::runtime::Ctx<'_>, reply_to: ActorAddress) {
        let Some(delivery) = self.deliveries.get(&reply_to) else {
            return;
        };
        let Some((sequence, observation)) = delivery.events.front() else {
            return;
        };
        // Keep ownership through transport acceptance until the recipient
        // acknowledges this exact sequence. Route/connection changes replay it.
        let _ = ctx.send(
            reply_to,
            crate::orchestration::actor::OrchestratorMsg::ContextualEvent {
                observation: observation.clone(),
                delivery_owner: Some(ctx.self_addr()),
                sequence: *sequence,
            },
        );
    }

    fn advance(&mut self, ctx: &swactor::runtime::Ctx<'_>, reply_to: ActorAddress) {
        if self
            .deliveries
            .get(&reply_to)
            .is_some_and(|delivery| delivery.events.is_empty())
        {
            self.deliveries.remove(&reply_to);
        } else {
            self.deliver(ctx, reply_to);
        }
        if self.sealed && self.deliveries.is_empty() {
            ctx.stop_self();
        }
    }

    fn acknowledged(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        reply_to: ActorAddress,
        sequence: u64,
    ) {
        let Some(delivery) = self.deliveries.get_mut(&reply_to) else {
            return;
        };
        if delivery.events.front().map(|(pending, _)| *pending) != Some(sequence) {
            return;
        }
        delivery.events.pop_front();
        self.advance(ctx, reply_to);
    }

    fn cancel(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        reply_to: ActorAddress,
        request_id: &str,
    ) {
        let Some(delivery) = self.deliveries.get_mut(&reply_to) else {
            return;
        };
        let previous = delivery.events.front().map(|(sequence, _)| *sequence);
        delivery
            .events
            .retain(|(_, event)| event.request_id != request_id);
        if delivery.events.front().map(|(sequence, _)| *sequence) != previous {
            self.advance(ctx, reply_to);
        }
    }
}

impl swactor::actor::ActorInterface for ContextualEventDelivery {
    type Incoming = ContextualEventDeliveryIn;
    type Response = ();

    fn handle(&mut self, ctx: &swactor::runtime::Ctx<'_>, message: Self::Incoming) {
        match message {
            ContextualEventDeliveryIn::Enqueue { reply_to, event } => {
                self.enqueue(ctx, reply_to, event)
            }
            ContextualEventDeliveryIn::RouteChanged { reply_to } => self.deliver(ctx, reply_to),
            ContextualEventDeliveryIn::Acknowledge { reply_to, sequence } => {
                self.acknowledged(ctx, reply_to, sequence)
            }
            ContextualEventDeliveryIn::Cancel {
                reply_to,
                request_id,
            } => self.cancel(ctx, reply_to, &request_id),
            ContextualEventDeliveryIn::Seal => {
                self.sealed = true;
                if self.deliveries.is_empty() {
                    ctx.stop_self();
                }
            }
        }
    }
}

// TODO: Why is this here?
pub(crate) struct ContextualProcessController {
    logical_node_id: u64,
    spawner: Arc<ContextualProcessSpawner>,
    sender: swactor::runtime::ExternalSender,
    materializer: Option<ContextualProgramMaterializer>,
    resource_probe: Option<ContextualResourceProbe>,
    resource_sequence: u64,
    pending_resource_censuses: HashMap<u64, PendingResourceCensus>,
    event_delivery: ActorAddress,
    next_program_operation: u64,
    pending_programs: HashMap<String, PendingProgramSpawn>,
    pending_stops: HashMap<u64, PendingStop>,
    by_process: HashMap<ActorAddress, LiveExecutionState>,
    process_by_request: HashMap<String, ActorAddress>,
    /// Request IDs that reached a terminal state, newest first. A Spawn
    /// redelivered after completion (client retry or transport redelivery)
    /// must never execute again: the re-spawned child claims a bootstrap
    /// material nobody will send, hangs forever, and leaks its whole actor
    /// assembly past every census.
    finished_requests: VecDeque<String>,
    finished_controls: VecDeque<String>,
}

/// Upper bound on remembered terminal requests. Campaigns issue hundreds of
/// request IDs; the bound keeps long-lived nodes bounded while covering any
/// realistic redelivery window.
const FINISHED_REQUEST_MEMORY: usize = 4096;

impl ContextualProcessController {
    pub(crate) fn new(
        runtime: Runtime,
        logical_node_id: u64,
        spawner: Arc<ContextualProcessSpawner>,
        sender: swactor::runtime::ExternalSender,
        routes: Arc<dyn HostRouteRegistrar>,
    ) -> Result<Self, swactor::Error> {
        let event_delivery = runtime.spawn(ContextualEventDelivery {
            routes,
            sender: sender.clone(),
            deliveries: HashMap::new(),
            next_sequence: 1,
            sealed: false,
        })?;
        Ok(Self {
            logical_node_id,
            spawner,
            sender,
            materializer: None,
            resource_probe: None,
            resource_sequence: 0,
            pending_resource_censuses: HashMap::new(),
            event_delivery,
            next_program_operation: 1,
            pending_programs: HashMap::new(),
            pending_stops: HashMap::new(),
            by_process: HashMap::new(),
            process_by_request: HashMap::new(),
            finished_requests: VecDeque::new(),
            finished_controls: VecDeque::new(),
        })
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
        &mut self,
        _ctx: &swactor::runtime::Ctx<'_>,
        reply_to: ActorAddress,
        request_id: String,
        event: ContextualProcessEventKind,
    ) {
        if matches!(event, ContextualProcessEventKind::SpawnRejected { .. })
            && !self.process_by_request.contains_key(&request_id)
            && !self.pending_programs.contains_key(&request_id)
        {
            self.remember_finished(request_id.clone());
        }
        if matches!(
            event,
            ContextualProcessEventKind::StopAccepted { .. }
                | ContextualProcessEventKind::StopRejected { .. }
                | ContextualProcessEventKind::ControlUnavailable { .. }
                | ContextualProcessEventKind::LiveExecutions { .. }
        ) {
            self.finished_controls.push_front(request_id.clone());
            if self.finished_controls.len() > FINISHED_REQUEST_MEMORY {
                self.finished_controls.pop_back();
            }
        }
        let _ = self.sender.send_to(
            self.event_delivery,
            ContextualEventDeliveryIn::Enqueue {
                reply_to,
                event: ContextualProcessEvent {
                    request_id,
                    logical_node_id: self.logical_node_id,
                    event,
                },
            },
        );
    }

    fn remember_finished(&mut self, request_id: String) {
        if self.finished_requests.contains(&request_id) {
            return;
        }
        self.finished_requests.push_front(request_id);
        if self.finished_requests.len() > FINISHED_REQUEST_MEMORY {
            self.finished_requests.pop_back();
        }
    }

    fn take_program(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: &str,
    ) -> Option<PendingProgramSpawn> {
        let pending = self.pending_programs.remove(request_id)?;
        let _ = ctx.stop_actor(pending.child);
        Some(pending)
    }

    fn fail_program(&mut self, ctx: &swactor::runtime::Ctx<'_>, request_id: String, error: String) {
        let Some(pending) = self.take_program(ctx, &request_id) else {
            return;
        };

        self.emit(
            ctx,
            pending.reply_to,
            request_id,
            ContextualProcessEventKind::SpawnRejected { error },
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
        let operation = self.next_program_operation;
        self.next_program_operation = operation
            .checked_add(1)
            .expect("program operation identity exhausted");
        match ctx.spawn(ProgramNamespaceResolver {
            namespace_proxy: materializer.namespace_proxy,
            source_path,
            request_id: request_id.clone(),
            controller: ctx.self_addr(),
            operation,
            completed: false,
        }) {
            Ok(child) => {
                self.pending_programs.insert(
                    request_id,
                    PendingProgramSpawn {
                        spec,
                        reply_to,
                        operation,
                        child,
                        phase: ProgramLaunchPhase::Resolving,
                    },
                );
            }
            Err(error) => self.emit(
                ctx,
                reply_to,
                request_id,
                ContextualProcessEventKind::SpawnRejected {
                    error: format!("spawn uploaded program resolver: {error}"),
                },
            ),
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
        let operation = self.next_program_operation;
        self.next_program_operation = operation
            .checked_add(1)
            .expect("contextual observation identity exhausted");
        let relay = match ctx.spawn(ContextualOutputRelay {
            request_id: request_id.clone(),
            operation,
            controller: Some(ctx.self_addr()),
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
        let sender = self.sender.clone();
        let owner = ctx.self_addr();
        let observed_request = request_id.clone();
        let output = ContextualProcessOutputConfig::disabled(relay).with_lifecycle_observer(
            Arc::new(move |output| {
                let _ = sender.send_to(
                    owner,
                    ContextualProcessControllerIn::Observed {
                        request_id: observed_request.clone(),
                        operation,
                        output,
                    },
                );
            }),
        );
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
                        operation,
                        output: relay,
                        output_drained: false,
                        observer_lost: false,
                        pending_terminal: None,
                        reply_to,
                        started_pid: None,
                        phase: ExecutionPhase::Launching,
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
        operation: u64,
        result: Result<BlobBinding, String>,
    ) {
        let Some(pending) = self.pending_programs.get(&request_id) else {
            return;
        };
        if pending.operation != operation || pending.phase != ProgramLaunchPhase::Resolving {
            return;
        }
        let binding = match result {
            Ok(binding) => binding,
            Err(error) => {
                self.fail_program(ctx, request_id, error);
                return;
            }
        };
        let Some(materializer) = self.materializer.clone() else {
            self.fail_program(
                ctx,
                request_id,
                "uploaded program materializer was removed".to_owned(),
            );
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
                remove_program_tree(&path);
                self.fail_program(ctx, request_id, error);
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
            drop(file);
            remove_program_tree(&path);
            self.fail_program(
                ctx,
                request_id,
                format!("retain uploaded program source route: {error}"),
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
            operation,
            source: binding.source,
            length: binding.length,
            transfer_id,
            receiver: materializer.receiver,
            routes: materializer.routes,
            sender: materializer.sender,
            file: Some(file),
            path: path.clone(),
            offer: None,
            written: 0,
            phase: ProgramTransferPhase::AwaitingRoute,
            route_watch: None,
        };
        match ctx.spawn(transfer) {
            Ok(child) => {
                let pending = self
                    .pending_programs
                    .get_mut(&request_id)
                    .expect("resolving operation retained");
                pending.child = child;
                pending.phase = ProgramLaunchPhase::Transferring;
            }
            Err(error) => {
                remove_program_tree(&path);
                self.fail_program(
                    ctx,
                    request_id,
                    format!("spawn uploaded program transfer: {error}"),
                );
            }
        }
    }

    fn program_prepared(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: String,
        operation: u64,
        result: Result<PathBuf, String>,
    ) {
        if !self
            .pending_programs
            .get(&request_id)
            .is_some_and(|pending| {
                pending.operation == operation && pending.phase == ProgramLaunchPhase::Transferring
            })
        {
            if let Ok(path) = result {
                let owned = self
                    .by_process
                    .values()
                    .any(|execution| execution.staged_program.as_ref() == Some(&path));
                if !owned && !self.pending_programs.contains_key(&request_id) {
                    remove_program_tree(&path);
                }
            }
            return;
        }
        let mut pending = self
            .take_program(ctx, &request_id)
            .expect("prepared operation retained");

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

    fn cancel(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: String,
        reply_to: ActorAddress,
    ) {
        if self
            .pending_programs
            .get(&request_id)
            .is_some_and(|pending| pending.reply_to == reply_to)
        {
            self.take_program(ctx, &request_id);
            self.remember_finished(request_id.clone());
        }
        let mut cancelled_control = false;
        self.pending_stops.retain(|_, pending| {
            let retain = pending.request_id != request_id || pending.reply_to != reply_to;
            cancelled_control |= !retain;
            retain
        });
        self.pending_resource_censuses.retain(|_, pending| {
            let retain = pending.request_id != request_id || pending.reply_to != reply_to;
            cancelled_control |= !retain;
            retain
        });
        if cancelled_control {
            self.finished_controls.push_front(request_id.clone());
            if self.finished_controls.len() > FINISHED_REQUEST_MEMORY {
                self.finished_controls.pop_back();
            }
        }
        let _ = ctx.send(
            self.event_delivery,
            ContextualEventDeliveryIn::Cancel {
                reply_to,
                request_id,
            },
        );
    }

    fn stop(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: String,
        target_request_id: String,
        force: bool,
        reply_to: ActorAddress,
    ) {
        if self.control_pending_or_finished(&request_id) {
            return;
        }
        if let Some(pending) = self.pending_programs.get(&target_request_id) {
            let process = pending.child.to_full_hex();
            self.fail_program(
                ctx,
                target_request_id,
                "contextual launch cancelled before native spawn".to_owned(),
            );
            self.emit(
                ctx,
                reply_to,
                request_id,
                ContextualProcessEventKind::StopAccepted { process },
            );
            return;
        }
        let Some(process) = self.process_by_request.get(&target_request_id).copied() else {
            self.emit(
                ctx,
                reply_to,
                request_id,
                ContextualProcessEventKind::StopRejected {
                    error: "contextual process is not live on this node".to_owned(),
                },
            );
            return;
        };
        let operation = self.next_program_operation;
        self.next_program_operation = operation
            .checked_add(1)
            .expect("contextual operation identity exhausted");
        self.pending_stops.insert(
            operation,
            PendingStop {
                request_id,
                reply_to,
                process,
            },
        );

        let command = if force {
            ContextualProcessCommand::CancelAcknowledged { operation }
        } else {
            ContextualProcessCommand::StopGracefullyAcknowledged { operation }
        };
        if let Err(error) = send_contextual_process_command(&self.sender, process, command) {
            self.stop_completed(ctx, operation, Err(error.to_string()));
        }
    }

    fn stop_completed(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        operation: u64,
        result: Result<(), String>,
    ) {
        let Some(pending) = self.pending_stops.remove(&operation) else {
            return;
        };

        let event = match result {
            Ok(()) => ContextualProcessEventKind::StopAccepted {
                process: pending.process.to_full_hex(),
            },
            Err(error) => ContextualProcessEventKind::StopRejected { error },
        };
        self.emit(ctx, pending.reply_to, pending.request_id, event);
    }

    fn control_pending_or_finished(&self, request_id: &str) -> bool {
        self.finished_controls
            .iter()
            .any(|finished| finished == request_id)
            || self
                .pending_stops
                .values()
                .any(|pending| pending.request_id == request_id)
            || self
                .pending_resource_censuses
                .values()
                .any(|pending| pending.request_id == request_id)
    }

    fn query(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: String,
        reply_to: ActorAddress,
    ) {
        if self.control_pending_or_finished(&request_id) {
            return;
        }
        let mut executions = self
            .by_process
            .values()
            .map(|execution| LiveContextualExecution {
                request_id: execution.request_id.clone(),
                process: execution.process.to_full_hex(),
                identity: execution_identity(execution.identity),
                started_pid: execution.started_pid,
                context_ready: execution.phase == ExecutionPhase::Ready,
            })
            .collect::<Vec<_>>();
        executions.sort_by(|left, right| left.request_id.cmp(&right.request_id));
        if let Some(probe) = &self.resource_probe {
            self.resource_sequence = self
                .resource_sequence
                .checked_add(1)
                .expect("resource census identity exhausted");
            let sample_sequence = self.resource_sequence;
            let execution_ownership = {
                let ownership = probe.engine.ownership_snapshot();
                ResourceExecutionOwnership {
                    tasks: ownership.tasks,
                    actor_timers: ownership.actor_timers,
                    blocking: ownership.blocking,
                }
            };
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
            self.pending_resource_censuses.insert(
                sample_sequence,
                PendingResourceCensus {
                    request_id,
                    reply_to,
                    executions,
                    request,
                    execution_ownership,
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
        mut response: ListActorsResponse,
    ) {
        let Some(pending) = self.pending_resource_censuses.remove(&sample_sequence) else {
            return;
        };
        let probe = self
            .resource_probe
            .as_ref()
            .expect("pending resource census has a probe");
        let current_ownership = probe.engine.ownership_snapshot();
        pending.request.cancel();
        if current_ownership.tasks != pending.execution_ownership.tasks
            || current_ownership.blocking != pending.execution_ownership.blocking
            || current_ownership.actor_timers != pending.execution_ownership.actor_timers
        {
            self.emit(
                ctx,
                pending.reply_to,
                pending.request_id,
                ContextualProcessEventKind::ControlUnavailable {
                    error: "worker execution ownership changed during census".to_owned(),
                },
            );
            return;
        }
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
        let arena = probe.arena.lock().sample(sample_sequence);
        let arena = ResourceArena {
            seq: arena.seq,
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
            execution_ownership: pending.execution_ownership,
            arena,
            transport,
        };
        self.emit(
            ctx,
            pending.reply_to,
            pending.request_id,
            ContextualProcessEventKind::LiveExecutions {
                executions: pending.executions,
                resources: Some(Box::new(resources)),
            },
        );
    }

    fn output_finished(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: String,
        operation: u64,
        lost: bool,
    ) {
        let Some(process) = self.process_by_request.get(&request_id).copied() else {
            return;
        };
        let Some(execution) = self.by_process.get_mut(&process) else {
            return;
        };
        if execution.operation != operation {
            return;
        }
        execution.output_drained = true;
        execution.observer_lost |= lost;
        if let Some(output) = execution.pending_terminal.take() {
            self.observe(ctx, request_id, operation, output);
        } else if lost {
            let _ = send_contextual_process_command(
                &self.sender,
                process,
                ContextualProcessCommand::Cancel,
            );
        }
    }

    fn observe(
        &mut self,
        ctx: &swactor::runtime::Ctx<'_>,
        request_id: String,
        operation: u64,
        output: ContextualProcessOutput,
    ) {
        let Some(process) = self.process_by_request.get(&request_id).copied() else {
            return;
        };
        if self
            .by_process
            .get(&process)
            .is_none_or(|execution| execution.operation != operation)
        {
            return;
        }
        if matches!(
            &output,
            ContextualProcessOutput::Process(
                ProcessOutput::SpawnFailed { .. }
                    | ProcessOutput::Exited { .. }
                    | ProcessOutput::Error { .. }
            )
        ) && let Some(execution) = self.by_process.get_mut(&process)
            && !execution.output_drained
            && !execution.observer_lost
        {
            execution.pending_terminal = Some(output);
            return;
        }
        if let ContextualProcessOutput::StopAcknowledged { operation } = output {
            if self
                .pending_stops
                .get(&operation)
                .is_some_and(|pending| pending.process == process)
            {
                self.stop_completed(ctx, operation, Ok(()));
            }
            return;
        }
        let (reply_to, event, terminal) = {
            let Some(execution) = self.by_process.get_mut(&process) else {
                return;
            };
            let event = match output {
                ContextualProcessOutput::StopAcknowledged { .. } => {
                    unreachable!("stop acknowledgements dispatched above")
                }
                ContextualProcessOutput::Process(ProcessOutput::Started { pid }) => {
                    if execution.phase != ExecutionPhase::Launching {
                        return;
                    }
                    execution.phase = ExecutionPhase::AwaitingContext;
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
                    if execution.phase != ExecutionPhase::AwaitingContext {
                        return;
                    }
                    execution.phase = ExecutionPhase::Ready;
                    ContextualProcessEventKind::ContextReady
                }
                ContextualProcessOutput::BootstrapFailed { reason } => {
                    if execution.phase == ExecutionPhase::Stopping {
                        return;
                    }
                    execution.phase = ExecutionPhase::Stopping;
                    ContextualProcessEventKind::BootstrapFailed {
                        error: bootstrap_failure(&reason),
                    }
                }
            };
            let terminal = event.is_terminal();
            (execution.reply_to, event, terminal)
        };
        self.emit(ctx, reply_to, request_id.clone(), event);
        if terminal {
            let operations: Vec<_> = self
                .pending_stops
                .iter()
                .filter_map(|(operation, pending)| {
                    (pending.process == process).then_some(*operation)
                })
                .collect();
            for operation in operations {
                self.stop_completed(
                    ctx,
                    operation,
                    Err("contextual process terminated before stop acknowledgement".to_owned()),
                );
            }
            if let Some(execution) = self.by_process.remove(&process) {
                let _ = ctx.stop_actor(execution.output);
                if let Some(path) = execution.staged_program {
                    remove_program_tree(&path);
                }
            }
            self.process_by_request.remove(&request_id);
            self.remember_finished(request_id);
        }
    }
}

fn program_path(root: &Path, request_id: &str) -> PathBuf {
    let digest = blake3::hash(request_id.as_bytes()).to_hex();
    root.join(digest.as_str()).join("program")
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
            pending.request.cancel();
        }
        let _ = self
            .sender
            .send_to(self.event_delivery, ContextualEventDeliveryIn::Seal);
    }
}

impl swactor::actor::ActorInterface for ContextualProcessController {
    type Incoming = ContextualProcessControllerIn;
    type Response = ();

    fn on_stop(&mut self, ctx: &swactor::runtime::Ctx<'_>) {
        for (_, pending) in std::mem::take(&mut self.pending_stops) {
            self.emit(
                ctx,
                pending.reply_to,
                pending.request_id,
                ContextualProcessEventKind::StopRejected {
                    error: "contextual controller stopped before stop acknowledgement".to_owned(),
                },
            );
        }
        for (request_id, pending) in std::mem::take(&mut self.pending_programs) {
            let _ = ctx.stop_actor(pending.child);
            self.emit(
                ctx,
                pending.reply_to,
                request_id,
                ContextualProcessEventKind::SpawnRejected {
                    error: "contextual controller stopped during launch".to_owned(),
                },
            );
        }
        for (_, pending) in std::mem::take(&mut self.pending_resource_censuses) {
            pending.request.cancel();
            self.emit(
                ctx,
                pending.reply_to,
                pending.request_id,
                ContextualProcessEventKind::ControlUnavailable {
                    error: "contextual controller stopped during census".to_owned(),
                },
            );
        }
        for (_, execution) in std::mem::take(&mut self.by_process) {
            let _ = send_contextual_process_command(
                &self.sender,
                execution.process,
                ContextualProcessCommand::Cancel,
            );
            self.emit(
                ctx,
                execution.reply_to,
                execution.request_id,
                ContextualProcessEventKind::ProcessError {
                    error: "contextual controller stopped".to_owned(),
                },
            );
            if let Some(path) = execution.staged_program {
                remove_program_tree(&path);
            }
        }
        self.process_by_request.clear();
        let _ = self
            .sender
            .send_to(self.event_delivery, ContextualEventDeliveryIn::Seal);
    }

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
                force,
                reply_to,
            }) => self.stop(ctx, request_id, target_request_id, force, reply_to),
            ContextualProcessControllerIn::Command(ContextualNodeCommand::Query {
                request_id,
                reply_to,
            }) => self.query(ctx, request_id, reply_to),
            ContextualProcessControllerIn::Command(ContextualNodeCommand::Cancel {
                request_id,
                reply_to,
            }) => self.cancel(ctx, request_id, reply_to),
            ContextualProcessControllerIn::Command(ContextualNodeCommand::AcknowledgeEvent {
                delivery_owner,
                reply_to,
                sequence,
            }) => {
                let _ = ctx.send(
                    delivery_owner,
                    ContextualEventDeliveryIn::Acknowledge { reply_to, sequence },
                );
            }
            ContextualProcessControllerIn::Observed {
                request_id,
                operation,
                output,
            } => {
                self.observe(ctx, request_id, operation, output);
            }
            ContextualProcessControllerIn::OutputObserverLost {
                request_id,
                operation,
            } => {
                self.output_finished(ctx, request_id, operation, true);
            }
            ContextualProcessControllerIn::OutputDrained {
                request_id,
                operation,
            } => {
                self.output_finished(ctx, request_id, operation, false);
            }
            ContextualProcessControllerIn::ProgramResolved {
                request_id,
                operation,
                result,
            } => {
                self.program_resolved(ctx, request_id, operation, result);
            }
            ContextualProcessControllerIn::ProgramPrepared {
                request_id,
                operation,
                result,
            } => {
                self.program_prepared(ctx, request_id, operation, result);
            }
            ContextualProcessControllerIn::ResourceCensusObserved {
                sample_sequence,
                response,
            } => self.resource_census(ctx, sample_sequence, response),
        }
    }
}

#[cfg(test)]
mod contextual_event_tests {
    use super::*;
    use data_plane::path::SessionAccess;
    use parking_lot::Mutex;
    use std::any::Any;
    use std::sync::Weak;
    use std::sync::atomic::{AtomicBool, Ordering};
    use swactor::runtime::RemoteSink;
    use swactor::runtime::{RuntimeConfig, RuntimeParts};
    use swactor_engine::{Engine, SteppingBackend};
    use swactor_process_context::ProvisionedContext;

    #[derive(Default)]
    struct Routes {
        ready: AtomicBool,
        changed: Mutex<Vec<Weak<dyn Fn() + Send + Sync>>>,
    }

    impl Routes {
        fn wake(&self) {
            let changed: Vec<_> = self
                .changed
                .lock()
                .iter()
                .filter_map(Weak::upgrade)
                .collect();
            for changed in changed {
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
            self.changed.lock().push(Arc::downgrade(&changed));
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

    #[derive(Default)]
    struct RecoveringSink {
        ready: AtomicBool,
        messages: Mutex<Vec<crate::orchestration::actor::OrchestratorMsg>>,
    }

    impl RemoteSink for RecoveringSink {
        fn send(
            &self,
            _: ActorAddress,
            message: Box<dyn Any + Send>,
        ) -> Result<(), swactor::Error> {
            if !self.ready.load(Ordering::Acquire) {
                return Err(swactor::Error::from("route unavailable"));
            }
            let message = message
                .downcast::<crate::orchestration::actor::OrchestratorMsg>()
                .map_err(|_| swactor::Error::from("unexpected contextual delivery type"))?;
            self.messages.lock().push(*message);
            Ok(())
        }
    }

    struct UnusedProvisioner;

    impl ContextProvisioner for UnusedProvisioner {
        fn provision(
            &self,
            _: RuntimeExecutionIdentity,
            _: SessionAccess,
        ) -> Result<ProvisionedContext, String> {
            Err("unused provisioner".to_owned())
        }
    }

    fn settle(backend: &SteppingBackend) {
        for _ in 0..32 {
            backend.step();
        }
    }

    #[test]
    fn contextual_events_redeliver_in_order_after_route_recovers() {
        let parts = RuntimeParts::new(RuntimeConfig {
            worker_count: 1,
            ..RuntimeConfig::default()
        });
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).unwrap();
        let sink = Arc::new(RecoveringSink::default());
        runtime.set_remote_sink(sink.clone());
        let routes = Arc::new(Routes::default());
        let spawner = Arc::new(ContextualProcessSpawner::new(
            engine.handle(),
            Arc::new(UnusedProvisioner),
        ));
        let controller = runtime
            .spawn(
                ContextualProcessController::new(
                    runtime.clone(),
                    7,
                    spawner,
                    runtime.create_sender(),
                    routes.clone(),
                )
                .unwrap(),
            )
            .unwrap();
        let reply_to = ActorAddress([73; 32]);
        for request_id in ["first", "second"] {
            runtime
                .send_to(
                    controller,
                    ContextualProcessControllerIn::Command(ContextualNodeCommand::Query {
                        request_id: request_id.to_owned(),
                        reply_to,
                    }),
                )
                .unwrap();
        }
        settle(&backend);
        assert!(sink.messages.lock().is_empty());

        sink.ready.store(true, Ordering::Release);
        routes.wake();
        settle(&backend);
        let first = sink.messages.lock().pop().expect("first event routed");
        let crate::orchestration::actor::OrchestratorMsg::ContextualEvent {
            observation,
            delivery_owner: Some(owner),
            sequence,
        } = first
        else {
            panic!("contextual event");
        };
        assert_eq!(observation.request_id, "first");
        assert!(
            sink.messages.lock().is_empty(),
            "second event waits for acknowledgement"
        );
        routes.wake();
        settle(&backend);
        assert!(
            matches!(sink.messages.lock().pop(), Some(crate::orchestration::actor::OrchestratorMsg::ContextualEvent { sequence: retry, .. }) if retry == sequence)
        );
        runtime
            .send_to(
                owner,
                ContextualEventDeliveryIn::Acknowledge {
                    reply_to,
                    sequence: sequence + 1,
                },
            )
            .unwrap();
        settle(&backend);
        assert!(
            sink.messages.lock().is_empty(),
            "stale acknowledgement cannot advance ownership"
        );
        runtime
            .send_to(
                owner,
                ContextualEventDeliveryIn::Acknowledge { reply_to, sequence },
            )
            .unwrap();
        settle(&backend);
        let second = sink
            .messages
            .lock()
            .pop()
            .expect("second event follows acknowledgement");
        let crate::orchestration::actor::OrchestratorMsg::ContextualEvent {
            observation,
            sequence,
            ..
        } = second
        else {
            panic!("contextual event");
        };
        assert_eq!(observation.request_id, "second");
        runtime
            .send_to(
                owner,
                ContextualEventDeliveryIn::Acknowledge { reply_to, sequence },
            )
            .unwrap();
        runtime
            .send_to(
                owner,
                ContextualEventDeliveryIn::Acknowledge { reply_to, sequence },
            )
            .unwrap();
        settle(&backend);
        routes.wake();
        settle(&backend);
        assert!(sink.messages.lock().is_empty());
        assert!(
            routes
                .changed
                .lock()
                .iter()
                .all(|watch| watch.upgrade().is_none())
        );

        runtime
            .send_to(
                controller,
                ContextualProcessControllerIn::Command(ContextualNodeCommand::Query {
                    request_id: "cancelled-delivery".to_owned(),
                    reply_to,
                }),
            )
            .unwrap();
        settle(&backend);
        let Some(crate::orchestration::actor::OrchestratorMsg::ContextualEvent {
            sequence: cancelled_sequence,
            ..
        }) = sink.messages.lock().pop()
        else {
            panic!("pending cancellable delivery");
        };
        runtime.stop_actor(controller).unwrap();
        settle(&backend);
        runtime
            .send_to(
                owner,
                ContextualEventDeliveryIn::Acknowledge { reply_to, sequence },
            )
            .unwrap();
        settle(&backend);
        routes.wake();
        settle(&backend);
        assert!(matches!(
            sink.messages.lock().pop(),
            Some(crate::orchestration::actor::OrchestratorMsg::ContextualEvent { sequence, .. })
                if sequence == cancelled_sequence
        ));
        runtime
            .send_to(
                owner,
                ContextualEventDeliveryIn::Cancel {
                    reply_to,
                    request_id: "cancelled-delivery".to_owned(),
                },
            )
            .unwrap();
        settle(&backend);
        assert!(!runtime.stats().actors.iter().any(|actor| actor.0 == owner));
        assert!(
            routes
                .changed
                .lock()
                .iter()
                .all(|watch| watch.upgrade().is_none())
        );
    }

    #[test]
    fn program_route_events_preserve_transfer_ownership_and_lifetime() {
        let parts = RuntimeParts::new(RuntimeConfig {
            worker_count: 1,
            ..RuntimeConfig::default()
        });
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let _engine = Engine::new(parts, backend.clone()).unwrap();
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
                request_id: "route-events".to_owned(),
                controller: *controller.addr(),
                operation: 1,
                source: *source.addr(),
                length: 3,
                transfer_id,
                receiver: Arc::new(Receiver),
                routes: routes.clone(),
                sender: runtime.create_sender(),
                file: Some(File::create(&path).unwrap()),
                path: path.clone(),
                offer: None,
                written: 0,
                phase: ProgramTransferPhase::AwaitingRoute,
                route_watch: None,
            })
            .unwrap();
        settle(&backend);
        routes.wake();
        settle(&backend);
        assert!(source.try_recv().is_none());
        assert!(controller.try_recv().is_none());

        // Route publication initiates the transfer without an independent driver.
        routes.ready.store(true, Ordering::Release);
        routes.wake();
        settle(&backend);
        assert!(matches!(
            source.try_recv(),
            Some(BlobSourceIn::BeginTransfer { .. })
        ));
        assert!(source.try_recv().is_none());

        // Reconnection re-drives the same offer until transfer completion.
        routes.wake();
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

        assert!(
            routes
                .changed
                .lock()
                .iter()
                .all(|watch| watch.upgrade().is_none())
        );
        routes.wake();
        settle(&backend);
        assert!(source.try_recv().is_none());
    }
    #[test]
    fn event_delivery_survives_controller_shutdown_and_restart_without_releasing_before_ack() {
        let parts = RuntimeParts::new(RuntimeConfig {
            worker_count: 1,
            ..RuntimeConfig::default()
        });
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).unwrap();
        let sink = Arc::new(RecoveringSink::default());
        runtime.set_remote_sink(sink.clone());
        let routes = Arc::new(Routes::default());
        let spawner = Arc::new(ContextualProcessSpawner::new(
            engine.handle(),
            Arc::new(UnusedProvisioner),
        ));
        let first_controller = runtime
            .spawn(
                ContextualProcessController::new(
                    runtime.clone(),
                    7,
                    spawner.clone(),
                    runtime.create_sender(),
                    routes.clone(),
                )
                .unwrap(),
            )
            .unwrap();
        let reply_to = ActorAddress([74; 32]);
        runtime
            .send_to(
                first_controller,
                ContextualProcessControllerIn::Command(ContextualNodeCommand::Query {
                    request_id: "before-restart".to_owned(),
                    reply_to,
                }),
            )
            .unwrap();
        settle(&backend);
        runtime.stop_actor(first_controller).unwrap();
        settle(&backend);
        let restarted = runtime
            .spawn(
                ContextualProcessController::new(
                    runtime.clone(),
                    7,
                    spawner,
                    runtime.create_sender(),
                    routes.clone(),
                )
                .unwrap(),
            )
            .unwrap();
        runtime
            .send_to(
                restarted,
                ContextualProcessControllerIn::Command(ContextualNodeCommand::Query {
                    request_id: "after-restart".to_owned(),
                    reply_to,
                }),
            )
            .unwrap();
        settle(&backend);
        assert!(sink.messages.lock().is_empty());
        sink.ready.store(true, Ordering::Release);
        routes.wake();
        settle(&backend);
        let mut received = std::mem::take(&mut *sink.messages.lock())
            .into_iter()
            .map(|message| {
                let crate::orchestration::actor::OrchestratorMsg::ContextualEvent {
                    observation,
                    delivery_owner: Some(owner),
                    sequence,
                } = message
                else {
                    panic!("event");
                };
                (observation.request_id, owner, sequence)
            })
            .collect::<Vec<_>>();
        received.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            received
                .iter()
                .map(|entry| entry.0.as_str())
                .collect::<Vec<_>>(),
            ["after-restart", "before-restart"]
        );
        assert_ne!(
            received[0].1, received[1].1,
            "restart cannot reuse delivery identity"
        );
        let previous_owner = received[1].1;
        assert!(
            runtime
                .stats()
                .actors
                .iter()
                .any(|actor| actor.0 == previous_owner),
            "stopped controller transfers pending ownership"
        );
        for (_, owner, sequence) in received {
            runtime
                .send_to(
                    owner,
                    ContextualEventDeliveryIn::Acknowledge { reply_to, sequence },
                )
                .unwrap();
        }
        settle(&backend);
        assert!(
            !runtime
                .stats()
                .actors
                .iter()
                .any(|actor| actor.0 == previous_owner),
            "sealed owner retires only after acknowledgement"
        );
        routes.wake();
        settle(&backend);
        assert!(sink.messages.lock().is_empty());
    }

    #[test]
    fn cancellation_during_namespace_resolution_fences_late_preparation_and_resolves_once() {
        let parts = RuntimeParts::new(RuntimeConfig {
            worker_count: 1,
            ..RuntimeConfig::default()
        });
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).unwrap();
        let namespace = runtime.new_inbox::<NamespaceClientIn>().unwrap();
        let events = runtime
            .new_inbox::<crate::orchestration::actor::OrchestratorMsg>()
            .unwrap();
        let root = tempfile::tempdir().unwrap();
        let spawner = Arc::new(ContextualProcessSpawner::new(
            engine.handle(),
            Arc::new(UnusedProvisioner),
        ));
        let controller = runtime
            .spawn(
                ContextualProcessController::new(
                    runtime.clone(),
                    7,
                    spawner,
                    runtime.create_sender(),
                    Arc::new(Routes::default()),
                )
                .unwrap()
                .with_program_materializer(ContextualProgramMaterializer {
                    namespace_proxy: *namespace.addr(),
                    receiver: Arc::new(Receiver),
                    routes: Arc::new(Routes::default()),
                    sender: runtime.create_sender(),
                    root: root.path().to_path_buf(),
                }),
            )
            .unwrap();
        let spec = ContextualProcessSpec {
            command: "unused-native-program".to_owned(),
            args: vec![],
            env: Default::default(),
            working_dir: None,
            label: None,
            execution_id: "cancelled".to_owned(),
            read_prefixes: vec!["/programs".to_owned()],
            write_prefixes: vec![],
            staged_program: Some(myelin_control_contract::ContextualProgramFile {
                namespace_path: "/programs/pending".to_owned(),
            }),
        };
        runtime
            .send_to(
                controller,
                ContextualProcessControllerIn::Command(ContextualNodeCommand::Spawn {
                    request_id: "launch".to_owned(),
                    spec,
                    reply_to: *events.addr(),
                }),
            )
            .unwrap();
        settle(&backend);
        let Some(NamespaceClientIn::Request {
            reply_to: resolver, ..
        }) = namespace.try_recv()
        else {
            panic!("resolution was accepted before cancellation");
        };
        runtime
            .send_to(
                controller,
                ContextualProcessControllerIn::Command(ContextualNodeCommand::Stop {
                    request_id: "cancel".to_owned(),
                    target_request_id: "launch".to_owned(),
                    force: false,
                    reply_to: *events.addr(),
                }),
            )
            .unwrap();
        settle(&backend);
        assert!(matches!(
            namespace.try_recv(),
            Some(NamespaceClientIn::Cancel { reply_to }) if reply_to == resolver
        ));
        let mut observed = Vec::new();
        for _ in 0..2 {
            let Some(crate::orchestration::actor::OrchestratorMsg::ContextualEvent {
                observation,
                delivery_owner: Some(owner),
                sequence,
            }) = events.try_recv()
            else {
                panic!("terminal operation response");
            };
            observed.push(observation);
            runtime
                .send_to(
                    owner,
                    ContextualEventDeliveryIn::Acknowledge {
                        reply_to: *events.addr(),
                        sequence,
                    },
                )
                .unwrap();
            settle(&backend);
        }
        assert!(matches!(
            observed[0].event,
            ContextualProcessEventKind::SpawnRejected { .. }
        ));
        assert_eq!(observed[0].request_id, "launch");
        assert!(matches!(
            observed[1].event,
            ContextualProcessEventKind::StopAccepted { .. }
        ));
        assert_eq!(observed[1].request_id, "cancel");
        runtime
            .send_to(
                controller,
                ContextualProcessControllerIn::ProgramResolved {
                    request_id: "launch".to_owned(),
                    operation: 1,
                    result: Err("late".to_owned()),
                },
            )
            .unwrap();
        let late_path = program_path(root.path(), "launch");
        fs::create_dir_all(late_path.parent().unwrap()).unwrap();
        fs::write(&late_path, b"late program").unwrap();
        runtime
            .send_to(
                controller,
                ContextualProcessControllerIn::ProgramPrepared {
                    request_id: "launch".to_owned(),
                    operation: 1,
                    result: Ok(late_path),
                },
            )
            .unwrap();
        settle(&backend);
        assert!(
            events.try_recv().is_none(),
            "late completion cannot resolve the launch again"
        );
        assert!(!program_path(root.path(), "launch").exists());
    }
}
