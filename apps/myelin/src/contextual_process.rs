use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use data_plane::blob_transfer::{BlobTransferReceiver, BlobTransferSender};
use data_plane::host::HostRouteRegistrar;
use data_plane::namespace::NamespaceClient;
use data_plane::source::BlobSourcePublisher;
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

struct LiveContextualExecution {
    request_id: String,
    process: ActorAddress,
    identity: ExecutionIdentity,
    reply_to: ActorAddress,
    started_pid: Option<u32>,
    context_ready: bool,
}

pub(crate) struct ContextualProcessController {
    logical_node_id: u64,
    spawner: Arc<ContextualProcessSpawner>,
    sender: swactor::runtime::ExternalSender,
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
            by_process: HashMap::new(),
            process_by_request: HashMap::new(),
        }
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
        spec: ContextualProcessSpecWire,
        reply_to: ActorAddress,
    ) {
        if self.process_by_request.contains_key(&request_id) {
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
        let spec = match spec.into_spec() {
            Ok(spec) => spec,
            Err(error) => {
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
            self.by_process.remove(&process);
            self.process_by_request.remove(&request_id);
        }
    }
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
        }
    }
}
