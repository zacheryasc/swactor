//! Node-side job actor — the spec §5/§7 executor. It receives orchestrator
//! commands over the actor plane and runs `setup`/`run` as supervised processes
//! via `swactor-process`. Bulk bytes (workspace push / output pull) travel either
//! as chunked actor messages (the in-process test path) or over the EDGE_ALPN
//! byte transport driven by the integration layer (the real iroh path); which
//! path is used is decided by which optional edge capabilities the constructor is
//! given. Lifecycle stays on the actor plane either way.

use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::orchestrator::OrchestratorJobMsg;
use crate::wire::{
    CHUNK_SIZE, EDGE_RECORD_SIZE, JobEdgeSink, NodeJobCommand, NodeJobEvent, OutputChunk,
};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::ExternalSender;
use swactor_engine::{BlockingWorkSender, EngineHandle};
use swactor_process::{
    ExitStatus, ProcessOutput, ProcessOutputConfig, ProcessSpec, spawn_local_process,
};

/// Which supervised phase a process exit belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobPhase {
    Setup,
    Run,
}

struct PendingOutputs {
    job_id: u64,
    outputs: Vec<String>,
    deadline: Instant,
}
struct PendingRun {
    job_id: u64,
    command: String,
    env: BTreeMap<String, String>,
}

/// Application-owned bridge between a supervised job process and the shared
/// cluster data plane. The job-runner remains transport agnostic; the embedding
/// application returns only the local environment needed by the process.
pub trait JobRouteRegistrar: Send + Sync + 'static {
    fn register(&self, actor: ActorAddress, node: [u8; 32]) -> Result<(), String>;
}

pub trait JobDataPlanePort: Send + Sync + 'static {
    fn configure(&self, job_id: u64, result_peer: &str)
    -> Result<BTreeMap<String, String>, String>;

    fn session_ended(&self, _job_id: u64) {}
}

/// The node-side executor. `Incoming` is the orchestrator↔node wire command.
pub struct NodeJobActor {
    orchestrator: ActorAddress,
    workdir: PathBuf,
    sender: ExternalSender,
    job_id: u64,
    workspace_buf: Vec<u8>,
    actor_timers: Option<EngineHandle>,
    blocking_work: Option<BlockingWorkSender>,
    workspace_wait: Option<(u64, Instant)>,
    pending_outputs: Option<PendingOutputs>,
    /// Edge-mode workspace-ready flag. When set, the orchestrator pushed the
    /// workspace tar over EDGE_ALPN (drained + extracted by the integration
    /// layer); `MaterializeWorkspace` waits for it before emitting
    /// `WorkspaceMaterialized`. `None` ⇒ in-process chunk path.
    workspace_ready: Option<Arc<AtomicBool>>,
    /// Edge-mode output sink slot, filled by the integration layer once the iroh
    /// connection to the orchestrator is up. `None` ⇒ chunk path (outputs stream
    /// back as `OutputChunk` actor messages).
    output_sink: Option<Arc<Mutex<Option<Box<dyn JobEdgeSink>>>>>,
    data_plane: Option<Arc<dyn JobDataPlanePort>>,
    route_registrar: Option<Arc<dyn JobRouteRegistrar>>,
    data_plane_env: BTreeMap<String, String>,
    data_plane_error: Option<String>,
    data_plane_pending: bool,
    pending_run: Option<PendingRun>,
    assignment_pending: bool,
}

/// How long the node waits for an edge-mode workspace transfer to land before
/// faulting. Generous because the workspace is pushed over a relay.
const WORKSPACE_EDGE_WAIT: Duration = Duration::from_secs(60 * 10);
/// How long the node waits for the integration layer to arm the output edge sink.
const OUTPUT_EDGE_ARM_WAIT: Duration = Duration::from_secs(60 * 5);
/// Poll interval for edge-mode spin-waits inside the actor.
const EDGE_SPIN: Duration = Duration::from_millis(25);

impl NodeJobActor {
    pub fn new(
        orchestrator: ActorAddress,
        workdir: PathBuf,
        sender: ExternalSender,
        job_id: u64,
    ) -> Self {
        Self {
            orchestrator,
            workdir,
            sender,
            job_id,
            workspace_buf: Vec::new(),
            actor_timers: None,
            blocking_work: None,
            workspace_wait: None,
            pending_outputs: None,
            workspace_ready: None,
            output_sink: None,
            data_plane: None,
            route_registrar: None,
            data_plane_env: BTreeMap::new(),
            data_plane_error: None,
            data_plane_pending: false,
            pending_run: None,
            assignment_pending: false,
        }
    }

    /// Construct an embedded executor whose orchestrator is assigned over the
    /// wire before the first job command.
    pub fn unbound(workdir: PathBuf, sender: ExternalSender) -> Self {
        Self::new(ActorAddress::default(), workdir, sender, 0)
    }

    /// Give edge-mode waits access only to engine-owned typed actor timers.
    pub fn with_actor_timers(mut self, engine: EngineHandle) -> Self {
        self.blocking_work = Some(engine.blocking_work_sender());
        self.actor_timers = Some(engine);
        self
    }

    pub fn with_route_registrar(mut self, registrar: Arc<dyn JobRouteRegistrar>) -> Self {
        self.route_registrar = Some(registrar);
        self
    }

    /// Edge mode: workspace bytes arrive over EDGE_ALPN and are extracted by the
    /// integration layer, which sets `flag` once the workspace is on disk.
    pub fn with_workspace_ready(mut self, flag: Arc<AtomicBool>) -> Self {
        self.workspace_ready = Some(flag);
        self
    }

    /// Edge mode: ship collected outputs over EDGE_ALPN through `slot`, which the
    /// integration layer fills with a byte sink once the connection is up.
    pub fn with_output_sink_slot(mut self, slot: Arc<Mutex<Option<Box<dyn JobEdgeSink>>>>) -> Self {
        self.output_sink = Some(slot);
        self
    }

    pub fn with_data_plane(mut self, data_plane: Arc<dyn JobDataPlanePort>) -> Self {
        self.data_plane = Some(data_plane);
        self
    }

    fn begin_data_plane_config(&mut self, ctx: &Ctx, job_id: u64, result_peer: String) {
        let Some(data_plane) = self.data_plane.clone() else {
            self.data_plane_error = Some("job data-plane bridge is unavailable".to_owned());
            return;
        };
        let Some(blocking_work) = self.blocking_work.clone() else {
            self.data_plane_error =
                Some("job data-plane bridge has no blocking-I/O owner".to_owned());
            return;
        };
        let sender = self.sender.clone();
        let actor = ctx.self_addr();
        self.data_plane_pending = true;
        if blocking_work
            .submit(Box::new(move || {
                let (env, error) = match data_plane.configure(job_id, &result_peer) {
                    Ok(env) => (env, None),
                    Err(error) => (BTreeMap::new(), Some(error)),
                };
                let _ = sender.send_to(
                    actor,
                    NodeJobCommand::DataPlaneConfigured { job_id, env, error },
                );
            }))
            .is_err()
        {
            self.data_plane_pending = false;
            self.data_plane_error =
                Some("job data-plane blocking-I/O owner is unavailable".to_owned());
        }
    }

    fn emit(&self, ctx: &Ctx, event: NodeJobEvent) {
        let _ = ctx.send(self.orchestrator, OrchestratorJobMsg::NodeEvent(event));
    }

    fn acknowledge_assignment(&self, ctx: &Ctx, job_id: u64) {
        self.emit(ctx, NodeJobEvent::Assigned { job_id });
        if self.assignment_pending
            && let Some(engine) = &self.actor_timers
        {
            engine.send_after(
                EDGE_SPIN,
                self.sender.clone(),
                ctx.self_addr(),
                NodeJobCommand::CheckAssignment { job_id },
            );
        }
    }

    fn spawn_supervised(
        &self,
        ctx: &Ctx,
        phase: JobPhase,
        command: String,
        env: &BTreeMap<String, String>,
    ) {
        let relay = match ctx.spawn(ProcessExitRelay {
            orchestrator: self.orchestrator,
            phase,
            job_id: self.job_id,
            data_plane: (phase == JobPhase::Run)
                .then(|| self.data_plane.clone())
                .flatten(),
        }) {
            Ok(addr) => addr,
            Err(e) => {
                self.emit(
                    ctx,
                    NodeJobEvent::NodeFault {
                        job_id: self.job_id,
                        reason: format!("spawn relay: {e}"),
                    },
                );
                return;
            }
        };
        let spec = ProcessSpec {
            command: "bash".to_owned(),
            args: vec!["-c".to_owned(), command],
            env: env.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            working_dir: Some(self.workdir.clone()),
            label: Some(format!("job-runner-{:?}", phase).to_lowercase()),
        };
        if let Err(e) = spawn_local_process(
            ctx,
            &self.sender,
            spec,
            ProcessOutputConfig::disabled(relay),
        ) {
            self.emit(
                ctx,
                NodeJobEvent::NodeFault {
                    job_id: self.job_id,
                    reason: format!("spawn process: {e}"),
                },
            );
        }
    }

    /// Edge-mode output collection: pack every declared output into a single tar
    /// and ship it over EDGE_ALPN, then announce collection. Dropping the sink
    /// finishes the edge stream so the orchestrator observes end-of-stream.
    fn collect_outputs_edge(
        &self,
        ctx: &Ctx,
        job_id: u64,
        outputs: &[String],
        sink: Box<dyn JobEdgeSink>,
    ) {
        let bytes = match pack_outputs_tar(&self.workdir, outputs) {
            Ok(b) => b,
            Err(e) => {
                self.emit(
                    ctx,
                    NodeJobEvent::NodeFault {
                        job_id,
                        reason: format!("pack outputs: {e}"),
                    },
                );
                // Finish the stream + announce so the lifecycle FSM can terminate.
                let _ = sink.send_bytes(Vec::new());
                drop(sink);
                self.emit(ctx, NodeJobEvent::OutputsCollected { job_id });
                return;
            }
        };
        let mut send_err: Option<String> = None;
        for record in bytes.chunks(EDGE_RECORD_SIZE) {
            if let Err(e) = sink.send_bytes(record.to_vec()) {
                send_err = Some(e);
                break;
            }
        }
        if let Some(e) = send_err {
            self.emit(
                ctx,
                NodeJobEvent::NodeFault {
                    job_id,
                    reason: format!("edge output send: {e}"),
                },
            );
        }
        drop(sink);
        self.emit(ctx, NodeJobEvent::OutputsCollected { job_id });
    }

    fn schedule_edge_check(&self, ctx: &Ctx, job_id: u64, message: NodeJobCommand) -> bool {
        let Some(engine) = &self.actor_timers else {
            self.emit(
                ctx,
                NodeJobEvent::NodeFault {
                    job_id,
                    reason: "edge mode requires engine-owned actor timers".to_owned(),
                },
            );
            return false;
        };
        engine.send_after(EDGE_SPIN, self.sender.clone(), ctx.self_addr(), message);
        true
    }

    fn begin_workspace_wait(&mut self, ctx: &Ctx, job_id: u64) {
        if self
            .workspace_ready
            .as_ref()
            .is_some_and(|ready| ready.load(Ordering::Acquire))
        {
            self.emit(ctx, NodeJobEvent::WorkspaceMaterialized { job_id });
            return;
        }
        self.workspace_wait = Some((job_id, Instant::now() + WORKSPACE_EDGE_WAIT));
        if !self.schedule_edge_check(ctx, job_id, NodeJobCommand::CheckWorkspaceReady { job_id }) {
            self.workspace_wait = None;
        }
    }

    fn check_workspace_ready(&mut self, ctx: &Ctx, job_id: u64) {
        let Some((pending_job, deadline)) = self.workspace_wait else {
            return;
        };
        if pending_job != job_id {
            return;
        }
        if self
            .workspace_ready
            .as_ref()
            .is_some_and(|ready| ready.load(Ordering::Acquire))
        {
            self.workspace_wait = None;
            self.emit(ctx, NodeJobEvent::WorkspaceMaterialized { job_id });
        } else if Instant::now() >= deadline {
            self.workspace_wait = None;
            self.emit(
                ctx,
                NodeJobEvent::NodeFault {
                    job_id,
                    reason: "workspace edge transfer did not land".to_owned(),
                },
            );
        } else {
            self.schedule_edge_check(ctx, job_id, NodeJobCommand::CheckWorkspaceReady { job_id });
        }
    }

    fn take_ready_output_sink(&self) -> Option<Box<dyn JobEdgeSink>> {
        self.output_sink
            .as_ref()
            .and_then(|slot| slot.lock().take())
    }

    fn begin_output_wait(&mut self, ctx: &Ctx, job_id: u64, outputs: Vec<String>) {
        if let Some(sink) = self.take_ready_output_sink() {
            self.collect_outputs_edge(ctx, job_id, &outputs, sink);
            return;
        }
        self.pending_outputs = Some(PendingOutputs {
            job_id,
            outputs,
            deadline: Instant::now() + OUTPUT_EDGE_ARM_WAIT,
        });
        if !self.schedule_edge_check(ctx, job_id, NodeJobCommand::CheckOutputSink { job_id }) {
            self.pending_outputs = None;
        }
    }

    fn check_output_sink(&mut self, ctx: &Ctx, job_id: u64) {
        let Some(pending) = self.pending_outputs.as_ref() else {
            return;
        };
        if pending.job_id != job_id {
            return;
        }
        if let Some(sink) = self.take_ready_output_sink() {
            let pending = self.pending_outputs.take().expect("pending output state");
            self.collect_outputs_edge(ctx, job_id, &pending.outputs, sink);
        } else if Instant::now() >= pending.deadline {
            self.pending_outputs = None;
            self.emit(
                ctx,
                NodeJobEvent::NodeFault {
                    job_id,
                    reason: "output edge sink was never armed".to_owned(),
                },
            );
        } else {
            self.schedule_edge_check(ctx, job_id, NodeJobCommand::CheckOutputSink { job_id });
        }
    }
}

impl ActorInterface for NodeJobActor {
    type Incoming = NodeJobCommand;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, cmd: NodeJobCommand) {
        match cmd {
            NodeJobCommand::RegisterController { controller, node } => {
                if let Some(registrar) = &self.route_registrar
                    && registrar.register(controller, node).is_ok()
                {
                    self.orchestrator = controller;
                    let _ = ctx.send(controller, OrchestratorJobMsg::ControllerRegistered);
                }
            }
            NodeJobCommand::Assign {
                job_id,
                orchestrator,
                result_peer,
            } => {
                self.job_id = job_id;
                self.orchestrator = orchestrator;
                self.data_plane_env.clear();
                self.data_plane_error = None;
                self.data_plane_pending = false;
                self.pending_run = None;
                self.assignment_pending = true;
                if let Some(result_peer) = result_peer {
                    self.begin_data_plane_config(ctx, job_id, result_peer);
                }
                self.acknowledge_assignment(ctx, job_id);
            }
            NodeJobCommand::DataPlaneConfigured { job_id, env, error } => {
                if job_id != self.job_id {
                    return;
                }
                self.data_plane_pending = false;
                self.data_plane_env = env;
                self.data_plane_error = error;
                if let Some(pending) = self.pending_run.take() {
                    let _ = ctx.send(
                        ctx.self_addr(),
                        NodeJobCommand::RunJob {
                            job_id: pending.job_id,
                            command: pending.command,
                            env: pending.env,
                        },
                    );
                }
            }
            NodeJobCommand::CheckAssignment { job_id } => {
                if self.assignment_pending && job_id == self.job_id {
                    self.acknowledge_assignment(ctx, job_id);
                }
            }
            NodeJobCommand::MaterializeWorkspace { job_id } => {
                self.assignment_pending = false;
                if self.workspace_ready.is_some() {
                    self.begin_workspace_wait(ctx, job_id);
                } else {
                    // Chunk mode: clear the buffer; bytes arrive as WorkspaceChunk.
                    self.workspace_buf.clear();
                }
            }
            NodeJobCommand::WorkspaceChunk {
                job_id, data, eof, ..
            } => {
                self.workspace_buf.extend_from_slice(&data);
                if eof {
                    let buf = std::mem::take(&mut self.workspace_buf);
                    match extract_tar(&buf, &self.workdir) {
                        Ok(()) => self.emit(ctx, NodeJobEvent::WorkspaceMaterialized { job_id }),
                        Err(e) => self.emit(
                            ctx,
                            NodeJobEvent::NodeFault {
                                job_id,
                                reason: format!("untar workspace: {e}"),
                            },
                        ),
                    }
                }
            }
            NodeJobCommand::RunSetup { command, env, .. } => {
                self.assignment_pending = false;
                self.spawn_supervised(ctx, JobPhase::Setup, command, &env);
            }
            NodeJobCommand::RunJob {
                job_id,
                command,
                mut env,
            } => {
                self.assignment_pending = false;
                if self.data_plane_pending {
                    if self.pending_run.is_some() {
                        self.emit(
                            ctx,
                            NodeJobEvent::NodeFault {
                                job_id,
                                reason: "duplicate run command while data-plane setup is pending"
                                    .to_owned(),
                            },
                        );
                    } else {
                        self.pending_run = Some(PendingRun {
                            job_id,
                            command,
                            env,
                        });
                    }
                    return;
                }
                if let Some(reason) = self.data_plane_error.clone() {
                    self.emit(ctx, NodeJobEvent::NodeFault { job_id, reason });
                    return;
                }
                env.extend(self.data_plane_env.clone());
                self.spawn_supervised(ctx, JobPhase::Run, command, &env);
            }
            NodeJobCommand::CollectOutputs { job_id, outputs } => {
                if self.output_sink.is_some() {
                    self.begin_output_wait(ctx, job_id, outputs);
                } else {
                    for name in &outputs {
                        let path = self.workdir.join(name);
                        let bytes = match pack_path(&path) {
                            Ok(b) => b,
                            Err(e) => {
                                self.emit(
                                    ctx,
                                    NodeJobEvent::NodeFault {
                                        job_id,
                                        reason: format!("pack output {name}: {e}"),
                                    },
                                );
                                continue;
                            }
                        };
                        stream_chunks(ctx, self.orchestrator, self.job_id, name, &bytes);
                    }
                    self.emit(ctx, NodeJobEvent::OutputsCollected { job_id });
                }
            }
            NodeJobCommand::CheckWorkspaceReady { job_id } => {
                self.check_workspace_ready(ctx, job_id);
            }
            NodeJobCommand::CheckOutputSink { job_id } => {
                self.check_output_sink(ctx, job_id);
            }
        }
    }
}

/// Relays `swactor-process` exits to the orchestrator as lifecycle events.
pub struct ProcessExitRelay {
    pub orchestrator: ActorAddress,
    pub phase: JobPhase,
    pub job_id: u64,
    pub data_plane: Option<Arc<dyn JobDataPlanePort>>,
}

impl ActorInterface for ProcessExitRelay {
    type Incoming = ProcessOutput;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, output: ProcessOutput) {
        if let ProcessOutput::Exited { status } = output {
            if let Some(data_plane) = &self.data_plane {
                data_plane.session_ended(self.job_id);
            }
            let event = match (self.phase, status) {
                (JobPhase::Setup, ExitStatus::Code(0)) => NodeJobEvent::SetupCompleted {
                    job_id: self.job_id,
                },
                (JobPhase::Setup, ExitStatus::Code(c)) => NodeJobEvent::NodeFault {
                    job_id: self.job_id,
                    reason: format!("setup exited {c}"),
                },
                (JobPhase::Setup, _) => NodeJobEvent::NodeFault {
                    job_id: self.job_id,
                    reason: "setup exited without a code".to_owned(),
                },
                (JobPhase::Run, ExitStatus::Code(c)) => NodeJobEvent::JobExited {
                    job_id: self.job_id,
                    code: c,
                },
                (JobPhase::Run, _) => NodeJobEvent::JobExited {
                    job_id: self.job_id,
                    code: 1,
                },
            };
            let _ = ctx.send(self.orchestrator, OrchestratorJobMsg::NodeEvent(event));
        }
    }
}

pub fn extract_tar(bytes: &[u8], dst: &std::path::Path) -> Result<(), String> {
    std::fs::create_dir_all(dst).map_err(|e| e.to_string())?;
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    archive.unpack(dst).map_err(|e| e.to_string())
}

fn pack_path(path: &std::path::Path) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    let mut builder = tar::Builder::new(&mut buf);
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "output".into());
    if path.is_dir() {
        builder
            .append_dir_all(&name, path)
            .map_err(|e| e.to_string())?;
    } else if path.is_file() {
        let mut f = std::fs::File::open(path).map_err(|e| e.to_string())?;
        builder
            .append_file(&name, &mut f)
            .map_err(|e| e.to_string())?;
    } else {
        // Missing outputs are skipped (best-effort): emit an empty eof chunk.
    }
    builder.finish().map_err(|e| e.to_string())?;
    drop(builder);
    Ok(buf)
}

/// Pack every declared output (relative to `workdir`) into a single tar, each
/// entry named by its declared output path. Missing outputs are skipped
/// (best-effort). Used by the edge-mode output path to ship all outputs in one
/// EDGE_ALPN stream.
fn pack_outputs_tar(workdir: &Path, outputs: &[String]) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    let mut builder = tar::Builder::new(&mut buf);
    for name in outputs {
        let path = workdir.join(name);
        if path.is_dir() {
            builder
                .append_dir_all(name, &path)
                .map_err(|e| e.to_string())?;
        } else if path.is_file() {
            let mut f = std::fs::File::open(&path).map_err(|e| e.to_string())?;
            builder
                .append_file(name, &mut f)
                .map_err(|e| e.to_string())?;
        }
        // Missing outputs are skipped (best-effort).
    }
    builder.finish().map_err(|e| e.to_string())?;
    drop(builder);
    Ok(buf)
}

fn stream_chunks(ctx: &Ctx, orchestrator: ActorAddress, job_id: u64, name: &str, bytes: &[u8]) {
    if bytes.is_empty() {
        let _ = ctx.send(
            orchestrator,
            OrchestratorJobMsg::OutputChunk(OutputChunk {
                job_id,
                name: name.to_owned(),
                seq: 0,
                data: Vec::new(),
                eof: true,
            }),
        );
        return;
    }
    let chunks: Vec<&[u8]> = bytes.chunks(CHUNK_SIZE).collect();
    let total = chunks.len() as u64;
    for (i, chunk) in chunks.iter().enumerate() {
        let _ = ctx.send(
            orchestrator,
            OrchestratorJobMsg::OutputChunk(OutputChunk {
                job_id,
                name: name.to_owned(),
                seq: i as u64,
                data: chunk.to_vec(),
                eof: i as u64 + 1 == total,
            }),
        );
    }
}
