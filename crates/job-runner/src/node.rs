//! Node-side job actor — the spec §5/§7 executor. It receives orchestrator
//! commands over the actor plane and runs `setup`/`run` as supervised processes
//! via `swactor-process`. Bulk bytes (workspace push / output pull) travel either
//! as chunked actor messages (the in-process test path) or over the EDGE_ALPN
//! byte transport driven by the integration layer (the real iroh path); which
//! path is used is decided by which optional edge capabilities the constructor is
//! given. Lifecycle stays on the actor plane either way.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::ExternalSender;
use swactor_process::{
    ExitStatus, ProcessOutput, ProcessOutputConfig, ProcessSpec, spawn_local_process,
};
use crate::orchestrator::OrchestratorJobMsg;
use crate::wire::{EDGE_RECORD_SIZE, JobEdgeSink, NodeJobCommand, NodeJobEvent, OutputChunk, CHUNK_SIZE};

/// Which supervised phase a process exit belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobPhase {
    Setup,
    Run,
}

/// The node-side executor. `Incoming` is the orchestrator↔node wire command.
pub struct NodeJobActor {
    orchestrator: ActorAddress,
    workdir: PathBuf,
    sender: ExternalSender,
    job_id: u64,
    workspace_buf: Vec<u8>,
    /// Edge-mode workspace-ready flag. When set, the orchestrator pushed the
    /// workspace tar over EDGE_ALPN (drained + extracted by the integration
    /// layer); `MaterializeWorkspace` waits for it before emitting
    /// `WorkspaceMaterialized`. `None` ⇒ in-process chunk path.
    workspace_ready: Option<Arc<AtomicBool>>,
    /// Edge-mode output sink slot, filled by the integration layer once the iroh
    /// connection to the orchestrator is up. `None` ⇒ chunk path (outputs stream
    /// back as `OutputChunk` actor messages).
    output_sink: Option<Arc<Mutex<Option<Box<dyn JobEdgeSink>>>>>,
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
            workspace_ready: None,
            output_sink: None,
        }
    }

    /// Edge mode: workspace bytes arrive over EDGE_ALPN and are extracted by the
    /// integration layer, which sets `flag` once the workspace is on disk.
    pub fn with_workspace_ready(mut self, flag: Arc<AtomicBool>) -> Self {
        self.workspace_ready = Some(flag);
        self
    }

    /// Edge mode: ship collected outputs over EDGE_ALPN through `slot`, which the
    /// integration layer fills with a byte sink once the connection is up.
    pub fn with_output_sink_slot(
        mut self,
        slot: Arc<Mutex<Option<Box<dyn JobEdgeSink>>>>,
    ) -> Self {
        self.output_sink = Some(slot);
        self
    }

    fn emit(&self, ctx: &Ctx, event: NodeJobEvent) {
        let _ = ctx.send(self.orchestrator, OrchestratorJobMsg::NodeEvent(event));
    }

    fn spawn_supervised(&self, ctx: &Ctx, phase: JobPhase, command: String, env: &BTreeMap<String, String>) {
        let relay = match ctx.spawn(ProcessExitRelay {
            orchestrator: self.orchestrator,
            phase,
            job_id: self.job_id,
        }) {
            Ok(addr) => addr,
            Err(e) => {
                self.emit(ctx, NodeJobEvent::NodeFault {
                    job_id: self.job_id,
                    reason: format!("spawn relay: {e}"),
                });
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
        if let Err(e) = spawn_local_process(ctx, &self.sender, spec, ProcessOutputConfig::disabled(relay)) {
            self.emit(ctx, NodeJobEvent::NodeFault {
                job_id: self.job_id,
                reason: format!("spawn process: {e}"),
            });
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
        slot: Arc<Mutex<Option<Box<dyn JobEdgeSink>>>>,
    ) {
        let sink = match self.take_output_sink(ctx, job_id, &slot) {
            Some(sink) => sink,
            None => return, // already faulted while waiting for the sink
        };
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

    /// Take the edge output sink from the shared slot, waiting briefly for the
    /// integration layer to arm it. Emits a fault and returns `None` on timeout.
    fn take_output_sink(
        &self,
        ctx: &Ctx,
        job_id: u64,
        slot: &Arc<Mutex<Option<Box<dyn JobEdgeSink>>>>,
    ) -> Option<Box<dyn JobEdgeSink>> {
        let deadline = Instant::now() + OUTPUT_EDGE_ARM_WAIT;
        loop {
            {
                let mut guard = slot.lock();
                if guard.is_some() {
                    return guard.take();
                }
            }
            if Instant::now() >= deadline {
                self.emit(
                    ctx,
                    NodeJobEvent::NodeFault {
                        job_id,
                        reason: "output edge sink was never armed".to_owned(),
                    },
                );
                return None;
            }
            std::thread::sleep(EDGE_SPIN);
        }
    }

}

impl ActorInterface for NodeJobActor {
    type Incoming = NodeJobCommand;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, cmd: NodeJobCommand) {
        match cmd {
            NodeJobCommand::MaterializeWorkspace { job_id } => {
                if let Some(flag) = self.workspace_ready.as_ref() {
                    // Edge mode: the workspace tar traveled over EDGE_ALPN and
                    // was extracted into `workdir` by the integration layer.
                    // Wait for its readiness signal before announcing ready.
                    let deadline = Instant::now() + WORKSPACE_EDGE_WAIT;
                    while !flag.load(Ordering::Acquire) {
                        if Instant::now() >= deadline {
                            self.emit(
                                ctx,
                                NodeJobEvent::NodeFault {
                                    job_id,
                                    reason: "workspace edge transfer did not land".to_owned(),
                                },
                            );
                            return;
                        }
                        std::thread::sleep(EDGE_SPIN);
                    }
                    self.emit(ctx, NodeJobEvent::WorkspaceMaterialized { job_id });
                } else {
                    // Chunk mode: clear the buffer; bytes arrive as WorkspaceChunk.
                    self.workspace_buf.clear();
                }
            }
            NodeJobCommand::WorkspaceChunk { job_id, data, eof, .. } => {
                self.workspace_buf.extend_from_slice(&data);
                if eof {
                    let buf = std::mem::take(&mut self.workspace_buf);
                    match extract_tar(&buf, &self.workdir) {
                        Ok(()) => self.emit(ctx, NodeJobEvent::WorkspaceMaterialized { job_id }),
                        Err(e) => self.emit(ctx, NodeJobEvent::NodeFault { job_id, reason: format!("untar workspace: {e}") }),
                    }
                }
            }
            NodeJobCommand::RunSetup { command, env, .. } => {
                self.spawn_supervised(ctx, JobPhase::Setup, command, &env);
            }
            NodeJobCommand::RunJob { command, env, .. } => {
                self.spawn_supervised(ctx, JobPhase::Run, command, &env);
            }
            NodeJobCommand::CollectOutputs { job_id, outputs } => {
                if let Some(slot) = self.output_sink.clone() {
                    // Edge mode: pack all outputs into one tar and ship over
                    // EDGE_ALPN, then announce collection. Dropping the sink
                    // finishes the stream so the orchestrator sees end-of-stream.
                    self.collect_outputs_edge(ctx, job_id, &outputs, slot);
                } else {
                    for name in &outputs {
                        let path = self.workdir.join(name);
                        let bytes = match pack_path(&path) {
                            Ok(b) => b,
                            Err(e) => {
                                self.emit(ctx, NodeJobEvent::NodeFault { job_id, reason: format!("pack output {name}: {e}") });
                                continue;
                            }
                        };
                        stream_chunks(ctx, self.orchestrator, self.job_id, name, &bytes);
                    }
                    self.emit(ctx, NodeJobEvent::OutputsCollected { job_id });
                }
            }
        }
    }
}

/// Relays `swactor-process` exits to the orchestrator as lifecycle events.
pub struct ProcessExitRelay {
    pub orchestrator: ActorAddress,
    pub phase: JobPhase,
    pub job_id: u64,
}

impl ActorInterface for ProcessExitRelay {
    type Incoming = ProcessOutput;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, output: ProcessOutput) {
        if let ProcessOutput::Exited { status } = output {
            let event = match (self.phase, status) {
                (JobPhase::Setup, ExitStatus::Code(0)) => NodeJobEvent::SetupCompleted { job_id: self.job_id },
                (JobPhase::Setup, ExitStatus::Code(c)) => NodeJobEvent::NodeFault {
                    job_id: self.job_id,
                    reason: format!("setup exited {c}"),
                },
                (JobPhase::Setup, _) => NodeJobEvent::NodeFault {
                    job_id: self.job_id,
                    reason: "setup exited without a code".to_owned(),
                },
                (JobPhase::Run, ExitStatus::Code(c)) => NodeJobEvent::JobExited { job_id: self.job_id, code: c },
                (JobPhase::Run, _) => NodeJobEvent::JobExited { job_id: self.job_id, code: 1 },
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
    let name = path.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_else(|| "output".into());
    if path.is_dir() {
        builder.append_dir_all(&name, path).map_err(|e| e.to_string())?;
    } else if path.is_file() {
        let mut f = std::fs::File::open(path).map_err(|e| e.to_string())?;
        builder.append_file(&name, &mut f).map_err(|e| e.to_string())?;
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
            builder.append_dir_all(name, &path).map_err(|e| e.to_string())?;
        } else if path.is_file() {
            let mut f = std::fs::File::open(&path).map_err(|e| e.to_string())?;
            builder.append_file(name, &mut f).map_err(|e| e.to_string())?;
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
