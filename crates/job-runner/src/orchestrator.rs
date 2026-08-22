//! Orchestrator-side job FSM actor — spec §6. It owns the job's lifecycle,
//! claims a ready node, drives the transition table, routes commands to the node
//! job actor over the actor plane, streams the workspace to the node, and
//! reassembles output chunks the node streams back.

use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor_transport::NetworkMessage;

use crate::fsm::{JobCommand, JobEvent, JobState, TransitionCtx, transition};
use crate::model::Job;
use crate::wire::{CHUNK_SIZE, NodeJobCommand, NodeJobEvent, OutputChunk};

/// Reports a terminal job state to an observer address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobDone {
    pub state: JobState,
    pub exit_code: Option<i32>,
}

/// Messages the orchestrator job actor accepts. A `NetworkMessage` so it can
/// cross runtime boundaries (the node reports events/chunks back over iroh).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OrchestratorJobMsg {
    Submit {
        job: Job,
        node_actor: ActorAddress,
    },
    SubmitDataPlane {
        job: Job,
        node_actor: ActorAddress,
        result_peer: String,
    },
    ControllerRegistered,
    NodeEvent(NodeJobEvent),
    OutputChunk(OutputChunk),
}

impl NetworkMessage for OrchestratorJobMsg {
    fn type_tag() -> &'static str {
        "job_runner::OrchestratorJobMsg"
    }
}

pub struct OrchestratorJobActor {
    report_to: ActorAddress,
    node: Option<ActorAddress>,
    state: JobState,
    ctx_fsm: TransitionCtx,
    job: Option<Job>,
    exit_code: Option<i32>,
    landing: PathBuf,
    output_bufs: HashMap<String, Vec<u8>>,
    pending_outputs: HashSet<String>,
    outputs_collected: bool,
    /// Edge mode: bulk bytes (workspace + outputs) travel over EDGE_ALPN,
    /// driven by the integration layer, not as actor messages. `false` keeps the
    /// in-process chunk path (what the tests exercise).
    edge_mode: bool,
    controller_node: Option<[u8; 32]>,
    waiting_for_controller: bool,
}

impl OrchestratorJobActor {
    const JOB_ID: u64 = 0;

    pub fn new(report_to: ActorAddress, landing: PathBuf) -> Self {
        Self {
            report_to,
            node: None,
            state: JobState::Pending,
            ctx_fsm: TransitionCtx::new(false, false),
            job: None,
            exit_code: None,
            landing,
            output_bufs: HashMap::new(),
            pending_outputs: HashSet::new(),
            outputs_collected: false,
            edge_mode: false,
            controller_node: None,
            waiting_for_controller: false,
        }
    }

    /// Edge mode: bulk bytes (workspace + outputs) travel over the EDGE_ALPN
    /// transport driven by the integration layer, not as actor messages. The
    /// orchestrator still emits the small `MaterializeWorkspace`/`CollectOutputs`
    /// commands and drives the lifecycle FSM. Defaults to `false` (chunk path),
    /// which is what the in-process tests exercise.
    pub fn with_edge_mode(mut self, edge: bool) -> Self {
        self.edge_mode = edge;
        self
    }

    pub fn with_controller_node(mut self, node: [u8; 32]) -> Self {
        self.controller_node = Some(node);
        self
    }

    fn reset_for_next_job(&mut self) {
        self.node = None;
        self.state = JobState::Pending;
        self.ctx_fsm = TransitionCtx::new(false, false);
        self.job = None;
        self.exit_code = None;
        self.output_bufs.clear();
        self.pending_outputs.clear();
        self.outputs_collected = false;
        self.waiting_for_controller = false;
    }

    fn submit(
        &mut self,
        ctx: &Ctx,
        job: Job,
        node_actor: ActorAddress,
        result_peer: Option<String>,
    ) {
        if matches!(self.state, JobState::Completed | JobState::Failed) {
            self.reset_for_next_job();
        }
        let wait_for_assignment = result_peer.is_some();
        self.node = Some(node_actor);
        if result_peer.is_some() {
            let _ = ctx.send(
                node_actor,
                NodeJobCommand::Assign {
                    job_id: Self::JOB_ID,
                    orchestrator: ctx.self_addr(),
                    result_peer,
                },
            );
        }
        self.ctx_fsm = TransitionCtx::new(job.has_workspace(), job.has_setup());
        self.job = Some(job);
        let (state, _) = transition(self.state, &JobEvent::JobSubmitted, self.ctx_fsm);
        self.state = state;
        if wait_for_assignment {
            return;
        }
        if let Some(node) = self.controller_node {
            self.waiting_for_controller = true;
            let _ = ctx.send(
                node_actor,
                NodeJobCommand::RegisterController {
                    controller: ctx.self_addr(),
                    node,
                },
            );
            return;
        }
        self.begin_job(ctx);
    }

    fn begin_job(&mut self, ctx: &Ctx) {
        let (state, command) = transition(self.state, &JobEvent::NodeReady, self.ctx_fsm);
        self.state = state;
        if let Some(command) = command {
            self.emit_command(ctx, command);
        }
    }

    fn emit_command(&mut self, ctx: &Ctx, cmd: JobCommand) {
        let Some(node) = self.node else { return };
        let job_id = Self::JOB_ID;
        match cmd {
            JobCommand::MaterializeWorkspace => {
                let _ = ctx.send(node, NodeJobCommand::MaterializeWorkspace { job_id });
                if self.edge_mode {
                    // Workspace bytes travel over EDGE_ALPN (the integration layer
                    // pushed them before the job was submitted).
                    return;
                }
                if let Some(job) = self.job.as_ref() {
                    match pack_workspace(job) {
                        Ok(bytes) => stream_workspace(ctx, node, job_id, &bytes),
                        Err(e) => {
                            eprintln!("job-runner: pack workspace failed: {e}");
                            self.state = JobState::Failed;
                            self.finish(ctx);
                        }
                    }
                }
            }
            JobCommand::RunSetup => {
                let _ = ctx.send(
                    node,
                    NodeJobCommand::RunSetup {
                        job_id,
                        command: self
                            .job
                            .as_ref()
                            .and_then(|j| j.setup.clone())
                            .unwrap_or_default(),
                        env: self.job.as_ref().map(|j| j.env.clone()).unwrap_or_default(),
                    },
                );
            }
            JobCommand::RunJob => {
                let _ = ctx.send(
                    node,
                    NodeJobCommand::RunJob {
                        job_id,
                        command: self.job.as_ref().map(|j| j.run.clone()).unwrap_or_default(),
                        env: self.job.as_ref().map(|j| j.env.clone()).unwrap_or_default(),
                    },
                );
            }
            JobCommand::CollectOutputs => {
                let outputs = self
                    .job
                    .as_ref()
                    .map(|j| j.outputs.clone())
                    .unwrap_or_default();
                self.outputs_collected = false;
                if !self.edge_mode {
                    // Chunk mode: track each output until its eof chunk lands.
                    // Edge mode ships outputs over EDGE_ALPN; the integration
                    // layer drains them, so there are no chunks to wait for.
                    self.pending_outputs = outputs.iter().cloned().collect();
                }
                let _ = ctx.send(node, NodeJobCommand::CollectOutputs { job_id, outputs });
            }
        }
    }

    fn finish(&mut self, ctx: &Ctx) {
        let _ = ctx.send(
            self.report_to,
            JobDone {
                state: self.state,
                exit_code: self.exit_code,
            },
        );
    }

    fn observe_outputs_collected(&mut self, ctx: &Ctx) {
        self.outputs_collected = true;
        self.finish_if_output_collection_complete(ctx);
    }

    fn finish_if_output_collection_complete(&mut self, ctx: &Ctx) {
        if !self.outputs_collected || !self.pending_outputs.is_empty() {
            return;
        }
        self.outputs_collected = false;
        let (new_state, cmd) = transition(self.state, &JobEvent::OutputsCollected, self.ctx_fsm);
        self.state = new_state;
        if matches!(self.state, JobState::Completed | JobState::Failed) {
            self.finish(ctx);
            return;
        }
        if let Some(cmd) = cmd {
            self.emit_command(ctx, cmd);
        }
    }
}

impl ActorInterface for OrchestratorJobActor {
    type Incoming = OrchestratorJobMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: OrchestratorJobMsg) {
        match msg {
            OrchestratorJobMsg::Submit { job, node_actor } => {
                self.submit(ctx, job, node_actor, None);
            }
            OrchestratorJobMsg::SubmitDataPlane {
                job,
                node_actor,
                result_peer,
            } => {
                self.submit(ctx, job, node_actor, Some(result_peer));
            }
            OrchestratorJobMsg::ControllerRegistered => {
                if self.waiting_for_controller {
                    self.waiting_for_controller = false;
                    self.begin_job(ctx);
                }
            }
            OrchestratorJobMsg::NodeEvent(ev) => {
                if let NodeJobEvent::JobExited { code, .. } = ev {
                    self.exit_code = Some(code);
                    self.ctx_fsm.prior_exit = Some(code);
                }
                let Some(fsm_ev) = (match ev {
                    NodeJobEvent::Assigned { .. } => Some(JobEvent::NodeReady),
                    NodeJobEvent::WorkspaceMaterialized { .. } => {
                        Some(JobEvent::WorkspaceMaterialized)
                    }
                    NodeJobEvent::SetupCompleted { .. } => Some(JobEvent::SetupCompleted),
                    NodeJobEvent::JobExited { code, .. } => Some(JobEvent::JobExited(code)),
                    NodeJobEvent::OutputsCollected { .. } => {
                        self.observe_outputs_collected(ctx);
                        None
                    }
                    NodeJobEvent::NodeFault { reason, .. } => Some(JobEvent::NodeFault(reason)),
                }) else {
                    return;
                };
                let (new_state, cmd) = transition(self.state, &fsm_ev, self.ctx_fsm);
                self.state = new_state;
                if matches!(self.state, JobState::Completed | JobState::Failed) {
                    self.finish(ctx);
                    return;
                }
                if let Some(cmd) = cmd {
                    self.emit_command(ctx, cmd);
                }
            }
            OrchestratorJobMsg::OutputChunk(chunk) => {
                let entry = self.output_bufs.entry(chunk.name.clone()).or_default();
                entry.extend_from_slice(&chunk.data);
                if chunk.eof {
                    let bytes = self.output_bufs.remove(&chunk.name).unwrap_or_default();
                    if !bytes.is_empty()
                        && let Err(e) = extract_tar(&bytes, &self.landing)
                    {
                        eprintln!("job-runner: untar output `{}` failed: {e}", chunk.name);
                    }
                    self.pending_outputs.remove(&chunk.name);
                    self.finish_if_output_collection_complete(ctx);
                }
            }
        }
    }
}

/// Pack the job's workspace tree (minus `exclude` globs) into a tar. Returns an
/// empty buffer when the job declares no workspace. Exposed so the integration
/// layer can push the workspace over EDGE_ALPN without touching the actor plane.
pub fn pack_workspace(job: &Job) -> Result<Vec<u8>, String> {
    match job.workspace.as_ref() {
        Some(ws) => pack_dir(&ws.workdir, &ws.exclude),
        None => Ok(Vec::new()),
    }
}

fn pack_dir(src: &Path, exclude: &[String]) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    let mut builder = tar::Builder::new(&mut buf);
    append_dir_filtered(&mut builder, src, src, exclude)?;
    builder.finish().map_err(|e| e.to_string())?;
    drop(builder);
    Ok(buf)
}

fn append_dir_filtered(
    builder: &mut tar::Builder<&mut Vec<u8>>,
    root: &Path,
    dir: &Path,
    exclude: &[String],
) -> Result<(), String> {
    let mut entries = std::fs::read_dir(dir)
        .map_err(|e| format!("read dir {}: {e}", dir.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let path = entry.path();
        let rel = path.strip_prefix(root).map_err(|e| e.to_string())?;
        if is_excluded(rel, exclude) {
            continue;
        }
        let file_type = entry.file_type().map_err(|e| e.to_string())?;
        if file_type.is_dir() {
            append_dir_filtered(builder, root, &path, exclude)?;
        } else if file_type.is_file() {
            builder
                .append_path_with_name(&path, rel)
                .map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

fn is_excluded(rel: &Path, exclude: &[String]) -> bool {
    let rel_text = path_slash(rel);
    let name = rel
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let components = rel
        .iter()
        .map(|c| c.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    for raw in exclude {
        let pattern = raw.trim();
        if pattern.is_empty() {
            continue;
        }
        if let Some(anchored) = pattern.strip_prefix('/') {
            let anchored = anchored.trim_matches('/');
            if rel_text == anchored || rel_text.starts_with(&format!("{anchored}/")) {
                return true;
            }
        } else if pattern.contains('*') {
            if wildcard_matches(pattern, &rel_text) || wildcard_matches(pattern, &name) {
                return true;
            }
        } else if components.iter().any(|component| component == pattern) {
            return true;
        }
    }
    false
}

fn path_slash(path: &Path) -> String {
    path.iter()
        .map(|c| c.to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn wildcard_matches(pattern: &str, text: &str) -> bool {
    let parts = pattern.split('*').collect::<Vec<_>>();
    if parts.len() == 1 {
        return pattern == text;
    }
    let mut pos = 0;
    for (idx, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if idx == 0 && !pattern.starts_with('*') {
            if !text[pos..].starts_with(part) {
                return false;
            }
            pos += part.len();
            continue;
        }
        let Some(found) = text[pos..].find(part) else {
            return false;
        };
        pos += found + part.len();
    }
    pattern.ends_with('*') || parts.last().is_none_or(|last| text.ends_with(last))
}

fn extract_tar(bytes: &[u8], dst: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dst).map_err(|e| e.to_string())?;
    tar::Archive::new(Cursor::new(bytes))
        .unpack(dst)
        .map_err(|e| e.to_string())
}

fn stream_workspace(ctx: &Ctx, node: ActorAddress, job_id: u64, bytes: &[u8]) {
    if bytes.is_empty() {
        let _ = ctx.send(
            node,
            NodeJobCommand::WorkspaceChunk {
                job_id,
                seq: 0,
                data: Vec::new(),
                eof: true,
            },
        );
        return;
    }
    let chunks: Vec<&[u8]> = bytes.chunks(CHUNK_SIZE).collect();
    let total = chunks.len() as u64;
    for (i, chunk) in chunks.iter().enumerate() {
        let _ = ctx.send(
            node,
            NodeJobCommand::WorkspaceChunk {
                job_id,
                seq: i as u64,
                data: chunk.to_vec(),
                eof: i as u64 + 1 == total,
            },
        );
    }
}
