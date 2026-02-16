//! LocalCoordinator actor: single-machine CI brain.
//!
//! Receives webhook events, queues pipelines, executes jobs one at a time
//! directly on the host. Supports branch-level supersede for queued pipelines.

use std::collections::{HashMap, VecDeque};
use std::process::Command;
use std::sync::{Arc, Mutex};

use swactor::actor::{ActorAddress, ActorInterface, Ctx};

use crate::pipeline::PipelineExecution;
use crate::status_reporter::{JobOutput, StatusReporterMsg};
use crate::yaml::{self, CiYaml};
use crate::{
    JobComplete, JobId, JobProgress, JobStatus, LocalCiConfig, LocalStartJob, PipelineId,
    PipelineStatus, StatusUpdate, WebhookEvent,
};

/// Messages the LocalCoordinator can receive.
#[derive(Debug, Clone)]
pub enum LocalCoordinatorMsg {
    Webhook(WebhookEvent),
    SetCiYaml(CiYaml),
    JobProgress(JobProgress),
    JobComplete(JobComplete),
    GitReady {
        pipeline_id: PipelineId,
        job_name: String,
        work_dir: String,
    },
}

/// Lightweight snapshot of coordinator state (no dashboard dependency).
/// The binary layer converts this to `CiSnapshot` for the dashboard.
#[derive(Debug, Clone, Default)]
pub struct LocalCiSnapshot {
    pub active_pipelines: Vec<PipelineExecution>,
    pub recent_pipelines: Vec<PipelineExecution>,
    pub has_running_job: bool,
}

/// The LocalCoordinator actor state.
pub struct LocalCoordinator {
    config: LocalCiConfig,
    ci_yaml: Option<CiYaml>,
    pipelines: HashMap<PipelineId, PipelineExecution>,
    next_pipeline_id: u64,
    /// FIFO queue of pipeline IDs awaiting execution.
    queue: VecDeque<PipelineId>,
    /// The pipeline currently being executed.
    active_pipeline: Option<PipelineId>,
    /// At most one running job (job_id, runner actor address).
    running_job: Option<(JobId, ActorAddress)>,
    /// Status reporter actor address.
    status_reporter_addr: Option<ActorAddress>,
    /// Bounded ring of finished pipelines.
    completed: VecDeque<PipelineExecution>,
    /// Shared snapshot for external consumers (e.g. dashboard binary).
    ci_snapshot: Arc<Mutex<LocalCiSnapshot>>,
}

impl LocalCoordinator {
    pub fn new(config: LocalCiConfig) -> Self {
        Self {
            config,
            ci_yaml: None,
            pipelines: HashMap::new(),
            next_pipeline_id: 1,
            queue: VecDeque::new(),
            active_pipeline: None,
            running_job: None,
            status_reporter_addr: None,
            completed: VecDeque::new(),
            ci_snapshot: Arc::new(Mutex::new(LocalCiSnapshot::default())),
        }
    }

    pub fn with_status_reporter(mut self, addr: ActorAddress) -> Self {
        self.status_reporter_addr = Some(addr);
        self
    }

    pub fn with_ci_yaml(mut self, yaml: CiYaml) -> Self {
        self.ci_yaml = Some(yaml);
        self
    }

    pub fn ci_snapshot(&self) -> Arc<Mutex<LocalCiSnapshot>> {
        Arc::clone(&self.ci_snapshot)
    }

    pub fn pipelines(&self) -> &HashMap<PipelineId, PipelineExecution> {
        &self.pipelines
    }

    pub fn completed(&self) -> &VecDeque<PipelineExecution> {
        &self.completed
    }

    pub fn queue(&self) -> &VecDeque<PipelineId> {
        &self.queue
    }

    pub fn active_pipeline(&self) -> Option<PipelineId> {
        self.active_pipeline
    }

    pub fn running_job(&self) -> Option<&(JobId, ActorAddress)> {
        self.running_job.as_ref()
    }

    fn handle_webhook(&mut self, ctx: &Ctx, event: WebhookEvent) {
        let ci = match &self.ci_yaml {
            Some(ci) => ci.clone(),
            None => return,
        };

        let matched = yaml::matching_pipelines(&ci, &event);
        for pipeline_name in matched {
            let pipeline_def = &ci.pipelines[&pipeline_name];
            let pipeline_id = PipelineId(self.next_pipeline_id);
            self.next_pipeline_id += 1;

            let job_defs: Vec<_> = pipeline_def
                .jobs
                .iter()
                .map(|(name, def)| yaml::to_job_definition(name, def))
                .collect();

            let pipeline = PipelineExecution::new(
                pipeline_id,
                pipeline_name.clone(),
                event.repo_owner.clone(),
                event.repo_name.clone(),
                event.commit_sha.clone(),
                event.branch.clone(),
                job_defs,
            );

            self.emit_status(
                ctx,
                StatusUpdate {
                    repo_owner: event.repo_owner.clone(),
                    repo_name: event.repo_name.clone(),
                    commit_sha: event.commit_sha.clone(),
                    state: "pending".into(),
                    context: format!("ci/{pipeline_name}"),
                    description: format!("Pipeline '{pipeline_name}' is pending"),
                    target_url: None,
                },
            );

            self.pipelines.insert(pipeline_id, pipeline);
            self.enqueue_pipeline(pipeline_id, &event.branch);
        }

        self.try_schedule_next(ctx);
    }

    /// Enqueue a pipeline, superseding any queued pipeline for the same branch.
    fn enqueue_pipeline(&mut self, pipeline_id: PipelineId, branch: &str) {
        // Scan queue for entry with same branch (not the active pipeline).
        let supersede_idx = self.queue.iter().position(|&qid| {
            self.pipelines
                .get(&qid)
                .map(|p| p.branch == branch)
                .unwrap_or(false)
        });

        if let Some(idx) = supersede_idx {
            let old_id = self.queue[idx];
            // Mark old pipeline as superseded.
            if let Some(old_pipeline) = self.pipelines.get_mut(&old_id) {
                old_pipeline.status = PipelineStatus::Error {
                    reason: "superseded".into(),
                };
                // Mark all pending jobs as skipped.
                let job_names: Vec<String> = old_pipeline.jobs.keys().cloned().collect();
                for name in job_names {
                    if old_pipeline.jobs[&name].status == JobStatus::Pending {
                        old_pipeline.set_job_status(&name, JobStatus::Skipped);
                    }
                }
            }
            // Archive the superseded pipeline.
            if let Some(old_pipeline) = self.pipelines.remove(&old_id) {
                self.archive_pipeline(old_pipeline);
            }
            // Replace queue entry.
            self.queue[idx] = pipeline_id;
        } else {
            self.queue.push_back(pipeline_id);
        }
    }

    /// Core scheduling: one job at a time.
    fn try_schedule_next(&mut self, ctx: &Ctx) {
        // If a job is already running, nothing to do.
        if self.running_job.is_some() {
            return;
        }

        // If we have an active pipeline, try to find eligible jobs.
        if let Some(active_id) = self.active_pipeline {
            if let Some(pipeline) = self.pipelines.get(&active_id) {
                let eligible = pipeline.eligible_jobs();
                if !eligible.is_empty() {
                    let job_name = eligible[0].clone();
                    self.start_job(ctx, active_id, &job_name);
                    return;
                }

                // No eligible jobs — check if pipeline is terminal.
                if pipeline.status.is_terminal() {
                    let pipeline = self.pipelines.remove(&active_id).unwrap();
                    self.emit_status(
                        ctx,
                        StatusUpdate {
                            repo_owner: pipeline.repo_owner.clone(),
                            repo_name: pipeline.repo_name.clone(),
                            commit_sha: pipeline.commit_sha.clone(),
                            state: pipeline.status.forgejo_state().into(),
                            context: format!("ci/{}", pipeline.pipeline_name),
                            description: format!(
                                "Pipeline '{}' {}",
                                pipeline.pipeline_name,
                                pipeline.status.forgejo_state()
                            ),
                            target_url: None,
                        },
                    );
                    self.emit_pipeline_comment(ctx, &pipeline);
                    self.archive_pipeline(pipeline);
                    self.active_pipeline = None;
                    // Recurse to pick next from queue.
                    self.try_schedule_next(ctx);
                    return;
                }
            }
            // Pipeline exists but no eligible jobs and not terminal — waiting for running job.
            return;
        }

        // No active pipeline — pop from queue.
        if let Some(next_id) = self.queue.pop_front() {
            self.active_pipeline = Some(next_id);
            self.try_schedule_next(ctx);
        }
    }

    fn start_job(&mut self, ctx: &Ctx, pipeline_id: PipelineId, job_name: &str) {
        let pipeline = match self.pipelines.get_mut(&pipeline_id) {
            Some(p) => p,
            None => return,
        };
        let job = match pipeline.jobs.get_mut(job_name) {
            Some(j) => j,
            None => return,
        };

        job.status = JobStatus::Running;

        let work_dir = format!(
            "{}/pipeline-{}",
            self.config.work_dir, pipeline_id.0
        );

        // Build CI env overrides.
        let mut env_overrides = HashMap::new();
        env_overrides.insert("CI".into(), "true".into());
        env_overrides.insert("CI_COMMIT_SHA".into(), pipeline.commit_sha.clone());
        env_overrides.insert("CI_BRANCH".into(), pipeline.branch.clone());
        env_overrides.insert("CI_PIPELINE_ID".into(), pipeline_id.0.to_string());
        env_overrides.insert("CI_JOB_NAME".into(), job_name.to_string());

        let start_job = LocalStartJob {
            job_id: job.job_id.clone(),
            work_dir: work_dir.clone(),
            job_def: job.definition.clone(),
            env_overrides,
        };

        // Perform git checkout inline (blocks this worker, acceptable for local runner).
        let sha = pipeline.commit_sha.clone();
        let repo_url = self.config.repo_url.clone();
        let git_ok = self.git_checkout(&repo_url, &sha, &work_dir);

        if !git_ok {
            if let Some(pipeline) = self.pipelines.get_mut(&pipeline_id) {
                pipeline.set_job_status(
                    job_name,
                    JobStatus::Failed {
                        reason: "git checkout failed".into(),
                    },
                );
            }
            self.try_schedule_next(ctx);
            return;
        }

        // Emit per-job running status.
        if let Some(pipeline) = self.pipelines.get(&pipeline_id) {
            self.emit_status(
                ctx,
                StatusUpdate {
                    repo_owner: pipeline.repo_owner.clone(),
                    repo_name: pipeline.repo_name.clone(),
                    commit_sha: pipeline.commit_sha.clone(),
                    state: "pending".into(),
                    context: format!("ci/{job_name}"),
                    description: format!("Job '{job_name}' is running"),
                    target_url: None,
                },
            );
        }

        // Spawn LocalRunner actor.
        let runner =
            crate::local_runner::LocalRunner::new(ctx.self_addr(), start_job);

        match ctx.spawn(runner) {
            Ok(runner_addr) => {
                let job_id = JobId {
                    pipeline_id,
                    job_name: job_name.to_string(),
                };
                self.running_job = Some((job_id, runner_addr));
            }
            Err(_) => {
                if let Some(pipeline) = self.pipelines.get_mut(&pipeline_id) {
                    pipeline.set_job_status(
                        job_name,
                        JobStatus::Failed {
                            reason: "failed to spawn runner".into(),
                        },
                    );
                }
                self.try_schedule_next(ctx);
            }
        }
    }

    fn git_checkout(&self, repo_url: &str, sha: &str, work_dir: &str) -> bool {
        let path = std::path::Path::new(work_dir);
        if path.join(".git").exists() {
            // Already cloned — fetch and checkout.
            let fetch = Command::new("git")
                .args(["fetch", "origin"])
                .current_dir(work_dir)
                .output();
            if fetch.is_err() || !fetch.unwrap().status.success() {
                return false;
            }
            let checkout = Command::new("git")
                .args(["checkout", sha])
                .current_dir(work_dir)
                .output();
            checkout.map(|o| o.status.success()).unwrap_or(false)
        } else {
            // Fresh clone.
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let clone = Command::new("git")
                .args(["clone", repo_url, work_dir])
                .output();
            if clone.is_err() || !clone.as_ref().unwrap().status.success() {
                return false;
            }
            let checkout = Command::new("git")
                .args(["checkout", sha])
                .current_dir(work_dir)
                .output();
            checkout.map(|o| o.status.success()).unwrap_or(false)
        }
    }

    fn handle_job_complete(&mut self, ctx: &Ctx, complete: JobComplete) {
        let pipeline_id = complete.job_id.pipeline_id;
        let job_name = complete.job_id.job_name.clone();

        let status = match complete.result {
            Ok(_) => JobStatus::Passed,
            Err(ref failure) => JobStatus::Failed {
                reason: failure.to_string(),
            },
        };

        // Build a description with output tail for failures.
        let description = match &status {
            JobStatus::Passed => format!("Job '{job_name}' passed"),
            JobStatus::Failed { reason } => {
                let output_tail = self
                    .pipelines
                    .get(&pipeline_id)
                    .and_then(|p| p.jobs.get(&job_name))
                    .map(|j| {
                        let lines: Vec<&str> = j
                            .output_lines
                            .iter()
                            .rev()
                            .take(10)
                            .map(|s| s.as_str())
                            .collect();
                        lines.into_iter().rev().collect::<Vec<_>>().join("\n")
                    })
                    .unwrap_or_default();

                let mut desc = format!("Job '{job_name}' failed: {reason}");
                if !output_tail.is_empty() {
                    desc.push_str("\n");
                    desc.push_str(&output_tail);
                }
                // Cap at ~250 chars for the status description field.
                if desc.len() > 250 {
                    desc.truncate(247);
                    desc.push_str("...");
                }
                desc
            }
            _ => format!("Job '{job_name}' completed"),
        };

        // Emit per-job final status.
        if let Some(pipeline) = self.pipelines.get(&pipeline_id) {
            self.emit_status(
                ctx,
                StatusUpdate {
                    repo_owner: pipeline.repo_owner.clone(),
                    repo_name: pipeline.repo_name.clone(),
                    commit_sha: pipeline.commit_sha.clone(),
                    state: match &status {
                        JobStatus::Passed => "success".into(),
                        _ => "failure".into(),
                    },
                    context: format!("ci/{job_name}"),
                    description,
                    target_url: None,
                },
            );
        }

        if let Some(pipeline) = self.pipelines.get_mut(&pipeline_id) {
            pipeline.set_job_status(&job_name, status);
        }

        // Clear running job.
        self.running_job = None;

        // Schedule next.
        self.try_schedule_next(ctx);
    }

    fn emit_status(&self, ctx: &Ctx, update: StatusUpdate) {
        if let Some(reporter_addr) = self.status_reporter_addr {
            let _ = ctx.send(
                reporter_addr,
                StatusReporterMsg::Report {
                    update,
                    forgejo_url: self.config.ci.forgejo_url.clone(),
                    forgejo_token: self.config.ci.forgejo_token.clone(),
                },
            );
        }
    }

    fn emit_pipeline_comment(&self, ctx: &Ctx, pipeline: &crate::pipeline::PipelineExecution) {
        let reporter_addr = match self.status_reporter_addr {
            Some(addr) => addr,
            None => return,
        };

        let job_outputs: Vec<JobOutput> = pipeline
            .jobs
            .values()
            .map(|job| {
                let passed = job.status == JobStatus::Passed;
                let failure_reason = match &job.status {
                    JobStatus::Failed { reason } => Some(reason.clone()),
                    _ => None,
                };
                JobOutput {
                    job_name: job.definition.name.clone(),
                    passed,
                    failure_reason,
                    output_lines: job.output_lines.clone(),
                }
            })
            .collect();

        let _ = ctx.send(
            reporter_addr,
            StatusReporterMsg::PostPipelineComment {
                repo_owner: pipeline.repo_owner.clone(),
                repo_name: pipeline.repo_name.clone(),
                commit_sha: pipeline.commit_sha.clone(),
                branch: pipeline.branch.clone(),
                pipeline_name: pipeline.pipeline_name.clone(),
                pipeline_state: pipeline.status.forgejo_state().to_string(),
                job_outputs,
                forgejo_url: self.config.ci.forgejo_url.clone(),
                forgejo_token: self.config.ci.forgejo_token.clone(),
            },
        );
    }

    fn archive_pipeline(&mut self, pipeline: PipelineExecution) {
        self.completed.push_back(pipeline);
        if self.completed.len() > 50 {
            self.completed.pop_front();
        }
    }

    fn update_snapshot(&self) {
        let active: Vec<PipelineExecution> = self.pipelines.values().cloned().collect();
        let recent: Vec<PipelineExecution> = self
            .completed
            .iter()
            .rev()
            .take(20)
            .cloned()
            .collect();

        if let Ok(mut snap) = self.ci_snapshot.lock() {
            snap.active_pipelines = active;
            snap.recent_pipelines = recent;
            snap.has_running_job = self.running_job.is_some();
        }
    }
}

impl ActorInterface for LocalCoordinator {
    type Incoming = LocalCoordinatorMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: LocalCoordinatorMsg) {
        match msg {
            LocalCoordinatorMsg::Webhook(event) => self.handle_webhook(ctx, event),
            LocalCoordinatorMsg::SetCiYaml(yaml) => {
                self.ci_yaml = Some(yaml);
            }
            LocalCoordinatorMsg::JobProgress(progress) => {
                if let Some(pipeline) = self.pipelines.get_mut(&progress.job_id.pipeline_id) {
                    if let Some(job) = pipeline.jobs.get_mut(&progress.job_id.job_name) {
                        job.output_lines.push(progress.output_line);
                    }
                }
            }
            LocalCoordinatorMsg::JobComplete(complete) => {
                self.handle_job_complete(ctx, complete);
            }
            LocalCoordinatorMsg::GitReady {
                pipeline_id,
                job_name,
                work_dir,
            } => {
                // Git ready is used in the async variant; for now handled inline in start_job.
                let _ = (pipeline_id, job_name, work_dir);
            }
        }

        self.update_snapshot();
    }
}
