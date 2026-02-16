//! Local CI simulation: deterministic, round-based execution of the local
//! coordinator's queue + scheduling logic.
//!
//! No actors, no IO. Models the one-at-a-time scheduling with supersede.
//! Follows the same pattern as `sim.rs`.

use std::collections::{HashMap, VecDeque};

use swactor_ci::pipeline::PipelineExecution;
use swactor_ci::yaml::{self, CiYaml};
use swactor_ci::{JobId, JobStatus, PipelineId, PipelineStatus, StatusUpdate, WebhookEvent};

// ─── Simulation Config ──────────────────────────────────────────────────────

/// Configuration for a local CI simulation run.
#[derive(Debug, Clone)]
pub struct LocalSimConfig {
    pub name: String,
    pub num_rounds: usize,
    pub ci_yaml: String,
    pub webhook_schedule: Vec<(usize, WebhookEvent)>,
    /// Rounds a job takes to execute.
    pub job_duration: usize,
    /// Force specific jobs to fail: (round, job_name_substring).
    pub job_failure_schedule: Vec<(usize, String)>,
}

impl Default for LocalSimConfig {
    fn default() -> Self {
        Self {
            name: "local-sim".into(),
            num_rounds: 50,
            ci_yaml: String::new(),
            webhook_schedule: Vec::new(),
            job_duration: 3,
            job_failure_schedule: Vec::new(),
        }
    }
}

// ─── Simulation Trace ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum LocalSimEvent {
    WebhookReceived { commit_sha: String },
    PipelineCreated { pipeline_id: PipelineId, name: String },
    PipelineSuperseded { pipeline_id: PipelineId },
    PipelineCompleted { pipeline_id: PipelineId, status: PipelineStatus },
    JobStarted { job_id: JobId },
    JobCompleted { job_id: JobId, passed: bool },
    JobSkipped { job_id: JobId },
}

#[derive(Debug, Clone)]
pub struct LocalSimSnapshot {
    pub queued_pipelines: usize,
    pub active_pipeline: Option<PipelineId>,
    pub running_job: Option<JobId>,
    pub completed_pipelines: usize,
}

#[derive(Debug, Clone)]
pub struct LocalSimTrace {
    pub name: String,
    pub events: Vec<(usize, LocalSimEvent)>,
    pub snapshots: Vec<LocalSimSnapshot>,
    pub status_updates: Vec<StatusUpdate>,
    pub num_rounds: usize,
    pub final_pipelines: Vec<PipelineExecution>,
}

// ─── Simulation State ───────────────────────────────────────────────────────

struct RunningJob {
    job_id: JobId,
    started_round: usize,
}

/// Run a local CI simulation and return the trace.
pub fn run_simulation(config: LocalSimConfig) -> LocalSimTrace {
    let ci_yaml: CiYaml =
        yaml::parse_ci_yaml(&config.ci_yaml).expect("LocalSimConfig.ci_yaml must be valid YAML");

    let mut events: Vec<(usize, LocalSimEvent)> = Vec::new();
    let mut snapshots: Vec<LocalSimSnapshot> = Vec::new();
    let mut all_status_updates: Vec<StatusUpdate> = Vec::new();

    // Coordinator state.
    let mut pipelines: HashMap<PipelineId, PipelineExecution> = HashMap::new();
    let mut next_pipeline_id: u64 = 1;
    let mut queue: VecDeque<PipelineId> = VecDeque::new();
    let mut active_pipeline: Option<PipelineId> = None;
    let mut running_job: Option<RunningJob> = None;
    let mut completed_pipelines: Vec<PipelineExecution> = Vec::new();

    for round in 1..=config.num_rounds {
        // 1. Inject webhook events for this round.
        for (sched_round, event) in &config.webhook_schedule {
            if *sched_round == round {
                events.push((
                    round,
                    LocalSimEvent::WebhookReceived {
                        commit_sha: event.commit_sha.clone(),
                    },
                ));

                let matched = yaml::matching_pipelines(&ci_yaml, event);
                for pipeline_name in matched {
                    let pipeline_id = PipelineId(next_pipeline_id);
                    next_pipeline_id += 1;

                    let pipeline_def = &ci_yaml.pipelines[&pipeline_name];
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

                    events.push((
                        round,
                        LocalSimEvent::PipelineCreated {
                            pipeline_id,
                            name: pipeline_name.clone(),
                        },
                    ));

                    all_status_updates.push(StatusUpdate {
                        repo_owner: event.repo_owner.clone(),
                        repo_name: event.repo_name.clone(),
                        commit_sha: event.commit_sha.clone(),
                        state: "pending".into(),
                        context: format!("ci/{pipeline_name}"),
                        description: format!("Pipeline '{pipeline_name}' is pending"),
                        target_url: None,
                    });

                    pipelines.insert(pipeline_id, pipeline);

                    // Enqueue with supersede logic.
                    let supersede_idx = queue.iter().position(|&qid| {
                        pipelines
                            .get(&qid)
                            .map(|p| p.branch == event.branch)
                            .unwrap_or(false)
                    });

                    if let Some(idx) = supersede_idx {
                        let old_id = queue[idx];
                        if let Some(old_pipeline) = pipelines.get_mut(&old_id) {
                            old_pipeline.status = PipelineStatus::Error {
                                reason: "superseded".into(),
                            };
                            let job_names: Vec<String> =
                                old_pipeline.jobs.keys().cloned().collect();
                            for name in job_names {
                                if old_pipeline.jobs[&name].status == JobStatus::Pending {
                                    old_pipeline.set_job_status(&name, JobStatus::Skipped);
                                }
                            }
                        }
                        events.push((
                            round,
                            LocalSimEvent::PipelineSuperseded {
                                pipeline_id: old_id,
                            },
                        ));
                        if let Some(old_pipeline) = pipelines.remove(&old_id) {
                            // Emit terminal status for superseded pipeline.
                            all_status_updates.push(StatusUpdate {
                                repo_owner: old_pipeline.repo_owner.clone(),
                                repo_name: old_pipeline.repo_name.clone(),
                                commit_sha: old_pipeline.commit_sha.clone(),
                                state: "error".into(),
                                context: format!("ci/{}", old_pipeline.pipeline_name),
                                description: "superseded".into(),
                                target_url: None,
                            });
                            completed_pipelines.push(old_pipeline);
                        }
                        queue[idx] = pipeline_id;
                    } else {
                        queue.push_back(pipeline_id);
                    }
                }
            }
        }

        // 2. Complete running job if it has reached duration.
        if let Some(ref rj) = running_job {
            if round - rj.started_round >= config.job_duration {
                let job_id = rj.job_id.clone();

                let should_fail = config
                    .job_failure_schedule
                    .iter()
                    .any(|(r, name_sub)| *r <= round && job_id.job_name.contains(name_sub.as_str()));

                let passed = !should_fail;

                if passed {
                    if let Some(pipeline) = pipelines.get_mut(&job_id.pipeline_id) {
                        pipeline.set_job_status(&job_id.job_name, JobStatus::Passed);
                    }
                } else {
                    if let Some(pipeline) = pipelines.get_mut(&job_id.pipeline_id) {
                        pipeline.set_job_status(
                            &job_id.job_name,
                            JobStatus::Failed {
                                reason: "command failed".into(),
                            },
                        );
                    }
                }

                events.push((
                    round,
                    LocalSimEvent::JobCompleted {
                        job_id: job_id.clone(),
                        passed,
                    },
                ));

                // Emit skipped events for any jobs that were skipped due to failure.
                if !passed {
                    if let Some(pipeline) = pipelines.get(&job_id.pipeline_id) {
                        for (_name, job) in &pipeline.jobs {
                            if job.status == JobStatus::Skipped {
                                events.push((
                                    round,
                                    LocalSimEvent::JobSkipped {
                                        job_id: job.job_id.clone(),
                                    },
                                ));
                            }
                        }
                    }
                }

                running_job = None;
            }
        }

        // 3. Schedule next (one-at-a-time).
        schedule_next(
            &mut pipelines,
            &mut queue,
            &mut active_pipeline,
            &mut running_job,
            &mut completed_pipelines,
            &mut events,
            &mut all_status_updates,
            round,
        );

        // 4. Snapshot.
        snapshots.push(LocalSimSnapshot {
            queued_pipelines: queue.len(),
            active_pipeline,
            running_job: running_job.as_ref().map(|rj| rj.job_id.clone()),
            completed_pipelines: completed_pipelines.len(),
        });
    }

    // Collect remaining active pipelines into final output.
    let mut final_pipelines: Vec<PipelineExecution> = pipelines.into_values().collect();
    final_pipelines.extend(completed_pipelines);

    LocalSimTrace {
        name: config.name,
        events,
        snapshots,
        status_updates: all_status_updates,
        num_rounds: config.num_rounds,
        final_pipelines,
    }
}

#[allow(clippy::too_many_arguments)]
fn schedule_next(
    pipelines: &mut HashMap<PipelineId, PipelineExecution>,
    queue: &mut VecDeque<PipelineId>,
    active_pipeline: &mut Option<PipelineId>,
    running_job: &mut Option<RunningJob>,
    completed_pipelines: &mut Vec<PipelineExecution>,
    events: &mut Vec<(usize, LocalSimEvent)>,
    status_updates: &mut Vec<StatusUpdate>,
    round: usize,
) {
    // If a job is running, nothing to do.
    if running_job.is_some() {
        return;
    }

    // If we have an active pipeline, try eligible jobs.
    if let Some(active_id) = *active_pipeline {
        if let Some(pipeline) = pipelines.get(&active_id) {
            let eligible = pipeline.eligible_jobs();
            if !eligible.is_empty() {
                let job_name = eligible[0].clone();
                let job_id = JobId {
                    pipeline_id: active_id,
                    job_name: job_name.clone(),
                };

                // Mark as running.
                if let Some(pipeline) = pipelines.get_mut(&active_id) {
                    if let Some(job) = pipeline.jobs.get_mut(&job_name) {
                        job.status = JobStatus::Running;
                    }
                }

                events.push((round, LocalSimEvent::JobStarted { job_id: job_id.clone() }));

                *running_job = Some(RunningJob {
                    job_id,
                    started_round: round,
                });
                return;
            }

            // No eligible jobs — check terminal.
            if pipeline.status.is_terminal() {
                let pipeline = pipelines.remove(&active_id).unwrap();
                let status = pipeline.status.clone();

                status_updates.push(StatusUpdate {
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
                });

                events.push((
                    round,
                    LocalSimEvent::PipelineCompleted {
                        pipeline_id: active_id,
                        status,
                    },
                ));

                completed_pipelines.push(pipeline);
                *active_pipeline = None;

                // Recurse.
                schedule_next(
                    pipelines,
                    queue,
                    active_pipeline,
                    running_job,
                    completed_pipelines,
                    events,
                    status_updates,
                    round,
                );
                return;
            }
        }
        // Pipeline exists but no eligible jobs and not terminal — waiting.
        return;
    }

    // No active pipeline — pop from queue.
    if let Some(next_id) = queue.pop_front() {
        *active_pipeline = Some(next_id);
        schedule_next(
            pipelines,
            queue,
            active_pipeline,
            running_job,
            completed_pipelines,
            events,
            status_updates,
            round,
        );
    }
}

// ─── Properties ─────────────────────────────────────────────────────────────

/// At most one job running in any snapshot.
pub fn check_one_at_a_time(trace: &LocalSimTrace) -> bool {
    trace
        .snapshots
        .iter()
        .all(|s| s.running_job.is_some() as usize <= 1)
}

/// Superseded pipelines never have a Running job.
pub fn check_superseded_no_running(trace: &LocalSimTrace) -> bool {
    let superseded: Vec<PipelineId> = trace
        .events
        .iter()
        .filter_map(|(_, e)| match e {
            LocalSimEvent::PipelineSuperseded { pipeline_id } => Some(*pipeline_id),
            _ => None,
        })
        .collect();

    let started_jobs: Vec<&JobId> = trace
        .events
        .iter()
        .filter_map(|(_, e)| match e {
            LocalSimEvent::JobStarted { job_id } => Some(job_id),
            _ => None,
        })
        .collect();

    for pid in &superseded {
        if started_jobs
            .iter()
            .any(|jid| jid.pipeline_id == *pid)
        {
            return false;
        }
    }
    true
}

/// All non-superseded pipelines reach terminal status.
pub fn check_termination(trace: &LocalSimTrace) -> bool {
    let superseded: Vec<PipelineId> = trace
        .events
        .iter()
        .filter_map(|(_, e)| match e {
            LocalSimEvent::PipelineSuperseded { pipeline_id } => Some(*pipeline_id),
            _ => None,
        })
        .collect();

    for pipeline in &trace.final_pipelines {
        if superseded.contains(&pipeline.pipeline_id) {
            continue;
        }
        if !pipeline.status.is_terminal() {
            return false;
        }
    }
    true
}

/// Within a pipeline, jobs respect dependency order.
pub fn check_dag_ordering(trace: &LocalSimTrace) -> bool {
    let mut started: HashMap<(u64, &str), usize> = HashMap::new();
    let mut completed: HashMap<(u64, &str), usize> = HashMap::new();

    for (round, event) in &trace.events {
        match event {
            LocalSimEvent::JobStarted { job_id } => {
                started.insert(
                    (job_id.pipeline_id.0, job_id.job_name.as_str()),
                    *round,
                );
            }
            LocalSimEvent::JobCompleted { job_id, .. } => {
                completed.insert(
                    (job_id.pipeline_id.0, job_id.job_name.as_str()),
                    *round,
                );
            }
            _ => {}
        }
    }

    for pipeline in &trace.final_pipelines {
        for (name, job) in &pipeline.jobs {
            if let Some(&start_round) = started.get(&(pipeline.pipeline_id.0, name.as_str())) {
                for dep in &job.definition.needs {
                    if let Some(&dep_complete_round) =
                        completed.get(&(pipeline.pipeline_id.0, dep.as_str()))
                    {
                        if dep_complete_round > start_round {
                            return false;
                        }
                    }
                }
            }
        }
    }
    true
}

/// Different branches execute in queue order (FIFO).
pub fn check_fifo_order(trace: &LocalSimTrace) -> bool {
    // Collect pipeline creation order and first job start per pipeline.
    let mut creation_order: Vec<PipelineId> = Vec::new();
    let mut first_start: HashMap<PipelineId, usize> = HashMap::new();

    for (round, event) in &trace.events {
        if let LocalSimEvent::PipelineCreated { pipeline_id, .. } = event {
            creation_order.push(*pipeline_id);
        }
        if let LocalSimEvent::JobStarted { job_id } = event {
            first_start
                .entry(job_id.pipeline_id)
                .or_insert(*round);
        }
    }

    // For each pair of pipelines created in order, if both started, the earlier-created
    // one should have started no later.
    for i in 0..creation_order.len() {
        for j in (i + 1)..creation_order.len() {
            let pid_a = creation_order[i];
            let pid_b = creation_order[j];
            if let (Some(&start_a), Some(&start_b)) =
                (first_start.get(&pid_a), first_start.get(&pid_b))
            {
                if start_a > start_b {
                    return false;
                }
            }
        }
    }
    true
}

/// Every webhook produces a terminal status (success/failure/error).
pub fn check_all_webhooks_terminate(trace: &LocalSimTrace) -> bool {
    let webhook_commits: Vec<&str> = trace
        .events
        .iter()
        .filter_map(|(_, e)| match e {
            LocalSimEvent::WebhookReceived { commit_sha } => Some(commit_sha.as_str()),
            _ => None,
        })
        .collect();

    for sha in webhook_commits {
        let has_terminal = trace.status_updates.iter().any(|u| {
            u.commit_sha == sha
                && (u.state == "success" || u.state == "failure" || u.state == "error")
        });
        if !has_terminal {
            return false;
        }
    }
    true
}
