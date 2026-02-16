//! CI protocol simulation: deterministic, round-based execution of the CI pipeline
//! lifecycle without real IO (no SSH, no cloud API, no HTTP).
//!
//! Follows the same pattern as `crates/simulation/src/distribution/sim.rs`:
//! configure → run rounds → collect trace → analyze properties.

use std::collections::HashMap;

use swactor_ci::pipeline::PipelineExecution;
use swactor_ci::yaml::{self, CiYaml};
use swactor_ci::{
    JobId, JobStatus, PipelineId, ProvisionRequest, StatusUpdate, WebhookEvent,
};

// ─── Simulation Config ──────────────────────────────────────────────────────

/// Configuration for a CI simulation run.
#[derive(Debug, Clone)]
pub struct CiSimConfig {
    pub name: String,
    pub num_rounds: usize,

    /// CI YAML to use for all simulated repos.
    pub ci_yaml: String,

    /// Webhook events to inject at specific rounds.
    pub webhook_schedule: Vec<(usize, WebhookEvent)>,

    /// Rounds of latency for provisioning to complete.
    pub provision_latency: usize,

    /// Probability that provisioning fails (0.0-1.0).
    pub provision_failure_rate: f64,

    /// Rounds of latency for a job to complete.
    pub job_duration: usize,

    /// Force specific jobs to fail: (round, job_name_substring).
    pub job_failure_schedule: Vec<(usize, String)>,

    /// Rounds during which the provisioner is offline: (start_round, end_round).
    pub provisioner_offline_schedule: Vec<(usize, usize)>,

    /// Round at which a specific job's instance is interrupted.
    pub instance_interrupt_schedule: Vec<(usize, String)>,
}

impl Default for CiSimConfig {
    fn default() -> Self {
        Self {
            name: "ci-sim".into(),
            num_rounds: 50,
            ci_yaml: String::new(),
            webhook_schedule: Vec::new(),
            provision_latency: 2,
            provision_failure_rate: 0.0,
            job_duration: 3,
            job_failure_schedule: Vec::new(),
            provisioner_offline_schedule: Vec::new(),
            instance_interrupt_schedule: Vec::new(),
        }
    }
}

// ─── Simulation Trace ───────────────────────────────────────────────────────

/// Event recorded during simulation.
#[derive(Debug, Clone)]
pub enum CiSimEvent {
    WebhookReceived { commit_sha: String },
    PipelineCreated { pipeline_id: PipelineId, name: String },
    ProvisionRequested { job_id: JobId },
    ProvisionCompleted { job_id: JobId, success: bool },
    JobStarted { job_id: JobId },
    JobCompleted { job_id: JobId, passed: bool },
    JobSkipped { job_id: JobId },
    InstanceTerminated { instance_id: String },
    ProvisionerWentOffline,
    ProvisionerCameOnline,
    StatusUpdateEmitted(StatusUpdate),
}

/// Per-round snapshot of simulation state.
#[derive(Debug, Clone)]
pub struct CiSimSnapshot {
    pub active_pipelines: usize,
    pub completed_pipelines: usize,
    pub active_provisions: usize,
    pub active_jobs: usize,
    pub provisioner_online: bool,
    pub active_instances: usize,
}

/// Complete trace output from a CI simulation.
#[derive(Debug, Clone)]
pub struct CiSimTrace {
    pub name: String,
    pub events: Vec<(usize, CiSimEvent)>,
    pub snapshots: Vec<CiSimSnapshot>,
    pub status_updates: Vec<StatusUpdate>,
    pub num_rounds: usize,
    /// Final state of all pipelines.
    pub final_pipelines: Vec<PipelineExecution>,
    /// Instances that were provisioned.
    pub provisioned_instances: Vec<String>,
    /// Instances that were terminated.
    pub terminated_instances: Vec<String>,
}

// ─── Simulation State ───────────────────────────────────────────────────────

/// Tracks an in-flight provision request.
struct PendingProvision {
    request: ProvisionRequest,
    started_round: usize,
}

/// Tracks an in-flight job execution.
struct RunningJob {
    job_id: JobId,
    started_round: usize,
    instance_id: String,
}

/// Run a CI simulation and return the trace.
pub fn run_simulation(config: CiSimConfig) -> CiSimTrace {
    let ci_yaml: CiYaml = yaml::parse_ci_yaml(&config.ci_yaml)
        .expect("CiSimConfig.ci_yaml must be valid YAML");

    let mut events: Vec<(usize, CiSimEvent)> = Vec::new();
    let mut snapshots: Vec<CiSimSnapshot> = Vec::new();

    // Coordinator state (simulated directly, not as actor).
    let mut pipelines: HashMap<PipelineId, PipelineExecution> = HashMap::new();
    let mut next_pipeline_id: u64 = 1;
    let mut all_status_updates: Vec<StatusUpdate> = Vec::new();

    // Provisioner state.
    let mut provisioner_online = true;
    let mut pending_provisions: Vec<PendingProvision> = Vec::new();
    let mut queued_provisions: Vec<ProvisionRequest> = Vec::new();
    let mut provisioned_instances: Vec<String> = Vec::new();
    let mut terminated_instances: Vec<String> = Vec::new();
    let mut active_instances: Vec<String> = Vec::new();
    let mut next_instance_id: u64 = 1;

    // Runner state.
    let mut running_jobs: Vec<RunningJob> = Vec::new();

    // Simple deterministic "RNG" for provision failure decisions.
    let mut rng_counter: u64 = 0x853c49e6748fea9b;
    let mut det_random = || -> f64 {
        rng_counter = rng_counter.wrapping_mul(6364136223846793005).wrapping_add(1);
        (rng_counter >> 33) as f64 / (u32::MAX as f64)
    };

    for round in 1..=config.num_rounds {
        // 1. Apply provisioner online/offline schedule.
        let should_be_offline = config
            .provisioner_offline_schedule
            .iter()
            .any(|(start, end)| round >= *start && round <= *end);

        if should_be_offline && provisioner_online {
            provisioner_online = false;
            events.push((round, CiSimEvent::ProvisionerWentOffline));

            // Move pending provisions to queue.
            for pending in pending_provisions.drain(..) {
                queued_provisions.push(pending.request);
            }
        } else if !should_be_offline && !provisioner_online {
            provisioner_online = true;
            events.push((round, CiSimEvent::ProvisionerCameOnline));

            // Flush queued provisions.
            for request in queued_provisions.drain(..) {
                pending_provisions.push(PendingProvision {
                    request,
                    started_round: round,
                });
            }

            // Also resubmit any WaitingForProvisioner jobs.
            let mut resubmits = Vec::new();
            for pipeline in pipelines.values_mut() {
                for job in pipeline.jobs.values_mut() {
                    if job.status == JobStatus::WaitingForProvisioner {
                        job.status = JobStatus::Provisioning;
                        resubmits.push(ProvisionRequest {
                            job_id: job.job_id.clone(),
                            instance_spec: swactor_ci::InstanceSpec {
                                docker_required: job.definition.docker,
                                ..Default::default()
                            },
                        });
                    }
                }
            }
            for request in resubmits {
                events.push((round, CiSimEvent::ProvisionRequested { job_id: request.job_id.clone() }));
                pending_provisions.push(PendingProvision {
                    request,
                    started_round: round,
                });
            }
        }

        // 2. Inject webhook events for this round.
        for (sched_round, event) in &config.webhook_schedule {
            if *sched_round == round {
                events.push((
                    round,
                    CiSimEvent::WebhookReceived {
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
                        CiSimEvent::PipelineCreated {
                            pipeline_id,
                            name: pipeline_name.clone(),
                        },
                    ));

                    // Emit pending status.
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
                }
            }
        }

        // 3. Complete provisions that have reached latency.
        let mut completed_provisions = Vec::new();
        pending_provisions.retain(|pending| {
            if round - pending.started_round >= config.provision_latency {
                completed_provisions.push(pending.request.clone());
                false
            } else {
                true
            }
        });

        for request in completed_provisions {
            let should_fail = det_random() < config.provision_failure_rate;

            if should_fail {
                events.push((
                    round,
                    CiSimEvent::ProvisionCompleted {
                        job_id: request.job_id.clone(),
                        success: false,
                    },
                ));

                if let Some(pipeline) = pipelines.get_mut(&request.job_id.pipeline_id) {
                    pipeline.set_job_status(
                        &request.job_id.job_name,
                        JobStatus::Failed {
                            reason: "provision failed".into(),
                        },
                    );
                }
            } else {
                let instance_id = format!("instance-{next_instance_id}");
                next_instance_id += 1;
                provisioned_instances.push(instance_id.clone());
                active_instances.push(instance_id.clone());

                events.push((
                    round,
                    CiSimEvent::ProvisionCompleted {
                        job_id: request.job_id.clone(),
                        success: true,
                    },
                ));

                // Mark job as running and record instance.
                if let Some(pipeline) = pipelines.get_mut(&request.job_id.pipeline_id) {
                    if let Some(job) = pipeline.jobs.get_mut(&request.job_id.job_name) {
                        job.status = JobStatus::Running;
                        job.instance_id = Some(instance_id.clone());
                    }
                }

                events.push((
                    round,
                    CiSimEvent::JobStarted {
                        job_id: request.job_id.clone(),
                    },
                ));

                running_jobs.push(RunningJob {
                    job_id: request.job_id,
                    started_round: round,
                    instance_id,
                });
            }
        }

        // 4. Apply instance interruptions.
        for (interrupt_round, job_name_sub) in &config.instance_interrupt_schedule {
            if *interrupt_round == round {
                running_jobs.retain(|rj| {
                    if rj.job_id.job_name.contains(job_name_sub.as_str()) {
                        // Instance interrupted.
                        if let Some(pipeline) = pipelines.get_mut(&rj.job_id.pipeline_id) {
                            pipeline.set_job_status(&rj.job_id.job_name, JobStatus::Interrupted);
                        }
                        events.push((
                            round,
                            CiSimEvent::JobCompleted {
                                job_id: rj.job_id.clone(),
                                passed: false,
                            },
                        ));
                        // Terminate the instance.
                        active_instances.retain(|id| id != &rj.instance_id);
                        terminated_instances.push(rj.instance_id.clone());
                        events.push((
                            round,
                            CiSimEvent::InstanceTerminated {
                                instance_id: rj.instance_id.clone(),
                            },
                        ));
                        false
                    } else {
                        true
                    }
                });
            }
        }

        // 5. Complete jobs that have reached duration.
        let mut newly_completed = Vec::new();
        running_jobs.retain(|rj| {
            if round - rj.started_round >= config.job_duration {
                newly_completed.push((rj.job_id.clone(), rj.instance_id.clone()));
                false
            } else {
                true
            }
        });

        for (job_id, instance_id) in newly_completed {
            // Check if this job should fail per the schedule.
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
                CiSimEvent::JobCompleted {
                    job_id: job_id.clone(),
                    passed,
                },
            ));

            // Terminate instance.
            active_instances.retain(|id| id != &instance_id);
            terminated_instances.push(instance_id.clone());
            events.push((
                round,
                CiSimEvent::InstanceTerminated {
                    instance_id: instance_id.clone(),
                },
            ));
        }

        // 6. Advance all pipelines: schedule newly-eligible jobs.
        let pipeline_ids: Vec<PipelineId> = pipelines.keys().copied().collect();
        for pid in pipeline_ids {
            let eligible = pipelines[&pid].eligible_jobs();
            for job_name in eligible {
                let job_id = JobId {
                    pipeline_id: pid,
                    job_name: job_name.clone(),
                };
                let docker_required = pipelines[&pid].jobs[&job_name].definition.docker;

                let request = ProvisionRequest {
                    job_id: job_id.clone(),
                    instance_spec: swactor_ci::InstanceSpec {
                        docker_required,
                        ..Default::default()
                    },
                };

                if provisioner_online {
                    if let Some(pipeline) = pipelines.get_mut(&pid) {
                        if let Some(job) = pipeline.jobs.get_mut(&job_name) {
                            job.status = JobStatus::Provisioning;
                        }
                    }
                    events.push((
                        round,
                        CiSimEvent::ProvisionRequested { job_id },
                    ));
                    pending_provisions.push(PendingProvision {
                        request,
                        started_round: round,
                    });
                } else {
                    if let Some(pipeline) = pipelines.get_mut(&pid) {
                        if let Some(job) = pipeline.jobs.get_mut(&job_name) {
                            job.status = JobStatus::WaitingForProvisioner;
                        }
                    }
                    queued_provisions.push(request);
                }
            }

            // Emit final status updates for terminal pipelines.
            if let Some(pipeline) = pipelines.get(&pid) {
                if pipeline.status.is_terminal() {
                    // Check if we already emitted a terminal status for this pipeline.
                    let context = format!("ci/{}", pipeline.pipeline_name);
                    let already_emitted = all_status_updates.iter().any(|u| {
                        u.context == context
                            && u.commit_sha == pipeline.commit_sha
                            && (u.state == "success" || u.state == "failure" || u.state == "error")
                    });
                    if !already_emitted {
                        all_status_updates.push(StatusUpdate {
                            repo_owner: pipeline.repo_owner.clone(),
                            repo_name: pipeline.repo_name.clone(),
                            commit_sha: pipeline.commit_sha.clone(),
                            state: pipeline.status.forgejo_state().into(),
                            context,
                            description: format!(
                                "Pipeline '{}' {}",
                                pipeline.pipeline_name,
                                pipeline.status.forgejo_state()
                            ),
                            target_url: None,
                        });

                        // Emit per-job skipped events.
                        for (_name, job) in &pipeline.jobs {
                            if job.status == JobStatus::Skipped {
                                events.push((
                                    round,
                                    CiSimEvent::JobSkipped {
                                        job_id: job.job_id.clone(),
                                    },
                                ));
                            }
                        }
                    }
                }
            }
        }

        // 7. Snapshot.
        let completed_count = pipelines.values().filter(|p| p.status.is_terminal()).count();
        snapshots.push(CiSimSnapshot {
            active_pipelines: pipelines.len() - completed_count,
            completed_pipelines: completed_count,
            active_provisions: pending_provisions.len(),
            active_jobs: running_jobs.len(),
            provisioner_online,
            active_instances: active_instances.len(),
        });
    }

    CiSimTrace {
        name: config.name,
        events,
        snapshots,
        status_updates: all_status_updates,
        num_rounds: config.num_rounds,
        final_pipelines: pipelines.into_values().collect(),
        provisioned_instances,
        terminated_instances,
    }
}

// ─── Properties ─────────────────────────────────────────────────────────────

/// Every webhook eventually produces a terminal Forgejo status (success/failure/error).
pub fn check_all_webhooks_terminate(trace: &CiSimTrace) -> bool {
    let webhook_commits: Vec<&str> = trace
        .events
        .iter()
        .filter_map(|(_, e)| match e {
            CiSimEvent::WebhookReceived { commit_sha } => Some(commit_sha.as_str()),
            _ => None,
        })
        .collect();

    for sha in webhook_commits {
        let has_terminal = trace.status_updates.iter().any(|u| {
            u.commit_sha == sha && (u.state == "success" || u.state == "failure" || u.state == "error")
        });
        if !has_terminal {
            return false;
        }
    }
    true
}

/// Job DAG ordering is always respected: no job runs before its `needs`.
pub fn check_dag_ordering(trace: &CiSimTrace) -> bool {
    // Build a map of (pipeline_id, job_name) → round when started.
    let mut started: HashMap<(u64, &str), usize> = HashMap::new();
    let mut completed: HashMap<(u64, &str), usize> = HashMap::new();

    for (round, event) in &trace.events {
        match event {
            CiSimEvent::JobStarted { job_id } => {
                started.insert(
                    (job_id.pipeline_id.0, job_id.job_name.as_str()),
                    *round,
                );
            }
            CiSimEvent::JobCompleted { job_id, .. } => {
                completed.insert(
                    (job_id.pipeline_id.0, job_id.job_name.as_str()),
                    *round,
                );
            }
            _ => {}
        }
    }

    // For each pipeline, check that if job B needs job A, then A completed before B started.
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

/// Every provisioned instance is eventually terminated (no resource leaks).
pub fn check_no_instance_leaks(trace: &CiSimTrace) -> bool {
    // Every instance that was provisioned should also be terminated.
    for instance_id in &trace.provisioned_instances {
        if !trace.terminated_instances.contains(instance_id) {
            return false;
        }
    }
    true
}

/// Coordinator state is bounded: active pipeline count doesn't grow unboundedly.
pub fn check_bounded_state(trace: &CiSimTrace, max_active: usize) -> bool {
    trace
        .snapshots
        .iter()
        .all(|s| s.active_pipelines <= max_active)
}
