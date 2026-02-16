//! Coordinator actor: central brain of the CI system.
//!
//! Receives webhook events, manages pipeline lifecycles, dispatches jobs
//! to the Provisioner and RunnerSupervisor actors.

use std::collections::HashMap;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};

use crate::pipeline::PipelineExecution;
use crate::yaml::{self, CiYaml};
use crate::{
    CiConfig, JobComplete, JobId, JobProgress, JobStatus, PipelineId, ProvisionRequest,
    ProvisionResponse, StatusUpdate, TerminateRequest, WebhookEvent,
};

/// Messages the Coordinator can receive.
#[derive(Debug, Clone)]
pub enum CoordinatorMsg {
    /// A webhook event from Forgejo.
    Webhook(WebhookEvent),
    /// The CI YAML config to use (loaded externally or from the repo).
    SetCiYaml(CiYaml),
    /// Response from the Provisioner.
    ProvisionResponse(ProvisionResponse),
    /// Streamed output from a RunnerSupervisor.
    JobProgress(JobProgress),
    /// Final result from a RunnerSupervisor.
    JobComplete(JobComplete),
    /// Notification that the provisioner is offline (detected via SWIM).
    ProvisionerOffline,
    /// Notification that the provisioner is back online.
    ProvisionerOnline,
}

/// The Coordinator actor state.
pub struct Coordinator {
    config: CiConfig,
    ci_yaml: Option<CiYaml>,
    pipelines: HashMap<PipelineId, PipelineExecution>,
    next_pipeline_id: u64,
    provisioner_addr: Option<ActorAddress>,
    provisioner_online: bool,
    /// Maps job_id → runner supervisor address.
    runner_addrs: HashMap<JobId, ActorAddress>,
    /// Captured status updates (for testing/simulation).
    status_updates: Vec<StatusUpdate>,
    /// Jobs waiting for the provisioner to come online.
    queued_provisions: Vec<ProvisionRequest>,
}

impl Coordinator {
    pub fn new(config: CiConfig) -> Self {
        Self {
            config,
            ci_yaml: None,
            pipelines: HashMap::new(),
            next_pipeline_id: 1,
            provisioner_addr: None,
            provisioner_online: false,
            runner_addrs: HashMap::new(),
            status_updates: Vec::new(),
            queued_provisions: Vec::new(),
        }
    }

    pub fn with_provisioner(mut self, addr: ActorAddress) -> Self {
        self.provisioner_addr = Some(addr);
        self.provisioner_online = true;
        self
    }

    pub fn with_ci_yaml(mut self, yaml: CiYaml) -> Self {
        self.ci_yaml = Some(yaml);
        self
    }

    pub fn pipelines(&self) -> &HashMap<PipelineId, PipelineExecution> {
        &self.pipelines
    }

    pub fn status_updates(&self) -> &[StatusUpdate] {
        &self.status_updates
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

            // Build job definitions.
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

            // Set pending status on Forgejo.
            self.emit_status_update(StatusUpdate {
                repo_owner: event.repo_owner.clone(),
                repo_name: event.repo_name.clone(),
                commit_sha: event.commit_sha.clone(),
                state: "pending".into(),
                context: format!("ci/{pipeline_name}"),
                description: format!("Pipeline '{pipeline_name}' is pending"),
                target_url: None,
            });

            self.pipelines.insert(pipeline_id, pipeline);

            // Start eligible jobs.
            self.advance_pipeline(ctx, pipeline_id);
        }
    }

    fn advance_pipeline(&mut self, ctx: &Ctx, pipeline_id: PipelineId) {
        let pipeline = match self.pipelines.get(&pipeline_id) {
            Some(p) => p,
            None => return,
        };

        // If pipeline is already terminal, emit final status.
        if pipeline.status.is_terminal() {
            let update = StatusUpdate {
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
            };
            self.emit_status_update(update);
            return;
        }

        let eligible = pipeline.eligible_jobs();
        let repo_owner = pipeline.repo_owner.clone();
        let repo_name = pipeline.repo_name.clone();
        let commit_sha = pipeline.commit_sha.clone();

        for job_name in eligible {
            let job_id = JobId {
                pipeline_id,
                job_name: job_name.clone(),
            };

            let spec = {
                let pipeline = self.pipelines.get(&pipeline_id).unwrap();
                let job = &pipeline.jobs[&job_name];
                crate::InstanceSpec {
                    docker_required: job.definition.docker,
                    ..Default::default()
                }
            };

            if self.provisioner_online {
                // Request provisioning.
                if let Some(prov_addr) = self.provisioner_addr {
                    let request = ProvisionRequest {
                        job_id: job_id.clone(),
                        instance_spec: spec,
                    };
                    let _ = ctx.send(prov_addr, crate::provisioner::ProvisionerMsg::Provision(request));
                }
                if let Some(pipeline) = self.pipelines.get_mut(&pipeline_id) {
                    if let Some(job) = pipeline.jobs.get_mut(&job_name) {
                        job.status = JobStatus::Provisioning;
                    }
                }
            } else {
                // Queue for later.
                let request = ProvisionRequest {
                    job_id: job_id.clone(),
                    instance_spec: spec,
                };
                self.queued_provisions.push(request);
                if let Some(pipeline) = self.pipelines.get_mut(&pipeline_id) {
                    if let Some(job) = pipeline.jobs.get_mut(&job_name) {
                        job.status = JobStatus::WaitingForProvisioner;
                    }
                }
            }

            // Emit per-job status.
            self.emit_status_update(StatusUpdate {
                repo_owner: repo_owner.clone(),
                repo_name: repo_name.clone(),
                commit_sha: commit_sha.clone(),
                state: "pending".into(),
                context: format!("ci/{job_name}"),
                description: format!("Job '{job_name}' is provisioning"),
                target_url: None,
            });
        }
    }

    fn handle_provision_response(&mut self, ctx: &Ctx, response: ProvisionResponse) {
        let pipeline_id = response.job_id.pipeline_id;
        let job_name = response.job_id.job_name.clone();

        match response.result {
            Ok(instance) => {
                // Store instance_id for cleanup.
                if let Some(pipeline) = self.pipelines.get_mut(&pipeline_id) {
                    if let Some(job) = pipeline.jobs.get_mut(&job_name) {
                        job.instance_id = Some(instance.instance_id.clone());
                        job.status = JobStatus::Running;
                    }
                }

                // Spawn a RunnerSupervisor for this job.
                let pipeline = &self.pipelines[&pipeline_id];
                let job = &pipeline.jobs[&job_name];

                let start_job = crate::StartJob {
                    job_id: response.job_id.clone(),
                    instance: instance.clone(),
                    repo_url: format!(
                        "{}/{}/{}",
                        self.config.forgejo_url, pipeline.repo_owner, pipeline.repo_name
                    ),
                    commit_sha: pipeline.commit_sha.clone(),
                    job_def: job.definition.clone(),
                };

                let runner = crate::runner::RunnerSupervisor::new(
                    ctx.self_addr(),
                    start_job,
                );

                match ctx.spawn(runner) {
                    Ok(runner_addr) => {
                        self.runner_addrs.insert(response.job_id, runner_addr);
                    }
                    Err(_) => {
                        // Failed to spawn runner — mark job as failed.
                        if let Some(pipeline) = self.pipelines.get_mut(&pipeline_id) {
                            pipeline.set_job_status(
                                &job_name,
                                JobStatus::Failed {
                                    reason: "failed to spawn runner".into(),
                                },
                            );
                        }
                        self.advance_pipeline(ctx, pipeline_id);
                    }
                }
            }
            Err(err) => {
                if let Some(pipeline) = self.pipelines.get_mut(&pipeline_id) {
                    pipeline.set_job_status(
                        &job_name,
                        JobStatus::Failed {
                            reason: err.to_string(),
                        },
                    );
                }
                self.advance_pipeline(ctx, pipeline_id);
            }
        }
    }

    fn handle_job_complete(&mut self, ctx: &Ctx, complete: JobComplete) {
        let pipeline_id = complete.job_id.pipeline_id;
        let job_name = complete.job_id.job_name.clone();

        // Send terminate request for the instance.
        if let Some(pipeline) = self.pipelines.get(&pipeline_id) {
            if let Some(job) = pipeline.jobs.get(&job_name) {
                if let Some(ref instance_id) = job.instance_id {
                    let terminate = TerminateRequest {
                        job_id: complete.job_id.clone(),
                        instance_id: instance_id.clone(),
                    };
                    if let Some(prov_addr) = self.provisioner_addr {
                        let _ = ctx.send(
                            prov_addr,
                            crate::provisioner::ProvisionerMsg::Terminate(terminate),
                        );
                    }
                }
            }
        }

        // Update job status.
        let status = match complete.result {
            Ok(_) => JobStatus::Passed,
            Err(ref failure) => JobStatus::Failed {
                reason: failure.to_string(),
            },
        };

        let (repo_owner, repo_name, commit_sha) = {
            let pipeline = match self.pipelines.get(&pipeline_id) {
                Some(p) => p,
                None => return,
            };
            (
                pipeline.repo_owner.clone(),
                pipeline.repo_name.clone(),
                pipeline.commit_sha.clone(),
            )
        };

        // Emit per-job final status.
        self.emit_status_update(StatusUpdate {
            repo_owner,
            repo_name,
            commit_sha,
            state: match &status {
                JobStatus::Passed => "success".into(),
                _ => "failure".into(),
            },
            context: format!("ci/{job_name}"),
            description: format!("Job '{job_name}' completed"),
            target_url: None,
        });

        if let Some(pipeline) = self.pipelines.get_mut(&pipeline_id) {
            pipeline.set_job_status(&job_name, status);
        }

        // Remove runner address.
        self.runner_addrs.remove(&complete.job_id);

        // Advance pipeline to schedule downstream jobs.
        self.advance_pipeline(ctx, pipeline_id);
    }

    fn handle_provisioner_offline(&mut self) {
        self.provisioner_online = false;

        // Mark all provisioning jobs as waiting.
        for pipeline in self.pipelines.values_mut() {
            for job in pipeline.jobs.values_mut() {
                if job.status == JobStatus::Provisioning {
                    job.status = JobStatus::WaitingForProvisioner;
                }
            }
        }
    }

    fn handle_provisioner_online(&mut self, ctx: &Ctx) {
        self.provisioner_online = true;

        // Flush queued provision requests.
        let queued = std::mem::take(&mut self.queued_provisions);
        for request in queued {
            if let Some(prov_addr) = self.provisioner_addr {
                let _ = ctx.send(prov_addr, crate::provisioner::ProvisionerMsg::Provision(request));
            }
        }

        // Re-advance pipelines that have waiting jobs.
        let pipeline_ids: Vec<PipelineId> = self.pipelines.keys().copied().collect();
        for pid in pipeline_ids {
            // Move waiting jobs back to provisioning.
            if let Some(pipeline) = self.pipelines.get_mut(&pid) {
                let waiting_jobs: Vec<String> = pipeline
                    .jobs
                    .iter()
                    .filter(|(_, j)| j.status == JobStatus::WaitingForProvisioner)
                    .map(|(name, _)| name.clone())
                    .collect();

                for job_name in waiting_jobs {
                    if let Some(job) = pipeline.jobs.get_mut(&job_name) {
                        job.status = JobStatus::Pending;
                    }
                }
            }
            self.advance_pipeline(ctx, pid);
        }
    }

    fn emit_status_update(&mut self, update: StatusUpdate) {
        self.status_updates.push(update);
    }
}

impl ActorInterface for Coordinator {
    type Incoming = CoordinatorMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: CoordinatorMsg) {
        match msg {
            CoordinatorMsg::Webhook(event) => self.handle_webhook(ctx, event),
            CoordinatorMsg::SetCiYaml(yaml) => {
                self.ci_yaml = Some(yaml);
            }
            CoordinatorMsg::ProvisionResponse(resp) => {
                self.handle_provision_response(ctx, resp);
            }
            CoordinatorMsg::JobProgress(progress) => {
                if let Some(pipeline) = self.pipelines.get_mut(&progress.job_id.pipeline_id) {
                    if let Some(job) = pipeline.jobs.get_mut(&progress.job_id.job_name) {
                        job.output_lines.push(progress.output_line);
                    }
                }
            }
            CoordinatorMsg::JobComplete(complete) => {
                self.handle_job_complete(ctx, complete);
            }
            CoordinatorMsg::ProvisionerOffline => self.handle_provisioner_offline(),
            CoordinatorMsg::ProvisionerOnline => self.handle_provisioner_online(ctx),
        }
    }
}
