pub mod coordinator;
pub mod local_coordinator;
pub mod local_runner;
pub mod pipeline;
pub mod provisioner;
pub mod runner;
pub mod status_reporter;
pub mod webhook_server;
pub mod yaml;

use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;

use serde::{Deserialize, Serialize};

// ─── Core Identifiers ───────────────────────────────────────────────────────

/// Unique identifier for a pipeline execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PipelineId(pub u64);

impl fmt::Display for PipelineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pipeline-{}", self.0)
    }
}

/// Unique identifier for a job within a pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId {
    pub pipeline_id: PipelineId,
    pub job_name: String,
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.pipeline_id, self.job_name)
    }
}

// ─── Job Status ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    Pending,
    WaitingForProvisioner,
    Provisioning,
    Running,
    Passed,
    Failed { reason: String },
    Skipped,
    Interrupted,
}

impl JobStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobStatus::Passed | JobStatus::Failed { .. } | JobStatus::Skipped | JobStatus::Interrupted
        )
    }
}

// ─── Pipeline Status ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineStatus {
    Pending,
    Running,
    Passed,
    Failed,
    Error { reason: String },
}

impl PipelineStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            PipelineStatus::Passed | PipelineStatus::Failed | PipelineStatus::Error { .. }
        )
    }

    /// Convert to Forgejo commit status string.
    pub fn forgejo_state(&self) -> &'static str {
        match self {
            PipelineStatus::Pending => "pending",
            PipelineStatus::Running => "pending",
            PipelineStatus::Passed => "success",
            PipelineStatus::Failed => "failure",
            PipelineStatus::Error { .. } => "error",
        }
    }
}

// ─── Instance Types ─────────────────────────────────────────────────────────

/// Specification for a spot instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceSpec {
    pub min_cpus: u32,
    pub min_ram_mb: u32,
    pub min_disk_gb: u32,
    pub docker_required: bool,
    pub region_preferences: Vec<String>,
}

impl Default for InstanceSpec {
    fn default() -> Self {
        Self {
            min_cpus: 2,
            min_ram_mb: 2048,
            min_disk_gb: 20,
            docker_required: false,
            region_preferences: Vec::new(),
        }
    }
}

/// Connection details for a provisioned spot instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceReady {
    pub instance_id: String,
    pub ip: IpAddr,
    pub ssh_port: u16,
    pub ssh_host_key: String,
}

// ─── Provisioner ↔ Coordinator Messages ─────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisionRequest {
    pub job_id: JobId,
    pub instance_spec: InstanceSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisionResponse {
    pub job_id: JobId,
    pub result: Result<InstanceReady, ProvisionError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProvisionError {
    NoCapacity,
    ProviderError(String),
    Timeout,
    ProvisionerOffline,
}

impl fmt::Display for ProvisionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProvisionError::NoCapacity => write!(f, "no capacity available"),
            ProvisionError::ProviderError(msg) => write!(f, "provider error: {msg}"),
            ProvisionError::Timeout => write!(f, "provisioning timed out"),
            ProvisionError::ProvisionerOffline => write!(f, "provisioner is offline"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminateRequest {
    pub job_id: JobId,
    pub instance_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminateAck {
    pub job_id: JobId,
}

// ─── Coordinator ↔ RunnerSupervisor Messages ────────────────────────────────

/// Sent from Coordinator to RunnerSupervisor to begin a job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartJob {
    pub job_id: JobId,
    pub instance: InstanceReady,
    pub repo_url: String,
    pub commit_sha: String,
    pub job_def: JobDefinition,
}

/// Streamed output from RunnerSupervisor back to Coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobProgress {
    pub job_id: JobId,
    pub output_line: String,
}

/// Final result from RunnerSupervisor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobComplete {
    pub job_id: JobId,
    pub result: Result<JobSuccess, JobFailure>,
    pub artifacts: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSuccess;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobFailure {
    CommandFailed { exit_code: i32, last_lines: Vec<String> },
    SshError(String),
    ExecError(String),
    Timeout,
    Interrupted,
}

impl fmt::Display for JobFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobFailure::CommandFailed { exit_code, .. } => {
                write!(f, "command exited with code {exit_code}")
            }
            JobFailure::SshError(msg) => write!(f, "SSH error: {msg}"),
            JobFailure::ExecError(msg) => write!(f, "exec error: {msg}"),
            JobFailure::Timeout => write!(f, "job timed out"),
            JobFailure::Interrupted => write!(f, "spot instance interrupted"),
        }
    }
}

// ─── Job Definition ─────────────────────────────────────────────────────────

/// A parsed job from the .ci.yml file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDefinition {
    pub name: String,
    pub run: Vec<String>,
    pub needs: Vec<String>,
    pub timeout_secs: u64,
    pub docker: bool,
    pub artifacts: Vec<String>,
    pub env: HashMap<String, String>,
}

// ─── Webhook Types ──────────────────────────────────────────────────────────

/// Parsed webhook event from Forgejo.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEvent {
    pub event_type: EventType,
    pub repo_owner: String,
    pub repo_name: String,
    pub branch: String,
    pub commit_sha: String,
    pub tag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    Push,
    Tag,
    Merge,
}

// ─── Forgejo Status Updates ─────────────────────────────────────────────────

/// A commit status update to send to Forgejo's API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusUpdate {
    pub repo_owner: String,
    pub repo_name: String,
    pub commit_sha: String,
    pub state: String,
    pub context: String,
    pub description: String,
    pub target_url: Option<String>,
}

// ─── Coordinator Config ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CiConfig {
    pub webhook_port: u16,
    pub webhook_secret: String,
    pub forgejo_url: String,
    pub forgejo_token: String,
    pub data_dir: String,
}

impl Default for CiConfig {
    fn default() -> Self {
        Self {
            webhook_port: 8787,
            webhook_secret: String::new(),
            forgejo_url: String::new(),
            forgejo_token: String::new(),
            data_dir: "./ci-data".into(),
        }
    }
}

// ─── Local CI Types ──────────────────────────────────────────────────────────

/// Job execution request for local runner (no InstanceReady needed).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalStartJob {
    pub job_id: JobId,
    pub work_dir: String,
    pub job_def: JobDefinition,
    pub env_overrides: HashMap<String, String>,
}

/// Configuration for the local CI runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalCiConfig {
    pub ci: CiConfig,
    pub repo_url: String,
    pub work_dir: String,
    pub ci_yaml_path: String,
}
