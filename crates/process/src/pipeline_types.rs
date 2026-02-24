//! Core pipeline types extracted from the CI crate.
//!
//! General-purpose pipeline execution primitives: identifiers, statuses,
//! job definitions, and result types. No Forgejo-specific concerns.

use std::collections::HashMap;
use std::fmt;

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
}

// ─── Job Definition ─────────────────────────────────────────────────────────

/// A parsed job from a pipeline YAML file.
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

// ─── Job Execution Results ──────────────────────────────────────────────────

/// Streamed output from a running job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobProgress {
    pub job_id: JobId,
    pub output_line: String,
}

/// Final result of a job execution.
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

// ─── Local Pipeline Types ───────────────────────────────────────────────────

/// Job execution request for a local pipeline runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalStartJob {
    pub job_id: JobId,
    pub work_dir: String,
    pub job_def: JobDefinition,
    pub env_overrides: HashMap<String, String>,
}

/// Configuration for a local pipeline runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalPipelineConfig {
    pub repo_url: String,
    pub work_dir: String,
    pub pipeline_yaml_path: String,
}
