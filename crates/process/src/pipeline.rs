//! Pipeline resolution, DAG execution logic, job ordering, and type definitions.

use std::collections::{HashMap, HashSet, VecDeque};
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

/// A pipeline execution: tracks the DAG of jobs and their statuses.
#[derive(Debug, Clone)]
pub struct PipelineExecution {
    pub pipeline_id: PipelineId,
    pub pipeline_name: String,
    pub repo_owner: String,
    pub repo_name: String,
    pub commit_sha: String,
    pub branch: String,
    pub status: PipelineStatus,
    pub jobs: HashMap<String, JobExecution>,
}

/// State of a single job within a pipeline execution.
#[derive(Debug, Clone)]
pub struct JobExecution {
    pub job_id: JobId,
    pub definition: JobDefinition,
    pub status: JobStatus,
    pub output_lines: Vec<String>,
    pub instance_id: Option<String>,
}

impl PipelineExecution {
    /// Create a new pipeline execution from a set of job definitions.
    pub fn new(
        pipeline_id: PipelineId,
        pipeline_name: String,
        repo_owner: String,
        repo_name: String,
        commit_sha: String,
        branch: String,
        job_defs: Vec<JobDefinition>,
    ) -> Self {
        let mut jobs = HashMap::new();
        for def in job_defs {
            let job_id = JobId {
                pipeline_id,
                job_name: def.name.clone(),
            };
            jobs.insert(
                def.name.clone(),
                JobExecution {
                    job_id,
                    definition: def,
                    status: JobStatus::Pending,
                    output_lines: Vec::new(),
                    instance_id: None,
                },
            );
        }
        Self {
            pipeline_id,
            pipeline_name,
            repo_owner,
            repo_name,
            commit_sha,
            branch,
            status: PipelineStatus::Pending,
            jobs,
        }
    }

    /// Return job names that are eligible for execution: Pending with all needs satisfied.
    pub fn eligible_jobs(&self) -> Vec<String> {
        self.jobs
            .values()
            .filter(|job| {
                job.status == JobStatus::Pending
                    && job.definition.needs.iter().all(|dep| {
                        self.jobs
                            .get(dep)
                            .map(|d| d.status == JobStatus::Passed)
                            .unwrap_or(false)
                    })
            })
            .map(|job| job.definition.name.clone())
            .collect()
    }

    /// Mark a job as a given status. If a job fails, propagate skip to dependents.
    pub fn set_job_status(&mut self, job_name: &str, status: JobStatus) {
        if let Some(job) = self.jobs.get_mut(job_name) {
            job.status = status.clone();
        }

        // If failure or interruption, skip all transitive dependents.
        if matches!(
            status,
            JobStatus::Failed { .. } | JobStatus::Interrupted
        ) {
            let to_skip = self.transitive_dependents(job_name);
            for dep_name in to_skip {
                if let Some(dep_job) = self.jobs.get_mut(&dep_name)
                    && dep_job.status == JobStatus::Pending {
                        dep_job.status = JobStatus::Skipped;
                    }
            }
        }

        // Update pipeline status.
        self.update_pipeline_status();
    }

    /// Compute the overall pipeline status from individual job statuses.
    fn update_pipeline_status(&mut self) {
        let all_terminal = self.jobs.values().all(|j| j.status.is_terminal());
        let any_running = self.jobs.values().any(|j| {
            matches!(
                j.status,
                JobStatus::Running | JobStatus::Provisioning | JobStatus::WaitingForProvisioner
            )
        });
        let any_failed = self.jobs.values().any(|j| {
            matches!(
                j.status,
                JobStatus::Failed { .. } | JobStatus::Interrupted
            )
        });

        if all_terminal {
            self.status = if any_failed {
                PipelineStatus::Failed
            } else {
                PipelineStatus::Passed
            };
        } else if any_running
            || self
                .jobs
                .values()
                .any(|j| j.status == JobStatus::Passed)
        {
            self.status = PipelineStatus::Running;
        }
    }

    /// Find all transitive dependents of a job (jobs that directly or indirectly need it).
    fn transitive_dependents(&self, job_name: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut queue: VecDeque<&str> = VecDeque::new();
        queue.push_back(job_name);
        let mut visited = HashSet::new();

        while let Some(current) = queue.pop_front() {
            for (name, job) in &self.jobs {
                if job.definition.needs.iter().any(|n| n == current) && visited.insert(name.clone())
                {
                    result.push(name.clone());
                    queue.push_back(name.as_str());
                }
            }
        }
        result
    }
}

// ─── DAG Validation ─────────────────────────────────────────────────────────

/// Topological sort of job definitions. Returns ordered job names or an error if cyclic.
pub fn topological_sort(jobs: &HashMap<String, JobDefinition>) -> Result<Vec<String>, DagError> {
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();

    for (name, def) in jobs {
        in_degree.entry(name.as_str()).or_insert(0);
        for dep in &def.needs {
            if !jobs.contains_key(dep) {
                return Err(DagError::MissingDependency {
                    job: name.clone(),
                    dependency: dep.clone(),
                });
            }
            dependents.entry(dep.as_str()).or_default().push(name.as_str());
            *in_degree.entry(name.as_str()).or_insert(0) += 1;
        }
    }

    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(&name, _)| name)
        .collect();

    // Sort the initial queue for deterministic ordering.
    let mut sorted_queue: Vec<&str> = queue.drain(..).collect();
    sorted_queue.sort();
    queue.extend(sorted_queue);

    let mut result = Vec::new();
    while let Some(node) = queue.pop_front() {
        result.push(node.to_string());
        if let Some(deps) = dependents.get(node) {
            let mut next_nodes = Vec::new();
            for &dep in deps {
                if let Some(deg) = in_degree.get_mut(dep) {
                    *deg -= 1;
                    if *deg == 0 {
                        next_nodes.push(dep);
                    }
                }
            }
            next_nodes.sort();
            queue.extend(next_nodes);
        }
    }

    if result.len() != jobs.len() {
        return Err(DagError::Cycle);
    }
    Ok(result)
}

#[derive(Debug, Clone)]
pub enum DagError {
    Cycle,
    MissingDependency { job: String, dependency: String },
}

impl std::fmt::Display for DagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DagError::Cycle => write!(f, "job dependency cycle detected"),
            DagError::MissingDependency { job, dependency } => {
                write!(f, "job '{job}' depends on unknown job '{dependency}'")
            }
        }
    }
}

impl std::error::Error for DagError {}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use super::JobDefinition;

    fn make_job(name: &str, needs: &[&str]) -> JobDefinition {
        JobDefinition {
            name: name.into(),
            run: vec!["echo test".into()],
            needs: needs.iter().map(|s| s.to_string()).collect(),
            timeout_secs: 300,
            docker: false,
            artifacts: Vec::new(),
            env: HashMap::new(),
        }
    }

    #[test]
    fn topological_sort_linear_chain() {
        let mut jobs = HashMap::new();
        jobs.insert("a".into(), make_job("a", &[]));
        jobs.insert("b".into(), make_job("b", &["a"]));
        jobs.insert("c".into(), make_job("c", &["b"]));

        let order = topological_sort(&jobs).unwrap();
        assert_eq!(order, vec!["a", "b", "c"]);
    }

    #[test]
    fn topological_sort_diamond() {
        let mut jobs = HashMap::new();
        jobs.insert("a".into(), make_job("a", &[]));
        jobs.insert("b".into(), make_job("b", &["a"]));
        jobs.insert("c".into(), make_job("c", &["a"]));
        jobs.insert("d".into(), make_job("d", &["b", "c"]));

        let order = topological_sort(&jobs).unwrap();
        let pos = |name: &str| order.iter().position(|n| n == name).unwrap();
        assert!(pos("a") < pos("b"));
        assert!(pos("a") < pos("c"));
        assert!(pos("b") < pos("d"));
        assert!(pos("c") < pos("d"));
    }

    #[test]
    fn topological_sort_detects_cycle() {
        let mut jobs = HashMap::new();
        jobs.insert("a".into(), make_job("a", &["b"]));
        jobs.insert("b".into(), make_job("b", &["a"]));

        let result = topological_sort(&jobs);
        assert!(matches!(result, Err(DagError::Cycle)));
    }

    #[test]
    fn topological_sort_independent_jobs() {
        let mut jobs = HashMap::new();
        jobs.insert("a".into(), make_job("a", &[]));
        jobs.insert("b".into(), make_job("b", &[]));
        jobs.insert("c".into(), make_job("c", &[]));

        let order = topological_sort(&jobs).unwrap();
        // All jobs present, order is alphabetical for independent nodes
        assert_eq!(order.len(), 3);
        assert_eq!(order, vec!["a", "b", "c"]);
    }

    #[test]
    fn pipeline_eligible_jobs_respects_dag() {
        let jobs = vec![
            make_job("fmt", &[]),
            make_job("clippy", &[]),
            make_job("test", &["fmt", "clippy"]),
        ];

        let mut pipeline = PipelineExecution::new(
            PipelineId(1),
            "check".into(),
            "user".into(),
            "repo".into(),
            "abc123".into(),
            "main".into(),
            jobs,
        );

        // Initially, fmt and clippy are eligible
        let mut eligible = pipeline.eligible_jobs();
        eligible.sort();
        assert_eq!(eligible, vec!["clippy", "fmt"]);

        // After fmt passes, test is still not eligible (clippy pending)
        pipeline.set_job_status("fmt", JobStatus::Passed);
        let eligible = pipeline.eligible_jobs();
        assert_eq!(eligible, vec!["clippy"]);

        // After clippy passes, test becomes eligible
        pipeline.set_job_status("clippy", JobStatus::Passed);
        let eligible = pipeline.eligible_jobs();
        assert_eq!(eligible, vec!["test"]);
    }

    #[test]
    fn pipeline_failure_skips_dependents() {
        let jobs = vec![
            make_job("fmt", &[]),
            make_job("test", &["fmt"]),
            make_job("bench", &["test"]),
        ];

        let mut pipeline = PipelineExecution::new(
            PipelineId(1),
            "check".into(),
            "user".into(),
            "repo".into(),
            "abc123".into(),
            "main".into(),
            jobs,
        );

        // fmt fails → test and bench should be skipped
        pipeline.set_job_status(
            "fmt",
            JobStatus::Failed {
                reason: "formatting error".into(),
            },
        );

        assert_eq!(pipeline.jobs["test"].status, JobStatus::Skipped);
        assert_eq!(pipeline.jobs["bench"].status, JobStatus::Skipped);
        assert_eq!(pipeline.status, PipelineStatus::Failed);
    }

    #[test]
    fn pipeline_all_pass_yields_passed() {
        let jobs = vec![make_job("a", &[]), make_job("b", &["a"])];

        let mut pipeline = PipelineExecution::new(
            PipelineId(1),
            "check".into(),
            "user".into(),
            "repo".into(),
            "abc123".into(),
            "main".into(),
            jobs,
        );

        pipeline.set_job_status("a", JobStatus::Passed);
        pipeline.set_job_status("b", JobStatus::Passed);
        assert_eq!(pipeline.status, PipelineStatus::Passed);
    }
}
