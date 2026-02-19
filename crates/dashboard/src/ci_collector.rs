//! CI Dashboard extension: provides HTTP API endpoints and stats for CI pipelines.

use serde::{Deserialize, Serialize};

// CI types defined locally to avoid a cyclic dependency with swactor-ci.

/// Unique identifier for a pipeline execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PipelineId(pub u64);

/// Unique identifier for a job within a pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId {
    pub pipeline_id: PipelineId,
    pub job_name: String,
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineStatus {
    Pending,
    Running,
    Passed,
    Failed,
    Error { reason: String },
}

// ─── Stats Provider ─────────────────────────────────────────────────────────

/// Trait for providing CI snapshot data to the dashboard.
pub trait CiStatsProvider: Send + Sync {
    fn snapshot(&self) -> CiSnapshot;
}

/// Point-in-time snapshot of CI system state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CiSnapshot {
    pub active_pipelines: Vec<PipelineSnapshot>,
    pub recent_pipelines: Vec<PipelineSnapshot>,
    pub provisioner_status: ProvisionerStatus,
    pub active_instances: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProvisionerStatus {
    Online,
    Offline,
    Unknown,
}

/// Snapshot of a single pipeline execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSnapshot {
    pub pipeline_id: PipelineId,
    pub pipeline_name: String,
    pub repo_owner: String,
    pub repo_name: String,
    pub commit_sha: String,
    pub branch: String,
    pub status: PipelineStatus,
    pub jobs: Vec<JobSnapshot>,
}

/// Snapshot of a single job within a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSnapshot {
    pub job_id: JobId,
    pub job_name: String,
    pub status: JobStatus,
    pub output_line_count: usize,
}

// ─── HTTP API Responses ─────────────────────────────────────────────────────

/// Response for GET /api/ci/pipelines
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineListResponse {
    pub pipelines: Vec<PipelineSnapshot>,
}

/// Response for GET /api/ci/pipelines/{id}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDetailResponse {
    pub pipeline: PipelineSnapshot,
}

/// Response for GET /api/ci/status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatusResponse {
    pub provisioner_status: ProvisionerStatus,
    pub active_pipelines: usize,
    pub active_instances: usize,
}

/// Response for GET /api/ci/pipelines/{id}/jobs/{job_id}/log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobLogResponse {
    pub job_id: JobId,
    pub lines: Vec<String>,
}

// ─── Route Matching ─────────────────────────────────────────────────────────

/// Parsed API route for the CI dashboard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CiRoute {
    ListPipelines,
    PipelineDetail { id: u64 },
    JobLog { pipeline_id: u64, job_name: String },
    Artifact { job_id: String, path: String },
    SystemStatus,
    NotFound,
}

/// Parse a request path into a CiRoute.
pub fn parse_route(path: &str) -> CiRoute {
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match parts.as_slice() {
        ["api", "ci", "pipelines"] => CiRoute::ListPipelines,
        ["api", "ci", "pipelines", id] => {
            if let Ok(id) = id.parse() {
                CiRoute::PipelineDetail { id }
            } else {
                CiRoute::NotFound
            }
        }
        ["api", "ci", "pipelines", id, "jobs", job_name, "log"] => {
            if let Ok(pipeline_id) = id.parse() {
                CiRoute::JobLog {
                    pipeline_id,
                    job_name: job_name.to_string(),
                }
            } else {
                CiRoute::NotFound
            }
        }
        ["api", "ci", "artifacts", job_id, rest @ ..] if !rest.is_empty() => CiRoute::Artifact {
            job_id: job_id.to_string(),
            path: rest.join("/"),
        },
        ["api", "ci", "status"] => CiRoute::SystemStatus,
        _ => CiRoute::NotFound,
    }
}

/// Render a CiSnapshot into a JSON response for the given route.
pub fn handle_route(route: &CiRoute, snapshot: &CiSnapshot) -> Option<String> {
    match route {
        CiRoute::ListPipelines => {
            let resp = PipelineListResponse {
                pipelines: snapshot
                    .active_pipelines
                    .iter()
                    .chain(snapshot.recent_pipelines.iter())
                    .cloned()
                    .collect(),
            };
            serde_json::to_string(&resp).ok()
        }
        CiRoute::PipelineDetail { id } => {
            let pipeline = snapshot
                .active_pipelines
                .iter()
                .chain(snapshot.recent_pipelines.iter())
                .find(|p| p.pipeline_id.0 == *id)?;
            let resp = PipelineDetailResponse {
                pipeline: pipeline.clone(),
            };
            serde_json::to_string(&resp).ok()
        }
        CiRoute::SystemStatus => {
            let resp = SystemStatusResponse {
                provisioner_status: snapshot.provisioner_status.clone(),
                active_pipelines: snapshot.active_pipelines.len(),
                active_instances: snapshot.active_instances,
            };
            serde_json::to_string(&resp).ok()
        }
        CiRoute::JobLog { .. } | CiRoute::Artifact { .. } => {
            // These require access to stored data beyond the snapshot.
            None
        }
        CiRoute::NotFound => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_parsing() {
        assert_eq!(parse_route("/api/ci/pipelines"), CiRoute::ListPipelines);
        assert_eq!(
            parse_route("/api/ci/pipelines/42"),
            CiRoute::PipelineDetail { id: 42 }
        );
        assert_eq!(
            parse_route("/api/ci/pipelines/1/jobs/test/log"),
            CiRoute::JobLog {
                pipeline_id: 1,
                job_name: "test".into()
            }
        );
        assert_eq!(
            parse_route("/api/ci/artifacts/job-1/target/release/bin"),
            CiRoute::Artifact {
                job_id: "job-1".into(),
                path: "target/release/bin".into()
            }
        );
        assert_eq!(parse_route("/api/ci/status"), CiRoute::SystemStatus);
        assert_eq!(parse_route("/api/ci/unknown"), CiRoute::NotFound);
    }

    #[test]
    fn handle_system_status() {
        let snapshot = CiSnapshot {
            active_pipelines: vec![],
            recent_pipelines: vec![],
            provisioner_status: ProvisionerStatus::Online,
            active_instances: 2,
        };

        let route = CiRoute::SystemStatus;
        let json = handle_route(&route, &snapshot).unwrap();
        let resp: SystemStatusResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(resp.provisioner_status, ProvisionerStatus::Online);
        assert_eq!(resp.active_instances, 2);
    }
}
