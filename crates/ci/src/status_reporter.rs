//! StatusReporter actor: fire-and-forget Forgejo commit status updates.
//!
//! Receives status update messages and POSTs them to the Forgejo API.
//! Can also post pipeline summary comments to PRs.

use swactor::actor::{ActorInterface, Ctx};

use crate::StatusUpdate;

/// Captured output for a single job, used to build PR comments.
#[derive(Debug, Clone)]
pub struct JobOutput {
    pub job_name: String,
    pub passed: bool,
    pub failure_reason: Option<String>,
    pub output_lines: Vec<String>,
}

/// Messages the StatusReporter can receive.
#[derive(Debug, Clone)]
pub enum StatusReporterMsg {
    Report {
        update: StatusUpdate,
        forgejo_url: String,
        forgejo_token: String,
    },
    PostPipelineComment {
        repo_owner: String,
        repo_name: String,
        commit_sha: String,
        branch: String,
        pipeline_name: String,
        pipeline_state: String,
        job_outputs: Vec<JobOutput>,
        forgejo_url: String,
        forgejo_token: String,
    },
}

/// StatusReporter actor state.
pub struct StatusReporter;

impl StatusReporter {
    pub fn new() -> Self {
        Self
    }

    #[cfg(feature = "local")]
    fn post_status(update: &StatusUpdate, forgejo_url: &str, forgejo_token: &str) {
        let url = format!(
            "{}/api/v1/repos/{}/{}/statuses/{}",
            forgejo_url.trim_end_matches('/'),
            update.repo_owner,
            update.repo_name,
            update.commit_sha,
        );

        let mut body = serde_json::json!({
            "state": update.state,
            "context": update.context,
            "description": update.description,
        });

        if let Some(ref target_url) = update.target_url {
            body["target_url"] = serde_json::Value::String(target_url.clone());
        }

        let result = ureq::post(&url)
            .set("Authorization", &format!("token {forgejo_token}"))
            .set("Content-Type", "application/json")
            .send_string(&body.to_string());

        if let Err(e) = result {
            eprintln!("StatusReporter: failed to post status to {url}: {e}");
        }
    }

    #[cfg(feature = "local")]
    fn find_pr_for_branch(
        forgejo_url: &str,
        forgejo_token: &str,
        repo_owner: &str,
        repo_name: &str,
        branch: &str,
    ) -> Option<u64> {
        let url = format!(
            "{}/api/v1/repos/{}/{}/pulls?state=open&limit=50",
            forgejo_url.trim_end_matches('/'),
            repo_owner,
            repo_name,
        );

        let response = ureq::get(&url)
            .set("Authorization", &format!("token {forgejo_token}"))
            .call();

        let response = match response {
            Ok(r) => r,
            Err(e) => {
                eprintln!("StatusReporter: failed to list PRs: {e}");
                return None;
            }
        };

        let body: serde_json::Value = match response.into_json() {
            Ok(v) => v,
            Err(e) => {
                eprintln!("StatusReporter: failed to parse PR list: {e}");
                return None;
            }
        };

        let prs = body.as_array()?;
        for pr in prs {
            let head_ref = pr.get("head")?.get("ref")?.as_str()?;
            if head_ref == branch {
                return pr.get("number")?.as_u64();
            }
        }
        None
    }

    #[cfg(feature = "local")]
    fn post_pr_comment(
        forgejo_url: &str,
        forgejo_token: &str,
        repo_owner: &str,
        repo_name: &str,
        pr_number: u64,
        body_text: &str,
    ) -> Option<String> {
        let url = format!(
            "{}/api/v1/repos/{}/{}/issues/{}/comments",
            forgejo_url.trim_end_matches('/'),
            repo_owner,
            repo_name,
            pr_number,
        );

        let body = serde_json::json!({
            "body": body_text,
        });

        let result = ureq::post(&url)
            .set("Authorization", &format!("token {forgejo_token}"))
            .set("Content-Type", "application/json")
            .send_string(&body.to_string());

        match result {
            Ok(response) => {
                let json: serde_json::Value = response.into_json().ok()?;
                json.get("html_url")?.as_str().map(|s| s.to_string())
            }
            Err(e) => {
                eprintln!("StatusReporter: failed to post PR comment: {e}");
                None
            }
        }
    }

    #[cfg(feature = "local")]
    fn build_pipeline_comment(
        pipeline_name: &str,
        pipeline_state: &str,
        commit_sha: &str,
        job_outputs: &[JobOutput],
    ) -> String {
        let mut md = format!("## Pipeline `{pipeline_name}` — {pipeline_state}\n\n");
        let short_sha = if commit_sha.len() > 7 {
            &commit_sha[..7]
        } else {
            commit_sha
        };
        md.push_str(&format!("Commit: `{short_sha}`\n\n"));

        for job in job_outputs {
            let status_label = if job.passed {
                "passed".to_string()
            } else {
                match &job.failure_reason {
                    Some(reason) => format!("failed: {reason}"),
                    None => "failed".to_string(),
                }
            };

            md.push_str(&format!(
                "<details>\n<summary>{} — {}</summary>\n\n",
                job.job_name, status_label
            ));

            let max_lines = 100;
            let total = job.output_lines.len();
            let lines: &[String] = if total > max_lines {
                md.push_str(&format!("_Showing last {max_lines} of {total} lines_\n\n"));
                &job.output_lines[total - max_lines..]
            } else {
                &job.output_lines
            };

            md.push_str("```\n");
            for line in lines {
                md.push_str(line);
                md.push('\n');
            }
            md.push_str("```\n\n</details>\n\n");
        }

        md
    }

    #[cfg(feature = "local")]
    fn handle_pipeline_comment(
        repo_owner: &str,
        repo_name: &str,
        commit_sha: &str,
        branch: &str,
        pipeline_name: &str,
        pipeline_state: &str,
        job_outputs: &[JobOutput],
        forgejo_url: &str,
        forgejo_token: &str,
    ) {
        let pr_number = match Self::find_pr_for_branch(
            forgejo_url,
            forgejo_token,
            repo_owner,
            repo_name,
            branch,
        ) {
            Some(n) => n,
            None => {
                eprintln!(
                    "StatusReporter: no open PR for branch '{branch}', skipping comment"
                );
                return;
            }
        };

        let comment_body =
            Self::build_pipeline_comment(pipeline_name, pipeline_state, commit_sha, job_outputs);

        let comment_url = Self::post_pr_comment(
            forgejo_url,
            forgejo_token,
            repo_owner,
            repo_name,
            pr_number,
            &comment_body,
        );

        // Re-post pipeline status with target_url pointing to the comment.
        if let Some(ref url) = comment_url {
            let update = StatusUpdate {
                repo_owner: repo_owner.to_string(),
                repo_name: repo_name.to_string(),
                commit_sha: commit_sha.to_string(),
                state: pipeline_state.to_string(),
                context: format!("ci/{pipeline_name}"),
                description: format!("Pipeline '{pipeline_name}' {pipeline_state}"),
                target_url: Some(url.clone()),
            };
            Self::post_status(&update, forgejo_url, forgejo_token);
        }
    }
}

impl ActorInterface for StatusReporter {
    type Incoming = StatusReporterMsg;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, msg: StatusReporterMsg) {
        match msg {
            StatusReporterMsg::Report {
                update,
                forgejo_url,
                forgejo_token,
            } => {
                #[cfg(feature = "local")]
                Self::post_status(&update, &forgejo_url, &forgejo_token);

                #[cfg(not(feature = "local"))]
                {
                    let _ = (update, forgejo_url, forgejo_token);
                }
            }
            StatusReporterMsg::PostPipelineComment {
                repo_owner,
                repo_name,
                commit_sha,
                branch,
                pipeline_name,
                pipeline_state,
                job_outputs,
                forgejo_url,
                forgejo_token,
            } => {
                #[cfg(feature = "local")]
                Self::handle_pipeline_comment(
                    &repo_owner,
                    &repo_name,
                    &commit_sha,
                    &branch,
                    &pipeline_name,
                    &pipeline_state,
                    &job_outputs,
                    &forgejo_url,
                    &forgejo_token,
                );

                #[cfg(not(feature = "local"))]
                {
                    let _ = (
                        repo_owner,
                        repo_name,
                        commit_sha,
                        branch,
                        pipeline_name,
                        pipeline_state,
                        job_outputs,
                        forgejo_url,
                        forgejo_token,
                    );
                }
            }
        }
    }
}
