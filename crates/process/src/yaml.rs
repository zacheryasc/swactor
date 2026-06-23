//! Parser for `.ci.yml` pipeline configuration files.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::pipeline::JobDefinition;

/// Root of a `.ci.yml` file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CiYaml {
    pub pipelines: HashMap<String, PipelineDef>,
}

/// A single pipeline definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDef {
    pub triggers: Vec<TriggerDef>,
    pub jobs: HashMap<String, JobDef>,
}

/// A trigger condition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerDef {
    pub event: TriggerEvent,
    #[serde(default)]
    pub branches: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default)]
    pub pattern: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TriggerEvent {
    Push,
    Tag,
    Merge,
}

/// A job definition in YAML form.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDef {
    pub run: RunCommand,
    #[serde(default)]
    pub needs: Vec<String>,
    #[serde(default)]
    pub timeout: Option<u64>,
    #[serde(default)]
    pub docker: bool,
    #[serde(default)]
    pub artifacts: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

/// `run` can be a single string or an array of strings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RunCommand {
    Single(String),
    Multiple(Vec<String>),
}

impl RunCommand {
    pub fn into_vec(self) -> Vec<String> {
        match self {
            RunCommand::Single(s) => vec![s],
            RunCommand::Multiple(v) => v,
        }
    }
}

// ─── Webhook / trigger event ─────────────────────────────────────────────────

/// Parsed event from a webhook or other trigger source.
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

// ─── Parsing ────────────────────────────────────────────────────────────────

/// Parse a `.ci.yml` string into a `CiYaml`.
pub fn parse_ci_yaml(input: &str) -> Result<CiYaml, ParseError> {
    let ci: CiYaml = serde_yaml::from_str(input).map_err(ParseError::Yaml)?;

    // Validate: check for unknown job references in `needs`
    for (pipeline_name, pipeline) in &ci.pipelines {
        let job_names: Vec<&str> = pipeline.jobs.keys().map(|s| s.as_str()).collect();
        for (job_name, job) in &pipeline.jobs {
            for dep in &job.needs {
                if !job_names.contains(&dep.as_str()) {
                    return Err(ParseError::UnknownDependency {
                        pipeline: pipeline_name.clone(),
                        job: job_name.clone(),
                        dependency: dep.clone(),
                    });
                }
            }
        }
    }

    Ok(ci)
}

/// Convert a YAML `JobDef` to the runtime `JobDefinition`.
pub fn to_job_definition(name: &str, def: &JobDef) -> JobDefinition {
    JobDefinition {
        name: name.to_string(),
        run: def.run.clone().into_vec(),
        needs: def.needs.clone(),
        timeout_secs: def.timeout.unwrap_or(300),
        docker: def.docker,
        artifacts: def.artifacts.clone(),
        env: def.env.clone(),
    }
}

// ─── Trigger Matching ───────────────────────────────────────────────────────

/// Returns the names of pipelines whose triggers match the given webhook event.
pub fn matching_pipelines(ci: &CiYaml, event: &WebhookEvent) -> Vec<String> {
    ci.pipelines
        .iter()
        .filter(|(_, pipeline)| pipeline.triggers.iter().any(|t| trigger_matches(t, event)))
        .map(|(name, _)| name.clone())
        .collect()
}

/// Check whether a single trigger matches a webhook event.
fn trigger_matches(trigger: &TriggerDef, event: &WebhookEvent) -> bool {
    // Event type must match.
    let event_matches = match (&trigger.event, &event.event_type) {
        (TriggerEvent::Push, EventType::Push) => true,
        (TriggerEvent::Tag, EventType::Tag) => true,
        (TriggerEvent::Merge, EventType::Merge) => true,
        _ => false,
    };
    if !event_matches {
        return false;
    }

    // For tag events, check pattern.
    if trigger.event == TriggerEvent::Tag {
        if let Some(ref pattern) = trigger.pattern {
            return glob_matches(pattern, event.tag.as_deref().unwrap_or(""));
        }
        return true;
    }

    // For push/merge, check branch filters.
    let branch = &event.branch;

    // If exclude patterns are specified and branch matches any, reject.
    if trigger.exclude.iter().any(|pat| glob_matches(pat, branch)) {
        return false;
    }

    // If branch patterns are specified, at least one must match.
    if trigger.branches.is_empty() {
        return true;
    }
    trigger.branches.iter().any(|pat| glob_matches(pat, branch))
}

/// Simple glob matching supporting `*` (any chars) and `?` (one char).
pub fn glob_matches(pattern: &str, text: &str) -> bool {
    glob_matches_inner(pattern.as_bytes(), text.as_bytes())
}

fn glob_matches_inner(pat: &[u8], text: &[u8]) -> bool {
    match (pat.first(), text.first()) {
        (None, None) => true,
        (Some(b'*'), _) => {
            // '*' matches zero or more characters
            glob_matches_inner(&pat[1..], text)
                || (!text.is_empty() && glob_matches_inner(pat, &text[1..]))
        }
        (Some(b'?'), Some(_)) => glob_matches_inner(&pat[1..], &text[1..]),
        (Some(a), Some(b)) if a == b => glob_matches_inner(&pat[1..], &text[1..]),
        _ => false,
    }
}

// ─── Errors ─────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ParseError {
    Yaml(serde_yaml::Error),
    UnknownDependency {
        pipeline: String,
        job: String,
        dependency: String,
    },
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::Yaml(e) => write!(f, "YAML parse error: {e}"),
            ParseError::UnknownDependency {
                pipeline,
                job,
                dependency,
            } => write!(
                f,
                "pipeline '{pipeline}', job '{job}': unknown dependency '{dependency}'"
            ),
        }
    }
}

impl std::error::Error for ParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_ci_yaml() {
        let yaml = r#"
pipelines:
  check:
    triggers:
      - event: push
        branches: ["*"]
    jobs:
      test:
        run: cargo test
"#;
        let ci = parse_ci_yaml(yaml).unwrap();
        assert_eq!(ci.pipelines.len(), 1);
        assert!(ci.pipelines.contains_key("check"));
        let check = &ci.pipelines["check"];
        assert_eq!(check.jobs.len(), 1);
        assert!(check.jobs.contains_key("test"));
    }

    #[test]
    fn parse_full_ci_yaml() {
        let yaml = r#"
pipelines:
  check:
    triggers:
      - event: push
        branches: ["*"]
        exclude: ["master"]
    jobs:
      fmt:
        run: cargo fmt -- --check
      clippy:
        run: cargo clippy --all-features -- -D warnings
      test:
        needs: [fmt, clippy]
        run: cargo test
  full:
    triggers:
      - event: push
        branches: ["master"]
    jobs:
      test:
        run: cargo test --all-features
        timeout: 600
      bench:
        needs: [test]
        run: cargo bench -- --output-format json
        artifacts: ["target/criterion/**"]
      docker:
        needs: [test]
        run: cargo test -p distribution --features iroh -- --ignored
        docker: true
  release:
    triggers:
      - event: tag
        pattern: "v*"
    jobs:
      build:
        run: cargo build --release -p xtask
        artifacts: ["target/release/xtask"]
"#;
        let ci = parse_ci_yaml(yaml).unwrap();
        assert_eq!(ci.pipelines.len(), 3);
        assert!(ci.pipelines.contains_key("check"));
        assert!(ci.pipelines.contains_key("full"));
        assert!(ci.pipelines.contains_key("release"));

        let full = &ci.pipelines["full"];
        assert_eq!(full.jobs.len(), 3);
        assert_eq!(full.jobs["bench"].needs, vec!["test"]);
        assert!(full.jobs["docker"].docker);
    }

    #[test]
    fn unknown_dependency_rejected() {
        let yaml = r#"
pipelines:
  check:
    triggers:
      - event: push
        branches: ["*"]
    jobs:
      test:
        needs: [nonexistent]
        run: cargo test
"#;
        let result = parse_ci_yaml(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, ParseError::UnknownDependency { .. }),
            "expected UnknownDependency, got: {err}"
        );
    }

    #[test]
    fn trigger_matching_push_branch() {
        let yaml = r#"
pipelines:
  check:
    triggers:
      - event: push
        branches: ["*"]
        exclude: ["master"]
    jobs:
      test:
        run: cargo test
  full:
    triggers:
      - event: push
        branches: ["master"]
    jobs:
      test:
        run: cargo test
"#;
        let ci = parse_ci_yaml(yaml).unwrap();

        // Push to feature branch → matches "check" only
        let event = WebhookEvent {
            event_type: EventType::Push,
            repo_owner: "user".into(),
            repo_name: "repo".into(),
            branch: "feature-x".into(),
            commit_sha: "abc123".into(),
            tag: None,
        };
        let mut matched = matching_pipelines(&ci, &event);
        matched.sort();
        assert_eq!(matched, vec!["check"]);

        // Push to master → matches "full" only (check excludes master)
        let event = WebhookEvent {
            event_type: EventType::Push,
            repo_owner: "user".into(),
            repo_name: "repo".into(),
            branch: "master".into(),
            commit_sha: "abc123".into(),
            tag: None,
        };
        let mut matched = matching_pipelines(&ci, &event);
        matched.sort();
        assert_eq!(matched, vec!["full"]);
    }

    #[test]
    fn trigger_matching_tag() {
        let yaml = r#"
pipelines:
  release:
    triggers:
      - event: tag
        pattern: "v*"
    jobs:
      build:
        run: cargo build --release
"#;
        let ci = parse_ci_yaml(yaml).unwrap();

        let event = WebhookEvent {
            event_type: EventType::Tag,
            repo_owner: "user".into(),
            repo_name: "repo".into(),
            branch: "master".into(),
            commit_sha: "abc123".into(),
            tag: Some("v1.0.0".into()),
        };
        let matched = matching_pipelines(&ci, &event);
        assert_eq!(matched, vec!["release"]);

        // Non-matching tag
        let event = WebhookEvent {
            event_type: EventType::Tag,
            repo_owner: "user".into(),
            repo_name: "repo".into(),
            branch: "master".into(),
            commit_sha: "abc123".into(),
            tag: Some("nightly-1".into()),
        };
        let matched = matching_pipelines(&ci, &event);
        assert!(matched.is_empty());
    }

    #[test]
    fn glob_matching() {
        assert!(glob_matches("*", "anything"));
        assert!(glob_matches("v*", "v1.0.0"));
        assert!(!glob_matches("v*", "nightly"));
        assert!(glob_matches("feature-?", "feature-x"));
        assert!(!glob_matches("feature-?", "feature-xy"));
        assert!(glob_matches("master", "master"));
        assert!(!glob_matches("master", "main"));
    }

    #[test]
    fn run_command_single_and_multiple() {
        let yaml = r#"
pipelines:
  check:
    triggers:
      - event: push
        branches: ["*"]
    jobs:
      single:
        run: cargo test
      multi:
        run:
          - cargo fmt -- --check
          - cargo test
"#;
        let ci = parse_ci_yaml(yaml).unwrap();
        let check = &ci.pipelines["check"];

        let single = to_job_definition("single", &check.jobs["single"]);
        assert_eq!(single.run, vec!["cargo test"]);

        let multi = to_job_definition("multi", &check.jobs["multi"]);
        assert_eq!(multi.run, vec!["cargo fmt -- --check", "cargo test"]);
    }
}
