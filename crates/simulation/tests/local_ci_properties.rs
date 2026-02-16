//! Property-based tests for the local CI simulation.
//!
//! These verify invariants that should hold across all possible simulation configurations.

use simulation::ci::local_sim::{self, LocalSimConfig};
use swactor_ci::{EventType, WebhookEvent};

fn simple_yaml() -> String {
    r#"
pipelines:
  check:
    triggers:
      - event: push
        branches: ["*"]
    jobs:
      fmt:
        run: cargo fmt -- --check
      test:
        needs: [fmt]
        run: cargo test
"#
    .into()
}

fn push(branch: &str, sha: &str) -> WebhookEvent {
    WebhookEvent {
        event_type: EventType::Push,
        repo_owner: "user".into(),
        repo_name: "repo".into(),
        branch: branch.into(),
        commit_sha: sha.into(),
        tag: None,
    }
}

// ─── Property: One-at-a-time ────────────────────────────────────────────────

#[test]
fn property_one_at_a_time_single_push() {
    let config = LocalSimConfig {
        name: "prop-1at1-single".into(),
        num_rounds: 30,
        ci_yaml: simple_yaml(),
        webhook_schedule: vec![(1, push("main", "sha-1"))],
        job_duration: 2,
        ..Default::default()
    };
    let trace = local_sim::run_simulation(config);
    assert!(
        local_sim::check_one_at_a_time(&trace),
        "at most one job running at a time"
    );
}

#[test]
fn property_one_at_a_time_burst() {
    let config = LocalSimConfig {
        name: "prop-1at1-burst".into(),
        num_rounds: 100,
        ci_yaml: simple_yaml(),
        webhook_schedule: (1..=10)
            .map(|i| (1, push(&format!("branch-{i}"), &format!("sha-{i}"))))
            .collect(),
        job_duration: 2,
        ..Default::default()
    };
    let trace = local_sim::run_simulation(config);
    assert!(
        local_sim::check_one_at_a_time(&trace),
        "at most one job running at a time under burst"
    );
}

#[test]
fn property_one_at_a_time_staggered() {
    let config = LocalSimConfig {
        name: "prop-1at1-stagger".into(),
        num_rounds: 80,
        ci_yaml: simple_yaml(),
        webhook_schedule: vec![
            (1, push("a", "sha-a")),
            (3, push("b", "sha-b")),
            (5, push("c", "sha-c")),
            (7, push("d", "sha-d")),
        ],
        job_duration: 3,
        ..Default::default()
    };
    let trace = local_sim::run_simulation(config);
    assert!(
        local_sim::check_one_at_a_time(&trace),
        "at most one job running at a time under stagger"
    );
}

// ─── Property: Supersede correctness ────────────────────────────────────────

#[test]
fn property_superseded_pipelines_never_run() {
    let config = LocalSimConfig {
        name: "prop-supersede".into(),
        num_rounds: 60,
        ci_yaml: simple_yaml(),
        // Same branch, rapid pushes while jobs are long.
        webhook_schedule: (1..=8)
            .map(|i| (i, push("feature", &format!("sha-ss-{i}"))))
            .collect(),
        job_duration: 4,
        ..Default::default()
    };
    let trace = local_sim::run_simulation(config);
    assert!(
        local_sim::check_superseded_no_running(&trace),
        "superseded pipelines should never have a running job"
    );
}

// ─── Property: Termination ──────────────────────────────────────────────────

#[test]
fn property_all_non_superseded_terminate() {
    let config = LocalSimConfig {
        name: "prop-terminate".into(),
        num_rounds: 100,
        ci_yaml: simple_yaml(),
        webhook_schedule: vec![
            (1, push("a", "sha-t1")),
            (2, push("b", "sha-t2")),
            (3, push("a", "sha-t3")),
            (10, push("c", "sha-t4")),
        ],
        job_duration: 3,
        ..Default::default()
    };
    let trace = local_sim::run_simulation(config);
    assert!(
        local_sim::check_termination(&trace),
        "all non-superseded pipelines must reach terminal status"
    );
}

#[test]
fn property_all_webhooks_terminate() {
    let config = LocalSimConfig {
        name: "prop-wh-terminate".into(),
        num_rounds: 100,
        ci_yaml: simple_yaml(),
        webhook_schedule: (1..=5)
            .map(|i| (i * 3, push("main", &format!("sha-wh-{i}"))))
            .collect(),
        job_duration: 2,
        ..Default::default()
    };
    let trace = local_sim::run_simulation(config);
    assert!(
        local_sim::check_all_webhooks_terminate(&trace),
        "every webhook must produce a terminal status"
    );
}

// ─── Property: DAG ordering ─────────────────────────────────────────────────

#[test]
fn property_dag_ordering_deep_chain() {
    let yaml = r#"
pipelines:
  deep:
    triggers:
      - event: push
        branches: ["*"]
    jobs:
      a:
        run: echo a
      b:
        needs: [a]
        run: echo b
      c:
        needs: [b]
        run: echo c
      d:
        needs: [c]
        run: echo d
"#;
    let config = LocalSimConfig {
        name: "prop-dag-deep".into(),
        num_rounds: 50,
        ci_yaml: yaml.into(),
        webhook_schedule: vec![(1, push("main", "sha-dag"))],
        job_duration: 2,
        ..Default::default()
    };
    let trace = local_sim::run_simulation(config);
    assert!(
        local_sim::check_dag_ordering(&trace),
        "deep DAG ordering must be respected"
    );
}

#[test]
fn property_dag_ordering_diamond() {
    let yaml = r#"
pipelines:
  diamond:
    triggers:
      - event: push
        branches: ["*"]
    jobs:
      root:
        run: echo root
      left:
        needs: [root]
        run: echo left
      right:
        needs: [root]
        run: echo right
      merge:
        needs: [left, right]
        run: echo merge
"#;
    let config = LocalSimConfig {
        name: "prop-dag-diamond".into(),
        num_rounds: 50,
        ci_yaml: yaml.into(),
        webhook_schedule: vec![(1, push("main", "sha-dia"))],
        job_duration: 2,
        ..Default::default()
    };
    let trace = local_sim::run_simulation(config);
    assert!(
        local_sim::check_dag_ordering(&trace),
        "diamond DAG ordering must be respected"
    );
}

// ─── Property: FIFO across branches ─────────────────────────────────────────

#[test]
fn property_fifo_across_branches() {
    let config = LocalSimConfig {
        name: "prop-fifo".into(),
        num_rounds: 80,
        ci_yaml: simple_yaml(),
        webhook_schedule: vec![
            (1, push("a", "sha-f1")),
            (2, push("b", "sha-f2")),
            (3, push("c", "sha-f3")),
        ],
        job_duration: 3,
        ..Default::default()
    };
    let trace = local_sim::run_simulation(config);
    assert!(
        local_sim::check_fifo_order(&trace),
        "branches must execute in FIFO queue order"
    );
}
