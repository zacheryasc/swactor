//! Scenario tests for the local CI simulation.
//!
//! Each test tells a story: set up a scenario, run the simulation, verify outcomes.

use simulation::ci::local_sim::{self, LocalSimConfig, LocalSimEvent};
use swactor_ci::{EventType, WebhookEvent};

fn basic_ci_yaml() -> String {
    r#"
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
        run: cargo clippy
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
        run: cargo bench
"#
    .into()
}

fn push_event(branch: &str, sha: &str) -> WebhookEvent {
    WebhookEvent {
        event_type: EventType::Push,
        repo_owner: "user".into(),
        repo_name: "repo".into(),
        branch: branch.into(),
        commit_sha: sha.into(),
        tag: None,
    }
}

// ─── Scenario: Single push → single job → passes ────────────────────────────

#[test]
fn single_push_runs_and_completes() {
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
    let config = LocalSimConfig {
        name: "single-push".into(),
        num_rounds: 20,
        ci_yaml: yaml.into(),
        webhook_schedule: vec![(1, push_event("main", "sha1"))],
        job_duration: 2,
        ..Default::default()
    };

    let trace = local_sim::run_simulation(config);

    let created = trace
        .events
        .iter()
        .filter(|(_, e)| matches!(e, LocalSimEvent::PipelineCreated { .. }))
        .count();
    assert_eq!(created, 1);

    let completed = trace
        .events
        .iter()
        .filter(|(_, e)| matches!(e, LocalSimEvent::JobCompleted { passed: true, .. }))
        .count();
    assert_eq!(completed, 1);

    assert!(local_sim::check_all_webhooks_terminate(&trace));
}

// ─── Scenario: Push A, push A again while queued → only latest runs ─────────

#[test]
fn supersede_queued_same_branch() {
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
    let config = LocalSimConfig {
        name: "supersede-queued".into(),
        num_rounds: 30,
        ci_yaml: yaml.into(),
        // Push A at round 1 occupies the runner. Push A' at round 2 queues.
        // Push A'' at round 3 should supersede A'.
        webhook_schedule: vec![
            (1, push_event("feature", "sha-a1")),
            (2, push_event("feature", "sha-a2")),
            (3, push_event("feature", "sha-a3")),
        ],
        job_duration: 5,
        ..Default::default()
    };

    let trace = local_sim::run_simulation(config);

    // sha-a2 should be superseded.
    let superseded = trace
        .events
        .iter()
        .filter(|(_, e)| matches!(e, LocalSimEvent::PipelineSuperseded { .. }))
        .count();
    assert!(superseded >= 1, "at least one pipeline should be superseded");

    // sha-a2 should have an "error" status (superseded).
    let a2_error = trace
        .status_updates
        .iter()
        .any(|u| u.commit_sha == "sha-a2" && u.state == "error");
    assert!(a2_error, "superseded pipeline should report error status");

    // sha-a1 and sha-a3 should both reach terminal status.
    let a1_terminal = trace
        .status_updates
        .iter()
        .any(|u| u.commit_sha == "sha-a1" && (u.state == "success" || u.state == "failure"));
    let a3_terminal = trace
        .status_updates
        .iter()
        .any(|u| u.commit_sha == "sha-a3" && (u.state == "success" || u.state == "failure"));
    assert!(a1_terminal, "first push should complete");
    assert!(a3_terminal, "latest push should complete");

    assert!(local_sim::check_superseded_no_running(&trace));
}

// ─── Scenario: Push A, push A while running → running completes, new queues ─

#[test]
fn push_while_running_does_not_supersede_active() {
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
    let config = LocalSimConfig {
        name: "no-supersede-active".into(),
        num_rounds: 30,
        ci_yaml: yaml.into(),
        // Push A at round 1 starts running immediately.
        // Push A' at round 2 should queue (not cancel the running job).
        webhook_schedule: vec![
            (1, push_event("feature", "sha-run1")),
            (2, push_event("feature", "sha-run2")),
        ],
        job_duration: 4,
        ..Default::default()
    };

    let trace = local_sim::run_simulation(config);

    // Both should reach terminal status.
    let run1_terminal = trace
        .status_updates
        .iter()
        .any(|u| u.commit_sha == "sha-run1" && u.state == "success");
    let run2_terminal = trace
        .status_updates
        .iter()
        .any(|u| u.commit_sha == "sha-run2" && u.state == "success");

    assert!(run1_terminal, "running pipeline should complete normally");
    assert!(run2_terminal, "queued pipeline should run after");

    assert!(local_sim::check_one_at_a_time(&trace));
}

// ─── Scenario: Push A, push B → both run in FIFO order ─────────────────────

#[test]
fn different_branches_run_fifo() {
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
    let config = LocalSimConfig {
        name: "fifo-branches".into(),
        num_rounds: 30,
        ci_yaml: yaml.into(),
        webhook_schedule: vec![
            (1, push_event("feature-a", "sha-fa")),
            (1, push_event("feature-b", "sha-fb")),
        ],
        job_duration: 3,
        ..Default::default()
    };

    let trace = local_sim::run_simulation(config);

    // Both should complete.
    assert!(local_sim::check_all_webhooks_terminate(&trace));

    // FIFO order respected.
    assert!(local_sim::check_fifo_order(&trace));

    // One at a time.
    assert!(local_sim::check_one_at_a_time(&trace));
}

// ─── Scenario: Job failure → dependents skipped → next pipeline starts ──────

#[test]
fn job_failure_skips_dependents_and_advances() {
    let config = LocalSimConfig {
        name: "failure-skip-advance".into(),
        num_rounds: 40,
        ci_yaml: basic_ci_yaml(),
        webhook_schedule: vec![
            (1, push_event("feature-x", "sha-fail")),
            (2, push_event("feature-y", "sha-next")),
        ],
        job_duration: 2,
        // fmt fails.
        job_failure_schedule: vec![(0, "fmt".into())],
        ..Default::default()
    };

    let trace = local_sim::run_simulation(config);

    // test should be skipped in feature-x pipeline (needs fmt which fails).
    let test_skipped = trace.events.iter().any(|(_, e)| {
        matches!(e, LocalSimEvent::JobSkipped { job_id } if job_id.job_name == "test")
    });
    assert!(test_skipped, "test should be skipped when fmt fails");

    // Both pipelines should reach terminal.
    assert!(local_sim::check_all_webhooks_terminate(&trace));

    // One at a time.
    assert!(local_sim::check_one_at_a_time(&trace));
}

// ─── Scenario: Diamond DAG → jobs serialize respecting deps ─────────────────

#[test]
fn diamond_dag_serialized_with_deps() {
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
        name: "diamond-dag".into(),
        num_rounds: 50,
        ci_yaml: yaml.into(),
        webhook_schedule: vec![(1, push_event("main", "sha-diamond"))],
        job_duration: 2,
        ..Default::default()
    };

    let trace = local_sim::run_simulation(config);

    // All 4 jobs should complete.
    let completed = trace
        .events
        .iter()
        .filter(|(_, e)| matches!(e, LocalSimEvent::JobCompleted { .. }))
        .count();
    assert_eq!(completed, 4, "all 4 diamond jobs should complete");

    // DAG ordering respected.
    assert!(local_sim::check_dag_ordering(&trace));

    // One at a time (serial).
    assert!(local_sim::check_one_at_a_time(&trace));

    // Pipeline should succeed.
    let success = trace
        .status_updates
        .iter()
        .any(|u| u.state == "success" && u.context == "ci/diamond");
    assert!(success, "diamond pipeline should succeed");
}
