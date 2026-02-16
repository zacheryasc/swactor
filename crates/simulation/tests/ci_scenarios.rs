//! Scenario tests for the CI simulation.
//!
//! Each test tells a story: set up a scenario, run the simulation, verify outcomes.

use swactor_ci::{EventType, WebhookEvent};
use simulation::ci::sim::{self, CiSimConfig, CiSimEvent};

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

// ─── Scenario: Single push triggers correct pipeline ────────────────────────

#[test]
fn single_push_to_feature_branch_triggers_check_pipeline() {
    let config = CiSimConfig {
        name: "single-push-feature".into(),
        num_rounds: 30,
        ci_yaml: basic_ci_yaml(),
        webhook_schedule: vec![(1, push_event("feature-x", "abc123"))],
        provision_latency: 1,
        job_duration: 2,
        ..Default::default()
    };

    let trace = sim::run_simulation(config);

    // A pipeline was created.
    let pipeline_created = trace
        .events
        .iter()
        .filter(|(_, e)| matches!(e, CiSimEvent::PipelineCreated { .. }))
        .count();
    assert_eq!(pipeline_created, 1, "exactly one pipeline should be created");

    // The pipeline name should be "check" (not "full", since branch is not master).
    let check_created = trace.events.iter().any(|(_, e)| {
        matches!(e, CiSimEvent::PipelineCreated { name, .. } if name == "check")
    });
    assert!(check_created, "pipeline 'check' should be created");

    // All jobs eventually complete (fmt, clippy, test).
    let completed_jobs: Vec<_> = trace
        .events
        .iter()
        .filter_map(|(_, e)| match e {
            CiSimEvent::JobCompleted { job_id, passed } => Some((job_id.job_name.clone(), *passed)),
            _ => None,
        })
        .collect();

    assert_eq!(completed_jobs.len(), 3, "all 3 jobs should complete");
    assert!(
        completed_jobs.iter().all(|(_, passed)| *passed),
        "all jobs should pass"
    );

    // A terminal Forgejo status is emitted.
    assert!(
        sim::check_all_webhooks_terminate(&trace),
        "webhook should produce terminal status"
    );
}

// ─── Scenario: Push to master triggers full pipeline ────────────────────────

#[test]
fn push_to_master_triggers_full_pipeline() {
    let config = CiSimConfig {
        name: "push-master".into(),
        num_rounds: 30,
        ci_yaml: basic_ci_yaml(),
        webhook_schedule: vec![(1, push_event("master", "def456"))],
        provision_latency: 1,
        job_duration: 2,
        ..Default::default()
    };

    let trace = sim::run_simulation(config);

    let full_created = trace.events.iter().any(|(_, e)| {
        matches!(e, CiSimEvent::PipelineCreated { name, .. } if name == "full")
    });
    assert!(full_created, "pipeline 'full' should be created");

    // Both test and bench should eventually complete.
    let completed: Vec<String> = trace
        .events
        .iter()
        .filter_map(|(_, e)| match e {
            CiSimEvent::JobCompleted { job_id, .. } => Some(job_id.job_name.clone()),
            _ => None,
        })
        .collect();

    assert!(completed.contains(&"test".to_string()), "test should complete");
    assert!(completed.contains(&"bench".to_string()), "bench should complete");
}

// ─── Scenario: Jobs execute in DAG order ────────────────────────────────────

#[test]
fn jobs_execute_in_dag_order() {
    let config = CiSimConfig {
        name: "dag-order".into(),
        num_rounds: 30,
        ci_yaml: basic_ci_yaml(),
        webhook_schedule: vec![(1, push_event("feature-y", "aaa111"))],
        provision_latency: 1,
        job_duration: 2,
        ..Default::default()
    };

    let trace = sim::run_simulation(config);

    // test should start after fmt and clippy complete.
    assert!(
        sim::check_dag_ordering(&trace),
        "DAG ordering must be respected"
    );
}

// ─── Scenario: Job failure skips dependents ─────────────────────────────────

#[test]
fn job_failure_skips_downstream_dependents() {
    let config = CiSimConfig {
        name: "failure-skip".into(),
        num_rounds: 30,
        ci_yaml: basic_ci_yaml(),
        webhook_schedule: vec![(1, push_event("feature-z", "bbb222"))],
        provision_latency: 1,
        job_duration: 2,
        // fmt will fail, so test (which needs fmt) should be skipped.
        job_failure_schedule: vec![(0, "fmt".into())],
        ..Default::default()
    };

    let trace = sim::run_simulation(config);

    // test should be skipped.
    let test_skipped = trace.events.iter().any(|(_, e)| {
        matches!(e, CiSimEvent::JobSkipped { job_id } if job_id.job_name == "test")
    });
    assert!(test_skipped, "test job should be skipped when fmt fails");

    // Pipeline should be marked as failed.
    let pipeline_failed = trace.status_updates.iter().any(|u| {
        u.context == "ci/check" && u.state == "failure"
    });
    assert!(pipeline_failed, "pipeline should report failure status");
}

// ─── Scenario: Parallel pushes execute independently ────────────────────────

#[test]
fn parallel_pushes_execute_independently() {
    let config = CiSimConfig {
        name: "parallel-pushes".into(),
        num_rounds: 40,
        ci_yaml: basic_ci_yaml(),
        webhook_schedule: vec![
            (1, push_event("feature-a", "ccc333")),
            (1, push_event("feature-b", "ddd444")),
        ],
        provision_latency: 1,
        job_duration: 2,
        ..Default::default()
    };

    let trace = sim::run_simulation(config);

    // Two pipelines should be created.
    let pipeline_count = trace
        .events
        .iter()
        .filter(|(_, e)| matches!(e, CiSimEvent::PipelineCreated { .. }))
        .count();
    assert_eq!(pipeline_count, 2, "two pipelines should be created");

    // Both should have terminal status.
    assert!(
        sim::check_all_webhooks_terminate(&trace),
        "both webhooks should produce terminal statuses"
    );
}

// ─── Scenario: Provisioner goes offline, jobs queue and resume ──────────────

#[test]
fn provisioner_offline_queues_then_resumes() {
    let config = CiSimConfig {
        name: "provisioner-offline".into(),
        num_rounds: 50,
        ci_yaml: basic_ci_yaml(),
        // Push at round 3, provisioner offline rounds 1-10.
        webhook_schedule: vec![(3, push_event("feature-q", "eee555"))],
        provision_latency: 1,
        job_duration: 2,
        provisioner_offline_schedule: vec![(1, 10)],
        ..Default::default()
    };

    let trace = sim::run_simulation(config);

    // Provisioner went offline and came back.
    let went_offline = trace
        .events
        .iter()
        .any(|(_, e)| matches!(e, CiSimEvent::ProvisionerWentOffline));
    let came_online = trace
        .events
        .iter()
        .any(|(_, e)| matches!(e, CiSimEvent::ProvisionerCameOnline));
    assert!(went_offline, "provisioner should go offline");
    assert!(came_online, "provisioner should come back online");

    // Despite the outage, all jobs should eventually complete.
    assert!(
        sim::check_all_webhooks_terminate(&trace),
        "pipeline should complete after provisioner returns"
    );
}

// ─── Scenario: Spot instance interrupted mid-job ────────────────────────────

#[test]
fn spot_instance_interruption_reports_failure() {
    // Use a simpler pipeline so we have a clear target to interrupt.
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
    let config = CiSimConfig {
        name: "spot-interrupt".into(),
        num_rounds: 30,
        ci_yaml: yaml.into(),
        webhook_schedule: vec![(1, push_event("main", "fff666"))],
        provision_latency: 1,
        job_duration: 5,
        // Interrupt the test job at round 4 (while it's still running).
        instance_interrupt_schedule: vec![(4, "test".into())],
        ..Default::default()
    };

    let trace = sim::run_simulation(config);

    // The pipeline should be terminal (failed due to interruption).
    let has_failure_status = trace
        .status_updates
        .iter()
        .any(|u| u.state == "failure" || u.state == "error");
    assert!(
        has_failure_status,
        "interrupted job should produce a failure status"
    );

    // The instance should be terminated.
    assert!(
        sim::check_no_instance_leaks(&trace),
        "interrupted instance should be terminated"
    );
}

// ─── Scenario: All jobs pass → pipeline success ─────────────────────────────

#[test]
fn all_jobs_pass_marks_pipeline_success() {
    let yaml = r#"
pipelines:
  check:
    triggers:
      - event: push
        branches: ["*"]
    jobs:
      lint:
        run: cargo clippy
      test:
        needs: [lint]
        run: cargo test
"#;
    let config = CiSimConfig {
        name: "all-pass".into(),
        num_rounds: 30,
        ci_yaml: yaml.into(),
        webhook_schedule: vec![(1, push_event("main", "ggg777"))],
        provision_latency: 1,
        job_duration: 2,
        ..Default::default()
    };

    let trace = sim::run_simulation(config);

    let has_success = trace
        .status_updates
        .iter()
        .any(|u| u.state == "success" && u.context == "ci/check");
    assert!(has_success, "pipeline should be marked as success");
}

// ─── Scenario: Every provisioned instance is terminated ─────────────────────

#[test]
fn no_instance_resource_leaks() {
    let config = CiSimConfig {
        name: "no-leaks".into(),
        num_rounds: 40,
        ci_yaml: basic_ci_yaml(),
        webhook_schedule: vec![
            (1, push_event("feature-1", "h1")),
            (5, push_event("feature-2", "h2")),
        ],
        provision_latency: 1,
        job_duration: 2,
        ..Default::default()
    };

    let trace = sim::run_simulation(config);

    assert!(
        sim::check_no_instance_leaks(&trace),
        "all provisioned instances must be terminated"
    );
}
