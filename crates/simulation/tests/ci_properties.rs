//! Property-based tests for the CI simulation.
//!
//! These verify invariants that should hold across all possible simulation configurations.

use swactor_ci::{EventType, WebhookEvent};
use simulation::ci::sim::{self, CiSimConfig};

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

// ─── Property: Every webhook produces a terminal status ─────────────────────

#[test]
fn property_all_webhooks_terminate_single() {
    let config = CiSimConfig {
        name: "prop-single".into(),
        num_rounds: 40,
        ci_yaml: simple_yaml(),
        webhook_schedule: vec![(1, push("main", "sha-1"))],
        provision_latency: 1,
        job_duration: 2,
        ..Default::default()
    };
    let trace = sim::run_simulation(config);
    assert!(
        sim::check_all_webhooks_terminate(&trace),
        "single webhook must terminate"
    );
}

#[test]
fn property_all_webhooks_terminate_burst() {
    // Burst of webhooks all at once.
    let config = CiSimConfig {
        name: "prop-burst".into(),
        num_rounds: 60,
        ci_yaml: simple_yaml(),
        webhook_schedule: (1..=5)
            .map(|i| (1, push("main", &format!("sha-burst-{i}"))))
            .collect(),
        provision_latency: 1,
        job_duration: 2,
        ..Default::default()
    };
    let trace = sim::run_simulation(config);
    assert!(
        sim::check_all_webhooks_terminate(&trace),
        "burst of 5 webhooks must all terminate"
    );
}

#[test]
fn property_all_webhooks_terminate_staggered() {
    // Webhooks spread across rounds.
    let config = CiSimConfig {
        name: "prop-staggered".into(),
        num_rounds: 60,
        ci_yaml: simple_yaml(),
        webhook_schedule: vec![
            (1, push("main", "sha-s1")),
            (5, push("main", "sha-s2")),
            (10, push("main", "sha-s3")),
            (15, push("main", "sha-s4")),
        ],
        provision_latency: 2,
        job_duration: 3,
        ..Default::default()
    };
    let trace = sim::run_simulation(config);
    assert!(
        sim::check_all_webhooks_terminate(&trace),
        "staggered webhooks must all terminate"
    );
}

// ─── Property: DAG ordering is always respected ─────────────────────────────

#[test]
fn property_dag_ordering_always_respected() {
    // Deep chain: a → b → c → d
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
    let config = CiSimConfig {
        name: "prop-dag-deep".into(),
        num_rounds: 40,
        ci_yaml: yaml.into(),
        webhook_schedule: vec![(1, push("main", "sha-dag"))],
        provision_latency: 1,
        job_duration: 2,
        ..Default::default()
    };
    let trace = sim::run_simulation(config);
    assert!(
        sim::check_dag_ordering(&trace),
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
    let config = CiSimConfig {
        name: "prop-dag-diamond".into(),
        num_rounds: 40,
        ci_yaml: yaml.into(),
        webhook_schedule: vec![(1, push("main", "sha-diamond"))],
        provision_latency: 1,
        job_duration: 2,
        ..Default::default()
    };
    let trace = sim::run_simulation(config);
    assert!(
        sim::check_dag_ordering(&trace),
        "diamond DAG ordering must be respected"
    );
}

// ─── Property: No instance leaks ────────────────────────────────────────────

#[test]
fn property_no_instance_leaks_under_failures() {
    let config = CiSimConfig {
        name: "prop-no-leaks-fail".into(),
        num_rounds: 40,
        ci_yaml: simple_yaml(),
        webhook_schedule: vec![
            (1, push("main", "sha-leak1")),
            (3, push("main", "sha-leak2")),
        ],
        provision_latency: 1,
        job_duration: 2,
        job_failure_schedule: vec![(0, "fmt".into())],
        ..Default::default()
    };
    let trace = sim::run_simulation(config);
    assert!(
        sim::check_no_instance_leaks(&trace),
        "no instance leaks even when jobs fail"
    );
}

#[test]
fn property_no_instance_leaks_under_interruption() {
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
        name: "prop-no-leaks-interrupt".into(),
        num_rounds: 40,
        ci_yaml: yaml.into(),
        webhook_schedule: vec![(1, push("main", "sha-int"))],
        provision_latency: 1,
        job_duration: 5,
        instance_interrupt_schedule: vec![(4, "test".into())],
        ..Default::default()
    };
    let trace = sim::run_simulation(config);
    assert!(
        sim::check_no_instance_leaks(&trace),
        "interrupted instances must be terminated"
    );
}

// ─── Property: Bounded state ────────────────────────────────────────────────

#[test]
fn property_bounded_state_under_rapid_pushes() {
    let config = CiSimConfig {
        name: "prop-bounded".into(),
        num_rounds: 100,
        ci_yaml: simple_yaml(),
        webhook_schedule: (1..=20)
            .map(|i| (i, push("main", &format!("sha-rapid-{i}"))))
            .collect(),
        provision_latency: 1,
        job_duration: 2,
        ..Default::default()
    };
    let trace = sim::run_simulation(config);

    // With 20 pushes, each triggering 1 pipeline with 2 jobs, we should never
    // have more than 20 active pipelines at once (and in practice much fewer).
    assert!(
        sim::check_bounded_state(&trace, 20),
        "active pipelines should be bounded"
    );
}

// ─── Property: Provisioner offline doesn't lose work ────────────────────────

#[test]
fn property_provisioner_offline_eventually_resolves() {
    let config = CiSimConfig {
        name: "prop-offline-resolve".into(),
        num_rounds: 60,
        ci_yaml: simple_yaml(),
        webhook_schedule: vec![(3, push("main", "sha-offline"))],
        provision_latency: 1,
        job_duration: 2,
        provisioner_offline_schedule: vec![(1, 15)],
        ..Default::default()
    };
    let trace = sim::run_simulation(config);
    assert!(
        sim::check_all_webhooks_terminate(&trace),
        "webhooks during provisioner outage must still terminate"
    );
}
