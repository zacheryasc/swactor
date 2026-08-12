use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use provisioning::*;
use std::convert::Infallible;

fn group(id: &str, count: u32) -> RunNodeGroupSpec {
    RunNodeGroupSpec {
        run_id: RunId(7),
        group_id: NodeGroupId(id.to_owned()),
        role: RoleId("worker".to_owned()),
        count,
        provider: ProviderKind::new("mock"),
        shape: DesiredNodeShape {
            image: "node:v1".to_owned(),
            disk_gb: 20,
            gpu_name: None,
            min_gpu_ram_mb: None,
            min_down_mbps: None,
            min_up_mbps: None,
            min_reliability: None,
            require_verified: false,
            provider_labels: BTreeMap::new(),
        },
        boot: BootSpec {
            ssh_user: "root".to_owned(),
            verify_commands: vec!["true".to_owned()],
            start_swactor_command: "swactor".to_owned(),
            stdout_sources: Vec::new(),
            stderr_sources: Vec::new(),
            env: Vec::new(),
            args: Vec::new(),
            mounts: Vec::new(),
        },
        swarm_join: SwarmJoinTemplate {
            orch_swactor_addr: "127.0.0.1:9000".to_owned(),
            join_token_ref: "token".to_owned(),
        },
    }
}

fn shape(generation: u64, groups: Vec<RunNodeGroupSpec>) -> ClusterShape {
    ClusterShape {
        run_id: RunId(7),
        generation,
        groups,
    }
}

fn lease(id: &str, endpoint: bool) -> CreateLeaseResult {
    let provider = ProviderKind::new("mock");
    let lease_id = ProviderLeaseId(id.to_owned());
    CreateLeaseResult {
        lease: LeaseFacts {
            provider: provider.clone(),
            lease_id: lease_id.clone(),
            provider_contract_id: format!("contract-{id}"),
            offer_id: None,
            destroy_handle: DestroyHandle {
                provider,
                lease_id,
                provider_contract_id: format!("contract-{id}"),
            },
            provider_metadata: BTreeMap::new(),
        },
        endpoint: endpoint.then(|| SshEndpoint {
            host: "127.0.0.1".to_owned(),
            port: 22,
            user: "root".to_owned(),
            auth_ref: "test-key".to_owned(),
        }),
    }
}

#[derive(Default)]
struct RecordingExecutor {
    effects: Vec<PlannedEffect>,
}

impl EffectExecutor for RecordingExecutor {
    type SubmitError = Infallible;

    fn submit(&mut self, effect: &PlannedEffect) -> Result<(), Self::SubmitError> {
        self.effects.push(effect.clone());
        Ok(())
    }
}

fn drive(driver: &mut ClusterDriver, now: SystemTime) -> Vec<PlannedEffect> {
    let mut executor = RecordingExecutor::default();
    driver.drive_until_blocked(now, &mut executor).unwrap();
    executor.effects
}

fn succeed(
    driver: &mut ClusterDriver,
    effect: &PlannedEffect,
    outcome: OperationOutcome,
    now: SystemTime,
) {
    assert!(driver.apply_observation(
        &effect.node,
        effect.operation.attempt,
        NodeObservation::OperationSucceeded {
            operation: effect.operation,
            outcome,
        },
        now,
    ));
}

fn only_effect(driver: &mut ClusterDriver, now: SystemTime) -> PlannedEffect {
    let effects = drive(driver, now);
    assert_eq!(effects.len(), 1, "expected one effect, got {effects:?}");
    effects.into_iter().next().unwrap()
}

#[test]
fn shape_expansion_validates_run_and_group_identity() {
    let duplicate = shape(1, vec![group("gpu", 1), group("gpu", 2)]);
    assert!(
        duplicate
            .expand()
            .unwrap_err()
            .reason
            .contains("duplicate node group")
    );

    let mut wrong_run = group("cpu", 1);
    wrong_run.run_id = RunId(8);
    assert!(
        shape(1, vec![wrong_run])
            .expand()
            .unwrap_err()
            .reason
            .contains("expected 7")
    );

    let mut non_finite = group("invalid-metrics", 1);
    non_finite.shape.min_down_mbps = Some(f64::NAN);
    assert!(
        shape(1, vec![non_finite])
            .expand()
            .unwrap_err()
            .reason
            .contains("non-finite min_down_mbps")
    );
}

#[test]
fn reconcile_is_sorted_pure_and_allocates_distinct_attempts() {
    let desired = shape(4, vec![group("z", 1), group("a", 2)]);
    let observed = ClusterState::default();
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);

    let first = reconcile(&observed, &desired, now).unwrap();
    let second = reconcile(&observed, &desired, now).unwrap();
    assert_eq!(first, second);
    assert_eq!(observed, ClusterState::default());
    assert_eq!(first.observed_generation, 4);
    assert_eq!(
        first
            .actions
            .iter()
            .map(|action| action.node().0.as_str())
            .collect::<Vec<_>>(),
        vec!["a-0", "a-1", "z-0"]
    );
    assert_eq!(
        first
            .actions
            .iter()
            .map(|action| match action {
                NodeAction::Insert { attempt, .. } => attempt.0,
                other => panic!("unexpected action {other:?}"),
            })
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[test]
fn scale_down_marks_only_highest_logical_slots_for_deletion() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(50);
    let mut driver =
        ClusterDriver::new(shape(1, vec![group("gpu", 3)]), RetryPolicy::default()).unwrap();
    assert_eq!(drive(&mut driver, now).len(), 3);

    driver
        .update_desired(shape(2, vec![group("gpu", 1)]))
        .unwrap();
    assert!(drive(&mut driver, now).is_empty());

    let intent = |id: &str| {
        driver
            .state()
            .nodes
            .get(&LogicalNodeId(id.to_owned()))
            .unwrap()
            .intent
    };
    assert_eq!(
        intent("gpu-0"),
        NodeIntent::Active,
        "the lowest stable slot remains desired"
    );
    assert_eq!(intent("gpu-1"), NodeIntent::Deleting);
    assert_eq!(intent("gpu-2"), NodeIntent::Deleting);
}

#[test]
fn driver_records_operations_before_returning_effects_and_reaches_ready() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let mut driver =
        ClusterDriver::new(shape(1, vec![group("gpu", 1)]), RetryPolicy::default()).unwrap();

    let create = only_effect(&mut driver, now);
    let node = driver.state().nodes.get(&create.node).unwrap();
    assert_eq!(node.pending.as_ref().unwrap().id, create.operation);
    assert_eq!(node.record.stage, NodeStage::LeaseRequested);
    assert!(!driver.is_converged());

    succeed(
        &mut driver,
        &create,
        OperationOutcome::LeaseCreated(lease("lease-1", true)),
        now,
    );
    let bootstrap = only_effect(&mut driver, now);
    assert!(matches!(
        bootstrap.command,
        NodeManagerCommand::StartBootstrap(_)
    ));
    succeed(
        &mut driver,
        &bootstrap,
        OperationOutcome::BootstrapStarted {
            session_id: BootstrapSessionId(41),
        },
        now,
    );
    assert!(drive(&mut driver, now).is_empty());

    let id = bootstrap.node.clone();
    let attempt = bootstrap.operation.attempt;
    assert!(driver.apply_observation(
        &id,
        attempt,
        NodeObservation::SwactorJoined {
            session_id: BootstrapSessionId(41),
            swactor_id: SwactorId("swactor-1".to_owned()),
        },
        now,
    ));
    let convergence = only_effect(&mut driver, now);
    assert!(matches!(
        convergence.command,
        NodeManagerCommand::BootstrapConvergenceObserved { .. }
    ));
    succeed(
        &mut driver,
        &convergence,
        OperationOutcome::BootstrapConvergenceAccepted,
        now,
    );
    assert!(driver.apply_observation(
        &id,
        attempt,
        NodeObservation::BootstrapClosed {
            session_id: BootstrapSessionId(41),
        },
        now,
    ));
    assert!(drive(&mut driver, now).is_empty());
    assert!(driver.is_converged());
}

#[test]
fn deletion_cancels_bootstrap_then_destroys_lease_then_reaps() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(200);
    let mut driver =
        ClusterDriver::new(shape(1, vec![group("gpu", 1)]), RetryPolicy::default()).unwrap();
    let create = only_effect(&mut driver, now);
    succeed(
        &mut driver,
        &create,
        OperationOutcome::LeaseCreated(lease("lease-2", true)),
        now,
    );
    let bootstrap = only_effect(&mut driver, now);
    succeed(
        &mut driver,
        &bootstrap,
        OperationOutcome::BootstrapStarted {
            session_id: BootstrapSessionId(9),
        },
        now,
    );
    drive(&mut driver, now);

    driver.update_desired(shape(2, Vec::new())).unwrap();
    let cancel = only_effect(&mut driver, now);
    assert!(matches!(
        cancel.command,
        NodeManagerCommand::CancelBootstrap {
            session_id: BootstrapSessionId(9)
        }
    ));
    assert!(
        driver
            .state()
            .nodes
            .get(&cancel.node)
            .unwrap()
            .record
            .lease
            .is_some()
    );

    succeed(
        &mut driver,
        &cancel,
        OperationOutcome::BootstrapCancelled,
        now,
    );
    let destroy = only_effect(&mut driver, now);
    assert!(matches!(
        destroy.command,
        NodeManagerCommand::DestroyLease(_)
    ));
    succeed(&mut driver, &destroy, OperationOutcome::LeaseDestroyed, now);
    assert!(drive(&mut driver, now).is_empty());
    assert!(driver.state().nodes.is_empty());
    assert_eq!(driver.state().observed_generation, 2);
}

#[test]
fn failed_bootstrap_attempt_rejects_late_join_and_close() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(250);
    let mut driver =
        ClusterDriver::new(shape(1, vec![group("gpu", 1)]), RetryPolicy::default()).unwrap();
    let create = only_effect(&mut driver, now);
    succeed(
        &mut driver,
        &create,
        OperationOutcome::LeaseCreated(lease("lease-failed-bootstrap", true)),
        now,
    );
    let bootstrap = only_effect(&mut driver, now);
    succeed(
        &mut driver,
        &bootstrap,
        OperationOutcome::BootstrapStarted {
            session_id: BootstrapSessionId(91),
        },
        now,
    );

    assert!(driver.apply_observation(
        &bootstrap.node,
        bootstrap.operation.attempt,
        NodeObservation::BootstrapFailed {
            session_id: BootstrapSessionId(91),
            reason: "remote start failed".to_owned(),
        },
        now,
    ));
    assert!(!driver.apply_observation(
        &bootstrap.node,
        bootstrap.operation.attempt,
        NodeObservation::SwactorJoined {
            session_id: BootstrapSessionId(91),
            swactor_id: SwactorId("late-swactor".to_owned()),
        },
        now,
    ));
    assert!(!driver.apply_observation(
        &bootstrap.node,
        bootstrap.operation.attempt,
        NodeObservation::BootstrapClosed {
            session_id: BootstrapSessionId(91),
        },
        now,
    ));

    let node = driver.state().nodes.get(&bootstrap.node).unwrap();
    assert_eq!(node.record.stage, NodeStage::Failed);
    assert!(!node.record.ready);
    assert!(node.record.swactor.is_none());
}

#[test]
fn replacement_waits_for_cleanup_and_uses_a_fresh_attempt() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(300);
    let mut initial_group = group("gpu", 1);
    let mut driver = ClusterDriver::new(
        shape(1, vec![initial_group.clone()]),
        RetryPolicy::default(),
    )
    .unwrap();
    let create = only_effect(&mut driver, now);
    let old_attempt = create.operation.attempt;
    succeed(
        &mut driver,
        &create,
        OperationOutcome::LeaseCreated(lease("lease-3", true)),
        now,
    );
    let bootstrap = only_effect(&mut driver, now);
    succeed(
        &mut driver,
        &bootstrap,
        OperationOutcome::BootstrapStarted {
            session_id: BootstrapSessionId(10),
        },
        now,
    );
    drive(&mut driver, now);

    initial_group.shape.image = "node:v2".to_owned();
    driver
        .update_desired(shape(2, vec![initial_group.clone()]))
        .unwrap();
    initial_group.shape.image = "node:v3".to_owned();
    driver
        .update_desired(shape(3, vec![initial_group]))
        .unwrap();
    let cancel = only_effect(&mut driver, now);
    succeed(
        &mut driver,
        &cancel,
        OperationOutcome::BootstrapCancelled,
        now,
    );
    let destroy = only_effect(&mut driver, now);
    succeed(&mut driver, &destroy, OperationOutcome::LeaseDestroyed, now);
    let replacement_create = only_effect(&mut driver, now);
    assert_ne!(replacement_create.operation.attempt, old_attempt);
    let replacement = driver.state().nodes.get(&replacement_create.node).unwrap();
    assert_eq!(replacement.record.desired.shape.image, "node:v3");

    assert!(!driver.apply_observation(
        &replacement_create.node,
        old_attempt,
        NodeObservation::OperationFailed {
            operation: create.operation,
            error: EffectError::definite("late old result"),
        },
        now,
    ));
    assert_eq!(
        driver
            .state()
            .nodes
            .get(&replacement_create.node)
            .unwrap()
            .pending
            .as_ref()
            .unwrap()
            .id,
        replacement_create.operation
    );
}

#[test]
fn retry_backoff_is_per_node_and_stores_one_deadline() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(400);
    let retry = RetryPolicy {
        initial_delay: Duration::from_secs(5),
        max_delay: Duration::from_secs(30),
        jitter: Duration::ZERO,
        operation_timeout: Duration::from_secs(60),
        endpoint_probe_interval: Duration::from_secs(2),
    };
    let mut driver = ClusterDriver::new(shape(1, vec![group("gpu", 2)]), retry).unwrap();
    let effects = drive(&mut driver, now);
    assert_eq!(effects.len(), 2);
    let failed = &effects[0];
    let progressing = &effects[1];
    assert!(driver.submission_failed(failed, "provider unavailable", now));
    succeed(
        &mut driver,
        progressing,
        OperationOutcome::LeaseCreated(lease("lease-4", true)),
        now,
    );

    let next = drive(&mut driver, now);
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].node, progressing.node);
    assert!(matches!(
        next[0].command,
        NodeManagerCommand::StartBootstrap(_)
    ));
    let failed_node = driver.state().nodes.get(&failed.node).unwrap();
    assert_eq!(
        failed_node.retry.next_effect_at,
        now.checked_add(Duration::from_secs(5))
    );
    assert_eq!(driver.requeue_at(), failed_node.retry.next_effect_at);
}

#[test]
fn ambiguous_bootstrap_start_retries_adoption_without_replacing_the_attempt() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(450);
    let retry_delay = Duration::from_secs(3);
    let retry = RetryPolicy {
        initial_delay: retry_delay,
        max_delay: retry_delay,
        jitter: Duration::ZERO,
        ..RetryPolicy::default()
    };
    let mut driver = ClusterDriver::new(shape(1, vec![group("gpu", 1)]), retry).unwrap();
    let create = only_effect(&mut driver, now);
    succeed(
        &mut driver,
        &create,
        OperationOutcome::LeaseCreated(lease("lease-ambiguous-start", true)),
        now,
    );
    let start = only_effect(&mut driver, now);

    assert!(driver.apply_executor_result(
        ExecutorResult {
            node: start.node.clone(),
            operation: start.operation,
            result: Err(EffectError::ambiguous("bootstrap start timed out")),
        },
        now,
    ));
    assert!(drive(&mut driver, now).is_empty());
    let node = driver.state().nodes.get(&start.node).unwrap();
    assert_eq!(node.attempt, start.operation.attempt);
    assert_eq!(node.intent, NodeIntent::Active);
    assert_eq!(node.record.stage, NodeStage::EndpointKnown);
    assert!(node.active_bootstrap.is_none());
    assert!(node.retry.restart_at.is_none());
    assert_eq!(node.retry.next_effect_at, now.checked_add(retry_delay));

    assert!(driver.trigger_if_due(now + retry_delay));
    let retry = only_effect(&mut driver, now + retry_delay);
    assert_eq!(retry.operation.attempt, start.operation.attempt);
    assert_ne!(retry.operation, start.operation);
    assert!(matches!(
        retry.command,
        NodeManagerCommand::StartBootstrap(_)
    ));
}

#[test]
fn deleting_node_resolves_ambiguous_create_before_marking_destroyed() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(475);
    let mut driver =
        ClusterDriver::new(shape(1, vec![group("gpu", 1)]), RetryPolicy::default()).unwrap();
    let create = only_effect(&mut driver, now);
    let timeout_at = driver
        .state()
        .nodes
        .get(&create.node)
        .unwrap()
        .pending
        .as_ref()
        .unwrap()
        .deadline;

    driver.update_desired(shape(2, Vec::new())).unwrap();
    assert!(drive(&mut driver, now).is_empty());
    assert_eq!(
        driver.state().nodes.get(&create.node).unwrap().intent,
        NodeIntent::Deleting
    );
    let due = driver.pending_operations_due(timeout_at).pop().unwrap();
    assert!(driver.operation_timed_out(&due, "ambiguous create", timeout_at));
    assert!(drive(&mut driver, timeout_at).is_empty());

    let retry_at = timeout_at + Duration::from_secs(1);
    assert!(driver.trigger_if_due(retry_at));
    let adopt = only_effect(&mut driver, retry_at);
    assert!(matches!(adopt.command, NodeManagerCommand::CreateLease(_)));
    succeed(
        &mut driver,
        &adopt,
        OperationOutcome::LeaseCreated(lease("adopted-before-delete", true)),
        retry_at,
    );
    let destroy = only_effect(&mut driver, retry_at);
    assert!(matches!(
        destroy.command,
        NodeManagerCommand::DestroyLease(_)
    ));
    succeed(
        &mut driver,
        &destroy,
        OperationOutcome::LeaseDestroyed,
        retry_at,
    );
    assert!(drive(&mut driver, retry_at).is_empty());
    assert!(driver.state().nodes.is_empty());
}

#[test]
fn deleting_node_resolves_ambiguous_bootstrap_before_cleanup() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(500);
    let mut driver =
        ClusterDriver::new(shape(1, vec![group("gpu", 1)]), RetryPolicy::default()).unwrap();
    let create = only_effect(&mut driver, now);
    succeed(
        &mut driver,
        &create,
        OperationOutcome::LeaseCreated(lease("lease-ambiguous-delete", true)),
        now,
    );
    let start = only_effect(&mut driver, now);
    let timeout_at = driver
        .state()
        .nodes
        .get(&start.node)
        .unwrap()
        .pending
        .as_ref()
        .unwrap()
        .deadline;

    driver.update_desired(shape(2, Vec::new())).unwrap();
    assert!(drive(&mut driver, now).is_empty());
    let due = driver.pending_operations_due(timeout_at).pop().unwrap();
    assert!(driver.operation_timed_out(&due, "ambiguous bootstrap", timeout_at));
    assert!(drive(&mut driver, timeout_at).is_empty());

    let retry_at = timeout_at + Duration::from_secs(1);
    assert!(driver.trigger_if_due(retry_at));
    let adopt = only_effect(&mut driver, retry_at);
    assert!(matches!(
        adopt.command,
        NodeManagerCommand::StartBootstrap(_)
    ));
    succeed(
        &mut driver,
        &adopt,
        OperationOutcome::BootstrapStarted {
            session_id: BootstrapSessionId(92),
        },
        retry_at,
    );
    let cancel = only_effect(&mut driver, retry_at);
    assert!(matches!(
        cancel.command,
        NodeManagerCommand::CancelBootstrap {
            session_id: BootstrapSessionId(92)
        }
    ));
    succeed(
        &mut driver,
        &cancel,
        OperationOutcome::BootstrapCancelled,
        retry_at,
    );
    let destroy = only_effect(&mut driver, retry_at);
    succeed(
        &mut driver,
        &destroy,
        OperationOutcome::LeaseDestroyed,
        retry_at,
    );
    assert!(drive(&mut driver, retry_at).is_empty());
    assert!(driver.state().nodes.is_empty());
}

#[test]
fn driver_rejects_generation_and_run_contract_violations() {
    let mut driver =
        ClusterDriver::new(shape(3, vec![group("gpu", 1)]), RetryPolicy::default()).unwrap();
    let mut changed = group("gpu", 1);
    changed.shape.image = "changed".to_owned();
    assert!(matches!(
        driver.update_desired(shape(3, vec![changed])),
        Err(DriverError::ShapeChangedWithoutGeneration { generation: 3 })
    ));
    assert!(matches!(
        driver.update_desired(shape(2, vec![group("gpu", 1)])),
        Err(DriverError::GenerationRegressed {
            current: 3,
            supplied: 2
        })
    ));
    let mut other_run = shape(4, vec![group("gpu", 1)]);
    other_run.run_id = RunId(9);
    assert!(matches!(
        driver.update_desired(other_run),
        Err(DriverError::RunChanged { .. })
    ));
}

#[test]
fn attempt_failure_cleans_up_immediately_but_delays_replacement() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(500);
    let retry = RetryPolicy {
        initial_delay: Duration::from_secs(10),
        max_delay: Duration::from_secs(10),
        jitter: Duration::ZERO,
        operation_timeout: Duration::from_secs(60),
        endpoint_probe_interval: Duration::from_secs(2),
    };
    let mut driver = ClusterDriver::new(shape(1, vec![group("gpu", 1)]), retry).unwrap();
    let create = only_effect(&mut driver, now);
    succeed(
        &mut driver,
        &create,
        OperationOutcome::LeaseCreated(lease("lease-5", true)),
        now,
    );
    let bootstrap = only_effect(&mut driver, now);
    assert!(driver.submission_failed(&bootstrap, "bootstrap refused", now));

    let destroy = only_effect(&mut driver, now);
    assert!(matches!(
        destroy.command,
        NodeManagerCommand::DestroyLease(_)
    ));
    let failed = driver.state().nodes.get(&destroy.node).unwrap();
    assert_eq!(failed.record.failed_at, Some(now));
    assert_eq!(
        failed.retry.restart_at,
        now.checked_add(Duration::from_secs(10))
    );
    succeed(&mut driver, &destroy, OperationOutcome::LeaseDestroyed, now);
    assert!(drive(&mut driver, now).is_empty());
    let destroyed = driver.state().nodes.get(&destroy.node).unwrap();
    assert_eq!(destroyed.record.stage, NodeStage::Destroyed);
    assert_eq!(
        driver.requeue_at(),
        now.checked_add(Duration::from_secs(10))
    );

    let restart_at = now + Duration::from_secs(10);
    assert!(driver.trigger_if_due(restart_at));
    let replacement = only_effect(&mut driver, restart_at);
    assert_ne!(replacement.operation.attempt, create.operation.attempt);
}

#[test]
fn endpoint_not_ready_uses_probe_deadline_without_counting_a_failure() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(600);
    let mut driver =
        ClusterDriver::new(shape(1, vec![group("gpu", 1)]), RetryPolicy::default()).unwrap();
    let create = only_effect(&mut driver, now);
    succeed(
        &mut driver,
        &create,
        OperationOutcome::LeaseCreated(lease("lease-6", false)),
        now,
    );
    let lookup = only_effect(&mut driver, now);
    assert!(matches!(
        lookup.command,
        NodeManagerCommand::LookupEndpoint(_)
    ));
    succeed(
        &mut driver,
        &lookup,
        OperationOutcome::EndpointLookup(None),
        now,
    );
    assert!(drive(&mut driver, now).is_empty());
    let node = driver.state().nodes.get(&lookup.node).unwrap();
    assert_eq!(node.retry.consecutive_failures, 0);
    let probe_at = now + Duration::from_secs(2);
    assert_eq!(node.retry.next_effect_at, Some(probe_at));

    assert!(driver.trigger_if_due(probe_at));
    let retried_lookup = only_effect(&mut driver, probe_at);
    assert!(matches!(
        retried_lookup.command,
        NodeManagerCommand::LookupEndpoint(_)
    ));
}

#[test]
fn executor_deadline_is_exposed_and_timeout_is_correlated() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(700);
    let retry = RetryPolicy {
        initial_delay: Duration::from_secs(3),
        max_delay: Duration::from_secs(30),
        jitter: Duration::ZERO,
        operation_timeout: Duration::from_secs(10),
        endpoint_probe_interval: Duration::from_secs(2),
    };
    let mut driver = ClusterDriver::new(shape(1, vec![group("gpu", 1)]), retry).unwrap();
    let create = only_effect(&mut driver, now);
    assert!(
        driver
            .pending_operations_due(now + Duration::from_secs(9))
            .is_empty()
    );
    let due = driver.pending_operations_due(now + Duration::from_secs(10));
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].operation, create.operation);

    let timeout_at = now + Duration::from_secs(10);
    assert!(driver.operation_timed_out(&due[0], "executor timeout", timeout_at));
    let node = driver.state().nodes.get(&create.node).unwrap();
    assert!(node.pending.is_none());
    assert_eq!(
        node.retry.next_effect_at,
        Some(timeout_at + Duration::from_secs(3))
    );
    assert!(!driver.operation_timed_out(&due[0], "duplicate timeout", timeout_at));
}

#[test]
fn deleting_node_never_becomes_ready_from_late_bootstrap_completion() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(800);
    let desired = shape(1, vec![group("gpu", 1)])
        .expand()
        .unwrap()
        .into_values()
        .next()
        .unwrap();
    let mut node = ManagedNode::new(NodeAttemptId(1), desired);
    node.intent = NodeIntent::Deleting;
    node.active_bootstrap = Some(BootstrapSessionId(8));
    node.record.swactor = Some(SwactorFacts {
        swactor_id: SwactorId("joined".to_owned()),
        joined_at: now,
        handed_off_at: None,
    });

    observe(
        &mut node,
        NodeObservation::BootstrapClosed {
            session_id: BootstrapSessionId(8),
        },
        now,
        &RetryPolicy::default(),
    );

    assert_eq!(node.intent, NodeIntent::Deleting);
    assert!(!node.record.ready);
    assert_eq!(node.record.stage, NodeStage::Dormant);
}

#[test]
fn same_generation_rejects_any_changed_shape_content() {
    let mut driver = ClusterDriver::new(shape(1, Vec::new()), RetryPolicy::default()).unwrap();
    let changed = shape(1, vec![group("empty", 0)]);

    assert!(matches!(
        driver.update_desired(changed),
        Err(DriverError::ShapeChangedWithoutGeneration { generation: 1 })
    ));
}

#[derive(Default)]
struct RejectingExecutor {
    seen: Vec<PlannedEffect>,
}

impl EffectExecutor for RejectingExecutor {
    type SubmitError = String;

    fn submit(&mut self, effect: &PlannedEffect) -> Result<(), Self::SubmitError> {
        self.seen.push(effect.clone());
        Err("queue closed".to_owned())
    }
}

#[test]
fn submission_failure_is_folded_after_pending_is_recorded() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(900);
    let mut driver =
        ClusterDriver::new(shape(1, vec![group("gpu", 1)]), RetryPolicy::default()).unwrap();
    let mut executor = RejectingExecutor::default();

    assert_eq!(
        driver
            .drive_until_blocked(now, &mut executor)
            .expect("driver pass"),
        0
    );
    assert_eq!(executor.seen.len(), 1);
    let node = driver.state().nodes.values().next().unwrap();
    assert!(node.pending.is_none());
    assert_eq!(node.record.stage, NodeStage::LeaseRequested);
    assert!(node.retry.next_effect_at > Some(now));
    assert!(
        node.retry
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("queue closed"))
    );
}

#[test]
fn repeated_queued_triggers_coalesce_to_one_dispatch() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let mut driver =
        ClusterDriver::new(shape(1, vec![group("gpu", 1)]), RetryPolicy::default()).unwrap();
    driver.trigger();
    driver.trigger();
    driver.trigger();

    let effects = drive(&mut driver, now);
    assert_eq!(effects.len(), 1);
    assert!(
        driver
            .state()
            .nodes
            .values()
            .next()
            .unwrap()
            .pending
            .is_some()
    );
    assert!(!driver.is_queued());
}

#[test]
fn cleanup_failure_retries_without_releasing_live_facts() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_100);
    let retry = RetryPolicy {
        initial_delay: Duration::from_secs(4),
        max_delay: Duration::from_secs(4),
        jitter: Duration::ZERO,
        operation_timeout: Duration::from_secs(60),
        endpoint_probe_interval: Duration::from_secs(2),
    };
    let mut driver = ClusterDriver::new(shape(1, vec![group("gpu", 1)]), retry).unwrap();
    let create = only_effect(&mut driver, now);
    succeed(
        &mut driver,
        &create,
        OperationOutcome::LeaseCreated(lease("cleanup", true)),
        now,
    );
    let bootstrap = only_effect(&mut driver, now);
    succeed(
        &mut driver,
        &bootstrap,
        OperationOutcome::BootstrapStarted {
            session_id: BootstrapSessionId(77),
        },
        now,
    );
    drive(&mut driver, now);
    driver.update_desired(shape(2, Vec::new())).unwrap();
    let cancel = only_effect(&mut driver, now);
    assert!(driver.submission_failed(&cancel, "cancel busy", now));

    let node = driver.state().nodes.get(&cancel.node).unwrap();
    assert_eq!(node.active_bootstrap, Some(BootstrapSessionId(77)));
    assert!(node.record.lease.is_some());
    assert!(drive(&mut driver, now).is_empty());

    let retry_at = now + Duration::from_secs(4);
    assert!(driver.trigger_if_due(retry_at));
    let retried = only_effect(&mut driver, retry_at);
    assert!(matches!(
        retried.command,
        NodeManagerCommand::CancelBootstrap { .. }
    ));
    assert_ne!(retried.operation, cancel.operation);
}
