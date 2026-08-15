use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use provisioning::*;

#[derive(Clone, Default)]
struct InlineSpawner;

impl BlockingEffectSpawner for InlineSpawner {
    type SpawnError = Infallible;

    fn spawn_blocking(&self, work: BlockingEffectWork) -> Result<(), Self::SpawnError> {
        work();
        Ok(())
    }
}

#[derive(Clone, Default)]
struct QueuedSpawner {
    work: Arc<Mutex<Vec<BlockingEffectWork>>>,
}

impl QueuedSpawner {
    fn run_all(&self) {
        loop {
            let work = std::mem::take(&mut *self.work.lock().unwrap());
            if work.is_empty() {
                return;
            }
            for operation in work {
                operation();
            }
        }
    }

    fn len(&self) -> usize {
        self.work.lock().unwrap().len()
    }
}

impl BlockingEffectSpawner for QueuedSpawner {
    type SpawnError = Infallible;

    fn spawn_blocking(&self, work: BlockingEffectWork) -> Result<(), Self::SpawnError> {
        self.work.lock().unwrap().push(work);
        Ok(())
    }
}

#[derive(Default)]
struct CountingBackend {
    calls: AtomicUsize,
}

impl EffectBackend for CountingBackend {
    fn execute(&self, effect: &PlannedEffect) -> Result<OperationOutcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(outcome_for(effect))
    }
}

struct PanickingBackend;

impl EffectBackend for PanickingBackend {
    fn execute(&self, _effect: &PlannedEffect) -> Result<OperationOutcome, EffectError> {
        panic!("backend panic")
    }
}

#[derive(Default)]
struct AdoptingBackend {
    creates: AtomicUsize,
    lease: Mutex<Option<CreateLeaseResult>>,
}

impl EffectBackend for AdoptingBackend {
    fn execute(&self, effect: &PlannedEffect) -> Result<OperationOutcome, EffectError> {
        if !matches!(effect.command, NodeManagerCommand::CreateLease(_)) {
            return Ok(outcome_for(effect));
        }
        let mut live = self.lease.lock().unwrap();
        if let Some(lease) = live.as_ref() {
            return Ok(OperationOutcome::LeaseCreated(lease.clone()));
        }
        self.creates.fetch_add(1, Ordering::SeqCst);
        *live = Some(lease_result(effect.operation.attempt));
        Err(EffectError::ambiguous(
            "provider accepted create before transport failed",
        ))
    }
}

fn logical_spec(node: &str) -> LogicalNodeSpec {
    LogicalNodeSpec {
        run_id: RunId(3),
        logical_node_id: LogicalNodeId(node.to_owned()),
        group_id: NodeGroupId("workers".to_owned()),
        role: RoleId("worker".to_owned()),
        provider: ProviderKind::new("mock"),
        shape: DesiredNodeShape {
            image: "node:v1".to_owned(),
            disk_gb: 10,
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
            verify_commands: Vec::new(),
            start_swactor_command: "swactor".to_owned(),
            stdout_sources: Vec::new(),
            stderr_sources: Vec::new(),
            env: Vec::new(),
            args: Vec::new(),
            mounts: Vec::new(),
        },
        swarm_join: SwarmJoinSpec {
            orch_swactor_addr: "orchestrator".to_owned(),
            join_token_ref: "token".to_owned(),
            expected_logical_node_id: LogicalNodeId(node.to_owned()),
        },
    }
}

fn create_effect(node: &str, attempt: u64, sequence: u64) -> PlannedEffect {
    PlannedEffect {
        node: LogicalNodeId(node.to_owned()),
        operation: OperationId {
            attempt: NodeAttemptId(attempt),
            sequence,
        },
        command: NodeManagerCommand::CreateLease(CreateLeaseRequest {
            spec: logical_spec(node),
        }),
    }
}

fn lease_result(attempt: NodeAttemptId) -> CreateLeaseResult {
    let provider = ProviderKind::new("mock");
    let lease_id = ProviderLeaseId(format!("lease-{}", attempt.0));
    CreateLeaseResult {
        lease: LeaseFacts {
            provider: provider.clone(),
            lease_id: lease_id.clone(),
            provider_contract_id: format!("contract-{}", attempt.0),
            offer_id: None,
            destroy_handle: DestroyHandle {
                provider,
                lease_id,
                provider_contract_id: format!("contract-{}", attempt.0),
            },
            provider_metadata: BTreeMap::new(),
        },
        endpoint: Some(SshEndpoint {
            host: "127.0.0.1".to_owned(),
            port: 22,
            user: "root".to_owned(),
            auth_ref: "key".to_owned(),
        }),
    }
}

fn outcome_for(effect: &PlannedEffect) -> OperationOutcome {
    match &effect.command {
        NodeManagerCommand::CreateLease(_) => {
            OperationOutcome::LeaseCreated(lease_result(effect.operation.attempt))
        }
        NodeManagerCommand::LookupEndpoint(_) => {
            OperationOutcome::EndpointLookup(Some(SshEndpoint {
                host: "127.0.0.1".to_owned(),
                port: 22,
                user: "root".to_owned(),
                auth_ref: "key".to_owned(),
            }))
        }
        NodeManagerCommand::StartBootstrap(_) => OperationOutcome::BootstrapStarted {
            session_id: BootstrapSessionId(effect.operation.attempt.0),
        },
        NodeManagerCommand::BootstrapConvergenceObserved { .. } => {
            OperationOutcome::BootstrapConvergenceAccepted
        }
        NodeManagerCommand::CancelBootstrap { .. } => OperationOutcome::BootstrapCancelled,
        NodeManagerCommand::DestroyLease(_) => OperationOutcome::LeaseDestroyed,
    }
}

#[test]
fn repeated_operation_id_returns_cached_outcome_without_reexecution() {
    let effect = create_effect("worker-0", 1, 1);
    let mut executor = IdempotentEffectExecutor::new(CountingBackend::default(), InlineSpawner);

    executor.submit(&effect).unwrap();
    executor.submit(&effect).unwrap();

    let results = executor.drain_results();
    assert_eq!(results.len(), 2);
    assert!(
        results
            .iter()
            .all(|result| result.operation == effect.operation)
    );
    assert_eq!(executor.backend().calls.load(Ordering::SeqCst), 1);
}

#[test]
fn operation_identity_reuse_with_different_input_is_rejected() {
    let mut executor = IdempotentEffectExecutor::new(CountingBackend::default(), InlineSpawner);
    let first = create_effect("worker-0", 1, 1);
    executor.submit(&first).unwrap();
    executor.drain_results();

    let mut conflicting = first.clone();
    let NodeManagerCommand::CreateLease(request) = &mut conflicting.command else {
        unreachable!();
    };
    request.spec.shape.image = "different:v2".to_owned();
    let error = executor.submit(&conflicting).unwrap_err();

    assert!(error.reason.contains("reused with different input"));
    assert_eq!(executor.backend().calls.load(Ordering::SeqCst), 1);
    assert!(executor.drain_results().is_empty());
}

#[test]
fn backend_panic_becomes_a_correlated_error_result() {
    let effect = create_effect("worker-0", 1, 1);
    let mut executor = IdempotentEffectExecutor::new(PanickingBackend, InlineSpawner);

    executor.submit(&effect).unwrap();
    let results = executor.drain_results();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].node, effect.node);
    assert_eq!(results[0].operation, effect.operation);
    assert!(
        results[0]
            .result
            .as_ref()
            .unwrap_err()
            .reason
            .contains("effect backend panicked: backend panic")
    );
    assert_eq!(
        executor.operation_status(effect.operation),
        ExecutorOperationStatus::Completed
    );
}

#[test]
fn expired_ambiguous_create_is_adopted_and_late_result_is_discarded() {
    let spawner = QueuedSpawner::default();
    let mut executor = IdempotentEffectExecutor::new(AdoptingBackend::default(), spawner.clone());
    let effect = create_effect("worker-0", 1, 1);
    executor.submit(&effect).unwrap();

    assert!(executor.expire(effect.operation, "ambiguous timeout"));
    let timeout = executor.drain_results();
    assert_eq!(timeout.len(), 1);
    assert_eq!(
        timeout[0].result.as_ref().unwrap_err(),
        &EffectError::ambiguous("ambiguous timeout")
    );

    let retry = create_effect("worker-0", 1, 2);
    executor.submit(&retry).unwrap();
    assert_eq!(
        spawner.len(),
        1,
        "retry must wait for the ambiguous physical operation to finish"
    );
    spawner.run_all();
    let retry_result = executor.drain_results();
    assert_eq!(retry_result.len(), 1);
    assert_eq!(retry_result[0].operation, retry.operation);
    assert!(matches!(
        retry_result[0].result,
        Ok(OperationOutcome::LeaseCreated(_))
    ));
    assert_eq!(executor.backend().creates.load(Ordering::SeqCst), 1);

    executor.submit(&effect).unwrap();
    assert_eq!(
        executor.drain_results()[0].result.as_ref().unwrap_err(),
        &EffectError::ambiguous("ambiguous timeout")
    );
}
#[test]
fn different_attempts_are_accepted_as_independent_blocking_work() {
    let spawner = QueuedSpawner::default();
    let mut executor = IdempotentEffectExecutor::new(CountingBackend::default(), spawner.clone());
    let first = create_effect("worker-0", 1, 1);
    let second = create_effect("worker-1", 2, 1);

    executor.submit(&first).unwrap();
    executor.submit(&second).unwrap();

    assert_eq!(spawner.len(), 2);
    assert_eq!(
        executor.operation_status(first.operation),
        ExecutorOperationStatus::InFlight
    );
    assert_eq!(
        executor.operation_status(second.operation),
        ExecutorOperationStatus::InFlight
    );
    spawner.run_all();
    assert_eq!(executor.drain_results().len(), 2);
}

#[test]
fn one_attempt_cannot_run_two_distinct_operations_concurrently() {
    let spawner = QueuedSpawner::default();
    let mut executor = IdempotentEffectExecutor::new(CountingBackend::default(), spawner.clone());
    let first = create_effect("worker-0", 1, 1);
    let second = create_effect("worker-0", 1, 2);

    executor.submit(&first).unwrap();
    let error = executor.submit(&second).unwrap_err();

    assert!(error.reason.contains("already has operation"));
    assert_eq!(spawner.len(), 1);
}

#[test]
fn newer_create_operation_adopts_an_ambiguous_live_resource() {
    let mut executor = IdempotentEffectExecutor::new(AdoptingBackend::default(), InlineSpawner);
    let ambiguous = create_effect("worker-0", 1, 1);
    let retry = create_effect("worker-0", 1, 2);

    executor.submit(&ambiguous).unwrap();
    let first = executor.drain_results();
    assert_eq!(first.len(), 1);
    assert!(first[0].result.is_err());

    executor.submit(&retry).unwrap();
    let second = executor.drain_results();
    assert_eq!(second.len(), 1);
    assert!(matches!(
        second[0].result,
        Ok(OperationOutcome::LeaseCreated(_))
    ));
    assert_eq!(executor.backend().creates.load(Ordering::SeqCst), 1);
}

#[test]
fn successful_outcomes_track_one_live_lease_and_bootstrap_per_attempt() {
    let attempt = NodeAttemptId(4);
    let mut executor = IdempotentEffectExecutor::new(CountingBackend::default(), InlineSpawner);
    let create = create_effect("worker-0", attempt.0, 1);
    executor.submit(&create).unwrap();
    executor.drain_results();
    assert_eq!(
        executor.attempt_resources(attempt),
        AttemptResources {
            lease_live: true,
            bootstrap_live: false
        }
    );

    let mut start = create.clone();
    start.operation.sequence = 2;
    start.command = NodeManagerCommand::StartBootstrap(BootstrapSessionSpec {
        run_id: RunId(3),
        logical_node_id: LogicalNodeId("worker-0".to_owned()),
        lease_id: ProviderLeaseId("lease-4".to_owned()),
        ssh: lease_result(attempt).endpoint.unwrap(),
        boot: logical_spec("worker-0").boot,
        swarm_join: logical_spec("worker-0").swarm_join,
        telemetry: TelemetryStreamId("bootstrap".to_owned()),
    });
    executor.submit(&start).unwrap();
    executor.drain_results();
    assert!(executor.attempt_resources(attempt).bootstrap_live);

    let mut cancel = start.clone();
    cancel.operation.sequence = 3;
    cancel.command = NodeManagerCommand::CancelBootstrap {
        session_id: BootstrapSessionId(attempt.0),
    };
    executor.submit(&cancel).unwrap();
    executor.drain_results();
    assert!(!executor.attempt_resources(attempt).bootstrap_live);

    let mut destroy = cancel;
    destroy.operation.sequence = 4;
    destroy.command = NodeManagerCommand::DestroyLease(lease_result(attempt).lease.destroy_handle);
    executor.submit(&destroy).unwrap();
    executor.drain_results();
    assert_eq!(
        executor.attempt_resources(attempt),
        AttemptResources::default()
    );
}
