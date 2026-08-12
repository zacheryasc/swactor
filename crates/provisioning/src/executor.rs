//! Identity-aware asynchronous effect execution for the cluster reconciler.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::reconciler::{
    ExecutorResult, NodeAttemptId, OperationId, OperationKind, OperationOutcome, PlannedEffect,
};

/// Blocking work accepted by an execution substrate.
pub type BlockingEffectWork = Box<dyn FnOnce() + Send + 'static>;

/// Substrate seam used to keep provider work outside reconcile transitions.
pub trait BlockingEffectSpawner: Clone + Send + Sync + 'static {
    type SpawnError: fmt::Display;

    fn spawn_blocking(&self, work: BlockingEffectWork) -> Result<(), Self::SpawnError>;
}

/// Provider/bootstrap implementation behind the identity-aware executor.
///
/// Implementations must use `(run_id, logical_node_id, attempt)` from the effect
/// as the external request identity. Create and bootstrap-start operations must
/// look up and adopt an existing resource for that identity before creating a
/// new one. Cancel and destroy must treat an already-absent resource as success.
pub trait EffectBackend: Send + Sync + 'static {
    fn execute(&self, effect: &PlannedEffect) -> Result<OperationOutcome, EffectError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectFailureDisposition {
    Definite,
    Ambiguous,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectError {
    pub reason: String,
    pub disposition: EffectFailureDisposition,
}

impl EffectError {
    pub fn definite(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            disposition: EffectFailureDisposition::Definite,
        }
    }

    pub fn ambiguous(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            disposition: EffectFailureDisposition::Ambiguous,
        }
    }
}

impl fmt::Display for EffectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.reason)
    }
}

impl std::error::Error for EffectError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutorSubmitError {
    pub reason: String,
}

impl ExecutorSubmitError {
    fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl fmt::Display for ExecutorSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.reason)
    }
}

impl std::error::Error for ExecutorSubmitError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutorOperationStatus {
    Unknown,
    InFlight,
    Completed,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AttemptResources {
    pub lease_live: bool,
    pub bootstrap_live: bool,
}

// Ledger transitions retain effects inline to avoid a heap allocation per operation.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
enum LedgerEntry {
    Running(PlannedEffect),
    Queued(PlannedEffect),
    Completed {
        effect: PlannedEffect,
        result: ExecutorResult,
    },
}

impl LedgerEntry {
    fn effect(&self) -> &PlannedEffect {
        match self {
            Self::Running(effect) | Self::Queued(effect) | Self::Completed { effect, .. } => effect,
        }
    }
}

#[derive(Default)]
struct ExecutorLedger {
    operations: BTreeMap<OperationId, LedgerEntry>,
    in_flight_by_attempt: BTreeMap<NodeAttemptId, OperationId>,
    running_by_attempt: BTreeMap<NodeAttemptId, OperationId>,
    queued_by_attempt: BTreeMap<NodeAttemptId, OperationId>,
    resources: BTreeMap<NodeAttemptId, AttemptResources>,
}

/// Process-local executor that deduplicates operation submissions and runs each
/// accepted operation as substrate-hosted blocking work.
///
/// One instance belongs to one `ClusterDriver` run. Completed outcomes remain
/// cached for that lifetime, so resubmitting an `OperationId` returns the
/// recorded result without executing the backend again.
pub struct IdempotentEffectExecutor<B, S>
where
    B: EffectBackend,
    S: BlockingEffectSpawner,
{
    backend: Arc<B>,
    spawner: S,
    ledger: Arc<Mutex<ExecutorLedger>>,
    result_tx: Sender<ExecutorResult>,
    result_rx: Receiver<ExecutorResult>,
}

impl<B, S> IdempotentEffectExecutor<B, S>
where
    B: EffectBackend,
    S: BlockingEffectSpawner,
{
    pub fn new(backend: B, spawner: S) -> Self {
        let (result_tx, result_rx) = mpsc::channel();
        Self {
            backend: Arc::new(backend),
            spawner,
            ledger: Arc::new(Mutex::new(ExecutorLedger::default())),
            result_tx,
            result_rx,
        }
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    pub fn drain_results(&mut self) -> Vec<ExecutorResult> {
        let mut results = Vec::new();
        loop {
            match self.result_rx.try_recv() {
                Ok(result) => results.push(result),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return results,
            }
        }
    }

    pub fn operation_status(&self, operation: OperationId) -> ExecutorOperationStatus {
        match lock_ledger(&self.ledger).operations.get(&operation) {
            None => ExecutorOperationStatus::Unknown,
            Some(LedgerEntry::Running(_) | LedgerEntry::Queued(_)) => {
                ExecutorOperationStatus::InFlight
            }
            Some(LedgerEntry::Completed { .. }) => ExecutorOperationStatus::Completed,
        }
    }

    /// Classifies a still-running operation as ambiguous after its stored
    /// deadline. The backend work is not forcibly cancelled; a late completion
    /// is discarded, and any retry must adopt by the stable attempt identity.
    pub fn expire(&self, operation: OperationId, reason: impl Into<String>) -> bool {
        let result = {
            let mut ledger = lock_ledger(&self.ledger);
            let effect = match ledger.operations.get(&operation).cloned() {
                Some(LedgerEntry::Running(effect) | LedgerEntry::Queued(effect)) => effect,
                None | Some(LedgerEntry::Completed { .. }) => return false,
            };
            let result = ExecutorResult {
                node: effect.node.clone(),
                operation,
                result: Err(EffectError::ambiguous(reason)),
            };
            ledger.operations.insert(
                operation,
                LedgerEntry::Completed {
                    effect,
                    result: result.clone(),
                },
            );
            if ledger.in_flight_by_attempt.get(&operation.attempt) == Some(&operation) {
                ledger.in_flight_by_attempt.remove(&operation.attempt);
            }
            if ledger.queued_by_attempt.get(&operation.attempt) == Some(&operation) {
                ledger.queued_by_attempt.remove(&operation.attempt);
            }
            result
        };
        let _ = self.result_tx.send(result);
        true
    }

    pub fn attempt_resources(&self, attempt: NodeAttemptId) -> AttemptResources {
        lock_ledger(&self.ledger)
            .resources
            .get(&attempt)
            .copied()
            .unwrap_or_default()
    }

    fn submit_inner(&mut self, effect: &PlannedEffect) -> Result<(), ExecutorSubmitError> {
        let should_spawn = {
            let mut ledger = lock_ledger(&self.ledger);
            if let Some(entry) = ledger.operations.get(&effect.operation).cloned() {
                if entry.effect() != effect {
                    return Err(ExecutorSubmitError::new(format!(
                        "operation identity for attempt {} sequence {} was reused with different input",
                        effect.operation.attempt.0, effect.operation.sequence
                    )));
                }
                return match entry {
                    LedgerEntry::Completed { result, .. } => {
                        self.result_tx.send(result).map_err(|_| {
                            ExecutorSubmitError::new("executor result receiver is closed")
                        })
                    }
                    LedgerEntry::Running(_) | LedgerEntry::Queued(_) => Ok(()),
                };
            }
            if let Some(in_flight) = ledger.in_flight_by_attempt.get(&effect.operation.attempt) {
                return Err(ExecutorSubmitError::new(format!(
                    "attempt {} already has operation {} in flight",
                    effect.operation.attempt.0, in_flight.sequence
                )));
            }

            let attempt = effect.operation.attempt;
            let physically_running = ledger.running_by_attempt.contains_key(&attempt);
            let entry = if physically_running {
                if ledger.queued_by_attempt.contains_key(&attempt) {
                    return Err(ExecutorSubmitError::new(format!(
                        "attempt {} already has an adoption operation queued",
                        attempt.0
                    )));
                }
                ledger.queued_by_attempt.insert(attempt, effect.operation);
                LedgerEntry::Queued(effect.clone())
            } else {
                ledger.running_by_attempt.insert(attempt, effect.operation);
                LedgerEntry::Running(effect.clone())
            };
            ledger.operations.insert(effect.operation, entry);
            ledger
                .in_flight_by_attempt
                .insert(attempt, effect.operation);
            !physically_running
        };

        if !should_spawn {
            return Ok(());
        }
        if let Err(error) = spawn_effect(
            self.spawner.clone(),
            Arc::clone(&self.backend),
            Arc::clone(&self.ledger),
            self.result_tx.clone(),
            effect.clone(),
        ) {
            let mut ledger = lock_ledger(&self.ledger);
            if matches!(
                ledger.operations.get(&effect.operation),
                Some(LedgerEntry::Running(running)) if running == effect
            ) {
                ledger.operations.remove(&effect.operation);
            }
            if ledger.in_flight_by_attempt.get(&effect.operation.attempt) == Some(&effect.operation)
            {
                ledger
                    .in_flight_by_attempt
                    .remove(&effect.operation.attempt);
            }
            if ledger.running_by_attempt.get(&effect.operation.attempt) == Some(&effect.operation) {
                ledger.running_by_attempt.remove(&effect.operation.attempt);
            }
            return Err(ExecutorSubmitError::new(format!(
                "blocking effect submission failed: {error}"
            )));
        }
        Ok(())
    }
}

fn spawn_effect<B, S>(
    spawner: S,
    backend: Arc<B>,
    ledger: Arc<Mutex<ExecutorLedger>>,
    result_tx: Sender<ExecutorResult>,
    submitted: PlannedEffect,
) -> Result<(), S::SpawnError>
where
    B: EffectBackend,
    S: BlockingEffectSpawner,
{
    let nested_spawner = spawner.clone();
    spawner.spawn_blocking(Box::new(move || {
        let operation = submitted.operation;
        let expected_kind = OperationKind::for_command(&submitted.command);
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| backend.execute(&submitted)))
                .map_err(|panic| {
                    EffectError::definite(format!(
                        "effect backend panicked: {}",
                        panic_reason(panic)
                    ))
                })
                .and_then(|result| result)
                .and_then(|outcome| {
                    if outcome.kind() == expected_kind {
                        Ok(outcome)
                    } else {
                        Err(EffectError::definite(format!(
                            "executor returned {:?} for {:?}",
                            outcome.kind(),
                            expected_kind
                        )))
                    }
                });
        let executor_result = ExecutorResult {
            node: submitted.node.clone(),
            operation,
            result,
        };
        let (publish, next) = {
            let mut ledger = lock_ledger(&ledger);
            let publish = matches!(
                ledger.operations.get(&operation),
                Some(LedgerEntry::Running(effect)) if effect == &submitted
            );
            if publish {
                ledger.operations.insert(
                    operation,
                    LedgerEntry::Completed {
                        effect: submitted,
                        result: executor_result.clone(),
                    },
                );
                if ledger.in_flight_by_attempt.get(&operation.attempt) == Some(&operation) {
                    ledger.in_flight_by_attempt.remove(&operation.attempt);
                }
                if let Ok(outcome) = &executor_result.result {
                    update_resources(&mut ledger, operation.attempt, outcome);
                }
            }
            if ledger.running_by_attempt.get(&operation.attempt) == Some(&operation) {
                ledger.running_by_attempt.remove(&operation.attempt);
            }
            let next = promote_queued(&mut ledger, operation.attempt);
            (publish, next)
        };
        if publish {
            let _ = result_tx.send(executor_result);
        }
        if let Some(next) = next {
            spawn_promoted(nested_spawner, backend, ledger, result_tx, next);
        }
    }))
}

fn spawn_promoted<B, S>(
    spawner: S,
    backend: Arc<B>,
    ledger: Arc<Mutex<ExecutorLedger>>,
    result_tx: Sender<ExecutorResult>,
    effect: PlannedEffect,
) where
    B: EffectBackend,
    S: BlockingEffectSpawner,
{
    if let Err(error) = spawn_effect(
        spawner.clone(),
        Arc::clone(&backend),
        Arc::clone(&ledger),
        result_tx.clone(),
        effect.clone(),
    ) {
        let (result, next) = record_spawn_failure(
            &ledger,
            &effect,
            format!("blocking effect submission failed: {error}"),
        );
        if let Some(result) = result {
            let _ = result_tx.send(result);
        }
        if let Some(next) = next {
            spawn_promoted(spawner, backend, ledger, result_tx, next);
        }
    }
}

fn record_spawn_failure(
    ledger: &Mutex<ExecutorLedger>,
    effect: &PlannedEffect,
    reason: String,
) -> (Option<ExecutorResult>, Option<PlannedEffect>) {
    let mut ledger = lock_ledger(ledger);
    let operation = effect.operation;
    let publish = matches!(
        ledger.operations.get(&operation),
        Some(LedgerEntry::Running(running)) if running == effect
    );
    let result = publish.then(|| ExecutorResult {
        node: effect.node.clone(),
        operation,
        result: Err(EffectError::definite(reason)),
    });
    if let Some(result) = result.as_ref() {
        ledger.operations.insert(
            operation,
            LedgerEntry::Completed {
                effect: effect.clone(),
                result: result.clone(),
            },
        );
    }
    if ledger.in_flight_by_attempt.get(&operation.attempt) == Some(&operation) {
        ledger.in_flight_by_attempt.remove(&operation.attempt);
    }
    if ledger.running_by_attempt.get(&operation.attempt) == Some(&operation) {
        ledger.running_by_attempt.remove(&operation.attempt);
    }
    let next = promote_queued(&mut ledger, operation.attempt);
    (result, next)
}

fn promote_queued(ledger: &mut ExecutorLedger, attempt: NodeAttemptId) -> Option<PlannedEffect> {
    let operation = ledger.queued_by_attempt.remove(&attempt)?;
    let Some(LedgerEntry::Queued(effect)) = ledger.operations.get(&operation).cloned() else {
        return None;
    };
    ledger
        .operations
        .insert(operation, LedgerEntry::Running(effect.clone()));
    ledger.running_by_attempt.insert(attempt, operation);
    Some(effect)
}

impl<B, S> crate::reconciler::EffectExecutor for IdempotentEffectExecutor<B, S>
where
    B: EffectBackend,
    S: BlockingEffectSpawner,
{
    type SubmitError = ExecutorSubmitError;

    fn submit(&mut self, effect: &PlannedEffect) -> Result<(), Self::SubmitError> {
        self.submit_inner(effect)
    }
}

fn lock_ledger(ledger: &Mutex<ExecutorLedger>) -> MutexGuard<'_, ExecutorLedger> {
    ledger
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn panic_reason(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(reason) = panic.downcast_ref::<&str>() {
        (*reason).to_owned()
    } else if let Some(reason) = panic.downcast_ref::<String>() {
        reason.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

fn update_resources(
    ledger: &mut ExecutorLedger,
    attempt: NodeAttemptId,
    outcome: &OperationOutcome,
) {
    let resources = ledger.resources.entry(attempt).or_default();
    match outcome {
        OperationOutcome::LeaseCreated(_) => resources.lease_live = true,
        OperationOutcome::BootstrapStarted { .. } => resources.bootstrap_live = true,
        OperationOutcome::BootstrapCancelled => resources.bootstrap_live = false,
        OperationOutcome::LeaseDestroyed => {
            resources.bootstrap_live = false;
            resources.lease_live = false;
        }
        OperationOutcome::EndpointLookup(_) | OperationOutcome::BootstrapConvergenceAccepted => {}
    }
}
