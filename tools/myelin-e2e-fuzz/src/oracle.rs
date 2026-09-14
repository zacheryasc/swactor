//! Behavioral invariants checked against observed program output.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::budget::Budget;
use crate::ir::{
    ActionObservation, ActionOp, BarrierObservation, BehaviorCase, CaseObservation, DataKind,
    DescriptorFinish, DescriptorObservation, DescriptorReadMethod, DescriptorTerminalResult,
    ExecutionObservation, ExpectedOutcome, FailureInjection, LaunchFailureKind, ProcessProgram,
    ProcessStopPhase,
};

/// Durable behavioral identity, independent of request, process, path and attempt IDs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureSignature {
    pub invariant: String,
    pub failure_class: FailureClass,
    pub causal_role: CausalRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_outcome: Option<ObservedOutcome>,
}

/// Typed outcome evidence without diagnostic text or race-group identities.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedOutcome {
    pub expected: OutcomeExpectation,
    pub outcome: String,
    pub errno: Option<i32>,
    pub error_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum OutcomeExpectation {
    Ok,
    Error(i32),
    Exception(String),
    Linearized { successes: u8, error: i32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    ContractViolation,
    BudgetExceeded,
    MissingEvidence,
    InvalidEvidence,
    MissingIncarnation,
    InvalidIncarnation,
    ConflictingIncarnation,
    MilestoneOrder,
    MissingStreamSource,
    StreamSourceContent,
    ReservationNotReleased,
    MissingTransfer,
    PayloadMismatch,
    OutcomeMismatch,
    NamespaceHistoryConflict,
    RetainedBlobMismatch,
    MissingStreamEof,
    FrameCountMismatch,
    FrameOrderMismatch,
    FrameContentMismatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "role", content = "operation", rename_all = "snake_case")]
pub enum CausalRole {
    Case,
    Lifecycle,
    Namespace,
    Action(SemanticAction),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticAction {
    PublishBlob,
    ReadBlob,
    StreamWrite,
    StreamRead,
    StreamReadWithRetry,
    StreamRoundTrip,
    GatedStreamWrite,
    GatedStreamRead,
    StreamReadInto,
    Lookup,
    AwaitEntry,
    WaitForQuiescent,
    Rename,
    Unlink,
    DescriptorWrite,
    DescriptorRead,
    MappingExportClose,
}

impl From<&ActionOp> for SemanticAction {
    fn from(action: &ActionOp) -> Self {
        match action {
            ActionOp::PublishBlob { .. } => Self::PublishBlob,
            ActionOp::ReadBlob { .. } => Self::ReadBlob,
            ActionOp::StreamWrite { .. } => Self::StreamWrite,
            ActionOp::StreamRead { .. } => Self::StreamRead,
            ActionOp::StreamReadWithRetry { .. } => Self::StreamReadWithRetry,
            ActionOp::StreamRoundTrip { .. } => Self::StreamRoundTrip,
            ActionOp::GatedStreamWrite { .. } => Self::GatedStreamWrite,
            ActionOp::GatedStreamRead { .. } => Self::GatedStreamRead,
            ActionOp::StreamReadInto { .. } => Self::StreamReadInto,
            ActionOp::Lookup { .. } => Self::Lookup,
            ActionOp::AwaitEntry { .. } => Self::AwaitEntry,
            ActionOp::WaitForQuiescent { .. } => Self::WaitForQuiescent,
            ActionOp::Rename { .. } => Self::Rename,
            ActionOp::Unlink { .. } => Self::Unlink,
            ActionOp::DescriptorWrite { .. } => Self::DescriptorWrite,
            ActionOp::DescriptorRead { .. } => Self::DescriptorRead,
            ActionOp::MappingExportClose { .. } => Self::MappingExportClose,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleViolation {
    pub invariant: &'static str,
    pub detail: String,
    pub signature: FailureSignature,
}

impl OracleViolation {
    fn new(invariant: &'static str, detail: impl Into<String>) -> Self {
        let failure_class = match invariant {
            "execution_budget" => FailureClass::BudgetExceeded,
            "missing_execution" | "missing_action" | "namespace_model_evidence" => {
                FailureClass::MissingEvidence
            }
            "payload_integrity" | "stream_bytes" | "blob_publication" => {
                FailureClass::PayloadMismatch
            }
            "unexpected_error" | "typed_error" | "typed_exception" => FailureClass::OutcomeMismatch,
            "namespace_linearizability" => FailureClass::NamespaceHistoryConflict,
            _ => FailureClass::ContractViolation,
        };
        let causal_role = if invariant.starts_with("namespace_") {
            CausalRole::Namespace
        } else if matches!(
            invariant,
            "duplicate_lifecycle"
                | "bootstrap_resolution"
                | "context_order"
                | "missing_context"
                | "user_before_ready"
                | "terminal_resolution"
                | "output_after_terminal"
                | "process_failure"
                | "stop_phase"
                | "launch_failure_kind"
        ) {
            CausalRole::Lifecycle
        } else {
            CausalRole::Case
        };
        Self {
            invariant,
            detail: detail.into(),
            signature: FailureSignature {
                invariant: invariant.to_owned(),
                failure_class,
                causal_role,
                observed_outcome: None,
            },
        }
    }

    fn classified(mut self, failure_class: FailureClass, causal_role: CausalRole) -> Self {
        self.signature.failure_class = failure_class;
        self.signature.causal_role = causal_role;
        self
    }

    fn at_action(mut self, action: &ActionOp) -> Self {
        self.signature.causal_role = CausalRole::Action(action.into());
        self
    }

    fn with_outcome(mut self, expected: ExpectedOutcome, result: &ActionObservation) -> Self {
        self.signature.observed_outcome = Some(ObservedOutcome {
            expected: match expected {
                ExpectedOutcome::Ok => OutcomeExpectation::Ok,
                ExpectedOutcome::Error(errno) => OutcomeExpectation::Error(errno),
                ExpectedOutcome::Exception(exception) => {
                    OutcomeExpectation::Exception(exception.as_str().to_owned())
                }
                ExpectedOutcome::Linearized {
                    successes, error, ..
                } => OutcomeExpectation::Linearized { successes, error },
            },
            outcome: result.outcome.clone(),
            errno: result.errno,
            error_type: result.error_type.clone(),
        });
        self
    }
}

impl std::fmt::Display for OracleViolation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.invariant, self.detail)
    }
}

impl std::error::Error for OracleViolation {}

pub struct BehaviorOracle;

fn is_barrier_record(result: &ActionObservation) -> bool {
    result.outcome == "barrier"
}

fn check_budget(budget: Option<&Budget>, predicate: &str) -> Result<(), OracleViolation> {
    if let Some(budget) = budget {
        budget
            .check(predicate)
            .map_err(|detail| OracleViolation::new("execution_budget", detail))?;
    }
    Ok(())
}

/// An action exception explains an unexecuted suffix only when the original
/// cursor ends in a normal nonzero Python exit, not cancellation or a failed
/// transport/bootstrap. The collector separately proves cursor durability.
fn terminal_action_error(execution: &ExecutionObservation) -> Option<&ActionObservation> {
    if execution.exit_success || !execution.terminal {
        return None;
    }
    let status: serde_json::Value = serde_json::from_str(execution.exit_status.as_deref()?).ok()?;
    if status.get("kind")?.as_str()? != "code"
        || status.get("value")?.as_i64()? == 0
        || execution.lifecycle.iter().any(|event| {
            matches!(
                event.as_str(),
                "bootstrap_failed"
                    | "spawn_failed"
                    | "spawn_rejected"
                    | "process_error"
                    | "stop_accepted"
                    | "stop_rejected"
            )
        })
    {
        return None;
    }
    let result = execution.results.last()?;
    let error_type = result.error_type.as_deref()?;
    if result.outcome != "error"
        || error_type.is_empty()
        || matches!(
            error_type,
            "TimeoutError" | "CancelledError" | "KeyboardInterrupt" | "SystemExit" | "MemoryError"
        )
    {
        return None;
    }
    Some(result)
}

/// Recognize a complete, causally identified action prefix ending in an
/// unexpected typed exception. Never fill in the suffix the program did not run.
pub(crate) fn recorded_action_failure(
    program: &ProcessProgram,
    execution: &ExecutionObservation,
) -> Option<usize> {
    let failure = terminal_action_error(execution)?;
    if execution.process != program.id
        || execution.logical_node_id != program.logical_node_id
        || execution.request_id.is_empty()
        || BehaviorOracle::verify_execution(execution).is_err()
    {
        return None;
    }
    let mut next = 0;
    for result in &execution.results {
        let action = program.actions.get(result.step)?;
        if result.process != program.id
            || !stream_record_action_matches(&action.operation, result)
            || result.path != action.operation.path()
        {
            return None;
        }
        if is_barrier_record(result) {
            continue;
        }
        if result.step != next {
            return None;
        }
        next += 1;
        if result.step != failure.step
            && !matches!(result.outcome.as_str(), "ok" | "expected_error")
        {
            return None;
        }
    }
    (next == failure.step + 1).then_some(failure.step)
}

/// The controller and oracle share only this observation predicate, not the
/// namespace model: a stop is released by public, matching endpoint milestones.
pub(crate) fn stream_stop_ready<'a>(
    case: &BehaviorCase,
    target: &str,
    phase: ProcessStopPhase,
    executions: impl Iterator<Item = (&'a str, &'a [ActionObservation], &'a [String])> + Clone,
) -> bool {
    let Some((_, target_results, lifecycle)) = executions
        .clone()
        .find(|(process, _, _)| *process == target)
    else {
        return false;
    };
    if !lifecycle.iter().any(|event| event == "context_ready") {
        return false;
    }
    let endpoint = |program: &ProcessProgram,
                    results: &[ActionObservation],
                    step: usize,
                    incarnation: u64,
                    writer: bool| {
        let Some(action) = program.actions.get(step) else {
            return false;
        };
        let valid = |record: &ActionObservation| {
            record.step == step
                && record.process == program.id
                && record.path == action.operation.path()
                && stream_record_action_matches(&action.operation, record)
                && is_barrier_record(record)
        };
        let opened = results.iter().position(|record| valid(record)
            && matches!(record.barrier, Some(BarrierObservation::StreamOpened { incarnation: observed })
                if observed == incarnation));
        let progress = results.iter().position(|record| valid(record) && if writer {
            record.action == "stream_write"
                && matches!(record.barrier, Some(BarrierObservation::StreamFrame { incarnation: observed, index: 0, .. })
                    if observed == incarnation)
        } else {
            matches!(record.barrier, Some(BarrierObservation::StreamFirstFrame { incarnation: observed })
                if observed == incarnation)
        });
        incarnation != 0
            && opened
                .zip(progress)
                .is_some_and(|(opened, progress)| opened < progress)
    };
    let matched_peer = |program: &ProcessProgram, step: usize, incarnation: u64, writer: bool| {
        let path = namespace_path(program, program.actions[step].operation.path());
        executions.clone().any(|(peer, results, _)| {
            let Some(peer_program) = case.processes.iter().find(|program| program.id == peer)
            else {
                return false;
            };
            peer_program
                .actions
                .iter()
                .enumerate()
                .any(|(step, action)| {
                    let Some((peer_writer, _, _)) = stream_parameters(&action.operation) else {
                        return false;
                    };
                    (peer_writer != writer
                        || matches!(action.operation, ActionOp::StreamRoundTrip { .. }))
                        && namespace_path(peer_program, action.operation.path()) == path
                        && endpoint(peer_program, results, step, incarnation, !writer)
                })
        })
    };
    if phase == ProcessStopPhase::AfterSiblingStreamFirstFrame {
        return executions.clone().any(|(process, results, lifecycle)| {
            process != target
                && lifecycle.iter().any(|event| event == "context_ready")
                && case
                    .processes
                    .iter()
                    .find(|program| program.id == process)
                    .is_some_and(|program| {
                        results.iter().any(|record| {
                            let Some(BarrierObservation::StreamFirstFrame { incarnation }) =
                                record.barrier
                            else {
                                return false;
                            };
                            program.actions.get(record.step).is_some_and(|action| {
                                matches!(action.operation, ActionOp::GatedStreamRead { .. })
                            }) && endpoint(program, results, record.step, incarnation, false)
                                && matched_peer(program, record.step, incarnation, false)
                        })
                    })
        });
    }
    let Some(program) = case.processes.iter().find(|program| program.id == target) else {
        return false;
    };
    target_results.iter().any(|record| {
        let Some(BarrierObservation::StreamOpened { incarnation }) = record.barrier else {
            return false;
        };
        let Some(action) = program.actions.get(record.step) else {
            return false;
        };
        let Some((writer, _, _)) = stream_parameters(&action.operation) else {
            return false;
        };
        if !endpoint(program, target_results, record.step, incarnation, writer) {
            return false;
        }
        matched_peer(program, record.step, incarnation, writer)
    })
}

impl BehaviorOracle {
    pub fn verify(
        case: &BehaviorCase,
        observation: &CaseObservation,
    ) -> Result<(), OracleViolation> {
        Self::verify_inner(case, observation, None, None)
    }

    pub fn verify_with_budget(
        case: &BehaviorCase,
        observation: &CaseObservation,
        budget: &Budget,
    ) -> Result<(), OracleViolation> {
        Self::verify_inner(case, observation, Some(budget), None)
    }

    /// The caller supplies an authoritative, complete final snapshot of owned
    /// blobs as path -> (namespace revision, byte length). This constrains the
    /// same history search; it never picks an arbitrary race winner for health.
    pub fn verify_retained_blobs_with_budget(
        case: &BehaviorCase,
        observation: &CaseObservation,
        retained: &BTreeMap<String, (u64, u64)>,
        budget: &Budget,
    ) -> Result<(), OracleViolation> {
        Self::verify_inner(case, observation, Some(budget), Some(retained))
    }

    fn verify_inner(
        case: &BehaviorCase,
        observation: &CaseObservation,
        budget: Option<&Budget>,
        retained: Option<&BTreeMap<String, (u64, u64)>>,
    ) -> Result<(), OracleViolation> {
        check_budget(budget, "oracle model validation")?;
        case.validate()
            .map_err(|detail| OracleViolation::new("invalid_model", detail))?;
        verify_route_expected_values(case)?;
        if observation.case_id != case.id {
            return violation("case_identity", "observation belongs to a different case");
        }
        if observation.executions.len() != case.processes.len() {
            return violation(
                "execution_count",
                format!(
                    "expected {} executions, observed {}",
                    case.processes.len(),
                    observation.executions.len()
                ),
            );
        }
        let mut execution_ids = BTreeSet::new();
        let mut request_ids = BTreeSet::new();
        for execution in &observation.executions {
            if !execution_ids.insert(&execution.process) {
                return violation(
                    "duplicate_execution",
                    format!("process {} was observed more than once", execution.process),
                );
            }
            if execution.request_id.is_empty() || !request_ids.insert(&execution.request_id) {
                return violation(
                    "execution_identity",
                    "execution request identity is empty or reused",
                );
            }
            let program = case
                .processes
                .iter()
                .find(|program| program.id == execution.process)
                .ok_or_else(|| OracleViolation::new("execution_identity", "unknown process"))?;
            if execution.request_id != case.execution_request_id(program) {
                return violation(
                    "execution_identity",
                    format!(
                        "{} has an execution request from another attempt",
                        program.id
                    ),
                );
            }
            if execution.logical_node_id != program.logical_node_id {
                return violation(
                    "execution_placement",
                    format!("{} executed outside its assigned logical node", program.id),
                );
            }
            let mut next_step = 0;
            for result in &execution.results {
                let action = program.actions.get(result.step).ok_or_else(|| {
                    OracleViolation::new(
                        "action_identity",
                        format!("{} reported unplanned step {}", program.id, result.step),
                    )
                })?;
                if result.process != program.id
                    || !stream_record_action_matches(&action.operation, result)
                {
                    return violation(
                        "action_identity",
                        format!(
                            "{} step {} belongs to another action",
                            program.id, result.step
                        ),
                    );
                }
                if result.path != action.operation.path() {
                    return violation(
                        "path_consistency",
                        format!(
                            "{} step {} reported the wrong path",
                            program.id, result.step
                        ),
                    );
                }
                // Endpoint-preparation barriers may arrive for later actions,
                // but terminal action records follow this process's serial IR.
                // Do not reconstruct a different local history by sorting steps.
                if !is_barrier_record(result) {
                    if result.step < next_step {
                        return violation(
                            "duplicate_action",
                            format!("{} emitted step {} twice", program.id, result.step),
                        );
                    }
                    if result.step != next_step {
                        return Err(OracleViolation::new(
                            "action_order",
                            format!(
                                "{} expected step {next_step}, observed {}",
                                program.id, result.step
                            ),
                        )
                        .classified(
                            FailureClass::MilestoneOrder,
                            CausalRole::Action((&action.operation).into()),
                        ));
                    }
                    next_step += 1;
                }
            }
        }
        // A typed failure must not hide missing evidence in a later sibling.
        // Successful runs retain the ordinary invariant diagnostic ordering.
        if observation
            .executions
            .iter()
            .any(|execution| !execution.exit_success)
        {
            for program in &case.processes {
                check_budget(budget, "terminal action-prefix evidence")?;
                let execution = observation
                    .executions
                    .iter()
                    .find(|execution| execution.process == program.id)
                    .ok_or_else(|| OracleViolation::new("missing_execution", &program.id))?;
                if process_failure_is_expected(case, program) {
                    let mut lifecycle_only = execution.clone();
                    lifecycle_only.results.clear();
                    Self::verify_execution(&lifecycle_only)?;
                    Self::verify_expected_process_failure(case, program, execution)?;
                    continue;
                }
                Self::verify_execution(execution)?;
                let complete = if execution.exit_success {
                    let mut next = 0;
                    execution.results.iter().all(|result| {
                        let Some(action) = program.actions.get(result.step) else {
                            return false;
                        };
                        if result.process != program.id
                            || !stream_record_action_matches(&action.operation, result)
                            || result.path != action.operation.path()
                        {
                            return false;
                        }
                        if is_barrier_record(result) {
                            return true;
                        }
                        if result.step != next {
                            return false;
                        }
                        next += 1;
                        true
                    }) && next == program.actions.len()
                } else {
                    recorded_action_failure(program, execution).is_some()
                };
                if !complete {
                    return Err(OracleViolation::new(
                        "missing_action",
                        format!(
                            "{} lacks a complete action history or typed terminal prefix",
                            program.id
                        ),
                    ));
                }
            }
            if let FailureInjection::StopProcess { process, phase, .. } = &case.failure
                && matches!(
                    phase,
                    ProcessStopPhase::AfterStreamFirstFrame
                        | ProcessStopPhase::AfterSiblingStreamFirstFrame
                )
                && !stream_stop_ready(
                    case,
                    process,
                    *phase,
                    observation.executions.iter().map(|execution| {
                        (
                            execution.process.as_str(),
                            execution.results.as_slice(),
                            execution.lifecycle.as_slice(),
                        )
                    }),
                )
            {
                return violation(
                    "stop_phase",
                    "stream stop lacks matching first-frame milestones",
                );
            }
        }
        for program in &case.processes {
            check_budget(budget, "oracle process and action evidence")?;
            let execution = observation
                .executions
                .iter()
                .find(|execution| execution.process == program.id)
                .ok_or_else(|| {
                    OracleViolation::new(
                        "missing_execution",
                        format!("process {} has no execution observation", program.id),
                    )
                })?;
            if process_failure_is_expected(case, program) {
                if execution.exit_success {
                    return violation(
                        "process_failure",
                        "expected failed process exited successfully",
                    );
                }
                let mut lifecycle_only = execution.clone();
                lifecycle_only.results.clear();
                Self::verify_execution(&lifecycle_only)?;
                Self::verify_expected_process_failure(case, program, execution)?;
                continue;
            }
            Self::verify_execution(execution)?;
            let mut seen = BTreeSet::new();
            for result in execution
                .results
                .iter()
                .filter(|result| !is_barrier_record(result))
            {
                if !seen.insert(result.step) {
                    return violation(
                        "duplicate_action",
                        format!("{} emitted step {} twice", program.id, result.step),
                    );
                }
            }
            if execution
                .results
                .iter()
                .filter(|result| !is_barrier_record(result))
                .count()
                != program.actions.len()
                && recorded_action_failure(program, execution).is_none()
            {
                return violation(
                    "action_count",
                    format!(
                        "{} expected {} action results, observed {}",
                        program.id,
                        program.actions.len(),
                        execution.results.len(),
                    ),
                );
            }
            let mut last_revisions = BTreeMap::<String, u64>::new();
            let mut last_mutation_revision = None;
            for (step, action) in program.actions.iter().enumerate() {
                let result = execution
                    .results
                    .iter()
                    .find(|result| result.step == step && !is_barrier_record(result))
                    .ok_or_else(|| {
                        OracleViolation::new(
                            "missing_action",
                            format!("{} omitted step {step}", program.id),
                        )
                        .at_action(&action.operation)
                    })?;
                match action.expected {
                    ExpectedOutcome::Ok if result.outcome != "ok" => {
                        return Err(OracleViolation::new(
                            "unexpected_error",
                            format!("{} step {step}: {:?}", program.id, result.error),
                        )
                        .at_action(&action.operation)
                        .with_outcome(action.expected, result));
                    }
                    ExpectedOutcome::Error(expected)
                        if result.outcome != "expected_error" || result.errno != Some(expected) =>
                    {
                        return Err(OracleViolation::new(
                            "typed_error",
                            format!(
                                "{} step {step} expected errno {expected}, observed {:?}",
                                program.id, result.errno
                            ),
                        )
                        .at_action(&action.operation)
                        .with_outcome(action.expected, result));
                    }
                    ExpectedOutcome::Linearized {
                        error: expected, ..
                    } if result.outcome != "ok"
                        && (result.outcome != "expected_error"
                            || result.errno != Some(expected)) =>
                    {
                        return Err(OracleViolation::new(
                            "typed_error",
                            format!("{} step {step} expected success or errno {expected}, observed {:?}",
                                program.id, result.errno),
                        ).at_action(&action.operation).with_outcome(action.expected, result));
                    }
                    ExpectedOutcome::Exception(expected)
                        if result.outcome != "expected_error"
                            || result.error_type.as_deref() != Some(expected.as_str()) =>
                    {
                        return Err(OracleViolation::new(
                            "typed_exception",
                            format!(
                                "{} step {step} expected {}, observed {:?}",
                                program.id,
                                expected.as_str(),
                                result.error_type
                            ),
                        )
                        .at_action(&action.operation)
                        .with_outcome(action.expected, result));
                    }
                    _ => {}
                }
                verify_action_payload(&program.id, step, &action.operation, result)
                    .map_err(|error| error.at_action(&action.operation))?;
                if let Some(revision) = result.revision {
                    if matches!(
                        action.operation,
                        ActionOp::Rename { .. } | ActionOp::Unlink { .. }
                    ) {
                        if last_mutation_revision.is_some_and(|previous| revision <= previous) {
                            return violation(
                                "namespace_revision",
                                format!(
                                    "{} step {step} mutation did not advance revision {revision}",
                                    program.id
                                ),
                            );
                        }
                        last_mutation_revision = Some(revision);
                    } else {
                        let path = action.operation.path();
                        if last_revisions
                            .get(path)
                            .is_some_and(|previous| revision < *previous)
                        {
                            return violation(
                                "namespace_revision",
                                format!(
                                    "{} step {step} observed older revision {revision} for {path}",
                                    program.id
                                ),
                            );
                        }
                        last_revisions.insert(path.to_owned(), revision);
                    }
                }
            }
        }
        verify_namespace_histories(case, observation, budget, retained)?;
        Self::verify_linearized_groups(case, observation)?;
        check_budget(budget, "oracle completion")?;
        Ok(())
    }

    fn verify_linearized_groups(
        case: &BehaviorCase,
        observation: &CaseObservation,
    ) -> Result<(), OracleViolation> {
        let mut groups = BTreeMap::<u32, (u8, usize, usize)>::new();
        for program in &case.processes {
            let execution = observation
                .executions
                .iter()
                .find(|execution| execution.process == program.id)
                .expect("execution presence checked before linearization");
            for (step, action) in program.actions.iter().enumerate() {
                let ExpectedOutcome::Linearized {
                    group, successes, ..
                } = action.expected
                else {
                    continue;
                };
                let Some(result) = execution
                    .results
                    .iter()
                    .find(|result| result.step == step && !is_barrier_record(result))
                else {
                    return violation(
                        "namespace_model_evidence",
                        format!("{} step {step} has no race result", program.id),
                    );
                };
                let entry = groups.entry(group).or_insert((successes, 0, 0));
                if entry.0 != successes {
                    return violation(
                        "linearization_contract",
                        format!("group {group} declared conflicting success counts"),
                    );
                }
                entry.1 += 1;
                entry.2 += usize::from(result.outcome == "ok");
            }
        }
        for (group, (expected_successes, members, observed_successes)) in groups {
            if members < 2 || observed_successes != usize::from(expected_successes) {
                return violation(
                    "linearizability",
                    format!(
                        "group {group} expected {expected_successes} successes across {members} operations, observed {observed_successes}"
                    ),
                );
            }
        }
        Ok(())
    }

    pub fn verify_execution(execution: &ExecutionObservation) -> Result<(), OracleViolation> {
        let event_count = |expected: &str| {
            execution
                .lifecycle
                .iter()
                .filter(|event| event.as_str() == expected)
                .count()
        };
        let started_count = event_count("process_started");
        let ready_count = event_count("context_ready");
        let bootstrap_failed_count = event_count("bootstrap_failed");
        if started_count > 1 || ready_count > 1 || bootstrap_failed_count > 1 {
            return violation(
                "duplicate_lifecycle",
                format!(
                    "observed process_started={started_count}, context_ready={ready_count}, bootstrap_failed={bootstrap_failed_count}"
                ),
            );
        }
        if ready_count != 0 && bootstrap_failed_count != 0 {
            return violation(
                "bootstrap_resolution",
                "bootstrap reported both ready and failed",
            );
        }

        let started = execution
            .lifecycle
            .iter()
            .position(|event| event == "process_started");
        let ready = execution
            .lifecycle
            .iter()
            .position(|event| event == "context_ready");
        if let Some(ready) = ready
            && started.is_none_or(|started| started >= ready)
        {
            return violation("context_order", "context became ready before process start");
        }
        if execution.exit_success && (started.is_none() || ready.is_none()) {
            return violation(
                "missing_context",
                "successful process did not report start and context readiness",
            );
        }
        if !execution.results.is_empty() {
            let first_result = execution
                .lifecycle
                .iter()
                .position(|event| event == "user_result");
            if ready.is_none_or(|ready| first_result.is_none_or(|result| ready >= result)) {
                return violation(
                    "user_before_ready",
                    "user output preceded context readiness",
                );
            }
        }

        let terminal_positions = execution
            .lifecycle
            .iter()
            .enumerate()
            .filter(|(_, event)| {
                matches!(
                    event.as_str(),
                    "exited" | "spawn_failed" | "spawn_rejected" | "process_error"
                )
            })
            .map(|(position, _)| position)
            .collect::<Vec<_>>();
        if terminal_positions.len() != 1 || !execution.terminal {
            return violation(
                "terminal_resolution",
                format!(
                    "expected one terminal event, observed {}",
                    terminal_positions.len()
                ),
            );
        }
        if execution
            .lifecycle
            .iter()
            .enumerate()
            .any(|(position, event)| event == "user_result" && position > terminal_positions[0])
        {
            return violation(
                "output_after_terminal",
                "user output followed process termination",
            );
        }
        if !execution.exit_success
            && !execution.results.is_empty()
            && terminal_action_error(execution).is_none()
        {
            return violation(
                "process_failure",
                "process failed after emitting action results",
            );
        }
        Ok(())
    }

    fn verify_expected_process_failure(
        case: &BehaviorCase,
        program: &ProcessProgram,
        execution: &ExecutionObservation,
    ) -> Result<(), OracleViolation> {
        let has = |event: &str| execution.lifecycle.iter().any(|seen| seen == event);
        match &case.failure {
            FailureInjection::StopProcess {
                process,
                phase: ProcessStopPhase::DuringBootstrap,
                ..
            } if process == &program.id => {
                if !has("process_started") || has("context_ready") {
                    return violation(
                        "stop_phase",
                        "bootstrap stop did not occur after native start and before context readiness",
                    );
                }
            }
            FailureInjection::StopProcess {
                process,
                phase: ProcessStopPhase::AfterContextReady,
                ..
            } if process == &program.id => {
                if !has("context_ready") {
                    return violation(
                        "stop_phase",
                        "post-attachment stop occurred before context readiness",
                    );
                }
            }
            FailureInjection::StopProcess { process, phase, .. }
                if process == &program.id
                    && matches!(
                        phase,
                        ProcessStopPhase::AfterStreamFirstFrame
                            | ProcessStopPhase::AfterSiblingStreamFirstFrame
                    ) =>
            {
                if !has("context_ready") || !has("stop_accepted") {
                    return violation("stop_phase", "stream stop lacks readiness or acceptance");
                }
            }
            FailureInjection::LaunchFailure { process, kind } if process == &program.id => {
                let expected = match kind {
                    LaunchFailureKind::EmptyCommand => has("spawn_rejected"),
                    LaunchFailureKind::MissingExecutable => {
                        has("spawn_failed") || has("process_error")
                    }
                    LaunchFailureKind::MalformedExecutionIdentity => {
                        has("spawn_rejected") || has("bootstrap_failed")
                    }
                    LaunchFailureKind::PythonSyntax => {
                        has("bootstrap_failed") && !has("context_ready")
                    }
                    LaunchFailureKind::PythonRuntime => {
                        has("context_ready") && has("exited") && !has("bootstrap_failed")
                    }
                };
                if !expected {
                    return violation(
                        "launch_failure_kind",
                        format!(
                            "launch failure {kind:?} produced lifecycle {:?}",
                            execution.lifecycle
                        ),
                    );
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobPublicationTrace {
    pub declared_length: usize,
    pub declared_digest: String,
    pub observed_bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamIncarnationTrace {
    Opened { incarnation: u64 },
    Bytes { incarnation: u64, bytes: Vec<u8> },
    Closed { incarnation: u64 },
}

impl BehaviorOracle {
    pub fn verify_blob_publication(trace: &BlobPublicationTrace) -> Result<(), OracleViolation> {
        if !publication_matches(
            Some(trace.declared_length),
            Some(&trace.declared_digest),
            &trace.observed_bytes,
        ) {
            return violation(
                "torn_publication",
                "published blob length or digest differs from committed metadata",
            );
        }
        Ok(())
    }

    pub fn verify_stream_incarnations(
        trace: &[StreamIncarnationTrace],
    ) -> Result<(), OracleViolation> {
        let mut current = None;
        let mut greatest = 0_u64;
        for event in trace {
            match event {
                StreamIncarnationTrace::Opened { incarnation } => {
                    if *incarnation <= greatest || current.is_some() {
                        return violation(
                            "stale_incarnation",
                            format!("stream incarnation {incarnation} opened out of order"),
                        );
                    }
                    greatest = *incarnation;
                    current = Some(*incarnation);
                }
                StreamIncarnationTrace::Bytes { incarnation, .. } => {
                    if current != Some(*incarnation) {
                        return violation(
                            "stale_incarnation",
                            format!("bytes arrived for stale incarnation {incarnation}"),
                        );
                    }
                }
                StreamIncarnationTrace::Closed { incarnation } => {
                    if current != Some(*incarnation) {
                        return violation(
                            "stale_incarnation",
                            format!("close arrived for stale incarnation {incarnation}"),
                        );
                    }
                    current = None;
                }
            }
        }
        Ok(())
    }
}

fn verify_action_payload(
    process: &str,
    step: usize,
    action: &ActionOp,
    result: &ActionObservation,
) -> Result<(), OracleViolation> {
    let success = result.outcome == "ok";
    if descriptor_first_terminal_succeeded(result)
        && !success
        && result
            .transfer
            .as_ref()
            .is_none_or(|transfer| !transfer.complete)
    {
        return Err(OracleViolation::new(
            "payload_integrity",
            format!("{process} step {step} omitted completed transfer before terminal error"),
        )
        .classified(
            FailureClass::MissingTransfer,
            CausalRole::Action(action.into()),
        ));
    }
    match action {
        ActionOp::PublishBlob { bytes, .. }
        | ActionOp::ReadBlob {
            expected: bytes, ..
        }
        | ActionOp::StreamRead {
            expected: bytes, ..
        }
        | ActionOp::StreamReadWithRetry {
            expected: bytes, ..
        }
        | ActionOp::GatedStreamRead {
            expected: bytes, ..
        }
        | ActionOp::StreamReadInto {
            expected: bytes, ..
        }
        | ActionOp::DescriptorWrite { bytes, .. }
        | ActionOp::DescriptorRead {
            expected: bytes, ..
        } => {
            verify_transferred_payload(process, step, result, std::iter::once(bytes.as_slice()))?;
        }
        ActionOp::StreamWrite { chunks, .. }
        | ActionOp::StreamRoundTrip { chunks, .. }
        | ActionOp::GatedStreamWrite { frames: chunks, .. } => {
            verify_transferred_payload(process, step, result, chunks.iter().map(Vec::as_slice))?;
        }
        ActionOp::Lookup { expected_kind, .. } | ActionOp::AwaitEntry { expected_kind, .. } => {
            if success
                && (result.kind.as_deref() != Some(expected_kind) || result.revision.is_none())
            {
                return violation(
                    "namespace_kind",
                    format!("{process} step {step} returned an inconsistent namespace node"),
                );
            }
        }
        ActionOp::WaitForQuiescent { .. } => {
            if success
                && (result.kind.as_deref() != Some("stream")
                    || result.revision.is_none()
                    || result.active != Some(false))
            {
                return violation(
                    "stream_quiescence",
                    format!("{process} step {step} did not observe a quiescent stream node"),
                );
            }
        }
        ActionOp::MappingExportClose { .. } => {}
        ActionOp::Rename { .. } | ActionOp::Unlink { .. } => {
            if success && result.revision.is_none() {
                return violation(
                    "namespace_revision",
                    format!("{process} step {step} omitted mutation revision"),
                );
            }
        }
    }
    Ok(())
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Clone, Copy)]
enum RoutePayload<'a> {
    Bytes(&'a [u8]),
    Frames(&'a [Vec<u8>]),
}

impl RoutePayload<'_> {
    fn checked_len(self) -> Option<usize> {
        match self {
            Self::Bytes(bytes) => Some(bytes.len()),
            Self::Frames(frames) => frames
                .iter()
                .try_fold(0_usize, |length, frame| length.checked_add(frame.len())),
        }
    }

    fn update_range(self, hasher: &mut Sha256, start: usize, end: usize) -> Option<()> {
        if start > end || end > self.checked_len()? {
            return None;
        }
        match self {
            Self::Bytes(bytes) => hasher.update(&bytes[start..end]),
            Self::Frames(frames) => {
                let mut offset = 0_usize;
                for frame in frames {
                    let frame_end = offset.checked_add(frame.len())?;
                    let overlap_start = start.max(offset);
                    let overlap_end = end.min(frame_end);
                    if overlap_start < overlap_end {
                        hasher.update(&frame[overlap_start - offset..overlap_end - offset]);
                    }
                    offset = frame_end;
                    if offset >= end {
                        break;
                    }
                }
            }
        }
        Some(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct PayloadFingerprint {
    length: usize,
    digest: [u8; 32],
}

fn route_payload_fingerprint(payload: RoutePayload<'_>) -> Option<PayloadFingerprint> {
    let length = payload.checked_len()?;
    let mut hasher = Sha256::new();
    payload.update_range(&mut hasher, 0, length)?;
    Some(PayloadFingerprint {
        length,
        digest: hasher.finalize().into(),
    })
}

fn update_route_payload_range(
    hasher: &mut Sha256,
    payloads: &[RoutePayload<'_>],
    start: usize,
    end: usize,
) -> Option<()> {
    let mut offset = 0_usize;
    for payload in payloads {
        let length = payload.checked_len()?;
        let payload_end = offset.checked_add(length)?;
        let overlap_start = start.max(offset);
        let overlap_end = end.min(payload_end);
        if overlap_start < overlap_end {
            payload.update_range(hasher, overlap_start - offset, overlap_end - offset)?;
        }
        offset = payload_end;
        if offset >= end {
            break;
        }
    }
    Some(())
}

fn transformed_route_fingerprint(
    inputs: &[RoutePayload<'_>],
    rotation: usize,
) -> Option<PayloadFingerprint> {
    let length = inputs.iter().try_fold(0_usize, |length, payload| {
        length.checked_add(payload.checked_len()?)
    })?;
    let rotation = if length == 0 { 0 } else { rotation % length };
    let mut hasher = Sha256::new();
    update_route_payload_range(&mut hasher, inputs, rotation, length)?;
    update_route_payload_range(&mut hasher, inputs, 0, rotation)?;
    Some(PayloadFingerprint {
        length,
        digest: hasher.finalize().into(),
    })
}

fn invalid_route_value(route: &str, path: &str, detail: &str) -> OracleViolation {
    OracleViolation::new(
        "route_expected_value",
        format!("route {route} path {path}: {detail}"),
    )
    .classified(FailureClass::InvalidEvidence, CausalRole::Case)
}

fn route_endpoint_payload<'a>(
    case: &'a BehaviorCase,
    route: &str,
    role: &str,
    path: &str,
    kind: DataKind,
    source: bool,
) -> Result<RoutePayload<'a>, OracleViolation> {
    let process = case
        .processes
        .iter()
        .find(|process| process.id == role)
        .ok_or_else(|| invalid_route_value(route, path, "endpoint role is absent"))?;
    let mut found = None;
    for action in &process.actions {
        if action.operation.path() != path {
            continue;
        }
        let payload = match (&action.operation, kind, source) {
            (ActionOp::PublishBlob { bytes, .. }, DataKind::Blob, true)
            | (
                ActionOp::ReadBlob {
                    expected: bytes, ..
                },
                DataKind::Blob,
                false,
            )
            | (
                ActionOp::StreamRead {
                    expected: bytes, ..
                }
                | ActionOp::StreamReadWithRetry {
                    expected: bytes, ..
                }
                | ActionOp::GatedStreamRead {
                    expected: bytes, ..
                }
                | ActionOp::StreamReadInto {
                    expected: bytes, ..
                },
                DataKind::Stream,
                false,
            ) => RoutePayload::Bytes(bytes),
            (
                ActionOp::StreamWrite { chunks, .. }
                | ActionOp::GatedStreamWrite { frames: chunks, .. },
                DataKind::Stream,
                true,
            ) => RoutePayload::Frames(chunks),
            _ => continue,
        };
        if found.replace(payload).is_some() {
            return Err(invalid_route_value(
                route,
                path,
                "endpoint role has more than one data action",
            ));
        }
    }
    found.ok_or_else(|| invalid_route_value(route, path, "endpoint data action is absent"))
}

/// Derive every relay/join output from the route graph and root payload rather
/// than trusting the reader and writer operands to agree with each other.
fn verify_route_expected_values(case: &BehaviorCase) -> Result<(), OracleViolation> {
    for route in &case.routes {
        if case.topology == crate::ir::TopologyFamily::RingWalk {
            let mut previous = None;
            let first_source = route.edges.first().map(|edge| edge.source_role.as_str());
            for (index, edge) in route.edges.iter().enumerate() {
                if index > 0 && Some(edge.source_role.as_str()) == first_source {
                    previous = None;
                }
                let writer = route_endpoint_payload(
                    case,
                    &route.id,
                    &edge.source_role,
                    &edge.path,
                    route.kind,
                    true,
                )?;
                let reader = route_endpoint_payload(
                    case,
                    &route.id,
                    &edge.destination_role,
                    &edge.path,
                    route.kind,
                    false,
                )?;
                let writer = route_payload_fingerprint(writer).ok_or_else(|| {
                    invalid_route_value(&route.id, &edge.path, "payload length overflow")
                })?;
                let reader = route_payload_fingerprint(reader).ok_or_else(|| {
                    invalid_route_value(&route.id, &edge.path, "payload length overflow")
                })?;
                if previous.is_some_and(|expected| expected != writer) {
                    return Err(invalid_route_value(
                        &route.id,
                        &edge.path,
                        "relay changed the lap token",
                    ));
                }
                if reader != writer {
                    return Err(invalid_route_value(
                        &route.id,
                        &edge.path,
                        "reader expectation differs from the independently derived token",
                    ));
                }
                previous = Some(writer);
            }
            continue;
        }

        let mut completed = vec![false; route.edges.len()];
        let mut pending = route
            .edges
            .iter()
            .map(|edge| edge.source_role.as_str())
            .collect::<BTreeSet<_>>();
        let mut root = None;
        while !pending.is_empty() {
            let Some(role) = pending.iter().copied().find(|role| {
                route
                    .edges
                    .iter()
                    .enumerate()
                    .all(|(index, edge)| edge.destination_role != *role || completed[index])
            }) else {
                return Err(invalid_route_value(
                    &route.id,
                    "<graph>",
                    "route dependencies are cyclic",
                ));
            };
            let inputs = route
                .edges
                .iter()
                .enumerate()
                .filter(|(_, edge)| edge.destination_role == role)
                .map(|(_, edge)| {
                    route_endpoint_payload(
                        case,
                        &route.id,
                        &edge.source_role,
                        &edge.path,
                        route.kind,
                        true,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            for (index, edge) in route.edges.iter().enumerate() {
                if edge.source_role != role {
                    continue;
                }
                let writer =
                    route_endpoint_payload(case, &route.id, role, &edge.path, route.kind, true)?;
                let reader = route_endpoint_payload(
                    case,
                    &route.id,
                    &edge.destination_role,
                    &edge.path,
                    route.kind,
                    false,
                )?;
                let writer = route_payload_fingerprint(writer).ok_or_else(|| {
                    invalid_route_value(&route.id, &edge.path, "payload length overflow")
                })?;
                let reader = route_payload_fingerprint(reader).ok_or_else(|| {
                    invalid_route_value(&route.id, &edge.path, "payload length overflow")
                })?;
                let expected = if inputs.is_empty() {
                    *root.get_or_insert(writer)
                } else {
                    transformed_route_fingerprint(&inputs, index + 1).ok_or_else(|| {
                        invalid_route_value(&route.id, &edge.path, "joined payload length overflow")
                    })?
                };
                if writer != expected {
                    return Err(invalid_route_value(
                        &route.id,
                        &edge.path,
                        "writer payload differs from the independently derived relay output",
                    ));
                }
                if reader != expected {
                    return Err(invalid_route_value(
                        &route.id,
                        &edge.path,
                        "reader expectation differs from the independently derived relay output",
                    ));
                }
                completed[index] = true;
            }
            pending.remove(role);
        }
        if completed.iter().any(|complete| !complete) {
            return Err(invalid_route_value(
                &route.id,
                "<graph>",
                "route edge has no derivable source",
            ));
        }
    }
    Ok(())
}

/// Hash ordered immutable IR chunks, retaining their separate framing evidence.
/// `prefix` limits hashing, not checked total-length accounting.
fn payload_summary<'a>(
    chunks: impl Iterator<Item = &'a [u8]>,
    prefix: Option<usize>,
) -> Option<(usize, String)> {
    let mut length = 0_usize;
    let mut remaining = prefix.unwrap_or(usize::MAX);
    let mut hasher = Sha256::new();
    for chunk in chunks {
        length = length.checked_add(chunk.len())?;
        let take = remaining.min(chunk.len());
        hasher.update(&chunk[..take]);
        remaining -= take;
    }
    Some((length, format!("{:x}", hasher.finalize())))
}

fn verify_transferred_payload<'a>(
    process: &str,
    step: usize,
    result: &ActionObservation,
    chunks: impl Iterator<Item = &'a [u8]> + Clone,
) -> Result<(), OracleViolation> {
    let mut completed_summary = None;
    if let Some(transfer) = &result.transfer {
        let (length, expected_digest) = payload_summary(chunks.clone(), Some(transfer.length))
            .ok_or_else(|| {
                OracleViolation::new("payload_integrity", "IR payload length overflow")
            })?;
        if transfer.length > length
            || (transfer.complete && transfer.length != length)
            || (result.outcome == "ok" && !transfer.complete)
            || transfer.digest != expected_digest
        {
            return violation(
                "payload_integrity",
                format!("{process} step {step} changed the observed transfer or prefix"),
            );
        }
        if transfer.complete {
            completed_summary = Some((length, expected_digest));
        }
    }
    // Terminal-error records may contain only a proven prefix. Legacy aggregate
    // fields still describe a completed transfer and must not contradict it.
    if result.outcome == "ok" || result.length.is_some() || result.digest.is_some() {
        let (length, expected_digest) = completed_summary
            .or_else(|| payload_summary(chunks, None))
            .ok_or_else(|| {
                OracleViolation::new("payload_integrity", "IR payload length overflow")
            })?;
        if result.length != Some(length) || result.digest.as_deref() != Some(&expected_digest) {
            return violation(
                "payload_integrity",
                format!("{process} step {step} returned the wrong length or digest"),
            );
        }
    }
    Ok(())
}

fn descriptor_first_terminal_succeeded(result: &ActionObservation) -> bool {
    let terminal_results = match &result.descriptor {
        Some(DescriptorObservation::Write {
            terminal_results, ..
        })
        | Some(DescriptorObservation::Read {
            terminal_results, ..
        }) => terminal_results,
        _ => return false,
    };
    matches!(terminal_results.first(), Some(DescriptorTerminalResult::Ok))
}

fn descriptor_completion(
    action: &ActionOp,
    result: &ActionObservation,
) -> Result<bool, OracleViolation> {
    let success = result.outcome == "ok" && result.errno.is_none();
    if let ActionOp::DescriptorWrite {
        method,
        finish: DescriptorFinish::Drop,
        ..
    } = action
    {
        if !success {
            return Ok(false);
        }
        if !matches!(&result.descriptor,
            Some(DescriptorObservation::Write {
                method: actual_method, finish: DescriptorFinish::Drop,
                terminal_results, dropped: true, reservation_released: true,
            }) if actual_method == method && terminal_results.is_empty())
        {
            return Err(OracleViolation::new(
                "namespace_model_evidence",
                "writer drop did not prove absence and exclusive reservation reuse",
            )
            .classified(
                FailureClass::ReservationNotReleased,
                CausalRole::Action(action.into()),
            ));
        }
        return Ok(true);
    }
    if success {
        return Ok(true);
    }
    if !namespace_errno(result, libc::EBADF) {
        return Ok(false);
    }
    let (finish, terminals) = match (action, &result.descriptor) {
        (
            ActionOp::DescriptorWrite { method, finish, .. },
            Some(DescriptorObservation::Write {
                method: observed_method,
                finish: observed_finish,
                terminal_results,
                ..
            }),
        ) if method == observed_method && finish == observed_finish => (finish, terminal_results),
        (
            ActionOp::DescriptorRead { method, finish, .. },
            Some(DescriptorObservation::Read {
                method: observed_method,
                finish: observed_finish,
                terminal_results,
            }),
        ) if method == observed_method && finish == observed_finish => (finish, terminal_results),
        _ => return Ok(false),
    };
    if !matches!(
        finish,
        DescriptorFinish::CloseTwice | DescriptorFinish::AbortTwice
    ) {
        return Ok(false);
    }
    if !matches!(terminals.as_slice(), [
        DescriptorTerminalResult::Ok,
        DescriptorTerminalResult::Error { errno: Some(libc::EBADF), error_type }
    ] if error_type == "OSError")
    {
        return Err(OracleViolation::new(
            "namespace_model_evidence",
            "repeated descriptor finish omitted first-terminal success and second-terminal EBADF",
        )
        .classified(
            FailureClass::InvalidEvidence,
            CausalRole::Action(action.into()),
        ));
    }
    Ok(true)
}

fn publication_matches(length: Option<usize>, content_digest: Option<&str>, bytes: &[u8]) -> bool {
    length == Some(bytes.len()) && content_digest == Some(digest(bytes).as_str())
}

// Publications separate lookup/open, exclusive reservation, and commit.
// Streams separate pending creation, matched attachment, byte/EOF progress,
// completion, and asynchronous namespace teardown. Every creation/mutation
// consumes one authority revision slot; attached identities constrain that
// exact slot. Names may be replaced independently of old handles.
// Neither execution-vector order nor collector receipt order is causal.
#[derive(Clone, Debug)]
enum NamespaceOperation<'a> {
    Publish {
        path: String,
        bytes: &'a [u8],
        commit: bool,
        exclusive: bool,
        require_existing: bool,
    },
    Read {
        path: String,
        expected: &'a [u8],
        offset: usize,
        whole: bool,
    },
    Lookup {
        path: String,
    },
    MutationOrigin {
        path: String,
        revision: u64,
    },
    Rename {
        source: String,
        destination: String,
        replace: bool,
    },
    Unlink {
        path: String,
    },
    StreamOpen {
        path: String,
        incarnation: Option<u64>,
        source: bool,
        replace: bool,
        chunks: Option<&'a [Vec<u8>]>,
        gated: bool,
    },
    StreamAttach {
        path: String,
        incarnation: u64,
        roundtrip: bool,
    },
    StreamTransfer {
        path: String,
        incarnation: u64,
    },
    StreamFirstFrame {
        path: String,
        incarnation: u64,
        source: bool,
    },
    StreamComplete {
        path: String,
        incarnation: u64,
        source: bool,
        expected: Option<&'a [u8]>,
        frame_sources: Vec<&'a [Vec<u8>]>,
        frames_observed: bool,
    },
    StreamPendingFailure {
        path: String,
    },
    StreamStop {
        path: String,
        incarnation: Option<u64>,
        source: bool,
        replace: bool,
        roundtrip: bool,
        chunks: Option<&'a [Vec<u8>]>,
        gated: bool,
        first_frame: bool,
        eof: bool,
        frame_sources: Vec<&'a [Vec<u8>]>,
        stop_gates: Vec<(String, u64, Option<String>)>,
    },
    StreamProbe {
        path: String,
    },
    GatePublish {
        path: String,
    },
    GateLookup {
        path: String,
    },
    Ignore,
}

impl NamespaceOperation<'_> {
    fn path(&self) -> Option<&str> {
        match self {
            Self::Publish { path, .. }
            | Self::Read { path, .. }
            | Self::Lookup { path }
            | Self::MutationOrigin { path, .. }
            | Self::Unlink { path }
            | Self::StreamOpen { path, .. }
            | Self::StreamAttach { path, .. }
            | Self::StreamTransfer { path, .. }
            | Self::StreamFirstFrame { path, .. }
            | Self::StreamComplete { path, .. }
            | Self::StreamPendingFailure { path }
            | Self::StreamStop { path, .. }
            | Self::StreamProbe { path }
            | Self::GatePublish { path }
            | Self::GateLookup { path } => Some(path),
            Self::Rename { source, .. } => Some(source),
            Self::Ignore => None,
        }
    }

    fn writes(&self) -> bool {
        matches!(
            self,
            Self::Publish { .. }
                | Self::Rename { .. }
                | Self::Unlink { .. }
                | Self::StreamOpen { source: true, .. }
                | Self::StreamFirstFrame { source: true, .. }
                | Self::StreamComplete { source: true, .. }
                | Self::GatePublish { .. }
        )
    }
}

struct NamespaceAction<'a> {
    step: usize,
    // Preparations and transfers share an endpoint owner, not a scheduler slot.
    stream_owner: Option<usize>,
    preparation: bool,
    operation: NamespaceOperation<'a>,
    role: SemanticAction,
    result: Option<&'a ActionObservation>,
    denied: bool,
    publication_finished: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct NamespaceBinding<'a> {
    bytes: &'a [u8],
    revision: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct NamespaceHistory<'a> {
    positions: Vec<usize>,
    publications: BTreeSet<usize>,
    publication_lookups: BTreeSet<usize>,
    reservations: BTreeMap<String, usize>,
    streams: BTreeMap<String, usize>,
    active_streams: BTreeMap<String, usize>,
    sessions: BTreeMap<usize, StreamSession<'a>>,
    open_streams: BTreeMap<usize, usize>, // endpoint owner -> incarnation slot
    entries: BTreeMap<String, NamespaceBinding<'a>>,
    initial_revision: u64,
    // Each slot is a committed mutation, in authority order. A publication
    // without a receipt has an unknown revision until a lookup constrains it.
    revisions: Vec<Option<u64>>,
}

// Namespace entries and attached handles are deliberately separate. A name
// may be replaced while its old participants still have unfinished actions.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct StreamSession<'a> {
    path: String,
    source: Option<usize>,
    sink: Option<usize>,
    chunks: Option<&'a [Vec<u8>]>,
    source_attached: bool,
    first_frame_permitted: bool,
    first_frame: bool,
    eof: bool,
    aborted: bool,
}

impl StreamSession<'_> {
    fn matched(&self) -> bool {
        self.source.is_some() && self.sink.is_some()
    }
}

impl NamespaceHistory<'_> {
    fn constrain_revision(&mut self, slot: usize, revision: u64) -> bool {
        if self.revisions[slot].is_some_and(|known| known != revision) {
            return false;
        }
        self.revisions[slot] = Some(revision);
        self.revisions_fit()
    }

    fn revisions_fit(&self) -> bool {
        let mut previous = self.initial_revision;
        let mut distance = 0;
        for known in &self.revisions {
            distance += 1;
            if let Some(known) = known {
                if known.checked_sub(previous).is_none_or(|gap| gap < distance) {
                    return false;
                }
                previous = *known;
                distance = 0;
            }
        }
        previous.checked_add(distance).is_some()
    }

    fn commit_revision(&mut self, revision: Option<u64>) -> Option<usize> {
        let slot = self.revisions.len();
        self.revisions.push(None);
        if let Some(revision) = revision
            && !self.constrain_revision(slot, revision)
        {
            return None;
        }
        self.revisions_fit().then_some(slot)
    }
}

fn namespace_path(program: &ProcessProgram, path: &str) -> String {
    if let Some(suffix) = path.strip_prefix("/runs/self")
        && (suffix.is_empty() || suffix.starts_with('/'))
    {
        format!("/runs/{}{suffix}", program.access.execution_id)
    } else {
        path.to_owned()
    }
}

fn namespace_prefix_allows(prefixes: &[String], path: &str) -> bool {
    prefixes.iter().any(|prefix| {
        path == prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

fn namespace_errno(result: &ActionObservation, errno: i32) -> bool {
    result.outcome == "expected_error" && result.errno == Some(errno)
}
fn namespace_binding_error(result: &ActionObservation, errno: i32, raw: bool) -> bool {
    if raw {
        return namespace_errno(result, errno);
    }
    let exception = match errno {
        libc::ENXIO | libc::ECONNRESET | libc::EPIPE => Some("StreamError"),
        libc::EEXIST | libc::EIO => Some("SessionError"),
        _ => None,
    };
    match exception {
        Some(exception) => {
            result.outcome == "expected_error"
                && result.errno.is_none()
                && result.error_type.as_deref() == Some(exception)
        }
        None => namespace_errno(result, errno),
    }
}

fn initial_fixture_revision(
    case: &BehaviorCase,
    observation: &CaseObservation,
) -> Result<u64, OracleViolation> {
    let mut revisions = BTreeMap::new();
    for execution in &observation.executions {
        for result in &execution.results {
            if !case.read_only_fixture_paths.contains(&result.path) || result.outcome != "ok" {
                continue;
            }
            if let Some(revision) = result.revision {
                if revision == 0
                    || revisions
                        .insert(&result.path, revision)
                        .is_some_and(|known| known != revision)
                {
                    return violation(
                        "namespace_linearizability",
                        format!("immutable fixture {} changed revision", result.path),
                    );
                }
            }
        }
    }
    Ok(revisions.values().copied().max().unwrap_or(0))
}

type StreamFrameSources<'a> = BTreeMap<(&'a str, usize), Vec<&'a [Vec<u8>]>>;

struct StreamFrameEvidence<'a> {
    program: &'a ProcessProgram,
    step: usize,
    incarnation: u64,
    sent: Vec<(u64, &'a str)>,
    received: Vec<(u64, &'a str)>,
    complete: bool,
}

/// Hash each immutable writer frame once, outside the bounded history search.
/// Candidate membership is subsequently constrained by the source actually
/// attached in that history; equal aggregate bytes never equate different
/// logical frame boundaries. Cross-process collector order is irrelevant.
fn verify_stream_frames<'a>(
    case: &'a BehaviorCase,
    observation: &'a CaseObservation,
    budget: Option<&Budget>,
) -> Result<StreamFrameSources<'a>, OracleViolation> {
    let mut evidence = Vec::new();
    let mut sources = Vec::new();
    for program in &case.processes {
        let execution = observation
            .executions
            .iter()
            .find(|execution| execution.process == program.id)
            .expect("execution identities checked");
        for record in &execution.results {
            if matches!(record.barrier, Some(BarrierObservation::StreamFrame { .. }))
                && program
                    .actions
                    .get(record.step)
                    .is_none_or(|action| stream_parameters(&action.operation).is_none())
            {
                return Err(OracleViolation::new(
                    "stream_frames",
                    "frame evidence has no stream action",
                )
                .classified(FailureClass::InvalidEvidence, CausalRole::Namespace));
            }
        }
        for (step, action) in program.actions.iter().enumerate() {
            let Some((source, _, chunks)) = stream_parameters(&action.operation) else {
                continue;
            };
            check_budget(budget, "logical stream frame evidence")?;
            let records = || {
                execution
                    .results
                    .iter()
                    .filter(|record| record.step == step)
            };
            if !records().any(|_| true) {
                if let Some(chunks) = chunks
                    && matches!(&case.failure, FailureInjection::StopProcess { process, .. }
                        if process == &program.id)
                    && execution.terminal
                    && !execution.exit_success
                    && execution
                        .lifecycle
                        .iter()
                        .any(|event| event == "context_ready")
                {
                    // Native rendezvous may attach the reader before the
                    // stopped writer can emit its Python opened milestone.
                    // This is only a candidate: the history search must still
                    // attach this exact IR source to the observed incarnation.
                    sources.push((
                        namespace_path(program, action.operation.path()),
                        None,
                        chunks,
                        chunks
                            .iter()
                            .map(|chunk| (chunk.len() as u64, digest(chunk)))
                            .collect(),
                    ));
                }
                continue;
            }
            let Some(incarnation) =
                stream_incarnation_evidence(execution, step, &program.id, &action.operation)?
            else {
                continue;
            };
            let result = records().find(|record| !is_barrier_record(record));
            let complete = result.is_some_and(|result| {
                result.outcome == "ok"
                    || result
                        .transfer
                        .as_ref()
                        .is_some_and(|transfer| transfer.complete)
            }) || records()
                .any(|record| matches!(record.barrier, Some(BarrierObservation::StreamEof { .. })));
            let mut frames = StreamFrameEvidence {
                program,
                step,
                incarnation,
                sent: Vec::new(),
                received: Vec::new(),
                complete,
            };
            for record in records() {
                if let Some(BarrierObservation::StreamFrame { length, digest, .. }) =
                    &record.barrier
                {
                    if record.action == "stream_write" {
                        frames.sent.push((*length, digest.as_str()));
                    } else {
                        frames.received.push((*length, digest.as_str()));
                    }
                }
            }
            let progress =
                if source && !matches!(action.operation, ActionOp::StreamRoundTrip { .. }) {
                    &frames.sent
                } else {
                    &frames.received
                };
            let length = progress
                .iter()
                .try_fold(0_u64, |total, frame| total.checked_add(frame.0))
                .ok_or_else(|| {
                    OracleViolation::new("stream_frames", "logical frame lengths overflow")
                        .classified(
                            FailureClass::FrameContentMismatch,
                            CausalRole::Action((&action.operation).into()),
                        )
                })?;
            if result
                .and_then(|result| result.transfer.as_ref())
                .is_some_and(|transfer| u64::try_from(transfer.length).ok() != Some(length))
            {
                return Err(OracleViolation::new(
                    "stream_frames",
                    "logical frames disagree with transferred bytes",
                )
                .classified(
                    FailureClass::FrameCountMismatch,
                    CausalRole::Action((&action.operation).into()),
                ));
            }
            if let Some(chunks) = chunks {
                let expected = chunks
                    .iter()
                    .map(|chunk| (chunk.len() as u64, digest(chunk)))
                    .collect::<Vec<_>>();
                check_logical_frames(&frames.sent, &expected, complete).map_err(|class| {
                    OracleViolation::new(
                        "stream_frames",
                        format!("{} step {step}: sent frames differ from IR", program.id),
                    )
                    .classified(class, CausalRole::Action((&action.operation).into()))
                })?;
                sources.push((
                    namespace_path(program, action.operation.path()),
                    Some(incarnation),
                    chunks,
                    expected,
                ));
            }
            evidence.push(frames);
        }
    }
    let mut constraints = BTreeMap::new();
    for frames in evidence {
        let action = &frames.program.actions[frames.step].operation;
        let (source, _, own_chunks) = stream_parameters(action).expect("stream evidence");
        if source && !matches!(action, ActionOp::StreamRoundTrip { .. }) {
            constraints.insert(
                (frames.program.id.as_str(), frames.step),
                vec![own_chunks.expect("writer chunks")],
            );
            continue;
        }
        let path = namespace_path(frames.program, action.path());
        let mut same_path = false;
        let mut same_incarnation = false;
        let mut frame_failure = None;
        let mut matching = Vec::new();
        for (source_path, incarnation, chunks, expected) in &sources {
            if *source_path != path {
                continue;
            }
            same_path = true;
            if incarnation.is_some_and(|incarnation| incarnation != frames.incarnation)
                || own_chunks.is_some_and(|own| !std::ptr::eq(own, *chunks))
            {
                continue;
            }
            same_incarnation = true;
            match check_logical_frames(&frames.received, expected, frames.complete) {
                Ok(()) => matching.push(*chunks),
                Err(class) => frame_failure = Some(class),
            }
        }
        if matching.is_empty() {
            let class = if !same_path {
                FailureClass::MissingStreamSource
            } else if !same_incarnation {
                FailureClass::ConflictingIncarnation
            } else {
                frame_failure.unwrap_or(FailureClass::FrameContentMismatch)
            };
            return Err(OracleViolation::new(
                "namespace_linearizability",
                format!(
                    "{} step {}: no matching logical frame source",
                    frames.program.id, frames.step
                ),
            )
            .classified(class, CausalRole::Action(action.into())));
        }
        constraints.insert((frames.program.id.as_str(), frames.step), matching);
    }
    Ok(constraints)
}

fn check_logical_frames(
    observed: &[(u64, &str)],
    expected: &[(u64, String)],
    complete: bool,
) -> Result<(), FailureClass> {
    if observed.len() > expected.len() || (complete && observed.len() != expected.len()) {
        return Err(FailureClass::FrameCountMismatch);
    }
    if observed.iter().zip(expected).any(
        |((length, digest), (expected_length, expected_digest))| {
            length != expected_length || *digest != expected_digest.as_str()
        },
    ) {
        return Err(FailureClass::FrameContentMismatch);
    }
    Ok(())
}

fn verify_namespace_histories(
    case: &BehaviorCase,
    observation: &CaseObservation,
    budget: Option<&Budget>,
    retained: Option<&BTreeMap<String, (u64, u64)>>,
) -> Result<(), OracleViolation> {
    let frame_sources = verify_stream_frames(case, observation, budget)?;
    let mut paths = BTreeSet::new();
    for program in &case.processes {
        for action in &program.actions {
            let path = namespace_path(program, action.operation.path());
            match &action.operation {
                ActionOp::PublishBlob { .. }
                | ActionOp::DescriptorWrite { .. }
                | ActionOp::Unlink { .. } => {
                    paths.insert(path);
                }
                ActionOp::Rename { destination, .. } => {
                    paths.insert(path);
                    paths.insert(namespace_path(program, destination));
                }
                ActionOp::StreamWrite { .. }
                | ActionOp::StreamRoundTrip { .. }
                | ActionOp::StreamRead { .. }
                | ActionOp::StreamReadWithRetry { .. }
                | ActionOp::StreamReadInto { .. } => {
                    paths.insert(path);
                }
                ActionOp::WaitForQuiescent { .. }
                    if !case.read_only_fixture_paths.contains(&path) =>
                {
                    paths.insert(path);
                }
                ActionOp::GatedStreamWrite { release_path, .. } => {
                    paths.insert(path);
                    paths.insert(namespace_path(program, release_path));
                }
                ActionOp::GatedStreamRead { observed_path, .. } => {
                    paths.insert(path);
                    paths.insert(namespace_path(program, observed_path));
                }
                ActionOp::ReadBlob { .. }
                | ActionOp::DescriptorRead { .. }
                | ActionOp::Lookup { .. }
                | ActionOp::AwaitEntry { .. }
                | ActionOp::WaitForQuiescent { .. }
                | ActionOp::MappingExportClose { .. } => {}
            }
            // A read alone cannot turn an initially absent attempt-owned name
            // into a fixture. Model its absence even when no writer was planned.
            let path = namespace_path(program, action.operation.path());
            if !case.read_only_fixture_paths.contains(&path)
                && (path.starts_with("/cases/") || path.starts_with("/runs/"))
                && matches!(
                    action.operation,
                    ActionOp::ReadBlob { .. }
                        | ActionOp::DescriptorRead { .. }
                        | ActionOp::Lookup { .. }
                        | ActionOp::AwaitEntry { .. }
                )
            {
                paths.insert(path);
            }
            // A linearized expectation on a declared read-only fixture path
            // races nothing (validate forbids mutating it); its outcome is
            // judged by the legacy success-count check on declared facts.
            if matches!(action.expected, ExpectedOutcome::Linearized { .. })
                && !case
                    .read_only_fixture_paths
                    .contains(&namespace_path(program, action.operation.path()))
            {
                paths.insert(namespace_path(program, action.operation.path()));
            }
        }
    }
    // Declared read-only fixtures cannot be mutated (case.validate enforces
    // this), so their IR kind/content assertions remain initial facts checked
    // by the action oracle. They do not participate in namespace races.
    // In particular a read-only snapshot probe must not recursively require
    // another snapshot merely to validate its own observations.
    // Hints order constructive attempts only, never prune the frontier.
    // A path can be replaced or renamed: its smallest observed revision is
    // not necessarily the receipt of the currently enabled mutation.
    let mut revision_hints = BTreeMap::<String, u64>::new();
    for execution in &observation.executions {
        let program = case
            .processes
            .iter()
            .find(|program| program.id == execution.process)
            .expect("execution identity checked before namespace verification");
        for result in &execution.results {
            if result.outcome == "ok" {
                if let Some(revision) = result.revision {
                    revision_hints
                        .entry(namespace_path(program, &result.path))
                        .and_modify(|known| *known = (*known).min(revision))
                        .or_insert(revision);
                }
            }
        }
    }
    if let Some(retained) = retained {
        for (path, (revision, _)) in retained {
            revision_hints
                .entry(path.clone())
                .and_modify(|known| *known = (*known).min(*revision))
                .or_insert(*revision);
        }
    }
    let mut programs = Vec::with_capacity(case.processes.len());
    let mut dependencies = Vec::with_capacity(case.processes.len());
    for program in &case.processes {
        check_budget(budget, "namespace evidence preparation")?;
        let execution = observation
            .executions
            .iter()
            .find(|execution| execution.process == program.id)
            .expect("execution presence checked before namespace verification");
        let stopped = matches!(&case.failure, FailureInjection::StopProcess { process, .. } if process == &program.id)
            && !execution.exit_success
            && execution.terminal
            && execution
                .lifecycle
                .iter()
                .any(|event| event == "context_ready");
        // A launch failure produces no action results: nothing executed, so
        // the process contributes no namespace actions at all. Requiring
        // per-step evidence here would conflate "never ran" with
        // "interrupted mid-action".
        if execution.results.is_empty() && !stopped {
            programs.push(Vec::new());
            dependencies.push(Vec::new());
            continue;
        }
        let mut actions = Vec::with_capacity(program.actions.len());
        for (step, action) in program.actions.iter().enumerate() {
            let path = namespace_path(program, action.operation.path());
            let relevant = paths.contains(&path);
            if stopped
                && !execution
                    .results
                    .iter()
                    .any(|record| record.step == step && !is_barrier_record(record))
            {
                if let Some(stopped_action) = namespace_stopped_stream(
                    case,
                    observation,
                    program,
                    execution,
                    step,
                    &action.operation,
                    &frame_sources,
                )? {
                    if let ActionOp::GatedStreamRead { observed_path, .. } = &action.operation
                        && let Some(record) = execution.results.iter().find(|record| {
                            record.step == step
                                && matches!(
                                    record.barrier,
                                    Some(BarrierObservation::StreamFirstFrame { .. })
                                )
                        })
                        && let Some(BarrierObservation::StreamFirstFrame { incarnation }) =
                            record.barrier
                    {
                        // The generated reader commits its marker before this
                        // milestone. A stop cannot erase that namespace effect.
                        let marker = namespace_path(program, observed_path);
                        for operation in [
                            NamespaceOperation::StreamOpen {
                                path: path.clone(),
                                incarnation: Some(incarnation),
                                source: false,
                                replace: false,
                                chunks: None,
                                gated: false,
                            },
                            NamespaceOperation::StreamAttach {
                                path: path.clone(),
                                incarnation,
                                roundtrip: false,
                            },
                            NamespaceOperation::StreamFirstFrame {
                                path: path.clone(),
                                incarnation,
                                source: false,
                            },
                            NamespaceOperation::GatePublish { path: marker },
                        ] {
                            let prefixes = if operation.writes() {
                                &program.access.write_prefixes
                            } else {
                                &program.access.read_prefixes
                            };
                            actions.push(NamespaceAction {
                                step,
                                stream_owner: None,
                                preparation: false,
                                denied: operation
                                    .path()
                                    .is_some_and(|path| !namespace_prefix_allows(prefixes, path)),
                                operation,
                                role: (&action.operation).into(),
                                result: Some(record),
                                publication_finished: true,
                            });
                        }
                    }
                    actions.push(stopped_action);
                }
                // Only the first uncompleted transfer can have started.
                // Route preparations for later steps are added independently.
                break;
            }
            let operation = match &action.operation {
                ActionOp::PublishBlob { bytes, .. } => NamespaceOperation::Publish {
                    path,
                    bytes,
                    commit: true,
                    exclusive: false,
                    require_existing: false,
                },
                ActionOp::DescriptorWrite {
                    flags,
                    length,
                    bytes,
                    finish,
                    ..
                } if relevant => {
                    let exclusive =
                        *flags == (libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC);
                    let require_existing = *flags == (libc::O_WRONLY | libc::O_TRUNC);
                    if (!exclusive && !require_existing) || *length != Some(bytes.len() as u64) {
                        return violation(
                            "namespace_model_evidence",
                            format!(
                                "{} step {step}: descriptor publication needs canonical flags/allocation",
                                program.id,
                            ),
                        );
                    }
                    NamespaceOperation::Publish {
                        path,
                        bytes,
                        commit: matches!(
                            finish,
                            DescriptorFinish::Close | DescriptorFinish::CloseTwice
                        ),
                        exclusive,
                        require_existing,
                    }
                }
                ActionOp::ReadBlob { expected, .. } if relevant => NamespaceOperation::Read {
                    path,
                    expected,
                    offset: 0,
                    whole: true,
                },
                ActionOp::DescriptorRead {
                    flags,
                    expected,
                    method,
                    offset,
                    ..
                } if relevant => {
                    if *flags != libc::O_RDONLY {
                        return violation(
                            "namespace_model_evidence",
                            format!(
                                "{} step {step}: namespace descriptor read needs modeled flags",
                                program.id,
                            ),
                        );
                    }
                    NamespaceOperation::Read {
                        path,
                        expected,
                        offset: if *method == DescriptorReadMethod::Mapping {
                            usize::try_from(*offset).map_err(|_| {
                                OracleViolation::new(
                                    "namespace_model_evidence",
                                    "descriptor mapping offset exceeds model address space",
                                )
                                .at_action(&action.operation)
                            })?
                        } else {
                            0
                        },
                        whole: false,
                    }
                }
                ActionOp::Lookup { .. } | ActionOp::AwaitEntry { .. } if relevant => {
                    NamespaceOperation::Lookup { path }
                }
                ActionOp::Rename {
                    destination,
                    replace,
                    ..
                } => NamespaceOperation::Rename {
                    source: path,
                    destination: namespace_path(program, destination),
                    replace: *replace,
                },
                ActionOp::Unlink { .. } => NamespaceOperation::Unlink { path },
                ActionOp::StreamWrite {
                    replace, chunks, ..
                } => NamespaceOperation::StreamOpen {
                    path,
                    incarnation: stream_incarnation_evidence(
                        execution,
                        step,
                        &program.id,
                        &action.operation,
                    )?,
                    source: true,
                    replace: *replace,
                    chunks: Some(chunks),
                    gated: false,
                },
                ActionOp::StreamRoundTrip { chunks, .. } => NamespaceOperation::StreamOpen {
                    path,
                    incarnation: stream_incarnation_evidence(
                        execution,
                        step,
                        &program.id,
                        &action.operation,
                    )?,
                    source: true,
                    replace: false,
                    chunks: Some(chunks),
                    gated: false,
                },
                ActionOp::GatedStreamWrite {
                    frames, replace, ..
                } => NamespaceOperation::StreamOpen {
                    path,
                    incarnation: stream_incarnation_evidence(
                        execution,
                        step,
                        &program.id,
                        &action.operation,
                    )?,
                    source: true,
                    replace: *replace,
                    chunks: Some(frames),
                    gated: true,
                },
                ActionOp::StreamRead { .. }
                | ActionOp::StreamReadWithRetry { .. }
                | ActionOp::GatedStreamRead { .. }
                | ActionOp::StreamReadInto { .. } => NamespaceOperation::StreamOpen {
                    path,
                    incarnation: stream_incarnation_evidence(
                        execution,
                        step,
                        &program.id,
                        &action.operation,
                    )?,
                    source: false,
                    replace: false,
                    chunks: None,
                    gated: false,
                },
                ActionOp::WaitForQuiescent { .. } if relevant => {
                    NamespaceOperation::StreamProbe { path }
                }
                ActionOp::MappingExportClose { .. } if relevant => {
                    return violation(
                        "namespace_model_evidence",
                        format!(
                            "{} step {step}: mapped descriptor namespace effects are not observed",
                            program.id,
                        ),
                    );
                }
                ActionOp::ReadBlob { .. }
                | ActionOp::DescriptorRead { .. }
                | ActionOp::DescriptorWrite { .. }
                | ActionOp::Lookup { .. }
                | ActionOp::AwaitEntry { .. }
                | ActionOp::WaitForQuiescent { .. }
                | ActionOp::MappingExportClose { .. } => NamespaceOperation::Ignore,
            };
            let Some(result) = execution
                .results
                .iter()
                .find(|result| result.step == step && !is_barrier_record(result))
            else {
                if operation.path().is_some() {
                    return violation(
                        "namespace_model_evidence",
                        format!(
                            "{} step {step}: interrupted namespace action has no commit/reservation evidence",
                            program.id,
                        ),
                    );
                }
                continue;
            };
            let mut mutation_seen = false;
            for record in execution
                .results
                .iter()
                .filter(|record| record.step == step)
            {
                let Some(BarrierObservation::MutationApplied {
                    from_revision,
                    to_revision,
                }) = &record.barrier
                else {
                    continue;
                };
                let receipt = match &operation {
                    NamespaceOperation::Rename { .. } | NamespaceOperation::Unlink { .. } => {
                        result.revision
                    }
                    NamespaceOperation::StreamOpen {
                        source: true,
                        replace: true,
                        incarnation,
                        ..
                    } => *incarnation,
                    _ => None,
                };
                if mutation_seen
                    || result.outcome != "ok"
                    || result.errno.is_some()
                    || !is_barrier_record(record)
                    || record.process != program.id
                    || record.path != action.operation.path()
                    || record.action != action.operation.class().as_str()
                    || receipt.is_none()
                    || *to_revision != receipt
                    || execution
                        .results
                        .iter()
                        .position(|seen| std::ptr::eq(seen, record))
                        >= execution
                            .results
                            .iter()
                            .position(|seen| std::ptr::eq(seen, result))
                {
                    return Err(OracleViolation::new(
                        "namespace_model_evidence",
                        format!("{} step {step}: inconsistent mutation receipt", program.id),
                    )
                    .classified(
                        FailureClass::InvalidEvidence,
                        CausalRole::Action((&action.operation).into()),
                    ));
                }
                mutation_seen = true;
                if let Some(revision) = from_revision {
                    let path = namespace_path(program, action.operation.path());
                    actions.push(NamespaceAction {
                        step,
                        stream_owner: None,
                        preparation: false,
                        denied: !namespace_prefix_allows(&program.access.read_prefixes, &path),
                        operation: NamespaceOperation::MutationOrigin {
                            path,
                            revision: *revision,
                        },
                        role: (&action.operation).into(),
                        result: None,
                        publication_finished: true,
                    });
                }
            }
            let publication_finished = descriptor_completion(&action.operation, result)
                .map_err(|error| error.at_action(&action.operation))?;
            let prefixes = if operation.writes() {
                &program.access.write_prefixes
            } else {
                &program.access.read_prefixes
            };
            let denied = operation
                .path()
                .is_some_and(|path| !namespace_prefix_allows(prefixes, path))
                || matches!(&operation, NamespaceOperation::Rename { destination, .. }
                    if !namespace_prefix_allows(prefixes, destination));
            if !denied {
                for path in operation.path().into_iter().chain(match &operation {
                    NamespaceOperation::Rename { destination, .. } => Some(destination.as_str()),
                    _ => None,
                }) {
                    // Attempt-owned paths are cleaned before execution. Fixture
                    // names alone do not prove their initial kind or contents.
                    if !case.read_only_fixture_paths.contains(path)
                        && !(path.starts_with("/cases/") || path.starts_with("/runs/"))
                    {
                        return violation(
                            "namespace_model_evidence",
                            format!(
                                "initial namespace snapshot missing for {path}: need kind, revision, active, blob length and digest",
                            ),
                        );
                    }
                }
            }
            let stream = match &operation {
                NamespaceOperation::StreamOpen {
                    path,
                    incarnation,
                    source,
                    ..
                } if !denied
                    && (incarnation.is_some() || namespace_errno(result, libc::ESTALE)) =>
                {
                    Some((path.clone(), *incarnation, *source))
                }
                _ => None,
            };
            actions.push(NamespaceAction {
                step,
                stream_owner: None,
                preparation: false,
                role: (&action.operation).into(),
                operation,
                result: Some(result),
                denied,
                publication_finished,
            });
            if let Some((path, incarnation, source)) = stream {
                let mut push = |operation| {
                    actions.push(NamespaceAction {
                        step,
                        stream_owner: None,
                        preparation: false,
                        role: (&action.operation).into(),
                        operation,
                        result: Some(result),
                        denied: false,
                        publication_finished: true,
                    })
                };
                let Some(incarnation) = incarnation else {
                    push(NamespaceOperation::StreamPendingFailure { path });
                    continue;
                };
                push(NamespaceOperation::StreamAttach {
                    path: path.clone(),
                    incarnation,
                    roundtrip: matches!(action.operation, ActionOp::StreamRoundTrip { .. }),
                });
                match &action.operation {
                    ActionOp::GatedStreamWrite { release_path, .. }
                        if result.outcome == "ok"
                            || execution.results.iter().any(|record| {
                                record.step == step
                                    && matches!(
                                        &record.barrier,
                                        Some(BarrierObservation::ReleaseObserved { .. })
                                    )
                            }) =>
                    {
                        push(NamespaceOperation::StreamFirstFrame {
                            path: path.clone(),
                            incarnation,
                            source,
                        });
                        let release_path = namespace_path(program, release_path);
                        if execution.results.iter().any(|record| record.step == step
                            && matches!(&record.barrier, Some(BarrierObservation::ReleaseObserved { path: observed })
                                if namespace_path(program, observed) != release_path))
                        {
                            return violation("namespace_model_evidence", "gated release milestone names the wrong path");
                        }
                        if !namespace_prefix_allows(&program.access.read_prefixes, &release_path) {
                            return violation(
                                "namespace_model_evidence",
                                "gated release lacks read authority",
                            );
                        }
                        push(NamespaceOperation::GateLookup { path: release_path });
                    }
                    ActionOp::GatedStreamRead { observed_path, .. }
                        if result.outcome == "ok"
                            || execution.results.iter().any(|record| {
                                record.step == step
                                    && matches!(
                                        &record.barrier,
                                        Some(BarrierObservation::StreamFirstFrame { .. })
                                    )
                            }) =>
                    {
                        push(NamespaceOperation::StreamFirstFrame {
                            path: path.clone(),
                            incarnation,
                            source,
                        });
                        let observed_path = namespace_path(program, observed_path);
                        if !namespace_prefix_allows(&program.access.write_prefixes, &observed_path)
                        {
                            return violation(
                                "namespace_model_evidence",
                                "gated publication lacks write authority",
                            );
                        }
                        push(NamespaceOperation::GatePublish {
                            path: observed_path,
                        });
                    }
                    _ => {}
                }
                let expected = match &action.operation {
                    ActionOp::StreamRead { expected, .. }
                    | ActionOp::StreamReadWithRetry { expected, .. }
                    | ActionOp::GatedStreamRead { expected, .. }
                    | ActionOp::StreamReadInto { expected, .. } => Some(expected.as_slice()),
                    _ => None,
                };
                push(NamespaceOperation::StreamComplete {
                    path,
                    incarnation,
                    source,
                    expected,
                    frame_sources: frame_sources
                        .get(&(program.id.as_str(), step))
                        .cloned()
                        .unwrap_or_default(),
                    frames_observed: execution.results.iter().any(|record| {
                        record.step == step
                            && matches!(
                                record.barrier,
                                Some(BarrierObservation::StreamFrame { .. })
                            )
                    }),
                });
            }
        }
        programs.push(actions);
        dependencies.push(
            program
                .depends_on
                .iter()
                .map(|dependency| {
                    case.processes
                        .iter()
                        .position(|candidate| &candidate.id == dependency)
                        .expect("dependencies validated before namespace verification")
                })
                .collect::<Vec<_>>(),
        );
    }
    prepare_namespace_routes(
        case,
        observation,
        &mut programs,
        &mut dependencies,
        &frame_sources,
    )?;
    let initial = NamespaceHistory {
        positions: vec![0; programs.len()],
        publications: BTreeSet::new(),
        publication_lookups: BTreeSet::new(),
        reservations: BTreeMap::new(),
        streams: BTreeMap::new(),
        active_streams: BTreeMap::new(),
        open_streams: BTreeMap::new(),
        sessions: BTreeMap::new(),
        entries: BTreeMap::new(),
        initial_revision: initial_fixture_revision(case, observation)?,
        revisions: Vec::new(),
    };
    // Greedy constructive replay: on success it IS a legal history (sound
    // acceptance without any interleaving search). On failure the full
    // frontier exploration below remains the deciding authority.
    match verify_namespace_constructively(
        &initial,
        &programs,
        &dependencies,
        &revision_hints,
        budget,
        retained,
    ) {
        Ok(()) => return Ok(()),
        Err(error) if error.invariant == "execution_budget" => return Err(error),
        Err(_) => {}
    }
    // Depth-first exploration retains each discovered legal state once. The
    // visited and queued sets together are the bounded legal-state frontier;
    // exceeding the bound is a model explosion, never permission to select a
    // convenient winner.
    let mut visited = BTreeSet::new();
    let mut queued = BTreeSet::from([initial.clone()]);
    let mut stack = Vec::from([initial]);
    let mut path_keys = BTreeMap::<&str, u64>::new();
    for program in &programs {
        for action in program {
            let observed_value = action
                .result
                .and_then(|result| result.revision.or(result.incarnation))
                .or_else(|| match action.operation {
                    NamespaceOperation::MutationOrigin { revision, .. } => Some(revision),
                    _ => None,
                })
                .filter(|value| *value != 0);
            if let (Some(path), Some(value)) = (action.operation.path(), observed_value) {
                path_keys
                    .entry(path)
                    .and_modify(|known| *known = (*known).min(value))
                    .or_insert(value);
            }
        }
    }
    let mut dead_end_role: Option<(usize, SemanticAction)> = None;
    let mut retained_conflict = false;
    while let Some(mut state) = stack.pop() {
        queued.remove(&state);
        check_budget(budget, "namespace interleaving search")?;
        // Invisible, unrelated operations commute; collapse them without
        // charging the race bound.
        loop {
            let mut advanced = false;
            for process in 0..programs.len() {
                if dependencies[process]
                    .iter()
                    .any(|dependency| state.positions[*dependency] < programs[*dependency].len())
                {
                    continue;
                }
                while programs[process]
                    .get(state.positions[process])
                    .is_some_and(|action| matches!(action.operation, NamespaceOperation::Ignore))
                {
                    state.positions[process] += 1;
                    advanced = true;
                }
            }
            if !advanced {
                break;
            }
        }
        if !visited.insert(state.clone()) {
            continue;
        }
        if visited.len() > case.resource_bounds.max_race_states as usize {
            return violation(
                "namespace_model_explosion",
                format!(
                    "namespace search exceeds {} legal states",
                    case.resource_bounds.max_race_states
                ),
            );
        }
        // Pinned-value feasibility: the next committed slot's value can no
        // longer fall below initial + slots + 1, so any pending operation
        // whose observed revision or incarnation is pinned below that floor
        // can never commit (renames and unlinks always allocate; a stream
        // open can only survive as an attach to a live session already
        // carrying that value). Prune such states instead of exploring the
        // interleavings that strand them.
        let mut previous = state.initial_revision;
        let mut distance = 0_u64;
        for known in &state.revisions {
            distance += 1;
            if let Some(known) = known {
                previous = *known;
                distance = 0;
            }
        }
        let next_floor = previous
            .checked_add(distance)
            .and_then(|value| value.checked_add(1));
        // These causal checks are unconditional. Environment or traversal
        // order must never change which legal histories the oracle retains.
        let mut doomed = false;
        for process in 0..programs.len() {
            for action in &programs[process][state.positions[process]..] {
                let Some(value) = action
                    .result
                    .filter(|result| result.outcome == "ok")
                    .and_then(|result| result.revision.or(result.incarnation))
                else {
                    continue;
                };
                if next_floor.is_some_and(|floor| value >= floor) {
                    continue;
                }
                match &action.operation {
                    NamespaceOperation::Rename { .. } | NamespaceOperation::Unlink { .. } => {
                        doomed = true;
                    }
                    NamespaceOperation::Lookup { path } => {
                        // A lookup observes the path's value at lookup time.
                        // If the entry already carries a different pinned
                        // value it can never return to this one (values only
                        // grow), and an absent entry can only be created at
                        // or beyond the floor. An unconstrained slot may
                        // still legally take this value when the lookup
                        let entry = state.entries.get(path);
                        let entry_value = entry.and_then(|binding| {
                            state.revisions.get(binding.revision).copied().flatten()
                        });
                        let stream_slot = state.streams.get(path);
                        let stream_value = stream_slot
                            .and_then(|slot| state.revisions.get(*slot).copied().flatten());
                        let doomed_entry = matches!(entry_value, Some(pinned) if pinned != value);
                        let doomed_stream = matches!(stream_value, Some(pinned) if pinned != value);
                        let absent = entry.is_none()
                            && stream_slot.is_none()
                            && !state.reservations.contains_key(path);
                        if doomed_entry || doomed_stream || absent {
                            doomed = true;
                        }
                    }
                    NamespaceOperation::StreamOpen { .. } => {
                        let attachable = state.sessions.keys().any(|slot| {
                            state.revisions.get(*slot).copied().flatten() == Some(value)
                        });
                        if !attachable {
                            doomed = true;
                        }
                    }
                    _ => {}
                }
                if doomed {
                    break;
                }
            }
            if doomed {
                break;
            }
        }
        if doomed {
            continue;
        }
        if namespace_history_complete(&state, &programs) {
            if namespace_retained_matches(&state, retained) {
                return Ok(());
            }
            retained_conflict = true;
            continue;
        }
        // Evidence-ordered exploration: successors whose observed revision
        // or incarnation evidence orders them first (the same candidate key
        // the constructive replay uses) are expanded before unconstrained
        // commits, and slot-free asynchronous stream events lead. Blind
        // process-order search drowns in the interleavings of independent
        // mutations even when every observation pins a legal history.
        let mut successors = Vec::<(u64, NamespaceHistory)>::new();
        for process in 0..programs.len() {
            let Some(action) = ready_action(&state, &programs, &dependencies, process) else {
                continue;
            };
            let key = action
                .result
                .and_then(|result| result.revision.or(result.incarnation))
                .or_else(|| match action.operation {
                    NamespaceOperation::StreamOpen { incarnation, .. } => incarnation,
                    _ => None,
                })
                .or_else(|| {
                    operation_paths(&action.operation).find_map(|path| path_keys.get(path).copied())
                })
                .unwrap_or(u64::MAX);
            if let Some(next_state) = namespace_transition(&state, process, action) {
                successors.push((key, next_state));
            }
            if let Some(next_state) = namespace_interrupted_open(&state, process, action) {
                successors.push((key, next_state));
            }
        }
        for path in state.active_streams.keys() {
            if let Some(next_state) = namespace_quiescence(&state, path) {
                successors.push((0, next_state));
            }
            if let Some(next_state) = namespace_first_frame(&state, path) {
                successors.push((0, next_state));
            }
        }
        if successors.is_empty() {
            let progress = state.positions.iter().sum();
            let role = (0..programs.len())
                .filter_map(|process| ready_action(&state, &programs, &dependencies, process))
                .map(|action| action.role)
                .min();
            if let Some(role) = role
                && dead_end_role.is_none_or(|(previous, previous_role)| {
                    progress > previous || (progress == previous && role < previous_role)
                })
            {
                dead_end_role = Some((progress, role));
            }
        }
        successors.sort_by(|left, right| left.0.cmp(&right.0));
        for (_, next_state) in successors.into_iter().rev() {
            if visited.contains(&next_state) || !queued.insert(next_state.clone()) {
                continue;
            }
            if visited.len() + queued.len() > case.resource_bounds.max_race_states as usize {
                return violation(
                    "namespace_model_explosion",
                    format!(
                        "namespace search exceeds {} legal states",
                        case.resource_bounds.max_race_states
                    ),
                );
            }
            stack.push(next_state);
        }
    }
    if retained_conflict {
        return Err(OracleViolation::new(
            "namespace_linearizability",
            "final retained blob snapshot has no matching history",
        )
        .classified(FailureClass::RetainedBlobMismatch, CausalRole::Namespace));
    }
    Err(namespace_history_failure(
        case,
        observation,
        dead_end_role.map(|(_, role)| role),
    ))
}

fn namespace_retained_matches(
    state: &NamespaceHistory<'_>,
    retained: Option<&BTreeMap<String, (u64, u64)>>,
) -> bool {
    let Some(retained) = retained else {
        return true;
    };
    if state.entries.len() != retained.len()
        || state.entries.iter().any(|(path, binding)| {
            retained.get(path).is_none_or(|(_, length)| {
                usize::try_from(*length).ok() != Some(binding.bytes.len())
            })
        })
    {
        return false;
    }
    let mut constrained = state.clone();
    retained.iter().all(|(path, (revision, _))| {
        constrained.constrain_revision(state.entries[path].revision, *revision)
    })
}
/// Diagnose an already-rejected history from typed facts, never from its
/// formatted diagnostic. This does not prune or alter the successor set.
fn namespace_history_failure(
    case: &BehaviorCase,
    observation: &CaseObservation,
    role: Option<SemanticAction>,
) -> OracleViolation {
    let failure = || {
        OracleViolation::new(
        "namespace_linearizability",
        "no namespace history satisfies action results, process order, dependencies and revisions",
    ).classified(FailureClass::NamespaceHistoryConflict, role.map_or(CausalRole::Namespace, CausalRole::Action))
    };
    for program in &case.processes {
        let Some(execution) = observation
            .executions
            .iter()
            .find(|entry| entry.process == program.id)
        else {
            continue;
        };
        for (step, action) in program.actions.iter().enumerate() {
            let expected = match &action.operation {
                ActionOp::StreamRead { expected, .. }
                | ActionOp::StreamReadWithRetry { expected, .. }
                | ActionOp::GatedStreamRead { expected, .. }
                | ActionOp::StreamReadInto { expected, .. } => expected,
                _ => continue,
            };
            let Some(result) = execution
                .results
                .iter()
                .find(|record| record.step == step && !is_barrier_record(record))
            else {
                continue;
            };
            let Some(incarnation) = result.incarnation else {
                continue;
            };
            let path = namespace_path(program, action.operation.path());
            let mut same_path = false;
            let mut same_incarnation = false;
            let mut matching_content = false;
            for producer in &case.processes {
                let Some(producer_execution) = observation
                    .executions
                    .iter()
                    .find(|entry| entry.process == producer.id)
                else {
                    continue;
                };
                for (producer_step, producer_action) in producer.actions.iter().enumerate() {
                    let chunks = match &producer_action.operation {
                        ActionOp::StreamWrite { chunks, .. }
                        | ActionOp::StreamRoundTrip { chunks, .. }
                        | ActionOp::GatedStreamWrite { frames: chunks, .. } => chunks,
                        _ => continue,
                    };
                    if namespace_path(producer, producer_action.operation.path()) != path {
                        continue;
                    }
                    same_path = true;
                    if !producer_execution.results.iter().any(|record| record.step == producer_step
                        && (record.incarnation == Some(incarnation)
                            || matches!(record.barrier, Some(BarrierObservation::StreamOpened { incarnation: observed })
                                if observed == incarnation)))
                    { continue; }
                    same_incarnation = true;
                    matching_content |= if let Some(transfer) = &result.transfer {
                        payload_summary(chunks.iter().map(Vec::as_slice), Some(transfer.length))
                            .is_some_and(|(length, digest)| {
                                transfer.length <= length
                                    && (!transfer.complete || transfer.length == length)
                                    && transfer.digest == digest
                            })
                    } else {
                        chunks.iter().flatten().eq(expected.iter())
                    };
                }
            }
            let class = if !same_path {
                FailureClass::MissingStreamSource
            } else if !same_incarnation {
                FailureClass::ConflictingIncarnation
            } else if !matching_content {
                FailureClass::StreamSourceContent
            } else {
                continue;
            };
            return failure().classified(class, CausalRole::Action((&action.operation).into()));
        }
    }
    failure()
}

fn stream_parameters(operation: &ActionOp) -> Option<(bool, bool, Option<&[Vec<u8>]>)> {
    match operation {
        ActionOp::StreamWrite {
            chunks, replace, ..
        } => Some((true, *replace, Some(chunks))),
        ActionOp::StreamRoundTrip { chunks, .. } => Some((true, false, Some(chunks))),
        ActionOp::GatedStreamWrite {
            frames, replace, ..
        } => Some((true, *replace, Some(frames))),
        ActionOp::StreamRead { .. }
        | ActionOp::StreamReadWithRetry { .. }
        | ActionOp::GatedStreamRead { .. }
        | ActionOp::StreamReadInto { .. } => Some((false, false, None)),
        _ => None,
    }
}

fn namespace_stopped_stream<'a>(
    case: &BehaviorCase,
    observation: &CaseObservation,
    program: &ProcessProgram,
    execution: &ExecutionObservation,
    step: usize,
    operation: &'a ActionOp,
    frame_sources: &StreamFrameSources<'a>,
) -> Result<Option<NamespaceAction<'a>>, OracleViolation> {
    let Some((source, replace, chunks)) = stream_parameters(operation) else {
        return Ok(None);
    };
    let records = || {
        execution
            .results
            .iter()
            .filter(move |record| record.step == step)
    };
    if records().any(|record| {
        record.process != program.id
            || record.path != operation.path()
            || !stream_record_action_matches(operation, record)
    }) {
        return violation(
            "namespace_model_evidence",
            "stopped stream milestone belongs to another action",
        );
    }
    let incarnation = if records().any(|record| {
        matches!(
            record.barrier,
            Some(
                BarrierObservation::StreamOpened { .. }
                    | BarrierObservation::StreamFirstFrame { .. }
                    | BarrierObservation::StreamFrame { .. }
                    | BarrierObservation::StreamEof { .. }
            )
        )
    }) {
        stream_incarnation_evidence(execution, step, &program.id, operation)?
    } else {
        None
    };
    let prefixes = if source {
        &program.access.write_prefixes
    } else {
        &program.access.read_prefixes
    };
    let path = namespace_path(program, operation.path());
    let mut stop_gates = Vec::new();
    if let FailureInjection::StopProcess { phase, .. } = &case.failure
        && matches!(
            phase,
            ProcessStopPhase::AfterStreamFirstFrame
                | ProcessStopPhase::AfterSiblingStreamFirstFrame
        )
    {
        for peer in &case.processes {
            let Some(execution) = observation
                .executions
                .iter()
                .find(|execution| execution.process == peer.id)
            else {
                continue;
            };
            for record in &execution.results {
                let Some(BarrierObservation::StreamFirstFrame {
                    incarnation: observed,
                }) = record.barrier
                else {
                    continue;
                };
                let Some(action) = peer.actions.get(record.step) else {
                    continue;
                };
                let peer_path = namespace_path(peer, action.operation.path());
                let marker = match &action.operation {
                    ActionOp::GatedStreamRead { observed_path, .. } => {
                        Some(namespace_path(peer, observed_path))
                    }
                    _ => None,
                };
                let relevant = if *phase == ProcessStopPhase::AfterSiblingStreamFirstFrame {
                    peer.id != program.id && marker.is_some()
                } else {
                    peer_path == path && incarnation == Some(observed)
                };
                if relevant {
                    stop_gates.push((peer_path, observed, marker));
                }
            }
        }
        if stop_gates.is_empty() {
            return violation(
                "namespace_model_evidence",
                "stream stop omitted its causal first-frame gate",
            );
        }
    }
    Ok(Some(NamespaceAction {
        step,
        stream_owner: None,
        preparation: false,
        role: operation.into(),
        denied: !namespace_prefix_allows(prefixes, &path),
        operation: NamespaceOperation::StreamStop {
            path,
            incarnation,
            source,
            replace,
            roundtrip: matches!(operation, ActionOp::StreamRoundTrip { .. }),
            chunks,
            gated: matches!(operation, ActionOp::GatedStreamWrite { .. }),
            first_frame: records().any(|record| {
                matches!(
                    record.barrier,
                    Some(
                        BarrierObservation::StreamFirstFrame { .. }
                            | BarrierObservation::StreamFrame { .. }
                    )
                )
            }),
            eof: records()
                .any(|record| matches!(record.barrier, Some(BarrierObservation::StreamEof { .. }))),
            frame_sources: frame_sources
                .get(&(program.id.as_str(), step))
                .cloned()
                .unwrap_or_default(),
            stop_gates,
        },
        result: None,
        publication_finished: false,
    }))
}

// Endpoint preparation starts beside the serial prefix so a prefix blocked on
// an upstream route cannot deadlock a downstream stream rendezvous. Every open
// must still precede the original scheduler slot's transfer suffix; receipt
// order never orders different opens.
fn prepare_namespace_routes<'a>(
    case: &'a BehaviorCase,
    observation: &'a CaseObservation,
    programs: &mut Vec<Vec<NamespaceAction<'a>>>,
    dependencies: &mut Vec<Vec<usize>>,
    frame_sources: &StreamFrameSources<'a>,
) -> Result<(), OracleViolation> {
    let route_paths: BTreeSet<_> = case
        .routes
        .iter()
        .flat_map(|route| route.edges.iter().map(|edge| edge.path.as_str()))
        .collect();
    for (process, program) in case.processes.iter().enumerate() {
        let stream_steps: BTreeSet<_> = program
            .actions
            .iter()
            .enumerate()
            .filter(|(_, action)| {
                route_paths.contains(action.operation.path())
                    && stream_parameters(&action.operation).is_some()
            })
            .map(|(step, _)| step)
            .collect();
        if stream_steps.is_empty() {
            continue;
        }
        let first_step = program
            .actions
            .iter()
            .position(|action| {
                route_paths.contains(action.operation.path())
                    && (stream_parameters(&action.operation).is_some()
                        || matches!(
                            action.operation,
                            ActionOp::PublishBlob { .. } | ActionOp::ReadBlob { .. }
                        ))
            })
            .expect("a stream route endpoint is a route transfer");
        let execution = observation
            .executions
            .iter()
            .find(|execution| execution.process == program.id)
            .expect("execution presence checked before namespace verification");
        let evidence_failure = |class, step: Option<usize>, detail: String| {
            let role = step
                .and_then(|step| program.actions.get(step))
                .map_or(CausalRole::Namespace, |action| {
                    CausalRole::Action((&action.operation).into())
                });
            OracleViolation::new("namespace_model_evidence", detail).classified(class, role)
        };
        let stopped = matches!(&case.failure, FailureInjection::StopProcess { process, .. }
            if process == &program.id)
            && execution
                .lifecycle
                .iter()
                .any(|event| event == "context_ready");
        if execution.results.is_empty() && !stopped {
            continue;
        }
        let mut opened = BTreeMap::new();
        let mut next_terminal = 0;
        let mut last_step = None;
        let mut transferring = false;
        for record in &execution.results {
            let Some(action) = program.actions.get(record.step) else {
                return Err(evidence_failure(
                    FailureClass::InvalidEvidence,
                    None,
                    "route record names an unknown step".to_owned(),
                ));
            };
            if record.process != program.id
                || record.path != action.operation.path()
                || !stream_record_action_matches(&action.operation, record)
            {
                return Err(evidence_failure(
                    FailureClass::InvalidEvidence,
                    Some(record.step),
                    "route record belongs to another action".to_owned(),
                ));
            }
            if stream_steps.contains(&record.step)
                && matches!(
                    record.barrier,
                    Some(BarrierObservation::StreamOpened { .. })
                )
            {
                let already_opened = opened.insert(record.step, record).is_some();
                if !is_barrier_record(record) || transferring || already_opened {
                    return Err(evidence_failure(
                        FailureClass::MilestoneOrder,
                        Some(record.step),
                        format!(
                            "{} step {}: route preparation violates the transfer fence \
                             (transferring={transferring}, already_opened={already_opened})",
                            program.id, record.step
                        ),
                    ));
                }
                continue;
            }
            if last_step.is_some_and(|step| record.step < step)
                || record.step > next_terminal
                || (!is_barrier_record(record) && record.step != next_terminal)
            {
                return Err(evidence_failure(
                    FailureClass::MilestoneOrder,
                    Some(record.step),
                    "route data results violate action order".to_owned(),
                ));
            }
            last_step = Some(record.step);
            if record.step >= first_step {
                transferring = true;
                if opened.len() != stream_steps.len() {
                    return Err(evidence_failure(
                        FailureClass::MissingEvidence,
                        Some(record.step),
                        "route transfer precedes complete endpoint preparation".to_owned(),
                    ));
                }
            }
            if !is_barrier_record(record) {
                next_terminal += 1;
            }
        }
        // A stopped prefix never reached the common preparation phase.
        if next_terminal < first_step {
            continue;
        }
        if stopped {
            for &step in &stream_steps {
                if !programs[process].iter().any(|action| action.step == step)
                    && let Some(action) = namespace_stopped_stream(
                        case,
                        observation,
                        program,
                        execution,
                        step,
                        &program.actions[step].operation,
                        frame_sources,
                    )?
                {
                    programs[process].push(action);
                }
            }
        }
        let mut prefix = std::mem::take(&mut programs[process]);
        let prefix_end = prefix.partition_point(|action| action.step < first_step);
        let actions = prefix.split_off(prefix_end);
        let original_dependencies = std::mem::take(&mut dependencies[process]);
        let mut transfer_dependencies = Vec::new();
        if !prefix.is_empty() {
            let prefix_process = programs.len();
            programs.push(prefix);
            dependencies.push(original_dependencies.clone());
            transfer_dependencies.push(prefix_process);
        }
        let mut owners = BTreeMap::new();
        for &step in &stream_steps {
            let owner = programs.len();
            owners.insert(step, owner);
            programs.push(Vec::new());
            dependencies.push(original_dependencies.clone());
            transfer_dependencies.push(owner);
        }
        dependencies[process] = transfer_dependencies;
        let all_prepared = opened.len() == stream_steps.len();
        for mut action in actions {
            let Some(&owner) = owners.get(&action.step) else {
                programs[process].push(action);
                continue;
            };
            action.stream_owner = Some(owner);
            match &action.operation {
                NamespaceOperation::MutationOrigin { .. }
                | NamespaceOperation::StreamOpen { .. } => {
                    action.preparation = true;
                    programs[owner].push(action);
                }
                NamespaceOperation::StreamAttach {
                    path, incarnation, ..
                } => {
                    programs[process].push(NamespaceAction {
                        step: action.step,
                        stream_owner: Some(owner),
                        preparation: false,
                        operation: NamespaceOperation::StreamTransfer {
                            path: path.clone(),
                            incarnation: *incarnation,
                        },
                        role: action.role,
                        result: action.result,
                        denied: false,
                        publication_finished: true,
                    });
                    action.preparation = true;
                    programs[owner].push(action);
                }
                NamespaceOperation::StreamStop {
                    path,
                    incarnation: Some(incarnation),
                    source,
                    replace,
                    chunks,
                    gated,
                    roundtrip,
                    ..
                } => {
                    let result = opened.get(&action.step).copied().ok_or_else(|| {
                        evidence_failure(
                            FailureClass::MissingEvidence,
                            Some(action.step),
                            "stopped route endpoint omitted its preparation".to_owned(),
                        )
                    })?;
                    if !programs[owner].iter().any(|prepared| {
                        matches!(prepared.operation, NamespaceOperation::StreamAttach { .. })
                    }) {
                        for operation in [
                            NamespaceOperation::StreamOpen {
                                path: path.clone(),
                                incarnation: Some(*incarnation),
                                source: *source,
                                replace: *replace,
                                chunks: *chunks,
                                gated: *gated,
                            },
                            NamespaceOperation::StreamAttach {
                                path: path.clone(),
                                incarnation: *incarnation,
                                roundtrip: *roundtrip,
                            },
                        ] {
                            programs[owner].push(NamespaceAction {
                                step: action.step,
                                stream_owner: Some(owner),
                                preparation: true,
                                operation,
                                role: action.role,
                                result: Some(result),
                                denied: action.denied,
                                publication_finished: true,
                            });
                        }
                    }
                    if all_prepared {
                        if action.step == next_terminal {
                            programs[process].push(NamespaceAction {
                                step: action.step,
                                stream_owner: Some(owner),
                                preparation: false,
                                operation: NamespaceOperation::StreamTransfer {
                                    path: path.clone(),
                                    incarnation: *incarnation,
                                },
                                role: action.role,
                                result: Some(result),
                                denied: false,
                                publication_finished: true,
                            });
                        }
                        programs[process].push(action);
                    } else {
                        action.preparation = true;
                        programs[owner].push(action);
                    }
                }
                NamespaceOperation::StreamStop { .. } => {
                    action.preparation = true;
                    programs[owner].push(action);
                }
                _ => programs[process].push(action),
            }
        }
        if programs[process].is_empty() {
            // Empty scheduler slots bypass dependencies. Keep a completion
            // fence even when cancellation left no transfer to execute.
            programs[process].push(NamespaceAction {
                step: first_step,
                stream_owner: None,
                preparation: false,
                operation: NamespaceOperation::Ignore,
                role: (&program.actions[first_step].operation).into(),
                result: None,
                denied: false,
                publication_finished: true,
            });
        }
    }
    Ok(())
}

fn ready_action<'a>(
    state: &NamespaceHistory<'_>,
    programs: &'a [Vec<NamespaceAction<'a>>],
    dependencies: &[Vec<usize>],
    process: usize,
) -> Option<&'a NamespaceAction<'a>> {
    if dependencies[process]
        .iter()
        .any(|dependency| state.positions[*dependency] < programs[*dependency].len())
    {
        return None;
    }
    programs[process].get(state.positions[process])
}

fn operation_paths<'a>(operation: &'a NamespaceOperation<'_>) -> impl Iterator<Item = &'a str> {
    let first = operation.path();
    let second = match operation {
        NamespaceOperation::Rename { destination, .. } => Some(destination.as_str()),
        _ => None,
    };
    first.into_iter().chain(second)
}

fn namespace_action_key(
    action: &NamespaceAction<'_>,
    revision_hints: &BTreeMap<String, u64>,
) -> u64 {
    action
        .result
        .and_then(|result| result.revision.or(result.incarnation))
        .or_else(|| match action.operation {
            NamespaceOperation::StreamOpen { incarnation, .. } => incarnation,
            _ => None,
        })
        .or_else(|| {
            operation_paths(&action.operation).find_map(|path| revision_hints.get(path).copied())
        })
        .unwrap_or(u64::MAX)
}

fn namespace_commit_key(
    action: &NamespaceAction<'_>,
    revision_hints: &BTreeMap<String, u64>,
) -> Option<u64> {
    if action.denied {
        return None;
    }
    let result = action.result;
    let success = result.is_some_and(|result| result.outcome == "ok" && result.errno.is_none());
    let path_hint = || {
        action
            .operation
            .path()
            .and_then(|path| revision_hints.get(path).copied())
    };
    match action.operation {
        NamespaceOperation::StreamOpen {
            incarnation: Some(incarnation),
            ..
        } => Some(incarnation),
        NamespaceOperation::Publish { commit: true, .. }
        | NamespaceOperation::Rename { .. }
        | NamespaceOperation::Unlink { .. }
            if success =>
        {
            result.and_then(|result| result.revision).or_else(path_hint)
        }
        NamespaceOperation::GatePublish { .. } => path_hint(),
        _ => None,
    }
}
pub(crate) fn stream_record_action_matches(action: &ActionOp, record: &ActionObservation) -> bool {
    record.action == action.class().as_str()
        || (matches!(action, ActionOp::StreamRoundTrip { .. })
            && record.action == "stream_read"
            && is_barrier_record(record)
            && matches!(record.barrier, Some(BarrierObservation::StreamFrame { .. })))
}

fn stream_incarnation_evidence(
    execution: &ExecutionObservation,
    step: usize,
    process: &str,
    action: &ActionOp,
) -> Result<Option<u64>, OracleViolation> {
    let fail = |class, detail| {
        OracleViolation::new("namespace_model_evidence", detail)
            .classified(class, CausalRole::Action(action.into()))
    };
    let records = || {
        execution
            .results
            .iter()
            .filter(move |result| result.step == step)
    };
    let result = records().find(|record| !is_barrier_record(record));
    let observed = result.and_then(|result| result.incarnation);
    let opened = records().find_map(|record| match record.barrier.as_ref() {
        Some(BarrierObservation::StreamOpened { incarnation }) => Some(*incarnation),
        _ => None,
    });
    // Only typed open failures can lack attachment identity. The transition
    // model still requires the exact collision/displacement state; a generic
    // StreamError is not itself proof of any legal transition.
    let Some(incarnation) = observed.or(opened) else {
        if result.is_some_and(|result| {
            [
                libc::EACCES,
                libc::ENXIO,
                libc::EEXIST,
                libc::ESTALE,
                libc::EIO,
            ]
            .into_iter()
            .any(|errno| namespace_binding_error(result, errno, false))
                && result.transfer.is_none()
        }) && !records().any(|record| {
            matches!(
                &record.barrier,
                Some(
                    BarrierObservation::StreamFirstFrame { .. }
                        | BarrierObservation::StreamFrame { .. }
                        | BarrierObservation::StreamEof { .. }
                )
            )
        }) {
            return Ok(None);
        }
        return Err(fail(
            FailureClass::MissingIncarnation,
            format!("{process} step {step}: stream action omitted its incarnation identity"),
        ));
    };
    if incarnation == 0 {
        return Err(fail(
            FailureClass::InvalidIncarnation,
            format!("{process} step {step}: zero stream incarnation"),
        ));
    }
    let mut attached = false;
    let mut first_frame = false;
    let mut eof = false;
    let mut terminal = false;
    let mut sent = 0_u64;
    let mut received = 0_u64;
    let reader = matches!(
        action,
        ActionOp::StreamRoundTrip { .. }
            | ActionOp::StreamRead { .. }
            | ActionOp::StreamReadWithRetry { .. }
            | ActionOp::GatedStreamRead { .. }
            | ActionOp::StreamReadInto { .. }
    );
    let bound = match action {
        ActionOp::StreamWrite { chunks, .. }
        | ActionOp::StreamRoundTrip { chunks, .. }
        | ActionOp::GatedStreamWrite { frames: chunks, .. } => chunks.iter().map(Vec::len).sum(),
        ActionOp::StreamRead { expected, .. }
        | ActionOp::StreamReadWithRetry { expected, .. }
        | ActionOp::GatedStreamRead { expected, .. }
        | ActionOp::StreamReadInto { expected, .. } => expected.len(),
        _ => 0,
    };
    for record in records() {
        let barrier_incarnation = match &record.barrier {
            Some(BarrierObservation::StreamOpened { incarnation })
            | Some(BarrierObservation::StreamFirstFrame { incarnation })
            | Some(BarrierObservation::StreamFrame { incarnation, .. })
            | Some(BarrierObservation::StreamEof { incarnation }) => Some(*incarnation),
            _ => None,
        };
        if barrier_incarnation.is_some_and(|identity| identity != incarnation)
            || record
                .incarnation
                .is_some_and(|identity| identity != incarnation)
        {
            return Err(fail(
                FailureClass::ConflictingIncarnation,
                format!(
                    "{process} step {step}: stream milestone disagrees with the action incarnation"
                ),
            ));
        }
        let stream_lifecycle = matches!(
            &record.barrier,
            Some(
                BarrierObservation::StreamOpened { .. }
                    | BarrierObservation::StreamFirstFrame { .. }
                    | BarrierObservation::StreamFrame { .. }
                    | BarrierObservation::StreamEof { .. }
                    | BarrierObservation::ReleaseObserved { .. }
            )
        );
        if is_barrier_record(record) && !stream_lifecycle {
            continue;
        }
        if !is_barrier_record(record) {
            if stream_lifecycle {
                return Err(fail(
                    FailureClass::InvalidEvidence,
                    format!("{process} step {step}: terminal record contains stream milestone"),
                ));
            }
            terminal = true;
            continue;
        }
        if record.process != process
            || record.path != action.path()
            || !stream_record_action_matches(action, record)
        {
            return Err(fail(
                FailureClass::InvalidEvidence,
                format!("{process} step {step}: stream milestone belongs to another action"),
            ));
        }
        let legal = !terminal
            && match &record.barrier {
                Some(BarrierObservation::StreamOpened { .. }) => {
                    let legal = !attached && !eof;
                    attached = true;
                    legal
                }
                Some(BarrierObservation::StreamFrame { index, length, .. }) => {
                    let next = if record.action == "stream_write" {
                        &mut sent
                    } else {
                        &mut received
                    };
                    if *index != *next {
                        return Err(fail(
                            FailureClass::FrameOrderMismatch,
                            format!(
                                "{process} step {step}: missing, repeated, or reordered logical frame index"
                            ),
                        ));
                    }
                    *next = next.checked_add(1).ok_or_else(|| {
                        fail(
                            FailureClass::FrameOrderMismatch,
                            format!("{process} step {step}: logical frame index overflow"),
                        )
                    })?;
                    if *length > bound as u64 {
                        return Err(fail(
                            FailureClass::FrameContentMismatch,
                            format!("{process} step {step}: frame exceeds its IR payload bound"),
                        ));
                    }
                    attached && !eof
                }
                Some(BarrierObservation::StreamFirstFrame { .. }) => {
                    let legal = attached && !first_frame && !eof && received == 1;
                    first_frame = true;
                    legal
                }
                Some(BarrierObservation::StreamEof { .. }) => {
                    let legal = attached && !eof && reader;
                    eof = true;
                    legal
                }
                Some(BarrierObservation::ReleaseObserved { .. }) => attached && !eof,
                _ => true,
            };
        if !legal {
            return Err(fail(
                FailureClass::MilestoneOrder,
                format!("{process} step {step}: stream milestones violate local causal order"),
            ));
        }
    }
    if !attached {
        return Err(fail(
            FailureClass::MissingEvidence,
            format!("{process} step {step}: stream omitted its opened milestone"),
        ));
    }
    if reader
        && result.is_some_and(|result| {
            result.outcome == "ok"
                || result
                    .transfer
                    .as_ref()
                    .is_some_and(|transfer| transfer.complete)
        })
        && !eof
    {
        return Err(fail(
            FailureClass::MissingStreamEof,
            format!("{process} step {step}: completed reader omitted clean EOF"),
        ));
    }
    if let Some(result) = result
        && result.outcome != "ok"
        && (first_frame || eof || received != 0 || (!reader && sent != 0))
        && result
            .transfer
            .as_ref()
            .is_none_or(|transfer| eof && !transfer.complete)
    {
        return Err(fail(
            FailureClass::MissingTransfer,
            format!("{process} step {step}: stream progress omitted transferred-prefix evidence"),
        ));
    }
    Ok(Some(incarnation))
}

fn namespace_history_complete(
    state: &NamespaceHistory<'_>,
    programs: &[Vec<NamespaceAction<'_>>],
) -> bool {
    state
        .positions
        .iter()
        .zip(programs)
        .all(|(position, actions)| *position == actions.len())
        && state.publications.is_empty()
        && state.publication_lookups.is_empty()
        && state.reservations.is_empty()
        && state.open_streams.is_empty()
}

fn stream_initial_frame(chunks: Option<&[Vec<u8>]>, _gated: bool) -> bool {
    chunks.is_some_and(|chunks| !chunks.is_empty())
}

// A first frame can be delivered before the source's aggregate action has
// completed. This matters when a consuming process is stopped mid-stream.
fn namespace_first_frame<'a>(
    state: &NamespaceHistory<'a>,
    path: &str,
) -> Option<NamespaceHistory<'a>> {
    let revision = state.active_streams.get(path)?;
    let session = state.sessions.get(revision)?;
    if !session.matched()
        || !session.source_attached
        || session.first_frame
        || session.aborted
        || !session.first_frame_permitted
    {
        return None;
    }
    let mut next = state.clone();
    next.sessions.get_mut(revision)?.first_frame = true;
    Some(next)
}

// A stop with no action record can occur before invocation or while the
// namespace open is pending. These are distinct legal branches, not a
// fabricated successful action. Any attached milestone constrains the same
// commit slot as every other participant before the stop can complete.
fn namespace_interrupted_open<'a>(
    state: &NamespaceHistory<'a>,
    process: usize,
    action: &NamespaceAction<'a>,
) -> Option<NamespaceHistory<'a>> {
    let owner = action.stream_owner.unwrap_or(process);
    let NamespaceOperation::StreamStop {
        path,
        incarnation,
        source,
        replace,
        roundtrip,
        chunks,
        gated,
        eof,
        ..
    } = &action.operation
    else {
        return None;
    };
    if action.denied
        || state.open_streams.contains_key(&owner)
        || state.reservations.contains_key(path)
    {
        return None;
    }
    if !(path.starts_with("/cases/") || path.starts_with("/runs/")) {
        return None;
    }
    let pending = state.active_streams.get(path).and_then(|revision| {
        state
            .sessions
            .get(revision)
            .map(|session| (*revision, session))
    });
    let compatible = pending.is_some_and(|(_, session)| {
        !session.matched()
            && if *source {
                session.source.is_none()
            } else {
                session.sink.is_none()
            }
    });
    if (pending.is_some() && !compatible && !replace)
        || (state.entries.contains_key(path) && !replace)
        || (compatible && *roundtrip)
    {
        return None;
    }
    let mut next = state.clone();
    let revision = if compatible {
        let (revision, _) = pending?;
        if let Some(incarnation) = incarnation
            && !next.constrain_revision(revision, *incarnation)
        {
            return None;
        }
        let session = next.sessions.get_mut(&revision)?;
        if *source {
            session.source = Some(owner);
            session.chunks = *chunks;
            session.source_attached = incarnation.is_some() && !action.preparation;
            session.first_frame_permitted = stream_initial_frame(*chunks, *gated);
        } else {
            session.sink = Some(owner);
        }
        revision
    } else {
        let revision = next.commit_revision(*incarnation)?;
        next.entries.remove(path);
        next.streams.insert(path.clone(), revision);
        next.active_streams.insert(path.clone(), revision);
        next.sessions.insert(
            revision,
            StreamSession {
                path: path.clone(),
                source: source.then_some(owner),
                sink: (!source || *roundtrip).then_some(owner),
                chunks: *chunks,
                source_attached: *source && incarnation.is_some() && !action.preparation,
                first_frame_permitted: stream_initial_frame(*chunks, *gated),
                first_frame: *roundtrip && *eof && chunks.is_some_and(|chunks| !chunks.is_empty()),
                eof: *roundtrip && *eof,
                aborted: false,
            },
        );
        revision
    };
    next.open_streams.insert(owner, revision);
    Some(next)
}

// Endpoint teardown sends an asynchronous namespace close. It can become
// visible before or after the action result, and consumes no revision.
fn namespace_quiescence<'a>(
    state: &NamespaceHistory<'a>,
    path: &str,
) -> Option<NamespaceHistory<'a>> {
    let revision = state.active_streams.get(path)?;
    if !state
        .sessions
        .get(revision)
        .is_some_and(|session| session.eof || session.aborted)
    {
        return None;
    }
    let mut next = state.clone();
    next.active_streams.remove(path);
    Some(next)
}

fn namespace_transition<'a>(
    state: &NamespaceHistory<'a>,
    process: usize,
    action: &NamespaceAction<'a>,
) -> Option<NamespaceHistory<'a>> {
    let owner = action.stream_owner.unwrap_or(process);
    if let NamespaceOperation::StreamStop {
        path,
        incarnation,
        first_frame,
        eof,
        frame_sources,
        stop_gates,
        ..
    } = &action.operation
    {
        if !stop_gates.is_empty()
            && !stop_gates.iter().any(|(path, incarnation, marker)| {
                marker
                    .as_ref()
                    .is_none_or(|marker| state.entries.contains_key(marker))
                    && state.sessions.iter().any(|(revision, session)| {
                        session.path == *path
                            && session.matched()
                            && session.first_frame
                            && !session.aborted
                            && state.revisions[*revision] == Some(*incarnation)
                    })
            })
        {
            return None;
        }
        let mut next = Cow::Borrowed(state);
        if let Some(revision) = state.open_streams.get(&owner) {
            let session = state.sessions.get(revision)?;
            if session.path != *path
                || incarnation.is_some_and(|identity| {
                    state.revisions[*revision] != Some(identity) || !session.matched()
                })
                || (*first_frame && !session.first_frame)
                || (*eof && !session.eof)
                || (incarnation.is_some()
                    && !frame_sources.iter().any(|chunks| {
                        session
                            .chunks
                            .is_some_and(|source| std::ptr::eq(*chunks, source))
                    }))
            {
                return None;
            }
            next.to_mut().sessions.get_mut(revision)?.aborted = true;
            next.to_mut().open_streams.remove(&owner);
        } else if incarnation.is_some() || *first_frame || *eof {
            return None;
        }
        next.to_mut().positions[process] += 1;
        return Some(next.into_owned());
    }
    if let NamespaceOperation::MutationOrigin { path, revision } = &action.operation {
        if action.denied || state.reservations.contains_key(path) {
            return None;
        }
        let slot = state
            .entries
            .get(path)
            .map(|binding| binding.revision)
            .or_else(|| state.streams.get(path).copied())?;
        let mut next = state.clone();
        if !next.constrain_revision(slot, *revision) {
            return None;
        }
        next.positions[process] += 1;
        return Some(next);
    }
    if matches!(action.operation, NamespaceOperation::Ignore) {
        let mut next = state.clone();
        next.positions[process] += 1;
        return Some(next);
    }
    let result = action.result?;
    let mut next = Cow::Borrowed(state);
    if action.denied {
        if !namespace_errno(result, libc::EACCES)
            || matches!(
                action.operation,
                NamespaceOperation::StreamOpen {
                    incarnation: Some(_),
                    ..
                }
            )
        {
            return None;
        }
        next.to_mut().positions[process] += 1;
        return Some(next.into_owned());
    }
    let success = result.outcome == "ok" && result.errno.is_none();
    let mut complete = true;
    match &action.operation {
        NamespaceOperation::Publish {
            path,
            bytes,
            commit,
            exclusive,
            require_existing,
        } => {
            if state.publications.contains(&process) {
                if !action.publication_finished
                    || state
                        .reservations
                        .get(path)
                        .is_some_and(|owner| *owner != process)
                {
                    return None;
                }
                next.to_mut().publications.remove(&process);
                next.to_mut().reservations.remove(path);
                if *commit {
                    let revision = next.to_mut().commit_revision(result.revision)?;
                    next.to_mut().streams.remove(path);
                    next.to_mut().active_streams.remove(path);
                    next.to_mut().entries.insert(
                        path.clone(),
                        NamespaceBinding {
                            bytes: *bytes,
                            revision,
                        },
                    );
                }
            } else if *exclusive && state.publication_lookups.contains(&process) {
                if state.entries.contains_key(path)
                    || state.streams.contains_key(path)
                    || state.reservations.contains_key(path)
                {
                    if !namespace_errno(result, libc::EEXIST) {
                        return None;
                    }
                } else {
                    if !action.publication_finished {
                        return None;
                    }
                    next.to_mut().publications.insert(process);
                    next.to_mut().reservations.insert(path.clone(), process);
                    complete = false;
                }
                next.to_mut().publication_lookups.remove(&process);
            } else if (*exclusive && state.entries.contains_key(path))
                || state.reservations.contains_key(path)
            {
                if !namespace_binding_error(result, libc::EEXIST, *exclusive || *require_existing) {
                    return None;
                }
            } else if state.streams.contains_key(path) {
                // Staged-blob opens reject a stream seen by their lookup.
                // Only an already-open publication can replace one.
                if !namespace_binding_error(result, libc::ENXIO, *exclusive || *require_existing) {
                    return None;
                }
            } else if *require_existing && !state.entries.contains_key(path) {
                if !namespace_errno(result, libc::ENOENT) {
                    return None;
                }
            } else if *exclusive {
                // A stream may appear between lookup and reservation:
                // EEXIST at reservation is distinct from ENXIO at lookup.
                next.to_mut().publication_lookups.insert(process);
                complete = false;
            } else {
                if !action.publication_finished {
                    return None;
                }
                next.to_mut().publications.insert(process);
                complete = false;
            }
        }
        NamespaceOperation::Read {
            path,
            expected,
            offset,
            whole,
        } => {
            if state.reservations.contains_key(path) {
                if !namespace_binding_error(result, libc::EEXIST, !*whole) {
                    return None;
                }
            } else if let Some(binding) = state.entries.get(path) {
                if !action.publication_finished {
                    return None;
                }
                let bytes = if *whole {
                    binding.bytes
                } else {
                    let end = offset.checked_add(expected.len())?;
                    binding.bytes.get(*offset..end)?
                };
                if bytes != *expected {
                    return None;
                }
                if let Some(revision) = result.revision
                    && !next.to_mut().constrain_revision(binding.revision, revision)
                {
                    return None;
                }
            } else if state.streams.contains_key(path) {
                if !namespace_binding_error(result, libc::ENXIO, !*whole) {
                    return None;
                }
            } else if !namespace_errno(result, libc::ENOENT) {
                return None;
            }
        }
        NamespaceOperation::Lookup { path } => {
            if state.reservations.contains_key(path) {
                if !namespace_errno(result, libc::EEXIST) {
                    return None;
                }
            } else if let Some(binding) = state.entries.get(path) {
                if !success
                    || result.kind.as_deref() != Some("blob")
                    || result.active == Some(true)
                    || !next
                        .to_mut()
                        .constrain_revision(binding.revision, result.revision?)
                {
                    return None;
                }
            } else if let Some(revision) = state.streams.get(path) {
                if !success
                    || result.kind.as_deref() != Some("stream")
                    || result.active != Some(state.active_streams.contains_key(path))
                    || !next
                        .to_mut()
                        .constrain_revision(*revision, result.revision?)
                {
                    return None;
                }
            } else if !namespace_errno(result, libc::ENOENT) {
                return None;
            }
        }
        NamespaceOperation::Rename {
            source,
            destination,
            replace,
        } => {
            if !state.entries.contains_key(source) && !state.streams.contains_key(source) {
                if !namespace_errno(result, libc::ENOENT) {
                    return None;
                }
            } else if state.reservations.contains_key(source)
                || state.reservations.contains_key(destination)
            {
                if !namespace_errno(result, libc::EEXIST) {
                    return None;
                }
            } else if state.active_streams.contains_key(source)
                || state.active_streams.contains_key(destination)
            {
                if !namespace_errno(result, libc::EBUSY) {
                    return None;
                }
            } else if source != destination
                && (state.entries.contains_key(destination)
                    || state.streams.contains_key(destination))
                && !replace
            {
                if !namespace_errno(result, libc::EEXIST) {
                    return None;
                }
            } else {
                if !success {
                    return None;
                }
                let binding = next.to_mut().entries.remove(source);
                let stream = next.to_mut().streams.remove(source);
                let revision = next.to_mut().commit_revision(result.revision)?;
                next.to_mut().entries.remove(destination);
                next.to_mut().streams.remove(destination);
                if let Some(mut binding) = binding {
                    binding.revision = revision;
                    next.to_mut().entries.insert(destination.clone(), binding);
                } else if stream.is_some() {
                    next.to_mut().streams.insert(destination.clone(), revision);
                }
            }
        }
        NamespaceOperation::Unlink { path } => {
            if state.active_streams.contains_key(path) {
                if !namespace_errno(result, libc::ENXIO) {
                    return None;
                }
            } else if state.reservations.contains_key(path) {
                if !namespace_errno(result, libc::EEXIST) {
                    return None;
                }
            } else if state.entries.contains_key(path) || state.streams.contains_key(path) {
                if !success {
                    return None;
                }
                next.to_mut().entries.remove(path);
                next.to_mut().streams.remove(path);
                next.to_mut().commit_revision(result.revision)?;
            } else if !namespace_errno(result, libc::ENOENT) {
                return None;
            }
        }
        NamespaceOperation::StreamOpen {
            path,
            incarnation,
            source,
            replace,
            chunks,
            gated,
        } => {
            if state.open_streams.contains_key(&owner) {
                return None;
            }
            let pending = state.active_streams.get(path).and_then(|revision| {
                state
                    .sessions
                    .get(revision)
                    .map(|session| (*revision, session))
            });
            let compatible = pending.is_some_and(|(_, session)| {
                !session.matched()
                    && if *source {
                        session.source.is_none()
                    } else {
                        session.sink.is_none()
                    }
            });
            let error = if state.reservations.contains_key(path) {
                Some(libc::EEXIST)
            } else if pending.is_some() && !compatible && !replace {
                // DuplicateStreamRole maps to SessionFailed/EIO, not EEXIST.
                Some(libc::EIO)
            } else if state.entries.contains_key(path) && !replace {
                Some(libc::ENXIO)
            } else {
                None
            };
            if let Some(errno) = error {
                if incarnation.is_some() || !namespace_binding_error(result, errno, false) {
                    return None;
                }
            } else {
                if incarnation.is_none() && !namespace_errno(result, libc::ESTALE) {
                    return None;
                }
                if compatible {
                    let (revision, _) = pending?;
                    if let Some(incarnation) = incarnation
                        && !next.to_mut().constrain_revision(revision, *incarnation)
                    {
                        return None;
                    }
                    let session = next.to_mut().sessions.get_mut(&revision)?;
                    if *source {
                        session.first_frame_permitted = stream_initial_frame(*chunks, *gated);
                        session.source = Some(owner);
                        session.chunks = *chunks;
                    } else {
                        session.sink = Some(owner);
                    }
                    next.to_mut().open_streams.insert(owner, revision);
                } else {
                    // Pending creation commits a fresh revision. An open
                    // displaced before attachment has no handle identity;
                    // its symbolic slot is constrained by later evidence,
                    // never filled with a guessed incarnation.
                    let revision = next.to_mut().commit_revision(*incarnation)?;
                    next.to_mut().entries.remove(path);
                    next.to_mut().streams.insert(path.clone(), revision);
                    next.to_mut().active_streams.insert(path.clone(), revision);
                    next.to_mut().open_streams.insert(owner, revision);
                    next.to_mut().sessions.insert(
                        revision,
                        StreamSession {
                            path: path.clone(),
                            source: source.then_some(owner),
                            sink: (!source).then_some(owner),
                            chunks: *chunks,
                            source_attached: false,
                            first_frame_permitted: stream_initial_frame(*chunks, *gated),
                            first_frame: false,
                            eof: false,
                            aborted: false,
                        },
                    );
                }
            }
        }
        NamespaceOperation::StreamAttach {
            path,
            incarnation,
            roundtrip,
        } => {
            let Some(&revision) = state.open_streams.get(&owner) else {
                return None;
            };
            let Some(session) = state.sessions.get(&revision) else {
                return None;
            };
            if session.path != *path {
                return None;
            }
            if state.revisions[revision] != Some(*incarnation) {
                return None;
            }
            if *roundtrip {
                if session.source != Some(owner)
                    || session.sink.is_some()
                    || state.active_streams.get(path) != Some(&revision)
                {
                    return None;
                }
            } else if !session.matched() {
                return None;
            }
            let session = next.to_mut().sessions.get_mut(&revision)?;
            if *roundtrip {
                session.sink = Some(owner);
            }
            if session.source == Some(owner) && !action.preparation {
                session.source_attached = true;
            }
        }
        NamespaceOperation::StreamTransfer { path, incarnation } => {
            let revision = *state.open_streams.get(&owner)?;
            let session = state.sessions.get(&revision)?;
            if session.path != *path
                || state.revisions[revision] != Some(*incarnation)
                || !session.matched()
                || (session.source != Some(owner) && session.sink != Some(owner))
            {
                return None;
            }
            if session.source == Some(owner) {
                next.to_mut().sessions.get_mut(&revision)?.source_attached = true;
            }
        }
        NamespaceOperation::StreamFirstFrame {
            path,
            incarnation,
            source,
        } => {
            let revision = *state.open_streams.get(&owner)?;
            let session = state.sessions.get(&revision)?;
            if state.active_streams.get(path) != Some(&revision)
                || !session.matched()
                || state.revisions[revision] != Some(*incarnation)
            {
                return None;
            }
            if *source {
                let session = next.to_mut().sessions.get_mut(&revision)?;
                session.first_frame |= session.first_frame_permitted;
            } else if !session.first_frame {
                return None;
            }
        }
        NamespaceOperation::StreamComplete {
            path,
            incarnation,
            source,
            expected,
            frame_sources,
            frames_observed,
        } => {
            let revision = *state.open_streams.get(&owner)?;
            let session = state.sessions.get(&revision)?;
            if !session.matched()
                || session.path != *path
                || state.revisions[revision] != Some(*incarnation)
                || !frame_sources.iter().any(|chunks| {
                    session
                        .chunks
                        .is_some_and(|source| std::ptr::eq(*chunks, source))
                })
                || (!source && *frames_observed && !session.first_frame)
                || if *source {
                    session.source != Some(owner)
                } else {
                    session.sink != Some(owner)
                }
            {
                return None;
            }
            if !*source && let Some(transfer) = &result.transfer {
                let (length, expected_digest) = payload_summary(
                    session.chunks?.iter().map(Vec::as_slice),
                    Some(transfer.length),
                )?;
                if transfer.length > length
                    || transfer.digest != expected_digest
                    || (transfer.complete && transfer.length != length)
                    || (transfer.complete && !session.eof)
                {
                    return None;
                }
            }
            let current = state.streams.get(path) == Some(&revision);
            if !current {
                if !namespace_errno(result, libc::ESTALE) {
                    return None;
                }
            } else if session.aborted {
                if !namespace_binding_error(result, libc::ECONNRESET, false)
                    && !namespace_binding_error(result, libc::EPIPE, false)
                {
                    return None;
                }
            } else if !success {
                return None;
            } else if *source {
                let session = next.to_mut().sessions.get_mut(&revision)?;
                session.first_frame = !session.chunks?.is_empty();
                session.eof = true;
            } else {
                if !session.eof {
                    return None;
                }
                if !session
                    .chunks?
                    .iter()
                    .flat_map(|chunk| chunk.iter())
                    .eq((*expected)?.iter())
                {
                    return None;
                }
            }
            next.to_mut().open_streams.remove(&owner);
        }
        NamespaceOperation::StreamPendingFailure { path } => {
            let revision = *state.open_streams.get(&owner)?;
            let session = state.sessions.get(&revision)?;
            if session.path != *path
                || session.matched()
                || state.active_streams.get(path) == Some(&revision)
                || !namespace_errno(result, libc::ESTALE)
            {
                return None;
            }
            next.to_mut().open_streams.remove(&owner);
        }
        NamespaceOperation::StreamProbe { path } => {
            let revision = *state.streams.get(path)?;
            if !success
                || result.kind.as_deref() != Some("stream")
                || result.active != Some(false)
                || state.active_streams.contains_key(path)
                || !next.to_mut().constrain_revision(revision, result.revision?)
            {
                return None;
            }
        }
        NamespaceOperation::GatePublish { path } => {
            if state.reservations.contains_key(path) {
                return None;
            }
            if state.publications.contains(&process) {
                next.to_mut().publications.remove(&process);
                let revision = next.to_mut().commit_revision(None)?;
                next.to_mut().streams.remove(path);
                next.to_mut().active_streams.remove(path);
                next.to_mut().entries.insert(
                    path.clone(),
                    NamespaceBinding {
                        bytes: &[],
                        revision,
                    },
                );
            } else {
                if state.streams.contains_key(path) {
                    return None;
                }
                next.to_mut().publications.insert(process);
                complete = false;
            }
        }
        NamespaceOperation::GateLookup { path } => {
            if state.reservations.contains_key(path)
                || (!state.entries.contains_key(path) && !state.streams.contains_key(path))
            {
                return None;
            }
            let revision = state.open_streams.get(&owner)?;
            let session = next.to_mut().sessions.get_mut(revision)?;
            session.first_frame_permitted = stream_initial_frame(session.chunks, false);
        }
        NamespaceOperation::StreamStop { .. } | NamespaceOperation::MutationOrigin { .. } => {
            return None;
        }
        NamespaceOperation::Ignore => {}
    }
    if complete {
        next.to_mut().positions[process] += 1;
    }
    Some(next.into_owned())
}

fn violation<T>(invariant: &'static str, detail: impl Into<String>) -> Result<T, OracleViolation> {
    Err(OracleViolation::new(invariant, detail))
}

pub(crate) fn process_failure_is_expected(case: &BehaviorCase, process: &ProcessProgram) -> bool {
    match &case.failure {
        FailureInjection::None | FailureInjection::SlowProcess { .. } => false,
        FailureInjection::StopProcess {
            process: target, ..
        }
        | FailureInjection::LaunchFailure {
            process: target, ..
        } => target == &process.id,
    }
}

#[cfg(test)]
mod namespace_tests {
    use super::*;
    use crate::ir::{
        AccessSpec, Action, DataEdge, DataRoute, DescriptorObservation, DescriptorTerminalResult,
        DescriptorWriteMethod, PythonException, TransferObservation,
    };

    fn program(id: &str, actions: Vec<Action>, dependencies: &[&str]) -> ProcessProgram {
        ProcessProgram {
            id: id.to_owned(),
            logical_node_id: 1,
            access: AccessSpec::unrestricted(id),
            depends_on: dependencies
                .iter()
                .map(|dependency| (*dependency).to_owned())
                .collect(),
            actions,
        }
    }

    fn case(processes: Vec<ProcessProgram>) -> BehaviorCase {
        BehaviorCase {
            schema_version: crate::ir::CASE_SCHEMA_VERSION,
            generator_version: crate::ir::GENERATOR_VERSION,
            id: "namespace-model".to_owned(),
            seed: 1,
            live_nodes: BTreeSet::from([1, 2]),
            topology: Default::default(),
            scenarios: Default::default(),
            routes: Vec::new(),
            read_only_fixture_paths: BTreeSet::new(),
            resource_bounds: Default::default(),
            processes,
            failure: FailureInjection::None,
        }
    }

    fn publication(path: &str, bytes: &[u8]) -> ActionOp {
        ActionOp::PublishBlob {
            path: path.to_owned(),
            bytes: bytes.to_vec(),
        }
    }

    fn exclusive(path: &str, bytes: &[u8]) -> ActionOp {
        ActionOp::DescriptorWrite {
            path: path.to_owned(),
            flags: libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC,
            length: Some(bytes.len() as u64),
            bytes: bytes.to_vec(),
            method: DescriptorWriteMethod::Write,
            finish: DescriptorFinish::Close,
        }
    }

    fn observe(case: &BehaviorCase) -> CaseObservation {
        CaseObservation {
            case_id: case.id.clone(),
            executions: case
                .processes
                .iter()
                .map(|program| ExecutionObservation {
                    process: program.id.clone(),
                    request_id: case.execution_request_id(program),
                    logical_node_id: program.logical_node_id,
                    lifecycle: ["process_started", "context_ready", "user_result", "exited"]
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                    results: program
                        .actions
                        .iter()
                        .enumerate()
                        .map(|(step, action)| {
                            let bytes = match &action.operation {
                                ActionOp::PublishBlob { bytes, .. }
                                | ActionOp::DescriptorWrite { bytes, .. }
                                | ActionOp::ReadBlob {
                                    expected: bytes, ..
                                } => Some(bytes),
                                _ => None,
                            };
                            let errno = match action.expected {
                                ExpectedOutcome::Error(errno) => Some(errno),
                                _ => None,
                            };
                            ActionObservation {
                                process: program.id.clone(),
                                step,
                                action: action.operation.class().as_str().to_owned(),
                                path: action.operation.path().to_owned(),
                                outcome: if errno.is_some()
                                    || matches!(action.expected, ExpectedOutcome::Exception(_))
                                {
                                    "expected_error"
                                } else {
                                    "ok"
                                }
                                .to_owned(),
                                length: bytes.map(Vec::len),
                                digest: bytes.map(|bytes| digest(bytes)),
                                kind: match &action.operation {
                                    ActionOp::Lookup { expected_kind, .. } => {
                                        Some(expected_kind.clone())
                                    }
                                    _ => None,
                                },
                                revision: matches!(
                                    action.operation,
                                    ActionOp::Rename { .. }
                                        | ActionOp::Unlink { .. }
                                        | ActionOp::Lookup { .. }
                                )
                                .then_some(10 + step as u64),
                                active: None,
                                errno,
                                error_type: match action.expected {
                                    ExpectedOutcome::Exception(exception) => {
                                        Some(exception.as_str().to_owned())
                                    }
                                    ExpectedOutcome::Error(_) => Some("OSError".to_owned()),
                                    _ => None,
                                },
                                error: None,
                                descriptor: match &action.operation {
                                    ActionOp::DescriptorWrite { method, finish, .. }
                                        if errno.is_none() =>
                                    {
                                        Some(DescriptorObservation::Write {
                                            method: *method,
                                            finish: *finish,
                                            terminal_results: vec![DescriptorTerminalResult::Ok],
                                            dropped: false,
                                            reservation_released: false,
                                        })
                                    }
                                    _ => None,
                                },
                                barrier: None,
                                incarnation: None,
                                token: None,
                                lap: None,
                                transfer: None,
                            }
                        })
                        .collect(),
                    terminal: true,
                    exit_success: true,
                    exit_status: None,
                    stdout: String::new(),
                    stderr: String::new(),
                })
                .collect(),
        }
    }

    #[test]
    fn successful_actions_require_process_local_order() {
        let case = case(vec![program(
            "owner",
            vec![
                Action::ok(publication("/cases/order/blob", b"value")),
                Action::ok(ActionOp::ReadBlob {
                    path: "/cases/order/blob".to_owned(),
                    expected: b"value".to_vec(),
                }),
            ],
            &[],
        )]);
        let mut observation = observe(&case);
        BehaviorOracle::verify(&case, &observation).unwrap();
        observation.executions[0].results.reverse();
        assert_eq!(
            BehaviorOracle::verify(&case, &observation)
                .unwrap_err()
                .signature
                .failure_class,
            FailureClass::MilestoneOrder,
        );
    }

    #[test]
    fn owned_reads_cannot_invent_an_initial_fixture() {
        let path = "/cases/absent/blob";
        let mut case = case(vec![program(
            "reader",
            vec![Action::ok(ActionOp::ReadBlob {
                path: path.to_owned(),
                expected: b"value".to_vec(),
            })],
            &[],
        )]);
        let observation = observe(&case);
        assert_eq!(
            BehaviorOracle::verify(&case, &observation)
                .unwrap_err()
                .invariant,
            "namespace_linearizability",
        );
        case.read_only_fixture_paths.insert(path.to_owned());
        BehaviorOracle::verify(&case, &observation).unwrap();
    }

    #[test]
    fn exhausted_revision_space_rejects_without_overflowing_search() {
        let fixture = "/models/revision-limit";
        let mut case = case(vec![program(
            "owner",
            vec![
                Action::ok(ActionOp::Lookup {
                    path: fixture.to_owned(),
                    expected_kind: "blob".to_owned(),
                }),
                Action::ok(publication("/cases/revision/blob", b"value")),
            ],
            &[],
        )]);
        case.read_only_fixture_paths.insert(fixture.to_owned());
        let mut observation = observe(&case);
        observation.executions[0].results[0].revision = Some(u64::MAX);
        assert_eq!(
            BehaviorOracle::verify(&case, &observation)
                .unwrap_err()
                .invariant,
            "namespace_linearizability",
        );
    }

    #[test]
    fn exclusive_winner_permutations_ignore_observation_vector_order() {
        let case = case(vec![
            program(
                "a",
                vec![Action::linearized(
                    exclusive("/cases/race/name", b"a"),
                    1,
                    1,
                    libc::EEXIST,
                )],
                &[],
            ),
            program(
                "b",
                vec![Action::linearized(
                    exclusive("/cases/race/name", b"b"),
                    1,
                    1,
                    libc::EEXIST,
                )],
                &[],
            ),
        ]);
        for winner in 0..2 {
            let mut observation = observe(&case);
            let loser = &mut observation.executions[1 - winner].results[0];
            loser.outcome = "expected_error".to_owned();
            loser.errno = Some(libc::EEXIST);
            loser.descriptor = None;
            assert!(BehaviorOracle::verify(&case, &observation).is_ok());
            observation.executions.reverse();
            assert!(BehaviorOracle::verify(&case, &observation).is_ok());
        }
    }

    #[test]
    fn declared_success_count_cannot_legalize_double_exclusive_publication() {
        let case = case(vec![
            program(
                "a",
                vec![Action::linearized(
                    exclusive("/cases/race/name", b"a"),
                    1,
                    2,
                    libc::EEXIST,
                )],
                &[],
            ),
            program(
                "b",
                vec![Action::linearized(
                    exclusive("/cases/race/name", b"b"),
                    1,
                    2,
                    libc::EEXIST,
                )],
                &[],
            ),
        ]);
        assert_eq!(
            BehaviorOracle::verify(&case, &observe(&case))
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );
    }

    #[test]
    fn replacement_cannot_leave_displaced_blob_visible() {
        let mut case = case(vec![program(
            "p",
            vec![
                Action::ok(publication("/cases/race/source", b"new")),
                Action::ok(publication("/cases/race/destination", b"old")),
                Action::ok(ActionOp::Rename {
                    source: "/cases/race/source".to_owned(),
                    destination: "/cases/race/destination".to_owned(),
                    replace: true,
                }),
                Action::ok(ActionOp::ReadBlob {
                    path: "/cases/race/destination".to_owned(),
                    expected: b"old".to_vec(),
                }),
            ],
            &[],
        )]);
        assert_eq!(
            BehaviorOracle::verify(&case, &observe(&case))
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );
        case.processes[0].actions[3] = Action::ok(ActionOp::ReadBlob {
            path: "/cases/race/destination".to_owned(),
            expected: b"new".to_vec(),
        });
        BehaviorOracle::verify(&case, &observe(&case)).unwrap();
    }

    #[test]
    fn truncating_descriptor_replacement_requires_an_existing_blob() {
        let path = "/cases/truncate/blob";
        let replacement = Action::ok(ActionOp::DescriptorWrite {
            path: path.to_owned(),
            flags: libc::O_WRONLY | libc::O_TRUNC,
            length: Some(3),
            bytes: b"new".to_vec(),
            method: DescriptorWriteMethod::Write,
            finish: DescriptorFinish::Close,
        });
        let mut case = case(vec![
            program(
                "writer",
                vec![Action::ok(publication(path, b"old")), replacement],
                &[],
            ),
            program(
                "reader",
                vec![Action::ok(ActionOp::ReadBlob {
                    path: path.to_owned(),
                    expected: b"new".to_vec(),
                })],
                &["writer"],
            ),
        ]);
        BehaviorOracle::verify(&case, &observe(&case)).unwrap();
        case.processes[0].actions.remove(0);
        assert!(BehaviorOracle::verify(&case, &observe(&case)).is_err());
        case.processes[0].actions[0].expected = ExpectedOutcome::Error(libc::ENOENT);
        case.processes.pop();
        BehaviorOracle::verify(&case, &observe(&case)).unwrap();
    }

    #[test]
    fn mutation_origin_is_a_causal_lookup_and_destination_matches_receipt() {
        let case = case(vec![program(
            "owner",
            vec![
                Action::ok(publication("/cases/mutation/source", b"value")),
                Action::ok(ActionOp::Lookup {
                    path: "/cases/mutation/source".to_owned(),
                    expected_kind: "blob".to_owned(),
                }),
                Action::ok(ActionOp::Rename {
                    source: "/cases/mutation/source".to_owned(),
                    destination: "/cases/mutation/destination".to_owned(),
                    replace: false,
                }),
            ],
            &[],
        )]);
        let mut observation = observe(&case);
        observation.executions[0].results[1].revision = Some(20);
        observation.executions[0].results[2].revision = Some(21);
        let mut mutation = observation.executions[0].results[2].clone();
        mutation.outcome = "barrier".to_owned();
        mutation.barrier = Some(BarrierObservation::MutationApplied {
            from_revision: Some(20),
            to_revision: Some(21),
        });
        observation.executions[0].results.insert(2, mutation);
        BehaviorOracle::verify(&case, &observation).unwrap();
        let mut errored_receipt = observation.clone();
        errored_receipt.executions[0].results[3].errno = Some(libc::EIO);
        assert_eq!(
            BehaviorOracle::verify(&case, &errored_receipt)
                .unwrap_err()
                .signature
                .failure_class,
            FailureClass::InvalidEvidence
        );
        observation.executions[0].results[2].barrier = Some(BarrierObservation::MutationApplied {
            from_revision: Some(19),
            to_revision: Some(21),
        });
        assert_eq!(
            BehaviorOracle::verify(&case, &observation)
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );
        observation.executions[0].results[2].barrier = Some(BarrierObservation::MutationApplied {
            from_revision: Some(20),
            to_revision: Some(22),
        });
        assert_eq!(
            BehaviorOracle::verify(&case, &observation)
                .unwrap_err()
                .signature
                .failure_class,
            FailureClass::InvalidEvidence
        );
    }

    #[test]
    fn mutation_origin_orders_its_unobserved_publication_among_unrelated_paths() {
        let race = "/cases/origin-order/race";
        let paths = [
            "/cases/origin-order/prelude",
            "/cases/origin-order/a",
            "/cases/origin-order/b",
            "/cases/origin-order/c",
        ];
        let mut case = case(vec![
            program(
                "prelude-writer",
                vec![Action::ok(publication(paths[0], b"p"))],
                &[],
            ),
            program(
                "prelude-reader",
                vec![Action::ok(ActionOp::Lookup {
                    path: paths[0].to_owned(),
                    expected_kind: "blob".to_owned(),
                })],
                &["prelude-writer"],
            ),
            program("setup", vec![Action::ok(publication(race, b"race"))], &[]),
            program(
                "winner",
                vec![Action::linearized(
                    ActionOp::Unlink {
                        path: race.to_owned(),
                    },
                    7,
                    1,
                    libc::ENOENT,
                )],
                &["setup"],
            ),
            program(
                "loser",
                vec![Action::linearized(
                    ActionOp::Unlink {
                        path: race.to_owned(),
                    },
                    7,
                    1,
                    libc::ENOENT,
                )],
                &["setup"],
            ),
            program(
                "topology-writer",
                paths[1..]
                    .iter()
                    .map(|path| Action::ok(publication(path, path.as_bytes())))
                    .collect(),
                &[],
            ),
            program(
                "topology-reader",
                paths[1..]
                    .iter()
                    .map(|path| {
                        Action::ok(ActionOp::Lookup {
                            path: (*path).to_owned(),
                            expected_kind: "blob".to_owned(),
                        })
                    })
                    .collect(),
                &["topology-writer"],
            ),
        ]);
        case.resource_bounds.max_race_states = 64;
        let mut observation = observe(&case);
        observation.executions[1].results[0].revision = Some(100);
        for (result, revision) in observation.executions[6].results.iter_mut().zip(102..=104) {
            result.revision = Some(revision);
        }
        observation.executions[3].results[0].revision = Some(105);
        let mut receipt = observation.executions[3].results[0].clone();
        receipt.outcome = "barrier".to_owned();
        receipt.barrier = Some(BarrierObservation::MutationApplied {
            from_revision: Some(101),
            to_revision: Some(105),
        });
        observation.executions[3].results.insert(0, receipt);
        let loser = &mut observation.executions[4].results[0];
        loser.outcome = "expected_error".to_owned();
        loser.revision = None;
        loser.errno = Some(libc::ENOENT);
        loser.error_type = Some("OSError".to_owned());
        BehaviorOracle::verify(&case, &observation).unwrap();
    }

    #[test]
    fn failed_rename_cannot_claim_a_mutation_receipt() {
        let case = case(vec![program(
            "owner",
            vec![
                Action::ok(publication("/cases/mutation/source", b"source")),
                Action::ok(ActionOp::Lookup {
                    path: "/cases/mutation/source".to_owned(),
                    expected_kind: "blob".to_owned(),
                }),
                Action::ok(publication("/cases/mutation/destination", b"destination")),
                Action::error(
                    ActionOp::Rename {
                        source: "/cases/mutation/source".to_owned(),
                        destination: "/cases/mutation/destination".to_owned(),
                        replace: false,
                    },
                    libc::EEXIST,
                ),
            ],
            &[],
        )]);
        let mut observation = observe(&case);
        observation.executions[0].results[1].revision = Some(20);
        observation.executions[0].results[3].revision = Some(22);
        BehaviorOracle::verify(&case, &observation).unwrap();
        let mut receipt = observation.executions[0].results[3].clone();
        receipt.outcome = "barrier".to_owned();
        receipt.errno = None;
        receipt.error_type = None;
        receipt.barrier = Some(BarrierObservation::MutationApplied {
            from_revision: Some(20),
            to_revision: Some(22),
        });
        observation.executions[0].results.insert(3, receipt);
        assert_eq!(
            BehaviorOracle::verify(&case, &observation)
                .unwrap_err()
                .signature
                .failure_class,
            FailureClass::InvalidEvidence
        );
    }

    #[test]
    fn auxiliary_barriers_require_the_owning_action_identity() {
        let case = case(vec![program(
            "owner",
            vec![Action::ok(publication("/cases/barrier/blob", b"value"))],
            &[],
        )]);
        let mut observation = observe(&case);
        let mut marker = observation.executions[0].results[0].clone();
        marker.outcome = "barrier".to_owned();
        marker.barrier = Some(BarrierObservation::LapCompleted {
            token: "token".to_owned(),
            lap: 1,
        });
        observation.executions[0].results.push(marker);
        BehaviorOracle::verify(&case, &observation).unwrap();
        for (field, invariant) in [
            ("process", "action_identity"),
            ("step", "action_identity"),
            ("action", "action_identity"),
            ("path", "path_consistency"),
        ] {
            let mut foreign = observation.clone();
            let marker = foreign.executions[0].results.last_mut().unwrap();
            match field {
                "process" => marker.process = "foreign".to_owned(),
                "step" => marker.step = 1,
                "action" => marker.action = "lookup".to_owned(),
                "path" => marker.path = "/cases/barrier/foreign".to_owned(),
                _ => unreachable!(),
            }
            assert_eq!(
                BehaviorOracle::verify(&case, &foreign)
                    .unwrap_err()
                    .invariant,
                invariant
            );
        }
    }

    #[test]
    fn dependent_double_unlink_has_no_history_even_with_matching_count() {
        let case = case(vec![
            program(
                "setup",
                vec![Action::ok(publication("/cases/race/name", b"value"))],
                &[],
            ),
            program(
                "a",
                vec![Action::linearized(
                    ActionOp::Unlink {
                        path: "/cases/race/name".to_owned(),
                    },
                    1,
                    2,
                    libc::ENOENT,
                )],
                &["setup"],
            ),
            program(
                "b",
                vec![Action::linearized(
                    ActionOp::Unlink {
                        path: "/cases/race/name".to_owned(),
                    },
                    1,
                    2,
                    libc::ENOENT,
                )],
                &["a"],
            ),
        ]);
        let mut observation = observe(&case);
        observation.executions[2].results[0].revision = Some(11);
        assert_eq!(
            BehaviorOracle::verify(&case, &observation)
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );
    }

    #[test]
    fn lookup_revision_constrains_the_winning_rename() {
        let mut case = case(vec![
            program(
                "setup",
                vec![Action::ok(publication("/cases/race/source", b"value"))],
                &[],
            ),
            program(
                "a",
                vec![Action::linearized(
                    ActionOp::Rename {
                        source: "/cases/race/source".to_owned(),
                        destination: "/cases/race/a".to_owned(),
                        replace: false,
                    },
                    1,
                    1,
                    libc::ENOENT,
                )],
                &["setup"],
            ),
            program(
                "b",
                vec![Action::linearized(
                    ActionOp::Rename {
                        source: "/cases/race/source".to_owned(),
                        destination: "/cases/race/b".to_owned(),
                        replace: false,
                    },
                    1,
                    1,
                    libc::ENOENT,
                )],
                &["setup"],
            ),
            program(
                "reader",
                vec![Action::ok(ActionOp::Lookup {
                    path: "/cases/race/a".to_owned(),
                    expected_kind: "blob".to_owned(),
                })],
                &["a", "b"],
            ),
        ]);
        for (winner, destination) in [(1, "/cases/race/a"), (2, "/cases/race/b")] {
            case.processes[3].actions[0] = Action::ok(ActionOp::Lookup {
                path: destination.to_owned(),
                expected_kind: "blob".to_owned(),
            });
            let mut observation = observe(&case);
            let loser = &mut observation.executions[3 - winner].results[0];
            loser.outcome = "expected_error".to_owned();
            loser.errno = Some(libc::ENOENT);
            loser.revision = None;
            assert!(BehaviorOracle::verify(&case, &observation).is_ok());
            observation.executions.reverse();
            assert!(BehaviorOracle::verify(&case, &observation).is_ok());
            observation
                .executions
                .iter_mut()
                .find(|execution| execution.process == "reader")
                .unwrap()
                .results[0]
                .revision = Some(9);
            assert_eq!(
                BehaviorOracle::verify(&case, &observation)
                    .unwrap_err()
                    .invariant,
                "namespace_linearizability"
            );
        }
    }

    #[test]
    fn namespace_frontier_limit_is_inclusive_and_never_selects_a_winner() {
        let mut races = case(vec![
            program(
                "a",
                vec![Action::error(
                    ActionOp::Unlink {
                        path: "/cases/race/missing".to_owned(),
                    },
                    libc::ENOENT,
                )],
                &[],
            ),
            program(
                "b",
                vec![Action::error(
                    ActionOp::Unlink {
                        path: "/cases/race/missing".to_owned(),
                    },
                    libc::ENOENT,
                )],
                &[],
            ),
        ]);
        races.resource_bounds.max_race_states = 2;
        let mut observed = observe(&races);
        // Both orders verify: acceptance never depends on observation
        // vector order, and a complete greedy legal history needs no
        // interleaving search at any bound.
        assert!(BehaviorOracle::verify(&races, &observed).is_ok());
        observed.executions.reverse();
        assert!(BehaviorOracle::verify(&races, &observed).is_ok());

        // A case no greedy history completes falls back to frontier
        // exploration; an inclusive bound of one legal state still allows
        // exactly one frontier member, and exceeding it is an explicit
        // model failure rather than an arbitrary winner.
        let mut impossible = case(vec![
            program(
                "setup",
                vec![Action::ok(publication("/cases/race/name", b"value"))],
                &[],
            ),
            program(
                "a",
                vec![Action::ok(ActionOp::Unlink {
                    path: "/cases/race/name".to_owned(),
                })],
                &["setup"],
            ),
            program(
                "b",
                vec![Action::ok(ActionOp::Unlink {
                    path: "/cases/race/name".to_owned(),
                })],
                &["setup"],
            ),
        ]);
        impossible.resource_bounds.max_race_states = 1;
        assert_eq!(
            BehaviorOracle::verify(&impossible, &observe(&impossible))
                .unwrap_err()
                .invariant,
            "namespace_model_explosion"
        );
        impossible.resource_bounds.max_race_states = 8;
        assert_eq!(
            BehaviorOracle::verify(&impossible, &observe(&impossible))
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );
    }

    #[test]
    fn slow_branch_release_is_proved_causally_not_by_observation_arrival() {
        let parked = "/cases/slow/parked";
        let release = "/cases/slow/release";
        let await_blob = |path: &str| {
            Action::ok(ActionOp::AwaitEntry {
                path: path.to_owned(),
                expected_kind: "blob".to_owned(),
            })
        };
        let mut case = case(vec![
            program(
                "slow",
                vec![
                    Action::ok(publication(parked, b"")),
                    await_blob(release),
                    Action::ok(publication("/cases/slow/completed", b"released")),
                ],
                &[],
            ),
            program(
                "healthy",
                vec![
                    await_blob(parked),
                    Action::ok(publication("/cases/slow/healthy", b"work")),
                ],
                &[],
            ),
            program(
                "release",
                vec![await_blob(parked), Action::ok(publication(release, b""))],
                &["healthy"],
            ),
        ]);
        case.failure = FailureInjection::SlowProcess {
            process: "slow".to_owned(),
            parked_path: parked.to_owned(),
            release_path: release.to_owned(),
        };
        let mut observation = observe(&case);
        for execution in &mut observation.executions {
            for result in &mut execution.results {
                if result.action == "lookup" {
                    result.kind = Some("blob".to_owned());
                    result.revision = Some(if result.path == parked { 100 } else { 102 });
                    result.active = Some(false);
                }
            }
        }
        BehaviorOracle::verify(&case, &observation).unwrap();
        observation.executions.reverse();
        BehaviorOracle::verify(&case, &observation).unwrap();
        let slow = observation
            .executions
            .iter_mut()
            .find(|execution| execution.process == "slow")
            .unwrap();
        slow.results[1].revision = Some(99);
        assert_eq!(
            BehaviorOracle::verify(&case, &observation)
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );
        let scoped = case.for_attempt(7, true);
        scoped.validate().unwrap();
        assert!(matches!(&scoped.failure,
            FailureInjection::SlowProcess { parked_path, release_path, .. }
                if parked_path == scoped.processes[0].actions[0].operation.path()
                    && release_path == scoped.processes[0].actions[1].operation.path()
                    && parked_path != parked
        ));
        case.processes[2].depends_on.push("slow".to_owned());
        assert!(
            case.validate().is_err(),
            "release cannot depend on the parked process"
        );
        assert!(
            serde_json::from_value::<FailureInjection>(serde_json::json!({
                "type": "slow_process", "process": "slow", "delay_ms": 1000
            }))
            .is_err(),
            "elapsed-only legacy cases must not silently change meaning"
        );
    }

    #[test]
    fn ordinary_publication_replacement_accepts_either_last_commit() {
        for bytes in [b"a".as_slice(), b"b".as_slice()] {
            let case = case(vec![
                program(
                    "a",
                    vec![Action::ok(publication("/cases/race/name", b"a"))],
                    &[],
                ),
                program(
                    "b",
                    vec![Action::ok(publication("/cases/race/name", b"b"))],
                    &[],
                ),
                program(
                    "reader",
                    vec![Action::ok(ActionOp::ReadBlob {
                        path: "/cases/race/name".to_owned(),
                        expected: bytes.to_vec(),
                    })],
                    &["a", "b"],
                ),
            ]);
            let mut observation = observe(&case);
            assert!(BehaviorOracle::verify(&case, &observation).is_ok());
            observation.executions.reverse();
            assert!(BehaviorOracle::verify(&case, &observation).is_ok());
        }
    }

    #[test]
    fn unobserved_stream_incarnations_cannot_pass_namespace_race_verification() {
        let case = case(vec![
            program(
                "a",
                vec![Action::ok(ActionOp::StreamWrite {
                    path: "/cases/race/stream".to_owned(),
                    chunks: vec![b"a".to_vec()],
                    replace: false,
                })],
                &[],
            ),
            program(
                "b",
                vec![Action::ok(ActionOp::StreamWrite {
                    path: "/cases/race/stream".to_owned(),
                    chunks: vec![b"a".to_vec()],
                    replace: true,
                })],
                &[],
            ),
        ]);
        let mut observation = observe(&case);
        for execution in &mut observation.executions {
            execution.results[0].length = Some(1);
            execution.results[0].digest = Some(digest(b"a"));
        }
        assert_eq!(
            BehaviorOracle::verify(&case, &observation)
                .unwrap_err()
                .invariant,
            "namespace_model_evidence"
        );
    }

    fn stream_observation(
        case: &BehaviorCase,
        identities: &[(&str, usize, u64)],
    ) -> CaseObservation {
        let mut observation = observe(case);
        for execution in &mut observation.executions {
            let program = case
                .processes
                .iter()
                .find(|program| program.id == execution.process)
                .unwrap();
            let mut records = Vec::new();
            for mut result in std::mem::take(&mut execution.results) {
                let action = &program.actions[result.step].operation;
                let Some((_, _, own_chunks)) = stream_parameters(action) else {
                    records.push(result);
                    continue;
                };
                result.incarnation = identities
                    .iter()
                    .find(|(process, step, _)| {
                        *process == execution.process && *step == result.step
                    })
                    .map(|(_, _, incarnation)| *incarnation);
                let expected = match action {
                    ActionOp::StreamRead { expected, .. }
                    | ActionOp::StreamReadWithRetry { expected, .. }
                    | ActionOp::GatedStreamRead { expected, .. }
                    | ActionOp::StreamReadInto { expected, .. } => expected.clone(),
                    _ => own_chunks.unwrap().concat(),
                };
                let chunks = own_chunks.or_else(|| {
                    case.processes
                        .iter()
                        .flat_map(|source| {
                            source
                                .actions
                                .iter()
                                .enumerate()
                                .map(move |(step, action)| (source, step, action))
                        })
                        .filter_map(|(source, step, source_action)| {
                            let (_, _, chunks) = stream_parameters(&source_action.operation)?;
                            let chunks = chunks?;
                            if namespace_path(source, source_action.operation.path())
                                != namespace_path(program, action.path())
                            {
                                return None;
                            }
                            let identity = identities
                                .iter()
                                .find(|(id, known_step, _)| *id == source.id && *known_step == step)
                                .map(|(_, _, incarnation)| *incarnation);
                            Some((identity != result.incarnation, chunks))
                        })
                        .min_by_key(|(different, _)| *different)
                        .map(|(_, chunks)| chunks)
                });
                let fallback = vec![expected.clone()];
                let complete = result.outcome == "ok";
                if complete {
                    result.length = Some(expected.len());
                    result.digest = Some(digest(&expected));
                } else {
                    result.length = None;
                    result.digest = None;
                }
                let frames = if complete {
                    chunks.unwrap_or(&fallback)
                } else {
                    &[]
                };
                records.extend(framed_records(result, action, frames, complete));
            }
            execution.results = records;
        }
        observation
    }

    fn framed_records(
        mut terminal: ActionObservation,
        action: &ActionOp,
        chunks: &[Vec<u8>],
        complete: bool,
    ) -> Vec<ActionObservation> {
        let Some(incarnation) = terminal.incarnation else {
            return vec![terminal];
        };
        if !complete && !chunks.is_empty() {
            terminal.transfer = Some(TransferObservation {
                length: chunks.iter().map(Vec::len).sum(),
                digest: digest(&chunks.concat()),
                complete: false,
            });
        }
        let mut records = Vec::new();
        let mut emit = |barrier: BarrierObservation, direction: Option<&str>| {
            let mut record = terminal.clone();
            record.outcome = "barrier".to_owned();
            record.errno = None;
            record.error_type = None;
            record.length = None;
            record.digest = None;
            record.transfer = None;
            record.barrier = Some(barrier);
            if let Some(direction) = direction {
                record.action = direction.to_owned();
            }
            records.push(record);
        };
        emit(BarrierObservation::StreamOpened { incarnation }, None);
        let (source, _, _) = stream_parameters(action).unwrap();
        if source {
            for (index, chunk) in chunks.iter().enumerate() {
                emit(
                    BarrierObservation::StreamFrame {
                        incarnation,
                        index: index as u64,
                        length: chunk.len() as u64,
                        digest: digest(chunk),
                    },
                    Some("stream_write"),
                );
                if index == 0
                    && let ActionOp::GatedStreamWrite { release_path, .. } = action
                {
                    emit(
                        BarrierObservation::ReleaseObserved {
                            path: release_path.clone(),
                        },
                        None,
                    );
                }
            }
        }
        if !source || matches!(action, ActionOp::StreamRoundTrip { .. }) {
            for (index, chunk) in chunks.iter().enumerate() {
                emit(
                    BarrierObservation::StreamFrame {
                        incarnation,
                        index: index as u64,
                        length: chunk.len() as u64,
                        digest: digest(chunk),
                    },
                    Some("stream_read"),
                );
                if index == 0 {
                    emit(BarrierObservation::StreamFirstFrame { incarnation }, None);
                }
            }
            if complete {
                emit(BarrierObservation::StreamEof { incarnation }, None);
            }
        }
        records.push(terminal);
        records
    }

    fn stream_result(execution: &mut ExecutionObservation, step: usize) -> &mut ActionObservation {
        execution
            .results
            .iter_mut()
            .find(|result| result.step == step && !is_barrier_record(result))
            .unwrap()
    }

    fn retag_stream(execution: &mut ExecutionObservation, step: usize, identity: u64) {
        for record in execution
            .results
            .iter_mut()
            .filter(|record| record.step == step)
        {
            record.incarnation = Some(identity);
            match &mut record.barrier {
                Some(
                    BarrierObservation::StreamOpened { incarnation }
                    | BarrierObservation::StreamFrame { incarnation, .. }
                    | BarrierObservation::StreamFirstFrame { incarnation }
                    | BarrierObservation::StreamEof { incarnation },
                ) => *incarnation = identity,
                _ => {}
            }
        }
    }

    fn stream_roundtrip(path: &str) -> Action {
        Action::ok(ActionOp::StreamRoundTrip {
            path: path.to_owned(),
            chunks: vec![b"a".to_vec(), b"b".to_vec()],
        })
    }

    #[test]
    fn logical_frame_loss_duplication_order_and_content_are_independent_of_aggregate_bytes() {
        let chunks = vec![
            Vec::new(),
            b"a".to_vec(),
            Vec::new(),
            b"bc".to_vec(),
            Vec::new(),
        ];
        let case = case(vec![
            program(
                "writer",
                vec![Action::ok(ActionOp::StreamWrite {
                    path: "/cases/frames/stream".to_owned(),
                    chunks,
                    replace: false,
                })],
                &[],
            ),
            program(
                "reader",
                vec![Action::ok(ActionOp::StreamReadInto {
                    path: "/cases/frames/stream".to_owned(),
                    expected: b"abc".to_vec(),
                    buffer_sizes: vec![1, 17, 2],
                })],
                &[],
            ),
        ]);
        let observed = stream_observation(&case, &[("writer", 0, 20), ("reader", 0, 20)]);
        BehaviorOracle::verify(&case, &observed).unwrap();
        let mut reversed = observed.clone();
        reversed.executions.reverse();
        BehaviorOracle::verify(&case, &reversed).unwrap();
        for process in 0..2 {
            for corruption in 0..6 {
                let mut forged = observed.clone();
                let records = &mut forged.executions[process].results;
                let positions = records
                    .iter()
                    .enumerate()
                    .filter_map(|(position, record)| {
                        matches!(record.barrier, Some(BarrierObservation::StreamFrame { .. }))
                            .then_some(position)
                    })
                    .collect::<Vec<_>>();
                let class = match corruption {
                    0 => {
                        // Empty trailing frames cannot disappear behind the same byte digest.
                        records.remove(positions[4]);
                        FailureClass::FrameCountMismatch
                    }
                    1 => {
                        let mut repeated = records[positions[4]].clone();
                        if let Some(BarrierObservation::StreamFrame { index, .. }) =
                            &mut repeated.barrier
                        {
                            *index = 5;
                        }
                        records.insert(positions[4] + 1, repeated);
                        FailureClass::FrameCountMismatch
                    }
                    2 => {
                        records.swap(positions[1], positions[3]);
                        FailureClass::FrameOrderMismatch
                    }
                    3 => {
                        if let Some(BarrierObservation::StreamFrame { digest, .. }) =
                            &mut records[positions[1]].barrier
                        {
                            *digest = super::digest(b"x");
                        }
                        FailureClass::FrameContentMismatch
                    }
                    4 => {
                        if let Some(BarrierObservation::StreamFrame { length, .. }) =
                            &mut records[positions[1]].barrier
                        {
                            *length = 2;
                        }
                        if let Some(BarrierObservation::StreamFrame { length, .. }) =
                            &mut records[positions[3]].barrier
                        {
                            *length = 1;
                        }
                        FailureClass::FrameContentMismatch
                    }
                    _ => {
                        if let Some(BarrierObservation::StreamFrame { digest, .. }) =
                            &mut records[positions[2]].barrier
                        {
                            *digest = super::digest(b"not empty");
                        }
                        FailureClass::FrameContentMismatch
                    }
                };
                // Terminal length/digest remain the correct aggregate "abc".
                let failure = BehaviorOracle::verify(&case, &forged).unwrap_err();
                assert_eq!(failure.signature.failure_class, class);
            }
        }
    }

    #[test]
    fn every_successful_reader_and_roundtrip_requires_its_own_clean_eof() {
        let case = case(vec![
            program(
                "writer",
                vec![Action::ok(ActionOp::StreamWrite {
                    path: "/cases/eof/stream".to_owned(),
                    chunks: vec![Vec::new()],
                    replace: false,
                })],
                &[],
            ),
            program(
                "reader",
                vec![Action::ok(ActionOp::StreamRead {
                    path: "/cases/eof/stream".to_owned(),
                    expected: Vec::new(),
                })],
                &[],
            ),
            program(
                "roundtrip",
                vec![stream_roundtrip("/cases/eof/roundtrip")],
                &[],
            ),
        ]);
        let observed = stream_observation(
            &case,
            &[("writer", 0, 20), ("reader", 0, 20), ("roundtrip", 0, 21)],
        );
        BehaviorOracle::verify(&case, &observed).unwrap();
        for process in [1, 2] {
            let mut incomplete = observed.clone();
            incomplete.executions[process].results.retain(|record| {
                !matches!(record.barrier, Some(BarrierObservation::StreamEof { .. }))
            });
            assert_eq!(
                BehaviorOracle::verify(&case, &incomplete)
                    .unwrap_err()
                    .signature
                    .failure_class,
                FailureClass::MissingStreamEof
            );
        }
    }

    #[test]
    fn stream_consumers_require_the_exact_created_identity_and_payload() {
        let mut case = case(vec![
            program(
                "writer",
                vec![Action::ok(ActionOp::StreamWrite {
                    path: "/cases/race/stream".to_owned(),
                    chunks: vec![b"a".to_vec(), b"b".to_vec()],
                    replace: false,
                })],
                &[],
            ),
            program(
                "reader",
                vec![Action::ok(ActionOp::StreamRead {
                    path: "/cases/race/stream".to_owned(),
                    expected: b"ab".to_vec(),
                })],
                &[],
            ),
        ]);
        let observed = stream_observation(&case, &[("writer", 0, 20), ("reader", 0, 20)]);
        assert!(BehaviorOracle::verify(&case, &observed).is_ok());
        let expired = Budget::new(std::time::Duration::ZERO);
        assert_eq!(
            BehaviorOracle::verify_with_budget(&case, &observed, &expired)
                .unwrap_err()
                .invariant,
            "execution_budget",
        );
        let invented = stream_observation(&case, &[("writer", 0, 20), ("reader", 0, 21)]);
        assert_eq!(
            BehaviorOracle::verify(&case, &invented)
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );
        // Both observations satisfy their own payload assertions, but the
        // consumer must still receive the bytes of its matched source.
        if let ActionOp::StreamRead { expected, .. } = &mut case.processes[1].actions[0].operation {
            *expected = b"ba".to_vec();
        }
        let wrong_bytes = stream_observation(&case, &[("writer", 0, 20), ("reader", 0, 20)]);
        assert_eq!(
            BehaviorOracle::verify(&case, &wrong_bytes)
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );
    }

    #[test]
    fn ring_token_barriers_after_stream_results_do_not_violate_causal_order() {
        let case = case(vec![program(
            "relay",
            vec![Action::ok(ActionOp::StreamRoundTrip {
                path: "/cases/race/stream".to_owned(),
                chunks: vec![b"a".to_vec(), b"b".to_vec()],
            })],
            &[],
        )]);
        let mut observed = stream_observation(&case, &[("relay", 0, 20)]);
        assert!(BehaviorOracle::verify(&case, &observed).is_ok());
        for execution in &mut observed.executions {
            let mut forwarded = execution.results[0].clone();
            forwarded.outcome = "barrier".to_owned();
            forwarded.errno = None;
            forwarded.incarnation = None;
            forwarded.barrier = Some(BarrierObservation::TokenForwarded {
                token: "ring-token".to_owned(),
                lap: 0,
                edge_index: 0,
            });
            let mut lap = forwarded.clone();
            execution.results.push(forwarded);
            lap.barrier = Some(BarrierObservation::LapCompleted {
                token: "ring-token".to_owned(),
                lap: 1,
            });
            execution.results.push(lap);
        }
        assert!(BehaviorOracle::verify(&case, &observed).is_ok());
    }

    #[test]
    fn stream_reopen_and_blob_mutations_share_one_revision_order() {
        let case = case(vec![program(
            "owner",
            vec![
                stream_roundtrip("/cases/race/stream"),
                Action::ok(publication("/cases/race/blob", b"blob")),
                Action::ok(ActionOp::Lookup {
                    path: "/cases/race/blob".to_owned(),
                    expected_kind: "blob".to_owned(),
                }),
                stream_roundtrip("/cases/race/stream"),
            ],
            &[],
        )]);
        let mut observed = stream_observation(&case, &[("owner", 0, 20), ("owner", 3, 22)]);
        stream_result(&mut observed.executions[0], 2).revision = Some(21);
        assert!(BehaviorOracle::verify(&case, &observed).is_ok());
        // Reusing either a prior incarnation or the intervening blob's
        // revision cannot be legalized by generation counts or inequalities.
        for invalid in [20, 21] {
            retag_stream(&mut observed.executions[0], 3, invalid);
            assert_eq!(
                BehaviorOracle::verify(&case, &observed)
                    .unwrap_err()
                    .invariant,
                "namespace_linearizability"
            );
        }
    }

    #[test]
    fn gated_displacement_requires_stale_old_handles_not_new_identities() {
        let mut case = case(vec![
            program(
                "writer",
                vec![Action::error(
                    ActionOp::GatedStreamWrite {
                        path: "/cases/race/stream".to_owned(),
                        replace: false,
                        frames: vec![b"a".to_vec(), b"b".to_vec()],
                        release_path: "/cases/race/release".to_owned(),
                    },
                    libc::ESTALE,
                )],
                &[],
            ),
            program(
                "reader",
                vec![Action::error(
                    ActionOp::GatedStreamRead {
                        path: "/cases/race/stream".to_owned(),
                        expected: b"ab".to_vec(),
                        observed_path: "/cases/race/ready".to_owned(),
                        retry_attach: false,
                        park_after_first_frame: false,
                    },
                    libc::ESTALE,
                )],
                &[],
            ),
            program(
                "replacer",
                vec![
                    Action::ok(publication("/cases/race/stream", b"replacement")),
                    Action::ok(publication("/cases/race/release", b"")),
                ],
                &[],
            ),
        ]);
        let mut observed = stream_observation(&case, &[("writer", 0, 20), ("reader", 0, 20)]);
        // Both displaced endpoints prove only the first complete logical frame.
        for process in 0..2 {
            let terminal = stream_result(&mut observed.executions[process], 0).clone();
            observed.executions[process].results = framed_records(
                terminal,
                &case.processes[process].actions[0].operation,
                &[b"a".to_vec()],
                false,
            );
        }
        assert!(BehaviorOracle::verify(&case, &observed).is_ok());
        let mut corrupt_prefix = observed.clone();
        let transferred = corrupt_prefix.executions[1]
            .results
            .iter_mut()
            .find(|result| !is_barrier_record(result))
            .unwrap();
        transferred.transfer.as_mut().unwrap().digest = digest(b"X");
        assert_eq!(
            BehaviorOracle::verify(&case, &corrupt_prefix)
                .unwrap_err()
                .signature
                .failure_class,
            FailureClass::PayloadMismatch
        );
        for process in 0..2 {
            let mut corrupted = observed.clone();
            let frame = corrupted.executions[process]
                .results
                .iter_mut()
                .find_map(|record| match &mut record.barrier {
                    Some(BarrierObservation::StreamFrame { digest, .. }) => Some(digest),
                    _ => None,
                })
                .unwrap();
            *frame = digest(b"X");
            assert_eq!(
                BehaviorOracle::verify(&case, &corrupted)
                    .unwrap_err()
                    .signature
                    .failure_class,
                FailureClass::FrameContentMismatch
            );
        }
        observed.executions.reverse();
        assert!(BehaviorOracle::verify(&case, &observed).is_ok());
        observed.executions.reverse();
        // Release is causally after blob replacement. An old writer cannot
        // finish successfully by pretending its whole action preceded it.
        case.processes[0].actions[0].expected = ExpectedOutcome::Ok;
        let mut terminal = stream_result(&mut observed.executions[0], 0).clone();
        terminal.outcome = "ok".to_owned();
        terminal.errno = None;
        terminal.error_type = None;
        terminal.length = Some(2);
        terminal.digest = Some(digest(b"ab"));
        terminal.transfer = None;
        observed.executions[0].results = framed_records(
            terminal,
            &case.processes[0].actions[0].operation,
            &[b"a".to_vec(), b"b".to_vec()],
            true,
        );
        assert_eq!(
            BehaviorOracle::verify(&case, &observed)
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );
    }

    #[test]
    fn pending_stream_rename_is_busy_and_quiescent_rename_changes_revision() {
        let mut case = case(vec![
            program(
                "writer",
                vec![Action::ok(ActionOp::GatedStreamWrite {
                    path: "/cases/race/stream".to_owned(),
                    replace: false,
                    frames: vec![b"a".to_vec(), b"b".to_vec()],
                    release_path: "/cases/race/release".to_owned(),
                })],
                &[],
            ),
            program(
                "reader",
                vec![Action::ok(ActionOp::GatedStreamRead {
                    path: "/cases/race/stream".to_owned(),
                    expected: b"ab".to_vec(),
                    observed_path: "/cases/race/ready".to_owned(),
                    retry_attach: false,
                    park_after_first_frame: false,
                })],
                &[],
            ),
            program(
                "renamer",
                vec![
                    Action::ok(ActionOp::Lookup {
                        path: "/cases/race/ready".to_owned(),
                        expected_kind: "blob".to_owned(),
                    }),
                    Action::error(
                        ActionOp::Rename {
                            source: "/cases/race/stream".to_owned(),
                            destination: "/cases/race/moved".to_owned(),
                            replace: false,
                        },
                        libc::EBUSY,
                    ),
                    Action::ok(publication("/cases/race/release", b"")),
                    Action::ok(ActionOp::WaitForQuiescent {
                        path: "/cases/race/stream".to_owned(),
                    }),
                    Action::ok(ActionOp::Rename {
                        source: "/cases/race/stream".to_owned(),
                        destination: "/cases/race/moved".to_owned(),
                        replace: false,
                    }),
                    Action::ok(ActionOp::Lookup {
                        path: "/cases/race/moved".to_owned(),
                        expected_kind: "stream".to_owned(),
                    }),
                ],
                &[],
            ),
        ]);
        // Global receipts: stream=20, ready=21, release=22, rename=23.
        let mut observed = stream_observation(&case, &[("writer", 0, 20), ("reader", 0, 20)]);
        let results = &mut observed.executions[2].results;
        results[0].revision = Some(21);
        results[1].revision = None;
        results[3].revision = Some(20);
        results[3].kind = Some("stream".to_owned());
        results[3].active = Some(false);
        results[4].revision = Some(23);
        results[5].revision = Some(23);
        results[5].active = Some(false);
        assert!(BehaviorOracle::verify(&case, &observed).is_ok());
        // A renamed stream node is a new namespace revision, not its old
        // transport incarnation.
        observed.executions[2].results[5].revision = Some(20);
        assert_eq!(
            BehaviorOracle::verify(&case, &observed)
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );
        // Keep this assertion about behavior rather than a specific error
        // string or the private representation of runtime endpoints.
        case.processes[2].actions[5].operation = ActionOp::Lookup {
            path: "/cases/race/stream".to_owned(),
            expected_kind: "stream".to_owned(),
        };
        observed.executions[2].results[5].path = "/cases/race/stream".to_owned();
        assert_eq!(
            BehaviorOracle::verify(&case, &observed)
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );
    }

    #[test]
    fn stream_type_errors_and_quiescent_unlink_preserve_namespace_state() {
        let case = case(vec![program(
            "owner",
            vec![
                stream_roundtrip("/cases/race/stream"),
                Action::error(exclusive("/cases/race/stream", b"x"), libc::ENXIO),
                Action::error(
                    ActionOp::Unlink {
                        path: "/cases/race/stream".to_owned(),
                    },
                    libc::ENXIO,
                ),
                Action::ok(ActionOp::WaitForQuiescent {
                    path: "/cases/race/stream".to_owned(),
                }),
                Action::ok(ActionOp::Unlink {
                    path: "/cases/race/stream".to_owned(),
                }),
                Action::ok(publication("/cases/race/stream", b"blob")),
                Action::ok(ActionOp::Lookup {
                    path: "/cases/race/stream".to_owned(),
                    expected_kind: "blob".to_owned(),
                }),
                Action::exception(
                    ActionOp::StreamWrite {
                        path: "/cases/race/stream".to_owned(),
                        chunks: vec![b"x".to_vec()],
                        replace: false,
                    },
                    PythonException::StreamError,
                ),
            ],
            &[],
        )]);
        let mut observed = stream_observation(&case, &[("owner", 0, 20)]);
        stream_result(&mut observed.executions[0], 2).revision = None;
        let probe = stream_result(&mut observed.executions[0], 3);
        probe.revision = Some(20);
        probe.kind = Some("stream".to_owned());
        probe.active = Some(false);
        stream_result(&mut observed.executions[0], 4).revision = Some(21);
        stream_result(&mut observed.executions[0], 6).revision = Some(22);
        BehaviorOracle::verify(&case, &observed).unwrap();
        // A pre-open type error must not claim it attached to the old stream.
        let terminal = stream_result(&mut observed.executions[0], 7);
        terminal.incarnation = Some(20);
        assert_eq!(
            BehaviorOracle::verify(&case, &observed)
                .unwrap_err()
                .invariant,
            "namespace_model_evidence"
        );
    }

    #[test]
    fn blocked_disjoint_lookup_cannot_prune_the_history_that_enables_it() {
        let case = case(vec![
            program(
                "waiter",
                vec![Action::ok(ActionOp::Lookup {
                    path: "/cases/race/ready".to_owned(),
                    expected_kind: "blob".to_owned(),
                })],
                &[],
            ),
            program(
                "a",
                vec![Action::ok(publication("/cases/race/name", b"a"))],
                &[],
            ),
            program(
                "b",
                vec![Action::ok(publication("/cases/race/name", b"b"))],
                &[],
            ),
            program(
                "reader",
                vec![
                    Action::ok(ActionOp::ReadBlob {
                        path: "/cases/race/name".to_owned(),
                        expected: b"a".to_vec(),
                    }),
                    Action::ok(publication("/cases/race/ready", b"")),
                ],
                &["a", "b"],
            ),
        ]);
        // Greedy a-before-b dead-ends. Full replay must explore b-before-a,
        // then the reader which enables the initially blocked waiter. Those
        // two actions are path-disjoint, but cannot be pruned as commuting.
        let mut observed = observe(&case);
        observed.executions[0].results[0].revision = Some(30);
        assert!(BehaviorOracle::verify(&case, &observed).is_ok());
        observed.executions.reverse();
        assert!(BehaviorOracle::verify(&case, &observed).is_ok());
    }

    #[test]
    fn replacement_displaces_pending_open_without_inventing_a_handle_identity() {
        let case = case(vec![
            program(
                "pending",
                vec![Action::error(
                    ActionOp::StreamWrite {
                        path: "/cases/race/stream".to_owned(),
                        chunks: vec![b"old".to_vec()],
                        replace: false,
                    },
                    libc::ESTALE,
                )],
                &[],
            ),
            program(
                "replacement",
                vec![Action::ok(ActionOp::StreamWrite {
                    path: "/cases/race/stream".to_owned(),
                    chunks: vec![b"new".to_vec()],
                    replace: true,
                })],
                &[],
            ),
            program(
                "reader",
                vec![Action::ok(ActionOp::StreamReadWithRetry {
                    path: "/cases/race/stream".to_owned(),
                    expected: b"new".to_vec(),
                })],
                &[],
            ),
        ]);
        let mut observed = stream_observation(&case, &[("replacement", 0, 20), ("reader", 0, 20)]);
        assert!(BehaviorOracle::verify(&case, &observed).is_ok());
        // The pending open must commit before its replacement. Revision one
        // leaves no possible earlier nonzero revision for that creation.
        retag_stream(&mut observed.executions[1], 0, 1);
        retag_stream(&mut observed.executions[2], 0, 1);
        assert_eq!(
            BehaviorOracle::verify(&case, &observed)
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );
    }

    #[test]
    fn stream_replacement_cannot_reuse_an_attached_incarnation() {
        let case = case(vec![
            program(
                "old-writer",
                vec![Action::error(
                    ActionOp::StreamWrite {
                        path: "/cases/race/stream".to_owned(),
                        chunks: vec![b"old".to_vec()],
                        replace: false,
                    },
                    libc::ESTALE,
                )],
                &[],
            ),
            program(
                "old-reader",
                vec![Action::error(
                    ActionOp::StreamRead {
                        path: "/cases/race/stream".to_owned(),
                        expected: b"old".to_vec(),
                    },
                    libc::ESTALE,
                )],
                &[],
            ),
            program(
                "new-writer",
                vec![Action::ok(ActionOp::StreamWrite {
                    path: "/cases/race/stream".to_owned(),
                    chunks: vec![b"new".to_vec()],
                    replace: true,
                })],
                &[],
            ),
            program(
                "new-reader",
                vec![Action::ok(ActionOp::StreamRead {
                    path: "/cases/race/stream".to_owned(),
                    expected: b"new".to_vec(),
                })],
                &[],
            ),
        ]);
        let mut observed = stream_observation(
            &case,
            &[
                ("old-writer", 0, 20),
                ("old-reader", 0, 20),
                ("new-writer", 0, 21),
                ("new-reader", 0, 21),
            ],
        );
        assert!(BehaviorOracle::verify(&case, &observed).is_ok());
        retag_stream(&mut observed.executions[2], 0, 20);
        retag_stream(&mut observed.executions[3], 0, 20);
        assert_eq!(
            BehaviorOracle::verify(&case, &observed)
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );
    }

    #[test]
    fn stopped_pending_reader_can_cancel_without_an_attached_identity() {
        let mut case = case(vec![
            program(
                "stopped",
                vec![
                    Action::ok(ActionOp::StreamRead {
                        path: "/cases/race/pending".to_owned(),
                        expected: b"never".to_vec(),
                    }),
                    Action::ok(publication("/cases/race/unexecuted", b"never")),
                ],
                &[],
            ),
            program(
                "observer",
                vec![
                    Action::ok(ActionOp::Lookup {
                        path: "/cases/race/pending".to_owned(),
                        expected_kind: "stream".to_owned(),
                    }),
                    Action::error(
                        ActionOp::Lookup {
                            path: "/cases/race/unexecuted".to_owned(),
                            expected_kind: "blob".to_owned(),
                        },
                        libc::ENOENT,
                    ),
                ],
                &["stopped"],
            ),
        ]);
        case.failure = FailureInjection::StopProcess {
            process: "stopped".to_owned(),
            phase: ProcessStopPhase::AfterContextReady,
            kill_after_ms: Some(10),
        };
        let mut observed = observe(&case);
        observed.executions[0].results.clear();
        observed.executions[0].exit_success = false;
        observed.executions[0]
            .lifecycle
            .retain(|event| event != "user_result");
        observed.executions[1].results[0].revision = Some(20);
        observed.executions[1].results[0].active = Some(false);
        observed.executions[1].results[1].revision = None;
        assert!(BehaviorOracle::verify(&case, &observed).is_ok());
        // Cancellation leaves the committed stream name but cannot execute
        // the stopped process's subsequent publication.
        case.processes[1].actions[1].expected = ExpectedOutcome::Ok;
        observed.executions[1].results[1].outcome = "ok".to_owned();
        observed.executions[1].results[1].errno = None;
        observed.executions[1].results[1].revision = Some(21);
        assert_eq!(
            BehaviorOracle::verify(&case, &observed)
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );
    }

    #[test]
    fn empty_logical_frame_releases_the_gate_before_the_tail() {
        let case = case(vec![
            program(
                "writer",
                vec![Action::ok(ActionOp::GatedStreamWrite {
                    path: "/cases/race/stream".to_owned(),
                    replace: false,
                    frames: vec![Vec::new(), b"tail".to_vec()],
                    release_path: "/cases/race/ready".to_owned(),
                })],
                &[],
            ),
            program(
                "reader",
                vec![Action::ok(ActionOp::GatedStreamRead {
                    path: "/cases/race/stream".to_owned(),
                    expected: b"tail".to_vec(),
                    observed_path: "/cases/race/ready".to_owned(),
                    retry_attach: false,
                    park_after_first_frame: false,
                })],
                &[],
            ),
        ]);
        let observed = stream_observation(&case, &[("writer", 0, 20), ("reader", 0, 20)]);
        BehaviorOracle::verify(&case, &observed).unwrap();
    }

    #[test]
    fn retry_read_requires_a_real_matching_source_and_current_incarnation() {
        let path = "/cases/retry/stream";
        let reader = || {
            program(
                "reader",
                vec![Action::ok(ActionOp::StreamReadWithRetry {
                    path: path.to_owned(),
                    expected: b"ab".to_vec(),
                })],
                &[],
            )
        };
        let phantom = case(vec![reader()]);
        let no_identity = stream_observation(&phantom, &[]);
        assert_eq!(
            BehaviorOracle::verify(&phantom, &no_identity)
                .unwrap_err()
                .signature
                .failure_class,
            FailureClass::MissingIncarnation
        );
        let invented = stream_observation(&phantom, &[("reader", 0, 20)]);
        assert_eq!(
            BehaviorOracle::verify(&phantom, &invented)
                .unwrap_err()
                .signature
                .failure_class,
            FailureClass::MissingStreamSource
        );

        let mut exchange = case(vec![
            program(
                "writer",
                vec![Action::ok(ActionOp::StreamWrite {
                    path: path.to_owned(),
                    chunks: vec![vec![], b"a".to_vec(), vec![], b"b".to_vec()],
                    replace: false,
                })],
                &[],
            ),
            reader(),
        ]);
        let matching = stream_observation(&exchange, &[("writer", 0, 20), ("reader", 0, 20)]);
        BehaviorOracle::verify(&exchange, &matching).unwrap();
        let stale = stream_observation(&exchange, &[("writer", 0, 20), ("reader", 0, 19)]);
        let stale_failure = BehaviorOracle::verify(&exchange, &stale).unwrap_err();
        assert_eq!(
            stale_failure.signature.failure_class,
            FailureClass::ConflictingIncarnation
        );
        if let ActionOp::StreamReadWithRetry { path, .. } =
            &mut exchange.processes[1].actions[0].operation
        {
            *path = "/cases/retry/unrelated".to_owned();
        }
        let unrelated = stream_observation(&exchange, &[("writer", 0, 20), ("reader", 0, 20)]);
        assert_eq!(
            BehaviorOracle::verify(&exchange, &unrelated)
                .unwrap_err()
                .signature
                .failure_class,
            FailureClass::MissingStreamSource
        );
        exchange.processes[1].actions[0].operation = ActionOp::StreamReadWithRetry {
            path: path.to_owned(),
            expected: b"ba".to_vec(),
        };
        let wrong_source = stream_observation(&exchange, &[("writer", 0, 20), ("reader", 0, 20)]);
        let content_failure = BehaviorOracle::verify(&exchange, &wrong_source).unwrap_err();
        assert_eq!(
            content_failure.signature.failure_class,
            FailureClass::StreamSourceContent
        );
        assert_eq!(content_failure.invariant, stale_failure.invariant);
        assert_ne!(content_failure.signature, stale_failure.signature);
    }

    #[test]
    fn binding_collision_exceptions_require_the_operation_specific_namespace_state() {
        let path = "/cases/collision/name";
        let stream_collision = Action::exception(
            ActionOp::StreamWrite {
                path: path.to_owned(),
                chunks: vec![b"stream".to_vec()],
                replace: false,
            },
            PythonException::StreamError,
        );
        let blob_first = case(vec![program(
            "owner",
            vec![
                Action::ok(publication(path, b"blob")),
                stream_collision.clone(),
            ],
            &[],
        )]);
        let mut observed = observe(&blob_first);
        BehaviorOracle::verify(&blob_first, &observed).unwrap();
        observed.executions[0].results[1].error_type = Some("SessionError".to_owned());
        assert!(BehaviorOracle::verify(&blob_first, &observed).is_err());
        let absent = case(vec![program("owner", vec![stream_collision], &[])]);
        assert_eq!(
            BehaviorOracle::verify(&absent, &observe(&absent))
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );

        let stream_first = case(vec![program(
            "owner",
            vec![
                stream_roundtrip(path),
                Action::exception(publication(path, b"blob"), PythonException::StreamError),
                Action::error(exclusive(path, b"raw"), libc::ENXIO),
            ],
            &[],
        )]);
        let mut observed = stream_observation(&stream_first, &[("owner", 0, 20)]);
        BehaviorOracle::verify(&stream_first, &observed).unwrap();
        // A high-level exception cannot masquerade as the raw descriptor errno.
        stream_result(&mut observed.executions[0], 2).errno = None;
        stream_result(&mut observed.executions[0], 2).error_type = Some("StreamError".to_owned());
        assert!(BehaviorOracle::verify(&stream_first, &observed).is_err());
    }

    #[test]
    fn writer_drop_requires_release_without_commit_and_allows_exclusive_reuse() {
        let path = "/cases/drop/name";
        let mut drop = exclusive(path, b"uncommitted");
        if let ActionOp::DescriptorWrite { finish, .. } = &mut drop {
            *finish = DescriptorFinish::Drop;
        }
        let mut case = case(vec![program(
            "writer",
            vec![
                Action::ok(drop),
                Action::error(
                    ActionOp::Lookup {
                        path: path.to_owned(),
                        expected_kind: "blob".to_owned(),
                    },
                    libc::ENOENT,
                ),
                Action::ok(exclusive(path, b"reused")),
                Action::ok(ActionOp::ReadBlob {
                    path: path.to_owned(),
                    expected: b"reused".to_vec(),
                }),
            ],
            &[],
        )]);
        let mut observed = observe(&case);
        observed.executions[0].results[0].descriptor = Some(DescriptorObservation::Write {
            method: DescriptorWriteMethod::Write,
            finish: DescriptorFinish::Drop,
            terminal_results: vec![],
            dropped: true,
            reservation_released: true,
        });
        BehaviorOracle::verify(&case, &observed).unwrap();
        if let Some(DescriptorObservation::Write {
            reservation_released,
            ..
        }) = &mut observed.executions[0].results[0].descriptor
        {
            *reservation_released = false;
        }
        assert_eq!(
            BehaviorOracle::verify(&case, &observed)
                .unwrap_err()
                .signature
                .failure_class,
            FailureClass::ReservationNotReleased
        );
        if let Some(DescriptorObservation::Write {
            reservation_released,
            ..
        }) = &mut observed.executions[0].results[0].descriptor
        {
            *reservation_released = true;
        }
        // Even internally consistent claims of accidental publication are illegal.
        case.processes[0].actions[1].expected = ExpectedOutcome::Ok;
        let published = &mut observed.executions[0].results[1];
        published.outcome = "ok".to_owned();
        published.errno = None;
        assert_eq!(
            BehaviorOracle::verify(&case, &observed)
                .unwrap_err()
                .invariant,
            "namespace_linearizability"
        );
    }

    #[test]
    fn descriptor_terminal_error_cannot_hide_a_completed_corrupt_fixture_read() {
        let path = "/models/fixture";
        let mut case = case(vec![program(
            "reader",
            vec![Action::error(
                ActionOp::DescriptorRead {
                    path: path.to_owned(),
                    flags: libc::O_RDONLY,
                    expected: b"a".to_vec(),
                    method: DescriptorReadMethod::Read,
                    offset: 0,
                    finish: DescriptorFinish::CloseTwice,
                },
                libc::EBADF,
            )],
            &[],
        )]);
        case.read_only_fixture_paths.insert(path.to_owned());
        let mut observed = observe(&case);
        let result = &mut observed.executions[0].results[0];
        result.descriptor = Some(DescriptorObservation::Read {
            method: DescriptorReadMethod::Read,
            finish: DescriptorFinish::CloseTwice,
            terminal_results: vec![
                DescriptorTerminalResult::Ok,
                DescriptorTerminalResult::Error {
                    errno: Some(libc::EBADF),
                    error_type: "OSError".to_owned(),
                },
            ],
        });
        result.transfer = Some(TransferObservation {
            length: 1,
            digest: digest(b"a"),
            complete: true,
        });
        BehaviorOracle::verify(&case, &observed).unwrap();
        observed.executions[0].results[0]
            .transfer
            .as_mut()
            .unwrap()
            .digest = digest(b"X");
        assert_eq!(
            BehaviorOracle::verify(&case, &observed)
                .unwrap_err()
                .signature
                .failure_class,
            FailureClass::PayloadMismatch
        );
        observed.executions[0].results[0].transfer = None;
        assert_eq!(
            BehaviorOracle::verify(&case, &observed)
                .unwrap_err()
                .signature
                .failure_class,
            FailureClass::MissingTransfer
        );
        // A failure before open/read has neither terminal success nor invented payload.
        observed.executions[0].results[0].descriptor = None;
        BehaviorOracle::verify(&case, &observed).unwrap();
    }

    #[test]
    fn typed_failure_signature_ignores_attempt_identity_but_distinguishes_evidence_failures() {
        let mut case = case(vec![program(
            "reader",
            vec![Action::ok(ActionOp::StreamReadWithRetry {
                path: "/cases/original/stream".to_owned(),
                expected: b"a".to_vec(),
            })],
            &[],
        )]);
        let original = BehaviorOracle::verify(&case, &stream_observation(&case, &[])).unwrap_err();
        case.id = "fresh-attempt".to_owned();
        case.processes[0].id = "fresh-process".to_owned();
        if let ActionOp::StreamReadWithRetry { path, .. } =
            &mut case.processes[0].actions[0].operation
        {
            *path = "/cases/fresh/stream".to_owned();
        }
        let mut fresh_observation = stream_observation(&case, &[]);
        let fresh = BehaviorOracle::verify(&case, &fresh_observation).unwrap_err();
        assert_eq!(original.signature, fresh.signature);
        fresh_observation.executions[0].results[0].incarnation = Some(0);
        let zero = BehaviorOracle::verify(&case, &fresh_observation).unwrap_err();
        assert_eq!(original.invariant, zero.invariant);
        assert_ne!(original.signature, zero.signature);
        let serialized = serde_json::to_vec(&fresh.signature).unwrap();
        assert_eq!(
            serde_json::from_slice::<FailureSignature>(&serialized).unwrap(),
            fresh.signature
        );
    }

    #[test]
    fn typed_action_abort_signatures_preserve_outcomes_not_attempt_identities() {
        let mut case = case(vec![program(
            "owner",
            vec![Action::ok(ActionOp::Lookup {
                path: "/cases/abort/name".to_owned(),
                expected_kind: "blob".to_owned(),
            })],
            &[],
        )]);
        let abort = |case: &BehaviorCase, errno: i32, error_type: &str| {
            let mut observation = observe(case);
            let execution = &mut observation.executions[0];
            execution.exit_success = false;
            execution.exit_status = Some(r#"{"kind":"code","value":1}"#.to_owned());
            let result = &mut execution.results[0];
            result.outcome = "error".to_owned();
            result.errno = Some(errno);
            result.error_type = Some(error_type.to_owned());
            result.error = Some(format!("failure in attempt {}", case.id));
            BehaviorOracle::verify(case, &observation)
                .unwrap_err()
                .signature
        };
        let missing = abort(&case, libc::ENOENT, "FileNotFoundError");
        let denied = abort(&case, libc::EACCES, "PermissionError");
        assert_eq!(missing.failure_class, FailureClass::OutcomeMismatch);
        assert_ne!(missing, denied);
        assert_ne!(missing, abort(&case, libc::ENOENT, "OSError"));
        assert_ne!(missing, abort(&case, libc::EACCES, "FileNotFoundError"));
        case.id = "another-attempt".to_owned();
        case.processes[0].id = "another-process".to_owned();
        case.processes[0].access.execution_id = "another-execution".to_owned();
        assert_eq!(missing, abort(&case, libc::ENOENT, "FileNotFoundError"));
        case.processes[0].actions[0].expected = ExpectedOutcome::Error(libc::ENOENT);
        let expected_missing = abort(&case, libc::EIO, "OSError");
        case.processes[0].actions[0].expected = ExpectedOutcome::Error(libc::EACCES);
        assert_ne!(expected_missing, abort(&case, libc::EIO, "OSError"));
        let encoded = serde_json::to_vec(&missing).unwrap();
        assert_eq!(
            serde_json::from_slice::<FailureSignature>(&encoded).unwrap(),
            missing
        );
    }

    #[test]
    fn typed_action_abort_cannot_hide_a_misplaced_later_sibling() {
        let case = case(vec![
            program(
                "failed",
                vec![Action::ok(ActionOp::Lookup {
                    path: "/cases/abort/name".to_owned(),
                    expected_kind: "blob".to_owned(),
                })],
                &[],
            ),
            program(
                "sibling",
                vec![Action::ok(publication("/cases/abort/sibling", b"value"))],
                &[],
            ),
        ]);
        let mut observation = observe(&case);
        let execution = &mut observation.executions[0];
        execution.exit_success = false;
        execution.exit_status = Some(r#"{"kind":"code","value":1}"#.to_owned());
        execution.results[0].outcome = "error".to_owned();
        execution.results[0].errno = Some(libc::ENOENT);
        execution.results[0].error_type = Some("FileNotFoundError".to_owned());
        assert_eq!(
            BehaviorOracle::verify(&case, &observation)
                .unwrap_err()
                .signature
                .failure_class,
            FailureClass::OutcomeMismatch
        );
        observation.executions[1].logical_node_id = 2;
        assert_eq!(
            BehaviorOracle::verify(&case, &observation)
                .unwrap_err()
                .invariant,
            "execution_placement"
        );
    }

    #[test]
    fn self_scoped_observation_cannot_be_replayed_from_another_attempt() {
        let template = case(vec![program(
            "owner",
            vec![Action::ok(publication("/runs/self/blob", b"value"))],
            &[],
        )]);
        let earlier = template.for_attempt(7, true);
        let later = template.for_attempt(8, true);
        let stale = observe(&earlier);
        BehaviorOracle::verify(&earlier, &stale).unwrap();
        BehaviorOracle::verify(&later, &observe(&later)).unwrap();
        assert_eq!(
            BehaviorOracle::verify(&later, &stale)
                .unwrap_err()
                .invariant,
            "execution_identity"
        );
    }

    #[test]
    fn typed_peer_loss_requires_an_observed_stopped_attached_retry_reader() {
        let path = "/cases/peer/stream";
        let mut case = case(vec![
            program(
                "writer",
                vec![Action::exception(
                    ActionOp::StreamWrite {
                        path: path.to_owned(),
                        chunks: vec![b"a".to_vec(), b"b".to_vec()],
                        replace: false,
                    },
                    PythonException::StreamError,
                )],
                &[],
            ),
            program(
                "reader",
                vec![Action::ok(ActionOp::StreamReadWithRetry {
                    path: path.to_owned(),
                    expected: b"ab".to_vec(),
                })],
                &[],
            ),
        ]);
        case.failure = FailureInjection::StopProcess {
            process: "reader".to_owned(),
            phase: ProcessStopPhase::AfterContextReady,
            kill_after_ms: Some(10),
        };
        let mut observed = stream_observation(&case, &[("writer", 0, 20), ("reader", 0, 20)]);
        let writer_terminal = observed.executions[0].results.pop().unwrap();
        observed.executions[0].results = framed_records(
            writer_terminal,
            &case.processes[0].actions[0].operation,
            &[b"a".to_vec()],
            false,
        );
        let stopped = &mut observed.executions[1];
        let first_frame = stopped
            .results
            .iter()
            .position(|record| {
                matches!(
                    record.barrier,
                    Some(BarrierObservation::StreamFirstFrame { .. })
                )
            })
            .unwrap();
        stopped.results.truncate(first_frame + 1);
        stopped.exit_success = false;
        BehaviorOracle::verify(&case, &observed).unwrap();

        case.failure = FailureInjection::None;
        let mut impossible = stream_observation(&case, &[("writer", 0, 20), ("reader", 0, 20)]);
        let writer_terminal = stream_result(&mut impossible.executions[0], 0).clone();
        // Matching sent/received frames cannot justify peer loss without a stopped reader.
        impossible.executions[0].results = framed_records(
            writer_terminal,
            &case.processes[0].actions[0].operation,
            &[b"a".to_vec(), b"b".to_vec()],
            false,
        );
        let violation = BehaviorOracle::verify(&case, &impossible).unwrap_err();
        assert_eq!(violation.invariant, "namespace_linearizability");
        assert_eq!(
            violation.signature.failure_class,
            FailureClass::NamespaceHistoryConflict
        );
    }

    #[test]
    fn zero_frame_peer_loss_allows_a_writer_stopped_before_its_opened_record() {
        let path = "/cases/peer/zero-frame";
        let mut case = case(vec![
            program(
                "writer",
                vec![Action::ok(ActionOp::StreamWrite {
                    path: path.to_owned(),
                    chunks: vec![b"unsent".to_vec()],
                    replace: false,
                })],
                &[],
            ),
            program(
                "reader",
                vec![Action::exception(
                    ActionOp::StreamRead {
                        path: path.to_owned(),
                        expected: b"unsent".to_vec(),
                    },
                    PythonException::StreamError,
                )],
                &[],
            ),
        ]);
        case.failure = FailureInjection::StopProcess {
            process: "writer".to_owned(),
            phase: ProcessStopPhase::AfterContextReady,
            kill_after_ms: Some(0),
        };
        let mut observed = stream_observation(&case, &[("reader", 0, 20)]);
        observed.executions[0].results.clear();
        observed.executions[0].exit_success = false;
        BehaviorOracle::verify(&case, &observed).unwrap();
        observed.executions.reverse();
        BehaviorOracle::verify(&case, &observed).unwrap();
        case.failure = FailureInjection::None;
        assert!(BehaviorOracle::verify(&case, &observed).is_err());
    }

    #[test]
    fn first_frame_stop_waits_for_both_matched_endpoint_milestones() {
        let path = "/cases/stop-gate/stream";
        let case = case(vec![
            program(
                "writer",
                vec![Action::ok(ActionOp::StreamWrite {
                    path: path.to_owned(),
                    chunks: vec![b"first".to_vec()],
                    replace: false,
                })],
                &[],
            ),
            program(
                "reader",
                vec![Action::ok(ActionOp::StreamRead {
                    path: path.to_owned(),
                    expected: b"first".to_vec(),
                })],
                &[],
            ),
        ]);
        let observed = stream_observation(&case, &[("writer", 0, 20), ("reader", 0, 20)]);
        let ready = |observation: &CaseObservation| {
            stream_stop_ready(
                &case,
                "writer",
                ProcessStopPhase::AfterStreamFirstFrame,
                observation.executions.iter().map(|execution| {
                    (
                        execution.process.as_str(),
                        execution.results.as_slice(),
                        execution.lifecycle.as_slice(),
                    )
                }),
            )
        };
        assert!(ready(&observed));
        let mut writer_only = observed.clone();
        writer_only.executions[1].results.retain(|record| {
            !matches!(
                record.barrier,
                Some(BarrierObservation::StreamFirstFrame { .. })
            )
        });
        assert!(!ready(&writer_only));
        let mut different_session = observed.clone();
        retag_stream(&mut different_session.executions[1], 0, 21);
        assert!(!ready(&different_session));
        let mut reader_only = observed;
        reader_only.executions[0].results.retain(|record| {
            !matches!(record.barrier, Some(BarrierObservation::StreamFrame { .. }))
        });
        assert!(!ready(&reader_only));
    }

    #[test]
    fn sibling_first_frame_stop_matches_the_gated_step_not_an_earlier_stream() {
        let prior = "/cases/stop-gate/prior";
        let gated = "/cases/stop-gate/gated";
        let case = case(vec![
            program(
                "target",
                vec![Action::ok(publication(
                    "/cases/stop-gate/target",
                    b"target",
                ))],
                &[],
            ),
            program(
                "writer",
                [prior, gated]
                    .into_iter()
                    .map(|path| {
                        Action::ok(ActionOp::StreamWrite {
                            path: path.to_owned(),
                            chunks: vec![b"first".to_vec()],
                            replace: false,
                        })
                    })
                    .collect(),
                &[],
            ),
            program(
                "reader",
                vec![
                    Action::ok(ActionOp::StreamRead {
                        path: prior.to_owned(),
                        expected: b"first".to_vec(),
                    }),
                    Action::ok(ActionOp::GatedStreamRead {
                        path: gated.to_owned(),
                        expected: b"first".to_vec(),
                        observed_path: "/cases/stop-gate/observed".to_owned(),
                        retry_attach: false,
                        park_after_first_frame: false,
                    }),
                ],
                &[],
            ),
        ]);
        let observed = stream_observation(
            &case,
            &[
                ("writer", 0, 20),
                ("reader", 0, 20),
                ("writer", 1, 21),
                ("reader", 1, 21),
            ],
        );
        let ready = |observation: &CaseObservation| {
            stream_stop_ready(
                &case,
                "target",
                ProcessStopPhase::AfterSiblingStreamFirstFrame,
                observation.executions.iter().map(|execution| {
                    (
                        execution.process.as_str(),
                        execution.results.as_slice(),
                        execution.lifecycle.as_slice(),
                    )
                }),
            )
        };
        assert!(ready(&observed));
        let mut undrained_writer = observed.clone();
        undrained_writer.executions[1].results.retain(|record| {
            record.step != 1
                || matches!(
                    record.barrier,
                    Some(BarrierObservation::StreamOpened { .. })
                )
        });
        assert!(!ready(&undrained_writer));
        let mut wrong_incarnation = observed.clone();
        retag_stream(&mut wrong_incarnation.executions[1], 1, 22);
        assert!(!ready(&wrong_incarnation));
        let mut wrong_path = observed;
        for record in &mut wrong_path.executions[1].results {
            if record.step == 1 {
                record.path = prior.to_owned();
            }
        }
        assert!(!ready(&wrong_path));
    }

    #[test]
    fn retained_blob_snapshot_constrains_any_legal_race_winner_without_guessing_revisions() {
        let path = "/cases/retained/name";
        let case = case(vec![
            program("short", vec![Action::ok(publication(path, b"a"))], &[]),
            program("long", vec![Action::ok(publication(path, b"long"))], &[]),
        ]);
        let observed = observe(&case);
        let verify = |retained: BTreeMap<String, (u64, u64)>| {
            BehaviorOracle::verify_retained_blobs_with_budget(
                &case,
                &observed,
                &retained,
                &Budget::new(std::time::Duration::from_secs(60)),
            )
        };
        verify(BTreeMap::from([(path.to_owned(), (20, 1))])).unwrap();
        verify(BTreeMap::from([(path.to_owned(), (20, 4))])).unwrap();
        assert!(verify(BTreeMap::new()).is_err());
        assert!(verify(BTreeMap::from([(path.to_owned(), (20, 2))])).is_err());
        assert!(verify(BTreeMap::from([(path.to_owned(), (1, 1))])).is_err());
        assert!(
            verify(BTreeMap::from([(
                "/cases/retained/unrelated".to_owned(),
                (20, 1)
            )]))
            .is_err()
        );
    }

    #[test]
    fn retained_revisions_order_commits_hidden_behind_stream_completion() {
        let stream = "/cases/retained/stream";
        let early = "/cases/retained/early";
        let late = "/cases/retained/late";
        let mut case = case(vec![
            program(
                "writer",
                vec![
                    Action::ok(ActionOp::StreamWrite {
                        path: stream.to_owned(),
                        chunks: vec![b"frame".to_vec()],
                        replace: false,
                    }),
                    Action::ok(publication(early, b"a")),
                ],
                &[],
            ),
            program(
                "reader",
                vec![Action::ok(ActionOp::StreamRead {
                    path: stream.to_owned(),
                    expected: b"frame".to_vec(),
                })],
                &[],
            ),
            program("late", vec![Action::ok(publication(late, b"b"))], &[]),
        ]);
        case.processes[1].logical_node_id = 2;
        case.routes.push(DataRoute {
            id: "stream-route".to_owned(),
            kind: DataKind::Stream,
            edges: vec![DataEdge {
                source: 1,
                destination: 2,
                source_role: "writer".to_owned(),
                destination_role: "reader".to_owned(),
                path: stream.to_owned(),
            }],
            join_inputs: Vec::new(),
        });
        let observed = stream_observation(&case, &[("writer", 0, 20), ("reader", 0, 20)]);
        BehaviorOracle::verify_retained_blobs_with_budget(
            &case,
            &observed,
            &BTreeMap::from([(early.to_owned(), (21, 1)), (late.to_owned(), (22, 1))]),
            &Budget::new(std::time::Duration::from_secs(5)),
        )
        .unwrap();
    }

    #[test]
    fn incremental_chunk_digest_checks_prefix_across_empty_frames() {
        let chunks = [vec![], b"ab".to_vec(), vec![], b"cd".to_vec(), vec![]];
        assert_eq!(
            payload_summary(chunks.iter().map(Vec::as_slice), None),
            Some((4, digest(b"abcd")))
        );
        assert_eq!(
            payload_summary(chunks.iter().map(Vec::as_slice), Some(3)),
            Some((4, digest(b"abc")))
        );
        assert_eq!(
            payload_summary(chunks.iter().map(Vec::as_slice), Some(0)),
            Some((4, digest(b"")))
        );
    }

    #[test]
    fn read_only_quiescent_stream_probe_is_an_initial_fact() {
        let path = "/cases/recovery/persisted-stream";
        let mut case = case(vec![program(
            "persisted-path-probe",
            vec![
                Action::ok(ActionOp::Lookup {
                    path: path.to_owned(),
                    expected_kind: "stream".to_owned(),
                }),
                Action::ok(ActionOp::WaitForQuiescent {
                    path: path.to_owned(),
                }),
            ],
            &[],
        )]);
        case.read_only_fixture_paths.insert(path.to_owned());
        let mut observed = observe(&case);
        observed.executions[0].results[0].active = Some(false);
        let mut quiescent = observed.executions[0].results[0].clone();
        quiescent.step = 1;
        observed.executions[0].results[1] = quiescent;
        BehaviorOracle::verify(&case, &observed).unwrap();
    }
}

fn verify_namespace_constructively(
    initial: &NamespaceHistory<'_>,
    programs: &[Vec<NamespaceAction<'_>>],
    dependencies: &[Vec<usize>],
    revision_hints: &BTreeMap<String, u64>,
    budget: Option<&Budget>,
    retained: Option<&BTreeMap<String, (u64, u64)>>,
) -> Result<(), OracleViolation> {
    let mut state = initial.clone();
    while !namespace_history_complete(&state, programs) {
        check_budget(budget, "namespace constructive replay")?;
        // An exact revision anywhere behind a process-local or dependency
        // fence is a lower bound for every commit that can run now. Crossing
        // it would permanently make that observed authority order impossible.
        // Looking only at the current action misses a publish hidden behind a
        // read or stream completion in the same process.
        let pending_commit_key = programs
            .iter()
            .enumerate()
            .flat_map(|(process, program)| program[state.positions[process]..].iter())
            .filter_map(|action| namespace_commit_key(action, revision_hints))
            .min();
        // Try observed namespace revisions/incarnations first, never collector
        // receipt order. An unsuccessful construction falls back to all states.
        let mut candidates: Vec<(u64, usize, usize)> = Vec::new();
        for process in 0..programs.len() {
            let Some(action) = ready_action(&state, programs, dependencies, process) else {
                continue;
            };
            candidates.push((
                namespace_action_key(action, revision_hints),
                process,
                state.positions[process],
            ));
        }
        let commit_floor = pending_commit_key;
        if candidates.is_empty() {
            if let Some(next) = state.active_streams.keys().find_map(|path| {
                namespace_first_frame(&state, path).or_else(|| namespace_quiescence(&state, path))
            }) {
                state = next;
                continue;
            }
            return violation(
                "namespace_linearizability",
                "no namespace history satisfies action results, process order, dependencies and revisions",
            );
        }
        candidates.sort_unstable();
        let mut progressed = false;
        for &(key, process, _) in &candidates {
            let action = &programs[process][state.positions[process]];
            let Some(next) = namespace_transition(&state, process, action)
                .or_else(|| namespace_interrupted_open(&state, process, action))
            else {
                continue;
            };
            let appended_key = next
                .revisions
                .get(state.revisions.len())
                .and_then(|revision| *revision)
                .or_else(|| namespace_commit_key(action, revision_hints))
                .unwrap_or(key);
            if next.revisions.len() > state.revisions.len()
                && commit_floor.is_some_and(|floor| floor < appended_key)
            {
                continue;
            }
            // Appending a revision slot advances the shared authority
            // counter past every slot committed after it, so a commit must
            // never overtake a candidate ordered earlier by its evidence
            // that only an asynchronous event holds back. Two-party
            // rendezvous blocks (an attach whose counterpart has not
            // joined) consume no revision slot and are legitimately
            // overtaken; teardown starvation is not, because it strands
            // the pinned commit behind the probe that waits for it.
            state = if next.revisions.len() > state.revisions.len()
                && let Some(yielded) = namespace_async_yield(&state, &candidates, programs, key)
            {
                yielded
            } else {
                next
            };
            progressed = true;
            break;
        }
        // When every ready operation is waiting on an asynchronous stream
        // event, advance the event that makes the earliest evidence-ordered
        // operation legal. Arbitrarily advancing the first active path can
        // strand a valid history and force an exponential frontier search.
        if !progressed
            && let Some(next) = namespace_async_yield(&state, &candidates, programs, u64::MAX)
        {
            state = next;
            progressed = true;
        }
        if !progressed
            && let Some(next) = state.active_streams.keys().find_map(|path| {
                namespace_first_frame(&state, path).or_else(|| namespace_quiescence(&state, path))
            })
        {
            state = next;
            progressed = true;
        }
        if !progressed {
            return violation(
                "namespace_linearizability",
                "no namespace history satisfies action results, process order, dependencies and revisions",
            );
        }
    }
    if namespace_retained_matches(&state, retained) {
        Ok(())
    } else {
        violation(
            "namespace_linearizability",
            "final retained blob snapshot has no matching history",
        )
    }
}

// Asynchronous namespace events (endpoint teardown, first-frame delivery)
// consume no revision slot, so deferring one can never help another
// candidate — but it can permanently strand one: a probe that waits for
// teardown holds its process back while unrelated candidates append ever
// higher revision slots, until the commit behind the probe (typically an
// unlink pinned to its observed revision) can no longer fit the counter.
// Before a candidate appends a revision slot, yield instead to any
// available event that demonstrably unblocks a candidate whose observed
// evidence orders it before that commit.
fn namespace_async_yield<'a>(
    state: &NamespaceHistory<'a>,
    candidates: &[(u64, usize, usize)],
    programs: &[Vec<NamespaceAction<'a>>],
    commit_key: u64,
) -> Option<NamespaceHistory<'a>> {
    for &(key, process, _) in candidates {
        if key >= commit_key {
            break;
        }
        let action = &programs[process][state.positions[process]];
        if namespace_transition(state, process, action)
            .or_else(|| namespace_interrupted_open(state, process, action))
            .is_some()
        {
            continue;
        }
        for path in state.active_streams.keys() {
            for event in [
                namespace_quiescence(state, path),
                namespace_first_frame(state, path),
            ]
            .into_iter()
            .flatten()
            {
                if namespace_transition(&event, process, action)
                    .or_else(|| namespace_interrupted_open(&event, process, action))
                    .is_some()
                {
                    return Some(event);
                }
            }
        }
    }
    None
}
