//! Behavioral invariants checked against observed program output.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::codegen::digest;
use crate::ir::{
    ActionObservation, ActionOp, BehaviorCase, CaseObservation, ExecutionObservation,
    ExpectedOutcome, FailureInjection, LaunchFailureKind, ProcessProgram, ProcessStopPhase,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleViolation {
    pub invariant: &'static str,
    pub detail: String,
}

impl std::fmt::Display for OracleViolation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.invariant, self.detail)
    }
}

impl std::error::Error for OracleViolation {}

pub struct BehaviorOracle;

impl BehaviorOracle {
    pub fn verify(
        case: &BehaviorCase,
        observation: &CaseObservation,
    ) -> Result<(), OracleViolation> {
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
        for execution in &observation.executions {
            if !execution_ids.insert(&execution.process) {
                return violation(
                    "duplicate_execution",
                    format!("process {} was observed more than once", execution.process),
                );
            }
        }
        for program in &case.processes {
            let execution = observation
                .executions
                .iter()
                .find(|execution| execution.process == program.id)
                .ok_or_else(|| OracleViolation {
                    invariant: "missing_execution",
                    detail: format!("process {} has no execution observation", program.id),
                })?;
            if process_failure_is_expected(case, program) && !execution.exit_success {
                let mut lifecycle_only = execution.clone();
                lifecycle_only.results.clear();
                Self::verify_execution(&lifecycle_only)?;
                Self::verify_expected_process_failure(case, program, execution)?;
                continue;
            }
            Self::verify_execution(execution)?;
            let mut seen = BTreeSet::new();
            for result in &execution.results {
                if !seen.insert(result.step) {
                    return violation(
                        "duplicate_action",
                        format!("{} emitted step {} twice", program.id, result.step),
                    );
                }
            }
            if execution.results.len() != program.actions.len() {
                return violation(
                    "action_count",
                    format!(
                        "{} expected {} action results, observed {}",
                        program.id,
                        program.actions.len(),
                        execution.results.len()
                    ),
                );
            }
            let mut last_revisions = BTreeMap::<String, u64>::new();
            let mut last_mutation_revision = None;
            for (step, action) in program.actions.iter().enumerate() {
                let result = execution
                    .results
                    .iter()
                    .find(|result| result.step == step)
                    .ok_or_else(|| OracleViolation {
                        invariant: "missing_action",
                        detail: format!("{} omitted step {step}", program.id),
                    })?;
                if result.process != program.id
                    || result.action != action.operation.class().as_str()
                {
                    return violation(
                        "action_identity",
                        format!(
                            "{} step {step} reported process {:?} and action {:?}",
                            program.id, result.process, result.action
                        ),
                    );
                }
                if result.path != action.operation.path() {
                    return violation(
                        "path_consistency",
                        format!("{} step {step} reported the wrong path", program.id),
                    );
                }
                match action.expected {
                    ExpectedOutcome::Ok if result.outcome != "ok" => {
                        return violation(
                            "unexpected_error",
                            format!("{} step {step}: {:?}", program.id, result.error),
                        );
                    }
                    ExpectedOutcome::Error(expected)
                        if result.outcome != "expected_error" || result.errno != Some(expected) =>
                    {
                        return violation(
                            "typed_error",
                            format!(
                                "{} step {step} expected errno {expected}, observed {:?}",
                                program.id, result.errno
                            ),
                        );
                    }
                    ExpectedOutcome::Linearized {
                        error: expected, ..
                    } if result.outcome != "ok"
                        && (result.outcome != "expected_error"
                            || result.errno != Some(expected)) =>
                    {
                        return violation(
                            "typed_error",
                            format!(
                                "{} step {step} expected success or errno {expected}, observed {:?}",
                                program.id, result.errno
                            ),
                        );
                    }
                    ExpectedOutcome::Exception(expected)
                        if result.outcome != "expected_error"
                            || result.error_type.as_deref() != Some(expected.as_str()) =>
                    {
                        return violation(
                            "typed_exception",
                            format!(
                                "{} step {step} expected {}, observed {:?}",
                                program.id,
                                expected.as_str(),
                                result.error_type
                            ),
                        );
                    }
                    _ => {}
                }
                if result.outcome == "ok" {
                    verify_action_payload(&program.id, step, &action.operation, result)?;
                }
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
        Self::verify_linearized_groups(case, observation)?;
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
                let result = execution
                    .results
                    .iter()
                    .find(|result| result.step == step)
                    .expect("action presence checked before linearization");
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
        if !execution.exit_success && !execution.results.is_empty() {
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
        if trace.observed_bytes.len() != trace.declared_length
            || digest(&trace.observed_bytes) != trace.declared_digest
        {
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
    let expected_bytes = match action {
        ActionOp::PublishBlob { bytes, .. }
        | ActionOp::ReadBlob {
            expected: bytes, ..
        }
        | ActionOp::StreamRead {
            expected: bytes, ..
        }
        | ActionOp::StreamReadInto {
            expected: bytes, ..
        } => Some(bytes.as_slice()),
        ActionOp::StreamWrite { chunks, .. } => {
            let concatenated = chunks.concat();
            if result.length != Some(concatenated.len())
                || result.digest.as_deref() != Some(&digest(&concatenated))
            {
                return violation(
                    "stream_bytes",
                    format!("{process} step {step} changed stream bytes"),
                );
            }
            None
        }
        ActionOp::Lookup { expected_kind, .. } | ActionOp::AwaitEntry { expected_kind, .. } => {
            if result.kind.as_deref() != Some(expected_kind) || result.revision.is_none() {
                return violation(
                    "namespace_kind",
                    format!("{process} step {step} returned an inconsistent namespace node"),
                );
            }
            None
        }
        ActionOp::WaitForQuiescent { .. } => {
            if result.kind.as_deref() != Some("stream")
                || result.revision.is_none()
                || result.active != Some(false)
            {
                return violation(
                    "stream_quiescence",
                    format!("{process} step {step} did not observe a quiescent stream node"),
                );
            }
            None
        }
        ActionOp::DescriptorWrite { bytes, .. }
        | ActionOp::DescriptorRead {
            expected: bytes, ..
        } => Some(bytes.as_slice()),
        ActionOp::MappingExportClose { .. } => None,
        ActionOp::Rename { .. } | ActionOp::Unlink { .. } => {
            if result.revision.is_none() {
                return violation(
                    "namespace_revision",
                    format!("{process} step {step} omitted mutation revision"),
                );
            }
            None
        }
    };
    if let Some(expected) = expected_bytes
        && (result.length != Some(expected.len())
            || result.digest.as_deref() != Some(&digest(expected)))
    {
        return violation(
            "payload_integrity",
            format!("{process} step {step} returned the wrong length or digest"),
        );
    }
    Ok(())
}

fn violation<T>(invariant: &'static str, detail: impl Into<String>) -> Result<T, OracleViolation> {
    Err(OracleViolation {
        invariant,
        detail: detail.into(),
    })
}

fn process_failure_is_expected(case: &BehaviorCase, process: &ProcessProgram) -> bool {
    match &case.failure {
        FailureInjection::None => false,
        FailureInjection::StopProcess {
            process: target, ..
        } => target == &process.id,
        FailureInjection::LaunchFailure {
            process: target, ..
        } => target == &process.id,
        FailureInjection::KillNode { logical_node_id } => {
            *logical_node_id == process.logical_node_id
        }
        FailureInjection::StopOrchestrator => true,
    }
}
