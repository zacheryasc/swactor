//! Contextual process control: case execution, spawn/stop, failure injection.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufWriter, Write as IoWrite};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine as _;
use myelin_control_contract::{
    ContextualControlReply, ContextualEventCursor, ContextualEventsAckRequest,
    ContextualEventsRequest, ContextualExecutionView, ContextualExitStatus,
    ContextualProcessEventKind, ContextualProcessSpec, ContextualSpawnRequest,
    ContextualStopRequest, Versioned,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::budget::Budget;
use crate::campaign::{PersistedFixtureEntry, RecoveryCase};
use crate::codegen::render_case_python;
use crate::harness::{ClusterHarness, FixturePathObservation, FixturePathSnapshot};
use crate::ir::{
    AccessSpec, Action, ActionObservation, ActionOp, BehaviorCase, CaseObservation,
    ExecutionObservation, FailureInjection, LaunchFailureKind, ProcessProgram, ProcessStopPhase,
    TopologyFamily,
};
use crate::oracle::{
    BehaviorOracle, FailureClass, FailureSignature, recorded_action_failure, stream_stop_ready,
};
use crate::resources::http_json_budget;

use super::{GENERATED_LAUNCHER, HarnessProvider, POLL_INTERVAL};

const GENERATED_SOURCE_ENV: &str = "MYELIN_E2E_PROGRAM_B64";
const BOOTSTRAP_HOLD_ENV: &str = "MYELIN_E2E_HOLD_BEFORE_BOOTSTRAP";
const MAX_CONTEXTUAL_SOURCE_ENV_BYTES: usize = 48 * 1024;
const EVENT_REQUEST_SLICE: Duration = Duration::from_secs(1);
const MIN_RECONCILE_INTERVAL: Duration = Duration::from_millis(5);

/// The one immutable namespace and execution ownership map for an attempt.
/// Preparing it does not grant permission to remove any of these paths.
pub(super) struct CaseAttempt {
    id: u64,
    directory: PathBuf,
    serialization_ns: u128,
    pub(super) case: BehaviorCase,
    pub(super) cleanup_paths: Vec<String>,
    pub(super) observation: std::sync::OnceLock<CaseObservation>,
}

impl CaseAttempt {
    fn prepare(id: u64, artifacts: &Path, case: BehaviorCase) -> Result<Self, String> {
        case.validate()?;
        let cleanup_paths = case.owned_paths().into_iter().collect();
        let directory = artifacts.join(&case.id).join(format!("attempt-{id}"));
        fs::create_dir_all(directory.parent().expect("attempt has case parent"))
            .map_err(|error| format!("create case artifact directory: {error}"))?;
        fs::create_dir(&directory)
            .map_err(|error| format!("exclusively claim attempt artifact directory: {error}"))?;
        let started = Instant::now();
        write_json(&directory.join("case.json"), &case)?;
        let serialization_ns = started.elapsed().as_nanos();
        Ok(Self {
            id,
            directory,
            serialization_ns,
            case,
            cleanup_paths,
            observation: std::sync::OnceLock::new(),
        })
    }

    pub(super) fn persist_retained_blobs(
        &self,
        reply: &myelin_control_contract::RetainedBlobsReply,
    ) -> Result<(), String> {
        write_json(&self.directory.join("retained-blobs.json"), reply)
    }
}

pub(super) struct PendingAttempt {
    attempt: Arc<CaseAttempt>,
    preconditions_verified: bool,
}

impl ClusterHarness {
    pub fn last_failure_signature(&self) -> Option<&FailureSignature> {
        self.last_failure_signature.as_ref()
    }

    pub fn last_attempt_id(&self) -> Option<u64> {
        self.last_attempt_id
    }

    fn allocate_attempt_id(&mut self) -> Result<u64, String> {
        let budget = self.operation_budget().clone();
        reserve_attempt_identity(
            &self.state_dir,
            &self.config.artifacts,
            &mut self.next_attempt_id,
            &budget,
        )
    }

    /// Allocate both recovery phases before any persistence/absence probe.
    /// They share a scoped namespace but have distinct execution identities.
    pub fn prepare_recovery_attempt(
        &mut self,
        recovery: &RecoveryCase,
    ) -> Result<RecoveryCase, String> {
        self.cleanup_budget = None;
        if let Some(reason) = self.quarantine_reason() {
            return Err(format!("fixture is quarantined: {reason}"));
        }
        if recovery.pre_restart.id == recovery.post_restart.id {
            return Err("recovery phases must have distinct case identities".to_owned());
        }
        for case in [&recovery.pre_restart, &recovery.post_restart] {
            if self.active_attempts.contains_key(&case.id)
                || self.pending_attempts.contains_key(&case.id)
            {
                return Err(format!(
                    "case {} already has an owned or pending attempt",
                    case.id
                ));
            }
        }
        let namespace_id = self.allocate_attempt_id()?;
        let mut scoped = recovery.for_attempt(namespace_id);
        let pre = Arc::new(CaseAttempt::prepare(
            namespace_id,
            &self.config.artifacts,
            scoped.pre_restart.clone(),
        )?);
        let post_id = self.allocate_attempt_id()?;
        scoped.post_restart = scoped.post_restart.for_attempt(post_id, false);
        let post = Arc::new(CaseAttempt::prepare(
            post_id,
            &self.config.artifacts,
            scoped.post_restart.clone(),
        )?);
        self.pending_attempts.insert(
            scoped.pre_restart.id.clone(),
            PendingAttempt {
                attempt: pre,
                preconditions_verified: false,
            },
        );
        self.pending_attempts.insert(
            scoped.post_restart.id.clone(),
            PendingAttempt {
                attempt: post,
                preconditions_verified: false,
            },
        );
        Ok(scoped)
    }
    pub(crate) fn persistence_probe_case<'a>(
        template: &BehaviorCase,
        entries: &[PersistedFixtureEntry],
        node_ids: &[u64],
        absent_paths: impl IntoIterator<Item = &'a String>,
        probe_id: u64,
    ) -> Result<BehaviorCase, String> {
        if entries.is_empty() {
            return Err("recovery case did not declare persisted fixture entries".to_owned());
        }
        let logical_node_id = node_ids
            .first()
            .copied()
            .ok_or_else(|| "fixture has no live node for persistence probe".to_owned())?;
        let mut probe = probe_case(template, format!("{}-persistence", template.id), node_ids);
        probe.read_only_fixture_paths = entries
            .iter()
            .map(|entry| entry.path().to_owned())
            .collect();
        let mut actions = Vec::new();
        for entry in entries {
            match entry {
                PersistedFixtureEntry::Blob { path, expected } => {
                    actions.push(Action::ok(ActionOp::Lookup {
                        path: path.clone(),
                        expected_kind: "blob".to_owned(),
                    }));
                    actions.push(Action::ok(ActionOp::ReadBlob {
                        path: path.clone(),
                        expected: expected.clone(),
                    }));
                }
                PersistedFixtureEntry::QuiescentStream { path } => {
                    actions.push(Action::ok(ActionOp::Lookup {
                        path: path.clone(),
                        expected_kind: "stream".to_owned(),
                    }));
                    actions.push(Action::ok(ActionOp::WaitForQuiescent {
                        path: path.clone(),
                    }));
                }
            }
        }
        for path in absent_paths {
            if !probe.read_only_fixture_paths.insert(path.clone()) {
                return Err(format!(
                    "persisted fixture path {path} is also claimed by the next attempt"
                ));
            }
            actions.push(Action::error(
                ActionOp::Lookup {
                    path: path.clone(),
                    expected_kind: "absent".to_owned(),
                },
                libc::ENOENT,
            ));
        }
        probe.resource_bounds.max_actions = probe
            .resource_bounds
            .max_actions
            .max(u32::try_from(actions.len()).unwrap_or(u32::MAX));
        probe.processes = vec![ProcessProgram {
            id: "persisted-path-probe".to_owned(),
            logical_node_id,
            access: read_only_access(
                format!("{}-persistence-attempt-{probe_id}", template.id),
                &probe.read_only_fixture_paths,
            ),
            depends_on: Vec::new(),
            actions,
        }];
        probe.validate()?;
        Ok(probe)
    }

    pub fn snapshot_fixture_paths(
        &mut self,
        template: &BehaviorCase,
        entries: &[PersistedFixtureEntry],
        absent_case: Option<&BehaviorCase>,
    ) -> Result<FixturePathSnapshot, String> {
        let remaining = self
            .execution_budget
            .remaining("reserve recovery snapshot and namespace cleanup")?;
        let total = remaining.min(self.config.deadline.saturating_mul(2));
        let reserve = self.config.deadline.min(total / 2);
        let probe_budget = self.execution_budget.child(total.saturating_sub(reserve));
        self.cleanup_budget = Some(self.execution_budget.child(total));
        let absent_attempt = absent_case
            .map(|case| {
                let pending = self.pending_attempts.get(&case.id).ok_or_else(|| {
                    format!(
                        "case {} has no prepared attempt for fused preconditions",
                        case.id
                    )
                })?;
                if pending.attempt.case != *case {
                    return Err(format!(
                        "case {} changed after attempt preparation",
                        case.id
                    ));
                }
                Ok(Arc::clone(&pending.attempt))
            })
            .transpose()?;
        let probe_id = self.allocate_attempt_id()?;
        let probe = Self::persistence_probe_case(
            template,
            entries,
            &self.node_ids,
            absent_attempt
                .iter()
                .flat_map(|attempt| &attempt.cleanup_paths),
            probe_id,
        )?;
        let observation = self.run_read_only_probe(&probe, &probe_budget)?;
        let snapshot = persisted_fixture_snapshot(entries, &observation)?;
        probe_budget.check("complete recovery namespace snapshot")?;
        if let Some(attempt) = absent_attempt {
            self.pending_attempts
                .get_mut(&attempt.case.id)
                .expect("pending attempt remains installed during read-only probe")
                .preconditions_verified = true;
        }
        Ok(snapshot)
    }
    /// Recover persisted facts from the verified actions of the retained
    /// segment itself. This avoids launching a redundant observer process.
    pub fn retained_fixture_snapshot(
        &self,
        case: &BehaviorCase,
        entries: &[PersistedFixtureEntry],
    ) -> Result<FixturePathSnapshot, String> {
        let attempt = self
            .active_attempts
            .get(&case.id)
            .filter(|attempt| attempt.case == *case)
            .ok_or_else(|| format!("case {} has no exact retained attempt", case.id))?;
        let observation = attempt
            .observation
            .get()
            .ok_or_else(|| format!("case {} has no verified retained observation", case.id))?;
        persisted_fixture_snapshot(entries, observation)
    }

    pub fn abort_case_processes(&self, case: &BehaviorCase) {
        let Some(owned_case) = self.active_attempts.get(&case.id) else {
            return;
        };
        let requests = owned_case
            .case
            .processes
            .iter()
            .map(|process| (&owned_case.case).execution_request_id(process))
            .collect::<Vec<_>>();
        let errors = self.stop_owned_processes(&requests, self.operation_budget());
        if !errors.is_empty() {
            let _ = write_json(
                &self
                    .config
                    .artifacts
                    .join(&case.id)
                    .join("abort-errors.json"),
                &json!({"schema_version": 1, "errors": errors}),
            );
        }
    }

    pub fn run_case(&mut self, case: &BehaviorCase) -> Result<CaseObservation, String> {
        self.execute_with_failure_cleanup(case, true, true, true)
            .map(|(_, observation)| observation)
    }

    /// Return the exact attempt-scoped IR alongside its verified observation.
    pub fn run_case_with_identity(
        &mut self,
        case: &BehaviorCase,
    ) -> Result<(BehaviorCase, CaseObservation), String> {
        self.execute_with_failure_cleanup(case, true, true, true)
            .map(|(attempt, observation)| {
                let case = Arc::try_unwrap(attempt)
                    .map_or_else(|attempt| attempt.case.clone(), |attempt| attempt.case);
                (case, observation)
            })
    }

    /// Run an unchanged phase from `prepare_recovery_attempt`, retaining its
    /// namespace ownership and sealing its verified observation.
    pub fn run_case_retained(&mut self, case: &BehaviorCase) -> Result<(), String> {
        self.run_case_retained_inner(case, true)
    }

    /// Retain one phase of a batched recovery campaign. The caller must seal
    /// the aggregate retained baseline before restarting or running more work.
    pub fn run_case_retained_deferred(&mut self, case: &BehaviorCase) -> Result<(), String> {
        self.run_case_retained_inner(case, false)
    }

    fn run_case_retained_inner(
        &mut self,
        case: &BehaviorCase,
        seal_baseline: bool,
    ) -> Result<(), String> {
        let (attempt, observation) =
            self.execute_with_failure_cleanup(case, false, false, seal_baseline)?;
        let result = attempt
            .observation
            .set(observation)
            .map_err(|_| "retained attempt observation was already sealed".to_owned())
            .and_then(|()| {
                if seal_baseline {
                    self.seal_retained_attempts()
                } else {
                    Ok(())
                }
            });
        if let Err(error) = result {
            let cleanup = self.cleanup_case(case);
            return Err(match cleanup {
                Ok(()) => error,
                Err(cleanup) => format!("{error}; retained cleanup failed: {cleanup}"),
            });
        }
        Ok(())
    }

    pub fn seal_retained_attempts(&self) -> Result<(), String> {
        let baseline = self.restore_owned_baseline(None)?;
        for attempt in self.active_attempts.values() {
            if attempt.observation.get().is_some() {
                write_json(&attempt.directory.join("retained-baseline.json"), &baseline)?;
            }
        }
        Ok(())
    }

    /// Run an already prepared recovery phase, preserving its scoped paths.
    /// Arbitrary templates are rejected; this is not a preflight bypass.
    pub fn run_case_unscoped(&mut self, case: &BehaviorCase) -> Result<CaseObservation, String> {
        self.execute_with_failure_cleanup(case, true, false, true)
            .map(|(_, observation)| observation)
    }

    fn execute_with_failure_cleanup(
        &mut self,
        case: &BehaviorCase,
        cleanup: bool,
        isolate_paths: bool,
        restore_baseline: bool,
    ) -> Result<(Arc<CaseAttempt>, CaseObservation), String> {
        self.last_failure_signature = None;
        self.last_attempt_id = None;
        self.cleanup_budget = None;
        if let Some(reason) = self.quarantine_reason() {
            return Err(format!("fixture is quarantined: {reason}"));
        }
        if self.active_attempts.contains_key(&case.id) {
            return Err(format!("case {} still owns an uncleaned attempt", case.id));
        }
        let prepared = if isolate_paths {
            if self.pending_attempts.contains_key(&case.id) {
                return Err(format!(
                    "case {} already has a prepared recovery attempt",
                    case.id
                ));
            }
            let id = self.allocate_attempt_id()?;
            PendingAttempt {
                attempt: Arc::new(CaseAttempt::prepare(
                    id,
                    &self.config.artifacts,
                    case.for_attempt(id, true),
                )?),
                preconditions_verified: false,
            }
        } else {
            let pending = self.pending_attempts.get(&case.id).ok_or_else(|| {
                format!(
                    "case {} must be prepared as a recovery attempt before unscoped execution",
                    case.id
                )
            })?;
            if pending.attempt.case != *case {
                return Err(format!(
                    "case {} changed after attempt preparation",
                    case.id
                ));
            }
            self.pending_attempts
                .remove(&case.id)
                .expect("pending attempt checked")
        };
        let attempt = prepared.attempt;
        self.last_attempt_id = Some(attempt.id);
        // Allocate once, before dispatch. Cancellation belongs to the narrower
        // execution child, never to the parent that owns the cleanup reserve.
        let parent = self.execution_budget.clone();
        let remaining = parent.remaining("reserve attempt execution and cleanup")?;
        let total = remaining.min(self.config.deadline.saturating_mul(2));
        let reserve = self.config.deadline.min(total / 2);
        let cleanup_budget = parent.child(total);
        self.execution_budget = parent.child(total.saturating_sub(reserve));
        let started = Instant::now();
        let baseline = if restore_baseline {
            self.take_or_restore_local_baseline().and_then(|baseline| {
                write_json(&attempt.directory.join("resource-baseline.json"), &baseline)
            })
        } else {
            Ok(())
        };
        let result = baseline.and_then(|()| {
            self.execute_case(&attempt, prepared.preconditions_verified, &cleanup_budget)
        });
        if let Err(error) = &result {
            if error.contains("fixture quarantine:") {
                self.quarantine(error.clone());
            }
        }
        self.execution_budget.cancel();
        self.execution_budget = parent;
        self.cleanup_budget = Some(cleanup_budget);
        let cleanup_started = Instant::now();
        let cleanup_result = if cleanup || result.is_err() {
            self.cleanup_case(case)
        } else {
            Ok(())
        };
        if cleanup_result.is_err() {
            self.last_failure_signature = None;
        }
        let evidence = write_json(
            &attempt.directory.join("attempt-timing.json"),
            &json!({
                "schema_version": 1,
                "attempt_id": attempt.id,
                "elapsed_ns": started.elapsed().as_nanos(),
                "cleanup_ns": cleanup_started.elapsed().as_nanos(),
                "execution_allocation_ns": total.saturating_sub(reserve).as_nanos(),
                "cleanup_reserved_ns": reserve.as_nanos(),
                "execution_error": result.as_ref().err(),
                "cleanup_error": cleanup_result.as_ref().err(),
            }),
        );
        match (result, cleanup_result, evidence) {
            (Ok(observation), Ok(()), Ok(())) => Ok((attempt, observation)),
            (result, cleanup, evidence) => {
                let mut errors = Vec::new();
                if let Err(error) = result {
                    errors.push(error);
                }
                if let Err(error) = cleanup {
                    errors.push(format!(
                        "fixture quarantine: terminal cleanup failed: {error}"
                    ));
                }
                if let Err(error) = evidence {
                    self.last_failure_signature = None;
                    errors.push(error);
                }
                Err(errors.join("; "))
            }
        }
    }

    fn execute_case(
        &mut self,
        attempt: &Arc<CaseAttempt>,
        preconditions_verified: bool,
        cleanup_budget: &Budget,
    ) -> Result<CaseObservation, String> {
        let started = Instant::now();
        let case = &attempt.case;
        let case_dir = &attempt.directory;
        let mut timings = BTreeMap::new();
        timings.insert("serialization_ns", attempt.serialization_ns);
        let mut trackers = case
            .processes
            .iter()
            .map(|process| {
                ExecutionTracker::new(
                    process,
                    (case).execution_request_id(process),
                    stop_injection(case, process),
                )
            })
            .collect::<Vec<_>>();
        let mut event_log = BufWriter::new(
            fs::File::create(case_dir.join("execution-events.jsonl"))
                .map_err(|error| format!("create execution event log: {error}"))?,
        );
        let execution = (|| {
            self.operation_budget().check("validate attempt")?;
            case.validate()?;
            let fixture_nodes = self.node_ids.iter().copied().collect::<BTreeSet<_>>();
            if case.live_nodes != fixture_nodes {
                return Err(format!(
                    "case live set {:?} differs from fixture live set {fixture_nodes:?}",
                    case.live_nodes
                ));
            }
            let stage = Instant::now();
            if !preconditions_verified {
                self.probe_absent_paths(attempt).map_err(|error| {
                    format!(
                        "fixture quarantine: attempt {} absence precondition failed: {error}",
                        attempt.id
                    )
                })?;
            }
            // Only a successful absence proof grants namespace cleanup
            // ownership. In particular an unexpected existing path remains
            // untouched even when the proof or its transport fails.
            self.active_attempts
                .insert(case.id.clone(), Arc::clone(attempt));
            timings.insert("precondition_ns", stage.elapsed().as_nanos());
            let stage = Instant::now();
            let mut sources = Vec::with_capacity(case.processes.len());
            for process in &case.processes {
                let remaining = self
                    .operation_budget()
                    .remaining("prepare generated source")?;
                let source = render_case_python(&case, process, remaining);
                fs::write(case_dir.join(format!("{}.py", process.id)), &source)
                    .map_err(|error| format!("write generated program {}: {error}", process.id))?;
                sources.push(source);
            }
            timings.insert("source_preparation_ns", stage.elapsed().as_nanos());
            let stage = Instant::now();
            let errors = self.schedule_case(
                &case,
                &sources,
                &mut trackers,
                &mut event_log,
                started,
                self.operation_budget(),
            );
            timings.insert("spawn_and_collection_ns", stage.elapsed().as_nanos());
            if !errors.is_empty() {
                return Err(errors.join("\n--- sibling failure ---\n"));
            }
            Ok(())
        })();
        // Every scoped transport worker has a cancellable owner and has joined
        // before cleanup can touch namespace state. Drain original cursors under
        // the reserved owner, retaining both failure and unwind evidence.
        let mut errors = execution.err().into_iter().collect::<Vec<_>>();
        if !errors.is_empty() {
            let stage = Instant::now();
            let requests = trackers
                .iter()
                .filter(|state| state.dispatched && !state.terminal)
                .map(|state| state.request_id.clone())
                .collect::<Vec<_>>();
            errors.extend(self.stop_owned_processes(&requests, cleanup_budget));
            errors.extend(self.drain_owned_executions(
                &mut trackers,
                cleanup_budget,
                &mut event_log,
                started,
            ));
            timings.insert("failure_stop_and_drain_ns", stage.elapsed().as_nanos());
            if trackers
                .iter()
                .any(|state| state.dispatched && !state.terminal)
            {
                let reason = "fixture quarantine: owned executions lack terminal retraction proof"
                    .to_owned();
                self.quarantine(reason.clone());
                errors.push(reason);
            }
        }
        let observation = CaseObservation {
            case_id: case.id.clone(),
            executions: trackers
                .iter()
                .filter(|state| state.dispatched)
                .map(ExecutionTracker::partial_observation)
                .collect(),
        };
        if errors.is_empty() {
            let stage = Instant::now();
            if let Err(error) =
                BehaviorOracle::verify_with_budget(&case, &observation, self.operation_budget())
            {
                errors.push(error.to_string());
                if !matches!(
                    error.signature.failure_class,
                    FailureClass::BudgetExceeded
                        | FailureClass::MissingEvidence
                        | FailureClass::InvalidEvidence
                ) {
                    self.last_failure_signature = Some(error.signature);
                }
            }
            if let Err(error) = self.operation_budget().check("complete behavioral oracle") {
                self.last_failure_signature = None;
                errors.push(error);
            }
            timings.insert("oracle_ns", stage.elapsed().as_nanos());
        }
        if let Err(error) = event_log.flush() {
            errors.push(format!("flush original execution records: {error}"));
            self.last_failure_signature = None;
        }
        if let Err(error) = write_json(&case_dir.join("observed.json"), &observation) {
            errors.push(error);
            self.last_failure_signature = None;
        }
        if let Err(error) = write_json(
            &case_dir.join("pending-executions.json"),
            &json!({
                "schema_version": 1,
                "errors": errors,
                "executions": trackers.iter().map(ExecutionTracker::pending_evidence).collect::<Vec<_>>(),
            }),
        ) {
            errors.push(error);
            self.last_failure_signature = None;
        }
        timings.insert("total_execution_ns", started.elapsed().as_nanos());
        if let Err(error) = write_json(
            &case_dir.join("stage-timings.json"),
            &json!({
                "schema_version": 2, "stages_ns": timings,
                "executions": trackers.iter().map(ExecutionTracker::timing_evidence).collect::<Vec<_>>(),
            }),
        ) {
            errors.push(error);
            self.last_failure_signature = None;
        }
        if !errors.is_empty() {
            let fleet = http_json_budget(
                "GET",
                &format!("{}/api/control/fleet", self.base_url),
                None,
                &cleanup_budget.child(EVENT_REQUEST_SLICE),
            );
            let telemetry = self
                .telemetry_census
                .lock()
                .map(|census| census.snapshot())
                .map_err(|_| "telemetry census is poisoned");
            let diagnostics = self.orchestrator_diagnostics();
            if let Err(error) = write_json(
                &case_dir.join("failure-state.json"),
                &json!({
                    "schema_version": 1, "attempt_id": attempt.id,
                    "model": "case.json", "processes": "pending-executions.json",
                    "fleet": fleet, "recent_telemetry": telemetry,
                    "orchestrator": diagnostics,
                }),
            ) {
                errors.push(error);
                self.last_failure_signature = None;
            }
        }
        if errors.is_empty() {
            Ok(observation)
        } else {
            Err(errors.join("\n--- sibling failure ---\n"))
        }
    }

    pub fn cleanup_case(&mut self, case: &BehaviorCase) -> Result<(), String> {
        let result = self.cleanup_case_inner(case);
        if let Err(error) = &result {
            self.quarantine(format!("case {} cleanup failed: {error}", case.id));
        }
        result
    }
    /// Atomically clean all attempt-scoped recovery namespaces, then prove one
    /// exact fixture baseline after every path is absent.
    pub fn cleanup_recovery_cases(&mut self, cases: &[&BehaviorCase]) -> Result<(), String> {
        let result = self.cleanup_recovery_cases_inner(cases);
        if let Err(error) = &result {
            self.quarantine(format!("recovery campaign cleanup failed: {error}"));
        }
        result
    }

    fn cleanup_recovery_cases_inner(&mut self, cases: &[&BehaviorCase]) -> Result<(), String> {
        if cases.is_empty() {
            return Err("recovery cleanup requires at least one owned case".to_owned());
        }
        let mut attempts = Vec::with_capacity(cases.len());
        let mut owners = BTreeMap::<String, String>::new();
        for case in cases {
            self.pending_attempts.remove(&case.id);
            let attempt = self
                .active_attempts
                .get(&case.id)
                .filter(|attempt| attempt.case == **case)
                .map(Arc::clone)
                .ok_or_else(|| format!("recovery case {} has no exact active attempt", case.id))?;
            for path in &attempt.cleanup_paths {
                if let Some(prior) = owners.insert(path.clone(), case.id.clone()) {
                    return Err(format!(
                        "recovery cases {prior} and {} share cleanup path {path}",
                        case.id
                    ));
                }
            }
            attempts.push(attempt);
        }
        let pending = self.pending_control_processes.lock().map_err(|_| {
            "owned execution tracking is poisoned; refusing recovery cleanup".to_owned()
        })?;
        if !pending.is_empty() {
            return Err(format!(
                "owned executions lack terminal retraction proof; refusing recovery cleanup: {pending:?}"
            ));
        }
        drop(pending);

        let paths = owners.keys().cloned().collect::<Vec<_>>();
        let request_prefix = format!(
            "recovery-campaign-cleanup-{}-{}",
            self.config.seed,
            self.next_observation_generation()
        );
        let mut reply = myelin_control_contract::NamespaceCleanupReply {
            schema_version: myelin_control_contract::SCHEMA_VERSION,
            request_id: request_prefix.clone(),
            paths: Vec::with_capacity(paths.len()),
        };
        let requests = paths
            .chunks(16)
            .enumerate()
            .map(|(chunk_index, chunk)| {
                let request_id = format!("{request_prefix}-{chunk_index}");
                let request = myelin_control_contract::NamespaceCleanupRequest {
                    schema_version: myelin_control_contract::SCHEMA_VERSION,
                    request_id: request_id.clone(),
                    paths: chunk.to_vec(),
                };
                serde_json::to_value(request)
                    .map(|value| (chunk_index, request_id, value))
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let budget = self.operation_budget().clone();
        let cleanup_url = format!("{}/api/control/contextual/namespace-cleanup", self.base_url);
        let chunk_results = requests
            .iter()
            .map(|(_, _, request)| {
                http_json_budget("POST", &cleanup_url, Some(request.clone()), &budget).and_then(
                    |reply| {
                        serde_json::from_value(reply).map_err(|error| {
                            format!("decode batched namespace cleanup proof: {error}")
                        })
                    },
                )
            })
            .collect::<Vec<_>>();
        for ((chunk_index, request_id, _), chunk_result) in requests.iter().zip(chunk_results) {
            let chunk_reply: myelin_control_contract::NamespaceCleanupReply = chunk_result?;
            if chunk_reply.schema_version != myelin_control_contract::SCHEMA_VERSION
                || chunk_reply.request_id != *request_id
            {
                return Err("batched namespace cleanup proof identity mismatch".to_owned());
            }
            write_json(
                &self
                    .config
                    .artifacts
                    .join(format!("recovery-namespace-cleanup-{chunk_index}.json")),
                &chunk_reply,
            )?;
            reply.paths.extend(chunk_reply.paths);
        }
        let stdout = reply
            .paths
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("serialize batched namespace cleanup proof: {error}"))?
            .join("\n");
        verify_cleanup_paths(&paths, &stdout)?;
        self.mark_health_boundary()?;
        let baseline = self.restore_health(&[])?;

        for attempt in &attempts {
            let owned = attempt.cleanup_paths.iter().collect::<BTreeSet<_>>();
            let proof = myelin_control_contract::NamespaceCleanupReply {
                schema_version: reply.schema_version,
                request_id: reply.request_id.clone(),
                paths: reply
                    .paths
                    .iter()
                    .filter(|record| owned.contains(&record.path))
                    .cloned()
                    .collect(),
            };
            write_json(&attempt.directory.join("namespace-cleanup.json"), &proof)?;
            write_json(&attempt.directory.join("cleanup-baseline.json"), &baseline)?;
        }
        for case in cases {
            self.active_attempts.remove(&case.id);
        }
        self.remember_local_baseline(&baseline);
        Ok(())
    }

    fn take_or_restore_local_baseline(&mut self) -> Result<Value, String> {
        let Some(baseline) = self.verified_local_baseline.take() else {
            return self.restore_owned_baseline(None);
        };
        if !matches!(self.config.provider, HarnessProvider::LocalMock)
            || !self.active_attempts.is_empty()
            || !self
                .pending_control_processes
                .lock()
                .map_err(|_| "owned execution tracking is poisoned".to_owned())?
                .is_empty()
        {
            return self.restore_owned_baseline(None);
        }
        let cursor = self.control_revision(self.operation_budget())?;
        if baseline["control_revision"] == cursor {
            Ok(baseline)
        } else {
            self.restore_owned_baseline(None)
        }
    }

    fn remember_local_baseline(&mut self, baseline: &Value) {
        if matches!(self.config.provider, HarnessProvider::LocalMock) {
            self.verified_local_baseline = Some(baseline.clone());
        }
    }

    fn cleanup_case_inner(&mut self, case: &BehaviorCase) -> Result<(), String> {
        // A prepared-but-unadmitted attempt owns no namespace resources.
        self.pending_attempts.remove(&case.id);
        // Ownership is removed only after successful cleanup. Repeated
        // terminal-path cleanup must not launch another request for an
        // already-cleaned attempt, or fall back to its unscoped template.
        let Some(attempt) = self.active_attempts.get(&case.id).map(Arc::clone) else {
            let baseline = self.restore_owned_baseline(None)?;
            self.remember_local_baseline(&baseline);
            return Ok(());
        };
        {
            let pending = self.pending_control_processes.lock().map_err(|_| {
                "owned execution tracking is poisoned; refusing namespace cleanup".to_owned()
            })?;
            if !pending.is_empty() {
                return Err(format!(
                    "owned executions lack terminal retraction proof; refusing namespace unlink: {pending:?}"
                ));
            }
        }
        let paths = &attempt.cleanup_paths;
        if paths.is_empty() {
            self.mark_health_boundary()?;
            let baseline = self.restore_owned_baseline(Some(&case.id))?;
            write_json(&attempt.directory.join("cleanup-baseline.json"), &baseline)?;
            self.active_attempts.remove(&case.id);
            self.remember_local_baseline(&baseline);
            return Ok(());
        }
        let budget = self.operation_budget().clone();
        let request_id = format!(
            "{}-cleanup-{}-{}",
            case.id,
            case.seed,
            self.next_observation_generation()
        );
        let request = myelin_control_contract::NamespaceCleanupRequest {
            schema_version: myelin_control_contract::SCHEMA_VERSION,
            request_id: request_id.clone(),
            paths: paths.clone(),
        };
        let reply: myelin_control_contract::NamespaceCleanupReply =
            serde_json::from_value(http_json_budget(
                "POST",
                &format!("{}/api/control/contextual/namespace-cleanup", self.base_url),
                Some(serde_json::to_value(&request).map_err(|error| error.to_string())?),
                &budget,
            )?)
            .map_err(|error| format!("decode typed namespace cleanup proof: {error}"))?;
        if reply.schema_version != myelin_control_contract::SCHEMA_VERSION
            || reply.request_id != request_id
        {
            return Err("namespace cleanup proof identity mismatch".to_owned());
        }
        let stdout = reply
            .paths
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("serialize namespace cleanup proof: {error}"))?
            .join("\n");
        verify_cleanup_paths(paths, &stdout)?;
        self.mark_health_boundary()?;
        let baseline = self.restore_owned_baseline(Some(&case.id))?;
        let (cleanup_evidence, baseline_evidence) = thread::scope(|scope| {
            let cleanup = scope
                .spawn(|| write_json(&attempt.directory.join("namespace-cleanup.json"), &reply));
            let health = scope
                .spawn(|| write_json(&attempt.directory.join("cleanup-baseline.json"), &baseline));
            (cleanup.join(), health.join())
        });
        cleanup_evidence.map_err(|_| "cleanup evidence writer panicked".to_owned())??;
        baseline_evidence.map_err(|_| "baseline evidence writer panicked".to_owned())??;
        self.active_attempts.remove(&case.id);
        self.remember_local_baseline(&baseline);
        Ok(())
    }

    fn spawn_program_budget(
        &self,
        process: &ProcessProgram,
        request_id: &str,
        source: &str,
        hold_before_bootstrap: bool,
        launch_failure: Option<LaunchFailureKind>,
        budget: &Budget,
    ) -> Result<(), String> {
        budget.check("compress contextual program")?;
        let mut compressor =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        compressor
            .write_all(source.as_bytes())
            .map_err(|error| format!("compress generated Python source: {error}"))?;
        let compressed = compressor
            .finish()
            .map_err(|error| format!("finish generated Python compression: {error}"))?;
        let encoded = base64::engine::general_purpose::STANDARD.encode(compressed);
        if encoded.len() > MAX_CONTEXTUAL_SOURCE_ENV_BYTES {
            return Err(format!(
                "compressed generated Python source is {} bytes; control limit is {}",
                encoded.len(),
                MAX_CONTEXTUAL_SOURCE_ENV_BYTES
            ));
        }
        let mut env = BTreeMap::from([(GENERATED_SOURCE_ENV.to_owned(), encoded)]);
        if hold_before_bootstrap {
            env.insert(BOOTSTRAP_HOLD_ENV.to_owned(), "1".to_owned());
        }
        let (command, args) = match launch_failure {
            Some(LaunchFailureKind::EmptyCommand) => (String::new(), Vec::<String>::new()),
            Some(LaunchFailureKind::MissingExecutable) => (
                "/definitely/missing/myelin-e2e-executable".to_owned(),
                Vec::new(),
            ),
            _ => (
                GENERATED_LAUNCHER.to_owned(),
                vec![
                    "--source-env-zlib".to_owned(),
                    GENERATED_SOURCE_ENV.to_owned(),
                ],
            ),
        };
        let execution_id = if launch_failure == Some(LaunchFailureKind::MalformedExecutionIdentity)
        {
            String::new()
        } else {
            process.access.execution_id.clone()
        };
        let attach_timeout_ms =
            u64::try_from(budget.remaining("attach contextual program")?.as_millis())
                .unwrap_or(u64::MAX)
                .max(1);
        let spawn_url = format!("{}/api/control/contextual/spawn", self.base_url);
        let spawn_body = serde_json::to_value(ContextualSpawnRequest {
            logical_node_id: process.logical_node_id,
            request_id: request_id.to_owned(),
            spec: ContextualProcessSpec {
                command,
                args,
                env,
                working_dir: None,
                label: Some(process.id.clone()),
                execution_id,
                read_prefixes: process.access.read_prefixes.clone(),
                write_prefixes: process.access.write_prefixes.clone(),
                attach_timeout_ms,
                staged_program: None,
            },
        })
        .map_err(|error| format!("serialize contextual spawn request: {error}"))?;
        // A request is never retried: an ambiguous reply is reconciled through
        // the already registered execution identity, not another spawn.
        let reply = match http_json_budget("POST", &spawn_url, Some(spawn_body), budget) {
            Ok(reply) => reply,
            Err(_)
                if self.contextual_execution_exists(
                    request_id,
                    process.logical_node_id,
                    budget,
                ) =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let reply = serde_json::from_value::<ContextualControlReply>(reply)
            .map_err(|error| format!("decode contextual spawn reply: {error}"))?;
        let accepted = matches!(
            &reply,
            ContextualControlReply::Event { observation }
                if observation.request_id == request_id
                    && observation.logical_node_id == process.logical_node_id
                    && (matches!(
                        &observation.event,
                        ContextualProcessEventKind::Spawned { .. }
                    ) || (launch_failure.is_some()
                        && matches!(
                            &observation.event,
                            ContextualProcessEventKind::SpawnRejected { .. }
                        )))
        );
        if !accepted {
            return Err(format!("contextual spawn was not accepted: {reply:?}"));
        }
        Ok(())
    }
    fn contextual_execution_exists(
        &self,
        request_id: &str,
        logical_node_id: u64,
        budget: &Budget,
    ) -> bool {
        let started = Instant::now();
        let Ok(reply) = http_json_budget(
            "GET",
            &format!(
                "{}/api/control/contextual/{request_id}/events?after_sequence=0",
                self.base_url
            ),
            None,
            budget,
        ) else {
            return false;
        };
        // This reconciliation GET may already contain terminal output. Retain
        // its original reply too, even if the attempt expires before the main
        // collector can request the same cursor again.
        let Ok(mut log) = self.probe_event_log(request_id, "spawn-reconciliation") else {
            return false;
        };
        if write_record(
            &mut log,
            &WireEvidence {
                elapsed_ns: started.elapsed().as_nanos(),
                request_ns: started.elapsed().as_nanos(),
                response: &reply,
            },
        )
        .and_then(|()| log.flush().map_err(|error| error.to_string()))
        .is_err()
        {
            return false;
        }
        serde_json::from_value::<ContextualControlReply>(reply).is_ok_and(|reply| {
            matches!(
                reply,
                ContextualControlReply::Events { execution }
                    if execution.request_id == request_id
                        && execution.logical_node_id == logical_node_id
            )
        })
    }

    fn stop_process_budget(
        &self,
        request_id: &str,
        kill_after_ms: Option<u64>,
        budget: &Budget,
    ) -> Result<(), String> {
        let control_request_id = format!("stop-{request_id}");
        let request = serde_json::to_value(ContextualStopRequest {
            control_request_id: control_request_id.clone(),
            kill_after_ms,
        })
        .map_err(|error| format!("serialize contextual stop request: {error}"))?;
        let reply = http_json_budget(
            "POST",
            &format!("{}/api/control/contextual/{request_id}/stop", self.base_url),
            Some(request),
            &budget.child(self.config.deadline),
        )?;
        let reply = serde_json::from_value::<ContextualControlReply>(reply)
            .map_err(|error| format!("decode contextual stop reply: {error}"))?;
        let resolved = match &reply {
            ContextualControlReply::Event { observation }
                if observation.request_id == control_request_id =>
            {
                match &observation.event {
                    ContextualProcessEventKind::StopAccepted { .. } => true,
                    // The target can retire before its stop reaches the node.
                    // Its retained terminal cursor still provides cleanup proof.
                    ContextualProcessEventKind::StopRejected { error }
                        if error == "contextual process is not live on this node" =>
                    {
                        true
                    }
                    _ => false,
                }
            }
            _ => false,
        };
        if !resolved {
            return Err(format!("contextual stop was not accepted: {reply:?}"));
        }
        Ok(())
    }

    pub(super) fn kill_node(&self, logical_node_id: u64) -> Result<(), String> {
        let budget = self.operation_budget().child(self.config.deadline);
        http_json_budget(
            "POST",
            &format!("{}/api/control/nodes/{logical_node_id}/kill", self.base_url),
            Some(json!({
                "command_id": format!("e2e-kill-{}-{logical_node_id}", self.config.seed),
            })),
            &budget,
        )?;
        loop {
            budget.check(&format!("node {logical_node_id} stopped"))?;
            self.ensure_orchestrator_live()?;
            let fleet = http_json_budget(
                "GET",
                &format!("{}/api/control/fleet", self.base_url),
                None,
                &budget,
            )?;
            let phase = fleet
                .pointer("/FleetStatus/nodes")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .find(|node| {
                    node.get("logical_node_id").and_then(Value::as_u64) == Some(logical_node_id)
                })
                .and_then(|node| node.get("phase"))
                .and_then(Value::as_str);
            match phase {
                Some("stopped") => return Ok(()),
                Some("stop_failed") => {
                    return Err(format!("node {logical_node_id} failed to stop: {fleet}"));
                }
                _ => budget.wait(
                    POLL_INTERVAL,
                    &format!("node {logical_node_id} stopped; last phase {phase:?}"),
                )?,
            }
        }
    }

    fn schedule_case(
        &self,
        case: &BehaviorCase,
        sources: &[impl AsRef<str>],
        trackers: &mut [ExecutionTracker],
        log: &mut BufWriter<fs::File>,
        started: Instant,
        owner: &Budget,
    ) -> Vec<String> {
        let budget = owner.child(self.config.deadline);
        let (mut errors, receiver) = thread::scope(|scope| {
            let _cancel_before_join = CancelOnDrop(budget.clone());
            let (sender, receiver) = mpsc::channel::<ControlCompletion>();
            let mut controls = 0;
            let mut errors = Vec::new();
            loop {
                while let Ok(done) = receiver.try_recv() {
                    controls -= 1;
                    if let Err(error) = trackers[done.index].control_completed(done) {
                        errors.push(error);
                    }
                }
                if !errors.is_empty() {
                    break;
                }
                if let Err(error) = budget.check("DAG ready set and execution terminal cursors") {
                    errors.push(error);
                    break;
                }
                if let FailureInjection::StopProcess { process, phase, .. } = &case.failure
                    && matches!(
                        phase,
                        ProcessStopPhase::AfterStreamFirstFrame
                            | ProcessStopPhase::AfterSiblingStreamFirstFrame
                    )
                    && stream_stop_ready(
                        case,
                        process,
                        *phase,
                        trackers.iter().map(|state| {
                            (
                                state.process.as_str(),
                                state.results.as_slice(),
                                state.lifecycle.as_slice(),
                            )
                        }),
                    )
                    && let Some(state) = trackers.iter_mut().find(|state| state.process == *process)
                {
                    state.stop_due = true;
                }
                // Stop controls have reserved capacity independent of blocked
                // spawn acknowledgements, so bootstrap holds can be released.
                for (index, state) in trackers.iter_mut().enumerate() {
                    if !state.stop_due || state.stop_requested {
                        continue;
                    }
                    state.stop_requested = true;
                    state.stop_pending = true;
                    let request = state.request_id.clone();
                    let grace = state.stop_at.and_then(|(_, grace)| grace);
                    let sender = sender.clone();
                    let worker_budget = budget.child(self.config.deadline);
                    state.stop_budget = Some(worker_budget.clone());
                    controls += 1;
                    scope.spawn(move || {
                        let begin = Instant::now();
                        let result = catch_unwind(AssertUnwindSafe(|| {
                            self.stop_process_budget(&request, grace, &worker_budget)
                        }))
                        .map_err(|_| "stop worker panicked".to_owned())
                        .and_then(std::convert::identity);
                        let _ = sender.send(ControlCompletion {
                            index,
                            kind: ControlKind::Stop,
                            result,
                            elapsed: begin.elapsed(),
                        });
                    });
                }
                let completed = trackers
                    .iter()
                    .filter(|state| state.completed)
                    .map(|state| state.process.as_str())
                    .collect::<BTreeSet<_>>();
                let ready = case
                    .processes
                    .iter()
                    .enumerate()
                    .filter(|(index, process)| {
                        !trackers[*index].dispatched
                            && process
                                .depends_on
                                .iter()
                                .all(|dependency| completed.contains(dependency.as_str()))
                    })
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>();
                for index in ready {
                    let state = &mut trackers[index];
                    // Install the identity before dispatch. Event collection starts
                    // after spawn acceptance; retained cursors make that lossless.
                    match self.pending_control_processes.lock() {
                        Ok(mut pending) => {
                            pending.insert(state.request_id.clone());
                        }
                        Err(_) => {
                            errors.push(
                                "owned execution tracking is poisoned before dispatch".to_owned(),
                            );
                            break;
                        }
                    }
                    state.dispatched = true;
                    state.spawn_pending = true;
                    state.dispatched_ns = Some(started.elapsed().as_nanos());
                    let request = state.request_id.clone();
                    let process = &case.processes[index];
                    let source = sources[index].as_ref();
                    let hold = state
                        .stop_at
                        .is_some_and(|(phase, _)| phase == ProcessStopPhase::DuringBootstrap);
                    let launch_failure = match &case.failure {
                        FailureInjection::LaunchFailure {
                            process: target,
                            kind,
                        } if target == &process.id => Some(*kind),
                        _ => None,
                    };
                    let sender = sender.clone();
                    let worker_budget = budget.child(self.config.deadline);
                    state.spawn_budget = Some(worker_budget.clone());
                    controls += 1;
                    scope.spawn(move || {
                        let begin = Instant::now();
                        let result = catch_unwind(AssertUnwindSafe(|| {
                            self.spawn_program_budget(
                                process,
                                &request,
                                source,
                                hold,
                                launch_failure,
                                &worker_budget,
                            )
                        }))
                        .map_err(|_| "spawn worker panicked".to_owned())
                        .and_then(std::convert::identity);
                        let _ = sender.send(ControlCompletion {
                            index,
                            kind: ControlKind::Spawn,
                            result,
                            elapsed: begin.elapsed(),
                        });
                    });
                }
                let progress = match self.collect_executions(trackers, &budget, log, started) {
                    Ok(progress) => progress,
                    Err(error) => {
                        errors.push(error);
                        break;
                    }
                };
                for state in trackers
                    .iter_mut()
                    .filter(|state| state.terminal && !state.completed)
                {
                    if state.spawn_pending || state.stop_pending {
                        continue;
                    }
                    if let Err(error) = state.validate_terminal() {
                        errors.push(error);
                    } else if !state.exit_success
                        && !expected_process_failure(case, &state.process)
                        && !case.processes.iter().any(|program| {
                            program.id == state.process
                                && recorded_action_failure(program, &state.partial_observation())
                                    .is_some()
                        })
                    {
                        errors.push(format!(
                            "process {} failed with status {:?}\nstdout={}\nstderr={}",
                            state.process,
                            state.exit_status,
                            String::from_utf8_lossy(&state.stdout),
                            String::from_utf8_lossy(&state.stderr),
                        ));
                    } else {
                        // Expected failures and complete typed action aborts
                        // release dependencies without canceling healthy siblings.
                        state.completed = true;
                    }
                }
                if !errors.is_empty()
                    || (trackers.iter().all(|state| state.completed && state.acked)
                        && controls == 0)
                {
                    break;
                }
                if controls == 0
                    && !trackers
                        .iter()
                        .any(|state| state.dispatched && !state.completed)
                    && trackers.iter().any(|state| !state.dispatched)
                {
                    if case.processes.iter().enumerate().any(|(index, process)| {
                        !trackers[index].dispatched
                            && process.depends_on.iter().all(|dependency| {
                                trackers
                                    .iter()
                                    .any(|state| state.process == *dependency && state.completed)
                            })
                    }) {
                        continue;
                    }
                    errors.push("case dependency graph made no progress".to_owned());
                    break;
                }
                if progress {
                    thread::yield_now();
                }
                // The batch request parks on real event revisions. Only an
                // outstanding spawn or ACK needs short bounded reconciliation.
                if !progress
                    && (trackers.iter().any(|state| state.spawn_pending)
                        || trackers
                            .iter()
                            .all(|state| !state.dispatched || state.terminal))
                    && let Err(error) = budget.wait(
                        MIN_RECONCILE_INTERVAL,
                        "spawn reply or terminal acknowledgement",
                    )
                {
                    errors.push(error);
                    break;
                }
            }
            // HTTP checks shared cancellation during connect/write/read. The
            // scope therefore joins owned workers; it never detaches a stalled
            // request and calls that request canceled.
            budget.cancel();
            (errors, receiver)
        });
        // The receiver outlives scoped joins, so final acceptance/stop outcomes
        // cannot disappear when cancellation races a worker's final send.
        for done in receiver.try_iter() {
            if let Err(error) = trackers[done.index].control_completed(done) {
                errors.push(error);
            }
        }
        // No scheduler worker can dispatch these queued controls after scope
        // exit. Keep requested controls pending until their native outcome.
        for state in trackers.iter_mut().filter(|state| !state.stop_requested) {
            state.stop_due = false;
        }
        errors
    }

    fn collect_executions(
        &self,
        trackers: &mut [ExecutionTracker],
        budget: &Budget,
        log: &mut BufWriter<fs::File>,
        started: Instant,
    ) -> Result<bool, String> {
        self.ensure_orchestrator_live()?;
        budget.check("collect contextual event cursors")?;
        let cursors = trackers
            .iter()
            .filter(|state| {
                state.dispatched
                    && !state.spawn_pending
                    && !state.acked
                    && !(state.terminal && state.gap.is_some())
            })
            .map(|state| ContextualEventCursor {
                request_id: state.request_id.clone(),
                after_sequence: state.cursor,
            })
            .collect::<Vec<_>>();
        if cursors.is_empty() {
            return Ok(false);
        }
        let wait_ms = if trackers
            .iter()
            .all(|state| !state.dispatched || state.terminal)
        {
            0
        } else {
            100
        };
        let body =
            serde_json::to_value(Versioned::new(ContextualEventsRequest { cursors, wait_ms }))
                .map_err(|error| format!("serialize event cursors: {error}"))?;
        let begin = Instant::now();
        let reply = http_json_budget(
            "POST",
            &format!("{}/api/control/contextual/events", self.base_url),
            Some(body),
            &budget.child(EVENT_REQUEST_SLICE),
        );
        let elapsed = begin.elapsed().as_nanos();
        for state in trackers
            .iter_mut()
            .filter(|state| state.dispatched && !state.acked)
        {
            state.requests += 1;
            state.request_ns += elapsed;
        }
        let reply = match reply {
            Ok(reply) => reply,
            Err(error) => {
                let idle = wait_ms != 0 && error.contains("status code 504");
                for state in trackers
                    .iter_mut()
                    .filter(|state| state.dispatched && !state.acked)
                {
                    if !idle {
                        state.retries += 1;
                        state.last_transport_error = Some(error.clone());
                    }
                }
                write_record(
                    log,
                    &json!({
                        "elapsed_ns": started.elapsed().as_nanos(), "request_ns": elapsed,
                        "retry": error, "predicate": "contextual event cursor reconciliation",
                    }),
                )?;
                budget.check("reconcile failed event transport")?;
                if !idle {
                    let retries = trackers
                        .iter()
                        .map(|state| state.retries)
                        .max()
                        .unwrap_or(0)
                        .min(5);
                    let backoff = (MIN_RECONCILE_INTERVAL * (1_u32 << retries)).min(POLL_INTERVAL);
                    budget.wait(backoff, "bounded event transport reconciliation")?;
                }
                return Ok(false);
            }
        };
        // Persist the original wire records, including duplicates, unknown
        // events and malformed output, before interpretation can fail.
        write_record(
            log,
            &WireEvidence {
                elapsed_ns: started.elapsed().as_nanos(),
                request_ns: elapsed,
                response: &reply,
            },
        )?;
        let reply = serde_json::from_value::<Versioned<ContextualControlReply>>(reply)
            .map_err(|error| format!("decode versioned batch events: {error}"))?
            .into_payload()?;
        let ContextualControlReply::EventsBatch(batch) = reply else {
            return Err(format!(
                "contextual event batch returned the wrong reply type: {reply:?}"
            ));
        };
        let executions = &batch.executions;
        let missing = &batch.missing;
        let mut answered = BTreeSet::new();
        let mut errors = Vec::new();
        let mut progress = false;
        for execution in executions {
            let request = execution.request_id.as_str();
            let Some(state) = trackers
                .iter_mut()
                .find(|state| state.dispatched && state.request_id == request)
            else {
                errors.push(format!("event batch returned unowned request {request}"));
                continue;
            };
            if !answered.insert(request) {
                errors.push(format!("event batch repeated request {request}"));
                continue;
            }
            let previous_cursor = state.cursor;
            let drain_started = Instant::now();
            let ingestion = state.ingest(execution, drain_started.duration_since(started));
            state.event_drain_ns += drain_started.elapsed().as_nanos();
            match ingestion {
                Ok(changed) => {
                    progress |= changed;
                    if changed {
                        state.max_event_backlog_records = state
                            .max_event_backlog_records
                            .max(state.cursor - previous_cursor);
                        // A complete prior snapshot excludes these records at
                        // some instant after its request began. This bounds
                        // their drain lag using only the runner's clock.
                        if let Some(earliest) =
                            state.last_snapshot_request_ns.or(state.dispatched_ns)
                        {
                            let bound = started.elapsed().as_nanos().saturating_sub(earliest);
                            state.event_drain_lag_upper_bound_ns =
                                Some(state.event_drain_lag_upper_bound_ns.unwrap_or(0).max(bound));
                        }
                    }
                    state.last_snapshot_request_ns = Some(begin.duration_since(started).as_nanos());
                }
                Err(error) => {
                    state.gap = Some(error.clone());
                    errors.push(error);
                }
            }
            if state.terminal {
                self.pending_control_processes
                    .lock()
                    .map_err(|_| "owned execution tracking is poisoned after terminal".to_owned())?
                    .remove(&state.request_id);
            }
        }
        for request in missing {
            let request = request.as_str();
            if !answered.insert(request) {
                errors.push(format!("event batch repeated missing request {request}"));
                continue;
            }
            match trackers.iter().find(|state| state.request_id == request) {
                Some(state) if state.terminal || state.spawn_pending || !state.accepted => {}
                Some(_) => errors.push(format!("accepted contextual execution {request} disappeared before terminal reconciliation")),
                None => errors.push(format!("event batch reported unowned missing request {request}")),
            }
        }
        for state in trackers.iter().filter(|state| {
            state.dispatched
                && !state.spawn_pending
                && !state.acked
                && !(state.terminal && state.gap.is_some())
        }) {
            if !answered.contains(state.request_id.as_str()) {
                errors.push(format!(
                    "event batch omitted requested cursor {}",
                    state.request_id
                ));
            }
        }
        if trackers.iter().any(ExecutionTracker::ack_ready) {
            log.flush()
                .map_err(|error| format!("flush terminal execution records: {error}"))?;
            log.get_ref()
                .sync_data()
                .map_err(|error| format!("persist terminal execution records: {error}"))?;
            // A stalled terminal ACK cannot serialize all siblings' event
            // drains behind one request slice per completed execution.
            let acknowledgements = thread::scope(|scope| {
                let jobs = trackers
                    .iter()
                    .enumerate()
                    .filter(|(_, state)| state.ack_ready())
                    .map(|(index, state)| {
                        scope.spawn(move || (index, self.acknowledge_execution(state, budget)))
                    })
                    .collect::<Vec<_>>();
                jobs.into_iter().map(|job| job.join()).collect::<Vec<_>>()
            });
            for acknowledgement in acknowledgements {
                match acknowledgement {
                    Ok((index, Ok(()))) => trackers[index].acked = true,
                    Ok((index, Err(error))) => {
                        trackers[index].retries += 1;
                        trackers[index].last_transport_error = Some(error);
                    }
                    Err(_) => errors.push("terminal acknowledgement worker panicked".to_owned()),
                }
            }
        }
        if errors.is_empty() {
            Ok(progress)
        } else {
            Err(errors.join("; "))
        }
    }

    fn acknowledge_execution(
        &self,
        state: &ExecutionTracker,
        budget: &Budget,
    ) -> Result<(), String> {
        let incarnation = state
            .execution_incarnation
            .as_ref()
            .ok_or_else(|| format!("terminal execution {} lacks incarnation", state.request_id))?;
        let reply = http_json_budget(
            "POST",
            &format!(
                "{}/api/control/contextual/{}/ack",
                self.base_url, state.request_id
            ),
            Some(
                serde_json::to_value(Versioned::new(ContextualEventsAckRequest {
                    through_sequence: state.cursor,
                    execution_incarnation: incarnation.clone(),
                }))
                .map_err(|error| format!("serialize terminal acknowledgement: {error}"))?,
            ),
            &budget.child(EVENT_REQUEST_SLICE),
        )?;
        let reply = serde_json::from_value::<Versioned<ContextualControlReply>>(reply)
            .map_err(|error| format!("decode terminal acknowledgement: {error}"))?
            .into_payload()?;
        let ContextualControlReply::Acknowledged(acknowledgement) = &reply else {
            return Err(format!(
                "terminal acknowledgement returned the wrong reply type: {reply:?}"
            ));
        };
        if acknowledgement.request_id != state.request_id
            || acknowledgement.execution_incarnation != *incarnation
            || acknowledgement.through_sequence != state.cursor
        {
            return Err(format!(
                "terminal acknowledgement did not cover owned cursor: {reply:?}"
            ));
        }
        Ok(())
    }

    fn stop_owned_processes(&self, requests: &[String], budget: &Budget) -> Vec<String> {
        // The IR admits at most twenty processes. Launch every independent stop
        // before waiting for any reply; leave half the remaining reserve for
        // terminal/retraction collection and namespace cleanup.
        let stop_cap = budget
            .remaining("stop all owned executions")
            .unwrap_or_default()
            / 2;
        let stop_budget = budget.child(stop_cap.min(self.config.deadline));
        thread::scope(|scope| {
            let jobs = requests
                .iter()
                .map(|request| {
                    let budget = &stop_budget;
                    scope.spawn(move || {
                        self.stop_process_budget(request, Some(0), budget)
                            .map_err(|error| format!("stop owned execution {request}: {error}"))
                    })
                })
                .collect::<Vec<_>>();
            jobs.into_iter()
                .filter_map(|job| match job.join() {
                    Ok(Ok(())) => None,
                    // A durable terminal ACK already retired this identity.
                    Ok(Err(error)) if error.contains("does not exist") => None,
                    Ok(Err(error)) => Some(error),
                    Err(_) => Some("owned execution stop worker panicked".to_owned()),
                })
                .collect()
        })
    }

    fn drain_owned_executions(
        &self,
        trackers: &mut [ExecutionTracker],
        budget: &Budget,
        log: &mut BufWriter<fs::File>,
        started: Instant,
    ) -> Vec<String> {
        let mut errors = Vec::new();
        let mut pause = MIN_RECONCILE_INTERVAL;
        while trackers.iter().any(|state| {
            state.dispatched && (!state.terminal || (!state.acked && state.gap.is_none()))
        }) {
            if let Err(error) = budget.check("terminal retraction of all owned executions") {
                errors.push(error);
                break;
            }
            match self.collect_executions(trackers, budget, log, started) {
                Ok(progress) => {
                    pause = if progress {
                        MIN_RECONCILE_INTERVAL
                    } else {
                        (pause * 2).min(POLL_INTERVAL)
                    };
                }
                Err(error) => {
                    // A gap remains failure and is NEVER acknowledged away.
                    // Continue draining every other owned execution.
                    if !errors.contains(&error) {
                        errors.push(error);
                    }
                }
            }
            if !trackers.iter().any(|state| {
                state.dispatched && (!state.terminal || (!state.acked && state.gap.is_none()))
            }) {
                break;
            }
            if let Err(error) = budget.wait(pause, "owned execution terminal/retraction progress") {
                errors.push(error);
                break;
            }
        }
        errors
    }

    fn probe_event_log(
        &self,
        request_id: &str,
        owner: &str,
    ) -> Result<BufWriter<fs::File>, String> {
        let directory = self.config.artifacts.join("control-executions");
        fs::create_dir_all(&directory)
            .map_err(|error| format!("create probe evidence directory: {error}"))?;
        let digest = format!("{:x}", Sha256::digest(request_id.as_bytes()));
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(directory.join(format!("{digest}.{owner}.jsonl")))
            .map_err(|error| format!("open probe evidence for {request_id}: {error}"))?;
        Ok(BufWriter::new(file))
    }

    fn run_owned_probe(
        &self,
        process: &ProcessProgram,
        request_id: &str,
        source: &str,
        owner: &Budget,
        work: &Budget,
    ) -> Result<ExecutionObservation, String> {
        let mut tracker = ExecutionTracker::new(process, request_id.to_owned(), None);
        let mut log = self.probe_event_log(request_id, "collector")?;
        write_record(&mut log, &ProbeSourceEvidence { request_id, source })?;
        let started = Instant::now();
        let probe = BehaviorCase {
            schema_version: crate::ir::CASE_SCHEMA_VERSION,
            generator_version: crate::ir::GENERATOR_VERSION,
            id: process.id.clone(),
            seed: self.config.seed,
            live_nodes: self.node_ids.iter().copied().collect(),
            topology: TopologyFamily::Fixed,
            scenarios: BTreeSet::new(),
            routes: Vec::new(),
            read_only_fixture_paths: BTreeSet::new(),
            resource_bounds: Default::default(),
            processes: vec![process.clone()],
            failure: FailureInjection::None,
        };
        let errors = self.schedule_case(
            &probe,
            &[source],
            std::slice::from_mut(&mut tracker),
            &mut log,
            started,
            work,
        );
        let result = if errors.is_empty() {
            tracker
                .validate_terminal()
                .map(|()| tracker.partial_observation())
        } else {
            Err(errors.join("; "))
        };
        if result.is_err() {
            work.cancel();
            let mut errors = self.stop_owned_processes(&[request_id.to_owned()], owner);
            errors.extend(self.drain_owned_executions(
                std::slice::from_mut(&mut tracker),
                owner,
                &mut log,
                started,
            ));
            write_record(
                &mut log,
                &json!({
                    "pending": tracker.pending_evidence(), "error": result.as_ref().err(), "cleanup_errors": errors,
                }),
            )?;
            if !errors.is_empty() || !tracker.terminal {
                log.flush()
                    .map_err(|error| format!("flush pending probe evidence: {error}"))?;
                return Err(format!(
                    "{}; fixture quarantine: probe terminal cleanup failed: {}",
                    result.expect_err("failure branch"),
                    errors.join("; "),
                ));
            }
        }
        log.flush()
            .map_err(|error| format!("flush fresh probe evidence: {error}"))?;
        result
    }

    fn run_read_only_probe(
        &self,
        probe: &BehaviorCase,
        budget: &Budget,
    ) -> Result<CaseObservation, String> {
        probe.validate()?;
        if !probe.owned_paths().is_empty()
            || probe.processes.len() != 1
            || !probe.processes[0].access.write_prefixes.is_empty()
        {
            return Err(
                "read-only probe cannot acquire namespace write or cleanup ownership".to_owned(),
            );
        }
        let process = &probe.processes[0];
        let request = (probe).execution_request_id(process);
        let work = reserve_probe_cleanup(budget)?;
        let source =
            render_case_python(probe, process, work.remaining("generate read-only probe")?);
        let observed = self.run_owned_probe(process, &request, &source, budget, &work)?;
        let observation = CaseObservation {
            case_id: probe.id.clone(),
            executions: vec![observed],
        };
        BehaviorOracle::verify_with_budget(probe, &observation, &work)
            .map_err(|error| error.to_string())?;
        Ok(observation)
    }

    fn probe_absent_paths(&self, attempt: &CaseAttempt) -> Result<(), String> {
        if attempt.cleanup_paths.is_empty() {
            return Ok(());
        }
        let reply =
            self.query_retained_blobs(attempt.cleanup_paths.clone(), self.operation_budget())?;
        verify_typed_absence(&reply)?;
        write_json(&attempt.directory.join("precondition-absence.json"), &reply)
    }
}

fn verify_typed_absence(reply: &myelin_control_contract::RetainedBlobsReply) -> Result<(), String> {
    let non_absent = reply
        .non_blobs
        .iter()
        .filter_map(|(path, state)| {
            (!matches!(state, myelin_control_contract::RetainedNonBlob::Absent)).then_some(path)
        })
        .cloned()
        .collect::<Vec<_>>();
    if !reply.blobs.is_empty() || !non_absent.is_empty() {
        return Err(format!(
            "typed precondition found owned paths: blobs={:?}, non_absent={non_absent:?}",
            reply.blobs.keys().collect::<Vec<_>>()
        ));
    }
    Ok(())
}

fn persisted_fixture_snapshot(
    entries: &[PersistedFixtureEntry],
    observation: &CaseObservation,
) -> Result<FixturePathSnapshot, String> {
    let mut snapshots = BTreeMap::new();
    for entry in entries {
        let path = entry.path();
        let results = observation
            .executions
            .iter()
            .flat_map(|execution| &execution.results)
            .filter(|result| result.path == path)
            .collect::<Vec<_>>();
        let namespace = results
            .iter()
            .rev()
            .find(|result| result.kind.is_some() && result.revision.is_some())
            .ok_or_else(|| format!("persistence probe omitted namespace facts for {path}"))?;
        let payload = results.iter().find(|result| result.action == "read_blob");
        let snapshot = FixturePathObservation {
            kind: namespace.kind.clone().expect("namespace kind checked"),
            revision: namespace.revision.expect("namespace revision checked"),
            active: namespace.active.ok_or_else(|| {
                format!("persistence probe omitted active-state evidence for {path}")
            })?,
            length: payload.and_then(|result| result.length),
            digest: payload.and_then(|result| result.digest.clone()),
        };
        match entry {
            PersistedFixtureEntry::Blob { .. }
                if snapshot.kind != "blob"
                    || snapshot.length.is_none()
                    || snapshot.digest.is_none() =>
            {
                return Err(format!("incomplete persisted blob facts for {path}"));
            }
            PersistedFixtureEntry::QuiescentStream { .. }
                if snapshot.kind != "stream" || snapshot.active =>
            {
                return Err(format!("persisted stream {path} is not quiescent"));
            }
            _ => {}
        }
        snapshots.insert(path.to_owned(), snapshot);
    }
    Ok(FixturePathSnapshot { entries: snapshots })
}

fn reserve_attempt_identity(
    state: &Path,
    artifacts: &Path,
    cursor: &mut u64,
    budget: &Budget,
) -> Result<u64, String> {
    // Claim in both durable fixture state and the evidence root. A retained
    // fixture with a new evidence directory, or a fresh local fixture reusing
    // its evidence directory, must never restart execution/path identities.
    let roots = [
        state.join("owned-attempt-identities"),
        artifacts.join("attempt-identities"),
    ];
    for root in &roots {
        fs::create_dir_all(root)
            .map_err(|error| format!("create attempt identity ledger: {error}"))?;
    }
    'candidate: loop {
        budget.check("reserve durable attempt identity")?;
        *cursor = cursor
            .checked_add(1)
            .ok_or_else(|| "case attempt identity exhausted".to_owned())?;
        for root in &roots {
            let claim = root.join(format!("attempt-{cursor}"));
            match fs::create_dir(&claim) {
                Ok(()) => {
                    fs::File::open(root)
                        .and_then(|directory| directory.sync_all())
                        .map_err(|error| format!("persist attempt identity claim: {error}"))?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    continue 'candidate;
                }
                Err(error) => return Err(format!("exclusively claim attempt identity: {error}")),
            }
        }
        return Ok(*cursor);
    }
}

fn read_only_access(execution_id: String, paths: &BTreeSet<String>) -> AccessSpec {
    AccessSpec {
        execution_id,
        read_prefixes: paths.iter().cloned().collect(),
        write_prefixes: Vec::new(),
    }
}

fn reserve_probe_cleanup(owner: &Budget) -> Result<Budget, String> {
    let remaining = owner.remaining("reserve fresh probe cleanup")?;
    Ok(owner.child(remaining.saturating_sub((remaining / 4).min(Duration::from_secs(5)))))
}

#[derive(serde::Serialize)]
struct ProbeSourceEvidence<'a> {
    request_id: &'a str,
    source: &'a str,
}

#[derive(serde::Serialize)]
struct WireEvidence<'a> {
    elapsed_ns: u128,
    request_ns: u128,
    response: &'a Value,
}

struct CancelOnDrop(Budget);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

enum ControlKind {
    Spawn,
    Stop,
}

struct ControlCompletion {
    index: usize,
    kind: ControlKind,
    result: Result<(), String>,
    elapsed: Duration,
}

/// One cursor owns each execution's byte stream. Arrival order between cursors
/// is deliberately never used as causal order; the Python action records carry
/// the causal identities the oracle consumes.
struct ExecutionTracker {
    process: String,
    request_id: String,
    logical_node_id: u64,
    execution_incarnation: Option<String>,
    dependencies: Vec<String>,
    dispatched: bool,
    spawn_pending: bool,
    spawn_budget: Option<Budget>,
    accepted: bool,
    completed: bool,
    cursor: u64,
    terminal: bool,
    acked: bool,
    exit_success: bool,
    exit_status: Option<String>,
    lifecycle: Vec<String>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    results: Vec<ActionObservation>,
    parsed_stdout: usize,
    output_error: Option<String>,
    gap: Option<String>,
    stop_at: Option<(ProcessStopPhase, Option<u64>)>,
    stop_due: bool,
    stop_requested: bool,
    stop_pending: bool,
    stop_budget: Option<Budget>,
    dispatched_ns: Option<u128>,
    spawn_ns: Option<u128>,
    attach_ns: Option<u128>,
    first_output_ns: Option<u128>,
    terminal_ns: Option<u128>,
    stop_ns: Option<u128>,
    requests: u64,
    retries: u64,
    request_ns: u128,
    last_snapshot_request_ns: Option<u128>,
    max_event_backlog_records: u64,
    event_drain_ns: u128,
    event_drain_lag_upper_bound_ns: Option<u128>,
    last_transport_error: Option<String>,
}

impl ExecutionTracker {
    fn new(
        process: &ProcessProgram,
        request_id: String,
        stop_at: Option<(ProcessStopPhase, Option<u64>)>,
    ) -> Self {
        Self {
            process: process.id.clone(),
            request_id,
            logical_node_id: process.logical_node_id,
            dispatched: false,
            spawn_pending: false,
            accepted: false,
            completed: false,
            cursor: 0,
            terminal: false,
            acked: false,
            exit_success: false,
            exit_status: None,
            lifecycle: Vec::new(),
            stdout: Vec::new(),
            stderr: Vec::new(),
            results: Vec::new(),
            parsed_stdout: 0,
            output_error: None,
            gap: None,
            stop_at,
            stop_due: false,
            stop_requested: false,
            stop_pending: false,
            dispatched_ns: None,
            spawn_ns: None,
            attach_ns: None,
            first_output_ns: None,
            terminal_ns: None,
            stop_ns: None,
            requests: 0,
            retries: 0,
            request_ns: 0,
            last_snapshot_request_ns: None,
            max_event_backlog_records: 0,
            event_drain_ns: 0,
            event_drain_lag_upper_bound_ns: None,
            last_transport_error: None,
            spawn_budget: None,
            stop_budget: None,
            execution_incarnation: None,
            dependencies: process.depends_on.clone(),
        }
    }

    fn ack_ready(&self) -> bool {
        self.terminal
            && !self.acked
            && self.gap.is_none()
            && !self.stop_pending
            && !(self.stop_due && !self.stop_requested)
    }

    fn control_completed(&mut self, done: ControlCompletion) -> Result<(), String> {
        match done.kind {
            ControlKind::Spawn => {
                self.spawn_pending = false;
                self.spawn_budget = None;
                self.spawn_ns = Some(done.elapsed.as_nanos());
                match done.result {
                    Ok(()) => {
                        self.accepted = true;
                        if self
                            .stop_at
                            .is_some_and(|(phase, _)| phase == ProcessStopPhase::AfterSpawn)
                        {
                            self.stop_due = true;
                        }
                    }
                    // Cursor evidence reconciles an ambiguous spawn response;
                    // it never authorizes redelivery of the spawn request.
                    Err(error) if self.cursor != 0 => {
                        self.last_transport_error = Some(error);
                        self.accepted = true;
                    }
                    Err(error) => return Err(format!("spawn process {}: {error}", self.process)),
                }
            }
            ControlKind::Stop => {
                self.stop_pending = false;
                self.stop_ns = Some(done.elapsed.as_nanos());
                self.stop_budget = None;
                if let Err(error) = done.result {
                    if self.lifecycle.iter().any(|event| event == "stop_accepted") {
                        self.last_transport_error = Some(error);
                    } else {
                        return Err(format!("stop process {}: {error}", self.process));
                    }
                }
            }
        }
        Ok(())
    }

    fn ingest(
        &mut self,
        execution: &ContextualExecutionView,
        elapsed: Duration,
    ) -> Result<bool, String> {
        if execution.request_id != self.request_id
            || execution.logical_node_id != self.logical_node_id
        {
            return Err(format!(
                "event identity differs from owned execution {}: {execution:?}",
                self.request_id
            ));
        }
        let incarnation = execution.execution_incarnation.as_str();
        if incarnation.is_empty() {
            return Err(format!(
                "{} events lack execution incarnation",
                self.request_id
            ));
        }
        if self
            .execution_incarnation
            .as_deref()
            .is_some_and(|owned| owned != incarnation)
        {
            return Err(format!(
                "execution incarnation changed for {}; retained cursor belongs to {:?}",
                self.request_id, self.execution_incarnation
            ));
        }
        self.execution_incarnation
            .get_or_insert_with(|| incarnation.to_owned());
        let truncated = execution.truncated_before;
        let next = execution.next_sequence;
        let declared_terminal = execution.terminal;
        let before = self.cursor;
        if truncated > self.cursor {
            self.gap.get_or_insert_with(|| format!(
                "contextual event history for {} was truncated before sequence {truncated}; requested {}",
                self.request_id, self.cursor,
            ));
        }
        if next < self.cursor {
            self.gap.get_or_insert_with(|| {
                format!(
                    "contextual cursor for {} regressed from {} to {next}",
                    self.request_id, self.cursor,
                )
            });
        }
        for record in &execution.events {
            let sequence = record.sequence;
            if sequence < self.cursor {
                // A reconnect may replay an acknowledged prefix. Its original
                // envelope remains in the wire log, but bytes are decoded once.
                continue;
            }
            if sequence != self.cursor {
                self.gap.get_or_insert_with(|| {
                    format!(
                        "contextual event gap for {}: expected {}, received {sequence}",
                        self.request_id, self.cursor,
                    )
                });
            }
            if sequence >= next {
                self.gap.get_or_insert_with(|| {
                    format!(
                        "contextual event {} sequence {sequence} is outside next cursor {next}",
                        self.request_id,
                    )
                });
            }
            if record.observation.request_id != self.request_id
                || record.observation.logical_node_id != self.logical_node_id
            {
                return Err(format!(
                    "contextual event record identity mismatch for {}: {record:?}",
                    self.request_id
                ));
            }
            let event = &record.observation.event;
            let kind = match event {
                ContextualProcessEventKind::Spawned { .. } => "spawned",
                ContextualProcessEventKind::SpawnRejected { .. } => "spawn_rejected",
                ContextualProcessEventKind::ProcessStarted { .. } => "process_started",
                ContextualProcessEventKind::ContextReady => "context_ready",
                ContextualProcessEventKind::Stdout { .. } => "stdout",
                ContextualProcessEventKind::Stderr { .. } => "stderr",
                ContextualProcessEventKind::SpawnFailed { .. } => "spawn_failed",
                ContextualProcessEventKind::BootstrapFailed { .. } => "bootstrap_failed",
                ContextualProcessEventKind::Exited { .. } => "exited",
                ContextualProcessEventKind::ProcessError { .. } => "process_error",
                ContextualProcessEventKind::StopAccepted { .. } => "stop_accepted",
                ContextualProcessEventKind::StopRejected { .. } => "stop_rejected",
                ContextualProcessEventKind::LiveExecutions { .. } => "live_executions",
                ContextualProcessEventKind::ControlUnavailable { .. } => "control_unavailable",
            };
            match event {
                ContextualProcessEventKind::Spawned { .. }
                | ContextualProcessEventKind::ProcessStarted { .. }
                | ContextualProcessEventKind::ContextReady => {
                    self.lifecycle.push(kind.to_owned());
                    if matches!(event, ContextualProcessEventKind::ContextReady) {
                        self.attach_ns.get_or_insert(elapsed.as_nanos());
                    }
                }
                ContextualProcessEventKind::Stdout { bytes } => {
                    self.stdout.extend_from_slice(bytes);
                    self.lifecycle.push("user_result".to_owned());
                    self.first_output_ns.get_or_insert(elapsed.as_nanos());
                    self.parse_output(false);
                }
                ContextualProcessEventKind::Stderr { bytes } => {
                    self.stderr.extend_from_slice(bytes);
                }
                ContextualProcessEventKind::Exited { status } => {
                    self.lifecycle.push(kind.to_owned());
                    self.terminal = true;
                    self.terminal_ns = Some(elapsed.as_nanos());
                    self.exit_status = Some(
                        serde_json::to_string(status)
                            .map_err(|error| format!("encode contextual exit status: {error}"))?,
                    );
                    self.exit_success = matches!(status, &ContextualExitStatus::Code(0));
                }
                ContextualProcessEventKind::SpawnFailed { .. }
                | ContextualProcessEventKind::ProcessError { .. }
                | ContextualProcessEventKind::SpawnRejected { .. } => {
                    self.lifecycle.push(kind.to_owned());
                    self.terminal = true;
                    self.terminal_ns = Some(elapsed.as_nanos());
                }
                // Bootstrap failure is followed by a real process exit. It is
                // not sufficient proof of reaping or namespace retraction.
                ContextualProcessEventKind::BootstrapFailed { .. }
                | ContextualProcessEventKind::StopAccepted { .. }
                | ContextualProcessEventKind::StopRejected { .. } => {
                    self.lifecycle.push(kind.to_owned());
                }
                ContextualProcessEventKind::LiveExecutions { .. } => {}
                ContextualProcessEventKind::ControlUnavailable { error } => {
                    return Err(format!(
                        "contextual control became unavailable for {}: {error}",
                        self.request_id
                    ));
                }
            }
            let trigger = match self.stop_at.map(|(phase, _)| phase) {
                Some(ProcessStopPhase::AfterSpawn) => Some("spawned"),
                Some(ProcessStopPhase::DuringBootstrap) => Some("process_started"),
                Some(ProcessStopPhase::AfterContextReady) => Some("context_ready"),
                Some(
                    ProcessStopPhase::AfterStreamFirstFrame
                    | ProcessStopPhase::AfterSiblingStreamFirstFrame,
                )
                | None => None,
            };
            if trigger == Some(kind) {
                self.stop_due = true;
            }
            self.cursor = sequence
                .checked_add(1)
                .ok_or_else(|| "contextual event sequence overflow".to_owned())?;
        }
        if self.cursor != next {
            self.gap.get_or_insert_with(|| {
                format!(
                    "contextual event gap for {}: drained {}, advertised next {next}",
                    self.request_id, self.cursor,
                )
            });
        }
        if declared_terminal != self.terminal {
            self.gap.get_or_insert_with(|| format!(
                "contextual terminal reconciliation gap for {}: terminal flag {declared_terminal}, terminal record {}",
                self.request_id, self.terminal,
            ));
        }
        if self.terminal {
            self.parse_output(true);
        }
        // Once the cursor proves acceptance, a withheld POST response no
        // longer owns progress. Cancel only that transport child, preserving
        // both the execution collector and every sibling's budget.
        if self.cursor != 0 {
            if let Some(budget) = &self.spawn_budget {
                budget.cancel();
            }
        }
        if self.lifecycle.iter().any(|event| event == "stop_accepted") {
            if let Some(budget) = &self.stop_budget {
                budget.cancel();
            }
        }
        if let Some(error) = &self.gap {
            Err(error.clone())
        } else {
            Ok(before != self.cursor)
        }
    }

    fn parse_output(&mut self, terminal: bool) {
        while self.parsed_stdout < self.stdout.len() {
            let remaining = &self.stdout[self.parsed_stdout..];
            let Some(length) = remaining
                .iter()
                .position(|byte| *byte == b'\n')
                .map(|position| position + 1)
                .or_else(|| terminal.then_some(remaining.len()))
            else {
                break;
            };
            let line = &remaining[..length];
            self.parsed_stdout += length;
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let parsed = serde_json::from_slice::<Value>(line).and_then(|value| {
                if value.get("type").and_then(Value::as_str) == Some("path_cleanup") {
                    // Auxiliary cleanup facts are not IR actions. Keep their
                    // original stdout and validate the separate protocol.
                    if value.get("absent").and_then(Value::as_bool) != Some(true)
                        || !value.get("error").is_some_and(Value::is_null)
                    {
                        self.output_error
                            .get_or_insert_with(|| format!("path cleanup failed: {value}"));
                    }
                    Ok(None)
                } else {
                    serde_json::from_value::<ActionObservation>(value).map(Some)
                }
            });
            match parsed {
                Ok(Some(result)) => self.results.push(result),
                Ok(None) => {}
                Err(error) => {
                    self.output_error.get_or_insert_with(|| format!(
                        "parse structured Python output for {}: {error}; original bytes retained", self.process,
                    ));
                }
            }
        }
    }

    fn validate_terminal(&self) -> Result<(), String> {
        if let Some(error) = self.gap.as_ref().or(self.output_error.as_ref()) {
            return Err(error.clone());
        }
        if !self.terminal {
            return Err(format!(
                "{} has no observed terminal record at cursor {}",
                self.request_id, self.cursor
            ));
        }
        if self.stop_at.is_some() && !self.stop_requested {
            return Err(format!(
                "contextual process {} terminated before the requested stop phase",
                self.request_id
            ));
        }
        if std::str::from_utf8(&self.stdout).is_err() {
            return Err(format!(
                "{} stdout is not UTF-8; original bytes retained",
                self.process
            ));
        }
        Ok(())
    }

    fn partial_observation(&self) -> ExecutionObservation {
        ExecutionObservation {
            process: self.process.clone(),
            request_id: self.request_id.clone(),
            logical_node_id: self.logical_node_id,
            lifecycle: self.lifecycle.clone(),
            results: self.results.clone(),
            terminal: self.terminal,
            exit_success: self.exit_success,
            exit_status: self.exit_status.clone(),
            stdout: String::from_utf8_lossy(&self.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&self.stderr).into_owned(),
        }
    }

    fn pending_evidence(&self) -> Value {
        json!({
            "process": self.process, "request_id": self.request_id, "logical_node_id": self.logical_node_id,
            "execution_incarnation": self.execution_incarnation, "dependencies": self.dependencies,
            "dispatched": self.dispatched, "accepted": self.accepted, "spawn_pending": self.spawn_pending,
            "next_sequence": self.cursor, "terminal": self.terminal, "acknowledged": self.acked,
            "stop_due": self.stop_due, "stop_requested": self.stop_requested, "stop_pending": self.stop_pending,
            "gap": self.gap, "output_error": self.output_error, "last_transport_error": self.last_transport_error,
            "pending_predicate": if !self.dispatched { "DAG terminal dependencies" }
                else if !self.terminal { "terminal execution record and retraction" }
                else if !self.acked { "durable terminal cursor acknowledgement" } else { "complete" },
        })
    }

    fn timing_evidence(&self) -> Value {
        json!({
            "request_id": self.request_id,
            "dispatch_elapsed_ns": self.dispatched_ns, "spawn_acceptance_ns": self.spawn_ns,
            "context_ready_observed_elapsed_ns": self.attach_ns,
            "first_output_observed_elapsed_ns": self.first_output_ns,
            "terminal_observed_elapsed_ns": self.terminal_ns, "stop_request_ns": self.stop_ns,
            "event_request_count": self.requests, "event_request_total_ns": self.request_ns,
            "transport_retry_count": self.retries, "event_records": self.cursor,
            "max_event_backlog_records": self.max_event_backlog_records,
            "event_drain_ns": self.event_drain_ns,
            "event_drain_lag_upper_bound_ns": self.event_drain_lag_upper_bound_ns,
            "stdout_bytes": self.stdout.len(), "stderr_bytes": self.stderr.len(),
            "action_records": self.results.len(), "unparsed_stdout_bytes": self.stdout.len() - self.parsed_stdout,
        })
    }
}

fn write_record<T: serde::Serialize>(writer: &mut impl IoWrite, value: &T) -> Result<(), String> {
    serde_json::to_writer(&mut *writer, value)
        .map_err(|error| format!("serialize execution evidence: {error}"))?;
    writer
        .write_all(b"\n")
        .map_err(|error| format!("write execution evidence: {error}"))
}

fn write_json<T: serde::Serialize>(path: &std::path::Path, value: &T) -> Result<(), String> {
    let mut writer = BufWriter::new(
        fs::File::create(path).map_err(|error| format!("create {}: {error}", path.display()))?,
    );
    serde_json::to_writer(&mut writer, value)
        .map_err(|error| format!("serialize {}: {error}", path.display()))?;
    writer
        .flush()
        .map_err(|error| format!("flush {}: {error}", path.display()))
}

fn stop_injection(
    case: &BehaviorCase,
    process: &ProcessProgram,
) -> Option<(ProcessStopPhase, Option<u64>)> {
    match &case.failure {
        FailureInjection::StopProcess {
            process: target,
            phase,
            kill_after_ms,
        } if target == &process.id => Some((*phase, *kill_after_ms)),
        _ => None,
    }
}

fn expected_process_failure(case: &BehaviorCase, process: &str) -> bool {
    match &case.failure {
        FailureInjection::StopProcess {
            process: target, ..
        }
        | FailureInjection::LaunchFailure {
            process: target, ..
        } => target == process,
        FailureInjection::None | FailureInjection::SlowProcess { .. } => false,
    }
}

fn probe_case(template: &BehaviorCase, id: String, node_ids: &[u64]) -> BehaviorCase {
    // Do not clone the workload's payloads/routes merely to replace every
    // process. Probe source contains only the boundary facts it actually uses.
    BehaviorCase {
        schema_version: template.schema_version,
        generator_version: template.generator_version,
        id,
        seed: template.seed,
        live_nodes: node_ids.iter().copied().collect(),
        topology: TopologyFamily::Fixed,
        scenarios: BTreeSet::new(),
        routes: Vec::new(),
        read_only_fixture_paths: BTreeSet::new(),
        resource_bounds: template.resource_bounds.clone(),
        processes: Vec::new(),
        failure: FailureInjection::None,
    }
}

fn verify_cleanup_paths(paths: &[String], stdout: &str) -> Result<(), String> {
    let expected = paths.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let mut observed = BTreeSet::new();
    for line in stdout.lines().filter(|line| !line.trim().is_empty()) {
        let record: Value = serde_json::from_str(line)
            .map_err(|error| format!("decode cleanup absence evidence: {error}"))?;
        let path = record
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("cleanup record has no path: {record}"))?;
        if record.get("type").and_then(Value::as_str) != Some("path_cleanup")
            || !expected.contains(path)
            || !observed.insert(path.to_owned())
        {
            return Err(format!(
                "cleanup returned duplicate, unowned, or unknown path evidence: {record}"
            ));
        }
        if record.get("attempted").and_then(Value::as_bool) != Some(true)
            || record.get("absent").and_then(Value::as_bool) != Some(true)
            || !record.get("error").is_some_and(Value::is_null)
        {
            return Err(format!("cleanup did not prove path absence: {record}"));
        }
        if let Some(quiescence) = record.get("quiescence").filter(|value| !value.is_null()) {
            if quiescence.get("active").and_then(Value::as_bool) != Some(false)
                || quiescence.get("revision").and_then(Value::as_u64).is_none()
            {
                return Err(format!(
                    "cleanup stream lacks exact inactive revision: {record}"
                ));
            }
        }
    }
    let missing = expected
        .into_iter()
        .filter(|path| !observed.contains(*path))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(format!(
            "cleanup omitted owned path absence evidence: {missing:?}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use myelin_control_contract::{
        ContextualEventRecord, ContextualEventsAcknowledgement, ContextualEventsBatch,
        ContextualProcessEvent, ExecutionIdentity,
    };

    #[test]
    fn persisted_stream_snapshot_requires_explicit_quiescence() {
        let path = "/cases/persisted/stream";
        let entries = [PersistedFixtureEntry::QuiescentStream {
            path: path.to_owned(),
        }];
        let mut observation: CaseObservation = serde_json::from_value(json!({
            "case_id": "persistence",
            "executions": [{
                "process": "persisted-path-probe",
                "request_id": "persistence-probe",
                "logical_node_id": 3,
                "lifecycle": [
                    "spawned", "process_started", "context_ready", "user_result", "exited"
                ],
                "results": [
                    {
                        "process": "persisted-path-probe",
                        "step": 0,
                        "action": "lookup",
                        "path": path,
                        "outcome": "ok",
                        "kind": "stream",
                        "revision": 7
                    },
                    {
                        "process": "persisted-path-probe",
                        "step": 1,
                        "action": "wait_for_quiescent",
                        "path": path,
                        "outcome": "ok",
                        "kind": "stream",
                        "revision": 7
                    }
                ],
                "terminal": true,
                "exit_success": true,
                "stdout": "",
                "stderr": ""
            }]
        }))
        .unwrap();

        assert!(
            persisted_fixture_snapshot(&entries, &observation).is_err(),
            "successful wait without active-state evidence must not prove quiescence"
        );
        for result in &mut observation.executions[0].results {
            result.active = Some(false);
        }
        persisted_fixture_snapshot(&entries, &observation)
            .expect("explicit inactive stream must be accepted");
        for result in &mut observation.executions[0].results {
            result.active = Some(true);
        }
        assert!(
            persisted_fixture_snapshot(&entries, &observation).is_err(),
            "active stream must not be accepted as a persisted quiescent stream"
        );
    }

    fn attempt_template() -> BehaviorCase {
        let mut writer = process("writer");
        writer.actions.push(Action::ok(ActionOp::PublishBlob {
            path: "/cases/owned/value".to_owned(),
            bytes: vec![1],
        }));
        BehaviorCase {
            schema_version: crate::ir::CASE_SCHEMA_VERSION,
            generator_version: crate::ir::GENERATOR_VERSION,
            id: "attempt-isolation".to_owned(),
            seed: 7,
            live_nodes: BTreeSet::from([1, 3]),
            topology: TopologyFamily::Fixed,
            scenarios: BTreeSet::new(),
            routes: Vec::new(),
            read_only_fixture_paths: BTreeSet::new(),
            resource_bounds: Default::default(),
            processes: vec![writer],
            failure: FailureInjection::None,
        }
    }

    #[test]
    fn restarted_attempts_never_reuse_paths_or_execution_identity() {
        // Exercise restart persistence without coupling the deadline to host disk flushes.
        let root = tempfile::tempdir_in("/dev/shm").expect("Linux tmpfs fixture storage");
        let state = root.path().join("state");
        let evidence = root.path().join("evidence");
        let mut cursor = 0;
        // Each simulated runner owns a fresh deadline; only its durable claims survive.
        let reserve = |state: &Path, evidence: &Path, cursor: &mut u64| {
            let budget = Budget::new(Duration::from_secs(5));
            reserve_attempt_identity(state, evidence, cursor, &budget).unwrap()
        };
        let first = reserve(&state, &evidence, &mut cursor);
        // The runner restarts with an empty in-memory counter.
        cursor = 0;
        let restarted = reserve(&state, &evidence, &mut cursor);
        // Retained fixture, new evidence directory.
        cursor = 0;
        let relocated = reserve(&state, &root.path().join("new-evidence"), &mut cursor);
        // New local fixture, reused evidence directory.
        cursor = 0;
        let recreated = reserve(&root.path().join("new-state"), &evidence, &mut cursor);
        let template = attempt_template();
        for lineage in [[first, restarted, relocated], [first, restarted, recreated]] {
            let attempts = lineage.map(|id| template.for_attempt(id, true));
            let paths = attempts
                .iter()
                .flat_map(BehaviorCase::owned_paths)
                .collect::<BTreeSet<_>>();
            let requests = attempts
                .iter()
                .map(|case| (case).execution_request_id(&case.processes[0]))
                .collect::<BTreeSet<_>>();
            assert_eq!(
                paths.len(),
                attempts.len(),
                "restarted attempts must not share writable paths"
            );
            assert_eq!(
                requests.len(),
                attempts.len(),
                "restarted executions must not alias retired requests"
            );
        }
    }

    #[test]
    fn typed_absence_rejects_non_absent_namespace_entries() {
        use myelin_control_contract::{RetainedBlobsReply, RetainedNonBlob, SCHEMA_VERSION};

        let mut reply = RetainedBlobsReply {
            schema_version: SCHEMA_VERSION,
            request_id: "precondition".to_owned(),
            blobs: BTreeMap::new(),
            non_blobs: BTreeMap::from([("/cases/absent".to_owned(), RetainedNonBlob::Absent)]),
        };
        verify_typed_absence(&reply).unwrap();
        reply.non_blobs.insert(
            "/cases/live".to_owned(),
            RetainedNonBlob::QuiescentStream { revision: 7 },
        );
        assert!(verify_typed_absence(&reply).is_err());
    }

    fn process(id: &str) -> ProcessProgram {
        ProcessProgram {
            id: id.to_owned(),
            logical_node_id: 3,
            access: AccessSpec::unrestricted(format!("execution-{id}")),
            depends_on: Vec::new(),
            actions: Vec::new(),
        }
    }

    fn record(
        request: &str,
        sequence: u64,
        event: ContextualProcessEventKind,
    ) -> ContextualEventRecord {
        ContextualEventRecord {
            sequence,
            observation: ContextualProcessEvent {
                request_id: request.to_owned(),
                logical_node_id: 3,
                event,
            },
        }
    }

    fn view(
        request: &str,
        next: u64,
        terminal: bool,
        events: Vec<ContextualEventRecord>,
    ) -> ContextualExecutionView {
        ContextualExecutionView {
            request_id: request.to_owned(),
            execution_incarnation: format!("incarnation-{request}"),
            logical_node_id: 3,
            process: None,
            terminal,
            truncated_before: 0,
            next_sequence: next,
            events,
        }
    }

    fn exited() -> ContextualProcessEventKind {
        ContextualProcessEventKind::Exited {
            status: ContextualExitStatus::Code(0),
        }
    }

    fn spawned() -> ContextualProcessEventKind {
        ContextualProcessEventKind::Spawned {
            process: "actor".to_owned(),
            identity: ExecutionIdentity {
                execution_id: 1,
                generation: 1,
            },
        }
    }

    #[test]
    fn reconnect_replays_prefix_without_duplicating_split_causal_record() {
        let mut state = ExecutionTracker::new(&process("reader"), "request".to_owned(), None);
        let output = b"{\"process\":\"reader\",\"step\":0,\"action\":\"lookup\",\"path\":\"/cases/x\",\"outcome\":\"ok\"}\n";
        let first = record(
            "request",
            0,
            ContextualProcessEventKind::Stdout {
                bytes: output[..23].to_vec(),
            },
        );
        state
            .ingest(
                &view("request", 1, false, vec![first.clone()]),
                Duration::ZERO,
            )
            .unwrap();
        assert!(
            state.partial_observation().results.is_empty(),
            "a torn line is not a causal record"
        );
        state
            .ingest(
                &view(
                    "request",
                    3,
                    true,
                    vec![
                        first,
                        record(
                            "request",
                            1,
                            ContextualProcessEventKind::Stdout {
                                bytes: output[23..].to_vec(),
                            },
                        ),
                        record("request", 2, exited()),
                    ],
                ),
                Duration::from_millis(1),
            )
            .unwrap();
        state.validate_terminal().unwrap();
        let observed = state.partial_observation();
        assert_eq!(observed.stdout.as_bytes(), output);
        assert_eq!(observed.results.len(), 1);
        assert_eq!(observed.results[0].path, "/cases/x");
    }

    #[test]
    fn reused_request_with_equal_cursor_cannot_replace_owned_incarnation() {
        let mut state = ExecutionTracker::new(&process("reader"), "request".to_owned(), None);
        state
            .ingest(
                &view("request", 1, true, vec![record("request", 0, exited())]),
                Duration::ZERO,
            )
            .unwrap();
        let mut replacement = view("request", 1, true, vec![record("request", 0, exited())]);
        replacement.execution_incarnation = "replacement".to_owned();
        assert!(state.ingest(&replacement, Duration::ZERO).is_err());
        assert_eq!(
            state.execution_incarnation.as_deref(),
            Some("incarnation-request")
        );
        assert!(!state.acked);
    }

    #[test]
    fn terminal_flag_cannot_replace_a_missing_terminal_record() {
        let mut state = ExecutionTracker::new(&process("reader"), "request".to_owned(), None);
        assert!(
            state
                .ingest(&view("request", 1, true, vec![]), Duration::ZERO)
                .is_err()
        );
        assert!(!state.partial_observation().terminal);
        assert!(state.validate_terminal().is_err());
    }

    #[test]
    fn sequence_gap_retains_later_output_but_cannot_acknowledge_success() {
        let mut state = ExecutionTracker::new(&process("reader"), "request".to_owned(), None);
        let output = b"{\"process\":\"reader\",\"step\":2,\"action\":\"lookup\",\"path\":\"/cases/later\",\"outcome\":\"ok\"}\n";
        assert!(
            state
                .ingest(
                    &view(
                        "request",
                        4,
                        true,
                        vec![
                            record(
                                "request",
                                2,
                                ContextualProcessEventKind::Stdout {
                                    bytes: output.to_vec(),
                                }
                            ),
                            record("request", 3, exited()),
                        ]
                    ),
                    Duration::ZERO
                )
                .is_err()
        );
        assert_eq!(state.partial_observation().results[0].path, "/cases/later");
        assert_eq!(state.partial_observation().stdout.as_bytes(), output);
        assert!(state.partial_observation().terminal);
        assert!(state.validate_terminal().is_err());
        assert!(!state.acked);
    }

    #[test]
    fn bootstrap_failure_waits_for_reaping_and_preserves_deferred_stop() {
        let mut state = ExecutionTracker::new(
            &process("held"),
            "request".to_owned(),
            Some((ProcessStopPhase::DuringBootstrap, Some(0))),
        );
        state
            .ingest(
                &view(
                    "request",
                    2,
                    false,
                    vec![
                        record(
                            "request",
                            0,
                            ContextualProcessEventKind::ProcessStarted { pid: 1 },
                        ),
                        record(
                            "request",
                            1,
                            ContextualProcessEventKind::BootstrapFailed {
                                error: "injected test failure".to_owned(),
                            },
                        ),
                    ],
                ),
                Duration::ZERO,
            )
            .unwrap();
        assert!(state.stop_due);
        assert!(
            !state.terminal,
            "bootstrap failure does not prove the child was reaped"
        );
        state.stop_requested = true;
        state
            .ingest(
                &view("request", 3, true, vec![record("request", 2, exited())]),
                Duration::ZERO,
            )
            .unwrap();
        state.validate_terminal().unwrap();
    }

    #[test]
    fn observed_acceptance_cancels_only_the_stalled_spawn_reply() {
        let owner = Budget::new(Duration::from_secs(2));
        let spawn = owner.child(Duration::from_secs(1));
        let sibling = owner.child(Duration::from_secs(1));
        let mut state = ExecutionTracker::new(&process("accepted"), "request".to_owned(), None);
        state.spawn_pending = true;
        state.spawn_budget = Some(spawn.clone());
        state
            .ingest(
                &view("request", 1, false, vec![record("request", 0, spawned())]),
                Duration::ZERO,
            )
            .unwrap();
        assert!(spawn.check("withheld spawn response").is_err());
        owner.check("attempt collector").unwrap();
        sibling.check("independent healthy sibling").unwrap();
        state
            .control_completed(ControlCompletion {
                index: 0,
                kind: ControlKind::Spawn,
                result: Err("canceled reply after observed acceptance".to_owned()),
                elapsed: Duration::ZERO,
            })
            .unwrap();
        assert!(state.accepted);
        assert!(!state.spawn_pending);
    }

    #[test]
    fn cleanup_requires_each_unique_owned_path_and_inactive_stream_revision() {
        let paths = vec!["/cases/a".to_owned(), "/cases/b".to_owned()];
        let record = |path: &str, quiescence: Value| {
            json!({
                "type": "path_cleanup", "path": path, "attempted": true,
                "absent": true, "error": null, "quiescence": quiescence,
            })
            .to_string()
        };
        let a = record("/cases/a", Value::Null);
        let b = record("/cases/b", json!({"revision": 7, "active": false}));
        assert!(verify_cleanup_paths(&paths, &a).is_err());
        assert!(verify_cleanup_paths(&paths, &format!("{a}\n{a}\n{b}")).is_err());
        assert!(
            verify_cleanup_paths(
                &paths,
                &format!(
                    "{a}\n{}",
                    record("/cases/b", json!({"revision": 7, "active": true}))
                )
            )
            .is_err()
        );
        verify_cleanup_paths(&paths, &format!("{a}\n{b}")).unwrap();
    }

    #[test]
    fn expected_branch_failure_does_not_mask_failed_sibling() {
        let failed = process("failed");
        let healthy = process("healthy");
        let case = BehaviorCase {
            schema_version: crate::ir::CASE_SCHEMA_VERSION,
            generator_version: crate::ir::GENERATOR_VERSION,
            id: "branch".to_owned(),
            seed: 1,
            live_nodes: BTreeSet::from([1, 3]),
            topology: TopologyFamily::Fixed,
            scenarios: BTreeSet::new(),
            routes: Vec::new(),
            read_only_fixture_paths: BTreeSet::new(),
            resource_bounds: Default::default(),
            processes: vec![failed.clone(), healthy.clone()],
            failure: FailureInjection::LaunchFailure {
                process: failed.id.clone(),
                kind: LaunchFailureKind::MissingExecutable,
            },
        };
        assert!(expected_process_failure(&case, &failed.id));
        assert!(!expected_process_failure(&case, &healthy.id));
        let mut failed_state = ExecutionTracker::new(&failed, "failed".to_owned(), None);
        failed_state
            .ingest(
                &view(
                    "failed",
                    1,
                    true,
                    vec![record(
                        "failed",
                        0,
                        ContextualProcessEventKind::SpawnFailed {
                            error: "injected test failure".to_owned(),
                        },
                    )],
                ),
                Duration::ZERO,
            )
            .unwrap();
        let mut sibling = ExecutionTracker::new(&healthy, "healthy".to_owned(), None);
        sibling
            .ingest(
                &view("healthy", 1, true, vec![record("healthy", 0, exited())]),
                Duration::ZERO,
            )
            .unwrap();
        sibling.validate_terminal().unwrap();
        assert!(sibling.partial_observation().exit_success);
        assert!(!failed_state.partial_observation().exit_success);
    }

    #[test]
    fn canceled_stalled_http_joins_and_preserves_final_channel_outcome() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted, accepted_rx) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (_socket, _) = listener.accept().unwrap();
            accepted.send(()).unwrap();
            let _ = release_rx.recv_timeout(Duration::from_secs(2));
        });
        let budget = Budget::new(Duration::from_secs(2));
        let receiver = thread::scope(|scope| {
            let cancel_before_join = CancelOnDrop(budget.clone());
            let (sender, receiver) = mpsc::channel();
            let worker_budget = &budget;
            scope.spawn(move || {
                let result = http_json_budget(
                    "POST",
                    &format!("http://{address}/stalled-spawn"),
                    None,
                    worker_budget,
                );
                sender.send(result).unwrap();
            });
            accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            drop(cancel_before_join);
            receiver
        });
        assert!(
            receiver
                .recv_timeout(Duration::from_millis(100))
                .unwrap()
                .is_err()
        );
        release.send(()).unwrap();
        server.join().unwrap();
    }

    struct PeerChild(std::process::Child);

    impl Drop for PeerChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            self.0.wait().expect("reap owned loopback child");
        }
    }

    fn collector_peer_harness(artifacts: &Path, address: std::net::SocketAddr) -> ClusterHarness {
        use std::os::fd::FromRawFd;
        use std::sync::{Mutex, atomic::AtomicU64};

        let mut orchestrator = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, orchestrator.id(), 0) as i32 };
        if fd < 0 {
            let _ = orchestrator.kill();
            orchestrator.wait().unwrap();
            panic!("pin collector peer process");
        }
        ClusterHarness {
            config: super::super::ClusterHarnessConfig {
                workspace: artifacts.to_owned(),
                artifacts: artifacts.to_owned(),
                node_count: 2,
                seed: 1,
                image: None,
                build_image: false,
                deadline: Duration::from_secs(1),
                provider: super::super::HarnessProvider::LocalMock,
                selected_offer_ids: Vec::new(),
                state_dir: None,
                reset_state: false,
                adopt_only: false,
                offer_search_id: None,
                provision: false,
                relay_url: None,
                run_id: 1,
            },
            execution_budget: Budget::new(Duration::from_secs(1)),
            cleanup_budget: None,
            fixture_cleanup_budget: Budget::new(Duration::from_secs(10)),
            binaries: crate::resources::resource_deadline_tests::unused_binaries_for_control_peer(),
            provider_client: None,
            lifecycle_started: Instant::now(),
            observation_generation: AtomicU64::new(0),
            health_boundary: AtomicU64::new(0),
            base_url: format!("http://{address}"),
            state_dir: artifacts.to_owned(),
            dashboard_port: address.port(),
            image: String::new(),
            container_prefix: String::new(),
            telemetry: artifacts.join("telemetry"),
            telemetry_census: Mutex::new(Default::default()),
            provider_baseline: BTreeSet::new(),
            fixture_mapping: BTreeMap::new(),
            stopped_nodes: BTreeSet::new(),
            provider_accounting_baseline: None,
            node_generation_baseline: BTreeMap::new(),
            fixture_blob_baseline: None,
            fixture_actor_baseline: BTreeSet::new(),
            verified_local_baseline: None,
            orchestrator,
            orchestrator_pidfd: unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) },
            stdout: Default::default(),
            stderr: Default::default(),
            node_ids: vec![1, 3],
            next_attempt_id: 0,
            last_attempt_id: None,
            pending_attempts: BTreeMap::new(),
            active_attempts: BTreeMap::new(),
            pending_control_processes: Mutex::new(BTreeSet::new()),
            last_failure_signature: None,
            quarantine_reason: Mutex::new(None),
            // No provider resources exist; Drop still kills and reaps the child.
            torn_down: true,
        }
    }

    #[test]
    fn remote_retention_reaps_after_injected_exit_or_execution_cancellation() {
        for already_exited in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let mut harness = collector_peer_harness(root.path(), ([127, 0, 0, 1], 9).into());
            harness.config.provider = super::super::HarnessProvider::StaticSsh {
                manifest: root.path().join("unused-manifest"),
                identity: root.path().join("unused-identity"),
                bundle: root.path().join("unused-bundle"),
            };
            let pid = harness.orchestrator.id() as libc::pid_t;
            if already_exited {
                harness.orchestrator.kill().unwrap();
                super::super::wait_for_child(
                    &mut harness.orchestrator,
                    &harness.fixture_cleanup_budget,
                    "observe injected orchestrator exit",
                )
                .unwrap();
            } else {
                harness.execution_budget.cancel();
            }
            harness.retain_remote_fixture(true).unwrap();
            let mut status = 0;
            assert_eq!(
                unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) },
                -1
            );
            assert_eq!(
                std::io::Error::last_os_error().raw_os_error(),
                Some(libc::ECHILD)
            );
        }
    }

    #[test]
    fn withheld_terminal_event_expires_after_healthy_sibling_and_drains_remaining_stops() {
        use std::io::Read;
        use std::net::TcpListener;
        use std::process::{Command, Stdio};

        // Keep real evidence fsyncs, but isolate control deadlines from host disk latency.
        let root = tempfile::tempdir_in("/dev/shm").expect("Linux tmpfs fixture storage");
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut case = attempt_template();
        case.processes = ["withheld", "healthy", "cleanup"]
            .map(|name| {
                let mut program = process(name);
                program.actions.push(Action::ok(ActionOp::Lookup {
                    path: "/cases/read-only".to_owned(),
                    expected_kind: "blob".to_owned(),
                }));
                program
            })
            .to_vec();
        case.read_only_fixture_paths
            .insert("/cases/read-only".to_owned());
        let requests = case
            .processes
            .iter()
            .map(|process| (process.id.clone(), (&case).execution_request_id(process)))
            .collect::<BTreeMap<_, _>>();
        let attempt = Arc::new(CaseAttempt::prepare(1, root.path(), case).unwrap());
        let mut harness = collector_peer_harness(root.path(), address);
        let server_budget = Budget::new(Duration::from_secs(8));
        let started = Instant::now();
        let (result, stops, acknowledgements, reaped) = thread::scope(|scope| {
            let owner = &server_budget;
            let server = scope.spawn(move || {
                let mut children = BTreeMap::<String, (String, PeerChild)>::new();
                let mut stops = BTreeSet::new();
                let mut acknowledgements = BTreeSet::new();
                let mut reaped = BTreeSet::new();
                'requests: while owner.check("loopback collector peer").is_ok() {
                    let (mut socket, _) = match listener.accept() {
                        Ok(connection) => connection,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            let _ = owner.wait(Duration::from_millis(1), "collector request");
                            continue;
                        }
                        Err(error) => panic!("accept collector request: {error}"),
                    };
                    socket
                        .set_read_timeout(Some(Duration::from_secs(1)))
                        .unwrap();
                    socket
                        .set_write_timeout(Some(Duration::from_secs(1)))
                        .unwrap();
                    let mut header = Vec::new();
                    let mut byte = [0_u8; 1];
                    while !header.ends_with(b"\r\n\r\n") {
                        if socket.read_exact(&mut byte).is_err() {
                            continue 'requests;
                        }
                        header.push(byte[0]);
                        assert!(header.len() < 65536, "bounded HTTP header");
                    }
                    let header = String::from_utf8(header).unwrap();
                    let path = header.split_whitespace().nth(1).unwrap();
                    let length = header
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    assert!(length < 1024 * 1024, "bounded HTTP body");
                    let mut body = vec![0; length];
                    if socket.read_exact(&mut body).is_err() {
                        continue 'requests;
                    }
                    let body: Value = serde_json::from_slice(&body).unwrap();
                    let reply = if path.ends_with("/spawn") {
                        let request = body["request_id"].as_str().unwrap().to_owned();
                        let label = body["label"].as_str().unwrap().to_owned();
                        let child = if label == "healthy" {
                            Command::new("true").spawn().unwrap()
                        } else {
                            Command::new("sleep")
                                .arg("30")
                                .stdin(Stdio::null())
                                .spawn()
                                .unwrap()
                        };
                        assert!(
                            children
                                .insert(request.clone(), (label.clone(), PeerChild(child)))
                                .is_none()
                        );
                        serde_json::to_value(ContextualControlReply::Event {
                            observation: ContextualProcessEvent {
                                request_id: request,
                                logical_node_id: 3,
                                event: ContextualProcessEventKind::Spawned {
                                    process: label,
                                    identity: ExecutionIdentity {
                                        execution_id: 1,
                                        generation: 1,
                                    },
                                },
                            },
                        })
                        .unwrap()
                    } else if path.ends_with("/events") {
                        let mut executions = Vec::new();
                        let mut missing = Vec::new();
                        for cursor in body["payload"]["cursors"].as_array().unwrap() {
                            let request = cursor["request_id"].as_str().unwrap();
                            let Some((label, child)) = children.get_mut(request) else {
                                missing.push(request.to_owned());
                                continue;
                            };
                            let status = child.0.try_wait().unwrap();
                            if status.is_some() {
                                reaped.insert(request.to_owned());
                            }
                            // The child really exits/reaps on stop, but this peer
                            // independently withholds its terminal cursor forever.
                            let terminal = label != "withheld" && status.is_some();
                            let mut events = vec![record(request, 0, spawned())];
                            if terminal {
                                use std::os::unix::process::ExitStatusExt;
                                let status = status.unwrap();
                                let status = match status.code() {
                                    Some(code) => ContextualExitStatus::Code(code),
                                    None => status.signal().map_or(
                                        ContextualExitStatus::Unknown,
                                        ContextualExitStatus::Signal,
                                    ),
                                };
                                events.push(record(
                                    request,
                                    1,
                                    ContextualProcessEventKind::Exited { status },
                                ));
                            }
                            let next = events.len() as u64;
                            let after = cursor["after_sequence"].as_u64().unwrap();
                            events.retain(|record| record.sequence >= after);
                            executions.push(view(request, next, terminal, events));
                        }
                        serde_json::to_value(Versioned::new(ContextualControlReply::EventsBatch(
                            ContextualEventsBatch {
                                executions,
                                missing,
                            },
                        )))
                        .unwrap()
                    } else if path.ends_with("/stop") {
                        let request = path
                            .trim_start_matches("/api/control/contextual/")
                            .trim_end_matches("/stop");
                        let (label, child) = children.get_mut(request).unwrap();
                        child.0.kill().unwrap();
                        child.0.wait().unwrap();
                        reaped.insert(request.to_owned());
                        stops.insert(request.to_owned());
                        serde_json::to_value(ContextualControlReply::Event {
                            observation: ContextualProcessEvent {
                                request_id: request.to_owned(),
                                logical_node_id: 3,
                                event: ContextualProcessEventKind::StopAccepted {
                                    process: label.clone(),
                                },
                            },
                        })
                        .unwrap()
                    } else if path.ends_with("/ack") {
                        acknowledgements.insert(
                            path.trim_start_matches("/api/control/contextual/")
                                .trim_end_matches("/ack")
                                .to_owned(),
                        );
                        serde_json::to_value(Versioned::new(ContextualControlReply::Acknowledged(
                            ContextualEventsAcknowledgement {
                                request_id: path
                                    .trim_start_matches("/api/control/contextual/")
                                    .trim_end_matches("/ack")
                                    .to_owned(),
                                execution_incarnation: body["payload"]["execution_incarnation"]
                                    .as_str()
                                    .unwrap()
                                    .to_owned(),
                                through_sequence: body["payload"]["through_sequence"]
                                    .as_u64()
                                    .unwrap(),
                            },
                        )))
                        .unwrap()
                    } else {
                        panic!("unexpected collector request: {path}");
                    };
                    let reply = reply.to_string();
                    let _ = write!(
                        socket,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{reply}",
                        reply.len()
                    );
                }
                // PeerChild reaps all children even if the client panics.
                (stops, acknowledgements, reaped)
            });
            let cancel_before_join = CancelOnDrop(server_budget.clone());
            let cleanup = Budget::new(Duration::from_secs(3));
            let result = harness.execute_case(&attempt, true, &cleanup);
            drop(cancel_before_join);
            let (stops, acknowledgements, reaped) = server.join().unwrap();
            (result, stops, acknowledgements, reaped)
        });
        let elapsed = started.elapsed();
        let error = result.unwrap_err();
        assert!(elapsed < Duration::from_secs(5), "{elapsed:?}: {error}");
        assert!(error.contains("budget exhausted"), "{error}");
        assert!(harness.is_quarantined());
        assert!(harness.last_failure_signature().is_none());
        assert_eq!(
            stops,
            BTreeSet::from([requests["withheld"].clone(), requests["cleanup"].clone()])
        );
        assert_eq!(
            acknowledgements,
            BTreeSet::from([requests["healthy"].clone(), requests["cleanup"].clone()])
        );
        assert_eq!(reaped, requests.values().cloned().collect::<BTreeSet<_>>());
        assert_eq!(
            *harness.pending_control_processes.lock().unwrap(),
            BTreeSet::from([requests["withheld"].clone()])
        );
        let pending: Value = serde_json::from_slice(
            &fs::read(attempt.directory.join("pending-executions.json")).unwrap(),
        )
        .unwrap();
        let withheld = pending["executions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|execution| execution["process"] == "withheld")
            .unwrap();
        assert_eq!(withheld["request_id"], requests["withheld"]);
        assert_eq!(withheld["next_sequence"], 1);
        assert_eq!(withheld["terminal"], false);
        assert_eq!(withheld["acknowledged"], false);
        assert_eq!(withheld["gap"], Value::Null);
        let wire = fs::read_to_string(attempt.directory.join("execution-events.jsonl")).unwrap();
        let reconciliations = wire
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|record| {
                record
                    .pointer("/response/payload/executions")
                    .and_then(Value::as_array)
                    .is_some_and(|executions| {
                        executions.iter().any(|execution| {
                            execution["request_id"] == requests["withheld"]
                                && execution["terminal"] == false
                        })
                    })
            })
            .count();
        assert!(
            reconciliations >= 2,
            "the collector must reconcile a live cursor repeatedly, not fail one stalled HTTP request"
        );
        let observed: CaseObservation =
            serde_json::from_slice(&fs::read(attempt.directory.join("observed.json")).unwrap())
                .unwrap();
        assert!(
            observed
                .executions
                .iter()
                .find(|execution| execution.process == "healthy")
                .unwrap()
                .exit_success
        );
        let orchestrator_pid = harness.orchestrator.id() as libc::pid_t;
        drop(harness);
        let mut status = 0;
        assert_eq!(
            unsafe { libc::waitpid(orchestrator_pid, &mut status, libc::WNOHANG) },
            -1
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ECHILD),
            "the existing harness owner must reap its child, not just signal it"
        );
    }
}
