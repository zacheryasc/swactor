//! Monotonic, hierarchical execution ownership and out-of-band timing evidence.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug)]
struct Cancellation {
    cancelled: AtomicBool,
    parent: Option<Arc<Cancellation>>,
}

#[derive(Debug, Default)]
struct Wake {
    revision: Mutex<u64>,
    changed: Condvar,
}

/// Clones share cancellation and an absolute deadline. Child cancellation is
/// isolated; parent cancellation wakes and stops every descendant.
#[derive(Clone, Debug)]
pub struct Budget {
    deadline: Instant,
    cancellation: Arc<Cancellation>,
    wake: Arc<Wake>,
}

impl Budget {
    pub fn new(duration: Duration) -> Self {
        let now = Instant::now();
        Self {
            // An unrepresentable allocation must fail closed, not be unbounded.
            deadline: now.checked_add(duration).unwrap_or(now),
            cancellation: Arc::new(Cancellation {
                cancelled: AtomicBool::new(false),
                parent: None,
            }),
            wake: Arc::new(Wake::default()),
        }
    }

    pub fn child(&self, duration: Duration) -> Self {
        let now = Instant::now();
        Self {
            deadline: self.deadline.min(now.checked_add(duration).unwrap_or(now)),
            cancellation: Arc::new(Cancellation {
                cancelled: AtomicBool::new(false),
                parent: Some(Arc::clone(&self.cancellation)),
            }),
            wake: Arc::clone(&self.wake),
        }
    }

    pub fn remaining(&self, predicate: &str) -> Result<Duration, String> {
        let mut cancellation = Some(self.cancellation.as_ref());
        while let Some(state) = cancellation {
            if state.cancelled.load(Ordering::Acquire) {
                record_pending(predicate, "cancelled");
                return Err(format!("budget cancelled; pending predicate: {predicate}"));
            }
            cancellation = state.parent.as_deref();
        }
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            record_pending(predicate, "deadline_exhausted");
            return Err(format!("budget exhausted; pending predicate: {predicate}"));
        }
        Ok(remaining)
    }

    pub fn check(&self, predicate: &str) -> Result<(), String> {
        self.remaining(predicate).map(|_| ())
    }

    /// Interruptible pacing/reconciliation wait; never extends its owner.
    pub fn wait(&self, duration: Duration, predicate: &str) -> Result<(), String> {
        let started = Instant::now();
        let target = started.checked_add(duration).unwrap_or(self.deadline);
        let mut revision = self
            .wake
            .revision
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut wake_count = 0_u64;
        let result = loop {
            let remaining = match self.remaining(predicate) {
                Ok(remaining) => remaining,
                Err(error) => break Err(error),
            };
            let until_target = target.saturating_duration_since(Instant::now());
            if until_target.is_zero() {
                break Ok(());
            }
            let (guard, _) = self
                .wake
                .changed
                .wait_timeout(revision, remaining.min(until_target))
                .unwrap_or_else(|error| error.into_inner());
            revision = guard;
            wake_count = wake_count.saturating_add(1);
        };
        drop(revision);
        record_wakes(wake_count);
        result
    }

    pub fn cancel(&self) {
        let mut revision = self
            .wake
            .revision
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.cancellation.cancelled.store(true, Ordering::Release);
        *revision = revision.wrapping_add(1);
        self.wake.changed.notify_all();
    }
}

/// Fixed lifecycle categories: identities and predicates belong in diagnostic
/// evidence, never in metric keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStage {
    PreparationAdmission,
    ProcessLaunchReadiness,
    WorkloadExecution,
    EventWaiting,
    EventIngestion,
    TerminalAcknowledgement,
    NamespaceCleanup,
    FixtureQuiescenceHealth,
    DeploymentTransferActivation,
    ExternalInspection,
    EvidenceSerialization,
    EvidenceSynchronization,
    EvidenceCompaction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkKind {
    RunnerOverhead,
    /// Use only for measured productive work, not request round-trip latency.
    RemoteWork,
    RemoteWait,
}

#[derive(Default, Serialize)]
struct StageEvidence {
    count: u64,
    aggregate_ns: u128,
    max_elapsed_ns: u128,
    wall_active_ns: u128,
    bytes: u64,
    records: u64,
    active: u64,
    peak_active: u64,
    #[serde(skip)]
    active_since_ns: u128,
}

impl StageEvidence {
    fn begin(&mut self, now: u128) {
        if self.active == 0 {
            self.active_since_ns = now;
        }
        self.active = self.active.saturating_add(1);
        self.peak_active = self.peak_active.max(self.active);
    }

    fn finish(&mut self, started: u128, now: u128, bytes: u64, records: u64) {
        let elapsed = now.saturating_sub(started);
        self.count = self.count.saturating_add(1);
        self.aggregate_ns = self.aggregate_ns.saturating_add(elapsed);
        self.max_elapsed_ns = self.max_elapsed_ns.max(elapsed);
        self.bytes = self.bytes.saturating_add(bytes);
        self.records = self.records.saturating_add(records);
        self.active = self.active.saturating_sub(1);
        if self.active == 0 {
            self.wall_active_ns = self
                .wall_active_ns
                .saturating_add(now.saturating_sub(self.active_since_ns));
        }
    }

    fn snapshot(&self, now: u128) -> Value {
        let mut value = serde_json::to_value(self).unwrap_or(Value::Null);
        if self.active != 0 {
            value["wall_active_ns"] = json!(
                self.wall_active_ns
                    .saturating_add(now.saturating_sub(self.active_since_ns))
            );
        }
        value
    }
}

#[derive(Default, Serialize)]
struct RemoteWorkEvidence {
    count: u64,
    aggregate_ns: u128,
    max_elapsed_ns: u128,
}

#[derive(Default)]
struct ExecutionEvidence {
    stages: BTreeMap<ExecutionStage, BTreeMap<WorkKind, StageEvidence>>,
    remote_work: BTreeMap<ExecutionStage, RemoteWorkEvidence>,
    pending: BTreeMap<String, BTreeMap<String, u64>>,
    omitted_pending: u64,
    wake_count: u64,
    immutable_artifacts: BTreeMap<String, Value>,
}

static ORIGIN: LazyLock<Instant> = LazyLock::new(Instant::now);
static EVIDENCE: LazyLock<Mutex<ExecutionEvidence>> = LazyLock::new(Mutex::default);

fn evidence() -> &'static Mutex<ExecutionEvidence> {
    &EVIDENCE
}

/// Observational interval ownership. Starting and dropping a span never changes
/// budgets, cancellation, or the workload's causal output protocol.
pub struct ExecutionSpan {
    stage: ExecutionStage,
    kind: WorkKind,
    started_ns: u128,
    bytes: u64,
    records: u64,
}

pub fn execution_span(stage: ExecutionStage, kind: WorkKind) -> ExecutionSpan {
    let mut evidence = evidence().lock().unwrap_or_else(|error| error.into_inner());
    let started_ns = ORIGIN.elapsed().as_nanos();
    evidence
        .stages
        .entry(stage)
        .or_default()
        .entry(kind)
        .or_default()
        .begin(started_ns);
    ExecutionSpan {
        stage,
        kind,
        started_ns,
        bytes: 0,
        records: 0,
    }
}

/// Remote clocks are not synchronized with the runner. Preserve their measured
/// durations, but never manufacture cross-host wall intervals from receipt time.
pub fn record_remote_work(stage: ExecutionStage, elapsed: Duration) {
    let mut evidence = evidence().lock().unwrap_or_else(|error| error.into_inner());
    let entry = evidence.remote_work.entry(stage).or_default();
    entry.count = entry.count.saturating_add(1);
    entry.aggregate_ns = entry.aggregate_ns.saturating_add(elapsed.as_nanos());
    entry.max_elapsed_ns = entry.max_elapsed_ns.max(elapsed.as_nanos());
}

impl ExecutionSpan {
    pub fn add_bytes(&mut self, bytes: u64) {
        self.bytes = self.bytes.saturating_add(bytes);
    }

    pub fn add_records(&mut self, records: u64) {
        self.records = self.records.saturating_add(records);
    }
}

impl Drop for ExecutionSpan {
    fn drop(&mut self) {
        let mut evidence = evidence().lock().unwrap_or_else(|error| error.into_inner());
        let now = ORIGIN.elapsed().as_nanos();
        if let Some(entry) = evidence
            .stages
            .get_mut(&self.stage)
            .and_then(|kinds| kinds.get_mut(&self.kind))
        {
            entry.finish(self.started_ns, now, self.bytes, self.records);
        }
    }
}

fn record_wakes(wakes: u64) {
    let mut evidence = evidence().lock().unwrap_or_else(|error| error.into_inner());
    evidence.wake_count = evidence.wake_count.saturating_add(wakes);
}

fn record_pending(predicate: &str, reason: &str) {
    let mut evidence = evidence().lock().unwrap_or_else(|error| error.into_inner());
    // Pending identities are bounded diagnostic examples, not timing categories.
    if predicate.len() > 4096
        || (evidence.pending.len() >= 64 && !evidence.pending.contains_key(predicate))
    {
        evidence.omitted_pending = evidence.omitted_pending.saturating_add(1);
        return;
    }
    let count = evidence
        .pending
        .entry(predicate.to_owned())
        .or_default()
        .entry(reason.to_owned())
        .or_default();
    *count = count.saturating_add(1);
}

/// Record a completed interval from helper code that measured its own start.
/// Labels select only a fixed lifecycle category; they are never metric keys.
pub(crate) fn record_execution_stage(label: &str, elapsed: Duration, bytes: u64, records: u64) {
    let stage = if label.starts_with("telemetry.ingest") {
        ExecutionStage::EventIngestion
    } else if label.starts_with("telemetry.snapshot") {
        ExecutionStage::FixtureQuiescenceHealth
    } else if label.starts_with("http.") {
        ExecutionStage::ExternalInspection
    } else {
        ExecutionStage::PreparationAdmission
    };
    let kind = if label.starts_with("http.connect")
        || label.starts_with("http.read")
        || label.starts_with("http.attempt")
        || label.starts_with("http.total")
    {
        WorkKind::RemoteWait
    } else {
        WorkKind::RunnerOverhead
    };
    let now = ORIGIN.elapsed().as_nanos();
    let started = now.saturating_sub(elapsed.as_nanos());
    let mut evidence = evidence().lock().unwrap_or_else(|error| error.into_inner());
    let entry = evidence
        .stages
        .entry(stage)
        .or_default()
        .entry(kind)
        .or_default();
    entry.begin(started);
    entry.finish(started, now, bytes, records);
}

/// Retain verified source/build/image identities alongside timing evidence.
pub(crate) fn record_execution_identity(name: &str, identity: Value) {
    evidence()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .immutable_artifacts
        .insert(name.to_owned(), identity);
}

/// Union wall time and aggregate operation time are intentionally separate.
/// Categories can overlap; summing wall_active_ns across them is not a causal
/// critical path. elapsed_wall_ns is the measured reporting window.
pub fn execution_evidence() -> Value {
    let evidence = evidence().lock().unwrap_or_else(|error| error.into_inner());
    let now = ORIGIN.elapsed().as_nanos();
    let stages: BTreeMap<_, BTreeMap<_, _>> = evidence
        .stages
        .iter()
        .map(|(stage, kinds)| {
            (
                *stage,
                kinds
                    .iter()
                    .map(|(kind, entry)| (*kind, entry.snapshot(now)))
                    .collect(),
            )
        })
        .collect();
    json!({
        "schema_version": 2,
        "clock": "monotonic",
        "elapsed_wall_ns": now,
        "wall_time_semantics": "union of active intervals per category; categories may overlap",
        "aggregate_time_semantics": "sum of completed intervals; includes concurrent and nested work",
        "stages": stages,
        "remote_work": evidence.remote_work,
        "remote_work_semantics": "remote action durations; includes action-local waits; no shared wall clock",
        "wake_count": evidence.wake_count,
        "pending_predicates": evidence.pending,
        "omitted_pending_predicates": evidence.omitted_pending,
        "immutable_artifacts": evidence.immutable_artifacts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn overlapping_work_counts_union_wall_time_without_hiding_concurrent_waits() {
        let mut entry = StageEvidence::default();
        entry.begin(10);
        entry.begin(20);
        entry.finish(10, 30, 5, 1);
        assert_eq!(entry.snapshot(35)["wall_active_ns"], 25);
        entry.finish(20, 40, 7, 1);
        assert_eq!(entry.aggregate_ns, 40);
        assert_eq!(entry.wall_active_ns, 30);
        assert_eq!(entry.peak_active, 2);
        entry.begin(50);
        entry.finish(50, 60, 0, 1);
        assert_eq!(entry.aggregate_ns, 50);
        assert_eq!(entry.wall_active_ns, 40);
        assert_eq!(entry.snapshot(80)["wall_active_ns"], 40);
    }

    #[test]
    fn child_cancellation_does_not_cancel_parent_or_sibling() {
        let parent = Budget::new(Duration::from_secs(5));
        let child = parent.child(Duration::from_secs(2));
        let sibling = parent.child(Duration::from_secs(2));
        child.clone().cancel();
        assert!(child.check("child").is_err());
        parent.check("parent").unwrap();
        sibling.check("sibling").unwrap();
        parent.cancel();
        assert!(sibling.check("parent cancelled").is_err());
    }

    #[test]
    fn ancestor_cancellation_interrupts_descendant_wait() {
        let engine = swactor_engine::Engine::new(
            swactor::runtime::RuntimeParts::new(swactor::config::RuntimeConfig::default()),
            swactor_engine::TokioBackend::new(swactor_engine::TokioConfig::default()).unwrap(),
        )
        .unwrap();
        let blocking_work = engine.handle().blocking_work_sender();
        let parent = Budget::new(Duration::from_secs(30));
        let child = parent.child(Duration::from_secs(30));
        let started = Instant::now();
        let waiter = blocking_work
            .submit_joinable(move || child.wait(Duration::from_secs(20), "withheld event"))
            .unwrap();
        parent.cancel();
        assert!(
            waiter
                .join()
                .unwrap()
                .unwrap_err()
                .contains("withheld event")
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn child_never_renews_expired_parent() {
        let parent = Budget::new(Duration::ZERO);
        let child = parent.child(Duration::from_secs(30));
        assert!(
            child
                .remaining("nested operation")
                .unwrap_err()
                .contains("nested operation")
        );
    }
}
