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
        record_stage(
            &format!("wait:{predicate}"),
            started.elapsed(),
            0,
            0,
            wake_count,
        );
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

#[derive(Default, Serialize)]
struct StageEvidence {
    count: u64,
    elapsed_ns: u128,
    max_elapsed_ns: u128,
    bytes: u64,
    records: u64,
    wake_count: u64,
}

#[derive(Default)]
struct ExecutionEvidence {
    stages: BTreeMap<String, StageEvidence>,
    pending: BTreeMap<String, BTreeMap<String, u64>>,
    immutable_artifacts: BTreeMap<String, Value>,
}

static EVIDENCE: LazyLock<Mutex<ExecutionEvidence>> = LazyLock::new(Mutex::default);

fn evidence() -> &'static Mutex<ExecutionEvidence> {
    &EVIDENCE
}

/// Record runner work without changing the causal workload stdout protocol.
pub fn record_execution_stage(stage: &str, elapsed: Duration, bytes: u64, records: u64) {
    record_stage(stage, elapsed, bytes, records, 0);
}

fn record_stage(stage: &str, elapsed: Duration, bytes: u64, records: u64, wakes: u64) {
    let mut evidence = evidence().lock().unwrap_or_else(|error| error.into_inner());
    let entry = evidence.stages.entry(stage.to_owned()).or_default();
    entry.count = entry.count.saturating_add(1);
    entry.elapsed_ns = entry.elapsed_ns.saturating_add(elapsed.as_nanos());
    entry.max_elapsed_ns = entry.max_elapsed_ns.max(elapsed.as_nanos());
    entry.bytes = entry.bytes.saturating_add(bytes);
    entry.records = entry.records.saturating_add(records);
    entry.wake_count = entry.wake_count.saturating_add(wakes);
}

fn record_pending(predicate: &str, reason: &str) {
    let mut evidence = evidence().lock().unwrap_or_else(|error| error.into_inner());
    let count = evidence
        .pending
        .entry(predicate.to_owned())
        .or_default()
        .entry(reason.to_owned())
        .or_default();
    *count = count.saturating_add(1);
}

/// Retain verified source/build/image identities alongside timing evidence.
pub(crate) fn record_execution_identity(name: &str, identity: Value) {
    evidence()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .immutable_artifacts
        .insert(name.to_owned(), identity);
}

/// A versioned cumulative snapshot persisted at phase and failure boundaries,
/// separately from observations used to establish causal ordering.
pub fn execution_evidence() -> Value {
    let evidence = evidence().lock().unwrap_or_else(|error| error.into_inner());
    json!({
        "schema_version": 1,
        "clock": "monotonic",
        "stages": evidence.stages,
        "pending_predicates": evidence.pending,
        "immutable_artifacts": evidence.immutable_artifacts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let parent = Budget::new(Duration::from_secs(30));
        let child = parent.child(Duration::from_secs(30));
        let started = Instant::now();
        let waiter =
            std::thread::spawn(move || child.wait(Duration::from_secs(20), "withheld event"));
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
