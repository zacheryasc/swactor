//! Cross-task notification for on-demand snapshots (T1.4 pull-trigger).
//!
//! The collector signals "snapshot now" via a [`Hints`] block returned
//! on any POST response (`DIAGNOSTICS_PLAN.md` T1.4). The drainer task
//! inside the [`HttpSink`] sees the hint, but the snapshot is taken by
//! the [`Aggregator`]'s periodic task. [`SnapshotSignal`] is the
//! shared notification carrier that lets the drainer wake the periodic
//! task.
//!
//! The signal uses [`tokio::sync::Notify`] under the hood, so it costs
//! nothing while idle and is cheap to clone. Hints are coalesced: if
//! the periodic task has not yet drained the previous signal, a second
//! `request()` is absorbed.
//!
//! [`Hints`]: super::collector::Hints
//! [`HttpSink`]: super::HttpSink
//! [`Aggregator`]: super::Aggregator

use std::sync::Arc;

use tokio::sync::Notify;

/// Cloneable handle to a one-permit notification. The drainer side
/// calls [`Self::request`]; the snapshot task awaits [`Self::wait`].
#[derive(Clone, Debug, Default)]
pub struct SnapshotSignal {
    inner: Arc<Notify>,
}

impl SnapshotSignal {
    /// Build an empty signal. Cheap; no background work.
    pub fn new() -> Self {
        Self::default()
    }

    /// Wake any task currently awaiting [`Self::wait`], or store a
    /// single permit for the next waiter. Coalescing is intentional:
    /// hints repeated faster than the snapshot task can drain them
    /// produce at most one snapshot per drain.
    pub fn request(&self) {
        self.inner.notify_one();
    }

    /// Suspend until a permit is available. Consumes the permit.
    pub async fn wait(&self) {
        self.inner.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn request_before_wait_is_observed() {
        let s = SnapshotSignal::new();
        s.request();
        // Without the stored permit this would hang; with it, wait
        // returns immediately.
        tokio::time::timeout(Duration::from_millis(200), s.wait())
            .await
            .expect("signal stored a permit and wait returned promptly");
    }

    #[tokio::test]
    async fn repeated_requests_coalesce_to_one_wakeup() {
        let s = SnapshotSignal::new();
        for _ in 0..10 {
            s.request();
        }
        // First wait drains the permit.
        s.wait().await;
        // Second wait should *not* return immediately — there is no
        // queue of pending signals, only a single permit.
        let res = tokio::time::timeout(Duration::from_millis(80), s.wait()).await;
        assert!(res.is_err(), "expected wait to time out after the single permit was consumed");
    }
}
