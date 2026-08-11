//! Core driving loop.
//!
//! Substrate-neutral: each driver owns one core worker. A poll runs one
//! [`Worker::try_tick`] — synchronous, returns immediately — then re-schedules
//! itself by waking its own waker. There is no backend reference on the hot path,
//! no per-turn boxed yield, no inbox-wake or readiness mechanism, and no
//! `has_work()` gate; later polls observe newly delivered messages
//! (ENGINE_SPEC.md).

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::backend::{BoxTask, ExecutionBackend};
use swactor::worker::Worker;


/// One core-driving loop for one worker.
///
/// Each poll runs one [`Worker::try_tick`] — synchronous, returns immediately —
/// then re-arms itself via `cx.waker().wake_by_ref()` and returns `Pending`.
/// Rescheduling through the waker hands control back to the substrate
/// scheduler between ticks, so other engine work progresses. The driver holds
/// no backend reference and allocates nothing per turn (ENGINE_SPEC.md).
///
/// On a real executor (e.g. Tokio) `wake_by_ref` re-enqueues the task rather
/// than re-polling inline, so other ready tasks run between ticks. On the
/// stepping test backend the waker is a no-op and each `step` re-polls every
/// task, so one `step` still advances the driver by exactly one tick.
///
/// # Non-reentrancy
/// Each worker is moved into exactly one driver, and `try_tick` runs to
/// completion within a poll, so the same worker is never borrowed concurrently
/// by the engine.
///
/// [`Worker::try_tick`]: swactor::worker::Worker::try_tick
/// [`Worker`]: swactor::worker::Worker
struct CoreDriver {
    worker: Worker,
}

// Core drivers are the engine's sole core-progression path; this is the one
// place permitted to call `Worker::try_tick` (ENGINE_SPEC.md §2).
#[allow(clippy::disallowed_methods)]
impl Future for CoreDriver {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // `CoreDriver` is `Unpin`; it contains no self-referential state.
        let this = self.get_mut();
        this.worker.try_tick();
        // Re-arm immediately: the substrate scheduler redispatches this task,
        // yielding to other engine work between ticks. No boxed yield is
        // allocated per turn and no backend reference is retained.
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Install one core-driving loop per worker onto `backend`.
///
/// One task is allocated per worker at engine construction and runs for the
/// engine's lifetime; the substrate cancels it when the backend is dropped.
pub(crate) fn install(workers: Vec<Worker>, backend: &Arc<dyn ExecutionBackend>) {
    for worker in workers {
        let driver: BoxTask = Box::pin(CoreDriver { worker });
        backend.spawn(driver);
    }
}
