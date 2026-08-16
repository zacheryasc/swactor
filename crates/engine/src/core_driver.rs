//! Core driving loop.
//!
//! Substrate-neutral: each driver owns one core worker. A poll runs one
//! [`Worker::try_tick`] — synchronous, returns immediately — then re-arms
//! itself. While its worker keeps doing work the driver re-arms immediately by
//! waking its own waker; once a tick finds no work the driver parks on a
//! backend timer for the backend's idle interval instead of spinning at
//! scheduler speed. There is no inbox-wake or readiness mechanism; the next
//! poll — self-wake or timer — observes newly delivered messages
//! (ENGINE_SPEC.md).

use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use crate::backend::{BoxTask, BoxTimer, ExecutionBackend};
use swactor::worker::Worker;

/// One core-driving loop for one worker.
///
/// Each poll runs one [`Worker::try_tick`] — synchronous, returns immediately.
/// The re-arm policy depends on the outcome and the backend's
/// [`core_idle_poll`](ExecutionBackend::core_idle_poll):
///
/// - The worker did work, or the interval is zero: re-arm immediately via
///   `cx.waker().wake_by_ref()`, handing control back to the substrate
///   scheduler between ticks so other engine work progresses.
/// - The worker was idle: arm one backend timer for the idle interval and park
///   on it. The timer firing re-polls, so newly delivered work is observed
///   within one interval.
///
/// Either way every poll ticks exactly once. The driver allocates nothing per
/// busy turn and touches the backend only on idle transitions, never on the
/// busy path (ENGINE_SPEC.md §8).
///
/// On the stepping test backend the default interval is zero and the waker is
/// a no-op; each `step` re-polls every task, so one `step` still advances the
/// driver by exactly one tick.
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
    /// Weak backend reference, upgraded only when arming an idle timer.
    backend: Weak<dyn ExecutionBackend>,
    /// Idle park interval; zero means re-arm immediately after every tick.
    idle_poll: Duration,
    /// The armed idle timer, present only while parked.
    idle_timer: Option<BoxTimer>,
}

// Core drivers are the engine's sole core-progression path; this is the one
// place permitted to call `Worker::try_tick` (ENGINE_SPEC.md §2).
#[allow(clippy::disallowed_methods)]
impl Future for CoreDriver {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // `CoreDriver` is `Unpin`; it contains no self-referential state.
        let this = self.get_mut();
        let did_work = this.worker.try_tick();

        if did_work || this.idle_poll.is_zero() {
            // Busy (or the backend has no idle parking): drop any armed
            // timer and re-arm immediately. The substrate scheduler
            // redispatches this task, yielding to other engine work between
            // ticks.
            this.idle_timer = None;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        // Idle: park on a backend timer instead of spinning.
        if this.idle_timer.is_none() {
            let Some(backend) = this.backend.upgrade() else {
                // Engine dropped: the substrate cancels this task on backend
                // drop, so park rather than spin.
                return Poll::Pending;
            };
            this.idle_timer = Some(backend.timer(this.idle_poll));
        }
        // Poll the armed timer to register `cx` with it; when it fires the
        // task is woken, re-polled, and ticks again.
        if this
            .idle_timer
            .as_mut()
            .unwrap()
            .as_mut()
            .poll(cx)
            .is_ready()
        {
            this.idle_timer = None;
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

/// Install one core-driving loop per worker onto `backend`.
///
/// One task is allocated per worker at engine construction and runs for the
/// engine's lifetime; the substrate cancels it when the backend is dropped.
pub(crate) fn install(workers: Vec<Worker>, backend: &Arc<dyn ExecutionBackend>) {
    let idle_poll = backend.core_idle_poll();
    for worker in workers {
        let driver: BoxTask = Box::pin(CoreDriver {
            worker,
            backend: Arc::downgrade(backend),
            idle_poll,
            idle_timer: None,
        });
        backend.spawn(driver);
    }
}
