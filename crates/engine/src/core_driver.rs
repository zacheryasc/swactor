//! Core driving loop.
//!
//! Substrate-neutral: the driver is one allocated task that runs one
//! [`Runtime::try_tick`] per poll — synchronous, returns immediately — then
//! re-schedules itself by waking its own waker. There is no backend reference
//! on the hot path, no per-turn boxed yield, no inbox-wake or readiness
//! mechanism, and no `has_work()` gate; later polls observe newly delivered
//! messages (ENGINE_SPEC.md).

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::backend::{BoxTask, ExecutionBackend};

/// The sole core-driving loop, installed once per engine.
///
/// Each poll runs one [`Runtime::try_tick`] — synchronous, returns immediately
/// — then re-arms itself via `cx.waker().wake_by_ref()` and returns `Pending`.
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
/// Only one driver is installed per engine, and `try_tick` runs to completion
/// within a poll, so the runtime's worker is never borrowed concurrently.
/// This is what makes `unsafe impl Sync` on [`Runtime`] sound under a single
/// driver (see ENGINE_SPEC.md §5/§8).
///
/// [`Runtime::try_tick`]: swactor::runtime::Runtime::try_tick
/// [`Runtime`]: swactor::runtime::Runtime
struct CoreDriver {
    runtime: Arc<swactor::runtime::Runtime>,
}

// The core driver is the engine's sole core-progression path; it is the one
// place permitted to call `Runtime::try_tick` (ENGINE_SPEC.md §2).
#[allow(clippy::disallowed_methods)]
impl Future for CoreDriver {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // `CoreDriver` is `Unpin` (`Arc<…>` is `Unpin`), so field access
        // through `Pin<&mut Self>` is sound without projection.
        self.runtime.try_tick();
        // Re-arm immediately: the substrate scheduler redispatches this task,
        // yielding to other engine work between ticks. No boxed yield is
        // allocated per turn and no backend reference is retained.
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Install the sole core-driving loop for `runtime` onto `backend`.
///
/// One task is allocated at engine construction and runs for the engine's
/// lifetime; the substrate cancels it when the backend is dropped.
pub(crate) fn install(runtime: Arc<swactor::runtime::Runtime>, backend: &Arc<dyn ExecutionBackend>) {
    let driver: BoxTask = Box::pin(CoreDriver { runtime });
    backend.spawn(driver);
}
