//! Engine time types: a monotonic clock instant, a delay timer, and a recurring
//! interval.

use std::pin::Pin;
use std::sync::Weak;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::backend::{BoxTimer, ExecutionBackend};

/// A monotonic engine time instant.
///
/// Wraps [`std::time::Instant`] on the native Tokio backend. A later
/// deterministic engine can substitute virtual time behind the same type
/// without changing engine-hosted code (see ENGINE_SPEC.md §6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EngineInstant {
    pub(crate) instant: Instant,
}

impl EngineInstant {
    /// Create an `EngineInstant` from the real monotonic clock.
    ///
    /// Engine-hosted code should prefer [`EngineHandle::now`](crate::EngineHandle::now)
    /// so that a deterministic engine can substitute virtual time. This
    /// constructor exists for contexts that need a wall-clock reference
    /// point outside the engine handle (e.g. a test mock backend).
    pub fn now() -> Self {
        Self {
            instant: Instant::now(),
        }
    }

    /// Return the underlying [`Instant`].
    ///
    /// Core protocol messages that carry `std::time::Instant` (e.g.
    /// `SwimIn::Tick { now }`) read the engine clock via
    /// [`EngineHandle::now`](crate::EngineHandle::now) and convert here, so
    /// that a deterministic engine can still substitute virtual time through
    /// the same path (ENGINE_SPEC.md).
    pub fn to_instant(self) -> Instant {
        self.instant
    }
}

/// A future that completes after a configured delay.
///
/// Constructed by [`EngineHandle::timer`](crate::EngineHandle::timer). Safe to
/// construct outside the substrate runtime (ENGINE_SPEC.md §7): the underlying
/// delay is armed lazily on first poll, which runs inside an engine task where
/// the substrate's time driver is available. The delay is therefore measured
/// from first poll, not from construction.
pub struct Timer {
    pub(crate) inner: BoxTimer,
}

impl Timer {
    /// A timer that never fires — used when the owning engine has been
    /// dropped, so a handle can still produce a [`Timer`] without keeping the
    /// backend alive (ENGINE_SPEC.md).
    pub(crate) fn closed() -> Self {
        Timer {
            inner: Box::pin(std::future::pending()),
        }
    }
}

impl Future for Timer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // `Timer` is `Unpin` (it holds only a `Pin<Box<…>>`), so projecting the
        // pin onto the inner timer is sound without `unsafe`.
        self.get_mut().inner.as_mut().poll(cx)
    }
}
/// Error returned when a [`Timeout`] elapses before the inner future completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

/// A future that races an inner future against an engine [`Timer`].
///
/// Constructed by [`EngineHandle::timeout`](crate::EngineHandle::timeout).
/// This is a composed helper — not a new backend primitive (ENGINE_SPEC.md). It polls an engine timer and the inner future; whichever completes
/// first determines the result. When the timer fires first the future resolves
/// to [`Err(Elapsed)`](Elapsed).
///
/// The inner future is boxed once at construction so the type is `Unpin`
/// regardless of `F`.
pub struct Timeout<F: Future> {
    future: Pin<Box<F>>,
    timer: Timer,
}

impl<F: Future> Timeout<F> {
    pub(crate) fn new(timer: Timer, future: F) -> Self {
        Timeout {
            future: Box::pin(future),
            timer,
        }
    }
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Timeout<F> is Unpin: Timer is Unpin and Pin<Box<F>> is always Unpin.
        let this = self.get_mut();
        if Pin::new(&mut this.timer).poll(cx).is_ready() {
            return Poll::Ready(Err(Elapsed));
        }
        if let Poll::Ready(value) = this.future.as_mut().poll(cx) {
            return Poll::Ready(Ok(value));
        }
        Poll::Pending
    }
}

/// A future that recurs at a fixed period.
///
/// Constructed by [`EngineHandle::interval`](crate::EngineHandle::interval).
/// Each completion re-arms a fresh backend timer, so awaiting it repeatedly
/// yields one ready per period. The first contract requires recurrence only;
/// it does not define whether missed periods burst, skip, or shift.
///
/// Unlike the generic `Future` convention, polling after a `Poll::Ready` is
/// defined behavior for this type: it re-arms and fires again on the next
/// period. This is what lets it be awaited in a loop.
pub struct Interval {
    pub(crate) period: Duration,
    pub(crate) backend: Weak<dyn ExecutionBackend>,
    pub(crate) current: Option<BoxTimer>,
}

impl Future for Interval {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        // Arm a timer on first poll (lazy: safe to construct off-runtime). If
        // the owning engine is gone there is no substrate to arm against, so
        // the interval simply stops firing.
        if this.current.is_none() {
            if let Some(backend) = this.backend.upgrade() {
                this.current = Some(backend.timer(this.period));
            } else {
                return Poll::Pending;
            }
        }
        // Poll the active timer; the mutable borrow ends at the `;` so the
        // re-arm assignment below is legal.
        let ready = this
            .current
            .as_mut()
            .expect("timer armed above")
            .as_mut()
            .poll(cx)
            .is_ready();
        if ready {
            // Re-arm for the next period; if the engine dropped between ticks,
            // drop the spent timer so the next poll returns Pending.
            this.current = this.backend.upgrade().map(|b| b.timer(this.period));
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
