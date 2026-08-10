//! Deterministic single-threaded test execution backend.
//!
//! It demonstrates that the substrate-neutral engine contract works without
//! Tokio, without real async I/O, and with substitutable virtual time.
//!
//! The backend is `Clone` (shares state through `Arc`), so a test keeps one
//! copy to call [`SteppingBackend::step`] / [`SteppingBackend::advance_time`]
//! while passing another to [`Engine::new`](crate::Engine::new).
//!
//! ## How it works
//!
//! - **Tasks** are stored in a Vec and polled cooperatively. Each
//!   [`step`](Self::step) polls every live task exactly once using a noop
//!   waker — progress is driven by repeated `step` calls, not by wakeups.
//! - **Core driving** uses a self-waking driver: each poll runs one
//!   `try_tick` and re-arms via the (noop) waker, so each `step` advances the
//!   core-driving loop by exactly one tick.
//! - **Time** is virtual: [`now`](crate::ExecutionBackend::now) returns a
//!   clock the test advances explicitly via
//!   [`advance_time`](Self::advance_time). Timers compare against this clock.
//! - **Blocking** work runs on a real `std::thread` (truthful isolation),
//!   joined when the backend's last clone drops.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::backend::{BoxTask, BoxTimer, BoxWork, Capabilities, ExecutionBackend};
use crate::time::EngineInstant;

// ── Noop waker ──────────────────────────────────────────────────────────────

/// A waker whose `wake` is a no-op. The stepping executor polls all tasks
/// unconditionally on each [`step`](SteppingBackend::step), so it never relies
/// on wakeups for rescheduling.
struct NoopWaker;

impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}

fn noop_waker() -> Waker {
    Waker::from(Arc::new(NoopWaker))
}

// ── SteppingTimer future ────────────────────────────────────────────────────

/// A timer future driven by the stepping backend's virtual clock.
///
/// Completes when the virtual clock reaches `deadline`. Checked on each poll,
/// so it fires as soon as [`advance_time`](SteppingBackend::advance_time) has
/// moved the clock far enough.
struct SteppingTimer {
    deadline: Instant,
    clock: Arc<Mutex<Instant>>,
}

impl Future for SteppingTimer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if *self.clock.lock() >= self.deadline {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

// ── SteppingInner ───────────────────────────────────────────────────────────

/// Shared inner state behind [`SteppingBackend`].
struct SteppingInner {
    /// Live spawned tasks, polled on each `step`.
    tasks: Mutex<Vec<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>>,
    /// Handles for spawned blocking threads, joined on drop.
    blocking: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl Drop for SteppingInner {
    fn drop(&mut self) {
        let handles: Vec<_> = self.blocking.lock().drain(..).collect();
        for handle in handles {
            let _ = handle.join();
        }
    }
}

// ── SteppingBackend ─────────────────────────────────────────────────────────

/// A deterministic, single-threaded, test-controlled execution backend.
///
/// Clone to obtain a controller handle: one clone goes to
/// [`Engine::new`](crate::Engine::new), the test keeps another to drive
/// execution via [`step`](Self::step) and [`advance_time`](Self::advance_time).
///
/// This backend advertises `tasks`, `timers`, and `blocking` but **not** `io`.
/// It proves the engine contract is substrate-neutral: core progresses,
/// supporting work progresses, and engine time is substitutable — all without
/// Tokio (ENGINE_SPEC.md).
#[derive(Clone)]
pub struct SteppingBackend {
    inner: Arc<SteppingInner>,
    /// Virtual monotonic clock, shared with timers. Kept as a separate
    /// `Arc<Mutex<Instant>>` so timer futures are self-contained.
    clock: Arc<Mutex<Instant>>,
}

impl Default for SteppingBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl SteppingBackend {
    /// Create a new stepping backend with virtual time starting at `Instant::now()`.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SteppingInner {
                tasks: Mutex::new(Vec::new()),
                blocking: Mutex::new(Vec::new()),
            }),
            clock: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Create a stepping backend whose virtual clock starts at `start`.
    ///
    /// Useful for deterministic tests that need a known clock origin.
    pub fn with_clock(start: Instant) -> Self {
        Self {
            inner: Arc::new(SteppingInner {
                tasks: Mutex::new(Vec::new()),
                blocking: Mutex::new(Vec::new()),
            }),
            clock: Arc::new(Mutex::new(start)),
        }
    }

    /// Poll every live task exactly once.
    ///
    /// Each call advances the core-driving loop by one tick and gives every
    /// spawned task one scheduling turn. Call repeatedly to drive execution.
    pub fn step(&self) {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Drain all tasks, poll each, keep those still alive. New tasks
        // spawned during polling land in the mutex; we merge them back after.
        let mut tasks: Vec<_> = self.inner.tasks.lock().drain(..).collect();
        let mut alive = Vec::with_capacity(tasks.len());
        for mut task in tasks.drain(..) {
            if task.as_mut().poll(&mut cx).is_pending() {
                alive.push(task);
            }
            // Ready tasks are dropped — their futures have completed.
        }
        self.inner.tasks.lock().extend(alive);
    }

    /// Advance the virtual clock by `duration`.
    ///
    /// Pending timers fire on the next [`step`](Self::step) after their
    /// deadline has been reached.
    pub fn advance_time(&self, duration: Duration) {
        let mut now = self.clock.lock();
        *now += duration;
    }

    /// Read the current virtual time.
    pub fn virtual_now(&self) -> EngineInstant {
        EngineInstant {
            instant: *self.clock.lock(),
        }
    }

    /// Number of live (not-yet-completed) tasks in the queue.
    pub fn pending_task_count(&self) -> usize {
        self.inner.tasks.lock().len()
    }
}

/// This impl is the substrate implementor for the deterministic stepping
/// backend; blocking work runs on a std thread (ENGINE_SPEC.md §2).
#[allow(clippy::disallowed_methods)]
impl ExecutionBackend for SteppingBackend {
    fn spawn(&self, task: BoxTask) {
        self.inner.tasks.lock().push(task);
    }

    fn spawn_blocking(&self, work: BoxWork) {
        let handle = std::thread::spawn(work);
        self.inner.blocking.lock().push(handle);
    }

    fn timer(&self, delay: Duration) -> BoxTimer {
        let deadline = *self.clock.lock() + delay;
        Box::pin(SteppingTimer {
            deadline,
            clock: Arc::clone(&self.clock),
        })
    }

    fn now(&self) -> EngineInstant {
        EngineInstant {
            instant: *self.clock.lock(),
        }
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            tasks: true,
            timers: true,
            blocking: true,
            io: false,
        }
    }
}
