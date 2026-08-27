//! Composite engine and cloneable scheduler handle.

use parking_lot::{Condvar, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::backend::{Capabilities, EngineError, ExecutionBackend};
use crate::time::{EngineInstant, Interval, Timeout, Timer};
use swactor::actor::{ActorAddress, Message};
use swactor::runtime::{ExternalSender, Runtime, RuntimeParts};

/// The composite engine: retains a configured core runtime handle and its
/// execution backend, and owns one core-driving loop per worker.
///
/// Construct with [`Engine::new`]; obtain a scheduler handle with
/// [`Engine::handle`].
pub struct Engine {
    /// Retained so the engine owns the runtime handle it drives for its full
    /// lifetime. Core workers are moved into substrate tasks at construction.
    _runtime: Runtime,
    backend: Arc<dyn ExecutionBackend>,
}

impl Engine {
    /// Construct an engine over `parts` driven by `backend`.
    ///
    /// The runtime parts must be fully configured beforehand; after construction
    /// the engine owns every worker and is their sole driver. Construction fails
    /// if `backend` does not advertise a capability the engine requires (at
    /// minimum, `tasks`).
    pub fn new(parts: RuntimeParts, backend: impl ExecutionBackend) -> Result<Self, EngineError> {
        let backend: Arc<dyn ExecutionBackend> = Arc::new(backend);
        if !backend.capabilities().tasks {
            return Err(EngineError::MissingRequiredCapability);
        }
        let runtime = parts.runtime().clone();
        let workers = parts.into_workers();
        // Install one core-driving loop per worker. This is substrate-neutral —
        // no Tokio feature gate — so core progression does not silently
        // disappear when an alternate backend is used (ENGINE_SPEC.md).
        crate::core_driver::install(workers, &backend);
        Ok(Engine {
            _runtime: runtime,
            backend,
        })
    }

    /// Return a clonable handle for scheduling engine work.
    ///
    /// The handle holds a *weak* backend reference, so handles — and engine
    /// work that captures them — never keep the backend alive. Dropping the
    /// [`Engine`] releases the backend (and its owned runtime / core-driver
    /// task) once no other strong reference remains (ENGINE_SPEC.md).
    pub fn handle(&self) -> EngineHandle {
        EngineHandle {
            backend: Arc::downgrade(&self.backend),
        }
    }
}

/// A cloneable scheduler handle.
///
/// Schedules work and reads engine time without exposing the underlying
/// backend; in particular it never hands out a raw `tokio::runtime::Handle`.
/// The handle holds a **weak** backend reference: it does not keep the engine
/// or its backend alive. Using a handle after its engine has been dropped
/// degrades gracefully — scheduled work is dropped, timers never fire, and
/// capability checks report no capabilities — rather than retaining the
/// backend (ENGINE_SPEC.md).
#[derive(Clone)]
pub struct EngineHandle {
    backend: Weak<dyn ExecutionBackend>,
}
/// A clonable substrate route for one-shot blocking I/O work.
///
/// Domain actors decide which effect to execute; this handle only moves its
/// mechanics onto the engine backend's dedicated blocking pool.
#[derive(Clone)]
pub struct BlockingWorkSender {
    backend: Weak<dyn ExecutionBackend>,
}

impl BlockingWorkSender {
    /// Submit one blocking I/O operation without occupying an actor worker.
    ///
    /// Returns the operation unchanged if the owning engine has stopped.
    pub fn submit(&self, work: crate::BoxWork) -> Result<(), crate::BoxWork> {
        let Some(backend) = self.backend.upgrade() else {
            return Err(work);
        };
        backend.spawn_blocking(work);
        Ok(())
    }
}

/// Cancellation handle for an engine-owned actor message timer.
///
/// Cancellation is idempotent. A timer may already have fired when cancellation
/// races its deadline, so actor messages should still carry an operation or
/// generation identity that lets the receiver reject stale work.
#[derive(Clone, Debug)]
pub struct ActorTimer {
    cancelled: Arc<AtomicBool>,
}

impl ActorTimer {
    /// Prevent this timer from delivering future messages.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Report whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

enum CompletionState<T> {
    Pending,
    Ready(T),
    Consumed,
}

/// One actor-owned terminal observation for a synchronous process entrypoint.
///
/// The waiting thread cannot poll or advance domain state. An actor decides
/// when the operation is complete and publishes the value; [`Self::wait`]
/// blocks until then, while [`Self::wait_deadline`] bounds the wait for
/// entrypoints whose failure must be observable even if the actor stalls.
///
pub struct ActorCompletion<T> {
    inner: Arc<(Mutex<CompletionState<T>>, Condvar)>,
}

impl<T> Clone for ActorCompletion<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Default for ActorCompletion<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ActorCompletion<T> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new((Mutex::new(CompletionState::Pending), Condvar::new())),
        }
    }

    /// Publish the terminal observation once.
    pub fn complete(&self, value: T) -> Result<(), T> {
        let (state, ready) = &*self.inner;
        let mut state = state.lock();
        if !matches!(*state, CompletionState::Pending) {
            return Err(value);
        }
        *state = CompletionState::Ready(value);
        ready.notify_all();
        Ok(())
    }

    /// Block the process entrypoint until the owning actor completes.
    pub fn wait(&self) -> T {
        let (state, ready) = &*self.inner;
        let mut state = state.lock();
        loop {
            if matches!(*state, CompletionState::Pending) {
                ready.wait(&mut state);
                continue;
            }
            match std::mem::replace(&mut *state, CompletionState::Consumed) {
                CompletionState::Ready(value) => return value,
                CompletionState::Consumed => {
                    panic!("ActorCompletion::wait called after value was consumed")
                }
                CompletionState::Pending => unreachable!("pending state handled above"),
            }
        }
    }

    /// Block until the owning actor completes or `timeout` elapses.
    ///
    /// Returns `None` on timeout with the completion still pending, so a
    /// bootstrap entrypoint can fail boundedly instead of hanging forever.
    pub fn wait_deadline(&self, timeout: Duration) -> Option<T> {
        let (state, ready) = &*self.inner;
        let mut state = state.lock();
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if !matches!(*state, CompletionState::Pending) {
                return match std::mem::replace(&mut *state, CompletionState::Consumed) {
                    CompletionState::Ready(value) => Some(value),
                    CompletionState::Consumed => {
                        panic!("ActorCompletion::wait called after value was consumed")
                    }
                    CompletionState::Pending => unreachable!("pending state handled above"),
                };
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return None;
            }
            ready.wait_until(&mut state, deadline);
        }
    }
}

impl EngineHandle {
    /// Upgrade to the live backend, or `None` if the owning engine is gone.
    fn backend(&self) -> Option<Arc<dyn ExecutionBackend>> {
        self.backend.upgrade()
    }

    /// Schedule `task` as cooperative engine work.
    ///
    /// A no-op once the owning engine has been dropped: the work is discarded
    /// rather than keeping the backend alive.
    pub fn spawn<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if let Some(backend) = self.backend() {
            backend.spawn(Box::pin(task));
        }
    }
    /// Create a route to the backend's dedicated blocking-I/O pool.
    pub fn blocking_work_sender(&self) -> BlockingWorkSender {
        BlockingWorkSender {
            backend: self.backend.clone(),
        }
    }

    /// Produce a future that completes after `delay`.
    ///
    /// Once the owning engine has been dropped this returns a timer that never
    /// fires.
    pub fn timer(&self, delay: Duration) -> Timer {
        match self.backend() {
            Some(backend) => Timer {
                inner: backend.timer(delay),
            },
            None => Timer::closed(),
        }
    }

    /// Produce a future that recurs every `period`.
    pub fn interval(&self, period: Duration) -> Interval {
        Interval {
            period,
            backend: self.backend.clone(),
            current: None,
        }
    }

    /// Schedule one typed message for delivery after `delay`.
    ///
    /// The engine owns the timer task; domain code receives no future or
    /// scheduling callback. The actor receiving `message` owns the deadline
    /// decision and should reject stale operation identities.
    pub fn send_after<M>(
        &self,
        delay: Duration,
        sender: ExternalSender,
        actor: ActorAddress,
        message: M,
    ) -> ActorTimer
    where
        M: Message,
    {
        let actor_timer = ActorTimer {
            cancelled: Arc::new(AtomicBool::new(false)),
        };
        let cancelled = Arc::clone(&actor_timer.cancelled);
        let timer = self.timer(delay);
        self.spawn(async move {
            timer.await;
            if !cancelled.load(Ordering::Acquire) {
                let _ = sender.send_to(actor, message);
            }
        });
        actor_timer
    }

    /// Schedule a cloned typed message after every `period`.
    ///
    /// Delivery stops after cancellation or when the actor address no longer
    /// accepts messages.
    pub fn send_every<M>(
        &self,
        period: Duration,
        sender: ExternalSender,
        actor: ActorAddress,
        message: M,
    ) -> ActorTimer
    where
        M: Message,
    {
        let actor_timer = ActorTimer {
            cancelled: Arc::new(AtomicBool::new(false)),
        };
        let cancelled = Arc::clone(&actor_timer.cancelled);
        let handle = self.clone();
        let mut interval = Box::pin(handle.interval(period));
        self.spawn(std::future::poll_fn(move |cx| {
            if cancelled.load(Ordering::Acquire) {
                return std::task::Poll::Ready(());
            }
            match std::future::Future::poll(interval.as_mut(), cx) {
                std::task::Poll::Ready(()) => {
                    if cancelled.load(Ordering::Acquire)
                        || sender.send_to(actor, message.clone()).is_err()
                    {
                        std::task::Poll::Ready(())
                    } else {
                        cx.waker().wake_by_ref();
                        std::task::Poll::Pending
                    }
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        }));
        actor_timer
    }
    /// Race `future` against an engine timer.
    ///
    /// Resolves to `Ok` with the future's output if it completes within
    /// `duration`, or [`Err(Elapsed)`](crate::Elapsed) when the timer fires
    pub fn timeout<F: std::future::Future>(&self, duration: Duration, future: F) -> Timeout<F> {
        Timeout::new(self.timer(duration), future)
    }

    /// Read the engine's monotonic clock.
    ///
    /// Falls back to the real wall clock once the owning engine has been
    /// dropped, since the substrate clock is no longer available.
    pub fn now(&self) -> EngineInstant {
        match self.backend() {
            Some(backend) => backend.now(),
            None => EngineInstant::now(),
        }
    }

    /// Report the backend's advertised capabilities.
    ///
    /// Reports no capabilities once the owning engine has been dropped.
    pub fn capabilities(&self) -> Capabilities {
        match self.backend() {
            Some(backend) => backend.capabilities(),
            None => Capabilities::NONE,
        }
    }

    /// Validate that this engine satisfies `required` before starting work.
    ///
    /// Returns `Err` if the backend cannot provide a requested capability, or
    /// if the owning engine has been dropped. Call this before allocating
    /// resources, starting background work, or becoming externally visible so
    /// that an incompatible engine is rejected early (ENGINE_SPEC.md).
    pub fn require(&self, required: Capabilities) -> Result<(), EngineError> {
        match self.backend() {
            Some(backend) if backend.capabilities().satisfies(required) => Ok(()),
            _ => Err(EngineError::MissingRequiredCapability),
        }
    }
}
