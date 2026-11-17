//! Composite engine and explicit execution capabilities.

use parking_lot::{Condvar, Mutex};
use std::future::poll_fn;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Poll, Waker};
use std::time::Duration;

use crate::backend::{Capabilities, EngineError, ExecutionBackend};
use crate::time::{EngineInstant, Interval, Timeout, Timer};
use swactor::actor::{ActorAddress, Message};
use swactor::runtime::{ExternalSender, Runtime, RuntimeParts};

/// The composite engine: retains a configured core runtime handle and its
/// execution backend, and owns one core-driving loop per worker.
///
/// Construct with [`Engine::new`]; obtain clock/substrate access with
/// [`Engine::handle`] and task authority with [`Engine::task_spawner`].
pub struct Engine {
    backend: Arc<dyn ExecutionBackend>,
    /// Retained so the engine owns the runtime handle it drives for its full
    /// lifetime. Core workers are moved into substrate tasks at construction.
    _runtime: Runtime,
    ownership: Arc<OwnershipCounters>,
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
            backend,
            _runtime: runtime,
            ownership: Arc::new(OwnershipCounters::default()),
        })
    }

    /// Return a clonable handle for engine time and blocking-I/O submission.
    ///
    /// The handle holds a *weak* backend reference, so handles — and engine
    /// work that captures them — never keep the backend alive. Dropping the
    /// [`Engine`] releases the backend (and its owned runtime / core-driver
    /// task) once no other strong reference remains (ENGINE_SPEC.md).
    pub fn handle(&self) -> EngineHandle {
        EngineHandle {
            backend: Arc::downgrade(&self.backend),
            ownership: Arc::clone(&self.ownership),
        }
    }

    /// Mint task authority at a binary entrypoint or test composition root.
    ///
    /// Pass this capability explicitly to adapters that own engine-hosted work.
    /// Actors must not own or receive it. Like [`EngineHandle`], it retains the
    /// backend weakly and cannot extend the engine's lifetime.
    pub fn task_spawner(&self) -> TaskSpawner {
        TaskSpawner {
            backend: Arc::downgrade(&self.backend),
            ownership: Arc::clone(&self.ownership),
        }
    }
}

/// Cloneable engine clock and substrate access, without task-creation authority.
///
/// This handle never exposes the underlying runtime and holds the backend
/// weakly. After engine shutdown, timers never fire and capability checks
/// report no capabilities.
#[derive(Clone)]
pub struct EngineHandle {
    backend: Weak<dyn ExecutionBackend>,
    ownership: Arc<OwnershipCounters>,
}

/// Explicit authority to schedule engine-owned asynchronous work.
///
/// Only an [`Engine`] can mint this capability. Composition roots supply it
/// to execution adapters, never to actors or actor messages. A retained clone
/// does not keep the engine alive.
#[derive(Clone)]
pub struct TaskSpawner {
    backend: Weak<dyn ExecutionBackend>,
    ownership: Arc<OwnershipCounters>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EngineOwnershipSnapshot {
    pub tasks: u64,
    pub actor_timers: u64,
    pub blocking: u64,
}

#[derive(Default)]
struct OwnershipCounters {
    tasks: AtomicU64,
    actor_timers: AtomicU64,
    blocking: AtomicU64,
}

#[derive(Clone, Copy)]
enum OwnershipKind {
    Task,
    ActorTimer,
    Blocking,
}

struct OwnershipGuard {
    counters: Arc<OwnershipCounters>,
    kind: OwnershipKind,
}

impl OwnershipCounters {
    fn begin(self: &Arc<Self>, kind: OwnershipKind) -> OwnershipGuard {
        let counter = match kind {
            OwnershipKind::Task => &self.tasks,
            OwnershipKind::ActorTimer => &self.actor_timers,
            OwnershipKind::Blocking => &self.blocking,
        };
        counter.fetch_add(1, Ordering::AcqRel);
        OwnershipGuard {
            counters: Arc::clone(self),
            kind,
        }
    }

    fn snapshot(&self) -> EngineOwnershipSnapshot {
        EngineOwnershipSnapshot {
            tasks: self.tasks.load(Ordering::Acquire),
            actor_timers: self.actor_timers.load(Ordering::Acquire),
            blocking: self.blocking.load(Ordering::Acquire),
        }
    }
}

impl Drop for OwnershipGuard {
    fn drop(&mut self) {
        let counter = match self.kind {
            OwnershipKind::Task => &self.counters.tasks,
            OwnershipKind::ActorTimer => &self.counters.actor_timers,
            OwnershipKind::Blocking => &self.counters.blocking,
        };
        let previous = counter.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(previous, 0, "engine ownership counter underflow");
    }
}
impl TaskSpawner {
    /// Schedule cooperative work, discarding it if the engine has stopped.
    pub fn spawn<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if let Some(backend) = self.backend.upgrade() {
            let guard = self.ownership.begin(OwnershipKind::Task);
            backend.spawn(Box::pin(async move {
                let _guard = guard;
                task.await;
            }));
        }
    }
}

/// A clonable engine capability for explicitly owned blocking I/O work.
///
/// Finite work uses the backend's blocking pool; resource-owned joined I/O
/// has independent progress so it cannot deadlock a caller using that pool.
#[derive(Clone)]
pub struct BlockingWorkSender {
    backend: Weak<dyn ExecutionBackend>,
    ownership: Arc<OwnershipCounters>,
}

impl BlockingWorkSender {
    /// Submit one blocking I/O operation without occupying an actor worker.
    ///
    /// Returns the operation unchanged if the owning engine has stopped.
    pub fn submit(&self, work: crate::BoxWork) -> Result<(), crate::BoxWork> {
        let Some(backend) = self.backend.upgrade() else {
            return Err(work);
        };
        let guard = self.ownership.begin(OwnershipKind::Blocking);
        backend.spawn_blocking(Box::new(move || {
            let _guard = guard;
            work();
        }));
        Ok(())
    }

    /// Submit owned I/O state, returning it unchanged if the engine has stopped.
    ///
    /// Unlike constructing an erased closure first, this preserves resource
    /// ownership on rejection without an extra shared allocation.
    pub fn submit_owned<T, F>(&self, value: T, work: F) -> Result<(), T>
    where
        T: Send + 'static,
        F: FnOnce(T) + Send + 'static,
    {
        let Some(backend) = self.backend.upgrade() else {
            return Err(value);
        };
        let guard = self.ownership.begin(OwnershipKind::Blocking);
        backend.spawn_blocking(Box::new(move || {
            let _guard = guard;
            work(value);
        }));
        Ok(())
    }

    /// Submit blocking work whose completion must be joined by its I/O owner.
    ///
    /// Panics are returned by [`BlockingWorkHandle::join`]. If queued work is
    /// discarded during backend shutdown, joining also returns an error rather
    /// than waiting forever. Actors must observe completion through messages,
    /// not call `join` from a handler.
    pub fn submit_joinable<F, T>(&self, work: F) -> std::io::Result<BlockingWorkHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let Some(backend) = self.backend.upgrade() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "engine stopped",
            ));
        };
        let inner = Arc::new((Mutex::new(None), Condvar::new()));
        let mut completion = BlockingCompletion {
            inner: Arc::clone(&inner),
            completed: false,
            ownership: Some(self.ownership.begin(OwnershipKind::Blocking)),
        };
        backend.spawn_blocking(Box::new(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work));
            drop(completion.ownership.take());
            completion.completed = true;
            *completion.inner.0.lock() = Some(result);
            completion.inner.1.notify_all();
        }));
        Ok(BlockingWorkHandle { inner })
    }

    /// Start resource-owned I/O that must progress independently of the
    /// backend's bounded blocking pool.
    ///
    /// The engine accounts for the thread, but its resource owner must retain
    /// the returned handle and release any I/O cancellation/EOF condition before
    /// joining or dropping it. Dropping the handle joins; it never detaches.
    pub fn spawn_owned_thread<F, T>(
        &self,
        name: &'static str,
        work: F,
    ) -> std::io::Result<OwnedIoThread<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let Some(backend) = self.backend.upgrade() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "engine stopped",
            ));
        };
        if !backend.capabilities().blocking {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "engine has no blocking I/O capability",
            ));
        }
        let ownership = self.ownership.begin(OwnershipKind::Blocking);
        let join = std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                let _ownership = ownership;
                work()
            })?;
        Ok(OwnedIoThread { join: Some(join) })
    }
}

/// An engine-accounted I/O thread whose resource owner must join completion.
/// Unlike a raw join handle, dropping this value also joins the thread.
#[must_use = "retain the I/O owner until its cancellation or completion is joined"]
pub struct OwnedIoThread<T> {
    join: Option<std::thread::JoinHandle<T>>,
}

impl<T> OwnedIoThread<T> {
    pub fn is_finished(&self) -> bool {
        self.join
            .as_ref()
            .is_none_or(std::thread::JoinHandle::is_finished)
    }

    pub fn join(mut self) -> std::thread::Result<T> {
        self.join
            .take()
            .expect("owned I/O thread already joined")
            .join()
    }
}

impl<T> Drop for OwnedIoThread<T> {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

type BlockingResult<T> = Arc<(Mutex<Option<std::thread::Result<T>>>, Condvar)>;

/// Completion of engine-owned blocking work, without exposing an OS thread.
pub struct BlockingWorkHandle<T> {
    inner: BlockingResult<T>,
}

impl<T> BlockingWorkHandle<T> {
    /// Whether work has completed, panicked, or been discarded during shutdown.
    pub fn is_finished(&self) -> bool {
        self.inner.0.lock().is_some()
    }

    /// Wait for the terminal result. Never call this from an actor handler.
    pub fn join(self) -> std::thread::Result<T> {
        let mut result = self.inner.0.lock();
        loop {
            if let Some(result) = result.take() {
                return result;
            }
            self.inner.1.wait(&mut result);
        }
    }
}

struct BlockingCompletion<T> {
    inner: BlockingResult<T>,
    completed: bool,
    ownership: Option<OwnershipGuard>,
}

impl<T> Drop for BlockingCompletion<T> {
    fn drop(&mut self) {
        drop(self.ownership.take());
        if self.completed {
            return;
        }
        let mut result = self.inner.0.lock();
        if result.is_none() {
            *result = Some(Err(Box::new("engine discarded pending blocking work")));
            self.inner.1.notify_all();
        }
    }
}

/// Cancellation handle for an engine-owned actor message timer.
///
/// Cancellation is idempotent. A timer may already have fired when cancellation
/// races its deadline, so actor messages should still carry an operation or
/// generation identity that lets the receiver reject stale work.
#[derive(Debug)]
struct TimerCancellation {
    cancelled: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl TimerCancellation {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(waker) = self.waker.lock().take() {
            waker.wake();
        }
    }

    fn register(&self, waker: &Waker) {
        let mut slot = self.waker.lock();
        if slot
            .as_ref()
            .is_none_or(|current| !current.will_wake(waker))
        {
            *slot = Some(waker.clone());
        }
    }
}

/// Cancellation handle for an engine-owned actor message timer.
#[derive(Clone, Debug)]
pub struct ActorTimer {
    cancellation: Arc<TimerCancellation>,
}

impl ActorTimer {
    /// Prevent this timer from delivering future messages and wake its owner so
    /// cancellation releases the timer without waiting for its deadline.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Report whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.cancelled.load(Ordering::Acquire)
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

    /// Create a route to the backend's dedicated blocking-I/O pool.
    pub fn blocking_work_sender(&self) -> BlockingWorkSender {
        BlockingWorkSender {
            backend: self.backend.clone(),
            ownership: Arc::clone(&self.ownership),
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
        let cancellation = Arc::new(TimerCancellation {
            cancelled: AtomicBool::new(false),
            waker: Mutex::new(None),
        });
        let actor_timer = ActorTimer {
            cancellation: Arc::clone(&cancellation),
        };
        let mut timer = Box::pin(self.timer(delay));
        if let Some(backend) = self.backend() {
            let guard = self.ownership.begin(OwnershipKind::ActorTimer);
            backend.spawn(Box::pin(poll_fn(move |cx| {
                let _guard = &guard;
                if cancellation.cancelled.load(Ordering::Acquire) {
                    return Poll::Ready(());
                }
                cancellation.register(cx.waker());
                if cancellation.cancelled.load(Ordering::Acquire) {
                    return Poll::Ready(());
                }
                if Future::poll(timer.as_mut(), cx).is_ready() {
                    let _ = sender.send_to(actor, message.clone());
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })));
        }
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
        let cancellation = Arc::new(TimerCancellation {
            cancelled: AtomicBool::new(false),
            waker: Mutex::new(None),
        });
        let actor_timer = ActorTimer {
            cancellation: Arc::clone(&cancellation),
        };
        let handle = self.clone();
        let mut interval = Box::pin(handle.interval(period));
        if let Some(backend) = self.backend() {
            let guard = self.ownership.begin(OwnershipKind::ActorTimer);
            backend.spawn(Box::pin(poll_fn(move |cx| {
                let _guard = &guard;
                if cancellation.cancelled.load(Ordering::Acquire) {
                    return Poll::Ready(());
                }
                cancellation.register(cx.waker());
                if cancellation.cancelled.load(Ordering::Acquire) {
                    return Poll::Ready(());
                }
                match Future::poll(interval.as_mut(), cx) {
                    Poll::Ready(()) => {
                        if cancellation.cancelled.load(Ordering::Acquire)
                            || sender.send_to(actor, message.clone()).is_err()
                        {
                            Poll::Ready(())
                        } else {
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                    }
                    Poll::Pending => Poll::Pending,
                }
            })));
        }
        actor_timer
    }
    /// Race `future` against an engine timer.
    ///
    /// Resolves to `Ok` with the future's output if it completes within
    /// `duration`, or [`Err(Elapsed)`](crate::Elapsed) when the timer fires
    pub fn timeout<F: Future>(&self, duration: Duration, future: F) -> Timeout<F> {
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

    /// Snapshot explicitly scheduled work. Core driver tasks are infrastructure
    /// and are intentionally excluded; task-spawner work, actor timers, and
    /// blocking submissions have visible ownership here.
    pub fn ownership_snapshot(&self) -> EngineOwnershipSnapshot {
        self.ownership.snapshot()
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
