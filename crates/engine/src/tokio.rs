//! Native Tokio execution backend.
//!
//! Owns a Tokio multi-threaded runtime whose `Handle` stays private. The
//! [`ExecutionBackend`](crate::ExecutionBackend) impl schedules cooperative
//! work onto that runtime; the `Handle` is never exposed through
//! [`EngineHandle`](crate::EngineHandle).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::backend::{BoxTask, BoxTimer, BoxWork, Capabilities, EngineError, ExecutionBackend};
use crate::time::EngineInstant;

/// Configuration for [`TokioBackend`].
#[derive(Debug, Clone, Copy)]
pub struct TokioConfig {
    /// Number of async worker threads backing the runtime.
    pub worker_threads: usize,
    /// How long a core driver parks between ticks while its worker is idle.
    ///
    /// Bounds the wakeup latency for work delivered to an idle worker
    /// (an external send, transport delivery, process output). Busy workers
    /// never park. `Duration::ZERO` restores the always-immediate re-arm.
    pub core_idle_poll: Duration,
}

impl Default for TokioConfig {
    fn default() -> Self {
        Self {
            worker_threads: 2,
            core_idle_poll: Duration::from_micros(500),
        }
    }
}

/// An [`ExecutionBackend`](crate::ExecutionBackend) backed by an owned Tokio
/// multi-threaded runtime.
///
/// The runtime's `Handle` is never exposed through
/// [`EngineHandle`](crate::EngineHandle).
pub struct TokioBackend {
    pub(crate) runtime: tokio::runtime::Runtime,
    core_idle_poll: Duration,
}

impl TokioBackend {
    /// Build a backend with its own Tokio runtime tuned by `config`.
    ///
    /// The runtime is owned and self-driving: its worker threads start at
    /// construction, so spawned tasks progress without an ambient runtime or a
    /// `block_on` driver. Tokio cancels spawned tasks (including the
    /// core-driving loop) on `Runtime::drop`, so dropping the backend is
    /// deterministic.
    // The engine's Tokio backend is the substrate owner: it is the one place
    // permitted to construct a Tokio runtime (ENGINE_SPEC.md §2).
    pub fn new(config: TokioConfig) -> Result<Self, EngineError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.worker_threads)
            .enable_all()
            .build()
            .map_err(|e| EngineError::BackendSetup(e.to_string()))?;
        Ok(Self {
            runtime,
            core_idle_poll: config.core_idle_poll,
        })
    }

    /// Adopt a caller-tuned Tokio runtime, moving it into engine ownership.
    /// Core drivers park for the default idle interval.
    pub fn from_runtime(runtime: tokio::runtime::Runtime) -> Self {
        Self {
            runtime,
            core_idle_poll: TokioConfig::default().core_idle_poll,
        }
    }
}

/// This impl is the Tokio substrate implementor: it is the one place permitted
/// to schedule directly on the owned runtime (ENGINE_SPEC.md §2).
impl ExecutionBackend for TokioBackend {
    fn spawn(&self, task: BoxTask) {
        // The handle is used ephemerally and never stored or returned.
        self.runtime.handle().spawn(task);
    }

    fn spawn_blocking(&self, work: BoxWork) {
        // Routed onto the runtime's dedicated blocking pool — separate from
        // the async worker threads — so blocking work cannot starve actor
        // ticks (ENGINE_SPEC.md §8 progress independence).
        self.runtime.handle().spawn_blocking(work);
    }

    fn timer(&self, delay: Duration) -> BoxTimer {
        // Construct the `tokio::time::sleep` lazily on first poll rather than
        // here: `EngineHandle::timer` may be called outside the runtime
        // (ENGINE_SPEC.md §7), but `tokio::time::sleep` needs the time driver
        // at construction. First poll runs inside an engine task where the
        // driver is available. See `LazySleep`.
        Box::pin(LazySleep::new(delay))
    }

    fn now(&self) -> EngineInstant {
        EngineInstant {
            instant: Instant::now(),
        }
    }

    fn capabilities(&self) -> Capabilities {
        // The substrate physically provides tasks, timers, blocking, and I/O.
        // `enable_all()` starts both the I/O reactor and the time driver, so
        // advertising `io: true` is truthful — integrations such as Iroh rely
        // on the native Tokio I/O environment (ENGINE_SPEC.md §6/§9).
        Capabilities {
            tasks: true,
            timers: true,
            blocking: true,
            io: true,
        }
    }

    fn core_idle_poll(&self) -> Duration {
        self.core_idle_poll
    }
}

/// A `tokio::time::sleep` whose construction is deferred to first poll.
///
/// `EngineHandle::timer` may be called outside the substrate runtime
/// (ENGINE_SPEC.md §7: creating a timer must not require entering or possessing
/// the runtime). `tokio::time::sleep` itself needs the time driver at
/// construction and panics ("there is no reactor running") when built outside a
/// Tokio context. This wrapper holds only the delay until first poll, which
/// runs inside an engine task where the driver is available, then builds and
/// delegates to the real `Sleep`.
struct LazySleep {
    delay: Option<Duration>,
    inner: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl LazySleep {
    fn new(delay: Duration) -> Self {
        Self {
            delay: Some(delay),
            inner: None,
        }
    }
}

// `LazySleep` arms a `tokio::time::sleep` inside an engine task where the time
// driver is available; this is the substrate's own time primitive.
impl Future for LazySleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // `LazySleep` is `Unpin`: both fields (`Option<Duration>` and
        // `Option<Pin<Box<_>>>`) are `Unpin`, so `get_mut` is sound.
        let this = self.get_mut();
        if let Some(delay) = this.delay.take() {
            this.inner = Some(Box::pin(tokio::time::sleep(delay)));
        }
        this.inner
            .as_mut()
            .expect("LazySleep polled after completion")
            .as_mut()
            .poll(cx)
    }
}
