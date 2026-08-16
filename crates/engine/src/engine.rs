//! Composite engine and cloneable scheduler handle.

use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::backend::{Capabilities, EngineError, ExecutionBackend};
use crate::time::{EngineInstant, Interval, Timeout, Timer};
use swactor::runtime::{Runtime, RuntimeParts};

/// The composite engine: retains a configured core runtime handle and its
/// execution backend, and owns one core-driving loop per worker.
///
/// Construct with [`Engine::new`]; obtain a scheduler handle with
/// [`Engine::handle`].
pub struct Engine {
    /// Retained so the engine owns the runtime handle it drives for its full
    /// lifetime. Core workers are moved into substrate tasks at construction.
    #[allow(dead_code)]
    runtime: Runtime,
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
        Ok(Engine { runtime, backend })
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

    /// Schedule `work` on a dedicated blocking thread.
    ///
    /// A no-op once the owning engine has been dropped.
    pub fn spawn_blocking<F>(&self, work: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Some(backend) = self.backend() {
            backend.spawn_blocking(Box::new(work));
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
