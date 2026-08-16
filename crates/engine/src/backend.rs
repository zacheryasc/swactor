//! Execution backend SPI.
//!
//! The [`ExecutionBackend`] trait is an implementation seam the engine uses to
//! schedule work and read engine time. It is not the interface applications
//! consume; that is [`crate::EngineHandle`]. A native implementation erases
//! tasks once when they are installed; this representation is not a
//! cross-target requirement (see `ENGINE_SPEC.md`).

use std::pin::Pin;
use std::time::Duration;

use crate::time::EngineInstant;

/// A boxed, sendable future returned by the engine substrate.
pub type BoxTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
/// A boxed, sendable timer future produced by the substrate.
pub type BoxTimer = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
/// A boxed, sendable one-shot blocking workload.
pub type BoxWork = Box<dyn FnOnce() + Send + 'static>;

/// The substrate-specific execution surface an engine schedules onto.
///
/// Object-safe so the composite [`Engine`](crate::Engine) can store it as
/// `Arc<dyn ExecutionBackend>` without being generic over the backend.
pub trait ExecutionBackend: Send + Sync + 'static {
    /// Schedule `task` to run as cooperative engine work.
    fn spawn(&self, task: BoxTask);
    /// Schedule `work` on a dedicated blocking thread.
    fn spawn_blocking(&self, work: BoxWork);
    /// Produce a future that completes after `delay`.
    fn timer(&self, delay: Duration) -> BoxTimer;
    /// Read the engine's monotonic clock.
    fn now(&self) -> EngineInstant;
    /// Report the substrate's advertised capabilities.
    fn capabilities(&self) -> Capabilities;
    /// How long the core driver parks between ticks when its worker is idle.
    ///
    /// `Duration::ZERO` (the default) re-arms the driver immediately after
    /// every tick — a poll loop at scheduler speed. Backends with real timers
    /// return a small interval so an idle core parks instead of spinning;
    /// newly delivered work is observed within one interval. Every poll still
    /// runs one tick, so this only bounds idle wakeup latency.
    fn core_idle_poll(&self) -> Duration {
        Duration::ZERO
    }
}

/// Capabilities an execution backend advertises.
///
/// - `tasks`: cooperative task scheduling.
/// - `timers`: timer and interval support.
/// - `blocking`: dedicated blocking-thread pools.
/// - `io`: asynchronous I/O reactor (e.g. Tokio's I/O driver from `enable_all`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    pub tasks: bool,
    pub timers: bool,
    pub blocking: bool,
    pub io: bool,
}

impl Capabilities {
    /// Convenience: only baseline task execution.
    pub const TASKS_ONLY: Self = Self {
        tasks: true,
        timers: false,
        blocking: false,
        io: false,
    };

    /// Convenience: every capability.
    pub const ALL: Self = Self {
        tasks: true,
        timers: true,
        blocking: true,
        io: true,
    };

    /// Convenience: no capabilities. Reported by an [`EngineHandle`](crate::EngineHandle)
    /// whose owning engine has been dropped (ENGINE_SPEC.md).
    pub const NONE: Self = Self {
        tasks: false,
        timers: false,
        blocking: false,
        io: false,
    };

    /// Whether `self` satisfies every capability marked `true` in `required`.
    ///
    /// A `required` field set to `false` is treated as "not required" — the
    /// backend may or may not provide it. Used by
    /// [`EngineHandle::require`](crate::EngineHandle::require) to validate
    /// integration requirements (ENGINE_SPEC.md).
    pub fn satisfies(&self, required: Capabilities) -> bool {
        (!required.tasks || self.tasks)
            && (!required.timers || self.timers)
            && (!required.blocking || self.blocking)
            && (!required.io || self.io)
    }
}

/// Errors that can arise while constructing an engine.
#[derive(Debug, Clone)]
pub enum EngineError {
    /// A capability required by the engine is not advertised by the backend.
    MissingRequiredCapability,
    /// A backend-owned substrate could not be constructed (e.g. a Tokio
    /// runtime failed to build).
    BackendSetup(String),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::MissingRequiredCapability => {
                write!(f, "backend is missing a capability required by the engine")
            }
            EngineError::BackendSetup(msg) => {
                write!(f, "backend substrate setup failed: {msg}")
            }
        }
    }
}

impl std::error::Error for EngineError {}
