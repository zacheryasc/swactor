//! swactor-engine: an execution engine that drives a `swactor` core runtime on
//! a selected substrate without exposing that substrate through its handle.
//!
//! The default substrate is Tokio; a deterministic [`SteppingBackend`] provides
//! a non-Tokio portability proof. The public contract is defined by
//! [`ENGINE_SPEC.md`](../ENGINE_SPEC.md).
#![deny(clippy::disallowed_methods)]
mod backend;
mod core_driver;
mod engine;
mod stepping;
mod time;

#[cfg(feature = "tokio")]
mod tokio;

pub use backend::{BoxTask, BoxTimer, BoxWork, Capabilities, EngineError, ExecutionBackend};
pub use engine::{ActorCompletion, ActorTimer, BlockingWorkSender, Engine, EngineHandle};
pub use stepping::SteppingBackend;
pub use time::{Elapsed, EngineInstant, Interval, Timeout, Timer};

#[cfg(feature = "tokio")]
pub use tokio::{TokioBackend, TokioConfig};
