//! Per-runtime hook for observing output from processes spawned through the
//! process facility.
//!
//! This trait lives in swactor rather than `swactor-process` so the runtime can
//! store an observer without a dependency cycle: `swactor-process` depends on
//! swactor, and a telemetry emitter (which lives even higher up) implements this
//! trait. The runtime is merely the carrier — it never interprets the bytes.
//!
//! The observer is per-node (per-runtime), installed via
//! [`Runtime::set_process_output_observer`](crate::runtime::Runtime::set_process_output_observer).
//! The process facility hands every managed process's output to it automatically,
//! labeled by the process's command basename, so a node taps all of its managed
//! processes with no per-spawn wiring.

/// Observes every chunk of stdout/stderr from processes the runtime spawns.
pub trait ProcessOutputObserver: Send + Sync {
    /// `label` identifies the process (its command basename); `is_stderr`
    /// selects the stream; `data` is one raw output chunk.
    fn on_output(&self, label: &str, is_stderr: bool, data: &[u8]);
}
