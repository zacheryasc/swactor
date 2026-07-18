//! Legacy/custom hook for adapters that choose to observe stdout/stderr bytes.
//!
//! Current `swactor-process` managed-process core does not call this hook.
//! Managed-process lifecycle/control output is delivered as `ProcessOutput` to
//! the configured upstream owner and may be mirrored to datastream through
//! `ProcessOutputConfig::datastream_mirror`.
//!
//! The runtime is only the storage location for callers that still wire this
//! hook themselves; it does not interpret the bytes.

/// Observes chunks supplied by legacy/custom process-output adapters.
pub trait ProcessOutputObserver: Send + Sync {
    /// `label` identifies the adapter-defined source; `is_stderr` selects the
    /// stream; `data` is one raw output chunk.
    fn on_output(&self, label: &str, is_stderr: bool, data: &[u8]);
}
