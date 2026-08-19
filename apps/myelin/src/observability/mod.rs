//! Myelin observability public surface.

pub(crate) mod benchmark;
pub(crate) mod frame_archive;
pub(crate) mod frame_collector;
// Lifecycle event schema is currently consumed only by the test harness
// (mock nodes + guarantee tests); the production binary never constructs it.
#[cfg(test)]
pub(crate) mod lifecycle;
pub(crate) mod orch_telemetry;
pub(crate) mod provisioning_logs;
pub(crate) mod telemetry;
