//! MVP observability public surface.

pub mod benchmark;
#[cfg(feature = "dashboard")]
pub mod dashboard_view;
pub mod frame_archive;
pub mod lifecycle;
pub mod provisioning_logs;
pub mod telemetry;
