//! MVP observability public surface.

pub(crate) mod benchmark;
#[cfg(feature = "dashboard")]
pub(crate) mod dashboard_view;
pub(crate) mod frame_archive;
pub(crate) mod lifecycle;
pub(crate) mod provisioning_logs;
pub(crate) mod telemetry;
