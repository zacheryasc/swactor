//! Telemetry-owned self-health record.

use serde::{Deserialize, Serialize};

use crate::record::Record;

/// Telemetry self-health — the mux's own integrity counters.
pub const TELEMETRY_HEALTH: &str = "telemetry.health";

/// `assigned` is the gap-free high-water mark (every position handed out);
/// `dropped` is the frames lost to mux overflow.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryHealth {
    #[serde(default)]
    pub assigned: u64,
    #[serde(default)]
    pub dropped: u64,
    /// `dropped / assigned × 1_000_000`; `0` when nothing has been assigned yet.
    #[serde(default)]
    pub loss_rate_ppm: u32,
}

impl Record for TelemetryHealth {
    const CHANNEL: &'static str = TELEMETRY_HEALTH;
}
