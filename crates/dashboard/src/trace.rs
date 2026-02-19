use serde::{Deserialize, Serialize};
use swactor::stats::RuntimeStats;

use crate::layer::DashboardEvent;

/// Complete trace of a runtime execution, suitable for saving/loading.
#[derive(Debug, Serialize, Deserialize)]
pub struct RuntimeTrace {
    pub events: Vec<DashboardEvent>,
    pub stats_timeline: Vec<TimestampedStats>,
}

/// A stats snapshot with a wall-clock timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampedStats {
    pub timestamp_ms: u64,
    pub stats: RuntimeStats,
}
