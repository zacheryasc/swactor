//! Lifeguard protocol extensions for SWIM.
//!
//! Based on the Hashicorp Lifeguard paper. Three key mechanisms:
//!
//! 1. **Local Health Multiplier (LHM)**: degraded nodes (high nack rate, slow acks)
//!    increase their own probe interval and timeouts, reducing false accusations.
//!
//! 2. **Dynamic suspect timeout**: scales with `ceil(log2(n+1))` where `n` is the
//!    cluster size, giving larger clusters proportionally more time.
//!
//! 3. **Protocol period scaling**: under load (high LHM), probe intervals stretch
//!    rather than dropping probes.
//!
//! All three mechanisms are combined through a single `LifeguardConfig` that can
//! be applied to `SwimConfig` dynamically.

use std::time::Duration;

/// Lifeguard configuration parameters.
#[derive(Debug, Clone)]
pub struct LifeguardConfig {
    /// Maximum LHM value (caps the multiplier).
    pub max_health_score: u32,
    /// How much each nack/timeout adds to the health score.
    pub nack_penalty: u32,
    /// How much each successful ack decreases the health score.
    pub ack_reward: u32,
    /// Base suspicion timeout (before log(n) scaling).
    pub base_suspicion_timeout: Duration,
    /// Minimum suspect timeout regardless of cluster size.
    pub min_suspicion_timeout: Duration,
    /// Maximum suspect timeout regardless of cluster size.
    pub max_suspicion_timeout: Duration,
}

impl Default for LifeguardConfig {
    fn default() -> Self {
        // Wall-clock equivalents of the prior tick defaults (30 / 15 / 120
        // ticks) at the production 20 ms tick period.
        Self {
            max_health_score: 8,
            nack_penalty: 1,
            ack_reward: 1,
            base_suspicion_timeout: Duration::from_millis(600),
            min_suspicion_timeout: Duration::from_millis(300),
            max_suspicion_timeout: Duration::from_millis(2400),
        }
    }
}

/// Local Health Multiplier — tracks the node's own health and produces
/// a multiplier that stretches timeouts and probe intervals.
pub struct HealthMultiplier {
    config: LifeguardConfig,
    /// Current health score (0 = perfectly healthy, higher = more degraded).
    score: u32,
}

impl HealthMultiplier {
    pub fn new(config: LifeguardConfig) -> Self {
        Self { config, score: 0 }
    }

    /// Record a successful ack — decrease health score.
    pub fn record_ack(&mut self) {
        self.score = self.score.saturating_sub(self.config.ack_reward);
    }

    /// Record a nack/timeout — increase health score.
    pub fn record_nack(&mut self) {
        self.score = (self.score + self.config.nack_penalty).min(self.config.max_health_score);
    }

    /// Current health score (0 = healthy).
    pub fn score(&self) -> u32 {
        self.score
    }

    /// The multiplier for timeouts and intervals: `1 + score`.
    /// A healthy node returns 1 (no scaling). A degraded node returns higher.
    pub fn multiplier(&self) -> u64 {
        1 + self.score as u64
    }

    /// Apply the health multiplier to a base probe interval.
    pub fn scaled_probe_interval(&self, base: u64) -> u64 {
        base * self.multiplier()
    }

    /// Apply the health multiplier to a base probe timeout.
    pub fn scaled_probe_timeout(&self, base: u64) -> u64 {
        base * self.multiplier()
    }

    /// Compute the dynamic suspect timeout based on cluster size and health.
    ///
    /// Formula: `clamp(base * ceil(log2(n+1)) * multiplier, min, max)`
    pub fn dynamic_suspicion_timeout(&self, cluster_size: usize) -> Duration {
        let log_n = log2_ceil(cluster_size.saturating_add(1) as u64).max(1);
        let scale = (log_n * self.multiplier()).min(u32::MAX as u64) as u32;
        let timeout = self.config.base_suspicion_timeout * scale;
        timeout.clamp(
            self.config.min_suspicion_timeout,
            self.config.max_suspicion_timeout,
        )
    }
}

/// Compute `ceil(log2(n))`, returning 0 for n <= 1.
fn log2_ceil(n: u64) -> u64 {
    if n <= 1 {
        return 0;
    }
    // Number of bits needed = position of highest set bit
    
    64 - (n - 1).leading_zeros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log2_ceil_values() {
        assert_eq!(log2_ceil(0), 0);
        assert_eq!(log2_ceil(1), 0);
        assert_eq!(log2_ceil(2), 1);
        assert_eq!(log2_ceil(3), 2);
        assert_eq!(log2_ceil(4), 2);
        assert_eq!(log2_ceil(5), 3);
        assert_eq!(log2_ceil(8), 3);
        assert_eq!(log2_ceil(9), 4);
        assert_eq!(log2_ceil(16), 4);
        assert_eq!(log2_ceil(100), 7);
        assert_eq!(log2_ceil(1000), 10);
    }
}
