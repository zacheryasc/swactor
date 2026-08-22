use std::time::Duration;

use crate::types::{LifecyclePolicy, SelectionPolicy};

pub const ENV_IMAGE_SIZE_GB: &str = "PP_IMAGE_SIZE_GB";
pub const ENV_GPU_MIN_RAM_MB: &str = "PP_GPU_MIN_RAM_MB";
pub const ENV_MIN_COMPUTE_CAP: &str = "PP_MIN_COMPUTE_CAP";
pub const ENV_MIN_INET_DOWN_MBPS: &str = "PP_MIN_INET_DOWN_MBPS";
pub const ENV_MIN_INET_UP_MBPS: &str = "PP_MIN_INET_UP_MBPS";
pub const ENV_MIN_RELIABILITY: &str = "PP_MIN_RELIABILITY";
pub const ENV_REQUIRE_VERIFIED: &str = "PP_REQUIRE_VERIFIED";
pub const ENV_DROP_CHEAP_FRAC: &str = "PP_DROP_CHEAP_FRAC";
pub const ENV_MAX_DPH_TOTAL: &str = "PP_MAX_DPH_TOTAL";
pub const ENV_LEASE_PACE_MS: &str = "PP_LEASE_PACE_MS";
pub const ENV_STATE_TIMEOUT_SECS: &str = "PP_STATE_TIMEOUT_SECS";
pub const ENV_BLACKLIST_HOSTS: &str = "PP_BLACKLIST_HOSTS";
pub const ENV_ASSUME_YES: &str = "PP_ASSUME_YES";

pub fn truthy_env(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|s| matches!(s.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

fn env_positive_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
}

fn env_nonnegative_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|&v| v >= 0.0)
        .unwrap_or(default)
}

fn env_optional_positive_f64(name: &str) -> Option<f64> {
    std::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|&v| v > 0.0)
}

impl SelectionPolicy {
    /// Build selection policy from the historical `PP_*` environment knobs.
    pub fn from_env() -> Self {
        let defaults = Self::default();
        let mut blacklist_hosts = defaults.blacklist_hosts;
        if let Ok(raw) = std::env::var(ENV_BLACKLIST_HOSTS) {
            blacklist_hosts.extend(
                raw.split(',')
                    .filter_map(|value| value.trim().parse::<u64>().ok()),
            );
        }
        Self {
            gpu_name: defaults.gpu_name,
            min_gpu_ram_mb: env_positive_u64(ENV_GPU_MIN_RAM_MB),
            min_compute_cap: env_positive_u64(ENV_MIN_COMPUTE_CAP).or(defaults.min_compute_cap),
            min_down_mbps: env_nonnegative_f64(ENV_MIN_INET_DOWN_MBPS, 100.0),
            min_reliability: std::env::var(ENV_MIN_RELIABILITY)
                .ok()
                .and_then(|value| value.trim().parse::<f64>().ok())
                .filter(|&value| (0.0..=1.0).contains(&value))
                .unwrap_or(0.95),
            require_verified: truthy_env(ENV_REQUIRE_VERIFIED),
            min_up_mbps: env_optional_positive_f64(ENV_MIN_INET_UP_MBPS),
            max_dph_total: env_optional_positive_f64(ENV_MAX_DPH_TOTAL),
            blacklist_hosts,
            drop_cheap_frac: std::env::var(ENV_DROP_CHEAP_FRAC)
                .ok()
                .and_then(|value| value.trim().parse::<f64>().ok())
                .filter(|value| value.is_finite())
                .map(|value| value.clamp(0.0, 0.99))
                .unwrap_or(0.30),
            image_size_gb: env_optional_positive_f64(ENV_IMAGE_SIZE_GB),
        }
    }
}

impl LifecyclePolicy {
    /// Build lifecycle policy from environment, using caller-provided poll cadence.
    pub fn from_env(poll_interval: Duration) -> Self {
        Self {
            lease_pace: Duration::from_millis(
                std::env::var(ENV_LEASE_PACE_MS)
                    .ok()
                    .and_then(|value| value.trim().parse::<u64>().ok())
                    .unwrap_or(600),
            ),
            poll_interval,
            state_timeout: env_positive_u64(ENV_STATE_TIMEOUT_SECS)
                .map(Duration::from_secs)
                .unwrap_or_else(|| Self::default().state_timeout),
        }
    }
}
