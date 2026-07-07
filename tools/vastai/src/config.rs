use std::time::Duration;

use crate::types::{LifecyclePolicy, SelectionPolicy};

pub const ENV_IMAGE_SIZE_GB: &str = "PP_IMAGE_SIZE_GB";
pub const ENV_GPU_MIN_RAM_MB: &str = "PP_GPU_MIN_RAM_MB";
pub const ENV_MIN_INET_DOWN_MBPS: &str = "PP_MIN_INET_DOWN_MBPS";
pub const ENV_MIN_INET_UP_MBPS: &str = "PP_MIN_INET_UP_MBPS";
pub const ENV_MIN_RELIABILITY: &str = "PP_MIN_RELIABILITY";
pub const ENV_REQUIRE_VERIFIED: &str = "PP_REQUIRE_VERIFIED";
pub const ENV_DROP_CHEAP_FRAC: &str = "PP_DROP_CHEAP_FRAC";
pub const ENV_LEASE_PACE_MS: &str = "PP_LEASE_PACE_MS";
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
        let mut policy = Self::default();
        policy.min_gpu_ram_mb = env_positive_u64(ENV_GPU_MIN_RAM_MB);
        policy.min_down_mbps = env_nonnegative_f64(ENV_MIN_INET_DOWN_MBPS, 100.0);
        policy.min_reliability = std::env::var(ENV_MIN_RELIABILITY)
            .ok()
            .and_then(|s| s.trim().parse::<f64>().ok())
            .filter(|&v| (0.0..=1.0).contains(&v))
            .unwrap_or(0.95);
        policy.require_verified = truthy_env(ENV_REQUIRE_VERIFIED);
        policy.min_up_mbps = env_optional_positive_f64(ENV_MIN_INET_UP_MBPS);
        policy.drop_cheap_frac = std::env::var(ENV_DROP_CHEAP_FRAC)
            .ok()
            .and_then(|s| s.trim().parse::<f64>().ok())
            .filter(|v| v.is_finite())
            .map(|v| v.clamp(0.0, 0.99))
            .unwrap_or(0.30);
        policy.image_size_gb = env_optional_positive_f64(ENV_IMAGE_SIZE_GB);
        if let Ok(raw) = std::env::var(ENV_BLACKLIST_HOSTS) {
            policy
                .blacklist_hosts
                .extend(raw.split(',').filter_map(|s| s.trim().parse::<u64>().ok()));
        }
        policy
    }
}

impl LifecyclePolicy {
    /// Build lifecycle policy from environment, using caller-provided poll cadence.
    pub fn from_env(poll_interval: Duration) -> Self {
        let mut policy = Self::default();
        policy.lease_pace = Duration::from_millis(
            std::env::var(ENV_LEASE_PACE_MS)
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
                .unwrap_or(600),
        );
        policy.poll_interval = poll_interval;
        policy
    }
}
