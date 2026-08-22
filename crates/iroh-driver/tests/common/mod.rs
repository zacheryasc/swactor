//! Shared test config for the iroh-driver integration tests.

use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use std::time::Duration;

/// Wall-clock granularity of one gossip round. SWIM is wall-clock driven, so
/// the actor stack advances its clock by `TICK` per round; the old integer
/// tick-count config carries over as multiples of `TICK`.
const TICK: Duration = Duration::from_millis(10);

/// Default test config shared across all integration tests.
pub fn test_config() -> DistributedNodeConfig {
    DistributedNodeConfig {
        swim: SwimConfig {
            probe_interval: TICK,
            probe_timeout: TICK * 3,
            indirect_probes: 1,
            suspicion_timeout: TICK * 5,
            dead_reprobe_interval: Duration::ZERO,
            ..SwimConfig::default()
        },
        cache_capacity: 100,
        registry: RegistryConfig::default(),
        metadata_lambda: 3,
    }
}

pub mod iroh;
