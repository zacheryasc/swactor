//! Distributed-node configuration.
//!
//! The `DistributedNode` orchestrator that once lived here has been retired:
//! SWIM membership, the cluster name registry, node-metadata dissemination, and
//! the actor→host directory now each run as an independent actor on the swactor
//! runtime — see [`crate::swim::actor`], [`crate::registry_actor`],
//! [`crate::node_metadata_actor`], and [`crate::directory_actor`]. Concrete
//! network drivers (for example `iroh-driver`) feed those actors. All that
//! remains here is the shared configuration bundle the node binary uses to
//! construct them.

use crate::registry::RegistryConfig;
use crate::swim::probe::SwimConfig;

/// Configuration for a distributed node — the bundle of per-protocol configs the
/// node binary unpacks to construct the SWIM / registry / metadata actors.
#[derive(Clone)]
pub struct DistributedNodeConfig {
    pub swim: SwimConfig,
    pub cache_capacity: usize,
    pub registry: RegistryConfig,
    /// Dissemination multiplier for node metadata (default: 3).
    pub metadata_lambda: usize,
}

impl Default for DistributedNodeConfig {
    fn default() -> Self {
        Self {
            swim: SwimConfig::default(),
            cache_capacity: 10_000,
            registry: RegistryConfig::default(),
            metadata_lambda: 3,
        }
    }
}
