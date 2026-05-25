//! Local name-registry scrape for tier-2 snapshots.
//!
//! Mirrors the [`crate::diagnostics::swim_introspect`] pattern: the
//! registry lives inside the driver-owned [`crate::node::DistributedNode`]
//! and is not `Sync`, so the introspector is a recorder rather than a
//! poller. The node calls [`RegistryIntrospect::capture_now`] after each
//! mutation (register / unregister / gossip-merge), the introspector
//! stores the latest [`Tier2Registry`] view behind its own `Mutex`, and
//! [`crate::diagnostics::RegistryIntrospector::capture`] reads that
//! aggregate without touching the registry.

use std::sync::Mutex;

use crate::diagnostics::snapshot::{RegistryIntrospector, Tier2Registry};
use crate::registry::ClusterRegistry;

/// Records the most recent registry view for inclusion in tier-2
/// snapshots. Designed to be shared via `Arc` between
/// [`crate::node::DistributedNode`] (which drives the writes) and the
/// diagnostics aggregator (which reads at snapshot time).
#[derive(Debug, Default)]
pub struct RegistryIntrospect {
    inner: Mutex<Tier2Registry>,
}

impl RegistryIntrospect {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the cached view with a fresh capture of `registry`.
    /// Called from `DistributedNode` after every registry mutation, so
    /// the next snapshot reflects the post-mutation state.
    pub fn capture_now(&self, registry: &ClusterRegistry) {
        let view = registry.capture();
        let mut guard = self
            .inner
            .lock()
            .expect("registry introspect mutex poisoned");
        *guard = view;
    }
}

impl RegistryIntrospector for RegistryIntrospect {
    fn capture(&self) -> Tier2Registry {
        self.inner
            .lock()
            .expect("registry introspect mutex poisoned")
            .clone()
    }
}
