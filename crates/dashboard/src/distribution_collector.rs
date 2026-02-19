//! Distribution stats provider for the runtime dashboard.
//!
//! The application implements `DistributionStatsProvider` to let the dashboard
//! read a single node's distribution state (SWIM membership, Kademlia routing,
//! LRU cache, etc.) without reaching out to other nodes.

use std::sync::{Arc, Mutex};

use distribution::snapshot::DistributionNodeSnapshot;

/// Trait for providing distribution stats to the dashboard.
///
/// Implementations capture a point-in-time snapshot of the local
/// `DistributedNode`'s state. The dashboard polls this every ~200ms.
pub trait DistributionStatsProvider: Send + Sync {
    fn snapshot(&self) -> Option<DistributionNodeSnapshot>;
}

/// Simple implementation wrapping an `Arc<Mutex<T>>` where T implements
/// a `snapshot()` method (e.g. `DistributedNode`).
pub struct DistributionCollector<T> {
    inner: Arc<Mutex<T>>,
}

impl<T> DistributionCollector<T> {
    pub fn new(inner: Arc<Mutex<T>>) -> Self {
        Self { inner }
    }
}

impl DistributionStatsProvider for DistributionCollector<distribution::node::DistributedNode> {
    fn snapshot(&self) -> Option<DistributionNodeSnapshot> {
        self.inner.lock().ok().map(|node| node.snapshot())
    }
}
