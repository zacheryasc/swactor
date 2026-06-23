//! LRU cache for resolved actor locations: `ActorAddress → NodeId`.
//!
//! Bounded capacity, no TTL (caller invalidates on delivery failure).

use std::collections::HashMap;

use swactor::actor::ActorAddress;

use crate::types::NodeId;

/// A cached actor location.
#[derive(Debug, Clone)]
struct CacheEntry {
    node_id: NodeId,
    /// Position in the LRU ordering (higher = more recent).
    order: u64,
}

/// LRU cache mapping actor addresses to the node that hosts them.
pub struct LocationCache {
    entries: HashMap<ActorAddress, CacheEntry>,
    capacity: usize,
    counter: u64,
}

impl LocationCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            capacity: capacity.max(1),
            counter: 0,
        }
    }

    /// Look up a cached location. Marks the entry as most-recently-used.
    pub fn get(&mut self, addr: &ActorAddress) -> Option<NodeId> {
        if let Some(entry) = self.entries.get_mut(addr) {
            self.counter += 1;
            entry.order = self.counter;
            Some(entry.node_id)
        } else {
            None
        }
    }

    /// Look up without updating LRU order.
    pub fn peek(&self, addr: &ActorAddress) -> Option<NodeId> {
        self.entries.get(addr).map(|e| e.node_id)
    }

    /// Insert or update a cached location.
    pub fn insert(&mut self, addr: ActorAddress, node_id: NodeId) {
        self.counter += 1;
        if self.entries.len() >= self.capacity && !self.entries.contains_key(&addr) {
            self.evict_lru();
        }
        self.entries.insert(
            addr,
            CacheEntry {
                node_id,
                order: self.counter,
            },
        );
    }

    /// Evict a stale entry (e.g. on delivery failure).
    pub fn invalidate(&mut self, addr: &ActorAddress) -> bool {
        self.entries.remove(addr).is_some()
    }

    /// Evict all entries for a specific node (e.g. when the node is declared dead).
    pub fn invalidate_node(&mut self, node_id: &NodeId) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, e| e.node_id != *node_id);
        before - self.entries.len()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Snapshot of all cache entries as `(ActorAddress, NodeId)` pairs.
    pub fn entries(&self) -> Vec<(ActorAddress, NodeId)> {
        self.entries
            .iter()
            .map(|(addr, entry)| (*addr, entry.node_id))
            .collect()
    }

    fn evict_lru(&mut self) {
        if let Some((&addr, _)) = self.entries.iter().min_by_key(|(_, e)| e.order) {
            self.entries.remove(&addr);
        }
    }
}
