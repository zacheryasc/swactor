//! Directory republish and churn repair.
//!
//! - On node death: identify affected entries, mark for re-replication.
//! - Periodic republish: spawning nodes re-STORE their entries.
//! - TTL expiration: entries whose host is confirmed dead expire after grace period.

use std::collections::HashMap;

use swactor::actor::ActorAddress;

use crate::types::{DirectoryEntry, NodeId};
use super::directory::DirectoryShard;

/// Tracks entries that need re-replication after node failures.
pub struct RepairQueue {
    /// Entries needing re-replication, keyed by actor address.
    pending: HashMap<ActorAddress, DirectoryEntry>,
}

impl Default for RepairQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl RepairQueue {
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    /// Handle a node death: extract all entries from the shard that were
    /// authored by the dead node and queue them for re-replication.
    pub fn on_node_death(&mut self, dead_node: &NodeId, shard: &mut DirectoryShard) -> usize {
        let removed = shard.remove_by_node(dead_node);
        let count = removed.len();
        for entry in removed {
            self.pending.insert(entry.actor_addr, entry);
        }
        count
    }

    /// Take all pending entries for re-replication.
    pub fn drain(&mut self) -> Vec<DirectoryEntry> {
        self.pending.drain().map(|(_, e)| e).collect()
    }

    /// Number of entries pending re-replication.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

/// Tracks locally-spawned actors for periodic republishing.
pub struct RepublishTracker {
    /// Actor addresses spawned on this node, with their current generation.
    local_actors: HashMap<ActorAddress, u64>,
    /// Ticks between republish cycles.
    interval: u64,
    /// Next republish tick.
    next_republish: u64,
}

impl RepublishTracker {
    pub fn new(interval: u64) -> Self {
        Self {
            local_actors: HashMap::new(),
            interval,
            next_republish: interval,
        }
    }

    /// Register a locally-spawned actor.
    pub fn register(&mut self, addr: ActorAddress, generation: u64) {
        self.local_actors.insert(addr, generation);
    }

    /// Unregister an actor (e.g. when it's stopped).
    pub fn unregister(&mut self, addr: &ActorAddress) {
        self.local_actors.remove(addr);
    }

    /// Check if it's time to republish. Returns the list of actors to re-STORE.
    pub fn tick(&mut self, current_tick: u64) -> Vec<(ActorAddress, u64)> {
        if current_tick < self.next_republish {
            return Vec::new();
        }
        self.next_republish = current_tick + self.interval;
        self.local_actors
            .iter()
            .map(|(addr, g)| (*addr, *g))
            .collect()
    }

    pub fn count(&self) -> usize {
        self.local_actors.len()
    }
}
