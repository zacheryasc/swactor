//! Node metadata dissemination — gossip-propagated per-node metadata.
//!
//! Each node may have metadata (currently: relay URL) that should be visible
//! cluster-wide. Uses higher-generation-wins semantics and SWIM-style
//! dissemination budgets (Λ * log₂(n)).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// A single metadata entry for one node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeMetadataEntry {
    pub node_id: NodeId,
    pub relay_url: Option<String>,
    pub generation: u64,
}

/// Dissemination state for one entry.
#[derive(Debug, Clone)]
struct PendingEntry {
    entry: NodeMetadataEntry,
    remaining: usize,
}

/// Per-node metadata store with gossip dissemination.
pub struct NodeMetadataDisseminator {
    store: HashMap<NodeId, NodeMetadataEntry>,
    pending: Vec<PendingEntry>,
    lambda: usize,
    local_generation: u64,
}

impl NodeMetadataDisseminator {
    pub fn new(lambda: usize) -> Self {
        Self {
            store: HashMap::new(),
            pending: Vec::new(),
            lambda,
            local_generation: 0,
        }
    }

    /// Set this node's relay URL and enqueue for dissemination.
    pub fn set_local(&mut self, node_id: NodeId, relay_url: Option<String>, cluster_size: usize) {
        self.local_generation += 1;
        let entry = NodeMetadataEntry {
            node_id,
            relay_url,
            generation: self.local_generation,
        };
        self.store.insert(node_id, entry.clone());
        self.enqueue(entry, cluster_size);
    }

    /// Merge incoming entries from gossip. Re-enqueues changed entries for forwarding.
    pub fn apply_incoming(&mut self, entries: Vec<NodeMetadataEntry>, cluster_size: usize) {
        for entry in entries {
            let dominated = match self.store.get(&entry.node_id) {
                Some(existing) => entry.generation <= existing.generation,
                None => false,
            };
            if dominated {
                continue;
            }
            self.store.insert(entry.node_id, entry.clone());
            self.enqueue(entry, cluster_size);
        }
    }

    /// Take pending entries for piggyback, up to `max_count`.
    pub fn take_pending(&mut self, max_count: usize) -> Vec<NodeMetadataEntry> {
        let count = max_count.min(self.pending.len());
        let mut result = Vec::with_capacity(count);

        for entry in self.pending.iter_mut().take(count) {
            result.push(entry.entry.clone());
            entry.remaining = entry.remaining.saturating_sub(1);
        }

        self.pending.retain(|e| e.remaining > 0);
        result
    }

    /// Look up a node's relay URL.
    pub fn relay_url(&self, node_id: &NodeId) -> Option<&str> {
        self.store
            .get(node_id)
            .and_then(|e| e.relay_url.as_deref())
    }

    /// Remove metadata for a dead node.
    pub fn remove_node(&mut self, node_id: &NodeId) {
        self.store.remove(node_id);
        self.pending.retain(|e| &e.entry.node_id != node_id);
    }

    /// Re-enqueue all entries for dissemination (anti-entropy on membership recovery).
    pub fn re_disseminate_all(&mut self, cluster_size: usize) {
        for entry in self.store.values().cloned().collect::<Vec<_>>() {
            self.enqueue(entry, cluster_size);
        }
    }

    fn transmit_budget(&self, cluster_size: usize) -> usize {
        let n = cluster_size.max(2) as f64;
        let log_n = n.log2().ceil() as usize;
        self.lambda * log_n.max(1)
    }

    fn enqueue(&mut self, entry: NodeMetadataEntry, cluster_size: usize) {
        let budget = self.transmit_budget(cluster_size);

        // Replace existing pending entry for same node if present.
        if let Some(existing) = self.pending.iter_mut().find(|e| e.entry.node_id == entry.node_id)
        {
            existing.entry = entry;
            existing.remaining = budget;
            return;
        }

        self.pending.push(PendingEntry {
            entry,
            remaining: budget,
        });
    }
}
