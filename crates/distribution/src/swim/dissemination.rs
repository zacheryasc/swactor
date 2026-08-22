//! SWIM piggybacked dissemination queue.
//!
//! Membership updates are piggybacked on existing protocol messages (pings, acks,
//! ping-reqs). Each update is transmitted `Λ * ceil(log2(n))` times before eviction,
//! where Λ is the dissemination multiplier and n is the cluster size.
//!
//! Priority ordering: Dead > Suspect > Alive (most urgent first).

use std::cmp::Reverse;

use crate::messages::MembershipUpdate;
use crate::types::{MemberState, NodeId};

/// A queued membership update with a remaining transmit budget.
#[derive(Debug, Clone)]
struct DisseminationEntry {
    update: MembershipUpdate,
    /// Remaining number of times to piggyback this update.
    remaining: usize,
}

/// The dissemination queue.
pub struct DisseminationQueue {
    entries: Vec<DisseminationEntry>,
    /// Λ multiplier — how many times log(n) to transmit each update.
    lambda: usize,
}

impl DisseminationQueue {
    pub fn new(lambda: usize) -> Self {
        Self {
            entries: Vec::new(),
            lambda,
        }
    }

    /// Enqueue a membership update for dissemination.
    ///
    /// If an update for the same node already exists, it's replaced if the new
    /// update has higher priority (higher incarnation, or same incarnation with
    /// higher-priority state).
    pub fn enqueue(&mut self, update: MembershipUpdate, cluster_size: usize) {
        let budget = self.transmit_budget(cluster_size);

        // Check for existing entry for this node
        if let Some(existing) = self
            .entries
            .iter_mut()
            .find(|e| e.update.node_id == update.node_id)
        {
            let dominated = update.incarnation > existing.update.incarnation
                || (update.incarnation == existing.update.incarnation
                    && update.state > existing.update.state);
            if dominated {
                existing.update = update;
                existing.remaining = budget;
            }
            return;
        }

        self.entries.push(DisseminationEntry {
            update,
            remaining: budget,
        });
    }

    /// Take up to `max_count` updates to piggyback on an outgoing message.
    ///
    /// Returns the updates sorted by priority (Dead first), and decrements
    /// their remaining transmit count. Entries with zero remaining are evicted.
    pub fn take(&mut self, max_count: usize) -> Vec<MembershipUpdate> {
        // Sort by priority: Dead (2) > Suspect (1) > Alive (0), descending
        self.entries
            .sort_by_key(|entry| Reverse(entry.update.state.priority()));

        let count = max_count.min(self.entries.len());
        let mut result = Vec::with_capacity(count);

        for entry in self.entries.iter_mut().take(count) {
            result.push(entry.update.clone());
            entry.remaining = entry.remaining.saturating_sub(1);
        }

        // Evict exhausted entries
        self.entries.retain(|e| e.remaining > 0);

        result
    }

    /// Serialize piggyback data for inclusion in a wire envelope.
    pub fn pack_piggyback(&mut self, max_updates: usize) -> Vec<u8> {
        let updates = self.take(max_updates);
        if updates.is_empty() {
            return Vec::new();
        }
        serde_json::to_vec(&updates).unwrap_or_default()
    }

    /// Deserialize piggybacked membership updates from a wire envelope.
    pub fn unpack_piggyback(bytes: &[u8]) -> Vec<MembershipUpdate> {
        if bytes.is_empty() {
            return Vec::new();
        }
        serde_json::from_slice(bytes).unwrap_or_default()
    }

    /// Number of queued entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove all pending updates for a given node.
    ///
    /// Called when clearing a Dead member before re-peering, so stale
    /// `(node_id, Dead, incarnation)` gossip doesn't leak out and re-infect
    /// the cluster.
    pub fn purge_node(&mut self, node_id: &NodeId) {
        self.entries.retain(|e| e.update.node_id != *node_id);
    }

    /// Compute the transmit budget: `Λ * ceil(log2(max(n, 2)))`.
    fn transmit_budget(&self, cluster_size: usize) -> usize {
        let n = cluster_size.max(2) as f64;
        let log_n = n.log2().ceil() as usize;
        self.lambda * log_n.max(1)
    }
}

/// Convenience: create a `MembershipUpdate` from components.
pub fn membership_update(
    node_id: NodeId,
    state: MemberState,
    incarnation: u64,
) -> MembershipUpdate {
    MembershipUpdate {
        node_id,
        state,
        incarnation,
    }
}
