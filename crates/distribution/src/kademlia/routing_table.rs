//! Kademlia k-bucket routing table.
//!
//! 256 buckets indexed by `XOR(self_id, target).leading_zeros()`.
//! Each bucket holds up to `k` nodes in LRU order (most-recently-seen at tail).
//! Prefers long-lived nodes: when a bucket is full, new nodes go to a
//! replacement cache and only promote when an existing node is evicted.

use std::collections::VecDeque;

use crate::types::NodeId;

/// Default replication parameter.
pub const K: usize = 20;

/// Number of buckets (one per bit of the 256-bit key space).
const NUM_BUCKETS: usize = 256;

/// A node entry in the routing table.
#[derive(Debug, Clone)]
pub struct NodeEntry {
    pub node_id: NodeId,
}

/// A single k-bucket with an LRU list and replacement cache.
struct KBucket {
    /// LRU ordered: front = least-recently-seen, back = most-recently-seen.
    nodes: VecDeque<NodeEntry>,
    /// Replacement cache for when the bucket is full.
    replacements: VecDeque<NodeEntry>,
    k: usize,
}

impl KBucket {
    fn new(k: usize) -> Self {
        Self {
            nodes: VecDeque::with_capacity(k),
            replacements: VecDeque::with_capacity(k),
            k,
        }
    }

    /// Insert or update a node. Returns `true` if the node was added/moved.
    fn insert(&mut self, entry: NodeEntry) -> bool {
        // If already present, move to back (most-recently-seen)
        if let Some(pos) = self.nodes.iter().position(|n| n.node_id == entry.node_id) {
            self.nodes.remove(pos);
            self.nodes.push_back(entry);
            return true;
        }

        // Bucket not full — just add
        if self.nodes.len() < self.k {
            self.nodes.push_back(entry);
            return true;
        }

        // Bucket full — add to replacement cache (evict oldest replacement if full)
        if let Some(pos) = self.replacements.iter().position(|n| n.node_id == entry.node_id) {
            self.replacements.remove(pos);
        }
        if self.replacements.len() >= self.k {
            self.replacements.pop_front();
        }
        self.replacements.push_back(entry);
        false
    }

    /// Remove a node. If there's a replacement, promote it.
    fn remove(&mut self, node_id: &NodeId) -> bool {
        if let Some(pos) = self.nodes.iter().position(|n| &n.node_id == node_id) {
            self.nodes.remove(pos);
            // Promote from replacement cache
            if let Some(replacement) = self.replacements.pop_front() {
                self.nodes.push_back(replacement);
            }
            return true;
        }
        // Also check replacement cache
        if let Some(pos) = self.replacements.iter().position(|n| &n.node_id == node_id) {
            self.replacements.remove(pos);
            return true;
        }
        false
    }

    fn contains(&self, node_id: &NodeId) -> bool {
        self.nodes.iter().any(|n| &n.node_id == node_id)
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }
}

/// Kademlia routing table: 256 k-buckets indexed by XOR distance prefix length.
pub struct RoutingTable {
    self_id: NodeId,
    buckets: Vec<KBucket>,
}

impl RoutingTable {
    pub fn new(self_id: NodeId) -> Self {
        Self::with_k(self_id, K)
    }

    pub fn with_k(self_id: NodeId, k: usize) -> Self {
        let mut buckets = Vec::with_capacity(NUM_BUCKETS);
        for _ in 0..NUM_BUCKETS {
            buckets.push(KBucket::new(k));
        }
        Self { self_id, buckets }
    }

    pub fn self_id(&self) -> NodeId {
        self.self_id
    }

    /// Insert or update a node in the routing table.
    pub fn insert(&mut self, node_id: NodeId) -> bool {
        if node_id == self.self_id {
            return false;
        }
        let idx = self.bucket_index(&node_id);
        self.buckets[idx].insert(NodeEntry { node_id })
    }

    /// Remove a node from the routing table.
    pub fn remove(&mut self, node_id: &NodeId) -> bool {
        if *node_id == self.self_id {
            return false;
        }
        let idx = self.bucket_index(node_id);
        self.buckets[idx].remove(node_id)
    }

    /// Check if a node is in the routing table (main list, not replacements).
    pub fn contains(&self, node_id: &NodeId) -> bool {
        if *node_id == self.self_id {
            return false;
        }
        let idx = self.bucket_index(node_id);
        self.buckets[idx].contains(node_id)
    }

    /// Find the `count` closest nodes to `target` by XOR distance.
    pub fn closest(&self, target: &NodeId, count: usize) -> Vec<NodeEntry> {
        let mut all: Vec<(NodeEntry, [u8; 32])> = Vec::new();

        for bucket in &self.buckets {
            for entry in &bucket.nodes {
                let dist = entry.node_id.xor_distance(target);
                all.push((entry.clone(), dist));
            }
        }

        // Sort by XOR distance (lexicographic comparison of byte arrays)
        all.sort_by(|a, b| a.1.cmp(&b.1));
        all.truncate(count);
        all.into_iter().map(|(entry, _)| entry).collect()
    }

    /// Total number of nodes in the routing table.
    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// All nodes currently in the routing table (main lists only).
    pub fn all_nodes(&self) -> Vec<NodeEntry> {
        self.buckets
            .iter()
            .flat_map(|b| b.nodes.iter().cloned())
            .collect()
    }

    /// Non-empty bucket sizes as `(bucket_index, count)` pairs.
    pub fn bucket_sizes(&self) -> Vec<(usize, usize)> {
        self.buckets
            .iter()
            .enumerate()
            .filter(|(_, b)| !b.nodes.is_empty())
            .map(|(i, b)| (i, b.nodes.len()))
            .collect()
    }

    /// Bucket index for a node: number of leading zeros in XOR distance.
    /// Clamped to [0, 255].
    fn bucket_index(&self, node_id: &NodeId) -> usize {
        let lz = self.self_id.xor_leading_zeros(node_id) as usize;
        // lz = 256 means same node (shouldn't happen, we filter self).
        // Clamp to last bucket.
        lz.min(NUM_BUCKETS - 1)
    }
}
