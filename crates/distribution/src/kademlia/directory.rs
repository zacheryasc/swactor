//! Actor directory — STORE and FIND_VALUE with quorum reads.
//!
//! Each node holds a shard of the directory: `ActorAddress → Vec<DirectoryEntry>`.
//! STORE replicates entries to the `r` closest nodes (by XOR on the actor address
//! treated as a 256-bit key). FIND_VALUE does quorum reads with signature verification.

use std::collections::HashMap;

use swactor::actor::ActorAddress;

use crate::crypto;
use crate::types::{DirectoryEntry, NodeId};

/// Local directory shard storage.
pub struct DirectoryShard {
    entries: HashMap<ActorAddress, Vec<DirectoryEntry>>,
}

impl Default for DirectoryShard {
    fn default() -> Self {
        Self::new()
    }
}

impl DirectoryShard {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Store a directory entry. Verifies the signature before storing.
    /// Returns `true` if the entry was stored (new or higher generation).
    pub fn store(&mut self, entry: DirectoryEntry) -> bool {
        // Verify signature
        if !crypto::verify_directory_entry(&entry) {
            return false;
        }

        let entries = self.entries.entry(entry.actor_addr).or_default();

        // Check if we already have an entry from this node
        if let Some(existing) = entries.iter_mut().find(|e| e.node_id == entry.node_id) {
            if entry.generation > existing.generation {
                *existing = entry;
                return true;
            }
            return false;
        }

        entries.push(entry);
        true
    }

    /// Look up entries for an actor address.
    pub fn get(&self, actor_addr: &ActorAddress) -> Option<&[DirectoryEntry]> {
        self.entries.get(actor_addr).map(|v| v.as_slice())
    }

    /// Remove all entries for a specific node (e.g. when declared dead).
    pub fn remove_by_node(&mut self, node_id: &NodeId) -> Vec<DirectoryEntry> {
        let mut removed = Vec::new();
        for entries in self.entries.values_mut() {
            let _before = entries.len();
            let drained: Vec<_> = std::mem::take(entries);
            for entry in drained {
                if entry.node_id == *node_id {
                    removed.push(entry);
                } else {
                    entries.push(entry);
                }
            }
        }
        // Clean up empty vecs
        self.entries.retain(|_, v| !v.is_empty());
        removed
    }

    /// Remove entries that match a predicate (e.g. TTL expiration).
    pub fn remove_where<F: Fn(&DirectoryEntry) -> bool>(&mut self, predicate: F) -> Vec<DirectoryEntry> {
        let mut removed = Vec::new();
        for entries in self.entries.values_mut() {
            let drained: Vec<_> = std::mem::take(entries);
            for entry in drained {
                if predicate(&entry) {
                    removed.push(entry);
                } else {
                    entries.push(entry);
                }
            }
        }
        self.entries.retain(|_, v| !v.is_empty());
        removed
    }

    /// All actor addresses in this shard.
    pub fn actor_addresses(&self) -> Vec<ActorAddress> {
        self.entries.keys().copied().collect()
    }

    /// Total number of entries across all actors.
    pub fn entry_count(&self) -> usize {
        self.entries.values().map(|v| v.len()).sum()
    }
}

// ─── Quorum resolution ─────────────────────────────────────────────────────

/// Result of a quorum FIND_VALUE resolution.
#[derive(Debug)]
pub enum QuorumResult {
    /// Quorum achieved — this is the authoritative entry.
    Resolved(DirectoryEntry),
    /// Not enough agreement — here are all entries received.
    NoQuorum(Vec<DirectoryEntry>),
    /// No entries found at all.
    NotFound,
}

/// Resolve a set of directory entries from multiple nodes using quorum reads.
///
/// - `entries`: all entries received from `r` nodes
/// - `quorum`: minimum agreement count (`f + 1`)
///
/// Quorum rule: entries agreeing on `(node_id, generation)` with valid signatures.
/// Among quorum groups, highest generation wins.
pub fn resolve_quorum(entries: &[DirectoryEntry], quorum: usize) -> QuorumResult {
    if entries.is_empty() {
        return QuorumResult::NotFound;
    }

    // Group entries by (node_id, generation)
    let mut groups: HashMap<(NodeId, u64), Vec<&DirectoryEntry>> = HashMap::new();
    for entry in entries {
        if crypto::verify_directory_entry(entry) {
            groups
                .entry((entry.node_id, entry.generation))
                .or_default()
                .push(entry);
        }
    }

    // Find groups that meet quorum
    let mut quorum_groups: Vec<_> = groups
        .into_iter()
        .filter(|(_, group)| group.len() >= quorum)
        .collect();

    if quorum_groups.is_empty() {
        return QuorumResult::NoQuorum(entries.to_vec());
    }

    // Highest generation wins among quorum groups
    quorum_groups.sort_by(|a, b| b.0 .1.cmp(&a.0 .1));

    QuorumResult::Resolved(quorum_groups[0].1[0].clone())
}

/// Compute the `NodeId` that an actor address would be closest to in the
/// Kademlia keyspace. This is simply the actor address bytes interpreted as a NodeId.
pub fn actor_addr_as_node_id(addr: &ActorAddress) -> NodeId {
    NodeId(addr.0)
}
