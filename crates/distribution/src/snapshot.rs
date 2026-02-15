//! Serializable snapshot of a `DistributedNode`'s state.
//!
//! Used by the runtime-dashboard to display distribution monitoring data
//! for a single node without reaching out to other nodes.

use serde::{Deserialize, Serialize};

use crate::node::DistributedNode;
use crate::types::{MemberState, NodeId};

/// Snapshot of a single member in the SWIM membership list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberInfo {
    pub node_id: String,
    pub addr: Option<String>,
    pub state: String,
    pub incarnation: u64,
}

/// Snapshot of a node in the Kademlia routing table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeighborInfo {
    pub node_id: String,
    pub addr: Option<String>,
}

/// Snapshot of a single LRU cache entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntryInfo {
    pub actor_addr: String,
    pub node_id: String,
}

/// Snapshot of a single registry entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryEntryInfo {
    pub name: String,
    pub actor_addr: String,
    pub node_id: String,
    pub tombstone: bool,
}

/// Complete snapshot of a `DistributedNode`'s observable state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionNodeSnapshot {
    /// This node's ID (hex-encoded).
    pub node_id: String,
    /// This node's listen address (filled by driver, None for protocol-only snapshots).
    pub listen_addr: Option<String>,

    // ─── SWIM membership ─────────────────────────────────────────────
    /// All known members with their state.
    pub members: Vec<MemberInfo>,
    /// Count of alive members.
    pub alive_count: usize,
    /// Count of suspected members.
    pub suspect_count: usize,
    /// Count of dead members.
    pub dead_count: usize,

    // ─── Kademlia routing table ──────────────────────────────────────
    /// Total nodes in the routing table.
    pub routing_table_size: usize,
    /// Non-empty buckets as (bucket_index, entry_count).
    pub routing_buckets: Vec<(usize, usize)>,
    /// All nodes in the routing table.
    pub routing_neighbors: Vec<NeighborInfo>,

    // ─── Location cache ──────────────────────────────────────────────
    /// Number of entries in the LRU cache.
    pub cache_size: usize,
    /// All cache entries (actor → node).
    pub cache_entries: Vec<CacheEntryInfo>,

    // ─── Directory & repair ──────────────────────────────────────────
    /// Total directory entries in this node's shard.
    pub directory_entry_count: usize,
    /// Number of entries pending re-replication.
    pub repair_queue_size: usize,

    // ─── Registry ────────────────────────────────────────────────────
    /// Number of entries in the cluster registry (including tombstones).
    pub registry_size: usize,
    /// Number of tombstoned entries.
    pub registry_tombstones: usize,
    /// All registry entries.
    pub registry_entries: Vec<RegistryEntryInfo>,

    // ─── Gossip pairs ────────────────────────────────────────────────
    /// Recent SWIM probe targets (most recent last).
    pub recent_probe_targets: Vec<String>,
}

fn node_id_hex(id: &NodeId) -> String {
    id.0.iter().map(|b| format!("{:02x}", b)).collect()
}

fn state_str(state: MemberState) -> String {
    match state {
        MemberState::Alive => "alive".into(),
        MemberState::Suspect => "suspect".into(),
        MemberState::Dead => "dead".into(),
    }
}

impl DistributedNode {
    /// Capture a serializable snapshot of this node's current state.
    ///
    /// Address fields are left as `None` — the driver layer enriches them
    /// from its own address book.
    pub fn snapshot(&self) -> DistributionNodeSnapshot {
        let all_members = self.all_members();
        let members: Vec<MemberInfo> = all_members
            .iter()
            .map(|m| MemberInfo {
                node_id: node_id_hex(&m.node_id),
                addr: None,
                state: state_str(m.state),
                incarnation: m.incarnation,
            })
            .collect();

        let alive_count = all_members.iter().filter(|m| m.state == MemberState::Alive).count();
        let suspect_count = all_members.iter().filter(|m| m.state == MemberState::Suspect).count();
        let dead_count = all_members.iter().filter(|m| m.state == MemberState::Dead).count();

        let rt = self.routing_table();
        let routing_neighbors: Vec<NeighborInfo> = rt
            .all_nodes()
            .iter()
            .map(|n| NeighborInfo {
                node_id: node_id_hex(&n.node_id),
                addr: None,
            })
            .collect();

        let cache_entries: Vec<CacheEntryInfo> = self
            .cache()
            .entries()
            .iter()
            .map(|(actor, node)| CacheEntryInfo {
                actor_addr: format!("{:?}", actor),
                node_id: node_id_hex(node),
            })
            .collect();

        let recent_targets: Vec<String> = self
            .recent_probe_targets()
            .iter()
            .map(|id| node_id_hex(id))
            .collect();

        let registry = self.registry();
        let registry_entries: Vec<RegistryEntryInfo> = registry
            .entries()
            .map(|e| RegistryEntryInfo {
                name: e.name.clone(),
                actor_addr: format!("{}", e.actor_addr),
                node_id: node_id_hex(&e.node_id),
                tombstone: e.tombstone,
            })
            .collect();

        DistributionNodeSnapshot {
            node_id: node_id_hex(&self.node_id()),
            listen_addr: None,
            members,
            alive_count,
            suspect_count,
            dead_count,
            routing_table_size: rt.len(),
            routing_buckets: rt.bucket_sizes(),
            routing_neighbors,
            cache_size: self.cache().len(),
            cache_entries,
            directory_entry_count: self.directory().entry_count(),
            repair_queue_size: self.repair_queue_len(),
            registry_size: registry.len(),
            registry_tombstones: registry.tombstone_count(),
            registry_entries,
            recent_probe_targets: recent_targets,
        }
    }
}
