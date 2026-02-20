//! Pool disseminator — gossip-converged state for pool membership,
//! capacity, content locations, and ACL.
//!
//! Implements `GossipChannel` to plug into the generic gossip system
//! via `DistributedNode::register_channel()`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use distribution::gossip_channel::{DisseminationBuffer, GossipChannel, deserialize_each, serialize_each};
use distribution::types::NodeId;
use shared_types::ContentHash;
use shared_types::pool::*;

// ─── PoolDisseminator ──────────────────────────────────────────────────────

/// Configuration for the pool disseminator.
#[derive(Debug, Clone)]
pub struct PoolDisseminatorConfig {
    pub tombstone_ttl: u64,
    pub gc_interval: u64,
}

impl Default for PoolDisseminatorConfig {
    fn default() -> Self {
        Self {
            tombstone_ttl: 3600,
            gc_interval: 1000,
        }
    }
}

/// Manages converged pool state via gossip dissemination.
#[derive(Debug)]
pub struct PoolDisseminator {
    pool_id: PoolId,
    pool_name: String,
    local_node_id: NodeId,

    // Converged state maps
    members: HashMap<[u8; 32], PoolMemberEntry>,
    capacity: HashMap<[u8; 32], PoolCapacityEntry>,
    content_locations: HashMap<(ContentHash, [u8; 32]), ContentLocationEntry>,
    acl: HashMap<[u8; 32], PoolACLEntry>,

    // Dissemination buffer
    buffer: DisseminationBuffer<PoolEntry>,

    // Local generation counters
    local_member_gen: u64,
    local_capacity_gen: u64,

    config: PoolDisseminatorConfig,
    tick_count: u64,
}

impl PoolDisseminator {
    pub fn new(pool_id: PoolId, pool_name: String, local_node_id: NodeId, lambda: usize) -> Self {
        Self {
            pool_id,
            pool_name,
            local_node_id,
            members: HashMap::new(),
            capacity: HashMap::new(),
            content_locations: HashMap::new(),
            acl: HashMap::new(),
            buffer: DisseminationBuffer::new(lambda),
            local_member_gen: 0,
            local_capacity_gen: 0,
            config: PoolDisseminatorConfig::default(),
            tick_count: 0,
        }
    }

    pub fn with_config(mut self, config: PoolDisseminatorConfig) -> Self {
        self.config = config;
        self
    }

    pub fn pool_id(&self) -> PoolId {
        self.pool_id
    }

    // ─── Lifecycle methods ──────────────────────────────────────────────

    /// Join the pool. Announces Active membership.
    pub fn join(&mut self, cluster_size: usize) {
        self.local_member_gen += 1;
        let entry = PoolMemberEntry {
            pool_id: self.pool_id,
            node_id: self.local_node_id.0,
            state: PoolMemberState::Active,
            generation: self.local_member_gen,
        };
        self.merge_membership(entry.clone());
        self.buffer.enqueue(PoolEntry::Membership(entry), cluster_size);
    }

    /// Leave the pool. Announces Left membership.
    pub fn leave(&mut self, cluster_size: usize) {
        self.local_member_gen += 1;
        let entry = PoolMemberEntry {
            pool_id: self.pool_id,
            node_id: self.local_node_id.0,
            state: PoolMemberState::Left,
            generation: self.local_member_gen,
        };
        self.merge_membership(entry.clone());
        self.buffer.enqueue(PoolEntry::Membership(entry), cluster_size);
    }

    /// Announce storage capacity.
    pub fn announce_capacity(&mut self, total: u64, used: u64, cluster_size: usize) {
        self.local_capacity_gen += 1;
        let entry = PoolCapacityEntry {
            pool_id: self.pool_id,
            node_id: self.local_node_id.0,
            total_bytes: total,
            used_bytes: used,
            generation: self.local_capacity_gen,
        };
        self.merge_capacity(entry.clone());
        self.buffer.enqueue(PoolEntry::Capacity(entry), cluster_size);
    }

    /// Announce that this node has a piece of content.
    pub fn announce_content(&mut self, hash: ContentHash, cluster_size: usize) {
        let key = (hash, self.local_node_id.0);
        let next_gen = self.content_locations.get(&key).map_or(1, |e| e.generation + 1);
        let entry = ContentLocationEntry {
            pool_id: self.pool_id,
            content_hash: hash,
            node_id: self.local_node_id.0,
            generation: next_gen,
            tombstone: false,
        };
        self.merge_content_location(entry.clone());
        self.buffer.enqueue(PoolEntry::ContentLocation(entry), cluster_size);
    }

    /// Remove content announcement (tombstone).
    pub fn remove_content(&mut self, hash: ContentHash, cluster_size: usize) {
        let key = (hash, self.local_node_id.0);
        let next_gen = self.content_locations.get(&key).map_or(1, |e| e.generation + 1);
        let entry = ContentLocationEntry {
            pool_id: self.pool_id,
            content_hash: hash,
            node_id: self.local_node_id.0,
            generation: next_gen,
            tombstone: true,
        };
        self.merge_content_location(entry.clone());
        self.buffer.enqueue(PoolEntry::ContentLocation(entry), cluster_size);
    }

    /// Grant access to a node.
    pub fn grant_access(&mut self, target: NodeId, cluster_size: usize) {
        let next_gen = self.acl.get(&target.0).map_or(1, |e| e.generation + 1);
        let entry = PoolACLEntry {
            pool_id: self.pool_id,
            node_id: target.0,
            granted_by: self.local_node_id.0,
            generation: next_gen,
            revoked: false,
        };
        self.merge_acl(entry.clone());
        self.buffer.enqueue(PoolEntry::ACL(entry), cluster_size);
    }

    /// Revoke access from a node.
    pub fn revoke_access(&mut self, target: NodeId, cluster_size: usize) {
        let next_gen = self.acl.get(&target.0).map_or(1, |e| e.generation + 1);
        let entry = PoolACLEntry {
            pool_id: self.pool_id,
            node_id: target.0,
            granted_by: self.local_node_id.0,
            generation: next_gen,
            revoked: true,
        };
        self.merge_acl(entry.clone());
        self.buffer.enqueue(PoolEntry::ACL(entry), cluster_size);
    }

    // ─── Query API ──────────────────────────────────────────────────────

    /// All active pool members.
    pub fn active_members(&self) -> Vec<NodeId> {
        self.members
            .values()
            .filter(|m| m.state == PoolMemberState::Active)
            .map(|m| NodeId(m.node_id))
            .collect()
    }

    /// Total and used capacity across the pool.
    pub fn pool_capacity_summary(&self) -> (u64, u64) {
        let mut total = 0u64;
        let mut used = 0u64;
        for cap in self.capacity.values() {
            // Only count active members
            if let Some(m) = self.members.get(&cap.node_id) {
                if m.state == PoolMemberState::Active {
                    total = total.saturating_add(cap.total_bytes);
                    used = used.saturating_add(cap.used_bytes);
                }
            }
        }
        (total, used)
    }

    /// Find which nodes have a given content hash.
    pub fn locate_content(&self, hash: &ContentHash) -> Vec<NodeId> {
        self.content_locations
            .iter()
            .filter(|((h, _), entry)| h == hash && !entry.tombstone)
            .map(|((_, node_id), _)| NodeId(*node_id))
            .collect()
    }

    /// Find the node with the most free space.
    pub fn node_with_most_free_space(&self) -> Option<NodeId> {
        self.capacity
            .values()
            .filter(|cap| {
                self.members
                    .get(&cap.node_id)
                    .is_some_and(|m| m.state == PoolMemberState::Active)
            })
            .max_by_key(|cap| cap.total_bytes.saturating_sub(cap.used_bytes))
            .map(|cap| NodeId(cap.node_id))
    }

    /// Check if a node is authorized to join this pool.
    pub fn is_node_authorized(&self, node_id: &NodeId) -> bool {
        // If no ACL entries exist, the pool is open
        if self.acl.is_empty() {
            return true;
        }
        self.acl
            .get(&node_id.0)
            .is_some_and(|entry| !entry.revoked)
    }

    /// Number of active members.
    pub fn member_count(&self) -> usize {
        self.members
            .values()
            .filter(|m| m.state == PoolMemberState::Active)
            .count()
    }

    /// Number of live content location entries (non-tombstone).
    pub fn content_count(&self) -> usize {
        self.content_locations
            .values()
            .filter(|e| !e.tombstone)
            .count()
    }

    /// Serialize the current pool state to a JSON string for the dashboard.
    pub fn snapshot_json(&self) -> String {
        fn hex(bytes: &[u8; 32]) -> String {
            bytes.iter().map(|b| format!("{b:02x}")).collect()
        }

        let (total_bytes, used_bytes) = self.pool_capacity_summary();

        let members: Vec<serde_json::Value> = self
            .members
            .values()
            .filter(|m| m.state == PoolMemberState::Active)
            .map(|m| {
                let cap = self.capacity.get(&m.node_id);
                serde_json::json!({
                    "node_id": hex(&m.node_id),
                    "state": format!("{:?}", m.state),
                    "generation": m.generation,
                    "total_bytes": cap.map_or(0, |c| c.total_bytes),
                    "used_bytes": cap.map_or(0, |c| c.used_bytes),
                })
            })
            .collect();

        // Group content locations by hash
        let mut by_hash: HashMap<ContentHash, Vec<[u8; 32]>> = HashMap::new();
        for ((hash, _), entry) in &self.content_locations {
            if !entry.tombstone {
                by_hash.entry(*hash).or_default().push(entry.node_id);
            }
        }
        let content_locations: Vec<serde_json::Value> = by_hash
            .iter()
            .map(|(hash, nodes)| {
                serde_json::json!({
                    "content_hash": hash.to_hex(),
                    "nodes": nodes.iter().map(hex).collect::<Vec<_>>(),
                    "replica_count": nodes.len(),
                })
            })
            .collect();

        let acl: Vec<serde_json::Value> = self
            .acl
            .values()
            .map(|a| {
                serde_json::json!({
                    "node_id": hex(&a.node_id),
                    "granted_by": hex(&a.granted_by),
                    "revoked": a.revoked,
                })
            })
            .collect();

        let acl_mode = if self.acl.is_empty() { "open" } else { "allow-list" };

        serde_json::json!({
            "pool_name": self.pool_name,
            "pool_id": self.pool_id.to_hex(),
            "member_count": self.member_count(),
            "content_count": self.content_count(),
            "total_bytes": total_bytes,
            "used_bytes": used_bytes,
            "members": members,
            "content_locations": content_locations,
            "acl": acl,
            "acl_mode": acl_mode,
        })
        .to_string()
    }

    // ─── Internal merge logic ───────────────────────────────────────────

    fn merge_membership(&mut self, entry: PoolMemberEntry) -> bool {
        let key = entry.node_id;
        if let Some(existing) = self.members.get(&key) {
            if entry.generation <= existing.generation {
                return false;
            }
        }
        self.members.insert(key, entry);
        true
    }

    fn merge_capacity(&mut self, entry: PoolCapacityEntry) -> bool {
        let key = entry.node_id;
        if let Some(existing) = self.capacity.get(&key) {
            if entry.generation <= existing.generation {
                return false;
            }
        }
        self.capacity.insert(key, entry);
        true
    }

    fn merge_content_location(&mut self, entry: ContentLocationEntry) -> bool {
        let key = (entry.content_hash, entry.node_id);
        if let Some(existing) = self.content_locations.get(&key) {
            if entry.generation <= existing.generation {
                return false;
            }
        }
        self.content_locations.insert(key, entry);
        true
    }

    fn merge_acl(&mut self, entry: PoolACLEntry) -> bool {
        let key = entry.node_id;
        if let Some(existing) = self.acl.get(&key) {
            if entry.generation <= existing.generation {
                return false;
            }
        }
        self.acl.insert(key, entry);
        true
    }

    /// Merge a single pool entry and return whether state changed.
    fn merge_entry(&mut self, entry: PoolEntry) -> bool {
        match entry {
            PoolEntry::Membership(m) => self.merge_membership(m),
            PoolEntry::Capacity(c) => self.merge_capacity(c),
            PoolEntry::ContentLocation(cl) => self.merge_content_location(cl),
            PoolEntry::ACL(a) => self.merge_acl(a),
        }
    }

    /// Take pending entries (internal, typed).
    fn take_pending_inner(&mut self, max_count: usize) -> Vec<PoolEntry> {
        self.buffer.take(max_count)
    }

    /// Apply incoming entries (internal, typed).
    fn apply_incoming_inner(&mut self, entries: Vec<PoolEntry>, cluster_size: usize) {
        for entry in entries {
            if self.merge_entry(entry.clone()) {
                self.buffer.enqueue(entry, cluster_size);
            }
        }
    }

    /// Re-enqueue all state (internal).
    fn re_disseminate_all_inner(&mut self, cluster_size: usize) {
        let mut all_entries: Vec<PoolEntry> = Vec::new();

        for m in self.members.values().cloned() {
            all_entries.push(PoolEntry::Membership(m));
        }
        for c in self.capacity.values().cloned() {
            all_entries.push(PoolEntry::Capacity(c));
        }
        for cl in self.content_locations.values().cloned() {
            all_entries.push(PoolEntry::ContentLocation(cl));
        }
        for a in self.acl.values().cloned() {
            all_entries.push(PoolEntry::ACL(a));
        }

        self.buffer.re_enqueue_all(all_entries, cluster_size);
    }

    /// GC: evict tombstones past TTL.
    fn gc_tick_inner(&mut self) {
        self.tick_count += 1;
        if self.tick_count % self.config.gc_interval != 0 {
            return;
        }

        let ttl = self.config.tombstone_ttl;
        let tick = self.tick_count;

        // GC left members
        self.members.retain(|_, m| {
            if m.state == PoolMemberState::Left {
                m.generation + ttl > tick
            } else {
                true
            }
        });

        // GC tombstoned content locations
        self.content_locations.retain(|_, cl| {
            if cl.tombstone {
                cl.generation + ttl > tick
            } else {
                true
            }
        });

        // GC revoked ACL entries
        self.acl.retain(|_, a| {
            if a.revoked {
                a.generation + ttl > tick
            } else {
                true
            }
        });
    }
}

// ─── SharedPoolChannel ─────────────────────────────────────────────────────

/// Wrapper around `Arc<Mutex<PoolDisseminator>>` that implements `GossipChannel`.
///
/// This enables shared ownership between the `PoolCoordinator` actor
/// (which needs query/lifecycle access) and `DistributedNode` (which
/// drives gossip piggyback).
pub struct SharedPoolChannel {
    inner: Arc<Mutex<PoolDisseminator>>,
}

impl SharedPoolChannel {
    pub fn new(disseminator: Arc<Mutex<PoolDisseminator>>) -> Self {
        Self { inner: disseminator }
    }

    /// Consume the channel and return the underlying `Arc<Mutex<PoolDisseminator>>`.
    pub fn into_inner(self) -> Arc<Mutex<PoolDisseminator>> {
        self.inner
    }
}

impl GossipChannel for SharedPoolChannel {
    fn topic_tag(&self) -> &'static str {
        "pool"
    }

    fn take_pending_bytes(&mut self, max_entries: usize) -> Vec<Vec<u8>> {
        let entries = self.inner.lock().unwrap().take_pending_inner(max_entries);
        serialize_each(&entries)
    }

    fn apply_incoming_bytes(&mut self, entries: &[Vec<u8>], cluster_size: usize) {
        let parsed: Vec<PoolEntry> = deserialize_each(entries);
        self.inner.lock().unwrap().apply_incoming_inner(parsed, cluster_size);
    }

    fn re_disseminate_all(&mut self, cluster_size: usize) {
        self.inner.lock().unwrap().re_disseminate_all_inner(cluster_size);
    }

    fn on_node_death(&mut self, _node_id: &NodeId) {
        // Pool membership is explicit (join/leave), not auto-removed on node death.
        // Capacity becomes unreliable but we don't remove it.
    }

    fn gc_tick(&mut self) {
        self.inner.lock().unwrap().gc_tick_inner();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_id(b: u8) -> NodeId {
        NodeId([b; 32])
    }

    fn make_disseminator(b: u8) -> PoolDisseminator {
        PoolDisseminator::new(
            PoolId::from_name("test-pool"),
            "test-pool".into(),
            node_id(b),
            3,
        )
    }

    #[test]
    fn join_and_query_members() {
        let mut d = make_disseminator(1);
        d.join(2);

        assert_eq!(d.member_count(), 1);
        assert_eq!(d.active_members(), vec![node_id(1)]);
    }

    #[test]
    fn leave_removes_from_active() {
        let mut d = make_disseminator(1);
        d.join(2);
        d.leave(2);

        assert_eq!(d.member_count(), 0);
        assert!(d.active_members().is_empty());
    }

    #[test]
    fn announce_and_locate_content() {
        let mut d = make_disseminator(1);
        d.join(2);
        let hash = ContentHash::of(b"test-data");
        d.announce_content(hash, 2);

        let locations = d.locate_content(&hash);
        assert_eq!(locations, vec![node_id(1)]);
    }

    #[test]
    fn remove_content_tombstones() {
        let mut d = make_disseminator(1);
        d.join(2);
        let hash = ContentHash::of(b"test-data");
        d.announce_content(hash, 2);
        d.remove_content(hash, 2);

        assert!(d.locate_content(&hash).is_empty());
    }

    #[test]
    fn capacity_summary() {
        let mut d = make_disseminator(1);
        d.join(2);
        d.announce_capacity(1000, 300, 2);

        let (total, used) = d.pool_capacity_summary();
        assert_eq!(total, 1000);
        assert_eq!(used, 300);
    }

    #[test]
    fn acl_grant_and_check() {
        let mut d = make_disseminator(1);
        d.grant_access(node_id(2), 2);

        assert!(d.is_node_authorized(&node_id(2)));
        assert!(!d.is_node_authorized(&node_id(3)));
    }

    #[test]
    fn acl_revoke() {
        let mut d = make_disseminator(1);
        d.grant_access(node_id(2), 2);
        d.revoke_access(node_id(2), 2);

        assert!(!d.is_node_authorized(&node_id(2)));
    }

    #[test]
    fn empty_acl_means_open() {
        let d = make_disseminator(1);
        assert!(d.is_node_authorized(&node_id(99)));
    }

    #[test]
    fn higher_generation_wins_merge() {
        let mut d1 = make_disseminator(1);
        let mut d2 = make_disseminator(2);

        // d1 joins
        d1.join(2);
        // d1 leaves
        d1.leave(2);

        // Gossip d1's entries to d2 out of order:
        // First send the Active (gen 1), then the Left (gen 2)
        let active_entry = PoolEntry::Membership(PoolMemberEntry {
            pool_id: PoolId::from_name("test-pool"),
            node_id: [1u8; 32],
            state: PoolMemberState::Active,
            generation: 1,
        });
        let left_entry = PoolEntry::Membership(PoolMemberEntry {
            pool_id: PoolId::from_name("test-pool"),
            node_id: [1u8; 32],
            state: PoolMemberState::Left,
            generation: 2,
        });

        // Apply Left first (gen 2), then Active (gen 1) — Active should be rejected
        d2.merge_entry(left_entry);
        let changed = d2.merge_entry(active_entry);
        assert!(!changed, "lower generation should not win");

        // d2 should see node_id(1) as Left
        assert_eq!(d2.member_count(), 0); // Active count is 0
    }

    #[test]
    fn two_disseminators_converge_via_gossip_exchange() {
        let mut d1 = make_disseminator(1);
        let mut d2 = make_disseminator(2);

        // d1 joins and announces content
        d1.join(2);
        let hash = ContentHash::of(b"shared-file");
        d1.announce_content(hash, 2);

        // d2 joins
        d2.join(2);

        // Simulate gossip: d1 → d2
        let pending = d1.take_pending_inner(100);
        let bytes = serialize_each(&pending);
        let parsed: Vec<PoolEntry> = deserialize_each(&bytes);
        d2.apply_incoming_inner(parsed, 2);

        // d2 should now see d1 as a member and know about the content
        assert_eq!(d2.member_count(), 2);
        assert_eq!(d2.locate_content(&hash), vec![node_id(1)]);

        // Simulate gossip: d2 → d1
        let pending = d2.take_pending_inner(100);
        let bytes = serialize_each(&pending);
        let parsed: Vec<PoolEntry> = deserialize_each(&bytes);
        d1.apply_incoming_inner(parsed, 2);

        // d1 should now see d2 as a member
        assert_eq!(d1.member_count(), 2);
    }

    #[test]
    fn three_node_convergence_loop() {
        let pool = PoolId::from_name("test-pool");
        let mut nodes: Vec<PoolDisseminator> = (0..3)
            .map(|i| PoolDisseminator::new(pool, "test-pool".into(), node_id(i as u8), 3))
            .collect();

        // Each node joins
        for n in &mut nodes {
            n.join(3);
        }

        // Node 0 announces content
        let hash = ContentHash::of(b"convergence-test");
        nodes[0].announce_content(hash, 3);

        // Run 5 gossip rounds where each node exchanges with all others
        for _ in 0..5 {
            // Collect pending from each node
            let pending_bytes: Vec<Vec<Vec<u8>>> = nodes
                .iter_mut()
                .map(|n| serialize_each(&n.take_pending_inner(100)))
                .collect();

            // Apply each node's pending to all other nodes
            for (sender_idx, bytes) in pending_bytes.iter().enumerate() {
                for (receiver_idx, node) in nodes.iter_mut().enumerate() {
                    if sender_idx != receiver_idx {
                        let parsed: Vec<PoolEntry> = deserialize_each(bytes);
                        node.apply_incoming_inner(parsed, 3);
                    }
                }
            }
        }

        // All nodes should agree on membership and content locations
        for (i, node) in nodes.iter().enumerate() {
            assert_eq!(node.member_count(), 3, "node {i} should see 3 members");
            assert_eq!(
                node.locate_content(&hash),
                vec![node_id(0)],
                "node {i} should know content is on node 0"
            );
        }
    }
}
