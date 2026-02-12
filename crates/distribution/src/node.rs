//! `DistributedNode` — the top-level integration type.
//!
//! Composes SWIM membership, Kademlia routing, directory, cache, and
//! transport into a single public API.

use std::net::SocketAddr;

use swactor::actor::ActorAddress;

use crate::cache::LocationCache;
use crate::crypto::Keypair;
use crate::kademlia::directory::{actor_addr_as_node_id, DirectoryShard};
use crate::kademlia::repair::{RepairQueue, RepublishTracker};
use crate::kademlia::routing_table::RoutingTable;
use crate::swim::node::{NodeAction, SwimNode};
use crate::swim::probe::SwimConfig;
use crate::types::{MemberState, NodeId, NodeRecord};

/// Configuration for a distributed node.
pub struct DistributedNodeConfig {
    pub listen_addr: SocketAddr,
    pub swim: SwimConfig,
    pub cache_capacity: usize,
    pub republish_interval: u64,
}

impl Default for DistributedNodeConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            swim: SwimConfig::default(),
            cache_capacity: 10_000,
            republish_interval: 1000,
        }
    }
}

/// The integrated distributed node.
///
/// Owns the node identity, SWIM membership, Kademlia routing table,
/// actor directory shard, location cache, and repair infrastructure.
pub struct DistributedNode {
    keypair: Keypair,
    swim: SwimNode,
    routing_table: RoutingTable,
    directory: DirectoryShard,
    cache: LocationCache,
    repair_queue: RepairQueue,
    republish: RepublishTracker,
    tick_count: u64,
}

impl DistributedNode {
    /// Create a new node with a fresh keypair.
    pub fn new(config: DistributedNodeConfig) -> Self {
        let keypair = Keypair::generate();
        Self::with_keypair(keypair, config)
    }

    /// Create a node with a specific keypair (for deterministic tests).
    pub fn with_keypair(keypair: Keypair, config: DistributedNodeConfig) -> Self {
        let node_id = keypair.node_id();
        Self {
            swim: SwimNode::new(node_id, config.listen_addr, config.swim),
            routing_table: RoutingTable::new(node_id),
            directory: DirectoryShard::new(),
            cache: LocationCache::new(config.cache_capacity),
            repair_queue: RepairQueue::new(),
            republish: RepublishTracker::new(config.republish_interval),
            tick_count: 0,
            keypair,
        }
    }

    // ─── Identity ───────────────────────────────────────────────────────

    pub fn node_id(&self) -> NodeId {
        self.keypair.node_id()
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.swim.self_addr()
    }

    pub fn keypair(&self) -> &Keypair {
        &self.keypair
    }

    // ─── Cluster operations ─────────────────────────────────────────────

    /// Join a cluster by contacting seed nodes.
    pub fn join(&self, seeds: &[SocketAddr]) -> Vec<NodeAction> {
        self.swim.join(seeds)
    }

    /// Leave the cluster gracefully.
    pub fn leave(&mut self) -> Vec<NodeAction> {
        self.swim.leave()
    }

    /// Current cluster members (non-dead).
    pub fn members(&self) -> Vec<NodeRecord> {
        self.swim
            .members()
            .alive_members()
            .into_iter()
            .map(|e| e.to_record())
            .collect()
    }

    /// All known members (including dead).
    pub fn all_members(&self) -> Vec<NodeRecord> {
        self.swim
            .members()
            .all_members()
            .into_iter()
            .map(|e| e.to_record())
            .collect()
    }

    // ─── Tick ───────────────────────────────────────────────────────────

    /// Advance the node by one tick. Drives SWIM probes, republishing, etc.
    /// Returns actions that the caller must translate into network I/O.
    pub fn tick(&mut self) -> Vec<NodeAction> {
        self.tick_count += 1;

        // Drive SWIM
        let actions = self.swim.tick();

        // Process membership changes from SWIM
        let membership_changes: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                NodeAction::MembershipChanged { node_id, state, .. } => Some((*node_id, *state)),
                _ => None,
            })
            .collect();

        for (node_id, state) in membership_changes {
            self.handle_membership_change(node_id, state);
        }

        // Periodic republish
        let to_republish = self.republish.tick(self.tick_count);
        for (_actor_addr, _generation) in to_republish {
            // In a real implementation, this would trigger STORE operations
            // For now, just a no-op placeholder — the caller would need to
            // re-sign and re-STORE these entries.
        }

        actions
    }

    // ─── SWIM message handling (delegate to SwimNode) ───────────────────

    pub fn handle_ping(&mut self, from: NodeId, from_addr: SocketAddr, sequence: u64, piggyback: &[u8]) -> Vec<NodeAction> {
        let actions = self.swim.handle_ping(from, from_addr, sequence, piggyback);
        self.maybe_update_routing_table(from, from_addr);
        actions
    }

    pub fn handle_ack(&mut self, from: NodeId, sequence: u64, piggyback: &[u8]) -> Vec<NodeAction> {
        self.swim.handle_ack(from, sequence, piggyback)
    }

    pub fn handle_ping_req(&mut self, from: NodeId, target: NodeId, target_addr: SocketAddr, sequence: u64, piggyback: &[u8]) -> Vec<NodeAction> {
        self.swim.handle_ping_req(from, target, target_addr, sequence, piggyback)
    }

    pub fn handle_join_request(&mut self, from: NodeId, from_addr: SocketAddr) -> Vec<NodeAction> {
        let actions = self.swim.handle_join_request(from, from_addr);
        self.maybe_update_routing_table(from, from_addr);
        actions
    }

    pub fn handle_join_response(&mut self, members: Vec<NodeRecord>) -> Vec<NodeAction> {
        for m in &members {
            if m.state != MemberState::Dead {
                self.routing_table.insert(m.node_id, m.addr);
            }
        }
        self.swim.handle_join_response(members)
    }

    // ─── Directory operations ───────────────────────────────────────────

    /// Register a locally-spawned actor in the directory.
    /// Returns a signed DirectoryEntry that should be STOREd on the
    /// `r` closest nodes.
    pub fn register_actor(&mut self, actor_addr: ActorAddress, generation: u64) -> crate::types::DirectoryEntry {
        let entry = self.keypair.sign_directory_entry(actor_addr, generation);
        self.directory.store(entry.clone());
        self.cache.insert(actor_addr, self.node_id());
        self.republish.register(actor_addr, generation);
        entry
    }

    /// Store a directory entry received from a remote STORE request.
    pub fn store_directory_entry(&mut self, entry: crate::types::DirectoryEntry) -> bool {
        self.directory.store(entry)
    }

    /// Resolve an actor's location: cache → local directory → needs network lookup.
    pub fn resolve_actor(&mut self, actor_addr: &ActorAddress) -> ResolveResult {
        // 1. Check cache
        if let Some(node_id) = self.cache.get(actor_addr) {
            return ResolveResult::Cached(node_id);
        }

        // 2. Check local directory shard
        if let Some(entries) = self.directory.get(actor_addr) {
            if let Some(entry) = entries.first() {
                self.cache.insert(*actor_addr, entry.node_id);
                return ResolveResult::Cached(entry.node_id);
            }
        }

        // 3. Need to do a Kademlia lookup
        let target = actor_addr_as_node_id(actor_addr);
        let closest = self.routing_table.closest(&target, 3);
        if closest.is_empty() {
            return ResolveResult::NotFound;
        }

        ResolveResult::NeedsLookup {
            closest_nodes: closest.into_iter().map(|e| (e.node_id, e.addr)).collect(),
        }
    }

    /// Invalidate a cached location (e.g. after delivery failure).
    pub fn invalidate_cache(&mut self, actor_addr: &ActorAddress) {
        self.cache.invalidate(actor_addr);
    }

    // ─── Accessors ──────────────────────────────────────────────────────

    pub fn routing_table(&self) -> &RoutingTable {
        &self.routing_table
    }

    pub fn directory(&self) -> &DirectoryShard {
        &self.directory
    }

    pub fn cache(&self) -> &LocationCache {
        &self.cache
    }

    pub fn repair_queue(&mut self) -> &mut RepairQueue {
        &mut self.repair_queue
    }

    pub fn repair_queue_len(&self) -> usize {
        self.repair_queue.len()
    }

    /// Recent SWIM probe targets (who this node has pinged recently).
    pub fn recent_probe_targets(&self) -> Vec<NodeId> {
        self.swim.recent_probe_targets().iter().copied().collect()
    }

    // ─── Internal ───────────────────────────────────────────────────────

    fn maybe_update_routing_table(&mut self, node_id: NodeId, addr: SocketAddr) {
        self.routing_table.insert(node_id, addr);
    }

    fn handle_membership_change(&mut self, node_id: NodeId, state: MemberState) {
        match state {
            MemberState::Alive => {
                if let Some(entry) = self.swim.members().get(&node_id) {
                    self.routing_table.insert(node_id, entry.addr);
                }
            }
            MemberState::Dead => {
                self.routing_table.remove(&node_id);
                self.cache.invalidate_node(&node_id);
                self.repair_queue.on_node_death(&node_id, &mut self.directory);
            }
            MemberState::Suspect => {
                // Keep in routing table but could downprioritize
            }
        }
    }
}

/// Result of resolving an actor's location.
#[derive(Debug)]
pub enum ResolveResult {
    /// Found in cache or local directory.
    Cached(NodeId),
    /// Need to do a Kademlia FIND_VALUE — here are the closest known nodes.
    NeedsLookup { closest_nodes: Vec<(NodeId, SocketAddr)> },
    /// No nodes known at all.
    NotFound,
}
