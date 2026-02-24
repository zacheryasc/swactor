//! `DistributedNode` — the top-level integration type.
//!
//! Composes SWIM membership, Kademlia routing, directory, cache, and
//! transport into a single public API.

use swactor::actor::ActorAddress;

use crate::cache::LocationCache;
use crate::crypto::{Keypair, KeypairExt};
use crate::kademlia::directory::{actor_addr_as_node_id, DirectoryShard};
use crate::kademlia::repair::{RepairQueue, RepublishTracker};
use crate::kademlia::routing_table::RoutingTable;
use crate::node_metadata::{NodeMetadataDisseminator, NodeMetadataEntry};
use crate::registry::{
    pack_combined_piggyback, unpack_combined_piggyback, ClusterRegistry, RegistryConfig,
    RegistryEntry, RegistryEvent,
};
use crate::swim::node::{NodeAction, SwimNode};
use crate::swim::probe::SwimConfig;
use crate::types::{MemberState, NodeId, NodeRecord};

/// Configuration for a distributed node.
#[derive(Clone)]
pub struct DistributedNodeConfig {
    pub swim: SwimConfig,
    pub cache_capacity: usize,
    pub republish_interval: u64,
    pub registry: RegistryConfig,
    /// Dissemination multiplier for node metadata (default: 3).
    pub metadata_lambda: usize,
}

impl Default for DistributedNodeConfig {
    fn default() -> Self {
        Self {
            swim: SwimConfig::default(),
            cache_capacity: 10_000,
            republish_interval: 1000,
            registry: RegistryConfig::default(),
            metadata_lambda: 3,
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
    registry: ClusterRegistry,
    metadata: NodeMetadataDisseminator,
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
            swim: SwimNode::new(node_id, config.swim),
            routing_table: RoutingTable::new(node_id),
            directory: DirectoryShard::new(),
            cache: LocationCache::new(config.cache_capacity),
            repair_queue: RepairQueue::new(),
            republish: RepublishTracker::new(config.republish_interval),
            registry: ClusterRegistry::new(config.registry),
            metadata: NodeMetadataDisseminator::new(config.metadata_lambda),
            tick_count: 0,
            keypair,
        }
    }

    // ─── Identity ───────────────────────────────────────────────────────

    pub fn node_id(&self) -> NodeId {
        self.keypair.node_id()
    }

    pub fn keypair(&self) -> &Keypair {
        &self.keypair
    }

    // ─── Cluster operations ─────────────────────────────────────────────

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
        self.process_membership_changes(&actions);

        // Periodic republish
        let to_republish = self.republish.tick(self.tick_count);
        for (_actor_addr, _generation) in to_republish {
            // In a real implementation, this would trigger STORE operations
            // For now, just a no-op placeholder — the caller would need to
            // re-sign and re-STORE these entries.
        }

        // Registry GC
        self.registry.gc_tick();

        // Wrap outgoing piggyback with registry + metadata entries
        self.inject_piggyback(actions)
    }

    // ─── SWIM message handling (delegate to SwimNode) ───────────────────

    pub fn handle_ping(&mut self, from: NodeId, sequence: u64, piggyback: &[u8]) -> Vec<NodeAction> {
        let (membership_bytes, registry_entries, metadata_entries) =
            unpack_combined_piggyback(piggyback);
        let actions = self.swim.handle_ping(from, sequence, &membership_bytes);
        self.process_membership_changes(&actions);
        self.merge_registry_entries(registry_entries);
        self.merge_metadata_entries(metadata_entries);
        self.maybe_update_routing_table(from);
        self.inject_piggyback(actions)
    }

    pub fn handle_ack(&mut self, from: NodeId, sequence: u64, piggyback: &[u8]) -> Vec<NodeAction> {
        let (membership_bytes, registry_entries, metadata_entries) =
            unpack_combined_piggyback(piggyback);
        let actions = self.swim.handle_ack(from, sequence, &membership_bytes);
        self.process_membership_changes(&actions);
        self.merge_registry_entries(registry_entries);
        self.merge_metadata_entries(metadata_entries);
        self.inject_piggyback(actions)
    }

    pub fn handle_ping_req(&mut self, from: NodeId, target: NodeId, sequence: u64, piggyback: &[u8]) -> Vec<NodeAction> {
        let (membership_bytes, registry_entries, metadata_entries) =
            unpack_combined_piggyback(piggyback);
        let actions = self.swim.handle_ping_req(from, target, sequence, &membership_bytes);
        self.process_membership_changes(&actions);
        self.merge_registry_entries(registry_entries);
        self.merge_metadata_entries(metadata_entries);
        self.inject_piggyback(actions)
    }

    pub fn handle_indirect_ack(&mut self, target: NodeId, sequence: u64, piggyback: &[u8]) -> Vec<NodeAction> {
        let (membership_bytes, registry_entries, metadata_entries) =
            unpack_combined_piggyback(piggyback);
        let actions = self.swim.handle_indirect_ack(target, sequence, &membership_bytes);
        self.process_membership_changes(&actions);
        self.merge_registry_entries(registry_entries);
        self.merge_metadata_entries(metadata_entries);
        self.inject_piggyback(actions)
    }

    pub fn handle_join_request(&mut self, from: NodeId) -> Vec<NodeAction> {
        let actions = self.swim.handle_join_request(from);
        self.maybe_update_routing_table(from);
        actions
    }

    pub fn handle_join_response(&mut self, members: Vec<NodeRecord>) -> Vec<NodeAction> {
        for m in &members {
            if m.state != MemberState::Dead {
                self.routing_table.insert(m.node_id);
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
        if let Some(entries) = self.directory.get(actor_addr)
            && let Some(entry) = entries.first() {
                self.cache.insert(*actor_addr, entry.node_id);
                return ResolveResult::Cached(entry.node_id);
            }

        // 3. Need to do a Kademlia lookup
        let target = actor_addr_as_node_id(actor_addr);
        let closest = self.routing_table.closest(&target, 3);
        if closest.is_empty() {
            return ResolveResult::NotFound;
        }

        ResolveResult::NeedsLookup {
            closest_nodes: closest.into_iter().map(|e| e.node_id).collect(),
        }
    }

    /// Invalidate a cached location (e.g. after delivery failure).
    pub fn invalidate_cache(&mut self, actor_addr: &ActorAddress) {
        self.cache.invalidate(actor_addr);
    }

    // ─── Registry (name → actor mapping) ──────────────────────────────

    /// Register a human-readable name for an actor on this node.
    pub fn register_name(&mut self, name: String, actor_addr: ActorAddress) {
        self.registry.register(name, actor_addr, self.node_id(), self.cluster_size());
    }

    /// Unregister a name (creates a tombstone).
    pub fn unregister_name(&mut self, name: &str) {
        self.registry.unregister(name, self.node_id(), self.cluster_size());
    }

    /// Resolve a name to its current (ActorAddress, NodeId).
    pub fn resolve_name(&self, name: &str) -> Option<(ActorAddress, NodeId)> {
        self.registry.resolve(name)
    }

    /// Drain registry events (Registered / Unregistered).
    pub fn registry_events(&mut self) -> Vec<RegistryEvent> {
        self.registry.drain_events()
    }

    /// Read-only access to the registry.
    pub fn registry(&self) -> &ClusterRegistry {
        &self.registry
    }

    // ─── Node metadata (relay URL) ─────────────────────────────────────

    /// Set this node's relay URL and begin gossiping it to the cluster.
    pub fn set_relay_url(&mut self, url: Option<String>) {
        self.metadata
            .set_local(self.node_id(), url, self.cluster_size());
    }

    /// Look up a node's relay URL.
    pub fn relay_url(&self, node_id: &NodeId) -> Option<&str> {
        self.metadata.relay_url(node_id)
    }

    /// Read-only access to the metadata disseminator.
    pub fn metadata(&self) -> &NodeMetadataDisseminator {
        &self.metadata
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

    fn maybe_update_routing_table(&mut self, node_id: NodeId) {
        self.routing_table.insert(node_id);
    }

    fn process_membership_changes(&mut self, actions: &[NodeAction]) {
        for action in actions {
            if let NodeAction::MembershipChanged { node_id, state, .. } = action {
                self.handle_membership_change(*node_id, *state);
            }
        }
    }

    fn handle_membership_change(&mut self, node_id: NodeId, state: MemberState) {
        match state {
            MemberState::Alive => {
                self.routing_table.insert(node_id);
                // Re-disseminate registry + metadata entries so the recovering
                // node catches up on state accumulated during the partition.
                let size = self.cluster_size();
                self.registry.re_disseminate_all(size);
                self.metadata.re_disseminate_all(size);
            }
            MemberState::Dead => {
                self.routing_table.remove(&node_id);
                self.cache.invalidate_node(&node_id);
                self.repair_queue.on_node_death(&node_id, &mut self.directory);
                self.registry.tombstone_node(node_id, self.cluster_size());
                self.metadata.remove_node(&node_id);
            }
            MemberState::Suspect => {
                // Keep in routing table but could downprioritize
            }
        }
    }

    fn cluster_size(&self) -> usize {
        self.swim.members().alive_count() + 1 // +1 for self
    }

    /// Post-process outgoing actions: wrap each piggyback with registry + metadata entries.
    fn inject_piggyback(&mut self, actions: Vec<NodeAction>) -> Vec<NodeAction> {
        actions
            .into_iter()
            .map(|action| match action {
                NodeAction::SendPing { to, sequence, piggyback } => {
                    let registry_entries = self.registry.take_pending(8);
                    let metadata_entries = self.metadata.take_pending(4);
                    let combined = pack_combined_piggyback(piggyback, registry_entries, metadata_entries);
                    NodeAction::SendPing { to, sequence, piggyback: combined }
                }
                NodeAction::SendAck { to, sequence, piggyback } => {
                    let registry_entries = self.registry.take_pending(8);
                    let metadata_entries = self.metadata.take_pending(4);
                    let combined = pack_combined_piggyback(piggyback, registry_entries, metadata_entries);
                    NodeAction::SendAck { to, sequence, piggyback: combined }
                }
                NodeAction::SendPingReq { relay, target, sequence, piggyback } => {
                    let registry_entries = self.registry.take_pending(8);
                    let metadata_entries = self.metadata.take_pending(4);
                    let combined = pack_combined_piggyback(piggyback, registry_entries, metadata_entries);
                    NodeAction::SendPingReq { relay, target, sequence, piggyback: combined }
                }
                NodeAction::ForwardAck { to, target, sequence, piggyback } => {
                    let registry_entries = self.registry.take_pending(8);
                    let metadata_entries = self.metadata.take_pending(4);
                    let combined = pack_combined_piggyback(piggyback, registry_entries, metadata_entries);
                    NodeAction::ForwardAck { to, target, sequence, piggyback: combined }
                }
                other => other,
            })
            .collect()
    }

    /// Merge registry entries received from a piggyback payload.
    fn merge_registry_entries(&mut self, entries: Vec<RegistryEntry>) {
        if !entries.is_empty() {
            self.registry.merge_batch(entries, self.cluster_size());
        }
    }

    /// Merge metadata entries received from a piggyback payload.
    fn merge_metadata_entries(&mut self, entries: Vec<NodeMetadataEntry>) {
        if !entries.is_empty() {
            self.metadata.apply_incoming(entries, self.cluster_size());
        }
    }
}

/// Result of resolving an actor's location.
#[derive(Debug)]
pub enum ResolveResult {
    /// Found in cache or local directory.
    Cached(NodeId),
    /// Need to do a Kademlia FIND_VALUE — here are the closest known nodes.
    NeedsLookup { closest_nodes: Vec<NodeId> },
    /// No nodes known at all.
    NotFound,
}
