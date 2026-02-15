//! Cluster Registry — gossip-propagated naming via LWW-Register CRDT.
//!
//! Maps human-readable names to `(ActorAddress, NodeId)` pairs, propagated
//! through SWIM gossip piggyback. Uses last-writer-wins semantics with
//! tie-breaking on (timestamp, generation, node_id).

use std::collections::{HashMap, VecDeque};

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;

use crate::types::NodeId;

// ─── Configuration ──────────────────────────────────────────────────────────

/// Configuration for the cluster registry.
#[derive(Clone)]
pub struct RegistryConfig {
    /// Maximum number of events to buffer before dropping old ones.
    pub max_events: usize,
    /// How long (in ticks) a tombstone is retained before GC.
    pub tombstone_ttl: u64,
    /// How often (in ticks) to run garbage collection.
    pub gc_interval: u64,
    /// Dissemination multiplier (Λ) — same role as in SWIM dissemination.
    pub dissemination_lambda: usize,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            max_events: 256,
            tombstone_ttl: 3600,
            gc_interval: 1000,
            dissemination_lambda: 3,
        }
    }
}

// ─── Wire types ─────────────────────────────────────────────────────────────

/// A single registry entry — the unit of replication.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegistryEntry {
    pub name: String,
    pub actor_addr: ActorAddress,
    pub node_id: NodeId,
    /// Logical timestamp (monotonically increasing per-registry).
    pub timestamp: u64,
    /// Generation counter for the same name (disambiguates re-registrations).
    pub generation: u64,
    /// If true, this entry is a tombstone (name was unregistered).
    pub tombstone: bool,
}

/// Combined piggyback payload: membership bytes + registry entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiggybackPayload {
    /// Raw SWIM membership piggyback bytes (opaque to registry).
    pub membership: Vec<u8>,
    /// Registry entries to disseminate.
    pub registry: Vec<RegistryEntry>,
}

// ─── Events ─────────────────────────────────────────────────────────────────

/// Events emitted when the registry changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryEvent {
    Registered {
        name: String,
        actor_addr: ActorAddress,
        node_id: NodeId,
    },
    Unregistered {
        name: String,
        previous_addr: ActorAddress,
    },
}

// ─── Dissemination entry ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct DisseminationEntry {
    entry: RegistryEntry,
    remaining: usize,
}

// ─── ClusterRegistry ────────────────────────────────────────────────────────

/// CRDT-based cluster registry with LWW semantics and gossip dissemination.
pub struct ClusterRegistry {
    /// Current state: name → latest entry.
    entries: HashMap<String, RegistryEntry>,
    /// Pending entries to disseminate via piggyback.
    dissemination: Vec<DisseminationEntry>,
    /// Monotonic logical clock for this node's writes.
    clock: u64,
    /// Buffered events for consumers.
    events: VecDeque<RegistryEvent>,
    config: RegistryConfig,
    tick_count: u64,
}

impl ClusterRegistry {
    pub fn new(config: RegistryConfig) -> Self {
        Self {
            entries: HashMap::new(),
            dissemination: Vec::new(),
            clock: 0,
            events: VecDeque::new(),
            config,
            tick_count: 0,
        }
    }

    /// Register a name → actor binding from the local node.
    pub fn register(&mut self, name: String, actor_addr: ActorAddress, node_id: NodeId, cluster_size: usize) {
        self.clock += 1;
        let generation = self.next_generation(&name);
        let entry = RegistryEntry {
            name,
            actor_addr,
            node_id,
            timestamp: self.clock,
            generation,
            tombstone: false,
        };
        self.merge_and_enqueue(entry, cluster_size);
    }

    /// Unregister a name (create a tombstone).
    pub fn unregister(&mut self, name: &str, node_id: NodeId, cluster_size: usize) {
        self.clock += 1;
        let generation = self.next_generation(name);
        // Use the existing actor_addr if present, otherwise a zero address.
        let actor_addr = self.entries
            .get(name)
            .map(|e| e.actor_addr)
            .unwrap_or(ActorAddress([0; 32]));
        let entry = RegistryEntry {
            name: name.to_string(),
            actor_addr,
            node_id,
            timestamp: self.clock,
            generation,
            tombstone: true,
        };
        self.merge_and_enqueue(entry, cluster_size);
    }

    /// Resolve a name to its current (ActorAddress, NodeId), or None if
    /// not registered or tombstoned.
    pub fn resolve(&self, name: &str) -> Option<(ActorAddress, NodeId)> {
        self.entries.get(name).and_then(|e| {
            if e.tombstone {
                None
            } else {
                Some((e.actor_addr, e.node_id))
            }
        })
    }

    /// Merge a single remote entry. Returns true if state changed.
    pub fn merge(&mut self, remote: RegistryEntry) -> bool {
        if let Some(existing) = self.entries.get(&remote.name) {
            if !lww_wins(&remote, existing) {
                return false;
            }
        }

        let changed = match self.entries.get(&remote.name) {
            Some(existing) => existing != &remote,
            None => true,
        };

        if changed {
            self.emit_event(&remote);
            // Advance clock to stay ahead of remote timestamps.
            if remote.timestamp >= self.clock {
                self.clock = remote.timestamp + 1;
            }
        }

        self.entries.insert(remote.name.clone(), remote);
        changed
    }

    /// Merge a batch of entries received from gossip.
    /// Changed entries are re-enqueued for further dissemination.
    pub fn merge_batch(&mut self, entries: Vec<RegistryEntry>, cluster_size: usize) {
        for entry in entries {
            if self.merge(entry.clone()) {
                self.enqueue(entry, cluster_size);
            }
        }
    }

    /// Take pending entries for piggyback, up to `max_count`.
    pub fn take_pending(&mut self, max_count: usize) -> Vec<RegistryEntry> {
        let count = max_count.min(self.dissemination.len());
        let mut result = Vec::with_capacity(count);

        for entry in self.dissemination.iter_mut().take(count) {
            result.push(entry.entry.clone());
            entry.remaining = entry.remaining.saturating_sub(1);
        }

        // Evict exhausted entries.
        self.dissemination.retain(|e| e.remaining > 0);

        result
    }

    /// Tombstone all entries owned by a dead node.
    pub fn tombstone_node(&mut self, dead_node_id: NodeId, cluster_size: usize) {
        let owned: Vec<String> = self.entries
            .iter()
            .filter(|(_, e)| e.node_id == dead_node_id && !e.tombstone)
            .map(|(name, _)| name.clone())
            .collect();

        for name in owned {
            self.clock += 1;
            let generation = self.next_generation(&name);
            let actor_addr = self.entries[&name].actor_addr;
            let entry = RegistryEntry {
                name,
                actor_addr,
                node_id: dead_node_id,
                timestamp: self.clock,
                generation,
                tombstone: true,
            };
            self.merge_and_enqueue(entry, cluster_size);
        }
    }

    /// Re-enqueue all entries for dissemination (anti-entropy on membership change).
    ///
    /// Called when a previously-dead node comes back alive, ensuring that
    /// registry state accumulated during a partition is gossiped to the
    /// recovering node.
    pub fn re_disseminate_all(&mut self, cluster_size: usize) {
        for entry in self.entries.values().cloned().collect::<Vec<_>>() {
            self.enqueue(entry, cluster_size);
        }
    }

    /// Periodic GC: remove tombstones past TTL with exhausted dissemination budgets.
    pub fn gc_tick(&mut self) {
        self.tick_count += 1;
        if self.tick_count % self.config.gc_interval != 0 {
            return;
        }

        let ttl = self.config.tombstone_ttl;
        let clock = self.clock;
        // Names still being disseminated — don't GC those.
        let pending_names: std::collections::HashSet<String> = self.dissemination
            .iter()
            .map(|e| e.entry.name.clone())
            .collect();

        self.entries.retain(|name, entry| {
            if entry.tombstone && !pending_names.contains(name) {
                // Remove if old enough.
                let age = clock.saturating_sub(entry.timestamp);
                age < ttl
            } else {
                true
            }
        });
    }

    /// Drain buffered events.
    pub fn drain_events(&mut self) -> Vec<RegistryEvent> {
        self.events.drain(..).collect()
    }

    /// Number of registry entries (including tombstones).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Number of tombstones.
    pub fn tombstone_count(&self) -> usize {
        self.entries.values().filter(|e| e.tombstone).count()
    }

    /// Iterate all entries (for snapshot).
    pub fn entries(&self) -> impl Iterator<Item = &RegistryEntry> {
        self.entries.values()
    }

    // ─── Internal ───────────────────────────────────────────────────────

    fn next_generation(&self, name: &str) -> u64 {
        self.entries
            .get(name)
            .map(|e| e.generation + 1)
            .unwrap_or(1)
    }

    fn transmit_budget(&self, cluster_size: usize) -> usize {
        let n = cluster_size.max(2) as f64;
        let log_n = n.log2().ceil() as usize;
        self.config.dissemination_lambda * log_n.max(1)
    }

    fn enqueue(&mut self, entry: RegistryEntry, cluster_size: usize) {
        let budget = self.transmit_budget(cluster_size);

        // Replace existing entry for same name if present.
        if let Some(existing) = self.dissemination.iter_mut().find(|e| e.entry.name == entry.name) {
            existing.entry = entry;
            existing.remaining = budget;
            return;
        }

        self.dissemination.push(DisseminationEntry {
            entry,
            remaining: budget,
        });
    }

    fn merge_and_enqueue(&mut self, entry: RegistryEntry, cluster_size: usize) {
        let merged = self.merge(entry.clone());
        if merged {
            self.enqueue(entry, cluster_size);
        }
    }

    fn emit_event(&mut self, entry: &RegistryEntry) {
        let event = if entry.tombstone {
            RegistryEvent::Unregistered {
                name: entry.name.clone(),
                previous_addr: entry.actor_addr,
            }
        } else {
            RegistryEvent::Registered {
                name: entry.name.clone(),
                actor_addr: entry.actor_addr,
                node_id: entry.node_id,
            }
        };
        self.events.push_back(event);
        while self.events.len() > self.config.max_events {
            self.events.pop_front();
        }
    }
}

// ─── LWW conflict resolution ───────────────────────────────────────────────

/// Returns true if `incoming` wins over `existing` under LWW rules:
/// higher timestamp > higher generation > higher node_id (byte-level).
fn lww_wins(incoming: &RegistryEntry, existing: &RegistryEntry) -> bool {
    if incoming.timestamp != existing.timestamp {
        return incoming.timestamp > existing.timestamp;
    }
    if incoming.generation != existing.generation {
        return incoming.generation > existing.generation;
    }
    incoming.node_id.0 > existing.node_id.0
}

// ─── Piggyback pack/unpack ──────────────────────────────────────────────────

/// Combine membership piggyback bytes and registry entries into a single payload.
pub fn pack_combined_piggyback(membership: Vec<u8>, registry: Vec<RegistryEntry>) -> Vec<u8> {
    let payload = PiggybackPayload { membership, registry };
    serde_json::to_vec(&payload).unwrap_or_default()
}

/// Split a combined piggyback payload into membership bytes and registry entries.
/// If deserialization fails, treats the entire blob as membership bytes (backwards compat).
pub fn unpack_combined_piggyback(bytes: &[u8]) -> (Vec<u8>, Vec<RegistryEntry>) {
    if bytes.is_empty() {
        return (Vec::new(), Vec::new());
    }
    match serde_json::from_slice::<PiggybackPayload>(bytes) {
        Ok(payload) => (payload.membership, payload.registry),
        Err(_) => (bytes.to_vec(), Vec::new()),
    }
}
