use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;

use crate::protocol::VersionedValue;

// ── Shared handles ───────────────────────────────────────────────────────

/// Shared, append-only event log.
pub type EventLog = Arc<Mutex<Vec<GossipEvent>>>;

/// Shared tick counter — the simulation harness increments this.
pub type TickCounter = Arc<AtomicU64>;

/// Maps actor addresses to human-readable names like `"node-0"`.
pub type NameRegistry = Arc<Mutex<HashMap<ActorAddress, String>>>;

// ── TraceContext ─────────────────────────────────────────────────────────

/// Bundles the three shared handles needed for tracing into one value.
pub struct TraceContext {
    pub event_log: EventLog,
    pub tick_counter: TickCounter,
    pub name_registry: NameRegistry,
}

impl TraceContext {
    pub fn current_tick(&self) -> u64 {
        self.tick_counter.load(Ordering::Relaxed)
    }

    pub fn resolve_name(&self, addr: ActorAddress) -> String {
        self.name_registry
            .lock()
            .unwrap()
            .get(&addr)
            .cloned()
            .unwrap_or_else(|| format!("{:?}", &addr.0[..4]))
    }

    pub fn record_event(&self, event: GossipEvent) {
        self.event_log.lock().unwrap().push(event);
    }
}

// ── Event types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipEvent {
    pub tick: u64,
    pub node_name: String,
    pub node_addr: ActorAddress,
    pub thread_name: Option<String>,
    pub kind: GossipEventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipEventKind {
    /// A local `Set { key, .. }` was processed.
    LocalSet { key: String },
    /// `DoGossipRound` chose a peer and sent a Push.
    GossipRoundStarted { target_name: String },
    /// `DoGossipRound` had no peers.
    GossipRoundNoPeers,
    /// Received a Push from another node.
    PushReceived {
        from_name: String,
        keys_updated: usize,
    },
    /// Received a Query.
    QueryReceived { key: String },
    /// A peer was added.
    PeerAdded { peer_name: String },
    /// A peer was removed.
    PeerRemoved { peer_name: String },
    /// Full state snapshot (requested via `TakeSnapshot`).
    StateSnapshot { snapshot: NodeSnapshot },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSnapshot {
    pub entries: HashMap<String, VersionedValue>,
    pub peer_count: usize,
}

// ── Simulation trace (complete run output) ───────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationTrace {
    pub name: String,
    pub node_names: Vec<String>,
    pub node_addrs: Vec<ActorAddress>,
    pub topology_edges: Vec<(String, String)>,
    pub events: Vec<GossipEvent>,
    pub snapshots_per_round: Vec<Vec<(String, NodeSnapshot)>>,
    pub num_rounds: usize,
    pub total_keys: usize,
}
