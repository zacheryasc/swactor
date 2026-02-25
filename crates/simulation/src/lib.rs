pub mod runner;
pub mod topology;
pub mod properties;

#[cfg(feature = "distribution")]
#[path = "distribution_mod.rs"]
pub mod distribution;

#[cfg(feature = "gossip")]
pub mod gossip;

#[cfg(feature = "dashboard")]
pub mod dashboard;

// ─── Generic Simulation Node Traits ─────────────────────────────────────────

/// A message produced by a simulated node.
pub trait SimMessage {
    type NodeId;
    /// The target node for this message, if any.
    /// `None` means the message is a local notification (no delivery needed).
    fn target(&self) -> Option<&Self::NodeId>;
}

/// A simulated protocol node.
pub trait SimNode: Sized {
    type Config: Clone;
    type NodeId: Clone + Eq + std::hash::Hash + std::fmt::Debug;
    type Message: SimMessage<NodeId = Self::NodeId>;
    type Snapshot: serde::Serialize;
    type EventKind: serde::Serialize;

    fn new(config: Self::Config) -> Self;
    fn node_id(&self) -> Self::NodeId;
    fn tick(&mut self) -> Vec<Self::Message>;
    fn receive(&mut self, from: Self::NodeId, msg: Self::Message) -> Vec<Self::Message>;
    fn snapshot(&self) -> Self::Snapshot;
}

// ─── Simulation Configuration ───────────────────────────────────────────────

use crate::topology::Topology;

/// Generic simulation configuration (protocol-agnostic).
#[derive(Debug, Clone)]
pub struct SimConfig {
    pub name: String,
    pub topology: Topology,
    pub num_nodes: usize,
    pub num_rounds: usize,
    pub ticks_per_round: usize,
    /// If `Some(r)`, cross-partition links are added after round `r`.
    pub heal_after_round: Option<usize>,
    /// Number of worker threads: 1 = deterministic single-threaded.
    pub num_threads: usize,
}

// ─── Trace Types ────────────────────────────────────────────────────────────

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Shared tick counter — the simulation harness increments this.
pub type TickCounter = Arc<AtomicU64>;

/// A single simulation event, generic over the event kind `K`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize",
    deserialize = "K: serde::de::DeserializeOwned"
))]
pub struct Event<K> {
    pub tick: u64,
    pub node_name: String,
    pub kind: K,
}

/// Complete output of a simulation run, generic over event kind `K` and snapshot type `S`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, S: Serialize",
    deserialize = "K: serde::de::DeserializeOwned, S: serde::de::DeserializeOwned"
))]
pub struct SimulationTrace<K, S> {
    pub name: String,
    /// Discriminator for dashboard rendering ("gossip" or "distribution").
    #[serde(default)]
    pub trace_type: String,
    pub node_names: Vec<String>,
    pub topology_edges: Vec<(String, String)>,
    pub events: Vec<Event<K>>,
    pub snapshots_per_round: Vec<Vec<(String, S)>>,
    pub num_rounds: usize,
}
