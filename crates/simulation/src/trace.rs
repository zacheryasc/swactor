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
