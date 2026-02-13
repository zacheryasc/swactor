use serde::{Deserialize, Serialize};

/// Events emitted during a distribution simulation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DistributionEventKind {
    Joined { seed_addr: String },
    MembershipChanged { target: String, new_state: String },
    PingSent { target: String },
    AckReceived { from: String },
    ActorRegistered { actor_id: String },
    ActorStored { actor_id: String, on_node: String },
    ActorResolved { actor_id: String, found_on: String },
    ActorResolveFailed { actor_id: String, reason: String },
    NameRegistered { name: String, node_idx: usize },
    NameUnregistered { name: String, node_idx: usize },
    NameResolved { name: String, result: String },
    NodeKilled,
    NodeRevived,
}

/// Per-node snapshot for distribution simulation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionSnapshot {
    pub member_count: usize,
    pub routing_table_size: usize,
    pub directory_entry_count: usize,
    pub cache_size: usize,
    pub repair_queue_size: usize,
    pub registry_size: usize,
    pub registry_tombstone_count: usize,
    pub is_alive: bool,
}
