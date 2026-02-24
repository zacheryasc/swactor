//! Generic simulation runner — network state and message delivery.
//!
//! Provides `NetworkState` for simulating partitions, drops, NAT/firewall
//! topology, and relay penalties. Can be used with any protocol that
//! implements `SimNode`.

use std::collections::{HashMap, HashSet};

/// Network location of a simulated node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeLocation {
    /// Publicly reachable (e.g. cloud VPS). Can receive inbound from anyone.
    Public,
    /// Behind NAT. Can only receive inbound from same LAN group or via relay.
    Nat { group: String },
    /// Completely firewalled — no inbound or outbound.
    Firewalled,
}

/// Network topology describing NAT/firewall/relay placement.
#[derive(Debug, Clone)]
pub struct NetworkTopology {
    /// Per-node location (indexed by node_idx). Length must equal num_nodes.
    pub locations: Vec<NodeLocation>,
    /// Node indices that act as relay forwarders for cross-NAT traffic.
    pub relay_nodes: Vec<usize>,
}

/// A network partition between two sets of nodes.
#[derive(Debug, Clone)]
pub struct Partition {
    pub side_a: Vec<usize>,
    pub side_b: Vec<usize>,
    /// If true, A→B is blocked but B→A works (asymmetric).
    pub asymmetric: bool,
}

/// Schedule entry for network faults.
#[derive(Debug, Clone)]
pub enum NetworkFault {
    /// Introduce a partition at the given round.
    Partition { round: usize, partition: Partition },
    /// Heal a partition at the given round (restores full connectivity).
    Heal { round: usize },
    /// Set message drop rate (0.0 = no drops, 1.0 = drop all).
    SetDropRate { round: usize, rate: f64 },
    /// Per-link drop rate. rate=0.0 clears the fault.
    LinkFault { round: usize, from: usize, to: usize, rate: f64, bidirectional: bool },
    /// Relay penalty — extra drop probability for relay-routed messages.
    SetRelayPenalty { round: usize, rate: f64 },
}

impl NetworkFault {
    /// The round at which this fault is scheduled.
    pub fn round(&self) -> usize {
        match self {
            NetworkFault::Partition { round, .. } => *round,
            NetworkFault::Heal { round } => *round,
            NetworkFault::SetDropRate { round, .. } => *round,
            NetworkFault::LinkFault { round, .. } => *round,
            NetworkFault::SetRelayPenalty { round, .. } => *round,
        }
    }
}

/// Tracks active network state during simulation.
pub struct NetworkState {
    /// Set of (from_idx, to_idx) pairs where messages are blocked.
    blocked: HashSet<(usize, usize)>,
    /// Probability of dropping a message [0.0, 1.0].
    drop_rate: f64,
    /// Simple counter-based deterministic "random" for drop decisions.
    drop_counter: u64,
    /// Optional NAT/firewall topology.
    topology: Option<NetworkTopology>,
    /// Per-node alive status (indexed by node_idx).
    alive: Vec<bool>,
    /// Per-link drop rates (from, to) -> rate.
    link_drop_rates: HashMap<(usize, usize), f64>,
    /// Extra drop probability for relay-routed messages.
    relay_penalty: f64,
}

impl NetworkState {
    pub fn new() -> Self {
        Self {
            blocked: HashSet::new(),
            drop_rate: 0.0,
            drop_counter: 0x853c49e6748fea9b,
            topology: None,
            alive: Vec::new(),
            link_drop_rates: HashMap::new(),
            relay_penalty: 0.0,
        }
    }

    pub fn new_with_topology(topology: Option<NetworkTopology>, num_nodes: usize) -> Self {
        Self {
            blocked: HashSet::new(),
            drop_rate: 0.0,
            drop_counter: 0x853c49e6748fea9b,
            topology,
            alive: vec![true; num_nodes],
            link_drop_rates: HashMap::new(),
            relay_penalty: 0.0,
        }
    }

    pub fn set_alive(&mut self, idx: usize, alive: bool) {
        if idx < self.alive.len() {
            self.alive[idx] = alive;
        }
    }

    pub fn apply_fault(&mut self, fault: &NetworkFault, num_nodes: usize) {
        match fault {
            NetworkFault::Partition { partition, .. } => {
                for &a in &partition.side_a {
                    for &b in &partition.side_b {
                        if a < num_nodes && b < num_nodes {
                            self.blocked.insert((a, b));
                            if !partition.asymmetric {
                                self.blocked.insert((b, a));
                            }
                        }
                    }
                }
            }
            NetworkFault::Heal { .. } => {
                self.blocked.clear();
            }
            NetworkFault::SetDropRate { rate, .. } => {
                self.drop_rate = rate.clamp(0.0, 1.0);
            }
            NetworkFault::LinkFault { from, to, rate, bidirectional, .. } => {
                let rate = rate.clamp(0.0, 1.0);
                if rate == 0.0 {
                    self.link_drop_rates.remove(&(*from, *to));
                    if *bidirectional {
                        self.link_drop_rates.remove(&(*to, *from));
                    }
                } else {
                    self.link_drop_rates.insert((*from, *to), rate);
                    if *bidirectional {
                        self.link_drop_rates.insert((*to, *from), rate);
                    }
                }
            }
            NetworkFault::SetRelayPenalty { rate, .. } => {
                self.relay_penalty = rate.clamp(0.0, 1.0);
            }
        }
    }

    /// Check if `from` can directly initiate a connection to `to`.
    fn directly_reachable(&self, from: usize, to: usize) -> bool {
        let topo = match &self.topology {
            Some(t) => t,
            None => return true,
        };
        if from >= topo.locations.len() || to >= topo.locations.len() {
            return true;
        }
        match (&topo.locations[from], &topo.locations[to]) {
            (_, NodeLocation::Firewalled) => false,
            (NodeLocation::Firewalled, _) => false,
            (_, NodeLocation::Public) => true,
            (NodeLocation::Public, NodeLocation::Nat { .. }) => false,
            (NodeLocation::Nat { group: g1 }, NodeLocation::Nat { group: g2 }) => g1 == g2,
        }
    }

    /// Check if two nodes can communicate (bidirectional once established).
    fn can_reach(&self, from: usize, to: usize) -> bool {
        let topo = match &self.topology {
            Some(t) => t,
            None => return true,
        };
        if self.directly_reachable(from, to) || self.directly_reachable(to, from) {
            return true;
        }
        for &r in &topo.relay_nodes {
            if r == from || r == to {
                continue;
            }
            if !self.alive.get(r).copied().unwrap_or(false) {
                continue;
            }
            let from_reaches_r = self.directly_reachable(from, r) || self.directly_reachable(r, from);
            let to_reaches_r = self.directly_reachable(to, r) || self.directly_reachable(r, to);
            if from_reaches_r && to_reaches_r {
                return true;
            }
        }
        false
    }

    /// Returns true when neither direction is directly reachable but a relay path exists.
    fn requires_relay(&self, from: usize, to: usize) -> bool {
        if self.topology.is_none() {
            return false;
        }
        if self.directly_reachable(from, to) || self.directly_reachable(to, from) {
            return false;
        }
        self.can_reach(from, to)
    }

    /// Returns true if this message should be delivered.
    pub fn should_deliver(&mut self, from_idx: usize, to_idx: usize) -> bool {
        if self.blocked.contains(&(from_idx, to_idx)) {
            return false;
        }
        if self.topology.is_some() && !self.can_reach(from_idx, to_idx) {
            return false;
        }
        let base_rate = self.link_drop_rates
            .get(&(from_idx, to_idx))
            .copied()
            .unwrap_or(self.drop_rate);
        let effective_rate = if self.relay_penalty > 0.0 && self.requires_relay(from_idx, to_idx) {
            1.0 - (1.0 - base_rate) * (1.0 - self.relay_penalty)
        } else {
            base_rate
        };
        if effective_rate > 0.0 {
            self.drop_counter = self.drop_counter.wrapping_mul(6364136223846793005).wrapping_add(1);
            let r = (self.drop_counter >> 33) as f64 / (u32::MAX as f64);
            if r < effective_rate {
                return false;
            }
        }
        true
    }
}
