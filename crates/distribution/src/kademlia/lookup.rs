//! Kademlia iterative FIND_NODE lookup.
//!
//! A state machine that drives the iterative lookup process:
//! 1. Start with the α closest nodes from the local routing table.
//! 2. Query them in parallel (caller dispatches the actual I/O).
//! 3. Incorporate responses (closer nodes discovered).
//! 4. Repeat until the k closest nodes have all been queried or max rounds exceeded.
//!
//! The lookup does NOT do I/O — it produces `LookupAction`s that the caller
//! translates into real network requests.

use std::collections::{HashMap, HashSet};

use crate::types::NodeId;
use super::routing_table::{RoutingTable, K};

/// Concurrency parameter — how many queries to issue in parallel per round.
pub const ALPHA: usize = 3;

/// Maximum lookup rounds before termination.
const MAX_ROUNDS: usize = 20;

/// Actions produced by the lookup state machine.
#[derive(Debug, Clone)]
pub enum LookupAction {
    /// Send a FIND_NODE query to this node.
    Query { node_id: NodeId },
    /// The lookup is complete — here are the k closest nodes found.
    Done { closest: Vec<NodeId> },
}

/// State of a single iterative FIND_NODE lookup.
pub struct NodeLookup {
    target: NodeId,
    k: usize,
    alpha: usize,
    /// All nodes discovered during the lookup, with their distances.
    known: HashMap<NodeId, [u8; 32]>,
    /// Nodes we've already queried.
    queried: HashSet<NodeId>,
    /// Nodes we've sent queries to but haven't received responses yet.
    pending: HashSet<NodeId>,
    round: usize,
    done: bool,
}

impl NodeLookup {
    /// Start a new lookup for `target` using the local routing table as seeds.
    pub fn start(target: NodeId, routing_table: &RoutingTable) -> (Self, Vec<LookupAction>) {
        Self::start_with_params(target, routing_table, K, ALPHA)
    }

    /// Start with custom k and alpha parameters.
    pub fn start_with_params(
        target: NodeId,
        routing_table: &RoutingTable,
        k: usize,
        alpha: usize,
    ) -> (Self, Vec<LookupAction>) {
        let seeds = routing_table.closest(&target, k);

        let mut known = HashMap::new();
        for entry in &seeds {
            let dist = entry.node_id.xor_distance(&target);
            known.insert(entry.node_id, dist);
        }

        let mut lookup = Self {
            target,
            k,
            alpha,
            known,
            queried: HashSet::new(),
            pending: HashSet::new(),
            round: 0,
            done: false,
        };

        let actions = lookup.next_round();
        (lookup, actions)
    }

    /// Feed a response from a queried node. Returns new actions (more queries, or done).
    pub fn handle_response(
        &mut self,
        from: NodeId,
        closer_nodes: Vec<NodeId>,
    ) -> Vec<LookupAction> {
        if self.done {
            return vec![self.done_action()];
        }

        self.pending.remove(&from);

        // Incorporate newly discovered nodes
        for node_id in closer_nodes {
            if node_id == self.target {
                // Skip the target itself (it's what we're looking for)
                continue;
            }
            self.known.entry(node_id).or_insert_with(|| {
                node_id.xor_distance(&self.target)
            });
        }

        // If no more pending queries, start the next round
        if self.pending.is_empty() {
            return self.next_round();
        }

        Vec::new()
    }

    /// Handle a timeout or failure for a queried node.
    pub fn handle_failure(&mut self, node_id: NodeId) -> Vec<LookupAction> {
        self.pending.remove(&node_id);
        if self.pending.is_empty() && !self.done {
            return self.next_round();
        }
        Vec::new()
    }

    /// Is the lookup complete?
    pub fn is_done(&self) -> bool {
        self.done
    }

    fn next_round(&mut self) -> Vec<LookupAction> {
        self.round += 1;

        if self.round > MAX_ROUNDS {
            self.done = true;
            return vec![self.done_action()];
        }

        // Find the closest unqueried nodes
        let mut candidates: Vec<_> = self
            .known
            .iter()
            .filter(|(id, _)| !self.queried.contains(id))
            .map(|(id, dist)| (*id, *dist))
            .collect();

        candidates.sort_by(|a, b| a.1.cmp(&b.1));
        candidates.truncate(self.alpha);

        if candidates.is_empty() {
            // No more nodes to query — we're done
            self.done = true;
            return vec![self.done_action()];
        }

        // Check termination: if all k closest nodes have been queried
        let all_known_sorted = self.k_closest();
        let all_k_queried = all_known_sorted
            .iter()
            .take(self.k)
            .all(|id| self.queried.contains(id));

        if all_k_queried && !all_known_sorted.is_empty() {
            self.done = true;
            return vec![self.done_action()];
        }

        let mut actions = Vec::new();
        for (node_id, _) in candidates {
            self.queried.insert(node_id);
            self.pending.insert(node_id);
            actions.push(LookupAction::Query { node_id });
        }

        actions
    }

    fn k_closest(&self) -> Vec<NodeId> {
        let mut sorted: Vec<_> = self
            .known
            .iter()
            .map(|(id, dist)| (*id, *dist))
            .collect();
        sorted.sort_by(|a, b| a.1.cmp(&b.1));
        sorted.truncate(self.k);
        sorted.into_iter().map(|(id, _)| id).collect()
    }

    fn done_action(&self) -> LookupAction {
        LookupAction::Done {
            closest: self.k_closest(),
        }
    }
}
