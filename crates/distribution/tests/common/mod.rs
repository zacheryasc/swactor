//! Shared test harness for N-node `DistributedNode` tests.
//!
//! `TestCluster` makes sender-misattribution structurally impossible by
//! tagging every response with the responder's index, mirroring the
//! simulation crate's `deliver_actions_tagged_with_net`.
#![allow(dead_code)]

use distribution::node::{DistributedNode, DistributedNodeConfig};
use distribution::registry::RegistryConfig;
use distribution::swim::node::NodeAction;
use distribution::swim::probe::SwimConfig;
use distribution::types::NodeId;
use std::ops::{Index, IndexMut};

/// Default test config shared across all integration tests.
pub fn test_config() -> DistributedNodeConfig {
    DistributedNodeConfig {
        swim: SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 1,
            suspicion_timeout: 5,
            dead_reprobe_interval: 0,
            ..SwimConfig::default()
        },
        cache_capacity: 100,
        republish_interval: 50,
        registry: RegistryConfig::default(),
        metadata_lambda: 3,
    }
}

/// Deliver actions to the appropriate target nodes, returning responses
/// tagged with the responder's index. Nodes whose index appears in
/// `excluded` silently drop messages (simulates death / network loss).
fn deliver_actions_tagged(
    actions: &[NodeAction],
    sender_id: NodeId,
    ids: &[NodeId],
    nodes: &mut [DistributedNode],
    excluded: &[usize],
) -> Vec<(usize, Vec<NodeAction>)> {
    let mut tagged: Vec<(usize, Vec<NodeAction>)> = Vec::new();

    for action in actions {
        match action {
            NodeAction::SendPing {
                to,
                sequence,
                piggyback,
                ..
            } => {
                if let Some(idx) = ids.iter().position(|id| id == to) {
                    if !excluded.contains(&idx) {
                        let resp = nodes[idx].handle_ping(sender_id, *sequence, piggyback);
                        if !resp.is_empty() {
                            tagged.push((idx, resp));
                        }
                    }
                }
            }
            NodeAction::SendAck {
                to,
                sequence,
                piggyback,
                ..
            } => {
                if let Some(idx) = ids.iter().position(|id| id == to) {
                    if !excluded.contains(&idx) {
                        let resp = nodes[idx].handle_ack(sender_id, *sequence, piggyback);
                        if !resp.is_empty() {
                            tagged.push((idx, resp));
                        }
                    }
                }
            }
            NodeAction::SendJoinResponse { to, members, .. } => {
                if let Some(idx) = ids.iter().position(|id| id == to) {
                    if !excluded.contains(&idx) {
                        let resp = nodes[idx].handle_join_response(members.clone());
                        if !resp.is_empty() {
                            tagged.push((idx, resp));
                        }
                    }
                }
            }
            NodeAction::SendPingReq {
                relay,
                target,
                sequence,
                piggyback,
                ..
            } => {
                if let Some(idx) = ids.iter().position(|id| id == relay) {
                    if !excluded.contains(&idx) {
                        let resp =
                            nodes[idx].handle_ping_req(sender_id, *target, *sequence, piggyback);
                        if !resp.is_empty() {
                            tagged.push((idx, resp));
                        }
                    }
                }
            }
            NodeAction::ForwardAck { to, target, sequence, piggyback } => {
                if let Some(idx) = ids.iter().position(|id| id == to) {
                    if !excluded.contains(&idx) {
                        let resp = nodes[idx].handle_indirect_ack(*target, *sequence, piggyback);
                        if !resp.is_empty() {
                            tagged.push((idx, resp));
                        }
                    }
                }
            }
            NodeAction::MembershipChanged { .. } => {}
        }
    }

    tagged
}

/// An N-node test cluster with correct-by-construction message delivery.
pub struct TestCluster {
    ids: Vec<NodeId>,
    nodes: Vec<DistributedNode>,
}

impl TestCluster {
    /// Create an N-node cluster using `test_config()`. Nodes 1..N join via node 0.
    pub fn new(n: usize) -> Self {
        Self::with_config(n, test_config())
    }

    /// Create an N-node cluster with a custom config. Nodes 1..N join via node 0.
    pub fn with_config(n: usize, config: DistributedNodeConfig) -> Self {
        assert!(n >= 2, "TestCluster requires at least 2 nodes");

        let mut nodes: Vec<DistributedNode> =
            (0..n).map(|_| DistributedNode::new(config.clone())).collect();
        let ids: Vec<NodeId> = nodes.iter().map(|node| node.node_id()).collect();

        // All nodes join through node 0 (seed).
        for i in 1..n {
            let actions = nodes[0].handle_join_request(ids[i]);
            deliver_actions_tagged(&actions, ids[0], &ids, &mut nodes, &[]);
        }

        Self { ids, nodes }
    }

    /// Get the `NodeId` for the node at `idx`.
    pub fn node_id(&self, idx: usize) -> NodeId {
        self.ids[idx]
    }

    /// Run one gossip round: tick all live nodes, deliver with tagged
    /// responses, deliver responses back. Excluded indices are skipped.
    fn gossip_round_excluding(&mut self, excluded: &[usize]) {
        let n = self.nodes.len();

        // 1. Tick all live nodes, collect actions.
        let mut all_actions: Vec<(usize, Vec<NodeAction>)> = Vec::new();
        for idx in 0..n {
            if excluded.contains(&idx) {
                continue;
            }
            let actions = self.nodes[idx].tick();
            if !actions.is_empty() {
                all_actions.push((idx, actions));
            }
        }

        // 2. Deliver each sender's actions → get tagged responses.
        // 3. Deliver responses back using the responder's identity.
        for (sender_idx, actions) in all_actions {
            let tagged_responses = deliver_actions_tagged(
                &actions,
                self.ids[sender_idx],
                &self.ids,
                &mut self.nodes,
                excluded,
            );
            for (responder_idx, response_actions) in tagged_responses {
                deliver_actions_tagged(
                    &response_actions,
                    self.ids[responder_idx],
                    &self.ids,
                    &mut self.nodes,
                    excluded,
                );
            }
        }
    }

    /// Run `n` gossip rounds with all nodes participating.
    pub fn gossip_rounds(&mut self, n: usize) {
        for _ in 0..n {
            self.gossip_round_excluding(&[]);
        }
    }

    /// Run `n` gossip rounds; dead nodes neither tick nor receive.
    pub fn gossip_rounds_excluding(&mut self, dead: &[usize], n: usize) {
        for _ in 0..n {
            self.gossip_round_excluding(dead);
        }
    }
}

impl Index<usize> for TestCluster {
    type Output = DistributedNode;
    fn index(&self, idx: usize) -> &Self::Output {
        &self.nodes[idx]
    }
}

impl IndexMut<usize> for TestCluster {
    fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
        &mut self.nodes[idx]
    }
}

#[cfg(feature = "iroh")]
pub mod iroh;
