//! Integrated SWIM node — composes probe cycle, dissemination, and join protocol.
//!
//! This is the top-level SWIM state machine that a `DistributedNode` will drive.
//! It produces `SwimAction`s that the caller translates into real network I/O.

use crate::identity::hex_encode;
use crate::messages::MembershipUpdate;
use crate::types::{MemberState, NodeId, NodeRecord};

use super::dissemination::{membership_update, DisseminationQueue};
use super::member_list::MemberList;
use super::probe::{SwimAction, SwimConfig, SwimEvent, SwimProbe};

// ─── SwimNode Actions (superset of probe actions) ───────────────────────────

/// Actions produced by the integrated SWIM node.
#[derive(Debug, Clone)]
pub enum NodeAction {
    /// Send a SWIM ping.
    SendPing { to: NodeId, sequence: u64, piggyback: Vec<u8> },
    /// Send an indirect ping request through a relay.
    SendPingReq {
        relay: NodeId,
        target: NodeId,
        sequence: u64,
        piggyback: Vec<u8>,
    },
    /// Send a SWIM ack.
    SendAck { to: NodeId, sequence: u64, piggyback: Vec<u8> },
    /// Forward an indirect ack back to the original prober.
    ForwardAck { to: NodeId, target: NodeId, sequence: u64, piggyback: Vec<u8> },
    /// Send a join response with the current member list.
    SendJoinResponse { to: NodeId, members: Vec<NodeRecord> },
    /// Notification: a node state changed (for wiring into Kademlia).
    MembershipChanged { node_id: NodeId, state: MemberState, incarnation: u64 },
}

// ─── SwimNode ───────────────────────────────────────────────────────────────

pub struct SwimNode {
    members: MemberList,
    probe: SwimProbe,
    dissemination: DisseminationQueue,
    /// Maximum piggybacked updates per message.
    max_piggyback: usize,
    /// PingReqs we forwarded: (requester, target, sequence).
    /// When we receive an ack matching (target, sequence), forward it to requester.
    pending_relays: Vec<(NodeId, NodeId, u64)>,
}

impl SwimNode {
    pub fn new(self_id: NodeId, config: SwimConfig) -> Self {
        Self {
            members: MemberList::new(self_id),
            probe: SwimProbe::new(config),
            dissemination: DisseminationQueue::new(3), // Λ = 3
            max_piggyback: 8,
            pending_relays: Vec::new(),
        }
    }

    pub fn self_id(&self) -> NodeId {
        self.members.self_id()
    }

    pub fn members(&self) -> &MemberList {
        &self.members
    }

    /// Recent probe targets from the SWIM probe cycle.
    pub fn recent_probe_targets(&self) -> &std::collections::VecDeque<NodeId> {
        self.probe.recent_probe_targets()
    }

    /// Process a tick — drives the probe cycle.
    pub fn tick(&mut self) -> Vec<NodeAction> {
        let probe_actions = self.probe.step(SwimEvent::Tick, &mut self.members);
        self.translate_probe_actions(probe_actions)
    }

    /// Handle a received ping.
    pub fn handle_ping(&mut self, from: NodeId, sequence: u64, piggyback: &[u8]) -> Vec<NodeAction> {
        let mut actions = self.apply_piggyback(piggyback);

        // Ensure the sender is in our member list
        self.members.apply(from, MemberState::Alive, 0);

        // Reply with ack
        let pb = self.dissemination.pack_piggyback(self.max_piggyback);
        actions.push(NodeAction::SendAck {
            to: from,
            sequence,
            piggyback: pb,
        });
        actions
    }

    /// Handle a received ack.
    pub fn handle_ack(&mut self, from: NodeId, sequence: u64, piggyback: &[u8]) -> Vec<NodeAction> {
        let mut actions = self.apply_piggyback(piggyback);
        let probe_actions = self.probe.step(
            SwimEvent::AckReceived { from, sequence },
            &mut self.members,
        );
        actions.extend(self.translate_probe_actions(probe_actions));

        // Check if this ack completes a pending relay (indirect ping path)
        if let Some(pos) = self.pending_relays.iter().position(|(_, t, s)| *t == from && *s == sequence) {
            let (requester, target, seq) = self.pending_relays.remove(pos);
            let pb = self.dissemination.pack_piggyback(self.max_piggyback);
            actions.push(NodeAction::ForwardAck {
                to: requester, target, sequence: seq, piggyback: pb,
            });
        }

        actions
    }

    /// Handle a received indirect ping request.
    pub fn handle_ping_req(
        &mut self,
        from: NodeId,
        target: NodeId,
        sequence: u64,
        piggyback: &[u8],
    ) -> Vec<NodeAction> {
        let mut actions = self.apply_piggyback(piggyback);

        // Record the pending relay so we can forward the ack back
        if self.pending_relays.len() >= 16 {
            self.pending_relays.remove(0);
        }
        self.pending_relays.push((from, target, sequence));

        // Forward a ping to the target on behalf of the requester
        let pb = self.dissemination.pack_piggyback(self.max_piggyback);
        actions.push(NodeAction::SendPing {
            to: target,
            sequence,
            piggyback: pb,
        });
        actions
    }

    /// Handle a received indirect ack (forwarded by a relay node).
    pub fn handle_indirect_ack(&mut self, target: NodeId, sequence: u64, piggyback: &[u8]) -> Vec<NodeAction> {
        let mut actions = self.apply_piggyback(piggyback);
        let probe_actions = self.probe.step(
            SwimEvent::IndirectAckReceived { target, sequence },
            &mut self.members,
        );
        actions.extend(self.translate_probe_actions(probe_actions));
        actions
    }

    /// Handle a join request from a new node.
    pub fn handle_join_request(&mut self, from: NodeId) -> Vec<NodeAction> {
        // Add the new node to our member list
        let changed = self.members.apply(from, MemberState::Alive, 0);
        let mut actions = Vec::new();

        if changed {
            // Enqueue the join for dissemination
            self.dissemination.enqueue(
                membership_update(from, MemberState::Alive, 0),
                self.cluster_size(),
            );
            actions.push(NodeAction::MembershipChanged {
                node_id: from,
                state: MemberState::Alive,
                incarnation: 0,
            });
        }

        // Send the current member list to the joiner (including ourselves)
        let mut members = self.members.snapshot();
        members.push(NodeRecord {
            node_id: self.members.self_id(),
            state: MemberState::Alive,
            incarnation: self.members.self_incarnation(),
        });
        actions.push(NodeAction::SendJoinResponse {
            to: from,
            members,
        });

        actions
    }

    /// Handle a join response (we received the member list from a seed).
    pub fn handle_join_response(&mut self, members: Vec<NodeRecord>) -> Vec<NodeAction> {
        let mut actions = Vec::new();
        for record in members {
            let changed = self.members.apply(
                record.node_id,
                record.state,
                record.incarnation,
            );
            if changed {
                actions.push(NodeAction::MembershipChanged {
                    node_id: record.node_id,
                    state: record.state,
                    incarnation: record.incarnation,
                });
            }
        }
        actions
    }

    /// Announce ourselves as dead (graceful leave).
    pub fn leave(&mut self) -> Vec<NodeAction> {
        self.dissemination.enqueue(
            membership_update(
                self.members.self_id(),
                MemberState::Dead,
                self.members.self_incarnation(),
            ),
            self.cluster_size(),
        );
        Vec::new()
    }

    fn cluster_size(&self) -> usize {
        self.members.alive_count() + 1 // +1 for self
    }

    fn apply_piggyback(&mut self, bytes: &[u8]) -> Vec<NodeAction> {
        let updates = DisseminationQueue::unpack_piggyback(bytes);
        let mut actions = Vec::new();
        for update in updates {
            actions.extend(self.apply_membership_update(update));
        }
        actions
    }

    fn apply_membership_update(&mut self, update: MembershipUpdate) -> Vec<NodeAction> {
        // Check if this is about us
        if update.node_id == self.members.self_id() {
            if update.state == MemberState::Suspect || update.state == MemberState::Dead {
                // Refute: bump incarnation and disseminate
                let new_inc = self.members.refute();
                self.dissemination.enqueue(
                    membership_update(
                        self.members.self_id(),
                        MemberState::Alive,
                        new_inc,
                    ),
                    self.cluster_size(),
                );
            }
            return Vec::new();
        }

        let changed = self.members.apply(
            update.node_id,
            update.state,
            update.incarnation,
        );
        if changed {
            if update.state == MemberState::Alive {
                eprintln!("SWIM: alive {}", &hex_encode(&update.node_id.0)[..8]);
            }
            // Re-disseminate the update
            self.dissemination.enqueue(
                membership_update(update.node_id, update.state, update.incarnation),
                self.cluster_size(),
            );
            vec![NodeAction::MembershipChanged {
                node_id: update.node_id,
                state: update.state,
                incarnation: update.incarnation,
            }]
        } else {
            Vec::new()
        }
    }

    fn translate_probe_actions(&mut self, probe_actions: Vec<SwimAction>) -> Vec<NodeAction> {
        let mut actions = Vec::new();
        for pa in probe_actions {
            match pa {
                SwimAction::SendPing { to, sequence } => {
                    // If the target is suspect or dead, re-enqueue its state
                    // so it piggybacks on this message. This is the key mechanism
                    // for partition-heal recovery: the target learns it was
                    // suspected/declared dead and refutes by bumping its incarnation.
                    if let Some(entry) = self.members.get(&to) {
                        if entry.state == MemberState::Dead || entry.state == MemberState::Suspect {
                            self.dissemination.enqueue(
                                membership_update(to, entry.state, entry.incarnation),
                                self.cluster_size(),
                            );
                        }
                    }
                    let pb = self.dissemination.pack_piggyback(self.max_piggyback);
                    actions.push(NodeAction::SendPing {
                        to,
                        sequence,
                        piggyback: pb,
                    });
                }
                SwimAction::SendPingReq { relay, target, sequence } => {
                    let pb = self.dissemination.pack_piggyback(self.max_piggyback);
                    actions.push(NodeAction::SendPingReq {
                        relay,
                        target,
                        sequence,
                        piggyback: pb,
                    });
                }
                SwimAction::Suspect(node_id) => {
                    eprintln!("SWIM: suspect {}", &hex_encode(&node_id.0)[..8]);
                    if self.members.suspect(node_id) {
                        if let Some(entry) = self.members.get(&node_id) {
                            self.dissemination.enqueue(
                                membership_update(node_id, MemberState::Suspect, entry.incarnation),
                                self.cluster_size(),
                            );
                        }
                        actions.push(NodeAction::MembershipChanged {
                            node_id,
                            state: MemberState::Suspect,
                            incarnation: self.members.get(&node_id).map(|e| e.incarnation).unwrap_or(0),
                        });
                    }
                }
                SwimAction::DeclareDead(node_id) => {
                    eprintln!("SWIM: dead {}", &hex_encode(&node_id.0)[..8]);
                    if let Some(entry) = self.members.get(&node_id) {
                        let inc = entry.incarnation;
                        self.dissemination.enqueue(
                            membership_update(node_id, MemberState::Dead, inc),
                            self.cluster_size(),
                        );
                        actions.push(NodeAction::MembershipChanged {
                            node_id,
                            state: MemberState::Dead,
                            incarnation: inc,
                        });
                    }
                }
                SwimAction::Refute { new_incarnation } => {
                    self.dissemination.enqueue(
                        membership_update(
                            self.members.self_id(),
                            MemberState::Alive,
                            new_incarnation,
                        ),
                        self.cluster_size(),
                    );
                }
            }
        }
        actions
    }
}
