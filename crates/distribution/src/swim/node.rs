//! Integrated SWIM node — composes probe cycle, dissemination, and join protocol.
//!
//! This is the top-level SWIM state machine that a `DistributedNode` will drive.
//! It produces `SwimAction`s that the caller translates into real network I/O.

use std::net::SocketAddr;

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
    SendPing { to: NodeId, to_addr: SocketAddr, sequence: u64, piggyback: Vec<u8> },
    /// Send an indirect ping request through a relay.
    SendPingReq {
        relay: NodeId,
        relay_addr: SocketAddr,
        target: NodeId,
        target_addr: SocketAddr,
        sequence: u64,
        piggyback: Vec<u8>,
    },
    /// Send a SWIM ack.
    SendAck { to: NodeId, to_addr: SocketAddr, sequence: u64, piggyback: Vec<u8> },
    /// Send a join request to a seed.
    SendJoinRequest { to_addr: SocketAddr },
    /// Send a join response with the current member list.
    SendJoinResponse { to: NodeId, to_addr: SocketAddr, members: Vec<NodeRecord> },
    /// Notification: a node state changed (for wiring into Kademlia).
    MembershipChanged { node_id: NodeId, state: MemberState, incarnation: u64 },
}

// ─── SwimNode ───────────────────────────────────────────────────────────────

pub struct SwimNode {
    members: MemberList,
    probe: SwimProbe,
    dissemination: DisseminationQueue,
    self_addr: SocketAddr,
    /// Maximum piggybacked updates per message.
    max_piggyback: usize,
}

impl SwimNode {
    pub fn new(self_id: NodeId, self_addr: SocketAddr, config: SwimConfig) -> Self {
        Self {
            members: MemberList::new(self_id),
            probe: SwimProbe::new(config),
            dissemination: DisseminationQueue::new(3), // Λ = 3
            self_addr,
            max_piggyback: 8,
        }
    }

    pub fn self_id(&self) -> NodeId {
        self.members.self_id()
    }

    pub fn self_addr(&self) -> SocketAddr {
        self.self_addr
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
    pub fn handle_ping(&mut self, from: NodeId, from_addr: SocketAddr, sequence: u64, piggyback: &[u8]) -> Vec<NodeAction> {
        self.apply_piggyback(piggyback);

        // Ensure the sender is in our member list
        self.members.apply(from, from_addr, MemberState::Alive, 0);

        // Reply with ack
        let pb = self.dissemination.pack_piggyback(self.max_piggyback);
        vec![NodeAction::SendAck {
            to: from,
            to_addr: from_addr,
            sequence,
            piggyback: pb,
        }]
    }

    /// Handle a received ack.
    pub fn handle_ack(&mut self, from: NodeId, sequence: u64, piggyback: &[u8]) -> Vec<NodeAction> {
        self.apply_piggyback(piggyback);
        let probe_actions = self.probe.step(
            SwimEvent::AckReceived { from, sequence },
            &mut self.members,
        );
        self.translate_probe_actions(probe_actions)
    }

    /// Handle a received indirect ping request.
    pub fn handle_ping_req(
        &mut self,
        _from: NodeId,
        target: NodeId,
        target_addr: SocketAddr,
        sequence: u64,
        piggyback: &[u8],
    ) -> Vec<NodeAction> {
        self.apply_piggyback(piggyback);

        // Forward a ping to the target on behalf of the requester
        let pb = self.dissemination.pack_piggyback(self.max_piggyback);
        vec![NodeAction::SendPing {
            to: target,
            to_addr: target_addr,
            sequence,
            piggyback: pb,
        }]
    }

    /// Handle a join request from a new node.
    pub fn handle_join_request(&mut self, from: NodeId, from_addr: SocketAddr) -> Vec<NodeAction> {
        // Add the new node to our member list
        let changed = self.members.apply(from, from_addr, MemberState::Alive, 0);
        let mut actions = Vec::new();

        if changed {
            // Enqueue the join for dissemination
            self.dissemination.enqueue(
                membership_update(from, from_addr, MemberState::Alive, 0),
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
            addr: self.self_addr,
            state: MemberState::Alive,
            incarnation: self.members.self_incarnation(),
        });
        actions.push(NodeAction::SendJoinResponse {
            to: from,
            to_addr: from_addr,
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
                record.addr,
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

    /// Initiate joining a cluster by contacting seed nodes.
    pub fn join(&self, seeds: &[SocketAddr]) -> Vec<NodeAction> {
        seeds
            .iter()
            .map(|addr| NodeAction::SendJoinRequest { to_addr: *addr })
            .collect()
    }

    /// Announce ourselves as dead (graceful leave).
    pub fn leave(&mut self) -> Vec<NodeAction> {
        self.dissemination.enqueue(
            membership_update(
                self.members.self_id(),
                self.self_addr,
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

    fn apply_piggyback(&mut self, bytes: &[u8]) {
        let updates = DisseminationQueue::unpack_piggyback(bytes);
        for update in updates {
            self.apply_membership_update(update);
        }
    }

    fn apply_membership_update(&mut self, update: MembershipUpdate) {
        // Check if this is about us
        if update.node_id == self.members.self_id() {
            if update.state == MemberState::Suspect || update.state == MemberState::Dead {
                // Refute: bump incarnation and disseminate
                let new_inc = self.members.refute();
                self.dissemination.enqueue(
                    membership_update(
                        self.members.self_id(),
                        self.self_addr,
                        MemberState::Alive,
                        new_inc,
                    ),
                    self.cluster_size(),
                );
            }
            return;
        }

        let changed = self.members.apply(
            update.node_id,
            update.addr,
            update.state,
            update.incarnation,
        );
        if changed {
            // Re-disseminate the update
            self.dissemination.enqueue(
                membership_update(update.node_id, update.addr, update.state, update.incarnation),
                self.cluster_size(),
            );
        }
    }

    fn translate_probe_actions(&mut self, probe_actions: Vec<SwimAction>) -> Vec<NodeAction> {
        let mut actions = Vec::new();
        for pa in probe_actions {
            match pa {
                SwimAction::SendPing { to, to_addr, sequence } => {
                    // If the target is dead, re-enqueue the death declaration
                    // so it piggybacks on this message. This is the key mechanism
                    // for partition-heal recovery: the dead node learns it was
                    // declared dead and refutes by bumping its incarnation.
                    if let Some(entry) = self.members.get(&to) {
                        if entry.state == MemberState::Dead {
                            self.dissemination.enqueue(
                                membership_update(to, to_addr, MemberState::Dead, entry.incarnation),
                                self.cluster_size(),
                            );
                        }
                    }
                    let pb = self.dissemination.pack_piggyback(self.max_piggyback);
                    actions.push(NodeAction::SendPing {
                        to,
                        to_addr,
                        sequence,
                        piggyback: pb,
                    });
                }
                SwimAction::SendPingReq { relay, relay_addr, target, target_addr, sequence } => {
                    let pb = self.dissemination.pack_piggyback(self.max_piggyback);
                    actions.push(NodeAction::SendPingReq {
                        relay,
                        relay_addr,
                        target,
                        target_addr,
                        sequence,
                        piggyback: pb,
                    });
                }
                SwimAction::Suspect(node_id) => {
                    if self.members.suspect(node_id) {
                        if let Some(entry) = self.members.get(&node_id) {
                            self.dissemination.enqueue(
                                membership_update(node_id, entry.addr, MemberState::Suspect, entry.incarnation),
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
                    if let Some(entry) = self.members.get(&node_id) {
                        let inc = entry.incarnation;
                        let addr = entry.addr;
                        if self.members.declare_dead(node_id) {
                            self.dissemination.enqueue(
                                membership_update(node_id, addr, MemberState::Dead, inc),
                                self.cluster_size(),
                            );
                            actions.push(NodeAction::MembershipChanged {
                                node_id,
                                state: MemberState::Dead,
                                incarnation: inc,
                            });
                        }
                    }
                }
                SwimAction::Refute { new_incarnation } => {
                    self.dissemination.enqueue(
                        membership_update(
                            self.members.self_id(),
                            self.self_addr,
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
