//! Integrated SWIM node — composes probe cycle, dissemination, and join protocol.
//!
//! This is the top-level SWIM state machine that a `DistributedNode` will drive.
//! It produces `SwimAction`s that the caller translates into real network I/O.

use std::sync::Arc;

use swactor::transport::hex_encode;
use crate::diagnostics::{noop_emitter, DynEmitter, Event as DiagEvent, EventEmitter, PeerState};
use crate::diagnostics::swim_introspect::SwimIntrospect;
use crate::diagnostics::snapshot::Tier2SwimConfig;
use crate::messages::MembershipUpdate;
use crate::types::{MemberState, NodeId, NodeRecord};

use super::dissemination::{membership_update, DisseminationQueue};
use super::member_list::MemberList;
use super::probe::{ProbeMode, SwimAction, SwimConfig, SwimEvent, SwimProbe};

/// Map SWIM's internal `MemberState` to the diagnostics wire type.
fn to_peer_state(state: MemberState) -> PeerState {
    match state {
        MemberState::Alive => PeerState::Alive,
        MemberState::Suspect => PeerState::Suspect,
        MemberState::Dead => PeerState::Dead,
    }
}

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
    /// Diagnostics sink. Defaults to a no-op so untouched call sites
    /// stay free of observability overhead. Production wires this with
    /// [`crate::diagnostics::Aggregator`] via [`Self::set_diagnostics`].
    diagnostics: DynEmitter,
    /// Optional tier-2 introspector. Populated by
    /// [`Self::install_introspect`] and updated from every state-
    /// changing entry point. None until installed so library tests
    /// that do not opt in stay free of the bookkeeping.
    introspect: Option<Arc<SwimIntrospect>>,
    /// Λ multiplier used by the dissemination queue — captured here
    /// so the introspector can report it without leaking into the
    /// dissemination layer's API.
    gossip_lambda: usize,
}

impl SwimNode {
    pub fn new(self_id: NodeId, config: SwimConfig) -> Self {
        const GOSSIP_LAMBDA: usize = 3;
        // Maximum membership updates piggybacked per outgoing message.
        // Lowered from 8 to 6 as part of the N3 tuning pass (see
        // `crates/simulation/SWIM_TUNING_REPORT.md`): smaller piggybacks
        // cap the wire size each refute-cascade can balloon to without
        // visibly slowing convergence at the cluster sizes the §10.3
        // gossip-flap property exercises.
        const MAX_PIGGYBACK: usize = 6;
        Self {
            members: MemberList::new(self_id),
            probe: SwimProbe::new(config),
            dissemination: DisseminationQueue::new(GOSSIP_LAMBDA),
            max_piggyback: MAX_PIGGYBACK,
            pending_relays: Vec::new(),
            diagnostics: noop_emitter(),
            introspect: None,
            gossip_lambda: GOSSIP_LAMBDA,
        }
    }

    /// Install a tier-2 SWIM introspector and return the
    /// `Arc<SwimIntrospect>` so the caller can register it with the
    /// diagnostics aggregator. The introspector is initialized from
    /// this node's protocol configuration and current member set.
    /// Subsequent state changes flow through the same Arc.
    ///
    /// Calling this a second time replaces the previous introspector.
    pub fn install_introspect(&mut self) -> Arc<SwimIntrospect> {
        let config = self.tier2_config();
        let introspect = Arc::new(SwimIntrospect::new(config, self.members.self_id()));
        introspect.note_self_incarnation(self.members.self_incarnation());
        for entry in self.members.all_members() {
            introspect.note_peer_state(entry.node_id, to_peer_state(entry.state), entry.incarnation);
        }
        self.introspect = Some(Arc::clone(&introspect));
        introspect
    }

    /// Snapshot of the protocol parameters this node was built with,
    /// in the wire-shape the diagnostics layer expects.
    fn tier2_config(&self) -> Tier2SwimConfig {
        let cfg = self.probe.config();
        let probe_mode = match &cfg.probe_mode {
            ProbeMode::Periodic => "periodic".to_string(),
            ProbeMode::Reactive {
                safety_sweep_interval,
            } => format!("reactive({safety_sweep_interval})"),
        };
        Tier2SwimConfig {
            probe_interval_ticks: cfg.probe_interval,
            probe_timeout_ticks: cfg.probe_timeout,
            suspicion_timeout_ticks: cfg.suspicion_timeout,
            indirect_probes_k: cfg.indirect_probes as u32,
            dead_reprobe_interval_ticks: cfg.dead_reprobe_interval,
            gossip_fanout_lambda: self.gossip_lambda as u32,
            max_piggyback: self.max_piggyback as u32,
            probe_mode,
        }
    }

    /// Borrow the installed introspector (read-only). Returns `None`
    /// until [`Self::install_introspect`] has been called.
    pub fn introspect(&self) -> Option<&Arc<SwimIntrospect>> {
        self.introspect.as_ref()
    }

    /// Install a diagnostics emitter so SWIM transitions surface as
    /// structured [`DiagEvent`]s. Safe to call at any time; events
    /// before the call are dropped.
    pub fn set_diagnostics(&mut self, emitter: DynEmitter) {
        self.diagnostics = emitter;
    }

    /// Borrow the current diagnostics emitter (read-only).
    pub fn diagnostics(&self) -> &DynEmitter {
        &self.diagnostics
    }

    fn emit_transition(&self, peer: NodeId, from: PeerState, to: PeerState, reason: &str) {
        self.diagnostics.emit_event(DiagEvent::SwimTransition {
            peer,
            from,
            to,
            reason: reason.to_string(),
        });
        if let Some(intro) = &self.introspect {
            let incarnation = self
                .members
                .get(&peer)
                .map(|e| e.incarnation)
                .unwrap_or(0);
            intro.note_peer_state(peer, to, incarnation);
            if to == PeerState::Suspect {
                intro.note_suspect_started(peer);
            }
        }
    }

    pub fn self_id(&self) -> NodeId {
        self.members.self_id()
    }

    pub fn members(&self) -> &MemberList {
        &self.members
    }

    /// Clear a Dead member entry so a subsequent JoinResponse can re-establish it.
    ///
    /// Used by the re-peer flow: a JoinResponse carries the remote node's
    /// self-report as `(Alive, incarnation)`, but SWIM merge semantics reject
    /// Alive at the same incarnation when the local entry is Dead.  Removing
    /// the stale Dead entry lets the fresh Alive record take effect.
    pub fn clear_dead_member(&mut self, node_id: NodeId) {
        if let Some(entry) = self.members.get(&node_id) {
            if entry.state == MemberState::Dead {
                self.members.remove(&node_id);
                self.dissemination.purge_node(&node_id);
                if let Some(intro) = &self.introspect {
                    intro.drop_peer(node_id);
                }
            }
        }
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
        if let Some(intro) = &self.introspect {
            intro.note_ping_received(from, sequence);
        }
        let mut actions = self.apply_piggyback(from, piggyback);

        // Ensure the sender is in our member list
        let prior = self
            .members
            .get(&from)
            .map(|e| to_peer_state(e.state))
            .unwrap_or(PeerState::Unknown);
        if self.members.apply(from, MemberState::Alive, 0) {
            self.emit_transition(from, prior, PeerState::Alive, "ping-received");
        }

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
        if let Some(intro) = &self.introspect {
            intro.note_ack_received(from, sequence);
        }
        let mut actions = self.apply_piggyback(from, piggyback);
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
        if let Some(intro) = &self.introspect {
            intro.note_ping_req_received(from, target, sequence);
        }
        let mut actions = self.apply_piggyback(from, piggyback);

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

    /// Report that a send to `target` failed, triggering a reactive probe.
    pub fn report_send_failure(&mut self, target: NodeId) -> Vec<NodeAction> {
        let probe_actions = self.probe.step(
            SwimEvent::SendFailed { to: target },
            &mut self.members,
        );
        self.translate_probe_actions(probe_actions)
    }

    /// Handle a received indirect ack (forwarded by a relay node).
    pub fn handle_indirect_ack(&mut self, target: NodeId, sequence: u64, piggyback: &[u8]) -> Vec<NodeAction> {
        if let Some(intro) = &self.introspect {
            intro.note_indirect_ack_received(target, sequence);
        }
        // `target` is the indirectly-probed peer; the membership data
        // ultimately came from there even though a relay forwarded it.
        // Crediting `target` as the gossip source matches the bundle
        // reader's intent ("which peer's news is this").
        let mut actions = self.apply_piggyback(target, piggyback);
        let probe_actions = self.probe.step(
            SwimEvent::IndirectAckReceived { target, sequence },
            &mut self.members,
        );
        actions.extend(self.translate_probe_actions(probe_actions));
        actions
    }

    /// Handle a join request from a new node.
    pub fn handle_join_request(&mut self, from: NodeId) -> Vec<NodeAction> {
        if let Some(intro) = &self.introspect {
            intro.note_join_request_received(from);
        }
        // Add the new node to our member list
        let prior = self
            .members
            .get(&from)
            .map(|e| to_peer_state(e.state))
            .unwrap_or(PeerState::Unknown);
        let changed = self.members.apply(from, MemberState::Alive, 0);
        let mut actions = Vec::new();

        if changed {
            self.emit_transition(from, prior, PeerState::Alive, "join-request");
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
        if let Some(intro) = &self.introspect {
            // The first record in a join response is conventionally
            // the responding peer; fall back to self-id if the list is
            // somehow empty so the message still surfaces.
            let from = members
                .first()
                .map(|r| r.node_id)
                .unwrap_or(self.members.self_id());
            intro.note_join_response_received(from, members.len());
        }
        let mut actions = Vec::new();
        for record in members {
            let prior = self
                .members
                .get(&record.node_id)
                .map(|e| to_peer_state(e.state))
                .unwrap_or(PeerState::Unknown);
            let changed = self.members.apply(
                record.node_id,
                record.state,
                record.incarnation,
            );
            if changed {
                self.emit_transition(
                    record.node_id,
                    prior,
                    to_peer_state(record.state),
                    "join-response",
                );
                actions.push(NodeAction::MembershipChanged {
                    node_id: record.node_id,
                    state: record.state,
                    incarnation: record.incarnation,
                });
                // In reactive mode, probe newly discovered alive peers so they
                // don't decay to dead before we ever exchange a ping/ack.
                if record.state == MemberState::Alive && record.node_id != self.members.self_id() {
                    self.probe.enqueue_demand_probe(record.node_id);
                }
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

    fn apply_piggyback(&mut self, from: NodeId, bytes: &[u8]) -> Vec<NodeAction> {
        let updates = DisseminationQueue::unpack_piggyback(bytes);
        // Spec §10 (gap 10): typed receipt event per piggyback. Fires
        // for every payload-bearing receipt so a bundle reader can
        // reconstruct gossip propagation per (source, kind) without
        // grepping the SWIM internals.
        if !bytes.is_empty() {
            self.diagnostics.emit_event(DiagEvent::GossipReceived {
                source_peer: from,
                payload_kind: "swim_piggyback".to_string(),
                payload_bytes: bytes.len().min(u32::MAX as usize) as u32,
                item_count: updates.len().min(u32::MAX as usize) as u32,
            });
        }
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
                if let Some(intro) = &self.introspect {
                    intro.note_self_incarnation(new_inc);
                }
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

        let prior = self
            .members
            .get(&update.node_id)
            .map(|e| to_peer_state(e.state))
            .unwrap_or(PeerState::Unknown);

        let changed = self.members.apply(
            update.node_id,
            update.state,
            update.incarnation,
        );
        if changed {
            self.emit_transition(
                update.node_id,
                prior,
                to_peer_state(update.state),
                "gossip",
            );
            if update.state == MemberState::Alive {
                eprintln!("SWIM: alive {}", &hex_encode(&update.node_id.0)[..8]);
                // In reactive mode, probe newly discovered alive peers so they
                // don't decay to dead before we ever exchange a ping/ack.
                self.probe.enqueue_demand_probe(update.node_id);
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
                    if let Some(entry) = self.members.get(&to)
                        && (entry.state == MemberState::Dead || entry.state == MemberState::Suspect) {
                            self.dissemination.enqueue(
                                membership_update(to, entry.state, entry.incarnation),
                                self.cluster_size(),
                            );
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
                    let prior = self
                        .members
                        .get(&node_id)
                        .map(|e| to_peer_state(e.state))
                        .unwrap_or(PeerState::Unknown);
                    if self.members.suspect(node_id) {
                        self.emit_transition(node_id, prior, PeerState::Suspect, "probe-timeout");
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
                    // The probe layer already flipped Suspect→Dead in
                    // `MemberList` before producing this action, so the
                    // current entry reads Dead. SWIM's lifecycle is
                    // Alive→Suspect→Dead, so we always come from Suspect.
                    if let Some(entry) = self.members.get(&node_id) {
                        let inc = entry.incarnation;
                        self.dissemination.enqueue(
                            membership_update(node_id, MemberState::Dead, inc),
                            self.cluster_size(),
                        );
                        self.emit_transition(
                            node_id,
                            PeerState::Suspect,
                            PeerState::Dead,
                            "suspicion-timeout",
                        );
                        actions.push(NodeAction::MembershipChanged {
                            node_id,
                            state: MemberState::Dead,
                            incarnation: inc,
                        });
                    }
                }
                SwimAction::Refute { new_incarnation } => {
                    if let Some(intro) = &self.introspect {
                        intro.note_self_incarnation(new_incarnation);
                    }
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
