//! Integrated SWIM node — composes probe cycle, dissemination, and join protocol.
//!
//! This is the top-level SWIM state machine, driven by the
//! [`crate::swim::actor::SwimActor`].
//! It produces `SwimAction`s that the caller translates into real network I/O.
//!
//! Membership and probe lifecycle surface through an optional
//! [`SwimObserver`] (diagnostics-free): production installs none and pays
//! nothing, while the simulator records [`SwimObservation`]s for scenario
//! evaluation. Node-level telemetry is captured separately by the datastream
//! membership channel, which diffs the member view each tick.

use std::time::Instant;

use crate::messages::MembershipUpdate;
use crate::types::{MemberState, NodeId, NodeRecord};

use super::dissemination::{DisseminationQueue, membership_update};
use super::member_list::MemberList;
use super::probe::{SwimAction, SwimConfig, SwimDiagEvent, SwimEvent, SwimProbe};

// ─── SwimNode Actions (superset of probe actions) ───────────────────────────

/// Actions produced by the integrated SWIM node.
#[derive(Debug, Clone)]
pub enum NodeAction {
    /// Send a SWIM ping.
    SendPing {
        to: NodeId,
        sequence: u64,
        piggyback: Vec<u8>,
    },
    /// Send an indirect ping request through a relay.
    SendPingReq {
        relay: NodeId,
        target: NodeId,
        sequence: u64,
        piggyback: Vec<u8>,
    },
    /// Send a SWIM ack.
    SendAck {
        to: NodeId,
        sequence: u64,
        piggyback: Vec<u8>,
    },
    /// Forward an indirect ack back to the original prober.
    ForwardAck {
        to: NodeId,
        target: NodeId,
        sequence: u64,
        piggyback: Vec<u8>,
    },
    /// Send a join response with the current member list.
    SendJoinResponse {
        to: NodeId,
        members: Vec<NodeRecord>,
    },
    /// Notification: a node state changed (for wiring into the directory actor).
    MembershipChanged {
        node_id: NodeId,
        state: MemberState,
        incarnation: u64,
    },
}

// ─── Observation (diagnostics-free) ─────────────────────────────────────────

/// A read-only observation of SWIM activity — carries no protocol effect.
/// Production installs no observer and pays nothing.
#[derive(Debug, Clone)]
pub enum SwimObservation {
    /// A peer's membership state changed. `from` is `None` when the peer
    /// was previously unknown to this node.
    Transition {
        peer: NodeId,
        from: Option<MemberState>,
        to: MemberState,
        reason: &'static str,
    },
    /// A probe was initiated (`kind` is `"direct"` or `"indirect"`).
    ProbeSent {
        target: NodeId,
        sequence: u64,
        kind: &'static str,
    },
    /// An in-flight probe was answered.
    ProbeAcked {
        target: NodeId,
        sequence: u64,
        kind: &'static str,
    },
    /// An in-flight probe ran out its budget. `budget_ticks` is the
    /// configured `probe_timeout`.
    ProbeTimedOut {
        target: NodeId,
        sequence: u64,
        kind: &'static str,
        budget_ticks: u64,
    },
}

/// Receiver for [`SwimObservation`]s. Installed via [`SwimNode::set_observer`].
pub trait SwimObserver: Send {
    fn observe(&self, observation: SwimObservation);
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
    /// Optional observation hook. `None` until installed via
    /// [`Self::set_observer`]; production leaves it unset.
    observer: Option<Box<dyn SwimObserver>>,
    /// Last clock observed via [`Self::tick`]. Non-tick events
    /// (ack/indirect-ack/send-failure) feed it to the probe state
    /// machine so every `SwimProbe::step` gets a wall-clock `now`
    /// without threading it through every handler signature. The probe
    /// loop ticks every driver iteration, so this is at most one
    /// iteration stale.
    clock: Instant,
}

impl SwimNode {
    pub fn new(self_id: NodeId, config: SwimConfig, now: Instant) -> Self {
        const GOSSIP_LAMBDA: usize = 3;
        // Maximum membership updates piggybacked per outgoing message.
        // Lowered from 8 to 6 in the N3 tuning pass: smaller piggybacks
        // cap the wire size each refute-cascade can balloon to without
        // visibly slowing convergence at the cluster sizes covered by
        // gossip-flap checks.
        const MAX_PIGGYBACK: usize = 6;
        Self {
            members: MemberList::new(self_id),
            probe: SwimProbe::new(config, now),
            dissemination: DisseminationQueue::new(GOSSIP_LAMBDA),
            max_piggyback: MAX_PIGGYBACK,
            pending_relays: Vec::new(),
            observer: None,
            clock: now,
        }
    }

    /// Install an observation hook so membership transitions and probe
    /// lifecycle surface as [`SwimObservation`]s. Safe to call at any
    /// time; observations before the call are dropped.
    pub fn set_observer(&mut self, observer: Box<dyn SwimObserver>) {
        self.observer = Some(observer);
    }

    fn observe(&self, observation: SwimObservation) {
        if let Some(obs) = &self.observer {
            obs.observe(observation);
        }
    }

    fn observe_transition(
        &self,
        peer: NodeId,
        from: Option<MemberState>,
        to: MemberState,
        reason: &'static str,
    ) {
        self.observe(SwimObservation::Transition {
            peer,
            from,
            to,
            reason,
        });
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
            }
        }
    }

    /// Recent probe targets from the SWIM probe cycle.
    pub fn recent_probe_targets(&self) -> &std::collections::VecDeque<NodeId> {
        self.probe.recent_probe_targets()
    }

    /// Process a tick at wall-clock `now` — drives the probe cycle.
    pub fn tick(&mut self, now: Instant) -> Vec<NodeAction> {
        self.clock = now;
        let probe_actions = self.probe.step(now, SwimEvent::Tick, &mut self.members);
        self.translate_probe_actions(probe_actions)
    }

    /// Handle a received ping.
    pub fn handle_ping(
        &mut self,
        from: NodeId,
        sequence: u64,
        piggyback: &[u8],
    ) -> Vec<NodeAction> {
        let mut actions = self.apply_piggyback(from, piggyback);

        // Ensure the sender is in our member list
        let prior = self.members.get(&from).map(|e| e.state);
        if self.members.apply(from, MemberState::Alive, 0) {
            self.observe_transition(from, prior, MemberState::Alive, "ping-received");
            // §10.2 learn-sender flag: `MembershipChanged` is the actor's sole
            // membership observable (§6.3), so first-learning a sender via its
            // Ping routes through it — consistent with Join (§10.8) and gossip
            // (§10.0). The spec does NOT re-gossip this learn (no dissemination
            // enqueue); it is a notification only.
            actions.push(NodeAction::MembershipChanged {
                node_id: from,
                state: MemberState::Alive,
                incarnation: 0,
            });
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
        let mut actions = self.apply_piggyback(from, piggyback);
        let probe_actions = self.probe.step(
            self.clock,
            SwimEvent::AckReceived { from, sequence },
            &mut self.members,
        );
        actions.extend(self.translate_probe_actions(probe_actions));

        // Check if this ack completes a pending relay (indirect ping path)
        if let Some(pos) = self
            .pending_relays
            .iter()
            .position(|(_, t, s)| *t == from && *s == sequence)
        {
            let (requester, target, seq) = self.pending_relays.remove(pos);
            let pb = self.dissemination.pack_piggyback(self.max_piggyback);
            actions.push(NodeAction::ForwardAck {
                to: requester,
                target,
                sequence: seq,
                piggyback: pb,
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
            self.clock,
            SwimEvent::SendFailed { to: target },
            &mut self.members,
        );
        self.translate_probe_actions(probe_actions)
    }

    /// Handle a received indirect ack (forwarded by a relay node).
    pub fn handle_indirect_ack(
        &mut self,
        target: NodeId,
        sequence: u64,
        piggyback: &[u8],
    ) -> Vec<NodeAction> {
        // `target` is the indirectly-probed peer; the membership data
        // ultimately came from there even though a relay forwarded it.
        // Crediting `target` as the gossip source matches the bundle
        // reader's intent ("which peer's news is this").
        let mut actions = self.apply_piggyback(target, piggyback);
        let probe_actions = self.probe.step(
            self.clock,
            SwimEvent::IndirectAckReceived { target, sequence },
            &mut self.members,
        );
        actions.extend(self.translate_probe_actions(probe_actions));
        actions
    }

    /// Handle a join request from a new node.
    pub fn handle_join_request(&mut self, from: NodeId) -> Vec<NodeAction> {
        // Add the new node to our member list
        let prior = self.members.get(&from).map(|e| e.state);
        let changed = self.members.apply(from, MemberState::Alive, 0);
        let mut actions = Vec::new();

        if changed {
            self.observe_transition(from, prior, MemberState::Alive, "join-request");
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
        actions.push(NodeAction::SendJoinResponse { to: from, members });

        actions
    }

    /// Handle a join response (we received the member list from a seed).
    pub fn handle_join_response(&mut self, members: Vec<NodeRecord>) -> Vec<NodeAction> {
        let mut actions = Vec::new();
        for record in members {
            let prior = self.members.get(&record.node_id).map(|e| e.state);
            let changed = self
                .members
                .apply(record.node_id, record.state, record.incarnation);
            if changed {
                self.observe_transition(record.node_id, prior, record.state, "join-response");
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

    fn apply_piggyback(&mut self, _from: NodeId, bytes: &[u8]) -> Vec<NodeAction> {
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
            // Layer-B1 refute-on-stale-Suspect gate: only
            // refute when the incoming Suspect/Dead update is at our
            // *current* incarnation. A gossip path that carries a
            // stale Suspect/Dead record at incarnation N while our
            // local incarnation has already advanced past N is news
            // we have already refuted — refuting again creates a
            // non-zero floor on `self_incarnation_peak` that no
            // tuning can collapse. Under the relay-mediated path the
            // `1779733878` deploy exposed, stale Suspects can sit in
            // the dissemination queue for many probe cycles; gating
            // on incarnation is what keeps the storm bounded.
            if (update.state == MemberState::Suspect || update.state == MemberState::Dead)
                && update.incarnation >= self.members.self_incarnation()
            {
                // Refute: bump incarnation and disseminate
                let new_inc = self.members.refute();
                self.dissemination.enqueue(
                    membership_update(self.members.self_id(), MemberState::Alive, new_inc),
                    self.cluster_size(),
                );
            }
            return Vec::new();
        }

        let prior = self.members.get(&update.node_id).map(|e| e.state);
        let changed = self
            .members
            .apply(update.node_id, update.state, update.incarnation);
        if changed {
            self.observe_transition(update.node_id, prior, update.state, "gossip");
            if update.state == MemberState::Alive {
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
                    // Record the probe initiation. An observer joins
                    // (target, sequence) across `ProbeSent` / `ProbeAcked` /
                    // `ProbeTimedOut` to reconstruct per-probe RTT.
                    self.observe(SwimObservation::ProbeSent {
                        target: to,
                        sequence,
                        kind: "direct",
                    });
                    // If the target is suspect or dead, re-enqueue its state
                    // so it piggybacks on this message. This is the key mechanism
                    // for partition-heal recovery: the target learns it was
                    // suspected/declared dead and refutes by bumping its incarnation.
                    if let Some(entry) = self.members.get(&to)
                        && (entry.state == MemberState::Dead || entry.state == MemberState::Suspect)
                    {
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
                SwimAction::SendPingReq {
                    relay,
                    target,
                    sequence,
                } => {
                    // Indirect-phase probe initiation.
                    self.observe(SwimObservation::ProbeSent {
                        target,
                        sequence,
                        kind: "indirect",
                    });
                    let pb = self.dissemination.pack_piggyback(self.max_piggyback);
                    actions.push(NodeAction::SendPingReq {
                        relay,
                        target,
                        sequence,
                        piggyback: pb,
                    });
                }
                SwimAction::Suspect(node_id) => {
                    let prior = self.members.get(&node_id).map(|e| e.state);
                    if self.members.suspect(node_id) {
                        self.observe_transition(
                            node_id,
                            prior,
                            MemberState::Suspect,
                            "probe-timeout",
                        );
                        if let Some(entry) = self.members.get(&node_id) {
                            self.dissemination.enqueue(
                                membership_update(node_id, MemberState::Suspect, entry.incarnation),
                                self.cluster_size(),
                            );
                        }
                        actions.push(NodeAction::MembershipChanged {
                            node_id,
                            state: MemberState::Suspect,
                            incarnation: self
                                .members
                                .get(&node_id)
                                .map(|e| e.incarnation)
                                .unwrap_or(0),
                        });
                    }
                }
                SwimAction::DeclareDead(node_id) => {
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
                        self.observe_transition(
                            node_id,
                            Some(MemberState::Suspect),
                            MemberState::Dead,
                            "suspicion-timeout",
                        );
                        actions.push(NodeAction::MembershipChanged {
                            node_id,
                            state: MemberState::Dead,
                            incarnation: inc,
                        });
                    }
                }
                // Probe ack/timeout lifecycle — no protocol effect, surfaced to
                // the observer for per-probe RTT reconstruction.
                SwimAction::Diag(diag) => match diag {
                    SwimDiagEvent::ProbeAcked {
                        target,
                        sequence,
                        kind,
                    } => {
                        self.observe(SwimObservation::ProbeAcked {
                            target,
                            sequence,
                            kind,
                        });
                    }
                    SwimDiagEvent::ProbeTimedOut {
                        target,
                        sequence,
                        kind,
                        budget_ticks,
                    } => {
                        self.observe(SwimObservation::ProbeTimedOut {
                            target,
                            sequence,
                            kind,
                            budget_ticks,
                        });
                    }
                },
            }
        }
        actions
    }
}
