//! SWIM probe cycle state machine.
//!
//! Pure function design: `(state, event) → (state, actions)`.
//! No I/O, no timers — the caller drives the clock.

use std::collections::VecDeque;
use std::net::SocketAddr;

use crate::types::NodeId;

use super::member_list::MemberList;

/// Maximum number of recent probe targets to remember.
const PROBE_HISTORY_SIZE: usize = 16;

// ─── Configuration ──────────────────────────────────────────────────────────

/// SWIM protocol configuration.
#[derive(Debug, Clone)]
pub struct SwimConfig {
    /// Ticks between probe cycles.
    pub probe_interval: u64,
    /// Ticks to wait for a direct ack before sending indirect probes.
    pub probe_timeout: u64,
    /// Number of indirect probe relays (k in the SWIM paper).
    pub indirect_probes: usize,
    /// Ticks a node stays in Suspect before being declared Dead.
    pub suspicion_timeout: u64,
}

impl Default for SwimConfig {
    fn default() -> Self {
        Self {
            probe_interval: 10,
            probe_timeout: 3,
            indirect_probes: 3,
            suspicion_timeout: 30,
        }
    }
}

// ─── Events (inputs) ────────────────────────────────────────────────────────

/// Events fed into the probe state machine.
#[derive(Debug, Clone)]
pub enum SwimEvent {
    /// A tick of the clock.
    Tick,
    /// Received an ack for a specific sequence number.
    AckReceived { from: NodeId, sequence: u64 },
    /// Received an indirect ack (forwarded through a relay).
    IndirectAckReceived { target: NodeId, sequence: u64 },
}

// ─── Actions (outputs) ──────────────────────────────────────────────────────

/// Actions produced by the probe state machine.
#[derive(Debug, Clone)]
pub enum SwimAction {
    /// Send a direct ping to a node.
    SendPing { to: NodeId, to_addr: SocketAddr, sequence: u64 },
    /// Send an indirect ping request through a relay.
    SendPingReq {
        relay: NodeId,
        relay_addr: SocketAddr,
        target: NodeId,
        target_addr: SocketAddr,
        sequence: u64,
    },
    /// A node is now suspected.
    Suspect(NodeId),
    /// A node is declared dead.
    DeclareDead(NodeId),
    /// Our node was suspected — refute with bumped incarnation.
    Refute { new_incarnation: u64 },
}

// ─── Probe State ────────────────────────────────────────────────────────────

#[derive(Debug)]
enum ProbePhase {
    /// Waiting for the next probe cycle.
    Idle,
    /// Direct ping sent, waiting for ack.
    WaitingDirectAck {
        target: NodeId,
        target_addr: SocketAddr,
        sequence: u64,
        sent_at: u64,
    },
    /// Indirect probes sent, waiting for any ack.
    WaitingIndirectAck {
        target: NodeId,
        sequence: u64,
        sent_at: u64,
    },
}

/// Suspicion timer for a single node.
#[derive(Debug)]
struct SuspicionTimer {
    node_id: NodeId,
    started_at: u64,
}

/// The SWIM probe state machine.
pub struct SwimProbe {
    config: SwimConfig,
    tick: u64,
    next_probe_tick: u64,
    sequence: u64,
    phase: ProbePhase,
    /// Round-robin index into the member list for probe target selection.
    probe_index: usize,
    /// Shuffled ordering of members to probe.
    probe_order: Vec<NodeId>,
    /// Active suspicion timers.
    suspicion_timers: Vec<SuspicionTimer>,
    /// Ring buffer of recent probe targets (most recent at back).
    recent_targets: VecDeque<NodeId>,
}

impl SwimProbe {
    pub fn new(config: SwimConfig) -> Self {
        Self {
            next_probe_tick: config.probe_interval,
            config,
            tick: 0,
            sequence: 0,
            phase: ProbePhase::Idle,
            probe_index: 0,
            probe_order: Vec::new(),
            suspicion_timers: Vec::new(),
            recent_targets: VecDeque::with_capacity(PROBE_HISTORY_SIZE),
        }
    }

    /// Process an event and produce zero or more actions.
    pub fn step(&mut self, event: SwimEvent, members: &mut MemberList) -> Vec<SwimAction> {
        let mut actions = Vec::new();

        match event {
            SwimEvent::Tick => {
                self.tick += 1;
                self.check_probe_timeout(members, &mut actions);
                self.check_suspicion_timeouts(members, &mut actions);
                self.maybe_start_probe(members, &mut actions);
            }
            SwimEvent::AckReceived { from, sequence } => {
                self.handle_ack(from, sequence, members, &mut actions);
            }
            SwimEvent::IndirectAckReceived { target, sequence } => {
                self.handle_indirect_ack(target, sequence, members, &mut actions);
            }
        }

        actions
    }

    /// Recent probe targets (most recent last).
    pub fn recent_probe_targets(&self) -> &VecDeque<NodeId> {
        &self.recent_targets
    }

    fn next_sequence(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }

    /// Pick the next probe target using round-robin over a shuffled order.
    fn pick_probe_target(&mut self, members: &MemberList) -> Option<(NodeId, SocketAddr)> {
        let alive = members.alive_members();
        if alive.is_empty() {
            return None;
        }

        // Rebuild probe order when exhausted or membership changed
        if self.probe_index >= self.probe_order.len() || self.probe_order.len() != alive.len() {
            self.probe_order = alive.iter().map(|e| e.node_id).collect();
            // Simple shuffle using XOR of tick and index
            let n = self.probe_order.len();
            for i in (1..n).rev() {
                let j = ((self.tick as usize).wrapping_mul(31).wrapping_add(i)) % (i + 1);
                self.probe_order.swap(i, j);
            }
            self.probe_index = 0;
        }

        let target_id = self.probe_order[self.probe_index];
        self.probe_index += 1;

        members.get(&target_id).map(|e| (e.node_id, e.addr))
    }

    /// Pick `k` random relay nodes (excluding `target`).
    fn pick_relays(&self, members: &MemberList, target: NodeId) -> Vec<(NodeId, SocketAddr)> {
        let alive: Vec<_> = members
            .alive_members()
            .into_iter()
            .filter(|e| e.node_id != target)
            .collect();

        let k = self.config.indirect_probes.min(alive.len());
        // Simple selection: take first k after a rotation based on tick
        let start = if alive.is_empty() { 0 } else { self.tick as usize % alive.len() };
        let mut relays = Vec::with_capacity(k);
        for i in 0..k {
            let idx = (start + i) % alive.len();
            relays.push((alive[idx].node_id, alive[idx].addr));
        }
        relays
    }

    fn maybe_start_probe(&mut self, members: &MemberList, actions: &mut Vec<SwimAction>) {
        if self.tick < self.next_probe_tick {
            return;
        }
        if !matches!(self.phase, ProbePhase::Idle) {
            return;
        }

        self.next_probe_tick = self.tick + self.config.probe_interval;

        if let Some((target, target_addr)) = self.pick_probe_target(&mut MemberList::clone_shallow(members)) {
            // Record this probe target in history
            if self.recent_targets.len() >= PROBE_HISTORY_SIZE {
                self.recent_targets.pop_front();
            }
            self.recent_targets.push_back(target);

            let seq = self.next_sequence();
            actions.push(SwimAction::SendPing {
                to: target,
                to_addr: target_addr,
                sequence: seq,
            });
            self.phase = ProbePhase::WaitingDirectAck {
                target,
                target_addr,
                sequence: seq,
                sent_at: self.tick,
            };
        }
    }

    fn check_probe_timeout(&mut self, members: &MemberList, actions: &mut Vec<SwimAction>) {
        match &self.phase {
            ProbePhase::WaitingDirectAck { target, target_addr, sequence, sent_at } => {
                if self.tick - sent_at >= self.config.probe_timeout {
                    let target = *target;
                    let target_addr = *target_addr;
                    let sequence = *sequence;

                    // Send indirect probes through relays
                    let relays = self.pick_relays(members, target);
                    for (relay, relay_addr) in relays {
                        actions.push(SwimAction::SendPingReq {
                            relay,
                            relay_addr,
                            target,
                            target_addr,
                            sequence,
                        });
                    }

                    self.phase = ProbePhase::WaitingIndirectAck {
                        target,
                        sequence,
                        sent_at: self.tick,
                    };
                }
            }
            ProbePhase::WaitingIndirectAck { target, sequence: _, sent_at } => {
                if self.tick - sent_at >= self.config.probe_timeout {
                    let target = *target;
                    // No ack received — suspect this node
                    actions.push(SwimAction::Suspect(target));
                    self.start_suspicion_timer(target);
                    self.phase = ProbePhase::Idle;
                }
            }
            ProbePhase::Idle => {}
        }
    }

    fn handle_ack(&mut self, from: NodeId, sequence: u64, _members: &mut MemberList, _actions: &mut Vec<SwimAction>) {
        match &self.phase {
            ProbePhase::WaitingDirectAck { target, sequence: expected, .. }
            | ProbePhase::WaitingIndirectAck { target, sequence: expected, .. } => {
                if from == *target && sequence == *expected {
                    // Successful ack — cancel any suspicion timer for this node
                    self.cancel_suspicion_timer(from);
                    self.phase = ProbePhase::Idle;
                }
            }
            ProbePhase::Idle => {}
        }
    }

    fn handle_indirect_ack(&mut self, target: NodeId, sequence: u64, _members: &mut MemberList, _actions: &mut Vec<SwimAction>) {
        match &self.phase {
            ProbePhase::WaitingIndirectAck { target: expected, sequence: expected_seq, .. } => {
                if target == *expected && sequence == *expected_seq {
                    self.cancel_suspicion_timer(target);
                    self.phase = ProbePhase::Idle;
                }
            }
            _ => {}
        }
    }

    fn start_suspicion_timer(&mut self, node_id: NodeId) {
        // Don't start duplicate timers
        if self.suspicion_timers.iter().any(|t| t.node_id == node_id) {
            return;
        }
        self.suspicion_timers.push(SuspicionTimer {
            node_id,
            started_at: self.tick,
        });
    }

    fn cancel_suspicion_timer(&mut self, node_id: NodeId) {
        self.suspicion_timers.retain(|t| t.node_id != node_id);
    }

    fn check_suspicion_timeouts(&mut self, members: &mut MemberList, actions: &mut Vec<SwimAction>) {
        let timeout = self.config.suspicion_timeout;
        let tick = self.tick;
        let expired: Vec<NodeId> = self
            .suspicion_timers
            .iter()
            .filter(|t| tick - t.started_at >= timeout)
            .map(|t| t.node_id)
            .collect();

        for node_id in expired {
            if members.declare_dead(node_id) {
                actions.push(SwimAction::DeclareDead(node_id));
            }
            self.cancel_suspicion_timer(node_id);
        }
    }
}

// Helper: we need a read-only borrow of members in pick_probe_target
// while also having &mut self. Use a shallow clone pattern.
impl MemberList {
    /// Cheap snapshot of just the IDs and addresses for probe target selection.
    fn clone_shallow(original: &MemberList) -> MemberList {
        let mut copy = MemberList::new(original.self_id());
        for entry in original.all_members() {
            copy.apply(entry.node_id, entry.addr, entry.state, entry.incarnation);
        }
        copy
    }
}
