//! SWIM probe cycle state machine.
//!
//! Pure function design: `(state, event) → (state, actions)`.
//! No I/O, no timers — the caller drives the clock.

use std::collections::VecDeque;

use crate::types::{MemberState, NodeId};

use super::member_list::MemberList;

/// Maximum number of recent probe targets to remember.
const PROBE_HISTORY_SIZE: usize = 16;

// ─── Configuration ──────────────────────────────────────────────────────────

/// Controls how the probe cycle triggers probes.
#[derive(Debug, Clone, PartialEq)]
pub enum ProbeMode {
    /// Classic SWIM: probe one random member every `probe_interval` ticks.
    Periodic,
    /// No periodic probing. Probes triggered externally via `SendFailed`.
    /// Safety sweep probes one random member every `safety_sweep_interval` ticks.
    Reactive { safety_sweep_interval: u64 },
}

/// SWIM protocol configuration.
#[derive(Debug, Clone)]
pub struct SwimConfig {
    /// Ticks between probe cycles (used in Periodic mode).
    pub probe_interval: u64,
    /// Ticks to wait for a direct ack before sending indirect probes.
    pub probe_timeout: u64,
    /// Number of indirect probe relays (k in the SWIM paper).
    pub indirect_probes: usize,
    /// Ticks a node stays in Suspect before being declared Dead.
    pub suspicion_timeout: u64,
    /// Ticks between dead-node reprobe attempts. 0 = disabled.
    /// When enabled, periodically pings dead nodes to detect partition heals.
    pub dead_reprobe_interval: u64,
    /// Probe mode: Periodic (default) or Reactive (probe-on-failure).
    pub probe_mode: ProbeMode,
}

impl Default for SwimConfig {
    fn default() -> Self {
        Self {
            probe_interval: 10,
            probe_timeout: 3,
            indirect_probes: 3,
            suspicion_timeout: 30,
            dead_reprobe_interval: 50,
            probe_mode: ProbeMode::Periodic,
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
    /// A send to the given peer failed (reactive probe trigger).
    SendFailed { to: NodeId },
}

// ─── Actions (outputs) ──────────────────────────────────────────────────────

/// Actions produced by the probe state machine.
#[derive(Debug, Clone)]
pub enum SwimAction {
    /// Send a direct ping to a node.
    SendPing { to: NodeId, sequence: u64 },
    /// Send an indirect ping request through a relay.
    SendPingReq {
        relay: NodeId,
        target: NodeId,
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

/// Maximum demand queue size to prevent unbounded growth.
const MAX_DEMAND_QUEUE: usize = 32;

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
    /// Tick at which the next dead-node reprobe should fire.
    next_reprobe_tick: u64,
    /// Round-robin index into the dead member list for reprobe target selection.
    reprobe_index: usize,
    /// Peers needing probes due to send failures (reactive mode).
    demand_queue: VecDeque<NodeId>,
    /// Tick at which the next safety sweep fires (reactive mode).
    next_sweep_tick: u64,
}

impl SwimProbe {
    pub fn new(config: SwimConfig) -> Self {
        let next_reprobe = if config.dead_reprobe_interval > 0 {
            config.dead_reprobe_interval
        } else {
            u64::MAX
        };
        let next_sweep = match &config.probe_mode {
            ProbeMode::Reactive { safety_sweep_interval } => *safety_sweep_interval,
            ProbeMode::Periodic => u64::MAX,
        };
        Self {
            next_probe_tick: config.probe_interval,
            next_reprobe_tick: next_reprobe,
            reprobe_index: 0,
            config,
            tick: 0,
            sequence: 0,
            phase: ProbePhase::Idle,
            probe_index: 0,
            probe_order: Vec::new(),
            suspicion_timers: Vec::new(),
            recent_targets: VecDeque::with_capacity(PROBE_HISTORY_SIZE),
            demand_queue: VecDeque::new(),
            next_sweep_tick: next_sweep,
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
                match &self.config.probe_mode {
                    ProbeMode::Periodic => {
                        self.maybe_start_probe(members, &mut actions);
                    }
                    ProbeMode::Reactive { .. } => {
                        self.maybe_start_demand_probe(members, &mut actions);
                        self.maybe_safety_sweep(members, &mut actions);
                    }
                }
                self.maybe_reprobe_dead(members, &mut actions);
            }
            SwimEvent::AckReceived { from, sequence } => {
                self.handle_ack(from, sequence, members, &mut actions);
            }
            SwimEvent::IndirectAckReceived { target, sequence } => {
                self.handle_indirect_ack(target, sequence, members, &mut actions);
            }
            SwimEvent::SendFailed { to } => {
                self.handle_send_failed(to, members, &mut actions);
            }
        }

        actions
    }

    /// Recent probe targets (most recent last).
    pub fn recent_probe_targets(&self) -> &VecDeque<NodeId> {
        &self.recent_targets
    }

    /// Read-only access to the configured timeouts and fanouts. Used
    /// by the diagnostics layer to capture protocol parameters into
    /// tier-2 snapshots without having to thread them in separately.
    pub fn config(&self) -> &SwimConfig {
        &self.config
    }

    fn next_sequence(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }

    /// Pick the next probe target using round-robin over a shuffled order.
    fn pick_probe_target(&mut self, members: &MemberList) -> Option<NodeId> {
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

        Some(target_id)
    }

    /// Pick `k` random relay nodes (excluding `target`).
    fn pick_relays(&self, members: &MemberList, target: NodeId) -> Vec<NodeId> {
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
            relays.push(alive[idx].node_id);
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

        if let Some(target) = self.pick_probe_target(&MemberList::clone_shallow(members)) {
            // Record this probe target in history
            if self.recent_targets.len() >= PROBE_HISTORY_SIZE {
                self.recent_targets.pop_front();
            }
            self.recent_targets.push_back(target);

            let seq = self.next_sequence();
            actions.push(SwimAction::SendPing {
                to: target,
                sequence: seq,
            });
            self.phase = ProbePhase::WaitingDirectAck {
                target,
                sequence: seq,
                sent_at: self.tick,
            };
        }
    }

    fn check_probe_timeout(&mut self, members: &MemberList, actions: &mut Vec<SwimAction>) {
        match &self.phase {
            ProbePhase::WaitingDirectAck { target, sequence, sent_at } => {
                if self.tick - sent_at >= self.config.probe_timeout {
                    let target = *target;
                    let sequence = *sequence;

                    // Send indirect probes through relays
                    let relays = self.pick_relays(members, target);
                    for relay in relays {
                        actions.push(SwimAction::SendPingReq {
                            relay,
                            target,
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
        if let ProbePhase::WaitingIndirectAck { target: expected, sequence: expected_seq, .. } = &self.phase {
            if target == *expected && sequence == *expected_seq {
                self.cancel_suspicion_timer(target);
                self.phase = ProbePhase::Idle;
            }
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
            // Only declare dead if still suspect. A refutation (Alive with
            // higher incarnation) clears the suspect state in MemberList;
            // honour that by dropping the stale timer instead of killing the node.
            let still_suspect = members
                .get(&node_id)
                .is_some_and(|e| e.state == MemberState::Suspect);

            if still_suspect && members.declare_dead(node_id) {
                actions.push(SwimAction::DeclareDead(node_id));
                // If we're currently probing the dead node, cancel immediately
                // so we can probe live members on this same tick.
                match &self.phase {
                    ProbePhase::WaitingDirectAck { target, .. }
                    | ProbePhase::WaitingIndirectAck { target, .. }
                        if *target == node_id =>
                    {
                        self.phase = ProbePhase::Idle;
                    }
                    _ => {}
                }
            }
            self.cancel_suspicion_timer(node_id);
        }
    }

    /// Periodically ping a dead node to detect partition heals.
    fn maybe_reprobe_dead(&mut self, members: &MemberList, actions: &mut Vec<SwimAction>) {
        if self.config.dead_reprobe_interval == 0 {
            return;
        }
        if self.tick < self.next_reprobe_tick {
            return;
        }

        self.next_reprobe_tick = self.tick + self.config.dead_reprobe_interval;

        let dead = members.dead_members();
        if dead.is_empty() {
            return;
        }

        let idx = self.reprobe_index % dead.len();
        self.reprobe_index = self.reprobe_index.wrapping_add(1);

        let target = &dead[idx];
        let seq = self.next_sequence();
        actions.push(SwimAction::SendPing {
            to: target.node_id,
            sequence: seq,
        });
    }

    // ─── Reactive mode ─────────────────────────────────────────────────

    /// Enqueue a demand probe for a newly discovered peer (reactive mode only).
    ///
    /// Called when gossip or a join response introduces a new Alive member.
    /// In Periodic mode this is a no-op (periodic probing covers it).
    pub fn enqueue_demand_probe(&mut self, target: NodeId) {
        if matches!(self.config.probe_mode, ProbeMode::Periodic) {
            return;
        }
        if self.is_currently_probing(target) || self.demand_queue.contains(&target) {
            return;
        }
        if self.demand_queue.len() < MAX_DEMAND_QUEUE {
            self.demand_queue.push_back(target);
        }
    }

    /// Handle a send failure: start a probe immediately or queue it.
    fn handle_send_failed(&mut self, target: NodeId, members: &MemberList, actions: &mut Vec<SwimAction>) {
        // Ignore failures for dead peers, self, or already-queued targets
        if let Some(entry) = members.get(&target) {
            if entry.state == MemberState::Dead {
                return;
            }
        } else {
            // Unknown peer — nothing to probe
            return;
        }

        // Ignore if we're already probing this target
        if self.is_currently_probing(target) {
            return;
        }

        // Ignore if already in demand queue
        if self.demand_queue.contains(&target) {
            return;
        }

        if matches!(self.phase, ProbePhase::Idle) {
            // Start probe immediately
            self.start_probe_for(target, actions);
        } else {
            // Queue it (capped)
            if self.demand_queue.len() < MAX_DEMAND_QUEUE {
                self.demand_queue.push_back(target);
            }
        }
    }

    /// Start a directed probe to a specific target.
    fn start_probe_for(&mut self, target: NodeId, actions: &mut Vec<SwimAction>) {
        if self.recent_targets.len() >= PROBE_HISTORY_SIZE {
            self.recent_targets.pop_front();
        }
        self.recent_targets.push_back(target);

        let seq = self.next_sequence();
        actions.push(SwimAction::SendPing {
            to: target,
            sequence: seq,
        });
        self.phase = ProbePhase::WaitingDirectAck {
            target,
            sequence: seq,
            sent_at: self.tick,
        };
    }

    /// On each tick in reactive mode, if idle and queue non-empty, pop and probe.
    fn maybe_start_demand_probe(&mut self, _members: &MemberList, actions: &mut Vec<SwimAction>) {
        if !matches!(self.phase, ProbePhase::Idle) {
            return;
        }
        if let Some(target) = self.demand_queue.pop_front() {
            self.start_probe_for(target, actions);
        }
    }

    /// At safety_sweep_interval, probe one random alive member.
    fn maybe_safety_sweep(&mut self, members: &MemberList, actions: &mut Vec<SwimAction>) {
        if self.tick < self.next_sweep_tick {
            return;
        }
        let interval = match &self.config.probe_mode {
            ProbeMode::Reactive { safety_sweep_interval } => *safety_sweep_interval,
            ProbeMode::Periodic => return,
        };
        self.next_sweep_tick = self.tick + interval;

        if !matches!(self.phase, ProbePhase::Idle) {
            return;
        }

        if let Some(target) = self.pick_probe_target(&MemberList::clone_shallow(members)) {
            self.start_probe_for(target, actions);
        }
    }

    /// Check if we are currently probing a specific target.
    fn is_currently_probing(&self, target: NodeId) -> bool {
        match &self.phase {
            ProbePhase::WaitingDirectAck { target: t, .. }
            | ProbePhase::WaitingIndirectAck { target: t, .. } => *t == target,
            ProbePhase::Idle => false,
        }
    }
}

// Helper: we need a read-only borrow of members in pick_probe_target
// while also having &mut self. Use a shallow clone pattern.
impl MemberList {
    /// Cheap snapshot of just the IDs for probe target selection.
    fn clone_shallow(original: &MemberList) -> MemberList {
        let mut copy = MemberList::new(original.self_id());
        for entry in original.all_members() {
            copy.apply(entry.node_id, entry.state, entry.incarnation);
        }
        copy
    }
}
