//! SWIM probe cycle state machine.
//!
//! Pure function design: `(state, event) → (state, actions)`.
//! No I/O, no timers — the caller drives the clock.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::types::{MemberState, NodeId};

use super::lifeguard::{HealthMultiplier, LifeguardConfig};
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
    /// Safety sweep probes one random member every `safety_sweep_interval`.
    Reactive { safety_sweep_interval: Duration },
}

/// SWIM protocol configuration.
///
/// All durations are wall-clock: the state machine is driven by an `Instant`
/// passed into [`SwimProbe::step`], not a logical tick counter.
#[derive(Debug, Clone)]
pub struct SwimConfig {
    /// Time between probe cycles (used in Periodic mode).
    pub probe_interval: Duration,
    /// Time to wait for a direct ack before sending indirect probes.
    pub probe_timeout: Duration,
    /// Number of indirect probe relays (k in the SWIM paper).
    pub indirect_probes: usize,
    /// How long a node stays in Suspect before being declared Dead.
    pub suspicion_timeout: Duration,
    /// Time between dead-node reprobe attempts. Zero = disabled.
    /// When enabled, periodically pings dead nodes to detect partition heals.
    pub dead_reprobe_interval: Duration,
    /// Probe mode: Periodic (default) or Reactive (probe-on-failure).
    pub probe_mode: ProbeMode,
    /// Lifeguard adaptive-timeout config. The probe state machine
    /// keeps a local health multiplier per node — degraded nodes
    /// (high nack rate) stretch their suspicion timeout per
    /// `HealthMultiplier::dynamic_suspicion_timeout`, reducing
    /// false-Dead declarations on partially-reachable peers.
    /// `None` disables the adaptive path (the suspicion timeout stays
    /// at `suspicion_timeout` regardless of health) — used by tests
    /// and callers that want deterministic timing.
    pub lifeguard: Option<LifeguardConfig>,
}

impl Default for SwimConfig {
    fn default() -> Self {
        // Retuned against the `1779733878` deployment shape. Tick units;
        // the production runtime chooses the tick period.
        //
        // The prior tune calibrated against
        // 60 ms simulated latency. The `1779733878` deployment ran
        // entirely over relay-mediated paths with tier-2 RTTs of
        // 181–405 ms; the 0.3 s wall-clock probe budget the prior
        // defaults gave production (15 ticks × 20 ms tick) was below
        // the legitimate-probe-RTT p99 and produced 1701
        // `SwimTransition` events in a 7-minute run.
        //
        // The retune's calibration scenario
        // (`scenarios/calibration/n3_1779733878_repro.toml`) at the
        // chosen operating point produces 158 transitions in the
        // same 7-minute window — a 10× collapse against the §5.3
        // target of <300. The detection time (probe_timeout +
        // suspicion_timeout = 3000 ticks ≈ 60 s at the production
        // runtime's 20 ms tick) sits well under the 7-minute
        // operator deadstop budget the postmortem named.
        //
        // - `probe_interval = 10` ticks (unchanged): the protocol
        //   period is not load-bearing in the calibration sweep.
        // - `probe_timeout = 750` ticks (15 s at 20 ms tick): exceeds
        //   the deployment's relay-mediated p99 RTT (tier-2 plus a
        //   relay HOL queueing margin) by a factor that absorbs
        //   load-driven spikes per §3.1.
        // - `suspicion_timeout = 2250` ticks (45 s at 20 ms tick):
        //   covers several probe cycles so transient probe failures
        //   do not flap Suspect → Alive → Suspect within the window
        //   per §3.2.
        // - `indirect_probes = 2` (unchanged): the prior tune's §3.3
        //   lower bound; dropping below 2 collapses indirect
        //   coverage.
        // - `dead_reprobe_interval = 50` ticks (unchanged).
        // Wall-clock equivalents of the retuned tick values at the
        // production runtime's 20 ms tick period (see the tick math above):
        // 10 / 750 / 2250 / 50 ticks → 200 ms / 15 s / 45 s / 1 s.
        Self {
            probe_interval: Duration::from_millis(200),
            probe_timeout: Duration::from_secs(15),
            indirect_probes: 2,
            suspicion_timeout: Duration::from_secs(45),
            dead_reprobe_interval: Duration::from_secs(1),
            probe_mode: ProbeMode::Periodic,
            // Lifeguard wiring §3.6 is opt-in (default = None). The
            // adaptive band lives in `LifeguardConfig::default()`;
            // callers that want adaptive timeouts construct
            // `SwimConfig { lifeguard: Some(LifeguardConfig {
            // base_suspicion_timeout: <static>, ... }), .. }`. The
            // Lifeguard calibration scenarios set the adaptive band explicitly
            // so sweep observations are reproducible. The wiring's anti-target
            // (dead-code condition) is met: `dynamic_suspicion_timeout` is
            // consumed by `SwimProbe::check_suspicion_timeouts` when
            // `lifeguard` is `Some`.
            lifeguard: None,
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
    // (No `Refute` action: refutation is NOT a probe action — it lives solely in
    // the §7.1 self-refute gate, `node.rs::apply_membership_update`. The probe
    // engine never refutes; a node never probes or suspects itself, §1 inv. 3.)
    /// Diagnostic-only signal — no protocol effect. The host adapter
    /// translates these into typed `Event` records for coverage 2.6
    /// (per-SWIM-probe RTT). Threading them as a `SwimAction` variant
    /// keeps the probe state machine pure (no emitter handle) while
    /// still letting the caller observe ack/timeout lifecycle without
    /// reaching into private phase state.
    Diag(SwimDiagEvent),
}

/// Diagnostic-only events produced by the probe state machine.
///
/// `kind` is `"direct"` for the direct-phase ack/timeout (i.e. a
/// `SendPing` initiating the probe) and `"indirect"` for the
/// indirect-phase ack/timeout (i.e. a `SendPingReq` fanout). The
/// strings match the `kind` field on `Event::SwimProbeSent` /
/// `SwimProbeAcked` / `SwimProbeTimedOut` so the host adapter is a
/// 1:1 translation.
#[derive(Debug, Clone)]
pub enum SwimDiagEvent {
    /// An ack matched the in-flight probe and the probe is complete.
    ProbeAcked {
        target: NodeId,
        sequence: u64,
        kind: &'static str,
    },
    /// The configured budget elapsed before the in-flight probe got
    /// its ack. `budget_ticks` is the configured `probe_timeout`, now
    /// reported in **milliseconds** (the protocol clock is wall-clock,
    /// not ticks); the field name is retained for diagnostics wire
    /// compatibility.
    ProbeTimedOut {
        target: NodeId,
        sequence: u64,
        kind: &'static str,
        budget_ticks: u64,
    },
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
        sent_at: Instant,
    },
    /// Indirect probes sent, waiting for any ack.
    WaitingIndirectAck {
        target: NodeId,
        sequence: u64,
        sent_at: Instant,
    },
}

/// Suspicion timer for a single node.
#[derive(Debug)]
struct SuspicionTimer {
    node_id: NodeId,
    started_at: Instant,
}

/// Maximum demand queue size to prevent unbounded growth.
const MAX_DEMAND_QUEUE: usize = 32;

/// The SWIM probe state machine.
pub struct SwimProbe {
    config: SwimConfig,
    /// Current wall-clock time, refreshed at the top of every `step`. The
    /// state machine stays pure — the caller supplies the clock.
    now: Instant,
    /// Monotonic step counter, used only as a deterministic PRNG seed for
    /// probe-target shuffling and relay rotation (decoupled from the clock).
    step_counter: u64,
    /// When the next probe cycle may fire (Periodic mode). `None` until the
    /// first tick initializes it to `now + probe_interval`.
    next_probe_at: Option<Instant>,
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
    /// When the next dead-node reprobe should fire. `None` until initialized
    /// (also stays `None`-driven when `dead_reprobe_interval` is zero).
    next_reprobe_at: Option<Instant>,
    /// Round-robin index into the dead member list for reprobe target selection.
    reprobe_index: usize,
    /// Peers needing probes due to send failures (reactive mode).
    demand_queue: VecDeque<NodeId>,
    /// When the next safety sweep fires (reactive mode). `None` until initialized.
    next_sweep_at: Option<Instant>,
    /// Adaptive-timeout state per Lifeguard. `None` when
    /// `config.lifeguard` is `None`.
    health: Option<HealthMultiplier>,
}

impl SwimProbe {
    /// Create a probe anchored at wall-clock `now` (the moment the caller
    /// considers "start"). Deadlines are computed relative to `now`, mirroring
    /// the old tick-counter init that anchored at tick 0.
    pub fn new(config: SwimConfig, now: Instant) -> Self {
        let next_reprobe_at = if config.dead_reprobe_interval > Duration::ZERO {
            Some(now + config.dead_reprobe_interval)
        } else {
            None
        };
        let next_sweep_at = match &config.probe_mode {
            ProbeMode::Reactive {
                safety_sweep_interval,
            } => Some(now + *safety_sweep_interval),
            ProbeMode::Periodic => None,
        };
        let next_probe_at = Some(now + config.probe_interval);
        let health = config.lifeguard.clone().map(HealthMultiplier::new);
        Self {
            now,
            step_counter: 0,
            next_probe_at,
            next_reprobe_at,
            next_sweep_at,
            reprobe_index: 0,
            config,
            sequence: 0,
            phase: ProbePhase::Idle,
            probe_index: 0,
            probe_order: Vec::new(),
            suspicion_timers: Vec::new(),
            recent_targets: VecDeque::with_capacity(PROBE_HISTORY_SIZE),
            demand_queue: VecDeque::new(),
            health,
        }
    }

    /// Process an event and produce zero or more actions.
    pub fn step(
        &mut self,
        now: Instant,
        event: SwimEvent,
        members: &mut MemberList,
    ) -> Vec<SwimAction> {
        self.now = now;
        let mut actions = Vec::new();

        match event {
            SwimEvent::Tick => {
                self.step_counter = self.step_counter.wrapping_add(1);
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
                let j = ((self.step_counter as usize)
                    .wrapping_mul(31)
                    .wrapping_add(i))
                    % (i + 1);
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
        let start = if alive.is_empty() {
            0
        } else {
            self.step_counter as usize % alive.len()
        };
        let mut relays = Vec::with_capacity(k);
        for i in 0..k {
            let idx = (start + i) % alive.len();
            relays.push(alive[idx].node_id);
        }
        relays
    }

    fn maybe_start_probe(&mut self, members: &MemberList, actions: &mut Vec<SwimAction>) {
        if let Some(at) = self.next_probe_at
            && self.now < at
        {
            return;
        }
        if !matches!(self.phase, ProbePhase::Idle) {
            return;
        }

        self.next_probe_at = Some(self.now + self.config.probe_interval);

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
                sent_at: self.now,
            };
        }
    }

    fn check_probe_timeout(&mut self, members: &MemberList, actions: &mut Vec<SwimAction>) {
        match &self.phase {
            ProbePhase::WaitingDirectAck {
                target,
                sequence,
                sent_at,
            } => {
                if self.now.saturating_duration_since(*sent_at) >= self.config.probe_timeout {
                    let target = *target;
                    let sequence = *sequence;
                    let budget = self.config.probe_timeout;

                    // The direct phase expired — signal coverage 2.6 first,
                    // then fan out the indirect probes.
                    actions.push(SwimAction::Diag(SwimDiagEvent::ProbeTimedOut {
                        target,
                        sequence,
                        kind: "direct",
                        budget_ticks: budget.as_millis() as u64,
                    }));

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
                        sent_at: self.now,
                    };
                }
            }
            ProbePhase::WaitingIndirectAck {
                target,
                sequence,
                sent_at,
            } => {
                if self.now.saturating_duration_since(*sent_at) >= self.config.probe_timeout {
                    let target = *target;
                    let sequence = *sequence;
                    let budget = self.config.probe_timeout;

                    // Indirect phase expired — coverage 2.6 signal first, then
                    // declare suspect.
                    actions.push(SwimAction::Diag(SwimDiagEvent::ProbeTimedOut {
                        target,
                        sequence,
                        kind: "indirect",
                        budget_ticks: budget.as_millis() as u64,
                    }));

                    // No ack received — suspect this node
                    actions.push(SwimAction::Suspect(target));
                    self.start_suspicion_timer(target);
                    self.phase = ProbePhase::Idle;
                    if let Some(health) = &mut self.health {
                        health.record_nack();
                    }
                }
            }
            ProbePhase::Idle => {}
        }
    }

    fn handle_ack(
        &mut self,
        from: NodeId,
        sequence: u64,
        _members: &mut MemberList,
        actions: &mut Vec<SwimAction>,
    ) {
        let kind = match &self.phase {
            ProbePhase::WaitingDirectAck {
                target,
                sequence: expected,
                ..
            } if from == *target && sequence == *expected => Some("direct"),
            ProbePhase::WaitingIndirectAck {
                target,
                sequence: expected,
                ..
            } if from == *target && sequence == *expected => Some("indirect"),
            _ => None,
        };
        if let Some(kind) = kind {
            // Successful ack — coverage 2.6 signal, cancel suspicion, idle.
            actions.push(SwimAction::Diag(SwimDiagEvent::ProbeAcked {
                target: from,
                sequence,
                kind,
            }));
            self.cancel_suspicion_timer(from);
            self.phase = ProbePhase::Idle;
            if let Some(health) = &mut self.health {
                health.record_ack();
            }
        }
    }

    fn handle_indirect_ack(
        &mut self,
        target: NodeId,
        sequence: u64,
        _members: &mut MemberList,
        actions: &mut Vec<SwimAction>,
    ) {
        if let ProbePhase::WaitingIndirectAck {
            target: expected,
            sequence: expected_seq,
            ..
        } = &self.phase
            && target == *expected
            && sequence == *expected_seq
        {
            actions.push(SwimAction::Diag(SwimDiagEvent::ProbeAcked {
                target,
                sequence,
                kind: "indirect",
            }));
            self.cancel_suspicion_timer(target);
            self.phase = ProbePhase::Idle;
            if let Some(health) = &mut self.health {
                health.record_ack();
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
            started_at: self.now,
        });
    }

    fn cancel_suspicion_timer(&mut self, node_id: NodeId) {
        self.suspicion_timers.retain(|t| t.node_id != node_id);
    }

    fn check_suspicion_timeouts(
        &mut self,
        members: &mut MemberList,
        actions: &mut Vec<SwimAction>,
    ) {
        // Lifeguard §3.6: a degraded local health multiplier stretches
        // the suspect-to-dead window. The clamp band in
        // `LifeguardConfig` keeps a healthy node's effective timeout
        // at `config.suspicion_timeout` and lets a degraded node grow
        // up to the configured max before declaring Dead. The probe
        // state machine is the only consumer; `MemberList` does not
        // know about health.
        let timeout = match &self.health {
            Some(health) => self
                .config
                .suspicion_timeout
                .max(health.dynamic_suspicion_timeout(members.len())),
            None => self.config.suspicion_timeout,
        };
        let now = self.now;
        let expired: Vec<NodeId> = self
            .suspicion_timers
            .iter()
            .filter(|t| now.saturating_duration_since(t.started_at) >= timeout)
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
        if self.config.dead_reprobe_interval == Duration::ZERO {
            return;
        }
        if let Some(at) = self.next_reprobe_at
            && self.now < at
        {
            return;
        }

        self.next_reprobe_at = Some(self.now + self.config.dead_reprobe_interval);

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
    fn handle_send_failed(
        &mut self,
        target: NodeId,
        members: &MemberList,
        actions: &mut Vec<SwimAction>,
    ) {
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
            sent_at: self.now,
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
        if let Some(at) = self.next_sweep_at
            && self.now < at
        {
            return;
        }
        let interval = match &self.config.probe_mode {
            ProbeMode::Reactive {
                safety_sweep_interval,
            } => *safety_sweep_interval,
            ProbeMode::Periodic => return,
        };
        self.next_sweep_at = Some(self.now + interval);

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
