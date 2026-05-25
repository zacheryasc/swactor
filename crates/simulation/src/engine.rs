//! The engine (SIM_SPEC §4).
//!
//! Owns the virtual clock, the scheduling queue, the host table, the
//! network, and the bundle writer. Pops one event at a time, advances
//! the clock to the event's time, dispatches by kind, and processes
//! returned host actions in order.
//!
//! The engine never reads any clock other than the popped event's
//! `virtual_time_ns`. It never re-orders host actions. It treats
//! message bytes as opaque.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use crate::bundle::{
    BundleRecord, BundleWriter, DeliveryDropReason, EventPayload, EventRecord, MutationRecord,
    SnapshotRecord,
};
use crate::evaluator::{EventLine, SnapshotEntry, StreamingEvaluator};
use crate::host::{Action, Host, HostFactory, HostId, HostMessage};
use crate::network::{DeliveryId, Network, NetworkNotification, SendOutcome};
use crate::rng::{SubstreamKey, SubstreamRng};
use crate::scenario::{Mutation, MutationKind, Scenario};

// ──────────────────────────────────────────────────────────────────────
// Public surface
// ──────────────────────────────────────────────────────────────────────

/// A fatal violation: the host produced something the engine cannot
/// route. Distinct from a routine drop or refused send; the run aborts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineAbort {
    /// A host referenced an id the engine has never heard of.
    UnknownDestination { from: String, to: String },
    /// A host attempted to send to itself, which the spec leaves
    /// undefined and we refuse.
    SelfSend { host: String },
    /// RELAY_SPEC §5.3 — a `WorkerExit` mutation targeted a host of a
    /// kind that does not accept the envelope. The MVP only the
    /// `stage` host kind accepts `WorkerExit`; anything else aborts
    /// rather than silently dropping the envelope (the entire point
    /// of the mutation being explicit is that targeting the wrong
    /// kind should be loud).
    WorkerExitOnWrongKind {
        peer: String,
        kind: String,
        mutation_index: usize,
    },
}

/// Termination cause; tests inspect this to verify §4.8 / §4.10.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminationReason {
    DurationReached,
    EarlyAllAssertionsResolved,
    Aborted(EngineAbort),
}

pub struct Engine<W: BundleWriter> {
    network: Network,
    writer: W,
    hosts: BTreeMap<HostId, Box<dyn Host>>,
    /// Hosts that returned `Action::Halt`. Per §4.6 Halt suppresses
    /// further ticks but recv still flows.
    halted: BTreeMap<HostId, bool>,
    /// Hosts a `PeerKill` mutation has stopped. Per §5.5 a killed
    /// peer's ticks are stopped, its in-flight deliveries are
    /// invalidated, and the engine treats it as not-live (no
    /// snapshots, no future deliveries) until a `PeerResurrect`
    /// brings it back.
    killed: BTreeSet<HostId>,
    /// Hosts whose `recv` returned `Action::Halt`. Per §4.6 "Inbound
    /// recv still flows … *until the host's `recv` itself returns
    /// `Halt`*" — i.e. Halt-from-recv terminates recv flow too,
    /// while Halt-from-tick only suppresses ticks.
    recv_halted: BTreeSet<HostId>,
    tick_period_ns: BTreeMap<HostId, u64>,
    /// Per-peer construction specs the engine retains so it can
    /// rebuild a host via the registered factory on a
    /// `PeerResurrect { preserve_state: false }` mutation.
    peer_specs: BTreeMap<HostId, PeerSpec>,
    /// Roster of every declared peer id, kept in scenario order so
    /// each freshly built host gets the full neighbour list.
    peer_roster: Vec<HostId>,
    /// Per-kind host factories. None registered ⇒ tests
    /// pre-install hosts via `install_host`. Both modes coexist.
    factories: BTreeMap<&'static str, Box<dyn HostFactory>>,
    queue: BinaryHeap<Reverse<QueueEntry>>,
    next_seq: u64,
    /// Delivery ids the engine has already invalidated; deliveries
    /// referencing them are dropped at the mutation's time.
    invalidated: BTreeMap<DeliveryId, DeliveryDropReason>,
    now_ns: u64,
    duration_ns: u64,
    /// Optional bound on how many pops to make; tests use it to
    /// guard against runaway loops. None ⇒ unbounded.
    pop_budget: Option<u64>,
    /// §10.4 streaming evaluator — fed the same record stream the
    /// bundle writer sees, so the engine can ask `all_resolved()`
    /// for §4.8 early-termination. None ⇒ early termination is off.
    streaming: Option<StreamingEvaluator>,
    /// Tracks whether `scenario.early_terminate_on_all_assertions_resolved`
    /// is set; the engine only honours the streaming evaluator's
    /// `all_resolved` when this is true.
    early_terminate_on_resolved: bool,
    /// Monotonic line counter the streaming evaluator uses to label
    /// EventLines. Mirrors the bundle writer's ordering rule
    /// in spirit (the engine emits records in dispatch order; the
    /// streaming side sees them in that same order).
    next_event_line_idx: usize,
    /// Per-host monotonic snapshot counter for the streaming side.
    next_snapshot_seq: BTreeMap<HostId, u32>,
}

/// Whether a Vec<Action> came from a host's `tick` or its `recv`.
/// §4.6 differentiates Halt behaviour by source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActionSource {
    Tick,
    Recv,
}

#[derive(Debug, Clone)]
struct PeerSpec {
    kind: String,
    kind_config: toml::value::Table,
    tick_period_ns: u64,
}

impl<W: BundleWriter> Engine<W> {
    pub fn new(scenario: &Scenario, network: Network, writer: W) -> Self {
        let hosts: BTreeMap<HostId, Box<dyn Host>> = BTreeMap::new();
        let mut halted = BTreeMap::new();
        let mut tick_period_ns = BTreeMap::new();
        let mut peer_specs = BTreeMap::new();
        let mut peer_roster = Vec::new();
        for peer in &scenario.peers {
            let period = peer
                .tick_period_ns_override
                .unwrap_or(scenario.default_tick.period_ns);
            tick_period_ns.insert(peer.id.clone(), period);
            halted.insert(peer.id.clone(), false);
            peer_specs.insert(
                peer.id.clone(),
                PeerSpec {
                    kind: peer.kind.clone(),
                    kind_config: peer.kind_config.clone(),
                    tick_period_ns: period,
                },
            );
            peer_roster.push(peer.id.clone());
        }
        let mut e = Self {
            network,
            writer,
            hosts,
            halted,
            killed: BTreeSet::new(),
            recv_halted: BTreeSet::new(),
            tick_period_ns,
            peer_specs,
            peer_roster,
            factories: BTreeMap::new(),
            queue: BinaryHeap::new(),
            next_seq: 0,
            invalidated: BTreeMap::new(),
            now_ns: 0,
            duration_ns: scenario.duration_ns,
            pop_budget: None,
            streaming: None,
            early_terminate_on_resolved: scenario.early_terminate_on_all_assertions_resolved,
            next_event_line_idx: 0,
            next_snapshot_seq: BTreeMap::new(),
        };
        // §4.1 pre-population: tick per host (with offset), mutation per
        // scenario mutation, snapshot per scenario snapshot, terminate at
        // duration_ns.
        for peer in &scenario.peers {
            let offset = tick_offset_ns(
                scenario.seed,
                &peer.id,
                e.tick_period_ns[&peer.id],
            );
            e.enqueue(offset, EventKind::Tick { host: peer.id.clone() });
        }
        for (idx, m) in scenario.mutations.iter().enumerate() {
            e.enqueue(
                m.at_ns,
                EventKind::Mutation {
                    mutation: m.clone(),
                    index: idx,
                },
            );
        }
        for s in &scenario.snapshots {
            e.enqueue(s.at_ns, EventKind::Snapshot);
        }
        e.enqueue(scenario.duration_ns, EventKind::Terminate);
        e
    }

    /// Install a host instance. Hosts must be installed before `run`
    /// is called; once `run` starts the host table is frozen *unless*
    /// a `PeerResurrect { preserve_state: false }` mutation fires and
    /// a factory is registered for the host's kind.
    pub fn install_host(&mut self, host: Box<dyn Host>) {
        self.hosts.insert(host.id().to_string(), host);
    }

    /// Register a host factory for a kind tag. The engine uses it on
    /// `auto_install_hosts` and on `PeerResurrect { preserve_state:
    /// false }` to (re)build a host instance from the scenario's
    /// `peer.kind_config`.
    pub fn register_factory(&mut self, factory: Box<dyn HostFactory>) {
        self.factories.insert(factory.kind_tag(), factory);
    }

    /// Build a fresh host instance for every declared peer whose kind
    /// has a registered factory. Tests with manually installed stub
    /// hosts skip this; integration code calls it once after
    /// `Engine::new` + `register_factory`.
    pub fn auto_install_hosts(&mut self) {
        let specs: Vec<(HostId, PeerSpec)> = self
            .peer_specs
            .iter()
            .map(|(id, spec)| (id.clone(), spec.clone()))
            .collect();
        for (id, spec) in specs {
            if self.hosts.contains_key(&id) {
                continue;
            }
            if let Some(factory) = self.factories.get(spec.kind.as_str()) {
                let host = factory.build(
                    &id,
                    &spec.kind_config,
                    &self.peer_roster,
                    spec.tick_period_ns,
                );
                self.hosts.insert(id, host);
            }
        }
    }

    pub fn set_pop_budget(&mut self, n: u64) {
        self.pop_budget = Some(n);
    }

    /// Install a §10.4 streaming evaluator. The engine forwards every
    /// record it writes into the evaluator and, if the scenario set
    /// `early_terminate_on_all_assertions_resolved = true`, halts the
    /// main loop as soon as every assertion is resolved (§4.8).
    pub fn enable_streaming(&mut self, evaluator: StreamingEvaluator) {
        self.streaming = Some(evaluator);
    }

    /// Forward a `BundleRecord` to both the bundle writer and (if
    /// enabled) the streaming evaluator. Every code path in the
    /// engine writes records through here.
    fn write_record(&mut self, rec: BundleRecord) {
        if let Some(streamer) = self.streaming.as_mut() {
            match &rec {
                BundleRecord::Event(e) => {
                    let line = EventLine::from_event_record(e, self.next_event_line_idx);
                    streamer.feed_event(line);
                    self.next_event_line_idx += 1;
                }
                BundleRecord::Mutation(m) => {
                    let line = EventLine::from_mutation_record(m, self.next_event_line_idx);
                    streamer.feed_event(line);
                    self.next_event_line_idx += 1;
                }
                BundleRecord::Snapshot(s) => {
                    let seq = self
                        .next_snapshot_seq
                        .entry(s.host_id.clone())
                        .or_insert(0);
                    let entry = SnapshotEntry::from_snapshot_record(s, *seq);
                    *seq += 1;
                    streamer.feed_snapshot(s.host_id.clone(), entry);
                }
            }
        }
        self.writer.write(rec);
    }

    pub fn writer(&self) -> &W {
        &self.writer
    }

    pub fn into_writer(self) -> W {
        self.writer
    }

    pub fn now_ns(&self) -> u64 {
        self.now_ns
    }

    /// Drive the main loop until `Terminate` pops (or the pop budget
    /// runs out). Returns the termination cause.
    pub fn run(&mut self) -> TerminationReason {
        let mut pops = 0u64;
        loop {
            if let Some(b) = self.pop_budget {
                if pops >= b {
                    return TerminationReason::DurationReached;
                }
            }
            pops += 1;
            let Some(Reverse(entry)) = self.queue.pop() else {
                return TerminationReason::DurationReached;
            };
            self.now_ns = entry.time_ns;
            match entry.kind {
                EventKind::Terminate => {
                    return TerminationReason::DurationReached;
                }
                EventKind::Tick { host } => {
                    if let Err(abort) = self.dispatch_tick(host) {
                        return TerminationReason::Aborted(abort);
                    }
                }
                EventKind::Deliver { from, to, delivery_id, encoded } => {
                    if let Some(reason) = self.invalidated.remove(&delivery_id) {
                        // Already accounted for at the mutation's time
                        // via DropOnDelivery; do not deliver.
                        let _ = reason;
                        continue;
                    }
                    if let Err(abort) = self.dispatch_deliver(from, to, delivery_id, encoded) {
                        return TerminationReason::Aborted(abort);
                    }
                }
                EventKind::LocalRecv { to, message } => {
                    if let Err(abort) = self.dispatch_local_recv(to, message) {
                        return TerminationReason::Aborted(abort);
                    }
                }
                EventKind::Mutation { mutation, index } => {
                    if let Err(abort) = self.dispatch_mutation(mutation, index) {
                        return TerminationReason::Aborted(abort);
                    }
                }
                EventKind::Snapshot => self.dispatch_snapshot(),
            }
            // §4.8 early termination — after every dispatch, ask the
            // streaming evaluator whether every assertion is resolved.
            // Only honour the answer when the scenario opted in.
            if self.early_terminate_on_resolved {
                if let Some(streamer) = self.streaming.as_ref() {
                    if streamer.all_resolved() {
                        return TerminationReason::EarlyAllAssertionsResolved;
                    }
                }
            }
        }
    }

    // ── Dispatch ────────────────────────────────────────────────────

    fn dispatch_tick(&mut self, host_id: HostId) -> Result<(), EngineAbort> {
        let period = *self
            .tick_period_ns
            .get(&host_id)
            .expect("tick scheduled for unknown host");
        // §4.3 — schedule next tick after dispatch so action-emitted
        // events at `now_ns` precede the next tick at
        // `now_ns + period`. Halt (§4.6) and PeerKill (§5.5) both
        // suppress the call; PeerKill additionally stops the next
        // tick from being scheduled until PeerResurrect.
        let killed = self.killed.contains(&host_id);
        let halted = *self.halted.get(&host_id).unwrap_or(&false);
        if !killed && !halted {
            let now = self.now_ns;
            let Some(mut host) = self.hosts.remove(&host_id) else {
                // Host installed only by id table; if missing, treat as halted.
                self.enqueue(now.saturating_add(period), EventKind::Tick { host: host_id });
                return Ok(());
            };
            let actions = host.tick(now);
            self.hosts.insert(host_id.clone(), host);
            self.process_actions(&host_id, actions, ActionSource::Tick)?;
        }
        if killed {
            // §5.5 — no further ticks until resurrected; the
            // PeerResurrect handler re-arms one.
            return Ok(());
        }
        let next = self.now_ns.saturating_add(period);
        if next < self.duration_ns {
            self.enqueue(next, EventKind::Tick { host: host_id });
        }
        Ok(())
    }

    fn dispatch_deliver(
        &mut self,
        from: HostId,
        to: HostId,
        delivery_id: DeliveryId,
        encoded: Vec<u8>,
    ) -> Result<(), EngineAbort> {
        let now = self.now_ns;
        // §4.4 — if destination is killed (PeerKill) or has terminated
        // recv via a Halt-from-recv (§4.6 "until the host's recv itself
        // returns Halt"), drop and emit DropOnDelivery. Action::Halt
        // from `tick` does NOT suppress recv.
        if self.killed.contains(&to) {
            self.write_record(BundleRecord::Event(EventRecord {
                virtual_time_ns: now,
                host_id: Some(to.clone()),
                kind_tag: "engine".into(),
                event: EventPayload::DropOnDelivery {
                    to: to.clone(),
                    reason: DeliveryDropReason::HostKilled,
                },
            }));
            // Tell the network this delivery is done so it doesn't
            // sit in `in_flight` forever.
            self.network.notify_delivered(&from, &to, delivery_id);
            return Ok(());
        }
        if self.recv_halted.contains(&to) {
            self.write_record(BundleRecord::Event(EventRecord {
                virtual_time_ns: now,
                host_id: Some(to.clone()),
                kind_tag: "engine".into(),
                event: EventPayload::DropOnDelivery {
                    to: to.clone(),
                    reason: DeliveryDropReason::HostHalted,
                },
            }));
            self.network.notify_delivered(&from, &to, delivery_id);
            return Ok(());
        }
        let Some(mut host) = self.hosts.remove(&to) else {
            self.write_record(BundleRecord::Event(EventRecord {
                virtual_time_ns: now,
                host_id: Some(to.clone()),
                kind_tag: "engine".into(),
                event: EventPayload::DropOnDelivery {
                    to: to.clone(),
                    reason: DeliveryDropReason::HostHalted,
                },
            }));
            self.network.notify_delivered(&from, &to, delivery_id);
            return Ok(());
        };
        let actions = host.recv(HostMessage::App(encoded), now);
        self.hosts.insert(to.clone(), host);
        // §3.2 invariant: the network's in_flight list must not
        // include deliveries that have already been processed.
        self.network.notify_delivered(&from, &to, delivery_id);
        self.process_actions(&to, actions, ActionSource::Recv)
    }

    fn dispatch_local_recv(
        &mut self,
        to: HostId,
        message: HostMessage,
    ) -> Result<(), EngineAbort> {
        let now = self.now_ns;
        // Local recv (timer / send-failed) does not touch the network.
        // PeerKill (§5.5) and Halt-from-recv (§4.6) both suppress recv.
        if self.killed.contains(&to) || self.recv_halted.contains(&to) {
            return Ok(());
        }
        let Some(mut host) = self.hosts.remove(&to) else {
            return Ok(());
        };
        let actions = host.recv(message, now);
        self.hosts.insert(to.clone(), host);
        self.process_actions(&to, actions, ActionSource::Recv)
    }

    fn dispatch_mutation(
        &mut self,
        mutation: Mutation,
        index: usize,
    ) -> Result<(), EngineAbort> {
        let at = self.now_ns;
        // RELAY_SPEC §5.3 / §6.1 — `WorkerExit` is special-cased:
        // it is a mutation whose effect is to deliver a `recv`
        // envelope to a host, not to mutate the network. Validate
        // the target kind here so the abort surfaces before any
        // record is written.
        if let MutationKind::WorkerExit {
            peer,
            reason,
            status_code,
            signal,
        } = &mutation.kind
        {
            let spec_kind = self
                .peer_specs
                .get(peer)
                .map(|s| s.kind.as_str())
                .unwrap_or("");
            if spec_kind != "stage" {
                return Err(EngineAbort::WorkerExitOnWrongKind {
                    peer: peer.clone(),
                    kind: spec_kind.to_string(),
                    mutation_index: index,
                });
            }
            // Record the mutation itself (mirrors the parent's §4.5
            // emission). The WorkerExit envelope is dispatched
            // *synchronously* — i.e. the host's `recv` is called
            // before this method returns, and the recorded events
            // reach the bundle writer before the main loop has a
            // chance to pop anything else at the same virtual time.
            //
            // The synchronous dispatch is normative per §6A.3:
            // "the event must reach the bundle writer before the
            // engine acts on the halt." With an enqueued LocalRecv,
            // a `Terminate` whose sequence number was assigned at
            // construction (and so lower than the just-now-enqueued
            // LocalRecv) would pop first and the worker_exited
            // event would be lost — that is exactly the boundary
            // failure §6A.6 names as "Event-before-halt is
            // observable."
            self.write_record(BundleRecord::Mutation(MutationRecord {
                virtual_time_ns: at,
                mutation: mutation.clone(),
            }));
            let envelope = HostMessage::WorkerExit {
                reason: reason.clone(),
                status_code: *status_code,
                signal: *signal,
            };
            // Mirror dispatch_local_recv's pre-checks: PeerKill and
            // Halt-from-recv both suppress recv.
            if self.killed.contains(peer) || self.recv_halted.contains(peer) {
                return Ok(());
            }
            if let Some(mut host) = self.hosts.remove(peer) {
                let actions = host.recv(envelope, at);
                self.hosts.insert(peer.clone(), host);
                return self.process_actions(peer, actions, ActionSource::Recv);
            }
            return Ok(());
        }
        let invalidated = self.network.apply_mutation(&mutation, at);
        // The drop reason for invalidated deliveries depends on which
        // mutation invalidated them. Partition → `Partition`,
        // PeerKill → `HostKilled`, RelayKill → `HostKilled` (the
        // outbound peer never got the message); nothing else
        // invalidates deliveries today.
        let drop_reason = match &mutation.kind {
            MutationKind::Partition { .. } => DeliveryDropReason::Partition,
            MutationKind::PeerKill { .. } => DeliveryDropReason::HostKilled,
            _ => DeliveryDropReason::HostKilled,
        };
        // §5.5 — PeerKill / PeerResurrect are engine-observable
        // events: the engine, not the network, decides whether the
        // host receives ticks and snapshots.
        match &mutation.kind {
            MutationKind::PeerKill { peer } => {
                self.killed.insert(peer.clone());
            }
            MutationKind::PeerResurrect { peer, preserve_state } => {
                // §4.3 calls PeerResurrect the *only* mechanism to
                // un-halt. So clear kill and both halt-flavours;
                // re-arm the cadence if the peer was suppressed
                // under any flag.
                let was_killed = self.killed.remove(peer);
                let was_halted = self
                    .halted
                    .get(peer)
                    .copied()
                    .unwrap_or(false);
                if was_halted {
                    self.halted.insert(peer.clone(), false);
                }
                let was_recv_halted = self.recv_halted.remove(peer);
                let was_halted = was_halted || was_recv_halted;
                // §5.5 — `preserve_state = false` means rebuild the
                // host from its scenario declaration. If no factory
                // is registered for the host's kind, the engine
                // leaves the existing instance alone and records the
                // omission via the mutation record (the caller can
                // detect it by reading the bundle).
                if !*preserve_state {
                    let rebuilt = self
                        .peer_specs
                        .get(peer)
                        .cloned()
                        .and_then(|spec| {
                            self.factories.get(spec.kind.as_str()).map(|f| {
                                f.build(
                                    peer,
                                    &spec.kind_config,
                                    &self.peer_roster,
                                    spec.tick_period_ns,
                                )
                            })
                        });
                    if let Some(host) = rebuilt {
                        self.hosts.insert(peer.clone(), host);
                    }
                }
                if was_killed || was_halted {
                    if let Some(period) = self.tick_period_ns.get(peer).copied() {
                        let next = at.saturating_add(period);
                        if next < self.duration_ns {
                            self.enqueue(next, EventKind::Tick { host: peer.clone() });
                        }
                    }
                }
            }
            _ => {}
        }
        // Record the mutation itself.
        self.write_record(BundleRecord::Mutation(MutationRecord {
            virtual_time_ns: at,
            mutation,
        }));
        // For each invalidated delivery: emit DropOnDelivery now,
        // remember the delivery id so the actual Deliver pop is
        // skipped.
        for inv in invalidated {
            self.invalidated.insert(inv.delivery_id, drop_reason);
            self.write_record(BundleRecord::Event(EventRecord {
                virtual_time_ns: at,
                host_id: Some(inv.to.clone()),
                kind_tag: "engine".into(),
                event: EventPayload::DropOnDelivery {
                    to: inv.to,
                    reason: drop_reason,
                },
            }));
        }
        self.drain_network_notifications();
        Ok(())
    }

    fn dispatch_snapshot(&mut self) {
        let at = self.now_ns;
        // §4.5: snapshots ask "every live host". §7.3: iterate in
        // BTreeMap (key-sorted) order. Killed peers are not live.
        let ids: Vec<HostId> = self
            .hosts
            .keys()
            .filter(|id| !self.killed.contains(*id))
            .cloned()
            .collect();
        for id in ids {
            let kind_tag = self.hosts[&id].kind_tag().to_string();
            let bytes = self.hosts[&id].snapshot();
            self.write_record(BundleRecord::Snapshot(SnapshotRecord {
                virtual_time_ns: at,
                host_id: id,
                kind_tag,
                snapshot: bytes,
            }));
        }
    }

    // ── Action processing ───────────────────────────────────────────

    fn process_actions(
        &mut self,
        host_id: &str,
        actions: Vec<Action>,
        source: ActionSource,
    ) -> Result<(), EngineAbort> {
        for action in actions {
            match action {
                Action::Send { to, encoded } => {
                    self.process_send(host_id, &to, encoded)?;
                }
                Action::RecordEvent { kind_tag, event } => {
                    self.write_record(BundleRecord::Event(EventRecord {
                        virtual_time_ns: self.now_ns,
                        host_id: Some(host_id.to_string()),
                        kind_tag,
                        event: EventPayload::Bytes(event),
                    }));
                }
                Action::ScheduleTimer { at_ns, token } => {
                    self.enqueue(
                        at_ns,
                        EventKind::LocalRecv {
                            to: host_id.to_string(),
                            message: HostMessage::TimerFired { token },
                        },
                    );
                }
                Action::Halt => {
                    self.halted.insert(host_id.to_string(), true);
                    // §4.6 "until the host's recv itself returns
                    // Halt": Halt-from-recv terminates recv flow
                    // too. Halt-from-tick only suppresses ticks.
                    if source == ActionSource::Recv {
                        self.recv_halted.insert(host_id.to_string());
                    }
                }
            }
        }
        Ok(())
    }

    fn process_send(
        &mut self,
        from: &str,
        to: &str,
        encoded: Vec<u8>,
    ) -> Result<(), EngineAbort> {
        if from == to {
            return Err(EngineAbort::SelfSend {
                host: from.to_string(),
            });
        }
        if !self.tick_period_ns.contains_key(to) {
            return Err(EngineAbort::UnknownDestination {
                from: from.to_string(),
                to: to.to_string(),
            });
        }
        let byte_len = encoded.len() as u64;
        let outcome = self.network.send(from, to, byte_len, self.now_ns);
        self.drain_network_notifications();
        match outcome {
            SendOutcome::Arrive { delivery_id, at_ns } => {
                self.enqueue(
                    at_ns,
                    EventKind::Deliver {
                        from: from.to_string(),
                        to: to.to_string(),
                        delivery_id,
                        encoded,
                    },
                );
            }
            SendOutcome::Drop { reason } => {
                self.write_record(BundleRecord::Event(EventRecord {
                    virtual_time_ns: self.now_ns,
                    host_id: Some(from.to_string()),
                    kind_tag: "engine".into(),
                    event: EventPayload::DropOnSend {
                        from: from.to_string(),
                        to: to.to_string(),
                        reason,
                    },
                }));
                // SendFailed flows back through the sender's recv at
                // current virtual time; not a network delivery, so
                // no notify_delivered hookup.
                self.enqueue(
                    self.now_ns,
                    EventKind::LocalRecv {
                        to: from.to_string(),
                        message: HostMessage::SendFailed {
                            to: to.to_string(),
                            reason,
                        },
                    },
                );
            }
        }
        Ok(())
    }

    fn drain_network_notifications(&mut self) {
        let notes = self.network.take_pending_notifications();
        for note in notes {
            let record = match note {
                NetworkNotification::CacheStateChange { from, to, at_ns, transition } => {
                    EventRecord {
                        virtual_time_ns: at_ns,
                        host_id: Some(from.clone()),
                        kind_tag: "engine".into(),
                        event: EventPayload::CacheStateChange { from, to, transition },
                    }
                }
                NetworkNotification::DialStart { from, to, at_ns } => EventRecord {
                    virtual_time_ns: at_ns,
                    host_id: Some(from.clone()),
                    kind_tag: "engine".into(),
                    event: EventPayload::DialStart { from, to },
                },
                NetworkNotification::DialOutcome { from, to, at_ns, warm } => EventRecord {
                    virtual_time_ns: at_ns,
                    host_id: Some(from.clone()),
                    kind_tag: "engine".into(),
                    event: EventPayload::DialOutcome { from, to, warm },
                },
                NetworkNotification::RelayEnqueue { relay, from, to, byte_len, at_ns } => {
                    EventRecord {
                        virtual_time_ns: at_ns,
                        host_id: None,
                        kind_tag: "relay".into(),
                        event: EventPayload::RelayEnqueue { relay, from, to, byte_len },
                    }
                }
                NetworkNotification::RelayDequeue { relay, from, to, byte_len, at_ns } => {
                    EventRecord {
                        virtual_time_ns: at_ns,
                        host_id: None,
                        kind_tag: "relay".into(),
                        event: EventPayload::RelayDequeue { relay, from, to, byte_len },
                    }
                }
                NetworkNotification::RelayDrop {
                    relay,
                    from,
                    to,
                    byte_len,
                    reason,
                    at_ns,
                } => EventRecord {
                    virtual_time_ns: at_ns,
                    host_id: None,
                    kind_tag: "relay".into(),
                    event: EventPayload::RelayDrop { relay, from, to, byte_len, reason },
                },
            };
            self.write_record(BundleRecord::Event(record));
        }
    }

    // ── Queue plumbing ──────────────────────────────────────────────

    fn enqueue(&mut self, time_ns: u64, kind: EventKind) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.queue.push(Reverse(QueueEntry { time_ns, seq, kind }));
    }
}

// ──────────────────────────────────────────────────────────────────────
// Internals
// ──────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct QueueEntry {
    time_ns: u64,
    seq: u64,
    kind: EventKind,
}

impl PartialEq for QueueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.time_ns == other.time_ns && self.seq == other.seq
    }
}

impl Eq for QueueEntry {}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time_ns
            .cmp(&other.time_ns)
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
enum EventKind {
    Tick {
        host: HostId,
    },
    /// A real network delivery. `from` and `to` are the route; the
    /// engine calls `network.notify_delivered(from, to, delivery_id)`
    /// after `recv` returns so the network can drop the message from
    /// its in-flight list.
    Deliver {
        from: HostId,
        to: HostId,
        delivery_id: DeliveryId,
        encoded: Vec<u8>,
    },
    /// A non-network host inbox: timer firings and send-failure
    /// envelopes. No network bookkeeping happens for these.
    LocalRecv {
        to: HostId,
        message: HostMessage,
    },
    Mutation {
        mutation: Mutation,
        index: usize,
    },
    Snapshot,
    Terminate,
}

/// Compute the host's tick offset: a deterministic value in
/// `[0, period)` derived from the scenario seed and the host id, so
/// each host's first tick lands at a stable instant different from
/// (almost) every other host's first tick.
pub(crate) fn tick_offset_ns(seed: u64, host_id: &str, period_ns: u64) -> u64 {
    if period_ns == 0 {
        return 0;
    }
    let mut rng = SubstreamRng::derive(
        seed,
        &SubstreamKey::Host {
            host_id: host_id.to_string(),
            label: "tick_offset",
        },
    );
    rng.next_u64() % period_ns
}
