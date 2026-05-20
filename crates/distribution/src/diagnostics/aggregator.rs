//! The single per-process owner of diagnostic state.
//!
//! The aggregator (`DIAGNOSTICS_PLAN.md` A.1) owns:
//! - The reachability log (T1.2) — updated from incoming events.
//! - The monotonic sequence counter used to order intra-node events.
//! - A handle to a [`Sink`] that ships records and snapshots somewhere.
//!
//! Higher tiers add the snapshot timer, the periodic spool drain, and
//! tier-2/3 snapshot fields. This stage adds the wire-shape wrapping
//! ([`EventRecord`]) and the auto-`boot` on construction.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::diagnostics::event::{Event, EventRecord, PeerState};
use crate::diagnostics::identity::Identity;
use crate::diagnostics::reachability::{PeerReachability, StateTransition, node_id_hex};
use crate::diagnostics::sink::{EventEmitter, Sink};
use crate::diagnostics::snapshot::{
    HostIntrospector, IrohIntrospector, ProbeIntrospector, ProcessIntrospector, Snapshot,
    SnapshotBody, SnapshotTrigger, SwimIntrospector, VastaiIntrospector,
};
use crate::types::NodeId;

/// Default cadence for periodic snapshots (`DIAGNOSTICS_PLAN.md` T1.4).
pub const DEFAULT_PERIODIC_INTERVAL: Duration = Duration::from_secs(5);

/// Owns this process's view of its own observable state. Generic over
/// the [`Sink`] so test code can swap in an in-memory sink without
/// touching production paths.
pub struct Aggregator<S: Sink> {
    identity: Identity,
    sink: S,
    seq: AtomicU64,
    snapshot_seq: AtomicU64,
    peers: Mutex<HashMap<String, PeerReachability>>,
    snapshot_on_transition: AtomicBool,
    iroh_introspector: Mutex<Option<Arc<dyn IrohIntrospector>>>,
    swim_introspector: Mutex<Option<Arc<dyn SwimIntrospector>>>,
    host_introspector: Mutex<Option<Arc<dyn HostIntrospector>>>,
    probe_introspector: Mutex<Option<Arc<dyn ProbeIntrospector>>>,
    vastai_introspector: Mutex<Option<Arc<dyn VastaiIntrospector>>>,
    process_introspector: Mutex<Option<Arc<dyn ProcessIntrospector>>>,
}

/// Configuration for the periodic snapshot task spawned by
/// [`spawn_periodic_snapshots`].
///
/// Defaults match the spec — 5s cadence, transition snapshots on.
#[derive(Debug, Clone)]
pub struct PeriodicConfig {
    /// How often to emit a [`SnapshotTrigger::Periodic`] snapshot.
    pub interval: Duration,
    /// If true, every [`Event::SwimTransition`] emit also triggers a
    /// [`SnapshotTrigger::Transition`] snapshot in-line — the local-
    /// transition trigger in T1.4.
    pub snapshot_on_transition: bool,
}

impl Default for PeriodicConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_PERIODIC_INTERVAL,
            snapshot_on_transition: true,
        }
    }
}

impl PeriodicConfig {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            snapshot_on_transition: true,
        }
    }

    pub fn with_transition_snapshots(mut self, enabled: bool) -> Self {
        self.snapshot_on_transition = enabled;
        self
    }
}

impl<S: Sink> Aggregator<S> {
    /// Build an aggregator for this process.
    ///
    /// `identity` is the boot-time record (see [`Identity`]).
    /// `sink` decides where data ends up — `NoopSink` for tests and
    /// existing untouched call sites; `InMemorySink` for tests that
    /// want to assert on the event stream; [`crate::diagnostics::HttpSink`]
    /// for production.
    ///
    /// The aggregator notifies the sink of the identity via
    /// [`Sink::boot`] before returning, so HTTP sinks can POST it
    /// without the caller needing a second step.
    pub fn new(identity: Identity, sink: S) -> Self {
        sink.boot(&identity);
        Self {
            identity,
            sink,
            seq: AtomicU64::new(0),
            snapshot_seq: AtomicU64::new(0),
            peers: Mutex::new(HashMap::new()),
            snapshot_on_transition: AtomicBool::new(true),
            iroh_introspector: Mutex::new(None),
            swim_introspector: Mutex::new(None),
            host_introspector: Mutex::new(None),
            probe_introspector: Mutex::new(None),
            vastai_introspector: Mutex::new(None),
            process_introspector: Mutex::new(None),
        }
    }

    /// Install a tier-2 iroh introspector. After this returns, every
    /// snapshot will include a [`Tier2IrohState`] populated by the
    /// introspector. Replace by calling again; uninstall with
    /// [`Self::clear_iroh_introspector`].
    ///
    /// [`Tier2IrohState`]: crate::diagnostics::snapshot::Tier2IrohState
    pub fn set_iroh_introspector(&self, introspector: Arc<dyn IrohIntrospector>) {
        let mut slot = self
            .iroh_introspector
            .lock()
            .expect("aggregator iroh_introspector mutex poisoned");
        *slot = Some(introspector);
    }

    /// Remove any installed iroh introspector. Subsequent snapshots
    /// will omit the tier-2 iroh block.
    pub fn clear_iroh_introspector(&self) {
        let mut slot = self
            .iroh_introspector
            .lock()
            .expect("aggregator iroh_introspector mutex poisoned");
        *slot = None;
    }

    /// Install a tier-2 SWIM introspector (`DIAGNOSTICS_PLAN.md` T2.6).
    /// After this returns, every snapshot will include a
    /// [`Tier2SwimState`] populated by the introspector. Replace by
    /// calling again; uninstall with [`Self::clear_swim_introspector`].
    ///
    /// [`Tier2SwimState`]: crate::diagnostics::snapshot::Tier2SwimState
    pub fn set_swim_introspector(&self, introspector: Arc<dyn SwimIntrospector>) {
        let mut slot = self
            .swim_introspector
            .lock()
            .expect("aggregator swim_introspector mutex poisoned");
        *slot = Some(introspector);
    }

    /// Remove any installed SWIM introspector. Subsequent snapshots
    /// will omit the tier-2 SWIM block.
    pub fn clear_swim_introspector(&self) {
        let mut slot = self
            .swim_introspector
            .lock()
            .expect("aggregator swim_introspector mutex poisoned");
        *slot = None;
    }

    /// Install a tier-3 host introspector (`DIAGNOSTICS_PLAN.md` T3.1 +
    /// T3.2). After this returns, every snapshot will include a
    /// [`Tier3HostState`] populated by the introspector. Replace by
    /// calling again; uninstall with [`Self::clear_host_introspector`].
    ///
    /// [`Tier3HostState`]: crate::diagnostics::snapshot::Tier3HostState
    pub fn set_host_introspector(&self, introspector: Arc<dyn HostIntrospector>) {
        let mut slot = self
            .host_introspector
            .lock()
            .expect("aggregator host_introspector mutex poisoned");
        *slot = Some(introspector);
    }

    /// Remove any installed host introspector. Subsequent snapshots
    /// will omit the tier-3 host block.
    pub fn clear_host_introspector(&self) {
        let mut slot = self
            .host_introspector
            .lock()
            .expect("aggregator host_introspector mutex poisoned");
        *slot = None;
    }

    /// Install a tier-3 probe introspector (`DIAGNOSTICS_PLAN.md` T3.3).
    /// After this returns, every snapshot will include a
    /// [`Tier3ProbeState`] populated by the introspector.
    ///
    /// [`Tier3ProbeState`]: crate::diagnostics::snapshot::Tier3ProbeState
    pub fn set_probe_introspector(&self, introspector: Arc<dyn ProbeIntrospector>) {
        let mut slot = self
            .probe_introspector
            .lock()
            .expect("aggregator probe_introspector mutex poisoned");
        *slot = Some(introspector);
    }

    /// Remove any installed probe introspector. Subsequent snapshots
    /// will omit the tier-3 probe block.
    pub fn clear_probe_introspector(&self) {
        let mut slot = self
            .probe_introspector
            .lock()
            .expect("aggregator probe_introspector mutex poisoned");
        *slot = None;
    }

    /// Install a tier-3 vast.ai-context introspector
    /// (`DIAGNOSTICS_PLAN.md` T3.4). After this returns, every snapshot
    /// will include a [`Tier3VastaiContext`] populated by the
    /// introspector. Vastai context is fixed at boot (env vars only),
    /// so the captured value will be identical across snapshots.
    ///
    /// [`Tier3VastaiContext`]: crate::diagnostics::snapshot::Tier3VastaiContext
    pub fn set_vastai_introspector(&self, introspector: Arc<dyn VastaiIntrospector>) {
        let mut slot = self
            .vastai_introspector
            .lock()
            .expect("aggregator vastai_introspector mutex poisoned");
        *slot = Some(introspector);
    }

    /// Remove any installed vastai introspector.
    pub fn clear_vastai_introspector(&self) {
        let mut slot = self
            .vastai_introspector
            .lock()
            .expect("aggregator vastai_introspector mutex poisoned");
        *slot = None;
    }

    /// Install a tier-3 process-stats introspector (`DIAGNOSTICS_PLAN.md`
    /// T3.5). After this returns, every snapshot will include a
    /// [`Tier3ProcessStats`] populated by the introspector.
    ///
    /// [`Tier3ProcessStats`]: crate::diagnostics::snapshot::Tier3ProcessStats
    pub fn set_process_introspector(&self, introspector: Arc<dyn ProcessIntrospector>) {
        let mut slot = self
            .process_introspector
            .lock()
            .expect("aggregator process_introspector mutex poisoned");
        *slot = Some(introspector);
    }

    /// Remove any installed process-stats introspector.
    pub fn clear_process_introspector(&self) {
        let mut slot = self
            .process_introspector
            .lock()
            .expect("aggregator process_introspector mutex poisoned");
        *slot = None;
    }

    /// Toggle the local-transition trigger (T1.4). When enabled
    /// (default), every [`Event::SwimTransition`] emit fires an
    /// in-line [`SnapshotTrigger::Transition`] snapshot before
    /// returning. Disable for tests that want to assert event counts
    /// without the bookkeeping snapshots in the way.
    pub fn set_snapshot_on_transition(&self, enabled: bool) {
        self.snapshot_on_transition.store(enabled, Ordering::Relaxed);
    }

    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    /// Read-only access to the sink. Useful in tests.
    pub fn sink(&self) -> &S {
        &self.sink
    }

    /// Emit a single event.
    ///
    /// The aggregator updates its internal reachability log from
    /// transition-shaped events, wraps the event in an [`EventRecord`]
    /// tagged with this node's id, the next monotonic sequence, and
    /// wall clock, and forwards the record to the sink.
    ///
    /// SWIM transitions also fire a [`SnapshotTrigger::Transition`]
    /// snapshot inline (T1.4 local-transition trigger) unless
    /// disabled via [`Self::set_snapshot_on_transition`].
    pub fn emit(&self, event: Event) {
        let now = wall_ms_now();
        self.update_reachability(&event, now);
        let is_transition = matches!(&event, Event::SwimTransition { .. });
        let record = self.wrap_at(event, now);
        let seq = record.monotonic_seq;
        self.sink.emit(record);
        if is_transition && self.snapshot_on_transition.load(Ordering::Relaxed) {
            self.snapshot(SnapshotTrigger::Transition(format!("seq-{seq}")));
        }
    }

    /// Number of events emitted by this aggregator so far. Used by
    /// the snapshot envelope's `monotonic_seq` field.
    pub fn current_seq(&self) -> u64 {
        self.seq.load(Ordering::Relaxed)
    }

    /// Capture the current reachability state into a fresh
    /// [`Snapshot`] and forward it to the sink. Returns the snapshot.
    pub fn snapshot(&self, trigger: SnapshotTrigger) -> Snapshot {
        let seq = self.snapshot_seq.fetch_add(1, Ordering::Relaxed);
        let snap_id = format!("{}-{}", self.identity.node_id_short, seq);
        let iroh = self
            .iroh_introspector
            .lock()
            .expect("aggregator iroh_introspector mutex poisoned")
            .as_ref()
            .map(|intro| intro.capture());
        let swim = self
            .swim_introspector
            .lock()
            .expect("aggregator swim_introspector mutex poisoned")
            .as_ref()
            .map(|intro| intro.capture());
        let host = self
            .host_introspector
            .lock()
            .expect("aggregator host_introspector mutex poisoned")
            .as_ref()
            .map(|intro| intro.capture());
        let probes = self
            .probe_introspector
            .lock()
            .expect("aggregator probe_introspector mutex poisoned")
            .as_ref()
            .map(|intro| intro.capture());
        let vastai = self
            .vastai_introspector
            .lock()
            .expect("aggregator vastai_introspector mutex poisoned")
            .as_ref()
            .map(|intro| intro.capture());
        let process = self
            .process_introspector
            .lock()
            .expect("aggregator process_introspector mutex poisoned")
            .as_ref()
            .map(|intro| intro.capture());
        let body = SnapshotBody {
            reachability: self.reachability_log(),
            events: Vec::new(),
            iroh,
            swim,
            host,
            probes,
            vastai,
            process,
        };
        let snap = Snapshot {
            identity: self.identity.clone(),
            run_id: self.identity.run_id.clone(),
            snapshot_id: snap_id,
            wall_ms: wall_ms_now(),
            monotonic_seq: self.current_seq(),
            trigger,
            body,
        };
        self.sink.snapshot(snap.clone());
        snap
    }

    /// Send a finalize record to the sink. The body is opaque metadata
    /// (exit reason, run summary) attached to the wire request.
    ///
    /// Also captures a final on-demand snapshot before forwarding so
    /// the finalize-caller's own state lands in the bundle — the
    /// collector cannot deliver the `snapshot_now` hint back to us
    /// during its assembly window because our drainer is blocked
    /// waiting on the finalize response.
    pub fn finalize(&self, body: serde_json::Value) {
        self.snapshot(SnapshotTrigger::OnDemand);
        self.sink.finalize(body);
    }

    /// Borrow a clone of the per-peer reachability log. Sorted by
    /// peer hex for stable serialization.
    pub fn reachability_log(&self) -> Vec<PeerReachability> {
        let mut peers: Vec<PeerReachability> = self
            .peers
            .lock()
            .expect("aggregator peers mutex poisoned")
            .values()
            .cloned()
            .collect();
        peers.sort_by(|a, b| a.peer_node_id_hex.cmp(&b.peer_node_id_hex));
        peers
    }

    /// Wrap a raw [`Event`] in an [`EventRecord`] tagged with this
    /// node's id, the next monotonic sequence number, and the current
    /// wall clock. Public so the spool (later stage) can persist the
    /// same shape that's sent over the wire.
    pub fn wrap(&self, event: Event) -> EventRecord {
        self.wrap_at(event, wall_ms_now())
    }

    fn wrap_at(&self, event: Event, wall_ms: u64) -> EventRecord {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        EventRecord {
            node_id: self.node_id(),
            monotonic_seq: seq,
            wall_ms,
            event,
        }
    }

    fn node_id(&self) -> NodeId {
        // Best effort: rehydrate the NodeId from the hex string. Avoids
        // having to thread NodeId separately through Identity for now.
        decode_node_id(&self.identity.node_id_hex).unwrap_or(NodeId([0u8; 32]))
    }

    fn update_reachability(&self, event: &Event, now_ms: u64) {
        let mut peers = self.peers.lock().expect("aggregator peers mutex poisoned");
        match event {
            Event::SwimTransition {
                peer, from, to, reason,
            } => {
                let hex = node_id_hex(peer);
                let entry = peers
                    .entry(hex.clone())
                    .or_insert_with(|| PeerReachability::new(hex));
                entry.record_transition(StateTransition {
                    from: *from,
                    to: *to,
                    at_ms: now_ms,
                    reason: reason.clone(),
                });
            }
            Event::MessageReceived { peer, .. } => {
                let hex = node_id_hex(peer);
                let entry = peers
                    .entry(hex.clone())
                    .or_insert_with(|| PeerReachability::new(hex));
                entry.last_inbound_packet_at_ms = Some(now_ms);
            }
            Event::MessageSent { peer, .. } => {
                let hex = node_id_hex(peer);
                let entry = peers
                    .entry(hex.clone())
                    .or_insert_with(|| PeerReachability::new(hex));
                entry.last_outbound_success_at_ms = Some(now_ms);
            }
            Event::DialStarted { peer, .. } => {
                let hex = node_id_hex(peer);
                let entry = peers
                    .entry(hex.clone())
                    .or_insert_with(|| PeerReachability::new(hex));
                entry.last_dial_started_at_ms = Some(now_ms);
            }
            Event::DialOutcome {
                peer, outcome, duration_ms, ..
            } => {
                let hex = node_id_hex(peer);
                let entry = peers
                    .entry(hex.clone())
                    .or_insert_with(|| PeerReachability::new(hex));
                entry.last_dial_outcome = Some(format!("{outcome:?}"));
                entry.last_dial_duration_ms = Some(*duration_ms);
            }
            Event::SwimMetadataReceived { peer, version, .. } => {
                let hex = node_id_hex(peer);
                let entry = peers
                    .entry(hex.clone())
                    .or_insert_with(|| PeerReachability::new(hex));
                entry.metadata_version_seen = Some(*version);
            }
            _ => {}
        }
        // Keep current_swim_opinion in sync if we have never recorded
        // a transition but have message activity — the log already
        // initialises with Unknown, which is what we want.
        let _ = PeerState::Unknown;
    }
}

use crate::diagnostics::wall_ms_now;

/// Bridge from the subsystem-facing [`EventEmitter`] trait to the full
/// aggregator pipeline (sequencing, reachability, sink forwarding).
impl<S: Sink + Send + Sync + 'static> EventEmitter for Aggregator<S> {
    fn emit_event(&self, event: Event) {
        self.emit(event);
    }
}

/// Spawn the periodic-snapshot task for an aggregator (T1.4).
///
/// One tokio task drives both the periodic timer and the on-demand
/// signal from the collector (the snapshot-pull hint). The task takes
/// strong ownership of the `Arc<Aggregator<S>>`; drop the returned
/// handle to stop the task (its `await` will short-circuit when the
/// aggregator is dropped, but in practice callers abort the handle
/// during shutdown).
#[cfg(feature = "collector")]
pub fn spawn_periodic_snapshots<S>(
    aggregator: std::sync::Arc<Aggregator<S>>,
    config: PeriodicConfig,
    signal: crate::diagnostics::signal::SnapshotSignal,
) -> tokio::task::JoinHandle<()>
where
    S: Sink + Send + Sync + 'static,
{
    aggregator.set_snapshot_on_transition(config.snapshot_on_transition);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // First tick fires immediately; consume it so we don't snapshot
        // at startup before any events exist.
        interval.tick().await;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    aggregator.snapshot(SnapshotTrigger::Periodic);
                }
                _ = signal.wait() => {
                    aggregator.snapshot(SnapshotTrigger::OnDemand);
                }
            }
        }
    })
}

fn decode_node_id(hex: &str) -> Option<NodeId> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        out[i] = (hi << 4) | lo;
    }
    Some(NodeId(out))
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::identity::Role;
    use crate::diagnostics::sink::InMemorySink;

    fn agg() -> Aggregator<InMemorySink> {
        let id = Identity::new(NodeId([0xab; 32]), Role::stage(), "run-a");
        Aggregator::new(id, InMemorySink::new())
    }

    #[test]
    fn emit_forwards_record_to_sink_and_updates_reachability() {
        let a = agg();
        a.emit(Event::SwimTransition {
            peer: NodeId([0x01; 32]),
            from: PeerState::Alive,
            to: PeerState::Suspect,
            reason: "missed-acks".into(),
        });
        assert_eq!(a.sink().record_count(), 1);
        let records = a.sink().records();
        assert_eq!(records[0].monotonic_seq, 1);
        let log = a.reachability_log();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].current_swim_opinion, PeerState::Suspect);
        assert_eq!(log[0].swim_transition_history.len(), 1);
    }

    #[test]
    fn new_announces_boot_to_sink() {
        let a = agg();
        let boots = a.sink().boots();
        assert_eq!(boots.len(), 1);
        assert_eq!(boots[0].run_id, "run-a");
    }

    #[test]
    fn snapshot_writes_to_sink_and_carries_identity() {
        let a = agg();
        a.emit(Event::MessageReceived {
            peer: NodeId([0x02; 32]),
            kind: "swim_ping".into(),
            size: 42,
        });
        let snap = a.snapshot(SnapshotTrigger::Periodic);
        assert_eq!(snap.identity.role, Role::stage());
        assert_eq!(snap.run_id, "run-a");
        assert_eq!(snap.body.reachability.len(), 1);
        assert!(snap.body.reachability[0].last_inbound_packet_at_ms.is_some());
        assert_eq!(a.sink().snapshot_count(), 1);
    }

    #[test]
    fn finalize_forwards_body_to_sink() {
        let a = agg();
        a.finalize(serde_json::json!({"exit_reason": "ok"}));
        let f = a.sink().finalizes();
        assert_eq!(f.len(), 1);
        assert_eq!(f[0]["exit_reason"], "ok");
    }
}
