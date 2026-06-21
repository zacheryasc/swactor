//! Production SWIM telemetry — the observer the live node installs.
//!
//! Outside `simulation`, SWIM ran with `observer = None`, so three live signals
//! never reached the datastream: per-probe round-trip time, the recent probe
//! targets, and the *cause* of each membership transition (the M4 state-diff can
//! see *that* a peer changed but not *why*). This installs a real
//! [`SwimObserver`] that reconstructs all three from the observation stream, and
//! exposes them for the node's telemetry tick to read.
//!
//! It is a side-channel diagnostic: it never feeds back into the protocol, takes
//! `&self` (interior mutability behind one `Mutex`), and is safe to read from the
//! node's main loop while SWIM fires observations on its own worker thread.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::types::{MemberState, NodeId};

use super::node::{SwimObservation, SwimObserver};

/// Recent RTT samples kept for the running median.
const RTT_RING: usize = 64;
/// Recent probe targets kept (most recent last) — matches the probe engine's own
/// history depth so `dist.state.recent_probe_targets` looks the same either way.
const TARGET_RING: usize = 16;
/// Cap on undrained transitions, so a consumer that stops draining can't grow
/// this without bound. Oldest are dropped first (a lost transition is a gap, not
/// a renumber — the same tolerance as the mux).
const TRANSITION_CAP: usize = 256;
/// In-flight probes older than this are pruned defensively. The probe state
/// machine resolves every probe (ack or timeout), so this only guards against a
/// dropped observation leaking an entry forever.
const IN_FLIGHT_TTL: Duration = Duration::from_secs(30);

/// One captured membership transition, carrying the real cause string the
/// state-diff path could never know.
#[derive(Debug, Clone)]
pub struct ObservedTransition {
    pub peer: NodeId,
    pub from: Option<MemberState>,
    pub to: MemberState,
    pub reason: &'static str,
}

#[derive(Default)]
struct Inner {
    /// `(target, sequence)` → when its probe was sent, to time the round-trip.
    in_flight: HashMap<(NodeId, u64), Instant>,
    /// Recent round-trip samples (ms), newest last.
    rtts: VecDeque<u32>,
    /// Recent probe targets, newest last.
    targets: VecDeque<NodeId>,
    /// Transitions awaiting drain by the node's telemetry tick.
    transitions: VecDeque<ObservedTransition>,
}

/// The installed SWIM observer plus the readouts the node consumes each tick.
/// Construct with [`new`](Self::new), install a clone as the observer
/// (`Arc<SwimTelemetry>` implements [`SwimObserver`]), and read the rest.
pub struct SwimTelemetry {
    inner: Mutex<Inner>,
}

impl SwimTelemetry {
    /// A fresh, empty telemetry sink behind an `Arc` (shared between the observer
    /// install and the reading node).
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
        })
    }

    /// Median (p50) of the recent round-trip samples in milliseconds, or `0` when
    /// no probe has completed yet (an honest zero — not a fabricated latency).
    pub fn rtt_ms_p50(&self) -> u32 {
        let inner = self.inner.lock().expect("swim telemetry poisoned");
        if inner.rtts.is_empty() {
            return 0;
        }
        let mut samples: Vec<u32> = inner.rtts.iter().copied().collect();
        samples.sort_unstable();
        samples[samples.len() / 2]
    }

    /// The recent probe targets (most recent last).
    pub fn recent_targets(&self) -> Vec<NodeId> {
        self.inner
            .lock()
            .expect("swim telemetry poisoned")
            .targets
            .iter()
            .copied()
            .collect()
    }

    /// Take the transitions captured since the last call (FIFO, then cleared).
    pub fn drain_transitions(&self) -> Vec<ObservedTransition> {
        self.inner
            .lock()
            .expect("swim telemetry poisoned")
            .transitions
            .drain(..)
            .collect()
    }

    /// The most recent transition cause per peer, **without** draining the queue.
    /// A snapshot reader (e.g. the orchestrator's Distribution view) uses this to
    /// label each member with *why* it last changed state; draining stays
    /// reserved for the fleet emitter's `membership` channel. Reads oldest→newest
    /// so the latest reason per peer wins.
    pub fn last_reasons(&self) -> HashMap<NodeId, &'static str> {
        let inner = self.inner.lock().expect("swim telemetry poisoned");
        let mut out = HashMap::new();
        for t in &inner.transitions {
            out.insert(t.peer, t.reason);
        }
        out
    }

    fn record(&self, observation: SwimObservation) {
        let mut inner = self.inner.lock().expect("swim telemetry poisoned");
        match observation {
            SwimObservation::ProbeSent { target, sequence, .. } => {
                // Drop any leaked in-flight entries before tracking a new probe.
                inner.in_flight.retain(|_, sent| sent.elapsed() < IN_FLIGHT_TTL);
                inner.in_flight.insert((target, sequence), Instant::now());
                if inner.targets.len() >= TARGET_RING {
                    inner.targets.pop_front();
                }
                inner.targets.push_back(target);
            }
            SwimObservation::ProbeAcked { target, sequence, .. } => {
                if let Some(sent) = inner.in_flight.remove(&(target, sequence)) {
                    let rtt = sent.elapsed().as_millis().min(u32::MAX as u128) as u32;
                    if inner.rtts.len() >= RTT_RING {
                        inner.rtts.pop_front();
                    }
                    inner.rtts.push_back(rtt);
                }
            }
            SwimObservation::ProbeTimedOut { target, sequence, .. } => {
                // A timeout is not a round-trip — drop the in-flight entry, no sample.
                inner.in_flight.remove(&(target, sequence));
            }
            SwimObservation::Transition { peer, from, to, reason } => {
                if inner.transitions.len() >= TRANSITION_CAP {
                    inner.transitions.pop_front();
                }
                inner.transitions.push_back(ObservedTransition { peer, from, to, reason });
            }
        }
    }
}

impl SwimObserver for Arc<SwimTelemetry> {
    fn observe(&self, observation: SwimObservation) {
        self.record(observation);
    }
}
