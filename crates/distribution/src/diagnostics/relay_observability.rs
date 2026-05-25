//! Relay-side session bookkeeping (spec §1, gap 1).
//!
//! A relay binary installs a [`RelayObservability`] on its
//! [`crate::diagnostics::Aggregator`]; whichever process wraps the
//! actual relay engine then calls [`RelayObservability::note_session_opened`]
//! / [`RelayObservability::note_session_closed`] as sessions come and
//! go. The helper:
//!
//! - emits typed [`crate::diagnostics::Event::RelaySessionOpened`] /
//!   [`crate::diagnostics::Event::RelaySessionClosed`] events into the
//!   bundle's event stream (lifecycle view),
//! - maintains the running totals the
//!   [`crate::diagnostics::snapshot::Tier3RelayServer`] snapshot block
//!   exposes (current-value view), broken down by close reason so the
//!   post-processor's per-peer correlation can name *who closed and
//!   why* without consulting an external system.
//!
//! The bridge to the underlying relay implementation is intentionally
//! decoupled: the relay binary owns the calls into `note_*`, which
//! means a future iroh-relay that exposes session hooks, a forked
//! relay, or a thin HTTP middleware all wire up the same way.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::diagnostics::event::Event;
use crate::diagnostics::sink::{DynEmitter, EventEmitter, noop_emitter};
use crate::diagnostics::snapshot::{RelayServerIntrospector, Tier3RelayServer};
use crate::diagnostics::wall_ms_now;

/// Bookkeeping for a single relay binary's observed sessions.
///
/// Cheap to construct, shareable as `Arc<RelayObservability>`. Two
/// internal mutexes — `state` for running totals, `emitter` for the
/// event sink — kept separate so the introspector path never blocks
/// on the emitter path and vice versa.
pub struct RelayObservability {
    state: Mutex<RelayState>,
    emitter: Mutex<DynEmitter>,
}

impl std::fmt::Debug for RelayObservability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelayObservability")
            .field(
                "state",
                &self.state.lock().ok().map(|s| RelayStateDebug {
                    active_sessions: s.active_sessions,
                    total_opens: s.total_opens,
                    total_closes: s.total_closes,
                }),
            )
            .finish()
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct RelayStateDebug {
    active_sessions: u64,
    total_opens: u64,
    total_closes: u64,
}

#[derive(Debug, Default)]
struct RelayState {
    active_sessions: u64,
    total_opens: u64,
    total_closes: u64,
    bytes_rx_total: u64,
    bytes_tx_total: u64,
    closes_by_reason: BTreeMap<String, u64>,
}

impl Default for RelayObservability {
    fn default() -> Self {
        Self::new()
    }
}

impl RelayObservability {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(RelayState::default()),
            emitter: Mutex::new(noop_emitter()),
        }
    }

    /// Install an event emitter so per-session lifecycle events
    /// (`RelaySessionOpened` / `RelaySessionClosed`) reach the bundle.
    /// The default is a no-op emitter, which is fine if the caller
    /// only wants the aggregate snapshot view.
    pub fn set_emitter(&self, emitter: DynEmitter) {
        *self
            .emitter
            .lock()
            .expect("relay observability emitter mutex poisoned") = emitter;
    }

    /// Cheap shareable handle for installing on an aggregator.
    pub fn into_arc(self) -> Arc<dyn RelayServerIntrospector> {
        Arc::new(self)
    }

    /// Record a new session opening. Increments `active_sessions` and
    /// `total_opens`, then emits `RelaySessionOpened`.
    pub fn note_session_opened(&self, peer_node_id_hex: impl Into<String>, at_ms: u64) {
        let peer = peer_node_id_hex.into();
        {
            let mut state = self
                .state
                .lock()
                .expect("relay observability state mutex poisoned");
            state.active_sessions = state.active_sessions.saturating_add(1);
            state.total_opens = state.total_opens.saturating_add(1);
        }
        let emitter = self
            .emitter
            .lock()
            .expect("relay observability emitter mutex poisoned")
            .clone();
        emitter.emit_event(Event::RelaySessionOpened {
            peer_node_id_hex: peer,
            at_ms,
        });
    }

    /// Record a session close. Decrements `active_sessions`, bumps
    /// `total_closes` and the per-reason counter, accumulates the
    /// byte totals, then emits `RelaySessionClosed`. `close_initiator`
    /// is one of `"relay"`, `"remote"`, `"idle_timeout"`.
    #[allow(clippy::too_many_arguments)]
    pub fn note_session_closed(
        &self,
        peer_node_id_hex: impl Into<String>,
        opened_at_ms: u64,
        closed_at_ms: u64,
        close_initiator: impl Into<String>,
        close_reason: impl Into<String>,
        bytes_rx: u64,
        bytes_tx: u64,
    ) {
        let peer = peer_node_id_hex.into();
        let initiator = close_initiator.into();
        let reason = close_reason.into();
        let duration_ms = closed_at_ms.saturating_sub(opened_at_ms);
        {
            let mut state = self
                .state
                .lock()
                .expect("relay observability state mutex poisoned");
            state.active_sessions = state.active_sessions.saturating_sub(1);
            state.total_closes = state.total_closes.saturating_add(1);
            state.bytes_rx_total = state.bytes_rx_total.saturating_add(bytes_rx);
            state.bytes_tx_total = state.bytes_tx_total.saturating_add(bytes_tx);
            *state.closes_by_reason.entry(reason.clone()).or_insert(0) += 1;
        }
        let emitter = self
            .emitter
            .lock()
            .expect("relay observability emitter mutex poisoned")
            .clone();
        emitter.emit_event(Event::RelaySessionClosed {
            peer_node_id_hex: peer,
            opened_at_ms,
            closed_at_ms,
            duration_ms,
            close_initiator: initiator,
            close_reason: reason,
            bytes_rx,
            bytes_tx,
        });
    }
}

impl RelayServerIntrospector for RelayObservability {
    fn capture(&self) -> Tier3RelayServer {
        let state = self
            .state
            .lock()
            .expect("relay observability state mutex poisoned");
        let mut closes_by_reason: Vec<(String, u64)> = state
            .closes_by_reason
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        closes_by_reason.sort_by(|a, b| a.0.cmp(&b.0));
        Tier3RelayServer {
            active_sessions: state.active_sessions,
            total_opens: state.total_opens,
            total_closes: state.total_closes,
            bytes_rx_total: state.bytes_rx_total,
            bytes_tx_total: state.bytes_tx_total,
            closes_by_reason,
            scraped_at_ms: wall_ms_now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::sink::InMemorySink;
    use crate::diagnostics::{Aggregator, Identity, Role};
    use crate::types::NodeId;

    #[test]
    fn note_open_close_round_trips_through_aggregator_snapshot() {
        let obs = Arc::new(RelayObservability::new());
        let id = Identity::new(NodeId([0x42; 32]), Role::custom("relay"), "run-r");
        let agg = Aggregator::new(id, InMemorySink::new());
        agg.set_relay_server_introspector(obs.clone() as Arc<dyn RelayServerIntrospector>);

        obs.note_session_opened("aa".repeat(32), 100);
        obs.note_session_opened("bb".repeat(32), 200);
        obs.note_session_closed("aa".repeat(32), 100, 500, "relay", "idle", 1024, 2048);

        let snap = agg.snapshot(crate::diagnostics::snapshot::SnapshotTrigger::Periodic);
        let rs = snap.body.relay_server.expect("relay_server present");
        assert_eq!(rs.active_sessions, 1);
        assert_eq!(rs.total_opens, 2);
        assert_eq!(rs.total_closes, 1);
        assert_eq!(rs.bytes_rx_total, 1024);
        assert_eq!(rs.bytes_tx_total, 2048);
        assert_eq!(rs.closes_by_reason, vec![("idle".to_string(), 1u64)]);
    }
}
