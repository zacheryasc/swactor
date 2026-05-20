//! SWIM-internal scrape for tier-2 snapshots (`DIAGNOSTICS_PLAN.md`
//! T2.6).
//!
//! SWIM state lives behind a `&mut SwimNode` owned by the driver and
//! is therefore not `Sync`; we cannot poll it from a background task
//! the way [`crate::diagnostics::iroh_introspect`] polls iroh's
//! `Endpoint`. Instead the introspector is a *recorder* — SWIM (and
//! the driver) push observations through `record_*` / `note_*` methods
//! as they happen, the introspector keeps the latest aggregate behind
//! its own internal `Mutex`, and [`SwimIntrospector::capture`] reads
//! that aggregate without touching SWIM.
//!
//! The trade-off mirrors the
//! [`crate::diagnostics::ConnectionCacheTracker`] pattern from S7: a
//! handful of cheap method calls at well-defined SWIM hook points
//! versus a complex polling story over a non-`Sync` state machine.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use crate::diagnostics::event::PeerState;
use crate::diagnostics::snapshot::{
    SwimIntrospector, Tier2SwimConfig, Tier2SwimMessage, Tier2SwimPeer, Tier2SwimState,
};
use crate::diagnostics::wall_ms_now;
use crate::types::NodeId;

/// Records SWIM membership / probe / message activity into a snapshot
/// view. Designed to be shared via `Arc` between the `SwimNode` (which
/// drives the `note_*` writes) and the diagnostics aggregator (which
/// reads through the [`SwimIntrospector`] trait at snapshot time).
#[derive(Debug)]
pub struct SwimIntrospect {
    config: Tier2SwimConfig,
    self_id_hex: String,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    self_incarnation: u64,
    metadata_local_version: u64,
    peers: HashMap<NodeId, PeerEntry>,
    recent_messages: VecDeque<Tier2SwimMessage>,
}

#[derive(Debug, Default, Clone)]
struct PeerEntry {
    state: Option<PeerState>,
    incarnation: u64,
    last_ping_sent_at_ms: Option<u64>,
    last_ack_received_at_ms: Option<u64>,
    last_ping_received_at_ms: Option<u64>,
    suspect_started_at_ms: Option<u64>,
    metadata_version_seen: Option<u64>,
}

impl SwimIntrospect {
    /// Build an introspector for a SWIM node whose protocol parameters
    /// are described by `config`. `self_id` is just the node id whose
    /// SWIM state this introspector reflects — embedded into the
    /// snapshot so the post-processor can tell self apart from peers.
    pub fn new(config: Tier2SwimConfig, self_id: NodeId) -> Self {
        Self {
            config,
            self_id_hex: node_id_hex_lower(&self_id),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Read-only accessor for the configured timeouts/fanouts. Useful
    /// for tests that want to compare against the running config
    /// without round-tripping through a snapshot.
    pub fn config(&self) -> &Tier2SwimConfig {
        &self.config
    }

    /// Record that this node sent a SWIM ping with `sequence` to
    /// `peer`. Stamps `last_ping_sent_at_ms` for the peer.
    pub fn note_ping_sent(&self, peer: NodeId, _sequence: u64) {
        let mut inner = self.lock();
        inner.peer_mut(peer).last_ping_sent_at_ms = Some(wall_ms_now());
    }

    /// Record that this node received a SWIM ping from `peer`. Stamps
    /// `last_ping_received_at_ms` and pushes a `ping` entry onto the
    /// recent-messages ring.
    pub fn note_ping_received(&self, peer: NodeId, sequence: u64) {
        let now = wall_ms_now();
        let mut inner = self.lock();
        inner.peer_mut(peer).last_ping_received_at_ms = Some(now);
        inner.push_message(Tier2SwimMessage {
            kind: "ping".into(),
            peer_node_id_hex: node_id_hex_lower(&peer),
            at_ms: now,
            sequence: Some(sequence),
        });
    }

    /// Record an inbound SWIM ack. Stamps `last_ack_received_at_ms`
    /// and pushes an `ack` entry onto the recent-messages ring.
    pub fn note_ack_received(&self, peer: NodeId, sequence: u64) {
        let now = wall_ms_now();
        let mut inner = self.lock();
        inner.peer_mut(peer).last_ack_received_at_ms = Some(now);
        inner.push_message(Tier2SwimMessage {
            kind: "ack".into(),
            peer_node_id_hex: node_id_hex_lower(&peer),
            at_ms: now,
            sequence: Some(sequence),
        });
    }

    /// Record an inbound indirect ping-request — `from` is the
    /// requester; `target` is whom we are being asked to ping on
    /// their behalf.
    pub fn note_ping_req_received(&self, from: NodeId, target: NodeId, sequence: u64) {
        let now = wall_ms_now();
        let mut inner = self.lock();
        inner.push_message(Tier2SwimMessage {
            kind: "ping_req".into(),
            peer_node_id_hex: node_id_hex_lower(&from),
            at_ms: now,
            sequence: Some(sequence),
        });
        // The target field stays out of the wire shape — bundle
        // readers correlate via the matching `ack` entry that the
        // relay will forward back. Keep the ring entry shape uniform.
        let _ = target;
    }

    /// Record an inbound indirect ack forwarded by a relay.
    /// `target` is the peer the ack is about.
    pub fn note_indirect_ack_received(&self, target: NodeId, sequence: u64) {
        let now = wall_ms_now();
        let mut inner = self.lock();
        inner.peer_mut(target).last_ack_received_at_ms = Some(now);
        inner.push_message(Tier2SwimMessage {
            kind: "indirect_ack".into(),
            peer_node_id_hex: node_id_hex_lower(&target),
            at_ms: now,
            sequence: Some(sequence),
        });
    }

    /// Record a received `JoinRequest` from `peer`.
    pub fn note_join_request_received(&self, peer: NodeId) {
        let now = wall_ms_now();
        let mut inner = self.lock();
        inner.push_message(Tier2SwimMessage {
            kind: "join_request".into(),
            peer_node_id_hex: node_id_hex_lower(&peer),
            at_ms: now,
            sequence: None,
        });
    }

    /// Record a received `JoinResponse` from `peer` carrying
    /// `member_count` records.
    pub fn note_join_response_received(&self, peer: NodeId, member_count: usize) {
        let now = wall_ms_now();
        let mut inner = self.lock();
        inner.push_message(Tier2SwimMessage {
            kind: "join_response".into(),
            peer_node_id_hex: node_id_hex_lower(&peer),
            at_ms: now,
            sequence: Some(member_count as u64),
        });
    }

    /// Update the recorded SWIM opinion + incarnation for a peer.
    /// Called from the SWIM transition site. Clears the suspect
    /// timestamp on transitions out of Suspect.
    pub fn note_peer_state(&self, peer: NodeId, state: PeerState, incarnation: u64) {
        let mut inner = self.lock();
        let entry = inner.peer_mut(peer);
        entry.state = Some(state);
        entry.incarnation = incarnation;
        if state != PeerState::Suspect {
            entry.suspect_started_at_ms = None;
        }
    }

    /// Record that a peer just entered the Suspect state. Stamps the
    /// per-peer `suspect_started_at_ms`.
    pub fn note_suspect_started(&self, peer: NodeId) {
        let now = wall_ms_now();
        let mut inner = self.lock();
        inner.peer_mut(peer).suspect_started_at_ms = Some(now);
    }

    /// Drop a peer from the introspector's view entirely (e.g. after
    /// `clear_dead_member`).
    pub fn drop_peer(&self, peer: NodeId) {
        let mut inner = self.lock();
        inner.peers.remove(&peer);
    }

    /// Replace this node's local incarnation number — bumped each time
    /// SWIM refutes a suspicion against us.
    pub fn note_self_incarnation(&self, value: u64) {
        let mut inner = self.lock();
        inner.self_incarnation = value;
    }

    /// Replace this node's local metadata generation. Called from
    /// [`crate::node::DistributedNode`] every time
    /// `set_relay_url` / `set_node_name` bumps the disseminator.
    pub fn note_metadata_local_version(&self, value: u64) {
        let mut inner = self.lock();
        inner.metadata_local_version = value;
    }

    /// Record a remote peer's gossiped metadata generation. Lets the
    /// post-processor diff metadata visibility across nodes.
    pub fn note_peer_metadata_version(&self, peer: NodeId, version: u64) {
        let mut inner = self.lock();
        inner.peer_mut(peer).metadata_version_seen = Some(version);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .expect("swim introspect mutex poisoned")
    }
}

impl Inner {
    fn peer_mut(&mut self, peer: NodeId) -> &mut PeerEntry {
        self.peers.entry(peer).or_default()
    }

    fn push_message(&mut self, msg: Tier2SwimMessage) {
        if self.recent_messages.len() >= Tier2SwimState::RECENT_MESSAGES_CAP {
            self.recent_messages.pop_front();
        }
        self.recent_messages.push_back(msg);
    }
}

impl SwimIntrospector for SwimIntrospect {
    fn capture(&self) -> Tier2SwimState {
        let inner = self.lock();
        let mut peers: Vec<Tier2SwimPeer> = inner
            .peers
            .iter()
            .map(|(node_id, entry)| Tier2SwimPeer {
                peer_node_id_hex: node_id_hex_lower(node_id),
                state: entry.state.unwrap_or(PeerState::Unknown),
                incarnation: entry.incarnation,
                last_ping_sent_at_ms: entry.last_ping_sent_at_ms,
                last_ack_received_at_ms: entry.last_ack_received_at_ms,
                last_ping_received_at_ms: entry.last_ping_received_at_ms,
                suspect_started_at_ms: entry.suspect_started_at_ms,
                metadata_version_seen: entry.metadata_version_seen,
            })
            .collect();
        peers.sort_by(|a, b| a.peer_node_id_hex.cmp(&b.peer_node_id_hex));
        let recent_messages = inner.recent_messages.iter().cloned().collect();
        Tier2SwimState {
            config: self.config.clone(),
            self_node_id_hex: self.self_id_hex.clone(),
            self_incarnation: inner.self_incarnation,
            metadata_local_version: inner.metadata_local_version,
            peers,
            recent_messages,
            scraped_at_ms: wall_ms_now(),
        }
    }
}

fn node_id_hex_lower(id: &NodeId) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(64);
    for b in id.0 {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Tier2SwimConfig {
        Tier2SwimConfig {
            probe_interval_ticks: 10,
            probe_timeout_ticks: 3,
            suspicion_timeout_ticks: 30,
            indirect_probes_k: 3,
            dead_reprobe_interval_ticks: 50,
            gossip_fanout_lambda: 3,
            max_piggyback: 8,
            probe_mode: "periodic".into(),
        }
    }

    #[test]
    fn capture_reports_self_metadata_and_protocol_config() {
        let introspect = SwimIntrospect::new(cfg(), NodeId([0xaa; 32]));
        introspect.note_self_incarnation(7);
        introspect.note_metadata_local_version(42);
        let snap = introspect.capture();
        assert_eq!(snap.self_incarnation, 7);
        assert_eq!(snap.metadata_local_version, 42);
        assert_eq!(snap.config.probe_interval_ticks, 10);
        assert_eq!(snap.config.gossip_fanout_lambda, 3);
        assert!(snap.peers.is_empty());
        assert!(snap.recent_messages.is_empty());
    }

    #[test]
    fn recent_messages_ring_is_bounded() {
        let introspect = SwimIntrospect::new(cfg(), NodeId([0; 32]));
        let peer = NodeId([1u8; 32]);
        for seq in 0..(Tier2SwimState::RECENT_MESSAGES_CAP as u64 + 5) {
            introspect.note_ping_received(peer, seq);
        }
        let snap = introspect.capture();
        assert_eq!(snap.recent_messages.len(), Tier2SwimState::RECENT_MESSAGES_CAP);
        // Oldest 5 entries should have been dropped — first remaining
        // entry's sequence is 5 (the FIFO order is preserved).
        assert_eq!(snap.recent_messages.first().unwrap().sequence, Some(5));
        assert_eq!(
            snap.recent_messages.last().unwrap().sequence,
            Some(Tier2SwimState::RECENT_MESSAGES_CAP as u64 + 4),
        );
    }

    #[test]
    fn note_peer_state_clears_suspect_timestamp_on_recovery() {
        let introspect = SwimIntrospect::new(cfg(), NodeId([0; 32]));
        let peer = NodeId([2u8; 32]);
        introspect.note_suspect_started(peer);
        introspect.note_peer_state(peer, PeerState::Suspect, 4);
        let s1 = introspect.capture();
        let p1 = s1.peers.iter().find(|p| p.incarnation == 4).unwrap();
        assert!(p1.suspect_started_at_ms.is_some());

        introspect.note_peer_state(peer, PeerState::Alive, 5);
        let s2 = introspect.capture();
        let p2 = s2.peers.iter().find(|p| p.incarnation == 5).unwrap();
        assert_eq!(p2.state, PeerState::Alive);
        assert!(p2.suspect_started_at_ms.is_none());
    }
}
