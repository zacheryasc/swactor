//! Per-peer per-direction reachability log (T1.2).
//!
//! Maintained continuously per node, emitted as part of every
//! snapshot. The post-processor diffs `node_A.last_inbound_from_B`
//! against `node_B.last_outbound_to_A` to surface asymmetric routing.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::diagnostics::event::PeerState;
use crate::types::NodeId;

/// How a state change was reported. Stored in the per-peer history
/// ring so that the post-processor can render the why next to the when.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateTransition {
    pub from: PeerState,
    pub to: PeerState,
    pub at_ms: u64,
    pub reason: String,
}

/// The locally observed reachability record for a single remote peer.
///
/// Field names and units come straight from `DIAGNOSTICS_PLAN.md`
/// T1.2 — keep them that way so the post-processor can be schema-
/// driven rather than carry version-specific glue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerReachability {
    pub peer_node_id_hex: String,
    pub last_inbound_packet_at_ms: Option<u64>,
    pub last_inbound_via: Option<String>,
    pub last_outbound_success_at_ms: Option<u64>,
    pub last_outbound_via: Option<String>,
    pub last_dial_started_at_ms: Option<u64>,
    pub last_dial_outcome: Option<String>,
    pub last_dial_duration_ms: Option<u64>,
    pub current_swim_opinion: PeerState,
    pub current_swim_opinion_since_ms: Option<u64>,
    /// Bounded ring of recent transitions (newest last).
    pub swim_transition_history: VecDeque<StateTransition>,
    pub metadata_version_seen: Option<u64>,
    pub metadata_relay_url_seen: Option<String>,
}

impl PeerReachability {
    /// Number of transitions kept in the per-peer ring buffer.
    /// Matches the value in `DIAGNOSTICS_PLAN.md` T1.2.
    pub const HISTORY_CAP: usize = 32;

    pub fn new(peer_node_id_hex: String) -> Self {
        Self {
            peer_node_id_hex,
            last_inbound_packet_at_ms: None,
            last_inbound_via: None,
            last_outbound_success_at_ms: None,
            last_outbound_via: None,
            last_dial_started_at_ms: None,
            last_dial_outcome: None,
            last_dial_duration_ms: None,
            current_swim_opinion: PeerState::Unknown,
            current_swim_opinion_since_ms: None,
            swim_transition_history: VecDeque::with_capacity(Self::HISTORY_CAP),
            metadata_version_seen: None,
            metadata_relay_url_seen: None,
        }
    }

    /// Append a transition, evicting the oldest entry if the ring is
    /// full. Updates `current_swim_opinion` to the new state.
    pub fn record_transition(&mut self, t: StateTransition) {
        self.current_swim_opinion = t.to;
        self.current_swim_opinion_since_ms = Some(t.at_ms);
        if self.swim_transition_history.len() == Self::HISTORY_CAP {
            self.swim_transition_history.pop_front();
        }
        self.swim_transition_history.push_back(t);
    }
}

/// Helper to render a [`NodeId`] as lowercase hex without dragging
/// in extra deps.
pub fn node_id_hex(id: &NodeId) -> String {
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

    #[test]
    fn history_ring_evicts_oldest_when_full() {
        let mut p = PeerReachability::new("aabb".into());
        for i in 0..(PeerReachability::HISTORY_CAP as u64 + 5) {
            p.record_transition(StateTransition {
                from: PeerState::Alive,
                to: PeerState::Suspect,
                at_ms: 1_000 + i,
                reason: format!("t{i}"),
            });
        }
        assert_eq!(p.swim_transition_history.len(), PeerReachability::HISTORY_CAP);
        // Oldest 5 were evicted; the front should now start at t=5.
        let first = p.swim_transition_history.front().unwrap();
        assert_eq!(first.reason, "t5");
        // Current opinion is the latest transition's target.
        assert_eq!(p.current_swim_opinion, PeerState::Suspect);
    }

    #[test]
    fn node_id_hex_renders_lowercase_64_chars() {
        let mut bytes = [0u8; 32];
        bytes[31] = 0xff;
        let hex = node_id_hex(&NodeId(bytes));
        assert_eq!(hex.len(), 64);
        assert!(hex.ends_with("ff"));
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }
}
