//! Typed events emitted by observed subsystems.
//!
//! Events are the append-only structured stream that replaces ad-hoc
//! free-text logging for the iroh / SWIM layer. They carry just enough
//! structure for the post-processor (see `DIAGNOSTICS_PLAN.md` A.3) to
//! reconstruct ordered causality across nodes.

use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// SWIM membership opinion as seen by this node. Mirrors the
/// in-process [`crate::types::MemberState`] but is owned by the
/// diagnostics module so this surface does not break when SWIM
/// internals churn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerState {
    Alive,
    Suspect,
    Dead,
    /// We have heard of this peer but have no opinion yet.
    Unknown,
}

/// iroh's view of the live connection type to a peer at the moment
/// the event was emitted. Strings rather than newtypes — iroh's own
/// vocabulary evolves and we want the wire format to be forgiving.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnType {
    Direct,
    Relay,
    Mixed,
    None,
}

/// Outcome of a single dial attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DialOutcome {
    Success,
    Timeout,
    Refused,
    NoRoute,
    /// Catch-all with the error rendered as a string. Keeps the wire
    /// stable across iroh API changes.
    Error(String),
}

/// Append-only typed events. Variants map 1:1 to the event types in
/// `DIAGNOSTICS_PLAN.md` T1.3, plus an open-ended [`Event::Custom`]
/// for extension by downstream crates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Event {
    SwimTransition {
        peer: NodeId,
        from: PeerState,
        to: PeerState,
        reason: String,
    },
    DialStarted {
        peer: NodeId,
        attempt: u32,
        timeout_ms: u64,
    },
    DialOutcome {
        peer: NodeId,
        attempt: u32,
        outcome: DialOutcome,
        duration_ms: u64,
    },
    IrohConnTypeChanged {
        peer: NodeId,
        old: ConnType,
        new: ConnType,
    },
    RelayChanged {
        old_url: Option<String>,
        new_url: Option<String>,
    },
    SwimMetadataSent {
        version: u64,
        payload_hash: u64,
    },
    SwimMetadataReceived {
        peer: NodeId,
        version: u64,
        payload_hash: u64,
    },
    ConnectionCacheHit {
        peer: NodeId,
        generation: u64,
    },
    ConnectionCacheMiss {
        peer: NodeId,
    },
    ConnectionCacheInvalidated {
        peer: NodeId,
        generation: u64,
        reason: String,
    },
    NodeMapUpdate {
        peer: NodeId,
        from_source: String,
        accepted: bool,
    },
    MessageSent {
        peer: NodeId,
        kind: String,
        size: u32,
    },
    MessageReceived {
        peer: NodeId,
        kind: String,
        size: u32,
    },
    ProbeSent {
        target: String,
        kind: String,
    },
    ProbeReceived {
        target: String,
        kind: String,
        rtt_ms: Option<u64>,
        outcome: String,
    },
    Error {
        component: String,
        message: String,
        peer: Option<NodeId>,
    },
    /// Escape hatch for downstream crates that want to record an
    /// event the core enum does not model. Free-form payload.
    Custom {
        kind: String,
        fields: serde_json::Value,
    },
}

/// Envelope that wraps an [`Event`] with per-node ordering metadata.
///
/// `monotonic_seq` is a per-process counter that never decreases —
/// it lets the post-processor reconstruct intra-node order even when
/// the wall clock jumps. `wall_ms` is best-effort and is aligned
/// post-hoc against collector time (see T1.5).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    pub node_id: NodeId,
    pub monotonic_seq: u64,
    pub wall_ms: u64,
    #[serde(flatten)]
    pub event: Event,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_serializes_with_tag_field() {
        let ev = Event::DialStarted {
            peer: NodeId([0u8; 32]),
            attempt: 1,
            timeout_ms: 500,
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["type"], "DialStarted");
        assert_eq!(json["attempt"], 1);
        assert_eq!(json["timeout_ms"], 500);
    }

    #[test]
    fn custom_event_preserves_arbitrary_fields() {
        let ev = Event::Custom {
            kind: "discovery_resolve_started".into(),
            fields: serde_json::json!({ "peer": "abc", "attempt": 3 }),
        };
        let s = serde_json::to_string(&ev).unwrap();
        let back: Event = serde_json::from_str(&s).unwrap();
        match back {
            Event::Custom { kind, fields } => {
                assert_eq!(kind, "discovery_resolve_started");
                assert_eq!(fields["attempt"], 3);
            }
            _ => panic!("expected Custom variant"),
        }
    }
}
