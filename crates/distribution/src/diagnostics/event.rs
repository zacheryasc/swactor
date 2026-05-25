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
    /// Home-relay change (spec §3 home-change variant). Fired by the
    /// iroh introspector's relay watcher when the URL the node uses
    /// as home changes — including transitions to/from `None`.
    RelayChanged {
        old_url: Option<String>,
        new_url: Option<String>,
    },
    /// Tunnel-state transition between two distinct status values
    /// (spec §3 session-state variant). The authoritative source for
    /// "did the tunnel flap" — a grep for this variant across the
    /// bundle tells you which nodes saw flaps and when. The
    /// corresponding snapshot field is
    /// [`crate::diagnostics::snapshot::Tier2RelaySession::status`];
    /// counters (e.g. `relay_home_change`) are retained for sanity
    /// totals.
    RelaySessionStateChanged {
        relay_url: Option<String>,
        from_status: String,
        to_status: String,
        /// Short reason string when available; `None` when the
        /// transport library does not supply one.
        reason: Option<String>,
    },
    /// Relay-side: a remote node opened a session against this relay
    /// (spec §1). Emitted by the relay binary, not by node-side code.
    /// `peer_node_id_hex` is the hex of the remote node's public key
    /// as observed by the relay; the bundle reader can correlate
    /// against the same hex on the node-side `peers` block.
    RelaySessionOpened {
        peer_node_id_hex: String,
        at_ms: u64,
    },
    /// Relay-side: a session ended (spec §1). Carries everything a
    /// bundle reader needs to answer "who closed and why" without
    /// consulting an external system:
    ///   - `close_initiator`: `"relay"` | `"remote"` | `"idle_timeout"`
    ///   - `close_reason`: short string the relay assigned
    ///   - `duration_ms`, `bytes_rx`, `bytes_tx`: per-session totals
    RelaySessionClosed {
        peer_node_id_hex: String,
        opened_at_ms: u64,
        closed_at_ms: u64,
        duration_ms: u64,
        close_initiator: String,
        close_reason: String,
        bytes_rx: u64,
        bytes_tx: u64,
    },
    /// A payload arrived through the gossip / dissemination layer —
    /// SWIM membership piggyback, name-registry update, anything
    /// similar (spec §10, gap 10). The authoritative source for "did
    /// node X ever hear about name Y from peer Z"; the existing
    /// coarse [`Event::MessageReceived`] counter stays for backward
    /// compatibility, but bundle readers should prefer this typed
    /// event when reconstructing dissemination paths.
    GossipReceived {
        source_peer: NodeId,
        /// Free-form string, extensible. Today's emitters use
        /// `"swim_piggyback"` for SWIM membership gossip; future
        /// callers (registry layer, etc.) supply their own kind.
        payload_kind: String,
        payload_bytes: u32,
        /// Number of items inside the payload (e.g. number of
        /// piggybacked membership updates). `0` is meaningful — an
        /// empty payload still counts as a receipt.
        item_count: u32,
    },
    /// A subprocess this node owns has been spawned (spec §4
    /// lifecycle contract). Replaces the ad-hoc
    /// `Custom { kind: "worker_starting" }` strings the example crate
    /// used to emit. Carries `label` so a bundle reader can answer
    /// "did the actor ever ask the OS to spawn this child?" without
    /// inferring from output. Stage-agnostic and worker-agnostic —
    /// the introspector only knows about (label, PID, command).
    SubprocessSpawned {
        label: String,
        pid: u32,
        command: String,
    },
    /// A subprocess this node owned has exited (spec §4 lifecycle
    /// contract). Replaces the ad-hoc
    /// `Custom { kind: "worker_exited" }` strings. The bundle reader
    /// can immediately distinguish "spawned then crashed" (this
    /// event + `exit_code`/`exit_signal`) from "spawned and stayed
    /// alive but never produced protocol output" (no
    /// `SubprocessExited`, no `worker_ready` Custom event).
    SubprocessExited {
        label: String,
        pid: u32,
        command: String,
        exit_code: Option<i32>,
        exit_signal: Option<i32>,
        uptime_ms: Option<u64>,
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
