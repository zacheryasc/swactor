//! Wire format for the diagnostics collector protocol.
//!
//! Shared between the collector binary and the (future) `HttpSink` in
//! [`crate::diagnostics::sink`]. Stable enough that bumping the
//! collector independently of nodes is OK — additive fields only, and
//! everything tolerates unknown fields on deserialize.

use serde::{Deserialize, Serialize};

/// Round-trip timing observed at the collector for a single POST.
///
/// Sent in every response so the node (or the post-processor) can
/// compute its offset from collector time to within RTT/2 — see
/// `DIAGNOSTICS_PLAN.md` T1.5.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClockEcho {
    /// The `node_send_ms` the node provided in the request, echoed
    /// back so the node can match without keeping a per-request table.
    pub node_send_ms_echoed: u64,
    /// Collector wall clock when the request was received.
    pub collector_recv_ms: u64,
    /// Collector wall clock when the response was sent.
    pub collector_send_ms: u64,
}

/// Out-of-band signals the collector pushes back in any response.
///
/// Today only `snapshot_now` is defined; T1.4 pull-triggers add to
/// this. Fields default to "no signal" so adding new hints does not
/// require coordinated node upgrades.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Hints {
    /// If true, the node should take a snapshot at its next
    /// opportunity. The collector sets this during finalize and
    /// (later) when an operator manually triggers a global snapshot.
    #[serde(default, skip_serializing_if = "is_false")]
    pub snapshot_now: bool,
}

impl Hints {
    /// True when no hint is set — used to decide whether to emit
    /// the block on the wire at all.
    pub fn is_empty(&self) -> bool {
        !self.snapshot_now
    }
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// Standard envelope returned from every POST.
///
/// `clock` is always present (T1.5); `hints` is omitted when empty so
/// the wire stays quiet in the common case. Endpoints that produce a
/// payload (e.g. `/diag/finalize`) attach it under `body`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostAck<T = serde_json::Value> {
    pub clock: ClockEcho,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hints: Option<Hints>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<T>,
}

/// Record kind for the four POST endpoints. Used as the filename
/// prefix on disk (`{kind}-{seq}.json`) and as the path segment in
/// the URL (`POST /diag/{kind}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    Boot,
    Events,
    Snapshot,
    Finalize,
}

impl RecordKind {
    pub fn as_str(self) -> &'static str {
        match self {
            RecordKind::Boot => "boot",
            RecordKind::Events => "events",
            RecordKind::Snapshot => "snapshot",
            RecordKind::Finalize => "finalize",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "boot" => Some(RecordKind::Boot),
            "events" => Some(RecordKind::Events),
            "snapshot" => Some(RecordKind::Snapshot),
            "finalize" => Some(RecordKind::Finalize),
            _ => None,
        }
    }
}

/// Per-node summary written into `MANIFEST.json` at the root of a
/// finalized bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestNode {
    /// Full hex of the node's key — the directory name under the run
    /// is the node's *short* hex (or a role-derived label); this field
    /// preserves the full identifier so manifests are unambiguous.
    pub node_id_hex: String,
    /// Friendly directory label inside the tarball — `orchestrator`,
    /// `stage-0`, or `node-{short}` if the boot record was missing.
    pub label: String,
    pub role: Option<String>,
    pub stage_index: Option<u32>,
    pub boot_recorded: bool,
    pub event_batches: u64,
    pub snapshots: u64,
    pub finalize_recorded: bool,
}

/// Top-level manifest written into the tarball root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub run_id: String,
    /// Earliest `collector_recv_ms` observed for this run.
    pub run_start_collector_ms: Option<u64>,
    /// Latest `collector_recv_ms` observed for this run.
    pub run_end_collector_ms: Option<u64>,
    pub finalize_received: bool,
    pub nodes: Vec<ManifestNode>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_hints_round_trip_drops_the_field() {
        // Round-trips the *envelope* — empty hints should not appear
        // in the JSON, otherwise we leak meaningless noise to every
        // response in the common case.
        let ack: PostAck = PostAck {
            clock: ClockEcho {
                node_send_ms_echoed: 1,
                collector_recv_ms: 2,
                collector_send_ms: 3,
            },
            hints: None,
            body: None,
        };
        let s = serde_json::to_string(&ack).unwrap();
        assert!(!s.contains("hints"));
        assert!(!s.contains("body"));
        let back: PostAck = serde_json::from_str(&s).unwrap();
        assert_eq!(back.clock.node_send_ms_echoed, 1);
    }

    #[test]
    fn record_kind_round_trips_through_str() {
        for k in [
            RecordKind::Boot,
            RecordKind::Events,
            RecordKind::Snapshot,
            RecordKind::Finalize,
        ] {
            assert_eq!(RecordKind::parse(k.as_str()), Some(k));
        }
        assert_eq!(RecordKind::parse("unknown"), None);
    }
}
