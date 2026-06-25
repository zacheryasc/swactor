//! Serializable shape of a node's observable distribution state.
//!
//! This is the JSON contract distribution observers render. The node no longer
//! *collects* it by polling — per-node telemetry now flows over the datastream.
//! The type is retained as the shared wire shape so external consumers (the
//! example clusters, the docker integration tests) can deserialize a node's
//! `/api/distribution` response, and so producers that build the shape directly
//! have one definition to target.

use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// Snapshot of a single member in the SWIM membership list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberInfo {
    pub node_id: String,
    pub addr: Option<String>,
    pub state: String,
    pub incarnation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_authorized: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relay_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub node_name: Option<String>,
    /// Cause of this member's most recent SWIM liveness transition (the
    /// production observer's reason string, e.g. "ping-received",
    /// "probe-timeout"). `None` until a transition has been observed; the
    /// old state-diff path could never carry this.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub reason: Option<String>,
}

/// Snapshot of a single LRU cache entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntryInfo {
    pub actor_addr: String,
    pub node_id: String,
}

/// Snapshot of a single registry entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryEntryInfo {
    pub name: String,
    pub actor_addr: String,
    pub node_id: String,
    pub tombstone: bool,
}

/// Snapshot of a join attempt's real-time status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinStatusInfo {
    pub node_id: String,
    /// "connecting", "sending", "sent", "failed"
    pub phase: String,
    /// E.g. "2/5" for attempt progress, or error message for failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub has_relay: bool,
    pub has_direct: bool,
    pub direct_addr_count: usize,
}

/// Complete shape of a node's observable distribution state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionNodeSnapshot {
    /// This node's ID (hex-encoded).
    pub node_id: String,
    /// This node's listen address (filled by driver, None for protocol-only snapshots).
    pub listen_addr: Option<String>,

    // ─── SWIM membership ─────────────────────────────────────────────
    /// All known members with their state.
    pub members: Vec<MemberInfo>,
    /// Count of alive members.
    pub alive_count: usize,
    /// Count of suspected members.
    pub suspect_count: usize,
    /// Count of dead members.
    pub dead_count: usize,

    // ─── Location cache ──────────────────────────────────────────────
    /// Number of entries in the LRU cache.
    pub cache_size: usize,
    /// All cache entries (actor → node).
    pub cache_entries: Vec<CacheEntryInfo>,

    // ─── Directory ───────────────────────────────────────────────────
    /// Number of actor→host routes this node currently knows (the converged
    /// directory `RouteView` size).
    #[serde(default)]
    pub directory_route_count: usize,

    // ─── Registry ────────────────────────────────────────────────────
    /// Number of entries in the cluster registry (including tombstones).
    pub registry_size: usize,
    /// Number of tombstoned entries.
    pub registry_tombstones: usize,
    /// All registry entries.
    pub registry_entries: Vec<RegistryEntryInfo>,

    // ─── Gossip pairs ────────────────────────────────────────────────
    /// Recent SWIM probe targets (most recent last).
    pub recent_probe_targets: Vec<String>,

    // ─── Peer auth ──────────────────────────────────────────────────
    /// "open" or "allow-list".
    #[serde(default)]
    pub peer_auth_mode: String,
    /// Number of authorized peers (None if open mode).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorized_peer_count: Option<usize>,

    /// Human-readable node name (e.g. "swift-falcon").
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub node_name: Option<String>,

    /// Base58-encoded invite code for this node.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub invite_code: Option<String>,

    /// This node's relay URL, if running an embedded relay server.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub relay_url: Option<String>,

    /// Build version string (e.g. "branch @ hash").
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub version: Option<String>,

    /// Real-time join statuses for peers being connected to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub join_statuses: Vec<JoinStatusInfo>,
}

fn node_id_hex(id: &NodeId) -> String {
    id.0.iter().map(|b| format!("{:02x}", b)).collect()
}

impl DistributionNodeSnapshot {
    /// An empty shape for `node_id`, with every protocol field zeroed/empty.
    pub fn empty(node_id: NodeId) -> Self {
        Self {
            node_id: node_id_hex(&node_id),
            listen_addr: None,
            members: Vec::new(),
            alive_count: 0,
            suspect_count: 0,
            dead_count: 0,
            cache_size: 0,
            cache_entries: Vec::new(),
            directory_route_count: 0,
            registry_size: 0,
            registry_tombstones: 0,
            registry_entries: Vec::new(),
            recent_probe_targets: Vec::new(),
            peer_auth_mode: "open".into(),
            authorized_peer_count: None,
            node_name: None,
            invite_code: None,
            relay_url: None,
            version: None,
            join_statuses: Vec::new(),
        }
    }
}
