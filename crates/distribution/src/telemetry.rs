//! Distribution-owned datastream records.

use datastream::Record;
use serde::{Deserialize, Serialize};

/// Transport internals — connectivity to peers and relay.
pub const TRANSPORT_INTERNALS: &str = "transport.internals";
/// Membership / liveness transitions.
pub const MEMBERSHIP: &str = "membership";
/// Distribution-subsystem state: cache, directory, registry, probes, peer auth.
pub const DIST_STATE: &str = "dist.state";

/// Transport-internals record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransportInternals {
    #[serde(default)]
    pub relay_connected: bool,
    #[serde(default)]
    pub direct_peers: u32,
    #[serde(default)]
    pub relay_peers: u32,
    #[serde(default)]
    pub rtt_ms_p50: u32,
}

/// Membership / liveness transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipTransition {
    pub peer: String,
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub reason: String,
}

/// Consolidated distribution-subsystem state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DistributionState {
    #[serde(default)]
    pub cache_size: u32,
    #[serde(default)]
    pub cache_entries: Vec<CacheEntryRec>,
    #[serde(default)]
    pub directory_route_count: u32,
    #[serde(default)]
    pub registry_size: u32,
    #[serde(default)]
    pub registry_tombstones: u32,
    #[serde(default)]
    pub registry_entries: Vec<RegistryEntryRec>,
    #[serde(default)]
    pub recent_probe_targets: Vec<String>,
    /// "open" or "allow-list".
    #[serde(default)]
    pub peer_auth_mode: String,
    /// Authorized peers in allow-list mode; 0 in open mode.
    #[serde(default)]
    pub authorized_peer_count: u32,
}

/// One location-cache entry: which node an actor address resolves to.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEntryRec {
    #[serde(default)]
    pub actor_addr: String,
    #[serde(default)]
    pub node_id: String,
}

/// One cluster-registry entry, possibly a tombstone.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryEntryRec {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub actor_addr: String,
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub tombstone: bool,
}

impl Record for TransportInternals {
    const CHANNEL: &'static str = TRANSPORT_INTERNALS;
}
impl Record for MembershipTransition {
    const CHANNEL: &'static str = MEMBERSHIP;
}
impl Record for DistributionState {
    const CHANNEL: &'static str = DIST_STATE;
}
