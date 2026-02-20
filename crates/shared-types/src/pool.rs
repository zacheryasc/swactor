//! Shared types for the pooled datastore protocol.
//!
//! Types live in `shared-types` because both `distribution` and `datastore`
//! depend on them.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::ContentHash;

// Re-export NodeId-shaped bytes — pool uses the same 32-byte node identifier.
// The actual NodeId type lives in `distribution::types`, but we use raw [u8; 32]
// here to avoid a circular dependency. Callers convert as needed.

// ─── PoolId ────────────────────────────────────────────────────────────────

/// Unique pool identifier: blake3(name_bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PoolId(pub [u8; 32]);

impl PoolId {
    /// Create a pool ID from a human-readable name.
    pub fn from_name(name: &str) -> Self {
        PoolId(*blake3::hash(name.as_bytes()).as_bytes())
    }

    /// Encode as lowercase hex string.
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in &self.0 {
            use fmt::Write;
            write!(s, "{:02x}", b).unwrap();
        }
        s
    }

    /// Parse a 64-character hex string into a PoolId.
    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 64 {
            return None;
        }
        let mut bytes = [0u8; 32];
        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            let hi = hex_digit(chunk[0])?;
            let lo = hex_digit(chunk[1])?;
            bytes[i] = (hi << 4) | lo;
        }
        Some(PoolId(bytes))
    }
}

impl fmt::Display for PoolId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0[..8] {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "\u{2026}")
    }
}

// ─── Pool Membership ───────────────────────────────────────────────────────

/// Whether a node is actively participating in a pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PoolMemberState {
    Active,
    Left,
}

/// A node's membership in a pool. Higher generation always wins.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolMemberEntry {
    pub pool_id: PoolId,
    pub node_id: [u8; 32],
    pub state: PoolMemberState,
    pub generation: u64,
}

// ─── Pool Capacity ─────────────────────────────────────────────────────────

/// A node's storage capacity announcement. Higher generation always wins.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolCapacityEntry {
    pub pool_id: PoolId,
    pub node_id: [u8; 32],
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub generation: u64,
}

// ─── Content Location ──────────────────────────────────────────────────────

/// Where a content hash is stored. Key: (pool_id, content_hash, node_id).
/// Higher generation wins. Tombstones indicate deletion.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContentLocationEntry {
    pub pool_id: PoolId,
    pub content_hash: ContentHash,
    pub node_id: [u8; 32],
    pub generation: u64,
    pub tombstone: bool,
}

// ─── Pool ACL ──────────────────────────────────────────────────────────────

/// Authorization for a node to join a pool. Higher generation wins.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolACLEntry {
    pub pool_id: PoolId,
    pub node_id: [u8; 32],
    pub granted_by: [u8; 32],
    pub generation: u64,
    pub revoked: bool,
}

// ─── Tagged Union ──────────────────────────────────────────────────────────

/// All pool entry variants, used for gossip serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PoolEntry {
    Membership(PoolMemberEntry),
    Capacity(PoolCapacityEntry),
    ContentLocation(ContentLocationEntry),
    ACL(PoolACLEntry),
}

// ─── Pool Config ───────────────────────────────────────────────────────────

/// Configuration for a pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    pub pool_name: String,
    pub pool_id: PoolId,
    /// Total bytes this node pledges to the pool.
    pub capacity_bytes: u64,
    /// Tombstone TTL in ticks before GC.
    pub tombstone_ttl: u64,
    /// GC interval in ticks.
    pub gc_interval: u64,
    /// Dissemination multiplier (Λ).
    pub dissemination_lambda: usize,
}

impl PoolConfig {
    pub fn new(pool_name: &str, capacity_bytes: u64) -> Self {
        Self {
            pool_name: pool_name.to_string(),
            pool_id: PoolId::from_name(pool_name),
            capacity_bytes,
            tombstone_ttl: 3600,
            gc_interval: 1000,
            dissemination_lambda: 3,
        }
    }
}

// ─── Helpers ───────────────────────────────────────────────────────────────

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_id_from_name_is_deterministic() {
        let a = PoolId::from_name("test-pool");
        let b = PoolId::from_name("test-pool");
        assert_eq!(a, b);
    }

    #[test]
    fn pool_id_different_names_differ() {
        let a = PoolId::from_name("pool-a");
        let b = PoolId::from_name("pool-b");
        assert_ne!(a, b);
    }

    #[test]
    fn pool_id_hex_roundtrip() {
        let id = PoolId::from_name("roundtrip-test");
        let hex = id.to_hex();
        let recovered = PoolId::from_hex(&hex).unwrap();
        assert_eq!(id, recovered);
    }

    #[test]
    fn pool_entry_serde_roundtrip() {
        let entry = PoolEntry::Membership(PoolMemberEntry {
            pool_id: PoolId::from_name("test"),
            node_id: [1u8; 32],
            state: PoolMemberState::Active,
            generation: 1,
        });
        let bytes = serde_json::to_vec(&entry).unwrap();
        let recovered: PoolEntry = serde_json::from_slice(&bytes).unwrap();
        match recovered {
            PoolEntry::Membership(m) => {
                assert_eq!(m.node_id, [1u8; 32]);
                assert_eq!(m.state, PoolMemberState::Active);
                assert_eq!(m.generation, 1);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn higher_generation_wins_for_membership() {
        let old = PoolMemberEntry {
            pool_id: PoolId::from_name("test"),
            node_id: [1u8; 32],
            state: PoolMemberState::Active,
            generation: 1,
        };
        let new = PoolMemberEntry {
            pool_id: PoolId::from_name("test"),
            node_id: [1u8; 32],
            state: PoolMemberState::Left,
            generation: 2,
        };
        assert!(new.generation > old.generation);
    }

    #[test]
    fn content_location_tombstone_semantics() {
        let live = ContentLocationEntry {
            pool_id: PoolId::from_name("test"),
            content_hash: ContentHash::of(b"hello"),
            node_id: [1u8; 32],
            generation: 1,
            tombstone: false,
        };
        let dead = ContentLocationEntry {
            pool_id: PoolId::from_name("test"),
            content_hash: ContentHash::of(b"hello"),
            node_id: [1u8; 32],
            generation: 2,
            tombstone: true,
        };
        assert!(!live.tombstone);
        assert!(dead.tombstone);
        assert!(dead.generation > live.generation);
    }
}
