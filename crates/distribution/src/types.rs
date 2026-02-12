use std::fmt;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;

// ─── NodeId ─────────────────────────────────────────────────────────────────

/// A node's identity — the raw bytes of an ed25519 public key.
///
/// Also serves as the Kademlia key for XOR distance calculations.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub [u8; 32]);

impl NodeId {
    /// XOR distance between two node IDs (Kademlia metric).
    pub fn xor_distance(&self, other: &NodeId) -> [u8; 32] {
        let mut out = [0u8; 32];
        for i in 0..32 {
            out[i] = self.0[i] ^ other.0[i];
        }
        out
    }

    /// Number of leading zero bits in the XOR distance to `other`.
    /// Returns 0..=256. Used to select the k-bucket index.
    pub fn xor_leading_zeros(&self, other: &NodeId) -> u32 {
        let dist = self.xor_distance(other);
        let mut zeros = 0u32;
        for byte in dist {
            if byte == 0 {
                zeros += 8;
            } else {
                zeros += byte.leading_zeros();
                break;
            }
        }
        zeros
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId(")?;
        for b in &self.0[..4] {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "\u{2026})")
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0[..8] {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "\u{2026}")
    }
}

// ─── Signature ──────────────────────────────────────────────────────────────

/// An ed25519 signature (64 bytes).
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Signature(pub [u8; 64]);

impl Serialize for Signature {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        if bytes.len() != 64 {
            return Err(serde::de::Error::custom(format!(
                "expected 64 bytes for Signature, got {}",
                bytes.len()
            )));
        }
        let mut arr = [0u8; 64];
        arr.copy_from_slice(&bytes);
        Ok(Signature(arr))
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Sig(")?;
        for b in &self.0[..4] {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "\u{2026})")
    }
}

// ─── MemberState ────────────────────────────────────────────────────────────

/// SWIM membership state for a node.
///
/// Ordering: `Dead > Suspect > Alive` — within the same generation,
/// a higher-priority state wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemberState {
    Alive,
    Suspect,
    Dead,
}

impl MemberState {
    /// SWIM override priority: Dead (2) > Suspect (1) > Alive (0).
    pub fn priority(self) -> u8 {
        match self {
            MemberState::Alive => 0,
            MemberState::Suspect => 1,
            MemberState::Dead => 2,
        }
    }
}

impl PartialOrd for MemberState {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MemberState {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority().cmp(&other.priority())
    }
}

// ─── NodeRecord ─────────────────────────────────────────────────────────────

/// SWIM membership record for a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRecord {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub state: MemberState,
    /// Incarnation number — bumped by the node itself to refute suspicion.
    pub incarnation: u64,
}

// ─── DirectoryEntry ─────────────────────────────────────────────────────────

/// Signed binding of an actor address to a node.
///
/// Stored in the Kademlia directory. The spawning node signs the entry
/// to prove it owns the actor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub actor_addr: ActorAddress,
    pub node_id: NodeId,
    /// Generation counter — incremented on re-registration (e.g. after restart).
    pub generation: u64,
    pub signature: Signature,
}

/// The signable payload of a directory entry (excludes the signature itself).
#[derive(Serialize)]
pub struct DirectoryEntryPayload {
    pub actor_addr: ActorAddress,
    pub node_id: NodeId,
    pub generation: u64,
}

impl DirectoryEntry {
    /// Extract the signable payload.
    pub fn payload(&self) -> DirectoryEntryPayload {
        DirectoryEntryPayload {
            actor_addr: self.actor_addr,
            node_id: self.node_id,
            generation: self.generation,
        }
    }
}
