use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;

pub use crate::crypto::Signature;
pub use swactor_transport::NodeId;

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
    pub state: MemberState,
    /// Incarnation number — bumped by the node itself to refute suspicion.
    pub incarnation: u64,
}

// ─── DirectoryEntry ─────────────────────────────────────────────────────────

/// Signed binding of an actor address to a node.
///
/// Disseminated through the gossip directory (see `docs/DIRECTORY.md` and
/// [`crate::directory_actor`]). The spawning node signs the entry over
/// `(actor, host, generation)` to prove it owns the actor.
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
