//! SWIM membership CRDT.
//!
//! Each node maintains a map of `NodeId → (MemberState, incarnation)`.
//! The merge rule is:
//! 1. Higher incarnation wins unconditionally.
//! 2. Same incarnation: higher-priority state wins (Dead > Suspect > Alive).

use std::collections::HashMap;

use crate::types::{MemberState, NodeId, NodeRecord};

/// A single membership entry.
#[derive(Debug, Clone)]
pub struct MemberEntry {
    pub node_id: NodeId,
    pub state: MemberState,
    pub incarnation: u64,
}

impl MemberEntry {
    pub fn to_record(&self) -> NodeRecord {
        NodeRecord {
            node_id: self.node_id,
            state: self.state,
            incarnation: self.incarnation,
        }
    }
}

/// The membership list — the core CRDT of the SWIM protocol.
pub struct MemberList {
    /// Our own node identity.
    self_id: NodeId,
    /// Our own incarnation number.
    self_incarnation: u64,
    /// All known members (excluding self).
    members: HashMap<NodeId, MemberEntry>,
}

impl MemberList {
    pub fn new(self_id: NodeId) -> Self {
        Self {
            self_id,
            self_incarnation: 0,
            members: HashMap::new(),
        }
    }

    pub fn self_id(&self) -> NodeId {
        self.self_id
    }

    pub fn self_incarnation(&self) -> u64 {
        self.self_incarnation
    }

    /// Bump our incarnation number (used to refute suspicion).
    pub fn refute(&mut self) -> u64 {
        self.self_incarnation += 1;
        self.self_incarnation
    }

    /// Get a member's entry.
    pub fn get(&self, id: &NodeId) -> Option<&MemberEntry> {
        self.members.get(id)
    }

    /// All non-dead members (candidates for probing).
    pub fn alive_members(&self) -> Vec<&MemberEntry> {
        self.members
            .values()
            .filter(|e| e.state != MemberState::Dead)
            .collect()
    }

    /// All dead members (candidates for reprobe).
    pub fn dead_members(&self) -> Vec<&MemberEntry> {
        self.members
            .values()
            .filter(|e| e.state == MemberState::Dead)
            .collect()
    }

    /// All members regardless of state.
    pub fn all_members(&self) -> Vec<&MemberEntry> {
        self.members.values().collect()
    }

    /// Number of non-dead members.
    pub fn alive_count(&self) -> usize {
        self.members
            .values()
            .filter(|e| e.state != MemberState::Dead)
            .count()
    }

    /// Total members including dead.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Apply an update. Returns `true` if the state changed (for dissemination).
    ///
    /// SWIM merge semantics:
    /// - Higher incarnation always wins.
    /// - Same incarnation: higher-priority state wins.
    /// - Lower incarnation is ignored.
    pub fn apply(&mut self, node_id: NodeId, state: MemberState, incarnation: u64) -> bool {
        // Don't store entries about ourselves
        if node_id == self.self_id {
            return false;
        }

        match self.members.get_mut(&node_id) {
            Some(existing) => {
                if incarnation > existing.incarnation {
                    existing.state = state;
                    existing.incarnation = incarnation;
                    true
                } else if incarnation == existing.incarnation && state > existing.state {
                    existing.state = state;
                    true
                } else {
                    false
                }
            }
            None => {
                self.members.insert(node_id, MemberEntry {
                    node_id,
                    state,
                    incarnation,
                });
                true
            }
        }
    }

    /// Mark a node as suspect (if currently alive and same/higher incarnation).
    pub fn suspect(&mut self, node_id: NodeId) -> bool {
        if let Some(entry) = self.members.get_mut(&node_id) {
            if entry.state == MemberState::Alive {
                entry.state = MemberState::Suspect;
                return true;
            }
        }
        false
    }

    /// Mark a node as dead. Only transitions from Suspect → Dead,
    /// enforcing the SWIM lifecycle invariant (Alive → Suspect → Dead).
    pub fn declare_dead(&mut self, node_id: NodeId) -> bool {
        if let Some(entry) = self.members.get_mut(&node_id) {
            if entry.state == MemberState::Suspect {
                entry.state = MemberState::Dead;
                return true;
            }
        }
        false
    }

    /// Snapshot for join responses.
    pub fn snapshot(&self) -> Vec<NodeRecord> {
        self.members.values().map(|e| e.to_record()).collect()
    }
}
