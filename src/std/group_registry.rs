use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use crate::actor::ActorAddress;
use crate::{AddrBuildHasher, AddrMap, AddrSet};

/// Actor groups (pub-sub). Actors join/leave named groups; messages can be
/// broadcast to all members of a group.
///
/// Groups are created lazily on first join and removed when empty.
pub struct GroupRegistry {
    /// group_name → set of member addresses
    groups: RwLock<HashMap<String, AddrSet>>,
    /// actor_addr → set of group names (reverse map for O(G) cleanup on death)
    memberships: RwLock<AddrMap<HashSet<String>>>,
}

impl Default for GroupRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupRegistry {
    pub fn new() -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
            memberships: RwLock::new(HashMap::with_hasher(AddrBuildHasher)),
        }
    }

    /// Add an actor to a named group. Group is created if it doesn't exist.
    pub fn join(&self, group: String, addr: ActorAddress) {
        self.groups
            .write()
            .unwrap()
            .entry(group.clone())
            .or_insert_with(|| HashSet::with_hasher(AddrBuildHasher))
            .insert(addr);
        self.memberships
            .write()
            .unwrap()
            .entry(addr)
            .or_default()
            .insert(group);
    }

    /// Remove an actor from a named group. Empty groups are auto-deleted.
    pub fn leave(&self, group: &str, addr: &ActorAddress) {
        let mut groups = self.groups.write().unwrap();
        if let Some(members) = groups.get_mut(group) {
            members.remove(addr);
            if members.is_empty() {
                groups.remove(group);
            }
        }
        drop(groups);
        if let Some(membership) = self.memberships.write().unwrap().get_mut(addr) {
            membership.remove(group);
        }
    }

    /// Return all members of a group.
    pub fn members(&self, group: &str) -> Vec<ActorAddress> {
        self.groups
            .read()
            .unwrap()
            .get(group)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Remove a dead actor from all its groups.
    pub fn cleanup(&self, addr: &ActorAddress) {
        let group_names = self.memberships.write().unwrap().remove(addr);
        if let Some(names) = group_names {
            let mut groups = self.groups.write().unwrap();
            for name in names {
                if let Some(members) = groups.get_mut(&name) {
                    members.remove(addr);
                    if members.is_empty() {
                        groups.remove(&name);
                    }
                }
            }
        }
    }

    /// Return all active group names.
    pub fn group_names(&self) -> Vec<String> {
        self.groups.read().unwrap().keys().cloned().collect()
    }
}
