use std::sync::RwLock;

use crate::actor::ActorAddress;
use crate::{AddrMap, AddrSet};

/// Tracks parent → children relationships for orphan cleanup.
///
/// When a parent dies, unsupervised children are stopped automatically.
/// Entries are added in `on_spawn` and cleaned up on actor death.
pub struct ChildrenRegistry {
    /// parent_addr → set of child addresses
    children: RwLock<AddrMap<AddrSet>>,
}

impl Default for ChildrenRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ChildrenRegistry {
    pub fn new() -> Self {
        Self {
            children: RwLock::new(AddrMap::default()),
        }
    }

    /// Register a parent → child relationship.
    pub fn register(&self, parent: ActorAddress, child: ActorAddress) {
        self.children
            .write()
            .unwrap()
            .entry(parent)
            .or_default()
            .insert(child);
    }

    /// Remove and return all children of a parent (for orphan handling).
    pub fn take_children(&self, parent: &ActorAddress) -> Vec<ActorAddress> {
        self.children
            .write()
            .unwrap()
            .remove(parent)
            .map(|set| set.into_iter().collect())
            .unwrap_or_default()
    }

    /// Clean up entries for dead actors (as both parent and child).
    pub fn cleanup(&self, dead: &[ActorAddress]) {
        let mut map = self.children.write().unwrap();
        for addr in dead {
            // Remove as parent
            map.remove(addr);
            // Remove as child from any parent's set
            for set in map.values_mut() {
                set.remove(addr);
            }
        }
        // Remove empty parent entries
        map.retain(|_, set| !set.is_empty());
    }
}
