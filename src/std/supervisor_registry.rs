use std::sync::RwLock;

use crate::actor::ActorAddress;
use crate::AddrMap;

/// Maps supervised children to their supervisor.
///
/// Follows the same pattern as `MonitorRegistry`, `GroupRegistry`, etc.
/// Entries are added in `Supervisor::start_child` and cleaned up on actor death.
pub struct SupervisorRegistry {
    /// child_addr → supervisor_addr
    children: RwLock<AddrMap<ActorAddress>>,
}

impl Default for SupervisorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SupervisorRegistry {
    pub fn new() -> Self {
        Self {
            children: RwLock::new(AddrMap::default()),
        }
    }

    /// Register a supervisor → child relationship.
    pub fn register(&self, supervisor: ActorAddress, child: ActorAddress) {
        self.children.write().unwrap().insert(child, supervisor);
    }

    /// Look up the supervisor of a child actor.
    pub fn lookup(&self, child: &ActorAddress) -> Option<ActorAddress> {
        self.children.read().unwrap().get(child).copied()
    }

    /// Remove entries where `dead_addr` is either a child or a supervisor.
    pub fn cleanup(&self, dead_addr: &ActorAddress) {
        let mut map = self.children.write().unwrap();
        // Remove the dead actor as a child
        map.remove(dead_addr);
        // Remove all children supervised by the dead actor
        map.retain(|_, supervisor| supervisor != dead_addr);
    }
}
