use std::collections::HashMap;
use std::sync::RwLock;

use crate::actor::ActorAddress;
use crate::{AddrBuildHasher, AddrMap};

/// Named actor registry — maps human-readable names to actor addresses.
///
/// `RwLock<HashMap>` — same pattern as `AddressMap`. Write-rare (spawn/death),
/// read-often (lookup). A reverse map enables O(1) cleanup on actor death.
pub struct NameRegistry {
    names: RwLock<HashMap<String, ActorAddress>>,
    reverse: RwLock<AddrMap<String>>,
}

impl Default for NameRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl NameRegistry {
    pub fn new() -> Self {
        Self {
            names: RwLock::new(HashMap::new()),
            reverse: RwLock::new(HashMap::with_hasher(AddrBuildHasher)),
        }
    }

    /// Register a name → address mapping. Returns `Err` if the name is already taken.
    pub fn register(&self, name: String, addr: ActorAddress) -> Result<(), crate::Error> {
        let mut names = self.names.write().unwrap();
        if names.contains_key(&name) {
            return Err(crate::Error::from("Name already registered"));
        }
        names.insert(name.clone(), addr);
        drop(names);
        self.reverse.write().unwrap().insert(addr, name);
        Ok(())
    }

    /// Look up an actor address by name.
    pub fn lookup(&self, name: &str) -> Option<ActorAddress> {
        self.names.read().unwrap().get(name).copied()
    }

    /// Unregister a name, returning the address it was bound to.
    pub fn unregister(&self, name: &str) -> Option<ActorAddress> {
        let addr = self.names.write().unwrap().remove(name)?;
        self.reverse.write().unwrap().remove(&addr);
        Some(addr)
    }

    /// Remove a name by address (called on actor death for auto-cleanup).
    pub fn unregister_by_addr(&self, addr: &ActorAddress) {
        if let Some(name) = self.reverse.write().unwrap().remove(addr) {
            self.names.write().unwrap().remove(&name);
        }
    }

    /// Look up the name bound to an actor address (reverse lookup).
    pub fn lookup_by_addr(&self, addr: &ActorAddress) -> Option<String> {
        self.reverse.read().unwrap().get(addr).cloned()
    }

    /// Return all registered names.
    pub fn registered_names(&self) -> Vec<String> {
        self.names.read().unwrap().keys().cloned().collect()
    }
}
