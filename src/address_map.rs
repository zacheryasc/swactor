use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::actor::ActorAddress;

/// Identifies a worker thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct WorkerId(pub(crate) usize);

impl WorkerId {
    pub fn as_usize(self) -> usize {
        self.0
    }
}

/// Maps actor addresses to the worker that owns them.
///
/// `RwLock<HashMap>` — zero contention for parallel reads, write-rare (only on spawn).
pub(crate) struct AddressMap {
    inner: RwLock<HashMap<ActorAddress, WorkerId>>,
}

impl AddressMap {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: RwLock::new(HashMap::with_capacity(cap)),
        }
    }

    pub fn insert(&self, addr: ActorAddress, worker: WorkerId) {
        self.inner.write().unwrap().insert(addr, worker);
    }

    pub fn remove(&self, addr: &ActorAddress) {
        self.inner.write().unwrap().remove(addr);
    }

    pub fn lookup(&self, addr: &ActorAddress) -> Option<WorkerId> {
        self.inner.read().unwrap().get(addr).copied()
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    /// Returns a snapshot of all (address, worker) pairs.
    pub fn snapshot(&self) -> Vec<(ActorAddress, WorkerId)> {
        self.inner
            .read()
            .unwrap()
            .iter()
            .map(|(addr, wid)| (*addr, *wid))
            .collect()
    }
}

/// Round-robin actor placement strategy.
pub(crate) struct Placement {
    next: AtomicUsize,
    num_workers: usize,
}

impl Placement {
    pub fn new(num_workers: usize) -> Self {
        Self {
            next: AtomicUsize::new(0),
            num_workers,
        }
    }

    pub fn next_worker(&self) -> WorkerId {
        let id = self.next.fetch_add(1, Ordering::Relaxed) % self.num_workers;
        WorkerId(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_lookup() {
        let map = AddressMap::new();
        let addr = ActorAddress::default();
        let wid = WorkerId(3);
        map.insert(addr, wid);
        assert_eq!(map.lookup(&addr), Some(wid));
    }

    #[test]
    fn lookup_missing_returns_none() {
        let map = AddressMap::new();
        let addr = ActorAddress::default();
        assert_eq!(map.lookup(&addr), None);
    }

    #[test]
    fn remove_works() {
        let map = AddressMap::new();
        let addr = ActorAddress::default();
        map.insert(addr, WorkerId(0));
        map.remove(&addr);
        assert_eq!(map.lookup(&addr), None);
    }

    #[test]
    fn len_tracks_entries() {
        let map = AddressMap::with_capacity(10);
        assert_eq!(map.len(), 0);
        let addr1 = ActorAddress::default();
        map.insert(addr1, WorkerId(0));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn round_robin() {
        let p = Placement::new(3);
        assert_eq!(p.next_worker(), WorkerId(0));
        assert_eq!(p.next_worker(), WorkerId(1));
        assert_eq!(p.next_worker(), WorkerId(2));
        assert_eq!(p.next_worker(), WorkerId(0));
    }
}
