use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use crate::actor::{ActorAddress, AnyActor, Message};
use crate::channel::Sender;
use crate::config::RuntimeConfig;
use crate::Error;

// ─── Address Map Types ───────────────────────────────────────────────────────

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

// ─── Delivery Types ──────────────────────────────────────────────────────────

/// A type-erased message envelope for cross-worker delivery.
///
/// Uses `Box` (no atomic refcount) and move semantics (no clone).
pub(crate) struct Envelope {
    dest: ActorAddress,
    payload: Box<dyn Any + Send>,
}

impl Envelope {
    pub fn new(dest: ActorAddress, payload: Box<dyn Any + Send>) -> Self {
        Self { dest, payload }
    }

    pub fn dest(&self) -> ActorAddress {
        self.dest
    }

    pub fn downcast<M: 'static>(self) -> Option<M> {
        self.payload.downcast::<M>().ok().map(|b| *b)
    }

    pub fn into_payload(self) -> Box<dyn Any + Send> {
        self.payload
    }
}

/// Type-erased sender for external inboxes.
pub(crate) trait SenderT: Send + Sync {
    fn try_send_any(&self, msg: Box<dyn Any + Send>);
}

impl<M: Message> SenderT for Sender<M> {
    fn try_send_any(&self, msg: Box<dyn Any + Send>) {
        if let Ok(typed) = msg.downcast::<M>() {
            let _ = Sender::try_send(self, *typed);
        }
    }
}

/// Registry of external inboxes — replaces the Router's role for non-actor receivers.
pub(crate) struct InboxRegistry {
    senders: RwLock<HashMap<ActorAddress, Arc<dyn SenderT>>>,
}

impl InboxRegistry {
    pub fn new() -> Self {
        Self {
            senders: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, addr: ActorAddress, sender: Arc<dyn SenderT>) {
        self.senders.write().unwrap().insert(addr, sender);
    }

    pub fn try_deliver(
        &self,
        addr: ActorAddress,
        msg: Box<dyn Any + Send>,
    ) -> Result<(), Error> {
        let senders = self.senders.read().unwrap();
        if let Some(sender) = senders.get(&addr) {
            sender.try_send_any(msg);
            Ok(())
        } else {
            Err(Error::from("Address not found"))
        }
    }
}

/// Shared state passed to tick_once — single thin pointer avoids register spill.
pub(crate) struct TickContext<'a> {
    pub(crate) address_map: &'a AddressMap,
    pub(crate) transfer_txs: &'a [Sender<Envelope>],
    pub(crate) spawn_txs: &'a [Sender<(ActorAddress, Box<dyn AnyActor>)>],
    pub(crate) placement: &'a Placement,
    pub(crate) inbox_registry: &'a InboxRegistry,
    pub(crate) config: &'a RuntimeConfig,
}

#[cfg(test)]
mod address_map_tests {
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
