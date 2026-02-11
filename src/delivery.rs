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
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: RwLock::new(HashMap::with_capacity(cap)),
        }
    }

    pub fn insert(&self, addr: ActorAddress, worker: WorkerId) {
        self.inner.write().unwrap().insert(addr, worker);
    }

    pub fn lookup(&self, addr: &ActorAddress) -> Option<WorkerId> {
        self.inner.read().unwrap().get(addr).copied()
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
            Sender::send(self, *typed);
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

    /// Check if an address is registered without consuming a message.
    pub fn contains(&self, addr: &ActorAddress) -> bool {
        self.senders.read().unwrap().contains_key(addr)
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
    pub(crate) stats_hook: Option<&'a dyn crate::stats::StatsHook>,
    #[cfg(feature = "transport")]
    pub(crate) codec_registry: Option<&'a crate::transport::CodecRegistry>,
    #[cfg(feature = "transport")]
    pub(crate) transport_router: Option<&'a crate::transport::TransportRouter>,
}

impl<'a> TickContext<'a> {
    /// Route a message whose destination is not in the local address map.
    /// Tries inbox registry, then remote transport, then falls back to inbox error.
    pub(crate) fn route_nonlocal(
        &self,
        addr: ActorAddress,
        msg: Box<dyn Any + Send>,
    ) -> Result<(), Error> {
        #[cfg(feature = "transport")]
        {
            if self.inbox_registry.contains(&addr) {
                return self.inbox_registry.try_deliver(addr, msg);
            }
            if let (Some(cr), Some(tr)) = (self.codec_registry, self.transport_router) {
                return crate::transport::send_via_transport(addr, msg, cr, tr);
            }
        }
        self.inbox_registry.try_deliver(addr, msg)
    }
}
