use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use parking_lot::RwLock;
use std::sync::Arc;
use crate::Error;
use crate::actor::{ActorAddress, Message, SpawnRequest};
use crate::channel::Sender;
use crate::config::RuntimeConfig;
use crate::stats::WorkerStats;

// ─── Identity Hasher for ActorAddress ───────────────────────────────────────

/// Identity hasher for ActorAddress keys.
///
/// ActorAddress contains 32 cryptographically random bytes. The custom `Hash`
/// impl on ActorAddress writes only the first 8 bytes as a `u64`. This hasher
/// passes that u64 through as the hash value directly — no mixing, no SipHash.
///
/// This is safe because the input is already random (uniform distribution),
/// so additional mixing would be redundant.
pub struct AddrHasher(u64);

impl Hasher for AddrHasher {
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }

    fn write(&mut self, _: &[u8]) {
        // unreachable for ActorAddress (uses write_u64 via custom Hash)
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

/// BuildHasher for creating AddrHasher instances.
#[derive(Default, Clone)]
pub struct AddrBuildHasher;

impl BuildHasher for AddrBuildHasher {
    type Hasher = AddrHasher;

    fn build_hasher(&self) -> Self::Hasher {
        AddrHasher(0)
    }
}

/// HashMap optimized for ActorAddress keys.
/// Uses identity hashing since ActorAddress bytes are already random.
pub type AddrMap<V> = HashMap<ActorAddress, V, AddrBuildHasher>;

/// HashSet optimized for ActorAddress keys.
pub type AddrSet = HashSet<ActorAddress, AddrBuildHasher>;

// ─── Address Registry ───────────────────────────────────────────────────────

/// Tracks which actor addresses belong to this runtime.
///
/// `RwLock<AddrSet>` — zero contention for parallel reads, write-rare (only on spawn).
pub(crate) struct AddressMap {
    inner: RwLock<AddrSet>,
}

impl AddressMap {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: RwLock::new(HashSet::with_capacity_and_hasher(
                capacity,
                AddrBuildHasher,
            )),
        }
    }

    pub fn insert(&self, addr: ActorAddress) {
        self.inner.write().insert(addr);
    }

    pub fn contains(&self, addr: &ActorAddress) -> bool {
        self.inner.read().contains(addr)
    }

    pub fn remove(&self, addr: &ActorAddress) {
        self.inner.write().remove(addr);
    }

    pub fn addresses(&self) -> Vec<ActorAddress> {
        self.inner.read().iter().copied().collect()
    }
}

// ─── Delivery Types ──────────────────────────────────────────────────────────

/// A type-erased message envelope for depositing into the worker's inbox.
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
    senders: RwLock<AddrMap<Arc<dyn SenderT>>>,
}

impl InboxRegistry {
    pub fn new() -> Self {
        Self {
            senders: RwLock::new(HashMap::with_hasher(AddrBuildHasher)),
        }
    }

    pub fn register(&self, addr: ActorAddress, sender: Arc<dyn SenderT>) {
        self.senders.write().insert(addr, sender);
    }

    /// Check if an address is registered without consuming a message.
    #[cfg(feature = "transport")]
    pub fn contains(&self, addr: &ActorAddress) -> bool {
        self.senders.read().contains_key(addr)
    }

    pub fn try_deliver(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error> {
        let senders = self.senders.read();
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
    pub(crate) spawn_tx: &'a Sender<SpawnRequest>,
    pub(crate) transfer_tx: &'a Sender<Envelope>,
    pub(crate) inbox_registry: &'a InboxRegistry,
    pub(crate) config: &'a RuntimeConfig,
    pub(crate) extension: Option<&'a dyn crate::extension::RuntimeExtension>,
    pub(crate) process_output_observer:
        Option<&'a Arc<dyn crate::process_observer::ProcessOutputObserver>>,
    pub(crate) stats_hook: Option<&'a dyn crate::stats::StatsHook>,
    pub(crate) worker_stats: &'a WorkerStats,
    pub(crate) created_at: crate::Instant,
    #[cfg(feature = "transport")]
    pub(crate) remote_sink: Option<&'a dyn crate::runtime::RemoteSink>,
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
            if let Some(sink) = self.remote_sink {
                return sink.send(addr, msg);
            }
        }
        self.inbox_registry.try_deliver(addr, msg)
    }
}
