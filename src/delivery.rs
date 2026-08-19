use crate::Error;
use crate::actor::{ActorAddress, Message, SpawnRequest};
use crate::channel::Sender;
use crate::config::RuntimeConfig;
use crate::stats::{StatsHook, WorkerStats};
use parking_lot::RwLock;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;

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

// ─── Worker identity ────────────────────────────────────────────────────────

/// Opaque internal identity of a logical worker within one runtime.
///
/// Created during runtime construction and used only to index the arrays
/// (transfer/spawn/admin producers and per-worker stats) that belong to that
/// same runtime. It is intentionally `pub(crate)`: no code outside core
/// constructs or compares `WorkerId` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct WorkerId(pub(crate) usize);

impl WorkerId {
    pub(crate) fn index(self) -> usize {
        self.0
    }
}

// ─── Address Registry ───────────────────────────────────────────────────────

/// Routes actor addresses to their owning logical worker.
///
/// `RwLock<AddrMap<WorkerId>>` — zero contention for parallel reads; writes
/// happen only at spawn (insert) and cleanup (remove).
pub(crate) struct AddressMap {
    inner: RwLock<AddrMap<WorkerId>>,
}

impl AddressMap {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: RwLock::new(HashMap::with_capacity_and_hasher(capacity, AddrBuildHasher)),
        }
    }

    /// Number of routed actor addresses (runtime-wide actor count).
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }
    /// Record that `addr` lives on `worker`.
    pub fn insert(&self, addr: ActorAddress, worker: WorkerId) {
        self.inner.write().insert(addr, worker);
    }

    /// Resolve the owning worker for `addr`, if it is a local actor.
    pub fn worker_of(&self, addr: &ActorAddress) -> Option<WorkerId> {
        self.inner.read().get(addr).copied()
    }

    /// Remove the routing entry for `addr` (called when the actor terminates).
    pub fn remove(&self, addr: &ActorAddress) {
        self.inner.write().remove(addr);
    }

    /// Iterate `(address, worker)` pairs for stats reporting.
    pub fn placements(&self) -> Vec<(ActorAddress, WorkerId)> {
        self.inner.read().iter().map(|(&a, &w)| (a, w)).collect()
    }
}

// ─── Delivery Types ──────────────────────────────────────────────────────────

/// A type-erased message envelope for depositing into a worker's transfer queue.
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

/// Shared routing context passed into a worker pass.
///
/// Borrows the runtime-wide shared state plus the per-worker slices needed to
/// route messages. The current worker's identity (`worker_id`) selects its own
/// producer handles; cross-worker sends index `transfer_txs` by the target's
/// `WorkerId`.
pub(crate) struct TickContext<'a> {
    pub(crate) address_map: &'a AddressMap,
    pub(crate) spawn_txs: &'a [Sender<SpawnRequest>],
    pub(crate) transfer_txs: &'a [Sender<Envelope>],
    pub(crate) inbox_registry: &'a InboxRegistry,
    pub(crate) config: &'a RuntimeConfig,
    pub(crate) extension: Option<&'a dyn crate::extension::RuntimeExtension>,
    pub(crate) process_output_observer:
        Option<&'a Arc<dyn crate::process_observer::ProcessOutputObserver>>,
    pub(crate) stats_hook: Option<&'a dyn StatsHook>,
    pub(crate) worker_stats: &'a WorkerStats,
    pub(crate) num_workers: usize,
    pub(crate) worker_id: WorkerId,
    pub(crate) created_at: crate::Instant,
    #[cfg(feature = "transport")]
    pub(crate) remote_sink: Option<&'a dyn crate::runtime::RemoteSink>,
}

impl<'a> TickContext<'a> {
    pub(crate) fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    /// Resolve the owning worker for `addr`, if it is a local actor.
    pub(crate) fn worker_of(&self, addr: &ActorAddress) -> Option<WorkerId> {
        self.address_map.worker_of(addr)
    }

    /// Borrow the transfer producer for `worker`.
    pub(crate) fn transfer_tx(&self, worker: WorkerId) -> &'a Sender<Envelope> {
        &self.transfer_txs[worker.index()]
    }

    /// Borrow the spawn producer for `worker`.
    pub(crate) fn spawn_tx(&self, worker: WorkerId) -> &'a Sender<SpawnRequest> {
        &self.spawn_txs[worker.index()]
    }

    /// Route a message whose destination is not a local actor address.
    /// Tries the process-local inbox registry, then remote transport, then
    /// falls back to an inbox error.
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
