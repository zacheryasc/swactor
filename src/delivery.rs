use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::thread::Thread;

use crate::actor::{ActorAddress, AnyActor, Message};
use crate::channel::Sender;
use crate::config::RuntimeConfig;
use crate::stats::WorkerStats;
use crate::worker::WatchRegistry;
use crate::Error;

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
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, _bytes: &[u8]) {
        // Unused — ActorAddress::hash calls write_u64 directly.
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}

/// BuildHasher for creating AddrHasher instances.
#[derive(Default, Clone)]
pub struct AddrBuildHasher;

impl BuildHasher for AddrBuildHasher {
    type Hasher = AddrHasher;

    #[inline]
    fn build_hasher(&self) -> AddrHasher {
        AddrHasher(0)
    }
}

/// HashMap optimized for ActorAddress keys.
/// Uses identity hashing since ActorAddress bytes are already random.
pub type AddrMap<V> = HashMap<ActorAddress, V, AddrBuildHasher>;

/// HashSet optimized for ActorAddress keys.
pub type AddrSet = HashSet<ActorAddress, AddrBuildHasher>;

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
    inner: RwLock<AddrMap<WorkerId>>,
}

impl AddressMap {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: RwLock::new(HashMap::with_capacity_and_hasher(cap, AddrBuildHasher)),
        }
    }

    pub fn insert(&self, addr: ActorAddress, worker: WorkerId) {
        self.inner.write().unwrap().insert(addr, worker);
    }

    pub fn lookup(&self, addr: &ActorAddress) -> Option<WorkerId> {
        self.inner.read().unwrap().get(addr).copied()
    }

    /// Remove an actor address from the map (e.g., after permanent poisoning).
    pub fn remove(&self, addr: &ActorAddress) {
        self.inner.write().unwrap().remove(addr);
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

/// Load-aware actor placement strategy.
///
/// Picks the worker with the lowest load score (actor count + mailbox depth).
/// When all workers have equal load (e.g., before any ticks), falls back to
/// round-robin via a rotating start position for the scan.
pub(crate) struct Placement {
    next: AtomicUsize,
    num_workers: usize,
    worker_stats: Vec<Arc<WorkerStats>>,
}

impl Placement {
    pub fn new(num_workers: usize, worker_stats: Vec<Arc<WorkerStats>>) -> Self {
        Self {
            next: AtomicUsize::new(0),
            num_workers,
            worker_stats,
        }
    }

    pub fn next_worker(&self) -> WorkerId {
        let n = self.num_workers;
        if n == 1 {
            return WorkerId(0);
        }

        // Rotate the scan start for round-robin tie-breaking
        let rr = self.next.fetch_add(1, Ordering::Relaxed);

        let mut best_id = rr % n;
        let mut best_score = usize::MAX;

        for offset in 0..n {
            let i = (rr + offset) % n;
            let actors = self.worker_stats[i].num_actors.load(Ordering::Relaxed);
            let depth = self.worker_stats[i].total_mailbox_depth.load(Ordering::Relaxed);
            let score = actors + depth;
            if score < best_score {
                best_score = score;
                best_id = i;
            }
        }

        WorkerId(best_id)
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
    senders: RwLock<AddrMap<Arc<dyn SenderT>>>,
}

impl InboxRegistry {
    pub fn new() -> Self {
        Self {
            senders: RwLock::new(HashMap::with_hasher(AddrBuildHasher)),
        }
    }

    pub fn register(&self, addr: ActorAddress, sender: Arc<dyn SenderT>) {
        self.senders.write().unwrap().insert(addr, sender);
    }

    /// Check if an address is registered without consuming a message.
    #[cfg(feature = "transport")]
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
    pub(crate) extension: Option<&'a dyn crate::extension::RuntimeExtension>,
    pub(crate) stats_hook: Option<&'a dyn crate::stats::StatsHook>,
    /// Thread handles for waking parked workers on cross-worker sends.
    pub(crate) worker_threads: &'a [OnceLock<Thread>],
    pub(crate) watch_registry: Option<&'a Arc<Mutex<WatchRegistry>>>,
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
