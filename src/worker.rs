use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use crate::actor::{ActorAddress, AnyActor, ContextInner, Ctx};
use crate::channel::Receiver;
use crate::delivery::{Envelope, TickContext, WorkerId};
use crate::stats::WorkerStats;
use crate::Error;

/// A worker owns a set of actors and runs them in a loop.
pub(crate) struct Worker {
    pub(crate) id: WorkerId,
    pub(crate) pool: ActorPool,
    transfer_rx: Receiver<Envelope>,
    spawn_rx: Receiver<(ActorAddress, Box<dyn AnyActor>)>,
    stats: Arc<WorkerStats>,
}

impl Worker {
    pub(crate) fn new(
        id: WorkerId,
        transfer_rx: Receiver<Envelope>,
        spawn_rx: Receiver<(ActorAddress, Box<dyn AnyActor>)>,
        stats: Arc<WorkerStats>,
    ) -> Self {
        Self {
            id,
            pool: ActorPool::new(),
            transfer_rx,
            spawn_rx,
            stats,
        }
    }

    /// Run one iteration of the worker loop. Returns `true` if any work was done.
    pub(crate) fn tick_once(&mut self, tc: &TickContext) -> bool {
        let mut did_work = false;

        // 1. Drain spawn queue → add actors to pool
        while let Some((addr, actor)) = self.spawn_rx.try_recv() {
            self.pool.insert(addr, actor);
            did_work = true;
        }

        // 2. Drain transfer queue → deliver envelopes to actors
        while let Some(envelope) = self.transfer_rx.try_recv() {
            let dest = envelope.dest();
            let payload = envelope.into_payload();
            self.pool.deliver(&dest, payload);
            did_work = true;
        }

        // 3. Tick all actors with WorkerContext
        let pending_local: RefCell<Vec<(ActorAddress, Box<dyn Any + Send>)>> =
            RefCell::new(Vec::new());

        let processed;
        {
            let worker_ctx = WorkerContext {
                worker_id: self.id,
                tc,
                pending_local: &pending_local,
            };
            processed = self.pool.tick_all(&worker_ctx);
            if processed > 0 {
                did_work = true;
            }
        }

        // 4. Drain spawn queue again — actors spawned during step 3
        //    must be in the pool before pending_local delivery.
        while let Some((addr, actor)) = self.spawn_rx.try_recv() {
            self.pool.insert(addr, actor);
            did_work = true;
        }

        // 5. Drain pending_local buffer → deliver to local actors
        let pending = pending_local.into_inner();
        if !pending.is_empty() {
            did_work = true;
        }
        for (addr, msg) in pending {
            self.pool.deliver(&addr, msg);
        }

        // 6. Publish stats
        self.stats.num_actors.store(self.pool.len(), Ordering::Relaxed);
        self.stats.total_mailbox_depth.store(self.pool.total_mailbox_depth(), Ordering::Relaxed);
        self.stats.messages_processed.fetch_add(processed as u64, Ordering::Relaxed);

        did_work
    }

    pub(crate) fn run(&mut self, tc: &TickContext, is_running: &AtomicBool) {
        let backoff = &tc.config.backoff_policy;
        let mut idle_count: u32 = 0;
        while is_running.load(Ordering::Acquire) {
            let did_work = self.tick_once(tc);
            if did_work {
                idle_count = 0;
            } else {
                idle_count = idle_count.saturating_add(1);
                if idle_count < backoff.spin_threshold {
                    // Hot spin
                } else if idle_count < backoff.yield_threshold {
                    thread::yield_now();
                } else {
                    let micros = std::cmp::min(
                        (idle_count - backoff.yield_threshold) as u64 * backoff.sleep_increment_us,
                        backoff.sleep_max_us,
                    );
                    thread::sleep(std::time::Duration::from_micros(micros));
                }
            }
        }
    }
}

/// The `ContextInner` impl for worker threads.
///
/// Same-worker sends are buffered in `pending_local` (delivered after current tick round).
/// Cross-worker sends go through the transfer queue.
struct WorkerContext<'a> {
    worker_id: WorkerId,
    tc: &'a TickContext<'a>,
    pending_local: &'a RefCell<Vec<(ActorAddress, Box<dyn Any + Send>)>>,
}

impl ContextInner for WorkerContext<'_> {
    fn send_any(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error> {
        match self.tc.address_map.lookup(&addr) {
            Some(wid) if wid == self.worker_id => {
                // Same worker: buffer for local delivery (after current tick round)
                self.pending_local.borrow_mut().push((addr, msg));
                Ok(())
            }
            Some(wid) => {
                // Cross worker: envelope through transfer queue
                let envelope = Envelope::new(addr, msg);
                let _ = self.tc.transfer_txs[wid.as_usize()].try_send(envelope);
                Ok(())
            }
            None => {
                // Try inbox registry (external inboxes)
                self.tc.inbox_registry.try_deliver(addr, msg)
            }
        }
    }

    fn spawn_any(&self, addr: ActorAddress, actor: Box<dyn AnyActor>) -> Result<(), Error> {
        let worker_id = self.tc.placement.next_worker();
        self.tc.address_map.insert(addr, worker_id);
        self.tc.spawn_txs[worker_id.as_usize()]
            .try_send((addr, actor))
            .map_err(|_| Error::from("Spawn queue full"))
    }

}

struct ActorSlot {
    mailbox: VecDeque<Box<dyn Any + Send>>,
    actor: Box<dyn AnyActor>,
}

/// Per-worker actor storage. Owns per-actor mailboxes.
pub(crate) struct ActorPool {
    actors: HashMap<ActorAddress, ActorSlot>,
}

impl ActorPool {
    pub fn new() -> Self {
        Self {
            actors: HashMap::new(),
        }
    }

    pub fn insert(&mut self, addr: ActorAddress, actor: Box<dyn AnyActor>) {
        self.actors.insert(addr, ActorSlot {
            mailbox: VecDeque::new(),
            actor,
        });
    }

    pub fn remove(&mut self, addr: &ActorAddress) -> Option<Box<dyn AnyActor>> {
        self.actors.remove(addr).map(|slot| slot.actor)
    }

    /// Deliver a type-erased message to the actor at `addr`.
    /// Returns `true` if the actor exists (message is queued; type check deferred to tick).
    pub fn deliver(&mut self, addr: &ActorAddress, msg: Box<dyn Any + Send>) -> bool {
        if let Some(slot) = self.actors.get_mut(addr) {
            slot.mailbox.push_back(msg);
            true
        } else {
            false
        }
    }

    /// Tick all actors in the pool. Returns the number of messages processed.
    pub fn tick_all(&mut self, inner: &dyn ContextInner) -> usize {
        let mut count = 0;
        for (&addr, slot) in self.actors.iter_mut() {
            let ctx = Ctx::new(inner, addr);
            while let Some(msg) = slot.mailbox.pop_front() {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    slot.actor.handle_any(&ctx, msg);
                }));
                if result.is_err() {
                    eprintln!("swactor: actor {addr} panicked in handler");
                }
                count += 1;
            }
        }
        count
    }

    pub fn len(&self) -> usize {
        self.actors.len()
    }

    pub fn total_mailbox_depth(&self) -> usize {
        self.actors.values().map(|slot| slot.mailbox.len()).sum()
    }
}



