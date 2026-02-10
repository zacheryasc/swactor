use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use crate::actor::{ActorAddress, AnyActor, ContextInner, Ctx};
use crate::channel::Receiver;
use crate::delivery::{Envelope, TickContext, WorkerId};
use crate::stats::{TickTiming, WorkerStats};
use crate::Error;

/// A worker owns a set of actors and runs them in a loop.
pub(crate) struct Worker {
    pub(crate) id: WorkerId,
    pub(crate) pool: ActorPool,
    transfer_rx: Receiver<Envelope>,
    spawn_rx: Receiver<(ActorAddress, Box<dyn AnyActor>)>,
    stats: Arc<WorkerStats>,
    /// Shared snapshot of per-actor mailbox depths, readable by Runtime::stats().
    mailbox_snapshot: Arc<std::sync::Mutex<Vec<(ActorAddress, usize)>>>,
}

impl Worker {
    pub(crate) fn new(
        id: WorkerId,
        transfer_rx: Receiver<Envelope>,
        spawn_rx: Receiver<(ActorAddress, Box<dyn AnyActor>)>,
        stats: Arc<WorkerStats>,
        mailbox_snapshot: Arc<std::sync::Mutex<Vec<(ActorAddress, usize)>>>,
    ) -> Self {
        Self {
            id,
            pool: ActorPool::new(),
            transfer_rx,
            spawn_rx,
            stats,
            mailbox_snapshot,
        }
    }

    /// Run one iteration of the worker loop. Returns `true` if any work was done.
    pub(crate) fn tick_once(&mut self, tc: &TickContext) -> bool {
        #[cfg(feature = "tracing")]
        let _span = tracing::trace_span!("worker.tick", worker_id = self.id.0).entered();

        let mut did_work = false;
        let t0 = Instant::now();

        // 1. Drain spawn queue → add actors to pool
        #[cfg(feature = "tracing")]
        let mut spawn_count: usize = 0;
        while let Some((addr, actor)) = self.spawn_rx.try_recv() {
            self.pool.insert(addr, actor);
            #[cfg(feature = "tracing")]
            { spawn_count += 1; }
            did_work = true;
        }
        #[cfg(feature = "tracing")]
        if spawn_count > 0 {
            tracing::debug!(worker_id = self.id.0, count = spawn_count, "worker.spawns_drained");
        }
        let t1 = Instant::now();

        // 2. Drain transfer queue → deliver envelopes to actors
        while let Some(envelope) = self.transfer_rx.try_recv() {
            let dest = envelope.dest();
            let payload = envelope.into_payload();
            self.pool.deliver(&dest, payload);
            did_work = true;
        }
        let t2 = Instant::now();

        // 3. Tick all actors with WorkerContext
        let pending_local: RefCell<Vec<(ActorAddress, Box<dyn Any + Send>)>> =
            RefCell::new(Vec::new());

        let processed;
        {
            let worker_ctx = WorkerContext {
                worker_id: self.id,
                tc,
                pending_local: &pending_local,
                stats: &self.stats,
            };
            processed = self.pool.tick_all(&worker_ctx, &self.stats);
            if processed > 0 {
                did_work = true;
            }
        }
        let t3 = Instant::now();

        #[cfg(feature = "tracing")]
        if processed > 0 {
            tracing::debug!(
                worker_id = self.id.0,
                messages_processed = processed,
                "worker.tick_all"
            );
        }

        // 4. Drain spawn queue again — actors spawned during step 3
        //    must be in the pool before pending_local delivery.
        while let Some((addr, actor)) = self.spawn_rx.try_recv() {
            self.pool.insert(addr, actor);
            did_work = true;
        }
        let t4 = Instant::now();

        // 5. Drain pending_local buffer → deliver to local actors
        let pending = pending_local.into_inner();
        if !pending.is_empty() {
            did_work = true;
        }
        for (addr, msg) in pending {
            self.pool.deliver(&addr, msg);
        }
        let t5 = Instant::now();

        // 6. Publish stats (skip entirely when idle to avoid allocation + mutex)
        if did_work {
            self.stats.num_actors.store(self.pool.len(), Ordering::Relaxed);
            self.stats.total_mailbox_depth.store(self.pool.total_mailbox_depth(), Ordering::Relaxed);
            self.stats.messages_processed.fetch_add(processed as u64, Ordering::Relaxed);

            let mut snap = self.mailbox_snapshot.lock().unwrap();
            self.pool.mailbox_depths_into(&mut snap);
        }

        let t6 = Instant::now();

        // Record tick timing
        let timing = TickTiming {
            phase_us: [
                t1.duration_since(t0).as_micros() as u64,
                t2.duration_since(t1).as_micros() as u64,
                t3.duration_since(t2).as_micros() as u64,
                t4.duration_since(t3).as_micros() as u64,
                t5.duration_since(t4).as_micros() as u64,
                t6.duration_since(t5).as_micros() as u64,
            ],
            messages_processed: processed,
            did_work,
        };
        self.stats.push_tick_timing(timing);

        #[cfg(feature = "tracing")]
        if did_work {
            tracing::debug!(
                worker_id = self.id.0,
                num_actors = self.pool.len(),
                mailbox_depth = self.pool.total_mailbox_depth(),
                messages_processed = processed,
                "worker.stats"
            );
        }

        did_work
    }

    pub(crate) fn run(&mut self, tc: &TickContext, is_running: &AtomicBool) {
        #[cfg(feature = "tracing")]
        let _span = tracing::info_span!("worker.run", worker_id = self.id.0).entered();

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
    stats: &'a WorkerStats,
}

impl ContextInner for WorkerContext<'_> {
    fn send_any(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error> {
        match self.tc.address_map.lookup(&addr) {
            Some(wid) if wid == self.worker_id => {
                self.stats.local_sends.fetch_add(1, Ordering::Relaxed);
                self.pending_local.borrow_mut().push((addr, msg));
                Ok(())
            }
            Some(wid) => {
                self.stats.cross_sends.fetch_add(1, Ordering::Relaxed);
                self.tc.transfer_txs[wid.as_usize()].send(Envelope::new(addr, msg));
                Ok(())
            }
            None => {
                self.stats.inbox_sends.fetch_add(1, Ordering::Relaxed);
                self.tc.route_nonlocal(addr, msg)
            }
        }
    }

    fn spawn_any(&self, addr: ActorAddress, actor: Box<dyn AnyActor>) {
        let worker_id = self.tc.placement.next_worker();
        self.tc.address_map.insert(addr, worker_id);
        self.tc.spawn_txs[worker_id.as_usize()]
            .send((addr, actor))
    }
}

struct ActorSlot {
    mailbox: VecDeque<Box<dyn Any + Send>>,
    actor: Box<dyn AnyActor>,
    poisoned: bool,
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
            mailbox: VecDeque::with_capacity(16),
            actor,
            poisoned: false,
        });
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
    pub fn tick_all(&mut self, inner: &dyn ContextInner, stats: &WorkerStats) -> usize {
        let mut count = 0;
        for (&addr, slot) in self.actors.iter_mut() {
            if slot.poisoned {
                // Discard all messages for poisoned actors
                slot.mailbox.clear();
                continue;
            }
            let ctx = Ctx::new(inner, addr);
            while let Some(msg) = slot.mailbox.pop_front() {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    slot.actor.handle_any(&ctx, msg)
                }));
                match result {
                    Ok(false) => {
                        stats.type_mismatches.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        stats.panics.fetch_add(1, Ordering::Relaxed);
                        eprintln!("swactor: actor {addr} panicked — poisoned, future messages will be discarded");
                        #[cfg(feature = "tracing")]
                        tracing::error!(actor_addr = %addr, "actor.panicked");
                        slot.poisoned = true;
                        slot.mailbox.clear();
                        break;
                    }
                    Ok(true) => {}
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

    /// Fill `out` with per-actor mailbox depths, reusing the existing allocation.
    pub fn mailbox_depths_into(&self, out: &mut Vec<(ActorAddress, usize)>) {
        out.clear();
        out.extend(self.actors.iter().map(|(&addr, slot)| (addr, slot.mailbox.len())));
    }
}
