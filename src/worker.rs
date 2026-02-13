use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use crate::Instant;

use crate::actor::{ActorAddress, AnyActor, ContextInner, Ctx, StopReason, StopSignal};
use crate::channel::Receiver;
use crate::config::MailboxOverflow;
use crate::delivery::{AddrBuildHasher, AddrMap, Envelope, TickContext, WorkerId};
use crate::stats::{ActorSnapshot, TickTiming, WorkerStats};
use crate::Error;

use crate::extension::WorkerExtension;

/// Route a message: try local pool first, then address_map for cross-worker,
/// then inbox_registry for external receivers.
fn route_to_pool_or_remote(
    pool: &mut ActorPool,
    tc: &TickContext,
    dest: ActorAddress,
    msg: Box<dyn Any + Send>,
) {
    if pool.contains(&dest) {
        pool.deliver(&dest, msg);
    } else {
        match tc.address_map.lookup(&dest) {
            Some(wid) => {
                tc.transfer_txs[wid.as_usize()].send(Envelope::new(dest, msg));
                crate::runtime::notify_worker(tc.worker_threads, wid.as_usize());
            }
            None => {
                let _ = tc.inbox_registry.try_deliver(dest, msg);
            }
        }
    }
}

// ─── Worker ─────────────────────────────────────────────────────────────────

/// A worker owns a set of actors and runs them in a loop.
pub(crate) struct Worker {
    pub(crate) id: WorkerId,
    pub(crate) pool: ActorPool,
    transfer_rx: Receiver<Envelope>,
    spawn_rx: Receiver<(ActorAddress, Box<dyn AnyActor>)>,
    stats: Arc<WorkerStats>,
    /// Reusable scratch buffer for building per-actor snapshots.
    snapshot_buf: Vec<ActorSnapshot>,
    /// Per-worker extension (e.g., timer wheel). Created by RuntimeExtension factory.
    pub(crate) worker_ext: Option<Box<dyn WorkerExtension>>,
}

impl Worker {
    pub(crate) fn new(
        id: WorkerId,
        transfer_rx: Receiver<Envelope>,
        spawn_rx: Receiver<(ActorAddress, Box<dyn AnyActor>)>,
        stats: Arc<WorkerStats>,
        default_mailbox_capacity: usize,
        default_overflow_policy: MailboxOverflow,
    ) -> Self {
        Self {
            id,
            pool: ActorPool::new(default_mailbox_capacity, default_overflow_policy),
            transfer_rx,
            spawn_rx,
            stats,
            snapshot_buf: Vec::new(),
            worker_ext: None,
        }
    }

    /// Run one iteration of the worker loop. Returns `true` if any work was done.
    /// Drain the spawn queue, inserting new actors into the pool.
    /// Used in phases 1 and 4 of tick_once.
    fn drain_spawns(&mut self) -> bool {
        let mut did_work = false;
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
        did_work
    }

    /// Phase 7: clean up dead actors, deliver death notifications, GC extension state.
    fn cleanup_dead_actors(&mut self, tc: &TickContext) -> bool {
        let cleanup_pending: RefCell<Vec<(ActorAddress, Box<dyn Any + Send>)>> =
            RefCell::new(Vec::new());
        let cleanup_stops: RefCell<Vec<ActorAddress>> = RefCell::new(Vec::new());
        let cleanup_requests: RefCell<Vec<Box<dyn Any + Send>>> = RefCell::new(Vec::new());
        let dead = {
            let cleanup_ctx = WorkerContext {
                worker_id: self.id,
                tc,
                pending_local: &cleanup_pending,
                stop_requests: &cleanup_stops,
                worker_requests: &cleanup_requests,
                stats: &self.stats,
            };
            self.pool.cleanup_dead(&cleanup_ctx)
        };

        let had_dead = !dead.is_empty();
        if had_dead {
            for &(addr, _) in &dead {
                tc.address_map.remove(&addr);
            }

            if let Some(ext) = tc.extension {
                let notifications = ext.on_actor_death(&dead);
                let dead_addrs: Vec<_> = dead.iter().map(|(a, _)| *a).collect();
                ext.cleanup_dead(&dead_addrs);
                for (dest, msg) in notifications {
                    route_to_pool_or_remote(&mut self.pool, tc, dest, msg);
                }
            }

            self.stats.num_actors.store(self.pool.len(), Ordering::Relaxed);
        }

        // Deliver any messages sent during on_stop callbacks
        for (addr, msg) in cleanup_pending.into_inner() {
            self.pool.deliver(&addr, msg);
        }

        // GC per-worker extension state for dead actors
        if let Some(ext) = &mut self.worker_ext {
            let dead_addrs: Vec<ActorAddress> = dead.iter().map(|(a, _)| *a).collect();
            ext.gc_dead(&dead_addrs);
        }

        had_dead
    }

    pub(crate) fn tick_once(&mut self, tc: &TickContext) -> bool {
        #[cfg(feature = "tracing")]
        let _span = tracing::trace_span!("worker.tick", worker_id = self.id.0).entered();

        let mut did_work = false;
        let t0 = Instant::now();

        // 1. Drain spawn queue → add actors to pool
        did_work |= self.drain_spawns();
        let t1 = Instant::now();

        // 2. Drain transfer queue → deliver envelopes to actors
        while let Some(envelope) = self.transfer_rx.try_recv() {
            let dest = envelope.dest();
            let payload = envelope.into_payload();
            self.pool.deliver(&dest, payload);
            did_work = true;
        }
        let t2 = Instant::now();

        // 2.5. Fire per-worker extension (e.g., timers) → deliver before tick_all
        let ext_msgs: Vec<_> = self.worker_ext.as_mut()
            .map(|ext| ext.on_tick())
            .unwrap_or_default();
        for (dest, msg) in ext_msgs {
            route_to_pool_or_remote(&mut self.pool, tc, dest, msg);
            did_work = true;
        }

        // 3. Tick all actors with WorkerContext
        let pending_local: RefCell<Vec<(ActorAddress, Box<dyn Any + Send>)>> =
            RefCell::new(Vec::new());
        let stop_requests: RefCell<Vec<ActorAddress>> = RefCell::new(Vec::new());
        let worker_requests: RefCell<Vec<Box<dyn Any + Send>>> = RefCell::new(Vec::new());

        let processed;
        {
            let worker_ctx = WorkerContext {
                worker_id: self.id,
                tc,
                pending_local: &pending_local,
                stop_requests: &stop_requests,
                worker_requests: &worker_requests,
                stats: &self.stats,
            };
            processed = self.pool.tick_all(&worker_ctx, &self.stats, tc.config.actor_message_budget, &stop_requests);
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
        did_work |= self.drain_spawns();
        let t4 = Instant::now();

        // 5. Drain pending_local buffer → deliver to local actors
        let pending = pending_local.into_inner();
        if !pending.is_empty() {
            did_work = true;
        }
        for (addr, msg) in pending {
            self.pool.deliver(&addr, msg);
        }

        // 5.5. Process worker extension requests from handlers (e.g., timer scheduling)
        if let Some(ext) = &mut self.worker_ext {
            for request in worker_requests.into_inner() {
                ext.handle_request(request);
            }
        }

        let t5 = Instant::now();

        // 6. Publish stats (skip entirely when idle to avoid allocation + mutex)
        let drops = self.pool.take_drops();
        if did_work {
            self.stats.num_actors.store(self.pool.len(), Ordering::Relaxed);
            self.stats.total_mailbox_depth.store(self.pool.total_mailbox_depth(), Ordering::Relaxed);
            self.stats.messages_processed.fetch_add(processed as u64, Ordering::Relaxed);
            if drops > 0 {
                self.stats.messages_dropped.fetch_add(drops as u64, Ordering::Relaxed);
            }

            if let Some(hook) = tc.stats_hook {
                self.pool.mailbox_depths_into(&mut self.snapshot_buf);
                hook.on_tick(self.id.0, &self.snapshot_buf);
            }
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

        // 7. Clean up poisoned and stopping actors
        did_work |= self.cleanup_dead_actors(tc);

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
                    // park_timeout allows instant wakeup via Thread::unpark()
                    // when new work arrives (send_to/spawn notify the target worker)
                    thread::park_timeout(std::time::Duration::from_micros(micros));
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
    stop_requests: &'a RefCell<Vec<ActorAddress>>,
    worker_requests: &'a RefCell<Vec<Box<dyn Any + Send>>>,
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
                crate::runtime::notify_worker(self.tc.worker_threads, wid.as_usize());
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
            .send((addr, actor));
        crate::runtime::notify_worker(self.tc.worker_threads, worker_id.as_usize());
    }

    fn request_stop(&self, addr: ActorAddress) {
        self.stop_requests.borrow_mut().push(addr);
    }

    fn post_worker_request(&self, request: Box<dyn Any + Send>) {
        self.worker_requests.borrow_mut().push(request);
    }

    fn extension(&self) -> Option<&dyn crate::extension::RuntimeExtension> {
        self.tc.extension
    }
}

struct ActorSlot {
    mailbox: VecDeque<Box<dyn Any + Send>>,
    actor: Box<dyn AnyActor>,
    poisoned: bool,
    /// Graceful stop requested (via StopSignal).
    stopping: bool,
    /// Whether on_start has been called for this actor.
    started: bool,
    last_msg_type: Option<&'static str>,
    messages_processed: u64,
    /// Per-message-type counters (bounded to 32 entries).
    msg_type_counts: HashMap<&'static str, u64>,
    /// Per-actor mailbox capacity. 0 = unbounded.
    mailbox_capacity: usize,
    overflow_policy: MailboxOverflow,
}

/// Per-worker actor storage. Owns per-actor mailboxes.
pub(crate) struct ActorPool {
    actors: AddrMap<ActorSlot>,
    default_mailbox_capacity: usize,
    default_overflow_policy: MailboxOverflow,
    /// Messages dropped this tick due to mailbox overflow. Reset after publishing to stats.
    drops_this_tick: usize,
}

impl ActorPool {
    pub fn new(default_mailbox_capacity: usize, default_overflow_policy: MailboxOverflow) -> Self {
        Self {
            actors: HashMap::with_hasher(AddrBuildHasher),
            default_mailbox_capacity,
            default_overflow_policy,
            drops_this_tick: 0,
        }
    }

    pub fn insert(&mut self, addr: ActorAddress, actor: Box<dyn AnyActor>) {
        let cap = self.default_mailbox_capacity;
        let prealloc = if cap > 0 { cap.min(64) } else { 16 };
        self.actors.insert(addr, ActorSlot {
            mailbox: VecDeque::with_capacity(prealloc),
            actor,
            poisoned: false,
            stopping: false,
            started: false,
            last_msg_type: None,
            messages_processed: 0,
            msg_type_counts: HashMap::new(),
            mailbox_capacity: self.default_mailbox_capacity,
            overflow_policy: self.default_overflow_policy,
        });
    }

    /// Deliver a type-erased message to the actor at `addr`.
    /// Returns `true` if the actor exists (message handled or dropped; type check deferred to tick).
    pub fn deliver(&mut self, addr: &ActorAddress, msg: Box<dyn Any + Send>) -> bool {
        if let Some(slot) = self.actors.get_mut(addr) {
            if slot.mailbox_capacity > 0 && slot.mailbox.len() >= slot.mailbox_capacity {
                match slot.overflow_policy {
                    MailboxOverflow::DropNewest => {
                        self.drops_this_tick += 1;
                        return true;
                    }
                    MailboxOverflow::DropOldest => {
                        slot.mailbox.pop_front();
                        self.drops_this_tick += 1;
                    }
                }
            }
            slot.mailbox.push_back(msg);
            true
        } else {
            false
        }
    }

    /// Take and reset the drop counter for this tick.
    pub fn take_drops(&mut self) -> usize {
        std::mem::replace(&mut self.drops_this_tick, 0)
    }

    /// Tick all actors in the pool. Returns the number of messages processed.
    ///
    /// Each actor processes up to `budget` messages per tick (0 = unlimited).
    /// This prevents a single hot actor from starving others on the same worker.
    pub fn tick_all(
        &mut self,
        inner: &dyn ContextInner,
        stats: &WorkerStats,
        budget: usize,
        stop_requests: &RefCell<Vec<ActorAddress>>,
    ) -> usize {
        let mut count = 0;
        for (&addr, slot) in self.actors.iter_mut() {
            if slot.poisoned || slot.stopping {
                // Discard all messages for poisoned/stopping actors
                slot.mailbox.clear();
                continue;
            }

            #[cfg(feature = "tracing")]
            let _actor_span = tracing::trace_span!("actor.tick", actor_addr = %addr).entered();

            let ctx = Ctx::new(inner, addr);

            // Call on_start once, before first message
            if !slot.started {
                let start_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    slot.actor.on_start(&ctx);
                }));
                slot.started = true;
                if start_result.is_err() {
                    stats.panics.fetch_add(1, Ordering::Relaxed);
                    eprintln!("swactor: actor {addr} panicked in on_start — poisoned");
                    #[cfg(feature = "tracing")]
                    tracing::error!(actor_addr = %addr, "actor.on_start_panicked");
                    slot.poisoned = true;
                    slot.mailbox.clear();
                    continue;
                }
                // Check if on_start requested stop
                {
                    let stops = stop_requests.borrow();
                    if !stops.is_empty() && stops.contains(&addr) {
                        drop(stops);
                        slot.stopping = true;
                        stats.stops.fetch_add(1, Ordering::Relaxed);
                        slot.mailbox.clear();
                        continue;
                    }
                }
            }

            let mut actor_count = 0usize;
            while let Some(msg) = slot.mailbox.pop_front() {
                // Intercept StopSignal (from external runtime.stop_actor)
                if msg.is::<StopSignal>() {
                    slot.stopping = true;
                    stats.stops.fetch_add(1, Ordering::Relaxed);
                    slot.mailbox.clear();
                    #[cfg(feature = "tracing")]
                    tracing::info!(actor_addr = %addr, "actor.stop_requested");
                    break;
                }

                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    slot.actor.handle_any(&ctx, msg)
                }));
                match result {
                    Ok(None) => {
                        stats.type_mismatches.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        stats.panics.fetch_add(1, Ordering::Relaxed);
                        slot.mailbox.clear();
                        eprintln!("swactor: actor {addr} panicked — poisoned, future messages will be discarded");
                        #[cfg(feature = "tracing")]
                        tracing::error!(actor_addr = %addr, "actor.panicked");
                        slot.poisoned = true;
                        slot.mailbox.clear();
                        break;
                    }
                    Ok(Some(type_name)) => {
                        slot.last_msg_type = Some(type_name);
                        slot.messages_processed += 1;
                        // Track per-type counts (bounded to 32 distinct types)
                        if slot.msg_type_counts.len() < 32 || slot.msg_type_counts.contains_key(type_name) {
                            *slot.msg_type_counts.entry(type_name).or_insert(0) += 1;
                        }
                    }
                }
                count += 1;
                actor_count += 1;

                // Check if handler requested self-stop (via ctx.stop_self())
                {
                    let stops = stop_requests.borrow();
                    if !stops.is_empty() && stops.contains(&addr) {
                        drop(stops);
                        slot.stopping = true;
                        stats.stops.fetch_add(1, Ordering::Relaxed);
                        slot.mailbox.clear();
                        break;
                    }
                }

                if budget > 0 && actor_count >= budget {
                    break;
                }
            }
        }
        count
    }

    pub fn len(&self) -> usize {
        self.actors.len()
    }

    pub fn contains(&self, addr: &ActorAddress) -> bool {
        self.actors.contains_key(addr)
    }

    pub fn total_mailbox_depth(&self) -> usize {
        self.actors.values().map(|slot| slot.mailbox.len()).sum()
    }

    /// Remove poisoned and stopping actors, returning their addresses and stop reasons.
    /// Called after tick_all so the caller can clean up the address map.
    ///
    /// For stopping actors: calls `on_stop()` before removal (wrapped in catch_unwind).
    /// For poisoned actors: `on_stop()` is NOT called (state may be corrupt).
    pub fn cleanup_dead(&mut self, inner: &dyn ContextInner) -> Vec<(ActorAddress, StopReason)> {
        let dead: Vec<(ActorAddress, StopReason)> = self
            .actors
            .iter()
            .filter(|(_, slot)| slot.poisoned || slot.stopping)
            .map(|(&addr, slot)| {
                let reason = if slot.poisoned { StopReason::Panicked } else { StopReason::Normal };
                (addr, reason)
            })
            .collect();
        for &(addr, _) in &dead {
            if let Some(mut slot) = self.actors.remove(&addr) {
                // Call on_stop for gracefully stopping actors only
                if slot.stopping && !slot.poisoned {
                    let ctx = Ctx::new(inner, addr);
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        slot.actor.on_stop(&ctx);
                    }));
                }
                // slot is dropped here — actor resources freed
            }
        }
        dead
    }

    /// Fill `out` with per-actor snapshots, reusing the existing allocation.
    pub fn mailbox_depths_into(&self, out: &mut Vec<ActorSnapshot>) {
        out.clear();
        out.extend(self.actors.iter().map(|(&addr, slot)| {
            let mut type_counts: Vec<(&'static str, u64)> =
                slot.msg_type_counts.iter().map(|(&k, &v)| (k, v)).collect();
            type_counts.sort_by(|a, b| b.1.cmp(&a.1));
            ActorSnapshot {
                address: addr,
                mailbox_depth: slot.mailbox.len(),
                last_msg_type: slot.last_msg_type,
                messages_processed: slot.messages_processed,
                poisoned: slot.poisoned,
                message_type_counts: type_counts,
            }
        }));
    }
}
