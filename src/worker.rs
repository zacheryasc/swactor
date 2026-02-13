use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use crate::actor::{ActorAddress, AnyActor, CloneMsg, ContextInner, Ctx, StopReason, StopSignal, TimerRequest};
use crate::channel::Receiver;
use crate::config::MailboxOverflow;
use crate::delivery::{AddrBuildHasher, AddrMap, Envelope, TickContext, WorkerId};
use crate::stats::{ActorSnapshot, TickTiming, WorkerStats};
use crate::Error;

// ─── Per-Worker Timer Wheel ─────────────────────────────────────────────────

struct OnceTimer {
    fire_at: u64,
    dest: ActorAddress,
    msg: Box<dyn Any + Send>,
}

struct IntervalTimer {
    next_fire: u64,
    period: u64,
    dest: ActorAddress,
    msg: Box<dyn CloneMsg>,
}

/// Per-worker tick-counting timer wheel.
///
/// Timers are deterministic (tick-counted, not wall-clock). One-shot timers
/// fire once and are consumed; interval timers fire repeatedly every N ticks.
struct TimerWheel {
    current_tick: u64,
    once_timers: Vec<OnceTimer>,
    interval_timers: Vec<IntervalTimer>,
}

impl TimerWheel {
    fn new() -> Self {
        Self {
            current_tick: 0,
            once_timers: Vec::new(),
            interval_timers: Vec::new(),
        }
    }

    /// Advance the tick counter and collect all due timer messages.
    /// Returns the messages to be routed by the caller (may target local or remote actors/inboxes).
    fn fire(&mut self) -> Vec<(ActorAddress, Box<dyn Any + Send>)> {
        self.current_tick += 1;
        let tick = self.current_tick;
        let mut result = Vec::new();

        // Fire one-shot timers (swap-remove for O(1) removal)
        let mut i = 0;
        while i < self.once_timers.len() {
            if self.once_timers[i].fire_at <= tick {
                let timer = self.once_timers.swap_remove(i);
                result.push((timer.dest, timer.msg));
            } else {
                i += 1;
            }
        }

        // Fire interval timers
        for timer in &mut self.interval_timers {
            if timer.next_fire <= tick {
                let msg = timer.msg.clone_boxed();
                result.push((timer.dest, msg));
                timer.next_fire = tick + timer.period;
            }
        }

        result
    }

    /// Remove interval timers whose target was just removed from the worker.
    /// Only GCs timers for addresses in `dead` — inboxes and cross-worker actors
    /// are not in the local pool but are still valid targets.
    fn gc_dead_intervals(&mut self, dead: &[ActorAddress]) {
        if dead.is_empty() {
            return;
        }
        self.interval_timers.retain(|t| !dead.iter().any(|d| *d == t.dest));
    }

    /// Add a one-shot timer.
    fn add_once(&mut self, dest: ActorAddress, msg: Box<dyn Any + Send>, ticks: u64) {
        self.once_timers.push(OnceTimer {
            fire_at: self.current_tick + ticks,
            dest,
            msg,
        });
    }

    /// Add an interval timer. First fire is after `period` ticks.
    fn add_interval(&mut self, dest: ActorAddress, msg: Box<dyn CloneMsg>, period: u64) {
        let period = period.max(1); // prevent zero-period infinite loop
        self.interval_timers.push(IntervalTimer {
            next_fire: self.current_tick + period,
            period,
            dest,
            msg,
        });
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
    /// Per-worker tick-counting timer wheel.
    timers: TimerWheel,
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
            timers: TimerWheel::new(),
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

        // 2.5. Fire due timers → deliver to mailboxes before tick_all
        let timer_msgs = self.timers.fire();
        for (dest, msg) in timer_msgs {
            if self.pool.contains(&dest) {
                // Same-worker: deliver directly to actor's mailbox
                self.pool.deliver(&dest, msg);
            } else {
                // Inbox or cross-worker: route through address map / inbox registry
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
            did_work = true;
        }

        // 3. Tick all actors with WorkerContext
        let pending_local: RefCell<Vec<(ActorAddress, Box<dyn Any + Send>)>> =
            RefCell::new(Vec::new());
        let stop_requests: RefCell<Vec<ActorAddress>> = RefCell::new(Vec::new());
        let timer_requests: RefCell<Vec<TimerRequest>> = RefCell::new(Vec::new());

        let processed;
        {
            let worker_ctx = WorkerContext {
                worker_id: self.id,
                tc,
                pending_local: &pending_local,
                stop_requests: &stop_requests,
                timer_requests: &timer_requests,
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

        // 5.5. Process timer requests from handlers
        for request in timer_requests.into_inner() {
            match request {
                TimerRequest::Once { dest, msg, ticks } => {
                    self.timers.add_once(dest, msg, ticks);
                }
                TimerRequest::Interval { dest, msg, period } => {
                    self.timers.add_interval(dest, msg, period);
                }
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
        //    on_stop() may send messages, so provide a fresh pending_local buffer.
        let cleanup_pending: RefCell<Vec<(ActorAddress, Box<dyn Any + Send>)>> =
            RefCell::new(Vec::new());
        let cleanup_stops: RefCell<Vec<ActorAddress>> = RefCell::new(Vec::new());
        let cleanup_timers: RefCell<Vec<TimerRequest>> = RefCell::new(Vec::new());
        let dead = {
            let cleanup_ctx = WorkerContext {
                worker_id: self.id,
                tc,
                pending_local: &cleanup_pending,
                stop_requests: &cleanup_stops,
                timer_requests: &cleanup_timers,
                stats: &self.stats,
            };
            let dead = self.pool.cleanup_dead(&cleanup_ctx);
            if !dead.is_empty() {
                for &(addr, _) in &dead {
                    tc.address_map.remove(&addr);
                }

                if let Some(ext) = tc.extension {
                    // Get death notifications (monitors) before cleaning up state
                    let notifications = ext.on_actor_death(&dead);

                    // Clean up extension state (names, groups, dead watcher monitors)
                    let dead_addrs: Vec<_> = dead.iter().map(|(a, _)| *a).collect();
                    ext.cleanup_dead(&dead_addrs);

                    // Deliver Down notifications through normal routing
                    for (dest, msg) in notifications {
                        if self.pool.contains(&dest) {
                            self.pool.deliver(&dest, msg);
                        } else {
                            match tc.address_map.lookup(&dest) {
                                Some(wid) => {
                                    tc.transfer_txs[wid.as_usize()]
                                        .send(Envelope::new(dest, msg));
                                    crate::runtime::notify_worker(tc.worker_threads, wid.as_usize());
                                }
                                None => {
                                    let _ = tc.inbox_registry.try_deliver(dest, msg);
                                }
                            }
                        }
                    }
                }

                // Re-publish num_actors after cleanup so stats reflect removal
                self.stats.num_actors.store(self.pool.len(), Ordering::Relaxed);
                did_work = true;
            }
            dead
        };
        // Deliver any messages sent during on_stop callbacks
        for (addr, msg) in cleanup_pending.into_inner() {
            self.pool.deliver(&addr, msg);
        }

        // GC orphaned interval timers for actors that were just removed
        let dead_addrs: Vec<ActorAddress> = dead.iter().map(|(a, _)| *a).collect();
        self.timers.gc_dead_intervals(&dead_addrs);

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
    timer_requests: &'a RefCell<Vec<TimerRequest>>,
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

    fn schedule_timer(&self, request: TimerRequest) {
        self.timer_requests.borrow_mut().push(request);
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
                        break;
                    }
                    Ok(Some(type_name)) => {
                        slot.last_msg_type = Some(type_name);
                        slot.messages_processed += 1;
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
            ActorSnapshot {
                address: addr,
                mailbox_depth: slot.mailbox.len(),
                last_msg_type: slot.last_msg_type,
                messages_processed: slot.messages_processed,
                poisoned: slot.poisoned,
            }
        }));
    }
}
