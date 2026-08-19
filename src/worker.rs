use crate::Instant;
use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::Error;
use crate::actor::{
    ActorAddress, AnyActor, ContextInner, Ctx, Environment, ExitValue, ResumeSignal, SpawnRequest,
    StopReason, StopSignal, StopWithSignal, SystemInfo,
};
use crate::admin::{
    ActorStatus, ActorSummary, AdminCommand, AdminError, AdminResult, InspectActorResponse,
    ListActorsResponse, OperationResult,
};
use crate::channel::Receiver;
use crate::delivery::{AddrBuildHasher, AddrMap, Envelope, TickContext, WorkerId};
use crate::stats::{ActorSnapshot, TickTiming, WorkerStats};

use crate::extension::WorkerExtension;
use crate::runtime::RuntimeShared;

/// Whether an actor should be skipped during `tick_all`.
pub(crate) fn should_skip_actor(poisoned: bool, stopping: bool, suspended: bool) -> bool {
    poisoned || stopping || suspended
}

/// Whether `on_stop` should fire for an actor being cleaned up.
pub(crate) fn is_on_stop_eligible(stopping: bool, poisoned: bool) -> bool {
    stopping && !poisoned
}

/// Determine the `StopReason` for a dead actor based on its flags.
pub(crate) fn determine_stop_reason(poisoned: bool, has_exit_value: bool) -> StopReason {
    if poisoned {
        StopReason::Panicked
    } else if has_exit_value {
        StopReason::Completed
    } else {
        StopReason::Normal
    }
}

/// Route a runtime-injected message (extension output, death notification) to
/// its destination: same-worker pool first, then the owning worker's transfer
/// queue, then the non-local (inbox/transport) seam.
fn route_runtime_message(
    pool: &mut ActorPool,
    tc: &TickContext,
    dest: ActorAddress,
    msg: Box<dyn Any + Send>,
) {
    if pool.contains(&dest) {
        pool.deliver(&dest, msg);
    } else {
        match tc.worker_of(&dest) {
            Some(w) => tc.transfer_tx(w).send(Envelope::new(dest, msg)),
            None => {
                let _ = tc.route_nonlocal(dest, msg);
            }
        }
    }
}

/// A worker owns a disjoint set of actors and processes them via [`Worker::try_tick`].
///
/// Each worker is owned by exactly one execution host (a [`SingleThreadRuntime`]
/// or an engine driver) and requires only `Send`, not `Sync`.
pub struct Worker {
    pub(crate) id: WorkerId,
    pub(crate) shared: Arc<RuntimeShared>,
    pub(crate) pool: ActorPool,
    transfer_rx: Receiver<Envelope>,
    spawn_rx: Receiver<SpawnRequest>,
    admin_rx: Receiver<AdminCommand>,
    pub(crate) stats: Arc<WorkerStats>,
    /// Reusable scratch buffer for building per-actor snapshots.
    snapshot_buf: Vec<ActorSnapshot>,
    /// Per-worker extension (e.g., timer wheel). Created by RuntimeExtension factory.
    pub(crate) worker_ext: Option<Box<dyn WorkerExtension>>,
    /// True if the previous tick did work — ensures one full tick follows a productive
    /// tick so pending_local messages delivered to mailboxes get drained.
    has_backlog: bool,
    /// Transfers whose address is mapped to this worker but whose spawn request
    /// has not reached the pool yet.
    deferred_transfers: VecDeque<Envelope>,
    /// Targeted admin commands waiting for their mapped spawn to be installed.
    deferred_admin: VecDeque<AdminCommand>,
}

impl Worker {
    pub(crate) fn new(
        id: WorkerId,
        shared: Arc<RuntimeShared>,
        transfer_rx: Receiver<Envelope>,
        spawn_rx: Receiver<SpawnRequest>,
        admin_rx: Receiver<AdminCommand>,
        stats: Arc<WorkerStats>,
    ) -> Self {
        Self {
            id,
            shared,
            pool: ActorPool::new(),
            transfer_rx,
            spawn_rx,
            admin_rx,
            stats,
            snapshot_buf: Vec::new(),
            worker_ext: None,
            has_backlog: false,
            deferred_transfers: VecDeque::new(),
            deferred_admin: VecDeque::new(),
        }
    }

    pub(crate) fn has_work(&self) -> bool {
        self.has_backlog
            || !self.spawn_rx.is_empty()
            || !self.transfer_rx.is_empty()
            || !self.deferred_transfers.is_empty()
            || !self.admin_rx.is_empty()
            || !self.deferred_admin.is_empty()
            || self
                .worker_ext
                .as_ref()
                .map_or(false, |e| e.has_pending_work())
    }

    /// Run one synchronous worker pass. Returns `true` if any work was done.
    ///
    /// This is the only core worker transition. The owning host calls it; the
    /// worker never drives itself.
    pub fn try_tick(&mut self) -> bool {
        let wid = self.id;

        // Fast idle path: skip the entire tick when nothing could have changed.
        // Cost: ~3 atomic loads, zero syscalls, zero actor iteration.
        if !self.has_work() {
            return false;
        }

        // Build the routing context from disjoint fields so the mutable
        // per-pass state (pool, queues, extension) can still be borrowed below.
        let shared = &self.shared;
        let tc = TickContext {
            address_map: &shared.address_map,
            spawn_txs: &shared.spawn_txs,
            transfer_txs: &shared.transfer_txs,
            inbox_registry: &shared.inbox_registry,
            config: &shared.config,
            extension: shared.extension.get().map(|a| a.as_ref()),
            process_output_observer: shared.process_output_observer.get(),
            stats_hook: shared.stats_hook.get().map(|a| a.as_ref()),
            worker_stats: &self.stats,
            num_workers: shared.worker_stats.len(),
            worker_id: wid,
            created_at: shared.created_at,
            #[cfg(feature = "transport")]
            remote_sink: shared.remote_sink.get().map(|a| a.as_ref()),
        };

        #[cfg(feature = "tracing")]
        let _span = tracing::trace_span!("worker.tick", worker_id = wid.index()).entered();

        let mut did_work = false;
        let t0 = Instant::now();

        // 1. Drain spawn queue → add actors to pool
        did_work |= Self::drain_spawns(
            &mut self.pool,
            &self.spawn_rx,
            &tc,
            tc.config.worker_ingress_budget,
        );
        let t1 = Instant::now();

        // 2. Drain retained transfers first, then the transfer queue → deliver
        // envelopes to actors or retain them until their mapped spawn installs.
        did_work |= Self::drain_transfers(
            &mut self.pool,
            &self.transfer_rx,
            &mut self.deferred_transfers,
            &tc,
            tc.config.worker_ingress_budget,
        );
        let t2 = Instant::now();

        // 3. Drain admin queue → inspect or mutate worker-owned slots before handlers
        did_work |= Self::drain_admin(
            &mut self.pool,
            &self.admin_rx,
            &mut self.deferred_admin,
            &tc,
            tc.config.worker_ingress_budget,
        );

        // 4. Fire per-worker extension (e.g., timers) → deliver before tick_all
        let ext_msgs: Vec<_> = self
            .worker_ext
            .as_mut()
            .map(|ext| ext.on_tick())
            .unwrap_or_default();
        for (dest, msg) in ext_msgs {
            route_runtime_message(&mut self.pool, &tc, dest, msg);
            did_work = true;
        }

        // 5. Tick all actors with WorkerContext
        let pending_local: RefCell<Vec<(ActorAddress, Box<dyn Any + Send>)>> =
            RefCell::new(Vec::new());
        let stop_requests: RefCell<Vec<ActorAddress>> = RefCell::new(Vec::new());
        let stop_with_values: RefCell<Vec<(ActorAddress, ExitValue)>> = RefCell::new(Vec::new());
        let suspend_requests: RefCell<Vec<ActorAddress>> = RefCell::new(Vec::new());
        let worker_requests: RefCell<Vec<Box<dyn Any + Send>>> = RefCell::new(Vec::new());

        let processed;
        {
            let worker_ctx = WorkerContext {
                tc: &tc,
                pending_local: &pending_local,
                stop_requests: &stop_requests,
                stop_with_values: &stop_with_values,
                suspend_requests: &suspend_requests,
                worker_requests: &worker_requests,
                stats: &self.stats,
            };
            processed =
                self.pool
                    .tick_all(&worker_ctx, &self.stats, tc.config.actor_message_budget);
            if processed > 0 {
                did_work = true;
            }
        }
        let t3 = Instant::now();

        #[cfg(feature = "tracing")]
        if processed > 0 {
            tracing::debug!(
                worker_id = wid.index(),
                messages_processed = processed,
                "worker.tick_all"
            );
        }

        // 6. Drain spawn queue again — actors spawned during step 5
        //    must be in the pool before pending_local delivery.
        did_work |= Self::drain_spawns(
            &mut self.pool,
            &self.spawn_rx,
            &tc,
            tc.config.worker_ingress_budget,
        );
        let t4 = Instant::now();

        // 7. Drain pending_local buffer → deliver to local actors
        let pending = pending_local.into_inner();
        if !pending.is_empty() {
            did_work = true;
        }
        for (addr, msg) in pending {
            Self::deliver_or_defer_transfer(
                &mut self.pool,
                &tc,
                &mut self.deferred_transfers,
                Envelope::new(addr, msg),
            );
        }

        // 7.5. Process worker extension requests from handlers (e.g., timer scheduling)
        if let Some(ext) = &mut self.worker_ext {
            for request in worker_requests.into_inner() {
                ext.handle_request(request);
            }
        }

        let t5 = Instant::now();

        // 8. Publish stats (skip entirely when idle to avoid allocation + mutex)
        if did_work {
            self.stats
                .num_actors
                .store(self.pool.len(), Ordering::Relaxed);
            self.stats
                .total_mailbox_depth
                .store(self.pool.total_mailbox_depth(), Ordering::Relaxed);
            self.stats
                .messages_processed
                .fetch_add(processed as u64, Ordering::Relaxed);

            if let Some(hook) = tc.stats_hook {
                self.pool.mailbox_depths_into(&mut self.snapshot_buf);
                hook.on_tick(wid.index(), &self.snapshot_buf);
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
                worker_id = wid.index(),
                num_actors = self.pool.len(),
                mailbox_depth = self.pool.total_mailbox_depth(),
                messages_processed = processed,
                "worker.stats"
            );
        }

        // 9. Clean up poisoned and stopping actors
        did_work |= Self::cleanup_dead_actors(
            &mut self.pool,
            &mut self.worker_ext,
            &mut self.deferred_transfers,
            &tc,
        );

        self.has_backlog = did_work;
        did_work
    }

    fn drain_spawns(
        pool: &mut ActorPool,
        spawn_rx: &Receiver<SpawnRequest>,
        tc: &TickContext,
        budget: usize,
    ) -> bool {
        let mut did_work = false;
        let mut count = 0usize;
        #[cfg(feature = "tracing")]
        let mut spawn_count: usize = 0;
        while let Some(mut req) = spawn_rx.try_recv() {
            if let Some(ext) = tc.extension {
                req.env = ext.on_spawn(
                    req.addr,
                    req.parent,
                    req.env,
                    tc.created_at.elapsed().as_millis() as u64,
                );
            }
            pool.insert(req);
            #[cfg(feature = "tracing")]
            {
                spawn_count += 1;
            }
            did_work = true;
            count += 1;
            if budget != 0 && count >= budget {
                break;
            }
        }
        #[cfg(feature = "tracing")]
        if spawn_count > 0 {
            tracing::debug!(
                worker_id = tc.worker_id().index(),
                count = spawn_count,
                "worker.spawns_drained"
            );
        }
        did_work
    }

    fn deliver_or_defer_transfer(
        pool: &mut ActorPool,
        tc: &TickContext,
        deferred: &mut VecDeque<Envelope>,
        envelope: Envelope,
    ) -> bool {
        let dest = envelope.dest();
        if pool.contains(&dest) {
            pool.deliver(&dest, envelope.into_payload());
            true
        } else if tc.worker_of(&dest) == Some(tc.worker_id()) {
            deferred.push_back(envelope);
            false
        } else {
            true
        }
    }

    fn drain_transfers(
        pool: &mut ActorPool,
        transfer_rx: &Receiver<Envelope>,
        deferred: &mut VecDeque<Envelope>,
        tc: &TickContext,
        budget: usize,
    ) -> bool {
        let mut did_work = false;
        let mut count = 0usize;

        while budget == 0 || count < budget {
            let Some(envelope) = deferred.pop_front() else {
                break;
            };
            if Self::deliver_or_defer_transfer(pool, tc, deferred, envelope) {
                did_work = true;
            }
            count += 1;
        }

        while budget == 0 || count < budget {
            let Some(envelope) = transfer_rx.try_recv() else {
                break;
            };
            did_work = true;
            Self::deliver_or_defer_transfer(pool, tc, deferred, envelope);
            count += 1;
        }

        did_work
    }

    fn admin_target(cmd: &AdminCommand) -> Option<ActorAddress> {
        match cmd {
            AdminCommand::ListActors { .. } => None,
            AdminCommand::InspectActor { actor, .. }
            | AdminCommand::GetActorState { actor, .. }
            | AdminCommand::ReplaceActorState { actor, .. }
            | AdminCommand::StopActor { actor, .. }
            | AdminCommand::SuspendActor { actor, .. }
            | AdminCommand::ResumeActor { actor, .. } => Some(*actor),
        }
    }

    fn should_defer_admin_command(pool: &ActorPool, tc: &TickContext, cmd: &AdminCommand) -> bool {
        Self::admin_target(cmd).is_some_and(|actor| {
            !pool.contains(&actor) && tc.worker_of(&actor) == Some(tc.worker_id())
        })
    }

    fn apply_or_defer_admin_command(
        pool: &mut ActorPool,
        tc: &TickContext,
        deferred: &mut VecDeque<AdminCommand>,
        cmd: AdminCommand,
    ) -> bool {
        if Self::should_defer_admin_command(pool, tc, &cmd) {
            deferred.push_back(cmd);
            false
        } else {
            Self::apply_admin_command(pool, tc, cmd);
            true
        }
    }

    fn drain_admin(
        pool: &mut ActorPool,
        admin_rx: &Receiver<AdminCommand>,
        deferred: &mut VecDeque<AdminCommand>,
        tc: &TickContext,
        budget: usize,
    ) -> bool {
        let mut did_work = false;
        let mut count = 0usize;

        while budget == 0 || count < budget {
            let Some(cmd) = deferred.pop_front() else {
                break;
            };
            if Self::apply_or_defer_admin_command(pool, tc, deferred, cmd) {
                did_work = true;
            }
            count += 1;
        }

        while budget == 0 || count < budget {
            let Some(cmd) = admin_rx.try_recv() else {
                break;
            };
            did_work = true;
            Self::apply_or_defer_admin_command(pool, tc, deferred, cmd);
            count += 1;
        }

        did_work
    }

    fn send_admin_reply<T: crate::actor::Message>(
        tc: &TickContext,
        reply_to: ActorAddress,
        result: AdminResult<T>,
    ) {
        let _ = tc.inbox_registry.try_deliver(reply_to, Box::new(result));
    }

    fn apply_admin_command(pool: &mut ActorPool, tc: &TickContext, cmd: AdminCommand) {
        match cmd {
            AdminCommand::ListActors { acc } => {
                let mut local = Vec::new();
                pool.actor_summaries_into(&mut local, tc.worker_id);
                {
                    let mut summaries = acc.summaries.lock();
                    summaries.extend(local);
                }
                if acc.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                    let actors = {
                        let mut summaries = acc.summaries.lock();
                        std::mem::take(&mut *summaries)
                    };
                    Self::send_admin_reply(tc, acc.reply_to, Ok(ListActorsResponse { actors }));
                }
            }
            AdminCommand::InspectActor { actor, reply_to } => {
                let result = pool
                    .actor_summary(actor, tc.worker_id)
                    .map(|summary| InspectActorResponse { summary });
                Self::send_admin_reply(tc, reply_to, result);
            }
            AdminCommand::GetActorState {
                actor,
                reply_to,
                get,
                not_found,
            } => {
                let boxed = match pool.get_actor_erased(actor) {
                    Some(erased) => get(actor, erased, erased.metadata()),
                    None => not_found(actor),
                };
                let _ = tc.inbox_registry.try_deliver(reply_to, boxed);
            }
            AdminCommand::ReplaceActorState {
                actor,
                reply_to,
                replace,
            } => {
                let result = match pool.get_actor_erased_mut(actor) {
                    Some(erased) => {
                        let metadata = erased.metadata();
                        replace(erased, metadata)
                    }
                    None => Err(AdminError::ActorNotFound { actor }),
                };
                Self::send_admin_reply(tc, reply_to, result);
            }
            AdminCommand::StopActor { actor, reply_to } => {
                let result = pool.stop_actor_admin(actor, tc.worker_stats);
                Self::send_admin_reply(tc, reply_to, result);
            }
            AdminCommand::SuspendActor { actor, reply_to } => {
                let result = pool.suspend_actor_admin(actor);
                Self::send_admin_reply(tc, reply_to, result);
            }
            AdminCommand::ResumeActor { actor, reply_to } => {
                let result = pool.resume_actor_admin(actor);
                Self::send_admin_reply(tc, reply_to, result);
            }
        }
    }

    /// Phase 9: clean up dead actors, deliver death notifications, GC extension state.
    fn cleanup_dead_actors(
        pool: &mut ActorPool,
        worker_ext: &mut Option<Box<dyn WorkerExtension>>,
        deferred_transfers: &mut VecDeque<Envelope>,
        tc: &TickContext,
    ) -> bool {
        let cleanup_pending: RefCell<Vec<(ActorAddress, Box<dyn Any + Send>)>> =
            RefCell::new(Vec::new());
        let cleanup_stops: RefCell<Vec<ActorAddress>> = RefCell::new(Vec::new());
        let cleanup_stop_withs: RefCell<Vec<(ActorAddress, ExitValue)>> = RefCell::new(Vec::new());
        let cleanup_suspends: RefCell<Vec<ActorAddress>> = RefCell::new(Vec::new());
        let cleanup_requests: RefCell<Vec<Box<dyn Any + Send>>> = RefCell::new(Vec::new());
        let dead = {
            let cleanup_ctx = WorkerContext {
                tc,
                pending_local: &cleanup_pending,
                stop_requests: &cleanup_stops,
                stop_with_values: &cleanup_stop_withs,
                suspend_requests: &cleanup_suspends,
                worker_requests: &cleanup_requests,
                stats: tc.worker_stats,
            };
            pool.cleanup_dead(&cleanup_ctx)
        };

        let had_dead = !dead.is_empty();
        if had_dead {
            for (addr, _, _) in &dead {
                tc.address_map.remove(addr);
            }

            if let Some(ext) = tc.extension {
                let notifications = ext.on_actor_death(&dead);
                let dead_addrs: Vec<_> = dead.iter().map(|(a, _, _)| *a).collect();
                ext.cleanup_dead(&dead_addrs);
                for (dest, msg) in notifications {
                    route_runtime_message(pool, tc, dest, msg);
                }
            }

            tc.worker_stats
                .num_actors
                .store(pool.len(), Ordering::Relaxed);
        }

        // Deliver any messages sent during on_stop callbacks.
        for (addr, msg) in cleanup_pending.into_inner() {
            Self::deliver_or_defer_transfer(pool, tc, deferred_transfers, Envelope::new(addr, msg));
        }

        // GC per-worker extension state for dead actors
        if let Some(ext) = worker_ext {
            let dead_addrs: Vec<ActorAddress> = dead.iter().map(|(a, _, _)| *a).collect();
            ext.gc_dead(&dead_addrs);
        }

        had_dead
    }
}

/// The `ContextInner` impl for in-worker sends.
///
/// Same-worker sends are staged in `pending_local` (eligible next pass).
/// Cross-worker sends move an `Envelope` into the target worker's transfer
/// queue. Non-actor addresses route to the inbox registry / transport seam.
struct WorkerContext<'a> {
    tc: &'a TickContext<'a>,
    pending_local: &'a RefCell<Vec<(ActorAddress, Box<dyn Any + Send>)>>,
    stop_requests: &'a RefCell<Vec<ActorAddress>>,
    stop_with_values: &'a RefCell<Vec<(ActorAddress, ExitValue)>>,
    suspend_requests: &'a RefCell<Vec<ActorAddress>>,
    worker_requests: &'a RefCell<Vec<Box<dyn Any + Send>>>,
    stats: &'a WorkerStats,
}

impl ContextInner for WorkerContext<'_> {
    fn send_any(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error> {
        match self.tc.worker_of(&addr) {
            Some(w) if w == self.tc.worker_id() => {
                self.stats.local_sends.fetch_add(1, Ordering::Relaxed);
                self.pending_local.borrow_mut().push((addr, msg));
                Ok(())
            }
            Some(w) => {
                // Cross-worker: move the payload through shared memory.
                self.stats.local_sends.fetch_add(1, Ordering::Relaxed);
                self.tc.transfer_tx(w).send(Envelope::new(addr, msg));
                Ok(())
            }
            None => {
                self.stats.inbox_sends.fetch_add(1, Ordering::Relaxed);
                self.tc.route_nonlocal(addr, msg)
            }
        }
    }

    fn spawn_any(&self, request: SpawnRequest) {
        // ctx.spawn pins the child to the current worker.
        self.tc
            .address_map
            .insert(request.addr, self.tc.worker_id());
        self.tc.spawn_tx(self.tc.worker_id()).send(request);
    }

    fn request_stop(&self, addr: ActorAddress) {
        self.stop_requests.borrow_mut().push(addr);
    }

    fn request_stop_with(&self, addr: ActorAddress, value: ExitValue) {
        self.stop_with_values.borrow_mut().push((addr, value));
    }

    fn request_suspend(&self, addr: ActorAddress) {
        self.suspend_requests.borrow_mut().push(addr);
    }

    fn request_resume(&self, addr: ActorAddress) {
        self.pending_local
            .borrow_mut()
            .push((addr, Box::new(ResumeSignal)));
    }

    fn post_worker_request(&self, request: Box<dyn Any + Send>) {
        self.worker_requests.borrow_mut().push(request);
    }

    fn extension(&self) -> Option<&dyn crate::extension::RuntimeExtension> {
        self.tc.extension
    }

    fn process_output_observer(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::process_observer::ProcessOutputObserver>> {
        self.tc.process_output_observer.cloned()
    }

    fn system_info(&self) -> SystemInfo {
        SystemInfo {
            worker_id: self.tc.worker_id().index(),
            num_workers: self.tc.num_workers,
            total_actors: self.tc.address_map.len(),
            uptime_ms: self.tc.created_at.elapsed().as_millis() as u64,
        }
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
    /// Actor is suspended — messages queue but are not processed.
    suspended: bool,
    last_msg_type: Option<&'static str>,
    messages_processed: u64,
    /// Per-message-type counters (bounded to 32 entries).
    msg_type_counts: HashMap<&'static str, u64>,
    /// Address of the actor that spawned this one, or `None` for externally-spawned actors.
    parent_addr: Option<ActorAddress>,
    /// Inherited environment from parent (or empty for runtime-spawned actors).
    env: Environment,
    /// Typed exit value set by `ctx.stop_with()`.
    exit_value: Option<ExitValue>,
}

/// Per-worker actor storage. Owns per-actor mailboxes.
pub(crate) struct ActorPool {
    actors: AddrMap<ActorSlot>,
}

impl ActorPool {
    pub fn new() -> Self {
        Self {
            actors: HashMap::with_hasher(AddrBuildHasher),
        }
    }

    pub fn insert(&mut self, req: SpawnRequest) {
        self.actors.insert(
            req.addr,
            ActorSlot {
                mailbox: VecDeque::with_capacity(16),
                actor: req.actor,
                poisoned: false,
                stopping: false,
                started: false,
                suspended: false,
                last_msg_type: None,
                messages_processed: 0,
                msg_type_counts: HashMap::new(),
                parent_addr: req.parent,
                env: req.env,
                exit_value: None,
            },
        );
    }

    /// Deliver a type-erased message to the actor at `addr`.
    /// Returns `true` if the actor exists (message enqueued; type check deferred to tick).
    pub fn deliver(&mut self, addr: &ActorAddress, msg: Box<dyn Any + Send>) -> bool {
        if let Some(slot) = self.actors.get_mut(addr) {
            // Intercept control signals for suspended actors: they skip tick_all
            // so we must handle resume/stop at delivery time.
            if slot.suspended {
                if msg.is::<ResumeSignal>() {
                    slot.suspended = false;
                    return true;
                }
                if msg.is::<StopSignal>() {
                    slot.stopping = true;
                    slot.mailbox.clear();
                    return true;
                }
                if msg.is::<StopWithSignal>() {
                    if let Ok(sig) = msg.downcast::<StopWithSignal>() {
                        slot.exit_value = Some(sig.0);
                    }
                    slot.stopping = true;
                    slot.mailbox.clear();
                    return true;
                }
            }
            slot.mailbox.push_back(msg);
            true
        } else {
            false
        }
    }

    fn get_actor_erased(&self, addr: ActorAddress) -> Option<&dyn AnyActor> {
        self.actors.get(&addr).map(|slot| slot.actor.as_ref())
    }

    fn get_actor_erased_mut(
        &mut self,
        addr: ActorAddress,
    ) -> Option<&mut (dyn AnyActor + 'static)> {
        self.actors.get_mut(&addr).map(|slot| slot.actor.as_mut())
    }

    fn actor_summary_from_slot(
        address: ActorAddress,
        slot: &ActorSlot,
        worker: WorkerId,
    ) -> ActorSummary {
        let metadata = slot.actor.metadata();
        ActorSummary {
            address,
            actor_type: metadata.actor_type_name,
            message_type: metadata.message_type_name,
            worker_id: worker.index(),
            parent: slot.parent_addr,
            mailbox_depth: slot.mailbox.len(),
            status: ActorStatus {
                started: slot.started,
                suspended: slot.suspended,
                stopping: slot.stopping,
                poisoned: slot.poisoned,
            },
            last_message_type: slot.last_msg_type,
            messages_handled: slot.messages_processed,
        }
    }

    fn actor_summary(&self, addr: ActorAddress, worker: WorkerId) -> AdminResult<ActorSummary> {
        self.actors
            .get(&addr)
            .map(|slot| Self::actor_summary_from_slot(addr, slot, worker))
            .ok_or(AdminError::ActorNotFound { actor: addr })
    }

    fn actor_summaries_into(&self, out: &mut Vec<ActorSummary>, worker: WorkerId) {
        out.clear();
        out.extend(
            self.actors
                .iter()
                .map(|(&addr, slot)| Self::actor_summary_from_slot(addr, slot, worker)),
        );
    }

    fn suspend_actor_admin(&mut self, addr: ActorAddress) -> AdminResult<OperationResult> {
        match self.actors.get_mut(&addr) {
            Some(slot) => {
                slot.suspended = true;
                Ok(OperationResult { applied: true })
            }
            None => Err(AdminError::ActorNotFound { actor: addr }),
        }
    }

    fn resume_actor_admin(&mut self, addr: ActorAddress) -> AdminResult<OperationResult> {
        match self.actors.get_mut(&addr) {
            Some(slot) => {
                slot.suspended = false;
                Ok(OperationResult { applied: true })
            }
            None => Err(AdminError::ActorNotFound { actor: addr }),
        }
    }

    fn stop_actor_admin(
        &mut self,
        addr: ActorAddress,
        stats: &WorkerStats,
    ) -> AdminResult<OperationResult> {
        match self.actors.get_mut(&addr) {
            Some(slot) => {
                if !slot.stopping {
                    stats.stops.fetch_add(1, Ordering::Relaxed);
                }
                slot.stopping = true;
                slot.mailbox.clear();
                Ok(OperationResult { applied: true })
            }
            None => Err(AdminError::ActorNotFound { actor: addr }),
        }
    }

    /// Tick all actors in the pool. Returns the number of messages processed.
    ///
    /// Each actor processes up to `budget` messages per tick (0 = unlimited).
    /// This prevents a single hot actor from starving others on the same worker.
    fn tick_all(&mut self, wctx: &WorkerContext<'_>, stats: &WorkerStats, budget: usize) -> usize {
        let mut count = 0;
        for (&addr, slot) in self.actors.iter_mut() {
            if should_skip_actor(slot.poisoned, slot.stopping, slot.suspended) {
                // Discard all messages for poisoned/stopping actors (not suspended — those queue)
                if slot.poisoned || slot.stopping {
                    slot.mailbox.clear();
                }
                continue;
            }

            debug_assert!(
                !slot.poisoned && !slot.stopping && !slot.suspended,
                "G4: non-processable actor reached processing"
            );

            #[cfg(feature = "tracing")]
            let _actor_span = tracing::trace_span!("actor.tick", actor_addr = %addr).entered();

            // Snapshot self-stats before creating Ctx
            let snap_processed = slot.messages_processed;
            let snap_depth = slot.mailbox.len();
            let mut snap_type_counts: Vec<(&'static str, u64)> =
                slot.msg_type_counts.iter().map(|(&k, &v)| (k, v)).collect();
            snap_type_counts.sort_by(|a, b| b.1.cmp(&a.1));

            let ctx = Ctx::new(
                wctx,
                addr,
                slot.parent_addr,
                slot.env.clone(),
                snap_processed,
                snap_depth,
                snap_type_counts,
            );

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
                // Check if on_start requested stop or stop_with
                {
                    let stops = wctx.stop_requests.borrow();
                    if !stops.is_empty() && stops.contains(&addr) {
                        drop(stops);
                        slot.stopping = true;
                        stats.stops.fetch_add(1, Ordering::Relaxed);
                        slot.mailbox.clear();
                        // Check for stop_with value
                        let mut sws = wctx.stop_with_values.borrow_mut();
                        if let Some(pos) = sws.iter().position(|(a, _)| *a == addr) {
                            let (_, val) = sws.swap_remove(pos);
                            slot.exit_value = Some(val);
                        }
                        continue;
                    }
                }
                // Check if on_start requested stop_with (without plain stop)
                {
                    let mut sws = wctx.stop_with_values.borrow_mut();
                    if let Some(pos) = sws.iter().position(|(a, _)| *a == addr) {
                        let (_, val) = sws.swap_remove(pos);
                        slot.exit_value = Some(val);
                        slot.stopping = true;
                        stats.stops.fetch_add(1, Ordering::Relaxed);
                        slot.mailbox.clear();
                        continue;
                    }
                }
                // Check if on_start requested suspend
                {
                    let suspends = wctx.suspend_requests.borrow();
                    if !suspends.is_empty() && suspends.contains(&addr) {
                        drop(suspends);
                        slot.suspended = true;
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

                // Intercept StopWithSignal (from external runtime)
                if msg.is::<StopWithSignal>() {
                    if let Ok(sig) = msg.downcast::<StopWithSignal>() {
                        slot.exit_value = Some(sig.0);
                    }
                    slot.stopping = true;
                    stats.stops.fetch_add(1, Ordering::Relaxed);
                    slot.mailbox.clear();
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
                        eprintln!(
                            "swactor: actor {addr} panicked — poisoned, future messages will be discarded"
                        );
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
                        if slot.msg_type_counts.len() < 32
                            || slot.msg_type_counts.contains_key(type_name)
                        {
                            *slot.msg_type_counts.entry(type_name).or_insert(0) += 1;
                        }
                    }
                }
                count += 1;
                actor_count += 1;

                // Check if handler requested self-stop or stop_with
                {
                    let stops = wctx.stop_requests.borrow();
                    let has_stop = !stops.is_empty() && stops.contains(&addr);
                    drop(stops);

                    let mut sws = wctx.stop_with_values.borrow_mut();
                    let sw_pos = sws.iter().position(|(a, _)| *a == addr);

                    if has_stop || sw_pos.is_some() {
                        if let Some(pos) = sw_pos {
                            let (_, val) = sws.swap_remove(pos);
                            slot.exit_value = Some(val);
                        }
                        drop(sws);
                        slot.stopping = true;
                        stats.stops.fetch_add(1, Ordering::Relaxed);
                        slot.mailbox.clear();
                        break;
                    }
                    drop(sws);
                }

                // Check if handler requested suspend
                {
                    let suspends = wctx.suspend_requests.borrow();
                    if !suspends.is_empty() && suspends.contains(&addr) {
                        drop(suspends);
                        slot.suspended = true;
                        break; // stop processing this actor's messages this tick
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

    /// Remove poisoned and stopping actors, returning their addresses, stop reasons,
    /// and optional exit values.
    /// Called after tick_all so the caller can clean up the address map.
    ///
    /// For stopping actors: calls `on_stop()` before removal (wrapped in catch_unwind).
    /// For poisoned actors: `on_stop()` is NOT called (state may be corrupt).
    pub fn cleanup_dead(
        &mut self,
        inner: &dyn ContextInner,
    ) -> Vec<(ActorAddress, StopReason, Option<ExitValue>)> {
        let dead_addrs: Vec<ActorAddress> = self
            .actors
            .iter()
            .filter(|(_, slot)| slot.poisoned || slot.stopping)
            .map(|(&addr, _)| addr)
            .collect();
        let mut dead = Vec::with_capacity(dead_addrs.len());
        for addr in dead_addrs {
            if let Some(mut slot) = self.actors.remove(&addr) {
                let reason = determine_stop_reason(slot.poisoned, slot.exit_value.is_some());
                // Call on_stop for gracefully stopping actors only
                debug_assert!(
                    slot.poisoned || slot.stopping,
                    "G4: non-dead actor reached cleanup_dead"
                );
                if is_on_stop_eligible(slot.stopping, slot.poisoned) {
                    let mut type_counts: Vec<(&'static str, u64)> =
                        slot.msg_type_counts.iter().map(|(&k, &v)| (k, v)).collect();
                    type_counts.sort_by(|a, b| b.1.cmp(&a.1));
                    let ctx = Ctx::new(
                        inner,
                        addr,
                        slot.parent_addr,
                        slot.env.clone(),
                        slot.messages_processed,
                        slot.mailbox.len(),
                        type_counts,
                    );
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        slot.actor.on_stop(&ctx);
                    }));
                }
                dead.push((addr, reason, slot.exit_value.take()));
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
            let metadata = slot.actor.metadata();
            ActorSnapshot {
                address: addr,
                mailbox_depth: slot.mailbox.len(),
                last_msg_type: slot.last_msg_type,
                actor_type: Some(metadata.actor_type_name),
                message_type: Some(metadata.message_type_name),
                messages_processed: slot.messages_processed,
                poisoned: slot.poisoned,
                message_type_counts: type_counts,
            }
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::StopReason;

    #[test]
    fn lifecycle_decision_helpers_cover_all_inputs() {
        let mut skip_combinations = 0;
        for poisoned in [false, true] {
            for stopping in [false, true] {
                for suspended in [false, true] {
                    assert_eq!(
                        should_skip_actor(poisoned, stopping, suspended),
                        poisoned || stopping || suspended
                    );
                    skip_combinations += 1;
                }
            }
        }
        assert_eq!(skip_combinations, 8);

        let mut on_stop_combinations = 0;
        for stopping in [false, true] {
            for poisoned in [false, true] {
                assert_eq!(
                    is_on_stop_eligible(stopping, poisoned),
                    stopping && !poisoned
                );
                on_stop_combinations += 1;
            }
        }
        assert_eq!(on_stop_combinations, 4);

        let mut reason_combinations = 0;
        for poisoned in [false, true] {
            for has_exit_value in [false, true] {
                let expected = if poisoned {
                    StopReason::Panicked
                } else if has_exit_value {
                    StopReason::Completed
                } else {
                    StopReason::Normal
                };
                assert_eq!(determine_stop_reason(poisoned, has_exit_value), expected);
                reason_combinations += 1;
            }
        }
        assert_eq!(reason_combinations, 4);
    }

    #[test]
    fn bounded_lifecycle_model_preserves_callback_invariants() {
        #[derive(Clone, Copy)]
        enum Step {
            Tick,
            RequestStop,
            Poison,
            Suspend,
            Resume,
            Cleanup,
        }

        #[derive(Clone, Copy, Default)]
        struct ActorProbe {
            started: bool,
            stopping: bool,
            poisoned: bool,
            suspended: bool,
            removed: bool,
            on_start_count: u8,
            handle_count: u8,
            on_stop_count: u8,
            handled_after_on_stop: bool,
        }

        impl ActorProbe {
            fn apply(&mut self, step: Step) {
                if self.removed {
                    return;
                }

                match step {
                    Step::Tick => {
                        let eligible =
                            !should_skip_actor(self.poisoned, self.stopping, self.suspended);
                        if !self.started && eligible {
                            self.started = true;
                            self.on_start_count += 1;
                        }
                        if self.started && eligible {
                            if self.on_stop_count > 0 {
                                self.handled_after_on_stop = true;
                            }
                            self.handle_count += 1;
                        }
                    }
                    Step::RequestStop => {
                        self.stopping = true;
                    }
                    Step::Poison => {
                        self.poisoned = true;
                    }
                    Step::Suspend => {
                        self.suspended = true;
                    }
                    Step::Resume => {
                        self.suspended = false;
                    }
                    Step::Cleanup => {
                        if self.stopping || self.poisoned {
                            if is_on_stop_eligible(self.stopping, self.poisoned) {
                                self.on_stop_count += 1;
                            }
                            self.removed = true;
                        }
                    }
                }
            }

            fn assert_invariants(self) {
                assert!(self.on_start_count <= 1, "on_start fired more than once");
                assert!(self.on_stop_count <= 1, "on_stop fired more than once");
                assert!(
                    !self.handled_after_on_stop,
                    "handle fired after on_stop cleanup"
                );
                if self.handle_count > 0 {
                    assert!(self.started, "handle fired before on_start");
                }
                if self.on_stop_count > 0 {
                    assert!(self.removed, "on_stop fired without cleanup");
                    assert!(!self.poisoned, "poisoned actor ran on_stop");
                }
            }
        }

        fn walk(depth: usize, state: ActorProbe, checked: &mut usize) {
            state.assert_invariants();
            *checked += 1;

            if depth == 0 {
                return;
            }

            for step in [
                Step::Tick,
                Step::RequestStop,
                Step::Poison,
                Step::Suspend,
                Step::Resume,
                Step::Cleanup,
            ] {
                let mut next = state;
                next.apply(step);
                walk(depth - 1, next, checked);
            }
        }

        let mut checked = 0;
        walk(6, ActorProbe::default(), &mut checked);
        assert_eq!(checked, 55_987);
    }
}
