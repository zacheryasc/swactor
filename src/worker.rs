use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crate::actor::{ActorAddress, AnyActor, Message};
use crate::address_map::{AddressMap, Placement, WorkerId};
use crate::channel::{Receiver, Sender};
use crate::config::{BackoffPolicy, RuntimeConfig};
use crate::runtime::{ContextInner, Envelope, InboxRegistry};
use crate::Error;

/// Shared state passed to tick_once — single thin pointer avoids register spill.
pub(crate) struct TickContext<'a> {
    pub address_map: &'a AddressMap,
    pub transfer_txs: &'a [Sender<Envelope>],
    pub spawn_txs: &'a [Sender<(ActorAddress, Box<dyn AnyActor>)>],
    pub placement: &'a Placement,
    pub inbox_registry: &'a InboxRegistry,
    pub config: &'a RuntimeConfig,
}

/// A worker owns a set of actors and runs them in a loop.
pub(crate) struct Worker {
    id: WorkerId,
    pool: ActorPool,
    transfer_rx: Receiver<Envelope>,
    spawn_rx: Receiver<(ActorAddress, Box<dyn AnyActor>)>,
}

impl Worker {
    pub fn new(
        id: WorkerId,
        transfer_rx: Receiver<Envelope>,
        spawn_rx: Receiver<(ActorAddress, Box<dyn AnyActor>)>,
    ) -> Self {
        Self {
            id,
            pool: ActorPool::new(),
            transfer_rx,
            spawn_rx,
        }
    }

    /// Run one iteration of the worker loop. Returns `true` if any work was done.
    pub fn tick_once(&mut self, tc: &TickContext) -> bool {
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

        {
            let worker_ctx = WorkerContext {
                worker_id: self.id,
                address_map: tc.address_map,
                transfer_txs: tc.transfer_txs,
                spawn_txs: tc.spawn_txs,
                placement: tc.placement,
                inbox_registry: tc.inbox_registry,
                config: tc.config,
                pending_local: &pending_local,
            };
            if self.pool.tick_all(&worker_ctx) {
                did_work = true;
            }
        }

        // 4. Drain pending_local buffer → deliver to local actors
        let pending = pending_local.into_inner();
        if !pending.is_empty() {
            did_work = true;
        }
        for (addr, msg) in pending {
            self.pool.deliver(&addr, msg);
        }

        did_work
    }

    pub(crate) fn run(&mut self, tc: &TickContext, is_running: &AtomicBool, backoff: &BackoffPolicy) {
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
    address_map: &'a AddressMap,
    transfer_txs: &'a [Sender<Envelope>],
    spawn_txs: &'a [Sender<(ActorAddress, Box<dyn AnyActor>)>],
    placement: &'a Placement,
    inbox_registry: &'a InboxRegistry,
    config: &'a RuntimeConfig,
    pending_local: &'a RefCell<Vec<(ActorAddress, Box<dyn Any + Send>)>>,
}

impl ContextInner for WorkerContext<'_> {
    fn send_any(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error> {
        match self.address_map.lookup(&addr) {
            Some(wid) if wid == self.worker_id => {
                // Same worker: buffer for local delivery (after current tick round)
                self.pending_local.borrow_mut().push((addr, msg));
                Ok(())
            }
            Some(wid) => {
                // Cross worker: envelope through transfer queue
                let envelope = Envelope::new(addr, msg);
                let _ = self.transfer_txs[wid.as_usize()].try_send(envelope);
                Ok(())
            }
            None => {
                // Try inbox registry (external inboxes)
                self.inbox_registry.try_deliver(addr, msg)
            }
        }
    }

    fn spawn_any(&self, addr: ActorAddress, actor: Box<dyn AnyActor>) -> Result<(), Error> {
        let worker_id = self.placement.next_worker();
        self.address_map.insert(addr, worker_id);
        self.spawn_txs[worker_id.as_usize()]
            .try_send((addr, actor))
            .map_err(|_| Error::from("Spawn queue full"))
    }

    fn mailbox_waterlevel(&self) -> usize {
        self.config.mailbox_waterlevel
    }
}

/// Per-worker actor storage.
pub(crate) struct ActorPool {
    actors: HashMap<ActorAddress, Box<dyn AnyActor>>,
}

impl ActorPool {
    pub fn new() -> Self {
        Self {
            actors: HashMap::new(),
        }
    }

    pub fn insert(&mut self, addr: ActorAddress, actor: Box<dyn AnyActor>) {
        self.actors.insert(addr, actor);
    }

    pub fn remove(&mut self, addr: &ActorAddress) -> Option<Box<dyn AnyActor>> {
        self.actors.remove(addr)
    }

    /// Deliver a type-erased message to the actor at `addr`.
    /// Returns `true` if the actor was found and the message type matched.
    pub fn deliver(&mut self, addr: &ActorAddress, msg: Box<dyn Any + Send>) -> bool {
        if let Some(actor) = self.actors.get_mut(addr) {
            actor.deliver(msg)
        } else {
            false
        }
    }

    /// Tick all actors in the pool. Returns `true` if any actor processed messages.
    pub fn tick_all(&mut self, inner: &dyn ContextInner) -> bool {
        let mut did_work = false;
        for actor in self.actors.values_mut() {
            if actor.tick(inner) {
                did_work = true;
            }
        }
        did_work
    }

    pub fn len(&self) -> usize {
        self.actors.len()
    }
}



pub struct Mailbox<M: Message> {
    queue: VecDeque<M>,
    waterlevel: usize,
}

impl<M: Message> Mailbox<M> {
    pub fn new(waterlevel: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            waterlevel,
        }
    }

    pub fn push(&mut self, msg: M) {
        self.queue.push_back(msg);
    }

    pub fn pop(&mut self) -> Option<M> {
        self.queue.pop_front()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// How many messages to process this tick:
    /// - `len < waterlevel` → process all (`len`)
    /// - `len >= waterlevel` → process half (`len >> 1`)
    pub fn drain_count(&self) -> usize {
        let len = self.queue.len();
        if len < self.waterlevel {
            len
        } else {
            len >> 1
        }
    }
}

