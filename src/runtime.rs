use std::any::Any;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crate::actor::{Actor, ActorAddress, ActorInterface, AnyActor, Message};
use crate::channel::{Receiver, Sender};
// Re-export config types so existing code using `runtime::RuntimeConfig` still works
pub use crate::config::{BackoffPolicy, RuntimeConfig};
use crate::delivery::{AddressMap, Envelope, InboxRegistry, Placement, TickContext, WorkerId};
use crate::stats::{StatsHook, WorkerStats};
// Re-export stats types so existing code using `runtime::*` still works
pub use crate::stats::{RuntimeStats, WorkerInfo};
use crate::worker::Worker;
use crate::Error;

/// Generic message inbox for receiving messages outside of the runtime.
pub struct Inbox<M: Message> {
    addr: ActorAddress,
    inner: Receiver<M>,
}

impl<M: Message> Inbox<M> {
    pub fn addr(&self) -> &ActorAddress {
        &self.addr
    }

    pub fn try_recv(&self) -> Option<M> {
        self.inner.try_recv()
    }
}

/// Handle for dealing with a runtime that has started via the `Runtime::run()` method.
pub struct RuntimeHandle {
    pub runtime: Arc<Runtime>,
    threads: Vec<JoinHandle<()>>,
}

impl RuntimeHandle {
    pub fn join(self) {
        for handle in self.threads {
            let _ = handle.join();
        }
    }

    /// Simple helper, calls the inner `Runtime::shutdown()` method
    pub fn shutdown(&self) {
        self.runtime.shutdown();
    }
}

// Re-export Ctx for backwards compatibility
pub use crate::actor::Ctx;
use crate::actor::ContextInner;

// ─── Runtime ─────────────────────────────────────────────────────────────────

/// The `Runtime` struct is the primary gateway for interacting with the framework.
pub struct Runtime {
    config: RuntimeConfig,
    address_map: Arc<AddressMap>,
    inbox_registry: Arc<InboxRegistry>,
    transfer_txs: Vec<Sender<Envelope>>,
    spawn_txs: Vec<Sender<(ActorAddress, Box<dyn AnyActor>)>>,
    placement: Placement,
    is_running: AtomicBool,
    worker_stats: Vec<Arc<WorkerStats>>,
    stats_hook: Option<Arc<dyn StatsHook>>,
    /// Workers available for tick(). run() drains this and moves workers to threads.
    tick_workers: RefCell<Vec<Worker>>,
    created_at: Instant,
    #[cfg(feature = "transport")]
    codec_registry: Option<Arc<crate::transport::CodecRegistry>>,
    #[cfg(feature = "transport")]
    transport_router: Option<Arc<crate::transport::TransportRouter>>,
}

// Safety: RefCell<Vec<Worker>> is only accessed from the owning thread via tick().
// After run() the RefCell is empty and not accessed by worker threads.
unsafe impl Sync for Runtime {}

/// Globally unique identity of a swactor runtime instance.
/// Pure identity — no networking info. A runtime can exist on any device,
/// any protocol, or no network at all.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RuntimeAddress(pub [u8; 32]);

impl std::fmt::Display for RuntimeAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in &self.0[..8] {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "\u{2026}")
    }
}

impl RuntimeAddress {
    pub fn new_random() -> Self {
        let mut bytes = [0u8; 32];
        crate::get_random(&mut bytes);
        Self(bytes)
    }
}

impl Runtime {
    /// Builds a new `Runtime` struct, but does not yet run anything. If multithreaded, call
    /// `run()`, if single threaded, needs to be driven by calls to the `tick()` method.
    pub fn new(config: RuntimeConfig) -> Self {
        let num_workers = if config.num_threads < 2 {
            1
        } else {
            config.num_threads
        };

        let address_map = Arc::new(AddressMap::with_capacity(config.max_actors));
        let inbox_registry = Arc::new(InboxRegistry::new());
        let placement = Placement::new(num_workers);

        let mut transfer_txs = Vec::with_capacity(num_workers);
        let mut spawn_txs = Vec::with_capacity(num_workers);
        let mut worker_stats = Vec::with_capacity(num_workers);
        let mut workers = Vec::with_capacity(num_workers);

        for i in 0..num_workers {
            let transfer_rx = Receiver::<Envelope>::new(config.channel_buffer_size);
            let transfer_tx = transfer_rx.new_sender();
            transfer_txs.push(transfer_tx);

            let spawn_rx =
                Receiver::<(ActorAddress, Box<dyn AnyActor>)>::new(config.max_actors);
            let spawn_tx = spawn_rx.new_sender();
            spawn_txs.push(spawn_tx);

            let stats = Arc::new(WorkerStats::new());
            worker_stats.push(stats.clone());
            workers.push(Worker::new(WorkerId(i), transfer_rx, spawn_rx, stats));
        }

        let rt = Self {
            config,
            address_map,
            inbox_registry,
            transfer_txs,
            spawn_txs,
            placement,
            is_running: AtomicBool::new(false),
            worker_stats,
            stats_hook: None,
            tick_workers: RefCell::new(workers),
            created_at: Instant::now(),
            #[cfg(feature = "transport")]
            codec_registry: None,
            #[cfg(feature = "transport")]
            transport_router: None,
        };

        #[cfg(feature = "tracing")]
        tracing::info!(
            num_workers,
            max_actors = rt.config.max_actors,
            "runtime.created"
        );

        rt
    }

    /// Spawn an actor, returns its address
    pub fn spawn<A: ActorInterface>(&self, actor: A) -> Result<ActorAddress, Error> {
        let addr = ActorAddress::new_random();
        let worker_id = self.placement.next_worker();
        self.address_map.insert(addr, worker_id);
        let boxed: Box<dyn AnyActor> = Box::new(Actor::new(actor));
        self.spawn_txs[worker_id.as_usize()]
            .send((addr, boxed));

        #[cfg(feature = "tracing")]
        tracing::info!(
            actor_addr = %addr,
            worker_id = worker_id.as_usize(),
            "actor.spawned"
        );

        Ok(addr)
    }

    /// Send a message to an actor address
    pub fn send_to<M: Message>(&self, addr: ActorAddress, msg: M) -> Result<(), Error> {
        let result = self.send_any(addr, Box::new(msg));

        #[cfg(feature = "tracing")]
        tracing::trace!(dest = %addr, "message.sent");

        result
    }

    /// Create an external inbox for receiving messages in the outer process containing the runtime
    pub fn new_inbox<M: Message>(&self) -> Result<Inbox<M>, Error> {
        let addr = ActorAddress::new_random();
        let receiver = Receiver::<M>::new(self.config.channel_buffer_size);
        let sender = receiver.new_sender();
        self.inbox_registry.register(addr, Arc::new(sender));
        Ok(Inbox {
            addr,
            inner: receiver,
        })
    }

    fn make_tick_context(&self) -> TickContext<'_> {
        TickContext {
            address_map: &self.address_map,
            transfer_txs: &self.transfer_txs,
            spawn_txs: &self.spawn_txs,
            placement: &self.placement,
            inbox_registry: &self.inbox_registry,
            config: &self.config,
            stats_hook: self.stats_hook.as_deref(),
            #[cfg(feature = "transport")]
            codec_registry: self.codec_registry.as_deref(),
            #[cfg(feature = "transport")]
            transport_router: self.transport_router.as_deref(),
        }
    }

    /// Drive one tick of the single-threaded worker.
    ///
    /// Panics if called on a multi-threaded runtime — use `run()` instead.
    pub fn tick(&self) {
        assert!(
            self.config.num_threads < 2,
            "tick() is only valid for single-threaded runtimes; use run() for multi-threaded"
        );
        let tc = self.make_tick_context();
        for worker in self.tick_workers.borrow_mut().iter_mut() {
            worker.tick_once(&tc);
        }
    }

    /// Spawn worker threads and start processing, returning a handle
    /// to interact with the runtime and join the threads later.
    ///
    /// Works in both single-threaded and multi-threaded configurations.
    /// In single-threaded mode, one background thread is spawned.
    pub fn run(self) -> Result<RuntimeHandle, Error> {
        self.is_running.store(true, Ordering::Release);

        #[cfg(feature = "tracing")]
        tracing::info!(num_workers = self.config.num_threads.max(1), "runtime.started");

        let workers: Vec<Worker> = self.tick_workers.replace(Vec::new());

        let rt = Arc::new(self);
        let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(workers.len());

        for mut worker in workers {
            let rt_clone = rt.clone();
            let name = format!("swactor-worker-{}", worker.id.0);
            let handle = thread::Builder::new()
                .name(name)
                .spawn(move || {
                    let tc = rt_clone.make_tick_context();
                    worker.run(&tc, &rt_clone.is_running);
                })
                .expect("failed to spawn worker thread");
            handles.push(handle);
        }

        Ok(RuntimeHandle {
            runtime: rt,
            threads: handles,
        })
    }

    /// Returns a snapshot of runtime stats: actor placements and per-worker info.
    pub fn stats(&self) -> RuntimeStats {
        let num_workers = if self.config.num_threads < 2 { 1 } else { self.config.num_threads };

        let workers = self.worker_stats.iter().enumerate()
            .map(|(i, ws)| ws.snapshot(i))
            .collect();

        let actors = self.address_map.snapshot().into_iter()
            .map(|(addr, wid)| (addr, wid.as_usize()))
            .collect();

        let tick_timings = self.worker_stats.iter()
            .map(|ws| ws.drain_tick_timings())
            .collect();

        let uptime_ms = self.created_at.elapsed().as_millis() as u64;

        RuntimeStats { num_workers, uptime_ms, actors, workers, actor_details: Vec::new(), tick_timings }
    }

    /// Signal all workers to stop
    pub fn shutdown(&self) {
        #[cfg(feature = "tracing")]
        tracing::info!("runtime.shutdown");

        self.is_running.store(false, Ordering::Release);
    }

    /// Set a stats hook to receive per-actor snapshots from workers.
    ///
    /// Must be called before [`run()`](Self::run) or [`tick()`](Self::tick).
    pub fn set_stats_hook(&mut self, hook: Arc<dyn StatsHook>) {
        self.stats_hook = Some(hook);
    }

    /// Set the codec registry for remote transport.
    #[cfg(feature = "transport")]
    pub fn set_codec_registry(&mut self, registry: Arc<crate::transport::CodecRegistry>) {
        self.codec_registry = Some(registry);
    }

    /// Set the transport router for remote message delivery.
    #[cfg(feature = "transport")]
    pub fn set_transport_router(&mut self, router: Arc<crate::transport::TransportRouter>) {
        self.transport_router = Some(router);
    }

    /// Deliver a raw deserialized message into the runtime.
    ///
    /// Used by [`CodecRegistry::receive`](crate::transport::CodecRegistry::receive)
    /// to inject incoming messages from remote runtimes.
    #[cfg(feature = "transport")]
    pub fn deliver_raw(
        &self,
        addr: ActorAddress,
        msg: Box<dyn Any + Send>,
    ) -> Result<(), Error> {
        match self.address_map.lookup(&addr) {
            Some(wid) => {
                self.transfer_txs[wid.as_usize()]
                    .send(Envelope::new(addr, msg));
                Ok(())
            }
            None => self.inbox_registry.try_deliver(addr, msg),
        }
    }
}

impl ContextInner for Runtime {
    fn send_any(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error> {
        match self.address_map.lookup(&addr) {
            Some(wid) => {
                self.transfer_txs[wid.as_usize()]
                    .send(Envelope::new(addr, msg));
                Ok(())
            }
            None => self.make_tick_context().route_nonlocal(addr, msg),
        }
    }

    fn spawn_any(&self, addr: ActorAddress, actor: Box<dyn AnyActor>) {
        let worker_id = self.placement.next_worker();
        self.address_map.insert(addr, worker_id);
        self.spawn_txs[worker_id.as_usize()]
            .send((addr, actor));
    }
}
