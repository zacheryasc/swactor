use std::any::Any;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
#[cfg(not(target_arch = "wasm32"))]
use std::thread::{self, JoinHandle};
use std::thread::Thread;
use crate::Instant;

use crate::actor::{Actor, ActorAddress, ActorInterface, AnyActor, Environment, ExitValue, Message, ResumeSignal, SpawnRequest, StopSignal, StopWithSignal, SystemInfo};
use crate::channel::{Receiver, Sender};
// Re-export config types so existing code using `runtime::RuntimeConfig` still works
pub use crate::config::{BackoffPolicy, MailboxOverflow, RuntimeConfig};
use crate::delivery::{AddressMap, Envelope, InboxRegistry, Placement, TickContext, WorkerId};
use crate::extension::RuntimeExtension;
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

/// Pending ask response — wraps an inbox with convenience recv methods.
///
/// Created by [`Runtime::ask`]. Provides `try_recv()` for polling and
/// `recv_ticking()` for automatic tick-until-response.
pub struct Ask<R: Message> {
    inbox: Inbox<R>,
}

impl<R: Message> Ask<R> {
    /// Try to receive the response without ticking.
    pub fn try_recv(&self) -> Option<R> {
        self.inbox.try_recv()
    }

    /// Tick the runtime until a response arrives or `max_ticks` is exhausted.
    ///
    /// Only valid for single-threaded runtimes (panics if `num_threads >= 2`).
    pub fn recv_ticking(&self, rt: &Runtime, max_ticks: usize) -> Result<R, Error> {
        for _ in 0..max_ticks {
            rt.tick();
            if let Some(resp) = self.inbox.try_recv() {
                return Ok(resp);
            }
        }
        Err(Error::from("ask timeout: no response within max_ticks"))
    }

    /// Get the reply address (for manual message construction).
    pub fn reply_addr(&self) -> &ActorAddress {
        self.inbox.addr()
    }
}

/// Handle for dealing with a runtime that has started via the `Runtime::run()` method.
#[cfg(not(target_arch = "wasm32"))]
pub struct RuntimeHandle {
    pub runtime: Arc<Runtime>,
    threads: Vec<JoinHandle<()>>,
}

#[cfg(not(target_arch = "wasm32"))]
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
    extension: Option<Arc<dyn RuntimeExtension>>,
    transfer_txs: Vec<Sender<Envelope>>,
    spawn_txs: Vec<Sender<SpawnRequest>>,
    placement: Placement,
    is_running: AtomicBool,
    worker_stats: Vec<Arc<WorkerStats>>,
    stats_hook: Option<Arc<dyn StatsHook>>,
    /// Workers available for tick(). run() drains this and moves workers to threads.
    tick_workers: RefCell<Vec<Worker>>,
    /// Thread handles for waking parked workers. Set by workers on startup via OnceLock.
    worker_threads: Arc<Vec<OnceLock<Thread>>>,
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

/// A cloneable, `Send + Sync` handle for injecting messages into actor mailboxes
/// from any thread — including non-actor I/O threads.
///
/// Created via [`Runtime::create_sender`]. The primary use case is bridging
/// background I/O (e.g., pipe readers, network listeners) with the tick-based
/// actor system.
pub struct ExternalSender {
    address_map: Arc<AddressMap>,
    transfer_txs: Vec<Sender<Envelope>>,
    worker_threads: Arc<Vec<OnceLock<Thread>>>,
}

impl Clone for ExternalSender {
    fn clone(&self) -> Self {
        Self {
            address_map: self.address_map.clone(),
            transfer_txs: self.transfer_txs.clone(),
            worker_threads: self.worker_threads.clone(),
        }
    }
}

// Safety: All fields are Send+Sync (Arc<AddressMap> uses RwLock,
// Sender<Envelope> wraps Arc<HybridChannel>, Thread is Send+Sync).
unsafe impl Send for ExternalSender {}
unsafe impl Sync for ExternalSender {}

impl ExternalSender {
    /// Send a typed message to an actor address, waking the owning worker thread.
    ///
    /// Returns `Err` if the address is not found in the runtime's address map.
    pub fn send_to<M: Message>(&self, addr: ActorAddress, msg: M) -> Result<(), Error> {
        match self.address_map.lookup(&addr) {
            Some(wid) => {
                self.transfer_txs[wid.as_usize()]
                    .send(Envelope::new(addr, Box::new(msg)));
                notify_worker(&self.worker_threads, wid.as_usize());
                Ok(())
            }
            None => Err(Error::from("Address not found")),
        }
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

        let mut transfer_txs = Vec::with_capacity(num_workers);
        let mut spawn_txs = Vec::with_capacity(num_workers);
        let mut worker_stats = Vec::with_capacity(num_workers);
        let mut workers = Vec::with_capacity(num_workers);

        for i in 0..num_workers {
            let transfer_rx = Receiver::<Envelope>::new(config.channel_buffer_size);
            let transfer_tx = transfer_rx.new_sender();
            transfer_txs.push(transfer_tx);

            let spawn_rx =
                Receiver::<SpawnRequest>::new(config.max_actors);
            let spawn_tx = spawn_rx.new_sender();
            spawn_txs.push(spawn_tx);

            let stats = Arc::new(WorkerStats::new());
            worker_stats.push(stats.clone());
            workers.push(Worker::new(
                WorkerId(i),
                transfer_rx,
                spawn_rx,
                stats,
                config.default_mailbox_capacity,
                config.mailbox_overflow,
            ));
        }

        let placement = Placement::new(num_workers, worker_stats.clone());

        let worker_threads: Arc<Vec<OnceLock<Thread>>> =
            Arc::new((0..num_workers).map(|_| OnceLock::new()).collect());

        let rt = Self {
            config,
            address_map,
            inbox_registry,
            extension: None,
            transfer_txs,
            spawn_txs,
            placement,
            is_running: AtomicBool::new(false),
            worker_stats,
            stats_hook: None,
            tick_workers: RefCell::new(workers),
            worker_threads,
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
            .send(SpawnRequest { addr, actor: boxed, parent: None, env: Environment::new() });

        #[cfg(feature = "tracing")]
        tracing::info!(
            actor_addr = %addr,
            worker_id = worker_id.as_usize(),
            "actor.spawned"
        );

        Ok(addr)
    }

    /// Spawn an actor with a pre-built environment, returns its address.
    pub fn spawn_with_env<A: ActorInterface>(&self, actor: A, env: Environment) -> Result<ActorAddress, Error> {
        let addr = ActorAddress::new_random();
        let worker_id = self.placement.next_worker();
        self.address_map.insert(addr, worker_id);
        let boxed: Box<dyn AnyActor> = Box::new(Actor::new(actor));
        self.spawn_txs[worker_id.as_usize()]
            .send(SpawnRequest { addr, actor: boxed, parent: None, env });

        #[cfg(feature = "tracing")]
        tracing::info!(
            actor_addr = %addr,
            worker_id = worker_id.as_usize(),
            "actor.spawned"
        );

        Ok(addr)
    }

    /// Install a runtime extension. Extensions provide higher-level features
    /// (naming, monitoring, groups) via lifecycle hooks.
    ///
    /// Must be called before `run()` or `tick()`.
    pub fn with_extension(mut self, ext: Arc<dyn RuntimeExtension>) -> Self {
        // Create per-worker extensions (e.g., timer wheels)
        for worker in self.tick_workers.get_mut().iter_mut() {
            if let Some(wext) = ext.create_worker_extension() {
                worker.worker_ext = Some(wext);
            }
        }
        self.extension = Some(ext);
        self
    }

    /// Access the installed runtime extension (if any).
    pub fn extension(&self) -> Option<&dyn RuntimeExtension> {
        self.extension.as_deref()
    }

    /// Send a request and get a handle for the response.
    ///
    /// Creates a temporary inbox, calls `msg_builder` with the inbox's address
    /// (so you can embed it as `reply_to`), sends the message, and returns an
    /// [`Ask`] handle for receiving the response.
    pub fn ask<Req: Message, Resp: Message>(
        &self,
        addr: ActorAddress,
        msg_builder: impl FnOnce(ActorAddress) -> Req,
    ) -> Result<Ask<Resp>, Error> {
        let inbox = self.new_inbox::<Resp>()?;
        let msg = msg_builder(*inbox.addr());
        self.send_to(addr, msg)?;
        Ok(Ask { inbox })
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

    /// Create an [`ExternalSender`] handle for injecting messages from any thread.
    ///
    /// The returned handle is `Clone + Send + Sync` and can be moved into
    /// background I/O threads to bridge external events into the actor system.
    pub fn create_sender(&self) -> ExternalSender {
        ExternalSender {
            address_map: self.address_map.clone(),
            transfer_txs: self.transfer_txs.iter().cloned().collect(),
            worker_threads: self.worker_threads.clone(),
        }
    }

    fn make_tick_context(&self) -> TickContext<'_> {
        TickContext {
            address_map: &self.address_map,
            transfer_txs: &self.transfer_txs,
            spawn_txs: &self.spawn_txs,
            placement: &self.placement,
            inbox_registry: &self.inbox_registry,
            config: &self.config,
            extension: self.extension.as_deref(),
            stats_hook: self.stats_hook.as_deref(),
            worker_threads: &self.worker_threads,
            worker_stats: &self.worker_stats,
            created_at: self.created_at,
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
    ///
    /// Not available on wasm32 — use the browser crate's Web Worker-based run instead.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn run(self) -> Result<RuntimeHandle, Error> {
        self.is_running.store(true, Ordering::Release);

        #[cfg(feature = "tracing")]
        tracing::info!(num_workers = self.config.num_threads.max(1), "runtime.started");

        let workers: Vec<Worker> = self.tick_workers.replace(Vec::new());

        let rt = Arc::new(self);
        let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(workers.len());

        for mut worker in workers {
            let rt_clone = rt.clone();
            let worker_id = worker.id.0;
            let name = format!("swactor-worker-{}", worker_id);
            let handle = thread::Builder::new()
                .name(name)
                .spawn(move || {
                    // Register this thread so send_to/spawn can unpark us
                    let _ = rt_clone.worker_threads[worker_id].set(thread::current());
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

    /// Request an actor to stop gracefully.
    ///
    /// The actor's `on_stop()` hook is called before removal. Pending messages
    /// in the mailbox are discarded. The stop takes effect on the next tick.
    ///
    /// Returns `Err` if the actor address is not found in the runtime.
    pub fn stop_actor(&self, addr: ActorAddress) -> Result<(), Error> {
        match self.address_map.lookup(&addr) {
            Some(wid) => {
                self.transfer_txs[wid.as_usize()]
                    .send(Envelope::new(addr, Box::new(StopSignal)));
                notify_worker(&self.worker_threads, wid.as_usize());
                Ok(())
            }
            None => Err(Error::from("Actor not found")),
        }
    }

    /// Signal all workers to stop and wake any that are parked.
    pub fn shutdown(&self) {
        #[cfg(feature = "tracing")]
        tracing::info!("runtime.shutdown");

        self.is_running.store(false, Ordering::Release);
        // Wake all parked workers so they see the shutdown flag immediately
        for thread in self.worker_threads.iter() {
            if let Some(t) = thread.get() {
                t.unpark();
            }
        }
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

/// Wake a parked worker thread so it can process new work.
/// No-op if the thread handle hasn't been registered yet (single-threaded tick mode).
#[inline]
pub(crate) fn notify_worker(threads: &[OnceLock<Thread>], wid: usize) {
    if let Some(t) = threads.get(wid).and_then(|o| o.get()) {
        t.unpark();
    }
}

#[allow(private_interfaces)]
impl ContextInner for Runtime {
    fn send_any(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error> {
        match self.address_map.lookup(&addr) {
            Some(wid) => {
                self.transfer_txs[wid.as_usize()]
                    .send(Envelope::new(addr, msg));
                notify_worker(&self.worker_threads, wid.as_usize());
                Ok(())
            }
            None => self.make_tick_context().route_nonlocal(addr, msg),
        }
    }

    fn spawn_any(&self, request: SpawnRequest) {
        let worker_id = self.placement.next_worker();
        self.address_map.insert(request.addr, worker_id);
        self.spawn_txs[worker_id.as_usize()]
            .send(request);
        notify_worker(&self.worker_threads, worker_id.as_usize());
    }

    fn request_stop(&self, addr: ActorAddress) {
        // From spawn context (outside worker), send StopSignal through transfer queue
        if let Some(wid) = self.address_map.lookup(&addr) {
            self.transfer_txs[wid.as_usize()]
                .send(Envelope::new(addr, Box::new(StopSignal)));
            notify_worker(&self.worker_threads, wid.as_usize());
        }
    }

    fn request_stop_with(&self, addr: ActorAddress, value: ExitValue) {
        if let Some(wid) = self.address_map.lookup(&addr) {
            self.transfer_txs[wid.as_usize()]
                .send(Envelope::new(addr, Box::new(StopWithSignal(value))));
            notify_worker(&self.worker_threads, wid.as_usize());
        }
    }

    fn request_suspend(&self, addr: ActorAddress) {
        // Outside worker context — not supported (suspend is per-actor, from handler)
        eprintln!("swactor: request_suspend called outside worker context for {addr} — ignored");
    }

    fn request_resume(&self, addr: ActorAddress) {
        if let Some(wid) = self.address_map.lookup(&addr) {
            self.transfer_txs[wid.as_usize()]
                .send(Envelope::new(addr, Box::new(ResumeSignal)));
            notify_worker(&self.worker_threads, wid.as_usize());
        }
    }

    fn post_worker_request(&self, _request: Box<dyn Any + Send>) {
        // Worker requests (e.g., timers) are per-worker; posting from outside
        // a worker context (e.g., rt.spawn() callback) is not supported.
        eprintln!("swactor: post_worker_request called outside worker context — ignored");
    }

    fn extension(&self) -> Option<&dyn RuntimeExtension> {
        self.extension.as_deref()
    }

    fn system_info(&self) -> SystemInfo {
        let num_workers = self.config.num_threads.max(1);
        let total_actors: usize = self.worker_stats.iter()
            .map(|ws| ws.num_actors.load(Ordering::Relaxed))
            .sum();
        SystemInfo {
            worker_id: 0,
            num_workers,
            total_actors,
            uptime_ms: self.created_at.elapsed().as_millis() as u64,
        }
    }
}
