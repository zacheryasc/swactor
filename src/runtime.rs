use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};

use crate::actor::{Actor, ActorAddress, ActorInterface, AnyActor, Message};
use crate::address_map::{AddressMap, Placement, WorkerId};
use crate::channel::{Receiver, Sender};
// Re-export config types so existing code using `runtime::RuntimeConfig` still works
pub use crate::config::{BackoffPolicy, RuntimeConfig};
use crate::worker::Mailbox;
use crate::worker::{TickContext, Worker};
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

/// Actor syscall interface — passed to `ActorInterface::handle()`.
///
/// Wraps a `&dyn ContextInner` to solve the object-safety problem while
/// providing a typed public API.
pub struct Ctx<'a> {
    inner: &'a dyn ContextInner,
    self_addr: ActorAddress,
}

impl<'a> Ctx<'a> {
    pub(crate) fn new(inner: &'a dyn ContextInner, self_addr: ActorAddress) -> Self {
        Self { inner, self_addr }
    }

    #[cfg(feature = "python")]
    pub(crate) fn raw_inner(&self) -> &dyn ContextInner {
        self.inner
    }

    /// Returns the address of the actor currently being ticked.
    pub fn self_addr(&self) -> ActorAddress {
        self.self_addr
    }

    /// Send a typed message to an actor address.
    pub fn send<M: Message>(&self, addr: ActorAddress, msg: M) -> Result<(), Error> {
        self.inner.send_any(addr, Box::new(msg))
    }

    /// Spawn a new actor, returning its address.
    pub fn spawn<A: ActorInterface>(&self, actor: A) -> Result<ActorAddress, Error> {
        let addr = ActorAddress::new_random();
        let waterlevel = self.inner.mailbox_waterlevel();
        let actor = Actor::new(addr, Mailbox::new(waterlevel), actor);
        let boxed: Box<dyn AnyActor> = Box::new(actor);
        self.inner.spawn_any(addr, boxed)?;
        Ok(addr)
    }
}

/// Type-erased sender for external inboxes.
pub(crate) trait SenderT: Send + Sync {
    fn try_send_any(&self, msg: Box<dyn Any + Send>);
}

impl<M: Message> SenderT for Sender<M> {
    fn try_send_any(&self, msg: Box<dyn Any + Send>) {
        if let Ok(typed) = msg.downcast::<M>() {
            let _ = Sender::try_send(self, *typed);
        }
    }
}


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
    /// Single-threaded mode: worker stored inline
    single_worker: Option<RefCell<Worker>>,
    /// Multi-threaded mode: workers waiting to be assigned to threads by run()
    pending_workers: Option<Vec<Worker>>,
}

// Safety: RefCell<Worker> is only accessed from the thread that owns the Runtime
// in single-threaded mode. In multi-threaded mode, single_worker is None and
// pending_workers is consumed by run() before Arc sharing.
unsafe impl Sync for Runtime {}

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
        let mut workers = Vec::with_capacity(num_workers);

        for i in 0..num_workers {
            let transfer_rx = Receiver::<Envelope>::new(config.actor_max_messages);
            let transfer_tx = transfer_rx.new_sender();
            transfer_txs.push(transfer_tx);

            let spawn_rx =
                Receiver::<(ActorAddress, Box<dyn AnyActor>)>::new(config.max_actors);
            let spawn_tx = spawn_rx.new_sender();
            spawn_txs.push(spawn_tx);

            workers.push(Worker::new(WorkerId(i), transfer_rx, spawn_rx));
        }

        if config.num_threads < 2 {
            // Single-threaded: store one worker inline
            let worker = workers.remove(0);
            Self {
                config,
                address_map,
                inbox_registry,
                transfer_txs,
                spawn_txs,
                placement,
                is_running: AtomicBool::new(false),
                single_worker: Some(RefCell::new(worker)),
                pending_workers: None,
            }
        } else {
            // Multi-threaded: stash workers for run()
            Self {
                config,
                address_map,
                inbox_registry,
                transfer_txs,
                spawn_txs,
                placement,
                is_running: AtomicBool::new(false),
                single_worker: None,
                pending_workers: Some(workers),
            }
        }
    }

    /// Spawn an actor, returns its address
    pub fn spawn<A: ActorInterface>(&self, actor: A) -> Result<ActorAddress, Error> {
        let addr = ActorAddress::new_random();
        let worker_id = self.placement.next_worker();
        self.address_map.insert(addr, worker_id);
        let actor = Actor::new(addr, Mailbox::new(self.config.mailbox_waterlevel), actor);
        let boxed: Box<dyn AnyActor> = Box::new(actor);
        self.spawn_txs[worker_id.as_usize()]
            .try_send((addr, boxed))
            .map_err(|_| Error::from("Runtime error: spawn queue full"))?;
        Ok(addr)
    }

    /// Send a message to an actor address
    pub fn send_to<M: Message>(&self, addr: ActorAddress, msg: M) -> Result<(), Error> {
        let msg_box: Box<dyn Any + Send> = Box::new(msg);
        match self.address_map.lookup(&addr) {
            Some(wid) => self.transfer_txs[wid.as_usize()]
                .try_send(Envelope::new(addr, msg_box))
                .map_err(|_| Error::from("Transfer queue full")),
            None => self.inbox_registry.try_deliver(addr, msg_box),
        }
    }

    /// Create an external inbox for receiving messages in the outer process containing the runtime
    pub fn new_inbox<M: Message>(&self) -> Result<Inbox<M>, Error> {
        let addr = ActorAddress::new_random();
        let receiver = Receiver::<M>::new(self.config.actor_max_messages);
        let sender = receiver.new_sender();
        self.inbox_registry.register(addr, Arc::new(sender));
        Ok(Inbox {
            addr,
            inner: receiver,
        })
    }

    /// Drive one tick of the single-threaded worker.
    pub fn tick(&self) {
        if let Some(ref worker) = self.single_worker {
            let tc = TickContext {
                address_map: &self.address_map,
                transfer_txs: &self.transfer_txs,
                spawn_txs: &self.spawn_txs,
                placement: &self.placement,
                inbox_registry: &self.inbox_registry,
                config: &self.config,
            };
            worker.borrow_mut().tick_once(&tc);
        }
    }

    /// Spawn worker threads and start processing, returning a handle
    /// to interact with the runtime and join the threads later.
    ///
    /// Works in both single-threaded and multi-threaded configurations.
    /// In single-threaded mode, one background thread is spawned.
    pub fn run(mut self) -> Result<RuntimeHandle, Error> {
        self.is_running.store(true, Ordering::Release);

        let mut workers: Vec<Worker> = Vec::new();

        if let Some(w) = self.single_worker.take() {
            workers.push(w.into_inner());
        }
        if let Some(ws) = self.pending_workers.take() {
            workers.extend(ws);
        }

        let rt = Arc::new(self);
        let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(workers.len());

        for mut worker in workers {
            let rt_clone = rt.clone();
            let handle = thread::spawn(move || {
                let tc = TickContext {
                    address_map: &rt_clone.address_map,
                    transfer_txs: &rt_clone.transfer_txs,
                    spawn_txs: &rt_clone.spawn_txs,
                    placement: &rt_clone.placement,
                    inbox_registry: &rt_clone.inbox_registry,
                    config: &rt_clone.config,
                };
                worker.run(&tc, &rt_clone.is_running, &rt_clone.config.backoff_policy);
            });
            handles.push(handle);
        }

        Ok(RuntimeHandle {
            runtime: rt,
            threads: handles,
        })
    }

    /// Signal all workers to stop
    pub fn shutdown(&self) {
        self.is_running.store(false, Ordering::Release);
    }
}



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

    pub fn downcast<M: 'static>(self) -> Option<M> {
        self.payload.downcast::<M>().ok().map(|b| *b)
    }

    pub fn into_payload(self) -> Box<dyn Any + Send> {
        self.payload
    }
}


// ─── InboxRegistry ───────────────────────────────────────────────────────────

/// Registry of external inboxes — replaces the Router's role for non-actor receivers.
pub(crate) struct InboxRegistry {
    senders: RwLock<HashMap<ActorAddress, Arc<dyn SenderT>>>,
}

impl InboxRegistry {
    pub fn new() -> Self {
        Self {
            senders: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, addr: ActorAddress, sender: Arc<dyn SenderT>) {
        self.senders.write().unwrap().insert(addr, sender);
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




/// Object-safe inner trait for sending type-erased messages.
pub(crate) trait ContextInner {
    fn send_any(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error>;
    fn spawn_any(&self, addr: ActorAddress, actor: Box<dyn AnyActor>) -> Result<(), Error>;
    fn mailbox_waterlevel(&self) -> usize;
}


impl ContextInner for Runtime {
    fn send_any(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error> {
        match self.address_map.lookup(&addr) {
            Some(wid) => {
                let _ = self.transfer_txs[wid.as_usize()].try_send(Envelope::new(addr, msg));
                Ok(())
            }
            None => self.inbox_registry.try_deliver(addr, msg),
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
