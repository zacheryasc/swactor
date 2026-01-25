use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use crossbeam_queue::ArrayQueue;

use crate::{
    Error,
    actor::{Actor, ActorAddress, ActorInterface, AnyActor, Message},
    ring_buffer::{Receiver, Sender},
    router::{Router, RouterMessage},
};

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

/// The tunable settings for the runtime.
pub struct RuntimeConfig {
    pub max_actors: usize,
    pub router_max_messages: usize,
    pub actor_max_messages: usize,
    pub num_threads: usize,
}

/// 8kB for the `Box<..>` before counting the rest of the memory
const DEFAULT_MAX_ACTORS: usize = 1_000;

/// 160kB for the `Arc<..>` before counting the rest of the memory
const DEFAULT_ROUTER_MAX_MESSAGES: usize = 10_000;

/// 16kB PER ACTOR to alloc space for storing the `Arc<..>` pointers
/// With default setting of [DEFAULT_MAX_ACTORS] this is:
/// 1_000 * 16kB = 16MB
const DEFAULT_ACTOR_MAX_MESSAGES: usize = 1_000;

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_actors: DEFAULT_MAX_ACTORS,
            router_max_messages: DEFAULT_ROUTER_MAX_MESSAGES,
            actor_max_messages: DEFAULT_ACTOR_MAX_MESSAGES,
            num_threads: 1,
        }
    }
}

/// The `Runtime` struct is the primary gateway for interacting with the framework.
pub struct Runtime {
    config: RuntimeConfig,
    actor_queue: ArrayQueue<Box<dyn AnyActor>>,
    router_interface: Sender<RouterMessage>,
    router: Option<Actor<Router>>, // `None` if single-threaded

    // for multithreaded contexts
    is_running: AtomicBool,
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

impl Runtime {
    /// Builds a new `Runtime` struct, but does not yet run anything. If multithreaded, call
    /// `run()`, if single threaded, needs to be driven by calls to the `tick()` method.
    pub fn new(config: RuntimeConfig) -> Self {
        let actor_queue = ArrayQueue::new(config.max_actors);

        // router is a unique actor in that the runtime needs access to it's `Sender` handle
        let router_inner = Router::new();
        let router_inbox: Receiver<RouterMessage> =
            Receiver::<<Router as ActorInterface>::Incoming>::new(config.router_max_messages);
        let router_sender = router_inbox.new_sender();
        let router = Actor::new(router_inbox, router_inner);

        // Single-threaded: router goes in queue. Multi-threaded: stays in Option
        let router_option = if config.num_threads < 2 {
            actor_queue
                .push(Box::new(router) as Box<dyn AnyActor>)
                .map_err(|_| "failed to add router to actor queue")
                .expect("failed to spawn router at runtime initialization.");
            None
        } else {
            Some(router)
        };

        Self {
            config,
            actor_queue,
            router_interface: router_sender,
            is_running: AtomicBool::new(false),
            router: router_option,
        }
    }

    /// Spawn an actor, returns its address
    pub fn spawn<A: ActorInterface>(&self, actor: A) -> Result<ActorAddress, Error> {
        // assign a stochastic
        let addr = ActorAddress::new_random();
        let inbox = Receiver::<A::Incoming>::new(self.config.actor_max_messages);
        let sender = inbox.new_sender();

        // Register the sender with the router
        self.router_interface
            .try_send(RouterMessage::AddAddr(addr, Arc::new(sender)))
            .map_err(|_| {
                Error::from("Runtime error: failed to add actor to router. Router inbox full")
            })?;

        self.actor_queue
            .push(Box::new(Actor::new(inbox, actor)))
            .map_err(|_| Error::from("Runtime error: Failed to spawn actor. Queue full."))?;

        Ok(addr)
    }

    /// Send a message to an actor address
    pub fn send_to<M: Message>(&self, addr: ActorAddress, msg: M) -> Result<(), Error> {
        self.router_interface
            .try_send(RouterMessage::SendToAddr {
                addr,
                msg: Arc::new(msg),
            })
            .map_err(|_| Error::from("Failed to send message to router."))
    }

    /// Create an external inbox for receiving messages in the outer process containing the runtime
    pub fn new_inbox<M: Message>(&self) -> Result<Inbox<M>, Error> {
        let addr = ActorAddress::new_random();

        let receiver = Receiver::<M>::new(self.config.actor_max_messages);
        let sender = receiver.new_sender();

        // Register the sender with the router
        self.router_interface
            .try_send(RouterMessage::AddAddr(addr, Arc::new(sender)))
            .map_err(|_| {
                Error::from(
                    "Runtime error: failed to add a new inbox channel. Router inbox is full.",
                )
            })?;

        Ok(Inbox {
            addr,
            inner: receiver,
        })
    }

    /// Spawn worker threads and start processing, returning a set of handles and
    /// a Runtime object to interface with.
    ///
    /// ### WARN: 
    /// ##### This function panics if the configuration is set as single threaded
    /// `config.num_threads == 1`
    pub fn run(mut self) -> Result<RuntimeHandle, Error> {
        if self.config.num_threads < 2 {
            return Err(Error::from(
                "Runtime error: cannot call `Runtime::run()` from a single-threaded context.",
            ));
        }

        self.is_running.store(true, Ordering::Release);

        // Take router out before wrapping in Arc - it will be owned by router thread
        let mut router = self
            .router
            .take()
            .expect("Router must be present for multi-threaded runtime");

        let rt = Arc::new(self);
        let mut handles: Vec<JoinHandle<()>> = vec![];

        // Router thread owns the router directly - no synchronization needed
        let router_handle = {
            let ctx = rt.clone();
            thread::spawn(move || {
                while ctx.is_running.load(Ordering::Acquire) {
                    router.tick(&ctx);
                    thread::yield_now();
                }
            })
        };
        handles.push(router_handle);

        // Spawn worker threads
        let num_workers = rt.config.num_threads - 1;
        for _ in 0..num_workers {
            let ctx = rt.clone();
            let handle = thread::spawn(move || {
                while ctx.is_running.load(Ordering::Acquire) {
                    if let Some(mut actor) = ctx.actor_queue.pop() {
                        actor.tick(&ctx);
                        if let Err(_) = ctx.actor_queue.push(actor) {
                            panic!(
                                "Runtime panic: attempted to return an actor to the queue, but queue was full."
                            )
                        }
                    } else {
                        thread::yield_now();
                    }
                }
            });
            handles.push(handle);
        }

        Ok(RuntimeHandle {
            runtime: rt,
            threads: handles,
        })
    }

    /// Pop the actor off the top of the queue and process it's messages, returning it to the back of
    /// the queue upon completion.
    pub fn tick(&self) {
        if let Some(mut actor) = self.actor_queue.pop() {
            actor.tick(&self);
            let _ = self.actor_queue.push(actor);
        }
    }

    /// Signal all workers to stop
    pub fn shutdown(&self) {
        self.is_running.store(false, Ordering::Release);
    }
}
