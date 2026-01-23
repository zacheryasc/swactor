mod ring_buffer;

use std::collections::HashMap;

use crossbeam_queue::ArrayQueue;
use ring_buffer::{Receiver, Sender};

pub mod error;
use error::Error;

#[cfg(feature = "getrandom")]
pub fn get_random(buf: &mut [u8]) {
    getrandom::getrandom(buf).unwrap()
}

/// The strategy for message processing is such:
/// if total_messages < WATERLEVEL:
///     process all
/// else
///     process total_messages // 2
const WATERLEVEL: usize = 10;

const DEFAULT_INBOX_CAPACITY: usize = 100;

pub trait Message: 'static + Sized + Clone + Send {}
pub type Envelope = Box<dyn std::any::Any + Send>;

pub trait ActorInterface: 'static + Send {
    type Incoming: Message;
    type Response: Message;
    fn handle(&mut self, ctx: &Runtime, msg: Self::Incoming);
}

pub type ActorAddress = u64;

pub struct Actor<A>
where
    A: ActorInterface,
{
    _addr: ActorAddress,
    inbox: Receiver<A::Incoming>,
    inner: A,
}

/// Trait for type-erased actors
trait AnyActor: Send {
    fn tick(&mut self, ctx: &Runtime);
}

impl<A> AnyActor for Actor<A>
where
    A: ActorInterface,
{
    fn tick(&mut self, ctx: &Runtime) {
        let total_messages = self.inbox.len();
        let messages_to_process = if total_messages < WATERLEVEL {
            total_messages
        } else {
            total_messages >> 1
        };

        for _ in 0..messages_to_process {
            match self.inbox.try_recv() {
                Some(msg) => self.inner.handle(ctx, msg),
                None => unreachable!(
                    "We checked number of unprocessed messages in the queue ahead of processing"
                ),
            }
        }
    }
}

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

#[derive(Debug, Default)]
pub enum RuntimeFlavor {
    #[default]
    SingleThreaded,
    Multithreaded(usize),
}

pub struct Runtime {
    flavor: RuntimeFlavor,
    router: Router,
    router_inbox: Sender<RouterMessage>,
    actor_queue: ArrayQueue<Box<dyn AnyActor>>,
}

impl Runtime {
    pub fn new(capacity: usize, flavor: Option<RuntimeFlavor>) -> Self {
        let router = Router::new(DEFAULT_INBOX_CAPACITY);
        let router_inbox = router.new_sender();
        Self {
            flavor: flavor.unwrap_or_default(),
            router,
            router_inbox,
            actor_queue: ArrayQueue::new(capacity),
        }
    }

    pub fn spawn<A: ActorInterface>(&self, actor: A) -> Result<ActorAddress, Error> {
        let addr = {
            let mut bytes = u64::to_le_bytes(0);
            get_random(&mut bytes);
            u64::from_le_bytes(bytes)
        };
        let inbox = Receiver::<A::Incoming>::new(DEFAULT_INBOX_CAPACITY);
        let sender = inbox.new_sender();

        // Register the sender with the router
        let _ = self
            .router_inbox
            .try_send(RouterMessage::AddAddr(addr, Box::new(sender)));

        self.actor_queue
            .push(Box::new(Actor {
                _addr: addr,
                inbox,
                inner: actor,
            }))
            .map_err(|_| Error::from("Runtime error: Failed to spawn actor."))?;

        Ok(addr)
    }

    pub fn send_to<M: Message>(&self, addr: ActorAddress, msg: M) -> Result<(), ()> {
        let envelope: Envelope = Box::new(msg);
        self.router_inbox
            .try_send(RouterMessage::SendToAddr {
                addr,
                msg: envelope,
            })
            .map_err(|_| ())
    }

    pub fn tick(&mut self) {
        // Pop actor, tick it, push it back
        if let Some(mut actor) = self.actor_queue.pop() {
            actor.tick(self);
            let _ = self.actor_queue.push(actor);
        }

        match self.flavor {
            RuntimeFlavor::Multithreaded(_) => (), // router has its own thread
            RuntimeFlavor::SingleThreaded => self.router.tick(),
        }
    }

    pub fn new_inbox<M: Message>(&self) -> Inbox<M> {
        let addr = {
            let mut bytes = u64::to_le_bytes(0);
            get_random(&mut bytes);
            u64::from_le_bytes(bytes)
        };
        let receiver = Receiver::<M>::new(DEFAULT_INBOX_CAPACITY);
        let sender = receiver.new_sender();
        // Register the sender with the router
        let _ = self
            .router_inbox
            .try_send(RouterMessage::AddAddr(addr, Box::new(sender)));
        Inbox {
            addr,
            inner: receiver,
        }
    }
}

pub trait SenderT: Send {
    fn try_send(&self, envelope: Envelope);
}

impl<M: Message> SenderT for Sender<M> {
    fn try_send(&self, envelope: Envelope) {
        if let Ok(msg) = envelope.downcast::<M>() {
            let _ = Sender::try_send(self, *msg);
        }
    }
}

/// Internal messages for the Router's own inbox
pub enum RouterMessage {
    /// register addrs <addr> with sender <sender>
    AddAddr(ActorAddress, Box<dyn SenderT>),
    /// remove an actor from the address book
    RemoveAddr(ActorAddress),
    /// send <msg> to <addr>
    SendToAddr { addr: ActorAddress, msg: Envelope },
}

struct Router {
    directory: HashMap<ActorAddress, Box<dyn SenderT>>,
    inbox: Receiver<RouterMessage>,
}

impl Router {
    pub fn new(cap: usize) -> Self {
        Self {
            directory: HashMap::new(),
            inbox: Receiver::new(cap),
        }
    }

    pub fn tick(&mut self) {
        let total_messages = self.inbox.len();
        let messages_to_process = if total_messages < WATERLEVEL {
            total_messages
        } else {
            total_messages >> 1
        };

        for _ in 0..messages_to_process {
            match self.inbox.try_recv() {
                Some(msg) => self.handle(msg),
                None => unreachable!("We ran checks on total messages before processing."),
            }
        }
    }

    pub fn new_sender(&self) -> Sender<RouterMessage> {
        self.inbox.new_sender()
    }

    fn handle(&mut self, msg: RouterMessage) {
        match msg {
            RouterMessage::AddAddr(addr, sender) => {
                self.directory.insert(addr, sender);
            }
            RouterMessage::RemoveAddr(addr) => {
                self.directory.remove(&addr);
            }
            RouterMessage::SendToAddr { addr, msg } => {
                if let Some(sender) = self.directory.get(&addr) {
                    sender.try_send(msg);
                }
            }
        }
    }
}
