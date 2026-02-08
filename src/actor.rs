use std::any::Any;

use crate::Error;

/// The primary trait defining data that can be passed to and from actor processes
pub trait Message: 'static + Sized + Clone + Send + Sync {}
impl<T: 'static + Sized + Clone + Send + Sync> Message for T {}

pub trait ActorInterface: 'static + Send {
    type Incoming: Message;
    type Response: Message;
    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming);
}

/// A unique address for this actor. 32 bytes is overkill for a small application,
/// but most systems are powerful, and this allows us to create a global map of
/// actor processes in the future, without worrying about collision.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActorAddress(pub [u8; 32]);

impl std::fmt::Display for ActorAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in &self.0[..8] {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "\u{2026}")
    }
}
impl ActorAddress {
    pub fn new_random() -> Self {
        let mut bytes = [0u8; 32];
        crate::get_random(&mut bytes);
        Self(bytes)
    }
}

/// The actor process as represented in the Runtime — thin wrapper around user state.
pub struct Actor<A: ActorInterface>(A);

impl<A: ActorInterface> Actor<A> {
    pub fn new(inner: A) -> Self {
        Self(inner)
    }
}

/// Trait for type-erased actors — single-message handler.
pub trait AnyActor: Send {
    fn handle_any(&mut self, ctx: &Ctx, msg: Box<dyn Any + Send>);
}

impl<A> AnyActor for Actor<A>
where
    A: ActorInterface,
{
    fn handle_any(&mut self, ctx: &Ctx, msg: Box<dyn Any + Send>) {
        if let Ok(typed) = msg.downcast::<A::Incoming>() {
            self.0.handle(ctx, *typed);
        }
    }
}

/// Object-safe inner trait for sending type-erased messages.
pub trait ContextInner {
    fn send_any(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error>;
    fn spawn_any(&self, addr: ActorAddress, actor: Box<dyn AnyActor>) -> Result<(), Error>;
    fn mailbox_waterlevel(&self) -> usize;
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

    pub fn raw_inner(&self) -> &dyn ContextInner {
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
        let boxed: Box<dyn AnyActor> = Box::new(Actor::new(actor));
        self.inner.spawn_any(addr, boxed)?;
        Ok(addr)
    }
}
