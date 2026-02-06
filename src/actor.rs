use std::any::Any;

use crate::{get_random, runtime::{ContextInner, Ctx}, worker::Mailbox};

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
pub struct ActorAddress(pub [u8; 32]);
impl ActorAddress {
    pub fn new_random() -> Self {
        let mut bytes = [0u8; 32];
        get_random(&mut bytes);
        Self(bytes)
    }
}

/// The actor process as represented in the Runtime, with the actor state stored with its mailbox.
pub(crate) struct Actor<A>
where
    A: ActorInterface,
{
    addr: ActorAddress,
    mailbox: Mailbox<A::Incoming>,
    inner: A,
}

impl<A: ActorInterface> Actor<A> {
    pub(crate) fn new(addr: ActorAddress, mailbox: Mailbox<A::Incoming>, inner: A) -> Self {
        Self {
            addr,
            mailbox,
            inner,
        }
    }
}

/// Trait for type-erased actors
pub(crate) trait AnyActor: Send {
    /// Tick the actor, processing pending messages. Returns `true` if any work was done.
    fn tick(&mut self, inner: &dyn ContextInner) -> bool;
    /// Deliver a type-erased message into this actor's mailbox.
    /// Returns `true` if the downcast succeeded.
    fn deliver(&mut self, msg: Box<dyn Any + Send>) -> bool;
}

impl<A> AnyActor for Actor<A>
where
    A: ActorInterface,
{
    fn tick(&mut self, inner: &dyn ContextInner) -> bool {
        let n = self.mailbox.drain_count();
        if n > 0 {
            let ctx = Ctx::new(inner, self.addr);
            for _ in 0..n {
                if let Some(msg) = self.mailbox.pop() {
                    self.inner.handle(&ctx, msg);
                }
            }
        }
        n > 0
    }

    fn deliver(&mut self, msg: Box<dyn Any + Send>) -> bool {
        if let Ok(typed) = msg.downcast::<A::Incoming>() {
            self.mailbox.push(*typed);
            true
        } else {
            false
        }
    }
}
