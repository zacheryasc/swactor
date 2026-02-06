use std::any::Any;

use crate::runtime::Ctx;

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
        crate::get_random(&mut bytes);
        Self(bytes)
    }
}

/// The actor process as represented in the Runtime — thin wrapper around user state.
pub(crate) struct Actor<A: ActorInterface>(A);

impl<A: ActorInterface> Actor<A> {
    pub(crate) fn new(inner: A) -> Self {
        Self(inner)
    }
}

/// Trait for type-erased actors — single-message handler.
pub(crate) trait AnyActor: Send {
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
