use crate::{runtime::Runtime, WATERLEVEL, get_random, ring_buffer::Receiver};

/// The primary trait defining data that can be passed to and from actor processes
pub trait Message: 'static + Sized + Clone + Send + Sync {}
impl<T: 'static + Sized + Clone + Send + Sync> Message for T {}

/// The trait that needs to be implemented in order to run a process as an `Actor`
/// 
/// The `Incoming` type represents `Messages` that can be delivered to the `Actor`.
/// 
/// The `Response` type represents possible `Messages` the actor may attempt to reply with.
/// 
/// The `fn handle(..)` is where you implement the logic for handling `Incoming` messages
/// 
/// # Example
/// ```
/// use swactor::{actor::{ActorAddress, ActorInterface}, runtime::Runtime};
/// 
/// struct Greeter {
///     num_greeted: usize,
/// }
/// 
/// #[derive(Clone)] // required to auto implement `Message`
/// struct GreetMessage {
///     who: String,
///     return_addr: ActorAddress,
/// }
/// 
/// #[derive(Clone)]
/// struct GreetResponse(String);
/// 
/// impl ActorInterface for Greeter {
///     type Incoming = GreetMessage;
///     type Response = GreetResponse;
///     
///     fn handle(&mut self, ctx: &Runtime, msg: Self::Incoming) {
///         let response = GreetResponse(format!("Hello, {}!", msg.who).to_string());
///         if let Ok(_) = ctx.send_to(msg.return_addr, response) {
///             self.num_greeted += 1;
///         }
///     }
/// }
/// ```
pub trait ActorInterface: 'static + Send {
    type Incoming: Message;
    type Response: Message;
    fn handle(&mut self, ctx: &Runtime, msg: Self::Incoming);
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

/// The actor process as represented in the Runtime, with the actor state stored with it's inbox.
pub(crate) struct Actor<A>
where
    A: ActorInterface,
{
    inbox: Receiver<A::Incoming>,
    inner: A,
}

impl<A: ActorInterface> Actor<A> {
    pub(crate) fn new(inbox: Receiver<A::Incoming>, inner: A) -> Self {
        Self {
            inbox,
            inner,
        }
    }
}

/// Trait for type-erased actors
pub(crate) trait AnyActor: Send {
    fn tick(&mut self, ctx: &Runtime);
}

impl<A> AnyActor for Actor<A>
where
    A: ActorInterface,
{
    fn tick(&mut self, ctx: &Runtime) {
        // TODO: WATERLEVEL is hard coded, and so is this message handling scheme. We should
        // make it so both are more flexible, with sane defaults.
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
