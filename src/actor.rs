use std::any::Any;

use crate::Error;

/// Why an actor exited.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ExitReason {
    /// Actor was explicitly stopped or removed from the pool.
    Stopped,
    /// Actor panicked during message handling.
    Panicked,
    /// The node hosting the actor left the cluster (SWIM Dead).
    NodeDown,
}

/// Delivered to watchers when a watched actor exits.
///
/// Implements `Message` (Clone + Send + Sync + 'static) so it can be
/// delivered through normal mailbox channels.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActorExited {
    /// The address of the actor that died.
    pub addr: ActorAddress,
    /// Why it exited.
    pub reason: ExitReason,
}

/// The primary trait defining data that can be passed to and from actor processes
pub trait Message: 'static + Sized + Clone + Send + Sync {}
impl<T: 'static + Sized + Clone + Send + Sync> Message for T {}

pub trait ActorInterface: 'static + Send {
    type Incoming: Message;
    type Response: Message;
    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming);

    /// Called once after the actor is added to a worker, before the first message.
    /// Receives `&Ctx` so the actor can send messages or spawn children during init.
    ///
    /// If `on_start` panics, the actor is immediately poisoned (no restart attempted).
    fn on_start(&mut self, _ctx: &Ctx) {}

    /// Called when the actor is being gracefully stopped (via `ctx.stop_self()` or
    /// `Runtime::stop_actor()`), before removal from the worker pool.
    ///
    /// NOT called when an actor is poisoned by panic — panicked actors may have
    /// corrupt state and calling methods on them is unsafe.
    fn on_stop(&mut self, _ctx: &Ctx) {}

    /// Called when a monitored actor dies (via [`Ctx::monitor`]).
    ///
    /// Override this to react to death notifications without making [`Down`]
    /// your `Incoming` type. Default: no-op (the `Down` message is silently consumed).
    ///
    /// If your `Incoming` type IS `Down`, this method is never called — the
    /// normal `handle()` receives the message instead.
    fn handle_down(&mut self, _ctx: &Ctx, _down: Down) {}

    /// Called when a watched actor exits. Override to react to death notifications.
    ///
    /// Default: no-op (notification is silently consumed).
    fn on_actor_exit(&mut self, _ctx: &Ctx, _exited: ActorExited) {}
}

/// A unique address for this actor. 32 bytes is overkill for a small application,
/// but most systems are powerful, and this allows us to create a global map of
/// actor processes in the future, without worrying about collision.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActorAddress(pub [u8; 32]);

/// Custom Hash: only hash the first 8 bytes since all 32 are random.
/// SipHash on 8 bytes is ~3x faster than on 32 bytes, with identical
/// collision properties (2^64 possible values from cryptographic randomness).
impl std::hash::Hash for ActorAddress {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // SAFETY: ActorAddress is always 32 bytes, so [..8] is valid.
        state.write_u64(u64::from_ne_bytes(
            self.0[..8].try_into().unwrap(),
        ));
    }
}

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
pub struct Actor<A: ActorInterface> {
    inner: A,
}

impl<A: ActorInterface> Actor<A> {
    pub fn new(inner: A) -> Self {
        Self { inner }
    }
}

/// Trait for type-erased actors — single-message handler.
///
/// Returns `Some(type_name)` if handled, `None` on type mismatch.
pub trait AnyActor: Send {
    fn handle_any(&mut self, ctx: &Ctx, msg: Box<dyn Any + Send>) -> Option<&'static str>;

    /// Called once after spawn, before first message. See [`ActorInterface::on_start`].
    fn on_start(&mut self, _ctx: &Ctx) {}

    /// Called on graceful stop, before removal. See [`ActorInterface::on_stop`].
    fn on_stop(&mut self, _ctx: &Ctx) {}
}

impl<A> AnyActor for Actor<A>
where
    A: ActorInterface,
{
    fn handle_any(&mut self, ctx: &Ctx, msg: Box<dyn Any + Send>) -> Option<&'static str> {
        let msg = match msg.downcast::<A::Incoming>() {
            Ok(typed) => {
                self.inner.handle(ctx, *typed);
                return Some(std::any::type_name::<A::Incoming>());
            }
            Err(msg) => msg,
        };
        let msg = match msg.downcast::<Down>() {
            Ok(down) => {
                self.inner.handle_down(ctx, *down);
                return Some("swactor::actor::Down");
            }
            Err(msg) => msg,
        };
        match msg.downcast::<ActorExited>() {
            Ok(exited) => {
                self.inner.on_actor_exit(ctx, *exited);
                Some("ActorExited")
            }
            Err(_) => None,
        }
    }

    fn on_start(&mut self, ctx: &Ctx) {
        self.inner.on_start(ctx);
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        self.inner.on_stop(ctx);
    }
}

/// Unique token identifying a monitor subscription.
///
/// Returned by [`Ctx::monitor`] and used with [`Ctx::demonitor`] to cancel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MonitorRef(pub(crate) u64);

impl MonitorRef {
    /// Construct a MonitorRef from a raw id. Used by extension crates.
    pub fn from_raw(id: u64) -> Self {
        Self(id)
    }
}

/// Reason an actor was removed from the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StopReason {
    /// Graceful stop (via `ctx.stop_self()` or `Runtime::stop_actor()`).
    Normal,
    /// Actor panicked and could not be restarted.
    Panicked,
}

/// Death notification delivered as a normal message when a monitored actor dies.
///
/// Subscribe via [`Ctx::monitor`]. The `Down` message arrives in the watcher's
/// regular `handle()` method — no special callback needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Down {
    /// Address of the dead actor.
    pub addr: ActorAddress,
    /// Why it died.
    pub reason: StopReason,
}

/// Internal sentinel message for graceful actor stop.
/// Not a `Message` — intercepted in `tick_all` before reaching `handle_any`.
pub(crate) struct StopSignal;

/// Object-safe inner trait for sending type-erased messages.
///
/// Minimal core interface: send, spawn, stop, and extension access.
/// Registry methods (naming, monitoring, groups) and timer scheduling
/// are provided by extension traits in `swactor-std`.
#[allow(private_interfaces)]
pub trait ContextInner {
    fn send_any(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error>;
    fn spawn_any(&self, addr: ActorAddress, actor: Box<dyn AnyActor>);
    /// Request graceful stop for an actor. Takes effect after the current message.
    fn request_stop(&self, addr: ActorAddress);
    /// Post a request to the per-worker extension (e.g., timer scheduling).
    fn post_worker_request(&self, request: Box<dyn Any + Send>);
    /// Access the runtime extension (if installed).
    fn extension(&self) -> Option<&dyn crate::extension::RuntimeExtension>;
}

/// Actor syscall interface — passed to `ActorInterface::handle()`.
///
/// Wraps a `&dyn ContextInner` to solve the object-safety problem while
/// providing a typed public API. Registry methods (naming, monitoring, groups)
/// are provided by extension traits in `swactor-std`.
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

    /// Access the runtime extension (if installed).
    pub fn extension(&self) -> Option<&dyn crate::extension::RuntimeExtension> {
        self.inner.extension()
    }

    /// Send a typed message to an actor address.
    pub fn send<M: Message>(&self, addr: ActorAddress, msg: M) -> Result<(), Error> {
        self.inner.send_any(addr, Box::new(msg))
    }

    /// Spawn a new actor, returning its address.
    pub fn spawn<A: ActorInterface>(&self, actor: A) -> Result<ActorAddress, Error> {
        let addr = ActorAddress::new_random();
        let boxed: Box<dyn AnyActor> = Box::new(Actor::new(actor));
        self.inner.spawn_any(addr, boxed);
        Ok(addr)
    }

    /// Request graceful stop for this actor after the current message completes.
    ///
    /// The actor's `on_stop()` hook is called and the actor is removed from the
    /// worker pool. Pending messages in the mailbox are discarded.
    pub fn stop_self(&self) {
        self.inner.request_stop(self.self_addr);
    }

    /// Send a graceful stop request to another actor.
    ///
    /// The target actor will process any messages already in its mailbox before
    /// the stop signal, then its `on_stop()` hook is called and it is removed.
    /// Uses PoisonPill semantics — queued after existing messages.
    pub fn stop_actor(&self, addr: ActorAddress) -> Result<(), Error> {
        self.inner.send_any(addr, Box::new(StopSignal))
    }

}
