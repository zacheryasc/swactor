use std::any::Any;
use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::Error;

// ─── ExitValue ──────────────────────────────────────────────────────────────

/// Opaque typed value attached to a completed actor's exit.
///
/// Created via [`Ctx::stop_with`]. Delivered to monitors/watchers in
/// [`Down::exit_value`] and [`ActorExited::exit_value`].
///
/// Clone is an `Arc` bump (zero allocation).
#[derive(Clone)]
pub struct ExitValue(Arc<dyn Any + Send + Sync>);

impl ExitValue {
    /// Wrap a typed value as an opaque exit value.
    pub fn new<T: Any + Send + Sync>(value: T) -> Self {
        Self(Arc::new(value))
    }

    /// Attempt to downcast to a concrete type by reference.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.0.downcast_ref::<T>()
    }
}

impl PartialEq for ExitValue {
    fn eq(&self, _other: &Self) -> bool {
        false // opaque blob — always not equal
    }
}

impl Eq for ExitValue {}

impl std::fmt::Debug for ExitValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ExitValue(..)")
    }
}

/// System-level information visible to actors.
#[derive(Debug, Clone)]
pub struct SystemInfo {
    /// Index of the worker thread this actor is running on.
    pub worker_id: usize,
    /// Total number of worker threads in the runtime.
    pub num_workers: usize,
    /// Total number of live actors across all workers.
    pub total_actors: usize,
    /// Milliseconds since the runtime was created.
    pub uptime_ms: u64,
}

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
    /// Actor stopped with a typed exit value (via [`Ctx::stop_with`]).
    Completed,
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
    /// Typed exit value if the actor called [`Ctx::stop_with`].
    #[cfg_attr(feature = "serde", serde(skip))]
    pub exit_value: Option<ExitValue>,
}

impl PartialEq for ActorExited {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr && self.reason == other.reason
    }
}

impl Eq for ActorExited {}

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
        state.write_u64(u64::from_ne_bytes(self.0[..8].try_into().unwrap()));
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

    /// Full 64-character hex encoding of all 32 bytes, for displays that need
    /// the untruncated address (dashboards). [`Display`](std::fmt::Display)
    /// stays short for logs.
    pub fn to_full_hex(&self) -> String {
        use std::fmt::Write;
        let mut s = String::with_capacity(64);
        for b in &self.0 {
            let _ = write!(s, "{:02x}", b);
        }
        s
    }
}

// ─── Environment ─────────────────────────────────────────────────────────────

/// A typed key-value map that flows from parent to child at spawn time.
///
/// Analogous to Unix `environ` — provides inherited configuration without
/// threading values through every constructor. Clone is an Arc bump (zero allocation).
///
/// Values are stored as `Arc<dyn Any>` so that [`EnvironmentBuilder::from_env`]
/// can clone individual entries cheaply (Arc bump) for copy-on-write overrides.
#[derive(Clone, Default)]
pub struct Environment {
    inner: Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl Environment {
    /// Create an empty environment.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HashMap::new()),
        }
    }

    /// Read a typed value from the environment.
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.inner
            .get(&TypeId::of::<T>())
            .and_then(|v| v.downcast_ref::<T>())
    }

    /// Check if the environment contains a value of type `T`.
    pub fn contains<T: Any + Send + Sync>(&self) -> bool {
        self.inner.contains_key(&TypeId::of::<T>())
    }

    /// Returns `true` if the environment has no values.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Number of typed values in the environment.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if the environment contains a value with the given `TypeId`.
    ///
    /// Type-erased version of [`contains`](Self::contains) for extension code
    /// that merges pre-built values without knowing their concrete types.
    pub fn contains_type_id(&self, type_id: TypeId) -> bool {
        self.inner.contains_key(&type_id)
    }
}

/// Builder for constructing an [`Environment`].
///
/// Allows inserting/replacing typed values before freezing into an immutable `Environment`.
pub struct EnvironmentBuilder {
    map: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl EnvironmentBuilder {
    /// Create an empty builder.
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Create a builder pre-populated with values from an existing environment.
    ///
    /// This enables copy-on-write overrides: clone the parent's map, modify, then freeze.
    /// Cloning entries is cheap — each value is `Arc`-wrapped.
    pub fn from_env(env: &Environment) -> Self {
        let map = env.inner.as_ref().clone();
        Self { map }
    }

    /// Insert or replace a typed value.
    pub fn set<T: Any + Send + Sync>(mut self, value: T) -> Self {
        self.map.insert(TypeId::of::<T>(), Arc::new(value));
        self
    }

    /// Insert or replace a typed value (mutable reference version).
    pub fn set_mut<T: Any + Send + Sync>(&mut self, value: T) -> &mut Self {
        self.map.insert(TypeId::of::<T>(), Arc::new(value));
        self
    }

    /// Insert a type-erased value by `TypeId`.
    ///
    /// Used by extension code to merge pre-built values without knowing concrete
    /// types at compile time.
    pub fn set_raw(&mut self, type_id: TypeId, value: Arc<dyn Any + Send + Sync>) -> &mut Self {
        self.map.insert(type_id, value);
        self
    }

    /// Freeze the builder into an immutable `Environment`.
    pub fn build(self) -> Environment {
        Environment {
            inner: Arc::new(self.map),
        }
    }
}

impl Default for EnvironmentBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Well-Known Environment Keys ─────────────────────────────────────────────

/// Milliseconds since runtime creation when this actor was spawned.
///
/// Reserved environment key for custom extensions that want a spawn timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SpawnTimestamp(pub u64);

/// Logical name assigned to an actor.
///
/// Reserved environment key for custom naming extensions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalName(pub String);

impl LogicalName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A typed service binding stored in the environment.
///
/// Reserved environment value for custom resource/service extensions. `S` is a
/// zero-sized marker type that identifies the service (e.g., `struct Datastore;`).
#[derive(Clone, Debug)]
pub struct ServiceBinding<S: 'static + Send + Sync> {
    pub addr: ActorAddress,
    _marker: std::marker::PhantomData<S>,
}

impl<S: 'static + Send + Sync> ServiceBinding<S> {
    pub fn new(addr: ActorAddress) -> Self {
        Self {
            addr,
            _marker: std::marker::PhantomData,
        }
    }
}

// ─── Capabilities ────────────────────────────────────────────────────────────

/// Per-actor capability set controlling what operations the actor can perform.
///
/// When present in an actor's [`Environment`], enforcement is active — the actor
/// can only perform operations granted by the set. When absent, the actor is
/// unrestricted (backward compatible). Inherits from parent to child via normal
/// environment inheritance.
///
/// Built via fluent API: `CapabilitySet::new().with_send(addr).with_spawn()`.
#[derive(Clone, Default)]
pub struct CapabilitySet {
    send_any: HashSet<ActorAddress, crate::delivery::AddrBuildHasher>,
    send_typed: HashSet<(TypeId, ActorAddress)>,
    can_spawn: bool,
    service_types: HashSet<TypeId>,
    monitor_targets: HashSet<ActorAddress, crate::delivery::AddrBuildHasher>,
}

impl CapabilitySet {
    pub fn new() -> Self {
        Self::default()
    }

    // ── Builder methods (fluent) ──
    pub fn with_send(mut self, addr: ActorAddress) -> Self {
        self.send_any.insert(addr);
        self
    }
    pub fn with_send_typed<M: Message>(mut self, addr: ActorAddress) -> Self {
        self.send_typed.insert((TypeId::of::<M>(), addr));
        self
    }
    pub fn with_spawn(mut self) -> Self {
        self.can_spawn = true;
        self
    }
    pub fn with_service<S: 'static + Send + Sync>(mut self) -> Self {
        self.service_types.insert(TypeId::of::<S>());
        self
    }
    pub fn with_monitor(mut self, addr: ActorAddress) -> Self {
        self.monitor_targets.insert(addr);
        self
    }

    // ── Mutable builder methods ──
    pub fn grant_send(&mut self, addr: ActorAddress) -> &mut Self {
        self.send_any.insert(addr);
        self
    }
    pub fn grant_send_typed<M: Message>(&mut self, addr: ActorAddress) -> &mut Self {
        self.send_typed.insert((TypeId::of::<M>(), addr));
        self
    }

    // ── Check methods ──
    pub fn check_send<M: Message>(&self, addr: ActorAddress) -> Result<(), crate::Error> {
        if self.send_any.contains(&addr) {
            return Ok(());
        }
        if self.send_typed.contains(&(TypeId::of::<M>(), addr)) {
            return Ok(());
        }
        Err(crate::Error::from("capability denied: send"))
    }
    pub fn check_send_addr(&self, addr: ActorAddress) -> Result<(), crate::Error> {
        if self.send_any.contains(&addr) {
            return Ok(());
        }
        Err(crate::Error::from("capability denied: send"))
    }
    pub fn check_spawn(&self) -> Result<(), crate::Error> {
        if self.can_spawn {
            Ok(())
        } else {
            Err(crate::Error::from("capability denied: spawn"))
        }
    }
    pub fn check_service<S: 'static + Send + Sync>(&self) -> Result<(), crate::Error> {
        if self.service_types.contains(&TypeId::of::<S>()) {
            Ok(())
        } else {
            Err(crate::Error::from("capability denied: service"))
        }
    }
    pub fn check_monitor(&self, addr: ActorAddress) -> Result<(), crate::Error> {
        if self.monitor_targets.contains(&addr) {
            Ok(())
        } else {
            Err(crate::Error::from("capability denied: monitor"))
        }
    }
}

impl std::fmt::Debug for CapabilitySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CapabilitySet")
            .field("send_any", &self.send_any.len())
            .field("send_typed", &self.send_typed.len())
            .field("can_spawn", &self.can_spawn)
            .field("services", &self.service_types.len())
            .field("monitors", &self.monitor_targets.len())
            .finish()
    }
}

// ─── SpawnRequest ────────────────────────────────────────────────────────────

/// Bundled arguments for spawning an actor.
///
/// Replaces the spawn channel 3-tuple to stop tuple growth as new fields are added.
pub struct SpawnRequest {
    pub addr: ActorAddress,
    pub actor: Box<dyn AnyActor>,
    pub parent: Option<ActorAddress>,
    pub env: Environment,
}

/// The actor process as represented in the Runtime — thin wrapper around user state.
pub struct Actor<A: ActorInterface> {
    inner: A,
}

impl<A: ActorInterface> Actor<A> {
    pub fn new(inner: A) -> Self {
        Self { inner }
    }

    pub(crate) fn inner(&self) -> &A {
        &self.inner
    }

    pub(crate) fn replace_inner(&mut self, inner: A) {
        self.inner = inner;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorTypeMetadata {
    pub actor_type_id: TypeId,
    pub actor_type_name: &'static str,
    pub message_type_id: TypeId,
    pub message_type_name: &'static str,
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

    fn metadata(&self) -> ActorTypeMetadata;

    fn as_any(&self) -> &dyn Any;

    fn as_any_mut(&mut self) -> &mut dyn Any;
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

    fn metadata(&self) -> ActorTypeMetadata {
        ActorTypeMetadata {
            actor_type_id: TypeId::of::<A>(),
            actor_type_name: std::any::type_name::<A>(),
            message_type_id: TypeId::of::<A::Incoming>(),
            message_type_name: std::any::type_name::<A::Incoming>(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
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
    /// Actor stopped with a typed exit value (via [`Ctx::stop_with`]).
    Completed,
}

/// Death notification delivered as a normal message when a monitored actor dies.
///
/// Subscribe via [`Ctx::monitor`]. The `Down` message arrives in the watcher's
/// regular `handle()` method — no special callback needed.
#[derive(Debug, Clone)]
pub struct Down {
    /// Address of the dead actor.
    pub addr: ActorAddress,
    /// Why it died.
    pub reason: StopReason,
    /// Typed exit value if the actor called [`Ctx::stop_with`].
    pub exit_value: Option<ExitValue>,
}

impl PartialEq for Down {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr && self.reason == other.reason
    }
}

impl Eq for Down {}

/// Internal sentinel message for graceful actor stop.
/// Not a `Message` (not Clone) — intercepted in `tick_all` / `deliver` before
/// reaching `handle_any`. Public so extension crates can construct it for
/// orphan cleanup, but users cannot send it via `ctx.send()`.
pub struct StopSignal;

/// Like [`StopSignal`] but carries a typed exit value.
/// Intercepted in `tick_all` / `deliver`.
pub struct StopWithSignal(pub ExitValue);

/// Internal sentinel message for resuming a suspended actor.
/// Intercepted in [`ActorPool::deliver`] — not delivered to user code.
pub(crate) struct ResumeSignal;

/// Object-safe inner trait for sending type-erased messages.
///
/// Minimal core interface: send, spawn, stop, and extension access.
/// Registry methods (naming, monitoring, groups) and timer scheduling
/// are provided by extension traits in `swactor-std`.
#[allow(private_interfaces)]
pub trait ContextInner {
    fn send_any(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error>;
    fn spawn_any(&self, request: SpawnRequest);
    /// Request graceful stop for an actor. Takes effect after the current message.
    fn request_stop(&self, addr: ActorAddress);
    /// Request graceful stop with a typed exit value. Takes effect after the current message.
    fn request_stop_with(&self, addr: ActorAddress, value: ExitValue);
    /// Request suspension for an actor. Takes effect after the current message.
    fn request_suspend(&self, addr: ActorAddress);
    /// Request resumption for a suspended actor. Sends a [`ResumeSignal`].
    fn request_resume(&self, addr: ActorAddress);
    /// Post a request to the per-worker extension (e.g., timer scheduling).
    fn post_worker_request(&self, request: Box<dyn Any + Send>);
    /// Access the runtime extension (if installed).
    fn extension(&self) -> Option<&dyn crate::extension::RuntimeExtension>;
    /// Access the per-runtime process-output observer (if installed).
    fn process_output_observer(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::process_observer::ProcessOutputObserver>> {
        None
    }
    /// Return system-level information (worker count, actor count, uptime).
    fn system_info(&self) -> SystemInfo;
}

/// Actor syscall interface — passed to `ActorInterface::handle()`.
///
/// Wraps a `&dyn ContextInner` to solve the object-safety problem while
/// providing a typed public API. Registry methods (naming, monitoring, groups)
/// are provided by extension traits in `swactor-std`.
pub struct Ctx<'a> {
    inner: &'a dyn ContextInner,
    self_addr: ActorAddress,
    self_parent_addr: Option<ActorAddress>,
    self_env: Environment,
    self_messages_processed: u64,
    self_mailbox_depth: usize,
    self_msg_type_counts: Vec<(&'static str, u64)>,
}

impl<'a> Ctx<'a> {
    pub(crate) fn new(
        inner: &'a dyn ContextInner,
        self_addr: ActorAddress,
        self_parent_addr: Option<ActorAddress>,
        self_env: Environment,
        self_messages_processed: u64,
        self_mailbox_depth: usize,
        self_msg_type_counts: Vec<(&'static str, u64)>,
    ) -> Self {
        Self {
            inner,
            self_addr,
            self_parent_addr,
            self_env,
            self_messages_processed,
            self_mailbox_depth,
            self_msg_type_counts,
        }
    }

    pub fn raw_inner(&self) -> &dyn ContextInner {
        self.inner
    }

    /// Returns the address of the actor currently being ticked.
    pub fn self_addr(&self) -> ActorAddress {
        self.self_addr
    }

    /// Returns the address of the actor that spawned this one, or `None`
    /// if this actor was spawned externally via `Runtime::spawn`.
    pub fn parent(&self) -> Option<ActorAddress> {
        self.self_parent_addr
    }

    /// Access the runtime extension (if installed).
    pub fn extension(&self) -> Option<&dyn crate::extension::RuntimeExtension> {
        self.inner.extension()
    }

    /// Return system-level information (worker count, actor count, uptime).
    pub fn system_info(&self) -> SystemInfo {
        self.inner.system_info()
    }

    /// Total messages this actor has successfully processed (before the current tick).
    pub fn messages_processed(&self) -> u64 {
        self.self_messages_processed
    }

    /// Number of messages in this actor's mailbox at the start of the current tick.
    pub fn mailbox_depth(&self) -> usize {
        self.self_mailbox_depth
    }

    /// Per-message-type counts for this actor, sorted descending by count.
    pub fn message_type_counts(&self) -> &[(&'static str, u64)] {
        &self.self_msg_type_counts
    }

    /// Check if this actor has a capability set (i.e., is restricted).
    fn capabilities(&self) -> Option<&CapabilitySet> {
        self.self_env.get::<CapabilitySet>()
    }

    /// Send a typed message to an actor address.
    ///
    /// Returns `Ok(())` if the message was accepted for routing. This does **not**
    /// guarantee delivery — the recipient may stop before processing it. If
    /// delivery confirmation is needed, implement an application-level ACK.
    ///
    /// Returns `Err` if the address is unknown to the runtime.
    pub fn send<M: Message>(&self, addr: ActorAddress, msg: M) -> Result<(), Error> {
        if let Some(caps) = self.capabilities()
            && addr != self.self_addr
        {
            caps.check_send::<M>(addr)?;
        }
        self.inner.send_any(addr, Box::new(msg))
    }

    /// Read a typed value from this actor's environment.
    pub fn env<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.self_env.get::<T>()
    }

    /// Access this actor's full environment.
    pub fn environment(&self) -> &Environment {
        &self.self_env
    }

    /// Spawn a new actor, returning its address.
    ///
    /// The child inherits this actor's environment (Arc clone — zero allocation).
    pub fn spawn<A: ActorInterface>(&self, actor: A) -> Result<ActorAddress, Error> {
        if let Some(caps) = self.capabilities() {
            caps.check_spawn()?;
        }
        let addr = ActorAddress::new_random();
        let boxed: Box<dyn AnyActor> = Box::new(Actor::new(actor));
        self.inner.spawn_any(SpawnRequest {
            addr,
            actor: boxed,
            parent: Some(self.self_addr),
            env: self.self_env.clone(),
        });
        Ok(addr)
    }

    /// Create a [`SpawnBuilder`] to spawn an actor with environment overrides.
    ///
    /// Common case (`ctx.spawn(actor)`) is unchanged — this is for when you
    /// need to add or replace environment values for the child.
    pub fn spawn_builder<A: ActorInterface>(&self, actor: A) -> SpawnBuilder<'_, A> {
        SpawnBuilder {
            ctx: self,
            actor,
            env_builder: None,
        }
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
        if let Some(caps) = self.capabilities() {
            caps.check_send_addr(addr)?;
        }
        self.inner.send_any(addr, Box::new(StopSignal))
    }

    /// Stop this actor with a typed exit value.
    ///
    /// Like [`stop_self`](Self::stop_self), but the value is delivered to
    /// monitors (in [`Down::exit_value`]) and watchers (in [`ActorExited::exit_value`]).
    /// The stop reason is [`StopReason::Completed`].
    pub fn stop_with<T: Any + Send + Sync + 'static>(&self, value: T) {
        self.inner
            .request_stop_with(self.self_addr, ExitValue::new(value));
    }

    /// Suspend this actor. Messages continue to queue but are not processed
    /// until a supervisor (or self) calls resume.
    pub fn suspend_self(&self) {
        self.inner.request_suspend(self.self_addr);
    }
}

// ─── SpawnBuilder ────────────────────────────────────────────────────────────

/// Builder for spawning an actor with environment overrides.
///
/// Created via [`Ctx::spawn_builder`]. Lazily clones the parent environment
/// on the first `.env()` call to avoid allocation when no overrides are needed.
pub struct SpawnBuilder<'a, A: ActorInterface> {
    ctx: &'a Ctx<'a>,
    actor: A,
    env_builder: Option<EnvironmentBuilder>,
}

impl<'a, A: ActorInterface> SpawnBuilder<'a, A> {
    /// Add or replace a typed environment value for the child.
    ///
    /// On the first call, lazily clones the parent's environment map.
    pub fn env<T: Any + Send + Sync>(mut self, value: T) -> Self {
        let builder = self
            .env_builder
            .get_or_insert_with(|| EnvironmentBuilder::from_env(self.ctx.environment()));
        builder.set_mut(value);
        self
    }

    /// Spawn the actor, returning its address.
    pub fn finish(self) -> Result<ActorAddress, Error> {
        if let Some(caps) = self.ctx.capabilities() {
            caps.check_spawn()?;
        }
        let addr = ActorAddress::new_random();
        let boxed: Box<dyn AnyActor> = Box::new(Actor::new(self.actor));
        let env = match self.env_builder {
            Some(builder) => builder.build(),
            None => self.ctx.self_env.clone(),
        };
        self.ctx.inner.spawn_any(SpawnRequest {
            addr,
            actor: boxed,
            parent: Some(self.ctx.self_addr),
            env,
        });
        Ok(addr)
    }
}
