use crate::actor::{ActorAddress, ActorInterface, Ctx, Environment, LogicalName, Message, MonitorRef, SystemInfo};
use crate::Error;

use super::resource_handle::ResourceHandle;
use super::StdExtension;
use super::timer_wheel::{CloneMsg, TimerRequest};

pub(crate) fn get_ext<'a>(ctx: &'a Ctx) -> &'a StdExtension {
    ctx.extension()
        .expect("StdExtension not installed — use Runtime::with_extension()")
        .as_any()
        .downcast_ref::<StdExtension>()
        .expect("Extension is not StdExtension")
}

/// Monitoring extension for [`Ctx`].
///
/// Provides `monitor` / `demonitor` via the [`StdExtension`] monitor registry.
pub trait CtxMonitoring {
    /// Subscribe to death notifications from `target`. Returns a [`MonitorRef`]
    /// that can be used to cancel the subscription.
    fn monitor(&self, target: ActorAddress) -> Result<MonitorRef, Error>;

    /// Cancel a monitor subscription.
    fn demonitor(&self, mref: MonitorRef);
}

impl CtxMonitoring for Ctx<'_> {
    fn monitor(&self, target: ActorAddress) -> Result<MonitorRef, Error> {
        if let Some(caps) = self.env::<crate::CapabilitySet>() {
            caps.check_monitor(target)?;
        }
        Ok(get_ext(self).monitor_registry.register(self.self_addr(), target))
    }

    fn demonitor(&self, mref: MonitorRef) {
        get_ext(self).monitor_registry.deregister(mref);
    }
}

/// Naming extension for [`Ctx`].
///
/// Provides `where_is`, `register_name`, and `spawn_named` via the [`StdExtension`]
/// name registry.
pub trait CtxNaming {
    /// Look up an actor address by its registered name.
    fn where_is(&self, name: &str) -> Option<ActorAddress>;

    /// Register a name for the given address.
    fn register_name(&self, name: impl Into<String>, addr: ActorAddress) -> Result<(), Error>;

    /// Spawn an actor with a registered name, returning its address.
    fn spawn_named<A: ActorInterface>(&self, name: impl Into<String>, actor: A) -> Result<ActorAddress, Error>;
}

impl CtxNaming for Ctx<'_> {
    fn where_is(&self, name: &str) -> Option<ActorAddress> {
        get_ext(self).name_registry.lookup(name)
    }

    fn register_name(&self, name: impl Into<String>, addr: ActorAddress) -> Result<(), Error> {
        get_ext(self).name_registry.register(name.into(), addr)
    }

    fn spawn_named<A: ActorInterface>(&self, name: impl Into<String>, actor: A) -> Result<ActorAddress, Error> {
        let name = name.into();
        let addr = self.spawn_builder(actor).env(LogicalName(name.clone())).finish()?;
        if let Err(e) = get_ext(self).name_registry.register(name, addr) {
            let _ = self.stop_actor(addr);
            return Err(e);
        }
        Ok(addr)
    }
}

/// Watching extension for [`Ctx`].
///
/// Provides `watch` / `unwatch` via the [`StdExtension`] watch registry.
/// When a watched actor dies, the watcher receives an [`ActorExited`] message
/// delivered to its `on_actor_exit()` callback.
pub trait CtxWatching {
    /// Watch another actor's liveness. If the target dies, this actor
    /// receives an `ActorExited` message.
    ///
    /// Calling watch() multiple times on the same target is idempotent —
    /// only one notification is delivered.
    fn watch(&self, target: ActorAddress);

    /// Stop watching an actor. No notification will be delivered if the
    /// target subsequently dies.
    fn unwatch(&self, target: ActorAddress);
}

impl CtxWatching for Ctx<'_> {
    fn watch(&self, target: ActorAddress) {
        get_ext(self).watch_registry.watch(self.self_addr(), target);
    }

    fn unwatch(&self, target: ActorAddress) {
        get_ext(self).watch_registry.unwatch(self.self_addr(), target);
    }
}

/// Timer extension for [`Ctx`].
///
/// Provides `send_after_ticks` / `send_interval_ticks` via the per-worker
/// [`TimerWheel`](super::timer_wheel::TimerWheel).
pub trait CtxTimers {
    /// Schedule a one-shot timer: deliver `msg` to `addr` after `ticks` worker ticks.
    ///
    /// The message is delivered as a normal mailbox message during the fire tick,
    /// before `tick_all` processes messages. The timer is tick-counted (deterministic),
    /// not wall-clock based.
    fn send_after_ticks<M: Message>(&self, addr: ActorAddress, msg: M, ticks: u64);

    /// Schedule a repeating timer: deliver a clone of `msg` to `addr` every `period` ticks.
    ///
    /// The first delivery happens after `period` ticks. The message is cloned for each
    /// delivery. The timer continues until the target actor is stopped/poisoned.
    fn send_interval_ticks<M: Message>(&self, addr: ActorAddress, msg: M, period: u64);
}

impl CtxTimers for Ctx<'_> {
    fn send_after_ticks<M: Message>(&self, addr: ActorAddress, msg: M, ticks: u64) {
        self.raw_inner().post_worker_request(Box::new(TimerRequest::Once {
            dest: addr,
            msg: Box::new(msg),
            ticks,
        }));
    }

    fn send_interval_ticks<M: Message>(&self, addr: ActorAddress, msg: M, period: u64) {
        self.raw_inner().post_worker_request(Box::new(TimerRequest::Interval {
            dest: addr,
            msg: Box::new(msg) as Box<dyn CloneMsg>,
            period,
        }));
    }
}

/// Group extension for [`Ctx`].
///
/// Provides `join_group`, `leave_group`, `publish`, and `group_members` via
/// the [`StdExtension`] group registry.
pub trait CtxGroups {
    /// Add this actor to a named group.
    fn join_group(&self, group: impl Into<String>);

    /// Remove this actor from a named group.
    fn leave_group(&self, group: &str);

    /// Broadcast a message to all members of a named group.
    /// Returns the number of messages successfully enqueued.
    fn publish<M: Message>(&self, group: &str, msg: M) -> usize;

    /// Return all members of a named group.
    fn group_members(&self, group: &str) -> Vec<ActorAddress>;
}

impl CtxGroups for Ctx<'_> {
    fn join_group(&self, group: impl Into<String>) {
        get_ext(self).group_registry.join(group.into(), self.self_addr());
    }

    fn leave_group(&self, group: &str) {
        get_ext(self).group_registry.leave(group, &self.self_addr());
    }

    fn publish<M: Message>(&self, group: &str, msg: M) -> usize {
        let members = get_ext(self).group_registry.members(group);
        let mut count = 0;
        for member in &members {
            if self.send(*member, msg.clone()).is_ok() {
                count += 1;
            }
        }
        count
    }

    fn group_members(&self, group: &str) -> Vec<ActorAddress> {
        get_ext(self).group_registry.members(group)
    }
}

/// System introspection extension for [`Ctx`].
///
/// Provides convenience accessors for system-level information. Does NOT
/// require [`StdExtension`] — the data comes from the core runtime.
pub trait CtxSystem {
    /// Returns the full [`SystemInfo`] snapshot.
    fn system_info(&self) -> SystemInfo;

    /// Index of the worker thread this actor is running on.
    fn worker_id(&self) -> usize;

    /// Total number of worker threads in the runtime.
    fn num_workers(&self) -> usize;

    /// Total number of live actors across all workers.
    fn total_actors(&self) -> usize;

    /// Milliseconds since the runtime was created.
    fn uptime_ms(&self) -> u64;
}

impl CtxSystem for Ctx<'_> {
    fn system_info(&self) -> SystemInfo {
        Ctx::system_info(self)
    }

    fn worker_id(&self) -> usize {
        Ctx::system_info(self).worker_id
    }

    fn num_workers(&self) -> usize {
        Ctx::system_info(self).num_workers
    }

    fn total_actors(&self) -> usize {
        Ctx::system_info(self).total_actors
    }

    fn uptime_ms(&self) -> u64 {
        Ctx::system_info(self).uptime_ms
    }
}

/// Lineage extension for [`Ctx`].
///
/// Exposes the actor's parent and supervisor. `parent()` does NOT require
/// [`StdExtension`] — the data is stored in core per-actor state.
/// `supervisor()` returns `None` gracefully when StdExtension is absent.
pub trait CtxLineage {
    /// Returns the address of the actor that spawned this one, or `None`
    /// if this actor was spawned externally via `Runtime::spawn`.
    fn parent(&self) -> Option<ActorAddress>;

    /// Returns the address of this actor's supervisor, or `None` if
    /// unsupervised or StdExtension is not installed.
    fn supervisor(&self) -> Option<ActorAddress>;
}

impl CtxLineage for Ctx<'_> {
    fn parent(&self) -> Option<ActorAddress> {
        Ctx::parent(self)
    }

    fn supervisor(&self) -> Option<ActorAddress> {
        let ext = self.extension()?
            .as_any()
            .downcast_ref::<StdExtension>()?;
        ext.supervisor_registry.lookup(&self.self_addr())
    }
}

/// Per-actor self-introspection extension for [`Ctx`].
///
/// Exposes the actor's own operational metrics. Does NOT require
/// [`StdExtension`] — the data is snapshotted from core before each tick.
pub trait CtxSelfStats {
    /// Total messages this actor has successfully processed (before the current tick).
    fn messages_processed(&self) -> u64;

    /// Number of messages in this actor's mailbox at the start of the current tick.
    fn mailbox_depth(&self) -> usize;

    /// Per-message-type counts for this actor, sorted descending by count.
    fn message_type_counts(&self) -> &[(&'static str, u64)];
}

impl CtxSelfStats for Ctx<'_> {
    fn messages_processed(&self) -> u64 {
        Ctx::messages_processed(self)
    }

    fn mailbox_depth(&self) -> usize {
        Ctx::mailbox_depth(self)
    }

    fn message_type_counts(&self) -> &[(&'static str, u64)] {
        Ctx::message_type_counts(self)
    }
}

/// Service resource extension for [`Ctx`].
///
/// Provides typed service discovery via the environment. Does NOT require
/// [`StdExtension`] — reads from the core environment (same as [`CtxEnvironment`]).
pub trait CtxResources {
    /// Look up a service address by marker type `S`.
    ///
    /// Returns `None` if no `ServiceBinding<S>` is present in the environment.
    fn resource<S: 'static + Send + Sync>(&self) -> Option<ActorAddress>;
}

impl CtxResources for Ctx<'_> {
    fn resource<S: 'static + Send + Sync>(&self) -> Option<ActorAddress> {
        if let Some(caps) = self.env::<crate::CapabilitySet>()
            && caps.check_service::<S>().is_err() {
                return None;
            }
        self.env::<crate::ServiceBinding<S>>().map(|b| b.addr)
    }
}

/// Environment extension for [`Ctx`].
///
/// Provides access to the actor's inherited typed key-value environment.
/// Does NOT require [`StdExtension`] — the data is stored in core per-actor state.
pub trait CtxEnvironment {
    /// Read a typed value from this actor's environment.
    fn env<T: std::any::Any + Send + Sync>(&self) -> Option<&T>;

    /// Access this actor's full environment.
    fn environment(&self) -> &Environment;
}

impl CtxEnvironment for Ctx<'_> {
    fn env<T: std::any::Any + Send + Sync>(&self) -> Option<&T> {
        Ctx::env(self)
    }

    fn environment(&self) -> &Environment {
        Ctx::environment(self)
    }
}

/// Resource handle extension for [`Ctx`].
///
/// Provides `handle::<H>()` to construct typed proxy structs wrapping service
/// addresses for ergonomic domain-specific APIs. See [`ResourceHandle`] for
/// how to define a handle type.
pub trait CtxHandles {
    /// Construct a typed resource handle from the service registry.
    ///
    /// Returns `None` if no `ServiceBinding<H::Service>` is present in the
    /// actor's environment (consistent with `ctx.resource()`, `ctx.where_is()`, etc).
    fn handle<H: ResourceHandle>(&self) -> Option<H>;
}

impl CtxHandles for Ctx<'_> {
    fn handle<H: ResourceHandle>(&self) -> Option<H> {
        let binding = self.env::<crate::ServiceBinding<H::Service>>()?;
        Some(H::from_parts(binding.addr, self.self_addr()))
    }
}

/// Lifecycle extension for [`Ctx`].
///
/// Provides suspend/resume capabilities with authorization:
/// only the actor itself or its supervisor can resume it.
pub trait CtxLifecycle {
    /// Suspend this actor. Messages continue to queue but are not processed
    /// until resumed by self or supervisor.
    fn suspend_self(&self);

    /// Resume a suspended actor. Only the actor itself or its supervisor
    /// may call this. Returns `Err` if the caller is not authorized.
    fn resume(&self, target: ActorAddress) -> Result<(), Error>;
}

impl CtxLifecycle for Ctx<'_> {
    fn suspend_self(&self) {
        Ctx::suspend_self(self);
    }

    fn resume(&self, target: ActorAddress) -> Result<(), Error> {
        // Self-resume is always allowed
        if target == self.self_addr() {
            self.raw_inner().request_resume(target);
            return Ok(());
        }
        // Supervisor can resume its child
        let ext = get_ext(self);
        if ext.supervisor_registry.lookup(&target) == Some(self.self_addr()) {
            self.raw_inner().request_resume(target);
            return Ok(());
        }
        Err(Error::from("resume denied: caller is not self or supervisor"))
    }
}

/// Capability introspection extension for [`Ctx`].
pub trait CtxCapabilities {
    fn capabilities(&self) -> Option<&crate::CapabilitySet>;
    fn is_restricted(&self) -> bool;
}

impl CtxCapabilities for Ctx<'_> {
    fn capabilities(&self) -> Option<&crate::CapabilitySet> {
        Ctx::env(self)
    }
    fn is_restricted(&self) -> bool {
        self.env::<crate::CapabilitySet>().is_some()
    }
}
