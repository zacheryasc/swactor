use swactor::actor::{ActorAddress, ActorInterface, Ctx, Message, MonitorRef};
use swactor::Error;

use crate::StdExtension;

fn get_ext<'a>(ctx: &'a Ctx) -> &'a StdExtension {
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
    fn monitor(&self, target: ActorAddress) -> MonitorRef;

    /// Cancel a monitor subscription.
    fn demonitor(&self, mref: MonitorRef);
}

impl CtxMonitoring for Ctx<'_> {
    fn monitor(&self, target: ActorAddress) -> MonitorRef {
        get_ext(self).monitor_registry.register(self.self_addr(), target)
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
        let addr = self.spawn(actor)?;
        if let Err(e) = get_ext(self).name_registry.register(name, addr) {
            let _ = self.stop_actor(addr);
            return Err(e);
        }
        Ok(addr)
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
