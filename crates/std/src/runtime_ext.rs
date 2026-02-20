use swactor::actor::{ActorAddress, ActorInterface, EnvironmentBuilder, LogicalName, Message};
use swactor::runtime::Runtime;
use swactor::Error;

use crate::StdExtension;

fn get_ext(rt: &Runtime) -> &StdExtension {
    rt.extension()
        .expect("StdExtension not installed — use Runtime::with_extension()")
        .as_any()
        .downcast_ref::<StdExtension>()
        .expect("Extension is not StdExtension")
}

/// Naming extension for [`Runtime`].
///
/// Provides `spawn_named`, `where_is`, `unregister`, and `registered_names`
/// via the [`StdExtension`] name registry.
pub trait RuntimeNaming {
    /// Register a name for an already-spawned actor. Returns `Err` if name is taken.
    fn register_name(&self, name: impl Into<String>, addr: ActorAddress) -> Result<(), Error>;

    /// Spawn an actor with a registered name, returning its address.
    fn spawn_named<A: ActorInterface>(&self, name: impl Into<String>, actor: A) -> Result<ActorAddress, Error>;

    /// Look up an actor address by its registered name.
    fn where_is(&self, name: &str) -> Option<ActorAddress>;

    /// Unregister a name. Returns the address it was bound to, or `None`.
    fn unregister(&self, name: &str) -> Option<ActorAddress>;

    /// Return all currently registered actor names.
    fn registered_names(&self) -> Vec<String>;
}

impl RuntimeNaming for Runtime {
    fn register_name(&self, name: impl Into<String>, addr: ActorAddress) -> Result<(), Error> {
        get_ext(self).name_registry.register(name.into(), addr)
    }

    fn spawn_named<A: ActorInterface>(&self, name: impl Into<String>, actor: A) -> Result<ActorAddress, Error> {
        let name = name.into();
        let env = EnvironmentBuilder::new()
            .set(LogicalName(name.clone()))
            .build();
        let addr = self.spawn_with_env(actor, env)?;
        if let Err(e) = get_ext(self).name_registry.register(name, addr) {
            let _ = self.stop_actor(addr);
            return Err(e);
        }
        Ok(addr)
    }

    fn where_is(&self, name: &str) -> Option<ActorAddress> {
        get_ext(self).name_registry.lookup(name)
    }

    fn unregister(&self, name: &str) -> Option<ActorAddress> {
        get_ext(self).name_registry.unregister(name)
    }

    fn registered_names(&self) -> Vec<String> {
        get_ext(self).name_registry.registered_names()
    }
}

/// Watching extension for [`Runtime`].
///
/// Provides `watch` / `unwatch` via the [`StdExtension`] watch registry.
pub trait RuntimeWatching {
    /// Register a watch: `watcher` receives `ActorExited` when `target` dies.
    fn watch(&self, watcher: ActorAddress, target: ActorAddress);

    /// Cancel a watch.
    fn unwatch(&self, watcher: ActorAddress, target: ActorAddress);
}

impl RuntimeWatching for Runtime {
    fn watch(&self, watcher: ActorAddress, target: ActorAddress) {
        get_ext(self).watch_registry.watch(watcher, target);
    }

    fn unwatch(&self, watcher: ActorAddress, target: ActorAddress) {
        get_ext(self).watch_registry.unwatch(watcher, target);
    }
}

/// Group extension for [`Runtime`].
///
/// Provides `join_group`, `leave_group`, `publish_to`, `group_members`,
/// and `groups` via the [`StdExtension`] group registry.
pub trait RuntimeGroups {
    /// Add an actor to a named group. The group is created if it doesn't exist.
    fn join_group(&self, addr: ActorAddress, group: impl Into<String>);

    /// Remove an actor from a named group. Empty groups are auto-deleted.
    fn leave_group(&self, addr: ActorAddress, group: &str);

    /// Broadcast a message to all members of a named group.
    /// Returns the number of messages successfully enqueued.
    fn publish_to<M: Message>(&self, group: &str, msg: M) -> usize;

    /// Return all current members of a named group.
    fn group_members(&self, group: &str) -> Vec<ActorAddress>;

    /// Return all active group names.
    fn groups(&self) -> Vec<String>;
}

impl RuntimeGroups for Runtime {
    fn join_group(&self, addr: ActorAddress, group: impl Into<String>) {
        get_ext(self).group_registry.join(group.into(), addr);
    }

    fn leave_group(&self, addr: ActorAddress, group: &str) {
        get_ext(self).group_registry.leave(group, &addr);
    }

    fn publish_to<M: Message>(&self, group: &str, msg: M) -> usize {
        let members = get_ext(self).group_registry.members(group);
        let mut count = 0;
        for member in &members {
            if self.send_to(*member, msg.clone()).is_ok() {
                count += 1;
            }
        }
        count
    }

    fn group_members(&self, group: &str) -> Vec<ActorAddress> {
        get_ext(self).group_registry.members(group)
    }

    fn groups(&self) -> Vec<String> {
        get_ext(self).group_registry.group_names()
    }
}

/// Service registry extension for [`Runtime`].
///
/// Allows registering typed service bindings that are automatically injected
/// into every actor's environment at spawn time.
pub trait RuntimeResources {
    /// Register a service address under marker type `S`.
    ///
    /// All actors spawned after this call will have `ServiceBinding<S>` in
    /// their environment (unless overridden via `spawn_builder`).
    fn register_service<S: 'static + Send + Sync>(&self, addr: ActorAddress);
}

impl RuntimeResources for Runtime {
    fn register_service<S: 'static + Send + Sync>(&self, addr: ActorAddress) {
        get_ext(self).service_registry.register::<S>(addr);
    }
}
