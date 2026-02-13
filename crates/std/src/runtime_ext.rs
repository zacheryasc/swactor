use swactor::actor::{ActorAddress, ActorInterface, Message};
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
    fn spawn_named<A: ActorInterface>(&self, name: impl Into<String>, actor: A) -> Result<ActorAddress, Error> {
        let name = name.into();
        let addr = self.spawn(actor)?;
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
