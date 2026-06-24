use crate::Error;
use crate::actor::{ActorAddress, Message};
use crate::runtime::Runtime;

use super::StdExtension;

fn get_ext(rt: &Runtime) -> &StdExtension {
    rt.extension()
        .expect("StdExtension not installed — use Runtime::with_extension()")
        .as_any()
        .downcast_ref::<StdExtension>()
        .expect("Extension is not StdExtension")
}

/// Naming extension for [`Runtime`].
///
/// Provides explicit name registration and lookup via [`StdExtension`]'s name registry.
pub trait RuntimeNaming {
    /// Register a name for an already-spawned actor. Returns `Err` if name is taken.
    fn register_name(&self, name: impl Into<String>, addr: ActorAddress) -> Result<(), Error>;

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
/// Provides runtime-level group membership and publication via [`StdExtension`]'s
/// group registry.
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
