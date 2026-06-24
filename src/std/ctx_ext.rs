use crate::actor::{ActorAddress, Ctx};

use super::StdExtension;

pub(crate) fn get_ext<'a>(ctx: &'a Ctx) -> &'a StdExtension {
    ctx.extension()
        .expect("StdExtension not installed — use Runtime::with_extension()")
        .as_any()
        .downcast_ref::<StdExtension>()
        .expect("Extension is not StdExtension")
}

/// Watching extension for [`Ctx`].
///
/// Provides actor-side death notification via [`StdExtension`]'s watch registry.
pub trait CtxWatching {
    /// Watch another actor's liveness. If the target dies, this actor receives an
    /// [`crate::actor::ActorExited`] message through `on_actor_exit`.
    fn watch(&self, target: ActorAddress);
}

impl CtxWatching for Ctx<'_> {
    fn watch(&self, target: ActorAddress) {
        get_ext(self).watch_registry.watch(self.self_addr(), target);
    }
}

/// Group extension for [`Ctx`].
///
/// Provides actor-side group membership via [`StdExtension`]'s group registry.
pub trait CtxGroups {
    /// Add this actor to a named group.
    fn join_group(&self, group: impl Into<String>);
}

impl CtxGroups for Ctx<'_> {
    fn join_group(&self, group: impl Into<String>) {
        get_ext(self)
            .group_registry
            .join(group.into(), self.self_addr());
    }
}
