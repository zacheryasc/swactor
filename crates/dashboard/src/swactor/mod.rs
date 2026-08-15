//! Actor-side folding shared by the fused control-plane view.

pub(crate) mod actor_view;

pub(crate) use actor_view::{ActorState, RuntimeState};

pub const RUNTIME_STATS: &str = "runtime.stats";
pub const RUNTIME_ACTORS: &str = "runtime.actors";
