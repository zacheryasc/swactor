use std::any::Any;

use crate::actor::{ActorAddress, StopReason};

/// Extension hook for runtime lifecycle events.
///
/// Stored as `Arc<dyn RuntimeExtension>` in the Runtime. Workers access it
/// via TickContext. Ctx methods that need registry access downcast to the
/// concrete type via `as_any()`.
///
/// Core calls these methods at appropriate tick phases:
/// - `on_actor_death`: called during phase 7 (cleanup_dead) with newly dead actors
/// - `cleanup_dead`: called during phase 7 to clean up extension state
pub trait RuntimeExtension: Send + Sync {
    /// Called during phase 7 (cleanup_dead) for each dead actor.
    /// Returns (destination, message) pairs for death notifications.
    /// The core delivers these through normal routing (pending_local or transfer queue).
    fn on_actor_death(
        &self,
        dead: &[(ActorAddress, StopReason)],
    ) -> Vec<(ActorAddress, Box<dyn Any + Send>)>;

    /// Clean up extension state for dead actors (names, groups, monitors).
    fn cleanup_dead(&self, dead: &[ActorAddress]);

    /// Downcast support for Ctx extension traits.
    fn as_any(&self) -> &dyn Any;
}
