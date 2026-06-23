use std::any::Any;

use crate::actor::{ActorAddress, Environment, ExitValue, StopReason};

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
        dead: &[(ActorAddress, StopReason, Option<ExitValue>)],
    ) -> Vec<(ActorAddress, Box<dyn Any + Send>)>;

    /// Clean up extension state for dead actors (names, groups, monitors).
    fn cleanup_dead(&self, dead: &[ActorAddress]);

    /// Called for each newly spawned actor, before it enters the pool.
    /// Extensions can enrich the actor's environment (e.g., inject SpawnTimestamp).
    /// `child` is the address of the newly spawned actor.
    /// `parent` is the address of the spawning actor, or `None` for runtime-spawned actors.
    /// `uptime_ms` is milliseconds since runtime creation.
    /// Default: no-op (returns env unchanged).
    fn on_spawn(
        &self,
        child: ActorAddress,
        parent: Option<ActorAddress>,
        env: Environment,
        uptime_ms: u64,
    ) -> Environment {
        let _ = (child, parent, uptime_ms);
        env
    }

    /// Downcast support for Ctx extension traits.
    fn as_any(&self) -> &dyn Any;

    /// Create a per-worker extension instance. Called once per worker during init.
    ///
    /// Unlike `RuntimeExtension` (shared across all workers), each worker owns
    /// its own `WorkerExtension` instance for per-worker state like timer wheels.
    fn create_worker_extension(&self) -> Option<Box<dyn WorkerExtension>> {
        None
    }
}

/// Per-worker extension state, created by [`RuntimeExtension::create_worker_extension`].
///
/// Each worker owns its own instance. Core calls these methods during tick phases:
/// - `on_tick`: phase 2.5 — before tick_all, returns messages to deliver
/// - `handle_request`: phase 5.5 — processes deferred requests from handlers
/// - `gc_dead`: after cleanup_dead — removes state for dead actors
pub trait WorkerExtension: Send {
    /// Returns `true` if this extension has pending work (e.g., active timers).
    /// Used by the fast idle path to avoid unnecessary ticks.
    fn has_pending_work(&self) -> bool {
        false
    }

    /// Called each tick before tick_all. Returns messages to deliver.
    fn on_tick(&mut self) -> Vec<(ActorAddress, Box<dyn Any + Send>)>;

    /// Process a deferred request posted during handle() via `post_worker_request`.
    fn handle_request(&mut self, request: Box<dyn Any + Send>);

    /// Clean up state for dead actors.
    fn gc_dead(&mut self, dead: &[ActorAddress]);
}
