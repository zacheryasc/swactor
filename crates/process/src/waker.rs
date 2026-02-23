use std::sync::Arc;

/// A handle that I/O threads use to wake the owning actor.
///
/// Constructed with a closure that sends a `ProcessCommand::PollTick`
/// to the actor via `ExternalSender`. Thread-safe and cloneable.
#[derive(Clone)]
pub struct ProcessWaker(Arc<dyn Fn() + Send + Sync>);

impl std::fmt::Debug for ProcessWaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessWaker").finish_non_exhaustive()
    }
}

impl ProcessWaker {
    pub fn new(f: impl Fn() + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    /// Wake the owning actor so it drains pending events.
    pub fn wake(&self) {
        (self.0)();
    }
}
