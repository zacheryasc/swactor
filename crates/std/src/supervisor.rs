use std::sync::Arc;

use swactor::actor::{ActorAddress, ActorInterface, Ctx, Down, MonitorRef, StopReason};
use swactor::Error;

use crate::CtxMonitoring;

/// How a child should be restarted when it dies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartPolicy {
    /// Always restart, regardless of stop reason.
    Permanent,
    /// Restart only on abnormal exit (Panicked). Normal stops are final.
    Transient,
    /// Never restart. The child is removed on any exit.
    Temporary,
}

/// Strategy for handling child failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorStrategy {
    /// Only restart the failed child. Other children are unaffected.
    OneForOne,
    /// Terminate all children and restart them all in spec order.
    OneForAll,
    /// Terminate children started after the failed child, then restart
    /// the failed child and all terminated children in spec order.
    RestForOne,
}

/// Specification for a supervised child actor.
///
/// The `start` closure is called with `&Ctx` and should spawn the child actor
/// (typically via `ctx.spawn()`). The supervisor monitors the returned address
/// and applies the restart policy when the child dies.
pub struct ChildSpec {
    /// Unique identifier for this child.
    pub id: String,
    /// How to restart this child.
    pub restart: RestartPolicy,
    /// Factory to spawn the child. Called with `&Ctx`, returns the child's address.
    pub start: Arc<dyn Fn(&Ctx) -> Result<ActorAddress, Error> + Send + Sync>,
}

impl ChildSpec {
    pub fn new(
        id: impl Into<String>,
        restart: RestartPolicy,
        start: impl Fn(&Ctx) -> Result<ActorAddress, Error> + Send + Sync + 'static,
    ) -> Self {
        Self {
            id: id.into(),
            restart,
            start: Arc::new(start),
        }
    }
}

/// Tracked state for an active child within a supervisor or router.
pub(crate) struct ActiveChild {
    pub(crate) addr: ActorAddress,
    pub(crate) _monitor_ref: MonitorRef,
}

/// Internal phase for coordinating multi-child restart (OneForAll, RestForOne).
///
/// In `Normal` phase, the supervisor processes Down messages and applies the strategy.
/// When a coordinated restart is needed, it transitions to `Stopping` (sends stop
/// signals, waits for Down confirmations) then restarts all affected children.
enum SupervisorPhase {
    /// Normal operation — process Down messages and apply strategy.
    Normal,
    /// Waiting for children to confirm death before restarting.
    Stopping {
        /// Children we're still waiting for Down confirmation.
        awaiting: Vec<ActorAddress>,
        /// Spec indices to restart once all confirmations received.
        restart_set: Vec<usize>,
    },
}

/// A supervisor actor that manages child actors according to a restart strategy.
///
/// Children are spawned during `on_start`. When a child dies, the supervisor
/// receives a [`Down`] notification via [`ActorInterface::handle_down`] and
/// applies the configured strategy and restart policy.
///
/// # Strategies
///
/// - **OneForOne**: Only the failed child is restarted.
/// - **OneForAll**: All children are stopped, then all restarted in spec order.
/// - **RestForOne**: The failed child and all children started after it are
///   stopped, then restarted in spec order.
///
/// # Restart Intensity
///
/// The supervisor tracks total restarts. When `total_restarts > max_restarts`,
/// the supervisor stops itself (meltdown protection), escalating the failure
/// to its own supervisor if one exists.
///
/// # Example
///
/// ```ignore
/// let sup = Supervisor::new(
///     SupervisorStrategy::OneForOne,
///     5, // max 5 restarts before meltdown
///     vec![
///         ChildSpec::new("worker", RestartPolicy::Permanent, |ctx| {
///             ctx.spawn(MyWorker::new())
///         }),
///     ],
/// );
/// let sup_addr = rt.spawn(sup)?;
/// ```
pub struct Supervisor {
    strategy: SupervisorStrategy,
    max_restarts: u32,
    specs: Vec<ChildSpec>,
    children: Vec<Option<ActiveChild>>,
    total_restarts: u32,
    phase: SupervisorPhase,
}

impl Supervisor {
    pub fn new(
        strategy: SupervisorStrategy,
        max_restarts: u32,
        specs: Vec<ChildSpec>,
    ) -> Self {
        let children = (0..specs.len()).map(|_| None).collect();
        Self {
            strategy,
            max_restarts,
            specs,
            children,
            total_restarts: 0,
            phase: SupervisorPhase::Normal,
        }
    }

    fn start_child(&mut self, ctx: &Ctx, idx: usize) -> Result<(), Error> {
        let addr = (self.specs[idx].start)(ctx)?;
        let mref = ctx.monitor(addr);
        self.children[idx] = Some(ActiveChild {
            addr,
            _monitor_ref: mref,
        });
        Ok(())
    }

    fn find_child_idx(&self, addr: ActorAddress) -> Option<usize> {
        self.children
            .iter()
            .position(|c| c.as_ref().map_or(false, |ac| ac.addr == addr))
    }

    /// Check meltdown intensity — returns true if we should stop.
    fn check_intensity(&mut self) -> bool {
        self.total_restarts += 1;
        self.total_restarts > self.max_restarts
    }

    /// Try to finish the coordinated restart: restart all children in `restart_set`.
    fn finish_restart(&mut self, ctx: &Ctx) {
        let restart_set = match &mut self.phase {
            SupervisorPhase::Stopping { restart_set, .. } => {
                std::mem::take(restart_set)
            }
            _ => return,
        };
        self.phase = SupervisorPhase::Normal;

        for idx in restart_set {
            if let Err(e) = self.start_child(ctx, idx) {
                eprintln!(
                    "swactor: supervisor failed to restart child '{}': {}",
                    self.specs[idx].id, e
                );
            }
        }
    }

    /// Begin a coordinated restart for the given spec indices.
    /// Stops any living children in the set, then waits for their Down messages.
    fn begin_coordinated_restart(&mut self, ctx: &Ctx, restart_indices: Vec<usize>) {
        let mut awaiting = Vec::new();
        for &idx in &restart_indices {
            if let Some(child) = self.children[idx].take() {
                let _ = ctx.stop_actor(child.addr);
                awaiting.push(child.addr);
            }
        }

        if awaiting.is_empty() {
            // All children already dead — restart immediately.
            for idx in &restart_indices {
                if let Err(e) = self.start_child(ctx, *idx) {
                    eprintln!(
                        "swactor: supervisor failed to restart child '{}': {}",
                        self.specs[*idx].id, e
                    );
                }
            }
        } else {
            self.phase = SupervisorPhase::Stopping {
                awaiting,
                restart_set: restart_indices,
            };
        }
    }
}

impl ActorInterface for Supervisor {
    type Incoming = ();
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}

    fn on_start(&mut self, ctx: &Ctx) {
        for idx in 0..self.specs.len() {
            if let Err(e) = self.start_child(ctx, idx) {
                eprintln!(
                    "swactor: supervisor failed to start child '{}': {}",
                    self.specs[idx].id, e
                );
            }
        }
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        for child in self.children.iter().flatten() {
            let _ = ctx.stop_actor(child.addr);
        }
    }

    fn handle_down(&mut self, ctx: &Ctx, down: Down) {
        // During coordinated restart: track Down confirmations.
        if matches!(self.phase, SupervisorPhase::Stopping { .. }) {
            // Clear from children tracking
            if let Some(idx) = self.find_child_idx(down.addr) {
                self.children[idx] = None;
            }
            // Remove from awaiting list
            if let SupervisorPhase::Stopping { awaiting, .. } = &mut self.phase {
                awaiting.retain(|a| *a != down.addr);
            }
            let done = matches!(&self.phase,
                SupervisorPhase::Stopping { awaiting, .. } if awaiting.is_empty());
            if done {
                self.finish_restart(ctx);
            }
            return;
        }

        // Normal phase: handle child death.
        let Some(idx) = self.find_child_idx(down.addr) else {
            return;
        };
        self.children[idx] = None;

        let should_restart = match self.specs[idx].restart {
            RestartPolicy::Permanent => true,
            RestartPolicy::Transient => down.reason == StopReason::Panicked,
            RestartPolicy::Temporary => false,
        };

        if !should_restart {
            return;
        }

        if self.check_intensity() {
            eprintln!(
                "swactor: supervisor reached max restarts ({}), shutting down",
                self.max_restarts
            );
            ctx.stop_self();
            return;
        }

        match self.strategy {
            SupervisorStrategy::OneForOne => {
                if let Err(e) = self.start_child(ctx, idx) {
                    eprintln!(
                        "swactor: supervisor failed to restart child '{}': {}",
                        self.specs[idx].id, e
                    );
                }
            }
            SupervisorStrategy::OneForAll => {
                // Stop all other living children, then restart all in order.
                let restart_indices: Vec<usize> = (0..self.specs.len()).collect();
                self.begin_coordinated_restart(ctx, restart_indices);
            }
            SupervisorStrategy::RestForOne => {
                // Stop children after the failed one, then restart failed + rest.
                let restart_indices: Vec<usize> = (idx..self.specs.len()).collect();
                self.begin_coordinated_restart(ctx, restart_indices);
            }
        }
    }
}
