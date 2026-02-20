use std::any::Any;

use swactor::actor::{ActorAddress, Down, Environment, EnvironmentBuilder, ExitReason, ExitValue, SpawnTimestamp, StopReason, StopSignal};
use swactor::extension::{RuntimeExtension, WorkerExtension};

use crate::children_registry::ChildrenRegistry;
use crate::group_registry::GroupRegistry;
use crate::monitor_registry::MonitorRegistry;
use crate::name_registry::NameRegistry;
use crate::service_registry::ServiceRegistry;
use crate::supervisor_registry::SupervisorRegistry;
use crate::timer_wheel::TimerWheel;
use crate::watch_registry::WatchRegistry;

/// Standard library extension — provides naming, monitoring, watching, and group registries.
///
/// Install on a `Runtime` via `runtime.with_extension(Arc::new(StdExtension::new()))`.
pub struct StdExtension {
    pub(crate) name_registry: NameRegistry,
    pub(crate) monitor_registry: MonitorRegistry,
    pub(crate) watch_registry: WatchRegistry,
    pub(crate) group_registry: GroupRegistry,
    pub(crate) supervisor_registry: SupervisorRegistry,
    pub(crate) service_registry: ServiceRegistry,
    pub(crate) children_registry: ChildrenRegistry,
}

impl StdExtension {
    pub fn new() -> Self {
        Self {
            name_registry: NameRegistry::new(),
            monitor_registry: MonitorRegistry::new(),
            watch_registry: WatchRegistry::new(),
            group_registry: GroupRegistry::new(),
            supervisor_registry: SupervisorRegistry::new(),
            service_registry: ServiceRegistry::new(),
            children_registry: ChildrenRegistry::new(),
        }
    }

    /// Resolve a human-readable name for an actor address (reverse lookup).
    pub fn resolve_name(&self, addr: &ActorAddress) -> Option<String> {
        self.name_registry.lookup_by_addr(addr)
    }

    /// Register a supervisor → child relationship.
    ///
    /// This is used by the built-in [`Supervisor`](crate::Supervisor) and can
    /// also be called by custom supervisor implementations.
    pub fn register_supervisor(&self, supervisor: ActorAddress, child: ActorAddress) {
        self.supervisor_registry.register(supervisor, child);
    }
}

impl Default for StdExtension {
    fn default() -> Self {
        Self::new()
    }
}

/// Map StopReason → ExitReason for watch notifications.
fn stop_to_exit(reason: StopReason) -> ExitReason {
    match reason {
        StopReason::Normal => ExitReason::Stopped,
        StopReason::Panicked => ExitReason::Panicked,
        StopReason::Completed => ExitReason::Completed,
    }
}

impl RuntimeExtension for StdExtension {
    fn on_actor_death(
        &self,
        dead: &[(ActorAddress, StopReason, Option<ExitValue>)],
    ) -> Vec<(ActorAddress, Box<dyn Any + Send>)> {
        let mut notifications = Vec::new();

        for (addr, reason, exit_value) in dead {
            let addr = *addr;
            let reason = *reason;

            // Monitor notifications (Down)
            let watchers = self.monitor_registry.take_monitors(&addr);
            for (_mref, watcher) in watchers {
                let down = Down { addr, reason, exit_value: exit_value.clone() };
                notifications.push((watcher, Box::new(down) as Box<dyn Any + Send>));
            }

            // Watch notifications (ActorExited)
            let watch_notifications = self.watch_registry.notify_death(addr, stop_to_exit(reason), exit_value.clone());
            for (watcher, exited) in watch_notifications {
                notifications.push((watcher, Box::new(exited) as Box<dyn Any + Send>));
            }

            // Orphan handling: kill unsupervised children
            let children = self.children_registry.take_children(&addr);
            for child in children {
                if self.supervisor_registry.lookup(&child).is_none() {
                    notifications.push((child, Box::new(StopSignal) as Box<dyn Any + Send>));
                }
            }
        }

        notifications
    }

    fn cleanup_dead(&self, dead: &[ActorAddress]) {
        self.children_registry.cleanup(dead);
        for addr in dead {
            self.name_registry.unregister_by_addr(addr);
            self.group_registry.cleanup(addr);
            self.monitor_registry.remove_watcher(addr);
            self.watch_registry.cleanup_watcher(addr);
            self.supervisor_registry.cleanup(addr);
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn on_spawn(&self, child: ActorAddress, parent: Option<ActorAddress>, env: Environment, uptime_ms: u64) -> Environment {
        // Register parent → child relationship for orphan cleanup
        if let Some(parent_addr) = parent {
            self.children_registry.register(parent_addr, child);
        }
        let env = self.service_registry.inject_into(env);
        EnvironmentBuilder::from_env(&env)
            .set(SpawnTimestamp(uptime_ms))
            .build()
    }

    fn create_worker_extension(&self) -> Option<Box<dyn WorkerExtension>> {
        Some(Box::new(TimerWheel::new()))
    }
}
