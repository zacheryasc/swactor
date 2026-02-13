use std::any::Any;

use swactor::actor::{ActorAddress, Down, ExitReason, StopReason};
use swactor::extension::{RuntimeExtension, WorkerExtension};

use crate::group_registry::GroupRegistry;
use crate::monitor_registry::MonitorRegistry;
use crate::name_registry::NameRegistry;
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
}

impl StdExtension {
    pub fn new() -> Self {
        Self {
            name_registry: NameRegistry::new(),
            monitor_registry: MonitorRegistry::new(),
            watch_registry: WatchRegistry::new(),
            group_registry: GroupRegistry::new(),
        }
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
    }
}

impl RuntimeExtension for StdExtension {
    fn on_actor_death(
        &self,
        dead: &[(ActorAddress, StopReason)],
    ) -> Vec<(ActorAddress, Box<dyn Any + Send>)> {
        let mut notifications = Vec::new();

        for &(addr, reason) in dead {
            // Monitor notifications (Down)
            let watchers = self.monitor_registry.take_monitors(&addr);
            for (_mref, watcher) in watchers {
                let down = Down { addr, reason };
                notifications.push((watcher, Box::new(down) as Box<dyn Any + Send>));
            }

            // Watch notifications (ActorExited)
            let watch_notifications = self.watch_registry.notify_death(addr, stop_to_exit(reason));
            for (watcher, exited) in watch_notifications {
                notifications.push((watcher, Box::new(exited) as Box<dyn Any + Send>));
            }
        }

        notifications
    }

    fn cleanup_dead(&self, dead: &[ActorAddress]) {
        for addr in dead {
            self.name_registry.unregister_by_addr(addr);
            self.group_registry.cleanup(addr);
            self.monitor_registry.remove_watcher(addr);
            self.watch_registry.cleanup_watcher(addr);
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create_worker_extension(&self) -> Option<Box<dyn WorkerExtension>> {
        Some(Box::new(TimerWheel::new()))
    }
}
