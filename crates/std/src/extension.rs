use std::any::Any;

use swactor::actor::{ActorAddress, Down, StopReason};
use swactor::extension::RuntimeExtension;

use crate::group_registry::GroupRegistry;
use crate::monitor_registry::MonitorRegistry;
use crate::name_registry::NameRegistry;

/// Standard library extension — provides naming, monitoring, and group registries.
///
/// Install on a `Runtime` via `runtime.with_extension(Arc::new(StdExtension::new()))`.
pub struct StdExtension {
    pub(crate) name_registry: NameRegistry,
    pub(crate) monitor_registry: MonitorRegistry,
    pub(crate) group_registry: GroupRegistry,
}

impl StdExtension {
    pub fn new() -> Self {
        Self {
            name_registry: NameRegistry::new(),
            monitor_registry: MonitorRegistry::new(),
            group_registry: GroupRegistry::new(),
        }
    }
}

impl Default for StdExtension {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeExtension for StdExtension {
    fn on_actor_death(
        &self,
        dead: &[(ActorAddress, StopReason)],
    ) -> Vec<(ActorAddress, Box<dyn Any + Send>)> {
        let mut notifications = Vec::new();
        for &(addr, reason) in dead {
            let watchers = self.monitor_registry.take_monitors(&addr);
            for (_mref, watcher) in watchers {
                let down = Down { addr, reason };
                notifications.push((watcher, Box::new(down) as Box<dyn Any + Send>));
            }
        }
        notifications
    }

    fn cleanup_dead(&self, dead: &[ActorAddress]) {
        for addr in dead {
            self.name_registry.unregister_by_addr(addr);
            self.group_registry.cleanup(addr);
            self.monitor_registry.remove_watcher(addr);
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
