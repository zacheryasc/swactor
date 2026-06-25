use std::any::Any;

use crate::actor::{ActorAddress, Environment, ExitReason, ExitValue, StopReason};
use crate::extension::RuntimeExtension;

use super::group_registry::GroupRegistry;
use super::name_registry::NameRegistry;
use super::watch_registry::WatchRegistry;

/// Standard library extension — provides naming, watching, and group registries.
///
/// Install on a `Runtime` via `runtime.with_extension(Arc::new(StdExtension::new()))`.
pub struct StdExtension {
    pub(crate) name_registry: NameRegistry,
    pub(crate) watch_registry: WatchRegistry,
    pub(crate) group_registry: GroupRegistry,
}

impl StdExtension {
    pub fn new() -> Self {
        Self {
            name_registry: NameRegistry::new(),
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
        StopReason::Normal | StopReason::Completed => ExitReason::Completed,
        StopReason::Panicked => ExitReason::Panicked,
    }
}

impl RuntimeExtension for StdExtension {
    fn on_actor_death(
        &self,
        dead: &[(ActorAddress, StopReason, Option<ExitValue>)],
    ) -> Vec<(ActorAddress, Box<dyn Any + Send>)> {
        let mut notifications = Vec::new();

        for (addr, reason, exit_value) in dead {
            let watch_notifications =
                self.watch_registry
                    .notify_death(*addr, stop_to_exit(*reason), exit_value.clone());
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
            self.watch_registry.cleanup_watcher(addr);
        }
    }

    fn on_spawn(
        &self,
        _child: ActorAddress,
        _parent: Option<ActorAddress>,
        env: Environment,
        _uptime_ms: u64,
    ) -> Environment {
        env
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
