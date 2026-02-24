use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::actor::{Environment, EnvironmentBuilder};

/// Stores typed service bindings for injection into actor environments.
///
/// Bindings are registered at the runtime level (e.g., during startup) and
/// automatically injected into every actor's environment via the `on_spawn`
/// hook. Existing environment keys are **not** overwritten — this preserves
/// per-subtree overrides set via `spawn_builder`.
pub struct ServiceRegistry {
    bindings: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self {
            bindings: RwLock::new(HashMap::new()),
        }
    }

    /// Register a service binding by marker type `S`.
    ///
    /// Overwrites any previous binding for the same marker type.
    pub fn register<S: 'static + Send + Sync>(&self, addr: crate::actor::ActorAddress) {
        let binding = crate::actor::ServiceBinding::<S>::new(addr);
        let type_id = TypeId::of::<crate::actor::ServiceBinding<S>>();
        self.bindings
            .write()
            .unwrap()
            .insert(type_id, Arc::new(binding));
    }

    /// Merge all registered bindings into an environment, skipping keys
    /// that are already present (preserves spawn_builder overrides).
    pub fn inject_into(&self, env: Environment) -> Environment {
        let bindings = self.bindings.read().unwrap();
        if bindings.is_empty() {
            return env;
        }

        let mut builder = EnvironmentBuilder::from_env(&env);
        for (&type_id, value) in bindings.iter() {
            if !env.contains_type_id(type_id) {
                builder.set_raw(type_id, Arc::clone(value));
            }
        }
        builder.build()
    }
}
