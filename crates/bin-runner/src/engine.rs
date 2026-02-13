use std::sync::Arc;

use wasmtime::Engine;

/// A shared, cheaply-cloneable Wasm engine.
///
/// Created once and reused across multiple [`WasmActor`](crate::WasmActor) instances.
/// Configured with maximum sandboxing — no threads, no SIMD, no reference types.
#[derive(Clone)]
pub struct SharedEngine(Arc<Engine>);

impl std::fmt::Debug for SharedEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SharedEngine").field(&"<Engine>").finish()
    }
}

impl SharedEngine {
    /// Create a new engine with sandboxed defaults.
    pub fn new() -> Result<Self, wasmtime::Error> {
        let mut config = wasmtime::Config::new();
        config.wasm_threads(false);
        config.wasm_simd(false);
        config.wasm_relaxed_simd(false);
        config.wasm_reference_types(false);
        config.wasm_multi_value(false);
        config.wasm_bulk_memory(true);
        let engine = Engine::new(&config)?;
        Ok(Self(Arc::new(engine)))
    }

    pub(crate) fn inner(&self) -> &Engine {
        &self.0
    }
}
