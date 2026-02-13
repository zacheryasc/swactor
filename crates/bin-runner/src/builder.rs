use swactor::actor::ActorAddress;
use wasmtime::{Linker, Module, Store, TypedFunc};

use crate::actor::{HostState, WasmActor};
use crate::engine::SharedEngine;
use crate::error::WasmActorError;

/// Compiles a Wasm module and produces a ready-to-use [`WasmActor`].
pub struct WasmActorBuilder {
    engine: SharedEngine,
    wasm_bytes: Vec<u8>,
}

impl WasmActorBuilder {
    pub fn new(engine: SharedEngine, wasm_bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            engine,
            wasm_bytes: wasm_bytes.into(),
        }
    }

    /// Compile the module, link host functions, and instantiate.
    pub fn build(self) -> Result<WasmActor, WasmActorError> {
        let engine = self.engine.inner();
        let module = Module::new(engine, &self.wasm_bytes)?;

        let mut linker: Linker<HostState> = Linker::new(engine);
        Self::link_send(&mut linker)?;

        let mut store = Store::new(engine, HostState::default());
        let instance = linker.instantiate(&mut store, &module)?;

        // Extract required exports
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or(WasmActorError::MissingExport("memory"))?;

        let alloc: TypedFunc<i32, i32> = instance
            .get_typed_func(&mut store, "alloc")
            .map_err(|_| WasmActorError::MissingExport("alloc"))?;

        let handle: TypedFunc<(i32, i32), ()> = instance
            .get_typed_func(&mut store, "handle")
            .map_err(|_| WasmActorError::MissingExport("handle"))?;

        Ok(WasmActor {
            store,
            memory,
            alloc,
            handle,
        })
    }

    /// Link the `swactor.send` host import.
    fn link_send(linker: &mut Linker<HostState>) -> Result<(), WasmActorError> {
        linker.func_wrap(
            "swactor",
            "send",
            |mut caller: wasmtime::Caller<'_, HostState>,
             dest_ptr: i32,
             payload_ptr: i32,
             payload_len: i32|
             -> Result<(), wasmtime::Error> {
                let mem = caller
                    .get_export("memory")
                    .and_then(|e| e.into_memory())
                    .ok_or_else(|| wasmtime::Error::msg("guest must export memory"))?;
                let data = mem.data(&caller);
                let mem_len = data.len();

                // Validate non-negative arguments
                if dest_ptr < 0 || payload_ptr < 0 || payload_len < 0 {
                    return Err(wasmtime::Error::msg(
                        "negative argument in swactor.send",
                    ));
                }

                let dest_ptr = dest_ptr as usize;
                let payload_ptr = payload_ptr as usize;
                let payload_len = payload_len as usize;

                // Bounds-check with overflow protection
                let dest_end = dest_ptr
                    .checked_add(32)
                    .ok_or_else(|| wasmtime::Error::msg("dest_ptr overflow"))?;
                let payload_end = payload_ptr
                    .checked_add(payload_len)
                    .ok_or_else(|| wasmtime::Error::msg("payload range overflow"))?;
                if dest_end > mem_len || payload_end > mem_len {
                    return Err(wasmtime::Error::msg(
                        "out-of-bounds memory access in swactor.send",
                    ));
                }

                // Read 32-byte destination address
                let mut addr_bytes = [0u8; 32];
                addr_bytes.copy_from_slice(&data[dest_ptr..dest_end]);
                let dest = ActorAddress(addr_bytes);

                // Read payload
                let payload = data[payload_ptr..payload_end].to_vec();

                caller.data_mut().outbox.push((dest, payload));
                Ok(())
            },
        )?;
        Ok(())
    }
}
