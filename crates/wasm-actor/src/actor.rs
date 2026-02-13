use swactor::actor::{ActorInterface, Ctx};
use wasmtime::{Memory, Store, TypedFunc};

use crate::ByteMessage;

/// State accessible to host functions during guest execution.
#[derive(Default)]
pub(crate) struct HostState {
    pub outbox: Vec<(swactor::actor::ActorAddress, Vec<u8>)>,
}

/// An actor whose logic is defined by a WebAssembly guest module.
///
/// Messages arrive as [`ByteMessage`], are copied into Wasm linear memory,
/// and processed by the guest's `handle` export. The guest can send messages
/// back via the `swactor.send` host import.
pub struct WasmActor {
    pub(crate) store: Store<HostState>,
    pub(crate) memory: Memory,
    pub(crate) alloc: TypedFunc<i32, i32>,
    pub(crate) handle: TypedFunc<(i32, i32), ()>,
}

impl ActorInterface for WasmActor {
    type Incoming = ByteMessage;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: ByteMessage) {
        let bytes = &msg.0;
        let len: i32 = match i32::try_from(bytes.len()) {
            Ok(n) => n,
            Err(_) => return, // message too large for i32 ABI
        };

        // 1. Allocate space in guest memory
        let ptr = match self.alloc.call(&mut self.store, len) {
            Ok(ptr) if ptr < 0 => return,        // invalid pointer
            Ok(0) if len > 0 => return,           // OOM — drop message
            Ok(ptr) => ptr,
            Err(_) => return,                     // alloc trapped — drop message
        };

        // 2. Write message bytes into guest memory
        let mem = self.memory.data_mut(&mut self.store);
        let end = (ptr as usize).saturating_add(bytes.len());
        if end > mem.len() {
            return; // alloc returned OOB pointer — drop message
        }
        mem[ptr as usize..end].copy_from_slice(bytes);

        // 3. Call guest handle
        if self.handle.call(&mut self.store, (ptr, len)).is_err() {
            self.store.data_mut().outbox.clear(); // discard sends from incomplete operation
            return; // handle trapped — drop message, keep actor alive
        }

        // 4. Drain outbox → send via ctx
        let outbox: Vec<_> = self.store.data_mut().outbox.drain(..).collect();
        for (dest, payload) in outbox {
            let _ = ctx.send(dest, ByteMessage(payload));
        }
    }
}
