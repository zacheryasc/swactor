# Wasm Actor

The `swactor-wasm-actor` crate runs WebAssembly guest code inside a swactor
actor. The Wasm instance is sandboxed by [wasmtime](https://wasmtime.dev/).

## Architecture

```
  ┌─ Runtime ──────────────────────────────────────────────────────────────┐
  │                                                                        │
  │  ┌─ WasmActor ──────────────────────────────────────────────────────┐  │
  │  │                                                                  │  │
  │  │  Store<HostState>    -- wasmtime store with outbox               │  │
  │  │  Memory              -- guest linear memory                      │  │
  │  │  alloc: TypedFunc    -- guest allocator                          │  │
  │  │  handle: TypedFunc   -- guest message handler                    │  │
  │  │                                                                  │  │
  │  │  impl ActorInterface for WasmActor                               │  │
  │  │    Incoming = ByteMessage                                        │  │
  │  │    Response = ()                                                 │  │
  │  │                                                                  │  │
  │  └──────────────────────────────────────────────────────────────────┘  │
  │                                                                        │
  │  ┌─ Native Actors ─────────────────────────────────────────────────┐   │
  │  │  (can exchange ByteMessage with WasmActors normally)            │   │
  │  └─────────────────────────────────────────────────────────────────┘   │
  │                                                                        │
  └────────────────────────────────────────────────────────────────────────┘
```

## Message Flow

```
  Host                          Guest (Wasm)
  ────                          ────────────

  ByteMessage arrives
       │
       ├─1─ call alloc(len) ──────────► bump-allocate, return ptr
       │
       ├─2─ write bytes at ptr ───────► (memory updated)
       │
       ├─3─ call handle(ptr, len) ────► process message
       │                                   │
       │    ◄── swactor.send() ────────────┤  (0..N times)
       │    (buffered in HostState.outbox)  │
       │                                   │
       ├─4─ drain outbox ◄────────────── handle returns
       │
       v
  ctx.send(dest, ByteMessage) for each outbox entry
```

## Guest Contract

Guests are standalone `wasm32-unknown-unknown` modules. They export three
symbols and may import one:

| Direction | Module | Symbol | Signature |
|-----------|--------|--------|-----------|
| **export** | — | `memory` | linear memory |
| **export** | — | `alloc` | `(i32) -> i32` |
| **export** | — | `handle` | `(i32, i32) -> ()` |
| **import** | `swactor` | `send` | `(i32, i32, i32) -> ()` |

The `send` import takes `(dest_ptr, payload_ptr, payload_len)` where
`dest_ptr` points to a 32-byte `ActorAddress` in guest memory.

## Usage

```rust
use swactor::runtime::{Runtime, RuntimeConfig};
use swactor_wasm_actor::{ByteMessage, SharedEngine, WasmActorBuilder};

// Create a shared engine (once)
let engine = SharedEngine::new().unwrap();

// Build an actor from .wasm bytes
let wasm_bytes = std::fs::read("my_guest.wasm").unwrap();
let actor = WasmActorBuilder::new(engine, wasm_bytes)
    .build()
    .unwrap();

// Use it like any other actor
let rt = Runtime::new(RuntimeConfig::default());
let addr = rt.spawn(actor).unwrap();
rt.send_to(addr, ByteMessage(b"hello".to_vec())).unwrap();
rt.tick();
```

## Sandboxing

The `SharedEngine` disables all optional Wasm proposals:

- Threads — disabled
- SIMD / relaxed SIMD — disabled
- Reference types — disabled
- Multi-value — disabled
- Bulk memory — **enabled** (required by most Rust/LLVM toolchains)

No WASI imports are linked. Guests have no access to the filesystem, network,
clock, or random number generator. The only host function available is
`swactor.send`.

## Where Things Live

| File | Purpose |
|------|---------|
| `crates/wasm-actor/src/lib.rs` | `ByteMessage` + re-exports |
| `crates/wasm-actor/src/engine.rs` | `SharedEngine` — sandboxed wasmtime config |
| `crates/wasm-actor/src/builder.rs` | `WasmActorBuilder` — compile, link, instantiate |
| `crates/wasm-actor/src/actor.rs` | `WasmActor` — `ActorInterface` impl |
| `crates/wasm-actor/src/error.rs` | `WasmActorError` |
| `crates/wasm-actor/tests/guests/` | Three test guest crates (echo, double, silent) |
| `crates/wasm-actor/tests/wasm_actor.rs` | 7 integration tests |
