# Wasm Actor Crate — Development History

> Adds a new crate (`crates/bin-runner/`) that runs WebAssembly guest code
> **inside** a swactor actor. The Wasm instance lives in the actor — not as a
> separate OS process. Messages arrive as bytes, get written into Wasm linear
> memory, and the guest's `handle` export is called.
>
> ~350 lines of Rust (host) · 3 guest modules · 7 tests

---

## Table of Contents

1. [Overview & Motivation](#1-overview--motivation)
2. [What Was Built](#2-what-was-built)
3. [Guest ↔ Host Contract](#3-guest--host-contract)
4. [Handle Cycle (Hot Path)](#4-handle-cycle-hot-path)
5. [Guest Modules](#5-guest-modules)
6. [Design Decisions & Tradeoffs](#6-design-decisions--tradeoffs)
7. [Known Gaps & Future Improvements](#7-known-gaps--future-improvements)
8. [Test Coverage Summary](#8-test-coverage-summary)

---

## 1. Overview & Motivation

Swactor already supported running *inside* a browser via `crates/wasm/`
(wasm-bindgen). This crate flips the direction: run untrusted Wasm code
*inside* an actor, sandboxed by wasmtime. Use cases include user-defined
plugins, multi-language actors, and capability-restricted compute.

The main swactor crate has no wasmtime dependency — all Wasm machinery is
isolated in `crates/bin-runner/`.

---

## 2. What Was Built

| Component | Location | Purpose |
|-----------|----------|---------|
| `swactor-bin-runner` crate | `crates/bin-runner/` | Host-side: engine, builder, actor impl |
| 3 guest crates | `crates/bin-runner/tests/guests/{echo,double,silent}/` | `#![no_std]` Wasm modules for testing |
| Integration tests | `crates/bin-runner/tests/wasm_actor.rs` | 7 behavioral tests |

### Crate modules

```
crates/bin-runner/src/
  lib.rs        — ByteMessage, re-exports
  engine.rs     — SharedEngine (Arc<wasmtime::Engine>)
  builder.rs    — WasmActorBuilder (compile + link + instantiate)
  actor.rs      — WasmActor implementing ActorInterface
  error.rs      — WasmActorError enum
```

### Public types

- **`ByteMessage(pub Vec<u8>)`** — message type for Wasm actors. Satisfies
  `Message` bounds trivially.
- **`SharedEngine`** — wraps `Arc<wasmtime::Engine>`. Created once, cloned
  cheaply across actors. Sandboxed config: no threads, no SIMD, no reference
  types.
- **`WasmActorBuilder`** — takes an engine + raw `.wasm` bytes, compiles the
  module, links the `swactor.send` host import, extracts typed function handles,
  returns a `WasmActor`.
- **`WasmActor`** — implements `ActorInterface<Incoming = ByteMessage, Response = ()>`.
- **`WasmActorError`** — `MissingExport(&'static str)` or `Wasmtime(wasmtime::Error)`.

---

## 3. Guest ↔ Host Contract

**Guest must export:**

| Export | Signature | Purpose |
|--------|-----------|---------|
| `memory` | WebAssembly linear memory | Host reads/writes message bytes here |
| `alloc` | `(size: i32) -> i32` | Allocate `size` bytes, return pointer |
| `handle` | `(ptr: i32, len: i32)` | Process message at `(ptr, len)` |

**Guest may import:**

| Import | Module | Signature | Purpose |
|--------|--------|-----------|---------|
| `send` | `swactor` | `(dest_ptr: i32, payload_ptr: i32, payload_len: i32)` | Send a message to another actor |

`dest_ptr` points to 32 bytes of `ActorAddress` in guest linear memory.
`payload_ptr` + `payload_len` describe the message bytes.

---

## 4. Handle Cycle (Hot Path)

```
  ByteMessage arrives
       │
       v
  1. host calls guest alloc(msg.len) → ptr
       │
       v
  2. host writes msg bytes into guest memory at ptr
       │
       v
  3. host calls guest handle(ptr, len)
       │
       ├── guest may call swactor.send() N times
       │   └── each appends (ActorAddress, Vec<u8>) to HostState.outbox
       │
       v
  4. host drains outbox → ctx.send(dest, ByteMessage(payload)) for each
```

Traps during `alloc` or `handle` will panic. Swactor's existing
`catch_unwind` in `tick_all` poisons the actor — consistent with the
panic-safety model.

---

## 5. Guest Modules

Three `#![no_std]` Rust crates compiled to `wasm32-unknown-unknown`:

| Guest | Behavior | Tests it supports |
|-------|----------|-------------------|
| `echo` | Reads 32-byte dest + payload from message; sends payload back to dest | Echo roundtrip, binary preservation |
| `double` | Same framing; sends payload back **twice** | Multi-send verification |
| `silent` | Receives bytes; does nothing | No-output / no-error baseline |

Each guest uses a simple inline bump allocator (64 KiB heap, 8-byte aligned)
and a `#[panic_handler]` that loops. No external dependencies.

Message framing convention: the first 32 bytes of the `ByteMessage` payload
are the destination `ActorAddress`, followed by the actual message bytes.
This allows guests to send replies without hardcoding addresses.

### Building guests

```bash
rustup target add wasm32-unknown-unknown   # one-time

cd crates/bin-runner/tests/guests/echo   && cargo build --target wasm32-unknown-unknown --release
cd crates/bin-runner/tests/guests/double && cargo build --target wasm32-unknown-unknown --release
cd crates/bin-runner/tests/guests/silent && cargo build --target wasm32-unknown-unknown --release
```

Each guest crate has its own `[workspace]` marker to stay independent of the
root workspace.

---

## 6. Design Decisions & Tradeoffs

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | **wasmtime, not wasmer/wasm3** | Best-maintained, fuel metering support, cranelift JIT |
| 2 | **Raw bytes, not structured messages** | Keeps the boundary simple; framing/serialization is the guest's concern |
| 3 | **Separate crate, not a feature flag** | wasmtime is ~30 crates; most users don't need it in their dependency tree |
| 4 | **Bump allocator in guests** | Zero-dependency, predictable, sufficient for request/response patterns |
| 5 | **Dest address in message payload** | Avoids hardcoded addresses; guests can send to any actor the host tells them about |
| 6 | **Traps = panics (no Result)** | Matches swactor's existing panic-safety model; `catch_unwind` in `tick_all` poisons the actor |
| 7 | **Engine sharing via Arc** | Module compilation is expensive; `SharedEngine` amortizes it across actors |
| 8 | **Maximum sandboxing defaults** | Disabled: threads, SIMD, relaxed SIMD, reference types, multi-value. Enabled: bulk memory (required by most compilers) |

---

## 7. Known Gaps & Future Improvements

| # | Gap | Notes |
|---|-----|-------|
| 1 | **No fuel metering** | wasmtime supports fuel; maps naturally to per-tick actor budgets. Deferred to follow-up. |
| 2 | **No WASI** | No filesystem, network, random, or clock access. Intentional for sandboxing, but limits guest capabilities. |
| 3 | **No guest SDK crate** | The test guests serve as examples. A published `swactor-guest` crate with the alloc/handle/send glue would reduce boilerplate. |
| 4 | **Bump allocator never frees** | Fine for short-lived handle calls, but long-running actors would need a real allocator. |
| 5 | **No pre-compilation cache** | `Module::new()` recompiles every time. wasmtime supports serialized modules for faster cold starts. |
| 6 | **`cargo test -p` doesn't resolve** | Must use `--manifest-path`. Workspace resolution quirk. |

---

## 8. Test Coverage Summary

7 behavioral tests in `crates/bin-runner/tests/wasm_actor.rs`:

| Test | Scenario |
|------|----------|
| `echo_returns_same_payload` | Send bytes → wasm echoes them back to inbox |
| `echo_preserves_binary_payload` | All 256 byte values survive the roundtrip |
| `silent_produces_no_output` | Guest does nothing; no error, no messages |
| `double_sends_two_copies` | One message in → two messages out |
| `missing_alloc_export_returns_error` | WAT module with no exports → `WasmActorError::MissingExport` |
| `shared_engine_serves_multiple_actors` | Two actors from the same `SharedEngine` work independently |
| `native_actor_communicates_with_wasm_actor` | Native Rust actor → WasmActor → inbox (two-tick delivery) |
