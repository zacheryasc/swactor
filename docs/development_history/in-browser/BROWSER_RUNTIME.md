# Browser Runtime API — Development History

> Stage 2 of the in-browser swactor runtime. Replaces the hardcoded PoC with
> a generic, type-safe API using opaque address handles and typed inboxes.

---

## Changes

### Core Types

**`WasmRuntime`** — wraps `swactor::Runtime` in single-threaded mode.
Methods: `tick()`, `actor_count()`, `send_u32()`, `send_bytes()`,
`stop_actor()`, `uptime_ms()`, `new_inbox_u32()`, `new_inbox_bytes()`.
Also exposes `runtime()` for Rust-side custom spawn functions.

**`WasmAddr`** — opaque handle wrapping `ActorAddress`. Returned by spawn
functions, passed to send functions. JS holds it as an opaque object.
Has `toString()` for debugging.

**`WasmInboxU32`** / **`WasmInboxBytes`** — typed inboxes for receiving
results from actors. Each has `addr()` → `WasmAddr` (so actors know where
to send) and `try_recv()` → `Option<T>`.

### Design Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | Opaque `WasmAddr` handles instead of indices | Type-safe, stable identity, no out-of-bounds errors |
| 2 | Typed inbox types instead of generic `Inbox<T>` | wasm-bindgen doesn't support generics; concrete types are explicit |
| 3 | Free-standing `spawn_*` functions, not methods | Each actor type gets its own spawn function with typed args |
| 4 | `send_u32`/`send_bytes` on runtime | Common send types; custom types use typed spawn wrappers |
| 5 | Evolved existing `crates/wasm/` instead of new crate | Less churn, existing build/test infrastructure |

### Actor Pattern

Users expose actors to JS by writing one `#[wasm_bindgen]` spawn function
per actor type:

```rust
#[wasm_bindgen]
pub fn spawn_my_actor(rt: &WasmRuntime, arg: JsValue) -> WasmAddr {
    let actor = MyActor::from_js(arg);
    let addr = rt.runtime().spawn(actor).unwrap();
    WasmAddr(addr)
}
```

## Test Coverage

10 Node.js tests in `crates/wasm/test.mjs`:

| Test | Scenario |
|------|----------|
| accumulator | Counter processes messages, reports running totals to inbox |
| relay | Relay forwards messages to counter (cross-actor, 2 ticks) |
| multiple counters | Two independent counters report to same inbox |
| actor_count | Spawning 3 actors reflects in stats |
| WasmAddr toString | Address has non-empty debug representation |
| stop_actor | Graceful stop removes actor from runtime |
| bytes inbox | WasmInboxBytes receives Uint8Array correctly |
| uptime_ms | Returns non-negative number |

## Verification

- `cargo test -p swactor` — native tests pass (no regressions)
- `cargo build --target wasm32-unknown-unknown -p wasm` — compiles
- `wasm-pack build --target nodejs` in `crates/wasm/` — builds pkg/
- `node test.mjs` in `crates/wasm/` — 10/10 tests pass
