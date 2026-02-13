# Platform Abstraction Layer — Development History

> Stage 1 of the in-browser swactor runtime. Makes core swactor compile for
> `wasm32-unknown-unknown` without behavioral changes on native targets.

---

## Changes

### 1. `web-time` dependency + `wasm` feature flag

**File**: `Cargo.toml`

Added `web-time` as an optional dependency and a `wasm` feature that bundles
`no_random` + `web-time`:

```toml
wasm = ["no_random", "dep:web-time"]
web-time = { version = "0.2", optional = true }
```

`web-time` is a drop-in replacement for `std::time::Instant`:
- Native: re-exports `std::time::Instant` (zero-cost)
- wasm32: uses `performance.now()` via `js-sys`

### 2. Platform-aware `Instant` re-export

**File**: `src/lib.rs`

```rust
#[cfg(feature = "wasm")]
pub(crate) use web_time::Instant;
#[cfg(not(feature = "wasm"))]
pub(crate) use std::time::Instant;
```

All modules (`runtime.rs`, `worker.rs`) now use `crate::Instant` instead of
`std::time::Instant`. Single point of truth — no cfg noise in consumer code.

### 3. cfg-gated `Runtime::run()` and `RuntimeHandle`

**File**: `src/runtime.rs`

`Runtime::run()` calls `std::thread::spawn()` which is not available on wasm32.
Both `run()` and `RuntimeHandle` (which holds `JoinHandle<()>`) are gated:

```rust
#[cfg(not(target_arch = "wasm32"))]
pub fn run(self) -> Result<RuntimeHandle, Error> { ... }
```

On wasm32, the browser crate will provide its own `run()` via Web Workers.
`tick()` remains available on all platforms for single-threaded driving.

### 4. Updated `crates/wasm/` to use `wasm` feature

**File**: `crates/wasm/Cargo.toml`

Changed from `features = ["no_random"]` to `features = ["wasm"]` to pick up
the `web-time` Instant on wasm32.

## What Did NOT Need Abstraction

Key discovery: on wasm32 with the `+atomics` target feature, most of
`std::sync` and `std::thread` works:

- `OnceLock<Thread>` — compiles and works (futex-based)
- `Thread::unpark()` — works (futex → `memory.atomic.notify`)
- `thread::park_timeout()` — works (futex → `memory.atomic.wait32`)
- `thread::yield_now()` — works (no-op on wasm)
- `Mutex`, `RwLock` — work (futex-based)
- `crossbeam-queue` — works (uses `core::sync::atomic`)
- `AtomicBool/Usize/U64` — work (wasm atomic instructions)

Only `std::thread::spawn()` and `JoinHandle` are not functional on wasm32.

## Verification

- `cargo test` — all native tests pass (no regressions)
- `cargo test --features wasm` — all native tests pass with wasm feature
- `cargo build --target wasm32-unknown-unknown --features wasm --no-default-features` — compiles
- `cargo build --target wasm32-unknown-unknown -p wasm` — existing PoC crate compiles
