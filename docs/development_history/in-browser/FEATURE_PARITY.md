# Feature Parity — Development History

> Stage 4 of the in-browser swactor runtime. Enables swactor-std extensions
> (naming, monitoring, groups) and core actor watching in the wasm crate.

---

## Changes

### swactor-std wasm compilation

- Added `wasm` feature to `crates/std/Cargo.toml` (forwards to `swactor/wasm`)
- Changed swactor dependency to `default-features = false`, forwarding `getrandom`
  feature when active (`getrandom = ["dep:getrandom", "swactor/getrandom"]`)
- Cfg-gated `getrandom::getrandom()` call in `router.rs` `RoutingStrategy::Random`
  — falls back to round-robin when `getrandom` feature is disabled (wasm mode)

### RuntimeNaming: register_name

- Added `register_name(name, addr)` method to `RuntimeNaming` trait and impl
  — allows registering a name for an already-spawned actor from outside the runtime
  — complements existing `spawn_named` (which spawns + registers atomically)

### Core watching fix: StopSignal death notifications

- Fixed gap in `worker.rs` tick_all: externally-stopped actors (via `rt.stop_actor()`)
  were not added to the `deaths` list, so core WatchRegistry (phase 5b) never fired
  for them. Added `deaths.push((addr, ExitReason::Stopped))` when StopSignal is
  intercepted (line 737). All 140 existing native tests continue to pass.

### WasmRuntime: StdExtension + new APIs

- `WasmRuntime::new()` now installs `StdExtension` automatically
- New inbox type: `WasmInboxString` for receiving string notifications
- **Naming API**: `register_name`, `where_is`, `unregister_name`, `registered_names`
- **Groups API**: `join_group`, `leave_group`, `publish_to_group_u32`,
  `group_member_count`, `group_names`
- **Stats API**: `total_messages`, `total_panics` (returned as f64 for JS compat)
- New demo actors:
  - `Sentinel` — watches a target via `ctx.watch()`, reports death to string inbox
  - `GroupMember` — joins a group on start, forwards u32 messages to report inbox

### Design Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | StdExtension always installed | Browser runtime should have full naming/groups by default |
| 2 | Stats as f64, not u64 | wasm-bindgen maps u64 to BigInt which JSON.stringify rejects |
| 3 | Sentinel actor for watching | Demonstrates core watching from JS without exposing Watch API directly |
| 4 | register_name on RuntimeNaming | Needed for post-spawn registration from JS (no actor context available) |
| 5 | Round-robin fallback for Random routing | wasm mode disables getrandom; graceful degradation preferred |

## Test Coverage

22 new assertions across 10 new test scenarios (30 total, from 10):

| Test | Scenario |
|------|----------|
| naming — register_name and where_is | Register name, resolve, verify not-found returns undefined |
| naming — unregister_name | Unregister returns previous addr, name no longer resolves |
| naming — registered_names | Lists all registered names as CSV |
| naming — duplicate name rejected | Second registration with same name fails |
| groups — join_group and publish_to_group_u32 | Two members receive broadcast message |
| groups — leave_group | Member count decreases after leave |
| groups — group_names | Lists all active group names |
| watching — sentinel reports actor death | Stop target → sentinel receives death notification |
| stats — total_messages | Counts processed messages across workers |
| stats — total_panics starts at zero | Fresh runtime has zero panics |

## Verification

- `cargo test -p swactor -p swactor-std` — 157 native tests pass (no regressions)
- `cargo build --target wasm32-unknown-unknown -p wasm` — compiles
- `wasm-pack build --target nodejs` in `crates/wasm/` — builds pkg/
- `node test.mjs` in `crates/wasm/` — 30/30 tests pass
