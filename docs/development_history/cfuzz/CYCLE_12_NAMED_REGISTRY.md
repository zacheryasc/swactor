# Cycle 12: Named Actor Registry with Auto-Cleanup — Development History

> Commit: `66a8523` · 6 files · 267 insertions, 7 deletions

---

## Motivation

Actors in swactor were only addressable by opaque `ActorAddress` values returned from spawn. There was no way to look up an actor by name — callers needed to pass addresses around manually. Named registration is one of the most fundamental actor runtime features, enabling service discovery within a runtime.

## Competitor Analysis

| Framework | Key Type | Storage | Scope | Auto-Cleanup |
|-----------|----------|---------|-------|-------------|
| Erlang | Atom | ETS table | Per-node or global | Yes (on process exit) |
| Actix | TypeId | SystemRegistry | Per-Arbiter | Yes (on actor stop) |
| Bastion | Path | Hierarchy | Global | Yes (structural) |
| Ractor | String | DashMap (global static) | Global | Yes (on actor death) |
| xactor | TypeId | Singleton registry | Global | N/A (singletons) |
| Akka | ServiceKey[T] | Receptionist | Cluster-wide | Yes (via DeathWatch) |
| **Swactor** | **String** | **RwLock\<HashMap\>** | **Per-runtime** | **Yes (on death)** |

### Key Findings
- **TypeId keys** (Actix, xactor) don't fit swactor's type-erased model — multiple actors of the same type can't share a TypeId key
- **Global static** (Ractor) breaks multi-runtime scenarios (tests, embedding)
- **Erlang's `register/whereis`** is the gold standard: atom keys, per-node scope, automatic cleanup on process exit

## Implementation

### NameRegistry
- `NameRegistry` in `delivery.rs` with forward + reverse maps:
  - `names: RwLock<HashMap<String, ActorAddress>>` — name → address lookup
  - `addrs: RwLock<HashMap<ActorAddress, String>>` — address → name (for O(1) cleanup)
- Added to `Runtime` as `Arc<NameRegistry>`, threaded through `TickContext`

### Runtime API
- `spawn_named(name, actor)` — spawn and register atomically
- `where_is(name)` — look up address by name
- `unregister(name)` — manual unregistration (actor keeps running)
- `registered_names()` — list all registered names

### Context API
- `ctx.spawn_named(name, actor)` — register from within a handler
- `ctx.where_is(name)` — look up from within a handler

### Auto-Cleanup
- `cleanup_dead` phase calls `name_registry.unregister_by_addr()` for each dead actor
- Name is freed immediately — can be reused for a replacement actor

### TOCTOU Prevention
- Name reservation is immediate (before spawn queue push) — prevents race between checking name availability and registering it

**Key files modified:** `src/delivery.rs`, `src/runtime.rs`, `src/actor.rs`, `src/worker.rs`, `tests/runtime_api.rs`

## Design Decisions

- **String keys** — most flexible. Atoms (Erlang) aren't idiomatic in Rust. TypeId (Actix) is too restrictive. Strings allow any naming convention.
- **Per-runtime scope** — matches swactor's architecture (one runtime per application). Global registries (Ractor) cause problems in tests and embedded scenarios.
- **RwLock\<HashMap\>** — matches the existing `AddressMap` and `InboxRegistry` pattern. RwLock allows concurrent reads (lookups) with exclusive writes (registration).
- **Collision returns error** — `spawn_named` returns `Err` if the name is already taken. The original binding is preserved. This is explicit and predictable, matching Erlang's behavior.
- **Reverse map for O(1) cleanup** — without the reverse map, cleanup would require scanning all entries. The reverse map adds memory proportional to registered actors but makes cleanup constant-time.
- **Immediate reservation** — name is reserved before the spawn is queued, preventing TOCTOU races where two `spawn_named` calls for the same name could both succeed.

## Tests Added

11 new tests (95 → 106 total):

- `named_actor_lookup_returns_spawn_address` — spawn_named → where_is roundtrip
- `named_actor_receives_messages_via_lookup` — send to looked-up address works
- `duplicate_name_returns_error` — collision error, original binding preserved
- `where_is_returns_none_for_unknown_name` — nonexistent name → None
- `name_auto_unregistered_on_actor_death` — stop_actor → name freed
- `name_can_be_reused_after_actor_death` — death → respawn with same name succeeds
- `name_auto_unregistered_on_panic` — panic → name freed
- `registered_names_lists_all` — all registered names returned
- `manual_unregister_frees_name_but_actor_lives` — unregister doesn't kill the actor
- `ctx_where_is_resolves_inside_handler` — where_is works from handler context
- `ctx_spawn_named_registers_from_handler` — spawn_named works from handler context

## Result

- 106 tests pass (99 behavioral + 7 proptest)
- All workspace crates compile, zero warnings
