# Runtime Guarantees

This document describes the beta core-runtime guarantees that remain after the
alpha-only `std` features were pruned. Tests now separate core correctness from
optional library patterns.

## Verification layers

1. Unit/integration tests exercise observable runtime behavior.
2. Kani/model-check modules cover core finite-state lifecycle decisions where
   enabled.
3. Exhaustive correspondence tests enumerate production decision-function truth
   tables and compare them with runtime behavior.
4. `tests/core_extension_seams.rs` verifies the generic extension seam without
   depending on unused std features.

## Core decision functions

| Function | Defined in | Runtime use |
| --- | --- | --- |
| `should_skip_actor(poisoned, stopping, suspended) -> bool` | `worker.rs` | `tick_all` skips actors that must not process mailbox messages. |
| `is_on_stop_eligible(stopping, poisoned) -> bool` | `worker.rs` | Cleanup decides whether `on_stop` should run. |
| `determine_stop_reason(poisoned, has_exit_value) -> StopReason` | `worker.rs` | Cleanup reports normal, panic, or completed exits. |

## Retained core guarantees

### G4: Lifecycle ordering

Actors process messages only while eligible. Stopping actors do not handle later
mailbox messages, poisoned actors do not run `on_stop`, and graceful stops run
`on_stop` exactly once.

Evidence:
- `src/guarantees/correspondence.rs`
- `src/guarantees/g4_lifecycle.rs` when Kani is enabled
- `src/guarantees/stateright_lifecycle.rs`

### G5: Fault isolation

A panicking actor is removed/poisoned without preventing unrelated actors from
continuing to process messages.

Evidence:
- `src/guarantees/g5_fault_isolation.rs`
- `tests/actor_lifecycle.rs`

### Core extension seam correctness

The core runtime correctly invokes extension hooks independent of any specific
std feature:

- `RuntimeExtension::on_spawn` can mutate the spawned actor environment.
- `RuntimeExtension::on_actor_death` can return messages, and core routes them
  normally.
- `RuntimeExtension::cleanup_dead` receives dead actor batches.
- `RuntimeExtension::create_worker_extension` installs a per-worker extension.
- `WorkerExtension::handle_request`, `on_tick`, `gc_dead`, and
  `has_pending_work` participate in worker progress and message routing.

Evidence:
- `tests/core_extension_seams.rs`

## Retained std beta guarantees

Only std code used by production crates remains in the beta surface:

- runtime naming registration, lookup, unregister, listing, and dead-actor cleanup
- runtime groups join, leave, publish, membership listing, and dead-actor cleanup
- actor-side `ctx.watch(target)` death notifications via `ActorExited`
- actor-side `ctx.join_group(group)` membership

Evidence:
- `tests/std_extension.rs`

## Removed alpha-only guarantees

The following were tied to unused `std` features and are no longer part of the
beta guarantee set:

- monitor/`Down` delivery and demonitor cancellation
- tick timers and interval timers
- supervisor restart policies and strategies
- router distribution/replacement/meltdown behavior
- service/resource injection and typed resource handles
- std wrapper traits for lifecycle, lineage, capabilities, system info,
  self-stats, and environment access
- supervised-orphan distinction

If one of these features becomes production-used again, reintroduce it with a
focused beta API and fresh correctness tests for that feature.
