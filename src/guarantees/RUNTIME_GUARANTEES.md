# Runtime Guarantees

**Date**: 2026-03-19
**Branch**: `runtime-guarantees`
**Enforcement**: Compiler (type system), Kani (bounded model checking), exhaustive correspondence (deterministic enumeration), Stateright-style DFS model checking (exhaustive state exploration)

## Abstract

This document catalogs the guarantees the swactor runtime makes to its users. Each guarantee is a contract: if the runtime compiles and its verification suite passes, the guarantee holds. Guarantees are enforced in layers — the compiler prevents the most fundamental violations statically, Kani proofs exhaust bounded state spaces for production decision functions, deterministic correspondence tests enumerate every reachable input combination, and exhaustive DFS model checking explores every reachable state of the runtime state machines.

A guarantee listed here is a **promise**. Code that violates a guarantee is a bug in the runtime, not in the user's actor.

---

## Enforcement Strategy

Four layers, ordered by strength:

1. **Compiler (type system)** — Make violations unrepresentable. `Send + 'static` bounds, ownership, lack of `&mut` aliasing. Zero runtime cost, impossible to bypass without `unsafe`.

2. **Kani (bounded model checking)** — Symbolically execute all reachable states within bounded inputs. Proves invariants exhaustively for production decision functions (`should_skip_actor`, `is_on_stop_eligible`, `determine_stop_reason`, `should_restart`, `compute_restart_set`). CI cost only.

3. **Exhaustive correspondence (deterministic enumeration)** — Every reachable combination of inputs is tested deterministically via nested loops with explicit enumeration counters. No random sampling (proptest has been removed from correspondence). Covers decision function truth tables and runtime behavioral agreement.

4. **DFS model checking (exhaustive state exploration)** — A minimal inline DFS model checker (`src/guarantees/model_checker.rs`) explores every reachable state of bounded runtime state machines. Properties are checked in every visited state. `always` properties prove safety invariants; `sometimes` properties prove liveness (non-vacuousness). Models cover lifecycle (G4/G5), death notifications and orphan cleanup (G6/G7), and supervisor restart (G8).

A guarantee is **fully contracted** when all applicable layers enforce it.

---

## Verification Architecture

### Production Decision Functions

Core decision logic has been extracted from `tick_all`, `cleanup_dead`, and `Supervisor::handle_down` into standalone pure functions in production code. These are the functions that Kani proves and correspondence tests enumerate:

| Function | Defined in | Called by |
|----------|-----------|-----------|
| `should_skip_actor(poisoned, stopping, suspended) → bool` | `worker.rs` | `tick_all` loop |
| `is_on_stop_eligible(stopping, poisoned) → bool` | `worker.rs` | `cleanup_dead` |
| `determine_stop_reason(poisoned, has_exit_value) → StopReason` | `worker.rs` | `cleanup_dead` |
| `RestartPolicy::should_restart(reason) → bool` | `std/supervisor.rs` | `Supervisor::handle_down` |
| `compute_restart_set(strategy, dead_idx, num_children) → Vec<usize>` | `std/supervisor.rs` | `Supervisor::handle_down` |

Kani proofs and Stateright models call these production functions directly — not test-only mirrors.

### Model Bounds

| Model | Actors/Children | Mailbox/Events | Other bounds | Min. unique states |
|-------|----------------|----------------|--------------|-------------------|
| Lifecycle (G4/G5) | 3 actors | max_handle=2 | — | >100 |
| Monitor (G6) | 3 actors | — | 6 monitor pairs | >100 |
| Orphan (G7) | 4 actors | — | max 3 parent-child links | >100 |
| Supervisor (G8) | 4 children | — | max_restarts=3, max_deaths=4, 243 init states (3 strategies × 3⁴ policies) | >100 |

These bounds are sufficient because:
- The decision functions are pure over small enum/boolean domains — the state space is inherently finite.
- Stateright models explore *every* reachable state via DFS, not a sample. The bounds limit model size to keep exploration tractable while covering all behavioral combinations.
- Liveness canaries (`sometimes` properties) verify that interesting states (panics, restarts, cascading cleanup, meltdown) are actually reachable, preventing vacuous proofs.

---

## Guarantee Catalog

### G1: No Shared Mutable State

> Two actors never hold mutable references to the same memory.

**Status**: Fully contracted (compiler).

**Enforcement**: The `Message` trait requires `'static + Clone + Send + Sync`. Actor state is owned by `Box<dyn AnyActor>` inside `ActorSlot`, which is only accessed by the owning worker's `tick_all`. The `ActorInterface` trait requires `Send + 'static`. Rust's ownership system makes aliased mutable access a compile error.

**No additional verification needed.** This is a language-level guarantee.

---

### G2: Single-Threaded Actor Execution

> An actor's `handle()`, `on_start()`, and `on_stop()` are never called concurrently. No reentrancy.

**Status**: Fully contracted (compiler).

**Enforcement**: `ActorSlot` is stored in `ActorPool`, which is owned (not shared) by a single `Worker`. `tick_all` takes `&mut self` on the pool and iterates actors sequentially. There is no `Arc<Mutex<ActorSlot>>` — the pool is thread-local. An actor cannot be called from two threads because it literally exists on only one thread's stack.

**No additional verification needed.** Structural ownership makes concurrent calls uncompilable.

---

### G3: Actor Identity Uniqueness

> No two live actors share an `ActorAddress`. An address identifies exactly one actor for its lifetime.

**Status**: Fully contracted (compiler + runtime structure).

**Enforcement**: `ActorAddress::new_random()` generates 32 cryptographically random bytes. The `AddressMap` is a `HashMap<ActorAddress, WorkerId>` — duplicate insertion overwrites, but since addresses are 256-bit random, collision probability is ~2^-128 (birthday bound). The address map is the single source of truth for routing; an address not in the map is dead.

---

### G4: Lifecycle Ordering

> For every actor: `on_start()` is called exactly once before the first `handle()`. `on_stop()` is called at most once, after the last `handle()`. No `handle()` calls occur after `on_stop()` or after the actor is poisoned.

**Status**: Fully contracted (compiler + Kani + exhaustive correspondence + DFS model checking).

**Enforcement**: `ActorSlot` has boolean flags `started`, `stopping`, `poisoned`. The production function `should_skip_actor(poisoned, stopping, suspended)` determines whether to skip an actor during `tick_all`. `is_on_stop_eligible(stopping, poisoned)` determines whether `on_stop` fires in `cleanup_dead`.

**Kani** (`src/guarantees/g4_lifecycle.rs`): Five proof harnesses calling production functions:
- `proof_g4a_on_start_exactly_once` — `on_start` fires exactly once before any `handle`.
- `proof_g4b_no_handle_when_stopping_or_poisoned` — `should_skip_actor` prevents handle calls.
- `proof_g4c_on_stop_conditions` — `is_on_stop_eligible` fires only when `stopping && !poisoned`.
- `proof_g4d_no_handle_after_on_stop` — no handle after on_stop.
- `proof_g4e_suspension_pauses_handle` — `should_skip_actor` blocks handle during suspension.

**Exhaustive correspondence** (`src/guarantees/correspondence.rs`): Deterministic enumeration of all 16 boolean flag combinations (2⁴ for `should_skip_actor`, `is_on_stop_eligible`, `determine_stop_reason` truth tables). Runtime behavioral tests verify agreement between decision functions and actual actor behavior for healthy, poisoned, stopping, and panicking actors.

**DFS model checking** (`src/guarantees/stateright_lifecycle.rs`): Exhaustive DFS over 3-actor lifecycle state machine. Properties verified in every reachable state:
- `on_start_count ≤ 1` for every actor
- `handle_count > 0 ⇒ on_start_count == 1`
- Poisoned/stopping actors never increment `handle_count`
- `on_stop_count ≤ 1` for every actor
- `on_stop` only fires when `stopping && !poisoned`
- No handle after on_stop

Liveness canaries confirm reachable states where `handle_count > 0`, `on_stop_count == 1`, and fault isolation (one actor poisoned while another handles).

---

### G5: Fault Isolation

> A panic in actor A does not corrupt actor B's state, skip B's messages, or prevent B's lifecycle hooks from firing.

**Status**: Fully contracted (catch_unwind + structural separation + exhaustive tests + DFS model checking).

**Enforcement**: `handle()` is wrapped in `std::panic::catch_unwind`. On panic, only the panicking actor's slot is marked `poisoned` and its mailbox cleared. Other actors in the same pool are unaffected — iteration continues. Each actor's state is in its own `ActorSlot`; there is no shared mutable structure between slots.

**DFS model checking** (`src/guarantees/stateright_lifecycle.rs`): G5 properties verified in every reachable state:
- A `Panic` action on actor `i` never changes any flag or counter of actor `j ≠ i`.
- After a tick containing a panic for actor `i`, all other actors' `handle_count` reflects their full mailbox drain (not short-circuited).

**Runtime tests** (`src/guarantees/g5_fault_isolation.rs`): Three deterministic scenarios:
- Panic at random index isolates siblings.
- `on_start` panic isolates siblings.
- Multiple panics in same tick isolate non-panicking actors.

---

### G6: Death Notification Completeness

> If actor A monitors actor B (via `monitor()` or `watch()`), and B dies, A receives exactly one `Down` (for monitors) or `ActorExited` (for watchers) notification.

**Status**: Fully contracted (exhaustive tests + DFS model checking).

**Enforcement**: `MonitorRegistry` and `WatchRegistry` in `StdExtension` track monitor/watch relationships. `on_actor_death()` iterates all registered monitors/watchers for the dead actor and emits notifications. `cleanup_dead()` removes the dead actor's entries.

**DFS model checking** (`src/guarantees/stateright_death_orphan.rs`, MonitorModel): Exhaustive DFS over 3-actor monitor model with 6 monitor pairs. Properties verified in every reachable state:
- For every (watcher, watched) pair where watched is dead: notification count == 1.
- For every actor still alive: notification count == 0.
- Demonitored pairs produce zero notifications.

Liveness canaries: monitor fires, demonitor suppresses notification.

**Runtime tests** (`src/guarantees/g6_g7_death_orphan.rs`): Deterministic tests covering monitor notifications, watch notifications, demonitor suppression, and multiple monitors per target.

---

### G7: Orphan Cleanup

> If an actor dies and its children are not supervised, all unsupervised children are stopped.

**Status**: Fully contracted (exhaustive tests + DFS model checking).

**Enforcement**: `ChildrenRegistry` tracks parent-child relationships. On parent death, `cleanup_dead` checks if each child has a supervisor. Unsupervised children receive `StopSignal`.

**DFS model checking** (`src/guarantees/stateright_death_orphan.rs`, OrphanModel): Exhaustive DFS over 4-actor orphan model with max 3 parent-child links. Properties verified in every reachable state:
- After orphan cleanup, all unsupervised children of the dead parent are dead.
- Supervised children survive orphan cleanup.
- Cascading: if an orphan-cleaned parent's child also dies and is orphan-cleaned, its unsupervised children are dead too.

Liveness canaries: orphan cleanup triggers, cascading cleanup is reachable, supervised child survives cleanup.

**Runtime tests** (`src/guarantees/g6_g7_death_orphan.rs`): Deterministic tests covering orphan cleanup, supervised children surviving, and cascading cleanup through multiple tree levels.

---

### G8: Supervisor Restart Correctness

> A supervisor restarts exactly the children specified by its strategy (`OneForOne`, `OneForAll`, `RestForOne`) and respects the restart policy (`Permanent`, `Transient`, `Temporary`) of each child.

**Status**: Fully contracted (Kani + exhaustive correspondence + DFS model checking).

**Kani** (`src/guarantees/g10_supervisor.rs`): Proofs call production functions `RestartPolicy::should_restart` and `compute_restart_set`. Symbolically verifies all combinations of strategy, policy, dead index, and death reason for up to 4 children.

**Exhaustive correspondence** (`src/guarantees/correspondence.rs`): Deterministic enumeration of:
- `should_restart` truth table: 3 policies × 2 reasons = 6 combinations
- `compute_restart_set`: 3 strategies × 4 child counts × all dead indices = 30 combinations
- Runtime behavioral verification: supervisor setup → child death → restart observation for each combination

**DFS model checking** (`src/guarantees/stateright_supervisor.rs`): Exhaustive DFS over supervisor model with 4 children, 243 initial states (3 strategies × 3⁴ policy combinations), max 3 restarts, max 4 deaths. Properties verified in every reachable state:
- `OneForOne`: only dead child in restart log (if policy permits).
- `OneForAll`: all children in restart log (if policy permits).
- `RestForOne`: dead child + successors in restart log (if policy permits).
- `Temporary`: never restarted regardless of reason or strategy.
- `Transient` + normal death: not restarted.
- `Transient` + panic: restarted (if not meltdown).
- Meltdown: if total restarts exceed max, supervisor stops, no further restarts.

Liveness canaries: restart occurs, meltdown is reachable, each strategy is exercised.

---

## Conformance Summary

| Guarantee | Compiler | Kani | Exhaustive Correspondence | DFS Model Check | Conforms |
|-----------|----------|------|--------------------------|-----------------|----------|
| G1: No shared mutable state | Yes | — | — | — | Yes |
| G2: Single-threaded execution | Yes | — | — | — | Yes |
| G3: Address uniqueness | Yes | — | — | — | Yes |
| G4: Lifecycle ordering | Partial | Yes | Yes | Yes | Yes |
| G5: Fault isolation | Partial | — | — | Yes | Yes |
| G6: Death notification completeness | — | — | — | Yes | Yes |
| G7: Orphan cleanup | — | — | — | Yes | Yes |
| G8: Supervisor restart correctness | — | Yes | Yes | Yes | Yes |
