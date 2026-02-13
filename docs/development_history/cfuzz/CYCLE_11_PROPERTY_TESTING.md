# Cycle 11: Property-Based Testing and Extended Fuzz Targets — Development History

> Commit: `9b1518b` · 5 files · 534 insertions, 3 deletions

---

## Motivation

After 10 cycles of behavioral tests, the test suite relied entirely on manually-written scenarios. Property-based testing can explore state spaces that humans wouldn't think to test, automatically finding minimal failing cases. With swactor's deterministic tick model, property-based testing is an especially good fit — no concurrency noise to mask bugs.

## Competitor Analysis

| Framework/Tool | Testing Approach | Fit for Swactor |
|----------------|-----------------|-----------------|
| Tokio + Loom | Model-checking for lock-free code | Poor fit — swactor isn't lock-free |
| Erlang + PropEr/QuickCheck | Property-based with shrinking | Good model for swactor |
| Shuttle | Concurrency permutation testing | Moderate — useful for MT tests |
| proptest-state-machine | Stateful property testing for Rust | **Perfect fit** — deterministic ticks |
| cargo-fuzz | Coverage-guided fuzzing | Already in use, extended here |

### Ranked Approaches
1. **proptest-state-machine** — perfect fit for deterministic ticks, generates random operation sequences, automatic shrinking
2. Extend cargo-fuzz with new action types
3. Simple proptest (stateless properties)
4. Shuttle (concurrency permutations)
5. Loom (lock-free verification)

### Key Finding: Feature Gap Analysis
While researching testing approaches, also surveyed remaining feature gaps: named actors/registry, actor monitoring/death watch, actor groups/pub-sub, and ask pattern. These became Cycles 12–15.

## Implementation

### Property-Based Tests (proptest)
Added `proptest` and `proptest-state-machine` to dev-dependencies. New test file: `tests/proptest_runtime.rs` with 7 tests:

| Test | Property Verified |
|------|-------------------|
| `fifo_ordering_for_any_message_sequence` | FIFO preserved for 1–100 random messages |
| `budget_limits_per_actor_processing` | Budget caps per-tick processing for 2–10 actors |
| `one_shot_timer_fires_at_correct_tick` | Timer with delay 1–20 fires at exact right tick |
| `interval_timer_fires_at_correct_period` | Period 1–10, verifies 3 consecutive firings |
| `bounded_mailbox_never_exceeds_capacity` | Capacity 1–20, 1–200 messages, never exceeds |
| `spawn_n_actors_all_tracked` | 1–50 actors, all unique, all in stats |
| `swactor_state_machine` | Random Spawn/Send/Tick/Stop/CheckStats sequences |

### State Machine Test
The `swactor_state_machine` test is the most sophisticated:
- **Reference model**: `HashMap<id, alive>` tracking expected actor lifecycle
- **Operations**: random Spawn, Send, Tick, Stop, CheckStats transitions (up to 40 per test, 128 cases)
- **Invariants checked after every transition**: worker count, actor placement, mailbox safety
- **Automatic shrinking**: finds minimal failing sequences when invariants break

### Extended Fuzz Targets
Added 4 new `RawAction` variants to `fuzz/fuzz_targets/fuzz_runtime.rs`:
- `StopActor` — graceful stop via `runtime.stop_actor`
- `SpawnRestartable` — `spawn_restartable` with configurable `max_restarts`
- `ScheduleTimer` — one-shot timer via TimerSchedulerActor
- `ScheduleInterval` — interval timer via IntervalSchedulerActor

3 new actor types added to fuzz: `TimerSchedulerActor`, `IntervalSchedulerActor`, `RestartableEchoActor`

**Key files modified:** `Cargo.toml`, `tests/proptest_runtime.rs` (new), `fuzz/fuzz_targets/fuzz_runtime.rs`

## Design Decisions

- **proptest-state-machine over Loom** — Loom is designed for lock-free concurrent data structures. Swactor's primary correctness properties are sequential (within a tick). The state machine approach tests the actor lifecycle model, which is where bugs are most likely.
- **Reference model pattern** — the state machine test maintains a separate `HashMap` as the "expected" state and compares it against the runtime's actual state after each operation. This catches any divergence between the mental model and reality.
- **Extending existing fuzz targets** — rather than creating new fuzz targets, extended the existing `fuzz_runtime.rs` with new action variants. This means the fuzzer explores interactions between the new features (timers, restart, stop) and existing operations (spawn, send, tick).

### Bug Found
The state machine test immediately caught an invariant mismatch: `address_map` tracks spawned actors immediately (on spawn), but per-worker `num_actors` lags until the first tick (when the spawn is drained). Fixed the invariant to use `<=` check instead of exact equality.

## Tests Added

7 new property tests (88 → 95 total):

- 6 stateless property tests covering FIFO, budget, timers, mailbox bounds, and spawn tracking
- 1 stateful state machine test covering random operation sequences

## Result

- 95 tests pass (88 behavioral + 7 proptest)
- Fuzz targets compile with new action variants
- Bug found: stats lag vs address_map on spawn (invariant relaxed)
