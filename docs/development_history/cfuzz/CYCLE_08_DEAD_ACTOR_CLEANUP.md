# Cycle 8: Dead Actor Cleanup (Memory Leak Fix) — Development History

> Commit: `0213938` · 4 files · 120 insertions, 14 deletions

---

## Motivation

After Cycles 7 (recovery) and the pre-existing poison-on-panic behavior, dead actors accumulated in both `ActorPool` and `AddressMap` forever. Their slots were never reclaimed, their addresses remained registered, and the system gradually leaked memory. This is a known bug class in actor frameworks.

## Competitor Analysis

| Framework | Dead Actor Handling | Known Bugs |
|-----------|-------------------|------------|
| Akka | Automatic cleanup via DeathWatch | #22990 — ActorRef leak in certain paths |
| CAF | Manual cleanup expected | #420 — actor leak in specific failure modes |
| Erlang/OTP | Automatic — process exits free all resources | N/A (VM handles cleanup) |
| Ractor | Supervisor-driven cleanup | Memory bloat per actor at scale |
| **Swactor (before)** | **None — permanent leak** | **Both ActorPool and AddressMap leak** |

## Implementation

- Added `AddressMap::remove(addr)` to `delivery.rs` — O(1) removal from address map
- Added `ActorPool::cleanup_dead()` to `worker.rs` — collects and removes poisoned actors, returns their addresses
- Added Phase 7 to `tick_once`: `cleanup_dead` → remove from address_map → re-publish `num_actors` stat
- Stats immediately reflect removal (no stale counts)

**Key files modified:** `src/delivery.rs`, `src/worker.rs`, `tests/runtime_api.rs`

## Design Decisions

- **Automatic cleanup in tick_once** — no manual API needed. Dead actors are cleaned up every tick, preventing accumulation.
- **Phase 7 (after all message processing)** — cleanup happens after `tick_all` and `pending_local`, so any final messages to dead actors correctly fail. No risk of cleaning up an actor that's about to receive a message.
- **Re-publish `num_actors` after cleanup** — ensures stats are immediately consistent. Without this, stats would show stale actor counts until the next tick.

### Behavior Change
- **Before**: Sending to a poisoned actor silently discarded the message (address still in map, delivery succeeded, but processing was skipped)
- **After**: Sending to a cleaned-up actor returns `Err` (address removed from map, send fails)
- This is **better** — callers learn the actor is gone instead of silently losing messages.

## Tests Added

2 new tests + 2 existing tests updated (68 → 70 total):

- `dead_actor_cleaned_up_from_stats_and_address_map` — good actor persists, bad actor's address is removed
- `bulk_dead_actor_cleanup` — 20 panicked actors all cleaned up in one tick
- Updated `send_to_poisoned_actor_is_a_silent_black_hole` → now asserts send returns `Err` (behavior change)
- Updated `poisoned_actor_messages_not_counted_as_processed` → sends fail to cleaned-up actor

## Result

- 70 tests pass
- All workspace crates compile
- Memory leak closed: dead actors no longer accumulate in ActorPool or AddressMap
