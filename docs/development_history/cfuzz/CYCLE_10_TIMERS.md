# Cycle 10: Per-Worker Tick-Counting Timers — Development History

> Commit: `d58a999` · 5 files · 247 insertions, 5 deletions

---

## Motivation

Actors often need to schedule delayed or periodic work (timeouts, heartbeats, polling intervals). Before this change, swactor had no timer mechanism — actors had to manually count ticks or rely on external scheduling. The synchronous tick model makes wall-clock timers inappropriate, but tick-counting timers are a natural fit and provide deterministic behavior.

## Competitor Analysis

| Framework | Timer Model | Deterministic? |
|-----------|------------|---------------|
| Erlang | `timer:send_after`, `erlang:start_timer` (wall-clock ms) | No |
| Akka | `scheduleOnce`, `scheduler` (wall-clock duration) | No |
| Actix | `ctx.run_later`, `ctx.run_interval` (wall-clock) | No |
| Kameo | `tokio::time::sleep` (wall-clock) | No |
| Tokio | `tokio::time` (wall-clock, pausable for testing) | With `time::pause()` |
| Go | `time.After`, `time.NewTicker` (wall-clock) | No |
| **Swactor** | **Tick-counting** | **Yes — fully deterministic** |

### Key Insight
Swactor's synchronous tick model makes tick-counting timers uniquely valuable: a timer scheduled for "5 ticks from now" fires at exactly tick N+5, regardless of wall-clock speed. This makes timer behavior reproducible in tests and simulations — something no other framework provides natively.

Also researched but **rejected**: priority messages (lifecycle hooks from Cycle 9 cover 95% of use cases) and SmallBox optimization (deferred: measure allocation cost first before adding unsafe code).

## Implementation

### Timer Types
- `OnceTimer` — fire once at `fire_at` tick, consumed after firing
- `IntervalTimer` — fire every `period` ticks, message cloned via `CloneMsg` trait

### Timer Infrastructure
- `CloneMsg` trait — type-erased clone for interval timer messages (blanket impl for `Message + Clone`)
- `TimerRequest` enum: `Once { dest, msg, ticks }` | `Interval { dest, msg, period }`
- Per-worker `TimerWheel` — stores pending timers, checked each tick

### Integration into tick_once
- **Phase 2.5**: Fire due timers, route through full delivery system (pool.deliver for local actors, transfer_txs for cross-worker, inbox_registry for inboxes)
- **Phase 5.5**: Drain timer requests from handler buffer into TimerWheel
- **After cleanup_dead**: GC interval timers for dead actors

### API
- `ctx.send_after_ticks(addr, msg, ticks)` — one-shot timer
- `ctx.send_interval_ticks(addr, msg, period)` — interval timer
- `Runtime::schedule_timer()` — no-op with warning (timers are per-worker only, must be scheduled from within a handler)

**Key files modified:** `src/actor.rs`, `src/worker.rs`, `src/runtime.rs`, `src/delivery.rs`, `tests/runtime_api.rs`

## Design Decisions

- **Tick-counting, not wall-clock** — deterministic behavior is a core swactor advantage. Wall-clock timers would break test reproducibility and simulation fidelity.
- **Per-worker timer wheel** — timers are local to the worker that owns the actor. No cross-worker synchronization needed. Timer routing uses the same delivery system as regular messages.
- **CloneMsg trait** — interval timers need to clone the message for each firing. A blanket impl covers all `Message + Clone` types, so users don't need to implement anything extra.
- **Timer GC for dead actors** — interval timers must be cleaned up when their target actor dies, otherwise they fire forever into the void.

### Bug Fixed
`gc_dead_intervals` was initially over-aggressive — it removed timers for ANY address not in the local pool, including inboxes and cross-worker actors. Fixed to only GC timers for addresses in the `dead` set from `cleanup_dead`.

## Tests Added

6 new tests (82 → 88 total):

- `one_shot_timer_fires_after_n_ticks` — timer with delay=3 fires on tick 4
- `handler_can_schedule_one_shot_timer` — timer scheduled from within a handler fires correctly
- `one_shot_timer_fires_only_once` — consumed after firing, doesn't repeat
- `interval_timer_fires_repeatedly` — period=2, fires every 2 ticks (3 firings verified)
- `interval_timer_cleaned_up_when_actor_dies` — GC removes orphaned interval timers
- `timer_with_zero_delay_fires_next_tick` — delay=0 fires on next tick (not same tick)

## Result

- 88 tests pass
- All workspace crates compile
- Bug found and fixed: over-aggressive timer GC for cross-worker addresses
