# Cycle 4: Shutdown Fix + Bug-Inspired Tests — Development History

> Commit: `cf61619` · 3 files · 161 insertions, 12 deletions

---

## Motivation

Cycle 3 introduced thread parking, but created a new problem: `shutdown()` didn't unpark workers. Parked workers wouldn't notice the shutdown signal until their `park_timeout` expired, causing delayed shutdown. Additionally, studying bug reports from competitor projects (Ractor, Kameo, Actix) revealed specific failure modes worth testing in swactor.

## Competitor Bug Analysis

The 5 new tests were directly inspired by real bug reports from other actor frameworks:

| Test | Inspired By | Bug |
|------|-------------|-----|
| `stats_snapshot_is_read_only` | Ractor #310 | `get_children()` was destructive — moved children out of supervisor |
| `stats_under_load_do_not_interfere_with_processing` | General | Stats collection shouldn't slow down message processing |
| `shutdown_wakes_parked_workers_immediately` | Cycle 3 regression | Parked workers must notice shutdown promptly |
| `mt_send_after_run_delivers_to_running_actors` | Kameo #185 | Messages sent after `run()` weren't delivered during startup race |
| `budget_respected_even_with_self_sends` | Actix #515 | Self-sends bypassed mailbox capacity, defeating backpressure |

## Implementation

### Shutdown Fix
- `shutdown()` now iterates all workers and calls `unpark()` on each thread handle
- Parked workers wake immediately and check the shutdown flag
- Workers that aren't parked are unaffected (unpark is a no-op)

### Bug-Inspired Tests
Each test encodes a real bug class discovered in competitor frameworks, ensuring swactor doesn't have the same vulnerability.

**Key files modified:** `src/runtime.rs`, `tests/runtime_api.rs`

## Design Decisions

- **Unpark-all on shutdown** rather than a dedicated shutdown condvar — simpler, reuses existing parking infrastructure from Cycle 3
- **Bug-inspired testing methodology** — studying competitor bug trackers yields high-value test cases that target real failure modes, not theoretical ones

## Tests Added

5 new tests (52 → 57 total):

- `stats_snapshot_is_read_only` — reading stats doesn't mutate runtime state (from Ractor #310)
- `stats_under_load_do_not_interfere_with_processing` — stats don't affect message processing throughput
- `shutdown_wakes_parked_workers_immediately` — validates fast shutdown with thread parking
- `mt_send_after_run_delivers_to_running_actors` — messages sent after run() are delivered (from Kameo #185)
- `budget_respected_even_with_self_sends` — self-sends don't bypass budget (from Actix #515)

## Result

- 57 tests pass
- All workspace crates compile
- Shutdown latency with parked workers reduced from up to 1ms to near-zero
