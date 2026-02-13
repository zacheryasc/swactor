# Cycle 5: Load-Aware Actor Placement + Work Stealing Research — Development History

> Commit: `7d00e65` · 6 files · 184 insertions, 13 deletions

---

## Motivation

With fairness (Cycle 1), thread parking (Cycle 3), and shutdown (Cycle 4) resolved, the next bottleneck was actor placement. Swactor used blind round-robin to assign actors to workers — ignoring current load. If actors have unequal workloads, round-robin produces persistent imbalance. This cycle also included deep research into work stealing to decide whether full actor migration was worthwhile.

## Competitor Analysis: Work Stealing Deep Dive

| Aspect | Tokio | Go | BEAM | ForkJoinPool |
|--------|-------|-----|------|-------------|
| Queue | Fixed 256-slot ring | 256-slot ring + runnext | Per-priority linked | Growable array deque |
| Steal granularity | Half victim's queue | Half victim's runq | Individual processes | One task at a time |
| LIFO fast-path | Dedicated slot (3-use cap) | runnext (stealable 4th try) | None | Owner pops from top |
| Global queue | Mutex intrusive list | Checked 1/61 ticks | Per-priority migration | Even-indexed queues |
| Searcher limit | N/2 workers | GOMAXPROCS/2 | N/A (proactive migration) | Idle stack in ctl |
| Balance strategy | Reactive steal | Reactive steal | **Proactive migration** + reactive | Reactive scan |

### Key Patterns Discovered

1. **LIFO slot** — every runtime has one; improves cache locality by running the recipient immediately after the sender. Tokio caps at 3 consecutive uses to prevent starvation.
2. **Steal-half** — Tokio and Go both steal half the victim's queue, amortizing cross-thread coordination overhead.
3. **N/2 searcher limit** — both Tokio and Go cap concurrent searchers to prevent thundering herd (O(N²) cache-line bouncing).
4. **BEAM's migration** — unique dual approach: reactive stealing when idle + proactive migration via periodic `check_balance()`.

### Feasibility for Swactor

- **Full actor migration**: Mechanically possible (ActorSlot is `Send`), but has a 1-tick message loss window during migration and requires push-based donation (`ActorPool` is not `Sync` → no pull stealing)
- **Message stealing without actors**: Impossible — the actor IS the state; messages without the actor are meaningless
- **Decision: Load-aware placement over work stealing** — zero correctness risk, handles the primary imbalance source (uneven spawn distribution), full work stealing deferred

## Implementation

- `Placement::next_worker()` now reads per-worker stats (`num_actors` + `mailbox_depth`)
- Selects the worker with lowest combined load
- Scan starts from a rotating position → round-robin fallback when all stats are equal (initial burst, before first tick publishes stats)
- O(N) relaxed atomic loads per spawn — trivial for N ≤ 8 workers

**Key files modified:** `src/delivery.rs`, `tests/runtime_api.rs`, `benches/runtime_benchmarks.rs`

## Design Decisions

- **Load-aware placement instead of work stealing** — zero message loss risk, no ordering changes, trivial implementation cost. Handles the #1 source of imbalance: uneven spawn distribution.
- **Combined metric (actors + depth)** — neither actor count alone nor mailbox depth alone captures load accurately. Combined metric approximates total pending work per worker.
- **Relaxed atomics for stat reads** — stats are advisory (best-effort), so relaxed ordering is sufficient. No need for acquire/release which would add synchronization cost.
- **Round-robin fallback** — before the first tick, all workers report zero stats. Falling back to round-robin ensures even initial distribution rather than always picking worker 0.
- **Full work stealing deferred** — would require migration channels, address map coordination, forwarding tombstones, and a message loss window. Benefit uncertain for N ≤ 8 workers.

## Tests Added

3 new tests (57 → 60 total):

- `load_aware_placement_prefers_lighter_worker` — imbalanced load biases spawn toward the lighter worker
- `load_aware_placement_single_worker_degrades_gracefully` — single-thread mode works correctly
- `load_aware_placement_falls_back_to_round_robin_on_fresh_runtime` — even distribution before ticks produce stats

**Benchmark added:** `placement/spawn_under_load` (2-thread and 4-thread variants)

## Result

- 60 tests pass
- All workspace crates compile
- Comprehensive work-stealing research documented for future reference
