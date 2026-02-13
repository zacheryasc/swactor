# Cycle 1: Per-Actor Message Budget for Tick Fairness — Development History

> Commit: `ef87f7e` · 8 files · Priority: P0 (critical bug fix)

---

## Motivation

The `tick_all` function in `worker.rs` drained the **entire mailbox** for each actor before moving to the next:

```rust
while let Some(msg) = slot.mailbox.pop_front() {
    // processes ALL messages for actor A before moving to actor B
}
```

If actor A had 10,000 queued messages, all other actors on the same worker were completely starved until A finished. This is a critical fairness bug — every other runtime studied prevents this.

## Competitor Analysis

| Runtime | Fairness Mechanism | Budget |
|---------|-------------------|--------|
| Erlang/OTP BEAM | Reduction counting, preemptive | 4,000 reductions |
| Tokio | Cooperative budgeting | 128–256 operations |
| libuv/Node.js | Round-robin across handlers | No single handler drains completely |
| Linux CFS | vruntime-based fairness | Time slices enforced |
| Ractor | N/A (1 task = 1 actor via tokio) | Inherited from tokio |
| **Swactor (before)** | **None** | **Unlimited drain** |

The BEAM's reduction budget (4,000 per process before preemption) is the gold standard for actor fairness. Tokio's cooperative budget (128 ops) serves a similar purpose for async tasks. Actix has a 256-message assertion guard that validates the approach.

## Implementation

- Added `actor_message_budget: usize` to `RuntimeConfig` (default: 64)
- Modified `tick_all` in `worker.rs` to break after `budget` messages per actor per tick
- `budget=0` means unlimited (100% backward compatible)
- Updated `RuntimeConfig` struct literals across all crates (python, runtime-dashboard, mt_benchmarks)

**Key files modified:** `src/worker.rs`, `src/config.rs`, `benches/runtime_benchmarks.rs`, `tests/runtime_api.rs`

## Design Decisions

- **Budget of 64 chosen** as default — between BEAM's 4,000 (too generous for swactor's coarser granularity) and tokio's 128 (per-op vs per-message). Benchmarks showed budget=32 was slightly faster for throughput, but 64 provides more fairness headroom.
- **Per-runtime, not per-actor** — simpler configuration, matching the BEAM model where the reduction budget is global. Per-actor budgets could be added later as an extension.
- **budget=0 means unlimited** — backward compatibility for users who want the old behavior.

## Tests Added

3 new behavioral tests (42 → 45 total):

- `hot_actor_does_not_starve_cold_actor` — hot actor with many messages doesn't prevent cold actor from processing
- `unlimited_budget_drains_all` — budget=0 preserves old behavior
- `budget_messages_drain_across_multiple_ticks` — excess messages carry over to next tick

**Benchmarks added:** `fairness/cold_latency_under_pressure`, `fairness/throughput_by_budget`

## Result

- 45 tests pass (42 original + 3 new)
- All workspace crates compile
- Baseline benchmarks established for future comparison
