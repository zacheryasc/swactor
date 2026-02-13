# Cycle 2: Stress Tests, Expanded Benchmarks, and Research Extension — Development History

> Commit: `10cb078` · 4 files · 517 insertions

---

## Motivation

After fixing the fairness bug in Cycle 1, the runtime needed stress testing under adversarial conditions to find edge cases. Additionally, the competitor survey was extended to cover Kameo and Actix — two frameworks with distinct approaches to mailbox management and message dispatch.

## Competitor Analysis

### Kameo (v0.19)
- Fully async on tokio, one task per actor
- Dual mailbox: bounded (default 64) or unbounded tokio mpsc channels
- Typed signals via vtable dispatch (no `Box<dyn Any>` downcast)
- Erlang-style links for supervision (`on_link_died`)
- `on_panic` hook can restart actor (vs swactor's then-permanent poisoning)
- Known bugs: deadlocks in link establishment, leaked ActorRef preventing stop

### Actix (v0.13)
- Context-as-Future model — each actor is a single pollable Future on an Arbiter
- **Custom Vyukov lock-free MPSC queue** (not tokio channels) — push is single atomic_swap
- Default mailbox capacity: 16 (tiny)
- `do_send()` bypasses capacity for internal notifications
- **256-message assertion guard** — validates swactor's budget approach
- vtable dispatch via `Box<dyn EnvelopeProxy<A>>` — no Any downcast
- WHY FAST: custom MPSC queue, no async overhead, same-thread actors avoid cross-thread coordination

### Key Insight
Both frameworks use vtable dispatch instead of `Box<dyn Any>` downcast. Actix's 256-message assertion guard independently validates the per-actor budget concept from Cycle 1.

## Implementation

### Stress Tests (6 new)
- `message_ordering_preserved_under_budget` — FIFO order with budget=8
- `mt_stress_many_senders_one_receiver` — 50 senders × 100 msgs on 4 threads
- `mt_stress_concurrent_spawn_and_send` — 200 concurrent spawn+send on 4 threads
- `mt_chain_spawning_under_load` — 50-level chain across 2 workers
- `mt_panic_isolation_under_load` — 10 panicking + 10 healthy actors on 4 threads
- `sustained_throughput_does_not_drop_messages` — 10 batches × 100 msgs

### Benchmarks (2 new groups)
- `msg_size` group: throughput and send_latency by message size (8B, 64B, 256B, 1KB, 4KB)
- `contention` group: fanin (1–100 senders to 1 sink), cross_worker (1–4 threads)

**Key files modified:** `tests/runtime_api.rs`, `benches/runtime_benchmarks.rs`, `CLAUDE/notes/research_synthesis.md`

## Design Decisions

- **Multi-threaded stress tests** included because single-threaded testing can't catch cross-worker races
- **Panic isolation test** inspired by Actix's SyncArbiter model — ensures one panicking actor doesn't take down healthy actors on other workers
- **Message ordering test** validates that the budget mechanism (Cycle 1) doesn't break FIFO guarantees
- **Chain spawning** tests the spawn+send-in-same-handler pattern across worker boundaries

## Tests Added

6 new stress tests (45 → 51 total):

| Test | Pattern | Purpose |
|------|---------|---------|
| `message_ordering_preserved_under_budget` | FIFO verification | Budget doesn't break ordering |
| `mt_stress_many_senders_one_receiver` | Fan-in | 50:1 contention on 4 threads |
| `mt_stress_concurrent_spawn_and_send` | Concurrent spawn | Race condition hunting |
| `mt_chain_spawning_under_load` | Cascading spawn | Cross-worker chain delivery |
| `mt_panic_isolation_under_load` | Fault isolation | Panics don't spread |
| `sustained_throughput_does_not_drop_messages` | Sustained load | No message loss over time |

## Result

- 51 tests pass (42 original + 3 fairness + 6 stress)
- All workspace crates compile
- No bugs found — the runtime handles adversarial conditions correctly
- Benchmark data provides baselines for message size sensitivity and contention scaling
