# cfuzz Branch — Development History Overview

> 19 improvement cycles on the `cfuzz` branch.
> Research-driven methodology: study competitors → identify gap → implement → test → benchmark.
> Grew test suite from 42 → 148 passing tests.

---

## Methodology

Each cycle followed a consistent pattern:

1. **Research** — Study how competitors (Erlang/OTP, Tokio, Akka, Ractor, Actix, Kameo) handle the problem
2. **Identify gap** — Find a specific deficiency in swactor
3. **Implement** — Fix the gap with minimal, targeted changes
4. **Test** — Write behavioral tests (Given/When/Then) from the consumer's perspective
5. **Benchmark** — Measure impact where applicable

### Constraints

- `src/` structure is frozen — no new files or modules, only modify existing files in-place
- No new dependencies on the root crate
- Behavioral tests only — no white-box/structural tests
- All `cargo test` must pass before each commit
- Never delete tests for active code

---

## Baseline Benchmarks (Pre-Improvement)

| Benchmark | Time | Throughput |
|-----------|------|-----------|
| spawn | 1.28 µs | — |
| message_roundtrip | 2.24 µs | — |
| send_fire_and_forget | 1.50 µs | — |
| single_actor/1000 | 57.5 µs | 17.4 Melem/s |
| multi_actor/100x100 | 610.6 µs | 16.4 Melem/s |
| ring/100 | 99.9 µs | 1.01 Melem/s |

---

## Cycle Summary

| Cycle | Commit | Topic | Tests Added | Cumulative Tests |
|-------|--------|-------|-------------|-----------------|
| 1 | `ef87f7e` | [Fairness (message budget)](CYCLE_01_FAIRNESS.md) | 3 | 45 |
| 2 | `10cb078` | [Stress tests + benchmarks](CYCLE_02_STRESS_TESTS.md) | 6 | 51 |
| 3 | `acacc1b` | [Thread parking](CYCLE_03_THREAD_PARKING.md) | 1 | 52 |
| 4 | `cf61619` | [Shutdown fix + bug-inspired tests](CYCLE_04_SHUTDOWN_FIX.md) | 5 | 57 |
| 5 | `7d00e65` | [Load-aware placement](CYCLE_05_LOAD_AWARE_PLACEMENT.md) | 3 | 60 |
| 6 | `265992c` | [Mailbox backpressure](CYCLE_06_BACKPRESSURE.md) | 4 | 64 |
| 7 | `1779ad6` | [Actor recovery](CYCLE_07_ACTOR_RECOVERY.md) | 4 | 68 |
| 8 | `0213938` | [Dead actor cleanup](CYCLE_08_DEAD_ACTOR_CLEANUP.md) | 2 (+2 updated) | 70 |
| 9 | `e28aca0` | [Lifecycle hooks + graceful stop](CYCLE_09_LIFECYCLE_HOOKS.md) | 12 | 82 |
| 10 | `d58a999` | [Actor timers](CYCLE_10_TIMERS.md) | 6 | 88 |
| 11 | `9b1518b` | [Property-based testing](CYCLE_11_PROPERTY_TESTING.md) | 7 | 95 |
| 12 | `66a8523` | [Named actor registry](CYCLE_12_NAMED_REGISTRY.md) | 11 | 106 |
| 13 | `8782638` | [Actor monitoring](CYCLE_13_MONITORING.md) | 7 | 113 |
| 14 | `4d18874` | [Actor groups](CYCLE_14_GROUPS.md) | 9 | 122 |
| 15 | `902471b` | [Ask pattern](CYCLE_15_ASK_PATTERN.md) | 5 | 127 |
| 16 | `0ef6df9` | [Registry benchmarks](CYCLE_16_REGISTRY_BENCHMARKS.md) | 0 | 127 |
| 17 | `a70bd86` | [Supervision trees](CYCLE_17_SUPERVISION.md) | 10 | 138 |
| 18 | `771c38c` | [OneForAll + RestForOne](CYCLE_18_SUPERVISOR_STRATEGIES.md) | 3 | 141 |
| 19 | `c688f0a` | [Router](CYCLE_19_ROUTER.md) | 7 | 148 |

---

## Thematic Groupings

### Scheduling & Performance (Cycles 1–5)
Foundation work: fairness guarantees, stress testing, thread parking, shutdown reliability, and load-aware actor placement. Research thread: BEAM reductions → tokio coop budget → Kameo/Actix mailboxes → tokio parker → work stealing survey.

### Resilience & Lifecycle (Cycles 6–10)
Production hardening: backpressure, crash recovery, memory leak fix, lifecycle hooks, and deterministic timers. Narrative arc: from "actors crash permanently" to "actors have a fully managed lifecycle."

### Testing & Service Discovery (Cycles 11–16)
Property-based testing for invariant verification, plus four registry features (names, monitoring, groups, ask pattern) and benchmarks to validate them. Research shifted from scheduling to service discovery patterns.

### Supervision (Cycles 17–19)
Capstone features built on everything preceding: supervision trees with configurable restart strategies, and routers for actor pool management. Directly modeled on Erlang/OTP supervision trees.

---

## Frameworks Studied

| Framework | Language | Key Lessons |
|-----------|----------|-------------|
| Erlang/OTP BEAM | Erlang | 4000-reduction budget, supervision trees, pg groups, gen_server:call |
| Tokio | Rust | 128-op coop budget, work-stealing, parker state machine |
| Akka | Scala/Java | SupervisorStrategy, Router actors, PoisonPill |
| Ractor | Rust | String-based registry, SupervisionEvent, bug history |
| Actix | Rust | Vyukov MPSC queue, 256-message guard, ctx.stop() |
| Kameo | Rust | Dual mailbox (bounded/unbounded), on_panic hook, ActorPool |
| Linux CFS/EEVDF | C | vruntime fairness, NO_HZ adaptive ticks |
| libuv/Node.js | C | Phase-based event loop, round-robin handlers |
