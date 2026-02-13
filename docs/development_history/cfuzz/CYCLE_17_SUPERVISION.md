# Cycle 17: Supervision Trees with handle_down and Supervisor Actor — Development History

> Commit: `a70bd86` · 4 files · 754 insertions, 7 deletions

---

## Motivation

With monitoring (Cycle 13), lifecycle hooks (Cycle 9), and factory-based restart (Cycle 7) in place, swactor had all the building blocks for supervision trees — the signature feature of Erlang/OTP. Supervision trees provide structured fault tolerance: a parent actor (supervisor) monitors children and restarts them according to configurable policies when they fail.

## Competitor Analysis

| Framework | Supervisor Model | Strategies | Child Spec | Meltdown Protection |
|-----------|-----------------|------------|------------|---------------------|
| Erlang/OTP | Built-in `supervisor` behaviour | one_for_one, one_for_all, rest_for_one, simple_one_for_one | `{Id, MFA, Restart, Shutdown, Type}` | Intensity/period limits |
| Akka | SupervisorStrategy | Resume, Restart, Stop, Escalate + BackoffSupervisor | N/A (inline) | MaxNrOfRetries/withinTimeRange |
| Ractor | `ractor-supervisor` crate | External crate, event-based | SupervisionEvent callback | N/A |
| Bastion | Built-in hierarchy | Redundancy groups | Structural (parent-child) | N/A |
| CAF | No built-in supervisor | Monitor-based (manual) | N/A | N/A |
| **Swactor** | **User-space `Supervisor` actor** | **OneForOne** (Cycle 17), **OneForAll/RestForOne** (Cycle 18) | **`ChildSpec`** | **max_restarts budget** |

### Key Findings
- Swactor has all the building blocks: monitor (Cycle 13), `spawn_restartable` (Cycle 7), lifecycle hooks (Cycle 9), `Down` messages (Cycle 13)
- **Decision**: Supervisor as a user-space actor built on existing primitives (like Ractor's `ractor-supervisor` crate), not a special runtime construct
- **`handle_down` callback** enables any actor to react to monitored deaths without requiring `Incoming = Down` — this is the key API gap that needed filling

## Implementation

### 1. `handle_down` Callback on ActorInterface

The core API addition enabling supervision:

- `fn handle_down(&mut self, ctx: &Ctx, down: Down)` — default no-op, called when a monitored actor dies and the actor's `Incoming` type is NOT `Down`
- Implemented via second downcast attempt in `handle_any`: if the message is `Down` and the actor's `Incoming` type doesn't match, call `handle_down` instead of `handle`
- Fully backward-compatible: actors with `Incoming = Down` still receive via `handle()` as before
- This decouples supervision logic from the actor's primary message type

### 2. `ctx.stop_actor(addr)` — Stop Another Actor

- Sends graceful stop to another actor from handler context
- Uses `StopSignal` through normal message routing (PoisonPill semantics)
- Enables supervisor-controlled shutdown of children

### 3. `Supervisor` Actor

A user-space actor managing child actors:

- **`SupervisorStrategy::OneForOne`** — only the failed child is restarted (Cycle 17)
- **`RestartPolicy`**: `Permanent` (always restart), `Transient` (restart only on panic, not normal stop), `Temporary` (never restart)
- **`ChildSpec`** — `{ id: String, restart: RestartPolicy, factory: Fn(&Ctx) -> Result<ActorAddress> }`
- Children spawned in `on_start`, monitored via `ctx.monitor()`
- Death detected via `handle_down`, restart policy consulted, factory invoked for replacement
- **Meltdown detection**: stops itself when `total_restarts > max_restarts`
- **Cascading shutdown**: `on_stop` sends stop signals to all living children

### ActiveChild Struct
- Tracks `addr: ActorAddress` and `monitor_ref: MonitorRef` per child
- Reused by Router (Cycle 19)

**Key files modified:** `src/actor.rs`, `tests/runtime_api.rs`, `docs/runtime.md`

## Design Decisions

- **User-space actor (not runtime primitive)** — the Supervisor is just an actor that uses existing APIs (monitor, spawn, stop). No special runtime support needed. This validates the composability of the monitoring and lifecycle systems.
- **`handle_down` as opt-in callback** — adding `handle_down` to `ActorInterface` with a default no-op means existing actors don't need to change. Actors that want to react to deaths override it. The alternative (requiring `Incoming = Down`) would force actors to handle `Down` as their primary message type.
- **Factory takes `&Ctx`** — the factory closure receives the context so it can use `ctx.spawn`, `ctx.monitor`, etc. during child creation. This enables the supervisor to monitor new children immediately.
- **Meltdown protection** — if children keep crashing faster than they can be restarted, the supervisor stops itself rather than looping forever. Matches Erlang's intensity/period limits.
- **Cascading shutdown** — when the supervisor stops, all living children receive stop signals. This prevents orphaned actors.

## Tests Added

10 new tests (127 → 138 total, counting 130 behavioral + 7 proptest + 1 doctest):

- `handle_down_receives_death_notification` — handle_down callback fires on monitored death
- `handle_down_skipped_when_incoming_is_down` — backward compat: Incoming=Down uses handle()
- `ctx_stop_actor_stops_target` — one actor stops another via ctx.stop_actor()
- `supervisor_restarts_permanent_child_on_panic` — panic → restart (OneForOne + Permanent)
- `supervisor_does_not_restart_transient_child_on_normal_stop` — Normal stop → no restart
- `supervisor_restarts_transient_child_on_panic` — Panicked → restart (Transient)
- `supervisor_never_restarts_temporary_child` — Temporary → never restart
- `supervisor_meltdown_after_max_restarts` — exceeding max_restarts stops supervisor
- `supervisor_one_for_one_only_restarts_failed_child` — multi-child, only crashed child restarted
- `supervisor_on_stop_kills_children` — supervisor shutdown cascades to children

## Result

- 138 tests pass (130 behavioral + 7 proptest + 1 doctest)
- Zero warnings, full workspace compiles
- Supervisor validates the composability of Cycles 7 (recovery), 9 (lifecycle), and 13 (monitoring)
