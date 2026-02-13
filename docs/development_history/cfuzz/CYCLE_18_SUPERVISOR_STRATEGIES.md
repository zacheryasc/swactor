# Cycle 18: OneForAll and RestForOne Supervisor Strategies — Development History

> Commit: `771c38c` · 4 files · 277 insertions, 2 deletions

---

## Motivation

Cycle 17 introduced supervision with the `OneForOne` strategy (only the failed child is restarted). Erlang/OTP defines two additional coordinated restart strategies that handle interdependent children:

- **`one_for_all`** — when one child fails, ALL children are restarted (for tightly coupled children that share state assumptions)
- **`rest_for_one`** — when one child fails, it and all children started AFTER it are restarted (for chains where later children depend on earlier ones)

These strategies require coordinated shutdown: the supervisor must stop living siblings, wait for all of them to die, then restart the affected set in the original spec order.

### Research Detour: SmallBox/InlineAny Optimization
Before choosing this cycle's topic, investigated SmallBox optimization for message dispatch — a 44% queue throughput improvement was measured. However, it was deferred because:
- Requires `unsafe` code in a core path
- Would touch 32+ call sites across the codebase
- Violates the "src/ structure frozen" constraint

Extended the Supervisor with coordinated strategies instead — higher value, zero risk.

## Competitor Analysis

| Framework | OneForAll | RestForOne | Coordinated Shutdown |
|-----------|-----------|------------|---------------------|
| Erlang/OTP | Yes | Yes | Built into supervisor behaviour |
| Akka | No (different model: Resume/Restart/Stop/Escalate) | No | N/A |
| Ractor | No | No | N/A |
| Bastion | Implicit (redundancy groups) | No | Implicit |
| **Swactor** | **Yes** | **Yes** | **Phase-based state machine** |

### Erlang's Coordinated Restart
In Erlang, `one_for_all` and `rest_for_one` stop affected children in reverse start order, wait for all to terminate, then restart in start order. This guarantees initialization dependencies are respected.

## Implementation

### SupervisorPhase State Machine
- `Normal` — steady state, processing handle_down events normally
- `Stopping { awaiting: HashSet<ActorAddress>, restart_set: Vec<usize> }` — coordinated shutdown in progress

### SupervisorStrategy Extensions
- `SupervisorStrategy::OneForAll` — all children restarted when one fails
- `SupervisorStrategy::RestForOne` — failed child + all children after it (in spec order) restarted

### Coordinated Restart Flow
1. Child dies → `handle_down` called
2. Strategy determines affected indices (OneForAll: all, RestForOne: failed + later)
3. `begin_coordinated_restart(ctx, indices)`:
   - Sends stop signals to living siblings in the restart set
   - Transitions to `Stopping` phase with `awaiting` set
   - Already-dead children handled: if all targets are already dead, skip to immediate restart
4. Subsequent `handle_down` calls during `Stopping` phase:
   - Remove from `awaiting` set
   - When `awaiting` is empty → all stopped
5. `finish_restart(ctx)`:
   - Restart all children in the restart set, in spec order
   - Transition back to `Normal` phase

### Refactoring
- `check_intensity()` factored out of `handle_down` for restart budget checking — shared by all strategies

**Key files modified:** `src/actor.rs`, `tests/runtime_api.rs`, `docs/runtime.md`

## Design Decisions

- **Phase-based state machine** — the `Stopping` phase cleanly separates "waiting for siblings to die" from "normal operation." This prevents races where a new death arrives while a coordinated restart is in progress.
- **Stop signals (not kill)** — affected siblings are stopped gracefully (PoisonPill semantics), giving them a chance to run `on_stop` for cleanup. This matches Erlang's `terminate/2` being called during supervised shutdown.
- **Restart in spec order** — children are restarted in the order they appear in the ChildSpec list, regardless of which child triggered the restart. This preserves initialization dependencies.
- **Already-dead optimization** — if all children in the restart set are already dead (e.g., cascading failures), skip the `Stopping` phase entirely and restart immediately. Without this, the supervisor would wait forever for Down messages that already arrived.
- **Meltdown protection shared** — the same `max_restarts` budget applies across all strategies. OneForAll restarts count as one restart event (not N), matching Erlang's behavior.

## Tests Added

3 new tests (138 → 141 total):

- `supervisor_one_for_all_restarts_all_on_single_failure` — one child panics, all 3 get new addresses
- `supervisor_rest_for_one_restarts_rest_after_failed` — child_b panics, child_a unchanged, child_b + child_c restarted
- `supervisor_one_for_all_waits_for_all_downs_before_restart` — verifies coordinated shutdown completes before restart begins

## Result

- 141 tests pass (133 behavioral + 7 proptest + 1 doctest)
- Zero warnings, full workspace compiles
- All three Erlang-standard supervision strategies now available: OneForOne, OneForAll, RestForOne
