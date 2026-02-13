# Cycle 7: Actor Recovery via Factory-Based Restart — Development History

> Commit: `1779ad6` · 6 files · 167 insertions, 10 deletions

---

## Motivation

Before this change, a panicking actor was permanently poisoned — it could never process messages again. Its address remained in the address map but silently discarded all messages. In production, this means a single panic permanently degrades the system. Every mature actor framework provides some form of crash recovery.

## Competitor Analysis

| Framework | Recovery Model | State After Restart | Mailbox After Restart |
|-----------|---------------|--------------------|-----------------------|
| Erlang/OTP | Factory (MFA tuple), fresh process | Fresh (new init/1) | Lost (new PID) |
| Akka | Replace internals, keep ActorRef | Fresh (preRestart hook) | Preserved (docs say "usually wrong") |
| Kameo | `on_panic(&mut self)` hook | Potentially corrupt | Preserved |
| Actix | `Supervised` trait, re-create context | Fresh | Lost |
| Ractor | `SupervisionEvent` callback | Up to supervisor | Up to supervisor |
| **Swactor (before)** | **None — permanent poison** | **N/A** | **Silently discarded** |

### Key Insight
Akka's approach of preserving state by replacing internals is documented as "usually wrong" — the state that caused the panic is likely corrupt. Kameo's `on_panic(&mut self)` is risky for the same reason. Erlang's factory-based restart (fresh process from MFA tuple) is the safest approach: guaranteed clean state.

## Implementation

- `Actor<A>` expanded from tuple struct to named fields: `inner`, `restart_factory`, `max_restarts`, `restart_count`
- `AnyActor::try_restart(&self) -> Option<Box<dyn AnyActor>>` trait method (default `None`, backward compatible)
- Factory stored as `Arc<dyn Fn() -> A + Send + Sync>` — called to produce fresh actor instance on restart
- `spawn_restartable(actor, factory, max_restarts)` added to both `Runtime` and `Ctx`
- `tick_all` panic handler: `try_restart()` before poisoning; on success, replace actor, clear mailbox, reset state
- `restarts` counter added to `WorkerStats` and `WorkerInfo`

**Key files modified:** `src/actor.rs`, `src/runtime.rs`, `src/worker.rs`, `src/stats.rs`, `tests/runtime_api.rs`

## Design Decisions

- **Factory-based restart (Erlang model)** — safest approach, guaranteed clean state. Factory closure is `Arc<dyn Fn() -> A>`, cloned into fresh `Actor<A>` on each restart.
- **max_restarts limit** — prevents infinite restart loops. When exceeded, actor is permanently poisoned. Mirrors Erlang's restart intensity.
- **Mailbox cleared on restart** — messages that triggered the panic are discarded. Fresh actor starts with empty mailbox. (Erlang does this too — new PID means new mailbox.)
- **Same address preserved** — unlike Erlang (new PID), the restarted actor keeps its `ActorAddress`. This is simpler for callers and matches Akka's model.
- **Factory fields are "cold"** — `restart_factory` and `max_restarts` are never touched by `handle_any` (the hot path). After `catch_unwind`, these fields are guaranteed safe to read.
- **Non-restartable actors unchanged** — `try_restart()` returns `None` by default, preserving the existing poison-on-panic behavior.

## Tests Added

4 new tests (64 → 68 total):

- `restartable_actor_recovers_after_panic` — basic restart works: panic, recover, process new messages
- `restartable_actor_resets_state_on_restart` — fresh state confirmed post-restart (counter resets to zero)
- `restartable_actor_respects_max_restarts` — 2 restarts allowed, 3rd panic → permanent poison
- `non_restartable_actor_still_poisons_on_panic` — backward compatibility: default actors still poison

## Result

- 68 tests pass
- All workspace crates compile
- Swactor weakness "panicked actors permanently poisoned" resolved
- Foundation laid for supervision trees (Cycle 17)
