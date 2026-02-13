# Cycle 9: Lifecycle Hooks and Graceful Actor Stop — Development History

> Commit: `e28aca0` · 8 files · 427 insertions, 27 deletions

---

## Motivation

Before this change, actors had no initialization or teardown callbacks and no way to stop gracefully. An actor started processing messages immediately (no setup phase) and could only die by panicking. Every mature actor framework provides lifecycle hooks for resource management and graceful shutdown.

## Competitor Analysis

| Framework | on_start | on_stop | on_panic | Self-stop | External stop |
|-----------|----------|---------|----------|-----------|---------------|
| Erlang | `init/1` | `terminate/2` (NOT on crash) | N/A | `{stop,Reason,State}` | `gen_server:stop` |
| Akka | `preStart` | `postStop` (always) | `preRestart` | `context.stop(self)` | PoisonPill / stop |
| Actix | `started` | `stopped` | N/A | `ctx.stop()` | `addr.do_send(Stop)` |
| Kameo | `on_start` | `on_stop` | `on_panic` | `Context::stop()` | `stop_gracefully/kill` |
| Ractor | `pre_start` | `post_stop` (NOT on kill/panic) | N/A | `stop()` | `Signal::Kill` |
| **Swactor** | **`on_start`** | **`on_stop` (NOT on panic)** | N/A | **`ctx.stop_self()`** | **`runtime.stop_actor()`** |

### Key Findings
- Most frameworks do NOT call `on_stop` on panic — state may be corrupt, running teardown on corrupt state is unsafe. Erlang and Ractor agree. Akka is the outlier (always calls `postStop`).
- Self-stop should be immediate (after current message). External stop should be queued (PoisonPill semantics — process pending messages first).
- Restarted actors should get `on_start` called again on the fresh instance.

## Implementation

### Lifecycle Hooks
- `ActorInterface::on_start(&mut self, ctx: &Ctx)` — default no-op, called on first tick before any messages
- `ActorInterface::on_stop(&mut self, ctx: &Ctx)` — default no-op, called during cleanup for gracefully-stopped actors
- `AnyActor::on_start()`/`on_stop()` — forwarded from `Actor<A>` implementation
- `ActorSlot` gains `started: bool` flag — tracks whether `on_start` has been called
- `on_start` called in `tick_all` before first message; panic in `on_start` → immediate poison
- `on_stop` called in `cleanup_dead` for stopping (not poisoned) actors, wrapped in `catch_unwind`
- Restarted actors get `started=false` so `on_start` fires again on fresh instance

### Graceful Stop (Dual Mode)
- `ctx.stop_self()` — **immediate** stop after current message via `request_stop` buffer
- `runtime.stop_actor(addr)` — **external** stop via `StopSignal` message (PoisonPill semantics: queued after existing messages)
- `ActorSlot` gains `stopping: bool` flag
- Phase 7 `cleanup_dead` now handles both poisoned AND stopping actors

### Stats
- `stops: AtomicU64` added to `WorkerStats` and `WorkerInfo` — tracks graceful stops separately from panics

**Key files modified:** `src/actor.rs`, `src/worker.rs`, `src/runtime.rs`, `src/delivery.rs`, `src/stats.rs`, `tests/runtime_api.rs`

## Design Decisions

- **`on_stop` NOT called on panic** — matches Erlang and Ractor. Corrupt state after panic makes teardown unsafe. If you need cleanup, use `spawn_restartable` (Cycle 7) to get a fresh instance.
- **Dual stop modes** — `ctx.stop_self()` is immediate (actor decides "I'm done after this message"). `runtime.stop_actor()` is queued (external signal processed after pending messages). This matches Erlang's `{stop, Reason, State}` vs `gen_server:stop`.
- **StopSignal as a message** — external stop uses the same delivery pipeline as regular messages. No special-case routing needed. The PoisonPill pattern (Akka) is well-proven.
- **`on_start` panic → immediate poison** — initialization failure is fatal. No restart attempted because the factory might produce the same broken actor. Matches Erlang's `{stop, Reason}` from `init/1`.
- **Default no-ops** — both hooks are optional. Existing actors don't need to change. 100% backward compatible.

## Tests Added

12 new tests (70 → 82 total):

- `on_start_called_before_first_message` — on_start fires on first tick, before messages
- `on_start_called_per_actor` — 5 actors each get exactly one on_start call
- `on_start_panic_poisons_actor` — panic in on_start → poisoned, no messages processed
- `actor_can_stop_self` — 5 msgs sent, stops after 3, only 3 processed, on_stop called
- `runtime_can_stop_actor` — external stop via runtime, on_stop called, actor removed
- `send_to_stopped_actor_returns_error` — stopped actor gone from address map
- `stop_vs_panic_tracked_separately_in_stats` — stops and panics counted independently
- `on_stop_can_send_messages` — farewell message sent during on_stop is delivered
- `on_start_called_again_after_restart` — restartable actor gets on_start on fresh instance
- `external_stop_is_queued_after_pending_messages` — PoisonPill semantics verified
- `external_stop_before_new_messages_prevents_processing` — stop before send blocks new msgs
- `stop_nonexistent_actor_returns_error` — stop on bad address returns Err

## Result

- 82 tests pass
- All workspace crates compile
- Swactor weaknesses "no lifecycle hooks" and "no graceful stop" both resolved
- Foundation for supervision (Cycle 17) — `on_stop` enables resource cleanup, `stop_actor` enables supervisor-controlled shutdown
