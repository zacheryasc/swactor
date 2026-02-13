# Cycle 13: Actor Monitoring with Down Message Notifications — Development History

> Commit: `8782638` · 6 files · 268 insertions, 10 deletions

---

## Motivation

Actors had no way to know when other actors died. If actor A depended on actor B, and B panicked or was stopped, A would continue sending messages into the void with no notification. Monitoring (also called "death watch") is essential for building fault-tolerant systems — it's the foundation that supervision trees are built on.

## Competitor Analysis

| Framework | Mechanism | Direction | Notification |
|-----------|-----------|-----------|-------------|
| Erlang | `monitor/2` | Unidirectional | `DOWN` message |
| Akka | `watch` | Unidirectional | `Terminated` message |
| Ractor | `link` | Bidirectional | `SupervisionEvent` |
| Actix | None built-in | N/A | N/A |
| Kameo | `link` | Bidirectional | `on_link_died` callback |
| **Swactor** | **`ctx.monitor()`** | **Unidirectional** | **`Down` message** |

### Key Findings
- **Erlang's unidirectional monitor + message delivery** is the best fit for swactor — it reuses the existing type-erased message handler, requires zero trait changes, and is composable
- **Callbacks** (Ractor/Kameo style) rejected — would require adding a new method to `AnyActor`/`ActorInterface` traits, forcing all actors to implement it
- **Bidirectional links** deferred — can be layered on top of monitors later
- **Stacking** (Erlang) — multiple monitors of the same target produce independent notifications

## Implementation

### Types (in `actor.rs`)
- `MonitorRef(u64)` — unique token from `AtomicU64` counter, used for demonitor
- `Down { addr: ActorAddress, reason: StopReason }` — delivered as normal mailbox message
- `StopReason` enum: `Normal` (graceful stop) | `Panicked` (panic, not restartable)

### MonitorRegistry (in `delivery.rs`)
- `watchers: RwLock<HashMap<ActorAddress, Vec<(MonitorRef, ActorAddress)>>>` — watched → list of (ref, watcher)
- `refs: RwLock<HashMap<MonitorRef, ActorAddress>>` — ref → watched (for O(1) demonitor)

### API
- `ctx.monitor(target) → MonitorRef` — subscribe to death notifications
- `ctx.demonitor(mref)` — cancel a subscription

### Integration with cleanup_dead
- `cleanup_dead` now returns `Vec<(ActorAddress, StopReason)>` instead of `Vec<ActorAddress>`
- After cleanup: iterate dead actors, take monitors from registry, route `Down` through normal delivery (pool.deliver for same-worker, transfer_txs for cross-worker, inbox_registry for inboxes)
- Dead watcher cleanup: `remove_watcher()` strips monitor subscriptions for dead watchers (prevents ghost subscriptions)

**Key files modified:** `src/actor.rs`, `src/delivery.rs`, `src/runtime.rs`, `src/worker.rs`, `tests/runtime_api.rs`

## Design Decisions

- **Unidirectional monitors (Erlang model)** — simpler than bidirectional links, no cascading death. The watcher is notified but doesn't automatically die. This gives the watcher full control over how to react.
- **Down as a regular message** — delivered through the same mailbox as other messages. Actors with `Incoming = Down` receive it via `handle()`. This reuses the entire existing delivery pipeline with zero special-case code.
- **MonitorRef for demonitor** — each monitor subscription gets a unique ref. This supports stacking (multiple monitors of the same target) and precise cancellation.
- **StopReason distinguishes Normal vs Panicked** — watchers can decide how to react based on whether the death was graceful or a crash. Matches Erlang's `DOWN` message which includes the exit reason.
- **Dead watcher cleanup** — if the watcher dies before the watched actor, its monitor subscriptions are cleaned up. Without this, dead watchers would accumulate as ghost entries in the registry.

## Tests Added

7 new tests (106 → 113 total):

- `monitor_notifies_on_graceful_stop` — Down{reason: Normal} on graceful stop
- `monitor_notifies_on_panic` — Down{reason: Panicked} on panic
- `multiple_watchers_all_notified` — two watchers both receive Down
- `demonitor_cancels_notification` — demonitor → no Down delivered
- `dead_watcher_does_not_receive_down` — dead watcher's monitors cleaned up
- `down_delivered_to_external_inbox` — Down forwarded through inbox
- `stacked_monitors_produce_multiple_notifications` — two monitors on same target → two Downs

## Result

- 113 tests pass (106 behavioral + 7 proptest)
- All workspace crates compile, zero warnings
- Foundation for supervision trees (Cycle 17) — monitors provide the death detection mechanism
