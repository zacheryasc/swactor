# Cycle 19: Router Actor for Pooled Message Distribution — Development History

> Commit: `c688f0a` · 4 files · 528 insertions, 3 deletions

---

## Motivation

Many workloads benefit from distributing messages across a pool of identical worker actors. Before this change, users had to manually manage actor pools: spawn N workers, track their addresses, implement distribution logic, and handle worker replacement on failure. A Router actor encapsulates this pattern — it receives messages and transparently forwards them to pool members using a configurable strategy.

## Competitor Analysis

| Framework | Pool/Router Model | Strategies | Auto-Replace |
|-----------|------------------|------------|-------------|
| Erlang | `poolboy` (checkout/checkin), `wpool` (transparent forwarding, 6 strategies + custom) | RoundRobin, Random, BestWorker, Hash, Available, custom | Manual |
| Akka | Router actors (Pool vs Group), Resizer for dynamic sizing | RoundRobin, Random, SmallestMailbox, Balancing, Broadcast, ScatterGather, TailChopping, ConsistentHashing | Pool auto-creates, Group manual |
| Actix | SyncArbiter (shared queue, implicit work-stealing) | N/A (shared queue) | N/A |
| Kameo | ActorPool (least-connections, auto-replace dead workers) | Least-connections | Yes |
| Ractor | No built-in router (process groups only) | N/A | N/A |
| **Swactor** | **`Router<M>` actor** | **RoundRobin, Random, Broadcast** | **Yes (via monitor + handle_down)** |

### Key Findings
- **Router-as-actor** with transparent forwarding (wpool/Akka style) is the best fit — the router looks like a regular actor to callers
- **User-space actor** like Supervisor (Cycle 17), reusing monitor + handle_down for worker replacement
- **SmallestMailbox deferred** — requires runtime stats access not available in user-space
- **ConsistentHashing deferred** — requires a hash function parameter, can be added later as a builder method

## Implementation

### Router\<M\> Actor
- Generic over `M: Message` — same `Incoming` type as workers, enabling transparent forwarding
- Workers spawned in `on_start`, monitored via `ctx.monitor()`, auto-replaced via `handle_down`
- Reuses `ActiveChild` struct from Supervisor (addr + monitor_ref)

### Routing Strategies
- `RoutingStrategy::RoundRobin` — sequential circular distribution via counter
- `RoutingStrategy::Random` — random worker selection via `get_random()` helper
- `RoutingStrategy::Broadcast` — clone message to all live workers (`M: Clone` required)

### Fault Tolerance
- Dead worker detected via `handle_down` → factory invoked → new worker spawned and monitored
- **Meltdown protection**: `total_restarts > max_restarts` → `ctx.stop_self()`
- **Cascading shutdown**: `on_stop` sends stop signals to all workers

### Configuration
- `Router::new(pool_size, strategy, factory, max_restarts)` — all-in-one constructor
- Factory: `Arc<dyn Fn(&Ctx) -> Result<ActorAddress, Error> + Send + Sync>`

**Key files modified:** `src/actor.rs`, `tests/runtime_api.rs`, `docs/runtime.md`

## Design Decisions

- **Router-as-actor (transparent forwarding)** — callers send messages to the router's address as if it were a regular actor. The router forwards to pool members. This is the cleanest API: no special send function, no pool handle, just an address.
- **User-space actor (not runtime primitive)** — like Supervisor, Router is built entirely on existing APIs (spawn, monitor, handle_down, stop). This validates the actor system's composability.
- **Generic over M** — `Router<M>` has `Incoming = M`, same as the workers. Messages are forwarded with zero transformation. Type safety is enforced at compile time.
- **Broadcast requires Clone** — broadcasting clones the message for each worker. The Clone bound is only required when using the Broadcast strategy, enforced at the type level.
- **SmallestMailbox deferred** — would require reading per-actor mailbox depth from runtime stats, which isn't available from within a handler. Could be added with a stats query API.
- **ConsistentHashing deferred** — requires a hash function parameter (user must define which part of the message determines the routing key). Better to add as a builder method with a closure parameter.
- **Reuses ActiveChild from Supervisor** — the pattern of "track address + monitor ref, replace on death" is identical. Code sharing confirms the design consistency between Supervisor and Router.

## Tests Added

7 new tests (141 → 148 total):

- `router_round_robin_distributes_across_workers` — 6 msgs to 3 workers, each gets 2
- `router_broadcast_sends_to_all_workers` — 1 msg, all 3 workers receive
- `router_random_delivers_to_some_worker` — 30 msgs across 3 workers, at least 2 workers used
- `router_replaces_dead_worker` — panicked worker auto-replaced, pool size maintained
- `router_meltdown_after_max_restarts` — 3 deaths with max_restarts=2 → router stops
- `router_on_stop_kills_workers` — stopping router cascades to all workers
- `router_broadcast_multiple_messages_all_received` — 5 msgs × 3 workers = 15 received

## Result

- 148 tests pass (140 behavioral + 7 proptest + 1 doctest)
- Zero warnings, full workspace compiles
- Router validates the composability of the entire cfuzz feature set: monitoring (Cycle 13), lifecycle hooks (Cycle 9), handle_down (Cycle 17), and the ActiveChild pattern (Cycle 17)
- The cfuzz branch concludes with a comprehensive actor runtime featuring: fairness, backpressure, recovery, lifecycle management, timers, named registry, monitoring, groups, ask pattern, supervision trees, and routers
