# Cycle 14: Actor Groups with Pub-Sub Broadcast — Development History

> Commit: `4d18874` · 5 files · 307 insertions, 8 deletions

---

## Motivation

Named registry (Cycle 12) provides one-to-one name→actor mapping. Many patterns require one-to-many: broadcasting events to subscribers, load distribution across a pool, or topic-based message routing. Actor groups provide this — a named collection of actors that can receive messages as a group.

## Competitor Analysis

| Framework | Mechanism | Key Design | Auto-Cleanup |
|-----------|-----------|------------|-------------|
| Erlang `pg` | Scopes, join/leave/get_members | Flat groups, atom keys | Yes (on process exit) |
| Akka | DistributedPubSub (mediator, topics) | Cluster-wide pub-sub | Yes (via DeathWatch) |
| Ractor | `pg` module (join/leave/broadcast) | Erlang-style, global | Yes |
| Bastion | Dispatcher | Hierarchy-based routing | Structural |
| Redis pub/sub | Channels, patterns | External service | N/A |
| **Swactor** | **GroupRegistry** | **Erlang pg-style, per-runtime** | **Yes (on death)** |

### Common Patterns Across Frameworks
- Auto-cleanup on death (universal)
- At-most-once delivery (no re-delivery guarantees)
- String-based naming (flat, not hierarchical)
- Lazy group creation/deletion (groups created on first join, deleted when empty)

## Implementation

### GroupRegistry (in `delivery.rs`)
- Forward map: `groups: RwLock<HashMap<String, HashSet<ActorAddress>>>` — group → members
- Reverse map: `memberships: RwLock<HashMap<ActorAddress, HashSet<String>>>` — actor → groups (for cleanup)
- Groups auto-create on first join, auto-delete when empty

### Runtime API
- `join_group(addr, name)` — add actor to group
- `leave_group(addr, name)` — remove actor from group
- `publish_to(group, msg)` — broadcast to all group members
- `group_members(group)` — list members
- `groups()` — list all groups

### Context API (from handler)
- `ctx.join_group(name)` — join from inside handler
- `ctx.leave_group(name)` — leave from inside handler
- `ctx.publish(group, msg)` — broadcast from inside handler
- `ctx.group_members(group)` — query from inside handler

### Message Delivery
- `publish` clones message at the typed level (`Message: Clone`), sends to each member via normal routing
- Uses the same delivery pipeline as regular messages (pool.deliver, transfer_txs, inbox_registry)

### Auto-Cleanup
- `group_registry.cleanup(&addr)` called in `cleanup_dead` phase
- Uses reverse map to find all groups the dead actor belonged to, removes from each

**Key files modified:** `src/delivery.rs`, `src/runtime.rs`, `src/actor.rs`, `src/worker.rs`, `tests/runtime_api.rs`

## Design Decisions

- **Erlang `pg` model** — flat groups with string keys. Simpler than Akka's mediator/topic model, and sufficient for the common use cases (event broadcasting, worker pools).
- **Clone-based broadcast** — message is cloned for each recipient. This is O(N) but straightforward and type-safe. Alternative (shared Arc) would complicate the message pipeline.
- **Reverse map for cleanup** — without it, cleaning up a dead actor would require scanning all groups. O(1) per group membership vs O(groups) scan.
- **Lazy lifecycle** — groups are created implicitly on first join and deleted when the last member leaves. No explicit create/delete API needed. Matches Erlang `pg`.
- **publish requires `Message: Clone`** — enforced at the type level. If a message type isn't Clone, it can't be broadcast. This is a compile-time safety guarantee.

## Tests Added

9 new tests (113 → 122 total):

- `group_members_returns_joined_actors` — join + query returns members
- `empty_group_returns_no_members` — nonexistent group → empty set
- `publish_broadcasts_to_all_members` — 2 members, both receive the message
- `leave_group_stops_receiving_publishes` — leave → excluded from future broadcasts
- `dead_actor_auto_removed_from_group` — stop → removed from group
- `actor_removed_from_all_groups_on_death` — multi-group membership cleanup
- `empty_group_auto_deleted` — last member leaves → group removed from `groups()`
- `ctx_join_group_from_handler` — join via on_start
- `ctx_publish_broadcasts_from_handler` — publish via handler

## Result

- 122 tests pass (115 behavioral + 7 proptest)
- All workspace crates compile, zero warnings
