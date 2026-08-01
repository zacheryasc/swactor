# Actor Panel — v1 Spec (DRAFT)

Status: **DRAFT, v1 scope.** Converged scope for the first menu item of the
swactor dashboard: live observation of actors, their messages, mailbox, and
throughput. North star is `tokio-console`, scoped down to ship fast and iterate.

---

## 1. Goal

An **actor-first** roster plus a per-actor **dossier**, focused on identity,
lifecycle, mailbox, and message throughput. Streaming-feel, single-runtime in
focus with a runtime selector. Replaces the worker page's worker-centric framing
with actors as the primary axis; workers become a column and a filter.

One sentence: *a live console over the actor population and what each actor is
doing with its messages.*

## 2. Non-goals (deferred backlog)

Explicitly out of v1, queued for later iterations:

- Tree lens (spawn/supervision hierarchy) — needs `parent`.
- Flow/message graph (who talks to whom).
- All relationship facets: parent, children, senders/receivers, monitors.
- Per-actor **arrival** rate (saturation is derived from mailbox growth instead).
- Death-event capture with `StopReason` + `ExitValue` (dead actors get a stale
  badge from the last retained snapshot only).
- Rich lifecycle transition timeline (v1 shows current state + age).
- SSE streaming transport (v1 polls, matching the existing pages).
- Actor state inspection (`GetActorState` — security-gated, tier-3).
- Per-actor busy/poll handler timing.

## 3. Roster (primary view)

Sortable, filterable live table. Default sort: **mailbox depth descending**
(hot actors bubble up). Matches the existing worker page's poll cadence (750 ms)
and runtime selector.

| Column        | Source                                  | Notes |
|---------------|-----------------------------------------|-------|
| `name · addr` | `LogicalName` + `ActorAddress` (short)  | short hex identity + human label |
| `type`        | `actor_type_name`                       | **new** — the actor's Rust type |
| `state`       | derived from lifecycle flags (see §6)   | **up from poisoned-only**; colored badge |
| `mailbox`     | `mailbox_depth` + growth color          | trend color: steady / rising / runaway |
| `msg/s`       | derived from `messages_processed` delta | processed rate |
| `processed`   | `messages_processed`                    | lifetime total |
| `last msg`    | `last_msg_type`                         | last handled message type |
| `worker`      | `worker_id`                             | placement; also a filter |
| `age`         | from `spawn_time`                       | **new** — alive duration |

Filters: by **state** (e.g. "show all poisoned"), by **type**, by **worker**, by
**name/address** substring.

## 4. Dossier (row click)

Focus panel with three facets, all about the actor itself — no relationships.

### 4.1 Lifecycle & identity
Full address, `actor_type`, the `message_type` it accepts, spawn age, current
state. If the actor is dead, show the last-known state as stale (no
`StopReason`/`ExitValue` in v1 — deferred).

### 4.2 Mailbox dynamics
Depth-over-time chart + growth rate. Saturation signal. Backed by a **per-actor
history ring buffer** the view maintains (bounded `VecDeque`, same pattern as
the existing `HistorySample` in `worker_view.rs`). No core change — the view
folds each incoming snapshot into the buffer.

### 4.3 Message diet  *(signature feature)*
`message_type_counts` rendered as a sorted bar list (top-N) with counts, plus the
last message. Data already exists. This is the column `tokio-console` cannot have
(tasks are opaque); swactor actors are message-typed, so "what does this actor
do" is answered by what it eats. Lean into it visually.

## 5. Summary strip

Throughput at the runtime level, above the roster. Same card shape as the worker
view, actor-centric:

`actors` · `total msg/s` · `total mailbox` · `poisoned` · `uptime`

## 6. Lifecycle state derivation

A single display state derived from the four flags, in priority order:

```
poisoned               → "poisoned"   (red)
else stopping          → "stopping"   (orange)
else suspended         → "suspended"  (yellow)
else !started          → "new"        (blue)
else                   → "running"    (green)
```

swactor's flags *are* the states — cleaner than `tokio-console`'s running/idle.

## 7. Data model — the one core prerequisite

The live feed is `runtime.actors`, emitted by `DatastreamStatsHook`
(`crates/datastream/src/endpoint.rs`) from `ActorSnapshot` (`src/stats.rs:123`),
built in `ActorPool::mailbox_depths_into` (`src/worker.rs:995`). `ActorSlot`
(`src/worker.rs:558`) already holds every field v1 needs; the change is purely
additive in the snapshot, keeping the read-only push contract.

### Enrich `ActorSnapshot` with

| Field           | Type                  | Source on `ActorSlot` / actor        |
|-----------------|-----------------------|--------------------------------------|
| `started`       | `bool`                | `slot.started`                       |
| `suspended`     | `bool`                | `slot.suspended`                     |
| `stopping`      | `bool`                | `slot.stopping`                      |
| `actor_type`    | `&'static str`        | `slot.actor.metadata().actor_type_name` |
| `message_type`  | `&'static str`        | `slot.actor.metadata().message_type_name` |
| `spawn_time`    | `Option<u64>`         | `slot.env` → `SpawnTimestamp` (ms since runtime creation) |

`poisoned` already present. Propagate through `RuntimeActorSnapshotRecord`
(`endpoint.rs`) so the JSON wire payload carries the new keys.

**`parent` is excluded** — only needed for the tree lens (deferred).

### `name` continues via the existing merge

`name` is a `swactor-std` registry concern, not core. It already flows through the
`runtime.stats` → `actor_details` (`ActorInfo.name`) path and is merged by the
existing worker view. v1 reuses that merge; no new core plumbing for names.

## 8. View-side state (dashboard)

New view module, mirroring `SwactorWorkerView`'s structure but actor-centric.

### Per-actor view state

```rust
struct ActorState {
    address: String,
    name: Option<String>,
    actor_type: Option<String>,
    message_type: Option<String>,
    // lifecycle
    started: bool,
    suspended: bool,
    stopping: bool,
    poisoned: bool,
    spawn_time: Option<u64>,
    // throughput
    mailbox_depth: u32,
    mailbox_growth: f64,                  // depth/s, derived from history
    messages_processed: u64,
    msg_per_sec: f64,                     // derived via assign_u64_rate
    last_msg_type: Option<String>,
    message_type_counts: Vec<(String, u64)>,
    worker_id: Option<u32>,
    history: VecDeque<HistorySample>,     // per-actor mailbox chart buffer
    last_update: Option<Instant>,
}
```

`apply_json` reuses the tolerant multi-alias field helpers already in
`worker_view.rs` (`u32_field`, `string_field`, `assign_u64_rate`,
`parse_message_type_counts`) so the new keys land gracefully across versions.

### Snapshot JSON contract

Same envelope as the worker view:

```jsonc
{
  "runtimes": [
    {
      "stream": { "key": "...", "node": "...", "life": 0 },
      "live": true,
      "last_seen_ms_ago": 42,
      "summary": { "actors": 0, "msg_per_sec": 0.0, "mailbox_depth": 0, "poisoned": 0, "uptime_ms": 0 },
      "actors": [ { /* ActorState fields */ } ]
    }
  ]
}
```

Keyed by `stream_key = "{node}#{life}"` (one entry per runtime stream), matching
the worker view so the runtime selector is shared.

## 9. Transport & feel (kept cheap for v1)

- **Poll-first**, 750 ms, matching `worker_page.rs`. SSE is an iteration upgrade.
- **State-badge color** carries the alarm; no flash/animation machinery.
- **Dead actors**: last snapshot retained with a stale badge (`now - last_seen >
  LIVE_TTL`), no death-event capture.
- `LIVE_TTL` ~8 s (match the worker view).

## 10. File map

| File | Change |
|------|--------|
| `src/stats.rs` | add fields to `ActorSnapshot` (§7) |
| `src/worker.rs` (`mailbox_depths_into`) | populate new fields from `ActorSlot` |
| `crates/datastream/src/endpoint.rs` (`RuntimeActorSnapshotRecord`) | serialize new fields |
| `crates/dashboard/src/swactor/actor_view.rs` | **new** — `ActorPanelView: DashboardView` |
| `crates/dashboard/src/swactor/actor_page.rs` | **new** — `ACTOR_HTML` const |
| `crates/dashboard/src/swactor/mod.rs` | `pub fn actor_view()`, register |
| `crates/dashboard/src/server.rs` / `root_page.rs` | list **first** in the menu |

## 11. Open decisions

1. **Menu first**: render the index dynamically from `ViewRegistry::descriptors()`
   (today the static root HTML ignores it) vs. hardcode the actor link ahead of
   workers. Recommend dynamic — scales as views grow.
2. **Type cardinality display**: with many instances per type, do we also offer a
   type-aggregated rollup (OrleansDashboard-style) in v1, or instance-only?
   Recommend instance-only for v1; rollup is a fast follow.
