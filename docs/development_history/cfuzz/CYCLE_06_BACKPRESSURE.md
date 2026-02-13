# Cycle 6: Per-Actor Mailbox Backpressure — Development History

> Commit: `265992c` · 6 files · 163 insertions, 8 deletions

---

## Motivation

Before this change, swactor mailboxes were unbounded — a fast producer could flood a slow consumer's mailbox without limit, eventually exhausting memory. Every production actor framework provides some form of backpressure. This was identified as a key weakness in the competitor analysis.

## Competitor Analysis

| Framework | Default Capacity | Overflow Policy | Backpressure Model |
|-----------|-----------------|----------------|-------------------|
| Erlang/OTP | Unbounded | N/A (pobox for opt-in bounding) | Process isolation limits blast radius |
| Actix | 16 | `do_send()` bypasses for internal msgs | Tiny default, force callers to handle |
| Kameo | 64 | Bounded tokio mpsc (sender blocks) | Blocking backpressure |
| Tokio mpsc | User-specified | Bounded (sender blocks or permit pattern) | Blocking or try_send |
| Go channels | User-specified | Blocking send / non-blocking select | Blocking backpressure |
| **Swactor (before)** | **Unbounded** | **None** | **None** |

Key observation: Actix's default capacity of 16 is aggressive — it forces callers to think about message flow. Kameo's 64 matches swactor's message budget. The consensus across frameworks: bounded by default, with configurable overflow policy.

## Implementation

- Added `MailboxOverflow` enum: `DropNewest` (discard incoming when full) and `DropOldest` (evict oldest to make room)
- Added `default_mailbox_capacity` and `mailbox_overflow` to `RuntimeConfig`
- Default: `capacity=0` (unbounded) — 100% backward compatible
- `ActorSlot` stores per-actor capacity and policy (initialized from runtime defaults at spawn time)
- `deliver()` in worker enforces bounds; dropped messages tracked via `drops_this_tick` counter
- `messages_dropped: AtomicU64` added to `WorkerStats` and `WorkerInfo`

**Key files modified:** `src/config.rs`, `src/worker.rs`, `src/runtime.rs`, `src/stats.rs`, `tests/runtime_api.rs`

## Design Decisions

- **DropNewest vs DropOldest (not blocking)** — swactor's synchronous tick model can't block the sender (it would deadlock the entire worker). Drop policies are the only viable option for a sync runtime.
- **Default unbounded** — backward compatibility. Users opt into backpressure by setting capacity > 0.
- **Per-runtime defaults, not per-actor** — simpler configuration. Per-actor overrides could be added later via a builder pattern on spawn.
- **Drop counting** — critical for observability. Without it, users can't tell if their system is losing messages.
- **No DropRandom** — the two policies cover the common cases. DropNewest protects against producer floods (newest messages are redundant). DropOldest keeps the freshest state (useful for sensor/status actors).

## Tests Added

4 new tests (60 → 64 total):

- `bounded_mailbox_drop_newest_caps_at_capacity` — 50 msgs sent, capacity 10 → only 10 delivered (oldest 10)
- `bounded_mailbox_drop_oldest_keeps_newest` — 10 msgs sent, capacity 5 → newest 5 kept
- `unbounded_mailbox_delivers_all_messages` — backward compatibility: capacity=0 delivers everything
- `bounded_mailbox_refills_after_processing` — capacity 5, process batch, refill works correctly

## Result

- 64 tests pass
- All workspace crates compile
- Swactor weakness "no backpressure" resolved
