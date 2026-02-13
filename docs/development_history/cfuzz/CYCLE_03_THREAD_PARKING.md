# Cycle 3: Thread Parking for Instant Worker Wakeup — Development History

> Commit: `acacc1b` · 4 files · 59 insertions, 10 deletions

---

## Motivation

Before this change, idle workers used `thread::sleep` with a fixed timeout to wait for new work. This meant an idle worker wouldn't notice new messages until its sleep timer expired — up to 1ms of unnecessary latency on the idle-to-active transition. Under bursty workloads, this sleep-based backoff wastes both time and power.

## Competitor Analysis

| Runtime | Idle Strategy | Wakeup Mechanism |
|---------|--------------|-----------------|
| Tokio | Parker state machine (notified/sleeping/empty) | `unpark()` via atomic CAS |
| Linux | NO_HZ adaptive ticks (stop tick when idle) | Interrupt on new work |
| Go | `notewakeup` / futex | OS-level wake |
| BEAM | Scheduler sleep + signal | Thread signal |
| **Swactor (before)** | **`thread::sleep(1ms)`** | **Timer expiry only** |

Tokio's parker uses a 3-state machine (notified → sleeping → empty) with atomic transitions. The key insight: `unpark()` is a **no-op** if the thread isn't parked, so callers pay zero cost on the hot path.

## Implementation

- Replaced `thread::sleep` with `thread::park_timeout` in worker run loop
- Workers register `thread::current()` via `OnceLock<Thread>` on startup
- `send_to` and `spawn` call `Thread::unpark()` on target worker after enqueuing work
- Cross-worker sends from `WorkerContext` also unpark the target
- Zero new dependencies — uses only `std::sync::OnceLock` + `std::thread::park_timeout`

**Key files modified:** `src/worker.rs`, `src/runtime.rs`, `src/delivery.rs`

## Design Decisions

- **`OnceLock<Thread>` for thread handle storage** — set-once semantics match the worker lifecycle (one thread per worker, never changes). Simpler than `Mutex<Option<Thread>>`.
- **`park_timeout` instead of `park`** — timeout ensures workers periodically wake even without explicit unpark, preventing permanent sleep if an unpark is missed.
- **Unpark on `send_to` and `spawn`** — these are the two operations that create work for a worker. The cost is a single atomic store (no-op if thread is already running).
- **No condvar** — `thread::park/unpark` is simpler and avoids the spurious wakeup complexity of condition variables. Tokio's parker validates this approach.

## Tests Added

1 new test (51 → 52 total):

- `mt_parked_worker_wakes_on_send` — verifies that a parked worker processes a message immediately after send (not after timeout)

## Result

- 52 tests pass
- All workspace crates compile
- Idle-to-active latency reduced from up to 1ms to near-zero
- No overhead on hot path — `unpark()` is a no-op when thread isn't parked
