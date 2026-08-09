# swactor process-local multicore runtime — specification

Id: 2
Last modified:
Last reviewed:

**Scope:** multicore (multi-worker) execution and message delivery within a single process (shared address space).

## 1. Scope

**In scope**

- How N workers run concurrently on N cores within one process.
- How a message is routed and delivered between actors on different workers (foreign-thread, shared memory).
- How a message is delivered between actors on the same worker.
- The seam between core and the hosting engine.

**Out of scope (deferred to separate specs)**

- Cross-isolation delivery (JS web workers) — no shared heap; requires serialization.
- Cross-process / WAN delivery — owned by the `transport` and `distribution` crates.
- Actor migration, work-stealing, and load balancing beyond spawn-time worker selection.
- A ready-queue / runnable-set optimization.

**Constraint.** Core (`src/`) stays free of any specific engine: no tokio dependency, no owned thread pool. Any engine that can host a blocking or async receiver can host a worker.

## 2. Model

- A **worker** owns a disjoint set of actors and processes them one at a time. It is the unit of parallelism: N workers on N cores run up to N actors concurrently.
- An actor is **pinned**: assigned to one worker at spawn, never moved. Worker selection at spawn is deliberately simple: an actor spawned from within a worker (`ctx.spawn`) pins to that same worker; an actor spawned from outside the runtime (via the runtime handle) is assigned round-robin across workers. A side effect is that a parent and the children it talks to stay co-located, so their traffic stays on the same-worker fast path.
- Multicore parallelizes *different actors*. One actor's work is never split across cores. This preserves the single-writer invariant: at most one message is handled per actor at any instant, across all workers.
- Workers are **autonomous and independent**: each runs its own loop. There is no global tick, no barrier, no per-step cross-worker synchronization.
- Workers are **reactive**: when idle they wait; when work arrives they run a pass.

## 3. Responsibilities (the core / engine seam)

- **Runtime** (core): owns the actor address space, the `address → worker` routing map, the per-worker inbox deposit handles, and spawn-time worker selection (same-worker for in-runtime spawns, round-robin for external spawns). It routes. It does not execute and does not own threads.
- **Worker** (core logic, engine-driven): owns its pinned actor pool. Each pass drains its inbox into actor mailboxes and processes non-empty mailboxes up to a fairness budget.
- **Engine** (integrator-supplied — std::thread, tokio, …): decides how many workers to create, hosts each worker's loop, and owns the inbox's consumer side (how the worker idles and how often it drains). **Core only transitions. The engine drives.**

## 4. Delivery regimes (this spec)

| target lives on…                     | delivery                                          |
| ------------------------------------ | ------------------------------------------------- |
| the same worker                      | inline, within the current pass (no queue)        |
| another worker, same process         | pointer-move into that worker's MPSC inbox        |
| another process / isolated / remote  | out of scope — `transport` / `distribution`       |

## 5. Routing and delivery mechanism

A send resolves the target actor to its owning worker and deposits the message. There is **no wake step**. The receiving worker idles on its own inbox, so depositing into it is what makes the next transition runnable.

### 5.1 Send path

For a send of `M` to `addr` from any in-process sender (an actor handler, or an external thread holding a sender handle):

1. Box `M` once → `Box<dyn Any + Send>` (a heap pointer). The payload is never copied again.
2. Look up `addr` in the routing map → `WorkerId`.
3. Branch:
   - **Same worker** as sender → append `(addr, M)` to the worker's local `pending_local` buffer. Delivered within the current pass. No queue, no cross-thread.
   - **Different worker** → wrap as `Envelope { dest: addr, payload: M }` and deposit into that worker's inbox (a pointer-move into shared memory). Return. No signal is sent to the receiver.
   - **Not in the map** → defer to the non-local seam (`transport` / `distribution`). Out of scope here.

### 5.2 The inbox

- One MPSC queue per worker. Many producers (any foreign thread); one consumer (the owning worker).
- **Producer side** (the deposit): non-blocking, unbounded, loss-free, FIFO. Core holds this handle per worker, indexed by `WorkerId`.
- **Consumer side** (the worker's idle point): engine-chosen. A blocking channel under std::thread; an async channel under tokio. Receiving *is* the idle point, so depositing makes the next transition runnable with no separate wake primitive. The engine owns this side and the drain cadence.

### 5.3 The crossing

The message crosses the thread boundary exactly once, inside the inbox queue. The producer writes a pointer into a slot in shared memory; the consumer, blocked or awaiting on that queue, returns it. No serialization, no copy of the payload, no inter-thread signal beyond the queue's own readiness.

## 6. Guarantees

- **Single-writer.** At most one message handled per actor at any instant, across all workers.
- **Per-(sender, target) FIFO.** Messages from one sender to one target are delivered in send order. Cross-sender ordering to the same target is not guaranteed.
- **Loss-free / non-blocking producer.** The inbox never drops and never blocks the sender (unbounded). Mailboxes likewise.
- **Fairness.** No actor processes more than `budget` messages per pass, so one actor cannot starve the others on its worker.
- **Panic isolation.** A panicking actor is poisoned and skipped; it does not take down its worker or other actors. (Existing behavior, retained.)

## 7. Data structures

**Runtime-wide (shared, read-mostly)**

- `address_map`: `RwLock<HashMap<ActorAddress, WorkerId, identity-hash>>` — the routing table; read on send, written at spawn.
- `inbox_txs`: per-worker inbox deposit handles, indexed by `WorkerId`.
- `rr_worker`: `AtomicUsize` round-robin counter, used only for external (out-of-runtime) spawns. In-runtime spawns (`ctx.spawn`) need no counter — the child pins to the caller's worker.

**Per-worker inbox (cross-thread)**

- MPSC queue. Producer = deposit (pointer-move; lock-free ring + overflow). Consumer = the worker's wait point (engine-typed).

**Per-worker, worker-local (single-threaded)**

- `pool`: `HashMap<ActorAddress, ActorSlot>`.
- `ActorSlot { mailbox: VecDeque<Box<dyn Any + Send>>, actor, lifecycle flags }`.
- `pending_local`: `Vec<(ActorAddress, Box<dyn Any + Send>)>` — same-worker buffer.

**Envelope**: `{ dest: ActorAddress, payload: Box<dyn Any + Send> }`.

## 9. Worked example

**Note on `WorkerId` indexing.** `WorkerId` is an opaque internal newtype — minted only by the runtime at spawn and used only to index that same runtime's own `inbox_txs` / `spawn_txs` slices. It never crosses the public API as a raw index, so misuse is bounded to internal code. The per-message cost on the cross-worker path is the routing-map lookup (§11 defers eliminating it via address-encoded routing), not the slice index that follows — the latter is a single pointer-add. The fast path is the same-worker arm (`pending_local`), which bypasses the inbox, the `Envelope`, and the second thread entirely.

X on worker 0 (thread T0) sends `M` to `addr`, which is Y on worker 1 (thread T1):

1. `ctx.send(addr, M)` → `Box::new(M)` (one allocation).
2. `send_any`: `address_map.lookup(addr)` → worker 1; not self → `inbox_txs[1].send(Envelope { addr, M })`. Pointer into worker 1's inbox ring. Return. No signal.
3. T1 was blocked on `inbox.recv()`; the deposit unblocks it and returns the `Envelope`.
4. T1 drains: `pool[addr].mailbox.push_back(M)`.
5. Pass walks the pool, finds Y's mailbox non-empty, pops, `Y.handle(ctx, M)`.

X learned nothing about threads. The only thread-aware steps were the one map read and the queue the pointer sat in.

Had `addr` been on worker 0: step 2 takes the same-worker arm, `M` goes to `pending_local`, and Y handles it later in this same pass — no `Envelope`, no ring, no second thread.

## 10. Changes vs current `src/`

**Removed**

- `Runtime::run()` spawning owned OS threads.
- `thread::park()` / `thread::unpark()` wakeup.
- `notify_worker()` and the `worker_threads: Vec<OnceLock<Thread>>` plumbing (including inside `ExternalSender`).
- `Placement` (the load-aware selector) and its `WorkerStats`-driven `next_worker()` scan; replaced by same-worker pinning for in-runtime spawns and a single round-robin counter (`rr_worker`) for external spawns.

**Changed**

- The per-worker transfer queue becomes the worker **inbox**, and its consumer side becomes the worker's idle point (engine-supplied). Deposit no longer signals the engine.

**Retained unchanged**

- `tick()` / `try_tick()` inline all-workers mode (deterministic, wasm, tests).
- `ActorPool`, `ActorSlot`, mailboxes, `pending_local`, budget, panic isolation, `ExternalSender` / `Inbox` / `Ask` (minus the removed wake).

**Added**

- The engine seam: a way for an integrator to create and register workers, supply each worker's inbox consumer and wait, and drive each worker's loop. Exact API is defined per engine in follow-on integration notes.

## 11. Deferred

- Actor migration, work-stealing, and load balancing beyond spawn-time worker selection.
- Ready-queue optimization.
- Cross-isolation delivery (web workers) and cross-process / WAN delivery (`transport`, `distribution`).
- Address-encoded worker routing (eliminating the routing-map lookup).
