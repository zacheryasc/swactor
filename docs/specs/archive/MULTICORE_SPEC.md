# swactor process-local multicore runtime — specification

Id: 2
Last modified: b887e941cbe6f1e209339abd0375507aca9bfe52
Last reviewed:
> Any edit to this spec must update `Last modified` above to the current `git HEAD` commit.

**Scope:** multicore (multi-worker) execution and message delivery within a single process (shared address space).

## 1. Scope

**In scope**

- How N logical workers execute concurrently within one process.
- How messages are routed between actors on different workers through shared memory.
- How messages are delivered between actors on the same worker.
- The ownership seam between core, the hosting engine, and the explicit single-thread adapter.

**Out of scope (deferred to separate specs)**

- Cross-isolation delivery (JS web workers) — no shared heap; requires serialization.
- Cross-process / WAN delivery — owned by the `transport` and `distribution` crates.
- Actor migration, work-stealing, and load balancing beyond spawn-time worker selection.
- Ready-queue and readiness-driven idle optimizations.

**Constraint.** Core (`src/`) stays free of any specific engine: no tokio dependency and no owned thread pool. Core exposes synchronous worker transitions; the selected host decides when and where to call them.
## 2. Model

- A **worker** owns a disjoint set of actors and processes them one at a time. It is the logical unit of parallelism. The hosting engine may run different workers concurrently.
- A worker is not an OS thread or physical core. The engine may move its driver between substrate threads; no affinity is guaranteed.
- An actor is **pinned** to one logical worker at spawn and never moved. `ctx.spawn` selects the caller's worker. Spawns through the runtime handle are assigned round-robin.
- Multicore parallelizes different actors. One actor's work is never split across workers.
- Workers are autonomous: each has its own state and transition loop. There is no global tick, barrier, or per-pass cross-worker synchronization.
- Each engine-owned worker driver remains schedulable and invokes one worker pass per scheduling turn. Readiness-driven idling may replace this policy after measurement without changing actor semantics.
## 3. Ownership and the core / engine seam

- **`RuntimeConfig`** selects `worker_count` before routing begins and configures `worker_ingress_budget`.
- **`Runtime`** is a cloneable shared handle. It owns routing state, per-worker producer handles, process-local inbox routing, configuration, extensions, and statistics. It routes and spawns; it has no `tick()` or `try_tick()`.
- **`Worker`** owns one actor pool and the consumer sides of its transfer, spawn, and admin queues. `Worker::try_tick(&mut self)` performs one synchronous pass and returns immediately.
- **`RuntimeParts`** linearly owns one `Runtime` handle and its `Vec<Worker>`. It is the construction bundle consumed by exactly one execution host.
- **`Engine`** consumes `RuntimeParts` and installs one driver task per worker. Each task directly owns its `Worker`; no runtime lock or shared worker borrow is required on the hot path.
- **`SingleThreadRuntime`** is the explicit manual adapter. It consumes `RuntimeParts` and sequentially calls each worker through `&mut self`.

Linear ownership prevents two engines, or an engine and the single-thread adapter, from driving the same workers. **Core only transitions. The selected host drives.**
## 4. Delivery regimes

| target lives on…                    | delivery                                                        |
| ----------------------------------- | --------------------------------------------------------------- |
| the sender's worker                 | append to `pending_local`; eligible on the next worker pass      |
| another worker in the same process  | move an `Envelope` into that worker's transfer queue             |
| a process-local external `Inbox`    | deliver through the existing `InboxRegistry`                    |
| another process / isolated / remote | out of scope — handled by `transport` / `distribution`           |
## 5. Routing and delivery

The runtime resolves actor addresses to logical workers. Engine drivers poll workers continuously, so depositing work requires no separate engine wake operation in the initial implementation.

### 5.1 Send path

For a send of `M` to `addr`:

1. Resolve `addr` in `address_map`.
2. If it names an actor:
   - From that actor's owning worker to the same worker: type-erase the payload and append `(addr, payload)` to `pending_local`.
   - From another worker: type-erase the payload, create `Envelope { dest, payload }`, and deposit it into the target's transfer queue.
   - From the runtime handle or an `ExternalSender`: there is no source worker identity, so deposit into the target's transfer queue.
3. If it is not an actor address, try the process-local `InboxRegistry` before the non-local transport seam.

The payload is moved and type-erased, not cloned. Same-worker delivery bypasses the concurrent transfer queue but does not recursively run another actor in the current pass.

### 5.2 Per-worker ingress

Each worker initially retains the existing three MPSC inputs:

- transfer envelopes;
- spawn requests;
- admin commands.

`Worker::try_tick` checks all three, so no queue is solely responsible for waking an idle worker. The initial queue implementation may remain non-blocking and unbounded, but those properties are not permanent public guarantees; later bounded queues or backpressure may change them.

### 5.3 Shared-memory crossing

Cross-worker delivery moves an `Envelope` containing the boxed payload through shared memory. It performs no serialization or payload clone. Because workers are logical, this may or may not cross an OS-thread boundary on a particular scheduling turn.
## 6. Worker progression and budgets

`Worker::try_tick(&mut self)` performs one worker pass:

1. Drain bounded batches from spawn, transfer, and admin ingress.
2. Run per-worker extension work.
3. Process each runnable actor up to `actor_message_budget`.
4. Install actors spawned during handlers before delivering messages staged for them.
5. Append `pending_local` messages to target mailboxes.
6. Publish statistics and clean up stopped or poisoned actors.

`worker_ingress_budget` is the maximum number of items consumed by one drain operation for each ingress queue; `0` means unlimited. A pass may perform a second bounded spawn drain after handlers so children exist before local delivery. The budget counts ingress items, not distinct target actors.

Messages appended from `pending_local` become eligible on the next pass. This keeps a pass finite under local send chains and preserves the actor message budget across self-sends.

`try_tick` returns whether the pass did work. An engine driver calls it once per future poll, re-arms its own waker, and yields to the substrate. `SingleThreadRuntime::try_tick(&mut self)` calls each owned worker once sequentially and combines their results.

## 7. Guarantees

- **Single-writer.** Each `Worker` has one owning driver, so at most one message is handled per actor at any instant.
- **Stable placement.** An actor remains on its assigned logical worker for its lifetime.
- **Per-(sender, target) FIFO.** Sequential sends from one sender to one target are delivered in send order. Cross-sender ordering is unspecified.
- **Acceptance, not processing.** A successful send means the runtime accepted the message for routing. Actor stop, panic, type mismatch, or later queue policy may prevent processing.
- **Fairness.** A nonzero actor budget bounds one actor's work per pass; a nonzero ingress budget bounds each ingress drain.
- **Panic isolation.** A panicking actor is poisoned and removed without taking down its worker or other actors.
## 8. Data structures

**Configuration**

- `worker_count: usize` — number of logical workers created with the runtime.
- `worker_ingress_budget: usize` — maximum items per ingress drain; `0` is unlimited.
- Existing actor and channel capacity settings remain.

**Runtime-wide shared state**

- `address_map: RwLock<HashMap<ActorAddress, WorkerId, identity-hash>>`.
- `transfer_txs`, `spawn_txs`, and `admin_txs`, indexed by `WorkerId`.
- `worker_stats`, indexed by `WorkerId`.
- `rr_worker: AtomicUsize` for runtime-handle spawns.
- Existing `InboxRegistry`, runtime extension, observers, and remote sink.

`WorkerId` is an opaque internal newtype created during runtime construction. It indexes only arrays belonging to that same runtime.

**Linear construction ownership**

- `RuntimeParts { runtime: Runtime, workers: Vec<Worker> }`.
- `Runtime { shared: Arc<RuntimeShared> }`.
- `SingleThreadRuntime { runtime: Runtime, workers: Vec<Worker> }`.

**Per worker**

- `id: WorkerId`.
- Consumer sides of the transfer, spawn, and admin queues.
- `pool: HashMap<ActorAddress, ActorSlot>`.
- `ActorSlot { mailbox: VecDeque<Box<dyn Any + Send>>, actor, lifecycle flags }`.
- Worker-local staging including `pending_local`.
- One `WorkerStats` and optional `WorkerExtension`.

**Envelope**

- `{ dest: ActorAddress, payload: Box<dyn Any + Send> }`.
## 9. Worked example

X on logical worker 0 sends `M` to Y on logical worker 1:

1. `ctx.send(addr, M)` resolves `addr` to worker 1.
2. The payload is boxed and moved into `transfer_txs[1]` as an `Envelope`.
3. Worker 1's engine driver receives a scheduling turn and calls `worker.try_tick()`.
4. The transfer drain appends the payload to Y's mailbox.
5. The actor pass pops the message and calls `Y.handle(ctx, M)`.

The engine may execute these worker drivers on different OS threads, the same OS thread at different times, or different threads on later turns. Actor placement remains worker-stable either way.

If Y is on worker 0, the send appends to `pending_local`. At the end of the pass it enters Y's mailbox and becomes eligible on worker 0's next pass. No concurrent transfer queue is used.
## 10. Changes required in current `src/` and `crates/engine`

**Core**

- Add `worker_count` and `worker_ingress_budget` to `RuntimeConfig`.
- Replace the address set with `ActorAddress → WorkerId` routing.
- Replace the single transfer, spawn, admin, stats, and worker fields with per-worker construction.
- Split the shared `Runtime` handle from linearly owned `Worker` values assembled in `RuntimeParts`.
- Remove `Runtime::tick()` / `Runtime::try_tick()` and the `RefCell<Worker>` / unsafe `Runtime: Sync` arrangement.
- Give `Worker` its real `WorkerId`; update `SystemInfo`, admin responses, tracing, hooks, and stats.
- Route targeted admin commands to the owning worker and broadcast aggregate commands such as actor listing.
- Create one `WorkerExtension` per worker.

**Engine**

- Change engine construction to consume `RuntimeParts`.
- Replace the single `Runtime::try_tick` driver with one self-polling driver future per worker.
- Each driver owns its `Worker` and invokes `Worker::try_tick(&mut self)` directly.

**Single-thread execution**

- Add `SingleThreadRuntime`, which consumes `RuntimeParts` and sequentially advances all workers without locks.
- Move manual ticking helpers such as `recv_ticking` to this explicit adapter.

**Retained**

- `ActorPool`, `ActorSlot`, mailboxes, next-pass `pending_local`, actor message budgets, and panic isolation.
- `ExternalSender`, process-local `Inbox` / `Ask`, runtime extensions, administration, statistics, and transport routing.
- The current queue implementation as the initial policy, without making it a permanent API guarantee.
## 11. Deferred

- Readiness-driven idle workers and other polling optimizations, pending performance evidence.
- Bounded ingress and explicit backpressure policy.
- Actor migration, work-stealing, and load balancing beyond spawn-time selection.
- Ready-queue optimization.
- Physical thread/core affinity.
- Cross-isolation delivery and cross-process / WAN delivery.
- Address-encoded worker routing.
