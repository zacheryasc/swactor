# swactor engine — specification

Id: 1
Last modified:
Last reviewed:

**Scope:** the execution substrate that drives swactor workers and hosts their async side-work, defined as an interface implemented per environment.

## 1. Purpose

swactor actors are synchronous, single-writer message handlers. Real systems need work actors cannot do inline: draining byte streams, running retry backoffs, polling on an interval, blocking GPU calls. That work lives in *tasks* on an execution substrate. Today that substrate is Tokio — hardcoded and reinvented per crate (ambient `Handle::try_current()`, silently-owned runtimes, ad-hoc `block_on` sync facades, a mix of tokio tasks and std threads).

This spec defines the **engine**: a single execution substrate, expressed as an interface, that (a) drives swactor workers and (b) runs the async tasks that back them. Tokio is one implementation; a minimal std-thread engine, a Go engine, a JS-worker engine, and a deterministic test engine are others. Authoring the interface from swactor's needs lets core and each engine implementation be optimized independently on either side of the seam.

## 2. Scope

**In scope**

- The engine interface: what swactor requires of an engine, and what an engine provides.
- The responsibility split between actor-workers and the engine.
- How the engine drives workers (hosting the worker loop, the inbox as the wait seam).
- The capability surface: tasks, timers, async I/O, blocking, time.
- The bridge contract: how a task delivers into an actor mailbox, and the sync/async boundary rules.
- The invariants an engine must uphold.
- Reference instantiations (non-normative).

**Out of scope**

- Actor execution semantics — single-writer, per-(sender,target) FIFO, fairness, panic isolation. Those belong to the actor-worker / core.
- Backpressure policy. Producers and consumers share one engine; pressure handling is the application's decision, not swactor's.
- Cancellation and shutdown lifecycle (deferred; nice-to-have).
- Failure / observability propagation, except where it falls out of the bridge contract.
- Cross-process / cross-isolation delivery and serialization.
- Specific protocols and codecs (iroh/QUIC, datastream framing). Those are crate logic built *on* the engine.

## 3. Model

- An **actor-worker** owns a disjoint set of actors, processes them one at a time, and is the unit of actor execution. It holds the pool, mailboxes, routing, and a single synchronous entry point: run one **pass** (`tick_once`), which drains its inbox into mailboxes and processes non-empty mailboxes up to a fairness budget.
- The **engine** is the execution substrate. It does exactly two things:
  1. **Drives workers** — hosts each worker's loop: wait until the worker has work, run a pass, repeat.
  2. **Runs tasks** — schedules the async side-work (timers, I/O pumps, blocking calls) that backs the actors.
- **The engine owns all progression.** Actor handlers never `.await`. Every handler is a synchronous transition that returns control immediately. The engine runs the loop that drives them and holds every long-lived flow (a worker idle on its inbox, a task doing I/O, a timer). The actor world is pure transition. Only the engine carries control flow across time.
- Workers and tasks share one substrate and one scheduler. There is no separate "I/O runtime" beside the actor runtime.

## 4. The engine interface

The interface is authored from swactor's needs. It is a **contract** — operations plus their semantics and invariants. A Rust trait is its canonical Rust binding; Go, JS, and other hosts implement the same contract natively. This spec defines the contract, not the Rust signature.

**The engine provides:**
// USER: The `host_worker(id, pass)` fn needs more explanation and justification
// USER: Why are we including a timer as a core function necessary to the engine. Can it not go somewhere else?

| operation | meaning |
|---|---|
| `host_worker(id, pass) → deposit` | Create the worker's inbox, start its reactive loop (wait on the inbox, call `pass`), and return the **deposit** handle core uses to route messages into it. |
| `spawn(task)` | Schedule an async unit of work on the substrate. |
| `spawn_blocking(work)` | Schedule blocking CPU / syscall work off the async path. |
| `timer(delay)` / `interval(period)` | Schedule future or recurring work. |
| `now()` | The engine's monotonic clock. |

**Core provides back to the engine and to tasks:**
// USER: Core is fine as it is. We are not modifying core, it was carefully designed and is very pure. The engine is to be abstracted in such a way as to complement the abstractions core gives us. I think we can satisfy these fns through existing core, but we don't say that core provides xyz, as that is not the framing of this spec.

| surface | meaning |
|---|---|
| `pass` (per worker) | The synchronous entry point `tick_once(&tc) → did_work`, run once per pass. |
| `deliver` | A handle to deposit a message into an actor mailbox by address — the bridge (§7). Cloneable; captured by tasks. |

The split is deliberate. Core owns actor logic, routing, and the *deposit* side of every inbox. The engine owns the *idle* side and all scheduling. **Core only transitions. The engine drives.** The deposit handle returned by `host_worker` is engine-agnostic (loss-free, non-blocking push) so core's routing can deposit without knowing which engine is in use.

## 5. Driving workers
// USER: `pass` is stupid when we already have a `tick()` built in.

- The engine hosts N workers. For each, it runs: call `pass`; if it did work, call it again (a productive pass may have buffered same-worker sends that need draining); if it did no work, idle on the inbox until a deposit makes a pass runnable. There is no separate wake primitive. A deposit into the inbox is what makes the next transition runnable, so the engine drives it.
- The **inbox is the wait seam.** The engine creates each inbox and holds its consumer side, choosing how to wait (a blocking recv under std threads; an async `recv().await` under tokio; an event under JS). Core holds the deposit side for routing.
- **Non-reentrancy.** The engine must never run two passes of the same worker concurrently. A worker's `&mut self` is live only for the duration of a synchronous `pass` call — never held across a wait.
- **Scheduling strategy is the engine's choice.** Whether a pass runs inline on the executor (cooperative) or on a blocking thread is an implementation tradeoff the engine owns; core is agnostic to it.

## 6. Capability surface
// USER: Maybe just I/O instead of explicitly async? So we can have a blocking I/O if our engine only supports that
// USER: Not sure I want to put time inside the engine. I am open to being convinced, but the added complexity and tying it
// USER: to what I wanted to be a simple task/execution api is worrying me about future compatability.

The primitives an engine may provide. Capabilities are **per-implementation and discoverable**: each engine reports which it supports, and binding an engine that lacks a required capability fails at construction, never at runtime.

- **Tasks** — `spawn` of an async unit of work; the substrate's unit of concurrency.
- **Timers** — one-shot delay and recurring interval.
- **Async I/O** — streams, sockets, files. This is where implementations diverge most: a tokio engine offers sockets / QUIC / streams; a JS engine offers fetch / WebSocket; a std-thread engine offers none (only blocking I/O via `spawn_blocking`).
- **Blocking** — `spawn_blocking` for CPU-bound or syscall work that must not stall the executor.
- **Time** — `now()`. In a test engine this is virtual, advanced by the test; this is what makes deterministic testing possible.

An engine that provides only tasks + blocking + time is still a valid (if unperformant) engine. Crates that need async I/O bind to an engine that provides it.

## 7. The bridge contract
// USER: Why this contract, why are tasks delivering directly to actors?

How an engine task gets a result into an actor mailbox.

- A task captures a **deliver** handle (obtained from core, not from the engine) bound to a destination address, or a runtime-wide `send_to(addr, msg)`. Delivering deposits the message into the owning worker's inbox — a loss-free, non-blocking pointer-move along the same path any sender uses. No serialization, no copy, within one address space.
- Deliver is **fire-and-forget from the task's view**: it returns immediately; the actor handles the message on a later pass of its worker.
- **Boundary rules:**
  - Actor handlers are synchronous and single-writer. They never `.await`.
  - `&mut Worker` and any actor state is live only during a synchronous `pass`; it is never held across a wait and never sent into a task.
  - All `.await` lives in tasks. Tasks never touch actor state directly; they communicate only via the deliver handle and the inbox.
- The inbox a task delivers into is the same FIFO, loss-free, unbounded queue the worker waits on. Mailbox ordering semantics (per-(sender,target) FIFO) are the actor-worker's concern; the engine's only obligation is that the inbox itself is FIFO and loss-free.

## 8. Invariants

An engine must uphold:

- **Non-reentrant passes.** At most one `pass` per worker at any instant.
- **Loss-free, non-blocking delivery.** The inbox never drops and never blocks the sender (unbounded).
- **FIFO inbox.** Messages depart an inbox in deposit order.
- **Progress independence.** A long-running or blocked task must not stall worker passes, and vice versa. The engine provides enough concurrency that workers and tasks progress independently (on a cooperative single-thread host like JS, this is a discipline the engine enforces: no blocking calls in tasks or passes).
- **Actors never await.** No `.await` reaches actor code; the engine owns every wait.

## 9. Reference instantiations (non-normative)

Illustrations of how each environment satisfies the contract — not prescription.

- **tokio.** Workers and tasks are tokio tasks; a worker loop is `loop { inbox.recv().await; pass(); }` with `&mut Worker` live only across the synchronous `pass` (long passes may be moved to `spawn_blocking`; that scheduling choice is the engine's, per §5). Async I/O, `spawn_blocking`, and `now()` are tokio's. This is today's de-facto engine, made explicit.
- **std-thread.** Each worker is an OS thread blocking on its inbox; tasks are OS threads or a small pool; `spawn_blocking` is a thread; there is no async I/O, only blocking I/O. Simple, unperformant, dependency-free — and a valid engine.
- **deterministic test engine.** A single-threaded stepping scheduler: workers and tasks are entries the test advances manually; `now()` is virtual time advanced by the test; async I/O is faked or mocked. It implements the same contract, so crates test against the interface with no real network and no threads, fully deterministic. It falls out of the contract; it is not specified separately.
- **Go / JS-worker (illustrative).** Workers and tasks map to goroutines + channels, or to the JS event loop + `postMessage` / callbacks. Each provides the capability subset its runtime supports.

## 10. What this spec does not define

The boundary, stated plainly:

- Actor execution semantics (single-writer, FIFO, fairness, panic isolation).
- Backpressure.
- Cancellation and shutdown.
- Failure / observability propagation beyond the bridge.
- Cross-process / cross-isolation delivery and serialization.
- Specific protocols and codecs.
