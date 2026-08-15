# swactor engine — specification

Id: 1
Last modified: b887e941cbe6f1e209339abd0375507aca9bfe52
Last reviewed: f8fc594b95871813a890b5d60f60dee505ef93bc
> Any edit to this spec must update `Last modified` above to the current `git HEAD` commit.

**Scope:** the execution substrate that drives swactor workers and hosts its async side-work, defined as an interface implemented per environment.

**Status: implemented** for the default native Tokio engine. The substrate-neutral contract, capability model, and engine time are in place, verified by the native behavioral contract and a non-Tokio portability proof. The VastAI provider adapter remains explicitly out of scope pending its separate redesign.

## Motivation

This abstraction was motivated by the original `IrohDriver` and `apps/myelin` integration. Before the engine, `IrohDriver` accepted and stored a Tokio `Handle`, called `spawn` for accepts, reads, dials, and writes, and used `block_on` during synchronous construction and shutdown, with a legacy constructor that detected an ambient Tokio runtime or silently created one. Separately, `apps/myelin` constructed the Tokio runtime, constructed the swactor runtime, and manually sequenced protocol tick injection, Iroh ingress, `Runtime::tick()`, Iroh egress, and polling sleeps. The execution substrate was thus hardcoded and reinvented per crate (ambient `Handle::try_current()`, silently-owned runtimes, ad-hoc `block_on` sync facades, a mix of tokio tasks and std threads).

The engine resolves this. swactor consumes and retains the selected execution substrate—Tokio, std threads, JS, or another implementation—and exposes one explicit engine contract to integrations such as Iroh and Myelin. That engine hosts actor progression and the asynchronous or blocking side-work supporting actors as one system, with one lifecycle and one place controlling execution semantics. `IrohDriver` now receives a swactor engine handle rather than a raw Tokio handle, and application code no longer assembles an independent actor driver beside an independent task runtime. Core remains engine-independent and synchronous.

This process-local execution engine is distinct from Myelin's cluster-level `orchestration::engine_builder`, which acquires nodes, waits for convergence, assigns roles, and returns a cluster handle. A Myelin node owns a swactor execution engine; the two abstractions operate at different layers.

## 1. Purpose

swactor actors are synchronous, single-writer message handlers. Real systems need work actors cannot do inline: draining byte streams, running retry backoffs, polling on an interval, blocking GPU calls. That work lives in *tasks* on an execution substrate. Without an engine, each integration reinvents that substrate independently (see Motivation); the engine gives those tasks one swactor-owned home.

This spec defines the **engine**: a swactor-owned composite that retains a selected execution substrate, drives the core runtime, and hosts the supporting work that backs actors. Tokio/native, WASM/event-loop, embedded/cooperative, minimal std-thread, Go, JS-worker, and deterministic test schedulers are substrate implementations behind the same swactor engine contract. Authoring that contract from swactor's needs keeps execution ownership and semantics in swactor while allowing core and each substrate implementation to remain independently optimized.

## 2. Scope

**In scope**

- The engine interface: what swactor requires of an engine, and what an engine provides.
- The responsibility split between actor-workers and the engine.
- How the engine drives core through its existing synchronous tick semantics.
- The capability surface: tasks, timers, I/O, blocking, time.
- How engine-hosted work may communicate through integration-owned boundaries (non-normative).
- The invariants an engine must uphold.
- Reference instantiations (non-normative).

**Out of scope**

- Actor execution semantics — single-writer, per-(sender,target) FIFO, fairness, panic isolation. Those belong to the actor-worker / core.
- Backpressure policy. Producers and consumers share one engine; pressure handling is the application's decision, not swactor's.
- Cancellation and shutdown lifecycle (deferred; nice-to-have).
- Failure / observability propagation from engine-hosted work.
- Cross-process / cross-isolation delivery and serialization.
- Specific protocols and codecs (iroh/QUIC, telemetry framing). Those are crate logic built *on* the engine.
- Provider adapters, including the VastAI provider adapter. Their private runtimes, manually driven swactor runtimes, blocking facades, polling threads, and provider lifecycle are crate-level concerns built *beside* the engine, not on it; they require their own redesign rather than incremental engine migration.

## 3. Model

- The **core runtime** is constructed as `RuntimeParts`: a cloneable runtime handle plus a fixed set of owned workers. The handle owns shared routing, inboxes, and producer queues; each worker owns its actor pool and mailbox drains. Actor execution remains synchronous and single-writer; each worker exposes a synchronous transition that advances its state machine and returns immediately.
- The **engine** is a swactor-owned composite. It retains the selected execution substrate, retains the core runtime handle, moves every worker into a core driver, and does exactly two things:
  1. **Drives actor execution** — schedules worker transitions without application involvement.
  2. **Runs supporting work** — schedules the I/O, blocking calls, retries, and other long-lived flows that back actors.
- **The engine owns all progression.** Actor handlers never `.await`. Every handler is a synchronous transition that returns control immediately; the engine carries control flow across time.
- Actor execution and supporting work are not independently driven systems. They share one engine, one execution policy, and one lifecycle. An engine may use multiple internal pools, threads, scheduler domains, or substrate-native facilities to meet its progression and performance requirements.
- Integrations receive a cloneable engine handle through which they schedule supporting work. They do not receive or own the underlying Tokio, thread-pool, or host-runtime handle.

## 4. The engine interface

The interface is authored from swactor's needs. It is a **contract** — operations plus their semantics and invariants. A Rust trait is its canonical Rust binding; Go, JS, and other hosts implement the same contract natively. This spec defines the contract, not the Rust signature.

Constructing a swactor engine consumes configured `RuntimeParts` and the selected execution substrate, then establishes core driving for the engine's lifetime. Worker installation is internal engine behavior: applications and integrations do not register workers or receive core routing handles.

**The engine handle provides:**

| operation | meaning |
|---|---|
| `spawn(task)` | Schedule an async unit of work on the substrate. |
| `spawn_blocking(work)` | Schedule blocking CPU / syscall work off the async path. |
| `timer(delay)` / `interval(period)` | Schedule future or recurring work. |
| `now()` | The engine's monotonic clock. |

Time belongs to the engine rather than actor core. Engine-hosted work needs delays, intervals, retry deadlines, and timeouts; leaving those operations outside the contract would keep integrations such as Iroh and Myelin coupled to `tokio::time` or `std::thread::sleep`. An engine-owned monotonic clock also gives all hosted work one time source and allows a deterministic engine to substitute virtual time without changing integration code.

**Existing core integration.** The engine wraps and drives core without redefining it. Actor progression uses one core driver per worker; each driver calls that worker's synchronous transition and never touches any other worker. Message delivery continues through existing runtime and sender APIs. Core implements no engine trait, exposes no worker callback, and receives no engine-specific routing handle. Core owns actor logic, routing, inboxes, and delivery; the engine owns when core transitions run and schedules all supporting work on the same substrate. **Core only transitions. The engine drives.**

## 5. Driving workers

- Driving core is intrinsic to the engine and is established during engine construction. Application code and integrations never register or manually drive workers.
- The engine advances core through one long-lived driver task per worker. Each poll runs one worker transition to completion and returns control to the engine scheduler.
- **Non-reentrancy.** The engine must never invoke the same worker concurrently. Moving each worker into exactly one driver is the native Rust implementation's non-reentrancy proof.
- **Scheduling strategy is the engine's choice.** Tick cadence, batching, thread placement, and cooperative scheduling are implementation decisions, subject to the progress guarantees in §8.

## 6. Capability surface

The primitives an engine may provide. Capabilities are **per-implementation and discoverable**: each engine reports which it supports, and binding an engine that lacks a required capability fails at construction, never at runtime.

- **Tasks** — `spawn` of an async unit of work; the substrate's unit of concurrency.
- **Timers** — one-shot delay and recurring interval.
- **I/O** — streams, sockets, files, and protocol endpoints used by engine-hosted work. An engine may implement I/O through asynchronous operations, blocking operations on managed threads, callbacks, or host-native facilities. Integrations declare the I/O capabilities they require, and binding fails at construction when the selected engine cannot provide them.
- **Blocking** — `spawn_blocking` for CPU-bound or syscall work that must not stall the executor.
- **Time** — `now()`. In a test engine this is virtual, advanced by the test; this is what makes deterministic testing possible.

An engine that provides only tasks + time is still valid. Blocking and I/O are additional capabilities declared by integrations that require them.

### Native Rust binding scope

The implemented Rust SPI is the **native, sendable** binding: its task representation erases a task once when installed and requires `Send + Sync`. That `Send + Sync` model is the native binding — it is not a claim that this SPI is the Rust/WASM-local binding. The deterministic stepping backend proves executor and time independence on native Rust; it does not prove support for non-`Send` browser futures. A separate local-task binding for non-`Send` futures may be introduced when a real WASM implementation exists; until then the contract deliberately avoids conditional trait hierarchies, associated-future abstractions, or target-specific generic complexity.

## 7. Integration boundary (non-normative)

The engine executes opaque supporting work. It does not define how the results of that work become actor messages.

- Integrations own the handles and buffers through which their supporting work communicates. Valid patterns include capturing an existing core sender, writing to an integration-owned queue drained by actor-facing code, completing a callback or result channel, or producing no actor message at all.
- The engine does not define actor addressing, message delivery, mailbox ordering, or delivery guarantees. Those remain core and integration concerns.
- Actor state remains synchronous and single-writer. Supporting work must not retain mutable actor or worker state across engine scheduling points.

The current Iroh integration illustrates the queued pattern: background network readers write wire frames to an Iroh-owned ingress queue; an Iroh actor adapter drains and decodes those frames and calls the existing `Runtime::deliver_raw()` surface. Outbound actor frames pass through the integration-owned outbox to engine-hosted network writers. Another integration may instead capture an `ExternalSender` and deliver directly. Both patterns use the same engine without making actor delivery part of the engine contract.

## 8. Invariants

An engine must uphold:

- **Non-reentrant ticks.** At most one tick per worker at any instant.
- **Progress independence.** A long-running or blocked piece of supporting work must not stall actor ticks, and actor execution must not stall unrelated supporting work. The engine provides enough concurrency or cooperative scheduling for both to progress.
- **Actors never await.** No `.await` reaches actor code; the engine owns every long-lived flow.
- **Hot-path transparency.** The engine contract imposes no required per-item allocation, copy, serialization, actor hop, dynamic dispatch, or scheduler transition. Long-lived engine-hosted work may retain substrate-native I/O resources and transfer data directly through integration-owned buffers.

## 9. Reference instantiations (non-normative)

Illustrations of how each environment satisfies the contract — not prescription.

- **tokio.** A swactor engine owns a Tokio runtime and uses it to schedule both core ticks and supporting futures. I/O, blocking work, and actor progression share that runtime; integrations receive a swactor engine handle rather than a raw Tokio `Handle`.
- **std-thread.** A swactor engine owns its threads or small pool and schedules both core ticks and blocking supporting work there. It offers no native async I/O, but preserves the same ownership and progression contract.
- **deterministic test engine.** A single-threaded stepping scheduler advances core and supporting work under test control. It implements the same contract with no real network or threads.
- **Go / JS-worker (illustrative).** Core ticks and supporting work share goroutines plus the Go scheduler, or the JS event loop plus workers. Each implementation exposes only a swactor engine handle to integrations.

### Iroh / Myelin integration

1. `apps/myelin` constructs a swactor engine with a Tokio substrate.
2. The swactor engine retains Tokio, owns the core runtime, and drives actor execution.
3. `IrohDriver` receives a swactor engine handle rather than a raw Tokio `Handle`; accepts, reads, dials, writes, and blocking work are scheduled through that handle.
4. Actor execution and Iroh work therefore share one engine and lifecycle. Myelin does not assemble an independent actor driver beside an independent task runtime.

## 10. What this spec does not define

The boundary, stated plainly:

- Actor execution semantics (single-writer, FIFO, fairness, panic isolation).
- Backpressure.
- Cancellation and shutdown.
- Failure / observability propagation across integration boundaries.
- Cross-process / cross-isolation delivery and serialization.
- Specific protocols and codecs.
- Provider adapters (e.g. the VastAI provider adapter) and their private runtimes / provider lifecycle.
