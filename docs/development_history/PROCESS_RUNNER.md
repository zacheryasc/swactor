# Process Runner Design: Async Process Management in Swactor

## Context

Swactor is a synchronous, tick-based actor framework (Erlang-inspired). Actors must return quickly from `handle()` — blocking stalls the entire worker thread. There is no built-in async I/O.

The goal: let actors manage long-lived async "processes" — OS subprocesses and SSH shells — with full lifecycle control. Must support both interactive use (live shell, bidirectional real-time I/O) and automated execution (run commands, stream output, report exit).

Constraints from discussion:
- Backends: SSH + local processes (two backends, not more)
- Scale: Architecture should support thousands; first implementation handles tens
- This is a standalone new feature — not related to or derived from the CI runner system

---

## Architecture: State Machine + Driver + Process-as-Actor

### Data Flow (full picture)

```
OS process stdout/stderr
       │ (background thread reads pipe)
       ▼
  EventQueue (Arc<SegQueue>) — shared lock-free buffer
       │ (background thread calls ProcessWaker → ExternalSender → PollTick)
       ▼
  Actor handle(PollTick)
       │ calls driver.poll() which drains EventQueue
       ▼
  Vec<ProcessEvent>
       │
       ▼
  session.apply(event) → Vec<ProcessAction>
       │
       ├─ Driver commands → driver.execute(action) → OS I/O
       ├─ Notifications → ctx.send(subscriber, ProcessNotification)
       └─ SelfTerminate → ctx.stop_self()
```

### The Layers

| Layer | Purpose | Status |
|-------|---------|--------|
| 1 — ProcessSession | Pure-logic state machine | **Implemented** |
| 2 — ProcessDriver trait + MockDriver | Driver abstraction + test double | **Implemented** |
| 3 — Process Actor + ExternalSender | Swactor integration, waker, event queue | **Implemented** |
| 4 — LocalDriver | `std::process::Command` + pipe I/O + signal | **Implemented** |
| 5 — SshDriver | SSH library + channel I/O | Not started |

---

## Implemented: Layers 1 + 2 (Pure Logic)

Crate: `crates/process/` (`swactor-process`)

### Layer 1 — ProcessSession (State Machine)

The core state machine. Pure logic, no I/O, fully deterministic.

**States:** `Starting` → `Running` → `Stopping` → `Exited`

State transitions are monotonic — the state never goes backward. `Exited` is terminal.

**Construction:**

```rust
let (session, initial_actions) = ProcessSession::new(spec);
// initial_actions == [SpawnProcess { spec }]
// session.state() == Starting
```

**Event loop:**

```rust
let actions = session.apply(event);
for action in actions {
    match action {
        ProcessAction::SpawnProcess { .. } |
        ProcessAction::WriteStdin { .. } |
        ProcessAction::SendSignal { .. } |
        ProcessAction::ResizePty { .. } |
        ProcessAction::CloseStdin |
        ProcessAction::ScheduleKillTimeout { .. } => driver.execute(action),

        ProcessAction::NotifyStarted { subscribers } |
        ProcessAction::NotifyOutput { subscribers, .. } |
        ProcessAction::NotifyExited { subscribers, .. } |
        ProcessAction::NotifyError { subscribers, .. } => { /* send to subscribers */ }

        ProcessAction::SelfTerminate => { /* actor stops itself */ }
    }
}
```

**Key invariants (all verified by property-based tests):**
- Invalid events produce `NotifyError` actions — never panic
- `SelfTerminate` is always the last action when entering `Exited`
- State monotonicity: Starting ≤ Running ≤ Stopping ≤ Exited
- Subscriber count always matches add/remove operations
- No panics for arbitrary event sequences

**Event handling by state:**

| Event | Starting | Running | Stopping | Exited |
|-------|----------|---------|----------|--------|
| Started | → Running (+ NotifyStarted) | error | error | error |
| SpawnFailed | → Exited (+ NotifyError + SelfTerminate) | error | error | error |
| OutputReceived | error | NotifyOutput | NotifyOutput | error |
| Exited | error | → Exited (+ NotifyExited + SelfTerminate) | → Exited (+ NotifyExited + SelfTerminate) | error |
| ConnectionLost | error | → Exited (+ NotifyError + SelfTerminate) | → Exited (+ NotifyError + SelfTerminate) | error |
| WriteStdin | error | WriteStdin (or buffer/error) | error | error |
| SendSignal | error | SendSignal | SendSignal (escalation) | error |
| ResizePty | error | ResizePty | error | error |
| CloseStdin | error | CloseStdin (+ clear buffer) | CloseStdin (+ set flag) | error |
| CloseRequested | set deferred flag | → Stopping (+ SendSignal Terminate [+ ScheduleKillTimeout]) | no-op | error |
| KillTimeout | silent | silent | SendSignal Kill | silent |
| Subscribe | add subscriber | add subscriber | add subscriber | add subscriber |
| Unsubscribe | remove subscriber | remove subscriber | remove subscriber | remove subscriber |
| StdinWritten | update flow | update flow + drain buffer | update flow | update flow |
| SignalSent | silent | silent | silent | silent |
| PtyResized | silent | silent | silent | silent |

**Special behaviors:**
- **Close-before-start:** If `CloseRequested` arrives in `Starting`, a flag is set. When `Started` arrives, the session transitions through Running straight to Stopping and emits `SendSignal(Terminate)` (plus `ScheduleKillTimeout` if configured).
- **Kill timeout:** When `spec.kill_timeout` is `Some(duration)`, entering `Stopping` emits `ScheduleKillTimeout { duration }` alongside `SendSignal(Terminate)`. If the process hasn't exited when the timeout fires, the `KillTimeout` event triggers `SendSignal(Kill)`. `KillTimeout` in non-Stopping states is silently consumed (harmless late arrival after the process already exited).
- **Backpressure:** When `spec.stdin_buffer_limit` is `Some(limit)` and `pending_stdin_bytes >= limit`, `WriteStdin` events are buffered in a `VecDeque` instead of emitting actions. When `StdinWritten` acks reduce `pending_stdin_bytes` below the limit, buffered writes drain in FIFO order. The buffer is cleared on `CloseRequested`, `CloseStdin`, `ConnectionLost`, and `Exited`. When `stdin_buffer_limit` is `None`, all writes pass through immediately (original behavior).
- **FlowControl:** `pending_stdin_bytes` is incremented on `WriteStdin` emission, decremented on `StdinWritten` receipt (saturating).
- **Stdin closed:** Once `CloseStdin` is applied, further `WriteStdin` events produce `InvalidState` errors. Duplicate `CloseStdin` is a no-op. Closing stdin also clears any buffered writes.
- **Late acks in Exited:** `StdinWritten`, `SignalSent`, `PtyResized`, and `KillTimeout` are silently consumed in all states (including Exited) — they never produce errors.

### Types

**ProcessSpec** — describes how to spawn a process:
- `command: String`, `args: Vec<String>`, `env: HashMap<String, String>`
- `working_dir: Option<String>`, `mode: ProcessMode`, `initial_pty_size: Option<PtySize>`
- `kill_timeout: Option<Duration>` — escalate SIGTERM → SIGKILL after this duration (None = no escalation)
- `stdin_buffer_limit: Option<usize>` — buffer stdin writes when pending bytes exceed limit (None = unlimited)

**ProcessMode** — `Interactive` | `Automated` (Copy)

**ExitStatus** — `Code(i32)` | `Signal(i32)` | `Unknown` (Copy)

**Signal** — `Terminate` | `Kill` | `Hangup` | `Interrupt` | `Other(i32)` (Copy)

**ProcessError** — `SpawnFailed { reason }` | `ConnectionLost { reason }` | `InvalidState { attempted, current_state }`

**OutputStream** — `Stdout` | `Stderr` (Copy)

**SubscriberSet** — deduplicated `Vec<ActorAddress>` with linear-scan dedup. Methods: `add()`, `remove()`, `snapshot()`, `count()`.

### Layer 2 — ProcessDriver Trait + MockDriver

```rust
pub trait ProcessDriver: Send {
    fn execute(&mut self, action: ProcessAction);
    fn poll(&mut self) -> Vec<ProcessEvent>;
}
```

**MockDriver** — test-oriented implementation:
- `inject(event)` / `inject_many(events)` — queue events for `poll()`
- `executed_actions()` — view recorded actions
- `take_executed_actions()` — take + clear recorded actions
- `pending_event_count()` — number of queued events
- `poll()` drains all pending events, `execute()` records actions

---

## Implemented: Layers 3 + 4 (Actor Integration + Local OS Processes)

### ExternalSender (swactor core primitive)

A `Clone + Send + Sync` handle for injecting messages into actor mailboxes from any thread. Lives in the `swactor` crate (because `Envelope` and `AddressMap` are `pub(crate)`).

```rust
// Create from a runtime
let sender = runtime.create_sender();

// Use from any thread (including I/O background threads)
sender.send_to(actor_addr, MyMessage { ... })?;
```

**Implementation:** Clones of the runtime's `Arc<AddressMap>`, per-worker `Sender<Envelope>` channels, and `Arc<Vec<OnceLock<Thread>>>` for worker thread unparking. The `send_to` method looks up the actor's worker, pushes an envelope, and unparks the worker thread.

**Changes to swactor core:**
- `src/channel.rs` — Added `Clone` for `Sender<T>` (clones the inner `Arc`)
- `src/runtime.rs` — Changed `worker_threads` from `Vec<OnceLock<Thread>>` to `Arc<Vec<OnceLock<Thread>>>`, added `ExternalSender` struct and `Runtime::create_sender()` factory

### Layer 3 — Process Actor

**`ProcessActor<D: ProcessDriver>`** — generic actor implementing `ActorInterface` with `Incoming = ProcessCommand`.

**Message types:**

```rust
pub enum ProcessCommand {
    WriteStdin { data: Vec<u8> },
    SendSignal { signal: Signal },
    ResizePty { size: PtySize },
    CloseStdin,
    Close,
    Subscribe { address: ActorAddress },
    Unsubscribe { address: ActorAddress },
    PollTick,  // internal: sent by waker from I/O threads
}

pub enum ProcessNotification {
    Started { process: ActorAddress },
    Output { process: ActorAddress, data: Vec<u8>, stream: OutputStream },
    Exited { process: ActorAddress, status: ExitStatus },
    Error { process: ActorAddress, error: ProcessError },
}
```

**Handle ordering:** Commands are processed first, then I/O events are drained. This ensures `Subscribe` registers the subscriber before `Started` (or other buffered events) get dispatched. `PollTick` has no command effect — it just triggers the drain.

**Event queue (`EventQueue`):** Thin wrapper around `Arc<SegQueue<ProcessEvent>>`. I/O threads push events; `driver.poll()` drains them.

**Waker (`ProcessWaker`):** `Arc<dyn Fn() + Send + Sync>` — constructed with a closure that sends `PollTick` via `ExternalSender`. I/O threads call `waker.wake()` after pushing events.

**Factory functions:**

```rust
// Spawn with real OS subprocess
let addr = spawn_local_process(ctx, &sender, spec)?;

// Spawn with custom driver (for testing)
let addr = spawn_process(ctx, &sender, spec, driver, waker_slot)?;
```

The factory creates the driver, session, and actor, spawns it, then fills the waker slot with a closure that sends `PollTick` to the actor's address.

### Layer 4 — LocalDriver

Real OS process management via `std::process::Command` with piped I/O.

**Components:**

| File | Purpose |
|------|---------|
| `local/mod.rs` | `LocalDriver` struct, `ProcessDriver` impl, process spawning |
| `local/pipes.rs` | Background thread reading stdout/stderr pipes (8KB buffer) |
| `local/signal.rs` | `Signal` → libc constant mapping, `kill()` wrapper |
| `local/wait.rs` | Background `waitpid()` thread with WIFEXITED/WIFSIGNALED decoding |

**Thread structure per process:**
- 1 stdout reader thread
- 1 stderr reader thread
- 1 waitpid thread

Each thread pushes events to the shared `EventQueue` and calls `waker.wake()`.

**Drop behavior:** Closes stdin, kills the process, waits for exit.

**PTY support:** Not yet implemented — `ResizePty` is a no-op that returns a `PtyResized` ack. Pipe-based I/O only in this phase.

---

## File Structure

```
swactor (root crate):
  src/
    channel.rs       — + Clone for Sender<T>
    runtime.rs       — + ExternalSender, create_sender(), Arc<worker_threads>

crates/process/ (swactor-process):
  Cargo.toml         — + crossbeam-queue, libc deps
  src/
    lib.rs           — module declarations + re-exports
    types.rs         — ProcessSpec, ProcessMode, ExitStatus, Signal, PtySize, etc.
    event.rs         — ProcessEvent enum
    action.rs        — ProcessAction enum + OutputStream
    subscriber.rs    — SubscriberSet
    session.rs       — ProcessSession state machine
    driver.rs        — ProcessDriver trait
    mock.rs          — MockDriver
    queue.rs         — EventQueue (Arc<SegQueue>)
    waker.rs         — ProcessWaker (Arc<dyn Fn>)
    message.rs       — ProcessCommand, ProcessNotification
    actor.rs         — ProcessActor<D> impl ActorInterface
    spawn.rs         — spawn_local_process(), spawn_process() factory functions
    local/
      mod.rs         — LocalDriver struct + ProcessDriver impl
      pipes.rs       — Pipe reader background threads
      signal.rs      — OS signal delivery
      wait.rs        — waitpid background thread
  tests/
    session_scenarios.rs  — 26 session state machine scenario tests
    proptest_session.rs   — 5 property-based session tests (KillTimeout included in arb_event)
    actor_scenarios.rs    — 6 actor integration tests (TestDriver)
    local_driver.rs       — 6 LocalDriver integration tests (real processes)
    e2e_process.rs        — 2 end-to-end tests (Runtime + LocalDriver + real processes)
```

---

## Test Coverage

### Layers 1 + 2 — Session + MockDriver (31 tests)

**Scenario tests** (26 tests in `tests/session_scenarios.rs`):
1. Happy path automated: new → Started → OutputReceived×N → Exited(0)
2. Interactive session with subscriber lifecycle (add/remove, verify notification membership)
3. Spawn failure → error notification + SelfTerminate
4. Connection loss mid-run → Exited with Unknown status
5. Close before start → deferred SIGTERM on belated start
6. Invalid event in Starting → NotifyError (no panic)
7. Invalid event in Exited → NotifyError (no panic)
8. Stdin closed then write → NotifyError
9. MockDriver round-trip (driver + session in simulated tick loop)
10. Signal escalation in Stopping (Kill after Terminate)
11. Late acks in Exited silently consumed
12. Flow control tracks pending stdin bytes (including saturating subtract)
13. CloseStdin allowed in Stopping
14. Connection loss in Stopping → Exited
15. Duplicate CloseRequested in Stopping → no-op
16. CloseRequested with kill_timeout emits both SendSignal{Terminate} and ScheduleKillTimeout
17. Close-before-start with kill_timeout schedules timer on belated start
18. KillTimeout in Stopping → SendSignal{Kill}, state stays Stopping
19. KillTimeout silently consumed in Starting, Running, Exited
20. CloseRequested without kill_timeout emits no ScheduleKillTimeout
21. Full escalation flow: CloseRequested → KillTimeout → Exited{Signal(9)}
22. Backpressure buffers writes when pending bytes exceed limit
23. StdinWritten ack drains buffered chunks in FIFO order
24. CloseRequested clears stdin buffer
25. No backpressure when limit is None (all writes pass through)
26. Exited clears stdin buffer

**Property-based tests** (5 tests in `tests/proptest_session.rs`):
1. No panics for arbitrary event sequences (up to 50 events, including KillTimeout)
2. Exited is terminal (state never leaves Exited)
3. SelfTerminate always last action when entering Exited
4. Subscriber count matches add/remove operations
5. State monotonicity (state ordinal never decreases)

### Layer 3 — Actor Integration (6 tests)

Tests in `tests/actor_scenarios.rs` using a `TestDriver` (shared `EventQueue` + recorded actions):

1. **Happy path** — spawn → Started → Output → Exited → subscriber gets all notifications → actor stops
2. **PollTick drains queued events** — three events buffered, single PollTick delivers all three notifications
3. **Close triggers graceful shutdown** — Close command produces SIGTERM via driver
4. **WriteStdin/SendSignal forwarded** — commands reach the driver as actions
5. **Spawn failure** — error notification sent to subscriber, actor self-terminates
6. **Subscribe/Unsubscribe routing** — two subscribers, unsubscribe one, only remaining gets subsequent notifications

### Layer 4 — LocalDriver Integration (6 tests)

Tests in `tests/local_driver.rs` using real OS processes, no actor layer:

1. **`echo hello`** — Started + OutputReceived("hello\n") + Exited(0)
2. **`cat` stdin echo** — write "ping\n" → read "ping\n" back → close stdin → Exited(0)
3. **`sleep 60` + SIGTERM** — Started → send Terminate → Exited(Signal)
4. **Bad command** → SpawnFailed
5. **`seq 1 10000`** — large output integrity (no data loss, correct start/end)
6. **Kill timeout escalation** — spawn SIGTERM-ignoring process, ScheduleKillTimeout fires KillTimeout, SIGKILL terminates it

### End-to-End (2 tests)

Tests in `tests/e2e_process.rs` — full stack (Runtime + ExternalSender + ProcessActor + LocalDriver + real process):

1. **`echo hello` lifecycle** — spawn, subscribe, verify Started → Output("hello") → Exited(0) in order
2. **Bad command** — spawn nonexistent binary, verify Error notification arrives

---

## Design Decisions Made

1. **ExternalSender over WorkerExtension:** The I/O → actor bridge is a general-purpose swactor core primitive, not process-specific. Any crate can use `ExternalSender` to inject messages from background threads.

2. **Handle ordering (command first, then drain):** Processing the incoming command before draining I/O events ensures that `Subscribe` registers the subscriber before buffered events (like `Started`) are dispatched. This avoids a race where early lifecycle events are sent to an empty subscriber list.

3. **ProcessActor is generic over `D: ProcessDriver`:** Enables testing with `TestDriver` while production uses `LocalDriver`. No trait object overhead.

4. **Thread-per-pipe model:** Each LocalDriver spawns 3 threads (stdout reader, stderr reader, waitpid). Simple, debuggable, correct for Phase 1 (tens of processes).

5. **EventQueue is lock-free:** Uses `crossbeam_queue::SegQueue` — no contention between I/O writer threads and the actor's poll draining.

6. **Waker uses OnceLock:** The waker slot (`Arc<OnceLock<ProcessWaker>>`) is filled after the actor address is known. I/O threads that call `waker.get()` before it's set simply skip the wake — events accumulate in the EventQueue and are drained on the next message.

---

## Next Steps

### Near-term

1. **PTY support for Interactive mode** — The `LocalDriver` currently uses pipes only. Interactive mode needs PTY allocation (via raw libc: `openpty()` → `fork()` → `setsid()` + `ioctl(TIOCSCTTY)` + `dup2` + `execvp`), `SIGWINCH` for resize, and merged stdout/stderr on a single PTY master FD. The `ResizePty` action is already wired through as a no-op.

2. **Output buffering policies** — Subscribers currently receive every raw byte chunk. Add optional line-buffering or size-buffering in the session layer for consumers that want complete lines.

### Layer 5 — SshDriver

SSH-based process management. Same `ProcessDriver` trait, different backend.

**Open decisions:**
- **SSH library:** `russh` (pure Rust, async — needs tokio bridge) vs. `ssh2` (libssh2 bindings, synchronous — fits the thread model naturally)
- **Authentication:** Password, key file, agent forwarding, or pluggable credential provider
- **Connection multiplexing:** One SSH connection per process actor, or connection pool with multiple channels
- **Health monitoring:** Heartbeat/keepalive to detect connection drops → `ConnectionLost` events

### Scaling Path

The architecture isolates scaling concerns in the driver layer:

- **Phase 1 (tens):** Each driver spawns OS threads for I/O. Simple, debuggable. ← **current**
- **Phase 2 (hundreds):** Shared thread pool for driver I/O. Replace per-process threads with a pool that multiplexes reads across processes.
- **Phase 3 (thousands):** Async internals (tokio tasks for I/O). State machine and actor layers unchanged — only `ProcessDriver` implementations change.

---

## Alternative Approaches Considered

### WorkerExtension Approach

Managing processes as a per-worker extension (like TimerWheel). Rejected because:
- Ties processes to specific workers, complicating supervision
- Processes can't benefit from the actor model's naming, grouping, and monitoring
- The API would be less intuitive than "send a message to the process"
- Tick-bound latency is problematic for interactive use

### Pure Bridge Actor Approach

A single centralized bridge actor owning all processes (like IrohDriver). Rejected as the primary design because:
- Doesn't give individual processes actor identity — can't supervise, name, or monitor them independently
- Centralizes failure — the bridge dying kills all processes
- However, this pattern does appear inside the recommended approach: the driver layer within each process actor is essentially a tiny bridge

### Pure Process-as-Actor (without state machine)

Just actors with embedded I/O logic, no state machine separation. Rejected because:
- Untestable without real processes or SSH connections
- Can't simulate
- Backend-specific logic (SSH vs. local) interleaved with lifecycle logic
