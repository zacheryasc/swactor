# Runtime Architecture

The `Runtime` is the main entry point. It creates workers, owns the shared
infrastructure, and provides the public API for spawning actors and sending
messages.

## Structure

```
┌─ Runtime ─────────────────────────────────────────────────────────────────┐
│                                                                           │
│  config: RuntimeConfig         -- tunable knobs (see config.rs)           │
│  is_running: AtomicBool        -- shutdown flag, read by all workers      │
│                                                                           │
│  ┌─ Shared State (lives on Arc<Runtime>) ──────────────────────────────┐  │
│  │                                                                     │  │
│  │  address_map:      Arc<AddressMap>       -- actor -> worker lookup   │  │
│  │  inbox_registry:   Arc<InboxRegistry>    -- external inbox delivery  │  │
│  │  placement:        Placement             -- load-aware worker picker │  │
│  │  worker_stats:     Vec<Arc<WorkerStats>> -- atomic stat counters     │  │
│  │  extension:        Option<Arc<dyn RuntimeExtension>>                 │  │
│  │    (StdExtension holds: NameRegistry, MonitorRegistry,              │  │
│  │     GroupRegistry, WatchRegistry)                                   │  │
│  │                                                                     │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
│  ┌─ Channel Endpoints ─────────────────────────────────────────────────┐  │
│  │                                                                     │  │
│  │  transfer_txs: Vec<Sender<Envelope>>  -- one per worker (messages)  │  │
│  │  spawn_txs:    Vec<Sender<(Addr,Box)>> -- one per worker (spawns)   │  │
│  │                                                                     │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
│  tick_workers: RefCell<Vec<Worker>>  -- for tick(); run() drains these   │
│  worker_threads: Vec<OnceLock<Thread>>  -- for waking parked workers     │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

## Two Modes of Operation

```
  SINGLE-THREADED                        MULTI-THREADED
  ──────────────                         ──────────────

  let rt = Runtime::new(config);         let mut config = RuntimeConfig::default();
                                         config.num_threads = 4;
                                         let rt = Runtime::new(config);

  rt.spawn(my_actor)?;                   rt.spawn(my_actor)?;
  rt.send_to(addr, msg)?;               rt.send_to(addr, msg)?;

  loop { rt.tick(); }                    let handle = rt.run()?;
   ^                                      ^
   |                                      |
   caller drives each tick                workers run on their own threads
                                          handle.join() blocks until shutdown
```

Single-threaded mode keeps the `Worker` inline and requires the caller to
call `rt.tick()` to advance the simulation. This is useful for deterministic
testing, WASM, or game loops where you want frame-level control.

Multi-threaded mode consumes the `Runtime` via `run()`, wraps it in an
`Arc`, and spawns one OS thread per worker. Returns a `RuntimeHandle`.

## Ctx — the Actor Syscall Interface

When an actor's `handle()` method runs, it receives a `&Ctx`. This is the
only way for actors to interact with the outside world.

```
┌─ Ctx<'a> ─────────────────────────────────────────────────────────────────┐
│                                                                           │
│  inner:     &dyn ContextInner     -- polymorphic dispatch                 │
│  self_addr: ActorAddress          -- address of the current actor         │
│                                                                           │
│  ┌─ Core API ─────────────────────────────────────────────────────────┐  │
│  │                                                                     │  │
│  │  ctx.self_addr()                   -> ActorAddress                  │  │
│  │  ctx.send(addr, msg)               -> Result<(), Error>            │  │
│  │  ctx.spawn(actor)                  -> Result<ActorAddress, Error>   │  │
│  │  ctx.stop_self()                                                    │  │
│  │  ctx.stop_actor(addr)              -> Result<(), Error>             │  │
│  │  ctx.extension()                   -> Option<&dyn RuntimeExtension> │  │
│  │                                                                     │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
│  ┌─ Extension Traits (swactor-std) ──────────────────────────────────┐  │
│  │                                                                     │  │
│  │  CtxNaming:     spawn_named, where_is                               │  │
│  │  CtxMonitoring: monitor, demonitor                                  │  │
│  │  CtxWatching:   watch, unwatch                                      │  │
│  │  CtxGroups:     join_group, leave_group, publish, group_members     │  │
│  │  CtxTimers:     send_after_ticks, send_interval_ticks               │  │
│  │                                                                     │  │
│  │  These use ctx.extension() + downcast to StdExtension.              │  │
│  │  Also: spawn_restartable (via CtxNaming)                            │  │
│  │                                                                     │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
│  ┌─ ContextInner dispatch ─────────────────────────────────────────────┐  │
│  │                                                                     │  │
│  │  Five methods: send_any, spawn_any, request_stop,                   │  │
│  │                post_worker_request, extension                       │  │
│  │                                                                     │  │
│  │  In single-threaded mode:  inner = &Runtime                         │  │
│  │    send → transfer_txs[wid], spawn → spawn_txs[wid]                 │  │
│  │                                                                     │  │
│  │  In multi-threaded mode:   inner = &WorkerContext                   │  │
│  │    send → pending_local (same worker) or transfer_txs (cross)       │  │
│  │    spawn → spawn_txs[target_wid]                                    │  │
│  │    post_worker_request → worker_requests (drained phase 5.5)        │  │
│  │                                                                     │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

The `ContextInner` trait is the object-safe bridge. It's not public — actors
interact only through the typed `Ctx` wrapper.

## Inbox — Receiving Messages Outside the Runtime

`Inbox<M>` lets external code (the "main" thread, a game loop, an HTTP
handler, etc.) receive typed messages from actors.

```
  ┌─ Creation ─────────────────────────────────────────────────────────────┐
  │                                                                        │
  │  let inbox = rt.new_inbox::<MyResponse>()?;                            │
  │                                                                        │
  │  Under the hood:                                                       │
  │    addr     = ActorAddress::new_random()                               │
  │    receiver = Receiver::<M>::new(capacity)                             │
  │    sender   = receiver.new_sender()                                    │
  │    inbox_registry.register(addr, Arc::new(sender))                     │
  │                                                                        │
  └────────────────────────────────────────────────────────────────────────┘

  ┌─ Usage ────────────────────────────────────────────────────────────────┐
  │                                                                        │
  │  // give inbox.addr() to actors so they know where to reply            │
  │  rt.send_to(greeter, GreetMsg { return_addr: *inbox.addr() })?;       │
  │                                                                        │
  │  // poll for responses                                                 │
  │  if let Some(msg) = inbox.try_recv() { ... }                           │
  │                                                                        │
  └────────────────────────────────────────────────────────────────────────┘
```

## Ask — Typed Request-Response

`Ask<R>` wraps an `Inbox<R>` for convenient request-response:

```
  let response: Pong = rt.ask(actor, |reply_to| Ping { reply_to })?
      .recv_ticking(&rt, 10)?;    // tick until response or timeout
```

## Named Actors

Actors can be spawned with a registered name for discovery:

```
  let addr = rt.spawn_named("coordinator", my_actor)?;
  let found = rt.where_is("coordinator");   // -> Some(addr)
  // Names are auto-unregistered when the actor dies.
```

## Actor Monitoring (Death Watch)

Subscribe to death notifications via `ctx.monitor()`:

```
  let mref = ctx.monitor(target_addr);
  // When target dies, a Down { addr, reason } message arrives in
  // the watcher's normal handle() method. No special callback needed.
```

`StopReason`: `Normal` (graceful stop) | `Panicked` (panic, not restartable)

## Actor Groups (Pub-Sub)

Named groups for broadcast messaging:

```
  ctx.join_group("workers");
  ctx.publish("workers", StatusUpdate { ... });    // all members receive it
  // Members auto-removed on death. Groups auto-deleted when empty.
```

## Lifecycle Hooks

```
  fn on_start(&mut self, ctx: &Ctx) {}   -- called once before first message
  fn on_stop(&mut self, ctx: &Ctx) {}    -- called on graceful stop (not panic)
```

## Actor Recovery

Factory-based restart after panic:

```
  rt.spawn_restartable(actor, || MyActor::new(), 3)?;
  // On panic: mailbox cleared, factory creates fresh instance, up to 3 times.
  // After max_restarts: permanently poisoned.
```

## Supervision Trees

The `Supervisor` actor manages child actors with configurable restart policies:

```
  let sup = Supervisor::new(
      SupervisorStrategy::OneForOne,   // only the failed child is restarted
      // Also: OneForAll  — all children restarted when one fails
      //        RestForOne — failed child + all children after it restarted
      5,                                // max 5 restarts before meltdown
      vec![
          ChildSpec::new("worker_a", RestartPolicy::Permanent, |ctx| {
              ctx.spawn(MyWorker::new())
          }),
          ChildSpec::new("worker_b", RestartPolicy::Transient, |ctx| {
              ctx.spawn(MyOtherWorker::new())
          }),
      ],
  );
  let sup_addr = rt.spawn(sup)?;
```

Restart policies:
- `Permanent`: always restart
- `Transient`: restart only on panic, not normal stop
- `Temporary`: never restart

Strategies:
- `OneForOne`: only the failed child is restarted (default)
- `OneForAll`: all children are stopped and restarted when one fails
- `RestForOne`: the failed child and all children after it (in spec order) are restarted

Coordinated restart (OneForAll/RestForOne): the supervisor enters a `Stopping` phase,
sends stop signals to affected siblings, waits for all `Down` confirmations, then
restarts the full set in spec order. Already-dead children are handled immediately.

Meltdown: supervisor stops itself when total restarts exceed `max_restarts`.
Cascading: supervisor stops all children in `on_stop`.

## Router — Actor Pool with Message Routing

The `Router<M>` actor manages a pool of identical workers and distributes
incoming messages across them. Callers send messages to the router's address,
and the router forwards them according to the configured strategy.

```
  let router = Router::new(
      RoutingStrategy::RoundRobin,
      5,                                // pool size
      |ctx| ctx.spawn(MyWorker::new()), // worker factory
      10,                               // max restarts before meltdown
  );
  let router_addr = rt.spawn(router)?;
  rt.send_to(router_addr, WorkerMsg::DoWork(42))?;
```

Routing strategies:
- `RoundRobin`: sequential circular distribution
- `Random`: random worker selection
- `Broadcast`: clone message to all workers

Workers are monitored and automatically replaced on failure. Meltdown
protection stops the router when total restarts exceed `max_restarts`.

### handle_down Callback

Any actor can override `handle_down` to react to monitored actor deaths
without making `Down` its `Incoming` type:

```
  impl ActorInterface for MyActor {
      type Incoming = MyMsg;
      // ...
      fn handle_down(&mut self, ctx: &Ctx, down: Down) {
          // React to monitored actor death
      }
  }
```

### ctx.stop_actor

Actors can stop other actors from handlers:

```
  ctx.stop_actor(other_addr)?;  // PoisonPill semantics — queued after existing msgs
```

## Per-Worker Timers (swactor-std)

Deterministic tick-counting timers (not wall-clock). Requires `StdExtension`
and the `CtxTimers` extension trait:

```
  use swactor_std::CtxTimers;

  ctx.send_after_ticks(addr, msg, 5);       // one-shot: fires after 5 ticks
  ctx.send_interval_ticks(addr, msg, 10);   // repeating: every 10 ticks
```

The `TimerWheel` lives as a per-worker extension (`WorkerExtension`),
created by `StdExtension::create_worker_extension()`. Timer requests are
dispatched via `ctx.post_worker_request()` and processed in phase 5.5.

## RuntimeHandle

Returned by `run()`. Holds `Arc<Runtime>` and the thread `JoinHandle`s.

```
┌─ RuntimeHandle ───────────────────────────────────────────────────────────┐
│                                                                           │
│  runtime:  Arc<Runtime>           -- still usable for spawn/send/stats    │
│  threads:  Vec<JoinHandle<()>>    -- one per worker                       │
│                                                                           │
│  handle.shutdown()   →  runtime.is_running.store(false)                   │
│  handle.join()       →  waits for all worker threads to exit              │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

## RuntimeStats

`rt.stats()` (or `handle.runtime.stats()`) returns a snapshot:

```
┌─ RuntimeStats ────────────────────────────────────────────────────────────┐
│                                                                           │
│  num_workers: usize                                                       │
│  uptime_ms: u64                                                           │
│  actors: Vec<(ActorAddress, worker_id)>     -- from AddressMap snapshot   │
│  workers: Vec<WorkerInfo>                                                 │
│    ├─ id: usize                                                           │
│    ├─ num_actors: usize                     -- from atomic counter        │
│    ├─ mailbox_depth: usize                  -- total queued messages      │
│    ├─ messages_processed: u64               -- cumulative count           │
│    ├─ messages_dropped: u64                 -- overflow drops             │
│    ├─ panics: u64                                                         │
│    ├─ restarts: u64                                                       │
│    └─ stops: u64                                                          │
│  tick_timings: Vec<Vec<TickTiming>>         -- per-phase timing data      │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

Stats are published by workers via atomic stores at the end of each tick,
so they're always slightly stale but never block.
