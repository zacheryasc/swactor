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
│  │  address_map:    Arc<AddressMap>      -- actor -> worker lookup      │  │
│  │  inbox_registry: Arc<InboxRegistry>   -- external inbox delivery    │  │
│  │  placement:      Placement            -- round-robin worker picker  │  │
│  │  worker_stats:   Vec<Arc<WorkerStats>> -- atomic stat counters      │  │
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
│  ┌─ Mode ──────────────────────────────────────────────────────────────┐  │
│  │                                                                     │  │
│  │  SINGLE-THREADED:  single_worker: Some(RefCell<Worker>)             │  │
│  │  MULTI-THREADED:   pending_workers: Some(Vec<Worker>)               │  │
│  │                                                                     │  │
│  │  After run() is called, both are None — workers move to threads.    │  │
│  │                                                                     │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
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
│  ┌─ Public API ────────────────────────────────────────────────────────┐  │
│  │                                                                     │  │
│  │  ctx.self_addr()             -> ActorAddress                        │  │
│  │  ctx.send(addr, msg)         -> Result<(), Error>                   │  │
│  │  ctx.spawn(actor)            -> Result<ActorAddress, Error>         │  │
│  │                                                                     │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
│  ┌─ ContextInner dispatch ─────────────────────────────────────────────┐  │
│  │                                                                     │  │
│  │  In single-threaded mode:  inner = &Runtime                         │  │
│  │    send → transfer_txs[wid], spawn → spawn_txs[wid]                 │  │
│  │                                                                     │  │
│  │  In multi-threaded mode:   inner = &WorkerContext                   │  │
│  │    send → pending_local (same worker) or transfer_txs (cross)       │  │
│  │    spawn → spawn_txs[target_wid]                                    │  │
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

  ┌─ Delivery Path ────────────────────────────────────────────────────────┐
  │                                                                        │
  │  actor calls ctx.send(inbox_addr, response)                            │
  │       │                                                                │
  │       v                                                                │
  │  address_map.lookup(inbox_addr) → None  (inboxes aren't actors)        │
  │       │                                                                │
  │       v                                                                │
  │  inbox_registry.try_deliver(addr, msg)                                 │
  │       │                                                                │
  │       v                                                                │
  │  downcast Box<Any> → M, push into Receiver<M>                          │
  │                                                                        │
  └────────────────────────────────────────────────────────────────────────┘
```

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
│  actors: Vec<(ActorAddress, worker_id)>     -- from AddressMap snapshot   │
│  workers: Vec<WorkerInfo>                                                 │
│    ├─ id: usize                                                           │
│    ├─ num_actors: usize                     -- from atomic counter        │
│    ├─ mailbox_depth: usize                  -- total queued messages      │
│    └─ messages_processed: u64               -- cumulative count           │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

Stats are published by workers via atomic stores at the end of each tick,
so they're always slightly stale but never block.
