# Worker Thread Architecture

## Structure

```
┌─ Worker ───────────────────────────────────────────────────────────────┐
│                                                                        │
│  id: WorkerId                                                          │
│                                                                        │
│  ┌─ spawn_rx ─────────────────────┐  ┌─ transfer_rx ──────────────────┐│
│  │ Receiver<(Addr, Box<AnyActor>)>│  │ Receiver<Envelope>             ││
│  │                                │  │                                ││
│  │ from: Runtime.spawn()          │  │ from: other workers, Runtime   ││
│  │       ctx.spawn()              │  │       ctx.send()               ││
│  └────────────────────────────────┘  └────────────────────────────────┘│
│                                                                        │
│  ┌─ ActorPool ──────────────────────────────────────────────────────┐  │
│  │                                                                  │  │
│  │  actors: HashMap<ActorAddress, ActorSlot>                        │  │
│  │                                                                  │  │
│  │  ┌─ ActorSlot [addr_0] ──────────────────────────────────────┐   │  │
│  │  │                                                           │   │  │
│  │  │  ┌─ mailbox ──────────────────────────────────────────┐   │   │  │
│  │  │  │ VecDeque<Box<dyn Any + Send>>                      │   │   │  │
│  │  │  │                                                    │   │   │  │
│  │  │  │ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                    │   │   │  │
│  │  │  │ │ msg │ │ msg │ │ msg │ │ ... │  <- push_back      │   │   │  │
│  │  │  │ └─────┘ └─────┘ └─────┘ └─────┘                    │   │   │  │
│  │  │  │ pop_front ->                 untyped; Box<Any>     │   │   │  │
│  │  │  └────────────────────────────────────────────────────┘   │   │  │
│  │  │                                                           │   │  │
│  │  │  ┌─ actor ────────────────────────────────────────────┐   │   │  │
│  │  │  │ Box<dyn AnyActor>                                  │   │   │  │
│  │  │  │                                                    │   │   │  │
│  │  │  │ wraps Actor<A>(A) where A: ActorInterface          │   │   │  │
│  │  │  │                                                    │   │   │  │
│  │  │  │ handle_any(ctx, msg):                              │   │   │  │
│  │  │  │   downcast Box<Any> -> A::Incoming                 │   │   │  │
│  │  │  │   ok  -> A.handle(ctx, typed_msg)                  │   │   │  │
│  │  │  │   err -> silently drop                             │   │   │  │
│  │  │  └────────────────────────────────────────────────────┘   │   │  │
│  │  │                                                           │   │  │
│  │  └───────────────────────────────────────────────────────────┘   │  │
│  │                                                                  │  │
│  │  ┌─ ActorSlot [addr_1] ──────────────────────────────────────┐   │  │
│  │  │ ...                                                       │   │  │
│  │  └───────────────────────────────────────────────────────────┘   │  │
│  │                                                                  │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                        │
│  ┌─ worker_ext: Option<Box<dyn WorkerExtension>> ─────────────────┐  │
│  │  Per-worker extension state, created by RuntimeExtension        │  │
│  │  factory. StdExtension provides a TimerWheel here.              │  │
│  │  on_tick() → fire due messages    (phase 2.5)                   │  │
│  │  handle_request() → schedule timers etc.  (phase 5.5)           │  │
│  │  gc_dead() → clean up dead actor state    (phase 7)             │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

## Shared State (borrowed via TickContext)

Lives on `Arc<Runtime>`, shared read-only across all worker threads.

```
┌─ TickContext<'a> ──────────────────────────────────────────────────────┐
│                                                                        │
│  address_map:       &AddressMap        -- ActorAddress -> WorkerId     │
│  transfer_txs:      &[Sender]          -- one Sender per worker        │
│  spawn_txs:         &[Sender]          -- one Sender per worker        │
│  placement:         &Placement         -- load-aware worker picker     │
│  inbox_registry:    &InboxRegistry     -- external Inbox<M> receivers  │
│  config:            &RuntimeConfig     -- budget, backoff, etc.        │
│  extension:         Option<&dyn RuntimeExtension> -- shared ext        │
│  stats_hook:        Option<&dyn StatsHook> -- per-tick stats callback  │
│  worker_threads:    &[OnceLock<Thread>] -- for unpark on send/spawn   │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

## Run Loop

```
┌─ Worker::run ──────────────────────────────────────────────────────────┐
│                                                                        │
│  ┌────────────────────────────────────┐                                │
│  │       is_running.load() ?          │                                │
│  └──────────┬─────────────────────────┘                                │
│         yes │                                                          │
│             v                                                          │
│  ┌────────────────────────────────────┐                                │
│  │        tick_once(&tc)              │─────────┐                      │
│  └──────────┬─────────────────────────┘         │                      │
│             │                                    │                     │
│        ┌────┴────┐                               │                     │
│        v         v                               │                     │
│     did work   no work                           │                     │
│        │         │                               │                     │
│        v         v                               │                     │
│  idle = 0    idle++                              │                     │
│        │         │                               │                     │
│        │    ┌────┴──────────────────────────────┐  │                     │
│        │    │ idle < spin_thr:  spin            │  │                     │
│        │    │ idle < yield_thr: yield_now       │  │                     │
│        │    │ else: park_timeout(incr, capped)  │  │                     │
│        │    │   (instant wake via Thread::unpark │  │                     │
│        │    │    when send/spawn targets worker) │  │                     │
│        │    └──────────────────────────────┬─────┘  │                     │
│        │                              │          │                     │
│        └──────────┬───────────────────┘          │                     │
│                   │                              │                     │
│                   └──── loop back ───────────────┘                     │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

## Tick Once (eight phases)

```
┌─ tick_once ────────────────────────────────────────────────────────────┐
│                                                                        │
│  PHASE 1 --- Drain Spawn Queue                                         │
│  ┌────────────────────────────────────────────────────────────────────┐│
│  │                                                                    ││
│  │  spawn_rx --try_recv()--> (addr, Box<dyn AnyActor>)                ││
│  │                                   │                                ││
│  │                                   v                                ││
│  │                          pool.insert(addr, actor)                  ││
│  │                            started: false                          ││
│  │                            stopping: false                         ││
│  │                            mailbox_capacity: from config           ││
│  │                                                                    ││
│  └────────────────────────────────────────────────────────────────────┘│
│      │                                                                 │
│      v                                                                 │
│  PHASE 2 --- Drain Transfer Queue                                      │
│  ┌────────────────────────────────────────────────────────────────────┐│
│  │                                                                    ││
│  │  transfer_rx --try_recv()--> Envelope { dest, payload }            ││
│  │                                         │                          ││
│  │                                         v                          ││
│  │                             pool.deliver(&dest, payload)           ││
│  │                               (enforces mailbox_capacity;          ││
│  │                                drop newest/oldest on overflow)     ││
│  │                                                                    ││
│  └────────────────────────────────────────────────────────────────────┘│
│      │                                                                 │
│      v                                                                 │
│  PHASE 2.5 --- Fire Per-Worker Extension                                │
│  ┌────────────────────────────────────────────────────────────────────┐│
│  │                                                                    ││
│  │  worker_ext.on_tick() → Vec<(dest, msg)>                          ││
│  │    (StdExtension provides TimerWheel: advances tick, fires due)    ││
│  │       │                                                            ││
│  │       v                                                            ││
│  │  for (dest, msg) in ext_msgs:                                      ││
│  │    route_to_pool_or_remote(pool, tc, dest, msg)                    ││
│  │      local → pool.deliver | cross → transfer_tx | → inbox_registry ││
│  │                                                                    ││
│  └────────────────────────────────────────────────────────────────────┘│
│      │                                                                 │
│      v                                                                 │
│  PHASE 3 --- Tick All Actors                                           │
│  ┌────────────────────────────────────────────────────────────────────┐│
│  │                                                                    ││
│  │  ┌─ WorkerContext (on stack) ─────────────────────────────────┐    ││
│  │  │  implements ContextInner                                   │    ││
│  │  │  pending_local:    RefCell<Vec<(Addr, Box<Any>)>>          │    ││
│  │  │  stop_requests:    RefCell<Vec<ActorAddress>>              │    ││
│  │  │  worker_requests:  RefCell<Vec<Box<dyn Any + Send>>>       │    ││
│  │  └────────────────────────────────────────────────────────────┘    ││
│  │                                                                    ││
│  │  for each (addr, slot) in pool:                                    ││
│  │    if poisoned or stopping → clear mailbox, skip                   ││
│  │                                                                    ││
│  │    ┌─ on_start (once per actor) ──────────────────────────────┐    ││
│  │    │  if !slot.started:                                       │    ││
│  │    │    catch_unwind(actor.on_start(&ctx))                    │    ││
│  │    │      panic → poisoned (immediate, no messages)           │    ││
│  │    │      ok    → started = true                              │    ││
│  │    └──────────────────────────────────────────────────────────┘    ││
│  │                                                                    ││
│  │    ┌─ message loop (budget-limited) ──────────────────────────┐    ││
│  │    │  repeat up to `budget` times (budget=0 → unlimited):     │    ││
│  │    │    msg = slot.mailbox.pop_front()                        │    ││
│  │    │                                                          │    ││
│  │    │    if msg is StopSignal:                                 │    ││
│  │    │      slot.stopping = true; clear mailbox; break          │    ││
│  │    │                                                          │    ││
│  │    │    catch_unwind(actor.handle_any(&ctx, msg))             │    ││
│  │    │      panic → try_restart (factory) or poison             │    ││
│  │    │      ok    → count += 1                                  │    ││
│  │    │                                                          │    ││
│  │    │    if stop_requests contains addr:                       │    ││
│  │    │      slot.stopping = true; clear mailbox; break          │    ││
│  │    └──────────────────────────────────────────────────────────┘    ││
│  │                                                                    ││
│  │    ┌─ WorkerContext routes ─────────────────────────────────────┐  ││
│  │    │                                                            │  ││
│  │    │  send_any(addr, msg):                                      │  ││
│  │    │    ┌──────────────┬──────────────┬─────────────────┐       │  ││
│  │    │    │ same worker  │ other worker │ unknown addr    │       │  ││
│  │    │    │              │              │                 │       │  ││
│  │    │    │ pending_     │ transfer_tx  │ inbox_registry  │       │  ││
│  │    │    │  local.push()│  + unpark    │  .try_deliver() │       │  ││
│  │    │    └──────────────┴──────────────┴─────────────────┘       │  ││
│  │    │                                                            │  ││
│  │    │  spawn_any(addr, actor):                                   │  ││
│  │    │    wid = placement.next_worker()  (load-aware)             │  ││
│  │    │    address_map.insert(addr, wid)                           │  ││
│  │    │    spawn_txs[wid].send((addr, actor)) + unpark             │  ││
│  │    │                                                            │  ││
│  │    │  request_stop(addr):      → stop_requests.push(addr)       │  ││
│  │    │  post_worker_request(r): → worker_requests.push(r)        │  ││
│  │    │  extension():            → tc.extension                    │  ││
│  │    │                                                            │  ││
│  │    └────────────────────────────────────────────────────────────┘  ││
│  │                                                                    ││
│  └────────────────────────────────────────────────────────────────────┘│
│      │                                                                 │
│      v                                                                 │
│  PHASE 4 --- Drain Spawn Queue (again)                                 │
│  ┌────────────────────────────────────────────────────────────────────┐│
│  │  Actors spawned during phase 3 must be in the pool before         ││
│  │  pending_local delivery (phase 5).                                ││
│  └────────────────────────────────────────────────────────────────────┘│
│      │                                                                 │
│      v                                                                 │
│  PHASE 5 --- Drain Pending Local                                       │
│  ┌────────────────────────────────────────────────────────────────────┐│
│  │                                                                    ││
│  │  for (addr, msg) in pending_local.into_inner():                    ││
│  │      pool.deliver(&addr, msg)                                      ││
│  │  these sit in the mailbox until NEXT tick                          ││
│  │                                                                    ││
│  └────────────────────────────────────────────────────────────────────┘│
│      │                                                                 │
│      v                                                                 │
│  PHASE 5.5 --- Drain Worker Extension Requests                          │
│  ┌────────────────────────────────────────────────────────────────────┐│
│  │                                                                    ││
│  │  for request in worker_requests:                                   ││
│  │    worker_ext.handle_request(request)                              ││
│  │    (StdExtension: downcasts to TimerRequest, schedules timers)     ││
│  │                                                                    ││
│  └────────────────────────────────────────────────────────────────────┘│
│      │                                                                 │
│      v                                                                 │
│  PHASE 6 --- Publish Stats                                             │
│  ┌────────────────────────────────────────────────────────────────────┐│
│  │                                                                    ││
│  │  if did_work:                                                      ││
│  │    stats.num_actors, total_mailbox_depth, messages_processed       ││
│  │    stats.messages_dropped (if any overflow drops)                  ││
│  │    stats_hook.on_tick(worker_id, snapshots) if configured          ││
│  │                                                                    ││
│  │  record TickTiming (6-element phase_us array + processed + flag)   ││
│  │                                                                    ││
│  └────────────────────────────────────────────────────────────────────┘│
│      │                                                                 │
│      v                                                                 │
│  PHASE 7 --- Cleanup Dead Actors                                       │
│  ┌────────────────────────────────────────────────────────────────────┐│
│  │                                                                    ││
│  │  pool.cleanup_dead() → Vec<(ActorAddress, StopReason)>            ││
│  │    stopping actors: call on_stop(&ctx) before removal             ││
│  │    poisoned actors: skip on_stop (state may be corrupt)           ││
│  │                                                                    ││
│  │  for each dead addr:                                                ││
│  │    address_map.remove(&addr)                                       ││
│  │                                                                    ││
│  │  if extension installed:                                           ││
│  │    notifications = ext.on_actor_death(&dead)                       ││
│  │      (StdExtension: emits Down/ActorExited, unregisters names,    ││
│  │       removes from groups, takes monitors)                        ││
│  │    ext.cleanup_dead(&dead_addrs)                                   ││
│  │    route notifications via route_to_pool_or_remote()               ││
│  │                                                                    ││
│  │  deliver any messages sent during on_stop callbacks                ││
│  │                                                                    ││
│  │  worker_ext.gc_dead(&dead_addrs)                                   ││
│  │    (StdExtension: removes orphaned interval timers)                ││
│  │                                                                    ││
│  └────────────────────────────────────────────────────────────────────┘│
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

## External Interactions

Everything the worker talks to, and everything that talks to it.

### Who writes into the worker's queues

```
┌─ User Code ────────────────────────────────────────────────────────────┐
│                                                                        │
│  let rt = Runtime::new(config);                                        │
│  let addr = rt.spawn(my_actor)?;     // --┐                            │
│  rt.send_to(addr, MyMsg(42))?;       // --┤                            │
│                                       // │                             │
└──────────────────────────────────┼──┼───────────────────────────────────┘
                                   │  │
        ┌───────────────────────────┘  │
        │                              │
        v                              v
┌─ Runtime ──────────────────────────────────────────────────────────────┐
│                                                                        │
│  spawn():                                                              │
│    addr = ActorAddress::new_random()                                   │
│    wid  = placement.next_worker()          -- round-robin pick         │
│    address_map.insert(addr, wid)           -- register globally        │
│    spawn_txs[wid].try_send((addr, boxed))  -- push to worker queue     │
│         │                                                              │
│         │       ┌────────────────────────────────────────────┐         │
│         └──────>│ Worker.spawn_rx  (Receiver side)           │         │
│                 └────────────────────────────────────────────┘         │
│                                                                        │
│  send_to():                                                            │
│    wid = address_map.lookup(&addr)                                     │
│    transfer_txs[wid].try_send(Envelope::new(addr, msg))                │
│         │                                                              │
│         │       ┌────────────────────────────────────────────┐         │
│         └──────>│ Worker.transfer_rx  (Receiver side)        │         │
│                 └────────────────────────────────────────────┘         │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

### Who the worker talks to during a tick

```
┌─ Worker (during phase 3: tick_all) ────────────────────────────────────┐
│                                                                        │
│  An actor calls ctx.send(addr, msg) or ctx.spawn(new_actor).           │
│  These go through WorkerContext, which implements ContextInner.        │
│                                                                        │
│  ctx.send(addr, msg)                                                   │
│       │                                                                │
│       v                                                                │
│  ┌─ WorkerContext.send_any ─────────────────────────────────────────┐  │
│  │                                                                  │  │
│  │  address_map.lookup(addr) --> which worker owns this actor?      │  │
│  │       │                                                          │  │
│  │  ┌────┴──────────────┬──────────────────┬──────────────────┐     │  │
│  │  │                   │                  │                  │     │  │
│  │  v                   v                  v                  │     │  │
│  │  SAME WORKER         OTHER WORKER       NOT FOUND          │     │  │
│  │  │                   │                  │                  │     │  │
│  │  │ pending_local     │ transfer_txs     │ inbox_registry   │     │  │
│  │  │   .push(addr,msg) │  [wid].send()    │  .try_deliver()  │     │  │
│  │  │                   │                  │                  │     │  │
│  │  │ stays in this     │ crosses to       │ goes to an       │     │  │
│  │  │ worker; delivered │ another worker   │ external         │     │  │
│  │  │ in phase 4        │ thread's         │ Inbox<M>         │     │  │
│  │  │                   │ transfer_rx      │ receiver          │    │  │
│  │  └───────────────────┴──────────────────┴──────────────────┘     │  │
│  │                                                                  │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                        │
│  ctx.spawn(new_actor)                                                  │
│       │                                                                │
│       v                                                                │
│  ┌─ WorkerContext.spawn_any ────────────────────────────────────────┐  │
│  │                                                                  │  │
│  │  wid = placement.next_worker()  -- round-robin target            │  │
│  │  address_map.insert(addr, wid)  -- register in global map        │  │
│  │  spawn_txs[wid].try_send(...)   -- enqueue for target worker     │  │
│  │                                                                  │  │
│  │  may land on THIS worker or a DIFFERENT worker                   │  │
│  │  target picks it up in phase 1 of its next tick                  │  │
│  │                                                                  │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

### The channel connecting everything

Each worker has two inbound channels. The channels are lock-free MPSC queues
backed by `crossbeam::ArrayQueue` with a `SegQueue` overflow.

```
┌─ HybridChannel<T> ─────────────────────────────────────────────────────┐
│                                                                        │
│  ┌─ ring: ArrayQueue<T> ──────────────────────────────────────┐        │
│  │  fixed capacity, lock-free, bounded                        │        │
│  │  ┌───┬───┬───┬───┬───┬───┬───┬───┐                         │        │
│  │  │   │   │   │   │   │   │   │   │  (pre-allocated)        │        │
│  │  └───┴───┴───┴───┴───┴───┴───┴───┘                         │        │
│  └────────────────────────────────────────────────────────────┘        │
│                                                                        │
│  ┌─ overflow: SegQueue<T> ────────────────────────────────────┐        │
│  │  unbounded, lock-free, linked nodes                        │        │
│  │  used only when ring is full                               │        │
│  └────────────────────────────────────────────────────────────┘        │
│                                                                        │
│  push(v):  try ring first, spill to overflow                           │
│  pop():    drain ring first, then overflow                             │
│                                                                        │
│  ┌─ Sender<T> ─────────┐       ┌─ Receiver<T> ─────────┐               │
│  │ Arc<HybridChannel<T>>│       │ Arc<HybridChannel<T>>  │             │
│  │ .try_send(v)         │──────>│ .try_recv() -> Option  │             │
│  │ clonable (new_sender)│ same  │ single consumer        │             │
│  └──────────────────────┘ Arc   └────────────────────────┘             │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘

Who holds what:

  transfer channel:
    Sender   held by: Runtime.transfer_txs[i], workers via TickContext
    Receiver held by: Worker[i].transfer_rx

  spawn channel:
    Sender   held by: Runtime.spawn_txs[i], workers via TickContext
    Receiver held by: Worker[i].spawn_rx
```

### The AddressMap: global actor directory

```
┌─ AddressMap ───────────────────────────────────────────────────────────┐
│                                                                        │
│  RwLock< HashMap<ActorAddress, WorkerId> >                             │
│                                                                        │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  addr_0 -> WorkerId(0)                                           │  │
│  │  addr_1 -> WorkerId(2)                                           │  │
│  │  addr_2 -> WorkerId(0)                                           │  │
│  │  addr_3 -> WorkerId(1)                                           │  │
│  │  ...                                                             │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                        │
│  READERS (concurrent, RwLock read):                                    │
│    WorkerContext.send_any() -- every message send does a lookup        │
│    Runtime.send_to()       -- external sends do a lookup               │
│                                                                        │
│  WRITERS (rare, exclusive lock):                                       │
│    Runtime.spawn()          -- registers new actor at spawn time       │
│    WorkerContext.spawn_any() -- actor spawns another actor             │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

### The InboxRegistry: escape hatch to user code

```
┌─ InboxRegistry ────────────────────────────────────────────────────────┐
│                                                                        │
│  RwLock< HashMap<ActorAddress, Arc<dyn SenderT>> >                     │
│                                                                        │
│  For addresses belonging to external Inbox<M>, not actors.             │
│                                                                        │
│  ┌─ Registration ─────────────────────────────────────────────────┐    │
│  │                                                                │    │
│  │  Runtime.new_inbox::<M>()                                      │    │
│  │    addr = ActorAddress::new_random()                           │    │
│  │    receiver = Receiver::<M>::new(capacity)                     │    │
│  │    sender   = receiver.new_sender()                            │    │
│  │    inbox_registry.register(addr, Arc::new(sender))             │    │
│  │    returns Inbox { addr, inner: receiver }                     │    │
│  │                                                                │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                        │
│  ┌─ Delivery (when address_map lookup fails) ─────────────────────┐    │
│  │                                                                │    │
│  │  WorkerContext.send_any(addr, msg)                             │    │
│  │    address_map.lookup(addr) -> None                            │    │
│  │    inbox_registry.try_deliver(addr, msg)                       │    │
│  │      senders.read().get(addr).try_send_any(msg)                │    │
│  │        downcast Box<Any> -> M, push into Receiver<M>           │    │
│  │                                                                │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                        │
│  ┌─ Consumption (user code) ──────────────────────────────────────┐    │
│  │                                                                │    │
│  │  let inbox = rt.new_inbox::<MyMsg>()?;                         │    │
│  │  // later, from any thread:                                    │    │
│  │  if let Some(msg) = inbox.try_recv() { ... }                   │    │
│  │                                                                │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

### Full system topology

```
┌─ User Code ────────────────────────────────────────────────────────────┐
│ rt.spawn()       rt.send_to()       inbox.try_recv()   rt.shutdown()  │
│ rt.spawn_named() rt.ask()           rt.where_is()      rt.stop_actor()│
│ rt.join_group()  rt.publish_to()    rt.group_members()                │
└────┬──────────────────┬──────────────────┬──────────────────┬──────────┘
     │                  │                  ^                  │
     v                  v                  │                  v
┌─ Arc<Runtime> ────────────────────────────────────────────────────────────┐
│                                                                           │
│ ┌───────────┐ ┌────────────┐ ┌─────────────┐ ┌────────────┐              │
│ │AddressMap │ │ Placement  │ │InboxRegistry│ │ is_running │              │
│ │ addr->wid │ │ load-aware │ │ addr->Sender│ │ AtomicBool │              │
│ └─────┬─────┘ └──────┬─────┘ └──────┬──────┘ └──────┬─────┘              │
│       │               │              │               │                    │
│ ┌─ extension: Arc<dyn RuntimeExtension> ──────────────────────────┐      │
│ │  StdExtension holds: NameRegistry, MonitorRegistry,             │      │
│ │  GroupRegistry, WatchRegistry (accessed via downcast)            │      │
│ └─────────────────────────────────┬───────────────────────────────┘      │
│                                   │                                      │
│ ┌─────────────────────────────────┴──────────────────────────────────┐   │
│ │                  TickContext (borrows all above)                     │   │
│ └──────────────────────────┬──────────────────────────────────────────┘   │
│                            │                                              │
│ ┌─ transfer_txs[] ────┐   │   ┌─ spawn_txs[] ──────┐                     │
│ │ [0]: Sender<Envelope│   │   │ [0]: Sender<(A,Box)>│                     │
│ │ [1]: Sender<Envelope│   │   │ [1]: Sender<(A,Box)>│                     │
│ │ [2]: Sender<Envelope│   │   │ [2]: Sender<(A,Box)>│                     │
│ └──┬──────┬──────┬────┘   │   └──┬──────┬──────┬────┘                     │
│    │      │      │        │      │      │      │                          │
└────┼──────┼──────┼────────┼──────┼──────┼──────┼──────────────────────────┘
     │      │      │        │      │      │      │
     v      v      v        │      v      v      v
┌────────┐┌────────┐┌───────┴┐┌────────┐┌────────┐┌────────┐
│xfer    ││xfer    ││xfer    ││ spawn  ││ spawn  ││ spawn  │
│_rx[0]  ││_rx[1]  ││_rx[2]  ││ _rx[0] ││ _rx[1] ││ _rx[2] │
└───┬────┘└───┬────┘└───┬────┘└───┬────┘└───┬────┘└───┬────┘
    │         │         │         │         │         │
    v         v         v         v         v         v
┌─ Worker 0 ──────┐ ┌─ Worker 1 ──────┐ ┌─ Worker 2 ──────┐
│                  │ │                  │ │                  │
│ ┌─ pool ──────┐  │ │ ┌─ pool ──────┐  │ │ ┌─ pool ──────┐  │
│ │ ┌──────────┐│  │ │ │ ┌──────────┐│  │ │ │ ┌──────────┐│  │
│ │ │ slot:    ││  │ │ │ │ slot:    ││  │ │ │ │ slot:    ││  │
│ │ │  mailbox ││  │ │ │ │  mailbox ││  │ │ │ │  mailbox ││  │
│ │ │  actor   ││  │ │ │ │  actor   ││  │ │ │ │  actor   ││  │
│ │ └──────────┘│  │ │ │ └──────────┘│  │ │ │ └──────────┘│  │
│ │ ┌──────────┐│  │ │ │ ┌──────────┐│  │ │ │             │  │
│ │ │ slot:    ││  │ │ │ │ slot:    ││  │ │ └─────────────┘  │
│ │ │  mailbox ││  │ │ │ │  mailbox ││  │ │                  │
│ │ │  actor   ││  │ │ │ │  actor   ││  │ │ thread 2         │
│ │ └──────────┘│  │ │ │ └──────────┘│  │ └──────────────────┘
│ └─────────────┘  │ │ └─────────────┘  │
│                  │ │                  │
│ thread 0         │ │ thread 1         │
└──────────────────┘ └──────────────────┘

Workers also send to EACH OTHER during phase 3:
  WorkerContext.send_any()  -> transfer_txs[other_wid].try_send()
  WorkerContext.spawn_any() -> spawn_txs[target_wid].try_send()
```

## Message Lifecycle

```
                       PRODUCERS
       ┌──────────────────┬──────────────────────┐
       │                  │                       │
       v                  v                       v
┌─────────────┐  ┌──────────────┐  ┌──────────────────────┐
│ Runtime     │  │ ctx.send()   │  │ ctx.send()           │
│  .send_to() │  │ same worker  │  │ other worker         │
└──────┬──────┘  └──────┬───────┘  └──────────┬───────────┘
       │                │                      │
       v                v                      v
┌─────────────┐  ┌─────────────┐  ┌──────────────────────┐
│ transfer_tx │  │ pending_    │  │ transfer_tx          │
│ [wid].send()│  │ local.push()│  │ [wid].send()         │
└──────┬──────┘  └──────┬──────┘  └──────────┬───────────┘
       │                │                     │
       │          (end of phase 3)            │
       │                │                     │
       │          phase 4:                    │
       │                │                     │
       v                v                     v
┌────────────────────────────────────────────────────┐
│                                                    │
│           pool.deliver(&addr, msg)                 │
│                    │                               │
│                    v                               │
│      slot.mailbox.push_back(msg)                   │
│                                                    │
└───────────────────────┬────────────────────────────┘
                        │
                  next tick_once
                    phase 3
                        │
                        v
┌────────────────────────────────────────────────────┐
│                                                    │
│  msg = slot.mailbox.pop_front()                    │
│                 │                                  │
│                 v                                  │
│  slot.actor.handle_any(&ctx, msg)                  │
│                 │                                  │
│                 v                                  │
│  ┌──────────────────────────────────────────┐      │
│  │ downcast Box<dyn Any> to A::Incoming     │      │
│  │                                          │      │
│  │ ok:  A.handle(ctx, typed_msg)            │      │
│  │ err: silently dropped                    │      │
│  └──────────────────────────────────────────┘      │
│                                                    │
└────────────────────────────────────────────────────┘
```

## Shutdown Flow

```
┌─ User Code ──────┐
│                   │
│  rt.shutdown()    │
│       │           │
└───────┼───────────┘
        │
        v
┌─ Runtime ──────────────────────────────────────┐
│                                                 │
│  is_running.store(false, Release)               │
│                                                 │
└────────────────────────┬────────────────────────┘
                         │
       ┌─────────────────┼─────────────────┐
       │                 │                 │
       v                 v                 v
┌─ Worker 0 ────┐ ┌─ Worker 1 ────┐ ┌─ Worker 2 ────┐
│                │ │                │ │                │
│ is_running     │ │ is_running     │ │ is_running     │
│  .load(Acquire)│ │  .load(Acquire)│ │  .load(Acquire)│
│   -> false     │ │   -> false     │ │   -> false     │
│                │ │                │ │                │
│ run() returns  │ │ run() returns  │ │ run() returns  │
│ thread exits   │ │ thread exits   │ │ thread exits   │
└────────────────┘ └────────────────┘ └────────────────┘
       │                 │                 │
       └─────────────────┼─────────────────┘
                         │
                         v
               ┌─ RuntimeHandle ──┐
               │                   │
               │  .join()          │
               │  waits for all    │
               │  JoinHandles      │
               │                   │
               └───────────────────┘
```

