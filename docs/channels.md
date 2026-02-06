# Channels & Shared State

All communication between workers (and between the `Runtime` and workers)
goes through lock-free channels. There are no mutexes in the hot path.

## HybridChannel

The core primitive. A lock-free MPSC queue with bounded fast path and
unbounded overflow.

```
┌─ HybridChannel<T> ───────────────────────────────────────────────────────┐
│                                                                           │
│  ┌─ ring: ArrayQueue<T> (crossbeam) ─────────────────────────────────┐   │
│  │  Pre-allocated, fixed capacity, lock-free CAS                      │   │
│  │  ┌───┬───┬───┬───┬───┬───┬───┬───┐                                │   │
│  │  │   │   │   │   │   │   │   │   │                                │   │
│  │  └───┴───┴───┴───┴───┴───┴───┴───┘                                │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                                                           │
│  ┌─ overflow: SegQueue<T> (crossbeam) ───────────────────────────────┐   │
│  │  Unbounded linked-list queue, lock-free                            │   │
│  │  Only used when ring is full                                       │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                                                           │
│  push(v):                                                                 │
│    ring.push(v) → Ok:  done                                              │
│    ring.push(v) → Err: overflow.push(v)                                  │
│                                                                           │
│  pop():                                                                   │
│    ring.pop()     → Some: return it                                      │
│    overflow.pop() → Some: return it                                      │
│    otherwise      → None                                                 │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

The fast path (ring) avoids allocation. The overflow (SegQueue) acts as a
safety net — the system never drops messages due to capacity, but
performance degrades under sustained overflow.

## Sender and Receiver

```
┌─ Sender<T> ──────────┐          ┌─ Receiver<T> ────────────┐
│                       │          │                           │
│  queue: Arc<Hybrid>   │──same──▶│  queue: Arc<Hybrid>       │
│                       │   Arc    │                           │
│  try_send(v) → push   │          │  try_recv() → pop         │
│                       │          │                           │
│  Clone: new_sender()  │          │  Single consumer          │
│  (clones the Arc)     │          │  (not Clone)              │
│                       │          │                           │
└───────────────────────┘          └───────────────────────────┘
```

`Sender` is `Clone` — multiple producers can send into the same channel.
`Receiver` is not `Clone` — exactly one consumer drains it.

## What Channels Exist

Each worker gets two inbound channels, created at `Runtime::new()` time:

```
  Per Worker:

    transfer channel:  carries Envelope (messages to actors)
      Senders:   Runtime, other workers (via TickContext)
      Receiver:  this Worker

    spawn channel:     carries (ActorAddress, Box<dyn AnyActor>)
      Senders:   Runtime, other workers (via TickContext)
      Receiver:  this Worker
```

For N workers, the runtime holds N transfer senders and N spawn senders.
Every worker can reach every other worker's queues through `TickContext`.

## AddressMap

Global directory mapping actor addresses to the worker that owns them.

```
┌─ AddressMap ──────────────────────────────────────────────────────────────┐
│                                                                           │
│  RwLock< HashMap<ActorAddress, WorkerId> >                                │
│                                                                           │
│  Read path (very frequent):                                               │
│    Every ctx.send() and Runtime.send_to() does a lookup.                  │
│    RwLock allows concurrent readers — no contention.                      │
│                                                                           │
│  Write path (rare):                                                       │
│    Only on spawn. Takes exclusive lock briefly.                           │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

## Placement

Decides which worker gets a newly spawned actor.

```
┌─ Placement ───────────────────────────────────────────────────────────────┐
│                                                                           │
│  next: AtomicUsize                                                        │
│  num_workers: usize                                                       │
│                                                                           │
│  next_worker() → WorkerId( next.fetch_add(1) % num_workers )              │
│                                                                           │
│  Simple round-robin. No load balancing, no affinity.                      │
│  Actors stay on their assigned worker for life.                           │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

## Where Things Live in the Code

| Concept | File |
|---------|------|
| `HybridChannel`, `Sender`, `Receiver` | `src/channel.rs` |
| `AddressMap`, `WorkerId`, `Placement` | `src/address_map.rs` |
| `Envelope` | `src/runtime.rs` |
| `InboxRegistry` | `src/runtime.rs` |
| `RuntimeConfig`, `BackoffPolicy` | `src/config.rs` |
