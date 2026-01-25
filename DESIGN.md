# Design goals
Get as much usability and speed as possible while keeping line count low. Aim for no footguns, ability to plug in
logic easily, and run near anywhere. We may make this a `![no_std]` library, but the MVP will use the
memory allocator and threading provided by the rust standard library.

We are not building a new erlang/BEAM. Minimal feature set means spawning actor processes, not having supervisiors, lots of process
monitoring tools, prempting, etc.

## Actor model

An actor has:

    - An inbox:
        this is a mpsc channel that the runtime/router dumps messages into and the actor consumes when the runtime loads it
        Implemented as a barebones atomic ring buffer. The router is responsible for inserting messages.

    - an outbox channel connection:
        this is a mpmc channel that is implemented by the runtime and router. Actors on this specific channel put responses and outgoing messages into this channel, to be routed to the given address.

    - a growable and mutable state:
        An actor owns some, from the runtime perspective, type erased bytes. The actor when processing messages can access its own state, but no other task can. This includes viewing.

    - a set of functions for processing messages:
        When the runtime loads the actor, it locks the inbox and attempts to process the messages therein.

## Runtime

In order for an actor to consume and send messages, it is processed by a runtime. The runtime, in order to negotiate messages between
actors, possesses a router.

A runtime has:
    - An actor processing thread(s):
        the processor will mark an actor as busy, load its state and inbox, and begin consuming messages from the inbox. The number of messages consumed is determined by the runtime. A good start is a backpressure strategy: after loading, process messages until mailbox is empty or size drops below a threshold (e.g., "drain to 50%").

    - A message router:
        the router is responsible for ensuring messages posted by actors get delivered to the appropriate inbox.

    - An atomic ring buffer containing thread-safe references to actors that are not currently loaded. Actors are popped off the buffer, messages are
        processed, and the reference is returned to the buffer/queue before the next actor is loaded.

## Router

The router is the engine for message delivery. It posesses:

    - An actor address book:
        The address book maps actor ids to `Sender` references that can be used to deliver messages to the actor inbox.

    - Its own inbox:
        The router possesses its own mpsc queue where references to messages are stored. The router will process this queue by dereferencing and writing directly into the recipient's inbox buffer.

### Misc

A means of providing an emergency overflow without adding much more code complexity. The mutex means
this will not be `no_std` however. 

```rust
struct HybridChannel<T> {
    // Start with lock-free ring buffer
    ring: AtomicRingBuffer<T>,
    
    // When full, spill into a Mutex<VecDeque<T>>
    overflow: parking_lot::Mutex<VecDeque<T>>,
    
    // Track overflow frequency to resize ring proactively
    overflow_count: AtomicUsize,
}

impl<T> HybridChannel<T> {
    fn push(&self, value: T) {
        if self.ring.push(value).is_err() {
            self.overflow.lock().push_back(value);
            self.overflow_count.fetch_add(1, Relaxed);
            // Optionally: if overflow_count > threshold, grow ring
        }
    }
}
```

### Kimi Suggestions

IMPROVEMENTS FITTING DESIGN GOALS
Here are improvements aligned with the stated goals: "maximum usability and speed while keeping line count low" and "no footguns."
Priority 1: Critical Bug Fixes & MVP Completion
1. Fix Runtime Constructor (~5 lines)
   - Implement Runtime::new()
   - Implement Builder::build()
   - Fix examples to compile
2. Handle Full Inboxes Gracefully (~15 lines)
   - Return Result<(), Error> from send_to on full inbox
   - Provide backpressure signal instead of silent drop
   - Add try_send() vs send() distinction
3. Implement Multithreaded Runtime (~30-40 lines)
   - Complete threading infrastructure (already partially designed)
   - Router runs in separate thread
   - Actor processing pool with work-stealing (simple round-robin first)
Priority 2: Usability Enhancements (Low Line Count)
4. Ergonomic Macros (~20-30 lines procedural macro crate)
      #[derive(Actor)]
   struct MyActor { ... }
      - Auto-impl ActorInterface for simple cases
   - Reduces boilerplate significantly
5. Request/Response Helper (~15 lines)
      let resp: Response = rt.request(addr, msg).await?;
      - Common pattern many users need
   - Maintains simplicity
6. Inbox Capacity Configuration (~5 lines)
   - Per-actor capacity instead of global constant
   - Allow spawn_with_capacity()
Priority 3: Performance Optimizations
7. Sharded Router (~30-40 lines)
   - Multiple HashMaps based on address hash
   - Reduces contention on messaging hot path
   - Maintains O(1) lookups
8. Actor Work Stealing (~40-50 lines)
   - Multiple actor queues instead of single global queue
   - Threads steal work when idle
   - Improves cache locality
9. Hybrid Channel (from DESIGN.md) (~25 lines)
   - Implements the overflow mechanism described
   - Ring buffer + Mutex<VecDeque> for emergencies
   - Prevent message loss under burst loads
10. Actor State Colocation (~15 lines)
    - Group related actors by affinity
    - Optional "actor system" or "node" concept
    - Better cache locality
Priority 4: Observability (Minimal Overhead)
11. Lightweight Metrics (~15-20 lines)
    - Message counts per actor (atomic counters)
    - Overflow/drop tracking
    - Optional, compile-time feature flag
12. Message Tracing (~10-15 lines opt-in)
    - Optional trace ID in envelope
    - Zero-cost when disabled (feature flag)
Priority 5: Reliability Patterns
13. Bounded Channels with Overflow (~20 lines)
    - Implement HybridChannel from design doc
    - Graceful degradation under load
14. Watchdog Timer Pattern (~15 lines example)
    - Show pattern: actor checking heartbeats
    - Keep library simple, document patterns
---
SPECIFIC CODE IMPROVEMENTS
Fix Silent Failures (Priority: CRITICAL)
Current (src/runtime.rs:85-93):
pub fn send_to<M: Message>(&self, addr: ActorAddress, msg: M) -> Result<(), ()> {
    let envelope: Envelope = Box::new(msg);
    self.router_inbox
        .try_send(RouterMessage::SendToAddr { addr, msg: envelope })
        .map_err(|_| ())
}
Improved:
pub fn try_send<M: Message>(&self, addr: ActorAddress, msg: M) -> Result<(), Error> {
    let envelope: Envelope = Box::new(msg);
    self.router_inbox
        .try_send(RouterMessage::SendToAddr { addr, msg: envelope })
        .map_err(|_| Error::from("Router inbox full"))
}
// Add send that blocks/resizes
pub fn send<M: Message>(&self, addr: ActorAddress, msg: M) -> Result<(), Error> { ... }
Implement HybridChannel (Priority: HIGH)
From design doc, add to ring_buffer.rs:
pub struct HybridChannel<T> {
    ring: ArrayQueue<T>,
    overflow: Mutex<VecDeque<T>>,
    overflow_count: AtomicUsize,
}
impl<T> HybridChannel<T> {
    fn push(&self, value: T) -> Result<(), T> {
        if self.ring.push(value).is_err() {
            self.overflow.lock().push_back(value);
            self.overflow_count.fetch_add(1, Relaxed);
            // Optionally resize ring if overflow_count > threshold
        }
        Ok(())
    }
}
Fix Runtime Construction (Priority: CRITICAL)
Current: Runtime::new() doesn't exist but examples use it.
Add to runtime.rs:
impl Runtime {
    pub fn new(capacity: usize, flavor: Option<RuntimeFlavor>) -> Self {
        let router = Router::new(capacity);
        let router_inbox = router.new_sender();
        
        Self {
            flavor: flavor.unwrap_or_default(),
            router,
            router_inbox,
            actor_queue: ArrayQueue::new(capacity),
            thread_pool: Vec::new(),
        }
    }
}
Add Sharded Router (Priority: MEDIUM)
Current: Single HashMap for all addresses
Improved: N HashMaps based on address modulo
pub(crate) struct Router {
    shards: Vec<HashMap<ActorAddress, Box<dyn SenderT>>>,
    shard_mask: usize, // shards.len() - 1 (power of 2)
    inbox: Receiver<RouterMessage>,
}
impl Router {
    fn get_shard(&self, addr: ActorAddress) -> &HashMap<...> {
        &self.shards[(addr as usize) & self.shard_mask]
    }
}
---
RECOMMENDED ROADMAP
Phase 1: Bug Fixes & MVP (1-2 days)
1. Implement Runtime::new()
2. Implement Builder::build()
3. Fix compilation errors
4. Add error handling for full inboxes
5. Document API
Phase 2: Single-Threaded Polish (1 week)
1. Ergonomic macros
2. Request/response helpers
3. Inbox capacity configuration
4. Example improvements
5. Basic tests
Phase 3: Multi-Threaded (2 weeks)
1. Implement threaded runtime
2. Worker thread pool
3. Router in separate thread
4. Work-stealing queues
5. Performance benchmarks
Phase 4: Production Hardening (2 weeks)
1. Sharded router
2. Hybrid channels
3. Metrics (opt-in)
4. Message tracing (opt-in)
5. Stress testing
Phase 5: Documentation & Patterns (1 week)
1. Actor patterns guide
2. Performance tuning guide
3. WASM integration examples
4. Best practices documentation
---
ALTERNATIVE ARCHITECTURES TO CONSIDER
For Even Smaller Line Count
If the goal is absolutely minimal code, consider:
- Single-threaded only: Remove multi-threading complexity entirely
- No router: Direct mpsc channels between actors (more Erlang-like)
- Simpler scheduling: Round-robin over actors array
Tradeoff: Less flexible, but potentially <200 lines total.
For Better Performance
If performance outweighs minimalism:
- Lock-free HashMap: Use dashmap or equivalent for router
- SegQueue: Better for work-stealing than ArrayQueue
- Pre-allocated: Fixed-size actor pool with object pool pattern
- Lock-free message passing: Use crossbeam or tokio channels throughout
Tradeoff: More dependencies, larger binary size.
For Better Ergonomics
If usability is primary goal:
- Async/Await native: Integrate with tokio or async-std
- Actor supervision: Basic supervisor trees (small implementation)
- Message DSL: Macro for pattern-matching message handlers
Tradeoff: Increases complexity substantially, may conflict with "small" goal.
---
CONCLUSION
swactor has a solid, minimalist architecture that delivers on its core promise: a small, WASM-compatible actor library. The design is clean, modular, and avoids unnecessary complexity.
Current Grade: C+ (Incomplete MVP)
- Architecture: B+
- Ease of Use: D (examples don't compile, silent failures)
- Performance: B (good primitives but scalability concerns)
Potential Grade with improvements: A-
- Fixing critical bugs would make it immediately usable
- Sharded router + work-stealing would address scalability
- Ergonomic macros would dramatically improve UX
- Hybrid channels would solve burst-load scenarios
Recommendation: Focus on completing Phase 1 (bug fixes) and Phase 2 (usability). The architecture is sound—it's just incomplete. Avoid premature optimization; measure performance first, then implement sharding/work-stealing if benchmarks show contention.