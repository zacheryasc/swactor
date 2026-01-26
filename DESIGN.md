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
