# Actor Model

Swactor's actor model is intentionally minimal. An actor is a struct that
implements one trait, receives one message type, and communicates only
through `Ctx`.

## Defining an Actor

```rust
use swactor::actor::ActorInterface;
use swactor::runtime::Ctx;

#[derive(Debug, Default, Clone)]
struct Ping { return_addr: ActorAddress }

#[derive(Debug, Default, Clone)]
struct Pong;

struct MyActor {
    count: usize,
}

impl ActorInterface for MyActor {
    type Incoming = Ping;
    type Response = Pong;     // not enforced at runtime — a documentation hint

    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        self.count += 1;
        let _ = ctx.send(msg.return_addr, Pong);
    }
}
```

That's it. No lifecycle hooks, no supervision trees, no async. Just a
`handle` method.

## The Traits

```
┌─ Message ─────────────────────────────────────────────────────────────────┐
│                                                                           │
│  trait Message: 'static + Sized + Clone + Send + Sync {}                  │
│                                                                           │
│  Blanket-implemented for any type that meets the bounds.                  │
│  You never implement this manually.                                       │
│                                                                           │
│  Why Clone + Send + Sync?                                                 │
│    Clone  — messages may be duplicated (Python bindings, stats, etc.)     │
│    Send   — messages cross thread boundaries                              │
│    Sync   — required by the type-erased Any + Send path                   │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘

┌─ ActorInterface ──────────────────────────────────────────────────────────┐
│                                                                           │
│  trait ActorInterface: 'static + Send {                                   │
│      type Incoming: Message;                                              │
│      type Response: Message;                                              │
│      fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming);                │
│  }                                                                        │
│                                                                           │
│  This is what you implement. The actor owns mutable state (&mut self)     │
│  and receives typed messages.                                             │
│                                                                           │
│  Actors are Send but NOT Sync — only one worker thread ever touches       │
│  a given actor.                                                           │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

## Type Erasure

Actors in the runtime are stored as `Box<dyn AnyActor>`, which erases the
concrete type. Messages are stored as `Box<dyn Any + Send>`. Type checking
happens at delivery time via `downcast`:

```
                        compile time                    runtime
                        ───────────                    ───────
  ctx.send(addr, msg)
       │
       v
  Box::new(msg) as Box<dyn Any + Send>     -- type erased here
       │
       v
  enqueued in mailbox (VecDeque<Box<dyn Any + Send>>)
       │
       v
  actor.handle_any(ctx, msg)
       │
       v
  msg.downcast::<A::Incoming>()            -- type recovered here
       │
  ┌────┴────┐
  │         │
  ok        err
  │         │
  v         v
  A.handle  silently dropped
  (ctx,msg)
```

Why silent drop? In a dynamic system (especially with Python bindings),
type mismatches aren't crashes — they're routing errors. The actor simply
ignores messages it doesn't understand.

## ActorAddress

```
┌─ ActorAddress ────────────────────────────────────────────────────────────┐
│                                                                           │
│  pub struct ActorAddress(pub [u8; 32]);                                   │
│                                                                           │
│  32 random bytes — globally unique, no coordination needed.               │
│  Generated via get_random() (system RNG or deterministic counter          │
│  for WASM builds).                                                        │
│                                                                           │
│  Derives: Debug, Default, Clone, Copy, PartialEq, Eq, Hash               │
│                                                                           │
│  Used as keys in:                                                         │
│    AddressMap  (actor → worker lookup)                                    │
│    ActorPool   (actor → mailbox + state)                                  │
│    InboxRegistry (external inbox lookup)                                  │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

## Where Things Live in the Code

| Concept | File | Key lines |
|---------|------|-----------|
| `Message` trait | `src/actor.rs` | blanket impl |
| `ActorInterface` trait | `src/actor.rs` | user-facing trait |
| `ActorAddress` | `src/actor.rs` | 32-byte random ID |
| `Actor<A>` wrapper | `src/actor.rs` | wraps user state |
| `AnyActor` trait | `src/actor.rs` | type-erased handler |
| `ActorPool` | `src/worker/mod.rs` | per-worker storage |
| `ActorSlot` | `src/worker/mod.rs` | mailbox + actor pair |
