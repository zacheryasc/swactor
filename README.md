# swactor

Small, WASM-compatible actor runtime for Rust, with Python and WebAssembly
bindings.

## Quick Start (Rust)

```rust
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, Runtime, RuntimeConfig};

#[derive(Clone)]
struct Greet { name: String, reply_to: ActorAddress }

#[derive(Clone)]
struct Greeting(String);

struct Greeter;

impl ActorInterface for Greeter {
    type Incoming = Greet;
    type Response = Greeting;

    fn handle(&mut self, ctx: &Ctx, msg: Greet) {
        let _ = ctx.send(msg.reply_to, Greeting(format!("Hello, {}!", msg.name)));
    }
}

fn main() {
    let rt = Runtime::new(RuntimeConfig::default());
    let addr = rt.spawn(Greeter).unwrap();
    let inbox = rt.new_inbox::<Greeting>().unwrap();

    rt.send_to(addr, Greet { name: "world".into(), reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    rt.tick();

    println!("{}", inbox.try_recv().unwrap().0); // "Hello, world!"
}
```

## Quick Start (Python)

```bash
uv pip install .    # builds the Rust extension automatically
```

Single-threaded — caller drives each tick:

```python
from swactor import Runtime

def echo(ctx, msg):
    ctx.send(msg["reply_to"], f"hello, {msg['name']}!")

rt = Runtime()
addr = rt.spawn(echo)
inbox = rt.inbox()
rt.send(addr, {"name": "world", "reply_to": inbox.addr})
rt.tick()
print(inbox.try_recv())  # "hello, world!"
```

Multi-threaded — workers run on background threads:

```python
import asyncio
from swactor import Runtime, RuntimeConfig

async def main():
    rt = Runtime(RuntimeConfig(num_threads=2))

    def echo(ctx, msg):
        ctx.send(msg["reply_to"], f"hello, {msg['name']}!")

    addr = rt.spawn(echo)
    inbox = rt.inbox()
    handle = rt.run()  # spawns worker threads, consumes rt

    for name in ["alice", "bob", "charlie"]:
        handle.send(addr, {"name": name, "reply_to": inbox.addr})
        while (reply := inbox.try_recv()) is None:
            await asyncio.sleep(0.01)
        print(reply)

    handle.shutdown()
    handle.join()

asyncio.run(main())
```

## Quick Start (WASM)

The `wasm/` crate wraps swactor for use from JavaScript via `wasm-bindgen`.
It runs single-threaded with the caller driving `tick()` — a natural fit
for game loops, simulations, or any frame-based update cycle.

```bash
cd wasm && wasm-pack build --target nodejs    # or --target web
```

```javascript
import { SwactorRuntime } from "./wasm/pkg/swactor_wasm.js";

const rt = new SwactorRuntime();

// spawn a counter actor — accumulates values sent to it
const counter = rt.spawn_counter();

// spawn a relay that forwards messages to the counter
const relay = rt.spawn_relay(counter);

// send through the relay
rt.send(relay, 5);
rt.send(relay, 7);

rt.tick();  // relay receives and forwards
rt.tick();  // counter receives forwarded messages

// drain results from the inbox
let v;
while ((v = rt.try_recv()) !== undefined) {
    console.log(v);  // 5, then 12
}

rt.free();
```

The WASM crate uses the `no_random` feature (deterministic address
generation) so there's no dependency on system RNG.

## Running the Examples

```bash
cargo run --example hello    # single actor, request/response
cargo run --example ring     # 500 actors in a ring topology
```

## Multi-threaded Mode

Pass `num_threads` in the config. The runtime spawns OS threads and runs
workers autonomously — no `tick()` calls needed.

```rust
let mut config = RuntimeConfig::default();
config.num_threads = 4;
let rt = Runtime::new(config);

let addr = rt.spawn(MyActor::default()).unwrap();
let handle = rt.run().unwrap();  // consumes rt, spawns 4 threads

// use handle.runtime to spawn/send while workers run
handle.runtime.send_to(addr, MyMsg).unwrap();

handle.shutdown();
handle.join();
```

## Architecture

The runtime is layered: **Runtime** → **Workers** → **ActorPool** → **Actors**.

```
┌─ Runtime (Arc, shared) ──────────────────────────────────────┐
│                                                               │
│  AddressMap    Placement    InboxRegistry    is_running        │
│  (addr→worker) (round-robin) (external inboxes) (AtomicBool)  │
│                                                               │
│  transfer_txs[]              spawn_txs[]                      │
│  (one Sender per worker)     (one Sender per worker)          │
│                                                               │
└───────┬───────────────┬───────────────┬───────────────────────┘
        │               │               │
        v               v               v
   ┌─ Worker 0 ──┐ ┌─ Worker 1 ──┐ ┌─ Worker 2 ──┐
   │  ActorPool   │ │  ActorPool   │ │  ActorPool   │
   │  ┌────────┐  │ │  ┌────────┐  │ │  ┌────────┐  │
   │  │mailbox │  │ │  │mailbox │  │ │  │mailbox │  │
   │  │ actor  │  │ │  │ actor  │  │ │  │ actor  │  │
   │  └────────┘  │ │  └────────┘  │ │  └────────┘  │
   │  ┌────────┐  │ │  ┌────────┐  │ │              │
   │  │mailbox │  │ │  │mailbox │  │ └──────────────┘
   │  │ actor  │  │ │  │ actor  │  │
   │  └────────┘  │ │  └────────┘  │
   └──────────────┘ └──────────────┘
```

Each worker runs a **four-phase tick loop**:

1. **Drain spawn queue** — add newly spawned actors to the pool
2. **Drain transfer queue** — deliver cross-worker messages to mailboxes
3. **Tick all actors** — pop messages, call handlers, buffer outgoing sends
4. **Drain pending local** — deliver same-worker messages for the next tick

Messages are type-erased (`Box<dyn Any + Send>`) in transit and downcast
back to the concrete type at delivery. Mismatched types are silently dropped.

Detailed architecture docs live in `docs/`:

| Document | Covers |
|----------|--------|
| [Worker Thread](docs/worker-thread.md) | Tick phases, backoff, message routing, full system topology |
| [Runtime](docs/runtime.md) | Runtime, Ctx, Inbox, RuntimeHandle, stats |
| [Actor Model](docs/actor-model.md) | Traits, type erasure, addresses |
| [Channels & Shared State](docs/channels.md) | HybridChannel, AddressMap, Placement |

## Source Layout

```
src/
├── lib.rs           module root, feature gates, get_random()
├── actor.rs         Message, ActorInterface, ActorAddress, type erasure
├── runtime.rs       Runtime, Ctx, Inbox, RuntimeHandle, InboxRegistry
├── worker/
│   ├── mod.rs       Worker, WorkerContext, ActorPool, tick loop
│   └── tests.rs     worker unit tests with step-based DSL
├── channel.rs       HybridChannel (ArrayQueue + SegQueue), Sender/Receiver
├── config.rs        RuntimeConfig, BackoffPolicy
├── address_map.rs   AddressMap (RwLock<HashMap>), Placement (round-robin)
├── error.rs         Error type
└── python.rs        PyO3 bindings (feature = "python")

wasm/
├── Cargo.toml       separate crate, depends on swactor with no_random
├── src/lib.rs       wasm-bindgen wrapper (SwactorRuntime)
└── test.mjs         Node.js test suite

examples/
├── hello.rs                    echo actor
├── ring.rs                     ring topology
└── python/
    ├── hello_single_thread.py  minimal Python example
    ├── hello_async.py          multi-threaded + asyncio
    └── getting_started.ipynb   Jupyter notebook

tests/
├── runtime_api.rs   single + multi-thread integration tests
├── stats_demo.rs    stats snapshot tests
└── test_python.py   Python binding tests
```

## Building & Testing

```bash
# Rust
cargo test                              # run all tests
cargo run --example hello               # run an example
cargo bench                             # benchmarks (criterion)

# Python bindings (requires Rust toolchain on PATH)
uv pip install .                        # build + install
uv run python3 tests/test_python.py     # run Python tests

# WASM bindings
cd wasm && wasm-pack build --target nodejs
node test.mjs                           # run WASM tests
```

## Feature Flags

| Flag | Default | What it does |
|------|---------|--------------|
| `getrandom` | yes | System RNG for actor addresses |
| `no_random` | no | Deterministic counter (for WASM / reproducible tests) |
| `python` | no | PyO3 bindings, builds cdylib wheel |
