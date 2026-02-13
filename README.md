# swactor

Minimal actor runtime for Rust. Single-threaded or multi-threaded, with
Python and WebAssembly bindings.

## Quick Start

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

## Features

### Actor Model

Actors implement one trait (`ActorInterface`), receive one message type, and
hold mutable state. No lifecycle hooks, no supervision trees, no async.

Every actor gets a 32-byte globally unique `ActorAddress`. The same
`ctx.send(addr, msg)` call works whether the target is on the same worker,
a different worker thread, an external inbox, or a remote process.

Single-threaded mode (`rt.tick()`) gives deterministic frame-level control.
Multi-threaded mode (`rt.run()`) spawns OS threads with adaptive backoff.

See [docs/runtime/actor-model.md](docs/runtime/actor-model.md) and
[docs/runtime/runtime.md](docs/runtime/runtime.md) for the full model.

### Transport

Pluggable cross-process messaging. User-provided codecs handle serialization
(gRPC/protobuf, bincode, hand-rolled — no serde bounds imposed) and
user-provided transports handle delivery (TCP, in-memory, gRPC channel).

```bash
cargo build --features transport
cargo run --example tcp_ping_pong --features transport -- receiver  # terminal 1
cargo run --example tcp_ping_pong --features transport -- sender    # terminal 2
```

See [docs/distribution/transport.md](docs/distribution/transport.md) for the routing chain, codec
registry, and address resolution.

### Runtime Dashboard

Live web dashboard for monitoring actors, message throughput, and mailbox
depths. Supports trace recording and replay at configurable speed.

Includes hand-authored SVG diagrams (actor lifecycle, message lifecycle,
tick cycle, transport routing) and generated diagrams from DOT sources
(architecture, dataflow, type erasure).

See [crates/runtime-dashboard/](crates/runtime-dashboard/README.md).

### Language Bindings

**Python** — PyO3 via Maturin. Spawn actors from Python callables, pass
dicts as messages, single-threaded or multi-threaded.

```bash
cd crates/swactor-python && maturin develop
```

Examples in `examples/python/` (single-thread, async, Jupyter notebook).

**WASM** — wasm-bindgen. Runs single-threaded with deterministic addressing
(`no_random` feature).

```bash
cd crates/swactor-wasm && wasm-pack build --target nodejs
```

### Connectome Analysis

Structural analysis of the internal dependency graph.

- **depgraph** (`tools/depgraph/`) — AST-based extraction of module
  dependencies, outputs GraphViz DOT
- **spectral** (`tools/spectral/`) — Laplacian eigenvalue analysis,
  Connectome Complexity Index (CCI), coupling heatmaps, interactive HTML
  dashboard

```bash
cargo run --manifest-path tools/depgraph/Cargo.toml -- --src-dir src/ --output deps
python tools/spectral/spectral_analysis.py deps.dot
```

See [docs/connectome/connectome.md](docs/connectome/connectome.md) for metric interpretation.

## Building & Testing

```bash
cargo test                              # all tests
cargo test --features transport         # include transport tests
cargo run --example hello               # single actor example
cargo run --example ring                # 500-actor ring topology
cargo bench                             # benchmarks (criterion)
```

## Feature Flags

| Flag | Default | What it does |
|------|---------|--------------|
| `getrandom` | yes | System RNG for actor addresses |
| `no_random` | no | Deterministic counter (WASM / reproducible tests) |
| `transport` | no | Pluggable remote messaging (codec + transport) |
| `tracing` | no | `tracing` instrumentation for runtime internals |
| `serde` | no | Serde derives for stats types |
| `python` | no | PyO3 bindings (cdylib wheel) |

## Documentation

| Document | Covers |
|----------|--------|
| [Actor Model](docs/runtime/actor-model.md) | Traits, type erasure, addresses |
| [Runtime](docs/runtime/runtime.md) | Runtime, Ctx, Inbox, RuntimeHandle, stats |
| [Worker Thread](docs/runtime/worker-thread.md) | Tick phases, backoff, routing, full system topology |
| [Channels](docs/runtime/channels.md) | HybridChannel, AddressMap, Placement |
| [Transport](docs/distribution/transport.md) | Codec, Transport, remote messaging, address resolution |
| [Distribution](docs/distribution/distribution.md) | SWIM membership, Kademlia, NodeDriver |
| [Connectome](docs/connectome/connectome.md) | CCI metrics, spectral analysis interpretation |
| [Dashboard](crates/runtime-dashboard/README.md) | Live web UI, trace recording, diagram index |
