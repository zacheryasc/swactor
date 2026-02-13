# Transport-Agnostic Messaging

Enables actors on different runtimes to communicate transparently via
pluggable codecs (serialization) and transports (delivery protocol).

**Feature-gated:** `#[cfg(feature = "transport")]`. Without the flag, the
binary is identical to the baseline runtime.

```bash
cargo build --features transport
cargo test  --features transport
```

## Two Layers of Pluggability

- **`Codec<M>`** — HOW bytes are encoded. gRPC/protobuf, bincode, custom, etc.
  No serde bounds — the codec defines what it needs from `M`.
- **`Transport`** — WHERE bytes are sent. InMemory (testing), TCP, gRPC, etc.

See [transport_routing.svg](../diagrams/transport_routing.svg)
for the extended routing chain, and
[transport_encode_decode.svg](../diagrams/transport_encode_decode.svg)
for the encode/decode data flow.

## Core Types

| Type | Role |
|------|------|
| `Codec<M>` | User-implemented encode/decode for a message type |
| `NetworkMessage` | Marker trait: adds `fn type_tag() -> &'static str` for wire routing |
| `WireEnvelope` | `{ dest: ActorAddress, type_tag: String, payload: Vec<u8> }` |
| `Transport` | `fn send(WireEnvelope) -> Result<()>` — pluggable delivery |
| `CodecRegistry` | Maps `TypeId → encoder` (send side) and `type_tag → decoder` (receive side) |
| `TransportRouter` | Maps `ActorAddress → Arc<dyn Transport>` for remote addresses |
| `TransportBridge` | Deserializes incoming `WireEnvelope` → `(ActorAddress, Box<dyn Any + Send>)` |
| `InMemoryTransport` | mpsc-backed transport for testing |

## Routing Chain

Without transport, unresolved addresses fall through to `InboxRegistry`.
With transport enabled, a third step is inserted:

1. **AddressMap::lookup** → local worker delivery (zero-copy, no serialize)
2. **InboxRegistry::contains** → external inbox delivery
3. **TransportRouter::lookup** → codec.encode + transport.send (remote)
4. **Fallback** → `InboxRegistry::try_deliver` (Err if not found)

This chain runs in `Runtime::send_to`, `ContextInner for Runtime`, and
`WorkerContext::send_any` — all three follow the same logic.

## Type Erasure Bridge

Messages are `Box<dyn Any + Send>` before routing, but `dyn Any` can't be
serialized. The bridge:

**Send:** `Box<dyn Any>` → `(*msg).type_id()` → `encoders[TypeId]` →
downcast to `M` → `Codec<M>::encode` → `(type_tag, Vec<u8>)` → `WireEnvelope`

**Receive:** `WireEnvelope` → `decoders[type_tag]` → `Codec<M>::decode` →
`Box::new(msg) as Box<dyn Any + Send>` → `runtime.deliver_raw(addr, msg)`

`TypeId` (compiler-assigned, process-local) is used for encoding.
`type_tag` (user-defined, stable) is used on the wire for decoding.

## Setup

```rust
// 1. Register codecs
let mut codecs = CodecRegistry::new();
codecs.register::<Ping, _>(MyCodec);
codecs.register::<Pong, _>(MyCodec);

// 2. Create transport + router
let (transport, rx) = InMemoryTransport::pair();
let router = TransportRouter::new();
router.add_route(remote_addr, transport);

// 3. Attach to runtime
let mut rt = Runtime::new(RuntimeConfig::default());
rt.set_codec_registry(Arc::new(codecs));
rt.set_transport_router(Arc::new(router));

// 4. Send transparently
rt.send_to(remote_addr, Ping { value: 42, reply_to: inbox_addr }).unwrap();

// 5. Receive side: bridge deserializes, deliver_raw injects
let bridge = TransportBridge::new(codecs_arc);
let (addr, msg) = bridge.receive(wire_envelope).unwrap();
rt.deliver_raw(addr, msg).unwrap();
```

## Address Resolution

Addresses are **not automatically discovered**. Each runtime must be told
which remote addresses exist via `router.add_route()`. Since addresses are
32 random bytes, runtimes must exchange them out-of-band (e.g., over the TCP
connection itself — see `examples/tcp_ping_pong.rs`).

## Distribution Driver

The distribution layer's `NodeDriver` (`crates/distribution/src/driver.rs`)
is the primary consumer of the TCP transport. It uses `TcpTransport` for
outgoing SWIM messages and `TcpAcceptor` for incoming, bypassing the
`Codec<M>` trait in favor of direct `serde_json` serialization (see
[DOCKER_REALIZATION.md](../development_history/DOCKER_REALIZATION.md)
§10.2 for rationale).

## Limitations

- **No automatic discovery** — manual address exchange required
- **Transport::send is synchronous** — blocking transports stall the worker
- **One route per address** — no wildcard/prefix routing
- **No ordering guarantees** across transports (depends on transport impl)
- **No back-pressure** from remote — fire-and-forget delivery
- **TypeId is not stable** across compilations (only used process-locally; wire uses type_tag)

## Where Things Live

| Concept | File |
|---------|------|
| All transport types | `src/transport.rs` |
| `InboxRegistry::contains()` | `src/delivery.rs` |
| Transport fields on `TickContext` | `src/delivery.rs` |
| `Runtime::deliver_raw`, setters | `src/runtime.rs` |
| Transport fallback in worker | `src/worker.rs` |
| Integration tests | `tests/transport_api.rs` |
| TCP example | `examples/tcp_ping_pong.rs` |
