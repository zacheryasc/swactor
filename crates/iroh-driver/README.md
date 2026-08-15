# iroh-driver

`iroh-driver` is the iroh-backed transport bridge for the actorized distribution stack. It owns the concrete iroh endpoint, QUIC connections, relay configuration, peer authorization, and frame shuttling between iroh and swactor actor mailboxes.

## Engine ownership

The driver runs on a caller-supplied swactor [`EngineHandle`](swactor_engine) — the
single engine that owns the node's Tokio substrate.  All accepts, reads, dials,
writes, retries, and teardown are scheduled through that handle; the driver
stores no raw Tokio handle and performs no ambient-runtime detection
(ENGINE_SPEC.md §7).

```rust
let driver = IrohDriver::with_engine(engine.handle(), config)?;
```

The driver validates that the engine provides the `tasks`, `timers`, and `io`
capabilities before binding the endpoint or starting any background work
(ENGINE_SPEC.md).  Endpoint construction runs as an engine-hosted
task; `with_engine` blocks on a synchronous channel until the endpoint is bound
(or fails), so callers need not enter or possess the raw substrate runtime.

## Engine-hosted progression

All adapter progression — actor-bridge ingress/egress, telemetry ingress, and
edge ingress — is driven by an engine-hosted interval pump installed via
`install_actor_bridge_pump`. Applications do not (and cannot) manually pump
these adapters; the single engine owns progression for the node's lifetime
(ENGINE_SPEC.md). `snapshot` is a pure-synchronous read of driver
state, callable from any thread. The `shutdown` method closes the endpoint via
an engine-hosted task, blocking on a synchronous channel until completion.
