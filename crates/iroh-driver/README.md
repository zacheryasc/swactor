# iroh-driver

`iroh-driver` is the iroh-backed transport bridge for the actorized distribution stack. It owns the concrete iroh endpoint, QUIC connections, relay configuration, peer authorization, and frame shuttling between iroh and swactor actor mailboxes.

## Tokio runtime ownership

The driver needs Tokio because iroh's endpoint, accepts, dials, stream reads/writes, retry timers, and shutdown APIs are async. New call sites should make that engine explicit by constructing the driver with:

```rust
let driver = IrohDriver::with_handle(tokio_handle, config)?;
```

`with_handle` does not own the Tokio runtime. The caller must keep the runtime alive for as long as the driver exists.

## Legacy implicit constructor

`IrohDriver::new(config)` is still present as a compatibility convenience, but it hides runtime ownership:

- If called inside an existing Tokio runtime, it uses `Handle::try_current()` and shares that ambient engine.
- If called outside Tokio, it silently builds and owns a multi-threaded Tokio runtime with `enable_all()`.

Avoid `IrohDriver::new` in new production code. Use `with_handle` or an explicit engine wrapper at the application boundary so every Tokio engine in the process is visible in construction code.

## Sync facade caveat

The synchronous facade methods that bridge to async with `block_on` must run from a non-async thread. Do not call those methods from inside tasks running on the same Tokio runtime; Tokio will panic on nested `block_on`.
