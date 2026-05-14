# Plan: Local Mock Integration Test

## Context

The smoke-test crate has 5 component test groups (T-codec, T-worker, T-vastai, T-actor, T-cluster) that each test a piece of the distributed inference pipeline in isolation. What's missing is a single test that wires them together: two swactor nodes on localhost, one running the real `InferenceActor` (with `echo_worker.py`), communicating over iroh/QUIC. This proves the full local chain before spending money on vast.ai.

## Design Problem

`InferenceActor::Incoming` is `InferenceActorMsg` (a union of `Request` and `Process` variants). But the network codec delivers raw `InferenceRequest`. When `rt.deliver_raw()` delivers a deserialized `InferenceRequest` to the `InferenceActor`, the downcast to `InferenceActorMsg` fails silently.

**Solution:** Add a `RequestBridge` actor — same pattern as the existing `ProcessBridge`. It receives `InferenceRequest` from the network, wraps it as `InferenceActorMsg::Request(req)`, and forwards to the `InferenceActor`. ~10 lines.

## Changes

### 1. Add `RequestBridge` to `examples/single-gpu-inference/src/inference_actor.rs`

A public actor struct placed after the existing `ProcessBridge` (~line 55). Fields: `target: ActorAddress`. Implements `ActorInterface` with `Incoming = InferenceRequest`, wraps and forwards to target as `InferenceActorMsg::Request`.

### 2. Export it from `examples/single-gpu-inference/src/lib.rs`

Already exports `pub mod inference_actor` — `RequestBridge` just needs to be `pub`.

### 3. Create `examples/single-gpu-inference/tests/t_integration.rs`

One test function: `distributed_inference_through_echo_worker`.

**Setup (reuse patterns from t_cluster.rs and t_actor.rs):**
- Copy iroh helpers: `make_driver`, `make_converged_pair`, `IrohActorTransport`, `encode_wire`/`decode_wire`, `drain_actor_messages` (with reduced internal sleep for localhost — 100ms instead of 500ms)
- Copy process helpers: `echo_worker_spec`, `is_process_alive`

**Test flow:**
1. Converge two iroh drivers via `make_converged_pair()`
2. Create `rt_a` (local) and `rt_b` (remote) runtimes
3. On `rt_b`: spawn `InferenceActor` (with `echo_worker_spec()`) + `RequestBridge` pointing at it
4. On `rt_a`: create `response_inbox` for `InferenceResponse`
5. Build `IrohActorTransport` in each direction, wire transport routers:
   - `rt_a`: `bridge_addr → transport_a_to_b`
   - `rt_b`: `inbox_addr → transport_b_to_a`
6. Install codec registries and transport routers (`&mut self` — must happen after all spawns/inbox creation)
7. Tick `rt_b` in a polling loop until `InferenceActorStatus::WorkerReady` (echo_worker.py started)
8. `rt_a.send_to(bridge_addr, InferenceRequest { prompt: "Hello from node A", reply_to: inbox_addr, ... })`
9. Pump loop (10s timeout): `drain_actor_messages` on both drivers → tick both runtimes → check `response_inbox`
10. Assert response text contains `"Hello from node A"` (echo worker reflects prompt)
11. Cleanup: stop inference actor, tick until echo_worker.py pid is dead, shutdown both drivers

**Message path through the system:**
```
rt_a.send_to(bridge_addr, InferenceRequest)
  → transport router → IrohActorTransport (QUIC to node B)
  → drain_actor_messages → rt_b.deliver_raw(bridge_addr, InferenceRequest)
  → RequestBridge.handle() → ctx.send(inference_addr, InferenceActorMsg::Request(req))
  → InferenceActor.handle() → writes JSON to echo_worker.py stdin
  → echo_worker.py → writes JSON to stdout
  → ProcessActor → ProcessNotification::Output → ProcessBridge → InferenceActor
  → InferenceActor.process_output_line() → ctx.send(reply_to, InferenceResponse)
  → transport router → IrohActorTransport (QUIC to node A)
  → drain_actor_messages → rt_a.deliver_raw(inbox_addr, InferenceResponse)
  → response_inbox.try_recv() ✓
```

## Files Modified

| File | Change |
|---|---|
| `examples/single-gpu-inference/src/inference_actor.rs` | Add `pub struct RequestBridge` (~10 lines) |
| `examples/single-gpu-inference/tests/t_integration.rs` | New file — one integration test (~200 lines) |

## Verification

```bash
# Run just the new test
cargo test --manifest-path examples/single-gpu-inference/Cargo.toml t_integration

# Confirm existing tests still pass
cargo test --manifest-path examples/single-gpu-inference/Cargo.toml
```

Expected: all 22 tests pass (21 existing + 1 new). Test runtime ~10-15s (dominated by iroh drain sleeps and echo_worker.py process I/O).
