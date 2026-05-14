# Smoke Test Specification: Rent a GPU, Run Inference, Say Hello

## 1. Wanted Behavior

`smoke-run --vastai --api-key <key>` runs on the local machine. It:
1. Finds the cheapest available GPU on vast.ai and rents it.
2. Starts a local swactor node with iroh transport.
3. The rented instance boots our pre-built Docker image containing `gpu-node` + tinygrad.
4. The remote `gpu-node` reads `SEED_ADDR` from env and joins the local cluster via iroh/QUIC.
5. The local node discovers the remote `InferenceActor` via SWIM name resolution (`"inference"`).
6. The local node sends an `InferenceRequest` to the remote actor.
7. The remote actor runs tinygrad inference, replies with `InferenceResponse`.
8. Asserts the response is non-empty text and prints it.
9. Destroys the vast.ai instance.
10. Exits 0.

If anything fails, the script destroys the instance (if one was created) and exits 1.

Total wall-clock budget: 10 minutes.

```
local machine (smoke-run)               vast.ai GPU (gpu-node)
    |                                       |
    |  1. search offers (REST)              |
    |  2. create instance (REST)            |
    |     SEED_ADDR=<local node id>         |
    | ------------------------------------> |
    |                                       | [pulling docker image]
    |  3. poll vast.ai status every 10s     | [gpu-node starting]
    | ------------------------------------> | [tinygrad loading model into VRAM]
    |       "loading"                       |
    | <------------------------------------ |
    |       "running"                       |
    | <------------------------------------ |
    |                                       |
    |  4. SWIM cluster join (iroh/QUIC)     |
    | <--------------------------------->>> |
    |       [cluster converged]             |
    |       [name "inference" resolved]     |
    |                                       |
    |  5. InferenceRequest (swactor msg)    |
    |     "Say hello"                       |
    | ------------------------------------> |
    |                                       | [tinygrad forward pass]
    |       InferenceResponse               |
    |       "Hello! How can I help you?"    |
    | <------------------------------------ |
    |                                       |
    |  6. assert len(response) > 0          |
    |  7. DELETE instance (vast.ai REST)    |
    | ------------------------------------> |
    |  8. exit 0                            |
```

---

## 2. What Already Exists

The entire pipeline is implemented and tested locally. The remaining work is deploying it to vast.ai for real.

### Implemented components

| Component | File(s) | Status |
|---|---|---|
| Message types + codec | `src/messages.rs` | Done, tested (T-codec: 7 tests) |
| InferenceActor + ProcessBridge | `src/inference_actor.rs` | Done, tested (T-actor: 4 tests) |
| RequestBridge (network→actor type bridge) | `src/inference_actor.rs` | Done |
| Iroh actor transport (shared) | `src/iroh_transport.rs` | Done, used by tests + binaries |
| vast.ai REST client | `src/vastai.rs` | Done, tested (T-vastai: 7 tests) |
| tinygrad worker (real GGUF model) | `tinygrad_worker.py` | Done, tested (T-worker: 5 test classes) |
| Cluster transport tests | `tests/t_cluster.rs` | Done (3 tests) |
| In-process integration test (echo) | `tests/t_integration.rs` | Done, passes |
| In-process integration test (tinygrad) | `tests/t_integration.rs` | Done, passes (`#[ignore]`, needs .venv) |
| `gpu-node` binary | `src/bin/gpu_node.rs` | Done, compiles |
| `smoke-run` binary (localhost + vastai) | `src/bin/smoke_run.rs` | Done, compiles |
| Binary e2e test (echo) | `tests/t_binary.rs` | Done, passes |
| Binary e2e test (tinygrad) | `tests/t_binary.rs` | Done, passes (`#[ignore]`, needs .venv) |
| Dockerfile | `Dockerfile` | Done |

### Test commands

```bash
# Fast tests (no GPU, no downloads, no vast.ai) — 22 tests
cargo test --manifest-path examples/single-gpu-inference/Cargo.toml

# Slow tests (downloads ~1GB GGUF model, runs tinygrad on CPU)
cargo test --manifest-path examples/single-gpu-inference/Cargo.toml -- --ignored

# Binary e2e on localhost (spawns gpu-node + smoke-run as child processes)
cargo test --manifest-path examples/single-gpu-inference/Cargo.toml binary_e2e_echo_worker
```

---

## 3. What Remains — Deployment to vast.ai

### 3.1 Fix relay mode for WAN

Both binaries currently use `RelayMode::Disabled`, which works on localhost but not over WAN. The remote `gpu-node` behind a vast.ai NAT cannot reach the local node without iroh relay servers.

**Changes needed:**
- `gpu-node`: change `RelayMode::Disabled` to `RelayMode::Default` so iroh uses its public relay infrastructure for NAT traversal.
- `smoke-run` (vastai path): same — use `RelayMode::Default`.
- The localhost path (`smoke-run --seed`) can keep `RelayMode::Disabled`.

### 3.2 Fix worker script path in Dockerfile

The `Dockerfile` copies `tinygrad_worker.py` to `/usr/local/share/tinygrad_worker.py`, but `gpu-node` defaults `WORKER_SCRIPT` to `./tinygrad_worker.py`.

**Fix**: either change the Dockerfile `COPY` destination to `/app/tinygrad_worker.py` and set `WORKDIR /app`, or set `ENV WORKER_SCRIPT=/usr/local/share/tinygrad_worker.py` in the Dockerfile.

### 3.3 Add `pid` to tinygrad worker ready signal

`tinygrad_worker.py` emits `{"status": "ready"}` but `echo_worker.py` emits `{"status": "ready", "pid": <pid>}`. The `InferenceActor` parses the `pid` field for `WorkerReady { pid }` status reporting. Without it, `worker_pid` is `None` — not fatal, but cleanup assertions in tests rely on it.

**Fix**: change the ready signal to `_write({"status": "ready", "pid": os.getpid()})`.

### 3.4 Build and push the Docker image

```bash
# Cross-compile gpu-node for linux/amd64 (if not already on linux/amd64)
cargo build --release --bin gpu-node --manifest-path examples/single-gpu-inference/Cargo.toml

# Build image
docker build -t <your-registry>/swactor-gpu:latest -f examples/single-gpu-inference/Dockerfile .

# Push to registry (vast.ai pulls from here)
docker push <your-registry>/swactor-gpu:latest
```

The image name in `vastai.rs::create_instance` is hardcoded to `"swactor-gpu:latest"`. Update this to match whatever registry you push to, or make it a parameter.

### 3.5 Run it for real

```bash
VAST_API_KEY=<key> cargo run --manifest-path examples/single-gpu-inference/Cargo.toml --bin single-gpu-inference -- \
    --vastai --api-key <key> --gpu RTX_4090
```

---

## 4. Architecture

### Message flow (same for localhost and vast.ai)

```
smoke-run                                    gpu-node
─────────                                    ────────
rt.send_to(bridge_addr, InferenceRequest)
  → codec encodes → TransportRouter
  → IrohActorTransport (QUIC to gpu-node)
                                             drain_and_collect_reply_addrs()
                                               → decode_wire → codecs.receive()
                                               → rt.deliver_raw(bridge_addr, InferenceRequest)
                                               → extract reply_to, add return transport route
                                             RequestBridge.handle()
                                               → ctx.send(inference_addr, InferenceActorMsg::Request)
                                             InferenceActor.handle()
                                               → JSON to tinygrad_worker.py stdin
                                             tinygrad_worker.py
                                               → Transformer forward pass
                                               → JSON response to stdout
                                             InferenceActor.process_output_line()
                                               → ctx.send(reply_to, InferenceResponse)
                                               → TransportRouter → IrohActorTransport (QUIC back)
drain_actor_messages()
  → decode_wire → codecs.receive()
  → rt.deliver_raw(inbox_addr, InferenceResponse)
response_inbox.try_recv() ✓
```

### Name discovery

The `gpu-node` registers its `RequestBridge` under the name `"inference"` via `DistributedNode::register_name`. This propagates through SWIM gossip piggyback. The `smoke-run` orchestrator calls `driver.node().resolve_name("inference")` to discover the bridge's `ActorAddress` without needing to know it ahead of time.

### Dynamic return routing

The `gpu-node`'s drain loop inspects incoming `InferenceRequest` payloads to extract the `reply_to` address. It then dynamically registers a transport route for that address pointing back to the only alive SWIM member (the orchestrator). This is necessary because the `gpu-node` doesn't know the orchestrator's inbox address at startup.

---

## 5. tinygrad Worker

`tinygrad_worker.py` — managed by swactor's process crate via stdin/stdout JSON.

**Model**: `llama3.2:1b` from tinygrad's built-in GGUF catalog (~1GB download, fits in any modern GPU's VRAM). Loaded via `tinygrad.apps.llm.Transformer.from_gguf()`.

**Protocol**:
```
← stdout: {"status": "ready", "pid": 12345}
→ stdin:  {"prompt": "Say hello", "max_tokens": 64, "temperature": 0.7}
← stdout: {"response": "Hello! How can I help you today?"}
→ stdin:  {invalid json}
← stdout: {"error": "invalid JSON: ..."}
```

**Modes**:
- Default: downloads and loads real GGUF model, runs real inference.
- `--stub`: canned responses, no tinygrad import (for fast protocol tests).
- `--model <name>`: override model (e.g., `qwen3:0.6b` for smaller download).

**Environment**:
- `CUDA=1` → tinygrad uses CUDA backend (GPU).
- No env var → tinygrad auto-detects (CUDA if available, else CPU).
- `PYTHON=1` → forces tinygrad's pure-Python CPU backend (no clang needed).

---

## 6. Error Modes

| Failure | Detection | Response |
|---------|-----------|----------|
| No GPU offers available | vast.ai returns empty list | Print error, exit 1. No cleanup needed. |
| Instance creation rejected | API returns non-success | Print error, exit 1. No cleanup needed. |
| Instance never reaches `running` | 60 polls exhausted or terminal status | Destroy instance, exit 1. |
| Cluster never converges | SWIM timeout (2 min) | Destroy instance, exit 1. |
| Name `"inference"` never resolves | Timeout (1 min) | Destroy instance, exit 1. |
| tinygrad worker never reports ready | Process timeout (10 min) | Destroy instance, exit 1. |
| Empty/malformed response | Assertion on response text | Destroy instance, exit 1. |
| Destroy fails | Catch around cleanup | Print warning, exit 1. |

Every path that allocates an instance also destroys it.

---

## 7. Environment Requirements

- Rust toolchain
- Python 3.10+ with tinygrad 0.12.0 + numpy (in `.venv/`)
- Docker (for building the GPU node image)
- `VAST_API_KEY` (vast.ai deployment only — not needed for local tests)
- Network access to iroh relay servers (vast.ai deployment only)

---

## 8. What This Proves

1. swactor nodes on rented GPUs can join a cluster with a local node over WAN via iroh/QUIC.
2. swactor messages traverse the WAN transparently — no HTTP layer needed for node-to-node communication.
3. tinygrad can load and run a LLaMA model on rented hardware via its CUDA backend.
4. The process crate manages the Python child process lifecycle cleanly.
5. SWIM gossip propagates actor name registrations, enabling dynamic service discovery.
6. The full lifecycle (rent → cluster → discover → infer → teardown) is fully automated.
