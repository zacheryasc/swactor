# Pipeline-Parallel Inference Spec: Two Nodes, Say Hello

## 1. Wanted Behavior

`pp-smoke-run --vastai --api-key <key>` runs on the local machine. It:

1. Rents two GPUs on vast.ai (cheapest available matching the constraint).
2. Starts a local swactor node with iroh transport.
3. Both rented instances boot the `gpu-node` image. One is assigned `STAGE=0`, the other `STAGE=1`, via env var at instance-create time. Both also receive `NUM_STAGES=2` and `SEED_ADDR=<local node id>`.
4. Both nodes join the local cluster via iroh/QUIC.
5. Stage 0 loads layers `[0..k)` of `llama3.2:1b`; stage 1 loads layers `[k..N)` plus the final norm and output head. `k = num_blocks / 2`.
6. The orchestrator resolves the SWIM name `"pp-entry"` (registered by stage 0) and sends `InferenceRequest { prompt: "Say hello", max_tokens: 64 }` to it.
7. Stage 0 tokenizes the prompt, runs embedding + its layer range, sends a `StageActivation` message to stage 1.
8. Stage 1 runs its layer range + output norm + LM head, samples a token, and sends it back to stage 0 as the next decode input. Loop until EOS or `max_tokens`.
9. Stage 1 sends the final detokenized text to the orchestrator as `InferenceResponse`.
10. Orchestrator asserts response is non-empty, prints it, destroys both instances, exits 0.

If anything fails, both instances are destroyed (best-effort) and the command exits 1.

Total wall-clock budget: 5 minutes.

```
local (pp-smoke-run)        vast.ai stage-0           vast.ai stage-1
       |                          |                         |
       |  rent x2, set STAGE env  |                         |
       | -----------------------> |                         |
       | --------------------------------------------> .... |
       |                          | [load blocks 0..k)      |
       |                          |                         | [load blocks k..N, output head]
       |  SWIM join (iroh/QUIC)   |                         |
       | <----------------------> | <---------------------> |
       |                          |                         |
       |  InferenceRequest        |                         |
       |  "Say hello"             |                         |
       | -----------------------> |                         |
       |                          |  StageActivation        |
       |                          |  (hidden state, pos)    |
       |                          | ----------------------> |
       |                          |                         | [sample token]
       |                          |  next-token feedback    |
       |                          | <---------------------- |
       |                          | [decode loop ...]       |
       |                          |                         |
       |       InferenceResponse "Hello! ..."               |
       | <------------------------------------------------- |
       |                          |                         |
       |  DELETE both instances   |                         |
       |  exit 0                  |                         |
```

---

## 2. Scope and Non-Goals

**In scope.** Exactly two nodes. One static layer split, decided by the orchestrator at boot. One `InferenceRequest` per run. tinygrad worker with manually sliced `Transformer.blk`. Reuses the single-GPU example's vast.ai client, iroh transport, and swactor process bridge unchanged.

**Out of scope.** N > 2 nodes; dynamic stage assignment or rebalancing; microbatching; multiple in-flight requests; streaming output; fault tolerance of any kind; per-token latency or throughput goals; tensor parallelism; non-tinygrad workers; quantization choices beyond what the single-GPU example already uses.

---

## 3. Architecture

### Topology

A linear chain of two stages. Stage 0 is the chain entry point and registers itself under the SWIM name `"pp-entry"`. Stage 1 is the chain exit and registers under `"pp-exit"`. The orchestrator resolves `"pp-entry"` to submit the request and `"pp-exit"` is the address stage 1 uses for replies. Stage 0 resolves `"pp-stage-1"` to know where to send activations; stage 1 resolves `"pp-stage-0"` to send sampled tokens back for the next decode step.

Each stage knows its own `STAGE` index and `NUM_STAGES` from env. The "next stage" name (`pp-stage-{i+1}`) and "previous stage" name (`pp-stage-{i-1}`) are computed locally — no central topology config.

### Message types

```
InferenceRequest {
  reply_to:  ActorAddress,
  prompt:    String,
  max_tokens: u32,
}

StageActivation {
  request_id: u64,
  position:   u32,        // start_pos for KV cache
  hidden:     Vec<u8>,    // bf16 tensor bytes, shape [seq_len, hidden_dim]
  seq_len:    u32,
  is_prefill: bool,
}

NextToken {
  request_id: u64,
  token_id:   u32,
  position:   u32,        // position to use for the next forward
  done:       bool,       // EOS or max_tokens reached
}

InferenceResponse {
  text: String,
}
```

`StageActivation` flows stage 0 → stage 1. `NextToken` flows stage 1 → stage 0 (closes the autoregressive loop). `InferenceResponse` flows stage 1 → orchestrator once decode terminates.

### Per-request flow

1. Orchestrator → stage 0: `InferenceRequest { reply_to, prompt, max_tokens }`.
2. Stage 0 tokenizes, runs `token_embd` + blocks `[0..k)` on the full prompt (prefill), produces hidden state for the final position.
3. Stage 0 → stage 1: `StageActivation { request_id, position: 0, hidden, seq_len: prompt_len, is_prefill: true }`.
4. Stage 1 runs blocks `[k..N)` + `output_norm` + `output`, samples next token.
5. If EOS or `max_tokens` reached: stage 1 → orchestrator: `InferenceResponse { text }`. Done.
6. Otherwise: stage 1 → stage 0: `NextToken { request_id, token_id, position: prompt_len, done: false }`. Stage 1 also accumulates `token_id` into its local output buffer for detokenization at the end.
7. Stage 0 runs `token_embd` + blocks `[0..k)` on the single new token at `position`, sends `StageActivation { ..., seq_len: 1, is_prefill: false }`. Back to step 4.

KV cache is held on the actor that owns its layers — stage 0 caches K/V for blocks `[0..k)`, stage 1 for `[k..N)`. This falls out of tinygrad's per-block cache without any extra plumbing.

### Wire format

Hidden-state tensors are sent as raw little-endian bf16 bytes plus the shape. No protobuf, no msgpack — the existing swactor codec carries `Vec<u8>` plus typed scalars and that's enough.

For `llama3.2:1b` (`hidden=2048`, bf16): one decode-step activation is `2048 * 2 = 4096` bytes per hop. Prefill is `prompt_len * 4096` bytes. Negligible.

---

## 4. Worker

The worker is `pp_tinygrad_worker.py`, a fork of `tinygrad_worker.py` from the single-GPU example. Same stdin/stdout JSON protocol with the swactor process bridge, different operations.

### Loading

On startup, the worker reads `STAGE`, `NUM_STAGES`, and `MODEL` from env. It calls `Transformer.from_gguf(...)` to load the full model. This is wasteful — both nodes hold all weights in VRAM — but it's the simplest path and `llama3.2:1b` fits trivially. Pruning unused blocks is a later optimization.

The worker computes its layer range:

```
total = len(model.blk)
k = total // NUM_STAGES
my_range = (STAGE * k, (STAGE + 1) * k if STAGE < NUM_STAGES - 1 else total)
```

### Per-stage operations

**Stage 0** handles two operations:

- `embed_and_forward(token_ids, position)`: run `model.token_embd(tokens)`, then iterate `model.blk[0:k]` calling `block(x, start_pos=position)`. Return hidden state bytes.
- `decode_step(token_id, position)`: same as above with a single-token input.

**Stage 1** handles one operation:

- `forward_and_sample(hidden_bytes, position, seq_len)`: rebuild the tensor, iterate `model.blk[k:total]` calling `block(x, start_pos=position)`, apply `model.output_norm`, apply `model.output`, take logits at the last position, sample (argmax for MVP), return `token_id`.

Sampling is `argmax` only. No temperature, no top-p — matches what `Transformer.forward()` does at line 178 of `tinygrad/apps/llm.py`.

### What we bypass in tinygrad

- `Transformer.forward()` is not used — it runs the entire model. We call `model.blk[i:j]` directly.
- `Transformer.forward_jit` is not used — it wraps the full forward. Each stage's own forward is uncompiled. Slower, fine for MVP.
- `Transformer.generate()` is not used — the autoregressive loop is split across stages, not a single Python generator.

### What we get for free

- Per-block KV cache (`self.cache_kv` in `TransformerBlock._attention`) is allocated lazily on first call and persists across decode steps. Stage 0's cache holds K/V for blocks 0..k; stage 1's holds k..N. Correct by construction.
- The tokenizer (`SimpleTokenizer.from_gguf_kv`) is loaded on stage 0 only.
- Detokenization happens on stage 1, which accumulates sampled token ids and decodes once on EOS/max_tokens via `tok.decode([...])`. The tokenizer is also loaded on stage 1 for this — cheap.

### Worker protocol additions

In addition to the single-GPU example's `{"status": "ready", "pid": <pid>}`, the worker accepts and replies to stage-specific request types over the same stdin/stdout JSON channel:

```
Stage 0:
→ {"op": "embed_and_forward", "request_id": 1, "tokens": [...], "position": 0}
← {"request_id": 1, "hidden_b64": "...", "seq_len": <int>}

Stage 1:
→ {"op": "forward_and_sample", "request_id": 1, "hidden_b64": "...", "position": 0, "seq_len": 42}
← {"request_id": 1, "token_id": 1234}
```

Hidden-state tensors are base64-encoded on the stdin/stdout boundary (process bridge is text JSON). On the actor-to-actor wire they are raw bytes — no base64 — via the swactor codec.

---

## 5. What's Reused from single-gpu-inference

| Component | Reused as-is | Notes |
|---|---|---|
| `vastai.rs` REST client | yes | Just called twice instead of once. Image name parameterized. |
| `iroh_transport.rs` | yes | Same `RelayMode::Default` over WAN. |
| swactor process bridge | yes | Same stdin/stdout JSON pattern; different ops inside. |
| `Dockerfile` | mostly | Adds `pp_tinygrad_worker.py`. `STAGE`/`NUM_STAGES` env are runtime, not baked. |
| `gpu-node` binary | forked | New `pp-gpu-node` reads `STAGE`/`NUM_STAGES`, registers stage-specific SWIM names, instantiates the stage actor. |
| `smoke-run` binary | forked | New `pp-smoke-run` rents two instances, waits for both, sends one request, tears both down. |
| Message types | extended | New `StageActivation`, `NextToken` in addition to the existing `InferenceRequest`/`InferenceResponse`. |
| Tinygrad worker | forked | New `pp_tinygrad_worker.py` exposing per-stage ops instead of full inference. |

---

## 6. Error Modes

| Failure | Detection | Response |
|---|---|---|
| Fewer than 2 GPU offers available | vast.ai search returns < 2 | Print error, exit 1. No cleanup needed. |
| Either instance creation rejected | API non-success on either call | Destroy any instance already created, exit 1. |
| Either instance never reaches `running` | 60 polls exhausted | Destroy both, exit 1. |
| Cluster never converges to 3 members | SWIM timeout (2 min) | Destroy both, exit 1. |
| `pp-entry` or `pp-exit` never resolves | Timeout (1 min) | Destroy both, exit 1. |
| Either worker never reports ready | Process timeout | Destroy both, exit 1. |
| Stage 1 reports `done` but no text accumulated | Assertion on response | Destroy both, exit 1. |
| Either node drops out mid-decode | Iroh connection error / SWIM marks dead | Destroy both, exit 1. No retry. |
| Destroy fails for either instance | Catch around cleanup | Print warning, exit 1. |

Any path that allocates an instance also destroys it (best-effort).

---

## 7. Environment Requirements

- Rust toolchain.
- Python 3.10+ with tinygrad 0.12.0 + numpy (in `.venv/`).
- Docker.
- `VAST_API_KEY` (vast.ai deployment only).
- Network access to iroh relay servers.

Same as single-gpu-inference. No new dependencies.

---

## 8. What This Proves

1. swactor nodes on rented GPUs can form a chain that carries activations stage-to-stage over WAN.
2. Model weights can be split across machines and the resulting forward pass still produces coherent output.
3. KV cache locality (each stage holds K/V for its own layers) is preserved across decode steps without explicit cache management on the orchestrator.
4. The autoregressive feedback edge (stage 1 → stage 0) works as a swactor message and closes the loop without involving the orchestrator on every token.
5. The full lifecycle (rent x2 → cluster → distribute layers → infer → teardown) is automated.

This is the foundation for the next rung: N > 2 stages, where the message types and per-stage worker contract here generalize directly (each intermediate stage just forwards activations to `pp-stage-{i+1}`).
