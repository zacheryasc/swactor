# pipeline-parallel-inference

A two-node pipeline-parallel inference example over swactor + iroh.
Stage 0 holds the embedding and the first half of the transformer
blocks; stage 1 holds the second half plus the output norm and LM head.
The two stages exchange `StageActivation` (forward) and `NextToken`
(feedback) messages over real iroh QUIC; the orchestrator submits one
`InferenceRequest` to `pp-entry` and waits for the `InferenceResponse`.

The model is `llama3.2:1b`. See `SPEC.md` for the wire protocol and
`TEST_SPEC.md` for the authoritative list of test names.

---

## Layout

```
examples/pipeline-parallel-inference/
├── Cargo.toml              # standalone crate; two binary targets
├── pp_tinygrad_worker.py   # stage worker (stub + real modes)
├── src/
│   ├── lib.rs              # public modules
│   ├── messages.rs         # InferenceRequest/Response, StageActivation, NextToken + codecs
│   ├── stage_actor.rs      # Stage0Actor, Stage1Actor, ProcessBridge plumbing
│   ├── topology.rs         # pp-entry / pp-exit / pp-stage-{i} name helpers
│   ├── iroh_transport.rs   # actor message pump over iroh
│   ├── vastai.rs           # rent N / destroy-all helpers
│   └── bin/
│       ├── pp_gpu_node.rs  # one stage per process, env-configured
│       └── pp_smoke_run.rs # orchestrator (--seed | --vastai)
└── tests/
    ├── t_codec.rs          # §1
    ├── test_worker.py      # §2, §3
    ├── t_actor.rs          # §4
    ├── t_topology.rs       # §5
    ├── t_cluster.rs        # §6
    ├── t_vastai.rs         # §7
    ├── t_integration.rs    # §8 (stub) + §9 equivalence (#[ignore])
    └── t_binary.rs         # §10
```

---

## Running the tests

All paths below assume the repository root as the working directory.

### Rust fast tier

Everything that does not need a real model:

```sh
cargo test --manifest-path examples/pipeline-parallel-inference/Cargo.toml
```

This covers codecs, stage actors against the stub worker, topology /
SWIM name resolution, the in-process iroh cluster, the wiremock-backed
vast.ai client, the stub end-to-end integration tests, and the two stub
binary E2E tests in `t_binary.rs`.

### Rust slow tier (`#[ignore]`)

The slow tier needs:

- `clang` (tinygrad's CLANG backend) **or** an NVIDIA GPU with the CUDA
  toolchain (set `CUDA=1`), with `tinygrad` importable on `python3`.
- The `llama3.2:1b` GGUF cached in tinygrad's standard model location.
- ~30s startup per worker plus per-token decode time; budget at least a
  few minutes per test.

```sh
cargo test --manifest-path examples/pipeline-parallel-inference/Cargo.toml -- --ignored
```

Slow Rust tests:

- `t_integration.rs::sliced_pipeline_matches_full_generate_*` — drives
  the real-mode pipeline against a single-process reference and asserts
  token-for-token equality (`max_tokens=8`).
- `t_binary.rs::binary_e2e_two_tinygrad_workers_returns_response` —
  real-mode variant of the binary smoke run.

### Python tests

```sh
pytest examples/pipeline-parallel-inference/tests/
```

Fast pytest run covers the stub-mode protocol: env handling, op
dispatch, stage-0 / stage-1 boundary cases, error handling, malformed
JSON, EOF behavior. The real-worker class is gated behind an env var:

```sh
PP_REAL_WORKER_TESTS=1 pytest examples/pipeline-parallel-inference/tests/test_worker.py::TestRealTinygradWorker
```

The gated class loads `llama3.2:1b` and exercises one prefill + decode
round-trip per stage. Same prerequisites as the Rust slow tier.

---

## Localhost smoke run

```sh
cargo build --manifest-path examples/pipeline-parallel-inference/Cargo.toml --release
```

Then:

```sh
PP_WORKER_STUB=1 ./examples/pipeline-parallel-inference/target/release/pp-smoke-run \
    --seed \
    --gpu-node ./examples/pipeline-parallel-inference/target/release/pp-gpu-node \
    --worker  ./examples/pipeline-parallel-inference/pp_tinygrad_worker.py \
    --prompt "Say hello" \
    --max-tokens 16
```

`PP_WORKER_STUB=1` selects the deterministic stub worker — no GPU, no
GGUF, no tinygrad import. The orchestrator forwards `PP_WORKER_STUB`,
`MODEL`, `PYTHON`, `CUDA`, and `WORKER_CMD` from its own environment to
each child `pp-gpu-node`, so changing modes is a single env-var flip.

For real-mode local inference, drop `PP_WORKER_STUB` and set the
appropriate backend env:

```sh
CUDA=1 MODEL=llama3.2:1b ./examples/pipeline-parallel-inference/target/release/pp-smoke-run \
    --seed \
    --gpu-node ./examples/pipeline-parallel-inference/target/release/pp-gpu-node \
    --worker  ./examples/pipeline-parallel-inference/pp_tinygrad_worker.py \
    --prompt "Say hello" \
    --max-tokens 64
```

Use `PYTHON=1` for the CPU fallback. On success the orchestrator prints
the response between `=== pipeline-parallel Inference Response ===`
banners and exits 0. It always kills both `pp-gpu-node` children before
returning.

### Flags

| Flag | Default | Purpose |
|---|---|---|
| `--seed` / `--vastai` | (required, exactly one) | mode selector |
| `--prompt <text>` | `Say hello` | request prompt |
| `--max-tokens <n>` | `64` | decode-loop cap |
| `--gpu-node <path>` | sibling of `pp-smoke-run` | `pp-gpu-node` binary path |
| `--worker <path>` | `pp_tinygrad_worker.py` next to `Cargo.toml` | worker script path |
| `--api-key <key>` | — | vast.ai API key (`--vastai` only) |
| `--gpu <name>` | `RTX 4090` | GPU constraint (`--vastai` only) |
| `--image <name>` | `swactor-pp-gpu:latest` | container image (`--vastai` only) |

---

## vast.ai mode (documented, not executed here)

`pp-smoke-run --vastai` rents two GPU instances, deploys the
`pp-gpu-node` image to each (one with `STAGE=0`, one with `STAGE=1`,
both with `SEED_ADDR=<local node>` and an iroh relay URL), waits for
SWIM convergence over WAN, sends one `InferenceRequest`, prints the
response, and destroys both instances on the way out — including on
partial-success rentals or destroy failures.

```sh
./examples/pipeline-parallel-inference/target/release/pp-smoke-run \
    --vastai \
    --api-key "$VASTAI_API_KEY" \
    --gpu "RTX 4090" \
    --image swactor-pp-gpu:latest \
    --prompt "Say hello" \
    --max-tokens 64
```

This path is exercised end-to-end against a `wiremock` server in
`t_vastai.rs` (rental, partial-success rollback, destroy-all error
fan-out). It is **not** run against real vast.ai by the test suite; the
real run is the human-driven step described in TEST_SPEC §11.

---

## Stub vs real mode

The Python worker is a single file (`pp_tinygrad_worker.py`) that
branches on `--stub` / `PP_WORKER_STUB=1`:

- **Stub mode.** No tinygrad import, no GGUF load. Deterministic
  byte-shaped activations, a small synthetic vocab, and an EOS rule
  that lets the integration tests cover the EOS termination path. The
  whole test suite except the slow tiers runs in this mode.
- **Real mode.** Loads `llama3.2:1b` via `Transformer.from_gguf`,
  slices `model.blk` to its stage's range, runs the real forward and
  argmax sampler. Stage 0 owns tokenization + `model.token_embd`;
  stage 1 owns `model.output_norm` + `model.output` and detokenization
  via `SimpleTokenizer.from_gguf_kv`.

Switching modes from Rust: `pp-smoke-run` reads `PP_WORKER_STUB` from
its environment and forwards it to each child. `pp-gpu-node` and the
stage actors toggle their tokenization path on the same variable, so a
real-mode run does not feed synthetic whitespace-split ids into a real
embed lookup.
