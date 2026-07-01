# MVP node image + GGUF deployment shape

## Goal

Prepare the first deployment-test node image for one rented GPU node.

The image should be minimal: swactor MVP node binary plus tinygrad with CUDA support. The node should stay at the swactor level; Python is only the compute helper, not the top-level application runtime.

The first GGUF target is a small ~1B parameter model fetched whole from HuggingFace and cached on the node.

## Existing reference

The closest existing image is in `apps/old-pipeline-parallel-inference`:

- `Dockerfile.base`
  - CUDA runtime/base image layering;
  - Python, tinygrad, numpy;
  - NVRTC runtime;
  - CUDA headers copied from a builder stage;
  - sshd;
  - PID-1 entrypoint.

- `Dockerfile`
  - thin code layer over the base image;
  - copies Rust binaries and worker script.

- `pp_entrypoint.sh`
  - starts sshd deterministically;
  - accepts VastAI-injected public keys;
  - runs the worker as a child;
  - keeps the container alive after worker exit for postmortem.

- `pp_tinygrad_worker.py`
  - contains practical GGUF/tinygrad loading lessons;
  - fetches model artifacts;
  - uses tinygrad tokenizer/model code;
  - old runtime enters the Python worker protocol directly.

The new image should reuse the packaging/runtime lessons, not the old pipeline-parallel application shape.

## Runtime architecture

```text
VastAI or Docker provisioner
  -> container entrypoint starts sshd
  -> entrypoint starts mvp node binary
  -> mvp node joins orchestrator over iroh/swactor
  -> mvp node registers node actor(s)
  -> mvp node starts tinygrad helper as a child process
  -> helper fetches/caches GGUF
  -> helper loads model on CUDA
  -> orchestrator drives prompt/inference via swactor messages
```

## Binary shape

Add or identify a dedicated deployment node binary, for example:

```text
crates/mvp-system/src/bin/mvp-node.rs
```

Responsibilities:

- read deployment env/config;
- start iroh/swactor runtime;
- join the orchestrator;
- register the MVP node actor path;
- start and supervise the tinygrad helper process;
- emit a ready JSON line on stdout once the swactor node is reachable;
- continue pumping runtime until shutdown or fault.

This should be the deployment form of the existing local node role, not coupled to local-only TCP/test assumptions.

## Image shape

Layering should follow the old image pattern:

```text
mvp-node-base
  CUDA runtime
  NVRTC runtime library
  CUDA headers required by tinygrad runtime JIT
  python3
  tinygrad + numpy
  openssh-server
  mvp entrypoint

mvp-node
  /usr/local/bin/mvp-node
  /usr/local/share/mvp/tinygrad_worker.py
```

NVRTC is needed because tinygrad's CUDA backend JIT-compiles kernels at runtime. The old image installs the runtime library in the final image and copies CUDA headers from a builder stage; keep that lesson unless a better tinygrad-compatible base is chosen.

## Entrypoint shape

The entrypoint should:

- install/accept SSH public keys from VastAI-compatible env;
- start `sshd` before the node binary;
- print bootstrap progress to stdout/stderr for SSH datastream capture;
- run `/usr/local/bin/mvp-node` as a child;
- tee node output to a log file readable over SSH;
- keep PID 1 alive after node crash for postmortem.

This mirrors the old `pp_entrypoint.sh` behavior, with MVP names and paths.

## GGUF shape

First deployment target:

- whole-file HuggingFace GGUF fetch;
- small ~1B parameter model;
- node-local cache path;
- progress/fault observations surfaced through the node/datastream path;
- tokenizer from GGUF or adjacent tokenizer source, depending on what the chosen model path supports.

Existing MVP planning code already has concepts for this:

- `run_plan.rs::GgufSource::{LocalPath, HuggingFaceGguf}`;
- `TokenizerSource::{EmbeddedGguf, LocalPath}`.

Known gap:

- actor/runtime weight commands currently collapse to test artifacts in the node-agent/stage-controller path.
- real `GgufSource` must be carried through to the node/worker load command before this deployment path is real.

## Tinygrad helper shape

Python helper remains subordinate to swactor.

Responsibilities:

- import tinygrad lazily;
- verify CUDA with a small tensor realization;
- fetch/cache the requested GGUF;
- load the model/tokenizer;
- execute inference operations requested by the swactor node;
- emit structured lifecycle/progress/fault events.

Do not make Python the deployment app entrypoint. The app entrypoint is the swactor node binary.

## Local verification target

This workstation has a GPU and Docker CUDA should work. The deployment image should be verified locally before VastAI.

Target E2E:

1. Build base image.
2. Build thin MVP node image.
3. Run with `docker run --gpus all`.
4. Confirm bootstrap stdout/stderr are capturable.
5. Confirm `mvp-node` starts and joins the orchestrator path.
6. Confirm native datastream frame collection from the container.
7. Confirm tinygrad imports and realizes a CUDA tensor.
8. Confirm the chosen GGUF is fetched/cached from HuggingFace.
9. Confirm one prompt/inference request completes.

The Docker E2E should use the same image shape expected by VastAI; only provisioning differs.

## Non-goals for this doc

- designing the full prompt loop schema;
- full VastAI provisioning policy;
- sharded/range GGUF fetching;
- multi-node pipeline-parallel layout.
