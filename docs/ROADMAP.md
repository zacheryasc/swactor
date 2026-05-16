# swactor Inference Buildout Roadmap

## 1. Where we are

Two examples are working end-to-end:

* `examples/single-gpu-inference` — one rented vast.ai GPU, one tinygrad
  worker, full request/response lifecycle including teardown.
* `examples/pipeline-parallel-inference` — two rented GPUs, `llama3.2:1b`
  split half-and-half, autoregressive decode loop closed across the chain.

Both share: swactor `Runtime`, the `distribution` crate (iroh transport +
SWIM membership + name registry), the `swactor-process` crate (Python
worker as a child process), and a tinygrad-based worker contract.

## 2. Where we are going

**Long-term goal.** Inference a large MoE model (Kimi-scale) on consumer
GPUs over WAN, at ~20 tok/s.

**Working hypothesis.** Breadth first, then depth. We can't sensibly
optimise something we haven't built — bottlenecks at N=2 don't predict
bottlenecks at N=20 or with expert parallelism. So we expand the
**capability surface** until the topologies we want to run actually
boot, then measure, then optimise.

This document sets the sequencing. Each rung gets its own spec
(`docs/<rung>-spec.md`).

## 3. The sequence

```
   ┌──── done ────┐                                  ┌── future ──┐
   │              │                                  │            │
   ├ single-gpu   │   ┌── 1 ──┐   ┌── 5 ──┐   ┌─ 2 ─┐│   6 + 7    │
   │              ├─►  N-stage  ─► topology ─► expert ├─►  fault + │ ─► …
   ├ 2-stage PP   │      PP        planner      parallelism  timing
   └──────────────┘
```

### 3.1 Rung 1 — generalise PP from 2 → N stages

Spec: [`n-stage-pp-spec.md`](./n-stage-pp-spec.md).

The current PP example is hardcoded to two stages
(`pp_gpu_node.rs:214` rejects `NUM_STAGES != 2`; there are two distinct
`Stage0Actor` / `Stage1Actor` types). The topology helpers
(`topology.rs`) already parameterise on N, the worker
(`pp_tinygrad_worker.py`) already reads `STAGE`/`NUM_STAGES` from env
and computes its layer range generically — but the orchestrator and the
actor structure do not.

**Outcome.** A linear chain of N ≥ 2 stages runs the same `llama3.2:1b`
end-to-end via `pp-smoke-run --vastai --num-stages N`. N=4 on vast.ai is
the acceptance test.

**Why first.** Smallest delta against existing code. Surfaces the first
real bottleneck (per-hop activation latency compounding across the
chain) and is the lowest-risk validation that the swactor abstractions
hold up under the load they were designed for. Generalising the
orchestrator's setup dance (address propagation, name-registration
ordering) is also a prerequisite for everything that follows: rung 5
**emits** topologies, and rung 2 **uses** the same propagation
machinery for expert placements.

### 3.2 Rung 5 — topology planner

Spec: [`topology-planner-spec.md`](./topology-planner-spec.md).

Today, "stage 0 owns layers `[0..k)`, stage 1 owns `[k..N)` with
`k = N/2`" is hand-coded in the worker. Adding more stages means
spreading more hand-coding across more places. The planner takes
`(model_layout, [node_descriptor])` and emits a `Plan` describing what
runs where. The orchestrator consumes the `Plan` to drive
`create_instance` calls and the per-stage env injection.

**Outcome.** A new `crates/planner` crate. `pp-smoke-run` consults it
instead of computing the split inline; the per-stage env vars become a
JSON `Plan` artifact passed via env or stdin.

**Why before rung 2.** Expert parallelism's whole purpose is "this
expert lives on that consumer GPU." That mapping has to come from
*somewhere*, and that somewhere is the planner. Building EP without a
planner means hand-coding expert placement, which is the exact pain
that the planner exists to eliminate. The planner V1 (PP-only) is
small; V2 (EP-aware) is the natural next iteration.

### 3.3 Rung 2 — expert parallelism

Spec: TBD (lands as `docs/expert-parallelism-spec.md` once rung 5 is in
flight).

The shape: each MoE layer has a `Router` actor that picks top-k
experts; each expert is its own actor on (potentially) a different
node, registered under `moe-expert-{layer}-{idx}`. Activations
fan out to k experts, results gather, the layer continues. This is a
different topology than PP — point-to-multipoint, dynamic per token —
and it's the one that maps cleanly onto "lots of cheap consumer GPUs
each holding a few experts."

**Outcome.** A toy MoE model (or a synthetic MoE worker) runs across
≥ 4 nodes with experts placed by the planner.

**Why third.** EP needs both N-stage chain plumbing (most real MoE
models still have a PP dimension; experts live inside transformer
blocks, not instead of them) and the planner (to decide which experts
go where). Doing it first means re-doing it after both arrive.

### 3.4 Rung 6 + 7 — fault tolerance & timing harness

Specs: TBD.

These run in parallel because they barely interact:

* **(6) Fault tolerance.** Today any iroh drop kills the whole run.
  Cheapest first step: retry-with-backoff on a single hop's activation
  send, idempotent at the receiver because of `request_id`. Later: stage
  re-elections, alternate-path routing through the planner.
* **(7) Timing harness.** Per-token latency breakdown by hop:
  tokenize, per-stage compute, per-hop wire RTT, activation byte
  count. Output a flame-graph-ish trace. This is the **prerequisite for
  the optimisation phase** — without it, we cannot prioritise where to
  spend engineering effort against the 20 tok/s goal.

**Why last.** Both are most useful when the topology surface is wide.
A timing harness on N=2 is mildly informative; on N=8 + MoE it is the
only way to find the bottleneck. Fault tolerance on N=2 has nowhere to
fall back to; on a richer topology, re-routing is meaningful.

## 4. Out of this roadmap (deliberately)

These are real and important but not on the critical path to the goal:

* **Tensor parallelism within a stage.** Only matters if a single dense
  layer overflows one consumer GPU. For Kimi the dense pieces are
  smallish per-token; defer until evidence forces it.
* **Heterogeneous device backends** (Metal/ROCm/CPU). Cheap in code
  (tinygrad already supports them), expensive in CI matrix. Worth doing
  but does not gate any rung above.
* **Microbatching / pipeline-bubble elimination.** A throughput
  optimisation that becomes relevant after rung 7 tells us bubbles are
  in fact the bottleneck.
* **Quantised activations on the wire.** Same reasoning — only after
  rung 7 measures activation bandwidth as a real cost.

## 5. Definition-of-done for the roadmap

The roadmap closes when `pp-smoke-run --plan kimi.json` (or whatever
the CLI ends up being) brings up a heterogeneous WAN cluster, runs a
Kimi-class MoE model end-to-end, and emits a per-token trace. **Hitting
20 tok/s is a separate, follow-on optimisation phase** — this roadmap
gets us to the point where we can credibly start that work.
