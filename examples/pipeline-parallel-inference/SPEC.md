# Pipeline-Parallel Inference — Spec (N stages)

> This spec covers the N-stage generalisation of the pipeline-parallel
> example. For the deployment-shape rationale and where this rung sits
> in the broader buildout, see [`/docs/ROADMAP.md`](../../docs/ROADMAP.md)
> §3.1. For the test enumeration, see
> [`./TEST_SPEC.md`](./TEST_SPEC.md).
>
> Behavioural snapshot of the **2-stage** deployment, as it actually
> runs today, is preserved in
> [`../single-gpu-inference/BEHAVIOR.md`](../single-gpu-inference/BEHAVIOR.md)
> for cross-reference and in
> [`./BEHAVIOR.md`](./BEHAVIOR.md) (to be written once the N-stage path
> is deployed).

## 1. Goal

`pp-smoke-run --vastai --num-stages N --api-key <key>` runs a linear
chain of `N ≥ 2` GPU nodes that jointly serve one `InferenceRequest`
end-to-end. Acceptance test: `N = 5` on vast.ai, `llama3.2:1b`,
prompt `"Say hello"`, produces non-empty text within the 5-minute
budget. The localhost (`--seed`) path supports the same N values via
spawning N child processes.

`N = 1` is **not** supported by this example — single-node inference
is what `examples/single-gpu-inference` exists for, and the role-based
actor design here is built on the assumption that "first" and "last"
are distinct stages. `N = 2` is the minimum: first stage handles the
embed + first half; last stage handles the second half + head; no
middle stages.

## 2. What changes vs. the 2-stage MVP

| Area | 2-stage today | N-stage target |
|---|---|---|
| `pp-gpu-node` binary | hard-checks `NUM_STAGES == 2` (line 214) and dispatches to `run_stage_0` or `run_stage_1` | accepts any `N ≥ 2`; dispatches to a single `run_stage` function parameterised by `(stage, num_stages)` |
| Stage actors | `Stage0Actor` and `Stage1Actor` are separate types | one `StageActor` parameterised by **role** (`First` / `Middle` / `Last`) |
| Message bridges | three bridge types (`Stage0RequestBridge`, `Stage0NextTokenBridge`, `Stage1ActivationBridge`) bound to specific actor types | same three bridge shapes, but generic over the unified `StageMsg` |
| Orchestrator (`pp-smoke-run`) | spawns exactly two children, hands stage 1 stage 0's `PEER_DIRECT` | spawns `N` children in order; each non-first child receives its **predecessor's** addressing as `PEER_DIRECT` |
| Topology helpers | already generic (`stage_name`, `next_stage_name`, `prev_stage_name`) | unchanged |
| Worker (`pp_tinygrad_worker.py`) | accepts stage-0 ops (`embed_and_forward`, `decode_step`) and stage-N-1 ops (`forward_and_sample`); already param on `STAGE`/`NUM_STAGES` for layer range | adds `forward_range` op for middle stages; layer-range computation unchanged |

The worker contract is the **anchor of stability**. Anything that
doesn't have to change, doesn't.

## 3. Actor design — a single `StageActor`

### 3.1 Role enum

```rust
pub enum StageRole {
    /// stage == 0. Owns: prompt entry, tokenizer, embed + first range.
    /// Reacts to: `InferenceRequest` from network; `NextToken` from
    /// the *last* stage (decode-loop feedback).
    First,

    /// 0 < stage < num_stages - 1. Owns: a middle block range, no
    /// tokenizer, no sampler. Reacts to: `StageActivation` from prev.
    Middle,

    /// stage == num_stages - 1. Owns: last block range, output head,
    /// sampler, detokenizer. Reacts to: `StageActivation` from prev.
    /// Emits: `NextToken` to stage 0; `InferenceResponse` to
    /// `reply_to` on terminate.
    Last,
}
```

Role is set once at construction and never changes. The actor's
per-message logic branches on it. The role is computed by the binary
from `(STAGE, NUM_STAGES)` and is a pure function (see TEST_SPEC §3).

### 3.2 Unified incoming message

```rust
pub enum StageMsg {
    Inference(InferenceRequest),     // First only
    Activation(StageActivation),     // Middle and Last
    NextToken(NextToken),            // First only
    Process(ProcessNotification),    // all roles
    SetNeighbors { prev: Option<ActorAddress>,
                   next: Option<ActorAddress>,
                   reply_to: Option<ActorAddress> },
    Reset,
}
```

Compared to today, this collapses `Stage0Msg` and `Stage1Msg` into one
type. `prev` and `next` are `Option` because the chain ends are
asymmetric. `reply_to` is `Some` only for `Last`. An actor that
receives a variant outside its role's set logs and drops it — this is
defensive, not load-bearing, because the orchestrator wires bridges
matching the role.

### 3.3 Per-role message behaviour

| Role | On `Inference` | On `Activation` | On `NextToken` |
|---|---|---|---|
| **First** | `tokenize` → `embed_and_forward` → send `StageActivation { is_prefill: true }` to `next` | drop | `done=true` → drop; else `decode_step` → send `StageActivation { is_prefill: false, seq_len: 1 }` to `next` |
| **Middle** | drop | `forward_range` → send `StageActivation { … }` to `next`, preserving `request_id`, `position`, `seq_len`, `is_prefill` | drop |
| **Last** | drop | `forward_and_sample` → send `NextToken` to `prev` (= the first stage); on terminate, `detokenize` → send `InferenceResponse` to `reply_to` | drop |

Note: the autoregressive feedback edge is **last → first**, not
last → prev. This is the same shape as the 2-stage MVP (where prev
*is* first), and the right shape for arbitrary N — the first stage is
the only one that owns the embed step.

### 3.4 New worker op: `forward_range`

Middle stages need a worker op that takes a hidden state, runs blocks
`[lo..hi)`, and returns the new hidden state. Stage 0's
`embed_and_forward` already runs a block range; stage 1's
`forward_and_sample` runs a range and then samples. The new op is the
range-only variant.

```
→ {"op": "forward_range", "request_id": rid, "hidden_b64": "...",
   "position": p, "seq_len": s}
← {"request_id": rid, "hidden_b64": "...", "seq_len": s}
```

In `pp_tinygrad_worker.py` this is the same code path as the prefix of
`forward_and_sample`, factored out and exposed as its own dispatch
case. Tinygrad's per-block KV cache continues to be allocated lazily
per block, so each middle stage owns the K/V for its own range
automatically.

## 4. Orchestrator changes (`pp-smoke-run`)

### 4.1 Seed mode

Today: spawn stage 0, read its `PP_GPU_NODE_ADDR`, spawn stage 1 with
that addressing as `PEER_DIRECT`. For N stages:

```
for i in 0..N:
    spawn child i with PEER_DIRECT = (addr of child i-1, if i > 0)
    read child i's PP_GPU_NODE_ADDR
```

Sequential, **not parallel.** Each subsequent child needs its
predecessor's direct addressing in `PEER_DIRECT`, so the spawn loop is
inherently a chain. Each child only needs its *predecessor's*
addressing (not all earlier addresses): the outbound dial from i → i-1
makes i-1's iroh stack learn i's source-socket reverse-path. The
seed/orchestrator is dialed by every child, so it always learns
everyone.

### 4.2 vast.ai mode

Today: `find_offer` twice (excluding the first offer's id), then
`create_pipeline_instances` with both offer ids. For N stages:

* `find_offer` N times, each call excluding all previously chosen
  offer ids.
* `create_pipeline_instances` takes a slice of N offer ids, sets
  `STAGE`, `NUM_STAGES`, `SEED_ADDR`, `SEED_RELAY` on each; in vast.ai
  mode, **no `PEER_DIRECT` is passed** — relay-mediated NAT traversal
  + SWIM gossip handle peer discovery (this is already how vast.ai
  mode works for 2 stages today, no change).
* `wait_for_running` once per contract, sequentially (same as today,
  just `N` times instead of 2).
* `destroy_all_instances` is already a slice-aware helper.

### 4.3 Convergence target

The orchestrator waits for SWIM to report `alive >= N` peers (today:
`alive >= 2`, i.e. N=2). One-line change in both `run_seed` and
`run_vastai`.

### 4.4 Submitting the request

Unchanged: resolve `pp-entry`, build route, `send_to`,
`await_response`. The chain is invisible to the orchestrator past the
entry node.

## 5. `pp-gpu-node` changes

### 5.1 Boot sequence (generalised)

```
1. Parse STAGE, NUM_STAGES (any N ≥ 2; remove the == 2 check).
2. Determine role: First if STAGE==0; Last if STAGE==N-1; else Middle.
3. Build iroh driver, join seed.
4. Wait for SWIM convergence (>= 1 alive peer — unchanged).
5. Spawn the StageActor with the role + placeholder addresses.
6. Register the per-index name pp-stage-{STAGE} immediately so
   neighbours can resolve us.
7. Wait for worker ready (unchanged).
8. Resolve neighbours by role:
     First:  resolve pp-stage-1 (next).
             Note: the feedback target (NextToken from last) is *us* —
             the last stage looks up pp-stage-0 itself.
     Middle: resolve pp-stage-{stage+1} (next).
             A middle does not send to prev; nothing to resolve there.
     Last:   resolve pp-stage-0 (the NextToken destination),
             plus pp-orchestrator (for InferenceResponse).
9. Build IrohActorTransport routes for each resolved neighbour.
10. SetNeighbors on the actor with the resolved addresses.
11. Pump 100ms to drain the setup message.
12. Register pp-entry (First) / pp-exit (Last). pp-entry must be
    registered last because the orchestrator may send InferenceRequest
    as soon as it resolves — and First is not ready to forward until
    pp-stage-1 is wired.
13. Enter main pump loop.
```

The step-8 "who do I resolve" matrix is the only real new logic. It
flows from the per-role message table in §3.3.

### 5.2 Name registry summary

| Stage | Registers | Resolves (and routes to) |
|---|---|---|
| First (0) | `pp-stage-0`, `pp-entry` | `pp-stage-1` |
| Middle (i) | `pp-stage-i` | `pp-stage-{i+1}` |
| Last (N-1) | `pp-stage-{N-1}`, `pp-exit` | `pp-stage-0`, `pp-orchestrator` |

This is exactly what `topology.rs::register_stage_names` already
emits — the topology helpers do not change.

## 6. Failure modes (deltas only)

The 2-stage failure table previously in this spec applies stage-by-stage.
N-stage-specific additions:

| Failure | Detection | Response |
|---|---|---|
| Fewer than N offers available | `find_offer` returns empty on attempt < N | Destroy any allocated instances, exit 1. |
| Any one of N instances never reaches running | Per-contract `wait_for_running` | Destroy *all* contracts, exit 1. |
| SWIM never converges to N alive peers | 180s timeout in vast.ai mode | Destroy all, exit 1. Fetch logs from each instance. |
| Any one of N workers never reports ready | Per-node `wait_for_worker_ready` (600s) | That stage exits 1; SWIM marks it dead; orchestrator times out on convergence and destroys all. |
| Middle stage drops a `StageActivation` (mid-decode) | Orchestrator's 600s response timeout | Destroy all, exit 1. Recovery is rung 6 (fault tolerance), out of scope here. |

The teardown invariant — "every code path that creates an instance
also destroys it" — generalises to "any creation failure rolls back
all already-created instances." `destroy_all_instances` already does
this; the loops above just have to call it on every error branch.

## 7. Test plan

The full enumeration of behavioural tests lives in
[`./TEST_SPEC.md`](./TEST_SPEC.md). Summary of tiers in landing order:

1. **Pure-Rust unit** — codec, topology helpers, role computation,
   per-stage layer-range math.
2. **Python worker contract** — `forward_range` (new), all existing
   ops verified against stub mode, layer-range math up to N=8.
3. **Stage actor (in-process, stub worker)** — `StageActor` in each
   of its three roles plus role-mismatch defensive drops.
4. **Cluster transport (in-process, real iroh)** — N-node clusters
   (N ∈ {2, 3, 4}) carry every message type intact across every hop.
5. **vast.ai client (mocked HTTP)** — N-instance create/destroy with
   distinct `STAGE` env vars and rollback on partial failure.
6. **In-process N-stage integration (stub worker, real iroh)** —
   full chain at N ∈ {2, 3, 4, 5}.
7. **Sliced-vs-full equivalence (real tinygrad on CPU, gated)** —
   the load-bearing correctness test: N-stage output token sequence
   matches single-process `Transformer.generate()` up to `max_tokens`.
   Run at N=2 (carried forward), N=3, and N=4.
8. **Localised binary E2E (`pp-smoke-run --seed`)** — the
   **pre-deploy gate**: runs the actual `pp-gpu-node` binaries
   spawned by the actual `pp-smoke-run` binary in `--seed` mode at
   N ∈ {2, 3, 5}.
9. **Manual smoke run on vast.ai at N=5** — gated, rare, the
   acceptance test for the rung.

A change that breaks any tier 1–4 test must not pass review. Tier 8
must pass before any change is deployed. Tiers 5–7 are CI-gated;
real-tinygrad and equivalence are `#[ignore]` and run on demand or in
a slow lane.

## 8. Migration

Land in this order, each its own PR:

1. **Worker op `forward_range`.** Additive in
   `pp_tinygrad_worker.py`. No call sites yet. Tested with Python
   tests in `test_worker.py`.
2. **Unify `Stage0Actor` + `Stage1Actor` into `StageActor`.** Keep
   message-shape behaviour identical for N=2. Delete the old types.
   Existing `t_actor.rs` tests adapt to the unified API. **This PR
   does not change the wire or the binary.**
3. **Add `Middle` role + actor logic** behind a constructor variant.
   Add N=3 / N=4 integration tests against the stub worker. Binary
   still rejects N != 2 to avoid premature deploys.
4. **Generalise `pp-gpu-node`** (drop the N==2 check; generalise the
   resolve/route step). Update `t_binary.rs` to cover N=3 in seed
   mode.
5. **Generalise `pp-smoke-run`** (spawn loop, find-offer loop,
   convergence target). Add `--num-stages` flag, default 2.
6. **Land tier-8 N=5 localised E2E.** This is the pre-deploy gate.
7. **Manual vast.ai run at N=5.** No code change — just exercise the
   path and capture the result in `BEHAVIOR.md`.

Each PR is independently mergeable and ships behind the existing
2-stage acceptance. Tier 8 at N=5 must pass before step 7.

## 9. Out of scope (this rung)

* Microbatching, multiple in-flight requests, streaming output.
* Dynamic stage assignment / rebalancing. Stages are decided at boot.
* Fault tolerance of any kind beyond "destroy everything on any
  failure." Rung 6.
* Heterogeneous layer splits — every stage still owns
  `total / N` blocks ± 1. The planner (rung 5) takes over splitting.
* `N = 1`. Use `examples/single-gpu-inference`.
