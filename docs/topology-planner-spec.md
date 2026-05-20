# Topology Planner — Spec

> Companion to [`ROADMAP.md`](./ROADMAP.md), Rung 5. Builds on the
> [N-stage PP spec](./n-stage-pp-spec.md), Rung 1.

## 1. Goal

A pure function:

```
plan(model: ModelLayout, nodes: Vec<NodeDescriptor>, constraints: Constraints) -> Plan
```

The orchestrator (`pp-smoke-run` today, future inference launchers
later) calls `plan(...)` *before* renting anything, and uses the
returned `Plan` to drive both provisioning (which offers to rent) and
per-stage configuration (which layers/experts run where). No
hand-coded `k = total / NUM_STAGES` survives in the worker or in the
binary.

The V1 milestone is **N-stage pipeline parallelism only**. The
abstraction is shaped so that V2 (expert parallelism, rung 2) is a
proper extension, not a rewrite.

## 2. Why this exists

Three forces push the planner into existence at this point:

1. **N-stage PP (rung 1) makes "where does layer X live" a free
   variable.** With heterogeneous consumer GPUs the answer is no
   longer "halfway." It depends on VRAM, FLOPs, and link bandwidth.
2. **Expert parallelism (rung 2) makes "where does expert E live" a
   free variable.** Hand-encoding that mapping in the binary is the
   pain that rung 5 exists to prevent. Pulling the abstraction out
   *before* EP means EP slots in instead of duplicating placement
   logic.
3. **Without a planner, the orchestrator and the workers have to
   share placement assumptions through env vars** (the current
   `STAGE` / `NUM_STAGES` pattern). That works for one configuration;
   it does not scale to "different node has different layer count."

## 3. Non-goals (V1)

* Not a global scheduler. One `plan(...)` call per `pp-smoke-run`
  invocation. No re-planning mid-run.
* Not an online cost model. V1 uses static node descriptors (vast.ai
  offer metadata). Live profiling lands with rung 7 (timing harness).
* Not aware of network topology beyond "every node can reach every
  other node via iroh." Edge-cost-aware placement is a V3 concern.
* Not a CLI in its own right. It is a library function called by
  orchestrator binaries.

## 4. Crate placement

New crate: **`crates/planner`**.

* No dependency on `swactor` core, on `distribution`, or on the
  `examples/` crates. The planner is pure data-in, data-out — it
  should be testable without spinning up a runtime.
* Depends only on `serde`, `serde_json`, and `thiserror` (already in
  the workspace).
* Examples (`pipeline-parallel-inference`, future MoE example)
  declare `planner = { path = "../../crates/planner" }`.

This isolation is the load-bearing design choice. The moment the
planner reaches into runtime state it stops being testable as a pure
function, and the temptation to embed it inside a binary returns.

## 5. Data model

### 5.1 Inputs

```rust
/// Static description of the model to be served. V1 covers dense
/// transformer models like llama3. V2 will extend with `experts`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ModelLayout {
    pub name: String,            // "llama3.2:1b"
    pub num_blocks: u32,         // total transformer blocks
    pub hidden_dim: u32,         // for activation-bandwidth math
    pub bytes_per_activation_elem: u8, // 2 for bf16, 1 for fp8 future
    pub block_params_bytes: u64, // average weight bytes per block
    pub head_params_bytes: u64,  // output_norm + lm_head combined
    pub embed_params_bytes: u64, // token_embd table
}

/// Static description of a candidate node. Populated from vast.ai
/// offer metadata (vastai::find_offer already returns dph_total, GPU
/// name, VRAM); the planner does not call vast.ai itself.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NodeDescriptor {
    pub id: String,              // opaque to the planner; "offer-12345"
    pub gpu_name: String,        // "RTX 4090" — for logs only
    pub vram_bytes: u64,         // hard capacity constraint
    pub flops_estimate: f64,     // bf16 TFLOPs, rough; for balancing
    pub link_mbps: f64,          // best-effort outbound; for warnings
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Constraints {
    /// Minimum number of stages to use. Default: 1. Bumped by the
    /// orchestrator when the model does not fit on the largest node.
    pub min_stages: Option<u32>,
    /// Maximum number of stages to use. Default: nodes.len().
    pub max_stages: Option<u32>,
    /// Reserve VRAM headroom per node (KV cache, framework overhead,
    /// safety margin). Default: 20% of vram_bytes.
    pub vram_headroom_frac: Option<f64>,
}
```

These types live in `crates/planner/src/model.rs`. They are
`Serialize + Deserialize` because the orchestrator passes a serialised
`Plan` to each `pp-gpu-node` at boot (see §7).

### 5.2 Output

```rust
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Plan {
    pub topology: Topology,
    /// One entry per stage, ordered by stage index. Length == N.
    pub assignments: Vec<StageAssignment>,
    /// Diagnostics — populated by the planner so the orchestrator can
    /// log the placement decision.
    pub rationale: PlanRationale,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Topology {
    /// V1: linear chain. Stage `i` sends activations to stage `i+1`;
    /// stage `N-1` sends NextToken back to stage 0.
    PipelineChain { num_stages: u32 },
    // Future:
    // PipelineWithExperts { num_stages: u32, experts_per_layer: u32 },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StageAssignment {
    pub stage: u32,                  // 0..N
    pub node_id: String,             // matches NodeDescriptor.id
    pub block_range: (u32, u32),     // [lo, hi) into ModelLayout.num_blocks
    pub owns_embed: bool,            // stage 0 only, V1
    pub owns_head: bool,             // stage N-1 only, V1
    pub estimated_vram_bytes: u64,   // sanity for the orchestrator
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PlanRationale {
    pub strategy: String,            // "balance-by-vram"
    pub notes: Vec<String>,          // free-form, human-readable
}
```

### 5.3 Errors

```rust
#[derive(thiserror::Error, Debug)]
pub enum PlanError {
    #[error("model does not fit: needs {needed} bytes, total available {available}")]
    InsufficientVram { needed: u64, available: u64 },
    #[error("no node can hold the largest indivisible component ({needed} bytes)")]
    NoNodeFitsHead { needed: u64 },
    #[error("constraints unsatisfiable: {reason}")]
    Infeasible { reason: String },
}
```

Insufficient VRAM is the most common real failure. The planner
**returns errors, never panics** — the orchestrator decides whether to
retry with different offers or surface the error to the user.

## 6. V1 algorithm — balance-by-vram

Greedy fit. Decision: how many stages, then which blocks per stage.

```
1. total_weights = num_blocks * block_params_bytes
                 + embed_params_bytes + head_params_bytes
2. usable_per_node[i] = vram_bytes[i] * (1 - vram_headroom_frac)
3. Sort nodes by usable VRAM descending. Pick the smallest N
   such that:
     - N is between (min_stages, max_stages)
     - N is ≤ nodes.len()
     - sum(usable_per_node[0..N]) >= total_weights
     - usable_per_node[0] >= head_params_bytes
       (stage N-1 always holds the head)
     - usable_per_node[0] >= embed_params_bytes
       (stage 0 always holds the embed)
   If no such N exists → PlanError::InsufficientVram or
   ::NoNodeFitsHead.

4. Sort the chosen N nodes by usable VRAM ascending. Walk blocks in
   order, assigning blocks to nodes such that no node exceeds its
   usable VRAM minus its fixed-component reservation (embed for
   stage 0, head for stage N-1, zero otherwise). Resulting splits are
   contiguous ranges by block index.

5. Pin stage 0 to the smaller of the two end nodes that can hold the
   embed; stage N-1 to the smaller end node that can hold the head;
   this leaves the largest interior node(s) for middle stages, which
   typically hold the most blocks.

6. Emit StageAssignment[] in stage order; emit Topology::PipelineChain.
   Populate rationale.notes with the per-stage VRAM utilisation and
   the slack-vs-tightness numbers, so the operator can see whether
   the run is hugging the VRAM ceiling.
```

The algorithm is intentionally dumb. It's a greedy bin-pack that
respects two anchor constraints (head and embed). It does **not**
optimise for compute balance — stages will not run at the same speed.
That is fine for V1 because:

* PP latency is dominated by the slowest stage, and the slowest stage
  is almost always the VRAM-tightest one.
* Rung 7 (timing harness) will give us real per-stage compute
  numbers, at which point V2 of the planner can switch to a joint
  VRAM + FLOPs objective.

### 6.1 Pseudocode test cases

```
Case A: 2 homogeneous nodes (24GB each), llama3.2:1b
  → Plan { N=2, blocks=[(0..8), (8..16)] }, slack ~80%

Case B: 3 mixed nodes (24, 16, 12 GB), llama3.2:1b
  → Plan { N=2 chosen — smallest N that fits with headroom },
    even if 3 are available. Constraints can force N=3 if min_stages=3.

Case C: 4 nodes (24, 24, 8, 8 GB), llama3.1:70b quantised
  → Plan { N=4, head on a 24GB end, embed on the other 24GB end,
           middles on the 8GB nodes with proportionally fewer blocks }

Case D: 2 nodes, head doesn't fit
  → PlanError::NoNodeFitsHead
```

## 7. Integration with the orchestrator

### 7.1 Before the planner exists (today, 2-stage MVP)

```
find_offer × 2 → create_pipeline_instances (STAGE/NUM_STAGES env)
   → pp-gpu-node computes block range internally from NUM_STAGES.
```

### 7.2 With the planner (target)

```
1. Operator runs `pp-smoke-run --vastai --num-stages 4 --model llama3.2:1b`
2. Orchestrator builds ModelLayout from a static table (next bullet)
   and Constraints from CLI flags.
3. Orchestrator queries vast.ai for an N-sized pool of offers,
   collecting NodeDescriptors from offer metadata.
4. Orchestrator calls planner::plan(model, nodes, constraints).
5. On PlanError: orchestrator may widen the offer pool and retry, or
   surface the error.
6. On Plan: orchestrator calls create_pipeline_instances with the
   exact offer ids the planner chose, AND injects the serialised
   StageAssignment for each stage as the env var PLAN_JSON.
7. pp-gpu-node reads PLAN_JSON at boot, locates its own
   StageAssignment by STAGE index, and passes the block range +
   owns_embed/owns_head flags to the worker via stdin JSON instead
   of the current "compute from NUM_STAGES" path.
```

The `ModelLayout` table lives in
`crates/planner/src/models/known.rs` — a small hardcoded map of
known models. Adding a model is a code change. (A model-info
discovery service is out of scope for V1; the universe of
ModelLayouts is small.)

### 7.3 Worker contract delta

The worker today reads `STAGE` and `NUM_STAGES` and computes the
range. With the planner, the orchestrator can override:

```
PLAN_STAGE_BLOCKS=lo,hi   # optional. When set, overrides the
                          # NUM_STAGES-based range computation.
PLAN_OWNS_EMBED=1         # optional. When set, this stage runs the
                          # embed step.
PLAN_OWNS_HEAD=1          # optional. When set, this stage runs the
                          # output head + sampler.
```

Backwards-compatible: when the env vars are absent, the worker falls
back to the current `NUM_STAGES`-based logic. This lets the planner
land before any orchestrator starts using it.

## 8. API surface

```rust
// crates/planner/src/lib.rs

pub mod model;     // ModelLayout, NodeDescriptor, Constraints
pub mod plan;      // Plan, Topology, StageAssignment, PlanRationale, PlanError
pub mod models;    // models::known::lookup(name) -> Option<ModelLayout>

pub fn plan(
    model: &model::ModelLayout,
    nodes: &[model::NodeDescriptor],
    constraints: &model::Constraints,
) -> Result<plan::Plan, plan::PlanError>;
```

That's it. One public function, two data modules, one constants
module. The whole crate should be under 500 lines.

## 9. Test plan

### 9.1 Property tests

Property tests are the right shape here because the planner is a pure
function over structured data.

* **Block conservation.** For any `(model, nodes, constraints)` that
  produces a `Plan`, the union of `block_range`s covers
  `[0..num_blocks)` exactly once. No gaps, no overlaps.
* **VRAM respect.** For every `StageAssignment`,
  `estimated_vram_bytes ≤ node.vram_bytes * (1 - headroom)`.
* **Anchor invariants.** Exactly one stage has `owns_embed=true`
  (stage 0); exactly one has `owns_head=true` (stage N-1).
* **Determinism.** Same input → same output. Useful as a regression
  test once the bin-packing has tie-breaks.

### 9.2 Scenario tests

Hand-rolled cases A–D from §6.1. Assertions are about high-level
shape (N, which node holds head, slack percentage) — not the
exact integer block counts, since those are an algorithm detail and
should be free to evolve.

### 9.3 What we **do not** test

* No "insert into Plan, look it up" round-trip tests.
* No assertions echoing the algorithm ("if VRAM sorted ascending,
  then assignment[0].node = nodes[0]"). Those bind the test to the
  implementation and break on refactors.
* No tests for the JSON serialisation shape itself — Serde-derive is
  already tested upstream.

## 10. Migration

1. **Stand up `crates/planner` as a no-op.** Public API, types,
   property-test scaffolding. `plan(...)` returns `Plan` derived from
   `Constraints::min_stages.unwrap_or(1)` and an even split — i.e. it
   reproduces today's `k = num_blocks / NUM_STAGES`. No call sites
   yet.
2. **Wire `pp-smoke-run --plan-debug`** to call `plan(...)` and print
   the result without using it for provisioning. Lets us validate the
   shape against real vast.ai offers without changing deploy
   behaviour.
3. **Extend `pp_tinygrad_worker.py`** to read `PLAN_STAGE_BLOCKS` etc.
   with fallback. Land before the orchestrator uses them.
4. **Implement balance-by-vram** in the planner (this is the only
   non-trivial code change).
5. **Switch `pp-smoke-run --vastai`** to consume the `Plan` for both
   offer selection and stage env injection. The default-balanced
   path becomes a special case (homogeneous nodes → equal split).
6. **Update `n-stage-pp-spec.md` §5.1**: the per-stage range
   computation moves from the worker to the plan. Mark the env-var
   fallback in the worker as deprecated.

Each step ships independently. The crate exists and is testable
before any code depends on it.

## 11. V2 preview (not part of this rung)

When EP (rung 2) lands, the planner extends:

* `ModelLayout` grows `experts: Option<ExpertLayout>` with
  `num_experts`, `experts_per_token (k)`, `expert_params_bytes`.
* `Topology` grows `PipelineWithExperts { num_stages, experts_per_layer }`.
* `StageAssignment` gains `owned_experts: Vec<ExpertId>`.
* Algorithm gains an expert-packing phase: fill each node's remaining
  VRAM after blocks with experts, balanced for top-k routing locality
  (a soft objective — keep experts that are commonly co-activated on
  the same node, if such data is available).

None of this changes the V1 API for dense models. That property is
the test of whether the V1 abstraction is in the right place.
