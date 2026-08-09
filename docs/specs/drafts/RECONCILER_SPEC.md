# cluster reconciler — specification

Id: 3
Last modified:
Last reviewed:

**Scope:** a level-triggered reconciler that drives a declared cluster shape toward
convergence over the existing node lifecycle, living in `crates/provisioning`
alongside `NodeManager`.

## 1. Purpose

Today, node lifecycle is **edge-triggered and imperative**: `NodeManager` reacts to
discrete events (`LeaseCreated`, `EndpointKnown`, `BootstrapObserved`…) and emits
commands, and `apps/myelin`'s orchestration module drives those managers by hand,
deciding *when* to start, retry, and tear down each node. There is no object that
owns "the cluster should look like *this*." Scaling, replacement, and recovery are
woven into imperative workflow code.

This spec introduces a **reconciler**: a pure, level-triggered function that, given
a desired cluster shape and the currently observed cluster state, emits the effects
that move observed → desired. A **driver** calls it repeatedly (periodically and on
events), an **executor** applies the effects against providers, and observations
fold back into state for the next pass. The system converges; it does not execute a
script.

The mindset shift: **edge-triggered imperative workflow → level-triggered
declarative convergence.** The reconciler never asks "what event just happened?" It
asks "given where this node is and where it must be, what is the next step?"

## 2. Scope

**In scope**

- The reconciler contract: desired state, observed state, the pure `reconcile`
  function, and the effect vocabulary.
- Driving semantics: periodic + event triggers, one-step-per-pass convergence,
  idempotency, failure/backoff.
- Cluster-topology reconciliation: scale up/down across node groups, replacement on
  shape change.
- The responsibility split between the reconciler (pure decider), the driver
  (stateful owner of the node fleet), and the executor (applies effects to
  providers).
- How the reconciler maps onto the existing `NodeManager` / `NodeStage` state
  machine without inventing a parallel lifecycle.

**Out of scope**

- Actor workload placement on reconciled nodes (future scope; topology only for v1).
- Data-plane / connectivity reconciliation (iroh mesh, datastream links) as part of
  the shape (future scope).
- Specific provider adapters (Vast.ai, Docker). Those implement the existing
  `ProviderPlugin` / `ProvisionPlugin` executor seams.
- Backpressure and admission policy across the whole cluster.
- Persistence and leader election (single driver instance assumed for v1).

## 3. Model

Three roles, one invariant.

- **Reconciler** — a pure, deterministic function
  `reconcile(observed, desired) → plan`. No I/O, no clocks beyond an injected
  `now()`, no mutation of inputs. Given identical inputs it yields an identical
  plan. This is the load-bearing seam: everything testable and provider-neutral
  lives here.
- **Driver** — the stateful loop. It owns the fleet of per-node state machines
  (today: a `NodeManager` per logical node), calls the reconciler each pass,
  dispatches the plan to the executor, folds provider/bootstrap observations back
  into observed state, and decides *when* to run (periodic tick + event triggers).
  The driver is the only writer of observed state.
- **Executor** — applies effects against reality. Maps to the existing seams:
  `ProviderPlugin` (lease lifecycle: create / lookup / destroy) and
  `ProvisionPlugin` (node process) plus bootstrap sessions. `apps/myelin`'s
  orchestration module becomes this layer.

**Invariant — the reconciler is pure; the driver owns all state and waiting.**
This mirrors the engine split (ENGINE_SPEC §3): core never waits, the engine owns
all waiting. Here, the reconciler never waits or mutates; the driver owns the
node fleet, the clocks, and the retry timers.

## 4. State

### Desired state

Authoritative, supplied by the caller, held immutably between shape edits:

```rust
// already defined in node.rs — unchanged
pub struct RunNodeGroupSpec { /* run_id, group_id, role, count, provider, shape, boot, swarm_join */ }
pub fn expand_node_group(group: &RunNodeGroupSpec) -> Vec<LogicalNodeSpec>;

// new
pub struct ClusterShape {
    pub run_id: RunId,
    pub groups: Vec<RunNodeGroupSpec>,
}
impl ClusterShape {
    pub fn expand(&self) -> Vec<LogicalNodeSpec> { /* flatMap expand_node_group */ }
}
```

`ClusterShape` is a thin bag over the existing group spec; `expand` reuses
`expand_node_group`. The expanded `Vec<LogicalNodeSpec>` is the set of logical nodes
the cluster **should** contain.

### Observed state

The set of logical nodes the cluster **does** contain, each with its lifecycle
facts. `NodeRecord` already is per-node observed state. The cluster wraps it:

```rust
// NodeRecord already holds: desired, stage, ready, lease, connection, bootstrap,
// swactor, failed_reason, destroyed_at.

pub struct ClusterState {
    pub nodes: BTreeMap<LogicalNodeId, NodeRecord>,
}
```

The driver is the sole writer of `ClusterState`. Observations (lease results,
endpoints, bootstrap progress, swactor joins, failures) are folded into `NodeRecord`
between passes — exactly the work `NodeManager::handle` already does internally;
under the reconciler that folding is the driver's job (see §7).

## 5. The reconciler contract

```rust
pub struct ReconcilePlan {
    /// Desired nodes with no observed record: begin their lifecycle.
    pub to_start: Vec<LogicalNodeSpec>,
    /// Observed nodes with no desired entry: tear them down.
    pub to_destroy: Vec<LogicalNodeId>,
    /// Live nodes: the next one or more commands to advance each toward desired.
    pub per_node: Vec<(LogicalNodeId, Vec<NodeManagerCommand>)>,
}

/// Pure. Deterministic. No I/O.
pub fn reconcile(
    observed: &ClusterState,
    desired: &ClusterShape,
    now: SystemTime,
) -> ReconcilePlan;
```

`reconcile` is **level-triggered**: it reads only `observed` + `desired` (+ `now`
for backoff; see §8). It does not know which event triggered the pass. It is
**idempotent**: re-running with unchanged inputs yields the same plan, and an effect
whose result is already reflected in observed state is never re-emitted (e.g. a node
whose `record.lease` is `Some` never yields `CreateLease` again).

### Per-node reconcile

For each live node, `reconcile` computes the next step from `(NodeRecord,
LogicalNodeSpec)` — a pure function over the existing `NodeStage` machine:

```rust
/// One pass = at most one lifecycle step per node. Convergence happens across
/// passes, not within one.
fn reconcile_node(record: &NodeRecord, desired: &LogicalNodeSpec, now: SystemTime)
    -> Vec<NodeManagerCommand>;
```

The mapping reuses the existing `NodeManagerCommand` vocabulary and the existing
`NodeStage` transitions — it is the **level** reading of the same state machine that
`NodeManager::handle` expresses in **edge** form:

| observed (`record`) | next effect(s) |
|---|---|
| `New`, no lease | `CreateLease` |
| `LeaseCreated`, lease carries endpoint | `StartBootstrap` |
| `LeaseCreated`, endpoint unknown | `LookupEndpoint` |
| `EndpointKnown` / `BootstrapRunning` | (none — awaiting bootstrap observation) |
| `BootstrapRunning`, swactor joined | `BootstrapConvergenceObserved` |
| `HandedOff` / `Dormant`, `ready` | (none — steady state) |
| `Failed`, within backoff window | (none — waiting; see §8) |
| `Failed`, backoff elapsed | reset to `New` → `CreateLease` (retry) |
| destroy requested | `CancelBootstrap` (if active) + `DestroyLease` |

**One step per node per pass.** This is the heart of the level-triggered model:
the reconciler never waits within a pass. It emits the step the current observed
state permits; the executor applies it; observation updates the record; the next
pass emits the next step. Ordering across the lease → bootstrap → join chain falls
out of convergence, not from an explicit workflow.

## 6. Topology reconciliation

`reconcile` first diffs the expanded desired set against the observed set by
`LogicalNodeId`:

- **desired, not observed** → `to_start`. The driver instantiates a `NodeManager`
  for the spec; the first pass emits `CreateLease`.
- **observed, not desired** → `to_destroy`. The driver runs the destroy path
  (`CancelBootstrap` + `DestroyLease`); once `stage == Destroyed` the record is
  reaped.
- **both** → `per_node` via `reconcile_node`.

**Scale policy (v1, deliberately simple):** logical node identity is
`{group_id}-{index}`. Scaling a group up adds higher indices; scaling down removes
the **highest** indices first. A group's `shape` is treated as immutable per node:
changing a field that is not achievable in place (image, gpu, disk) is a
**replacement** — the affected logical nodes move to `to_destroy` and fresh specs to
`to_start` — not an in-place mutation. This is k8s-style immutable-spec rolling
replacement, kept coarse for v1.

## 7. Driving semantics

The driver runs a pass when **either** (a) a periodic tick fires, or (b) an event
arrives — a desired-shape edit, or a provider/bootstrap observation that changed
observed state. Each pass:

1. Snapshot current `ClusterState` and `ClusterShape`.
2. Call `reconcile(&observed, &desired, now)` → `ReconcilePlan`.
3. Apply the plan: create `NodeManager`s for `to_start`, drive destroy for
   `to_destroy`, dispatch each `per_node` command to the executor.
4. Fold executor results + pending observations into `NodeRecord`s (the driver's
   only write).
5. Repeat. Terminal when every desired node is `ready` and no orphans remain.

**Observation folding is the bridge from edge to level.** Provider/bootstrap events
arrive as the existing observation types (`CreateLeaseResult`, `SshEndpoint`,
`BootstrapObservation`, swactor-join, failures). The driver folds each into the
node's `NodeRecord` between passes — the same field updates `NodeManager::handle`
performs today (`record.lease = …`, `record.bootstrap.last_stage = …`, etc.). The
reconciler then reads the updated record and emits the next step. The edge-triggered
`NodeManager::handle` and the level-triggered `reconcile_node` are two readings of
one state machine; see §10 for the migration choice.

**Non-reentrancy.** A pass is synchronous and exclusive: the driver never runs two
passes concurrently. This matches the worker non-reentrancy invariant (ENGINE_SPEC
§5).

## 8. Failure and backoff

Failure is **observed state**, not a control-flow signal. A node reaching
`NodeStage::Failed` records `failed_reason` and `failed_at`. The reconciler emits no
command for that node **until its per-node backoff window elapses** (hence `now` in
the signature); after the window it resets the node to `New` and re-emits
`CreateLease`. Backoff is **per-node and isolated** — one failed node never blocks
another (this is the pay-off of the hybrid granularity chosen in §9). Destroyed
nodes that were desired are simply re-started by the topology diff.

Backoff parameters (initial delay, cap, jitter) are driver configuration, not
reconciler logic — the reconciler only reads `failed_at` + the configured window
and decides "retry now" vs "wait." v1 uses a fixed window; exponential backoff is a
driver-side refinement.

## 9. Granularity and growth path

**Hybrid, by design.** v1 ships one top-level `reconcile` whose body is: topology
diff + fan-out to `reconcile_node`. The contract — pure function of (observed,
desired) → effects — is **identical at every level**, so the growth ladder is
internal refactor, never a contract change:

1. **Now** — one loop, topology diff + per-node reconcile. Simple.
2. **When shapes diversify** — fan out to sub-reconcilers per concern (node-groups,
   roles, future: data-plane links), each with the same signature; the top level
   merges their effect streams.
3. **If independent backoff/isolation/work-queues are ever needed** — promote a
   sub-reconciler to its own loop + driver. Same contract; the migration is
   mechanical.

`NodeManager` is already a sub-reconciler in waiting. The contract is what must not
ossify; the loop count is cheap to grow.

## 10. Relationship to existing code

| exists today | role under the reconciler |
|---|---|
| `NodeManager` + `NodeStage` | per-node observed state + lifecycle transitions. Kept. |
| `NodeRecord` | per-node observed state record. Kept; the driver writes it. |
| `NodeManagerCommand` | the effect vocabulary. Reused verbatim by `reconcile_node`. |
| `RunNodeGroupSpec` / `expand_node_group` / `LogicalNodeSpec` | desired state. Wrapped by `ClusterShape`; reused. |
| `ProviderPlugin` / `ProvisionPlugin` | executor seams. Unchanged; the driver calls them. |
| `apps/myelin` orchestration | becomes the driver + executor. Imperative workflow code is replaced by `reconcile` calls. |

**Open design choice — edge handle vs. level reconcile for `NodeManager`:**
`NodeManager::handle(msg) → Vec<NodeManagerCommand>` is edge-triggered; the
reconciler needs the level form. Two options, to be settled at implementation:

- **(A) Add `reconcile_node` alongside `handle`.** `handle` keeps folding streaming
  observations into the record (the parts that genuinely need event semantics, e.g.
  bootstrap seq numbers); `reconcile_node` reads the record and emits the next step.
  Minimal churn; two readings of one machine coexist. *Recommended for v1.*
- **(B) Split into `observe(&mut record, obs)` + `reconcile_node(&record, desired)`**
  and retire `handle`. One level-triggered path; more churn, cleaner end state.

Either way the *contract* in §5 is unchanged; only `NodeManager`'s internal shape
differs.

## 11. Invariants (normative)

1. **Purity.** `reconcile` performs no I/O, reads no global state, mutates no input.
   `now` is its only non-input dependency.
2. **Level-triggered.** `reconcile` is a function of `(observed, desired, now)`,
   never of "which event fired." Safe to call at any time.
3. **Idempotent.** Re-running with unchanged inputs yields the same plan; effects
   already reflected in observed state are not re-emitted.
4. **One step per node per pass.** Convergence is across passes, not within one.
5. **Driver is the sole writer of observed state.** The reconciler and executor
   never mutate `ClusterState`.
6. **Non-reentrant passes.** The driver never runs two passes concurrently.
7. **Failure isolation.** A failed node's backoff never blocks another node's
   progress.
