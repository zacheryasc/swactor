# cluster reconciler — specification

Id: 3
Last modified: af49ba5c2cbcf7d742a69c6e213597ce5664fb43
Last reviewed:
> Any edit to this spec must update `Last modified` above to the current `git HEAD` commit.

**Scope:** a level-triggered reconciler that drives a declared cluster shape toward
convergence over the existing node lifecycle, living in `crates/provisioning`
alongside the node lifecycle types.

## 1. Purpose

Today, node lifecycle is edge-triggered and imperative: lifecycle handlers react
to discrete observations and emit commands, while `apps/myelin` explicitly
sequences node acquisition, bootstrap, readiness, retry, and teardown. There is
no object that owns "the cluster should look like this."

This spec introduces a cluster reconciler. A pure decider compares the latest
desired shape with driver-owned observed state and returns the next actions that
move observed toward desired. A stateful driver coalesces triggers, applies state
transitions, dispatches effects, folds results back into observed state, and runs
the decider again. The system converges from whatever state is currently known;
it does not resume an imperative script from an event-specific continuation.

The design borrows Kubernetes controller mechanics—level-based decisions,
spec/status separation, keyed and coalesced triggers, durable deletion intent,
retries outside business transitions, and distinct logical versus concrete
identity—without requiring a Kubernetes API server. Swactor supplies serialized
state transitions; the engine supplies effect execution and timers.

## 2. Scope

**In scope**

- Desired cluster shape and driver-owned observed cluster state.
- A pure `reconcile(observed, desired, now) -> plan` contract.
- Observation folding that is separate from effect selection.
- Scale up/down and immutable-spec replacement across node groups.
- Stable logical-node identity and per-incarnation attempt identity.
- Coalesced event and periodic driving, non-reentrancy, and timed requeue.
- Pending-operation identity, effect-result correlation, and stale-result
  rejection.
- Per-node retry/backoff and cleanup-before-restart semantics.
- Mapping these mechanics onto `NodeRecord`, `NodeStage`, and
  `NodeManagerCommand` without a second node lifecycle.

**Out of scope**

- Actor workload placement on reconciled nodes.
- Data-plane and connectivity reconciliation as part of cluster shape.
- Provider-specific implementation details.
- Cluster-wide admission and backpressure policy.
- Persistence, process-crash recovery, and leader election. v1 assumes one
  process-local driver. The state model must not preclude later persistence.
- Configurable rollout availability budgets. v1 may replace multiple stale
  nodes concurrently; replacement is phased per logical node, not advertised as
  an availability-preserving rolling update.

## 3. Roles and ownership

Three roles uphold one state-ownership rule.

- **Reconciler** — a pure deterministic decider. It reads snapshots of desired
  and observed state plus injected time and returns driver actions. It performs
  no I/O and mutates no input. Purity is a Myelin testing seam, not a claim that
  Kubernetes reconcilers themselves are pure.
- **Driver** — the sole writer of observed state. It owns the node map, folds
  observations, coalesces triggers, calls the reconciler, applies driver-state
  actions, records operations as pending before dispatch, and schedules timed
  requeues.
- **Executor** — applies provider and bootstrap effects outside the driver
  transition. It reports accepted results or failures tagged with the operation
  and node-attempt identity. Blocking provider calls run as engine-hosted
  blocking work and never block a reconcile pass.

**Invariant — there is one authoritative observed state.**
`ClusterState.nodes` is authoritative. Observation reducers and the reconciler
operate on that state. A refactored `NodeManager` must not retain a second copy
of the same `NodeRecord` beside `ClusterState`.

## 4. Kubernetes-derived controller mechanics

The following mechanics are normative for this spec.

1. **Triggers carry identity, not decision input.** An event means only that the
   cluster may be dirty. Reconciliation rereads the latest complete state; it
   never branches on which event caused the pass.
2. **Latest desired state wins.** If desired shape changes A -> B -> C before a
   pass, convergence may proceed directly toward C. There is no obligation to
   touch B.
3. **Triggers coalesce.** Repeated triggers while a pass is queued collapse into
   one pass. A trigger arriving during a pass marks the cluster dirty and causes
   one further pass after the current pass completes.
4. **Desired and observed revisions are distinct.** `generation` identifies a
   desired-shape revision. `observed_generation` says only that the driver has
   evaluated that revision; readiness separately reports convergence.
5. **Deletion is state, not absence plus a one-shot command.** Once cleanup has
   begun it runs to completion. Reintroducing the same logical node while its
   old attempt is deleting does not resurrect the old attempt; the latest
   desired spec starts a fresh attempt after cleanup.
6. **Logical identity differs from concrete identity.** A stable logical slot
   may have many sequential attempts. Results from an old attempt cannot mutate
   the current attempt.
7. **Actuation is interruption-safe within the v1 process lifetime.** An effect
   is recorded as pending before dispatch, tagged with a stable operation ID,
   and correlated on completion. Ambiguous create outcomes use lookup/adoption
   rather than blind duplicate creation.
8. **Retry scheduling is controller state.** Backoff and timed requeue do not
   masquerade as node lifecycle stages. A retry deadline is sampled once,
   stored, and read by the pure reconciler.

## 5. Desired and observed state

### 5.1 Desired state

`RunNodeGroupSpec`, `LogicalNodeSpec`, and `expand_node_group` remain the desired
node vocabulary. `ClusterShape` adds a caller-controlled generation:

```rust
pub struct ClusterShape {
    pub run_id: RunId,
    /// Strictly increases whenever the supplied desired shape changes.
    pub generation: u64,
    pub groups: Vec<RunNodeGroupSpec>,
}

impl ClusterShape {
    pub fn expand(
        &self,
    ) -> Result<BTreeMap<LogicalNodeId, LogicalNodeSpec>, ShapeError>;
}
```

Shape expansion validates before reconciliation:

- every group has `run_id == ClusterShape::run_id`;
- group IDs are unique; and
- expanded logical-node IDs are unique.

Separately, the driver requires `run_id` to remain fixed for its lifetime,
rejects a generation lower than the last accepted desired generation, and
rejects changed shape content at the same generation. `expand` does not depend
on driver history.

The map, rather than an unvalidated `Vec`, is the desired set. Expansion still
uses the existing `{group_id}-{index}` identity convention. Scaling up adds
higher indices; scaling down makes higher indices absent first.

For v1, a `LogicalNodeSpec` is an immutable attempt template. Any inequality
between the current attempt's `record.desired` and the latest desired spec—role,
provider, shape, boot, or swarm-join data—requires replacement. In-place node
mutation can be introduced later only with an explicit field policy and
transition contract.

### 5.2 Observed state

`NodeRecord` remains the provider-neutral lifecycle fact record. It gains
`failed_at: Option<SystemTime>`; its existing `desired` field is the immutable
spec snapshot implemented by that concrete attempt.

Cluster-level mechanics wrap, rather than duplicate, the node lifecycle:

```rust
pub struct NodeAttemptId(pub u64);

pub struct OperationId {
    pub attempt: NodeAttemptId,
    pub sequence: u64,
}

pub enum NodeIntent {
    Active,
    Deleting,
}

pub struct PendingOperation {
    pub id: OperationId,
    pub kind: OperationKind,
    /// Executor timeout sampled and stored before dispatch.
    pub deadline: SystemTime,
}

pub struct RetryState {
    pub consecutive_failures: u32,
    /// Earliest time another external effect may be dispatched.
    pub next_effect_at: Option<SystemTime>,
    /// Earliest time a destroyed failed attempt may be replaced.
    pub restart_at: Option<SystemTime>,
    pub last_error: Option<String>,
    /// Timed-out create/start operation that must be adopted before cleanup.
    pub ambiguous_operation: Option<OperationKind>,
}

pub struct ManagedNode {
    pub attempt: NodeAttemptId,
    pub intent: NodeIntent,
    pub record: NodeRecord,
    /// Currently addressable bootstrap session; facts may outlive this handle.
    pub active_bootstrap: Option<BootstrapSessionId>,
    pub pending: Option<PendingOperation>,
    pub next_operation_sequence: u64,
    pub retry: RetryState,
}

pub struct ClusterState {
    /// Latest desired generation evaluated by a completed pass.
    pub observed_generation: u64,
    /// Cluster-wide monotonic allocator; attempt IDs are never reused.
    pub next_attempt_id: u64,
    pub nodes: BTreeMap<LogicalNodeId, ManagedNode>,
}
```

`LogicalNodeId` identifies the stable slot. `NodeAttemptId` is allocated from
`ClusterState::next_attempt_id`, is unique within the run, and is never reused
after reaping a slot. `OperationId` combines that attempt with a monotonically
increasing per-attempt sequence.

A `NodeRecord` is replaced only when a new attempt starts. Old records may be
emitted to observability before replacement, but they are not simultaneously
live under the same map key.

`BeginDelete` immediately makes the record non-ready but preserves all live
resource facts and any pending operation so its eventual result can still be
folded. `Restart` requires `Destroyed`, installs a fresh globally allocated
attempt and `NodeRecord` from the latest desired spec, clears
pending/live-session state and deadlines, resets the per-attempt operation
sequence, and preserves the consecutive-failure count until the new attempt
becomes ready. `Reap` requires `Destroyed`.

## 6. Reconciler contract

The plan contains driver transitions as well as external effects. This is
necessary because a pure decider cannot itself mark deletion, install a fresh
attempt, or record an operation as pending.

```rust
pub struct ReconcilePlan {
    /// At most one action per logical node, sorted by LogicalNodeId.
    pub actions: Vec<NodeAction>,
    /// The desired generation evaluated by this plan.
    pub observed_generation: u64,
    /// Earliest known deadline requiring another pass without an event.
    pub requeue_at: Option<SystemTime>,
}

pub enum NodeAction {
    Insert {
        attempt: NodeAttemptId,
        desired: LogicalNodeSpec,
    },
    BeginDelete {
        node: LogicalNodeId,
        expected_attempt: NodeAttemptId,
    },
    MarkDestroyed {
        node: LogicalNodeId,
        expected_attempt: NodeAttemptId,
    },
    Restart {
        node: LogicalNodeId,
        expected_attempt: NodeAttemptId,
        new_attempt: NodeAttemptId,
        desired: LogicalNodeSpec,
    },
    Reap {
        node: LogicalNodeId,
        expected_attempt: NodeAttemptId,
    },
    Dispatch(PlannedEffect),
}

pub struct PlannedEffect {
    pub node: LogicalNodeId,
    pub operation: OperationId,
    pub command: NodeManagerCommand,
}

/// Pure and deterministic for identical inputs, including `now`.
pub fn reconcile(
    observed: &ClusterState,
    desired: &ClusterShape,
    now: SystemTime,
) -> Result<ReconcilePlan, ShapeError>;
```

Every action carries enough identity or precondition to be safe if the driver
has changed since the snapshot. A stale action is discarded and the cluster is
marked dirty; it is never applied to a different attempt.

While constructing the sorted plan, `reconcile` assigns distinct sequential
attempt IDs from the snapshotted `next_attempt_id`. The driver applies
`Insert`/`Restart` only when each assigned ID equals the current allocator, then
advances it with checked arithmetic. An allocator mismatch invalidates that and
all later allocated-attempt actions in the plan and marks the cluster dirty.

A pass chooses at most one action per logical node. Different nodes can advance
in the same pass. A driver transition such as `Insert`, `BeginDelete`,
`MarkDestroyed`, `Restart`, or `Reap` completes that node's step for the pass;
its resulting external effect is considered only in a later pass.

Before submitting `Dispatch`, the driver atomically:

1. verifies the attempt, operation sequence, and absence of another pending
   operation;
2. samples and stores the executor deadline in `PendingOperation`, schedules
   that deadline, advances `next_operation_sequence`, and applies any
   command-requested status such as `LeaseRequested`; and
3. submits the effect to the executor.

If submission itself fails, the driver folds that as an operation failure. No
pass can observe an unrecorded in-flight effect.

## 7. Observation folding and per-node progression

Observation folding and effect selection are separate operations:

```rust
pub fn observe(
    node: &mut ManagedNode,
    observation: NodeObservation,
    now: SystemTime,
    retry: &RetryPolicy,
);

pub fn reconcile_node(
    node: &ManagedNode,
    desired: Option<&LogicalNodeSpec>,
    now: SystemTime,
) -> NodeDecision;
```

`observe` mutates facts and emits no command. `reconcile_node` reads facts and
returns no more than one action. Time and retry policy enter mutation only
through the driver-provided arguments; neither function reads a global clock or
random source.

Executor results and asynchronous observations carry `LogicalNodeId`,
`NodeAttemptId`, and, for command results, `OperationId`. Results for a stale
attempt or non-current operation are ignored after observability is recorded.

### 7.1 Effect-result folding

- `CreateLease` success stores `LeaseFacts`; an included endpoint also stores
  `connection` and yields `EndpointKnown`, otherwise the stage is
  `LeaseCreated`.
- `LookupEndpoint` with an endpoint stores it and yields `EndpointKnown`.
  "Not available yet" retains the lease and stores a future probe deadline; it
  is not an attempt-ending failure.
- `StartBootstrap` success returns a `BootstrapSessionId`, stores it as
  `active_bootstrap`, stores `BootstrapFacts`, and yields `BootstrapRunning`.
- Bootstrap observations update stage and sequence facts only.
- A swactor-join observation stores `SwactorFacts` and yields
  `SwactorJoined`; it does not itself emit convergence commands.
- Bootstrap convergence/closure clears `active_bootstrap`, marks handoff
  complete, and yields the existing ready `Dormant` state.
- Bootstrap cancellation clears `active_bootstrap` while retaining terminal
  bootstrap facts for observability.
- Lease destruction clears the live lease facts. A later `MarkDestroyed`
  transition yields `Destroyed` and records `destroyed_at`.

A successful command result clears the matching pending operation before the
next decision. An operation failure also clears it, records retry state, and
follows §11.

### 7.2 Level-to-effect table

`pending.is_some()` always means wait for its result or stored executor
deadline. Every dispatch row below also requires `next_effect_at` to be absent
or due; otherwise the node waits and contributes that deadline to
`requeue_at`. With no pending operation, progression is:

| intent / observed facts | next action |
|---|---|
| active, `New` or `LeaseRequested`, no lease, retry due | `CreateLease` |
| active, lease known, no connection, probe due | `LookupEndpoint` |
| active, connection known, no bootstrap session | `StartBootstrap` |
| active, `BootstrapRunning`, no swactor | none; await observation |
| active, `SwactorJoined`, active bootstrap | `BootstrapConvergenceObserved` |
| active, `HandedOff` / `Dormant`, ready | none; steady state |
| active, attempt-ending `Failed` | `BeginDelete` |
| deleting, active bootstrap | `CancelBootstrap` |
| deleting, no active bootstrap, lease present | `DestroyLease` |
| deleting, no active bootstrap or lease | `MarkDestroyed` |
| `Destroyed`, desired present, restart deadline due | `Restart` with latest desired spec |
| `Destroyed`, desired absent | `Reap` |

Cleanup ordering is deliberately sequential: cancel bootstrap, then destroy the
lease, then mark/reap or restart. The earlier draft's simultaneous cancel and
destroy effects violated one-step progression and made partial success
ambiguous.

## 8. Topology and replacement

Top-level reconciliation compares the validated desired map with observed nodes:

- **desired only** — `Insert` a `ManagedNode` with the next globally allocated
  attempt ID, `Active` intent, and a fresh `NodeRecord`. A later pass emits
  `CreateLease`.
- **observed only** — if active, `BeginDelete`; if already deleting, continue
  cleanup; if destroyed, `Reap`.
- **both, same spec** — run per-node progression.
- **both, different spec** — if active, `BeginDelete`. Once the old attempt is
  destroyed and any restart deadline has elapsed, `Restart` installs the latest
  desired spec under the next globally allocated attempt ID.

Replacement never places one logical ID in simultaneous start and destroy
lists. Once `Deleting` begins it is not cancelled, even if the old spec becomes
desired again; cleanup finishes and the latest desired spec starts as a new
attempt. This is the process-local equivalent of a Kubernetes object name having
successive concrete UIDs.

Scale-down order follows identity expansion: higher indices become absent
first. Multiple independent topology actions may occur in one pass. v1 defines
no availability budget across replacements; adding one is a group-policy
extension over this per-node lifecycle.

## 9. Driver and workqueue semantics

v1 uses one cluster reconcile key and one non-reentrant driver. Triggers come
from:

- desired-shape generation changes;
- executor results and bootstrap/swactor observations;
- stored retry or probe deadlines; and
- a periodic safety tick.

The driver maintains queued, processing, and dirty state equivalent to a
single-key Kubernetes workqueue:

- adding an already queued key is a no-op;
- adding the key while it is processing marks it dirty; and
- completing a dirty pass immediately queues one further pass.

Each pass:

1. snapshots `ClusterState` and the latest `ClusterShape`;
2. calls `reconcile`;
3. applies each still-valid driver transition or records-and-submits each
   `Dispatch` without waiting for provider I/O, scheduling every newly stored
   pending-operation deadline;
4. records `plan.observed_generation` after the pass has evaluated that shape;
5. schedules `plan.requeue_at`, if any; and
6. immediately runs again if marked dirty while processing.

Applying `Insert`, `BeginDelete`, `MarkDestroyed`, or `Restart` marks the cluster
dirty so its next lifecycle step cannot depend on an external event. `Reap`
needs no follow-up unless another trigger is already pending. `Dispatch` waits
for its correlated result or stored deadline.

Observations are folded by serialized driver transitions before they can affect
a later snapshot. A periodic tick is a safety net, not the primary progress
mechanism.

The cluster is converged for a generation when every desired node has the same
spec snapshot, is ready, has active intent, and has no pending operation; no
undesired or deleting nodes remain. `observed_generation == generation` alone
does not mean converged.

## 10. Effect identity and idempotency

A deterministic plan is not by itself an idempotent side effect. Safety comes
from observed facts, pending-operation state, and executor behavior.

- The driver records an operation before dispatch and never emits a second
  operation for that node while one is pending.
- The executor deduplicates repeated submissions of the same `OperationId`
  within the driver process and returns the recorded outcome when known.
- The executor permits at most one live lease and one live bootstrap session
  for `(run_id, logical_node_id, attempt)`, including across retries with newer
  operation IDs.
- Provider creates use a deterministic external label or request token derived
  from `(run_id, logical_node_id, attempt)`. After an ambiguous create outcome,
  the executor looks up and adopts that attempt before creating again.
- Destroy and cancel treat "already absent" as success.
- Completion observations are correlated to both attempt and operation. Late
  observations cannot mutate a replacement attempt.
- An executor timeout may clear pending state only after the executor has
  stopped the operation or classified its outcome as ambiguous. Retrying an
  ambiguous create or bootstrap start performs lookup/adoption first; it never
  runs a concurrent blind duplicate.

Because persistence is out of scope, v1 does not claim recovery from a process
crash between an external side effect and its in-memory observation. The stable
identities and adoption rule are the required shape for adding that guarantee
later.

## 11. Failure and backoff

Failure is observed state, but not every failed external call destroys the
whole attempt. The driver classifies by operation:

| failure | retained state and retry behavior |
|---|---|
| lease creation | retain no lease; retry `CreateLease` after backoff |
| endpoint lookup / endpoint not ready | retain lease; retry lookup after backoff / probe interval |
| bootstrap start, bootstrap runtime, or join | mark attempt failed and begin cleanup immediately; delay only the later restart |
| bootstrap cancel | remain deleting; retry cancel after backoff |
| lease destroy | remain deleting with lease facts; retry destroy after backoff |

Attempt-ending failure records `failed_reason` and `failed_at` and stores
`restart_at`; the next decision returns `BeginDelete`. It never resets a record
with live resources directly to `New`. Cleanup begins on the following pass and
is not delayed by the restart backoff. Once cleanup reaches `Destroyed`, the
node waits until `restart_at`, then starts a fresh attempt if it is still
desired.

`RetryPolicy` is driver configuration: initial delay, cap, jitter, executor
operation timeout, and endpoint probe interval. When an observation is folded,
the driver computes the next deadline once and stores it in `RetryState`.
Random jitter is therefore not sampled by `reconcile`, preserving determinism.
Success clears `next_effect_at`; reaching ready steady state resets consecutive
failure count.

Backoff is per node. A node waiting for a deadline contributes `requeue_at` but
does not prevent actions for other nodes. Provider work is dispatched outside
the pass, so slow or failed I/O for one node cannot block reconciliation of
another.

## 12. Relationship to existing code

| existing abstraction | role under this spec |
|---|---|
| `NodeRecord` / `NodeStage` | provider-neutral lifecycle facts retained inside `ManagedNode` |
| `NodeManagerCommand` | effect payload retained inside identity-bearing `PlannedEffect` |
| `RunNodeGroupSpec` / `expand_node_group` / `LogicalNodeSpec` | desired templates validated and wrapped by `ClusterShape` |
| provider and provisioning plugins | executor implementations behind effect dispatch and result correlation |
| `apps/myelin` orchestration | driver integration, executor wiring, and observation routing |

`NodeManager::handle(msg) -> Vec<NodeManagerCommand>` currently couples
observation mutation and effect selection. That shape cannot serve as the
observation-only half of this contract: some handlers advance state past the
point where a later level decision would emit the returned command.

Implementation therefore performs a clean split:

1. move the authoritative record into `ManagedNode`;
2. extract observation-only mutation into `observe`;
3. extract pure level decisions into `reconcile_node`; and
4. retire `handle` after callers migrate.

This is one lifecycle expressed as a reducer plus a decider, not parallel edge
and level state machines.

## 13. Kubernetes references (non-normative)

The mechanics in §4 are adapted from:

- [Kubernetes API conventions](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md), especially spec/status, generation, level-based behavior, and operation sequencing;
- [controller-runtime's reconcile contract](https://github.com/kubernetes-sigs/controller-runtime/blob/main/pkg/reconcile/reconcile.go), especially key-only requests and requeue semantics;
- [client-go workqueue](https://github.com/kubernetes/client-go/blob/master/util/workqueue/queue.go), especially dirty-key coalescing and per-key serialization;
- [Kubernetes finalizers](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/), especially durable cleanup-before-delete; and
- [Cluster API's InfraMachine contract](https://cluster-api.sigs.k8s.io/developer/providers/contracts/infra-machine), the closest analogue for provider-backed machine lifecycle.

The cited systems persist controller objects in an API server. This spec adopts
their state-machine mechanics over an in-process Swactor/Myelin substrate; it
does not import their storage or network architecture.

## 14. Invariants (normative)

1. **Purity.** `reconcile` and `reconcile_node` perform no I/O, read no global
   state, and mutate no input.
2. **Level-based decisions.** Effect selection depends on the latest desired and
   observed state, never on the triggering event.
3. **Latest desired wins.** Intermediate desired generations need not be
   visited.
4. **Single observed-state owner.** Only the driver mutates `ClusterState`.
5. **Observation/decision separation.** Observation folding emits no effects.
6. **One action per node per pass.** Convergence occurs across passes.
7. **Record before dispatch.** Every external effect has a pending operation in
   observed state before execution begins.
8. **Attempt correlation.** An observation from an old attempt cannot mutate a
   newer attempt.
9. **Non-reentrant, coalesced driving.** One cluster pass runs at a time; a
   trigger during a pass guarantees a later pass without concurrent mutation.
10. **Cleanup before reuse.** A logical slot is not restarted or reaped until
    bootstrap and lease cleanup for its old attempt is observed complete.
11. **Distinct identities.** Logical-node, node-attempt, and operation identity
    are not interchangeable.
12. **Per-node failure isolation.** Retry or I/O for one node does not prevent
    progress for another.
13. **No waiting in reconciliation.** Provider I/O and timers live in executor
    or driver/engine work, never inside a reconcile pass.
14. **Observed generation is acknowledgment, not readiness.** Convergence is
    determined from node state and pending topology work.
15. **Determinism.** Identical `(observed, desired, now)` inputs produce the same
    plan; sampled retry deadlines are stored before reconciliation reads them.
