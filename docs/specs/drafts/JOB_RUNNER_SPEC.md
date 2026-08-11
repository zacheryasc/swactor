# Synaptic Job Runner Specification

Id: 4
Last modified: b887e941cbe6f1e209339abd0375507aca9bfe52
Last reviewed:
> Any edit to this spec must update `Last modified` above to the current `git HEAD` commit.

---

## 1. Purpose

Run an arbitrary command job on a rented GPU node, end to end, through swactor:
provision a node → push the workspace → setup → run → collect outputs →
teardown. swactor is the substrate: provisioning, the node runtime, the data
plane, and observability are reused; the job runner is built on top of them.

v1 is the single-node command job: one job → one node → one attempt. Multi-node,
sharded, and pipeline jobs are future work-descriptions over the same core.

---

## 2. What swactor grows, what swactor reuses

**New — the job runner's content:**

- a node primitive that runs an arbitrary command as a supervised process and
  reports exit via datastream;
- a bulk-transfer service on the shared iroh endpoint for workspace push
  (operator → node) and output pull (node → operator) — a new service
  paralleling the existing edge and datastream ALPN services, not the
  arena/tensor data plane;
- a job FSM and the orchestrator↔node protocol that sequence the above.

**Reused unchanged:** the VastAI provider adapter (provision/teardown), SWIM
membership, the datastream event infrastructure, the shared iroh endpoint (which
already multiplexes the edge and datastream services), and the swactor actor
runtime.

---

## 3. Design principle: job = work, gpu-agnostic

A job describes *what to do*, not what hardware to run on — no provider, no GPU,
no node. Hardware (provider, GPU type/count/VRAM, disk, image, selection) lives
in a *pool designation* owned by the provider adapter. This keeps a future
sharded-model job the same shape against different placement; sharding is not a
v1 feature.

---

## 4. Job description

| field       | required | meaning                                              |
|-------------|----------|------------------------------------------------------|
| `name`      | yes      | job identity                                         |
| `run`       | yes      | the command that does the work                       |
| `setup`     | no       | one-time command run before `run` (env install)      |
| `workspace` | no       | `{ workdir, exclude }` — pushed to the node first    |
| `outputs`   | no       | paths to collect back after run                      |
| `env`       | no       | environment variables injected into setup and run    |

Nothing else. No resources, no provider, no GPU, no cleanup policy.

```toml
[job]
name = "airfrans_smoke_01"
setup = "uv sync --no-dev"
run = "uv run remote-run smoke-train configs/aggressive_smoke.toml"
workspace = { workdir = ".", exclude = ["/artifacts", "/.venv", "__pycache__"] }
outputs = ["metrics.jsonl", "final_metrics.json", "checkpoint_latest.pt"]
env = { HF_TOKEN = "..." }
```

A job is submitted with a *pool designation* (capacity) that drives provisioning
via the existing VastAI adapter. The pool is not part of the job and is not
specified further here.

---

## 5. Roles

- **Orchestrator (job authority):** places the job, drives the lifecycle, owns
  the terminal outcome, orders teardown.
- **Node (executor):** runs `setup` and `run` as supervised processes,
  materializes the workspace, exposes outputs, reports lifecycle via datastream.
- **Provider adapter:** provisions and tears down nodes per the pool designation.

---

## 6. Lifecycle

States: `PENDING → RUNNING → COMPLETED | FAILED`.

| event observed                    | command emitted                          | transition    |
|-----------------------------------|------------------------------------------|---------------|
| `JobSubmitted{job, pool}`         | `ProvisionNode`                          | → `PENDING`   |
| `NodeReady`                       | `MaterializeWorkspace`                   | → `RUNNING`   |
| `WorkspaceMaterialized`           | `RunSetup`                               |               |
| `SetupCompleted`                  | `RunJob`                                 |               |
| `JobExited{0}`                    | `CollectOutputs`                         |               |
| `OutputsCollected`                | `TeardownNode`                           | → `COMPLETED` |
| `JobExited{non-zero}`             | `CollectOutputs` (best-effort), `TeardownNode` | → `FAILED` |
| `NodeFault` / `NodeLost` / `OperatorStop` | `TeardownNode`                   | → `FAILED`    |

- A job waits for *its one node's* readiness, not pool-wide convergence.
- No `setup` → `WorkspaceMaterialized` goes straight to `RunJob`. No
  `workspace` → materialization is skipped.
- The exit code is authoritative: `0` → `COMPLETED`, non-zero → `FAILED`. Declared
  outputs are collected either way; collection on failure is best-effort.
- The node is torn down on every terminal state (completed or failed).
- Every transition emits a datastream event.

`OperatorStop` is the operator kill switch for a running job.

---

## 7. Behavior contracts

The orchestrator commands these contracts; the node owns the local mechanism.
Control and lifecycle travel as orchestrator↔node messages and datastream
events. Bulk workspace and output bytes travel over a dedicated transfer
service on the shared iroh endpoint, not in actor messages.

**Supervised command execution (node).** On `RunSetup` / `RunJob`, the node
spawns the command in `workdir` with the declared `env`, supervises it, and
reports `SetupCompleted` / `JobExited{code}` (or `NodeFault`) via datastream. A
failed setup is a failed job.

**Workspace materialization (operator → node).** On `MaterializeWorkspace`, the
`workdir` tree (with `exclude` applied) is pushed from the operator to the node
over the iroh transfer service, landing at the node's `workdir`. The node reports
`WorkspaceMaterialized`.

**Output collection (node → operator).** On `CollectOutputs`, declared `outputs`
are pulled from the node to a per-run operator landing directory over the iroh
transfer service. The node reports `OutputsCollected`. Missing outputs do not change
the outcome — the exit code already decided it; whatever exists is gathered.

---

## 8. Relationship to the broader work model

This spec defines only the job plugin. The intended generalization is a *work
description*: a graph of work-units with typed ports, placed by a planner. The v1
job resolves to a single work-unit with no edges; multi-unit, sharded, and
pipeline jobs are future work-descriptions over the same core. The gpu-agnostic
job shape is what keeps that path open.
