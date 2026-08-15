# Synaptic Job Runner Specification

Id: 4
Last modified: b8aff00dc1628665e24fe9628a6f12603df368ad
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
  reports exit via orchestrator↔node messages;
- a bulk-transfer service on the shared iroh endpoint for workspace push
  (operator → node) and output pull (node → operator) — a new service
  paralleling the existing edge and telemetry ALPN services, not the
  arena/tensor data plane;
- a job FSM and the orchestrator↔node protocol that sequence the above.

**Reused unchanged:** the VastAI provider adapter (provision/teardown), SWIM
membership, the telemetry event infrastructure, the shared iroh endpoint (which
already multiplexes the edge and telemetry services), and the swactor actor
runtime.

---

## 3. Design principle: job = work, gpu-agnostic

A job describes *what to do*, not what hardware to run on — no provider, no GPU,
no node. Hardware (provider, GPU type/count/VRAM, disk, image, selection) lives
in a separate *cluster config* that is bootstrapped before any job is submitted.
This keeps a future sharded-model job the same shape against different placement;
sharding is not a v1 feature.

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

**Outputs-root convention.** Every job writes its artifacts under one fixed,
node-known outputs root (conventionally the job's `workdir`), and `outputs`
lists paths *relative to that root*. The node resolves each declared output
against the root; collection pulls whatever of them exists. This avoids the
fragility of globbing timestamped run directories: the job decides where under
the root it writes, and the runner never has to discover a fresh, unpredictable
output path. Paths outside the root are not collected.

A job is submitted alone — no hardware, no pool. It runs on a ready node from a
separately bootstrapped cluster (§3).
---

## 5. Roles

- **Orchestrator (job authority):** places the job on a ready node, drives the
  lifecycle, owns the terminal outcome.
- **Node (executor):** runs `setup` and `run` as supervised processes,
  materializes the workspace, exposes outputs, reports lifecycle via orchestrator↔node messages.

Node existence — provisioning and teardown — is a separate cluster-config concern
driven by the reconciler, not part of the job runner. A job assumes at least one
ready node; it neither provisions nor tears down nodes.

---

## 6. Lifecycle

States: `PENDING → RUNNING → COMPLETED | FAILED`.

Ownership: the job runner does not own node existence — that is the separate
cluster-config/reconciler concern. The orchestrator drives only what executes on
an already-ready node. The telemetry is observability-only — metrics and
logging; lifecycle events are mirrored to it but it is never a control channel.

Command routing — what the orchestrator emits, and where it goes:

| command                | target | kind                        |
|------------------------|--------|-----------------------------|
| `MaterializeWorkspace` | node   | orchestrator↔node message   |
| `RunSetup`             | node   | orchestrator↔node message   |
| `RunJob`               | node   | orchestrator↔node message   |
| `CollectOutputs`       | node   | orchestrator↔node message   |

Event sourcing — what the orchestrator observes, and where it comes from:

| event                   | source                          | channel                     |
|-------------------------|---------------------------------|-----------------------------|
| `NodeReady`             | reconciler — node reached ready | reconciler observed state  |
| `WorkspaceMaterialized` | node                            | orchestrator↔node message |
| `SetupCompleted`        | node                            | orchestrator↔node message |
| `JobExited{code}`       | node                            | orchestrator↔node message |
| `OutputsCollected`      | node                            | orchestrator↔node message |
| `NodeFault`             | node — supervised process       | orchestrator↔node message |
| `NodeLost`              | SWIM membership                 | membership signal         |
| `OperatorStop`          | operator                        | orchestrator API          |

Transition table — events and commands are sourced and routed as above:

| event observed                    | command emitted                          | transition    |
|-----------------------------------|------------------------------------------|---------------|
| `JobSubmitted{job}`               | —                                        | → `PENDING`   |
| `NodeReady`                       | `MaterializeWorkspace`                   | → `RUNNING`   |
| `WorkspaceMaterialized`           | `RunSetup`                               |               |
| `SetupCompleted`                  | `RunJob`                                 |               |
| `JobExited{0}`                    | `CollectOutputs`                         |               |
| `OutputsCollected`                | —                                        | → `COMPLETED` |
| `JobExited{non-zero}`             | `CollectOutputs` (best-effort)           | → `FAILED`    |
| `NodeFault` / `NodeLost` / `OperatorStop` | —                                | → `FAILED`    |

- A job claims one ready node from the bootstrapped cluster and waits for that
  node's readiness, not cluster-wide convergence.
- No `setup` → `WorkspaceMaterialized` goes straight to `RunJob`. No
  `workspace` → materialization is skipped.
- The exit code is authoritative: `0` → `COMPLETED`, non-zero → `FAILED`. Declared
  outputs are collected either way; collection on failure is best-effort.
- The job does not tear down its node; cluster teardown is a separate
  operator/reconciler concern.
- Every transition emits a telemetry event.

`OperatorStop` is the operator kill switch for a running job.

---

## 7. Behavior contracts

The orchestrator commands these contracts; the node owns the local mechanism.
Control and lifecycle travel as orchestrator↔node messages; the telemetry
mirrors them for observability (metrics and logging) and is never a control
channel. Bulk workspace and output bytes travel over a dedicated transfer
service on the shared iroh endpoint, not in actor messages.

**Supervised command execution (node).** On `RunSetup` / `RunJob`, the node
spawns the command in `workdir` with the declared `env`, supervises it, and
reports `SetupCompleted` / `JobExited{code}` (or `NodeFault`) via
orchestrator↔node messages. A failed setup is a failed job.

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
