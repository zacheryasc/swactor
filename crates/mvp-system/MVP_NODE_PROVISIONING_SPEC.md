# MVP Node Provisioning Specification

**Status:** draft node-provisioning specification.

This document defines the MVP path from a static runplan node requirement to a
remote swactor runtime joined to the orchestrator-side swarm. It covers provider
leasing, SSH bootstrap, stdout/stderr collection, handoff, and known-lease
teardown.

It intentionally does not define general run management, automatic replacement,
provider recovery, or a second post-handoff health system.

---

## 1. Purpose

The MVP needs to rent GPU nodes, bring them to the point where swactor can manage
them, and then stop managing them through SSH.

The intended path is:

```text
static runplan
  -> logical node specs
  -> one NodeManager actor per logical node
  -> provider plugin creates a lease
  -> BootstrapSession holds SSH until swactor convergence
  -> stdout/stderr flows into datastream
  -> remote swactor joins
  -> BootstrapSession closes SSH and exits
  -> NodeManager becomes dormant and keeps lease state for teardown
```

The actor system owns state transitions. Provider plugins perform provider I/O.
SSH bootstrap is a temporary pre-swactor transport, not long-term node
management.

---

## 2. Scope

In scope:

- static runplan node-group shape
- expansion of node groups into logical node specs
- one `NodeManager` actor per logical node
- node-local inventory/ledger owned by `NodeManager`
- readiness as a `NodeManager` state flag
- stateless provider plugin boundary for Vast.ai
- transient `BootstrapSession` for SSH, remote boot observation, and swactor
  startup
- stdout/stderr forwarding from bootstrap SSH to datastream
- handoff from SSH bootstrap to swactor control
- teardown of leases already known to `NodeManager`

Out of scope:

- automatic replacement after lease, bootstrap, or runtime failure
- provider-label recovery or hidden provider scans
- post-handoff heartbeat layer outside swactor
- bidding/account/billing policy beyond selecting and destroying leases
- repairing a node after it disappears from swactor

---

## 3. Design Commitments

`NodeManager` owns one node. It owns both the node finite-state machine and that
node's inventory record.

There is no central actor that owns the run. A short-lived bootstrap procedure may
expand a runplan and spawn node actors, but it does not retain authority over
node state.

Readiness is node-local. A node is ready only when its `NodeManager` has recorded
successful swactor handoff.

The provider plugin is stateless with respect to the run. It maps desired node
shape to provider API calls and maps known lease handles to destroy calls.

`BootstrapSession` owns SSH and early stdout/stderr. It exists only between
provider endpoint availability and swactor convergence.

After swactor convergence, swactor is the live control path. The MVP does not add
another liveness or heartbeat system.

Known lease state is explicit. Teardown uses only lease handles already recorded
by `NodeManager`.

---

## 4. Identifiers

Identifier types are schematic. Concrete Rust APIs may wrap these as newtypes.

```rust
struct RunId(u64);
struct LogicalNodeId(String);      // e.g. "workers-0"
struct NodeGroupId(String);        // e.g. "workers"
struct RoleId(String);             // e.g. "worker"
struct ProviderLeaseId(String);    // e.g. "vastai:123456"
struct SwactorId(String);
struct BootstrapSessionId(u64);
struct DatastreamStreamId(String);
```

`LogicalNodeId` is stable for the run. It is assigned before provisioning and is
used to correlate provider lease, SSH bootstrap logs, and swactor identity.

`ProviderLeaseId` names the external billing/lease resource. For Vast.ai it wraps
the contract id.

`SwactorId` is not known until the remote runtime joins.

---

## 5. Static Runplan Node Shape

The runplan describes desired node groups. It does not describe provider API
steps or SSH polling details.

```rust
struct RunNodeGroupSpec {
    run_id: RunId,
    group_id: NodeGroupId,
    role: RoleId,
    count: u32,
    provider: ProviderKind,
    shape: DesiredNodeShape,
    boot: BootSpec,
    swarm_join: SwarmJoinSpec,
}
```

Provider-neutral desired shape:

```rust
struct DesiredNodeShape {
    image: String,
    disk_gb: u32,
    gpu_name: Option<String>,
    min_gpu_ram_mb: Option<u64>,
    min_down_mbps: Option<f64>,
    min_up_mbps: Option<f64>,
    min_reliability: Option<f64>,
    require_verified: bool,
    provider_labels: BTreeMap<String, String>,
}
```

Remote boot specification:

```rust
struct BootSpec {
    ssh_user: String,
    verify_commands: Vec<String>,
    start_swactor_command: String,
    stdout_sources: Vec<String>,
    stderr_sources: Vec<String>,
    timeout_policy: BootstrapTimeoutPolicy,
}
```

Swarm join material:

```rust
struct SwarmJoinSpec {
    orch_swactor_addr: String,
    join_token_ref: String,
    expected_logical_node_id: LogicalNodeId,
}
```

A bootstrap procedure expands each group into logical specs:

```text
workers count=3
  -> workers-0
  -> workers-1
  -> workers-2
```

Each expanded logical spec starts one `NodeManager` actor.

---

## 6. Runtime Topology


The orchestrator host runs the swactor actor runtime and a datastream producer.

```text
orchestrator host
  Swactor actor runtime
    NodeManager(workers-0)
      BootstrapSession(workers-0) while pre-handoff
    NodeManager(workers-1)
      BootstrapSession(workers-1) while pre-handoff

    Orchestrator control endpoint
      receives remote runtime joins
      owns post-handoff actor communication

  Provider plugins
    VastAiPlugin, called by NodeManager

  Datastream
    receives bootstrap stdout/stderr records
```

---

## 7. NodeManager Actor

### 7.1 Responsibility

`NodeManager` owns one logical node's state and lifecycle.

It:

- stores desired node spec
- requests a provider lease
- records lease facts
- waits for provider endpoint facts when needed
- starts a `BootstrapSession`
- records compact bootstrap observations
- records swactor identity on join
- sets `ready = true` after handoff
- keeps known lease state while dormant
- releases its known lease on `Destroy`

It does not:

- aggregate run readiness
- replace failed nodes
- poll post-handoff liveness
- own provider search state after a plugin call returns
- store full stdout/stderr logs

### 7.2 Node Record

```rust
struct NodeRecord {
    logical_node_id: LogicalNodeId,
    run_id: RunId,
    group_id: NodeGroupId,
    role: RoleId,

    desired: LogicalNodeSpec,
    stage: NodeStage,
    ready: bool,

    lease: Option<LeaseFacts>,
    connection: Option<SshEndpoint>,
    bootstrap: Option<BootstrapFacts>,
    swactor: Option<SwactorFacts>,

    failed_reason: Option<String>,
    destroyed_at: Option<SystemTime>,
}
```

Provider lease facts:

```rust
struct LeaseFacts {
    provider: ProviderKind,
    lease_id: ProviderLeaseId,
    provider_contract_id: String,
    offer_id: Option<String>,
    destroy_handle: DestroyHandle,
    provider_metadata: BTreeMap<String, String>,
}
```

SSH endpoint:

```rust
struct SshEndpoint {
    host: String,
    port: u16,
    user: String,
    auth_ref: String,
}
```

Bootstrap facts are compact. Full logs belong to datastream.

```rust
struct BootstrapFacts {
    session_id: BootstrapSessionId,
    last_stage: BootstrapStage,
    last_stdout_seq: Option<u64>,
    last_stderr_seq: Option<u64>,
    last_observed_at: SystemTime,
}
```

Swactor facts:

```rust
struct SwactorFacts {
    swactor_id: SwactorId,
    joined_at: SystemTime,
    handed_off_at: Option<SystemTime>,
}
```

### 7.3 Node Stages

```text
New
  -> LeaseRequested
  -> LeaseCreated
  -> EndpointKnown
  -> BootstrapRunning
  -> SwactorJoined
  -> HandedOff
  -> Dormant
```

Terminal stages:

```text
Failed
Destroyed
```

`ready = true` only after handoff has completed. `Dormant` means the actor keeps
state for query and teardown but performs no polling, heartbeating, or repair.

### 7.4 Inbound Messages

Messages are logical actor signals. Some implementations may deliver provider
results as awaited futures and then enqueue the equivalent event to the actor FSM.

```rust
enum NodeManagerMsg {
    Start(LogicalNodeSpec),

    LeaseCreated(LeaseFacts, Option<SshEndpoint>),
    LeaseFailed(String),
    EndpointKnown(SshEndpoint),
    EndpointFailed(String),

    BootstrapObserved(BootstrapObservation),
    BootstrapFailed(String),
    BootstrapClosed,

    SwactorJoined { swactor_id: SwactorId },
    HandoffComplete { swactor_id: SwactorId },

    Destroy,
    GetStatus { reply_to: ActorAddress },
    GetRecord { reply_to: ActorAddress },
}
```

### 7.5 Outbound Effects

`NodeManager` may perform these effects:

```text
ProviderPlugin.create_lease(shape)
ProviderPlugin.lookup_endpoint(lease)
spawn BootstrapSession(spec)
BootstrapSession.ConvergenceObserved(swactor_id)
ProviderPlugin.destroy_lease(destroy_handle)
reply with node status or record
```

It does not send full logs. `BootstrapSession` writes logs directly to
datastream.

---

## 8. NodeManager FSM Behavior

### 8.1 Start

On `Start(spec)`:

```text
record.desired = spec
record.stage = New
record.ready = false
record.failed_reason = None
```

Then:

```text
record.stage = LeaseRequested
call ProviderPlugin.create_lease(spec.shape)
```

### 8.2 Lease Result

On `LeaseCreated(lease, endpoint)`:

```text
record.lease = lease
record.stage = LeaseCreated
```

If `endpoint` is present:

```text
record.connection = endpoint
record.stage = EndpointKnown
spawn BootstrapSession
record.stage = BootstrapRunning
```

If `endpoint` is absent:

```text
call ProviderPlugin.lookup_endpoint(lease) until endpoint timeout or success
```

On `LeaseFailed(reason)`:

```text
record.stage = Failed
record.ready = false
record.failed_reason = reason
```

### 8.3 Endpoint Result

On `EndpointKnown(endpoint)`:

```text
record.connection = endpoint
record.stage = EndpointKnown
spawn BootstrapSession
record.stage = BootstrapRunning
```

On `EndpointFailed(reason)`:

```text
record.stage = Failed
record.ready = false
record.failed_reason = reason
```

A lease may still exist after endpoint failure. It is destroyed only when the
actor later receives `Destroy`.

### 8.4 Bootstrap Observations

On `BootstrapObserved(obs)`:

```text
record.bootstrap.last_stage = obs.stage
record.bootstrap.last_observed_at = now
record.bootstrap.last_stdout_seq = obs.last_stdout_seq if present
record.bootstrap.last_stderr_seq = obs.last_stderr_seq if present
```

The node stage remains `BootstrapRunning` until swactor join. Optional UI views
may display the finer bootstrap stage from `record.bootstrap.last_stage`.

On `BootstrapFailed(reason)`:

```text
record.stage = Failed
record.ready = false
record.failed_reason = reason
```

### 8.5 Swactor Join And Handoff

On `SwactorJoined { swactor_id }`:

```text
record.swactor.swactor_id = swactor_id
record.swactor.joined_at = now
record.stage = SwactorJoined
send BootstrapSession.ConvergenceObserved(swactor_id)
```

On `BootstrapClosed` after swactor join:

```text
record.swactor.handed_off_at = now
record.stage = HandedOff
record.ready = true
record.stage = Dormant
```

The SSH handle must be closed before `ready` becomes true.

### 8.6 Destroy

On `Destroy`:

```text
if BootstrapSession active:
  cancel BootstrapSession

if lease exists and not destroyed:
  call ProviderPlugin.destroy_lease(lease.destroy_handle)

record.stage = Destroyed on success
record.destroyed_at = now
record.ready = false
```

Destroy uses only the lease stored in `NodeRecord`. There is no provider scan.

---

## 9. Provider Plugin Boundary

Provider plugins are adapters. They are not run supervisors.

```rust
trait ProviderPlugin {
    async fn create_lease(&self, request: CreateLeaseRequest)
        -> Result<CreateLeaseResult, ProviderError>;

    async fn lookup_endpoint(&self, lease: &LeaseFacts)
        -> Result<Option<SshEndpoint>, ProviderError>;

    async fn destroy_lease(&self, handle: &DestroyHandle)
        -> Result<(), ProviderError>;
}
```

`create_lease` may search, filter, rank, and create a provider lease. For Vast.ai
this maps to offer search and instance creation.

`lookup_endpoint` may poll provider APIs until SSH endpoint facts are known. It
must not open SSH or inspect remote boot.

`destroy_lease` destroys a known provider lease.

Provider plugin output must include enough facts for teardown:

```rust
struct CreateLeaseResult {
    lease: LeaseFacts,
    endpoint: Option<SshEndpoint>,
}
```

The plugin must not:

- own `NodeRecord`
- stream stdout/stderr
- start swactor
- infer run readiness
- replace failed nodes
- recover unknown leases by provider label

---

## 10. Vast.ai Plugin Mapping

For Vast.ai, `CreateLeaseRequest` is derived from `DesiredNodeShape`:

```text
gpu_name              -> SelectionPolicy.gpu_name
min_gpu_ram_mb        -> SelectionPolicy.min_gpu_ram_mb
min_down_mbps         -> SelectionPolicy.min_down_mbps
min_up_mbps           -> SelectionPolicy.min_up_mbps
min_reliability       -> SelectionPolicy.min_reliability
require_verified      -> SelectionPolicy.require_verified
image                 -> CreateInstanceRequest.image
disk_gb               -> CreateInstanceRequest.disk_gb
provider labels       -> CreateInstanceRequest.label / env labels as needed
```

The plugin may wait until Vast.ai exposes a usable SSH endpoint. Once that
endpoint is returned, provider provisioning is complete from the plugin's
perspective.

The plugin does not determine whether the remote image booted correctly. That is
`BootstrapSession` work.

---

## 11. BootstrapSession

### 11.1 Responsibility

`BootstrapSession` is a transient child of `NodeManager`.

It owns:

- SSH connection attempts
- SSH handle
- remote bootstrap command handles
- stdout/stderr collection before swactor handoff
- boot verification commands
- swactor start command
- waiting for convergence acknowledgement
- closing SSH after handoff

It exits after either convergence or failure.

### 11.2 Input

```rust
struct BootstrapSessionSpec {
    run_id: RunId,
    logical_node_id: LogicalNodeId,
    lease_id: ProviderLeaseId,
    ssh: SshEndpoint,
    boot: BootSpec,
    swarm_join: SwarmJoinSpec,
    datastream: DatastreamStreamId,
    timeout_policy: BootstrapTimeoutPolicy,
}
```

### 11.3 Stages

```text
Created
  -> SshConnecting
  -> SshReady
  -> StdoutStreaming
  -> BootChecking
  -> SwactorStarting
  -> WaitingForSwactorJoin
  -> Converged
  -> Closed
```

Failure stages:

```text
SshTimeout
BootCheckFailed
StartFailed
JoinTimeout
StreamError
Cancelled
```

### 11.4 Behavior

1. Connect SSH until timeout.
2. Prove the machine is touchable by running a small command and reading output.
3. Start stdout/stderr capture for configured sources.
4. Emit full log records to datastream.
5. Emit compact observations to `NodeManager`.
6. Run boot verification commands.
7. Run or verify the swactor start command with the join spec.
8. Wait for convergence acknowledgement.
9. Flush datastream writes.
10. Close SSH.
11. Notify `NodeManager` with `BootstrapClosed`.

### 11.5 Datastream Records

Bootstrap logs use a stable stream per logical node.

```rust
struct BootstrapLogRecord {
    run_id: RunId,
    logical_node_id: LogicalNodeId,
    lease_id: ProviderLeaseId,
    source: BootstrapLogSource, // ssh-bootstrap
    stream: BootstrapLogStream, // stdout | stderr
    seq: u64,
    timestamp: SystemTime,
    line: String,
}
```

`NodeManager` stores only sequence numbers and the latest compact observation.
It does not retain log bodies.

### 11.6 Convergence Signal

Preferred signal path:

```text
remote swactor runtime -> orchestrator control endpoint -> NodeManager.SwactorJoined(swactor_id)
NodeManager -> BootstrapSession.ConvergenceObserved(swactor_id)
BootstrapSession -> NodeManager.BootstrapClosed
```

This keeps swactor membership authoritative while still letting
`BootstrapSession` close the SSH transport.

---

## 12. Handoff Contract

Handoff is complete only when all are true:

- the remote swactor runtime has joined the orchestrator-side actor runtime
- the actor runtime can address the remote by `SwactorId`
- `NodeManager` has recorded `SwactorFacts`
- bootstrap stdout/stderr records have been flushed
- SSH has been closed
- `NodeManager.ready == true`
- `NodeManager.stage == Dormant`

After handoff:

- `BootstrapSession` is gone
- `NodeManager` does not poll the node
- the swactor actor runtime owns live communication
- the node is considered usable by the MVP run

---

## 13. Failure Behavior

Failures before handoff are terminal for the logical node.

```text
LeaseFailed
EndpointFailed
BootstrapFailed
JoinTimeout
```

Terminal behavior:

```text
record.stage = Failed
record.ready = false
record.failed_reason = reason
```

The MVP does not create a replacement lease.

A failed node with a known lease is still eligible for explicit teardown through
`Destroy`.

Failures after handoff are handled by actor-runtime behavior. `NodeManager` is
dormant and does not repair the node. If the runtime loses the remote node, the
run stalls or fails according to existing swactor behavior.

---

## 14. Teardown Behavior

Teardown targets `NodeManager` actors.

```text
Teardown caller -> NodeManager.Destroy
NodeManager -> BootstrapSession.Cancel if active
NodeManager -> ProviderPlugin.destroy_lease if lease known
NodeManager records Destroyed
```

Rules:

- only known leases are destroyed
- no provider label sweep
- no hidden recovery of missing state
- destroy failure is recorded as node-local failure state

If the process lost all `NodeManager` state, this MVP spec does not define an
automatic cleanup path. Operator/provider-side cleanup remains manual for that
case.

---

## 15. Single End-To-End Worked Example

Input runplan node group:

```text
run_id: 42
group: workers
role: worker
count: 2
provider: vastai
shape:
  image: ghcr.io/acme/mvp-worker:sha123
  disk_gb: 80
  gpu_name: RTX 4090
  min_gpu_ram_mb: 20000
boot:
  ssh_user: root
  verify_commands:
    - test -x /opt/mvp/swactor
  start_swactor_command:
    /opt/mvp/swactor-node --join ${ORCH_ADDR} --node ${LOGICAL_NODE_ID}
swarm_join:
  orch_swactor_addr: quic://orch.example:9443
  join_token_ref: secret://run-42-join-token
```

Expansion:

```text
workers-0
workers-1
```

For `workers-0`:

1. Bootstrap procedure spawns `NodeManager(workers-0)` with its logical spec.
2. `NodeManager` records `New`, then `LeaseRequested`.
3. `NodeManager` calls `VastAiPlugin.create_lease`.
4. `VastAiPlugin` searches offers, creates a Vast.ai instance, and returns:

```text
lease_id: vastai:123
contract_id: 123
offer_id: 9001
ssh: root@203.0.113.10:22001
```

5. `NodeManager` records `LeaseCreated`, `EndpointKnown`, then spawns
   `BootstrapSession(workers-0)` and records `BootstrapRunning`.
6. `BootstrapSession` connects over SSH, runs a probe command, and sends:

```text
BootstrapObserved(stage=SshReady)
```

7. `BootstrapSession` streams bootstrap stdout/stderr into datastream:

```text
run=42 node=workers-0 stream=stdout seq=1 line="container boot entered"
run=42 node=workers-0 stream=stdout seq=2 line="swactor binary found"
```

8. `BootstrapSession` runs `test -x /opt/mvp/swactor`, then runs the swactor
   start command with `LOGICAL_NODE_ID=workers-0` and the run join material.
9. Remote swactor runtime joins the orchestrator-side actor runtime.
10. The orchestrator control endpoint sends:

```text
NodeManager(workers-0).SwactorJoined(swactor_id=swactor-a7)
```

11. `NodeManager` records `SwactorJoined` and tells the bootstrap session that
    convergence was observed.
12. `BootstrapSession` flushes datastream writes, closes SSH, and sends
    `BootstrapClosed`.
13. `NodeManager` records:

```text
stage = Dormant
ready = true
swactor_id = swactor-a7
lease_id = vastai:123
```

The same sequence runs independently for `workers-1`.

The run bootstrap caller can determine node readiness by querying both
`NodeManager` actors:

```text
workers-0.ready == true
workers-1.ready == true
```

At run completion, teardown sends `Destroy` to both node managers. Each manager
destroys only its recorded Vast.ai contract and records `Destroyed`.

---

## 16. Implementation Boundaries

The code should preserve these boundaries even if local test plugins combine
steps for convenience:

- provider lease creation is not SSH bootstrap
- SSH bootstrap is not post-handoff supervision
- `NodeManager` state is the per-node ledger
- datastream owns log bodies
- the swactor actor runtime owns live communication after handoff
- teardown uses known lease handles only

A local Docker test provider may emit lease, endpoint, bootstrap, and swactor
join observations quickly, but the observations should still map onto the same
FSM stages. This keeps local tests aligned with Vast.ai behavior.
