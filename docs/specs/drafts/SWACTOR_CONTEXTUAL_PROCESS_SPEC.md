# Swactor Contextual Process Feature and Migration Specification

Status: Implemented

Last modified: c705c7428960d287f190cad6bfbf57c193da31df

This specification supersedes the job-runner design. It defines the target
behavior, architecture, migration, implementation order, and verification for
spawning an OS process that may claim a Swactor context.

---

## 1. Objective

Swactor MUST support two explicit forms of supervised process execution:

1. **Native process:** the existing managed OS process, with lifecycle,
   termination, stdout, and stderr supervision.
2. **Contextual process:** the same managed OS process plus a provisioned,
   process-scoped Swactor context that an installed language binding may claim.

Contextual execution MUST reuse `swactor-process`; it MUST NOT introduce a job,
job runner, scheduler, workspace protocol, setup/run phases, output collector,
or job FSM.

For Python, the guest API remains:

```python
import swactor

async def main(ctx):
    ...

swactor.run(main)
```

`swactor.run` MUST obtain everything required to construct `ctx` without the
program supplying actor addresses, transport endpoints, capabilities, arena
metadata, or routing configuration.

---

## 2. Scope

### 2.1 In scope

- safe supervision of contextual OS processes;
- one opaque host-to-child bootstrap handle;
- data-plane session, arena, routing, and capability provisioning;
- exactly-once binding attachment;
- observable distinction between OS start and context readiness;
- deterministic lifecycle and cleanup under races and failures;
- migration of the existing Python binding;
- removal of the job runner and job-specific process/data-plane terminology;
- a deterministic, model-based test architecture that exercises arbitrary legal
  event DAGs and rejects minimally invalid variants.

### 2.2 Out of scope

- automatic language or entrypoint detection;
- transparent libc, allocator, syscall, CUDA, or device interception;
- making an unaware executable use a Swactor context;
- dependency installation, workspaces, setup commands, or output collection;
- scheduling, placement, retry, or node provisioning policy;
- UI submission and remote artifact transfer;
- context inheritance across `fork`;
- generalized interactive process support.

A contextual executable opts in by invoking a supported binding. An executable
that does not invoke a binding remains valid only on the native spawn path.

---

## 3. Terminology

- **Process core:** `swactor-process`, responsible only for OS process lifecycle.
- **Contextual spawner:** the composition layer that provisions a context and
  delegates OS supervision to the process core.
- **Bootstrap handle:** the sole opaque descriptor inherited by a contextual
  child.
- **Bootstrap claim:** the binding's one permitted use of that handle.
- **Context ready:** the binding has attached successfully and can use the
  provisioned data plane.
- **Session:** the process-scoped host data-plane session.
- **Session capability:** the unforgeable authorization for that session.
- **Session access:** the execution identity and namespace prefixes authorized
  for that session.
- **Execution:** one contextual process lifetime. This is correlation identity,
  not a job abstraction.

---

## 4. Required behavior

### 4.1 Native process behavior

`spawn_local_process` and `ProcessSpec` retain their existing contract. Native
spawn MUST NOT:

- allocate an arena;
- create a data-plane session;
- inherit a bootstrap handle;
- wait for a binding;
- emit contextual lifecycle events;
- depend on the contextual-spawn crate.

### 4.2 Contextual process behavior

A contextual spawn MUST:

1. validate its process specification and session access;
2. provision the arena, session capability, host session, and private bootstrap
   channel;
3. transfer the child end of that channel through the process core as an
   explicitly owned inherited resource;
4. start the OS child using the existing process actor;
5. allow the installed binding to claim the bootstrap exactly once;
6. construct a usable binding context without caller coordination;
7. resolve context readiness or bootstrap failure;
8. preserve native process lifecycle facts;
9. stop a started child whose context cannot become ready;
10. revoke the session and release every contextual resource on every terminal
    path.

The contextual spawner owns this sequence. Its caller supplies normal process
configuration and session authorization, not transport internals.

### 4.3 Guest-visible bootstrap surface

The child MUST inherit exactly one Swactor-owned bootstrap descriptor. The
private binding ABI SHOULD use a fixed descriptor number so no environment
variable is required.

The following environment variables MUST be removed:

```text
SWACTOR_ARENA_FD
SWACTOR_DATA_PLANE_ACTOR
SWACTOR_JOB_CAPABILITY
SWACTOR_DATA_PLANE_ENDPOINT
```

The binding MUST treat the bootstrap descriptor as opaque. A one-use Unix
`SOCK_SEQPACKET` channel is the preferred Linux implementation. The host sends
private, versioned session material and transfers the arena descriptor with
`SCM_RIGHTS` only after accepting the claim.

Possession of the bootstrap descriptor is a bearer capability. The protocol does
not promise to conceal its bytes from a malicious child; it promises that these
bytes are not configuration or application API and that another contextual
spawn cannot accidentally receive them.

### 4.4 Binding behavior

`swactor.run(main)` MUST:

1. locate the private bootstrap descriptor;
2. perform one versioned bootstrap claim;
3. receive and validate the arena descriptor and private session material;
4. establish child routing;
5. attach the child data plane;
6. notify the host that context attachment succeeded;
7. invoke `main(ctx)` using the existing Python `Context` and data-plane API.

A missing, closed, malformed, incompatible, already-claimed, or rejected
bootstrap MUST raise `BootstrapError`. Attachment errors MUST retain their
existing typed translation where possible and MUST also resolve the host-side
bootstrap as failed.

`swactor.run` MUST NOT silently fall back to an uncontextualized execution.

---

## 5. Lifecycle contract

Native process facts and context attachment facts are related but distinct.

### 5.1 Observable events

The contextual layer exposes:

```text
Process(ProcessOutput)
ContextReady
BootstrapFailed { reason }
```

`ProcessOutput` remains owned by `swactor-process` and retains its current
`Started`, `SpawnFailed`, `Exited`, `Error`, stdout, and stderr semantics.

### 5.2 Ordering and resolution

- `ContextReady` MUST occur only after `Process(Started)`.
- `ContextReady` and `BootstrapFailed` are mutually exclusive.
- At most one context-resolution event may be emitted.
- If OS spawn fails, the contextual layer emits the native `SpawnFailed` fact and
  no context-resolution event: no child existed to claim a context.
- After `Process(Started)`, a process terminal event MUST be preceded by exactly
  one context-resolution event.
- Exit, channel closure, attachment failure, or attachment deadline before
  readiness resolves as `BootstrapFailed`.
- `BootstrapFailed` after OS start MUST request native process termination.
- A late claim, attachment result, timeout, or stop acknowledgement MUST NOT
  change a resolved context outcome.
- Native process stdout and stderr remain observable before and after context
  resolution until the native process terminates.

### 5.3 Readiness

OS `Started` means only that the executable was created. `ContextReady` means the
binding has attached and its data plane is usable. Callers MUST use
`ContextReady`, not `Started`, when they require Swactor API availability.

The attachment deadline begins after OS start. It is configured by the
contextual spawner and driven by its clock service; bindings MUST NOT embed a
separate hard-coded policy deadline.

### 5.4 Stop and cleanup

A stop request may race provisioning, OS spawn, bootstrap claim, attachment, or
exit. It MUST be idempotent.

- Before OS start, stop prevents or cancels spawn where possible.
- After OS start, stop delegates to `send_process_command` and its existing
  terminate/kill policy.
- Stop before readiness resolves the context as failed unless OS spawn itself
  fails first.
- Bootstrap channel closure and session revocation may begin immediately after
  context failure.
- Arena backing and resources reachable by the child MUST remain owned until the
  OS child has terminated.
- Cleanup effects MUST execute at most once and MUST eventually complete after
  the process reaches a terminal state.
- No terminal path may leave a routable child session, active capability,
  bootstrap descriptor, or arena owner behind.

---

## 6. Behavioral invariants

The implementation and every test oracle MUST enforce these invariants.

### Identity and isolation

- **I1:** One contextual spawn owns one execution identity, session capability,
  host session, arena, and bootstrap channel.
- **I2:** A bootstrap handle can be claimed at most once.
- **I3:** A handle created for execution A cannot attach execution B.
- **I4:** Concurrent spawn cannot leak one Swactor-owned child descriptor into
  another child.
- **I5:** Stale events from an earlier execution or generation cannot affect a
  later execution.

### Ordering and outcomes

- **I6:** Context cannot resolve before OS spawn resolves.
- **I7:** Context ready requires OS start, accepted claim, successful routing, and
  successful data-plane attachment.
- **I8:** Context ready and bootstrap failed cannot both occur.
- **I9:** Once emitted, process and context terminal facts are immutable.
- **I10:** Started contextual processes produce one context resolution before
  their native terminal event is forwarded.
- **I11:** Native spawn failure never masquerades as bootstrap failure.

### Authorization and API boundary

- **I12:** The session accepts only its own capability and current generation.
- **I13:** Session access limits every namespace open independently of path
  discoverability.
- **I14:** No child-facing environment or argument exposes Swactor routing,
  actor, capability, endpoint, or arena internals.
- **I15:** Native spawn receives no contextual authority or resources.

### Ownership and cleanup

- **I16:** Bootstrap resources outlive the attempt to `exec` the intended child.
- **I17:** Context resources outlive the running child and no longer.
- **I18:** Revocation and cleanup are idempotent under duplicate and reordered
  completion events.
- **I19:** Quiescence after any terminal path leaves no live bootstrap endpoint,
  host session, session capability, route, arena owner, or deadline.
- **I20:** Failure in one execution cannot stop, revoke, or corrupt another.

---

## 7. Public API target

Exact Rust layout may adapt to existing actor conventions, but the ownership and
observable types below are normative.

### 7.1 Retained process API

```text
ProcessSpec
ProcessCommand
ProcessOutput
ProcessOutputConfig
spawn_local_process
send_process_command
```

### 7.2 Process-core resource seam

`swactor-process` adds a narrow resource-bearing spawn primitive:

```rust
pub struct ProcessSpawnResources {
    // Owned descriptor mappings; construction validates unique child targets.
}

pub fn spawn_local_process_with_resources(
    ctx: &Ctx,
    sender: &ExternalSender,
    spec: ProcessSpec,
    resources: ProcessSpawnResources,
    output: ProcessOutputConfig,
) -> Result<ActorAddress, Error>;
```

`ProcessSpawnResources` owns descriptors until the OS spawn attempt resolves.
Descriptor sources remain close-on-exec in the parent. Child descriptor mapping
MUST happen atomically in the child through `posix_spawn` file actions or an
equivalent safe `pre_exec` mapping; the implementation MUST NOT create an
ambient parent-side non-`CLOEXEC` inheritance window.

The existing `spawn_local_process` is the empty-resource path.

### 7.3 Contextual process API

A new `swactor-process-context` composition crate exposes:

```rust
pub struct ContextualProcessSpawner { /* node services */ }

pub struct ContextualProcessSpec {
    pub process: ProcessSpec,
    pub access: SessionAccess,
    pub attach_deadline: Duration,
}

pub enum ContextualProcessOutput {
    Process(ProcessOutput),
    ContextReady,
    BootstrapFailed { reason: BootstrapFailure },
}

impl ContextualProcessSpawner {
    pub fn spawn(
        &self,
        ctx: &Ctx,
        sender: &ExternalSender,
        spec: ContextualProcessSpec,
        output: ContextualProcessOutputConfig,
    ) -> Result<ActorAddress, Error>;
}
```

`ContextualProcessSpawner` is constructed once from node-owned runtime,
transport, namespace, arena, and route services. Per-spawn callers cannot supply
raw host actor addresses, endpoint addresses, arena generations, or session
capabilities.

### 7.4 Data-plane terminology

Clean cutover:

```text
JobCapability  → SessionCapability
JobContext     → SessionAccess
run_id         → execution_id
JobHandoff     → removed
install_session_env → removed
```

No compatibility aliases or deprecated environment path remain.

---

## 8. Code architecture

### 8.1 Dependency direction

```text
swactor-process                 data-plane
         \                         /
          \                       /
            swactor-process-context
                       |
                 language bindings
```

`swactor-process` MUST NOT depend on the data plane or contextual-spawn crate.
The contextual crate may depend on both. Python consumes the guest bootstrap
helper and existing data-plane API.

### 8.2 Deterministic coordinator

The contextual crate MUST separate lifecycle decisions from side effects.

A small coordinator owns plain state and implements:

```text
apply(Event) -> ordered list of Effect
```

Representative input events:

```text
SpawnRequested
ProvisionSucceeded / ProvisionFailed
ProcessStarted / ProcessSpawnFailed / ProcessExited / ProcessError
BootstrapClaimed / BootstrapRejected / BootstrapClosed
AttachmentSucceeded / AttachmentFailed
AttachmentDeadline
StopRequested
SessionFault
CleanupCompleted
```

Representative effects:

```text
ProvisionSession
SpawnNativeProcess
ArmAttachmentDeadline / CancelAttachmentDeadline
AcceptBootstrap / RejectBootstrap / CloseBootstrap
EmitContextReady / EmitBootstrapFailed / EmitProcessOutput
StopNativeProcess
RevokeSession
ReleaseArena
Finish
```

The actor adapter executes effects through narrow ports and feeds their outcomes
back as events. It MUST NOT contain independent lifecycle policy. The pure
coordinator is production code, not a test-only copy.

Required ports are limited to:

- process spawn/control;
- session and arena provisioning;
- bootstrap transport;
- clock/deadline scheduling;
- lifecycle output.

This split exists to make races, failures, and cleanup exhaustively testable
without real time, OS scheduling, or network nondeterminism. It MUST NOT grow
into a generic workflow or job framework.

### 8.3 Bootstrap implementation

The bootstrap transport belongs beside the data-plane bootstrap contract. Its
host and guest helpers own framing, version negotiation, descriptor transfer,
and closure. Language bindings MUST call the guest helper rather than parse wire
fields.

The arena header and mapping validation remain data-plane concerns. Actor
routing and session authorization remain private bootstrap payload fields.

---

## 9. Deterministic contract test architecture

### 9.1 Test objective

Tests MUST prove behavior over arbitrary partial orders of legal lifecycle
events, not only hand-authored happy paths. They MUST check invariants after each
step and at quiescence. They MUST also prove that the checker rejects traces just
outside the legal contract.

Tests MUST NOT depend on sleeps, wall-clock timing, random actor scheduling,
real network timing, or inspection of private implementation fields.

### 9.2 Scenario DAG

The contextual crate provides a test harness with a typed `ScenarioDag`:

```text
ScenarioDag
  nodes: typed external actions or port completions
  edges: required happens-before relationships
  identities: execution/session/generation correlation
  faults: explicit selected failure outcomes
```

The generator MUST:

1. choose one or more concurrent executions;
2. select legal terminal outcomes for provisioning, spawn, claim, attachment,
   stop, and exit;
3. add mandatory causal edges, such as spawn before OS start and claim before
   attachment result;
4. add arbitrary acyclic ordering edges between otherwise concurrent actions;
5. include races such as stop versus start, timeout versus claim, exit versus
   attachment, duplicate delivery, and stale completion;
6. reject contradictory outcome sets rather than normalizing them silently;
7. shrink while preserving graph validity and mandatory causal edges.

For each generated DAG, the harness executes multiple topological
linearizations. Small DAGs SHOULD execute every topological order; larger DAGs
execute deterministic seeded linearizations emphasizing first/last placement of
concurrent boundary events.

### 9.3 Deterministic driver

The SUT uses the production coordinator with fake ports:

- virtual monotonic clock;
- deterministic execution/session/generation identifiers;
- synthetic owned-descriptor identities;
- recorded process, bootstrap, session, and output effects;
- explicit effect completion controlled by DAG nodes.

After every delivered event, the harness records:

```text
input event
emitted effects
public lifecycle outputs
resource ledger
pending deadlines
context resolution
process resolution
```

No fake may make lifecycle decisions on behalf of the coordinator.

### 9.4 Independent contract oracle

The oracle MUST be declarative and separate from the coordinator transition
implementation. It checks the trace and resource ledger against §5 and §6; it
MUST NOT call the coordinator to calculate expected behavior.

At each prefix it checks safety properties, including uniqueness, ordering,
isolation, authorization, and terminal monotonicity. At quiescence it also checks
liveness obligations: required resolution occurred and the resource ledger is
empty.

Generated traces and minimal regressions are persisted using the repository's
existing `proptest` regression mechanism.

### 9.5 Deliberate rejection tests

The suite MUST bind each contract from both sides:

1. generate or construct a legal DAG and prove every selected linearization is
   accepted;
2. make one minimal illegal mutation and prove the oracle rejects it with the
   expected invariant identifier.

Required mutation pairs include:

| Accepted boundary | Deliberately rejected neighbor |
|---|---|
| one claim | duplicate claim (`I2`) |
| execution A claims A's handle | execution B claims A's handle (`I3`) |
| child inherits its own descriptor | child inherits a sibling descriptor (`I4`) |
| ready after start and attachment | ready before start or attachment (`I6`, `I7`) |
| one context outcome | ready and failed both emitted (`I8`) |
| bootstrap failure after start | bootstrap failure used for OS spawn failure (`I11`) |
| authorized path open | open outside session prefixes (`I13`) |
| native spawn with no context | native spawn receives bootstrap authority (`I15`) |
| cleanup once after terminal | cleanup omitted or repeated non-idempotently (`I18`, `I19`) |
| stale event ignored | stale event changes current execution (`I5`) |
| one execution fails in isolation | sibling resources are revoked (`I20`) |

These are passing tests that deliberately feed invalid traces to the checker and
assert a specific rejection. The suite MUST also include checker-calibration
fixtures with deliberately broken effect ledgers. This prevents a vacuous oracle
that accepts everything or never observes cleanup leaks.

### 9.6 Boundary integration tests

Model tests do not replace real boundary verification:

- **Process descriptor tests:** real concurrent Linux children prove fixed-target
  inheritance, `CLOEXEC`, ownership through `exec`, and no sibling leakage.
- **Bootstrap tests:** real Unix sockets prove one-use claim, version rejection,
  truncated framing, peer closure, and `SCM_RIGHTS` arena transfer.
- **Data-plane tests:** real session attachment proves capability and generation
  rejection and cleanup.
- **Python test:** a real Python child calls `swactor.run`, observes usable
  `ctx.data`, performs one namespace operation, and exits successfully.
- **Failure Python tests:** missing claim, duplicate claim, attachment rejection,
  and user exception remain distinguishable.
- **Native regression tests:** existing `swactor-process` lifecycle, output, and
  stop contracts remain unchanged.

Integration tests use explicit synchronization events or bounded virtual/test
engine progress. They MUST NOT use sleep as correctness synchronization.

---

## 10. Migration map

| Current | Target | Action |
|---|---|---|
| `swactor-process::spawn_local_process` | Native process primitive | Retain unchanged behavior. |
| `ProcessSpec` | OS execution description | Keep free of context internals. |
| No owned inherited-resource seam | `ProcessSpawnResources` | Add and verify atomic child-only inheritance. |
| `crates/process/pipeline.rs` and `yaml.rs` | Nothing | Remove job/pipeline layer and exports, subject to final callsite inventory. |
| `swactor-job-runner` | Nothing | Remove crate, FSM, wire protocol, packaging, and tests. |
| Myelin job deployment/reconciliation | Contextual execution submission | Replace required node launch behavior; delete setup/workspace/output job paths. |
| `JobHandoff` and environment assembly | Private bootstrap host endpoint | Replace; no compatibility path. |
| Four bootstrap environment variables | Fixed opaque bootstrap descriptor | Remove constants, parsing, tests, and deployment assumptions. |
| `JobCapability` | `SessionCapability` | Rename every wire and API use. |
| `JobContext { run_id, ... }` | `SessionAccess { execution_id, ... }` | Rename and migrate serialized/configured uses. |
| `install_session_env` | Nothing | Delete. |
| Python `job.rs` | Context/bootstrap implementation | Rename module and internal job-named symbols. |
| `JobRouting` | Private context routing state | Rename; never expose to guest code. |
| `swactor.run(main)` | Same guest API | Preserve observable behavior while replacing bootstrap source. |
| Dashboard `job-runner-*` fixtures | Execution/process labels | Update fixtures without adding dashboard control behavior. |
| Draft job-runner spec | Superseded | Archive or remove when this migration lands. |

No deprecated aliases, old environment fallback, dual bootstrap protocol, or
job-runner compatibility shim may remain after migration.

---

## 11. Implementation sequence

Each stage must leave one authoritative path for the contract it introduces.
Temporary compatibility is allowed only within an unmerged implementation
branch and must not appear in the completed feature.

1. **Inventory:** confirm all process pipeline, job-runner, Myelin, deployment,
   dashboard fixture, Python, and data-plane callsites named in §10.
2. **Process resources:** implement owned child descriptor mapping and its real
   Linux isolation tests.
3. **Terminology cutover:** rename data-plane job capability/context concepts to
   session concepts across wire codecs, tests, and bindings.
4. **Bootstrap channel:** implement host/guest one-use protocol and boundary
   tests; keep private payload construction in Swactor.
5. **Deterministic coordinator:** implement event/effect core, ports, contract
   oracle, DAG generator, legal properties, and deliberate rejection fixtures.
6. **Contextual actor:** connect coordinator effects to process, data-plane,
   bootstrap, clock, and output adapters.
7. **Python migration:** make `swactor.run` claim the bootstrap channel; remove
   environment parsing and hard-coded attachment deadline.
8. **End-to-end proof:** spawn a real Python process through the contextual API
   and exercise `ctx.data`.
9. **Application migration:** replace the node-side job execution path with
   contextual process submission where required for current application
   behavior.
10. **Deletion:** remove job-runner crate, process pipeline/YAML job layer, old
    codecs, app job FSMs, workspace/output packaging, environment bootstrap, and
    obsolete tests/configuration.
11. **Final verification:** run focused suites, workspace compilation, and static
    absence checks for all removed symbols and environment variables.

Deletion follows successful migration of required callers; it is not deferred as
follow-up cleanup.

---

## 12. Acceptance criteria

The feature is complete only when all of the following are true:

1. Native process behavior remains compatible and context-free.
2. A contextual Python process reaches `ContextReady`, uses `ctx.data`, and exits
   zero through real process supervision.
3. The child receives one opaque bootstrap descriptor and no Swactor bootstrap
   environment contract.
4. Concurrent contextual children cannot claim or inherit each other's
   resources.
5. Every started contextual process emits exactly one context resolution before
   its native terminal event.
6. Every failure and stop race reaches quiescence with an empty contextual
   resource ledger.
7. Arbitrary generated legal event DAGs satisfy every invariant under tested
   topological linearizations.
8. Minimal illegal mutations are rejected with the intended invariant IDs, and
   checker-calibration fixtures detect deliberately broken ledgers.
9. Python guest APIs and existing data-plane operations remain usable without
   caller-supplied routing or capability internals.
10. `swactor-job-runner`, process job/pipeline APIs, job-named data-plane session
    types, leaked bootstrap variables, and all compatibility paths are absent.
11. UI submission, remote transfer, generalized interception, scheduling, and
    provisioning remain outside this feature.
