# MVP System Module Boundary Specification

**Status:** draft module-boundary specification.

This document defines the intended behavioral modules for the MVP GPU pipeline
system and the reusable crates that should sit underneath it. It describes module
ownership in terms of behavior and contracts, not file layout.

The MVP system is an application layer on top of swactor. It coordinates trusted
GPU workers for one linear model pipeline. General runtime pieces should live in
reusable crates when their contracts do not depend on GGUF, tinygrad, prompt
execution, or MVP-specific run policy.

---

## 1. Purpose

The module split has three goals:

1. Separate reusable runtime infrastructure from MVP-specific GPU orchestration.
2. Give each module one durable ownership boundary and one black-box test surface.
3. Keep binaries as composition shells rather than homes for reusable behavior.

A module boundary is correct when its tests can describe behavior without knowing
how a complete MVP run is wired together.

---

## 2. Layering Principles

The system is divided into two layers:

```text
reusable crates
  -> MVP system modules
  -> binaries / operator entrypoints
```

Reusable crates own behavior that can be used outside the MVP GPU pipeline. MVP
modules own behavior that exists because this system runs staged GPU inference.
Binaries own process startup, command-line parsing, environment wiring, and
composition only.

Rules:

- Payload bytes remain opaque below MVP-specific worker/staging code.
- Provider leases do not know about model prompts or tensor shapes.
- Transport and data-plane modules do not know about GGUF, tinygrad, sampling, or
  prompt policy.
- Orchestration owns run authority, but not local byte movement or GPU execution.
- Staging owns stage-local control and weight lifetimes, but not global run
  policy.
- Worker owns GPU worker process control and device-facing protocol, but not
  provider provisioning or global planning.
- Observability records behavior; it must not become a second control path.

---

## 3. Reusable Crates

### 3.1 data-plane

The data-plane crate owns local payload memory and byte movement primitives that
are reusable outside the MVP GPU pipeline.

It provides arena-backed allocation, ring layout descriptions, object record
framing, cursor/wake contracts, and local ingress/egress movement between process
boundaries. It does not know about GGUF, stages, prompts, provisioning providers,
or model execution.

Responsibilities:

- create and manage arena-backed memory regions
- lease and release ring ranges
- describe ring layouts without process-local pointers
- define object record headers and sequence contracts
- expose local producer/consumer ring operations
- enforce quiescence before arena reuse
- validate object structure, extent, alignment, and sequence policy
- provide local ingress and egress byte movement contracts

Non-responsibilities:

- model planning
- GPU execution
- provider provisioning
- prompt handling
- network peer discovery
- dashboard rendering
- interpreting tensor values or token content

Test surface:

- arena lease ordering and non-overlap
- ring cursor/wake behavior
- object header validation
- ingress/egress complete-object and partial-ring behavior
- quiescence before release
- sequence violation rejection

### 3.2 provisioning

The provisioning crate owns provider-neutral node lease lifecycle.

It models desired nodes, provider leases, boot sessions, readiness probes,
destroy handles, and provider observations. Provider adapters may use local
processes, local containers, SSH sessions, or external lease APIs, but the core
lifecycle is provider-neutral.

Responsibilities:

- describe desired node shape and boot requirements
- acquire and track provider leases
- start boot sessions for leased nodes
- surface provider logs and lifecycle observations
- expose destroy handles for cleanup
- classify lease, boot, and teardown outcomes
- support deterministic mock providers for tests

Non-responsibilities:

- GPU stage assignment
- model weight planning
- prompt serving
- arena/ring allocation
- transport stream pumping
- provider-specific business logic beyond adapter boundaries

Test surface:

- lease acquisition success/failure
- boot session success/failure
- cleanup after partial acquisition
- retry and cancellation behavior
- provider adapter command/request construction
- destroy handle idempotence

### 3.3 process

The process crate owns supervised local process lifecycle.

It starts processes, forwards commands, captures output, records exits, and
provides lifecycle observability. It does not own the meaning of a particular
child protocol.

MVP worker JSON, tinygrad commands, and GPU worker states belong above this
crate.

### 3.4 transport, distribution, and iroh-driver

Transport crates own peer identity, message encoding, cluster membership,
routing, and concrete iroh/QUIC behavior.

Responsibilities are split by abstraction:

- transport: peer identity, codecs, envelope shapes, transport traits
- distribution: actor routing, node directory, membership, SWIM, route claims
- iroh-driver: concrete iroh endpoint, QUIC streams, relay/direct connectivity,
  edge transport, datastream transport

These crates do not know about stage ranges, GGUF, prompt policy, or worker
execution.

### 3.5 datastream and dashboard

The datastream crate owns observable frame transport, channel cataloging,
producer handles, ingest, storage, and read-side projections.

The dashboard crate owns read-only presentation of datastream state.

MVP-specific event names and payload vocabularies belong in the MVP
observability module, but frame transport and storage are reusable.

---

## 4. MVP System Modules

### 4.1 orchestration

The orchestration module owns run authority for one MVP pipeline execution.

It selects the intended node pool, builds the run plan, assigns stages and edges,
drives provisioning, observes readiness, injects prompts, consumes output tokens,
records terminal outcome, and initiates teardown.

Responsibilities:

- build and validate a run plan
- assign stage indices, layer ranges, edge ids, and object specs
- coordinate node availability and pool readiness
- dispatch stage provisioning
- enforce the global readiness barrier
- submit prompt work after readiness
- apply sampling and stop policy at the run boundary
- record completed, faulted, or operator-stopped outcomes
- initiate teardown after terminal outcome

Non-responsibilities:

- local ring allocation
- raw byte copying
- QUIC stream pumping
- GPU worker process control
- weight tensor loading internals
- provider-specific lease implementation
- dashboard rendering

Test surface:

- run plan validation
- layer and edge assignment invariants
- readiness gate behavior
- provisioning dispatch behavior
- prompt injection only after readiness
- sequence-ordered output consumption
- terminal outcome and teardown commands
- fault classification at the run boundary

### 4.2 node

The node module owns node-local runtime admission and composition state.

A node is a trusted participant in a run. It owns local boot state, local actor
addresses, node availability, and the binding between local modules needed to
serve a provisioned stage. It does not decide global topology.

Responsibilities:

- represent node boot and node availability
- reject run work before local availability
- host node-local actors and service endpoints
- accept provisioned work from the authorized orchestrator
- route local control events between staging, worker, data-plane, and transport
- report node-local lifecycle and fault events

Non-responsibilities:

- global planning
- provider lease acquisition
- tensor interpretation
- prompt policy
- reusable arena implementation
- reusable process supervision

Test surface:

- boot state progression
- rejection before availability
- authorized provisioning admission
- local fault fanout
- node shutdown behavior

### 4.3 staging

The staging module owns stage-local control for assigned model ranges and raw
weight tensor lifetimes.

A stage receives a provisioned role, materializes or locates the assigned weight
artifact, loads or binds the assigned layer range, validates inbound and outbound
edge readiness, and admits exactly one execution step at a time.

Responsibilities:

- validate stage provisioning
- own assigned stage index and layer range
- plan and track stage-local weight materialization
- load or bind assigned weights before readiness
- report stage readiness only after worker, weights, and edges are ready
- enforce per-stage sequence ordering
- issue worker execution commands for loaded input objects
- surface stage faults with stable reasons
- participate in teardown and weight/device release

Non-responsibilities:

- global run planning
- node leasing
- prompt tokenization
- raw ring cursor manipulation
- network transport
- provider bootstrap

Test surface:

- stage provisioning validation
- weight lifecycle success/failure
- readiness only after all local dependencies
- single active execution step
- sequence violation faulting
- teardown after ready, executing, and faulted states

### 4.4 node_data

The node_data module is the MVP-facing adapter over reusable data-plane
contracts.

It connects node-local arena/ring/object movement to MVP edge and worker use. It
may define MVP object specs and helper conversions, but the raw arena/ring
implementation remains reusable.

Responsibilities:

- bind MVP object specs to data-plane object records
- adapt local rings to worker ingress and egress behavior
- expose node-local object loaded/produced events
- preserve sequence and extent guarantees at the node boundary
- coordinate local quiescence requests during teardown

Non-responsibilities:

- allocation algorithms that belong in the reusable data-plane crate
- network membership
- provider leases
- model planning
- prompt stop policy

Test surface:

- MVP object spec conversion
- ingress object readiness after full payload
- egress object production with correct headers
- sequence propagation through local movement
- teardown quiescence handoff

### 4.5 transport

The transport module is the MVP-facing adapter over swactor distribution and iroh
transport crates.

It establishes the network-facing edge transport required by a run. It uses
cluster membership, route ownership, iroh endpoints, and persistent edge streams,
but it does not interpret payload bytes.

Responsibilities:

- bind run edge ids to concrete send/receive transport endpoints
- open and accept persistent edge streams
- bridge network stream lifecycle into node/stage edge lifecycle
- surface stream readiness and stream faults
- preserve ordering guarantees provided by the underlying stream

Non-responsibilities:

- arena allocation
- object header interpretation
- GPU execution
- provider leasing
- prompt policy

Test surface:

- edge preamble behavior
- send/receive establishment ordering
- stream-arrives-before-spec and spec-before-stream cases
- stream fault propagation
- connection reuse policy where applicable

### 4.6 worker

The worker module owns GPU worker process control and device-facing protocol.

It treats the worker process as a supervised, command-driven participant. It
validates worker generation, installed rings, device handles, command admission,
and process lifecycle. It does not own provider leases or global run planning.

Responsibilities:

- start and initialize the GPU worker process
- maintain worker generation
- install and uninstall worker-visible rings
- configure stage role execution
- issue explicit execution commands
- route worker events to node and staging control
- classify worker fatal, step, object, and ring failures
- release device handles according to protocol

Non-responsibilities:

- local process spawning mechanics below the generic process crate
- run-level terminal outcome
- provider boot
- global stage assignment
- prompt session UX
- dashboard presentation

Test surface:

- initialization success/failure
- generation invalidation after restart
- command rejection before readiness
- install/uninstall ring behavior
- execution command admission
- worker crash fault fanout
- device handle release behavior

### 4.7 prompt

The prompt module owns the operator-to-run prompt protocol and prompt result
contract.

It accepts prompt submissions, assigns request identity, sends work to the run
authority, receives token/text progress, and reports terminal prompt outcomes.
It does not own model planning or worker execution internals.

Responsibilities:

- define prompt request and prompt event protocol
- validate prompt request shape
- expose prompt progress and terminal events
- preserve request identity across async execution
- carry token/text output without becoming a transport hot path

Non-responsibilities:

- stdin UX
- provider leasing
- GPU sampling implementation internals
- raw ring movement
- global run lifecycle outside prompt admission/result

Test surface:

- request serialization and parsing
- terminal event detection
- progress event ordering
- malformed request rejection
- connection/session close behavior

### 4.8 observability

The observability module owns the MVP event vocabulary and emission helpers.

It defines stable event identities and records needed to prove lifecycle behavior.
It uses the reusable datastream crate for frame transport and storage. It must
not participate in control decisions except through ordinary module APIs that
observe recorded events.

Responsibilities:

- define MVP lifecycle event names and payload shapes
- register MVP datastream channels
- emit structured lifecycle, progress, and fault records
- archive events for benchmark and contract evidence
- bridge provider/bootstrap logs into the event stream
- provide dashboard views over MVP events when enabled

Non-responsibilities:

- actor routing
- provider lease decisions
- run state mutation
- retry policy
- prompt response generation

Test surface:

- stable event field presence
- channel registration
- archive ordering and gap reporting
- benchmark envelope construction
- log-to-event bridge behavior

### 4.9 chat

The chat module owns the MVP operator-facing wrapper.

It configures and launches an MVP run, prepares required runtime artifacts,
starts or embeds orchestration according to the selected mode, opens the prompt
session, and presents prompt results to the operator.

Responsibilities:

- parse chat command-line and chat-specific configuration
- prepare image/runtime artifacts for selected provider mode
- obtain explicit operator approval for rented-node runs when required
- start the orchestration layer in the supported wrapper mode
- run the prompt session loop
- emit chat lifecycle and benchmark events
- shut down the run on operator interrupt or session end

Non-responsibilities:

- standalone orchestration design
- provider-neutral lease lifecycle internals
- data-plane implementation
- worker protocol implementation
- model shard planning internals

Test surface:

- config and CLI resolution
- provider selection conflicts
- cached model selection
- prompt session behavior
- interrupt handling
- runtime preparation request construction
- operator approval behavior

---

## 5. Dependency Direction

Allowed dependency direction:

```text
reusable crates
  data-plane
  provisioning
  process
  transport / distribution / iroh-driver
  datastream / dashboard

MVP modules
  orchestration
  node
  staging
  node_data
  transport
  worker
  prompt
  observability
  chat

binaries
  mvp-chat
  mvp-worker-node
```

Rules:

- reusable crates must not depend on MVP modules
- chat may depend on orchestration, prompt, deployment behavior, and
  observability
- orchestration may depend on staging contracts, node contracts, provider
  contracts, prompt, transport, and observability
- staging may depend on worker, node_data, and GGUF/weight contracts
- worker may depend on process and node_data contracts
- node_data may depend on data-plane, not on orchestration or chat
- transport may depend on distribution and iroh-driver, not on staging or prompt
- observability may define MVP event vocabulary but must not call back into
  runtime control paths

---

## 6. Final Repository Map

The final repository shape should keep reusable runtime crates outside
`mvp-system` and keep MVP binaries as thin composition roots.

```text
crates/
  data-plane/
    arena, rings, object records, local byte movement

  provisioning/
    provider-neutral leases, boot sessions, destroy handles

  process/
    supervised local process lifecycle

  transport/
    peer identity, codecs, transport traits

  distribution/
    node directory, membership, routing, SWIM

  iroh-driver/
    iroh endpoint, QUIC streams, edge/datastream transport

  datastream/
    frame transport, catalog, ingest, archive, views

  dashboard/
    read-only datastream presentation

  mvp-system/
    orchestration/
    node/
    staging/
    node_data/
    transport/
    worker/
    prompt/
    observability/
    chat/

    src/bin/mvp_chat.rs
    src/bin/worker_node.rs
```

---

## 7. Binary Boundaries

Binaries are composition roots.

The chat binary owns operator entry and chat process lifetime. It should call the
chat module and avoid carrying reusable runtime logic.

The worker-node binary owns node process entry, environment extraction, signal
handling, and assembly of node-local modules. It should not define reusable
stage, worker, data-plane, transport, or observability behavior.

Binaries may contain:

- `main`
- argument parsing glue
- environment extraction glue
- signal/shutdown wiring
- module assembly
- top-level error rendering

Binaries should not contain:

- protocol definitions
- FSM implementations
- provider-neutral lifecycle rules
- reusable object/ring parsing
- worker command/event semantics
- datastream archive logic

---

## 8. Behavioral Test Surfaces

Each module must expose a black-box contract surface. Tests should assert
observable behavior, not private structure.

Preferred test subjects:

- commands emitted by an FSM
- lifecycle events
- stable fault reasons
- accepted/rejected requests
- sequence and readiness transitions
- teardown/quiescence outcomes
- serialized protocol records

Avoid tests that only restate implementation steps.

Module test boundaries:

```text
data-plane       arena/ring/object movement contracts
provisioning     lease/boot/destroy contracts
orchestration    run authority and readiness contracts
node             node admission and local routing contracts
staging          stage readiness, weights, sequence, teardown contracts
node_data        MVP object/ring adapter contracts
transport        edge stream establishment/fault contracts
worker           worker process/control/generation contracts
prompt           request/event/session contracts
observability    event vocabulary/archive contracts
chat             operator wrapper/config/session contracts
```

---

## 9. Migration Constraints

The module migration must preserve behavior while boundaries move.

Constraints:

- no token/activation algorithm changes during modularization
- no change to stage sequence semantics during code movement
- no change to run readiness requirements unless specified separately
- no change to provider behavior while extracting provider modules
- no new public duplicate type systems
- no long-lived compatibility shims after callsites migrate
- old paths may re-export during a short migration step, but final public surface
  should use module-owned names

A move is complete only when:

1. the module has a clear public contract
2. duplicate definitions are removed or re-exported from one owner
3. behavior tests live at the module boundary
4. binaries only compose the module
5. the old public path is deleted or intentionally retained as the owner
