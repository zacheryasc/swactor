# MVP Orchestrator Actor Specification

**Status:** normative contract for the actor-only MVP orchestrator.

This document defines the observable behavior of the MVP orchestrator as a Swactor actor or actor group. The orchestrator is not specified as an operating-system process, executable, command-line program, or owner of a Tokio/Iroh/Swactor engine. It runs inside any host that supplies a compatible Swactor runtime plus iroh-driver transport bridge.

---

## 1. Purpose and Contract Boundary

The orchestrator is the run authority for one MVP worker runtime. It turns an already-resolved launch request into actor commands, readiness decisions, prompt execution, lifecycle observations, and teardown decisions.

The orchestrator is responsible for:

- accepting a typed run request from its host or supervisor actor;
- building or accepting the execution shape for the run;
- requesting worker provisioning through actor-managed provider/provisioner leaves;
- driving readiness from actor reports plus engine route and membership facts;
- acknowledging worker runtime readiness;
- provisioning worker stages;
- waiting for weights and stage readiness;
- accepting prompt work through actor messages;
- dispatching direct or pipeline prompt execution through worker actors and token edges;
- emitting lifecycle, prompt, provisioning, readiness, and fault observations;
- initiating actor-based teardown for stages, token endpoints, and provider-owned workers.

The orchestrator is not responsible for:

- parsing CLI arguments, environment variables, or TOML files;
- owning an executable or binary launch contract;
- choosing or creating a Tokio runtime;
- creating or owning a Swactor runtime;
- creating or owning an `IrohDriver` endpoint;
- owning the actor scheduler, pump loop, or runtime shutdown;
- managing raw QUIC streams, ALPN negotiation, iroh connections, or Tokio tasks;
- supervising operating-system child processes directly;
- reading standard input or writing status to standard output/error;
- exposing prompt TCP RPC;
- implementing dashboard rendering;
- implementing provider marketplace behavior, worker internals, model execution, tokenizer quality, or GGUF parsing beyond the typed facts it receives.

The orchestrator may run in the same process as a wrapper, worker supervisor, dashboard, or test harness. That process is the host. Host behavior is outside this orchestrator actor contract unless it is observed through the actor/datastream surfaces defined here.

---

## 2. Engine and Host Contract

The host supplies an actor-capable engine. The engine must provide:

- a Swactor runtime with typed actor mailboxes;
- actor addresses (`ActorAddress`);
- local actor spawn/send/inbox semantics;
- registered codecs for MVP actor messages;
- an iroh-driver actor bridge for remote actor delivery when remote workers exist;
- a route view that can answer which node owns a remote actor address;
- a membership view that can answer whether a worker node is alive;
- datastream logical subscription and collection support when observability is enabled.

A conforming engine must be able to perform this work:

```text
Tokio handle
-> IrohDriver::with_handle
-> DistributionRuntimeStack
-> register_mvp_actor_codecs
-> enable_actor_bridge
-> spawn orchestrator actor/group
-> register local actor route
-> pump engine work
```

The exact host API is not part of this specification. The observable requirement is that actor messages accepted by the runtime make progress according to Swactor delivery semantics and remote actor traffic is routed through the iroh-driver actor bridge when the destination is remote.

The host owns engine progress. It may drive progress with a tick loop, a worker-thread runtime, or a wake-driven loop. A blocking host wait that prevents actor delivery, iroh ingress, iroh egress, datastream collection, membership updates, or route updates from progressing violates the orchestrator runtime model.

The orchestrator must not depend on a particular host executable, test harness, wrapper, or process name.

---

## 3. Actor Topology

The orchestrator contract is defined at actor-group boundaries. An implementation may split the group differently, but the same observable messages, reports, lifecycle decisions, and ordering guarantees must hold.

### 3.1 Required Actor Participants

Required participants:

- **Orchestrator actor/group**
  - owns run-level state;
  - accepts run observations, prompt submissions, shutdown requests, and snapshots;
  - emits commands and lifecycle reports.

- **Provisioner actor/group**
  - owns provider-facing node lifecycle;
  - starts/stops provider-owned worker leaves;
  - converts provider/plugin/managed-process observations into actor reports and datastream records.

- **Worker node agent actor**
  - lives on each worker runtime;
  - receives stage provisioning, readiness acknowledgements, prompt inference, tokenizer encode/decode, and stop messages;
  - reports runtime readiness, weights, stage readiness, faults, and stop completion back to the orchestrator.

- **Datastream publisher/collector actors or adapters**
  - carry logical datastream subscription and frame transport;
  - do not own lifecycle authority.

- **Prompt reply actor or reply target**
  - receives prompt events for an active request.

- **Tokenizer reply actor or reply target**
  - receives tokenizer encode/decode events in pipeline mode.

### 3.2 Optional Actor Participants

Optional participants:

- dashboard sinks;
- frame archive sinks;
- managed process actors for local workers or helper processes;
- provider-specific actor leaves;
- mock/stub actor leaves for tests.

Optional participants must not change orchestrator lifecycle authority. They may observe, mirror, or adapt behavior; they do not make readiness, prompt completion, or shutdown true by themselves.

### 3.3 Actor Codecs

The MVP actor codec registry must include the message families required by the active topology:

```text
NodeAgentMsg / NodeAgentReport
OrchestratorMsg / OrchestratorReport
ProvisionerMsg / ProvisionerReport
DatastreamPublisherMsg
PromptEvent / TokenizerEvent or their actor-prompt successors
```

The module or source file where a codec is registered is not a public contract. The contract is that every actor message that may cross the iroh actor bridge has a registered codec and type tag.

---

## 4. Inputs

The orchestrator accepts typed actor inputs only. Configuration files, environment variables, process arguments, filesystem discovery, interactive input, OS signals, and TCP connections are host or adapter inputs. A host may translate those inputs into typed actor messages, but the translation is outside this orchestrator actor contract.

### 4.1 Run Request

A run starts with a typed run request delivered to the orchestrator actor/group.

Required run request facts:

```text
RunRequest {
    run_id,
    orchestrator_node_id,
    provider_policy,
    worker_image_or_artifact_reference,
    worker_count_or_run_plan_input,
    model_identity,
    model_source,
    tokenizer_source,
    pipeline_stages,
    default_max_tokens,
    max_context,
    relay_or_endpoint_facts_needed_by workers,
    observability_policy,
    provider_preparation_result,
    prompt_policy,
}
```

The request must be typed before the orchestrator receives it. The orchestrator must not parse raw strings from CLI, TOML, or environment variables as part of this contract.

If a provider requires secrets or external preparation, the host or provider actor supplies prepared typed facts. Secret values must not be emitted by orchestrator-owned datastream records or lifecycle reports.

### 4.2 Run Plan Input

The orchestrator may either:

- receive a committed `RunPlan`; or
- receive locally inspectable model facts sufficient for a planner actor/component to produce a committed `RunPlan`.

A committed plan includes:

```text
RunPlan {
    run_id,
    orchestrator_node_id,
    stage_count,
    stage_refs,
    layer_ranges,
    token_in_edge,
    activation_edges,
    token_out_edge,
    object_specs,
    ring_specs,
    tokenizer_source,
    model_source,
}
```

Direct execution is represented as one stage. Pipeline execution is represented as two or more planned stages or any run whose prompt path requires token-in/token-out endpoints.

The orchestrator must not accept a worker-generated topology. Workers may report boot/runtime facts; they do not assign stage indexes, layer ranges, edge ids, object specs, or consumer endpoints.

### 4.3 Provisioning Reports

The orchestrator receives provisioning reports through actors. Active report kinds:

```text
ProvisionerReport::NodeLive { ... }
ProvisionerReport::NodeFailed { ... }
ProvisionerReport::LogLine { ... }
ProvisionerReport::NodesStopped { ... }
```

`LogLine` is observational. It must not make a node ready or failed by itself unless accompanied by a typed failure report.

### 4.4 Worker Runtime Reports

Workers report runtime readiness and stage state through actor messages.

A runtime-ready report contains:

```text
NodeRuntimeReady {
    run_id,
    node_id,
    stage_index,
    endpoint,
    node_actor,
    datastream_publisher,
    readiness_id,
}
```

A runtime-ready acknowledgement report contains:

```text
NodeRuntimeReadyAck {
    run_id,
    node_id,
    stage_index,
    readiness_id,
}
```

Weight/stage reports contain:

```text
WeightsReady { run_id, node_id, stage_index }
StageReady { run_id, stage_index }
StageFault { run_id, stage_index, reason }
StageStopped { run_id, stage_index }
```

Reports with mismatched run id, node id, stage index, or readiness id must not advance the active run.

### 4.5 Prompt Input

Prompt input is an actor message, not TCP RPC.

Prompt request shape:

```text
SubmitPrompt {
    request_id,
    prompt_text,
    max_tokens,
    reply_to,
}
```

`reply_to` is the actor address that receives prompt events for this request.

When `max_tokens` is absent or zero, the orchestrator does not impose an arbitrary generated-token cap. Generation may still stop on EOS, context/window exhaustion, worker fault, shutdown, or runtime/model limits.

### 4.6 Shutdown Input

Shutdown is an actor/control message.

Required shutdown shape:

```text
RequestShutdown {
    run_id,
    reason,
    reply_to: optional,
}
```

Shutdown reason examples:

```text
operator_requested
host_requested
prompt_session_closed
fatal_dependency
```

Text commands such as `stop`, `shutdown`, and `quit` are wrapper inputs only if a wrapper chooses to support them. They are not orchestrator actor inputs until translated into `RequestShutdown`.

### 4.7 Membership and Route Observations

The orchestrator may observe membership loss and route changes through host-provided actor messages or by querying engine-provided views.

Required facts:

```text
member_state(worker_swim_node_id) == Alive
route_owner(node_actor) == worker_swim_node_id
```

A membership loss for an active worker after readiness is a run fault unless the run is already tearing down.

---

## 5. Outputs

The orchestrator emits typed actor outputs and datastream observations.

### 5.1 Command Reports

Run commands are emitted as actor reports or sent directly to the responsible actor. Required command semantics:

```text
ProvisionNodes
ProvisionStage
CreateTokenInEndpoint
CreateTokenOutEndpoint
RuntimeReadyAck
SubscribeDatastream
InferPrompt
EncodePrompt
DecodeTokens
InjectTokenObject
StopRun
TearDownTokenEndpoints
StopNodes
```

A successful actor send means the runtime accepted the message for routing. It does not prove the destination acted. Every command requiring acknowledgement must have an explicit acknowledgement or later lifecycle report.

### 5.2 Lifecycle Reports

Lifecycle reports include:

```text
RunAccepted
RunRejected
RunPlanningStarted
RunPlanningReady
RunProvisioningStarted
RunReadinessStarted
RunReady
PromptAccepted
PromptCompleted
PromptFaulted
RunOperatorStopped
RunFaulted
RunTearingDown
RunTornDown
```

Lifecycle message names are normative at the actor boundary. Internal types may use different names as long as the required states remain observable.

### 5.3 Prompt Events

Prompt reply targets receive:

```text
PromptEvent::TextDelta { request_id, text }
PromptEvent::Done { request_id, final_text, tokens_generated, elapsed_ms }
PromptEvent::Fault { request_id, error }
```

`TextDelta` is non-terminal. `Done` and `Fault` are terminal. A prompt request must receive exactly one terminal event unless the reply target disappears; if the reply target disappears, the orchestrator must cancel or fault the active prompt and continue teardown rules correctly.

### 5.4 Datastream Output

Datastream records are observational. They may describe lifecycle transitions, prompt progress, provisioning events, worker logs, provider logs, membership transitions, route checks, token-edge progress, dashboard frames, or frame archive records.

Datastream records must not carry lifecycle authority. They must not be interpreted as commands. They must not make readiness, prompt completion, failure, or shutdown true.

Required core observation channels are logical, not process-owned:

```text
mvp.orch.bootstrap
mvp.orch.prompt
mvp.orch.lifecycle
mvp.orch.stage_route
mvp.swim.membership
mvp.provisioning.events
mvp.provisioning.logs.node.<node>.<stream>
```

The orchestrator actor/group does not own stdout/stderr channels. Worker/provider stdout/stderr may be observed by provider or managed-process adapters and published as provisioning log records.

---

## 6. Runtime Lifecycle

The actor-only lifecycle is:

```text
host starts engine
-> host spawns orchestrator actor/group
-> host sends RunRequest
-> orchestrator validates typed request
-> orchestrator obtains or builds committed run plan
-> orchestrator requests node provisioning through ProvisionerActor
-> provisioner reports nodes live/failure/logs
-> worker node agents report runtime ready
-> orchestrator waits for membership + route ownership
-> orchestrator sends runtime-ready acknowledgements
-> workers report runtime-ready acknowledgement
-> orchestrator provisions stages
-> workers report weights/stage ready
-> orchestrator reports prompt-ready
-> prompt messages are served through actors/token edges
-> shutdown/fault/completion triggers actor teardown
-> provisioner stops provider-owned worker leaves
-> orchestrator reports torn down or faulted teardown result
```

The host may stop the engine only after the orchestrator actor/group has reached a terminal lifecycle state or after the host has declared the actor group failed. Engine shutdown itself is outside this specification.

### 6.1 Request Validation

The orchestrator must reject a typed run request before provisioning when required facts are missing or invalid.

Examples:

- missing run id;
- duplicate stage indexes;
- stage count of zero;
- stage count inconsistent with the committed plan;
- unknown node in a committed placement;
- missing model or tokenizer source required by stage provisioning;
- missing provider/provisioner actor address;
- missing orchestrator node id for token-edge endpoints;
- missing worker image/artifact reference required by the selected provider policy.

Rejecting a run emits a typed lifecycle report and must not start workers.

### 6.2 Planning

Planning must complete before provisioning.

If the orchestrator builds a run plan, the plan must be derived from trusted host-supplied model facts or locally inspectable model metadata supplied through a typed component. Workers do not negotiate placement after boot.

A planning failure rejects the run before provisioning.

### 6.3 Provisioning

The orchestrator must request provisioning through actor-managed provider/provisioner leaves. It must not directly supervise worker processes as part of this contract.

Provisioning request shape:

```text
StartNodes {
    nodes: Vec<NodeProvisionSpec>,
    reply_to,
}
```

Each `NodeProvisionSpec` must include enough data for the provider leaf to start the worker runtime:

```text
NodeProvisionSpec {
    run_id,
    node_id,
    stage_index,
    image_or_artifact_reference,
    environment_or_runtime_facts,
    mounts_or_resource_bindings,
    coordinator_endpoint,
    orchestrator_actor,
}
```

If provisioning one node fails after earlier nodes started, the actor group must attempt to stop already-started nodes before reporting startup failure.

### 6.4 Runtime Readiness

A runtime-ready actor report is necessary but not sufficient.

A worker is runtime-ready only when all facts are true:

```text
matching NodeRuntimeReady report
+ expected run_id / node_id / stage_index
+ SWIM member state is Alive for the worker endpoint node id
+ route owner for node_actor is the worker endpoint node id
+ no provider/node failure has been reported
= runtime readiness barrier passed for that worker
```

For planned pipeline execution, every expected stage worker must pass this barrier. For direct execution, the single expected worker must pass it.

A TCP port, dashboard frame, provider log line, worker stdout line, or datastream frame must not satisfy runtime readiness.

### 6.5 Runtime-Ready Acknowledgement

After a worker passes the readiness barrier, the orchestrator sends:

```text
NodeAgentMsg::RuntimeReadyAck {
    run_id,
    node_id,
    stage_index,
    readiness_id,
}
```

The orchestrator must keep actor/transport progress running while waiting for acknowledgement reports.

If acknowledgement is not observed after the configured retry/timeout policy, startup fails.

If the datastream publisher route is available, the orchestrator may subscribe to worker datastream output during this phase. Subscription success is observability setup, not readiness authority.

### 6.6 Stage Provisioning

Stage provisioning occurs after runtime-ready acknowledgement.

Direct execution provisions one stage. Pipeline execution provisions stages from the committed run plan.

Stage provision payloads must include:

```text
run_id
orchestrator authority identity
node_id
stage_index
stage_count
layer range
inbound edge id and facts
outbound edge id and facts
model identity
GGUF/model source
tokenizer source
object specs
ring specs
consumer endpoint facts
```

A stage must reject unauthorized provisioning. `authorized_orchestrator` must be a stable orchestrator authority identity for the run, not a placeholder value.

### 6.7 Weights and Stage Readiness

A stage is ready only after the worker-side stage controller has observed all local readiness prerequisites:

```text
valid provision accepted
+ worker runtime ready
+ weights ready
+ inbound edge ready
+ outbound edge ready
= StageReady
```

In planned pipeline execution, stages are weight-loaded/provisioned sequentially unless a later spec explicitly introduces parallel load behavior. The orchestrator must not advance to the next unloaded stage until the active stage reports weights ready or faults.

### 6.8 Prompt Serving Readiness

The orchestrator reports prompt-ready only after:

```text
all expected workers provisioned
+ runtime readiness barriers passed
+ runtime-ready acknowledgements completed
+ stages provisioned
+ required weights/stages ready
+ token endpoints ready when pipeline mode uses them
```

Prompt-ready is an actor/datastream lifecycle state, not a TCP listener state.

---

## 7. Prompt Behavior

The orchestrator accepts at most one active prompt at a time. Additional prompt submissions remain pending in actor/mailbox order unless the actor group exposes a bounded queue and returns a typed `Fault` or rejection when full.

Prompt events are matched by `request_id`. Events for a non-active request must not advance the active prompt.

### 7.1 Direct Prompt Mode

Direct mode is used when no pipeline token-edge runtime is active.

Flow:

```text
SubmitPrompt actor message
-> active prompt state
-> NodeAgentMsg::InferPrompt { request_id, prompt, max_tokens, reply_to }
-> worker prompt engine
-> PromptEvent actor messages to reply target
-> terminal Done or Fault
```

The prompt reply target must be supplied to the worker node agent. A send failure to the node actor is a prompt-serving error for that request and may become a run fault if the worker path is no longer usable.

### 7.2 Pipeline Prompt Mode

Pipeline mode uses actor messages for tokenizer work and token-edge transport for generated token records.

Flow:

```text
SubmitPrompt actor message
-> tokenizer encode actor
-> TokenizerEvent::PromptEncoded
-> token-in edge bytes
-> pipeline stages
-> token-out edge bytes
-> tokenizer decode actor
-> TokenizerEvent::TokensDecoded
-> PromptEvent::TextDelta / Done / Fault
```

The first-stage node actor is the tokenizer encode actor unless the run request explicitly supplies a different tokenizer actor. The final-stage node actor is the tokenizer decode actor unless explicitly supplied otherwise.

Token-edge bytes are data-plane traffic. They must not be carried in actor mailboxes.

### 7.3 Token Sequence Rule

Pipeline token output records must be consumed in strict sequence order.

Expected rule:

```text
first expected token-out sequence = 0
received sequence must equal expected
on valid sequence: expected += 1
on EOS or max_tokens: complete prompt
on mismatch: fault prompt or run according to phase policy
```

Sequence validation protects prompt output order and prevents feedback injection out of order.

### 7.4 Prompt Terminal Rule

A prompt ends with exactly one terminal event:

```text
Done
Fault
```

Expected model/prompt failures should become prompt `Fault` events. Infrastructure failures that make the run unusable may also fault the run.

After a terminal prompt event, the active prompt state is cleared and the next pending prompt may begin if the run is still prompt-ready.

---

## 8. Shutdown and Teardown

Shutdown begins from one of these actor-visible causes:

- `RequestShutdown`;
- terminal prompt-serving policy for one-shot runs;
- stage fault;
- endpoint fault;
- membership loss;
- provider/node failure;
- host-declared fatal dependency failure.

The orchestrator must emit or send teardown commands:

```text
RunCommand::StopRun for each provisioned stage
RunCommand::TearDownTokenEndpoints when token endpoints exist
ProvisionerMsg::StopNodes for provider-owned workers
```

Worker stage teardown is complete only after every expected `StageStopped` report is observed. Token endpoint teardown is complete only after token endpoint stopped state is observed. Provider teardown is complete only after `ProvisionerReport::NodesStopped` or a typed provider stop failure is observed.

`RunTornDown` is emitted once, after all required teardown facts are observed.

Cleanup is best effort for external resources. A successful actor stop sequence proves only that the actor-managed stop calls completed. It does not prove that a cloud provider or OS removed every external resource.

---

## 9. Error and Fault Model

Errors are reported through typed lifecycle reports, prompt events, provisioner reports, and datastream records.

### 9.1 Run Rejection

Run rejection occurs before provisioning. Examples:

- invalid typed run request;
- invalid committed plan;
- missing provisioner actor;
- missing required provider preparation result;
- missing model/tokenizer facts;
- unsupported provider policy;
- unsupported pipeline shape.

A rejected run must not provision workers.

### 9.2 Startup Fault

Startup fault occurs after a run is accepted but before prompt-ready. Examples:

- provisioning failure;
- worker exit before readiness;
- runtime-ready report mismatch for expected worker;
- membership never reaches alive state within policy;
- route owner never matches expected worker;
- runtime-ready acknowledgement timeout;
- stage provisioning send failure;
- stage fault while loading weights;
- provider failure before prompt-ready.

Startup fault triggers teardown for any started workers.

### 9.3 Prompt-Serving Fault

Prompt-serving fault occurs after prompt-ready. Examples:

- actor send failure to an active worker path;
- tokenizer actor send failure;
- tokenizer fault;
- token sequence violation;
- token record decode failure;
- worker exit during active serving;
- provider failure during active serving;
- membership loss for an active worker.

A prompt-local fault may be returned as `PromptEvent::Fault` without faulting the entire run when the run remains usable. A worker/runtime fault must fault the run.

### 9.4 Teardown Fault

Teardown fault occurs when actor-managed stop/cleanup reports a failure. The orchestrator must continue attempting remaining stop actions and preserve the first stop failure for reporting.

### 9.5 Secret Redaction

Secret values must not appear in orchestrator-owned lifecycle, prompt, or datastream records.

Secrets include:

- provider API keys;
- Hugging Face tokens;
- SSH private-key material;
- provider credentials;
- raw bearer/session tokens.

Secret presence may be reported as metadata. Secret values must not be copied.

---

## 10. Actor Message Filtering

Reports must match the active orchestration context before they can advance state.

Filtering rules:

- run-scoped reports must match `run_id`;
- worker reports must match an expected `node_id`;
- stage reports must match an expected `stage_index`;
- runtime-ready ack reports must match `readiness_id`;
- prompt events must match the active `request_id`;
- tokenizer events must match the active `request_id`;
- route ownership must match the worker endpoint node identity;
- membership facts must apply to the expected worker node identity.

Mismatched reports are ignored, dropped, or reported as diagnostic observations according to phase policy. They must not make the active lifecycle progress.

---

## 11. Datastream and Logs

Datastream is the structured observation path. It is not the control path.

The orchestrator actor/group may publish:

- run accepted/rejected/faulted/completed/torn-down records;
- planning records;
- provisioning request/result records;
- runtime readiness barrier records;
- route and membership observations;
- prompt accepted/dispatched/delta/completed/faulted records;
- stage provision/ready/fault/stopped records;
- shutdown progress records;
- provider/provisioner log records received from actor-managed leaves;
- worker datastream frames collected through datastream subscriptions.

Provider logs, worker stdout/stderr, managed-process lifecycle records, and dashboard frames are adapter-owned observations. They may be included in the orchestrator observation stream, but they do not become orchestrator actor inputs unless translated into typed actor reports by their owning actors.

Frame archive output, when enabled by the host, is a datastream subscriber. It does not own lifecycle state and must not affect runtime behavior when absent.

---

## 12. Verification Requirements

Conforming systems must be verified by behavior, not by matching a preferred internal file layout.

### 12.1 Boundary Checks

Boundary compliance assertions:

- the orchestrator actor does not define process exit status;
- prompt submission is actor message delivery, not TCP RPC;
- CLI/env/TOML parsing happens outside the actor contract;
- shutdown is not standard-input stop-word handling;
- the orchestrator actor does not own stdout/stderr;
- spawning the orchestrator actor does not execute a separate orchestrator binary;
- worker processes are supervised by host, provider, or process actors.

### 12.2 Engine-Agnostic Spawn Check

Start an engine with the reusable swactor + iroh-driver stack. Spawn the orchestrator actor/group into that engine, register its route, and drive only the generic pump:

```text
tick_protocol_actors
-> pump_inbound_to_actors
-> runtime tick/run progress
-> drain_outbox
-> datastream adapter progress when enabled
```

Assert the orchestrator accepts a typed run request and emits actor reports without executing a separate orchestrator binary or binding prompt RPC.

### 12.3 Actor Delivery Check

Send orchestrator messages locally and, where applicable, through the iroh actor bridge. Assert reports arrive through actor reply targets/inboxes. Remote delivery must depend on codec registration and route ownership, not on hardcoded address construction.

### 12.4 Run FSM Checks

Keep or extend black-box FSM checks:

- planning/provisioning starts only after pool/plan prerequisites;
- stage readiness and token endpoint readiness gate initial prompt injection;
- token feedback injects the next sequence only after consuming the previous sequence;
- EOS and max token limit stop generation;
- the first run fault is terminal and sticky;
- operator stop is terminal and distinct from fault;
- teardown emits stop commands and `RunTornDown` only after every stage and token endpoint stop is observed.

### 12.5 Readiness Checks

Verify prompt-ready is not emitted until:

- expected runtime-ready actor reports arrive;
- SWIM membership is alive for each worker endpoint node id;
- route owner for each node actor matches that worker node id;
- runtime-ready acknowledgements are observed;
- stage provisioning is sent;
- weights/stage readiness is observed.

Negative checks:

- TCP port availability must not make readiness true;
- datastream frames must not make readiness true;
- provider log lines must not make readiness true;
- mismatched run/node/stage/readiness reports must not advance readiness.

### 12.6 Provisioner Actor Checks

With a stub provider/provisioner leaf:

- `StartNodes` emits node start observations;
- node start failure reports `NodeFailed` and stops already-started nodes;
- plugin/adapter log observations emit `LogLine` and datastream log records;
- runtime-ready/bootstrap completion reports `NodeLive` only once;
- clean stop reports `NodesStopped`;
- stop failure preserves the first error while continuing stop attempts.

### 12.7 Direct Prompt Checks

Submit a direct prompt by actor message. Assert:

- one active prompt;
- `InferPrompt` is sent to the expected node actor;
- prompt events are sent to the request reply target;
- mismatched request ids are ignored/dropped;
- terminal `Done` or `Fault` occurs exactly once;
- active prompt state clears after terminal event.

### 12.8 Pipeline Prompt Checks

Submit a pipeline prompt by actor message. Assert:

- tokenizer encode request goes to the expected actor;
- encoded prompt enters token-in edge as sequence zero;
- generated token records from token-out are consumed in order;
- decode requests go to the expected actor;
- text deltas preserve request id;
- EOS and max token limit produce `Done`;
- tokenizer fault produces prompt `Fault`;
- token sequence violation faults prompt or run according to phase policy.

### 12.9 Shutdown Checks

Send actor shutdown. Assert:

- no stdin, OS signal, or process-group control is required;
- stage stop commands are emitted for every provisioned stage;
- token endpoint teardown is emitted when endpoints exist;
- provider/provisioner stop is requested;
- all stop reports are required before `RunTornDown`;
- stop failures are reported while remaining stops continue.

### 12.10 Datastream Non-Authority Checks

Simulate logs and frames. Assert:

- worker stdout/stderr/provider logs are recorded as observations;
- datastream frames can be archived or sent to dashboard sinks;
- logs/frames do not advance readiness, prompt completion, failure, or teardown;
- secret values are redacted from orchestrator-owned records.

### 12.11 Wrapper/Host Boundary Checks

For wrapper or host implementations that start an MVP run:

- spawning the orchestrator actor does not resolve or execute a separate orchestrator binary;
- readiness is actor/datastream lifecycle readiness, not TCP connect success;
- prompt submission is actor message delivery;
- shutdown is actor/control shutdown;
- worker processes, if used, are managed leaves and not the orchestrator execution boundary.

---

## 13. Out of Scope

Out of scope for this orchestrator actor contract:

- wrapper CLI UX;
- TOML/env parsing;
- Cargo artifact resolution;
- binary launch policy;
- process exit codes;
- prompt TCP compatibility adapters;
- OS signal handling;
- standard input command handling;
- stdout/stderr terminal behavior;
- actor scheduler internals;
- Tokio runtime lifecycle;
- iroh endpoint construction;
- QUIC/ALPN/stream internals;
- SWIM protocol internals beyond observed membership state;
- directory/registry internals beyond observed route ownership;
- datastream storage internals;
- dashboard rendering;
- provider marketplace semantics;
- Docker image construction;
- worker model execution internals;
- tokenizer correctness;
- model output quality;
- external resource cleanup guarantees after actor-managed stop requests complete;
- recovery after host/engine crash;
- multi-run orchestration in one actor group unless a later spec adds it.
