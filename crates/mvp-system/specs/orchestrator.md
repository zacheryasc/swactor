# MVP Orchestrator Fixed Specification

**Status:** draft outline for `mvp-orchestrator`.

This document will define the intended public and runtime contract for the MVP orchestrator. Section contents are intentionally left for section-by-section review.

---

## 1. Purpose and Contract Boundary

This specification defines the observable contract of `mvp-orchestrator`: what it accepts, what it emits, how it moves a runtime from launch to shutdown, and what behavior callers may rely on.

The orchestrator is responsible for turning a resolved launch request into a running MVP worker runtime. That responsibility includes:

- Resolving runtime configuration from fixed defaults, optional TOML config, environment variables, and process arguments.

- Preparing provider prerequisites that must exist before workers can start, including Vast.ai SSH identity registration when the Vast.ai provider is selected.

- Initializing the local orchestration runtime: Tokio, Iroh transport, the Swactor distribution stack, actor codecs, local reply actors, datastream production, and optional dashboard publication.

- Building the execution shape for the run. For direct execution this is a single worker stage. For planned execution this is a pipeline run plan derived from cached or locally inspectable model metadata.

- Provisioning worker nodes through the selected provider. The orchestrator supplies each worker with its image, logical node id, stage index, environment, mounts, coordinator endpoint, and orchestrator actor address.

- Driving runtime readiness. The orchestrator waits for worker runtime-ready reports, SWIM membership, actor route ownership, runtime-ready acknowledgements, stage provisioning, and weight-loaded reports before accepting prompt work.

- Serving prompt requests through prompt RPC. In direct mode it forwards prompt requests to the worker node agent. In pipeline mode it coordinates tokenizer encode/decode work and token-edge traffic between the orchestrator and pipeline stages.

- Emitting runtime observations. Bootstrap progress, prompt progress, provisioning events, provider logs, worker stdout/stderr, orchestrator stdio capture, SWIM transitions, stage-route checks, node datastream frames, dashboard frames, and optional frame-archive records are all orchestrator outputs.

- Shutting down controlled runtimes. On stop, shutdown, prompt-serving completion, or fatal error, the orchestrator stops provider-owned worker nodes before exiting when it has enough state to do so.

The orchestrator is not responsible for model quality, worker-node internals, tokenizer implementation details, provider implementation internals, Docker image construction, Vast.ai marketplace behavior, dashboard rendering semantics, or the interactive user-facing prompt loop owned by `mvp-chat`.

---

## 2. Input Channels

The orchestrator accepts input through configuration files, environment variables, process arguments, filesystem paths, prompt/control RPC, Swactor inbox delivery, provisioning datastream observations, and Iroh datastream connections.

Configuration inputs describe the requested runtime. Runtime inputs report what workers, providers, and network peers do after launch. Prompt text and RPC control inputs enter only through prompt/control RPC. Downstream shutdown propagation uses Swactor runtime and actor delivery, not standard input.

### 2.1 Configuration File

The orchestrator reads configuration from the path provided by `--runtime-config <path>` when that argument is present. Otherwise, it reads `.config/config.toml` if it exists.

The config file is optional when the default path is used and absent. A provided config path must exist, be readable, and contain valid TOML.

Accepted TOML configuration:

- `[runtime]`: `profile`, `run_id`, `node_id`, `stage_index`, `layer_end_exclusive`, `pipeline_stages`
- `[provider]`: `kind`
- `[image]`: `node`
- `[relay]`: `mode`, `url`
- `[prompt]`: `rpc_addr`
- `[model]`: `id`, `gguf_local_path`, `gguf_repo`, `gguf_file`, `gguf_revision`, `tokenizer_local_path`, `max_context`
- `[docker]`: `gpus`
- `[observability]`: `datastream_frame_log`
- `[vastai]`: `image`, `api_key`, `bootstrap_command`, `disk_gb`, `ssh_user`, `confirm_lease`, `onstart`, `ssh_identity`, `gpu_name`, `min_gpu_ram_mb`, `min_down_mbps`, `min_up_mbps`, `min_reliability`, `require_verified`, `poll_interval_secs`

TOML values are configuration inputs, not protocol messages. Unsupported values, malformed values, or invalid combinations are configuration errors.

### 2.2 Environment Variables

Environment variables provide secrets and tokens that should not be written into configuration files. Environment variables are not a general runtime configuration surface: runtime identity, provider selection, worker image, relay configuration, prompt serving, observability, model paths, and pipeline shape are configured through TOML or the accepted process arguments.

Accepted environment inputs:

- provider credentials:
  - `VASTAI_API_KEY`

- model and tokenizer credentials:
  - `HF_TOKEN`

`VASTAI_API_KEY` supplies the Vast.ai API key when the Vast.ai provider is selected. If both `[vastai].api_key` and `VASTAI_API_KEY` are present, `VASTAI_API_KEY` is used.

`HF_TOKEN` is passed only to runtime paths that need Hugging Face authentication for model or tokenizer access.

Unsupported `MVP_*` environment variables are not accepted orchestrator configuration inputs.

Environment values are parsed only when the selected runtime path needs them. Missing required secret values, malformed secret values, or unsupported environment configuration are configuration errors.

### 2.3 Process Arguments

Process arguments are a small launch-time convenience surface. Values not listed here must be configured through TOML.

Accepted process arguments:

- configuration:
  - `--runtime-config <path>`

- provider:
  - `--provider <value>`

- worker launch:
  - `--worker-bin <path>`

- runtime shape:
  - `--pipeline-stages <count>`

- Vast.ai:
  - `--vastai-ssh-identity <path>`

`--runtime-config` selects the TOML file used for this invocation. Relative paths are resolved against the current working directory.

The other accepted process arguments override the corresponding TOML values for this invocation only.

No process argument is accepted for runtime identity, model or tokenizer selection, relay settings, prompt serving, observability, Docker GPU settings, Vast.ai API keys, Vast.ai lease parameters, or Vast.ai search parameters.

Unknown arguments, missing argument values, unreadable config paths, invalid TOML, invalid numbers, unsupported provider names, unsupported runtime profiles, and invalid paths required by the selected runtime are configuration errors.


### 2.4 Filesystem Inputs

Filesystem inputs are paths that the orchestrator reads or validates while preparing the runtime:

- default or configured TOML configuration path
- current executable path, used to derive the default worker binary path
- configured worker binary path
- configured cached model host path
- default pipeline cached model path
- configured local GGUF path
- configured local tokenizer path
- configured Vast.ai SSH identity path
- current working directory for relative paths
- model cache paths exposed to workers

A missing file is an error only when the selected runtime path requires that file.

### 2.5 Prompt and Control RPC Input

Prompt/control RPC input is newline-delimited JSON over TCP.

Prompt request datatype:

```text
SubmitPrompt {
    request_id: u64,
    prompt_text: String,
    max_tokens: Option<u32>,
}
```

`max_tokens` is optional. When omitted or set to `0`, the orchestrator does not impose an arbitrary generated-token cap. Generation still ends on model EOS, context/window exhaustion, worker fault, client disconnect, shutdown, or runtime/model limits. When positive, `max_tokens` is the maximum number of generated completion tokens the orchestrator permits for that request.

The RPC channel may also carry a shutdown control request. After accepted, downstream shutdown signals sent to actors, providers, and worker-runtime components are delivered through the Swactor runtime and actor planes.

A valid request is enqueued as prompt work. A malformed request is a prompt RPC protocol error for that connection.

A prompt RPC TCP connection supports at most one in-flight `SubmitPrompt` request
at a time. The client must not submit another prompt on the same connection until
the previous request has produced a terminal `Done` or `Fault` event. Submitting
a second prompt before the active prompt reaches a terminal event is a prompt RPC
protocol error for that connection.

### 2.6 Actor Message Input Channel

Actor-message input reaches the orchestrator through two surfaces:

- **Iroh actor plane surface**: remote actor messages arrive through the Iroh actor bridge and are handled by the Swactor runtime and registered actor routes.

- **Swactor inbox surface**: process-visible actor messages are delivered into local Swactor inboxes owned and drained by the orchestrator process.

This section defines the input surfaces only. Actor message schemas belong in the `Actors` section.

### 2.7 Provisioning Datastream Observations

Provisioner actor subsystems publish node lifecycle and log observations as datastream records. The orchestrator receives these records through datastream ingestion, not through a provider plugin control surface.

Observation payload datatype:

```text
PluginObservation
```

Accepted datastream observation kinds:

- worker stdout line
- worker stderr line
- provider log line
- provider datastream frame
- node exited
- provisioning failed

Node exit and provisioning-failed observations in this channel are datastream records only. They do not drive lifecycle decisions by themselves. Startup, prompt-serving, and shutdown failures are driven by actor-plane reports, provider actor results, prompt/control RPC shutdown, or runtime state checks.

### 2.8 Datastream Input

Worker datastream input arrives over the datastream ALPN.

Accepted datastream inputs:

- stream header
- channel declaration
- frame
- stream end

Frame payloads are opaque bytes at this boundary. The orchestrator records channel name, channel id, stream id, frame position, and payload, then forwards the frame to configured sinks.

---

## 3. Output Channels

The orchestrator emits output through process status, stdio log streams, provisional prompt output, Swactor actor delivery, Iroh transport, datastream frames, optional dashboard publication, and optional frame archives.

This section defines output channels only. Detailed actor message schemas belong in the `Actors` section. Detailed datastream record contents belong in `Datastream and Logs`.

### 3.1 Process Exit Status

The orchestrator exits with process status:

```text
0
```

for successful completion or controlled shutdown.

The orchestrator exits with process status:

```text
1
```

for configuration failure, startup failure, provisioning failure, prompt-serving failure, provider-stop failure, or any other fatal orchestrator error.

### 3.2 Standard Output and Standard Error

Standard output and standard error are log streams only. They are not user-facing status protocols, report channels, control channels, signaling channels, or fatal-error reporting contracts.

On Linux, after stdio capture is installed, orchestrator stdout and stderr lines are redirected into the orchestrator log/datastream path as log observations. Before capture is installed, any stdout or stderr bytes are still logs only and are not part of the orchestrator contract.

Provisioner and provider logs are owned by their managing actor subsystems and enter the orchestrator datastream through those subsystem publications.

Structured runtime state, lifecycle progress, failures, prompt progress, worker logs, provider logs, and datastream frames are emitted through datastream, RPC, or actor-plane outputs, not through standard output or standard error.

### 3.3 Prompt RPC Output

Prompt RPC output is provisional and intentionally not specified here.

The prompt interface is being moved from RPC/TCP response streams to Swactor messaging. This section will be rewritten with the Swactor prompt output contract when that migration is specified.

### 3.4 Swactor Actor Output Channel

The orchestrator has one actor-output surface: its owned local Swactor runtime.

All actor messages emitted by the orchestrator process are submitted to that runtime. Swactor owns routing. If the destination is local, the runtime routes locally. If the destination is remote, the runtime uses the Iroh actor bridge.

The orchestrator does not maintain separate local and remote actor outboxes.

Process-owned Swactor inboxes may be supplied as reply addresses for reports, prompt events, tokenizer events, or other actor responses. Those inboxes are input queues drained by the orchestrator process; they are not a second actor-output channel.

Outgoing actor traffic includes:

- runtime-ready acknowledgements to worker node agents;
- stage provisioning commands;
- direct prompt inference commands;
- tokenizer encode requests;
- tokenizer decode requests;
- datastream subscription requests;
- shutdown or stop commands to managed runtime actors.

This section defines the actor-output surface only. Message schemas belong in the `Actors` section.




### 3.9 Datastream Output

The orchestrator datastream is the process-owned observation stream for a single orchestrator run.

The orchestrator datastream is responsible for collecting:

- records produced directly by the orchestrator process;
- captured orchestrator stdout and stderr, converted into log records;
- datastreams published by provider/provisioner actor subsystems;
- datastreams published by provisioned worker processes and managed nodes.

Datastream records are observational. They do not carry lifecycle authority, actor commands, provider control, shutdown control, prompt control, or readiness gates. Lifecycle decisions are driven through the Swactor actor/control plane and runtime state checks.

The orchestrator directly owns these datastream channels:

- `mvp.orch.bootstrap`
  - Orchestrator startup, configuration resolution, runtime initialization, readiness waiting, prompt-service availability, shutdown progress, and process-exit observations.

- `mvp.orch.prompt`
  - Prompt-serving observations owned by the orchestrator process, such as prompt accepted, prompt dispatched, prompt completed, prompt faulted, or prompt cancelled.

- `mvp.orch.stdio.stdout`
  - Captured stdout lines emitted by the orchestrator process after stdio capture is installed.

- `mvp.orch.stdio.stderr`
  - Captured stderr lines emitted by the orchestrator process after stdio capture is installed.

- `mvp.swim.membership`
  - SWIM membership observations visible to the orchestrator runtime.

- `mvp.orch.stage_route`
  - Stage route and actor-route ownership observations used to explain readiness and routing state.

The orchestrator datastream also collects downstream datastream publications. These channels are not directly authored by the orchestrator process, but they must be published into the orchestrator datastream for observation:

- provisioning lifecycle event channels published by provider/provisioner actor subsystems;
- provisioning log channels published by provider/provisioner actor subsystems;
- worker log channels published by provisioned processes or managed nodes;
- worker-defined datastream channels declared by provisioned processes or managed nodes;
- provider-defined diagnostic channels published by provider/provisioner actor subsystems.

Collected downstream frames preserve their source identity, stream identity, channel name, channel id, frame position, and payload bytes. The orchestrator may add collection metadata, but it must not rewrite downstream payloads into prompt output, lifecycle control, or actor messages.

Provisioning events and logs are managed by provider/provisioner actor subsystems. The orchestrator datastream is responsible for collecting and publishing those records as observations, not for interpreting them as control signals.

Worker and node datastream frames collected by the orchestrator are forwarded to configured observability sinks, such as datastream subscribers, dashboard subscribers, or frame archives.


### 3.11 Filesystem Output

Filesystem output is optional and exists only when datastream frame logging is configured.

When enabled, the orchestrator starts a file-log sink task. That task subscribes to the orchestrator datastream endpoint and appends received datastream frames to the configured file.

The file-log sink is a datastream subscriber. It does not own lifecycle state, does not emit control signals, and does not change runtime behavior when absent.

Frame archive output is JSON lines appended to the configured path.

Frame archive records include:

```text
FrameArchiveRecord {
    arrival_seq,
    source,
    stream,
    channel,
    channel_id,
    position,
    payload,
}
```

The file-log sink may create parent directories for the configured frame archive path.

Without datastream frame logging, the file-log sink is not started and no frame archive is created.


---

## 4. Runtime Lifecycle

The orchestrator lifecycle is a single owned run: resolve configuration, initialize local runtime services, provision workers, wait for readiness, serve prompts, stop workers, and exit.

Lifecycle summary:

```text
configure
-> prepare provider prerequisites
-> initialize observability
-> initialize local runtime
-> build optional run plan
-> provision workers
-> wait for runtime readiness
-> acknowledge runtime readiness
-> provision stages
-> wait for weights loaded
-> bind prompt RPC
-> serve prompts
-> stop provider-owned workers
-> exit
```

A fatal error may terminate the lifecycle at any phase. Once worker handles are owned by the provisioned-cluster guard, dropping the guard attempts to stop all remaining workers.

### 4.1 Configuration Phase

The orchestrator first resolves its effective configuration from defaults, optional TOML, environment variables, and process arguments.

This phase also validates configuration that must be known before runtime setup, including provider kind, prompt RPC bind address, pipeline stage count, cached model path, relay configuration, and Vast.ai-specific requirements.

If the selected provider is Vast.ai, the orchestrator prepares the SSH identity before installing stdio capture. Preparation includes resolving the identity path, deriving the public key, ensuring the key is registered with the Vast.ai account, and recording the prepared identity in the resolved provider config.

Failure in this phase exits before workers are started.

### 4.2 Observability Startup Phase

The orchestrator installs its own stdout/stderr capture and creates the orchestrator datastream.

The first bootstrap records describe the resolved configuration and runtime setup progress. If frame archive logging is configured, the frame archive is opened during datastream initialization.

From this point forward, ordinary orchestrator stdout and stderr lines are captured as log records rather than treated as terminal UI.

### 4.3 Local Runtime Initialization Phase

The orchestrator initializes the local runtime services required to coordinate the cluster:

- Tokio runtime;
- Iroh driver;
- distribution runtime stack;
- actor codecs;
- actor bridge;
- datastream collector;
- optional dashboard support;
- local Swactor inboxes for orchestrator reports, prompt replies, and tokenizer replies;
- local orchestrator actor;
- stop-listener thread for standard-input control.

The Iroh driver and Swactor runtime are pumped together throughout later phases. Actor messages, transport traffic, SWIM state, route ownership, datastream connections, and prompt work only progress while the orchestrator pump loop is running.

### 4.4 Run Planning Phase

The orchestrator enters planned execution when either cached-model execution is selected or more than one pipeline stage is requested.

In planned execution, the orchestrator reads locally inspectable GGUF metadata and builds a run plan. The plan determines stage placement, layer ranges, token edges, activation edges, object sizes, and ring sizes.

In direct execution, no run plan is built. The runtime is treated as a single worker stage.

A planning failure exits before workers are started.

### 4.5 Worker Provisioning Phase

The orchestrator builds a provider-specific provisioner and computes one `NodeProvisionSpec` per worker.

Direct execution provisions one worker.

Planned pipeline execution provisions one worker per planned stage.

For each worker, the orchestrator emits provider-start progress, calls the provider plugin, captures provider observations, and stores the returned worker handle. If one worker fails to start, workers already started in that provisioning attempt are stopped before the error is returned.

Once all workers are started, the provisioned-cluster guard owns the worker handles.

### 4.6 Runtime Readiness Phase

After workers are started, the orchestrator waits for runtime readiness.

A worker is not considered ready when it merely reports its endpoint and actor addresses. The readiness barrier requires:

- a matching runtime-ready report for the active run;
- SWIM membership showing the worker node as alive;
- actor route ownership showing the node actor is reachable through that worker;
- no provider failure or premature node exit.

For planned pipeline execution, every expected stage worker must pass this readiness barrier.

For direct execution, the single expected worker must pass this readiness barrier.

### 4.7 Runtime-Ready Acknowledgement Phase

After readiness barriers pass, the orchestrator acknowledges each worker’s runtime-ready report.

The acknowledgement is sent to the worker node agent. The orchestrator also attempts to subscribe to the worker datastream publisher when the datastream publisher route is available.

Acknowledgements are retried until all expected acknowledgement reports arrive or the acknowledgement timeout expires.

Failure to receive required acknowledgements is a startup failure.

### 4.8 Stage Provisioning Phase

After worker readiness is acknowledged, the orchestrator provisions stages.

In direct execution, the orchestrator sends a single stage provisioning command to the worker node agent.

In planned pipeline execution, the orchestrator provisions stages from the run plan. Stage provisioning includes model identity, GGUF source, tokenizer source, layer range, stage index, stage count, inbound edge information, outbound edge information, object specs, ring specs, and consumer endpoint information.

Stage provisioning is complete only after the corresponding weight-loaded reports are observed.

### 4.9 Weight Loading Phase

After stage provisioning begins, the orchestrator waits for workers to report that weights are loaded.

Direct execution waits for the single configured stage.

Planned pipeline execution loads stages sequentially. The orchestrator sends or resends stage provisioning for the next unloaded pipeline stage, waits for its weight-ready report, then advances to the next stage.

A stage fault during weight loading is a startup failure.

A provider failure or worker exit during weight loading is a startup failure.

### 4.10 Prompt RPC Startup Phase

The prompt RPC listener is created only after workers are ready and weights are loaded.

When prompt RPC binds successfully, the orchestrator emits prompt-loop readiness. At that point external clients may submit prompt requests.

Prompt RPC bind failure is a startup failure.

### 4.11 Prompt Serving Phase

Prompt serving is the steady-state runtime phase.

During prompt serving, the orchestrator repeatedly:

- pumps Iroh and Swactor runtime work;
- drains provider observations;
- drains worker datastream frames;
- drains captured orchestrator stdio;
- checks for shutdown control input;
- accepts at most one active prompt request;
- forwards prompt work through direct or pipeline execution;
- streams prompt events back to the prompt RPC client.

In direct execution, prompt work is sent to the worker node agent as an inference command.

In planned pipeline execution, prompt work is encoded by the tokenizer actor, sent into the token-in edge, received from the token-out edge, decoded by the tokenizer actor, and streamed back as prompt RPC events.

Prompt serving continues until shutdown is requested or a fatal runtime error occurs.

### 4.12 Shutdown Phase

Shutdown begins when prompt serving returns successfully or with an error.

The orchestrator emits provider-stop progress and calls `stop_node` for every provisioned worker handle. Handles are stopped in guard-owned order until none remain. The first provider-stop error is preserved.

If prompt serving succeeded and provider stop succeeded, the orchestrator emits an orchestrator-exit success record and exits successfully.

If prompt serving failed, provider stop failed, or both failed, the orchestrator exits with failure after attempting worker cleanup.

### 4.13 Cleanup Guarantee

The orchestrator owns provider worker handles through `ProvisionedClusterGuard`.

The guard attempts to stop remaining workers on explicit shutdown and again on drop if any handles remain. This makes worker cleanup best-effort even when the lifecycle exits through an error path.

Cleanup is best-effort, not proof that external provider resources were removed. Provider failures during cleanup are reported when they occur through the explicit provider-stop path.

---

## 5. Core Runtime Dataflow

The orchestrator is a coordinator. It does not perform model inference itself. It transforms configuration into worker launch requests, worker reports into lifecycle decisions, prompt requests into actor or pipeline commands, and runtime observations into datastream/log outputs.

The core dataflow has five paths:

```text
configuration -> resolved runtime request -> worker provisioning

worker reports -> readiness/stage/prompt decisions

prompt RPC -> prompt execution -> prompt RPC events

worker/provider/orchestrator observations -> datastream/log sinks

shutdown request/error -> provider stop -> process exit
```

### 5.1 Runtime Pump

The runtime advances through an explicit pump loop.

Each pump cycle performs the same basic work:

```text
tick protocol actors
-> move inbound Iroh actor messages into Swactor
-> run local Swactor work once
-> drain Swactor outbound actor messages into Iroh
-> accept datastream connections
```

This pump is the coordination boundary between the local Swactor runtime and Iroh transport. Actor delivery, SWIM state, route ownership, datastream connection handling, and remote worker reports only progress while the orchestrator is pumping.

### 5.2 Configuration to Provisioning

Configuration data enters through TOML, environment variables, process arguments, and defaults.

The orchestrator resolves those inputs into one effective runtime request:

```text
defaults
-> optional TOML overlay
-> environment overlay
-> process argument overlay
-> Config
```

The resolved `Config` drives:

- provider selection;
- worker image selection;
- worker binary selection for process provider;
- Docker GPU configuration;
- relay configuration;
- prompt RPC bind address;
- model and tokenizer source selection;
- cached model selection;
- Vast.ai provisioning settings;
- observability settings.

For direct execution, the `Config` creates one `NodeProvisionSpec`.

For planned pipeline execution, the `Config` first creates a run plan, then creates one `NodeProvisionSpec` per planned stage.

### 5.3 Run Plan Dataflow

Planned execution starts from locally inspectable GGUF metadata.

```text
cached/local GGUF
-> planning metadata
-> model facts
-> run plan
-> stage provisioning wires
```

The run plan determines:

- stage count;
- worker node ids;
- stage indexes;
- layer ranges;
- token-in edge;
- activation edges;
- token-out edge;
- object specs;
- ring specs;
- consumer endpoints.

The run plan is used twice:

- before provisioning, to decide which workers to start;
- after readiness, to derive the stage provisioning commands sent to workers.

Direct execution skips this path.

### 5.4 Provisioning Dataflow

Worker provisioning flows from the orchestrator to the selected provider plugin.

```text
Config / RunPlan
-> NodeProvisionSpec
-> ProvisionPlugin::start_node
-> worker runtime
-> PluginObservation / OrchestratorReport
```

The `NodeProvisionSpec` carries the data the provider needs to start a worker:

```text
NodeProvisionSpec {
    run_id,
    node_id,
    stage_index,
    image,
    env,
    args,
    mounts,
}
```

The provider returns a worker handle. The orchestrator stores that handle in the provisioned-cluster guard.

After the worker has passed readiness acknowledgement, the orchestrator calls `complete_bootstrap` for that handle.

During shutdown, the same handle is passed to `stop_node`.

### 5.5 Worker Readiness Dataflow

Worker readiness reaches the orchestrator through actor reports.

```text
worker node agent
-> orchestrator actor
-> orchestrator report inbox
-> readiness wait loop
```

A runtime-ready report contains the worker endpoint, node actor address, datastream publisher address, stage index, and readiness id.

The orchestrator does not treat the report alone as sufficient. It combines the report with local runtime state:

```text
runtime-ready report
+ SWIM member is alive
+ actor route owner matches worker
= worker ready
```

After that barrier passes, the orchestrator sends runtime-ready acknowledgement back to the worker node agent.

### 5.6 Stage Provisioning Dataflow

Stage provisioning flows from the orchestrator to worker node agents after readiness acknowledgement.

Direct execution:

```text
Config
-> StageProvisionWire
-> NodeAgentMsg::ProvisionStage
-> worker loads stage
-> WeightsReady or StageFault report
```

Planned execution:

```text
RunPlan + worker readiness map
-> StageProvisionWire for stage N
-> NodeAgentMsg::ProvisionStage
-> worker loads stage N
-> WeightsReady or StageFault report
-> next stage
```

Pipeline stages are loaded sequentially. The orchestrator advances to the next unloaded stage only after the active stage reports weights ready.

### 5.7 Prompt Dataflow: Direct Mode

Direct prompt execution is used when no planned pipeline runtime is active.

```text
prompt RPC SubmitPrompt
-> PromptWork queue
-> serve_prompts loop
-> NodeAgentMsg::InferPrompt
-> worker prompt execution
-> PromptEvent inbox
-> prompt RPC response stream
```

The orchestrator accepts at most one active prompt request at a time.

Prompt events whose request id does not match the active prompt are dropped from the active prompt flow.

Terminal prompt events clear the active prompt and allow the next prompt request to begin.

### 5.8 Prompt Dataflow: Pipeline Mode

Pipeline prompt execution is used for planned pipeline runtimes.

```text
prompt RPC SubmitPrompt
-> PromptWork queue
-> tokenizer encode actor
-> token-in edge
-> pipeline stages
-> token-out edge
-> tokenizer decode actor
-> prompt RPC response stream
```

The pipeline prompt runtime keeps the active prompt state, generated token list, final text buffer, token sequence number, pending tokenizer encode state, and pending tokenizer decode state.

Prompt text is first sent to the tokenizer encode actor.

Encoded tokens are sent to the first pipeline stage over the token-in edge.

Generated token records return over the token-out edge.

Each generated token is sent to the tokenizer decode actor.

Decoded text is emitted to the prompt RPC client as `TextDelta`.

The prompt completes when the pipeline reports EOS or when the request reaches its max-token limit.

### 5.9 Observability Dataflow

Observability has three sources:

```text
orchestrator internal events
provider observations
worker datastream frames
```

Orchestrator internal events become bootstrap or prompt datastream records.

Provider observations become provisioning events or provisioning log records.

Worker datastream frames are collected over the datastream ALPN.

Configured observability sinks receive frames from these sources:

```text
orchestrator datastream
-> dashboard sink, if enabled
-> frame archive, if configured

worker datastream
-> dashboard sink, if enabled
-> frame archive, if configured
```

Prompt RPC does not receive observability frames. Prompt RPC receives only prompt events.

### 5.10 Shutdown Dataflow

Shutdown begins from either control input or a fatal lifecycle result.

```text
stdin stop/shutdown/quit
or prompt-serving error
or provider/runtime failure
-> serve_prompts returns
-> provider_stop starts
-> stop_node for each worker handle
-> process exit
```

The provisioned-cluster guard owns cleanup. Explicit shutdown drains the guard by calling `stop_node` for each worker handle. If the guard is dropped with handles still present, it attempts best-effort cleanup.

The process exit result is computed from prompt-serving result and provider-stop result.

---

## 6. Actors

Actors are the orchestrator control plane. They carry runtime state transitions, worker commands, prompt commands, tokenizer commands, and readiness reports.

Actor messages are distinct from prompt RPC records, provider plugin observations, datastream frames, worker stdout/stderr logs, and pipeline token-edge bytes.

Actor delivery is asynchronous. Messages only make progress while the orchestrator pumps the Swactor runtime and Iroh driver.

### 6.1 Actor Runtime

The orchestrator creates a local Swactor runtime as part of the distribution runtime stack.

The runtime is connected to Iroh through the actor bridge:

```text
Swactor runtime
<-> actor bridge
<-> Iroh driver
<-> remote worker node actors
```

Local actor messages stay inside the process.

Remote actor messages are serialized with registered codecs and delivered through the Iroh actor bridge.

Actor route availability is part of worker readiness. A worker node is not ready for orchestration until the route owner for its node actor matches the worker’s Iroh node identity.

The actor runtime has these operational states from the orchestrator’s perspective:

- **initializing**: codecs, local actors, and actor bridge are being registered;
- **pumping**: inbound Iroh messages, local actor work, and outbound actor messages are advanced by the orchestrator pump loop;
- **stopped by process exit**: actor progress ends when the orchestrator process exits.

There is no independent actor scheduler contract outside the orchestrator pump loop.

### 6.2 Actor Addresses

Actors are addressed by `ActorAddress`.

The orchestrator uses actor addresses for:

- the local orchestrator actor;
- the local orchestrator report inbox;
- the local prompt reply inbox;
- the local tokenizer reply inbox;
- each worker node agent actor;
- each worker datastream publisher actor.

Worker actor addresses are learned from runtime-ready reports. The orchestrator does not construct remote worker actor addresses by convention.

An actor address becomes usable for remote delivery only after the route view reports that the address is owned by the expected worker node.

### 6.3 Local Inbox Actors

The orchestrator creates local inbox actors for receiving reports and replies.

Local inbox actors are queue endpoints. Their state is the set of messages not yet drained by the orchestrator loop.

The local inbox actors are:

- **orchestrator report inbox**  
  Holds `OrchestratorReport` values emitted by the orchestrator actor.

- **prompt reply inbox**  
  Holds `PromptEvent` values emitted by direct prompt inference.

- **tokenizer reply inbox**  
  Holds `TokenizerEvent` values emitted by tokenizer encode/decode work in pipeline mode.

Inbox states are:

- **empty**: no pending messages;
- **pending**: one or more messages are queued;
- **drained**: the orchestrator loop has consumed all currently available messages.

Inbox actors do not own lifecycle decisions. They only buffer messages until the orchestrator loop drains them.

### 6.4 Orchestrator Actor

The orchestrator actor wraps the run-level FSM.

It receives `OrchestratorMsg` actor messages and may emit `OrchestratorReport` actor messages to the orchestrator report inbox.

State owned by the orchestrator actor:

```text
OrchestratorActor {
    core: OrchestratorRun,
    report_to: Option<ActorAddress>,
    command_cursor,
    event_cursor,
}
```

The `OrchestratorRun` state includes:

```text
OrchestratorRun {
    config,
    plan,
    pool_ready,
    provisioned,
    token_in_ready,
    token_out_ready,
    ready_stages,
    injected_sequences,
    expected_token_sequence,
    events,
    commands,
    terminal,
    teardown_started,
    stopped_stages,
    token_endpoints_stopped,
}
```

Logical states:

- **waiting for plan and pool**: the actor has a run config but cannot provision until required plan/pool facts are observed.
- **provisioned**: the FSM has emitted or recorded stage provisioning intent.
- **waiting for token endpoints**: the actor has not yet observed both token-in and token-out endpoint readiness.
- **waiting for stage readiness**: the actor is collecting ready stage indexes.
- **generating tokens**: the actor tracks injected token sequences and expected returned token sequence.
- **terminal**: the actor has observed run completion or a run fault. Further token receipt does not advance generation.
- **tearing down**: the actor has begun teardown and waits for stage stops and token endpoint teardown.
- **torn down**: all required stopped-stage and token-endpoint stopped facts have been observed.

Incoming actor messages to `OrchestratorActor` are variants of `OrchestratorMsg`.

Common incoming observations:

```text
OrchestratorMsg::ObserveNodeRuntimeReady { ... }
OrchestratorMsg::ObserveNodeRuntimeReadyAck { ... }
OrchestratorMsg::ObserveWeightsReady { ... }
OrchestratorMsg::ObserveStageReady { ... }
OrchestratorMsg::ObserveStageFault { ... }
OrchestratorMsg::ObserveStageStopped { ... }
OrchestratorMsg::ObserveTokenReceived { ... }
OrchestratorMsg::ObserveEndpointFault { ... }
OrchestratorMsg::ObserveTokenEndpointsStopped
```

These are typed actor messages delivered by Swactor. When sent by a remote worker, delivery passes through the Iroh actor bridge.

Reports emitted by `OrchestratorActor` are variants of `OrchestratorReport`.

Common report messages:

```text
OrchestratorReport::NodeRuntimeReady { ... }
OrchestratorReport::NodeRuntimeReadyAck { ... }
OrchestratorReport::WeightsReady { ... }
OrchestratorReport::StageReady { ... }
OrchestratorReport::StageFault { ... }
OrchestratorReport::Command(...)
OrchestratorReport::Lifecycle(...)
```

These are typed actor messages sent by `OrchestratorActor` to the local orchestrator report inbox. The orchestrator process drains that inbox and uses the reports to advance startup, provisioning, weight-loading, and failure handling.

The orchestrator binary uses direct readiness, acknowledgement, weight, and fault reports as lifecycle gates. FSM command and lifecycle reports remain part of the actor surface, but they are not the primary startup gates in the current binary.

### 6.5 Worker Node Agent Actor

Each worker has a node agent actor.

The node agent actor is the orchestrator’s primary control target on a worker. It receives stage provisioning, prompt, tokenizer, and readiness acknowledgement commands.

State owned by the node agent actor:

```text
NodeAgentActor {
    core: StageController,
    orchestrator: ActorAddress,
    report_to: Option<ActorAddress>,
    inbound_edge: Option<StageInboundEdgeWire>,
    outbound_edge: Option<StageOutboundEdgeWire>,
    command_cursor,
    event_cursor,
}
```

The node agent’s `StageController` state includes:

```text
StageController {
    provision,
    worker_ready,
    weights_ready,
    inbound_ready,
    outbound_ready,
    stage_ready_emitted,
    busy,
    expected_sequence,
    active_input,
    commands,
    events,
    faulted,
    stopped,
    stopping_run,
    local_edges_stopped,
    worker_rings_quiesced,
    release_reset_requested,
    device_objects_released,
    worker_role_reset,
}
```

Logical states:

- **unprovisioned**: no stage provision has been accepted.
- **provisioning**: a valid `ProvisionStage` message has been accepted. The stage controller has emitted commands to establish inbound edge, establish outbound edge, configure worker role, and load weights.
- **waiting for stage readiness**: the stage has provision data but has not yet observed every readiness prerequisite.
- **ready idle**: worker ready, weights ready, inbound edge ready, and outbound edge ready have all been observed. `StageReady` has been emitted. No step is active.
- **busy executing step**: a valid inbound object has arrived with the expected sequence. The controller has emitted an execute-step command and is waiting for completion or failure.
- **faulted**: the actor has observed an unauthorized provision, sequence violation, worker crash, step failure, object failure, output fault, or edge fault. Faulted stages do not accept new execution work.
- **stopping**: a stop has been requested for the run. The actor has emitted stop/release/reset commands and waits for local edges, worker rings, device objects, and worker role reset facts.
- **stopped**: all stop prerequisites have been observed and `StageStopped` has been emitted.

Important orchestrator-sent messages:

```text
NodeAgentMsg::RuntimeReadyAck { ... }
NodeAgentMsg::ProvisionStage(StageProvisionWire)
NodeAgentMsg::InferPrompt { ... }
NodeAgentMsg::EncodePrompt { ... }
NodeAgentMsg::DecodeTokens { ... }
```

Important worker-side or stage-side observations accepted by the node agent:

```text
NodeAgentMsg::RuntimeLoaded { ... }
NodeAgentMsg::MarkWeightsReady { ... }
NodeAgentMsg::MarkInboundEdgeReady { ... }
NodeAgentMsg::MarkOutboundEdgeReady { ... }
NodeAgentMsg::ObjectLoaded { ... }
NodeAgentMsg::StepCompleted { ... }
NodeAgentMsg::WorkerCrashed
NodeAgentMsg::StopRun { ... }
NodeAgentMsg::LocalEdgesStopped { ... }
NodeAgentMsg::WorkerRingsQuiesced { ... }
NodeAgentMsg::DeviceObjectsReleased { ... }
NodeAgentMsg::WorkerRoleReset { ... }
```

Important outputs to the orchestrator actor:

```text
OrchestratorMsg::ObserveNodeRuntimeReady { ... }
OrchestratorMsg::ObserveNodeRuntimeReadyAck { ... }
OrchestratorMsg::ObserveWeightsReady { ... }
OrchestratorMsg::ObserveStageReady { ... }
OrchestratorMsg::ObserveStageFault { ... }
OrchestratorMsg::ObserveStageStopped { ... }
```

These outputs are actor messages sent to `OrchestratorActor`, not datastream records or prompt RPC events.

### 6.6 Stage Provision Message

Stage provisioning is carried by `StageProvisionWire`.

```text
StageProvisionWire {
    run_id,
    authorized_orchestrator,
    node_id,
    stage_index,
    stage_count,
    layer_start,
    layer_end_exclusive,
    inbound_edge_id,
    outbound_edge_id,
    inbound_edge,
    outbound_edge,
    model_id,
    gguf_source,
    tokenizer,
}
```

For direct execution, inbound and outbound edge details may be absent.

For planned pipeline execution, inbound and outbound edge details describe the token or activation edge assigned by the run plan.

A node agent accepts a stage provision only when:

- `authorized_orchestrator` matches the expected orchestrator identity;
- `node_id` matches the local worker node id.

Invalid provisioning faults the stage.

### 6.7 Datastream Publisher Actor

Each worker reports a datastream publisher actor address.

The datastream publisher actor accepts:

```text
DatastreamPublisherMsg::Subscribe(DatastreamSubscribe)
```

Subscription request shape:

```text
DatastreamSubscribe {
    collector,
    request,
    flow_id,
    token,
}
```

State owned by the publisher actor:

```text
DatastreamPublisherActor {
    endpoint,
    on_subscribe,
}
```

Logical states:

- **waiting for subscription**: the actor owns a datastream endpoint and waits for subscribe messages.
- **subscription accepted**: a subscribe message has been accepted. The actor creates a local datastream subscription from the endpoint and passes it to the transport-specific `on_subscribe` callback.

The publisher actor does not itself stream bytes. It creates the subscription and hands it to transport code that writes datastream frames.

### 6.8 Provisioner Actor

The codebase defines a `ProvisionerActor`, but the orchestrator binary covered by this spec provisions workers directly through `ProvisionPlugin`.

Therefore, the `ProvisionerActor` is not part of the current orchestrator runtime lifecycle.

If a future orchestrator path uses it, its state and message contracts must be specified before it becomes part of this document’s active contract.

### 6.9 Prompt Actor Flow

Direct prompt mode uses actor messages for worker prompt execution.

```text
SubmitPrompt
-> PromptWork
-> NodeAgentMsg::InferPrompt
-> worker
-> PromptEvent
-> prompt reply inbox
-> prompt RPC stream
```

State involved in this flow:

- prompt-serving loop tracks the active prompt;
- prompt reply inbox buffers `PromptEvent`;
- node agent actor receives `InferPrompt`;
- worker prompt implementation produces prompt events.

The `reply_to` address in `InferPrompt` is the local prompt reply inbox.

Prompt events with a mismatched request id are dropped from the active prompt flow.

### 6.10 Pipeline Tokenizer Actor Flow

Pipeline prompt mode uses actor messages for tokenizer work and edge transport for generated tokens.

```text
SubmitPrompt
-> PromptWork
-> NodeAgentMsg::EncodePrompt
-> TokenizerEvent::PromptEncoded
-> token-in edge
-> token-out edge
-> NodeAgentMsg::DecodeTokens
-> TokenizerEvent::TokensDecoded
-> prompt RPC stream
```

State involved in this flow:

- pipeline prompt runtime tracks active prompt state;
- tokenizer reply inbox buffers `TokenizerEvent`;
- encode actor receives `EncodePrompt`;
- decode actor receives `DecodeTokens`;
- token-edge transport carries generated token records;
- prompt RPC stream receives decoded text.

The encode actor is the first-stage node actor.

The decode actor is the final-stage node actor.

The `reply_to` address for tokenizer messages is the local tokenizer reply inbox.

Tokenizer faults become prompt faults for the active request.

### 6.11 Actor Delivery Contracts

Actor send failure is a runtime error for the phase that attempted the send.

Actor delivery is not synchronous execution. A successful send means the message was accepted by the local runtime for delivery, not that the remote worker has acted on it.

Remote actor delivery requires:

- Iroh transport progress;
- actor bridge progress;
- route availability;
- SWIM membership state sufficient for route ownership.

The orchestrator must keep pumping runtime and transport while waiting for actor-driven results.

### 6.12 Actor Message Filtering

Actor reports are accepted only when they match the active orchestration context.

Lifecycle reports are filtered by `run_id`.

Worker readiness reports are filtered by expected `node_id`.

Stage reports are filtered by expected `stage_index`.

Prompt and tokenizer events are filtered by active `request_id`.

Mismatched reports do not advance the active lifecycle or prompt state.

---

## 7. Subcomponents and Behaviors

This section describes the in-process subcomponents that implement orchestration behavior.

Actors are covered in `Actors`. Datastream record schemas are covered in `Datastream and Logs`. This section focuses on non-actor runtime components and the state they own.

### 7.1 Configuration Resolver

The configuration resolver turns defaults, TOML, environment variables, and process arguments into one `Config`.

Owned state:

```text
Config {
    config_profile: RuntimeConfigProfile,
    provider: ProviderKind,
    image: String,
    docker_gpus: String,
    rpc_bind: SocketAddr,
    run_id: u64,
    node_id: u64,
    stage_index: u32,
    layer_end_exclusive: Option<u32>,
    pipeline_stages: u32,
    model_id: String,
    gguf_source: GgufSource,
    tokenizer: TokenizerSource,
    default_max_tokens: u32,
    dashboard: bool,
    max_context: Option<u32>,
    relay: RelayRuntimeConfig,
    vastai: Option<VastAiRuntimeConfig>,
    cached_model: Option<CachedModelConfig>,
    worker_bin: Option<PathBuf>,
    datastream_frame_log: Option<PathBuf>,
}
```

Defaults:

- `config_profile` defaults to `Local`.
- `provider` defaults from `config_profile`:
  - `Local` uses `Process`.
  - `Deploy` uses `VastAi`.
- `image` defaults to `swactor-mvp-node:latest`.
- `docker_gpus` defaults to `all`.
- `rpc_bind` defaults to `127.0.0.1:19777`.
- `run_id` defaults to `1`.
- `node_id` defaults to `1`.
- `stage_index` defaults to `0`.
- `layer_end_exclusive` defaults to absent.
- `pipeline_stages` defaults to `1`.
- `default_max_tokens` defaults to `64`.
- `dashboard` defaults to disabled.
- `max_context` defaults to absent.
- `vastai` defaults to absent unless the resolved provider is `VastAi`.
- `cached_model` defaults to absent.
- `worker_bin` defaults to absent.
- `datastream_frame_log` defaults to absent.

`model_id`, `gguf_source`, and `tokenizer` are resolved model inputs. This section records their typed presence in `Config`, but does not define a hardcoded default model contract.

Behavior:

- starts from fixed defaults;
- overlays optional TOML;
- overlays environment variables;
- overlays process arguments;
- validates provider-specific constraints;
- resolves relay configuration;
- resolves cached model configuration;
- rejects invalid pipeline stage counts;
- rejects unsupported provider/profile values;
- rejects malformed prompt RPC bind addresses.

The resolver is the only component that should interpret raw configuration strings. Later components receive typed runtime state.

### 7.2 Cached Model Resolver

The cached model resolver validates host-local cached model paths and converts them into worker-visible paths.

Owned state:

```text
CachedModelConfig {
    host_path,
    container_path,
}
```

Behavior:

- canonicalizes the host path;
- requires the host path to point to a file;
- derives a container path under the cached model container directory;
- exposes the host path to process workers;
- exposes the container path to Docker workers;
- enables planned execution when cached-model execution is selected.

Cached model paths are supported only for process and Docker providers.

### 7.3 Vast.ai Runtime Preparation

Vast.ai runtime preparation resolves provider-specific launch requirements before workers are started.

Owned state:

```text
VastAiRuntimeConfig {
    api_key,
    provisioning,
    bootstrap_command,
    ssh_identity,
    ssh_public_key,
    ssh_public_fingerprint,
}
```

Behavior:

- requires an API key when Vast.ai is selected;
- resolves the SSH identity path;
- verifies the SSH identity file exists;
- derives the public key from the identity;
- ensures the public key is registered with the Vast.ai account;
- records the prepared identity and public-key metadata into provider config;
- requires a bootstrap command before constructing the Vast.ai provisioner.

Failure in this component stops startup before worker provisioning begins.

### 7.4 Run Planner

The run planner is used only for planned execution.

Planned execution is selected when:

```text
cached_model is present
or pipeline_stages > 1
```

Behavior:

- reads locally inspectable GGUF metadata;
- converts metadata into model facts;
- rejects pipeline stage counts larger than model layer count;
- computes activation ring size;
- computes token ring size;
- assigns fixed linear stage placement;
- produces a `RunPlan`.

The run plan drives:

- number of workers;
- stage indexes;
- logical node ids;
- layer ranges;
- token-in edge;
- activation edges;
- token-out edge;
- object specs;
- ring specs;
- stage provisioning payloads.

Direct execution skips this component.

### 7.5 Provisioner Builder

The provisioner builder constructs the provider implementation used to start and stop workers.

Behavior by provider:

- **process**  
  Resolves the worker binary path and requires it to exist.

- **Docker**  
  Constructs a local Docker provisioner using the configured container name prefix.

- **Vast.ai**  
  Requires prepared Vast.ai config, API key, bootstrap command, and SSH identity; constructs a Vast.ai provisioning plugin backed by the Vast.ai client and SSH launcher.

The orchestrator does not use the `ProvisionerActor` in the current binary. It calls the selected `ProvisionPlugin` directly.

### 7.6 Provisioned Cluster Guard

The provisioned-cluster guard owns worker handles after successful provider startup.

Owned state:

```text
ProvisionedClusterGuard {
    provisioner,
    handles,
}
```

Behavior:

- stores each returned provider handle;
- calls `complete_bootstrap` for all handles after runtime-ready acknowledgement succeeds;
- calls `stop_node` for each handle during explicit shutdown;
- preserves the first stop error;
- attempts best-effort cleanup on drop if handles remain.

The guard is the ownership boundary for worker cleanup. Once a handle is in the guard, the orchestrator is responsible for attempting to stop it.

### 7.7 Runtime Stack and Iroh Driver

The runtime stack and Iroh driver provide transport, actor delivery, routing, SWIM membership, and datastream connection acceptance.

Owned state is split across:

- `IrohDriver`;
- `DistributionRuntimeStack`;
- route view;
- relay mirror;
- SWIM actor state;
- actor bridge routes;
- runtime outbox.

Behavior:

- registers actor and datastream codecs;
- enables the actor bridge;
- registers local actor routes;
- pumps inbound Iroh messages into actors;
- pumps local actor runtime work;
- drains outbound actor messages to Iroh;
- accepts datastream connections;
- exposes route owner and member state checks used by readiness barriers.

This subcomponent is not autonomous. It advances only when the orchestrator calls the pump function.

### 7.8 Prompt RPC Server

The prompt RPC server accepts external prompt submissions over TCP.

Owned state:

```text
Prompt RPC listener {
    bind_addr,
    work_tx,
    default_max_tokens,
}
```

Behavior:

- binds the configured prompt RPC address;
- spawns an accept loop;
- spawns one handler thread per accepted connection;
- reads newline-delimited `SubmitPrompt` JSON;
- applies default max tokens when request max tokens is zero;
- sends accepted work into the prompt work queue;
- writes newline-delimited `PromptEvent` JSON back to the client;
- stops writing for a request after `Done` or `Fault`.

Prompt RPC starts only after runtime readiness and weight loading have completed.

### 7.9 Prompt Serving Loop

The prompt serving loop is the steady-state coordinator after prompt RPC is ready.

Owned state:

```text
serve_prompts {
    active: Option<ActivePrompt>,
    optional pipeline runtime,
}
```

Behavior:

- pumps actor and transport runtime;
- drains provider observations;
- drains worker datastream frames;
- drains captured orchestrator stdio;
- checks for stop requests;
- accepts prompt work only when no prompt is active;
- sends direct prompt work to the worker node agent in direct mode;
- delegates prompt work to `PipelinePromptRuntime` in pipeline mode;
- forwards matching prompt events to the prompt RPC client;
- drops prompt events for non-active request ids;
- clears active prompt state on terminal prompt event.

The prompt serving loop enforces the one-active-prompt rule.

### 7.10 Pipeline Prompt Runtime

The pipeline prompt runtime coordinates prompt execution for planned pipeline mode.

Owned state:

```text
PipelinePromptRuntime {
    token_in_edge_id,
    token_out_edge_id,
    token_spec,
    token_out_spec,
    token_in_sender,
    recv_rx,
    recv_tx,
    recv_buffer,
    tokenizer_encode_actor,
    tokenizer_decode_actor,
    tokenizer_reply_to,
    pending_encode,
    pending_decode,
    next_sequence,
    generated_tokens,
    final_text,
    active,
    started_at,
}
```

Behavior:

- starts one active prompt;
- requests tokenizer encode for prompt text;
- sends encoded prompt tokens over the token-in edge;
- receives generated token records from the token-out edge;
- validates token sequence order;
- requests tokenizer decode for each generated token;
- emits text deltas to the prompt RPC client;
- appends decoded text to final text;
- stops on EOS or max-token limit;
- sends `Done` on successful completion;
- sends `Fault` on tokenizer failure or runtime error;
- clears active prompt state after terminal output.

The pipeline prompt runtime owns prompt-generation state, not worker stage state.

### 7.11 Pipeline Token Sender and Receiver

Pipeline token transport is handled by token sender, receiver, and acceptor helpers.

Behavior:

- token sender opens a unidirectional stream to the first stage endpoint;
- token sender writes encoded token-in records;
- token acceptor accepts incoming pipeline edge connections;
- token receiver reads bytes from accepted streams;
- received bytes are buffered until complete token records can be decoded.

The token transport carries bytes. The pipeline prompt runtime owns record sequencing and prompt semantics.

### 7.12 Orchestrator Datastream

The orchestrator datastream component emits runtime observations produced by the orchestrator itself.

Owned state:

```text
OrchDatastream {
    stream,
    endpoint,
    producer,
    channels,
    channel_names,
    archive,
}
```

Behavior:

- creates the orchestrator stream id;
- registers core channels;
- emits bootstrap records;
- emits prompt records;
- emits provisioning events;
- emits provisioning log records;
- emits arbitrary channel payloads from provider observations;
- flushes frames to dashboard if enabled;
- writes frames to the frame archive if configured.

The orchestrator datastream is the primary structured observation path for orchestrator-owned events.

### 7.13 Datastream Frame Archive

The frame archive records datastream frames to a JSON-lines file when configured.

Owned state:

```text
FrameArchive {
    file,
    next_seq,
}
```

Behavior:

- creates parent directories for the configured path when needed;
- opens the archive file in append mode;
- records frames with an arrival sequence;
- records source, stream, channel, channel id, position, and payload;
- encodes UTF-8 payloads as text;
- encodes non-UTF-8 payloads as bytes;
- flushes after each record.

Without configured datastream frame logging, this component is absent.

### 7.14 Orchestrator Stdio Capture

The stdio capture component redirects orchestrator stdout and stderr into provisioning log records.

Owned state:

```text
optional mpsc receiver of captured stdio lines
```

Behavior:

- on Linux, redirects stdout and stderr through pipes;
- spawns reader threads for captured stdout and stderr;
- converts captured lines into `OrchStdioLine`;
- drains captured lines into the orchestrator datastream as log records;
- on non-capturing targets, may be absent.

Captured orchestrator stdio is observability data. It is not a terminal UI contract.

### 7.15 Dashboard Support

Dashboard support is an optional sink for datastream frames.

Owned state:

```text
optional DashboardSupport
```

Behavior:

- starts only when dashboard support is enabled;
- receives frames from orchestrator datastream flushes;
- receives frames collected from worker datastream streams;
- publishes frames to the dashboard handle;
- does not affect runtime correctness when absent.

Dashboard output is derived from datastream frames and does not own lifecycle state.

### 7.16 Stop Listener

The stop listener watches standard input for shutdown control lines.

Owned state:

```text
mpsc receiver of stop notifications
```

Behavior:

- runs in a background thread;
- reads standard input line by line;
- trims each line;
- accepts `stop`, `shutdown`, or `quit` case-insensitively;
- sends one stop notification;
- causes wait loops or prompt serving to exit through controlled shutdown paths.

The stop listener is not a prompt input path.

---

## 8. Datastream and Logs

Datastream is the orchestrator’s structured observation path. Logs are represented as datastream records, not as terminal UI.

This section defines the datastream and log channels used by the orchestrator process. It does not define prompt RPC payloads, actor message schemas, or provider command protocols.

### 8.1 Datastream Model

A datastream is an ordered stream of frames.

Each frame has:

```text
Frame {
    channel,
    position,
    payload,
}
```

A channel gives meaning to the payload. The datastream transport itself treats payloads as opaque bytes.

The orchestrator uses datastream for:

- bootstrap progress;
- prompt progress;
- provisioning events;
- worker stdout/stderr logs;
- provider logs;
- captured orchestrator stdout/stderr logs;
- SWIM membership observations;
- stage route observations;
- worker-emitted datastream frames;
- dashboard publication;
- optional frame archive output.

Datastream records are observational. They do not drive prompt RPC response text, actor delivery, or provider lifecycle by themselves.

### 8.2 Orchestrator Datastream

The orchestrator creates its own datastream at startup.

Its stream identity is tied to the orchestrator and the active run id.

The orchestrator datastream owns:

```text
OrchDatastream {
    stream,
    endpoint,
    producer,
    channels,
    channel_names,
    archive,
}
```

The orchestrator datastream registers core channels, emits records into those channels, flushes produced frames to configured sinks, and records frames to the archive when frame logging is enabled.

### 8.3 Core Orchestrator Channels

The orchestrator emits these core channels:

```text
mvp.orch.bootstrap
mvp.orch.prompt
mvp.swim.membership
mvp.orch.stage_route
mvp.provisioning.events
mvp.provisioning.logs.node.<node_id>.stdout
mvp.provisioning.logs.node.<node_id>.stderr
mvp.provisioning.logs.node.<node_id>.provider
```

`mvp.orch.bootstrap` carries orchestrator lifecycle progress.

`mvp.orch.prompt` carries prompt-serving progress.

`mvp.swim.membership` carries observed membership transitions.

`mvp.orch.stage_route` carries route checks during pipeline stage provisioning.

`mvp.provisioning.events` carries node provisioning lifecycle events.

`mvp.provisioning.logs.node.<node_id>.<stream>` carries stdout, stderr, or provider log lines for a node id.

Provider-supplied datastream frames may create additional channels by name. Worker datastream frames may also use worker-defined channel names.

### 8.4 Bootstrap Records

Bootstrap records use this envelope:

```text
OrchBootstrap {
    type: "OrchBootstrap",
    phase,
    status,
    run_id,
    node_id,
    detail,
}
```

`phase` identifies the lifecycle area being reported.

`status` identifies the transition or outcome, such as:

```text
started
ready
failed
sent
observed
```

`detail` is phase-specific JSON.

Bootstrap records are emitted for runtime setup, provider start/stop, node specs, readiness waiting, stage provisioning, weight loading, prompt RPC readiness, shutdown, and process exit.

### 8.5 Prompt Records

Prompt records use this envelope:

```text
OrchPromptEvent {
    type: "OrchPromptEvent",
    phase,
    status,
    run_id,
    node_id,
    request_id,
    detail,
}
```

Prompt records describe orchestration progress for a prompt request. They are not the prompt response stream.

Prompt record phases include:

- prompt work observed;
- direct prompt send started/ready/failed;
- direct prompt event observed/dropped;
- prompt complete;
- pipeline tokenizer encode started/ready;
- pipeline token-in started/ready;
- pipeline token-out observed;
- pipeline tokenizer decode started/ready.

Prompt response text is emitted through prompt RPC as `PromptEvent`. Prompt datastream records are diagnostic and observational.

### 8.6 Provisioning Event Records

Provisioning lifecycle events are emitted on:

```text
mvp.provisioning.events
```

Record shape:

```text
MvpProvisionEventRecord {
    event: ProvisionEvent,
}
```

Provision event shape:

```text
ProvisionEvent {
    run_id,
    node_id,
    kind,
    provider,
    message,
}
```

Accepted event kinds:

```text
ProvisionStart
NodeLive
ProvisionFailed
NodeStopped
```

Provisioning events are emitted when provider startup begins, nodes become live, provider startup fails, or nodes stop.

### 8.7 Provisioning Log Records

Provisioning logs are emitted on node-specific log channels:

```text
mvp.provisioning.logs.node.<node_id>.stdout
mvp.provisioning.logs.node.<node_id>.stderr
mvp.provisioning.logs.node.<node_id>.provider
```

Record shape:

```text
MvpProvisionLogRecord {
    line: ProvisionLogLine,
}
```

Log line shape:

```text
ProvisionLogLine {
    run_id,
    node_id,
    stream,
    line,
}
```

Accepted log streams:

```text
Stdout
Stderr
Provider
```

Worker stdout, worker stderr, provider log lines, and captured orchestrator stdout/stderr are represented through this log record format.

Log lines are observational. They are not parsed as commands.

### 8.8 Orchestrator Stdio Logs

After stdio capture is installed, orchestrator stdout and stderr are redirected into log records.

Captured stdout becomes a provisioning log record with stream `Stdout`.

Captured stderr becomes a provisioning log record with stream `Stderr`.

The capture path is used so startup/runtime diagnostics appear in the same datastream/log stream as worker and provider logs.

Fatal errors before capture may still appear on process stderr.

### 8.9 Provider Observation Logs

Provider plugin observations are converted into datastream output.

Conversion rules:

- `StdoutLine` becomes a provisioning stdout log record.
- `StderrLine` becomes a provisioning stderr log record.
- `ProviderLine` becomes a provisioning provider log record.
- `DatastreamFrame` is emitted to the supplied channel as a raw payload.
- `Exited` becomes a provisioning node-stopped event.
- `Failed` becomes a provisioning failed event.

Provider observation logs keep provider output visible without making provider stdout/stderr a direct user interface contract.

### 8.10 Worker Datastream Collection

Worker datastream frames arrive over the datastream ALPN.

The orchestrator accepts datastream connections and reads:

- stream headers;
- channel declarations;
- frame deliveries;
- stream end notifications.

For each frame, the orchestrator records:

- source stream id;
- channel name;
- channel id;
- frame position;
- payload bytes.

Collected worker frames are forwarded to configured sinks:

```text
worker datastream frame
-> dashboard, if enabled
-> frame archive, if configured
```

Worker datastream frames are not re-emitted through prompt RPC.

### 8.11 Dashboard Sink

The dashboard receives datastream frames when dashboard support is enabled.

The dashboard sink consumes frames from:

- orchestrator datastream flushes;
- collected worker datastream frames.

Dashboard state is derived from datastream frames. The dashboard is not the source of runtime truth.

If dashboard support is disabled, the orchestrator still runs and emits datastream frames to other configured sinks.

### 8.12 Frame Archive

The frame archive is enabled by datastream frame log configuration.

Frame archive output is JSON lines.

Archive record shape:

```text
FrameArchiveRecord {
    arrival_seq,
    source,
    stream,
    channel,
    channel_id,
    position,
    payload,
}
```

`arrival_seq` is assigned by the archive and increases for each archived frame.

`position` is the frame position inside its source datastream.

`source` identifies the ingestion path, such as orchestrator-originated frames, node bootstrap stdio frames, or node cluster datastream frames.

Payload encoding is recorded as either:

```text
{ encoding: "utf8", value: <text> }
```

or:

```text
{ encoding: "bytes", value: <bytes> }
```

The archive may create parent directories for the configured path.

Without frame logging, the frame archive component is absent.

### 8.13 Ordering and Scope

Frame ordering is local to its stream and channel position.

Archive `arrival_seq` is the archive’s observed arrival order, not a global runtime ordering guarantee.

Datastream frames from different streams may interleave.

Log line ordering is preserved only to the extent that the producing stream, capture pipe, provider observation channel, and archive arrival order preserve it.

### 8.14 Secret Handling

Secret values must not be emitted as datastream payloads or log lines by orchestrator-owned records.

The orchestrator may emit secret presence as metadata, such as whether a Vast.ai API key is configured.

External tools and providers may produce output outside the orchestrator’s control. The orchestrator should avoid copying secret values into structured records when it handles provider errors.

### 8.15 Datastream Non-Goals

Datastream is not:

- prompt RPC;
- actor transport;
- provider control;
- worker stdin;
- an ordering authority across all runtime systems;
- a replacement for lifecycle gates.

Lifecycle gates are driven by explicit actor reports, provider results, process/control inputs, and runtime state checks. Datastream records explain what happened; they do not by themselves make the runtime ready, failed, or stopped.

---

## 9. Behavioral Contracts

Behavioral contracts are runtime invariants that callers, wrappers, tests, and maintainers may rely on.

### 9.1 Configuration Must Resolve Before Runtime Startup

The orchestrator must resolve configuration before it initializes the runtime stack or starts workers.

Configuration failure must prevent worker provisioning.

Configuration failures include:

- unknown process arguments;
- missing process-argument values;
- unsupported runtime profile;
- unsupported provider;
- invalid prompt RPC bind address;
- invalid pipeline stage count;
- invalid cached model path when cached model execution is selected;
- missing worker binary for process provider;
- missing Vast.ai requirements when Vast.ai is selected.

The mock provider is not a supported orchestrator runtime provider.

### 9.2 Provider Constraints Must Be Enforced Before Provisioning

Provider-specific constraints must be checked before workers are started.

Contracts:

- process provider requires a local worker binary;
- Docker provider may use Docker GPU and mount settings;
- cached model host paths are supported only for process and Docker providers;
- Vast.ai requires prepared API and SSH configuration;
- Vast.ai does not support multi-stage pipeline provisioning in the current orchestrator contract.

A provider constraint failure must stop startup before node provisioning.

### 9.3 Planned Execution Requires Local Model Metadata

Planned execution requires locally inspectable GGUF metadata before provisioning.

Planned execution is selected when:

```text
cached_model is present
or pipeline_stages > 1
```

The orchestrator must reject planned execution when it cannot inspect the selected GGUF metadata locally.

The orchestrator must reject a pipeline stage count greater than the model layer count.

### 9.4 Prompt RPC Must Start After Runtime Readiness

Prompt RPC must not be advertised or bound as ready until workers are ready and weights are loaded.

Required prerequisites:

```text
workers started
runtime-ready reports received
SWIM membership alive
actor routes owned by expected workers
runtime-ready acknowledgements completed
stage provisioning sent
weights loaded
```

If these prerequisites fail, prompt RPC startup must not be reported as ready.

### 9.5 Worker Runtime Readiness Requires More Than a Worker Report

A worker runtime-ready report is necessary but not sufficient.

A worker is ready only when all readiness facts are true:

```text
matching NodeRuntimeReady report
+ SWIM member state is Alive
+ route owner for node actor matches worker node
= worker ready
```

For planned pipeline execution, every expected worker must satisfy this barrier.

For direct execution, the single expected worker must satisfy this barrier.

### 9.6 Runtime-Ready Acknowledgement Must Complete

After readiness barriers pass, the orchestrator must send runtime-ready acknowledgements to every expected worker.

The acknowledgement must include:

```text
run_id
node_id
stage_index
readiness_id
```

The orchestrator retries acknowledgements until all expected acknowledgement reports arrive or the acknowledgement timeout expires.

Timeout is a startup failure.

### 9.7 Stage Provisioning Must Follow Readiness

Stage provisioning must occur after worker readiness and runtime-ready acknowledgement.

Direct execution provisions one stage.

Planned pipeline execution provisions stages from the run plan.

Stage provisioning must include the model identity, tokenizer source, layer range, stage index, stage count, and edge wiring needed by that stage.

A stage fault during provisioning or weight loading is a startup failure.

### 9.8 Pipeline Weight Loading Is Sequential

In planned pipeline execution, stages are weight-loaded sequentially.

The orchestrator must not advance to the next unloaded stage until the active stage reports weights ready.

If a stage faults while loading weights, pipeline startup fails.

If a worker exits while loading weights, pipeline startup fails.

### 9.9 Prompt Serving Allows One Active Prompt

The orchestrator accepts at most one active prompt at a time.

While a prompt is active:

- additional prompt work remains queued;
- direct prompt events are matched by request id;
- pipeline tokenizer events are matched by request id;
- mismatched prompt or tokenizer events are ignored or dropped for the active prompt.

A terminal prompt event clears active prompt state.

### 9.10 Prompt Terminal Events End a Request

A prompt request ends with exactly one terminal outcome:

```text
Done
Fault
```

`TextDelta` is non-terminal.

After `Done` or `Fault`, the prompt RPC response stream for that request is complete.

Expected model or prompt failures should be represented as prompt `Fault` events, not as orchestrator process errors, unless the orchestration path itself failed.

### 9.11 Direct Prompt Mode Must Use Node Actor Inference

In direct prompt mode, the orchestrator must send prompt work to the worker node actor as an inference command.

Direct prompt flow:

```text
SubmitPrompt
-> NodeAgentMsg::InferPrompt
-> PromptEvent
-> prompt RPC stream
```

The prompt reply actor address must be supplied as `reply_to`.

### 9.12 Pipeline Prompt Mode Must Use Tokenizer and Token Edges

In pipeline prompt mode, prompt text must flow through tokenizer encode, token-in edge, pipeline stages, token-out edge, tokenizer decode, and prompt RPC.

Pipeline prompt flow:

```text
SubmitPrompt
-> EncodePrompt
-> PromptEncoded
-> token-in edge
-> token-out edge
-> DecodeTokens
-> TokensDecoded
-> prompt RPC stream
```

The first-stage node actor is the tokenizer encode actor.

The final-stage node actor is the tokenizer decode actor.

Tokenizer failures become prompt faults for the active request.

### 9.13 Pipeline Token Sequence Must Be Monotonic

Pipeline token output records must arrive in the expected sequence order.

The orchestrator tracks the next expected token sequence.

If a received token record sequence does not equal the expected sequence, prompt serving fails.

Sequence validation protects prompt output ordering and prevents feeding token feedback out of order.

### 9.14 Runtime Pumping Is Required for Progress

Actor delivery, Iroh transport, SWIM membership, route ownership, datastream connection acceptance, provider observation draining, and prompt progress require the orchestrator pump loop to run.

A wait loop must keep pumping runtime work while waiting for actor or transport-driven facts.

A blocking wait that does not pump runtime work violates the runtime model.

### 9.15 Provider Failures Are Fatal in Active Runtime Phases

Provider observations can fail startup or prompt serving.

Contracts:

- provider failure before readiness is a startup failure;
- worker exit before readiness is a startup failure;
- worker exit while loading weights is a startup failure;
- worker exit during prompt serving is a runtime failure;
- provider stop failure is a shutdown failure.

Provider stdout/stderr/provider log lines are observational and do not by themselves indicate failure.

### 9.16 Shutdown Must Attempt Worker Cleanup

Once worker handles are owned by the provisioned-cluster guard, the orchestrator must attempt to stop all remaining workers on shutdown.

Shutdown cleanup is best-effort.

The orchestrator preserves and reports the first provider-stop error from explicit shutdown.

The guard also attempts cleanup on drop if handles remain.

Cleanup success does not prove that all external provider resources were removed; it only proves that the orchestrator’s provider stop calls completed successfully.

### 9.17 Stop Commands Are Controlled Shutdown Requests

The accepted standard-input stop commands are:

```text
stop
shutdown
quit
```

They are trimmed and compared case-insensitively.

A stop command requests controlled shutdown. It is not a prompt request.

### 9.18 Datastream Is Observational

Datastream records do not make runtime state true.

Readiness, provisioning, prompt completion, failure, and shutdown are driven by actor reports, provider results, runtime state checks, prompt events, and explicit control inputs.

Datastream records may describe those transitions, but they are not lifecycle gates.

### 9.19 Actor Reports Must Match Active Context

Actor reports must match the active run and expected target before they can advance lifecycle state.

Filtering rules:

- run-scoped reports must match `run_id`;
- worker readiness reports must match expected `node_id`;
- stage reports must match expected `stage_index`;
- prompt events must match active `request_id`;
- tokenizer events must match active `request_id`.

Mismatched reports are ignored or dropped for the active lifecycle path.

### 9.20 Secrets Must Not Be Emitted by Orchestrator-Owned Records

Orchestrator-owned datastream records and logs must not emit secret values.

Allowed secret-related output is limited to presence metadata, such as whether a Vast.ai API key is configured.

Provider tools may emit output outside the orchestrator’s control. When the orchestrator handles provider errors, it should redact or avoid copying secret values into structured records.

---

## 10. Error Handling

The orchestrator treats errors as phase-specific failures. Each failure should identify the phase that failed and the concrete operation or runtime condition that failed.

Top-level process error format:

```text
mvp-orchestrator: <error>
```

Top-level process exit code:

```text
0 = successful completion or controlled shutdown
1 = configuration, startup, provisioning, prompt-serving, shutdown, or runtime failure
```

### 10.1 Error Propagation Model

Most orchestrator operations return:

```text
Result<(), String>
```

or a typed success value with `String` error:

```text
Result<T, String>
```

The top-level `run()` function propagates the first unrecovered fatal error.

When prompt serving has already started, shutdown combines two results:

```text
prompt-serving result
provider-stop result
```

The process succeeds only when both succeed.

If prompt serving fails, the orchestrator still attempts provider stop.

If provider stop fails, the first provider-stop error is preserved and returned.

### 10.2 Configuration Errors

Configuration errors happen before worker provisioning.

Configuration errors include:

- unreadable or invalid TOML;
- unsupported runtime profile;
- unsupported provider;
- unknown process argument;
- missing process-argument value;
- invalid integer value;
- invalid floating-point value;
- invalid boolean value;
- invalid prompt RPC bind address;
- pipeline stage count of zero;
- pipeline stage count unsupported by the selected provider;
- missing process worker binary;
- invalid cached model path;
- cached model path selected with unsupported provider;
- missing required Vast.ai API key;
- missing required Vast.ai bootstrap command;
- missing or invalid Vast.ai SSH identity.

Configuration errors must stop startup before workers are provisioned.

### 10.3 Runtime Initialization Errors

Runtime initialization errors happen while creating local orchestration services.

Runtime initialization errors include:

- failure to install stdio capture;
- failure to open the datastream frame archive;
- failure to create the Tokio runtime;
- failure to create the Iroh driver;
- failure to create the distribution runtime stack;
- failure to create local actor inboxes;
- failure to spawn the local orchestrator actor;
- failure to register local actors with the actor bridge;
- failure to start dashboard support;
- failure to create the pipeline edge endpoint.

When possible, runtime initialization failures emit a bootstrap failure record before returning the error.

### 10.4 Planning Errors

Planning errors happen before workers are started.

Planning errors include:

- selected GGUF source is remote when local inspection is required;
- selected local GGUF path does not exist or is not a file;
- GGUF metadata cannot be read;
- GGUF metadata cannot be converted into model facts;
- requested pipeline stage count exceeds model layer count;
- activation ring sizing overflows;
- token ring sizing overflows;
- run planner rejects the requested placement or model facts.

Planning errors stop startup before provider provisioning.

### 10.5 Provider Preparation Errors

Provider preparation errors happen before or during provider construction.

Provider preparation errors include:

- process provider worker binary missing;
- Docker provisioner setup failure;
- Vast.ai API client construction failure;
- Vast.ai SSH identity resolution failure;
- Vast.ai public-key derivation failure;
- Vast.ai account key lookup failure;
- Vast.ai account key registration failure;
- missing Vast.ai bootstrap command;
- missing prepared Vast.ai SSH identity.

Provider preparation errors stop startup before workers are started.

### 10.6 Provisioning Errors

Provisioning errors happen while starting workers.

Provisioning errors include:

- provider `start_node` returns an error;
- provider reports `Failed`;
- provider reports worker `Exited` before readiness;
- worker process exits before ready;
- Docker container exits before ready;
- remote provider bootstrap fails before ready.

If one worker fails to start after earlier workers started in the same provisioning attempt, the orchestrator must stop the already-started workers before returning the provisioning error.

Once the provisioned-cluster guard owns worker handles, the guard is responsible for cleanup attempts.

### 10.7 Runtime Readiness Errors

Runtime readiness errors happen while waiting for workers to become usable.

Readiness errors include:

- shutdown requested while waiting for node ready;
- provider failure while waiting for node ready;
- worker exit while waiting for node ready;
- missing expected runtime-ready report;
- runtime-ready report for unexpected run or node;
- SWIM membership never reaches required alive state;
- actor route owner never matches expected worker;
- runtime-ready acknowledgement timeout;
- failure to send runtime-ready acknowledgement.

Readiness wait loops must keep pumping actor and transport runtime while waiting.

Some readiness waits do not have a fixed timeout. They end only when readiness succeeds, a shutdown request arrives, or a failure is observed.

### 10.8 Stage Provisioning and Weight Loading Errors

Stage provisioning and weight loading errors happen after runtime readiness and before prompt RPC readiness.

Errors include:

- unplanned single-stage execution missing required layer range;
- failure to send `ProvisionStage`;
- missing runtime-ready state for a planned stage;
- missing consumer endpoint for a planned edge;
- failure to derive stage provisioning from the run plan;
- stage fault while loading weights;
- worker exit while loading weights;
- provider failure while loading weights;
- shutdown requested while loading weights.

A stage fault during weight loading is a startup failure.

Prompt RPC must not be reported ready after a stage provisioning or weight-loading failure.

### 10.9 Prompt RPC Errors

Prompt RPC errors happen while binding, reading, writing, or forwarding prompt work.

Errors include:

- failure to bind the configured prompt RPC socket;
- failure to read the bound socket address;
- failure to clone a prompt TCP stream;
- malformed prompt request JSON;
- prompt work queue stopped;
- failure to serialize a prompt response;
- failure to write a prompt response;
- failure to flush a prompt response.

Prompt RPC bind failure is a startup failure.

Malformed prompt request handling is scoped to the client connection. It does not by itself require the orchestrator process to fail unless it stops prompt serving or exposes a runtime error.

### 10.10 Prompt Serving Errors

Prompt serving errors happen after prompt RPC is ready.

Errors include:

- provider reports failure;
- provider reports worker exit;
- actor send failure for direct prompt inference;
- actor send failure for tokenizer encode/decode;
- pipeline token-in sender stops;
- pipeline token sequence violation;
- token record decode failure;
- shutdown channel behavior that prevents controlled exit.

Expected prompt-level faults are not prompt-serving errors.

Prompt-level faults include:

- worker returns `PromptEvent::Fault`;
- tokenizer returns `TokenizerEvent::Fault`.

Prompt-level faults should be returned to the prompt RPC client as prompt `Fault` events for the active request.

### 10.11 Datastream and Log Errors

Datastream and log errors are split into startup errors and best-effort observation errors.

Startup datastream/log errors include:

- failure to create parent directories for the configured frame archive path;
- failure to open the configured frame archive file;
- failure to initialize the orchestrator datastream endpoint.

These are startup failures.

Best-effort observation errors include:

- malformed datastream frame from a node;
- closed datastream connection;
- per-frame archive write failure after archive open;
- dashboard publication failure, when the dashboard sink can drop or reject frames without affecting runtime state.

Best-effort observation errors should not change lifecycle state unless the code explicitly treats them as fatal.

### 10.12 Actor Delivery Errors

Actor delivery errors happen when sending through the Swactor runtime fails.

Actor send failures are phase errors.

Examples:

- failure to send runtime-ready acknowledgement;
- failure to send datastream subscription request;
- failure to send stage provisioning;
- failure to send direct prompt inference;
- failure to send tokenizer encode request;
- failure to send tokenizer decode request.

The error belongs to the lifecycle phase that attempted the send.

A successful send means the actor runtime accepted the message for delivery. It does not prove the remote actor processed the message.

### 10.13 Provider Stop Errors

Provider stop errors happen during explicit shutdown or guard cleanup.

Explicit provider stop behavior:

- stop every remaining worker handle;
- preserve the first stop error;
- continue attempting to stop remaining handles;
- return the first stop error after all handles have been attempted.

Drop cleanup behavior:

- attempt to stop remaining handles;
- ignore stop errors because drop cannot return them.

Provider stop failure makes the orchestrator exit with failure unless a prior fatal error is already being reported.

### 10.14 Controlled Shutdown

Controlled shutdown is requested by standard input control words:

```text
stop
shutdown
quit
```

Controlled shutdown is not an error by itself.

A controlled shutdown succeeds only if prompt serving exits cleanly and provider stop succeeds.

If controlled shutdown is requested while startup is waiting for readiness or weight loading, the wait loop returns a shutdown-requested error for that startup phase.

### 10.15 Error Reporting Through Datastream

When the orchestrator has a datastream available, it should emit failure records for the phase that failed.

Failure records should include:

- phase;
- status `failed`;
- run id;
- node id when applicable;
- provider when applicable;
- error string or reason.

Datastream failure records are diagnostic. The actual process result is still determined by returned errors and provider stop result.

### 10.16 Secret Redaction

Error messages and datastream failure records must avoid exposing configured secrets.

Secrets include:

- Vast.ai API keys;
- Hugging Face tokens;
- SSH private-key material;
- provider credentials.

Secret presence may be reported. Secret values must not be copied into orchestrator-owned records.

---

## 11. Out of Scope

This document defines the orchestrator contract. It does not define every subsystem the orchestrator calls, hosts, or observes.

Out of scope:

- `mvp-chat` behavior, including interactive terminal UX, wrapper argument parsing, image preparation, rebuild policy, and user-facing prompt formatting.

- Worker-node internals, including model execution, TinyGrad helper behavior, CUDA behavior, worker process command protocol, weight loading implementation, tensor allocation, and device cleanup details.

- Model quality, sampling quality, tokenizer correctness, generated text quality, or semantic correctness of model outputs.

- GGUF format semantics beyond the orchestrator’s need to inspect metadata for planned execution.

- Docker image construction, Dockerfile contents, registry authentication, image freshness, image tagging policy, image push behavior, and image garbage collection.

- Docker daemon behavior beyond the provider result and observations returned to the orchestrator.

- Vast.ai marketplace semantics, offer selection quality, billing behavior, host reliability, remote image pull behavior, and remote shell behavior beyond provider success, failure, logs, and bootstrap status.

- SSH protocol details, SSH agent behavior, host key policy, key generation UX, and remote shell semantics beyond the orchestrator’s use of a configured identity and provider bootstrap launcher.

- Iroh protocol internals, relay implementation details, NAT traversal behavior, transport congestion behavior, and cryptographic details beyond the actor and datastream connectivity required by this contract.

- Swactor runtime internals beyond actor addressing, message delivery through the local runtime, and Iroh actor bridge integration used by the orchestrator.

- Datastream library internals beyond the frame, channel, record, dashboard, and archive behavior stated in this document.

- Dashboard rendering semantics, dashboard UI layout, dashboard persistence, and dashboard query APIs.

- Full security threat model, authentication model, authorization model, secret storage policy, or audit-log policy.

- External provider resource cleanup guarantees after the orchestrator has issued its provider stop calls.

- Cross-process supervision outside the orchestrator process.

- Long-term compatibility guarantees for implementation-private phase names, debug details, or non-contract telemetry fields.

- Performance guarantees, latency targets, throughput targets, GPU utilization targets, and prompt generation speed.

- Retry policies not explicitly stated in this document.

- Recovery after orchestrator process crash.

- Multi-run orchestration in a single process.
