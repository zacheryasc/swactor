# MVP Chat Wrapper Fixed Specification

**Status:** draft target behavior for `mvp-chat`.

This document describes the intended public contract. Implementation details that
remain in the source but were marked out of scope are not part of this fixed
contract.

---

## 1. Purpose

`mvp-chat` is the user-facing process that starts the `mvp-chat` library runtime
and attaches an interactive prompt session to it.

The wrapper is responsible for:

- accepting the approved public inputs;
- resolving provider and runtime launch configuration;
- preparing required local runtime artifacts through Cargo unless rebuilds are
  explicitly skipped;
- starting the library runtime and its orchestrator, prompt, and datastream leaves;
- waiting until the runtime can accept prompt requests;
- running the interactive prompt loop;
- notifying the runtime to shut down on normal exit or interruption;
- reporting errors clearly to the user.

`mvp-chat` is not responsible for:

- model inference quality;
- worker internals;
- node-image construction internals;
- orchestrator argument naming;
- provider API details beyond the inputs needed to request a provider-backed
  runtime;
- dashboard rendering;
- non-Linux behavior.

---

## 2. Supported Platform

This specification covers Linux only.

Linux signal handling, managed component and process-leaf lifecycle, Cargo
artifact discovery, and runtime shutdown semantics are the only supported
platform behavior. Non-Linux behavior is out of scope until explicitly specified.

---

## 3. Public Input Surface

`mvp-chat` accepts inputs only from the surfaces listed in this section.
Commented-out or implementation-only inputs from the earlier draft are pruned
from the public contract.

### 3.1 Process Arguments

Provider selectors:

- `--process`
- `--docker`
- `--vastai`

General flags:

- `--config <path>`
- `--yes`
- `-y`
- `--pipeline-stages <count>`
- `--dump-logs`
- `--dump-logs=<path>`
- `--cached-model`
- `--cached-model=<path>`
- `--skip-rebuild`

Any process argument that is not one of the listed flags or a required or
attached value for one of those flags is a configuration error.

`--process`, `--docker`, and `--vastai` are mutually exclusive. Supplying more
than one provider selector is a configuration error.

If no provider selector is supplied, the provider is `process`.

`--pipeline-stages <count>` accepts a positive integer. Zero and invalid values
are configuration errors.

`--dump-logs` writes the consolidated log stream to the default file
`mvp-chat.log` in the current working directory.

`--dump-logs=<path>` writes the same stream to `<path>`. The `<path>` value is
the literal string after `=`, may start with `-`, and may be relative or
absolute. Relative paths are resolved relative to the current working directory.
`--dump-logs=` is a configuration error.

`--dump-logs <path>` is not accepted. Without `--dump-logs`, logs are not stored
in a file.

`--cached-model` enables cached-model use and discovers a cached model from
`.model-cache/`.

`--cached-model=<path>` uses `<path>` as the cached model file. The `<path>`
value is the literal string after `=`, may start with `-`, and may be relative
or absolute. Relative paths are resolved relative to the current working
directory. `--cached-model=` is a configuration error.

`--cached-model <path>` is not accepted. Without `--cached-model`, cached-model
use is disabled.

`--skip-rebuild` prevents `mvp-chat` from invoking Cargo builds. If a required
runtime artifact is unavailable while rebuilds are skipped, preparation fails
with a clear error.

### 3.2 Environment Variables

The public configuration environment surface is limited to secret material.

Accepted environment variable:

- `VAST_API_KEY`

`VAST_API_KEY` supplies the Vast.ai API key when the selected provider is
`vastai`. The value is trimmed before validation; an unset, empty, or
whitespace-only value is treated as missing.

No other environment variable is part of the public `mvp-chat` configuration
contract. Normal inherited process environment, such as the environment used by
Cargo or child processes, is ordinary OS execution context rather than
`mvp-chat` configuration.

### 3.3 Configuration File

`mvp-chat` reads configuration from a TOML file.

The config path is:

- `--config <path>`, when supplied;
- otherwise `.config/config.toml` relative to the current working directory.

A supplied `--config <path>` is required to be a readable file and parse as
TOML. The default `.config/config.toml` is optional and is read only when it
exists as a file. If the default config file exists but cannot be read or parsed,
configuration fails; if the default path is absent or not a file, built-in
defaults are used.

The fixed spec accepts only active behavior fields. Unused schema fields from the
earlier draft are pruned. Unknown top-level TOML tables and unknown fields inside
accepted tables are configuration errors.

Accepted provider field:

- `[provider].kind`

`[provider].kind` is trimmed before validation. After trimming, accepted values
are case-sensitive and exactly `process`, `docker`, or `vastai`.

Accepted runtime fields:

- `[runtime].pipeline_stages`
- `[runtime].max_tokens`

`[runtime].max_tokens` sets the maximum number of tokens requested for each
prompt submission. A value of `0` implies no specified limit.

Accepted observability fields:

- `[observability].dump_logs`
- `[observability].dump_log_path`

Accepted image fields:

- `[image].node`
- `[image].tag`

`[image].node` names the desired worker node image.

For provider `docker`, `[image].node` may name a local or remote image.

For provider `vastai`, `[image].node` must name a remote registry image that the
provider can pull.

`[image].tag` may provide an additional human-selected tag or alias for an image
prepared by `mvp-chat`. It does not replace the resolved image reference used for
freshness or content identity.

Accepted Vast.ai fields:

- `[vastai].relay_url`
- `[vastai].bootstrap_command`
- `[vastai].gpu_name`
- `[vastai].min_gpu_ram_mb`
- `[vastai].min_down_mbps`
- `[vastai].min_up_mbps`
- `[vastai].min_reliability`
- `[vastai].require_verified`
- `[vastai].disk_gb`
- `[vastai].onstart`
- `[vastai].ssh_identity`

`[vastai].ssh_identity` is a filesystem path to an SSH private-key identity file
used for provider bootstrap access. It is configuration, not a secret-value
environment variable.

### 3.4 Standard Input

Accepted standard input:

- interactive prompt lines;
- EOF or input disconnection;
- standard-input read error during prompt input;
- Vast.ai rental approval response when approval is required.

Prompt lines drive the prompt loop. EOF, input disconnection, and standard-input
read errors during prompt input end the prompt loop cleanly and trigger normal
runtime cleanup. There are no prompt text commands for exiting.

While waiting for prompt input, `mvp-chat` must still respond to EOF, input
disconnection, standard-input read errors, `SIGINT`, and `SIGTERM`. The exact
input-read mechanism is an implementation detail.

### 3.5 Signals

Accepted Linux signals:

- `SIGINT`
- `SIGTERM`

Both request controlled shutdown.

### 3.6 Filesystem Inputs

Filesystem inputs:

- current working directory;
- config file selected by Section 3.3;
- `.model-cache/` when `--cached-model` is supplied;
- cached model file path when `--cached-model=<path>` is supplied;
- Cargo workspace files needed by Cargo to build or locate runtime artifacts;
- Cargo target artifacts for the orchestrator and worker;
- optional Vast.ai SSH identity file path from config;
- optional dump-log output path parent directories.

Additional filesystem inputs for image preparation:

- node image Dockerfile;
- node image build context;
- worker binary artifact included in the image;
- source files used to determine image freshness;
- local Docker image metadata.

The current working directory is the root for relative paths.

When `--cached-model` is supplied, `mvp-chat` reads the direct files in
`.model-cache/`, filters for model files accepted by the runtime, sorts the
remaining files alphabetically by filename, and selects the first file. Failure
to read `.model-cache/`, read a direct directory entry, or stat a direct entry is
a preparation error. Direct entries that can be statted but do not match the
cached-model predicate are ignored. If no usable cached model is present,
preparation fails with a clear error.

When `--cached-model=<path>` is supplied, `mvp-chat` validates that the path is a
usable cached model file. A usable cached model path must resolve to a regular
file whose extension is `.gguf`, matched case-insensitively. If it is missing,
not a regular file, or not accepted by this predicate, preparation fails with a
clear error. Accepted cached-model paths are canonicalized before being included
in the runtime launch request.

### 3.7 Network and Runtime Inputs

Runtime inputs:

- prompt engine readiness outcome;
- prompt engine `PromptEvent` stream records;
- orchestrator and component progress events written to the local datastream.

Additional provider/image inputs:

- Docker daemon responses when checking local image availability;
- registry responses when checking remote image availability;
- registry responses when pushing images for remote providers.

Prompt engine stream records are runtime-local prompt events:

- text delta;
- request completion;
- request fault.

Detailed progress payload schemas are not specified here. This spec only
requires that `mvp-chat` receive enough progress information to present the
user-facing progress states defined in Section 7.

### 3.8 Managed Component Inputs

Managed component inputs observed by `mvp-chat`:

- orchestrator leaf start success or failure;
- orchestrator leaf readiness result;
- orchestrator leaf fault or exit before readiness;
- prompt engine leaf readiness result;
- prompt engine leaf fault before readiness;
- failure to request or wait for managed-component shutdown.

The OS process APIs, process actors, and actor-runtime notifications used to
observe these states are implementation details. The observable contract is the
resulting success, failure, readiness, fault, or controlled shutdown.

---

## 4. Provider Selection

`mvp-chat` has no public runtime-profile concept. The public provider choices
are:

- `process`
- `docker`
- `vastai`

Provider resolution order:

1. CLI provider selector.
2. TOML `[provider].kind`.
3. default `process`.

The accepted provider values are exactly:

- `process`
- `docker`
- `vastai`

Compatibility aliases may exist in implementation, but they are not part of the
fixed public contract.

If `vastai` is selected, required Vast.ai configuration must be present before
offer preview, approval, or launch. Missing required Vast.ai configuration is a
configuration error.

---

## 5. Configuration Resolution

Configuration is resolved from:

- process arguments;
- TOML configuration;
- approved secret environment variables;
- fixed defaults.

Process arguments override TOML where both define the same behavior.

The only approved environment override is `VAST_API_KEY` for the Vast.ai API key.
Other configuration must come from process arguments, TOML, fixed defaults, or
filesystem discovery.

Default values:

- provider: `process`;
- pipeline stages: `1`;
- max tokens: `0`;
- dump logs: disabled;
- cached model: disabled unless `--cached-model` is supplied;
- rebuild: enabled unless `--skip-rebuild` is supplied.

Invalid values must fail before runtime preparation begins.

For provider `process`, no node image is required.

For provider `docker`, an image reference is required. It may be local or remote.

For provider `vastai`, an image reference is required and must be a remote
registry image.

If a provider requires an image and no valid image reference is configured,
configuration fails before runtime preparation.

---

## 6. Runtime and Image Artifact Preparation

### 6.1 Cargo Runtime Artifacts

`mvp-chat` obtains the orchestrator and worker artifacts through Cargo.

The wrapper must not infer the orchestrator or worker path by changing the file
name of the current executable.

The current working directory is the artifact root and must be available. The
default orchestrator artifact path is `target/debug/mvp-orchestrator` under that
directory. The default worker artifact path is `target/debug/mvp-worker-node`
under that directory. No fallback artifact root is defined.

Unless `--skip-rebuild` is supplied, `mvp-chat` may invoke Cargo to make required
artifacts available.

Approved Cargo builds:

- `cargo build --quiet -p mvp-system --bin mvp-orchestrator`
- `cargo build --quiet -p mvp-system --bin mvp-worker-node`

When `--skip-rebuild` is supplied:

- `mvp-chat` must not invoke Cargo builds;
- required Cargo artifacts must already be available;
- missing Cargo artifacts are preparation errors.

Worker binary behavior mirrors orchestrator binary behavior: both are resolved
through Cargo artifacts, both honor `--skip-rebuild`, and both fail clearly when
required artifacts are unavailable.

### 6.2 Node Image Preparation

Node image preparation applies only to provider-backed runtimes:

- `docker`
- `vastai`

Provider `process` does not require a node image.

For provider-backed runtimes, `mvp-chat` performs node image resolution before
launching the orchestrator. Node image resolution consumes:

- the selected provider;
- `[image].node`;
- optional `[image].tag`;
- rebuild policy from `--skip-rebuild`;
- the worker binary artifact selected for this run;
- the approved node-image Dockerfile and build context;
- source files and metadata used to determine image freshness;
- Docker daemon observations for local images;
- registry observations for remote images.

Node image resolution produces the resolved image reference included in the
orchestrator launch request.

For provider `docker`, the resolved image must be runnable by the local Docker
daemon. The image may be local or remote.

For provider `vastai`, the resolved image must be pullable by the remote
provider and must include a registry/repository namespace. Local-only image names
are invalid.

When rebuilds are enabled, `mvp-chat` must determine whether the requested image
is already acceptable for the selected provider and current runtime inputs. If no
acceptable image is available, `mvp-chat` may build, tag, push, and validate an
image as required by the selected provider.

An acceptable prepared image is one that:

- is usable by the selected provider;
- was prepared from the approved node-image Dockerfile and build context;
- includes the selected worker binary artifact;
- is not stale with respect to the freshness inputs used by the
  image-preparation contract;
- has any configured `[image].tag` alias applied when applicable.

When `--skip-rebuild` is supplied, `mvp-chat` must not build, tag, or push
images. It must use only existing image artifacts and fail clearly if the
required image is missing, stale, unavailable, or unsuitable for the selected
provider.

The exact freshness algorithm, metadata format, Docker commands, cache policy,
and registry authentication mechanics are owned by the node-image preparation
contract.

### 6.3 Cached Models and Images

Cached model selection is independent from node image preparation unless an
approved image-preparation contract explicitly says otherwise.

By default, `mvp-chat` treats cached models as runtime inputs, not as image
contents. It must not silently bake cached models into prepared images.

---

## 7. Progress, Logs, and Datastream

Normal runtime logs are consolidated into one `mvp-chat` log stream.

Without `--dump-logs`, the stream is not stored in a file.

With `--dump-logs`, the stream is written to the default file defined in Section
3.1.

With `--dump-logs=<path>`, the stream is written to the specified path according
to the path parsing rules in Section 3.1.

`mvp-chat` must not create hidden startup archive files as part of the public
contract.

Progress observation uses the `mvp-chat` local datastream endpoint, not
archive-file polling.

The runtime owns one local endpoint:

- stream id: `StreamId::new(NodeId::new("mvp-chat"), Lifetime(run_id))`;
- label: `"mvp chat"`;
- origin: `StreamOrigin::Orchestrator` until a chat-specific origin exists.

Required channels:

- `mvp.chat.lifecycle`;
- `mvp.chat.runtime`;
- `mvp.chat.prompt`;
- `mvp.chat.component`.

Components write through cloned `DatastreamProducer` handles or through local
adapters installed when a component leaf starts. The datastream task drains the
local endpoint and fans frames out to subscribers. Payload schemas remain owned
by the datastream/progress contract.

The user-facing progress model must eventually define visible transitions for
runtime startup. Until that model is approved, this spec only fixes prompt-loop
output in Section 10 and keeps non-prompt progress output deferred.

---

## 8. Vast.ai Behavior

When provider is not `vastai`, Vast.ai config and approval are not used.

When provider is `vastai`, required configuration must be present before any
offer preview or launch.

Required Vast.ai inputs:

- API key from `VAST_API_KEY`;
- relay URL from config;
- node image reference from config;
- bootstrap command when required by the provider contract.

Optional Vast.ai selection inputs:

- GPU name;
- minimum GPU RAM;
- minimum downlink bandwidth;
- minimum uplink bandwidth;
- minimum reliability;
- verified-host requirement;
- disk size;
- onstart command;
- SSH identity file path.

If approval is required and `--yes` is not supplied, `mvp-chat` asks the user for
approval through the terminal. Only `y` and `yes`, after trimming and
case-folding, approve the rental. Any other answer declines.

If `--yes` or `-y` is supplied, approval is accepted non-interactively after
required configuration is validated.

If approval is required but standard input is not interactive, `mvp-chat` fails
unless `--yes` or `-y` is supplied.

---

## 9. Orchestrator Launch and Shutdown

Exact orchestrator argv is out of scope until the orchestrator launch contract is
specified.

`mvp-chat` is responsible for handing the resolved runtime request to the
orchestrator leaf through the library runtime. The production process-backed leaf
owns binary resolution, `ProcessSpec` construction, and managed process actor
startup; successor in-process leaves start the orchestrator actor group directly.

The semantic launch request must include, as applicable:

- selected provider;
- resolved node image reference for provider-backed runtimes;
- pipeline stage count;
- cached model selection result;
- dump-log configuration;
- provider-specific runtime configuration;
- datastream producer or adapter wiring required by the orchestrator leaf.

The orchestrator launch contract does not define prompt transport. Prompt work is
handled by the `mvp-chat` prompt engine actor/task through runtime-local
messages.

The resolved node image reference is the image the orchestrator must use for the
provider-backed node. Exact argv or wire encoding remains owned by the
orchestrator launch contract.

The wrapper starts the `mvp-chat` library runtime. The runtime starts the
orchestrator leaf, prompt engine leaf, datastream task, swactor runtime, and
control path. Production process-backed leaves are managed by process actors;
`mvp-chat` must not directly own `std::process::Child` for long-lived
components.

On shutdown, `mvp-chat` must request shutdown through the runtime control path.
The shutdown mechanism for each managed component is owned by that component's
leaf contract.

Shutdown must be idempotent from the user's perspective. Normal prompt exit,
input EOF, startup interruption, and signal interruption must not leave the
runtime running when `mvp-chat` can notify it.

---

## 10. Prompt Loop

The prompt loop accepts user prompt lines from standard input.

For each cycle, `mvp-chat` must:

- display a prompt marker;
- read one line of input;
- remove trailing whitespace from the input line before prompt handling, while
  preserving leading whitespace;
- exit cleanly for EOF, input disconnection, or standard-input read error;
- ignore prompts that are empty after whitespace trimming;
- submit non-empty prompts to the prompt engine actor/task;
- display that decoding has started;
- stream response text as `PromptEvent` values arrive;
- return to the prompt marker after completion or prompt fault.

Prompt requests carry:

- request id;
- prompt text;
- max token limit resolved from `mvp-chat` configuration;
- reply target for the `PromptEvent` stream.

Prompt responses are:

- `PromptEvent::TextDelta`;
- `PromptEvent::Done`;
- `PromptEvent::Fault`.

`mvp-chat` submits at most one prompt at a time to the prompt engine. It waits
for a terminal `Done` or `Fault` event before submitting the next prompt. The
prompt engine guarantees that response events for an active request arrive in
order on the reply target.

The prompt-loop output states are:

- waiting for prompt;
- prompt submitted;
- decoding;
- streaming response;
- request completed;
- request faulted;
- prompt loop exited.

Prompt-loop user output goes to standard output unless it is an actual wrapper
error. Expected model faults are prompt-loop results, not wrapper diagnostics.

A fixed transport or read timeout is not part of the contract. The implementation
must remain interruptible, but this spec does not require a timeout-based
mechanism.

---

## 11. Public Output Surface

### 11.1 Exit Codes

Exit code `0` means clean completion or controlled interrupted shutdown.

Exit code `1` means configuration failure, preparation failure, startup failure,
prompt engine failure, managed runtime failure, or another wrapper error.

### 11.2 Standard Output

Standard output is for expected user-facing behavior.

Standard output includes:

- prompt marker;
- decoding marker;
- response prefix;
- response text;
- prompt-loop completion formatting;
- expected prompt fault display;
- Vast.ai approval prompt when interactive approval is required.

Non-prompt startup progress output is deferred until the progress event model is
approved.

### 11.3 Standard Error

Standard error is for actual wrapper errors and exceptional diagnostics.

Standard error must not be used for ordinary status messages such as successful
provider selection, normal build status, normal cached-model selection, or normal
prompt-loop events.

Errors must be clear enough for the user to identify the failed input or failed
runtime phase.

Image preparation errors must be displayed clearly when image preparation fails.

Image-preparation errors include:

- missing required image reference;
- invalid image reference;
- required rebuild skipped;
- local image unavailable;
- remote image unavailable;
- image build failure;
- image tag failure;
- image push failure.

### 11.4 Filesystem Outputs

Filesystem outputs are limited to:

- Cargo build artifacts when rebuilds are enabled;
- dump-log file when `--dump-logs` is supplied;
- local Docker image layers when image rebuilds are allowed;
- local Docker image tags or aliases when image rebuilds are allowed;
- image build cache entries when image rebuilds are allowed;
- provider/runtime artifacts owned by external contracts, if those contracts are
  invoked.

`mvp-chat` MUST NOT create unspecified filesystem outputs.

### 11.5 Network Outputs

Network-visible outputs:

- `mvp-chat` datastream endpoint for dashboard/user observers when progress
  observation is active.

Additional network-visible outputs when preparing remote images:

- registry manifest checks;
- image layer uploads;
- image manifest or tag pushes.

Prompt submissions are runtime-local messages to the prompt engine actor/task;
they are not network-visible outputs.

### 11.6 Managed Runtime Outputs

Outputs to managed runtime components are limited to the approved orchestrator
leaf launch and shutdown contracts, prompt engine request messages, and
datastream frames.

This spec does not define exact argv names, stdin control strings, private
orchestrator flags, or internal actor message encodings beyond the prompt request
and event shapes in Section 10.

---

## 12. Error Handling

Configuration errors must be detected before runtime preparation where possible.

Preparation errors must identify the missing artifact, invalid file, failed Cargo
operation, or invalid provider configuration.

Startup errors must identify the failed startup phase when progress information
is available.

Unexpected prompt engine errors must identify whether request submission,
event-stream closure, prompt event handling, or component fault failed.

Controlled shutdown is not an error.

Errors are displayed clearly to the user and cause nonzero exit unless the error
occurs during a controlled shutdown path defined as successful by this spec.

---

## 13. Out of Scope

Out of scope for this document:

- path display formatting as a standalone contract;
- exact orchestrator argv;
- detailed datastream payload schemas;
- non-Linux support;
- Dockerfile contents;
- base-image implementation details;
- registry authentication UX beyond clear preparation errors;
- image optimization policy;
- image garbage-collection policy;
