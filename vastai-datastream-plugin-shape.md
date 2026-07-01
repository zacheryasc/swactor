# VastAI datastream + provisioning plugin shape

## Goal

First deployment preflight needs observability from the moment a VastAI node is provisioned.

The node should produce datastream frames in two phases:

1. **Bootstrap phase:** orchestrator reaches the node over SSH and captures remote stdout/stderr as datastream frames.
2. **Native phase:** once the remote swactor node is live, the node sends datastream frames back directly over the normal remote transport path.

The key invariant: the logical node stream identity stays stable across both phases. The source changes from SSH bridge to native remote datastream; the node stream does not.

## Motivation

If the node fails before swactor starts, native actor/datastream paths are unavailable. We still need early boot logs, image startup errors, dependency failures, worker launch errors, and ready-line parsing in the same observation surface used after handoff.

This avoids a blind gap between `vast.ai contract created` and `remote swactor joined`.

## Existing pieces to reuse

- `crates/mvp-system/src/provisioning.rs`
  - `ProvisionPlugin`
  - `PluginSink`
  - `PluginObservation::{ProviderLine, StdoutLine, StderrLine, RuntimeReady, Failed, Exited}`
  - Docker already parses stdout ready JSON.

- `crates/mvp-system/src/actors/provisioner.rs`
  - already turns plugin observations into provisioning reports and datastream records when given a `DatastreamProducer`.

- `crates/mvp-system/src/telemetry.rs`
  - provisioning events/log channels already exist.

- `crates/datastream`
  - inspect and reuse existing remote/transport APIs before adding anything new. // USER: Don't add anything new, you should not need to.
  - `StreamId` already includes node id + `Lifetime`; do not invent a second lifetime concept.

- `tools/vastai`
  - existing VastAI client/provisioning utility.
  - plugin design should wrap this, not duplicate provider API logic.

## Proposed code shape

### `crates/mvp-system/src/vastai_provisioning.rs`

Add an MVP VastAI provisioning plugin around `tools/vastai`.

Responsibilities:

- build a one-node VastAI provision request from MVP config/spec inputs;
- create/track the contract handle;
- obtain SSH endpoint details;
- start the SSH bootstrap/datastream bridge;
- emit provider/stdout/stderr/ready observations through `PluginSink`;
- destroy the known contract on stop.

Keep this focused. Full plugin details need their own design pass: offer policy, spend guard, retries, replacement, recovery, labels, and held-cluster behavior.

### `crates/mvp-system/src/bootstrap_datastream.rs`

Small bridge for pre-swactor visibility.

Responsibilities:

- read remote stdout/stderr lines from an SSH session or equivalent stream;
- submit those lines as datastream frames/records for the assigned node stream;
- also forward lines to `PluginSink` so existing provisioner reports/dashboard behavior still works;
- parse the same ready JSON shape Docker uses and emit `RuntimeReady`.

This module should not be VastAI-specific.

### Remote/native datastream hookup

Before implementing new APIs, inspect `crates/datastream` for existing remote transport support.

Desired behavior:

- provisioner assigns the node datastream identity before lease/bootstrap;
- bootstrap env passes that identity to the remote node;
- remote node starts native datastream emission once swactor/iroh is live;
- remote node emits a native-ready marker;
- orchestrator overlaps SSH capture briefly, then closes the SSH tail.

If an mvp-system adapter is needed, keep it thin and local to remote datastream receiver/handoff glue.

## Handoff model

States:

- `BootstrapSsh`: SSH bridge is authoritative.
- `Overlap`: first valid native frame or native-ready marker observed; keep SSH briefly.
- `NativeIroh`: native remote datastream is authoritative; SSH tail is closed.

Do not require native datastream to be available before bootstrap logs start.

## Test ladder

1. Fake VastAI provision returns a contract + SSH endpoint.
2. Fake SSH stdout/stderr line becomes a datastream observation.
3. Ready JSON on stdout emits `RuntimeReady` through the existing plugin/provisioner path.
4. Native-ready handoff moves from SSH bridge to native and closes SSH after overlap.
5. Stop destroys the known VastAI contract exactly once.
6. Dockerized E2E submits remote datastream frames over the remote transport and the orchestrator collects them.
7. VastAI plugin uses the same remote-frame receiver path; only lease/SSH acquisition differs.

## Non-goals for this doc

- exact VastAI offer-selection policy;
- exact spend/confirmation UX;
- full recovery of abandoned contracts;
