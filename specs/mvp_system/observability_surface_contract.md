# Observability Surface Contract

This document defines the behavioral contract for lifecycle and fault events
used by tests and operators. Observability transport and storage are
implementation details.

## Event Identity

- Required events include `run_id` when run-scoped.
- Required events include `node_id` when node-scoped.
- Required events include `stage_index` when stage-scoped.
- Required events include `edge_id` when edge-scoped.
- Required events include `ring_id` when ring-scoped.
- Object events include `object_id` and `sequence`.
- Step events include `step_id`.
- Worker events include `worker_generation`.

## Lifecycle Events

- Node boot emits `node_started` and `node_available` or `node_faulted`.
- Pool readiness emits `pool_ready`.
- Planning emits `run_planned`.
- Stage provisioning emits `stage_provision_started`.
- Weight work emits `weights_download_started`, `weights_downloaded`, and
  `weights_loaded` when those phases occur.
- Edge provisioning emits `edge_provision_started` and `edge_ready`.
- Stage readiness emits `stage_ready`.
- The global barrier emits `readiness_barrier_passed`.
- Prompt injection emits `prompt_injected`.
- Execution emits `object_loaded`, `execute_step_started`, `object_produced`,
  `step_completed`, and `token_received`.
- Terminal run state emits `run_completed` or `run_faulted`.
- Teardown emits `stop_run_sent`, `stage_stopped`, and `run_torn_down`.

## Fault Events

- Fault events include a stable reason enum.
- Fault events include the component that detected the fault.
- Tests do not need to scrape logs to determine lifecycle progress.
- Free-form logs may add diagnostics but do not replace structured events.

## Ordering

- Events reflect the same ordering guarantees as the component contracts.
- `prompt_injected` cannot precede `readiness_barrier_passed`.
- `stage_ready` cannot precede required local readiness.
- `run_torn_down` cannot precede teardown completion.
- A run emits exactly one terminal outcome event.

## Test Direction

Tests should subscribe to the stable event stream and assert event identities,
reason enums, and ordering. Contract tests should not depend on log text,
transport implementation, storage backend, or event batching policy.
