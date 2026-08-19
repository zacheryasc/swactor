# Myelin Fleet-Control Daemon

Myelin is a persistent, dashboard-first control plane for manually managed compute nodes. It boots an empty fleet, accepts explicit operator commands, records intent and observations, and never performs hidden replacement or teardown.

## Runtime shape

```text
myelin-orchestrator
  -> load stable iroh identity and cluster snapshot
  -> start engine, iroh endpoint, telemetry collector, and dashboard
  -> recover persisted provider resources without creating replacements
  -> idle event loop
       - ingest telemetry, membership, actor reports, and control messages
       - dispatch explicit Provision / Kill effects
       - atomically persist every state transition before dependent effects
  -> on exit, detach durable-provider handles without destroying resources
```

There is no desired-shape reconciler or automatic node replacement in the manual control path. Provisioning, Kill, recovery, runtime readiness, and worker rejoin are typed actor protocols; provider I/O and snapshot writes run outside actor handlers on the engine.

## Starting the daemon

From the workspace root:

```sh
cargo run -p myelin
```

The local default uses the process provider. Use Docker explicitly when required:

```sh
cargo run -p myelin -- --provider docker
```

Use Vast.ai without making valid credentials a startup prerequisite:

```sh
cargo run -p myelin -- --provider vastai
```

The dashboard starts in `unconfigured` or `configuration_error` state and accepts corrected credentials at runtime.

`MYELIN_DASHBOARD_PORT` selects the dashboard port. The dashboard root remains the read-only Fleet view; `/provision` is the Myelin-owned mutation surface. Its provider-neutral control protocol includes:

- `Provision { command_id, count, selected_offer_ids }`: create the requested nodes; Vast.ai requires and leases only the exact selected offer IDs.
- `Kill { command_id, logical_node_id }`: stop one managed process/container or destroy one Vast.ai contract while retaining its terminal snapshot record.
- `ConfigureProvider`: validate corrected in-memory Vast.ai credentials and bootstrap settings.
- `SearchOffers`: inspect filtered Vast.ai offers without leasing.
- `Query`: return provider readiness, recent commands, and managed node state.

Every mutation carries a caller-generated command ID. The daemon persists the full command record and node intent before provider work. Reusing an ID returns the original record and never repeats create or destroy. Node IDs are monotonic and never reused.

## Durable state

The state directory contains:

- `identity.key`: 32-byte iroh secret key. Preserving it keeps the daemon endpoint stable across restarts.
- `cluster.json`: schema-versioned snapshot containing the stable provider label, run id, next node id, full command records, node specs, selected offer IDs, provider references, runtime facts, phases, and errors.

Writes use a temporary file plus rename. A corrupt identity or snapshot is a hard startup error. `--reset-state` explicitly clears both files; startup never treats corruption as an empty fleet.

Provider state is ground truth during recovery:

- snapshot + provider resource: adopt and observe it;
- snapshot only: mark the node stopped; never recreate it;
- provider resource without managed intent: report it as an orphan and take no action.

The current orchestrator actor address is published under `myelin.manual-control` in the distributed name registry. A surviving worker observes a changed binding, sends `RejoinHello` with its persisted logical identity and current runtime facts, waits for the daemon to persist those facts, and only then rebinds to the returned actor address and control generation. No stable actor address is assumed.

## Lifecycle policy

Graceful shutdown leaves Docker containers and Vast.ai leases running so a later daemon can adopt them. Local process children are different: they cannot be adopted, so Ctrl-C stops them and clears their snapshot records. They also exit when their daemon-owned stdin supervision pipe closes, preventing an abrupt daemon crash from leaving invisible local workers.

Destruction of durable provider resources occurs only through an explicit dashboard command. The default process path re-enters the running orchestrator executable in an internal worker mode, so `cargo run -p myelin` never depends on a separately built or stale `myelin-worker` binary.

## Node image contract

The standard image contains a uniform Myelin agent, SSH bootstrap, and the CUDA runtime. It does not contain tinygrad, NumPy, PyTorch, vLLM, or model weights. Frameworks and application dependencies belong to job payload images. Nodes launched by this daemon set `MYELIN_AGENT_ONLY=1`, so the agent joins membership, announces readiness, and exports telemetry without starting an inference helper.
As part of runtime-ready bootstrap, the daemon dials the node's advertised iroh endpoint on `TELEMETRY_ALPN`, requests all telemetry channels, and retains that pull stream for the node's lifetime. The node serves the pull locally; it never needs to reverse-dial the dashboard. Pulled stream descriptors and frames feed both the Fleet view and the live telemetry explorer.

The retired chat/GGUF specification is archived at `archive/MYELIN_CHAT_SPEC.md`.
