# Myelin Fleet-Control Daemon

Myelin is a persistent, dashboard-first control plane for manually managed compute nodes. It boots an empty fleet, accepts explicit operator commands, records intent and observations, and never performs hidden replacement or teardown.

## Runtime shape

```text
myelin-orchestrator
  -> load stable iroh identity and cluster snapshot
  -> start engine, iroh endpoint, telemetry collector, and dashboard
  -> adopt provider resources carrying this daemon's stable label
  -> idle event loop
       - pump telemetry and membership
       - dispatch add / kill / destroy commands
       - atomically persist every state transition
  -> on exit, detach provider handles without destroying resources
```

There is no prompt RPC, chat loop, desired-shape reconciler, or automatic node replacement. GGUF pipeline code remains dormant for historical compatibility. Job submission and a node re-join handshake are deferred.

## Starting the daemon

From the workspace root:

```sh
cargo run -p myelin
```

The local default uses the process provider. Use Docker explicitly when required:

```sh
cargo run -p myelin -- --provider docker
```

`MYELIN_DASHBOARD_PORT` selects the dashboard port. The dashboard root is the fleet-control view. Its control routes dispatch the same provider-neutral commands intended for a future CLI:

- `Provision { command_id, count }`: add exactly `count` nodes, one transaction at a time.
- `Kill { command_id, node }`: stop a node but retain its dead snapshot record.
- `Remove { command_id, count }`: destroy the highest numbered managed nodes and remove their snapshot records.
- `EstablishEdge { command_id, node }`: rejected; workload topology is not part of fleet control.

Every request carries a caller-generated command id. The daemon persists that id before any provider mutation; retries are ignored across restarts. This is deliberately at-most-once: a crash after acceptance may require a new operator command, but can never double-rent or double-destroy a resource. Node ids are monotonic and never reused.

## Durable state

The state directory contains:

- `identity.key`: 32-byte iroh secret key. Preserving it keeps the daemon endpoint stable across restarts.
- `cluster.json`: schema-versioned snapshot containing the stable provider label, run id, next node id, accepted command ids, node specs, provider references, runtime facts, and observed status.

Writes use a temporary file plus rename. A corrupt identity or snapshot is a hard startup error. `--reset-state` explicitly clears both files; startup never treats corruption as an empty fleet.

Provider state is ground truth during adoption:

- snapshot + provider resource: adopt and observe it;
- snapshot only: mark dead;
- provider resource only: report as an orphan and take no action.

The current restart limitation is deliberate: an adopted node still has the prior orchestrator actor address in its environment. Provider monitoring and telemetry collection can resume, but actor-address re-join requires the deferred node handshake.

## Lifecycle policy

Graceful shutdown leaves Docker containers and Vast.ai leases running so a later daemon can adopt them. Local process children are different: they cannot be adopted, so Ctrl-C stops them and clears their snapshot records. They also exit when their daemon-owned stdin supervision pipe closes, preventing an abrupt daemon crash from leaving invisible local workers.

Destruction of durable provider resources occurs only through an explicit dashboard command or the development-only `--destroy-on-exit` flag. The default process path re-enters the running orchestrator executable in an internal worker mode, so `cargo run -p myelin` never depends on a separately built or stale `myelin-worker` binary.

## Node image contract

The standard image contains a uniform Myelin agent, SSH bootstrap, and the CUDA runtime. It does not contain tinygrad, NumPy, PyTorch, vLLM, or model weights. Frameworks and application dependencies belong to job payload images. Nodes launched by this daemon set `MYELIN_AGENT_ONLY=1`, so the agent joins membership, announces readiness, and exports telemetry without starting an inference helper.
As part of runtime-ready bootstrap, the daemon dials the node's advertised iroh endpoint on `TELEMETRY_ALPN`, requests all telemetry channels, and retains that pull stream for the node's lifetime. The node serves the pull locally; it never needs to reverse-dial the dashboard. Pulled stream descriptors and frames feed both the Fleet view and the live telemetry explorer.

The retired chat/GGUF specification is archived at `archive/MYELIN_CHAT_SPEC.md`.
