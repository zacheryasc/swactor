# EdgeEstablisher Contract

This document defines the behavioral contract for local edge establishment. It
turns `ProvisionTx` or `ProvisionRx` into a local edge actor, arena lease,
worker ring installation, driver establishment, and ready/fault outcome.

## Provisioning

- Each node has one `EdgeEstablisher`.
- `ProvisionTx` creates a local send edge record.
- `ProvisionRx` creates a local receive edge record.
- The producer needs the consumer `node_id`.
- The consumer needs the shared `edge_id`.
- Remote actor addresses are not required for data flow.

## Lease Flow

- Establishment requests one arena lease per edge end.
- `RingLeased` matching the request advances the edge record.
- `RingLeaseRejected` matching the request fails the edge record.
- Stale lease events for stopped records do not install worker or pump state.
- An unused fresh lease granted after cancellation is released.

## Worker Ring Installation

- The edge establishes driver state only after worker or token endpoint ring
  installation succeeds.
- `RingInstalled` matching the edge advances establishment.
- `RingFault` before readiness stops establishment.
- Ring installation uses the `ObjectSpec` and `RingSpec` from provisioning.

## Driver Establishment

- Send edges call `EstablishSend` with `edge_id`, consumer `node_id`, and local
  ring layout.
- Receive edges call `EstablishRecv` with `edge_id` and local ring layout.
- `DriverEdgeReady` marks the local edge actor ready.
- Stream and pump behavior are driver-owned after readiness.

## Ready And Hot Path

- Ready means local actor, arena lease, worker/token ring, and driver state are
  installed.
- The hot path runs without `EdgeEstablisher`.
- The establisher observes coarse ready, stopped, and fault events only.

## Stopping

- `StopEdge` cancels queued leases.
- `StopEdge` stops pumps if present.
- `StopEdge` uninstalls worker rings if installed.
- The arena lease is released only after quiescence proof.
- `Stopped` is terminal for that edge record.

## Test Direction

Tests should drive the public establishment FSM with lease, worker, driver, and
stop events. Successful tests should assert the order lease -> install ->
driver -> ready. Fault tests should inject lease rejection, ring fault, driver
fault, stale events, and stop races and assert no hot-path state survives
incorrectly.
