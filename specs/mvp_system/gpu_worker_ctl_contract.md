# GpuWorkerCtl Contract

This document defines the behavioral contract for the Rust-side GPU worker
controller. `GpuWorkerCtl` owns the supervised worker process boundary and
routes worker events to local control components.

## Worker Lifecycle

- `StartWorker` spawns the process actor and process bridge.
- After process start, `GpuWorkerCtl` sends `InitializeWorker`.
- `WorkerReady` moves the controller to running.
- `WorkerFatal`, process exit, or initialization timeout moves the controller
  to failed or crashed state.
- Worker generation increments on restart.

## Command Routing

- In running state, valid actor commands are serialized to the worker.
- `InstallRing` is sent only for the current worker generation.
- `ExecuteStep` is sent only with current-generation handles.
- `ReleaseDeviceObject` is sent only for current-generation handles.
- Payload bytes are never sent through worker control messages.

## Event Routing

- Worker events are parsed from process stdout.
- `RingInstalled` routes to edge establishment.
- `ObjectLoaded` routes to Rx or role layer.
- `ObjectProduced` routes to Tx or role layer.
- `StepCompleted` and `StepFailed` route to the StageController.
- Wake events route to the driver or worker side as appropriate.

## Crash Behavior

- Worker crash invalidates old device handles.
- Worker crash invalidates installed roles, rings, and in-flight steps.
- The controller synthesizes ring faults for installed rings.
- The controller asks the driver to stop pumps for affected rings.
- Old-generation handles are rejected after restart.

## Shutdown

- Graceful shutdown sends `ShutdownWorker`.
- Timeout escalates according to local process policy.
- Shutdown marks installed rings faulted or quiesced according to observed
  worker/process outcome.
- `WorkerStopped` is observed before terminal stopped when graceful shutdown
  succeeds.

## Test Direction

Tests should use a fake process adapter and public controller messages. Success
tests should assert start -> initialize -> ready, command serialization, and
event routing. Fault tests should inject malformed stdout, worker fatal, process
exit, restart, and old-generation handles and assert invalidation and fanout.
