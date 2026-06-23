# GPU Worker Process Adapter Contract

This document defines the behavioral contract for the stdin/stdout adapter
between `GpuWorkerCtl` and the Python/tinygrad worker process. The adapter is
not a distributed protocol.

## Control Stream Shape

- Commands are one JSON object per stdin line.
- Events are one JSON object per stdout line.
- Stderr is reserved for logs and diagnostics.
- Payload bytes are forbidden in JSON commands.
- Payload bytes are forbidden in JSON events.

## Initialization

- The worker reads arena environment variables.
- The worker waits for `InitializeWorker`.
- The worker maps the arena.
- The worker initializes the native ring helper.
- The worker initializes backend/tinygrad.
- Success emits `WorkerReady`.
- Failure emits `WorkerFatal` if possible and exits non-zero.

## Parsing

- Invalid JSON is a worker/process fault.
- Unknown event shape is a worker/process fault.
- Unsupported helper ABI emits `WorkerFatal`.
- Stderr output alone is diagnostic and does not define lifecycle state.

## Command Discipline

- `InstallRing` installs worker-side ring state.
- Wake hints reload ring cursors.
- `ExecuteStep` runs explicit role compute.
- `ReleaseDeviceObject` releases handles when safe.
- `ShutdownWorker` moves the worker toward draining.

## Test Direction

Tests should drive the adapter with fake stdin/stdout lines. Successful tests
should assert one-line command/event framing and initialization ordering. Fault
tests should inject malformed JSON, unknown events, ABI mismatch, and payload
bytes in control messages.
