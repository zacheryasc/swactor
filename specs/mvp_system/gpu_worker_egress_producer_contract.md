# GPU Worker Egress Producer Contract

This document defines the behavioral contract for worker-side egress
production. The worker writes object records to an egress ring from explicit
`ExecuteStep` output bindings.

## Output Admission

- Egress production starts only after `InstallRing(direction = Egress)`.
- The worker writes output only for an `ExecuteStep` output binding naming that
  ring.
- The output binding supplies object id, sequence, extent, and flags.
- The worker does not invent graph-visible object ids or sequence numbers.

## Header Production

- The worker creates an `ObjectHeader` according to the edge `ObjectSpec`.
- Header bytes are written before payload bytes.
- `commit` advances only after header bytes are valid.
- `RingReadable` is emitted or coalesced after committed header bytes.

## Payload Production

- The worker copies exactly `extent` bytes from device to the egress ring.
- `commit` advances only after host bytes are valid.
- Objects larger than the ring may stream through bounded ring spans.
- If no writable span exists, `ExecuteStep` may block on egress backpressure.

## ObjectProduced

- `ObjectProduced` is emitted after the full output object is committed.
- `StepCompleted` is emitted only after all declared outputs are produced and
  role state updates are complete.
- `ObjectProduced` includes ring id, edge id, port id, object id, sequence, and
  extent.

## Fault Behavior

- Invalid output ring fails the step.
- Output extent violation fails the step.
- Device copy failure fails the step or faults the ring.
- Worker shutdown rejects or aborts output production according to shutdown
  mode.

## Test Direction

Tests should drive `ExecuteStep` with fake device outputs and bounded egress
rings. Success tests should assert header-before-payload, exact extent, cursor
publication, `ObjectProduced`, and `StepCompleted` ordering. Fault tests should
cover invalid output ring, invalid extent, backpressure, and copy failure.
