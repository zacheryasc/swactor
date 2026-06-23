# GPU Worker Ingress Parser Contract

This document defines the behavioral contract for worker-side ingress parsing.
The worker parses object records from an ingress ring and emits `ObjectLoaded`
only after a complete logical object is available on device.

## Ring Admission

- Ingress parsing starts only after `InstallRing(direction = Ingress)`.
- The worker reloads cursors after `RingReadable`.
- The parser consumes committed bytes only.
- The parser does not read uncommitted bytes.

## Header Validation

- The parser waits until a complete `ObjectHeader` is committed.
- Unsupported magic rejects the object.
- Unsupported version rejects the object.
- Malformed header length rejects the object.
- `extent > ObjectSpec.max_extent` rejects the object.
- Extent alignment or layout violation rejects the object.
- Sequence violation rejects the object.

## Payload Loading

- Payload content values are trusted.
- The parser copies exactly `extent` payload bytes to device memory.
- `consume` advances only after copied bytes are safe to release.
- Objects larger than the ring may stream through bounded ring spans.
- EOF before the full payload faults the object.

## ObjectLoaded

- `ObjectLoaded` is emitted only after valid header, exact extent copy, copy
  completion, and device handle creation.
- `ObjectLoaded` includes ring id, edge id, port id, object id, sequence,
  extent, and device handle.
- The emitted handle belongs to the current worker generation.

## Fault Behavior

- Header malformed emits `ObjectFailed`.
- Extent exceeds max emits `ObjectFailed`.
- Sequence violation emits `ObjectFailed`.
- Device allocation or copy failure emits `ObjectFailed` or `RingFault`.
- After ring fault, the worker stops consuming until uninstall.

## Test Direction

Tests should write object records through the public ring helper and observe
worker events. Success tests should assert `ObjectLoaded` only after complete
payload and copy completion. Fault tests should cover malformed headers,
oversized extent, bad sequence, EOF mid-object, and copy failure.
