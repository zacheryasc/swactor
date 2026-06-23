# Shared Ring Helper ABI Contract

This document defines the behavioral contract for the shared ring ABI and native
helper. Both Rust hot-path code and the Python worker access process-crossing
rings through this helper.

## Ring Identity

- A ring is a bounded single-producer/single-consumer byte stream.
- Each ring has exactly one producer and one consumer.
- `RingId` is unique for the node lifetime.
- Stale wake events cannot alias replacement rings.

## Cursor Contract

- `commit` is the first byte after the committed readable prefix.
- `consume` is the first byte not yet released by the consumer.
- Producer-local `write` does not expose bytes to the consumer.
- Cursor values are monotonic logical byte positions.
- Physical indices are derived by `cursor % capacity`.

## Producer Rules

- The producer computes free space from acquired `consume`.
- The producer does not reserve beyond ring capacity.
- The producer writes bytes before publishing `commit`.
- Publishing `commit` uses release ordering.
- After publishing readable bytes, the producer sends or coalesces
  `RingReadable`.

## Consumer Rules

- The consumer computes readable bytes from acquired `commit`.
- The consumer does not read beyond committed bytes.
- The consumer advances `consume` only after bytes are safe to release.
- Publishing `consume` uses release ordering.
- After releasing space, the consumer sends or coalesces `RingWritable`.

## Wake Rules

- Wake hints are edge-trigger hints.
- Wake hints carry no byte ranges, counts, pointers, or credits.
- Receivers reload cursors from shared memory.
- Duplicate wakes may be coalesced only while durable scheduler state still
  makes the ring discoverable.
- Losing the only empty-to-readable or full-to-writable transition is a liveness
  bug.

## Helper ABI

- Python does not implement shared atomics directly.
- Python does not implement wrap arithmetic directly.
- Returned pointers are process-local addresses derived from arena base plus
  arena offsets.
- Helper operations use acquire/release semantics across the process boundary.

## Test Direction

Tests should use the public helper operations to exercise wraparound, full,
empty, publish, consume, and wake behavior. Safety tests should assert no
unwritten reads and no unread overwrite. Liveness tests should assert that wake
coalescing cannot hide a discoverable readable or writable transition.
