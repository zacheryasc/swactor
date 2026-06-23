# ArenaManager Contract

This document defines the behavioral contract for node-local arena management.
The `ArenaManager` mints stable arena-relative ring layouts and releases ranges
only after quiescence proof.

## Arena Formation

- The node constructs one sparse arena.
- The arena has one reservation ceiling.
- The arena mapping is stable for the node lifetime.
- Arena layouts contain offsets, not process-local pointers.
- Arena boot failure emits a typed arena fault.

## Ring Lease

- `LeaseRing` either emits `RingLeased`, queues the request, or emits
  `RingLeaseRejected`.
- A satisfiable request may queue under temporary arena pressure.
- A request that can never fit within the ceiling is rejected.
- A live lease has one `RingId`.
- `RingId` is unique for the node lifetime.

## Layout Safety

- Live leases do not overlap.
- Every lease lies within the arena ceiling.
- Every lease satisfies the requested alignment.
- Header and data offsets are stable for the lease lifetime.
- A layout never exposes process-local pointers.

## Cancellation

- `CancelLease` removes a queued request that has not been leased.
- A canceled queued request does not later install worker or pump state.
- If a fresh lease races with cancellation, it is released without becoming
  hot-path state.

## Release

- `ReleaseRing` requires quiescence proof.
- The arena manager does not infer quiescence.
- Released ranges may be reused after release.
- Ranges are not reused while a live pump, worker ring, or copy operation still
  owns them.

## Shutdown

- Shutdown rejects new leases.
- Shutdown does not corrupt existing live lease records.
- Shutdown does not release live ranges without proof.

## Test Direction

Tests should issue deterministic lease, cancel, release, and shutdown messages
and inspect public lease events. Successful tests should assert non-overlap,
alignment, reuse after release, and queue retry. Failure tests should assert
oversized rejection, canceled lease suppression, and no release without proof.
