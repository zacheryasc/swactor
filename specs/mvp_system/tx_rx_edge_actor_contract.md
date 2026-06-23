# Tx And Rx Edge Actor Contract

This document defines the behavioral contract for Tx and Rx edge actors. They
are role-facing lifecycle gates and never carry payload bytes.

## Actor Role

- A Tx actor represents the producer side of one edge.
- An Rx actor represents the consumer side of one edge.
- Each edge actor is tied to one `edge_id`.
- Edge actors receive lifecycle and object events only.
- Edge actors do not receive bytes, pointers, ranges, credits, or free-space
  counts.

## Tx Lifecycle

- Tx starts in provisioning.
- `EdgeReady` moves Tx to ready.
- Producing is allowed only after ready.
- `ObjectProduced` reports committed output object identity.
- Stream fault or object fault moves Tx to faulted.
- `StopEdge` moves Tx toward stopped.

## Rx Lifecycle

- Rx starts in provisioning.
- `EdgeReady` moves Rx to ready.
- `ObjectLoaded` reports complete logical input object identity and handle.
- Rx exposes loaded objects to the role layer only after `ObjectLoaded`.
- Object failure or stream fault moves Rx to faulted.
- `StopEdge` moves Rx toward stopped.

## Payload Isolation

- Payload bytes never travel in edge actor messages.
- Host pointers never travel in edge actor messages.
- Flow-control details never travel in edge actor messages.
- Edge actors traffic only identities, lifecycle events, opaque handles, and
  coarse faults.

## Fault Behavior

- `ObjectFailed` faults the corresponding edge actor.
- `StreamFault` faults the corresponding edge actor.
- Stale events for stopped actors are ignored.
- Events for a mismatched `edge_id` reject or fault according to local policy.

## Test Direction

Tests should drive Tx and Rx actors with public lifecycle and object events.
Successful tests should assert readiness before production/loading and payload
isolation in message types. Fault tests should inject stream and object faults
and assert no further run work is accepted before stop.
