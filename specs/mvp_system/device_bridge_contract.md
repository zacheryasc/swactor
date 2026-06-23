# Device Bridge Contract

This document defines the behavioral contract for the backend-specific device
bridge used by the GPU worker. The bridge is the worker's boundary between host
ring memory, device memory, and tinygrad-compatible views.

## Allocation

- `alloc_device(ObjectSpec, extent)` creates a device allocation suitable for
  the object spec and extent.
- Allocation failure is reported as device allocation failure.
- Allocations are tied to the current worker generation.
- `free_device` releases an allocation after no compute or copy event depends
  on it.

## Host To Device

- `host_to_device` copies exactly the requested host range to the requested
  device range.
- For synchronous copies, return means bytes are safe to release.
- For asynchronous copies, completion of the copy event means bytes are safe to
  release.
- Copy failure is reported as device copy failure.

## Device To Host

- `device_to_host` copies exactly the requested device range to the requested
  host range.
- For synchronous copies, return means host bytes are valid.
- For asynchronous copies, completion of the copy event means host bytes are
  valid.
- Copy failure is reported as output copy failure or device copy failure.

## Tinygrad View

- `wrap_for_tinygrad` creates a tinygrad-compatible view over a device
  allocation.
- The view matches the tensor view spec used by the role.
- Invalid view shape or dtype fails the step.

## Lifetime

- Device allocations are not freed while compute uses them.
- Device allocations are not freed while copy events use them.
- Worker restart invalidates all prior device handles.
- Old-generation handles are rejected.

## Test Direction

Tests should use a fake backend to observe allocation, copy, completion, view,
and free calls. Success tests should assert exact ranges and safe cursor-release
points. Fault tests should inject allocation failure, copy failure, invalid
view, and old-generation handles.
