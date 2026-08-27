# swactor data-plane descriptor API — specification

Id: 6
Last modified: c705c7428960d287f190cad6bfbf57c193da31df
Last reviewed:
> Any edit to this spec must update `Last modified` above to the current `git HEAD` commit.

**Status: implemented (first migration milestone).** This is the normative contract for the path-first, descriptor-based data-plane core. The POSIX expansion in Phase F remains future work.

## 1. Purpose

swactor is a distributed operating-system substrate. A process addresses data and devices by human-readable namespace paths; the data-plane authority performs the kernel role of resolving and authorizing those paths, serializing lifecycle transitions, and issuing capabilities. Once opened, data moves through the most direct available path rather than through the authority.

The data-plane core therefore uses the Unix shape:

```text
open(path, flags) -> descriptor
read(descriptor, destination) -> byte count
write(descriptor, source) -> byte count
map(descriptor, request) -> mapping
close(descriptor)
```

Blobs, streams, and future devices are object kinds behind this common descriptor contract. Typed blob and stream APIs remain the ordinary user-facing API and are implemented only as wrappers over the descriptor core. Raw descriptors are opt-in.

This migration is primarily an API and lifecycle unification. It must preserve the existing arena leases, mapped blobs, SPSC rings, stream transport, namespace authority, operation actors, backpressure, and zero-copy paths wherever their behavior already satisfies this contract.

## 2. Decision order

When design constraints conflict, implementations MUST apply this order:

1. **Unix/POSIX semantics win.** A supported operation must behave like its Unix/POSIX counterpart at the API boundary: access checks, partial I/O, offsets, EOF, blocking, close, and stable error classes.
2. **Preserve the existing data path.** Existing mappings, rings, bindings, transfer actors, namespace serialization, and lease fencing remain unless they cannot meet rule 1.
3. **Choose the smallest implementation change.** Generalize or wrap existing machinery before replacing it.
4. **Reject rather than imitate incorrectly.** A POSIX operation that cannot yet be implemented correctly MUST fail with `ENOTSUP`/`EOPNOTSUPP`; it must not expose a similarly named operation with conflicting behavior.

Swactor-specific object behavior MUST be explicit. In particular, a versioned blob may publish atomically at clean close, and a device may define controls beyond POSIX. Those are properties of the opened object, not alternative meanings for `read` or `write`.

## 3. Scope

### 3.1 First migration milestone

The first milestone includes:

- one path-first `open` operation;
- read-only, write-only, and read-write access modes where the object supports them;
- descriptor `read`, `read_into`, `write`, `write_from`, `map`, `close`, and abnormal `abort`;
- sequential descriptor offsets for finite blobs;
- partial-I/O and zero-length-I/O semantics;
- EOF, backpressure, blocking open, cancellation, and peer-failure semantics;
- blob and stream descriptor backends using the current arena and ring implementations;
- host and arena memory-region I/O, with a representation that can import device/GPU regions without changing descriptor APIs;
- existing Rust and Python blob/stream APIs rebuilt as descriptor wrappers;
- an opt-in raw Python descriptor API;
- structured errors with stable errno mappings;
- comprehensive positive state-sequence, negative action, concurrency, fault, and mutation-adequacy tests.

### 3.2 Required extension points, not first-milestone implementations

The types and ownership model MUST admit these without changing the meaning of first-milestone APIs:

- `pread`, `pwrite`, `readv`, and `writev`;
- `fstat`, `lseek`, `ftruncate`, and general file growth;
- `poll`/readiness and `O_NONBLOCK`;
- numeric per-process descriptor tables;
- `dup`, descriptor inheritance, and shared open-file descriptions;
- `openat`, directory descriptors, `rename`, and `unlink`;
- `mmap` variants, synchronization, pinned host memory, DMA-BUF, CUDA/device mappings, and direct registered-memory transport;
- `ioctl` and device-specific descriptor operations;
- process standard streams and redirection.

Unsupported flags or operations MUST fail at open/bind time when possible and otherwise at the operation with `ENOTSUP`. Capability discovery MUST make support inspectable.

### 3.3 Out of scope

- Replacing actor execution or the engine.
- Routing payload bytes through the orchestrator or namespace actor.
- Exposing arena offsets, ring framing, actor addresses, or transport descriptors as the ordinary API.
- Replacing the existing path syntax or `/runs/self` resolution.
- Making raw numeric descriptors the default Python interface.
- Claiming complete POSIX filesystem conformance in the first milestone.

## 4. Current implementation and migration rule

The current implementation is already close to the required kernel/data-path split:

| Current code | Existing responsibility | Migration treatment |
|---|---|---|
| `path.rs::DataPath`, `SessionAccess` | path validation, `/runs/self` resolution, read/write prefix authorization | retain |
| `namespace.rs::DataDirectoryActor` | authoritative, linearizable blob bindings and stream rendezvous | retain as namespace authority; add generic lookup/open metadata only where required |
| `host.rs::HostDataPlaneSessionActor` | session validation, authorization, binding ownership and cleanup | retain as host-side descriptor authority |
| `data_plane.rs::ChildDataPlaneSessionActor` | child attachment, operation correlation, cancellation | retain; generalize open entry point |
| `ReadBlobOperationActor`, `WriteBlobOperationActor` | per-open blob lifecycle | retain as blob descriptor backends |
| `HostBlobBindingActor` | allocation, transfer, publication and release | retain |
| `StreamOpenOperationActor`, `HostStreamBindingActor` | stream rendezvous, readiness, peer and close lifecycle | retain as stream descriptor backends |
| `blob.rs::{Blob, BlobWriter, BlobView, WritableArenaView}` | stable mapped leases, publication fencing and view lifetime | retain; place behind descriptor operations |
| `byte_ring.rs::Endpoint` | bounded SPSC framed byte movement | retain; add allocation-free partial-record consumption |
| `stream_transport.rs` | local/remote ring bridging | retain |
| Python `DataPlane` and typed classes | ergonomic path and context-manager API | retain surface; bind through descriptors |

There MUST be one semantic implementation of open/read/write/close. Typed wrappers may narrow capabilities and add policy, but MUST NOT send separate transport operations that bypass descriptor semantics.

Internal actor messages may remain specialized when that is the smallest implementation. A generic `open` may dispatch to the existing blob or stream open actor after namespace resolution. Specialized wire messages are implementation details, not separate public contracts.

## 5. Object and capability model

### 5.1 Namespace node

A namespace node binds a `DataPath` to an object kind and current revision/incarnation metadata. First-milestone kinds are:

```text
Blob    finite, versioned, readable and optionally writable/mappable
Stream  sequential, non-seekable, readiness-driven endpoint
```

Future kinds include directories, devices, sockets, processes, and anonymous memory objects.

**Guarantees — namespace nodes**

- N1. Each path has at most one object kind at one namespace revision.
- N2. Rebinding a path is linearized by the namespace authority.
- N3. A descriptor opened before rebinding remains attached to the object revision/incarnation selected by its successful open.
- N4. Rebinding cannot mutate or redirect an already-open descriptor.
- N5. Blob and stream type conflicts fail explicitly; no operation interprets the same live node as both kinds.
- N6. `/runs/self` is resolved exactly once during open, before authorization and binding.
- N7. Authorization is checked against the resolved path, not the caller spelling.
- N8. Paths remain human-readable primary identifiers; descriptors are capabilities obtained from paths.

The existing namespace stream rendezvous may be retained internally. If stream nodes are made persistent or predeclared, creation is a namespace-control operation analogous to `mkfifo`, not a new stream-specific read/write API. The high-level `read_stream`/`write_stream` wrappers may atomically ensure the stream node and open it to preserve their current one-call simplicity.

### 5.2 Open-file description

An open-file description owns the state shared by operations on one open object:

- selected namespace revision/incarnation;
- object kind;
- granted access mode and capabilities;
- current byte offset where the object is seekable/sequential finite data;
- backend binding and local arena/ring/device attachment;
- clean-close, fault, EOF, and cancellation state;
- references held by descriptors and mappings.

The first implementation may represent this using the existing operation/binding actor addresses and process-local Rust state. It MUST remain separable from a future numeric descriptor table so `dup` can later share an open-file description and its offset.

**Guarantees — open-file descriptions**

- O1. A successful open selects exactly one object and one access mode.
- O2. Concurrent open replies are correlated to their initiating request; no grant, error, lease, or ring may cross-wire between requests.
- O3. The object binding is stable until the final descriptor/mapping reference is released.
- O4. Offset-changing operations on one description are serialized and update the offset atomically with their returned byte count.
- O5. Rights can only stay the same or narrow after open; wrappers, mappings, imports, and future duplication cannot amplify them.
- O6. Exactly one terminal lifecycle wins: clean close, abnormal abort/fault, or session teardown.
- O7. Terminal completion is sticky. Later operations cannot revive or replace the open description.

### 5.3 Descriptor

A descriptor is the process-local capability used by callers. In Rust it is an owned object, not initially an integer:

```rust
pub struct Descriptor { /* private */ }

impl DataPlane {
    pub async fn open(
        &self,
        path: &DataPath,
        options: OpenOptions,
    ) -> Result<Descriptor, DataPlaneError>;
}
```

Conceptual first-milestone options:

```rust
pub enum AccessMode { ReadOnly, WriteOnly, ReadWrite }

pub struct OpenOptions {
    pub access: AccessMode,
    pub create: bool,
    pub exclusive: bool,
    pub truncate: bool,
    pub nonblocking: bool,
    pub allocation: Option<BlobAllocation>,
}

pub struct BlobAllocation {
    pub length: u64,
    pub digest: Option<ContentDigest>,
}
```

The canonical binding SHOULD use POSIX flag names or lossless equivalents (`O_RDONLY`, `O_WRONLY`, `O_RDWR`, `O_CREAT`, `O_EXCL`, `O_TRUNC`, `O_NONBLOCK`). It MUST reject invalid or unsupported combinations.

`allocation` is an explicit Swactor extension used by the existing fixed-length staged-blob backend. It is valid only for a creating/truncating writable blob open; other uses fail with `EINVAL`. It does not select stream versus blob for an existing path. Creating an absent path creates the default finite blob kind; named streams are ensured/created by namespace control before ordinary descriptor open.

A successful descriptor reports object kind and capabilities in its open grant so typed wrappers do not need another authority round trip.

```text
READ | WRITE | MAP_HOST | MAP_DEVICE | SEEK | POLL | CONTROL
```

Capabilities describe supported operations; access mode describes rights granted to this descriptor. Both checks apply.

First-milestone operation support:

| opened object/access | `read` | `write` | host `map` | clean `close` | `abort` |
|---|---:|---:|---:|---:|---:|
| blob, read-only | yes | `EBADF` | read-only | release descriptor | abnormal release |
| staged blob, write-only | `EBADF` | yes | writable | request publication | discard staging |
| staged blob, read-write | yes | yes | writable | request publication | discard staging |
| stream, read-only | yes | `EBADF` | `ENODEV` | detach reader | fault/detach |
| stream, write-only | `EBADF` | yes | `ENODEV` | flush, EOF, detach | fault/detach |

Support for read-write stream opens, seeking, and mapping stream objects is not implied by the presence of the corresponding generic descriptor operations. Unsupported combinations fail explicitly.

**Guarantees — descriptors**

- D1. Descriptor identity is unforgeable at the public API surface.
- D2. Operations after successful `close` fail with `EBADF` at the raw descriptor surface.
- D3. A second raw `close` fails with `EBADF`; high-level wrappers may make their own `close` idempotent by remembering that they already closed.
- D4. Dropping an unclosed descriptor is abnormal cleanup, not a successful close. A staged blob is not published and a stream peer observes a fault/unclean close.
- D5. Closing one descriptor cannot close another independently opened descriptor for the same path.
- D6. Session teardown eventually terminates every descriptor owned by the session and releases its bindings and leases exactly once.
- D7. Descriptor metadata never exposes actor addresses, arena offsets, ring headers, or transport-private descriptors unless an explicitly privileged debugging interface is used.

## 6. `open`

`open` performs, in order:

1. validate the path and flags;
2. resolve aliases such as `/runs/self`;
3. authorize requested read/write rights;
4. resolve or create the namespace node according to supported flags;
5. select and retain its revision/incarnation;
6. create the existing backend binding;
7. attach the local arena/ring/region fast path;
8. return one descriptor grant containing kind and capabilities.

The data-plane authority serializes namespace and lifecycle decisions. It MUST NOT proxy ordinary payload bytes after the open grant.

### 6.1 Blob open

- Read-only open selects the currently published blob revision.
- First-milestone writable blob open supports the current staged, fixed-length creation path used by `write_blob`. The length is object creation/allocation metadata supplied by the typed wrapper.
- Raw general file growth and non-truncating modification are unsupported until their POSIX behavior can be implemented; unsupported combinations fail rather than silently truncating or snapshot-replacing.
- A clean close of the staged writable descriptor publishes one sealed revision. Abnormal abort publishes nothing.
- Existing readers retain their selected revision after publication of a replacement.

The fixed-length creation parameter is a Swactor blob extension, not a redefinition of POSIX write. When `ftruncate` lands, the high-level `length` argument SHOULD lower to `open` plus `ftruncate`; an internal fused request is permitted if its observable result is identical.

### 6.2 Stream open

A stream behaves as a named FIFO for open and byte-I/O semantics:

- blocking read-open waits for a producer;
- blocking write-open waits for a consumer;
- pairing is symmetric;
- cancellation removes the waiting endpoint;
- only the selected stream incarnation is attached;
- replacement creates a fresh incarnation and faults/displaces the old waiter according to explicit replacement policy.

`O_NONBLOCK` is not partially emulated. Until fully supported, it fails with `ENOTSUP`. When implemented, FIFO open/read/write behavior follows POSIX, including `ENXIO` for a nonblocking writer with no reader.

### 6.3 Open guarantees

- OP1. An unsuccessful open returns no usable descriptor and leaks no waiter, binding, lease, ring, or namespace mutation not explicitly requested by flags.
- OP2. `O_EXCL|O_CREAT` is atomic with respect to competing creators.
- OP3. Authorization failure occurs before object data or transport capabilities are exposed.
- OP4. Opening a missing path without creation fails with `ENOENT`, except a high-level stream wrapper may first perform its documented ensure-node operation.
- OP5. Opening with unsupported access or flags fails deterministically; it does not downgrade access.
- OP6. A blocked open is cancellable. Once cancellation completes, a late grant is rejected and reclaimed.
- OP7. Concurrent opens of different paths and kinds cannot cross-wire.
- OP8. A session not in `Running` state rejects new opens.
- OP9. A stream open completes only after transport attachment is ready on both matched endpoints.
- OP10. A transport-start failure fails both matched opens and leaves no live incarnation.

## 7. `read`, `read_into`, `write`, and `write_from`

The allocation-free primitives operate on caller-provided memory:

```rust
impl Descriptor {
    pub async fn read(&mut self, destination: &mut [u8])
        -> Result<usize, DataPlaneError>;

    pub async fn write(&mut self, source: &[u8])
        -> Result<usize, DataPlaneError>;

    pub async fn read_into(&mut self, destination: RegionSlice<'_>)
        -> Result<usize, DataPlaneError>;

    pub async fn write_from(&mut self, source: RegionSlice<'_>)
        -> Result<usize, DataPlaneError>;
}
```

Exact Rust types may differ. The contract does not.

High-level `read(n)` may allocate a result buffer. High-level `write_all` loops over partial writes. Those are wrappers, never the primitive implementation.

### 7.1 Common I/O guarantees

- IO1. A successful operation returns a count no greater than the supplied region length.
- IO2. Only the returned prefix is read from or written to. Bytes outside that prefix and outside the supplied range remain untouched.
- IO3. Zero-length I/O on a valid descriptor returns zero immediately and has no data, offset, readiness, or EOF side effect.
- IO4. Read on a descriptor without read access and write on one without write access fail with `EBADF`.
- IO5. Partial completion is success. Callers use `read_exact`/`write_all` wrappers when they require full transfer.
- IO6. Bytes are delivered exactly once and in order for each stream/open-file description.
- IO7. A successful sequential read or write advances the open-file offset by exactly the returned count where an offset applies.
- IO8. An error before any byte transfers leaves the offset unchanged. If an implementation can complete bytes before a later fault, it returns the transferred count; the fault is observed by a subsequent operation, matching POSIX partial-I/O behavior.
- IO9. Cancellation has one linearization outcome: either a byte count completes or cancellation completes. The same byte range is never reported both completed and cancelled.
- IO10. No late completion may access a caller region after the operation future/completion has released that region.
- IO11. Multiple independent descriptors cannot corrupt, redirect, duplicate, or consume each other's data.
- IO12. The primitive hot path performs no required per-call heap allocation, payload serialization, or authority actor hop after attachment.

### 7.2 Blob read/write guarantees

- B1. A blob read returns bytes from the descriptor's selected revision, beginning at its current offset.
- B2. Reading at or beyond blob EOF returns zero and remains at EOF until the offset is changed by a future seek operation.
- B3. A read smaller than the remaining blob returns at most the supplied length and advances by that count.
- B4. A writable staged blob accepts writes only inside its supported allocated extent in the first milestone. A write requiring unsupported growth fails with `ENOTSUP` before transferring a byte and leaves the offset and staged bytes unchanged.
- B5. Read-write descriptors, when supported, read the bytes previously written through that staged description.
- B6. A writable descriptor cannot publish while an operation retains mutable access that has not reached a synchronization point.
- B7. Clean publication is at most once. Abort, failed open, dropped open, or failed publication never creates a readable namespace revision.
- B8. A successfully published revision has exactly the declared length and digest, if a digest was required.
- B9. A failed digest or lease validation exposes no bytes and publishes nothing.
- B10. Existing descriptors and mappings continue to reference their original revision after path replacement.

### 7.3 Stream read/write guarantees

- S1. Stream reads expose an unframed byte sequence. Internal ring record boundaries and individual writer call boundaries are not observable API guarantees.
- S2. Reads may return any positive prefix currently available, bounded by destination length.
- S3. EOF is returned as a zero-byte read only after every preceding byte has been delivered.
- S4. EOF is sticky: every subsequent read returns zero without waiting.
- S5. A clean writer close publishes EOF after all accepted bytes and waits for the existing flush condition required to prevent loss.
- S6. Write through an open descriptor after its peer has closed fails with `EPIPE`; write after local descriptor close fails with `EBADF`. Neither case reopens the stream.
- S7. Peer loss or unclean close produces a terminal error, not clean EOF, unless clean EOF was already observed.
- S8. Bounded capacity applies backpressure. A blocking write waits when it cannot accept any byte and resumes after capacity becomes available.
- S9. A partial write may return as soon as at least one byte is accepted. `write_all` is responsible for resubmission.
- S10. Producer and consumer signals follow data publication/consumption; registration plus recheck prevents lost wakeups.
- S11. One incarnation never consumes data, EOF, fault, or readiness from another incarnation.
- S12. The existing first-milestone stream remains one producer and one consumer. Multi-writer atomicity is not claimed until explicitly implemented and tested.

### 7.4 Required ring change

`StreamReader::read()` currently allocates a `Vec<u8>` and consumes one complete ring record. The descriptor primitive requires partial, allocation-free reads. `byte_ring` MUST gain a safe operation that can copy a range of the current committed record without releasing the complete record. `StreamReader` retains a payload offset and releases the record only after its payload is fully returned.

Required ring invariants remain:

- committed bytes are visible only after release publication;
- unread bytes are never overwritten;
- a partially returned record continues to pin its ring capacity;
- EOF/fault framing stays internal and cannot overtake payload bytes;
- wraparound is invisible to descriptor callers;
- dropping unrelated `PinnedRecord` users keeps the existing release-on-drop behavior unless they explicitly request deferred release.

Stream writes MUST select a payload no larger than both source length and currently writable framed capacity. They MUST not allocate a temporary payload solely to satisfy partial-write semantics.

## 8. Mapping and memory regions

Mapping is first-class because blobs are already arena-backed and near-term streaming must target pinned or GPU memory without a CPU allocation/copy API redesign.

### 8.1 Region model

A memory region is a capability over a bounded byte range. Backends may include:

```text
HostBorrowed       caller-owned CPU memory for one operation
Arena              swactor mapped-arena lease
PinnedHost         registered/pinned CPU allocation
Device             GPU/device allocation registration
Imported           DMA-BUF or provider-specific imported memory
```

A region carries or resolves:

- stable identity and generation;
- owner and lifetime;
- readable/writable permissions;
- byte length, offset, and alignment;
- locality/device affinity;
- host accessibility, if any;
- registration/import metadata;
- synchronization/completion requirements;
- supported direct-transfer routes.

Raw virtual addresses MUST NOT cross process or machine boundaries. Protocols carry capability identifiers, offsets, lengths, generations, and backend descriptors that the receiving endpoint validates and imports locally.

### 8.2 Mapping API

Conceptually:

```rust
pub struct MapRequest {
    pub protection: Protection,
    pub sharing: Sharing,
    pub target: MapTarget,
    pub offset: u64,
    pub length: u64,
}

pub enum MapTarget { Host, Device(DeviceId) }
pub enum Sharing { Shared, Private }
```

First-milestone host blob mapping lowers to the existing `BlobView` and `WritableArenaView`. Unsupported protection, sharing, object kind, target, alignment, or backend fails explicitly.

A stream itself is not treated as a regular mmap-able file. POSIX-style mapping of a FIFO fails. Direct streaming to GPU/device memory uses `read_into(device_region)` and `write_from(device_region)`, with registered regions and completion fences. A backend may additionally expose a privileged mapping of transfer buffers, but ring framing and cursor headers are not public ABI.

### 8.3 Mapping guarantees

- M1. Every mapping is bounds-checked against both the object and backing region before exposing memory.
- M2. Mapping protection cannot exceed descriptor rights or object capabilities.
- M3. Read-only mappings reject writable buffer exports.
- M4. Writable mappings preserve the existing single-writable-lease rule unless a future coherent shared-write mode explicitly replaces it.
- M5. A mapping retains the backing lease independently of the descriptor. Closing/dropping the descriptor cannot cause live mapped memory to be reclaimed or reused.
- M6. Unmapping or dropping the final view releases its lease exactly once when no other descriptor/mapping reference remains.
- M7. Stale generation, malformed metadata, premature publication state, and out-of-range descriptors fail before pointer exposure.
- M8. No allocator may reuse a mapped extent while any mapping can access it.
- M9. Device operations report completion only after the documented device/transport fence makes the transferred range safe for the next owner.
- M10. The selected route is observable as direct or staged. An implementation MUST NOT silently insert a pageable-host bounce that violates requested direct-transfer policy.
- M11. Failure or cancellation cannot leave a region simultaneously owned for incompatible producer/consumer access.
- M12. Python buffer exports retain their mapping owner; mapping close fails while exports are live, as it does today.

POSIX permits mappings to outlive a closed descriptor. The implementation MUST preserve that rule. Closing a staged writable blob while a writable mapping remains records clean-close intent and closes the descriptor, but publication may remain pending until the final writable mapping synchronizes and unmaps. The mapping remains usable and independently owns the backing lease. Explicit mapping synchronization/close reports its own errors; the high-level blob context requires all writable views closed before descriptor close so it can report publication success or failure synchronously.

## 9. Close, abort, cancellation, and failure

`close` is the only clean descriptor terminal operation. `abort` is an explicit distributed-I/O extension used for cancellation and exceptional unwinding; it does not redefine close.

### 9.1 Guarantees

- C1. Clean stream close drains accepted bytes, emits one EOF, and detaches the incarnation.
- C2. Clean staged-blob close requests at most one sealed publication. With no live writable mapping, close reports its result; with a surviving writable mapping, publication remains pending and completes only after required synchronization/unmap.
- C3. Abort publishes no staged blob and sends no stream EOF; peers receive the appropriate terminal fault.
- C4. Dropping an open future cancels it and reclaims any grant that races after cancellation.
- C5. Dropping an unclosed writer is equivalent to abnormal abort, not clean close.
- C6. Close/abort races have one winner and perform release/publication exactly once.
- C7. Session close rejects new opens, terminates active operations, and reaches `Closed` only after owned bindings are detached or placed in durable cleanup.
- C8. Peer loss wakes every blocked read, write, flush, close, and open affected by that peer.
- C9. A stale or duplicate terminal message is harmless and cannot terminate a newer incarnation or release a newer generation.
- C10. Cancellation and cleanup are bounded by actor progress; no cancelled waiter permanently consumes arena or stream capacity.

## 10. Async submission and completion

Rust futures and Python awaitables are bindings over an operation/completion contract comparable to asynchronous Unix I/O:

- submission identifies the descriptor, operation, region/range, and completion destination;
- the backend either completes immediately or registers exactly one waiter;
- after registering, it rechecks readiness before sleeping;
- publication/consumption sends wake notifications after state changes;
- wake causes the operation to retry the same primitive;
- cancellation removes or invalidates the waiter;
- operation identity rejects stale and cross-wired completions.

### 10.1 Guarantees

- A1. Each submission produces at most one terminal completion.
- A2. Completion identity includes enough generation/incarnation information to reject stale messages.
- A3. Immediate-ready and register-then-ready races cannot lose a wakeup.
- A4. Spurious or duplicate wakeups do not duplicate I/O and are otherwise harmless.
- A5. No actor handler blocks or awaits; waiting remains engine-hosted work plus actor notifications.
- A6. Blocking one descriptor does not prevent unrelated descriptors or actor workers from progressing.
- A7. Cancellation completion means the caller region is no longer accessed by that submission.
- A8. Byte counts and terminal errors preserve the partial-I/O rule in IO8.

The current `WaitData`, `WaitCapacity`, and `StreamWake` protocol already supplies most of this behavior and SHOULD be retained.

## 11. Authority, security, and cleanup

The orchestrator-owned data-plane authority acts as the kernel serializer for control state. In current code this responsibility is split across `DataDirectoryActor` and each `HostDataPlaneSessionActor`.

### 11.1 Authority responsibilities

- serialize namespace mutation and stream incarnation selection;
- resolve logical paths and `/runs/self`;
- authorize requested rights;
- issue operation/binding/lease capabilities;
- track session-owned bindings;
- revoke or fence stale generations;
- serialize publication, replacement, and close;
- recover durable namespace state;
- reclaim resources after cancellation, peer loss, or process death.

It does not copy ordinary payload bytes after granting a direct data path.

### 11.2 Guarantees

- K1. Possession of an actor address, arena offset, or guessed descriptor identifier alone is insufficient to acquire data-plane access.
- K2. Every grant is bound to its session capability, resolved path rights, object revision/incarnation, and backing generation.
- K3. A child cannot send control to a binding not owned by its attached session.
- K4. Stale leases and stream incarnations are rejected before memory or namespace mutation.
- K5. Restart/retry operation identifiers retain the existing at-most-once namespace mutation guarantee.
- K6. Authority restart does not resurrect closed stream incarnations or aborted staged blobs.
- K7. Namespace recovery preserves acknowledged mutations and rejects conflicting reuse of an operation identifier.
- K8. Control serialization introduces no required payload serialization or per-byte authority work.

## 12. Errors and language mapping

`DataPlaneError` remains structured and gains a stable errno classification. Diagnostic details remain available without making strings part of the contract.

Minimum classifications:

| Condition | Errno class | Python mapping |
|---|---:|---|
| invalid path/flags/range | `EINVAL` | `ValueError` or `OSError(EINVAL)` at raw surface |
| missing path | `ENOENT` | `FileNotFoundError` |
| unauthorized | `EACCES` | `PermissionError` |
| closed/wrong access descriptor | `EBADF` | `OSError(EBADF)` |
| wrong node/object type in a typed open | `ENXIO` | `OSError(ENXIO)` |
| mapping a non-mappable object | `ENODEV` | `OSError(ENODEV)` |
| unsupported operation/flag | `ENOTSUP`/`EOPNOTSUPP` | `OSError` |
| no arena/device capacity | `ENOSPC` or `ENOMEM` according to resource | `OSError`/`MemoryError` |
| would block | `EAGAIN` | `BlockingIOError` |
| cancelled operation | `ECANCELED` | `CancelledError` or `OSError(ECANCELED)` according to binding cancellation source |
| write to closed stream | `EPIPE` | `BrokenPipeError` |
| peer/transport loss | `ECONNRESET`/`EIO` | `ConnectionResetError`/`OSError` |
| stale generation/incarnation | `ESTALE` | `OSError(ESTALE)` |
| active incompatible mapping | `EBUSY` | `OSError(EBUSY)` |

**Guarantees — errors**

- E1. Raw descriptor errors carry a stable errno classification.
- E2. High-level typed exceptions may be more specific but cannot convert failure into success or lose cancellation.
- E3. Error strings are diagnostic only; tests assert variants/errno, not prose, except binding smoke tests for user-facing context.
- E4. An invalid operation that is specified as non-mutating leaves descriptor, namespace, cursor, region ownership, and capacity unchanged.
- E5. Internal actor, transfer, or allocator failures do not escape as panics across the public API.

## 13. High-level Rust API

The existing methods remain simple and path-first:

```rust
DataPlane::read_blob(path) -> Blob
DataPlane::write_blob(path, length) -> BlobWriter
DataPlane::read_stream(path) -> StreamReader
DataPlane::write_stream(path) -> StreamWriter
DataPlane::collect_stream(path, consumer) -> ActorCompletion
DataPlane::write_stream_replacing(path) -> StreamWriter
DataPlane::read_blob_path(path_string) -> Blob
DataPlane::write_blob_path(path_string, length) -> BlobWriter
```

They become wrappers over `open`:

- `read_blob`: open read-only, require `Blob`, return typed mapped-blob wrapper;
- `write_blob`: open a staged/truncating writable blob with declared length, return typed writer;
- `read_stream`: ensure/require `Stream`, open read-only, return stream reader;
- `write_stream`: ensure/require `Stream`, open write-only, return stream writer;
- `write_stream_replacing`: retain explicit replacement as a high-level namespace policy, then use the same write-only stream descriptor;
- `collect_stream`: drive the same stream descriptor read primitive from the existing actor-oriented consumer wrapper;
- `read_blob_path` and `write_blob_path`: parse the string once, then call their `DataPath` wrapper;
- `BlobWriter::seal`: remain as the explicit typed commit operation and delegate to the descriptor's clean staged-blob close/publication path;

Typed wrappers MAY provide `read_exact`, `write_all`, `flush`, mapping contexts, digest access, and clean/exception context behavior. Existing `StreamWriter::flush` remains available and uses the descriptor's accepted-versus-consumed progress; it does not define a second write path. Wrappers MUST delegate data operations and terminal state to the descriptor.

### 13.1 Rust wrapper guarantees

- R1. Typed wrappers add no authority round trip after the open grant solely to rediscover kind/capabilities.
- R2. A kind mismatch closes/aborts the obtained descriptor and returns a typed error without leaking resources.
- R3. `write_all` handles every legal partial-write boundary and returns only after all bytes or a terminal error.
- R4. Typed blob mapping preserves direct arena pointers and lease lifetime.
- R5. No wrapper reimplements transport, wakeup, offset, EOF, close, or publication state separately from the descriptor.

## 14. Python API

The ordinary surface changes minimally:

```python
blob = await ctx.data.read_blob(path)

async with ctx.data.write_blob(path, length=n) as blob:
    with blob.map() as view:
        ...

reader = await ctx.data.read_stream(path)
async with ctx.data.write_stream(path) as writer:
    await writer.write(data)       # write-all convenience
```

Raw opt-in access is added:

```python
fd = await ctx.data.open(path, swactor.O_RDONLY)
buffer = bytearray(65536)
n = await fd.readinto(buffer)
await fd.close()
```

The exact flag container may be an enum/bitflag object rather than accepting arbitrary host `os` flags, but names and semantics remain POSIX-like.

Python additions:

- `DataPlane.open(path, flags, ...)`;
- a raw `Descriptor` class with `read`, `readinto`, `write`, `writefrom`, `map`, `close`, and `abort` as supported;
- `StreamReader.readinto` and region-aware variants;
- region registration/import APIs when a concrete device backend lands.

### 14.1 Python guarantees

- P1. Existing high-level method names, path arguments, async context managers, blob buffer protocol, and stream write convenience remain available.
- P2. `StreamReader.read()` continues returning `bytes | None`; `None` is clean EOF. Chunk boundaries are unspecified.
- P3. `Descriptor.readinto(buffer)` modifies only `buffer[:n]` and retains the exporter until completion/cancellation.
- P4. Python async cancellation waits for or establishes backend cancellation before releasing exported mutable memory.
- P5. Read-only mappings reject writable buffer requests.
- P6. Mapping close rejects active Python buffer exports.
- P7. Clean context exit closes/commits; exceptional exit aborts the staged blob or stream unless the user explicitly selected different high-level policy.
- P8. Raw descriptor use is opt-in and does not expose internal actor/ring/arena fields through the ordinary public surface.

## 15. Performance contract

Correctness is mandatory; the descriptor layer must also preserve the reason the current data plane exists.

- PF1. `open` may perform namespace, authorization, actor, allocation, and attachment work once.
- PF2. Attached blob/stream reads and writes do not perform a namespace lookup or orchestrator round trip per byte operation.
- PF3. Blob host mappings remain zero-copy and point into the mapped data-plane arena.
- PF4. Stream host reads perform at most the existing ring-to-destination copy; stream writes perform at most source-to-ring copy on each side of a transport.
- PF5. The primitive Rust host-buffer API performs no required heap allocation per successful read/write.
- PF6. Partial stream reads do not allocate a remainder buffer; unread bytes remain pinned in the ring.
- PF7. Descriptor dispatch adds no payload serialization and no required dynamic allocation.
- PF8. Registered/device-region transfers use a direct route when required and supported. A staged route is explicit and measurable.
- PF9. Blocked I/O does not spin and does not block an actor worker.
- PF10. Performance tests compare open latency, steady-state throughput, allocation counts, copy counts, and backpressure behavior against the pre-migration direct path. A regression requires an explicit reviewed exception.

## 16. Concrete code changes

### 16.1 `crates/data-plane/src/data_plane.rs`

- Add `OpenOptions`, access flags, descriptor capabilities, `Descriptor`, and private backend enum.
- Add `DataPlane::open` and path-string adapter.
- Make existing `read_blob`, `read_blob_path`, `write_blob`, `write_blob_path`, `read_stream`, `write_stream`, `write_stream_replacing`, and `collect_stream` use the descriptor core and narrow its returned backend.
- Move common close/abort state into descriptor/open-description state.
- Add allocation-free `read`/`read_into` and partial `write` primitives.
- Keep `BlobWriter`, `StreamReader`, and `StreamWriter` as typed wrappers/backends rather than parallel implementations.
- Retain `BlobWriter::seal`, `BlobWriter::abort`, `StreamWriter::flush`, `StreamWriter::close`, and `StreamWriter::abort` as typed lifecycle wrappers over descriptor state.
- Change `StreamReader` from record-returning allocation to an internal partial-record cursor; preserve allocating `read()` as a wrapper.
- Change stream primitive write to return a byte count; preserve current full-buffer behavior as `write_all`/typed `write`.
- Make `StreamConsumerActor` share the descriptor stream-read state machine; it may retain actor-oriented scheduling but MUST NOT maintain conflicting EOF, partial-read, or close semantics.

### 16.2 `crates/data-plane/src/protocol.rs`

- Add serialized access/open flags, descriptor kind/capability metadata, and stable errno classification.
- Add a generic child open request/grant if required for untyped raw `open`.
- Preserve existing specialized host messages where dispatching to them is smaller and behaviorally identical.
- Replace kind-specific authorization semantics at the public boundary with read/write rights; kind may remain in diagnostics.
- Include operation identity, generation/incarnation, and enough metadata to reject stale grants/completions.

### 16.3 `crates/data-plane/src/host.rs`

- Generalize `validate_open` around requested read/write rights while retaining `SessionAccess::can_read/can_write`.
- Resolve object kind once and dispatch to the existing blob or stream binding actor.
- Return kind/capabilities with the grant.
- Retain `active_bindings`, `stream_bindings`, close progression, and ownership checks.
- Ensure generic cancellation reclaims late blob grants and waiting stream endpoints.

### 16.4 `crates/data-plane/src/namespace.rs` and `namespace_store.rs`

- Add a generic typed lookup result sufficient for `open` without attempting a blob-only resolve.
- Preserve linearizable revisions, durable blob bindings, operation-id idempotence, stream incarnation fencing, and explicit replacement.
- Represent/ensure named stream nodes sufficiently for raw open to determine kind. Internal ensure-and-open fusion is allowed if externally equivalent.
- Keep active stream endpoints/incarnations separate from the namespace node identity.
- Do not replace the namespace with descriptor identifiers.

### 16.5 `crates/data-plane/src/blob.rs`

- Preserve the existing header validation, generation fence, sealed/writable/aborted states, and lease guards.
- Add bounded offset-based copy helpers used by descriptor read/write without creating a public mapping for each call.
- Refactor writable ownership so mappings can retain the open description independently of descriptor lifetime.
- Preserve direct `BlobView`/`WritableArenaView` mapping and Python buffer behavior.
- Keep clean publish and abort exactly-once.

### 16.6 `crates/data-plane/src/byte_ring.rs`

- Add safe partial payload-copy support that can leave the current record pinned.
- Preserve record framing as internal EOF/fault/publication metadata.
- Preserve release ordering, generation checks, bounds validation, wraparound handling, and SPSC cursor ownership.
- Expose enough free-capacity information for allocation-free partial writes without exposing raw headers.

### 16.7 `crates/data-plane/src/mapped_arena.rs`, `arena.rs`, and device seams

- Introduce bounded region capabilities over existing arena leases.
- Keep stable process-local arena mapping and validated offsets.
- Add registration/import traits only when a concrete host/device backend consumes them; do not add unused abstraction layers.
- Make route selection and direct/staged policy observable.

### 16.8 `crates/bindings/python/src/job.rs`

- Bind raw `DataPlane.open` and `Descriptor` operations.
- Reimplement existing high-level methods through descriptors.
- Add `readinto` without allocating an intermediate `Vec`.
- Retain `BlobView` and writable buffer protocol ownership.
- Retain async context-manager clean/exception policy with descriptor close/abort.
- Map stable errno classes to Python built-in exceptions while retaining current Swactor exception subclasses where useful.

### 16.9 Tests

- Extend `namespace_host_read_guarantees.rs` for generic descriptor reads, staged publication, cancellation, and lease reclamation through the real host authority.
- Extend `stream_transport_guarantees.rs` where descriptor partial-I/O changes transport-visible progress or fault behavior.
- Add `crates/data-plane/tests/descriptor_guarantees.rs` for the public Rust contract.
- Extend `actor_blob_guarantees.rs` only for actor/failure integration that is not descriptor-surface behavior.
- Extend `byte_ring_guarantees.rs` for partial pinned reads and partial framed writes.
- Extend `namespace_guarantees.rs` for typed generic lookup/ensure and descriptor-stable rebinding.
- Extend Python `test_bootstrap.py` for raw descriptors, `readinto`, unchanged high-level behavior, cancellation, and exception mapping.
- Keep tests black-box at the narrowest public surface that owns each guarantee.

## 17. Verification methodology

The guarantee suite binds behavior from both directions:

1. **Legal action sequences** prove that every supported action preserves all invariants from every reachable legal state exercised by the suite.
2. **Forbidden/negative actions** prove that illegal actions fail with the required error and do not mutate otherwise legal state.
3. **Bad-behavior mutations and fault adapters** prove that the guarantees detect plausible implementation defects rather than merely execute code.

The tests exercise the actual data-plane API and implementation. They MUST NOT compare the implementation to a second implementation or a behavioral model of descriptor/session internals.

### 17.1 Legal state-sequence tests

A state-sequence harness drives real `DataPlane`, namespace, arena, ring, transport, and binding code. Its generator retains only test-owned facts needed to form legal calls:

- paths it requested;
- handles actually returned by the SUT;
- buffers and unique byte tags supplied by the test;
- mappings still owned by the test;
- completions/errors actually observed;
- whether the test itself already closed or cancelled a handle.

This is an action precondition ledger, not a model of actor state, allocator layout, namespace revisions, ring cursors, or expected implementation transitions. Oracles come from the normative guarantees and public observations.

Legal actions include, as applicable:

- attach and close sessions;
- open authorized blob/stream paths in supported modes;
- interleave multiple paths and concurrent opens;
- read/write zero, one, boundary, wraparound, capacity, and multi-capacity lengths;
- perform partial reads/writes with changing destination/source sizes;
- map/unmap and create/release Python buffer exports;
- close cleanly, abort explicitly, cancel pending opens and I/O;
- publish and reopen blobs;
- replace paths while older descriptors/mappings remain live;
- fill and drain stream capacity;
- close peers before, during, and after blocked operations;
- tear down and recover sessions/namespace authority at legal lifecycle points.

After every action the harness probes applicable public invariants, for example:

- tagged bytes observed so far are an exact ordered prefix with no duplication/cross-wiring;
- sentinels outside destination ranges remain unchanged;
- an old open still reads its selected revision after replacement;
- a cancelled path can be reopened and arena capacity is eventually reusable;
- EOF/fault remains sticky;
- public arena samples show no overlapping live leases and eventual release;
- namespace lookup reports the expected externally committed kind/path, derived from successful public operations rather than simulated internals;
- unrelated descriptors continue progressing.

Coverage uses three complementary dimensions:

- **bounded exhaustive sequences** over a reduced action/path/buffer domain to cover all short transition combinations;
- **property-generated sequences** with shrinking, varied depth, payload, capacity, path, cancellation point, and concurrency schedule;
- **targeted deep scenarios** that run hundreds or thousands of operations to force cursor wraparound, generation reuse, repeated publication, and cleanup cycles.

The suite records action/transition coverage. Every supported operation must be exercised from each legal descriptor/object lifecycle state where it is applicable. Merely running a configured number of random cases is insufficient.

### 17.2 Forbidden-action complement

For each reachable legal state, the suite defines the complementary operations that MUST fail. It first reaches the state using legal public calls, performs one forbidden action, asserts the exact error/errno, then proves the prior legal state remains usable unless the contract says the error is terminal.

Required negative families:

- malformed, relative, root, dot-segment, NUL, and unauthorized paths;
- invalid/contradictory/unsupported open flags;
- missing paths without creation and exclusive creation of existing paths;
- read on write-only and write on read-only descriptors;
- operation after raw close, double raw close, and use after terminal fault;
- map with excessive protection, wrong kind, bad offset/length/alignment, stale generation, or unsupported target;
- writable map while incompatible mutable access is live;
- close/publish while high-level writable exports are active;
- write beyond first-milestone fixed blob extent;
- clean publication after abort and abort after successful publication;
- duplicate stream role, wrong incarnation, stale wake/completion, and unauthorized binding control;
- nonblocking operations before `O_NONBLOCK` support;
- out-of-range or permission-incompatible regions;
- cancellation followed by a late grant/completion;
- session close followed by open/control attempts;
- corrupt arena/blob/ring headers and forged capability identifiers.

For non-mutating failures the complement test MUST verify:

- descriptor offset/data remains unchanged;
- namespace binding/revision remains unchanged;
- no bytes outside the requested region changed;
- no peer received data/EOF;
- no waiter, lease, ring capacity, or binding leaked;
- an unrelated legal operation still succeeds.

### 17.3 Concurrency and schedule breadth

Concurrency tests run real operations, not simulated transitions:

- many simultaneous opens across repeated and distinct paths;
- read/write with producer and consumer on different threads/tasks;
- close versus read/write/wait;
- cancellation versus grant/wake/publication;
- path replacement versus open;
- session teardown versus active descriptors and mappings;
- stale incarnation/generation messages arriving after replacement.

The harness varies engine worker counts, ring/arena capacities, task yield points, transport delay, and message delivery order permitted by the runtime. Unique payload and request tags detect cross-wiring, duplication, reordering, and stale completion acceptance.

### 17.4 Fault injection

Fault tests use real production state machines with controlled failing seams already present in the design:

- rejecting/black-hole transport;
- delayed or dropped host reply;
- source shorter/longer than declared;
- source/peer actor loss;
- namespace authority restart and delayed recovery;
- allocator exhaustion and queued-grant cancellation;
- corrupt/stale blob and ring metadata;
- device registration/import/fence failure when device regions land.

Fault injection MUST assert terminal result, peer result, cleanup, and continued operation of unrelated descriptors.

### 17.5 Mutation adequacy: tests that fail for bad behavior

Each guarantee family MUST name at least one plausible bad implementation mutation that the suite kills. Required mutations include:

- omit access check;
- advance an offset by requested rather than completed bytes;
- return EOF before queued bytes;
- release a partial ring record early;
- duplicate or drop a stream prefix;
- accept a wake/grant for the wrong request or incarnation;
- publish an aborted blob;
- reclaim an extent while a mapping is live;
- allow writable export of a read-only mapping;
- fail to unregister a cancelled waiter;
- perform close/publication twice;
- authorize the unresolved `/runs/self` spelling rather than resolved path;
- silently stage a transfer requested as direct;
- touch bytes beyond a supplied region;
- leak a lease after failed or cancelled open.

Mutation runs may be implemented with test-only fault adapters at defined seams or an external mutation tool. They must execute the same public guarantee assertions. A guarantee is incomplete until its designated mutation causes a test failure. Deliberately bad production branches or permanent mock implementations are prohibited.

### 17.6 Test isolation and determinism

- Every test owns unique namespace paths, capabilities, operation identifiers, and temporary stores.
- Tests do not rely on wall-clock sleeps when a deterministic completion or progress hook can establish the condition.
- Timeout is a deadlock guard, never the success oracle.
- Property failures retain seed and shrink to the shortest action sequence.
- Resource accounting is checked after teardown so suite order cannot hide leaks.
- Linux-only mapping tests remain explicitly gated; portable descriptor semantics are tested without assuming `/proc` except where verifying actual native mappings.

## 18. Guarantee-to-test organization

Tests are organized by owner, not by implementation file count:

| Surface | Positive guarantees | Negative complement |
|---|---|---|
| path/open | N1–N8, OP1–OP10 | malformed/unauthorized paths, flags, missing/wrong kind, cancellation |
| descriptor I/O | D1–D7, IO1–IO12 | wrong access, use-after-close, range overrun, unsupported operations |
| blob | B1–B10 | stale/invalid lease, overflow, active mapping, abort/publish conflicts |
| stream | S1–S12 | stale incarnation, duplicate role, late write, peer fault, capacity misuse |
| mappings/regions | M1–M12 | rights amplification, bounds, generation, live reuse, fence/import failure |
| async lifecycle | A1–A8, C1–C10 | stale/duplicate completion, cancellation race, double terminal operation |
| authority/security | K1–K8 | forged capability/binding, unresolved auth, retry conflict, restart |
| Rust/Python wrappers | R1–R5, P1–P8 | kind narrowing leak, buffer export misuse, exception cleanup |
| performance | PF1–PF10 | allocation/copy/authority-hop regression and silent staging |

Tests SHOULD use guarantee identifiers in names or comments so changing a guarantee identifies its complete test impact.

## 19. Implementation sequence

### Phase A — descriptor facade without data-path replacement

1. Add open/access/capability/error types.
2. Add `Descriptor` over existing blob and stream backend objects.
3. Add generic open dispatch while retaining existing operation/binding actors.
4. Route existing Rust high-level methods through descriptors.
5. Establish open, access, close, cancellation, correlation, and cleanup guarantees.

### Phase B — POSIX byte semantics

1. Add blob offset reads/writes within the supported fixed extent.
2. Add allocation-free partial stream reads and writes.
3. Add `read_exact`/`write_all` wrappers.
4. Enforce EOF, partial completion, offset, and stable errno guarantees.
5. Preserve existing stream wake/backpressure protocol.

### Phase C — mappings and region I/O

1. Put existing blob mappings behind descriptor `map`.
2. Establish descriptor-independent mapping lifetime.
3. Add host/arena region slices and `read_into`/`write_from`.
4. Add direct/staged route policy and observability.
5. Add the first concrete pinned/GPU region backend without changing descriptor APIs.

### Phase D — bindings

1. Bind raw Python descriptor API and errno mapping.
2. Rebuild typed Python blob/stream classes over descriptors.
3. Add `readinto` and preserve buffer/context behavior.
4. Migrate repository callers without changing ordinary path-first usage.
5. Remove old binding calls that bypass descriptors.

### Phase E — comprehensive guarantees

1. Add positive state-sequence and bounded exhaustive tests.
2. Add forbidden-action complement tests.
3. Add concurrency and fault schedules.
4. Add mutation-adequacy targets.
5. Add allocation/copy/throughput checks.
6. Delete obsolete API implementation paths only after equivalence and cleanup guarantees pass.

### Phase F — POSIX expansion

Implement `fstat`, `lseek`, `ftruncate`, vectored/positional I/O, polling/nonblocking mode, numeric descriptor tables, duplication/inheritance, directory-relative operations, richer mappings, and device control one contract at a time. Each operation lands only with its positive state-sequence, forbidden complement, concurrency/fault, and mutation-adequacy tests.

## 20. Completion criteria

The migration is complete when:

- every existing high-level blob and stream operation lowers to the descriptor core;
- raw open/read/write/map/close behavior satisfies all first-milestone guarantees;
- no separate binding or transport path implements conflicting blob/stream semantics;
- existing zero-copy blob mapping, stream order/backpressure/EOF, namespace durability, authorization, cancellation, and cleanup guarantees remain passing;
- legal action sequences preserve every observable invariant;
- every forbidden action fails with the specified error and non-mutation behavior;
- designated bad-behavior mutations are detected;
- performance verification shows no unreviewed allocation, copy, authority-hop, throughput, or latency regression;
- obsolete public operation-specific core entry points are removed or retained only as documented typed convenience wrappers.
