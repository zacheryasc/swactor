# The Datastream — Specification

Id: 7
Last modified:
Last reviewed:

---

## 1. What the datastream is

The datastream is a **per-node, append-only telemetry pipe**. Every stream is
produced by one node incarnation, identified by `(node, life)`. A node can feed
zero, one, or many local/remote consumers: the mux is drained once, then the
endpoint fans out the resulting events to subscribers.

Its core rule is unchanged:

> **Nothing between a producer and a view ever interprets a producer payload.**

The current implementation is catalog-aware. Producers register a channel name
and content class with their stream owner, receive a stream-local numeric
`ChannelId`, and submit opaque payload bytes on that id. A single per-stream
**mux** queues accepted payloads and stamps each drained frame with a position,
interleaving every channel into one ordered stream. A **catalog** maps numeric
channel ids back to human-readable names and decode/display metadata.
A best-effort **transport**
carries catalog declarations and frame events. **Ingest** reconstructs streams
by `StreamId` and position into a **store**. **Views** resolve channel names
from metadata and decode bytes back into meaning — and only here, at read time,
does anything look inside a producer payload.

```text
   producers                  register names, submit opaque bytes by ChannelId
        │
        ▼
   endpoint / catalog          allocate ids, declare stream/channel metadata
        │
        ▼
   per-stream MUX              queue payloads, assign positions on drain
        │
        ▼
   endpoint fanout             drain once, broadcast catalog-aware events
        │
        ▼
   transport                   best-effort: may drop / reorder / delay / duplicate
        │
        ▼
   ingest                      reconstruct each stream by position
        │
        ▼
   store (truth for frames)    whole, append-only, position-keyed
        │
        ▼
   views                       read-time projections; payload decoding lives here
```

This still collapses counters, gauges, histograms, logs, events, traces, and
binary blobs into one mechanism: positioned bytes on named channels. The pipe
stores and moves frames uniformly because it does not know what a payload
"means." That distinction exists in a view: "the last frame on this channel" is
a gauge, "every frame on this channel" is an event log, and both are projections
over the same stored frames.

### 1.1 Consequences that the rest of this spec spells out

- There is **one data-entry shape** (§4): register or reuse a channel id, then
  submit bytes on that id.
- A channel name is not a queue, counter, or buffer, but it **is cataloged**
  (§5): the stream owner allocates a stream-local numeric id and declares its
  name/content metadata.
- The **store is frame truth**, not catalog truth (§7). Durable name recovery
  requires catalog descriptors alongside numeric frames.
- All **producer-payload semantics live in views** (§8). Catalog content classes
  help route and display; they do not let mux, transport, ingest, or store
  inspect producer payloads.

---

## 2. The data model

### 2.1 The frame remains the unit of ordered data

```rust
pub struct Frame {
    pub channel: ChannelId,   // stream-local numeric lane
    pub position: Position,   // mux-assigned order within the stream
    pub payload: Vec<u8>,     // opaque bytes — never interpreted by the pipe
}
```

A typed record, a log line, and a binary blob are all frames: bytes on a
stream-local channel id at a stream-local position. `payload` is opaque to the
mux, transport, ingest, and store.

There is still **no timestamp field on `Frame`**. Time, wall-clock correlation,
latency, or tracing data is producer payload, not a datastream-owned sidecar or
property on the frame envelope.

### 2.2 Stream identity — incarnations never merge

```rust
pub struct StreamId { pub node: NodeId, pub life: Lifetime }
```

`StreamId` is the ingest key. Two streams with the same `node` but different
`life` are **different streams and must never merge**. A node that dies and is
restarted (re-rented, re-scheduled, or bootstrapped for a new run) begins a new
`life`, so the fresh stream does not append to or collide with the prior one.
Restart is visible, not silently glued over.

The stream descriptor declares where a stream came from:

```rust
pub enum StreamOrigin { Orchestrator, Bootstrap, RemoteNode }

pub struct StreamDescriptor {
    pub stream: StreamId,
    pub label: Option<String>,
    pub origin: StreamOrigin,
}
```

`StreamOrigin` is subscription metadata. It is not a clock and does not order
streams.

### 2.3 Position: per-stream, monotonic when assigned

```rust
pub struct Position(pub u64);
```

- **One sequence per stream**, not per channel. A stream's mux holds a single
  counter shared across every channel. A single channel therefore has sparse,
  non-contiguous positions interleaved with every other channel.
- **Assigned while draining.** `submit` only attempts to enqueue payload bytes.
  `drain` assigns positions to accepted payloads. A queue-full rejection happens
  before position assignment and therefore does not create a position gap.
- **Not comparable across streams.** Positions order frames within one
  `StreamId` only. There is no global stream order.

**Why one counter and not one per channel.** It keeps the mux a single position
authority once frames leave the queue and makes assigned-frame loss detection a
whole-stream property: a hole means *something already assigned* was lost. A
source needing contiguous domain accounting carries its own sequence number
inside its payload.

### 2.4 Channels: numeric ids plus catalog descriptors

A raw frame contains a numeric channel id:

```rust
pub struct ChannelId(pub u32);
```

`ChannelId` values are **stream-local**. `ChannelId(7)` in one stream is not the
same channel as `ChannelId(7)` in another stream unless their descriptors say
so. The current endpoint allocates user channels starting at `ChannelId(1)`;
`ChannelId(0)` is not allocated by the public registration path.

The human-readable channel name and display/routing metadata live in a channel
descriptor:

```rust
pub enum ChannelContentKind { Bytes, TextStream, JsonRecord }

pub enum ChannelContent {
    Bytes,
    TextStream,
    JsonRecord { schema: Option<String> },
}

pub struct ChannelDescriptor {
    pub stream: StreamId,
    pub id: ChannelId,
    pub name: String,
    pub label: Option<String>,
    pub content: ChannelContent,
}

pub struct ChannelRef {
    pub stream: StreamId,
    pub channel: ChannelId,
}
```

The globally resolved raw channel identity is `ChannelRef`, not a bare
`ChannelId`. A view usually needs the `ChannelDescriptor` for that `ChannelRef`
to recover the channel name and choose a decode/display path.

### 2.5 Datastream events

The live endpoint/subscription path carries catalog-aware events:

```rust
pub struct FrameDelivery {
    pub channel: ChannelRef,
    pub position: Position,
    pub payload: Vec<u8>,
}

pub enum DatastreamEvent {
    StreamDeclared(StreamDescriptor),
    ChannelDeclared(ChannelDescriptor),
    Frame(FrameDelivery),
    StreamEnded(StreamId),
}
```

`FrameDelivery` is the live-event form of a frame: stream and channel are
resolved into a `ChannelRef`, then position and payload follow. The older
`Delivery { stream, frame }` shape still exists for ingest, store tests, and
legacy adapters (§6.1, §7.1).

`StreamDeclared` is part of the event vocabulary, but current QUIC transport
puts the stream descriptor in the stream header and does not emit a separate
`StreamDeclared` event. `StreamEnded` is also part of the event vocabulary, but
current `DatastreamEndpoint` exposes no public `end_stream` method; terminal
source records remain a producer convention until a stream-ending API is added
(§5.7).


---

## 3. Addressing

### 3.1 Raw and resolved coordinates

A stored raw frame is addressed by:

```text
raw-frame @ (stream, numeric-channel, position)
             └ node/life ┘  ChannelId       order
```

A named channel source is addressed by resolving that raw channel through the
catalog:

```text
source @ (stream, channel-name)
          └ node/life ┘ descriptor.name
```

A payload datum is therefore either:

- raw: `(StreamId, ChannelId, Position)`, sufficient for storage and ordering;
- resolved: `(StreamId, ChannelDescriptor.name, Position)`, required for
  human-facing selection and decoding.

No bare `ChannelId` is globally meaningful. A consumer that receives a frame
before its descriptor can store it raw, but cannot render a stable name until the
catalog descriptor arrives or is recovered from durable catalog state.

### 3.2 Channel names: structured catalog paths, not wire ids

Channel names remain dotted paths, but they are catalog descriptor fields rather
than the frame's wire identity. The mux, ingest, and store do not split names;
subscription matching and views may match names through descriptors.

A channel name is a dotted sequence of segments:

```text
channel := namespace ( "." qualifier )*
namespace := owning subsystem          e.g. datastream, host, runtime, proc,
                                            mvp, transport, dist, identity
qualifier := instance-key | leaf       instance-key identifies a dynamic source;
                                            leaf names the signal
```

Current names and families seen in code include:

| Path or family                                      | Owner / meaning |
|-----------------------------------------------------|-----------------|
| `datastream.health`                                 | datastream self-health record |
| `host.cpu`, `host.gpu`, `host.net`                  | host hardware samples |
| `runtime.actors`                                    | swactor runtime actor stats |
| `proc.<label>.stdout`, `proc.<label>.stderr`        | managed process output streams |
| `mvp.lifecycle`                                     | MVP lifecycle record |
| `mvp.provisioning.events`                           | MVP provisioning events |
| `mvp.provisioning.logs.node.<id>.<stdout|stderr|provider>` | MVP provisioning logs |

Single-segment names are allowed, but most current code uses namespaced paths.
Instance segments appear only where a source has many instances: a process
label, node id, actor id, peer id, run id, or similar domain-owned key.

### 3.3 Namespacing and ownership

Collisions are prevented by namespace ownership plus catalog conflict checks:

- Each subsystem owns its first path segment. Only that subsystem should mint
  names below it.
- Dynamic instance segments must be stable identifiers already unique in that
  subsystem's domain.
- Within one stream catalog, registering the same name with the same content
  returns the existing id; registering the same name with conflicting content is
  an error.
- Two streams may allocate different numeric ids for the same channel name.

### 3.4 Source and channel selectors

Subscriptions use structured filters rather than a single textual selector:

```rust
pub struct SubscriptionRequest {
    pub sources: SourceFilter,
    pub channels: ChannelFilter,
}

pub enum SourceFilter {
    All,
    Origin(StreamOrigin),
    Node(NodeId),
    Stream(StreamId),
}

pub enum ChannelFilter {
    All,
    Name(String),
    Prefix(String),
    Content(ChannelContentKind),
}
```

`SourceFilter::Node(node)` means every known life of that node. `Stream(stream)`
means exactly one `(node, life)`. `Origin(origin)` matches stream descriptor
metadata. `ChannelFilter::Prefix(prefix)` is a plain string `starts_with` over
`ChannelDescriptor.name`; the old `.*` notation is user-interface sugar, not the
internal API. `ChannelFilter::Content(kind)` matches catalog content metadata,
not payload inspection.

Examples:

| Request | Meaning |
|---------|---------|
| `{ sources: All, channels: Prefix("proc.") }` | all process-output channels with descriptors visible to the subscriber |
| `{ sources: Node(X), channels: Prefix("proc.trainer.") }` | trainer process output from every life of node `X` |
| `{ sources: Stream(S), channels: Name("host.cpu") }` | exact host CPU channel in one stream |
| `{ sources: Origin(Bootstrap), channels: Content(JsonRecord) }` | JSON-record channels from bootstrap streams |

---

## 4. Population — the one way in

### 4.1 The endpoint owns stream-local allocation and fanout

The current public owner of a stream is `DatastreamEndpoint`:

```rust
pub struct DatastreamEndpoint { /* stream, mux, catalog, fanout, counters */ }
```

An endpoint owns:

- the stream id;
- the mux that assigns positions;
- the catalog that maps names to stream-local channel ids;
- local subscriber fanout;
- counters for assigned, drained, mux-dropped, and bitbucketed frames.

Code that emits telemetry normally asks the endpoint for a cloneable
`DatastreamProducer`:

```rust
let producer = endpoint.producer();
```

Legacy `DatastreamEmitter` / `DatastreamEventSink` APIs still exist for old
call sites. New code should register channels on `DatastreamEndpoint` or
`DatastreamProducer` and submit through `DatastreamProducer`.

### 4.2 Register or reuse a channel, then submit bytes

A producer registers a channel name and content descriptor, receiving a numeric
`ChannelId`:

```rust
fn register_channel(name: impl Into<String>, content: ChannelContent) -> ChannelId;
fn try_register_channel(
    name: impl Into<String>,
    content: ChannelContent,
) -> Result<ChannelId, ChannelRegistrationError>;
fn register_record<R: Record>() -> ChannelId;
```

Then it submits payload bytes on that id:

```rust
fn submit_record<R: Record>(&self, channel: ChannelId, record: &R) -> bool;
fn submit_text(&self, channel: ChannelId, text: impl AsRef<[u8]>) -> bool;
fn submit_text_owned(&self, channel: ChannelId, text: String) -> bool;
fn submit_bytes(&self, channel: ChannelId, bytes: Vec<u8>) -> bool;
```

Everything entering the stream goes through the same shape: numeric channel id
and payload bytes. The submit APIs return `true` when the payload entered the
mux queue and `false` when it was dropped before position assignment. Text and
typed records are API sugar over bytes. *When* a producer emits — timer,
callback, process output, once at boot — is the producer's business.

### 4.3 Observers and subsystem adapters

Subsystems should expose observer/hook seams that can be wired to a producer.
The current endpoint includes adapters for process output and runtime stats:

```rust
trait ProcessOutputObserver {
    fn on_output(&self, label: &str, is_stderr: bool, data: &[u8]);
}

trait StatsHook {
    fn on_tick(&self, worker_id: usize, snapshots: &[ActorSnapshot]);
}
```

Process output uses a caller-provided closure from `(label, is_stderr)` to an
already registered `ChannelId`. Runtime stats default to `runtime.actors` as a
JSON record channel. Other subsystems follow the same rule: register or reuse a
channel id, encode their own payload, submit bytes.

### 4.4 The mux: single position authority

```rust
struct PendingFrame {
    channel: ChannelId,
    payload: Vec<u8>,
}

pub struct Mux {
    stream: StreamId,
    next: AtomicU64,
    dropped: AtomicU64,
    tx: crossbeam_channel::Sender<PendingFrame>,
    rx: crossbeam_channel::Receiver<PendingFrame>,
}
```

The receiver is stored directly; no outer receiver mutex is used for `drain()`.

One mux owns one stream's position sequence. On `submit`:

1. The mux attempts a nonblocking send of `PendingFrame { channel, payload }`
   into a bounded queue.
2. If the send succeeds, `submit` returns `true`.
3. If the queue is full or disconnected, the mux increments `dropped` and
   returns `false`; no position has been consumed.
4. `drain` takes all currently queued pending frames, assigns each one the next
   stream position, and returns frames in queue-drain order.

The queue capacity is bounded and clamped by the implementation. Position is the
canonical store/view ordering key after drain; the drain/fanout batch itself is
not specified as a sorted-position replay guarantee.

### 4.5 Queue drops are pre-position; producers do not block

Telemetry must not apply back-pressure to the work it observes. A full mux queue
causes `try_send` failure; the producer does not block and no position is
assigned to the rejected payload.

The `dropped` mux counter counts submissions lost to mux overflow or
receiver-disconnect before they enter the queue. Once a frame has been drained
and assigned a position, later transport or store loss can still surface as an
interior gap if bracketing positions arrive.

> **Decision of record — lossy, not blocking.** A datastream is allowed to lose
> frames; it is not allowed to stall a producer or silently renumber around a
> loss. Anything that cannot tolerate loss does not belong on the datastream as
> its sole source of truth.

---

## 5. Channels

### 5.1 Channels are catalog entries, not independent resources

A channel is a cataloged name/content descriptor mapped to a stream-local
numeric id. It is **not** a separate queue, counter, lifecycle object, task, or
storage partition.

There is:

- one mux queue per stream, not per channel;
- one position counter per stream, not per channel;
- a stream-local catalog mapping `name <-> ChannelId`;
- a conflict check for duplicate names with different content metadata;
- no per-channel close or garbage collection operation.

A channel can be declared before its first frame. A frame can be stored with
only its numeric channel id, but human-readable decoding requires its descriptor.

### 5.2 Meaning is split: catalog content vs view classifier

There are two related but distinct classification layers.

Catalog metadata describes how a channel is expected to be routed/displayed:

```rust
pub enum ChannelContent {
    Bytes,
    TextStream,
    JsonRecord { schema: Option<String> },
}
```

This metadata is allowed in the endpoint, subscription snapshot matcher, and
transport catalog. It is not payload inspection.

View classification remains caller-owned:

```rust
pub enum ChannelKind { Typed, Text, Opaque }

pub trait ChannelClassifier {
    fn classify(&self, channel_name: &str) -> ChannelKind;
}
```

A `ChannelRegistry` can classify exact typed/text names and text prefixes, but
unknown names default to `Opaque`. The pipe may route by `ChannelContentKind`; a
view decides how far to decode a payload by `ChannelKind` and the caller's
registry.

### 5.3 Static channels and dynamic families

- **Static channel** — a fully literal path known at compile time, such as
  `host.cpu`, `host.net`, `datastream.health`, or `runtime.actors`.
- **Dynamic family** — a path template with domain-owned parameter segments,
  such as `proc.<label>.stdout` or
  `mvp.provisioning.logs.node.<id>.<stream>`.

Dynamic helpers should return channel **names** or registration descriptors, not
bare `ChannelId`s, unless they also have access to the endpoint/producer that
allocates ids. Current code registers each concrete dynamic name, then submits
on the allocated numeric id.

A caller-owned classifier can know a family shape without knowing every concrete
instance: for example, `proc.` may classify as text while
`proc.trainer.stdout` first appears only when the trainer emits and registers.

### 5.4 Bytes / TextStream / JsonRecord, and the raw fallback

Catalog content classes are:

- **Bytes** — arbitrary bytes. Display as raw unless a view knows more.
- **TextStream** — UTF-8-ish stream chunks. A view may render valid UTF-8 as
  text and invalid bytes as raw.
- **JsonRecord** — payloads encoded with serde JSON for a record type. The
  optional `schema` string is catalog metadata, not a versioned wire envelope.

View decode results are:

- **Record** — a typed/JSON channel decoded to a structured JSON value.
- **Text** — a text channel decoded as UTF-8.
- **Raw** — unknown, invalid, or intentionally opaque bytes.

> **Decision of record — forward-compatible fallback.** An unrecognized channel
> name is `Opaque` to the caller's view, never a pipe error. A newer producer may
> introduce a channel an older consumer has never heard of; that consumer stores
> the frame and renders raw bytes rather than rejecting it.

### 5.5 Record ↔ channel binding

A typed record binds to a channel name, not directly to a numeric id:

```rust
pub trait Record: Serialize + for<'de> Deserialize<'de> + Sized {
    const CHANNEL: &'static str;

    fn channel_name() -> &'static str { Self::CHANNEL }
    fn encode(&self) -> Vec<u8> { serde_json::to_vec(self).unwrap() }
    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error>;
}
```

To emit a record, register the record's channel name on a producer/endpoint to
obtain a `ChannelId`, then call `submit_record(channel, &record)`. The codec
contract is still `decode(encode(r)) == r` for compatible producer/consumer
versions.

### 5.6 Schema evolution and schema metadata

Schema evolution is still serde discipline:

- **Add fields freely.** New fields are emitted by new producers.
- **Tolerate missing.** `#[serde(default)]` fills fields an older producer did
  not send.
- **Ignore unknown.** A consumer drops fields it does not recognize.
- **Never remove or repurpose.** Retire a field by leaving it unused; introduce
  new meaning as a new field.

Current `JsonRecord` descriptors carry `schema: Option<String>`. In the current
implementation, `register_record<R>()` uses `Some(R::CHANNEL.to_owned())` as the
schema string. Treat this as catalog metadata for display/subscription tooling,
not as a frame-level schema version. There is still no schema version field in
`Frame` itself.

### 5.7 Lifecycle and liveness

A channel source's data lifecycle remains append-only: first frame, then more
frames, then silence. Prior frames remain in the store as history. There is no
channel teardown or channel garbage collection.

A producer may emit a terminal **data record** if the domain has something to
say, such as a process exit record. Separately, the event vocabulary includes
`DatastreamEvent::StreamEnded(StreamId)` for stream-level control, and QUIC can
encode it. Current `DatastreamEndpoint` does not expose a public method to emit
`StreamEnded`, so it is a defined control event whose emission policy is not yet
wired through the endpoint API.

Liveness remains a view/subscriber concern. A consumer decides a source is dead
by TTL-since-last-frame, a terminal data record, or a future stream-ended event.
The store does not infer liveness.

### 5.8 Discovery

Discovery has moved from store-only scanning to catalog metadata:

- The **catalog** answers which names and content classes have been declared for
  a stream.
- The **store** answers which numeric frames were delivered and stored.
- A **view** joins the two when it wants named projections.

A live subscriber receives an initial `DatastreamSnapshot { streams, channels }`
and future `ChannelDeclared` events. A durable store that must render names
after restart must persist or reconstruct catalog descriptors alongside frames;
frames alone contain only numeric channel ids.

### 5.9 Namespaces currently in use

The spec does not reserve every name below, but these current code paths should
not be contradicted:

| Namespace/path | Current meaning |
|----------------|-----------------|
| `datastream.health` | datastream self-health counters |
| `host.cpu`, `host.gpu`, `host.net` | host hardware samples |
| `runtime.actors` | runtime actor stats hook output |
| `proc.<label>.stdout`, `proc.<label>.stderr` | managed process output |
| `mvp.lifecycle` | MVP lifecycle facts |
| `mvp.provisioning.events` | MVP provisioning lifecycle facts |
| `mvp.provisioning.logs.node.<id>.<stream>` | MVP provisioning stdout/stderr/provider lines |

---

## 6. Transport

### 6.1 Two transport-facing shapes

The crate currently has two related seams.

The older store/test seam is `Delivery`:

```rust
pub struct Delivery {
    pub stream: StreamId,
    pub frame: Frame,
}
```

`Consumer::accept` ingests `Delivery`. Scripted transport and the legacy actor
wire envelope still use this shape.

The live endpoint/subscription seam is `DatastreamEvent` (§2.5). It carries
catalog declarations and `FrameDelivery` events. A transitional helper converts
`DatastreamEvent::Frame` back to `Delivery` when a stored-stream test or adapter
needs the old shape.

### 6.2 Local fanout: drain once, publish to subscribers

Draining the mux is destructive, so the endpoint drains once per tick:

```rust
pub struct EndpointTick {
    pub drained: usize,
    pub delivered: usize,
    pub dropped_for_subscribers: usize,
    pub subscribers: usize,
}
```

`DatastreamEndpoint::tick` drains queued mux frames, converts them to
`DatastreamEvent::Frame`, and publishes the batch to `DeliveryFanout`
subscribers. Future-event fanout is broadcast-only: every current subscriber is
offered every future event, regardless of its `SubscriptionRequest`. A slow
subscriber drops only its own copies; other subscribers can still receive the
same batch. If no subscribers exist, drained frames are bitbucketed and counted.

Subscriptions are **future-event streams plus an initial snapshot**. A new
subscriber receives catalog metadata filtered by its `SubscriptionRequest`; the
same request remains useful to subscribers and downstream filters, but the
endpoint does not filter future fanout events. It does not replay prior frames
unless a caller separately reads a store.

### 6.3 What transport may and may not do

A consumer/store must tolerate:

| May happen | Required response |
|------------|-------------------|
| Drop an assigned frame | surface an interior gap when bracketing positions exist |
| Reorder frames | assemble by position |
| Delay frames | same as reorder from the store's perspective |
| Duplicate frames | keep one frame for the position; first stored frame wins |
| Drop subscriber copies | count the subscriber drop; do not block producer or other subscribers |

A conforming transport may **not** corrupt payload bytes, renumber frames,
fabricate plausible frames, or merge two streams' frames under one `StreamId`.
Decode failures drop/abort the malformed record instead of yielding a plausible
but wrong frame.

Current `ScriptedTransport` deliberately models drops and reorders/delays only;
it does not inject duplicates even though ingest/store remain idempotent.

### 6.4 Wire formats in current use

The legacy delivery envelope encodes:

```text
node_len/node, life, position, channel_id_u32, payload_len/payload
```

This supports old actor transport and real-I/O envelope tests.

The current QUIC adapter uses a catalog-aware stream:

1. A header: magic, flow id, token, stream descriptor, channel descriptors.
2. Tagged records:
   - `ChannelDeclared` as JSON descriptor.
   - `Frame` as numeric channel id, position, and payload bytes.
   - `StreamEnded` as a tag.
   - `StreamDeclared` is skipped because the stream descriptor is already in the
     header.

The new spec treats the event stream as the live transport shape and the legacy
`Delivery` envelope as a compatibility/test seam unless a caller explicitly uses
it.

---

## 7. Ingest and storage

### 7.1 The consumer

```rust
pub struct Consumer { store: Store }
impl Consumer { fn accept(&mut self, d: Delivery) -> bool; }
```

Ingest routes each `Delivery` to its `StreamId` and records the frame at its
position. It does not decode, thin, aggregate, or inspect payload bytes.

Live `DatastreamEvent::Frame` values can be converted to `Delivery` when feeding
the store. Catalog events are not stored by `Store`; callers that need durable
name resolution must persist catalog metadata elsewhere or extend storage.

### 7.2 Out-of-order, idempotent assembly

```rust
pub struct StoredStream { frames: BTreeMap<u64, Frame> }
```

- **Out-of-order** deliveries self-assemble: each frame is placed by its
  position key, so it lands in order regardless of arrival order.
- **Duplicates collapse:** a position delivered twice is recorded once; the
  first frame wins. Ingest is idempotent.
- **Payloads stay whole:** a frame on an unknown channel id is stored exactly
  like any other frame.

### 7.3 The store is frame truth; gaps are derived

```rust
pub struct Store { streams: BTreeMap<StreamId, StoredStream> }
```

The store holds each stream's frames whole and append-only, keyed by `StreamId`.
Gaps are not stored. They are derived by walking adjacent stored positions. An
interior hole — a missing position between two stored frames — is a gap.
Trailing absence after the last delivered frame is not knowable as a gap; it is
just the stream ending or going silent from the store's perspective.

Gap derivation is proportional to the number of stored frames, not the size of
the missing position span.

### 7.4 Store/catalog boundary

A stored `Frame` contains `ChannelId`, `Position`, and payload bytes, but not the
channel name or `ChannelContent`. Therefore:

- the store can reconstruct order and gaps without catalog metadata;
- a raw view can render payload bytes without catalog metadata;
- a named/typed view needs a `ChannelId -> channel name` resolver and usually a
  classifier/registry;
- durable systems that want named views after restart must persist catalog
  descriptors together with or near the frame store.

---

## 8. Views — where payload semantics live

### 8.1 Read-time projections

A view is the only place producer payload bytes are interpreted. Everything
displayed is computed at read time from stored frames plus optional catalog
metadata/classifiers; nothing is precomputed in the mux, transport, ingest, or
store.

The current view API operates over `StoredStream`, whose frames carry numeric
channels. Named decoding therefore needs a resolver from `ChannelId` to channel
name and a caller-owned `ChannelClassifier`.

### 8.2 Implemented standard views

| View/API | Meaning |
|----------|---------|
| `merged_log(stream)` | all stored frames in position order, gaps surfaced as `LogEntry::Gap`, bodies raw |
| `merged_log_with_names(stream, classifier, resolve_name)` | merged log with `ChannelId -> name` resolution and caller-owned body decoding |
| `replay(stream)` | iterator over the raw merged timeline |
| `metric_series_on<R>(stream, channel)` | decode frames on one numeric channel as `R` |
| `metric_series<R>(stream)` | attempt to decode every frame as `R`, regardless of channel |
| `tail(n)` / `grep(needle)` / `filter(pred)` | windowed and predicate-restricted raw frame views |

`latest(channel)` was in the old spec but is not currently implemented in
`views.rs`; do not treat it as a current conformance requirement until an API is
added.

### 8.3 Graceful degradation

A view never fails the whole query because one payload is unknown or invalid:

- Unknown channel name or missing resolver result decodes to `Body::Raw(bytes)`.
- A typed/JSON payload that will not parse is shown raw or skipped by the typed
  series API, depending on the view.
- Invalid UTF-8 on a text channel is shown raw.
- A gap is rendered as a gap, not silently collapsed.

### 8.4 Gauge, log, and time series are projections

Gauge-vs-log-vs-series is still a query choice, not a pipe type:

- "latest value" means a view selects the last stored frame for a channel.
- "event log" means a view walks all frames, usually with gaps surfaced.
- "time series" means a view decodes selected frames into records keyed by
  `Position`.

The same stored bytes can participate in multiple projections.

---

## 9. Invariants and conformance notes

### 9.1 Invariants

A conforming implementation upholds these properties. Each should be testable
without reaching into private internals.

1. **Opaque producer payloads.** No code between producer submission and a view's
   decode inspects, parses, or branches on producer payload bytes.
2. **Stream-local numeric channel ids.** A raw `ChannelId` is meaningful only
   with its `StreamId`; named rendering requires a channel descriptor.
3. **Catalog consistency.** Within one stream, duplicate registration with the
   same name/content returns the existing channel id; duplicate registration
   with conflicting content errors.
4. **Drain-time position assignment.** Within a stream, drain-assigned positions
   are monotonic and never reused.
5. **Queue drops are pre-position.** A full mux queue drops before assignment and
   does not create a position hole.
6. **Assigned-frame drops are detectable when bracketed.** A lost interior frame
   appears as a derived gap, never as renumbering or backfill.
7. **No producer back-pressure.** A full mux queue drops instead of blocking a
   producer.
8. **Position order is canonical for store/views.** Store and view replay use
   mux positions; endpoint drain/fanout order is queue order, not a
   sorted-position guarantee.
9. **Incarnations never merge.** Frames from the same `node` but different
   `life` are stored as distinct streams.
10. **Idempotent ingest.** Delivering the same `(StreamId, position)` twice
    yields one stored frame; first stored frame wins.
11. **Order independence.** Reordered or delayed deliveries reconstruct the same
    stored stream as in-order delivery, modulo drops and duplicate collapse.
12. **Unknown channels survive.** A frame with an unknown numeric id/name is
    stored whole and rendered raw unless metadata later enables decoding.
13. **Version skew tolerance.** Typed JSON records use serde-compatible
    evolution: defaults for missing fields, ignored unknown fields, no field
    repurposing.
14. **Subscriber isolation.** A slow subscriber can lose its own event copies
    without blocking the endpoint or other subscribers.
15. **Semantics only in views.** Removing every view leaves a pipe that still
    allocates channels, orders frames, transports events, ingests deliveries,
    and stores frames correctly.

### 9.2 Compatibility notes

The old `DatastreamEmitter`, `DatastreamEventSink`, legacy actor envelope, and
`Delivery` test seam are compatibility surfaces. New producer code should prefer
`DatastreamEndpoint` and `DatastreamProducer`; new live transports should prefer
catalog-aware `DatastreamEvent` streams.
