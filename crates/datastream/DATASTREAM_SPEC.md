# The Datastream — Specification

> **Status.** This is the canonical specification for the `datastream` crate.
> It supersedes the implicit spec the old `crates/distribution/src/datastream`
> doc-comments referred to (`DATASTREAM_SPEC.md §4.1`, etc.). Those section
> numbers are not preserved; updating the stale `(spec §…)` references is part
> of the migration this document drives.

---

## 1. What the datastream is

The datastream is a **per-node, append-only telemetry pipe**. Every node
produces exactly one stream of its own observations; one consumer reconstructs
those streams and reads them.

Its entire value comes from one rule:

> **Nothing between a producer and a view ever interprets a payload.**

Producers tag bytes with a channel name and hand them off. A single per-node
**mux** stamps each with a position and interleaves them into one ordered
stream. A best-effort **transport** carries the stream. **Ingest** reconstructs
each node's stream by position into a **store**. **Views** decode bytes back
into meaning — and only here, at read time, does anything look inside a payload.

```text
   producers            tag bytes with a channel
        │
        ▼
   per-node MUX          stamp position, interleave into one ordered stream
        │
        ▼
   transport             best-effort: may drop / reorder / delay, never corrupt
        │
        ▼
   ingest                reconstruct each node's stream by position
        │
        ▼
   store (the truth)     whole, append-only, position-keyed
        │
        ▼
   views                 read-time projections — the only place bytes are decoded
```

This collapses the usual telemetry zoo — counters, gauges, histograms, logs,
events, traces — into **one** thing: positioned bytes on a named channel.
Collection, transport, and storage are uniform because none of them know which
of those a frame "is." That distinction does not exist in the pipe. It exists,
if at all, in a view: "the latest frame on this channel" is a gauge, "every
frame on this channel" is an event log, and they are the same bytes read two
ways.

### 1.1 Consequences that the rest of this spec just spells out

- There is **one way data enters** (§4). No per-channel "kind of producer."
- A channel is **a name, not a resource** (§5). Nothing to allocate or free.
- All **semantics live in views** (§8). The pipe is mechanism only.

---

## 2. The data model

### 2.1 The frame is the only unit

```rust
pub struct Frame {
    pub channel: ChannelId,   // the named lane these bytes belong to
    pub position: Position,   // the mux-assigned order within the node's stream
    pub payload: Vec<u8>,     // opaque bytes — never interpreted by the pipe
}
```

A typed record, a log line, and a binary blob are the same kind of thing here:
bytes on a channel. The `payload` is opaque to the mux, the transport, and the
store.

There is **no per-frame wall-clock timestamp.** Frames are ordered and
correlated by position alone. A producer that wants wall-clock time puts it
*inside* the payload, as a field of its record — it is data, not a property of
the pipe.

### 2.2 The coordinate

Every frame in the system has exactly one address:

```text
Frame  @  (node, life, channel, position)
            └── stream ──┘  source   order
```

| Part       | Type         | Answers          |
|------------|--------------|------------------|
| `node`     | `NodeId`     | *who* produced it |
| `life`     | `Lifetime`   | *which incarnation* of that node |
| `channel`  | `ChannelId`  | *which source* within that node |
| `position` | `Position`   | *where in order* within that stream |

`(node, life)` together are the **stream**; `channel` is the **source** within
it; `position` is the **order**. This four-tuple locates any datum the system
has ever produced. Addressing (§3) is built entirely on it.

### 2.3 Position: per-stream, monotonic, gap-free

```rust
pub struct Position(pub u64);
```

- **One sequence per stream**, not per channel. A node's mux holds a single
  counter shared across every channel, so a given channel's frames carry
  *non-contiguous* positions — a sparse projection of the node's one global
  sequence, interleaved with every other channel.
- **Monotonic and gap-free in assignment.** The mux never reuses a position and
  never skips one when numbering. A position that is assigned but never
  delivered surfaces downstream as a *missing* position — a detectable gap
  (§4.4, §6.3).
- **Not comparable across streams.** Positions order frames within one
  `(node, life)` only. There is no global clock.

**Why one counter and not one per channel.** It keeps the mux a single position
authority and makes gap detection a whole-stream property: a hole means
*something* was lost. The cost is that a drop **cannot be attributed to a
specific channel** — you know a frame is missing, not which channel it carried.
That is the right trade for telemetry.

> **Decision of record — drop attribution.** If a particular source ever needs
> guaranteed contiguous accounting (e.g. "did I lose a datastore op?"), it
> carries its *own* sequence number as a field in its record. The pipe's
> position stays global and dumb; domain counting is domain data.

### 2.4 Stream identity — incarnations never merge

```rust
pub struct StreamId { pub node: NodeId, pub life: Lifetime }
```

`StreamId` is the ingest key. Two streams with the same `node` but different
`life` are **different streams and must never merge.** A node that dies and is
restarted (re-rented, re-scheduled) begins a new `life`, so its fresh stream
does not append to — or collide with — its prior one. Restart is visible, not
silently glued over.

---

## 3. Addressing

### 3.1 The address is the coordinate

A frame is addressed by `(node, life, channel, position)`. A *source* is
addressed by the prefix `(node, life, channel)` — drop `position` and you are
naming a lane rather than a single datum. A *node's whole stream* is
`(node, life)`. Nothing else is needed; there is no separate registry of
producer identities.

### 3.2 Channel paths: opaque on the wire, structured at the edges

```rust
pub struct ChannelId(Arc<str>);   // an opaque token to the pipe
```

To the mux, transport, ingest, and store a `ChannelId` is an uninterpreted
string. They never split it, match it, or validate it. This is what keeps the
pipe dumb and lets a brand-new channel flow end to end with zero pipe changes.

Its **structure is a read-side convention** — known only to producers (to mint
paths they own) and to the catalog and consumers (to match and decode them). It
is therefore *both*: a flat string on the wire, a structured path at the edges.
There is no conflict, because the two views never meet inside the pipe.

### 3.3 The path grammar

A channel path is a dotted sequence of segments:

```text
channel := namespace ( "." qualifier )*
namespace := the owning subsystem        e.g. host, transport, swim, runtime,
                                              dist, datastore, proc, identity
qualifier := instance-key | leaf         instance-key identifies *which* of a
                                              dynamic source; leaf names the signal
```

Examples, current and proposed:

| Path                          | Namespace   | Instance      | Leaf       |
|-------------------------------|-------------|---------------|------------|
| `identity`                    | identity    | —             | —          |
| `host.resource`               | host        | —             | resource   |
| `transport.internals`         | transport   | —             | internals  |
| `proc.trainer.stdout`         | proc        | `trainer`     | stdout     |
| `transport.peer.<id>.conn` *(proposed)* | transport | `peer/<id>` | conn |

A single-segment path (`identity`, `membership`) is the degenerate case: a
namespace with one global signal and no multiplicity. Instance segments appear
only where a source has many instances (one per process, per peer, per actor).

### 3.4 Source selectors

A consumer addresses sources by a **stream scope plus a channel prefix**:

```text
selector := (node?, life?)  channel-prefix [ ".*" ]
```

- `(*, *)  membership`            — every node's membership signal.
- `(X, *)  proc.trainer.*`        — everything the trainer process on node `X`
                                     ever emitted, across that node's restarts.
- `(X, Y)  host.resource`         — one exact source.
- `(*, *)  proc.*`                — all process output, fleet-wide.

Prefix matching over the path **is** the query model. Because instances live in
the path, "all processes" or "one peer's view" is a prefix, never a side table
of what exists. This is the whole reason the path is structured.

---

## 4. Population — the one way in

### 4.1 A producer holds a submission handle

There is exactly one means of population. A producer holds a **submission
handle** to its node's mux and calls:

```rust
fn submit(&self, channel: impl Into<ChannelId>, payload: Vec<u8>);
```

Everything else is sugar over this. A typed record encodes itself and submits
(`record.emit_to(&handle)`); a text source submits a line; a binary source
submits bytes. There is no second mechanism, no "event vs sample vs one-shot"
path. *When* a producer calls `submit` — on a timer, in a callback, once at boot
— is the producer's own business and is invisible to the stream.

The handle is cheaply cloneable and `Send`, so any number of producers, on any
threads, feed the same mux. The mux serializes them into one order (§4.3).

### 4.2 One observer per subsystem

Every subsystem that produces telemetry exposes **one observer trait**, and the
node wires that observer to a submission handle. This is the uniform "means of
population" applied across the codebase:

```rust
// e.g. datastore
trait DatastoreEventObserver { fn on_event(&self, ev: DatastoreEvent); }
// the node installs an observer whose body is `handle.submit(...)`
```

This replaces today's inconsistent wiring (some subsystems install a sink, some
install `None`, some route through the node as an adapter). The rule: **a
subsystem emits by handing its observer a submission handle — nothing else.**
Existing observer-shaped seams (`DatastoreEventObserver`, `ProcessOutputObserver`,
the unused `SwimObserver`) all conform to this one shape.

### 4.3 The mux: single position authority

```rust
pub struct Mux { stream: StreamId, next: AtomicU64, buffer: Mutex<VecDeque<Frame>> }
```

One mux per node. It is the sole assigner of positions and the outgoing buffer:

1. On `submit`, it atomically takes the next position **before** buffering the
   frame. Positions are consumed in submission order and never reused.
2. The frame goes into a bounded buffer.
3. The transport `drain`s the buffer each tick, emptying it in insertion order.

Because the position is taken before buffering, the numbering is gap-free even
when the buffer is not.

### 4.4 Drops are gaps (the back-pressure policy)

The buffer is **bounded**. On overflow the mux **drops the frame but keeps its
position consumed.** The producer never blocks. Downstream, the dropped position
is simply missing — a detectable gap, never a silent renumber.

> **Decision of record — lossy, not blocking.** Telemetry must never apply
> back-pressure to the work it observes. A datastream is allowed to lose frames;
> it is not allowed to stall a producer or to hide that a loss happened. Anything
> that cannot tolerate loss does not belong on the datastream (§9).

---

## 5. Channels

### 5.1 Channels are names, not resources

This is the core of channel management, and it makes the rest of this section
short. A channel is **a name attached to frames** — nothing more. There is:

- no channel object, no `open`/`close`,
- no per-channel counter (positions are per-stream, §2.3),
- no per-channel buffer (one buffer per mux, §4.3),
- no registration call at runtime.

A channel **exists** the instant a frame bears its name, and not before.
Everything below follows from that.

### 5.2 The catalog: a registry of *meaning*, not of *existence*

The catalog maps a channel path to how it should be decoded. It is static,
compile-time, and finite. It answers *"given this path, what does it mean?"* —
never *"what paths exist?"* (that is the store's job, §5.8).

```rust
pub enum ChannelKind { Typed, Text, Opaque }
pub fn classify(channel: &ChannelId) -> ChannelKind;
```

The pipe never consults the catalog. Only producers (to tag) and views (to
decode) do. Adding a channel or teaching a view a new codec changes nothing in
the mux, transport, ingest, or store.

### 5.3 Static channels and dynamic families

- **Static channel** — a fully literal path known at compile time
  (`host.resource`). Registered as a constant.
- **Dynamic family** — a path *template* with parameter segments
  (`proc.{label}.stdout`). The template is registered statically; concrete
  instances are minted at runtime by binding parameters:

  ```rust
  pub fn process_output(label: &str, stream: ProcStream) -> ChannelId; // proc.<label>.<stream>
  ```

The catalog therefore knows every channel **shape** even though it cannot know
every concrete **instance**. "The catalog is the schema contract" survives
dynamic sources intact: `classify("proc.trainer.stdout")` matches the
`proc.{label}.*` family and returns `Text` without ever having heard of
`trainer`.

### 5.4 Typed / Text / Opaque, and the Opaque fallback

- **Typed** — decodes to a structured record under a JSON codec (§5.5).
- **Text** — opaque text; a view treats it as lines (`proc.*`,
  `datastore.events`).
- **Opaque** — *unknown to this consumer.* Retained whole, shown as raw bytes.

> **Decision of record — forward-compatible fallback.** An unrecognized path is
> `Opaque`, never an error. A newer producer may introduce a channel an older
> consumer has never heard of; that consumer carries it, stores it, and renders
> it as raw bytes rather than rejecting it. A consumer never drops a frame it
> does not understand.

### 5.5 Record ↔ channel binding

A typed channel's schema is a Rust type that knows its own channel and
round-trips through the codec:

```rust
pub trait Record: Serialize + DeserializeOwned + Sized {
    const CHANNEL: &'static str;            // or a template, for dynamic families
    fn channel() -> ChannelId;              // the concrete path
    fn encode(&self) -> Vec<u8>;            // serde_json by default
    fn decode(bytes: &[u8]) -> Result<Self>;
}
```

The codec contract is `decode(encode(r)) == r`. In practice most dynamic
families are `Text`/`Opaque` (process output), and typed records are mostly
static; typed-dynamic is supported but uncommon and deliberately un-elaborated.

### 5.6 Schema evolution

Schema evolution is serde discipline, stated as a rule rather than a mechanism:

- **Add fields freely.** New fields are emitted by new producers.
- **Tolerate missing.** `#[serde(default)]` fills a field an older producer did
  not send.
- **Ignore unknown.** A consumer drops fields it does not recognize.
- **Never remove or repurpose.** Retire a field by leaving it unused; introduce
  meaning as a new field.

There is **no schema-version number** in the path or the frame. Append-only
field evolution makes one unnecessary, and absent is simpler than present.

### 5.7 Lifecycle and liveness

Because the stream is append-only, **a source is never deleted.** Its lifecycle
is "first frame → silence":

- A source comes into existence by emitting.
- When it ends (a process exits, an actor stops), it emits a **terminal frame**
  if it has something to say (`proc.<label>.exit` with the code) and then stops.
- Its prior frames remain in the store as history. There is no teardown and no
  channel GC.

**Liveness is a view concern, never a pipe concern.** A consumer decides a
source is dead by TTL-since-last-frame or by observing its terminal frame. The
pipe has no notion of "alive."

Bounding, if any, is on the **stored frames** (the store has finite capacity),
never on the channel identity.

### 5.8 Discovery

Discovery splits cleanly along the catalog/store line:

- The **catalog** answers *meaning* — given a path, how to decode it. Static,
  finite, compile-time.
- The **store** answers *existence* — which paths have actually been seen.
  Dynamic, discovered by scanning stored frame keys at read time.

So "list every live process channel" is a **view over the store** (distinct
paths matching `proc.*`), not a registry lookup. The catalog says what is
*possible*; the store says what is *real*.

### 5.9 Namespacing and ownership

Collisions are prevented by construction, not by a central allocator:

- Each subsystem **owns its namespace** (the first segment): only the `proc`
  code mints `proc.*`, only the `swim` code mints `swim.*`. One module is the
  single source of a namespace's paths.
- An **instance segment must be a stable identifier already unique within that
  namespace's domain**: an actor address, a peer `NodeId`, a process label.
  Uniqueness is inherited from the domain; no coordination is required.

---

## 6. Transport

### 6.1 Best-effort carriage

```rust
pub trait FrameSink { fn deliver(&self, frames: &[Frame]); }
pub struct Delivery { pub stream: StreamId, pub frame: Frame }
```

The transport drains the mux and carries `(StreamId, Frame)` pairs to the
consumer. Implementations are pluggable: in-process, UDP datagrams, the cluster
actor transport, or a scripted transport that injects faults for tests. None of
them changes anything above or below.

### 6.2 Fan-out: drain once, distribute

A node may feed more than one consumer (a local dashboard render **and** a
remote collector). Draining is destructive — the buffer can be drained once —
so fan-out **cannot** be "each sink drains."

> **Decision of record — fan-out shape.** The driver drains the mux **once** per
> tick into a batch, then distributes that batch to each registered
> `FrameSink`. Fan-out is the driver's responsibility, not the mux's and not a
> sink-chaining trick. The mux stays a single drain; sinks stay independent and
> side-effect-only.

### 6.3 What the transport may and may not do

| May (consumer must tolerate) | May **not** |
|------------------------------|-------------|
| **Drop** a frame → a gap     | **Corrupt** a payload, channel, or position |
| **Reorder** frames           | **Renumber** a frame |
| **Delay** a frame            | **Fabricate** a frame |
| **Duplicate** a frame        | Merge two streams' frames |

The envelope is self-describing (length-prefixed fields) and decodes a delivery
or fails cleanly; it never yields a *plausible but wrong* frame. So ingest must
handle drop / reorder / delay / duplicate, and may assume away corruption.

---

## 7. Ingest and storage

### 7.1 The consumer

```rust
pub struct Consumer { store: Store }
impl Consumer { fn accept(&mut self, d: Delivery) -> bool; }
```

Ingest is stateless routing: route each delivery to its stream by `StreamId`,
record the frame at its position. No decoding, no thinning, no aggregation.

### 7.2 Out-of-order, idempotent assembly

```rust
pub struct StoredStream { frames: BTreeMap<u64, Frame> }
```

- **Out-of-order** deliveries self-assemble: each frame is placed by its
  position key, so it lands in order regardless of arrival order.
- **Duplicates collapse:** a position delivered twice is recorded once; the
  first frame wins. Ingest is idempotent.

### 7.3 The store is the truth; gaps are derived

```rust
pub struct Store { streams: BTreeMap<StreamId, StoredStream> }
```

The store holds each node's frames whole and append-only, keyed by `StreamId`.
**Gaps are not stored — they are derived** at read time by walking adjacent
stored positions. An interior hole (a position between two stored frames that is
itself absent) is a gap; trailing absence is just "nothing yet," not a gap.

---

## 8. Views — where all semantics live

### 8.1 Read-time projections

A view is the only thing that interprets a payload. Everything displayed is
*computed at read time* from the stored stream; nothing is precomputed in the
pipe. This is where the telemetry zoo of §1 re-enters — as queries, not as types.

### 8.2 Standard views

| View              | Meaning |
|-------------------|---------|
| `latest(channel)` | the most recent frame on a channel — i.e. a *gauge* |
| `merged_log()`    | all frames in position order, gaps surfaced as `LogEntry::Gap` — i.e. an *event log* |
| `metric_series<R>(channel)` | a typed channel decoded to `Vec<(Position, R)>` — a *time series* |
| `tail(n)` / `grep(pat)` / `filter(pred)` | windowed and predicate-restricted views |

`latest` vs `merged_log` over the *same channel* is exactly the gauge-vs-log
distinction — chosen per query by the consumer, never bound to the channel.

### 8.3 Graceful degradation

A view never fails on data it cannot interpret:

- An unknown channel decodes to `Body::Raw(bytes)` (§5.4).
- A typed frame that will not parse (version skew, truncation upstream) is
  skipped or shown raw, not fatal.
- A gap is rendered as a gap, not as missing context that silently corrupts a
  series.

---

## 9. Invariants (conformance checklist)

A conforming implementation upholds all of these. Each is a property a test can
assert without reaching into internals (scenario / property / contract style).

1. **Opaque payloads.** No code between a producer's `submit` and a view's
   decode inspects, parses, or branches on payload bytes.
2. **Gap-free numbering.** Within a stream, assigned positions are monotonic and
   never reused; a dropped frame leaves its position permanently absent.
3. **Drops are detectable, not silent.** A lost frame appears as a gap, never as
   a renumber or a backfill.
4. **No back-pressure.** A full buffer drops; it never blocks or stalls a
   producer.
5. **Incarnations never merge.** Frames from the same `node` but different `life`
   are stored as distinct streams.
6. **Idempotent ingest.** Delivering the same `(StreamId, position)` twice
   yields one stored frame.
7. **Order independence.** Reordered or delayed deliveries reconstruct the same
   stored stream as in-order delivery.
8. **Unknown channels survive.** A frame on an uncatalogued channel is stored
   whole and rendered as raw bytes, never dropped or errored.
9. **Version skew tolerance.** A typed record missing or carrying extra fields
   decodes (defaults fill, extras ignore) rather than failing.
10. **Semantics only in views.** Removing every view leaves a pipe that still
    transports and stores every frame correctly.
