# Iroh Driver Fixed Specification

Id: 5
Last modified: b887e941cbe6f1e209339abd0375507aca9bfe52
Last reviewed:
> Any edit to this spec must update `Last modified` above to the current `git HEAD` commit.

> Review checkpoint: reviewed through Section 3.2; resume with Section 3.3 Accepted Connection Output.

This document defines the observable contract of the `iroh-driver` crate: what it accepts, what it emits, how it moves from construction to running to shutdown, and what runtime components must exist around it.

---

## 1. Purpose and Contract Boundary

`iroh-driver` is the iroh-backed transport bridge for the actorized distribution stack.

The driver is responsible for:

- owning the concrete iroh endpoint used by one node;
- binding the endpoint with the configured relay mode, secret key, and ALPNs;
- accepting inbound iroh connections;
- dialing peer endpoints for joins and actor-message delivery;
- caching live peer connections;
- shuttling framed actor messages between iroh QUIC streams and Swactor actor mailboxes;
- running driver-owned adapters for accepted non-actor protocols and emitting protocol-level ingress/status;
- providing the telemetry QUIC adapter used by telemetry subscribers and collectors;
- reporting local endpoint identity, endpoint address, join status, and route-view extent.

The driver is not responsible for:

- SWIM membership semantics;
- registry, metadata, or directory convergence;
- actor scheduling;
- application actor behavior;
- model/runtime orchestration;
- telemetry record meaning;
- relay-server operation;
- durable logging or telemetry storage.

Those behaviors belong to the `distribution`, `swactor`, `telemetry`, and application crates. The driver supplies transport and pump seams that those systems use.

---

## 2. Input Channels

The driver accepts input through construction configuration, explicit method calls, an actor egress MPSC channel, inbound iroh connections, telemetry subscriptions, and shutdown requests.

### 2.1 Construction and Configuration Inputs

Construction input is `IrohDriverConfig` plus a swactor engine handle (see §2.1).

`IrohDriverConfig` contains:

```text
IrohDriverConfig {
    secret_key,
    relay_mode,
    node,
    peer_auth,
    additional_alpns,
}
```

`secret_key` selects the iroh endpoint identity. When absent, iroh generates a fresh identity.

`relay_mode` is passed to the iroh endpoint builder. Custom relay mode is supported as endpoint configuration. The driver does not start relay servers.

`node` carries distribution-node configuration used by callers to build protocol actors. The current driver stores transport-facing state and does not run distribution protocol logic itself.

`peer_auth` is an optional allow-list used on inbound accepts and outbound connection attempts.

`additional_alpns` registers internal wire ALPNs on the iroh endpoint. Non-actor connections negotiated on those wire protocols are handled by registered protocol adapters rather than returned directly to callers.

The target driver is constructed with a swactor engine handle — the single
engine that owns the process's Tokio substrate:

```text
IrohDriver::with_engine(engine_handle, config)
```

The driver validates engine capabilities (tasks, timers, io) before binding the
endpoint. It must not create, discover, or store a raw Tokio runtime handle.

### 2.2 Actor Egress Channel Input

The actor egress input is a per-peer actor egress queue with MPSC semantics.

The target queue shape is the core hybrid channel:

```text
many Sender<OutFrame> handles
-> HybridChannel<OutFrame>
   -> ArrayQueue hot path
   -> linked spillover queue
-> one Receiver<OutFrame>
-> peer-scoped iroh writer
```

The queue has multiple producer handles because Swactor worker threads and transport routes may enqueue frames concurrently. The driver owns the single receiver for each peer and is the only consumer of that peer's egress queue.

The driver does not create `OutFrame` values. They are created by the Swactor remote-send path installed by the distribution stack.

The egress path is:

```text
actor sends to a nonlocal ActorAddress
-> Swactor RemoteSink
-> CodecRemoteSink
-> CodecRegistry encodes the typed actor message
-> TransportRouter routes the destination to IrohEgressTransport
-> IrohEgressTransport resolves the destination address to a peer node id
-> IrohEgressTransport sends OutFrame into the peer's actor egress queue
-> peer-scoped iroh writer drains the queue and writes actor records over iroh
```

The target contract has one iroh egress transport with two address-resolution cases:

```text
if dest is a peer mailbox address:
    to = NodeId(dest bytes)
    wire_dest = dest

if dest is an actor address hosted by a remote peer:
    to = route_view[dest]
    wire_dest = dest
```

Peer-mailbox addresses carry distribution protocol traffic such as SWIM, registry, metadata, directory gossip, and join messages.

Directory-routed actor addresses carry application or protocol actor messages for actors that the directory route view says are hosted by a remote peer.

Both resolution cases produce the same frame shape:

```text
OutFrame {
    to,
    dest,
    type_tag,
    payload,
}
```

`to` is the destination peer node id. `dest` is the actor address carried on the wire. For distribution gossip, `dest` is the destination peer mailbox. For directory-routed application messages, `dest` is the target actor address. `type_tag` selects the actor codec entry. `payload` is an already-encoded actor message payload.

The target implementation should use the core hybrid channel rather than `Arc<Mutex<Vec<_>>>`. Enqueue from actor worker threads must be nonblocking. All actor control messages for a peer share the same per-peer egress queue and stream.

### 2.3 Peer Join Input

Cluster join input is a list of seed `EndpointAddr` values:

```text
IrohDriver::join(seeds)
```

Each seed carries the peer public key and may carry direct socket addresses and relay URLs.

A join request is sent as a framed actor message over the actor ALPN. The request is addressed to the seed node's peer mailbox and uses the `JoinRequest` network message type tag.

Join attempts run as engine-hosted tasks. The `join` call does not synchronously wait for connection establishment or membership convergence.

### 2.4 Incoming Iroh Connection Input

Inbound iroh connections enter through the endpoint accept loop spawned during driver construction.

For every accepted connection, the driver observes:

```text
remote peer identity
negotiated ALPN
connection object
```

If `peer_auth` rejects the remote node id, the connection is closed as unauthorized.

Connections negotiated on the actor ALPN enter the actor connection cache path. Connections negotiated on other ALPNs are routed to the registered protocol adapter for that internal wire protocol.

### 2.5 Additional Protocol Binding Input

`additional_alpns` are internal wire selectors registered on the same iroh endpoint in addition to the actor ALPN.

They exist so one node identity and one iroh endpoint can carry non-actor protocols such as telemetry transport or edge byte transport without routing those bytes through Swactor actor mailboxes.

The target caller-facing input is a logical protocol binding, not an accepted iroh connection:

```text
ProtocolBinding {
    protocol_id,
    wire_alpn,
    ingress_sink,
    egress_command_source,
    adapter,
}
```

`wire_alpn` is used by the driver and adapter for endpoint negotiation. Actors and protocol managers address the logical `protocol_id`, edge id, stream id, or protocol-specific message shape; they do not receive ALPNs, iroh connections, QUIC streams, or Tokio task handles.

Additional protocol lifecycle:

```text
driver construction registers additional wire ALPN bytes
-> remote peer connects to this endpoint with one of those wire protocols
-> iroh completes ALPN negotiation
-> driver accept loop validates peer authorization
-> driver selects the registered protocol adapter for the negotiated wire protocol
-> driver-owned adapter task owns the connection and transport stream lifecycle
-> adapter decodes protocol records or writes protocol rings
-> adapter emits protocol messages, logical-stream status, ring wakeups, or protocol faults
```

The target driver does not expose raw accepted connections for additional protocols. Any legacy drain method that returns an iroh connection is a compatibility surface, not the target orchestration contract.

### 2.6 Telemetry Subscription Input

Telemetry uses the actor plane for subscription control and the telemetry ALPN for frame transport.

Subscription control flow:

```text
collector/orchestrator
-> TelemetryPublisherMsg::Subscribe(TelemetrySubscribe)
-> source node's TelemetryPublisherActor
-> source TelemetryEndpoint::subscribe(request)
-> TelemetrySubscription
-> telemetry QUIC writer
```

`TelemetrySubscribe` carries:

```text
collector EndpointAddr
SubscriptionRequest
flow_id
token
```

The source node's publisher actor owns the local subscription step. Its transport callback starts the iroh writer for the returned `TelemetrySubscription`.

Outbound telemetry frame flow:

```text
local producers submit frames to TelemetryEndpoint
-> TelemetryEndpoint::tick drains the local mux
-> DeliveryFanout publishes future TelemetryEvent values to subscribers
-> telemetry QUIC writer dials collector over TELEMETRY_ALPN
-> writer opens a unidirectional stream
-> writer sends header snapshot
-> writer sends future subscription events until the subscription closes
```

The header snapshot contains stream identity and channel catalog state visible at subscription time. It is not a replay of prior frames.

Inbound telemetry frame flow:

```text
collector driver accepts TELEMETRY_ALPN internally
-> telemetry adapter owns the connection reader task
-> adapter accepts telemetry unidirectional streams
-> adapter decodes header and event records
-> adapter forwards TelemetryEvent values to the collector fanout or sink
```

Reader helpers:

```text
read_next_uni_from_connection
read_events_from_stream
read_stream_into_fanout
spawn_connection_reader
```

Writer helpers:

```text
spawn_subscription_writer
write_available_subscription
write_subscription_until_closed
write_event
```

Telemetry broadcasts are local fanout broadcasts inside `TelemetryEndpoint`. Over iroh, each remote collector subscription is a separate writer/connection flow. The driver crate transports telemetry events; it does not define the semantic meaning of telemetry channel payloads.

### 2.7 Shutdown Input

Shutdown input is async and runtime-owned:

```text
IrohDriver::close().await
```

`close` is the target teardown path. It closes the iroh endpoint from inside the engine substrate that owns the driver tasks.

---

## 3. Output Channels

The driver emits output through Swactor actor delivery, iroh QUIC streams, protocol adapter ingress/status queues, telemetry events, and observable driver state.

### 3.1 Swactor Actor Delivery Output

Inbound actor frames decoded from iroh are delivered to the Swactor runtime through:

```text
Runtime::deliver_raw(actor_address, boxed_message)
```

Delivery requires an installed actor bridge.

Gossip frames addressed to the local peer mailbox are routed by `type_tag` to the local protocol actor that owns that tag.

Application frames addressed to a concrete actor address are delivered directly to that actor address. If the destination actor is not local or delivery fails, the frame is dropped best-effort.

### 3.2 Iroh QUIC Send Output

Outbound actor frames are written over the cached iroh connection for the destination peer.

For each connected peer, the target driver owns one actor egress queue and one active actor egress stream in that direction.

All actor frames for that peer are sent into the peer's egress queue. A peer-scoped writer drains the queue and writes length-delimited actor records to the active actor egress stream.

The writer may batch queued records to reduce wakeups and small writes, but batching must remain latency-bounded. All actor traffic for that peer uses the same queue and stream.

On a connection cache miss, the driver starts or continues a background dial and drops the current frame best-effort.

### 3.3 Additional Protocol Ingress Output

Connections negotiated on non-actor wire protocols are not output as iroh connections. They are consumed internally by driver-owned protocol adapters.

Generic protocol ingress output:

```text
ProtocolIngress {
    peer,
    protocol_id,
    event,
}
```

Streaming protocols expose logical stream events, not transport streams:

```text
StreamOpened { peer, stream_id, protocol_stream_id }
StreamReady { stream_id }
StreamEnded { stream_id }
StreamFault { stream_id, reason }
```

For ring-backed protocols, payload bytes are not copied through actor messages. The driver recv pump writes inbound bytes into the protocol's ingress ring and wakes the ring reader. The driver send pump is woken by egress-ring commits and drains bytes from the ring to the network stream.

Actors observe lifecycle, readiness, wake, and fault messages. They do not receive iroh connections, negotiated ALPNs, QUIC streams, or Tokio task handles.

### 3.4 Telemetry QUIC Output

Telemetry writer functions emit telemetry headers and telemetry event records over iroh unidirectional QUIC streams.

Telemetry reader functions emit:

```text
TelemetryQuicRead {
    header,
    events,
}
```

or forward decoded `TelemetryEvent` values to a `DeliveryFanout` or `mpsc::Sender`.

### 3.5 Driver State and Join Status Output

The driver exposes local observable state through query methods:

```text
node_id
endpoint_addr
direct_addresses
listen_addr
snapshot
directory_route_count
location_cache_entries
join_statuses
relay_url
home_relay_url
```

These outputs are diagnostic and coordination aids. They do not by themselves make a peer alive, a route owned, or an actor message delivered.

---

## 4. Wire and Address Model

The driver defines ALPN selection, the concrete iroh wire format for actor messages, and the telemetry adapter's concrete iroh wire format for telemetry records.

### 4.1 ALPN Negotiation Model

ALPN means Application-Layer Protocol Negotiation.

In this driver, an ALPN is a byte-string protocol name attached to an iroh/QUIC connection attempt. The connecting peer asks for one protocol name, the accepting endpoint must have registered that protocol name, and the established connection records the negotiated protocol.

ALPN is connection-level protocol selection. It is not peer authorization, actor routing, telemetry channel selection, subscription selection, or payload decoding.

During driver construction, the endpoint is bound with:

```text
actor ALPN
+ additional ALPNs
```

When a peer connects, iroh negotiates exactly one ALPN for that connection. The driver accept loop reads that negotiated ALPN and routes the accepted connection:

```text
if negotiated ALPN == actor ALPN:
    cache connection for actor-message traffic

else:
    hand connection to the registered protocol adapter for that wire protocol
```

Actor-message connections may carry one or more actor-message streams. Each actor-message stream may carry one or more length-delimited actor records. Additional protocol connections may carry adapter-owned transport streams such as telemetry streams or edge byte streams. The bytes inside those streams are interpreted only by the selected adapter after the connection has been classified by ALPN.

### 4.2 Actor Message ALPN

Actor messages use:

```text
swactor/swim/1
```

The name is historical. The ALPN carries both distribution gossip messages and directory-routed application actor messages.

### 4.3 Actor Message Frame

An actor message is a length-delimited record carried inside an actor-message stream.

Target wire record:

```text
[32B dest][4B tag_len BE][tag bytes][4B payload_len BE][payload bytes]
```

`dest` is an `ActorAddress`.

`tag_len` is big-endian and bounded by the reader. Tags larger than `1024` bytes are rejected.

`tag` is a UTF-8 network message type tag.

`payload_len` is big-endian and bounded by the reader.

`payload` is the already-encoded actor message payload.

### 4.4 Telemetry ALPN

Telemetry records use:

```text
swactor/telemetry/0
```

The telemetry transport opens unidirectional streams over a connection negotiated with this ALPN.

### 4.5 Telemetry Frame

A telemetry unidirectional stream begins with a header:

```text
magic = "DSQ1"
flow_id: [u8; 16]
token_len: u16 LE
token bytes
stream descriptor JSON
channel descriptors JSON
```

Then zero or more length-prefixed records follow:

```text
record_len: u32 LE
record bytes
```

Record tags:

```text
0x01 = channel declared
0x02 = frame
0x03 = stream ended
```

A frame record contains:

```text
tag
channel_id: u32 LE
position: u64 LE
payload_len: u32 LE
payload bytes
```

Each telemetry record is bounded by `16 * 1024 * 1024` bytes.

`StreamDeclared` events are represented in the stream header and are not emitted as individual records by the current writer.

### 4.6 Node Identity and Endpoint Addressing

The driver node id is derived from the iroh endpoint public key.

When a secret key is configured, the same identity is used by iroh and by the distribution signing keypair reconstructed inside the driver.

The driver signs local actor host claims with this identity through:

```text
IrohDriver::register_actor(actor_addr, generation)
```

`EndpointAddr` is the peer addressing shape used for joins and dials. It may contain:

```text
public key
direct socket addresses
relay URLs
```

`endpoint_addr` starts from iroh's current endpoint address and augments unspecified bind sockets with discovered LAN or localhost addresses.

---

## 5. Code Architecture

The crate has two public modules:

```text
iroh_driver
telemetry_transport
```

### 5.1 Driver Core

`IrohDriver` owns:

```text
keypair
endpoint
engine handle
connection cache
pending joins
accepted actor connections
additional protocol adapter state
incoming actor frame queue
protocol ingress and status queues
eviction queue
peer relay cache
join status map
actor bridge
```

The driver core is intentionally pumpable. Network I/O runs in engine-hosted tasks. Pump methods drain in-memory queues and do not perform long blocking network operations.

### 5.2 Actor Bridge

`ActorBridge` is installed with:

```text
IrohDriver::enable_actor_bridge(...)
```

It owns the driver's view of:

```text
Swactor runtime
codec registry
type-tag ingress routes
SWIM actor address
local peer mailbox address
relay mirror
route view
```

Without an actor bridge, inbound actor frames can be accepted and queued but cannot be delivered into Swactor actors.

### 5.3 Background Accept, Join, Dial, and Reader Tasks

The construction path spawns one endpoint accept task.

Join requests spawn background tasks with retry and backoff.

Cache-miss egress dials spawn background connection tasks.

Each cached actor connection has reader work that accepts actor-message streams, decodes length-delimited actor records, and pushes raw actor frames into the incoming actor frame queue.

Writer work is peer-scoped. One writer drains the peer's actor egress queue and writes length-delimited records to the active actor egress stream. The writer may batch records.

Additional protocol bindings install driver-owned adapter work. Adapter tasks own non-actor connection lifetime, transport stream acceptance/opening, and protocol framing. Adapters emit protocol messages, logical stream status, ring wakeups, or protocol faults instead of returning iroh connections to callers.

For ring-backed edge-style protocols, send pumps wake on egress-ring commits, drain bytes from the ring, and write them to the network stream. Receive pumps read network bytes into ingress rings and wake the worker or parser that owns those rings.

### 5.4 Connection Cache and Eviction

The connection cache maps peer `NodeId` to a cached iroh connection and generation.

New accepted connections and completed join/dial connections are folded into the cache during pump ingress.

Send failures queue generation-scoped eviction records. A delayed failure from an old connection generation must not evict a newer replacement connection.

After evicting a current failed connection, the driver starts a background redial when it can reconstruct the peer public key.

### 5.5 Telemetry Transport Adapter

`telemetry_transport` is a QUIC adapter for `telemetry` subscriptions.

It owns:

```text
TELEMETRY_ALPN
TelemetryQuicHeader
TelemetryQuicWriteStats
TelemetryQuicRead
writer helpers
reader helpers
fanout helper
```

It is transport code only. Channel naming, record schemas, catalog semantics, and storage belong to the `telemetry` crate and callers.

---

## 6. Runtime Lifecycle

The driver lifecycle is a single endpoint lifecycle: construct and bind, install actor bridge and protocol adapters, join peers as requested, run pump cycles, drive protocol ingress/status queues, then close.

### 6.1 Boot and Endpoint Bind

Boot begins when a caller constructs the driver.

Construction:

```text
resolve swactor engine handle and required capabilities
build iroh endpoint with actor ALPN and additional protocol wire ALPNs
apply relay mode
apply secret key when configured
bind endpoint
reconstruct distribution keypair from endpoint secret
spawn accept loop
initialize queues and caches
```

Construction failure returns an error and no usable driver.

### 6.2 Actor Bridge Establishment

After the caller creates the Swactor runtime, distribution actors, codec registry, actor egress channel, relay mirror, and route view, it installs the actor bridge.

The bridge must be installed before inbound actor frames can be delivered to local actors and before send failures can be reported to SWIM.

### 6.3 Peer Join and Connection Establishment

Peer join begins when `join(seeds)` is called.

For each seed, the driver:

```text
records relay hints
normalizes the seed endpoint address with cached relay information when possible
spawns a background join task
updates join status during attempts
queues a successful connection for later cache folding
```

Membership convergence is not completed by `join` alone. SWIM and the distribution actors must progress after a connection exists.

### 6.4 Engine-Hosted Progression

Actor progression, protocol ticks, and all driver adapter work — inbound actor frames, actor egress, protocol adapter ingress/status, and ring-backed protocol pumps — are owned by a single swactor engine. The application installs the engine-hosted adapter pump via `IrohDriver::install_actor_bridge_pump` and does not pump any queue itself.

The engine-hosted pump cycle advances the following per interval:

```text
send protocol actor ticks
drain inbound iroh actor frames into Swactor
actor work progresses through the engine-owned worker drivers
drain actor egress channel to iroh
drain protocol adapter ingress/status queues
wake or drain ring-backed protocol pumps as needed
```

Protocol adapter drains carry messages, logical stream status, readiness, and faults. Payload bytes for ring-backed protocols move through rings and ring wakeups, not through actor-drained byte messages.

The engine owns the loop, timing, and protocol actor tick injection; the application only configures components, consumes reports, and owns domain queues (ENGINE_SPEC.md §5).

### 6.5 Telemetry Connection Handling

Telemetry handling is established by registering `TELEMETRY_ALPN` as an internal wire protocol for the telemetry adapter.

A collector installs a telemetry ingress sink or fanout. The driver-owned telemetry adapter consumes accepted telemetry connections internally, accepts unidirectional streams, decodes headers and events, and forwards `TelemetryEvent` values to that sink.

A publisher uses the telemetry transport writer adapter to connect to a collector endpoint address, open a telemetry stream, write the header snapshot, and stream subscription events until the subscription closes.

### 6.6 Shutdown and Close

Async shutdown calls:

```text
IrohDriver::close().await
```

`close` closes the iroh endpoint.

After endpoint close, the accept loop exits. Existing background tasks observe connection or stream errors and end according to iroh behavior.

The target driver does not create a private Tokio runtime and does not expose synchronous shutdown as part of the orchestration contract.

---

## 7. Behavioral Contracts

These contracts define the behavior callers may rely on.

### 7.1 Pump Progress

Actor delivery over iroh requires pump progress.

A driver that is constructed and joined but not pumped may have open iroh connections while actor messages, membership updates, registry gossip, metadata gossip, directory gossip, route ownership, and send-failure feedback do not reach their owning actors.

### 7.2 Nonblocking Dial and Send Behavior

Join, cache-miss dial, and send operations must not block the actor pump on long network operations.

Join and dial attempts run in background Tokio tasks.

Outbound actor send writes run in background Tokio tasks.

A cache miss starts or continues background dialing and drops the current outbound actor frame best-effort.

Actor egress enqueue must not block actor worker threads.

The target actor queue uses the core hybrid channel: an array-backed hot path with linked spillover. Normal enqueue does not wait for network I/O, connection establishment, or the peer writer task.

Ring-backed protocol egress is wake-driven. A producer commit to an egress ring wakes the driver send pump. The send pump reads directly from the ring and advances the consume cursor after successful network writes.

Ring-backed protocol ingress is wake-driven. The driver receive pump writes bytes into the ingress ring and wakes the worker or parser. If the ingress ring is full, the receive pump pauses until writable capacity is available.

Actors do not copy byte payloads between protocol rings and network pumps. Backpressure and failure are handled by ring capacity, protocol pumps, and protocol fault messages, not by blocking Swactor actor execution.

### 7.3 Best-Effort Delivery and Drop Rules

The driver does not guarantee delivery of every actor egress frame.

Frames may be dropped when:

- the target node id cannot be converted to an iroh public key;
- peer authorization rejects the target;
- no cached connection exists and a background dial is needed;
- an inbound frame has an unknown type tag;
- an inbound gossip tag has no local route;
- an inbound application destination is not local;
- raw delivery into Swactor fails.

Reliable higher-level behavior must be supplied by the owning protocol actors.

### 7.4 SWIM Send-Failure Feedback

Write failure for SWIM message tags reports:

```text
SwimIn::SendFailed { to }
```

to the local SWIM actor when the actor bridge is installed.

Write failure for non-SWIM gossip or application frames is best-effort and does not by itself report a lifecycle error.

### 7.5 Peer Authorization

When `peer_auth` is configured, inbound connections from unauthorized node ids are closed.

Outbound connection attempts to unauthorized node ids fail before dialing.

Peer authorization is a transport admission check. It does not replace distribution-level trust, directory-entry validation, or application-level authorization.

### 7.6 Connection Reuse, Eviction, and Redial

The driver reuses cached open connections.

Closed cached connections are removed before reuse.

Send failures enqueue generation-specific eviction. Only the current matching generation may be evicted.

After current-generation eviction, the driver attempts background redial when the peer public key is available.

### 7.7 Telemetry Ordering and Limits

Telemetry ordering is stream-local and follows the order in which events are read from one telemetry unidirectional stream.

The telemetry transport preserves each event's channel id, position, and payload bytes.

A telemetry record larger than `16 * 1024 * 1024` bytes is rejected.

A telemetry header token larger than `u16::MAX` bytes is rejected.

Telemetry channel semantics remain outside the iroh driver.

### 7.8 Transport Boundary Conformance

The driver boundary is a capability boundary. Raw Tokio and iroh transport capabilities are confined to `iroh-driver`.

Callers outside the driver must not depend on accepted iroh connections, QUIC stream handles, Tokio task handles, or caller-side ALPN dispatch for production transport behavior. They observe actor delivery, typed telemetry ingress/status, logical protocol status and faults, ring wakeups, and ring byte movement.

Conforming implementations satisfy four boundary rules:

```text
transport capability boundary:
    driver owns endpoint, ALPN dispatch, connections, streams, Tokio tasks, and shutdown
    callers do not accept connections, open streams, or manage transport task handles

protocol surface boundary:
    telemetry exposes decoded telemetry ingress/status/faults
    ring-backed protocols expose readiness, wakeups, closure, and faults
    no protocol surface exposes iroh connection or stream objects

pump ownership boundary:
    driver-owned tasks perform network reads, network writes, and ring byte pumps
    actors and protocol managers do not copy hot-path payload bytes between rings and network streams

shutdown boundary:
    driver close owns transport teardown
    callers are not responsible for stopping protocol runner or pump tasks
```

Behavioral verification should prove these boundaries through observable effects, not by relying on implementation names. A conforming implementation can be checked by:

```text
boundary check:
    production code outside iroh-driver cannot use raw iroh/Tokio transport capabilities except through explicitly marked compatibility surfaces

ALPN dispatch check:
    actor, telemetry, and ring-backed connections are accepted and dispatched inside the driver
    callers do not drain accepted iroh connections

telemetry check:
    telemetry QUIC traffic becomes typed telemetry ingress/status/fault output
    callers do not observe iroh connections, streams, ALPNs, or task handles

ring-backed protocol check:
    edge bytes move through driver-owned pumps into and out of rings
    pumps pause and resume through ring capacity and wakeups
    object-record parsing remains outside the driver

shutdown check:
    close stops endpoint accept, protocol runners, and pump work without caller-held task handles
```

Legacy APIs that expose endpoint clones, raw accepted connections, raw streams, or transport task handles are compatibility surfaces only. They are not part of the target behavioral contract.

---

## 8. Required Dependencies and Establishment Order

The driver is only one part of a running distributed actor node. Several components must be established around it.

### 8.1 Engine Substrate

The driver requires a swactor engine handle with task, timer, and I/O capability for:

- endpoint bind;
- endpoint accept;
- dialing;
- stream reads;
- stream writes;
- retry timers;
- async close.

Production code passes this substrate explicitly with `with_engine`.

The driver must not create, discover, or store a raw Tokio runtime. The caller keeps the owning swactor engine alive for at least as long as the driver is alive.

### 8.2 Iroh Endpoint

The iroh endpoint is the network identity and QUIC transport endpoint.

It must be bound before endpoint addresses can be advertised or peers can connect.

The endpoint must register the actor ALPN and every internal wire ALPN required by installed protocol adapters.

### 8.3 Swactor Runtime

The Swactor runtime handle owns local actor mailboxes and actor delivery APIs.

The driver stores a cloned `Runtime` handle only after the actor bridge is installed.

The driver delivers decoded inbound actor messages into this runtime with `deliver_raw`.

Actor execution is driven by the swactor engine that consumed `RuntimeParts`; the driver does not tick actors itself.

### 8.4 Distribution Actors

The distribution protocol actors are required for cluster behavior:

```text
SwimActor
RegistryActor
MetadataActor
DirectoryActor
```

The driver transports their messages but does not implement their state machines.

The caller must create these actors, route their message type tags, install the engine-hosted protocol tick pump, and subscribe/fan out membership changes as required by the distribution stack.

### 8.5 Codec Registry, Transport Router, and Actor Egress Channel

The codec registry maps wire `type_tag` values to concrete actor message values.

The transport router and actor egress channel are the path used by remote actor sends.

A typical node establishes:

```text
CodecRegistry
TransportRouter
CodecRemoteSink
IrohEgressTransport
peer-mailbox address resolution
directory route-view address resolution
HybridChannel<OutFrame>
Sender<OutFrame> handles
Receiver<OutFrame>
```

The driver owns the `Receiver<OutFrame>` for each peer. Transport routes and actor worker threads hold cloned `Sender<OutFrame>` handles. The queue implementation is the core hybrid channel: array-backed hot path with linked spillover, not a mutex-protected vector.

### 8.6 Telemetry Endpoint and Fanout

Telemetry producers, endpoints, subscriptions, and fanout are owned by the `telemetry` crate and application code.

The driver crate supplies the QUIC transport adapter. A collector provides a fanout or ingress sink for decoded events. A publisher provides subscriptions for outbound publishing. The driver-owned adapter handles accepted telemetry connections and stream reader/writer tasks internally.

---

## 9. Error Handling

Driver errors are localized to the operation that observes them. Higher-level lifecycle interpretation belongs to the caller.

### 9.1 Construction Errors

Construction can fail while building or binding the iroh endpoint.

Construction errors return from `with_engine`.

No runtime pump contract exists for a driver that failed construction.

### 9.2 Join and Dial Failures

Join attempts update join status through phases such as connecting, sending, sent, and failed.

A failed join task does not by itself terminate the driver.

Cache-miss dials that fail leave the current frame dropped. Future protocol ticks may produce more frames and more dial attempts.

### 9.3 Decode and Delivery Failures

Malformed actor streams are dropped without destroying the connection.

Unknown or undecodable actor frames are dropped.

Delivery failures into the Swactor runtime are ignored by the driver. The owning protocol must tolerate missing best-effort frames unless it has its own retry/timeout contract.

### 9.4 Send Failures

A send failure queues the failed connection generation for eviction.

If the frame was a SWIM frame and the actor bridge is installed, the driver reports `SendFailed` to SWIM.

The failed frame is not retried by the driver.

### 9.5 Telemetry Failures

Telemetry writer and reader adapters report invalid headers, invalid record tags, oversized records, truncated frames, stream read/write failures, and JSON decode failures as telemetry status or fault results.

Telemetry helper internals may use Tokio tasks, but callers observe protocol status through driver/telemetry outputs rather than managing task handles.

Telemetry failures are transport failures. Whether they are fatal to a runtime is decided by the caller.

### 9.6 Additional Protocol Adapter Failures

Protocol adapter failures are reported as protocol-level status or fault events, not as raw iroh errors.

Network read failure maps to a logical stream or protocol read fault.

Network write failure maps to a logical stream or protocol write fault.

Protocol framing errors map to protocol decode faults for the affected logical stream or connection.

Ring-backed protocol ingress pauses when the target ring is full and resumes when ring capacity becomes available. Ring-backed protocol egress pauses when the network cannot accept writes and resumes when the send pump can drain more ring bytes.

### 9.7 Shutdown Errors

Async close does not return a driver-level error. Synchronous shutdown is a legacy implementation detail and is outside the target orchestration contract.

---

## 10. Out of Scope

This specification does not define:

- distribution protocol actor internals;
- SWIM membership correctness;
- registry, metadata, or directory data models;
- application actor message schemas;
- model-runtime or orchestrator lifecycle semantics;
- telemetry channel schemas or dashboard rendering;
- relay server deployment;
- encryption beyond iroh's transport security;
- multi-endpoint ownership inside one driver;
- guaranteed delivery for actor egress frames;
- durable storage of transport observations.
