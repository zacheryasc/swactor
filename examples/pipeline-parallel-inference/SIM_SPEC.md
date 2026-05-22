# SIM_SPEC — simulator MVP

Status: draft, pre-implementation. Lives in `examples/pipeline-parallel-inference/`
so it is not touched by the ongoing simplification of `crates/simulation`. It
moves alongside the implementation once that cleanup lands.

This spec describes an architecture and an implementation strategy concretely
enough that two independent implementers, working from this document alone,
produce code that meshes. To that end it names components, the data that
crosses every boundary between them, and the behaviour each component owes
the others. Internal data structures, module layout, and helper types are the
implementer's call; the named boundaries are not.

The MVP's first consumer is SWIM, because SWIM is the algorithm whose
production failures motivated the simulator. The architecture is not SWIM-
specific: the engine knows about *hosts*, not about SWIM. A host is a piece
of code that consumes ticks and inbound messages and emits actions. SWIM is
the first such host kind; the second is whatever we need next.

---

## 0. Motivation

The pipeline-parallel-inference example failed eight live N≥3 vast.ai deploys.
The session report (`N3_DEPLOYMENT_REPORT.md`, next to this file) traces the
failure to a SWIM gossip-flap bug ("B1"): in a seven-minute run, the
orchestrator refuted Suspect claims against itself 228 times — roughly once
every 1.8 seconds — and one peer ended the run marked Dead despite probes
succeeding in both directions on both sides of the link. The bug is not
visible in any test we have today. It only appears with three or more peers,
multi-region latency, and enough cumulative gossip state for piggybacked
membership updates to grow into the multi-kilobyte range.

Catching that bug in production costs about two dollars of GPU rental per
attempt, forty-five to ninety minutes of engineer time per iteration, and
produces one non-reproducible bundle of evidence per run. The same source,
run twice, produces different outcomes.

The simulator exists to make the iteration loop sub-second and the outcomes
byte-identical for a fixed seed. It is not a complete model of production;
it is the smallest model that lets us tune SWIM without deploying. Future
algorithms layer onto the same engine without changing the SWIM behaviour
this MVP guarantees.

---

## 1. What "done" means

The MVP ships when these are simultaneously true.

A property test reproduces the gossip-flap bug deterministically against the
current SWIM source. The same scenario with the same seed produces byte-
identical output across runs and across the architectures we claim to
support.

The fix workflow does not deploy. A developer writes a property, runs the
sim, sees it fail, edits the SWIM source, re-runs, sees it pass — all
locally, in under a second per iteration.

The simulator is calibration-grounded against the three N3 bundles we have.
Distributions emitted by the sim, when configured to mirror a given N3 run,
are within declared tolerances of the corresponding live bundle.

A sim bundle diffs cleanly, at the schema level, against any prod bundle. A
sim bundle may be a strict subset of prod's observable surface — events for
subsystems the MVP does not model are listed in a known-gaps document that
shrinks over time — but it may never introduce events prod does not produce,
and may never omit an event from a subsystem it claims to model.

Each known live failure mode has at least one scenario in the library, with
a prose comment naming what it reproduces.

---

## 2. Deliberate non-goals for the MVP

These are out of scope; each has a re-entry point in §13.

- The MVP does not run real iroh or quinn. SWIM's transport is a stream of
  self-contained messages; the sim models the message bus, not the QUIC wire
  protocol. iroh-induced behaviours SWIM is sensitive to — connection-cache
  hit/miss latency, relay routing, cold-dial penalty — are exposed as link-
  policy knobs.
- The MVP does not virtualize async runtimes. The engine is single-threaded
  and synchronous; the SWIM state machine is synchronous.
- The MVP does not migrate the broader `distribution` crate onto a runtime
  facade. SWIM-tuning needs only the SWIM module.
- The MVP does not run the determinism detector as a peer.
- The MVP does not execute recorded production bundles. The scenario format
  is forward-compatible with replay; the converter is later work.
- The MVP does not model swactor mailboxes or any peer behaviour beyond
  SWIM membership.
- The MVP ships no GUI. The bundle is the artefact.

---

## 3. Architecture

### 3.1 Components

The simulator is one process holding six named components. Each component
has one responsibility and one set of inbound and outbound message types.
The boundaries between them are the contract two independent implementers
must agree on; nothing inside a component is.

```
            scenario.toml
                 │
                 ▼
          ┌──────────────┐
          │ scenario     │   parses and validates the input
          │ loader       │
          └──────┬───────┘
                 │ parsed scenario
                 ▼
          ┌──────────────┐    send queries     ┌──────────────┐
          │              ├────────────────────►│   network    │
          │   engine     │◄────────────────────┤              │
          │              │   arrival times,    └──────────────┘
          │              │   drop reasons
          │              │
          │              │    ticks, recv       ┌──────────────┐
          │              ├────────────────────► │   hosts      │
          │              │◄──────────────────── │  (per peer)  │
          │              │    actions           └──────────────┘
          │              │
          │              │    events, snapshots ┌──────────────┐
          │              ├────────────────────► │   bundle     │
          │              │                      │   writer     │
          └──────────────┘                      └──────┬───────┘
                                                       │
                                                       ▼
                                              bundle on disk
                                                       │
                                                       ▼
                                              ┌──────────────┐
                                              │ assertion    │
                                              │ evaluator    │
                                              └──────┬───────┘
                                                     │
                                                     ▼
                                              verdicts on disk
```

**Scenario loader.** Reads a TOML scenario, validates it, returns a parsed
scenario value (§8). No state; pure function from path to validated
scenario.

**Engine.** Owns the virtual clock, the scheduling queue, the table of
hosts, references to the network and the bundle writer. Single entry point:
given a parsed scenario, run to completion. §4 specifies behaviour.

**Network.** A directed-graph link model. Answers send queries
deterministically and accepts mutations on a timeline. Holds no schedule of
its own; the engine pops events, the network answers questions. §5
specifies behaviour.

**Host.** An instance of some host kind, one per peer in the scenario. The
host kind for the MVP is the production SWIM state machine wrapped in a
thin adapter. The host trait — what the engine calls and what the host
returns — is §6.

**Bundle writer.** The only filesystem-touching component. Receives event
and snapshot records from the engine, serializes them to the production
diagnostics schema, writes them to a bundle directory. §9 specifies output.

**Assertion evaluator.** Post-run reader of the bundle. Evaluates each
declared assertion against the event stream and snapshot directory. Emits a
verdict file. §10 specifies behaviour.

### 3.2 Data flow

Each boundary is named below with the data that crosses it.

**scenario.toml → scenario loader.** A TOML file. §8 names the schema.

**scenario loader → engine.** A validated parsed scenario value. The fields
are exactly those §8 names; the engine consumes nothing else from outside.

**engine ⇄ network.** Two query methods, no others.

- `send(from, to, byte_len, sent_at_ns) → SendOutcome`. `SendOutcome` is
  either `Arrive { at_ns }` or `Drop { reason }`. The network mutates its
  per-link state (last-send-time, warm/cold) as it answers.
- `apply_mutation(mutation, at_ns) → Vec<InvalidatedDelivery>`. The network
  updates its internal state and returns a list of currently-scheduled
  deliveries the mutation invalidates. The engine removes those from its
  queue.

**engine ⇄ host.** Three call sites, no others. The host responds to each
with `Vec<Action>`; the engine processes actions in returned order.

- `tick(now_ns)` — fired at the host's tick instants.
- `recv(message, now_ns)` — fired when a delivery event for this host pops.
- `snapshot() → SnapshotBytes` — fired on snapshot dispatch.

`Action` is a closed enum the engine handles exhaustively:

- `Send { to: HostId, message: HostMessage }` — engine asks the codec for
  the message's byte length, asks the network for arrival time, schedules a
  `Deliver` event or notifies the sender of `SendFailed` (next bullet).
- `RecordEvent { event: EventBytes }` — engine forwards to the bundle
  writer with the current virtual time.
- `ScheduleTimer { at_ns, token: TimerToken }` — engine schedules a
  `TimerFired` event delivered through `recv` at `at_ns`.
- `Halt` — engine stops issuing further ticks to this host. Inbound recv
  still flows (so the host can observe drains) until the host's `recv`
  itself returns `Halt`.

The engine never invents actions; the host produces them. The engine never
silently drops actions; an unknown variant aborts the run.

**engine → bundle writer.** A single method, `write_record(record)`. The
record is one of:

- `EventRecord { virtual_time_ns, host_id, kind_tag, event_bytes }` —
  produced by host `RecordEvent` actions and by engine-synthesized events
  (drops, cold-dial penalty firings, cache-invalidate firings, send-failure
  notifications).
- `SnapshotRecord { virtual_time_ns, host_id, snapshot_bytes }` — produced
  by snapshot dispatch.
- `MutationRecord { virtual_time_ns, mutation }` — produced when a mutation
  pops.

The bundle writer is append-only and accepts records in arbitrary order.
It is the writer's responsibility to organize records into the bundle
layout in §9.

**bundle → assertion evaluator.** The evaluator reads the finished bundle
from disk. Its input is the bundle path; its output is a `verdicts.json`
written into the same bundle. §10 names the verdict format.

### 3.3 Host kinds and the codec contract

A host kind is a `(host_trait_impl, codec)` pair. The codec exists because
the bandwidth model needs to know how many bytes a host's outgoing message
will occupy on the wire, and that number must equal what the production
transport would put on the wire for the same message, or the simulator's
bandwidth-driven failure modes diverge from the live ones.

For each host kind, the codec exposes:

- `encode(message) → bytes`.
- `decode(bytes) → message`.
- `kind_tag() → str`.

A contract test asserts byte-equality between the sim's encode path and the
production transport's encode path for a representative message set;
divergence breaks the build.

For the MVP, the SWIM host kind reuses the production transport's encoding
logic directly. If production code is too tangled with iroh to import
cleanly, the encoding is factored into a small shared module that both the
production transport and the sim host call; that refactor is part of the
MVP, not deferred.

### 3.4 Independent buildability

Each component in §3.1 can be built by an independent agent against the
contracts in §3.2 and §3.3 alone. Specifically:

- The scenario loader is built against §8.
- The network is built against §5 and the substream rule in §7.
- The engine is built against §4 plus the network and host call signatures.
- The SWIM host is built against §6 and the production SWIM API.
- The bundle writer is built against §9.
- The assertion evaluator is built against §10.

A change to a component's *internal* structure is invisible to the others.
A change to a contract in §3.2 / §3.3 is a spec amendment.

---

## 4. The engine

The engine owns the virtual clock, the scheduling queue, the host table,
the network reference, and the bundle-writer reference.

### 4.1 The scheduling queue

The queue is a priority queue over `(virtual_time_ns, sequence_number)`
keys. The sequence number is assigned monotonically on enqueue; it is the
only tie-break mechanism the engine permits. Two events at the same virtual
time pop in enqueue order.

Each entry carries one of:

- `Tick { host_id }`.
- `Deliver { host_id, message_bytes }`.
- `TimerFired { host_id, token }`.
- `Mutation { mutation }`.
- `Snapshot`.
- `Terminate`.

Pre-population: at engine start, one `Tick` is enqueued per host at the
host's first-tick virtual time, one `Mutation` per scenario mutation, one
`Snapshot` per scenario snapshot request, and one `Terminate` at
`duration_ns`.

### 4.2 The main loop

Pop the smallest entry. Advance the virtual clock to its time. Dispatch by
kind (§4.3 through §4.7). Repeat until `Terminate` pops or the
early-termination condition fires (§4.8). After termination, finalize the
bundle and run the assertion evaluator.

The clock advances *only* on pop. Nothing in the engine reads any other
clock, virtual or real.

### 4.3 Tick dispatch

For `Tick { host_id }`:

1. Call `host[host_id].tick(now_ns)`. If the host has `Halt`-ed, skip the
   call but still schedule the next tick (the host may un-halt only via a
   `PeerResurrect` mutation).
2. Process the returned action list in order (§4.6).
3. Enqueue the next `Tick { host_id }` at
   `now_ns + tick_period_ns[host_id]`.

The first tick for each host is offset by a stable, seed-derived per-host
offset. The offset is drawn from the host's RNG substream (§7) so peers do
not tick on the same virtual instants and silent symmetry artefacts do not
mask real timing bugs.

### 4.4 Delivery dispatch

For `Deliver { host_id, message_bytes }`:

1. If `host[host_id]` is halted or killed, drop the delivery and emit a
   `DropOnDelivery` engine-synthesized event.
2. Otherwise, decode the bytes via the host kind's codec.
3. Call `host[host_id].recv(message, now_ns)`.
4. Process the returned action list in order (§4.6).

### 4.5 Other dispatches

- `TimerFired { host_id, token }` is delivered through `recv` with a
  `TimerFired(token)` envelope and processed identically to a network
  delivery.
- `Mutation { mutation }` is forwarded to `network.apply_mutation`. The
  returned list of invalidated deliveries is removed from the queue (or
  tombstoned — the visible behaviour is identical). The engine emits a
  `MutationRecord` to the bundle writer.
- `Snapshot` asks every live host for its `snapshot()` and forwards each
  result to the bundle writer as a `SnapshotRecord`.
- `Terminate` ends the main loop.

### 4.6 Action processing

For each action returned by a host:

- `Send { to, message }` — encode via the codec, ask the network with the
  resulting byte length. On `Arrive`, enqueue a `Deliver` at the returned
  time. On `Drop`, emit a `DropOnSend` engine-synthesized event *and*
  deliver a `SendFailed` envelope to the sender via the same recv path the
  production transport would.
- `RecordEvent { event }` — forward to the bundle writer with current
  virtual time.
- `ScheduleTimer { at_ns, token }` — enqueue `TimerFired` at `at_ns`.
- `Halt` — mark the host halted (§4.3 covers re-entry).

### 4.7 Engine-synthesized events

The engine emits records for behaviours hosts do not see directly:

- `DropOnSend { from, to, reason }` — a send the network refused.
- `DropOnDelivery { to, reason }` — a delivery the engine refused at
  arrival time (host killed mid-flight).
- `CacheStateChange { from, to, transition }` — the link warmed, the link
  went cold by idle, the link was invalidated by a mutation. The network
  surfaces these to the engine through a side-channel on `send` and
  `apply_mutation`.
- `DialStart` / `DialOutcome` — emitted whenever the cold-dial penalty
  fires on a send.

These records exist so the bundle's observable surface matches what
production diagnostics emit for the same activity. The engine never
suppresses them and never emits them for activity that did not happen.

### 4.8 Early termination

A scenario may set `early_terminate_on_all_assertions_resolved = true`. If
set, after each event dispatch the engine polls the assertion evaluator's
streaming side (§10.4); if every declared assertion has a resolved verdict,
the engine fast-forwards to `Terminate`. The bundle still records every
event that fired up to that point.

### 4.9 Time unit

The engine's virtual clock is in integer nanoseconds. The manifest records
the unit so post-processors render times consistently. Sub-nanosecond
ordering is not modelled.

### 4.10 Behavioral tests

The engine's contract is the dispatch and action-processing behaviour of
§4. Its tests assert that it has the properties below; how each is
verified is the test author's call.

**Tick cadence.** Each host receives ticks at its declared period,
starting at a stable seed-derived offset. The offset is identical
across runs and differs across hosts in the same scenario.

**Action ordering.** A host's emitted actions are processed in returned
order. The effects of action N are fully observable before action N+1's
effects begin.

**Send semantics.** A `Send` whose network query returns `Arrive` results
in the recipient's `recv` being called at the returned arrival time with
the codec-produced bytes. A `Send` the network drops results in the
sender's `recv` being called with `SendFailed`, a `DropOnSend` record in
the bundle, and no recipient call.

**Timer fidelity.** A `ScheduleTimer { at_ns, token }` causes a
`TimerFired(token)` envelope to reach the host's `recv` at exactly
`at_ns`.

**Halt.** A halted host receives no further ticks; it continues to
receive deliveries.

**Closed action set.** An action outside the closed set §4.6 names
aborts the run with a structured error. The engine never silently
ignores or invents an action.

**Tie-break.** Events scheduled at the same virtual time pop in enqueue
order. The order is identical across runs and architectures.

**Mutation propagation.** Deliveries the network invalidates are removed
from the engine's queue; each emits a `DropOnDelivery` record at the
mutation's virtual time. No invalidated delivery reaches a host's `recv`.

**Snapshot fanout.** A scheduled snapshot produces exactly one record
per live host at the scheduled virtual time.

**Early termination is clean.** Every record emitted before the
termination time is preserved; no record carries a later virtual time.

**Determinism.** Same scenario, same seed ⇒ byte-identical
`events.ndjson`.

---

## 5. The network

### 5.1 Topology

A directed graph. Vertices are the host IDs declared in the scenario.
Edges carry link policies. An ordered pair with no declared edge is
permanently partitioned; the network returns `Drop(NoRoute)` for any send
on it. This is distinct from a temporary partition mutation, which can heal.

Asymmetry is allowed and intended: `policy(A→B)` and `policy(B→A)` are
independent.

### 5.2 Link policy

Each edge carries the following integer-valued fields:

- `latency_ns` — base one-way delivery time.
- `jitter_stddev_ns` — symmetric jitter; samples are drawn from a
  precomputed integer lookup table approximating a standard-normal
  distribution scaled by this stddev (§7 forbids float math in decisions).
- `loss_prob_ppm` — independent drop probability per send, parts-per-
  million.
- `reorder_prob_ppm` — probability of inserting extra delay sufficient to
  swap delivery order with the next message on the same edge.
- `bandwidth_bps` — bytes per second. A message of N bytes occupies the
  link for `(N * 1_000_000_000) / bandwidth_bps` ns.
- `cold_dial_penalty_ns` — extra latency added when the link is cold.
- `cache_warm_after_ns` — wall of warm-time after first contact before
  subsequent sends are warm.
- `cache_invalidate_after_idle_ns` — idle duration after which the link
  returns to cold.

### 5.3 Per-link state

Each edge tracks:

- `last_send_ns` — virtual time of the last `send` that returned `Arrive`.
- `last_arrive_ns` — virtual time of the latest scheduled arrival; used by
  the bandwidth model for the next message's serialization start.
- `cache_state` — `Cold`, `Warming(since_ns)`, or `Warm`.

State transitions happen inside `send` and `apply_mutation`.

### 5.4 The send algorithm

`send(from, to, byte_len, sent_at_ns) → SendOutcome`. Steps:

1. If the edge does not exist, return `Drop(NoRoute)`.
2. If the active partition set cuts `(from, to)`, return `Drop(Partitioned)`.
3. If a `LossBurst` mutation is active for this edge, use its override
   probability; otherwise use the edge's `loss_prob_ppm`. Draw a u32 from
   the edge's RNG substream; if `draw % 1_000_000 < prob_ppm`, return
   `Drop(Lossy)`.
4. Compute `serialization_start = max(sent_at_ns, last_arrive_ns)`. Compute
   `serialization_end = serialization_start + (byte_len * 1e9 / bandwidth_bps)`.
5. Compute `arrival = serialization_end + latency_ns + jitter_sample`,
   where `jitter_sample` is one draw from the per-link substream into the
   integer-Gaussian table, scaled by `jitter_stddev_ns`.
6. If a `LatencySpike` mutation is active, multiply the additive latency
   contribution (latency + jitter) by the spike factor before adding.
7. If a `RelayBuffer` mutation is active, take `arrival =
   max(arrival, sent_at_ns + floor_ns)`.
8. If `cache_state` is `Cold`, add `cold_dial_penalty_ns` to `arrival` and
   transition `cache_state` to `Warming(now)`. Emit a `DialStart` side-
   channel notification to the engine and a `DialOutcome` at the arrival
   time.
9. If the reorder draw fires, add enough delay so this message arrives
   after the next message scheduled on this edge.
10. Update `last_send_ns = sent_at_ns`, `last_arrive_ns = arrival`. If
    `cache_state` is `Warming(since)` and `now - since >= cache_warm_after_ns`,
    transition to `Warm` and emit a `CacheStateChange`.
11. Return `Arrive(arrival)`.

If `last_send_ns - now > cache_invalidate_after_idle_ns` at the start of a
send, the link returns to `Cold` and emits a `CacheStateChange` before
proceeding.

### 5.5 Mutations

Supported kinds:

- `Partition { peers_a, peers_b }` — set the active partition to cut every
  edge between the two groups in both directions. In-flight messages on
  cut edges are returned in the invalidated-deliveries list.
- `Heal` — clear the active partition set. No in-flight invalidations.
- `LatencySpike { links, factor_x100, duration_ns }` — multiply additive
  latency on named links by `factor_x100 / 100` for a duration. In-flight
  messages are not retroactively delayed.
- `LossBurst { links, prob_ppm, duration_ns }` — override loss probability
  on named links for a duration.
- `RelayBuffer { links, floor_ns, duration_ns }` — impose a minimum
  delivery delay on named links for a duration.
- `PeerKill { peer }` — drop the peer's inbox. In-flight deliveries to the
  peer are invalidated. The peer's ticks are stopped by the engine.
- `PeerResurrect { peer, preserve_state }` — restart the peer. If
  `preserve_state`, the engine reuses the host instance; otherwise a fresh
  host of the same kind is instantiated from the scenario's peer
  declaration.

### 5.6 Determinism within the network

Every random draw the network makes comes from a substream keyed by
`("link", from_id, to_id)` (§7). Editing one link's policy must not
perturb the draws on any other link.

### 5.7 Out of scope for the network model

The network does not model MTU, fragmentation, congestion control, TCP-
style backpressure, NAT state, or inter-peer clock skew. These limits are
named in the known-gaps document and re-entered when an algorithm under
test is sensitive to them.

### 5.8 Behavioral tests

The network is a pure function of (state, query). Its tests assert that
it has the properties §5 names; how each property is verified is the
test author's call.

**Reachability.** A `send` over an ordered pair returns `Arrive` iff
that pair is declared as an edge and the active partition set does not
cut it. A `send` the network refuses leaves the network's state
unchanged.

**Partition heals to identity.** A `Partition` followed by a `Heal` at
later virtual times leaves the network indistinguishable on subsequent
sends from one that experienced neither.

**Mutation invalidation is exact.** Every delivery a mutation renders
impossible appears in the mutation's invalidated-deliveries return. No
delivery the mutation does not invalidate appears in that return.

**Loss is Bernoulli.** Drops on a link are independent draws with the
link's declared probability. A `LossBurst` substitutes its override
probability for the duration it names and only the duration it names.

**Bandwidth serializes.** A link with finite bandwidth never overlaps
two messages' wire-occupancy intervals: each message's arrival is
delayed at least until the previous message's arrival plus that
message's transmission time.

**Latency is additive.** A send's arrival decomposes into base latency,
serialization delay, jitter, cold-dial penalty when applicable, and the
contributions of active mutations. The terms are independent in the
policy and combine without interaction beyond what §5.4 specifies.

**Jitter is symmetric and integer-valued.** Jitter samples come from
the precomputed table of §7, are symmetric around zero, and are never
non-integer.

**Cache state follows traffic.** The link's `cache_state` reflects
recent traffic: warm after sufficient activity, cold again after
sufficient idleness, with the thresholds the policy names. The
cold-dial penalty is paid by exactly the sends the network classifies
cold.

**Cache transitions are observable.** Every cold↔warm transition emits
exactly one `CacheStateChange` notification at the transition's virtual
time. No transition is silent and no notification fires without a
transition.

**Mutation scoping.** A mutation affects exactly the links its `links`
field names and exactly the duration it declares. Sends on other links,
or on the named links outside the duration, are unaffected.

**Substream isolation.** Editing one link's policy does not change any
draw the network makes on any other link. This is the property that
makes bisecting scenario edits possible.

**Determinism.** Same topology, same seed, same query sequence ⇒
identical `SendOutcome` sequence and identical invalidated-deliveries
returns.

---

## 6. Hosting an entity

### 6.1 The host trait

A host kind implements:

- `fn id(&self) -> HostId`.
- `fn kind_tag() -> &'static str`. Used for routing and bundle tagging.
- `fn tick(&mut self, now_ns: u64) -> Vec<Action>`.
- `fn recv(&mut self, message: HostMessage, now_ns: u64) -> Vec<Action>`.
- `fn snapshot(&self) -> SnapshotBytes`.

`HostMessage` is either a decoded inbound application message (the host
kind's own type, dispatched via the codec) or a `TimerFired(token)` or a
`SendFailed { to, reason }` envelope.

A host kind also exposes:

- `fn new_from_config(id: HostId, config: HostKindConfig, rng: HostRng) -> Self`.
- A codec (§3.3).
- A validation routine for its `HostKindConfig` (used by the scenario
  loader; §8).

### 6.2 The SWIM host kind

The SWIM host wraps the production SWIM state machine without re-
implementing it. It:

- Constructs the production state machine with the scenario's per-peer
  config.
- Installs the production diagnostics emitter against a shim that pushes
  every emission into a per-tick / per-recv `Vec<Action>` as
  `RecordEvent` actions.
- Installs the production tier-2 introspector against the host's
  `snapshot()` method so the snapshot bytes are exactly what production
  emits.
- Dispatches `recv` to the production handler matching the message kind
  (ping, ack, ping-request, indirect-ack, join-request, join-response).
- Translates production state-machine output (outgoing messages, timer
  requests) into `Send` and `ScheduleTimer` actions.

When the production state machine emits an output the host adapter does
not know how to route — a new message kind in a future SWIM version, for
example — the adapter panics. Silent fallback is exactly the class of bug
the simulator is meant to prevent.

The adapter does *not* substitute for any production logic. Its job is
purely translation between production data types and the host trait.

### 6.3 Adding a new host kind

Adding a new host kind is a strict superset operation:

1. Implement the host trait against the new algorithm.
2. Provide a codec.
3. Add a `kind` arm to the scenario loader's peer-declaration parser.
4. Register the kind with the engine's host-instantiation factory.

Existing host kinds continue to work without change. The network, the
bundle writer, the engine main loop, and the determinism contract are
host-kind-agnostic.

### 6.4 Behavioral tests

Host-kind tests come in two layers: kind-agnostic properties every
registered kind must satisfy, and per-kind properties specific to the
algorithm a kind hosts. The list below is what the tests must assert;
how is the test author's call.

**Trait conformance (every kind).** The kind exposes the §6.1 surface
with the §6.1 signatures. `kind_tag()` is a non-empty string unique
among registered kinds.

**Codec is invertible (every kind).** Encoding then decoding a message
is the identity on the kind's message type.

**Host determinism (every kind).** Same `HostKindConfig`, same RNG
seed, same `tick`/`recv` sequence ⇒ identical action sequence.

**SWIM emits no novel kinds.** Every event a SWIM host emits is of a
kind production's diagnostics also emits. The simulator invents no SWIM
event kind for itself.

**SWIM codec parity with production.** The SWIM host's encoding of an
outgoing message is byte-identical to the production transport's
encoding of the same message. Drift breaks the build.

**SWIM snapshot parity with production.** A SWIM host's `snapshot()`
conforms to the production tier-2 SWIM-state schema.

**SWIM unknown-output is loud.** A production state-machine output the
SWIM adapter does not route aborts the run with a structured error.
Silent fallback is a test failure.

---

## 7. Determinism

This section is normative. A violation is a ship-blocker.

### 7.1 Forbidden inputs

No part of the simulator reads any of: host wall clock, host monotonic
clock, host process or thread ID, host hostname, environment variables
outside a documented sim-internal prefix, `/dev/urandom` or any host RNG
source, network interface state, filesystem state outside the bundle
output path.

The repo's existing lint scanner catches the static cases. Drift is
caught by the cross-architecture parity test (§7.6).

### 7.2 The randomness tree

All randomness derives from one root random stream seeded by the
scenario's `seed` field. Substreams are derived by hashing a fixed,
documented tuple with a constant-key siphash:

- `("link", from_id, to_id)` — per-edge substream used by the network.
- `("host", host_id, label)` — per-host substream used for tick offsets
  and any RNG the host kind needs.
- `("mutation", index)` — per-mutation substream if a mutation needs
  randomness (none currently do).

The hash function and the key are fixed in code. Substreams are stable
across runs and across host architectures.

Substream layout matters: editing one link's policy must not perturb the
draws on any other link, or every test edit becomes a new random universe
and bisection is impossible.

### 7.3 No hash-randomized iteration

Wherever any component iterates a collection, the order is determined by
the natural key order (host IDs sort lexicographically; pairs sort
lexicographically on the pair) or by an insertion-order-preserving
structure. Point lookups into hash maps remain allowed; iteration is the
divergence source.

### 7.4 No floating point in decisions

Latencies are integer nanoseconds. Probabilities are integer parts-per-
million of a fixed denominator. Jitter samples come from a precomputed
integer lookup table approximating a standard-normal distribution; the
table is checked in. Bandwidth math uses integer arithmetic with explicit
scaling; the precise formula is in §5.4.

Floating point is permitted in post-hoc calibration tools that read a
bundle. It is forbidden in the engine, the network, the host adapter, the
bundle writer, and the assertion evaluator's verdict computation.

### 7.5 No work outside the scheduler

No background thread, no async runtime, no timer that fires without the
engine's knowledge. Every effect is the consequence of a popped event.

### 7.6 The cross-architecture parity test

The test suite contains a reference scenario whose bundle output has a
known checksum, checked in. The test runs the simulator on the reference
scenario on every supported architecture and compares the checksum to the
stored value. A mismatch is either a deliberate spec amendment (with
justification) or a bug.

### 7.7 Scope of determinism

The contracts in §4.10, §6.4, and §7.3 bind the simulator's own
components — the §3.1 list. Production code that a host adapter wraps
(the SWIM host adapter wraps `crates/distribution/src/swim/`) is a
dependency, not a §3.1 component. Hidden state inside a wrapped
dependency — allocator state, `std::collections::HashMap` `RandomState`,
any process-local entropy that does not flow through the §7.2
randomness tree — is out of scope. The simulator does not promise to
fix, mirror, or compensate for non-determinism inside a wrapped
dependency.

"Same RNG seed" in §6.4 is the simulator-controlled seed handed to the
host adapter via the §7.2 randomness tree. "Same seed" in §4.10 is the
scenario seed. Neither extends to hidden entropy held inside a wrapped
dependency.

A property test that compares two independently-allocated instances of
a wrapped state machine and asserts identical behaviour is testing the
dependency, not the simulator. The spec does not require it. Do not
write such a test; if one exists, delete it rather than propose a §15
amendment to relax a contract that, on this reading, the spec is not
making.

---

## 8. The scenario format

A scenario is a single TOML file. The file is the simulator's only input
and is byte-for-byte sufficient to reproduce any run. The format is TOML
because the repo already uses it; the choice is not load-bearing.

### 8.1 Schema

Top-level fields:

- `name: String`.
- `seed: u64`.
- `duration_ns: u64`.
- `early_terminate_on_all_assertions_resolved: bool` (default `false`).

A `[default_tick]` table:

- `period_ns: u64`.

A `[default_link]` table containing every field §5.2 names; per-edge
overrides under `[[links]]` may override any subset.

A `[[peers]]` array, each entry:

- `id: String`.
- `kind: String` — selects the host kind.
- `kind_config: { ... }` — host-kind-specific opaque table.
- `initial_state: String` — host-kind-specific.
- `tick_period_ns_override: u64` (optional).

A `[[links]]` array, each entry:

- `from: String`.
- `to: String`.
- Any subset of the §5.2 fields (overrides on top of `[default_link]`).

A `[[mutations]]` array, each entry:

- `at_ns: u64`.
- `kind: String` — one of the §5.5 variants.
- Variant-specific parameters.

A `[[snapshots]]` array, each entry:

- `at_ns: u64`.

A `[[assertions]]` array, each entry:

- `kind: String` — one of the §10.1 variants.
- Variant-specific parameters.

A `[base]` table with a single optional `extends: String` pointing to
another scenario file; the merge is deep, with child entries overriding
parent at the leaf.

### 8.2 Validation

The loader rejects:

- Duplicate peer IDs.
- Link, mutation, snapshot, or assertion references to undeclared peers.
- `duration_ns < max(mutation.at_ns)` or similar for snapshots /
  assertions.
- Any host-kind config that fails the host kind's own validation routine.
  For the SWIM kind, this includes `probe_interval < suspicion_timeout`.
- A `default_link` field that is non-integer, negative, or in a unit other
  than the §5.2 names (e.g., `latency_ms` is rejected; only `latency_ns`).

Validation failures produce a structured error with the file path, the
offending field, and a one-line explanation.

### 8.3 Library structure

Scenarios live under a `scenarios/` directory next to the simulator, in
four subdirectories:

- `smoke/` — happy-path scenarios. Three-node mesh no impairments;
  eight-node ring; chain; star. Each asserts continuous Alive.
- `reproduction/` — known live failures. Each is expected to fail until
  its cause is fixed.
- `topology/` — partition-and-heal, rolling restart, peer churn. No
  specific bug targeted.
- `calibration/` — paired with a captured production bundle. §11.

Every scenario carries a top-of-file prose comment naming what it
reproduces, the expected verdict (pass-now / fail-until-fix /
sensitivity-study), and any base scenario it extends.

### 8.4 Behavioral tests

The loader's contract is the schema and validation rules of §8. Its
tests assert that it has the properties below.

**Examples are well-formed.** Every shipped example scenario parses
and satisfies every rule in §8.2.

**Parse is invertible.** Parsing, re-emitting to TOML, and re-parsing
is the identity on scenario values.

**Validation is complete.** Every rule §8.2 names is enforced. A
scenario violating any rule is rejected; a scenario violating none is
accepted.

**Errors are structured.** A rejection names the file, the offending
field, and the violated rule in one line each. Generic errors are a
test failure.

**Host-kind validation is delegated.** A host-kind-config error
surfaces the kind's own rule, not a loader-generic one.

**Merge is leaves-override, lists-append.** Extending a base scenario
replaces leaf values and appends list entries, with no other effect.

**Loading is pure.** Loading the same file twice produces equal values
and performs no filesystem writes.

---

## 9. The bundle

### 9.1 Layout

A bundle is a directory laid out as follows:

```
<bundle_root>/
  manifest.json
  scenario.toml          # echo of the scenario that produced the run
  events.ndjson          # newline-delimited JSON event stream
  snapshots/
    <host_id>/
      <snapshot_seq>.json
  verdicts.json          # produced by the assertion evaluator (§10)
  known_gaps.md          # static copy of the known-gaps document
```

### 9.2 The event stream

Each line is one JSON object with the envelope:

```
{
  "virtual_time_ns": <u64>,
  "host_id": <string or null>,        # null for engine-synth events not bound to a host
  "kind_tag": <string>,               # "swim", "engine", etc.
  "event": <event-payload>
}
```

The event payload schema is exactly the production diagnostics schema for
that event kind. The simulator must not invent new event kinds; an event
the simulator emits is one production also emits.

Event kinds the MVP emits:

- SWIM state transitions, message-send and receive accounting, probe
  lifecycle (sent / received / timed out), self-incarnation bumps.
- Engine-synthesized cache state changes, dial start and outcome, drop on
  send, drop on delivery, send-failure errors.
- Mutation records.

Event kinds belonging to subsystems out of scope (iroh internals, node-
map updates, kademlia operations) are not emitted and are listed in
`known_gaps.md`. The schema-diff tool ignores them on the production side
when comparing.

### 9.3 Snapshots

A snapshot is exactly the production tier-2 SWIM-state JSON for that host
at that virtual time. The schema is unchanged from production.

### 9.4 The manifest

`manifest.json` records:

- Simulator version (commit hash).
- Path and SHA-256 of the scenario file.
- Seed.
- Duration in ns.
- Host architecture the run executed on.
- SHA-256 of `events.ndjson`.
- SHA-256 of each snapshot file, keyed by relative path.

Wall-time fields are derived from the virtual clock; the manifest declares
the unit so post-processors do not confuse virtual time with real time.

### 9.5 Renderability

The bundle is renderable by the same post-processor production uses. If
the renderer requires inputs the simulator does not have (collector-side
receive timestamps, for instance), the simulator substitutes the virtual-
clock equivalent and records the substitution in the manifest.

### 9.6 Behavioral tests

The writer's contract is the layout and schema of §9. Its tests assert
that it has the properties below.

**Layout.** Every produced bundle has the §9.1 entries. (`verdicts.json`
is the assertion evaluator's responsibility; §10.5.)

**Envelope conformance.** Every line of `events.ndjson` is valid JSON
conforming to the §9.2 envelope shape.

**Hash integrity.** Every manifest-recorded hash equals the actual hash
of the file it names.

**Snapshot organization.** Each `SnapshotRecord` corresponds to exactly
one file at `snapshots/<host_id>/<snapshot_seq>.json`. Sequence numbers
are monotonically increasing per host from zero.

**Ordering is deterministic and documented.** Identical record streams
produce byte-identical bundles. The ordering rule is named in §9 and the
writer obeys it.

**Arrival-order independence.** Records may arrive in any order; the
produced bundle depends only on the multiset of records and the
documented ordering rule, not on arrival order.

**Idempotency.** Writing the same record stream to a fresh output path
twice produces byte-identical bundles.

---

## 10. Assertions

### 10.1 The assertion catalog

Each assertion kind has a name and a parameter shape. The MVP catalog:

- `all_alive_at { at_ns, peers }` — at the given virtual time, every named
  peer's view of every other named peer is Alive.
- `all_alive_throughout { window_start_ns, window_end_ns, peers }` — the
  above, continuously, across a window.
- `convergence_after { after_ns, within_ns, peers }` — after the named
  time, the cluster reaches a consistent membership view within the
  bounded duration.
- `no_flap_while_probes_ok { peer, window_start_ns, window_end_ns }` — no
  peer transitions Suspect → Alive → Suspect within a window in which the
  peer's bidirectional probes are succeeding.
- `no_dead_when_probes_ok { peer, window_start_ns, window_end_ns }` — the
  peer is never marked Dead in any other peer's view while bidirectional
  probes are succeeding.
- `self_incarnation_bounded { peer, max_value }` — the peer's self-
  incarnation counter never exceeds the bound.
- `message_size_bounded { kind, max_bytes }` — no sent message of the
  named kind exceeds the byte threshold.
- `dead_peer_resurrects_within { peer, after_ns, within_ns }` — after the
  peer becomes reachable again, the cluster marks it Alive within a
  duration.
- `event_count { kind, max }` — bounds the absolute count of an event
  kind across the run.
- `event_rate { kind, window_ns, max_per_window }` — bounds the rate of
  an event kind.

Adding a kind is a deliberate amendment to this section.

### 10.2 The evaluator interface

The evaluator reads the bundle's `events.ndjson` and `snapshots/` and
evaluates each assertion. Per assertion it emits a verdict:

```
{
  "name": "<assertion-name>",
  "kind": "<assertion-kind>",
  "parameters": { ... },
  "outcome": "Pass" | "Fail" | "Inconclusive",
  "evidence": [
    { "virtual_time_ns": <u64>, "event_or_snapshot_ref": "<path>" }
  ]
}
```

`Inconclusive` is reserved for assertions whose preconditions did not
fire during the run (e.g., a peer the assertion names never became
reachable).

`verdicts.json` is an array of verdict objects, one per declared
assertion, in the order the scenario declared them.

### 10.3 Library properties

A library property is a parameterized assertion kind evaluated across a
generated distribution of scenarios. The MVP ships one such property: the
gossip-flap detector. The property generates scenarios from a declared
space (peer count, latency range, jitter range, loss range, duration) and
applies `no_flap_while_probes_ok` to each.

The framework records the random seed that produced any failing scenario
so the failure is reproducible. The current SWIM source must fail this
property. The proposed fix must pass it.

Library properties are otherwise identical to per-scenario assertions in
output shape; their verdicts go into the property runner's own output,
not into a single bundle's `verdicts.json`.

### 10.4 Streaming evaluation (for early termination)

For early termination (§4.8) the evaluator exposes a streaming side: as
events are emitted, the evaluator may resolve assertions whose verdicts
are determinable from the prefix. The engine polls this side after each
event dispatch. The streaming side is an optimization; the post-run side
remains the authoritative source for `verdicts.json`.

### 10.5 Behavioral tests

The evaluator's contract is the assertion catalog of §10.1, the verdict
shape of §10.2, and the streaming side of §10.4. Its tests assert that
it has the properties below.

**Per-kind soundness.** For every kind in §10.1: `Pass` is returned
exactly when the kind's stated condition holds over the bundle; `Fail`
exactly when the condition is violated; `Inconclusive` exactly when the
preconditions did not fire.

**Verdict shape.** Every verdict conforms to §10.2. `Fail` verdicts
carry evidence referencing the event or snapshot responsible.

**Verdict order is scenario-declared.** `verdicts.json` lists verdicts
in the order the scenario declared the corresponding assertions.

**Streaming agrees with post-run.** On any bundle, the streaming side
either does not resolve or resolves to the same verdict the post-run
side will return. The two are never inconsistent.

**Streaming resolves as early as possible.** When a verdict is
determinable from a prefix, the streaming side resolves no later than
the end of that prefix.

**Property failures replay exactly.** A library-property failure
recorded with seed S, replayed with seed S, produces the identical
scenario and the identical `Fail` verdict.

---

## 11. Calibration

Calibration measures the simulator's fidelity against captured production
bundles.

### 11.1 The corpus

The MVP corpus is the three N3 vast.ai bundles described in the
deployment report. Each pairs with a scenario in `scenarios/calibration/`
that approximates the conditions under which the bundle was produced.

### 11.2 The procedure

For each pair, the calibration tool:

1. Runs the simulator with the declared scenario.
2. Loads the corresponding production bundle.
3. Computes the comparison metrics (§11.3) on both bundles.
4. Reports per-metric pass/fail against per-metric tolerances.

### 11.3 The metrics

- Per-peer event-timespan summaries (p50, p90, p99) per event kind.
- State-transition reason distribution across all peers.
- Message-size distribution per message kind.
- Message-count per kind, per peer pair.
- Self-incarnation trajectory per peer.
- Connection-cache hit count.
- Dial-started count.
- Per-peer fraction of run time in the Alive state.

Tolerances ship as placeholders informed by intuition; the first
calibration pass against the N3 corpus sets the real numbers. A widening
of a tolerance is a documented degradation in the known-gaps document.

### 11.4 CI integration

Calibration runs on every change that touches the simulator or the SWIM
state machine. A regression — a previously-in-tolerance metric goes out
of tolerance — blocks merge. A widening of a tolerance is a separate,
justified commit.

---

## 12. Implementation phasing

Each phase ships independently. At every phase boundary the partial
simulator does something useful and is testable.

| Phase | Adds | Verifiable outcome |
|-------|------|-----|
| 1 | Scenario loader | Every shipped scenario parses; malformed input is rejected with a structured error. |
| 2 | Network (no engine, no host) | A microbenchmark queries `send` at scenario-scale rates and produces deterministic outputs. |
| 3 | Engine skeleton + a trivial echo host kind | The cross-architecture parity test passes on a reference scenario. |
| 4 | SWIM host kind + codec contract test | The smoke scenario runs to completion and asserts continuous Alive. |
| 5 | Assertion evaluator | The gossip-flap reproduction scenario fails on current SWIM, passes after the fix. |
| 6 | Bundle writer + schema-diff tool | A sim bundle renders through the production post-processor and diffs against a prod bundle reporting only known-gap events. |
| 7 | Calibration tool | At least one N3 pair passes calibration. |

MVP exit is the end of phase 7. Subsequent phases — proptest catalog
expansion, scenario library growth, the forward-compatibility work in
§13 — are post-MVP.

Phases 1, 2, 6 are independently buildable by separate agents from this
spec alone; phases 3 onwards require the prior phase as input.

---

## 13. Forward compatibility

Each MVP non-goal has a re-entry point that does not require revisiting
MVP-scope behaviour.

- **New algorithm.** Implement a new host kind (§6.3). The engine's
  dispatch, the network, the bundle, and the determinism contract are
  unchanged.
- **Real iroh / quinn.** A future host kind wraps real iroh's `Endpoint`
  around a sim-facade UDP that rides the network model; a virtual
  `tokio::time` reads from the engine's virtual clock. Existing SWIM
  hosts still work because they do not call into iroh.
- **Determinism detector as peer.** Once a host kind exists whose code
  traverses a facade, the detector becomes another host kind that probes
  the facade and emits verdict events. Until then it has nothing to probe.
- **Replay mode.** A converter reads a captured production bundle and
  emits a scenario. The engine does not change; only the converter is
  new.
- **Opaque subprocess peer.** A host kind whose tick / recv shim is a
  wrapped subprocess with syscall-level I/O virtualization.

The MVP architecture admits each of these without retracting any of the
contracts in §3 / §4 / §5 / §6 / §7.

---

## 14. Open questions

These are deliberately unanswered; they are expected to resolve during
phases 1–3.

- SWIM's reactive probe mode introduces an internal safety-sweep timer.
  Whether the engine needs a separate event kind for the safety sweep, or
  whether driving the host on its tick interval is enough, depends on
  details inside the production probe code that are easier to resolve
  once the engine skeleton exists.
- One peer corresponds to one host ID in the MVP. Production permits a
  single host to expose multiple endpoints. The MVP punts; if a
  calibration scenario needs the multi-endpoint shape, the scenario grows
  a per-peer endpoint list and the engine dispatches by endpoint.
- The starting tolerances in §11.3 are placeholders. The first
  calibration run against the N3 corpus sets the real numbers; those
  numbers replace the placeholders in a follow-up commit.
- The simulator's code lands under `crates/simulation/` after the current
  cleanup of that crate. If the cleanup renames or relocates the
  simulator, the phase plan in §12 needs a one-pass path refresh;
  nothing else in this spec depends on the path.

---

## 15. Spec change protocol

Changes that relax a contract in §3 / §4 / §5 / §6 / §7 — a widened
tolerance, a removed assertion, a relaxed determinism rule — are
behaviour-changing and require a deliberate commit whose subject names
the relaxation and whose body justifies it in prose. A tightening change
— a new assertion kind, a narrower tolerance, a more restrictive
determinism rule — can land in any commit. Adding new scenarios, new
properties, or new phases does not require special treatment.

The lint scanner, the determinism digest, the cross-architecture parity
test, the schema-diff tool, the encoding-symmetry contract test, and the
calibration tolerances are the load-bearing artefacts that enforce this
spec. If any of them is short-circuited — disabled in CI, allow-listed at
the call site, silenced with an exemption — the spec is being worked
around, and the workaround must surface in code review.

---

## 16. References

- `examples/pipeline-parallel-inference/N3_DEPLOYMENT_REPORT.md` —
  source of truth for the live failures the simulator must reproduce.
- `crates/simulation/NORTH_STAR.md` — the long-term simulator vision.
  This MVP is a strict subset and does not retract any of its claims.
- `crates/simulation/BLOCKED.md` — the staged plan this MVP supersedes
  for the immediate iteration. The deeper goals there (real quinn / iroh
  on a sim facade, detector as a peer, full distribution-crate facade
  migration) remain on the roadmap, just not gating SWIM-tuning.
- `crates/distribution/src/swim/` — the production SWIM state machine
  the SWIM host kind wraps.
- `crates/distribution/src/diagnostics/` — the schema the bundle must
  match and the renderer it must render through.
