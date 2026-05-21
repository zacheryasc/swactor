# Simulator — implementation specification

Working spec for the simulator described in [`NORTH_STAR.md`](./NORTH_STAR.md).
Read that first. The observable surface this spec commits the sim to
reproduce lives in [`OBSERVABILITY.md`](./OBSERVABILITY.md); every
obligation below cites the subsection it discharges.

## Table of contents

1. Scope and non-goals
2. Engine
3. Runtime facade
4. Network model
5. Hosted entities
6. Recording format
7. Replay
8. Observability
9. Calibration loop
10. Glossary

---

## 1. Scope and non-goals

**In scope.**

- The network the node-under-test touches: arbitrary topology, per-
  link physics (bandwidth, delay, loss, MTU), NAT and middlebox
  behavior, reachability, partitions, healing.
- The transports that run over it (iroh today; others as added).
  Transports are first-class: their internal state — connection
  cache, NAT bindings, congestion control — must be observable on
  the same schema as production. See OBSERVABILITY §3.6, §4.3, §4.6.
- The hosted peers: any node our code can run is sim-native; any
  node we cannot rebuild (third-party binary, foreign version) is
  accommodated via the opaque-binary escape hatch (§5.2).
- The shared infrastructure those peers depend on: relays,
  signaling, DNS, vast.ai-style host metadata (§4.4, §4.7,
  OBSERVABILITY §3.10).
- A deterministic engine (§2) so calibration has a stable baseline.
- A recording format identical to the production recording format
  (§6, OBSERVABILITY §3).

**Out of scope.**

- The algorithms under test. SWIM and whatever swactor hosts run as
  the same code paths in sim and prod. The sim does not stub them.
- The CPU / memory cost of running each peer's logic. We assume
  peers fit; the host-budget model (§4.8) only accounts for what is
  visible to the peer from the host's perspective.
- Anything from OBSERVABILITY §5 ("Out of scope"). The sim is not
  obligated to model kernel system calls, GPU compute internals,
  allocator behavior, or CPU performance counters. If that list
  shrinks, this scope grows accordingly.
- One-time configuration tooling. Topology specs are inputs; how
  they are authored is outside this spec.

**Non-goal: faster than wall-clock.** Speed is a nice property of
discrete-event simulation, not a requirement. The bar is *fidelity*,
measured against the calibration loop (§9). A run that completes in
half the time but diverges on a tier-2 observation is worse than a
run that takes twice as long but matches every distribution.

## 2. Engine

### 2.1 Determinism contract

Same input, same output, byte for byte. "Input" means: the topology
spec, the link policies, the peer set, the workload, and the seed.
"Output" means: every record emitted to the recording (§6), in the
same order, with the same values. Determinism extends to:

- Per-node `monotonic_seq` counters (OBSERVABILITY §3.3).
- `snapshot_id` strings (§3.4 there).
- Trace IDs once §4.2 lands (the deterministic RNG is the source).
- Any timing field driven by virtual time (§2.2).

Non-determinism that would cross this boundary — `SystemTime::now()`,
`tokio::spawn` ordering, `HashMap` iteration order seeded from the
process — is removed at the runtime-facade layer (§3) before it can
leak into peer code. The list of sources we close off lives in §2.4.

The contract is enforced by a divergence detector (§2.5). It is
*also* a calibration assumption: when a sim run and a prod run
diverge in distribution, we need to know that the sim run, at
least, is reproducible from its seed. If the sim is also
non-deterministic, calibration loses its baseline.

### 2.2 Virtual time and the event loop

The engine drives a discrete-event loop over a virtual-time
priority queue. Every action that takes time — a packet in flight,
a timer firing, a sleep — yields to the engine, which advances
virtual time to the next scheduled event and resumes the relevant
fiber.

Virtual time is the basis of every `wall_ms` field in the recording
(OBSERVABILITY §3.1, §3.3, §3.4). Sim-run `wall_ms` values are
exactly virtual-time millis; the §3.5 clock-alignment math sees
zero drift, which is the correct answer (and the parity check
should see zero drift on sim runs and bounded drift on prod runs).

Peer code does not observe virtual time directly. It calls the
runtime facade (§3), whose sim implementation hooks into the event
loop. From peer code's perspective, it ran in time — the time was
just controlled by the engine rather than the kernel.

### 2.3 Scheduler and executor

The engine owns a single-threaded executor that drives every fiber
in the run. Fibers correspond to peers, transport tasks, SWIM
ticks, snapshot timers, etc. The executor's pick order is
deterministic (sorted by virtual-time priority, then by fiber-id
tiebreaker).

When two events are scheduled at the same virtual tick, the
tiebreaker is **(node_id, fiber_id, event_seq)**. Same input ⇒
same tiebreaker resolution. No part of the executor ever falls
back to wall-clock ordering or thread-local randomness.

The OBSERVABILITY §4.5 actor-mailbox surface flows out of the
executor: per-actor mailbox depth and per-task age are the
executor's own state, exposed to the recording on every snapshot.

### 2.4 Sources of non-determinism

Each source below has a defined sim-side handling. Anything not on
this list that leaks non-determinism is a bug.

| Source | Sim handling |
|---|---|
| `SystemTime::now` | Facade-routed to virtual time. |
| `std::time::Instant` | Facade-routed to virtual time. |
| `tokio::spawn` ordering | Executor (§2.3) imposes deterministic order. |
| `HashMap` / `HashSet` iteration | Replaced with deterministic-iteration containers behind the facade. |
| Channel select races | Engine resolves via fiber-id tiebreaker. |
| OS UDP / TCP socket APIs | Routed to the simulated network (§4). |
| DNS resolution | Routed to the simulated DNS (§4.4). |
| `getrandom` / `rand::thread_rng` | Per-node deterministic RNG seeded from `(run_seed, node_id, stream_label)`. |
| File-system access | Per-node sandbox rooted at `{run_dir}/{node_id}/`; reads of system files (`/proc/net/udp`, `/etc/resolv.conf`) routed to the simulated host model (§4). |
| Process spawn | Sim-native: a new fiber, not a new OS process. Opaque-binary case is §5.2. |
| `std::env::var` | Routed to per-node env table from the topology spec. |
| Iterator orderings that depend on `Box<dyn Trait>` vtable addresses | Avoided at the facade boundary; any helper that returns trait objects has a defined-order wrapper. |

### 2.5 Replay and divergence detection

Running the engine with the same `(spec, seed)` twice produces
identical records (§2.1). Running it with the same `(spec, seed)`
and a *different* peer-code version produces records that diverge
at the first observable difference; the engine detects this by
hashing each emitted record's bytes and comparing against the
stored hash chain (if a baseline run is supplied) or storing one
(if a baseline is not). Divergence is a structured `Error` event
plus a non-zero exit; the bundle is preserved for inspection.

Divergence detection is the engine-level mechanism. The
*calibration*-level mechanism (sim vs. prod) is §9; the two are
distinct because sim-vs-prod is statistical, while sim-vs-sim is
exact.

## 3. Runtime facade

### 3.1 Surface

The facade is the narrow trait family that peer code calls instead
of the raw `std::*` / `tokio::*` / OS APIs. Its surface is
*exactly* what the peer code needs and nothing more. Concretely:

- A clock (`now`, `sleep_until`, `interval`).
- An UDP socket (`bind`, `send_to`, `recv_from`).
- A QUIC endpoint (iroh's `Endpoint` shape; not the OS UDP).
- A DNS resolver (`resolve_a`, `resolve_aaaa`).
- A spawn primitive (`spawn`, `spawn_local`).
- A randomness source (`get_rng()` returning a streamed RNG).
- A file-system handle (`open_read`, `open_write`) scoped to the
  per-node sandbox.
- An env-var read.
- A process metadata read (`hostname`, `pid`).

Anything the recording observes (OBSERVABILITY §3) is observable on
the facade — either because the facade *emits* the corresponding
event itself (e.g., `MessageSent`), or because the subsystem
calling the facade emits it.

### 3.2 Prod implementation

Each method delegates to the obvious real backend: tokio runtime,
std net stack, libc DNS, etc. The implementation lives in the
existing crates (`crates/distribution` and friends). Production
code paths never see a sim-aware branch — the swap is at the
facade-trait boundary, not at every call site.

### 3.3 Sim implementation

Each method delegates to the engine. `now` reads virtual time;
`sleep_until` schedules a wake-up event; UDP send hands a packet
to the network model; DNS reads the simulated zone; `spawn`
registers a fiber with the executor; `get_rng` returns a stream
keyed by `(node_id, stream_label)`.

The sim implementation owns the runtime facade for *all* sim-native
hosts in the run, in one OS process. Opaque-binary hosts (§5.2)
get their own runtime facade implementation that virtualizes the
boundary at the syscall level instead.

### 3.4 Build-time switching

The active implementation is chosen by a single Cargo feature. No
runtime branching, no `if cfg!(sim)` sprinkled through peer code.
Two binaries are built from the same source: a prod binary linked
against the prod facade, and a sim driver that links the sim
facade and exposes the topology-spec entry point. Peer code does
not need a `Cargo.toml` change to be used in the sim — being
written against the facade trait is sufficient.

## 4. Network model

The network is what peer code talks through. Its job is to be
*indistinguishable* from a real network on every channel the
recording observes (OBSERVABILITY §3.2, §3.3, §3.6, §3.7, §4.1).

### 4.1 Topology

A topology is a directed graph of nodes and links plus a set of
shared-infrastructure entities (relays, DNS zones, vast.ai-style
host metadata sources). Topologies are declarative — they are an
input to the engine, not assembled imperatively at run time.

Graphs are arbitrary. The sim does not enumerate named topologies;
it accepts whatever the spec describes. The spec lists each node's
role, its peer set, and the link to each peer (or to the shared
infrastructure). Asymmetric links are first-class:
`bandwidth(A→B) ≠ bandwidth(B→A)`, and `loss(A→B) ≠ loss(B→A)`,
without special-casing.

Partition and heal are topology mutations applied at scheduled
virtual times. Each mutation is a record on the recording stream
(emitted as a sim-only `Custom` event, OBSERVABILITY §6 rule 4) so
the post-processor can correlate it with peer-observed events.

### 4.2 Link physics

Each directed link carries:

- `bandwidth_bps` — capacity. Excess packets queue (per the queue
  policy) or drop (per the drop policy).
- `one_way_delay_ms` — base latency. Distributions (lognormal, etc.)
  are allowed; the spec declares which.
- `jitter_ms` — per-packet delta on top of the base.
- `loss_rate` — independent or correlated; the spec declares which.
- `mtu` — packets above MTU are fragmented per the L3 / L4 rules.
- `queue_policy` — FIFO / SFQ / FQ-CoDel; spec-declared.

These are the *applied* values; OBSERVABILITY §4.8 commits the sim
to also emit them on every snapshot in the `links_applied` block
(sim-only field) so a replay knows what conditions it ran under.
The *observed* values flow out of the same passive-estimation code
prod uses, on the `links` block (sim and prod).

Per-link parameters can vary over time per the spec. Time-varying
parameters are also part of the topology declaration; they are
applied at scheduled virtual times, the same way partitions are.

### 4.3 L3 — IP

The sim addresses every host with one or more IPv4 and IPv6
addresses. The address assignment is in the topology spec.
Per-host interface metadata (name, MTU, up/down) is read by the
host scrape (OBSERVABILITY §3.8); the sim populates it from the
spec.

IP-level behavior modelled:

- Per-packet TTL decrement at every hop. Packets with TTL=0 are
  dropped and the drop is emitted as a `WirePacket` (§4.1 in
  OBSERVABILITY) with outcome `dropped_ttl`.
- ECN bits passed through (no AQM model unless the spec adds one).
- Fragmentation when MTU is exceeded. Reassembly at the destination,
  with the standard timeout. Reassembly failures observable.
- Source-address validation. Unspoofable per default; the spec can
  override per-link to allow spoofing for adversarial scenarios.

IP-level behavior *not* modelled: ICMP error generation beyond the
two cases needed for path-MTU discovery (`Fragmentation Needed`)
and dead-peer detection (`Port Unreachable`). The sim emits both
when the corresponding condition occurs.

### 4.4 NAT and middleboxes

NATs are first-class entities in the topology, with type drawn from
{Full-Cone, Restricted-Cone, Port-Restricted-Cone, Symmetric}. Each
NAT maintains a binding table; bindings expire per the spec's
keepalive-timeout setting and are refreshed by outbound packets.

OBSERVABILITY §4.6 commits the sim to expose the binding table on
every snapshot (`nat_bindings` per peer). The sim's authoritative
table is the source; the recording shows the binding from the
hosted side. NAT-binding refresh interval and refresh count match
real-world dynamics under each modelled NAT class.

Beyond NAT, the spec can place middleboxes that drop packets by
DPI signature (used for adversarial tests of relay traffic).
Middlebox drops are observable as `WirePacket` outcomes.

### 4.5 L4 — UDP, TCP, QUIC

**UDP** is a thin wrapper over the L3 model: send-to / recv-from,
no flow control, drops surfaced as silent loss. Probes
(OBSERVABILITY §3.9) ride on this layer.

**TCP** is reserved for shared-infrastructure protocols (e.g., the
collector HTTP sink). It is modelled as a faithful loss-recovery
implementation, but its connection state is *not* exposed in
tier-2 because production iroh does not use it for peer traffic.

**QUIC** is the load-bearing L4. The sim runs the same QUIC
implementation iroh runs in prod (quinn), linked against the sim
UDP. The QUIC handshake, congestion control, and stream multiplexing
behave exactly as they do in prod because the *implementation* is
the same; only the underlying UDP is the sim's. This is what makes
the OBSERVABILITY §4.3 (congestion-control state) and §4.7
(handshake records) parity bars achievable: the data structures
reporting on each are the real ones.

### 4.6 Endpoint stack

Each sim host runs the full iroh stack (or its equivalent for
non-iroh transports): MagicSock, discovery, NodeMap, connection
cache. These are *not* re-implemented; they are the production
code linked against the sim runtime facade. That is the only way
their internal state — the state surfaced in OBSERVABILITY §3.6
(`Tier2IrohState`) — matches prod by construction.

Per-node configuration (relay set, discovery providers) is from
the topology spec. The DNS resolver used by iroh's relay-URL
lookup is the simulated one (§4.4 in this spec — *not* the OS
resolver — the simulated host model).

### 4.7 Clocks

Each host's clock is virtual time plus a per-host skew and drift
configured by the spec. Skew is a constant offset; drift is a
rate (ppm). Clock samples (OBSERVABILITY §3.5) capture the
relationship between each host's clock and the collector's
("virtual UTC"), so the post-processor's alignment code does the
same work on sim and prod bundles.

A host can experience a clock jump (e.g., NTP step) at a scheduled
virtual time. The jump is a topology mutation; it is observable
because every event after it carries the new `wall_ms` while
`monotonic_seq` (OBSERVABILITY §3.3) keeps going up.

### 4.8 Host budget

Each host has a modelled budget: CPU shares, memory bytes,
open-fd count. These map to the OBSERVABILITY §3.11 process-stats
fields. The sim does not enforce the budget against peer code
(peer code runs as fibers); instead, the budget is tracked
indirectly:

- CPU time per fiber is approximated from the number of times the
  fiber yields per virtual-time unit, weighted by the per-host CPU
  share. The result feeds the `cpu_ms` field on the process snapshot.
- Memory is tracked from the peer code's own allocations against
  the per-node allocator wrapper. RSS is reported as the
  high-water mark seen since the previous snapshot. VmSize is the
  current live byte count.
- Open-fd count is the number of sim-side sockets currently bound
  by the node (UDP, QUIC, TCP, plus simulated files).

This is the imperfect-but-honest implementation NORTH_STAR §"What
it models" calls for: the recording surface is populated; the
underlying numbers are approximations, marked as such in the
post-processor's documentation; calibration against prod will tell
us when the approximations are too loose.

## 5. Hosted entities

### 5.1 Sim-native hosts

A sim-native host is a peer whose code we own and rebuild against
the sim runtime facade. It is the same binary in spirit as the
prod binary — same crates, same code paths — linked against a
different runtime crate.

A run hosts arbitrarily many sim-native hosts. Each has its own
address space, its own runtime facade instance, its own
diagnostics aggregator (OBSERVABILITY §3). The engine routes I/O
between them through the network model.

Sim-native hosts cover all of OBSERVABILITY §3's parity bar
because they are running the production diagnostics stack — every
event and snapshot is emitted by the same code that emits them in
prod.

### 5.2 Opaque-binary hosts via virtualized I/O

When we need to host a peer whose code we cannot rebuild against
the facade — a third-party node, a peer running an old version,
a kernel that handles a packet a specific way — the sim drives
the real binary through a virtualized I/O boundary. The binary
runs as a real OS process; its socket calls go through a shim
(LD_PRELOAD-style on Linux, or a network-namespace + tun device
on platforms where syscall interception is brittle); the shim
routes packets to the engine the same way sim-native UDP does.

From the binary's perspective, it sees a real kernel, real sockets,
real time. The engine controls what crosses the wire. Records
emitted by the opaque binary (if any) are read off its own log
output and translated into the recording schema as best as the
adapter can manage; gaps appear as `None` fields, marked.

Opaque-binary hosts are an escape hatch, not the default. The
parity bar applies fully to sim-native hosts; opaque-binary hosts
have inherently incomplete recording (we cannot fully instrument
a binary we did not build).

### 5.3 Lifecycle

Every host has a defined lifecycle in the topology spec:

- `start_at_ms` — virtual time at which the host's runtime is
  brought up. Boot order is deterministic.
- `restart_at_ms[]` — scheduled restarts. `boot_sequence`
  (OBSERVABILITY §3.1) increments per restart.
- `stop_at_ms` — clean shutdown.
- `crash_at_ms` — uncontrolled exit. Distinct from `stop` because
  no draining occurs.

Each lifecycle event is a topology mutation; each is observable as
a `Custom` event on the recording (the host that experienced the
event reports it through its own diagnostics, if reachable; the
engine reports it as a sim-only record otherwise).

## 6. Recording format

### 6.1 Wire-level trace

The wire-level trace is the per-packet stream described in
OBSERVABILITY §4.1. The sim emits one record per packet in either
direction at every modelled hop. Records are written to
`{run_dir}/wire/{node_id}.ndjson` and tarred into the bundle.

Wire-level capture is gated by a per-run setting. Off by default
because the data volume is high; on for calibration runs and for
any run where the §4.1 OBSERVABILITY parity bar is being checked.

When a payload-bytes capture is requested, the bytes live next to
the trace as a separate file (`wire-payloads/...`) so the trace
itself stays small enough to grep through.

### 6.2 Internal-observation trace

This is the production recording surface itself — OBSERVABILITY §3
in its entirety. The sim writes the same files to the same paths
the production collector does, under
`{collector_root}/{run_id}/{node_id}/`:

```
boot.json
snapshots/{snapshot_id}.json
events/{batch_seq}.json
finalize.json
```

A sim run produces a bundle byte-for-byte indistinguishable from a
prod bundle of the same workload (modulo the sim-only fields
flagged in OBSERVABILITY §3.11 / §6.5 below). That isomorphism is
the parity bar.

### 6.3 Schema versioning

The schema is versioned. Every record carries a `schema_version`
field at the top of its envelope. The post-processor accepts any
version it knows about. Schema upgrades are documented in the
diagnostics crate's CHANGELOG; the sim and prod sides upgrade
together (NORTH_STAR §"Calibration is ongoing").

Adding a field to a record is a minor version bump; removing or
renaming is major. Old bundles remain readable forever; the
post-processor handles back-compat. The sim's emitted version
matches whatever version of the diagnostics crate it is linked
against, the same as prod.

### 6.4 Storage layout

Bundles are tarballs (`.tar.gz`) at
`{collector_root}/bundles/{run_id}.tar.gz`. The internal layout
matches `DIAGNOSTICS_PLAN.md` §"Bundle Format":

```
{run_id}/
  MANIFEST.json
  orchestrator/{boot, snapshots/, events/, finalize}.json
  stage-N/{boot, snapshots/, events/, finalize}.json
  collector.log
  summary.md            # post-processor output, optional
```

Sim runs additionally write:

```
{run_id}/
  sim/
    spec.toml            # the topology spec used
    seed                 # the run seed
    wire/{node_id}.ndjson  (if wire-level capture is on)
    links_applied.json   # ground-truth link params over time
    mutations.log        # partition/heal/restart events
```

The `sim/` subtree is sim-only and is ignored by the parity diff.
It is what enables a sim-vs-sim replay (§2.5) and what gives a
calibration run the ground truth to compare against.

### 6.5 Sim-only fields

Sim runs emit a small number of fields prod cannot: ground-truth
applied link parameters (`links_applied`, OBSERVABILITY §4.8),
sim-internal scheduler decisions (OBSERVABILITY §6 rule 4),
RNG-state traces. These are written to `sim/` (§6.4) and are never
read by code that also runs in prod. The node-under-test cannot
observe any of them.

## 7. Replay

### 7.1 Environment restoration

Replay takes a recording (produced by a prod run, a sim run, or
either one with a partial bundle) and plays back the *environment*
the recorded run was in. Concretely:

- Topology is reconstructed from the recording. NORTH_STAR §"Two
  modes, one engine" demands this be feasible from a real-world
  bundle, so the per-link parameters used by the sim must be
  derivable from what the recording exposes. Where the recording
  exposes only `links` (observed), the sim seeds its `links_applied`
  with the observed values and lets the parity diff quantify any
  drift. Where the recording exposes `links_applied` (sim-origin),
  the sim adopts them exactly.
- Peer set is reconstructed from the boot identity blocks across
  the bundle.
- Mutation schedule (partition, heal, restart) is reconstructed
  from the `Custom` events the original run emitted.
- Shared infrastructure (relays, DNS, vast.ai metadata) is
  reconstructed from whatever the bundle's host-scrape and
  identity blocks recorded.

### 7.2 Peer re-execution

Per NORTH_STAR §"Two modes, one engine": peers in a replay are
**re-executed peer code**, not stubs driven by recorded outputs.
The recording carries the *environment*; the peer logic that
runs against that environment is whatever version is under test
in the replay. That is how a replay tests a candidate change — by
exposing the same world to new code.

When the recording lacks information needed to drive a peer
faithfully (a third-party peer whose code we do not have), that
peer is hosted via the opaque-binary path (§5.2). If the original
recording was prod and the peer was iroh-stack swactor, the
sim-native path applies.

### 7.3 Divergence handling

A replay diverges when a re-executed peer makes a different choice
from what the recording shows the original peer made. Divergence
is *expected*: that is the point of replay — to see whether the new
code behaves differently against the same conditions.

The replay records the new behavior at the same level of detail
(§6) and the post-processor's diff mode is what surfaces the
delta. Divergence is not an error; only when the divergence is
*outside the parity envelope* (§9.3) is it a sim bug rather than
an algorithm-change observation.

## 8. Observability

### 8.1 Shared sinks with prod

The sim's diagnostics aggregator (the one running inside each
sim-native host) is the *production* diagnostics aggregator from
`crates/distribution/src/diagnostics`. It is linked against an
HTTP sink whose target is the sim's collector — itself a real
binary (also from `crates/distribution`), brought up by the engine
on a virtual host at the start of every sim run.

The collector writes the same bundle layout (§6.4). The
post-processor reads it the same way. The "shared" in "shared
sinks" means literal: the same code runs on both sides of the
sim/prod boundary, so the parity bar is enforced by construction
for every record kind enumerated in OBSERVABILITY §3.

### 8.2 Sim-only diagnostics

The sim emits a small set of additional records the node-under-test
cannot observe (OBSERVABILITY §6 rule 4): scheduler decision logs,
exact virtual-time tick streams, RNG-state traces, per-link
applied parameters (§6.5). They are written to the `sim/` subtree
of the bundle and are read only by the engine's own debugging
tooling, never by code that also runs in prod.

The discipline: any time we are tempted to add a sim-only field
that the node-under-test *can* observe, we have to either
(a) commit to making prod observe it too and put it in
OBSERVABILITY §3 or §4, or (b) keep it strictly under §8.2. There
is no third option.

## 9. Calibration loop

### 9.1 Procedure

Calibration is the act of measuring sim-vs-prod observable
equivalence. The loop is concrete:

1. Run a workload in prod. Collect the bundle.
2. Extract the topology + mutation schedule from the bundle (§7.1).
3. Run the *same* peer-code version in the sim against that
   reconstructed environment. Collect the bundle.
4. Diff the two bundles, record kind by record kind, against the
   parity bars defined in OBSERVABILITY §3 / §4.
5. Anywhere the distributions diverge beyond the noise floor
   (§9.2), file a sim bug. Anywhere prod is missing a record the
   sim emits or vice versa, file a recording bug or a sim bug
   (whichever direction the asymmetry runs — see OBSERVABILITY
   §6 rule 3).

The loop runs continuously, not as a one-time validation. Every
real deployment is evidence; every divergence is an issue.

### 9.2 Noise-floor estimation

Prod runs are inherently noisy: real link latencies jitter, real
kernels make scheduling decisions we cannot reproduce, real clocks
drift. Calibration cannot demand sim records to be byte-identical
with prod records — it demands them to be *statistically
indistinguishable* (NORTH_STAR §"The parity bar").

The noise floor is estimated from prod-vs-prod variance: two prod
runs of the same workload, under as-matched-as-possible
conditions, diff each other. The variance of that diff defines
the noise floor for each numeric field; a sim-vs-prod divergence
within that envelope is not a bug. A sim-vs-prod divergence beyond
it is.

Establishing the noise floor is an ongoing project — every new
metric needs its own estimate. The post-processor's calibration
report ships per-metric noise-floor numbers alongside the
sim-vs-prod diff.

### 9.3 Parity metrics

Per OBSERVABILITY's call for distributional parity, the
calibration report tracks for each metric:

- **Presence parity** — does the sim emit a record whenever prod
  does (and vice versa)? Boolean per record kind.
- **Mean parity** — is `mean_sim ≈ mean_prod` within the noise
  floor of the means?
- **Tail parity** — is `p99_sim ≈ p99_prod` within the noise floor
  of the tail? This is the bar OBSERVABILITY calls out
  explicitly; mean parity alone is not enough.
- **Causal parity** — does the sim emit events in the same order
  prod does, conditioned on the same upstream causes? Tied to the
  causal-trace-IDs surface (OBSERVABILITY §4.2).

A pass is "every metric within noise floor on every parity
dimension." A fail names the metric and dimension. The
post-processor's calibration report is structured exactly that
way; "did calibration pass" reduces to "did any line in the
report come back red."

## 10. Glossary

- **Aggregator** — per-process recording state owner. Owns the
  reachability log, event ring, snapshot assembler, sink. Same
  type in sim and prod.
- **Bundle** — the tarball at end-of-run containing every record
  emitted during the run. See §6.4.
- **Calibration** — measurement of sim-vs-prod observable
  equivalence. See §9.
- **Collector** — the HTTP server every aggregator POSTs to.
  Centralized per-run target; writes the bundle.
- **Engine** — the discrete-event simulator core. Owns virtual
  time, the executor, the network model, the topology graph. See
  §2.
- **Facade** — the trait surface peer code uses for all
  potentially-non-deterministic operations. Two implementations
  (prod, sim). See §3.
- **Fiber** — an executor-managed unit of concurrency. One fiber
  per peer task; per-fiber scheduling is deterministic. See §2.3.
- **Mutation** — a scheduled change to the topology during a run
  (partition, heal, restart, link-parameter change). Recorded as
  a `Custom` event. See §4.1.
- **Noise floor** — the variance between two prod-vs-prod runs of
  the same workload. Sets the bar that sim-vs-prod divergence is
  measured against. See §9.2.
- **Opaque binary** — a hosted entity whose code we do not own and
  cannot rebuild against the facade. Driven via syscall
  interception. See §5.2.
- **Parity bar** — the requirement, per OBSERVABILITY, that every
  record kind on the observable surface match between sim and
  prod. See OBSERVABILITY §6.
- **Recording** — the on-disk artifact of a run's observable
  surface. Same format in sim and prod. See §6.
- **Replay** — re-execution of peer code against an environment
  reconstructed from a recording. See §7.
- **Run** — one execution of the engine from boot to finalize.
  Identified by `run_id` (in the bundle) and `run_seed` (input).
- **Sim-native host** — a peer whose code is linked against the
  sim runtime facade. See §5.1.
- **Topology spec** — the input declaration of nodes, links,
  shared infrastructure, mutations, and run parameters. See §4.1.
- **Virtual time** — the engine's authoritative clock.
  Wall-clock `wall_ms` fields in the recording are populated from
  it on sim runs. See §2.2.
