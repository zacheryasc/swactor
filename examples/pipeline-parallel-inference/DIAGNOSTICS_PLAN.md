# Pipeline-Parallel Inference: Diagnostics Plan

This plan describes the data we intend to collect on every run of
`pp-smoke-run` (local, Docker, and especially vast.ai) so that failures
of the iroh / SWIM layer become explainable from a single artifact
produced by the run itself, without ssh, re-renting, or guessing.

It is the response to `VASTAI_STATUS.md`. The open questions in that
doc — "which stage is the dead one?", "is routing asymmetric?", "are
canary relays the cause?", "is the connection cache stale?" — should
all be answerable from the bundle produced by a single tier-1 run, and
the deeper *why* questions should be answerable from a tier-2 run.

This document describes **what** is collected, not how. An
implementation pass should treat each collection below as a discrete
unit of work whose shape is fixed by this doc but whose mechanism is
open.

---

## Goal

A single vast.ai run produces one tarball. The tarball contains
per-node snapshots, event streams, and host context, time-aligned
across nodes, plus a one-page human-readable summary. Reading the
tarball answers:

- Which peer (by hex) corresponds to which stage, on which host.
- For every peer-pair, in each direction, when packets last flowed.
- The exact moment, peer, and reason for the SWIM transition that
  killed the cluster.
- The iroh-internal view of each peer at that moment: connection type,
  known addresses, known relays, latency.
- The host's network and DNS state at that moment.
- Whether the orchestrator and the dead peer shared a relay or not.

If the bundle doesn't answer one of these, the plan has a gap and we
patch it before the next session.

## Principles

1. **Coverage of local observations, not global truth.** We cannot
   reconstruct a globally true mesh — silence is unrecorded and clocks
   skew. We can make each node's local record complete and time-tagged
   well enough to stitch together post-hoc.

2. **Delivery survives the failure being diagnosed.** Anything iroh+
   SWIM are responsible for, we don't ship diagnostics over. The
   collector path is HTTP, side-channel to the layer under test.

3. **Bundles are read by tools, not eyeballs.** Free-text logs are
   what cost us 8 vast.ai rentals. Every record is structured. The
   post-processing tool is part of the plan, not a follow-up.

4. **Boot identity is the keystone.** Every diagnostic is useless if
   we cannot bind `node_id_hex` ↔ `stage_index` ↔ `host`. Land that
   first or nothing else parses.

5. **Capture broadly, summarize narrowly.** We collect everything;
   the post-processor produces a one-pager. The raw data is for the
   tool, the one-pager is for the human.

6. **No code in core swactor.** Diagnostics live in the crate layers
   (`crates/distribution`, a new `crates/diagnostics`, the
   pipeline-parallel example) and consume swactor through its public
   surface. The actor runtime under `src/` stays clean; instrumentation
   lives where the behavior being observed lives.

7. **Crate-level feature, not pipeline-parallel-specific.** Event
   emission, the aggregator, and the collector protocol are a reusable
   feature consumed by any binary built on this stack.
   `pp-smoke-run` is the first consumer, not the owner. Other examples
   (single-gpu-inference, future binaries) get the same diagnostics for
   free.

---

# Tier 1 — Self-Contained Post-Mortem Bundle

Tier 1 is the skeleton. Around iroh, not inside it. Goal: produce a
parseable bundle that answers every open question in
`VASTAI_STATUS.md`. Each collection below is independent and can be
landed separately, but the tier is only useful when all are present.

## T1.1 — Identity Binding

A single canonical record emitted by every node at boot and
re-emitted in the header of every snapshot.

Fields:

- `node_id_hex` — full hex, never truncated in records
- `node_id_short` — first 8 hex chars (matches what orch logs print)
- `role` — `"orchestrator"` or `"stage"`
- `stage_index` — integer, 0..N-1 for stages, null for orchestrator
- `stage_count` — N for the current run
- `run_id` — opaque string assigned by `pp-smoke-run` at run start,
  identical across all nodes in the same run
- `vastai_contract_id` — null when not on vast.ai
- `host_ip_public` — best-effort, from vastai metadata or an external
  reflection probe at boot
- `host_country`, `datacenter_id` — from vast.ai metadata
- `hostname`, `container_id`
- `process_start_unix_ms`
- `boot_sequence` — incremented on restart (allows distinguishing
  reruns inside a single contract)
- `binary_version`, `git_sha`, `iroh_version`
- `home_relay_url_at_boot` — best-effort; null if iroh hasn't picked
  one yet at the moment of the record

Trigger: emitted to the collector on boot and embedded in every
snapshot header. Logged to stdout once at boot in a single grep-able
line.

Answers: "which peer hash is which stage" — without this, nothing
else parses.

## T1.2 — Per-Peer Per-Direction Reachability Log

Per-node, per-remote-peer record, maintained continuously. The
*local* reachability matrix from the perspective of this node.

Fields per peer:

- `peer_node_id_hex`
- `last_inbound_packet_at_ms` — wall time of last inbound traffic of
  any kind from this peer, plus `via_relay_url` (or `"direct"`)
- `last_outbound_success_at_ms` — last outbound message we observed
  succeed, plus `via_relay_url`
- `last_dial_started_at_ms`
- `last_dial_outcome` — `success | timeout | refused | no_route |
  error(string)`
- `last_dial_duration_ms`
- `current_swim_opinion` — `alive | suspect | dead | unknown`
- `current_swim_opinion_since_ms`
- `swim_transition_history` — bounded ring buffer (~32) of
  `{from, to, at_ms, reason}` tuples
- `metadata_version_seen` — latest metadata version we have from this
  peer
- `metadata_relay_url_seen` — relay URL this peer told us about (may
  differ from what iroh actually used)

Trigger: maintained in memory continuously; emitted as part of every
snapshot.

Answers: asymmetric routing. By aligning `node_A.last_inbound_from_B`
against `node_B.last_outbound_to_A` across the bundle, asymmetry
becomes immediately visible.

## T1.3 — Structured Event Stream

Per-node append-only stream of typed events. Free-text logs are
retained for human debugging but the diagnostic record is structured.

Common envelope:

```
{ node_id, monotonic_seq, wall_ms, event_type, fields }
```

`monotonic_seq` is a per-node integer that never decreases, used for
intra-node ordering even when wall clock jumps. `wall_ms` is best-
effort and aligned post-hoc via T1.5.

Event types (minimum set):

- `boot` — re-emits identity block
- `relay_changed` — `{old_url, new_url, reason?}` for our own home
  relay (subscribed via iroh; see T2.2)
- `swim_metadata_sent` — `{version, payload_hash}`
- `swim_metadata_received` — `{peer, version, payload_hash, fields}`
- `dial_started` — `{peer, attempt, timeout_ms}`
- `dial_outcome` — `{peer, attempt, outcome, duration_ms, error?}`
- `swim_transition` — `{peer, from, to, reason}`
- `connection_cache_hit` / `cache_miss` / `cache_invalidated`
  — `{peer, generation, reason?}`
- `message_sent` / `message_received` — `{peer, message_type, size}`
- `probe_sent` / `probe_received` — for our own probe layer (T3.3)
- `error` — `{component, message, peer?}`

Delivery: batched POST to the collector every ~1s and immediately on
state-transition events (`swim_transition`, `relay_changed`,
`cache_invalidated`).

Answers: ordered causality of failure within a node and, post-
alignment, across nodes.

## T1.4 — Snapshot Fan-Out

A snapshot is a full point-in-time dump of the local view: identity
block + reachability log (T1.2) + event tail since last snapshot +
(in tier 2) iroh and SWIM internals + (in tier 3) host context.

Triggers:

- **Periodic** — every 5s (default; configurable).
- **Local-transition** — each node snapshots immediately whenever its
  own SWIM view transitions any peer (alive→suspect→dead etc.).
  Different nodes will snapshot at different moments; the
  post-processor correlates them via T1.5 clock alignment plus the
  SWIM message versions both sides observed. This replaces what would
  otherwise be a fan-out broadcast — neither the orchestrator (NAT'd
  laptop) nor vast.ai workers can reliably push to each other on
  demand, so we rely on independent local triggers + post-hoc
  correlation.
- **Pull-trigger** — the collector can attach a `snapshot_now` hint in
  the response to any node's HTTP POST. Nodes honor the hint on their
  next opportunity. `pp-smoke-run` uses this at end-of-run to force a
  global final snapshot.

Snapshot record envelope:

```
{ identity_block, run_id, snapshot_id, wall_ms, monotonic_seq,
  trigger: periodic | transition(event_id) | on_demand,
  body: { ... tier-1/2/3 fields ... } }
```

Answers: correlated views at the smoking-gun moment. Periodic
sampling is the safety net; transition-driven is the smoking gun.

## T1.5 — Clock Alignment via Collector

Without a global clock, post-hoc alignment is the next best thing.
The collector is the canonical time source; alignment piggybacks on
every diagnostic POST and needs no dedicated channel.

Mechanism (described as data, not protocol): on every POST a node
makes to the collector, the node includes `node_send_ms` (its wall
clock at send). The collector's response includes
`{node_send_ms_echoed, collector_recv_ms, collector_send_ms}`. The
node records its own `wall_ms_at_receive`. From these four values
the node (or the post-processor) computes the node's offset to
collector time with bounded error (RTT/2 worst case).

Each `clock_sample` is recorded as a structured event so the post-
processor has a stream of offsets per node over the lifetime of the
run, not just a single calibration.

Every node — including the orchestrator on the user's laptop — talks
to the collector, so every node gets aligned to the same reference.

Sub-second precision is enough for our purposes.

Answers: when reading the bundle, "did stage 0 go dead before or
after stage 2's outbound dial timed out?" — currently unanswerable
because we only have local clocks.

## T1.6 — Out-of-Band Collector (VPS-Hosted)

A small HTTP server deployed once to a stable VPS with public ports.
All nodes — orchestrator (on the user's laptop, NAT'd) and every
stage (on vast.ai) — POST to the same collector URL. The URL is
injected into every process at startup via an environment variable
(`SWACTOR_DIAG_COLLECTOR_URL` or similar).

The collector is shared infrastructure, not part of any single run.
Multiple concurrent or sequential runs are separated by `run_id`. A
run's diagnostic bundle is the slice of collector storage tagged
with that id.

Why a VPS and not the orchestrator: the orchestrator runs on the
user's laptop with no stable public ingress, so vast.ai workers
cannot reach back to it. The collector must live somewhere both
sides can reach. The user already operates a VPS with stable ports;
the collector deploys there.

Endpoints (conceptual; one per record kind):

- `POST /diag/boot` — identity block on boot
- `POST /diag/events` — batch of structured events
- `POST /diag/snapshot` — a single snapshot record
- `POST /diag/finalize` — run end marker, includes summary metadata

All requests carry `run_id` and `node_id` headers. All responses
include a `clock` block (T1.5) and an optional `hints` block (T1.4
pull-trigger).

The collector persists to `{collector_root}/{run_id}/{node_id}/`.

If the collector is temporarily unreachable, the node spools records
to a local on-disk queue (`/tmp/swactor-diag/{run_id}/`) and retries
with exponential backoff. The on-disk spool is included in the
tarball post-hoc so we never silently lose data on transient
collector outages. No external-fallback collector is needed —
there's just the one collector, and the spool covers its downtime.

Answers: ensures diagnostics survive iroh failures, orchestrator
death, and individual stage isolation, because none of the failure
modes the system is being diagnosed for involve the diagnostic path
itself.

## T1.7 — Run Tarball Assembly

At end of run (success, failure, SIGTERM, or `wait_for_running`
timeout), `pp-smoke-run` POSTs `/diag/finalize` to the collector,
which:

- Sets a `snapshot_now` hint for every node still posting under this
  `run_id`, so each node emits a final snapshot
- Waits up to ~5s for stragglers
- Tars `{collector_root}/{run_id}/` into one archive
- Writes a `MANIFEST.json` at the tar root listing nodes, snapshot
  counts, event counts, run start/end times, exit reason
- Drops the archive at a configurable path on the collector host
  (default: `{collector_root}/bundles/{run_id}.tar.gz`)

`pp-smoke-run` can optionally fetch the tarball back to the laptop
via a `GET /diag/bundle/{run_id}` endpoint for offline inspection.

This is the artifact for the next session. Everything else exists to
fill it.

---

# Tier 2 — Why Did the Reachability Gap Exist?

Tier 1 tells us *that* the cluster failed and *where*. Tier 2 tells
us *why* by going inside iroh and inside SWIM. These collections live
inside snapshots (T1.4) and add events to the stream (T1.3).

## T2.1 — iroh RemoteInfo Scrape (per-peer)

For each peer iroh has heard of, capture iroh's own view:

- `conn_type` — `Direct | Relay | Mixed | None`
- `latency_ms` — if iroh reports
- `last_used_ms`, `last_received_ms` — iroh's accounting (compare
  against our own T1.2)
- `direct_addresses` — list of (ip, port) iroh has discovered
- `relay_urls` — list of relays iroh has for this peer
- `addr_sources` — for each address, where iroh learned it (discovery,
  add_node_addr, observed inbound)

Trigger: included in every snapshot. Additionally, emit a
`conn_type_changed` event whenever iroh transitions between
Direct/Relay/Mixed/None for a given peer.

Answers: "did iroh ever have a path to this peer?" — separates
"we never told iroh how to reach them" from "iroh tried and gave up."

## T2.2 — iroh Home-Relay Watch

Subscribe to iroh's home-relay watcher (per node, for our own home
relay). Emit `relay_changed` events on every transition.

Captured fields per event:

- `old_url`, `new_url`
- `reason` — if iroh exposes one
- `time_since_last_change_ms`

Answers: did our home relay flap mid-run? Did the orchestrator and
the dead stage actually share a relay at the moment they failed to
reach each other? Currently the doc says "every run landed on canary"
but we don't know if that was stable across the run.

## T2.3 — iroh Metrics Counters

Pull all `iroh-metrics` counter values into every tier-2 snapshot.
The set is whatever iroh exposes; we don't curate. The post-processor
computes deltas.

Counters of particular interest (named roughly per iroh's vocabulary;
exact names per iroh version):

- `relay_send_ok`, `relay_send_err`
- `magicsock_*` (holepunch attempts, successes, failures)
- `conn_open`, `conn_close`
- discovery counters

Answers: a fingerprint of what iroh is actually doing under the
hood. Deltas around a `swim_transition` event are the most diagnostic
slice.

## T2.4 — Connection-Cache Lifecycle

Our iroh driver caches one `Connection` per `NodeId`. The cache is
currently invisible. For each entry:

- `peer_node_id_hex`
- `generation` — incremented on every invalidation/recreate
- `created_at_ms`
- `last_successful_send_at_ms`, `last_successful_recv_at_ms`
- `last_failure_at_ms`, `last_failure_reason`
- `observed_conn_type_at_last_use` — Direct/Relay/Mixed at the last
  successful traffic moment (from T2.1)

Trigger: included in every tier-2 snapshot. Cache mutations
(`cache_hit`, `cache_miss`, `cache_invalidated`) already emit T1.3
events; this adds the *aggregate* view per peer.

Answers: the doc's open question about whether stale cached
connections matter. We currently never invalidate; this collection
shows when we should have.

## T2.5 — NodeMap Delta Tracking

Whenever we push address info into iroh (e.g. `add_node_addr` after
parsing a SWIM metadata update), record both the input and the
result:

- `peer_node_id_hex`
- `from_source` — `swim_metadata | discovery | static | other`
- `endpoint_addr_in` — relay URL, direct addrs as passed
- `iroh_return` — success, error, or "noop" if iroh ignored
- `diff_from_previous` — fields that changed vs. last known

Trigger: event-driven on every push, recorded as `nodemap_update`
events in T1.3.

Answers: did we *try* to tell iroh about a peer's relay URL but iroh
silently kept old state? The relay-url-gossip-via-SWIM-metadata fix
in `VASTAI_STATUS.md` assumes iroh accepts the update; we currently
have no way to verify.

## T2.6 — SWIM Internals Snapshot

For each peer in the SWIM membership list:

- `incarnation`
- `last_ping_sent_at_ms`, `last_ping_received_at_ms`
- `last_ack_sent_at_ms`, `last_ack_received_at_ms`
- `suspect_timer_started_at_ms`, `suspect_timer_expires_at_ms`
  (when applicable)
- `metadata_version`
- `current_state` — alive/suspect/dead (same as T1.2 but from SWIM's
  own structures, sanity-check against the reachability log)

Plus configured timeouts at the top of the SWIM block so the snapshot
is self-describing (`probe_interval_ms`, `suspect_timeout_ms`,
`ack_timeout_ms`, indirect-probe-k, gossip fanout, etc).

Plus a bounded ring buffer of recent SWIM messages received
(~64 entries): `{at_ms, from_peer, message_type, size_bytes}`.

Trigger: included in every tier-2 snapshot.

Answers: why did SWIM decide a peer was dead? Was the suspect timer
too short for the observed ack latency? Did we lose acks but receive
pings (a one-way break)?

## T2.7 — Discovery Activity

For each iroh discovery resolve attempt:

- `peer_node_id_hex`
- `started_at_ms`, `completed_at_ms`
- `outcome` — `success | timeout | not_found | error`
- `addresses_returned`
- `relay_returned`

Emitted as `discovery_resolve_started` / `_completed` events in T1.3.

Answers: did discovery contribute anything at this scale, or are we
purely relying on SWIM metadata? Useful when the metadata path itself
is suspect.

---

# Tier 3 — Causes Outside Our Process

Tier 2 might still leave the diagnosis at "iroh thought it had no
path to peer X." Tier 3 explains why the environment let that happen.

## T3.1 — Host Network Snapshot

Captured at boot and refreshed every ~30s (not every snapshot, too
heavy):

- Network interfaces: name, ip addresses (v4/v6), mtu, state up/down
- Default routes (v4, v6)
- `/proc/net/udp` entries for iroh's bound sockets (so we can confirm
  the socket exists and where it's bound)
- conntrack count, if available (best-effort, requires capability;
  null if not)
- IPv6 enabled? (from `/proc/sys/net/ipv6/conf/all/disable_ipv6`)
- Container's view of `/etc/resolv.conf` nameservers

Trigger: boot, then every ~30s; full snapshot includes the most
recent value.

Answers: did the host have IPv6 disabled when iroh expected it? Did
the bound UDP socket disappear? Did the route table change mid-run?

## T3.2 — DNS Resolution Snapshots

For every relay URL we've ever seen mentioned (ours or any peer's
via metadata), periodically resolve it and record:

- `hostname`
- `a_records` (list of v4)
- `aaaa_records` (list of v6)
- `ttl_seconds`
- `resolver_used` — from resolv.conf
- `resolved_at_ms`

Trigger: every ~30s, per known relay URL. Recorded into snapshots.

Answers: did orchestrator and the dead stage actually resolve the
same relay name to the same IP? Did relay DNS flap? This is a
classic asymmetric-connectivity cause that's invisible without
explicit collection.

## T3.3 — Outbound Reachability Probes

Independent of iroh. From each node, periodically:

- UDP probe to each known relay URL (ours and peers' as seen in
  metadata), on iroh's expected ports
- UDP probe to each other node's `host_ip_public` (from their
  identity block) on a known echo port if we expose one
- Baseline probe to a known-stable target (e.g., a public STUN or
  echo service) — gives a "network is up at all" signal

Each probe records `{target, kind, started_at_ms, outcome, rtt_ms?,
error?}` as `probe_sent` / `probe_received` events in T1.3.

Trigger: every ~10s.

Answers: separates "iroh can't reach this relay" from "this host
can't reach this relay at all." If raw UDP works but iroh fails,
the bug is iroh-side. If raw UDP fails, the bug is environment-side.

## T3.4 — vast.ai-Side Context

At boot, capture every piece of vast.ai metadata available to the
container:

- `CONTAINER_ID`, `VAST_*` env vars (whatever vast.ai exposes)
- The full offer record we created the instance from (orchestrator-
  side, since `pp-smoke-run` already has it — keyed by contract id
  in the bundle)
- `host_country`, `datacenter_id`, `machine_id` if exposed
- Advertised bandwidth, GPU type, driver version, CUDA version
- Container start time vs. our process start time (catches slow
  container-start hosts)

Plus capture *any* stderr/log output from vast.ai's runtime layer
that mentions failures (CDI, container init, etc) at startup. The
CDI errors in `VASTAI_STATUS.md` were caught by accident; this makes
them mandatory.

Trigger: once at boot; included in identity block extensions in
every snapshot.

Answers: the CDI-class question, the "is this a bad host pool"
question, and gives us correlations across runs (do failures cluster
on specific datacenter_ids?).

## T3.5 — Process Resource Snapshot

Per-snapshot, low-cost:

- RSS, VmSize
- Open FD count
- Tokio runtime stats: worker count, active tasks, blocking pool
  size, idle workers
- Per-actor mailbox depth (if the actor framework exposes it)
- CPU time used since last snapshot

Trigger: every snapshot.

Answers: occasional smoking gun — a tokio worker stalled on a sync
call delays SWIM probes enough to look like network failure. Cheap
to collect, sometimes decisive.

---

# Architecture

Three decoupled components. The split matters because each can be
implemented, tested, and changed independently. Per principle 6, none
of this lives in core swactor (`src/`); everything lives in crates
above it, with the protocol and aggregator implemented in a reusable
diagnostics crate (per principle 7).

## A.1 — Diagnostics Crate (`crates/diagnostics`)

A new crate that provides:

- **Event emission API** — a trait or lightweight macro consumed by
  the layers that produce events (`crates/distribution` for SWIM and
  iroh driver events, the pipeline-parallel example for actor-level
  events). Crates that want to be observable depend on
  `crates/diagnostics` and emit events through its API. Adding new
  event types is a matter of adding a variant, not plumbing new
  channels.
- **The aggregator** — one instance per process. Owns the event ring
  buffer, the reachability log (T1.2), snapshot assembly, the
  delivery queue to the collector (with on-disk spool fallback per
  T1.6), and clock-sample tracking (T1.5). Every observing subsystem
  reports *into* the aggregator via in-process channels. The
  aggregator is the single place that talks to the collector.
- **The collector protocol** — the wire format for `POST /diag/*`,
  shared between aggregator and collector binary.

Consumers (pp-smoke-run, pp-gpu-node, single-gpu-inference, future
binaries) construct one aggregator at startup and pass its handle to
whichever subsystems want to emit. Nothing about this is
pipeline-parallel-specific.

The isolation between aggregator and the systems it observes
matters: a bug in delivery never corrupts collection, and
back-pressure in delivery never blocks the actor runtime.

## A.2 — Collector Binary

A small standalone HTTP server, separate binary in the same
diagnostics crate. Deployed once to the VPS. Owns:

- The HTTP endpoints listed in T1.6
- Persistence to disk under `{root}/{run_id}/{node_id}/`
- Returning the `clock` block on every response (T1.5)
- Returning optional `snapshot_now` hints in response bodies
  (T1.4 pull-trigger)
- Tarball finalization at `/diag/finalize`
- Optional bundle retrieval at `GET /diag/bundle/{run_id}`

The collector is shared infrastructure — one deployment serves all
runs. It does not interpret data, only stores it. All interpretation
is in A.3.

Operational hygiene (disk usage, bundle expiry) is the collector's
responsibility — configurable retention (e.g. expire bundles older
than 14 days).

## A.3 — Post-Processing Tool

A standalone binary (separate from `pp-smoke-run`; runs against a
bundle). Inputs: a tarball. Outputs:

- **Reachability matrix over time** — for each time bucket (e.g.,
  5s), an N×N table per direction with cells colored by SWIM
  opinion and annotated with conn_type from T2.1.
- **Per-peer-pair timeline** — events in order, clock-aligned per
  T1.5, with conn_type transitions, dial outcomes, and SWIM
  transitions marked.
- **One-page summary** — the first peer to go dead, when, what each
  side's reachability log showed at the moment, what relay each side
  was on, what iroh's conn_type to that peer was, whether raw UDP
  probes to that peer's relay were working.
- **Cross-run diff mode** — given two bundles, highlight what's
  different (e.g., relay choice, host country, conn_type evolution).
  Useful for "why did N=3 fail but N=2 pass on the same day?"

The one-pager is the only artifact a human needs to look at in the
common case. Everything else is for deep dives.

---

# Bundle Format

The tarball at `./diagnostics/{run_id}.tar.gz` contains:

```
{run_id}/
  MANIFEST.json
  orchestrator/
    boot.json
    snapshots/{snapshot_id}.json
    events/{batch_seq}.json
    finalize.json
  stage-0/
    boot.json
    snapshots/...
    events/...
    finalize.json
  stage-1/
    ...
  collector.log         # diagnostic log of the collector itself
  summary.md            # produced by post-processor on first read
                        # (optional; the tool can also regenerate)
```

`MANIFEST.json` lists nodes, run start/end, exit reason, snapshot
and event counts per node, and the orchestrator's identity block.

Files within `snapshots/` and `events/` are JSON; the post-
processing tool is the canonical consumer.

---

# Open Decisions

The forks below are not blocking but should be settled before
implementation rather than during.

1. **Collector hosting and discovery.** *Resolved.* Collector
   deploys to the user's VPS, which has stable public ports. The
   orchestrator runs from the user's laptop behind NAT and cannot
   itself host an ingress reachable from vast.ai workers — that's
   why the collector lives elsewhere. Every node (orchestrator and
   stages) receives the collector URL via env var at process start.
   No port-sharing with anything inference-related; the collector is
   its own service. The VPS-hosted collector *is* the canonical
   collector — no separate "external fallback" exists.

2. **Snapshot cadence vs. data volume.** Default 5s seems right for
   our 3–15 minute runs. Configurable per-tier (tier 1 every 5s,
   tier 2 every 10s, tier 3 every 30s) if volume is an issue. Easy
   to dial later; the protocol shouldn't bake it in.

3. **Event-emission scope.** *Resolved.* Event emission is a
   crate-level feature in `crates/diagnostics`, consumed by
   `crates/distribution` (for SWIM and iroh events), by the
   pipeline-parallel example (for actor-level events), and by any
   future binary on the same stack. Not pipeline-parallel-specific.
   Per principle 6, no event-emission code lives inside
   `src/` (core swactor) — observing crates import the diagnostics
   trait and emit through it.

4. **Clock alignment mechanism.** *Resolved.* Piggybacks on every
   HTTP POST to the collector — no separate channel. The collector
   is the canonical time source. See T1.5.

5. **iroh API surface stability.** Tier 2 reaches into iroh's
   internals (RemoteInfo, metrics, home-relay watcher). The wrapping
   layer should be one file so iroh API churn has a local blast
   radius. Worth a brief check that the surfaces we want exist in
   iroh 0.96 specifically before tier 2 work begins.

6. **Probe targets for T3.3.** "Baseline UDP probe to a known target"
   needs a target. Options: a public STUN server (free, occasionally
   flaky), our own echo service (more reliable, infrastructure cost).
   Since we're already operating a VPS for the collector, running a
   tiny UDP echo there is essentially free — recommend co-locating
   the echo service with the collector and using it as the baseline
   target. STUN remains useful as a NAT-type probe; keep both.

7. **Tier 3.1 conntrack capability.** Capturing conntrack count
   needs `CAP_NET_ADMIN` or equivalent, which vast.ai containers may
   not have. Capture best-effort and null out when unavailable;
   don't gate the rest of T3.1 on it.

---

# Implementation Order

Suggested landing order — each step produces a usable artifact:

1. T1.1 (identity), T1.6 (collector), T1.7 (tarball) — minimum
   plumbing.
2. T1.2 (reachability log) + T1.3 (event stream) + T1.4 (snapshot
   fan-out) — tier 1 functionally complete.
3. T1.5 (clock alignment) + A.3 (post-processor v1 producing the
   one-pager from tier-1 data).
4. **Re-run vast.ai N=3.** Read the bundle. Most of the
   `VASTAI_STATUS.md` questions should now be answerable.
5. T2.1 (RemoteInfo) + T2.2 (home-relay watch) + T2.4 (cache
   lifecycle) — closes the iroh-side mystery class.
6. T2.3, T2.5, T2.6, T2.7 — fills in the rest of tier 2.
7. T3.3 (probes) — second-most-valuable tier-3 collection.
8. T3.1, T3.2, T3.4, T3.5 — environmental context.

The bet is that re-running vast.ai after step 4 will sharply narrow
the remaining work. Some of tier 2 / 3 may turn out to be unneeded
once we can read the tier-1 bundle from a real failure.
