# Observability surface — the parity contract

> Companion to [`NORTH_STAR.md`](./NORTH_STAR.md) and [`SPEC.md`](./SPEC.md).
> NORTH_STAR establishes that the simulator must be **observably
> equivalent** to production: every internal observation a process
> makes — retry counts, peer-state distributions, timer firings,
> transport stats, queue depths, tails as well as means — must be
> statistically indistinguishable from the corresponding observation
> in a real deployment under matched conditions. This document
> enumerates the surface against which "matched" is measured, both
> what is recorded today and what is in the near pipeline. The
> simulator's SPEC ties its obligations to this enumeration; if
> something in here is unimplemented in the sim, the sim is
> incomplete; if something is unimplemented in production recording,
> the recording is incomplete. Extending the two is the same project.

## Table of contents

1. Why this doc exists
2. How to read it
3. Surface today
   3.1 Identity
   3.2 Per-peer reachability log
   3.3 Structured event stream
   3.4 Snapshot envelope
   3.5 Clock-alignment samples
   3.6 Tier-2 iroh introspection
   3.7 Tier-2 SWIM introspection
   3.8 Tier-3 host / DNS state
   3.9 Tier-3 outbound probes
   3.10 Tier-3 vast.ai context
   3.11 Tier-3 process resource stats
   3.12 Bundle layout
4. Surface in the near pipeline
   4.1 Wire-level packet trace
   4.2 Causal trace IDs
   4.3 Per-flow congestion-control state
   4.4 Application observables (pipeline-parallel)
   4.5 Per-actor mailbox and scheduler state
   4.6 NAT-binding lifecycle
   4.7 Cryptographic handshake records
   4.8 Per-link bandwidth and loss telemetry
5. Out of scope (not in the floor)
6. Parity rules of the road

---

## 1. Why this doc exists

The simulator's job is not to imitate a network. It is to be
indistinguishable from one across whatever observation channels the
process has. The channels are not abstract: they are the records the
diagnostics stack writes to disk. As long as those records are the
only thing downstream tooling consumes, "the sim is faithful" reduces
to "the sim emits the same records, with the same distributions, as
production does."

Three properties follow:

- **The recording schema is the contract.** Adding an observation
  channel in production without simultaneously committing the sim to
  emit it is a parity failure — by NORTH_STAR §3, that asymmetry is
  always a bug. So every record kind enumerated here is either (a)
  already produced by both, or (b) committed to be produced by both
  in the same change.

- **Distributions, not just shapes.** Schema parity is necessary,
  not sufficient. The sim must reproduce the *tails* of every
  numeric field — latencies, retry counts, packet sizes — to within
  the noise floor of the real measurement. A schema match that
  papers over a tail-shape mismatch is a parity failure.

- **No back-channel introspection.** A sim-only diagnostic
  (queue-depth-inside-the-sim, deterministic-RNG-trace) is fine and
  useful, but must not be relied on by any code that also runs in
  production. The node-under-test must not be able to tell which
  runtime it is on. Sim-only diagnostics live next to, not inside,
  the production recording surface.

## 2. How to read it

Each subsection below describes *one record kind*. For each:

- **Source of truth** — the type or file in `crates/distribution`
  (today) or the location the future implementation will land.
- **Trigger** — when the record is emitted (boot, periodic, event-
  driven, on-demand).
- **Fields** — the observable signal carried. The level of detail
  matches what the post-processor (or any future analyzer) reads.
- **Sim obligation** — the specific surface the sim must reproduce
  for this record, including which fields require *distributional*
  parity vs. mere presence.

Section 3 covers what is in the wire today; the schemas there are
authoritative against the source files cited. Section 4 covers
what is committed but not yet landed; the schemas there are
indicative — the source files settle final field names.

---

## 3. Surface today

The numbered subsections below mirror the tiers from
`examples/pipeline-parallel-inference/DIAGNOSTICS_PLAN.md`. Each is
implemented in `crates/distribution/src/diagnostics/*` and the
record types are stable enough to bind the sim to.

### 3.1 Identity

**Source of truth.** `diagnostics::identity::Identity`. Re-emitted
in the header of every snapshot.

**Trigger.** Boot; re-embedded into every subsequent record so any
single record in the bundle is interpretable on its own.

**Fields.** `node_id_hex`, `node_id_short`, `role`, `stage_index`,
`stage_count`, `run_id`, `vastai_contract_id`, `host_ip_public`,
`host_country`, `datacenter_id`, `hostname`, `container_id`,
`process_start_unix_ms`, `boot_sequence`, `binary_version`,
`git_sha`, `iroh_version`, `home_relay_url_at_boot`.

**Sim obligation.** Every sim-hosted node must construct an
`Identity` with the same shape. `node_id` is generated by the sim
(deterministically, from seed + name). Environmental fields
(`vastai_contract_id`, `host_country`, `datacenter_id`,
`host_ip_public`) take values from the sim's topology spec rather
than the real metadata — they are *parameters* of the run, not
captured observations, but appear in the same field. `git_sha` and
`binary_version` carry the same values they would in prod (the
sim does not stub them out).

### 3.2 Per-peer reachability log

**Source of truth.** `diagnostics::reachability::PeerReachability`.
One entry per remote peer, maintained continuously in the
aggregator and emitted as part of every snapshot.

**Trigger.** In-memory, continuous. Serialized at every snapshot.

**Fields.** `peer_node_id_hex`, `last_inbound_packet_at_ms` +
`via_relay_url`, `last_outbound_success_at_ms` + `via_relay_url`,
`last_dial_started_at_ms`, `last_dial_outcome`,
`last_dial_duration_ms`, `current_swim_opinion`,
`current_swim_opinion_since_ms`, `swim_transition_history`
(bounded ring of ~32 `StateTransition`),
`metadata_version_seen`, `metadata_relay_url_seen`.

**Sim obligation.** The sim's transport layer must drive the same
log. Distributional parity required on
`last_dial_duration_ms` (must match the per-link RTT model);
`current_swim_opinion_since_ms` (must follow the same suspect
timer dynamics); the rate of `swim_transition_history` entries
under partition and heal.

### 3.3 Structured event stream

**Source of truth.** `diagnostics::event::Event` and `EventRecord`.
Append-only stream batched to the collector every ~1s and
immediately on state transitions.

**Trigger.** Event-driven. Each variant is fired by the subsystem
that observes the event (transport for `Dial*` / `Message*` /
`ConnectionCache*` / `IrohConnTypeChanged` / `RelayChanged` /
`NodeMapUpdate`; SWIM for `SwimTransition` / `SwimMetadata*`;
probe scheduler for `Probe*`; any subsystem for `Error` / `Custom`).

**Variants (minimum set).**

| Variant | Fired by | Carries |
|---|---|---|
| `SwimTransition` | SWIM | `peer`, `from`, `to`, `reason` |
| `DialStarted` | transport | `peer`, `attempt`, `timeout_ms` |
| `DialOutcome` | transport | `peer`, `attempt`, `outcome`, `duration_ms` |
| `IrohConnTypeChanged` | transport | `peer`, `old`, `new` |
| `RelayChanged` | transport | `old_url`, `new_url` |
| `SwimMetadataSent` / `Received` | SWIM | `version`, `payload_hash` (+ `peer` on Received) |
| `ConnectionCacheHit` / `Miss` / `Invalidated` | transport | `peer`, `generation`, `reason?` |
| `NodeMapUpdate` | transport | `peer`, `from_source`, `accepted` |
| `MessageSent` / `Received` | transport | `peer`, `kind`, `size` |
| `ProbeSent` / `Received` | probes | `target`, `kind`, `rtt_ms?`, `outcome` |
| `Error` | any | `component`, `message`, `peer?` |
| `Custom` | any | `kind`, `fields` (arbitrary JSON) |

The wrapping envelope (`EventRecord`) carries `node_id`,
`monotonic_seq` (per-process, never decreases), and best-effort
`wall_ms`.

**Sim obligation.** Every variant fires from the sim-side transport
and SWIM implementations at the same points it fires in prod.
Distributional parity required on:

- `DialOutcome.duration_ms` per outcome class.
- `ConnectionCacheInvalidated` rate per session.
- `MessageSent` / `MessageReceived` size and inter-arrival
  distributions per `kind`.
- `SwimTransition` cadence under each modelled fault scenario.

`monotonic_seq` is the sim-internal causal order; `wall_ms` is
populated from virtual time and post-aligned via §3.5 just as in
prod.

### 3.4 Snapshot envelope

**Source of truth.** `diagnostics::snapshot::Snapshot` /
`SnapshotBody` / `SnapshotTrigger`.

**Trigger.** Periodic (default 5s), on local SWIM transition
(`SnapshotTrigger::Transition`), or on collector hint
(`SnapshotTrigger::OnDemand`).

**Fields.** Identity (re-embedded), `run_id`, `snapshot_id`,
`wall_ms`, `monotonic_seq`, `trigger`, and a `SnapshotBody` with
`reachability` (always), an `events` tail since the last snapshot,
and `Option` blocks for `iroh` / `swim` / `host` / `probes` /
`vastai` / `process` (populated if the corresponding introspector
is installed).

**Sim obligation.** Sim-hosted nodes emit `Snapshot` records on the
same triggers. The periodic cadence is a *sim parameter* — the sim
honors whatever the run config sets, just as prod honors whatever
its periodic config sets. The `SnapshotTrigger` distribution
(what fraction of snapshots come from periodic vs. transition
vs. on-demand) must match the real-run distribution for the
matched scenario.

### 3.5 Clock-alignment samples

**Source of truth.** Sink handshake — every POST carries
`node_send_ms`; the response carries
`{node_send_ms_echoed, collector_recv_ms, collector_send_ms}`; the
node records its `wall_ms_at_receive`. Each round is logged as a
structured event so the post-processor has a per-node offset
stream over the run's lifetime, not a one-shot calibration.

**Trigger.** Every collector POST.

**Sim obligation.** The sim hosts a sim-side collector; its
"wall clock" is virtual time. Sim runs emit `clock_sample` events
with the same shape so the post-processor's alignment code is
unchanged across sim and prod. In sim, alignment error is zero by
construction — that *is* the answer the parity check expects, not
a sign of missing telemetry.

### 3.6 Tier-2 iroh introspection

**Source of truth.** `diagnostics::snapshot::Tier2IrohState`,
populated by `IrohIntrospector` (production implementation:
`diagnostics::iroh_introspect::IrohIntrospect`).

**Trigger.** Captured on every snapshot when an introspector is
installed.

**Fields per peer (`Tier2Peer`).** `peer_node_id_hex`, `conn_type`
(Direct / Relay / Mixed / None), `latency_ms`, `last_used_ms`,
`last_received_ms`, `direct_addresses` (each
`TransportAddrWire{addr, usage}`), `relay_urls`, `addr_sources`.

**Top-level fields.** `home_relay_url`, `peers`, `metrics`
(`Vec<MetricSample>` — flat dump of `iroh-metrics` counters /
gauges / histograms), `connection_cache` (per-peer
`Tier2ConnectionCache`: `generation`, `created_at_ms`,
`last_successful_send_at_ms`, `last_failure_at_ms`,
`last_failure_reason`, `observed_conn_type_at_last_use`),
`api_gaps`, `scraped_at_ms`.

**Sim obligation.** The sim's iroh-equivalent transport produces a
`Tier2IrohState` with the same field set populated. Where the
real iroh exposes nothing (today: `latency_ms`, `last_used_ms`,
`last_received_ms`, `addr_sources`), the sim *also* leaves those
`None` and lists them in `api_gaps`. Distributional parity
required on `conn_type` evolution per peer (the rate of Direct ↔
Relay flips under NAT churn), and on the deltas of every metric
in `metrics` across the run.

### 3.7 Tier-2 SWIM introspection

**Source of truth.** `diagnostics::snapshot::Tier2SwimState`,
populated by `SwimIntrospect`.

**Fields.** `config` (`Tier2SwimConfig`: probe / probe-timeout /
suspicion-timeout / dead-reprobe-interval ticks, indirect-probes-k,
gossip fanout Λ, max-piggyback, probe-mode string),
`self_node_id_hex`, `self_incarnation`, `metadata_local_version`,
`peers` (per-peer `Tier2SwimPeer`: state, incarnation,
last_ping_sent / last_ack_received / last_ping_received /
suspect_started timestamps, `metadata_version_seen`),
`recent_messages` (bounded ring of ~64 `Tier2SwimMessage`:
`{kind, peer, at_ms, sequence?}`), `scraped_at_ms`.

**Sim obligation.** The simulated SWIM emits this snapshot block
with `Tier2SwimConfig` echoing the configured parameters of the
sim run and `peers` reflecting the simulator's authoritative
membership view. The `recent_messages` ring matches the same cap
and ordering rule (oldest first). Distributional parity required
on suspect-timer durations and incarnation churn under modelled
fault scenarios.

### 3.8 Tier-3 host / DNS state

**Source of truth.** `Tier3HostState` / `Tier3HostNetwork` /
`Tier3DnsResolution`, populated by `HostIntrospect` at boot and on
~30s refresh.

**Host-network fields.** `interfaces` (`name`, `addresses`, `mtu`,
`up`), `default_routes` (`family`, `destination`, `gateway`,
`interface`), `udp_sockets` (rows from `/proc/net/udp{,6}`:
`local_addr`, `remote_addr`, `state`, `inode`),
`conntrack_count`, `ipv6_enabled`, `resolv_conf_nameservers`,
`refreshed_at_ms`.

**DNS fields.** Per known relay URL: `hostname`, `a_records`,
`aaaa_records`, `ttl_seconds`, `resolver_used`, `resolved_at_ms`,
`error`.

**Sim obligation.** Sim-hosted nodes emit a `Tier3HostState`
populated from the simulated host's modelled environment: virtual
interfaces (with whatever `mtu` / `up` the topology spec assigns),
modelled default routes, the modelled UDP-socket table (every
`bind()` the sim drives produces a row), modelled DNS results
(controlled by the sim's DNS model — see §4.6 in SPEC). Fields the
sim does not model (e.g. `conntrack_count` if the conntrack model
is not yet in) follow the prod convention: emit `None` and log a
single `Error` event noting the gap.

### 3.9 Tier-3 outbound probes

**Source of truth.** `Tier3ProbeState` / `Tier3Probe`, populated by
`ProbeScheduler`.

**Fields.** Per target: `target`, `kind` (`"udp_echo"`,
`"udp_relay"`, `"stun"`), `resolved_addr`, `last_attempted_at_ms`,
`last_outcome` (`"ok"`, `"timeout"`, `"refused"`, `"error"`,
`"unresolved"`), `last_rtt_ms`, `last_error`, `attempts`,
`successes`.

**Sim obligation.** The sim drives the probe scheduler exactly the
way prod does — registering targets at boot and letting the
scheduler run on its ~10s cadence. The sim's UDP stack accepts /
drops / delays probe packets according to its link physics, and
the resulting `Tier3Probe` block must show the same outcome
distribution per modelled link condition.

### 3.10 Tier-3 vast.ai context

**Source of truth.** `Tier3VastaiContext`, captured once at boot
by `VastaiContext::capture_now()`.

**Fields.** `container_id`, `hostname`, `env_vars` (filtered set
of `VAST_*` / `VASTAI_*` / `CONTAINER_*` / `CUDA_*` / `NVIDIA_*`,
with secret-bearing keys dropped), `process_start_ms`,
`captured_at_ms`.

**Sim obligation.** In sim runs the block is populated with the
*modelled* vast.ai context — whatever the topology spec assigns
each node, with the same key prefixes. If the sim run is not
modelling a vast.ai-style environment, the block is absent; this
matches the prod convention (orchestrator on a laptop, no
container, no block).

### 3.11 Tier-3 process resource stats

**Source of truth.** `Tier3ProcessStats`, populated by
`ProcessStats`.

**Fields.** `rss_bytes`, `vm_size_bytes`, `open_fd_count`,
`cpu_ms`, `tokio` (`Tier3TokioStats { flavor }`), `captured_at_ms`.

**Sim obligation.** The sim cannot directly produce RSS / VmSize
the way `/proc/self/status` does, because the sim hosts many nodes
in one OS process. Field-level approximation: the sim populates
these fields from its modelled per-node accounting (see SPEC §4.8
"host budget"). Where there is no model (e.g. open_fd_count is
not modelled today), the field is `None`, matching the
"absent ≠ zero" rule.

### 3.12 Bundle layout

**Source of truth.** Collector binary
(`diagnostics::collector::*`) and bundle assembler
(`postproc::tar`). See `DIAGNOSTICS_PLAN.md` §"Bundle Format" for
the on-disk shape.

**Sim obligation.** A sim run produces a bundle byte-for-byte
indistinguishable from a prod bundle of the same workload, given
the same `run_id`. The bundle is the artifact the calibration loop
diffs against.

---

## 4. Surface in the near pipeline

These items are not in the wire today but are within reach of the
next round of recording work. Each is listed so the sim's SPEC can
commit to producing it the moment prod recording lands it (and
vice versa — committing here means the sim is not allowed to lag
once production starts emitting).

### 4.1 Wire-level packet trace

**Need.** Today's records describe message-level activity
(`MessageSent` / `MessageReceived` count and size by `kind`).
That granularity hides protocol-internal behavior: QUIC frame
fragmentation, ACK timing, retransmits, MTU-driven path-MTU
probes. Calibration runs hit a ceiling: when the sim's
`MessageReceived` distribution matches prod's, but flow control
inside QUIC behaves differently, the discrepancy is invisible.

**Shape (proposed).** A `WirePacket` record stream:

```
{ node_id, monotonic_seq, wall_ms,
  direction: "tx" | "rx",
  l3: { src_ip, dst_ip, ttl, ecn },
  l4: { kind: "udp", src_port, dst_port, len },
  payload_hash: u64,
  payload_len: u32,
  via: { local_iface, peer_relay_url? } }
```

Payloads themselves are not stored by default; `payload_hash`
suffices for stitching a sim run against a prod run for the same
workload. A "full capture" mode (gated, off by default) keeps the
bytes for offline diffing.

**Sim obligation.** The sim emits a `WirePacket` per modelled
packet event. Distributional parity required on inter-packet
gap per `(src_port, dst_port)` flow.

### 4.2 Causal trace IDs

**Need.** Stitching is currently done by clock alignment + per-peer
reachability matching. That works for "did A go dead before B's
dial timed out" but fails for richer causality (e.g., "this
SWIM-piggybacked metadata update is the one that triggered the
NodeMapUpdate seven hops later"). A trace ID threaded through
each message makes causality direct.

**Shape (proposed).** Every `MessageSent` records a `trace_id`
(opaque 128-bit) and a `parent_trace_id`. Each `MessageReceived`
echoes the same `trace_id`. Subsystems that derive new work from
a received message (SWIM digest → SwimMetadataSent, transport
NodeMapUpdate, etc.) thread `trace_id` into their own emitted
events as `parent_trace_id`.

**Sim obligation.** The sim threads the same trace IDs through
its transport and SWIM implementations. Determinism extends to
the IDs: same seed, same trace IDs (derived from the in-process
deterministic RNG; see SPEC §2.1).

### 4.3 Per-flow congestion-control state

**Need.** Network-condition parity demands more than RTT and loss
rate — the *response* to those conditions (cwnd evolution,
retransmit timer, RTT estimator) is part of what production
observes (and so must be part of what the sim observes). Today
we infer congestion behavior from message size and timing; we
should record it directly.

**Shape (proposed).** Inside `Tier2IrohState`, a new `flows` array:

```
{ peer_node_id_hex, local_addr, remote_addr,
  cwnd_packets, rtt_estimate_ms, rtt_variance_ms,
  bytes_in_flight, retransmits_total,
  packet_loss_rate_recent }
```

**Sim obligation.** The sim's QUIC implementation exposes the same
state. Distributional parity required on `cwnd_packets` and
`rtt_estimate_ms` evolution under each modelled link condition;
on the cumulative `retransmits_total` per minute of run.

### 4.4 Application observables (pipeline-parallel)

**Need.** The diagnostics layer captures everything *under* the
application. Calibration of an inference deployment also needs
the application-level surface: per-stage activation transfer
timing, KV-cache pressure, per-token latency, worker queue
depths. Today these are local logs at best.

**Shape (proposed).** A new `Custom` event family with
well-known `kind` values, plus a tier-3 application block:

| Event `kind` | Carries |
|---|---|
| `pp.token_emitted` | `request_id`, `position`, `token_id`, `decode_ms` |
| `pp.activation_sent` | `request_id`, `from_stage`, `to_stage`, `bytes`, `is_prefill` |
| `pp.activation_received` | mirror of `_sent` |
| `pp.worker_dispatch` | `op`, `request_id`, `queue_depth_at_dispatch` |
| `pp.worker_complete` | `op`, `request_id`, `duration_ms` |

Plus a `Tier3AppState` block in `SnapshotBody`:

```
{ stage_index, worker_status,
  in_flight_requests: u32,
  kv_cache_blocks_used: u32, kv_cache_blocks_max: u32,
  worker_queue_depth: u32,
  gpu_mem_used_bytes?, gpu_mem_total_bytes? }
```

**Sim obligation.** Sim-native pipeline stages drive these events
through the same crate. Where the sim does not model the GPU
(today's case), `gpu_mem_*` is `None` and the gap is logged once.
Distributional parity required on `decode_ms` and `bytes`
distributions per `from_stage→to_stage` link.

### 4.5 Per-actor mailbox and scheduler state

**Need.** Tokio-runtime stats today report only the runtime flavor
(stable surface). Inside the actor framework, mailbox depth and
per-actor latency are observable but uncaptured. Pipeline stalls
caused by mailbox backpressure or executor starvation look
identical to network failures from outside.

**Shape (proposed).** A `Tier3ActorState` block:

```
{ actors: [{ name, mailbox_depth, oldest_msg_age_ms,
             total_msgs_processed, total_msgs_dropped,
             p50_process_ms, p99_process_ms }],
  scheduler: { task_count, oldest_pending_task_age_ms } }
```

**Sim obligation.** The sim-side actor runtime exposes the same
counters per actor. Distributional parity required on
`mailbox_depth` time series and on `p99_process_ms` under load.

### 4.6 NAT-binding lifecycle

**Need.** "Asymmetric routing" and "relay flapped" today get
diagnosed by stitching `RelayChanged`, reachability log, and
host scrape together. A direct NAT-binding observation would
collapse that to one record. Even without conntrack on every
host, the path-discovery layer in iroh has visibility into
when a binding was last refreshed.

**Shape (proposed).** Inside `Tier2IrohState`, a new `nat_bindings`
array:

```
{ local_addr, observed_remote_addr,
  first_seen_ms, last_refreshed_ms,
  refresh_count, keepalive_interval_ms }
```

**Sim obligation.** The sim's NAT model produces the same record.
Distributional parity required on `last_refreshed_ms` interval
under each modelled NAT class (cone / symmetric / pmp / upnp).

### 4.7 Cryptographic handshake records

**Need.** Handshake-level observations are part of the wire today
(MagicSock holepunch + QUIC TLS); they show up in metrics as
counter increments only. A direct record of each handshake
makes connection-establishment bugs first-class.

**Shape (proposed).** A new event family:

```
HandshakeStarted   { peer, role: "client" | "server",
                     local_addr, remote_addr, via_relay?, cipher_suite_offered }
HandshakeOutcome   { peer, outcome: "success" | "timeout" | "rejected" | "error(string)",
                     duration_ms, cipher_suite_selected? }
```

**Sim obligation.** The sim's transport emits both events on every
modelled handshake. The set of `cipher_suite_offered` and the
`cipher_suite_selected` distribution match what the real
transport would.

### 4.8 Per-link bandwidth and loss telemetry

**Need.** The sim's link physics (SPEC §4.2) is parameterized;
production today does not record link-level bandwidth or loss
directly. The path-forward is symmetric: the sim records its
*applied* link parameters as a snapshot field (so a replay knows
what the link was set to), and prod adds passive estimation of
the same parameters from observed flow behavior.

**Shape (proposed).** Inside `Tier2IrohState`, a `links` block:

```
{ peer_node_id_hex,
  bandwidth_bps_observed: u64,
  loss_rate_observed: f32,
  one_way_delay_ms_estimated: u64,
  measurement_window_ms: u64 }
```

For sim runs only, an additional `links_applied` block records
the configured ground truth — used by the post-processor to
quantify the gap between *applied* (sim) and *observed* (sim or
prod) link parameters.

**Sim obligation.** Drive the `links` block from the same
estimation code prod uses, plus the `links_applied` block from
the topology spec. Calibration validates that
`bandwidth_bps_observed` converges to `links_applied.bandwidth`
within the sim, and that prod-side estimation tracks it on real
deployments.

---

## 5. Out of scope (not in the floor)

The following are explicitly **not** part of the parity surface in
the current planning horizon. The sim is not obligated to
reproduce them; production is not obligated to record them. If
that changes, this list moves into §4 first, then into §3.

- Kernel-level system call traces (eBPF / strace).
- Allocator events (per-allocation timing, heap fragmentation).
- Dynamic linker / LD_PRELOAD-style observations.
- PTP / GPS clock sync internals.
- GPU compute-kernel internals beyond aggregate memory usage.
- CPU performance counters (cache misses, branch prediction).
- Per-thread scheduler decisions below the tokio-task level.

The line is drawn at "what an unprivileged userspace process can
observe about itself or the network it touches." Anything below
that line is outside the model floor today.

---

## 6. Parity rules of the road

1. **No silent gaps.** Every record kind in §3 is either populated
   or explicitly marked absent (`None` + an `Error` event noting
   the gap). A missing field that the reader cannot distinguish
   from "the value happens to be zero" is a parity bug.

2. **Schema match is the floor, distribution match is the bar.**
   The presence of a field is necessary but not sufficient. Each
   subsection above calls out which fields require *distributional*
   parity (matching tails, not just means) under matched scenarios.

3. **Symmetry between recording and simulation.** Both directions
   are bugs. A field prod records that the sim cannot emit is a
   sim incompleteness. A field the sim emits that prod cannot
   record is a recording incompleteness. The fix is always to
   extend both together — never to drop a field from one to
   restore symmetry.

4. **Determinism is the sim's superpower.** The sim is allowed
   (and expected) to expose *additional* records that have no
   prod equivalent — RNG traces, scheduler decision logs, exact
   virtual-time tick streams. These are sim-only diagnostics,
   never visible to the node-under-test, and never required for
   prod parity. They live in a separate tier (sim §8.2 in SPEC)
   so the parity check does not have to reason about their
   absence in prod runs.

5. **Calibration is the judge.** Whether §3 and §4 are correctly
   implemented is not decided in this doc. It is decided by the
   calibration loop (SPEC §9): deploy, collect the bundle, replay
   in sim, diff every record kind enumerated above. Divergence
   beyond the noise floor is a bug in this doc, in the sim, or in
   the recording — exactly one of the three.
