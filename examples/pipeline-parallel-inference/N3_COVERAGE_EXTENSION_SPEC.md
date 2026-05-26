# N=3 collection coverage extension — behavioral spec

Companion to `N3_POSTMORTEM_2026-05-25_1779733878.md`,
`N3_SIM_TEST_BATTERY_SPEC.md`, and the simulator's `SIM_SPEC.md`.
This document is the contract for a separate coding agent to extend
diagnostic collection coverage along three layers — **production
diagnostics**, **simulator emit/model**, and **simulator test
verification** — for the gaps the `1779733878` run surfaced.

This is a *behavioral* spec. It names the gap, the contract the
collected data must satisfy, and the layer(s) the contract threads
through. It does not prescribe field names, file layout, or
implementation choices.

---

## 0. Motivation

The `1779733878` run validated the prior observability upgrade —
A+B+C tiers were load-bearing, the bundle attributed the failure to
"dials to orchestrator fail by timeout 7/11 while every inter-stage
dial succeeds 19/19" in one table — and surfaced five residual
collection gaps the upgrade either left as carry-forward (gaps 1, 5,
8 from the original scorecard) or that this run exposed for the
first time (response-leg event absence, bundle-serve behavior under
run-id reuse).

A gap whose collection landed only in production but not in the sim
is a gap that the sim test battery can never guard — the next regression
in that field will be caught only by another live deploy. A gap whose
sim model exists but is not exercised by a test is dead code. The
coverage in this spec is required to thread through every layer
where it can — and the spec is explicit when a layer does not
apply.

Five coverages, each threaded through up to three layers. Each
coverage may close one of {Pass, Mixed, Fail} against the current
source, and each names the close criterion.

---

## 1. Cross-cutting requirements

These hold for every coverage in §2.

### 1.1 Three-layer threading

For each coverage, the spec names which of the three layers it
threads through:

- **D — Diagnostics**: the production bundle gains a field, event,
  or section that closes the gap the postmortem named.
- **S — Sim**: the simulator's relevant component (host kind,
  network, relay vertex, bundle writer) emits the same field /
  event / section under the same conditions, with bundle-shape
  parity per `SIM_SPEC.md §5` (cross-cutting "Bundle-shape parity
  with prod") and §9 (bundle schema).
- **T — Tests**: the sim test battery gains a scenario or property
  test asserting the bundle carries the new data when the
  triggering condition holds, and gains a discriminator assertion
  when absence is meaningful (per the honesty-under-absence pattern
  the prior upgrade established).

A coverage that threads through fewer than three layers is honest
about which it skips and why. Skipping S because "the simulator
does not model this surface" is acceptable; skipping it because
"this is not interesting" is not.

### 1.2 Honesty-under-absence carries forward

The prior upgrade's `status_source` discriminator pattern (a status
field always paired with a field naming how that status was derived
— `"iroh"` for native, `"derived"` for inferred) is the model. Any
new field whose value might be absent or derived must carry an
adjacent discriminator. A bundle reader must never be left guessing
"unknown means the thing is unknown" vs "we couldn't ask."

### 1.3 Additive evolution

Every new field on `SnapshotBody`, every new event variant, every
new section in the post-processor output is additive. An old bundle
reader on a new bundle still parses; a new bundle reader on an old
bundle reports the new field absent rather than erroring. The
prior upgrade established this contract; coverage 2.x preserves it.

### 1.4 Sim/prod schema parity

Per `SIM_SPEC.md §9.2`: the event payload schema is exactly the
production diagnostics schema for that kind. The sim invents no new
event kinds. A coverage that lands an event in prod and in sim
**uses the same schema in both**, verified by the existing parity
tests under `crates/simulation/tests/sim_cross_pollination.rs`. A
schema added to sim ahead of prod is a deliberate amendment and
declares so explicitly.

### 1.5 Verdict-first per layer

Every coverage in §2 declares, per layer, its expected status on
the current source: **landed** (the layer satisfies the contract;
the work is verification / regression-guard), **partial** (the
layer has structure but not data flow), **absent** (the layer has
nothing today). The implementing agent's work is to bring each
layer to "landed" against this spec or to file a structural reason
why a layer cannot land.

---

## 2. The coverages

Five, ordered by the postmortem's own ranking of residual gaps.

### 2.1 Orchestrator-side host provider metadata forwarding

**Source**: postmortem §"Observability upgrade scorecard" row "gap
5 host metadata" (◐); postmortem §"Data-collection / deployment
gaps surfaced by this run" item 1.

**Gap**: The orchestrator has each rental's public IP, datacenter,
country, and contract id at `lease_chain` return time. The
container can read these from `SWACTOR_DIAG_*` env vars. The
container env is never set. The boot record's
`host_ip_public`/`datacenter_id`/`host_country`/`vastai_contract_id`/
`home_relay_url_at_boot` fields are still null in every bundle.
The contract id arrives but in `container_id`, not
`vastai_contract_id` — the naming is currently load-bearing-but-wrong.

**Contract — what closing the gap looks like**:

- D: the orchestrator's per-rental env payload, at the point it
  creates each container, carries every field the boot record can
  consume — public IP, datacenter id, host country, vast.ai
  contract id, the home relay URL the container will use. The boot
  record reflects every field as a concrete value, not `null`,
  whenever the orchestrator had the data. The fields that name
  cloud-provider state stay absent only on hosts where they
  genuinely do not apply (e.g., local development), and the
  bundle's `## Hosts` section renders `?` for absent fields
  (already implemented per `S-A2`).
- S: scenarios declare per-peer host context as part of the peer's
  `kind_config`. The sim's stage host populates its boot record /
  `HostContext` from the scenario declaration the same way prod
  populates from env. A scenario without declared host context
  produces a bundle whose `## Hosts` section is all-`?` for that
  peer — same absence shape as a local-dev prod bundle.
- T: a scenario declaring heterogeneous host context across three
  peers (e.g., two datacenters, two countries) produces a bundle
  whose `## Hosts` section renders the declared fields verbatim.
  A scenario that declares no context for one peer and full context
  for the others produces a bundle distinguishable from "no context
  declared for any peer" by the `?` placement.

**Expected status**:

- D: partial. The container reads the env vars (per `S-A2`); the
  orchestrator does not set them. The misnaming of contract id →
  `container_id` is a separate cleanup.
- S: absent. The sim's stage host carries no host context in its
  current scenario schema.
- T: absent. No test exercises this discriminator.

**Close criterion**: a deployed bundle's `## Hosts` section names
the datacenter, country, public IP, and contract id of every
vast.ai rental, and the docker `container_id` field carries the
docker container id, not the vast.ai contract id. A sim bundle
with declared host context produces the matching shape.

### 2.2 Relay-port reachability probe

**Source**: postmortem §"Observability upgrade scorecard" row "gap
8 relay-port probe" (✗); postmortem §"Data-collection / deployment
gaps surfaced by this run" item 3.

**Gap**: Stage probe arrays carry only `collector_udp_echo`
(:9081). No probe targets the relay's actual port (:7843).
Whether a stage retained transport-level reachability to the relay
at the moment its peer-connection died is currently inferable only
from a *different* port on the same host. The `S-E1` work is
documented as landed (per the prior iteration log) but the
`1779733878` bundle shows no relay-port probe records. The wiring
is in place; the data is not.

**Contract — what closing the gap looks like**:

- D: every stage's snapshot carries a probe outcome for the
  relay's UDP listener (host + port resolved from the home relay
  URL). The outcome is one of the five-discriminator set the
  prior upgrade established: `ok` / `timeout` / `refused` /
  `unresolved` / `error`. A snapshot taken when the relay is
  reachable carries `ok` with an RTT; a snapshot taken when the
  relay is unreachable carries the appropriate failure
  discriminator with no silent fallback to "absent."
- S: the sim's stage host emits the same probe record on every
  snapshot, sourced from a query the network answers about the
  stage→relay edge. The relay vertex's `RelayKill` /
  `RelayCapacityChange` mutations are reflected in the probe's
  outcome distribution.
- T: a scenario that issues a `RelayKill` mutation mid-run
  produces a bundle whose every stage's relay-port probe outcome
  flips from `ok` to `unresolved` (or `timeout`, per the
  network's policy) at the mutation's `at_ns` and remains there
  through `RelayBoot`. The probe-outcome timeline is the test's
  discriminator between "tunnel down" and "tunnel up but peer
  conn down" — coverage 2.x.A from the battery spec consumes
  this signal.

**Expected status**:

- D: partial. Probe scheduler wires the target; emission to the
  bundle is unverified by this run's evidence.
- S: absent. The sim's network has no probe-query surface today.
- T: absent.

**Close criterion**: the next deployment's bundle has a
relay-port probe outcome on every stage's snapshots. A sim
scenario with `RelayKill` produces the probe-outcome flip in the
bundle.

### 2.3 Relay session lifecycle on the relay side

**Source**: postmortem §"Observability upgrade scorecard" row "gap
1 relay observability" (◐); postmortem §"Data-collection /
deployment gaps surfaced by this run" item 4.

**Gap**: The relay reports identity and 186 snapshots into the
bundle but cannot answer "who closed session X and why" — the
per-session lifecycle hooks are the documented skeleton with
`active=0 opens=0 closes=0`. `iroh_relay::server` exposes no
session hooks. Until it does, a relay-side eviction is
unanswerable from the relay's own data; the postmortem fell back
to node-side dial outcomes.

**Contract — what closing the gap looks like**:

- D: the relay's bundle contribution names, per peer session, the
  open time, close time, close-initiator discriminator
  (`relay` / `peer` / `transport` / `unknown`), close reason
  string (relay-specific or transport-specific), bytes
  transferred per direction, and duration. The mechanism is
  free — middleware around the relay binary, kernel-layer
  observation, a forked relay, or upstream hooks when iroh
  exposes them. The contract is the *shape*, not the source.
  When the source is unavailable, the relay's bundle
  contribution still emits the gap-1 absence-line the prior
  upgrade introduced in `summary.md` (the post-processor's
  acceptance branch for "no relay-role node has session data").
- S: the sim's relay vertex emits `RelaySessionOpened` /
  `RelaySessionClosed` records when it accepts and releases
  per-peer queues. The records carry the same shape D
  requires. A `RelayKill` mutation produces a
  `RelaySessionClosed { initiator: "relay", reason: "killed",
  ... }` for every session active at the mutation time.
- T: a scenario where the relay accepts three peer sessions, runs
  to steady state, then receives a `RelayKill` mutation,
  produces a bundle whose relay contribution names three
  `RelaySessionOpened` events at the convergence boundary and
  three `RelaySessionClosed { initiator: "relay" }` events at
  the mutation time. A scenario where a peer voluntarily
  disconnects produces a session-closed event with
  `initiator: "peer"`. The discriminator must hold.

**Expected status**:

- D: skeleton — wired call sites, no data flow. Whether the
  unblock path is upstream hooks, middleware, or kernel
  observation is implementer's call.
- S: partial. `RelayObservability` exists on the host side per the
  prior upgrade (`S-B1`); the sim's relay vertex itself does not
  emit lifecycle events as engine-synthesized records.
- T: absent.

**Close criterion**: a deployed bundle from a run that included a
peer dial failure attributable to a relay-side close names the
close-initiator and reason in the relay's bundle contribution. A
sim `RelayKill` scenario produces the matching event stream.

### 2.4 Inference response-leg instrumentation

**Source**: postmortem §"Data-collection / deployment gaps
surfaced by this run" item 5.

**Gap**: The `1779733878` postmortem's conclusion — "last stage
could not deliver the response" — was inferred from dial timeouts
plus the absence of an inbound `InferenceResponse`, not from a
typed event on the last stage saying "I tried to send the response
and the send outcome was X." The chain `stage-(N-1)
→ InferenceResponse → orchestrator's inbox` has no event on the
sending side. A typed event makes attribution a one-line read
rather than a triangulation.

**Contract — what closing the gap looks like**:

- D: the production stage actor, on attempting to send an
  `InferenceResponse` upstream, emits a typed event naming the
  target peer, the request id the response corresponds to, the
  byte size, and the send outcome. The outcome discriminator is
  the iroh-level result the transport returns (succeed / timeout
  / connection-closed / refused / unresolved / queued-but-not-
  acked-in-budget). The post-processor surfaces these in
  `summary.md` under a section that names which inference
  request was answered by which stage's send and how that send
  resolved.
- S: the sim's stage host kind grows a minimal inference
  message surface (`InferenceRequest` inbound to stage-0,
  `InferenceResponse` outbound from stage-(N-1), forwarded
  between adjacent stages as opaque payload in the MVP). The
  stage host emits the same typed response-send event when it
  attempts the outbound to the orchestrator. The codec contract
  (`SIM_SPEC.md §3.3`) carries the inference messages with
  byte-equality between sim and prod encoding.
- T: a scenario where the orchestrator's inbound path is broken
  via `RelayPeerConnDown` on the last leg (stage-(N-1) → orch)
  while every other leg works produces a bundle whose last
  stage emits exactly one `InferenceResponseSent` event with
  `send_outcome` in the failure-discriminator set. A scenario
  where every leg works produces an `InferenceResponseSent`
  with `send_outcome=success` and a matching
  `InferenceResponseReceived` (or equivalent) on the
  orchestrator's side.

**Expected status**:

- D: absent. The current stage actor's send call is not wrapped
  in a typed diagnostic event for the response leg.
- S: absent. The sim's stage host kind today produces no `Send`
  actions during its lifecycle (`SIM_SPEC.md §6A.5` notes this
  explicitly and defers inter-stage traffic to a later revision).
  Closing this coverage moves that deferral forward.
- T: absent.

**Close criterion**: a deployed bundle from any run where the
response did not return names the send outcome of the last
stage's response attempt in a single event. A sim scenario
modeling the same failure produces the same shape.

### 2.5 Bundle serve hardening under run-id reuse

**Source**: postmortem §"Bundle recovery" caveat; postmortem
§"Data-collection / deployment gaps surfaced by this run" item 2.

**Gap**: When a run id is reused across the failed-first-lease /
successful-second-lease shape the `1779733878` run exhibited, a
finalize record from the first phase pins a stale canonical
bundle in the collector's cache. A subsequent `GET` serves the
stale 5.3 KB bundle instead of synthesizing the rich 9.3 MB one
from current staging. Two adjacent quirks: `finalize_received`
stays `true` after the on-disk `finalize-*.json` is deleted, and
the synthesized manifest still lists a removed node directory.

**Contract — what closing the gap looks like**:

- D: the collector's `download_bundle` handler prefers the
  *richer* of {canonical-cached, synthesized-from-current-staging}
  by a size or node-count heuristic, or rebuilds canonical when
  staging has grown past the cached bundle's manifest. Deleting a
  node directory from staging clears the corresponding finalize
  record from in-memory state. The synthesized manifest reflects
  the current on-disk state, never a stale in-memory record. The
  `finalize_received` boolean is sourced from the same place the
  serve decision is sourced from — a single source of truth, not
  two diverging caches.
- S: not applicable. The sim writes bundles directly to a
  destination directory; there is no serve logic, no finalize
  cache, no run-id reuse semantics. The coverage threads through
  D only.
- T: not applicable as a *sim test*. The discriminator (stale vs
  fresh serve on a finalize-then-staging-growth sequence) is a
  collector unit-test concern living under
  `crates/distribution/tests/`, not a scenario the sim engine
  can express. The implementing agent should land the collector
  test alongside the D-layer change; it is named here so that the
  coverage's verification surface is honest about where it lives.

**Expected status**:

- D: absent. Current serve logic prefers cached canonical
  unconditionally when `finalize_received` is true.
- S: not applicable.
- T: collector unit test absent.

**Close criterion**: a collector unit test writes two phases of
staging with an intervening finalize, deletes the first-phase
node, and verifies the second `GET` serves the richer bundle and
that the cleared node does not appear in the manifest.

---

### 2.6 Per-SWIM-probe RTT and observed latency distribution

**Source**: postmortem §"SWIM churn and relay events" (1701
SwimTransitions over ~7 min, all with `conn_type=Relay`); postmortem
§"UDP echo probes" (tier-2 RTTs spread 181–405 ms; SWIM probes
ride a relay-mediated path on top of these); `SWIM_TUNING_REPORT.md`
§6 limit 3 ("SWIM host adapter does not emit `probe_sent` /
`probe_received` / `probe_timed_out` events").

**Gap**: The bundle has tier-2 UDP-echo RTT to docean:9081 — a
host-level surface that does not represent the latency SWIM
actually sees. SWIM rides a relay-mediated peer connection whose
RTT is at least one extra hop and is subject to relay-side HOL
queueing under load. The bundle currently exposes:

- per-snapshot iroh counters (cumulative `MessageSent` /
  `MessageReceived`),
- aggregate `SwimTransition` counts,
- per-peer dial outcomes (`Timeout` / `Success` rollup),

but it does not expose per-probe RTT, per-peer RTT distribution
over the run window, or correlation between
`probe_timed_out`-class outcomes and observed RTT spikes. Without
this surface, SWIM tuning is a guess against the deploy's actual
latency distribution rather than a measurement.

This gap also mirrors the simulator's own limit per
`SWIM_TUNING_REPORT.md` §6.3: the SWIM host adapter does not emit
the probe lifecycle events, so the §10 evaluator's
`no_flap_while_probes_ok` is structurally `Inconclusive`. Closing
the gap on both sides closes the assertion's precondition.

**Contract — what closing the gap looks like**:

- D: each SWIM ping/ack pair emits a typed event naming the
  observer, target, virtual-or-wall send time, virtual-or-wall
  receive time, the resulting RTT, and the discriminator
  (`success` / `timeout` / `connection-closed` / etc.). The
  post-processor surfaces a `## Probe RTT distribution` section
  with median, p95, p99 per (observer, target) pair, plus per
  five-second bucket so degradation over time is visible. A
  `probe_timed_out` outcome carries the configured timeout
  budget alongside the observed RTT (where one exists) so a
  reader sees "probe missed a 3 s budget by 200 ms" vs "no
  response within 3 s, never arrived."
- S: the simulator's SWIM host adapter emits the same probe
  lifecycle events. Per `SIM_SPEC.md §9.2` parity, the schema is
  identical to D's. This is the §6.3 limit from
  `SWIM_TUNING_REPORT.md` closing simultaneously with D — the
  bundle reader cannot tell a sim run from a prod run by this
  surface.
- T: a scenario with a declared per-link latency distribution
  (heavy-tailed, peer-symmetric) produces a bundle whose
  postproc RTT section's median, p95, p99 fall within stated
  tolerance of the scenario's declared distribution. A scenario
  with a `LatencySpike` mutation produces a bundle whose RTT
  section shows the spike at the mutation time. The
  precondition for `no_flap_while_probes_ok` is now satisfied;
  the assertion moves off `Inconclusive` for every scenario
  using a SWIM-host kind.

**Expected status**:

- D: absent. No per-probe event today.
- S: absent. `SWIM_TUNING_REPORT.md` §6.3 names this explicitly.
- T: absent.

**Close criterion**: a deployed bundle's postproc summary names
the median / p99 RTT per (observer, target) and a sim bundle
produces the matching surface. `no_flap_while_probes_ok` resolves
to `Pass` or `Fail` (not `Inconclusive`) on every SWIM scenario in
the calibration library.

**Downstream**: this coverage is the data surface
`N3_SWIM_TUNING_SPEC.md` consumes. SWIM tuning itself is
downstream of collection and lives in that sibling document.

---

## 3. Out of scope

- **Inference protocol surface beyond the response leg.** Coverage
  2.4 instruments the response-send event. A full inference-
  protocol event stream (microbatch routing, KV cache, per-stage
  worker activity) is broader than what the `1779733878` postmortem
  could not answer; it belongs in a separate spec when a
  postmortem demands it.
- **Post-processor summary enhancements.** SWIM transition
  distributions, per-(observer, target, reason) breakdowns,
  cross-node temporal alignment around the moment of failure —
  these are renderer concerns, not collection concerns. They
  presuppose the data is in the bundle; this spec is about the
  data.
- **Orchestrator-topology fixes.** The `1779733878` postmortem's
  item 6 names the root cause as a NAT'd local orchestrator with
  no reachable port. That is a deployment-shape question for the
  runbook, not a collection-coverage question.
- **Runbook fixes.** The `--gpu RTX_4090` vs `RTX 4090` line in
  `DEPLOYMENT_TEST.md` (postmortem item 7) is a runbook bug, not a
  collection gap.
- **Sim coverage of upstream-blocked surfaces.** If
  `iroh_relay::server` continues to expose no session hooks, the
  sim's relay vertex can model the lifecycle events the contract
  requires, but the production D layer of coverage 2.3 may remain
  partial. That partiality is a structural blind spot to file per
  the established blind-spot discipline; this spec does not
  resolve it.

---

## 4. References

- `N3_POSTMORTEM_2026-05-25_1779733878.md` — the second
  2026-05-25 deployment's postmortem. §"Observability upgrade
  scorecard" is the source for coverages 2.1, 2.2, 2.3; §"Data-
  collection / deployment gaps surfaced by this run" items 1–5 map
  to coverages 2.1, 2.5, 2.2, 2.3, 2.4 respectively.
- `N3_SIM_TEST_BATTERY_SPEC.md` — the sim-test battery spec. The
  battery's families A (relay peer-conn down) and the discriminator
  it builds against the relay-port probe (coverage 2.2) and the
  relay session lifecycle (coverage 2.3) consume the data this
  spec lands.
- `crates/simulation/SIM_SPEC.md` — the simulator's behavioral
  surface. §3.3 codec contract, §5A relay vertex, §6A stage host
  kind, §9 bundle layout are the load-bearing references for the
  S-layer contracts.
- `crates/simulation/SWIM_TUNING_REPORT.md` — the prior tuning
  pass against simulated 60 ms latency. §6 limits (especially
  §6.3 "SWIM host adapter does not emit `probe_sent` /
  `probe_received` / `probe_timed_out` events") are the source
  for the S-layer of coverage 2.6.
- `N3_SWIM_TUNING_SPEC.md` — the downstream spec that consumes
  coverage 2.6's data surface to retune SWIM against the
  observed `1779733878` latency distribution. Sibling document.
