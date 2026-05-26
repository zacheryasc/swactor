# N=3 sim-test battery — behavioral specification

Companion to `N3_POSTMORTEM_2026-05-25.md`, `N3_DATA_GAPS.md`,
`N3_DEPLOYMENT_REPORT.md`, `SIM_HARDENING_SPEC.md`, and the simulator's
`SIM_SPEC.md`. This document is the contract for a separate coding agent
that will land a battery of simulator tests covering the general failure
shapes the latest deployment exposed.

This is a *behavioral* spec. It names the failure shapes, the contracts
each test must establish, and the verdicts each must produce. It does
not prescribe file layout, TOML field values, or internal helper code.

---

## 0. Motivation and framing

The 2026-05-25 deployment surfaced one new failure shape (`stage-2`'s
relay-mediated path died at ~5 s and never recovered, while its tunnel
to the relay apparently survived) layered on top of failure shapes
prior deploys also exhibited (silent-worker subprocess, gossip-only
membership view, asymmetric host reachability, bundle-recovery only
via staging-file scrape). Together these are the **general** failure
cases the battery must cover — not one scenario per postmortem, but a
*family* per shape, as `SIM_HARDENING_SPEC.md §5` requires.

The simulator has now landed every observability and sim-cross-
pollination contract those postmortems demanded (`F1`–`F3`, `S-A1`
through `S-E2`; see `.loop/verdict.md`). The pieces needed to express
these scenarios all exist: `MutationKind::RelayPeerConnDown`, the
`stage` host kind with `WorkerExit`, the `relay` vertex with policy
mutations, and the §10.1 assertion catalog. **The battery is the
exercise of those pieces against the latest deployment's known shapes,
expressed end-to-end through scenario files and verdicts — not new
sim machinery.**

Why a *battery* rather than one test per shape: the
`SIM_HARDENING_SPEC §5` family rule. A fix that resolves the
2026-05-25 incident's specific timing (relay-peer-down at +5 s) but
regresses a sibling instance of the family (relay-peer-down at +30 s,
or during partition heal, or on only the inbound leg) is a regression
the battery must catch.

A diagnostic deployment is running concurrently to gather data we
don't yet have for the silent-worker class. This spec is written
against the evidence already in the bundle from 2026-05-25; the
implementing agent should not block on that deploy's results. When
results land they will sharpen the parameters of family **B**
(silent-worker) but will not change the shape of the battery.

---

## 1. Cross-cutting requirements

These hold for every family in §3.

### 1.1 No white-box / structural tests

A test in the battery passes or fails based on the *bundle* the
scenario produces and the verdicts the §10.1 assertion catalog
returns against that bundle. No test reads simulator internals, no
test inserts a value via one API path and reads it back via another,
no test asserts that an internal Rust struct has a particular field
shape. A test that would survive a refactor of the engine, the
network, or any host kind, but fail when the *deployment-relevant
behavior* drifts, is a test that belongs.

Litmus test: if removing the assertion would change the bundle's
prose summary in a way a deployment investigator would notice, the
assertion belongs. If removing it would not, the assertion is
echoing internals and does not belong.

### 1.2 Test taxonomy and priority

Each family ships at least one **scenario test** (story-shape:
declared scenario + declared assertion + declared expected verdict)
and where the parameter space is large, at least one **property
test** (a parameterized scenario whose `seed` ranges over the §1.3
family axes). Scenario tests are mandatory; property tests are
required only where §3 names them.

A small number of **contract tests** sit alongside the families: they
assert that the bundle's event schema matches the production
diagnostics schema for the event kinds the battery exercises (the
`SubprocessSpawned`/`SubprocessExited`, `RelaySessionStateChanged`,
`GossipReceived`, and `Tier2RelaySession` shapes the observability
upgrade landed). The contract tests are not per-family; they live
once and protect every family from sim/prod drift.

### 1.3 Family-based, not single-seed

Every family in §3 declares its **mutation axes** — the dimensions
along which the postmortem's parameters are "plausibly variable in
the wild" per `SIM_HARDENING_SPEC §5`. The family's scenario tests
cover the central case (the specific incident's parameters) and the
named extreme cases (e.g., "session closes at +1 s" and "session
closes at +5 min" for family A). The family's property test ranges
over the axes within their declared bounds.

### 1.4 Deterministic replay

Every scenario test's `(scenario, seed)` is recorded in the test
itself; running the test produces a byte-identical bundle to any
previous run on any supported architecture. A property-test failure
prints the seed; running the scenario with that seed reproduces the
failure. This is mechanical — the simulator already guarantees it
(`SIM_SPEC.md §7`); the battery must not undo it. No test reads any
wall-clock or system source of randomness.

### 1.5 Sub-second per scenario

A 3-node scenario test (including bundle assembly and verdict
evaluation) completes in under one second on the developer's
machine. The full battery completes in under thirty seconds locally
and under three minutes in CI. A scenario that grows above this
budget is a regression in the test, not in the simulator; the test
author tightens the scenario rather than relaxing the budget.

### 1.6 Verdict-first

Every test in the battery declares its **expected verdict on the
current source** before it lands: `Pass` (the simulator already
satisfies the contract; the test guards against regression), `Fail`
(the simulator currently violates the contract; landing the test
makes the failure visible, and the test is expected to pass after a
fix names in §4), or `Mixed` (some seeds pass, some fail — typical
for property tests against a probabilistic shape).

A test landing as `Fail` is **not** a build break in the test
binary; it is a verdict in the bundle's `verdicts.json` whose CI
exposure is named in §1.7. A test landing as `Pass` runs with
`#[test]` semantics — a regression in the simulator is a CI break.

### 1.7 CI exposure

Tests with expected verdict `Pass` run as standard `cargo test`
binaries under `crates/simulation/tests/`. Tests with expected
verdict `Fail` or `Mixed` run as a separate
`cargo test --package simulation --test battery_expected_failures`
binary that asserts the verdict matches expectation (`Fail` →
`Fail`, `Mixed` → at least one `Fail` across the seed range, at
least one `Pass`). Promoting a `Fail` test to `Pass` after a fix is
a one-line move between binaries and a deletion from the expected-
failures registry; the implementer should make this move trivial.

### 1.8 Library layout

The battery's scenarios live under
`crates/simulation/scenarios/reproduction/n3_2026_05_25/`, one
subdirectory per family. Each family directory contains:

- A `README.md` naming the family, pointing at the postmortem, and
  listing the family's mutation axes.
- One scenario file per named central or extreme case
  (`central.toml`, `extreme_*.toml`).
- A `property.toml` file declaring the property-test seed range and
  axis bounds where §3 requires a property test.

This layout is the existing `scenarios/reproduction/` convention
extended one level. No new top-level directories.

---

## 2. The shared scenario shape

Every scenario in the battery has the following shape unless its
family in §3 names a divergence:

- **Three peers**: one orchestrator-kind, two stage-kind. IDs
  `orch`, `stage-0`, `stage-2` (the latter named to match the
  postmortem's victim peer). The third stage from production is
  omitted only when its absence does not change the shape of the
  failure under test; families that require N=4 to manifest must say
  so explicitly. (`stage-1` may appear as a peer in families that
  need it; otherwise the simulator's N=3 minimum is the target.)
- **One relay vertex** `R`, with policy seeded from the
  `vastai-N3-2` calibration scenario (own-relay shape — widened
  egress, modest queue depth). Per-family scenarios may tighten or
  loosen this; the central case for each family uses the calibration
  defaults.
- **Routing**: all host-to-host edges declared `via = R`. The 2026-
  05-25 incident exercised the relay path exclusively; no direct
  edges in the battery's central cases. Extreme cases that need
  direct edges declare them per `SIM_SPEC.md §8.1`.
- **Duration**: 10 simulated minutes (`duration_ns = 600_000_000_000`)
  matching the 2026-05-25 run's wall-clock budget. Scenarios may
  shorten but not lengthen — long scenarios violate the sub-second
  budget in §1.5.
- **Snapshots**: at least one snapshot per peer per simulated
  minute, plus a snapshot one virtual nanosecond before and one
  after every named fault, so the bundle reader can see the state
  on each side of each transition. (This is a property of the
  scenario, not of the engine: the scenario's `[[snapshots]]` array
  declares these.)
- **Assertions**: each family in §3 names its required assertions.
  Scenarios may add further assertions from §10.1 to tighten the
  contract; they may not remove or relax the named ones.

---

## 3. The families

Six families, each named for the failure shape it covers. Families
A, B, and C are derived directly from the 2026-05-25 incident.
Families D, E, and F are derived from the broader N≥3 deployment
history that the latest run did not contradict and should not
regress.

### Family A — Relay-mediated peer-connection drop with surviving tunnel

**Source**: `N3_POSTMORTEM_2026-05-25.md` "iroh state — orchestrator's
view of stage-2"; `N3_DATA_GAPS.md` gaps 1, 2, 3.

**Shape**: A peer-to-peer path through a relay opens, succeeds for a
short window, then dies. The relay's tunnel to the victim peer
remains apparently healthy — the victim's `Tier2RelaySession.status`
stays `connected` or is reported as such by the relay, while the
orchestrator's `connection_cache[victim].last_failure_reason` shows
the path closed. iroh does not re-establish.

**Central case** (`central.toml`): `RelayPeerConnDown { relay: R,
from: orch, to: stage-2, at_ns: 5_000_000_000, duration_ns: 0 }`
(permanent until run end), inserted shortly after SWIM convergence.
No other faults.

**Mutation axes** (the family's parameter space):

1. `at_ns`: when the cut fires. Central +5 s; extremes +1 s, +30 s,
   +1 min, +5 min.
2. `duration_ns`: how long the cut persists. Central permanent;
   extremes 100 ms, 5 s, 30 s.
3. Direction: cut on `(orch → stage-2)` only, on `(stage-2 → orch)`
   only, or on both. The 2026-05-25 evidence is ambiguous about
   direction; the battery covers all three.
4. Flap: a sequence of `RelayPeerConnDown` mutations interleaved with
   their natural recovery — close, reopen, close. Inter-flap durations
   100 ms, 1 s, 5 s.
5. Phase: cut during SWIM convergence (before all peers Alive); cut
   during steady-state after convergence; cut during a
   `Partition`+`Heal` cycle's heal phase (per `SIM_HARDENING_SPEC §9`).

**Required assertions**:

- `no_flap_while_probes_ok { peer: stage-2, window_start_ns:
  at_ns, window_end_ns: duration_ns_end }` — the family asserts the
  *observability* contract that a relay-peer cut produces a typed
  event chain (`RelayPeerConnDown` mutation record →
  `RelaySessionStateChanged` or equivalent on the victim's view →
  `connection-closed` in the observer's cache). What it does *not*
  assert is that the simulator's SWIM tolerates the cut — the
  current simulator does not.
- `event_count { kind: "RelaySessionStateChanged", min: 1 }` on
  the central case — a cut must produce at least one transition
  event for the bundle reader to see.
- `dead_peer_resurrects_within { peer: stage-2, after_ns:
  heal_at_ns, within_ns: 30_000_000_000 }` on the finite-duration
  extreme cases — once the cut lifts, the cluster must reconverge.

**Property test**: `property.toml` ranges seeds 0..256 over axes 1,
2, and 5. The seed search reports any seed whose run violates
`no_flap_while_probes_ok` while the cut is *not* active (a
false-flap during a healthy window — the bug class the family
exists to catch).

**Expected verdict on current source**: `Mixed`. The central case
is expected `Fail` against the current SWIM source (the
deployment's actual failure mode); the flap extreme and the
phase-during-heal extreme are also expected `Fail`. The finite-
duration extremes with short cuts may pass.

**Family closes when**: a fix lands that lets the central case
pass and at least the flap and phase-during-heal extremes pass,
with no other family regressing.

### Family B — Silent stage subprocess (never spawned, spawned-and-stuck, spawned-and-exited)

**Source**: `N3_POSTMORTEM_2026-05-25.md` "Custom (worker) events"
table (`stage-2` emitted zero `worker_starting`, zero `worker_ready`);
`N3_DATA_GAPS.md` gap 4; `SIM_HARDENING_SPEC §5`.

**Shape**: A stage's worker subprocess fails to reach the
`worker_ready` state. The stage actor itself is alive — snapshots
still arrive, events still flow — but no work begins. The failure
splits into three buckets per the §4 spec the observability upgrade
already landed: never-spawned, spawned-and-stalled-before-ready,
spawned-and-exited-before-ready.

**Central case** (`central.toml`): install a `SubprocessFakeSpec`
on `stage-2` with `never_ready = true`, no `exit_after_ns`. The
orchestrator's view: `SubprocessSpawned` arrives, no `worker_ready`
Custom event ever does. The sim already supports this via `F1`.

**Mutation axes**:

1. Bucket: `never_spawned` (no `SubprocessFakeSpec` installed at
   all; stage actor never registers); `stalled` (spawned, never
   ready); `early_exit` (spawned, exits before ready with named
   exit code / signal).
2. `exit_after_ns` for the `early_exit` bucket: 100 ms (faster than
   any plausible ready), 1 s, 10 s.
3. Number of victim stages: one (central), two (whole stage layer
   silent), zero (control — all stages reach `worker_ready` —
   sanity).
4. Whether SWIM convergence completes before or after the worker
   silence is observable.

**Required assertions**:

- The bundle must make the three buckets distinguishable at the
  verdict level. The discriminator is the joint state of
  `SubprocessSpawned`, `SubprocessExited`, and the `worker_ready`
  Custom event for the victim peer, with the buckets mapping as:
  - `never_spawned`: `SubprocessSpawned == 0`, `worker_ready == 0`.
  - `stalled`: `SubprocessSpawned == 1`, `worker_ready == 0`, no
    `SubprocessExited` for the run's duration.
  - `early_exit`: `SubprocessSpawned == 1`, `worker_ready == 0`,
    `SubprocessExited == 1` with the declared reason.
- `name_resolves_within { name: "pp-entry", observers: [orch],
  within_ns: 300_000_000_000, from_ns: 0 }` — the orchestrator's
  resolution of the pipeline entry name must fail when any victim
  stage is silent. The contract: `Inconclusive` is **not**
  acceptable — the bundle must clearly say "the orchestrator looked
  and the name was absent," not "we don't know if the orchestrator
  looked."

**Property test**: not required for B. The bucket count is small
enough that all combinations land as scenario tests.

**Expected verdict on current source**: per-bucket. `never_spawned`
and `stalled` expected `Fail` on the `name_resolves_within`
assertion (correct — the cluster cannot resolve `pp-entry` if a
stage is silent). `early_exit` expected `Fail` on the same plus
`event_count { kind: "SubprocessExited", min: 1 }` with the
correct exit code observable in the bundle.

The battery's job here is to **prove the bucket is observable**, not
to prove the cluster recovers. Recovery from a silent worker is a
product question, not a sim contract.

**Family closes when**: the bundle's `summary.md` (rendered through
`swactor-diag-postproc`) names which bucket the victim stage is in,
in human-readable prose, for every scenario in the family.

### Family C — Gossip-arrival absence (control-plane vs data-plane discriminator)

**Source**: `N3_POSTMORTEM_2026-05-25.md` "iroh state — stage-2's
view of itself" (`peers: [orchestrator only]`); `N3_DATA_GAPS.md`
gap 10; `SIM_HARDENING_SPEC` §1 and §2.

**Shape**: A victim peer's local membership view contains only the
orchestrator, never its siblings. Two possible causes are
indistinguishable from the postmortem bundle: gossip about siblings
never arrived (control-plane failure), or gossip arrived but the
dials based on it never connected (data-plane failure). The battery
must let a single scenario+verdict pair disambiguate these.

**Central case** (`central.toml`): a `Partition` mutation that
isolates `stage-2` from `stage-0` and `stage-1` at the
network-graph layer (no direct, no relayed route between them),
while leaving each stage's path to `orch` intact. Stage-2 should
never receive gossip naming stage-0 / stage-1.

**Mutation axes**:

1. Topology: full isolation (central); one-way isolation (stage-2
   receives gossip, dials silently dropped); periodic gossip drops
   modulated by `LossBurst`.
2. Whether the orchestrator's gossip-piggyback ever names the
   siblings (which depends on its own membership view at the time
   stage-2 boots and receives its first ping).

**Required assertions**:

- `event_count { kind: "GossipReceived", peer: stage-2,
  payload_kind: "NameRegistry", min: N }` where `N` depends on the
  axis: for the central case, `N >= 1` (gossip must reach
  stage-2); the assertion lets us prove the discriminator. A
  scenario in which gossip *did* arrive but dials failed produces
  `GossipReceived >= 1` and `DialOutcome` with failure reasons for
  the siblings; a scenario in which gossip never arrived produces
  `GossipReceived == 0`. The two bundles are now distinguishable
  by the verdict.
- `event_count { kind: "DialStarted", peer: stage-2, target: stage-0,
  min: 1 }` on the one-way-isolation axis: dials must be observable
  in the data-plane-failure case.

**Property test**: not required.

**Expected verdict on current source**: `Pass` for all cases — the
observability upgrade landed `GossipReceived` (`S-E2`) and the
per-peer dial rollup (`S-A3`), so the discriminator is already
expressible. The battery's job is to *guard* this contract against
regression in the simulator or in the post-processor.

**Family closes when**: a probe-by-grep against the bundle's
`summary.md` confirms the discriminator is named in prose, not
buried in raw event counts.

### Family D — Asymmetric host reachability (NAT / mapping pathology)

**Source**: `N3_POSTMORTEM_2026-05-25.md` "UDP echo probes" (stage-2
1/12 timeout while others were clean); `N3_DATA_GAPS.md` gaps 8 and
11; `SIM_HARDENING_SPEC §2` host-environment-level faults.

**Shape**: One peer's host network behaves correctly *most* of the
time, but exhibits asymmetric loss, NAT-rebind, or kernel-UDP-buffer
overflow in a pattern that downstream iroh layers cannot
distinguish from a relay-side issue or a peer-software issue. The
postmortem could not tell which.

**Central case** (`central.toml`): a `LossBurst` on
`(stage-2 → R)` with `prob_ppm = 80_000` (8% loss) lasting 30 s
during steady state. This is the smallest fault that produces the
postmortem's "one peer flaky, others clean" symptom.

**Mutation axes**:

1. Symmetry: loss on outbound from victim, on inbound to victim,
   on both directions, none (control).
2. Burst shape: continuous low-rate loss vs short high-rate burst.
3. Co-occurrence: loss alone vs loss + clock skew on the same peer
   (compound — per `SIM_HARDENING_SPEC §7`).

**Required assertions**:

- The bundle's UDP echo probe records must show the victim's
  outcome distribution (`ok` / `timeout` / `refused` / `unresolved`
  / `error`) differing from the other peers' by a margin evident
  to a human reader.
- Across the run, the victim's
  `Tier3InterfaceCounters.rx_packets_dropped` or
  `Tier3UdpKernelStats.in_errors` is non-zero in the bundle, while
  the other peers' is zero. This is the "kernel saw the loss, not
  just iroh" contract gap 11 demanded.

**Property test**: required, seeds 0..128. Range over axes 1 and
2. The property: for every seed in which the victim's UDP echo
shows >5% loss, the bundle must surface a non-zero kernel-counter
delta on the same peer. (This is the discriminator gap 11 asked
for.)

**Expected verdict on current source**: `Mixed`. The observability
upgrade landed kernel counters in the bundle (`S-A4`); the
simulator's stage host needs to emit `Tier3InterfaceCounters` under
the loss-burst mutation for the discriminator to hold. If it does
not, that is a sim-coverage gap belonging in `SIM_BLIND_SPOTS.md`
per `SIM_HARDENING_SPEC §10`, not a reason to relax the assertion.

**Family closes when**: the property test runs to 128 seeds with
the loss-discriminator holding on every seed it sees loss; the
sim-coverage gap, if it exists, is filed.

### Family E — Bundle integrity under operator SIGKILL

**Source**: `N3_POSTMORTEM_2026-05-25.md` "Bundle recovery";
`N3_DATA_GAPS.md` gap 7; observability upgrade `S-D` (bundle
without finalize).

**Shape**: The orchestrator is killed ungracefully (SIGKILL via
TaskStop, not graceful shutdown). No finalize record is written.
The diagnostic bundle must still be assemblable from staging files
on disk, with `manifest.finalize_received: false`.

**Central case** (`central.toml`): a `PeerKill { peer: orch,
at_ns: 60_000_000_000 }` mutation 60 s into the run. No
`PeerResurrect`. The scenario's `duration_ns` extends 30 s past
the kill so the collector has time to observe and the bundle has
time to coalesce.

**Mutation axes**:

1. Timing of kill: during convergence, during steady state, during
   a partition heal.
2. Which peer: orchestrator, a stage, the relay.

**Required assertions**:

- The bundle's `manifest.json` must exist and contain
  `finalize_received: false`.
- Every peer's pre-kill events and snapshots must be present in
  the bundle (the kill must not erase prior records).
- The `verdicts.json` must contain a verdict for every declared
  assertion, with `Inconclusive` for any assertion whose
  preconditions did not fire (e.g., a steady-state assertion when
  steady state was never reached).

**Property test**: not required.

**Expected verdict on current source**: `Pass`. The observability
upgrade landed `S-D` (bundle assembly without finalize). This
family guards that contract against regression.

**Family closes when**: every scenario in the family produces a
parseable bundle whose `summary.md` renders cleanly through
`swactor-diag-postproc`.

### Family F — Compound faults under recovery

**Source**: `SIM_HARDENING_SPEC §7` and §9.

**Shape**: Two or more faults active during a single recovery
window — a partition heal during a relay-peer-down, a clock skew
during a worker respawn, a kernel UDP overflow during SWIM gossip
burst. The 2026-05-25 incident is consistent with at least two
overlapping faults (relay-peer-down + silent-worker); the battery
must cover the next overlap before it lands in prod.

**Central case** (`central.toml`): a `Partition` cutting `stage-2`
from `stage-0` from t=10 s to t=30 s; a `RelayPeerConnDown { from:
orch, to: stage-2, at_ns: 20_000_000_000, duration_ns:
20_000_000_000 }` overlapping the partition's last 10 s and
extending 10 s past its heal. The scenario tests whether SWIM
behaves under the *overlap* and the *heal* sequence the postmortem
mentions but did not isolate.

**Mutation axes**:

1. Which two faults overlap (cross product of the four single-fault
   families above, restricted to combinations that produce
   distinguishable bundles).
2. Overlap geometry: full overlap, partial overlap, abutting (one
   ends as the other begins).
3. Recovery phase: which recovery phase the second fault hits, per
   `SIM_HARDENING_SPEC §9`.

**Required assertions**: family-dependent — each compound test
combines the assertions of its constituent families. The compound
test passes only if every constituent assertion holds.

**Property test**: required, seeds 0..512. Range over all three
axes. The property: any seed in which a compound bundle violates
*more* assertions than the sum of the constituents' individual
violations is a true compound bug, reported separately.

**Expected verdict on current source**: `Mixed`. Compound failures
are the under-tested corner; the implementing agent should expect
to find at least one new sim-coverage gap during this family's
implementation and file it.

**Family closes when**: at least one compound bug is either fixed
or filed as a sim-coverage gap with a structural reason.

---

## 4. Out of scope

- Tuning the simulator's existing scenarios under
  `scenarios/calibration/` or `scenarios/smoke/`.
- Adding new failure shapes the 2026-05-25 deployment did not
  surface (the diagnostic deployment running in parallel may; if
  so, those land as a new spec, not as an amendment to this one).
- Changes to the simulator's engine, network, host kinds, bundle
  writer, or post-processor. The battery exercises them; it does
  not modify them.
- Changes to the production diagnostics code path. The
  observability upgrade landed; the battery consumes its output.
- Documentation of the simulator beyond `SIM_BLIND_SPOTS.md`
  amendments. `SIM_HARDENING_SPEC.md` and `SIM_SPEC.md` already
  exist; this document is the only new prose required.

---

## 5. References

- `N3_POSTMORTEM_2026-05-25.md` — source for families A, B, C, D, E.
- `N3_DATA_GAPS.md` — source for the gap-named contracts each
  family asserts the simulator's bundle must satisfy.
- `N3_DEPLOYMENT_REPORT.md` — historical context: Layers A/B/C from
  the prior eight deploys.
- `N3_OBSERVABILITY_UPGRADE_SPEC.md` — the contract the bundle
  *already* satisfies. The battery consumes that contract.
- `SIM_HARDENING_SPEC.md` — the family / mutation-axis discipline
  that §1 and §3 above enforce.
- `crates/simulation/SIM_SPEC.md` — the simulator's behavioral
  surface. §3.1 components, §5.5 mutations, §6A stage host, §10.1
  assertion catalog, §8 scenario format are the load-bearing
  references.
- `.loop/notes.md`, `.loop/verdict.md` — the observability-upgrade
  iteration log and verdict, current as of 2026-05-25; STATUS:
  DONE, VERDICT: PASS.
