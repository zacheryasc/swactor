# N=3 SWIM retuning against deployed latency — behavioral spec

Companion to `N3_POSTMORTEM_2026-05-25_1779733878.md`,
`N3_COVERAGE_EXTENSION_SPEC.md`, and the simulator's
`SWIM_TUNING_REPORT.md`. This document is the contract for a SWIM
retuning pass that uses the `1779733878` deployment's observed
latency and churn data as the evidence the tune is calibrated
against — rather than the simulated 60 ms latency the prior tune
used.

This is a *behavioral* spec. It names the targets the retuning
must hit, the evidence each target is calibrated against, and the
prerequisites that must be in place before a retuning pass can be
evidence-driven rather than guess-driven. It does not prescribe
specific knob values.

---

## 0. Motivation

`SWIM_TUNING_REPORT.md` documented a prior tuning pass against the
§10.3 gossip-flap property and the three N3 calibration scenarios.
That pass collapsed `self_incarnation_peak` from 86–94 to 7–10 — a
significant win — but its calibration latency was **60 ms RTT with
15 ms jitter** (per §2 of that report). The simulator's calibration
scenarios used these numbers because the live bundles available at
the time did not surface per-SWIM-probe RTT.

The `1779733878` run exposes a different reality:

- **Tier-2 (host-level) UDP echo RTT**: orchestrator 293 ms,
  stage-0 181 ms, stage-1 405 ms, stage-2 184 ms. p99 spread is
  multi-hundred-millisecond and asymmetric across peers.
- **Topology**: every peer connection is `conn_type=Relay`. No
  hole-punching succeeded. SWIM probes ride a relay-mediated path
  whose RTT is strictly higher than the tier-2 floor and is
  subject to relay-side HOL queueing.
- **Observed churn**: 1701 `SwimTransition` events over a ~7-minute
  run while iroh continued to exchange messages (`connect-timeout
  count = 0`). The chosen `probe_timeout = 15 ticks = 3 s` was
  selected against 60 ms RTT; against a relay-mediated path with
  p99 multi-hundred-millisecond tier-2 floor and load-driven
  queueing on top, that budget may be marginal or worse.
- **Configuration**: the prior tune's knobs landed at
  `probe_interval=10, probe_timeout=15, suspicion_timeout=75,
  indirect_probes=2, dead_reprobe_interval=50` ticks plus
  `max_piggyback=6`. These are committed defaults; the question
  this spec opens is whether they hold under the observed
  deployment shape, not whether the prior tuning method was
  correct.

The previous postmortem (run `1779720002`) could not have driven
this retune: its bundle lacked the data the observability upgrade
landed afterward. The `1779733878` bundle is the first one rich
enough to retune against. This spec captures the contract that
retuning must satisfy.

---

## 1. Prerequisites

Retuning is not evidence-driven until the data the tune calibrates
against is in the bundle. Three prerequisites are explicit.

### 1.1 Coverage 2.6 from `N3_COVERAGE_EXTENSION_SPEC.md`

Per-SWIM-probe RTT events and the post-processor's `## Probe RTT
distribution` section must land in production *and* in the sim
adapter. Until this coverage is in place:

- The deployed evidence is tier-2 RTT (UDP echo to the collector),
  which understates the relay-mediated SWIM RTT by an unknown
  factor.
- The simulator's `no_flap_while_probes_ok` assertion is
  `Inconclusive` on every SWIM scenario (per
  `SWIM_TUNING_REPORT.md` §6.3), so the assertion can neither
  pass nor fail the retune.

A retuning pass that lands without 2.6 is a guess against
tier-2 latency — the same mistake the prior tune made against
60 ms simulated latency, only with a different proxy for the real
number.

### 1.2 Determinism fix from `SWIM_TUNING_REPORT.md` §6.7

`MemberList`'s `HashMap<NodeId, _>` randomises iteration order per
process; the prior tune reports ±20 % run-to-run variance as a
result. A retuning pass that has to average across five samples
per grid point to estimate variance is exactly five times slower
and five times noisier than one against deterministic substream
selection. `HashMap` → `BTreeMap` is the one-line fix the prior
report names; it must land before retuning, not after, so the
retune's results have signal-to-noise high enough to read.

### 1.3 The Layer-B1 refute-on-stale-Suspect bug (`SWIM_TUNING_REPORT.md` §6.1)

`crates/distribution/src/swim/node.rs::apply_membership_update`
refutes against `self_id()` whenever
`update.state ∈ {Suspect, Dead}` regardless of whether
`update.incarnation` is current. This creates a non-zero floor on
`self_incarnation_peak` that no tuning can collapse. A retuning
pass against the `1779733878` shape, where the relay-mediated path
keeps stale Suspect entries in the dissemination queue for many
probe cycles, will hit this floor and conclude — incorrectly —
that further tuning gain is unavailable.

The one-condition gate the prior report names is the
priority-1 follow-up the prior tune deferred. It is a
prerequisite for evidence-driven retuning against this deployment,
not a downstream cleanup.

---

## 2. Calibration data the retune is driven by

The retuning pass's evidence comes from the `1779733878` bundle
(and any subsequent N=3 deploy bundles that land before the
retune). Three numbers anchor the calibration:

### 2.1 Observed tier-2 RTT distribution

| node         | RTT (ms) | echo success |
|--------------|----------|--------------|
| orchestrator | 293      | 34/35        |
| stage-0      | 181      | 55/55        |
| stage-1      | 405      | 27/28        |
| stage-2      | 184      | 38/38        |

The tier-2 echo path is collector-bound, not peer-bound. It
establishes the floor below which a relay-mediated SWIM probe
cannot land.

### 2.2 Per-SWIM-probe RTT distribution (post coverage 2.6)

After coverage 2.6 lands, the bundle will carry per-probe RTT
distributions per (observer, target) pair, plus per-bucket
distributions over the run window. The retune calibrates
`probe_timeout` such that the configured budget exceeds the
observed p99 of legitimate (non-failure) probe RTT with a margin
the retune explicitly justifies. Until 2.6 is collected against a
live run, the retune uses §2.1 as a lower-bound proxy and is
explicit about that.

### 2.3 SWIM churn and dial outcomes

`SwimTransition: 1701` across a ~7-minute run is the load-bearing
churn signal. The retune is calibrated such that a scenario
configured to mirror the `1779733878` shape produces a churn
count within a stated factor (target: <300, an order-of-magnitude
collapse comparable to the prior tune's `self_incarnation`
collapse).

Per-peer dials from the postmortem (orchestrator 7/11 timeout,
inter-stage 19/19 success) are the discriminator the retune must
not undo: a retuned SWIM that makes inter-stage probes flap is a
regression even if it makes orchestrator-bound probes more stable.

---

## 3. Retuning targets

Six, ordered by load-bearing impact.

### 3.1 `probe_timeout` against relay-mediated p99 RTT

**Target**: `probe_timeout` exceeds the bundle's observed p99 of
legitimate probe RTT (post-2.6) by a margin the retune justifies
in prose — the margin must account for relay-side HOL queueing
peaks the steady-state distribution does not capture.

**Anti-target**: the budget cannot be set so high that suspicion
takes longer than the operator's deadstop threshold. The
postmortem named ~7 min as the operator's deadstop budget; SWIM's
detection time (`probe_timeout + suspicion_timeout`) must remain
well under that, with a documented headroom.

**Evidence**: per-probe RTT histogram from §2.2; SwimTransition
churn count from §2.3.

### 3.2 `suspicion_timeout` under relay-mediated reachability

**Target**: a peer whose relay-mediated path is intermittently
unreachable (the `1779733878` shape — repeated probe failures
interleaved with successes) does not flap between Alive and
Suspect more than the prior tune's bound on the gossip-flap
property, when the scenario mirrors the deployment's latency
distribution.

**Anti-target**: a peer whose path is genuinely dead is not
falsely held Alive past the operator's deadstop window.

**Evidence**: §2.3 churn count; the `no_flap_while_probes_ok`
assertion (now resolvable post-2.6) against the calibration
scenario.

### 3.3 `indirect_probes` count against relay HOL behavior

**Target**: indirect probes still provide redundant coverage when
the direct probe times out, but their cumulative bandwidth
contribution to the relay's egress queue does not push the
`relay_queue_depth_bounded` assertion to fail under own-relay
policy.

**Anti-target**: dropping the count below the prior tune's 2
collapses indirect coverage, which the prior tune's §5 already
documents.

**Evidence**: `relay_queue_depth_bounded` under own-relay calibration;
churn count from §2.3.

### 3.4 `probe_interval` against the dial-rate signal

**Target**: probe rate is set such that the orchestrator-bound
dial failures the `1779733878` run exhibited (7/11 timeout) do
not bottleneck convergence beyond a tolerance the spec names.

**Anti-target**: probe rate is not lifted so high that
`message_size_bounded` regresses against own-relay policy.

**Evidence**: per-peer dial table from §2.3; piggyback byte
totals from the bundle.

### 3.5 `max_piggyback` against observed gossip-receipt sizes

**Target**: piggyback gossip stays within the
`message_size_bounded` envelope under own-relay policy, given
the `1779733878` per-node piggyback byte totals (806–1589
piggybacks per node, 194–522 KB total).

**Anti-target**: lowering `max_piggyback` below the prior tune's
6 stops convergence within the property's window
(`SWIM_TUNING_REPORT.md` §5).

**Evidence**: gossip-receipt totals from the postmortem's "Gossip
receipts" section; `message_size_bounded` assertion under
own-relay.

### 3.6 `LifeguardConfig` wiring (formerly out of scope)

**Target**: the dynamic suspicion-timeout formula in
`crates/distribution/src/swim/lifeguard.rs` is wired into
`SwimNode`'s suspicion state machine. Until wiring lands, the
constants in `lifeguard.rs` have no observable effect — per
`SWIM_TUNING_REPORT.md` §6.5, the prior tune could not sweep "the
lifeguard band" because it was dead code.

This target is the only one that requires code beyond a knob
change. It is included here because the prior tune named it as a
priority follow-up and because the `1779733878` data motivates
adaptive suspicion: a path whose RTT varies 2× under load benefits
from adaptive timeouts more than a static budget can capture.

**Anti-target**: landing the wiring without sweeping its
parameters reproduces the prior tune's dead-code condition for the
new fields. The wiring must come with a sweep against the
calibration scenarios.

**Evidence**: the new dynamic-suspicion code path is exercised by
at least one scenario whose assertion verdict changes when the
multiplier changes.

---

## 4. Calibration scenario updates

The three N3 calibration scenarios under
`crates/simulation/scenarios/calibration/` were last updated to
mirror the prior tune's defaults at the scenario's 200 ms tick
(`SWIM_TUNING_REPORT.md` §3). The retune updates these scenarios
along two axes:

- **Latency distribution**: per-link latency is set against the
  `1779733878` per-peer tier-2 RTT distribution, not the prior
  60 ms baseline. Heavy-tailed per `SIM_HARDENING_SPEC §8` (the
  prior battery spec's reference); the distribution's median, p95,
  and p99 fall within tolerance of the live bundle's after
  coverage 2.6 lands.
- **Topology**: every host-to-host link is routed through the
  relay vertex (`via = R` in scenario syntax). The
  `1779733878` shape had `conn_type=Relay` everywhere; the
  calibration scenarios must reflect that to be evidence-faithful.

The scenarios' `kind_config` blocks are updated to the retune's
chosen operating point. The current calibration block (per the
prior report) gives probes a 333 ms budget against 60 ms RTT;
against multi-hundred-millisecond relay-mediated RTT, the same
budget under-budgets by an order of magnitude. The retune's new
budget is the §3.1 target.

---

## 5. Acceptance

The retune is complete when:

1. Every prerequisite in §1 is in place (coverage 2.6, the
   determinism fix, the Layer-B1 gate).
2. Each target in §3 has a chosen operating point and a one-line
   prose justification anchored to the §2 evidence.
3. The `1779733878` calibration scenario, configured to mirror
   the deployment's latency and topology, produces fewer than
   300 `SwimTransition` events in a 7-minute virtual run (an
   order-of-magnitude reduction from 1701).
4. Inter-stage dial outcomes in the calibration bundle remain at
   the `1779733878` shape (≥95 % success on inter-stage edges)
   — the retune does not improve orchestrator-bound stability at
   the cost of inter-stage flakiness.
5. The §10.3 gossip-flap property's `self_incarnation_peak` does
   not regress from the prior tune's 7–10 band.
6. A retuning report (a successor to `SWIM_TUNING_REPORT.md`)
   documents the new operating point, the evidence each knob
   choice was calibrated against, the before/after numbers across
   every calibration scenario, and the limits the retune could
   not move.

---

## 6. Out of scope

- **Adding new SWIM features.** The retune adjusts existing knobs
  and lands the Layer-B1 gate / Lifeguard wiring the prior report
  named. New algorithmic features (push-pull anti-entropy,
  alternative failure detectors) are not in scope.
- **Relay-side fixes.** The `relay_queue_depth_bounded` failure
  on the canary topology is structurally out-of-reach for SWIM
  tuning (`SWIM_TUNING_REPORT.md` §6.2). The retune does not
  attempt to make canary pass; it does not regress own-relay.
- **Orchestrator-topology changes.** Running the orchestrator on
  a reachable host (the `1779733878` postmortem's item 6) is a
  deployment-shape change, not a SWIM-tuning change. The retune
  is calibrated against the NAT'd-orchestrator shape because that
  is the deployment we have, but the conclusion may be "even
  optimally-tuned SWIM cannot stabilize this topology" — that
  conclusion is a valid retune outcome.
- **The gossip-flap property's `self_incarnation_bounded`
  assertion.** The prior tune collapsed it from 86–94 to 7–10
  without removing the non-zero floor; the retune holds that
  result. Removing the floor is the Layer-B1 fix's job (a §1.3
  prerequisite, not a §3 target).
- **Scenarios beyond the calibration corpus.** The reproduction
  and topology scenarios remain on their current SWIM config.
  Retuning them is a follow-up that should wait for the
  calibration retune to converge.

---

## 7. References

- `N3_POSTMORTEM_2026-05-25_1779733878.md` — source of the
  observed latency distribution (§"UDP echo probes"), the churn
  signal (§"SWIM churn and relay events"), the dial outcomes
  (§"Per-peer dials"), and the topology context
  (`conn_type=Relay` everywhere, NAT'd orchestrator).
- `N3_COVERAGE_EXTENSION_SPEC.md §2.6` — the data surface this
  spec consumes. §1.1 of this spec is a hard prerequisite.
- `crates/simulation/SWIM_TUNING_REPORT.md` — the prior tuning
  pass. §3 (configuration), §5 (tradeoff curve), §6 (limits) are
  the load-bearing prior art the retune does not re-derive. §6.1,
  §6.5, §6.7 limits are §1.3, §3.6, §1.2 prerequisites
  respectively in this spec.
- `crates/simulation/SIM_SPEC.md` — the simulator's calibration
  contract (§11) and the assertion catalog (§10.1) the retune is
  scored against.
- `N3_SIM_TEST_BATTERY_SPEC.md` — the sim-test battery. A
  retuned SWIM that regresses any battery family is a retune
  regression, not a battery regression.
