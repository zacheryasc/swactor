# Simulator hardening — behavioral spec

Sister doc to `N3_OBSERVABILITY_UPGRADE_SPEC.md` and `SIM_SPEC.md`. The
observability spec says *what the bundle must contain after a real or
simulated run*. The sim spec says *what the simulator's MVP must do*.
This doc says *what the simulator must do beyond the MVP to be a credible
pre-deployment gate* — the behavior that closes the loop "we keep
deploying to vast.ai, finding one bug, fixing it, and finding the next
one in the next deploy."

Throughout: every contract is testable. A simulator that does not
satisfy these may still be useful for hand-written reproductions, but
it does not earn the right to block or unblock a deployment.

## 0. Motivation

Eight live N≥3 deploys have produced eight distinct failure modes.
Each one has been caught only by spending GPU rental, waiting 45–90
minutes for the cluster to come up, and reading the bundle after the
fact. The fix lands. The next deploy surfaces the next bug. The sim,
in its current form, has not preempted any of these failures — it
reproduces them after we know what to look for.

The gap is not that the simulator is wrong. It is that the simulator
is *narrow*. It exercises one host kind (SWIM), one transport model
(direct or one-relay), one fault dimension at a time, and one scenario
per fault. Production exercises three host kinds, two transports
stacked, multiple faults stacked, and a continuous distribution of
timing and size. The bugs live in the cross-product the sim doesn't
visit.

We are not running a database. We do not need 10^10 simulated years.
We need to *extrapolate heuristically from known failure shapes* —
treat each postmortem as the seed of a family of scenarios, and let
the sim explore the family densely while ignoring the rest of the
state space.

## Cross-cutting requirements

1. **Same code, sim and prod.** Every actor whose behavior matters
   for a known failure mode runs the same source in the sim as in
   prod. The sim wraps the actor in an adapter that routes its time,
   randomness, and I/O through the engine; it does not reimplement
   the actor's logic. A bug fix that lands in the actor lands in the
   sim automatically, with no separate sim-side change.

2. **Determinism from `(scenario, seed)`.** Every run is fully
   reproducible from the scenario file and the engine seed. Two runs
   of the same `(scenario, seed)` produce byte-identical bundles. A
   bug surfaced by the fuzzer is replayable by a developer with a
   single command and the printed seed.

3. **Heuristic over exhaustive.** The sim does not attempt to enumerate
   reachable states. It samples densely around shapes that have
   already broken in production and shapes that are structurally
   analogous to those. The unit of effort is "explore the
   neighborhood of one postmortem," not "explore the system."

4. **Failure surfaces at the moment of violation.** When an invariant
   is broken, the run halts at the violating step, not at end-of-run.
   The bundle records which invariant failed, the virtual time it
   failed at, and the state of every host at that instant. A
   developer reading the bundle never has to scroll backwards from a
   downstream symptom to find the originating event.

5. **Bundle-shape parity with prod.** A bundle produced by the sim is
   shape-identical to a bundle produced by a real deploy: same
   manifest schema, same event kinds, same snapshot fields, same
   post-processor output. A reader cannot tell sim from prod from
   data alone. (This requirement is shared with the observability
   spec's section "Sim cross-pollination.")

6. **Sub-second iteration.** A single sim run of a 3-node scenario,
   including bundle assembly and invariant evaluation, completes in
   under one second on the developer's machine. A failing seed found
   by the fuzzer replays in under one second too. This is what makes
   "extrapolate from a postmortem" cheap enough to do every time.

7. **What this is not.** Not a model checker. Not a proof of
   correctness. Not a replacement for staging deploys. Not a
   guarantee of zero bugs in prod. The sim is a high-bandwidth filter
   between "developer believes the change is correct" and "developer
   has paid two dollars and forty-five minutes to find out."

---

## 1. Production code path coverage

After this work, every actor whose misbehavior produced a known
production failure runs inside the sim engine, wrapped in a host
adapter, with its time / randomness / I/O routed through the engine.

The minimum set is:

- The SWIM state machine (already present).
- The iroh driver, including its relay-session state machine and its
  per-peer connection cache.
- The subprocess driver (`swactor_process` or its successor),
  including spawn, exit, signal delivery, and stdout/stderr capture.
- The pipeline stage supervisor lifecycle — the actor that owns "is
  my worker up, did it emit `worker_ready`, did it die for an
  internal reason."
- The orchestrator-side actor that consumes membership updates and
  decides whether the cluster is ready to accept inference.

A node simulated by the engine is a composition of these host
adapters, wired to a single virtual clock, RNG, and network. A
scenario that names "node X runs the orchestrator role" instantiates
all four adapters for node X; a scenario that names "node Y runs a
stage" instantiates the stage subset.

When the production code for one of these actors changes, the sim
host kind for it does not need to be edited. The adapter is a thin
shim over the production trait surface; rebuilding the sim with the
new actor source is the only update required.

Acceptance: a scenario that boots three nodes (one orchestrator, two
stages), advances the virtual clock until SWIM converges, and
inspects the resulting bundle, exercises the same `iroh_driver.rs`,
`stage_actor.rs`, and SWIM code paths that a live `pp-smoke-run`
exercises. Code coverage measured on the sim run matches code
coverage measured on a live run to within a stated tolerance, with
the gap attributable to OS-call-site stubs only.

---

## 2. Fault catalog

After this work, every fault the sim can inject is a value of a
closed enum. A scenario expresses its fault sequence as a list of
those values plus their timing; the fuzzer composes new sequences
from the same enum.

The enum's variants cover, at minimum, the dimensions production has
already hit and the dimensions adjacent to them. Not exhaustive of
all possible faults — exhaustive of the failure classes the
postmortems and the observability spec name. Concretely:

- **Network-level**: drop a packet, delay a packet by a duration
  drawn from a distribution, partition (symmetric or asymmetric)
  between two host subsets, reorder a packet relative to others on
  the same link, duplicate a packet, cap a link's bandwidth, jitter
  link latency around a baseline.
- **Relay-level**: close a relay session for a named reason at a
  named time, evict the relay's session for a peer when the relay's
  per-peer queue exceeds a size, drop one relay's tunnel to one peer
  while leaving its tunnel to others intact (the 2026-05-25 shape),
  flap a relay session repeatedly within a window.
- **Subprocess-level**: refuse a spawn, spawn-and-immediately-exit
  with a named exit code, spawn-and-stall-before-protocol-output,
  exit mid-run with a named signal, OOM-kill the subprocess at a
  named time, slow the subprocess's response loop by a factor.
- **Clock-level**: skew one node's clock by a duration, drift one
  node's clock at a rate, freeze one node's clock for a window.
- **Host-environment-level**: rebind the node's NAT mapping mid-run,
  change the node's apparent public IP, simulate a transient
  unreachable network namespace, simulate kernel UDP-buffer overflow.

Each variant has a deterministic semantics under the engine's virtual
clock. The fault catalog is the same value in scenarios and in
fuzzer-generated sequences; there is no "scenarios can do this,
fuzzer can do that" asymmetry.

Acceptance: the 2026-05-25 incident is expressible as a single
scenario file whose `faults` list is six or fewer entries drawn from
the catalog above. Replaying that scenario produces a bundle whose
diagnostics match the live bundle's shape within stated tolerance.

---

## 3. Mid-run invariants

After this work, the engine evaluates a declared set of invariants
continuously during a run. When an invariant is broken, the engine
records the violation and halts the run at the violating step. The
bundle's `verdicts.json` names the broken invariant, the virtual
time, the host whose state triggered the break, and the engine event
that immediately preceded it.

Invariants are written declaratively and registered against the
engine at scenario load. The minimum set covers:

- Membership convergence within a stated time of partition heal.
- No node alternates between alive and dead more than N times in a
  window (anti-flap).
- No microbatch lives without a stage assigned to it.
- No stage is assigned to two distinct microbatches simultaneously.
- Monotonic counters in snapshots are monotonic across consecutive
  snapshots.
- Every `SubprocessSpawned` event is eventually followed by either
  `SubprocessExited` or `worker_ready`.
- No relay session reports `connection-closed` more than N times
  against the same peer in a window.

The set is extensible. Adding an invariant is the same shape of work
as adding a post-run assertion today — there is no parallel API to
learn.

Per-invariant overhead is bounded: an invariant that requires reading
the full event stream every tick is not a valid invariant. The
contract is that the invariant set, in total, costs no more than a
small constant factor over a run with no invariants.

Acceptance: a scenario that injects the 2026-05-25 fault sequence
halts within the simulated second that contains the relay-close
event, reports the relay-close as the triggering engine event, and
the anti-flap or relay-session invariant as the broken one. A
developer running the scenario sees the failure in under a second of
wall time.

---

## 4. Seed-driven exploration

After this work, a single binary takes a scenario and a seed range,
runs each seed against the scenario, and reports the first seed whose
run violated an invariant. The report is the seed, the scenario, and
the broken invariant — sufficient input for the developer to
reproduce the run byte-identically with one further command.

The seed parameterizes:

- Initial RNG state for every host.
- The order in which the network resolves ties when two events are
  scheduled for the same virtual nanosecond.
- The specific timing of each fault within its declared window (a
  fault declared as "between t=1s and t=10s" picks one instant from
  that window per seed).
- The distribution sample for any latency / size / count drawn from
  a declared distribution.

A scenario without faults but with declared distributions still
benefits from seed exploration: the fuzzer probes the joint
distribution, not just the explicit fault list.

Parallelism is at the seed level. Running N seeds is N times the
wall time of one seed divided by the developer's core count, with no
shared state between runs.

Acceptance: a scenario file plus `--seeds 0..1000` produces, within
ten seconds of wall time on a developer machine, either "no
violations" or a printed seed that replays to the same violation
deterministically. The replay command and its output are the same
shape as a hand-written scenario run.

---

## 5. Heuristic extrapolation from known failures

After this work, every postmortem produces a *family* of scenarios in
the simulator's library, not a single scenario. The family is
generated by mutating the postmortem's parameters along axes the
implementer declares as "plausibly variable in the wild."

For the 2026-05-25 incident, the family includes at minimum:

- The original timing (relay session closes at +5s, never reopens).
- Sessions that close at +1s, +30s, +60s, +5min.
- Sessions that close with reasons other than `connection-closed`.
- Sessions closed from the relay side vs. from either endpoint.
- Sessions that flap (close + reopen + close, with varying
  inter-flap durations).
- Sessions that close on only one direction of the tunnel
  (split-brain at the relay).
- Sessions that close during convergence, during steady-state
  inference, during shutdown, during a partition heal.

The mutation axes are part of the scenario family's source. The
fuzzer ranges over them; a developer reading the library can tell
what is being varied and why. New mutation axes are added when a new
postmortem shows the existing axes were too narrow.

Coverage is *the family*, not the single seed. A new SWIM tuning
change that fixes the original 2026-05-25 case but regresses any
sibling case in the family is caught before deploy.

Acceptance: the 2026-05-25 family contains at least the variants
listed above, each parameterized rather than copy-pasted. Running
the family against the current SWIM source either passes all
variants (the deploy is unblocked) or names which variant fails (the
deploy is blocked on that variant).

---

## 6. Boundary-condition probing

After this work, the fuzzer explicitly samples values near boundaries
where distributed systems are historically fragile, in addition to
sampling the interior of declared distributions.

The boundaries are:

- **Size**: messages at exactly the max-payload limit, exactly one
  byte over, exactly one byte under. Piggybacked gossip just below
  the size where the relay starts buffering.
- **Timing**: faults at exactly the suspicion-timeout, exactly one
  tick before, exactly one tick after. Probes arriving exactly at
  the deadline. Snapshots taken at the exact moment of a state
  transition.
- **Counts**: peer counts at the minimum supported (N=2), one above
  (N=3, where multi-region failure modes emerge), one above the
  default (N=4). Fault counts that exhaust a recovery budget by one.
- **State transitions**: faults injected during a state transition
  rather than in a stable state — drop the first ack after a node
  enters Suspect, kill a subprocess between `spawn` and the actor's
  first `recv`, partition during a relay's session-renegotiation
  handshake.

These are not separate scenarios. They are sampling biases applied
to the seed search: the fuzzer spends a declared fraction of its
seeds at boundary values rather than at distribution interiors.

Acceptance: a scenario whose `faults` list includes a partition
declared as "between t=1s and t=10s" produces, across a fuzz run,
seeds that placed the partition exactly at SWIM's protocol-period
boundary and seeds that placed it one tick before and after. The
fuzzer's verdict is sensitive to this — a SWIM change that's correct
in the interior but wrong at the boundary fails the run.

---

## 7. Compound and asymmetric faults

After this work, scenarios and the fuzzer can express faults that
are simultaneously active, faults that overlap in defined ways, and
faults that are directionally asymmetric.

The required shapes:

- **Stacking**: two faults active during the same window. A partition
  active during a relay-session flap. A clock skew active during a
  subprocess respawn.
- **Asymmetry**: a partition that drops A→B traffic but allows B→A.
  A relay-eviction that affects one peer's outbound but not its
  inbound. Latency that is one-way slow.
- **Ordering**: fault X starts exactly when fault Y ends, or with a
  declared overlap, or with a declared gap.
- **Multi-victim**: one fault scoped to one peer pair, another scoped
  to a different peer pair, neither aware of the other.

Single faults are an under-sampled corner of the state space, not
the typical one. The implementations of (5) and (6) compose into (7)
by default — a postmortem family that mutates one axis at a time is
incomplete; the fuzzer samples joint mutations as well.

Acceptance: a scenario expressing "partition A↛B from t=2s, relay
session A↮R closes at t=3s, clock skew on B starts at t=4s" loads,
runs, and is replayable from `(scenario, seed)`. A SWIM regression
that is correct under each fault alone but wrong under the stack is
caught by the fuzzer.

---

## 8. Heavy-tailed distributions

After this work, every distribution the sim samples from has a
declared shape, and the shape defaults are heavy-tailed rather than
Gaussian.

Real network latency, real GC pause, real disk write, real subprocess
startup, and real cross-region RTT are heavy-tailed. A Gaussian
model with mean and stddev calibrated against a live bundle's median
will undersample the p99 by orders of magnitude, and most production
bugs live in the p99.

The sim's distributions are parameterized as
`(median, p99, max)` or `(median, shape, scale)` for log-normal /
Pareto, with the default-fitted parameters drawn from the calibration
bundles. A scenario can override per-link; the fuzzer samples each
seed from the declared distribution.

The fuzzer also exercises a "tail-amplified" mode that increases the
probability of drawing from the upper tail. This is the cheap
substitute for "run the sim for sim-years and hope a rare event
fires" — we move the rare events to the head of the distribution and
visit them in seconds.

Acceptance: a calibration scenario configured against `vastai-N3-2`
produces latency distributions whose p50, p95, and p99 fall within
stated tolerances of the live bundle's. The tail-amplified mode of
the same scenario produces a p99-heavy bundle in proportionally less
sim time.

---

## 9. Mid-recovery faults

After this work, the fuzzer routinely injects faults during recovery
phases, not only during steady state.

The recovery phases the sim recognises:

- During partition heal — the moment the network model resumes
  delivery on a previously-cut link.
- During SWIM's transition out of Suspect.
- During an iroh relay-session renegotiation after a close.
- During a subprocess respawn between exit and the new process's
  first protocol output.
- During the orchestrator's transition from "waiting for SWIM
  convergence" to "ready to accept inference."

A fault injected during recovery is a different bug class from a
fault injected during steady state. The fuzzer should not have to
discover the recovery windows itself; they are observable in the
event stream (or in declared scenario phases) and the fuzzer uses
them as sampling targets.

Acceptance: a scenario that partitions, heals, and then partitions
again exactly during the heal-induced SWIM gossip burst, reproduces
deterministically and exercises a code path that the steady-state
version of the same partition does not.

---

## 10. Failure library and postmortem-driven growth

After this work, the simulator's scenario library grows by one
family per postmortem. The growth is part of the postmortem-closure
checklist: a deploy failure is not considered "closed" until the
sim's library contains a scenario family that reproduces it and the
fix passes the family.

The library is a directory; each family is a subdirectory containing
the original-incident scenario, the mutation-axes declaration, and a
short prose comment naming the failure and pointing at the
postmortem. The directory layout is part of the contract.

A postmortem that closes without contributing a family is allowed
only when the implementer states, in the postmortem, why the failure
mode is structurally unrepresentable in the sim — and that is a
separate behavior contract:

- **Sim-blind-spot inventory.** Each such postmortem appends an
  entry to a `SIM_BLIND_SPOTS.md` adjacent to the library. The entry
  names the failure mode and the structural reason. Closing a
  blind-spot entry is a separate work item, prioritized by how often
  that mode has been hit since.

The library and the blind-spot list together are the answer to "have
we tested for this." There is no third place.

Acceptance: the library contains a family for each of the eight
prior live failures. `SIM_BLIND_SPOTS.md` contains an entry for each
mode not yet representable.

---

## 11. Adversarial scheduling

After this work, when the engine has a choice of which of several
ready events to dispatch first (two messages scheduled for the same
virtual nanosecond, two timers firing simultaneously), it does not
choose uniformly at random. Under the seed-driven exploration of
section 4, a fraction of seeds use an *adversarial* tie-break: prefer
the dispatch order that exercises an under-visited code path or
crosses a state-machine boundary.

The adversarial scheduler is not a model checker. It does not
enumerate orderings. It biases tie-breaks by a heuristic — for
example, prefer delivering the message whose target host has not
received any message in the longest virtual time, or prefer firing
the timer that fires least often across the seed batch.

Cheap to implement, cheap to run, and historically effective at
finding race conditions in actor systems. The fuzzer's "adversarial"
mode is the lever that lifts seed-driven exploration from random to
targeted.

Acceptance: a scenario that has a known race condition (e.g. SWIM
ack arrives the same nanosecond as the suspicion timer fires)
produces a fuzzer verdict that includes that race even when the race
is reachable from only a small fraction of tie-break orderings.

---

## 12. Sub-second reproduction

After this work, the developer's loop is:

1. Run the fuzzer against the current source. Failure prints the
   seed.
2. Run the replay command with the printed seed. Bundle written
   under one second.
3. Inspect the bundle. The broken invariant is named; the violating
   event and host are identified.
4. Edit the source. Re-run step 1.

Steps 1–3 are sub-second per iteration. The total loop time is
dominated by the developer's reading and editing, not by the sim.
This is the property that makes (5)+(6)+(11) worth doing — each
mutation costs nothing.

When the loop time grows above one second per iteration for a
3-node scenario, that is a regression in the simulator and is
addressed before further hardening work.

Acceptance: a continuous-integration job runs the full sim library
against the current source on every PR in under three minutes of
wall time on the project's CI tier. The same job, run locally,
completes in under thirty seconds on the developer's machine.

---

## Implementation order

Grouped by independence. Within a group, work is parallel-safe;
across groups, later groups depend on earlier groups' contracts being
agreed but not finished.

**Group A — production-code coverage**
- 1 (production code paths in the sim) — the load-bearing piece.
  Until this lands, every other section's adversariality is testing
  a model rather than the deploy artifact.

**Group B — fuzz and feedback**
- 2 (fault catalog) — depends on A naming the hosts that can be
  faulted.
- 3 (mid-run invariants) — independent of B's other pieces.
- 4 (seed-driven exploration) — depends on 2 and 3.

**Group C — adversarial sampling**
- 5 (heuristic extrapolation) — depends on 4.
- 6 (boundary-condition probing) — depends on 4.
- 7 (compound and asymmetric faults) — depends on 2 and 4.
- 8 (heavy-tailed distributions) — depends on 4 only.
- 9 (mid-recovery faults) — depends on 4.

**Group D — library and process**
- 10 (failure library and postmortem-driven growth) — process
  contract, can be drafted in parallel with any of the above.

**Group E — scheduling and loop time**
- 11 (adversarial scheduling) — depends on 4 and is cheap; lands
  late because the gain is marginal until the rest of B and C are
  in place.
- 12 (sub-second reproduction) — continuous obligation; a
  regression in this section blocks merges of the others.

The eight prior live failures would have been caught with A + B + C
alone. D + E are how the next eight are caught.

---

## What this spec does not promise

- It does not promise that the sim catches every bug. It promises
  that the sim catches the bug classes prior deploys have produced
  and the bug classes structurally adjacent to them.
- It does not promise the sim replaces a staging deploy. It promises
  that a staging deploy that follows a clean sim run is not a
  diagnostic exercise — it's a confirmation.
- It does not promise that fuzz runs are exhaustive. It promises
  that fuzz runs are dense around the parts of the state space we
  have evidence are dangerous.
- It does not promise sim-prod fidelity at the byte level for every
  field. It promises bundle-shape parity and behavioral parity for
  the actors named in section 1.

A simulator that satisfies this spec is the gate between the
developer and the next two-dollar GPU bill. It does not eliminate
that bill; it earns it.
