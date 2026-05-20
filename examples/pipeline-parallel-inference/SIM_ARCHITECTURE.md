# Simulator architecture for SWIM (and beyond)

Goal: a SWIM/cluster simulator that catches bugs *and* is the substrate for
behavioral optimization. Companion to `N3_DEPLOYMENT_REPORT.md`, which laid
out the bugs the sim is meant to catch.

## What we already have

`crates/simulation` exists and is more capable than I initially assumed.
Strategic decisions below should be read against this baseline, not from
zero.

- **Depth:** runs real `DistributedNode` (SWIM core + actor runtime + name
  registry) under `feature = "distribution"`. Already option (b) from D1
  below.
- **Topology models:** two, neither arbitrary.
  - `Topology` (closed enum: Ring / Star / FullMesh / Chain / Partitioned)
    — used by the gossip sim.
  - `NetworkTopology` (per-node `NodeLocation` of `Public` / `Nat{group}` /
    `Firewalled`, plus a list of relay-node indices) — used by the
    distribution sim. Richer, still not arbitrary per-link adjacency.
- **Faults:** scheduled `Partition` / `Heal`, global drop rate, per-link
  drop rate (uni- or bidirectional), relay penalty.
- **Determinism:** counter-based PRNG (LCG seeded constant). No
  `Instant::now()` in the sim path; rounds advance via an integer tick.
- **Replay:** `SimulationTrace<EventKind, Snapshot>` is the on-disk format,
  and the `dashboard` feature serves an HTML/JS replay UI. This is replay
  of sim-generated traces — not replay of a live diag bundle into the sim.
- **Tests:** ~9 integration tests covering cluster scenarios, deploy
  scenarios, lifecycle, registry, gossip convergence, properties, and
  adversarial topologies.

What it can't do today, mapped to bugs in `N3_DEPLOYMENT_REPORT.md`:

- Cannot reproduce the **canary-buffering Layer A** symptom — no bandwidth
  or queueing model, only per-message drops.
- Cannot reproduce the **SWIM gossip flap (Layer B)** at production timing —
  no latency distribution, no jitter, no per-link RTT. Drops alone don't
  reach the flap regime.
- Cannot consume a real diag bundle as input — no replay-from-production.

## The key insight

The diagnostic bundle format is **the right interface** for this. We already
have:

- `DiagEvent` — structured records for `SwimTransition`, `MessageSent/Received`,
  `DialStarted/Outcome`, `ConnectionCacheHit/Miss`, `IrohConnTypeChanged`,
  `Probe*`, etc.
- Per-node snapshots that capture full SWIM state (peers, incarnations,
  recent_messages, metadata version).
- The post-processor that turns a bundle into `summary.md`, `reachability.tsv`,
  per-direction timelines.

If the sim emits the **same bundle format**, then every tool we already wrote
works against sim runs. More importantly: sim runs and live runs become
**visually comparable** — open both `summary.md` files side-by-side and you can
ask "does the sim's behavior match production?" — which is the operational
test for whether the sim is realistic.

The bundle format is our lingua franca: sim and live both produce it, analysis
tools consume it, and "fidelity" has a concrete definition.

Today the sim emits its own `SimulationTrace` JSON, not a diag bundle. That's
a real gap if we want this insight to pay off.

## Key design decisions, with tradeoffs

### D1 — How deep does the sim go?

**Decided: arbitrary connection topology that swactor might ever support.**

This is broader than the depth question I originally framed. It means the
topology model has to express any graph swactor can run on — not just the
closed shapes in `Topology`, and not just NAT-group adjacency in
`NetworkTopology`. The unit of expression should be a **per-link descriptor**
(possibly directional, possibly with relay path) over the full N×N edge set,
parameterizable per scenario.

What "supports" means:
- Public ↔ public, NAT ↔ NAT, NAT ↔ public via relay.
- Asymmetric reachability (A→B works, B→A doesn't).
- Per-link RTT, bandwidth, loss, jitter, and whether the path is relayed.
- A node can be reachable to some peers and not others — mirrors the
  per-link `IrohConnTypeChanged` reality.

The existing `NetworkTopology` is a starting point but folds reachability
into NAT-group equality. Generalizing to per-edge descriptors is the right
direction.

Depth in the protocol-stack sense (SWIM only vs. SWIM + actors + registry)
is already settled: the sim runs the real `DistributedNode`, which includes
all three.

### D2 — Bit-exact determinism, or "mostly deterministic"?

Open.

- **Bit-exact:** same seed → byte-identical bundle. Requires banning sources
  of nondeterminism — `HashMap` iteration order (use `BTreeMap`), unspecified
  `f64` operations, anything that depends on OS scheduling. Enables
  **delta-debugging** (binary search a seed range to find minimal failing
  input).
- **Mostly deterministic:** same seed → same *outcome* (convergence time,
  final state) but messages may interleave slightly differently. Cheaper but
  limits some advanced uses.

Current state: counter-based PRNG, no clock, integer-tick rounds. That's
already most of the way to bit-exact for the gossip and distribution sims —
but if we add wall-time clocks, latency distributions, and threaded
delivery, that property is easy to lose.

My lean: bit-exact, because we're so close already. Cheap to preserve,
expensive to claw back later.

### D3 — Replay vs. synthesis

**Decided: replay is required.** Highest-fidelity sim possible; if replay
runs are too expensive to run continuously, run them only when needed, but
they must exist.

Two replay modes to keep distinct:

1. **Trace replay** (already shipped via `dashboard`): re-render a previously
   recorded `SimulationTrace`. Cheap. Useful for debugging sim runs.
2. **Bundle replay** (not yet built): consume a real production diag bundle
   and drive the sim's transport queue from its `MessageSent/Received`
   events, with the sim's protocol stack reacting. This is the one that
   reproduces a production failure deterministically.

The doc's earlier framing called replay a "nice-to-have." It's not — this
is the decision.

### D4 — Network model: how realistic?

**Decided: high fidelity.**

Existing model: partitions, drop rate (global + per-link), NAT/firewall +
relay topology, relay penalty.

What's missing for high fidelity, in roughly the order each is required to
reproduce a real bug we've seen:

- **Latency distribution per link** (e.g. `Normal(mean, sd)` or empirical
  CDF). SWIM probe timing is everything; without RTT variance, flap regimes
  don't reproduce.
- **Bandwidth + queue depth per link.** The 187s canary-buffering symptom
  was a queueing phenomenon, not a drop phenomenon — minimal-fidelity models
  silently skip past it.
- **Reorder + jitter.** Big SWIM ack bundles head-of-line block; reorder
  matters.
- **Per-node CPU saturation.** Plausibly relevant if name-registry GC or
  diag flushing competes with SWIM ticks. Lower priority than the link-level
  knobs.

### D6 — Where does the sim live?

**Decided: extend `crates/simulation`.** That's where it already lives. The
earlier "new crate" lean was wrong; the work is to evolve the existing
crate, not to create a parallel one.

Concrete evolution path (for orientation, not commitment):

- Generalize topology to per-edge descriptors (D1).
- Add latency / bandwidth / reorder / jitter to `NetworkState` (D4).
- Add bundle-replay input pathway (D3).
- Add bundle-output sink so sim runs go through `swactor-diag-postproc`
  (the key insight above).
- Audit determinism once any of the above land (D2).

## Open questions

- **D2 (determinism level):** bit-exact, or accept slight nondeterminism
  once latency/threads enter the picture? My lean: bit-exact, because we're
  close already.
- Anything else we want the sim to optimize for that hasn't surfaced in
  D1–D4?
