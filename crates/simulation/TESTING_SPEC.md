# Deterministic Testing Spec for the Simulator

> Companion to [`NORTH_STAR.md`](./NORTH_STAR.md), [`SPEC.md`](./SPEC.md),
> and [`OBSERVABILITY.md`](./OBSERVABILITY.md). Read those first.
>
> This document is the closed, enumerated contract of binary pass/fail
> checks the simulator must satisfy before it is considered built. It
> exists because the simulator's quality bar — observable equivalence
> with production, per NORTH_STAR — is statistical, and statistical
> bars are asymptotic by nature: there is always a metric just over
> the line. An asymptote cannot be the agent's done-criterion. The
> agent's done-criterion is "every check in §2–§12 of this document is
> green." Statistical parity (the calibration loop, SPEC §9) lives
> outside this spec and is gated by humans.
>
> The simulator is being built by a code agent operating without
> direct feedback. This spec, the three docs above, and the test
> suite it mandates are the only signals the agent has. Sections of
> this document must therefore be read as binding constraints, not
> guidelines.

## Table of contents

0. Reading instructions
1. Test corpus
2. Determinism oracle
3. Replay isomorphism
4. Facade integrity
5. Same-binary invariant
6. Schema floor coverage
7. Schema round-trip
8. Causality and time invariants
9. Equivariance
10. Adversarial sim-detector
11. Lifecycle observability
12. Test infrastructure integrity
13. Definition of done
14. Failure protocol
15. Out of scope
16. Glossary

---

## 0. Reading instructions

**Each section below names tests that must exist and pass.** The
checks are binary: zero tolerance, no thresholds, no "approximately."
Wherever a section calls a check binary, that is the literal
expectation — same SHA-256, identical byte ranges, identical record
counts, identical sorted symbol sets.

**The check list is closed.** New checks may be *added* (extensions
to §10 in particular are expected as new detection techniques are
discovered) but no check in this document may be removed, relaxed,
weakened, marked `#[ignore]`, gated behind an env var, or skipped
on any platform. Any change to this file or to the parity-bar test
directory requires a separate commit that is gated by §12.1.

**Tests are addressed to the code, not to themselves.** Per the
project conventions in `CLAUDE.md`, white-box / structural tests
("does this call this function") are not acceptable substitutes for
the checks listed here. Where a check is a scenario test, write the
scenario. Where it is a property, use property machinery. Where it
is an invariant on real output, assert against the real output.

**Where a check needs a fixture that does not yet exist, the agent
is to create the fixture inside the parity-bar tree, document its
provenance in `tests/parity-bar/fixtures/PROVENANCE.md`, and proceed.**
Fixtures are not optional. Tests that depend on fixtures are not
optional. There is no "future work" exit.

---

## 1. Test corpus

The schema authority for this version of the simulator is a single
production bundle from the `ds-inference` branch's N=3 vastai
investigation (see `examples/pipeline-parallel-inference/N3_DEPLOYMENT_REPORT.md`).
That bundle is the floor: every record kind present in it must be
emittable by the simulator; every field present in it must be
populated or explicitly `None` per OBSERVABILITY §6 rule 1; every
schema version in it must be readable by the post-processor against
which the simulator is checked.

### 1.1 Location

```
crates/simulation/tests/parity-bar/fixtures/
  vastai-n3-1/                   # the reference bundle (unpacked tar)
    MANIFEST.json
    orchestrator/{boot,snapshots/,events/,finalize}.json
    stage-0/{...}
    stage-1/{...}
    stage-2/{...}
    collector.log
  PROVENANCE.md                  # how this bundle was produced; do not touch
```

### 1.2 Corpus scope

This is the *only* prod-origin corpus the simulator is checked
against in v1. Schema-floor coverage (§6) is bounded by what this
bundle exposes; the simulator is not obligated to emit record kinds
that do not appear here, and is *forbidden* from emitting record
kinds that do not appear in either this bundle or OBSERVABILITY §3.

When additional prod bundles are checked in, the corresponding
sections of §6 expand. The expansion is a separate, human-gated
commit (§12.1).

### 1.3 Bundle modifications

The fixture bundle is immutable. The agent does not touch any file
under `tests/parity-bar/fixtures/`. Repairs to the corpus (e.g., a
field renamed in the schema) happen in a separate commit that also
updates the lock hash in §12.1.

---

## 2. Determinism oracle

Discharges SPEC §2.1, §2.4, §2.5.

### 2.1 Two-run byte equality

```
crates/simulation/tests/parity-bar/t_determinism.rs::two_run_byte_equality
```

Two invocations of the engine with the same `(topology spec, seed)`
produce two bundles whose every regular file has the same SHA-256.
Includes NDJSON byte order, tar header timestamps if present, and
the contents of `sim/` (§6.4 in SPEC).

Binary: zero file-level hash differences. Failure must name every
diverging path.

### 2.2 Two-process concurrent equality

```
crates/simulation/tests/parity-bar/t_determinism.rs::two_process_equality
```

The same `(spec, seed)` run in two separate OS processes concurrently
(spawned by the test harness, `std::process::Command` or equivalent)
produces byte-identical bundles. Catches non-determinism from
process-global state: allocator addresses, vtable layout under
ASLR, environment variable order, thread-local randomness.

Binary: zero file-level hash differences. The two processes must be
the same binary invoked twice; not two compilations.

### 2.3 Debug/release equality

```
crates/simulation/tests/parity-bar/t_determinism.rs::debug_release_equality
```

The same `(spec, seed)` run with `cargo run --release` and
`cargo run` (debug) produces byte-identical bundles. If this fails,
the cause is almost always floating-point ordering; the engine
must not use floating-point arithmetic on any code path whose
output reaches the recording.

Binary: zero file-level hash differences across profiles.

### 2.4 No wall-clock taint

```
crates/simulation/tests/parity-bar/t_determinism.rs::no_wall_clock_taint
```

Set `FAKETIME` (or equivalent libfaketime injection) to advance the
host clock by 1 hour. Run the engine. The bundle must be identical
to a run without the injection. Proves no kernel-time leak past the
facade (SPEC §2.4, `SystemTime`/`Instant` row).

Binary: zero file-level hash differences with vs. without injection.

### 2.5 Divergence detection

```
crates/simulation/tests/parity-bar/t_determinism.rs::divergence_detected
```

Run with `(spec, seed)`, store the bundle hash chain. Modify a
single byte in the engine's RNG output (via a `#[cfg(test)]`
poisoned-RNG facade switch — the *only* such test-only switch
permitted, and it must live in the sim facade crate). The divergence
detector (SPEC §2.5) must produce a structured `Error` event naming
the first divergent record and exit non-zero.

Binary: divergent run exits non-zero; clean run exits zero.

---

## 3. Replay isomorphism

Discharges SPEC §7.

### 3.1 Self-replay

```
crates/simulation/tests/parity-bar/t_replay.rs::self_replay_identical
```

Run the engine to produce bundle B₁. Pass B₁ to the engine in
replay mode (`--replay B₁/`). Produce B₂. B₂ must be byte-identical
to B₁.

This is the load-bearing test of recording self-sufficiency: any
piece of state needed to drive a replay must be in the recording.
If B₂ differs from B₁, the recording is missing information SPEC
§7.1 demands it carry.

Binary: zero file-level hash differences.

### 3.2 Mutation fidelity

```
crates/simulation/tests/parity-bar/t_replay.rs::mutations_preserved
```

A topology spec declaring `partition(at_ms=5000)`, `heal(at_ms=8000)`,
`restart(node=stage-1, at_ms=10000)` is run. The resulting bundle
must contain `Custom` mutation events for each, with virtual-time
`wall_ms` exactly matching the spec, in spec order, on the
recording streams SPEC §4.1 and §5.3 designate.

Binary: exact count, exact ordering, exact `wall_ms` per mutation.

### 3.3 Recording self-sufficiency under prod shape

```
crates/simulation/tests/parity-bar/t_replay.rs::replay_from_prod_shape_only
```

Run the engine to produce bundle B. Delete the `sim/` subtree from
B (SPEC §6.4 — the sim-only fields). Pass the stripped bundle to
replay. The replay must complete; the resulting bundle's non-`sim/`
files must be byte-identical to B's non-`sim/` files.

This proves replay does not depend on sim-only data leaking into
the prod-shape part of the recording. A prod bundle, which never
has `sim/`, must replay just as well.

Binary: zero file-level hash differences on non-`sim/` files;
replay exits zero.

### 3.4 Prod-bundle replay (corpus-anchored)

```
crates/simulation/tests/parity-bar/t_replay.rs::vastai_n3_replays
```

Pass `tests/parity-bar/fixtures/vastai-n3-1/` to the engine in
replay mode. The engine must:

- Reconstruct topology and peer set from the bundle's boot blocks
  (SPEC §7.1).
- Reconstruct mutation schedule from `Custom` events in the bundle.
- Complete the replay without panicking.
- Emit a bundle in the same layout (§6.4, §7.1).

This test does *not* require the emitted bundle to be byte-identical
to the input bundle. It is sufficient that the replay completes and
produces a structurally valid output. Statistical comparison
between input and output is a calibration concern (SPEC §9) and is
out of scope for this spec (§15).

Binary: replay exits zero; output bundle passes §7 checks.

---

## 4. Facade integrity

Discharges SPEC §3, §2.4.

### 4.1 Banned-API lint

```
crates/lint-deterministic/                    # new crate
crates/lint-deterministic/banned.toml          # the closed list
crates/lint-deterministic/tests/banned_apis_rejected.rs
```

A custom `cargo xtask lint-deterministic` step runs in CI before any
test. It rejects the workspace build if any module outside the
sim-facade implementation references the banned-API set. The set
is closed and version-locked in `banned.toml`. The current set:

```
std::time::SystemTime         → Facade::clock().now()
std::time::Instant            → Facade::clock().now()
std::thread::sleep            → Facade::clock().sleep_until(...)
std::thread::spawn            → Facade::spawn(...)
tokio::spawn                  → Facade::spawn(...)
tokio::time::sleep            → Facade::clock().sleep_until(...)
tokio::time::Instant          → Facade::clock().now()
std::collections::HashMap     → indexmap::IndexMap or BTreeMap
std::collections::HashSet     → indexmap::IndexSet or BTreeSet
rand::thread_rng              → Facade::rng(stream_label)
getrandom::getrandom          → Facade::rng(stream_label)
std::env::var                 → Facade::env(name)
std::env::vars                → Facade::env_iter()
std::fs                       → Facade::fs() (sandboxed per node)
std::net                      → Facade::udp() / Facade::tcp()
std::process::Command         → Facade::process(...) (errors in sim)
```

Allowlist (paths exempt from the lint):
```
crates/simulation/src/facade/sim/         # the sim facade itself
crates/runtime-facade/src/prod/           # the prod facade itself
crates/lint-deterministic/                # the lint itself
```

Binary: zero violations across the workspace.

### 4.2 Feature exclusivity

```
crates/simulation/tests/parity-bar/t_facade.rs::feature_exclusivity
```

A test in the workspace `xtask` confirms that `cargo build` with
features `facade-prod facade-sim` fails (compile error from a
`compile_error!` macro in the facade crate). The same test confirms
that `cargo build` with neither feature also fails.

Binary: both invocations exit non-zero with the expected error string.

### 4.3 No `cfg(sim)` in peer code

```
crates/lint-deterministic/tests/no_cfg_sim_in_peer.rs
```

```
grep -r 'cfg(\s*sim\s*)' \
  crates/distribution \
  crates/swactor \
  crates/node \
  crates/process \
  examples/
# must return zero matches
```

Peer code never branches on whether it is in the sim. The facade
is the only swap point (SPEC §3.4). Code that compiles
conditionally on sim-vs-prod inside a peer crate is a violation.

Binary: zero matches.

### 4.4 Facade trait surface is closed

```
crates/runtime-facade/tests/surface_locked.rs
```

The trait family exposed by `runtime-facade` is fingerprinted (each
trait's method signatures hashed in declaration order, hashes
concatenated). The fingerprint is stored in
`crates/runtime-facade/surface.lock`. Changes to the trait surface
require updating this lock in a separate commit gated by §12.1.

Binary: live fingerprint equals locked fingerprint.

---

## 5. Same-binary invariant

Discharges SPEC §3.4, §4.6, §5.1.

### 5.1 Shared load-bearing dependencies

```
crates/simulation/tests/parity-bar/t_same_binary.rs::shared_load_bearing
```

`cargo metadata` for the prod binary and the sim driver must list
the following crates, at *identical resolved versions*, in both
dependency graphs:

```
iroh
iroh-relay
quinn
quinn-proto
swactor                     # the SWIM implementation
distribution::diagnostics
postcard                    # codec
```

The set is closed in this spec. Adding to it requires a §12.1
commit.

Binary: exact set equality at exact version equality on both sides.

### 5.2 No sim-only forks of transport code

```
crates/simulation/tests/parity-bar/t_same_binary.rs::no_transport_forks
```

The sim has zero source files matching any of:
- `**/sim_swim/**`
- `**/sim_iroh/**`
- `**/swim_sim.rs`
- `**/iroh_sim.rs`
- `**/quinn_sim.rs`

The transports under sim are the production transports linked
against the sim facade (SPEC §4.6). Replacement implementations
defeat the purpose of the sim.

Binary: zero matches.

### 5.3 Symbol overlap floor

```
crates/simulation/tests/parity-bar/t_same_binary.rs::symbol_overlap
```

`nm` (or `cargo bloat`) on the prod binary and the sim driver
produces two symbol sets. Their intersection must contain every
public symbol from the §5.1 crates. The check is exact: any
listed crate's public surface missing from either binary fails.

Binary: full inclusion in both directions.

---

## 6. Schema floor coverage

Discharges OBSERVABILITY §3, NORTH_STAR §"Parity bar". Scope bounded
by the v1 corpus (§1).

### 6.1 Corpus record-kind census

```
crates/simulation/tests/parity-bar/t_schema_coverage.rs::corpus_kinds
```

The reference bundle (§1.1) is parsed at test start. The set of
record kinds present is computed and stored as the required set.
A reference sim scenario (`tests/parity-bar/fixtures/scenarios/reference.toml`)
is run. Every record kind in the corpus set must be present in the
sim output.

This is the *floor*: the sim must emit at least what prod emits.
The agent does not need to invent record kinds the corpus does not
exhibit.

Binary: corpus kinds ⊆ sim-output kinds.

### 6.2 No phantom records

```
crates/simulation/tests/parity-bar/t_schema_coverage.rs::no_phantom_records
```

The sim must not emit record kinds that are not in either:
- The reference bundle (§1.1), or
- The schemas enumerated in OBSERVABILITY §3 / §4.

This prevents the sim drifting toward sim-specific schemas that
the prod side cannot consume. Sim-only fields (SPEC §6.5) live
under `sim/` and are excluded from this check.

Binary: sim-output kinds ⊆ (corpus kinds ∪ OBSERVABILITY-documented kinds).

### 6.3 Field presence audit

```
crates/simulation/tests/parity-bar/t_schema_coverage.rs::no_silent_none
```

For every record emitted by the reference scenario, every field
documented in OBSERVABILITY §3 must be either:
- Populated (non-null), or
- Explicitly `None` with at least one `Error` event somewhere in
  the bundle whose `component` matches the introspector responsible
  and whose `message` notes the gap.

A silent `None` (no accompanying `Error`) is a failure (OBSERVABILITY
§6 rule 1).

Binary: zero silent-None fields.

### 6.4 Variant exhaustiveness

```
crates/simulation/tests/parity-bar/t_schema_coverage.rs::all_event_variants_fire
```

The reference scenario (which is a multi-mode scenario combining
boot, steady-state, partition, heal, restart, and shutdown) must
fire every `Event` variant the corpus exhibits. Variants present
in OBSERVABILITY §3.3 but absent from the corpus are *not* required
in v1.

Binary: corpus variants ⊆ sim-emitted variants.

### 6.5 Reference scenario contents

```
crates/simulation/tests/parity-bar/fixtures/scenarios/reference.toml
```

A 60-second scenario carrying:
- 3 sim-native peers in a chain (mirrors the N=3 corpus layout)
- 1 orchestrator
- 1 collector
- Boot, steady-state SWIM ticks for 20s
- Partition at t=20s isolating stage-2
- Heal at t=35s
- Restart of stage-1 at t=45s
- Clean shutdown at t=60s

This scenario is the floor on which §6.1–§6.4 run. It is the only
scenario whose contents §6 binds to. New scenarios can be added in
`tests/exploratory/`; they do not extend the §6 contract.

---

## 7. Schema round-trip

Discharges SPEC §6, OBSERVABILITY §3.

### 7.1 Prod parser eats sim bundle

```
crates/simulation/tests/parity-bar/t_round_trip.rs::prod_parser_reads_sim_bundle
```

The reference-scenario sim bundle is passed to the production
post-processor (`crates/distribution/src/diagnostics/postproc`). The
post-processor must return `Ok(_)` and its `warnings` array must
have zero `unknown_field` and zero `unknown_record_kind` entries.

Binary: post-processor exit zero, zero unknown-* warnings.

### 7.2 Sim parser eats prod bundle

```
crates/simulation/tests/parity-bar/t_round_trip.rs::sim_replay_parser_reads_prod_bundle
```

The corpus bundle (§1.1) is passed to the sim's replay loader.
The loader must return `Ok(_)` and emit zero `unknown_field` /
`unknown_record_kind` warnings. The sim parser and the prod parser
are the *same code* (per SPEC §8.1) — this test exists to confirm
that fact, not to test a separate codepath.

Binary: replay-loader exit zero, zero unknown-* warnings.

### 7.3 Bundle layout exact

```
crates/simulation/tests/parity-bar/t_round_trip.rs::bundle_layout_matches_spec
```

`tar tf` of the reference sim bundle is compared against the
expected file list:

```
{run_id}/
  MANIFEST.json
  orchestrator/boot.json
  orchestrator/snapshots/*.json
  orchestrator/events/*.json
  orchestrator/finalize.json
  stage-N/boot.json                   # for each peer
  stage-N/snapshots/*.json
  stage-N/events/*.json
  stage-N/finalize.json
  sim/spec.toml
  sim/seed
  sim/links_applied.json
  sim/mutations.log
  sim/wire/*.ndjson                   # if wire capture is enabled
```

Files that are not listed above must not be present. Files that are
listed but conditional (e.g., `wire/`) are present iff the run
config enabled them.

Note: the prod `collector.log` (SPEC §6.4) is not yet a binding
artifact in v1 — the collector binary itself lands with the
calibration loop (§15). The file's fixed bundle-root path conflicts
with §9.1's substring rename map (the engine writing a fixed
filename in both base and renamed runs forces a key mismatch under
`apply_path_rename`). v2 reintroduces the requirement once the
collector binary's diagnostics output names itself from the
collector host's spec name.

Binary: exact match modulo conditional sections.

### 7.4 Schema version pin

```
crates/simulation/tests/parity-bar/t_round_trip.rs::schema_version_pinned
```

Every record envelope's `schema_version` field equals the version
exported as `distribution::diagnostics::SCHEMA_VERSION`. The
constant is single-source.

Binary: zero records with a divergent `schema_version`.

---

## 8. Causality and time invariants

Discharges SPEC §2.2, §2.3, OBSERVABILITY §3.3.

### 8.1 `monotonic_seq` strictly increasing

```
crates/simulation/tests/parity-bar/t_causality.rs::monotonic_seq_strictly_increasing
```

For each `(node_id, boot_sequence)` in the reference bundle, the
sequence of `monotonic_seq` values across all records (events and
snapshots) is strictly increasing and contiguous (no gaps that
would imply lost records).

Binary: zero violations.

### 8.2 `wall_ms` non-decreasing modulo declared jumps

```
crates/simulation/tests/parity-bar/t_causality.rs::wall_ms_non_decreasing
```

For each node, the sequence of `wall_ms` values is non-decreasing,
except across instants marked by a `Custom { kind: "clock_jump" }`
event (which the sim emits when a topology-declared clock jump
fires per SPEC §4.7). Across each clock_jump, the delta in
`wall_ms` is allowed; before and after, monotonicity is required.

Binary: zero unexplained non-monotonic transitions.

### 8.3 Send precedes receive

```
crates/simulation/tests/parity-bar/t_causality.rs::send_precedes_receive
```

For every `MessageReceived { peer = P, kind = K, ... }` on node A,
there exists a `MessageSent { peer = A, kind = K, ... }` on node P
whose `wall_ms` is at least `link(P→A).one_way_delay_ms − link.jitter_ms`
earlier (in virtual time). The match is by `(peer, kind, size,
trace_id?)` — once causal trace IDs land (OBSERVABILITY §4.2),
the match becomes exact.

Binary: every received-message record has a matching causal sender.

### 8.4 `snapshot_id` uniqueness

```
crates/simulation/tests/parity-bar/t_causality.rs::snapshot_id_unique
```

Across the entire bundle, no two snapshots share a `snapshot_id`.

Binary: set size equals list length.

### 8.5 Tiebreaker order

```
crates/simulation/tests/parity-bar/t_causality.rs::executor_tiebreaker_deterministic
```

Construct a scenario in which three events fire at the same
virtual tick on the same node. The executor's order of resolution
must follow `(node_id, fiber_id, event_seq)` per SPEC §2.3. The
test runs the scenario twice and asserts the per-tick resolution
order is identical.

Binary: identical order across two runs.

---

## 9. Equivariance

Discharges SPEC §4.1.

### 9.1 Node-id rename invariance

```
crates/simulation/tests/parity-bar/t_equivariance.rs::rename_invariance
```

Spec₁ defines nodes `[alpha, beta, gamma]` with a given topology
graph. Spec₂ is identical but renames `[alpha→x, beta→y, gamma→z]`,
applying the rename consistently to every peer-set reference.
Both run with the same seed.

The bundles must be byte-identical after applying the rename map
to the output (file paths under `{node_id}/` get renamed, and
every occurrence of the old `node_id` strings in record contents
gets substituted).

Binary: zero diffs after rename-map application.

### 9.2 Spec ordering invariance

```
crates/simulation/tests/parity-bar/t_equivariance.rs::spec_ordering_invariance
```

Spec₁ lists nodes in `[alpha, beta, gamma]` order; spec₂ lists the
same nodes in `[gamma, alpha, beta]` order; same seed. The
canonical ordering (sorted by `node_id`) used by the engine means
the bundles must be byte-identical.

Binary: zero diffs.

### 9.3 Same-tick mutation reordering

```
crates/simulation/tests/parity-bar/t_equivariance.rs::same_tick_mutation_reorder
```

Two mutations scheduled at the same virtual tick (e.g.,
`partition(at_ms=5000, edges=[a-b])` and `partition(at_ms=5000, edges=[b-c])`)
are specified in two different orders across two spec files; same
seed. The bundles must be byte-identical.

Binary: zero diffs.

---

## 10. Adversarial sim-detector

Discharges NORTH_STAR §"Parity bar" ("must have no means, statistical
or otherwise, of detecting that it is in a sim").

### 10.1 Detector binary

```
crates/sim-detector/                      # new crate
crates/sim-detector/src/lib.rs            # enumerated detection techniques
crates/sim-detector/src/main.rs           # peer binary
```

The detector is a peer binary linked through the runtime facade.
It compiles in both `facade-prod` and `facade-sim` configurations.
Its job is to report, for each technique, one of:

```
enum Verdict {
    Indistinguishable,
    DetectedSim(&'static str),
    DetectedProd(&'static str),
}
```

A passing sim run is one where every technique returns
`Indistinguishable` or `DetectedProd`. A passing prod run is one
where every technique returns `Indistinguishable` or `DetectedSim`.
The detector exists to be wrong about *which* environment it is in
— it must never correctly identify the sim.

### 10.2 Closed enumerated technique set

The v1 set (closed; extensions require §12.1):

| ID  | Technique                                                  |
|-----|------------------------------------------------------------|
| D01 | `SystemTime::now()` drift across `sleep(1s)`               |
| D02 | `Instant::now()` vs facade clock alignment                 |
| D03 | `HashMap` iteration order across two identical inserts     |
| D04 | `Box<dyn Trait>` vtable address stability across runs      |
| D05 | `getrandom` entropy distribution test (chi-squared shape)  |
| D06 | Spawn ordering on 1000 tasks in a tight loop               |
| D07 | UDP loopback timing vs declared link delay                 |
| D08 | DNS resolution latency vs declared resolver latency        |
| D09 | Hostname / process-pid uniqueness across two peers         |
| D10 | Allocator address-range stability across runs              |
| D11 | TCP loopback handshake count (sim TCP per SPEC §4.5)       |
| D12 | `Thread::id()` stability across two facade `spawn`s        |

The detector binary, run inside the reference scenario as an
additional peer, must produce verdicts that satisfy the §10.1 rule.

```
crates/simulation/tests/parity-bar/t_detector.rs::sim_indistinguishable
crates/simulation/tests/parity-bar/t_detector.rs::prod_baseline
```

Binary: zero `DetectedSim` verdicts in sim runs; zero `DetectedProd`
verdicts in prod runs. `prod_baseline` is a sanity check that runs
the detector against the prod facade and confirms it does *not*
falsely report sim there.

### 10.3 Technique provenance

Every entry in §10.2 is documented in
`crates/sim-detector/src/techniques.md` with:

- The exact mechanism (which API, what value range constitutes detection).
- The reason it would betray a naive sim.
- The facade obligation that prevents it.

The agent must update this file when adding a technique (and must
not add techniques without a §12.1 commit).

---

## 11. Lifecycle observability

Discharges SPEC §5.3.

### 11.1 Boot-record presence

```
crates/simulation/tests/parity-bar/t_lifecycle.rs::start_emits_boot
```

Every `start_at_ms` in the spec produces exactly one `boot.json`
under that node's directory, with `boot_sequence = 0`.

Binary: one boot record per start declaration; `boot_sequence` correct.

### 11.2 Restart sequence integrity

```
crates/simulation/tests/parity-bar/t_lifecycle.rs::restart_increments_boot_sequence
```

For a node with `start_at_ms=0, restart_at_ms=[10000, 20000]`, the
bundle contains:

- `boot.json` with `boot_sequence=0` and a `finalize.json` ending the first epoch.
- A second epoch with `boot.json` `boot_sequence=1` and a `finalize.json`.
- A third epoch with `boot.json` `boot_sequence=2` (still running at end-of-run
  so finalize may be absent if the run ends mid-epoch — see §11.5).

Binary: exact count, exact `boot_sequence` values, exact order.

### 11.3 Crash distinguishability

```
crates/simulation/tests/parity-bar/t_lifecycle.rs::crash_omits_finalize
```

A node with `crash_at_ms=5000` and no further restart produces a
`boot.json` and *no* `finalize.json`. The engine emits a sim-only
`Custom { kind: "crash" }` record on the recording so the
post-processor can distinguish crash from incomplete capture.

Binary: missing `finalize.json` exactly when crashed; `Custom`
record present.

### 11.4 Clean shutdown

```
crates/simulation/tests/parity-bar/t_lifecycle.rs::stop_emits_clean_finalize
```

A node with `stop_at_ms=5000` produces a `finalize.json` whose
`shutdown_reason = "clean"` (or whatever the prod schema names
clean shutdown).

Binary: `finalize.json` present with correct reason.

### 11.5 Mid-run finalization

```
crates/simulation/tests/parity-bar/t_lifecycle.rs::end_of_run_finalizes_all_living
```

When the engine reaches its terminal virtual time with nodes still
running, each living node receives a synthetic clean shutdown and a
`finalize.json` is written. The synthetic shutdown is marked
(`shutdown_reason = "end_of_run"`).

Binary: every node has either an organic finalize, a crash mark, or
an `end_of_run` finalize. No node ends a run with neither.

---

## 12. Test infrastructure integrity

Discharges this document itself.

### 12.1 Parity directory hash lock

```
scripts/check-parity-lock.sh
tests/parity-bar/.locked-hashes
```

CI's first step computes the recursive SHA-256 of every file under
`crates/simulation/tests/parity-bar/` (excluding `.locked-hashes`
itself) and compares against the value committed in `.locked-hashes`.
Mismatch fails the build with an instruction to run
`scripts/update-parity-lock.sh` in a *separate* commit titled
`parity-bar: update lock`.

This prevents the agent from quietly weakening tests. The same
guard applies to this file: `crates/simulation/TESTING_SPEC.md` is
inside the lock.

Binary: computed hash equals committed hash.

### 12.2 No probabilistic primitives in parity tests

```
crates/lint-deterministic/tests/no_probabilistic_primitives.rs
```

Within `crates/simulation/tests/parity-bar/`, the following are
banned (compile-error if present):

```
proptest::*
quickcheck::*
rand::random
rand::thread_rng
fn fuzz_*
```

Parity-bar tests use seeded, committed inputs only. Probabilistic
testing belongs in `crates/simulation/tests/exploratory/`, which is
not part of this spec's binary checks.

Binary: zero matches.

### 12.3 No conditional skips in parity tests

```
crates/lint-deterministic/tests/no_skips_in_parity.rs
```

Within `tests/parity-bar/`, the following are banned:

```
#[ignore]
#[cfg(not(...))]                          # any cfg gate
if std::env::var(...).is_ok() { return; } # env-driven skip
#[cfg_attr(..., ignore)]
```

Binary: zero matches.

### 12.4 No `unwrap()` masking in parity tests

```
crates/lint-deterministic/tests/parity_tests_use_explicit_assert.rs
```

Within `tests/parity-bar/`, `.unwrap()` on `Result` returned by
engine APIs must be `.expect("<reason>")` with a reason string. This
is a readability lint, not a correctness one, but parity-test
failures must be debuggable from CI output alone — the agent will
not be there to add prints when something fails.

Binary: zero `Result::unwrap()` calls without messages.

### 12.5 Reference scenario locked

The reference scenario (§6.5) and the fixture bundle (§1.1) are
inside the parity directory and therefore are §12.1-protected.

---

## 13. Definition of done

The simulator is considered built when:

- The full check list in §2–§12 runs in CI via `cargo xtask parity-bar`.
- Every check is green.
- The `parity-bar` job is required on the `ds-inference` branch
  before merge.
- `tests/parity-bar/PROVENANCE.md` documents the corpus and its
  source run.
- `crates/sim-detector/src/techniques.md` documents each detection
  technique in §10.2.
- `crates/lint-deterministic/banned.toml` enumerates §4.1's set.
- The reference scenario (§6.5) runs end-to-end and produces a
  bundle passing §6, §7, §8, §11.
- The corpus replay (§3.4) succeeds.

The agent does *not* mark the simulator built when:

- Any check is `#[ignore]`d, gated, or partially-skipped.
- Any check has been replaced with a weaker version.
- Any check passes only on debug or only on release.
- Any check passes only with a specific `RUSTFLAGS` setting.
- Any check requires manual setup not encoded in `cargo xtask`.

Calibration (SPEC §9) is explicitly out of scope for v1's done
criterion. The deterministic substrate this spec defines is what
makes calibration meaningful later.

---

## 14. Failure protocol

When a check defined in this spec fails, the agent's response is
exactly one of:

1. **Fix the simulator** to satisfy the check. This is the default.
2. **Fix the check** by raising a defect in this document, *if and
   only if* the check is wrong in a way that an independent reader
   can verify against NORTH_STAR / SPEC / OBSERVABILITY. The fix is
   a separate commit titled `TESTING_SPEC: correct §N.M`, including
   a justification quoting the upstream doc.

The agent's response is *never*:

- `#[ignore]` the test.
- Replace the test with a weaker one.
- Add a `// TODO: re-enable when …` marker.
- Move the test to `exploratory/`.
- Edit `.locked-hashes` directly.
- Loosen a banned-API list.
- Skip the test on a specific platform or feature.
- Mark the check "blocked on external work" and proceed.

If the agent finds itself unable to fix the simulator and unable to
write a justified spec correction, the work stops. The artifact
checked in is the partial work plus a `BLOCKED.md` in the simulation
crate root naming the check, the symptom, and the obstacle. This is
the only acceptable exit short of green.

---

## 15. Out of scope (v1)

The following are *not* obligations under this spec. The simulator
is not required to satisfy any check below for v1 to be considered
built. Each will become a v2 obligation as the corpus and the
calibration loop mature.

- **Statistical / distributional parity.** KS tests, mean / p99
  parity, tail-shape parity. These are calibration (SPEC §9), gated
  by the noise-floor estimation work that needs a multi-bundle
  corpus.
- **Multi-bundle corpus.** v1's schema floor (§6) is bounded by the
  single vastai-n3 bundle. Adding bundles widens the floor.
- **Wire-level packet trace** (OBSERVABILITY §4.1). Optional in v1;
  the sim implements §4.1 only if a check in §6 binds to it (it does
  not, in v1).
- **Causal trace IDs** (OBSERVABILITY §4.2). Future surface. §8.3's
  send-precedes-receive check uses heuristic matching; once trace
  IDs land, the check becomes exact and §8 expands.
- **Opaque-binary hosts** (SPEC §5.2). v1 ships with sim-native
  peers only. The opaque-binary escape hatch is a v2 obligation.
- **Cross-architecture determinism.** v1 commits to determinism on
  one architecture (linux-x86_64); same-architecture / different-machine
  determinism is the §2 obligation. Cross-arch is a v2 obligation
  if and when a target requires it.
- **Statistical detector techniques.** §10.2's set is structural /
  behavioral. Statistical detectors (e.g., "the variance of inter-message
  intervals matches a sim-implausible distribution") are a v2
  expansion.

Anything listed above that the agent finds an unforced opportunity
to satisfy is welcome to satisfy. None is required.

---

## 16. Glossary

Specific to this spec; for engine / sim terms see SPEC §10.

- **Binary check.** A test whose outcome is pass or fail with no
  intermediate states, thresholds, or knobs. The unit of contract in
  this document.
- **Closed list.** A set defined explicitly in this document, locked
  by §12.1, modifiable only by a separate commit that itself updates
  the lock. New items can be added; existing items cannot be removed
  or weakened.
- **Corpus.** The set of prod-origin bundles the simulator is checked
  against. v1 corpus is §1.1.
- **Detector.** A peer binary (§10.1) attempting to identify whether
  it runs in sim or prod. Used to enforce NORTH_STAR's
  "indistinguishable" requirement as an executable test.
- **Facade.** Per SPEC §3 / §10. Banned-API allowlist scopes
  reference this term.
- **Lock.** The SHA-256 hash committed in
  `tests/parity-bar/.locked-hashes` covering every file in the
  parity-bar directory, this document included.
- **Parity directory.** `crates/simulation/tests/parity-bar/`. The
  only directory whose contents are bound by this spec.
- **Reference scenario.** §6.5. The single scenario whose execution
  is observed by the schema-floor (§6), schema-round-trip (§7),
  causality (§8), and lifecycle (§11) checks.
