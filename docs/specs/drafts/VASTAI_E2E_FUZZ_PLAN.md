# VastAI Real-Node E2E Fuzz Plan

## Start here

**Status as of 2026-09-14:** the latest retained run passed five-node Gate A
and the complete local campaign, then exceeded the complete warm-workflow
ceiling before remaining Gate B checks. Its source identity differs from the
reviewed tree. Fresh ordered qualification and paid acceptance remain pending.
Paid execution is blocked.

Checkpoint handoffs: [remote-ready work](VASTAI_REMOTE_READY.md) and
[local blockers, in priority order](VASTAI_LOCAL_BLOCKERS.md). The internal
workload/cleanup budget split is not a separate blocker for this checkpoint
review; the handoffs leave runtime limits and paid-access guards unchanged.

This is the behavioral acceptance contract for the existing real-binary fuzz
harness. It defines guarantees, scenarios, gate order, safety limits, and
required evidence—not file layout, internal APIs, identifiers, serialization,
or process-supervision mechanics. Follow existing repository conventions for
those implementation choices.

### Agent execution rules

1. Start at the earliest gate without fresh passing evidence:
   **Gate A → Gate B → paid canary**. Gate definitions are in Section 18;
   workflow order and failure transitions are in Sections 4 and 17.
2. Fix the behavior blocking that gate. Focused tests and diagnostic runs may
   explain a failure; they never substitute for the complete gate.
3. Preserve the case counts, coverage, independent oracle, isolation, deadlines,
   acquisition bounds, and cleanup proofs below. A partial campaign is a failed
   or diagnostic run, not reduced acceptance.
4. After source, executable, runtime-image, or fixture/deployment-identity
   changes, obtain fresh ordered evidence. Do not reuse an old pass or combine
   campaign coverage across deployment identities.
5. Paid campaign invocations require `--gate-attestation` referencing fresh,
   successful `ordered_acceptance.py` evidence. Cleanup-only requires no
   attestation and must remain available.
6. Report the gate, artifact path, exercised scope, PASS/FAIL result, and next
   blocker. Use Section 21 for readiness review. Do not carry forward old resume
   commands as authorization to skip gates.

### Evidence checkpoint (historical, not authorization)

| Evidence | Established scope | Limit |
|---|---|---|
| `target/vastai-plan-acceptance-20260910-3/warm-1/gate-a/deployment-e2e/gate-a-evidence.json` | Untraced five-node Gate A: all 12 rounds and complete cleanup | Subsequent source changes require ordered revalidation |
| `target/vastai-plan-campaign-diagnostic-2/vastai-e2e-000000000135282e/` | 26 normal cases passed before shared workload allocation exhausted at `normal-026-blob-4-5`; admission accounts for 208 cases / 224 segments | Not Gate B acceptance |
| `target/vastai-binding-shutdown-smoke.json` | Direct-only child bindings avoid unrelated default public relays; main-to-return teardown fell from 1.001 s to 0.002 s without shorter timeouts | Not complete campaign timing |
| `target/vastai-gate-b-scripted.json` and `target/vastai-gate-b-cleanup-safety.json` | Focused scripted-provider admission and cleanup safety checks passed | Not the complete ordered Gate B |

Ordered source attestation includes Cargo configuration and actor-control-flow
lint tooling alongside runtime and harness source. Historical evidence above
is a debugging reference, not a claim that current artifacts pass.

---

## 1. Objective

One manually invoked workflow MUST:

1. select five cheap, verified VastAI offers on distinct hosts;
2. provision and bootstrap those five nodes once;
3. prove that the cluster converged and is usable within ten minutes;
4. retain that cluster while generated E2E cases use fresh processes, namespace
   state, blobs, streams, descriptors, observations, and model state;
5. replay and shrink a failure without provisioning more infrastructure;
6. restart and recover the orchestrator without re-provisioning or
   re-bootstrapping nodes;
7. finish with five-to-four and four-to-three destructive phases; and
8. destroy and account for every contract created by the run.

Node acquisition and bootstrap are the expensive operations. The fixture is
therefore reused, but no generated case may consume state or observations from
a previous case.

Model-based generation is the center of the suite. Fixed examples are limited
to readiness checks, persisted minimized regressions, and scenarios whose exact
ordering is itself the contract.

---

## 2. Fixed campaign decisions

The fixed campaign shape and limits are:

- initial topology: exactly five worker nodes;
- normal campaign: 128 generated cases on all five nodes;
- recovery campaign: 16 cases that cross an orchestrator restart;
- first destructive phase: remove one node, then run 32 cases on the four exact
  survivors;
- second destructive phase: remove another node, then run 32 cases on the three
  exact survivors;
- paid preparation deadline: ten minutes for provisioning, bootstrap, convergence,
  readiness proof, readiness-proof cleanup, and prepared-fixture commit;
- complete local campaign deadline: five minutes, including preparation,
  all cases, recovery, destructive transitions, cleanup and evidence;
- prepared-fixture workload allocation: at most four minutes, shared by all
  208 logical cases / 224 pre/post workload segments and auxiliary probes;
- diagnosis allowance: at most 15 seconds, subordinate to remaining execution
  and fixture lifetime; exhaustion cannot discard the original failure;
- fixture cleanup reserve: 15 seconds locally; five minutes for paid cleanup,
  separate from the failing workload;
- per-case admission ceilings: 8 MiB payload and 64 MiB allocation;
- campaign admission ceilings: 512 MiB payload and 8 GiB cumulative allocation,
  accounting separately for endpoint rings and public binding copies; and
- invocation model: manual only, with no CI or recurring scheduler in scope.

Offer selection MUST use eligible verified offers, choose distinct hosts, and
prefer the cheapest eligible set. If five suitable distinct hosts are not
available, the run fails before provisioning.

The operator MUST provide explicit fixture lifetime and cost ceilings. The
workflow MUST reject a run before provisioning when its planned campaign cannot
fit those limits.

These are monotonic budget and admission ceilings, not measured speed or
resident-memory claims. The [speed migration](VASTAI_E2E_FUZZ_SPEED_MIGRATION.md)
requires three consecutive complete warm ordered workflows within ten minutes
each, with Gate A before all Gate B checks, plus separately qualified cold
timing. Paid planning reserves up to 20 minutes for preparation, campaign, and
cleanup; offer search and actual provider capacity are reported separately.
Foreground cleanup expiry fails acceptance; the durable paid cleanup duty
continues.

The run may acquire at most the five initially selected contracts and may start
at most one initial logical bootstrap session for each selected node. Provider
or initial-bootstrap failure during preparation fails the fixture; it does not
trigger replacement acquisition. Explicit retained-node binary redeployment is
a separately accounted operation: it may reuse those contracts, but it may
never acquire, replace, or remap infrastructure.

---

## 3. Existing harness changes required

Extend the existing case model, generated public-binding programs, independent
oracle, shrinker, regression corpus, real-binary process control, and fixture
lifecycle. Do not create a parallel VastAI-only harness. Required changes:

- select real-provider execution throughout fixture startup;
- separate fixture preparation from per-case execution and cleanup;
- generate cases over exact, possibly non-contiguous survivor node sets;
- support more processes and richer multi-node topologies;
- collect observations fairly from concurrent executions;
- replay and shrink against an already prepared fixture;
- restart the orchestrator while retaining prepared nodes; and
- use provider-neutral health and teardown checks.

Control requests, replies, provisioning facts, and artifacts MUST use shared,
versioned contracts rather than separate lookalike representations.

---

### 3.1 Blocking retained-node binary-redeployment gate

Retained-node binary redeployment MUST be implemented and proven before any
remaining paid-campaign work. It must provide:

```text
established provider resource with reachable root SSH
+ arbitrary prior Myelin/Swactor execution state
+ a new deployment bundle and deployment identity
→ the same provider resource running exactly the new binaries
+ fresh Iroh/Swactor membership, routing, readiness, and telemetry
```

The node's storage contents, caches, model state, and GPU state are outside this
contract. The redeployment may preserve or destroy them. It must not depend on
their contents. Its substrate preconditions are limited to reachable root SSH
and a functional node capable of receiving, writing, and executing the
deployment. Failure of those substrate capabilities is reported explicitly;
it must not be disguised as an indefinitely bootstrapping worker.

The local gate MUST use a configurable fleet of `N` pre-created Docker nodes
that behave as remote machines. Before paid execution, the gate MUST pass with
the five-node campaign shape. Each node MUST:

- use the production remote-node base environment;
- have an independently masked network, with no shared Docker LAN that can
  accidentally satisfy cluster communication;
- be reachable for deployment only through its published SSH endpoint; and
- retain the same container and network identity through every redeployment
  round.

Docker control may construct adversarial starting states and census fixture
resources. It may not install or launch the tested binaries. Artifact delivery,
reset, launch, and recovery MUST use the same orchestrator/provider path that a
retained VastAI node uses.

The gate uses at least two distinguishable deployment artifacts and identities.
It first deploys generation A, proves cluster behavior, damages the nodes into
different prior states, and then deploys generation B without recreating the
fixture:

```text
create N remote-shaped nodes once
→ deploy generation A through the orchestrator
→ prove membership, routing, telemetry, and an all-pairs behavioral exchange
→ quiesce generated work
→ construct adversarial and mutually different node states
→ deploy generation B to the same N nodes
→ prove generation-B membership, routing, telemetry, and all-pairs behavior
→ repeat with transport and process crashes at every deployment boundary
```

Across deterministic rounds, the adversarial states MUST include:

- no worker and no trustworthy deployment metadata;
- one healthy stale worker;
- a dead worker with stale pid, lock, socket, or completion metadata;
- multiple stale workers or process descendants;
- an interrupted artifact transfer or partial installation;
- a corrupt active binary, activation pointer, or deployment descriptor;
- a deployment transaction interrupted before launch, after launch, and before
  receipt observation;
- loss of the SSH client or orchestrator at each observable transaction
  boundary; and
- a temporarily unreachable node that later becomes reachable with its prior
  state intact.

The same node may be used for multiple states; every required state must be
covered before the gate passes. Fault construction must not teach the
deployment path which state was injected.

For every redeployment round, the harness MUST prove:

1. the exact provider resources, Docker containers, networks, logical node
   identities, and node-to-resource mapping did not change;
2. all stale Myelin/Swactor process incarnations and descendants are gone
   before a new worker is accepted;
3. exactly one worker incarnation per logical node reports the requested
   artifact digest and deployment generation;
4. the active executable bytes match the requested artifact;
5. a successful deployment requires a valid, matching launch receipt rather
   than merely a zero SSH exit status;
6. SSH closure or failure after remote launch is not interpreted as worker
   exit and does not create duplicate workers;
7. repeating an interrupted deployment is idempotent and converges without
   manual node repair;
8. stale runtime-ready, rejoin, membership, route, or telemetry observations
   cannot make an older generation live;
9. each new worker establishes communication and telemetry independently of
   the SSH session; and
10. cluster operations resume only after every required node has passed fresh
    membership, route-ownership, runtime-ready acknowledgement, telemetry, and
    all-pairs behavioral gates.

Deployment convergence is level-triggered. Transient SSH, process, and
transport failures retry with bounded backoff and no elapsed-time success or
failure criterion. Authentication rejection, an invalid artifact, or loss of
the substrate preconditions is a typed terminal failure.

An explicit retained-node redeployment on a paid development fixture follows
the same contract. It MUST begin from a quiescent fixture, retain the exact
contracts and logical topology, prohibit acquisition and replacement, assign a
fresh deployment identity, and rerun readiness and behavioral gates before
cases resume. A redeployment invalidates prior campaign coverage and fixture
baselines: coverage-bearing execution restarts from the beginning on the latest
deployment identity. This permits iterative remote debugging without claiming
that results collected across different binaries form one accepted campaign.

The redeployment gate is complete only when its fault-injection and cleanup
checks pass. The remaining local real-binary E2E suite runs only afterward.

---

## 4. Required execution order

Advancement to paid execution is strictly gated:

```text
implement retained-node binary redeployment
→ pass the local N-node remote-shaped redeployment fault suite
→ pass the complete remaining local real-binary E2E suite
→ pass the remaining mock, scripted-provider, safety, and cleanup checks
→ unlock real-VastAI development and canary execution
```

A failure at any local gate blocks every later gate. Passing an isolated test
or manually demonstrating a remote deployment cannot skip this order.

The paid workflow then runs in this order:

```text
preflight and generate/validate the complete campaign
→ start the cleanup owner and the real orchestrator
→ search for and select five offers
→ authorize the bounded paid operation
→ provision and converge five nodes
→ run the all-pairs readiness gate and clean its state
→ commit the prepared fixture and prohibit further acquisition
→ run compatible persisted regressions
→ run 128 five-node generated cases
→ run 16 orchestrator-recovery cases
→ remove one node and verify the exact survivor set
→ run 32 four-node generated cases
→ remove one node and verify the exact survivor set
→ run 32 three-node generated cases
→ stop the orchestrator and destroy remaining contracts
→ prove every contract created or discovered for this run is absent
```

The complete generated campaign and its resource bounds MUST be validated
before provider access. A failure before the prepared fixture is committed in a
formal acceptance run is cleanup-only: the workflow does not resume initial
bootstrap or restart preparation.

Once preparation succeeds, every path used by normal execution, recovery,
replay, shrinking, and regressions MUST be unable to acquire more
infrastructure. Retained-node binary redeployment is an explicit,
quiescent-fixture transition, never an automatic response to health failure. A
successful redeployment returns the workflow to the readiness gate and resets
campaign coverage; a failed redeployment permits only another explicit
redeployment attempt, evidence collection, or provider cleanup.

---

## 5. Fixture safety and lifecycle

### 5.1 Preparation

Preparation succeeds only when:

- all five selected nodes correspond to distinct expected contracts and hosts;
- all five nodes are running and contextual control is reachable;
- required provisioning and bootstrap milestones are present;
- no unexpected provider acquisition or bootstrap occurred;
- the all-pairs readiness gate passed;
- readiness workloads and resources were cleaned up; and
- the reusable fixture baseline was durably recorded before the deadline.

Repeated connection attempts while a selected node becomes reachable are
allowed. They remain part of that node's single logical bootstrap session.

Any selected-node failure, vanished contract, failed readiness proof, resource
leak, or preparation timeout immediately ends preparation and enters cleanup.
No generated case starts from a partially prepared fixture.

### 5.2 Paid-operation boundary

Every path capable of creating a contract or starting bootstrap MUST share one
run-scoped acquisition bound. The bound MUST:

- authorize only the five selected nodes;
- conservatively count ambiguous provider outcomes;
- survive runner or orchestrator failure;
- prevent retries from becoming replacement acquisitions; and
- become permanently deny-only when preparation succeeds.

The durable run record MUST contain enough expected provider identity to find
and clean contracts even if a process fails between provider acceptance and
normal telemetry.

No crash or retry may exceed the acquisition bound or leave a possibly live
contract unaccounted for. Representation, key derivation, persistence, and
locking remain implementation choices.

### 5.3 Per-case isolation

Infrastructure is reused; case state is not.

Every attempt MUST have a fresh isolated ownership scope covering all writable
namespace paths, processes, streams, descriptors, observations, and model state.
Before execution, the harness proves that owned state is absent and records the
fixture's resource baseline.

Every terminal path—success, expected process failure, launch failure, oracle
failure, or timeout—MUST:

1. stop remaining generated processes;
2. release or remove all case-owned data-plane resources;
3. prove owned state is absent;
4. prove all generated processes are terminal;
5. restore execution, actor, and live resource gauges to baseline; and
6. prove provider contracts and bootstrap counts did not change.

Execution and cleanup use separate deadlines. An execution timeout never skips
cleanup.

Failure to restore the baseline quarantines the fixture. No later case, replay,
or shrink candidate may run against it; only evidence collection and provider
cleanup remain.

---

## 6. Readiness gate

The prepared fixture MUST pass a fresh directed all-pairs data proof across all
five nodes:

- every ordered node pair transfers and validates a blob;
- every ordered node pair transfers and validates a framed stream;
- payloads include small, boundary-sized, and multi-chunk values;
- stream endpoints open concurrently so the proof does not depend on a
  writer-first or reader-first schedule;
- identity, order, length, content digest, and terminal stream state are
  checked; and
- every proof process and namespace resource is removed afterward.

The proof shares the ten-minute preparation deadline. It is a gate for the
entire campaign, not an ordinary generated case.

---

## 7. Generated case IR

A persisted generated case MUST describe behavior rather than concrete harness
implementation. It includes:

- schema and generator versions;
- seed and stable case identity;
- the exact live logical-node set required by the case;
- one to twenty process programs;
- process-to-node assignments;
- public-binding actions;
- acyclic process-completion dependencies;
- blob and stream routes, including branch and join structure;
- expected or admissible outcomes; and
- modeled non-node-loss faults.

Cases record exact node membership and never assume contiguous node numbering.
Replay requires the same live set. Shrinking may remove an unused node from a
case but may not silently substitute a different fixture node.

The IR distinguishes writable case-owned data from explicit read-only fixture
data. Concrete names, paths, request identities, and other execution-local
values are produced per attempt and are not part of the behavioral contract.

Node loss is not a reusable generated-case fault. It is a controlled campaign
phase transition.

---

## 8. Reference model and oracle

The independent model tracks the minimum state needed to judge behavior:

- namespace entry kind, revision, ownership, and legal mutation outcomes;
- blob length and content digest;
- stream incarnation, participants, frame sequence, clean close or abort, and
  replacement isolation;
- process lifecycle, dependencies, expected terminal class, and owned
  resources; and
- authorization boundaries and expected errors.

The oracle MUST derive expected payload and state transitions independently of
the system under test.

Concurrent exclusive publish, rename, unlink, replacement, and active-stream
mutation may have multiple legal outcomes. The oracle MUST retain the bounded
set of legal states and use causal observations to eliminate impossible states.
Polling order or local receipt time MUST NOT choose a race winner.

The case generator MUST reject a case before provisioning if its modeled race
space or planned resource usage exceeds configured bounds. Runtime model
explosion is a harness failure, not permission to choose an arbitrary outcome.

Case success requires both behavioral-oracle success and successful cleanup and
baseline restoration.

---

## 9. Topology and scenario generation

Healthy generated cases contain 16 to 64 actions over 2 to 20 processes. The
campaign uses these topology families:

| Weight | Family | Required shape |
|---:|---|---|
| 20% | Chain | 3 to 5 distinct nodes |
| 15% | Ring or random walk | repeated traversal without reusing a data edge |
| 15% | Fan-out | one source and 2 to 4 independent branches |
| 15% | Fan-in | 2 to 4 producers and one concurrent sink |
| 10% | Diamond | fork, independent transformed branches, and join |
| 25% | Random DAG | 3 to 20 vertices with bounded fan-in and fan-out |

A route may revisit a node, but each directed data edge has its own stream or
blob path and process role. Process-completion dependencies remain acyclic even
when the data route is a ring or walk.

Generated cases compose these behaviors:

- multi-hop blob and framed-stream relay;
- concurrent fan-in and fan-out with complete branch accounting;
- branch joins that depend on every expected input;
- multiple routes sharing nodes but not writable paths;
- descriptor access and namespace mutation;
- active and quiescent stream mutation with modeled outcomes;
- concurrent exclusive operations and legal loser errors;
- authorized and denied access-prefix operations;
- process churn while unrelated routes continue;
- writer abort, reader stop, and stream replacement;
- one expected-failed or slow branch without cancellation of healthy siblings;
- hot high-volume work while cold flows make observable progress; and
- expected process failure isolated from healthy processes and routes.

Payload selection MUST cover empty, small, framing/chunk boundaries, large
multi-chunk values, and randomized sizes within the campaign's byte budget.
Streams MUST test framing independent of transport chunk boundaries, multiple
logical frames, clean EOF, abort propagation, and stale-incarnation rejection.

The precise payload generator, frame binary layout, and buffering strategy are
implementation details. They must be deterministic across generated programs
and the independent oracle, bounded in memory, and capable of detecting loss,
duplication, reordering, corruption, and cross-attempt contamination.

---

## 10. Required ordering scenarios

Most concurrency is judged by partial order, not a total schedule. The following
scenarios require explicit ordering guarantees.

### 10.1 Concurrent stream startup

For rings, fan-in, fan-out, diamonds, and all-pairs readiness, participants open
required endpoints concurrently before waiting for peer completion. The test
MUST prove progress without relying on endpoint creation order.

### 10.2 Ring completion

Ring participants start concurrently. Tokens traverse the required laps, each
edge validates and forwards them, and completion propagates only after all
injected tokens are accounted for. The origin MUST continue receiving while
injection or forwarding can block, and the ring MUST terminate with clean EOF
rather than deadlock.

### 10.3 Hot/cold fairness

The generated scenario establishes this causal order:

```text
hot flow is active and parked
→ its first frame is observed
→ each cold flow starts useful work
→ every cold flow terminates
→ the hot flow receives release
→ the hot flow terminates
```

The hot stream remains active until release. The guarantee is observable
progress of cold work under concurrent load, not a latency threshold or a
particular scheduler behavior.

### 10.4 Failure isolation

An expected failure in one branch or process MUST NOT implicitly cancel healthy
siblings. The model and observations must account independently for every
branch's outcome and cleanup.

---

## 11. Coverage requirements

The five-node normal campaign MUST close both a planned coverage ledger and an
observed completed ledger. It requires:

- every topology family at least eight times;
- every logical node as source, sink, and interior relay for blobs and streams;
- every ordered node pair as a blob edge and stream edge;
- every existing primitive action-category adjacency;
- all required payload boundary classes;
- every descriptor read/write mode and terminal mode;
- successful operations, modeled errors, and contextual-process failures;
- concurrent starts for chain, ring/walk, fan-in/out, and diamond families; and
- the actor-style progress, churn, mutation, and failure-isolation scenarios in
  this plan.

Generation fills required coverage slots before applying random weights. A plan
that cannot meet coverage or resource bounds fails before provider access.
Observed incomplete coverage fails the campaign rather than being reported as a
reduced successful run.

Each survivor campaign MUST cover every remaining node as source, sink, and
relay, and every ordered survivor pair as both a blob and stream edge.

---

## 12. Observation requirements

Generated workloads use only the public application binding. Observations MUST
carry enough typed information for the independent oracle to establish:

- attempt and process-local identity;
- process lifecycle and local action order;
- route, token, hop, and stream-incarnation relationships;
- source and destination ownership where relevant;
- input/output lengths and digests;
- progress barriers and fault triggers; and
- typed outcomes and errors.

There is no assumed global event order across processes. The oracle constructs a
partial order from process-local order, dependencies, data-route edges, explicit
barriers, fault triggers, and namespace revisions.

Observation transport MUST be bounded, untorn, loss-detecting, and drained
fairly across live executions. The implementation may choose the encoding and
collection mechanics, but it MUST prove before a paid run that concurrent
records can be recovered completely without truncating required evidence.

Timeout artifacts include enough model, process, fleet, and recent telemetry
state to explain what remained pending.

---

## 13. Recovery campaign

Each of the 16 recovery cases has a pre-restart segment and a post-restart
segment within one case attempt.

Before restart:

- all pre-restart contextual processes are terminal;
- no stream is active;
- live resource gauges are at baseline; and
- the model explicitly identifies the blobs and quiescent namespace entries
  intended to persist.

The orchestrator is then stopped and replaced using the same prepared fixture
and persistent state. Recovery MUST:

- perform no offer search, contract creation, node bootstrap, or topology repair;
- adopt the exact existing contracts and logical nodes;
- restore all five nodes to running and reachable;
- begin with no live contextual executions;
- preserve persisted blob kind, revision, length, and digest;
- preserve quiescent stream namespace identity and inactive state, without
  treating consumed bytes as persistent stream contents; and
- allow fresh post-restart processes to complete the remaining segment.

Active contextual processes and active streams are outside the recovery
contract. Normal per-attempt cleanup occurs after the post-restart segment.

---

## 14. Destructive tail

After the normal and recovery campaigns, the workflow removes nodes through
normal orchestrator control.

For each removal, command acceptance alone is insufficient. The harness MUST
prove that:

- the selected target reached the stopped state;
- every non-target survivor remained running and reachable;
- the provider contract set lost exactly the target contract;
- the running logical-node set lost exactly the target node; and
- no replacement intent, node, contract, acquisition, or bootstrap appeared.

The four-node campaign runs on the exact first survivor set. The three-node
campaign runs on the exact second survivor set. Neither phase claims fresh
four-node or three-node bootstrap coverage.

A failing workload on a degraded live set may be replayed and shrunk before the
next node is removed. The node-removal transition itself is not part of a
shrinkable case.

---

## 15. Replay, shrinking, and regressions

The first unexpected behavioral failure stops ordinary campaign execution.
After the failed attempt has cleaned up and restored the fixture baseline, the
runner replays and shrinks it within the same fixture, lifetime, cost, and
resource bounds.

Replay MUST:

- require the recorded exact live-node set;
- use fresh per-attempt state;
- perform no infrastructure acquisition, repair, remapping, or node mutation;
- reproduce the same typed behavioral failure; and
- pass the same cleanup and health checks as an ordinary case.

Shrinking may reduce faults, processes, dependencies, actions, topology edges,
laps, tokens, payloads, and unused node participation. It MUST preserve the
failure's behavioral signature rather than incidental identities, timestamps,
or artifact positions.

Cleanup failure stops shrinking. The unshrunk failure remains available when the
fixture cannot safely run candidates.

Minimized compatible regressions run before random cases. Incompatible artifacts
are rejected clearly rather than interpreted under a different schema or
silently remapped.

---

## 16. Health and teardown

Provider-neutral health checks MUST verify:

- the orchestrator is live when expected;
- the exact expected logical nodes are in the expected phases;
- contextual control is reachable on live nodes;
- no unexpected execution, panic, poison, or transient actor remains;
- live resource gauges return to baseline; and
- contract and bootstrap accounting remains unchanged during case execution.

Health behavior MUST reflect the selected provider. Real-provider health and
cleanup cannot rely on local-container census or local mock-resource behavior.
The concrete provider abstraction is an implementation decision.

Teardown MUST continue despite individual errors and must:

1. stop all still-managed nodes;
2. stop and reap the orchestrator and its provider-facing work;
3. discover every contract attributable to the run, including contracts missing
   from ordinary telemetry after an ambiguous create;
4. destroy every discovered contract;
5. distinguish confirmed absence from provider query failure;
6. prove every exact contract absent before declaring cleanup successful; and
7. persist final accounting and all cleanup errors.

A successful destroy request is not proof of absence. Continued typed provider
absence is the spending-stop condition.

Cleanup discovery MUST be constrained to this run's expected labels and known
contract identities. It must never destroy or persist unrelated account
resources. Duplicate or ambiguous matches fail accounting but are still cleaned
to stop spend.

Credentials MUST never appear in arguments, URLs, manifests, telemetry,
artifacts, or durable diagnostics. Outside an explicitly retained development
fixture, cleanup-only recovery after runner, supervisor, or workstation failure
MUST be able to finish exact discovery, destruction, and absence proof, but
cannot search offers, acquire nodes, bootstrap, or run cases.

An explicitly retained development fixture may survive those failures only
while its cleanup owner, fixture-lifetime ceiling, and cost ceiling remain
active. Recovery may perform an explicit in-place binary redeployment or
provider cleanup; it may not acquire infrastructure or resume cases until the
full readiness and behavioral gates establish a new baseline.

---

## 17. Failure handling summary

| Failure point | Required result |
|---|---|
| Preflight or campaign validation | Zero provider acquisition; no cleanup needed |
| Cleanup-owner or orchestrator startup | Zero provider acquisition; stop started processes |
| Offer selection or admission | Zero provider acquisition |
| Provisioning, bootstrap, or readiness | Stop preparation; clean every possible contract |
| Case execution or oracle | Clean attempt; replay/shrink only from restored baseline |
| Local retained-node redeployment gate | Stop the local phase, clean its Docker fixture, and keep paid execution blocked |
| Attempt cleanup or health restoration | Quarantine fixture; skip further cases; clean provider |
| Formal orchestrator-recovery phase failure | Enter cleanup-only; never reacquire or repair |
| Explicit paid-fixture binary redeployment | Retain the exact contracts, pause cases, retry only in place, and reacquire nothing |
| Destructive transition | Do not continue to the next survivor campaign unless exact state is proven |
| Normal teardown | Continue best-effort cleanup and fail the run on any unresolved contract |
| Runner/supervisor/workstation loss outside an explicitly retained development fixture | Resume cleanup-only from durable run accounting |

---

## 18. Pre-paid verification

Pre-paid verification has two ordered gates. Gate B MUST NOT begin until Gate A
passes, and paid execution MUST NOT begin until both pass.

### 18.1 Gate A: retained-node redeployment

The local remote-shaped fleet and fault suite in Section 3.1 MUST pass first.
Its persisted evidence MUST identify every injected starting state and
interruption boundary and prove, for every redeployment round:

- unchanged fixture resources and logical topology;
- matching installed and runtime deployment identity;
- absence of stale and duplicate workers;
- independence of worker lifetime from SSH lifetime;
- restored Iroh/Swactor membership, routing, readiness, and telemetry;
- successful post-redeployment all-pairs behavior; and
- complete local fixture cleanup.

### 18.2 Gate B: remaining local and scripted verification

Only after Gate A passes, local real-binary, mock-provider, and
scripted-provider checks MUST prove these scenarios:

1. the complete five-node local real-binary campaign shape passes after the
   redeployment suite, not instead of it;
2. the generator, independent model, oracle, replay, and shrinker agree on
   fixed vectors and representative generated cases;
3. every control and telemetry contract round-trips through the shared
   versioned representation;
4. offer filtering selects five verified distinct hosts by the required price
   policy and rejects insufficient or malformed results before acquisition;
5. cost, lifetime, image, credential, and campaign-resource admission failures
   produce zero provider acquisition;
6. every provider-capable path shares the same five-node acquisition bound, and
   retries, replacement attempts, recovery, replay, shrinking, destructive
   phases, and retained-node redeployment cannot exceed it;
7. connection retries do not become extra initial logical bootstrap sessions
   or overlapping binary-redeployment transactions;
8. failures and injected crashes at each preparation boundary preserve enough
   accounting to clean every possible contract;
9. outside an explicitly retained development fixture, runner, orchestrator,
   cleanup-owner, and workstation failure paths enter cleanup-only behavior and
   do not resume acquisition or initial bootstrap;
10. recovery adopts the prepared fixture with zero acquisition and preserves
    the required namespace state while excluding active execution recovery;
11. all-pairs streams, rings, fan-in/out, joins, hot/cold fairness, and expected
    branch failures terminate with complete observations;
12. concurrent observation collection recovers all required records without
    using polling order as causal order;
13. every case failure path cleans before replay or shrinking;
14. real-provider health uses no local mock/container assumptions;
15. concurrent node and contract cleanup proves typed absence within the
    cleanup deadline while preserving unrelated account resources; and
16. injected credential values are absent from all diagnostics and artifacts.

A paid canary is allowed only after both gates pass in order.

---

## 19. Paid canary acceptance

The first paid run is accepted only when:

- five verified distinct hosts are provisioned once;
- all five nodes converge, pass typed readiness, complete the all-pairs proof,
  clean proof state, and commit the prepared fixture within ten minutes;
- acquisition accounting shows exactly five contract creations and five
  initial logical bootstrap sessions, with no later acquisition;
- every retained-node binary redeployment, if exercised during development, is
  explicit, preserves those exact contracts, has complete per-node transaction
  accounting, and is followed by fresh readiness and all-pairs proof;
- the accepted coverage ledger contains results from one final deployment
  identity only;
- selected worst-case cost remains within the operator ceiling;
- planned and observed coverage ledgers close;
- every attempt proves fresh preconditions and clean postconditions;
- recovery performs no acquisition or implicit binary redeployment and
  preserves the specified state;
- destructive phases run against exact survivor sets and create no replacement;
- replay and shrinking, if exercised, remain inside the prepared fixture;
- cleanup proves every contract attributable to the run absent; and
- paid evidence is persisted as typed, versioned artifacts.

A cleanup error fails the run even if all behavioral cases passed.

---

## 20. Non-goals

This work does not:

- create a scheduler or recurring test service;
- prescribe source files, module boundaries, internal APIs, or implementation
  steps beyond the required gate order;
- standardize identifier generation, concrete containers, frame layout, or
  persistence internals beyond the observable guarantees in this plan;
- test unverified hosts;
- automatically replace failed nodes;
- promise survival of active contextual processes or active streams across an
  orchestrator restart;
- treat degraded campaigns as fresh bootstrap coverage;
- bypass public application bindings in generated workloads;
- allow a later case to consume earlier case state;
- require storage, cache, model, dataset, or GPU state to survive a retained-node
  binary redeployment; or
- recover a node whose root SSH or basic write-and-execute substrate is broken.

---

## 21. Design-review checklist

A readiness review MUST return PASS or FAIL, with behavioral evidence, for each
item:

1. **Retained-node redeployment gate:** the local remote-shaped fleet converges
   from every required corrupt state and interruption boundary without changing
   fixture resources, duplicating workers, or depending on SSH after launch.
2. **Ordered advancement:** the redeployment gate passes before the remaining
   local E2E suite, and every local and scripted gate passes before paid access.
3. **Bounded paid work:** no execution or failure path can acquire more than the
   five selected nodes or leave a possible contract outside cleanup accounting.
4. **Prepared-fixture isolation:** every case begins fresh, ends at the recorded
   baseline, and quarantines the fixture on restoration failure.
5. **Topology executability:** all required topology families, stream startup,
   ring completion, joins, faults, and progress scenarios can terminate without
   relying on a favorable schedule.
6. **Independent oracle:** expected data and legal concurrent outcomes are
   derived independently and never selected by observation polling order.
7. **Zero-acquisition reuse:** recovery, replay, shrinking, regressions,
   survivor campaigns, and explicit binary redeployment cannot provision,
   repair, replace, or remap topology.
8. **Exact cleanup:** normal and crash-recovery paths discover every attributable
   contract, preserve unrelated resources, and prove typed absence.
9. **Integration evidence:** the complete local redeployment suite, local
   real-binary campaign, mock campaign, scripted failure and crash scenarios,
   credential-leak checks, and cleanup gates pass in the required order before
   paid execution.

---

## 22. Execution discipline

This plan is a verification checklist, not an implementation backlog.

- Do not edit without a concrete, focused reproduction of a violated
  requirement.
- Do not invent goals from unchecked requirements, speculative reviews, or
  agent suggestions.
- Keep exactly one active blocker. Defer everything unrelated.
- The full campaign is final qualification, never the debugging loop.
- After a campaign failure, extract and run the exact failing case directly. Do
  not rerun the campaign until that focused case fails before the fix and passes
  after it.
- Freeze source, binary, and image identities before running one complete
  ordered qualification.

### Current state — 2026-09-14

- `target/ordered-final-3/ordered-acceptance.json` records successful five-node,
  12-round Gate A (331.13 s) and complete local campaign (274.08 s).
- The complete warm workflow failed at 605.23 s against its 600 s ceiling,
  before the remaining Gate B checks. This is the active demonstrated blocker.
- A separate failure-case run passed in 14.39 s; it is not ordered acceptance.
- Review-time current-source checks passed: 169 harness library tests,
  5 shared-contract tests, 4 contextual-process tests, 4 telemetry transport
  tests, and 4 attestation-guard tests.
- The old active-stream replacement failure is historical; do not carry it
  forward as a current blocker without a new reproduction.
- Release executable hashes matched the retained ordered run at review time,
  but the source digest did not. No current identity has successful complete
  ordered qualification.
- No remote or paid execution was performed during the checkpoint review.
- Continue work using the remote and local handoffs linked above. Local
  artifact paths are not part of the committed evidence and must be retained
  separately.