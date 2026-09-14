# VastAI checkpoint: remote-ready work

Reviewed 2026-09-14. This is the remote-work handoff, not a paid-run authorization.
See [local blockers](VASTAI_LOCAL_BLOCKERS.md) for the parallel local track and
[the E2E plan](VASTAI_E2E_FUZZ_PLAN.md) for the acceptance contract.

## Decision

The deployment, runtime, and campaign machinery is implemented far enough to
move a frozen checkpoint onto an existing remote development host and test it
there. Do not wait for all local qualification work before collecting useful
remote evidence.

There are two different milestones:

- **Ready now: remote-host diagnostics.** Run the existing Docker/SSH fixture and
  real-binary workloads on infrastructure already available for development.
- **Not yet authorized: paid VastAI execution.** This still requires successful,
  fresh ordered qualification and the existing paid admission/cleanup guards.
  Moving a run to another machine does not bypass those requirements.

## What is ready to exercise

| Area | Remote work | Existing evidence |
|---|---|---|
| Deployment and retained-node redeployment | Install new bundles, replace prior worker state, interrupt SSH/orchestrator boundaries, and verify fresh membership without replacing nodes. | Five-node Gate A passed all 12 rounds, all-pairs behavior, and fixture cleanup. |
| Contextual processes and data paths | Exercise public Python bindings, process launch/stop, namespace/blob/stream behavior, and resource reclamation. | Complete local campaign plus current focused process-lifecycle tests. |
| Prepared-fixture reuse | Keep the fixture while running fresh cases, orchestrator recovery, and exact four-node/three-node survivor phases. | 128 normal, 16 recovery, 32 four-node, and 32 three-node cases completed: 208 logical cases / 224 workload segments. |
| Telemetry transport | Observe startup records, reconnect/replay, cancellation, and consistency between worker identity and collected observations. | Current transport tests passed catalog/frame delivery, compressed-frame validation, cancellation, and replay without duplicates. |
| Failure handling | Exercise the existing failure-case workload and inspect terminal observations and cleanup. | A separate local failure-case run records success; it is not complete ordered Gate B evidence. |

These are working test candidates, not claims of stability on real VastAI hosts.
Provider offer selection, real network conditions, and provider-side cleanup
still need live-provider evidence after admission is authorized.

## Remote track: execution outline

### 1. Freeze and prepare

- Use the committed checkpoint in a separate checkout from ongoing local edits.
- Build and record exact source, executable, deployment-bundle, and image
  identities. Do not assume the development machine's cached binaries belong to
  the new checkout.
- Use an existing development host with the required Docker, SSH, image, and
  networking capabilities. The static-SSH provider manages pre-created Docker
  nodes; it is not a generic adapter for arbitrary SSH machines.
- Keep the existing fixture ownership and cleanup machinery. Do not introduce a
  second deployment harness or manually install binaries inside tested nodes.

### 2. Exercise the established paths

Start with the five-node retained-node redeployment gate. After it passes,
exercise the complete campaign and focused failure cases. Preserve exact node
sets, public-binding workloads, and cleanup checks.

Measure preparation, redeployment rounds, campaign phases, and cleanup
separately. Useful findings include remote-only connectivity failures, stale
membership after redeployment, missing telemetry, process-lifetime problems,
and stages that dominate elapsed time.

An isolated diagnostic run is useful even when it is not ordered acceptance.
Label it as diagnostic; do not merge its coverage into a later accepted run.
Do not use another full campaign as the debugging loop after a failure: retain
and reproduce the exact failing case or deployment round.

### 3. Hand off failures without moving the baseline

For each result, record:

- checkpoint/source, executable, image, and deployment identities;
- host environment, gate or case, exact node set, and artifact location;
- PASS/FAIL, stage timings, and cleanup outcome;
- for failure, the first failing observation and smallest known reproduction.

Continue local fixes in the separate local checkout. Promote them to the remote
track only as a new checkpoint, with fresh identities and evidence. Do not
silently update binaries in the middle of an accepted campaign.

### 4. Advance to real VastAI only after qualification

The paid path requires fresh successful `ordered_acceptance.py` evidence,
matching artifacts and immutable image provenance, eligible distinct-host
offers, explicit cost/lifetime limits, credentials supplied through the existing
private configuration, and active durable cleanup ownership.

The current checkpoint has no such successful attestation. Leave the gate
checks intact. Do not substitute the static-SSH fixture, a scripted-provider
pass, or the historical local campaign for paid authorization.

## Evidence and limits

Development-machine artifacts inspected for this checkpoint:

- `target/ordered-final-3/ordered-acceptance.json`: Gate A passed in 331.13 s;
  campaign passed in 274.08 s; the complete workflow failed at 605.23 s before
  later Gate B checks.
- `target/ordered-final-3/warm-1/gate-a/deployment-e2e/gate-a-evidence.json`:
  five nodes, 12 rounds, successful behavior and complete cleanup.
- `target/ordered-final-3/warm-1/campaign/vastai-e2e-000000000135282e/`:
  coverage ledgers and checkpoint account for all campaign phases.
- `target/gate-b-failure-cases/timing-runs/1789241279248-532.json`:
  separate failure-case run passed in 14.39 s.

At review time all three release executable hashes matched the ordered-run
record, but the current source digest did not. These are historical execution
results, not current-tree qualification. The artifact paths are local build
outputs and are not included in Git; retain or transfer the evidence explicitly
when handing off a run.

Current-source verification during review: 169 harness library tests, 5 shared
contract tests, 4 contextual-process tests, 4 telemetry transport tests, and 4
attestation-guard tests passed. No new remote or paid run was performed.
