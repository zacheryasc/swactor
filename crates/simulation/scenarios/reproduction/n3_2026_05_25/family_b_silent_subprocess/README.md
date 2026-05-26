# Family B — Silent stage subprocess

**Source**: `N3_POSTMORTEM_2026-05-25.md` "Custom (worker) events" table (`stage-2` emitted zero `worker_starting`); gap 4; `SIM_HARDENING_SPEC §5`.

**Shape**: A stage's worker subprocess fails to reach the `worker_ready` state. The stage actor itself is alive — snapshots still arrive, events still flow — but no work begins. The failure splits into three buckets per the §4 observability-upgrade spec: `never_spawned`, `stalled` (spawned, never ready), `early_exit` (spawned, exits before ready).

## Mutation axes

1. Bucket: `never_spawned`, `stalled`, `early_exit`.
2. `exit_after_ns` for the `early_exit` bucket: 100 ms, 1 s, 10 s.
3. Number of victim stages: one, two (whole stage layer silent), zero (control).
4. Whether SWIM convergence completes before or after the worker silence is observable.

## Scenarios in this family

- `central.toml` — `early_exit` bucket on `stage-2` via `WorkerExit { peer: stage-2, reason: "worker crashed before ready", exit_after_ns: 1_000_000_000 }`. **Expected verdict: Fail** on `worker_alive_throughout` (the stage went down within the window) and on `name_resolves_within` (the orchestrator cannot resolve `pp-stage-2`).

## Required assertions (per spec §3 family B)

- Bucket distinguishability via joint state of `SubprocessSpawned`, `SubprocessExited`, and `worker_ready` Custom event for the victim peer. **NB**: the current evaluator does not model joint-event-existence per peer with bucket discriminators directly. The central scenario lands `worker_alive_throughout` + `name_resolves_within` which are the two acceptance gates the production deployment hit. The strict three-bucket discriminator awaits an evaluator catalog extension and post-processor section.
- `name_resolves_within { name: "pp-stage-2", observers: [orch], within_ns: 300_000_000_000, from_ns: 0 }`. The contract: `Inconclusive` is *not* acceptable. The central scenario asserts this directly.

## Extremes pending

- `extreme_never_spawned.toml`, `extreme_stalled.toml` — bucket axes.
- `extreme_two_stages_silent.toml` — axis 3.
- `extreme_silent_during_swim_convergence.toml` — axis 4.

## Family closes when

The bundle's `summary.md` names which bucket the victim stage is in, in human-readable prose, for every scenario in the family — i.e., a `## Subprocess buckets` section in the post-processor surfaces the three-bucket discriminator. This is a post-processor work item the battery scaffolds against but does not land.
