# Family E — Bundle integrity under operator SIGKILL

**Source**: `N3_POSTMORTEM_2026-05-25.md` "Bundle recovery"; gap 7; observability upgrade `S-D` (bundle without finalize).

**Shape**: The orchestrator is killed ungracefully. No finalize record is written. The diagnostic bundle must still be assemblable from staging files on disk, with `manifest.finalize_received: false`.

## Mutation axes

1. Timing of kill: during convergence, during steady state, during a partition heal.
2. Which peer: orchestrator, a stage, the relay.

## Scenarios in this family

- `central.toml` — `PeerKill { peer: orch, at_ns: 5_000_000_000 }`, run extends 5 s past the kill. **Expected verdict: Pass** (regression guard) — the observability upgrade landed `S-D`, and the family guards that contract.

## Required assertions (per spec §3 family E)

- The bundle's `manifest.json` must exist and contain `finalize_received: false`. **NB**: this is a bundle-shape contract, not a verdict-shape assertion. The central scenario lands a coarse `self_incarnation_bounded` assertion that should resolve Pass or Inconclusive (no flap), and the bundle-shape contract is verified by the test driver (the test inspects `manifest.json` directly).
- Every peer's pre-kill events and snapshots present in the bundle — verified by the test driver inspecting the bundle's per-peer event counts.
- Every assertion's verdict in `verdicts.json` — `Inconclusive` for any whose preconditions did not fire (e.g., steady-state assertion when steady state was never reached).

## Extremes pending

- `extreme_kill_during_convergence.toml`, `extreme_kill_during_steady_state.toml` — axis 1.
- `extreme_kill_stage.toml`, `extreme_kill_relay.toml` — axis 2.

## Family closes when

Every scenario produces a parseable bundle. The central scenario's test driver verifies the bundle-shape contracts named above.
