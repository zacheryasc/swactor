# Family A — Relay-mediated peer-connection drop with surviving tunnel

**Source**: `N3_POSTMORTEM_2026-05-25.md` "iroh state — orchestrator's view of stage-2"; gaps 1, 2, 3.

**Shape**: A peer-to-peer path through a relay opens, succeeds, then dies. The relay's tunnel to the victim peer remains apparently healthy; the orchestrator's `connection_cache[victim].last_failure_reason` shows the path closed. iroh does not re-establish.

## Mutation axes

1. `at_ns`: when the cut fires. Central +5 s; extremes +1 s, +30 s, +1 min, +5 min.
2. `duration_ns`: how long the cut persists. Central permanent; extremes 100 ms, 5 s, 30 s.
3. Direction: cut on `(orch → stage-2)` only, on `(stage-2 → orch)` only, or both.
4. Flap: a sequence of `RelayPeerConnDown` mutations interleaved with natural recovery.
5. Phase: cut during SWIM convergence; cut during steady-state; cut during partition heal.

## Scenarios in this family

- `central.toml` — central case: `RelayPeerConnDown { from: orch, to: stage-2, at_ns: 5_000_000_000, duration_ns: 0 }`. **Expected verdict: Fail** on `no_flap_while_probes_ok` (the deployment's actual failure mode against the current SWIM source).

## Required assertions (per spec §3 family A)

- `no_flap_while_probes_ok { peer: stage-2, window_start_ns: at_ns, window_end_ns: duration_ns_end }`. The family's load-bearing observability assertion.
- `event_count { event_kind: "swim_probe_timed_out", min: 1 }`. The spec's literal contract names `RelaySessionStateChanged` as the event kind, but the simulator does not have a relay-side observability adapter that emits that event when `RelayPeerConnDown` fires. Substituting `swim_probe_timed_out` — which fires when the cut peer's probes expire — preserves the "the cut produces an observable signal" close criterion. The `EventCount { min: ... }` catalog extension lands alongside this scenario; the literal `RelaySessionStateChanged` form remains pending a sim-side relay observability adapter (sibling family A README extreme).
- `dead_peer_resurrects_within { peer: stage-2, after_ns: heal_at_ns, within_ns: 30_000_000_000 }` on the finite-duration extreme cases (not yet landed).

## Extremes pending

- `extreme_flap.toml` — sequence of close/reopen pairs at +5 s.
- `extreme_phase_during_heal.toml` — cut during a `Partition`+`Heal` cycle's heal phase.
- `property.toml` — seed range 0..256 over axes 1, 2, and 5 (per spec §3).

## Family closes when

A fix lands that lets the central case pass `no_flap_while_probes_ok` and at least the flap and phase-during-heal extremes pass with no other family regressing.
