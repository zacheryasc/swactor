# Family F — Compound faults under recovery

**Source**: `SIM_HARDENING_SPEC §7` and §9.

**Shape**: Two or more faults active during a single recovery window — a partition heal during a relay-peer-down, a clock skew during a worker respawn, a kernel UDP overflow during SWIM gossip burst. The 2026-05-25 incident is consistent with at least two overlapping faults; the battery covers the next overlap before it lands in prod.

## Mutation axes

1. Which two faults overlap (cross product of single-fault families, restricted to combinations producing distinguishable bundles).
2. Overlap geometry: full overlap, partial overlap, abutting.
3. Recovery phase: which recovery phase the second fault hits.

## Scenarios in this family

- `central.toml` — `Partition` cutting `stage-2` from `stage-0` from +10 s to +30 s, plus a `RelayPeerConnDown { from: orch, to: stage-2, at_ns: +20 s, duration_ns: +20 s }` overlapping the partition's last 10 s and extending 10 s past its heal. **Expected verdict: Mixed**. Compound failures are the under-tested corner; the implementing agent expects to find at least one new sim-coverage gap during this family's implementation and file it.

## Required assertions (per spec §3 family F)

Family-dependent — each compound test combines the assertions of its constituent families. The compound test passes only if every constituent assertion holds. The central scenario lands `no_flap_while_probes_ok` on stage-2 over the overlap window, mirroring family A's assertion since the overlap exercises both A's and C's shapes.

## Extremes pending

- `extreme_loss_burst_plus_partition.toml` — families D + C.
- `extreme_worker_exit_during_heal.toml` — families B + C.
- `property.toml` — seeds 0..512 over all three axes (per spec §3).

## Family closes when

At least one compound bug is either fixed or filed as a sim-coverage gap with a structural reason.
