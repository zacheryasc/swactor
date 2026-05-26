# Family C — Gossip-arrival absence (control-plane vs data-plane discriminator)

**Source**: `N3_POSTMORTEM_2026-05-25.md` "iroh state — stage-2's view of itself" (`peers: [orchestrator only]`); gap 10; `SIM_HARDENING_SPEC` §1 and §2.

**Shape**: A victim peer's local membership view contains only the orchestrator, never its siblings. Two possible causes are indistinguishable from the postmortem bundle: gossip about siblings never arrived (control-plane), or gossip arrived but the dials based on it never connected (data-plane). The battery must let a single scenario+verdict pair disambiguate these.

## Mutation axes

1. Topology: full isolation (central); one-way isolation; periodic gossip drops modulated by `LossBurst`.
2. Whether the orchestrator's gossip-piggyback ever names the siblings.

## Scenarios in this family

- `central.toml` — `Partition` mutation isolating `stage-2` from `stage-0` at the network-graph layer, with each stage's path to `orch` left intact. **Expected verdict: Pass** (regression guard) — the bundle distinguishes the two causes by the presence/absence of `GossipReceived` events on stage-2 plus the presence/absence of `DialStarted` events.

## Required assertions (per spec §3 family C)

- `event_count { kind: "GossipReceived", peer: stage-2, payload_kind: "NameRegistry", min: N }` where N depends on the axis. **Partially landed**: the `EventCount` catalog now supports `min:` and `peer:` filters (iter 5 + iter 6). The central scenario uses the new peer filter to assert at least one `state_transition` lands on stage-2's stream. The literal `kind: "GossipReceived"` + `payload_kind: "NameRegistry"` form awaits a sim S-layer extension — the current SWIM host adapter rides gossip as piggyback bytes inside Ping/Ack messages rather than emitting typed `GossipReceived` events. `state_transition` is the closest filter target the partition reliably triggers.

## Extremes pending

- `extreme_one_way_isolation.toml` — stage-2 receives gossip but dials are silently dropped (data-plane failure).
- `extreme_periodic_loss.toml` — `LossBurst` modulating gossip arrival.

## Family closes when

The bundle's `summary.md` names the discriminator in prose (e.g., "stage-2 received N gossip messages naming `pp-stage-0`; dials started=K, succeeded=K — control-plane healthy"). The discriminator surface is already in the post-processor's `## Gossip receipts` and `## Per-peer dials` sections (per the prior observability upgrade); the battery's job is to guard against regression.
