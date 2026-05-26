# Family D — Asymmetric host reachability (NAT / mapping pathology)

**Source**: `N3_POSTMORTEM_2026-05-25.md` "UDP echo probes" (stage-2 1/12 timeout while others were clean); gaps 8 and 11; `SIM_HARDENING_SPEC §2`.

**Shape**: One peer's host network behaves correctly *most* of the time, but exhibits asymmetric loss, NAT-rebind, or kernel-UDP-buffer overflow in a pattern that downstream iroh layers cannot distinguish from a relay-side or peer-software issue.

## Mutation axes

1. Symmetry: loss on outbound from victim, on inbound, on both, none (control).
2. Burst shape: continuous low-rate vs short high-rate.
3. Co-occurrence: loss alone vs loss + clock skew on the same peer.

## Scenarios in this family

- `central.toml` — `LossBurst` on `(stage-2 → R)` with `prob_ppm = 80_000` (8% loss) lasting 30 s during steady state. **Expected verdict: Mixed**. The exact verdict depends on whether the simulator's stage host emits `Tier3InterfaceCounters` under the loss-burst mutation (per spec §3 family D close criterion); if it does not, that is a sim-coverage gap filed in `SIM_BLIND_SPOTS.md` rather than relaxed in the assertion.

## Required assertions (per spec §3 family D)

- The bundle's UDP echo probe records must show the victim's outcome distribution differing from the others' by a margin a human reader can see. **NB**: no typed assertion expresses this directly; the post-processor's `## Probe outcomes` section is the surface, and the family relies on visual inspection of the bundle.
- The victim's `Tier3InterfaceCounters.rx_packets_dropped` or `Tier3UdpKernelStats.in_errors` is non-zero in the bundle while the other peers' is zero — the "kernel saw the loss, not just iroh" contract from gap 11. The post-processor's existing `## Kernel network drops` section surfaces this; the assertion catalog does not currently express the discriminator.

The central scenario lands two `self_incarnation_bounded` assertions:

- `peer = "orch", max_value = 3` — coarse upper bound; the orchestrator's outbound is unaffected by the burst, so its incarnation should stay flat.
- `peer = "stage-2", max_value = 0` — refute-on-Suspect discriminator. Under the loss burst, the cluster will Suspect stage-2 and stage-2 will refute with a self-incarnation bump. The bound resolves Fail under the burst and would Pass without it; substitutes for the spec's literal kernel-counter discriminator (which awaits the catalog extension) by exercising the same victim/non-victim asymmetry through the SWIM refute path.

A stricter contract awaits an assertion-catalog extension for per-peer kernel-counter discriminators.

## Extremes pending

- `extreme_inbound_only.toml`, `extreme_both_directions.toml` — axis 1.
- `extreme_short_high_burst.toml` — axis 2.
- `extreme_loss_plus_skew.toml` — axis 3.
- `property.toml` — seeds 0..128 over axes 1 and 2 (per spec §3).

## Family closes when

The property test runs to 128 seeds with the loss-discriminator holding on every seed it sees loss; the sim-coverage gap, if it exists, is filed.
