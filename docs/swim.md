# SWIM Membership Protocol

SWIM (Scalable Weakly-consistent Infection-style Membership) handles failure
detection and membership dissemination. Each node periodically probes a random
peer; if the peer doesn't respond, indirect probes through relays confirm or
deny reachability before the node is suspected and eventually declared dead.

## Probe Cycle

See [swim_probe_cycle.svg](swim_probe_cycle.svg) for the full state machine.

The probe cycle is a pure state machine driven by ticks:

```
    ┌──────────────────────────────────────────────────────────────────────┐
    │                        ProbePhase                                    │
    │                                                                      │
    │   Idle ──probe_interval──► WaitingDirectAck ──timeout──►             │
    │    ▲                            │                                    │
    │    │                        ack received                             │
    │    │                            │                                    │
    │    └────────────────────────────┘                                    │
    │                                                                      │
    │   WaitingDirectAck ──timeout──► WaitingIndirectAck ──timeout──►      │
    │                                        │                             │
    │                                    indirect ack                      │
    │                                        │                             │
    │                                   back to Idle                       │
    │                                                                      │
    │   WaitingIndirectAck ──timeout──► Suspect(target) + start timer      │
    │                                                                      │
    │   Suspicion timer ──timeout──► DeclareDead(target)                   │
    └──────────────────────────────────────────────────────────────────────┘
```

Probe targets are selected via round-robin over a shuffled member list. Each
probe cycle picks one target and sends a direct `Ping`. If no `Ack` arrives
within `probe_timeout` ticks, indirect `PingReq` messages are sent through
`k` relay nodes. If no indirect ack arrives either, the target enters the
Suspect state.

A ring buffer of the last 16 probe targets is maintained for dashboard
visualization.

## Member States & Incarnation

```
┌─ MemberState ────────────────────────────────────────────────────────────┐
│                                                                          │
│  Alive ─────► Suspect ─────► Dead                                        │
│                                                                          │
│  Priority ordering: Dead (2) > Suspect (1) > Alive (0)                  │
│  Within the same incarnation, higher-priority state always wins.         │
│                                                                          │
│  Incarnation number: bumped only by the node itself to refute            │
│  suspicion. Higher incarnation unconditionally overrides any state.      │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

The membership list is a CRDT with merge semantics:
1. Higher incarnation wins unconditionally.
2. Same incarnation: higher-priority state wins (`Dead > Suspect > Alive`).
3. Lower incarnation is silently ignored.

When a node receives a `Suspect` about itself, it bumps its incarnation and
disseminates an `Alive` update — this is self-refutation.

## Dissemination Queue

Membership updates are not sent in dedicated messages. Instead, they are
**piggybacked** onto existing protocol messages (Pings, Acks, PingReqs).

Each update has a **transmit budget** of `Lambda * ceil(log2(n))` sends, where
`Lambda` (default 3) is the dissemination multiplier and `n` is the cluster
size. The budget ensures logarithmic scaling: a 10-node cluster sends each
update ~12 times; a 1000-node cluster sends it ~30 times.

Updates are sorted by priority when piggybacking, so `Dead` notifications
propagate faster than routine `Alive` heartbeats.

See `crates/distribution/DESIGN_NOTES.md` for the full rationale.

## Lifeguard Health Multiplier

Inspired by the Hashicorp Lifeguard paper, the `HealthMultiplier` tracks
the local node's network health:

- Each successful ack decreases the health score.
- Each nack/timeout increases it (capped at `max_health_score`).
- The score produces a multiplier `1 + score` that stretches probe intervals
  and timeouts.

```rust
// Dynamic suspicion timeout:
// clamp(base * ceil(log2(n+1)) * multiplier, min, max)
pub fn dynamic_suspicion_timeout(&self, cluster_size: usize) -> u64
```

This prevents unhealthy nodes from generating false accusations: a node
that can't reach peers slows its own probing rather than suspecting everyone.

## Join Protocol

A new node contacts seed addresses with `SendJoinRequest`. The receiving
node adds the newcomer to its member list, enqueues the event for
dissemination, and responds with a `SendJoinResponse` containing the full
membership snapshot. The joiner applies the snapshot, learning about all
existing cluster members in one round-trip.

## Where Things Live

| Type | File | Role |
|------|------|------|
| `SwimProbe` | `swim/probe.rs` | Probe cycle state machine |
| `SwimConfig` | `swim/probe.rs` | Protocol tuning knobs |
| `SwimEvent` / `SwimAction` | `swim/probe.rs` | State machine I/O |
| `MemberList` | `swim/member_list.rs` | Membership CRDT |
| `MemberEntry` | `swim/member_list.rs` | Single member record |
| `SwimNode` | `swim/node.rs` | Integrated SWIM facade |
| `NodeAction` | `swim/node.rs` | High-level network actions |
| `DisseminationQueue` | `swim/dissemination.rs` | Piggybacked update queue |
| `HealthMultiplier` | `swim/lifeguard.rs` | Local health tracking |
| `LifeguardConfig` | `swim/lifeguard.rs` | Lifeguard tuning knobs |
| `MemberState` | `types.rs` | `Alive` / `Suspect` / `Dead` enum |
| `NodeRecord` | `types.rs` | Wire-format membership record |
