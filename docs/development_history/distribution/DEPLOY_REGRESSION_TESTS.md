# Deploy Regression Tests — Development History

> Covers the addition of deployment topology simulation (NAT, relay, firewall),
> 14 deploy scenario tests, 8 adversarial topology tests, and the supporting
> simulation infrastructure. Motivated by two bugs discovered during a real
> 3-node DigitalOcean deploy.
>
> ~1,230 insertions across 15 modified files + 3 new files
>
> *Branch: `datastore-dashboard`*

---

## Table of Contents

1. [Overview & Motivation](#1-overview--motivation)
2. [The Deploy Bugs](#2-the-deploy-bugs)
3. [What Was Built](#3-what-was-built)
4. [Simulation Infrastructure](#4-simulation-infrastructure)
5. [Deploy Scenario Tests](#5-deploy-scenario-tests)
6. [Adversarial Topology Tests](#6-adversarial-topology-tests)
7. [Bug-Class Regression Validation](#7-bug-class-regression-validation)
8. [SWIM Protocol Enhancements](#8-swim-protocol-enhancements)
9. [Deploy Tooling](#9-deploy-tooling)
10. [Dashboard API](#10-dashboard-api)
11. [Design Decisions](#11-design-decisions)
12. [Known Gaps & Future Work](#12-known-gaps--future-work)

---

## 1. Overview & Motivation

The simulation crate had 15 cluster scenario tests (from the SIMULATION_TESTING
cycle) and 6 original distribution tests. All assumed flat network topologies —
every node could directly reach every other node. No tests modeled NAT, relay
dependencies, firewalled nodes, or the actual deployment sequence where a
controller script orchestrates peer introductions.

During a real 3-node DigitalOcean deploy (1 public VPS + 2 home NAT machines),
two bugs hit that the existing test suite could not have caught:

1. The deploy script sent `join_seed` to the seed node itself
2. Port 3340 was blocked by firewall — all NAT nodes couldn't reach the relay

Both were fixed in production, but nothing prevented the same *class* of bug
from recurring. This work adds simulation-level coverage for deployment
topologies and the controller-driven introduction flow, plus concrete regression
tests that replay the exact bugs.

---

## 2. The Deploy Bugs

### Bug 1: Self-Join ("Connecting to ourself")

**What happened**: The deploy script's peer-sync logic sent each node's own
`node_id` as part of the join-seed list. When the seed node received a
`join_seed` pointing to itself, iroh rejected the connection with "Connecting
to ourself." The seed never learned about other nodes.

**Root cause**: The peer-sync endpoint didn't filter `own_id` from the peer
list before initiating the SWIM join.

**Fix applied**: Filter `own_id` from new peers in `swactor-node/src/main.rs`
before calling join.

**Simulation gap**: No test sent a `Join { node_idx: X, seed_idx: X }` (self-join)
or `Introduce { node_a: X, node_b: X }` (self-introduction). Even if the
protocol handled it gracefully (no crash), the *consequence* — a deploy that
only sends self-joins and never makes real introductions — was untested.

### Bug 2: Firewall Blocks Relay Port

**What happened**: Port 3340 was blocked by the DigitalOcean firewall. All NAT
nodes behind home routers couldn't reach the public relay node. The cluster was
stuck at 0 peers — SWIM probes from NAT→relay were silently dropped.

**Root cause**: The deploy script didn't verify relay port reachability before
proceeding with introductions. The failure was silent — no error, just 0 peers
forever.

**Fix applied**: Added firewall rule for port 3340 to the deploy provisioning.

**Simulation gap**: No test modeled a topology where the relay was alive but
unreachable by NAT nodes. Existing relay-death tests killed the relay entirely,
which is a different failure mode (relay process crash vs. network-level block).

---

## 3. What Was Built

| Component | Location | Description |
|-----------|----------|-------------|
| Network topology model | `sim.rs` | `NodeLocation`, `NetworkTopology`, NAT/firewall reachability |
| Per-link faults | `sim.rs` | `LinkFault`, `SetRelayPenalty` in `NetworkFault` |
| Deferred join | `sim.rs` | Nodes that skip auto-join, require `SimAction::Join`/`Introduce` |
| Controller actions | `sim.rs` | `SimAction::Join`, `SimAction::Introduce` |
| 5 property checkers | `properties.rs` | Group convergence, stability, asymmetry, zero-convergence, staggered join |
| 14 deploy scenario tests | `deploy_scenarios.rs` | NAT topology, relay failure, controller actions, compound faults |
| 8 adversarial topology tests | `topology_adversarial.rs` | Per-link degradation, relay flapping, split-brain, hub saturation |
| Indirect ack forwarding | `swim/node.rs` | `ForwardAck` action for relay-mediated probes |
| `IndirectAck` wire message | `messages.rs` | New message type for forwarded acks |
| Peer sync endpoint | `dashboard/server.rs` | `POST /api/peers/sync` for bulk introduction |
| Native deploy pipeline | `xtask/deploy.rs` | 6-phase provisioning with convergence retry |

All 22 new simulation tests run in ~0.2s total. The full test suite
(existing + new) passes.

---

## 4. Simulation Infrastructure

### Network Topology Model

Three new types model node placement:

```rust
pub enum NodeLocation {
    Public,                       // Cloud VPS — accepts inbound from anyone
    Nat { group: String },        // Behind NAT — same-group LAN only, or via relay
    Firewalled,                   // No inbound or outbound
}

pub struct NetworkTopology {
    pub locations: Vec<NodeLocation>,  // Per-node, indexed by node_idx
    pub relay_nodes: Vec<usize>,       // Indices of relay-capable nodes
}
```

Reachability rules in `NetworkState::directly_reachable()`:

| From \ To | Public | Nat(same) | Nat(diff) | Firewalled |
|-----------|--------|-----------|-----------|------------|
| **Public** | yes | no (can't initiate to NAT) | no | no |
| **Nat(same)** | yes | yes (LAN) | no | no |
| **Nat(diff)** | yes | no | no | no |
| **Firewalled** | no | no | no | no |

Cross-NAT-group communication requires a relay path: both endpoints must be
able to reach an alive relay node (in either direction, since connections are
bidirectional once established).

### Per-Link Faults

Two new `NetworkFault` variants:

```rust
NetworkFault::LinkFault { round, from, to, rate, bidirectional }
NetworkFault::SetRelayPenalty { round, rate }
```

`LinkFault` sets a drop rate on a specific (from, to) pair, enabling targeted
degradation (e.g., "site-b gateway is lossy" without affecting site-a). The
`bidirectional` flag optionally blocks both directions.

`SetRelayPenalty` adds extra drop probability for relay-routed messages. The
composition formula ensures independent fault probabilities:

```
effective_rate = 1 - (1 - base_rate) * (1 - relay_penalty)
```

### Deferred Join & Controller Actions

`DistributionSimConfig` gained:

- `deferred_join: Vec<usize>` — nodes that skip the automatic seed-join during
  setup, modeling nodes that haven't been deployed yet
- `SimAction::Join { node_idx, seed_idx }` — mid-simulation join via a seed
- `SimAction::Introduce { node_a, node_b }` — bidirectional introduction
  modeling `POST /api/peers/sync`

`Introduce` is implemented as two back-to-back `handle_join_request` calls —
A introduces itself to B, then B introduces itself to A — matching the real
deploy flow.

### Property Checkers

Five new property functions in `properties.rs`:

| Function | Purpose |
|----------|---------|
| `check_group_convergence` | Subset of nodes converge (spread within tolerance) after a round |
| `check_membership_stability` | Counts direction flips in member_count (detects suspect→dead cycling) |
| `check_view_asymmetry` | Max spread of member_count across alive nodes |
| `check_zero_convergence` | Detects all-nodes-stuck-at-zero failure mode |
| `check_staggered_join` | Verifies deferred-join nodes reach quorum by deadline |

---

## 5. Deploy Scenario Tests

14 tests in `crates/simulation/tests/deploy_scenarios.rs`, organized by what
they exercise:

### Baseline Topology (Tests 1–3)

| # | Test | Topology | Assertion |
|---|------|----------|-----------|
| 1 | `home_cloud_topology_converges_via_relay` | 1 Public + 2 NAT("home") | 100% accuracy — the "happy path" home deploy |
| 2 | `multi_site_nat_communicates_via_relay` | 1 Public + 2 NAT("home") + 2 NAT("office") | 100% accuracy — multi-site |
| 3 | `relay_death_partitions_nat_groups` | Same as #2, kill relay at round 30 | Home/office groups maintain internal connectivity; cross-group lost |

### Deploy Lifecycle (Tests 4–6)

| # | Test | Scenario | Assertion |
|---|------|----------|-----------|
| 4 | `rolling_redeploy_with_reintroduction` | Kill node 1 at round 20, revive at 40, re-join at 45 | Revived node sees >= 1 member |
| 5 | `staggered_startup_seed_first` | 4 nodes, non-seed deferred, joined at rounds 10/20/30 | All 4 joined by round 100, >= 75% accuracy |
| 6 | `firewalled_node_isolated_others_converge` | 4 normal + 1 firewalled (deferred, never joins) | 4 normal converge; firewalled sees 0 |

### Controller Actions (Tests 7–9)

| # | Test | Scenario | Assertion |
|---|------|----------|-----------|
| 7 | `controller_driven_peer_introduction` | 4 Public nodes, all deferred, all 6 pairs introduced at round 10 | 100% accuracy via Introduce |
| 8 | `deploy_auth_race_recovery_via_two_pass` | 100% drop at round 5 (auth race), clear at 10, re-introduce at 15 | Recovery via two-pass introduction |
| 9 | `degenerate_controller_actions_do_not_degrade_convergence` | Self-joins + self-introductions + redundant re-introductions prepended to real introductions | Converges to 100%; speed gap <= 10 rounds vs. clean run |

### Relay & Fault Scenarios (Tests 10–12)

| # | Test | Scenario | Assertion |
|---|------|----------|-----------|
| 10 | `relay_dependency_failure_prevents_cross_group_convergence` | All NAT↔relay links blocked (firewall) | LAN groups converge internally; full cluster < 100%; not zero |
| 11 | `introduction_strategy_equivalence_under_nat_topology` | Star vs full-mesh vs chain introduction strategies | All >= 75% accuracy; spread <= 0.5 |
| 12 | `mid_deploy_compound_fault_recovery` | 80% drops + seed kill + partition + revive + heal + re-introduce | >= 75% accuracy after recovery; all 5 alive; global convergence by round 60 |

### Bug Replays (Tests 13–14)

| # | Test | Real Bug | Assertion |
|---|------|----------|-----------|
| 13 | `bug_replay_self_join_only_deploy_fails_to_converge` | Deploy sends only self-joins, never cross-node introductions | **Must fail**: zero-convergence, < 50% accuracy |
| 14 | `bug_replay_firewall_blocks_relay_port_silent_isolation` | Firewall blocks all NAT↔relay traffic for entire simulation | **Must fail**: < 100% accuracy; relay isolated at 0 members; LAN peers still see each other |

---

## 6. Adversarial Topology Tests

8 tests in `crates/simulation/tests/topology_adversarial.rs`, focused on
per-link degradation and relay-mediated failure modes:

| # | Test | Scenario | Assertion |
|---|------|----------|-----------|
| 1 | `per_link_degradation_causes_asymmetric_views` | Site-b at 40% link loss, site-a clean | Final spread reflects asymmetry |
| 2 | `relay_penalty_causes_false_suspicions` | 50% relay penalty + tight SWIM timeouts | Not zero-convergence; some accuracy maintained |
| 3 | `asymmetric_relay_links_create_view_divergence` | 60% one-direction loss on relay links | Bounded view divergence |
| 4 | `relay_flapping_causes_membership_oscillation` | 3 relay kill/revive cycles | Membership eventually stabilizes |
| 5 | `hub_saturation_degrades_spoke_connectivity` | Hub alive but 40% lossy to all spokes | Graceful degradation |
| 6 | `correlated_nat_gateway_failure` | All NAT gateway links fail simultaneously | LAN groups survive; cross-group degraded |
| 7 | `split_brain_with_dual_relays` | Kill relay-a, block group-a from relay-b | Detectable partition |
| 8 | `relay_is_target_causes_isolation_on_death` | Relay killed; NAT group loses only relay path | NAT group isolated |

---

## 7. Bug-Class Regression Validation

The two bug-replay tests (13, 14) validate that the simulation framework
*catches the bug class*, not just the specific instance. They model the exact
failure scenario and assert that the buggy deploy **fails to converge** — the
test passes by confirming the failure:

### Self-Join Regression (Test 13)

Models a deploy where the controller only sends self-joins (`Join{0,0}`,
`Join{1,1}`, `Join{2,2}`) and never sends cross-node introductions. All nodes
are deferred, so without correct introductions they never discover each other.

**Assertions (inverted — the test passes when the deploy fails):**
- `check_zero_convergence` must **fail** (all nodes stuck at 0 members)
- Membership accuracy < 0.5

This proves that test 9's assertions (convergence despite degenerate actions)
would catch a deploy that accidentally sends only self-joins.

### Firewall Regression (Test 14)

Models a deploy where `LinkFault { rate: 1.0, bidirectional: true }` blocks all
NAT↔relay traffic for the entire simulation. The deploy script introduces all
pairs, but messages to/from the relay are dropped.

**Assertions (inverted — the test passes when the deploy is degraded):**
- Membership accuracy < 1.0 (full convergence must NOT succeed)
- Relay node isolated at 0 members
- Same-group LAN peers still converge (the failure is cross-group, not total)

This proves that test 10's assertions (degraded accuracy under relay failure)
would detect a silently firewalled relay.

---

## 8. SWIM Protocol Enhancements

### Indirect Ack Forwarding

SWIM's indirect probe path (Prober → Relay → Target) previously had no return
path for the ack. When the relay forwarded a PingReq to the target and the
target replied with an Ack, the ack went directly from target to relay — but
relay didn't know to forward it back to the original prober.

**New flow:**

```
Prober --PingReq--> Relay --Ping--> Target
                    Relay <--Ack--- Target
Prober <--ForwardAck-- Relay
```

The relay tracks pending requests in `pending_relays: Vec<(requester, target, seq)>`.
When an ack arrives matching a pending relay entry, the relay generates a
`ForwardAck` action. The prober handles this via `handle_indirect_ack()`.

**Wire message**: New `IndirectAck` message type with tag `"swactor_dist::IndirectAck"`.

### SWIM Timeout Tuning

`swactor-node` SWIM config adjusted for relay-aware operation:
- `probe_timeout`: 3 → 6 (allows relay RTT)
- `suspicion_timeout`: 20 → 40 (allows refutation piggyback through relay path)

---

## 9. Deploy Tooling

### Native Deploy Pipeline (`xtask/src/deploy.rs`)

6-phase deployment replacing Docker-only approach:

1. **Build**: `cargo build --release -p swactor-node`
2. **Deploy**: Transfer binary + generate `node.toml` + install systemd unit
3. **Health**: Wait for all nodes' dashboard endpoints to respond
4. **Introduce**: `POST /api/peers/sync` with all peers + seed designation
5. **Convergence**: Poll member counts with multi-attempt retry + re-sync on failure
6. **Report**: Final cluster state

Key functions:
- `collect_node_info()` — Gather node IDs and relay URLs from all machines
- `pick_seed()` — Select a relay node as cluster seed
- `sync_peers()` — O(n) bulk peer sync replacing O(n^2) pairwise adds
- `native_deploy_to_machine()` — Full provisioning with absolute path handling

### Peer Introduction Strategy Shift

**Old**: O(n^2) individual `POST /api/peers/add` calls, one per pair.
**New**: Single O(n) `POST /api/peers/sync` per node, sending the full peer
list + seed designation. Each node atomically adds all peers and initiates
the SWIM join.

---

## 10. Dashboard API

### `POST /api/peers/sync` (`dashboard/server.rs`)

New endpoint for bulk peer introduction:

```json
{
  "peers": [
    { "node_id": "abc123...", "relay_url": "https://..." },
    ...
  ],
  "join_seed": "abc123..."
}
```

- Validates all peer node IDs before persisting
- Atomically adds peers and triggers SWIM join to seed
- Supports both hex and base58 node ID encodings
- Returns JSON response with peer count

---

## 11. Design Decisions

| Decision | Rationale |
|----------|-----------|
| LinkFault over RelayPenalty for firewall tests | RelayPenalty only affects relay-*routed* messages; SWIM gossip through the seed's direct NAT→Public connection still disseminates membership. LinkFault blocking all NAT↔relay traffic properly models the real firewall scenario. |
| Bug replays assert failure, not success | Proving a bad deploy *fails to converge* is stronger than proving a good deploy converges. It verifies the property checkers would actually catch the bug. |
| Deferred join as default for controller tests | Real deploys don't auto-join — the controller orchestrates introductions. Deferred join models this accurately. |
| O(n) peer-sync over O(n^2) pairwise | Reduces deploy-time network calls. Single atomic operation per node prevents partial-introduction races. |
| Relay pending_relays capped at 16 | FIFO eviction prevents memory growth from orphaned relay entries. 16 is generous — each probe cycle generates at most `indirect_probes` entries. |
| Inverted assertions for regression tests | `assert!(!zero_check.passed, ...)` reads clearly: "the buggy deploy *should* produce zero-convergence." |

---

## 12. Known Gaps & Future Work

| Gap | Priority | Notes |
|-----|----------|-------|
| Relay penalty + gossip interaction | Medium | RelayPenalty doesn't prevent convergence through gossip — may need a "relay-only topology" mode where cross-group messages MUST go through relay |
| Kademlia under NAT topology | Medium | Directory repair and lookup haven't been tested under NAT constraints |
| Deploy rollback testing | Medium | What happens when a deploy partially succeeds and needs rollback |
| Real DigitalOcean integration test | Low | Run the deploy pipeline against actual DO droplets in CI |
| Chaos engineering mode | Low | Random fault injection during deploy (a la BUGGIFY) |

---

## Files Created/Modified

| Action | File | Purpose |
|--------|------|---------|
| Created | `crates/simulation/tests/deploy_scenarios.rs` | 14 deploy scenario tests |
| Created | `crates/simulation/tests/topology_adversarial.rs` | 8 adversarial topology tests |
| Modified | `crates/simulation/src/distribution/sim.rs` | Topology model, deferred join, link faults, controller actions |
| Modified | `crates/simulation/src/distribution/properties.rs` | 5 new property checkers |
| Modified | `crates/simulation/src/distribution/trace.rs` | New event kinds for introductions |
| Modified | `crates/distribution/src/swim/node.rs` | ForwardAck, pending_relays, diagnostic logging |
| Modified | `crates/distribution/src/messages.rs` | IndirectAck message type |
| Modified | `crates/distribution/src/node.rs` | handle_indirect_ack, piggyback composition |
| Modified | `crates/distribution/src/driver.rs` | Route IndirectAck messages |
| Modified | `crates/distribution/src/iroh_driver.rs` | Relay URL caching |
| Modified | `crates/distribution/tests/common/mod.rs` | Handle ForwardAck in test harness |
| Modified | `crates/dashboard/src/server.rs` | POST /api/peers/sync endpoint |
| Modified | `crates/dashboard/examples/dashboard_demo.rs` | Handle ForwardAck in demo |
| Modified | `crates/swactor-node/src/main.rs` | SWIM timeout tuning, self-join filter |
| Modified | `xtask/src/deploy.rs` | Native deploy pipeline |
| Modified | `xtask/src/main.rs` | Config defaults, native deploy wiring |
| Modified | `.gitignore` | Ignore .deploy/ except example config |
