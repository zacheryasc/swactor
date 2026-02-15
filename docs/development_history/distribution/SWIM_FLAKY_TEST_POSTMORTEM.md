# After-Action: SWIM Flaky Test (`node_death_tombstones_entries`)

> A ~20% failure rate in a SWIM death-detection test sat undetected because every gate that should have caught it — CI, assertion design, test harness correctness, and flakiness discipline — was either absent or structurally unable to surface the bug.

---

## 1. Incident Summary

The `node_death_tombstones_entries` test in `registry.rs` failed roughly 1 in 5 runs. Two independent bugs conspired to produce the flakiness:

**Bug 1 — Protocol:** `check_suspicion_timeouts` in `probe.rs` declared nodes dead on timer expiry without checking whether the node was still `Suspect`. If a refutation (Alive with higher incarnation) arrived between suspicion and timeout, the node was killed anyway. The original code:

```rust
for node_id in expired {
    if members.declare_dead(node_id) {
        actions.push(SwimAction::DeclareDead(node_id));
    }
    self.cancel_suspicion_timer(node_id);
}
```

**Bug 2 — Test harness:** The 3-node gossip loop delivered actions to multiple targets in a single `deliver_actions` call, then attributed all responses to a single `sender_id`. When A ticked and sent pings to both B and C, the responses from both were delivered back to A as if they all came from A — misattributing the sender. This caused C's pings to look like self-pings, triggering false suspicions that fed into Bug 1.

The interaction: the sender misattribution created false suspicions at non-deterministic rates (depending on tick ordering), and the missing state guard turned those false suspicions into false death declarations. When B was falsely declared dead, its name registration was tombstoned and the test's soft assertion silently passed without verifying the mechanism worked.

**The fix** (commit `205dc23`, 2 files, +45/−14): added a `still_suspect` guard before `declare_dead`, and split the gossip loop to deliver to each target node separately so responses carry the correct `sender_id`.

See the commit diff for full technical details. The rest of this document focuses on how a 20% failure rate was merged and what changes prevent it from happening again.

---

## 2. How This Got Into the Repo

Five gates should have caught this. All five failed.

### 2a. No CI exists

There is no automated testing infrastructure. No pre-merge checks, no post-push smoke tests. The project uses Forgejo for source hosting. CI integration is being planned separately.

A 20% failure rate is invisible with a single manual `cargo test` — you hit the 80% pass rate and move on. CI running tests on every push would have surfaced the failure within a handful of commits. Without it, the only defense is the developer's willingness to run the test more than once. That is not a defense.

### 2b. The soft assertion hid failures

The test ended with this:

```rust
let a_resolved = a.resolve_name("b-service");

if a_resolved.is_none() {
    // A has tombstoned it — propagate to C.
    gossip_rounds(&mut a, a_id, &mut c, c_id, 5);
    assert_eq!(c.resolve_name("b-service"), None,
        "C should see tombstone after B's death propagates");
}
// If SWIM hasn't declared death yet, the test still passes — the mechanism
// is wired, just needs more ticks. The important thing: no panics, clean flow.
```

If SWIM didn't declare B dead — whether because it correctly needed more ticks *or* because the test harness was broken — the test passed. The comment "the mechanism is wired, just needs more ticks" was written with honest intent but created a test that could never fail for the wrong reason *and* never fail for the right reason. A test that can't fail is not a test.

### 2c. The 2-node test harness doesn't generalize to 3 nodes

The `deliver_actions` helper in `registry.rs` takes a `sender_id` parameter and delivers all actions to a list of target nodes, collecting all responses into a flat `Vec<NodeAction>`. When the caller attributes those responses with a single `sender_id`, the implicit assumption is: every response in the vec came from the same node.

This is correct for 2-node tests — if A sends to B, all responses came from B. Every other test in `registry.rs` and `node_integration.rs` uses exactly this pattern with exactly 2 nodes. It works perfectly.

The `node_death_tombstones_entries` test was the first to use 3 nodes. It passed actions to `deliver_actions` with both B and C in the targets list, then attributed all responses to a single sender. Nobody noticed the attribution broke because:

1. The helper's API doesn't prevent it — it returns a flat `Vec`, not a per-target map.
2. All other tests were 2-node, establishing a pattern that appeared safe.
3. The simulation layer (`sim.rs:deliver_actions_tagged_with_net`) already solved this correctly with response-tagged delivery: `Vec<(usize, Vec<NodeAction>)>`. The test harness didn't reuse that pattern.

### 2d. The simulation layer is a false safety net

The simulation test suite is extensive: 19 cluster scenarios and 96 total tests across 7 test files. They exercise partition healing, cascading failures, 50-node convergence, message loss, and actor resolution during network events. They all pass.

But simulation bypasses the unit test harness entirely. `deliver_actions_tagged_with_net` in `sim.rs` routes responses back with the correct `(responder_idx, Vec<NodeAction>)` tagging:

```rust
fn deliver_actions_tagged_with_net(
    actions: &[NodeAction],
    sender_idx: usize,
    sender_id: NodeId,
    nodes: &mut [Option<DistributedNode>],
    node_ids: &[NodeId],
    net: &mut NetworkState,
) -> Vec<(usize, Vec<NodeAction>)> {
```

The simulation proved the protocol works while the unit test harness was silently broken. The simulation caught 0% of this bug because the bug lived in the test harness, not the protocol. Bug 1 (the missing `still_suspect` guard) *could* have been caught by simulation — but only with a scenario specifically designed to refute a suspected node before timeout expiry. No such scenario existed because the guard's absence is only observable under that exact sequence.

### 2e. No flakiness detection discipline

There is no practice of running timing-sensitive tests multiple times before merge. No tooling (`cargo-nextest`, `just test-repeat`, a loop in a shell script) to surface intermittent failures.

A 20% failure rate requires only 5 runs to detect with 99.97% probability: `1 − 0.8^5 = 0.99968`. Nobody ran it 5 times.

---

## 3. Preventing This Class of Bug

Each recommendation addresses a specific gap from section 2. They are ordered from structural (make the bug class impossible) to procedural (catch it if it happens).

### 3a. Shared test harness with sender-tagged delivery — DONE

**Gap addressed:** 2c (harness doesn't generalize to 3+ nodes)

**Implemented:** A `TestCluster` harness was extracted into `crates/distribution/tests/common/mod.rs`. It contains:

- `test_config()` — the shared `DistributedNodeConfig` previously duplicated in both test files.
- `deliver_actions_tagged()` — free function returning `Vec<(usize, Vec<NodeAction>)>` (responses tagged by responder index), modeled on the simulation's `deliver_actions_tagged_with_net`. An `excluded` slice parameter handles death simulation (nodes that neither tick nor receive).
- `TestCluster` struct — owns parallel `Vec<NodeId>` and `Vec<DistributedNode>` (borrow-split friendly). Public API: `new(n)`, `with_config(n, config)`, `node_id(idx)`, `Index`/`IndexMut` for direct node access, `gossip_rounds(n)`, and `gossip_rounds_excluding(dead, n)`.

`gossip_round()` follows the simulation's `tick_all_and_deliver` pattern: tick all live nodes, deliver with tagged responses, deliver responses back using the responder's identity. Sender misattribution is **structurally impossible** — the tagged return type forces correct attribution at every delivery step.

All duplicated helpers (`deliver_actions`, `form_cluster`, `join_nodes`, `gossip_rounds`) were removed from both `registry.rs` and `node_integration.rs`. 4 multi-node tests in `registry.rs` and 6 in `node_integration.rs` were rewritten to use `TestCluster`. Single-node tests use only `test_config()` from common.

The formerly flaky `node_death_tombstones_entries` went from a 50-line manual per-target delivery loop to 3 calls: `cluster.gossip_rounds(5)`, `cluster.gossip_rounds_excluding(&[1], 20)`, `cluster.gossip_rounds_excluding(&[1], 5)`. Verified 50/50 passes post-rewrite.

### 3b. Hard assertions, no soft paths

**Gap addressed:** 2b (soft assertion)

Every test must assert on its expected outcome unconditionally. No `if result.is_none() { ... }` pass-either-way branches. If SWIM needs more ticks to detect death, give it more ticks. Don't let the test pass when the expected behavior didn't happen.

Add negative assertions where applicable. For instance, after B dies, assert that C is still Alive — not just that B is Dead. This catches false-positive death declarations that spill over to healthy nodes.

### 3c. `declare_dead` state contract

**Gap addressed:** defense in depth

`MemberList::declare_dead` currently accepts any non-`Dead` state:

```rust
pub fn declare_dead(&mut self, node_id: NodeId) -> bool {
    if let Some(entry) = self.members.get_mut(&node_id) {
        if entry.state != MemberState::Dead {
            entry.state = MemberState::Dead;
            return true;
        }
    }
    false
}
```

It should guard that the node is `Suspect` at the point of call, not leave correctness to callers. The SWIM protocol invariant is: a node transitions `Alive → Suspect → Dead`. Killing an `Alive` node directly violates that invariant. The caller (`check_suspicion_timeouts`) now checks, but the function's own contract should enforce the invariant independently.

### 3d. Multi-run flakiness detection — RESOLVED (policy)

**Gap addressed:** 2e (no flakiness discipline)

**Policy:** No flaky tests are accepted into the repository. Tests must pass deterministically. Timing-sensitive tests involving 3+ nodes should be run multiple times before merge to verify stability. Tooling (`cargo-nextest`, `justfile` targets) can be adopted as needed but the policy is the primary gate.

### 3e. CI

**Gap addressed:** 2a (no CI)

The project uses Forgejo for source hosting. CI workflow integration is being planned separately and is not an action item for this postmortem. When available, the CI pipeline should run `cargo test --workspace` on push to main and on PR. Stretch: `cargo nextest run --retries 3` to specifically surface flaky tests before merge.

### 3f. Simulation scenario for suspect-then-refute — DONE

**Gap addressed:** 2d (simulation didn't test the violated invariant)

**Implemented:** `suspect_refuted_before_timeout_no_false_death` in `crates/simulation/tests/cluster_scenarios.rs`. 3-node cluster with asymmetric partitions making node 1 unreachable by probes from nodes 0 and 2, while node 1's outgoing messages still carry incarnation bumps enabling refutation. Partitions heal before the suspicion timer fires. Asserts all 3 nodes alive (no false deaths) and 100% membership accuracy after refutation.

---

## 4. Files Modified

| File | Change |
|------|--------|
| `crates/distribution/src/swim/probe.rs` | Added `still_suspect` guard in `check_suspicion_timeouts`; cancel probe phase if target declared dead (+20/−2) |
| `crates/distribution/tests/registry.rs` | Split 3-node gossip delivery to per-target calls with correct sender attribution (+25/−12) |

### Follow-up: Flaky tests removed and replaced

The three tests with pass-either-way assertions were removed and replaced with deterministic equivalents:

| File | Test | Change |
|------|------|--------|
| `crates/distribution/tests/swim_node.rs` | `membership_updates_piggyback_on_pings` | Replaced `if !pings.is_empty()` guard with hard assertion; tick 6 to ensure probe fires |
| `crates/distribution/tests/swim_node.rs` | `leave_enqueues_death_for_dissemination` | Replaced tautological `alive_count() > 0` fallback with hard assertion on piggyback |
| `crates/distribution/tests/registry.rs` | `node_death_tombstones_entries` | Replaced soft `if a_resolved.is_none()` branch with unconditional `assert_eq!` |

Additionally, `MemberList::declare_dead` was tightened to only accept `Suspect → Dead` transitions, enforcing the SWIM lifecycle invariant at the function boundary.

### Follow-up: Shared `TestCluster` harness (recommendation 3a)

Extracted a shared test harness that makes sender-misattribution structurally impossible:

| File | Change |
|------|--------|
| `crates/distribution/tests/common/mod.rs` | **New** — `test_config()`, `deliver_actions_tagged()`, `TestCluster` struct |
| `crates/distribution/tests/registry.rs` | Removed `test_config`, `deliver_actions`, `form_cluster`, `gossip_rounds` helpers; added `mod common`; rewrote 4 multi-node tests to use `TestCluster` |
| `crates/distribution/tests/node_integration.rs` | Removed `test_config`, `deliver_actions`, `join_nodes` helpers; added `mod common`; rewrote 6 multi-node tests to use `TestCluster` |
| `crates/distribution/src/node.rs` | Added `#[derive(Clone)]` on `DistributedNodeConfig` (required by `TestCluster::with_config`) |
| `crates/distribution/src/registry.rs` | Added `#[derive(Clone)]` on `RegistryConfig` (transitive requirement) |

### Follow-up: `watch_notification` missing `StdExtension`

The `watch_notification` test in `crates/bin-runner/tests/wasm_actor.rs` panicked because the runtime was created without `StdExtension`, which `ctx.watch()` requires (via `get_ext()` in `crates/std/src/ctx_ext.rs`). Every other test in the workspace that uses `ctx.watch()` installs the extension — this one was simply missed.

Same root cause pattern as the SWIM flaky test: a test harness setup gap that was invisible because no other test exercised that path with that configuration. Unlike the SWIM bug, this was a hard failure (panic), not a flaky one — it failed 100% of the time.

| File | Change |
|------|--------|
| `crates/bin-runner/tests/wasm_actor.rs` | Added `StdExtension` to imports; installed `.with_extension(Arc::new(StdExtension::new()))` on the runtime in `watch_notification` |

32/32 bin-runner tests now pass.

---

## 5. Verification

- **Post-fix:** 0/50 failures (was ~10/50 pre-fix)
- **Full distribution suite:** all tests pass
- **Simulation suite:** all 96 tests pass (unaffected — the bugs were in the unit test harness and a protocol guard, not the simulation layer)
- **Post-`TestCluster` extraction:** all 149 distribution tests pass; `node_death_tombstones_entries` verified 50/50 passes after rewrite to `TestCluster`; all 65 simulation tests unaffected
