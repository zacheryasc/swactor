//! Property tests for SIM_SPEC §8.4 invariants the per-example tests
//! cannot cover exhaustively: parse invertibility on randomly-generated
//! scenarios, idempotence of load_from_str on identical input, and
//! merge-semantics for the `[base].extends` directive.
//!
//! These are property tests, not white-box: they fix inputs and assert
//! the contracts §8.4 names, not internal data shapes.

use std::path::Path;

use proptest::prelude::*;
use simulation::scenario::{HostKindRegistry, Scenario, load_from_str, to_toml};

fn registry() -> HostKindRegistry {
    HostKindRegistry::with_swim()
}

fn fake_path() -> &'static Path {
    Path::new("test://generated.toml")
}

// ── Strategies ────────────────────────────────────────────────────────

prop_compose! {
    fn arb_peer_id(idx: usize)(suffix in 0u32..1000) -> String {
        format!("peer_{idx}_{suffix}")
    }
}

prop_compose! {
    fn arb_scenario_text()
        (
            n_peers in 1usize..6usize,
            // TOML's number grammar is i64; cap seed accordingly. The
            // spec calls seed u64 to leave room for future encodings,
            // but every seed expressible in TOML today is below this
            // bound.
            seed in 0i64..i64::MAX,
            duration_ns in 1u64..1_000_000u64,
            tick_period_ns in 1u64..1000u64,
            latency_ns in 0u64..1000u64,
            jitter_ns in 0u64..100u64,
            loss_ppm in 0u32..1_000_000u32,
            bw in 1u64..1_000_000u64,
            probe in 1u64..50u64,
            timeout_extra in 1u64..50u64,
        )
        -> String
    {
        let mut text = format!(
            "name = \"g\"\nseed = {seed}\nduration_ns = {duration_ns}\n\n\
             [default_tick]\nperiod_ns = {tick_period_ns}\n\n\
             [default_link]\n\
             latency_ns = {latency_ns}\n\
             jitter_stddev_ns = {jitter_ns}\n\
             loss_prob_ppm = {loss_ppm}\n\
             reorder_prob_ppm = 0\n\
             bandwidth_bps = {bw}\n\
             cold_dial_penalty_ns = 0\n\
             cache_warm_after_ns = 0\n\
             cache_invalidate_after_idle_ns = 1\n\n"
        );
        let suspicion = probe + timeout_extra;
        for i in 0..n_peers {
            text.push_str(&format!(
                "[[peers]]\nid = \"p{i}\"\nkind = \"swim\"\ninitial_state = \"alive\"\n\
                 kind_config = {{ probe_interval_ns = {probe}, suspicion_timeout_ns = {suspicion} }}\n\n"
            ));
        }
        for i in 0..n_peers {
            for j in 0..n_peers {
                if i == j { continue; }
                text.push_str(&format!("[[links]]\nfrom = \"p{i}\"\nto = \"p{j}\"\n\n"));
            }
        }
        text
    }
}

// ── Properties ────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn parse_emit_parse_is_identity(text in arb_scenario_text()) {
        let first = load_from_str(fake_path(), &text, &registry())
            .map_err(|e| TestCaseError::fail(format!("first parse failed: {e}\n--- input ---\n{text}")))?;
        let emitted = to_toml(&first);
        let second = load_from_str(fake_path(), &emitted, &registry())
            .map_err(|e| TestCaseError::fail(format!("re-parse failed: {e}\n--- emitted ---\n{emitted}")))?;
        prop_assert_eq!(first, second);
    }

    #[test]
    fn load_is_idempotent(text in arb_scenario_text()) {
        let a = load_from_str(fake_path(), &text, &registry())
            .map_err(|e| TestCaseError::fail(format!("a: {e}")))?;
        let b = load_from_str(fake_path(), &text, &registry())
            .map_err(|e| TestCaseError::fail(format!("b: {e}")))?;
        prop_assert_eq!(a, b);
    }

    /// Permuting the order of [[links]] entries in TOML does not change
    /// the resulting Scenario except for the order of `links`. We
    /// canonicalise both by sorting on (from, to) before comparing the
    /// links list, and assert all other fields are equal as-is.
    #[test]
    fn link_order_does_not_affect_other_fields(text in arb_scenario_text()) {
        let a: Scenario = load_from_str(fake_path(), &text, &registry()).unwrap();
        // Reverse the [[links]] blocks within the text.
        let reversed = reverse_link_blocks(&text);
        let b: Scenario = load_from_str(fake_path(), &reversed, &registry()).unwrap();

        // Compare everything except links.
        prop_assert_eq!(&a.name, &b.name);
        prop_assert_eq!(a.seed, b.seed);
        prop_assert_eq!(a.duration_ns, b.duration_ns);
        prop_assert_eq!(&a.peers, &b.peers);
        prop_assert_eq!(a.default_tick, b.default_tick);
        prop_assert_eq!(a.default_link, b.default_link);
        prop_assert_eq!(&a.mutations, &b.mutations);
        prop_assert_eq!(&a.snapshots, &b.snapshots);
        prop_assert_eq!(&a.assertions, &b.assertions);

        // Links: same multiset.
        let mut a_links = a.links.clone();
        let mut b_links = b.links.clone();
        a_links.sort_by(|x, y| (x.from.clone(), x.to.clone()).cmp(&(y.from.clone(), y.to.clone())));
        b_links.sort_by(|x, y| (x.from.clone(), x.to.clone()).cmp(&(y.from.clone(), y.to.clone())));
        prop_assert_eq!(a_links, b_links);
    }
}

fn reverse_link_blocks(text: &str) -> String {
    // Split into "before first [[links]]" and "rest"; collect [[links]]
    // blocks; reverse the blocks; reassemble.
    let parts: Vec<&str> = text.split("[[links]]").collect();
    if parts.len() <= 2 {
        return text.to_string();
    }
    let head = parts[0];
    let mut blocks: Vec<&str> = parts[1..].to_vec();
    blocks.reverse();
    let mut out = head.to_string();
    for b in blocks {
        out.push_str("[[links]]");
        out.push_str(b);
    }
    out
}

// ── Merge semantics (§8.4 "Merge is leaves-override, lists-append") ──

#[test]
fn extends_merges_with_leaves_override_lists_append() {
    let tmp = tempdir();
    let base_path = tmp.path().join("base.toml");
    let child_path = tmp.path().join("child.toml");

    std::fs::write(
        &base_path,
        r#"
name = "base"
seed = 100
duration_ns = 1000

[default_tick]
period_ns = 50

[default_link]
latency_ns = 1
jitter_stddev_ns = 0
loss_prob_ppm = 0
reorder_prob_ppm = 0
bandwidth_bps = 1000
cold_dial_penalty_ns = 0
cache_warm_after_ns = 0
cache_invalidate_after_idle_ns = 1000

[[peers]]
id = "a"
kind = "swim"
initial_state = "alive"
kind_config = { probe_interval_ns = 1, suspicion_timeout_ns = 10 }

[[links]]
from = "a"
to = "a"
"#,
    )
    .unwrap();
    // ^ self-loop here is invalid; only used to test merge: child's
    // links must override / append the parent's. We give parent a
    // valid scenario by overriding via the child. Actually no — a
    // self-loop in the parent would surface in the merged validation.
    // Rewrite the parent without a links block.
    std::fs::write(
        &base_path,
        r#"
name = "base"
seed = 100
duration_ns = 1000

[default_tick]
period_ns = 50

[default_link]
latency_ns = 1
jitter_stddev_ns = 0
loss_prob_ppm = 0
reorder_prob_ppm = 0
bandwidth_bps = 1000
cold_dial_penalty_ns = 0
cache_warm_after_ns = 0
cache_invalidate_after_idle_ns = 1000

[[peers]]
id = "a"
kind = "swim"
initial_state = "alive"
kind_config = { probe_interval_ns = 1, suspicion_timeout_ns = 10 }
"#,
    )
    .unwrap();

    std::fs::write(
        &child_path,
        r#"
name = "child_override"

[base]
extends = "base.toml"

[[peers]]
id = "b"
kind = "swim"
initial_state = "alive"
kind_config = { probe_interval_ns = 1, suspicion_timeout_ns = 10 }

[[links]]
from = "a"
to = "b"

[[links]]
from = "b"
to = "a"
"#,
    )
    .unwrap();

    let scenario = simulation::scenario::load_from_path(&child_path, &registry())
        .expect("merged child must validate");

    // Leaves overridden: name comes from child, seed inherited from base.
    assert_eq!(scenario.name, "child_override");
    assert_eq!(scenario.seed, 100);
    assert_eq!(scenario.duration_ns, 1000);

    // Lists appended: peers contains both a (from base) and b (from child),
    // in parent-then-child order.
    let ids: Vec<_> = scenario.peers.iter().map(|p| p.id.as_str()).collect();
    assert_eq!(ids, vec!["a", "b"]);

    // Links appended from child only.
    assert_eq!(scenario.links.len(), 2);
}

fn tempdir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}
