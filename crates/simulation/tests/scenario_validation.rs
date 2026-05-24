//! §8.4 "Validation is complete" + "Errors are structured" +
//! "Host-kind validation is delegated". Scenario-level tests: each
//! constructs a malformed scenario by mutating a known-good template,
//! and asserts the loader rejects it with an error that names the
//! offending field and the violated rule.

use std::path::{Path, PathBuf};

use simulation::scenario::{HostKindRegistry, LoadError};

fn fake_path() -> PathBuf {
    PathBuf::from("test://inline.toml")
}

fn registry() -> HostKindRegistry {
    HostKindRegistry::with_swim()
}

/// A minimal SWIM scenario template. Tests mutate this string and
/// re-load; that way each rule-violation test is a self-contained
/// scenario tweak rather than a stitched-together fragment.
fn good_scenario() -> String {
    r#"
name = "good"
seed = 1
duration_ns = 1000

[default_tick]
period_ns = 100

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

[[peers]]
id = "b"
kind = "swim"
initial_state = "alive"
kind_config = { probe_interval_ns = 1, suspicion_timeout_ns = 10 }

[[links]]
from = "a"
to = "b"
"#
    .to_string()
}

fn load(text: &str) -> Result<(), LoadError> {
    let path = fake_path();
    simulation::scenario::load_from_str(Path::new(&path), text, &registry()).map(|_| ())
}

/// Assert the load fails and the error names the expected field
/// substring. We do not check the rule wording — that would be a
/// white-box test against current phrasing.
fn assert_rejects(text: &str, expected_field_substr: &str) {
    match load(text) {
        Ok(()) => panic!(
            "expected rejection for field containing {expected_field_substr:?}; scenario loaded successfully"
        ),
        Err(e) => {
            assert!(
                e.field.contains(expected_field_substr),
                "expected field to contain {expected_field_substr:?}, got error:\n{e}"
            );
            assert!(!e.rule.trim().is_empty(), "rule must not be empty");
            assert!(e.rule.lines().count() == 1, "rule must be one line: {:?}", e.rule);
            assert!(
                !e.file.as_os_str().is_empty(),
                "file path must be present in error"
            );
        }
    }
}

#[test]
fn the_template_loads() {
    load(&good_scenario()).expect("template must load");
}

#[test]
fn rejects_duplicate_peer_id() {
    let text = good_scenario().replace(r#"id = "b""#, r#"id = "a""#);
    assert_rejects(&text, "peers[1].id");
}

#[test]
fn rejects_link_to_undeclared_peer() {
    let text = good_scenario().replace(
        "[[links]]\nfrom = \"a\"\nto = \"b\"",
        "[[links]]\nfrom = \"a\"\nto = \"ghost\"",
    );
    assert_rejects(&text, "links[0].to");
}

#[test]
fn rejects_mutation_reference_to_undeclared_peer() {
    let mut text = good_scenario();
    text.push_str(
        r#"
[[mutations]]
kind = "peer_kill"
at_ns = 100
peer = "ghost"
"#,
    );
    assert_rejects(&text, "mutations[0].peer");
}

#[test]
fn rejects_snapshot_reference_to_undeclared_peer() {
    // §8.2 calls out "snapshot or assertion references to undeclared
    // peers"; snapshots have no peer in this schema but assertions do.
    let mut text = good_scenario();
    text.push_str(
        r#"
[[assertions]]
kind = "no_flap_while_probes_ok"
peer = "ghost"
window_start_ns = 0
window_end_ns = 1000
"#,
    );
    assert_rejects(&text, "assertions[0].peer");
}

#[test]
fn rejects_duration_shorter_than_a_mutation_at_ns() {
    let mut text = good_scenario();
    text.push_str(
        r#"
[[mutations]]
kind = "heal"
at_ns = 9999
"#,
    );
    assert_rejects(&text, "mutations[0].at_ns");
}

#[test]
fn rejects_duration_shorter_than_a_snapshot_at_ns() {
    let mut text = good_scenario();
    text.push_str(
        r#"
[[snapshots]]
at_ns = 9999
"#,
    );
    assert_rejects(&text, "snapshots[0].at_ns");
}

#[test]
fn rejects_swim_kind_config_violating_probe_interval_lt_suspicion_timeout() {
    // §8.2: "For the SWIM kind, this includes probe_interval <
    // suspicion_timeout."
    let text = good_scenario().replacen(
        "{ probe_interval_ns = 1, suspicion_timeout_ns = 10 }",
        "{ probe_interval_ns = 10, suspicion_timeout_ns = 10 }",
        1,
    );
    assert_rejects(&text, "kind_config");
}

#[test]
fn rejects_default_link_in_non_ns_unit() {
    // §8.2: "a default_link field that is non-integer, negative, or in
    // a unit other than the §5.2 names (e.g., latency_ms is rejected;
    // only latency_ns)."
    let text = good_scenario().replace("latency_ns = 1", "latency_ms = 1");
    assert_rejects(&text, "default_link");
}

#[test]
fn rejects_negative_default_link_field() {
    let text = good_scenario().replace("latency_ns = 1", "latency_ns = -5");
    assert_rejects(&text, "default_link.latency_ns");
}

#[test]
fn rejects_non_integer_default_link_field() {
    let text = good_scenario().replace("latency_ns = 1", "latency_ns = 1.5");
    // Non-integer parses as a float — toml will refuse to map a float
    // into the integer slot, which the loader surfaces as a parse
    // error against the (root). We accept either rejection as long as
    // it is structured.
    match load(&text) {
        Ok(()) => panic!("expected rejection for non-integer latency_ns"),
        Err(e) => assert!(!e.rule.is_empty()),
    }
}

#[test]
fn rejects_unknown_assertion_kind() {
    let mut text = good_scenario();
    text.push_str(
        r#"
[[assertions]]
kind = "unknown_kind_xyz"
"#,
    );
    assert_rejects(&text, "assertions[0].kind");
}

#[test]
fn rejects_unknown_mutation_kind() {
    let mut text = good_scenario();
    text.push_str(
        r#"
[[mutations]]
kind = "asteroid_strike"
at_ns = 100
"#,
    );
    assert_rejects(&text, "mutations[0].kind");
}

#[test]
fn rejects_unknown_host_kind() {
    let text = good_scenario().replacen(
        r#"kind = "swim""#,
        r#"kind = "paxos""#,
        1,
    );
    assert_rejects(&text, "peers[0].kind");
}

#[test]
fn host_kind_validation_surfaces_kind_rule_not_generic_one() {
    // §8.4 "Host-kind validation is delegated. A host-kind-config
    // error surfaces the kind's own rule, not a loader-generic one."
    let text = good_scenario().replacen(
        "{ probe_interval_ns = 1, suspicion_timeout_ns = 10 }",
        "{ probe_interval_ns = 11, suspicion_timeout_ns = 10 }",
        1,
    );
    let err = load(&text).expect_err("must fail");
    assert!(
        err.rule.contains("probe_interval") && err.rule.contains("suspicion_timeout"),
        "kind-specific rule must surface verbatim; got: {}",
        err.rule
    );
}

#[test]
fn duplicate_directed_edges_are_rejected() {
    let mut text = good_scenario();
    text.push_str(
        r#"
[[links]]
from = "a"
to = "b"
"#,
    );
    assert_rejects(&text, "links[1].from");
}

#[test]
fn empty_peers_table_is_rejected() {
    let text = r#"
name = "no peers"
seed = 1
duration_ns = 100

[default_tick]
period_ns = 1

[default_link]
latency_ns = 1
jitter_stddev_ns = 0
loss_prob_ppm = 0
reorder_prob_ppm = 0
bandwidth_bps = 1
cold_dial_penalty_ns = 0
cache_warm_after_ns = 0
cache_invalidate_after_idle_ns = 1
"#;
    assert_rejects(text, "peers");
}
