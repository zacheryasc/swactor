//! RELAY_SPEC §8.3 scenario-loader behavioural tests.
//!
//! Each test pairs with a rule §8.2 names; the body asserts that the
//! rule fires (the malformed scenario rejects) and that a sibling
//! scenario obeying the rule loads cleanly.

use simulation::scenario::{HostKindRegistry, load_from_str};
use std::path::Path;

fn registry() -> HostKindRegistry {
    HostKindRegistry::with_swim()
}

fn try_load(text: &str) -> Result<simulation::scenario::Scenario, String> {
    load_from_str(Path::new("(test)"), text, &registry()).map_err(|e| e.to_string())
}

fn must_load(text: &str) -> simulation::scenario::Scenario {
    try_load(text).expect("scenario must validate")
}

fn must_reject(text: &str, rule_substring: &str) -> String {
    let err = try_load(text).expect_err("scenario must reject");
    assert!(
        err.contains(rule_substring),
        "expected error to mention {rule_substring:?}, got: {err}"
    );
    err
}

const HEADER: &str = r#"
name = "test"
seed = 1
duration_ns = 1_000_000_000
[default_tick]
period_ns = 1_000_000
[default_link]
latency_ns = 1_000_000
jitter_stddev_ns = 0
loss_prob_ppm = 0
reorder_prob_ppm = 0
bandwidth_bps = 1_000_000_000
cold_dial_penalty_ns = 0
cache_warm_after_ns = 1_000_000_000
cache_invalidate_after_idle_ns = 10_000_000_000
"#;

const SWIM_PEERS_A_B: &str = r#"
[[peers]]
id = "a"
kind = "swim"
initial_state = "alive"
kind_config = { probe_interval_ns = 1_000_000, suspicion_timeout_ns = 5_000_000 }
[[peers]]
id = "b"
kind = "swim"
initial_state = "alive"
kind_config = { probe_interval_ns = 1_000_000, suspicion_timeout_ns = 5_000_000 }
"#;

// ────────────────────────────────────────────────────────────────────
// RELAY_SPEC §8.3 — Validation is complete for relay rules
// ────────────────────────────────────────────────────────────────────

#[test]
fn rejects_duplicate_id_across_peers_and_relays() {
    let text = format!(
        "{HEADER}{SWIM_PEERS_A_B}
        [[relays]]
        id = \"a\"
        ingress_capacity_bps = 1_000_000
        egress_capacity_bps_per_link = 1_000_000
        queue_depth_bytes = 1_000
        "
    );
    must_reject(&text, "collides with a declared peer id");
}

#[test]
fn rejects_via_pointing_at_a_peer_instead_of_a_relay() {
    // §8.2 — a via reference to a non-relay id is rejected.
    let text = format!(
        "{HEADER}{SWIM_PEERS_A_B}
        [[peers]]
        id = \"c\"
        kind = \"swim\"
        initial_state = \"alive\"
        kind_config = {{ probe_interval_ns = 1_000_000, suspicion_timeout_ns = 5_000_000 }}
        [[links]]
        from = \"a\"
        to = \"c\"
        via = \"b\"
        "
    );
    must_reject(&text, "via must reference a declared relay");
}

#[test]
fn rejects_ambiguous_route_direct_and_via() {
    // §4.1 — a host pair declared with both a direct edge and a
    // relayed route via shorthand is rejected.
    let text = format!(
        "{HEADER}{SWIM_PEERS_A_B}
        [[relays]]
        id = \"R\"
        ingress_capacity_bps = 1_000_000
        egress_capacity_bps_per_link = 1_000_000
        queue_depth_bytes = 1_000
        [[links]]
        from = \"a\"
        to = \"b\"
        [[links]]
        from = \"a\"
        to = \"b\"
        via = \"R\"
        "
    );
    must_reject(&text, "ambiguous route");
}

#[test]
fn rejects_link_with_relay_endpoint_and_via_field() {
    // §8.2 — a link with `via` must have host endpoints.
    let text = format!(
        "{HEADER}{SWIM_PEERS_A_B}
        [[relays]]
        id = \"R\"
        ingress_capacity_bps = 1_000_000
        egress_capacity_bps_per_link = 1_000_000
        queue_depth_bytes = 1_000
        [[links]]
        from = \"a\"
        to = \"R\"
        via = \"R\"
        "
    );
    must_reject(&text, "host endpoints");
}

#[test]
fn rejects_multi_hop_relay_to_relay_edge() {
    // §8.2 — multi-hop relayed routes (an edge between two relays)
    // are not supported in the MVP.
    let text = format!(
        "{HEADER}{SWIM_PEERS_A_B}
        [[relays]]
        id = \"R1\"
        ingress_capacity_bps = 1_000_000
        egress_capacity_bps_per_link = 1_000_000
        queue_depth_bytes = 1_000
        [[relays]]
        id = \"R2\"
        ingress_capacity_bps = 1_000_000
        egress_capacity_bps_per_link = 1_000_000
        queue_depth_bytes = 1_000
        [[links]]
        from = \"R1\"
        to = \"R2\"
        "
    );
    must_reject(&text, "multi-hop");
}

#[test]
fn rejects_worker_exit_targeting_non_stage_peer() {
    // §8.2 — a worker_exit mutation whose target peer is not
    // stage-kind is rejected at load time.
    let text = format!(
        "{HEADER}{SWIM_PEERS_A_B}
        [[mutations]]
        at_ns = 100
        kind = \"worker_exit\"
        peer = \"a\"
        reason = \"x\"
        "
    );
    must_reject(&text, "only \"stage\" peers accept it");
}

#[test]
fn relay_capacity_change_with_no_fields_is_rejected() {
    let text = format!(
        "{HEADER}{SWIM_PEERS_A_B}
        [[relays]]
        id = \"R\"
        ingress_capacity_bps = 1_000_000
        egress_capacity_bps_per_link = 1_000_000
        queue_depth_bytes = 1_000
        [[mutations]]
        at_ns = 100
        kind = \"relay_capacity_change\"
        relay = \"R\"
        "
    );
    must_reject(&text, "must change at least one");
}

#[test]
fn accepts_valid_relay_scenario_with_via_shorthand() {
    let text = format!(
        "{HEADER}{SWIM_PEERS_A_B}
        [[relays]]
        id = \"R\"
        ingress_capacity_bps = 1_000_000
        egress_capacity_bps_per_link = 1_000_000
        queue_depth_bytes = 1_000
        [[links]]
        from = \"a\"
        to = \"b\"
        via = \"R\"
        [[links]]
        from = \"b\"
        to = \"a\"
        via = \"R\"
        "
    );
    let scen = must_load(&text);
    assert_eq!(scen.relays.len(), 1);
    // Loader expanded shorthand into 4 explicit legs.
    assert_eq!(scen.links.len(), 4);
    // Two relayed routes (a↔b through R).
    assert_eq!(scen.routes.len(), 2);
}

#[test]
fn accepts_stage_peer_with_well_formed_kind_config() {
    let text = format!(
        "{HEADER}
        [[peers]]
        id = \"s\"
        kind = \"stage\"
        initial_state = \"cold\"
        kind_config = {{ name = \"pp-stage\", address = \"10.0.0.1:7700\" }}
        "
    );
    let scen = must_load(&text);
    assert_eq!(scen.peers.len(), 1);
    assert_eq!(scen.peers[0].kind, "stage");
}

#[test]
fn stage_kind_config_missing_name_is_rejected_via_kind_validator() {
    // §8.3 "Stage kind-config is delegated." The kind validator,
    // not the loader's generic rule, surfaces the missing-name
    // message.
    let text = format!(
        "{HEADER}
        [[peers]]
        id = \"s\"
        kind = \"stage\"
        initial_state = \"cold\"
        kind_config = {{ address = \"10.0.0.1:7700\" }}
        "
    );
    must_reject(&text, "required key missing: name");
}

#[test]
fn merge_preserves_relays_from_base() {
    // §8.3 "Merge preserves relays." We construct base + child via
    // file paths so the loader's resolve_extends path runs.
    use std::fs;
    use tempfile::tempdir;
    let dir = tempdir().unwrap();
    let base_path = dir.path().join("base.toml");
    let child_path = dir.path().join("child.toml");
    let base = format!(
        "{HEADER}{SWIM_PEERS_A_B}
        [[relays]]
        id = \"R\"
        ingress_capacity_bps = 1_000_000
        egress_capacity_bps_per_link = 1_000_000
        queue_depth_bytes = 1_000
        [[links]]
        from = \"a\"
        to = \"b\"
        via = \"R\"
        [[links]]
        from = \"b\"
        to = \"a\"
        via = \"R\"
        "
    );
    fs::write(&base_path, &base).unwrap();
    let child = "[base]\nextends = \"base.toml\"\n";
    fs::write(&child_path, child).unwrap();
    let scen = simulation::scenario::load_from_path(&child_path, &registry())
        .expect("child must validate via base");
    assert_eq!(scen.relays.len(), 1, "child inherits the base's relay");
}
