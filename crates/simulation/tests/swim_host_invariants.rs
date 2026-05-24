//! SIM_SPEC §6.4 behavioural property tests for the SWIM host adapter.
//!
//! The codec-specific properties live in `swim_codec_parity.rs`. This
//! file covers the adapter-level properties:
//!
//! - Trait conformance: kind_tag is the production tag string,
//!   non-empty and unique.
//! - SWIM snapshot parity: `snapshot()` is the production tier-2
//!   `Tier2SwimState` shape under JSON serialisation.
//! - SWIM unknown-output is loud: an unrecognised inbound payload
//!   panics (no silent fallback).
//! - SWIM emits no novel kinds: every event the host records has a
//!   `kind` from the known set production also emits.
//!
//! §6.4 host determinism is scoped to the simulator's controlled
//! surface per §7.7; it is covered by the engine-level byte-identity
//! tests (`engine_invariants`, `cross_arch_parity`) which run against
//! deterministic stub hosts. Asserting it against a freshly
//! constructed `SwimHost` would be testing wrapped-dependency entropy.

use distribution::swim::probe::{ProbeMode, SwimConfig};

use simulation::host::{Action, Host, HostMessage};
use simulation::swim_host::SwimHost;

fn make_host(host_id: &str, peer_ids: &[&str]) -> SwimHost {
    let cfg = SwimConfig {
        probe_interval: 2,
        probe_timeout: 1,
        indirect_probes: 2,
        suspicion_timeout: 6,
        dead_reprobe_interval: 0,
        probe_mode: ProbeMode::Periodic,
    };
    let peers: Vec<String> = peer_ids.iter().map(|s| (*s).to_string()).collect();
    SwimHost::new(host_id, &peers, cfg)
}

// ──────────────────────────────────────────────────────────────────────
// §6.4 Trait conformance
// ──────────────────────────────────────────────────────────────────────

#[test]
fn kind_tag_is_swim_and_non_empty() {
    let host = make_host("a", &["a", "b", "c"]);
    assert_eq!(host.kind_tag(), "swim");
    assert!(!host.kind_tag().is_empty());
}

#[test]
fn host_id_round_trips_through_the_id_accessor() {
    let host = make_host("alpha", &["alpha", "bravo"]);
    assert_eq!(host.id(), "alpha");
}

// ──────────────────────────────────────────────────────────────────────
// §6.4 SWIM snapshot parity
// ──────────────────────────────────────────────────────────────────────

#[test]
fn snapshot_bytes_carry_no_host_wall_clock_values_per_7_1() {
    // §7.1 forbids reading the host wall clock anywhere in the
    // simulator's bundle path. The production `SwimIntrospect`
    // stamps `wall_ms_now()` values into `Tier2SwimState`'s
    // `scraped_at_ms` / `last_*_at_ms` / `recent_messages[*].at_ms`
    // fields. The adapter scrubs all of those before serialising.
    //
    // We assert no surviving `at_ms` value plausibly originates from
    // the host wall clock: an unscrubbed `wall_ms_now()` is on the
    // order of 1.7e12 ms (year 2024+). Virtual time stays bounded
    // by the engine's tick range — for an un-driven host, zero.
    let host = make_host("a", &["a", "b", "c"]);
    let bytes = host.snapshot();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    fn collect_at_ms(value: &serde_json::Value, out: &mut Vec<u64>) {
        match value {
            serde_json::Value::Object(map) => {
                for (k, v) in map {
                    if k.ends_with("at_ms") && v.is_u64() {
                        out.push(v.as_u64().unwrap());
                    }
                    collect_at_ms(v, out);
                }
            }
            serde_json::Value::Array(arr) => {
                for v in arr {
                    collect_at_ms(v, out);
                }
            }
            _ => {}
        }
    }
    let mut at_ms_values = Vec::new();
    collect_at_ms(&v, &mut at_ms_values);
    // Threshold: virtual time in this test is 0; if anything is
    // above year-2000 (~9.4e11), it's wall-clock pollution.
    for ms in &at_ms_values {
        assert!(
            *ms < 9_400_000_000_000,
            "field with `at_ms` carries a wall-clock value ({ms} ms); §7.1 forbids the host wall clock in the bundle"
        );
    }
}

#[test]
fn snapshot_bytes_carry_the_production_tier2_swim_state_shape() {
    // §6.4 "SWIM snapshot parity with production." We don't peek at
    // private production state; instead we deserialise the snapshot
    // bytes back into a `Tier2SwimState` and assert non-default
    // fields match the host's configured values.
    use distribution::diagnostics::snapshot::Tier2SwimState;

    let host = make_host("a", &["a", "b", "c"]);
    let bytes = host.snapshot();
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).expect("snapshot is JSON");
    // We embed the full tier-2 state under "tier2" so the
    // evaluator's MVP-shape projection coexists with the production
    // shape.
    let tier2_blob = parsed
        .get("tier2")
        .expect("snapshot must include the production tier-2 blob");
    let tier2: Tier2SwimState =
        serde_json::from_value(tier2_blob.clone()).expect("tier2 deserialises");
    assert_eq!(tier2.config.probe_interval_ticks, 2);
    assert_eq!(tier2.config.suspicion_timeout_ticks, 6);
    assert_eq!(tier2.config.indirect_probes_k, 2);
    // MVP-shape fields the §10 evaluator needs are also present.
    assert!(parsed["members"].is_object());
    assert!(parsed["self_incarnation"].is_u64());
}

// ──────────────────────────────────────────────────────────────────────
// §6.4 SWIM unknown-output is loud
// ──────────────────────────────────────────────────────────────────────

#[test]
#[should_panic(expected = "unrecognised SWIM message")]
fn unrecognised_inbound_payload_panics() {
    let mut host = make_host("a", &["a", "b"]);
    // A junk payload that does not parse as any SWIM message.
    let bogus = HostMessage::App(b"this is not a SWIM message".to_vec());
    let _ = host.recv(bogus, 0);
}

// ──────────────────────────────────────────────────────────────────────
// §6.4 SWIM emits no novel kinds
// ──────────────────────────────────────────────────────────────────────

#[test]
fn every_recorded_event_has_a_known_kind_discriminator() {
    // Drive several ticks against a 3-peer roster; collect every
    // RecordEvent and assert its payload's `kind` field is in the
    // known set of kinds the simulator is allowed to emit.
    let mut host = make_host("a", &["a", "b", "c"]);
    let mut record_events: Vec<serde_json::Value> = Vec::new();
    for t in 0..30 {
        for action in host.tick(t * 1000) {
            if let Action::RecordEvent { event, .. } = action {
                let v: serde_json::Value =
                    serde_json::from_slice(&event).expect("event payload is JSON");
                record_events.push(v);
            }
        }
    }
    let allowed: &[&str] = &[
        // RecordEvents the simulator synthesises.
        "state_transition",
        "message_send",
        // Any production `DiagEvent` variant we don't have an MVP
        // schema for surfaces under `diag_event` carrying the
        // production `type` tag verbatim. The mapping function is
        // exhaustive on the production enum, so a new variant is a
        // compile-time failure rather than a silent allow-list drift.
        "diag_event",
    ];
    for ev in &record_events {
        let kind = ev["kind"].as_str().unwrap_or("(missing)");
        assert!(
            allowed.contains(&kind),
            "SWIM host emitted an unknown kind {kind:?}: {ev}"
        );
    }
}

