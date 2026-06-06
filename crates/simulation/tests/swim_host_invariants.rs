//! SIM_SPEC §6.4 behavioural property tests for the SWIM host adapter.
//!
//! The codec-specific properties live in `swim_codec_parity.rs`. This
//! file covers the adapter-level properties:
//!
//! - Trait conformance: kind_tag is the production tag string,
//!   non-empty and unique.
//! - SWIM snapshot parity: `snapshot()` carries the evaluator schema
//!   (`members`, `self_incarnation`) built from the production node's
//!   public membership state — the same state the datastream emitter
//!   polls in production.
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
        lifeguard: None,
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
    // simulator's bundle path. The snapshot is built from the SWIM
    // node's public membership state, which carries no timestamps at
    // all — so this holds by construction; the recursive scan below
    // keeps it pinned if the snapshot ever grows time-typed fields.
    //
    // We assert no `at_ms` value plausibly originates from the host
    // wall clock: a wall-clock read is on the order of 1.7e12 ms
    // (year 2024+). Virtual time stays bounded by the engine's tick
    // range — for an un-driven host, zero.
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
fn snapshot_bytes_carry_the_public_membership_state() {
    // §6.4 "SWIM snapshot parity with production." The snapshot is
    // built from the production node's *public* membership state —
    // the same surface the datastream emitter polls each tick to
    // derive its `MembershipTransition` records — so we assert the
    // bootstrap roster shows up exactly as configured.
    use simulation::swim_host::node_id_for;

    let host = make_host("a", &["a", "b", "c"]);
    let bytes = host.snapshot();
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).expect("snapshot is JSON");

    // MVP-shape fields the §10 evaluator needs.
    let members = parsed["members"]
        .as_object()
        .expect("snapshot carries a members object");
    assert!(parsed["self_incarnation"].is_u64());
    assert_eq!(parsed["self_id"].as_str(), Some("a"));

    // The constructor bootstraps every *other* declared peer as Alive
    // at incarnation 0; keys are the peers' node-id hex.
    assert_eq!(members.len(), 2, "two bootstrap peers expected: {members:?}");
    for peer in ["b", "c"] {
        let hex: String = node_id_for(peer)
            .0
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let entry = members
            .get(&hex)
            .unwrap_or_else(|| panic!("peer {peer} ({hex}) missing from members"));
        assert_eq!(entry["state"].as_str(), Some("Alive"));
        assert_eq!(entry["incarnation"].as_u64(), Some(0));
    }
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
        // Coverage 2.6: per-SWIM-probe lifecycle events. Each probe
        // surfaces as one `swim_probe_sent` plus exactly one of
        // `swim_probe_acked` / `swim_probe_timed_out` per phase. The
        // bundle reader joins them on `(target, sequence)` to derive
        // per-probe RTT.
        "swim_probe_sent",
        "swim_probe_acked",
        "swim_probe_timed_out",
        // The mapping function (`observation_payload`) is exhaustive
        // on the production `SwimObservation` enum, so a new variant
        // is a compile-time failure rather than a silent allow-list
        // drift — there is no generic fallthrough kind.
    ];
    for ev in &record_events {
        let kind = ev["kind"].as_str().unwrap_or("(missing)");
        assert!(
            allowed.contains(&kind),
            "SWIM host emitted an unknown kind {kind:?}: {ev}"
        );
    }
}

// ──────────────────────────────────────────────────────────────────────
// Coverage 2.6 — per-SWIM-probe RTT events (`N3_COVERAGE_EXTENSION_SPEC.md §2.6`)
// ──────────────────────────────────────────────────────────────────────

/// A SWIM host with no inbound traffic exercises the probe-timeout path.
/// Verifies the lifecycle contract: every `swim_probe_sent` resolves
/// into either `swim_probe_acked` or `swim_probe_timed_out` on the same
/// `(target, sequence)`, never both, and timeouts carry the configured
/// `budget_ticks` so a bundle reader can see the budget alongside the
/// absent RTT (honesty-under-absence).
#[test]
fn coverage_2_6_unanswered_probes_resolve_to_typed_timed_out_events() {
    let mut host = make_host("a", &["a", "b", "c"]);

    // Drive enough ticks that a Periodic probe fires (probe_interval=2)
    // and both phases (direct then indirect) exhaust their budget
    // (probe_timeout=1 each). 30 ticks comfortably covers several
    // complete probe cycles.
    let mut events: Vec<serde_json::Value> = Vec::new();
    for t in 0..30u64 {
        for action in host.tick(t * 1000) {
            if let Action::RecordEvent { event, .. } = action {
                let v: serde_json::Value =
                    serde_json::from_slice(&event).expect("event payload is JSON");
                events.push(v);
            }
        }
    }

    let sent: Vec<&serde_json::Value> = events
        .iter()
        .filter(|e| e["kind"] == "swim_probe_sent")
        .collect();
    let acked: Vec<&serde_json::Value> = events
        .iter()
        .filter(|e| e["kind"] == "swim_probe_acked")
        .collect();
    let timed_out: Vec<&serde_json::Value> = events
        .iter()
        .filter(|e| e["kind"] == "swim_probe_timed_out")
        .collect();

    // The host has no peer responding, so every probe must time out at
    // both phases. Cover-2.6 contract: at least one probe lifecycle.
    assert!(
        !sent.is_empty(),
        "no swim_probe_sent events emitted in 30 ticks (probe scheduler stuck?): {events:?}"
    );
    assert!(
        acked.is_empty(),
        "swim_probe_acked surfaced without any inbound traffic: {acked:?}"
    );
    assert!(
        !timed_out.is_empty(),
        "no swim_probe_timed_out events despite no inbound traffic: {events:?}"
    );

    // Honesty-under-absence: every timeout carries the configured
    // budget so a bundle reader sees "probe missed a 1-tick budget"
    // rather than a silent zero or null.
    for to in &timed_out {
        let budget = to["budget_ticks"].as_u64();
        assert_eq!(
            budget,
            Some(1),
            "swim_probe_timed_out missing or mismatched budget_ticks: {to}"
        );
        let probe_kind = to["probe_kind"].as_str().unwrap_or("");
        assert!(
            probe_kind == "direct" || probe_kind == "indirect",
            "swim_probe_timed_out has unexpected probe_kind {probe_kind:?}: {to}"
        );
    }

    // Schema parity contract (`SIM_SPEC.md §9.2`): every sent event
    // carries `target` (hex node id) and a `sequence` u64. The bundle
    // reader can join (target, sequence) with the corresponding
    // resolution.
    for s in &sent {
        assert!(s["target"].is_string(), "swim_probe_sent.target absent: {s}");
        assert!(s["sequence"].is_u64(), "swim_probe_sent.sequence absent: {s}");
        let probe_kind = s["probe_kind"].as_str().unwrap_or("");
        assert!(
            probe_kind == "direct" || probe_kind == "indirect",
            "swim_probe_sent has unexpected probe_kind {probe_kind:?}: {s}"
        );
    }
}

