//! SIM_SPEC §10.3 / §10.5 library-property runner tests.
//!
//! §10.3 names one MVP property (the gossip-flap detector). The
//! framework itself is host-kind-agnostic; we test the framework
//! against `parity_stub` because it is deterministic. The
//! SWIM-backed gossip-flap variant is a follow-up once the §15
//! amendment unblocks SWIM determinism (notes.md iteration 14).

use simulation::parity_host::{ParityStubFactory, ParityStubKindValidator};
use simulation::property::{
    AssertionTemplate, PropertySpace, replay_property, run_property,
};
use simulation::scenario::HostKindRegistry;

fn registry() -> HostKindRegistry {
    let mut r = HostKindRegistry::with_swim();
    r.register(Box::new(ParityStubKindValidator));
    r
}

fn parity_space() -> PropertySpace {
    let mut kind_config = toml::value::Table::new();
    // The peers list is fixed-shape in the parity_stub validator —
    // we'll set it to the runner-generated peer ids below via the
    // host kind's `peers` argument; the kind_config itself just
    // needs the `peers` key for the validator to accept the peer
    // declaration. We supply a placeholder that the factory
    // overrides on `build`.
    kind_config.insert(
        "peers".into(),
        toml::Value::Array(vec![toml::Value::String("placeholder".into())]),
    );
    PropertySpace {
        peer_count_min: 3,
        peer_count_max: 3,
        latency_ns_min: 1_000_000,
        latency_ns_max: 1_000_000,
        jitter_ns_min: 0,
        jitter_ns_max: 0,
        loss_ppm_min: 0,
        loss_ppm_max: 0,
        duration_ns: 1_000_000_000,
        default_tick_period_ns: 50_000_000,
        host_kind: "parity_stub".into(),
        kind_config,
        initial_state: "ready".into(),
        assertion_templates: vec![AssertionTemplate::EventCount {
            event_kind: "tick_record".into(),
            max: 100_000,
        }],
    }
}

// ──────────────────────────────────────────────────────────────────────
// §10.3 — runner produces a result per sample
// ──────────────────────────────────────────────────────────────────────

#[test]
fn run_property_produces_one_result_per_sample() {
    let space = parity_space();
    let registry = registry();
    let results = run_property(&space, 0xc0ffee, 3, &registry, || Box::new(ParityStubFactory));
    assert_eq!(results.len(), 3);
    let indices: Vec<u32> = results.iter().map(|r| r.sample_index).collect();
    assert_eq!(indices, vec![0, 1, 2]);
    // Every result has a verdict per template-derived assertion.
    for r in &results {
        assert!(!r.verdicts.is_empty(), "sample {} has no verdicts", r.sample_index);
    }
}

// ──────────────────────────────────────────────────────────────────────
// §10.5 "Property failures replay exactly"
// ──────────────────────────────────────────────────────────────────────

#[test]
fn replay_with_same_root_seed_and_index_yields_identical_verdicts() {
    let space = parity_space();
    let registry = registry();
    let root = 0xfade_cafe_dead_babeu64 & (i64::MAX as u64);

    let initial = run_property(&space, root, 4, &registry, || Box::new(ParityStubFactory));
    for original in &initial {
        let replayed = replay_property(
            &space,
            root,
            original.sample_index,
            &registry,
            Box::new(ParityStubFactory),
        );
        assert_eq!(replayed.seed, original.seed);
        assert_eq!(replayed.verdicts, original.verdicts);
        assert_eq!(replayed.scenario.name, original.scenario.name);
        // Compare the scenario via TOML round-trip — generated
        // scenarios are pure functions of (space, seed).
        assert_eq!(
            simulation::scenario::to_toml(&replayed.scenario),
            simulation::scenario::to_toml(&original.scenario),
        );
    }
}

// ──────────────────────────────────────────────────────────────────────
// Sensitivity — root seed change must produce different scenarios
// ──────────────────────────────────────────────────────────────────────

#[test]
fn different_root_seeds_yield_distinguishable_property_results() {
    // With identical space ranges (all min == max in parity_space)
    // the scenario content is identical regardless of seed. Widen
    // the latency range so the seed actually changes the scenario.
    let mut space = parity_space();
    space.latency_ns_min = 100_000;
    space.latency_ns_max = 10_000_000;
    let registry = registry();
    let a = run_property(&space, 0x01, 1, &registry, || Box::new(ParityStubFactory))
        .into_iter()
        .next()
        .unwrap();
    let b = run_property(&space, 0x02, 1, &registry, || Box::new(ParityStubFactory))
        .into_iter()
        .next()
        .unwrap();
    // Different root seeds derive different sample seeds.
    assert_ne!(a.seed, b.seed);
    // And the scenarios materially differ on the seeded parameter.
    assert_ne!(
        a.scenario.default_link.latency_ns,
        b.scenario.default_link.latency_ns
    );
}

// ──────────────────────────────────────────────────────────────────────
// Failed-sample bookkeeping — `PropertyResult::failed`
// ──────────────────────────────────────────────────────────────────────

#[test]
fn property_result_failed_flag_tracks_any_fail_verdict() {
    // Force a Fail by setting `event_count { max: 0 }` and letting
    // the parity-stub hosts emit at least one `tick_record`.
    let mut space = parity_space();
    space.assertion_templates = vec![AssertionTemplate::EventCount {
        event_kind: "tick_record".into(),
        max: 0,
    }];
    let registry = registry();
    let result = replay_property(&space, 0, 0, &registry, Box::new(ParityStubFactory));
    assert!(
        result.failed(),
        "expected a Fail verdict; got {:?}",
        result.verdicts
    );
    use simulation::evaluator::Outcome;
    assert!(result.verdicts.iter().any(|v| matches!(v.outcome, Outcome::Fail)));
}
