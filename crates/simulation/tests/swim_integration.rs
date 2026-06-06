//! End-to-end integration: scenario loader → engine →
//! SwimHostFactory → network → bundle writer → assertion evaluator.
//!
//! Two scenarios live here:
//!
//! - `swim_integration.toml` — the wiring smoke test for the §6.2
//!   SWIM host.  Both assertions pass; this verifies the pipeline
//!   reaches a clean bundle.
//! - `gossip_flap.toml` — the §1 "done" reproduction of the N3
//!   deployment SWIM gossip-flap bug.  Asserts that at least one
//!   verdict comes back Fail on the current production SWIM source.

use std::fs;
use std::path::{Path, PathBuf};

use tempfile::TempDir;

use simulation::bundle_file::FileBundleWriter;
use simulation::engine::{Engine, TerminationReason};
use simulation::evaluator::evaluate_bundle;
use simulation::network::Network;
use simulation::scenario::{HostKindRegistry, Scenario, load_from_path};
use simulation::swim_host::SwimHostFactory;

fn registry() -> HostKindRegistry {
    HostKindRegistry::with_swim()
}

fn load(rel: &str) -> Scenario {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(rel);
    load_from_path(&path, &registry()).expect("scenario validates")
}

fn run_to_bundle(scenario: &Scenario, out: &Path, pop_budget: u64) {
    let writer = FileBundleWriter::new(out, scenario.clone());
    let network = Network::new(scenario);
    let mut engine = Engine::new(scenario, network, writer);
    engine.register_factory(Box::new(SwimHostFactory));
    engine.auto_install_hosts();
    engine.set_pop_budget(pop_budget);
    let term = engine.run();
    assert!(
        matches!(
            term,
            TerminationReason::DurationReached | TerminationReason::EarlyAllAssertionsResolved
        ),
        "unexpected termination {term:?}"
    );
    let writer = engine.into_writer();
    writer.finalize().expect("finalize bundle");
}

#[test]
fn swim_integration_smoke_writes_a_bundle_with_swim_events_and_passes_assertions() {
    let scen = load("scenarios/reproduction/swim_integration.toml");
    let tmp = TempDir::new().unwrap();
    let out = tmp.path().join("bundle");
    // ~100 ticks * 3 hosts ~= 300 tick pops + delivers; 20k is generous.
    run_to_bundle(&scen, &out, 20_000);

    // Bundle layout exists.
    assert!(out.join("events.ndjson").is_file());
    assert!(out.join("manifest.json").is_file());
    assert!(out.join("snapshots").is_dir());

    // SWIM hosts actually emitted RecordEvents.
    let events_text = fs::read_to_string(out.join("events.ndjson")).unwrap();
    let mut swim_kind_tags = 0u64;
    let mut state_transitions = 0u64;
    for line in events_text.lines() {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        if v["kind_tag"] == "swim" {
            swim_kind_tags += 1;
        }
        if v["event"]["kind"] == "state_transition" {
            state_transitions += 1;
        }
    }
    assert!(
        swim_kind_tags > 0,
        "expected SWIM-tagged events in the bundle; events.ndjson has {} bytes",
        events_text.len()
    );
    // Every membership change rides production's own `SwimTransition`
    // diagnostic, which the sim records as `state_transition`. The
    // smoke scenario shouldn't trigger a flap, so we don't assert on
    // the count.
    let _ = state_transitions;

    // Snapshots are well-formed.
    let alpha_snap = fs::read_to_string(out.join("snapshots/alpha/0.json")).unwrap();
    let snap_v: serde_json::Value = serde_json::from_str(&alpha_snap).unwrap();
    assert!(snap_v["snapshot"]["members"].is_object());
    assert!(snap_v["snapshot"]["self_incarnation"].is_u64());

    // Run the assertion evaluator on the bundle. Both declared
    // assertions are generous bounds; they should pass.
    let verdicts = evaluate_bundle(&out).expect("evaluator runs");
    assert_eq!(verdicts.len(), 2);
    for v in &verdicts {
        use simulation::evaluator::Outcome;
        assert!(
            matches!(v.outcome, Outcome::Pass | Outcome::Inconclusive),
            "smoke assertion {v:?} unexpectedly failed"
        );
    }
}

#[test]
fn gossip_flap_repro_fails_self_incarnation_bounded_on_current_swim_source() {
    // SIM_SPEC §1 "done": a property test reproduces the gossip-flap
    // bug deterministically against the current SWIM source. We run
    // the canonical reproduction scenario end-to-end and assert that
    // at least one declared verdict comes back Fail. Inside that
    // failure set, `self_incarnation_bounded` is the one that
    // actually reproduces the production symptom — the orchestrator's
    // self_incarnation runs away from rebutted Suspect piggybacks.
    use simulation::evaluator::Outcome;

    let scen = load("scenarios/reproduction/gossip_flap.toml");
    let tmp = TempDir::new().unwrap();
    let out = tmp.path().join("bundle");
    // Generous: 20s sim @ 50ms ticks * 3 hosts = 1200 tick pops, plus
    // ~3-4× that in message deliveries. 200k caps a runaway loop
    // without truncating a healthy run.
    run_to_bundle(&scen, &out, 200_000);

    let verdicts = evaluate_bundle(&out).expect("evaluator runs");
    assert_eq!(
        verdicts.len(),
        3,
        "gossip_flap.toml declares three assertions"
    );

    let fails: Vec<&_> = verdicts
        .iter()
        .filter(|v| matches!(v.outcome, Outcome::Fail))
        .collect();
    assert!(
        !fails.is_empty(),
        "expected at least one Fail verdict on the current SWIM source; got {verdicts:#?}"
    );

    let self_inc = verdicts
        .iter()
        .find(|v| v.kind == "self_incarnation_bounded")
        .expect("gossip_flap.toml declares self_incarnation_bounded");
    assert_eq!(
        self_inc.outcome,
        Outcome::Fail,
        "self_incarnation_bounded should Fail (the production bug fingerprint); \
         got {self_inc:#?}"
    );
    assert!(
        !self_inc.evidence.is_empty(),
        "Fail verdict must carry evidence per §10.5"
    );
}
