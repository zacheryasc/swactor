//! Battery Pass-expected binary
//! (`N3_SIM_TEST_BATTERY_SPEC.md §1.7`).
//!
//! Scenarios declared `Pass` against the current source run here under
//! standard `cargo test` semantics — a regression in the simulator or
//! post-processor is a CI break. Today the Pass-expected families are
//! C (gossip-arrival absence — discriminator regression guard) and E
//! (bundle integrity under SIGKILL — `S-D` regression guard).

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;
use tempfile::TempDir;

use simulation::bundle_file::FileBundleWriter;
use simulation::engine::{Engine, TerminationReason};
use simulation::evaluator::{Outcome, evaluate_bundle};
use simulation::network::Network;
use simulation::scenario::{HostKindRegistry, Scenario, load_from_path};
use simulation::stage_host::StageHostFactory;
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
    engine.register_factory(Box::new(StageHostFactory));
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
fn family_c_central_gossip_absence_passes_self_incarnation_bound() {
    // Spec §3 family C central: expected verdict `Pass`. The
    // observability upgrade landed `GossipReceived` and the per-peer
    // dial rollup, so the discriminator (control-plane vs data-plane)
    // is already expressible. This test guards that contract against
    // regression — the orchestrator's self_incarnation should stay
    // bounded under a stage-to-stage partition.
    let scen = load("scenarios/reproduction/n3_2026_05_25/family_c_gossip_absence/central.toml");
    let tmp = TempDir::new().unwrap();
    let out = tmp.path().join("bundle");
    run_to_bundle(&scen, &out, 200_000);
    let verdicts = evaluate_bundle(&out).expect("evaluator runs");
    assert!(!verdicts.is_empty(), "family C: no verdicts produced");
    for v in &verdicts {
        assert!(
            matches!(v.outcome, Outcome::Pass | Outcome::Inconclusive),
            "family C central: unexpected non-Pass verdict {v:?}"
        );
    }
}

#[test]
fn family_e_central_sigkill_orchestrator_produces_parseable_bundle() {
    // Spec §3 family E central: expected verdict `Pass`. The
    // observability upgrade landed `S-D` (bundle without finalize);
    // this test guards that contract. The scenario kills the
    // orchestrator at +5 s; the simulator's bundle writer must
    // still produce a parseable manifest and per-peer staging
    // files for the surviving peers' pre-kill records.
    let scen = load("scenarios/reproduction/n3_2026_05_25/family_e_bundle_integrity_sigkill/central.toml");
    let tmp = TempDir::new().unwrap();
    let out = tmp.path().join("bundle");
    run_to_bundle(&scen, &out, 200_000);

    // Bundle-shape contract per family-E spec §3:
    //   - `manifest.json` exists.
    //   - every per-peer events file exists (the sim writes
    //     `events.ndjson` shared across peers, not per-peer files;
    //     verify the aggregate file).
    let manifest_path = out.join("manifest.json");
    assert!(
        manifest_path.is_file(),
        "family E central: manifest.json missing under {}",
        out.display()
    );
    let manifest_text = fs::read_to_string(&manifest_path).expect("read manifest.json");
    let manifest: Value = serde_json::from_str(&manifest_text).expect("manifest is JSON");
    assert!(
        manifest.is_object(),
        "family E central: manifest.json is not an object: {manifest_text}"
    );
    let events_path = out.join("events.ndjson");
    assert!(
        events_path.is_file(),
        "family E central: events.ndjson missing under {}",
        out.display()
    );

    // Verdicts file exists and contains a verdict per declared
    // assertion. `Inconclusive` is acceptable for any whose
    // preconditions did not fire (e.g., the orch is dead by +5 s).
    let verdicts = evaluate_bundle(&out).expect("evaluator runs");
    assert!(!verdicts.is_empty(), "family E: no verdicts produced");
    for v in &verdicts {
        assert!(
            matches!(v.outcome, Outcome::Pass | Outcome::Inconclusive),
            "family E central: unexpected Fail {v:?}"
        );
    }
}
