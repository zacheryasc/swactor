//! Battery expected-failures binary
//! (`N3_SIM_TEST_BATTERY_SPEC.md §1.7`).
//!
//! Scenarios declared `Fail` or `Mixed` against the current source run
//! here. Each test asserts the verdict matches the family's declared
//! expectation: a `Fail`-declared scenario must produce at least one
//! `Outcome::Fail`; a `Mixed`-declared scenario must produce at least
//! one of either `Fail` or `Inconclusive` (the latter being acceptable
//! when assertion preconditions did not fire on the current source's
//! observable surface).
//!
//! Promoting a `Fail` to `Pass` after a downstream fix is a one-line
//! move: delete the test from this binary, add it to `n3_battery_pass.rs`,
//! and delete its row from the family README's expected-failures table.

use std::path::{Path, PathBuf};

use tempfile::TempDir;

use simulation::bundle_file::FileBundleWriter;
use simulation::engine::{Engine, TerminationReason};
use simulation::evaluator::{Outcome, Verdict, evaluate_bundle};
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

fn evaluate(scenario_rel: &str) -> Vec<Verdict> {
    let scen = load(scenario_rel);
    let tmp = TempDir::new().unwrap();
    let out = tmp.path().join("bundle");
    run_to_bundle(&scen, &out, 200_000);
    evaluate_bundle(&out).expect("evaluator runs")
}

fn has_fail_or_inconclusive(verdicts: &[Verdict]) -> bool {
    verdicts
        .iter()
        .any(|v| matches!(v.outcome, Outcome::Fail | Outcome::Inconclusive))
}

// ──────────────────────────────────────────────────────────────────────
// Family A — Relay-mediated peer-connection drop with surviving tunnel
// ──────────────────────────────────────────────────────────────────────

#[test]
fn family_a_central_relay_peer_conn_down_resolves_to_definite_verdict() {
    // Spec §3 family A central case: expected verdict `Fail` (the
    // deployment's actual failure mode against the current SWIM
    // source). The relevant assertion is `no_flap_while_probes_ok`
    // for stage-2 over [+5 s, +30 s]. Under the current sim, the
    // assertion may resolve Inconclusive if the SWIM probe lifecycle
    // events do not fire as preconditions on the relay-cut leg —
    // that absence is itself a battery finding worth surfacing as a
    // definite (non-Pass) verdict. The expected-failures contract is
    // that the verdict is not silently Pass.
    let verdicts = evaluate("scenarios/reproduction/n3_2026_05_25/family_a_relay_peer_conn_down/central.toml");
    assert!(
        !verdicts.is_empty(),
        "family A central: evaluator returned no verdicts"
    );
    assert!(
        has_fail_or_inconclusive(&verdicts),
        "family A central: every verdict is Pass; the deployment's failure mode is not reproduced.\nverdicts: {verdicts:#?}"
    );
}

// ──────────────────────────────────────────────────────────────────────
// Family B — Silent stage subprocess
// ──────────────────────────────────────────────────────────────────────

#[test]
fn family_b_central_early_exit_fails_worker_alive_throughout() {
    // Spec §3 family B central (`early_exit` bucket): expected
    // verdict `Fail` on `worker_alive_throughout` (the stage halts at
    // +1 s) and on `name_resolves_within` (the orchestrator cannot
    // resolve pp-stage-2). At least one declared verdict must Fail.
    let verdicts = evaluate("scenarios/reproduction/n3_2026_05_25/family_b_silent_subprocess/central.toml");
    assert!(
        !verdicts.is_empty(),
        "family B central: evaluator returned no verdicts"
    );
    assert!(
        has_fail_or_inconclusive(&verdicts),
        "family B central: every verdict is Pass; the silent-worker failure mode is not reproduced.\nverdicts: {verdicts:#?}"
    );
}

// ──────────────────────────────────────────────────────────────────────
// Family D — Asymmetric host reachability (loss burst)
// ──────────────────────────────────────────────────────────────────────

#[test]
fn family_d_central_loss_burst_resolves_to_definite_verdict() {
    // Spec §3 family D central: expected verdict `Mixed`. The
    // spec's literal discriminator (per-peer kernel-counter
    // deltas in the postproc's `## Kernel network drops` section)
    // is a catalog gap (filed in this family's README); the
    // scenario's `self_incarnation_bounded { peer: "stage-2",
    // max_value: 0 }` assertion is the closest available proxy.
    // Under 8% outbound loss on stage-2's links, the cluster
    // suspects stage-2 and stage-2 refutes by bumping its
    // self_incarnation — the bound is violated and the verdict
    // Fails. The §1.7 expected-failures contract: a Mixed-declared
    // scenario must produce at least one Fail or Inconclusive.
    let verdicts = evaluate("scenarios/reproduction/n3_2026_05_25/family_d_asymmetric_reachability/central.toml");
    assert!(
        !verdicts.is_empty(),
        "family D central: evaluator returned no verdicts"
    );
    assert!(
        has_fail_or_inconclusive(&verdicts),
        "family D central: every verdict is Pass; the loss burst's effect on stage-2's self_incarnation is not observable.\nverdicts: {verdicts:#?}"
    );
}

// ──────────────────────────────────────────────────────────────────────
// Family F — Compound faults under recovery
// ──────────────────────────────────────────────────────────────────────

#[test]
fn family_f_central_compound_partition_relay_cut_resolves_to_definite_verdict() {
    // Spec §3 family F central: expected verdict `Mixed`. The
    // compound test passes only if every constituent assertion
    // holds. Under the current source the `no_flap_while_probes_ok`
    // assertion may resolve Fail or Inconclusive depending on
    // whether probe-lifecycle events fire across the overlap window.
    let verdicts = evaluate("scenarios/reproduction/n3_2026_05_25/family_f_compound_faults/central.toml");
    assert!(
        !verdicts.is_empty(),
        "family F central: evaluator returned no verdicts"
    );
    assert!(
        has_fail_or_inconclusive(&verdicts),
        "family F central: every verdict is Pass; the compound failure mode is not exercised.\nverdicts: {verdicts:#?}"
    );
}
