//! TESTING_SPEC §10 — Adversarial sim-detector.
//!
//! `sim_indistinguishable` runs the sim-detector as an additional
//! peer inside the reference scenario and asserts that no D01–D12
//! technique ever returns `DetectedSim`. `prod_baseline` runs the
//! detector against the prod facade and asserts the symmetric
//! invariant (no `DetectedProd`).

#[path = "common.rs"]
mod common;

use common::{reference_scenario_text, run_engine, workspace_root};
use std::process::Command;

// ── §10.2 — Sim run ────────────────────────────────────────────────

#[test]
fn sim_indistinguishable() {
    // Build the reference scenario augmented with an extra
    // `detector` host that runs the sim-detector binary. The bundle
    // contains a `detector_report.json` after the run; the test
    // asserts every D01–D12 verdict is `Indistinguishable` or
    // `DetectedProd` (never `DetectedSim`).
    let spec = augment_with_detector(&reference_scenario_text());
    let bundle = run_engine(&spec, 51);

    let report_path = bundle.root.join("detector").join("verdicts.json");
    assert!(
        report_path.is_file(),
        "detector verdicts.json must be present at {}",
        report_path.display()
    );
    let report = read_verdicts(&report_path);
    let detected_sim: Vec<&Verdict> = report
        .iter()
        .filter(|v| v.outcome == "DetectedSim")
        .collect();
    assert!(
        detected_sim.is_empty(),
        "detector reported DetectedSim verdicts (TESTING_SPEC §10.2): {detected_sim:?}"
    );
}

// ── §10.2 — Prod baseline ──────────────────────────────────────────

#[test]
fn prod_baseline() {
    // Run the sim-detector binary directly against the prod facade
    // (built with `--features facade-prod`). The detector's stdout
    // is a JSON array of `{ id, outcome, evidence }` records. No
    // technique may return `DetectedProd`.
    let report = run_detector_prod();
    let detected_prod: Vec<&Verdict> = report
        .iter()
        .filter(|v| v.outcome == "DetectedProd")
        .collect();
    assert!(
        detected_prod.is_empty(),
        "detector reported DetectedProd against the prod facade: {detected_prod:?}"
    );
}

// ── Helpers ────────────────────────────────────────────────────────

#[derive(Debug)]
struct Verdict {
    id: String,
    outcome: String,
    evidence: Option<String>,
}

fn read_verdicts(path: &std::path::Path) -> Vec<Verdict> {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let value: serde_json::Value =
        serde_json::from_str(&text).expect("verdicts.json is JSON");
    value
        .as_array()
        .expect("verdicts.json is an array")
        .iter()
        .map(|v| Verdict {
            id: v["id"].as_str().expect("verdict.id is a string").to_string(),
            outcome: v["outcome"]
                .as_str()
                .expect("verdict.outcome is a string")
                .to_string(),
            evidence: v["evidence"].as_str().map(String::from),
        })
        .collect()
}

fn augment_with_detector(spec: &str) -> String {
    format!(
        "{spec}\n[[hosts]]\nname = \"detector\"\nrole = \"detector\"\n\
         start_at_ms = 0\nstop_at_ms = 60000\n"
    )
}

fn run_detector_prod() -> Vec<Verdict> {
    let cargo = option_env!("CARGO").unwrap_or("cargo");
    let output = Command::new(cargo)
        .args([
            "run",
            "-p",
            "simulation",
            "--bin",
            "sim-detector",
            "--quiet",
            "--features",
            "facade-prod",
            "--",
            "--emit-json",
        ])
        .current_dir(workspace_root())
        .output()
        .expect("invoke sim-detector binary in prod mode");
    assert!(
        output.status.success(),
        "sim-detector exited {} (prod): {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout).to_string();
    let value: serde_json::Value =
        serde_json::from_str(&text).expect("sim-detector emits JSON on --emit-json");
    value
        .as_array()
        .expect("sim-detector JSON is an array")
        .iter()
        .map(|v| Verdict {
            id: v["id"].as_str().expect("verdict.id").to_string(),
            outcome: v["outcome"].as_str().expect("verdict.outcome").to_string(),
            evidence: v["evidence"].as_str().map(String::from),
        })
        .collect()
}
